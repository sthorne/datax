# Encryption at rest

`--enc-key <file>` makes a node encrypt everything Pebble writes to its
data directory — sstables, WAL, MANIFEST, OPTIONS — plus the periodic
metadata backup. The key file holds the 32-byte **store key**, as 32 raw
bytes or 64 hex characters (`openssl rand -hex 32 > store.key`). All
crypto is the Go standard library.

```sh
datax init --dir data1 --enc-key store.key ...
```

## Design

Encryption lives in `pkg/storage/enc` as a `vfs.FS` wrapper around
Pebble's filesystem interface, so Pebble itself is untouched and the
choice is per-node: replication carries logical KV data, never files, so
encrypted and plaintext nodes coexist in one cluster (encrypt a cluster
by rotating nodes through wiped, encrypted stores).

Two key levels:

- The **store key** (from `--enc-key`) only seals the key registry and
  the metadata backup with AES-256-GCM.
- **Data keys** encrypt file content. A fresh data key is minted and made
  active on every store open, bounding how much ciphertext any one key
  covers; old keys are kept in the registry so existing files stay
  readable.

The registry (`ENCRYPTION-REGISTRY` in the data dir) is
`"DXR1" | GCM nonce | AES-256-GCM(storeKey, JSON{active_key_id, keys})`,
written atomically. Its presence is what marks a store as encrypted.

Each file is `"DXE1" | data-key ID | 16-byte random IV`, then AES-256-CTR
ciphertext. The keystream counter is derived from the IV and the logical
offset, so random reads (`ReadAt`) never re-derive more than one block.
Details that matter for correctness and confidentiality:

- **`ReuseForWrite` never recycles.** Rewriting a recycled WAL under its
  original key+IV would reuse the CTR keystream — an actual plaintext
  recovery attack, not hygiene. The old file is removed and a fresh one
  (new IV, current data key) created. This forfeits Pebble's WAL
  recycling and is the dominant cost of encryption (see below).
- **`Fd()` passes through**, audited against Pebble v1.1.5: the fd is
  used only for flock, fadvise hints, fallocate and sync_file_range —
  syscalls that never read or write file content — and hiding it costs
  real WAL fsync latency (no preallocation). The audit must be repeated
  on any Pebble upgrade; an fd used for mmap would read ciphertext.
- `Stat` subtracts the 24-byte header; `SyncTo`/`Preallocate`/`Prefetch`
  offsets are shifted by it — Pebble's size bookkeeping sees logical
  sizes throughout.

## Opening a store: the validation matrix

| Store state | `--enc-key` | Result |
|---|---|---|
| registry present | correct key | opens; fresh data key minted |
| registry present | wrong key | refused: "encryption key does not match store" |
| registry present | none | refused: "--enc-key is required" |
| plaintext store exists | key given | refused — no silent in-place conversion |
| empty dir | key given | initializes encrypted |
| no registry | none | plaintext, exactly as before |

## Key rotation

- **Data keys** rotate themselves: every node start mints a new active
  key.
- **Store key**, online (the node stays up):

  ```sh
  datax debug rotate-enc-key --addr 10.0.0.1:26257 \
    --old-key store.key --new-key new.key --certs-dir certs [--user ops]
  ```

  The `rotate-store-key` admin RPC (admin role required) verifies the
  old key against the on-disk registry (GCM authentication), reseals
  the registry atomically (tmp + rename + directory fsync — the same
  path the offline mode uses), swaps the key the node seals artifacts
  with, and immediately re-seals the metadata backup. Data keys and
  file contents are untouched, so rotation is O(registry), not
  O(store). The request carries both store keys, so the op is served
  only over mutual TLS: on an insecure cluster it is refused, and the
  offline mode (`--dir`, node stopped) is the way to rotate. Restart the
  node with `--enc-key new.key` afterwards (the node logs a reminder
  that the old key file is now stale). The offline mode also remains
  for damaged stores.

- **Background re-encryption** retires old-data-key exposure on cold
  data. `datax debug reencrypt --addr ... [--wait]` starts a paced pass
  that rewrites live sstables still encrypted under retired data keys
  (64 MiB per burst, 500ms pauses) by scheduling manual compactions; a
  rewritten file carries the active key. Progress:
  `datax_reencryption_remaining_bytes` (gauge, 0 = attested: no live
  sstable under a retired key), `datax_reencryption_rewritten_bytes_total`,
  and `datax debug reencrypt-status`. Only sstables can be stale — the
  WAL, MANIFEST, and OPTIONS are recreated under the fresh active key at
  every open.

  Manual compaction is by key range, so a stale file's cost is the
  on-disk size of its span across all levels (a pre-shutdown flush file
  can span the whole keyspace); the burst budget counts that estimate,
  and `rewritten_bytes_total` reports it. A file a compaction cannot
  rewrite (a single-user-key file, a local-key-only file already at the
  bottom level) is attempted once per run and then skipped, so it never
  starves the files behind it; the worker stops when nothing more can
  be rewritten and logs what remains — those files retire with natural
  churn. `--wait` follows the worker and exits non-zero if bytes remain
  or the stale-file sweep failed; a status carrying `sweep_error`
  attests nothing (its counts are the last good reading).

  Mechanically, each stale file's compaction is *seeded* with a Pebble
  point tombstone at `smallest + 0x00` — provably not a valid MVCC key
  encoding, so nothing can ever write it and no reader can observe it —
  because Pebble's manual compaction otherwise never touches a file
  already resting in the bottom level and trivially MOVES single files
  without rewriting them. The seed gives it a real L0 input to compact
  through, and the compaction itself elides the seed. One caveat: a
  stale file spanning a single user key admits no interior seed and
  waits for natural churn.

## Recovery tooling

`datax debug metadata --dir ... --enc-key store.key` unseals the sealed
metadata backup (magic `DXMB1`); `datax debug unsafe-recover` takes the
same `--enc-key`. Losing the store key means losing the store — there is
deliberately no escrow.

## What it costs

`bench kv` single node, 16 workers, 30s, same machine (AES-NI present),
plaintext vs encrypted:

| | plaintext | encrypted |
|---|---|---|
| mixed 95/5 | 14,305 ops/s, p50 986µs | 12,383 ops/s (−13%), p50 1.12ms |
| read-only p50 | 253µs | 249µs (no measurable overhead) |
| write-only p50 | 4.9ms | 5.9ms (+20%) |

The read path is free in practice — Pebble's block cache holds decrypted
blocks, so cache hits never touch the wrapper. The write cost is **not**
AES (encrypting a commit's bytes is microseconds): it is the loss of WAL
recycling. `ReuseForWrite` never recycles a ciphertext file, so every WAL
is freshly allocated and every fdatasync journals block-allocation
metadata; a recycled plaintext WAL overwrites already-allocated blocks
and its fdatasync is data-only. CockroachDB's encrypted Pebble disables
WAL recycling for the same reason. Recorded runs live in issue #27.

## Limitations

- The `LOCK` file and directory entries (file names, sizes) are not
  encrypted; file names are Pebble's numeric names and leak no user data.
- Keys live in memory unprotected (no mlock/HSM integration).
- Re-encryption covers live sstables; a stale file spanning a single
  user key waits for natural compaction churn.
- After an online rotation the node's `--enc-key` file must be updated
  before its next restart (the node does not rewrite the operator's key
  file).
