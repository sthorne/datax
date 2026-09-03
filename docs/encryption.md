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
  offline mode (`--dir`, node stopped) is the way to rotate. One
  rotation runs at a time on a node: the registry reseal, the key swap
  and the backup reseal are serialized end to end, so two overlapping
  rotations cannot leave the registry and the backup under different
  keys. The offline mode also remains for damaged stores.

  **Stage the new key before rotating.** `--enc-key` accepts a
  comma-separated list of key files; at startup the node tries each
  against the store's registry and opens with the one that matches
  (`datax debug metadata` and `unsafe-recover` accept the same list).
  So the safe sequence is: write `new.key`, restart (or, for a running
  node, plan the next restart) with `--enc-key store.key,new.key`,
  rotate online, and retire `store.key` at leisure — a crash or a
  supervisor restart anywhere in that window finds a key that opens the
  store. After a rotation the node logs which of its key files holds
  the new key, or a warning that none does (then a restart before the
  file is added fails with "none of the store keys given matches").

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

  Manual compaction is by key range, not by file, and Pebble takes as
  inputs every file overlapping the span at every level, so what a
  stale file costs is set by the span compacted. An L0 file's span is
  unbounded — a small pre-shutdown flush of scattered writes covers
  most of the keyspace, and compacting its own bounds rewrote 30 MiB of
  a 63 MiB store to retire one 21 KiB file — so L0 files are compacted
  by the *narrowest* span that overlaps them, their smallest key
  through the seed: a single column, the file with its Lbase overlap
  and then one file per level below (Pebble splits compaction outputs
  at the next level's target size and grandparent overlap). Files below
  L0 are bounded by construction and are compacted by their own bounds,
  which retires their stale neighbours in the same compaction. The
  burst budget and `rewritten_bytes_total` count what Pebble actually
  wrote during each compaction, from its compacted-bytes counters, so
  they include any background compaction that happened to run at the
  same time. A file a compaction cannot rewrite (a single-user-key file,
  a local-key-only file already at the bottom level) is attempted once
  per run and then skipped, so it never starves the files behind it;
  the worker stops when nothing more can be rewritten and logs what
  remains — those files retire with natural churn. `--wait` follows the
  worker and exits non-zero if bytes remain or the stale-file sweep
  failed; a status carrying `sweep_error` attests nothing (its counts
  are the last good reading).

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
- The node never rewrites the operator's key file: after an online
  rotation, a restart needs the new key in one of the `--enc-key`
  files (stage it beforehand, see Key rotation).
