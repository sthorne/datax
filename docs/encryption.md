# Encryption at rest

`--enc-key <file>` makes a node encrypt everything Pebble writes to its
data directory — sstables, WAL, MANIFEST, OPTIONS — plus the periodic
metadata backup. The key file holds the 32-byte **store key**, as 32 raw
bytes or 64 hex characters (`openssl rand -hex 32 > store.key`). All
crypto is the Go standard library.

```sh
datax init --dir data1 --enc-key store.key ...
```

## What it protects, and what it does not

Encryption at rest answers first of all one threat: **a disk that leaves
the building** — a decommissioned drive, a stolen laptop, a cloud volume
snapshot in the wrong hands, a backup tape. Without the store key the
files are noise. That is confidentiality, and every encrypted store has
it.

What a store does against an attacker who can **write to** its files (a
compromised host, a shared or network-backed volume, a hostile
backup-and-restore path, an operator without database privileges)
depends on which file format it holds, and a store can be looked up
(`/api/security`, the console's security view, the node page's
*encryption* tile):

- **Unauthenticated** (`DXE1`, every store before cluster version v18).
  File content is AES-256-CTR with no authentication tag, and the file
  header is not authenticated either; only the key registry and the
  metadata backup are sealed with an authenticating mode (AES-256-GCM).
  CTR is bit-for-bit malleable, and Pebble's block checksums are
  error-detecting codes with no key — whoever can change the bytes can
  recompute them — so an attacker with write access can truncate a WAL
  (committed writes vanish on restart as if after a crash), flip chosen
  bits in a chosen row given knowledge of the plaintext (the node serves
  the altered value as authentic), swap a header's key id or IV (seen as
  corruption, not attack), or substitute or roll back a whole file.
  Nothing notices but the cross-replica consistency sweep
  (`--consistency-interval`, off by default), after the fact and only
  where a replica disagrees.
- **Authenticated** (`DXE2`, from cluster version v18; see *Authenticated
  files* below). File content is sealed with AES-256-GCM under a per-file
  key — sstables in 4 KiB chunks read at random, the WAL and MANIFEST as
  append-only frames, one per write — with the file's header in every
  tag, the chunk's or frame's position in its nonce, and, for sstables
  and WAL segments, the file's own name in every tag too. A byte altered
  anywhere, a chunk or frame moved within or between files, an sstable
  truncated anywhere but exactly between chunks, and a renamed sstable
  or WAL are **refused at read** — the node reports an error, it does not
  serve the data. What the format still does not catch: an sstable
  truncated exactly at a chunk boundary and a log cut anywhere (either
  looks like a crash, as before, and a crash is what a log's torn tail
  usually is; sstables and the MANIFEST carry their own length checks
  above this layer, a WAL's records their own), and a whole file replaced
  by an older version of itself or by another file of the same kind
  under the same name from a store sealed with the same data key, since
  nothing above the file layer hands it a version. The consistency sweep
  remains the thing that would notice those.

If the threat is tampering rather than theft, run a cluster at v18 or
later and let every store complete the switch, put the data directory on
storage the attacker cannot write, and run the sweep.

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

Each file is in one of two formats. The unauthenticated one is
`"DXE1" | data-key ID | 16-byte random IV`, then AES-256-CTR ciphertext,
the keystream counter derived from the IV and the logical offset so
random reads (`ReadAt`) never re-derive more than one block. The
authenticated one is `"DXE2" | data-key ID | flags | chunk size | 16-byte
random salt`, then either chunks of `nonce | AES-256-GCM(4 KiB of
plaintext) | tag` (sstables, OPTIONS, markers: anything read at random
offsets) or, for the files Pebble appends to and replays front to back
(WAL segments, MANIFEST files; a header flag says which), frames of
`length | nonce | AES-256-GCM(one write's bytes) | tag`. The file key is
HKDF-SHA256(data key, salt). The GCM nonce is the chunk's or frame's
index and eight random bytes drawn at every write and stored beside it,
so a chunk rewritten (an sstable writer extends its tail chunk) is never
sealed twice under one nonce and nothing has to be remembered across a
crash for that to hold. The whole header is in every tag's additional
data, so no header field can be edited, and for files Pebble never
renames — sstables, WAL segments — the base name is too. A chunked read
decrypts whole chunks and an append rewrites the tail chunk; a framed
write appends a frame and never touches a byte already on disk, which
is what lets a crash during an append cost only the record being
appended (a torn final frame is the end of the log, as Pebble's own
record checksum treats it; a frame that fails with frames after it is
refused). `Stat` and the offset hints translate between logical and
physical offsets. Details that matter for correctness and
confidentiality:

- **`ReuseForWrite` never recycles.** Rewriting a recycled WAL under its
  original key+IV would reuse the CTR keystream — an actual plaintext
  recovery attack, not hygiene. The old file is removed and a fresh one
  (new IV, current data key) created. This forfeits Pebble's WAL
  recycling and is the dominant cost of encryption (see below).
- **`Fd()` passes through**, audited against Pebble v1.1.5 and again
  against v2.1.7 (the 0.41.0 upgrade, issue #166): the fd is used only
  for flock, fadvise hints, fallocate and sync_file_range — syscalls
  that never read or write file content; nothing outside Pebble's `vfs`
  package touches it — and hiding it costs real WAL fsync latency (no
  preallocation). The audit must be repeated on any Pebble upgrade
  (`grep -rn '\.Fd()'` over the module, outside `vfs/` and the tests);
  an fd used for mmap would read ciphertext.
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

## Authenticated files

A store switches from `DXE1` to `DXE2` in two steps, both recorded in
its sealed key registry so neither can be undone without the store key:

1. **New files are written authenticated**, from the moment the node
   observes cluster version v18 (so every node can read the format
   before any writes it) and at every open from then on — Pebble creates
   its OPTIONS, MANIFEST and WAL before the engine can tell the node
   anything, so the FS has to know at open, from the registry. At each
   open the files Pebble keeps across opens and would otherwise leave in
   the old format for good — the MANIFEST, OPTIONS, its format markers, a
   WAL about to be replayed — are converted first (temp file, rename,
   directory sync). The sstables are the re-encryption sweep's:
   `datax debug reencrypt` counts an old-format sstable as stale and
   rewrites it exactly as it rewrites one under a retired data key.
2. **The old format is refused** once no file of it remains: checked
   whenever the node learns its cluster version (every start) and when
   a sweep finishes. Until then a reader that accepted both formats
   would accept a file rewritten as the old one — the downgrade the
   switch exists to close — so the switch is not complete, and the
   store says so, until the refusal is in force.

So the operator's path after finalizing v18 is: run
`datax debug reencrypt --wait` on each node, restart it, and read
`encrypted, authenticated` in the security view. The node logs each
transition once, and the log and the status say how many old-format
files remain in between. An old-format sstable Pebble has made obsolete
but not yet deleted counts among them and is not the sweep's to rewrite;
it goes when Pebble deletes it, shortly after. A conversion the node
cannot carry out — a file it cannot examine — holds the cutover rather
than passing the file over: the switch is one way, so an error means
"not yet". A store that has written any authenticated file is refused
by a v17 binary's version gate, like every store-format change before
it.

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

- **A store before cluster version v18, or one that has not completed
  the switch, is not authenticated** (see *What it protects*): its
  `--enc-key` protects a disk that leaves the building, not a store an
  attacker can write to. An attacker with write access can alter,
  truncate, substitute or roll back its files undetectably; the
  cross-replica consistency sweep is the only thing that would notice,
  and only after the fact.
- **The authenticated format does not see an sstable truncated exactly
  at a chunk boundary, a log cut anywhere, nor a whole file replaced by
  an older version of itself** or by another file of the same kind and
  name sealed with the same data key; Pebble's own length checks catch
  the first for sstables and the MANIFEST and a WAL's record checksums
  the second, and the consistency sweep is what notices the rest.
- **The key registry can be rolled back as a whole**: its two cutover
  flags cannot be cleared without the store key, but an older, validly
  sealed copy of the registry — a backup from before the switch — opens
  the store unauthenticated again, writing the old format until the node
  next observes cluster version v18 (which re-enables authenticated
  writes within seconds of start) and accepting old-format files until
  the cutover completes again. That is the whole-file rollback class
  above, applied to the one file whose rollback turns the feature off
  rather than one file's contents. The cluster version, in replicated
  KV, is a signal a single disk cannot roll back; cross-checking the
  registry against a record of the cutover kept there, before the store
  serves, is filed as #248.
- Authenticated files cost 24 bytes per 4 KiB chunk (about 0.6%) and 28
  bytes per WAL or MANIFEST append, a whole chunk decrypted for any read
  inside it (Pebble's block cache holds the plaintext, so a cache hit
  never touches the wrapper), and one chunk re-sealed and rewritten for
  every append to an sstable's tail chunk; a log append writes only its
  own frame.
- The `LOCK` file and directory entries (file names, sizes) are not
  encrypted; file names are Pebble's numeric names and leak no user data.
- Keys live in memory unprotected (no mlock/HSM integration).
- Re-encryption covers live sstables; a stale file spanning a single
  user key waits for natural compaction churn.
- The node never rewrites the operator's key file: after an online
  rotation, a restart needs the new key in one of the `--enc-key`
  files (stage it beforehand, see Key rotation).
