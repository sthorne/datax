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

### The switch, cross-checked

The registry's two cutover flags cannot be cleared without the store
key, but the registry can be replaced wholesale by an older, validly
sealed copy — a backup or a volume snapshot from before the switch —
and the store would then reopen unauthenticated. So the switch is
recorded twice more, where a registry rollback cannot reach (issue
#248):

- **in the store itself**, a marker in the state engine written after
  each registry transition. The registry file alone rolled back beside
  a store that was not is caught at open, before Pebble has written a
  file: the marker says more than the registry, and the node refuses
  to start with `encryption: the key registry was rolled back`.
- **in the cluster**, a record per store in replicated KV, written by
  the node once it has made each step. A whole volume rolled back —
  marker and all — opens, and is caught on the node's first heartbeat
  that can read KV: the record says the store had switched, the store
  opened without it, and the node logs the refusal, records a
  `security` event and stops serving.

Both compare against what the engines had when they were opened, not
what they have by the time the record is read: the node re-enables
authenticated writes within seconds of observing v18, which is what
a rolled-back store would show by then.

A rollback that was meant — a store restored from an old backup on
purpose — is accepted by starting the node once with
`--accept-encryption-rollback`: the marker and the record are lowered
to what the store has, and rise again as the store re-completes the
switch (writes within seconds; the old format refused after the sweep
and a restart, as above).

## Roots: where the store key lives

`--enc-key` makes the store key a file the operator writes. `--enc-root`
is the alternative: the node makes its own store key, and what the data
directory holds (`ENCRYPTION-STOREKEY`, beside the registry) is that key
**wrapped** by a *root* — something outside the data directory that can
encrypt a small blob and decrypt it again.

```
root          wraps    store key    seals    registry of data keys    which encrypt    files
(--enc-root)           (per node)            (per node)
```

Nothing below the store key changes. What changes is that a store key
exists in the clear only in the node's memory: not in a file, and not
in the request that rotates it.

```sh
datax init  --dir data1 --enc-root exec:/usr/local/bin/datax-wrap ...
datax start --dir data1 --enc-root exec:/usr/local/bin/datax-wrap ...
```

Two roots exist today:

- **`file:<path>`** — a 32-byte wrapping key in a file (raw or hex, as a
  key file is). This moves the problem one step: the wrapping key is a
  file where the store key was. It is for a wrapping key kept on
  storage other than the data's, for tests, and as a step when moving a
  store to another root.
- **`exec:<command> [args...]`** — the node runs the operator's command
  to do the wrapping. This is the bridge to anything with a command
  line: a TPM (`tpm2-tools`), a cloud KMS's CLI, an HSM vendor's tool.
  The command is run directly (no shell), with the node's environment,
  with one more argument:

  | verb | standard input | standard output |
  |---|---|---|
  | `wrap` | the store key, 32 raw bytes | the wrapped key, any bytes up to 64 KiB |
  | `unwrap` | the wrapped key, exactly as `wrap` wrote it | the store key: 32 raw bytes or 64 hex characters |
  | `key-id` | — | a one-line name for the wrapping key (optional; a non-zero exit means "unnamed") |

  Exit status zero is success; anything else is a failure, and the
  command's standard error is the reason the operator is shown. Each
  call is cut off after 30 seconds. A sketch for AWS KMS:

  ```sh
  #!/bin/sh
  KEY=arn:aws:kms:eu-west-1:111122223333:key/…
  case "$1" in
    key-id) echo "$KEY" ;;
    wrap)   aws kms encrypt --key-id "$KEY" --plaintext fileb:///dev/stdin \
              --query CiphertextBlob --output text ;;
    unwrap) base64 -d | aws kms decrypt --ciphertext-blob fileb:///dev/stdin \
              --query Plaintext --output text | base64 -d ;;
  esac
  ```

What the node does with a root, and refuses:

- A root's answer is checked before anything is sealed under it: the
  wrapped form must not contain the key, must be non-empty and bounded,
  and must unwrap to the same key. A root that fails, hangs or returns
  the key unwrapped is refused, and no wrapped-key file is written.
- The wrapped-key file is not secret, but it cannot be edited: each
  entry carries a tag, keyed from the store key it wraps, over the
  root's kind, its key id and the wrapped bytes.
- A node that cannot reach its root cannot start. A running node is
  unaffected: it unwrapped its key at start.
- `--enc-key` and `--enc-root` together are refused. A root does not
  take over a store it did not make: one opened with a key file is
  refused with a pointer to `datax debug enc-root adopt`, and a
  plaintext one is refused as with `--enc-key`.
- The command line of an `exec` root comes from the node's own flags
  only. No request can name a command for a node to run; an online
  re-wrap names its target by position in the node's `--enc-root` list.
- `/api/security` and the console's security view say, per store,
  whether its key is under a root (kind and key id) or from a key file.
  Every unwrap at start, and every rotation and re-wrap, is audited.

What a root does and does not give depends on the root. A `file` root
on the same disk as the data protects nothing a key file would not. An
`exec` root is as strong as what its command talks to. Neither changes
the fact that the store key is in the node's memory while it runs.
A root key split among operators is the next section. Roots handed
between running nodes, or held in a KMS spoken to directly, are planned
(#307, #308).

### A root key held as shares

`--enc-root shares` keeps the root key nowhere. The key is made once,
split into `n` shares of which any `k` rebuild it, the shares are given
to people, and the key is forgotten:

```
datax debug enc-root split --shares 2/3 --out-dir /dev/shm/shares
```

prints or writes three shares (`datax-share-…`, one per file, mode
0600, never over an existing file) and keeps nothing. The split is
Shamir's scheme over GF(256): each byte of the key is the constant term
of its own random polynomial of degree `k-1`, and a share is the
polynomials' values at one point. `k` shares determine the key; `k-1`
are consistent with every key equally, so they say nothing about it.
One root key serves a whole cluster: each node's store key is its own,
wrapped under the same root.

A node started with `--enc-root shares` cannot open its store, and
starts **sealed**:

```
$ datax start --dir data1 --enc-root shares --certs-dir certs ...
datax node is SEALED: its store key is under a root key held as shares ...

$ datax unseal --addr 10.0.0.1:26257 --certs-dir certs      # one share-holder
share:                                                       # typed, not echoed
share accepted
sealed: 1 of 2 shares in (share 3); waiting for shares of root key 644fe2bbf57d9721

$ datax unseal --addr 10.0.0.1:26257 --certs-dir certs      # another
share accepted: unsealed. The node is opening its store (root key 644fe2bbf57d9721)
```

- **Sealed means closed.** A sealed node has loaded its certificates
  and listens on its RPC address, where it serves `unseal` and
  `datax debug seal-status` and refuses everything else. Its SQL port
  and console are not open and it runs no raft: to the cluster it is a
  node that is down, and is counted as one. A new node starts sealed
  too — it needs the root key to wrap the store key it makes — and its
  first share says which root key it will be under.
- **Who may unseal.** The root user's client certificate. A sealed node
  cannot read its role table, so membership of the admin role cannot be
  checked until it is open. On a cluster without TLS a share is accepted
  only from the node's own machine, since it would otherwise cross the
  network in the clear.
- **A share is never a flag.** `datax unseal` asks for it without echo,
  or reads one line of standard input (for a password manager's CLI);
  it does not accept one as an argument or from the environment. Shares
  and the root key are never written to disk, logged, or put in an
  event. Every submission is audited — who, which share by its number,
  the outcome — without the share.
- **What is refused, and when.** A share carries a check over its own
  bytes and the id of the root key it is a share of. A mistyped or
  damaged share, a share of another cluster's key, a share already
  submitted and a share of a different split of the key are each
  refused as they arrive, with the reason. If a set of shares that all
  pass still does not rebuild the key they name — one was forged — the
  node stays sealed and discards them rather than open on a wrong key.
  More than ten refusals in a minute pause submissions; a share is 256
  random bits, so this is against noise, not guessing.
- **In memory while running.** Once unsealed the node holds the root
  key in memory, as it holds the store key: that is what lets it rotate
  its store key without the share-holders. A restart seals it again.
- **Offline tools ask for shares.** `datax debug metadata`,
  `rotate-enc-key --dir`, `unsafe-recover` and `enc-root
  adopt|rewrap|release` given `--enc-root shares` prompt for shares
  until they have a quorum.

Changing who holds shares, or how many:

```
datax debug enc-root resplit --shares 3/5 --out-dir /dev/shm/new   # asks for a quorum of the current shares
```

makes a new split of the **same** root key. The old shares still
rebuild it among themselves — a new split cancels nothing — so this is
for a holder trusted to have destroyed their share. To shut out one
who is not, replace the root key: make a new one with `split`, have its
holders submit shares to each **running** node with `datax unseal`
(there, shares assemble a second root key beside the one in use), then
move each store key under it and restart on the new shares when
convenient:

```
datax debug seal-status --addr 10.0.0.1:26257 --certs-dir certs       # "a new root key (…) is held as root 2"
datax debug enc-root rewrap --addr 10.0.0.1:26257 --certs-dir certs --to-root 2
```

What this root gives: a disk, every disk, or a powered-off machine
opens nothing, and no one person can open it either. What it costs: a
cold start of the cluster needs `k` people (a restart of one node does
not: see *Peer unseal*), and losing more than `n-k` shares
loses the root key — after which a node that is running can still be
moved to a new root key, and one that is stopped cannot be opened by
anyone.

### Peer unseal

With shares alone, every start of every node would need a quorum of
people. A running node holds the root key in memory, so a sealed node
asks the peers it knew for it before it settles down to wait for
shares, and a rolling restart needs nobody.

A running node hands the root key over only when all of these hold:

- the asker presents the cluster's **node** certificate over mutual TLS
  (a client certificate, root's included, is refused; a cluster without
  TLS has no peer unseal);
- it names this cluster, and the root key the answering node itself
  runs under;
- its node id is a member of the cluster and is not being
  decommissioned;
- that node is **not live**: its last heartbeat is older than the
  liveness grace (15 s by default), so a second copy of a running node
  is handed nothing. A node restarted at once therefore waits out the
  grace, asking every two seconds;
- peer unseal is on.

The key crosses inside the TLS connection sealed under a key agreed for
that one exchange (X25519, bound to the cluster, node and key ids), so
something that terminates and forwards the TLS sees no key; that layer
is against reading, not impersonation, which is the certificate's job.
The asker takes the key only if it is the one its own wrapped store key
names. Every request is audited on the node that answers, granted or
refused with the reason, appears in the console's events, and is
counted in the security view. The node keeps, unencrypted in its data
directory, the little it needs to ask before its store is open
(`SEAL-IDENTITY`: cluster id, node id, peers' addresses — none secret,
and trusted for nothing but where to ask).

```
datax debug peer-unseal --addr 10.0.0.1:26257 --certs-dir certs          # report
datax debug peer-unseal --addr 10.0.0.1:26257 --certs-dir certs --off    # every start needs shares
```

The setting is the cluster's; each node keeps a copy so it can answer
during a cold start, before there is a quorum to read it from.

What peer unseal keeps and gives up:

- It keeps the promise that matters for encryption at rest: a disk that
  leaves the building cannot open itself, because it cannot reach the
  cluster.
- It does **not** protect against someone who holds a node's
  certificate and private key **and** can reach the cluster while that
  node is down: that is a node, as far as the cluster can tell. With
  peer unseal on, a node key kept on the same disk as the data is all
  that stands between a stolen disk and its contents for an attacker on
  the network. Where that is in scope, keep the node key elsewhere (or
  in hardware), or turn peer unseal off.
- A cold start of the whole cluster still needs the share-holders, by
  design — for one node; the others take the key from it.
- A node that has never been a member (a new node joining) has no id
  to be vouched for and is unsealed with shares.

## Keys in memory

The keys a node holds for as long as it runs — its store key, the data
keys of its registry, a `file` root's wrapping key, a root key rebuilt
from shares — are kept in memory set aside for them rather than on the
ordinary heap (#309). On Linux that memory is `mlock`ed, so it is never
written to swap, and marked `MADV_DONTDUMP`, so it is left out of core
dumps; a key is zeroed when it is released (a store closing, a root
discarded). Short-lived copies made on the way — a key file's bytes as
read, a candidate key that did not match — are zeroed when done with.

A node started with `--enc-key` or `--enc-root` also turns core dumps
off for the whole process: its core-file limit is set to zero and it is
marked not dumpable, which additionally stops another process of the
same user from reading its memory. `--allow-core-dumps` leaves the
process dumpable, for debugging.

The start log says how this went, and `/api/security` (`key_memory`)
and the console's security view report it per store. Locking can fail
under a low memory-lock limit (16 KiB is needed; `ulimit -l`, or
`LimitMEMLOCK=` in a systemd unit): by default that is a warning that
names the remedy and the node runs; `--require-locked-keys` makes it,
and a failure to disable core dumps, a refusal to start.

What this does not cover:

- A cipher built from a key holds its own expanded copy, and the Go
  runtime may copy a key onto a stack; those copies are on ordinary
  memory. Disabling core dumps is what covers them against a dump, and
  nothing covers them against swap short of encrypted swap or none.
- A privileged user on the host, or anything that can read this
  process's memory, can read the keys regardless.
- Plaintext data in the block cache is not key material and is not
  locked.
- On platforms other than Linux keys are held on the heap and reported
  as not locked.

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

- **Under a root** (`--enc-root`), rotation involves no key file and
  no key in transit:

  ```sh
  datax debug rotate-enc-key --addr 10.0.0.1:26257 --certs-dir certs   # online
  datax debug rotate-enc-key --dir data1 --enc-root exec:/usr/local/bin/datax-wrap   # node stopped
  ```

  The node makes the new store key, has the root wrap it, and reseals
  the registry and the metadata backup. The request carries nothing but
  the operation's name, so it is served on any cluster. The order makes
  a crash anywhere survivable: the wrapped-key file is first rewritten
  to hold the new key's entry and the old one's, then the registry is
  resealed, then the old entry is dropped; a start in between unwraps
  both, uses the one the registry reached, finishes a split store's
  second registry if it was left behind, and drops the other.

  When the root's own key has rotated, or to move a store to another
  root, the store key is **re-wrapped** — one small file replaced in
  one rename, nothing else touched:

  ```sh
  datax debug enc-root rewrap --addr 10.0.0.1:26257 --certs-dir certs              # same root, its new key
  datax debug enc-root rewrap --addr 10.0.0.1:26257 --certs-dir certs --to-root 2  # to the node's second --enc-root
  ```

  To move roots online, restart the node with both (`--enc-root
  old,new`), re-wrap with `--to-root 2`, and from the next restart name
  the new one alone. `datax debug enc-root` also has `status` (what
  wrapped the key in a data directory; nothing secret is printed),
  `adopt` (put a key-file store's key under a root: node stopped, then
  start with `--enc-root` and rotate, so the key that was in the file
  opens nothing) and `release` (the way back: the store key is written
  to a new file, mode 0600, and the wrapped copy removed).

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
- **The key registry rolled back as a whole is caught, not prevented**:
  an older, validly sealed copy of the registry opens the store
  unauthenticated until the cross-check above refuses it — at open when
  the registry alone was rolled back, on the first heartbeat that reads
  KV when the whole store was. In the second case the store has opened
  and may have written a file or two in the old format before the node
  stops; they are rewritten by the sweep once the store re-completes
  the switch. (A store rolled back wholesale also carries a raft log
  shorter than what it acknowledged; a leader that still holds its
  progress for the store refuses that with a raft panic before the
  cross-check runs, which is a cruder refusal of the same rollback.) A
  rollback of every store in the cluster at once — the record with
  them — is not caught: the cluster's record is only as old as its
  oldest surviving replica, and a single-node cluster is that case by
  definition, its record living on the one store being rolled back.
  Nor is a store rolled back to before encryption was enabled at all,
  with `--enc-key` dropped from its configuration to match: it opens
  plaintext and neither check runs (with the key still configured it
  opens encrypted on a fresh registry, and the cluster's record
  catches it).
- Authenticated files cost 24 bytes per 4 KiB chunk (about 0.6%) and 28
  bytes per WAL or MANIFEST append, a whole chunk decrypted for any read
  inside it (Pebble's block cache holds the plaintext, so a cache hit
  never touches the wrapper), and one chunk re-sealed and rewritten for
  every append to an sstable's tail chunk; a log append writes only its
  own frame.
- The `LOCK` file and directory entries (file names, sizes) are not
  encrypted; file names are Pebble's numeric names and leak no user data.
- **Keys in memory are narrowed, not hidden** (see *Keys in memory*).
  The held copies are locked and out of core dumps; a cipher's expanded
  key, and a key on its way through a stack, are on the ordinary heap,
  and anyone who can read the process's memory can read all of it.
- An HSM or KMS is reachable through an `exec` root's command, not
  spoken to directly (#308).
- Re-encryption covers live sstables; a stale file spanning a single
  user key waits for natural compaction churn.
- The node never rewrites the operator's key file: after an online
  rotation, a restart needs the new key in one of the `--enc-key`
  files (stage it beforehand, see Key rotation).
