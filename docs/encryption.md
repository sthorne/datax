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

The two simplest roots:

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
- A wrapped store key can be bound to the cluster and node it was
  wrapped for, and is then refused as any other node's. The KMS roots
  bind it themselves; for the others a rewrap does (see *A root in a
  key management service*).
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
A root key split among operators is the next section, the handing of
it between running nodes the one after, and a root that is a key
management service spoken to directly — Vault, AWS KMS, Google Cloud
KMS, Azure Key Vault — the one after that.

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
- that node is **not live**: the last heartbeat the answering node
  knows of is older than the liveness grace (15 s by default), so a
  second copy of a running node is handed nothing. A node restarted at
  once therefore waits, asking every two seconds — for the grace less
  however stale the answering node's knowledge was, which is up to two
  heartbeat periods (six seconds);
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

### A root in a key management service

Four roots are a key management service spoken to directly (#308).
Wrapping and unwrapping the store key are calls to the service; the
service's key never reaches datax, and what the data directory holds is
the service's ciphertext of the store key.

| root | spec |
|---|---|
| HashiCorp Vault, Transit engine | `vault:<address>/<mount>/<key>` |
| AWS KMS | `awskms:<key arn>` |
| Google Cloud KMS | `gcpkms:projects/<p>/locations/<l>/keyRings/<r>/cryptoKeys/<k>` |
| Azure Key Vault | `azurekv:<vault>/<key>[/<version>]` |

```sh
datax start --dir data1 --enc-root 'vault:https://vault.example:8200/transit/datax?token_file=/run/vault/token' ...
```

They are written against each service's REST API with Go's standard
library; datax links no cloud SDK. **They are tested against fakes of
each API (`pkg/storage/enc/kmstest`), and have not been run against the
real services.** Try a root against your service — a node started on an
empty directory wraps a key, unwraps it and compares, and refuses to
start if any of that fails — before relying on it. The `exec` root
reaches the same services through their own command-line tools, and
remains the choice where you would rather trust the vendor's client.

**Options** ride on the spec the way a URL's query does, because
`--enc-root` separates several roots with commas:

```
<kind>:<key>?name=value&name=value
```

A value that needs a comma, `&` or `?` writes it percent-encoded
(`%2C`, `%26`, `%3F`). An option nobody recognises is an error. **No
option carries a secret.** A credential is a file an option names, or
comes from where the service's own tools look — the environment, the
instance's identity — so a spec can be put in a unit file, logged and
shown. Every KMS root takes:

| option | meaning | default |
|---|---|---|
| `timeout` | how long one request may take | `10s` |
| `retries` | how many times a request that failed transiently is tried again | `3` |
| `ca` | PEM file of the certificate authorities to trust, in place of the system's | — |

Requests go through the proxy the environment names (`HTTPS_PROXY`,
`NO_PROXY`), except those to an instance's metadata service. Plain
HTTP is accepted only to the machine itself (a local agent): every
request carries a credential. Redirects are not followed.

**What a node does when the service does not answer.** Each request has
the timeout above. A failure that may pass — no connection, no answer
in time, HTTP 429, any 5xx, and AWS's throttling errors — is tried
again, `retries` times, after a pause that doubles from 200 ms (with
jitter, capped at 5 s). A refusal — any other 4xx: denied, no such key,
a ciphertext the key did not make — is not tried again; the one
exception is a 401 from Google or Azure, where the cached token is
dropped and the call made again with a fresh one. Then:

- **A node cannot start while its KMS is unreachable**, and says which
  service, which key and why, in the service's own words:

  ```
  unwrapping the store key: the vault root could not unwrap the store key: Vault Transit key transit/datax at
  https://vault.example:8200: decrypt failed after 4 attempts: could not reach vault.example:8200: dial tcp
  10.0.0.9:8200: connect: connection refused
  ```

  With the defaults a start gives up after at most about 45 seconds.
- **A running node is unaffected.** It unwrapped its key at start and
  holds it; it does not ask again until a rotation or a rewrap.

**Credentials are yours to place**, and datax keeps them nowhere: not
in the cluster, not in the data directory. No token, secret, signature
or signed assertion appears in a log line, an error, an event or
`/api/security`; a root strikes whatever it has sent from any message
it produces, including a service's own error text.

**Binding to the cluster and node.** A KMS is reachable by every node
holding a credential for it, so a wrapped store key copied from one
node's directory would unwrap for another as readily. Each wrapped key
is therefore bound to its cluster id and node id, in three places:

1. *In the call to the service*, as its encryption context or
   associated data (below, per service). The service refuses to unwrap
   under any other, records it in its audit log, and — AWS — lets a key
   policy condition on it.
2. *Inside what the service encrypts*: datax wraps a digest of the
   binding together with the key, and checks it after every unwrap. This
   holds for a service that has no notion of associated data (RSA
   wrapping in Azure Key Vault) and for one that would answer for a
   context it was not asked about.
3. *In the wrapped-key file*, under the entry's tag, for every root
   including `file`, `exec` and `shares`.

What this does and does not give, honestly:

- A store key is first wrapped **before its node has an id**: the id is
  in the store, and the store cannot open without the key. That first
  wrap is bound to nothing. A node on a KMS root binds its key at the
  same start, the moment it has bootstrapped or joined (one more wrap
  and unwrap); if the service fails just then, the node runs with an
  unbound key, says so (a warning, and `"bound": false` in
  `/api/security`), and a rewrap binds it. A key policy that *requires*
  the context therefore refuses a new node's first start: grant the
  unconditioned call for commissioning, or condition only once the
  nodes are bound.
- An unbound entry is accepted, from any node. It has to be: that is
  what a store from before bindings, and a new node, has.
- At a restart the node does not yet know who it is, so it presents the
  binding its own wrapped-key file records, and compares that with the
  store's identity once the store is open — refusing to run on a
  mismatch. The binding is not a secret. What stops a holder of node
  B's credential from unwrapping node A's key by presenting A's context
  is the service's policy, where the service can condition on it; where
  it cannot (Vault without per-node tokens, Google, Azure), the binding
  catches a wrapped key *misplaced* and is recorded in the audit log,
  and it is separate credentials per node that keep one node from
  another's key.
- For `file`, `exec` and `shares` the binding is checked by datax
  alone, and is written when you rewrap
  (`datax debug enc-root rewrap`); a store's first entry under these
  roots is unbound until then. The offline tools keep whatever binding
  an entry has.

**Health.** A running node does not need its root, so one that has
become unreachable would otherwise be found at the next restart. Every
minute a node on a KMS root has the service wrap and unwrap a
throwaway 32-byte value — never the store key — which is exactly what
a restart needs to work and requires no permission beyond the two a
node already has. The outcome is:

- in `/api/security`, per store: `root.healthy`, `root.health_error`
  (the service's reason), `root.health_checked_at`,
  `root.last_healthy_at`;
- in the console's security view, said only when the root is not
  usable;
- a warning in the health document (`enc-root-unreachable`), and an
  event when the root goes and when it returns;
- the gauges `datax_enc_root_healthy` (1 or 0) and
  `datax_enc_root_last_healthy_timestamp_seconds`. Alert on the first:
  a node at 0 is serving, and would not come back from a restart.

A check costs two requests to the service a minute per node, which the
service bills and logs.

**The service's own key rotation.** The key id a node reports carries
the service's key version where the service names one
(`transit/datax@v3`). After the service rotates its key, everything
already wrapped still unwraps; the health check notices the new version
(`root.latest_key_id`, and a line in the console), and a rewrap moves
the node's store key onto it, after which the old version can be
retired in the service:

```sh
datax debug enc-root rewrap --addr 10.0.0.1:26257 --certs-dir certs   # per node
datax debug enc-root status --dir data1                                # stopped node: root, key id, binding
```

AWS KMS rotates a key's material behind one key id and names no
version, so there is nothing to see move: a rewrap re-encrypts under
the current material, and the key id stays the ARN.

#### HashiCorp Vault (Transit)

```
vault:https://vault.example:8200/transit/datax?token_file=/run/vault/token
```

The address is the scheme and host; the last path element is the key
and what lies between is the mount. (A Vault served under a path prefix
is not supported.) Calls: `POST /v1/<mount>/encrypt/<key>` and
`/decrypt/<key>`. Redirects are not followed, so name the active node,
a load balancer, or a cluster whose standbys forward requests (Vault's
default) — a standby that answers with a redirect is a refusal.

| option | environment | meaning |
|---|---|---|
| `token_file` | `VAULT_TOKEN_FILE` | file holding the token; **read on every call**, so a token Vault Agent renews is picked up |
| — | `VAULT_TOKEN`, then `~/.vault-token` | the token, when no file is named |
| `namespace` | `VAULT_NAMESPACE` | Vault Enterprise namespace |
| `ca` | `VAULT_CACERT` | CA that signed Vault's certificate |
| `derived` | — | `true` for a key created with `derived=true` |

The binding is sent as `associated_data`, which Vault authenticates for
its AEAD key types (`aes256-gcm96`, `chacha20-poly1305`); with
`derived=true` it is sent as the key-derivation `context` instead,
which a derived key requires. A Vault too old to know
`associated_data` ignores it, and the binding then rests on the digest
inside the ciphertext. The key version is the `v<N>` of Vault's
ciphertext. Minimum policy:

```hcl
path "transit/encrypt/datax" { capabilities = ["update"] }
path "transit/decrypt/datax" { capabilities = ["update"] }
```

Nothing else: not `transit/keys/*` (the node never reads, creates or
rotates the key) and not `rewrap`.

#### AWS KMS

```
awskms:arn:aws:kms:eu-west-1:111122223333:key/1234abcd-12ab-34cd-56ef-1234567890ab
```

The region is the ARN's. A symmetric key (`SYMMETRIC_DEFAULT`). Calls:
`Encrypt` and `Decrypt` (the JSON protocol), signed with Signature
Version 4; `Decrypt` names the key, so a ciphertext made under some
other key the credential can use is refused. Credentials are looked for
in this order:

1. `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY` (and
   `AWS_SESSION_TOKEN`);
2. `AWS_WEB_IDENTITY_TOKEN_FILE` with `AWS_ROLE_ARN` — the role is
   assumed through STS (EKS service accounts);
3. the shared credentials file (`AWS_SHARED_CREDENTIALS_FILE`, or
   `~/.aws/credentials`), profile `AWS_PROFILE` or `default`;
4. the container credentials endpoint
   (`AWS_CONTAINER_CREDENTIALS_RELATIVE_URI` or `_FULL_URI`: ECS, EKS
   Pod Identity);
5. the instance's role, from the instance metadata service — version 2
   only; version 1 is never fallen back to.

Temporary credentials are cached until two minutes before they expire.
Not supported: SSO, `credential_process`, and roles assumed through a
profile in `~/.aws/config`; use the `exec` root, or run something that
writes the shared credentials file.

| option | environment | meaning |
|---|---|---|
| `profile` | `AWS_PROFILE` | profile of the shared credentials file |
| `credentials_file` | `AWS_SHARED_CREDENTIALS_FILE` | the shared credentials file |
| `endpoint` | — | the KMS endpoint (a VPC or FIPS endpoint) |
| `sts_endpoint` | — | the STS endpoint, for web identity |
| `imds` | `AWS_EC2_METADATA_SERVICE_ENDPOINT` | the instance metadata endpoint |
| `ca` | `AWS_CA_BUNDLE` | CA bundle |

The binding is the encryption context
`{"datax-cluster": "<cluster id>", "datax-node": "<node id>"}`, which
appears in CloudTrail with every call. Minimum policy for a node's
role:

```json
{
  "Version": "2012-10-17",
  "Statement": [{
    "Effect": "Allow",
    "Action": ["kms:Encrypt", "kms:Decrypt"],
    "Resource": "arn:aws:kms:eu-west-1:111122223333:key/1234abcd-12ab-34cd-56ef-1234567890ab"
  }]
}
```

To hold a role to one cluster, add
`"Condition": {"StringEquals": {"kms:EncryptionContext:datax-cluster": "<cluster id>"}}`
(and `datax-node` for one role per node) — after the nodes have
started once, for the reason given under *Binding* above.

#### Google Cloud KMS

```
gcpkms:projects/acme/locations/europe-west1/keyRings/datax/cryptoKeys/store-root
```

The key's resource name, without a version; a symmetric
(`ENCRYPT_DECRYPT`) key. Calls: `cryptoKeys.encrypt` and
`cryptoKeys.decrypt`. The access token comes from a service-account
key file when one is named — a JWT signed with the account's key
(RS256) is exchanged at the file's `token_uri` — and otherwise from
the metadata server, as the instance's or the pod's service account
(Workload Identity on GKE). Tokens are cached until two minutes before
they expire. Not supported: workload identity federation
(`external_account` files), user credentials and impersonation.

| option | environment | meaning |
|---|---|---|
| `credentials` | `GOOGLE_APPLICATION_CREDENTIALS` | a service-account key file |
| `endpoint` | — | the KMS endpoint |
| `metadata` | `GCE_METADATA_HOST` | the metadata server |

The binding is the request's `additionalAuthenticatedData`. The key
version is the one `encrypt` reports (`…/cryptoKeys/store-root@3`).
Minimum IAM, on the key and nothing wider:
`roles/cloudkms.cryptoKeyEncrypterDecrypter` — that is,
`cloudkms.cryptoKeyVersions.useToEncrypt` and
`cloudkms.cryptoKeyVersions.useToDecrypt`.

#### Azure Key Vault

```
azurekv:datax-prod/store-root
azurekv:datax-prod/store-root/0123456789abcdef0123456789abcdef
```

The vault by name (`https://<name>.vault.azure.net`) or, for another
cloud, by full host name; an **RSA** key (2048 bits or more). With a
version in the spec the root wraps under that version always; without,
under the key's current one. Calls: `wrapKey` and `unwrapKey` with
`RSA-OAEP-256`; an unwrap names the version that wrapped, which the
wrapped key records. The access token comes from the client-secret
variables when all three are set (`AZURE_TENANT_ID`, `AZURE_CLIENT_ID`,
`AZURE_CLIENT_SECRET`) and otherwise from the instance's managed
identity. Not supported: workload identity federation, certificate
credentials, and Managed HSM (its token audience differs).

| option | environment | meaning |
|---|---|---|
| `client_id` | `AZURE_CLIENT_ID` | the user-assigned managed identity to use |
| `endpoint` | — | the vault's URL, in place of the one its name gives |
| `login` | `AZURE_AUTHORITY_HOST` | the token authority |
| `imds` | — | the instance metadata endpoint |

**RSA wrapping has no associated data**: Key Vault is told nothing of
the binding, cannot enforce it and gives a policy nothing to condition
on. The binding rests on the digest inside what is wrapped, which datax
checks after every unwrap, and on the wrapped-key file's tag. That
catches a wrapped key moved between nodes; it does not stop a holder of
any credential for the key from unwrapping any node's blob with their
own tool. Minimum access, on the key: the data actions
`Microsoft.KeyVault/vaults/keys/wrap/action` and
`…/unwrap/action` (the built-in role *Key Vault Crypto Service
Encryption User* is those two and reading the key's metadata), or, with
access policies, the key permissions `wrapKey` and `unwrapKey`.

#### HSMs and PKCS#11

datax does not speak PKCS#11: it would need cgo and a vendor's
library in the process, and the project builds without either. An HSM
is reached through the `exec` root — a small command around the
vendor's tool or `pkcs11-tool` — or through a KMS that fronts it
(Vault's seal or managed keys, AWS CloudHSM custom key stores, Cloud
HSM protection levels), which the roots above use without knowing.

## Cluster key

The cluster key (#310) is the first piece of the key service designed in
[design-key-service.md](design-key-service.md): one 32-byte key for the
whole cluster, versioned, that wraps the **named keys** kept in the
`datax_keys` system table. Nothing user-visible encrypts data with it
yet — the key service's SQL and sealed tables are later steps — but the
key, its distribution and its table are in place.

```
root (per node)  wraps →  cluster key (one per cluster)  wraps →  named keys (datax_keys rows)
```

Each node keeps the cluster key **wrapped by its own root**, in
`CLUSTER-KEY` beside `ENCRYPTION-STOREKEY`, and unwrapped only in held
memory (see *Keys in memory*) while it runs. The file is not secret and
cannot be edited: each entry is tagged under a key derived from the
cluster key it wraps, and bound to the cluster and node it was wrapped
for, as a wrapped store key is. The cluster's replicated record holds the
key's id, its current version and a check value
(HMAC-SHA256 under the key) — never the key — so a node can tell whether
the copy it holds is the right one. The key is never logged, never in an
event, and never in an API response except sealed for the CLI's own
exchange during an export.

### Enabling it

```sh
datax debug cluster-key init   --addr 10.0.0.1:26257 --certs-dir certs   # admin's client certificate
datax debug cluster-key status --addr 10.0.0.1:26257 --certs-dir certs
```

`init` is refused, saying why, unless:

- the cluster is at v22, finalized;
- the cluster is secure (the key moves between nodes over mutual TLS with
  the node certificate, which an insecure cluster does not have);
- every member that is not being decommissioned is **live** — a node that
  is down cannot be given the key; start it, or decommission it;
- every such member keeps its store key **under a root** (`--enc-root`).
  A node with a key file (`--enc-key`) is named, with the remedy: move it
  under a root with `datax debug enc-root adopt` (node stopped). A
  plaintext node is named too; it cannot be moved under a root in place.

The node `init` is sent to makes the key, keeps its own wrapped copy, and
records the operation as in progress. It then hands the key to each
member: it asks the member, at its registered address, for an exchange
key, and sends the cluster key sealed for that one exchange (X25519, the
construction peer unseal uses, under its own label). The member keeps the
key only if its check value matches what the replicated record of the
operation names — a peer cannot give it a key of its own choosing —
writes its own wrapped copy durably, and only then acknowledges. When
every member has acknowledged, the cluster key is recorded, the operation
cleared, and `datax_keys` created. **Named keys can be created only once
the key is recorded.**

If a member cannot be reached, `init` reports which and stops; the key is
recorded nowhere and nothing is under it. **Run `init` again**: from any
node that already holds the key it hands out the same key to the rest. Run
from a node that does not hold it (say, the original node has since lost
its disk), it makes a replacement under a new id and hands that out
instead; the abandoned key never wrapped anything, and nodes drop it once
the replacement is recorded.

### Joining and restarting nodes

A node that restarts with its `CLUSTER-KEY` intact unwraps its copy
through its root at start, as it unwraps its store key (a sealed node,
once its root is unsealed). A node that lacks a version the record names
— it joined after the key was made, its copy is gone, or its root cannot
open it — asks its running peers for it every few seconds (a
**hand-over**), and a peer hands it over when all of these hold:

- the asker presents the cluster's **node** certificate over mutual TLS;
- it names this cluster, is a member, and is not being decommissioned;
- it does not report its store key outside a root;
- the **liveness rule**: a node that is not live is answered, as under
  peer unseal. A node that asks is running, so it is live by the time it
  asks; it is answered only if its connection comes from the host the
  cluster has it registered at. That is the running node itself. A second
  copy of a running node elsewhere — someone holding a copy of its disk
  and its node certificate — is answered nothing until the node it claims
  to be has been silent for the liveness grace, which is peer unseal's
  rule and reason.

The key crosses sealed for the one exchange, the asker keeps it only if
its check value matches the replicated record, and writes its own wrapped
copy. Every request is audited on the node that answers, granted or
refused with the reason, appears in the console's events, and is counted
(`datax_cluster_key_handovers_total{outcome}`, and the security view's
per-node counts). What a node received is counted as
`datax_cluster_key_received_total{via}`.

**Hand-over is not governed by the peer-unseal setting.** That setting
decides whether a node's certificate alone may obtain the root key that
opens a node's *disk*, and is for operators who keep the node key on the
data disk. The cluster key opens no disk; a node that holds the node
certificate can already read the cluster's data over the internode KV
interface, which is why a node that lacks the cluster key cannot serve
the tables later steps will seal, and why the checks above, not a
setting, govern it. A node that a hand-over refuses (it is behind an
address translation, so its connections do not come from its registered
host) can be given the key by an operator:

```sh
datax debug cluster-key sync --addr 10.0.0.1:26257 --certs-dir certs
```

pushes the key to every live member that does not report holding it, the
way `init` does.

### Rotating it

```sh
datax debug cluster-key rotate --addr 10.0.0.1:26257 --certs-dir certs
```

makes a new version and hands it to every member under the same
conditions as `init` (every member live and under a root), records it as
current while the old version stays accepted, rewraps every `datax_keys`
row under it (a few kilobytes per thousand keys, in small transactions),
and then destroys the old version: it leaves the record and every node
drops its copy from memory and from `CLUSTER-KEY`. No user data is
rewritten. A rotation stopped anywhere — a node unreachable, the serving
node restarted — is finished by running `rotate` again: before the new
version is recorded it hands out the same new version; after, it
finishes the rewrap and the retirement. A key written under the old
version while the rewrap runs is caught: the retirement and the check that
no row is still under the old version are one transaction.

Rotating the cluster key is also how the exposure of old backups to a
compromised root is bounded: the old version, once destroyed, is gone from
every node (it remains, wrapped, in backups' `datax_keys` rows and in any
export taken before).

### Export, import and restore

A backup carries `datax_keys` as it is stored: each named key sealed by
the cluster key. Restoring it into **another** cluster needs that
cluster key, so it is exported, wrapped under a root the operator
supplies, and imported on the other side:

```sh
# on the source cluster
datax debug cluster-key export --addr 10.0.0.1:26257 --certs-dir certs \
  --to-root exec:/usr/local/bin/datax-wrap --out cluster-key.export
# on the target cluster: its own key first, then the source's
datax debug cluster-key init   --addr 10.1.0.1:26257 --certs-dir certs
datax debug cluster-key import --addr 10.1.0.1:26257 --certs-dir certs \
  --from-root exec:/usr/local/bin/datax-wrap --in cluster-key.export
datax restore ...
```

- The export root is any root spec — `file:`, `exec:` or a KMS (not
  `shares`) — and it is used **by the command, on the operator's machine**:
  no request names a root to a node. The node hands the CLI the cluster's
  keys (every version it still accepts, and keys it imported) sealed for
  the CLI's own exchange, over mutual TLS, to an admin; the CLI wraps them
  under the root and writes the file (mode 0600, never over an existing
  file). The export is audited on the node and by the CLI.
- `import` unwraps the file through the same root on the operator's
  machine and sends the keys over mutual TLS (as a store key travels for
  `rotate-enc-key`), to an admin; the target hands them to every member as
  `init` does and records them as imported. It is refused before the
  target's own cluster key is enabled — imported keys exist to be
  rewrapped from — and for a key that is the target's own.
- A **whole-cluster restore** of a backup holding named keys checks,
  before applying anything, that this cluster holds every cluster key
  they are sealed under, and refuses naming the missing ones otherwise.
  After the data is in, the restored keys are rewrapped under the target's
  current key and the summary says how many. (The `datax_keys` checksum in
  the restore summary therefore differs from the backup's.)
- Imported keys stay held by every node of the target (and are carried
  through its rotations unchanged), so that further backups of the source
  can be restored; nothing drops them in this version. A restore of tables (`--table`,
  `--database`) does not bring `datax_keys` back, as it brings no system
  table back.

### Failure modes

- **A member is down or unreachable**: `init`, `rotate` and `import`
  refuse, or stop and say so; run them again once it is back (or
  decommission it). Nothing half-applied is used.
- **A node loses `CLUSTER-KEY`** (or its root can no longer open it): it
  asks a peer and is handed the key; until then it says in
  `/api/security` that it does not hold it, and later steps' sealed tables
  will not be served from it.
- **Every node loses its copy at once**, or every node's root is lost:
  the cluster key is gone, and with it every named key. There is
  deliberately no escrow; an export (above) is the only copy outside the
  nodes, and an operator who wants one keeps it.
- **A node is moved off `--enc-root`** after the key is enabled: it
  cannot keep a copy, says so in its log, and is not handed the key.

### What it protects, and what it does not

- The cluster key is protected as each node's store key is: by that
  node's root. A stolen disk, or a backup, holds it only wrapped.
- Named keys in `datax_keys` are sealed under the cluster key with their
  id, name, version and kind bound in: a row's material does not open
  under another key's identity, and the wrapped bytes are useless without
  the cluster key. **No SQL session can read `datax_keys`**, root and
  admins included: a session that names it is shown a virtual table of
  the same name with the keys' metadata (id, name, kind, state, versions,
  generation, the cluster key version they are under, timestamps) and no
  material column, to admins only; every other path to the real table is
  refused.
- It does not protect against a running node's memory (see *Keys in
  memory*), nor against someone who holds a node certificate **and** can
  reach the cluster while the node it claims to be is not live (the same
  limit as peer unseal); and an export is exactly as safe as the root it
  is wrapped under.

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

## Backups

A backup (`datax backup`) is written outside the store, and the store
key has no part in it: without `--backup-key` its files are plaintext,
which is why an encrypted store refuses to write one unless told
`--allow-plaintext`. With `--backup-key <file>` the backup is sealed
under a key the operator supplies for it, in the same construction as
authenticated store files with the gaps a file that travels cannot
afford closed (docs/user/operations.md, *Sealed backups*, has the
operator's view):

- **Keys.** Each backup has a random 32-byte data key. `BACKUP.json` is
  an envelope: the data key sealed under the backup key (AES-256-GCM,
  magic `DXBW`), and the manifest sealed under the data key. Each file's
  key is HKDF-SHA256(data key, the file's random salt, a label and the
  file's name). The backup key therefore encrypts 32 bytes per backup
  and is never used on bulk data.
- **Files.** A sealed file is a `DXE2` stream: the 32-byte `DXE2` header
  (flags *bound* and *stream*, data-key id 0, 64 KiB chunks), then
  chunks of `nonce ‖ ciphertext ‖ tag`. Every tag covers the header, the
  file's name and a marker saying whether the chunk is the last; the
  chunk's index is in its nonce. So, unlike a store's sstable, a sealed
  backup file cut exactly between chunks is refused: the chunk it now
  ends on was not sealed as a last one.
- **What is refused.** A changed byte in any file; a file truncated
  anywhere, extended, or with chunks reordered; a file under another
  file's name; a file or the manifest from a different backup (its data
  key differs) — each before restore applies anything, because restore
  reads every file to its end first. A sealed backup without its key,
  and a plaintext backup presented with a key, are refused as well.
- **What is not protected.** The number of tables, their numeric ids
  (file names) and file sizes are visible. A whole sealed backup
  replaced by an **older sealed backup under the same key** opens: it is
  a genuine backup, and nothing tells restore which one was meant —
  compare the summary's cluster id, tables and checksums with what you
  recorded when the backup was taken. A chain is checked for continuity
  (each incremental's base timestamp against its predecessor's end), not
  for being the newest. Someone who holds the key can write any backup;
  the checks restore makes of every backup (file names, key spans)
  apply to sealed ones too, but a key holder can restore rows of their
  choosing, exactly as someone who can write a plaintext backup can.
- **The key in transit.** The key is sent to the serving node in the
  admin request: over mutual TLS on a secure cluster, and on an insecure
  one accepted only from the node's own machine. It is not logged,
  audited, or kept.

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
- **Backups are sealed only when asked** (`datax backup --backup-key`);
  otherwise they are plaintext whatever the store is. A sealed backup
  shows its table count, table ids and file sizes; a rollback to an
  older backup sealed under the same key is not detected; and a backup
  key is a key file only — it cannot yet be held by an `--enc-root`
  root (a KMS command or shares).
- **Keys in memory are narrowed, not hidden** (see *Keys in memory*).
  The held copies are locked and out of core dumps; a cipher's expanded
  key, and a key on its way through a stack, are on the ordinary heap,
  and anyone who can read the process's memory can read all of it.
- **The KMS roots have been tested against fakes of each service's
  API, not against the services.** Their request signing is held to
  AWS's published examples and their behaviour to what each API
  documents; the first start against a real service is the test that
  counts, and it fails closed.
- **A wrapped store key's binding to its node is enforced by the
  service only where the service's policy can condition on it** (AWS
  KMS encryption context). Elsewhere it catches a misplaced key and is
  audited; Azure Key Vault's RSA wrapping carries none at the service
  at all. A new node's first wrap is unbound until the node has an id.
- An HSM is reachable through an `exec` root's command or a KMS that
  fronts it; PKCS#11 is not spoken directly.
- Re-encryption covers live sstables; a stale file spanning a single
  user key waits for natural compaction churn.
- **The cluster key needs every node under `--enc-root`, a secure
  cluster, and every member live** to enable, rotate or import it. A node
  whose connections do not come from its registered host (behind an
  address translation) is handed the key only once its last heartbeat is
  older than the liveness grace, or by `datax debug cluster-key sync`.
- The node never rewrites the operator's key file: after an online
  rotation, a restart needs the new key in one of the `--enc-key`
  files (stage it beforehand, see Key rotation).
