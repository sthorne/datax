# Design proposal: a key service, and per-table and per-user data keys

Status: **agreed; step 1 built** (issue #310, the last piece of #292).
The issue asked for a written design to be agreed before any code. This
is that design; the owner settled its five open decisions on
2026-11-15, recorded under *Decisions* at the end. Step 1 of the order
of work — the cluster key, `datax_keys`, hand-over, export and import —
is built (docs/encryption.md, "Cluster key"); the choices it made where
this design was open are under *Choices made in step 1*. Nothing else
described here exists yet.

What an application should expect from this design — what is
encrypted, what is not, how to shape a schema for it, and how erasure
works — is written for application developers in
[encryption-keys-for-applications.md](encryption-keys-for-applications.md).

## What is being asked for

Two capabilities that share one key hierarchy:

- **A. A key service for applications.** An application creates a named
  key in datax and asks datax to wrap and unwrap its own data keys under
  it, instead of running a separate KMS.
- **B. Per-table or per-user data keys.** datax encrypts row values
  under a key that belongs to one table, or to one user's rows in a
  shared table. Destroying the key makes those rows unreadable on every
  replica and in every backup that does not hold the key
  (crypto-shredding).

## What exists to build on

- Encryption at rest is per node and per file (docs/encryption.md). A
  node's files mix every table's rows, so there is no file to give a
  table's key to: B cannot be done at the file layer.
- A node's store key can live under a root (`--enc-root`): a file, a
  command, a KMS, or a root key split among operators with peer unseal.
  Roots wrap small blobs; that is all the interface asks of them.
- Row values are encoded in `pkg/sql/rowenc` (`EncodeValue` /
  `DecodeValue`) and written to KV as they are. Primary-key and indexed
  columns are encoded into KV keys.
- Gateways cache table descriptors under a lease of 10 seconds.
- Backups can be sealed under a backup key (#291).

## The hierarchy

```
root (per node; exists today)
  └─ wraps → cluster key          one per cluster, 32 bytes, versioned
               └─ wraps → named keys     rows of a system table
                            └─ wrap → data keys   (A: the application's; B: a table's or a user's)
```

- **Cluster key.** Made once, when the feature is first enabled. Each
  node holds it wrapped by its own root, in its data directory beside
  the wrapped store key, and unwrapped in held memory (`enc.HoldKey`)
  while it runs. A node that joins, or restarts sealed, gets it the way
  it gets the root key under peer unseal: from a running peer, over
  mutual TLS, under the same membership and liveness checks. A cluster
  whose nodes use `--enc-key` files has no root to wrap under; the
  feature requires `--enc-root` on every node, and says so when enabled.
- **Named keys** live in a system table, `datax_keys`: name, kind
  (`wrapping` for A, `table`, `tenant`), version, state (`enabled`,
  `disabled`, `destroyed`), the key material wrapped by the cluster key
  (AES-256-GCM, the name and version as associated data), created and
  rotated timestamps, and an optional *external root* (see Erasure).
  The plaintext column is never selectable: the table is readable only
  through views that omit it, and the wrapped bytes are useless without
  the cluster key.
- **Rotation of the cluster key** rewraps the rows of `datax_keys` (a
  few kilobytes per thousand keys) and each node's wrapped copy; no user
  data is rewritten.

## A. The key service

SQL, so that it is reachable from every client and rides the existing
privilege and audit machinery:

```sql
CREATE ENCRYPTION KEY billing;                       -- kind: wrapping
SELECT datax_key_wrap('billing', $1::bytea, $2::bytea);    -- data key, context → wrapped
SELECT datax_key_unwrap('billing', $1::bytea, $2::bytea);  -- wrapped, context → data key
ALTER ENCRYPTION KEY billing ROTATE;                 -- new version; old versions still unwrap
ALTER ENCRYPTION KEY billing DISABLE | ENABLE;
DROP ENCRYPTION KEY billing;                         -- destroys every version
GRANT WRAP ON ENCRYPTION KEY billing TO ingest;      -- wrap only
GRANT UNWRAP ON ENCRYPTION KEY billing TO reader;    -- unwrap only
GRANT USAGE ON ENCRYPTION KEY billing TO app;        -- both: WRAP and UNWRAP
```

- `wrap` output carries the key name's id and version, so `unwrap`
  needs no version argument and a rotated key keeps opening old blobs.
  The context argument is bound as associated data: a blob wrapped for
  one tenant does not unwrap under another's context.
- Every use is audited (who, key, version, operation, outcome), never
  the material. Wrap and unwrap are rate-limited per role.
- **`unwrap` returns key material to the client** (decided). That is
  what a KMS's Decrypt does and what envelope encryption needs. The
  consequence, stated wherever the feature is: a role that may unwrap
  under a key can obtain every data key ever wrapped under it.
- **Wrap and unwrap are separate privileges** (decided). `WRAP` and
  `UNWRAP` can be granted apart, and `USAGE` means both. A service that
  only writes — an ingest worker — holds `WRAP` and can never read a
  data key back, so the roles that can decrypt are a short list.
- `datax_encrypt(key, plaintext, context)` and `datax_decrypt` are
  offered as well, for small values (tokens, secrets in a column): the
  key never leaves datax, at the price of the value travelling through
  SQL. They need `WRAP` and `UNWRAP` respectively. Values are capped
  (64 KiB) so that bulk data does not move this way.
- Wrapped blobs travel over the SQL connection. On a cluster without TLS
  the functions are refused except over loopback, like every other
  path that carries key material.

## B. Per-table and per-user data keys

```sql
CREATE TABLE notes (...) WITH (encryption_key = 'auto');           -- one key for the table
CREATE TABLE items (tenant_id UUID, ..., PRIMARY KEY (tenant_id, id))
  WITH (encryption_key_by = 'tenant_id');                           -- one key per tenant_id value
SELECT datax_destroy_tenant_key('items', '4f6c…');                  -- crypto-shred one tenant
```

- **What is encrypted.** The row's value — the output of
  `rowenc.EncodeValue` — sealed with AES-256-GCM under the row's data
  key before it is written to KV, with the table id, the key's id and
  version, and the row's KV key as associated data (a value cannot be
  moved to another row or table). Secondary-index entries that store
  columns (`STORING`) are sealed the same way.
- **What is not, and this limit is the headline of the feature:**
  primary-key columns and every indexed column. They live in KV keys,
  which must sort and be comparable on every replica, and stay
  protected by file-level encryption only. A tenant id, an item id, a
  timestamp in the key are not shredded with the tenant's key. Row
  counts and value sizes are visible too. The documentation and the
  `CREATE TABLE` notice must say so plainly; an application that needs
  identifiers erased must use opaque ones.
- **Choosing the key.** `encryption_key = 'auto'` makes one named key of
  kind `table`. `encryption_key_by = '<column>'` uses one key of kind
  `tenant` per distinct value of that column, created on first write.
  The column must be the leading primary-key column, so a tenant's rows
  are one key span (which is also what makes its purge and its
  per-tenant restore cheap). This is the answer to "a database per
  user": a shared table, a key per user, no catalog growth.
- **Where it runs.** In the gateway's SQL layer, at the boundary where
  rows become KV values and back. KV, raft, the storage engine,
  replicas, snapshots, the consistency checker and backups all see
  ciphertext and need no change. Constraint checks, statistics and
  `EXPLAIN` run above the boundary on plaintext; table statistics store
  histograms of plaintext values and are therefore **not collected**
  for sealed columns (they would survive the key). Index columns are
  plaintext by definition.
- **Key cache and revocation.** A gateway keeps unwrapped data keys in
  held memory beside its descriptor cache, each under a lease no longer
  than the descriptor lease (10 s). Disabling or destroying a key bumps
  a version that the lease check observes, so every gateway stops using
  it within one lease period; the statement that destroys a key waits
  out that period before reporting success, as schema changes do. The
  bound to document is therefore "within 10 seconds, and before the
  statement returns".
- **A row whose key is destroyed** cannot be decrypted, and a
  statement that reads one fails (`XX001`-class, naming the table)
  rather than skipping it or returning ciphertext. The erasure
  procedure therefore deletes a tenant's rows before destroying its
  key; the application guide sets out the order.
- **Rotation** of a table's or tenant's key creates a new version; new
  writes use it at once and reads accept both. A background, resumable
  job rewrites rows still under the old version in bounded batches
  (ordinary transactions, the way the row-TTL job deletes), after which
  the old version is destroyed. MVCC history written under the old
  version stays readable until it is garbage-collected; destroying the
  old version early is allowed and makes that history unreadable.
- **Time-series tables** use a different storage layout (columnar
  chunks). Sealing would apply per chunk, and a chunk mixes tenants.
  Out of scope for the first version (decided): refused at `CREATE
  TABLE` with a clear message.
- **Change notification, backup and restore.** `LISTEN/NOTIFY`
  payloads are the application's and are not sealed. Backups copy
  ciphertext values as they are; `datax_keys` travels in the backup
  only wrapped by the cluster key. Restoring into another cluster
  therefore needs the source's cluster key, exported wrapped under a
  key the operator supplies (`datax debug cluster-key export`). A
  backup sealed with `--backup-key` is sealed again on top.

## Erasure, stated honestly

Destroying a key erases the data only if no copy of the key survives.
With this design the copies are:

1. the row in `datax_keys` (overwritten with a tombstone, then removed
   from MVCC history by a purge of that row — #295 — so it does not
   wait out the GC TTL);
2. **every earlier backup that contains `datax_keys`**, where the key is
   wrapped by a cluster key that still exists;
3. a node that was down at the time and has not yet applied the change
   (it applies it before serving, since the row is replicated state).

Point 2 is the one that matters. Three positions were considered; the
first is the one decided:

- **Documented dependency (decided).** Erasure holds once backups older than the
  destruction have expired. Simple; the retention period of backups
  becomes the erasure delay, and that is stated wherever the feature is.
- **Keys kept out of backups (not in the first version).** `datax_keys` material for `tenant` keys
  is excluded from backups and kept only in the live cluster and in a
  separate, short-retention key export. A restore then recovers only
  tenants whose keys still exist — which is the desired property — at
  the cost of a second artifact to manage.
- **A per-key external root (not in the first version)** for callers who need erasure that does
  not depend on backup hygiene at all: the named key's material is
  additionally wrapped by a key in the tenant's own KMS (the KMS roots
  of #308). Destroying or disabling that KMS key makes every copy of
  the datax key, in every backup, useless. datax cannot verify the
  destruction; it reports what it was told.

The first version ships the documented dependency only. The second
and third remain possible later without a format change (the
`datax_keys` row already has room for an external root); either would
be its own issue.

What "destroyed" means under each root for the **cluster** key (whose
compromise undoes everything below it): under shares, the root key is
recoverable by a quorum for as long as the shares exist; under a KMS,
for as long as the KMS key does; under `file`/`exec`, for as long as
the operator's copy does. Rotating the cluster key and destroying the
old version bounds the exposure of old backups to the old root.

## Threat model

Protects against: a stolen disk or backup (as today); a reader of one
table's files learning another table's or tenant's values after that
key is destroyed; an operator restoring a backup taken after a
tenant's key was destroyed.

Does not protect, in the first version, against an operator restoring
a backup taken *before* the destruction: that backup still holds the
key, wrapped by a cluster key that still exists. This lasts until such
backups expire (decision 2).

Does not protect against: a compromised gateway while it holds a key in
its cache; a role with SELECT on the table (decryption is transparent
to authorized queries — this is not a substitute for row-level
security, #294); anything in primary keys and indexes; traffic
analysis by row count and size; a holder of the cluster key and an old
backup.

## Upgrade and version gate

One cluster version adds the `datax_keys` table, the cluster key, and
the descriptor fields (`EncryptionKey`, `EncryptionKeyBy`). Before it is
finalized the statements are refused. A sealed value begins with a
marker byte that no `EncodeValue` output begins with, so a table can
have sealing turned on (`ALTER TABLE ... SET (encryption_key = ...)`)
and its existing rows rewritten by the same background job that
rotation uses; reads accept both forms until the job finishes.

## Cost

One AES-GCM operation and 28 bytes per row value, on top of the file
layer's; a cache lookup per row for per-tenant keys. No extra round
trip on the hot path once a key is cached. To be measured against the
`kv` and `bank` workloads before the feature is called done; the
acceptance bar proposed is under 5% on those.

## Suggested order of work

1. Cluster key, `datax_keys`, hand-over to joining and restarting
   nodes, export/import. (Everything else needs it.)
2. A: the key service. Small, and useful by itself.
3. B for whole tables.
4. B per tenant, with destruction (erasure as decided: documented
   dependency on backup retention).
5. Online rotation and enabling sealing on an existing table.

## Decisions

Settled by the owner on 2026-11-15.

1. **`datax_key_unwrap` returns key material to a client** holding
   `UNWRAP` (or `USAGE`), audited and rate-limited. Wrap and unwrap are
   separate privileges.
2. **Erasure and backups: documented dependency.** A destroyed key's
   data is erased from backups once every backup taken before the
   destruction has expired; backup retention is the erasure delay, and
   that is stated wherever the feature is. Keys-out-of-backups and the
   per-key external root are not in the first version.
3. **Primary-key and index columns stay plaintext** (protected by
   file-level encryption only). This is accepted; applications shape
   their schemas for it, as set out in
   [encryption-keys-for-applications.md](encryption-keys-for-applications.md).
4. **The feature requires `--enc-root` on every node.** Key-file
   (`--enc-key`) nodes move first with `datax debug enc-root adopt`;
   enabling the feature on a cluster with any key-file node is refused
   with that pointer.
5. **Time-series tables are out of scope** for the first version.

## Choices made in step 1

Where the design above left a choice open, step 1 made these
(operator-facing detail in docs/encryption.md, "Cluster key"):

- **Enabling is explicit and all-or-nothing.** `datax debug cluster-key
  init` needs v22, a secure cluster, and every member not being
  decommissioned live and under `--enc-root`. One node makes the key and
  pushes it to every member (each keeps its own root-wrapped copy before
  acknowledging); the key is recorded — id, version and a check value
  HMAC-SHA256(key, constant ‖ id ‖ version) — only after every
  acknowledgement, and named keys only after that. A retry finishes the
  same key from any node that holds it.
- **Hand-over and the liveness rule.** A node that lacks the key asks
  while it runs, so peer unseal's "not live" rule cannot apply as it
  stands. The rule kept is its reason — a second copy of a running node
  is handed nothing — by answering a live asker only from the host the
  cluster has it registered at; any other asker waits out the liveness
  grace, as under peer unseal.
- **Hand-over is not governed by the peer-unseal setting.** That setting
  is about a root key that opens a disk; the cluster key opens none, and
  a node without it cannot serve sealed tables. An operator can push the
  key to a refused node with `datax debug cluster-key sync`.
- **`datax_keys` is one row per key**, every version in one column (each
  version sealed separately, the associated data being the key's id,
  name, version and kind), with a unique index on the name; all of a
  key's versions are under one cluster key version, so a rotation of the
  cluster key rewraps a row at a time. Destroyed keys keep their row and
  name. The table is a real (sealed) system table, so backups, restores
  and purges handle it as any table, but no SQL session may read it: a
  session naming it gets a virtual table of the same name with the
  metadata only, for admins.
- **The Go API is `pkg/keystore`**, with a per-key `generation` that
  every state change bumps, for the gateway caches of steps 3 and 4.
- **Export and import keep the operator's root on the operator's
  machine.** The CLI receives the keys sealed for its own exchange and
  wraps them under `--to-root` itself; no request names a root to a node.
  Import is refused before the target's own key is enabled; a
  whole-cluster restore refuses named keys it cannot open before applying
  anything, and rewraps them under the target's key after.
