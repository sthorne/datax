# Design proposal: a key service, and per-table and per-user data keys

Status: **proposal, not built** (issue #310, the last piece of #292).
The issue asks for a written design to be agreed before any code. This
is that design. Nothing described here exists yet; sections marked
*Decision needed* are the ones the owner should settle first.

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
GRANT USAGE ON ENCRYPTION KEY billing TO app;        -- wrap and unwrap
```

- `wrap` output carries the key name's id and version, so `unwrap`
  needs no version argument and a rotated key keeps opening old blobs.
  The context argument is bound as associated data: a blob wrapped for
  one tenant does not unwrap under another's context.
- Every use is audited (who, key, version, operation, outcome), never
  the material. Wrap and unwrap are rate-limited per role.
- *Decision needed:* whether `unwrap` returns key material to the
  client. The proposal is **yes** — that is what a KMS's Decrypt does
  and what envelope encryption needs — with the consequence stated: a
  role with USAGE can obtain every data key it has ever wrapped. The
  alternative (datax performs the encryption of the application's
  payloads itself, `datax_encrypt(key, plaintext)`) keeps keys inside
  but moves bulk data through SQL; it can be offered as well for small
  values.
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
- **Rotation** of a table's or tenant's key creates a new version; new
  writes use it at once and reads accept both. A background, resumable
  job rewrites rows still under the old version in bounded batches
  (ordinary transactions, the way the row-TTL job deletes), after which
  the old version is destroyed. MVCC history written under the old
  version stays readable until it is garbage-collected; destroying the
  old version early is allowed and makes that history unreadable.
- **Time-series tables** use a different storage layout (columnar
  chunks). Sealing would apply per chunk; proposed as out of scope for
  the first version, refused at `CREATE TABLE` with a clear message.
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

Point 2 is the one that matters. Three positions the owner can choose
between (*Decision needed*):

- **Documented dependency.** Erasure holds once backups older than the
  destruction have expired. Simple; the retention period of backups
  becomes the erasure delay, and that is stated wherever the feature is.
- **Keys kept out of backups.** `datax_keys` material for `tenant` keys
  is excluded from backups and kept only in the live cluster and in a
  separate, short-retention key export. A restore then recovers only
  tenants whose keys still exist — which is the desired property — at
  the cost of a second artifact to manage.
- **A per-key external root** for callers who need erasure that does
  not depend on backup hygiene at all: the named key's material is
  additionally wrapped by a key in the tenant's own KMS (the KMS roots
  of #308). Destroying or disabling that KMS key makes every copy of
  the datax key, in every backup, useless. datax cannot verify the
  destruction; it reports what it was told.

The proposal is to ship the first with the second as an option, and the
third for `tenant` keys when #308's roots are in.

What "destroyed" means under each root for the **cluster** key (whose
compromise undoes everything below it): under shares, the root key is
recoverable by a quorum for as long as the shares exist; under a KMS,
for as long as the KMS key does; under `file`/`exec`, for as long as
the operator's copy does. Rotating the cluster key and destroying the
old version bounds the exposure of old backups to the old root.

## Threat model

Protects against: a stolen disk or backup (as today); a reader of one
table's files learning another table's or tenant's values after that
key is destroyed; an operator restoring an old backup to recover a
shredded tenant (under the second or third erasure option).

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
4. B per tenant, with destruction and the erasure option chosen above.
5. Online rotation and enabling sealing on an existing table.

## Decisions needed before code

1. May `datax_key_unwrap` return key material to a client? (Proposed:
   yes, with USAGE, audited.)
2. Which erasure position for backups? (Proposed: documented
   dependency by default, keys-out-of-backups as an option, per-key
   external root later.)
3. Is the primary-key/index plaintext limit acceptable for the
   requesting application, or does it need opaque identifiers first?
4. Is requiring `--enc-root` on every node (no `--enc-key` files)
   acceptable for clusters that enable this?
5. Time-series tables out of scope for the first version?
