# Encryption keys for applications: what to expect

**Status: the key service, sealed whole tables and per-tenant keys are
built**, on a cluster at version v22 with its cluster key enabled. The
key service — named keys, `datax_key_wrap` / `datax_key_unwrap`,
`datax_encrypt` / `datax_decrypt` and their privileges — works as
described under [Using the key service](#using-the-key-service); sealed
whole tables (`WITH (encryption_key = 'auto')`) and per-tenant tables
(`WITH (encryption_key_by = '<column>')`, `datax_destroy_tenant_key`)
work as this guide describes (docs/sql.md and docs/encryption.md,
"Sealed tables" and "Per-tenant keys"). Two details the guide leaves
open are settled: `datax_destroy_tenant_key` returns the id of the key it
destroyed, and `NULL` for a tenant that never wrote to the table (so an
erasure can call it for every table); and a row left behind after its
tenant's key is destroyed cannot be deleted on its own — delete first,
as the procedure says. The design and its decisions are in
[design-key-service.md](design-key-service.md) (issue #310).

## In one paragraph

datax encrypts each row's **values** under a key that
belongs to one tenant (or one table), and destroys that key to make the
tenant's values unreadable everywhere: on every replica, and in every
backup once the backups taken before the destruction have expired. What
it **cannot** encrypt this way are the columns that make up the primary
key and the columns of any index — those must stay sortable, so they
stay readable to anyone who can read the database's storage. An
application gets real erasure only if it keeps personal data out of
its primary keys and indexes. Most of this guide is about how to do
that.

## The two features

**Sealed tables (per-tenant or per-table keys).** You declare it on the
table; datax does the rest. Encryption and decryption are transparent
to queries from roles allowed to read the table.

```sql
-- one key for the whole table
CREATE TABLE notes (id UUID PRIMARY KEY, body TEXT)
  WITH (encryption_key = 'auto');

-- one key per tenant: the tenant column must lead the primary key
CREATE TABLE items (
  tenant_id UUID,
  id        UUID,
  title     TEXT,
  details   JSONB,
  PRIMARY KEY (tenant_id, id)
) WITH (encryption_key_by = 'tenant_id');

-- crypto-shred one tenant (see "Erasing a tenant" for the full procedure)
SELECT datax_destroy_tenant_key('items', '4f6c…');
```

A tenant's key is created on its first write. There is no per-tenant
catalog growth: a million tenants are a million small rows in datax's
key table, not a million tables.

**The key service.** For data that does not live in datax — files in
object storage, messages, documents — datax holds named keys and wraps
and unwraps your application's own data keys under them, the way a
cloud KMS does. See [Using the key service](#using-the-key-service).

## What is encrypted, and what is not

| Part of a row | Under the tenant's key? | Erased when the key is destroyed? |
|---|---|---|
| Non-key, non-indexed column values | **Yes** | **Yes** |
| Columns copied into an index with `STORING` | Yes | Yes |
| Primary-key columns (including the tenant column) | **No** | **No** |
| Any column in an index, including `UNIQUE` constraints | **No** | **No** |
| Foreign-key columns, when indexed or part of a key | No | No |
| How many rows a tenant has, and how large each value is | No | No |
| `LISTEN` / `NOTIFY` payloads | No (they are the application's) | No |
| Table statistics | Not collected for sealed columns | Nothing to erase |

"No" does not mean unprotected: everything datax stores is still
encrypted at rest on disk with each node's own keys, as today. It means
that **anyone who can read the database's storage or a backup, and
holds the cluster's keys, can still read it after the tenant's key is
destroyed.** Primary keys and indexes have to be comparable and
sortable on every replica, which is why they are outside the tenant's
key; this is a property of the design, not something a later version
will lift.

## Shaping a schema for it

These rules decide whether erasure means anything for your data.

**1. Use opaque identifiers for every key column.** Tenant ids, user
ids and row ids should be random UUIDs (or similar), never an email
address, a username, a phone number, a customer number someone can
look up, or a name.

```sql
-- Good: nothing in the key says who this is
PRIMARY KEY (tenant_id UUID, id UUID)

-- Bad: the email survives the tenant's key
PRIMARY KEY (email TEXT)
```

**2. Lead the primary key with the tenant column** on per-tenant
tables (`encryption_key_by` requires it). This also keeps a tenant's
rows together, which makes reading one tenant, erasing one tenant and
restoring one tenant cheap.

**3. Do not index personal data.** An index on `email`, `name`,
`phone` or any free text keeps those values in plaintext. This includes
`UNIQUE` constraints, which are indexes.

**4. To look something up by a personal value, index a keyed hash of
it instead.** Store the real value in a sealed (non-indexed) column,
and a keyed hash (HMAC-SHA-256) of a normalized form in an indexed
column:

```sql
CREATE TABLE users (
  tenant_id  UUID,
  id         UUID,
  email      TEXT,      -- sealed: erased with the tenant's key
  email_hmac BYTEA,     -- indexed: HMAC-SHA-256(lookup_key, lower(trim(email)))
  PRIMARY KEY (tenant_id, id),
  UNIQUE (tenant_id, email_hmac)
) WITH (encryption_key_by = 'tenant_id');

SELECT id FROM users WHERE tenant_id = $1 AND email_hmac = $2;
```

- Normalize before hashing (lower-case, trim), or the same address
  hashes two ways.
- The hash key (`lookup_key`) is your application's secret. Keep it
  wrapped by datax's key service and unwrap it at start-up; never store
  it in the database in the clear.
- A keyed hash is not erasure by itself. Whoever holds the hash key can
  test a guess ("was alice@example.com a user?") against the stored
  hashes. So when a tenant is erased, **delete the rows that hold the
  hashes** (step 1 of the erasure procedure) rather than relying on the
  key destruction alone.
- If you must find a user before you know their tenant (a login by
  email), keep that in a small separate table keyed by the hash —
  `login_lookup (email_hmac BYTEA PRIMARY KEY, tenant_id UUID, user_id
  UUID)` — and delete its rows as part of erasing the tenant.

**5. Keep meaningful timestamps out of keys if timing is sensitive.**
`PRIMARY KEY (tenant_id, created_at, id)` leaves every creation time
readable after erasure. Put `created_at` in a sealed column unless you
need it for ordered scans.

**6. Know what a sealed column costs to query.** Filtering, sorting and
`CHECK` constraints on sealed columns work as usual — they run on
decrypted values — but a sealed column has no index, so a filter on it
reads the rows it could match. Within one tenant (the leading key
column) that is one tenant's rows, which is usually fine; across the
whole table it is a full scan. There are no statistics for sealed
columns, so the planner knows less about them.

**7. Time-series tables cannot be sealed** in the first version;
`CREATE TABLE` refuses the combination. Keep personal data out of
time-series tables, or in an ordinary sealed table that references
them.

## Erasing a tenant

Destroying a key alone removes the encrypted values, but not the
plaintext key columns, index entries or keyed hashes. The full
procedure:

1. **Delete the tenant's rows** in every table that holds them,
   including lookup tables such as `login_lookup`. Ordinary `DELETE`s.
2. **Purge the deleted versions** from each of those tables:
   `ALTER TABLE items PURGE`. Without this, deleted rows stay in the
   database's history until the table's GC window passes (`gc_ttl`,
   25 hours by default). A purge needs table ownership and runs outside
   a transaction; it reports per replica whether it finished.
3. **Destroy the tenant's key**: `SELECT
   datax_destroy_tenant_key('items', $tenant_id)`. The statement
   returns only after every gateway has stopped using the key (within
   about 10 seconds), and it purges the key's own record.

After that, the tenant's data is gone from the live cluster. A query
that still touches a row sealed under a destroyed key fails rather than
returning it; that is why step 1 comes first.

**Backups are the remaining copy, and the agreed behaviour is a
documented dependency:** a backup taken before step 3 still contains
the tenant's key (wrapped by the cluster's key) and its data. **The
tenant is fully erased once every backup taken before the destruction
has expired.** Your backup retention period is therefore your erasure
delay. If you keep backups for 35 days, say so in your privacy terms as
"within 35 days".

If a legal deadline is shorter than your backup retention, either
shorten the retention or tell us: a stronger option (keeping tenant
keys out of backups, or wrapping each tenant's key under the tenant's
own KMS key) was considered and left out of the first version, and can
be built if it is needed.

What survives erasure in any case: the fact that a tenant id existed
(if it appears elsewhere), and, in backups until they expire, row
counts and value sizes.

## Using the key service

The pattern is envelope encryption. Your application makes a random
data key, encrypts its payload with it locally, and asks datax to wrap
the data key under a named key. It stores the wrapped key beside the
ciphertext. To read, it asks datax to unwrap the data key and decrypts
locally.

```sql
CREATE ENCRYPTION KEY documents;

-- wrap: (key name, data key, context) → wrapped blob
SELECT datax_key_wrap('documents', $data_key, $context);

-- unwrap: (key name, wrapped blob, context) → data key
SELECT datax_key_unwrap('documents', $wrapped, $context);

ALTER ENCRYPTION KEY documents ROTATE;   -- new version; old blobs still unwrap
```

The arguments are `text, bytea, bytea` and the result is `bytea`. Pass
the data key, the blob and the context as **parameters** (`$1`, `$2`),
not as literals in the statement text: statement text is shown to
admins in `pg_stat_activity`. The functions take constants or
parameters only, not columns: read a stored blob first, then pass it.

Creating a key needs the admin role, and the creating role owns it;
an admin can hand a key to your application's owning role with `ALTER
ENCRYPTION KEY documents OWNER TO app_owner`. The owner (and admins)
rotate, disable, drop and grant on it; the owner may also use it.

- **Context.** Pass something that identifies what the key protects —
  the tenant id, or tenant id plus object id. A blob wrapped under one
  context will not unwrap under another, so one tenant's wrapped keys
  cannot be replayed for another's.
- **Rotation** creates a new version for new wraps. Existing wrapped
  blobs keep unwrapping; re-wrap them at your own pace if you need to.
- **Privileges.** `WRAP` and `UNWRAP` are separate. Give writer-only
  services `WRAP` alone; they can protect data but never recover a
  data key. Only services that must read get `UNWRAP`. `USAGE` grants
  both.
  ```sql
  GRANT WRAP   ON ENCRYPTION KEY documents TO uploader;
  GRANT UNWRAP ON ENCRYPTION KEY documents TO reader;
  ```
- **Know what UNWRAP means.** A role allowed to unwrap under a key can
  recover every data key ever wrapped under it. Treat those credentials
  like the data itself.
- **Admins are not readers by default.** An admin who does not own a
  key can manage it but cannot wrap or unwrap under it until it is
  granted the privilege — a grant the audit records.
- **Every wrap and unwrap is audited** (who, key, version, operation,
  outcome; never the key material) in datax's audit table, and both are
  **rate-limited per role** on each node (100 a second by default; an
  operator changes it with `SET CLUSTER SETTING
  key_service.rate_limit`). A call over the limit fails with SQLSTATE
  `53400`: back off and retry. Cache an unwrapped data key in memory for
  as long as you work on its object rather than unwrapping it per read.
- **Errors.** A blob that does not open — the wrong context, the wrong
  key, or a damaged blob — fails with `39000` and the same message
  whichever it is. A missing privilege is `42501`, an unknown key
  `42704`.
- **Disabling.** `ALTER ENCRYPTION KEY documents DISABLE` stops every
  use of the key — wraps and unwraps both — on every node before the
  statement returns (within about 10 seconds); `ENABLE` brings it
  back. `REVOKE` takes effect the same way.
- **Small values** (tokens, API secrets in a column) can instead use
  `datax_encrypt(key, plaintext, context)` / `datax_decrypt`, so the
  key never leaves datax. Values are capped at 64 KiB; use envelope
  encryption for anything larger.
- **Transport.** Data keys travel over the SQL connection. On a cluster
  without TLS these functions only work from the same machine.
- `DROP ENCRYPTION KEY documents` destroys every version: everything
  wrapped under it becomes unreadable, subject to the same backup
  dependency as above. The statement returns once no node uses the key
  any more. The name stays taken; a new key needs a new name.

## What this does not protect against

- A role that can `SELECT` from a sealed table reads plaintext:
  sealing is not access control. Use privileges and row-level security
  (`CREATE POLICY`) to decide who reads which rows.
- Anything in a primary key, an index, a row count or a value size.
- A compromised datax gateway, while it holds a key in memory.
- Someone who holds the cluster's keys and a backup taken before a
  tenant was erased, until that backup expires.

## Prerequisites on the cluster

- Every node runs with `--enc-root` (a root for its keys: a file, a
  command, a split root key, or a cloud KMS). Nodes using `--enc-key`
  files move over first with `datax debug enc-root adopt`, one stopped
  node at a time. Enabling the feature on a cluster that still has a
  key-file node is refused.
- The cluster runs the version that introduces the feature, finalized.
  Until then the statements above are refused.
- Restoring a backup into a different cluster needs the source
  cluster's key, exported by an operator
  (`datax debug cluster-key export`).

## Checklist for an application team

- [ ] Every primary-key column is an opaque id (no emails, names,
      phone numbers or lookup-able numbers).
- [ ] Per-tenant tables lead their primary key with the tenant column.
- [ ] No index or `UNIQUE` constraint on a personal value; lookups use
      a keyed hash of a normalized value instead.
- [ ] The hash key and any application data keys are held wrapped by
      datax's key service, not in the clear.
- [ ] Timestamps that reveal behaviour are not in keys.
- [ ] No personal data in time-series tables or in `NOTIFY` payloads.
- [ ] The erasure procedure deletes rows (including lookup rows),
      purges, then destroys the key — and is tested.
- [ ] The privacy terms state the erasure delay as the backup
      retention period.
- [ ] Writer-only services hold `WRAP`, not `UNWRAP` or `USAGE`.

## Questions to answer before relying on erasure

1. **What is your erasure deadline, and how long do you keep backups?**
   If the deadline is shorter, shorten retention or ask for the
   stronger option.
2. **Does any primary key or index hold personal data today?** If so,
   those tables need the changes above before sealing protects that
   data.
