# SQL layer and PostgreSQL wire protocol

datax's SQL surface is deliberately small in v1: enough to create tables,
read and write rows by primary key or scan, and run multi-statement
transactions — all over the standard PostgreSQL wire protocol.

## Grammar (v1)

```
CREATE TABLE t (col TYPE [NOT NULL], ..., PRIMARY KEY (col, ...))
DROP TABLE t
INSERT INTO t [(cols)] VALUES (v, ...), (v, ...)
SELECT * | col, ... | aggregates  FROM t [AS OF SYSTEM TIME 't'] [WHERE conjunction]
    [ORDER BY col [ASC|DESC], ...] [LIMIT n] [FOR UPDATE]
SELECT <literal exprs>                  -- e.g. SELECT 1 (client health checks)
UPDATE t SET col = value, ... [WHERE conjunction]
DELETE FROM t [WHERE conjunction]
BEGIN / COMMIT / ROLLBACK
SAVEPOINT name / RELEASE SAVEPOINT name / ROLLBACK TO SAVEPOINT name
SHOW TABLES
```

- Types: `INT8` (aliases INT, INTEGER, BIGINT), `FLOAT8` (DOUBLE PRECISION),
  `TEXT` (aliases STRING, VARCHAR), `BOOL` (BOOLEAN).
- WHERE: conjunctions (`AND`) of `col op literal`, ops `= != < <= > >=`,
  plus `col IS [NOT] NULL`.
- Every table must declare a `PRIMARY KEY`.
- Parameters (`$1 …`) are supported through the extended protocol (text
  format).

v2 additions: `CREATE [UNIQUE] INDEX name ON t (cols)`,
`EXPLAIN SELECT ...` (one-line access plan), `ORDER BY` (in-memory sort,
skipped when the access path already delivers the order; PG-default NULL
ordering), aggregates `COUNT(*)/COUNT(col)/SUM/AVG/MIN/MAX` (whole-table,
no GROUP BY, no mixing with plain columns), and
`ALTER TABLE t ADD COLUMN c TYPE` (nullable-only) / `DROP COLUMN c`
(lazy: old bytes are skipped on decode; PK/indexed columns refused;
column IDs are never reused, so re-adding a name cannot resurrect old
values). Descriptor leases (below) drain schema changes across gateways:
by the time a DDL statement returns, every live gateway plans against the
new descriptor version.

`SELECT ... FROM t AS OF SYSTEM TIME 't'` pins the read to a fixed past
timestamp — a negative duration (`'-5s'`), an RFC 3339 timestamp, or Unix
nanoseconds — and runs it in its own read-only historical transaction
(refused inside an explicit transaction block). Reads old enough to be
covered by the range's closed timestamp are served by the gateway's LOCAL
replica without contacting the leader — follower reads (see
docs/replication-and-placement.md); more recent ones fall back to leaders.
The usable window is bounded by the closed-timestamp lag (default 3s)
on the recent side and the GC TTL (default 25h) on the old side.

Still out of scope: joins, GROUP BY/HAVING, subqueries, DISTINCT,
constraints beyond PRIMARY KEY / NOT NULL, sequences, DEFAULT.

## Catalog

Table descriptors are JSON documents stored in system keys (range 1):

- `/system/desc/<tableID>` → `{ID, Name, Columns[{ID, Name, Type, NotNull}],
  PrimaryKey: [colID...]}`
- `/system/ns/<name>` → tableID (namespace index)
- `/system/idgen` → next descriptor ID (incremented transactionally)

DDL runs inside a normal transaction. Each gateway caches descriptors;
descriptor **versions and leases** make that cache safe across gateways:

- Every descriptor carries a `Version`, bumped by each change.
- A gateway that caches a descriptor writes a lease record at
  `/system/lease/<descID>/<gatewayUUID>` — `{version, expiration}` — and
  may serve from its cache only while the lease is unexpired (TTL 10s by
  default, configurable in the server config). A background
  loop renews leases at TTL/3 by re-reading the descriptor, which is also
  how a gateway adopts new versions.
- After a DDL statement's transaction commits, the issuing session
  **drains**: it waits until every live (unexpired) lease on the descriptor
  is at the new version or later. Expired leases cannot be used, so a
  crashed or partitioned gateway delays the drain by at most one TTL
  (hard cap 2×TTL). When the statement returns, no gateway is still
  planning against the old schema.

Remaining gap: a transaction that issued its `BEGIN` before the drain, on
another gateway, keeps the descriptor version it started with until it
commits. Statement-sized windows are closed; long-lived explicit
transactions are not (tracked in issue #22).

## Row encoding (v2)

- **Key**: `/t/<tableID>/<indexID>/` + order-preserving encoding of the
  index column values (`pkg/util/encoding`), so index order = key order and
  range scans work naturally. Primary rows are **index 1**; the layout
  reserves the space secondary indexes live in.
- **Value**: one version byte, then for each non-NULL non-PK column in
  ascending column-ID order: the column ID (uvarint), a type tag, and the
  type's payload (int/float: 8 bytes; string: length-prefixed; bool: 1
  byte). NULL = absent. Payloads are self-describing, so a decoder skips
  column IDs its descriptor does not know — which is exactly what makes
  lazy `DROP COLUMN` and nullable `ADD COLUMN` free. Binary encode is
  ~3× faster than the JSON encoding it replaced (see rowenc benchmarks).

## Secondary indexes (v2)

`CREATE [UNIQUE] INDEX name ON table (cols)` adds an index with its own
key space (`/t/<tableID>/<indexID>/`):

- **Non-unique**: key = indexed columns + primary key columns, value = a
  one-byte marker. A row with NULL in any indexed column has **no entry**
  (SQL equality never matches NULL, so equality lookups stay correct; such
  rows are found by full scans).
- **Unique**: key = indexed columns only, value = the encoded primary key.
  NULLs in unique-indexed columns are **rejected** (a deliberate divergence
  from PostgreSQL's multiple-NULLs-allowed behavior).

Maintenance happens inside the writing transaction — index entries ride the
same write batch as the row, so they commit or roll back atomically, and
uniqueness checks read through the transaction: two racing inserts of the
same value collide on the index key's intent, so at most one commits.
`CREATE INDEX` builds **online**, like PostgreSQL's
`CREATE INDEX CONCURRENTLY` (and, like it, refuses to run inside an
explicit transaction block — code 25001):

1. Publish the index in the **write-only** state and drain the descriptor
   lease: every gateway now maintains the index on writes, but the planner
   ignores it.
2. Backfill from a full table scan in its own transaction. Rows committed
   before the backfill's snapshot are written by the backfill; rows after
   are maintained by their writers (guaranteed by step 1's drain), so the
   union is complete. The backfill transaction deliberately touches only
   row and index data — the descriptor flip to **public** is a separate
   small transaction, so a stream of concurrent writes cannot starve the
   backfill's commit. Unique violations (including a NULL in a
   unique-indexed column) abort the build and remove the write-only index
   again.
3. Drain once more, so every gateway plans with the public index.

The backfill is **bounded and snapshot-planned**: a planning sweep reads
row keys inconsistently at a fixed boundary timestamp (so its row set is
frozen — concurrent writers cannot extend it and the sweep terminates no
matter how fast the table grows; rows committed after the boundary are
maintained by their post-drain writers), and each planned chunk then
re-reads its own narrow key span in a serializable transaction and writes
the entries. A concurrent delete or update inside a chunk invalidates its
read and forces a rescan, so entries always reflect rows that exist at the
chunk's commit; writes elsewhere in the table never restart a chunk.
Memory, transaction size, and raft entry size stay bounded regardless of
table size.

Left for later: a delete-only state for online index drops, and the
long-lived-transaction descriptor pinning noted under Catalog.

## Execution

There is no cost model. The executor ranks access paths:

1. WHERE pins every PK column by equality → primary point `Get`.
2. Every column of a unique index pinned → unique-index point lookup.
3. The best constrained scan of the primary key or an index: equality
   conjuncts pin a leading column prefix, and range conjuncts
   (`> >= < <=`) on the **next** key column become order-preserving scan
   bounds (`WHERE a = 1 AND b > 5 AND b <= 9` scans exactly the matching
   key span). Paths score 2 per pinned column plus 1 for a range; ties
   prefer the primary key (no index join).
4. Otherwise → full `Scan` of the primary rows with in-memory filtering.

Conjuncts a path cannot absorb (`!=`, `IS [NOT] NULL`, columns outside
the key prefix) stay as a **residual filter**: the executor re-checks the
complete WHERE clause against every fetched row regardless of path, so
plan bounds only ever narrow the scan. `LIMIT` is pushed into the KV scan
exactly when the residual is empty (every scanned row is a result row)
and any ORDER BY is already satisfied by the access path; scanned-row
counts are observable via `datax_sql_rows_scanned_total`.

A non-unique index has no entry for rows with NULL in any indexed column,
so the planner only uses one when the schema (`NOT NULL`) or the WHERE
clause (equality/range/`IS NOT NULL` conjuncts) proves those rows cannot
match — and `col IS NULL` therefore always reads the primary rows. Unique
indexes reject NULLs at write time and are always complete.

UPDATE and DELETE read the matching rows through the same path ranking,
then write inside the same transaction. `EXPLAIN SELECT ...` prints the
chosen path as a single row — bounds, order and limit-pushdown included —
which is what the tests assert plan selection with.

Every statement runs in a transaction: implicit (auto-commit, with
transparent server-side retry on serialization conflicts) or the session's
explicit `BEGIN ... COMMIT`.

## Wire protocol

`pkg/pgwire` implements the PostgreSQL v3 protocol on
[`pgproto3`](https://github.com/jackc/pgx/tree/master/pgproto3):

- **Startup**: in secure mode (`--certs-dir`), `SSLRequest` is answered
  with `S`, the connection upgrades to TLS, and the client authenticates
  with **SCRAM-SHA-256** (RFC 5802/7677, hand-implemented server-side with
  stdlib crypto; verifiers — never plaintext — live at
  `/system/users/<name>`; unknown users and wrong passwords fail with one
  uniform `28P01`, and a full dummy exchange runs for unknown users so the
  flow leaks nothing). Cleartext startup is refused in secure mode. In
  insecure mode `SSLRequest` gets `N` and authentication is trust.
  `CREATE USER / ALTER USER ... PASSWORD / DROP USER` manage credentials;
  `--root-password` seeds root's at startup. No roles or privileges
  (documented limitation): any authenticated user can do anything.
  Then `ParameterStatus` for `server_version` (reports a PG-13-compatible
  version string), `client_encoding=UTF8`, `DateStyle=ISO`,
  `integer_datetimes=on`, `standard_conforming_strings=on`, `TimeZone=UTC`;
  then `BackendKeyData` and `ReadyForQuery`.
- **Simple query** (`Q`): multi-statement strings split on `;`, each executed;
  results as `RowDescription` → `DataRow`* → `CommandComplete`, values in text
  format encoded via `pgtype` (OIDs: int8=20, float8=701, text=25, bool=16).
- **Extended protocol**: minimal but real, because pgx's default mode prepares
  statements: `Parse` / `Bind` / `Describe` / `Execute` / `Sync` / `Close`.
  Text-format parameters only; binary parameters are rejected with a clear
  error. No portal suspension.
- **Transaction status is load-bearing**: `ReadyForQuery` carries `I` (idle),
  `T` (in transaction), or `E` (failed transaction — everything except
  `ROLLBACK` is rejected with `25P02`).
- **Errors** carry proper SQLSTATEs: `40001` serialization_failure (retry
  me!), `42P01` undefined_table, `23505` unique_violation, `42601`
  syntax_error, `0A000` feature_not_supported.

## Client compatibility

Tested against `psql`, [pgx](https://github.com/jackc/pgx) v5 in both its
default (extended) and simple-protocol query modes, and `database/sql` via
pgx's stdlib adapter. Applications should treat SQLSTATE `40001` as "retry
the transaction", exactly as with CockroachDB or serializable PostgreSQL.
