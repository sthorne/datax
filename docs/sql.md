# SQL layer and PostgreSQL wire protocol

datax's SQL surface is deliberately small in v1: enough to create tables,
read and write rows by primary key or scan, and run multi-statement
transactions — all over the standard PostgreSQL wire protocol.

## Grammar (v1)

```
CREATE TABLE t (col TYPE [NOT NULL], ..., PRIMARY KEY (col, ...))
    [WITH (timeseries = true [, retention = '7d'] [, shards = N])]  -- see docs/timeseries.md
ALTER TABLE t SET (shards = M)          -- online re-shard of a sharded timeseries table
DROP TABLE t
INSERT INTO t [(cols)] VALUES (v, ...), (v, ...)
SELECT [DISTINCT] * | col, ... | aggregates
    FROM t [[AS] alias] | (SELECT ...) [AS] alias
    [[INNER | LEFT [OUTER]] JOIN t2 [[AS] alias] ON a.x = b.y [AND ...]]
    [AS OF SYSTEM TIME 't']
    [WHERE conjunction] [GROUP BY col, ...] [HAVING conjunction]
    [ORDER BY col [ASC|DESC], ...] [LIMIT n] [FOR UPDATE]
SELECT <literal exprs>                  -- e.g. SELECT 1 (client health checks)
UPDATE t SET col = value, ... [WHERE conjunction]
DELETE FROM t [WHERE conjunction]
BEGIN / COMMIT / ROLLBACK
SAVEPOINT name / RELEASE SAVEPOINT name / ROLLBACK TO SAVEPOINT name
SHOW TABLES
```

- Types: `INT8` (aliases INT, INTEGER, BIGINT), `FLOAT8` (DOUBLE PRECISION),
  `TEXT` (aliases STRING, VARCHAR), `BOOL` (BOOLEAN), `TIMESTAMPTZ`
  (alias TIMESTAMP [WITH TIME ZONE]; UTC nanoseconds internally,
  microsecond precision on the binary wire), `DATE`, `BYTES` (alias
  BYTEA; `\x` hex text format), `UUID`. String literals coerce into the
  new types (`WHERE at >= '2026-08-30 02:00:00Z'` becomes a key bound),
  and all are usable in primary keys and indexes via the
  order-preserving encodings. DECIMAL and JSONB remain out of scope.
- Columns may declare `DEFAULT <literal>` (constants only): INSERTs that
  omit the column store the default; an explicit NULL stays NULL.
- WHERE: conjunctions (`AND`) of `col op value`, ops `= != < <= > >=`,
  plus `col IS [NOT] NULL`, `col [NOT] IN (list | SELECT ...)`, and
  `[NOT] EXISTS (SELECT ...)`. A value is a literal, a parameter, a
  column reference, or a scalar subquery `(SELECT ...)` (correlated in
  WHERE; uncorrelated ones also work in INSERT values and UPDATE SET).
- Every table must declare a `PRIMARY KEY`.
- Parameters (`$1 …`) are supported through the extended protocol (text
  format).

v2 additions: `CREATE [UNIQUE] INDEX name ON t (cols)`,
`EXPLAIN SELECT ...` (one-line access plan), `ORDER BY` (in-memory sort,
skipped when the access path already delivers the order; PG-default NULL
ordering), aggregates `COUNT(*)/COUNT(col)/SUM/AVG/MIN/MAX`
(whole-table or per group), and
`ALTER TABLE t ADD COLUMN c TYPE [DEFAULT lit [NOT NULL]]` / `DROP
COLUMN c` (lazy: old bytes are skipped on decode; PK/indexed columns
refused; column IDs are never reused, so re-adding a name cannot
resurrect old values). An ADD with a DEFAULT is fill-on-read: rows
written before the ADD lack the column on disk and decode as the
default, while rows written afterwards store an explicit NULL marker
when NULL — so NULL and "predates the column" stay distinguishable and
no backfill is needed. `NOT NULL` on ADD requires a DEFAULT. Descriptor leases (below) drain schema changes across gateways:
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

Still out of scope: multi-level correlated subqueries,
join reordering (join order = syntactic order) and joins beyond 8 tables,
expressions in join select lists,
constraints beyond PRIMARY KEY / NOT NULL, sequences,
DECIMAL and JSONB column types, DEFAULT expressions beyond constants.

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

Joins (INNER and LEFT OUTER, up to 8 tables) are left-deep nested loops
executed **in syntactic order** — there is no join reordering, so the
FROM/JOIN order you write is the plan you get. The base (first) table is
fetched through the ranking above using the WHERE conjuncts that
reference only it; for each partial row, the next JOIN's ON equalities
become synthetic equality predicates on that table — so a join key that
hits its primary key or an index turns into a point/index lookup per
row. Each ON conjunct must equate a column of the table being joined
with a column of any *earlier* table (skip-level equalities are fine).
The full WHERE clause evaluates on complete rows only, which is what
makes it filter after NULL extension on a LEFT JOIN
(`WHERE inner.col IS NULL` is the anti-join) — WHERE conjuncts are never
pushed into a LEFT-joined side's scan. Column references may be
qualified (`t.c` or `alias.c`); unqualified names must be unambiguous
(`42702`). `EXPLAIN` prints one path per level:
`nested loop left join; outer (c): range scan of primary key (id > 1);
inner (o) per outer row: scan of index "by_customer" (1 column prefix) +
primary key join; then inner (i) per row: point lookup on primary key`.

Aggregates and GROUP BY compose with joins: the joined rows run through
the grouped executor, with `GROUP BY c.name`, `SUM(o.total)`, HAVING and
DISTINCT all accepting qualified references. ORDER BY on a grouped join
orders by *result* column name (the same rule single-table GROUP BY
follows). Plain (non-grouped) join select lists remain columns or `*`.

Uncorrelated subqueries evaluate eagerly, inside the same transaction,
before the outer statement plans: a scalar subquery splices in as a
literal (zero rows = NULL; two rows = `21000`), `[NOT] IN (SELECT ...)`
becomes a value list with SQL three-valued semantics (`NOT IN` over a
NULL-bearing set is never true), and `[NOT] EXISTS` collapses to a
constant conjunct. Because splicing happens before planning,
`WHERE k = (SELECT MAX(k) ...)` plans as a point lookup. A derived
table — `FROM (SELECT ...) AS alias` — materializes the inner result in
memory and runs the outer pipeline (WHERE, grouping, ORDER BY, DISTINCT,
LIMIT) over it, which is how you filter on an aggregate output.

**Correlated** subqueries — an inner reference to the outer row, like
`WHERE salary = (SELECT MAX(salary) FROM emp WHERE dept_id = e.dept_id)`
— are supported in the WHERE clause of single-table SELECT (plain or
aggregated), UPDATE, and DELETE, as scalar comparisons, `[NOT] IN`, and
`[NOT] EXISTS`, one correlation level deep, over plain single-table
inner selects. Names resolve inner-scope-first (an inner column shadows
an outer one; qualify with the outer alias to force the outer scope).
Execution is a nested loop: the correlated conjunct leaves the planned
WHERE clause and re-runs the inner query per fetched outer row with the
outer values spliced in, memoized per distinct correlation key —
`EXPLAIN` says so (`correlated filter: nested loop ...`), and the cost
is honestly O(outer rows × inner query). A name resolving in neither
scope is a plain `42703` (not the old blanket "correlated" error); a
correlated reference in GROUP BY / ORDER BY / aggregate arguments, in a
join or derived-table outer, or reaching more than one level up is
rejected with a clear error. The parsed statement is never mutated, so
prepared statements re-evaluate their subqueries on every execution.

Grouping is post-fetch and never changes the access path: `GROUP BY`
hash-groups the fetched rows on the group columns' datums (NULL keys form
one group, per SQL) with streaming per-group aggregate state, and every
non-aggregate output must appear in GROUP BY (else `42803`). `HAVING`
conjuncts — aggregate calls, group columns, or output names — filter after
aggregation; `DISTINCT` is a degenerate grouping over the projection. On a
grouped or DISTINCT select, ORDER BY sorts the output rows by
result-column name and LIMIT applies last, so limit pushdown never applies
there.

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
  with **SCRAM-SHA-256** or **SCRAM-SHA-256-PLUS** (RFC 5802/7677/5929,
  hand-implemented server-side with stdlib crypto). The `-PLUS` mechanism
  binds the exchange to the TLS session via `tls-server-end-point` —
  relaying through a MitM with a different certificate fails the proof —
  and a client that supports binding but claims the server does not
  (gs2 flag `y`) is rejected as a downgrade. Passwords are
  SASLprep-normalized (PRECIS OpaqueString — the same profile pgx applies
  client-side; non-UTF-8 or prohibited input falls back to exact bytes),
  so non-ASCII passwords interoperate with spec-compliant clients.
  Alternatively, a CA-signed **client certificate** whose CommonName is
  the SQL user authenticates with no password at all
  (`datax cert create-client --user alice`, then `sslcert`/`sslkey`);
  a CN mismatch falls back to SCRAM. Verifiers — never plaintext — live
  at `/system/users/<name>`; unknown users and wrong passwords fail with
  one uniform `28P01`, and a full dummy exchange runs for unknown users
  so the flow leaks nothing. Cleartext startup is refused in secure
  mode. In
  insecure mode `SSLRequest` gets `N` and authentication is trust.
  `CREATE USER / ALTER USER ... PASSWORD / DROP USER` manage credentials;
  `--root-password` seeds root's at startup. Authorization: `root` is
  all-powerful; members of the **admin role** (`GRANT ADMIN TO user`,
  `REVOKE ADMIN FROM user`; root is implicitly a member) may run DDL,
  manage users, and grant; everyone else needs per-table privileges —
  `GRANT SELECT, INSERT, UPDATE, DELETE | ALL ON t TO user` /
  `REVOKE ... ON t FROM user`, enforced at execution with `42501`
  (joins and subqueries check every table they touch). Grants ride the
  table descriptor, so they propagate through the same version leases
  as schema changes: by the time GRANT returns, every gateway enforces
  it. In insecure (trust) mode the username is client-claimed, so
  enforcement there is advisory — anyone can claim `root`.
  Then `ParameterStatus` for `server_version` (reports a PG-13-compatible
  version string), `client_encoding=UTF8`, `DateStyle=ISO`,
  `integer_datetimes=on`, `standard_conforming_strings=on`, `TimeZone=UTC`;
  then `BackendKeyData` and `ReadyForQuery`.
- **Simple query** (`Q`): multi-statement strings split on `;`, each executed;
  results as `RowDescription` → `DataRow`* → `CommandComplete`, values in text
  format encoded via `pgtype` (OIDs: int8=20, float8=701, text=25, bool=16).
- **Extended protocol**: minimal but real, because pgx's default mode prepares
  statements: `Parse` / `Bind` / `Describe` / `Execute` / `Sync` / `Close`.
  Parameters and results in both text and binary formats for every column
  type (int8, float8, bool, text, timestamptz, date, bytea, uuid). No
  portal suspension.
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
