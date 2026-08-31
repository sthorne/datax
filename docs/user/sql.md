# SQL reference

The complete supported surface, user's-eye view. For grammar internals and
design rationale see [docs/sql.md](../sql.md); for what's deliberately
missing versus PostgreSQL see [Differences](postgres-differences.md).

## Types

| Type | Aliases | Notes |
|---|---|---|
| `INT8` | `INT`, `INTEGER`, `BIGINT` | 64-bit |
| `FLOAT8` | `DOUBLE PRECISION` | IEEE 754 double |
| `DECIMAL` | `NUMERIC`, `DEC` | exact arbitrary precision; `DECIMAL(p,s)` is accepted but **not enforced** |
| `TEXT` | `STRING`, `VARCHAR` | |
| `BOOL` | `BOOLEAN` | |
| `TIMESTAMPTZ` | `TIMESTAMP [WITH TIME ZONE]` | UTC; microsecond precision on the binary wire |
| `DATE` | | |
| `BYTES` | `BYTEA` | `'\xdeadbeef'` hex literals |
| `UUID` | | |
| `JSONB` | `JSON` | normalized on write: sorted keys, compact |

- String literals coerce into every type: `WHERE at >= '2026-08-30 02:00:00Z'`,
  `WHERE tag = 'a0eebc99-...'` — and become index bounds.
- Non-integer numeric literals are **DECIMAL**, as in PostgreSQL: `0.1`
  survives to a DECIMAL column unrounded. Assigning one to a `FLOAT8`
  column converts (correctly rounded).
- DECIMAL arithmetic and `SUM`/`AVG` are exact; mixing in a `FLOAT8`
  operand demotes the expression to float. `AVG(DECIMAL)` rounds to 6
  fractional digits (half-even). Float values never implicitly convert
  *to* DECIMAL.
- JSONB stores normalized text; numbers keep their exact ingest form.
  Extraction: `j -> 'key'` (jsonb), `j ->> 'key'` (text, must be last),
  chainable: `j -> 'a' ->> 'b'`. Missing keys and non-objects yield NULL.
  JSONB cannot be a primary key or indexed, and `->`/`->>` work in
  single-table queries only.
- Every type except JSONB can appear in primary keys and indexes.

## DDL

```sql
CREATE TABLE t (
  a INT8 NOT NULL,
  b TEXT DEFAULT 'x',            -- constants only
  c DECIMAL,
  PRIMARY KEY (a)                -- or: a INT8 PRIMARY KEY
);
CREATE TABLE IF NOT EXISTS t (...);
DROP TABLE [IF EXISTS] t;

CREATE [UNIQUE] INDEX by_b ON t (b);       -- online: no write outage
ALTER TABLE t ADD COLUMN d INT8 DEFAULT 0; -- lazy; NOT NULL requires DEFAULT
ALTER TABLE t DROP COLUMN d;               -- refused for PK/indexed columns
SHOW TABLES;
```

`CREATE INDEX` backfills online in bounded chunks, like PostgreSQL's
`CREATE INDEX CONCURRENTLY`; it cannot run inside an explicit transaction.

## Reading

```sql
SELECT * | expr [AS alias], ... FROM t [AS a]
  [JOIN t2 [AS b] ON b.x = a.y [AND ...]]       -- INNER or LEFT [OUTER], up to 8 tables
  [WHERE conjunct AND conjunct AND ...]
  [GROUP BY cols] [HAVING ...]
  [ORDER BY col [ASC|DESC], ...] [LIMIT n];
```

- **WHERE** supports full boolean logic: `AND`, `OR`, `NOT`, and
  parentheses over conditions of the form `expr op value`
  (`= != < <= > >=`), `col IS [NOT] NULL`,
  `col [NOT] IN (list | SELECT ...)`, `[NOT] EXISTS (SELECT ...)`,
  `j ->> 'k' op value`. The left side may be a computed expression
  (`qty * 2 > 10`, `lower(name) = 'x'`); a value may be a literal,
  parameter, column, or scalar `(SELECT ...)`. Restrictions: subqueries
  cannot appear inside `OR`, and computed left-hand sides work in
  single-table queries only. `OR` conditions filter fetched rows — they
  never become index bounds, so pair them with an indexable `AND`
  condition on large tables.
- **Expressions**: arithmetic `+ - * /` with standard precedence and
  parentheses (exact on DECIMAL/INT8; integer division truncates;
  division by zero is SQLSTATE `22012`), and the functions `now()`,
  `coalesce(...)`, `length(s)`, `lower(s)`, `upper(s)`, `abs(n)` — in
  SELECT lists, WHERE, INSERT VALUES, and UPDATE SET. Computed SELECT
  outputs describe as text on the wire.
- **Aggregates**: `COUNT(*)`, `COUNT(col)`, `SUM`, `AVG`, `MIN`, `MAX`,
  whole-table or per `GROUP BY` group, including over joins. `HAVING`
  filters on aggregates or group columns. `DISTINCT` is supported.
- **Joins** execute left-deep in the order written (there is no
  reorderer — put the most selective table first). `ON` must equate a
  column of the newly joined table with one from an earlier table. Join
  select lists are plain columns or `*`.
- **Subqueries**: uncorrelated scalars anywhere a value goes; derived
  tables `FROM (SELECT ...) AS d`; correlated subqueries in
  `EXISTS`/`IN`/scalar positions up to 4 nesting levels.
- **ORDER BY** result-column names; sorts in memory unless the access path
  already delivers the order.

Check the plan with `EXPLAIN SELECT ...` — one line naming the access path:

```
point lookup on primary key
scan of index "by_city" (city = 'oslo') + primary key join
range scan of primary key (series = 'cpu.node1', at >= 2026-08-30 10:00:00+00)
full table scan
```

A `full table scan` on a big table is the thing to fix (add an index, or
constrain the leading PK columns).

## Writing

```sql
INSERT INTO t (a, b) VALUES (1, 'x'), (2, 'y');     -- multi-row is one atomic statement
UPDATE t SET b = 'z', c = c + 1 WHERE a = 1;        -- expressions: col ± value
DELETE FROM t WHERE a = 1;
```

Batching inserts matters enormously for throughput — a 100-row `INSERT` is
one replication round; 100 single-row statements are 100.

## Transactions and retries

```sql
BEGIN;                                   -- or START TRANSACTION
UPDATE accounts SET balance = balance - 10 WHERE id = 1;
UPDATE accounts SET balance = balance + 10 WHERE id = 2;
COMMIT;                                  -- ROLLBACK / ABORT to abandon
```

Isolation is always **serializable**. The price: any statement — including
`COMMIT` — can fail with SQLSTATE **`40001`** (serialization failure) when
transactions conflict. This is not an error to surface to users; it is an
instruction to retry:

```go
for {
    err := run(tx)                       // BEGIN ... COMMIT
    if pgErr, ok := err.(*pgconn.PgError); ok && pgErr.Code == "40001" {
        continue                         // retry the whole transaction
    }
    return err
}
```

Single statements outside `BEGIN` are transactions too, retried
automatically by the server. Other transaction tools:

```sql
SAVEPOINT sp;  ROLLBACK TO SAVEPOINT sp;  RELEASE SAVEPOINT sp;
SELECT balance FROM accounts WHERE id = 1 FOR UPDATE;   -- lock rows you'll update
```

`SELECT ... FOR UPDATE` locks the read rows, turning read-modify-write
races into waits instead of `40001` retries. After a failed statement the
transaction is poisoned (`25P02`) until `ROLLBACK` — or
`ROLLBACK TO SAVEPOINT`, which revives it.

Deadlocks are detected and broken automatically (one victim gets `40001`).

## Historical and follower reads

```sql
SELECT COUNT(*) FROM users AS OF SYSTEM TIME '-5s';
SELECT ... AS OF SYSTEM TIME '2026-08-30T10:00:00Z';
```

Pins the read to a fixed past timestamp; reads older than the closed
timestamp lag (~3s) are served by the **local** replica without contacting
the leader — cheap read scaling for dashboards and reports. Bounded by the
GC window (25h) on the old side. Not allowed inside `BEGIN`.

## Timeseries tables

Monotonic keys (time!) funnel all inserts into one range. Timeseries
tables shard the hot tail:

```sql
CREATE TABLE metrics (
  series TEXT, at TIMESTAMPTZ, value FLOAT8,
  PRIMARY KEY (series, at)
) WITH (timeseries = true, shards = 8, retention = '30d');
```

- `shards` (2–256): rows spread over that many hash buckets, each its own
  range — inserts scale across nodes instead of hammering one tail.
- `retention` (`30d`, `12h`, ...): rows older than this are garbage
  collected automatically.
- Queries are unchanged — `WHERE series = '...' AND at >= ...` fans out
  over the buckets (visible in `EXPLAIN`). Fan-out costs read latency:
  measured ~2× point-read p50 at `shards=8`, in exchange for ~linear
  insert scaling.
- Re-shard online: `ALTER TABLE metrics SET (shards = 16);`

## Parameters

`$1, $2, ...` work through the extended protocol in text and binary
formats — every driver's normal parameterized-query path. Parameter types
are inferred from column context; when a driver asks, unknown parameters
describe as text.

## EXPLAIN-driven checklist

1. Point lookups should say `point lookup` — if not, the WHERE clause
   doesn't pin every PK (or unique index) column with `=`.
2. Range queries should show your bounds; a bound that didn't appear is
   probably a type the literal didn't coerce into, or a `->>` conjunct
   (paths never plan as bounds).
3. Joins: the plan prints one line per join level, in execution order —
   confirm the first table is the filtered one.
