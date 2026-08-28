# SQL layer and PostgreSQL wire protocol

datax's SQL surface is deliberately small in v1: enough to create tables,
read and write rows by primary key or scan, and run multi-statement
transactions — all over the standard PostgreSQL wire protocol.

## Grammar (v1)

```
CREATE TABLE t (col TYPE [NOT NULL], ..., PRIMARY KEY (col, ...))
DROP TABLE t
INSERT INTO t [(cols)] VALUES (v, ...), (v, ...)
SELECT * | col, ...  FROM t [WHERE conjunction] [LIMIT n]
SELECT <literal exprs>                  -- e.g. SELECT 1 (client health checks)
UPDATE t SET col = value, ... [WHERE conjunction]
DELETE FROM t [WHERE conjunction]
BEGIN / COMMIT / ROLLBACK
SHOW TABLES
```

- Types: `INT8` (aliases INT, INTEGER, BIGINT), `FLOAT8` (DOUBLE PRECISION),
  `TEXT` (aliases STRING, VARCHAR), `BOOL` (BOOLEAN).
- WHERE: conjunctions (`AND`) of `col op literal`, ops `= != < <= > >=`.
- Every table must declare a `PRIMARY KEY`.
- Parameters (`$1 …`) are supported through the extended protocol (text
  format).

Not in v1: secondary indexes, joins, aggregates, GROUP BY, ORDER BY,
subqueries, ALTER, constraints beyond PRIMARY KEY / NOT NULL, sequences,
DEFAULT.

## Catalog

Table descriptors are JSON documents stored in system keys (range 1):

- `/system/desc/<tableID>` → `{ID, Name, Columns[{ID, Name, Type, NotNull}],
  PrimaryKey: [colID...]}`
- `/system/ns/<name>` → tableID (namespace index)
- `/system/idgen` → next descriptor ID (incremented transactionally)

DDL runs inside a normal transaction. Each gateway caches descriptors and
refreshes on miss; there is no descriptor versioning/leasing in v1
(concurrent `CREATE TABLE` + use from other gateways is best-effort — schema
changes beyond CREATE/DROP are out of scope).

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

## Execution

There is no optimizer. The executor pattern-matches:

- WHERE fully constrains the PK by equality → point `Get`.
- Otherwise → `Scan` of the table's key span with in-memory filtering.
- UPDATE / DELETE: read the matching rows, then write inside the same
  transaction.

Every statement runs in a transaction: implicit (auto-commit, with
transparent server-side retry on serialization conflicts) or the session's
explicit `BEGIN ... COMMIT`.

## Wire protocol

`pkg/pgwire` implements the PostgreSQL v3 protocol on
[`pgproto3`](https://github.com/jackc/pgx/tree/master/pgproto3):

- **Startup**: `SSLRequest` answered with `N` (no TLS in v1); **trust
  authentication** (any user, no password — SCRAM is future work);
  `ParameterStatus` for `server_version` (reports a PG-13-compatible version
  string), `client_encoding=UTF8`, `DateStyle=ISO`, `integer_datetimes=on`,
  `standard_conforming_strings=on`, `TimeZone=UTC`; then `BackendKeyData` and
  `ReadyForQuery`.
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
