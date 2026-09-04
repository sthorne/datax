# Differences from PostgreSQL

datax speaks the PostgreSQL wire protocol and a subset of its SQL. Standard
drivers, `psql`, and ORMs that stay inside the subset work unmodified. This
page is the honest list of where the road ends, with workarounds.

## Things that will error immediately

All of these are rejected with a clear error (usually SQLSTATE `42601`
syntax error or `0A000` feature not supported):

| Missing | Workaround |
|---|---|
| `OFFSET` | paginate by key: `WHERE id > $last ORDER BY id LIMIT n` (faster anyway) |
| `RETURNING` | `SELECT` after the write, in the same transaction |
| Functions beyond `now()`, `coalesce()`, `length()`, `lower()`, `upper()`, `abs()`, `array_to_string()`, `pg_size_pretty()` and the catalog functions tools call ([list](sql.md#reading)) — SQLSTATE `42883` | compute client-side |
| `INTERSECT` / `EXCEPT` (`UNION [ALL]` **is supported**) | merge client-side |
| `CHECK` / `FOREIGN KEY` / `UNIQUE` column constraints | `CREATE UNIQUE INDEX` covers uniqueness; enforce the rest in the application |
| Sequences / `SERIAL` / `DEFAULT` expressions | generate ids client-side (UUIDs distribute writes better than sequences here anyway) |
| `COPY ... TO`, COPY options beyond `FORMAT` | `COPY t FROM STDIN` **is supported** (text, CSV, binary — psql `\copy` and pgx `CopyFrom` work); export with `SELECT` instead |
| Schemas | `public` is the only schema: `db.public.t` and `public.t` are accepted, any other schema name is an error; `search_path` is accepted and ignored. Databases are real (`CREATE DATABASE`, the URL's database, `USE`, `SET database`, `current_database()`); see [Databases](sql.md#databases) |
| Views, triggers, stored procedures, `LISTEN/NOTIFY` | — |

## Things that exist but behave differently

- **Serializable only.** No isolation levels to choose; every transaction
  can fail with `40001` and clients **must** retry
  ([details](sql.md#transactions-and-retries)). Code written for
  PostgreSQL's default read-committed often has no retry loop — it needs
  one here.
- **`DECIMAL(p,s)` is enforced** (rescale to `s` half-even, `22003` on
  overflow, fixed-scale rendering), with small deviations: an invalid
  typmod (`DECIMAL(0)`, scale > precision) is a syntax error `42601`
  rather than PostgreSQL's `22023`; `VARCHAR(n)` and other typmods are
  still accepted and ignored; expression/aggregate results render in
  canonical form (no declared scale), as in PostgreSQL.
- **Bare decimal literals are DECIMAL** — same as PostgreSQL, but note
  `SELECT 1.5` describes as `NUMERIC`, not `float8`.
- **JSONB**: `->`/`->>` extraction (single-table queries and joins;
  grouped SELECT lists refuse), containment `@>` (single-table only),
  equality comparison, and `IS [NOT] NULL`. No indexing on
  JSONB columns (`@>` always filters — no inverted indexes), no ordering
  comparisons, no `<@`/`?`/`?|`/`?&`. `@>` is not accepted in `HAVING` or
  after a computed left-hand side. One asymmetry to know: `@>` compares
  numbers **numerically** (`1` contains `1.0`), while jsonb `=` compares
  normalized text (`'{"a":1}' = '{"a":1.0}'` is false).
- **`OR` is supported with full boolean grouping**, but `OR` conditions
  never become index bounds (they filter fetched rows) — keep an
  indexable `AND` condition alongside on large tables. `IN` and `EXISTS`
  subqueries cannot appear inside `OR` (scalar subqueries can). Computed
  left-hand sides (`qty * 2 > 10`) work in single-table queries and joins.
- **`LIKE` / `ILIKE` and regular expressions (`~`, `~*`)** filter fetched
  rows; there is no pattern-prefix index optimization.
- **Join order is execution order until statistics exist** — with
  statistics for every joined table (`ANALYZE` or the background
  sampler), INNER joins are cost-reordered greedily; LEFT joins,
  self-joins, cross joins and joins carrying ON filters or correlated
  subqueries always run in the written order, so put the most selective
  table first there. `ON` must include an equality against an earlier
  table (`ON true` is not accepted; use `CROSS JOIN` or `FROM a, b`).
- **Correlated subqueries are nested loops.** Each is re-run per row of
  the enclosing query (memoized on the referenced values), so a
  correlated subquery over a large outer table costs outer rows × inner
  query. Prefer a `JOIN` when the outer side is big.
- **Casts are absorbed.** `x::type` and `CAST(x AS type)` parse and do
  nothing (types come from the schema); the one exception is
  `'name'::regclass`, which resolves a table name to its OID. There is no
  `oid::regclass` back to a name.
- **`COPY FROM STDIN` is not atomic.** It commits in chunks (128 rows /
  1 MiB) as the stream arrives, so a failure partway leaves the earlier
  chunks committed — the error names the failing row and how many rows
  were already committed. For the same reason COPY is refused inside
  `BEGIN` (SQLSTATE `25001`) and only the `FORMAT` option is accepted.
- **`CREATE USER name PASSWORD '...'`** — no `WITH`.
- **`CREATE INDEX` is always online** (like `CONCURRENTLY`) and cannot run
  inside a transaction block.
- **`ORDER BY`** takes output names, positions and expressions, but an
  expression sort never uses an index; NULLs order last ASC / first DESC
  (PostgreSQL default). Under `GROUP BY` and `UNION`, output names only.
- **Timestamps are UTC** (`TimeZone` is always `UTC`); microsecond
  precision on the wire, nanosecond internally.
- **`EXPLAIN`** returns one plain-text line, not a plan tree; there is no
  `EXPLAIN ANALYZE`.
- **`ANALYZE`** exists but is admin-only (PostgreSQL allows table
  owners), takes no column list or options, and stores row counts +
  distinct estimates only — no histograms, no `pg_statistic`. `SHOW
  STATS FOR t` is the (non-PostgreSQL) way to inspect them.
- **`AS OF SYSTEM TIME`** exists (CockroachDB syntax, not PostgreSQL) —
  cheap historical/follower reads, including bounded staleness:
  `AS OF SYSTEM TIME with_max_staleness('10s')` reads the freshest data
  the local replicas can serve within the bound.
- **`SET x = y`** is parsed and ignored (drivers send these at startup);
  `SHOW x` answers for the settings datax has (`SHOW ALL` lists them) and
  is SQLSTATE `42704` otherwise. `server_version` reports a
  PostgreSQL-14-compatible string, so `psql` and drivers use their
  PostgreSQL-14 query flavors.

## Errors you should know

| SQLSTATE | Meaning | What to do |
|---|---|---|
| `40001` | serialization failure | retry the whole transaction |
| `25P02` | transaction poisoned by an earlier error | `ROLLBACK` (or `ROLLBACK TO SAVEPOINT`) |
| `23505` | unique violation | application-level conflict |
| `0A000` | feature not supported | you've left the subset — check this page |
| `42601` | syntax error | likewise, usually |

## What psql and ORMs can see

The `pg_catalog` and `information_schema` views ([list](sql.md#introspection-show-and-the-catalogs))
are real, read-only virtual tables over the live schema, and the SQL
they are queried with (joins with parenthesized `ON`, `array(SELECT
...)`, `= ANY(...)`, `unnest(...)`, `UNION ALL`, correlated subqueries
in the select list, `CASE`, `::casts`, `OPERATOR(pg_catalog.~)`,
`COLLATE`, `E'...'`) parses. In practice:

- **psql**: `\l`, `\dt`, `\di`, `\d t`, `\d+ t`, `\d index`, `\du`,
  `\dn`, `\dp` / `\z`, `\dT`, `\db`, `\dx`, `\df`, `\dS`, tab completion
  and the `+` variants render; the list commands for features datax lacks
  (`\dv`, `\ds`, `\dy`, `\dRp`, `\dew`, ...) render empty. `\dd` (object
  comments, a `UNION` inside a derived table joined to `pg_description`)
  is the known holdout. Sizes (`\dt+`, `\l+`) show blank: `pg_table_size`
  and `pg_database_size` are `NULL`.
- **Drivers**: pgx's, psycopg's and lib/pq's startup queries
  (`pg_type` lookups, `SHOW standard_conforming_strings`, `SET
  client_encoding`) work.
- **ORMs / migration tools**: schema introspection through
  `information_schema.tables` / `columns` / `table_constraints` /
  `key_column_usage`, `pg_indexes`, `pg_class` + `pg_attribute` +
  `pg_index` joins, `format_type`, `pg_get_indexdef`,
  `pg_get_constraintdef`, `'t'::regclass` and `to_regclass`-style
  existence checks (`SELECT 1 FROM pg_class WHERE relname = ...`) return
  what PostgreSQL would for a schema in the datax subset. Reads of
  features datax lacks (sequences, foreign keys, triggers, comments,
  extensions) come back empty rather than erroring.
- **Writes to a catalog** are refused with SQLSTATE `42501`.

## Client compatibility notes

- pgx (native and `database/sql`), lib/pq, and psql work, in both simple
  and extended protocol modes, text and binary formats.
- Prepared statements and portal suspension both work — JDBC-style fetch
  sizes are fine. The full result is materialized server-side at the
  first Execute and served in fetch-size chunks, so a fetch limit bounds
  wire traffic per round trip, not server memory.
- `sslmode=verify-full` works in secure mode with the datax CA
  ([Security](security.md)); `sslmode=disable` in insecure mode.
