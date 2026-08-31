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
| Functions beyond `now()`, `coalesce()`, `length()`, `lower()`, `upper()`, `abs()`; `CASE` expressions | compute client-side |
| `UNION` / `INTERSECT` / `EXCEPT` | merge client-side |
| `CHECK` / `FOREIGN KEY` / `UNIQUE` column constraints | `CREATE UNIQUE INDEX` covers uniqueness; enforce the rest in the application |
| Sequences / `SERIAL` / `DEFAULT` expressions | generate ids client-side (UUIDs distribute writes better than sequences here anyway) |
| `COPY ... TO`, COPY options beyond `FORMAT` | `COPY t FROM STDIN` **is supported** (text, CSV, binary — psql `\copy` and pgx `CopyFrom` work); export with `SELECT` instead |
| Multiple databases / schemas | one namespace; the URL's database name is accepted and ignored |
| Views, triggers, stored procedures, `LISTEN/NOTIFY` | — |

## Things that exist but behave differently

- **Serializable only.** No isolation levels to choose; every transaction
  can fail with `40001` and clients **must** retry
  ([details](sql.md#transactions-and-retries)). Code written for
  PostgreSQL's default read-committed often has no retry loop — it needs
  one here.
- **`DECIMAL(p,s)` typmod is ignored** (enforcement is planned, #39).
  `DECIMAL` itself is exact.
- **Bare decimal literals are DECIMAL** — same as PostgreSQL, but note
  `SELECT 1.5` describes as `NUMERIC`, not `float8`.
- **JSONB**: only `->` and `->>` (single-table queries), equality
  comparison, and `IS [NOT] NULL`. No containment `@>` (planned, #40), no
  indexing on JSONB columns, no ordering comparisons. Numbers inside JSONB
  keep exact textual fidelity rather than PostgreSQL's numeric parsing.
- **`OR` is supported with full boolean grouping**, but `OR` conditions
  never become index bounds (they filter fetched rows) — keep an
  indexable `AND` condition alongside on large tables. Subqueries cannot
  appear inside `OR`, and computed left-hand sides (`qty * 2 > 10`) work
  in single-table queries only.
- **Join order is execution order** — there is no cost-based reordering.
  Write the most selective table first. `ON` clauses must be equalities
  against earlier tables.
- **`COPY FROM STDIN` is not atomic.** It commits in chunks (128 rows /
  1 MiB) as the stream arrives, so a failure partway leaves the earlier
  chunks committed — the error names the failing row and how many rows
  were already committed. For the same reason COPY is refused inside
  `BEGIN` (SQLSTATE `25001`) and only the `FORMAT` option is accepted.
- **`CREATE USER name PASSWORD '...'`** — no `WITH`.
- **`CREATE INDEX` is always online** (like `CONCURRENTLY`) and cannot run
  inside a transaction block.
- **`ORDER BY`** uses output column names, not arbitrary expressions;
  NULLs order last ASC / first DESC (PostgreSQL default).
- **Timestamps are UTC** (`TimeZone` is always `UTC`); microsecond
  precision on the wire, nanosecond internally.
- **`EXPLAIN`** returns one plain-text line, not a plan tree; there is no
  `EXPLAIN ANALYZE`.
- **`AS OF SYSTEM TIME`** exists (CockroachDB syntax, not PostgreSQL) —
  cheap historical/follower reads.
- **`SET x = y`** is parsed and ignored (drivers send these at startup);
  `server_version` reports a PostgreSQL-13-compatible string.

## Errors you should know

| SQLSTATE | Meaning | What to do |
|---|---|---|
| `40001` | serialization failure | retry the whole transaction |
| `25P02` | transaction poisoned by an earlier error | `ROLLBACK` (or `ROLLBACK TO SAVEPOINT`) |
| `23505` | unique violation | application-level conflict |
| `0A000` | feature not supported | you've left the subset — check this page |
| `42601` | syntax error | likewise, usually |

## Client compatibility notes

- pgx (native and `database/sql`), lib/pq, and psql work, in both simple
  and extended protocol modes, text and binary formats.
- Prepared statements and portal suspension both work — JDBC-style fetch
  sizes are fine. The full result is materialized server-side at the
  first Execute and served in fetch-size chunks, so a fetch limit bounds
  wire traffic per round trip, not server memory.
- `sslmode=verify-full` works in secure mode with the datax CA
  ([Security](security.md)); `sslmode=disable` in insecure mode.
