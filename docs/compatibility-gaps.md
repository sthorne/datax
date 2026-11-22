# Compatibility gap register

Every known gap between datax and PostgreSQL, and between datax and
CockroachDB, in one place, so that each can be picked up and closed
when it is needed. This is a maintainer's register, not user
documentation: the user-facing account of the same ground, with
workarounds written for application developers, is
[Differences from PostgreSQL](user/postgres-differences.md).

**Verified against:** commit `fb15d88` (0.58.0, unreleased), on
2026-11-17. A single-node cluster built from that commit was probed with
psql 16.15 (`/usr/bin/psql`) and pg_dump 16; each statement's outcome
was recorded. CockroachDB was not run: its column is written from
CockroachDB's public documentation for v24.x and v25.x, as known to the
author, without fetching it again for this pass, and datax's side of each
CockroachDB row was checked on the live node or in datax's code and docs.

## Keeping it current

- **A gap closes:** move its row to [Closed](#closed), with the PR that
  closed it, and correct `user/postgres-differences.md` in the same PR.
  Do not reuse the ID.
- **A gap is found:** add a row to its area's table with the next free
  ID, and recount the [summary](#summary). If an issue is filed, link it.
- **A gap is being worked on:** put "in progress" beside its issue link.
- **Re-verify** when the version line above is more than a release old:
  rerun the probes (each row says what to run) and update the version
  line.

### Fields

| Field | Meaning |
|---|---|
| ID | Stable. `PG-<area>-NNN` against PostgreSQL, `CR-<area>-NNN` against CockroachDB |
| Gap | What is missing or different |
| Today | How it fails now: the error text and SQLSTATE, or "silently different" with what happens instead |
| Workaround | What a user can do now; "—" if nothing |
| P | P1 breaks common applications, ORMs, drivers or tools; P2 notable; P3 niche; P4 low priority by decision (Rails migration requirements) |
| Size | Rough effort: S (days), M (one to two weeks), L (weeks), XL (a project) |
| Src | **V** verified on a live node at `fb15d88`; **D** from datax's code or docs, not probed; **K** CockroachDB behaviour from its documentation (datax's side as V or D, said in the row) |
| Issue | The GitHub issue, if one exists |

## Summary

Open gaps by area and priority.

| Area | P1 | P2 | P3 | P4 | Total |
|---|---:|---:|---:|---:|---:|
| [PG: Query language](#query-language) | 0 | 10 | 12 | 0 | 22 |
| [PG: Functions and operators](#functions-and-operators) | 0 | 6 | 10 | 0 | 16 |
| [PG: Types](#types) | 0 | 9 | 17 | 0 | 26 |
| [PG: DDL](#ddl) | 0 | 14 | 9 | 1 | 24 |
| [PG: Programmability](#programmability) | 2 | 2 | 2 | 0 | 6 |
| [PG: Transactions and concurrency](#transactions-and-concurrency) | 1 | 6 | 5 | 0 | 12 |
| [PG: Security](#security) | 0 | 5 | 5 | 0 | 10 |
| [PG: Extensions](#extensions) | 0 | 6 | 4 | 0 | 10 |
| [PG: Wire protocol and clients](#wire-protocol-and-clients) | 0 | 2 | 4 | 0 | 6 |
| [PG: System catalogs and introspection](#system-catalogs-and-introspection) | 0 | 5 | 7 | 0 | 12 |
| [PG: Operations](#operations) | 0 | 4 | 7 | 0 | 11 |
| [CR: Changefeeds / CDC](#changefeeds-and-cdc) | 1 | 0 | 1 | 0 | 2 |
| [CR: Multi-region and placement](#multi-region-and-placement) | 0 | 5 | 2 | 0 | 7 |
| [CR: Indexes, computed columns, spatial](#indexes-computed-columns-and-spatial) | 0 | 6 | 1 | 0 | 7 |
| [CR: Bulk I/O, backup and restore](#bulk-io-backup-and-restore) | 0 | 5 | 4 | 0 | 9 |
| [CR: Jobs](#jobs) | 0 | 1 | 1 | 0 | 2 |
| [CR: Transactions and programmability](#transactions-and-programmability) | 1 | 2 | 3 | 0 | 6 |
| [CR: Row-level TTL and historical reads](#row-level-ttl-and-historical-reads) | 0 | 0 | 2 | 0 | 2 |
| [CR: Virtual clusters and cluster settings](#virtual-clusters-and-cluster-settings) | 0 | 1 | 1 | 0 | 2 |
| [CR: SHOW statements, crdb_internal, console](#show-statements-crdb_internal-and-the-console) | 0 | 2 | 2 | 0 | 4 |
| [CR: Schema changes, statistics, diagnostics](#schema-changes-statistics-and-diagnostics) | 0 | 2 | 2 | 0 | 4 |
| **PostgreSQL total** | **3** | **69** | **82** | **1** | **155** |
| **CockroachDB total** | **2** | **24** | **19** | **0** | **45** |
| **All** | **5** | **93** | **101** | **1** | **200** |

The most consequential gaps, in the order they should probably be taken:
see [Priorities](#priorities) at the end.

---

## Versus PostgreSQL (16/17)

datax reports `server_version` 14.0 so that clients use their
PostgreSQL 14 queries; the comparison here is with the PostgreSQL 16 and
17 that applications are written against. Features new in 17 (or 18)
say so.

### Query language

Probe tables for the rows below: `t (id INT8 PRIMARY KEY, a INT8, b
TEXT, c DECIMAL(10,2), d TIMESTAMPTZ, j JSONB, arr INT8[])` and `u (id
INT8 PRIMARY KEY, tid INT8, v TEXT)`.

| ID | Gap | Today | Workaround | P | Size | Src | Issue |
|---|---|---|---|---|---|---|---|
| PG-Q-003 | `DISTINCT ON` together with window functions; `DISTINCT ON` in a select without `FROM` (ignored) | `0A000`; ignored | `row_number() OVER (PARTITION BY ...)` in a derived table | P3 | S | D | — |
| PG-Q-004 | What `LATERAL` (#338) does not cover yet: a lateral subquery over a derived table or a set operation of its own, an outer reference in a lateral subquery's `ORDER BY` / `GROUP BY` / aggregate arguments, `RIGHT` / `FULL JOIN LATERAL` (PostgreSQL allows it when nothing to the left is read); `ROWS FROM (...)`; `json_table`; multi-argument `unnest`; a set-returning function in the select list of a query with `FROM` | `0A000 correlated subqueries are not supported`; `42P10`; `0A000` | a plain lateral select; expand in `FROM` | P2 | M | D | — |
| PG-Q-034 | An aggregate that appears only in `HAVING` (`SELECT 1 HAVING count(*) > 0`, also with `FROM t`) | `42803` | put the aggregate in the select list too | P3 | S | D | — |
| PG-Q-009 | Window functions over aggregates (`sum(count(*)) OVER ()`), a window call in `ORDER BY` | `0A000 grouped SELECT items must be plain columns or aggregates`; `42803 aggregate functions in ORDER BY require GROUP BY ...` | derived table | P2 | M | V | — |
| PG-Q-010 | Window frames: `RANGE` with an offset, `GROUPS`, `EXCLUDE`; `FILTER` and `DISTINCT` on a window call | `0A000 RANGE frames with an offset are not supported; use ROWS`; `42601 expected ")", found "groups" / "exclude"`; `0A000` for `FILTER` on a window call, with PostgreSQL's messages for `DISTINCT` / `ORDER BY` / `WITHIN GROUP` (which PostgreSQL refuses too) | `ROWS` frames; `CASE` inside the aggregate | P2 | M | V | — |
| PG-Q-011 | Functional dependency on the primary key: `GROUP BY t.id` then selecting `t.b` | `42803 column "t.b" must appear in the GROUP BY clause ...` | list every column in `GROUP BY` | P2 | M | V | — |
| PG-Q-012 | Row constructors and comparisons: `(a, b) > (1, 5)`, `(a, b) IN (SELECT ...)`, `ROW(1, 'a')`, whole-row references (`SELECT u FROM u`, `row_to_json(u)`) | `42601 expected ")", found ","`; `42883 unknown function "row"`; `42703 column "u" does not exist` | expand to column-wise `AND`/`OR`; build JSON with `jsonb_build_object` | P2 | M | V | — |
| PG-Q-014 | Correlated subqueries over a set operation; a correlated subquery in `UPDATE ... SET` | `0A000 correlated subqueries are not supported (column "t.id" does not exist)` | rewrite as join or per-row update | P2 | M | V | — |
| PG-Q-016 | `MERGE` (and PG 17's `MERGE ... RETURNING`, `WHEN NOT MATCHED BY SOURCE`) | `42601 unsupported statement "merge"` | `INSERT ... ON CONFLICT DO UPDATE`; `UPSERT` | P2 | L | V | — |
| PG-Q-035 | `COPY FROM` `DEFAULT` (16) and `ON_ERROR` (17); `ENCODING` other than UTF8 / SQL_ASCII (no conversion); `FREEZE` is accepted and ignored | `42601`; `0A000` | convert the file to UTF8 first | P3 | S | D | — |
| PG-Q-020 | `COPY FROM` is not atomic (commits in 128-row / 1 MiB chunks) and is refused inside `BEGIN` | `25001 COPY FROM cannot run inside a transaction block`; silently different on failure (earlier chunks stay) | load into a staging table, then `INSERT ... SELECT` | P2 | L | V | — |
| PG-Q-021 | SQL-level `PREPARE` / `EXECUTE` / `DEALLOCATE`; cursors: `DECLARE`, `FETCH`, `MOVE`, `CLOSE`, `WITH HOLD` (psycopg named cursors, some reporting tools) | `42601 unsupported statement "prepare" / "declare"` | protocol-level prepare and portal suspension (both work) | P2 | M | V | — |
| PG-Q-022 | `EXPLAIN` options (`FORMAT JSON`, `VERBOSE`, `BUFFERS`, `COSTS`, `SETTINGS`, `GENERIC_PLAN`), `EXPLAIN` of `INSERT`/`UPDATE`/`DELETE`, a plan tree | `42601 expected SELECT, VALUES or a parenthesized query, found "format"`; `0A000 EXPLAIN supports table SELECT statements only` | plain `EXPLAIN` / `EXPLAIN ANALYZE` of a `SELECT` (one line per stage) | P2 | M | V | — |
| PG-Q-036 | Collations other than C / POSIX / default / ucs_basic (`"en_US"`, ICU): text compares bytewise | `42704 collation "x" for encoding "UTF8" does not exist` | sort client-side, or `ORDER BY lower(x)` | P3 | M | D | — |
| PG-Q-025 | `WITH ... AS [NOT] MATERIALIZED`; `SEARCH` and `CYCLE` clauses of recursive CTEs | `42601 expected "(", found "materialized"`; `42601 unsupported statement "search" / "cycle"` | drop the keyword; track depth by hand | P3 | S | V | — |
| PG-Q-026 | `SELECT ... INTO new_table` | `42601 SELECT ... INTO is not supported: use CREATE TABLE ... AS SELECT` | `CREATE TABLE ... AS` | P3 | S | V | — |
| PG-Q-027 | `FETCH FIRST n ROWS WITH TIES`; `TABLESAMPLE` | `42601 expected ONLY in FETCH FIRST, found "with"`; `42601 unexpected "system"` | rank in a derived table; `random()` filter | P3 | S | V | — |
| PG-Q-028 | Operators and predicates: `OVERLAPS`, `IS [NOT] JSON` (16), `^@` (starts-with) | `42601 expected ")", found ","`; `42601 expected NULL, TRUE, FALSE or DISTINCT FROM after IS`; `42601 unexpected character '@'` | explicit comparisons; `starts_with()` | P3 | S | V | — |
| PG-Q-029 | `ON CONFLICT (cols) WHERE predicate` (partial unique index arbiter); `DO UPDATE` changing a primary-key column | `42601 expected DO NOTHING or DO UPDATE after ON CONFLICT, found "WHERE"`; key change refused (see user doc) | `ON CONFLICT ON CONSTRAINT name` (partial indexes exist since #358) | P3 | S | V | — |
| PG-Q-030 | A NOTICE for `IF EXISTS` / `IF NOT EXISTS` no-ops (`DROP TABLE IF EXISTS nosuch`) | silently different: no NOTICE (PostgreSQL: `table "nosuch" does not exist, skipping`) | — | P3 | S | V | — |
| PG-Q-032 | **DML and aggregate leftovers**: `UPDATE ... FROM` takes the first joined row for a target row with several matches (PostgreSQL leaves it unspecified); `EXPLAIN` of `UPDATE` / `DELETE`; a duplicate table name in a `FROM` list is `42P07` (PostgreSQL `42712`); an aggregate `ORDER BY` on a `jsonb` value is `0A000` (jsonb has no ordering), so `jsonb_agg(DISTINCT j)` keeps input order; `sum` / `avg` over floats ignore the aggregate `ORDER BY`; `string_agg`'s separator must be a constant or parameter | `0A000`; silently different | — | P3 | S | V | — |
| PG-Q-033 | **Join, correlation and COPY leftovers**: no hash or merge join, so a join with no usable equality is a nested loop (outer × inner rows); `SELECT * ... ORDER BY 1` over a join fails; a correlated scalar subquery column describes as `text`; correlated subqueries in a window-function select, `ORDER BY` one over a join or with `DISTINCT ON` / `FOR UPDATE`, and outer references inside aggregate arguments or `FILTER` are `0A000`; `SELECT 1/0 WHERE false` returns no row (PostgreSQL folds and raises); `COPY FROM` takes only the `FORMAT` option; `COPY` `ENCODING` takes only UTF8 / SQL_ASCII; `E''` strings lack `\b`, `\f`, `\xHH` | `0A000`; slow plans | add an equality to large joins | P3 | M | V | — |

### Functions and operators

Compared with `SHOW FUNCTIONS` (141 functions, the source of
[functions.md](user/functions.md)) and with what calls succeeded on the
node. `SHOW FUNCTIONS` does not list aggregates, window functions or the
catalog helpers (`pg_typeof`, `format_type`, `pg_get_indexdef`, ...),
which do exist: see PG-F-022.

| ID | Gap | Today | Workaround | P | Size | Src | Issue |
|---|---|---|---|---|---|---|---|
| PG-F-025 | Regular-expression back references and lookaround (patterns run on RE2) | `0A000` | rewrite the pattern, or match client-side | P3 | M | D | — |
| PG-F-002 | **Set-returning function leftovers**: `generate_subscripts`, `string_to_table`, `pg_get_keywords` | `42883` | `generate_series`, `regexp_split_to_table`, `unnest` (#339) | P3 | S | D | — |
| PG-F-003 | **JSON processing leftovers**: `jsonb_to_record[set]`, `jsonb_populate_record[set]`, `jsonb_set_lax`, `jsonb_object`, `array_to_json`, `row_to_json` (no row type); `jsonb_each` returns keys in stored order, not PostgreSQL's length-then-bytes order | `42883`; silently different order | `jsonb_each`, `jsonb_array_elements`, `jsonb_object_keys`, `jsonb_insert` (#339) | P2 | M | D | — |
| PG-F-006 | **Date/time function leftovers**: `date_bin`, `date_add` / `date_subtract` (16), `make_timestamptz(..., zone)` (and it ignores the session zone), `timeofday`, `to_char(interval, ...)`, `to_number` | `42883 unknown function "date_bin"`; `42601 make_timestamptz() takes 6 argument(s), got 7`; `42883 function does not exist for argument type INTERVAL` | `date_trunc`; `AT TIME ZONE` (#340) | P2 | M | V | — |
| PG-F-026 | `setseed` (no per-session random generator); `scale(1.50)` is 1, PostgreSQL says 2 | `42883`; silently different | — | P3 | S | D | — |
| PG-F-027 | Bitwise operators on `int2` / `int4` compute in `int8`: `1 << 31` on an `int4` is 2147483648, PostgreSQL wraps to -2147483648 | silently different | cast the result | P3 | S | D | — |
| PG-F-028 | `range_agg`, `xmlagg` (no range or xml types) | `42883` | — | P3 | S | D | — |
| PG-F-014 | **SQL/JSON**: the `jsonpath` type, `@?`, `@@`, `jsonb_path_query[_array\|_first]`, `jsonb_path_exists`, `jsonb_path_match`; SQL/JSON constructors and query functions `JSON_OBJECT`, `JSON_ARRAY`, `JSON_VALUE`, `JSON_QUERY`, `JSON_EXISTS`, `JSON_TABLE` (17), `JSON_ARRAYAGG` / `JSON_OBJECTAGG` (16), `json_scalar`, `json_serialize` | `42601 unexpected character '@'`; `42883 unknown function "jsonb_path_query"`; `42601 expected ")", found ":"` | `->` / `@>` / `?` | P2 | L | V | — |
| PG-F-015 | **Size functions return NULL**: `pg_table_size`, `pg_total_relation_size`, `pg_relation_size`, `pg_database_size`; `pg_indexes_size` and `pg_column_size` do not exist | silently different (NULL; psql's `\dt+`, `\l+`, `\di+` show blank sizes); `42883` for the last two | `datax debug` ranges, the console's data view | P2 | M | V | — |
| PG-F-016 | **Integer typing**: integer literals and `INT4` expressions are `int8` (`pg_typeof(1)` and `pg_typeof(int4_col)` are `int8`), so `2147483647::int4 + 1` is `2147483648` where PostgreSQL raises `22003`; `to_regtype('int4')` and `to_regtype('integer')` return `20` (int8's OID; PostgreSQL returns `integer`) | silently different | cast results; the column's own check still raises `22003` on store | P2 | M | V | — |
| PG-F-017 | **More system functions**: `inet_server_addr` / `inet_client_addr` / `inet_server_port`, `pg_postmaster_start_time`, `pg_conf_load_time`, `txid_current` / `pg_current_xact_id` / snapshot functions, `pg_reload_conf`, `pg_get_expr` outside a catalog query, `to_regproc` / `to_regnamespace` / `to_regrole`, `has_function_privilege`, `pg_collation_for`, `pg_input_is_valid` (16), `pg_listening_channels`, `pg_notification_queue_usage`, `num_nulls` / `num_nonnulls`, `current_query`, `pg_blocking_pids`, `pg_stat_reset`, `row_security_active`, `pg_partition_tree`, `pg_index_column_has_property`, `pg_options_to_table` | `42883 unknown function "..."` | — | P2 | M | V | — |
| PG-F-020 | Named-argument calls `f(x => 1)` | `42601 expected ")", found "="` | positional arguments | P3 | S | V | — |
| PG-F-021 | Ordered-set and hypothetical-set aggregates beyond scalar `percentile_cont` / `percentile_disc`: `percentile_*` over an array of fractions, `rank` / `dense_rank` / `percent_rank` / `cume_dist` `WITHIN GROUP` | `42601 RANK() requires an OVER clause`; `XX000 empty expression` | window functions | P3 | M | V | — |
| PG-F-022 | `SHOW FUNCTIONS` and `docs/user/functions.md` list 141 scalar functions but no aggregates, window functions or catalog functions, which exist | silently incomplete | `pg_proc` (173 rows) | P3 | S | V | — |
| PG-F-023 | Full-text functions not provided (`ts_debug`, `ts_stat`, `ts_lexize`, `querytree`, `array_to_tsvector`, `ts_rewrite`, `ts_filter`, `tsquery_phrase`, ...) — already listed in the user doc | `42883 unknown function "ts_debug"` | — | P3 | M | V | — |
| PG-F-024 | **System-function leftovers**: `has_language_privilege`, `has_parameter_privilege`; no column grants (a column's privilege is its table's); unnamed results of `SELECT user` etc. are `?column?` where PostgreSQL names them; `to_reg*` show OIDs as numbers; only the object argument of the privilege functions may be a column | `42883`; silently different | — | P3 | S | V | — |

### Types

| ID | Gap | Today | Workaround | P | Size | Src | Issue |
|---|---|---|---|---|---|---|---|
| PG-T-031 | `json_agg` / `jsonb_agg` render a `timestamptz` in UTC; zone abbreviations are a fixed list (no `timezone_abbreviations`, none DST-dependent); an invalid startup setting is ignored where PostgreSQL ends the connection | silently different | `to_jsonb` per row | P3 | S | D | — |
| PG-T-004 | `REAL` / `FLOAT4` columns are `float8` (`pg_typeof(real_col)` is `float8`) | silently different (precision and wire type differ) | — | P2 | M | V | — |
| PG-T-005 | `infinity` / `-infinity` dates and timestamps; BC dates; years outside 1678–2261; special inputs `'epoch'`, `'today'`, `'now'`, `'tomorrow'` | `22007 invalid input syntax for type date: "infinity"` (and likewise); a binary-format parameter or COPY field outside the range, PostgreSQL's infinity encodings included, is `22008` | sentinel values; `now()` | P2 | M | V | — |
| PG-T-006 | `TIMESTAMP` (without time zone) expressions and casts become `timestamptz`; `date + interval` is `timestamptz` (PostgreSQL: `timestamp`); a bind parameter for a `TIMESTAMP` column describes as `timestamptz` | silently different (wire type) | pass UTC | P2 | M | V | — |
| PG-T-007 | Arrays: multidimensional (`ARRAY[[1,2],[3,4]]`, `INT8[][]` is one-dimensional), slices `a[1:2]`, arrays in keys or btree indexes, GIN over arrays, arrays of `JSONB`, enums or vectors | `42601 expected value, found "["`; `42601 array slices (v[i:j]) are not supported`; `42601 JSONB[] is not supported`; `42601 arrays of enum type mood are not supported` | `JSONB` arrays | P2 | M | V | — |
| PG-T-008 | Composite (row) types: `CREATE TYPE ... AS (...)`, row-typed columns, `ROW(...)` values | `42601 CREATE TYPE supports AS ENUM only, found "("` | `JSONB` | P2 | L | V | — |
| PG-T-009 | Domains: `CREATE DOMAIN` | `42601 expected TABLE, found "domain"` | `CHECK` on each column | P2 | M | V | — |
| PG-T-010 | Range and multirange types (`int4range`, `tstzrange`, `daterange`, ..., operators `@>`, `&&`, `-\|-`, functions `lower`, `upper`, `isempty`) | columns `42704 type "tstzrange" does not exist`; `42883 unknown function "int4range"`; `'{[1,3)}'::int4multirange` is `42704` (since #343) | two columns | P2 | L | V | — |
| PG-T-011 | Network types `inet`, `cidr`, `macaddr`, `macaddr8`, their operators (`<<`, `>>`, `&&`, `+`) and functions (`host`, `network`, `masklen`, ...) | `42704 type "inet" does not exist` (casts too, since #343); `42883 unknown function "host"` | `TEXT` with `CHECK` | P2 | M | V | — |
| PG-T-032 | reg* values describe as `int8` on the wire and `pg_typeof` says `bigint`; reg casts of a column other than `::regclass` pass the number through; index and constraint OIDs (`relOID<<16 \| n`) pass 32 bits once table IDs reach 65536 | silently different | `::regclass` on a column | P2 | M | D | — |
| PG-T-014 | Geometric types (`point`, `line`, `lseg`, `box`, `path`, `polygon`, `circle`) and operators | `42704 type "point" does not exist`; `42883 unknown function "point"` | two `float8` columns; PostGIS is PG-E-002 | P3 | L | V | — |
| PG-T-015 | Bit strings `BIT(n)`, `BIT VARYING`, `B'1010'` and bit operators | `42601 expected ")", found "("`; `42601 unexpected character '&'` | `INT8` or `BYTEA` | P3 | M | V | — |
| PG-T-016 | `money` | `42704 type "money" does not exist`, for columns and casts | `DECIMAL(19,2)` | P3 | S | V | — |
| PG-T-017 | `xml` and the XML functions (`xmlparse`, `xpath`, `xmlelement`, `xmlagg`, ...) | `42704 type "xml" does not exist`, for columns and casts; `42601` / `42883` for the functions | `TEXT` | P3 | L | V | — |
| PG-T-018 | `TIME WITH TIME ZONE` (`timetz`) | column `42601 TIME WITH TIME ZONE is not supported`; `'12:00:00+02'::timetz` silently drops the offset (`12:00:00`) | `TIME` + offset column, or `TIMESTAMPTZ` | P3 | M | V | — |
| PG-T-019 | Interval: `IntervalStyle` other than `postgres` (it is not even a setting); field qualifiers in a literal (`INTERVAL '1 2:03:04' DAY TO SECOND`); qualifiers on a column are not enforced | `42704 unrecognized configuration parameter "intervalstyle"`; `42601 unexpected "TO" after statement` | — | P3 | S | V | — |
| PG-T-020 | `json` is stored and returned as `jsonb` (key order, whitespace and duplicate keys are not kept: `'{"b":1,  "a":2, "a":3}'` returns `{"a":3,"b":1}`) | silently different | — | P3 | S | V | — |
| PG-T-021 | Enums: `ADD VALUE BEFORE` / `AFTER`, `RENAME VALUE`, `RENAME TO`; `pg_typeof('ok'::mood)` is `text`; `DROP TYPE ... CASCADE` does not drop columns | `42601 ADD VALUE BEFORE / AFTER is not supported` | recreate the type | P3 | M | V | — |
| PG-T-022 | `pg_lsn`, `xid`, `xid8`, `tid`, `txid_snapshot`, `pg_snapshot` as types | cast to `pg_lsn` is `42704` (since #343); no columns | — | P3 | S | V | — |
| PG-T-023 | Type modifiers: an invalid typmod is `42601` (PostgreSQL `22023`); modifiers on other types (`FLOAT8(3)`) accepted and ignored | as stated | — | P3 | S | D | — |
| PG-T-024 | `CHAR(n)` is stored trimmed, so `c = 'ab  '` compares the literal with its spaces (PostgreSQL trims both sides) | silently different | trim the literal | P3 | S | D | — |
| PG-T-025 | `tsvector` / `tsquery`: own parser and stemmer, no ordering, no arrays of them, `english` and `simple` only, `regconfig` is text (detailed in the user doc) | as in the user doc | — | P3 | L | D | — |
| PG-T-026 | Expression results lose their declared typmod and describe as the family (`DECIMAL(10,2)` arithmetic is `numeric`; `a + 1` over `INT4` is `INT8`) | silently different (Describe output) | cast | P3 | M | D | — |
| PG-T-027 | `"char"`, `name` and other internal types behave as `text` | silently different | — | P3 | S | V | — |
| PG-T-028 | **Cast leftovers**: a schema qualifier on a cast type is ignored (`1::foo.int8` is accepted, PostgreSQL `3F000`); over the extended protocol an unknown cast type fails at Execute, not Parse; casts written in row-level security policy text are not checked when the policy is created; `foo 'lit'` for an unknown `foo` is `42601`, not `42704`; `json` and `jsonb` are one type, so `json \|\| json` merges where PostgreSQL refuses it (`42883`): a deliberate difference, kept | silently different, or a different SQLSTATE | — | P3 | S | V | — |
| PG-T-030 | **Identifier-case leftovers**: quoted role names are still folded to lower case; CTE and alias names are lower-cased in some executor paths; arrays of enums are not supported; `'role'::regtype` passes when only `"Role"` exists (PostgreSQL `42704`); an unquoted `nextval('Foo')` is not folded; `'idx'::regclass` does not resolve index names; `pg_get_expr` renders an enum default without its `::"Role"` cast | silently different; `0A000` | lower-case role names | P3 | S | V | — |

### DDL

| ID | Gap | Today | Workaround | P | Size | Src | Issue |
|---|---|---|---|---|---|---|---|
| PG-D-034 | **Temporary table leftovers**: `SERIAL` / identity columns on a temp table (no temp sequences); temporary views (a view over a temp table is refused, PostgreSQL makes it temporary); `WITH (...)` / `SET (...)` on a temp table; `CREATE TEMP TABLE ... AS` inside a transaction block (`25001`, as all CTAS); `ON COMMIT DELETE ROWS` deletes row by row and skips PostgreSQL's ON COMMIT / foreign-key combination check; `GLOBAL` raises no deprecation warning; `DROP OWNED` skips temp tables and `REASSIGN OWNED` also reassigns other sessions' temp tables; a node cut off for longer than `DeadNodeThreshold` can have its live sessions' temp tables swept (those sessions then get `42P01`) | `0A000`; `25001`; silently different | ordinary tables with session-unique names | P2 | M | V | — |
| PG-D-036 | **Schema leftovers**: temp tables are listed in `public`, not a per-session `pg_temp_N`; `CREATE SCHEMA ... CREATE TABLE ...` schema elements; `ALTER DEFAULT PRIVILEGES IN SCHEMA` for schemas other than `public`; renaming a schema, or moving a relation, that a view reads is refused (`2BP01`); `RESTORE TABLE` of a table outside `public`; pg_dump round-trips of databases with schemas are untested; `public` without `USAGE` is `42501` rather than skipped; a real table shadows a `pg_catalog` name; `s.t.c` binds by table name, so same-named tables from two schemas in one query need aliases, and same-named views from two schemas in one statement are `0A000`; an empty `search_path` still resolves through `public` (for pg_dump restores); the two-part `db.t` fallback is deprecated; a view rewritten by a pre-v22 binary in a mixed-version cluster loses its creation-time bindings | `2BP01`; `0A000`; `42501`; silently different | qualify names; alias tables | P3 | M | V | — |
| PG-D-037 | **Schema sequence and type leftovers**: an unqualified relation name checks the path's tables before its sequences, so a sequence in an earlier schema does not hide a later table of that name, and `nextval` can find a sequence behind a same-named table where PostgreSQL gives `42809`; `ALTER TABLE ... DROP COLUMN` of a `SERIAL` column drops its sequence even when another table's default is bound to it (that default then fails with `42P01`); `DROP SCHEMA ... CASCADE` refuses (`2BP01`) a type a column in another schema uses, where PostgreSQL drops the column; a view, default or CHECK naming a sequence or type blocks moving it (`2BP01`), where PostgreSQL's OID binding lets it move; `SHOW CREATE TABLE` prints an enum type from another schema unqualified; `RESTORE TABLE` of a `public` table whose enum type is in another schema maps it onto a `public` type of that name and labels, and a restored default bound to a sequence the restore did not bring back resolves in `public` | `42809` missing; `2BP01`; silently different | qualify names; drop dependents first | P3 | M | V | — |
| PG-D-010 | Generated columns `GENERATED ALWAYS AS (expr) STORED` (and 18's `VIRTUAL`) | `42601 expected IDENTITY, found "("` | compute in the application | P2 | M | V | — |
| PG-D-011 | `DEFERRABLE` / `INITIALLY DEFERRED` constraints and `SET CONSTRAINTS` | `42601 expected ")", found "deferrable"`; `42601 expected = or TO after SET constraints` | order the writes | P2 | M | V | — |
| PG-D-012 | `ADD PRIMARY KEY`, dropping or changing the primary key | `42601 ADD PRIMARY KEY is not supported: the primary key is fixed at CREATE TABLE` | recreate the table | P2 | L | V | — |
| PG-D-013 | Covering indexes `INCLUDE (...)` | `42601 unexpected "include" after statement` | add the columns to the key | P2 | M | V | — |
| PG-D-015 | Index access methods `hash`, `gist`, `spgist`, `brin`; GIN on anything but `JSONB` / `tsvector`; multi-column or unique GIN | `0A000 access method "hash" is not supported (btree, gin and ivfflat are)`; `0A000 column "arr" of type INT8[] cannot have a GIN index` | btree | P2 | L | V | — |
| PG-D-016 | Exclusion constraints (`EXCLUDE USING gist`), `ON DELETE SET DEFAULT`, `MATCH FULL` | `42601 expected ")", found "gist"`; `42601 only ON DELETE/UPDATE SET NULL is supported`; `42601 only MATCH SIMPLE is supported` | application checks | P2 | M | V | — |
| PG-D-017 | `ALTER COLUMN ... TYPE ... USING expr` | `42601 ALTER COLUMN TYPE ... USING is not supported` | add a column, backfill, swap | P2 | M | V | — |
| PG-D-018 | Materialized views (`CREATE`, `REFRESH [CONCURRENTLY]`) | `42601 expected TABLE, found "materialized"` | a table refreshed by a job | P2 | L | V | — |
| PG-D-019 | Views: updatable views, `WITH CHECK OPTION`, `CREATE RECURSIVE VIEW`, `ALTER VIEW ... RENAME` and other forms, `security_invoker`; renaming a column a view reads | `42809 cannot modify view "vup": views are read-only`; `42601 unexpected "with"`; `42601 ALTER VIEW supports only OWNER TO`; `2BP01 cannot rename column v: view "vup" depends on table "u"` | `CREATE OR REPLACE VIEW`; write the base table | P2 | M | V | — |
| PG-D-020 | Declarative partitioning (`PARTITION BY RANGE / LIST / HASH`, `ATTACH` / `DETACH PARTITION`) | `42601 unexpected "partition" after statement` | ranges split automatically; `SPLIT AT` | P2 | XL | V | — |
| PG-D-021 | `ALTER TABLE` forms: `ENABLE` / `DISABLE TRIGGER` (Rails fixtures use `DISABLE TRIGGER ALL`), `REPLICA IDENTITY`, `ALTER COLUMN SET STATISTICS` / `STORAGE` / `COMPRESSION`, `CLUSTER ON`, `INHERIT`, `OF type` | `42601 expected ADD, DROP, RENAME, ALTER COLUMN, VALIDATE CONSTRAINT, SET, RESET, PURGE, ROTATE ENCRYPTION KEY, SPLIT AT or UNSPLIT, found ...` | — | P2 | M | V | — |
| PG-D-033 | `ALTER DATABASE ... SET param FROM CURRENT`; `pg_db_role_setting` / `\drds` do not show database defaults; `LC_COLLATE` is reported, not applied | `42601`; silently different | — | P3 | S | D | — |
| PG-D-023 | Inheritance (`INHERITS`) | `42601 expected identifier, found ")"` | — | P3 | L | V | — |
| PG-D-024 | Tablespaces (`TABLESPACE` clause, `CREATE TABLESPACE`) | `42601 unexpected "tablespace" after statement` | omit; placement is per database | P3 | S | V | — |
| PG-D-025 | Storage parameters (`WITH (fillfactor = 70)`, `autovacuum_*`, `toast.*`) | `42601 unknown table option "fillfactor"` | omit | P3 | S | V | — |
| PG-D-026 | `COMMENT ON` objects other than tables, views, indexes, columns and constraints: `SEQUENCE`, `TYPE`, `DATABASE`, `SCHEMA`, `ROLE`, `FUNCTION`, `EXTENSION`, ... | `42601 COMMENT ON supports TABLE, VIEW, INDEX, COLUMN and CONSTRAINT, found "sequence"` | — | P3 | S | V | — |
| PG-D-027 | `CREATE COLLATION`, `CREATE CAST`, `CREATE OPERATOR [CLASS \| FAMILY]`, `CREATE AGGREGATE`, `CREATE STATISTICS`, `SECURITY LABEL` | `42601 expected TABLE, found "collation"` (and likewise); `42601 unsupported statement "security"` | — | P3 | L | V | — |
| PG-D-028 | `RENAME TO` across databases | refused (user doc) | recreate | P3 | S | D | — |
| PG-D-029 | **Index leftovers**: pattern operator classes are accepted but do not make `LIKE 'abc%'` use the index; `pg_index.indclass` / `indcollation` are not modelled and `pg_opclass` is empty; `SHOW CREATE TABLE` does not print operator classes or collations; a duplicate index name is `42710` (PostgreSQL `42P07`); a quoted `"concurrently"` index name is read as the keyword; `CREATE INDEX` in a transaction block is refused (`25001`) unless the block created the table, since the online build spans transactions | silently different, or `25001 CREATE INDEX cannot run inside a transaction block` | build the index outside the block | P2 | M | V | — |
| PG-D-030 | **Index-form and key leftovers**: the planner uses a partial index only when the query repeats the predicate's conjuncts (no implication, no `OR`, no parameter for a literal); an expression index serves `=` and ranges, not `ORDER BY`, `IN` or `LIKE`; zone-dependent expressions (`date_trunc`, timestamp casts) are refused in index expressions even over `timestamp without time zone`; `pg_get_indexdef` prints expressions as written, without PostgreSQL's casts; `DESC` keys are stored ascending, so a mixed-direction `ORDER BY` is sorted; `ALTER TABLE ... ADD PRIMARY KEY` on a table created without one is refused; `ADD COLUMN rowid` on such a table is `42710` | `0A000`; `42P17`; a sort instead of an index order | — | P2 | M | V | — |
| PG-D-031 | **`ALTER TABLE` leftovers**: a volatile `ADD COLUMN` default and every action that reads rows are refused inside a transaction block (`25001`), even on a table the block created (Rails needs `disable_ddl_transaction!`); a concurrent `INSERT` during `ADD COLUMN ... NOT NULL` without a default fails `23502` instead of waiting; an interrupted online `ALTER` can only be finished by re-running it; `DROP DATABASE ... CASCADE` against concurrent `CREATE TABLE` in that database keeps losing (each try runs for tens of seconds before its `40001`) where PostgreSQL's exclusive lock would make the creators wait; `ADD PRIMARY KEY` on a table created without one keeps the hidden rowid as the key and shows a unique index as the primary key | `25001`; `23502` | run the statement outside a block | P4 | M | V | — |

### Programmability

| ID | Gap | Today | Workaround | P | Size | Src | Issue |
|---|---|---|---|---|---|---|---|
| PG-P-001 | **User-defined functions** (`CREATE FUNCTION ... LANGUAGE sql / plpgsql`, `SECURITY DEFINER`, `RETURNS TABLE`, `SETOF`), `pg_proc` entries for them, `\df`. Design: docs/design/functions-and-triggers.md | `42601 expected TABLE, found "function"` | application code | P1 | XL | V | — |
| PG-P-002 | **Triggers** (`CREATE TRIGGER`, row and statement, `BEFORE` / `AFTER` / `INSTEAD OF`, transition tables) — `updated_at` and audit triggers, many schema generators. Design: docs/design/functions-and-triggers.md | `42601 expected TABLE, found "trigger"` | application code; the `datax_audit` table for auditing | P1 | XL | V | — |
| PG-P-003 | **PL/pgSQL** and `DO` blocks (migration scripts use `DO $$ BEGIN IF NOT EXISTS ... END $$` for idempotent DDL) | `42601 invalid parameter at position 3` | `IF NOT EXISTS` forms (widely supported here) | P2 | XL | V | — |
| PG-P-004 | Procedures and `CALL` | `42601 invalid parameter at position 38`; `42601 unsupported statement "call"` | — | P2 | L | V | — |
| PG-P-006 | Event triggers | `42601 expected TABLE, found "event"` (`\dy` lists none) | — | P3 | L | V | — |
| PG-P-007 | Rules (`CREATE RULE`) | `42601 expected TABLE, found "rule"` | — | P3 | M | V | — |

### Transactions and concurrency

| ID | Gap | Today | Workaround | P | Size | Src | Issue |
|---|---|---|---|---|---|---|---|
| PG-X-001 | **`READ COMMITTED` and `REPEATABLE READ`**: every transaction is serializable and can fail with `40001`; `SET TRANSACTION ISOLATION LEVEL READ COMMITTED` is accepted and `SHOW transaction_isolation` says `serializable`; `default_transaction_isolation` accepts every level but `SHOW` reports `serializable`. Design: docs/design/read-committed.md | silently different (stronger isolation, retries needed) | a retry loop around every transaction | P1 | XL | V | — |
| PG-X-004 | `FOR SHARE`, `FOR NO KEY UPDATE`, `FOR KEY SHARE`, `FOR UPDATE OF table` | `42601 expected UPDATE, found "share" / "no"`; `42601 unexpected "of"` | `FOR UPDATE` | P2 | M | V | — |
| PG-X-005 | `LOCK TABLE` | `42601 unsupported statement "lock"` | — | P2 | M | V | — |
| PG-X-006 | **datax's online statements run in transactions of their own**: `CREATE INDEX` on a table the block did not create, the `ALTER TABLE` actions that read the rows (`ADD CONSTRAINT`, `VALIDATE CONSTRAINT`, `SET NOT NULL`, `ALTER COLUMN TYPE`, `ADD COLUMN` with a volatile default, a constraint or `SERIAL`) or work outside a transaction (`SET (...)`, `RESET`, `SPLIT AT`, `UNSPLIT`, `PURGE`, `ROTATE ENCRYPTION KEY`), `CREATE TABLE ... AS`, `ANALYZE`, `AS OF SYSTEM TIME`, the encryption-key statements and `SET CLUSTER SETTING`. Refused inside `BEGIN`; in the implicit transaction of a multi-statement query or a pipeline (a documented difference, not to be closed) each commits the statements before it and runs alone, and the statements after it are a new implicit transaction, where PostgreSQL keeps the whole message atomic | `25001` in a block; silently less atomic in a multi-statement query or pipeline | run outside `BEGIN` | P2 | L | V | — |
| PG-X-007 | Concurrent inserts of one key may lose with `40001` instead of waiting and failing `23505`; `SKIP LOCKED` skips rows committed after the snapshot; `NOWAIT` can fail on a row it would have filtered (user doc) | silently different | retry `40001` | P2 | L | D | — |
| PG-X-008 | `SET CONSTRAINTS` (needs PG-D-011) | `42601` | — | P2 | S | V | — |
| PG-X-009 | Prepared transactions (two-phase commit): `PREPARE TRANSACTION`, `COMMIT PREPARED`, `pg_prepared_xacts` | `42601 unsupported statement "prepare"`; `42P01` for the view | — | P3 | L | V | — |
| PG-X-010 | `DEFERRABLE` transactions (`SERIALIZABLE READ ONLY DEFERRABLE`) | accepted and ignored (`BEGIN ... DEFERRABLE` parses since #346) | `AS OF SYSTEM TIME` reads | P3 | S | V | — |
| PG-X-011 | Snapshot export and import (`pg_export_snapshot`, `SET TRANSACTION SNAPSHOT`; parallel pg_dump) | `42883 unknown function "pg_export_snapshot"` | `AS OF SYSTEM TIME` at a chosen timestamp | P3 | M | V | — |
| PG-X-012 | Unique keys are checked at the end of an `UPDATE` (so `UPDATE t SET id = id + 1` succeeds where PostgreSQL fails `23505`); a self-referencing `ON UPDATE CASCADE` that overlaps its statement is refused `0A000` | silently different (more permissive) / `0A000` | split the statement | P3 | M | D | — |
| PG-X-013 | **Transaction-statement leftovers**: `SET TRANSACTION` with both an access mode and an isolation level applies only the access mode (datax is serializable anyway); `COMMIT WORK` / `ROLLBACK WORK` are syntax errors; `BEGIN` inside a block is an error where PostgreSQL warns; read-only refusals of DDL say "DDL" rather than the command; `nextval()` on a temporary sequence is refused in a read-only transaction | `42601`; `25001` | `COMMIT` / `ROLLBACK` | P3 | S | V | — |
| PG-X-014 | **Advisory-lock leftovers**: the arguments must be constants or parameters (`SELECT pg_try_advisory_lock(id) FROM jobs`, the Que style, is `0A000`); waiters are not served in arrival order; deadlocks between advisory locks are not detected (they end at `lock_timeout` / `statement_timeout`); `pg_locks` lists granted advisory locks only | `0A000 ...`; a timeout instead of `40P01` | lock in a loop over ids | P2 | M | V | — |

### Security

| ID | Gap | Today | Workaround | P | Size | Src | Issue |
|---|---|---|---|---|---|---|---|
| PG-S-011 | `CREATEROLE` grants nothing; `CONNECTION LIMIT` counts per node, not cluster-wide; `VALID UNTIL 'infinity'` shows as NULL | silently different | an admin manages roles | P3 | S | D | — |
| PG-S-002 | Per-role and per-database setting defaults: `ALTER ROLE ... SET`, `ALTER DATABASE ... SET`, `pg_db_role_setting` (empty) | `42601 unexpected "SET" in role options` | set at connect | P2 | M | V | — |
| PG-S-003 | Column-level privileges (`GRANT SELECT (col) ON t`) | `42601 expected ON, found "("` | a view (definer's rights) | P2 | M | V | — |
| PG-S-004 | Row-level security differences: no subqueries in a policy, no `security_invoker` views, `row_security` not a setting, admins exempt like owners, `TRUNCATE` refused to a filtered role, policies on views (user doc) | `0A000 USING: subqueries are not supported in a policy expression`; `42704 unrecognized configuration parameter "row_security"` | `current_setting` in policies | P2 | M | V | — |
| PG-S-005 | Authentication methods: no `pg_hba.conf` and no per-host / database / user rules; insecure mode is trust for every user; secure mode has SCRAM-SHA-256(-PLUS) and client certificates only — no `md5`, `password`, LDAP, GSSAPI / Kerberos, SSPI, RADIUS, PAM, ident / peer | not configurable (`SHOW hba_file`, `password_encryption`: `42704`) | client certificates | P2 | L | D | — |
| PG-S-006 | SSL modes: insecure mode answers `N` to `SSLRequest` (so `sslmode=require` fails) and secure mode refuses cleartext; TLS without the datax CA's client trust model is not offered; `ssl` and the `ssl_*` settings, `pg_stat_ssl` do not exist | connection refused; `42704 unrecognized configuration parameter "ssl"` | `sslmode=verify-full` with the datax CA | P2 | M | V | — |
| PG-S-007 | Privileges on functions, types, `LARGE OBJECT`, and the `TRIGGER`, `REFERENCES` and `MAINTAIN` (17) privileges | `42601 expected TO, found "ON"`; `42601 expected identifier, found "REFERENCES"` | — | P3 | S | V | — |
| PG-S-008 | `SET SESSION AUTHORIZATION` / `RESET SESSION AUTHORIZATION` | `42601 expected = or TO after SET authorization` | `SET ROLE` | P3 | S | V | — |
| PG-S-009 | Predefined roles (`pg_read_all_data`, `pg_write_all_data`, `pg_monitor`, `pg_signal_backend`, `pg_read_all_settings`, ...) | absent (datax has `admin`, `read_all`, `write_all`, `metrics`) | the datax roles | P3 | S | D | — |
| PG-S-010 | In insecure mode the username is whatever the client claims, so privileges are advisory | by design | secure mode | P3 | — | D | — |

### Extensions

datax has no extension mechanism. `CREATE EXTENSION vector` is accepted
because the type is built in; any other name is refused with `0A000
extension "<name>" is not available (datax has no extensions; "vector"
is accepted because its type and operators are built in)`.
`pg_extension` and `pg_available_extensions` are empty.

| ID | Gap | Today | Workaround | P | Size | Src | Issue |
|---|---|---|---|---|---|---|---|
| PG-E-002 | PostGIS: `geometry`, `geography`, `ST_*` functions, spatial indexes | `42704 type "geometry" does not exist`; `42883 unknown function "st_distance"` | — | P2 | XL | V | — |
| PG-E-003 | `pg_trgm`: `similarity`, `%`, `word_similarity`, `gin_trgm_ops` / `gist_trgm_ops` (fuzzy search, `LIKE '%x%'` acceleration) | `42883 unknown function "similarity"`; `42704 operator class "gin_trgm_ops" does not exist for access method "gin"` | full-text search | P2 | L | V | — |
| PG-E-004 | `pgcrypto` functions (`digest`, `hmac`, `crypt` / `gen_salt`, `gen_random_bytes`, `pgp_sym_encrypt`) and `uuid-ossp` functions beyond `uuid_generate_v4` (`uuid_generate_v1`, `_v1mc`, `_v3`, `_v5`, `uuid_nil`, `uuid_ns_*`; 18's `uuidv7`); `CREATE EXTENSION` of both is accepted since #349 | `42883 unknown function "digest"` | `sha256`, `md5`; sealed tables for encryption | P2 | M | V | — |
| PG-E-005 | `citext` | `42704 type "citext" does not exist`, for columns and casts | `lower()` on both sides with an expression index `((lower(c)))` (since #359) | P2 | M | V | — |
| PG-E-006 | `pg_stat_statements` | `42P01 table "pg_stat_statements" does not exist` | the console's statement fingerprints and slow list | P2 | M | V | — |
| PG-E-007 | pgvector differences: no HNSW (`0A000`), no `halfvec` / `sparsevec` / `bit` vectors, no `avg` / `sum` over vectors, ivfflat serves one query shape, no `vector[]`, no btree on vectors, `ivfflat.probes` defaults to √lists (user doc) | `0A000` | ivfflat | P2 | L | D | — |
| PG-E-008 | `hstore` | `42704 type "hstore" does not exist`, for columns and casts | `JSONB` | P3 | M | V | — |
| PG-E-009 | `unaccent`, `fuzzystrmatch` (`levenshtein`, `soundex`, `metaphone`), `btree_gin` / `btree_gist`, `intarray`, `ltree`, `tablefunc` (`crosstab`), `pg_cron`, `pg_partman` | `42883 unknown function "unaccent"` (and likewise) | — | P3 | L | V | — |
| PG-E-010 | Foreign data wrappers: `postgres_fdw`, `dblink`, `file_fdw`; `CREATE SERVER`, `USER MAPPING`, `FOREIGN TABLE` (the `pg_foreign_*` catalogs are empty stubs) | `0A000` for the extension; `42601` for the DDL | — | P3 | XL | D | — |
| PG-E-011 | TimescaleDB-style features beyond datax's time-series tables (which have shards, retention and online re-shard): continuous aggregates, `time_bucket` (and PostgreSQL's own `date_bin`, PG-F-006), compression, gap filling | absent | `date_trunc` | P3 | L | D | — |

### Wire protocol and clients

What works: simple and extended protocol, text and binary formats,
portal suspension, `CopyIn` (text, CSV, binary), `CancelRequest`
(routed to the node that runs the query, #211), `LISTEN`/`NOTIFY` across
nodes, the fast-path call for large objects, SCRAM-SHA-256 and
SCRAM-SHA-256-PLUS with `tls-server-end-point` channel binding, and
`statement_timeout` (`57014`).

| ID | Gap | Today | Workaround | P | Size | Src | Issue |
|---|---|---|---|---|---|---|---|
| PG-W-001 | Replication protocol and logical decoding: a `replication=database` startup is accepted but `IDENTIFY_SYSTEM`, `CREATE_REPLICATION_SLOT`, `START_REPLICATION` are not; no `pgoutput`, `wal2json` or `test_decoding`; no physical streaming; `pg_replication_slots` missing (Debezium and other CDC tools) | `ERROR: syntax error: unsupported statement "identify_system"` | `LISTEN`/`NOTIFY` (not durable); polling with `AS OF SYSTEM TIME` | P2 | XL | V | — |
| PG-W-002 | `CopyOutResponse` / `CopyBothResponse` (COPY TO, PG-Q-018; COPY BOTH for replication) | as PG-Q-018 | — | P2 | M | V | — |
| PG-W-004 | Fast-path function calls other than the large-object functions | `0A000` | SQL calls | P3 | S | D | — |
| PG-W-005 | GSSAPI encryption (`GSSENCRequest`) | declined: psql `gssencmode=require` fails with "GSSAPI encryption required but was impossible" | TLS | P3 | M | V | — |
| PG-W-006 | Protocol version 3.2 (PostgreSQL 18: longer cancel keys) and `NegotiateProtocolVersion` | not offered | protocol 3.0 | P3 | S | D | — |
| PG-W-007 | Few NOTICEs: no `RAISE` (no PL/pgSQL), no "skipping" notices for `IF [NOT] EXISTS` no-ops (PG-Q-030) | silently different (no notice) | — | P3 | S | V | — |

### System catalogs and introspection

What exists (read-only virtual tables over the live schema, scoped to the
current database): `pg_database`, `pg_namespace`, `pg_class`,
`pg_attribute`, `pg_type`, `pg_index`, `pg_constraint`, `pg_attrdef`,
`pg_am`, `pg_roles`, `pg_user`, `pg_auth_members`, `pg_settings`,
`pg_tables`, `pg_views`, `pg_indexes`, `pg_collation`, `pg_tablespace`,
`pg_sequence`, `pg_sequences`, `pg_enum`, `pg_description`,
`pg_policy`, `pg_policies`, `pg_proc` (builtins, 173 rows),
`pg_stat_user_tables`, `pg_stat_activity`, `pg_largeobject_metadata`;
`information_schema.schemata`, `tables`, `views`, `columns`,
`table_constraints`, `check_constraints`, `referential_constraints`,
`constraint_column_usage`, `key_column_usage`, `statistics`,
`role_table_grants`. Empty stubs: `pg_extension`,
`pg_available_extensions`, `pg_statistic_ext`, `pg_publication[_rel]`,
`pg_subscription`, `pg_cast`, `pg_operator`, `pg_language`, `pg_opclass`,
`pg_opfamily`, `pg_amop`, `pg_amproc`, `pg_trigger`, `pg_rewrite`,
`pg_event_trigger`, `pg_depend`, `pg_inherits`, `pg_range`,
`pg_partitioned_table`, `pg_matviews`, `pg_shdescription`,
`pg_db_role_setting`, `pg_conversion`, `pg_foreign_*`, `pg_user_mappings`,
`pg_ts_*`. psql 16's `\d`, `\d+`, `\dt[+]`, `\di[+]`, `\dv`, `\ds`, `\dn`,
`\du`, `\dg`, `\dp`, `\dT`, `\dx`, `\dd`, `\dconfig`, `\sv`, `\l[+]` and
the empty list commands all render.

| ID | Gap | Today | Workaround | P | Size | Src | Issue |
|---|---|---|---|---|---|---|---|
| PG-C-015 | `information_schema` leftovers: its own domains (`cardinal_number`, ...) are not listed in `domains` / `usage_privileges`; `routines` lists no builtins; `sequences.data_type` is always `bigint`; `check_clause` is datax's stored text | silently different | — | P3 | S | D | — |
| PG-C-002 | `pg_stat_*` and `pg_statio_*` views: `pg_stat_database`, `pg_stat_all_tables` / `sys_tables`, `pg_stat_[user\|all]_indexes`, `pg_statio_user_tables`, `pg_stat_bgwriter`, `pg_stat_wal`, `pg_stat_io` (16), `pg_stat_replication`, `pg_stat_ssl`, `pg_stat_progress_*`, `pg_stat_user_functions`; `pg_stat_user_tables` has six columns (monitoring agents, `postgres_exporter`) | `42P01 table "pg_catalog.pg_stat_database" does not exist` | `/metrics`, the console | P2 | M | V | — |
| PG-C-003 | `pg_locks` (and `pg_blocking_pids`) | `42P01` | the console's sessions; `SHOW SESSIONS` | P2 | M | V | — |
| PG-C-004 | `pg_stats` / `pg_statistic` (and data in `pg_statistic_ext`): no histograms or most-common values | `42P01 table "pg_catalog.pg_stats" does not exist` | `SHOW STATS FOR t` | P2 | M | V | — |
| PG-C-005 | `pg_depend` is empty (tools use it for sequences owned by columns, view dependencies, extension membership) | silently empty | `pg_sequences`, `pg_attrdef` | P2 | M | V | — |
| PG-C-006 | `ANALYZE` is admin-only, takes no column list or options, stores row counts and distinct / NULL counts only | `42601 unexpected "(" after statement` for `ANALYZE u (v)` | `ANALYZE t` | P2 | M | V | — |
| PG-C-009 | `pg_prepared_statements`, `pg_cursors`, `pg_prepared_xacts` | `42P01` | — | P3 | S | V | — |
| PG-C-010 | `pg_file_settings`, `pg_hba_file_rules`, `pg_config`, `pg_authid`, `pg_shadow` | `42P01` | `pg_settings`, `pg_roles` | P3 | S | V | — |
| PG-C-011 | `pg_aggregate`, `pg_init_privs`, `pg_default_acl`, `pg_seclabel`, `pg_shdepend`, `pg_rules`, `pg_user_mapping`, `pg_largeobject` | `42P01` | `pg_proc`; `SHOW GRANTS` | P3 | S | V | — |
| PG-C-012 | psql oddities: `\dT` shows an enum's name as `-`; `\sf` prints nothing; the reserved tables `datax_audit`, `datax_largeobject*` are listed among the user's tables and indexes | silently different | `SHOW TYPES`-style queries on `pg_type` | P3 | S | V | — |
| PG-C-013 | `pg_cast`, `pg_operator`, `pg_language` are empty, so tools that discover casts or operators find none | silently empty | — | P3 | M | V | — |
| PG-C-014 | `pg_stat_activity` and `SHOW SESSIONS` list the serving node's sessions only (user doc) | silently partial | the console's sessions view for the cluster | P3 | M | D | — |

### Operations

| ID | Gap | Today | Workaround | P | Size | Src | Issue |
|---|---|---|---|---|---|---|---|
| PG-O-001 | **Settings leftovers** (the common settings were added in #345): `IntervalStyle` and `bytea_output` change only top-level text columns on the wire, not `::text`, arrays, records or JSON; `SHOW statement_timeout` (and the other timeouts) gives `5000` where PostgreSQL gives `5s`; error HINTs are missing; `pg_settings` has no `enumvals`, `min_val` or `max_val`; `lc_messages` and `session_replication_role` need no superuser | silently different | — | P3 | S | D | — |
| PG-O-004 | Logical replication: `CREATE PUBLICATION`, `CREATE SUBSCRIPTION` (the catalogs are empty stubs) | `42601` | backup / restore; LISTEN/NOTIFY | P2 | XL | V | — |
| PG-O-005 | Collations and ICU: the only ordering is bytewise (`'a' < 'B'` is false, as under `C`); no ICU or nondeterministic (case-insensitive) collations, no `CREATE COLLATION`, no `COLLATE` in expressions or indexes; `\l` shows `C` | silently different from an `en_US` database | sort client-side | P2 | L | V | — |
| PG-O-006 | Encodings: UTF8 only | `22023 invalid value for parameter "client_encoding": only UTF8 is supported` | convert client-side | P2 | M | V | — |
| PG-O-007 | `DateStyle` other than ISO; `IntervalStyle` | `22023 invalid value for parameter "DateStyle": only the ISO date style is supported` | ISO | P3 | S | V | — |
| PG-O-008 | `REINDEX` | `42601 unsupported statement "reindex"` | create a second index and drop the first | P3 | S | V | — |
| PG-O-009 | `CLUSTER` | `42601 unsupported statement "cluster"` | — (rows are stored in key order) | P3 | S | V | — |
| PG-O-010 | `CHECKPOINT` | `42601 unsupported statement "checkpoint"` | — | P3 | S | V | — |
| PG-O-011 | Server-side file access: `COPY FROM 'file'`, `pg_read_file`, `pg_ls_dir`, server-side `lo_import` / `lo_export` | `42883 unknown function "pg_read_file"` | client-side `\copy`, `\lo_import` | P3 | S | V | — |
| PG-O-012 | WAL-based tooling: `pg_basebackup`, WAL archiving, `pg_rewind`, physical standbys | not applicable: datax replicates ranges with Raft and backs up with `datax backup` | `datax backup --revision-history` for point-in-time restore | P3 | — | D | — |
| PG-O-013 | **pg_dump leftovers**: roles are not dumped and `pg_dumpall` does not work (no `pg_authid`); `pg_restore --single-transaction` fails (`ADD CONSTRAINT` / `CREATE INDEX` refused in a block); `pg_restore -j` is not recommended: the catalog snapshot fails large objects with `40001`, `pg_depend` lacks the rows pg_restore orders its workers by, and a foreign key's auto-created index clashes with the dump's `CREATE INDEX` ([#371](https://github.com/sthorne/datax/issues/371)); pg_dump 17 and 18 stop on their first catalog queries ([#370](https://github.com/sthorne/datax/issues/370)); a dumped sequence resumes after its cached block (a gap); large objects are cluster-wide, so a restore into a second database of the same cluster says "already exists"; grants on `datax_*` tables dump as `ON pg_catalog.datax_x`; DDL within the clock-offset window of the dump's start can fail it with `40001`; only pg_dump 16 is tested | `0A000`; `40001` | create roles first; restore without `-j` | P2 | M | V | — |

---

## Versus CockroachDB (current, v24/v25)

The CockroachDB column of each row is from CockroachDB's documentation
for v24.x–v25.x (Src **K**); datax's side was probed (V) or read in
datax's code and docs (D), as the row says. datax's `version()` reports
`PostgreSQL 14.0 datax ...`, not CockroachDB, so the CockroachDB
dialects of ORMs and tools (sqlalchemy-cockroachdb, the ActiveRecord
CockroachDB adapter, Hibernate's `CockroachDialect`, Flyway) are not
engaged and they speak PostgreSQL to datax.

Where CockroachDB and PostgreSQL share a gap (functions, triggers,
partial and expression indexes, ...), the PostgreSQL row is canonical and
the CockroachDB row points to it.

### Changefeeds and CDC

| ID | Gap | Today | Workaround | P | Size | Src | Issue |
|---|---|---|---|---|---|---|---|
| CR-CDC-001 | Changefeeds: `CREATE CHANGEFEED FOR TABLE ... INTO 'kafka://' \| 'webhook-https://' \| 's3://' \| 'gs://' \| 'gcpubsub://' \| ...` with `resolved`, `updated`, `initial_scan`, `diff`, formats (JSON, Avro), enriched envelopes; CDC queries (`CREATE CHANGEFEED ... AS SELECT`); sinkless `EXPERIMENTAL CHANGEFEED FOR`. Design: docs/design/changefeeds.md | `42601 expected TABLE, found "changefeed"`; `42601 unsupported statement "experimental"` | `LISTEN`/`NOTIFY` from the application (at most once, not durable); polling with `AS OF SYSTEM TIME` and an `updated_at` column | P1 | XL | K, V | — |
| CR-CDC-002 | `SHOW CHANGEFEED JOBS`, changefeed metrics, protected timestamps for a lagging feed | `42704 unrecognized configuration parameter "changefeed_jobs"` | — | P3 | M | K, V | — |

### Multi-region and placement

datax has per-database placement: `CREATE / ALTER DATABASE ... WITH |
SET (replicas = n, constraints = ('region=a', ...))` and `SHOW PLACEMENT`
(cluster version v16), with locality tiers from `--locality`.

| ID | Gap | Today | Workaround | P | Size | Src | Issue |
|---|---|---|---|---|---|---|---|
| CR-MR-001 | Database regions: `CREATE DATABASE ... PRIMARY REGION ... REGIONS ...`, `ALTER DATABASE ... ADD / DROP REGION`, `SET PRIMARY REGION`, `SHOW REGIONS [FROM DATABASE]`, `crdb_region` enum | `42601 ALTER DATABASE supports RENAME TO, OWNER TO and SET (placement)`; `42704 unrecognized configuration parameter "regions"` | placement constraints per database | P2 | L | K, V | — |
| CR-MR-002 | Table localities: `REGIONAL BY TABLE [IN region]`, `REGIONAL BY ROW` (hidden `crdb_region` column, auto-homing, locality-optimized search), `GLOBAL` tables (non-blocking transactions, fast reads everywhere) | `42601 expected "(", found "locality"` | a database per region; follower reads | P2 | XL | K, V | — |
| CR-MR-003 | Survival goals: `ALTER DATABASE ... SURVIVE ZONE / REGION FAILURE`, `SHOW SURVIVAL GOAL` | `42601`; `42704` | `replicas = 5` with constraints across regions | P2 | M | K, V | — |
| CR-MR-004 | Zone configurations at table, index, partition and range level: `ALTER ... CONFIGURE ZONE USING num_replicas, num_voters, constraints, voter_constraints, lease_preferences, gc.ttlseconds, range_min_bytes, range_max_bytes`; `SHOW ZONE CONFIGURATIONS` | `42601 expected ADD, DROP, RENAME, ... found "configure"`; `42704` | database placement; table `gc_ttl` | P2 | L | K, V | — |
| CR-MR-005 | Lease preferences (where leaseholders live), non-voting replicas | absent (the allocator spreads leases by load) | — | P2 | M | K, D | — |
| CR-MR-006 | Super regions, secondary regions, `PLACEMENT RESTRICTED` | absent | — | P3 | M | K, D | — |
| CR-MR-007 | Table partitioning with per-partition zone configs (geo-partitioning) — see PG-D-020 | `42601 unexpected "partition"` | — | P3 | XL | K, V | — |

### Indexes, computed columns and spatial

| ID | Gap | Today | Workaround | P | Size | Src | Issue |
|---|---|---|---|---|---|---|---|
| CR-IX-001 | Hash-sharded indexes and primary keys (`USING HASH [WITH (bucket_count = n)]`, the hidden `crdb_internal_*_shard_n` column). datax shards only time-series tables, and the whole table (`WITH (timeseries = true, shards = N)`, a hidden leading `_shard` column, online re-shard); `shards` on any other table is refused | `42601 unexpected "using"`; `42601 the retention and shards options require timeseries=true` | a time-series table; a hashed leading key column written by the application | P2 | M | K, V | — |
| CR-IX-002 | Computed columns `AS (expr) STORED` / `VIRTUAL`, and indexes on virtual columns (also PG-D-010) | `42601 expected ")", found "AS"` | application-maintained column | P2 | M | K, V | — |
| CR-IX-003 | `STORING (...)` covering indexes (PG-D-013; partial and expression indexes exist since #358, #359) | `42601 unexpected "storing"` | — | P2 | M | K, V | — |
| CR-IX-004 | Inverted indexes beyond JSONB and tsvector: on arrays, multi-column, partial, trigram (`gin_trgm_ops`), spatial; the `CREATE INVERTED INDEX` spelling | `42601 expected TABLE, found "inverted"`; `0A000 ... cannot have a GIN index` | GIN on JSONB | P2 | M | K, V | — |
| CR-IX-005 | Spatial: `GEOMETRY`, `GEOGRAPHY`, `ST_*` functions, spatial indexes (PG-E-002) | `42704 type "geometry" does not exist` | — | P2 | XL | K, V | — |
| CR-IX-006 | `ALTER TABLE ... ALTER PRIMARY KEY USING COLUMNS (...)` (online) | `42601 expected identifier, found "PRIMARY"` | recreate the table | P2 | L | K, V | — |
| CR-IX-007 | Index hints (`t@idx`, `@{FORCE_INDEX=...}`), invisible indexes (`NOT VISIBLE`, partially visible) | `42601 unexpected character '@'`; `42601 ALTER INDEX supports only RENAME TO` | session settings such as `inverted_index_scan`, `vector_index_scan` | P3 | M | K, V | — |

### Bulk I/O, backup and restore

datax's `datax backup` / `datax restore` (CLI, over the admin RPC) has
full and incremental backups, revision history and point-in-time
restore (`--as-of`), table and database restore into a running cluster
(`--table`, `--database`, `--into`), sealed backups (`--backup-key`,
AES-256-GCM) and per-row commit-time bands for time-series retention.

| ID | Gap | Today | Workaround | P | Size | Src | Issue |
|---|---|---|---|---|---|---|---|
| CR-IO-001 | `BACKUP` / `RESTORE` / `SHOW BACKUP[S]` as SQL statements, run as jobs | `42601 unsupported statement "backup" / "restore"` | the `datax backup` CLI | P2 | M | K, V | — |
| CR-IO-002 | Cloud storage destinations (`s3://`, `gs://`, `azure-blob://`, `nodelocal://`, `userfile://`, `http://`), with implicit or specified credentials | absent: backups go to a directory on the serving node's filesystem | a shared mount; copy afterwards | P2 | L | K, D | — |
| CR-IO-003 | Scheduled backups (`CREATE SCHEDULE FOR BACKUP ... RECURRING ... FULL BACKUP ...`), `SHOW SCHEDULES`, `PAUSE / RESUME SCHEDULE` | `42601 expected TABLE, found "schedule"`; `42704 unrecognized configuration parameter "schedules"` | cron around `datax backup` | P2 | M | K, V | — |
| CR-IO-004 | `IMPORT INTO` (CSV, delimited, Avro) | `42601 unsupported statement "import"` | `COPY FROM STDIN` | P2 | L | K, V | — |
| CR-IO-005 | `EXPORT INTO CSV / PARQUET` | `42601 unsupported statement "export"` | `SELECT` client-side (no `COPY TO`, PG-Q-018) | P2 | M | K, V | — |
| CR-IO-006 | Backup encryption by passphrase or KMS URI (`kms = 'aws:///...'`); datax seals with a key file only (its KMS roots protect store keys, not backups) | absent | `--backup-key` | P3 | M | K, D | — |
| CR-IO-007 | Locality-aware backups (one URI per locality) | absent | — | P3 | M | K, D | — |
| CR-IO-008 | Backup inspection and checks: `SHOW BACKUP ... WITH check_files`, `RESTORE ... WITH schema_only, verify_backup_table_data`, per-table `exclude_data_from_backup`; a backup of one table or database (datax backs up the whole cluster, though it restores per table) | absent (`datax restore` checks checksums as it restores) | — | P3 | M | K, D | — |
| CR-IO-009 | Physical cluster replication and logical data replication (24.3+) between clusters | absent | backup / restore | P3 | XL | K, D | — |

### Jobs

| ID | Gap | Today | Workaround | P | Size | Src | Issue |
|---|---|---|---|---|---|---|---|
| CR-JOB-001 | The jobs framework: `SHOW JOBS`, `SHOW JOB WHEN COMPLETE`, `PAUSE / RESUME / CANCEL JOB`, `crdb_internal.jobs`. datax's background work (schema changes, row expiry, statistics, reclaim, encryption jobs in `datax_encryption_jobs`) shows in the console's operations view and in metrics only | `42704 unrecognized configuration parameter "jobs"`; `42601 unsupported statement "pause"` | console operations view; `datax_encryption_jobs` | P2 | L | K, V | — |
| CR-JOB-002 | `CANCEL QUERY`, `CANCEL SESSION` | `42601 unsupported statement "cancel"` | `pg_cancel_backend(pid)`, `pg_terminate_backend(pid)` (exist) | P3 | S | K, V | — |

### Transactions and programmability

| ID | Gap | Today | Workaround | P | Size | Src | Issue |
|---|---|---|---|---|---|---|---|
| CR-TX-001 | `READ COMMITTED` isolation (GA in v24.1) — see PG-X-001 (design: docs/design/read-committed.md) | serializable only | retries | P1 | XL | K, V | — |
| CR-TX-002 | `SELECT ... FOR SHARE` with shared, replicated locks (v23.2+; durable under read committed) — see PG-X-004 | `42601 expected UPDATE, found "share"` | `FOR UPDATE` | P2 | M | K, V | — |
| CR-TX-003 | User-defined functions (SQL and PL/pgSQL), stored procedures, triggers (v24.3+) — see PG-P-001 to PG-P-004 | `42601` | — | P2 | XL | K, V | — |
| CR-TX-004 | Transaction priorities (`BEGIN PRIORITY HIGH`, `SET TRANSACTION PRIORITY`) | `42601 unexpected "priority"` | — | P3 | S | K, V | — |
| CR-TX-005 | `UPDATE` / `DELETE ... ORDER BY ... LIMIT` | `42601 unexpected "ORDER" / "LIMIT" after statement` | `WHERE id IN (SELECT ... LIMIT n)` | P3 | S | K, V | — |
| CR-TX-006 | CockroachDB builtins clients use: `gen_random_ulid`, `crdb_internal.*` functions, `default_int_size` (CockroachDB's `INT` is `INT8`; datax's, like PostgreSQL's, is `INT4`) | `42883 unknown function "gen_random_ulid"`; `42704 unrecognized configuration parameter "default_int_size"` | `gen_random_uuid`, `unique_rowid` (exists) | P3 | S | K, V | — |

### Row-level TTL and historical reads

datax has row expiry (`WITH (ttl_expiration = col)` or `WITH
(ttl_column = col, ttl = '7 days')`, one cluster job, passes every 30 s,
ordinary `DELETE`s), `AS OF SYSTEM TIME` with a fixed time or an offset
and `with_max_staleness(...)` bounded-staleness reads.

| ID | Gap | Today | Workaround | P | Size | Src | Issue |
|---|---|---|---|---|---|---|---|
| CR-AOST-002 | `nearest_only = TRUE`; `follower_read_timestamp()` in a select list; expression operands (`now() - '10s'::interval`) | `0A000`; `42883`; `42601` | a literal timestamp or interval | P3 | S | D | — |
| CR-TTL-001 | CockroachDB's TTL options and controls: `ttl_expire_after` (with the hidden `crdb_internal_expiration` column), `ttl_expiration_expression` (any expression), `ttl_job_cron`, `ttl_select_batch_size`, `ttl_delete_batch_size`, `ttl_delete_rate_limit`, `ttl_pause`, `ttl_row_stats_poll_interval`; one TTL job per table, visible in `SHOW JOBS` | `42601 unknown table option "ttl_expire_after"` | datax's `ttl_column` + `ttl`, `ttl_expiration = col` | P3 | M | K, V | — |

### Virtual clusters and cluster settings

| ID | Gap | Today | Workaround | P | Size | Src | Issue |
|---|---|---|---|---|---|---|---|
| CR-CS-001 | Cluster settings: CockroachDB has hundreds (`SET / SHOW / RESET CLUSTER SETTING`, `SHOW [ALL] CLUSTER SETTINGS`); datax parses `SET / SHOW CLUSTER SETTING` but has one setting, `key_service.rate_limit`, and no listing | `42704 unrecognized configuration parameter "cluster_settings"` for `SHOW CLUSTER SETTINGS`; `42601 expected a value for cluster setting ...` for an unknown one | node flags | P2 | M | K, V | — |
| CR-VC-001 | Virtual clusters / multi-tenancy (`CREATE VIRTUAL CLUSTER`, `SHOW VIRTUAL CLUSTERS`, separate SQL namespaces and resource isolation per tenant). datax's tenancy is per-tenant encryption keys on sealed tables and row-level security | `42601 expected TABLE, found "virtual"` | databases, RLS, per-tenant keys | P3 | XL | K, V | — |

### SHOW statements, crdb_internal and the console

datax has `SHOW TABLES`, `DATABASES`, `INDEXES`, `COLUMNS`, `CREATE
TABLE`, `VIEWS`, `GRANTS`, `GRANTS ON ROLE`, `ROLES`, `USERS`,
`SEQUENCES`, `SESSIONS`, `FUNCTIONS`, `PLACEMENT`, `STATS FOR t`, `ALL`,
and `SHOW CLUSTER SETTING name`.

| ID | Gap | Today | Workaround | P | Size | Src | Issue |
|---|---|---|---|---|---|---|---|
| CR-SHOW-001 | `SHOW RANGES`, `SHOW ZONE CONFIGURATIONS`, `SHOW STATEMENTS` / `QUERIES`, `SHOW JOBS`, `SHOW SCHEDULES`, `SHOW REGIONS`, `SHOW SURVIVAL GOAL`, `SHOW LOCALITY`, `SHOW TRACE FOR SESSION`, `SHOW TRANSACTION STATUS`, `SHOW SAVEPOINT STATUS`, `SHOW LAST QUERY STATISTICS`, `SHOW PARTITIONS`, `SHOW DEFAULT PRIVILEGES`, `SHOW FULL TABLE SCANS`, `SHOW SCHEMAS`, `SHOW TYPES` / `ENUMS`, `SHOW CONSTRAINTS`, `SHOW STATISTICS`, `SHOW BACKUP[S]`, `SHOW CREATE ALL TABLES`. Each unknown form is read as a session variable, so the error misleads | `42704 unrecognized configuration parameter "ranges_from_table_t"` (and likewise) | `datax debug ranges`; the console; `pg_catalog` | P2 | M | K, V | — |
| CR-OBS-001 | `crdb_internal` (`ranges`, `jobs`, `node_*`, `cluster_*`, `statement_statistics`, `index_usage_statistics`, ...) | `3D000 database "crdb_internal" does not exist` | the console's JSON APIs (`/api/cluster`, `/status`), `datax_metrics` | P2 | L | K, V | — |
| CR-OBS-002 | DB Console pages with no datax counterpart: SQL Activity's Transactions page, Insights (slow and contended executions, index recommendations), Jobs and Schedules, statement diagnostics bundles, Advanced Debug (problem ranges, logs), Hot Ranges by table/index (datax has the ranges table sorted by load, a network matrix, failure-domain table, health findings with history, statement shapes over time, index usage and a key viewer) | absent | — | P3 | L | K, D | — |
| CR-OBS-003 | Contention events and lock visibility (`crdb_internal.transaction_contention_events`, `cluster_locks`) | absent | `SHOW SESSIONS` | P3 | M | K, D | — |

### Schema changes, statistics and diagnostics

| ID | Gap | Today | Workaround | P | Size | Src | Issue |
|---|---|---|---|---|---|---|---|
| CR-SC-001 | Online schema changes as broad as CockroachDB's declarative schema changer: online `ALTER PRIMARY KEY`, `ADD COLUMN` with any default (backfilled online), `ALTER COLUMN TYPE` of any kind, DDL in explicit transactions (with its caveats). datax builds indexes and widens / converts column types online, but refuses non-constant `ADD COLUMN` defaults and `CREATE INDEX` inside `BEGIN` | `0A000 ADD COLUMN takes a constant DEFAULT only`; `25001` | multi-step migrations | P2 | L | K, V | — |
| CR-STAT-001 | Table statistics: `CREATE STATISTICS name ON cols FROM t`, histograms, multi-column statistics, partial statistics, forecasting, `SHOW STATISTICS USING JSON`, `ALTER TABLE ... INJECT STATISTICS`. datax keeps row counts and per-column distinct and NULL counts (`ANALYZE`, a background sampler) | `42601 expected TABLE, found "statistics"` | `ANALYZE` | P2 | M | K, V | — |
| CR-STAT-002 | Statement diagnostics: `EXPLAIN ANALYZE (DEBUG)` bundles, `crdb_internal.request_statement_bundle`, `EXPLAIN (OPT / DISTSQL / VEC / DDL / GIST)` variants | `42601 expected SELECT, VALUES or a parenthesized query, found "debug"` | `EXPLAIN ANALYZE`; `--slow-statement-threshold` | P3 | M | K, V | — |
| CR-STAT-003 | Range management statements: `ALTER TABLE ... SCATTER`, `ALTER RANGE ... RELOCATE`, `EXPERIMENTAL_RELOCATE` (datax has `SPLIT AT` / `UNSPLIT` and `datax debug`) | `42601` | `datax debug` | P3 | S | K, V | — |

---

## Where datax goes beyond both

Useful context when weighing gaps; none of these is a gap.

- **`LISTEN` / `NOTIFY` across nodes** (#297): PostgreSQL's semantics on
  every node of the cluster; CockroachDB has no `LISTEN` / `NOTIFY`.
- **Large objects** (#303): PostgreSQL's `lo_*` API and the fast-path
  call, streamed in 256 KiB pages up to 1 TiB per object without holding
  a transaction's write set; CockroachDB has none.
- **Sealed tables and application keys** (#310): per-table and
  per-tenant data keys, crypto-shredding by destroying a key, online
  sealing and key rotation, the `datax_keys` table and the key service.
- **Encryption at rest** with authenticated encryption, roots in Vault,
  AWS KMS, GCP KMS and Azure Key Vault, an exec helper, or a root split
  among operators (sealed start, peer unseal), keys in locked memory.
  PostgreSQL has none built in; CockroachDB's is an enterprise feature
  with a store key file.
- **Time-series tables**: shard buckets, retention, online re-shard,
  K-way merge across buckets; retention carried across backups.
- **`ALTER TABLE ... PURGE`** and **per-table `gc_ttl`**: collect old
  versions now, with a per-replica report written to the audit log.
- **The audit table** (`datax_audit`) with per-node hash chains and
  offline verification (`datax debug audit verify`).
- **Point-in-time restore of chosen tables into a running cluster**
  (`datax restore --table ... --as-of ...`).
- **pgvector-compatible vectors built in** (no extension), with an
  ivfflat index; and **full-text search** built in.
- **`UNIQUE NULLS NOT DISTINCT`** (PostgreSQL 15+; CockroachDB does not
  have it) and **statement-level unique checks in `UPDATE`** (PostgreSQL
  needs a `DEFERRABLE` constraint for `UPDATE t SET id = id + 1`).
- **`ALTER TABLE ... ADD CONSTRAINT IF NOT EXISTS`** (#322).
- **Console** features: cluster-wide rollups, a network latency and
  clock-offset matrix, a failure-domain table ("what would losing this
  rack cost"), health findings with history, metrics history kept in
  SQL (`datax_metrics`).

---

## Closed

Gaps closed recently, newest first. From CHANGELOG 0.58.0 and the merged
PRs #317–#369 (issue numbers in the changelog are given where the PR
carried several).

| Closed gap | PR / issue |
|---|---|
| A multi-statement query, and an extended-protocol pipeline up to its `Sync`, are one implicit transaction (a failing statement no longer leaves the earlier ones committed; `_xact_` advisory locks span it, from PG-X-014), except around datax's online statements (PG-X-006) | PR (this branch) |
| Sequences and enum types in any schema: `CREATE SEQUENCE / TYPE s.x`, `SERIAL` and identity in a schema, `SET SCHEMA`, defaults bound to their sequence, schema privileges, the catalogs (was PG-D-035; leftovers are PG-D-037) | PR (this branch) |
| Schemas: `CREATE` / `DROP` / `ALTER SCHEMA`, objects in any schema, `SET SCHEMA`, `search_path`, schema privileges, schemas in the catalogs (was PG-D-001; leftovers are PG-D-035 and PG-D-036) | PR (this branch) |
| Temporary tables with `ON COMMIT`, and `UNLOGGED` tables (accepted, durable) (was PG-D-009; leftovers are PG-D-034) | PR (this branch) |
| Aggregates in a `SELECT` without `FROM` (was PG-Q-008; leftovers are PG-Q-034) | PR (this branch) |
| Quantified comparisons `< <= > >= = <>` and `LIKE` with `ANY` / `SOME` / `ALL` over subqueries and arrays, PostgreSQL's NULL rules (was PG-Q-013) | PR (this branch) |
| `VALUES` column types resolved across rows; multi-row `LATERAL (VALUES ...)`; `TABLE t` (was PG-Q-015) | PR (this branch) |
| Multi-column `UPDATE ... SET (a, b) = (...) / ROW(...) / (SELECT ...)`, also in `UPDATE ... FROM` and `ON CONFLICT DO UPDATE` (was PG-Q-017) | PR (this branch) |
| `COPY FROM` options `HEADER [MATCH]`, `DELIMITER`, `NULL`, `QUOTE`, `ESCAPE`, `FORCE_NULL`, `FORCE_NOT_NULL`, `ENCODING` in both syntaxes (was PG-Q-019; leftovers are PG-Q-035) | PR (this branch) |
| `COLLATE "C" / "POSIX" / "default" / ucs_basic` in expressions and `ORDER BY` (was PG-Q-023; leftovers are PG-Q-036) | PR (this branch) |
| `DISCARD ALL / PLANS / SEQUENCES / TEMP` (was PG-Q-024) | PR (this branch) |
| `regexp_instr`; `substring(s FROM pattern)` / `SIMILAR ... ESCAPE` (was PG-F-001; leftovers are PG-F-025) | PR (this branch) |
| jsonb `-`, `#-`, subscripting (also in `UPDATE ... SET j['a']`), PostgreSQL's jsonb ordering (was PG-F-005) | PR (this branch) |
| String functions: `overlay`, `bit_length`, `to_ascii`, `convert*`, `unistr`, `normalize`, `parse_ident`, sha2 / crc32, byte and bit access, `format()` widths (was PG-F-009) | PR (this branch) |
| Math functions: trigonometric, degree and hyperbolic variants, `degrees`, `radians`, `factorial`, `lcm`, `scale`, `min_scale`, `trim_scale`, `random(min, max)`, `random_normal`, `erf` (was PG-F-010; leftovers are PG-F-026) | PR (this branch) |
| Integer bitwise operators `& \| # << >>` and prefix `~ @ \|/ \|\|/` with PostgreSQL's precedence (was PG-F-011; leftovers are PG-F-027) | PR (this branch) |
| Array functions `array_dims`, `array_fill`, `array_replace`, `array_positions`, `array_sample`, `array_shuffle`, `trim_array`, `generate_subscripts`, `array_reverse`, `array_sort` (was PG-F-012) | PR (this branch) |
| Aggregates `corr`, `covar_*`, `regr_*`, `bit_and` / `bit_or` / `bit_xor`, `any_value`, the `json[b]_*agg_strict` / `_unique` forms (was PG-F-013; leftovers are PG-F-028) | PR (this branch) |
| Session functions nested in a call (`pg_cancel_backend(pg_backend_pid())`) (was PG-F-018) | PR (this branch) |
| Timestamps keep microsecond precision; jsonb renders them as PostgreSQL does (was PG-F-019) | PR (this branch) |
| Time zones in `ON` conditions, in `to_jsonb` / `format` / `array_to_string` / `concat`; more abbreviations; the startup `TimeZone` in `ParameterStatus` (was PG-T-001; leftovers are PG-T-031) | PR (this branch) |
| `bytea` `||`, trim / reverse on bytes, escape-format input, text-only functions refuse `bytea` (was PG-T-003) | PR (this branch) |
| reg* literals resolve to OIDs and print as names; catalog OIDs fit in 32 bits (was PG-T-012; leftovers are PG-T-032) | PR (this branch) |
| `numeric` `NaN` and `±Infinity`, ordering, arithmetic, wire format (was PG-T-013; `power` with them is not done) | PR (this branch) |
| `CREATE DATABASE` options (`ENCODING`, `TEMPLATE`, `LOCALE`, `OWNER`, `CONNECTION LIMIT`, ...); `ALTER DATABASE ... SET / RESET` and the other forms (was PG-D-022; leftovers are PG-D-033) | PR (this branch) |
| Dollar-quoted string literals anywhere (was PG-P-005) | PR (this branch) |
| Role attributes `SUPERUSER` (the admin role), `CREATEDB`, `CREATEROLE`, `REPLICATION`, `CONNECTION LIMIT`, `VALID UNTIL`; admin-only `pg_authid` (was PG-S-001; leftovers are PG-S-011) | PR (this branch) |
| PostgreSQL 14's full set of reported parameters and preset settings; `server_version` stays 14 on purpose, since pg_dump 16 dumps nothing from a server reporting 16 (was PG-W-003) | PR (this branch) |
| Every PostgreSQL 16 `information_schema` view, with PostgreSQL's columns, types and visibility rules (was PG-C-001; leftovers are PG-C-015) | PR (this branch) |
| `pg_get_indexdef` / `pg_get_constraintdef` of a literal OID or regclass; an index name is a regclass (was PG-C-007) | PR (this branch) |
| `pg_timezone_names`, `pg_timezone_abbrevs` (was PG-C-008) | PR (this branch) |
| `VACUUM` in all forms, a no-op; with `ANALYZE` it refreshes statistics (was PG-O-003) | PR (this branch) |
| `follower_read_timestamp()`, `with_min_timestamp`, `with_max_staleness` in `AS OF SYSTEM TIME`; `BEGIN` / `SET TRANSACTION AS OF SYSTEM TIME` (was CR-AOST-001; leftovers are CR-AOST-002) | PR (this branch) |
| Concurrent online `CREATE INDEX` on one table lost an index (was PG-D-032) | PR (this branch) (#368) |
| Quoted mixed-case type names, `""` in identifiers, quoted keywords, `E''` escapes (was PG-T-029; leftovers are PG-T-030) | PR (this branch) (#367) |
| Correlated subqueries follow PostgreSQL scoping; grouped correlated subqueries (was PG-Q-031) | PR (this branch) (#361) |
| `JOIN ... ON` with any condition (was PG-Q-006) | PR (this branch) (#363) |
| Several actions in one `ALTER TABLE`; `ADD COLUMN` with any default (were PG-D-007, PG-D-008; leftovers are PG-D-031) | PR (this branch) (#364) |
| `COPY ... TO STDOUT` (was PG-Q-018) | PR (this branch) (#365) |
| pg_dump / pg_restore end to end (was PG-O-002; leftovers are PG-O-013) | PR (this branch) (#366) |
| Advisory locks, cluster-wide (was PG-X-003; leftovers are PG-X-014) | PR (this branch) (#353) |
| Tables without a declared primary key get a hidden `rowid` (was PG-D-002) | PR (this branch) (#354) |
| `UPDATE ... FROM`, `DELETE ... USING`; `SET a = b, b = a` swaps (was PG-Q-007) | PR (this branch) (#355) |
| `ORDER BY` inside aggregate calls; `mode() WITHIN GROUP` (was PG-Q-005; part of PG-F-021) | PR (this branch) (#356) |
| `pg_get_serial_sequence`, `to_regclass`, `pg_is_in_recovery`, `has_*_privilege`, `current_role` / `user` (was PG-F-008; leftovers are PG-F-024) | PR (this branch) (#357) |
| Partial indexes (was PG-D-003) | PR (this branch) (#358) |
| Expression indexes (was PG-D-004) | PR (this branch) (#359) |
| `DESC` / `NULLS` in index definitions (was PG-D-014; leftovers are PG-D-030) | PR (this branch) (#360) |
| `json \|\| json` merges like jsonb (deliberate difference, PG-T-028) | PR #352 |
| `jsonb \|\| jsonb` merges (was PG-F-004) | PR (this branch) (#342) |
| Casts to unknown types fail with `42704`; multi-word type names in casts (was PG-T-002; leftovers are PG-T-028) | PR (this branch) (#343) |
| `length`, `octet_length`, `substring`, `position` and `md5` of `bytea` work on bytes (part of PG-T-003) | PR (this branch) (#344) |
| PostgreSQL's common settings (`client_min_messages`, `IntervalStyle`, `work_mem`, `enable_*`, ...), `SHOW server_version_num`, `SET standard_conforming_strings = on` (most of PG-O-001, part of PG-W-007) | PR (this branch) (#345) |
| Transaction modes on `BEGIN` / `START TRANSACTION`; `nextval` refused when read-only (was PG-X-002; leftovers are PG-X-013) | PR (this branch) (#346) |
| `CREATE` / `DROP INDEX CONCURRENTLY`; unnamed indexes; `CREATE INDEX` in the block that created the table (was PG-D-005; leftovers are PG-D-029) | PR (this branch) (#347) |
| Pattern operator classes and `COLLATE "C"` in indexes (was PG-D-006) | PR (this branch) (#348) |
| `CREATE` / `DROP EXTENSION` of `plpgsql`, `pgcrypto`, `uuid-ossp` and `vector` (was PG-E-001) | PR (this branch) (#349) |
| Expressions over aggregates in the select list, `HAVING` and `ORDER BY` (was PG-Q-001) | PR (this branch) (#335) |
| `GROUP BY` expressions and output names, `ROLLUP`, `CUBE`, `GROUPING SETS`, `GROUPING()` (was PG-Q-002) | PR (this branch) (#336) |
| `SELECT DISTINCT ON (...)` (was PG-Q-003; leftovers stay under that ID) | PR (this branch) (#337) |
| `LATERAL` subqueries and table functions reading earlier `FROM` members; `unnest` beside a table's columns (part of PG-Q-004) | PR (this branch) (#338) |
| `regexp_*` functions, `generate_series`, `jsonb_each` / `jsonb_array_elements` / `jsonb_object_keys` / `jsonb_insert`, set-returning functions in `FROM` and in a select list without `FROM` (parts of PG-F-001/002/003) | PR (this branch) (#339) |
| `AT TIME ZONE`, `timezone()`, and `TimeZone` honored for input, `::text`, `::date`, `date_trunc`, `extract`, `to_char`, `current_date` and interval arithmetic (was PG-F-007; parts of PG-F-006, PG-T-001) | PR (this branch) (#340) |
| `pg_settings` reports `vartype` and `unit` from the settings registry, and what `RESET` restores | PR #334 (#333) |
| `pg_settings` and `SHOW ALL` list custom settings; `current_setting(name)` per row raises `42704` for an unknown name, as PostgreSQL does | PR #332 (#329, #330) |
| psql's `\dd`, `\dAf`, `\dAo`, `\dAp` and `\dconfig`; `COMMENT ON CONSTRAINT`; the hidden `tableoid` column on catalogs; select-list aliases without `AS` | PR #326 |
| A comma or `CROSS` join keyed by `WHERE` equalities runs as a join, not a cross product | PR #326 |
| psql 17 and 18's describe commands (`connoinherit`, `conislocal`, `coninhcount`, ...) | PR #323 |
| `UPDATE` of primary-key columns; unique indexes take NULLs (NULLS DISTINCT); `UNIQUE NULLS NOT DISTINCT`; `ADD CONSTRAINT IF NOT EXISTS` | PR #323 (#322) |
| Parameters counted and typed in every position; a single table's alias in every query shape; clock keywords in `RETURNING`; `EXPLAIN` of a parameterized statement | PR #323 (#322) |
| `information_schema` and the per-database `pg_catalog` tables list the current database's objects only | PR #323 (#322) |
| `ORDER BY n` / `GROUP BY n` name the select item (not its output name); `SELECT * ... ORDER BY n` | PR #323 (#322) |
| Describe reports every row-returning statement over the extended protocol (`ALTER TABLE ... PURGE` and others) | PR #323 (#322) |
| `RIGHT` / `FULL JOIN` kept unmatched rows of a CTE, subquery, table function or catalog | 0.58.0 |
| `'1 ms'::interval` and `'1 us'` mean milliseconds and microseconds | 0.58.0 |
| An aborted transaction answers `40001` everywhere and never misses its own writes | PR #321 (#320) |
| Every role reads the catalogs; `EXPLAIN` takes `SELECT`; `TRUNCATE` takes the `TRUNCATE` privilege; `AS OF SYSTEM TIME` answers to today's privileges | #312 |
| `SELECT ... FOR UPDATE SKIP LOCKED` and `NOWAIT` | #296 (fixes in PR #321 and PR #323) |
| pgvector-compatible `vector(n)`, distance operators and ivfflat | #304 |
| Full-text search: `tsvector`, `tsquery`, `@@`, the `to_tsvector` family, ranking, GIN | #299 |
| Large objects, including the fast-path call | #303 |
| GIN indexes on JSONB (`@>`, `?`, `?\|`, `?&`) | #300 |
| Row-level security | #294 |
| `LISTEN` / `NOTIFY` / `pg_notify` | #297 |
| Row expiry on ordinary tables | #298 |
| Floating-point `NaN` ordering as in PostgreSQL | #304 |

---

## Priorities

The gaps that would most widen what runs unmodified, in order. The
earlier list's items 1–2 are closed (#367, #368); pg_dump 17/18 and
parallel restore (#370, #371) are tabled for now, and Rails migration
requirements (PG-D-031) are P4, out of this list.

1. **PG-F-002 / PG-F-003 / PG-F-006** what is left of the
   missing function families.
2. **PG-X-014** advisory locks taken per row (job queues).
3. **PG-D-030 / PG-D-029** index leftovers.
4. **PG-Q-033** a hash join, for large joins without a usable equality.
5. **PG-O-013** the pg_dump leftovers (#370, #371; tabled).
6. **PG-P-001 / PG-P-002 / PG-X-001 / CR-CDC-001** functions, triggers,
   read committed and changefeeds: the large structural gaps left
   (schemas, PG-D-001, are closed); each is XL. Functions and triggers
   (docs/design/functions-and-triggers.md), read committed
   (docs/design/read-committed.md) and changefeeds
   (docs/design/changefeeds.md) have designs awaiting the owner's
   answers to their open questions.
