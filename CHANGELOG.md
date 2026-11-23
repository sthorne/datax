# Changelog

Releases of datax, newest first. The version is `pkg/version.Release`,
bumped in the pull request that changes behavior (minor for a new
capability, patch for a fix); a git tag `vX.Y.Z` on `main` marks the
release, and the build workflow stamps binaries with the tag or with
`vX.Y.Z+<commit>` between tags. The cluster protocol version (`v1`, `v2`,
... in `pkg/version`) is separate: it changes only when the replicated
state or the internode protocol does, and an entry below says so.

## 0.58.0 — unreleased

### Added
- **`datax_range_latch_wait_seconds{op,outcome}`**: time spent waiting
  for a latch by range-wide operations (`split`, `merge`, `clear_range`,
  `closed_ts`), with outcome `acquired` or `abandoned`. The existing
  `datax_latch_wait_seconds` counts only waits that end in an acquire.
- **Cluster version v24: an INSERT checks uniqueness in its write.**
  Once v24 is finalized, a plain INSERT (and INSERT ... SELECT) writes
  its primary key and single-key unique-index entries with a conditional
  put, the new KV request `CPut`, which fails if the key holds a live
  value.
  - An autocommit single-row INSERT is one KV batch, a one-phase
    commit, instead of a read followed by a write.
  - A multi-row INSERT no longer reads every key first.
  - Errors, including which duplicate is reported first, are unchanged.
  - ON CONFLICT, UPSERT, UPDATE and COPY keep the read-then-write path.
  - Before the finalize, every node uses the old path. Only senders
    decide, so replicas never read the cluster version.
  - Measured: KV batches per INSERT 2 → 1; server CPU per op −12% at
    one row and −16% at 100 rows.
- **`TestKVRoundTripsPerStatement`** pins the exact KV batches and
  requests of point reads and writes, an index join, an explicit
  transaction and `SELECT 1`, over both protocols on one and three
  nodes. A change that adds a round trip to every statement now fails
  CI, where before only a throughput drop in a paused nightly run would
  have shown it. `datax bench` records carry `kv_batches_per_op`, and
  `datax bench compare` flags an increase as a regression.
- **`datax demo --gc-ttl`** sets the history window of a demo cluster,
  so an in-memory cluster that runs create-and-drop test suites need not
  hold 25 hours of dropped tables in memory (#391).
- **Cluster version v23.** Once the upgrade to it is finalized, a
  replica refuses at apply a write outside its range or into a range
  frozen for a merge, a split that would reuse a range ID, and a merge
  whose right-hand range does not start where the left ends (#389).
  The new metric `datax_range_id_collisions_total` should stay at 0.
- **Schemas** (was PG-D-001; docs/user/sql.md, "Schemas"; design in
  docs/design/schemas.md):
  - `CREATE SCHEMA [IF NOT EXISTS] [name] [AUTHORIZATION role]`,
    `DROP SCHEMA ... [CASCADE | RESTRICT]`, `ALTER SCHEMA ... RENAME TO`
    / `OWNER TO`, and `ALTER TABLE / VIEW ... SET SCHEMA`.
  - Tables and views in any schema. `search_path` (default
    `"$user", public`) is honoured for resolution and creation, as are
    `current_schema()` and `current_schemas()`. A schema on the path that
    the role has no `USAGE` on is skipped, as in PostgreSQL.
  - `GRANT` / `REVOKE USAGE` and `CREATE ON SCHEMA`, and
    `... ON ALL TABLES IN SCHEMA s`. A new schema grants nothing to
    PUBLIC; `public` keeps its current rules.
  - Views bind their relations when they are created. A table created
    later in a schema earlier on the view's path does not change what
    the view reads.
  - `pg_namespace`, `pg_class.relnamespace`, `information_schema` and
    psql's `\dn` and `\dt` report the real schemas. Cluster backups
    carry schemas.
  - Existing tables resolve exactly as before, and nothing on disk
    changes. `CREATE SCHEMA`, and restoring a backup that holds schemas,
    need cluster protocol version v22.
  - Leftovers are PG-D-036; sequences and enum types outside `public`
    (PG-D-035) are closed too, below.
- **Temporary tables** (was PG-D-009):
  - `CREATE [GLOBAL | LOCAL] TEMP[ORARY] TABLE` with
    `ON COMMIT PRESERVE ROWS | DELETE ROWS | DROP`,
    `CREATE TEMP TABLE ... AS`, indexes, and `pg_temp.t`.
  - A temporary table is private to its session: other sessions get
    `42P01` by every name and see no catalog rows for it.
  - It is dropped when its session ends, on `DISCARD TEMP` / `ALL`, or
    by a background sweep when its node crashed or restarted. A session
    whose temporary table the sweep dropped gets `42P01` for the name
    until it runs `DROP TABLE` or `DISCARD TEMP`; it never reaches an
    ordinary table of that name.
  - Leftovers are PG-D-034.
- **`UNLOGGED` tables**: `CREATE UNLOGGED TABLE` and
  `ALTER TABLE ... SET LOGGED | UNLOGGED` are accepted. Unlogged tables
  stay fully durable; only `pg_class.relpersistence` reports `u`.
  Temporary and unlogged tables need cluster protocol version v22.
- **Sequences and enum types in any schema** (was PG-D-035; design in
  docs/design/schemas.md, "Sequences and types"):
  - `CREATE SEQUENCE s.x`, `CREATE TYPE s.x AS ENUM (...)`,
    `nextval('s.x')`, `'a'::s.mood`, and unqualified names through
    `search_path`. `SERIAL` and identity columns of a table in a schema
    get their sequence in that schema.
  - `ALTER SEQUENCE / TYPE ... SET SCHEMA`; `ALTER TABLE ... SET SCHEMA`
    moves the table's own sequences with it. A move, or renaming the
    schema, is refused (`2BP01`) while a view names the object, or a
    default or CHECK casts to the type.
  - A column default's `nextval('x')` is bound to the sequence `x` named
    when the default was set, as PostgreSQL binds the `regclass`; a name
    that names no sequence is refused (`42P01`). Neither a session's
    `search_path` nor a sequence of that name created later can redirect
    it. `DROP SEQUENCE` of a sequence a default is bound to (and
    `DROP TABLE` of a table owning one), counting every session's
    temporary tables, is `2BP01` unless `CASCADE`, which drops the
    default.
  - Using a sequence or type by name in a schema other than `public`
    takes `USAGE` on the schema (also for `::regclass` / `::regtype`
    input), and creating one takes `CREATE` on it.
    `CREATE SEQUENCE ... OWNED BY` requires the table's owner (`55000`).
    `DROP SCHEMA` counts the schema's sequences and types, and `CASCADE`
    drops them.
  - The catalogs name the schema of sequences and types (`pg_class`,
    `pg_type`, `pg_sequences`, `information_schema.sequences`,
    `information_schema.columns.udt_schema`, `pg_get_serial_sequence`).
    `format_type`, `regtype` and `regclass` output qualify a name the
    search path does not reach and quote it where needed; the visibility
    functions follow the path, so psql's `\d`, `\ds` and `\dT` and
    pg_dump work.
  - Leftovers are PG-D-037.
- **PostgreSQL compatibility: the small P2 gaps, and `information_schema`**
  (docs/compatibility-gaps.md; leftovers are new P3 rows there).
  - **Queries.**
    - Aggregates in a `SELECT` without `FROM` (`SELECT count(*)`).
    - Quantified comparisons `< <= > >= = <>` and `LIKE` with
      `ANY` / `SOME` / `ALL` over subqueries, with PostgreSQL's NULL and
      empty-set rules.
    - `VALUES` column types resolved across rows; multi-row
      `LATERAL (VALUES ...)`; `TABLE t`.
    - `UPDATE ... SET (a, b) = (...) / ROW(...) / (SELECT ...)`, also in
      `UPDATE ... FROM` and `ON CONFLICT DO UPDATE`.
    - `COPY FROM` options `HEADER [MATCH]`, `DELIMITER`, `NULL`, `QUOTE`,
      `ESCAPE`, `FORCE_NULL`, `FORCE_NOT_NULL` and `ENCODING`, in both
      syntaxes. The text format no longer splits a row at an escaped
      delimiter.
    - `COLLATE "C" / "POSIX" / "default" / ucs_basic`.
    - `DISCARD ALL / PLANS / SEQUENCES / TEMP`.
    - Dollar-quoted string literals anywhere.
    - `VACUUM` in all forms, a no-op; `VACUUM ANALYZE` refreshes
      statistics.
  - **Functions and operators.**
    - jsonb `-`, `#-` and subscripting, also in `UPDATE ... SET j['a'] = ...`.
      PostgreSQL's jsonb ordering applies to comparisons, `ORDER BY` and
      `DISTINCT`.
    - Integer bitwise operators `& | # << >>` and prefix `~ @ |/ ||/`.
    - `regexp_instr`; `substring(s FROM pattern)` and
      `substring(s SIMILAR p ESCAPE e)`.
    - About twenty string functions (`overlay`, `to_ascii`, `convert*`,
      `unistr`, `normalize`, `parse_ident`, sha2 and crc32, byte and bit
      access) and `format()` widths.
    - The trigonometric, degree and hyperbolic functions, `factorial`,
      `lcm`, `scale`, `trim_scale`, `random(min, max)`, `random_normal`,
      `erf`.
    - Array functions `array_dims`, `array_fill`, `array_replace`,
      `array_positions`, `array_sample`, `array_shuffle`, `trim_array`,
      `generate_subscripts`, `array_reverse`, `array_sort`.
    - Aggregates `corr`, `covar_*`, `regr_*`, `bit_and` / `bit_or` /
      `bit_xor`, `any_value` and the `json[b]_*agg_strict` / `_unique`
      forms, also as window functions.
    - A session function nested in another's arguments resolves
      (`pg_cancel_backend(pg_backend_pid())`).
  - **Types.**
    - `numeric` `NaN` and `±Infinity`: ordering (index keys of finite
      values are unchanged), arithmetic and the binary wire format.
    - `bytea` `||`, trim and reverse on bytes, and escape-format input.
    - reg* literals (`regclass`, `regtype`, `regproc`, `regnamespace`,
      `regrole`, ...) resolve to OIDs and print as names.
    - Time zones are applied in `ON` conditions and in
      `to_jsonb` / `format` / `array_to_string`; about 110 more zone
      abbreviations.
  - **Catalogs.**
    - Every PostgreSQL 16 `information_schema` view, with PostgreSQL's
      columns, types and visibility rules: `sequences` (read by
      Hibernate), `table_privileges`, `role_table_grants`,
      `applicable_roles`, `view_table_usage` and the rest. Views for
      features datax lacks (`routines`, `triggers`, ...) are empty.
    - `pg_get_indexdef` / `pg_get_constraintdef` of a literal OID or
      regclass.
    - `pg_timezone_names`, `pg_timezone_abbrevs`.
    - Connections receive PostgreSQL 14's full set of reported
      parameters. `server_version` stays 14, because pg_dump 16 dumps no
      tables from a server that reports 16.
  - **Databases and roles.**
    - `CREATE DATABASE` options (`ENCODING`, `TEMPLATE`, `LOCALE`,
      `OWNER`, `CONNECTION LIMIT`, ...).
    - `ALTER DATABASE ... SET / RESET` defaults, applied when a session
      starts.
    - Role attributes `SUPERUSER` (the admin role), `CREATEDB`,
      `CREATEROLE`, `REPLICATION`, `CONNECTION LIMIT` (per node) and
      `VALID UNTIL` (enforced at password login), shown in `pg_roles`,
      `pg_user` and a new admin-only `pg_authid`.
  - **Historical reads.** `AS OF SYSTEM TIME follower_read_timestamp()`,
    `with_min_timestamp(...)` and `with_max_staleness(...)`. `BEGIN` /
    `SET TRANSACTION AS OF SYSTEM TIME` makes a read-only historical
    block.
- **`ALTER TABLE t PURGE WHERE <predicate>`** (#375) purges only the
  rows a primary-key prefix or range names, such as one tenant's
  (`WHERE tenant = 'acme'`, `WHERE (a, b) = (1, 2)`,
  `WHERE a = 1 AND b BETWEEN 10 AND 20`). Only those keys' older readers
  are cut off; other keys keep their history. Secondary indexes that lead
  with the same columns are purged over the same prefix; any other index
  is purged whole, with a `NOTICE` naming it, so the purged rows' old
  versions are gone from every index. Other predicates are refused with a
  hint naming the primary key: `42P10` for a non-key column, `0A000` for
  `OR` / `IN` / functions, `22023` for inexact or `NULL` values.
- **Backups are protected from purges** (#376). A running backup protects
  its read timestamp from `ALTER TABLE ... PURGE` (including
  `PURGE WHERE`), `datax debug purge`, garbage collection and the
  dropped-table cleanup, under a 2-minute lease so a crashed backup cannot
  hold collection back. A purge waits up to 10 seconds for running
  backups; if one is still running it removes everything deleted before
  that backup began, reports itself incomplete (`PURGE INCOMPLETE` and a
  `NOTICE`; `datax debug purge` exits non-zero and names the backups), and
  purges again automatically once the backup ends (lost if that node
  restarts first). Each backup now starts about a second later, twice the
  maximum clock offset.
- **An incremental backup survives a purge** (#376). When a purge or
  garbage collection has passed its base, the incremental holds the
  affected tables in full instead of failing, and restoring the chain
  takes those tables from that backup, so purged rows are not brought
  back. Manifests carrying a full table use the new format `DXBK4`, which
  older binaries refuse; a point-in-time restore to a moment inside such
  a table's gap is refused for that table.
- **Quoted identifiers as in PostgreSQL** (#367): a quoted type name keeps
  its case (`CREATE TYPE "Role"`, a column `r "Role"`, `::"Role"`, `CAST(x
  AS "public"."Role")`, `ALTER COLUMN ... TYPE "Role"`, `regtype`,
  `format_type`), so Prisma's enums work; `""` inside a quoted identifier is
  one `"`, for every kind of object; a quoted identifier is never a keyword
  (`"concurrently"`, `"select"`, `"user"`). `quote_ident`, `format('%I')`,
  `pg_get_indexdef`, `pg_get_constraintdef`, `pg_get_expr`, `format_type`
  and `SHOW CREATE TABLE` quote names as PostgreSQL does, with its keyword
  list; serial defaults read `nextval('"T_id_seq"')`, and mixed-case columns
  work in expression indexes and stored defaults. `E''` strings take `\b`,
  `\f`, `\xHH`, octal `\ooo`, `\uXXXX` and `\UXXXXXXXX`, with PostgreSQL's
  errors (`22021`, `22025`, `42601`). Quoted role names are still folded.
- **pg_dump and pg_restore** (#366): PostgreSQL 16's `pg_dump` dumps a datax
  database and the output restores into another: plain, custom and
  directory formats, `--schema-only` / `--data-only`, `--snapshot`, and
  `-Fd -j N` dumps. `TestPgDumpRestore` runs the real tools in CI. On the
  way: `LOCK TABLE` (weak modes), `PREPARE` / `EXECUTE` / `DEALLOCATE`,
  cursors (`DECLARE` / `FETCH` / `MOVE` / `CLOSE`), `pg_export_snapshot()` /
  `SET TRANSACTION SNAPSHOT` (over timestamped reads), `ALTER COLUMN ... ADD
  GENERATED AS IDENTITY`, `ALTER VIEW / SCHEMA public / LARGE OBJECT ...
  OWNER TO`, `ALTER SEQUENCE ... OWNED BY` (it was ignored), `ADD PRIMARY
  KEY` on a table created without one, `COPY FROM` inside a transaction
  block, ACL columns filled from grants, and `datax_*` tables listed in
  `pg_catalog` with 32-bit OIDs. Roles are not dumped, and `pg_restore -j` is
  not recommended yet. See docs/user/pg-dump.md.
- **`JOIN ... ON` takes any boolean condition** (#363): `ON true`, non-equi
  (`u.tid < t.id`, `BETWEEN`), `OR`, conditions on one side, expressions
  (`lower(a.x) = lower(b.y)`), for `INNER`, `LEFT`, `RIGHT` and `FULL` joins,
  in chains and beside `LATERAL`. With a usable equality the key lookup is
  kept and the rest filters matched pairs; without one the join is a nested
  loop over the joined table, read once (`EXPLAIN`: `fetched once, every row
  tried`, `+ join filter`). An `ON` naming a table joined after it is
  `42P01` / `42703`, as in PostgreSQL.
- **Several actions in one `ALTER TABLE`** (#364), as Rails `bulk: true`,
  Prisma and pg_dump write them: applied in order as one atomic schema
  change (one descriptor write when only the descriptor changes, otherwise
  one online publish and flip); a combination that cannot be atomic is
  `0A000`, naming the action. `ALTER TABLE ONLY t` and `t *` are accepted.
- **`ADD COLUMN` with any default** (#364): a stable expression (`now()`) is
  evaluated once by the `ALTER`; a volatile one (`gen_random_uuid()`,
  `random()`, `SERIAL`, identity) is backfilled per existing row online, in
  crash-safe, resumable chunks, with `NOT NULL` and `UNIQUE` checked over
  the rows. `ADD COLUMN ... NOT NULL` without a default works on an empty
  table (`23502` otherwise), and a duplicate `ADD COLUMN` is `42701`.
  Volatile defaults and row-reading actions are refused inside a
  transaction block (`25001`).
- **`COPY ... TO STDOUT`** (#365): `COPY t [(cols)] TO STDOUT` and `COPY
  (query) TO STDOUT` in text, CSV and binary, over both protocols, with
  every PostgreSQL option (`FORMAT`, `DELIMITER`, `NULL`, `HEADER`, `QUOTE`,
  `ESCAPE`, `FORCE_QUOTE`, `ENCODING`, and the `WITH CSV HEADER` spelling);
  the output is PostgreSQL's, byte for byte. A table streams in bounded
  memory under one snapshot, with `SELECT` privileges and row-level security
  applied; psql's `\copy ... to` and pgx `CopyTo` work. `TO 'file'` and `TO
  PROGRAM` are refused (`42501`, `0A000` for admins). `COPY FROM` accepts
  enum labels.
- **Correlated subqueries in grouped queries** (#361): in the select list,
  `HAVING` and `ORDER BY`, over one table or a join, they run once per
  group; a column that is not grouped is `42803`. An ungrouped `SELECT` can
  `ORDER BY` a correlated subquery, and a plain `UPDATE`'s `SET` can read
  the row being updated in a subquery.
- **Errors carry `DETAIL` and `HINT`** where datax sets them.
- **Advisory locks** (#353): `pg_advisory_lock`, `pg_try_advisory_lock`,
  `pg_advisory_unlock`, `pg_advisory_unlock_all`, `pg_advisory_xact_lock`,
  `pg_try_advisory_xact_lock` and their `_shared` variants, keyed by one
  `bigint` or two `int4`. They are cluster-wide: sessions on different nodes
  contend for the same lock. Session locks are reentrant and counted and are
  released by unlock or when the session ends, a client that disconnects
  included; transaction locks at `COMMIT`, `ROLLBACK` or `ROLLBACK TO
  SAVEPOINT`. Unlocking a lock not held returns false with PostgreSQL's
  `WARNING` (`01000`). Waits honour `lock_timeout` (`55P03`),
  `statement_timeout` and cancel (`57014`), and never interact with row
  locks; `pg_locks` lists held advisory locks. Each hold is a record under
  its node's lease: a dead node's locks are free `AdvisoryLockTTL` (10s)
  plus the clock offset after its last renewal, and a node that cannot renew
  ends its sessions holding advisory locks with FATAL `57P01` before another
  session can take them. Rails, Prisma, golang-migrate and Flyway migrations
  work. The arguments must be constants or parameters
  (`pg_try_advisory_lock(id) FROM jobs` is `0A000`). See
  docs/user/sql.md, "Advisory locks".
- **Tables without a primary key** (#354): join tables and imported schemas
  get the same hidden, generated `rowid` key `CREATE TABLE ... AS` has, so
  duplicate rows are allowed as in PostgreSQL; the rowid is left out of
  `SELECT *`, `RETURNING *`, `COPY` and the catalogs. A foreign key to such
  a table needs a unique constraint (`42830`, as PostgreSQL), and "no unique
  constraint matching given keys" is now `42830` (was `42P10`). `ALTER TABLE
  ... ADD PRIMARY KEY` parses: `0A000` on a rowid-keyed table, `42P16` on a
  keyed one. A table keyed only by a hidden rowid, `CREATE TABLE ... AS`
  tables included, no longer shows an empty `t_pkey` in the catalogs.
- **`UPDATE ... FROM` and `DELETE ... USING`** (#355): the list takes
  tables, subqueries, `VALUES`, `WITH` members, views and joins; `SET` and
  `RETURNING` read its columns, and `RETURNING *` returns the target's
  columns then the list's. A target row joining several rows changes once
  (`UPDATE` takes the first joined row). The list's tables need `SELECT`;
  row-level security, constraints and cascades apply. `UPDATE` and `DELETE`
  take a target alias, and a `FROM`-list subquery can be a `VALUES` list
  with column aliases (`(VALUES (1, 'a')) AS v(id, tag)`).
- **`ORDER BY` inside aggregate calls** (#356): `string_agg(b, ',' ORDER BY
  id DESC)`, `array_agg(x ORDER BY y NULLS FIRST)`, `jsonb_agg(... ORDER BY
  ...)`, in grouped queries (expressions, `ROLLUP`), joins, `HAVING` and
  subqueries. With `DISTINCT`, an `ORDER BY` expression that is no argument
  is `42P10` and the input is sorted on the arguments, as in PostgreSQL;
  `array_agg(DISTINCT ...)` and `json_agg(DISTINCT ...)` keep one NULL. On a
  window call it is `0A000`. `mode() WITHIN GROUP (ORDER BY x)` is added; an
  ordered-set call without `WITHIN GROUP`, or `WITHIN GROUP` on a plain
  aggregate, is `42883` (was `42601`). A subquery inside an aggregate's
  arguments, `FILTER` or `ORDER BY` works.
- **System functions ORMs and tools call** (#357): `pg_get_serial_sequence`,
  `to_regclass` / `to_regproc` / `to_regprocedure` / `to_regnamespace` /
  `to_regrole`, `pg_is_in_recovery()`, `current_schemas(bool)`,
  `pg_has_role` and the `has_*_privilege` family in every argument form,
  answered from datax's grants and roles (the stubs that said true are
  gone). `user`, `current_role` and `current_catalog` are keywords; any
  word may follow `AS`. `pg_depend` ties SERIAL, identity and `OWNED BY`
  sequences to their columns (Rails' `pk_and_sequence_for`). `::regclass`
  reads quoted and `public.`-qualified names and lower-cases unquoted ones,
  and a system catalog's OID is PostgreSQL's (`'pg_class'::regclass` is
  1259). `SET synchronize_seqscans`, which pg_dump sends, is accepted.
- **Partial and expression indexes** (#358, #359): `CREATE [UNIQUE] INDEX
  ... WHERE predicate` indexes only the matching rows, uniqueness holding
  among them (soft-delete uniqueness), and the planner uses it when the
  query's `WHERE` repeats the predicate's conjuncts. `CREATE [UNIQUE] INDEX
  ON t ((lower(email)))`, `(a, (b + 1))` and `((doc ->> 'k'))` index
  immutable expressions (`42P17` otherwise), unique on the value, and serve
  equality and range predicates on the same expression. Both show in
  `pg_get_indexdef`, `pg_indexes`, `\d` and `pg_index.indpred`, survive
  `RENAME COLUMN`, `LIKE ... INCLUDING INDEXES` and restore, and are refused
  on sealed tables. Cluster protocol version v22.
- **`ASC` / `DESC` and `NULLS FIRST | LAST` per index key** (#360) are
  accepted and shown; keys stay ascending, and an all-descending `ORDER BY`
  reverse-scans (Rails' `order: { created_at: :desc }`).
- **`datax start --locality-advertise=key=value@host:port[,...]`** (#350;
  alias `--locality-advertise-addr`): a node that shares the locality tier
  `key=value` with this node dials it at that address, for example over a
  private network inside a region, instead of at its `--advertise` address.
  Each tier must be one of the node's `--locality` tiers. A locality address
  is used once a dial to it succeeds within 1s; if it does not answer, peers
  fall back to the public address (logged at most once a minute per peer)
  and try the locality address again every 10s. `--join` addresses are
  dialed as given. The entries travel in the node descriptor (older nodes
  ignore them), and show in `/api/node`, `/api/cluster`, `/status`, the
  console's node list and node page and `datax debug nodes`. New metrics
  `datax_rpc_locality_dials_total` and `datax_rpc_locality_fallbacks_total`.
- **Transaction modes on `BEGIN` / `START TRANSACTION`** (#346):
  `BEGIN [WORK | TRANSACTION]` and `START TRANSACTION` take `SET
  TRANSACTION`'s modes (`ISOLATION LEVEL ...`, `READ ONLY` / `READ WRITE`,
  `[NOT] DEFERRABLE`), so pgx `BeginTx` with `TxOptions` and `database/sql`
  read-only transactions work. `READ ONLY` refuses writes with `25006`;
  isolation stays serializable. A read-only transaction now also refuses
  `nextval()` and `setval()`. An unknown isolation level is `42601`, not
  `22023`. Before, the modes were a syntax error.
- **PostgreSQL's common session settings** (#345): `client_min_messages`,
  `IntervalStyle`, `work_mem`, `maintenance_work_mem`, `synchronous_commit`,
  `jit`, the planner's `enable_*`, `random_page_cost`, `lc_*`,
  `default_transaction_isolation`, `default_transaction_deferrable`,
  `check_function_bodies`, `xmloption`, `default_tablespace`, `row_security`,
  `bytea_output`, `extra_float_digits` and more are recognised instead of
  failing with `42704`, with PostgreSQL 18's defaults, units and value
  checks, in `SHOW`, `current_setting()` and `pg_settings`. Most have no
  effect (listed in `docs/user/postgres-differences.md`); these do:
  `IntervalStyle` (`iso_8601`, as Rails sets, `sql_standard`,
  `postgres_verbose`) and `bytea_output = escape` change the text format of
  interval and bytea columns on the wire, and `IntervalStyle` is reported as
  a parameter status; `client_min_messages = warning` / `error` stops
  NOTICEs; `row_security = off` makes a query that row-level security would
  filter fail with `42501`, as pg_dump expects. `SHOW server_version_num` and
  `max_identifier_length` work, and `SET standard_conforming_strings = on`
  (the first line of a pg_dump script) is accepted. Unknown names still fail
  with `42704`.
- **`CREATE INDEX CONCURRENTLY`, pattern operator classes and the built-in
  extensions** (#347, #348, #349):
  - `CREATE INDEX CONCURRENTLY` and `DROP INDEX CONCURRENTLY` are accepted
    (every index build is already online) and refused inside a transaction
    block (`25001`), as in PostgreSQL. `CREATE INDEX` may leave out the
    index name, which is chosen as PostgreSQL does.
  - `CREATE INDEX` runs inside a transaction block when the same block
    created the table, as Rails and Django migrations do; on other tables it
    is still refused (`25001`), and now aborts the block.
  - Index columns accept `text_pattern_ops`, `varchar_pattern_ops`,
    `bpchar_pattern_ops` and the default `*_ops`, and `COLLATE "C"`,
    `"POSIX"` and `"default"`; `pg_get_indexdef` and `\d` show them. Other
    collations are refused (`0A000`). Django's `_like` indexes work.
  - `CREATE EXTENSION` and the new `DROP EXTENSION` accept `plpgsql`,
    `pgcrypto` (only `gen_random_uuid`), `uuid-ossp` (only
    `uuid_generate_v4`) and `vector` as no-ops, and `pg_extension` and
    `pg_available_extensions` list them as installed. Other extensions are
    still refused. `uuid_generate_v4()` now works outside a `DEFAULT`.
- **Multi-word type names in casts** (#343): `double precision`, `character
  varying(n)` and `timestamp/time with[out] time zone` work after `::` and
  in `CAST`, and `_int4`-style names mean the array type. Before, the first
  three were syntax errors and `_T` was text.
- **`SELECT DISTINCT ON (exprs)`** (#337): of each group of rows equal on the
  expressions, the first in `ORDER BY` order is kept, before `LIMIT` and
  `OFFSET`. The `ORDER BY` must begin with the `DISTINCT ON` expressions
  (`42P10` otherwise, at Describe too); positions and output names stand for
  the select items. Over single tables, joins, derived tables and grouped
  queries; not with window functions (`0A000`). Before, `DISTINCT ON` was a
  syntax error.
- **`GROUP BY` expressions, `ROLLUP`, `CUBE` and `GROUPING SETS`** (#336):
  `GROUP BY` takes any expression (`date_trunc('day', ts)`, `a % 10`), a
  position of an expression item, or an output name that is no input column;
  select items, `HAVING` and `ORDER BY` may use a grouped expression or compute
  over it. `ROLLUP (...)`, `CUBE (...)`, `GROUPING SETS (...)` and `()` group
  by each grouping set and return the union, the keys a set leaves out NULL,
  combined across items as in PostgreSQL; `GROUPING(...)` tells a rolled-up
  NULL from a NULL key, and an empty grouping set has its row over no input.
  Over single tables, joins and derived tables. Before, `GROUP BY` took
  columns and positions of column items only.
- **Expressions over aggregates** (#335): the select list, `HAVING` and
  `ORDER BY` of a grouped or aggregate query take any scalar expression over
  aggregate calls and grouping columns — `SUM(a) / COUNT(*)`, `ROUND(AVG(x),
  2)`, `COALESCE(MAX(x), 0)`, `COUNT(*) * 100.0 / (SELECT COUNT(*) FROM t)`,
  `CASE WHEN COUNT(*) > 1 ...`, casts — with or without `GROUP BY` and over
  joins; an aggregate keeps its `DISTINCT` and `FILTER` inside an expression.
  `HAVING` is a full boolean condition (`OR`, `NOT`, `IN`, `LIKE`, an
  aggregate on either side). The aggregates are computed per group and the
  expression is evaluated over them, and the extended protocol describes the
  result with its real type. A column that is neither grouped nor aggregated
  is refused with `42803`, as is an aggregate in `WHERE`. Before, an
  aggregate could only stand alone in the select list (`SUM(a) / COUNT(*)`
  was a syntax error) and `HAVING` took one comparison per conjunct.
- **Common PostgreSQL functions** (#339): the regular-expression family —
  `regexp_replace` (with flags, start and n), `regexp_match`,
  `regexp_matches`, `regexp_split_to_array`, `regexp_split_to_table`,
  `regexp_count`, `regexp_like`, `regexp_substr` (POSIX patterns run on RE2:
  back references and lookaround are `2201B`); `generate_series` over
  integers, numerics and timestamps with an interval step; the jsonb
  processing functions `jsonb_each`, `jsonb_each_text`,
  `jsonb_array_elements`, `jsonb_array_elements_text`, `jsonb_object_keys`
  (with their `json_` spellings) and `jsonb_insert`; and `timezone(zone,
  ts)` / `ts AT TIME ZONE zone` / `AT LOCAL` in both directions. The
  set-returning ones are tables in `FROM` (`FROM generate_series(1, 10) AS
  g(n)`, `FROM jsonb_each(doc)` with constant arguments) and expand into
  rows in a select list without `FROM`, several in lockstep; `pg_proc`
  marks them `proretset`. `key` reads as a column (jsonb_each's) where a
  value goes. A `::timestamp` cast now yields a timestamp without time zone
  (no offset in its text, OID 1114), as `ts ± interval` and `date_trunc` over
  one do.
- **`ALTER TABLE ... ADD CONSTRAINT IF NOT EXISTS name ...`** (#322), a datax
  extension (PostgreSQL has `IF NOT EXISTS` on `ADD COLUMN` and `CREATE
  INDEX`, both supported, but not here): when the table already has a
  constraint of that name the statement does nothing and sends the NOTICE
  `constraint "name" for relation "t" already exists, skipping`. Only the
  name is compared, never the definition.
- **`UPDATE` may change primary-key columns** (#322). A row whose key
  changes moves: its old row and index entries are deleted and the new
  version written at the new key, with its entries, in the statement's
  transaction; a hash-sharded table files it under the new bucket; a sealed
  table seals it afresh for the new key, and a per-tenant table under the
  new tenant's key when the tenant column changes. Foreign keys apply their
  `ON UPDATE` actions, row security checks `WITH CHECK` on the new row, and
  `RETURNING` shows it. Unique keys — the primary key and unique indexes —
  are now checked for the statement as a whole in every `UPDATE`, so
  `UPDATE t SET id = id + 1` and swaps succeed (PostgreSQL, checking a
  non-deferrable key row by row, refuses the first); before, a unique index
  refused `SET u = u - 1` over consecutive values. Where a foreign key
  references its own table with `ON UPDATE CASCADE` or `SET NULL`, a
  statement that changes both a referenced row and a row the cascade
  rewrites is refused (`0A000`) rather than losing one of the two changes,
  as it could before. `ON CONFLICT DO UPDATE` still cannot change the key.
  Before, any update of a key column was refused with `0A000`.
- **Vector columns and nearest-neighbour search** (#304): `vector(n)` columns
  compatible with pgvector (float4 elements, up to 16,000 dimensions; text
  `'[1,2,3]'` and pgvector's binary format; a fixed type OID found through
  `to_regtype('vector')` or `pg_type`, so pgvector clients' type registration
  works; `CREATE EXTENSION vector` is accepted and does nothing). Distance
  operators `<->` (L2), `<=>` (cosine) and `<#>` (negative inner product),
  `l2_distance`, `cosine_distance`, `inner_product`, `l1_distance`,
  `vector_dims`, `vector_norm`, `l2_normalize`, element-wise `+`/`-`.
  `ORDER BY embedding <-> $1 LIMIT k` is exact without an index. `CREATE
  INDEX ... USING ivfflat (col vector_l2_ops | vector_cosine_ops |
  vector_ip_ops) WITH (lists = n)` adds an approximate index, built online and
  tuned with `SET ivfflat.probes` (default √lists; recall@10 0.98 on a
  synthetic clustered test set — real embeddings were not measured).
  `vector_index_scan = off` forces exact answers. The index's centres are
  trained once and never move: after much change, build a new index and drop
  the old. HNSW is refused (`0A000`) rather than mapped to IVF. Vectors cannot
  be primary keys or in a btree index. New bench workloads `vector-knn` and
  `vector-knn-exact` record recall and build time. Cluster protocol version
  v21. See docs/user/sql.md, "Vectors".
- **Full-text search** (#299): the `TSVECTOR` and `TSQUERY` types and the `@@`
  operator, with `to_tsvector`, `to_tsquery`, `plainto_tsquery`,
  `phraseto_tsquery`, `websearch_to_tsquery`, `ts_rank`, `ts_rank_cd`,
  `ts_headline`, `setweight`, `strip`, `numnode`, `tsvector_to_array` and
  `tsvector || tsvector`. Queries support and, or, not, phrases (`<->`,
  `<N>`), prefixes (`word:*`) and weights. Two configurations: `simple`, and
  `english` with a Snowball stemmer and stop words, chosen by
  `default_text_search_config`. A GIN index on a `TSVECTOR` column, or on
  `to_tsvector('english', text_column)`, makes `@@` an index lookup (`EXPLAIN`
  shows an `inverted scan`); every row it finds is rechecked, so a phrase's
  word order and a weight are verified against the real document. Ranking is
  computed on the matching rows: `ORDER BY ts_rank(...) LIMIT k` reads every
  match. The tokenizer and stemmer are datax's own and differ from
  PostgreSQL's in places (hyphenated words, URLs, some numbers; the stemmer was
  not checked against PostgreSQL's): see docs/user/postgres-differences.md.
  Columns of the new types and full-text indexes need cluster protocol
  version v21. The bench suite gains a `text-search` workload, and
  `bench/workloads-large.json` runs it over a million documents.
- **Large objects** (#303): values too big to be a row value — files,
  recordings, tens of megabytes to many gigabytes — are stored in 256 KiB
  pages and written and read as a stream through PostgreSQL's large-object
  functions (`lo_create`, `lo_open`, `lowrite`, `loread`, `lo_lseek64`,
  `lo_truncate64`, `lo_unlink`, `lo_get`, `lo_put`, `lo_from_bytea`), from SQL
  (pgx's `tx.LargeObjects()`) and through the fast-path function call (libpq's
  `lo_*`, psql's `\lo_import` / `\lo_export`). An object created and written
  inside one transaction block can be of any size up to 1 TiB, holds no lock
  while it uploads, and appears whole at `COMMIT`; a rollback or a lost
  connection leaves no object, and a sweeper removes its pages and those of
  unlinked objects. Changes to an existing object are ordinary transactional
  writes, at most 64 MiB per transaction. A three-node cluster writes a 512 MiB
  object with under 100 MiB of memory growth across the whole process, where a
  32 MiB `BYTEA` INSERT takes over a gigabyte (1 GiB was measured on one node
  only). Full and incremental backups, sealed or not, carry large objects;
  `restore --table` does not. Ordinary `BYTEA`/`TEXT`/`JSONB` values are still
  stored whole: there is no out-of-line storage for column values. Stored in
  three reserved tables (`datax_largeobject*`); cluster protocol version v21.
  See the SQL guide's "Large objects".
- **Row-level security** (#294): a table shared between tenants can confine a
  role to its own rows. `ALTER TABLE t ENABLE | DISABLE | FORCE | NO FORCE ROW
  LEVEL SECURITY`; `CREATE POLICY p ON t [AS PERMISSIVE | RESTRICTIVE] [FOR ALL
  | SELECT | INSERT | UPDATE | DELETE] [TO roles] [USING (expr)] [WITH CHECK
  (expr)]`, `ALTER POLICY`, `DROP POLICY`; the `BYPASSRLS` role attribute.
  PostgreSQL's rules: default deny, permissive policies OR-ed and restrictive
  ones AND-ed, `42501` for a refused row. The policy is applied as a leading
  WHERE condition on every read of the table — scans, every index, joins,
  subqueries, aggregates, `ON CONFLICT`, `COPY`, `RETURNING`, locking reads,
  prepared statements, `AS OF SYSTEM TIME` (filtered by today's policies) — and
  bounds the scan, so a tenant-keyed policy reads one tenant's span. The owner
  and admins are exempt unless `FORCE`; `root`, `BYPASSRLS` roles and the
  node's own jobs always. Custom session settings (`SET app.tenant = ...`,
  `current_setting`, `set_config`) name the tenant for a connection. A filtered
  role is refused `TRUNCATE` and is shown no table statistics, no decoded range
  boundaries in the console, and — like every non-admin now — no other role's
  statement text in `SHOW SESSIONS` / `pg_stat_activity`. Foreign keys and
  unique indexes still see the whole table, as in PostgreSQL. Not supported:
  subqueries in a policy, security-invoker views. Behaviour follows
  PostgreSQL's documented rules; it was not compared against a running
  PostgreSQL. Cluster protocol version v21. See docs/user/security.md,
  "Row-level security".
- **Restore a table, or a database, into a running cluster** (#301): `datax
  restore --table db.t [--into name]` and `--database db [--into name]` bring
  tables back from a backup chain beside what is there; nothing else is
  touched. A restored table takes a new table id and its rows are re-keyed to
  it (indexes, GIN indexes included, come back as stored, no backfill).
  Everything is checked before anything is written; rows load in ordinary
  transactions under an id nothing names, are read back against the backup's
  checksum, and one transaction makes the tables visible, complete. A failed
  restore removes its rows; one whose node died is cleaned up by a janitor on
  any node (`restore-reclaim` in the operations view) and is run again, not
  resumed. A name that exists is refused — there is no replace flag; drop and
  rename with SQL. A foreign key to a table outside the restore is refused
  unless `--drop-foreign-keys`. Owned sequences come back as new sequences;
  grants come back for roles the cluster has. Views, enum columns into another
  database, and tables backed up mid-schema-change are refused. Cluster
  protocol version v21. See docs/user/operations.md, "Restoring a table into
  a running cluster".
- **Point-in-time restore** (#302): `datax backup --base … --revision-history`
  keeps every version written since the base, and `datax restore --as-of
  <time>` (RFC 3339 or Unix nanoseconds, the timestamp `AS OF SYSTEM TIME`
  takes) restores to any moment of such an interval, or to exactly the end of
  any backup in a chain — everything into an empty cluster, or chosen tables
  into a running one. Transactions are restored whole or not at all. A moment
  before the full backup, after the last backup, or inside an interval taken
  without history is refused with what the chain restores to. A history
  incremental must be taken within the GC TTL of its base and is refused
  otherwise; nothing holds garbage collection back for a chain. Table
  definitions, roles and sequences are kept as of each backup's end: a table
  created or altered inside the interval restored into is refused. A history
  backup's manifest has a new magic (`DXBK2`), which earlier binaries refuse;
  plain backups are unchanged. Taking one needs cluster protocol version v21.
  Sealed chains work the same. Backup and restore summaries print the
  backup's timestamp, and for a history backup the version count per table.
- **GIN indexes on JSONB columns** (#300). `CREATE INDEX name ON t USING GIN
  (doc)` builds an inverted index — one entry for every value in each
  document with the path to it, and one for every top-level key — and
  `doc @> '...'`, `doc ? 'k'`, `doc ?| '{a,b}'` and `doc ?& '{a,b}'` read it
  instead of scanning the table (`EXPLAIN` shows an `inverted scan` and its
  lookups). Results are those of the filter: every row the index finds is
  checked against the whole `WHERE` clause. The index is built online,
  maintained in the row's transaction (an `UPDATE` rewrites only the entries
  of the values that changed), and follows the table through `TRUNCATE`,
  re-sharding, backup and restore. One column, never `UNIQUE`;
  `jsonb_path_ops` is accepted and builds the same index; a document with
  more than 8,192 distinct values is refused (`54000`) on an indexed table.
  `<@`, negations, extracted-value comparisons and containment under a `->`
  path still filter. `SET inverted_index_scan = on | off | auto` overrides
  the planner's choice. On 50,000 documents a containment matching 50 rows
  takes 0.6 ms through the index against 240 ms as a filter (not measured at
  a million documents). Cluster protocol version v21: `CREATE INDEX ...
  USING GIN` is refused until the upgrade is finalized. `datax bench
  jsonb-contain` measures both paths.
- **A GC window per table** (#295). `CREATE TABLE … WITH (gc_ttl = '10m')` and
  `ALTER TABLE … SET / RESET (gc_ttl)` give one table its own GC window,
  shorter or longer than the node's 25h. It governs the ranges that hold only
  that table (the statement splits the table's edges, and housekeeping asks
  again if a range still straddles one); a range shared with other data keeps
  the longest window of what it holds, so no other table's history changes.
  A short window shortens `AS OF SYSTEM TIME`, follower reads, long
  transactions and the age an incremental backup's base may have, for that
  table: older reads fail with an error. Not per database. Cluster protocol
  version v21.
- **Purge** (#295). `ALTER TABLE t PURGE` and `datax debug purge` (a table or
  a key range) collect deleted and superseded versions now instead of a GC
  window later, truncate the raft log, compact the span on every replica,
  retire the write-ahead logs written before it, and return what each replica
  then holds, counted from the storage engine's keys. The result — complete
  only when every replica answered with no superseded version, tombstone or
  intent — is written whole to the audit log; an incomplete purge says which
  replica and why, and can be run again. Readers older than the purge are
  refused on that span, never answered short. A replica that was down applies
  the purge when it rejoins and serves no follower read until it has heard
  from its range's leader again; one that is partitioned but still running is
  not covered until it reconnects. Backups taken earlier still hold the data.
  Cluster protocol version v21.
- **Row expiry on ordinary tables** (#298). `WITH (ttl_expiration = col)`, or
  `WITH (ttl_column = col, ttl = '7 days')`, on any table that is not a
  time-series one; `ALTER TABLE … SET / RESET` on a table with data. One
  background job for the cluster deletes expired rows in batches with ordinary
  `DELETE`s, so secondary indexes, `ON DELETE CASCADE` and restricting foreign
  keys behave as for any delete (a row still referenced is skipped, counted and
  retried). An expired row is visible until the job deletes it, normally
  within 30 seconds. The option creates an index on the column if none leads
  with it. `ttl` without a column is refused: a lifetime is counted from a
  timestamp column you name. Metrics `datax_row_ttl_rows_deleted_total`,
  `datax_row_ttl_rows_blocked_total`, `datax_row_ttl_lag_seconds`; passes
  appear in the operations view. Cluster protocol version v21.
- **An audit table** (#293). Audit records are now also kept in the cluster,
  in the `datax_audit` system table, once v21 is finalized: readable with SQL
  by admins and by roles granted `SELECT` on it (not `read_all`), and not
  changeable through SQL by any role, `root` included — only
  `ALTER TABLE datax_audit SET (retention = ...)` (admins, at least a day;
  default 90 days) is accepted. Each node's records form a hash chain;
  `datax debug audit export | heads | verify` exports them as JSON lines and
  verifies them outside the cluster, detecting removed, altered and reordered
  records, and — against heads saved earlier — a chain rewritten underneath
  SQL. Recording never delays or fails the audited action: events are
  buffered in memory and written about once a second, a node without quorum
  writes them when it can, and what a full buffer drops is counted
  (`datax_audit_dropped_total`) and recorded as an `audit-gap`; the log lines
  remain. Offline commands (which run with no cluster) stay log-only, and a
  node that is killed loses what it had buffered. `/api/security` reports
  each node's chain head. New flag `--audit-table=false`.
  Cluster protocol version: v21.
- **Roots in a key management service** (#308): `--enc-root` takes
  `vault:<address>/<mount>/<key>` (HashiCorp Vault Transit),
  `awskms:<key arn>`, `gcpkms:projects/…/cryptoKeys/<key>` and
  `azurekv:<vault>/<key>[/<version>]`. Wrapping the store key is a call
  to the service, whose key never reaches datax; no helper process and
  no cloud SDK (standard library only). Options follow the key as
  `?name=value&name=value`; credentials are never options (token file,
  environment, instance identity). Requests have a timeout and bounded
  retries with backoff; a node cannot start while its KMS is
  unreachable and says which service, key and why; a running node is
  unaffected. Each wrapped store key is bound to its cluster and node
  (encryption context / associated data, and a digest inside the
  ciphertext), so one node's is refused as another's; a new node's key
  is bound once it has an id. The root's health is checked every minute
  and shown in `/api/security` (`root.healthy`, `root.health_error`,
  `root.bound`, `root.latest_key_id`), the console's security view, the
  health document (`enc-root-unreachable`) and the gauge
  `datax_enc_root_healthy`. The key id carries the service's key
  version; after the service rotates its key,
  `datax debug enc-root rewrap` moves a node to it. **Tested against
  fakes of each API, not against the services themselves.** PKCS#11 is
  not spoken: an HSM is reached through `exec`. See docs/encryption.md,
  "A root in a key management service".
- **`LISTEN` / `NOTIFY`** (#297). `LISTEN channel`, `UNLISTEN channel | *`,
  `NOTIFY channel [, 'payload']` and `pg_notify(channel, payload)`, delivered
  to listening sessions on every node of the cluster and pushed to an idle
  connection, so pgx's `WaitForNotification` and other drivers' listeners
  work as they do against PostgreSQL. The semantics are PostgreSQL's: a
  `NOTIFY` in a transaction is delivered if and when it commits (`ROLLBACK`
  and `ROLLBACK TO SAVEPOINT` discard it), identical notifications in one
  transaction collapse, `LISTEN` takes effect at commit, a session hears
  itself, channels belong to a database, a payload is under 8000 bytes
  (`22023`), and there are no privileges on channels. Nothing is stored: a
  notification goes to the sessions listening when it commits, at most once.
  A session that stays connected receives every one or is disconnected — with
  FATAL `54000` if it let 16 MiB pile up unread, or `57P01` if another node
  had to drop what it could not deliver to this one (it keeps 64 MiB per
  unreachable node for up to five minutes, and delivers late otherwise). The
  exception is a node that crashes between a commit and handing the
  notification to its peers. Notifications from one node arrive in its commit
  order; those from different nodes are not ordered against each other.
  `pg_notify` runs once per statement and its arguments cannot name columns.
  No cluster version change: a node on an older release is skipped by the
  fan-out and cannot serve `LISTEN`.
- **`SELECT ... FOR UPDATE SKIP LOCKED` and `NOWAIT`** (#296). `SKIP
  LOCKED` leaves out rows another transaction holds — or changed and
  committed since this transaction's snapshot — and locks the rest;
  `LIMIT n` counts the rows returned, so `SELECT id FROM jobs ORDER BY
  id LIMIT 1 FOR UPDATE SKIP LOCKED` is a job queue whose workers do not
  wait on each other. Skipped rows are not reads of the transaction and
  cannot fail it with `40001` later (on a primary-key scan; a queue
  ordered by a secondary index can still see one). `NOWAIT` fails at
  once with `55P03`. Neither aborts a lock holder. Adds a read-only
  `skip_locked` field to Get and Scan; no cluster version change (an
  older node ignores it and the read waits as before).
- **Sealed backups** (#291). `datax backup --backup-key <file>` seals every
  file of a backup — the manifest included — with AES-256-GCM under a key
  given at backup time (32 raw bytes or 64 hex characters), and
  `datax restore --backup-key` reads it. Each backup has its own random data
  key, wrapped under the backup key; each file is sealed in 64 KiB chunks
  under a key derived from the data key and the file's name, with the name,
  each chunk's position and the end of the file authenticated, so a changed
  byte, a truncated file, swapped files or another backup's manifest are
  refused before restore applies anything. A sealed backup is never read
  without its key and a plaintext backup is refused when a key is given.
  Chains may mix keys: restore takes one key for the chain or a
  comma-separated list matching `--src` (an empty entry for a plaintext
  backup); `--base-key` opens a base sealed under a different key. On an
  encrypted store `--backup-key` satisfies the plaintext gate;
  `--allow-plaintext` remains the explicit way to write an unsealed backup,
  and the two together are refused. The key is sent in the admin request:
  over mutual TLS, or on an insecure node only from the node's own machine;
  it is never logged or audited. Summary checksums are over the plaintext
  and match an unsealed backup of the same data. Still visible in a sealed
  directory: table count, table ids, file sizes. Backup keys are key files
  only (no `--enc-root`/KMS yet).
- **A manual split sticks** (#265). A range boundary made on purpose —
  `ALTER TABLE ... SPLIT AT`, `datax debug split` — was folded back by
  housekeeping within seconds whenever the two halves were small, which
  the halves of a pre-split always are. Such a boundary is now marked
  on its range (a sticky bit on the descriptor, replicated like the rest
  of it) and the merge queue leaves it alone. `SPLIT AT ... WITH
  EXPIRATION <timestamp or interval>` holds it only until then;
  `ALTER TABLE ... UNSPLIT AT VALUES (...)`, `UNSPLIT ALL` and `datax
  debug unsplit` release it, after which the ranges merge when they are
  small, and `DROP TABLE` and `TRUNCATE` release a table's boundaries
  with it. Splitting at a boundary that exists marks it. Splits the
  cluster makes for itself — by size, by load, and the pre-splits of
  timeseries tables, re-shards, placement and restore — are not sticky
  and merge back as before; `datax debug merge` merges a range whether
  or not its neighbor is sticky. `datax debug ranges`, `/api/cluster`
  and `/api/range` show the mark.
  Cluster protocol version: v20. Until it is finalized a manual split
  is an ordinary one and `UNSPLIT` and `WITH EXPIRATION` are refused;
  boundaries made before finalize are marked by running the `SPLIT AT`
  again.
- **bench: errors during a measured run fail the run** (#267). `datax
  bench run` runs every workload of the set, then exits non-zero if any
  did not complete or counted more errors than `--max-errors` (default
  0; `-1` disables; a workload's `"max_errors"` in the set takes
  precedence), naming each failing workload with its error count and
  most frequent error. The single-workload commands take the same flag.
  Serialization retries (40001) are not errors. `bench/run.sh` runs both
  targets before failing, and the nightly is red on that status; its
  summary and artifact are produced on red nights too.
- **A root key split among operators, and a sealed start** (#306, the
  second piece of #292). `datax debug enc-root split --shares 2/3`
  makes a root key, hands out three shares of which any two rebuild it
  (Shamir's scheme, standard library only), and keeps nothing. A node
  started with `--enc-root shares` starts **sealed**: it listens on its
  RPC address but its store, SQL port and console stay closed — to the
  cluster it is down — until enough share-holders have each run `datax
  unseal --addr <node>` and typed a share, which is asked for without
  echo and is never a flag. `datax debug seal-status` shows how many
  shares are in and whose. A mistyped share, a repeated one, a share of
  another cluster's key or of another split is refused as it arrives,
  and a set that does not rebuild the key it names never opens
  anything. Only the root user's certificate may unseal (a sealed node
  cannot read its roles); without TLS, only the node's own machine.
  `enc-root resplit` makes a new split of the same key; to replace the
  root key itself, the new key's shares are submitted to the running
  nodes and each store key is rewrapped under it. Shares and root keys
  are never written to disk, logged, or put in an event.
- **Peer unseal: a restarting node gets the root key from the running
  cluster** (#307). Under a root key held as shares, a sealed node asks
  its peers before waiting for people, so a rolling restart needs no
  operator and a cold start needs share-holders for one node only. A
  node hands the key over only to the cluster's node certificate, for
  this cluster and its own root key, for a member that is not being
  decommissioned and is not live (a second copy of a running node gets
  nothing); the key crosses sealed under a key agreed for the one
  exchange. Every request is audited and counted, granted or refused.
  `datax debug peer-unseal --off` makes every start need shares. With it
  on, whoever holds a node's certificate key and can reach the cluster
  while that node is down is a node; the documentation says what that
  means for where the node key is kept.
- **Keys are held in locked memory, and an encrypted node cannot be
  core-dumped** (#309). The store key, the registry's data keys and
  root keys live in memory that is `mlock`ed and marked
  `MADV_DONTDUMP` on Linux and zeroed on release, instead of on the
  heap. A node started with `--enc-key` or `--enc-root` sets its
  core-file limit to zero and marks itself not dumpable
  (`--allow-core-dumps` undoes that for debugging). A memory-lock limit
  too low to lock 16 KiB is a start-up warning with the remedy, or a
  refusal with `--require-locked-keys`; `/api/security` and the
  console's security view report the state. A cipher's expanded key and
  copies on a stack are not covered, and the documentation says so.
- **Console: rows that open are real links, cards sort on a phone, and
  time axes tick at round moments** (#286, #289). A table's name in the
  schema list and a range's id in the ranges table are anchors, so
  ctrl-, cmd- and middle-click open a new tab and the address can be
  copied; the row is still the single tab stop. On a narrow screen the
  nodes, failure domains and schema cards have a "sort by" list and a
  direction button sharing the header's sort state. A chart's time axis
  is labelled at multiples of a round step on the reader's clock, three
  to seven ticks. A heading that holds a filter or "copy as CSV" is
  underlined under its words only.
- **A store key can be kept under a root instead of in a file** (#305,
  the first piece of #292). `--enc-root file:<path>` or `--enc-root
  exec:<command>` is the alternative to `--enc-key`: the node makes its
  own store key and the data directory holds it only wrapped — by a
  wrapping key kept elsewhere, or by a command the operator supplies
  that talks to a TPM, a cloud KMS's CLI or an HSM tool. A store key is
  then in the clear only in the node's memory. Rotation under a root
  involves no key file and no key in transit: the node makes the new
  key, the root wraps it, and the request carries only the operation's
  name. `datax debug enc-root` re-wraps a key (after the root's own key
  rotates, or to move to another root), adopts a root on a key-file
  store, releases one, and reports what wrapped a store's key; the
  console's security view says where each store's key lives. A root
  that fails, hangs, or hands the key back unwrapped is refused before
  anything is written; a crash at any point of a rotation leaves a
  store that opens; and no request can name a command for a node to
  run. `--enc-key` is unchanged.
- **Tables sort from their headers, and the data view has one table of
  ranges** (#282, #283). The nodes, failure domains, schema and ranges
  tables sort by a column from a small arrow beside the header's words
  (after a name, before a figure, so the words stay over the column):
  once sorts a figure largest-first and a name A-first, again reverses
  it, a third time returns the table's own order. A row with no figure
  in that column stays last either way, a poll redraws the table in the
  order chosen, and "copy as CSV" follows it. The header's words are
  still the glossary term, so clicking them still explains the column.
  On a phone, where these tables are cards, there is no header to sort
  from. "Range hotspots" and "Ranges" were two tables over the same
  ranges with different columns, and for a few seconds after a merge
  the first listed ranges that no longer existed. They are one table:
  range, span, replicas, leader, QPS and size, the heaviest first, with
  the filter and the 200-row cap the ranges table had. Leader, QPS and
  size are what each node advertises for its heaviest and largest
  ranges, so a quiet, small range shows a dash; the CSV gains the three
  columns.
- **Charts say what they show** (#277). A chart is titled with what its
  series measures, in the catalog's words, with the metric's name
  beside it — "p99 KV batch latency `kv.batch_p99_us`" where it said
  only the name, on a line of its own on a phone — and its unit is a
  word. Gridlines fall on round
  values (`0, 500, 1000, 1500, 2000` where the axis was its maximum cut
  in four, `391, 783, 1174, 1566`), in powers of 1024 for bytes, and
  are labelled at the precision of their step — `0, 1 s, 2 s, 3 s`,
  `0%, 5%, 10%, 15%`, `0 B, 512 MiB, 1 GiB`; a
  series flat at zero has two gridlines where it had five labelled
  `1 B, 1 B, 1 B, 0 B, 0 B`. Line-end labels stay inside the plot when
  lines end together on the baseline. A window under ten minutes
  labels its time axis to the second. The note about marked events is
  said once for the page, not under every chart. And when the data
  begins more than halfway through the range — a cluster ten minutes
  old under "1h" — the charts start where the data does, all of them
  alike, and a line says so. `/api/metrics` results carry each series'
  `help`.
- **Index usage that survives a restart** (#208, closing #202 with
  #209). "Is anyone using this index" had no answer: the executor kept
  no count, and a count in memory would have reset with the node. Now
  the executor charges every scan and the rows it reads to the index
  the plan went through, where the scan opens — a secondary-index
  lookup to that index, a primary-key point and a full scan to the
  primary — and the metrics recorder writes the counts as
  `index.scans{index=table.name}` and `index.rows{…}` every tick, so
  they live in `datax_metrics` with the rest of the history. The table
  page's indexes list gains a usage column: scans and rows summed over
  the retention window across every node that recorded, with the most
  recent interval in which the count rose, and the primary key listed
  so the figures read as a ratio; `/api/index-usage?table=t` (admins)
  is the same document. Zero means no recorded scans in that window on
  those nodes, which the note under the table says is not the same as
  unused — a node with recording off, a workload whose cycle is longer
  than the window, or a cluster younger than it all leave a used index
  at zero, and someone will drop an index on this column.
- **Statement shapes over time** (#209). The fingerprint table is a
  cumulative count since the node started: it says which shape costs
  the most and nothing about when, so "was this slow yesterday" and
  "what did the workload look like during the incident" had no answer.
  Every tick each node now writes its 50 busiest shapes by total time
  — executions, time, rows scanned, retries, each a counter keyed by
  fingerprint — into `datax_metrics_shapes`, a second time-series
  table with a day's retention, and `/api/metrics` charts them like
  any counter (`stmt.total_us{fp=…}`, as a rate). A shape that falls
  out of the top 50 stops being written rather than written as zero,
  so the chart shows the gap; a restart shows as the counter's reset.
  A fingerprint is not a chart label, so the shape's text — the
  normalised statement with every literal replaced and its identifiers
  kept, so it carries no value but does name the schema —
  is kept in `datax_statement_shapes` and resolved by the catalog for
  admins: the picker lists the statement beside the series, the
  chart's title carries it, and the shapes view's detail has a "chart
  this shape" link. The representative text, which can carry data,
  stays in memory and admin-gated as before. The two tables are
  reserved like `datax_metrics`, created once the cluster has
  finalized **v19** (clusters upgraded from earlier need `datax debug
  upgrade`), and excluded from backups unless `--include-metrics`.
  Cluster protocol version: v19.
- **Brush to zoom on the charts** (#206). The time range was five
  presets ending at now, so the eleven minutes around 14:20 meant
  picking an hour and reading a fifth of the width, and yesterday
  afternoon meant a day or a week and squinting. Drag across any chart —
  on the metrics view, a node's page or the transactions charts — and
  every chart on the page narrows to the window dragged out; the band
  and its edge times are drawn while dragging, so what is about to be
  picked can be seen before it is. The window rides in the address
  (`range=custom&from=…&to=…`), so a narrowed chart is a link someone
  can paste into an incident channel; the header's range picker shows
  the window as its own entry while it is in force, and choosing a
  preset returns to one that ends at now. A selection narrower than
  eight of the chart's buckets is widened to eight: below that the
  server answers with the same samples drawn wider, which is resolution
  the data does not have. The keyboard has the same path (#149): focus
  a chart, `[` and `]` mark the window's ends at the crosshair with the
  band showing the selection so far, Enter applies it, Escape drops it,
  and `-` widens the window to twice its span. Panning is deliberately
  not included; selection plus a shareable address is most of the
  value.
- **A health finding has a history** (#207). A problem on the panel was
  a snapshot: it existed or it did not, and a row that appeared two
  minutes ago and one that had been there since Tuesday were the same
  row. Now each row says when this node's checks first found it — a
  moment, so it reads the viewer's way — and the two transitions, the
  problem appearing and the same problem clearing with how long it was
  open, are recorded on the node's event ring under the kind `health`.
  Transitions only: the checks run every few seconds, and a ring of
  five hundred entries fed one record per run per open problem would
  hold an hour of "still broken" and nothing else. A problem keeps its
  identity, and its date, by check, node and range; its summary carries
  figures that move every run, and a row re-dated whenever its figure
  moved would answer "when did this start" with "just now", always. A
  check that changes severity — `node-unresponsive` becoming
  `node-down` — is a clear and an appearance, in that order. Each row's
  **history** link opens the operations view filtered to the records
  that name its check (`#/ops?kind=health&q=<check>`; the view now reads
  both from the URL, so a filtered timeline can be shared), which is
  where "has this been flapping" is answered. `/api/health` carries the
  date as `since_unix_ms`.

  The issue asked for a decision on acknowledging a finding before one
  was built, and the decision is its third option: not in the console.
  An acknowledgement is state that outlives a process and belongs to
  the whole cluster, and the console does not write to the cluster
  (#144); the place with silences that carry an owner and an expiry is
  whatever alerts on `datax_health_problems`. A row that says how long
  it has been open is one an operator can decide about at a glance,
  which is most of what the permanently amber panel needed.
- **The operations view answers for the cluster** (#210). The event
  ring is per node, so the view showed whichever node served the page,
  and the operation an operator most wants to watch — a decommission
  driven by the draining node, a backup running where it was started, a
  re-encryption sweep of one store — was, by construction, usually
  running somewhere else; the per-node label was honest and did not
  stop anyone concluding the cluster was idle. A new `/api/operations`
  asks every node for its paired operations over the internode RPC, all
  at once rather than one after another (a node that does not answer
  costs the whole timeout, and the cluster's operations are wanted most
  when a node is not answering), and merges them with the node kept on
  each row. Partial is the normal case: a node that did not answer is
  named with the reason and its heartbeat age beside the rows that
  arrived, never a failed document. Under the whole-cluster scope the
  operations view's two tables and the overview's *Operations in
  flight* read it, with a node column and a link to that node; under a
  node's scope nothing changes. Admin-gated and polled every ten
  seconds, like the other fan-outs. The rings themselves stay per node:
  merging five hundred-entry rings into one feed would bury each node's
  record under the others', and the view already keeps the operations
  apart from the raw records.
- **The console's viewer preferences live in the cluster** (#204). Two
  display choices — light/dark/system, and whether a moment reads as how
  long ago it was or as the clock time it happened at — plus the place
  they are kept. They are not kept in the browser. A preference stored in
  one browser profile is not a preference at all to an operator who
  opens the console from a laptop, from a jump host, and from whichever
  node answered the load balancer that morning; it is three preferences,
  two of which are wrong. We are a database, so the cluster holds them:
  `/api/prefs` keys a preference by the signed-in user, and a choice made
  on one node's console is the choice on every node's console, in every
  browser that user signs in from, and after they clear site data.

  The theme override wins over the operating system's own setting in both
  directions — dark on a machine set to light, and light on a machine set
  to dark — by stamping `data-theme` on the root element. Absolute
  timestamps render in the reader's own zone with the offset spelled out
  (`14:20:05 +01:00`, dated when it is not today), because an incident is
  reconstructed against wall clocks and a bare clock time from an unknown
  zone is a trap. The choice reaches moments only: how long a transaction
  has been open, or a connection idle, is a length of time and keeps
  reading as one.

  The issue also asked for a reduced-motion preference. Nothing in this
  console moves — no transition, no animation, no smooth scrolling — so a
  `prefers-reduced-motion` block would have guarded nothing, and shipping
  one would have been a decoration that reads as a feature. What ships
  instead is a test that fails the day motion is added, and says to wrap
  it in the guard.

  The two controls live in a `display` menu in the header rather than
  loose beside `scope` and `range`: those are working controls an
  operator drives all day, these are set once and then never again. The
  control group is also a flex row that does not wrap, so putting them
  there made the page scroll sideways at every phone width — it wraps
  under 640px now, and the measurements are in the pull request.

  A preference write is bounded. `/api/prefs` writes through the node's
  internal system session, which bypasses privilege checks — so the
  principals it admits include ones that cannot write a byte over
  pgwire: a `SELECT`-only user, a metrics scrape account, a read-only
  certificate identity. Storing a value that is already stored is now
  suppressed entirely, and what remains meets a per-principal token
  bucket (ten back-to-back), so a caller with no write privilege
  anywhere cannot drive an unbounded stream of distributed transactions.
  A console re-sending its own value never meets the bucket, because the
  suppression comes first.
- **The key service** (#310, step 2; docs/design-key-service.md, "A. The key
  service"). Applications keep named keys in datax and wrap and unwrap their
  own data keys under them, as with a cloud KMS: `CREATE ENCRYPTION KEY k`
  (admins; the creator owns it), `ALTER ENCRYPTION KEY k ROTATE | DISABLE |
  ENABLE | OWNER TO role`, `DROP ENCRYPTION KEY [IF EXISTS] k` (destroys
  every version and purges the key's record's history), `SHOW ENCRYPTION
  KEYS`, and `GRANT | REVOKE WRAP | UNWRAP | USAGE ON ENCRYPTION KEY`.
  `datax_key_wrap(name, data_key, context)` and `datax_key_unwrap(name,
  wrapped, context)` return a blob and the data key; `datax_encrypt` and
  `datax_decrypt` keep the key in datax for values up to 64 KiB. A blob
  carries the key's id and version (unwrap needs no version, rotated keys
  keep opening old blobs) and is AES-256-GCM bound to the key, version and
  context; a wrong context, another key or a damaged blob all fail with one
  error (`39000`). WRAP and UNWRAP are separate privileges and USAGE is
  both; the owner holds both and an admin who does not own a key must be
  granted them. A disabled key refuses every function. Every call is
  audited in the audit table (role, key, version, operation, outcome; never
  material or context; identical uses within a flush fold into one record
  with a count) and rate-limited per role on each node by the new cluster
  setting `key_service.rate_limit` (`SET CLUSTER SETTING`; default 100 a
  second; refused with `53400`). Gateways cache key material under a
  10-second lease that checks the key's generation, and `DROP`, `DISABLE`,
  `REVOKE` and `OWNER TO` wait it out, so a key is out of use everywhere
  when they return. The functions are refused on a connection that is
  neither TLS nor loopback. `DROP ROLE` refuses a key's owner, `REASSIGN
  OWNED` moves keys, `DROP OWNED` revokes grants on keys and refuses to
  destroy owned keys. `datax_keys` gains `owner` and `privileges`. Needs
  cluster version v22 and the cluster key enabled. See docs/sql.md,
  "Encryption keys", docs/encryption.md, "The key service", and
  docs/encryption-keys-for-applications.md.
- **The cluster key and the `datax_keys` table** (#310, step 1 of the key
  service in docs/design-key-service.md). `datax debug cluster-key init`
  makes one 32-byte cluster key, hands it to every member over mutual TLS,
  and records it (id, version and a check value, never the key) once every
  member has kept its own copy, wrapped by its root, beside its wrapped
  store key (`CLUSTER-KEY`); it is refused unless the cluster is at v22,
  secure, and every member not being decommissioned is live and runs under
  `--enc-root` (a key-file node is named, with the pointer to `datax debug
  enc-root adopt`). An enabling that cannot reach every node is finished,
  with the same key, by running it again. A node that joins later or loses
  its copy is handed the key by a running peer under peer unseal's checks
  (node certificate, member, not being decommissioned, and a live asker
  only from the host it is registered at); every hand-over is audited and
  counted (`datax_cluster_key_handovers_total`). `rotate` hands out a new
  version, rewraps every named key and destroys the old version, resumably;
  `sync` hands the key to live nodes that lack it; `export --to-root` writes
  the cluster's keys to a file wrapped under a root the operator names, and
  `import --from-root` gives them to another cluster, whose whole-cluster
  restore of the first one's backups then rewraps the restored keys under
  its own key (a restore of named keys this cluster cannot open is refused
  before anything is applied). Named keys live in the new system table
  `datax_keys`, each version sealed under the cluster key with its id, name,
  version and kind as associated data; no SQL session may read the table,
  root included, and `SELECT * FROM datax_keys` shows admins the keys'
  metadata without the material. The Go API is `pkg/keystore` (create, get,
  list, unwrap, rotate, disable, enable, destroy with a purge of the row's
  history, and a per-key generation for later caches). `/api/security`
  and the security view report the cluster key and which nodes hold it. Cluster protocol version v22. See
  docs/encryption.md, "Cluster key".
- **Sealed tables, one key per table** (#310, step 3). `CREATE TABLE ...
  WITH (encryption_key = 'auto')` gives the table a key of its own in
  `datax_keys`, and every row value is encrypted under it (AES-256-GCM, the
  table, key, key version and row bound in) in the SQL layer before it
  reaches KV; replicas, snapshots, backups and the consistency checker see
  ciphertext. **Primary-key and indexed columns are not sealed**: `CREATE
  TABLE` and `CREATE INDEX` say which columns stay plaintext in a `NOTICE`.
  Queries, constraints and every index kind work as before; sealed columns
  get no statistics. `DROP TABLE` destroys the key, so the table's rows are
  unreadable at once, `AS OF SYSTEM TIME` included. A row whose key is
  destroyed, disabled or unavailable fails the statement with `XXK01` naming
  the table, within 10 seconds of the change on every gateway (the key
  cache's lease). A table restore shares the source table's key, or brings
  it back from the backup. Refused for time-series tables and before the
  cluster key is enabled; per-tenant keys (`encryption_key_by`) and sealing
  an existing table are later steps. `datax bench --sealed` runs a workload
  on a sealed table. Cluster protocol version v22. See docs/sql.md and
  docs/encryption.md, "Sealed tables".
- **Per-tenant keys and crypto-shredding** (#310, step 4). `CREATE TABLE ...
  WITH (encryption_key_by = 'tenant_id')`, the column leading the primary
  key, seals each tenant's row values under a key of that tenant's own, made
  on its first write (concurrent first writes of one tenant converge on one
  key; tenants do not contend) — a million tenants are a million small rows
  in `datax_keys`, not a million tables. `SELECT
  datax_destroy_tenant_key('items', $tenant)` destroys one tenant's key and
  purges its record, and returns once no gateway uses it (within 10
  seconds); with the rows deleted and purged first, the tenant's data is
  gone from the live cluster, and a row left behind fails with `XXK01`. The
  function needs table ownership, runs outside a transaction block, is
  audited, and is idempotent (`NULL` for a tenant that never wrote). A
  tenant's next write gets a fresh key. `DROP TABLE` destroys every tenant
  key in the background; a table restore brings them back in batches. The
  gateway's key cache is bounded (10,000 keys, least recently used dropped;
  `datax start --key-cache-keys`), its hit no longer takes a lock, and a
  background refresh checks the keys in use in batches every half lease. `datax bench --sealed=tenant
  --tenants N` runs `kv` per tenant, and every bench run with
  `--server-url` reports KV batches per operation. See docs/sql.md and
  docs/encryption.md, "Per-tenant keys".
- **Sealing an existing table and online key rotation** (#310, step 5).
  `ALTER TABLE t SET (encryption_key = 'auto')` and `SET (encryption_key_by
  = 'col')` seal a table that already holds rows: new writes are sealed as
  soon as the statement returns, reads accept plain and sealed rows while a
  background job seals the existing ones, and once it is done a plain value
  is corruption again. The rows' plaintext versions stay in history until GC
  — the `NOTICE` says so, and `ALTER TABLE ... PURGE` after the job removes
  them. `ALTER TABLE t ROTATE ENCRYPTION KEY [KEEP OLD VERSIONS]` and
  `datax_rotate_tenant_key(table, tenant [, keep_old])` give a table's or a
  tenant's key a new version for new writes, and the job rewrites the rows
  under older versions and then destroys them;
  `datax_destroy_old_key_versions` destroys them early (rows and history
  still under them become unreadable). The job runs on the node leading
  range 1 in batches of 100 rows, each an ordinary transaction that also
  records its progress, so it resumes after restarts and leadership
  changes; one per table; `datax_encryption_jobs` shows it. Unsealing (`RESET
  (encryption_key)`) is refused. `datax_remove_tenant_rows(table, tenant)`
  removes a tenant's rows left behind after its key was destroyed, with
  their index entries, without the key (a table referenced by foreign keys
  is refused). Key names beginning `table/`, `tenant/` or `destroyed/` are
  reserved, and `SHOW ENCRYPTION KEYS`, `DROP OWNED`, `REASSIGN OWNED` and
  `DROP ROLE` read only the key service's keys: 4 entries instead of 10,019
  with 10,000 tenant keys. See docs/sql.md and docs/encryption.md, "Sealing
  an existing table and rotating keys".
- **The console's table page shows row-level security** (#312): whether it
  is enabled and forced, and each policy — permissive or restrictive, the
  statements and roles it applies to, its `USING` and `WITH CHECK`
  expressions — and the reconstructed DDL reproduces them. `/api/schema`
  gains `row_security`, `force_row_security` and `policies` per table.
- **`LATERAL` subqueries and table functions that read other `FROM`
  members** (#338). `FROM t, LATERAL (SELECT ...) l`, `CROSS JOIN LATERAL`,
  `JOIN LATERAL ... ON cond` and `LEFT JOIN LATERAL ... ON true` (a miss
  NULL-extends) run the subquery once per row of the members to its left,
  reading their columns: top-N per group (`FROM users u, LATERAL (SELECT *
  FROM orders o WHERE o.user_id = u.id ORDER BY created DESC LIMIT 3) o`),
  per-row aggregates, chained and nested laterals. A table function call in
  `FROM` is implicitly lateral, as in PostgreSQL: `FROM t, unnest(t.tags)
  AS tag` works (before, it was refused with `0A000`). Execution is a
  correlated nested loop memoized on the referenced values, and works with
  `WHERE`, `ORDER BY`, `LIMIT`, aggregates over the result, `EXPLAIN`,
  Describe and prepared statements. A reference to a member further right
  is `42P01`, and a `RIGHT` or `FULL JOIN LATERAL` is refused (`42P10`).
  The lateral subquery must be a plain select (not itself over a derived
  table or a set operation), and cannot read the outer row in its `ORDER
  BY`, `GROUP BY` or aggregate arguments. A view may now join a subquery
  (`CREATE VIEW v AS SELECT ... JOIN (SELECT ...) d ON ...` was refused with
  `42P01`).

### Security
- **The HTTP port no longer holds connections forever.** It bounded
  only a request's headers (`--auth-timeout`), and Go closes an idle
  keep-alive connection only at an idle or read timeout, of which it set
  neither. Anyone who could reach the port, unauthenticated, could hold
  a connection by sending one request and then nothing, or by trickling
  a body to `/api/login` (which runs before authentication), and nothing
  capped how many, so file descriptors and goroutines could be
  exhausted. Now `--auth-timeout` bounds reading the whole request,
  body included; an idle keep-alive connection is closed after 2
  minutes; headers are capped at 64 KiB; and the new `--http-max-conns`
  (default 512, negative for no cap) closes connections past the cap as
  they are accepted, counted in the new
  `datax_http_conns_refused_total`. Responses are not bounded: long
  polls and pprof profiles run as before.
- **An Admin RPC can no longer stop a node through its clock.** The
  Admin RPC ratcheted the node's clock to the caller's timestamp before
  authorizing anything, and a timestamp past `--max-offset` exited the
  process, so any client certificate (or anyone, on an insecure cluster)
  could stop a node, sealed or not, with one call. A caller without the
  node certificate no longer moves the clock at all, and a Join or Admin
  call whose timestamp is past `--max-offset` now fails with
  `FailedPrecondition` instead of exiting. Raft, Batch, Ping and
  Snapshot messages from authenticated node peers still exit on a clock
  past `--max-offset`, as documented.
- **A Bind with the wrong number of format codes no longer crashes the
  node.** A Bind whose parameter format codes were neither 0, 1 nor one
  per parameter (for example 2 codes for 3 parameters) indexed past
  the end of the list. The panic took the whole process down. Such a
  Bind, and one with a wrong count of result format codes, is now
  refused with `08P01`. A panic on a connection's goroutine now closes
  only that connection: it is logged at error level with its stack, and
  the client gets a FATAL `XX000`.
- **A binary array parameter can no longer exhaust the node's memory.**
  The element count of a binary-format array (a Bind parameter or a
  binary COPY field) sized an allocation before any element was read,
  so a 20-byte `int8[]` declaring 0xF0000000 elements had the runtime
  ask for about 480 GB and the process died. A count the value's bytes
  cannot hold is now refused with `22P03`. A binary COPY field's buffer
  likewise grows with the data that arrives instead of being sized by
  the field's declared length.
- **Client messages are capped, before authentication most of all.**
  The server allocated the whole body a message header declared before
  reading any of it, and set no cap. So a client that had sent only a
  StartupMessage could cost about 2 GiB per connection with a 5-byte
  header (`p` and a length of 0x7ffffff0). A message is now capped at
  10000 bytes until the client has authenticated, and at 256 MiB after
  (`ServerOptions.MaxMessageSize`; PostgreSQL's cap is 1 GB). A longer
  message ends the connection with a FATAL `08P01` before its body is
  allocated. COPY is unaffected: the cap is per CopyData message.
- **Binary datetime parameters no longer wrap silently.** A binary
  timestamp, date, interval or time (a Bind parameter or a binary COPY
  field) was converted with no range check. The year 2400 was stored as
  1815, timestamptz infinity as 1999-12-31, date infinity as
  5881610-07-11, and an interval of 2^62 µs as `00:00:00`. A value
  outside what the text input accepts is now refused with `22008`, as
  the text path refuses it: timestamps 1678-01-01 to 2261-12-31, dates
  0000-01-01 to 9999-12-31, times 00:00 to 24:00, and intervals whose
  microseconds fit in nanoseconds. PostgreSQL's infinity encodings are
  refused the same way, with a message naming them (PG-T-005).
- **A client can no longer call another session's per-row closure.**
  A `set_config` whose arguments read the row (pg_dump's
  `set_config(name, ...) FROM pg_settings`) runs through a closure the
  session registers in a registry every session shares, under an id
  the query names through an internal function. That function was
  callable from SQL (`SELECT "datax:row_session_func"(n, ...)`) and
  the ids were sequential, so while one session ran such a statement,
  another could run its closure: `SET` a setting in the first
  session, with that session's privileges. Any call naming a function
  whose name holds a `:` (the internal hooks all do) is now `42883`
  at parse time, and the ids are random.
- **The toolchain moves to go1.26.11.** GO-2026-6859 (an HTTP/1 server
  crash from a `MaxBytesReader` header race in `net/http`) is reachable
  through the console's HTTP server and is fixed in go1.26.11, so the
  `toolchain` line in go.mod moves there; that is the whole change. The
  advisory was published after the last green weekly scan, and the
  scan had been red on every push since. Four advisories in required
  modules stay named in the verbose log and are not reached by any
  symbol the build calls: GO-2026-5841 and GO-2026-6865 in
  `klauspost/compress`, GO-2026-6866 and GO-2026-6860 in `x/net`'s
  `html` package.
- **The console's key viewer asks for schema USAGE** (#259). SQL
  authorizes a table read as the table's own privilege AND usage of its
  schema, and `REVOKE USAGE ON SCHEMA public FROM PUBLIC` turns the
  second on; the viewer asked only the first, so a `SELECT` grantee
  without `USAGE` was refused the row by SQL and still shown the table's
  range boundaries decoded into row values, and `/api/schema` still
  listed the table. The databases whose schema the caller may not use
  are resolved once per request, the way the roles are, and every
  predicate the viewer and the listing ask ANDs them in; a read that
  fails fails closed. SQL's own catalogs — `information_schema`,
  `pg_catalog`, and now `SHOW TABLES`, which listed every table of the
  database whatever the session's grants — ask the same predicate, so
  a table the session holds no privilege on, or whose schema it may
  not use, is not listed there either, and the console and SQL answer
  the listing question the same way (#197). The database list behind
  the console's half is reused
  for five seconds, so a revoke reaches it within that rather than
  costing every document a KV round trip.
- **SCRAM verifiers are derived at 16384 iterations** (#225), four times
  the RFC floor PostgreSQL defaults to. A verifier leaves the cluster in
  every plaintext backup, where the count is what an offline grind
  costs; 16384 is as far as the raise goes without moving the HTTP
  verification budget (a derivation is about 3 ms, so a source at the
  budget's 20 a second costs 6% of a core), and the SQL door pays
  nothing — SCRAM's server side runs no derivation. Existing verifiers
  keep the count they were made with, so the raise applies as passwords
  are set or rotated; no migration, no client change. The stand-in
  verifier for a name that does not exist no longer advertises the
  constant: beside real users still at the old count that would mark
  the names that are not users as surely as a shared salt did (#137),
  so it draws its count per name from the counts the stored verifiers
  carry, in their proportions, under the same secret as the per-name
  salt — a property of the replicated catalog, the same on every node,
  refreshed off the handshake's path — so neither the old count nor the
  new says whether a name exists. While the catalog cannot be read the
  stand-in falls back to the constant, the one window that remains.
- **The switch to authenticated encryption is cross-checked against
  records a registry rollback cannot reach** (#248). The sealed key
  registry's two cutover flags cannot be cleared without the store key,
  but an older, validly sealed copy of the registry — a backup from
  before the switch — reopened the store unauthenticated, writing the
  old format until the node next observed v18 and accepting it until
  the cutover completed again. The switch is now also recorded in the
  store itself, after each registry transition, and per store in
  replicated KV once the node has made each step. The registry alone
  rolled back is refused at open, before Pebble writes a file; a whole
  store rolled back opens, is caught on the first heartbeat that reads
  the cluster's record, and the node logs the refusal, records a
  `security` event and stops serving. Both compare against what the
  engines opened with, since the node re-enables authenticated writes
  within seconds of observing v18. A rollback that was meant is
  accepted by one start with `--accept-encryption-rollback`, which
  lowers the records to what the store has; they rise again as it
  re-completes the switch.
- **Restore holds a table's data file to the table's own span** (#255,
  from InfoSec on #254; the other half of #238). Restore wrote every
  record of `table_N.dxbk` back under the key the record named, so one
  record appended to a data file — with `BACKUP.json` left byte-for-byte
  as backup wrote it, since the manifest's checksums are reported after
  a restore and never enforced before one — had restore write any key
  in the cluster as the node, or as a tombstone delete one. The
  authentication secret was the key that mattered: a planted one keys
  every console session, admin included. Every record is now held to
  `keys.TableDataSpan` of the table its file is for, tombstones
  included, in a pass before the first chunk is committed and again at
  the write, so a file rewritten between the two reads is refused too.
  With #238's manifest half this closes the restore-writes-anything
  class: the manifest's raw groups are held to their spans, the data
  files to theirs, and the file names to the ones backup derives.
- **A range boundary decodes only for a caller who may read the row**
  (#253, from InfoSec on #251). The console's key viewer asked
  `CanSeeTable` before decoding a range boundary into the row value it
  was split at, and `CanSeeTable` answers the listing question — any
  grant, including `INSERT` alone, and `write_all`. So a grantee the SQL
  authorizer refuses `SELECT` was handed one exact row value per split
  boundary on `/api/cluster`, `/api/overview`, `/status`, `/api/node`
  and the split events. Decoding now asks `catalog.CanReadTable` —
  admins, the owner, `read_all`, and a `SELECT` grant held directly or
  through a role — while naming the table still asks `CanSeeTable`, so
  an `INSERT`-only grantee gets the table's name, its boundaries at
  their prefix, and `keys_redacted` set, which is what the console needs
  to label the copy.
- **Encryption at rest is authenticated** (#220, tiers 2 and 3; cluster
  version **v18**). Until now an encrypted store was confidential against
  a disk that leaves the building and nothing more: AES-256-CTR with no
  authentication tag, so an attacker who could write to a live store's
  files could truncate a WAL, flip chosen bits in a chosen row, or
  substitute a file, and the node served the result as data — Pebble's
  block checksums are keyless and sit inside the same keystream. A new
  file format (`DXE2`) seals the content with AES-256-GCM under a
  per-file key derived from the data key and a random salt: sstables and
  the other files read at random offsets in 4 KiB chunks, the WAL and the
  MANIFEST — the logs, appended to and replayed front to back — as
  append-only frames, one per write, so no byte an earlier sync made
  durable is ever written again and a crash during an append can damage
  only the record being appended (QA's finding on the review: a chunked
  log rewrote its tail chunk over synced bytes, and a torn rewrite left
  the log refused at open with the acknowledged writes in it). Each
  chunk's or frame's nonce is its position and eight random bytes drawn
  at every write, so nothing has to survive a crash for a nonce never to
  seal two plaintexts (InfoSec's point: a counter restarts from whatever
  reached the disk). The whole header is in every tag, so no field of it
  — the salt above all — can be edited; sstables and WAL segments, which
  Pebble never renames, carry their own name in every tag too. An altered
  byte, a chunk or frame moved within or between files, an sstable
  truncated anywhere but exactly between chunks, and a renamed sstable or
  WAL are refused at read with an error rather than served; a log's torn
  final frame is the end of the log, as Pebble's own record checksum
  treats it, and a frame that fails with frames after it is refused. Not
  caught, and said so in `docs/encryption.md`: an sstable truncated
  exactly at a chunk boundary and a log cut anywhere (either looks like a
  crash; Pebble's own length checks cover sstables and the MANIFEST, a
  WAL's record checksums the WAL), a whole file rolled back or replaced
  by another of the same kind and name from a store under the same data
  key, and the key registry itself rolled back to a copy from before the
  switch — the one rollback that turns the feature off rather than one
  file's contents; a cross-check against a record of the cutover in
  replicated KV, which a single disk cannot roll back, is filed as
  #248.
  A store switches in two steps, both recorded in its sealed registry:
  from cluster version v18 it writes new files authenticated — at every
  open from then on, converting the files Pebble keeps across opens (the
  MANIFEST, OPTIONS, its markers, a WAL to replay) before Pebble starts,
  and counting old-format sstables as stale for `datax debug reencrypt`
  to rewrite — and once no old-format file remains it refuses the old
  format for good, since a reader accepting both would accept a file
  rewritten as the old one. The list that gates that one-way step treats
  a file it cannot examine as an error, not as absent; each engine's
  registry is marked under the store key that seals it (the raft engine
  keeps its own across an interrupted rotation); a conversion streams a
  file rather than reading it whole, and sweeps the temporary a crashed
  one left behind. `/api/security`, `/api/node`, the console's
  security view and the node page's encryption tile say where a store
  is, in one vocabulary (`encryptionState`): unauthenticated, switching
  with N old-format files left, or authenticated — only the last is
  green; the other two are the states the switch exists to leave, and a
  node too old to send the field says so. The operator's path after
  finalizing v18 is
  `datax debug reencrypt --wait` and a restart per node. Tier 2's header
  binding is subsumed: the salt keys the file and the name is in the
  chunks' authenticated data. Tests: round trips across chunk
  boundaries, the WAL append pattern, each tampering case from the
  issue's list (`TestAEADRefusesTampering`), the formats' coexistence
  and the strict refusal, a flipped byte in a real sstable refused by
  the engine (`TestAuthenticatedStoreRefusesAnAlteredTable`), the whole
  switch on a store (`TestAuthenticatedCutover`) and on a disk node
  across a restart, and the WAL torn at its tail recovered with every
  acknowledged write while one altered in the middle is refused. Costs:
  24 bytes per 4 KiB chunk and 28 per log append, a chunk decrypted per
  read inside it (the block cache holds the plaintext), a chunk re-sealed
  per append to an sstable's tail. The console's test suite gained
  `TestNothingFollowsAnUnconditionalReturn` (from the UI review): a
  helper spliced into `renderNode` had closed it early, leaving the
  replicas, SQL, latency, events and settings sections unwritten while
  every existing check stayed green, since the file parsed and every
  element the dead code named exists; a statement after a function's
  unconditional return now fails the suite.
- **Every reachable advisory closed, by moving three modules and the
  toolchain.** The weekly govulncheck run #143 set up for exactly this
  — advisories appear against code that has not changed — had been
  red for seven consecutive weeks, since 2026-09-21, first with two
  reachable advisories and by 2026-11-02 with seven, and nothing was
  reading it; the alarm worked and went unanswered, which is #244.
  `golang.org/x/text` v0.39.0 → v0.42.0 closes GO-2026-6629, a panic
  on crafted input in `secure/precis`, reached from SASLprep — that is,
  from the password an unauthenticated client sends to the console's
  sign-in or as HTTP Basic credentials; from `CREATE`/`ALTER USER …
  PASSWORD` over the SQL port, which is privileged and whose panic the
  statement executor recovers (a SQL login itself cannot reach it: the
  SCRAM exchange never sees the plaintext); and from the
  `--root-password` seed at startup, the operator's own string.
  SASLprep's existing fallback to the raw string on a `precis` *error*
  does not cover this: it is a panic, and on the two unauthenticated
  paths it ends one HTTP request, not the process.
  `golang.org/x/net` v0.53.0 → v0.60.0 closes four HTTP/2 server
  advisories (GO-2026-6617 an HPACK encoder race that crashes the
  server, GO-2026-6612 a double flow-control refund, GO-2026-6611 CPU
  exhaustion from repeated initial-window changes, GO-2026-6603 memory
  exhaustion through trailer headers), reached through the internode
  gRPC server. `google.golang.org/grpc` v1.82.1 → v1.83.2 closes
  GO-2026-6443, a server panic on a request with no `:authority` or
  `Host` header, and GO-2026-6348, heap exhaustion through fragmented
  DATA frames, both on the internode server (v1.83.1 closes the second
  but not the first; the 1.83 line's fix for it is v1.83.2). `x/sys`,
  `x/sync` and `x/term` move as those require, and `go.sum` carries
  OpenTelemetry 1.43.0 → 1.44.0 as gRPC's own test dependency, which
  no package in the build imports.
  All five `x/` modules at these releases require `go 1.26.0`, and that
  directive is exact where `go 1.26` was a floor: CI's Go setup
  resolved `1.26` to the newest 1.26 patch and now installs go1.26.0,
  whose standard library carries thirty-six reachable advisories (in
  `net/http`, `crypto/tls`, `crypto/x509`, `html/template`, `net/url`,
  `os` and others; four are the `x/net` HTTP/2 ones again, in the
  bundled copy, this time reached through the console's HTTP server)
  fixed across go1.26.1 through go1.26.10. So go.mod now also says
  `toolchain go1.26.10`: every `go` command on the module, locally and
  in CI, switches to that release (`GOTOOLCHAIN=auto`, the default),
  and a standard-library advisory from now on is closed by moving that
  line, which the weekly run will ask for by going red. The run (and
  `make vulncheck`) now passes `-show verbose`, so the advisories in
  required modules whose symbols the code does not reach are named in
  the log instead of counted; until now nobody could say which they
  were. Nothing in the code changes.
- **Restore writes only the keys a backup collects** (#238, found in
  #237's review). A backup carries five groups of raw system records —
  users, admins, roles, databases, sequences — each collected from a
  named key span, and restore wrote every key back verbatim, with no
  check that it lay in the span it was collected from. So a crafted
  manifest could have restore write any key in the cluster, as the node,
  inside its own transaction: the authentication secret, which lies
  outside every span a backup collects and so appears in no legitimate
  backup, would have handed whoever wrote the manifest the console's
  session signing key — a forged `root` session, with no password and
  no certificate — on a cluster whose operator had chosen to trust the
  backup's *data*. Each group is now held to the spans backup reads it
  from, when the manifest is read, so both readers refuse it before
  anything is applied; databases and sequences legitimately mix spans
  (descriptors and names, and counters) and are held to their sets.
  Backups written by any release restore unchanged: nothing a backup
  writes lies outside the spans it was read from.
- **The encryption-at-rest threat model is stated** (#220, part). File
  content is AES-256-CTR with no authentication tag and the file header
  is not authenticated; only the key registry and the metadata backup
  are GCM-sealed. So `--enc-key` protects a disk that leaves the
  building and not a store an attacker can write to: with write access
  to a live store's files, a WAL can be truncated (committed writes
  vanish as if after a crash), chosen bits flipped in a chosen row with
  the block checksum repaired in the same pass, a header's key id or IV
  swapped, or a whole file substituted or rolled back — none of it
  detected, since Pebble's checksums are keyless and CTR is
  bit-malleable. The cross-replica consistency sweep is the one thing
  that would notice, after the fact, and it is off by default.
  `docs/encryption.md` now says all of this up front and in its
  limitations, and the package comment says it too, so the feature reads
  as protecting exactly what it protects. Authenticating the header and
  the content — the issue's second and third tiers — is a design
  decision recorded on the issue, not made here.
- **A query cancel is authorized on the node that acts on it** (#211).
  `CancelLocal` read a zero secret as "the caller is trusted, skip the
  check" — an in-band sentinel on a value that arrives from the network,
  standing in for an authorization performed on a different node from
  the one that acted. Two ways in.

  An unauthenticated 16-byte `CancelRequest` packet, which the SQL port
  served in cleartext even in secure mode, cancelled any statement
  anywhere in the cluster. Process IDs are `nodeID<<20|seq` with the
  sequence starting at 1, so the search space per node is a few thousand
  small integers; the packet was forwarded to the owning node under the
  forwarding node's own certificate, and the check was skipped again on
  arrival. And `cancel-query` was classified as an op any authenticated
  principal may run, so the holder of any CA-signed client certificate —
  one issued to an ordinary SQL user with no roles and no grants — could
  dial the internode port and terminate arbitrary sessions, root's
  included, which is authority the SQL layer reserves to the admin role.

  There are now exactly two authorities and neither is inferred from the
  other's absence. A wire client holds the secret: it must match
  exactly, it travels with the request and is checked again where the
  connection lives, and it authorizes a cancel of that one connection
  and nothing else. An administrator holds the admin role: it does not
  travel at all — the receiving node re-derives it from the forwarding
  node's certificate — and it is what `pg_cancel_backend` and
  `pg_terminate_backend` spend. `register` no longer issues a zero
  secret, which one connection in 2³² used to draw.

  **A `CancelRequest` sent in cleartext to a TLS listener is now
  refused.** The secret is the whole authorization, and in the clear it
  is there to be read and replayed by anyone on the path. pgx v5.10
  (the version datax pins) and libpq from PostgreSQL 17 encrypt the
  cancel connection whenever the connection it cancels is encrypted, so
  `datax sql` is unaffected; **libpq before PostgreSQL 17 does not, so
  Ctrl-C in psql 16 or earlier against a secure cluster now does
  nothing.** Upgrading psql restores it.

  No cluster protocol version: nothing new is sent, and a node running
  an older binary keeps forwarding exactly the requests it always did.
  **The fix is complete once every node is upgraded, and the roll itself
  is a window.** An un-upgraded node has neither the cleartext refusal
  nor the zero-secret door guard, so both vectors stay reachable through
  it: directly, at its own unchecked `cancel-query`; and onward, because
  when it forwards an attacker's zero-secret cancel it does so under the
  cluster's own certificate, and the bytes are identical to a legitimate
  forwarded `pg_cancel_backend`. The receiving node cannot tell them
  apart. Nothing can fix a binary that is already running, so the
  practical consequence is operational: roll promptly, and do not leave
  a half-upgraded cluster sitting.
- **The SQL port has pre-authentication bounds** (#212). #195 bounded
  password guessing at `/api/login` and HTTP Basic and left the SQL
  port unmetered — the door an attacker would choose, against the same
  verifiers. And the handshake itself was unbounded: a connection that
  sent nothing, or stalled between the server's SCRAM challenge and the
  client's final, was held indefinitely with its goroutines, session
  and cancel-registry entry, and nothing capped how many of those one
  peer could open.

  Three bounds now. **The guessing budget is one budget for every
  door**: the SQL listener consults the same per-source and
  per-(source, account) buckets `/api/login` spends, before the verifier
  lookup, and refuses with a FATAL 28000 that names no user — the
  budget is a function of what the source has already done, never of
  whether the name exists. The SQL door asks before the exchange and
  charges only a failure, so a pool opening more connections than the
  burst at once, every one with the right password, is never refused.
  What that order gives up is the bound on how many guesses can be in
  flight together — every one that asked before the first failure
  landed runs, up to the connection cap below — and not what they
  cost: each is charged when it lands, into debt, so a wave of forty
  puts its source forty seconds from its next attempt and buys nothing
  over the same guesses one at a time. The debt is floored at ten
  minutes or the pre-authentication cap below, whichever is deeper —
  derived from the cap in force, so raising the cap never lets a wave
  be wider than the floor — and an address is never held for longer
  than that after the guessing stops (a node with the cap removed has
  no floor, and the flag says so).
  **An authentication deadline** (`--auth-timeout`, default 60 s,
  PostgreSQL's `authentication_timeout`) closes a connection that has
  not reached ReadyForQuery, wherever in the handshake it stalled; on
  the HTTP port the same setting bounds reading a request, its headers
  and its body (see "The HTTP port no longer holds connections
  forever" above). **A cap on connections in the pre-authentication state**
  (`--sql-max-pending-auth`, default 512) refuses at accept, with a
  FATAL 53300 and before anything is allocated for the connection,
  rather than accepting and holding; a client refused mid-SSLRequest
  sees the refusal as a failed TLS negotiation, which is the price of
  refusing before a TLS handshake this node would otherwise pay for.

  New: `datax_sql_preauth_closed_total{cause="handshake-timeout"|
  "pending-full"}` and the `sql-preauth-closed` audit record; SQL
  rate-limit refusals count under the existing
  `datax_auth_throttled_total{cause="rate-limit"}` and `auth-throttled`
  (path `sql`), and reach `/api/security` and the health check with
  the HTTP ones.

  Two limits to know. **The bound is per node**: the limiter is
  in-memory on each node, so the per-account figure is 5 + 1/s *per
  node* — a nine-node cluster gives a guesser who spreads across it
  nine times that. #195 left cluster-wide backoff out deliberately
  and it stays out; the figure is stated so it is not discovered. And
  as with #211, **no cluster version, and the roll is a window**: an
  un-upgraded node has no bound at all on its SQL port, so for the
  duration of the roll an attacker simply picks one. Roll promptly.
- **A caller holding a valid credential is no longer exempt from the
  authentication bound** (#221). The attempt budget #195 added is
  refunded by a success — it has to be, or a legitimate user sharing an
  address with a guessing loop is locked out — and that made success a
  reset button: a caller re-sending a valid password on every request,
  which is what HTTP Basic does, ran a PBKDF2 derivation per request
  with nothing but the four-slot in-flight cap between it and the
  node's cores (measured at 470 a second). The cost of a verification
  is the same whether or not the password was right, so there is now a
  **verification budget per source** — a hundred in a burst, twenty a
  second sustained — spent by every verification the HTTP doors run
  and refunded by nothing. It is asked after the attempt budget, so a
  guesser is refused by the latter as before and never reaches it, and
  it is sized for how the doors are used (a scraper, a script, a
  support bundle) rather than for a person. Refusals count under a new
  `datax_auth_throttled_total{cause="verify-rate"}`, reach
  `/api/security` as `auth_throttled_verify_rate`, the health check,
  and the console's *refused before verification* figure. Nothing on
  the SQL port spends it: SCRAM's server side runs no derivation, so a
  pool reconnecting as often as it likes is not refused. The console's
  glossary entry for that figure, which said the SQL port had no
  limiter of its own, is corrected for #212. Unlike `--auth-timeout`
  and `--sql-max-pending-auth`, the two figures are constants, by
  decision rather than omission: the deployment that could meet them
  is a fleet of HTTP Basic automation behind one address (a NAT, a
  proxy, an egress gateway), whose symptom would be 429s on requests
  that worked yesterday, and the `verify-rate` cause names it. A knob
  is a small change if anyone meets it.

- **Restore reads only the files a backup writes** (#214). Backup names
  each table's data file from its id (`table_<id>.dxbk`); restore
  joined whatever name the manifest carried onto the backup directory,
  and `filepath.Join` resolves `..`, so a manifest saying
  `"file": "../../../etc/shadow"` had the node open that file with its
  own uid, parse it as backup records and quote bytes of it in the
  failure. Admin-only, and a read rather than a write, but a restore
  from a directory someone else prepared — a shared artifact store, a
  support bundle — is an ordinary workflow. Restore now derives the
  name from the table id again, exactly as backup wrote it, and a
  manifest naming anything else is refused when it is read, before any
  file is opened — at both doors that read one, `datax restore` and
  `--base` of an incremental. The filesystem can name a path too, as the
  review found: a symlink at the derived name reaches wherever it
  points, and restore would have applied what it found there. A backup
  writes plain files and nothing else, so only a regular file is read at
  one of its names — the manifest's or a data file's — and a symlink or
  a directory there is refused without being followed. Backups written
  by any earlier release restore unchanged: the name has been the same
  since backup existed.

- **Range boundary keys are shown to a caller only for the tables it
  may read** (#213). A range's boundary is a real row key, and the
  console rendered it back into its values — `/table/users/primary/
  "alice@example.com"`, one exact value from a row and the table it
  came from — on `/status`, `/api/cluster`, `/api/overview`,
  `/api/node` and in the split and merge events on `/api/events`, for
  every range in the cluster, to any authenticated user with no grant
  on anything. The rule the rest of the console follows (`/api/schema`
  since #197, `/api/security`) is to keep the document and filter the
  data-bearing fields to what the caller may read, and it now applies
  to keys: a table the caller may read renders as before; one it may
  not renders as its table-and-index prefix by id (`/table/7/1`) with
  the table's name left out, so a caller's `/api/cluster` table list
  agrees with the same caller's `/api/schema`. The range id, replicas,
  size and QPS are untouched. Split and merge events are recorded in
  both forms and the event feed serves the one the caller may see; the
  exact key stays on the admin-gated `/api/range` and in the node's
  log. System and meta keys carry no row and are unchanged. Admins and
  insecure mode see everything, as before. The document says when its
  keys are the shown form (`keys_redacted`), and the copy control and
  the CSV export label a key accordingly rather than hand a reader the
  shown form under a key's name (found in the console review of the
  combined PR, where #213 met #226's copy controls).
- **Every role reads the catalogs** (#312). A role that was not an admin
  was refused (`42501`) every `pg_catalog` and `information_schema` table,
  so `psql`'s `\d`, ORM introspection and `pg_policies` worked for admins
  only. They now follow PostgreSQL: any role reads them; `pg_catalog`
  lists every table of the current database, `information_schema` the
  tables the reader holds some privilege on, and `role_table_grants` the
  grants to or by the reader's roles. Statistics (`reltuples`,
  `n_live_tup`) are shown only for tables the reader may `SELECT` from and
  row security does not filter, `pg_sequences.last_value` only for a
  sequence it may read; no catalog carries a password, and another role's
  statement text stays hidden. `SELECT ... FROM` a sequence checks the
  sequence's `SELECT` privilege instead of requiring an admin. See
  docs/user/security.md, "Roles and privileges".
- **`EXPLAIN` takes `SELECT`** (#312). Plain `EXPLAIN` checked no
  privilege, so a role with no grant on a table could see its plan, its
  indexes and a row estimate drawn from its statistics. It now takes
  `SELECT` on every table the statement reads — joined tables, derived
  tables, subqueries and `WITH` members included — as running the
  statement would; a view's query is checked as the view's owner.
- **`SHOW STATS FOR t` takes `SELECT` on `t`** (#312). It checked no
  privilege, so any role could read a table's row count and its columns'
  distinct and NULL counts.
- **A `TRUNCATE` refusal names a referencing table only to a role that may
  know of it** (#312). A role holding `TRUNCATE` on a table and nothing on
  a table whose foreign key references it was told the referencing
  table's and constraint's names (`0A000`), or, under `CASCADE`, refused
  by name. It is now told that such a reference exists, without naming
  it (`0A000`; `42501` under `CASCADE`). A role with some privilege on the
  referencing table gets the message as before. The privilege checks on
  the tables the statement names already came first; `pg_constraint`
  lists foreign keys to every role, as in PostgreSQL.
- **`AS OF SYSTEM TIME` answers to today's privileges** (#312). A
  historical read checked a table's privileges, and the reader's role
  memberships, as of its timestamp, so a `REVOKE` did not stop a role
  from reading the table as it stood before. Privileges are now decided
  as row-security policies already were: by the table's grants and the
  role's memberships now, the rows being the timestamp's. A table renamed
  since answers to its grants under the new name; one dropped since is
  readable historically by admins only. Catalogs read `AS OF SYSTEM TIME`
  use today's roles and show statistics and sequence counters to admins
  only. "Now" is as fresh as the data the node can serve: an exact
  timestamp checks the current grants and policies; a bounded-staleness
  read checks the newest state the node's own replicas hold (the leased
  descriptor, or the catalog at its closed timestamp), so a `REVOKE` the
  node has replicated stops it, one not yet replicated is invisible to it
  as unreplicated rows are, and under a partition it is still served
  locally for every role. Row-security policies under a bounded-staleness
  read are read the same way; before, their check needed the
  leaseholders and could not finish on a partitioned node. (A statement
  prepared through the extended protocol still
  resolves its tables by today's names when it is described, so a table
  renamed or dropped since is reachable only through the simple
  protocol, as before.)

### Changed
- **Sequences, types and relation names** (with PG-D-035 above):
  - A `DEFAULT nextval('x')` written before `x` exists is `42P01`, as in
    PostgreSQL; it used to be accepted and resolved when used.
  - `regclass` output names sequences (it printed the number) and
    quotes identifiers that need it (`"Big"."Q"`), for tables too.
    An enum's `::regtype` is its OID, displayed by name.
  - A table, view or rename can no longer take a sequence's name in the
    same schema (`42P07`); a duplicate sequence is `42P07` and a
    duplicate type `42710` (both were `XX000`); creating in a schema
    that does not exist is `3F000`.
  - `'a'::"public"."Role"` and `'a'::s.mood` keep their schema
    qualifier instead of resolving the bare name through the path.
- **Two-part names with schemas.** `x.t` now means table `t` in schema
  `x` of the current database when that schema exists. Only when it does
  not, and a database `x` exists, does it mean `x.public.t`, as before.
  This fallback is deprecated: write `x.public.t`.
  - A name matching neither is now `42P01` (`3F000` for `CREATE`)
    instead of `3D000`.
  - `db.s.t` with another database and a schema other than `public` is
    `0A000`, as in PostgreSQL.
  - `public.t` names the permanent table even when a temporary `t`
    shadows it.
- **The default `search_path` is `"$user", public`**; `SHOW search_path`
  used to print `public`. Names resolve as before unless a schema named
  after the role exists.
- **Behaviour now matching PostgreSQL** (the compatibility batch
  above). Clients that depended on the old behaviour see these changes:
  - **`information_schema.columns.data_type`** no longer includes a
    length or precision (`character varying`, not
    `character varying(10)`). NOT NULL columns appear in
    `table_constraints` as CHECK rows, `check_clause` is
    parenthesized, and `views.view_definition` ends with `;`.
  - **Error codes:**
    - Regex back references and lookaround are refused with `0A000`
      (was `2201B`). `~` / `~*` with a bad pattern give `2201B`
      (was `22023`).
    - `ln` / `log` of zero or a negative number give `2201E`.
      `sqrt`, `power` and `factorial` domain errors give `2201F`.
    - Text-only functions applied to `bytea` give `42883`.
  - **Timestamps:**
    - `now()` and `clock_timestamp()` are truncated to microseconds,
      and timestamp input is rounded to microseconds.
    - Text output shows at most 6 fractional digits, so values stored
      earlier with nanoseconds render rounded.
  - **`pg_authid`** exists and is admin-only (`42501`), where it was
    missing (`42P01`).
  - **`ALTER ROLE me PASSWORD ...`** may change only the password: a
    statement that also sets an attribute needs an admin.
  - **Each new connection does one more catalog read**, for connection
    limits, database defaults and `is_superuser`. The read is best
    effort: if it fails, the session still opens.
- **The hosted CI, vulnerability-check and bench workflows are paused**:
  their push, pull-request and schedule triggers are commented out while
  the account has no GitHub Actions minutes. `scripts/ci-local.sh` runs
  the same gate locally; `docs/testing.md` describes it.
- **Running a request as a stopper task costs less** (#331). Every gRPC
  call, raft message, HTTP request and SQL connection has been a stopper
  task since #324, and each took the stopper's one mutex twice, updated
  a shared map of names and registered with the stopper's context. Tasks
  are now tracked in 32 shards, each with its own lock, and Stop cancels
  them itself. A task costs 175-250 ns and 2 allocations where it cost
  450-500 ns and 4 serially, and about 180 ns where it cost 0.8-1 µs
  with four cores starting tasks at once; its share of a node's CPU
  under `kv-50-50` on three nodes fell from 2.2 % to 1.3 %. Throughput
  and p99 of that workload did not move beyond run-to-run noise either
  way. The guarantees are those of #324: no task is admitted once a
  stop begins, none is running when the closers run, the wait is
  bounded, and a task still running at the bound is logged by name.
- **A unique index takes NULLs, as in PostgreSQL** (#322). A row with a NULL
  in any column of a unique index has no entry in it and conflicts with no
  other row (NULLS DISTINCT, PostgreSQL's default), on every path: `INSERT`,
  `COPY`, `UPDATE` to and from NULL, `ON CONFLICT (col)` (a NULL inserts),
  the backfill of `CREATE UNIQUE INDEX` and `ADD CONSTRAINT ... UNIQUE` over
  existing NULLs, and foreign keys referencing a unique column. Before, such
  a row was refused with `23502`. The planner no longer takes a unique index
  for a complete row source: like a non-unique one it scans it only where a
  NULL cannot match (`WHERE col = 'x'` is still a point lookup; `WHERE col
  IS NULL` reads the table). `UNIQUE NULLS NOT DISTINCT` (PostgreSQL 15) is
  supported on a column, as a table constraint and on `CREATE UNIQUE INDEX
  ... NULLS NOT DISTINCT`: its index keys each column behind a tag byte so a
  NULL has a key of its own (`pg_index.indnullsnotdistinct`,
  `pg_get_indexdef` and `pg_get_constraintdef` show it). Both need cluster
  version v22: before it is finalized a unique index refuses NULLs as
  before and `NULLS NOT DISTINCT` is refused, since a v21 planner reads a
  unique index as holding every row and a v21 node does not know the tag.
  Existing unique indexes hold no NULLs and need no change.
- `ORDER BY ... LIMIT n` over one table's primary-index scan keeps only the
  first `n` (plus `OFFSET`) rows in a heap instead of sorting every row, so it
  runs in bounded memory; `EXPLAIN` shows `top-k heap` (#304).
- Floating-point comparison is a total order, as in PostgreSQL: `NaN` equals
  `NaN` and sorts after every number. Before, a sort containing `NaN` had no
  consistent order (#304).
- A statement that writes more than 60 MiB to one range in a single piece (a
  huge row) is refused with `54000`. Before, a write of 64 MiB failed with
  `XX000` and left the range's leadership churning around an entry no follower
  could receive (#303).
- **The catalog's costs follow what is used, not every table in the
  cluster** (#311). Listing one database's tables (`SHOW TABLES`, `DROP
  DATABASE`, `GRANT ... ON ALL TABLES`, the `pg_catalog` views) reads that
  database's names and descriptors instead of scanning every descriptor:
  102 rows at 1,000 tables and at 5,000, where it was 1,005 and 5,005.
  The console's schema document is built when requested, cached for five
  seconds and shared by concurrent requests, rather than rebuilt on a
  timer on every node; a build is seven KV requests whatever the table
  count. Range labels, placement, index-usage names and the table gauges
  no longer depend on it. A gateway's descriptor cache holds at most
  1,024 descriptors (`DescCacheSize`) and gives up the lease of a table
  unused for three lease periods, so a schema change on an idle table
  waits for nothing; a table resolved while the cache is full is read
  from the catalog by each statement. `datax_table_rows`,
  `datax_table_ranges` and `datax_table_stats_age_seconds` are now read
  when `/metrics` is scraped. `information_schema.tables`, `.columns`
  and `.views`, role DDL and the per-table gauges still read every
  descriptor. Measured at 1,000 and 5,000 tables, not beyond. No cluster
  version change.
- A restarted node serves no follower reads from a range until that range's
  closed timestamp has passed the node's start time (a few seconds on a
  healthy cluster; not at all while the range has no leader). Before, it
  served from the closed timestamp it had persisted, which could predate a
  purge it had missed (#295).
- **The console, tidied** (#278, #279, #280, #281). A pass over what a
  person reads and reaches for, none of it new capability.
  - *The event timeline.* A split or merge names its keys the way the
    range tables do, `/table/orders/primary/42` where it said
    `/table/7/1/42`: the storage layer that records the event has no
    catalog, so the event now carries its keys and the server prints
    them, for a reader who may read every table they belong to. Who
    that is has not changed — the same check serves everyone else the
    keys at their prefix, by id, as before. A range or node named in an
    event is a link. A burst of four or more events of one kind within
    a minute of each other folds into one row that opens in place;
    thirteen lease moves after an ingest used to push the split that
    explained them off the screen. Health, consistency-failure,
    security and auth-failure events are never folded. A link to a
    range now carries it as `r=`: `range=` is the time window, and
    while the two shared a name a range's link lost its range under any
    window but the default (`#/data?range=<id>` still opens the range).
    A reserved table that has no rows yet — the console's preferences
    table before anyone saves one — is named in key spans like the
    others, where it was `/table/1099511627777`.
  - *On a phone.* The banner, title, navigation, controls and serving
    node took 300 of 844 pixels before any content; they take about
    200. The navigation sits beside the title, the pickers keep their
    labels for a screen reader only, and the insecure-mode banner
    leaves its remedy to the security view while still saying, on
    every view, that nobody is authenticated and who has full access.
  - *Rows that open.* The schema list and the ranges table opened a
    detail on click with nothing to say so but a note under the table.
    The cell that names the row reads as a link, and the row answers
    the pointer.
  - *Smaller things.* A length of time — how long a session has been
    idle, how long a transaction has been open — no longer ends in
    "ago". A table's statistics say when they were collected once, the
    viewer's way, where they gave the browser's locale date and then
    the age. The overview's strip is headed "Recent operations", which
    is what it lists, and says so when one is still in flight. Uptime
    appears once on a node's page; the version tile breaks at a space
    instead of inside `cluster` or `v19`; the admin badge beside a
    role's name is a badge, where the rows read "admin admin"; the
    jump button's hint is `Ctrl K` off a Mac; a health problem's age
    and links wrap together; and the header keeps the serving node's
    address for screens 1600px and wider, where it no longer pushes
    the sign-in onto a second line. Nor does a locality: one too
    long for the room left ends in an ellipsis, with the whole label
    in its title.
- **The console is tested in a browser** (#276). Its tests read the
  JavaScript as text, which is how two of the faults above shipped: a
  test checked that a moment was rendered by the right function and
  never what the function was handed. `TestConsoleRendersEveryRouteOpenedCold`
  starts a cluster and opens every view in headless Chrome, a new tab
  per view, at a desktop and a phone width, then reads the page: the
  header, the banner, the scope picker, every "when", script errors,
  and the widest thing on the page. It also renders health problems
  into the overview on purpose, so the panel is measured on every run
  and not only when the cluster is unwell. It drives Chrome over the DevTools
  pipe with the standard library, so it adds no dependency. It skips
  where there is no Chrome (`DATAX_CHROME` names one), and CI sets
  `DATAX_REQUIRE_BROWSER` so that a runner without one fails instead.
- **The ranges table shows two hundred rows at a time** (#227, raised
  on #226). Every range row carries a copy control, which is a tab stop
  (#205, #149), and the table rendered every range the filter left: on
  a real cluster, hundreds of stops to get past, falling hardest on a
  non-admin whose rows are not focusable. The table now renders the
  first 200 rows the filter left and says so beneath — "showing the
  first 200 of 1,340 ranges · show all 1,340" — with one control for the
  rest and one to go back; the filter narrows the set before the cap,
  and the CSV carries every row the filter left regardless of it. The
  copy controls stay in the tab order, which is what put them there.
  A change to the tables' keyboard model was proposed and not taken.
- **Every pair of chart series colours is at least 15 OKLab units apart,
  in both themes** (#240, from QA on #239). Six pairs sat below the
  floor two lines with no other channel need — light `n2`/`n4` 13.7 and
  `n2`/`n5` 12.9; dark `n3`/`n6` 8.8, `n1`/`n7` 9.8, `n2`/`n4` 10.6 and
  `n2`/`n5` 11.6 — and were carried as named exceptions. The slots
  involved are re-stepped by the smallest moves that clear every pair
  with margin (floor 15.6 in the search, so the test's 15 holds against
  rounding), keeping each slot's hue family and the palette's own chroma
  and panel-contrast floors, with `n1` (the console's accent) fixed:
  light `n2` `#eb6834` → `#db5e12` (a deeper
  orange, 4.2 units) and `n6` `#008300` → `#077c06` (2.2); dark `n2`
  `#d95926` → `#dc560d` (1.2), `n3` `#199e70` → `#0c9e73` (0.5), `n4`
  `#c98500` → `#d59d21` (6.4), `n5` `#d55181` → `#d34e94` (3.2), `n6`
  `#43b043` → `#5fc425` (7.0), `n7` `#9085e9` → `#b685f5` (6.1). The
  smallest pair is now 15.6 (light) and 15.7 (dark). Dark `n8` `#3ce6ff`
  → `#fe93a3`: it had been a cyan where light `n8` is a dark red (since
  the metrics view's first palette), the one slot whose hue family
  changed with the theme — and a theme is a stored preference that
  follows an operator to any browser (#204), so the same node could be
  "the red line" on one screen and cyan on the next. A red on a dark
  panel can only be a light one (the contrast floor forbids a deep red),
  so dark `n8` is a coral, 16.5 units from its nearest neighbour; no
  light-theme cyan clears the floors, so the pair joined on the red side
  (the UI review on #249). The search also held the first four slots at
  least 9 units apart under simulated protanopia and deuteranopia
  (Machado 2009, full severity): 9.1 light, 10.5 dark, where dark
  `n2`/`n4` had been 4.9 under deuteranopia. Five pairs remain under 9
  in one theme or the other: light `n2`/`n6` (1.2) and `n3`/`n5` (6.1);
  dark `n1`/`n7` (4.2), `n3`/`n5` (3.8), `n4`/`n6` (1.2), and `n3`/`n8`
  (7.9), which the coral created — joining the red family cost that one
  collision, and the facet gate below absorbs it — while orange against
  green, teal against pink and yellow against yellow-green collapse for
  those viewers whatever their hue, and no light green or further dark
  re-step clears them without colliding elsewhere. The next pair above
  the line is light `n3`/`n4` at 9.1. They are no longer left to the count-based facet rule, which
  never fired for a two-node cluster numbered 2 and 6: the metrics view
  facets whenever two nodes of such a pair would share a frame
  (`CVD_CLOSE_PAIRS` in `mustFacet`), its note says which pair, and the
  console test recomputes the pair set from the palette so the list and
  the colours cannot drift apart in either direction. The all-pairs
  test's exception list is now empty, so a pair that drops below the
  floor again fails the test rather than joining a list.
- **Cluster protocol version v17.** The console's preferences live in a
  new system table, `datax_ui_prefs`, at a reserved descriptor ID beside
  `datax_metrics`. A v16 node knows nothing of the reservation and would
  treat the table as an ordinary user table — droppable, and writable by
  its owner — so nothing creates it and `/api/prefs` stores nothing until
  the cluster has finalized v17. Until then the console still honours a
  choice for the tab and says plainly that it will not be kept. The table
  is included in backups: it is small, and unlike the metrics it cannot
  be regenerated from anything.
- **`TRUNCATE` takes the `TRUNCATE` privilege; `DELETE` no longer covers
  it** (#312). A `DELETE` grant let its holder `TRUNCATE` the table, a
  fallback kept from before cluster version v11, when `DELETE` was what
  `TRUNCATE` took — on every table, and for grants made long after v11.
  Now the fallback holds only while a cluster is below v11, and
  finalizing v11 grants `TRUNCATE` beside every `DELETE` grant in the
  same transaction. **A cluster already at v11 or later is not
  migrated**: a role that truncates a table on the strength of a `DELETE`
  grant is refused (`42501`) and needs `GRANT TRUNCATE ON t TO role`
  (`SHOW GRANTS` lists who holds `DELETE` without it). `ALL` and the
  `write_all` role include `TRUNCATE` as before.
- **`DROP TABLE` gives the table's space back** (#313). A dropped table's rows
  used to stay on disk for good. They now stay for the table's history window
  — its `gc_ttl`, a time-series table's retention, or the node's GC TTL (25h
  by default) — so `AS OF SYSTEM TIME` below the drop keeps working, and then
  a job on the node leading range 1 deletes them, purges the span and lets
  its ranges merge away. The job is resumable: the drop leaves a record in
  the system span, and a pass cut short by a restart is taken up again. A
  short `gc_ttl` set before the drop makes the reclaim correspondingly
  prompt. Tables dropped before this release are not reclaimed. The
  operations view shows each reclaim as a `drop-reclaim`.
- A restore of everything into a cluster that had dropped tables could mix
  their leftover rows into a restored table carrying the same id (a fresh
  cluster hands out the same first ids as any other); the restore now clears
  such an id's rows first (#313).
- The merge queue no longer merges two ranges whose GC thresholds are further
  apart than the lower side's GC window. A purged range's threshold is the
  purge's time, and a merged range adopts the higher threshold, so a purged
  range merged into its neighbour — the system range included — made the
  neighbour refuse `AS OF SYSTEM TIME` reads and incremental backups below
  the purge (#313).
- `datax backup --backup-key` asks the node for the admin op `backup-sealed`,
  and `datax restore --backup-key` (without `--table`, `--database` or
  `--as-of`) for `restore-sealed`, instead of `backup` and `restore` with
  extra fields (#313). A node from before sealed backups — one not yet
  upgraded in a rolling upgrade — ignored the key under the old names and
  wrote the backup in plaintext; it refuses the new ones. Nodes still accept
  `backup` and `restore` with keys from older commands. A command from this
  release pointed at a node from before it fails with an unknown op: run it
  against an upgraded node.
- A read below a range's GC threshold now reaches the client with a SQLSTATE
  that says what to do, instead of `XX000` (#313). A transaction the cluster
  timestamped — one that ran longer than a table's `gc_ttl`, or began before
  a `PURGE` of what it reads — is restarted like any other conflict: inside
  the cluster's own transactions automatically, and over SQL as `40001`,
  which succeeds when retried. `AS OF SYSTEM TIME` below the kept history
  fails with `72000` (PostgreSQL's `snapshot_too_old`): retrying the same
  timestamp cannot succeed. The KV error carries a typed detail
  (`BelowGCThreshold`); one from a node that predates it is recognized by
  its message.
- **Fewer reads of every table descriptor** (#313, following #311). `DROP`,
  `ALTER ... RENAME` and `DROP COLUMN` find the views reading a relation from
  records kept under it rather than by reading every descriptor: at 5,000
  tables a `DROP TABLE` read 5,004 descriptor rows and reads 2. The records
  are written with each view; they are trusted from cluster protocol version
  v22, once the node leading range 1 has recorded every existing view, and
  until then the old check runs. The retention provider behind GC and the
  re-shard janitor read every descriptor on their first pass and then only
  the descriptors changed since (an incremental export of the descriptor
  span): an idle pass at 5,000 tables read 5,000 rows and reads none. New
  measurement harness `TestCatalogScanScale` (`DATAX_CATALOG_SCALE`). Cluster
  protocol version v22.
- **Tables dropped before dropped-table records are reclaimed** (#316). A table
  dropped by a binary from before #313 left its rows under its id with
  nothing naming them. Once the cluster has finalized v22, the node leading
  range 1 sweeps once for ids that hold rows and have no descriptor, no
  dropped-table record and no table restore in flight — never a reserved id,
  never one issued after the sweep began — and records each as dropped when
  it first sees it; the reclaim job clears it once the GC TTL has passed from
  then. The sweep is resumable and records nothing twice. Cluster protocol
  version v22.
- **The statistics sampler reads only what changed** (#316). Its tick read every
  descriptor and every statistics record to pick the stalest table; it now
  reads both once and then only what was written since its last tick, as of
  one timestamp: at 5,000 tables a tick read 5,000 rows and reads 2 while it
  is collecting (the record it wrote last tick and the descriptor it
  collects), none with nothing to collect.
- **The metrics recorder writes the table and index series only when they
  change** (#316). Every tick wrote a `table.ranges` (and `table.rows`) row per
  table and an `index.scans` and `index.rows` row per index the node had
  read; from cluster protocol version v22 those are written when the value
  changes, once every 60 ticks regardless, and once with a NULL value when the
  series ends, beside a `recorder.tick` row per tick. `/api/metrics` and the
  dashboard fill in the unwritten ticks, so an idle table charts exactly as
  before. At 5,000 tables a tick wrote about 13,200 rows and writes about 370
  idle (1,000 tables: 3,042 → 94). New harness `TestMetricsRecorderScale`
  (`DATAX_CATALOG_SCALE`). A plain SQL query of `datax_metrics` sees the
  sparse rows; docs/user/operations.md says how to read them.
- **The catalog-following jobs resume after a restart** (#316). The retention
  provider behind GC, the re-shard janitor and the statistics sampler read
  every descriptor (the sampler also every statistics record) on their first
  pass after a node start, and the janitor and the sampler again on a node
  that took over range 1's leadership. Each now persists its state with the
  timestamp it is as of, in one value — the retention provider on the node's
  own engine, the janitor and the sampler under `/system/catalog-resume/` so
  that a new leader resumes too — and its first pass reads what changed since:
  at 5,000 tables 5,002, 5,002 and 5,007 rows become 3, 2 and 7. A record
  that is missing, of another format version, or older than the GC threshold
  leaves a full read, as before. The sampler's record, about 45 KB at 5,000
  tables, is written hourly; the others, a few hundred bytes, every minute
  and every ten minutes. Each job still checks its state against a full read
  about once a day (jittered); a difference is logged as a warning, counted in
  the new `datax_catalog_follower_divergence_total{job}`, and replaced by the
  full read's.

### Fixed
- **Status recovery no longer commits a transaction's earlier writes
  without its final ones.** A pipelined transaction that wrote a key and
  then rewrote it in its final batch, sent in parallel with the STAGING
  commit record, could be recovered as committed with the earlier value
  if the final batch never landed (the coordinator crashed, or the batch
  failed and the cleanup abort failed too): the probe counted any intent
  of the transaction as the staged write. It now counts only an intent
  at the staged record's epoch and at its sequence or above, which a
  push of the record reports; earlier-epoch intents left by a restart no
  longer fool it either. **Cluster version v25:** once finalized, the
  `RecoverTxn` that ends recovery also names the staged record it
  checked, and a record that re-staged in between is left for a fresh
  recovery. Before the finalize recovery finalizes as before; with a
  leaseholder on the old binary the probe matches on the transaction ID
  as before.
- **Health sees replicas that a live node cannot serve.** A raft-engine
  write error (ENOSPC, EIO) or a failed apply stops a replica's group
  until the node restarts, but the replica stayed in the store map with
  only a log line, and a replica lost before it reached disk is never
  loaded. Either way the node kept heartbeating, so `quorum-lost` and
  `under-replicated`, computed from liveness alone, said nothing while a
  range ran a replica short or had no quorum. Each node now reports such
  replicas in its heartbeat (failed ones at once, ranges whose
  descriptor has named it for over a minute with no replica loaded as
  missing), exports them as the new gauges `datax_replicas_failed` and
  `datax_replicas_missing`, and the health check counts them as
  unavailable and raises the new checks `replicas-failed` (critical) and
  `replicas-missing` (warning). Additive heartbeat fields, so no version
  gate. Dead-node repair still acts on liveness only. Each list in the
  heartbeat row holds at most 64 range IDs (the lowest), so a node with
  thousands of dead replicas does not bloat its row on every heartbeat;
  `failed_replica_count` and `missing_replica_count` carry the exact
  totals, which the health summaries report.
- **A node started without `--dir` says that its data is in memory.**
  `datax init` and `datax start` still default to an in-memory store,
  but nothing at startup said so, so a service that forgot `--dir`, or
  whose `--dir=$VAR` was empty, served normally until a restart lost
  everything. It now prints a warning banner to stderr and logs a
  warning. The new `--in-memory` flag runs in memory on purpose without
  the warning; it is refused together with `--dir`.
- **`SEAL-IDENTITY` and a resealed metadata backup survive a power
  loss.** A node under a share root writes `SEAL-IDENTITY` (where to ask
  for the key before its store opens), and a store-key rotation, online
  or offline, reseals `metadata-backup.json`; each was written to a
  temporary file and renamed with no fsync, so a power loss could leave
  it empty or truncated under its final name. Each file is now synced
  before the rename and the data directory after it.
- **Local copies of the registry and the peer-unseal setting survive a
  crash, and the metadata backup is no longer rewritten every beat.**
  The node keeps the registry in its state engine so a restart can dial
  its peers before any range has a leader, and a copy of the cluster's
  peer-unseal setting. Both used an unsynced write, which on a state
  engine with no WAL reaches disk only at the next memtable flush, so a
  crash could restart a node with addresses its peers had left, or roll
  the setting back. The registry is now written durably when a node
  joins, moves, starts draining or changes binary (a liveness- or
  load-only beat stays a plain write, and an unchanged one writes
  nothing); the setting is written durably whenever it changes.
  `metadata-backup.json`, fsynced on each write since the fix below, was
  written on every heartbeat; it is now written only when its metadata
  (less the capture time and per-beat registry figures) or the store key
  changed, or the file is missing.
- **A backup reported written survives a power loss.** Backup data
  files, the manifest, their renames and the destination directory were
  never fsynced, nor was `metadata-backup.json`, so a power loss soon
  after "backup written" could leave a manifest over empty or truncated
  data files, found only when a restore refused the chain. Each file is
  now synced before its rename and the directory after, and the backup
  reports success only then. On a split store the local copy of the
  peer-unseal setting is also flushed when it changes, so a crash cannot
  roll "off" back to "on".
- **A multi-statement query or a pipeline is one implicit transaction,
  as in PostgreSQL.** Outside `BEGIN`, each statement of a simple Query
  with several statements, and each Execute of an extended-protocol
  pipeline, committed on its own, so `INSERT ...; SELECT 1/0` or a pgx
  `SendBatch` whose last statement failed left the earlier writes
  committed. They now run in one implicit transaction, committed at the
  end of the Query or at the `Sync` and rolled back if a statement
  fails; `BEGIN`, `COMMIT` and `ROLLBACK` among them follow PostgreSQL's
  rules. A lone statement (one Execute before its `Sync`) still runs on
  its own, with the automatic retry. Savepoints, and the statements
  PostgreSQL refuses in a transaction block (`VACUUM`, `CREATE INDEX
  CONCURRENTLY`, `DROP INDEX CONCURRENTLY`, `DISCARD ALL`: `25001`), are
  refused in a multi-statement implicit transaction. datax's online
  statements (`CREATE INDEX` on an existing table, the `ALTER TABLE`
  actions that read the rows, `CREATE TABLE ... AS`, `ANALYZE`, `AS OF
  SYSTEM TIME`, ...) commit the statements before them and run on their
  own, so a migration file sent as one `Exec` keeps working.
- **A COMMIT no longer reports success for writes it then loses after
  a multi-range write failed in part.** When a write spanning ranges
  failed for any reason other than a duplicate key (a write shed under
  storage overload, a deadline, a routing failure), the part that
  created the transaction record, or other ranges' parts, could already
  have applied, but the transaction treated the write as not having
  happened. After `ROLLBACK TO SAVEPOINT`, a later `COMMIT` took the
  transaction for read-only and returned success; the record expired
  about 5 s later and every write was lost. The same failure let a write
  rolled back to a savepoint become visible at COMMIT, and left a
  `ROLLBACK`'s intents blocking their keys for about 5 s. Every key of
  such a write is now tracked, and COMMIT and ROLLBACK end the record.
- **A replica seeded by a preseed snapshot survives a crash on a split
  store.** The state engine there has no write-ahead log, and the new
  replica's descriptor reached disk only at the next memtable flush,
  while the replica was already voting through the raft engine. A crash
  in that window, which on a quiet store could last hours, left the
  range counting a voter that would never load again, and with too few
  voters it lost quorum for good. The store now flushes the state engine
  once per created replica, before its raft group starts. A store that
  drops raft messages for a range it holds raft state for now logs a
  rate-limited warning instead of a debug line.
- **Finalizing an upgrade no longer ignores nodes that are down or
  draining.** `datax debug upgrade` checked only live, non-draining
  nodes, so a node down (or decommissioning) on the old binary could
  come back, or keep serving, under the new cluster version: it failed
  to apply commands it did not know, skipped the write and moved on, and
  its replicas diverged. Finalize now refuses while any node that may
  hold replicas is not known to run the new binary (every registry row
  except fully decommissioned nodes, and every node in a range
  descriptor), and names each one with what to do: restart it on the
  new binary or decommission it. A node also never runs above its
  binary's version: one restarting into a newer cluster is refused at
  re-announce or join, and a running one that sees the version pass it
  stops serving, never adopting or persisting the version.
- **`ALTER COLUMN ... TYPE DECIMAL(p,s)` rounds the stored values.**
  Narrowing a DECIMAL column's scale checked each value against the new
  type but stored it unrounded, so after `ALTER COLUMN c TYPE
  DECIMAL(10,2)` a row holding `1.005` still held and showed `1.005`
  (PostgreSQL: `1.01`), and `c = 1.01` did not match it. The rewrite now
  rounds like every other write into the column (halves away from
  zero), and a value that no longer fits is `22003`. Narrowing the
  element type of an array column (`DECIMAL(10,3)[]` to
  `DECIMAL(10,2)[]`, `VARCHAR(10)[]` to `VARCHAR(3)[]`, `INT8[]` to
  `INT2[]`) was a descriptor-only change that kept values the new type
  cannot hold; it is now the online rewrite, which rounds or checks
  every element.
- **Numeric division and `avg` keep PostgreSQL's precision.** DECIMAL
  `/` and `avg` over DECIMAL or integer columns rounded to a fixed 6
  fractional digits, half to even, so `0.0000005::numeric / 1` gave
  `0`, `1::numeric / 3` gave `0.333333` (PostgreSQL:
  `0.33333333333333333333`) and a filter such as `WHERE x / y > 1e-7`
  silently dropped rows. Both now use PostgreSQL's result scale (at
  least 16 significant digits and at least either operand's scale, at
  most 1000), round halves away from zero, and show the result at that
  scale with PostgreSQL's trailing zeros, in text and binary.
- **`DECIMAL(p,s)` columns round halves away from zero, as PostgreSQL
  does.** A value written to a `DECIMAL(p,s)` / `NUMERIC(p,s)` column
  rounded half to even, so `1.005` into `DECIMAL(10,2)` stored `1.00`
  and `2.5` into `DECIMAL(5,0)` stored `2` (PostgreSQL: `1.01` and
  `3`), while `SELECT 1.005::decimal(10,2)` already gave `1.01`. Every
  write path (INSERT, UPDATE, ON CONFLICT, COPY, text input, prepared
  parameters, column defaults) now rounds like the cast; negative
  values round away from zero too (`-1.005` → `-1.01`). This changes
  the stored value of new writes only: rows already stored keep the
  value they were rounded to, and a value that ties now may land on a
  different primary or unique key than an identical value written
  before the upgrade.
- **A range under a steady write load keeps its closed timestamp.**
  A closed-timestamp promise at T now waits only for writes that could
  still land at or below T; writes at current timestamps and
  transaction-record operations pass it. Before, on a range taking bulk
  INSERTs or COPY, the promise timed out every round, the range never
  published a closed timestamp (so no follower reads), and each timeout
  delayed the promises of the store's other ranges.
  - Measured on the busy range: closed timestamp "never published" →
    at most about 4.4 s stale; latch wait under COPY on 3 nodes
    1.45 → 0.23 s per second.
- **Splits no longer block the left side, and range-wide operations are
  no longer starved.** A split latches only its right-hand side.
  Splits, merges, range clears and closed-timestamp promises queue
  fairly: once one waits, later conflicting requests wait behind it.
- **A `search_path` schema no longer costs a read per statement.** With
  a schema other than `public` first on the path (for example
  `SET search_path = app, public`, or a role with a schema of its own
  name), a statement on a `public` table no longer reads the name in
  that schema first: a point SELECT or INSERT is 1 KV batch instead of
  2, with about 30% fewer allocations.
  - CREATE TABLE/VIEW or RENAME to a name another schema in the
    database already holds, and `ALTER TABLE ... SET SCHEMA`, now
    drain gateways as CREATE SCHEMA does (about 0.5 s on a multi-node
    cluster).
- **Fewer allocations per row on three more scan shapes**, with output
  unchanged byte for byte:
  - Reverse scans (`ORDER BY pk DESC`, reverse index scans) copy rows
    into per-scan chunks: 2,016 → 26 allocations per 1,000 rows.
  - A streamed SELECT with computed select items or a computed WHERE
    decodes only the columns it reads into one reused row, and decodes
    the columns a filter does not read only for rows that pass:
    `SELECT k + 1, n * 2` 7.0 → 0.06 allocations per row; a filter
    passing one row in three 9.1 → 1.1. Select-list expressions are
    still evaluated only for the rows the client pulls.
  - A `to_tsvector(cfg, col) @@ q` filter matches the stored text in
    place: 12.2 → 0.2 allocations per row returned.
- **Large result sets stream at less than half the server CPU per row.**
  The wire output is unchanged byte for byte.
  - A scan whose select list is bare columns decodes each row into one
    reused row, with only the columns it reads. That is about 1
    allocation per row instead of 10.
  - DataRows are encoded straight into the connection's buffer.
  - Storage copies scan rows out of Pebble into per-scan chunks instead
    of allocating per row.
  - `bench scan` (20,000 rows per query) went from about 41 to 96
    queries/s on one node, and CPU per row from 3.9 to 1.6 µs.
- **Search and vector filters are much cheaper per row.** None of these
  changes alters a result.
  - **Exact vector nearest-neighbour scans** (`ORDER BY v <-> q LIMIT k`
    without an index) compute distances on the stored bytes. They decode
    only rows that make the top k. Server CPU per query fell 117 → 48 ms
    over 50,000 × 64-dimension vectors, with 4× fewer allocations.
    Distances and tie order are bit-identical.
  - **ivfflat** scores candidates without decoding index entries and,
    without a filter, keeps only the k nearest instead of sorting them
    all.
  - **`to_tsvector(cfg, text) @@ query` as a filter** checks the text
    against the query while tokenizing and stops once the answer is
    known, instead of building, rendering and re-parsing a tsvector for
    every row. About 24 → 1.2 µs per document.
    - A constant tsquery is made once per statement.
    - The GIN recheck shares the fast path, roughly doubling GIN query
      throughput.
    - `to_tsvector` and tsvector parsing allocate about 5× less.
  - **jsonb `@>` and `<@`** walk the stored text, stopping as soon as the
    answer is known, with a constant right operand compiled once per
    statement. An unindexed containment filter costs about 0.8 µs and 1
    allocation per row, against about 10 µs and 70. GIN-served `@>` is
    about 1.6× faster.
    - A streaming scan decodes only the columns its filter and
      projection read.
- **Point reads and writes are back to their speed before 0.58.0's
  catalog work** (performance analysis, 2026-11-20). Server CPU per point
  read had grown from 78 µs to 128 µs since 0.57, and `kv-95-5` had
  fallen 29% on one node and 34% on three. Three causes are fixed:
  - Every statement read `/system/schemans/<db>/"<role>"` to resolve the
    `"$user"` entry of the default `search_path`, an uncached KV round
    trip that almost always missed. Each node now caches every
    database's schemas under a lease, as it does tables. A schema change
    (`CREATE`, `ALTER` or `DROP SCHEMA`, `GRANT` / `REVOKE` on a schema,
    a restore) bumps the database's schema generation and returns once
    every node resolves through the new set, which adds up to about half
    a second on a multi-node cluster. A prepared point read is one KV
    batch again, and its server CPU fell from 109 to 81 µs.
  - Every transactional read looked up the range's abort span (#320).
    A transaction that has not written cannot have an entry, so the
    lookup now runs only when the batch's sequence or anchor key says it
    has written, which removes about 4 µs per read.
  - The statement path had picked up allocations: a prepared point
    SELECT made 159 server allocations, against 97 on 11-14, and a point
    UPDATE 346, against 259. They are down to 97 and 272 before the
    schema fix, which removes another 47 from the SELECT. The changes:
    - The activity fingerprint is taken once per prepared statement.
    - Wire messages and session resolution state are reused.
    - The routing fallback caches its node list.
    - A latch makes its release channel only when someone waits.
    - A lease-holding leader with no read-index round in flight
      confirms a read through its lease inline, the round's own first
      step, instead of through a goroutine.
- Executions of a prepared statement are recorded under one statement
  shape, the one its text has in the simple protocol. They were split
  between the qualified and the alias-stripped forms.
- **Bulk INSERTs of 1,000 rows no longer push and refresh each other.**
  Each statement's uniqueness checks filled a timestamp-cache generation
  every few statements, and folding it into the floor pushed every
  statement in flight. The pushed statements then refreshed reads
  condensed into spans covering most of the table, one batch per span,
  latching against every concurrent writer and usually failing. Now a
  generation that took reads within the last second grows instead of
  folding (up to 65,536 point reads). A transaction keeps 4,096 read
  spans before condensing (512 before), and a refresh sends its point
  spans in one batch. One node, 16 workers, 1,000-row batches: KV batches
  per statement 525 → 2.6, refreshes 195 → 0, restarts 365 → 0, p99
  8 s → 0.8 s.
- One-phase commits (autocommit writes) were missing from
  `datax_kv_batch_latency_seconds`; they are counted now.
- **Two range splits could be given the same range ID** when the range
  holding the ID counter was split or merged at the same moment, which
  heavy DDL load does (#389). A write queued behind a split was applied
  by the half that no longer owned its key, so two increments of the
  counter read the same value; one range then overwrote the other, a
  span was left with no replica, and every new connection failed with
  `no replica covering key /system/dbns/...` until a restart. The same
  race could hand out a descriptor ID or a sequence value twice. A
  write's key is now checked against the range once its latches are
  held, and a split draws another ID if the one it got is in use.
- **A statement could see another table's columns** (`42703 column
  ... does not exist` right after `CREATE TABLE` made it) when a
  descriptor ID was handed out twice and the second table overwrote the
  first's descriptor (#390). New objects skip an ID already in use, and
  a name entry that leads to another object's descriptor is reported as
  catalog damage (`XX000`) instead of used.
- **Autocommit DDL no longer fails with `40001`** when it conflicts with
  another catalog change (#392): the error a DDL statement made of the
  conflict had lost its link to the retryable cause, so the implicit
  retry gave up at once. Implicit statements are now re-run with backoff
  until they succeed, their `statement_timeout` nears, or 30 seconds
  pass; only an explicit transaction block still returns `40001`.
- **Writes shed under storage backpressure fail promptly with `40001`**
  instead of waiting past the client's deadline (#391): a shed write is
  retried for at most 5 seconds, or until just before the statement's
  deadline, and a shed non-transactional write is `40001` rather than
  `XX000`. A node no longer keeps routing-cache entries for ranges its
  merge queue merged away, and the dropped-table reclaim waits while the
  node is shedding writes.
- **A scalar subquery in a select list with a FROM is described with
  its own type** (#388), for example `count(*)` as `int8` rather than
  `text`, in Describe and the simple protocol alike, with its values,
  NULL included, sent in that type; `EXISTS` and `IN (SELECT ...)` are
  `boolean`. Describing a portal reports the result formats its Bind
  chose, so pgx's describe modes read binary columns correctly.
- **Rate-based health problems no longer miss a burst between two
  reads.** Health is sampled when it is read; a read longer ago than
  the rate window (five minutes by default) was dropped as the
  baseline, leaving the sample just taken, so every increase since read
  as zero and `auth-failures`, `auth-throttled` and the other rate
  checks never raised for a cluster polled less often than the window.
  The baseline is now the newest read at least a window old, and the
  rate is taken over the time since it.
- **Catalog queries with correlated subqueries are several times
  faster.** A correlated subquery over `pg_class`, `pg_attribute` and
  the other virtual catalogs gathered the whole catalog again for every
  outer row; pg_dump's catalog queries are full of them, so a dump of a
  small database took about 3 s (and a pg_dump test under the race
  detector overran its four-minute budget). A statement now gathers
  each virtual table once: the same dump takes under a second.
- **An untyped parameter or quoted literal in arithmetic with a number
  takes the number's type** (#381). `c.x = a.id + $1` failed with
  `XX000 cannot use FLOAT8 value as INT8` when `$1` arrived in text
  format (pgx exec or simple mode, or any text-format client): always
  inside a correlated subquery, in `WHERE` or a join's `ON`, and in a
  plain `x = 1 + $1`, because the value was read as a float. It is now
  read as the other operand's type, and is `22P02` when it does not
  parse, as in PostgreSQL. Comparisons no longer answer `XX000` on a
  type mismatch: an integer compared with a `float8` compares
  numerically, unparseable text is `22P02`, other mismatches are
  `42883`, and an outer column compared with a text parameter
  (`outer.id < $1`) works.
- **The temporary-table sweep no longer drops a live node's tables.**
  A node judged another node dead from its in-memory copy of the
  registry, which lags the rows by up to a liveness beat (3s), while a
  live node's own row is up to a beat old. With a dead-node threshold
  near a beat, a live node's sessions could lose their temporary tables
  to another node's sweep. The sweep now reads the registry rows fresh
  before judging another node (and judges none if it cannot), and never
  on less than two beats. The default threshold (30s) left a margin, so
  only short thresholds, or a node already lagging, were exposed.
- **Row expiry no longer stalls with a short interval.** Each table in
  a pass had a budget of one interval; with a short `RowTTLInterval` a
  query that needs a moment longer (the first read after a restart
  waits on a lease proposal) was cut off in every pass, so the table
  never lost an expired row, and nothing reported it. A table now gets
  at least 5 seconds. `TestRowTTLSurvivesRestartAndBackup` failed 4 of
  5 runs under `-race` because of it.
- **`pg_get_userbyid`** returns the owning role instead of always
  `root`.
- **Subqueries in a join's `ON` condition are evaluated** (#379). Before,
  they were silently skipped:
  - `b.id IN (SELECT ...)` in `ON` never matched, so the join returned no
    rows, or only NULL-extended ones for a `LEFT JOIN`.
  - `NOT IN (SELECT ...)` always matched.
  - Scalar and `EXISTS` subqueries failed with `XX000` or `42703`.

  Subqueries that read nothing from the join run once, as in `WHERE`. Ones
  that read the tables joined so far run per candidate row. This holds for
  inner, `LEFT`, `RIGHT`, `FULL` and `LATERAL` joins, and outer joins
  NULL-extend the unmatched side. `EXPLAIN` no longer runs a correlated
  `ON` subquery while planning.
- **A purge no longer surfaces as `72000 snapshot too old` in ordinary
  transactions** (#374). A transaction that ran into another's
  uncommitted write while `ALTER TABLE ... PURGE` ran could fail with
  `72000`: the intent push and resolution it sent are not transactional
  and could arrive below the new GC threshold. Requests that touch only
  transaction records and the transaction's own intents (push, heartbeat,
  recovery, intent resolution and rollback, an aborting end) are no
  longer checked against the threshold, and any other below-threshold
  refusal of a non-historical read is a `40001` restart; a single
  statement outside a transaction block retries it internally. `72000`
  is now only for `AS OF SYSTEM TIME` and imported-snapshot reads.
- **A `COMMIT` refused with `40001` releases its locks at once** (#374).
  The abort after a refused commit was itself refused below the GC
  threshold, so its intents stayed until the record expired and the next
  writer to those rows waited about 5 seconds. A `ROLLBACK TO SAVEPOINT`
  refused for the same reason reports `40001` instead of `3B001`.
- **Dropped tables are drained from every gateway** (#377). Once
  `DROP TABLE`, `DROP VIEW`, `DROP OWNED` or `DROP DATABASE ... CASCADE`
  returns, no gateway still serves the dropped table from its leased cache;
  lookups fail with `42P01`. Before, other gateways could plan against it
  until their next lease renewal, and a renewal racing the drop could
  re-cache it on the dropping gateway. A statement planned against the
  table before the drop can no longer commit after it, and the dropped-table
  cleanup does not clear a table's rows while any gateway still holds a
  lease on it. A drop now waits like `ALTER` does: a few seconds at the
  default lease TTL when other gateways cache the table.
- **Default privileges no longer reach the sealed system tables.** A rule
  for `root` (`ALTER DEFAULT PRIVILEGES GRANT SELECT ON TABLES TO ...`) made
  before `datax_audit`, the large-object tables or `datax_keys` were created
  stamped its grants on them, opening the audit trail and every large
  object's bytes to the grantee. Those tables are now created with no
  grants; rules still apply to `datax_metrics`, the shape tables and
  `datax_ui_prefs`, as they do to any table root creates. Admins may
  `REVOKE` any privilege on a sealed table to clean up grants stamped
  earlier, and a grant on `datax_keys` no longer shows its row count. See
  docs/user/security.md, "Default privileges and the cluster's own tables".
- **`GRANT`, `REVOKE` and the policy statements read the table's current
  descriptor.** They decided what to change from the gateway's cached copy,
  which can lag a change still draining from another gateway: a `REVOKE` of
  a privilege granted moments earlier elsewhere, a `GRANT` of one revoked
  meanwhile, or a `DROP POLICY IF EXISTS` of a new policy could report
  success and change nothing. They now read the descriptor in their own
  transaction.
- **Concurrent online `CREATE INDEX` on one table could lose an index**
  (#368): every phase of an online schema change wrote the table descriptor
  from the gateway's cached copy, a blind write, so one build could write
  the table back without the other's index (and reuse its index ID) while
  both reported success, or fail with "index vanished during backfill".
  Descriptor writes now compare the version they replace and retry instead
  of overwriting, and the phases re-read the descriptor in their own
  transaction and change only their own index; this covers `DROP INDEX`,
  `ALTER TABLE`, `ADD CONSTRAINT` and `TRUNCATE` too. An explicit
  transaction whose DDL follows another session's change to the same table
  now gets `40001` rather than writing over it.
- **Online `CREATE INDEX` failed with "conflicting write intents"** (#368)
  when a concurrent writer had an open transaction as the build drained.
- **Catalog values pg_dump reads** (#366): a table whose only index is its
  primary key reported `relhasindex` false, so a dump lost the key;
  `pg_get_viewdef` lacked the trailing `;`, so pg_dump cut the view's last
  character (`price < 10` became `price < 1`); `format_type` dropped the
  typmod; a policy's `USING` clause was lost; `set_config` over
  `pg_settings` ran once instead of per row.
- **A correlated subquery bound a table's name hidden by its alias**
  (#361): `SELECT grp, (SELECT count(*) FROM ev e2 WHERE e2.grp = ev.grp)
  FROM ev GROUP BY grp` counted the whole table, the inner `ev` answering to
  `ev.`. An aliased `FROM` item now answers only to its alias, as in
  PostgreSQL, and the name behind it is `42P01 invalid reference to
  FROM-clause entry`, with PostgreSQL's hint. The same rule fixed `ORDER BY`
  a correlated subquery or its alias, an `UPDATE` whose `SET` subquery reads
  the row, and queries PostgreSQL rejects that datax accepted; an ambiguous
  outer reference is `42702` (was `42703`).
- **A `FROM`-less `SELECT` ignored its `WHERE`**: `SELECT 1 WHERE false`
  returned a row, so `EXISTS (SELECT 1 WHERE ...)` was always true.
- **Float output is PostgreSQL's** (#365): fixed notation up to 1e15
  (`123456789.125`, not `1.23456789125e+08`) and `Infinity` / `-Infinity`;
  `extra_float_digits` 0 and below rounds; binary `NaN` has PostgreSQL's bit
  pattern.
- **Joins** (#363): an `ON` equality in a correlated subquery that reads the
  enclosing row failed with `42P01`; `x = a.col OR x = b.col` across joined
  tables failed with `42703`; a `RIGHT` or `FULL` join from an always-empty
  catalog table returned no rows.
- **A statement no longer plans against a schema a drain has written
  off.** A gateway's lease renewal published the new version, and a
  schema change's drain returned on seeing it, a moment before the
  gateway's cache held the new descriptor; a statement starting on that
  gateway in between was handed the old one (a missing new column, or a
  write that could commit after the drain). Until the new entry is
  installed, such a statement now reads the descriptor in its own
  transaction.
- **`UPDATE t SET a = b, b = a` swaps** (#355): every `SET` of a row reads
  the row as it was before the statement, as in PostgreSQL. Before, each
  value was written before the next was computed, so both columns took
  `b`.
- **`ALTER TABLE ... DROP COLUMN` drops the indexes that use the column**
  (#358), as PostgreSQL does; it was refused.
- **`jsonb || jsonb` merges** (#342) as in PostgreSQL instead of joining the
  two texts: objects merge (the right side wins), arrays concatenate, a
  non-array is appended or prepended, and two non-arrays make a two-element
  array. An untyped literal or parameter beside a jsonb is read as jsonb, so
  `UPDATE t SET j = j || '{"k":1}'` stores the merged document; the result
  describes as jsonb. `json || json` merges the same way (datax stores
  `json` as `jsonb`; PostgreSQL refuses it), and `jsonb || <non-text
  scalar>` is refused with `42883`.
- **Casts to a type datax does not have fail** (#343) with PostgreSQL's
  `42704 type "..." does not exist`: `'a'::citext`, `::money`, `::inet`,
  `::hstore`, `::xml`, `::pg_lsn`, range types and any unknown name, in
  every statement kind, checked before anything is read. Before, the value
  silently passed through as text.
- **`length`, `octet_length`, `substring`/`substr`, `position`/`strpos` and
  `md5` of a bytea work on its bytes** (#344), not its `\x` hex text:
  `length('\x0102'::bytea)` is 2, and `substring` of a bytea is a bytea.
- **`TimeZone` is honored** (#340). Before, a session's zone only changed
  how `TIMESTAMPTZ` values were rendered on the wire: text without an
  offset still read as UTC and `::text`, `::date`, `date_trunc`, `extract`,
  `to_char` and `current_date` computed in UTC. Now input without an offset
  (literals, text parameters, `COPY`) reads in the zone, those functions
  and casts, `||` and day and month arithmetic (across DST) follow it, and
  timestamp input may name a zone (`'2024-07-01 12:00 Europe/Paris'`); the
  stored instant and the binary format stay UTC. A POSIX `TimeZone` such as
  `'UTC+5'` or `'+05:30'` now means what it means in PostgreSQL (west of
  Greenwich), and abbreviations are refused there (`22023`).
- **`pg_settings` reports each setting's type and unit, and what `RESET`
  restores** (#333). `vartype` and `unit` came from a hard-coded list
  that had fallen behind the session's own registry of settings, so
  `statement_memory_limit` (an integer in bytes), `ivfflat.probes`,
  `vector_index_scan` and `inverted_index_scan` read `string` with no
  unit. They now come from that registry. `reset_val` showed the current
  value; it is now the value `RESET` restores, the one a new session
  starts with.
- **`current_setting(name)` read per row raises `42704` for a name that is
  no setting** (#329). Since #326, `current_setting(s.name)` over a column
  (psql's `\dconfig` over `pg_settings`) evaluates per row, but a row
  whose name was no setting, or a custom `app.x` never `SET`, read NULL;
  PostgreSQL raises `42704 unrecognized configuration parameter "<name>"`.
  It now does, as the constant form `current_setting('nosuch')` already
  did. `current_setting(name, true)` (`missing_ok`) returns NULL in both
  forms, and `missing_ok` may be a column too. A NULL name is NULL, as in
  PostgreSQL; the constant form raised `42704` for it. Row-security
  policies keyed on `current_setting('app.tenant')` are unchanged: they
  read a constant, which was and is `42704` until the setting is `SET`.
- **`pg_settings` and `SHOW ALL` list custom settings, and
  `foreign_key_cascade_limit` reports its source** (#330). A custom
  setting the session has `SET` (`app.tenant`, `set_config('app.x', ...)`)
  was missing from both, so psql's `\dconfig` never showed it. It is now
  listed as PostgreSQL lists a placeholder variable: its value, `source =
  'session'`, `vartype = 'string'`, no `boot_val`, `reset_val` `''`, in
  category `Customized Options`. `SET foreign_key_cascade_limit` was kept
  outside the session's variables, so `pg_settings` reported `source =
  'default'` and the set value as `boot_val`; it now reports `session` and
  the new-session value `10000`, like the other settings. Keeping it with
  the other variables also ends a `SET LOCAL foreign_key_cascade_limit`
  with its transaction block; before, it outlived the block.
- **A point-in-time restore no longer refuses the audit table a node
  created between two backups** (#327). A node creates `datax_audit`
  when it first writes an audit record. When that happened between a
  full backup and the next history incremental, a restore of everything
  `--as-of` a moment in between was refused for that table: "it was
  created between ... and ...". A user table created inside the
  interval is still refused that way, because its definition at the
  moment is unknown. The cluster's own tables are now restored with
  their rows as of the moment, or empty if they did not exist yet, since
  their definition comes with the binary. A restore of everything into a
  cluster that already has tables now reports that first, before any
  per-table check.
- **psql's `\dd`, `\dAf`, `\dAo`, `\dAp` and `\dconfig` work** (psql 16,
  17 and 18). Each failed with an error. `\dd` read the `tableoid` system
  column, which the catalogs lacked; each catalog with OIDs now has it,
  hidden from `*`. `\dd` lists comments on constraints, and the new
  `COMMENT ON CONSTRAINT name ON table IS ...` sets one on a primary
  key, `UNIQUE`, `CHECK` or foreign-key constraint. `\dAf` failed on a
  select-list alias written without `AS` (`(SELECT ...) "Applicable
  types"`), which the select list now takes, as PostgreSQL does.
  `\dAo` and `\dAp` read `pg_amop` and `pg_amproc`, now present and
  empty: datax's index methods have no operator families. `\dconfig`
  called `current_setting()` on a column, which worked only on a
  constant; it now works per row. `pg_settings` now reports `source =
  'session'` and the new-session value as `boot_val` for a setting the
  session changed, so `\dconfig` lists those settings, as it does on
  PostgreSQL. Before, every setting read `default` and the list was
  always empty.
- **A comma or `CROSS` join keyed in `WHERE` no longer runs as a cross
  product.** psql's `\d <index>` joins `pg_index`, `pg_class` twice and
  `pg_am` with commas, equated in `WHERE`. Such a join had no `ON` keys,
  so each level fetched every row of its table for every partial row,
  and only the final `WHERE` filtered the product. On a node started with
  `datax init`, whose catalogs list the system tables, that was 177,504
  joined rows and about 60,000 catalog scans: `\d <index>` took 20–36 s. A
  `WHERE` equality of a later side's column with an earlier side's
  column, or with a constant, is now that side's lookup key, as an `ON`
  equality is. The same `\d` takes 0.07 s, and `EXPLAIN` shows an inner
  join with a point or index lookup where a key allows one. The full
  `WHERE` still filters the result. A `LEFT` side, and any chain with a
  `RIGHT` or `FULL` join, are unchanged.
- **40001 errors say "restart transaction:" once.** A retryable error's
  own text already starts with that prefix, and the conversion to a SQL
  error added it again. Every 40001 from a transaction restart read
  "restart transaction: restart transaction: ...", for example a large
  object's `loread` failing with `TransactionAbortedError`. The prefix
  is now added only when missing, and a wrapped error has it moved to
  the front.
- **Stopping a node no longer closes its store under a running request**
  (#324). The internode gRPC server's stop, one of the shutdown steps,
  did not wait for calls already in progress, so an admin, KV, raft or
  snapshot handler could still be reading the storage engine when the
  next step closed it: a data race, then a crash (seen in `TestSealJobs`
  teardown, from a purge step reading intents). Every gRPC call, every
  HTTP request and every SQL connection now runs as a task of the node's
  stopper. Once a stop begins no new one is admitted (gRPC answers
  `Unavailable`, HTTP 503), the running ones have their context canceled
  and the gRPC server's connections are closed, and nothing is closed
  until they have returned. The wait is bounded: a task still running
  after a minute is logged by name and the shutdown goes on.
- **After an online store-key rotation, the metadata backup is always
  under the new key.** Each heartbeat rewrites the backup, and a
  rotation reseals it right after swapping the key. A heartbeat's write
  that had read the old key just before the swap could land after the
  reseal. The backup then stayed sealed under the retired key until the
  next heartbeat, about 3 s later. A crash in that window left a
  recovery backup no current key could open. The seal and the write are
  now serialized, so the rotation's reseal always writes last.
- **A seal job no longer stalls on a lock its own stopped batch left.**
  Each batch locks the job's record for its transaction. When the node
  running the job stopped mid-batch, that lock stayed. The job runner's
  scan and each step's read of the job were done outside a transaction, so
  they failed on the lock instead of pushing it aside. The job then
  stood still until some other transaction happened to read the record.
  Both reads now run in a transaction, which aborts the abandoned batch
  once its heartbeat has expired, so the job resumes within a few
  seconds.
- **psql 17 and 18's describe commands.** psql chooses its catalog
  queries by its own version as well as the server's, so a newer client
  sends queries psql 16 did not, though datax reports server version 14:
  `\d+` reads `pg_constraint.connoinherit` (with `conislocal` and
  `coninhcount`, now present: every constraint is local and inherits
  nothing), and psql 18's `\dx` joins `LEFT JOIN
  pg_available_extensions() ae(name, default_version, comment) ON ...`,
  which failed with `syntax error: expected ON, found "("`. A
  set-returning function in `FROM` is now a member like a table: heading
  the `FROM` or joined (inner, `LEFT`, `RIGHT`, `FULL`, `CROSS`; `ON` or
  `USING`), with `[AS] alias (column, ...)` renaming its columns (too many
  names is `42P10`); a call reading another member's columns (`LATERAL`)
  is still refused. `pg_available_extensions()` and the
  `pg_available_extensions` view are new and empty: datax packages no
  installable extension (`CREATE EXTENSION vector` is accepted because the
  type is built in). `pg_partition_ancestors(...)` in `FROM` returns the
  relation itself. `TestPsqlCatalogQueries` runs the client named by
  `DATAX_PSQL` when set, logs `psql --version`, covers eleven more
  commands, and passes with psql 16, 17 and 18.
- **`RIGHT` / `FULL JOIN` dropped the joined side's unmatched rows when
  that side was a `WITH` member, a subquery, a table function or a
  catalog** (`pg_class c RIGHT JOIN pg_namespace n ON ...` kept only the
  namespaces that matched). Those rows have no primary key, and unmatched
  rows were told from matched ones by it; they are now told by their
  values.
- **Parameters are counted and typed wherever they stand** (#322). A
  parameter used only inside a subquery (`WHERE sid IN (SELECT sid FROM s
  WHERE g = $1)`), or only under `EXPLAIN` (`EXPLAIN SELECT $1::text`), was
  not counted, so a prepared statement refused its argument ("expected 0
  arguments"); and a parameter compared with a qualified column of a join
  inside `OR` (`g.at < $1 OR (g.at = $1 AND ...)`) was described as text,
  so a client sent a timestamp as text and it failed to parse. One walker
  (`parser.Walker`) now visits every expression position — subqueries of
  every kind, WITH members, set members, `JOIN ... ON`, `HAVING`, `ORDER
  BY`, windows, `LIMIT` / `OFFSET`, `VALUES`, `SET`, `ON CONFLICT`,
  `RETURNING`, `EXPLAIN` — for counting and for typing. A parameter takes
  the type of the column it is compared with or assigned to, resolved per
  query level (aliases, join sides, derived tables, WITH members,
  correlated references), or the type it is cast to (`$1::int8`); one
  parameter has one type, and two columns fixing different ones are
  `42P08`, as in PostgreSQL. `LIMIT '5'` (a client interpolating a
  parameter) parses.
- **A single table's alias works in every query shape** (#322). `SELECT
  count(*) FROM gr AS g WHERE g.at < $1` failed with `column "g.at" does not
  exist`: the alias was removed only on the plain (ungrouped, unwindowed)
  path. It is now removed for grouped, windowed and explained queries too,
  including aggregate arguments, `HAVING` and window clauses.
- **`now()` and the clock keywords in `RETURNING`** (#322). `UPDATE ...
  RETURNING now()` (and in `INSERT`, `DELETE`, `UPSERT`, `ON CONFLICT DO
  UPDATE SET` / `WHERE`) failed with `now() was not resolved before
  execution`; `RETURNING current_timestamp` with `unknown function`. They
  are now the statement's start time there as everywhere. `current_time`
  and `localtime` are new, and `current_timestamp`, `localtimestamp`,
  `current_time` and `localtime` take a precision (`current_timestamp(3)`).
- **`EXPLAIN` of a parameterized statement** (#322) describes and binds
  the statement's parameters; `EXPLAIN ANALYZE` runs with the bound values;
  and a `SELECT` without `FROM` is explained instead of refused.
- **`DROP DATABASE ... CASCADE` of a large database failed with `40001`
  after about a minute** (#322), "write timestamp diverged from read
  timestamp", with no other load. It reads every table's name and
  descriptor; past 512 read spans a transaction gave up its read set, so
  once a drop's commit outlived the closed-timestamp lag (3 s) and was
  pushed above it, the transaction could not refresh and restarted, every
  attempt alike, 21 times. A transaction now condenses its read spans past
  512 into at most 64 covering ones and goes on refreshing; and `DROP
  DATABASE` no longer reads each table a second time and writes its drops
  in one batch instead of a round trip per key. A drop that loses to
  concurrent DDL in the same database still fails with `40001` as a
  whole. `CREATE TABLE` (and view, sequence) now reads its database in its
  own transaction: one racing `DROP DATABASE` could commit a table under
  the dropped database, where nothing would ever find or drop it.
- **A transaction blocked on another's lock noticed it was released up to
  100 ms late.** It re-asked the lock holder's transaction record every
  100 ms. Now the holder's coordinator wakes it as soon as the holder has
  committed or aborted and released its rows, when both run on the same
  node; otherwise it re-asks after 5 ms, doubling to 100 ms, with jitter so
  waiters behind one holder do not ask in step. On the bank workload (one
  node, 16 workers, 1,000 accounts) p99 latency fell from 111 ms to 45 ms
  on a busy machine and to 24 ms on a quiet one, for about 10 % more pushes
  per transfer; constructed deadlocks are broken in 10–20 ms instead of
  hundreds. New metric `datax_txn_pushes_total`. Nothing replicated or on
  the wire changed.
- **A dropped table is reclaimed with one raft command per range** (#316).
  The reclaim deleted a dropped table row by row and then collected every
  version; once the cluster has finalized v22 it empties each range of the
  table's span with a replicated range clear: the leader checks the span is
  a dropped table's, refuses an intent, and picks a time above every version
  and every read served; every replica deletes the span with one range
  deletion and raises the range's GC threshold to that time in the same
  command. The purge still follows, to take the bytes out of the files and
  the logs. A table of 100,000 rows on three nodes: 939 raft entries and
  43.9 MB of log payload in 3.0 s before, 24 entries, 4.1 kB and 0.30 s
  now. The consistency checker also compares each replica's size and GC
  threshold. New metrics `datax_range_clears_total` and
  `datax_raft_entry_bytes_appended_total`. Cluster protocol version v22.

### Fixed
- Finding a range's leader for a purge gave up when a replica listed before
  the leader was on a node that was down, though another replica could
  answer; it now moves on to the next replica.
- **A transaction whose commit failed kept its rows locked for up to five
  seconds** (#315). A `COMMIT` refused with a `40001` (its refresh failed,
  or its deadline had passed) stopped the transaction's heartbeat but left
  its record pending and its intents in place, and the `ROLLBACK` that
  followed found the transaction finished and did nothing, so everyone
  who met one of its rows waited for the record to expire. The failed
  commit now aborts the record and resolves its intents before it
  returns.
- **An aborted transaction kept answering and missed its own writes**
  (#320). A stronger transaction that aborted another removed the writes it
  ran into, and the aborted one learned of it only at its next heartbeat
  or its `COMMIT`: an `INSERT` reported `INSERT 0 1`, the `SELECT` of the
  same key in the same transaction found nothing, and only the `COMMIT`
  failed — a client that acted on the empty read and rolled back never saw
  an error. Nothing such a transaction did could commit. Now the range
  records the abort where the writes were removed (its abort span,
  replicated with the range: snapshots, splits, merges, the consistency
  checker), so the transaction's next statement there fails with `40001`;
  and once its coordinator knows it was aborted (its heartbeat, every
  second, or a refused statement) every further statement fails with
  `40001` and its remaining writes are released at once. Four
  transactions at a time writing one new key and reading it back
  (`INSERT ... ON CONFLICT DO NOTHING`, `UPSERT`, plain `INSERT`) missed
  their own write 51 times in 6,000 on 26250af and never now. Cluster
  protocol version v22 for the range's part; below it, the coordinator's.
- **`FOR UPDATE SKIP LOCKED ... LIMIT` with a filter failed most commits**
  (#320). With a condition the scan could not use (`WHERE runnable ORDER BY
  id LIMIT 1`), the statement read the whole table and kept all of it in
  the transaction's read set, so another worker's commit on any row made
  this one's commit fail with `40001` ("write timestamp diverged from read
  timestamp"): four workers on a 200-row queue lost a third of their
  attempts. A limited scan now keeps in its read set only what it read up
  to the last row it returned, which is all its result depended on; the
  same queue loses none. (The report's empty `SKIP LOCKED` results with
  rows free were the locks of failed commits, released since #315.)
- **`FOR UPDATE SKIP LOCKED ... LIMIT` through an index returned too few
  rows** (#322). Where an index served the filter and the order (a queue's
  `WHERE queue = $1 AND state = 'pending' ORDER BY visible_at LIMIT 1` over
  an index on those columns), a row another transaction held still used up
  the limit, since the lock is on the row and not its index entry: the claim
  returned no row while others were free, and `LIMIT 2` returned one. The
  index scan now continues past locked rows until the limit is met, and the
  index entries of rows it passed over leave the transaction's read set, as
  the rows themselves did, so the holder's change to one no longer fails
  the skipper's commit with `40001`. Primary-key scans and in-memory sorts
  were not affected; `NOWAIT` and plain `FOR UPDATE` behave as before.
- **A write keyed by a `SKIP LOCKED` subquery waited for the rows it
  skipped** (#322). `UPDATE jobs SET ... WHERE id = (SELECT id ... LIMIT 1
  FOR UPDATE SKIP LOCKED)` — and the `IN (...)`, `DELETE` and `WITH` forms
  — read the whole table when the subquery returned nothing (`id = NULL`)
  or a list (`IN` had no access path), and a plain read waits for any row
  another transaction holds: the claim waited for the very lock it was
  written to avoid, then updated nothing. The subquery itself always
  honoured the clause. The planner now looks up `WHERE pk IN (constants)`
  (the other key columns pinned by `=`) by primary-key point lookups, and
  reads nothing for a clause that cannot be true (`col = NULL`, `IN` over
  NULLs, a failed `EXISTS`); `EXPLAIN` says `point lookups on primary key
  (N keys)` and `no rows read (the WHERE clause cannot be true)`. The
  claim now takes the first free row at once.
- **A transaction waited for a lock it could not outwait** (#315). Blocked
  on another transaction's uncommitted write to a row it had itself read —
  on the bank workload, a transfer whose account a newer transfer was
  changing — a transaction polled until the other committed and then
  failed with `40001` (usually "read within uncertainty interval"), since
  the committed change invalidated its read. It now fails at once, unless
  its priority lets it abort the other transaction, which it still does.
  With the fix above, on the bank workload (one secure, encrypted on-disk
  node, 16 workers, 1,000 accounts) committed transfers went from a median
  of 238 a second to 974, and the attempts ending in a `40001` from 3.1 s
  each and about 70 % of the workers' time to 0.02 s and under 3 %.
- **`'1 ms'::interval` was a minute** and `'1 us'` was refused: interval
  units were matched after a trailing "s" was taken off, so the
  abbreviations `ms` and `us` read as `m` and `u`. Both now mean what
  PostgreSQL means; plural units are unchanged.
- **Online `CREATE INDEX` could keep entries for rows that changed while it
  was waiting for every gateway to adopt the index** (found while building
  #299). One gateway that had adopted the index wrote a row's entries,
  another that had not yet changed or deleted the row, and the entries
  stayed: queries through the index then reported "index entry points at a
  missing row" or returned a row under a value it no longer held. The build
  now empties the index after the wait and before its backfill, which
  rewrites every row's entries. A build of a heavily written table does one
  more pass over the new index.
- A store on the balanced profile could sit at its L0 backpressure gate
  indefinitely after a long run of writes in key order: non-overlapping L0
  files never triggered a compaction below the gate's file count (#303).
- A transaction that made its first write more than five seconds after `BEGIN`
  could be aborted by any transaction that met its row during the following
  second: its record was created with a stale heartbeat (#303).
- A gateway renewing its lease on a table that had been renamed could
  delete the lease it already held on the same table under its new name,
  leaving a cached descriptor no schema change waited for until the next
  renewal (#311).
- **A write in flight across a planned lease transfer is answered with
  its outcome** (#271). When the balancer shed a lease, writes proposed
  just before the hand-over were told at once "leadership lost with
  proposal in flight" — an unknown outcome, which a one-phase commit
  cannot resend, so a client occasionally saw `XX000`. For a transfer
  the replica itself asked for, those entries are in the new leader's
  log (raft hands over only to a follower holding the whole log), so
  their proposers now wait for the entries' own apply, bounded at two
  seconds. An unplanned loss of leadership is answered at once, as
  before.
- **A merge freeze left behind by an attempt that died no longer keeps
  the range out of service** (#266). A range frozen for a merge by an
  attempt that crashed, lost its leader or ended with its proposal's
  fate unknown stayed frozen for as long as a later pass could not
  finish the merge — with a replica behind, indefinitely. Such a freeze
  is now unwound safely: the left-hand range's generation is moved on
  first, which rules out that attempt's merge on every replica (a merge
  applies only at the generation it was built from, and a freeze now
  names the generation it was made for), and then the freeze is
  cleared. The range serves again and the merge is retried afresh;
  `datax_range_merges_unwound_total` counts these. One merge driver
  runs at a time per range on a node, and an attempt giving up on its
  own freeze can no longer clear one a later attempt made. Needs
  cluster version v20.
- **A replica change that lands on a range frozen for a merge is
  refused** (#268). The change is checked before it is proposed, but a
  merge's freeze could apply in between, and a freeze moves no
  generation, so the change applied anyway. A removal applied that way
  let a node drop its replica of the frozen range while keeping the
  range it was merging into, and the merge's apply there then stopped
  the node ("local RHS replica missing"). The change is now refused at
  apply, on every replica at the same entry, and the proposer retries.
- **`ALTER TABLE ... SPLIT AT` no longer fails because the range
  changed first** (#290). A split is built from the range's descriptor
  and applied a moment later; when another split or a merge landed in
  between, the statement failed with "split against generation 0 but
  range is at 1". The range's leader now looks at the range again and
  carries on — splits it, finds the boundary already there, or sends
  the request on to the range that holds the key now — for every caller
  of a split, not only SQL. The refusal is a typed error
  (`DescriptorChangedError`) instead of a message to match, a split
  that meets a range frozen for a merge waits the merge out like any
  other request, and the frozen mark now survives the trip between
  nodes: it was missing from the binary error encoding, so a gateway on
  another node than the range's leader re-resolved the range instead of
  waiting. A split at a key that is already a range boundary is success
  rather than an error.
- **A re-encryption pass reports the bytes its seed's compaction
  rewrote** (#287). Flushing a pass's seed can set off an automatic
  compaction that retires the stale file before the pass's own
  compaction runs; the pass then reported zero bytes, which the sweep
  reads as "nothing left that a compaction can rewrite" and could stop
  early. The pass now measures from before the seed is written. This
  was the cause of the flaky `TestReencryptPass`.
- **Clicking a term in the console explains it again.** A column
  header, a section heading, a tile's label and the header's controls
  each open the glossary's entry for the term when clicked, and carry it
  as a tooltip. None of them had since the statement-shape charts
  landed (#208, #209): the charts added a function with the name of one
  the glossary used, the console's scripts share one scope, and the
  later function silently replaced the earlier, which then found no
  term in anything. The two have different names, and a test refuses a
  name declared twice.
- **The console's "when" columns tell the time again** (#272). Every
  event, slow statement and retried shape read `29910444m ago` — the
  current Unix time, in minutes — on the overview, the operations
  timeline, a node's page and the SQL view, and had since the viewer
  preferences landed (#204). The formatter takes an instant; nine call
  sites handed it `Date.now()` minus an instant, an age, and it
  subtracted that from now a second time. In clock-time mode the same
  cells showed a date in January 1970. The call sites pass the instant.
  An age is also said in the unit a person would use: it stopped at
  minutes, so a day-old event read `1440m ago`; it now reads `59m ago`,
  `1h 25m ago`, `17h ago`, `3d ago`, rounded down in every unit.
- **A console page opened directly loads its header** (#273). The
  header — which node served the page, who is signed in, the nodes the
  scope picker offers — and the insecure-mode banner are filled from
  the cluster document, and the node, schema, table and metrics views
  never fetched it. Reached by a click from another view they looked
  right, because the header was already filled; opened from a bookmark,
  a shared link or a reload they sat on `connecting…` for as long as
  the tab stayed open, and a cluster that authenticates nobody did not
  say so. Those views now fetch the document on arrival and every 30 s
  after.
- **The console does not scroll sideways on a phone** (#274). The SQL,
  data and node views were up to twice the width of a 390 px screen.
  The tables scroll inside their own box, but each copy button carries
  a screen-reader label that is absolutely positioned, and a scroll box
  clips such an element only if the box is positioned itself; the
  labels of cells scrolled out of view widened the page instead. The
  box is positioned now. Running text also breaks a word longer than
  the line, which a table with a fifty-character name needed on its own
  page. And a health problem's row wraps: its links had nowhere to go
  but past the right edge, so the overview scrolled sideways exactly
  when a problem was open.
- **Schema and transaction statements are named in SQL** (#275). The
  statement list showed `CREATETABLE bench_kv`, `CREATEINDEX bench_ij`
  and `ROLLBACKTOSAVEPOINT`: a statement that is named by kind rather
  than rendered in full took the parser's type name, upper-cased whole.
  It is now the words that open the statement — `CREATE TABLE
  bench_kv`, `CREATE INDEX ON bench_ij`, `ROLLBACK TO SAVEPOINT`,
  `GRANT` or `REVOKE`, `COPY`, `ALTER TABLE t OWNER TO`, and `ALTER
  ROLE` where a password change used to be listed as `CREATEROLE`.
  `SET`, `RESET` and `SHOW name` are told apart: they share a parser
  type and were one shape, so the `SHOW` a driver sends on connect was
  counted with the session's `SET`s. The fingerprint is a hash of that
  text, so these shapes get new fingerprints: their counts start again,
  the old rows age out of `datax_metrics_shapes` with its retention,
  and during a rolling upgrade a schema statement can be listed under
  both names. Queries and writes are untouched.
- **A read caught by a leadership change is answered NotLeader instead
  of timing out** (#269). A consistent read confirms leadership with a
  quorum round before it evaluates. A leader that stepped down with
  such a round in flight never answered it: raft drops the request,
  and the reader waited out the round's 3 s timeout. That is also the
  gateway's deadline for one attempt at one replica, so the gateway
  returned the replica's `read index abandoned: context deadline
  exceeded` as the statement's result. The balancer shedding a lease
  from a busy range is the everyday trigger, and the nightly benchmark
  hit it reading the time-series table straight after ingesting it. A
  replica that loses leadership now releases its waiting readers with
  NotLeader at once, a replica that is not the leader no longer asks
  raft for a round at all, and the gateway treats a read attempt that
  ran out its own per-attempt deadline as unanswered and tries the
  next replica under the caller's deadline
  (`datax_read_attempt_timeouts_total` counts those). Writes are not
  resent by the gateway on a timeout: an abandoned proposal is
  ambiguous, and the transaction layer owns that decision.
- **A preseeded replica whose membership change never landed is
  collected** (#245). A replica addition preseeds the target before
  proposing the change, so a change that never lands — its proposal
  timed out, the leader lost leadership, or the apply refused it because
  the range changed under it (#200) — left the target holding a replica
  that is a member of nothing: a raft group of one, campaigning on its
  own timer, its data on disk for good on a range that never comes back
  to that node. The housekeeping pass now asks `/meta` about a replica
  that has led nothing, heard from no leader and applied nothing for a
  grace (60 s; `ReplicaGCGrace`), and removes one the record does not
  list — names another range for its span, names its range without this
  store, or lists this store with a newer replica. No record means
  nothing (a repair in flight); a listed store is a partitioned member,
  left alone however quiet; a frozen replica is the merge's to remove. A
  raft message from a replica id the descriptor has moved past is
  dropped rather than stepped. Two counters:
  `datax_replica_gc_removed_total` and
  `datax_raft_nonmember_messages_dropped_total`.
- **A range row is labelled from its own key** (#252, from QA on #251).
  `keys_redacted` became exact per document in #251; the console still
  applied it per row, so a reader with `SELECT` on some tables was told
  "your role does not see the full key" on the copy control of a key it
  was looking at in full. Each range now carries `start_key_redacted`
  and `end_key_redacted` — the server knows which keys it rendered at
  their prefix, and the document says so where the row is — and the
  copy control, which copies the start key, is labelled from the start
  key's flag. The CSV keeps the document's any-of header and gains a
  "keys as shown" column (start, end, both, none) so an exported row
  says the same. `keys_redacted` stays for the controls that need one
  answer.
- **Every series colour clears 3:1 against the light panel** (#250, from
  QA on #249). Four of the light theme's eight line colours sat below
  WCAG 1.4.11's floor for graphical objects on white — the yellow at
  2.17:1, the pink 2.69, the green 2.82, and the orange only just above
  — which is faint for reasons unrelated to telling a line from its
  neighbours, the thing #240 and #249 were about. The four are
  re-stepped darker along their own hues (`--series-2` `#c9442a`, `-3`
  `#4ba27e`, `-4` `#bd8919`, `-5` `#d670a4`), found by the same
  constrained search as #249: every pair stays 15.6 OKLab units apart,
  no colour-vision pair opens or closes (`CVD_CLOSE_PAIRS` is
  unchanged), and the dark theme, whose minimum was already 3.96, is
  untouched. `TestSeriesLinesClearTheGraphicalContrastFloor` pins every
  slot of both themes to the floor, with exceptions recorded by number
  rather than as validator WARNs. Two costs, recorded here so they are
  not discovered later (the UI review on the PR): the light palette is
  more muted — `n3`'s chroma is now the lowest in either theme, and
  `n4` and `n5` come down too, which is what 3:1 against white costs —
  and light `n3`/`n5` under a colour-vision deficiency moved from 6.1
  to 4.8 OKLab units apart, the wrong direction, though the pair was
  already in `CVD_CLOSE_PAIRS` and never shares a frame. Three slots
  sit within 0.13 of the floor, so the next nudge in that region is
  stopped by contrast before separation.
- **Backup collects the manifest's raw groups from the same table restore
  checks them against** (#256, from QA and InfoSec on #254). #254 pinned
  `manifestKeyGroups` to the manifest struct; the spans were still
  written twice, once in the table restore holds a manifest to and once
  by hand in `RunBackup`, and a span added to the table alone widened
  what restore accepted with no test failing. `RunBackup` now collects
  each group from `manifestKeyGroups`' spans, so there is one list and
  nothing to keep in step, and the unit test derives its outside-key
  assertions from the table: every group refuses the authentication
  secret, the cluster version, the meta span and table keys, rather than
  the two groups a hand-written list happened to name.
- **A red scheduled run files an issue** (#244). The weekly govulncheck
  run on `main` was red for seven consecutive weeks before anyone read
  it: a scheduled run's result lives on the Actions tab, which nobody
  opens on a quiet repository, where a push's result has the person who
  pushed. The `vulncheck` and `bench` workflows now end with a step
  that, on a scheduled (or manually dispatched) run, writes the result
  where it is read: a red run files one standing issue per workflow —
  "Scheduled run red: vulncheck" under the `scheduled-run` label — with
  the run's link and the last lines of its output, or adds the new run
  to the issue already open, and a green run closes it. One issue per
  workflow, updated rather than one per night, so the alarm stays
  quieter than what it reports. The script is
  `.github/scripts/standing-issue.sh`; both workflows gain
  `issues: write`.
- **The manifest's raw-record groups are pinned to the spans restore
  checks** (#242, from QA on #239). `checkManifestKeys` held each of the
  five groups a backup collects — users, admins, roles, databases,
  sequences — to its spans (#238), but the groups lived in two places
  kept in step by memory: the manifest's `[]backupKV` fields and the
  check's table. A sixth group added to both the struct and the
  collector, and not to the table, would have been restored unchecked
  with no test failing. The table is now a named value
  (`manifestKeyGroups`, keyed by the field's JSON tag) and
  `TestManifestKeyGroupsCoverTheManifest` joins it to the struct by
  reflection: every `[]backupKV` field is named in the table, every
  entry names a field, and each entry's accessor returns that very field
  (a sentinel set through reflection comes back through it). Nothing
  restore does changes.
- **`keys_redacted` says a key was redacted, not that the caller is not
  an admin** (#241, from QA on #239). The cluster document's flag — what
  tells the console to label a copied key as the shown form — was set
  for every non-admin, so a reader with privileges on every table saw
  full keys and was still told its role did not see them, and its CSV
  headers said "(as shown)". The viewer now records the first key it
  renders at its prefix, and the document sets the flag from that, after
  every key is rendered: a caller who sees every table's keys, admin or
  not, gets the plain labels; a caller with one table hidden gets the
  warning, as before; a caller whose roles could not be resolved, for
  whom every table key is redacted, gets it too. The console side is
  unchanged. `TestRangeKeysAreShownPerPrivilege` adds a `read_all`
  auditor and asserts its document decodes both boundaries and leaves
  the flag unset.
- **A split or a replica change built before a merge no longer rewrites
  the merged range to its old bounds** (#200, the reshard half; the
  nightly bench's `routing did not converge … answer carried r125@106`).
  A split, a merge and a replica change each build the descriptor they
  will propose from the one the leader holds and propose it afterwards,
  so two of them on the same range can be built from the same descriptor
  and raft decides which applies first. The merge apply refused a trigger
  built from a superseded descriptor; the split apply and the membership
  change did not, and adopted theirs. A split or an addition built before
  the range absorbed its neighbour and applied after thus set the range
  back to its pre-merge span at the merged generation: the absorbed span
  belonged to no range on any node, `/meta` still named the merged
  descriptor at that same generation, and every batch for a key in it
  retried against the same descriptor until the budget ran out — the
  same range id and generation on both sides of every attempt, which is
  what the routing trace showed. A split applied after a replica change
  did the reverse, resetting the membership. Every descriptor change is
  now checked at apply against the generation it was built from, like
  the merge's: a stale split is refused and its caller retries against
  the current descriptor; a stale membership change is refused too — raft
  is not told of it, which is its contract for a rejected configuration
  change — and the proposer sees the refusal rather than taking the
  generation it was waiting for, which the merge also reaches, as
  success. Every refusal counts in
  `datax_descriptor_change_refused_total{op}`, so a range starved of
  splits under sustained churn is visible rather than only logged. A
  refused addition leaves the node it preseeded holding a replica of
  nothing, which used to refuse every later attempt to add that node
  ("bounds changed; replica must be removed and re-added"): a preseed
  now says what the range was before its change, and a receiver holding
  a replica that is not a member of that — and no newer than it, for a
  preseed exactly one change on from it — is holding a leftover, which
  it replaces unless the leftover's span overlaps a replica the store
  still serves. `TestSplitBuiltBeforeAMergeIsRefused`,
  `TestReplicaChangeBuiltBeforeAMergeIsRefused`,
  `TestSplitBuiltBeforeAReplicaChangeIsRefused` and
  `TestLeftoverOverlappingALiveReplicaIsNotReplaced` hold each
  operation between building and proposing, change the range underneath
  it, and check the range stays whole, reachable and re-splittable, and
  that a leftover over a live replica is left alone.
- **A snapshot the receiver refuses is reported with the receiver's
  reason, not `EOF`** (found by #200's overlap test under the race
  detector). A receiver that refuses a snapshot at its header — a
  leftover over a live replica, an install below the applied index —
  ends the stream while the sender is still sending, and gRPC then fails
  the sender's next send with a bare end-of-file; the receiver's status
  is only read from the stream afterwards, which the sender did not do.
  Whether the refusal or `EOF` came back depended on which side was
  faster. The sender now reads the status after such a send.
  `TestSendSnapshotReportsTheReceiversRefusal` refuses at the header
  against a sender with two hundred chunks to send, twenty times over.
- **A schema change's drain waits for a statement whose lease entry a
  renewal had already replaced** (#234). A statement is pinned to the
  descriptor it planned against until that cache entry expires, and the
  drain (#185) waited for that expiration by reading it off the entry
  the new version replaced. A renewal between the statement and the
  schema change put a fresh entry at the same version in its place —
  one no statement had taken — so the drain found nothing to wait for
  and could return while the statement could still commit under the
  old descriptor: for `CREATE INDEX`, a row in the table and no entry
  in the index, in the window between the drain and the entry's
  expiration. The handed-out expiration now rides from entry to entry
  across same-version renewals and is published when the version
  changes. The flaky `TestDrainWaitsOutADescriptorHandedToAStatement`
  was this seen from the other side (its sample included such a fresh
  entry); it samples with renewal held off now, and a new test drives
  the renewal by hand and proves the drain outlasts the replaced entry.
- `TestIdleTxnDetail` failed about a third of the time (#228): it
  asserted on the idle transaction's age from the first sample that
  showed the transaction at all, which can be under a millisecond old.
  It now keeps polling until the age is reported.
- `TestAlterColumnType` failed every time it ran alone (#231): its
  concurrent writer counts ids up from 1000 for as long as the ALTER
  takes, and on an idle machine reached the fixed id 5000 the test
  inserted afterwards. The probe ids are now taken past wherever the
  writer got to.
- The health check's rate window is settable (`Config.HealthRateWindow`,
  default five minutes), and the two checks built on it — `auth-failures`
  and `auth-throttled` — are now tested through their whole shape: quiet,
  quiet just under the threshold, raised over it, and cleared once the
  pressure stops and the window rolls past it (#223, #224). That last
  step is the reason both report a rate rather than a lifetime total, and
  it had been asserted only in a comment.
- **Console: a computed severity is always shown** (#219). The
  stylesheet colours the status dot and nothing else, so a status span
  without a dot rendered as plain text — the clock-offset cell of a node
  past `--max-offset` and the quorum-risk cell of a failure domain, the
  two most consequential numbers on their views, looked exactly like
  healthy ones. Every judged number now goes through `warn` (the text
  cue that survives colour loss) and every state word through a new
  `tok` (the dot), and a console test refuses a status span that is
  neither. The round-trip matrix is no longer banded by absolute
  thresholds that painted a healthy WAN link red: a pair is flagged
  against the cluster's own median (over three times it, and over 5 ms),
  as a warning, never as "down", which in that column means unreachable.
  p99 is a column of the worst-pairs table rather than hover-only, and
  the glossary says what is flagged and why.
- **Console: a header control's glossary entry is reachable from the
  control** (#230). The help pop answered clicks only inside the views
  and the help panel was built from the current view alone, so the
  entries for scope, range, jump to, theme and timestamps — written and
  shipped — could not be reached from the controls they describe; the
  short `title` was all a reader got. Each control now keys its entry
  with `data-help`, the pop answers in the header, and the panel ends
  with the header's controls, the same on every view. The test's list
  of entries excused from matching the page shrank by those five, as
  the issue asked; what remains on it is prose terms and the copy
  buttons, not controls.
- **Console: tile sparklines are readings** (#218). They were drawn in
  node 1's series colour, stretched so the stroke thinned with the
  tile's width, with no baseline, no marker for which end is now, and
  each scaled to its own peak without saying so — two tiles moving
  40→44 and 4,000→4,400 drew the same line. Each now has a baseline at
  zero and a dot at the current value, the trend in the de-emphasis
  grey and the dot in the text colour, non-scaling strokes, and a title
  that says the line is the shape of the recent trend scaled to its own
  peak. A signed delta over the named window ("▲ +12% over 15 min")
  carries the magnitude the line does not. Tile figures are set in
  proportional numerals with a reserved width, so they no longer look
  loose and still do not jitter.
- **Console: chart annotations are their own kind of mark, and their
  text has a path that is not a hover** (#217). A mark was drawn in the
  colour of the node that served the event — on a chart plotting that
  node, one hue with two meanings — and its text lived only in a
  native tooltip on an 8-pixel target under the plot's own hit area, so
  it was reachable by nothing. Marks are now one neutral event colour
  with the node named in the label; the plot's hit rect resolves the
  nearest mark to the pointer, so marks a few pixels apart (a rolling
  restart) are each reachable; the crosshair readout carries the mark
  and is anchored under the crosshair for a reader on the keyboard,
  where the plot is focusable and the arrow keys walk the samples; each
  mark is focusable and reads the same on focus; and the chart's "as a
  table" view lists the marked events.
- **Console: lines that cannot be told apart are not drawn in one
  frame** (#216). The eight series colours fail all-pairs separation on
  both themes (down to ΔE 1.6 under deuteranopia for two of them), and
  any subset of node ids can share a chart since ids are never reused.
  No eight-hue ramp on these surfaces passes, so the fix is structural:
  past four nodes, or when a node beyond n8 would share a frame, the
  metrics view facets into one chart per node and says why; lines that
  do share a frame carry their node's name at the line end, so identity
  has a channel besides colour; and `--series-6`, the one slot that was
  byte-identical in both themes, is re-stepped for dark. A node's colour
  still follows its id, so filtering one out repaints nothing.
- The `CREATE TABLE` path assigned `datax_metrics`'s reserved descriptor
  ID to *any* system table by name. With one system table that was
  correct; with two it would have created the second one on top of the
  first. It now looks the ID up per table.
- The header's staleness pill read "last updated 5s ago ago".
- `IsSystemTableID` listed the reserved ids again instead of reading the
  registration map — the same shape as the `CREATE TABLE` bug above,
  left in the one place nothing had needed yet. The console review of the combined PR
  found the pair the issue was filed for still on screen: `--series-8`
  and `--series-2` were both warm reds at an OKLab distance of 7 in
  either theme, and faceting never engages for two lines. `--series-8`
  is re-stepped — a deep red on the light theme, a sky blue on the dark
  one, no shared family clearing the floors in both — so every pair
  involving it is at least 15 apart, and a test computes the distances
  rather than trusting the numbers.
- **`DROP OWNED BY r` takes `r` out of row-security policies** (#312). It
  left the policies naming the role alone, so the `DROP ROLE` that
  follows it was still refused (`2BP01`) until each policy was altered or
  dropped by hand. As in PostgreSQL, the role is now removed from every
  policy's `TO` list, and a policy that named only it is dropped (an
  empty list would mean every role). Each such drop is audit-logged as
  `policy-ddl`.
- **Backups did not hold enum types** (#313). A backup carried the tables
  whose columns are of an enum type, with the labels copied into each column,
  but not the `CREATE TYPE` itself: a restore of everything brought the
  columns back with no type behind them (`ALTER TYPE` and `'x'::mood` then
  failed with "type does not exist"). Backups now carry type descriptors and
  their names (manifest field `types`, held to the type key spans like the
  other system records) and a restore of everything brings them back under
  their ids, moving the id generator past them. A table restore no longer
  refuses a table with an enum column whose type the target database lacks:
  it creates the type there from the column's labels (which also works from a
  backup taken before this change) and says so in the summary. Onto a type of
  that name with the same labels it maps the column to it, as before; onto
  one with other labels it is refused, naming both label lists.
- A restore of everything from a plaintext backup did not hold the data files
  to the manifest's record counts and checksums: it reported them afterwards
  and enforced nothing, so a data file swapped for another backup's file of
  the same table restored silently (#313). Every file is now checked against
  its manifest — count, checksum and key order — in the pass made before
  anything is written, as a table restore already did; a mismatch is refused
  with the target left empty.
- A backup that failed part way left the data files of the tables it had
  already written in its destination, with no manifest (#313). The files are
  now written into a hidden `.backup-in-progress-*` directory inside the
  destination and moved into place only when every table has been written; a
  failure removes them. Backup files are created 0600 and a directory the
  backup creates 0700 (they were 0644 and 0755), sealed or not.
- **A restore brought back time-series rows that retention had expired, and
  kept every restored row a retention period too long** (#316). Retention
  expires rows by commit time and leaves no tombstone, so a full backup and
  an incremental taken after rows expired restored them again; and a restore
  writes every row at its own time, from which retention counted. A backup
  now keeps each time-series row's commit time (its manifest has the magic
  `DXBK3`, which older binaries refuse), and a restore of everything, a table
  restore and a point-in-time restore leave out the rows committed at or
  before the chain's end less the table's retention, then write the rest in
  sixteen bands of commit time and record the bands in the descriptor
  (`restored_rows`), so retention expires each restored row when the source
  would have, at most a sixteenth of the retention late. A row backfilled
  with an old timestamp is kept or dropped by its commit time, as the source
  did. A time-series table's files are read once per band. A chain with a
  backup from before has its time-series tables restored as before, and the
  restore's summary says so. Backups record commit times, and restores
  record bands, at cluster protocol version v22. See docs/user/operations.md,
  "Time-series retention and backups".
- **`ALTER TABLE ... PURGE` sent its report without a description of its
  columns over the extended protocol** (#322). Describe answered NoData and
  Execute then sent rows: pgx read one row of no columns, and psql's `\bind`
  failed with "server sent data without prior row description". The same
  held for `SPLIT AT`, `UNSPLIT AT`, `UNSPLIT ALL`, `SHOW DATABASES` and
  `SHOW PLACEMENT`; `SHOW GRANTS ON ROLE` was described with six columns
  and returned three, and `SHOW COLUMNS`, `INDEXES`, `USERS` and `ROLES`
  described their boolean and integer columns as text. Describe now reports
  the columns of every statement that returns rows, from the same list its
  executor uses, and Execute refuses a result whose shape Describe did not
  report rather than send it: with columns Describe said it had none of
  (`XX000`), or of another width (`0A000` "cached plan must not change
  result type", as PostgreSQL answers a `SELECT *` prepared before an
  `ALTER TABLE`). A test walks a statement of every kind the parser knows
  through Parse, Describe, Bind and Execute, and fails for a new kind
  without a sample.
- **`information_schema` listed every database's tables** (#322):
  `information_schema.tables`, `columns`, `views` and `schemata` listed the
  objects of every database in the cluster, so a query for the current
  database's tables (`WHERE table_schema = 'public'`) returned others' too,
  and `pg_type` and `pg_enum` listed every database's enum types. In
  PostgreSQL each database has its own catalogs. Every `information_schema`
  view and every per-database `pg_catalog` table now lists the current
  database's objects, with `table_catalog` its name; the shared catalogs
  (`pg_database`, `pg_roles`, `pg_user`, `pg_auth_members`,
  `pg_tablespace`, `pg_shdescription`) and `pg_stat_activity` list the
  cluster's. What a role sees is otherwise unchanged (#312). Reading
  `information_schema.tables`, `columns` and `views` no longer reads every
  table descriptor in the cluster.
- **`DROP OWNED BY r` and `DROP ROLE r` hung when `r` held default
  privileges** (`ALTER DEFAULT PRIVILEGES ... TO r` or `FOR ROLE r`), over
  pgwire, until the client gave up (#322). Removing the default privileges
  writes the database's descriptor in the statement's transaction, and the
  key service then looked up its table in a transaction of its own, which
  waited on that write. The key service's work inside a statement now reads
  its table in the statement's transaction.
- **`ORDER BY n` resolved select item n by its output name** (#322): over an
  unnamed expression it failed with `42703` (`column "?column?" does not
  exist`, as in `SELECT table_schema || '.' || table_name FROM
  information_schema.tables ORDER BY 1`), and over a column that another
  item's alias also named (`SELECT id AS b, b ... ORDER BY 1`) it sorted by
  the other column. A position now names the item itself — its column,
  aggregate or expression — and after a set operation the result's column
  n, whatever the last member calls it. `SELECT * ... ORDER BY n` works.
  `GROUP BY n` is accepted for a select item that is a column.

## 0.57.0 — unreleased

### Added
- **Getting things out of the console** (#205). The console is read-only,
  and nearly everything a reader wants from it they want somewhere else:
  a range key into `datax debug split`, an address into a psql connection
  string or a `--join` flag, a statement into a ticket or an `EXPLAIN`.
  One block on one page — the reconstructed DDL from #194 — had a copy
  button; everything else was select-and-drag, which for a long
  monospace key inside a horizontal scroller is the most annoying thing
  on the page. Keyed rendering (#148) had already fixed the blocking
  half of this by letting a selection survive a poll, which left the
  console merely inconsistent about it.

  There is now a copy control on the range keys (`#/data`, the node view
  and the overview's hot ranges), the node and SQL addresses, the
  statement text in every slow-statement list, and the statement shapes
  and their fingerprints. It appears on hover and on keyboard focus so a
  dense table is not a field of icons, and it is a real `<button>` in the
  tab order (#149) with a screen-reader label naming what it copies —
  "copy" repeated down a column says nothing about which row you are on.
  On a device with no pointer it is simply always shown.

  The DDL button's clipboard-with-a-fallback is now `copyText` in
  `js/10-core.js`, shared by every control rather than reimplemented. The
  clipboard API needs a secure context, and this console is often reached
  over plain HTTP — a node addressed directly, a port-forward — where it
  rejects; there the text is selected instead and the control says so,
  which leaves one keystroke to finish rather than a button that
  silently does nothing. The reply goes to a live region, so it is
  announced once instead of the control renaming itself under a screen
  reader's cursor.

- **Tables copy as CSV** (#205). The node table, the cluster range list,
  the failure-domain breakdown and the statement-shape list end up in
  incident reviews, capacity plans and tickets, and the only route out
  was a screenshot or retyping. Each section's heading now carries a
  **copy as CSV** control.

  It copies rather than downloads: the console is self-contained and
  often reached over a port-forward, so a clipboard copy needs no
  download plumbing, no `Content-Disposition` and no new endpoint. For a
  file rather than a paste, `/api/*` already returns the same figures as
  JSON that `curl` can take, and the glossary entry says so rather than
  the console growing a second path to the same bytes.

  What is exported is what is on screen — the filter, the sort and the
  scope already applied, because a CSV that ignores the filter the reader
  set is a different table from the one they are looking at. Keys are
  exported in full where the cells shorten them to fit a column: a key
  that has been shortened is not one a command will take. Fields holding
  a comma, a quote, a newline or an edge space are quoted, which
  statement text and range keys all manage.

### Changed
- `statusCell` is now `nodeState` plus its dot, so the word in an
  exported row and the word in the cell beside it cannot drift apart.
- A tile's value classifier no longer counts screen-reader-only text
  toward the length that demotes a long value to smaller type, so a copy
  control's hidden label cannot shrink a short figure.

## 0.56.0 — unreleased

### Added
- **Authentication refusals reach the console** (#203). #195 gave the node
  a limiter that refuses a sign-in before it spends CPU verifying a
  password, and a counter to go with it — but nothing showed the counter,
  so the console displayed two of the three authentication figures and
  omitted the one that means something is hammering the node right now.

  `/api/security` carries the total and both causes, ungated: they are
  counts of this node's own refusals and name nobody. *Which* addresses
  were throttled would name people and is deliberately still not
  published.

  The Security view gains a **refused before verification** tile, with its
  glossary entry. A new `auth-throttled` health check reports sustained
  throttling to the problems panel as a **rate** over the last five
  minutes, not a lifetime total — a cumulative count would leave the panel
  amber forever after one bad afternoon, which is how a panel stops being
  read.

### Changed
- **`datax_auth_throttled_total` is now labelled by cause** (#203), where
  it was an unlabelled counter in 0.55.0: `rate-limit` (one source, or one
  source and account, asking too often) and `verify-full` (this node
  already verifying as many passwords at once as it allows). They are
  counted apart because they call for different actions — the first is
  someone else's client to fix, the second is this node at its own
  ceiling.

  This changes the series' shape. A query selecting the bare metric now
  returns one series per cause where it returned a single series, so an
  existing alert, recording rule or single-stat panel needs
  `sum(datax_auth_throttled_total)` (or `sum by (cause) (…)` to keep the
  split) to read as it did before. Both children are created at
  registration, so the series is still present at `0` from the first
  scrape rather than appearing only once a refusal happens — a labelled
  counter otherwise turns an alert on an idle node from `0` into
  no-data.

## 0.55.0 — unreleased

Everything under #189: six defects the console review turned up, the
operations that never recorded a duration, and the one view that was
moved rather than rebuilt.

### Added
- **A table detail view** (#194). `#/schema` was moved to its own route
  under #144 and otherwise left as it was: one row per table across nine
  columns, with everything below that in `title` attributes. A tooltip
  does not appear on touch, is awkward to reach from a keyboard, is
  unreadable for a forty-column table and cannot be copied — and this is
  the one view whose audience is not the operator but the application
  developer, who opens it to find out whether their index finished
  building, whether the statistics are fresh enough for the planner to
  pick it, and who is granted what.

  The list keeps its job as an index and gains the level below it:
  `#/schema/<table>`, reached by clicking a row or focusing it and
  pressing Enter, and by the jump box, which now goes to the table rather
  than to the list filtered to it. The detail shows, as content: the
  columns with type, nullability, default and hidden flag (hidden shown
  as such — a time-series table's shard column leading the primary key is
  confusing precisely because it is invisible); the indexes with their
  build state, where `write-only` reads as "building" and says that the
  backfill publishes no progress; the constraints; the statistics with
  what their age means for the planner and the `ANALYZE` that refreshes
  them; the range footprint with a link into `#/data` filtered to the
  table; the grants; the statement shapes that name this table, joined
  against the `/api/statements` document `#/sql` already fetches; and the
  reconstructed DDL with a copy button.

  `/api/schema` gained the three fields the DDL cannot be rebuilt
  without — column defaults, constraints, and the table comment — with a
  foreign key's referenced table and columns resolved to names rather
  than IDs. Glossary entries for all fifteen new terms ship with it.
- **Operations that have a duration are now recorded as pairs** (#192).
  Four operations recorded start/end pairs; the rest were instants, so
  the operations timeline could not say how long a consistency sweep,
  a re-encryption pass or a decommission actually took. The ones with a
  duration now open and close a record, and the two cases deliberately
  left as instants — garbage collection with log truncation, and
  split/merge/rebalance — say so in `pkg/util/events/events.go` and in
  the glossary, rather than looking like an oversight.

### Fixed
- **Authentication attempts are throttled** (#195). Every unauthenticated
  attempt cost the node a SCRAM verification — 0.72 ms of CPU — with
  nothing bounding the rate, so an attacker who cannot guess a password
  could still spend the node's processor at will. A limiter keyed on the
  source address, and on the (source, account) pair, now refuses past a
  burst; a successful sign-in refunds what it spent, so a legitimate user
  sharing a source with a guessing loop is not locked out by it. Keying
  on the account alone would itself be a denial of service against a
  known username, and queuing rather than refusing would hold the
  goroutine the limiter exists to protect.
- **A node without the cluster's authentication secret refuses rather
  than answering with a stand-in salt** (#196). The per-process fallback
  salt differed from every other node's, so the same user got different
  salts from different nodes — which is user enumeration by another
  route. The node now refuses password authentication uniformly, for
  every user rather than only unknown ones, so there is no oracle: an
  availability trade on a node that is already degraded, and the client
  is told to try another node.
- **A running operation survives its start record leaving the ring**
  (#190). The event ring is 500 records shared with every split, merge,
  rebalance, lease shed, snapshot and audit record on the node — minutes
  of real work, well short of a decommission or a backup. An operation
  whose start record aged out stopped being reported as running, so "In
  flight" showed an idle cluster in the middle of moving every replica
  off a node. The ring now keeps a small map of what is open, keyed by
  (kind, op), so a long operation keeps its true start time and its true
  elapsed.
- **`/api/schema` shows a non-admin the tables it can actually read**
  (#197). Visibility was a direct-grant lookup, which missed a grant to
  `public`, a grant to a role the user is a member of, ownership (which
  is not a grant row at all) and the `read_all`/`write_all` roles. It
  failed closed, so nothing was disclosed — but in the common deployment
  where access is granted through roles, most non-admins saw no tables at
  all, and the console disagreed with `SHOW TABLES` about the same user's
  schema. Both now ask the same question.
- **EXPLAIN from the console plans as the operator who asked** (#193),
  not as the system session — so a plan reflects the privileges and
  session settings of the person reading it.
- **`tile()` escapes its value again** (#191). A refactor left the tile
  helper rendering its value as HTML with seven call sites passing
  unescaped strings. `tile()` escapes; a caller that really is composing
  markup calls `tileHTML()` and says so.

## 0.54.1 — unreleased

### Fixed
- **`CREATE INDEX` could finish with a row in the table and no entry in
  the index** (#185). A silent correctness bug: a query planned onto the
  new index would miss a row that exists.

  An online index build publishes the index write-only, waits until every
  gateway's descriptor lease has adopted it, and only then takes the
  boundary timestamp its backfill scans at — everything committed after
  the boundary is supposed to be the work of writers that maintain the
  index themselves.

  The drain was asking the wrong question. It waited for every gateway to
  have *adopted* the new version, which is not the same as no gateway
  still *using* the old one. A statement is pinned to the descriptor it
  planned against, and one served from a gateway's lease cache never read
  that descriptor inside its own transaction — so nothing but its commit
  deadline, the cache entry's expiration, stops it committing later. The
  renewal loop publishes the new version within a third of a TTL, which
  ended the drain while a statement holding the pre-index descriptor
  still had two thirds of a TTL to run. Its insert committed above the
  backfill boundary, maintaining no index, and the backfill had already
  passed.

  A gateway now publishes, alongside the version it has adopted, the
  expiration of the entry it handed out for the version before it, and
  the drain waits for that too. Since a statement's commit deadline is
  exactly that expiration, once it passes nothing anywhere can still
  commit against the old schema. Measured on the reported test: 6
  failures in 250 runs before, 0 in 250 after, and the rows that went
  missing were always ones the concurrent gateway wrote — never ones the
  backfill scanned.

  The cost is that a schema change waits out the superseded entry, up to
  one TTL, whenever a gateway served that descriptor from cache within
  the last renewal interval. Draining is what makes an online schema
  change safe; this was the part of it that was missing.

  `TestDrainWaitsOutADescriptorHandedToAStatement` pins the rule and
  fails on the old code every run rather than one in forty, which is what
  the race was worth as a test before. The count mismatch in
  `TestOnlineCreateIndexUnderConcurrentWrites` now also names the rows
  that have no entry — whether they are rows the backfill scanned or rows
  a concurrent gateway wrote is the whole diagnosis, and it was the first
  thing this needed and did not have.

  The longer drain also exposed a test that was relying on timing:
  `TestDatabases` read `/api/schema` immediately after creating a table
  and expected to see it. That document is rebuilt at most once every
  five seconds by design, so the browser was never a live view of the
  catalog; the test now polls for the table instead of assuming it. No
  product behaviour changed there.

- **Load splits were biased toward whatever was sampled last** (#186).
  A range that is hot but not large splits at a sampled key chosen to
  divide its traffic evenly. The chooser ranked candidates by the raw
  difference between the traffic seen either side of each — but a sample
  counts only what arrives after it enters the reservoir, and reservoir
  sampling replaces slots throughout the window, so two candidates can
  have observed amounts that differ by three orders of magnitude.

  A slot filled near the end of the window might hold `left=3, right=2`:
  an imbalance of one, unbeatable by any well-observed key, on five
  observations of a key carrying a fifth of the traffic. The guard
  against a fully one-sided sample did not catch it — a handful of
  requests either side is enough to pass it.

  Candidates are now ranked by the *share* of traffic either side rather
  than the difference, which is what makes samples with different
  observation counts comparable at all, and a sample must have seen a
  hundred requests before its balance is believed. A hundred holds the
  standard error of the observed share at or below five points, enough to
  tell a key near the traffic median from one at 80/20. Below it the
  caller falls back to the byte midpoint, which is honest about knowing
  nothing rather than acting on a share measured from almost nothing.

  A range only reaches the chooser after a full window sustained above
  the load threshold — thousands of requests at the default — so nothing
  is excluded in ordinary operation.

  `TestChooseSplitKeyBalances` now scores the chosen key against the
  distribution that was actually sent instead of asserting a prefix
  letter. That is the property the code is for, and it turned out the old
  assertion was not only flaky but too weak: on the old chooser the
  strengthened test also fails on splits that put 70% or 30% of the
  traffic on one side, which the letter check accepted.

## 0.54.0 — unreleased

### Added
- **Everything on the page explains itself.** The console was full of
  words that are precise and opaque at the same time — "compaction
  debt", "bare majority", "40001/s", "stats age", "closed ts". Each is
  exactly right and each is unreadable to anyone who does not already
  know it, and the person who most needs to know is the one opening the
  console during an incident.

  Every label, column heading and section title now carries its own
  explanation, reachable two ways because there are two questions:

  - *What is this one figure?* Terms that have an explanation are
    underlined the way a footnote marker is. Hovering one shows it;
    clicking one opens a note in place.
  - *What am I looking at?* The `?` in the header — or the `?` key —
    opens the glossary for the current view: every term on screen, in
    the order it appears, with its explanation. That is also the
    keyboard and screen reader path, which is why the terms themselves
    are not ninety separate tab stops.

  The entries say what a figure measures and what makes a reading worth
  acting on, not what its label already says. "40001/s" explains that it
  is the serializable-retry rate and that a steady one is normal while a
  rising one means transactions are fighting over the same rows; "rows
  scanned" explains that far more scanned than returned is a scan where
  an index would do, whether or not the statement is also slow.

  The glossary is keyed by the term as it is written on screen, so a
  column that says "leases" is explained by the entry called "leases" —
  a new column naming a term already in the glossary is explained
  without anyone wiring it up, and one that means something different in
  its view ("rows" is a result size on `#/sql` and an estimated row
  count on `#/schema`) is keyed by view. Terms the page generates rather
  than writes, like a column per node, are matched by pattern.

  Three tests make "everything" true rather than aspirational: one walks
  every heading and tile label the page shows and fails on a term
  nothing explains, one fails on an entry too short to add anything the
  label does not already say, and one reports an entry that has stopped
  matching anything on the page rather than leaving it to rot. The first
  of them found six terms the browser check could not see, because they
  live in a collapsed disclosure.

### Fixed
- **A tile's breakdown ran on after its figure in three more places** —
  the connections and worst-p99 tiles on `#/sql`, and connections,
  statements, 40001 and plan cache on the node page. Same fault as the
  cluster tiles below, same fix.

## 0.53.2 — unreleased

### Fixed
- **The node page always asked for node 0.** The view's cache object
  declared `id: 0` and nothing ever assigned the route's node to it, so
  every visit fetched `/api/node?id=0` — which the server is right to
  reject with a 400 — and the page rendered "Node n0" above the error.
  Opening a node from the nodes table therefore always led to a broken
  page. The route is now the single source of truth for which node the
  view is showing, and the cache only carries state between polls; a
  route with no node says so in words instead of asking for node 0.

- **Tile values overran their cards and pushed into what was below
  them.** A tile is a label, one figure and sometimes a qualifier, but
  the qualifier ran on after the figure inside the same 22px line:
  "connections" read `0 (0 active, 0 idle in txn) (0 of 2 live nodes)`
  as four lines of headline type, taller than its card, and — because a
  grid row is as tall as its tallest member — it stretched every other
  tile beside it into a mostly empty box.

  A qualifier is now a 12px line beneath the figure, and a value that is
  a phrase rather than a figure ("v0.53.1 · protocol v2 · cluster v16")
  is set to fit its card. `tile()` classifies by length rather than each
  caller remembering to, so a value that only grows long in the field is
  handled too.

- **Every view with a chart scrolled sideways below 460px.** The chart
  grid asked for `minmax(460px, 1fr)` tracks, and a track wider than the
  viewport widens the document rather than shrinking. The tile, chart
  and series-picker grids now cap their ideal track width at the
  container's, so nothing forces a horizontal scroll at any width.

- **The overview poll threw during a rolling upgrade.** It revealed a
  reload prompt by id when the node started serving a newer console than
  the tab was running, and the element was not in the markup: the poll
  threw on the null, and the console froze at "last updated never"
  exactly while the cluster was changing under it. The element exists
  now, the lookup is guarded, and a new test walks every
  `getElementById` in every script and fails on one the page cannot
  satisfy. That test also found dead code in the schema view that
  reached for a container removed earlier and would have clobbered the
  security view's roles table.

- **The node page rendered four of its sections twice.** SQL, Network,
  Settings and Recent events were duplicated verbatim in the markup,
  ids and all, so the second copy could never be filled and the page
  showed each section once with content and once empty. Also: a node
  that has not collected a GC percentile yet reports "—" rather than
  "NaN ms".

## 0.53.1 — unreleased

### Fixed
- **The console was unusable: the jump-to dialog covered the page from
  first paint.** Its markup carries the `hidden` attribute and every
  script that shows and hides it reads `.hidden`, but the stylesheet said
  `#jump { display: flex }` — and an id selector outranks the user
  agent's `[hidden] { display: none }`. The attribute did nothing, so a
  `position: fixed; inset: 0` overlay sat on top of the console
  swallowing every click while the code believed it was hidden. Pressing
  Escape and Enter appeared to toggle it without ever dismissing it.

  Fixed at the root: `[hidden] { display: none !important }` makes the
  attribute authoritative for every element that uses it, rather than
  patching the one selector that happened to outrank it.

  Two smaller faults in the same dialog, both of which made it feel
  undismissable:

  - Escape only closed it while the keyboard was inside its input; it now
    closes from anywhere.
  - Closing returned the keyboard to the button that opens it, so the
    next Enter reopened it. It now returns focus to wherever the reader
    was before it opened.

  A regression test asserts the override is present and fails without
  it — this was invisible to every existing test, because the server
  serves the same bytes either way and a DOM assertion on an element's
  text passes whether or not something is covering it.

## 0.53.0 — unreleased

### Added
- **Statement fingerprints: the shapes that cost the most** (#157). The
  console could show what was running now and what was slow recently,
  per node. Neither answered the question that drives optimisation work:
  which statement shape costs this cluster the most. A statement taking
  8 ms that runs forty thousand times an hour never enters a
  slow-statement ring, and is usually the thing worth fixing.

  Executions are now grouped by shape and counted: how often, how long in
  total, how long typically, how many rows returned and — the figure a
  cluster-wide counter could never attribute — how many rows *read*. Far
  more scanned than returned is a full scan, and it is now visible
  whether or not it is also slow.

  `#/sql` lists the shapes ranked by total time, sortable by executions,
  mean latency and rows scanned, each expanding to its per-node
  statistics, the tables it touches, a representative statement, and an
  `EXPLAIN` of it — closing the loop from "this is expensive" to "here is
  why". The plan is described, never run: `EXPLAIN`, not `EXPLAIN
  ANALYZE`, and the text comes from the node's own accounting rather than
  from the request, so the endpoint cannot run a statement of the
  caller's choosing.

  The normalisation walks the parsed statement, not the text it came
  from. That is the difference between correct and approximately correct:
  a lexer cannot tell a literal from an identifier that looks like one,
  cannot see through a comment or a cast, and cannot tell that two texts
  differing in whitespace, keyword case and parenthesisation are one
  query.

  Per-node tables are bounded by an LRU with an explicit cap — a client
  that inlines its parameters can generate unbounded shapes, and an
  accounting table that grows with what clients send is a leak with a
  nice name. What is dropped is counted, and the view says the list is a
  window over the busiest shapes rather than every shape that ever ran.

  Only summable figures are summed across nodes. There is no p99 of two
  p99s, so percentiles stay with the node that measured them.

  `/api/statements` and `/api/explain` need the admin role, alongside the
  rest of the statement surface: a representative statement can carry
  data.

### Changed
- The retry hot list from #154 now groups by the same AST-derived shape
  as the statement list, so the two panels on `#/sql` cannot disagree
  about what one shape is. The lexical normaliser it shipped with is
  gone (#157).

## 0.52.0 — unreleased

### Added
- **Certificate expiry, reported three ways** (#156). `--certs-dir`
  turns on mutual internode TLS and SQL TLS, and nothing said when any
  of it was about to stop working — although the dates were sitting in
  certificates the node had already parsed. Silently expiring node
  certificates are a well-known way to lose a cluster.

  `/metrics` now carries `datax_cert_expiry_seconds{kind,subject}`, so
  an alert fires whether or not anyone opens the console; the health
  checks warn thirty days out and turn critical at seven, naming what
  the lapse would cost rather than only that it is coming; and the
  console's `#/security` view lists the material with its dates, soonest
  first. An insecure cluster publishes no series rather than one
  asserting that nothing expires.

- **A capacity forecast** (#156). Free space was a point reading — amber
  under 15%, red under 5% — and by the time it was amber the question
  was "how long have I got", which nothing could answer.

  The recorded `store.disk_free` series is now fitted per store and the
  nodes table gains a **fills in** column beside the free-space figure,
  with the growth rate and the fit's quality on hover. A store on course
  to fill within a fortnight becomes a health problem and within three
  days a critical one.

  A forecast is only offered where the series has one to give: a flat or
  rising series says "not filling", a series too short or too noisy to
  fit says which, and neither is dressed up as a number. The fit is
  cached and refreshed in the background, so a console poll never waits
  on a day of samples.

- **A security view worth the name** (#156). Security state was
  scattered: audit records rode the general event ring behind an amber
  outline, the signed-in principal was in the header, and users were a
  small table appended to the schema section. Nothing answered "who can
  reach this cluster and how".

  `#/security` now reads from a new `/api/security`: the certificates
  and their expiry, every role with what it effectively holds through
  membership (built-in roles included, and NOINHERIT marked as what it
  is), who is connected and by which method — client certificate, SCRAM,
  or trusted in insecure mode — this node's authentication failures and
  denied admin operations, and the store's encryption state with any
  re-encryption still to do.

  The gate follows the data rather than the panel: certificate expiry
  and role membership are operational and open to any authenticated
  user, while the per-user connection breakdown and the client
  certificates this node has been shown name people, and travel with the
  audit records behind the admin gate.

### Fixed
- The console's users table listed logins and an admin flag; it now
  lists roles with their membership resolved, so "why is this an admin"
  is answerable without walking the graph by hand (#156).

## 0.51.0 — unreleased

### Added
- **Transactions and contention on the console's `#/sql` view** (#154).
  A serializable database lives and dies on retries, and the console had
  no transactions section: `txn.commits`, `txn.aborts`, `txn.retries` and
  `kv.batch_p99_us` were recorded every interval and reachable only by
  knowing to pick them out of the metrics picker. The one
  transaction-shaped figure on the page was a `40001/s` column.

  The view now opens the four series over the header's time range, one
  line per node, with tiles for the cluster's commit, abort and retry
  rates and the share of work being redone. KV batch latency is charted
  beside statement latency, which is what tells a slow statement from a
  slow cluster; the two latency tiles name the worst node rather than
  averaging, because there is no p99 of two p99s.

  A rate does not say what to change, so two panels say who and what.
  **The retry hot list** attributes every serialization failure to the
  statement shape that produced it — the statement with its literals
  replaced, so a retry storm is one row rather than a thousand — and to
  the user who ran it. **Idle in transaction** lists the sessions holding
  an open transaction with the user, the client, the application, how
  long they have been idle, how long the block has been open, and the
  statement they last ran: "oldest idle transaction: 3m" named nobody to
  talk to.

  The split follows the data, not the panel: rates are not sensitive and
  need no role, while statement shapes and sessions carry data and stay
  behind the admin gate that already covers `/api/activity`. Both new
  panels are one node's own counts and say so; nothing here is summed
  across the cluster.

  The shape table is bounded, and failures past the bound are counted in
  an overflow total rather than dropped or allowed to grow the map
  without limit.

### Fixed
- Charts drawn outside the metrics view no longer carry that view's
  event marks. The marks come from a fetch `#/metrics` makes for its own
  window, so a node page or the new transactions charts could show
  whichever window that view last looked at. Annotation is now something
  a chart asks for (#155 follow-up).

## 0.50.0 — unreleased

### Added
- **An operations timeline** on the console's `#/ops` view (#153). The
  event ring recorded instants, so a decommission that started twenty
  minutes ago and one that finished last week read identically: a
  timestamp, a kind, a summary. A rebalance storm looked like a quiet
  afternoon with a lot of old rows in it.

  An event may now carry an operation id, a phase and an outcome, and
  four long-running operations record both ends under one id — backup,
  restore, re-encryption sweeps and decommission drains. The view splits
  them into **in flight**, with how long each has been running, and
  **recently completed**, with its outcome and duration. Progress that
  cannot be measured is shown as elapsed time, never as a bar with a
  number nobody measured.

  The ring stays the audit trail it is: the pairing is a label on the
  records and the view is derived from it, not a job store. An operation
  whose start has already aged out of the ring is still listed, with no
  duration claimed for it.
- **Cluster events marked on the metrics charts** (#155). "Why did p99
  jump at 14:20" was a two-window investigation — read the chart on
  `#/metrics`, scroll the event log on `#/ops`, match timestamps by eye —
  although both sets are on the same page and the same clock.

  Charts now carry a tick per event in the window they draw, hovering to
  the kind, time and summary, drawn beneath the data lines so a mark
  never obscures the line it explains. Structural kinds (splits, merges,
  rebalances, repairs, decommissions, restarts, upgrades) are marked by
  default, with a control for every kind or none: a layer that draws
  three hundred marks is noise.

  `/api/events` grows a time window (`?from=`) and answers with the
  timestamp of the oldest record the ring still holds. A chart drawing a
  seven-day window over a ring covering two hours says so, rather than
  implying five quiet days.

## 0.49.0 — unreleased

### Added
- **Replication status and a failure-domain view** in the console
  (#152). Rack-aware placement is the product's headline claim and the
  console could not show whether it was holding: localities were a text
  column, and the health checks reported one example range each.

  `#/data` now opens with every range bucketed by replication state —
  healthy, under-replicated, over-replicated, no quorum, undiverse —
  each bucket expanding to its ranges. A range is measured against its
  own target replica count, so a database carrying a placement policy
  (#176) is judged by that policy rather than the cluster default, and
  counted once in its worst state.

  Below it, a **failure domain** table: per locality tier value, the
  nodes, replicas and leases it holds, and what losing the whole domain
  would cost — the ranges that would drop below a majority, and the
  ranges that would survive with no margin left. That is the question
  asked before every maintenance window, and it was previously
  answerable only by hand from the range list.

  And **range hotspots**, the heaviest and largest ranges each node
  advertises in its heartbeat. QPS there is the leader's own rate over
  the ranges it leads; it is not summed across nodes, and the view says
  so rather than implying a cluster total.

  The whole section is computed from the range descriptors and the node
  registry the serving node already holds — no fan-out, so it answers
  from a partitioned node like everything else on the page — and it is
  computed on the server rather than in the page, because it is a
  contract every node must agree on: two nodes reading the same
  descriptors bucket them identically, which is what the new
  `testcluster` test holds them to.

  `/api/cluster` gains a `replication` section and per-node
  `hot_ranges` / `big_ranges` (straight from the heartbeats the
  allocator already reads).

## 0.48.1 — unreleased

### Fixed
- A point read could miss a key that is there, on any table written
  before cluster version v15 (#178). `mvccSplit` decided the prefix
  boundary from the key's tail, which is ambiguous for a key datax did
  not write: an index-block separator in a pre-v15 table is a valid
  prefix followed by the *first few bytes* of a timestamp, because
  Pebble's default comparer truncated it with no suffix to respect.
  Reading such a key's whole length as its prefix made that "prefix" a
  strict extension of the prefix of the very keys it separates, so the
  two ordered one way whole and the other way by prefix — and
  `SeekPrefixGE` could then walk a legacy table's index to the wrong
  block and report the key absent.

  `Split` now finds the boundary by searching for the first `0x00 0x01`
  from the left. The escaping guarantees that is the terminator (a
  `0x00` in the user key becomes `0x00 0xff`), and it makes the prefixes
  prefix-free, which is the property `Compare` needs: a key whose prefix
  extended another's would carry that other terminator and be cut there
  instead. Under `-race` — where Pebble compiles in its assertion
  comparer — the old rule panicked instead of answering wrongly, which
  is how this surfaced; a non-race `go test ./...` never showed it.

  Existing stores need no rewrite and the key schema keeps its name: the
  two rules agree on every key datax writes, and differ only on the
  separators older tables already carry.
- `Comparer.ImmediateSuccessor` no longer loops forever (#178). It
  appended `0x00` until the result was its own prefix, which under the
  corrected `Split` never happens — a terminated prefix has no prefix-key
  extensions, because its own terminator stays the first one. The
  smallest prefix key above `esc(K) 0x00 0x01` is `esc(K) 0x00 0x02`, so
  it increments the terminator rather than extending the key. Pebble asks
  only for "the smallest prefix key larger than a"; nothing relied on the
  successor extending its prefix.

## 0.48.0 — unreleased

**Cluster protocol v16.** A database descriptor may carry a replica
placement policy. A v15 node reads the descriptor but not the policy, so
it would allocate replicas anywhere and undo the placement a v16 node
just made; the DDL that writes a policy is refused until the cluster
finalizes v16 with `datax debug upgrade`. Nothing else changes: a
descriptor written before v16 reads the same at either version.

### Added
- **Region-restricted replication** (#176). A database can say where its
  replicas may live and how many there are, and the allocator honours it
  for every range of that database's tables:

  ```sql
  CREATE DATABASE eu WITH (replicas = 3, constraints = ('region=eu-west-1', 'region=eu-central-1'));
  ALTER DATABASE eu SET (constraints = ('region=eu-west-1'));
  ALTER DATABASE eu SET (replicas = 5);      -- constraints untouched
  ALTER DATABASE eu SET (constraints = ());  -- lift the restriction
  SHOW PLACEMENT FOR DATABASE eu;
  ```

  `constraints` is a disjunction — a replica may live on any node whose
  locality carries any one of the listed `key=value` tiers — so naming
  two regions means "either of these". `replicas` overrides the cluster
  replication factor for that database alone; it must be odd and at most
  9. An option an `ALTER` does not name is left as it was, so the count
  and the constraints are set independently, and an empty constraint
  list is how a restriction is lifted.

  Up-replication, dead-node repair, decommission drain and both
  rebalancing passes now allocate within the policy, maximizing failure-
  domain diversity inside what the policy admits — a database pinned to
  one region still spreads across that region's racks. A new pass moves
  replicas that a policy no longer admits onto nodes that do, one range
  per tick, which is what makes `ALTER DATABASE ... SET` take effect on
  data that already exists.

  Writing a policy splits its tables into their own ranges and stops
  those ranges merging with neighbours under a different policy: a range
  inherits a policy only when it lies wholly inside one table, since a
  range straddling two tables could belong to two databases asking for
  different things.
- Two health findings and a counter for placement (#176):
  `placement-unsatisfiable` (critical) when no live node satisfies a
  range's policy, `placement-misplaced` (warning) while the allocator is
  still moving replicas home, and
  `datax_placement_replicas_moved_total`. When a policy cannot be met the
  allocator does nothing rather than placing a replica outside a region
  an operator named — the finding is how that is reported.
- `SHOW PLACEMENT [FOR DATABASE name]` reports the replica count the
  allocator will actually use, so a database with no policy of its own
  shows the cluster default and says where the number came from (#176).

## 0.47.0 — unreleased

### Fixed
- `datax sql` could not edit a statement once it spanned lines (#175).
  The shell composed a multi-line statement by calling a single-line
  editor once per line, so by the time the cursor reached column 0 of a
  continuation line the line above was already gone and backspace simply
  stopped. The whole statement is now the editor's buffer: backspace at
  the start of a line joins it to the one above and leaves the cursor at
  the join, forward delete at the end of a line pulls the next one up,
  and the arrows move through the statement rather than within one line
  of it.

### Added
- `Ctrl+←` and `Ctrl+→` move by a word in `datax sql` (#175). They did
  nothing before: the editor decoded `ESC [ 1 ; 3 C/D`, which is
  Alt+arrow, and had no case for `ESC [ 1 ; 5 C/D`, which is what
  Ctrl+arrow sends in every common terminal. `Alt+←`/`Alt+→`, `Alt-b`/
  `Alt-f` and `Ctrl+Alt+arrow` are accepted as the same motion, and a
  word is a SQL identifier part, so `Ctrl+←` through
  `orders.customer_id` stops at each half.
- The shell keeps its history, `Ctrl-A/E/K/U/W`, `Ctrl-L` and `Ctrl-D`,
  and gains `Ctrl-C` to abandon the statement being typed without
  leaving, `Alt+Backspace` and `Ctrl+Delete` for word-wise deletion, and
  bracketed paste — a pasted multi-line statement lands as text instead
  of running itself a line at a time (#175).

## 0.46.0 — unreleased

### Fixed
- The console showed the overview's sections underneath every other
  view, and the node page's own SQL, network, settings and events
  sections on the overview instead of on the node page: 0.44.0 shipped
  two `<main id="view-main">` containers, and the router could only ever
  hide the first (#151).

### Added
- The console is eight views behind one persistent nav, each a real
  route (#151): overview, nodes, data, sql, schema, metrics, ops and
  security, with each node's page at `#/node/N`. Three controls cross
  every view from the header — a **node scope** the node-scoped panels
  (statements, the event ring, audit) read instead of assuming the
  serving node, and which they name rather than leaving it to be
  guessed; a **time range** shared by every chart; and **jump to**
  (`⌘K`), resolving `n3`, `r128`, a table name or `rack=b` to the right
  view. The scope, the range, each view's filter and an open range
  drill-down all live in the URL, so a view is a link worth sharing, and
  each view remembers its scroll position. Health findings now link to
  the view that shows the figure: they used to link to `#sec-*` anchors,
  which the router read as an unknown route and resolved to the
  overview, stopping the other views' polls on the way.
- The console's script is kept as one file per view plus a shared core
  under `pkg/server/ui/js`, assembled into the single page the node
  serves (#151). No build step and no second request: the page stays
  self-contained for airgapped nodes. A missing, empty or misordered
  script file fails the build or a test rather than blanking the
  console.
## 0.45.0 — unreleased

### Added
- The console has a front door (#158). An unauthenticated browser
  navigation now gets a sign-in page instead of the browser's own
  credential dialog: it names the cluster, node, locality and version
  actually reached, says whether the cluster is secure, and explains
  that the credentials are database credentials. `POST /api/login`
  verifies them through the same path HTTP Basic uses and sets a session
  cookie (`HttpOnly`, `Secure`, `SameSite=Strict`, 12 hours);
  `POST /api/logout` and the console's new user menu — who you are, how
  you got in, which roles you hold — clear it. The cookie is a signed
  token carrying no password and needing no session store, keyed from
  the cluster's authentication secret, so any node accepts a token any
  other minted; roles are still resolved per request, so revoking
  `LOGIN` or admin takes effect at once rather than at expiry. Sign-ins,
  sign-outs and every refused or lapsed token are audited. Machine
  clients are untouched: `curl`, Prometheus and `datax debug` still get
  the `WWW-Authenticate` challenge (the doors are told apart by
  `Accept`, never by user agent), and HTTP Basic and client certificates
  authenticate every route exactly as before.
- The console states insecure mode across the top of the page rather
  than in a grey aside: a cluster that authenticates nobody says so
  (#158).

## 0.44.0 — unreleased

The console umbrella (#144), first batch.

### Changed
- Two published JSON names in `/status`, `/api/cluster` and `/api/node`
  (#146): the machine sample's load averages are `load1`, `load5`,
  `load15` (they were the untagged `Load1`, and the other two were not
  exposed), matching the heartbeat's `load1`; and the storage snapshot
  (`storage`, `raft_storage`) is snake_case throughout (`l0_files`,
  `compaction_debt_bytes`, `block_cache_hits`, ...) instead of Go-cased.
  Anything that read the old spellings must move; the console did in the
  same change.

### Added
- The console reports the cluster, not the node that served the page
  (#145). `/api/cluster` carries a `rollup` summed over the live nodes'
  heartbeats — QPS, data, leases, ranges and replicas, connections (by
  state and by user), statements and serialization failures (the page
  differences polls for rates), the worst p99 and the node that owns it
  — with the count of contributing nodes beside each sum, so a node that
  is down shows as a smaller count rather than a smaller figure; the same
  numbers come out whichever node is asked (`TestClusterRollup`). The
  overview's tiles are the rollup, the node table is a strip of cards
  (status, locality, cpu, load, memory, disk headroom, fds, leases, data,
  qps, connections) linking to each node's page, and the sections that
  described the serving node — its replicas, events, machine and storage
  health — live only on that page now; the header says which node served
  the page, as provenance. Health findings about the serving node's
  storage or events link to its page.
- `GET /api/overview` (#147): the cluster document, the health problems
  and the tail of the event ring in one document — what the overview
  draws, one request per poll instead of one per section — with a
  per-section `errors` map, so a section that cannot be produced is
  absent and named rather than failing the request; the individual
  endpoints stay. The console runs every poll through one scheduler:
  each view starts only the fetches it shows (the Metrics view fetches
  no schema and no statements), a hidden tab polls nothing and fetches
  everything once when shown, a failing fetch backs off exponentially
  (capped at a minute), and the header says how long since the last good
  update and when the next try is.
- The console's favicon takes the colour of the worst open health
  problem and the title carries the count — `(2) datax — n1` — so a
  cluster gone critical shows in the tab strip and the window switcher
  without switching to it; steady, never animated (#150).
- The console is usable on a keyboard, with a screen reader and on a
  phone (#149): a skip link, a visible focus ring on every control,
  range rows as real controls (Tab, Enter or Space), captions and column
  scope on every table, the problems panel as a live region, a text cue
  beside every colour-coded figure, a fluid container instead of a fixed
  width, and below 640 px the wide tables become one card per row and the
  network matrix a worst-pairs list.
- `datax_node_load5` and `datax_node_load15` on `/metrics`, beside
  `datax_node_load1` (#146).
- The console notices a rolling upgrade under a long-lived tab (#146):
  the page and every `/api/cluster` document carry `console_version`, a
  digest of the page the node embeds, and the tab offers a reload when
  they differ; the page is served with that digest as its `ETag` and
  `Cache-Control: no-cache`, so a reload after an upgrade fetches the
  new page and one before it is a 304.

### Fixed
- The console no longer destroys the page every three seconds (#148):
  a render replaces an element's markup only when it changed and never
  while it holds the text selection, the focused element or an open
  disclosure (the new markup lands when the interaction ends), and the
  tables keep their rows by identity across polls, so a range key can be
  selected and copied, an open drill-down or disclosure stays open and
  keyboard focus survives a refresh; the range drill-down's cached-HTML
  workaround is gone.
- The console's SQL statements panel explains a 403 from
  `/api/activity` the way the range drill-down does — the signed-in user
  and the `GRANT` that fixes it — instead of hiding (#146).
- The heartbeat's machine summary is checked to be a projection of the
  node's sample (`TestMachineSummaryProjectsTheSample`): every summary
  field exists in the sample under the same JSON name and is copied, so
  the console reads one shape and a figure cannot exist in only one
  (#146).

## 0.43.0 — unreleased

Cluster version **v15**.

### Added
- Prefix bloom filters (#161). Pebble consults a bloom filter only on
  a prefix seek and only for what the comparer's `Split` returns; with
  no `Split`, the filters #101 configured hashed whole engine keys and
  the MVCC read path — a seek to a key's metadata key, then to a
  version — never asked them. From cluster version v15 a store opens
  with the MVCC comparer, whose O(1) `Split` cuts an engine key at the
  terminator of the user key's encoding (no key-format change), the
  point reads and the write path's intent probe are prefix seeks, and
  a filter rules out the sstables that hold nothing of the key. The
  comparer keeps Pebble's name and ordering; the filter policy and the
  columnar key schema carry new names, so the tables from before read
  as they are (their filters unconsulted, never asked a prefix
  question) and are rewritten in the background by the re-encryption
  pass's machinery (`prefix_bloom_rewrite`, `store_prefix_bloom` in the
  node document; `datax_prefix_bloom_remaining_bytes`,
  `datax_prefix_bloom_rewritten_bytes_total`). The comparer is fixed at
  open, so a node switches at its first restart after the finalize
  (logged); a v14 binary does not know the key schema and the store's
  version gate refuses it. A point read that finds no intent and whose
  newest version is visible now returns it from the first seek.
  Measured on the storage benchmarks (100k rows, 128-byte values, the
  rows compacted to L6 as a store's bulk is): a point read of an absent
  key 3.18 → 1.90 µs, through a reused iterator 2.07 → 0.84 µs (the
  filters exclude 1.98 tables per read); a present key 3.49 → 3.87 µs
  and 2.13 → 2.36 µs — the one filter probe of the table that holds it,
  which is why the filters of L6 are consulted (Pebble's default skips
  them: without them a miss costs 4.2 µs here, more than before). On the
  harness (fresh stores, 20 s, two alternating rounds) `ingest-random`
  328 / 321 → 375 / 365 rows·100/s (+14 %: every batch's uniqueness
  probes are misses), `kv-95-5` 26.5k / 27.2k → 25.6k / 26.3k ops/s
  (−3.5 %: its reads all find their key, and pay the probe), `hot-row`,
  `bank` and `scan` at parity.

### Fixed
- Re-encryption and the table rewrite above seed a file with a
  tombstone so a lone file is rewritten rather than moved; Pebble v2's
  manual compaction no longer flushes the memtable first (v1 did), so
  the seed stayed in memory and single files were moved with their
  retired key. The pass now flushes the seed before compacting.

## 0.42.0 — unreleased

Cluster version **v14**.

### Added
- Pebble's columnar-block sstable format (format major version 19)
  behind cluster version v14 (#166, the first of the gated format
  steps). Finalizing v14 ratchets both of every node's engines online,
  within a heartbeat and with no restart; new sstables — flushes,
  compactions — are written in the format from then on and existing
  ones are read as they are. A fresh store bootstrapped by, or joining,
  a v14 cluster starts there. A v13 binary bundles a Pebble that does
  not know the format, so a store cannot go back to it after the
  finalize (the store's version gate refuses, as for v13). The node
  document reports `store_format`. `TestColumnarBlocksRatchet`.
  Measured: on the storage benchmarks (100k rows, 128-byte values, a
  store built at 19 vs 16) a point read 3.71 → 3.24 µs (hit) and
  3.22 → 2.72 µs (miss), through a reused iterator 2.55 → 2.11 µs
  (miss), a 1,000-row reverse scan 1.35 → 1.07 ms, a forward scan
  unchanged; on the harness (kv-95-5, index-join, scan, hot-row,
  ingest-random; fresh stores, two alternating rounds) every shape is at
  parity — the read path's cost sits above the sstable blocks there.

### Changed
- Intent metadata and transaction records are protobuf-encoded from
  cluster version v14 (#141); JSON before it, where `encoding/json` was
  about 45 % of the intent path. The coordinator flags each transaction
  (`TxnMeta.BinaryMeta`) when the cluster is at v14, the flag rides in
  every command so every replica encodes alike, and readers tell the
  encodings apart by the first byte (`kvpb.UnmarshalTxnRecord`,
  `storage.DecodeMVCCMetadata`, shared by the request path and GC), so
  records from before the finalize stay readable for as long as they
  live — no migration, no rewrite.
  Measured: decoding a record 4.5 → 0.6 µs, encoding 0.85 → 0.65 µs, an
  intent laid down, rewritten and read back 13.9 → 6.2 µs; on the
  harness (two alternating rounds) `hot-row` 310 / 319 → 347 / 349
  ops/s, `bank` p50 4.2 / 4.6 → 3.7 / 4.0 ms with its throughput inside
  its own contention noise, `kv-50-50` and `ingest-random` at parity
  (single-statement writes commit in one phase and lay no intent).

## 0.41.0 — unreleased

### Fixed
- A gateway's descriptor lease is taken in the transaction that read the
  descriptor. Written in a separate transaction, the lease record could
  claim a version a schema change had already superseded: a gateway
  whose previous lease had lapsed read version 1, the change committed
  version 2 and drained (a lapsed lease is nothing to wait for), and the
  gateway then recorded a fresh lease at version 1 and served it from
  its cache — `column does not exist` on the new column — for a whole
  TTL. Found by the race suite; `TestLeaseClaimsTheVersionItRead` holds
  a gateway between its read and its write while the change commits.

### Changed
- The MVCC read path allocates a fraction of what it did (#163). A user
  key's two iterator bounds come out of one allocation (the upper bound
  is the metadata key with its terminator bumped — exactly the end of
  the key's engine keys — instead of encoding the key's successor), the
  version keys a read seeks to are appended onto that buffer's spare
  capacity, `encoding.EncodeBytes` and `DecodeBytes` allocate once at
  the exact size, and a scan copies each row's prefix off the iterator
  rather than re-encoding the decoded key (and no longer copies the
  decoded key a second time). `storage.Getter` serves the point reads
  of one server batch — the read path's Gets and the write path's Gets
  and Increments — through one iterator re-bounded per key, refreshed
  past the batch's own writes where the reader is a batch. Measured on
  the storage benchmarks (100k rows, 128-byte values): a point read
  4.28 → 3.60 µs and 16 → 5 allocations (2.75 µs and 3 through a
  Getter), a miss 3.66 → 3.12 µs (2.63 reused) and 12 → 3 (1); a
  1,000-row scan over 3 versions 735 → 518 µs and 8,019 → 2,016
  allocations, in reverse 1,472 → 1,252 µs.
- A transaction's intent history is bounded to what a savepoint
  rollback could restore (#162). Every same-epoch rewrite of a key
  appended the superseded value to the intent's history and rewrote the
  whole history with it, so a transaction writing one key K times
  stored K copies and wrote O(K²) bytes — for data only
  `ROLLBACK TO SAVEPOINT` reads. The coordinator now tells the servers,
  in `TxnMeta.HistoryFloor` on every batch, how far back a rollback
  could reach: with no live savepoint nothing is kept; with the oldest
  live savepoint at sequence F, the newest entry at or below F and every
  entry above it; two entries at one sequence collapse to the later. A
  coordinator from before the field leaves it 0 and gets the old
  behavior, so no cluster-version gate is needed. Measured on
  `BenchmarkIntentRewriteDepth`: one more write to a key already
  written 64 times in the transaction 136 → 12 µs and 45.9 KB → 1.6 KB
  allocated, flat from depth 1 to 64 (under a savepoint taken before
  the first write the history is kept as before). `bench/workloads.json`
  gains `hot-row` — one row updated 16 times per transaction — for the
  harness. `TestIntentHistoryBounded`.
- The store runs on Pebble v2 (`github.com/cockroachdb/pebble/v2`
  v2.1.7, from v1.1.5; #166). The port is mechanical — the encrypting
  `vfs.FS` takes the disk-write category its methods gained and returns
  `vfs.FileInfo`, the option and metric renames, `Compact` with a
  context — and the on-disk format stays at 16 (`FormatVirtualSSTables`,
  the pin below; v2 opens every existing store as it is, since its
  minimum supported format is 13). Columnar blocks (19) and value
  separation (24) are not adopted here: each is a separate,
  cluster-version-gated step with its own before/after. Pebble's own
  log lines — v2 reports the WALs it finds at open and their replay at
  info — now go through the node's log at debug level instead of to
  stderr.
- The Pebble format version is pinned at `FormatVirtualSSTables` (16)
  instead of tracking `FormatNewest` (#166): the formats past it change
  what lands on disk (columnar blocks, value separation), and adopting
  one is a deliberate, cluster-version-gated step with its own
  measurements, not a side effect of a dependency bump. Nothing changes
  for existing stores (16 is what the bundled Pebble's newest is).

## 0.40.1 — unreleased

### Fixed
- `Stopper.Stop` returns to every caller only once the shutdown has
  finished (#139). A concurrent second caller waited for the workers
  only and could return while the first caller was still running the
  closers — with the engine still open for a caller that then removed
  the data directory or asserted on final state. A closer registered
  after Stop had taken the closers was appended to a list nobody read
  again, silently; it now runs at once (one registered while the
  workers wind down runs in Stop, as before).
- HTTP client-certificate authentication checks that the certificate's
  CommonName is a role that may log in (#138), as the SQL port always
  did. A CA-verified certificate was accepted as its principal with no
  role lookup, so `ALTER ROLE ... NOLOGIN` or `DROP ROLE` revoked SQL
  and HTTP Basic access but left a certificate holder the read-only
  HTTP endpoints until the certificate expired (five years, with no
  revocation). A refused certificate now gets the `401` the Basic path
  gives, Basic credentials on the same request still get their turn,
  and the cluster's own node certificate is admitted as before.
  `TestHTTPCertAuthChecksLogin`.
- The SCRAM exchange no longer tells which user names exist (#137).
  Unknown users authenticated against one shared stand-in verifier, so
  the salt in `server-first` — sent before any proof — was one constant
  for every name that is not a user and a random value for every name
  that is: one probe of an impossible name, then one `client-first` per
  candidate, enumerated users without a password guess. The stand-in
  salt is now derived from the user name under a cluster-wide secret
  (`/system/auth-secret`, created by the first node that needs it), so
  it is stable per name across handshakes and nodes and indistinguishable
  from a real one; authentication fails as uniformly as before, and HTTP
  Basic, which never shows a salt, keeps the shared stand-in as a timing
  equalizer. `TestSCRAMStandInSaltPerUser`, `TestMockVerifier`.
- A panic on the SQL statement path no longer kills the node (#136).
  The connection goroutine had no recover, so a bug in the planner,
  the executor, a builtin or an encoder — reached by one statement —
  ended the process with every connection on it. The statement path
  is now a panic barrier: the statement fails with `XX000` (its stack
  in the log, `datax_sql_statement_panics_total` counting it), its
  transaction fails as on any other error, and the connection and node
  keep serving; streamed results are covered on every pull. Stack
  exhaustion and out-of-memory remain fatal, as Go makes them.
  `TestStatementPanicBarrier`.
- Binary `NUMERIC` parameters are bounded on decode (#140): `weight` and
  `dscale` came off the wire unchecked, so an eight-byte parameter with
  no digit groups expanded to ~200 KB of zeros per value. Both are now
  limited to what `NUMERIC(p, s)` can hold (1,000 digits of integer part
  or scale) and refused with `22P03` past that — the SQLSTATE every
  undecodable binary parameter now carries, in place of `08P01`; the
  encoder refuses the same bounds instead of wrapping the weight.
- A statement nested more than 1,000 levels deep (parentheses,
  subqueries, derived tables, `CASE`) is refused with a syntax error
  (`42601`, "statement nests too deeply") instead of exhausting the
  goroutine stack — a fatal error that took the node and every
  connection on it down, reachable by any client with a ~240 KB
  statement (#135). One depth counter in the parser covers every
  recursive production.
- A split's right-hand range keeps the timestamp-cache protection for
  reads the parent served on its span (#134). The RHS inherited the
  parent's closed timestamp as its cache floor, which trails now() by
  the closed-timestamp lag (3 s); reads the parent served inside that
  window lived only in its in-memory cache, so a write at one of those
  timestamps could land on the fresh RHS beneath a read already served
  — a serializability violation for readers in the window. Every
  replica now bumps the RHS's cache floor to now() as it applies the
  split (as a merge does for an absorbed span); the one-time push this
  costs a transaction that began before the split and writes to the RHS
  after it is the same as a leadership change's.
  `TestSplitKeepsServedReadsProtected`.
- The one-phase commit path honors the transaction's commit deadline
  (#133). The deadline — a schema lease's expiration, pinned when a
  statement plans against the leased descriptor — was checked only on
  the classic commit path; the one-phase path every implicit
  single-range `INSERT`, `UPDATE` and `DELETE` takes skipped it, so a
  statement planned under a lease an index build had since drained could
  commit past the drain and leave a row the index never saw. The check
  now runs inside that path's retry loop too (a refresh moves the write
  timestamp, and the moved timestamp is what commits).
  `TestOnePhaseCommitHonorsDeadline`,
  `TestOnlineCreateIndexUnderLapsedLeaseImplicit`.

### Changed
- `TestDecommissionDrainsReplicas` asserts its steady state (#167): the
  "no churn after stopping the drained node" check sampled the range
  generations while the drain's last moves could still be settling, so
  a descriptor bump in flight counted as churn (about one run in ten).
  It now samples once the generations have held still. The other test
  #167 names, `TestMergeFrozenAndRecovery`, already waits for the RHS
  leader before it subsumes (since 0.35.0); the new
  `TestSplitKeepsServedReadsProtected` had the same race and now waits
  the same way (`waitForLeader`). The crash tests' child node inherits
  its listeners from the parent instead of binding ports the parent
  picked and released a process start earlier — a race with the
  packages testing alongside, whose nodes and clients take ephemeral
  ports on the same loopback, that killed the child on "address
  already in use" before it served (one CI run in a few); a child that
  fails to serve now has its log quoted in the failure. `make test` and
  `make test-race` pass `-timeout 30m`: the cluster suite outruns
  `go test`'s default 10 minutes on a small machine.
- CI runs `staticcheck` (pinned at 2025.1.1, before the suite so a
  failure is quick; `make staticcheck` / `make lint` run the same) on
  top of gofmt and `go vet` (#142). Its first run is the baseline: the
  17 findings it made — 13 unused functions, fields and types, a
  redundant `| 0`, two simplifications — are gone.
- A `vulncheck` workflow runs `govulncheck` (pinned at v1.7.0) on every
  push and pull request and weekly on a schedule, as a gate (#143): it
  reports only advisories whose vulnerable symbols this module reaches,
  and advisories appear against unchanged code, hence the timer.
  `make vulncheck` runs it locally. Its first run found three reachable
  advisories — GO-2026-6061 and GO-2026-4762 in `google.golang.org/grpc`
  v1.71.0 (the HTTP/2 transport server, an authorization bypass via
  the `:path` header), GO-2026-5970 in `golang.org/x/text` v0.29.0 (an
  infinite loop in normalization, reached from SASLprep) — so gRPC
  moves to v1.82.1 and x/text to v0.39.0 (with `x/net` v0.53.0 and
  `x/sync` v0.21.0 as they require).

## 0.40.0 — unreleased

### Changed
- Scans step instead of seeking (#160). `MVCCScan` advanced to the next
  row with an LSM seek and found each row's visible version with
  another; both are now bounded walks with `Next` (a reverse scan steps
  back with `Prev`), seeking only past a chain of more than eight
  versions, and versions are recognized by their encoded suffix rather
  than decoded per key. A 1,000-row scan over single-version rows takes
  under half the time it did; `TestScanStepMatchesSeekPerRow` checks the
  stepping scan against the seeking one over version chains, tombstones,
  intents and uncertainty windows, forward and reverse.

## 0.39.0 — unreleased

### Added
- A plan cache for prepared statements (#107). Each session keeps a
  bounded LRU (128) of what its single-table `SELECT`, `UPDATE` and
  `DELETE` statements planned against — the descriptor, the statistics,
  the projection and the shape of a primary-key point lookup — keyed by
  the statement as prepared and the current database; an execution
  whose lookups return the same descriptor and statistics reuses it,
  binding the parameters into the point plan without re-planning, and
  any schema change, `ANALYZE`, drop or re-create misses. A data
  statement resolves each table name once per execution. `EXPLAIN`
  appends `(cached plan)`; `datax_sql_plan_cache_hits_total`,
  `_misses_total`, `_evictions_total`; the hit rate on the console's
  node page and in the internal metrics table. Connections keep a parse
  cache of their last 64 simple-protocol texts
  (`datax_sql_parse_cache_hits_total`). Measured first: planning was
  ~8 % of a gateway's CPU on point lookups through the extended
  protocol, so the cache is scoped to that; the statement activity
  tracker's per-statement connection walk (3 %) is gone too.
- `datax bench kv` and `bank` send parameterized statements over the
  extended protocol (`--protocol simple|extended|auto`).

## 0.38.0 — unreleased

### Changed
- The timestamp cache is indexed by key (#108, with the latch index that
  landed in 0.34.0). A generation holds its point reads in a map — one
  entry per key, the newest read of it — and only its ranged reads in a
  slice, so a point write looks its keys up instead of scanning every
  entry against every span: a 100-key write against two full generations
  takes ~2 µs instead of ~1.5 ms, which was a quarter of a leader's CPU
  under batched ingest (each INSERT's uniqueness probes put one entry per
  row in the cache). Generations hold 4,096 entries (1,024 before) and a
  key read repeatedly costs one, so a hot key set no longer rotates the
  cache — and rotation is what briefly pushes every writer on the range.

## 0.37.0 — unreleased

### Changed
- The single-range write pipeline (#106). Measured first, one range on
  one node below SQL (`BenchmarkRangeWritePipeline`): the ceiling was
  the apply, not the disk — every MVCC write cost two LSM reads on a
  fresh iterator (~10 µs a row), serialized per range inside the raft
  pass that committed it, so a range topped out near 100k rows/s at any
  batch size with the sync on or stubbed out. Two changes: each write
  now finds what it lands on with one bounded seek on an iterator its
  batch keeps (`Batch.writeState`; a 100-row commit applies in ~0.4 ms
  instead of ~1 ms), and committed entries apply on a pool of apply
  workers off the raft pass, so a range's next append and sync run while
  its previous entries apply (conf changes still apply inline; a replica
  with more than 64 MiB queued gets no pass until it drains,
  `datax_raft_apply_backpressure_total`). One range on one node, 16
  writers: 100-row commits 857 → 1,739/s, single-row commits 12.6k →
  16.3k/s, 1,000-row commits 100 → 262/s. New metrics:
  `datax_raft_entries_appended_total`, `datax_raft_entries_applied_total`,
  `datax_raft_apply_seconds`, `datax_latch_wait_seconds`. A proposal whose
  replica stops (shutdown, removal, a failed apply) is answered with an
  ambiguous error instead of waiting out its context. Test-only:
  `DATAX_TESTING_NOSYNC=1` commits the raft log unsynced for a
  measurement. Capacity planning in `docs/user/operations.md` restates
  the per-range ceiling from the measured costs.

## 0.36.0 — unreleased

### Changed
- The split store (#105; cluster version **v13**). A store's raft state —
  every replica's HardState, log entries and truncated state — moves to
  a raft engine of its own under `--dir/raft`, and the state-machine
  engine under `--dir` runs without a write-ahead log: a replicated write
  reaches disk once, through the synced group-committed raft log, instead
  of twice. What a crash takes from the state engine's memtable is
  replayed from the log (`datax_raft_replayed_entries_total`); a clean
  shutdown flushes first. Log truncation is deferred until the state
  engine has flushed past the entries it removes
  (`datax_raft_deferred_truncations_total`; a truncation pending past
  30 s has the housekeeping tick flush for it,
  `datax_raft_truncation_flushes_total`); merges, replica removals and
  catch-up snapshots flush before touching the raft engine; raft state
  orphaned by a crash is swept at startup. A store created by a
  v13 binary or joining a v13 cluster is split from the start; an older
  store migrates on its first start after the finalize and then refuses
  a v12 binary (the one upgrade step that cannot roll back). Both
  engines are encrypted, rotated and re-encrypted together.
  `datax_storage_split`, `datax_storage_bytes_written_total{engine,kind}`;
  `/api/node` reports `engine_mode` and the raft engine's metrics. On
  the harness's single node, batched ingest writes about half as many
  bytes to disk per row (balanced profile: 7.5× → 3.5× of the row
  bytes with sequential keys, 14.2× → 6.9× with UUID keys) at 16–19 %
  more rows per second; the ingest profile goes 3.2× → 2.3× and
  6.2× → 4.1× at 3–5 % more.

## 0.35.0 — unreleased

### Changed
- Streaming SELECT execution (#104). A scan-shaped `SELECT` — one
  table, no join, aggregate, `DISTINCT`, window, set operation,
  correlated subquery or in-memory sort — no longer materializes its
  result on the gateway: the wire layer pulls rows from KV in pages of
  512 as it writes them, flushing every 64 kB, so the first row leaves
  before the last is read and a full-table `SELECT` holds one page at a
  time. A row-limited `Execute` (JDBC fetch sizes) pulls its rows on
  demand and a suspended portal keeps the scan open. An error after
  rows have gone out (a bad row, a cancellation, `statement_timeout`)
  arrives after them, as in PostgreSQL; an implicit transaction re-runs
  the statement on a retryable error only while nothing has been
  flushed, otherwise the `40001` is surfaced. `datax_sql_streamed_rows_total`,
  `datax_sql_stream_restarts_total`.
- `statement_memory_limit` (default `64MB`; `0` = none). The paths that
  do materialize — sorts, aggregates, joins, `DISTINCT`, `WITH` members,
  derived tables, index joins that collect their rows — charge what they
  hold against it and fail with `53200` beyond it instead of growing
  without bound. `SET`, `SET LOCAL`, `RESET`, `SHOW`, `pg_settings`;
  `datax_sql_memory_limit_hits_total`.
- `datax bench` records the time to the first row of the `scan` and
  `index-join` workloads (`first_row_p50_us`, `first_row_p99_us`;
  `bench compare` shows them).

## 0.34.0 — unreleased

### Changed
- Batched point reads in the executor (#103). An index join fetches the
  primary rows behind its index entries in pages of 256, each page one
  routed batch fanned out per range, in index order (`EXPLAIN ANALYZE`
  reports the batches); a lookup matching 1,000 entries is four round
  trips instead of 1,000. `INSERT` builds every row first and reads all
  its primary-key and unique-index uniqueness probes and its foreign-key
  parent lookups in one batch (as `COPY` did for primary keys; `COPY`
  now batches the unique-index and foreign-key probes too); `UPDATE`
  computes every row's new values first and batches the moved
  unique-index entries' probes and the changed keys' parent lookups;
  `SELECT ... FOR UPDATE` locks the selected rows in one batch. Each key
  still records its read timestamp for refresh, and a key the batch was
  not primed with still reads on its own. Before/after on the harness in
  the PR; the set gains `index-join-1pct` and `index-join-10pct` (200 and
  2,000 rows per lookup).
- The latch manager indexes point spans by key (#108, latch part). Its
  conflict check was a linear scan of every held latch's spans against
  every span of the new request, allocating per comparison; with the
  wide batches of #103 (100-key probes under the 8-way `ingest` load) it
  reached 40% of a node's CPU and cost ingest a quarter of its throughput.
  A point span now checks the holders under its key plus the ranged
  holders; only ranged spans (scans, splits, merges) still scan every
  holder, and overlap checks no longer allocate.

## 0.33.0 — unreleased

### Changed
- Coalesced heartbeats and range quiescence (#102, part c; cluster
  version **v12**). Heartbeats and their responses travel as one
  envelope per peer node per scheduler pass instead of one each, and an
  idle range — no proposal, read-index request or snapshot for 2 s,
  every follower caught up and answering — stops ticking and heartbeating
  on every replica until a message, a proposal or a client request wakes
  it; a woken leader heartbeats at once and re-establishes follower
  contact before its first lease read. An idle range's closed timestamp
  now travels off the log (with the leader's term and log index, honored
  by a follower only while it still follows that leader at that term and
  has applied that index; in memory only), so follower reads stay fresh
  on quiescent ranges without a raft entry and an fsync per range per
  second — and for quiescent ranges it is grouped: one promise per
  follower node per round covers every range registered there, so an
  idle store publishes a few envelopes a second however many ranges it
  holds. `/status` reports `quiescent` per range; new series
  `datax_quiescent_ranges`, `datax_raft_quiesces_total`,
  `datax_raft_unquiesces_total`, `datax_raft_heartbeat_envelopes_total`,
  `datax_raft_heartbeats_coalesced_total`,
  `datax_closed_timestamp_side_updates_total`,
  `datax_closed_timestamp_group_updates_total`. Both stay off until
  `datax debug upgrade` finalizes v12 (a v11 node reads neither).
  Before/after on the harness in the PR.
- Lease-based reads take a fast path: a leader that has committed an
  entry in its own term answers the read index with its commit index at
  once — what raft's lease-based read would put in the next Ready —
  instead of a scheduler pass and a Ready per read.

### Added
- `--merge-size-threshold` on `datax start` and `datax demo` (negative
  disables merging, e.g. to keep an empty pre-split for a benchmark).
  `datax bench` records carry `error_samples` (the distinct messages
  behind `errors`), and a `--presplit` run uses tables of its own
  (`bench_kv_r1000`, ...) so it neither inherits an earlier workload's
  rows nor collides with its keys. `ALTER TABLE ... SPLIT AT` waits out
  a merge in flight on the range instead of failing.

## 0.32.0 — unreleased

### Changed
- Store-level raft scheduler with group commit (#102, parts a and b): a
  node's raft groups are driven by one fixed pool of workers
  (`GOMAXPROCS`; `StoreConfig.RaftWorkers`) and one 100 ms ticker
  instead of a goroutine and a ticker per replica. A worker takes a
  group of queued replicas, handles one Ready each, and stages every
  HardState and log entry into one synced Pebble batch — ten ranges
  appending in the same moment cost one fsync, not ten — before any of
  them sends a message or applies. New series:
  `datax_raft_scheduler_latency_seconds`, `datax_raft_ready_passes_total`,
  `datax_raft_log_syncs_total`, `datax_raft_readies_per_sync`. The
  crash-consistency test kills the node at group-commit boundaries with
  eight writers over sixteen ranges. Before/after on the harness in the PR.

### Added
- `ALTER TABLE t SPLIT AT VALUES (k, ...), ...` carves ranges at
  primary-key tuples (a prefix of the key is allowed) and returns the
  boundaries; idempotent, refused inside a transaction block (`25001`)
  and on sharded timeseries tables (`0A000`, carved by shard). `datax
  bench ... --presplit N` uses it; the checked-in set gains
  `kv-50-50-1000-ranges` and `ingest-random-1000-ranges`.

## 0.31.0 — unreleased

### Changed
- Pebble tuning (#101): every store gets a block cache sized from the
  machine's memory (25 % capped at 8 GiB for `balanced`, 10 % capped at
  2 GiB for `ingest`; `--cache-size`; one cache per process, shared by
  every engine and released when the last closes), bloom filters
  (10 bits per key) on every level, the newest sstable format the
  bundled Pebble supports, and an open-file budget of half the process's
  descriptor limit (1000–16384) instead of Pebble's 8 MiB cache, no
  filters and 1000 files. `StorageMetrics`, `/metrics`
  (`datax_storage_block_cache_{bytes,size_bytes,hits_total,misses_total}`,
  `datax_storage_bloom_{hits,misses}_total`), the metrics table
  (`store.block_cache_*`, `store.bloom_*`) and the dashboard's storage
  section show the cache hit rate and bloom utility. Before/after on the
  harness in the PR.

### Fixed
- A scan whose rows exceeded gRPC's 4 MiB default message limit never
  came back from a range led by another node: every attempt ran into
  the per-attempt timeout and the statement hung until the lease moved
  (the harness's `scan` workload took 13 minutes per query on a 3-node
  cluster). A range now pages scan responses at 8 MiB with a resume
  key, the client stitches the pages, and the internode message limit
  is 64 MiB in both directions.
- A read whose leadership confirmation (the raft read index) timed out —
  a freshly split range still electing, a briefly partitioned quorum —
  failed the statement with `XX000: read index abandoned`. The replica
  now answers NotLeader, so the client re-routes and retries under the
  statement's own deadline. `datax bench` retries transient failures
  during its preload for the same reason.

## 0.30.0 — unreleased

### Added
- Profiling and benchmark harness (#100): `net/http/pprof` under
  `/debug/pprof/` on the HTTP port (admin-gated), mutex and block
  profiling always on at low rates, `datax debug profile --kind
  cpu|heap|allocs|mutex|block|goroutine|trace`. `datax bench` gains
  `--seed` (fixed by default), `--json` records (throughput, p50/p95/p99,
  errors, retries, the server counter deltas), `--cpuprofile` /
  `--memprofile` / `--trace` for the client, `--server-url` and
  `--server-profile cpu` for the node, `--keys
  random|sequential|uuid` for ingest, and the `index-join` and `scan`
  workloads. The checked-in set `bench/workloads.json`, `make bench`
  (a fresh single node and a fresh 3-node cluster), `datax bench run`,
  `datax bench compare BEFORE AFTER` (±5 % flags), a nightly workflow
  that keeps `main`'s records, `bench/README.md`. A crash-consistency
  helper (`pkg/testutils/crash`: a child node killed with SIGKILL at a
  fault point — `pkg/util/faultpoint`: after the raft log sync, after
  an entry applies, as a memtable flush begins — or from outside, then
  restarted; every acknowledged write present, applied index caught up
  with the log) with `TestCrashConsistency`; `/status` reports each
  range's `last_index`.

### Fixed
- `datax bench` keys are `INT8` again: `INT` became 32-bit in 0.24.0,
  so the ingest workloads' random keys were refused.

## 0.29.0 — unreleased

Cluster version **v11**: role descriptors (`/system/roles`) supersede
the user credential records and admin markers; `datax debug upgrade`
rewrites them at finalize.

### Added
- Roles and privilege scopes (#98): `CREATE / ALTER / DROP ROLE` and
  `USER` (`LOGIN` / `NOLOGIN`, `PASSWORD`, `INHERIT` / `NOINHERIT`, `IN
  ROLE`, `IF [NOT] EXISTS`; a role may change its own password), role
  membership (`GRANT role TO role [WITH ADMIN OPTION]`, `REVOKE [ADMIN
  OPTION FOR]`, inheritance, cycles refused), `SET [LOCAL] ROLE` /
  `RESET ROLE` with `current_user` vs `session_user`. Ownership: tables,
  views, sequences, types and databases record their creator, who
  holds every privilege and alone (with admins) may alter, drop or
  grant them; `ALTER ... OWNER TO`, `REASSIGN OWNED BY`, `DROP OWNED
  BY`; `DROP ROLE` refuses an owner (`2BP01`) and takes the role's
  grants and memberships with it. Scopes: `GRANT ... ON DATABASE`
  (`CONNECT`, `CREATE`), `ON SCHEMA public` (`USAGE`, `CREATE`; `USAGE`
  revocable from `PUBLIC`), `ON ALL TABLES | SEQUENCES IN SCHEMA
  public`, `ON SEQUENCE` (`USAGE`, `SELECT`, `UPDATE`; `nextval` /
  `currval` / `setval` now check them, a `SERIAL` column's sequence
  following `INSERT` on its table), the `TRUNCATE` privilege, `WITH
  GRANT OPTION` / `GRANT OPTION FOR`, `PUBLIC` as a grantee, `ALTER
  DEFAULT PRIVILEGES [FOR ROLE r] [IN SCHEMA public] GRANT | REVOKE ...
  ON TABLES | SEQUENCES`. A view's query runs with its owner's
  privileges (definer semantics). Built-in roles `admin` (the old
  `ADMIN` marker, `root` an implicit member), `read_all`, `write_all`,
  `metrics` (HTTP `/metrics` only). HTTP and admin-RPC authorization
  resolve through membership; audit records carry the session user and
  the current role. `SHOW ROLES`, `SHOW USERS` (`member_of`), `SHOW
  GRANTS` (`database_name, schema_name, relation_name, grantee,
  privilege_type, is_grantable`; `ON DATABASE`, `ON ROLE`), `pg_roles`
  (`rolcanlogin`, `rolinherit`), `pg_auth_members`, `pg_user`,
  `information_schema.role_table_grants` (`grantor`, `is_grantable`),
  psql's `\du`; the dashboard schema browser's user list follows.

### Changed
- `GRANT` / `REVOKE` name existing roles (`42704` otherwise) — in
  insecure mode too. `/metrics` takes the `metrics` role (or admin)
  instead of any user. `SHOW GRANTS` gained the schema and grantable
  columns; `SHOW ROLES` lists every role, `SHOW USERS` the login ones.
  `ALTER` / `DROP` of a table, view, index, sequence or type, `COMMENT
  ON`, `CREATE INDEX` and `TRUNCATE` are for the object's owner (and
  admins) rather than admins only; `DROP DATABASE` / `ALTER DATABASE`
  for its owner. A caller's own context deadline during a statement
  reports `canceling statement due to user request` (the statement
  timeout message is reserved for `statement_timeout`).

## 0.28.0 — unreleased

### Added
- Session and wire (#97): query cancellation works — every connection
  gets a process ID (the node in its high bits) and a secret, a
  `CancelRequest` (psql's Ctrl-C, pgx's context cancellation, pools)
  stops the statement in flight with `57014` and rolls its transaction
  back, and one landing on another node is forwarded over the internode
  admin RPC. `statement_timeout` (`57014`), `lock_timeout` (`55P03`
  instead of waiting the conflict budget out on a live intent),
  `idle_in_transaction_session_timeout` (`25P03`, the idle block rolled
  back and its intents released). Honored variables with `SET` / `SET
  LOCAL` / `RESET` / `RESET ALL` / `SHOW` / `SHOW ALL` / `pg_settings`:
  `application_name` (the startup parameter too; shown by the activity
  views), `TimeZone` (TIMESTAMPTZ text output rendered in the zone),
  `search_path`, `DateStyle`, `client_encoding`,
  `default_transaction_read_only` / `transaction_read_only` / `SET
  TRANSACTION READ ONLY` (`25006` on a write), `transaction_isolation`
  (every level accepted, `SHOW` says serializable), `SET SESSION
  CHARACTERISTICS AS TRANSACTION ...`, `SET TIME ZONE`, `SET NAMES`;
  changed reported parameters are announced with `ParameterStatus`.
  `pg_backend_pid()` is real; `pg_cancel_backend(pid)` /
  `pg_terminate_backend(pid)` (admin, any node); `SHOW SESSIONS` and
  `pg_stat_activity` list the node's sessions; `pg_sleep(seconds)`.

### Changed
- `SET` of an unknown variable is `42704` (it was silently accepted);
  an invalid value is `22023`.

## 0.27.0 — unreleased

### Added
- Type system, part four (#96, closing it): enums. `CREATE TYPE [IF NOT
  EXISTS] name AS ENUM ('a', 'b', ...)`, `ALTER TYPE name ADD VALUE [IF
  NOT EXISTS] 'c'` (appended; every column of the type learns the label
  in the same statement and its tables drain, so the label is usable at
  once on every gateway), `DROP TYPE [IF EXISTS] name` (refused while a
  column uses it). A column of the type stores the label's ordinal with
  the label (cluster version v10), orders by declaration in `ORDER
  BY`, `min` / `max`, indexes and primary keys, reads and writes labels,
  refuses an unknown one (`22P02`), takes `'a'::name` casts, `LIKE`,
  `CREATE TABLE AS` and `ALTER COLUMN TYPE` from and to text. `pg_type`
  (`typtype = 'e'`, an OID past the builtin range), `pg_enum`,
  `information_schema.columns` (`USER-DEFINED`), `format_type`, `SHOW
  CREATE TABLE`, psql's `\dT` and `\d`; the wire describes the type's
  OID and carries labels in both formats.

## 0.26.0 — unreleased

### Added
- Type system, part three (#96): arrays of every scalar family but
  `JSONB` as column types (`INT8[]`, `TEXT ARRAY`, `VARCHAR(3)[]`;
  cluster version **v10**, like `INTERVAL` and `TIME`). Literals
  (`'{1,2}'`, `'{a,"b c",NULL}'`), `ARRAY[...]`, subscripts `a[i]`,
  `ANY` / `ALL` and the comparison operators over arrays, `@>` / `<@` /
  `&&`, `||` concatenation, element-wise equality and ordering (`GROUP
  BY`, `ORDER BY`, `DISTINCT`), `unnest` (`FROM` and select list) with
  typed rows, `array_agg` returning a real array, `array_length`,
  `cardinality`, `array_append` / `array_prepend` / `array_cat` /
  `array_position` / `array_remove` / `array_to_string` /
  `string_to_array` / `array_upper` / `array_lower` / `array_ndims`,
  `::int8[]` casts, `array(SELECT ...)` as a typed array. The wire
  describes PostgreSQL's array OIDs (`_int8` 1016, `_text` 1009, ...)
  and speaks the text and binary array formats in both directions, so
  pgx scans into and binds Go slices and `WHERE id = ANY($1)` takes a
  slice (the parameter describes as the column's array type); `pg_type`
  carries the array types with `typelem` / `typarray`, `pg_attribute`
  `attndims`, `information_schema.columns` `ARRAY` / `_int8`. `CREATE
  TABLE AS`, `LIKE` and `ALTER COLUMN TYPE` from text carry arrays.
  Arrays are not indexable and cannot be keys.

### Changed
- `array_agg` and `array(SELECT ...)` return an array type (`_int8`,
  `_text`, ...) instead of text.

## 0.25.0 — unreleased

### Added
- Type system, part two (#96): `INTERVAL` and `TIME` as column types
  (cluster version **v10**: a v9 node cannot decode their rows, so a
  column of either is refused until the upgrade is finalized). An
  interval is PostgreSQL's months / days / clock triple: every input
  form (verbose, `'2h30m'`, `'... ago'`, SQL standard `'1-2 3
  04:05:06'`, ISO 8601), the `INTERVAL '...'` / `DATE '...'` / `TIME
  '...'` / `TIMESTAMP '...'` typed literals, PostgreSQL's rendering and
  comparison rule, `timestamp - timestamp` and `age()` now return an
  interval (they were text), `interval ± interval`, `interval * / n`,
  `time ± interval`, `time - time`, `date + time`, `extract` over
  intervals and times, `justify_hours` / `justify_days` /
  `justify_interval`, `make_interval` / `make_time`, `sum` / `avg` /
  `min` / `max` over intervals, indexes and primary keys on both,
  `interval` (1186) / `time` (1083) text and binary wire codecs (pgx's
  `pgtype.Interval`, `time.Duration`, `pgtype.Time`), `pg_type` rows,
  `ALTER COLUMN TYPE` from text. The timeseries `retention` option and
  `with_max_staleness` accept interval text.

### Changed
- `timestamp - timestamp`, `age()`, `make_interval()` and
  `justify_hours()` return `INTERVAL` (OID 1186) instead of text.

## 0.24.0 — unreleased

### Added
- Type system, part one (#96): the type modifiers a column declares
  are enforced and described. Integer widths — `INT2` / `SMALLINT`,
  `INT4` / `INT` / `INTEGER`, `INT8` / `BIGINT` — bound the values
  (`22003`) and describe with PostgreSQL's OIDs (21 / 23 / 20) and
  binary sizes, so drivers scan into `int16` / `int32`; `SERIAL` is
  `INT4`, `SMALLSERIAL` `INT2`. `VARCHAR(n)` / `CHAR(n)` refuse a
  longer value (`22001`; excess spaces are dropped) and `CHAR(n)` renders
  blank-padded (`varchar` 1043 / `bpchar` 1042 with the typmod).
  `TIMESTAMP` is now `TIMESTAMP WITHOUT TIME ZONE` (OID 1114: an input
  offset is ignored, the output carries none), `TIMESTAMPTZ` /
  `TIMESTAMP WITH TIME ZONE` is unchanged, and `TIMESTAMP(p)` /
  `TIMESTAMPTZ(p)` round to `p` digits on write. `SHOW CREATE TABLE`,
  `information_schema.columns`, `pg_attribute`, `pg_type` (five new
  rows), `format_type`, `LIKE` and `CREATE TABLE AS` carry the
  modifiers; `ALTER COLUMN TYPE` changes them — a widening is one
  descriptor write, a narrowing rewrites and checks every value. Storage
  is unchanged (the modifiers ride on the descriptor; no cluster
  version bump); until v9 is finalized a new column keeps the earlier
  meaning of its declaration.

### Changed
- `INT` / `INTEGER` columns are 32-bit (they were `INT8`): a value past
  ±2³¹ into a column created from now on is `22003`. Existing columns
  keep their width. `TIMESTAMP` columns created from now on render
  without the `+00` offset and ignore an input offset.

## 0.23.0 — unreleased

### Added
- DDL completeness, part three (#95, closing it): `CREATE TABLE ... AS
  query [WITH NO DATA]` (the query's shape and rows, streamed through
  the COPY chunk path; a hidden `rowid` key unless `PRIMARY KEY (cols)`
  is written among the column names; `SELECT ... INTO` refused with a
  pointer), `CREATE TABLE ... (LIKE t [INCLUDING | EXCLUDING DEFAULTS |
  CONSTRAINTS | INDEXES | COMMENTS | ALL])`, `ALTER TABLE ... ALTER
  COLUMN c [SET DATA] TYPE t` as an online rewrite (a hidden shadow
  column every write fills, a chunked conversion of the existing rows,
  a swap; widening and text conversions; cluster version v9), and
  `COMMENT ON TABLE | VIEW | INDEX | COLUMN ... IS 'text' | NULL` with
  `obj_description`, `col_description` and `pg_description` (psql's
  `\d+`, `\dt+`).

## 0.22.0 — unreleased

### Added
- DDL completeness, part two (#95): views. `CREATE [OR REPLACE] VIEW
  name [(cols)] AS query` stores the query; a statement that names the
  view runs it and reads the rows like a table (as a base, join side,
  subquery, set-operation member, `INSERT ... SELECT` source, inside
  `WITH`; a view over a view expands the same way). `DROP VIEW [IF
  EXISTS] ... [CASCADE]`; `DROP TABLE`, `DROP VIEW`, `RENAME TO`,
  `RENAME COLUMN` and `DROP COLUMN` refuse (`2BP01`) while a view
  depends on the relation unless `CASCADE` drops the views; DML and
  physical DDL on a view are `42809`. `SHOW VIEWS`, `SHOW CREATE VIEW`,
  `pg_class` (`relkind = 'v'`), `pg_views`, `information_schema.tables`
  and `.views`, the dashboard's schema browser, psql's `\dv` and `\d
  view`. Reading a view needs `SELECT` on the view and on the tables
  its query reads. Cluster version **v9**: `CREATE VIEW` is refused
  until `datax debug upgrade` finalizes it.

## 0.21.0 — unreleased

### Added
- DDL completeness, part one (#95): `DROP INDEX [IF EXISTS]` (the
  index leaves the schema at once; its entries are reclaimed after the
  commit and lease drain), `ALTER INDEX ... RENAME TO`, `ALTER TABLE
  ... RENAME TO / RENAME [COLUMN] / RENAME CONSTRAINT` (foreign keys,
  sequences and grants follow the table's ID; `CHECK` expressions are
  rewritten for a renamed column; a `UNIQUE` constraint and its index
  rename together), `ALTER TABLE ... ALTER COLUMN SET DEFAULT | DROP
  DEFAULT` (constants and expressions; a fill-on-read column keeps
  filling its old rows from the original constant), `TRUNCATE [TABLE] t
  [, ...] [RESTART IDENTITY] [CASCADE]` as a transactional layout swap
  (one descriptor write for any table size; the old layout serves `AS OF
  SYSTEM TIME` until the re-shard janitor reclaims it; referencing
  tables refused without `CASCADE`), and `IF [NOT] EXISTS` on `ALTER
  TABLE`, `ADD COLUMN`, `DROP COLUMN`, `CREATE INDEX`, `ALTER INDEX`,
  `ALTER SEQUENCE`, `CREATE USER` and `DROP USER`. The reference
  documents which DDL runs inside a transaction block (every
  single-descriptor-write statement) and which is refused (`25001`: the
  multi-transaction online statements).

### Changed
- `CREATE INDEX` on a taken name and `ADD COLUMN` on a taken name report
  `42710` (duplicate object) instead of `42601`.

## 0.20.0 — unreleased

### Added
- Query shapes, part four (#94, closing it): `IN` and `EXISTS`
  subqueries inside `OR` (uncorrelated ones evaluated once, correlated
  ones per row); correlated subqueries up to 8 nesting levels; scalar
  subqueries in `ORDER BY`; `EXPLAIN ANALYZE`, which runs the statement
  and reports each stage's actual rows and time (scans with their
  paths, join levels, group / window / set-operation and sort stages)
  and the total.

## 0.19.0 — unreleased

### Added
- Query shapes, part three (#94): window functions — `row_number`,
  `rank`, `dense_rank`, `percent_rank`, `cume_dist`, `ntile`, `lag`,
  `lead`, `first_value`, `last_value`, `nth_value` and every aggregate
  `OVER ([PARTITION BY ...] [ORDER BY ...] [ROWS | RANGE frame])`, a
  `WINDOW` clause, window calls inside expressions and predicates
  (`amount - lag(amount) OVER (...)`), over plain, joined and grouped
  queries; derived tables as join members (`JOIN (SELECT ...) AS d ON
  ...`); `EXPLAIN` notes the window stage.

## 0.18.0 — unreleased

### Added
- Query shapes, part two (#94): `WITH` — members materialized once, in
  order, readable anywhere a table is (base, join side, subquery
  source, set-operation member, `INSERT` source), with column lists,
  on `SELECT`, `INSERT`, `UPDATE` and `DELETE` and inside subqueries;
  data-modifying members with `RETURNING`; `WITH RECURSIVE` (seed
  `UNION [ALL]` step, capped at 10000 rounds and a million rows);
  `INSERT ... SELECT`; and derived tables as join sides (`FROM (SELECT
  ...) AS d JOIN t ...`).

## 0.17.0 — unreleased

### Added
- Query shapes, part one (#94): `OFFSET n`, `LIMIT ALL`, `FETCH FIRST
  n ROWS ONLY` on every query shape (`LIMIT 0` returns no rows);
  `ORDER BY ... NULLS FIRST | LAST`, positions and expressions over
  grouped and set-operation output, and aggregate calls (`ORDER BY
  count(*)`) in grouped queries; `INTERSECT` and `EXCEPT`, each
  `[ALL]`, with PostgreSQL's precedence, parenthesized members that keep
  their own `ORDER BY` / `LIMIT`, `VALUES` as a member or a statement,
  and column types unified across members; `RIGHT` and `FULL [OUTER]
  JOIN`, `JOIN ... USING (cols)` and `NATURAL JOIN` (the merged column
  shows once and reads as `COALESCE` across an outer join). `EXPLAIN`
  names the join kind and the offset.

## 0.16.0 — unreleased

### Added
- Graceful shutdown (#124): on `SIGTERM` or Ctrl-C a node drains before
  it stops — it announces itself as leaving (`shutting_down` in its
  registry row: peers hand it no leases and place nothing on it),
  transfers every lease it holds to a live peer, closes its SQL
  listener and ends its connections with `FATAL 57P01` (idle ones at
  once, busy ones at their next idle point, open transactions at the
  deadline) — bounded by `--drain-timeout` (default 10s; 0 stops at
  once). A second signal skips the rest of the drain; a third, or a
  stop that hangs past the timeout, exits. `datax demo` drains the same
  way. The dashboard and `/api/health` show a stopping node;
  `Node.Drain` returns what the drain achieved and the node logs it.

## 0.15.0 — unreleased

### Added
- The expression language and builtin functions (#93): a registry of
  88 scalar functions (`pkg/sql/builtins`) — conditionals (`coalesce`,
  `nullif`, `greatest`, `least`), strings (`substring`, `position`,
  `trim`, `lpad`, `split_part`, `format`, `md5`, `sha256`, `encode`,
  `initcap`, `translate`, ...), math (`round`, `trunc`, `mod`, `power`,
  `sqrt`, `ln`, `log`, `random`, `width_bucket`, ...), date and time
  (`date_trunc`, `extract` / `date_part`, `to_char`, `to_timestamp`,
  `to_date`, `make_date`, `make_timestamp`, `age`, `clock_timestamp`),
  JSON (`jsonb_build_object`, `jsonb_build_array`, `to_jsonb`,
  `jsonb_set`, `jsonb_typeof`, `jsonb_extract_path[_text]`, ...) and
  the session functions — with their arity checked by the parser,
  `pg_proc` (`provolatile`, `proisstrict`) and `SHOW FUNCTIONS` listing
  them, and the [Functions reference](docs/user/functions.md) generated
  from the same registry. Casts are now **performed** (`CAST(x AS t)`,
  `x::t`, chains) with PostgreSQL's text forms and error codes,
  `DECIMAL(p,s)` and `VARCHAR(n)` typmods applied on the cast, and
  `regclass` resolved. Operators `%`, `^`, integer overflow detection
  (`22003`), date arithmetic (`date + n`, `date - date`, `ts + '2
  hours'`, month steps clamp), predicates `BETWEEN [SYMMETRIC]` (index
  bounds on a keyed column), `IS [NOT] TRUE / FALSE / UNKNOWN`, `IS
  [NOT] DISTINCT FROM`, `LIKE ... ESCAPE`, `SIMILAR TO`, and a literal
  `LIKE` prefix becoming index bounds. The SQL-form calls
  `substring(s FROM n FOR m)`, `position(a IN b)`, `trim(BOTH x FROM
  s)`, `extract(f FROM ts)`. JSONB `#>` / `#>>` paths, array-index
  `->`, `<@`, `?`, `?|`, `?&` everywhere an expression goes, HAVING
  included. Aggregates over expressions with `DISTINCT` and `FILTER
  (WHERE ...)`: `string_agg`, `array_agg`, `bool_and` / `bool_or` /
  `every`, `stddev*` / `var*`, `percentile_cont` / `percentile_disc
  ... WITHIN GROUP`, `json[b]_agg`, `json[b]_object_agg`. Computed
  outputs describe with their real type on the wire (`qty * 2` is
  INT8, `qty > 3` BOOL, `now()` TIMESTAMPTZ), a cast column keeps its
  name, and `now()`, `current_timestamp` and `current_date` share one
  statement clock. Predicates used as values are three-valued (`NULL
  BETWEEN 1 AND 2` is NULL, not false).
- `CHECK` constraints may use the stable session functions (`CHECK (at
  <= now())`, `CHECK (who = current_user)`), evaluated per statement.

### Changed
- `AVG` of an `INT8` column is a `DECIMAL` (exact, 6 fractional
  digits), as in PostgreSQL, where it was a `FLOAT8`.

### Fixed
- A timestamp outside the representable years 1678 to 2261 (int64
  nanoseconds) was silently wrapped (`'2999-01-01'` became 1829); it
  is now refused.

## 0.14.0 — unreleased

### Added
- Table constraints (#92): `CHECK (expr)`, column and table `UNIQUE`,
  and `FOREIGN KEY ... REFERENCES t (cols) [ON DELETE | ON UPDATE
  RESTRICT | NO ACTION | CASCADE | SET NULL]` (MATCH SIMPLE), named or
  auto-named as PostgreSQL does; `ALTER TABLE ... ADD CONSTRAINT`
  (publishes, then validates the existing rows in bounded chunks; `NOT
  VALID` defers that to `VALIDATE CONSTRAINT`), `DROP CONSTRAINT [IF
  EXISTS]`, `ALTER COLUMN ... SET NOT NULL` (sweeps first) / `DROP NOT
  NULL`, `DROP TABLE ... CASCADE`. A CHECK passes on NULL and is
  `23514` otherwise; a foreign key is checked by a point read of the
  parent in the writing transaction (`23503`) and, on the parent side,
  through an index the constraint creates on the referencing columns
  when none covers them, so a parent delete never scans the child;
  cascades are bounded per statement by `SET
  foreign_key_cascade_limit` (default 10000, `54000` beyond it). `COPY`
  respects every constraint and names the failing row. The catalogs
  show them (`pg_constraint` with `conparentid`, `confupdtype`,
  `confdeltype`; `information_schema.check_constraints`,
  `referential_constraints`, `constraint_column_usage`; `pg_class`
  `relchecks` / `relhastriggers`), so psql's `\d` lists check
  constraints, foreign keys and "Referenced by". For those queries:
  `oid::regclass` on a column, `VALUES` as a `UNION` member,
  `pg_partition_ancestors`, table functions as join members (parsed),
  and selects over an always-empty catalog (`pg_trigger`, ...) answer
  empty whatever shape they take. **Cluster version v8**: descriptors
  gain the constraint fields, which a v7 node would ignore on write, so
  the DDL is refused with `0A000` until `datax debug upgrade` finalizes
  v8.
- `SET name = <number>` is accepted (numeric settings).

## 0.13.0 — unreleased

### Added
- Sequences, `SERIAL` / `BIGSERIAL` / `SMALLSERIAL`, `GENERATED
  {ALWAYS | BY DEFAULT} AS IDENTITY` columns and expression `DEFAULT`s
  (#91): `CREATE / ALTER / DROP SEQUENCE` with `INCREMENT`, `MINVALUE`,
  `MAXVALUE`, `START`, `CACHE`, `CYCLE`, `OWNED BY` and `RESTART`;
  `nextval`, `currval`, `lastval`, `setval`; `unique_rowid()` and
  `gen_random_uuid()`; `DEFAULT` as a value, `INSERT ... DEFAULT
  VALUES`, `OVERRIDING SYSTEM VALUE`, `UPDATE ... SET col = DEFAULT`;
  `SHOW SEQUENCES`, a sequence as a one-row relation, and the
  `pg_sequence(s)` / `pg_attrdef` / `information_schema.columns` rows
  psql and ORMs read. `nextval` advances one counter key with an atomic
  increment outside the transaction (never rolled back, gaps normal);
  each gateway serves `CACHE` values (default 32) per increment.
  Backups carry sequences and their counters. **Cluster version v7**:
  descriptors gain the default-expression, identity and owned-sequence
  fields, which older nodes cannot evaluate, so the DDL is refused with
  `0A000` until `datax debug upgrade` finalizes v7.

## 0.12.0 — unreleased

### Added
- `RETURNING` on `INSERT`, `UPDATE` and `DELETE` (#90): any expression
  over the written row, `*`, aliases; rows come from the values the
  statement has in hand, so `INSERT ... RETURNING id` costs no read.
- `ON CONFLICT (columns | ON CONSTRAINT name) DO NOTHING | DO UPDATE SET
  ... [WHERE ...]` with `EXCLUDED`, arbitrated by the primary key or a
  unique index (`42P10` otherwise; a conflict on another unique key stays
  `23505`), `ON CONFLICT DO NOTHING` without a target, and `UPSERT INTO`.
  Command tags are PostgreSQL's (`INSERT 0 n` counts inserted and
  updated rows; under `DO NOTHING`, inserted rows only).

## 0.11.0 — unreleased

### Added
- Keys and range spans print as readable paths everywhere (logs, `datax
  debug ranges|split|merge`, the dashboard's range tables and `/api/*`):
  `/Min`, `/Max`, `/meta/...`, `/system/desc/7`,
  `/table/orders/primary/1000`, `/table/orders/by_city/"oslo"/42` —
  with table and index names and typed key values where the schema is
  known, IDs and shape-decoded values where it is not (`/table/3/1/1000`)
  — instead of escaped bytes (`"\x04\x00...\x80..."`).
- PostgreSQL catalogs and `SHOW` (#89): read-only `pg_catalog` and
  `information_schema` virtual tables over the live schema
  (`pg_database`, `pg_namespace`, `pg_class`, `pg_attribute`, `pg_type`,
  `pg_index`, `pg_constraint`, `pg_attrdef`, `pg_roles`, `pg_settings`,
  `pg_tables`, `pg_indexes`, `pg_collation`, `pg_tablespace`, ...,
  `information_schema.tables` / `columns` / `table_constraints` /
  `key_column_usage` / `statistics` / `role_table_grants`, and empty
  stand-ins for the catalogs of features datax lacks), the catalog
  functions tools call (`format_type`, `pg_get_indexdef`,
  `pg_get_constraintdef`, `pg_get_expr`, `pg_get_userbyid`,
  `pg_table_is_visible`, `current_setting`, `array_to_string`,
  `pg_size_pretty`, `has_*_privilege`, ...), and `SHOW COLUMNS FROM t`,
  `SHOW INDEXES FROM t`, `SHOW CREATE TABLE t`, `SHOW USERS`, `SHOW
  GRANTS [ON t] [FOR user]`, `SHOW TABLES FROM db`, `SHOW ALL` and `SHOW
  <setting>` (unknown settings are `42704`). psql's `\d`, `\dt`, `\di`,
  `\l`, `\du`, `\dn`, `\dp` and their `+` forms render; ORM introspection
  sees the schema. `server_version` now reports 14.0.
- SQL the catalog queries (and everyone else) needed: `UNION [ALL]`;
  `[NOT] LIKE` / `ILIKE`; `= ANY | SOME | ALL (array)` and `= ANY
  (SELECT ...)`; `||`; `CASE`; comparisons and boolean expressions as
  values; `CAST(x AS type)` and `::type` (absorbed) with
  `'name'::regclass` resolving a table; `E'...'` strings; regular
  expression operators `~ !~ ~* !~*`; `OPERATOR(pg_catalog.op)`;
  `COLLATE` (ignored); `ORDER BY` expressions and output aliases;
  parenthesized `JOIN ... ON` with non-equality conjuncts as join
  filters; `CROSS JOIN` and `FROM a, b`; `array(SELECT ...)`; `FROM
  unnest(array) AS s(x)`; scalar subqueries as predicates and inside
  `OR`; correlated subqueries in the select list, in `CASE` arms and in
  `array(...)`, and correlated subqueries over joins; any `f(...)` parses
  and an unknown function is `42883`.

## 0.10.0 — unreleased

### Added
- Databases (#88): `CREATE DATABASE`, `DROP DATABASE [CASCADE]`, `ALTER
  DATABASE ... RENAME TO`, `SHOW DATABASES`, `USE` / `SET database`,
  `current_database()` and `current_schema()`; the connection URL's
  database selects the session's database and an unknown one is refused
  with `3D000`; qualified names (`db.t`, `db.public.t`, `public.t`) in
  every statement that names a table; `GRANT CREATE | CONNECT | ALL ON
  DATABASE ... TO user | PUBLIC`, with CONNECT checked when a session
  opens a database and CREATE letting a non-admin create tables. A new
  cluster has `datax` and a reserved `system` database. `SHOW TABLES` and
  a bare `ANALYZE` act on the current database; the schema browser shows
  each table's database; backups carry the database catalog. **Cluster
  version v6**: descriptors gain a database ID and table names move
  under their database; until `datax debug upgrade` finalizes v6 every
  table stays in the flat namespace (which v5 nodes read) and database
  DDL is refused; finalize migrates the catalog in one transaction.

## 0.9.0 — unreleased

### Added
- `datax sql` is a line editor when run on a terminal: the up and down
  arrows recall earlier lines, kept across sessions in
  `~/.datax_sql_history` (or `$DATAX_SQL_HISTORY`, the last 1000 lines);
  Left/Right, Home/End and the usual control keys edit the line; `\?`,
  `\h` and `help` print the keys, meta-commands and statement families;
  `\dt` lists tables. Ctrl-D quits, or cancels a multi-line statement in
  progress. Piped input keeps the plain line-by-line reader. Adds the
  `golang.org/x/term` dependency (its `x/sys` sibling was already one).

## 0.8.0 — unreleased

### Added
- A node detail page on the dashboard (`/#/node/N`, from a click on the
  Nodes table): identity and versions, machine tiles, the node's last 15
  minutes of CPU, QPS, statements and KV latency from the metrics
  table, storage with the debt gate, overload verdict and encryption
  status, the replicas it holds with their raft log depth, its SQL
  summary and (for admins) statements, its network row, its settings
  and its recent events. `/api/node?id=N` serves the document: the
  serving node's own to any user, another node's through the internode
  RPC for admins (a new `node-detail` admin op). Sub-KiB byte figures on
  the dashboard are rounded (#86).

## 0.7.0 — unreleased

Cluster version **v5**: the `datax_metrics` system table. Clusters
upgraded from earlier versions record metrics only after
`datax debug upgrade` finalizes v5.

### Added
- The cluster records its own metrics: every node writes about fifty
  series (host, storage, ranges, the network matrix, transactions, SQL,
  and once per cluster the table gauges) every `--metrics-record-interval`
  (default 10 s) into `datax_metrics`, a sharded time-series table with
  a 7-day retention that the nodes create at a reserved descriptor ID
  once the cluster has finalized v5. History survives restarts, is
  queryable with plain SQL from any client, and `/api/metrics` serves it
  aligned and downsampled per node (rates for counters). The dashboard
  gains a Metrics view (`/#/metrics`): a time-range picker, a grouped
  series picker, one chart per series with one line per node, a
  crosshair readout, a per-node mode, and a table view; every overview
  tile links to its series and draws its sparkline from the table. The
  table is reserved (create, drop and column DDL refused; retention and
  shards settable; a `SELECT` grant for reporting users; only admins
  write), excluded from backups unless `datax backup --include-metrics`,
  and tolerated by restore. `ALTER TABLE ... SET (retention = '...')` now
  works for any time-series table. `/metrics` gains
  `datax_metrics_record_rows_total`, `datax_metrics_record_skipped_total`
  and `datax_metrics_record_errors_total` (#115).

## 0.6.0 — unreleased

### Added
- Health checks and an events feed on the dashboard: every node runs a
  fixed set of checks against data it already holds (node liveness and
  draining, mixed binaries and unfinalized upgrades, lost quorum,
  under-replication and locality diversity, `/meta` reachability,
  storage backpressure, debt gate, write stalls and errors, overloaded
  followers, disk, file-descriptor and memory headroom, peer
  reachability and clock offset, consistency failures, authentication
  failure rate, stale statistics) and shows the findings in a problems
  panel at the top of the page, each linking to the section with the
  figure. A per-node ring of operational events (splits, merges,
  auto-splits, rebalances, lease sheds, dead-node repairs, snapshots,
  decommissions, upgrades, key rotations, backups and restores,
  consistency failures; the audit stream for admins) feeds an events
  section with a kind filter. New endpoints `/api/health` and
  `/api/events?since=N`; `/metrics` gains
  `datax_health_problems{severity,check}` (#85).

### Fixed
- Scans and reverse scans retried stale range routing thirty times with
  no pause, so a read that met a range mid-move or mid-merge could
  exhaust its retries in microseconds and fail with "scan routing did
  not converge" while the meta repair was still landing. They now back
  off between retries the way batches already did (10 ms per retry
  after the third, capped at 200 ms).

## 0.5.0 — unreleased

### Added
- SQL activity on the dashboard: the wire server now accounts for its
  connections by state (idle, active, idle inside an open transaction
  with the age of the oldest), statements by kind, statement latency
  percentiles, serialization failures and COPY rows; the summary rides
  each node's heartbeat, the dashboard's SQL section shows per-node
  connections, statements per second, the statement mix, the `40001`
  rate and p50/p99, and admins see the serving node's statements in
  flight and its slowest recent ones (`/api/activity`, threshold
  `SlowStatementThreshold`, default 500 ms). `/metrics` gains
  `datax_sql_connections{state}`, `datax_sql_statements_total{kind}`,
  `datax_sql_statement_latency_seconds`,
  `datax_sql_serialization_failures_total` and
  `datax_sql_copy_rows_total` (#84).

## 0.4.0 — unreleased

### Added
- Schema browser on the dashboard and `/api/schema`: every table with
  its columns, primary key, indexes (and whether one is still being
  built), time-series options, grants, statistics with their age, and
  range footprint (ranges cluster-wide; replicas, leaders and bytes on
  the serving node); the users for admins; a filter box that narrows the
  tables and both range lists. Ranges in `/api/cluster` and `/status` now
  name the table their keys belong to. Secure mode shows a non-admin
  user only the tables it holds a grant on. `/metrics` gains
  `datax_table_ranges{table}`, `datax_table_rows{table}` and
  `datax_table_stats_age_seconds{table}` (#83).

### Fixed
- `/api/cluster` and `/status` on a node cut off from the meta range's
  leader answered only when the client gave up: the range listing
  retried until then. The listing is now bounded (2 s) and falls back to
  the last list the node fetched, with its age noted in `error`; the
  table-name refresh runs in the background and `/api/schema`'s catalog
  scan is bounded (5 s) and reports the catalog unavailable instead of
  hanging.

## 0.3.0 — unreleased

### Added
- Inter-node latency and clock offset: each node pings every peer every
  2 s (an NTP-style exchange yielding both the round trip and the peer's
  clock offset), advertises its row on the heartbeat, and the dashboard
  shows the whole matrix with offsets judged against `--max-offset`;
  `/metrics` gains `datax_rpc_rtt_seconds{peer}`,
  `datax_clock_offset_seconds{peer}` and `datax_peer_reachable{peer}`, and
  a node logs a warning once a peer's offset passes half the tolerance
  (#82). New internode RPC `Ping`; a node on an older binary answers
  "unimplemented" and reads as unreachable until upgraded.

## 0.2.0 — 2026-09-04

Cluster protocol v4 (ordered range-addressing repair). Binaries from
0.1.x can join a v3 cluster and are finalized to v4 with `datax debug
upgrade`.

### Added
- Machine-level metrics per node: each node samples its host (CPU, load,
  memory, the store disk's size, throughput and utilization, network,
  file descriptors, Go runtime) and advertises a summary on its
  heartbeat; the dashboard's Nodes table shows every node's figures with
  warning colors, a Machine section shows the local node in full, and
  `/metrics` exports `datax_node_*`, `datax_store_disk_*` and
  `datax_process_*` next to the standard Go and process collectors (#81).
- The dashboard shows who it is signed in as and, without the admin
  role, explains the range drill-down refusal in those terms (#79).
- `datax sql --certs-dir DIR --user NAME` connects with a client
  certificate, like `debug`, `backup` and `restore` (#77).
- Every CLI client reports progress while connecting, under a separate
  `--connect-timeout` (default 10 s), and names the address and cause on
  failure; `datax sql` previously had no connect timeout at all (#78).
- Cluster version v4: split and merge repair `/meta` with
  generation-ordered updates, so a late repair can no longer resurrect a
  stale record (#74).
- Staged store keys: `--enc-key old.key,new.key` lets a node start with
  either key after an online rotation; background re-encryption of files
  under retired keys with bounded chunks (#67, #69).
- `datax version` prints the release and the cluster protocol range the
  binary speaks.

### Fixed
- Online index builds could miss a row written by a gateway whose lease
  renewals had stalled: the descriptor cache now shares the lease
  record's expiration, transactions take the lease's expiration as a
  commit deadline, and the backfill's chunk reads cover the whole key
  span (#110).
- A merge that raced a split of its right-hand range absorbed the
  pre-split span and left two ranges claiming the same keys; the merged
  descriptor is now built from the right-hand range as it stands after
  the subsume, checked again at apply (#111).
- Meta lookups retry a record that does not cover the key (the transient
  state of an addressing repair) instead of failing the batch (#111).
- The intermittent re-shard stall: write evaluation reports every
  conflicting intent at once and the client pushes each blocker once;
  proposals orphaned across a coalesced leadership change are answered
  and re-sent (#74).
- Follower overload verdicts stay sticky until the follower reports
  healthy; overlapping store-key rotations are serialized; merge apply
  exits when the right-hand raft loop stops; replicas are all loaded
  before any raft loop starts at restart (#65, #66, #70).
- Re-encryption cost is bounded per chunk and reported per span; the
  cleartext-rejection assertion in the encryption tests is meaningful
  (#69, #71); wall-time comment, health cache atomics, debt-gate refresh
  (#72).

### Docs
- README states the cluster version and a Scope section in place of the
  prototype status; security, getting-started and operations guides
  cover certificate auth for `datax sql`, connection feedback, the
  signed-in badge and the host metrics.

## 0.1.0

The initial tree: MVCC storage over Pebble, Raft-replicated ranges with
splits and merges, serializable transactions with parallel commit,
rack-aware placement, secondary indexes, a cost-based planner, sharded
time-series tables with online re-shard, follower reads, encryption at
rest, TLS + SCRAM with admin authorization and audit logging, backup and
restore, rolling upgrades (cluster protocol v3), Prometheus metrics and
the dashboard.
