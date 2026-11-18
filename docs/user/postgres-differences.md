# Differences from PostgreSQL

datax speaks the PostgreSQL wire protocol and a subset of its SQL. Standard
drivers, `psql`, and ORMs that stay inside the subset work unmodified. This
page is the honest list of where the road ends, with workarounds.
(Maintainers: every known gap, with its priority and status, is kept in
the [compatibility gap register](../compatibility-gaps.md).)

## Things that will error immediately

All of these are rejected with a clear error (usually SQLSTATE `42601`
syntax error or `0A000` feature not supported):

| Missing | Workaround |
|---|---|
| Functions beyond the [Functions reference](functions.md) (`SHOW FUNCTIONS`) — SQLSTATE `42883`; `row_to_json` and other whole-row functions; named arguments (`make_interval(days => 1)`); a set-returning function (`generate_series`, `jsonb_array_elements`, ...) in the select list of a query with `FROM`, or with arguments that read another `FROM` member (`0A000`); regular expressions with back references or lookaround (`2201B`: datax runs them on RE2); `TIME WITH TIME ZONE`; `INTERVAL` field qualifiers as a constraint (`INTERVAL DAY TO SECOND` stores the full triple); `IntervalStyle` other than `postgres` in `::text` casts, arrays and JSON (the wire's text format of an `INTERVAL` column follows it); user-defined functions | compute client-side or in a derived table; the regular-expression, set-returning and jsonb processing functions **are supported** ([set-returning functions](sql.md#set-returning-functions)); `INTERVAL` and `TIME` **are supported** ([types](sql.md#types)) |
| `EXPLAIN` options in parentheses (`FORMAT JSON`, `VERBOSE`, `BUFFERS`); `RANGE` frames with a numeric offset, `GROUPS` frames, `EXCLUDE`, `FILTER` on a window call; correlated subqueries over set operations | `EXPLAIN ANALYZE` (stage rows and times), expressions over aggregates and `GROUP BY` expressions, `ROLLUP` / `CUBE` / `GROUPING SETS`, `DISTINCT ON`, `FILTER (WHERE ...)` and `DISTINCT` on aggregate calls, window functions (ranking, offset, value and aggregate, with `PARTITION BY`, `ORDER BY`, `ROWS` frames and `WINDOW` names), `WITH` / `WITH RECURSIVE` (data-modifying members too), `INSERT ... SELECT`, `OFFSET`, `FETCH FIRST`, `UNION` / `INTERSECT` / `EXCEPT [ALL]`, `RIGHT` / `FULL` / `NATURAL` joins and `USING` **are supported** ([reference](sql.md#reading)) |
| `DEFERRABLE` constraints, `ON DELETE SET DEFAULT`, `MATCH FULL`, `ADD PRIMARY KEY`, `EXCLUDE` | `CHECK`, `UNIQUE` and `FOREIGN KEY` constraints **are supported** ([reference](sql.md#constraints-check-unique-and-foreign-key)), checked at statement end; the referencing side of a foreign key gets an index automatically, and a cascade is capped per statement (`foreign_key_cascade_limit`) |
| A volatile `DEFAULT` (`gen_random_uuid()`, `random()`), `SERIAL`, or a constraint on `ADD COLUMN`, and `ALTER TABLE` actions that read the rows (`ADD CONSTRAINT`, `SET NOT NULL`, `ALTER COLUMN TYPE`), inside a transaction block (`25001`) | run the statement outside the block (Rails: `disable_ddl_transaction!`); `ADD COLUMN` with any default, several actions in one `ALTER TABLE`, and the descriptor-only actions in a block **are supported** ([reference](sql.md#ddl)) |
| `ADD PRIMARY KEY` (a table created without one is keyed by a hidden `rowid`, [reference](sql.md#tables-without-a-primary-key); `0A000`) / `DROP` of the primary key, `ALTER TABLE ... SET SCHEMA`, `RENAME TO` across databases, `ALTER COLUMN TYPE ... USING expr` and conversions across type families (`FLOAT8` to `INT8`), `COMMENT ON` objects other than tables, views, indexes, columns and constraints, `SELECT ... INTO` | `DROP INDEX`, `ALTER INDEX RENAME`, the `RENAME` forms, `TRUNCATE`, `IF [NOT] EXISTS` everywhere, `ALTER COLUMN TYPE` (an online rewrite for widening, text conversions and narrower modifiers of the same family — `INT8` to `INT4`, a shorter `VARCHAR`, which check every stored value: `22003` / `22001`), `CREATE TABLE ... AS` (a hidden `rowid` key unless `PRIMARY KEY (cols)` is written among the column names), `CREATE TABLE ... (LIKE t INCLUDING ...)` and `COMMENT ON TABLE / VIEW / INDEX / COLUMN / CONSTRAINT` **are supported** ([reference](sql.md#ddl)); recreate the table for the rest |
| `COPY FROM` options beyond `FORMAT`; `COPY TO` a server file or program; `COPY ... ENCODING` other than `UTF8` | `COPY t FROM STDIN` **is supported** (text, CSV, binary — psql `\copy` and pgx `CopyFrom` work), and so is `COPY ... TO STDOUT` with every option ([reference](sql.md#exporting-with-copy-to)): use `\copy ... to` for a client-side file |
| Schemas | `public` is the only schema: `db.public.t` and `public.t` are accepted, any other schema name is an error; `search_path` is accepted and ignored. Databases are real (`CREATE DATABASE`, the URL's database, `USE`, `SET database`, `current_database()`); see [Databases](sql.md#databases) |
| Materialized views, `WITH CHECK OPTION`, updatable views, `SECURITY INVOKER` views, renaming a table or column a view reads (refused; replace the view) | Views **are supported** ([reference](sql.md#views)): `CREATE [OR REPLACE] VIEW`, `DROP VIEW [CASCADE]`, dependency tracking, definer semantics (the query runs with the owner's privileges), `pg_views`, `information_schema.views`, `\dv` |
| Column-level privileges, `REFERENCES` / `TRIGGER` / `MAINTAIN` privileges, `CREATEDB` / `CREATEROLE` / `SUPERUSER` role attributes, `VALID UNTIL`, `CONNECTION LIMIT`, `ALTER ROLE ... SET`, `SET SESSION AUTHORIZATION`, `pg_hba.conf` | Roles **are supported** ([security guide](security.md#roles-and-privileges)): `CREATE / ALTER / DROP ROLE` and `USER` (`LOGIN`, `PASSWORD`, `INHERIT`, `IN ROLE`), membership with `ADMIN OPTION`, `SET ROLE`, ownership (`OWNER TO`, `REASSIGN OWNED`, `DROP OWNED`), grants on tables, views, sequences, databases and `public` with `GRANT OPTION`, `ALL TABLES IN SCHEMA`, `ALTER DEFAULT PRIVILEGES`, `PUBLIC`, the built-in `admin`, `read_all`, `write_all` and `metrics` roles; `CREATE DATABASE` and role management take `admin`; the `BYPASSRLS` attribute |
| Subqueries in a policy expression, `SECURITY INVOKER` views over protected tables, policies on views | Row-level security **is supported** ([reference](sql.md#row-level-security), [security guide](security.md#row-level-security)): `ENABLE` / `DISABLE` / `FORCE` / `NO FORCE ROW LEVEL SECURITY`, `CREATE` / `ALTER` / `DROP POLICY` (permissive and restrictive, `FOR`, `TO`, `USING`, `WITH CHECK`), default deny, custom settings with `current_setting` / `set_config`. See below for where it differs |
| The fast-path function call for functions other than the large-object ones; `lo_import` / `lo_export` (server-side files), `GRANT ... ON LARGE OBJECT`, `ALTER LARGE OBJECT`, `lo_compat_privileges`, the `pg_largeobject` table itself, `COMMENT ON LARGE OBJECT` | Large objects **are supported** ([reference](sql.md#large-objects)): `lo_create`, `lo_creat`, `lo_open`, `lo_close`, `loread`, `lowrite`, `lo_lseek[64]`, `lo_tell[64]`, `lo_truncate[64]`, `lo_unlink`, `lo_get`, `lo_put`, `lo_from_bytea`, and `pg_largeobject_metadata`; import and export client-side (psql's `\lo_import` / `\lo_export` do) |
| Triggers, stored procedures | — (`LISTEN` / `NOTIFY` / `pg_notify` **are supported**, across nodes: [reference](sql.md#notifications-listen-and-notify); so are advisory locks, `pg_advisory_lock` and its relatives, cluster-wide: [reference](sql.md#advisory-locks)) |

## Things that exist but behave differently

- **Serializable only.** No isolation levels to choose; every transaction
  can fail with `40001` and clients **must** retry
  ([details](sql.md#transactions-and-retries)). Code written for
  PostgreSQL's default read-committed often has no retry loop — it needs
  one here.
- **`FOR UPDATE SKIP LOCKED` and `NOWAIT`** work, with two differences
  that follow from serializable isolation
  ([details](sql.md#not-waiting-for-a-lock-skip-locked-and-nowait)).
  `SKIP LOCKED` also passes over a row another transaction changed and
  committed after this transaction's snapshot — PostgreSQL's
  read-committed would lock the new version; here the row is skipped
  rather than failing the statement with `40001` — and a transaction
  block sees the table as of its snapshot, so rows inserted after
  `BEGIN` do not appear. `NOWAIT` can fail with `55P03` on a locked row
  the statement had to read but would have filtered out. `FOR SHARE`,
  `FOR NO KEY UPDATE`, `FOR KEY SHARE` and `FOR UPDATE OF table` are
  not supported.
- **Advisory locks** (`pg_advisory_lock`, `pg_try_advisory_lock`,
  `pg_advisory_xact_lock`, the `_shared` variants, `pg_advisory_unlock[_all]`)
  work cluster-wide with PostgreSQL's semantics, with four differences
  ([details](sql.md#advisory-locks)): the locks of a node that dies are
  released about 10 seconds after it stopped renewing its lease, and a
  node cut off from the cluster ends its sessions that held advisory
  locks (`57P01`) rather than let two sessions hold one lock; waiters
  are not served in arrival order; a deadlock between advisory locks is
  not detected (set `lock_timeout`); and the arguments must be constants
  or parameters, evaluated once per statement (`0A000` for a column).
- **Concurrent inserts of one key.** In PostgreSQL the second `INSERT`
  of a primary key another open transaction has just inserted waits for
  it, then fails with `23505` if it commits or succeeds if it rolls back.
  Here the second usually waits too and fails with `23505` (about two
  thirds of the losers when four transactions race for one key), but it
  can also get `40001`: at the `INSERT`, when the wait could only end in
  its own restart (the key it checked was taken after its snapshot); or
  after `INSERT 0 1`, at a later statement or the `COMMIT`, when the
  other insert had the priority to abort it. Two of them never both
  commit. Retrying a `40001` finds the committed row and reports `23505`,
  so an insert-or-fail loop that already retries `40001` behaves as in
  PostgreSQL; code that only expects `23505` there needs that retry.
- **Type modifiers are enforced** — `DECIMAL(p,s)` (rescale to `s`
  half-even, `22003` on overflow, fixed-scale rendering), the integer
  widths (`INT` / `INTEGER` is `INT4` as in PostgreSQL; `SERIAL` too),
  `VARCHAR(n)` / `CHAR(n)` (`22001`), `TIMESTAMP` without time zone and
  `TIMESTAMP(p)` — with small deviations: an invalid typmod
  (`DECIMAL(0)`, scale > precision, `VARCHAR(0)`, `TIMESTAMP(7)`) is a
  syntax error `42601` rather than PostgreSQL's `22023`; typmods on
  other types (`FLOAT8(3)`) are accepted and ignored; a `CHAR(n)`
  value is stored trimmed, so `c = 'ab  '` compares the literal with
  its spaces (PostgreSQL trims both sides) while `c = 'ab'`, `length`
  and `||` agree; expression results lose the modifier and describe
  as the family (`a + 1` over an `INT4` is `INT8`, `ts + '1 day'` over
  a `TIMESTAMP` stays a `TIMESTAMP`, as `::timestamp` yields one, `GROUPING()` is `INT8`),
  and a bind parameter for a `TIMESTAMP` column describes as
  `timestamptz` (pass UTC); expression/aggregate results render in
  canonical form (no declared scale), as in PostgreSQL.
- **Enums** are append-only (`ADD VALUE BEFORE` / `AFTER`, `RENAME
  VALUE`, `RENAME TO` are refused; `DROP TYPE ... CASCADE` does not
  drop columns), have no arrays, and compare with text by label
  (`m = 'ok'`; an order against a text literal is the labels' text
  order — cast the literal, `m > 'ok'::mood`, for declaration order).
  Creating a type takes the admin role or `CREATE` on the database;
  altering or dropping one takes its owner.
- **Arrays** are one-dimensional (`INT8[][]` is `INT8[]`; a
  multidimensional binary parameter is refused), cannot be indexed or
  be part of a key, have no slices (`a[1:2]`) or `JSONB[]`, and a
  subscript out of range is NULL as in PostgreSQL; `array_agg` over an
  untyped argument yields `text[]`; `unnest` runs in `FROM` (laterally
  too: `FROM t, unnest(t.tags) AS tag`) or in a FROM-less select list
  (not beside a table's columns).
- **Intervals** compare, group and index by PostgreSQL's rule (`'30
  days' = '1 month'`), but a primary key or unique index keeps `'30
  days'` and `'1 month'` as distinct keys (the stored triple is part of
  the key); `extract` on an interval reports the stored fields
  unjustified, as in PostgreSQL. A `TIME` input's offset is ignored;
  `TIME WITH TIME ZONE` is refused at parse (`42601`).
- **Bare decimal literals are DECIMAL** — same as PostgreSQL, but note
  `SELECT 1.5` describes as `NUMERIC`, not `float8`.
- **JSONB**: `->`/`->>` extraction (single-table queries, joins and
  grouped select lists), containment `@>` / `<@` and key
  existence `?` / `?|` / `?&` (single-table only), equality comparison,
  and `IS [NOT] NULL`; no ordering comparisons. `@>` is not accepted in
  `HAVING` or after a computed left-hand side. `jsonb || jsonb` merges
  as in PostgreSQL. `json` and `jsonb` are one type in datax (a `JSON`
  column is stored as `JSONB`), so `json || json` merges too, where
  PostgreSQL has no `||` on `json` and refuses it (`42883`). `-`, `#-` and subscripting (`j['a']`) are refused.
- **GIN indexes: JSONB and full-text search.** `CREATE INDEX ... USING GIN
  (col)` on a JSONB column serves `@>`, `?`, `?|` and `?&`. It indexes
  each value with its path (closer to PostgreSQL's `jsonb_path_ops` than
  to `jsonb_ops`, but it serves the key operators too), so
  `jsonb_path_ops` is accepted as a synonym of `jsonb_ops`, and
  `pg_get_indexdef` reports the index without an operator class. Not
  served, and evaluated as filters: `<@`, jsonpath (`@?`, `@@` do not
  exist), containment under a `->` path, and a right-hand side that is
  not a constant or a parameter. No GIN over arrays or
  `pg_trgm`; no multi-column or `UNIQUE` GIN; no `fastupdate` or other
  storage parameters. A document with more than 8,192 distinct indexed
  values is refused (`54000`) on a table with a GIN index. There is no
  bitmap scan: `EXPLAIN` reports an `inverted scan`, and
  `enable_bitmapscan` is accepted and changes nothing
  (`inverted_index_scan` decides).
- **Full-text search is a subset, and its parser and stemmer are
  datax's own.** `tsvector`, `tsquery`, `@@`, `to_tsvector`,
  `to_tsquery`, `plainto_tsquery`, `phraseto_tsquery`,
  `websearch_to_tsquery`, `ts_rank`, `ts_rank_cd`, `ts_headline`,
  `setweight`, `strip`, `numnode`, `tsvector_to_array`,
  `get_current_ts_config`, `tsvector || tsvector`, weights, phrases and
  prefixes work, and a GIN index serves `@@`
  ([reference](sql.md#full-text-search)). Where it differs:
  - *Configurations*: `english` and `simple` only. No `CREATE TEXT
    SEARCH ...`, no dictionaries, thesauri or `unaccent`; `regconfig` is
    not a type (`'english'::regconfig` is accepted and is text).
  - *Tokens*: the parser is simpler than PostgreSQL's. A hyphenated word
    is its parts only (PostgreSQL also emits the whole: `well-known`
    here is `well`, `known`); a URL is its scheme, host and path words,
    not PostgreSQL's `url` / `host` / `url_path` tokens; signed and
    scientific numbers, XML tags and entities are not recognized
    (`-3` is `3`, `1e5` is the word `1e5`); a version number such as
    `1.2.3` is `1.2` and `3`. Positions therefore differ from
    PostgreSQL's wherever these occur. Plain prose tokenizes the same.
  - *Stemming*: the Snowball English (Porter2) algorithm as long
    published, with PostgreSQL's 127 English stop words. PostgreSQL 17
    and later ship a newer revision of the Snowball sources that stems
    a handful of words differently. The implementation is checked
    against the algorithm's published sample vocabulary and hand-derived
    stems, not against a PostgreSQL server.
  - *Indexes*: `USING GIN (tsvector_column)` and `USING GIN
    (to_tsvector('config', text_column))` — the one expression an index
    may be built on in a GIN index; the configuration must be written
    out, as PostgreSQL requires. A GIN index on any other expression
    (a concatenation of columns, `setweight(...) || ...`) is refused —
    expression keys are for ordinary (btree) indexes — and there are no
    generated columns and no triggers to maintain a `tsvector` column:
    the application writes it. No GiST (`USING GIST` is refused), no
    `fastupdate`, no `gin_pending_list_limit`.
  - *Planning*: a negated term is never looked up, so a query that is
    only a negation (or an `OR` with one) scans; `tsquery @@ tsvector`
    (the query on the left) is evaluated as a filter. `ORDER BY ts_rank
    ... LIMIT n` ranks every matching row: there is no top-k shortcut
    (PostgreSQL has none with GIN either; RUM indexes do).
  - *Operators and functions not provided*: `text @@ tsquery` and `text
    @@ text` (refused with `0A000` rather than guessed at: wrap the text
    in `to_tsvector`), `tsquery && tsquery`, `||`, `!!`, `@>`, `<@`,
    `tsquery_phrase`, `querytree`, `ts_rewrite`, `ts_delete`,
    `ts_filter`, `ts_stat`, `ts_debug`, `ts_lexize`, `ts_parse`,
    `array_to_tsvector`, `unnest(tsvector)`, `length(tsvector)` (it
    returns the length of the text), `json(b)_to_tsvector`, and the `\dF`
    commands' catalogs (`pg_ts_config` and friends). No ordering of
    `tsvector` or `tsquery` values (`ORDER BY`, `<`), only equality; no
    arrays of them.
  - *Output*: `to_tsquery` does not rewrite a phrase over an `|` or `&`
    into PostgreSQL's normal form (`'a' <-> ( 'b' | 'c' )` stays as
    written where PostgreSQL prints `'a' <-> 'b' | 'a' <-> 'c'`; the two
    match the same documents). `ts_rank` and `ts_rank_cd` follow
    PostgreSQL's formulas and return `float8` rounded to single
    precision. `ts_headline` chooses its fragment by a simpler rule
    (the window with the most distinct query words), always returns one
    fragment (`MaxFragments`, `ShortWord` and `FragmentDelimiter` are
    accepted and ignored), and does not strip markup.
  - *Limits*: a row in a full-text index may have at most 8,192 distinct
    lexemes (`54000`); PostgreSQL's limits on lexeme length (2,046
    bytes), positions (16,383) and positions per lexeme (256) apply.
  One asymmetry to know: `@>` compares
  numbers **numerically** (`1` contains `1.0`), while jsonb `=` compares
  normalized text (`'{"a":1}' = '{"a":1.0}'` is false).
- **Vectors follow pgvector, built in, with one index method.**
  `vector(n)`, the `<->`, `<=>` and `<#>` operators, `l2_distance`,
  `cosine_distance`, `inner_product`, `l1_distance`, `vector_dims`,
  `vector_norm`, `l2_normalize`, element-wise `+` and `-`, the text and
  binary wire formats, and `CREATE INDEX ... USING ivfflat (col
  vector_l2_ops | vector_cosine_ops | vector_ip_ops) WITH (lists = n)`
  with `SET ivfflat.probes` behave as in pgvector
  ([Vectors](sql.md#vectors)). What differs:
  - *There is no extension to install.* `CREATE EXTENSION vector` is
    accepted and does nothing, `DROP EXTENSION vector` too, and
    `pg_extension` always lists it (see *Extensions* below);
    `vector_version()` does not exist. The
    type's OID is fixed (16000) rather than assigned at install time;
    `to_regtype('vector')` returns the OID as a number, and
    `'vector'::regtype` does not resolve — look the OID up with
    `to_regtype('vector')::oid` or in `pg_type`, as `pgvector-go` does.
  - *No HNSW.* `USING hnsw` is refused (`0A000`) rather than built as
    something else; `hnsw.ef_search` is not a setting. A graph index
    suits a transactional, range-partitioned store poorly; `ivfflat` is
    the index.
  - *`ivfflat.probes` defaults to the square root of `lists`*, not to 1:
    the default is a usable recall (about 0.98 on the test set) instead
    of the fastest setting. `SET ivfflat.probes = 1` gives pgvector's
    default. `ivfflat.iterative_scan` and `ivfflat.max_probes` do not
    exist; a filtered query already considers every row of the probed
    lists, nearest first, rather than only the nearest `k`.
  - *The index serves one query shape*: `ORDER BY col <op> value LIMIT
    k` over one table, ascending, the only sort key. Not through a
    join, a `GROUP BY`, `DISTINCT`, `FOR UPDATE` or a distance in
    `WHERE` alone; those run exactly. A filter that the primary key or
    an ordinary index can serve takes that path, exactly, in preference
    to the vector index.
  - *The index is not used without `LIMIT`*, and there is no cost model
    choosing between it and a scan: when the shape fits, it is used
    (`SET vector_index_scan = off` forces the exact scan, as `SET
    enable_indexscan = off` does in PostgreSQL).
  - *No `REINDEX`*: rebuild by creating a second index and dropping the
    first. `CREATE INDEX` is always online (`CONCURRENTLY` is accepted
    and changes nothing), and an index built on little or no data
    gets fewer lists than asked for — one, on an empty table — instead
    of pgvector's notice about low recall. `pg_indexes` shows the
    number of lists actually built.
  - *A vector cannot be a primary key or in a btree or unique index*
    (pgvector allows btree). `=`, `<>`, `<`, `ORDER BY`, `GROUP BY` and
    `DISTINCT` on a vector do work.
  - *Not there*: `halfvec`, `sparsevec`, `bit` vectors and their
    operators (`<+>` L1 as an operator, `<~>`, `<%>`), `vector *
    vector`, `||`, `subvector`, `binary_quantize`, and the `avg` and
    `sum` aggregates over vectors (`0A000`). An ivfflat index takes
    no expression (`(embedding::halfvec(3))`) and no `WHERE` clause.
    `vector[]` columns are refused.
  - *Dimensions*: 16,000 for the type and 2,000 for an index, as in
    pgvector. Elements are float4; a value outside float4's range is
    refused, as are `NaN` and infinities.
  - *NaN ordering*: the cosine distance to a zero vector is `NaN`,
    which sorts after every number (as PostgreSQL orders `float8`); an
    indexed scan can return such a row, last, where pgvector's cosine
    index leaves zero vectors out.
- **`OR` is supported with full boolean grouping**, but `OR` conditions
  never become index bounds (they filter fetched rows) — keep an
  indexable `AND` condition alongside on large tables. `IN`, `EXISTS`
  and scalar subqueries may appear inside `OR`. Computed left-hand sides
  (`qty * 2 > 10`) work in single-table queries and joins.
- **`LIKE` / `ILIKE` and regular expressions (`~`, `~*`)** filter fetched
  rows; there is no pattern-prefix index optimization.
- **Join order is execution order until statistics exist** — with
  statistics for every joined table (`ANALYZE` or the background
  sampler), INNER joins are cost-reordered greedily; LEFT joins,
  self-joins, cross joins and joins carrying ON filters, correlated
  subqueries or `LATERAL` members always run in the written order, so
  put the most selective table first there. `ON` may be any boolean
  condition (`ON true`, `u.tid < t.id`, `BETWEEN`, `OR`, a condition
  on one side, expressions), for every join type. An `ON` with an
  equality between the joined table and an earlier one looks rows up
  by it (through an index when there is one) and applies the rest per
  match; one without is a nested loop over the joined table, read once
  and tried against every row so far, so it costs rows × rows: add an
  equality on large tables where you can.
- **Correlated subqueries are nested loops.** Each is re-run per row of
  the enclosing query (memoized on the referenced values), so a
  correlated subquery over a large outer table costs outer rows × inner
  query. Prefer a `JOIN` when the outer side is big. A `LATERAL`
  subquery or a table function reading another `FROM` member's columns
  is the same nested loop. A lateral subquery must be a plain select
  (not over a derived table or a set operation of its own) and cannot
  read the outer row in its `ORDER BY`, `GROUP BY` or aggregate
  arguments (`0A000`); `RIGHT` / `FULL JOIN LATERAL` is refused
  (`42P10`) even when the subquery reads nothing to its left.
- **Casts are performed** (`'42'::int`, `3.7::int8`, `x::text`,
  [reference](sql.md#reading)). A cast to a type datax does not have —
  `citext`, `hstore`, `money`, `xml`, `inet`, `pg_lsn`, a range type, or
  a name that is no type at all — is refused as in PostgreSQL, `42704
  type "citext" does not exist`, before the statement reads anything.
  The catalog types tools cast to (`name`, `oid`, `regtype`, `xid`,
  `"char"`, ...) are accepted and behave as their text or number. A
  schema qualifier on a cast's type is ignored (`'1'::foo.int4` is an
  `int4`). And `'t'::regclass` displays as the table's OID (a number,
  where PostgreSQL displays `t`); `oid::regclass` gives the name back
  (`conrelid::regclass`). The name is read as PostgreSQL reads one
  (`'"Mixed"'::regclass`, `'public.t'::regclass`), and a system
  catalog's OID is PostgreSQL's (`'pg_class'::regclass` is 1259). The
  `to_regclass`, `to_regproc`, `to_regprocedure`, `to_regnamespace`
  and `to_regrole` lookups return the OID the same way (`NULL` where
  the cast would fail), not the name.
- **`COPY FROM STDIN` is not atomic.** It commits in chunks (128 rows /
  1 MiB) as the stream arrives, so a failure partway leaves the earlier
  chunks committed — the error names the failing row and how many rows
  were already committed. For the same reason COPY is refused inside
  `BEGIN` (SQLSTATE `25001`) and only the `FORMAT` option is accepted.
- **`ON CONFLICT`** works on the primary key and unique indexes (by
  columns or `ON CONSTRAINT <table>_pkey | <index name>`), with `DO
  NOTHING`, `DO UPDATE SET ... [WHERE]` and `EXCLUDED`; `DO UPDATE` cannot
  change a primary-key column (a plain `UPDATE` can), and there is no `ON
  CONFLICT ... DO UPDATE` over exclusion constraints (there are none).
- **Unique keys are checked at the end of an `UPDATE`**, as the SQL
  standard (and PostgreSQL for a `DEFERRABLE` constraint) has it:
  `UPDATE t SET id = id + 1` succeeds where PostgreSQL, checking a
  non-deferrable key row by row, fails with `23505`. Where a foreign
  key references its own table with `ON UPDATE CASCADE` or `SET NULL`,
  an `UPDATE` that changes both a referenced row and a row the cascade
  rewrites is refused (`0A000`); PostgreSQL cascades after the
  statement. `UPSERT INTO`
  (CockroachDB syntax) is the primary-key shorthand.
- **`UPDATE ... FROM` and `DELETE ... USING`** work as in PostgreSQL
  ([reference](sql.md#writing)), with two differences. A target row
  that joins several rows takes its new values from the first of them
  in the order the join reads the `FROM` list (a table by primary key,
  or by the index it is looked up through; a `VALUES` list as
  written), where PostgreSQL uses an unspecified one. The target needs
  only the `UPDATE` (`DELETE`) privilege, as a plain `UPDATE ... WHERE`
  does here; PostgreSQL also asks for `SELECT` on the target columns
  the statement reads.
- **`CREATE INDEX` is always online** (like `CONCURRENTLY`, which is
  accepted and changes nothing) and cannot run inside a transaction
  block (`25001`) — except on a table created in the same block, where
  it is built in the block and commits or rolls back with it (a Rails or
  Django migration's `create_table` and `add_index`). As in PostgreSQL,
  `CREATE INDEX CONCURRENTLY` and `DROP INDEX CONCURRENTLY` are refused
  in any block. The index name may be left out (`<table>_<columns>_idx`).
  There is no `REINDEX`.
- **Index keys compare bytewise**, the order of the `C` collation. The
  string types' operator classes (`text_pattern_ops`,
  `varchar_pattern_ops`, `bpchar_pattern_ops`, and the defaults
  `text_ops`, `varchar_ops`, `bpchar_ops`) and `COLLATE "C"`, `"POSIX"`
  and `"default"` are accepted on an index column and shown by
  `pg_get_indexdef` / `pg_indexes`; Django's `..._like` index works as
  written. Any other collation is refused in an index (`0A000`), as is
  any other operator class on an ordinary index; `pg_index.indclass`
  and `indcollation` are not modeled, and `pg_opclass` is empty. The
  pattern classes do not make `LIKE 'abc%'` use the index (below).
- **Extensions: `plpgsql`, `pgcrypto`, `uuid-ossp` and `vector` are
  built in.** `CREATE EXTENSION [IF NOT EXISTS] name [WITH] [SCHEMA s]
  [VERSION v] [CASCADE]` accepts them and does nothing, and so does
  `DROP EXTENSION [IF EXISTS]`: they cannot be removed. `pg_extension`
  and `pg_available_extensions` always list all four as installed, so
  `CREATE EXTENSION` without `IF NOT EXISTS` succeeds where PostgreSQL
  would say the extension already exists (`42710`). What each provides
  is limited: `pgcrypto` is `gen_random_uuid()` only — `digest`,
  `hmac`, `crypt`, `gen_salt`, `gen_random_bytes` and the `pgp_*`
  functions do not exist — and `uuid-ossp` is `uuid_generate_v4()` only
  (no `uuid_generate_v1`, `v1mc`, `v3`, `v5`, `uuid_nil` or the
  `uuid_ns_*` constants). Any other extension is refused (`0A000`);
  `DROP EXTENSION` of one is `42704`.
- **`ORDER BY`** takes output names, positions and expressions, but an
  expression sort never uses an index, an expression index included;
  NULLs order last ASC / first DESC (PostgreSQL default). An index's
  declared `DESC` / `NULLS FIRST` is shown but not stored: keys are
  ascending, an all-descending `ORDER BY` reverses the scan and a mixed
  one sorts ([reference](sql.md#ddl)).
- **Expression and partial indexes** are supported, with limits
  ([reference](sql.md#ddl)): a partial index is used only when its
  predicate's conjuncts appear verbatim in the query's WHERE clause (no
  implication proofs); a predicate may compare a date or time column
  only with `IS [NOT] NULL`; anything zone-dependent in an expression
  (`date_trunc`, casts of timestamps) is refused even where PostgreSQL
  marks the form immutable (`timestamp without time zone`); `ON
  CONFLICT (cols) WHERE predicate` is not parsed, so a partial unique
  index is never an `ON CONFLICT` arbiter except by name (`ON
  CONSTRAINT`); and `pg_get_indexdef` shows an expression and a
  predicate as written, without PostgreSQL's inserted casts
  (`'active'::text`). Under `UNION`, output names and positions
  only. A position after a `*` that is not the whole select
  list is refused.
- **Time zones**: `TimeZone` governs `TIMESTAMPTZ` as in PostgreSQL
  (input without an offset, text output, `::text`, `::date`,
  `date_trunc`, `extract`, `to_char`, `current_date`, day and month
  arithmetic), and `AT TIME ZONE` / `timezone()` convert both ways; the
  differences: abbreviations are a fixed list of the common ones, a
  join's `ON` condition reads a literal without an offset in UTC (put
  such a filter in `WHERE`), and `to_jsonb`, `array_to_string` and
  `format` render a `TIMESTAMPTZ` in UTC. Microsecond precision on the binary wire, nanosecond internally — and
  in text output, where computed values such as `now()` show nine
  fractional digits (`13:18:04.787705724+00`) where PostgreSQL shows at
  most six. `current_time` is a
  `time` (PostgreSQL's is `timetz`), and `localtimestamp` a
  `timestamptz` like `current_timestamp`.
- **`EXPLAIN`** returns one plain-text line, not a plan tree (`EXPLAIN
  ANALYZE` adds a line per stage), and explains `SELECT` only.
- **`ANALYZE`** exists but is admin-only (PostgreSQL allows table
  owners), takes no column list or options, and stores row counts +
  distinct estimates only — no histograms, no `pg_statistic`. `SHOW
  STATS FOR t` is the (non-PostgreSQL) way to inspect them.
- **`AS OF SYSTEM TIME`** exists (CockroachDB syntax, not PostgreSQL) —
  cheap historical/follower reads, including bounded staleness:
  `AS OF SYSTEM TIME with_max_staleness('10s')` reads the freshest data
  the local replicas can serve within the bound.
- **`LISTEN` / `NOTIFY`** behave as in PostgreSQL on one node and
  reach listeners on every node, with these differences
  ([reference](sql.md#notifications-listen-and-notify)): a notification
  goes to the sessions listening when it commits and is kept nowhere,
  and a listening session receives every one or is disconnected with a
  FATAL error — because it let 16 MiB of notifications pile up unread
  (`54000`), or because another node had to drop notifications it could
  not get to this one (`57P01`) — so a client that is still connected
  has missed nothing, with one exception: a node that crashes can take
  with it the notifications of the transactions it committed in its
  last instant. Notifications from one node arrive in that node's
  commit order; notifications from different nodes are not ordered
  against each other (PostgreSQL has one order for the whole server).
  `pg_notify` is evaluated once per statement and its arguments cannot
  name columns. There is no `pg_listening_channels()` or
  `pg_notification_queue_usage()`.
- **`SET x = y`** honors the variables datax has ([Session
  settings](sql.md#session-settings): the timeouts, `TimeZone`,
  `application_name`, `search_path`, the read-only and isolation
  characteristics, `DateStyle` and `client_encoding` in their supported
  values), knows the PostgreSQL settings that frameworks, drivers and
  `pg_dump` send (below), and refuses an unknown one with `42704` (an
  invalid value is `22023`), where PostgreSQL knows hundreds more.
  `transaction_isolation` and `default_transaction_isolation` accept
  every level and `SHOW` reports `serializable`, the only one there is.
  `server_version` reports a PostgreSQL-14-compatible string (and
  `server_version_num` `140000`), so `psql` and drivers use their
  PostgreSQL-14 query flavors.
- **Settings accepted with no effect.** These take PostgreSQL 18's
  values, are checked as PostgreSQL checks them, are kept per session
  and reported by `SHOW`, `current_setting()` and `pg_settings`, and
  `RESET` restores their defaults, but datax does not act on them:
  `check_function_bodies`, `xmloption`, `default_tablespace` (only
  `''`, `pg_default` and `pg_global` exist), `work_mem`,
  `maintenance_work_mem`, `effective_cache_size`, `random_page_cost`,
  `seq_page_cost`, `jit`, `geqo`, the planner's `enable_seqscan`,
  `enable_indexscan`, `enable_indexonlyscan`, `enable_bitmapscan`,
  `enable_hashjoin`, `enable_mergejoin`, `enable_nestloop`,
  `enable_sort`, `enable_hashagg` and `enable_material`,
  `synchronous_commit` (every commit is durable and replicated),
  `default_transaction_deferrable` and `SET SESSION CHARACTERISTICS AS
  TRANSACTION DEFERRABLE`, `lc_messages`, `lc_monetary`, `lc_numeric` and
  `lc_time` (messages, numbers, money and dates stay in the C locale;
  `C`, `POSIX` and names shaped like `en_US.UTF-8` are accepted),
  `escape_string_warning`, `array_nulls` (`off` does not make a
  `NULL` in array input a string), `quote_all_identifiers`,
  `synchronize_seqscans` (pg_dump sets it `off`) and
  `session_replication_role` (`replica` does not disable foreign keys).
  `default_with_oids` takes only `off`, as in PostgreSQL 12 and later.
  The ones that do act: `IntervalStyle` (`postgres`, `iso_8601`,
  `sql_standard`, `postgres_verbose`), `bytea_output` (`hex`,
  `escape`) and `extra_float_digits` (`1` to `3` print a `FLOAT8` in its
  shortest exact form, `0` and below round it to `15 + n` digits) set
  the text format of `INTERVAL`, `BYTEA` and `FLOAT8` columns on the
  wire and in `COPY TO`, though not of `::text` casts, arrays or JSON;
  `client_min_messages` at `warning` or `error` keeps `NOTICE`s from
  the client; `row_security = off` refuses a query a policy would
  filter (below). `max_identifier_length` reports `63` and, like
  `server_version_num`, cannot be changed (`55P02`).
  `pg_stat_activity` and `SHOW SESSIONS` list the serving node's
  sessions only.

### Row-level security

The rules are PostgreSQL's; these are the differences.

| | PostgreSQL | datax |
|---|---|---|
| Who is exempt | superusers and `BYPASSRLS` roles always; the owner unless `FORCE` | `root` and `BYPASSRLS` roles always; the owner **and members of `admin`** unless `FORCE` (an admin is treated as an owner of every object) |
| `TRUNCATE` | allowed with the `TRUNCATE` privilege, ignoring policies | refused (`42501`) to a role the table's policies apply to, whatever it was granted |
| Policy expressions | any expression, subqueries included | no subqueries; column references unqualified; of the session functions only `current_user` (also `current_role`, `user`), `session_user`, `current_setting`, `current_database` (also `current_catalog`), `current_schema` and the clock. They are evaluated once per statement |
| Predicate order | a leakproof-function analysis decides what may run before the policy | the policy is always evaluated first; no statement predicate sees a hidden row |
| Views | owner's rights by default, `security_invoker` to change it | owner's rights only |
| `SET row_security = off` | makes a filtered query an error instead (used by `pg_dump`) | the same: `42501` `query would be affected by row-level security policy for table "t"`, without PostgreSQL's hint for an owner under `FORCE` |
| Historical reads | — | `AS OF SYSTEM TIME` reads are filtered by today's policies |
| Statistics | `pg_stats` hidden; `pg_class.reltuples` visible | `SHOW STATS`, `reltuples`, `n_live_tup` and `EXPLAIN` estimates all withheld from a filtered role |
| `psql \d t` | lists policies with their expressions | lists policies without the expressions; `pg_policies` and `SHOW CREATE TABLE` have them |
| Custom settings | need a dot; any value; unknown until set | the same; `RESET` leaves the setting empty, and a transaction's rollback does not undo a plain `SET` or `set_config(..., false)` made inside it |
| Constraint errors | reveal that a hidden row holds a unique value; foreign keys see the whole parent | the same, with no value of the hidden row in the message |

### Large values

A column value is limited by the row, and a row by what one replicated
write can carry: a statement that writes more than 60 MiB to one range
in one piece is refused with `54000` (PostgreSQL allows a 1 GB field).
Well below that a large value is already expensive — it is written,
replicated and read whole, with no out-of-line storage — so values of
more than a few MiB belong in large objects.

### Large objects

The functions are PostgreSQL's and drivers use them unchanged; these
are the differences ([reference](sql.md#large-objects)):

- **A new object's content is not part of the transaction's write set.**
  It is stored as it arrives and becomes visible, whole, when the
  transaction that created the object commits — which is what lets an
  upload be larger than any transaction. The visible behaviour is
  PostgreSQL's (nothing before commit, everything after, nothing after a
  rollback) with two exceptions: `ROLLBACK TO SAVEPOINT` cannot undo
  such writes, so the transaction can then only roll back (`25P02` at
  `COMMIT`); and an upload left idle for 10 minutes is abandoned
  (`55000`).
- The commit of an upload fails with `40001` if anything the
  transaction read earlier — the catalog included — changed while the
  upload ran (the transaction is moved forward at commit so that it
  commits after its pages).
- Writes to an object that existed before the transaction are fully
  transactional and limited to 64 MiB per transaction (`54000`).
  PostgreSQL has no such limit; replace a large object by writing a new
  one.
- Pages are 256 KiB rather than 2 kB, and objects up to 1 TiB rather
  than 4 TiB.
- An object is its owner's and the admins'. PostgreSQL's per-object
  `SELECT` / `UPDATE` grants do not exist.
- A reader that opens an object while another transaction is in the
  middle of changing or unlinking it waits for that transaction, as
  reads of any row a transaction has written do here; PostgreSQL would
  read the old version without waiting.
- Object ids are 32-bit as in PostgreSQL, but generated at random
  (above 65,536) rather than from the OID counter.
- `vacuumlo` is not needed for space — an unlinked object's pages are
  removed by the cluster within about a minute and garbage-collected
  after the GC window — but objects orphaned by deleting the row that
  named them are still yours to unlink, exactly as in PostgreSQL.

## Things datax accepts that PostgreSQL does not

- **`ALTER TABLE t ADD CONSTRAINT IF NOT EXISTS name ...`**: when the
  table already has a constraint called `name` (its `<table>_pkey`, or a
  `CHECK`, `UNIQUE` or `FOREIGN KEY` constraint), the statement does
  nothing and says so in a NOTICE (`constraint "name" for relation "t"
  already exists, skipping`); otherwise it adds the constraint as `ADD
  CONSTRAINT` would. Only the name is compared, never the definition: a
  constraint of that name that checks something else is left as it is.
  An index made by `CREATE UNIQUE INDEX` is not a constraint, so its name
  is still refused (`42710`). PostgreSQL has `IF NOT EXISTS` on `ADD
  COLUMN` and `CREATE INDEX` (both supported here) but not on `ADD
  CONSTRAINT`; migration tools that want an idempotent constraint can
  use it, at the price of a schema PostgreSQL will not load.
- `UPSERT INTO` (CockroachDB syntax), above.

## Errors you should know

| SQLSTATE | Meaning | What to do |
|---|---|---|
| `40001` | serialization failure — including a transaction that read data whose history was garbage-collected under it (it ran longer than a table's `gc_ttl`, or a `PURGE` of what it reads ran meanwhile) | retry the whole transaction |
| `72000` | snapshot too old: `AS OF SYSTEM TIME` names a moment older than the table's GC window (`gc_ttl`, default 25h), or before a `PURGE` or a dropped table's reclaim | read at a later moment; retrying the same one cannot succeed |
| `25P02` | transaction poisoned by an earlier error | `ROLLBACK` (or `ROLLBACK TO SAVEPOINT`) |
| `55P03` | a lock was not available: `FOR UPDATE NOWAIT` met a locked row, or a wait (for a row or an advisory lock) outlasted `lock_timeout` | the row is busy — back off, or do something else; not a retry-now signal |
| `23505` | unique violation | application-level conflict |
| `54000` | program limit exceeded: a write of more than 60 MiB to one range in one piece (a huge row); more than 64 MiB written into existing large objects by one transaction; a large object past 1 TiB | store the value as a [large object](sql.md#large-objects), written in a new object inside one transaction block |
| `55000` on a large-object call or `COMMIT` | the upload sat idle for more than 10 minutes and was reclaimed | roll back and upload again |
| `XXK01` | a row of a [sealed table](sql.md#sealed-tables) whose key — the table's, or its tenant's — is destroyed, disabled or unavailable (datax's own code) | not retryable: the key was destroyed (the data is erased) or must be re-enabled |
| `XX001` | a sealed table's stored value does not authenticate (data corrupted) | report it; it is not a retry signal |
| `0A000` | feature not supported | you've left the subset — check this page |
| `42601` | syntax error | likewise, usually |
| `42601` "statement nests too deeply" | more than 1,000 nesting levels (parentheses, subqueries, derived tables, `CASE`) | flatten the statement; PostgreSQL bounds the same recursion by stack depth (issue #135) |

## What psql and ORMs can see

The `pg_catalog` and `information_schema` views ([list](sql.md#introspection-show-and-the-catalogs))
are real, read-only virtual tables over the live schema, and the SQL
they are queried with (joins with parenthesized `ON`, `array(SELECT
...)`, `= ANY(...)`, `unnest(...)`, `UNION ALL`, correlated subqueries
in the select list, `CASE`, `::casts`, `OPERATOR(pg_catalog.~)`,
`COLLATE`, `E'...'`) parses. In practice:

- **psql**: `\l`, `\dt`, `\di`, `\d t`, `\d+ t`, `\d index`, `\du`,
  `\dn`, `\dp` / `\z`, `\dT`, `\db`, `\dx`, `\df`, `\dS`, tab completion
  and the `+` variants render; `\dv` lists the views, `\ds` the
  sequences and `\sv` prints a view; the list commands for features
  datax lacks (`\dm`, `\dy`, `\dRp`, `\dew`, `\dAf`, `\dAo`, `\dAp`, ...)
  render empty. `\dd` lists constraint comments (`COMMENT ON CONSTRAINT`)
  and `\dconfig` the settings the session changed. Sizes (`\dt+`, `\l+`) show blank: `pg_table_size`
  and `pg_database_size` are `NULL`.
- **Drivers**: pgx's, psycopg's and lib/pq's startup queries
  (`pg_type` lookups, `SHOW standard_conforming_strings`, `SET
  client_encoding`) work.
- **ORMs / migration tools**: schema introspection through
  `information_schema.tables` / `columns` / `table_constraints` /
  `key_column_usage`, `pg_indexes`, `pg_class` + `pg_attribute` +
  `pg_index` joins, `format_type`, `pg_get_indexdef`,
  `pg_get_constraintdef`, `'t'::regclass`, `to_regclass()` and
  existence checks against the catalogs (`SELECT 1 FROM pg_class WHERE
  relname = ...`) return what PostgreSQL would for a schema in the
  datax subset. A column's sequence is found as Rails and pg_dump look
  for it: `pg_get_serial_sequence('t', 'id')` (`public.t_id_seq`, for a
  SERIAL, identity or `OWNED BY` sequence) and the `pg_depend` rows
  that tie each such sequence to its column. Reads of
  features datax lacks (triggers, comments) come back
  empty rather than erroring — whatever shape the tool's query takes;
  `pg_sequences`, `pg_sequence` and `pg_class` rows with `relkind =
  'S'` list the sequences, and `pg_constraint` (with
  `pg_get_constraintdef`, `conrelid::regclass`) the constraints.
- **Which database.** The current one, as in PostgreSQL, where each
  database has catalogs of its own: `information_schema` (with
  `table_catalog` the current database's name) and the per-database
  `pg_catalog` tables (`pg_class`, `pg_attribute`, `pg_type`'s enum
  types, `pg_sequences`, ...) list its objects only; another
  database's are reachable by qualified name (`db.t`) but not listed.
  The shared catalogs — `pg_database`, `pg_roles` / `pg_user`,
  `pg_auth_members`, `pg_tablespace`, `pg_shdescription` — and
  `pg_stat_activity` are the cluster's. Before 0.58.0
  `information_schema.tables`, `.columns`, `.views` and `.schemata`,
  and `pg_type` / `pg_enum`, listed every database's.
- **Who may read them.** Every role, as in PostgreSQL: `pg_catalog`
  lists every table of the current database to any reader, and
  `information_schema` the tables the reader holds some privilege on.
  Two differences: table statistics (`reltuples`, `n_live_tup`) read
  `0` for a table the reader may not `SELECT` from, where PostgreSQL
  shows them to everyone, and `pg_largeobject_metadata` lists the
  reader's own large objects only.
- **The privilege functions** (`has_table_privilege`,
  `has_column_privilege`, `has_any_column_privilege`,
  `has_sequence_privilege`, `has_schema_privilege`,
  `has_database_privilege`, `has_function_privilege`,
  `has_type_privilege`, `has_tablespace_privilege`, `pg_has_role`) take
  every argument form PostgreSQL's do and answer from datax's grants,
  ownership and role graph ([Functions
  reference](functions.md#system-information-and-privileges)). Where
  the models differ: a column's privilege is its table's (there are no
  column grants); `REFERENCES`, `TRIGGER` and `MAINTAIN` are the
  owner's and admins'; a sequence's `USAGE` grant also answers
  `SELECT`, and `INSERT` / `UPDATE` / `SELECT` on the table that owns a
  SERIAL sequence answer for the sequence, as `nextval` and `currval`
  check them; `EXECUTE` on functions and `USAGE` on types are
  everyone's; `TEMPORARY` on a database follows `CONNECT`; PostgreSQL's
  predefined roles (`pg_read_all_data`, `pg_write_all_data`) answer as
  `read_all` and `write_all`, the other `pg_*` ones as roles only
  admins hold. A user given as an OID no role has answers as `PUBLIC`.
  Only the object argument may name a column (`has_table_privilege(c.oid,
  'SELECT')` over `pg_class`); a column user or privilege argument is
  refused (`0A000`).
- **Writes to a catalog** are refused with SQLSTATE `42501`.
- **How many tables.** The catalogs, `SHOW TABLES` and name
  resolution cost in proportion to the current database, not to the
  cluster. See [Capacity
  planning](operations.md#capacity-planning) for what was measured (up
  to 5,000 tables) and what still grows with the table count.

## Client compatibility notes

- pgx (native and `database/sql`), lib/pq, and psql work, in both simple
  and extended protocol modes, text and binary formats.
- Prepared statements plan once per session and reuse the plan until the
  table's schema or statistics change (`EXPLAIN` shows `(cached plan)`);
  there is no custom-vs-generic plan choice — bounds stay parameterized
  and non-point shapes re-choose their path per execution.
- Prepared statements and portal suspension both work — JDBC-style fetch
  sizes are fine. A scan-shaped `SELECT` (one table, no sort, aggregate,
  join, `DISTINCT` or window) streams: each fetch pulls the next rows
  from storage, so a fetch limit bounds server memory too. Results that
  sort, aggregate or join are materialized at the first Execute and
  served in fetch-size chunks, under `statement_memory_limit` (`53200`
  beyond it).
- Large objects work through pgx's `tx.LargeObjects()` and through
  libpq's `lo_*` functions (the fast-path function call), which psql's
  `\lo_import` / `\lo_export` / `\lo_list` / `\lo_unlink` use; both
  are tested. The JDBC driver's `LargeObjectManager` and psycopg's
  `lobject` use the same fast-path calls and are untested. Only the
  large-object functions answer a fast-path call (`0A000` for any
  other). Write and read with buffers of about 1 MiB. pgx's `LargeObject.Read` into a
  buffer larger than half of `statement_memory_limit` is refused
  (`53200`) — use `io.Copy` with a buffer, not `io.ReadAll`.
- `sslmode=verify-full` works in secure mode with the datax CA
  ([Security](security.md)); `sslmode=disable` in insecure mode.
