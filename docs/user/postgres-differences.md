# Differences from PostgreSQL

datax speaks the PostgreSQL wire protocol and a subset of its SQL. Standard
drivers, `psql`, and ORMs that stay inside the subset work unmodified. This
page is the honest list of where the road ends, with workarounds.

## Things that will error immediately

All of these are rejected with a clear error (usually SQLSTATE `42601`
syntax error or `0A000` feature not supported):

| Missing | Workaround |
|---|---|
| Functions beyond the [Functions reference](functions.md) (`SHOW FUNCTIONS`) — SQLSTATE `42883`; `TIME WITH TIME ZONE`; `INTERVAL` field qualifiers as a constraint (`INTERVAL DAY TO SECOND` stores the full triple); `IntervalStyle` other than `postgres`; expressions over aggregates (`SUM(a) / COUNT(*)`); user-defined functions | compute client-side or in a derived table; `INTERVAL` and `TIME` **are supported** ([types](sql.md#types)) |
| `EXPLAIN` options in parentheses (`FORMAT JSON`, `VERBOSE`, `BUFFERS`); `RANGE` frames with a numeric offset, `GROUPS` frames, `EXCLUDE`, `FILTER` on a window call; correlated subqueries over set operations | `EXPLAIN ANALYZE` (stage rows and times), window functions (ranking, offset, value and aggregate, with `PARTITION BY`, `ORDER BY`, `ROWS` frames and `WINDOW` names), `WITH` / `WITH RECURSIVE` (data-modifying members too), `INSERT ... SELECT`, `OFFSET`, `FETCH FIRST`, `UNION` / `INTERSECT` / `EXCEPT [ALL]`, `RIGHT` / `FULL` / `NATURAL` joins and `USING` **are supported** ([reference](sql.md#reading)) |
| `DEFERRABLE` constraints, `ON DELETE SET DEFAULT`, `MATCH FULL`, `ADD PRIMARY KEY`, `EXCLUDE` | `CHECK`, `UNIQUE` and `FOREIGN KEY` constraints **are supported** ([reference](sql.md#constraints-check-unique-and-foreign-key)), checked at statement end; the referencing side of a foreign key gets an index automatically, and a cascade is capped per statement (`foreign_key_cascade_limit`) |
| `DEFAULT` expressions referencing other columns, `ALTER TABLE ... ADD COLUMN` with an expression default, `ALTER SEQUENCE ... OWNED BY` | sequences, `SERIAL`, identity columns, expression defaults and `ALTER COLUMN SET / DROP DEFAULT` **are supported** ([reference](sql.md#defaults-serial-identity-columns-and-sequences)); recreate the table for the rest |
| `ADD PRIMARY KEY` / `DROP` of the primary key, `ALTER TABLE ... SET SCHEMA`, `RENAME TO` across databases, `ALTER COLUMN TYPE ... USING expr` and narrowing conversions, `COMMENT ON` objects other than tables, views, indexes and columns, `SELECT ... INTO` | `DROP INDEX`, `ALTER INDEX RENAME`, the `RENAME` forms, `TRUNCATE`, `IF [NOT] EXISTS` everywhere, `ALTER COLUMN TYPE` (an online rewrite for widening and text conversions), `CREATE TABLE ... AS` (a hidden `rowid` key unless `PRIMARY KEY (cols)` is written among the column names), `CREATE TABLE ... (LIKE t INCLUDING ...)` and `COMMENT ON TABLE / VIEW / INDEX / COLUMN` **are supported** ([reference](sql.md#ddl)); recreate the table for the rest |
| `COPY ... TO`, COPY options beyond `FORMAT` | `COPY t FROM STDIN` **is supported** (text, CSV, binary — psql `\copy` and pgx `CopyFrom` work); export with `SELECT` instead |
| Schemas | `public` is the only schema: `db.public.t` and `public.t` are accepted, any other schema name is an error; `search_path` is accepted and ignored. Databases are real (`CREATE DATABASE`, the URL's database, `USE`, `SET database`, `current_database()`); see [Databases](sql.md#databases) |
| Materialized views, `WITH CHECK OPTION`, updatable views, `SECURITY INVOKER` views, renaming a table or column a view reads (refused; replace the view) | Views **are supported** ([reference](sql.md#views)): `CREATE [OR REPLACE] VIEW`, `DROP VIEW [CASCADE]`, dependency tracking, definer semantics (the query runs with the owner's privileges), `pg_views`, `information_schema.views`, `\dv` |
| Column-level privileges, `REFERENCES` / `TRIGGER` / `MAINTAIN` privileges, `CREATEDB` / `CREATEROLE` / `SUPERUSER` role attributes, `VALID UNTIL`, `CONNECTION LIMIT`, `ALTER ROLE ... SET`, `SET SESSION AUTHORIZATION`, `pg_hba.conf` | Roles **are supported** ([security guide](security.md#roles-and-privileges)): `CREATE / ALTER / DROP ROLE` and `USER` (`LOGIN`, `PASSWORD`, `INHERIT`, `IN ROLE`), membership with `ADMIN OPTION`, `SET ROLE`, ownership (`OWNER TO`, `REASSIGN OWNED`, `DROP OWNED`), grants on tables, views, sequences, databases and `public` with `GRANT OPTION`, `ALL TABLES IN SCHEMA`, `ALTER DEFAULT PRIVILEGES`, `PUBLIC`, the built-in `admin`, `read_all`, `write_all` and `metrics` roles; `CREATE DATABASE` and role management take `admin`; the `BYPASSRLS` attribute |
| Subqueries in a policy expression, `SECURITY INVOKER` views over protected tables, `row_security = off`, policies on views | Row-level security **is supported** ([reference](sql.md#row-level-security), [security guide](security.md#row-level-security)): `ENABLE` / `DISABLE` / `FORCE` / `NO FORCE ROW LEVEL SECURITY`, `CREATE` / `ALTER` / `DROP POLICY` (permissive and restrictive, `FOR`, `TO`, `USING`, `WITH CHECK`), default deny, custom settings with `current_setting` / `set_config`. See below for where it differs |
| The fast-path function call for functions other than the large-object ones; `lo_import` / `lo_export` (server-side files), `GRANT ... ON LARGE OBJECT`, `ALTER LARGE OBJECT`, `lo_compat_privileges`, the `pg_largeobject` table itself, `COMMENT ON LARGE OBJECT` | Large objects **are supported** ([reference](sql.md#large-objects)): `lo_create`, `lo_creat`, `lo_open`, `lo_close`, `loread`, `lowrite`, `lo_lseek[64]`, `lo_tell[64]`, `lo_truncate[64]`, `lo_unlink`, `lo_get`, `lo_put`, `lo_from_bytea`, and `pg_largeobject_metadata`; import and export client-side (psql's `\lo_import` / `\lo_export` do) |
| Triggers, stored procedures | — (`LISTEN` / `NOTIFY` / `pg_notify` **are supported**, across nodes: [reference](sql.md#notifications-listen-and-notify)) |

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
  a `TIMESTAMP` is `TIMESTAMPTZ`, `::timestamp` yields `TIMESTAMPTZ`),
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
  untyped argument yields `text[]`; `unnest` runs in `FROM` or in a
  FROM-less select list (not beside a table's columns, and not
  laterally), and an aggregate call takes no inner `ORDER BY`.
- **Intervals** compare, group and index by PostgreSQL's rule (`'30
  days' = '1 month'`), but a primary key or unique index keeps `'30
  days'` and `'1 month'` as distinct keys (the stored triple is part of
  the key); `extract` on an interval reports the stored fields
  unjustified, as in PostgreSQL. A `TIME` input's offset is ignored;
  `TIME WITH TIME ZONE` is refused at parse (`42601`).
- **Bare decimal literals are DECIMAL** — same as PostgreSQL, but note
  `SELECT 1.5` describes as `NUMERIC`, not `float8`.
- **JSONB**: `->`/`->>` extraction (single-table queries and joins;
  grouped SELECT lists refuse), containment `@>` / `<@` and key
  existence `?` / `?|` / `?&` (single-table only), equality comparison,
  and `IS [NOT] NULL`; no ordering comparisons. `@>` is not accepted in
  `HAVING` or after a computed left-hand side.
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
  `enable_bitmapscan` is not a setting (`inverted_index_scan` is).
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
    may be built on; the configuration must be written out, as
    PostgreSQL requires. No expression indexes otherwise (not over a
    concatenation of columns, not `setweight(...) || ...`), no generated
    columns and no triggers to maintain a `tsvector` column: the
    application writes it. No GiST (`USING GIST` is refused), no
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
  - *There is no extension.* `CREATE EXTENSION vector` is accepted and
    does nothing (any other extension name is refused, `0A000`); `DROP
    EXTENSION`, `pg_extension` and `vector_version()` do not exist. The
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
    first. `CREATE INDEX` is always online (there is no `CONCURRENTLY`
    keyword to ask for it), and an index built on little or no data
    gets fewer lists than asked for — one, on an empty table — instead
    of pgvector's notice about low recall. `pg_indexes` shows the
    number of lists actually built.
  - *A vector cannot be a primary key or in a btree or unique index*
    (pgvector allows btree). `=`, `<>`, `<`, `ORDER BY`, `GROUP BY` and
    `DISTINCT` on a vector do work.
  - *Not there*: `halfvec`, `sparsevec`, `bit` vectors and their
    operators (`<+>` L1 as an operator, `<~>`, `<%>`), `vector *
    vector`, `||`, `subvector`, `binary_quantize`, and the `avg` and
    `sum` aggregates over vectors (`0A000`). Expression indexes (an
    index over `(embedding::halfvec(3))`, or over a function of the
    column) and partial indexes do not exist in datax for any index.
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
- **`ON CONFLICT`** works on the primary key and unique indexes (by
  columns or `ON CONSTRAINT <table>_pkey | <index name>`), with `DO
  NOTHING`, `DO UPDATE SET ... [WHERE]` and `EXCLUDED`; `DO UPDATE` cannot
  change a primary-key column, and there is no `ON CONFLICT ... DO
  UPDATE` over exclusion constraints (there are none). `UPSERT INTO`
  (CockroachDB syntax) is the primary-key shorthand.
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
  values) and refuses an unknown one with `42704` (an invalid value is
  `22023`), where PostgreSQL knows hundreds more. `transaction_isolation`
  accepts every level and `SHOW` reports `serializable`, the only one
  there is. `server_version` reports a PostgreSQL-14-compatible string,
  so `psql` and drivers use their PostgreSQL-14 query flavors.
  `pg_stat_activity` and `SHOW SESSIONS` list the serving node's
  sessions only.

### Row-level security

The rules are PostgreSQL's; these are the differences.

| | PostgreSQL | datax |
|---|---|---|
| Who is exempt | superusers and `BYPASSRLS` roles always; the owner unless `FORCE` | `root` and `BYPASSRLS` roles always; the owner **and members of `admin`** unless `FORCE` (an admin is treated as an owner of every object) |
| `TRUNCATE` | allowed with the `TRUNCATE` privilege, ignoring policies | refused (`42501`) to a role the table's policies apply to, whatever it was granted |
| Policy expressions | any expression, subqueries included | no subqueries; column references unqualified; of the session functions only `current_user`, `session_user`, `current_setting`, `current_database`, `current_schema` and the clock. They are evaluated once per statement |
| Predicate order | a leakproof-function analysis decides what may run before the policy | the policy is always evaluated first; no statement predicate sees a hidden row |
| Views | owner's rights by default, `security_invoker` to change it | owner's rights only |
| `SET row_security = off` | makes a filtered query an error instead (used by `pg_dump`) | not a setting; `42704` |
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

## Errors you should know

| SQLSTATE | Meaning | What to do |
|---|---|---|
| `40001` | serialization failure — including a transaction that read data whose history was garbage-collected under it (it ran longer than a table's `gc_ttl`, or a `PURGE` of what it reads ran meanwhile) | retry the whole transaction |
| `72000` | snapshot too old: `AS OF SYSTEM TIME` names a moment older than the table's GC window (`gc_ttl`, default 25h), or before a `PURGE` or a dropped table's reclaim | read at a later moment; retrying the same one cannot succeed |
| `25P02` | transaction poisoned by an earlier error | `ROLLBACK` (or `ROLLBACK TO SAVEPOINT`) |
| `55P03` | a lock was not available: `FOR UPDATE NOWAIT` met a locked row, or a wait outlasted `lock_timeout` | the row is busy — back off, or do something else; not a retry-now signal |
| `23505` | unique violation | application-level conflict |
| `54000` | program limit exceeded: a write of more than 60 MiB to one range in one piece (a huge row); more than 64 MiB written into existing large objects by one transaction; a large object past 1 TiB | store the value as a [large object](sql.md#large-objects), written in a new object inside one transaction block |
| `55000` on a large-object call or `COMMIT` | the upload sat idle for more than 10 minutes and was reclaimed | roll back and upload again |
| `XXK01` | a row of a [sealed table](sql.md#sealed-tables) whose key is destroyed, disabled or unavailable (datax's own code) | not retryable: the key was destroyed (the data is erased) or must be re-enabled |
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
  features datax lacks (triggers, comments, extensions) come back
  empty rather than erroring — whatever shape the tool's query takes;
  `pg_sequences`, `pg_sequence` and `pg_class` rows with `relkind =
  'S'` list the sequences, and `pg_constraint` (with
  `pg_get_constraintdef`, `conrelid::regclass`) the constraints.
- **Who may read them.** Every role, as in PostgreSQL: `pg_catalog`
  lists every table of the current database to any reader, and
  `information_schema` the tables the reader holds some privilege on.
  Two differences: table statistics (`reltuples`, `n_live_tup`) read
  `0` for a table the reader may not `SELECT` from, where PostgreSQL
  shows them to everyone, and `pg_largeobject_metadata` lists the
  reader's own large objects only.
- **Writes to a catalog** are refused with SQLSTATE `42501`.
- **How many tables.** The `pg_catalog` views, `SHOW TABLES` and name
  resolution cost in proportion to the current database, not to the
  cluster; `information_schema.tables`, `.columns` and `.views` list
  every database and read every table descriptor. See [Capacity
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
