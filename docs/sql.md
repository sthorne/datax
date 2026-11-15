# SQL layer and PostgreSQL wire protocol

datax's SQL surface is deliberately small in v1: enough to create tables,
read and write rows by primary key or scan, and run multi-statement
transactions — all over the standard PostgreSQL wire protocol.

## Grammar (v1)

```
CREATE TABLE t (col TYPE [NOT NULL], ..., PRIMARY KEY (col, ...))
    [WITH (timeseries = true [, retention = '7d'] [, shards = N])]  -- see docs/timeseries.md
    [WITH (gc_ttl = '10m')]                                         -- the table's own GC window (v21)
    [WITH (ttl_expiration = col | ttl_column = col, ttl = '7 days')] -- row expiry (v21); see Table options below
ALTER TABLE t SET (shards = M)          -- online re-shard of a sharded timeseries table
ALTER TABLE [IF EXISTS] t SET (gc_ttl = ... | ttl_expiration = col | ttl_column = col | ttl = '...' | retention = '...')
ALTER TABLE [IF EXISTS] t RESET (gc_ttl | ttl | ttl_column | ttl_expiration | retention [, ...])
ALTER TABLE [IF EXISTS] t PURGE         -- collect the table's garbage now, compact it away, report per replica
ALTER TABLE [IF EXISTS] t ADD | DROP COLUMN ... | RENAME TO t2 | RENAME [COLUMN] a TO b | RENAME CONSTRAINT a TO b
    | ALTER [COLUMN] c SET DEFAULT v | DROP DEFAULT | SET NOT NULL | DROP NOT NULL | ADD | DROP | VALIDATE CONSTRAINT ...
CREATE [UNIQUE] INDEX [IF NOT EXISTS] i ON t [USING btree] (cols)  /  DROP INDEX [IF EXISTS] i  /  ALTER INDEX i RENAME TO j
CREATE INDEX [IF NOT EXISTS] i ON t USING gin (jsonb_col [jsonb_ops | jsonb_path_ops])
CREATE INDEX [IF NOT EXISTS] i ON t USING ivfflat (vector_col [vector_l2_ops | vector_cosine_ops | vector_ip_ops]) [WITH (lists = n)]   -- see Vectors below
CREATE EXTENSION [IF NOT EXISTS] vector  -- accepted, does nothing: the type is built in
TRUNCATE [TABLE] t [, ...] [RESTART IDENTITY] [CASCADE]   -- a layout swap: new index IDs, the old layout retired
DROP TABLE t
CREATE [OR REPLACE] VIEW v [(cols)] AS SELECT ...  /  DROP VIEW [IF EXISTS] v [, ...] [CASCADE]  /  SHOW VIEWS
CREATE TABLE t [(names [, PRIMARY KEY (cols)])] AS SELECT ... [WITH [NO] DATA]   -- streamed through the COPY chunk path
CREATE TABLE t (LIKE src [INCLUDING | EXCLUDING DEFAULTS | CONSTRAINTS | INDEXES | COMMENTS | ALL], ...)
ALTER TABLE t ALTER [COLUMN] c [SET DATA] TYPE type       -- online rewrite: shadow column, chunked convert, swap
ALTER TABLE [IF EXISTS] t SPLIT AT VALUES (k, ...) [, (k, ...)]... [WITH EXPIRATION expr]   -- manual (sticky) range boundaries; expr: timestamp | interval
ALTER TABLE [IF EXISTS] t UNSPLIT AT VALUES (k, ...) [, (k, ...)]... | UNSPLIT ALL          -- release them to the merge queue
COMMENT ON TABLE | VIEW | INDEX | COLUMN name IS 'text' | NULL
ALTER TABLE [IF EXISTS] t ENABLE | DISABLE | FORCE | NO FORCE ROW LEVEL SECURITY         -- row-level security (v21); see below
CREATE POLICY p ON t [AS PERMISSIVE | RESTRICTIVE] [FOR ALL | SELECT | INSERT | UPDATE | DELETE]
    [TO role [, ...] | PUBLIC] [USING (expr)] [WITH CHECK (expr)]
ALTER POLICY p ON t RENAME TO q  /  ALTER POLICY p ON t [TO role [, ...]] [USING (expr)] [WITH CHECK (expr)]
DROP POLICY [IF EXISTS] p ON t [CASCADE | RESTRICT]
CREATE | ALTER ROLE r ... [BYPASSRLS | NOBYPASSRLS]                                      -- exempt from every table's policies
SET [LOCAL] class.name = value  /  RESET class.name  /  SHOW class.name                  -- custom session settings (app.tenant)
INSERT INTO t [(cols)] VALUES (v, ...), (v, ...) | SELECT ...
COPY t [(cols)] FROM STDIN [WITH (FORMAT text|csv|binary)]   -- see Wire protocol below
[WITH [RECURSIVE] name [(cols)] AS (query), ...]   -- on SELECT, INSERT, UPDATE, DELETE
SELECT [DISTINCT] * | col, ... | aggregates | func() OVER ([PARTITION BY ...] [ORDER BY ...] [ROWS | RANGE frame])
    FROM t [[AS] alias] | (SELECT ...) [AS] alias
    [[INNER | LEFT | RIGHT | FULL [OUTER] | CROSS | NATURAL] JOIN t2 | (SELECT ...) [[AS] alias] ON a.x = b.y [AND ...] | USING (cols)]
    [AS OF SYSTEM TIME 't' | AS OF SYSTEM TIME with_max_staleness('d')]
    [WHERE conjunction] [GROUP BY col, ...] [HAVING conjunction] [WINDOW name AS (...), ...]
    [UNION | INTERSECT | EXCEPT [ALL] query]
    [ORDER BY col | n | expr | agg() [ASC|DESC] [NULLS FIRST|LAST], ...]
    [LIMIT n | ALL] [OFFSET n] [FETCH FIRST n ROWS ONLY] [FOR UPDATE [SKIP LOCKED | NOWAIT]]
SELECT <literal exprs>                  -- e.g. SELECT 1 (client health checks)
UPDATE t SET col = value, ... [WHERE conjunction]
DELETE FROM t [WHERE conjunction]
BEGIN / COMMIT / ROLLBACK
SAVEPOINT name / RELEASE SAVEPOINT name / ROLLBACK TO SAVEPOINT name
LISTEN channel / UNLISTEN channel | * / NOTIFY channel [, 'payload']   -- see LISTEN / NOTIFY below
SHOW TABLES
ANALYZE [t]                             -- collect table statistics (admin; not in a txn block)
SHOW STATS FOR t
```

- Types: `INT8` (BIGINT), `INT4` (INT, INTEGER) and `INT2` (SMALLINT) —
  one `Int` family, the width a column attribute (`catalog.Column.Width`)
  that bounds values and picks the wire OID; `FLOAT8` (DOUBLE PRECISION),
  `TEXT` (aliases STRING, VARCHAR) with `VARCHAR(n)` / `CHAR(n)` as
  attributes (`MaxLen`, `Char`), `BOOL` (BOOLEAN), `TIMESTAMPTZ`
  (TIMESTAMP WITH TIME ZONE; UTC nanoseconds internally, microsecond
  precision on the binary wire) and `TIMESTAMP` without time zone
  (attribute `NoTZ`: the same nanoseconds, rendered without an offset,
  an input offset ignored), `TIMESTAMP(p)` / `TIME(p)` (`TimePrecision`,
  stored as p+1), `DATE`, `TIME` (nanoseconds since midnight), `INTERVAL`
  (a months / days / nanoseconds triple: `Datum.Mo`, `Datum.Dy`,
  `Datum.I`; compared by PostgreSQL's 30-day-month, 24-hour-day value),
  enums (`CREATE TYPE ... AS ENUM`: a `TypeDescriptor` of labels under
  `/system/typedesc/<id>` and `/system/typens/<db>/<name>`; a column of
  the type copies the labels — `Column.EnumType`, `EnumLabels`, refreshed
  by `ADD VALUE` with a drain — and stores the ordinal with the label,
  rowenc tag 15, keys as ordinal then label, so nothing on a read path
  looks the type up),
  `BYTES` (alias BYTEA; `\x` hex text format), `UUID`, `DECIMAL` (aliases
  NUMERIC, DEC), `JSONB` (alias JSON), `VECTOR` (pgvector's `vector(n)`:
  float32 elements, four big-endian bytes each in `Datum.S`, the
  dimension in `Column.MaxLen`; rowenc tag 18; no key encoding — see
  Vectors below), and arrays of every scalar
  family but JSONB (`types.ArrayOf(elem)`, a composite Family value
  `Array | elem<<8` so one value names `INT8[]` wherever a family flows;
  elements in `Datum.A`; rowenc tag 14 encodes each element with its
  order-preserving key encoding). The attributes are enforced on
  every write by `catalog.Column.Conform` (a `ValueError` carries the
  SQLSTATE) and described by `sql.ResultColumn`, so pgwire picks the OID,
  size and rendering per described column; below cluster version v9 a
  new column drops them (the earlier meaning), and INTERVAL / TIME
  columns need v10 (their rowenc tags 12 and 13 are unknown to v9).
  String literals coerce into the new types (`WHERE at >= '2026-08-30
  02:00:00Z'` becomes a key bound), and all but JSONB are usable in
  primary keys and indexes via order-preserving encodings (an interval
  key is its comparison value followed by the months and days; JSONB
  has no ordering and refuses with
  `0A000`).
- DECIMAL is exact arbitrary-precision arithmetic (`math/big` coefficient
  × 10^exp). Non-integer numeric literals are DECIMAL, as in PostgreSQL —
  `0.1` survives to a DECIMAL column unrounded, and `SUM`/`AVG` over
  DECIMAL stay exact where float64 breaks (`AVG` quantizes to 6 fractional
  digits, round-half-even). Int operands mix exactly; a FLOAT8 operand
  demotes the expression to float. Float→Decimal coercion is rejected —
  cast through text if you really mean it.
- `DECIMAL(p,s)` is enforced at the two row-completion choke points every
  write funnels through (insert-row build — INSERT, COPY, defaults — and
  the UPDATE SET loop): values quantize to scale `s` round-half-even,
  then overflow past `p−s` integer digits is SQLSTATE `22003` (checked
  after rounding, so `9.999` into `DECIMAL(3,2)` rounds to `10.00` and
  then overflows — PostgreSQL order). Quantization happens before key
  encoding, so DECIMAL primary keys and index entries store the rounded
  value (two inserts that round together are a `23505`). Storage keeps
  the canonical trailing-zero-stripped text — equality, grouping, and
  memo identity are untouched — while the datum carries the declared
  scale as a display-only field: `Text()` pads to it (`9.90`), and the
  binary NUMERIC encoder derives its `dscale` from that padded render.
  RowDescription reports the typmod (`(p<<16)|(s+4)`; `n+4` for
  `VARCHAR(n)` / `CHAR(n)`, `p` for `TIMESTAMP(p)`) for enforced
  columns. Precision/scale and the other attributes live on the column
  descriptor as append-only `omitempty` JSON fields (zero = the
  pre-existing meaning), so old descriptors and rolling upgrades are
  unaffected. `CHAR(n)` stores its value trimmed and stamps a display
  pad (`Datum.Pad`), `TIMESTAMP` without time zone a display flag
  (`Datum.NoTZ`), both like `Dscale`: never part of identity.
- JSONB stores normalized text (sorted object keys, compact, duplicate
  keys last-wins). Numbers pass through `json.Number`, so integer
  fidelity is preserved on ingest; equality compares normalized text, and
  ordering comparisons are refused. Extraction: `col -> 'key'` (jsonb)
  and `col ->> 'key'` (text, terminal only), chainable
  (`j -> 'a' ->> 'b'`); missing keys and non-objects yield NULL.
  Path operators work in single-table queries and joins (select lists
  and WHERE, evaluated on the joined row; grouped SELECT lists refuse
  with `0A000`), and path conjuncts never become index bounds.
- JSONB containment `@>` (and its NOT-elimination twin `NOT @>`): decoded
  with `json.Number` and walked structurally — objects contain
  recursively, arrays contain each right element via SOME left element,
  a top-level array contains a matching scalar (top level only), scalars
  compare by value with numbers compared NUMERICALLY through the decimal
  package (`1` contains `1.0`; integers beyond float64 keep fidelity —
  note the deliberate asymmetry with jsonb `=`, which stays normalized-
  text comparison). Accepted only as a plain-column conjunct (optional
  `->` path on the left, which must still produce jsonb); not in HAVING
  or after a computed left-hand side; refused in join queries with
  `0A000` (both in the filter and before base-scan pushdown — an
  unhandled op there would silently drop rows). Never a key bound of an
  ordinary index; a GIN index on the column serves it (and `?`, `?|`,
  `?&`) — see Inverted indexes below — and without one it is a
  post-fetch filter.
- `TSVECTOR`, `TSQUERY` and the `@@` operator (either order of the two
  types; a text operand beside a typed one is read as a literal of the
  other type; `text @@ tsquery` is refused) — see Full-text search
  below. `@@` is one more comparison operator to the parser, with `NOT
  @@` as its negation.
- Columns may declare `DEFAULT <literal>` (constants only): INSERTs that
  omit the column store the default; an explicit NULL stays NULL.
- WHERE: conjunctions (`AND`) of `col op value`, ops `= != < <= > >=`
  (plus jsonb `@>` on jsonb columns), `col IS [NOT] NULL`,
  `col [NOT] IN (list | SELECT ...)`, and
  `[NOT] EXISTS (SELECT ...)`. A value is a literal, a parameter, a
  column reference, or a scalar subquery `(SELECT ...)` (correlated in
  WHERE; uncorrelated ones also work in INSERT values and UPDATE SET).
- Table statistics: `ANALYZE [t]` sweeps the primary index in 1024-row
  chunks at a frozen timestamp (the CREATE INDEX backfill pattern — no
  transaction, no locks, termination independent of concurrent ingest),
  counting rows exactly and estimating per-column distinct counts with a
  256-entry KMV (k-minimum-values) sketch over splitmix64-finalized
  fnv-1a hashes of each datum's canonical form (DECIMAL hashes its
  canonical text, so display scale never splits a value). The blob
  persists as append-only JSON at `/system/stats/<tableID>` — a separate
  key from the lease-hot descriptor — read through a per-gateway 30s
  stale-serving cache, deleted in the DROP TABLE transaction, and shown
  by `SHOW STATS FOR t` (to a role holding `SELECT` on `t`).
  Statistics feed the planner (below); their
  absence means the structural fallback, byte-identical to the
  pre-statistics planner.
- Parameters (`$1 …`) are supported through the extended protocol (text
  format).

### Row-level security (v21)

A table with row security enabled (`TableDescriptor.RowSecurity`,
issue #294) carries its policies in the descriptor
(`TableDescriptor.Policies`: name, permissive or restrictive, command,
roles, and the `USING` and `WITH CHECK` expressions as SQL text). The
implementation is `pkg/sql/rls.go`.

**Who is filtered.** `Session.rowSecurityActive` decides per statement
and table: never the node's own sessions or `root`; never a role with
the `BYPASSRLS` attribute (the role's own — it is not inherited); the
owner and members of `admin` only when the table says `FORCE`; everyone
else always. The actor is the one privileges are checked as, so inside
a view's query it is the view's owner.

**Where it is applied.** As WHERE conjuncts, when the statement is
planned. Every read of a table's rows passes one filter — `matchesWhere`
over the conjunct list the fetch was given (`executePlan`, the streaming
iterator, the inverted scan; there is no index-only read path) — so the
policy's conjuncts are put at the head of that list:

- `fetchRows`, which every join side, aggregate input, index lookup and
  subquery reads through, adds the `SELECT` policies itself. That is
  what applies a policy per table reference.
- the single-table `SELECT` path adds them to the statement's WHERE
  before the access path is chosen (and the `UPDATE` policies too for
  `FOR UPDATE`), so the stream, the `SKIP LOCKED` loop and `EXPLAIN`
  see the same list.
- `UPDATE` and `DELETE` add their own command's policies, and the
  `SELECT` policies when the statement reads the table (a WHERE clause,
  `RETURNING`, a `SET` computed from a column), then read through
  `fetchRowsRaw`.
- the foreign-key checks and cascades call `fetchRowsRaw` with no
  policy: referential integrity is checked against the whole table.

Being ordinary conjuncts, they bound the scan: a policy on the leading
primary-key column is a range scan of that prefix, a policy on an
indexed column an index scan (`EXPLAIN` shows the bound and `; row-level
security policy applied`). They go first so the statement's own
predicates never run against a row the policy hides — an error raised
by one would say something about that row.

The permissive policies that govern the command for the actor's roles
are OR-ed (one alone is inlined, so its conjuncts can bound the scan),
each restrictive one AND-ed; none applicable is the constant `FALSE`.
Expressions are parsed and bound afresh by every statement:
`current_user`, `current_setting()` and the clock are spliced to
literals as a WHERE clause's are (`resolveWhereSubs`), memoized for the
statement (`Session.policyMemo`).

**Writes.** `rowCheck` holds the bound `WITH CHECK` side (`USING` when
a policy gives none). `INSERT`, `COPY` and the insert half of `ON
CONFLICT` check every proposed row before the table is probed for its
key, so a refusal does not depend on whether a row exists. `UPDATE`
checks each new row. `ON CONFLICT DO UPDATE` also requires the existing
row to pass the `UPDATE` and `SELECT` policies, and fails rather than
skips when it does not. A refused row is `42501`, `new row violates
row-level security policy for table "t"`. `TRUNCATE` reads no row and
is refused to a role the policies confine.

**Plan cache.** A statement row security applies to plans in full every
time: the single-table `SELECT` runs on a copy of the statement, and
`UPDATE` / `DELETE` pass no cache key. The cache is keyed by statement
and database, which cannot tell two roles or two values of a setting
apart; a role the policy does not apply to keeps its cached plans.

**Historical reads.** `AS OF SYSTEM TIME` resolves the descriptor — and
so the policies — as of its timestamp. `historicalRowFilter` therefore
reads today's descriptor and roles in a transaction of its own and
filters the historical rows by today's policies; it fails closed.

**Elsewhere.** Statistics (row and distinct counts) are withheld from a
confined role in `SHOW STATS`, `pg_class.reltuples`,
`pg_stat_user_tables`, `/api/schema` and `EXPLAIN`'s estimates. The
console decodes no range boundary of such a table for it
(`catalog.CanReadTable`). `DROP COLUMN` refuses a column a policy
reads, `RENAME COLUMN` rewrites the policies, `DROP ROLE` refuses a
role a policy names, and a table restore keeps row security and cuts
each policy's roles to the ones the cluster has.

**Custom settings.** `sessionVars.custom` holds two-part names
(`app.tenant`); `SET`, `SET LOCAL`, `RESET`, `SHOW`, `current_setting`
and `set_config` work on them. One is unknown (`42704`) until first set.

### Table options: GC window, row expiry, purge

Three statements about how long a table's data lasts live in the table
descriptor and the node, not in the executor (`pkg/sql/tableopts.go`;
cluster version v21; issues #295 and #298).

**`gc_ttl`** (`TableDescriptor.GCTTLSeconds`) is the table's own GC
window. The retention provider that already resolves a range to a
time-series table's retention (`pkg/server/retention.go`) resolves it to
this too: a range wholly inside the table collects at the table's
window, shorter or longer than the node's; a range the table shares
with other data keeps the longest window of its tenants. The statement
that sets it splits the table's two edges, and the merge queue leaves
ranges with different windows apart, so the table's ranges are its own.
Nothing else changes: collection is the same replicated command, the
newest version of a row stays, and reads at or below the threshold are
refused. A retention is itself a window, so a table has one or the
other.

**Row expiry** (`TableDescriptor.RowTTL`: a timestamp column and a
lifetime counted from it, zero when the column names the expiry time
itself) is a background job, `pkg/server/rowttl.go`, on the node leading
range 1. It does not reuse the time-series expiry beneath SQL
(`RowExpiry` in `kvserver.StoreConfig`), and cannot: that rule judges a
key by a timestamp decoded from the key, which a secondary-index entry
does not carry and a foreign key knows nothing of. The job finds expired
rows with `SELECT pk, col FROM t WHERE col <= $cutoff ORDER BY col LIMIT
$n OFFSET $skipped` — an ordered, limit-pushed scan of the index on the
column, which the DDL creates if no index leads with it (online, when
the table has rows) — and deletes each batch in one transaction of
point `DELETE`s that repeat the `col <= $cutoff` test, so a row whose
expiry was moved while the batch was in flight is left. A batch a
foreign key refuses (`23503`) is retried row by row; the rows still
refused are counted and stepped over (the `OFFSET`) for the rest of the
pass. The cutoff is fixed for a pass. An expired row is visible until
the job deletes it: there is no filter on reads.

**`PURGE`** names the table's span to the node's purge
(`kvclient.DB.Purge` → `server.Node.PurgeSpan`; `docs/transactions.md`,
"Purge") and renders the per-replica report as rows: `range_id`,
`node_id`, `live_keys`, `garbage_versions`, `tombstones`, `intents`,
`compacted`, `status`. The tag is `PURGE <versions removed>` when every
replica answered clean and `PURGE INCOMPLETE` otherwise — the statement
succeeds either way, because the rows are the result.

`SET`, `RESET` and `PURGE` run outside a transaction block (`25001`
inside one) and need table ownership.

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

Still out of scope: correlated subqueries nested past 8 levels or over
set-operation shapes,
joins beyond 8 tables (INNER joins are cost-reordered when statistics
exist; outer joins and self-joins keep syntactic order),
`EXPLAIN` options in parentheses, RANGE frames with an offset,
deferrable constraints,
typmods on other types than DECIMAL, the integer widths, `VARCHAR(n)` /
`CHAR(n)`, `TIMESTAMP(p)` and `TIME(p)` (parsed and ignored),
`TIME WITH TIME ZONE`, multidimensional arrays and array slices, indexes
on array columns, `JSONB[]`, enum `ADD VALUE BEFORE` / `AFTER` and
`RENAME VALUE`, arrays of enums,
JSONB indexing beyond `@>`, `?`, `?|` and `?&` on the bare column
(`<@`, the negations, comparisons of an extracted value and containment
under a `->` path evaluate as filters),
expressions over aggregates (`SUM(a) / COUNT(*)`), user-defined functions,
DEFAULT expressions that reference other columns.

### Expressions and builtins

`pkg/sql/builtins` is the single registry of scalar functions: each
`Builtin` carries its argument and result families, arity (optional
and variadic tails), volatility (immutable / stable / volatile),
strictness and a one-line description. The parser checks arity against
it (`42883` for an unknown name or a wrong count), the evaluator
(`Session.evalFunc`) dispatches through `Builtin.Call` — which applies
strict-NULL handling and coerces arguments to the declared families
(numerics lift among themselves, anything renders as text for a text
parameter) — `pg_proc` is built from it (`provolatile`, `proisstrict`,
argument and result OIDs), `SHOW FUNCTIONS` lists it, and
`docs/user/functions.md` is generated from it (`go generate
./pkg/sql/builtins`; a test fails when the file drifts). Functions that
need the session, the catalog or the transaction (`now()`,
`current_user`, `nextval`, `pg_get_*`) are registered with `Session:
true` for listing and arity only; the session splices them before the
row loop (`resolveWhereSubs` and friends in subquery.go, `spliceVolatile`
in sequence.go), and a CHECK expression goes through the same splice
each statement so `CHECK (at <= now())` sees the statement's clock.

Casts are performed (`builtins.Cast`): every family pair PostgreSQL
allows, with its text forms and error codes, `DECIMAL(p,s)` /
`VARCHAR(n)` typmods applied on the cast, `regclass` resolved through
the catalog, and the catalog-only pseudo types (`name`, `oid`, `regtype`,
...) passing values through. A cast chain (`x::text::int`) applies in
order. `typing.go` types computed outputs statically (`exprFamily`:
arithmetic promotes INT8 → DECIMAL → FLOAT8, `^` and the float builtins
are FLOAT8, predicates BOOL, a cast its target, a builtin its declared
result or the family of the argument it mirrors), and `conformTo`
coerces each produced datum to the described family so the wire type
and the value agree. A cast column keeps the column's name.

Predicates evaluate three-valued: `cond3` returns TRUE / FALSE /
UNKNOWN and a predicate used as a value renders UNKNOWN as NULL, while
WHERE and HAVING treat it as not matching. `BETWEEN` lowers to two
conjuncts so the planner can turn it into scan bounds; a `LIKE` with a
literal prefix becomes bounds on a keyed column (`withLikeBounds`), the
rest of the pattern staying a residual regexp filter; `SIMILAR TO`
translates SQL regular expressions to Go's; `IS [NOT] DISTINCT FROM`,
`IS [NOT] TRUE / FALSE / UNKNOWN` and `ESCAPE` are null-aware
operators in `applyCmpOp`. Integer arithmetic overflow is `22003`;
date arithmetic (`DateArith`) adds days to dates, text intervals to
timestamps (a month step clamps to the end of the shorter month) and
subtracts dates to a day count. Timestamps are int64 nanoseconds, so
the representable range is 1678-01-01 to 2261-12-31 and a value
outside it is refused at parse time rather than wrapped.

Aggregates (`aggregate.go`) take an expression argument, `DISTINCT`,
`FILTER (WHERE ...)` and `WITHIN GROUP (ORDER BY ...)`: an `aggSpec`
per select item accumulates over the evaluated argument — sums exactly,
counts, string / array / json aggregation, the boolean aggregates, the
statistical ones (two-pass over the collected values), the percentiles collected then sorted — in
single-table and join grouping alike (`join_agg.go` canonicalizes the
expression text so `GROUP BY` output naming stays stable).

## Catalog

Table descriptors are JSON documents stored in system keys (range 1):

- `/system/desc/<tableID>` → `{ID, Name, Columns[{ID, Name, Type, NotNull}],
  PrimaryKey: [colID...]}`
- `/system/ns/<name>` → tableID (namespace index)
- `/system/idgen` → next descriptor ID (incremented transactionally)
- `/system/seqdesc/<seqID>` → sequence descriptor `{ID, Name,
  DatabaseID, Increment, MinValue, MaxValue, Start, Cache, Cycle,
  OwnerTable, OwnerColumn}`; `/system/seqns/<dbID>/<name>` → seqID;
  `/system/seq/<seqID>` → the counter, the last value handed out. The
  counter is advanced with a non-transactional `Increment` of
  `Cache × Increment` per block a gateway takes (`pkg/sql/sequence.go`),
  which is what makes `nextval` non-rolling-back and gappy. Columns
  carry `DefaultExpr` (the expression's text, re-parsed per statement),
  `Identity` (`always` / `default`) and `SequenceID` (the owned
  sequence).
- `Constraints` (v8, `pkg/sql/constraint.go`) — `{ID, Name, Kind:
  check|foreign|unique, Columns, Expr, RefTable, RefColumns, OnDelete,
  OnUpdate, IndexID, AutoIndex, Validated}` — and `InboundFKs`
  `[{TableID, ConstraintID}]`, the foreign keys of other tables that
  reference this one, so a parent delete finds its children without
  scanning the catalog. A CHECK is evaluated as the lowered negation of
  its expression (the row violates it when `NOT expr` is TRUE, so a
  NULL result passes); a UNIQUE constraint is a unique index of the
  same name; a foreign key is a point read of the parent on the child
  side and, on the parent side, a lookup of the children through the
  index on the referencing columns followed by the action, cascading
  into the statement's write batch under a per-statement row cap.
  `ALTER TABLE ... ADD CONSTRAINT` is a multi-transaction statement of
  the `CREATE INDEX` shape: publish (unvalidated), drain lease
  adoption, backfill the index, sweep the existing rows in chunks as of
  a boundary, mark validated.

`TRUNCATE` reuses the re-shard's layout swap: the descriptor moves the
primary rows and every secondary index to fresh index IDs in one write
and records the superseded layout in `RetiredLayouts`, where the
re-shard janitor reclaims it after the keep window — so a truncation
costs one descriptor write whatever the table's size or range count,
rolls back with its transaction, and keeps `AS OF SYSTEM TIME` reads
below it working meanwhile. `DROP INDEX` removes the index from the
descriptor and queues its keyspace for the same chunked wipe `DROP
CONSTRAINT` uses, run after the commit and the lease drain. `RENAME TO`
moves the namespace entry (the descriptor ID, and so every by-ID
reference, is unchanged); a gateway caching the old name drops that
entry at its next lease renewal, which the statement's drain waits for.

A view is a table descriptor carrying its query's text and no rows
(`ViewQuery`, cluster version v9). Before a statement runs — or is
described — every view it names is bound as a leading implicit `WITH`
member (pkg/sql/view.go over the relation machinery of pkg/sql/cte.go):
the view's query executes once as the member and the statement reads
the materialized rows through the ordinary access path, so a view works
wherever a table does and a view over a view expands as the member
executes. Views record the relations they read (`ViewDepends`) for the
drop / rename refusals; DML and physical DDL on a view are refused.

`ALTER COLUMN TYPE` is the third online state machine: publish a
hidden shadow column (`RetypeFrom` names the original; every
`rowenc.EncodeValue` derives the shadow's value from the original's,
so concurrent writers converge with the backfill), drain, convert the
existing rows in chunks as of a boundary, then swap the shadow into the
column's slot and drop the original — a failure after publish removes
the shadow and drains. `CREATE TABLE ... AS` creates the table, drains,
runs the query once and streams the rows through the `COPY` chunk
path; a failure drops the table again.

DDL runs inside a normal transaction. Each gateway caches descriptors;
descriptor **versions and leases** make that cache safe across gateways:

- Every descriptor carries a `Version`, bumped by each change.
- A gateway that caches a descriptor writes a lease record at
  `/system/lease/<descID>/<gatewayUUID>` — `{version, expiration}` — and
  may serve from its cache only while the lease is unexpired (TTL 10s by
  default, configurable in the server config). A background
  loop renews leases at TTL/3 by re-reading the descriptor, which is also
  how a gateway adopts new versions. The lease is written in the same
  serializable transaction that read the descriptor, so a record can
  never claim a version a schema change has already superseded: written
  in a separate transaction, a gateway whose previous lease had lapsed
  could read version 1, let the change commit version 2 and drain past
  it (a lapsed lease is nothing to wait for), and then record — and
  serve for a TTL — version 1.
- After a DDL statement's transaction commits, the issuing session
  **drains**: it waits until every live (unexpired) lease on the descriptor
  is at the new version or later. Expired leases cannot be used, so a
  crashed or partitioned gateway delays the drain by at most one TTL
  (hard cap 2×TTL). When the statement returns, no gateway is still
  planning against the old schema.
- The cache entry and the lease record share one expiration (the value
  written into the record), so a gateway never serves a descriptor the
  drain has already written off; and every transaction that plans against
  a leased descriptor takes the lease's expiration as its **commit
  deadline**: it may not commit at or past it, and instead fails with a
  retryable error (`40001`; implicit statements retry transparently and
  re-plan). The server commits at exactly the write timestamp the client
  sends, so the check is the client's, and it runs on both commit paths:
  the classic EndTxn and the one-phase commit an implicit single-range
  statement takes (issue #133 — the latter had skipped it, on the most
  common write path). Without it, a statement that
  planned under a lease just before it expired could commit after the
  drain took its backfill boundary, and an index build would miss the
  row (issue #110).

The deadline alone was not enough, because it assumed the drain waits for
every lease a statement could be running under. It did not: a gateway
whose renewals are perfectly healthy publishes the new version within a
third of a TTL, which ended the drain — while a statement it had already
served from cache was still pinned to the old descriptor and still able
to commit, up to that entry's expiration. Under `CREATE INDEX` that
statement's row reached the table and no index (issue #185).

So the lease record also carries `prior_expiration`: when a gateway
adopts a new version having handed the previous one to a statement, it
publishes the superseded entry's expiration, and the drain waits for that
as well as for the version. A statement's deadline is exactly that
expiration, so once it passes nothing can still commit against the old
schema. The cost is that a schema change waits out the superseded entry —
up to one TTL — whenever a gateway served that descriptor from cache
within the last renewal interval.

A gateway does not hold a lease for ever (issue #311). An entry no
statement has resolved a table through for three TTLs is dropped from
the cache, and its lease record is deleted rather than renewed. By then
every deadline taken under the entry has passed, so the record protects
nothing; with it gone, a schema change on a table the gateway has
stopped using waits for nothing from that gateway, where a record left
to lapse would have held it up for the rest of a TTL. The delete is
conditional on the record still being the evicted entry's: a statement
that resolves the table again in the meantime takes a new lease, and the
late delete leaves it alone. Using the table again is an ordinary first
use — the descriptor is read, and the lease taken, in one transaction.

The cache is bounded too: 1,024 descriptors by default (`DescCacheSize`
in the server config), which is also the most leases a gateway renews —
about three hundred lease writes a second at the default TTL when the
cache is full. A table resolved while it is full is neither cached nor
leased. Each statement then plans against the descriptor its own
transaction read, which costs two point reads and needs no deadline (a
schema change that commits after the read fails the transaction's
refresh), until idle entries make room. An entry in use is never pushed
out to admit another: it is the gateway's record of the deadlines a
drain must still wait for.

A statement whose descriptor came from the uncached path holds no
deadline and needs none: it read the descriptor inside its own
transaction, so a schema change committing after that read fails the
transaction's refresh (tracked in issue #22, now covered for both
paths).

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

A row value has no out-of-line part: a large column value makes a
large KV value, and the limits on that are not SQL's. Measured on three
nodes (issue #303): a 32 MiB `BYTEA` INSERT takes about a second and
grows the process by over a gigabyte (the value is copied at each of
parse, datum, row encoding, batch, Raft entry, apply — on three nodes);
an `UPDATE` of another column of that row rewrites the whole value and
costs the same; reading only its length reads all of it.
`statement_memory_limit` does not come into it — it bounds what a
statement collects, not what it is handed.

The hard limit is the Raft entry. A write batch for one range becomes
one entry, and an entry that reaches `rpc.MaxMessageBytes` (64 MiB) is
appended to the leader's log and can never be sent to a follower: the
client got `XX000` ("leadership lost with proposal in flight") and the
range stalled while leadership churned around the entry. The gateway
now refuses such a batch before sending it
(`kvclient.MaxWriteBatchBytes`, 60 MiB of keys and values to one
range), and SQL reports `54000` with the advice to use a large object.

## Large objects (v21)

`pkg/sql/largeobject.go`. PostgreSQL's large-object functions over
three system tables (reserved ids `1<<40 + 5..7`, sealed like the audit
table: only a node's own session writes them):

```
datax_largeobject_metadata (loid PK, cid, owner, created_at)   -- an object exists iff its row does
datax_largeobject          (cid, pageno, data; PK (cid, pageno)) -- 256 KiB pages of a content
datax_largeobject_pending  (cid PK, loid, kind, node, at)      -- contents nobody may be standing for
```

**Pages.** An object's bytes are rows of at most
`LargeObjectPageSize` (256 KiB); the object's size is where its last
page ends; an absent page, or a short page before the last, reads as
zeros. Pages are written and read at the KV level with the table's own
row encoding (`rowenc`), so the tables are ordinary to everything
below: ranges split between pages, snapshots, the consistency checker,
MVCC garbage collection, backup and encryption at rest need nothing.
A descriptor (`lo_open`) holds a position and one page, so a stream of
small `lowrite` calls costs one KV write per page, not per call.

**Content ids.** Pages are keyed by a content id (`cid`, a
`unique_rowid`), not by the object id, because an object id may be
chosen by the client and reused while the pages of the object that had
it before are still waiting to be deleted.

**The two write paths.** Writes to an object that was already committed
go through the session's transaction (`txn.Put` of the page rows): they
are isolated and atomic like any row write, a writer locks the object's
metadata row so that an unlink waits for it, and they are capped at
64 MiB per transaction because they are intents. A new object created
in a transaction block is different: its pages are written *outside*
the transaction, as non-transactional puts, while the block is open.
Nobody can see them, because the metadata row that would lead to them
is not written until `COMMIT`. This is the issue's "many transactions
with a commit record": the commit record is the metadata row.

**Orphans and the sweeper.** Before the first page of an upload, the
gateway writes a pending row (kind `upload`) outside the transaction.
At `COMMIT` the session's transaction reads that row under a lock,
deletes it and writes the metadata row — one atomic step — so the
pending row survives exactly when the object does not. A rollback or a
closed session rewrites the row as kind `unlink` on its way out;
`lo_unlink` of a committed object deletes the metadata row and writes a
pending row of kind `unlink` in its transaction. The sweeper
(`SweepLargeObjects`, driven by the range-1 leader every minute)
deletes the pages of every `unlink` row and then the row. For an upload
whose gateway died there is nobody to mark the row, so a live upload
refreshes its row's timestamp every 15 seconds of activity and the
sweeper claims an `upload` row older than the grace period (10 minutes)
by rewriting it as `unlink` in a transaction. The heartbeat, the commit
and the claim all read the row before changing it, so they are
serialized on it: an upload either commits with all its pages, or
learns (`55000`) that it was reclaimed.

An earlier shape kept the uploading transaction's intents on the
metadata and pending rows for the whole upload, which needs no
heartbeat. It was dropped because every scan of those tables — a
backup's export in particular — waits for a live transaction's intent,
so a ten-minute upload would have held up backups for ten minutes.

**Visibility.** A transaction that can see the metadata row must see
every page. Pages carry the timestamps at which they were written,
which are later than the uploading transaction's; so before writing the
metadata row the transaction is moved forward to the present
(`Txn.AdvanceTo`, the read-refresh a timestamp conflict uses), and
commits above every page. A backup taken at any timestamp therefore
holds an object whole or not at all.

The move can fail: it is a read refresh, so if anything the transaction
read has changed since, the commit is `40001` and the upload is lost
(its pages go to the sweeper). A transaction that only uploads has read
one thing, the absence of the metadata row. libpq reads the catalog in
the transaction (its lookup of the functions' OIDs in `pg_proc`), which
is why the nodes create the tables ahead of use rather than on first
use — creating them is a catalog write — and why DDL during a libpq
upload fails that upload.

**Fast path.** libpq's large-object API calls the functions through the
protocol's function-call message rather than SQL. `pkg/pgwire/fastpath.go`
serves that message for the large-object functions only, by running
`SELECT fn($1, ...)` with the decoded arguments, and encodes the result
at the width PostgreSQL declares (four bytes for descriptors, counts and
object ids, eight for the 64-bit positions).

**Savepoints.** Non-transactional page writes cannot be undone, so
`ROLLBACK TO SAVEPOINT` in a transaction that has created an object
marks the transaction as unable to commit.

Not built: a `BLOB` column type that owns its object (the lifecycle
would need a reference from row to object and a sweep of dropped rows;
the issue asked for one interface to be picked), streaming through
`COPY`, compression (the values this is for are already compressed or
sealed), and out-of-line storage of ordinary column values in the
manner of TOAST — a different feature, which would make large `BYTEA`
values cheaper to carry beside small columns but would still write and
read each value whole.

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
table size. The chunks' serializable reads together cover the **whole**
primary span — the last chunk runs to the span's end, and an empty table
still gets one — because those reads are what the timestamp cache
remembers: a writer that planned under a lease the drain has since
written off is either waited for (its intent already down: the chunk
indexes the row once it commits) or pushed above the backfill, where its
commit fails the lease deadline and re-plans with the index (issue #110).
A tail no chunk read would let such a write land in the past, below the
boundary, and never reach the index.

**The index is emptied between the drain and the backfill**
(`clearDrainEntries`). While step 1's drain waits, gateways adopt the
write-only index one at a time, so for up to a renewal interval some
writers maintain it and some do not. One that does writes a row's
entries; one that does not may then change or delete the row, and
leaves them. The backfill only adds and a writer only removes what it
believes a row had, so those entries would stay: a row returned for a
value it no longer has, or `entry points at a missing row`. (This is the
anomaly a delete-only state exists to prevent in other online schema
change designs.) After the drain every writer that can still commit
maintains the index, so one pass over the index's span deletes every
such entry, along with good ones — which the backfill, reading every
row after it, writes again. Found by the full-text search work (issue
#299), whose test builds a second index under writers; it applies to
every kind of index (`TestCreateIndexDiscardsEntriesWrittenDuringItsDrain`).

Left for later: a delete-only state for online index drops, and the
long-lived-transaction descriptor pinning noted under Catalog.

## Inverted indexes (v21)

`CREATE INDEX name ON table USING GIN (jsonb_col)` adds an **inverted**
index (issue #300): one entry per *element* of the column's value per
row, rather than one per row. It is an `IndexDescriptor` like any other
with `Inverted` naming its kind (`"jsonb_ops"`), so it shares the
ordinary index's key space, lifecycle and DDL; `UNIQUE`, more than one
column and a non-JSONB column are refused. `jsonb_ops` may be named, and
`jsonb_path_ops` is accepted as the same thing (PostgreSQL's smaller
class serves `@>` only; the one kind here serves it as selectively).

**The split.** `pkg/sql/inverted` holds everything that does not depend
on what is indexed; a `Kind` supplies the two things that do:

- `Elements(value)` — the distinct elements of a value, as opaque
  bytes (write path, backfill);
- `Query(op, rhs)` — for `column op rhs`, an `Expr`: a tree of AND/OR
  over element lookups that **every matching value satisfies** (read
  path). `nil` means the predicate says nothing the index can use.

That one-sided contract — the expression may admit rows the predicate
rejects, never miss one — is the whole correctness argument, and it is
tested as such (`TestJSONBQueryNeverMisses`). The text-search kinds
(below) supply the same two functions and inherit the rest: key layout,
maintenance, backfill, re-shard, truncate, backup, evaluation, recheck,
EXPLAIN.

**Keys.** `/t/<tableID>/<indexID>/ + EncodeBytes(element) + <primary
key>`, value a one-byte marker (`rowenc.IndexEntries`). The escaped
element is self-delimiting, so the primary key reads back from any
entry, and all rows holding one element are one contiguous span in
primary-key order — which is what makes intersecting two elements a
merge of two sorted lists.

**JSONB elements** (`pkg/sql/inverted/jsonb.go`). A *path* element is
one scalar with the way to it:

```
0x02 | step... | value
step   0x10 + EncodeString(key)   into an object
       0x11                       into an array (any position)
value  0x20 null | 0x21 false | 0x22 true
       0x23 + EncodeDecimal(n)    a number
       0x24 + text                a number the decimal type refuses
       0x25 + bytes               a string (to the end)
```

- An array step has no position: containment ignores order and
  repetition, so `[1,2,1]` has two elements.
- The value's type is its own tag: `"1"`, `1` and `true` never meet.
- Numbers use the order-preserving decimal encoding, so `1`, `1.0` and
  `1e0` are one element — `@>` compares numerically — and the numbers
  under a path sort numerically. Elements order by path, then type, then
  value; nothing reads ranges yet, but the layout allows it.
- Empty objects and arrays are not elements.
- A *key* element, `0x01 + string`, is one string the `?` operator
  finds at the top level: an object key, a string in a top-level array,
  or the document itself when it is a string. It exists so a key whose
  value is a container is one lookup, not a scan of every path under it.
- An element over 128 bytes (a long string, a deep path) is stored as
  `0xfe` + the first 20 bytes of its SHA-256. Keys therefore have a
  bounded length whatever the document; lookups are by exact element, so
  a digest finds what a digest stored, and a collision is a false
  candidate the recheck drops.
- A document with more than 8,192 distinct elements is refused at write
  (`54000`) rather than half indexed. Nesting is bounded only by the
  JSON parser (10,000 levels): depth costs digests, not key length.
- A NULL document has no entries.

**Queries.** For `doc @> v`: each scalar of `v` must be in the document
at the same path, ANDed. A top-level scalar is contained by itself or by
a top-level array holding it (two lookups, ORed) — the top-level-only
rule. An empty object or array in `v` asks only for a shape and
contributes nothing, except that a top-level key holding one still
requires the key (`doc @> '{"a": {}}'` looks up key `a`). `doc ? k` is
one key lookup, `?|` their OR, `?&` their AND. The clause's own shape is
handled once for every kind (`invertedExprFor`): top-level conjuncts are
ANDed and a conjunct the index cannot use is left out; an `OR` is the
union of its branches and usable only when **every** branch constrains
the index. The column must stand alone on the left and the right-hand
side must be known before any row is read (a literal or a parameter).

**The recheck.** Every row the index returns is filtered with the
complete WHERE clause. This is required, not belt and braces: digests
collide, positions are dropped (`'{"a":[{"b":1},{"c":2}]}'` holds both
elements of `'{"a":[{"b":1,"c":2}]}'` without containing it), empty
containers are not elements, and a conjunction may skip terms (next).
No `LIMIT` is pushed below it.

**Evaluation** (`inverted.Expr.Eval`). There are no per-element
statistics, so a conjunction finds its selective elements by looking:
each element is read up to 1,024 entries. Those that end within the
limit are intersected; those that do not are common elements, and are
left to the recheck — sound because fewer terms admit more rows. Only
when every term is common are all read in full and intersected. A
disjunction reads every branch. The result is a sorted list of primary
keys, fetched in batches like any index join, so rows arrive in
primary-key order.

**Maintenance.** `IndexEntries` returns every entry a row has in an
index — none or one for an ordinary index, one per element for an
inverted one — and the write path, the backfill and the re-shard copy
all go through it. `UPDATE` computes both rows' entries and writes only
the difference, so changing one value of a document touches the entries
of that value. Entries ride the row's write batch: same transaction,
same intents, atomic with the row. The online build is the ordinary one;
its argument holds for many entries per row because a chunk still
derives every entry from a row it read serializably, and a concurrent
writer's blind delete of an entry the chunk is about to write pushes the
chunk past the writer and fails its refresh.

**Planning.** An inverted scan ranks below both point lookups. Without
statistics it ranks below a scan that pins a key column by equality and
above a range alone or a full scan; with statistics it is costed like an
index scan at an assumed selectivity of 1/100. `SET inverted_index_scan
= on | off | auto` forces or forbids it (`on` still yields to a point
lookup) — for comparing the two paths, as the differential test does.

**Version gate.** `Inverted` is an `omitempty` descriptor field: a v20
node would drop it and treat the index as an ordinary one over a column
type it cannot key. `CREATE INDEX ... USING GIN` is refused until the
cluster has finalized v21.

Left for later: range lookups over a path's values (`doc->'n' > 5`),
containment under a `->` path, `<@`, and per-element statistics.

## Full-text search (v21)

Issue #299. `pkg/sql/tsearch` is the whole of it that is not SQL: it
imports nothing of the SQL layer, so `pkg/sql/types` can parse and print
the two types with it. Everything in it is a pure function.

**Types.** `TSVECTOR` and `TSQUERY` are two families (`types.Tsvector`,
`types.Tsquery`) whose datum holds the value's **canonical text** in
`S`, as JSONB's does: lexemes sorted bytewise, positions sorted and
distinct, one weight per position; a query printed the way PostgreSQL
prints one. Equal values have equal text, which is all `=`, `GROUP BY`
and `DISTINCT` need; there is no order. A value is parsed when it is
used (a recheck parses the row's vector; a query is parsed once per
row it is matched against — both are small next to fetching the row).
Rows store them under value tags of their own (16, 17). On the wire:
OIDs 3614 and 3615 (arrays 3643, 3645, never produced), text as stored,
binary in PostgreSQL's `tsvectorsend` / `tsquerysend` layouts
(`tsearch/binary.go`; pgx's `TSVector` codec round-trips against it in
`TestTextSearchOverPgwire`).

**From text to lexemes.** One tokenizer for every configuration
(`Tokenize`): at each position, an e-mail address, else a host name
(dotted labels ending in letters), else a decimal number, else a run of
Unicode letters, digits and marks; everything else separates. Tokens are
lower-cased by Unicode case mapping. It is much simpler than
PostgreSQL's parser, by choice: 23 token types and a hand-written state
machine buy little for prose, and every divergence is a documented rule
rather than a bug to chase (`docs/user/postgres-differences.md`).
A configuration then maps a token to a lexeme or drops it: `simple`
keeps it; `english` drops the 127 Snowball stop words and stems
all-letter word tokens with `Stem`. Positions count every token, stop
words included, from 1.

**The stemmer** (`tsearch/stem.go`) is Snowball English (Porter2)
written from the algorithm's description — no new dependency. It is
tested against the description's published sample (80 consecutive
vocabulary words with their stems), its per-step examples, and a few
hundred hand-derived pairs (`stem_test.go`). It has not been run against
libstemmer or a PostgreSQL server; later Snowball revisions (PostgreSQL
17+) changed a handful of words. An index and the queries against it use
the same code, so a disagreement with PostgreSQL changes which words are
conflated, never whether the index and the filter agree.

**Queries.** A `tsquery` is a binary tree of `&`, `|`, `!`, `<N>` over
operands (lexeme, prefix flag, weight set). `ParseQuery` reads the
literal syntax; `ToQuery` the same syntax with each operand normalized
by a configuration (several tokens become a phrase, a stop word a
placeholder); `clean` then removes the placeholders the way PostgreSQL's
`clean_stopword` does, widening the phrase distances around them.
`PlainToQuery`, `PhraseToQuery` and `WebsearchToQuery` build trees
directly.

**Matching** (`tsearch/match.go`) follows PostgreSQL's `TS_execute`.
Outside a phrase it is boolean logic over "the vector holds the lexeme"
(with one of the operand's weights, if it names any). A phrase evaluates
each side to the set of positions it ends at and keeps the positions of
the right side that have the left side exactly `N` before them; a
negation inside a phrase is a complemented set. A vector without
positions matches words and never phrases. `TestMatchRandom` checks it
against a brute-force definition that shares no code with it.

**Session default.** `to_tsvector(text)` and its relatives depend on
`default_text_search_config`. The session resolves that before the row
loop, where it resolves `now()`: `resolveValueExpr` rewrites a call that
names no configuration into the two-argument call with the session's
value as a literal (`builtins.TextSearchDefaultArg`). The builtins are
then pure, and the planner sees `to_tsvector('english', body)` whichever
way the statement spelled it. `ts_headline`'s forms cannot be told apart
by count — `(a, b, c)` is `(config, document, query)` or `(document,
query, options)` — so it is rewritten to a hidden builtin that carries
the default and decides from the argument types at run time.

**Indexes.** Three inverted kinds (`pkg/sql/inverted/tsearch.go`), one
implementation:

- `tsvector_ops`, over a `TSVECTOR` column: `Elements` are the vector's
  lexemes.
- `to_tsvector_english`, `to_tsvector_simple`, over a `TEXT` column:
  `Elements` are the lexemes of `to_tsvector(config, value)`. This is
  how `CREATE INDEX ... USING GIN (to_tsvector('english', body))` is
  stored — the parser accepts exactly `fn('constant', column)` as an
  index key and the DDL maps it to the kind — because there are neither
  expression indexes nor generated columns to build PostgreSQL's usual
  shape from. It costs no descriptor field: the index's column is the
  text column and `Inverted` names the kind. A kind that indexes a
  function of its column says so (`inverted.Computed`), and the planner
  then requires the conjunct's left-hand side to be that call on that
  column (`invertedLHS`) instead of the column itself.

An element is **the lexeme's bytes and nothing else** — no tag, no
position, no weight. Positions stay in the vector, so the index answers
"which rows hold this word" and the recheck answers everything finer.
A lexeme over 128 bytes is stored *cut* to 128, not digested as JSONB's
long elements are: a cut lexeme still starts with its own prefixes, and
prefix search depends on that.

`Query("@@", tsquery)` maps the tree onto lookups: an operand is a leaf,
`&` is AND, `|` is OR, a phrase is the AND of its sides (adjacency is
the recheck's), and a negation is `nil` — unconstrained — at any depth,
because the index lists the rows that hold a word and not the ones that
lack it. `And` drops unconstrained children and `Or` is unconstrained if
any child is, so `a & !b` looks up `a`, and `!b` or `a | !b` scan.
`TestTextSearchQueryNeverMisses` holds the kind to the contract on
random documents and queries.

**Prefix lookups** are the one addition to the kind-agnostic layer:
`inverted.Prefix(p)` (`OpPrefix`) admits the rows holding any element
that starts with `p`. Elements sort bytewise in the key space, so that
is one span (`rowenc.InvertedPrefixSpan`: the escaped prefix without its
terminator). Its rows come back ordered by element first, so the scan
function sorts and de-duplicates them, and now reports whether a probe
limit cut it short rather than leaving the caller to infer that from a
count; a conjunction treats a prefix too common to probe like a common
element. A kind may use `Prefix` only for elements it never digests.

**Ranking** is computed on the rows the query returns: `ts_rank` and
`ts_rank_cd` are ordinary functions (`tsearch/rank.go`, PostgreSQL's
formulas). `ORDER BY rank DESC LIMIT k` therefore fetches and rechecks
every match, ranks, sorts and cuts. A top-k that reads less needs
per-entry scores or positions in the index (a RUM-style index); not
attempted.

**Version gate.** No descriptor field is new, so table restore's
allow-lists are unchanged. The two families and their value tags are
new, and a v20 node can decode neither: any statement that would give a
table a column of either type (`CREATE TABLE`, `CREATE TABLE AS`, `ADD
COLUMN`, `ALTER COLUMN TYPE`) is refused until v21 is finalized, as
`USING GIN` already is. The functions and `@@` need no gate.

Left for later: more configurations (a stemmer and a stop list each);
PostgreSQL's richer token types; generated columns, which would make the
weighted title-and-body vector maintainable by the database; `query @@
vector` and negation-aware planning; ranked top-k.

## Vectors (v21)

Issue #304: pgvector's surface — `vector(n)`, the distance operators
`<->` (L2), `<=>` (cosine) and `<#>` (negative inner product), `ORDER BY
embedding <-> $1 LIMIT k` — in two steps: exact search, then an
approximate index.

**The type.** `types.Vector` is a family like any other: a datum holds
its elements in `Datum.S` as big-endian float32, which is at once the
stored form (rowenc tag 18, length-prefixed), the payload of pgvector's
binary wire form (`int16` dimension, `int16` zero, the elements) and
what an index entry carries, so a value is never rebuilt on its way
through. `pkg/sql/vector` has the arithmetic and knows nothing of
datums: parse and format (shortest float32 text), the three distances
accumulated in float64, k-means. `vector(n)`'s dimension is the column's
`MaxLen` — the one number the type takes, kept where a string's length
is so that every place a declared type is copied (CTAS, LIKE, ALTER
COLUMN TYPE, the result column) carries it without a new field — and
`Column.Conform` refuses a value of another dimension (`22000`). `NaN`
and infinities are refused at parse and at binary decode. The type has
no order-preserving key encoding (`types.IsIndexable` is false: a
6 KiB key per row would be the result), so it is not a primary key or an
ordinary index column; `Datum.Compare` orders vectors element by
element for `=`, `ORDER BY` and grouping. Float comparison was made a
total order while here (`NaN` equal to itself and after every number, as
PostgreSQL has it): the cosine distance to a zero vector is `NaN`, and a
sort over distances containing one had no consistent order.

**The OID.** pgvector is an extension, so its type's OID differs per
database and clients look it up by name. Here it is a constant
(`vector.OID`, 16000: in the range PostgreSQL leaves to initdb-time
objects, below the first user OID, so it meets no built-in and no
enum), answered by `to_regtype('vector')` and `pg_type`. `CREATE
EXTENSION vector` parses and does nothing; any other name is `0A000`. A
`$n` that is an operand of a distance operator, or that the client
declared with the vector OID, is typed as a vector in
`ParameterDescription`, which is what makes a registered client send
it in binary. (`parser.CountParams` did not look at `ORDER BY`
expressions; a statement whose only parameter was in one described no
parameters.)

**Operators.** The lexer takes `<->`, `<=>` and `<#>` as three-character
tokens (none is a prefix of anything valid: after `<-`, `<=` or `<#`, a
`>` could only begin an operand). They parse one level above `+` and
`-` (`parseAddExpr` → `parseSumExpr`), PostgreSQL's precedence for
"any other operator", as ordinary `BinOp` nodes, and evaluate through
`builtins.VectorOp` with a vector, a text literal or a numeric array on
either side.

**Exact search: the top-k scan** (`pkg/sql/topk.go`). `ORDER BY <expr>
LIMIT k` over a scan the access path does not deliver in order used to
fetch every matching row, sort, and cut. Now, when nothing between the
fetch and the sort needs the whole set (no DISTINCT, correlated filter
or `FOR UPDATE`) and the plan is one span of a real table's primary
index, the scan reads the span in pages of 512 keys and keeps a heap of
the `k` (limit + offset) best rows, worst on top; a row that does not
beat the worst is dropped where it stands, and a row that leaves the
heap gives back its charge against `statement_memory_limit`. Memory is
`k` rows and a page whatever the table. Ties keep scan order — equal
keys are ordered by the sequence the scan produced them in — so the
result is the stable sort's, row for row; the `k` survivors then go
through `sortRows` as before. `sortRows` and the heap share one key
evaluator (`orderKeyer`). The constant operand of a distance term is
read as a vector once per statement (`foldDistanceConstants`) and the
per-row distance is computed over the two stored forms without
decoding either (`vector.Metric.StoredDistance`): about 3 µs a row at
64 dimensions, most of it the scan and the row decode. This path is for
any ORDER BY ... LIMIT, not only distances. `EXPLAIN`: `...; top-k
heap: the first 10 rows in order kept while scanning, no full sort`.

**The index: IVF** (`pkg/sql/vector_index.go`, `catalog.VectorIndex`).
`CREATE INDEX ... USING ivfflat (col vector_l2_ops | vector_cosine_ops |
vector_ip_ops) WITH (lists = n)`. An "ordered key → rows" index cannot
hold nearness, and of the two families of approximate index the
clustering one maps onto an ordered, range-partitioned store and the
graph one does not:

- IVF: the vectors are clustered around `lists` centres; a row's entry
  is `/t/<table>/<index>/<list>/<pk> → vector`. A list is one contiguous
  key span, so a probe is one scan, lists split, merge and move as
  ranges like any data, and maintaining a row's entry is one key in the
  row's own transaction, a function of that row alone.
- HNSW: a query is hundreds of dependent point reads across ranges, and
  an insert rewrites the neighbour lists of *other* rows — writes to
  keys the inserting transaction did not otherwise touch, so unrelated
  inserts conflict under serializable isolation, and every one is a
  multi-range transaction. `USING hnsw` is refused by name.

*Centres.* `trainVectorIndex` runs before anything is published: it
reads the table at one timestamp without a transaction, keeps a
reservoir sample (50 rows per list, at least 2,000; only sampled rows
are decoded), runs k-means++ and at most 12 rounds of Lloyd's algorithm
(`vector.Train`; spherical — unit centres, direction only — for the
cosine and inner-product classes, which also file and probe by inner
product with the centre), and commits the centres at
`/t/<table>/0/"ivf"/<build>/<list>` (`keys.VectorCentroidPrefix`),
where `<build>` is a random identifier new for every CREATE INDEX. Only
then does the descriptor gain the index (`IndexDescriptor.Vector`:
method, metric, lists actually trained, dimension, build), in the
write-only state, and the ordinary online backfill runs. So no writer
ever sees the index without its final centres. Fewer distinct sample
vectors than `lists` train fewer lists; an empty table trains one, at
the origin — an exact index no faster than a scan.

*Why the centres never move.* An UPDATE or DELETE finds a row's entry
by recomputing its key from the old row (`rowenc.IndexEntries`, the one
encoder the write path, the backfill and the re-shard copy share): the
list is `Centroids.Assign(vector)`, a pure function. A centre that
moved would strand every entry filed before it did. The cost is drift:
rows unlike the training sample are filed under centres that fit them
badly and recall at a given `probes` falls. The remedy is a rebuild
(a second index, then drop the first — a new build, new centres); there
is no REINDEX and no incremental retraining.

*Why under index 0.* TRUNCATE and a re-shard move every index to a
fresh ID and retire the old span; centres under the index's own ID
would be wiped with it. Index ID 0 is never an index (the primary is 1),
so keys under it belong to the table: they survive renumbering, and a
backup, a table restore (which re-keys the whole table prefix; the
`vector` field is on its allow-list) and DROP TABLE carry or remove
them with the rows. DROP INDEX deletes them after its commit, with the
entries.

*How the encoder gets them.* `rowenc` has no transaction to read with.
The centres of each build are kept in a process-wide registry keyed by
build ID (`vector.Lookup`) — immutable, so never stale, and a build ID
is random, so two clusters in one test process do not collide.
`Session.lookup` (and the cascade's lookup by ID) loads the centres of a
table's vector indexes into it, outside the caller's transaction, before
the descriptor is used; an index whose centres cannot be read whole is
left unloaded and writes to its table fail by name
(`rowenc.ErrCentroidsNotLoaded`) rather than file a row under a guess.
Entries are dropped from the registry on DROP INDEX on that node and
otherwise live as long as the process.

*Maintenance.* One entry per row with a non-NULL vector, written,
moved or removed in the row's transaction like any index entry.
`entryDiff` now also rewrites an entry whose key is unchanged but whose
value differs: a new vector filed under the same list.

*Reading* (`pickVectorIndex`, `executeVectorScan`). The shape is one
ascending distance term as the only sort key, a LIMIT, the top-k
conditions above, an access plan that would otherwise be a full scan,
and a public index on the column for the term's metric. The scan ranks
the centres, reads the lists of the nearest `ivfflat.probes` (default
`⌈√lists⌉`; pgvector's default of 1 is a poor recall to hand out
unasked), computes each entry's true distance from the vector in the
entry, sorts the candidates (NaN last, ties by primary key, as the
exact scan breaks them), and fetches rows nearest first through
`fetchPrimaryRows` → `decodeFullRow` → `matchesWhere` — the ordinary
row path — until `k` have passed the full WHERE clause. The filter is
therefore applied to every candidate of the probed lists in distance
order, not to a pre-cut top `k`. A WHERE clause the primary key or an
ordinary index can serve keeps that plan and the exact top-k over what
it selects: correct, and usually cheaper than probing for a selective
filter. There is no cost model between the two and no index prefix
column (`WHERE tenant = $1` inside the index); both are left for later.
`SET vector_index_scan = off` forces the exact path. Rows with a NULL
vector have no entry, so the index never returns them.

*Recall.* Approximate means: every row returned is real, with its true
distance, in true order, and rows of unprobed lists are missing.
`TestVectorIndexRecall` (20,000 × 32 dimensions in 400 Gaussian
clusters, 100 lists, 3 nodes, k = 10) measures 0.83 at 1 probe, 0.92 at
3, 0.98 at the default 10, 0.999 at 20 and exactly 1 at 100; the
`vector-knn` bench workload records recall, latency and build time
(bench/README.md).

**Version gate.** A v20 node meets rowenc tag 18 as a corrupt row and
would drop `IndexDescriptor.Vector`; VECTOR columns and `USING ivfflat`
are refused until v21 is finalized.

Left for later: a prefix column for filtered search, a cost model
against the scan, quantized entries (the entry holds the full vector:
storage doubles per index), retraining in place, `halfvec` /
`sparsevec`, vector aggregates, parallel probes across lists.

## Execution

The executor ranks access paths:

1. WHERE pins every PK column by equality → primary point `Get`.
2. Every column of a unique index pinned → unique-index point lookup.
3. The best constrained scan of the primary key or an index: equality
   conjuncts pin a leading column prefix, and range conjuncts
   (`> >= < <=`) on the **next** key column become order-preserving scan
   bounds (`WHERE a = 1 AND b > 5 AND b <= 9` scans exactly the matching
   key span). Without statistics, paths score 2 per pinned column plus 1
   for a range; ties prefer the primary key (no index join).
4. Otherwise → full `Scan` of the primary rows with in-memory filtering.

An inverted index that the WHERE clause constrains is considered between
steps 2 and 4: taken unless step 3 pinned a key column by equality (see
Inverted indexes above). A vector index is not an access path for a
WHERE clause at all: it is chosen after this ranking, from the ORDER BY,
when the ranking ended at step 4 (see Vectors above).

With table statistics present (ANALYZE or the background sampler),
step 3 ranks competing scans by **estimated cost** instead of the
structural score: estimated rows (row count × 1/distinct per equality,
×⅓ per range bound, ×⅒ for a column the statistics never saw; naive
independence, floor of one row), times a ×4 index-join multiplier for
non-unique indexes (modelling the per-entry primary-key `Get`). The
cheapest path wins, exact ties keep the primary key, and a constrained
scan whose cost reaches the full-scan cost loses to the full scan — the
case the structural planner gets wrong on low-selectivity indexed
columns. The point-lookup short-circuits (steps 1–2) are unaffected, and
with no statistics the structural ranking runs byte-identically to the
pre-statistics planner. `EXPLAIN` appends ` [~N rows]` to a plan whose
estimate came from statistics.

Conjuncts a path cannot absorb (`!=`, `IS [NOT] NULL`, columns outside
the key prefix) stay as a **residual filter**: the executor re-checks the
complete WHERE clause against every fetched row regardless of path, so
plan bounds only ever narrow the scan. `LIMIT` is pushed into the KV scan
exactly when the residual is empty (every scanned row is a result row)
and any ORDER BY is already satisfied by the access path; scanned-row
counts are observable via `datax_sql_rows_scanned_total`. An ORDER BY
the path does not satisfy, with a LIMIT, keeps a heap of the limit's
rows while scanning instead of sorting them all (see Vectors above).

**Panic barrier.** The statement path is a panic barrier (#136): a
panic anywhere below `Session.Execute` — in the planner, the executor,
a builtin, an encoder — becomes an internal error (`XX000`) for that
statement, logged with its stack and counted in
`datax_sql_statement_panics_total`; the connection goes on, and the
transaction fails as it would on any other error (an implicit one
rolls back, a block is failed until `ROLLBACK`). The barrier sits at
three points: inside the transaction's retry loop (`execStmt`, so
`RunTxn` sees an error and rolls back), around the whole statement
(`Execute`: a `COMMIT`, a DDL finish or a session variable), and on
every pull of a streamed result (`RowStream.Next`, after `Execute`
returned). What Go cannot recover — a stack overflow (the parser
bounds nesting for that, #135), out of memory — still ends the
process, and the barrier does not hide bugs: each recovered panic is
one to fix, with its stack in the log.

**Streaming.** On a wire session a scan-shaped select — one table, a
scan plan (full, primary-key range or index), no join, aggregate,
DISTINCT, window, set operation, correlated conjunct or projection, no
`FOR UPDATE`, and any ORDER BY satisfied by the access path — returns a
`RowStream` instead of rows (`pkg/sql/stream.go`): the wire layer pulls
one row at a time, the iterator fetches KV pages of 512 (an index scan
pages its entries and fetches each page's primary rows in the #103
batches), and OFFSET / LIMIT apply as rows are pulled. The stream owns
an implicit transaction (committed when drained, rolled back on error
or when the portal is released) or rides the session's explicit block
(an error fails it). A retryable error re-runs the statement on a fresh
transaction only while the wire layer has flushed nothing; the wire
layer buffers encoded rows and writes every 64 kB, reporting the first
write to the stream, after which an error is surfaced after the rows
already sent. `statement_timeout` and `lock_timeout` apply to every
pull. `EXPLAIN ANALYZE` materializes (its stage accounting needs the
counts); internal sessions (the dashboard, the metrics recorder) never
stream. Everything that materializes — scans collecting their rows,
sorts, aggregates, joins, DISTINCT, WITH members, derived tables —
charges an estimate of what it holds against `statement_memory_limit`
(64 MiB by default) and fails with `53200` past it.

Whether an ORDER BY is satisfied is decided by `orderPlan`: skipping
columns the plan pins by equality (constants order nothing), the
remaining terms must follow the path's natural key order — all ascending,
or all descending via a **reverse scan** (a v3 KV primitive; below
cluster version v3 descending falls back to the sort). On a sharded
timeseries fan-out the natural order is the LOGICAL primary key, restored
by a K-way merge of the per-bucket scans (see docs/timeseries.md); the
per-bucket scans carry the pushed limit, so `ORDER BY ts DESC LIMIT n`
reads at most `buckets × n` rows.

A non-unique index has no entry for rows with NULL in any indexed column,
so the planner only uses one when the schema (`NOT NULL`) or the WHERE
clause (equality/range/`IS NOT NULL` conjuncts) proves those rows cannot
match — and `col IS NULL` therefore always reads the primary rows. Unique
indexes reject NULLs at write time and are always complete.

Joins (INNER and LEFT OUTER, up to 8 tables) are left-deep nested loops.
Without statistics they execute **in syntactic order** — the FROM/JOIN
order you write is the plan you get. The base (first) table is
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

With statistics present for **every** joined table, INNER joins are
cost-reordered: a greedy pass picks the side with the fewest estimated
rows (after its side-local WHERE conjuncts, using the single-table
selectivities) to drive the nested loop, then repeatedly adds the
cheapest side connected to the placed set by an ON equality. The rewrite
is a pure AST transformation on a clone (prepared statements re-plan
per execution): `SELECT *` is pre-expanded into qualified references in
the original side order so output columns never move, and every ON
conjunct is pooled and re-attached, fully qualified, at the level where
its later side lands. It declines — keeping byte-identical syntactic
order — for any LEFT join (NULL-extension is order-sensitive), a derived
base table, missing statistics, self-joins or any alias/table-name
shadowing (qualified references bind first-match), or a join graph the
ON equalities don't connect. `EXPLAIN` appends ` [~N rows]` to each side
planned with statistics and `; join reordered by cost` when the order
changed. Measured on a worst-first 3-way join (400×20×5 rows, one
selective product): 24.1ms/op syntactic vs 5.1ms/op reordered — within
~3% of the same query hand-ordered (5.2ms/op).

Aggregates and GROUP BY compose with joins: the joined rows run through
the grouped executor, with `GROUP BY c.name`, `SUM(o.total)`, HAVING and
DISTINCT all accepting qualified references. ORDER BY on a grouped join
orders by *result* column name (the same rule single-table GROUP BY
follows).

Plain (non-grouped) join select lists and WHERE conjuncts evaluate full
expressions and `->`/`->>` paths against the joined row: expression
evaluation is environment-abstracted (`exprEnv` — a column-lookup seam
with a single-table and a join implementation), so the join side reuses
the same literal/parameter/function/path/arithmetic machinery with
side-resolved column references. A NULL-extended LEFT side yields NULL
for its columns, which flows through paths and arithmetic as SQL NULL —
and path conjuncts apply before the IS NULL arm, so
`left.j ->> 'k' IS NULL` keeps NULL-extended rows. Computed select items
render as TEXT (the single-table rule); path items type by their chain.
Path and computed conjuncts are never pushed into the base scan (the
post-join filter re-evaluates the full WHERE — pushing would only be an
optimization, and for paths never a bound anyway). Remaining narrow
spots, all explicit `0A000`s: grouped join SELECT lists take plain
columns and aggregates only; `@>` stays single-table; join ORDER BY
sorts by side columns, not computed aliases.

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

**Correlated** subqueries — an inner reference to an enclosing row, like
`WHERE salary = (SELECT MAX(salary) FROM emp WHERE dept_id = e.dept_id)`
— are supported in the WHERE clause of single-table SELECT (plain or
aggregated), UPDATE, and DELETE, as scalar comparisons, `[NOT] IN`, and
`[NOT] EXISTS`, over plain single-table inner selects, nesting up to
**four** subquery levels deep. A reference may reach ANY enclosing
scope, not just the nearest: names resolve nearest-scope-first (a nearer
column shadows a farther one; qualify with an enclosing alias to pin a
scope). Execution is a nested loop composing one level at a time: each
statement binds only ITS row into the subquery tree (references to
intermediate levels stay symbolic) and the substituted subquery
re-enters the same machinery when it runs, memoized per distinct
correlation key at every level — `EXPLAIN` says so (`correlated filter:
nested loop ...`), and the cost is honestly the product of the level row
counts. A name resolving in no scope is a plain `42703`; a correlated
reference in GROUP BY / ORDER BY / aggregate arguments, in a join or
derived-table outer, or nesting past the depth cap is rejected with a
clear error. The parsed statement is never mutated, so prepared
statements re-evaluate their subqueries on every execution.

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

### Plan cache

A prepared statement is parsed once, but the work after parsing — the
view check over its table names, the alias strip, the projection, the
statistics lookup and the access-path choice — ran on every execution.
Measured with the harness (kv point lookups through pgx's extended
protocol, one gateway on two cores) it was about 8 % of the gateway's
CPU: the executor's own share is small next to the KV round trip, so
the cache (`pkg/sql/plancache.go`, issue #107) is scoped to what that
supports. Each session keeps a bounded LRU (128 statements) keyed by the
statement's parsed form — a prepared statement's, or the one the wire
layer's parse cache hands the simple protocol for a repeated text — and
the current database, for the single-table `SELECT`, `UPDATE` and
`DELETE` paths. An entry holds the descriptor the statement resolved to,
the statistics it planned with, the projection, and the shape of a
primary-key point lookup (which conjuncts pin which key column), so a
hit binds the parameters into a point plan without re-planning; other
shapes reuse the projection and re-run the (parameter-dependent) path
choice. Invalidation is by identity: the catalog hands out one
descriptor pointer per version and the statistics cache one per
refresh, so an entry is used only while the lookups it skips return the
same pointers — a schema change, `ANALYZE`, a dropped table or a
recreated one all miss. A data statement also resolves each table name
once per execution (the view check and the plan share the lookup).
A statement that row-level security applies to never enters the cache
(see Row-level security above).
`EXPLAIN` appends `(cached plan)` when the plan it shows is the cached
one; `datax_sql_plan_cache_hits_total` / `_misses_total` /
`_evictions_total`, and the hit rate on the console's node page.

## Wire protocol

`pkg/pgwire` implements the PostgreSQL v3 protocol on
[`pgproto3`](https://github.com/jackc/pgx/tree/master/pgproto3):

- **Parse cache**: a connection keeps the last 64 simple-protocol query
  texts it parsed; a repeated text skips the parser and reaches the
  session as the same statement, so the plan cache applies to it
  (`datax_sql_parse_cache_hits_total`). Per connection, since the
  executor annotates statements in place.
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
  one uniform `28P01`, and a full stand-in exchange runs for unknown
  users so the flow leaks nothing — including the salt server-first
  shows before anything is proven: it is derived from the user name
  under a cluster-wide secret (`/system/auth-secret`, 32 random bytes
  the first node that needs them writes in a transaction), so one name
  sees one salt on every node and no salt marks the names that do not
  exist (#137; PostgreSQL's mock authentication does the same). Cleartext
  startup is refused in secure mode. In
  insecure mode `SSLRequest` gets `N` and authentication is trust.
  Startup is bounded three ways (#212): password guessing spends the
  same per-source and per-(source, account) budget `/api/login` does —
  asked before the exchange, charged by a failure, refunded by a
  success — and is refused with a FATAL `28000` that names no user;
  a connection that has not reached ReadyForQuery within
  `--auth-timeout` (60 s) is closed wherever in the handshake it
  stalled; and past `--sql-max-pending-auth` (512) connections still in
  startup, a new one is refused at accept with a FATAL `53300` before
  anything is allocated for it (a client mid-`SSLRequest` sees that as
  a failed TLS negotiation).
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
  The virtual catalogs are read by every role (issue #312):
  `tablePrivAllowed` grants `SELECT` on a `pg_catalog` or
  `information_schema` descriptor to anyone and nothing else, and
  `virtualEnv` decides what each lists — every table of the scope for
  `pg_catalog`, the tables `catalog.CanSeeTable` admits (and whose
  schema the reader may use) for `information_schema` — and gathers
  statistics only for tables `catalog.CanSelectTable` admits and row
  security does not confine, and a sequence's counter only past
  `sequencePrivAllowed`. A sequence read as a relation (`SELECT
  last_value FROM s`) checks the sequence's `SELECT`.
  `EXPLAIN` (without `ANALYZE`, which runs the statement and checks as
  it goes) plans by shape and evaluates nothing, so it checks first:
  `checkSelectReads` walks every table the statement names
  (`stmtTables`), skips WITH members and bound relations, checks
  `SELECT` as the reader, and checks a view's query as the view's
  owner. `SHOW STATS FOR t` takes `SELECT` on `t`: its counts describe
  the rows.
  `TRUNCATE` takes the `TRUNCATE` privilege. Until cluster version v11
  it took `DELETE`, and while a cluster is below v11 it still does (a
  node of the older binary decides so); the finalize transaction that
  moves the cluster to v11 adds `TRUNCATE` beside every `DELETE` grant
  (`catalog.GrantTruncateWithDelete`), so grants made before keep their
  meaning and later `DELETE` grants do not carry `TRUNCATE`. The
  tables a `TRUNCATE` names are all privilege-checked before any
  foreign key is looked at, and a referencing table it does not name is
  named in the refusal only to a role holding some privilege on it
  (`mayKnowTable`); `pg_constraint` lists every foreign key to every
  role, as PostgreSQL's does, so this keeps errors consistent rather
  than secret.
  Then `ParameterStatus` for `server_version` (reports a PG-14-compatible
  version string), `client_encoding=UTF8`, `DateStyle=ISO`,
  `integer_datetimes=on`, `standard_conforming_strings=on`, `TimeZone=UTC`;
  then `BackendKeyData` and `ReadyForQuery`.
- **Simple query** (`Q`): multi-statement strings split on `;`, each executed;
  results as `RowDescription` → `DataRow`* → `CommandComplete`, values in text
  format encoded via `pgtype` (OIDs: int8=20, float8=701, text=25, bool=16).
- **Extended protocol**: minimal but real, because pgx's default mode prepares
  statements: `Parse` / `Bind` / `Describe` / `Execute` / `Sync` / `Close`.
  Parameters and results in both text and binary formats for every column
  type (int8, float8, bool, text, timestamptz, date, bytea, uuid,
  numeric as PostgreSQL's base-10000 digit groups, jsonb with the
  version-1 byte). Portal suspension is supported: a row-limited Execute
  returns up to that many rows and `PortalSuspended`; re-Execute resumes,
  and portals live to the end of the transaction (destroyed by Sync
  outside one) — JDBC fetch-size loops work. The statement runs once at
  the first Execute; a streamed result is pulled MaxRows rows per
  Execute from the open scan, a materialized one is served from its
  rows.
- **Copy-in sub-protocol**: `COPY t [(cols)] FROM STDIN [WITH (FORMAT
  text|csv|binary)]` (also pgx's pre-9.0 trailing `BINARY` spelling) is
  accepted in the simple protocol, as the last statement of the query.
  The server answers `CopyInResponse` and consumes `CopyData` messages as
  one byte stream — clients cut them with no row alignment, so rows,
  fields, and escape sequences may straddle messages. Text format
  implements the PG escape rules (`\N` null, `\t` `\n` … `\\`, octal,
  hex, the `\.` terminator); CSV distinguishes unquoted-empty (NULL) from
  `""` (empty string), which is why the parser is hand-rolled and
  streaming; binary is the `PGCOPY` header + per-field lengths, decoded
  with the same per-type routines as binary parameters (a missing `-1`
  trailer is tolerated — pgx omits it). Rows execute through the shared
  INSERT pipeline (defaults, shard bucket, NOT NULL, uniqueness, index
  entries) and commit in chunks of 128 rows / 1 MiB, one implicit
  transaction per chunk whose write batch is rebuilt on every retry
  attempt (the uniqueness reads must re-execute in the fresh
  transaction). A failure reports the row number and the rows already
  committed; `CopyFail` aborts with `57014`; stray copy messages outside
  copy mode are silently discarded (PG behavior — and what keeps an
  optimistically-streaming pgx from desyncing after a refused COPY).
  COPY inside `BEGIN` is refused (`25001`), like the other
  chunked-commit statements.
- **Transaction status is load-bearing**: `ReadyForQuery` carries `I` (idle),
  `T` (in transaction), or `E` (failed transaction — everything except
  `ROLLBACK` is rejected with `25P02`).
- **Errors** carry proper SQLSTATEs: `40001` serialization_failure (retry
  me!), `55P03` lock_not_available (`lock_timeout`, `FOR UPDATE NOWAIT`),
  `42P01` undefined_table, `23505` unique_violation, `42601`
  syntax_error, `0A000` feature_not_supported.

## LISTEN / NOTIFY

`LISTEN channel`, `UNLISTEN channel | *`, `NOTIFY channel [, 'payload']`
and `pg_notify(channel, payload)` (issue #297), with PostgreSQL's
semantics on a node and delivery to listeners on every node.

**In the session** (`pkg/sql/listen.go`). Inside a transaction block
nothing takes effect: notifications (deduplicated on database, channel
and payload) and subscription changes are recorded on the session, with
a mirror of the transaction's savepoints so `ROLLBACK TO` forgets what
came after one. When `COMMIT` succeeds the subscription changes are
applied and then the notifications published, in order; a rollback, a
failed commit or the end of the connection discards them. A statement
outside a block goes through the same steps as its own transaction —
`pg_notify` there is recorded per attempt (an automatic retry starts
over) and published after the implicit commit, which for a streamed
result is when the stream is drained. `pg_notify` is one of the
functions the session resolves before execution, so it runs once per
statement and takes constants and parameters, not columns. Channel
names are limited to 63 bytes and payloads to 7999 (`22023`), and a
channel belongs to the database the session is in. There are no
privileges on channels, as in PostgreSQL.

**On the node** (`pkg/sql/notify`). Each node has a hub: the listeners
of each (database, channel), a bounded queue per listening session, and
a bounded, ordered queue per peer node. Publishing queues the
notifications for the local listeners and for every peer under one
lock, so every node sees a given node's notifications in one order, and
returns without waiting for anyone. Nothing is written to the store.

**Between nodes.** A sender goroutine per peer drains that peer's queue
in batches over the internode admin RPC (op `notify`; in secure mode
accepted from the node certificate only, and not audited per call), retrying a batch with backoff until
the peer has taken it. Batches carry the sender's incarnation and a
sequence number, so a batch retried after its answer was lost is
dropped by the receiver rather than delivered twice. The fan-out is a
broadcast to every node in the registry whether or not anyone listens
there: tracking interest per node would need a protocol of its own and
would race with a `LISTEN` that had not propagated yet. A peer that
answers "unknown admin op" — an older release, which cannot have
listeners — has its batch dropped without retry, which is why the
feature needs no cluster version gate.

**The guarantee** is at-most-once delivery to the sessions listening at
commit, strengthened so that loss is never silent for a session that
stays connected. Two bounds can be exceeded, and both end the affected
sessions with a FATAL instead of skipping notifications: a session's
queue (16 MiB; a client that does not read, or sits in a transaction —
`54000`), and a peer's queue (64 MiB, or five minutes without reaching the peer; the
peer was unreachable, or a burst outran the link to it — the sender drops the queue, marks the next batch
`lost`, and the receiver ends every listening session it has,
`57P01`; once it has dropped a queue it queues nothing more for a peer
it still cannot reach, so a dead peer holds no memory). A drained node flushes
its peer queues before it stops. What is not covered: a node that
crashes between a commit and the hand-off to a peer loses those
notifications, and so does a commit whose result was ambiguous to the
gateway; a node that joined within the last registry refresh is not
yet a fan-out target. Notifications from different nodes are not
ordered against each other.

**On the wire** (`pkg/pgwire/notify.go`). Queued notifications are sent
as `NotificationResponse` ahead of any `ReadyForQuery` with status `I`
— so a session receives what its own `COMMIT` notified before the
`COMMIT` is acknowledged, and a session in a transaction block receives
nothing until the block ends — and, as PostgreSQL does, at any moment
while the connection is idle outside a transaction. The connection's
goroutine is blocked in a socket read then, so a second goroutine per
listening connection writes them; a flag under a mutex, set only while
the first is parked in that read at a cycle boundary with nothing
buffered, keeps the two from ever writing at once, and the first
clears it (waiting out a push in progress) before it answers the next
message. A client that only reads its socket after `LISTEN` — pgx's
`WaitForNotification`, lib/pq's `Listener` — is therefore notified
without sending anything.

## Client compatibility

Tested against `psql`, [pgx](https://github.com/jackc/pgx) v5 in both its
default (extended) and simple-protocol query modes, and `database/sql` via
pgx's stdlib adapter. Applications should treat SQLSTATE `40001` as "retry
the transaction", exactly as with CockroachDB or serializable PostgreSQL.
