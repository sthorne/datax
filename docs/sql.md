# SQL layer and PostgreSQL wire protocol

datax's SQL surface is deliberately small in v1: enough to create tables,
read and write rows by primary key or scan, and run multi-statement
transactions — all over the standard PostgreSQL wire protocol.

## Grammar (v1)

```
CREATE TABLE t (col TYPE [NOT NULL], ..., PRIMARY KEY (col, ...))
    [WITH (timeseries = true [, retention = '7d'] [, shards = N])]  -- see docs/timeseries.md
ALTER TABLE t SET (shards = M)          -- online re-shard of a sharded timeseries table
ALTER TABLE [IF EXISTS] t ADD | DROP COLUMN ... | RENAME TO t2 | RENAME [COLUMN] a TO b | RENAME CONSTRAINT a TO b
    | ALTER [COLUMN] c SET DEFAULT v | DROP DEFAULT | SET NOT NULL | DROP NOT NULL | ADD | DROP | VALIDATE CONSTRAINT ...
CREATE [UNIQUE] INDEX [IF NOT EXISTS] i ON t (cols)  /  DROP INDEX [IF EXISTS] i  /  ALTER INDEX i RENAME TO j
TRUNCATE [TABLE] t [, ...] [RESTART IDENTITY] [CASCADE]   -- a layout swap: new index IDs, the old layout retired
DROP TABLE t
CREATE [OR REPLACE] VIEW v [(cols)] AS SELECT ...  /  DROP VIEW [IF EXISTS] v [, ...] [CASCADE]  /  SHOW VIEWS
CREATE TABLE t [(names [, PRIMARY KEY (cols)])] AS SELECT ... [WITH [NO] DATA]   -- streamed through the COPY chunk path
CREATE TABLE t (LIKE src [INCLUDING | EXCLUDING DEFAULTS | CONSTRAINTS | INDEXES | COMMENTS | ALL], ...)
ALTER TABLE t ALTER [COLUMN] c [SET DATA] TYPE type       -- online rewrite: shadow column, chunked convert, swap
COMMENT ON TABLE | VIEW | INDEX | COLUMN name IS 'text' | NULL
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
    [LIMIT n | ALL] [OFFSET n] [FETCH FIRST n ROWS ONLY] [FOR UPDATE]
SELECT <literal exprs>                  -- e.g. SELECT 1 (client health checks)
UPDATE t SET col = value, ... [WHERE conjunction]
DELETE FROM t [WHERE conjunction]
BEGIN / COMMIT / ROLLBACK
SAVEPOINT name / RELEASE SAVEPOINT name / ROLLBACK TO SAVEPOINT name
SHOW TABLES
ANALYZE [t]                             -- collect table statistics (admin; not in a txn block)
SHOW STATS FOR t
```

- Types: `INT8` (aliases INT, INTEGER, BIGINT), `FLOAT8` (DOUBLE PRECISION),
  `TEXT` (aliases STRING, VARCHAR), `BOOL` (BOOLEAN), `TIMESTAMPTZ`
  (alias TIMESTAMP [WITH TIME ZONE]; UTC nanoseconds internally,
  microsecond precision on the binary wire), `DATE`, `BYTES` (alias
  BYTEA; `\x` hex text format), `UUID`, `DECIMAL` (aliases NUMERIC, DEC),
  `JSONB` (alias JSON). String literals coerce into the
  new types (`WHERE at >= '2026-08-30 02:00:00Z'` becomes a key bound),
  and all but JSONB are usable in primary keys and indexes via
  order-preserving encodings (JSONB has no ordering and refuses with
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
  RowDescription reports the typmod (`(p<<16)|(s+4)`) for enforced
  columns. Precision/scale live on the column descriptor as append-only
  `omitempty` JSON fields (zero = bare DECIMAL, the pre-existing
  meaning), so old descriptors and rolling upgrades are unaffected.
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
  unhandled op there would silently drop rows). Never an index bound —
  always a post-fetch filter; inverted indexes are out of scope.
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
  by `SHOW STATS FOR t`. Statistics feed the planner (below); their
  absence means the structural fallback, byte-identical to the
  pre-statistics planner.
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

Still out of scope: correlated subqueries nested past 8 levels or over
set-operation shapes,
joins beyond 8 tables (INNER joins are cost-reordered when statistics
exist; outer joins and self-joins keep syntactic order),
`EXPLAIN` options in parentheses, RANGE frames with an offset,
deferrable constraints,
typmod enforcement beyond DECIMAL on columns (`VARCHAR(n)` parsed and
ignored; casts apply both),
JSONB indexing (`@>`, `<@`, `?` and friends evaluate as filters; no
inverted indexes),
expressions over aggregates (`SUM(a) / COUNT(*)`), window functions, an
INTERVAL type (intervals are text), user-defined functions,
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
  how a gateway adopts new versions.
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
  sends, so the check is the client's. Without it, a statement that
  planned under a lease just before it expired could commit after the
  drain took its backfill boundary, and an index build would miss the
  row (issue #110).

Remaining gap: a transaction that issued its `BEGIN` before the drain, on
another gateway, keeps the descriptor version it started with until it
commits, and its gateway's renewals keep the lease live at the new
version meanwhile. The deadline closes this once the lease lapses;
long-lived explicit transactions under a healthy gateway are not covered
(tracked in issue #22).

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
table size. The chunks' serializable reads together cover the **whole**
primary span — the last chunk runs to the span's end, and an empty table
still gets one — because those reads are what the timestamp cache
remembers: a writer that planned under a lease the drain has since
written off is either waited for (its intent already down: the chunk
indexes the row once it commits) or pushed above the backfill, where its
commit fails the lease deadline and re-plans with the index (issue #110).
A tail no chunk read would let such a write land in the past, below the
boundary, and never reach the index.

Left for later: a delete-only state for online index drops, and the
long-lived-transaction descriptor pinning noted under Catalog.

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
counts are observable via `datax_sql_rows_scanned_total`.

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
  the first Execute and the result is served from the materialized rows
  (streaming resumption from KV is a possible later optimization).
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
  me!), `42P01` undefined_table, `23505` unique_violation, `42601`
  syntax_error, `0A000` feature_not_supported.

## Client compatibility

Tested against `psql`, [pgx](https://github.com/jackc/pgx) v5 in both its
default (extended) and simple-protocol query modes, and `database/sql` via
pgx's stdlib adapter. Applications should treat SQLSTATE `40001` as "retry
the transaction", exactly as with CockroachDB or serializable PostgreSQL.
