# SQL reference

The complete supported surface, user's-eye view. For grammar internals and
design rationale see [docs/sql.md](../sql.md); for what's deliberately
missing versus PostgreSQL see [Differences](postgres-differences.md).

## Types

| Type | Aliases | Notes |
|---|---|---|
| `INT8` | `BIGINT` | 64-bit |
| `INT4` | `INT`, `INTEGER` | 32-bit: values outside ±2³¹ are refused (`22003`); `int4` (OID 23) on the wire. Arithmetic over any integer width is 64-bit (`a + 1` describes as `INT8`, like the sum does in PostgreSQL) |
| `INT2` | `SMALLINT` | 16-bit, the same way (`int2`, OID 21) |
| `FLOAT8` | `DOUBLE PRECISION` | IEEE 754 double |
| `DECIMAL` | `NUMERIC`, `DEC` | exact arbitrary precision; `DECIMAL(p,s)` is **enforced**: values rescale to `s` (round-half-even), overflow past `p−s` integer digits is SQLSTATE `22003`, and stored values render with the declared fixed scale (`9.90`) |
| `TEXT` | `STRING`, `VARCHAR` | unbounded |
| `VARCHAR(n)` | `CHARACTER VARYING(n)` | at most `n` characters: a longer value is refused (`22001`) unless the excess is spaces, which are dropped; `varchar` (OID 1043) with the typmod on the wire |
| `CHAR(n)` | `CHARACTER(n)`; bare `CHAR` is `CHAR(1)` | fixed width: stored trimmed of trailing spaces, rendered blank-padded to `n` (`'ab'` in a `CHAR(3)` reads back as `'ab '`); `length()`, `||` and comparisons see the trimmed value; `bpchar` (OID 1042) on the wire |
| `BOOL` | `BOOLEAN` | |
| `TIMESTAMPTZ` | `TIMESTAMP WITH TIME ZONE` | UTC; microsecond precision on the binary wire; years 1678 to 2261 (a value outside is refused) |
| `TIMESTAMP` | `TIMESTAMP WITHOUT TIME ZONE` | wall-clock time: an offset in the input is ignored (`'2024-01-02 03:04:05+05'` stores `03:04:05`) and the output carries none; `timestamp` (OID 1114) on the wire. Expressions over it (`ts + '1 day'`, `date_trunc`) and casts (`::timestamp`) are `TIMESTAMPTZ` |
| `TIMESTAMP(p)`, `TIMESTAMPTZ(p)` | | `p` in 0–6: values round to `p` fractional digits on write (half away from zero) |
| `INTERVAL` | `INTERVAL` with field qualifiers (`DAY TO SECOND`, accepted and ignored) | PostgreSQL's months / days / clock triple, so `'1 month'` steps the calendar and `'1 day'` is a day whatever the hour; input in the verbose (`'1 year 2 mons 3 days 04:05:06'`, `'2h30m'`, `'1 day ago'`), SQL standard (`'1-2 3 04:05:06'`) and ISO 8601 (`'P1Y2M3DT4H5M6S'`) forms and as `INTERVAL '...'`; rendered as PostgreSQL does (`1 day -02:00:00`, `-1 days +12:00:00`); compares and sorts by PostgreSQL's rule (a month is 30 days, a day 24 hours: `'30 days' = '1 month'`); `interval` (OID 1186) on the wire; cluster version v10 for a column |
| `TIME` | `TIME WITHOUT TIME ZONE`, `TIME(p)` | time of day, microsecond precision on the wire (`time`, OID 1083); input `'04:05:06.789'`, `'4:05 PM'`, `'16:05'`, a timestamp text (its clock is taken), an offset is ignored; `24:00:00` allowed; `TIME WITH TIME ZONE` is refused; cluster version v10 for a column |
| enum types | `CREATE TYPE name AS ENUM ('a', 'b', ...)` | a column of the type stores the label's ordinal and orders by declaration (`ORDER BY`, `min` / `max`, indexes and keys); input and output are the label; an unknown label is `22P02`; `'a'::name` casts a literal; `ALTER TYPE name ADD VALUE [IF NOT EXISTS] 'c'` appends (no `BEFORE` / `AFTER`; usable at once on every gateway); `DROP TYPE [IF EXISTS] name` refuses while a column uses it (`2BP01`); `pg_type` (`typtype = 'e'`), `pg_enum`, `information_schema.columns` (`USER-DEFINED`), psql's `\dT` and `\d`; described on the wire with the type's OID (values as labels, text and binary); no arrays of enums, no `RENAME VALUE`; cluster version v10 |
| `T[]` | `T ARRAY`, `T[][]` (one-dimensional whatever the brackets) | an array of any scalar type but `JSONB` (`INT8[]`, `TEXT[]`, `VARCHAR(3)[]`, `DECIMAL(6,2)[]` — the modifiers apply per element); literals `'{1,2}'`, `'{a,"b c",NULL}'` and `ARRAY[1, 2]`; `a[i]` (1-based, NULL out of range; no slices); `= ANY(a)` / `<> ALL(a)` and the other comparison operators with `ANY` / `ALL`; `@>`, `<@`, `&&`; `||` with an array or an element; equality and ordering element by element; `unnest` in `FROM` (`FROM unnest(a) AS u(x)`) and in a FROM-less select list (`SELECT unnest($1::int8[])` expands a parameter into rows), `array_agg`, `array_length`, `cardinality`, `array_append` / `array_prepend` / `array_cat` / `array_position` / `array_remove` / `array_to_string` / `string_to_array` / `array_upper` / `array_lower` / `array_ndims`; PostgreSQL's array OIDs, text and binary formats on the wire (pgx slices scan and bind; `WHERE id = ANY($1)` with a slice); not indexable and not a key; `GROUP BY` and `ORDER BY` work; cluster version v10 for a column |
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
  operand demotes the expression to float. `AVG` of an `INT8` or a
  `DECIMAL` is a DECIMAL rounded to 6 fractional digits (half-even), as
  in PostgreSQL; `AVG(FLOAT8)` is a FLOAT8. Float values never
  implicitly convert *to* DECIMAL.
- `DECIMAL(p,s)` applies on every write — `INSERT`, `UPDATE`, parameters
  in both wire formats, `COPY`, and `DEFAULT` values — including primary
  key and indexed columns (two values that round to the same stored
  number collide as duplicates). Expression and aggregate *results* are
  plain DECIMALs and render canonically (`SUM` of `9.90`s can print
  `19.8`), like PostgreSQL's rule that expressions lose the typmod.
- The other type modifiers — the integer widths, `VARCHAR(n)` /
  `CHAR(n)`, `TIMESTAMP` without time zone, `TIMESTAMP(p)` — apply on
  the same paths, and a column that carries one describes with
  PostgreSQL's OID and typmod (`\d`, `information_schema.columns`,
  `pg_attribute`, `SHOW CREATE TABLE` spell it). Storage is unchanged
  by them (an `INT4` is the same varint as an `INT8`; a `TIMESTAMP` the
  same nanosecond count), so `ALTER COLUMN TYPE` between widths, lengths
  and timestamp forms never rewrites bytes it does not have to: widening
  (`INT2` → `INT4` → `INT8`, `VARCHAR(10)` → `VARCHAR(20)` → `TEXT`,
  `TIMESTAMP(3)` → `TIMESTAMP`) is a descriptor write, anything else is
  the online rewrite below, which checks every stored value. Typmods
  on other types (`FLOAT8(3)`) are accepted and ignored. Until the
  cluster version reaches v9 a new column keeps the earlier meaning of
  its declaration (`INT` 64-bit, `VARCHAR(n)` unbounded, `TIMESTAMP`
  = `TIMESTAMPTZ`), so a mixed-version cluster never carries a
  modifier an older binary would ignore.
- `SERIAL` is an `INT4` column (`SMALLSERIAL` `INT2`, `BIGSERIAL`
  `INT8`) drawing from an owned sequence, as in PostgreSQL.
- JSONB stores normalized text; numbers keep their exact ingest form.
  Extraction: `j -> 'key'` (jsonb), `j ->> 'key'` (text, must be last),
  chainable: `j -> 'a' ->> 'b'`. Missing keys and non-objects yield NULL.
  JSONB cannot be a primary key or indexed. `->`/`->>` (by key or array
  index) and `#>`/`#>>` (by path: `j #>> '{a,0,b}'`) work everywhere an
  expression does — single-table queries, joins, grouped select lists
  and HAVING; a LEFT-joined NULL side extracts to NULL.
- Containment: `j @> '{"tags":["go"]}'` and `<@` — PostgreSQL semantics
  (objects contain recursively, arrays contain each right element
  somewhere, a top-level array contains a matching scalar), numbers
  compared numerically (`1` contains `1.0`) — and key existence `j ?
  'k'`, `j ?| '{a,b}'`, `j ?& '{a,b}'`. All are filters (no index
  acceleration): pair them with an indexable condition on large tables.
- Every type except JSONB can appear in primary keys and indexes.

## DDL

```sql
CREATE TABLE t (
  a INT8 NOT NULL,
  b TEXT DEFAULT 'x',            -- a constant, or an expression (below)
  c DECIMAL,
  PRIMARY KEY (a)                -- or: a INT8 PRIMARY KEY
);
CREATE TABLE IF NOT EXISTS t (...);
DROP TABLE [IF EXISTS] t;

CREATE [UNIQUE] INDEX [IF NOT EXISTS] by_b ON t (b);   -- online: no write outage
DROP INDEX [IF EXISTS] by_b;                            -- entries reclaimed after the commit
ALTER INDEX [IF EXISTS] by_b RENAME TO by_b2;
ALTER TABLE [IF EXISTS] t ADD COLUMN [IF NOT EXISTS] d INT8 DEFAULT 0; -- lazy; NOT NULL requires DEFAULT; constants only
ALTER TABLE t DROP COLUMN [IF EXISTS] d;                -- refused for PK/indexed columns
ALTER TABLE t RENAME TO t2;                             -- foreign keys and sequences follow (they use IDs)
ALTER TABLE t RENAME [COLUMN] b TO body;                -- CHECK constraints on the column are rewritten
ALTER TABLE t RENAME CONSTRAINT t_b_key TO b_unique;    -- a UNIQUE constraint's index renames with it
ALTER TABLE t ALTER [COLUMN] b SET DEFAULT 'y';         -- a constant or an expression (below)
ALTER TABLE t ALTER [COLUMN] b DROP DEFAULT;
TRUNCATE [TABLE] t [, t2] [RESTART IDENTITY] [CASCADE]; -- one descriptor write, however many ranges
ALTER TABLE t ALTER [COLUMN] c [SET DATA] TYPE DECIMAL(12, 2);   -- online rewrite; widening and text conversions
ALTER TABLE t SPLIT AT VALUES (1000), (2000, 'x');      -- carve ranges at primary-key tuples (a prefix allowed); returns the boundaries
CREATE TABLE t2 (LIKE t INCLUDING ALL);                  -- columns, defaults, constraints, indexes, comments
CREATE TABLE big AS SELECT id, qty FROM t WHERE qty > 10;          -- a hidden rowid key
CREATE TABLE keyed (order_id, qty, PRIMARY KEY (order_id)) AS SELECT id, qty FROM t [WITH NO DATA];
COMMENT ON TABLE t IS 'orders';  COMMENT ON COLUMN t.b IS 'body';  COMMENT ON INDEX by_b IS NULL;
SHOW TABLES;
```

`CREATE INDEX` backfills online in bounded chunks, like PostgreSQL's
`CREATE INDEX CONCURRENTLY`; it cannot run inside an explicit transaction.
`DROP INDEX` names the index alone (it is found on whichever table of
the database carries it); an index a `UNIQUE` constraint owns is
refused (`2BP01`) — drop the constraint. The index disappears from the
schema at once and its entries are reclaimed after the statement's
transaction commits and every gateway has adopted the change.

`TRUNCATE` is a descriptor change, not a delete: the table (and each of
its indexes) moves to a fresh keyspace, so it costs the same for a
million rows spanning many ranges as for ten, and it is transactional
(`ROLLBACK` brings the rows back). The old rows stay on disk for `AS OF
SYSTEM TIME` reads below the truncation until the re-shard janitor
reclaims them after the historical window (the GC TTL). A table another
table references through a foreign key is refused unless the
referencing table is truncated in the same statement — `CASCADE` adds
every referencing table, transitively. `RESTART IDENTITY` resets the
sequences the table's `SERIAL` / identity columns own to their `START`.
Non-admins need the `DELETE` privilege on every table.

`RENAME` moves the name entry; the table's ID (the OID the catalogs
show) is unchanged, so foreign keys, owned sequences and grants follow.
Another gateway sees the new name as soon as the statement returns and
the old name no longer resolves (`42P01`). A renamed column stays
indexed and constrained: its `CHECK` expressions are rewritten to the
new name. Sequence names (`t_id_seq`) and the primary key's name do not
change, as in PostgreSQL.

**`CREATE TABLE ... AS query`** creates the table from the query's
output columns (names and types as Describe reports them; `(a, b) AS`
renames them positionally) and, unless `WITH NO DATA`, fills it: the
query runs once, at one timestamp, and its rows stream in through the
`COPY` chunk path, a bounded transaction per chunk — so the statement
never holds one transaction open over a million rows and cannot run
inside a transaction block (`25001`). A table needs a primary key, so
a `PRIMARY KEY (cols)` clause may be written among the names
(CockroachDB's form); without one the table gets a hidden `rowid`
column (`unique_rowid()`, invisible to `SELECT *`) — and duplicate
query rows are all kept. Parameters (`$1`) and views in the query
work. `SELECT ... INTO t` is refused with a pointer here.

**`LIKE source`** inside a column list copies the source's columns
(types, typmods, `NOT NULL`) where it stands; `INCLUDING DEFAULTS`
copies the defaults (an owned sequence's `nextval` is not copied — the
new table gets its own `SERIAL` if you declare one), `INCLUDING
CONSTRAINTS` the `CHECK` constraints, `INCLUDING INDEXES` the `UNIQUE`
constraints and secondary indexes (same names), `INCLUDING ALL` every
one; `EXCLUDING` reverses an option. The primary key is copied whenever
the statement declares none (PostgreSQL leaves it to `INCLUDING
INDEXES`; here a table needs one). Foreign keys are never copied.

**`ALTER COLUMN c TYPE t`** rewrites the column online, in the shape of
`CREATE INDEX`: a hidden shadow column of the new type joins the
descriptor and every write from then on fills it from the column's
value (whichever gateway writes), the existing rows are converted in
bounded chunks, and the shadow then takes the column's name, position,
`NOT NULL`, default and comment while the old column is dropped. The
conversion is the type's cast: widening (`INT8` to `DECIMAL` or
`FLOAT8`, `DATE` to `TIMESTAMPTZ`), anything to `TEXT`, and `TEXT` to a
typed column when every value parses (`INT8`, `DECIMAL`, `FLOAT8`,
`BOOL`, `TIMESTAMPTZ`, `DATE`, `BYTES`, `UUID`, `JSONB`); a `DECIMAL`
typmod change rescales, and a change of integer width, character
length or timestamp form checks every stored value against the new
modifier (`22003` / `22001`). A value that cannot convert fails the
statement (`22P02`) and the column is left as it was. A modifier change
that cannot lose a value (a wider integer, a longer `VARCHAR`, more
timestamp digits) skips the rewrite: one descriptor write, and the
column is not required to be free of indexes and constraints. Family
narrowing, the primary key, indexed columns, columns a constraint or
view uses, and columns drawing from a sequence are otherwise refused
(`0A000`) — drop or replace those first. Not inside a transaction block
(`25001`); cluster version v9.

**`COMMENT ON TABLE | VIEW | INDEX | COLUMN ... IS 'text' | NULL`**
stores the text in the descriptor; `obj_description`,
`col_description` and `pg_description` render it, so `psql`'s `\d+`,
`\dt+` and `\dv+` show the descriptions. Comments survive renames,
default changes and type rewrites.

**DDL in transaction blocks.** A statement that is one descriptor
write runs inside `BEGIN ... COMMIT` like any other and commits or rolls
back with the block: `CREATE TABLE`, `DROP TABLE`, `ADD` / `DROP
COLUMN`, every `RENAME`, `SET` / `DROP DEFAULT`, `DROP CONSTRAINT`,
`DROP NOT NULL`, `DROP INDEX`, `TRUNCATE`, `COMMENT ON`, `CREATE` /
`DROP VIEW`, the sequence, database, role and ownership statements,
`GRANT` / `REVOKE`. The multi-transaction statements — `CREATE INDEX`, `ADD
CONSTRAINT`, `VALIDATE CONSTRAINT`, `SET NOT NULL`, `ALTER COLUMN
TYPE`, `SET (shards = N)`, `CREATE TABLE ... AS`, `ANALYZE` — publish,
drain and sweep (or stream) in several transactions of their own and
are refused inside a block with `25001`; so is `SPLIT AT`, a range
operation rather than a descriptor write (idempotent: an existing
boundary is fine; a sharded timeseries table is carved by shard instead).

### Defaults, SERIAL, identity columns and sequences

```sql
CREATE TABLE orders (
  id      SERIAL PRIMARY KEY,                       -- INT8 DEFAULT nextval('orders_id_seq')
  ref     UUID DEFAULT gen_random_uuid(),
  rid     INT8 DEFAULT unique_rowid(),              -- node-local, ascending, no coordination
  at      TIMESTAMPTZ DEFAULT now(),
  who     TEXT DEFAULT current_user,
  retries INT8 DEFAULT 2 + 1
);
CREATE TABLE events (
  k INT8 GENERATED ALWAYS AS IDENTITY (START WITH 100) PRIMARY KEY,
  d INT8 GENERATED BY DEFAULT AS IDENTITY,
  v TEXT
);
ALTER TABLE orders ALTER COLUMN who SET DEFAULT lower(current_user);  -- change a default later
INSERT INTO orders (ref) VALUES (DEFAULT);                 -- DEFAULT as a value
INSERT INTO orders DEFAULT VALUES RETURNING id;
INSERT INTO archive (id, ref) SELECT id, ref FROM orders WHERE at < '2026-01-01';  -- INSERT ... SELECT
INSERT INTO events (k, v) OVERRIDING SYSTEM VALUE VALUES (5, 'x');  -- else 428C9 for an ALWAYS column
UPDATE orders SET retries = DEFAULT WHERE id = 1;

CREATE SEQUENCE s [INCREMENT [BY] n] [MINVALUE n | NO MINVALUE] [MAXVALUE n | NO MAXVALUE]
  [START [WITH] n] [CACHE n] [[NO] CYCLE] [OWNED BY t.col | NONE];
SELECT nextval('s'), currval('s'), lastval(), setval('s', 100), setval('s', 100, false);
SELECT last_value, is_called FROM s;                        -- the one-row relation, as in PostgreSQL
ALTER SEQUENCE s MAXVALUE 1000 RESTART [WITH n];
DROP SEQUENCE [IF EXISTS] s;
SHOW SEQUENCES;
```

A `DEFAULT` expression may use constants, operators, casts and the
immutable and stable [builtin functions](functions.md) (`now()`,
`current_user`, `lower(...)`, ...) plus the volatile ones — `nextval`,
`currval`, `lastval`, `setval`, `unique_rowid()`, `gen_random_uuid()`,
`random()` — but not other columns or subqueries. Volatile functions are evaluated **per row**: a 100-row
`INSERT` draws 100 sequence values. `now()` and `current_user` are fixed
for the statement.

`ALTER COLUMN ... SET DEFAULT` takes the same constants and expressions
and applies to rows inserted from then on; `DROP DEFAULT` makes an
omitted column NULL. A column added by `ALTER TABLE ... ADD COLUMN ...
DEFAULT` fills the rows that predate it from that original constant on
read, and keeps doing so whatever its default becomes later: changing
the default changes new inserts, never the old rows. An identity
column's default is its sequence and cannot be set (`42601`).

`SERIAL` (`SMALLSERIAL`, `BIGSERIAL`) and identity columns create a
sequence named `<table>_<column>_seq` owned by the column: it is dropped
with the column or the table and cannot be dropped on its own
(`2BP01`), and psql's `\d` shows the default. A `GENERATED ALWAYS`
column refuses an explicit value unless the statement says `OVERRIDING
SYSTEM VALUE`; `BY DEFAULT` takes one.

**Sequences are one counter key.** `nextval` advances it with an atomic
increment **outside** the calling transaction, so a value handed out is
never handed out again even if the transaction rolls back — gaps are
normal, exactly as in PostgreSQL. Each gateway node takes `CACHE` values
(default 32) per increment and serves `nextval` from that block, so a
hot sequence is not a round trip to one range per row; the price is that
values are ascending within a node but interleave across nodes, and a
node restart or `setval` on another node leaves a block unused. `CACHE
1` gives strictly ordered values at one increment per row. `currval` and
`lastval` are per session (`55000` before the session's first
`nextval`); reaching `MAXVALUE`/`MINVALUE` is `2200H` unless `CYCLE`.

For write throughput prefer keys that spread across ranges: a
sequence-keyed table appends every insert to the same range's tail
(and every sequence lives in the `/system` range), while
`unique_rowid()` — 48 bits of microsecond time above the node ID,
ascending on each node without coordination — and `gen_random_uuid()`
need no shared counter at all. Both are volatile defaults like any
other, so `... RETURNING id` returns them.

Expression defaults and sequences need **cluster version v7**: a
cluster upgraded from older binaries refuses the DDL with `0A000` until
`datax debug upgrade` finalizes v7 (a v6 node could not evaluate them).

### Constraints: CHECK, UNIQUE and FOREIGN KEY

```sql
CREATE TABLE orders (
  id      INT8 PRIMARY KEY,
  cust    INT8 NOT NULL REFERENCES customers ON DELETE CASCADE,      -- the primary key of customers
  sku     TEXT CONSTRAINT orders_sku_fkey REFERENCES products (sku)  -- or a UNIQUE column / constraint
            ON DELETE SET NULL ON UPDATE CASCADE,
  qty     INT8 CHECK (qty > 0),
  code    TEXT UNIQUE,
  region  TEXT,
  CONSTRAINT orders_region_code UNIQUE (region, code),
  CHECK (qty < 1000 OR region IS NULL),
  FOREIGN KEY (region, code) REFERENCES zones (region, code) ON DELETE RESTRICT
);

ALTER TABLE orders ADD CONSTRAINT qty_small CHECK (qty < 100);            -- validates existing rows
ALTER TABLE orders ADD CONSTRAINT qty_small CHECK (qty < 100) NOT VALID;  -- new writes only, for now
ALTER TABLE orders VALIDATE CONSTRAINT qty_small;
ALTER TABLE orders ADD UNIQUE (code);                                     -- an online unique index build
ALTER TABLE orders ADD FOREIGN KEY (cust) REFERENCES customers;
ALTER TABLE orders DROP CONSTRAINT [IF EXISTS] qty_small;
ALTER TABLE orders RENAME CONSTRAINT qty_small TO qty_bounded;
ALTER TABLE orders ALTER COLUMN region SET NOT NULL;                      -- sweeps existing rows first
ALTER TABLE orders ALTER COLUMN region DROP NOT NULL;
DROP TABLE customers CASCADE;                    -- also drops the foreign keys that reference it
SET foreign_key_cascade_limit = 50000;           -- per statement; default 10000
```

Names default to PostgreSQL's (`orders_qty_check`, `orders_code_key`,
`orders_cust_fkey`, numbered when taken); `\d`, `pg_constraint`,
`information_schema.table_constraints` / `check_constraints` /
`referential_constraints` / `key_column_usage` and `SHOW CREATE TABLE`
show them.

**CHECK** expressions use the row's columns, constants, operators,
casts and the non-volatile [builtin functions](functions.md) (no
subqueries, parameters, `random()`, or `nextval` and friends). A NULL result passes, as in PostgreSQL; a violation is
`23514`. They are checked on `INSERT`, `UPDATE`, `COPY` and on the rows
a cascade rewrites.

**UNIQUE** constraints are unique indexes carrying the constraint's
name (`23505` on a duplicate); `ON CONFLICT ON CONSTRAINT name` and
`ON CONFLICT (cols)` resolve through them. Adding one to a table with
rows is the online index build: it cannot run inside a transaction
block, and duplicates fail it with nothing left behind.

**FOREIGN KEY**s reference the parent's primary key or a unique
constraint / index (`42P10` otherwise), column types must match, and
the parent must be in the same database (`0A000`). `MATCH SIMPLE`
only: a NULL anywhere in the key exempts the row. The child side is a
point read of the parent inside the writing transaction (`23503` when
absent); the parent side, on `DELETE` or an update of the referenced
columns, finds the children through the referencing columns'
index — **the referencing side gets an index automatically**
(`<constraint>_idx`, dropped with the constraint) when none covers
them, so a parent delete never scans the child — and applies the
action: `RESTRICT` / `NO ACTION` (`23503`; both check at statement
end, there is no deferring), `CASCADE` (deletes or re-keys the
children, recursively), `SET NULL` (`23502` if the column is `NOT
NULL`). `SET DEFAULT` is not supported. A cascade is bounded per
statement by `foreign_key_cascade_limit` (`54000` beyond it): an
unbounded cascade is an unbounded transaction. Serializable isolation
already makes a parent delete and a concurrent child insert conflict
(one of them restarts with `40001`), so no extra locking is involved.

`ALTER TABLE ... ADD CONSTRAINT` publishes the constraint first — new
writes honor it as soon as every gateway has the descriptor — then
validates the existing rows in bounded chunks; a violation removes the
constraint again. `NOT VALID` skips the sweep (the catalogs show
`convalidated = false`); `VALIDATE CONSTRAINT` runs it later. Like
`CREATE INDEX`, these cannot run inside a transaction block. `DROP
CONSTRAINT`, `RENAME CONSTRAINT`, `DROP NOT NULL` and `CREATE TABLE`
constraints are ordinary transactional DDL. Dropping a column a constraint uses, or a
table a foreign key references, is refused (`2BP01`) until the
constraint is dropped or `DROP TABLE ... CASCADE` drops the keys with
it. `ADD COLUMN` takes no constraint: add the column, then the
constraint.

Constraints need **cluster version v8** (`0A000` until `datax debug
upgrade` finalizes it): a v7 node would write rows unchecked.

## Reading

```sql
[WITH [RECURSIVE] name [(cols)] AS (query | INSERT/UPDATE/DELETE ... RETURNING ...), ...]
SELECT * | expr [AS alias] | func(...) OVER ([PARTITION BY exprs] [ORDER BY terms] [frame]), ...
  FROM t [AS a] | (query) AS d
  [JOIN t2 | (query) AS d2 ON b.x = a.y [AND ...] | USING (cols)]   -- INNER, LEFT | RIGHT | FULL [OUTER],
                                                                    -- CROSS, NATURAL, or "t, t2"; up to 8 tables
  [WHERE conjunct AND conjunct AND ...]
  [GROUP BY cols] [HAVING ...]
  [WINDOW name AS (...), ...]
  [UNION | INTERSECT | EXCEPT [ALL] SELECT ... | VALUES ... | (query)]
  [ORDER BY col | position | expr [ASC|DESC] [NULLS FIRST|LAST], ...]
  [LIMIT n | ALL] [OFFSET n] [FETCH FIRST n ROWS ONLY];
```

- **WHERE** supports full boolean logic: `AND`, `OR`, `NOT`, and
  parentheses over conditions of the form `expr op value`
  (`= != < <= > >=`, `[NOT] LIKE` / `ILIKE`, regular expressions
  `~ !~ ~* !~*`, jsonb `j @> '...'`), `expr op ANY | SOME | ALL (array)`
  (`qty = ANY ('{3,4}')`, `= ANY (SELECT ...)` is `IN`),
  `col IS [NOT] NULL`, `col [NOT] IN (list | SELECT ...)`, `[NOT] EXISTS
  (SELECT ...)`, a bare boolean column or call (`WHERE active AND
  pg_table_is_visible(oid)`), a scalar `(SELECT ...)` used as a
  predicate, and `j ->> 'k' op value`. The left side may be a computed
  expression (`qty * 2 > 10`, `lower(name) = 'x'`); a value may be a
  literal, parameter, column, or scalar `(SELECT ...)`. Computed
  left-hand sides and `->`/`->>` conjuncts work in joins too (evaluated
  on the joined row). `IN`, `EXISTS` and scalar subqueries may appear
  inside `OR` (each is evaluated once, or per row when correlated).
  `OR` conditions, `LIKE`, `ANY` and path/computed conjuncts filter
  fetched rows — they never become index
  bounds, so pair them with an indexable `AND` condition on large tables.
- **Predicates**: `[NOT] BETWEEN [SYMMETRIC] a AND b` (two conjuncts,
  so a keyed column's range becomes index bounds), `IS [NOT] TRUE |
  FALSE | UNKNOWN`, `IS [NOT] DISTINCT FROM`, `[NOT] LIKE / ILIKE ...
  [ESCAPE 'c']` (a literal prefix — `LIKE 'ab%'` — becomes index bounds
  on a keyed column) and `[NOT] SIMILAR TO` (SQL regular expressions;
  an invalid pattern is SQLSTATE `2201B`). A predicate used as a value
  (`qty > 3 AS big`) is three-valued: NULL when an input is NULL.
- **Expressions**: arithmetic `+ - * / % ^` with standard precedence and
  parentheses (exact on DECIMAL/INT8; integer division truncates; `^` is
  always FLOAT8; division by zero is SQLSTATE `22012`, INT8 overflow
  `22003`), date and time arithmetic (`date + 1`, `date - date` in
  days, `timestamp ± interval` and `date ± interval` on the calendar
  with month steps clamped to the end of the month, `timestamp -
  timestamp` an `INTERVAL`, `interval ± interval`, `interval * 2`,
  `interval / 2`, `time ± interval` wrapping at midnight, `time - time`,
  `date + time` a timestamp; the interval operand may be a value,
  `INTERVAL '2 hours'` or plain text `'2 hours'`), text
  concatenation `||` (any operand renders as text), `CASE` (simple and
  searched), `CAST(x AS type)` and `x::type` — **performed**, in every
  position, with PostgreSQL's text forms and error codes (`'abc'::int`
  is `22P02`), `DECIMAL(p,s)` and `VARCHAR(n)` typmods applied on the
  cast, `'name'::regclass` resolving a table name to its catalog OID
  and `oid::regclass` on a column giving the table's name —, `E'...'`
  escape strings, and the builtin functions: conditionals (`coalesce`,
  `nullif`, `greatest`, `least`), strings (`length`, `lower`, `upper`,
  `substring` / `substr`, `position`, `trim` / `ltrim` / `rtrim` /
  `btrim`, `left`, `right`, `lpad`, `rpad`, `repeat`, `replace`,
  `reverse`, `split_part`, `starts_with`, `initcap`, `concat`,
  `concat_ws`, `format`, `md5`, `sha256`, `encode` / `decode`,
  `to_hex`, `chr` / `ascii`, `translate`, `quote_ident`,
  `quote_literal`), math (`abs`,
  `ceil`, `floor`, `round`, `trunc`, `mod`, `div`, `power`, `sqrt`,
  `cbrt`, `exp`, `ln`, `log`, `sign`, `pi`, `random`, the
  trigonometric functions, ...), date and time (`now()`,
  `current_timestamp`, `current_date`, `date_trunc`, `date_part` /
  `extract(field FROM x)`, `to_char`, `to_timestamp`, `to_date`,
  `make_date`, `make_timestamp`, `make_time`, `make_interval`, `age`,
  `extract` over intervals and times, `justify_hours`, `justify_days`,
  `justify_interval`), JSON
  (`jsonb_build_object`, `jsonb_build_array`, `to_jsonb`,
  `jsonb_typeof`, `jsonb_array_length`, `jsonb_extract_path[_text]`,
  `jsonb_set`, `jsonb_strip_nulls`, `jsonb_pretty`, ...), and the session and catalog functions tools call (`version()`,
  `current_user`, `current_schema()`, `current_setting(name)`,
  `format_type`, `pg_get_indexdef`, `pg_get_constraintdef`,
  `pg_get_expr`, `pg_typeof`, `pg_size_pretty`, `has_*_privilege`,
  ...) as well as the sequence and id functions `nextval`, `currval`,
  `lastval`, `setval`, `unique_rowid()`, `gen_random_uuid()` /
  `uuid_generate_v4()` ([Defaults and sequences](#defaults-serial-identity-columns-and-sequences)).
  The complete list with signatures, aliases and volatility is the
  [Functions reference](functions.md); `SHOW FUNCTIONS` prints the same
  from a session, and `pg_catalog.pg_proc` lists them for tools. All
  of it works in SELECT lists, WHERE, HAVING, ORDER BY, INSERT VALUES,
  UPDATE SET and RETURNING, in single-table queries and joins. An
  unknown function is SQLSTATE `42883`; the wrong number of arguments
  too. Computed SELECT outputs describe with their real type on the
  wire (`qty * 2` is INT8, `price * 1.1` DECIMAL, `qty > 3` BOOL,
  `now()` TIMESTAMPTZ, `j -> 'a'` JSONB); a cast column keeps the
  column's name (`at::date` describes as `at`).
- **JSONB operators**: `->` / `->>` (key or array index: `j -> 0`),
  `#>` / `#>>` with a text-array path (`j #>> '{a,0,b}'`), containment
  `@>` / `<@`, and key existence `?`, `?|`, `?&` — everywhere an
  expression goes, including HAVING and grouped select lists.
- **Aggregates**: `COUNT(*)`, `COUNT(col)`, `SUM`, `AVG`, `MIN`, `MAX`,
  `string_agg(x, sep)`, `array_agg`, `bool_and` / `bool_or` / `every`,
  `stddev[_pop|_samp]`, `variance` / `var_pop` / `var_samp`,
  `percentile_cont(f) WITHIN GROUP (ORDER BY x)`, `percentile_disc`,
  `json[b]_agg`, `json[b]_object_agg` — over a column or an expression
  (`SUM(qty * price)`, `MAX(lower(name))`), with `DISTINCT` and `FILTER
  (WHERE ...)`, whole-table or per `GROUP BY` group, including over
  joins. `HAVING` filters on aggregates or group columns (a bare
  boolean aggregate works: `HAVING bool_and(ok)`). An expression *over*
  an aggregate (`SUM(a) / COUNT(*)`) is not yet supported; compute it
  in the client or a derived table.
- **Joins** execute left-deep in the order written — until
  [statistics](#table-statistics) exist for every joined table, at which
  point INNER joins are automatically reordered to drive from the
  cheapest side (`EXPLAIN` says `join reordered by cost`; outer joins,
  self-joins, cross joins and joins with correlated subqueries or ON
  filters always keep the written order). `ON` is a boolean expression
  (parentheses welcome) that must include at least one equality between
  a column of the newly joined table and one from an earlier table; its
  other conjuncts (`tc.relkind = 't'`, `NOT a.attisdropped`, `x IN
  (...)`) are join conditions, evaluated per candidate match — an outer
  join NULL-extends when they fail, unlike a `WHERE` filter. `LEFT`
  keeps the earlier sides' unmatched rows, `RIGHT` the joined table's
  (appended after the lookups, NULL on every earlier side), `FULL` both.
  `JOIN ... USING (c)` and `NATURAL JOIN` (every column name the sides
  share; a cross join when none) equate the named columns, show each
  once in `*`, resolve it unqualified without ambiguity, and read it as
  `COALESCE` across an outer join. `CROSS JOIN` and the comma form
  `FROM a, b WHERE a.x = b.y` are cross products filtered by `WHERE`.
  Join select lists take columns, `*`, expressions, `->`/`->>` paths
  and subqueries; under `GROUP BY` they narrow to plain columns and
  aggregates.
- **WITH (common table expressions)**: `WITH name [(cols)] AS (query),
  ... SELECT ...` — also on `INSERT`, `UPDATE` and `DELETE`, and inside
  a subquery or a set-operation member. Each member is materialized
  once, in order (a member may read the ones before it), and then
  reads like a table anywhere in the statement: the base table, a join
  side, a subquery source, a set-operation member, an `INSERT` source;
  it shadows a real table of its name for the statement. A member may
  be a data-modifying statement with `RETURNING` (`WITH moved AS
  (DELETE ... RETURNING *) SELECT ...`). `WITH RECURSIVE name AS (seed
  UNION [ALL] step)` iterates: the step runs against the rows the
  previous round produced until it produces none (`UNION` drops rows
  seen before), capped at 10000 rounds and a million rows (`54000`).
- **Window functions**: `func(...) OVER ([PARTITION BY exprs] [ORDER BY
  terms] [ROWS | RANGE frame])` in the select list, also inside an
  expression (`amount - lag(amount) OVER (ORDER BY at)` for deltas,
  `count(*) OVER () > 3`, in a `CASE`), and named by a `WINDOW w AS
  (...)` clause (`OVER w`, or `OVER (w ROWS ...)` to add a frame). The
  ranking functions `row_number`, `rank`, `dense_rank`, `percent_rank`,
  `cume_dist`, `ntile(n)`; the offset functions `lag(x [, n [, default]])`
  and `lead`; the value functions `first_value`, `last_value`,
  `nth_value(x, n)`; and every aggregate (`sum`, `avg`, `count(*)`,
  `min`, `max`, `string_agg`, `bool_and`, ...) over the row's frame. The
  frame defaults to the partition up to the current row and its peers
  when ordered (so `sum(x) OVER (ORDER BY at)` is a running total) and
  to the whole partition when not; `ROWS BETWEEN n PRECEDING AND n
  FOLLOWING` (or `UNBOUNDED`, `CURRENT ROW`) gives sliding windows;
  `RANGE` takes only `UNBOUNDED PRECEDING | FOLLOWING` and `CURRENT ROW`
  (peer groups). Windows compute on the gateway over the rows the query
  fetched, after joins and grouping (`rank() OVER (ORDER BY sum(x)
  DESC)` works on a grouped query) and before `DISTINCT`, `ORDER BY`
  and `LIMIT`; a window aggregate is evaluated frame by frame, so a
  wide `ROWS` frame over a large partition costs its width. `DISTINCT`,
  `FILTER` and `WITHIN GROUP` are not accepted on a window call.
- **Subqueries**: scalars anywhere a value goes, `array(SELECT ...)`
  (the subquery's column as a text array, e.g. for `array_to_string`),
  derived tables `FROM (SELECT ...) AS d` — which join like tables, as
  the base or as a join member — and table functions `FROM
  unnest(array) [AS] s(x)`. Correlated subqueries — ones that reference
  the enclosing query's row, in `EXISTS`/`IN`/scalar positions, the
  select list, `array(...)`, `CASE` arms or `OR` groups, over a single
  table or a join — are evaluated per row of the enclosing query,
  memoized on the referenced values, up to 8 nesting levels. An
  uncorrelated scalar subquery can also sort: `ORDER BY x - (SELECT
  avg(x) FROM t)`.
- **Set operations**: `UNION`, `INTERSECT` and `EXCEPT`, each `[ALL]`,
  between selects, `VALUES` lists and parenthesized queries (which may
  carry their own `ORDER BY` / `LIMIT`) with the same number of
  columns. PostgreSQL's precedence: `INTERSECT` binds tighter, the
  others associate left to right. The distinct forms remove duplicates;
  `INTERSECT ALL` keeps the smaller count of a row, `EXCEPT ALL`
  subtracts the right side's count. Column names come from the first
  member and each column's type from all of them (`1 UNION 2.5` is
  DECIMAL; a text member makes the column text). A trailing `ORDER BY`
  / `LIMIT` / `OFFSET` applies to the whole result, by output name or
  position. Every member materializes on the gateway, capped at a
  million rows in flight (`54000`).
- **ORDER BY** takes result-column names, output aliases, positions,
  expressions (`ORDER BY lower(name), qty > 3`, `n * -1` over an
  alias) and, in a grouped query, aggregate calls (`ORDER BY count(*)
  DESC`, whether or not they are selected) and grouping columns; `NULLS
  FIRST | LAST` overrides the default placement (last ascending, first
  descending). It sorts in memory unless the access path already
  delivers the order — ascending along the key, descending via a
  reverse scan, and on sharded timeseries tables either direction via a
  K-way merge of the per-bucket scans (`ORDER BY ts DESC LIMIT n`
  dashboards stop early instead of scanning everything).
- **LIMIT and OFFSET**: `LIMIT n | ALL`, `OFFSET n [ROWS]`, `FETCH
  FIRST | NEXT [n] ROWS ONLY`, on every query shape. `OFFSET` skips
  rows after they are fetched (the scan reads `LIMIT + OFFSET` rows when
  the limit is pushed down), so deep pagination costs the whole prefix:
  prefer keyset pagination (`WHERE id > $last ORDER BY id LIMIT n`) on
  large tables. `LIMIT 0` returns no rows.

Check the plan with `EXPLAIN SELECT ...` — one line naming the access
path — or run it with `EXPLAIN ANALYZE SELECT ...`, which executes the
statement and reports the plan line followed by every stage with its
actual rows and time (each scan and its path, each join level, the
group / window / set-operation and sort stages), then the output row
count and the total time:

```
plan: nested loop inner join; outer (o): full table scan; inner (c) per outer row: point lookup on primary key
  scan orders: full table scan; 1000 rows in 3.412 ms
  scan customers: point lookup on primary key; 1 rows in 0.101 ms
  ...
  join level 1 (c, inner): 1000 rows
  sort: 1000 joined rows in memory
output: 1000 rows; total 41.220 ms
```

The plan-only form:

```
point lookup on primary key
scan of index "by_city" (city = 'oslo') + primary key join
range scan of primary key (series = 'cpu.node1', at >= 2026-08-30 10:00:00+00)
full table scan [~5000 rows]
```

An index scan's `primary key join` fetches the rows behind the matching
entries in pages of 256, each page one batch fanned out to the ranges
that hold the rows, in the index's order (`EXPLAIN ANALYZE` reports
`index join: N primary rows fetched in B batches`), so a lookup matching
a thousand entries is four round trips, not a thousand. The same
batching covers a statement's other point reads: `INSERT` reads all its
uniqueness probes and foreign-key parents at once, `UPDATE` the entries
it moves, `SELECT ... FOR UPDATE` locks its rows in one batch.

A `full table scan` on a big table is the thing to fix (add an index, or
constrain the leading PK columns). When [table statistics](#table-statistics)
exist, the plan carries a ` [~N rows]` estimate and competing paths are
ranked by cost rather than structure — in particular, an index on a
low-selectivity column (few distinct values) correctly loses to the full
scan it would out-fetch.

## Writing

```sql
INSERT INTO t (a, b) VALUES (1, 'x'), (2, 'y');     -- multi-row is one atomic statement
UPDATE t SET b = 'z', c = c + 1 WHERE a = 1;        -- expressions: col ± value
DELETE FROM t WHERE a = 1;

INSERT INTO t (a, b) VALUES (3, 'q') RETURNING a, b;            -- any expression over the written row, or *
UPDATE t SET c = c + 1 WHERE a = 1 RETURNING *;
DELETE FROM t WHERE a = 1 RETURNING a;

INSERT INTO t (a, b) VALUES (1, 'x') ON CONFLICT DO NOTHING;    -- any unique key; the tag counts inserted rows
INSERT INTO t (a, b, c) VALUES (1, 'x', 1)
  ON CONFLICT (a) DO UPDATE SET c = t.c + excluded.c, b = excluded.b
  WHERE t.c < 100                                               -- optional; EXCLUDED is the proposed row
  RETURNING c;
INSERT INTO t (a, b) VALUES (1, 'x') ON CONFLICT ON CONSTRAINT t_pkey DO NOTHING;
UPSERT INTO t (a, b, c) VALUES (1, 'x', 5);                     -- ON CONFLICT (primary key) DO UPDATE SET every column
```

`RETURNING` rows come from the values the statement already has (no
extra read). The `ON CONFLICT` target must be the primary key or a
unique index — by columns or by constraint name (`<table>_pkey`, the
index name) — else SQLSTATE `42P10`; a conflict on some other unique key
is still the usual `23505`. `ON CONFLICT DO NOTHING` without a target
accepts a conflict on any unique key. `DO UPDATE` cannot change primary
key columns, and the same key twice in one statement is `21000`. The
whole statement is one transaction: the read of the existing row and the
write are serializable, so two sessions upserting the same key never lose
an update — one of them restarts with `40001` and retries.

Batching inserts matters enormously for throughput — a 100-row `INSERT` is
one replication round; 100 single-row statements are 100.

### Bulk loading with COPY

```sql
COPY t (a, b) FROM STDIN;                  -- text format (psql \copy sends this)
COPY t FROM STDIN WITH (FORMAT csv);
```

```sh
psql "$URL" -c "\copy t FROM data.csv WITH (FORMAT csv)"
```

pgx's `CopyFrom` (binary format) works too and is the fastest loader.
COPY runs through the same pipeline as `INSERT` — defaults, `NOT NULL`,
unique checks, and secondary indexes all apply — but commits in **chunks**
of 128 rows / 1 MiB as data streams in. That makes huge loads safe
(bounded memory, bounded transactions), at a price worth knowing: a
mid-load failure keeps the chunks already committed (the error reports
the failing row and the committed count), and COPY cannot run inside
`BEGIN`. Only the `FORMAT text|csv|binary` option is supported; `COPY TO`
is not.

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

## Session settings

```sql
SET statement_timeout = '5s';                 -- 57014 when a statement runs longer
SET lock_timeout = 200;                       -- ms: 55P03 instead of waiting on a live intent
SET idle_in_transaction_session_timeout = '1min'; -- 25P03: the idle block is rolled back, the connection ended
SET TIME ZONE 'America/New_York';             -- TIMESTAMPTZ output rendered in the zone
SET application_name = 'billing';             -- shown by pg_stat_activity / SHOW SESSIONS
SET LOCAL statement_timeout = '30s';          -- inside a block: until COMMIT / ROLLBACK
SET TRANSACTION READ ONLY;                    -- this block refuses writes (25006)
SET default_transaction_read_only = on;       -- every transaction, until reset
RESET statement_timeout;  RESET ALL;  SET x = DEFAULT;
SHOW statement_timeout;  SHOW ALL;  SELECT * FROM pg_settings;
```

`SET [SESSION | LOCAL] name {TO | =} value`, `SET TIME ZONE`, `SET NAMES`,
`SET [SESSION CHARACTERISTICS AS] TRANSACTION ...`, `RESET name`, `RESET
ALL` and `SHOW name` work over these variables — every one of them is
honored or reports its real value; an unknown variable is `42704`, an
invalid value `22023`:

| Variable | Values | Effect |
|---|---|---|
| `statement_timeout` | ms, or `'5s'`, `'1min'`, `'500ms'`; `0` = none | a statement past it is cancelled: `57014`, its transaction block failed |
| `lock_timeout` | as above | a wait on another transaction's live write intent past it fails with `55P03` (without it the wait lasts the conflict budget, then `40001`) |
| `idle_in_transaction_session_timeout` | as above | a connection idle inside a block past it is ended with `25P03`; the block rolls back and its intents are released — the fix for stranded transactions from crashed clients |
| `TimeZone` | an IANA name, `UTC`, `+05:30` | `TIMESTAMPTZ` text output on the wire renders in the zone (`2024-07-04 08:00:00-04`); storage, comparison and the binary format stay UTC; `TIMESTAMP` (without time zone) is unaffected |
| `application_name` | any text; the startup parameter too | `pg_stat_activity`, `SHOW SESSIONS` and the dashboard's activity view show it |
| `search_path` | any list | accepted and reported; `public` is the only schema (`pg_catalog` and `information_schema` are always visible) |
| `transaction_read_only`, `default_transaction_read_only`, `SET TRANSACTION READ ONLY` / `READ WRITE` | `on` / `off` | a read-only transaction refuses `INSERT`, `UPDATE`, `DELETE`, `COPY` and DDL with `25006` |
| `transaction_isolation`, `SET TRANSACTION ISOLATION LEVEL ...` | any level | accepted (drivers set one on connect); every transaction is serializable and `SHOW` says so |
| `DateStyle`, `client_encoding` | `ISO[, order]`, `UTF8` | the supported values; anything else is `22023` |
| `foreign_key_cascade_limit` | a positive integer | the per-statement cascade cap |
| `role`, `SET [LOCAL] ROLE name \| NONE`, `RESET ROLE` | a role the session user belongs to (admins: any) | the current role: privilege checks and `current_user` follow it, `session_user` stays; `SHOW role` reports `none` without one (see the [security guide](security.md#roles-and-privileges)) |

`SET LOCAL` and `SET TRANSACTION` apply to the current block and end
with it (outside a block they do nothing, as in PostgreSQL). A changed
`application_name`, `TimeZone`, `DateStyle` or `client_encoding` is
announced to the client with `ParameterStatus`, as PostgreSQL does.

**Cancellation.** Every connection gets a process ID and a secret
(`BackendKeyData`); `psql`'s Ctrl-C, a driver's context cancellation
(pgx) and every pool's cancel path send a `CancelRequest`, which stops
the statement in flight with `57014` and rolls its transaction back.
The process ID names the node in its high bits, so a cancel that lands
on another node behind a load balancer is forwarded there. `SELECT
pg_backend_pid()` reports a session's ID; an admin cancels another
session with `pg_cancel_backend(pid)` or ends it with
`pg_terminate_backend(pid)` (`57P01`), on any node of the cluster.

**Sessions.** `SHOW SESSIONS` and `pg_stat_activity` list the serving
node's sessions (pid, user, database, `application_name`, client
address, state, the statement in flight or the last one, when the
connection, the block and the statement began); each node keeps its
own registry, so a cluster-wide view is the union over nodes (the
dashboard's activity view, `/api/activity`).

## Historical and follower reads

```sql
SELECT COUNT(*) FROM users AS OF SYSTEM TIME '-5s';
SELECT ... AS OF SYSTEM TIME '2026-08-30T10:00:00Z';
SELECT ... AS OF SYSTEM TIME with_max_staleness('10s');
```

The first two pin the read to a fixed past timestamp; reads older than the
closed timestamp lag (~3s) are served by the **local** replica without
contacting the leader — cheap read scaling for dashboards and reports.

`with_max_staleness('10s')` inverts the guess: instead of picking a
staleness and hoping it is old enough, you state the staleness you can
*tolerate*, and the gateway reads at **the freshest timestamp its own
replicas can serve** — never staler than the bound. Data the gateway holds
locally is answered without touching leaders (it keeps working even if
they are unreachable); ranges it cannot serve within the bound fall back
to their leaders transparently. One timestamp covers the whole statement,
so multi-range results stay consistent. Watch
`datax_follower_reads_total` (served locally) vs
`datax_follower_read_fallbacks_total` (sent to a leader) to see whether
your bound is doing its job.

All forms: bounded by the GC window (25h) on the old side; not allowed
inside `BEGIN`; a freshly written row may be invisible until the closed
timestamp passes its write (~the lag) — that is the staleness you signed
up for.

## Table statistics

```sql
ANALYZE users;      -- collect stats for one table (admin only)
ANALYZE;            -- all tables
SHOW STATS FOR users;
```

`ANALYZE` sweeps the table at a frozen timestamp (safe under concurrent
writes, no locks) and stores the exact row count plus per-column
distinct-value and NULL counts — distinct counts are exact up to 256
values and a sketch estimate (±~10%) beyond. It cannot run inside
`BEGIN`. The statistics feed the query planner; `SHOW STATS` (any user)
shows what the planner sees, one row per column.

A background sampler also keeps statistics fresh without ANALYZE: every
minute (configurable) one node re-collects at most one table whose
statistics are missing or older than 10 minutes, in paced 1024-row
chunks. Watch `datax_stats_refreshes_total` and
`datax_stats_rows_scanned_total` for its cost.

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
- `retention` (`30d`, `12h`, or interval text such as `'7 days'`; a
  month counts 30 days): rows older than this are garbage
  collected automatically.
- Queries are unchanged — `WHERE series = '...' AND at >= ...` fans out
  over the buckets (visible in `EXPLAIN`). Fan-out costs read latency:
  measured ~2× point-read p50 at `shards=8`, in exchange for ~linear
  insert scaling.
- Re-shard online: `ALTER TABLE metrics SET (shards = 16);` — writes
  never stop, and secondary indexes on the table are rebuilt and swapped
  along with the rows.

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

## Introspection: SHOW and the catalogs

```sql
SHOW TABLES [FROM db];          -- table_name
SHOW COLUMNS FROM t;            -- column_name, data_type, is_nullable, column_default, indices
SHOW INDEXES FROM t;            -- table_name, index_name, non_unique, seq_in_index, column_name
SHOW CREATE TABLE t;            -- the CREATE TABLE statement that recreates t
SHOW VIEWS;                     -- view_name, definition (SHOW CREATE VIEW v for one)
SHOW USERS;                     -- username, is_admin, member_of (the login roles)
SHOW ROLES;                     -- role_name, can_login, is_admin, member_of (every role)
SHOW GRANTS [ON t | ON DATABASE d] [FOR role];
                                -- database_name, schema_name, relation_name, grantee,
                                -- privilege_type, is_grantable
SHOW GRANTS ON ROLE [r] [FOR member];  -- role_name, member, is_admin
SHOW DATABASES;                 -- database_name, owner
SHOW STATS FOR t;               -- see Table statistics
SHOW FUNCTIONS;                 -- name, signature, category, volatility, aliases, description
SHOW ALL;                       -- every session setting
SHOW server_version;            -- one setting (SHOW TIME ZONE, SHOW search_path, ...);
                                -- an unknown name is SQLSTATE 42704
```

The PostgreSQL catalogs are there too, as read-only virtual tables over
the live schema: `pg_catalog.pg_database`, `pg_namespace`, `pg_class`,
`pg_attribute`, `pg_type`, `pg_index`, `pg_constraint`, `pg_attrdef`,
`pg_am`, `pg_roles`, `pg_user`, `pg_settings`, `pg_tables`, `pg_views`,
`pg_indexes`, `pg_collation`, `pg_tablespace`, `pg_stat_user_tables`
(and empty stand-ins for the catalogs of features datax lacks —
policies, publications, extensions, functions, triggers, ...), plus
`information_schema.schemata`, `tables`, `views`, `columns`,
`table_constraints`, `key_column_usage`, `statistics` and
`role_table_grants`. OIDs are stable across the cluster (a table's is its
descriptor ID; `'t'::regclass` gives it). A bare `pg_class` resolves to
the catalog when no table of that name exists in the current database.
This is what makes `psql`'s `\d`, `\dt`, `\dv`, `\di`, `\du`, `\l`, `\dn`, `\dp`
and friends, and ORM schema introspection, work unmodified — see
[Differences from PostgreSQL](postgres-differences.md#what-psql-and-orms-can-see).

## Reserved tables

`datax_metrics` belongs to the cluster: its nodes create it and record
their metrics into it (see the operations guide's "Metrics history").
`CREATE TABLE datax_metrics`, `DROP TABLE datax_metrics` and column DDL
on it are refused. Admins may read and delete from it and set its
`retention` and `shards`; `GRANT SELECT ON datax_metrics TO <user>` lets
another user read it, and no grant lets a non-admin write to it.

## Views

```sql
CREATE VIEW big_orders AS SELECT id, cust, qty FROM orders WHERE qty > 100;
CREATE OR REPLACE VIEW big_orders (id, customer, qty) AS SELECT id, cust, qty FROM orders WHERE qty > 50;
SELECT customer, count(*) FROM big_orders GROUP BY customer;   -- reads like a table
CREATE VIEW top AS SELECT customer FROM big_orders GROUP BY customer HAVING count(*) > 3;  -- a view over a view
SHOW VIEWS;                       -- view_name, definition
SHOW CREATE VIEW big_orders;
DROP VIEW [IF EXISTS] top [, ...] [CASCADE];
```

A view stores its query as written and runs it when a statement names
it: the view's rows are materialized for the statement — once, however
many times the statement names it — and it then reads anywhere a table
does: the base of a `SELECT`, a join side, a subquery, a set-operation
member, an `INSERT ... SELECT` source, inside `WITH`. A view over a
view expands the same way. A view's query is any `SELECT` (joins,
grouping, `WITH`, window functions, set operations) without
parameters, `AS OF SYSTEM TIME` or `FOR UPDATE`; the optional column
list renames its output. Views are read-only (`42809` on `INSERT`,
`UPDATE`, `DELETE`, `COPY`, `ALTER TABLE`, `CREATE INDEX`, `TRUNCATE`).
`SELECT *` in a view sees a column added to the base table later
(PostgreSQL freezes the list at creation).

**Dependencies.** A view records the tables and views it reads. `DROP
TABLE` / `DROP VIEW` refuse (`2BP01`) while a view depends on the
relation unless `CASCADE`, which drops the dependent views too;
`RENAME TO`, `RENAME COLUMN` and `DROP COLUMN` on a table a view reads
are refused the same way (drop or replace the view first — its query
is stored as text). `CREATE OR REPLACE VIEW` keeps the view's identity
and grants and may change its column set; a view cannot depend on
itself (`42P17`).

**Privileges.** Reading a view needs `SELECT` on the view; its query
runs with the view's *owner's* privileges (PostgreSQL's definer rule),
so a reader needs no grant on the tables behind it. `GRANT` /
`REVOKE ... ON view` work as on tables; creating a view needs the
database's `CREATE` privilege (and the creator's own access to what it
reads), dropping one is for its owner or an admin.

The catalogs show views as PostgreSQL does — `pg_class` with `relkind =
'v'`, `pg_views`, `information_schema.tables` (`VIEW`) and
`information_schema.views`, `pg_attribute` for their columns — so
`psql`'s `\dv` and `\d view` work. Views need **cluster version v9**
(`0A000` until `datax debug upgrade` finalizes it).

## Databases

A cluster holds any number of databases; a table belongs to exactly one.
A new cluster has `datax` (the database every connection URL in this
guide names) and a reserved, empty `system` database.

```sql
CREATE DATABASE app;
CREATE DATABASE IF NOT EXISTS app;
SHOW DATABASES;
USE app;                      -- or: SET database = app
SELECT current_database();    -- app
CREATE TABLE orders (id INT8 PRIMARY KEY);      -- app.orders
SELECT id FROM app.orders;                      -- from any database
SELECT id FROM app.public.orders;               -- public is the only schema
ALTER DATABASE app RENAME TO shop;
DROP DATABASE shop;           -- refused while it holds tables ...
DROP DATABASE shop CASCADE;   -- ... unless CASCADE drops them too
```

A connection starts in the database its URL names (`postgres://.../app`);
an unknown one is refused with SQLSTATE `3D000`, as in PostgreSQL. An
unqualified table name resolves in the session's current database;
`db.table` reaches another database. `SHOW TABLES` (tables only; `SHOW
VIEWS` lists the views) and `ANALYZE` (with no table) act on the current
database. `datax` and `system` cannot be
dropped or renamed, and the session cannot drop the database it is in.

Database privileges: `GRANT CREATE | CONNECT | ALL ON DATABASE app TO
bob`. `CREATE` lets a non-admin create tables there (admins and the
database's owner always can). `CONNECT` is granted to `PUBLIC` on every
database, as in PostgreSQL; `REVOKE CONNECT ON DATABASE app FROM PUBLIC`
closes it to everyone but admins, the owner and roles holding an
explicit `CONNECT` grant, checked when a session opens the database
(the URL or `USE`). Table grants take qualified names (`GRANT SELECT ON
app.orders TO bob`). Roles, ownership, the schema and sequence scopes,
`ALL TABLES IN SCHEMA`, default privileges and grant options are in
the [security guide](security.md#roles-and-privileges).

Databases arrive with cluster version v6. A cluster upgraded from an
earlier version keeps every existing table in `datax`; `datax debug
upgrade` finalizes v6 and moves the catalog entries in one transaction,
after which `CREATE DATABASE` works. Row data never moves: a table's
database is a catalog fact, and backups carry the database catalog along
with the tables.

