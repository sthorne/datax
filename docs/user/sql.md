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
| `TIMESTAMPTZ` | `TIMESTAMP WITH TIME ZONE` | an instant, stored as UTC; microsecond precision on the binary wire (UTC whatever the session's zone); years 1678 to 2261 (a value outside is refused). Text without an offset reads in the session's [`TimeZone`](#session-settings), text with one (`+05`, `Z`) or a zone name (`'2024-07-01 12:00 Europe/Paris'`) as it says; text output, `::text`, `::date`, `date_trunc`, `extract` and `to_char` are in the session's zone. `AT TIME ZONE` / `timezone(zone, ts)` convert to a `TIMESTAMP` in a zone and back (see [Time zones](#time-zones)) |
| `TIMESTAMP` | `TIMESTAMP WITHOUT TIME ZONE` | wall-clock time: an offset in the input is ignored (`'2024-01-02 03:04:05+05'` stores `03:04:05`) and the output carries none; `timestamp` (OID 1114) on the wire; the session's `TimeZone` never applies to it. A `::timestamp` cast, `ts ± interval`, `date_trunc` over it and `timestamptz AT TIME ZONE zone` are timestamps too; other expressions over it are `TIMESTAMPTZ` |
| `TIMESTAMP(p)`, `TIMESTAMPTZ(p)` | | `p` in 0–6: values round to `p` fractional digits on write (half away from zero) |
| `INTERVAL` | `INTERVAL` with field qualifiers (`DAY TO SECOND`, accepted and ignored) | PostgreSQL's months / days / clock triple, so `'1 month'` steps the calendar and `'1 day'` is a day whatever the hour; input in the verbose (`'1 year 2 mons 3 days 04:05:06'`, `'2h30m'`, `'1 day ago'`), SQL standard (`'1-2 3 04:05:06'`) and ISO 8601 (`'P1Y2M3DT4H5M6S'`) forms and as `INTERVAL '...'`; rendered as PostgreSQL does (`1 day -02:00:00`, `-1 days +12:00:00`); compares and sorts by PostgreSQL's rule (a month is 30 days, a day 24 hours: `'30 days' = '1 month'`); `interval` (OID 1186) on the wire; cluster version v10 for a column |
| `TIME` | `TIME WITHOUT TIME ZONE`, `TIME(p)` | time of day, microsecond precision on the wire (`time`, OID 1083); input `'04:05:06.789'`, `'4:05 PM'`, `'16:05'`, a timestamp text (its clock is taken), an offset is ignored; `24:00:00` allowed; `TIME WITH TIME ZONE` is refused; cluster version v10 for a column |
| enum types | `CREATE TYPE name AS ENUM ('a', 'b', ...)` | a column of the type stores the label's ordinal and orders by declaration (`ORDER BY`, `min` / `max`, indexes and keys); input and output are the label; an unknown label is `22P02`; `'a'::name` casts a literal; `ALTER TYPE name ADD VALUE [IF NOT EXISTS] 'c'` appends (no `BEFORE` / `AFTER`; usable at once on every gateway); `DROP TYPE [IF EXISTS] name` refuses while a column uses it (`2BP01`); `pg_type` (`typtype = 'e'`), `pg_enum`, `information_schema.columns` (`USER-DEFINED`), psql's `\dT` and `\d`; described on the wire with the type's OID (values as labels, text and binary); no arrays of enums, no `RENAME VALUE`; cluster version v10 |
| `T[]` | `T ARRAY`, `T[][]` (one-dimensional whatever the brackets) | an array of any scalar type but `JSONB`, `TSVECTOR` and `TSQUERY` (`INT8[]`, `TEXT[]`, `VARCHAR(3)[]`, `DECIMAL(6,2)[]` — the modifiers apply per element); literals `'{1,2}'`, `'{a,"b c",NULL}'` and `ARRAY[1, 2]`; `a[i]` (1-based, NULL out of range; no slices); `= ANY(a)` / `<> ALL(a)` and the other comparison operators with `ANY` / `ALL`; `@>`, `<@`, `&&`; `||` with an array or an element; equality and ordering element by element; `unnest` in `FROM` (`FROM unnest(a) AS u(x)`) and in a FROM-less select list (`SELECT unnest($1::int8[])` expands a parameter into rows), `array_agg`, `array_length`, `cardinality`, `array_append` / `array_prepend` / `array_cat` / `array_position` / `array_remove` / `array_to_string` / `string_to_array` / `array_upper` / `array_lower` / `array_ndims`; PostgreSQL's array OIDs, text and binary formats on the wire (pgx slices scan and bind; `WHERE id = ANY($1)` with a slice); not indexable and not a key; `GROUP BY` and `ORDER BY` work; cluster version v10 for a column |
| `DATE` | | |
| `BYTES` | `BYTEA` | `'\xdeadbeef'` hex literals; a value is stored and read whole, and a row cannot exceed 60 MiB (`54000`) — for more than a few MiB use a [large object](#large-objects) |
| `UUID` | | |
| `JSONB` | `JSON` | normalized on write: sorted keys, compact |
| `TSVECTOR` | | a document reduced for searching: its distinct words (lexemes), sorted, each with the positions it occurs at and their weights — `'cat':3 'fat':2,11A`. Made by `to_tsvector`, or written as a literal (taken as written: no stemming). `tsvector` (OID 3614) on the wire, text and binary. Equality only; not a key, not in an ordinary index — a GIN index serves `@@`. See [Full-text search](#full-text-search). Cluster version v21 for a column |
| `TSQUERY` | | a search: lexemes joined by `&`, `\|`, `!` and `<->` — `'fat' & ( 'cat' \| 'rat' )`. Made by `to_tsquery` and its relatives, or written as a literal. `tsquery` (OID 3615) on the wire, text and binary. Cluster version v21 for a column |

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
  JSONB cannot be a primary key or part of an ordinary index; a GIN
  index serves containment and key existence (below). `->`/`->>` (by key or array
  index) and `#>`/`#>>` (by path: `j #>> '{a,0,b}'`) work everywhere an
  expression does — single-table queries, joins, grouped select lists
  and HAVING; a LEFT-joined NULL side extracts to NULL.
- Containment: `j @> '{"tags":["go"]}'` and `<@` — PostgreSQL semantics
  (objects contain recursively, arrays contain each right element
  somewhere, a top-level array contains a matching scalar), numbers
  compared numerically (`1` contains `1.0`) — and key existence `j ?
  'k'`, `j ?| '{a,b}'`, `j ?& '{a,b}'`. Without an index they are
  filters over every row; `CREATE INDEX ... USING GIN (j)` makes `@>`
  and the three key operators index lookups — see
  [JSONB indexes](#jsonb-indexes).
- `VECTOR(n)` (pgvector's `vector`) holds `n` float4 elements, for
  embeddings: `'[0.1, 2, -3.5]'`. See [Vectors](#vectors) for the
  distance operators, nearest-neighbour queries and the index.
- Every type except JSONB, TSVECTOR, TSQUERY and VECTOR can appear in
  primary keys and ordinary indexes.

## DDL

```sql
CREATE TABLE t (
  a INT8 NOT NULL,
  b TEXT DEFAULT 'x',            -- a constant, or an expression (below)
  c DECIMAL,
  PRIMARY KEY (a)                -- or: a INT8 PRIMARY KEY
);
CREATE TABLE IF NOT EXISTS t (...);
DROP TABLE [IF EXISTS] t;       -- space reclaimed after the table's GC window (gc_ttl, else the node's 25h)

CREATE [UNIQUE] INDEX [CONCURRENTLY] [IF NOT EXISTS] by_b ON t (b);   -- online: no write outage
CREATE INDEX ON t (b, c);                                -- unnamed: t_b_c_idx (t_b_c_idx1, ... when taken)
CREATE INDEX t_b_like ON t (b text_pattern_ops);         -- also varchar_/bpchar_pattern_ops, COLLATE "C" / "POSIX" / "default"
CREATE INDEX [IF NOT EXISTS] by_doc ON t USING GIN (doc);  -- an inverted index on a JSONB column (below)
CREATE INDEX by_body ON t USING GIN (to_tsvector('english', body));  -- a full-text index on a TEXT column (below)
CREATE INDEX [IF NOT EXISTS] by_vec ON t USING ivfflat (v vector_l2_ops) WITH (lists = 100);  -- nearest-neighbour search (below)
DROP INDEX [CONCURRENTLY] [IF EXISTS] by_b;             -- entries reclaimed after the commit
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
ALTER TABLE t SPLIT AT VALUES (3000) WITH EXPIRATION INTERVAL '1 day';  -- ...held until then (an interval from now, or a timestamp)
ALTER TABLE t UNSPLIT AT VALUES (1000);                 -- stop holding a boundary; the ranges merge back when small
ALTER TABLE t UNSPLIT ALL;                              -- ...every manual boundary in the table
CREATE TABLE t2 (LIKE t INCLUDING ALL);                  -- columns, defaults, constraints, indexes, comments
CREATE TABLE big AS SELECT id, qty FROM t WHERE qty > 10;          -- a hidden rowid key
CREATE TABLE keyed (order_id, qty, PRIMARY KEY (order_id)) AS SELECT id, qty FROM t [WITH NO DATA];
COMMENT ON TABLE t IS 'orders';  COMMENT ON COLUMN t.b IS 'body';  COMMENT ON INDEX by_b IS NULL;
SHOW TABLES;
```

`CREATE INDEX` backfills online in bounded chunks, like PostgreSQL's
`CREATE INDEX CONCURRENTLY`, so `CONCURRENTLY` is accepted and changes
nothing. It cannot run inside an explicit transaction, except on a table
created in the same transaction block (see [DDL in transaction
blocks](#ddl)); the `CONCURRENTLY` forms of `CREATE INDEX` and `DROP
INDEX` never can (`25001`), as in PostgreSQL. An ordinary index compares
its keys bytewise — the order of the `C` collation — so the string
types' operator classes (`text_pattern_ops`, `varchar_pattern_ops`,
`bpchar_pattern_ops` and the defaults `text_ops`, `varchar_ops`,
`bpchar_ops`) and `COLLATE "C"`, `"POSIX"` or `"default"` are accepted
on a key column and shown by `pg_get_indexdef`; any other collation is
refused (`0A000`), as is an operator class of another type (`42804`).
They do not make `LIKE 'abc%'` use the index.
`DROP INDEX` names the index alone (it is found on whichever table of
the database carries it); an index a `UNIQUE` constraint owns is
refused (`2BP01`) — drop the constraint. The index disappears from the
schema at once and its entries are reclaimed after the statement's
transaction commits and every gateway has adopted the change.

### JSONB indexes

```sql
CREATE INDEX docs_doc ON docs USING GIN (doc);            -- or (doc jsonb_ops)

SELECT id FROM docs WHERE doc @> '{"owner": {"team": "search"}}';
SELECT id FROM docs WHERE doc @> '{"tags": ["pii", "eu"]}';     -- both tags, any order
SELECT id FROM docs WHERE doc ? 'expires_at';                    -- has the top-level key
SELECT id FROM docs WHERE doc ?| '{warn,block}' AND doc @> $1;  -- combined; parameters work
```

A GIN index on a JSONB column holds one entry for every value in each
document, together with the path to it, and one for every top-level key.
Four operators use it, with the column alone on the left and a literal
or parameter on the right: `@>`, `?`, `?|` and `?&`. They may be combined
with `AND` (and with any other condition), and with `OR` as long as every
branch of the `OR` is one of them. `EXPLAIN` shows the lookups:

```
inverted scan of index "docs_doc" ("owner"."team" = "search") + primary key join, rows rechecked
```

Good to know:

- **Results are the same as without the index.** The index finds
  candidate rows; each is then checked against the whole `WHERE` clause.
- **What it does not serve**: `<@`, `NOT (doc @> ...)`, a comparison of
  an extracted value (`doc->>'n' = '5'`), containment under a path
  (`doc->'a' @> ...`), and a right-hand side taken from another column.
  A query for an empty container alone (`doc @> '{}'`, `doc @> '[]'`)
  matches by shape, not by value, and scans. These all still work, as
  filters.
- **Writes** cost one KV write per value in the document, in the same
  transaction as the row. An `UPDATE` rewrites only the entries of the
  values that changed.
- **Limits**: a document may have at most 8,192 distinct indexed values
  and keys (`54000` beyond that; the write is refused). Long strings and
  deep paths are indexed by a digest, so there is no limit on their
  length. A GIN index takes one column and cannot be `UNIQUE`.
- **Choosing**: with a condition that pins an indexed or primary-key
  column by equality in the same query, the planner prefers that
  condition unless table statistics say otherwise. `SET
  inverted_index_scan = off` (or `on`) overrides the choice for a
  session, which is the way to compare the two plans.
- `jsonb_path_ops` is accepted and builds the same index.
- Needs cluster version v21; on a cluster that has not finalized it the
  statement is refused (`0A000`).

### Full-text search

```sql
CREATE TABLE articles (id INT8 PRIMARY KEY, title TEXT, body TEXT);
CREATE INDEX articles_body ON articles USING GIN (to_tsvector('english', body));

SELECT id FROM articles
 WHERE to_tsvector('english', body) @@ to_tsquery('english', 'index & (scan | lookup) & !slow');
SELECT id FROM articles
 WHERE to_tsvector('english', body) @@ phraseto_tsquery('english', 'full text search');   -- the words, adjacent, in order
SELECT id FROM articles
 WHERE to_tsvector('english', body) @@ websearch_to_tsquery('english', $1);                -- what a user typed in a search box

-- The ten best matches, best first, each with an excerpt.
SELECT id, ts_rank(to_tsvector('english', body), to_tsquery('english', $1)) AS rank,
       ts_headline('english', body, to_tsquery('english', $1)) AS excerpt
  FROM articles
 WHERE to_tsvector('english', body) @@ to_tsquery('english', $1)
 ORDER BY rank DESC LIMIT 10;
```

Text is searched by reducing it to a **`tsvector`** — its words, cut
down to *lexemes* so that "Indexes", "indexed" and "indexing" are all
`index` — and matching that against a **`tsquery`** with the `@@`
operator. A GIN index over the lexemes makes `@@` a lookup instead of a
read of every row; `EXPLAIN` shows it:

```
inverted scan of index "articles_body" ("index" AND "scan") + primary key join, rows rechecked
```

**Configurations** decide how text becomes lexemes. There are two:
`english` drops stop words (*the*, *of*, *and*: 127 of them) and stems
the rest with the Snowball English stemmer; `simple` only lower-cases.
Both split text the same way: a word is a run of letters and digits
(Unicode letters count; case is folded, accents are kept), `3.14` is one
number, `ann@example.com` and `example.com` are one token each, and
everything else — spaces, punctuation, hyphens, underscores, apostrophes
— separates words. A word with a digit in it, an address and a host name
are never stemmed. `SET default_text_search_config = 'simple'` changes
what a call that names no configuration uses (the default is `english`).

**Queries.** `to_tsquery` takes the operators: `a & b`, `a | b`, `!a`,
`a <-> b` (b directly after a), `a <3> b` (b three words after a),
parentheses, `prefix:*` (any word starting with it) and `word:AB` (only
at positions of weight A or B). `plainto_tsquery` requires every word of
a plain text, `phraseto_tsquery` requires them in order, and
`websearch_to_tsquery` reads search-box syntax (`"a phrase" -excluded
this or that`) and never raises a syntax error. Stop words are dropped
from a query as they are from a document; a phrase keeps the gap they
leave (`phraseto_tsquery('the cats ate the rats')` is `'cat' <-> 'ate'
<2> 'rat'`). A query made only of stop words matches nothing.

**Two ways to index.**

- `USING GIN (to_tsvector('english', body))` indexes a `TEXT` column
  directly. Queries must use the same expression —
  `to_tsvector('english', body) @@ ...` — and the same configuration
  (`to_tsvector(body)` counts when the session's default is that
  configuration). Nothing else needs maintaining: writing `body` updates
  the index.
- A `TSVECTOR` column with `USING GIN (v)`, for a vector that is not a
  plain function of one column — a title weighted above a body, say:

  ```sql
  ALTER TABLE articles ADD COLUMN v TSVECTOR;
  CREATE INDEX articles_v ON articles USING GIN (v);
  UPDATE articles SET v = setweight(to_tsvector('english', title), 'A') || to_tsvector('english', body);
  SELECT id FROM articles WHERE v @@ to_tsquery('english', 'index:A');   -- in the title
  ```

  The application keeps the column up to date: there are no generated
  columns and no triggers, so every statement that writes `title` or
  `body` must write `v` too.

**Ranking and excerpts.** `ts_rank(vector, query)` scores a row by how
often the query's words occur and, for words required together, how
close they are; `ts_rank_cd` by the shortest stretches of the document
that satisfy the query. Both take an optional leading array of four
weights (for D, C, B, A positions) and a trailing normalization bit
mask, as in PostgreSQL. `ts_headline([config,] text, query [, options])`
returns a fragment of the text with the matching words wrapped in
`<b>...</b>` (`StartSel`, `StopSel`, `MaxWords`, `MinWords`,
`HighlightAll`). The other functions — `setweight`, `strip`, `numnode`,
`tsvector_to_array`, `get_current_ts_config` — and `tsvector || tsvector`
are in the [functions reference](functions.md#full-text-search).

Good to know:

- **Results are the same as without the index.** The index holds
  lexemes only; positions and weights stay in the vector. It finds the
  rows that hold the query's words, and each is then checked against the
  whole `WHERE` clause — which is where a phrase's adjacency and a
  weight are decided.
- **What the index serves**: `@@` with the indexed column or expression
  on the left and a query known before any row is read (a literal, a
  parameter, a function of them) on the right; combined with `AND`
  freely and with `OR` when every branch is such a condition. `a & b`
  looks up both words, `a | b` either, `a <-> b` both (adjacency is
  rechecked), `a:*` every word with the prefix (one range of the index).
- **What it does not**: a negation says which words a row lacks, and the
  index lists the rows that hold a word. So `a & !b` looks up `a` and
  rechecks, and `!b` alone — or `a | !b`, or `NOT (v @@ ...)` — reads
  the whole table. The query on the left (`query @@ vector`) also works
  only as a filter.
- **Ranked queries read every match.** `ORDER BY ts_rank(...) DESC LIMIT
  10` fetches all matching rows, ranks them and keeps ten; there is no
  index order by rank. A query that matches a large part of the table is
  slow however small the `LIMIT`.
- **`text @@ tsquery` is refused** (`0A000`): write `to_tsvector(config,
  text) @@ query`. A string literal beside a typed operand is read as
  the other type (`v @@ 'cat & dog'`).
- **Writes** cost one KV write per distinct lexeme of the document, in
  the same transaction as the row; an `UPDATE` rewrites only the entries
  of the words that came or went.
- **Limits**: a lexeme is at most 2,046 bytes (a longer word is skipped
  by `to_tsvector`), positions run to 16,383 (later words are recorded
  at 16,383), a lexeme keeps its first 256 positions, and a row in a
  full-text index may have at most 8,192 distinct lexemes (`54000`
  beyond that; the write is refused).
- Needs cluster version v21 for the two column types and for the index.

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
and **`COMMENT ON CONSTRAINT name ON table IS ...`** store the text in
the descriptor; `obj_description`, `col_description` and
`pg_description` render it, so `psql`'s `\d+`, `\dt+` and `\dv+` show
the descriptions, and `\dd` the constraints' (a primary key, `UNIQUE`,
`CHECK` or foreign key; a constraint's comment is its own, apart from
its index's, and is dropped with it). Comments survive renames, default
changes and type rewrites.

**DDL in transaction blocks.** A statement that is one descriptor
write runs inside `BEGIN ... COMMIT` like any other and commits or rolls
back with the block: `CREATE TABLE`, `DROP TABLE`, `ADD` / `DROP
COLUMN`, every `RENAME`, `SET` / `DROP DEFAULT`, `DROP CONSTRAINT`,
`DROP NOT NULL`, `DROP INDEX`, `TRUNCATE`, `COMMENT ON`, `CREATE` /
`DROP VIEW`, the sequence, database, role and ownership statements,
`GRANT` / `REVOKE`, `CREATE` / `DROP EXTENSION`. So does `CREATE INDEX`
on a table created earlier in the same block — the table is invisible
to everyone else until the block commits, so the index is published at
once, filled from the block's own rows, and commits or rolls back with
the table (a Rails or Django migration's `create_table` and `add_index`).
After `ROLLBACK TO SAVEPOINT` the block no longer counts the tables it
created as its own. The multi-transaction statements — `CREATE INDEX` on
any other table, `ADD CONSTRAINT`, `VALIDATE CONSTRAINT`, `SET NOT NULL`,
`ALTER COLUMN TYPE`, `SET (shards = N)`, `CREATE TABLE ... AS`,
`ANALYZE` — publish, drain and sweep (or stream) in several transactions
of their own and are refused inside a block with `25001`; so are `SPLIT
AT` and `UNSPLIT`, range operations rather than descriptor writes, and
`USING ivfflat`, whose centres are trained in transactions of their own.

#### SPLIT AT and UNSPLIT

`ALTER TABLE t SPLIT AT VALUES (k, ...), ...` puts a range boundary at
each primary-key tuple (a prefix of the key is allowed) and returns the
boundaries as key paths. It is for pre-splitting: carving a table that is
about to take a bulk load or a benchmark into ranges up front, instead of
waiting for size- and load-based splits to catch up.

- **The boundaries stay.** Left to itself the cluster merges adjacent
  ranges that are both small, and the halves of a pre-split are empty —
  so a boundary made by `SPLIT AT` is marked as made on purpose (it is
  *sticky*) and housekeeping leaves it alone, however small the ranges
  on either side. Splits the cluster makes for itself, by size or load,
  are not marked and merge back when the data shrinks, as before.
- **`WITH EXPIRATION e`** holds the boundaries only until `e`: a
  timestamp, or an interval counted from now (`INTERVAL '1 day'`). After
  that they are ordinary boundaries again. The expiration must be in the
  future; without the clause a boundary is held until it is unsplit.
- **`UNSPLIT AT VALUES (k, ...), ...`** is the way back: it stops holding
  the boundaries named, and `UNSPLIT ALL` every manual boundary in the
  table. Neither merges anything itself — the ranges fold together on
  housekeeping's next passes if they are small enough, and stay split if
  they have since filled up. `UNSPLIT AT` fails on a key that is not a
  range boundary and does nothing to one that is not sticky; both return
  the boundaries they released.
- **Idempotent.** Splitting at a boundary that exists is not an error: it
  marks the boundary sticky (or changes its expiration to the one given).
  A load script can run its `SPLIT AT` every time.
- **Concurrent with everything.** If the range changes while the split
  is on its way — another split lands first, housekeeping is merging the
  range — the statement waits or re-reads the range and carries on; it
  does not fail because someone else got there first.
- A sharded timeseries table is carved by shard (`SET (shards = N)`), not
  by key, and refuses both statements. The splits a timeseries table gets
  at `CREATE` are the cluster's own and are not sticky.
- `DROP TABLE` and `TRUNCATE` release the table's manual boundaries:
  its old key space is empty for good (a truncated table continues in a
  fresh one), so the emptied ranges are left to merge away. Split again
  after a `TRUNCATE` to pre-split the reload. The release is immediate
  and is not undone if the statement's transaction rolls back.
- Sticky boundaries, `WITH EXPIRATION` and `UNSPLIT` need cluster version
  v20. On a cluster that has not finalized it, `SPLIT AT` splits as it
  always did — the boundary is not held — and the other two are refused
  with `0A000`.

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
ALTER TABLE orders ADD CONSTRAINT IF NOT EXISTS qty_small CHECK (qty < 100); -- datax: a no-op (NOTICE) if the name exists
ALTER TABLE orders ADD FOREIGN KEY (cust) REFERENCES customers;
ALTER TABLE orders DROP CONSTRAINT [IF EXISTS] qty_small;
ALTER TABLE orders RENAME CONSTRAINT qty_small TO qty_bounded;
ALTER TABLE orders ALTER COLUMN region SET NOT NULL;                      -- sweeps existing rows first
ALTER TABLE orders ALTER COLUMN region DROP NOT NULL;
DROP TABLE customers CASCADE;                    -- also drops the foreign keys that reference it
SET foreign_key_cascade_limit = 50000;           -- per statement; default 10000
```

`ADD CONSTRAINT IF NOT EXISTS name` is a datax extension (PostgreSQL has
none): when the table has a constraint called `name` it does nothing but
send a NOTICE, without comparing the definitions — a constraint of that
name that says something else stays as it is. The name of an index made
by `CREATE UNIQUE INDEX` is not a constraint's and is still refused.

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
block, and duplicates fail it with nothing left behind. As in
PostgreSQL, NULLs are distinct: a row with a NULL in any of the unique
columns conflicts with no other row, so a unique column takes any
number of NULLs (and `ON CONFLICT (col)` with a NULL inserts). `UNIQUE
NULLS NOT DISTINCT` (PostgreSQL 15) makes NULLs equal for the
constraint: on a column, as a table constraint `UNIQUE NULLS NOT
DISTINCT (a, b)`, or on an index `CREATE UNIQUE INDEX ... (cols) NULLS
NOT DISTINCT`. Both need cluster version v22; before it a unique index
refuses NULLs (`23502`).

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

### Row-level security

```sql
ALTER TABLE docs ENABLE ROW LEVEL SECURITY;      -- from now on: only what a policy allows
CREATE POLICY tenant_isolation ON docs
    USING (tenant_id = current_setting('app.tenant')::uuid);
CREATE POLICY own_rows ON notes FOR UPDATE TO editors
    USING (author = current_user) WITH CHECK (author = current_user);
CREATE POLICY not_archived ON docs AS RESTRICTIVE FOR SELECT USING (NOT archived);
ALTER POLICY own_rows ON notes TO editors, admins USING (author = current_user OR shared);
ALTER POLICY own_rows ON notes RENAME TO authors;
DROP POLICY [IF EXISTS] not_archived ON docs;
ALTER TABLE docs FORCE ROW LEVEL SECURITY;       -- the owner and admins are filtered too
ALTER TABLE docs NO FORCE ROW LEVEL SECURITY;
ALTER TABLE docs DISABLE ROW LEVEL SECURITY;     -- policies kept, not applied
ALTER ROLE auditor BYPASSRLS;                    -- admins only: exempt from every policy
```

With row security enabled, a role reads and changes only the rows the
table's policies allow it, on top of the table privileges it still
needs. The rules are PostgreSQL's:

- **Default deny.** Enabled with no policy that applies to the role, it
  sees no rows and can write none.
- **`USING`** filters the rows a statement reads or targets; **`WITH
  CHECK`** must be true for every row it writes (when omitted, `USING`
  stands in). A row that fails the check is `42501`: `new row violates
  row-level security policy for table "docs"`. An expression that is
  NULL counts as false in both.
- **`FOR`** names the statement kind a policy governs (`ALL` by
  default). `SELECT ... FOR UPDATE` needs the `UPDATE` policies as well
  as the `SELECT` ones; an `UPDATE` or `DELETE` with a `WHERE` clause or
  `RETURNING` needs the `SELECT` policies as well as its own.
- **Permissive** policies (the default) are OR-ed together;
  **restrictive** ones are AND-ed on top. Restrictive policies alone
  admit nothing.
- **`TO`** limits a policy to roles — held directly or through
  membership; without it, or with `PUBLIC`, it applies to everyone.
- **Exempt**: the table's owner and members of `admin`, unless the table
  is `FORCE`d; `root` and roles with `BYPASSRLS`, always.

A policy expression may use the row's columns (unqualified), constants,
the ordinary functions and operators, and `current_user`,
`session_user`, `current_setting(...)`, `current_database()` and
`now()`. Subqueries are not supported in a policy.

The policy is applied like a `WHERE` condition written first, so it
uses indexes: with the tenant column leading the primary key, a
tenant's queries scan that tenant's rows only. `EXPLAIN` shows it:

```
> EXPLAIN SELECT * FROM docs WHERE id = 7;
point lookup on primary key; row-level security policy applied
> EXPLAIN SELECT * FROM docs;
range scan of primary key (tenant_id = 11111111-1111-1111-1111-111111111111); row-level security policy applied
```

It covers every statement kind — joins and subqueries (each table
reference is filtered), aggregates, `INSERT ... SELECT`, `INSERT ... ON
CONFLICT` (the proposed row and, for `DO UPDATE`, the existing one),
`UPSERT`, `COPY FROM`, `RETURNING`, locking reads with `SKIP LOCKED` or
`NOWAIT`, prepared statements, and `AS OF SYSTEM TIME` reads, which are
filtered by the policies in force now rather than at the read's
timestamp. `TRUNCATE` is refused to a role the policies apply to.
Foreign-key checks and cascades look at the whole table regardless, as
in PostgreSQL. The [security guide](security.md#row-level-security) has
a multi-tenant recipe and the limits of what this protects.

Policy DDL takes the table's owner (or an admin), and takes effect on
every node by the time the statement returns. `SHOW CREATE TABLE`,
`pg_policies`, `pg_policy`, `pg_class.relrowsecurity` /
`relforcerowsecurity`, `pg_tables.rowsecurity` and
`pg_roles.rolbypassrls` describe it. A column a policy reads cannot be
dropped (`2BP01`); renaming it updates the policy. A role a policy
names cannot be dropped until the policy stops naming it.

### Sealed tables

A sealed table's row values are encrypted under a key of the table's
own, so that dropping the table makes its data unreadable everywhere —
on every replica at once, and in backups once those taken before have
expired:

```sql
CREATE TABLE notes (id UUID PRIMARY KEY, body TEXT) WITH (encryption_key = 'auto');
-- NOTICE:  table "notes" is sealed: ... not sealed ...: id (primary key)
```

**Primary-key and indexed columns are not sealed**: they stay readable to
anyone who can read the database's storage. `CREATE TABLE` and `CREATE
INDEX` say which columns those are. Queries work as on any table. A
statement that reads a row whose key has been destroyed or disabled fails
with `XXK01`. It needs the cluster key (`datax debug cluster-key init`,
see the operations guide) and is refused for time-series tables. How to
shape a schema so that erasure means something:
[encryption keys for applications](../encryption-keys-for-applications.md).

A table that already holds rows is sealed with `ALTER TABLE`, and a
sealed table's key is rotated the same way; in both cases new writes use
the new key at once and a background job rewrites the existing rows
(`datax_encryption_jobs` shows its progress):

```sql
ALTER TABLE notes SET (encryption_key = 'auto');   -- reads take plain and sealed rows until the job is done
ALTER TABLE notes PURGE;                           -- after the job: drops the old plaintext versions now
ALTER TABLE notes ROTATE ENCRYPTION KEY;           -- the old key version is destroyed when the job is done
SELECT * FROM datax_encryption_jobs;
```

Until garbage collection or a `PURGE`, the old plaintext versions of the
rows stay in the table's history. A sealed table cannot be unsealed
(copy it into a plain table instead).

A table can instead be sealed **per tenant**: each value of its leading
primary-key column gets a key of its own on its first write, and one
tenant's data can be erased without touching the others:

```sql
CREATE TABLE items (tenant_id UUID, id UUID, title TEXT,
  PRIMARY KEY (tenant_id, id)) WITH (encryption_key_by = 'tenant_id');

-- erase one tenant: delete its rows, purge, then destroy its key
DELETE FROM items WHERE tenant_id = $1;
ALTER TABLE items PURGE;
SELECT datax_destroy_tenant_key('items', $1);  -- returns once no node uses the key
```

`datax_destroy_tenant_key` needs ownership of the table and runs outside
a transaction block. A row of the tenant left behind fails with `XXK01`
afterwards, and `DELETE` cannot remove it, so delete first; should rows
be left behind, `SELECT datax_remove_tenant_rows('items', $1)` removes
them and their index entries without the key. The tenant's next write
gets a fresh key. One tenant's key is rotated with
`datax_rotate_tenant_key('items', $1)`.

## Reading

```sql
[WITH [RECURSIVE] name [(cols)] AS (query | INSERT/UPDATE/DELETE ... RETURNING ...), ...]
SELECT [DISTINCT [ON (expr, ...)]] * | expr [[AS] alias] | func(...) OVER ([PARTITION BY exprs] [ORDER BY terms] [frame]), ...
  FROM t [AS a] | (query) AS d
  [JOIN t2 | (query) AS d2 ON b.x = a.y [AND ...] | USING (cols)]   -- INNER, LEFT | RIGHT | FULL [OUTER],
                                                                    -- CROSS, NATURAL, or "t, t2"; up to 8 tables
  [WHERE conjunct AND conjunct AND ...]
  [GROUP BY expr | position | ROLLUP (...) | CUBE (...) | GROUPING SETS (...) | (), ...] [HAVING ...]
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
  and `oid::regclass` on a column giving the table's name; a cast to a
  type that does not exist is `42704` —, `E'...'`
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
  trigonometric functions, ...), date and time (`now()`, the SQL
  keywords `current_timestamp`, `current_date`, `current_time`,
  `localtimestamp` and `localtime` — no parentheses, an optional
  precision as in `current_timestamp(3)`, the statement's start time
  wherever an expression goes, `RETURNING` included — `date_trunc`, `date_part` /
  `extract(field FROM x)`, `to_char`, `to_timestamp`, `to_date`,
  `make_date`, `make_timestamp`, `make_time`, `make_interval`, `age`,
  `extract` over intervals and times, `justify_hours`, `justify_days`,
  `justify_interval`), JSON
  (`jsonb_build_object`, `jsonb_build_array`, `to_jsonb`,
  `jsonb_typeof`, `jsonb_array_length`, `jsonb_extract_path[_text]`,
  `jsonb_set`, `jsonb_strip_nulls`, `jsonb_pretty`, ...), and the session and catalog functions tools call (`version()`,
  `current_user`, `current_schema()`, `current_setting(name)`, `set_config`,
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
  `@>` / `<@`, key existence `?`, `?|`, `?&`, and concatenation `||`
  (objects merge, the right side winning; arrays concatenate; a
  non-array is appended or prepended as an element) — everywhere an
  expression goes, including HAVING and grouped select lists.
- **Aggregates**: `COUNT(*)`, `COUNT(col)`, `SUM`, `AVG`, `MIN`, `MAX`,
  `string_agg(x, sep)`, `array_agg`, `bool_and` / `bool_or` / `every`,
  `stddev[_pop|_samp]`, `variance` / `var_pop` / `var_samp`,
  `percentile_cont(f) WITHIN GROUP (ORDER BY x)`, `percentile_disc`,
  `json[b]_agg`, `json[b]_object_agg` — over a column or an expression
  (`SUM(qty * price)`, `MAX(lower(name))`), with `DISTINCT` and `FILTER
  (WHERE ...)`, whole-table or per `GROUP BY` group, including over
  joins. The select list, `HAVING` and `ORDER BY` take any expression
  over aggregate calls and grouping columns: arithmetic (`SUM(a) /
  COUNT(*)`, `COUNT(*) * 100.0 / (SELECT COUNT(*) FROM t)`), function
  calls (`ROUND(AVG(x), 2)`, `COALESCE(MAX(x), 0)`), `CASE`, casts and
  comparisons, with or without `GROUP BY`. `HAVING` is a full boolean
  condition like `WHERE` (`HAVING COUNT(*) > 5 OR MIN(x) < 0`; a bare
  boolean aggregate works: `HAVING bool_and(ok)`). A column that is
  neither grouped nor inside an aggregate is refused with `42803`, as
  is an aggregate in `WHERE` or nested in another aggregate.
- **GROUP BY** takes expressions (`GROUP BY date_trunc('day', at)`,
  `GROUP BY a % 10`), positions (`GROUP BY 1` groups by select item 1's
  expression) and output names that are no input column (`SELECT a % 10
  AS bucket ... GROUP BY bucket`). A select item, `HAVING` or `ORDER BY`
  term may use a grouped expression whole or compute over it
  (`upper(kind)` grouped, `upper(kind) || '!'` shown). `ROLLUP (a, b)`,
  `CUBE (a, b)` and `GROUPING SETS ((a, b), (a), ())` group by each of
  their grouping sets in turn and return the union, the keys a set leaves
  out NULL in its rows; items combine as in PostgreSQL (`GROUP BY a,
  ROLLUP (b, c)` is the sets `(a, b, c)`, `(a, b)`, `(a)`), and `(a, b)`
  inside them is one element. `GROUPING(a, b)` tells a rolled-up NULL
  from a NULL key: a bit per argument, set when the row's grouping set
  leaves that key out (`HAVING GROUPING(a) = 1` keeps the subtotals). An
  empty grouping set gives its row over no rows too (`COUNT(*)` 0). At
  most 4096 grouping sets and 12 `CUBE` elements.
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
  `FROM a, b WHERE a.x = b.y` are cross products filtered by `WHERE`;
  a `WHERE` equality of a later side's column with an earlier side's
  column, or with a constant, becomes that side's lookup key as an `ON`
  equality would, so `EXPLAIN` shows an inner join with a point or
  index lookup rather than a scan per row (not for a `LEFT` side, or
  with a `RIGHT` / `FULL` join in the chain).
  Join select lists take columns, `*`, expressions, `->`/`->>` paths
  and subqueries; under `GROUP BY`, expressions over grouping columns
  and aggregates.
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
  the base or as a join member — and table functions (`unnest(array)`,
  `pg_available_extensions()`, `pg_partition_ancestors(rel)` and the
  set-returning builtins: `generate_series`, `jsonb_each[_text]`,
  `jsonb_array_elements[_text]`, `jsonb_object_keys`,
  `regexp_split_to_table`, `regexp_matches`; see
  [Set-returning functions](#set-returning-functions)), which
  join like tables too: `FROM unnest(array) [AS] s(x)`, `LEFT JOIN
  pg_available_extensions() ae(name, default_version, comment) ON ...`.
  **`LATERAL`**: a subquery written `LATERAL (SELECT ...)`, and any
  table function call (implicitly lateral, as in PostgreSQL), may read
  the columns of the `FROM` members to its left — `FROM users u,
  LATERAL (SELECT * FROM orders o WHERE o.user_id = u.id ORDER BY
  created DESC LIMIT 3) o` (top-N per group), `FROM t, unnest(t.tags)
  AS tag`. It joins by comma, `CROSS JOIN LATERAL`, `JOIN LATERAL ...
  ON cond` or `LEFT JOIN LATERAL ... ON true` (a row with no match is
  kept, the lateral columns NULL); a lateral `ON` may be any condition.
  It runs once per row of the members to its left (a correlated nested
  loop, memoized on the values it reads; `EXPLAIN` shows the level),
  and laterals chain and nest. A reference to a member further right is
  `42P01`; `RIGHT` and `FULL JOIN LATERAL` are refused (`42P10`). The
  lateral subquery is a plain select (over a table, a join or no
  table; not over a derived table or a set operation of its own) and
  reads the outer row anywhere but its `ORDER BY`, `GROUP BY` and
  aggregate arguments. Correlated subqueries — ones that reference
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
  dashboards stop early instead of scanning everything). A sort with a
  `LIMIT` over a single table's scan keeps only the limit's rows while
  it reads (`top-k heap` in `EXPLAIN`): every matching row is still
  read, but memory is the limit's rows, not the table's.
  `ORDER BY n` sorts by select item n itself — its column, aggregate or
  expression — never by what its output name would resolve to, so an
  unnamed expression (`ORDER BY 1` over `a || b`) and a column another
  item's alias also names sort as written; after a set operation it is
  the result's column n. A position at or after a `*` is accepted only
  when the `*` is the whole select list. `GROUP BY n` groups by select
  item n's expression; an aggregate item is refused (`42803`).
- **DISTINCT** removes duplicate result rows. **DISTINCT ON (exprs)**
  keeps, of each group of rows equal on the expressions (NULLs equal),
  the first in `ORDER BY` order — `SELECT DISTINCT ON (player) player,
  game, score FROM scores ORDER BY player, score DESC` is each player's
  best game. The `ORDER BY` must begin with the `DISTINCT ON`
  expressions (in any order; `42P10` otherwise) and may go on to order
  within the groups; without `ORDER BY` the row kept is whichever comes
  first. A position or an output name in the list stands for that select
  item. It works over single tables, joins, derived tables and grouped
  queries, before `LIMIT` / `OFFSET`; not with window functions
  (`0A000`) or `FOR UPDATE`.
- **LIMIT and OFFSET**: `LIMIT n | ALL`, `OFFSET n [ROWS]`, `FETCH
  FIRST | NEXT [n] ROWS ONLY`, on every query shape. `OFFSET` skips
  rows after they are fetched (the scan reads `LIMIT + OFFSET` rows when
  the limit is pushed down), so deep pagination costs the whole prefix:
  prefer keyset pagination (`WHERE id > $last ORDER BY id LIMIT n`) on
  large tables. `LIMIT 0` returns no rows.

Check the plan with `EXPLAIN SELECT ...` — one line naming the access
path — or run it with `EXPLAIN ANALYZE SELECT ...`, which executes the
statement (with the bound values, when it has parameters) and reports the plan line followed by every stage with its
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

**Streaming.** A `SELECT` that reads one table without sorting in
memory, aggregating, joining, `DISTINCT` or a window function streams
its rows: the gateway reads storage in pages of 512 rows as it writes
to the client, so the first row arrives before the last is read, a
full-table `SELECT` holds one page at a time however big the table,
and a driver's fetch size (JDBC's, or a suspended portal) bounds server
memory as well as wire traffic. Because rows go out as they are found,
an error on a later row — a division by zero, a cancellation,
`statement_timeout` — arrives after the rows already sent, as in
PostgreSQL; the client discards the partial result. Statements that do
materialize (the sort, aggregate, join and `DISTINCT` shapes, `WITH`
members, derived tables) are bounded by `statement_memory_limit` and
fail with `53200` past it; `datax_sql_streamed_rows_total` counts what
streamed.

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
UPDATE t SET a = a + 100 WHERE a < 10;              -- the primary key too: the rows move
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
key columns (an `UPDATE` can), and the same key twice in one statement is `21000`.

An `UPDATE` may change primary-key columns: the row moves to its new key
with its index entries, foreign keys referencing it apply their `ON
UPDATE` action, and a sealed table seals it for its new key (and tenant).
Unique keys — the primary key and unique indexes — are checked for the
statement as a whole, so `UPDATE t SET id = id + 1` and swaps work
whatever order the rows are visited in; PostgreSQL checks a
non-deferrable key row by row and refuses the first of these. When a
foreign key references its own table with `ON UPDATE CASCADE` or `SET
NULL`, a statement that changes both a referenced row and a row the
cascade then rewrites is refused (`0A000`): change them in separate
statements. The
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

## Large objects

A column value — `BYTEA`, `TEXT`, `JSONB` — is written, replicated and
read in one piece, and a statement holds it in memory several times
over. That is the right shape up to a few megabytes. Past that it costs
memory out of proportion (a 32 MiB value takes over a gigabyte across
the cluster to insert), and a statement that writes more than 60 MiB to
one range in one piece is refused with `54000`: it could not be
replicated (see [Limits](#large-object-limits)). Files, recordings and other
things of tens of megabytes to many gigabytes go in a **large object**
instead: a value stored in pages and written and read as a stream, so
that no node ever holds more than a little of it.

The interface is PostgreSQL's large objects, which drivers already
speak: the functions are callable from SQL
([functions reference](functions.md#large-objects)), which is how pgx's
`tx.LargeObjects()` uses them, and through the protocol's fast-path
function call, which is how libpq's `lo_*` API does — and with it
psql's `\lo_import`, `\lo_export`, `\lo_list` and `\lo_unlink`.

```sql
BEGIN;
SELECT lo_create(0);                  -- → 3141592653: a new, empty object
SELECT lo_open(3141592653, 131072);   -- → 0: a descriptor, opened for writing (INV_WRITE)
SELECT lowrite(0, $1);                -- any number of times, any sizes
SELECT lo_close(0);
INSERT INTO documents (id, body) VALUES (7, 3141592653);   -- keep the id where you will find it
COMMIT;                               -- the object exists from here

BEGIN;
SELECT lo_open(3141592653, 262144);   -- INV_READ
SELECT loread(0, 1048576);            -- until it returns fewer bytes than asked for
COMMIT;

SELECT lo_unlink(3141592653);         -- remove it
```

```go
tx, _ := conn.Begin(ctx)
los := tx.LargeObjects()
oid, _ := los.Create(ctx, 0)
obj, _ := los.Open(ctx, oid, pgx.LargeObjectModeWrite)
io.CopyBuffer(obj, file, make([]byte, 1<<20))   // streams: memory stays flat
tx.Commit(ctx)
```

What to know:

- **A large upload is a new object written in one transaction block.**
  `lo_create` inside `BEGIN` makes an object nobody else can see yet;
  what is written to it before `COMMIT` is stored as it arrives, outside
  the transaction, so the upload can be of any size and holds no lock
  while it runs. `COMMIT` makes the whole object appear at once.
  `ROLLBACK`, an error, or a lost connection leaves no object, and the
  pages already stored are removed in the background.
- **Let the upload's transaction be about the upload.** At `COMMIT` the
  transaction is moved forward to the present, past the pages it
  stored, and like any transaction that has to move it fails with
  `40001` if something it read has changed meanwhile. Rows it read
  earlier in the block count, and so does the catalog: libpq looks the
  functions up in `pg_proc` inside the transaction, once per
  connection, so an upload on a fresh libpq connection (every psql
  `\lo_import` is one) fails at its commit if a table or role was
  created or altered while it ran. Repeat it.
- **Changes to an object that already exists are ordinary transactional
  writes** — isolated, rolled back with their transaction, visible at
  its commit — and for that reason bounded: one transaction may write at
  most 64 MiB into existing objects (`54000` beyond). The same holds for
  the one-statement forms outside a block (`lo_from_bytea`, `lo_put`).
  To replace a large object, write a new one and unlink the old in the
  same transaction.
- **Read in pieces.** `loread` returns up to the length asked for; what
  one call returns counts against `statement_memory_limit` (twice: it is
  assembled from pages), so with the default 64 MiB limit ask for no
  more than about 30 MiB a call — 1 MiB is a good size. `lo_get(oid)`
  returns a whole object as one value and is for small ones.
- **Descriptors live until the transaction ends**, as in PostgreSQL;
  outside a transaction block a descriptor is gone when its statement
  returns, so `lo_open` is only useful inside `BEGIN`.
- **Nothing ties an object to the rows that mention its id**, as in
  PostgreSQL: deleting the row does not unlink the object. Unlink it in
  the same transaction that deletes or repoints the row.
- **The functions run once per statement, on constant arguments**
  (literals and parameters), like `nextval`: `SELECT lo_unlink(body)
  FROM documents` is refused. Select the ids, then unlink each.
- An object belongs to the role that created it; the owner and admins
  may read, write and unlink it. There are no per-object grants.
- `lo_lseek64` / `lo_tell64` position a descriptor; writing past the end
  leaves a hole that reads as zeros and takes no space;
  `lo_truncate64` cuts or extends.
- An upload that goes quiet — no large-object call in its transaction —
  for more than 10 minutes is taken for abandoned: its pages are
  removed, and if the client does come back its next call or its
  `COMMIT` fails with `55000`. Start the upload again.
- `ROLLBACK TO SAVEPOINT` closes the transaction's descriptors and
  undoes its changes to existing objects. It cannot undo what was
  streamed into a new object, so a transaction that created one and then
  rolls back to a savepoint can no longer commit (`25P02`); roll it back.

### Large object limits

| | |
|---|---|
| Object size | up to 1 TiB (`54000` beyond). Tested to 1 GiB |
| Page size | 256 KiB, fixed. A 1 GiB object is 4,096 rows of `datax_largeobject` |
| Written by one transaction into existing objects | 64 MiB |
| Returned by one `loread` or `lo_get` | half of `statement_memory_limit` (about 30 MiB by default) |
| Upload idle time | 10 minutes |
| A single column value, for comparison | a statement may write at most 60 MiB to one range in one piece — a row with its index entries, or the rows of one `INSERT` that land in one range — and is refused with `54000` beyond; keep row values to a few MiB |

Throughput on a three-node cluster on one machine is about 20 MiB/s
written (each page is a replicated write of its own) and over 100 MiB/s
read; the process's memory grows by tens of megabytes whatever the
object's size.

Large objects are stored in three [reserved tables](#reserved-tables),
so full backups and restores carry them, encryption at rest covers
them, and their pages spread over ranges like any table's rows. Cluster
version v21.

## Vectors

```sql
CREATE EXTENSION IF NOT EXISTS vector;     -- accepted and does nothing: the type is built in
CREATE TABLE items (id INT8 PRIMARY KEY, tenant INT8, embedding vector(3));
INSERT INTO items VALUES (1, 7, '[1, 2, 3]'), (2, 7, ARRAY[4, 6, 3]);

SELECT id FROM items ORDER BY embedding <-> '[3, 1, 2]' LIMIT 5;        -- the 5 nearest, Euclidean
SELECT id FROM items ORDER BY embedding <=> $1 LIMIT 5;                 -- cosine distance
SELECT id FROM items ORDER BY embedding <#> $1 LIMIT 5;                 -- largest inner product
SELECT id, embedding <-> $1 AS d FROM items WHERE tenant = 7 ORDER BY d LIMIT 5;
```

This follows [pgvector](https://github.com/pgvector/pgvector), so its
clients and ORM integrations work; what differs is listed under
[PostgreSQL differences](postgres-differences.md).

**The type.** `vector(n)` holds exactly `n` elements, each a float4
(about seven significant digits); `vector` without `(n)` takes any
dimension, but only a column with a declared dimension can be indexed.
A value is written `'[1, 2.5, -3e2]'`, or converted from a numeric
array. Up to 16,000 dimensions; every element must be a finite number
(`NaN` and `Infinity` are refused, `22P02`), and a value of the wrong
dimension for its column is refused (`22000`). A 1,536-dimension vector
takes about 6 KiB in its row. Vectors compare for equality and order
element by element (`=`, `<>`, `<`, `ORDER BY v`, `GROUP BY v`,
`DISTINCT`), but a vector cannot be a primary key or part of an ordinary
or unique index. `ALTER TABLE ... ALTER COLUMN ... TYPE vector(n)`
converts a text column whose values are vectors.

**Operators and functions.**

| | |
|---|---|
| `a <-> b` | Euclidean (L2) distance; `l2_distance(a, b)` |
| `a <=> b` | cosine distance, `1 − cos(a, b)`, from 0 (same direction) to 2 (opposite); `NaN` when either is the zero vector, and `NaN` sorts after every number; `cosine_distance(a, b)` |
| `a <#> b` | **negative** inner product, so that ascending order is nearest first; `inner_product(a, b)` is the product itself |
| `a + b`, `a - b` | element-wise |
| `vector_dims(a)`, `vector_norm(a)`, `l2_normalize(a)`, `l1_distance(a, b)` | [functions reference](functions.md#vectors) |

Either operand may be a column, a `'[...]'` literal, a parameter or a
numeric array. The two operands must have the same dimension (`22000`).
A distance with a NULL operand is NULL, and sorts last. The distance
operators bind more loosely than `+` and `-` and more tightly than
comparisons, as in PostgreSQL: `embedding <-> $1 < 0.5` compares the
distance.

**Casts.** `'[1,2,3]'::vector`, `::vector(3)` (checks the dimension),
`ARRAY[1,2,3]::vector`, `v::text`, `v::float8[]`.

**Clients.** In text, a vector is its `'[...]'` form: send a string,
read a string. For binary, a client registers the type by OID, and
looks the OID up by name first, because in PostgreSQL an extension's
type has no fixed one: `SELECT to_regtype('vector')::oid` and `SELECT
oid FROM pg_type WHERE typname = 'vector'` both answer (16000, the same
on every node and in every database). With pgx, `pgvector-go`'s
`pgxvec.RegisterTypes(ctx, conn)` does exactly that, after which
`pgvector.NewVector(...)` values are sent and scanned in pgvector's
binary format. A parameter that is an operand of a distance operator or
is assigned to a vector column is described as a vector.

**Exact search.** Without an index, `ORDER BY <distance> LIMIT k`
returns exactly the `k` nearest rows. The table (or the part of it the
`WHERE` clause selects through a key or an index) is scanned once and
only the best `k` rows seen so far are kept, so memory is `k` rows
however large the table; `EXPLAIN` says so:

```
full table scan; top-k heap: the first 5 rows in order kept while scanning, no full sort
```

(The same holds for any `ORDER BY ... LIMIT` the access path cannot
deliver in order, not only distances.) Time is linear in the rows
scanned — about 3 µs a row for 64 dimensions on one core — which is
fine for tens of thousands of rows per query and is what the index is
for beyond that. Ties are returned in primary-key order.

**Approximate search: the ivfflat index.**

```sql
CREATE INDEX items_embedding ON items USING ivfflat (embedding vector_l2_ops) WITH (lists = 100);
--                                                   vector_cosine_ops for <=>, vector_ip_ops for <#>
SET ivfflat.probes = 10;      -- per session; SET LOCAL per transaction; RESET for the default
```

The index groups the table's vectors into `lists` clusters when it is
built. A query reads only the `ivfflat.probes` clusters whose centres
are nearest the query vector, ranks the rows in them by their true
distance, and returns the nearest `k`. `EXPLAIN` shows it:

```
vector scan of index "items_embedding" (ivfflat, <->: the nearest 10 of 100 lists probed, approximate) + primary key join, rows rechecked; top-k: ...
```

- **Results are approximate.** Every row returned is a real row with
  its true distance, in true distance order — but a row nearer than
  some of those returned may be missing, if it sits in a cluster that
  was not read. *Recall* is the share of the true `k` nearest that come
  back. It rises with `ivfflat.probes` and reaches 1 (exact) when
  `probes` equals `lists`; the cost rises with it, in proportion.
- **`ivfflat.probes`** defaults to the square root of the index's
  `lists` (`auto`: 10 for 100 lists). Measured on 50,000 clustered
  64-dimension vectors, `lists = 200`, `k = 10`, one node:

  | `ivfflat.probes` | recall@10 | median latency |
  |---|---|---|
  | 1 | 0.85 | 0.9 ms |
  | 5 | 0.96 | 3 ms |
  | 15 (`auto`) | 0.99 | 10 ms |
  | 40 | 1.00 | 29 ms |
  | no index (exact) | 1 | 164 ms |

  Your data decides these numbers: vectors with little cluster
  structure need more probes for the same recall. Measure on your own
  with `datax bench vector-knn`, or by comparing a query's result with
  the same query under `SET vector_index_scan = off`, which answers it
  exactly.
- **`lists`** (1 to 32,768, default 100): about `rows / 1000` is the
  usual starting point, and at least 10 × `lists` rows in the table when
  the index is built. More lists make each probe cheaper and need more
  probes for the same recall.
- **Build the index after loading data.** The clusters are found from
  a sample of the rows present when `CREATE INDEX` runs, and never
  move afterwards. An index built on an empty table has one cluster
  holding everything: correct, and no faster than a scan. The same
  happens gradually when the data changes character after the build
  (or after `TRUNCATE`, which keeps the clusters): the index stays
  correct, recall at a given `probes` falls. Rebuild it — create a
  second index, then drop the first; there is no `REINDEX`.
- **One operator class per index, and the query must use its operator**:
  an index built with `vector_cosine_ops` serves `ORDER BY v <=> ...`
  only. Create one per distance you query by.
- **The query shape it serves**: `ORDER BY column <op> value LIMIT k`
  (optionally `OFFSET`), ascending, as the only sort key, the value a
  literal, a parameter or a scalar subquery. Without a `LIMIT`, with a
  second sort key, in a join, a grouped query or with `DISTINCT` or `FOR
  UPDATE`, the query runs exactly, without the index.
- **Filters.** A `WHERE` clause is applied to the candidates nearest
  first until `k` rows pass, so a filter never costs answers within the
  clusters read. A selective filter may still leave fewer than `k` rows
  in them: raise `ivfflat.probes`. When the filter can use the primary
  key or an ordinary index (`WHERE tenant = 7` with an index on
  `tenant`), the planner uses that instead and the answer is exact over
  the rows it selects.
- **NULL vectors** are not in the index: an indexed query does not
  return them, while the exact scan returns them last.
- **Writes** cost one extra KV write per index, holding a copy of the
  vector, in the row's own transaction: an indexed table stores each
  vector twice per index. The index is built online, under concurrent
  writes, like any other.
- **Limits**: one column, at most 2,000 dimensions, not `UNIQUE`.
  `USING hnsw` is refused (`0A000`); use `ivfflat`.
- `vector_index_scan = off` makes every nearest-neighbour query exact
  for the session, whatever indexes exist.
- Needs cluster version v21, as does the type itself.

## Transactions and retries

```sql
BEGIN;                                   -- or START TRANSACTION; BEGIN READ ONLY etc. take SET TRANSACTION's modes
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

### Not waiting for a lock: SKIP LOCKED and NOWAIT

`FOR UPDATE` waits for a row another transaction holds. Two suffixes
say not to:

```sql
SELECT id FROM jobs ORDER BY id LIMIT 1 FOR UPDATE SKIP LOCKED;  -- the first job nobody else has
SELECT * FROM accounts WHERE id = 1 FOR UPDATE NOWAIT;           -- 55P03 if someone holds it
```

**`SKIP LOCKED`** leaves out every row it cannot lock at once and locks
and returns the rest. `LIMIT n` counts the rows returned: the scan goes
on past the rows it skips until it has locked `n` or run out, and only
the rows returned are locked. This is the job-queue statement — each
worker takes the first job no other worker has, and none of them waits:

```sql
BEGIN;
SELECT id, payload FROM jobs ORDER BY id LIMIT 1 FOR UPDATE SKIP LOCKED;
-- ... do the work ...
DELETE FROM jobs WHERE id = $1;
COMMIT;
```

What counts as "cannot lock":

- a row another transaction has locked or written and not yet finished
  with (`FOR UPDATE`, `UPDATE`, `DELETE`, or a row it is inserting);
- a row another transaction changed and **committed** after this
  transaction's snapshot was taken (or so close to it that the clocks
  cannot order the two). Plain `FOR UPDATE` treats that as a conflict
  and may fail with `40001`; `SKIP LOCKED` passes the row over. In a
  queue it is a job another worker took and finished a moment ago.

The result is therefore not a consistent picture of the table — that is
the point of the clause, as in PostgreSQL — and rows that were skipped
are not reads of the transaction: whatever happens to them later cannot
fail it. The transaction still reads at its snapshot, so a block that
began long ago does not see rows inserted since; take the snapshot late
(run the `SELECT` first in its block, or on its own) when draining a
queue. A lock whose transaction died without finishing does not hide
its row for good: skipping readers notice it once it has expired
(about five seconds) and release it.

Keep the retry loop: skipped rows cost no `40001`, but the rows the
transaction did read and write are under the same rules as anywhere
else. A queue drained this way sees `40001` rarely, not never. Give the
query an order the table or an index delivers (`ORDER BY` the primary
key, or an indexed column): with an in-memory sort every matching free
row is read before the first is chosen, and each of those is a read
another worker's `DELETE` can invalidate.

**`NOWAIT`** fails the statement with SQLSTATE **`55P03`**
(`could not obtain lock on row in relation "..."`) when it meets a row
another transaction holds, and locks nothing more. It is an ordinary
statement error — the block is poisoned until `ROLLBACK` or
`ROLLBACK TO SAVEPOINT` — and not a retry signal like `40001`: the row
is busy, and trying again at once will most likely find it busy still.
Unlike in PostgreSQL the refusal can come from a row the statement had
to read on the way and would then have filtered out (a full scan with a
`WHERE` no index serves reads every row); a lookup by key or by index
meets only the rows it selects.

Neither suffix lets a transaction take a lock away from its holder, and
both are refused wherever `FOR UPDATE` is (joins, aggregates,
`DISTINCT`, subqueries, views, `AS OF SYSTEM TIME`). `FOR SHARE` and
`FOR UPDATE OF table` are not supported.

## Notifications: LISTEN and NOTIFY

```sql
LISTEN jobs;                          -- this session hears channel "jobs"
NOTIFY jobs, 'order 42';              -- every listener gets (channel, payload, sender's pid)
SELECT pg_notify('jobs', $1);         -- the same, with a computed or bound payload
UNLISTEN jobs;   UNLISTEN *;
```

A session that has run `LISTEN` is sent a notification for every
`NOTIFY` on that channel that commits, whichever node of the cluster the
notifier and the listener are connected to. Drivers surface it the way
they do against PostgreSQL — pgx's `conn.WaitForNotification(ctx)`,
lib/pq's `Listener`, psycopg's `conn.notifies()` — and no polling
statement is needed: the server pushes to a connection that is sitting
idle.

```go
conn.Exec(ctx, "LISTEN jobs")
for {
    n, err := conn.WaitForNotification(ctx)   // blocks; sends nothing
    if err != nil {
        // reconnect, LISTEN again, then re-read the table: see below
    }
    handle(n.Channel, n.Payload)
}
```

The rules are PostgreSQL's:

- **Transactional.** A `NOTIFY` inside a transaction is delivered if and
  when the transaction commits, never before and never if it rolls back
  (`ROLLBACK TO SAVEPOINT` discards what was notified after the
  savepoint). Identical notifications — same channel, same payload — in
  one transaction are delivered once. `LISTEN` and `UNLISTEN` take
  effect at commit too.
- **Between statements.** A session receives notifications when it is
  idle or as a command completes, never in the middle of a result; a
  session inside a transaction block receives them when the block ends.
  A session hears its own notifications if it listens on the channel.
- **Channels** are identifiers (folded to lower case unless quoted, at
  most 63 bytes) and belong to a database: a listener hears the
  notifications of the database it was connected to when it listened.
  There are no privileges on channels: any session may listen and
  notify, so do not put secrets in a payload.
- **Payloads** are text shorter than 8000 bytes (`22023` beyond that).
  Send a key and let the listener read the row.
- `pg_notify(channel, payload)` runs once per statement, not once per
  row, and its arguments cannot name columns.

What is guaranteed, and what is not:

- A notification goes to the sessions that are listening when it
  commits. Nothing is stored: a client that is not connected at that
  moment never sees it, on datax as on PostgreSQL. Treat a notification
  as "look now", and on every (re)connect run `LISTEN` and then read the
  state the notifications describe.
- **A session that stays connected receives every notification once**,
  from every node, or it is disconnected — it is not left connected
  having missed one. Two things end a listening session with a FATAL
  error: it did not read its notifications (or sat in a transaction)
  while 16 MiB of them piled up (`54000`), or another node could not
  reach this one for so long that it had to drop notifications meant
  for it — it keeps up to 64 MiB per unreachable node, for up to five minutes,
  and delivers them, in order, when the node is back (`57P01`).
- **A node restart or a partition** therefore costs notifications only
  to sessions that were disconnected: the sessions on the restarted
  node (their connections ended with it), and the listeners of a node
  that was cut off for long enough. During a shorter partition
  notifications are late, not lost. A node that is drained before it
  stops hands over what it still owes first.
- **The exception is a crash.** A node that dies in the instant between
  committing a transaction and handing its notifications to the other
  nodes takes them with it, and listeners elsewhere are not told. The
  same goes for a `COMMIT` whose outcome the client never learned. A
  worker that must not miss work should also poll at a long interval.
- **Order.** All listeners see one node's notifications in the order
  that node committed them, and one transaction's in the order it sent
  them. Notifications committed on different nodes arrive in no
  particular order relative to each other.
- A node running a release from before this feature cannot serve
  `LISTEN` (a syntax error) and is skipped by the others; during a
  rolling upgrade, connect listeners to upgraded nodes.

## Set-returning functions

`generate_series`, `jsonb_each`, `jsonb_each_text`, `jsonb_array_elements`,
`jsonb_array_elements_text`, `jsonb_object_keys` (and their `json_`
spellings), `regexp_split_to_table` and `regexp_matches` return rows. In
`FROM` they are tables, with an alias and column names; in a select list
without `FROM` each call expands into rows, several calls in lockstep
(the shorter padded with NULLs, as in PostgreSQL 10 and later):

```sql
SELECT * FROM generate_series(1, 10, 2) AS g(n);                      -- 1 3 5 7 9
SELECT d FROM generate_series('2024-01-01'::timestamptz, '2024-01-31', interval '1 day') AS d;
SELECT key, value FROM jsonb_each('{"a":1,"b":[2]}');                 -- (key text, value jsonb)
SELECT value FROM jsonb_array_elements_text('["x","y"]');
SELECT generate_series(1, 3) AS n, 'row' AS label;                    -- three rows
INSERT INTO t (id) SELECT n FROM generate_series(1, 1000) AS g(n);
```

Their arguments are constants or parameters: a call that reads another
`FROM` member's columns (`FROM t, jsonb_array_elements(t.doc)`, a
lateral reference) is refused (`0A000`), and so is one in the select
list of a query with `FROM` (`SELECT generate_series(1, n) FROM t`).
A two-column function (`jsonb_each`) goes in `FROM`. The rows count
against `statement_memory_limit` as they are made.

## Time zones

A `TIMESTAMPTZ` is an instant; the session's `TimeZone` decides how one
is read from text that names no offset and how its calendar fields are
seen. `AT TIME ZONE` converts between the two kinds of timestamp in a
named zone, whatever the session's:

```sql
SET TimeZone = 'America/New_York';
SELECT '2024-07-01 12:00'::timestamptz;                       -- 2024-07-01 12:00:00-04 (16:00 UTC)
SELECT now()::date, date_trunc('day', now());                 -- New York's date and midnight
SELECT ts AT TIME ZONE 'Europe/Paris' FROM events;            -- timestamptz → Paris wall clock (timestamp)
SELECT '2024-03-10 02:30'::timestamp AT TIME ZONE 'America/New_York';  -- timestamp → instant
SELECT timezone('UTC', ts), ts AT LOCAL FROM events;          -- the function form; the session's zone
```

A zone is an IANA name (any letter case), a POSIX offset — `'UTC+5'`
and `'+5'` are five hours WEST of Greenwich, as in PostgreSQL — or, for
`AT TIME ZONE` and timestamp input only, an abbreviation (`EST`, `PST`,
`CET`, ...) or an `INTERVAL '-08:00'`; an unknown one is `22023`. A wall
clock a DST change skips (`02:30` on a spring-forward night) reads with
the offset before the change, one it repeats with the offset after it,
as PostgreSQL resolves them.
A literal without an offset in a join's `ON` condition still reads in
UTC; filter on it in `WHERE`.

## Session settings

```sql
SET statement_timeout = '5s';                 -- 57014 when a statement runs longer
SET statement_memory_limit = '256MB';         -- 53200 when a statement holds more than this
SET lock_timeout = 200;                       -- ms: 55P03 instead of waiting on a live intent
SET idle_in_transaction_session_timeout = '1min'; -- 25P03: the idle block is rolled back, the connection ended
SET TIME ZONE 'America/New_York';             -- TIMESTAMPTZ read and rendered in the zone
SET application_name = 'billing';             -- shown by pg_stat_activity / SHOW SESSIONS
SET LOCAL statement_timeout = '30s';          -- inside a block: until COMMIT / ROLLBACK
SET TRANSACTION READ ONLY;                    -- this block refuses writes (25006); BEGIN READ ONLY does the same
SET default_transaction_read_only = on;       -- every transaction, until reset
RESET statement_timeout;  RESET ALL;  SET x = DEFAULT;
SHOW statement_timeout;  SHOW ALL;  SELECT * FROM pg_settings;   -- source 'session' where this session changed it (psql's \dconfig)
SET app.tenant = 'acme';                      -- a custom setting: any class.name
SELECT set_config('app.tenant', $1, false);   -- the same, with the value as a parameter
SELECT current_setting('app.tenant');         -- 42704 if never set; current_setting(name, true) gives NULL
```

`SET [SESSION | LOCAL] name {TO | =} value`, `SET TIME ZONE`, `SET NAMES`,
`SET [SESSION CHARACTERISTICS AS] TRANSACTION ...`, `RESET name`, `RESET
ALL` and `SHOW name` work over these variables — every one of them is
honored or reports its real value; an unknown variable is `42704`, an
invalid value `22023`:

| Variable | Values | Effect |
|---|---|---|
| `statement_timeout` | ms, or `'5s'`, `'1min'`, `'500ms'`; `0` = none | a statement past it is cancelled: `57014`, its transaction block failed |
| `statement_memory_limit` | bytes, or `'64MB'`, `'1GB'`, `'512kB'`; `0` = none; default `64MB` | what one statement may hold in memory on the gateway (rows it sorts, aggregates, joins or otherwise collects); past it the statement fails with `53200` instead of growing without bound. Streamed `SELECT`s are unaffected (see [Streaming](#reading)) |
| `lock_timeout` | as above | a wait on another transaction's live write intent past it fails with `55P03` (without it the wait lasts the conflict budget, then `40001`); to not wait at all, see [`FOR UPDATE NOWAIT` and `SKIP LOCKED`](#not-waiting-for-a-lock-skip-locked-and-nowait) |
| `idle_in_transaction_session_timeout` | as above | a connection idle inside a block past it is ended with `25P03`; the block rolls back and its intents are released — the fix for stranded transactions from crashed clients |
| `TimeZone` | an IANA name (any letter case), `UTC`, a POSIX offset (`'UTC+5'`, `'-03'`: positive is WEST of Greenwich, as in PostgreSQL); not an abbreviation | the zone of a `TIMESTAMPTZ` as text: input without an offset (literals, text parameters, `COPY`) reads in it, and text output (the wire, `::text`, `||`, `to_char`), `::date`, `::time`, `::timestamp`, `date_trunc`, `extract`, `current_date` and day and month arithmetic (`+ interval '1 day'` across a DST change) follow it; storage, comparison and the binary format stay UTC; `TIMESTAMP` (without time zone) is unaffected. See [Time zones](#time-zones) |
| `application_name` | any text; the startup parameter too | `pg_stat_activity`, `SHOW SESSIONS` and the dashboard's activity view show it |
| `search_path` | any list | accepted and reported; `public` is the only schema (`pg_catalog` and `information_schema` are always visible) |
| `transaction_read_only`, `default_transaction_read_only`, `SET TRANSACTION READ ONLY` / `READ WRITE` (also as `BEGIN` / `START TRANSACTION` modes) | `on` / `off` | a read-only transaction refuses `INSERT`, `UPDATE`, `DELETE`, `COPY`, DDL, `nextval()` and `setval()` with `25006` |
| `transaction_isolation`, `SET TRANSACTION ISOLATION LEVEL ...` (also as a `BEGIN` / `START TRANSACTION` mode) | any level | accepted (drivers set one on connect); every transaction is serializable and `SHOW` says so |
| `DateStyle`, `client_encoding` | `ISO[, order]`, `UTF8` | the supported values; anything else is `22023` |
| `foreign_key_cascade_limit` | a positive integer | the per-statement cascade cap |
| `inverted_index_scan` | `auto` (default), `on`, `off` | whether the planner may, must (when the query allows it) or must not read a GIN index |
| `default_text_search_config` | `english` (default), `simple`; `pg_catalog.` may qualify either | the configuration `to_tsvector`, `to_tsquery` and their relatives use when a call names none; reported schema-qualified (`pg_catalog.english`), as PostgreSQL does |
| `ivfflat.probes` | `auto` (default: the square root of the index's lists) or 1 to 32768 | how many clusters of an ivfflat index a nearest-neighbour query reads: more is higher recall and slower ([Vectors](#vectors)) |
| `vector_index_scan` | `on` (default), `off` | `off` answers every nearest-neighbour query exactly, by scanning, whatever vector index exists |
| `IntervalStyle` | `postgres` (default), `iso_8601`, `sql_standard`, `postgres_verbose` | the text format of an `INTERVAL` column on the wire (`P1Y2M3DT4H5M6S` in `iso_8601`, as Rails expects); `::text` casts, arrays and JSON keep the `postgres` format; reported to the client as a parameter status |
| `bytea_output` | `hex` (default), `escape` | the text format of a `BYTEA` column on the wire |
| `client_min_messages` | `debug5` ... `debug1`, `log`, `notice` (default), `warning`, `error` | at `warning` or `error`, `NOTICE`s are not sent |
| `row_security` | `on` (default), `off` | `off` refuses (`42501`) a query that a table's row-level security policies would filter, as `pg_dump` expects |
| `server_version_num`, `max_identifier_length` | `140000`, `63` | read only (`55P02`) |
| PostgreSQL's other common settings: `work_mem`, `synchronous_commit`, `jit`, `enable_*`, `lc_*`, `default_transaction_isolation`, ... | PostgreSQL's values, checked as PostgreSQL checks them | accepted, kept and reported, with no effect: the list is in [Differences from PostgreSQL](postgres-differences.md) |
| `role`, `SET [LOCAL] ROLE name \| NONE`, `RESET ROLE` | a role the session user belongs to (admins: any) | the current role: privilege checks and `current_user` follow it, `session_user` stays; `SHOW role` reports `none` without one (see the [security guide](security.md#roles-and-privileges)) |

**Custom settings.** A name with a prefix and a dot (`app.tenant`,
`myapp.user_id`) is a setting of your own: `SET`, `SET LOCAL`, `RESET`
and `SHOW` take it, it holds any text, and `current_setting('app.tenant')`
reads it — which is how a [row-level security](#row-level-security)
policy learns which tenant a connection is working for.
`set_config(name, value, is_local)` sets one from an expression or a
parameter (`is_local` true is `SET LOCAL`). A custom setting does not
exist until it is first set: reading it before is `42704`, or NULL with
`current_setting(name, true)`, as in PostgreSQL. Once set it is listed in
`pg_settings` and `SHOW ALL` (`source` `session`, `vartype` `string`, no
`boot_val`), so psql's `\dconfig` shows it. `RESET` leaves it set and
empty. It belongs to the session, so a pooled connection must set it again (or
`SET LOCAL` it per transaction) for each request.

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

All forms: bounded by the GC window on the old side (25h, or the
table's own `gc_ttl`; nothing from before a [purge](#gc-window-and-purge)); not allowed
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
`BEGIN`. The statistics feed the query planner; `SHOW STATS` (any role
holding `SELECT` on the table) shows what the planner sees, one row
per column.

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
  collected automatically. (Any other table expires rows with
  [`ttl`](#row-expiry-ttl) instead.)
- Queries are unchanged — `WHERE series = '...' AND at >= ...` fans out
  over the buckets (visible in `EXPLAIN`). Fan-out costs read latency:
  measured ~2× point-read p50 at `shards=8`, in exchange for ~linear
  insert scaling.
- Re-shard online: `ALTER TABLE metrics SET (shards = 16);` — writes
  never stop, and secondary indexes on the table are rebuilt and swapped
  along with the rows.

## Row expiry (TTL)

A table can delete its own rows when their time is up. Either a column
holds each row's expiry time, or rows live a fixed time counted from a
timestamp column:

```sql
-- each row says when it goes
CREATE TABLE quarantine (
  id         INT8 PRIMARY KEY,
  item       TEXT,
  expires_at TIMESTAMPTZ
) WITH (ttl_expiration = expires_at);

-- every row lives 7 days from created_at
CREATE TABLE review (
  id         INT8 PRIMARY KEY,
  created_at TIMESTAMPTZ DEFAULT now(),
  body       TEXT
) WITH (ttl_column = created_at, ttl = '7 days');

ALTER TABLE review SET (ttl = '30 days');           -- change the lifetime
ALTER TABLE orders SET (ttl_expiration = purge_at); -- add expiry to a table with data
ALTER TABLE review RESET (ttl);                     -- stop expiring
```

- The column is `TIMESTAMPTZ` or `TIMESTAMP`. A row whose value is NULL
  never expires. `ttl` takes the forms `retention` does (`'7 days'`,
  `12h`).
- **A lifetime needs a column.** `WITH (ttl = '7 days')` alone is
  refused: there is no hidden "last written" time to count from. Give
  the table a `created_at TIMESTAMPTZ DEFAULT now()` and name it — and
  note that this counts from the insert, not from the last update,
  unless your updates set the column.
- **Expired rows are deleted by a background job, not hidden.** A row
  past its time is still returned by queries until the job reaches it —
  normally within a pass, 30 seconds by default. A query that must never
  see one says so: `WHERE expires_at > now()`.
- The deletes are ordinary `DELETE`s in ordinary transactions, so
  everything a `DELETE` does happens: secondary index entries go with
  the row, `ON DELETE CASCADE` takes the referencing rows, and a
  referencing row under a restricting foreign key keeps the expired row
  in place — it is skipped, counted
  (`datax_row_ttl_rows_blocked_total`), and deleted on a later pass
  once nothing references it.
- The job finds expired rows through an index on the column. The option
  creates one, `<table>_ttl_idx`, unless an index (or the primary key)
  already leads with the column; on a table with rows it is built online
  first, so `ALTER TABLE ... SET` can take as long as a `CREATE INDEX`.
  `RESET (ttl)` leaves the index — drop it if nothing else uses it. If
  you drop the index while the expiry is set, the job still works, by
  scanning the table.
- The expiry column cannot be dropped or retyped while the expiry is
  set. Time-series tables expire by `retention` and refuse these
  options.
- A deleted row is deleted the way any row is: its old versions stay
  until garbage collection. Give the table a short `gc_ttl`, or `PURGE`
  it, if expired data has to leave the disk (next section).
- Not inside `BEGIN`; needs table ownership and cluster version v21.
  `SHOW CREATE TABLE` shows the option, and a backup carries it.

One job runs for the whole cluster. See the operations guide's
[Row expiry](operations.md#row-expiry) for its pace, metrics and where
to watch it.

## GC window and purge

Deleting or updating a row leaves the old version behind: it is what
`AS OF SYSTEM TIME`, follower reads and incremental backups read. Old
versions are garbage collected after the GC window — 25 hours unless the
node says otherwise — and their bytes leave the files some time after
that, when the storage engine next compacts those files.

**A window per table:**

```sql
CREATE TABLE sessions (id UUID PRIMARY KEY, who INT8, data JSONB) WITH (gc_ttl = '10m');
ALTER TABLE audit_log SET (gc_ttl = '90 days');
ALTER TABLE sessions RESET (gc_ttl);          -- back to the node's window
```

The window applies to that table only; no other table's history gets
shorter or longer. What a short window costs that table:

- `AS OF SYSTEM TIME` and follower reads of it reach back only that far;
  older reads fail with an error naming the GC threshold.
- A transaction that has been open longer than the window fails when it
  next touches the table.
- An incremental backup needs its base to be younger than the shortest
  window it covers; an older base is refused (`incremental base too
  old`) and you take a full backup instead.

The statement does not print a warning — it cannot, there is no notice
channel — so this list is the warning. The minimum is one second; a
time-series table with a `retention` already has its window and refuses
`gc_ttl`.

**Purge: gone now, with a report.**

```sql
DELETE FROM documents WHERE account = 4711;
ALTER TABLE documents PURGE;
```
```
 range_id | node_id | live_keys | garbage_versions | tombstones | intents | compacted | status
----------+---------+-----------+------------------+------------+---------+-----------+--------
       12 |       1 |     90210 |                0 |          0 |       0 | t         | clean
       12 |       2 |     90210 |                0 |          0 |       0 | t         | clean
       12 |       3 |     90210 |                0 |          0 |       0 | t         | clean
PURGE 31
```

`PURGE` collects every old version and deletion marker in the table —
rows and index entries — as of now instead of a window ago, compacts the
table's files on every replica so the bytes are no longer in them,
truncates the write-ahead copies, and returns what each replica holds
afterwards, counted from the storage engine's keys rather than through a
query. A replica with zeros in `garbage_versions`, `tombstones` and
`intents` holds one version of each remaining row and nothing else.

- **The tag is the verdict.** `PURGE n` (n versions removed): every
  replica answered and is clean. `PURGE INCOMPLETE`: at least one is
  `unconfirmed` (its node did not answer) or `not clean` (a transaction
  was still writing there), and the `status` column says which. Run the
  statement again when the node is back; it is safe to repeat.
- **It removes what was deleted, not what is live.** Delete first.
- **Older readers are cut off, loudly.** After a purge the table cannot
  be read at any earlier time: `AS OF SYSTEM TIME` before it, and any
  transaction that began before it, get an error on their next
  statement against the table — never a result with rows missing. Retry
  the transaction. Other tables are unaffected.
- **A node that was down** applies the purge when it rejoins, before it
  serves anything from before it, and is compacted and counted by the
  next `PURGE`. A node that was cut off from the others but kept
  running is the one gap: until it reconnects it can still answer a
  follower read (`AS OF SYSTEM TIME`) from before the purge. It is
  reported `unconfirmed`, which is why an incomplete purge is not a
  finished one.
- **What it does not reach:** backups taken earlier (they still hold the
  data — expire or rewrite them yourself); a copy of the range being
  sent to a new replica at that moment; and, for rows written before
  the table had ranges of its own, the write-ahead log of the range
  they were first written to, which is trimmed as that range takes
  further writes. Create tables that will be purged with a `gc_ttl` —
  that gives them their own ranges from the start — and this last case
  does not arise.
- It costs a compaction of the table on every replica. Use it for the
  account-deletion case, not after every `DELETE`.
- Not inside `BEGIN`; needs table ownership and cluster version v21. A
  key range rather than a table: `datax debug purge`
  ([operations guide](operations.md#purging-deleted-data)).

## Parameters

`$1, $2, ...` work through the extended protocol in text and binary
formats — every driver's normal parameterized-query path. A parameter
counts wherever it is written: the select list, `WHERE`, `JOIN ... ON`,
`HAVING`, `ORDER BY`, `LIMIT` / `OFFSET`, `CASE`, any subquery (`IN`,
`EXISTS`, scalar, derived or joined), a `WITH` member, `VALUES`, `SET`,
`ON CONFLICT`, `RETURNING`, and the statement under `EXPLAIN [ANALYZE]`.

Its type, which the server reports when a driver describes the statement
(and which decides how the driver encodes the value), is inferred as in
PostgreSQL: a parameter compared with a column — bare, qualified by its
table or alias, or by a join side (`g.at < $1`), inside `OR`, in a
subquery — or assigned to one (`SET`, `VALUES`, `INSERT ... SELECT $1`)
takes the column's type; `$1::int8` takes the cast's; `LIMIT` and
`OFFSET` take `int8`. A parameter used more than once has one type; two
columns that fix different ones (`WHERE a.at = $1 OR b.name = $1`) are
an error, `42P08`, as in PostgreSQL. Columns of different numeric widths
(or a date and a timestamp) agree on the first. A parameter nothing
types describes as text.

Describe answers with the result's columns for every
statement that returns rows — queries, `RETURNING`, `EXPLAIN`, every
`SHOW`, `ALTER TABLE ... PURGE`, `SPLIT AT` and `UNSPLIT` — and with
NoData for the rest; a prepared statement whose result changes width
before it runs (`SELECT *` over a table altered since) is refused with
`0A000`, as PostgreSQL refuses it (pgx then drops the cached statement
and prepares it afresh on the next run).

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
SHOW PLACEMENT [FOR DATABASE d]; -- database_name, replicas, constraints, source
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
`pg_indexes`, `pg_collation`, `pg_tablespace`, `pg_stat_user_tables`,
`pg_policy`, `pg_policies`, `pg_description`, `pg_extension` and
`pg_available_extensions` (the built-in extensions: `plpgsql`,
`pgcrypto`, `uuid-ossp` and `vector`, always installed)
(and empty stand-ins for the catalogs of features datax lacks —
publications, functions, triggers, operator classes and
families, `pg_amop`, `pg_amproc`, ...), plus
`information_schema.schemata`, `tables`, `views`, `columns`,
`table_constraints`, `key_column_usage`, `statistics` and
`role_table_grants`. OIDs are stable across the cluster (a table's is its
descriptor ID; `'t'::regclass` gives it); a catalog whose rows have OIDs
also has PostgreSQL's `tableoid` system column (not in `*`). A bare `pg_class` resolves to
the catalog when no table of that name exists in the current database.
They describe the current database, as each PostgreSQL database's own
catalogs do (`table_catalog` is its name); `pg_database`, `pg_roles`,
`pg_auth_members` and `pg_tablespace` are the cluster's.
Every role may read them; `pg_catalog` lists every table and
`information_schema` the tables the reader holds a privilege on, as in
PostgreSQL ([security guide](security.md#roles-and-privileges)).
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

`datax_audit` is stricter. It is the cluster's audit record (see the
security guide's "Audit table"): admins and roles granted `SELECT` on it
read it, and no role — `root` and admins included — can `INSERT`,
`UPDATE`, `DELETE`, `TRUNCATE`, `DROP` or `ALTER` it, index it, or
reference it from a foreign key. The one statement it accepts is
`ALTER TABLE datax_audit SET (retention = '<period>')`, from an admin,
with a period of at least a day.

`datax_largeobject`, `datax_largeobject_metadata` and
`datax_largeobject_pending` hold [large objects](#large-objects): the
pages, the objects, and the contents waiting to be removed. They are
written through the `lo_*` functions alone — no role can change them
with SQL — and read by admins and by roles granted `SELECT` on them
(which shows every object's bytes, whoever owns it).
`pg_largeobject_metadata` lists the objects.

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
`DROP DATABASE ... CASCADE` is one transaction, however many tables it
drops: it retries itself like any single statement, and if it still
loses to DDL running in the same database it fails with `40001` having
dropped nothing.

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

### Region-restricted replication

A database can say where its replicas may live and how many there are.
The allocator then honours that for every range of every table in it:

```sql
CREATE DATABASE eu WITH (replicas = 3, constraints = ('region=eu-west-1', 'region=eu-central-1'));
ALTER DATABASE eu SET (constraints = ('region=eu-west-1'));  -- narrow it
ALTER DATABASE eu SET (replicas = 5);                        -- constraints untouched
ALTER DATABASE eu SET (constraints = ());                    -- lift the restriction
SHOW PLACEMENT FOR DATABASE eu;
SHOW PLACEMENT;                                              -- the session's own database
```

`constraints` is a **disjunction** of locality tiers a node declares
with `--locality`: a replica may live on any node carrying **any one**
of them, so naming two regions means "either of these". `replicas`
overrides the cluster replication factor for this database alone; it
must be odd (a majority of an even count tolerates no more failures than
the odd count below it) and at most 9. An option an `ALTER` does not
name is left as it was, and an empty constraint list is how a
restriction is lifted — there is no separate `RESET`.

Writing a policy moves no data: the allocator converges the ranges onto
nodes the policy admits over the following passes, one range at a time,
and `SHOW PLACEMENT` reports the count it will actually use, so a
database with no policy shows the cluster default and says so.

If **no live node** satisfies a policy, the allocator does nothing — it
never places a replica outside a region you named, and it never drops
one to satisfy a policy. The data stays where it is and the health API
reports `placement-unsatisfiable`; `placement-misplaced` marks the
window while replicas are still being moved home. Asking for more
replicas than the admitted nodes can hold (three replicas, two nodes in
the region) is a legal policy that simply cannot converge until a node
joins there.

Placement needs cluster version v16 and the database's ownership (or
admin). The mechanics — how a range finds its policy, why a policy
splits its tables into their own ranges, and what each allocator pass
does with it — are in
[replication and placement](../replication-and-placement.md#region-restricted-replication-per-database-placement).

