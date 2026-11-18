# Dumping and restoring with pg_dump

PostgreSQL's own `pg_dump`, `pg_restore` and `psql` dump a datax database
and restore it into another datax database (or cluster). This is the
portable, SQL-level complement to datax's own [backup and
restore](operations.md): use it to move a database between clusters, to
keep a human-readable copy of a schema, or to take a database to or from
PostgreSQL.

What is tested: the `pg_dump`, `pg_restore` and `psql` of PostgreSQL 16,
against every format (`plain`, `-Fc`, `-Fd`) and `--schema-only` /
`--data-only`. The test suite runs them on every CI build
(`TestPgDumpRestore`). Other versions of the client tools send other
catalog queries and may stop on one datax does not answer: pg_dump 18
stops on its first table query (`column "c.relallvisible" does not
exist`). Use the PostgreSQL 16 client (on Debian and Ubuntu,
`postgresql-client-16`, in `/usr/lib/postgresql/16/bin`) until 17 and
18 are supported (issue #370).

## Quick start

```sh
SRC="postgres://root@src-node:26433/app?sslmode=disable"
DST="postgres://root@dst-node:26433/app?sslmode=disable"

# Roles live in the cluster, not the database: pg_dump does not dump them.
psql "$DST" -c "CREATE ROLE reader" -c "CREATE USER alice PASSWORD '...'"

# Plain SQL, restored with psql.
pg_dump "$SRC" -f app.sql
psql "$DST" -v ON_ERROR_STOP=1 -f app.sql

# Or the custom format, restored with pg_restore.
pg_dump "$SRC" -Fc -f app.dump
pg_restore --exit-on-error -d "$DST" app.dump
```

The target database should be empty (a freshly `CREATE DATABASE`d one).
Restore **without** `--single-transaction` / `-1`: `ADD CONSTRAINT`,
`CREATE INDEX` and the other statements that read a table's rows run
online, in transactions of their own, and are refused inside a
transaction block. A plain `psql -f` and a default `pg_restore` run each
statement on its own.

## What is carried over

| Object | Notes |
|---|---|
| Tables, columns, types and typmods (`numeric(10,2)`, `varchar(n)`, arrays) | |
| `NOT NULL`, `DEFAULT` expressions, `CHECK`, `UNIQUE`, `FOREIGN KEY` (with their actions) | pg_dump adds the keys and foreign keys after loading the rows |
| Primary keys | see [Primary keys](#primary-keys) below |
| `SERIAL` columns, identity columns (`ALWAYS` / `BY DEFAULT`), sequences with every option | each sequence's current value (`setval`) too; see [Sequences](#sequences) |
| Indexes: unique, partial, expression, `DESC`, GIN, `text_pattern_ops` and the other operator classes | |
| Views | the definition as datax stores it |
| Enum types | |
| Comments on tables, views, columns, indexes and constraints | the only objects datax lets you comment on |
| Owners (`ALTER ... OWNER TO`), `GRANT`s on tables, views and sequences, `ALTER DEFAULT PRIVILEGES` | the roles must exist on the target |
| Row-level security: `ENABLE` / `FORCE ROW LEVEL SECURITY` and the policies | |
| Large objects, with their owners | see [Large objects](#large-objects) |
| Data of every type: `jsonb`, `bytea`, arrays, `timestamptz`, `numeric`, text with tabs, newlines and quotes, NULLs | through `COPY`, byte for byte |

A restored database dumps exactly as its source did.

## What is not, or differs

- **Roles.** `pg_dump` dumps one database, and the roles it names must
  already exist where it is restored. `pg_dumpall` (which dumps roles)
  does not work against datax; create the roles with `CREATE ROLE` /
  `CREATE USER` first.
- **Schemas, functions, triggers, extensions, ...** do not exist in
  datax, so a datax dump has none. `public` is the only schema. The dump
  says `ALTER SCHEMA public OWNER TO` the database's owner: datax accepts
  that (the public schema is always its database owner's) and refuses
  any other owner.
- <a id="primary-keys"></a>**Primary keys.** pg_dump creates each table
  without its key, loads the rows, and adds the key afterwards with
  `ALTER TABLE ... ADD CONSTRAINT ... PRIMARY KEY`. A datax table created
  without a key is keyed by a hidden `rowid`
  ([details](sql.md#tables-without-a-primary-key)), so the restored
  table keeps that storage key: the added key is its columns made `NOT
  NULL` plus a unique index, which the catalogs (and psql's `\d`, and a
  foreign key written without columns) treat as the primary key. It
  enforces the same rules. What differs is speed: a lookup by key reads
  the index and then the row, and rows are not stored in key order. For
  a large or hot table, restore the schema by hand with the key inside
  `CREATE TABLE` (`pg_dump --schema-only`, edit, run it, then restore
  with `--data-only`).
- <a id="sequences"></a>**Sequences.** A gateway takes sequence values in
  blocks (`CACHE`, 32 by default), and the value pg_dump reads is the
  end of the block handed out, so the restored sequence continues a
  little past the source's last value: gaps, never repeats. A `SERIAL`
  column's sequence is restored as `bigint` (every datax sequence is).
- <a id="large-objects"></a>**Large objects** belong to the cluster in
  datax, not to a database. Restored into another database of the same
  cluster, each `lo_create` reports that the object already exists (the
  data written after it is the same).
- **Grants on datax's own tables** (`GRANT SELECT ON datax_metrics TO
  grafana`) are dumped as grants on `pg_catalog.datax_metrics` — those
  tables are listed in `pg_catalog`, as PostgreSQL's system catalogs are,
  and not dumped themselves. On a cluster that has not yet created the
  table (they are made on first use), the `GRANT` fails; run it again
  later.
- **Consistency.** pg_dump reads everything in one transaction, so the
  dump is one consistent snapshot, while writes go on. A schema change
  that commits within the cluster's clock-offset window (500 ms by
  default) of the dump's start can still make that transaction fail
  with `40001`; run pg_dump again. To pin the snapshot, export one and
  hand it to pg_dump:

  ```sql
  BEGIN; SELECT pg_export_snapshot();   -- 18E8FED3B87BF79C-00000000-1
  ```

  ```sh
  pg_dump "$SRC" --snapshot=18E8FED3B87BF79C-00000000-1 -Fc -f app.dump
  ```

  A snapshot names a timestamp: a transaction that imports it with `SET
  TRANSACTION SNAPSHOT` reads the database as of that moment (as `AS OF
  SYSTEM TIME` does), never restarts, and refuses writes. Unlike
  PostgreSQL, the exporting transaction need not stay open; the
  identifier stays good for as long as that moment is within the
  cluster's GC window (`gc_ttl`). Parallel dumps (`-Fd -j N`) use the
  same mechanism for their workers.
- **Parallel restore** (`pg_restore -j N`) is not recommended yet: it
  builds several indexes of one table at the same time, which datax does
  not handle reliably, and the first large object it restores can fail
  with `40001`. Restore without `-j`.
- **`LOCK TABLE`.** pg_dump locks the tables it dumps `IN ACCESS SHARE
  MODE`; datax's schema changes are online and never wait for readers,
  so that lock (and `ROW SHARE` / `ROW EXCLUSIVE`) is checked and
  granted without anything to wait for. The stronger modes are refused
  (`0A000`) rather than granted without effect.

## SQL this added

These are what pg_dump and its restore need, and work for any client:

- `LOCK [TABLE] [ONLY] t [, ...] [IN ACCESS SHARE | ROW SHARE | ROW
  EXCLUSIVE MODE] [NOWAIT]`, in a transaction block.
- `PREPARE name [(types)] AS statement`, `EXECUTE name [(values)]`,
  `DEALLOCATE [PREPARE] {name | ALL}`.
- Cursors: `DECLARE name CURSOR [WITH HOLD] FOR query`, `FETCH [NEXT |
  FORWARD [n | ALL] | n | ALL] [FROM | IN] name`, `MOVE ...`, `CLOSE
  {name | ALL}` — forward only; the query runs when the cursor is
  declared.
- `pg_export_snapshot()` and `SET TRANSACTION SNAPSHOT 'id'` (above).
- `ALTER TABLE t ADD PRIMARY KEY (cols)` on a table keyed by its hidden
  `rowid` (above), `ALTER TABLE ... ALTER COLUMN c ADD GENERATED {ALWAYS |
  BY DEFAULT} AS IDENTITY [(SEQUENCE NAME s, options)]`, `ALTER SEQUENCE
  ... OWNED BY t.c | NONE`, `ALTER VIEW v OWNER TO r`, `ALTER SCHEMA
  public OWNER TO r`, `ALTER LARGE OBJECT oid OWNER TO r`.
- `COPY ... FROM STDIN` inside a transaction block, where it is atomic
  with the block (outside one it still commits in chunks).
- `SET default_table_access_method = heap`; `TRUNCATE ONLY`.
- The catalogs pg_dump reads: `acldefault()`, the ACL columns (`relacl`,
  `nspacl`, `datacl`) and `pg_default_acl` from the grants,
  `pg_options_to_table()`, `pg_init_privs`, `pg_transform`,
  `pg_seclabels` and the rest it probes (empty), and `pg_depend` rows
  for defaults on sequences and views on their tables, by which it
  orders its output.
