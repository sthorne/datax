# Schemas (gap PG-D-001)

Status: implemented, with the leftovers listed under "Deferred" at the
end.

PostgreSQL places every relation in a schema (a namespace inside a
database); names are resolved through `search_path`. Until this change
datax had one schema, `public`, which the parser stripped from names;
`search_path` was accepted, echoed and ignored; and a two-part name
`app.x` meant table `x` of *database* `app`. This document fixes the
catalog model, the name-resolution rules and the compatibility story,
and records what is deferred.

## Goals and constraints

1. **No data migration.** Every existing cluster opens unchanged and
   every existing table resolves exactly as before with the default
   `search_path`. No key moves; no descriptor is rewritten.
2. **PostgreSQL semantics** for `CREATE / DROP / ALTER SCHEMA`,
   qualified names, `search_path`, schema privileges and the catalogs,
   with PostgreSQL's SQLSTATEs (`3F000`, `42P06`, `2BP01`, `42501`,
   `0A000`).
3. **Keep the datax extension** that a two-part name may name a
   database, for the applications that use it, at a precedence below a
   schema of that name.
4. **Never resolve a name to the wrong table.** Every by-name catalog
   entry point resolves through one function, with the session's path;
   anything not yet schema-aware refuses (`0A000`) rather than guessing.

## Catalog model

### Schema descriptors

A schema is a `SchemaDescriptor` (`pkg/sql/catalog/schema.go`):

```
/system/schemadesc/<schemaID>          → JSON {id, name, database_id, owner,
                                            privileges, grant_options, version}
/system/schemans/<dbID>/<name>         → schemaID
```

IDs come from the shared descriptor counter (`keys.DescIDGenKey`), so a
schema ID never equals a table, sequence, type or database ID.

`public` is **implicit**: schema ID 0 in every database, never stored.
Its owner is the database's owner (PostgreSQL 15's
`pg_database_owner`) and its privileges stay where they already are, on
the database descriptor (`SchemaPrivileges`, `SchemaGrantOptions`,
`UsageRestricted`). Nothing about `public` changes, so nothing has to be
backfilled.

### Tables and views

`TableDescriptor` gains `schema_id` (omitempty; 0 = `public`). The name
entry of a table stays in its database's table namespace
(`/system/nsdb/<dbID>/<entry>`):

* `public` table: entry = the table's name — unchanged, which is why
  every existing table keeps resolving with no rewrite;
* table in schema S (ID n): entry = `"\x00schema\x00" + n + "\x00" + name`.

The temporary tables (PG-D-009) already file their entries under a name
holding NUL bytes, which no SQL text can spell and which every by-name
entry point refuses; a schema's entries reuse that mechanism. The
consequences fall out of the existing code:

* `ListIn` (one database's tables) reads all name entries of the
  database, so it lists schema tables, and `DROP DATABASE ... CASCADE`
  drops them, with no change.
* The gateway cache and the descriptor leases are keyed by
  `(dbID, entry)`: a schema table is cached and leased like any other
  table, and schema DDL drains like table DDL.
* Keying by schema **ID**, not name, makes `ALTER SCHEMA ... RENAME`
  one name-entry move: its tables' entries, leases and cache keys stay
  valid. (Keying by name would need every entry rewritten.)
* An old binary that met such an entry could not reach it by name (it
  holds a NUL byte), so a mixed-version cluster can never resolve a
  schema table as a `public` one. Schemas are still gated on the
  cluster version (below), so before finalize there are none.

### Sequences and types (gap PG-D-035)

Sequences and types keep their own per-database namespaces
(`/system/seqns/<dbID>/<entry>`, `/system/typens/<dbID>/<entry>`) and
file the same entries as tables: a `public` sequence or type under its
name, as before; one of schema S under `SchemaNamespaceName(S, name)`.
`SequenceDescriptor` and `TypeDescriptor` gain `schema_id` (omitempty),
so every existing sequence and type, and its key, is unchanged. Names
resolve through the same `resolve` as tables
(`Accessor.LookupSequenceIn`, `LookupTypeIn`), and a listing names one
exactly with `SequenceRelationName` / `TypeRelationName` (the
`RelationName` form with the object's entry). A sequence and a table of
one schema share the relation namespace; a type is refused a name a
table or sequence of its schema has.

A `SERIAL` or identity column's sequence goes in its table's schema,
and `ALTER TABLE ... SET SCHEMA` moves the table's sequences with it
(those of its columns and those `OWNED BY` it). `ALTER SEQUENCE ...
SET SCHEMA` refuses a sequence a column owns (`0A000`, as PostgreSQL);
`ALTER TYPE ... SET SCHEMA` moves a type, whose columns hold its ID.

**Defaults bind their sequences.** PostgreSQL stores `DEFAULT
nextval('x')` with the `regclass` resolved, so the default draws from
the sequence `x` named when it was set. datax stores the default as
text, so a column records beside it, for each literal sequence name of
its `nextval` / `currval` / `setval` calls, the sequence it resolved to
(`Column.SeqBindings`, name as written → sequence ID). A name that
names no sequence is refused when the default is set (`42P01`, as
PostgreSQL's cast to `regclass`). The default is evaluated with the
bound sequences, by ID, whatever the inserting session's search path,
and without asking `USAGE` on their schemas (PostgreSQL checks none for
a bound `regclass`): a sequence of the name created later in a schema
earlier on that path changes nothing. A name is never resolved through
the inserting session's path: binding 0 (a table restore leaves it for
a sequence it does not bring back) resolves an unqualified name in
`public`. A default set before bindings
existed has none, and resolves its names through `public` alone (they
could name nothing else then), so no descriptor is rewritten. Moving a
bound sequence (or its table, or renaming its schema) rewrites the
defaults' text to name it where it now is; `DROP SEQUENCE` of a bound
sequence is `2BP01` unless `CASCADE`, which drops those defaults, and
so is `DROP TABLE` of a table owning one; every session's temporary
tables count. `LIKE ... INCLUDING DEFAULTS` copies the binding (the
name is qualified when the path would resolve it to another sequence).

A view stores its query as text, and so do defaults and CHECK
constraints for their casts: `ALTER SEQUENCE / TYPE ... SET SCHEMA`,
`ALTER TABLE ... SET SCHEMA` of a table owning a sequence, and `ALTER
SCHEMA ... RENAME` are refused (`2BP01`) while a view names an affected
sequence or type (resolved through the view's path), or a default or
CHECK casts to an affected type by a name that could be it. `CREATE
SEQUENCE ... OWNED BY` takes a table of the same owner and schema
(`55000`). Tables, views and sequences of a schema share its relation
namespace (`42P07`, for `CREATE TABLE / VIEW` and `RENAME` too).

Reaching a sequence, type or relation of a schema other than `public`
by name (`nextval('s.q')`, `'s.q'::regclass`, `'a'::s.mood`,
`'s.mood'::regtype`) needs `USAGE` on the schema (`42501 permission
denied for schema s`), creating one `CREATE` on it; the search path skips a schema without `USAGE` for
sequences and types as for tables. The v22 gate needs nothing new:
no schema, so no sequence or type in one, exists before v22.

### Version gate

The temporary and unlogged tables (gap PG-D-009) are gated on v22 too
(`CREATE TEMP / UNLOGGED TABLE`, `ALTER TABLE ... SET UNLOGGED`): a v21
node would list a temporary table as an ordinary one and drop either
descriptor field when it rewrote the descriptor. A restore of a backup
holding schemas is refused below v22.

`CREATE SCHEMA` (the only way to create the new descriptors and
entries) is refused with `0A000` until the cluster has finalized
**v22**, the cluster version this tree is developing ("V22 also
introduces schemas" in `pkg/version`). A v21 node never sees a schema
descriptor or a schema table. Reading paths need no gate: with no
schema descriptors, every resolution reduces to the old one.

If v22 has shipped by the time this merges, the gate must move to a new
version (v23); the code names it in one place (`requireSchemas`).

## Name resolution

All resolution goes through `Accessor.resolve` (catalog) with the
session's *resolution context* (`catalog.WithResolution(ctx, path,
user)`), which the session attaches to every by-name catalog call it
makes (`Session.rctx`). A caller with no context (background jobs, the
server package) gets the path `public`, resolved without a schema
lookup: exactly the old behaviour and cost.

Where the session names a table it found by other means — a listing
(`DROP OWNED`, `DROP SCHEMA ... CASCADE`, `ALL TABLES IN SCHEMA`), a
foreign key's child, the views a drop cascades to, the index search —
it uses `catalog.RelationName(d)`: the database ID and the name entry
behind a marker no SQL text can spell, which resolves to exactly that
descriptor whatever the search path. The post-commit drain records each
changed table so too (`Session.drainName`), because a `SET LOCAL
search_path` has ended by the time `COMMIT` drains.

Cost: none in KV. A leasing gateway caches each database's schema set
(every schema's name and descriptor) under a lease at the database's
schema generation, which every schema change increments in its own
transaction; the session drains the set after the commit as it drains a
table, so once `CREATE`, `DROP` or `ALTER SCHEMA`, or a `GRANT` / `REVOKE`
on a schema, returns, every gateway resolves through the new set
(`pkg/sql/catalog/schemacache.go`). The `"$user"` lookup of the default
path, nearly always a miss, is answered from memory, as is a schema named
on the path or in a qualified name. The set also lists the name entries
of the tables and views filed in those schemas, so a table of public
behind another schema of the path (`SET search_path = app, public`, or a
role with a schema of its own name) is not first looked up in that
schema with a read: a lookup that finds the name nowhere reads the
entries the set answered for before it reports the table missing, so
the set need only be current for a name that two schemas hold. A
`CREATE` or `ALTER ... RENAME TO` that files a table or view under a
name another schema of the database holds too, and every `ALTER ... SET
SCHEMA` of a table, change the schema generation and drain like a schema
change; a table created under a name of its own does not wait for a
drain. A historical transaction, and one
that has itself changed the database's schemas, read the schema names in
the transaction instead; so does the resolution of where an object is
created (`ResolveTarget`), so that a concurrent `DROP SCHEMA` conflicts
with the creation.

A name reaches the catalog as the parser now keeps it: `x`, `a.b` or
`a.b.c` (the parser used to drop `public.`).

### `x` (unqualified)

1. The session's temporary tables (`pg_temp` leads the path, as in
   PostgreSQL), handled by the session as before.
2. Each schema of `search_path`, in order: `"$user"` expands to the
   current role's name; a schema that does not exist is skipped;
   `pg_catalog`, `pg_temp` and `information_schema` are skipped here.
   The first schema holding `x` wins.
3. A `pg_catalog` / `information_schema` virtual table named `x`
   (datax already let a real table shadow a catalog name; kept, and
   documented as a difference: PostgreSQL searches `pg_catalog` first
   unless the path lists it).
4. A sequence named `x`, read as a relation (as before).

The default `search_path` becomes PostgreSQL's `"$user", public`. With
no schema named after the role this is exactly the old `public`.

One datax extension: a path that names no schema but the system ones
(`''` or `pg_catalog`, what pg_dump sets on both ends) resolves an
existing unqualified name through `public`. datax renders view and
constraint definitions without qualifiers, so without it a dump of an
all-`public` database would no longer restore. Creation does not fall
back (`3F000`), and `current_schema()` is NULL.

### `s.x` (two parts)

1. `s` = `public`: the `public` schema of the current database. (This
   is what makes `public.t` skip a temporary table `t` that shadows it:
   the old parser had reduced `public.t` to `t`.)
2. `s` = `pg_temp`, `pg_catalog`, `information_schema`: as before.
3. A schema `s` of the current database: that schema.
4. **datax extension, deprecated:** no such schema, and a database `s`
   exists: table `x` of the `public` schema of database `s`.
5. Otherwise the relation does not exist (`42P01`, PostgreSQL's
   `relation "s.x" does not exist`); a `CREATE` into it is `3F000
   schema "s" does not exist`.

**Precedence:** a schema of the current database always beats a
database of the same name. An application that names databases with
two-part names keeps working as long as no schema of that name is
created in its current database; creating one silently redirects those
names, which is why the fallback is deprecated: it should be removed in
a future major release, and new code should use three-part names
(`db.public.x`) or connect to the database. No notice is emitted yet,
to avoid flooding the logs of the applications that rely on it.

### `d.s.x` (three parts)

* `d` = the current database: schema `s` of it (as `s.x`, without the
  database fallback).
* `d` another database and `s` = `public`: table `x` of `d`'s `public`
  schema — the existing datax extension (`db.public.t` worked before).
* `d` another database and `s` any other schema: `0A000
  cross-database references are not implemented: "d.s.x"`, as
  PostgreSQL.

### Where the name is created

`CREATE TABLE / VIEW / TABLE AS` with an unqualified name creates in
the first schema of `search_path` that exists (`"$user"` included,
`pg_temp` and the catalogs excluded); none: `3F000 no schema has been
selected to create in`. A qualified name creates where it resolves
(rule 3/4 above for two parts).

### Views

A view's query is stored as text and re-planned when the view is used,
with the owner's privileges. If its names resolved through the reader's
search path, a reader could point the view's definer-rights query at a
table of its choosing. So `CREATE VIEW` records the creator's path (the
schemas of it that exist, `view_search_path`; empty means `public`, how
every older view resolved), and the view's query always resolves
through it: the expanded member carries the path (`parser.CTE.SearchPath`)
and the session switches to it while the member runs or is checked
(`Session.viewPath`), skipping the reader's temporary tables and the
statement's lookup memo.

The path alone is not enough: a table created later in a schema
earlier on the path would take a name the view meant for another
relation, and the view would read it with its owner's privileges. So
`CREATE VIEW` also records, for each relation name its query writes,
the relation it resolved to (`view_bindings`, name as written → table
or view ID), and reading the view binds each such name to exactly that
relation again (`Session.viewBindings`), as PostgreSQL binds a view's
relations at creation; only names that are not relations (the catalog
views) go through the stored path. A view made before schemas has
neither and resolves through `public`, as it always did. Renaming a
schema a view reads or resolves through, dropping a schema in another
schema's view's path, or moving a relation a view reads, is refused
(`2BP01`), as renaming a table a view reads already was.

## Statements

* `CREATE SCHEMA [IF NOT EXISTS] name [AUTHORIZATION role]`,
  `CREATE SCHEMA [IF NOT EXISTS] AUTHORIZATION role` (name = role).
  Needs CREATE on the database (or the admin role / ownership), and
  membership in the AUTHORIZATION role. Names starting with `pg_` are refused
  (`42939 unacceptable schema name`); `public`, `information_schema`
  and existing names are `42P06 schema "s" already exists` (with IF
  NOT EXISTS: a notice-free no-op). Schema elements (`CREATE SCHEMA s
  CREATE TABLE ...`) are not supported (`0A000`).
* `DROP SCHEMA [IF EXISTS] name [, ...] [CASCADE | RESTRICT]`: owner or
  admin. Non-empty without CASCADE: `2BP01 cannot drop schema s because
  other objects depend on it`. CASCADE drops the schema's views and
  tables through `DROP TABLE / VIEW ... CASCADE` (so dependency rules,
  drains and the dropped-table reclaim all apply). `public`,
  `pg_catalog`, `information_schema` cannot be dropped (`42501` for the
  system ones; `public` is `0A000`, as it is implicit).
* `ALTER SCHEMA name RENAME TO new` (owner + CREATE on the database;
  `42P06` when taken) and `ALTER SCHEMA name OWNER TO role`.
  `ALTER SCHEMA public OWNER TO` keeps its existing behaviour.
  `REASSIGN OWNED` moves schemas, `DROP OWNED` drops them (one holding
  another role's objects only with CASCADE, and only in the current
  database), and `DROP ROLE` refuses an owner of a schema.
* `ALTER TABLE / VIEW [IF EXISTS] name SET SCHEMA s`: needs ownership
  and CREATE on `s`; moves the name entry, keeps the ID (indexes,
  constraints, foreign keys, view dependencies all hold). `42P07` when
  `s` has a relation of that name. Refused for temporary tables
  (`0A000`); a table's own sequences move with it.
  `ALTER SEQUENCE ... SET SCHEMA` and `ALTER TYPE ... SET SCHEMA`
  move sequences and types (see "Sequences and types").
* `GRANT / REVOKE { USAGE | CREATE | ALL } ON SCHEMA s [, ...]` with
  grant options, on any schema. `ALL TABLES IN SCHEMA s` names any
  schema.

## Privileges

* `public`: unchanged. `USAGE` is PUBLIC's unless revoked; `CREATE` is
  the database owner's and its grantees' only. That is PostgreSQL 15+
  behaviour (CREATE on `public` revoked from PUBLIC), and it is what
  datax already did, so nothing existing changes.
* Other schemas: the owner (and admins) hold everything; others only
  what is granted, PUBLIC included — PostgreSQL's defaults (no
  privileges for PUBLIC on a new schema).
* Reaching any object of a schema needs `USAGE` on it (`42501
  permission denied for schema s`), checked with the table privilege,
  as `public`'s already was. Creating in a schema needs `CREATE` on
  it.
* A schema of the search path the acting role holds no `USAGE` on is
  skipped, for resolution, as the creation target and in
  `current_schema()`, as in PostgreSQL (`catalog.Resolution.CanUse`):
  otherwise a role with CREATE on the database could create a schema
  named after another role and capture that role's unqualified names
  through `"$user"`. `public` is never skipped (a role whose USAGE on it
  was revoked gets `42501`, as before schemas); explicit qualification
  of a schema without USAGE is `42501`.

## Catalogs and functions

* `SET / SHOW search_path` round-trip as before; `current_schema()` is
  the first existing schema of the path (NULL when none);
  `current_schemas(b)` lists the existing path schemas, with
  `pg_catalog` (and `pg_temp_N` when the session has temporary
  tables) when `b`.
* `pg_namespace`: a row per schema of the current database (OID =
  16384 + schema ID, owner, ACL), besides `pg_catalog`,
  `information_schema` and `public`.
* `pg_class.relnamespace`, `pg_type.typnamespace` of a table's row
  type, `pg_constraint.connamespace`, `pg_tables` / `pg_views` /
  `pg_indexes` / `pg_stat*` `schemaname`, `information_schema.schemata`,
  `.tables`, `.columns`, `.views`, `.table_constraints`,
  `.key_column_usage` and the privilege views name the real schema.
  psql's `\dn`, `\dt s.*` and `\d s.t` work from these.
* `'s.t'::regclass` resolves; a `regclass` renders `s.t` when the table
  is not visible through the path (PostgreSQL's rule).
* `has_schema_privilege`, `::regnamespace` and `to_regnamespace`
  answer for any schema.
* `pg_get_userbyid` now names the role of the OID (it always said
  `root`), so psql's `\dn` and `\dt` show owners.

### Temporary tables

They stay listed under `public` with `relpersistence 't'` (a
per-session `pg_temp_N` namespace is deferred); `public.t` no longer
reaches a temporary `t` that shadows a `public` one (fixed by keeping
the qualifier), and `pg_temp.t` still names only the temporary table.

## Backup and restore

A cluster backup collects the schema descriptors and names with the
databases (`manifestKeyGroups["databases"]`, spans added), restores them
verbatim, and keeps the descriptor-ID counter past their IDs; a
schema table's name entry is restored under its schema. Backups taken
before this change restore unchanged (no schema keys). An older binary
refuses a backup carrying schema keys (they fall outside the spans it
accepts): a safe failure. Single-table restore (`RESTORE TABLE`)
refuses a table outside `public` (`0A000`) for now.

## What ships in this change

Schema descriptors and the version gate; `CREATE / DROP / ALTER
SCHEMA`; tables and views in schemas, with DML, `COPY`, DDL, indexes,
constraints and foreign keys across schemas; `SET SCHEMA` for tables and
views; qualified-name resolution with the precedence above;
`search_path`, `current_schema()`, `current_schemas()`; schema
privileges and their effect on ownership statements; views resolving
through their creator's path; the catalogs listed above; `DROP DATABASE`
dropping its schemas; backup and restore of schemas.

## Deferred (leftovers)

* Sequences and types (gap PG-D-035, now implemented), leftovers:
  an unqualified relation name resolves the path's tables before its
  sequences (a sequence in an earlier schema does not hide a table of
  the name in a later one, and `nextval` finds a sequence behind a table
  of its name); `ALTER TABLE ... DROP COLUMN` of a `SERIAL` column whose
  sequence another table's default is bound to drops it without asking
  (that default then fails, `42P01`); `DROP SCHEMA ... CASCADE` refuses
  a type a column of another schema uses, where PostgreSQL drops the
  column; `SHOW CREATE TABLE` prints an enum type of another schema
  unqualified (`format_type`, `regtype` output and the catalogs
  qualify it off the path, as PostgreSQL does); a view, default or
  CHECK naming a sequence or type by text blocks moving it (2BP01)
  where PostgreSQL's OID binding lets it move;
  `RESTORE TABLE` of a `public` table whose enum type is in another
  schema restores it onto (or creates) a `public` type of that name
  and labels.
* Views store text and a search path, not resolved OIDs (the rules above
  keep them safe; renames under them are refused).
* A per-session `pg_temp_N` namespace for temporary tables (they are
  listed under `public`, with `relpersistence 't'`).
* `CREATE SCHEMA ... CREATE TABLE ...` schema elements; `COMMENT ON
  SCHEMA`; `ALTER DEFAULT PRIVILEGES IN SCHEMA s` for a schema other
  than `public`; functions, collations and other objects in schemas
  (datax has no user functions yet).
* Column references with a schema (`s.t.c`) bind by table name only
  (alias two same-named tables of different schemas), and a statement
  reading two same-named views of different schemas is refused
  (`0A000`).
* pg_dump and pg_restore of a database with schemas other than `public`
  are not verified.
* `RESTORE TABLE` / `RESTORE DATABASE` of a table outside `public` is
  refused (a cluster restore carries schemas).
* `public` without `USAGE` is refused (`42501`) rather than skipped
  from the path as PostgreSQL does (kept from before schemas).
* Removing the two-part database fallback (a future major release).
