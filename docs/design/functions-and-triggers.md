# User-defined functions and triggers (gaps PG-P-001, PG-P-002)

Status: proposed. This is a design only: nothing here is implemented.
It also covers PL/pgSQL and `DO` (PG-P-003), because triggers need PL/pgSQL
trigger functions. Procedures and `CALL` (PG-P-004), event triggers
(PG-P-006) and rules (PG-P-007) are out of scope.

Today `CREATE FUNCTION` and `CREATE TRIGGER` both fail in the parser:
`42601 expected TABLE, found "function"` and `... found "trigger"`. Dollar
quoting is no longer what blocks them (PG-P-005 is closed), so the
register's "Today" text for PG-P-001 is out of date. Every application
that ships a function or a trigger in its schema fails to migrate. The
typical cases are an `updated_at` trigger, an audit trigger, a
`DO $$ ... $$` migration step, and an `immutable_unaccent()` wrapper.

PostgreSQL is the reference throughout. Its documentation is cited by
page name, for example `sql-createfunction`, under
`https://www.postgresql.org/docs/17/<page>.html`.

## Summary

* **Four phases, each shippable on its own.**
  1. `LANGUAGE sql` functions.
  2. PL/pgSQL and `DO`.
  3. Triggers on tables.
  4. The rest: `INSTEAD OF`, transition tables, user functions in index
     expressions, polymorphic and variadic signatures.

  Phase 1 alone unblocks most schemas: functions used by defaults, CHECKs,
  views and policies, and functions called from application SQL. Phase 3
  unblocks `updated_at` and audit triggers.
* **Storage.**
  * A function is a new descriptor kind: `/system/funcdesc/<id>`, with
    IDs from the shared descriptor counter, so its OID is its ID.
  * Its name entry is keyed by database, schema, name and the identity
    argument types.
  * Triggers live in the table descriptor, so the existing leases and
    drains carry them to every gateway.
  * Dependencies get one reverse-record keyspace (`/system/dep`), which
    also fills `pg_depend`.
* **Resolution follows PostgreSQL**, with one documented shortcut: a
  builtin that accepts the call wins without a catalog read. `pg_catalog`
  comes first unless the path lists it. Stored expressions (defaults,
  CHECKs, views, policies, trigger `WHEN`) bind to function IDs, so a later
  object on the search path cannot capture them.
* **Execution.**
  * Inlinable SQL functions are expanded into the calling expression.
  * Every other call runs the body through `Session.execStmt`, in the
    caller's transaction, from a per-row closure. The closure mechanism is
    `registerRowFunc`, which must be hardened first (see below).
* **New cluster version v23.** Everything that creates the new
  descriptors or descriptor fields is refused until finalize.
* **Prerequisites found in the code** (details under "What the code does
  today"):
  * the row-function registry can be called across sessions, so it must
    be closed;
  * `current_user` ignores definer rights;
  * temporary tables shadow names whatever the position of `pg_temp`;
  * the read-only check runs at the top level only;
  * savepoints cost O(write set);
  * `session_replication_role` can be set by anyone.

## What the code does today

The design leans on these facts. Each one either constrains it or is reused
by it.

| Area | Today (where) | Consequence |
|---|---|---|
| Parser | Hand-written recursive descent (`pkg/sql/parser/parser.go`). A call `name(` is checked against the builtin registry **at parse time**: arity errors are `42601` (`lower() takes 1 argument(s), got 2`). `FROM name(...)` is accepted only when `builtins.Lookup` says the function is set-returning (`isTableFunc`). Only the `pg_catalog.` qualifier is accepted and dropped. `SELECT s.f(1)` and `SELECT * FROM f(1)` are `42601 unexpected "("`, and `f(a => 1)` is `42601` (PG-F-020). | The arity check moves to resolution. Qualified calls, user functions in `FROM`, and named arguments need parser work. |
| AST | `parser.Expr.Func` is a bare lower-case name. Stored expressions (`Column.DefaultExpr`, CHECK `Constraint.Expr`, `Policy.Using/Check`, `IndexDescriptor.Exprs/Predicate`, `ViewQuery`) are **SQL text**, re-parsed when used. | Function references in stored text are name-based, so they need bindings (as views have `ViewBindings`). |
| Builtins | `pkg/sql/builtins`: **one entry per name**, argument *families*, lenient coercion (anything renders to text), `Vol` (immutable / stable / volatile), `NotStrict`. There are no overloads. | User overloading needs a real resolver. Builtins enter it as single candidates in `pg_catalog`. |
| Types | `types.Family`: `int2`, `int4` and `int8` are one family `Int`, told apart by a width (`catalog.Column.Width`, and `types.Datum.W` on a value: an integer literal is an `int4`, operators take the wider operand's width); typmods live on `catalog.Column`. There are no composite or row types (PG-Q-012). | Overloads that differ only by integer width (or other same-family types) cannot be told apart at a call. `RETURNS SETOF tbl` can work only in `FROM`. |
| Evaluation | AST-walking: `evalExprEnv` (`session.go`) has **no session, context or transaction**, and it has 84 call sites. Session-dependent calls are spliced before the row loop by `resolveValueNode` (`subquery.go`). A per-row session call is rewritten to `datax:row_session_func(id, args...)`, and `evalFunc` runs a closure from a **process-global** `sync.Map` (`rowfuncs.go`). | Per-row user function calls use this path, but see "Prerequisites": as it stands, any session can call another session's closure. |
| Volatile session functions | `volatileFuncs` (`nextval` ...) outside a write path are spliced **once per statement** (`spliceVolatile`). | User `VOLATILE` functions must not inherit this; they run per row. |
| Statements | `Session.execStmt(ctx, txn, stmt, params)` is re-entrant for SELECT (correlated subqueries, view members, CTEs). `$n` parameters come from `params`. A per-statement lookup memo (`stmtLookups`), `WITH` bindings (`s.rels`) and the view-resolution state (`viewPath`, `viewBindings`, `privAs`) are session fields that are saved and restored around view members (`cte.go`). | A function body is executed as a statement whose `params` are the arguments. Nested execution must save and restore the same state, and also clear `s.rels`, or the caller's `WITH` names would capture the body's table names. |
| Definer rights | Views run as their owner through `s.privAs` (`privileges.go: actor()`) and resolve names through their stored path. `current_user` is spliced from `s.user`, not `actor()` (`subquery.go`). | `SECURITY DEFINER` reuses `privAs` and `viewPath`, and also has to change what `current_user` returns. |
| Temporary tables | They shadow every other table "as pg_temp leads PostgreSQL's search path", **whatever the position of `pg_temp` in the path**. They are skipped only inside a view's query (`session.go`, `lookupTemp`). | PostgreSQL's `SET search_path = admin, pg_temp` defence for definer functions would not work. Functions with a `SET search_path` resolve as views do. |
| DML | UPDATE and DELETE fetch every target row first, compute new values, check keys as a set, and write **one buffered `WriteBatch`** at the end (`applyUpdate`, `applyDelete`). NOT NULL and typmods are checked inside the compute loop. FK cascades write child rows directly (`rowGuard.enforceReferencing`, `constraint.go`). COPY FROM commits in chunks of 128 rows or 1 MiB, each in its own transaction unless it runs inside a block (`copy.go`). | BEFORE ROW triggers go into the compute loop, before the checks. AFTER triggers run after `RunBatch`. Cascades and COPY need explicit trigger hooks. |
| Read-only | `readOnlyViolation(stmt)` runs once, in the top-level dispatcher (`session.go:850`). | Nested DML in a function would skip it. The check moves into `execStmt`. |
| Streaming / AS OF | A plain SELECT on a streaming session evaluates its projection lazily while the client pulls (`executeStreamingSelect`). `AS OF SYSTEM TIME` runs in a historical transaction. | A statement that calls a non-inlined user function is not streamed. Writes under `AS OF` are `25006`. |
| Transactions | Every transaction is serializable (PG-X-001). Implicit statements retry on `40001` transparently, and notices and notifications are discarded per attempt (`RunTxn` in `executeData`). Savepoints are real (`kvclient.Txn.Savepoint`), but `Savepoint` copies the whole write set and `RollbackToSavepoint` sends a rollback for **every** written key. | PL/pgSQL exception blocks map to savepoints. In a loop over a large transaction their cost is quadratic, so it needs fixing before Phase 2 ships. |
| Leases | Gateway descriptor cache and leases (`catalog/lease.go`), keyed by `(dbID, name)` and typed `*TableDescriptor`. DDL drains after commit (`FinishDDLIn`). | Triggers in the table descriptor get leasing and draining for free. Functions are read uncached, inside the statement's transaction, in Phases 1–3. |
| Dependencies | Views: forward `ViewDepends` and reverse `/system/viewdep/<rel>/<view>` (`keys.ViewDependencyKey`). FKs: `InboundFKs`. `pg_depend` is empty (PG-C-005). | A general reverse-dependency keyspace is added for the new kinds. |
| Version | `pkg/version`: `V22` is current and carries schemas. | The new descriptors need **V23**. |
| Backup | Raw catalog groups with span checks (`manifestKeyGroups`, `pkg/server/backup.go`). Schemas ride in the `databases` group, so older binaries refuse them. Table descriptors are copied verbatim. | Function keys ride in `databases` too. |
| Catalogs | `pg_proc` lists builtins only. OIDs are `1<<28 + position in builtins.All()`, so they **shift when a builtin is added** (`vtable/tables.go`, `classoid.go`). `pg_trigger`, `pg_language` and `pg_depend` are empty. `pg_get_functiondef`, `pg_get_function_*` and `pg_get_triggerdef` return NULL (`evalFunc`). `relhastriggers` is already true for FK parents. | A user object never stores a builtin's OID; it stores the builtin's name. User OIDs are descriptor IDs. |
| Errors | `sql.Error` has Code, Msg, Detail and Hint, but no CONTEXT (`Where`). Notices are plain strings (`Result.Notices`). | `Where` and structured notices are needed for PostgreSQL's `CONTEXT: SQL function "f" statement 1` and for `RAISE ... USING`. |
| Settings | `check_function_bodies` exists (accepted, inert). `session_replication_role` exists, has no effect, and **needs no privilege** (PG-O-001). | pg_dump's `check_function_bodies = false` must be honoured. `session_replication_role` must be restricted before Phase 3. |

### Prerequisites

These are fixed first, in their own PRs:

1. **The row-function registry.**
   * Closures live in a process-global map keyed by a sequential ID.
   * `evalFunc` runs whichever closure the ID names, and the parser
     accepts the quoted name `"datax:row_session_func"`.
   * So one session can run `SELECT "datax:row_session_func"(n, ...)` and
     invoke another session's closure while its statement runs. Today that
     closure is the per-row `set_config` used by pg_dump.
   * The fix: key the closures by session, check the calling session in
     `callRowFunc`, and make the hook unreachable from SQL text (an
     unexported `Expr` field, not a name).
   * User functions, `SECURITY DEFINER` ones included, would run through
     this path. It is a bug today and becomes a privilege escalation with
     this design.
2. **The read-only check.** It moves into `execStmt`, so that every
   nested statement is checked.
3. **Savepoint cost.** `kvclient.Txn` records the writes made after each
   savepoint, so that `Savepoint` is O(1) and rolling back touches only
   those writes.
4. **Error CONTEXT.** `sql.Error` gains `Where`, the wire layer sends
   field `W`, and notices gain a code, detail and hint.

## Goals and constraints

1. **PostgreSQL semantics and SQLSTATEs** for the statements and
   behaviours in scope. Where datax cannot match, it refuses with
   `0A000` rather than doing something different. The deliberate
   differences are listed where they arise.
2. **No regression for statements that call no user function.** Builtin
   calls pay no catalog read. Statements on tables with no triggers take
   today's paths unchanged.
3. **Never run the wrong code.** No name resolution may let an object
   created later capture a stored reference or a definer function's body.
   That is the rule `docs/design/schemas.md` set for views.
4. **pg_dump round-trips.** A database with functions and triggers dumps
   with pg_dump 16 and restores into datax, and a second dump is identical.
5. **Mixed versions are safe.** Nothing is created until v23 is
   finalized, and older binaries refuse backups that carry the new objects.

## Scope and phasing

### Phase 1: SQL functions

| In | Out (refused `0A000`, unless noted) |
|---|---|
| `CREATE [OR REPLACE] FUNCTION ... LANGUAGE sql AS '...'` (any quoting) | `LANGUAGE plpgsql` (Phase 2); `c`, `internal` and others (`42704 language "x" does not exist`, as PostgreSQL) |
| Scalar returns of any column type, including enums; `RETURNS void` | `RETURNS record`, a composite / table type outside `FROM`, `RETURNS trigger` (PostgreSQL: `42P13 SQL functions cannot return type trigger`) |
| `RETURNS SETOF type`, `RETURNS TABLE (...)`, `RETURNS SETOF tbl`, called in `FROM` (and in a FROM-less select list, as builtin SRFs are today) | A set-returning call in the select list of a query with `FROM` (as for builtins, PG-Q-004) |
| `IN` parameters, named or not; `DEFAULT` / `=` defaults; calls by position, by name (`=>`, `:=`) or mixed | `OUT` / `INOUT` (other than `RETURNS TABLE`), `VARIADIC`, polymorphic types (`anyelement` ...): Phase 4 |
| `IMMUTABLE` / `STABLE` / `VOLATILE`, `STRICT` / `RETURNS NULL ON NULL INPUT` / `CALLED ON NULL INPUT`, `SECURITY DEFINER` / `INVOKER`, `SET param = value` / `FROM CURRENT` | `SUPPORT`, `TRANSFORM`, `WINDOW`. `COST`, `ROWS`, `PARALLEL` and `LEAKPROOF` are accepted, stored, shown and ignored (`LEAKPROOF` needs an admin, as in PostgreSQL) |
| Bodies: one or more `SELECT` / `VALUES` / `INSERT` / `UPDATE` / `DELETE` (with `RETURNING`), `;`-separated; arguments as `$n` or by name | SQL-standard bodies (`RETURN expr`, `BEGIN ATOMIC ... END`); transaction control, `SAVEPOINT`, DDL and `COPY` in a body |
| Overloading across names, arity and type families | Overloads whose argument types fall in the same family (`f(int4)` and `f(int8)`): see "Identity and overloads" |
| `DROP FUNCTION [IF EXISTS] name[(args)] [, ...] [CASCADE \| RESTRICT]` | — |
| `ALTER FUNCTION ... RENAME TO / OWNER TO / SET SCHEMA / volatility / [NOT] STRICT / SECURITY / SET / RESET / COST / ROWS / PARALLEL` | `ALTER FUNCTION ... DEPENDS ON EXTENSION` |
| `GRANT / REVOKE EXECUTE ON FUNCTION f(args) / ALL FUNCTIONS IN SCHEMA s`; `ALTER DEFAULT PRIVILEGES ... ON FUNCTIONS`; `ROUTINE` as a synonym | `GRANT ... ON PROCEDURE` (PG-P-004) |
| `COMMENT ON FUNCTION` | — |
| User functions in DEFAULTs, CHECKs, views and `RETURNING`; in row-level-security policies when `IMMUTABLE` or `STABLE` (`rls.go` keeps functions with effects out of policies through `policyFuncsAllowed`, and a volatile user function is refused the same way, `0A000`); in index expressions and partial-index predicates only when the call is inlined (see "Inlining") | Non-inlinable calls in index expressions and partial-index predicates (index maintenance and backfill have no session): Phase 4, `0A000` until then |
| `pg_proc`, `pg_language`, `\df[+]`, `\sf`, `pg_get_functiondef`, `pg_get_function_arguments / _identity_arguments / _result`, `regproc` / `regprocedure`, `to_regproc` / `to_regprocedure`, `has_function_privilege`, `information_schema.routines` / `.parameters`, `pg_depend` rows for the new dependencies | — |

### Phase 2: PL/pgSQL and `DO`

This phase adds `LANGUAGE plpgsql` functions (scalar, `SETOF`,
`RETURNS TABLE`, `void`) and `DO [LANGUAGE plpgsql] $$ ... $$`. The
supported subset is listed under "PL/pgSQL". It leaves out:

* cursors (`OPEN` / `FETCH` / `refcursor`);
* `COMMIT` / `ROLLBACK` (procedures only, PG-P-004);
* `RETURNS trigger`, which is Phase 3;
* `plpgsql.variable_conflict` other than `error`;
* `#option` and `#print_strict_params`;
* `GET DIAGNOSTICS PG_CONTEXT`.

`DO` comes with this phase because migration scripts use it for
idempotent DDL, and once the interpreter exists it costs almost nothing.

### Phase 3: triggers on tables

* `CREATE [OR REPLACE] TRIGGER`:
  * `BEFORE` / `AFTER`;
  * `INSERT` / `UPDATE [OF cols]` / `DELETE` / `TRUNCATE`;
  * `FOR EACH ROW` / `FOR EACH STATEMENT`;
  * `WHEN (...)`;
  * `EXECUTE FUNCTION | PROCEDURE f(args)`.
* PL/pgSQL trigger functions: `NEW`, `OLD`, the `TG_*` variables,
  `TG_ARGV`, and the return-value rules.
* `DROP TRIGGER [IF EXISTS] ... ON t [CASCADE | RESTRICT]` and
  `ALTER TRIGGER ... RENAME TO`.
* `ALTER TABLE ... ENABLE / DISABLE [REPLICA | ALWAYS] TRIGGER name | ALL | USER`.
* `session_replication_role` with effect, which then needs an admin.
* The `TRIGGER` table privilege.
* `pg_trigger`, `information_schema.triggers`, `pg_get_triggerdef`,
  `pg_trigger_depth()`, and psql `\d t`.

Triggers fire from `INSERT` (including `ON CONFLICT` and `UPSERT`),
`UPDATE` (including `FROM` and primary-key changes), `DELETE` (including
`USING`), `COPY FROM`, `TRUNCATE`, and foreign-key cascades.

This phase leaves out (`0A000`):

* `INSTEAD OF` triggers on views;
* transition tables (`REFERENCING`);
* constraint triggers and `DEFERRABLE` (needs PG-D-011);
* triggers on views and on system tables.

### Phase 4: later

* `INSTEAD OF` row triggers on views. These make views writable, using
  the view's query for the target rows.
* Transition tables. The AFTER queue already holds the rows; they would
  be bound as a relation with `s.bindRelation`, as a `WITH` member is.
* User functions in index expressions.
* `OUT` / `INOUT` / `VARIADIC` / polymorphic signatures.
* SQL-standard bodies.
* Procedures and `CALL` (PG-P-004).
* A gateway cache for function descriptors.

## Catalog model

### Function descriptors

A function is a `FunctionDescriptor` (new, `pkg/sql/catalog/function.go`),
stored as JSON:

```
/system/funcdesc/<id>                                → FunctionDescriptor
/system/funcns/<dbID>/<schemaID>/<name>/<identity>   → id
```

The descriptor holds:

* `id`, `database_id`, `schema_id` (0 = `public`), `name`, `owner`;
* `params`: name, mode, type and default text for each;
* `returns`: type, `setof`, and the columns for `RETURNS TABLE`;
* `language`, `body`, `volatility`, `strict`, `security_definer`;
* `config`, the `SET` clauses;
* `cost`, `rows`, `parallel`, `leakproof`;
* `privileges` and `grant_options`;
* `comment`, `version`, and `depends` (type IDs).

Every field after the first release is `omitempty`, under
`pkg/version` rule 1.

* **IDs** come from `keys.DescIDGenKey`, the counter tables, sequences,
  types and schemas share. So the ID is unique among every descriptor
  kind, and it is the function's `pg_proc` OID. Builtins keep their OIDs
  at `1<<28` and above, far above any ID.
* **Types** are stored with the type fields `catalog.Column` uses (family,
  width, enum ID and name, `NoTZ`, `Char`), so that `coerceColumn` coerces
  arguments and results. Typmods are dropped at `CREATE`, as PostgreSQL
  does: `f(varchar(3))` is stored, shown and called as
  `f(character varying)`.
* **The body** is stored as text. Its parse (and for PL/pgSQL its
  compiled tree) is cached on each gateway by `(id, version)`. Both are
  immutable for a version, so the cache needs no invalidation beyond the
  key.
* **Namespace.** Functions have their own namespace in each schema, apart
  from relations and types, as `pg_proc` is apart from `pg_class`. A
  function may share a name with a table. The key is keyed by schema
  **ID**, so `ALTER SCHEMA ... RENAME` moves nothing (the reasoning is in
  `docs/design/schemas.md`). The temporary schema holds no functions.
* **The datax database fallback does not apply.** `app.f()` never means
  "function `f` of database `app`". `db.s.f()` with another database is
  `0A000 cross-database references are not implemented`, as for tables.

### Identity and overloads

PostgreSQL's identity is the name plus the input argument types
(`sql-createfunction`, "Overloading"). The key's `<identity>` is the
canonical type names of the `IN` parameters, as `format_type` renders
them (`integer,text`), so that `pg_get_function_identity_arguments`,
`DROP FUNCTION f(integer)` and pg_dump all agree.

datax cannot tell `int2`, `int4` and `int8` apart at a call (one family),
and likewise `text` and `varchar`, or `timestamp` and `timestamptz`. So
`CREATE FUNCTION` refuses an overload whose parameter **families** equal
an existing overload's:

```
0A000 function f(bigint) cannot be created: datax cannot distinguish it from f(integer) at a call
```

The identities still differ: `f(integer)` and `f(bigint)` are distinct
names that `DROP`, `GRANT` and `COMMENT` address. They simply cannot
coexist. Creating the same identity twice is
`42723 function "f" already exists with same argument types`.

### Dependencies

The new kinds share one reverse-record keyspace:

```
/system/dep/<referencedID>/<dependentID>  → JSON {kind, sub}
```

IDs are unique across descriptor kinds, so the referenced ID alone names
the object. Each record is written and removed in the transaction that
creates, alters or drops the dependent, as the view records are. There
are no records from before v23 to backfill, so no "complete" marker is
needed (unlike `ViewDependencyIndexKey`). The same records feed
`pg_depend`.

| Dependent → referenced | Recorded | On `DROP` of the referenced object (PostgreSQL: `sql-dropfunction`, `ddl-depend`) |
|---|---|---|
| Function → enum type of a parameter or result | yes | `2BP01 cannot drop type x because other objects depend on it`. `CASCADE` drops the function |
| Function → schema | via `schema_id` | `DROP SCHEMA` counts functions as contents. `CASCADE` drops them |
| Function → database, owner | via fields | `DROP DATABASE` drops them. `DROP ROLE` refuses an owner. `DROP OWNED` / `REASSIGN OWNED` include functions |
| Function body → tables, columns, other functions | **no** | Nothing. The call fails when it runs. PostgreSQL tracks no dependencies for string bodies either (`sql-createfunction`, notes on `sql_body`) |
| Column default → function | yes (`sub` = column ID) | `2BP01 ... default value for column c of table t depends on function f(integer)`. `CASCADE` drops the default |
| CHECK constraint → function | yes | `CASCADE` drops the constraint |
| View → function | yes | `CASCADE` drops the view (through `DROP VIEW ... CASCADE`) |
| Policy → function | yes | `CASCADE` drops the policy |
| Trigger → function | yes (`sub` = trigger ID) | `CASCADE` drops the trigger |
| Trigger → its table | in the table descriptor | Dropping the table drops its triggers |
| Trigger → columns of `UPDATE OF` and `WHEN` | `sub` on the table | `DROP COLUMN` is `2BP01`, and `CASCADE` drops the trigger. Renaming such a column is refused (`2BP01`), as for a column a view reads, because `WHEN` is stored as text |

`DROP FUNCTION ... RESTRICT` (the default) lists dependents in the
`DETAIL`, one per line, with `HINT: Use DROP ... CASCADE to drop the
dependent objects too.`, as PostgreSQL does. `CASCADE` runs the dependent
drops through their own statements' code (`DROP VIEW`, `ALTER TABLE ...
DROP CONSTRAINT / ALTER COLUMN DROP DEFAULT`, `DROP POLICY`,
`DROP TRIGGER`). That way their drains and records are handled as usual,
and every table touched is added to `extraDDL` for the post-commit drain.

`ALTER FUNCTION ... RENAME` and `SET SCHEMA` are refused (`2BP01`) while
a stored expression references the function by name, because the stored
text would no longer resolve. Triggers bind by ID and do not block a
rename.

### Triggers in the table descriptor

```go
// TableDescriptor
Triggers []Trigger `json:"triggers,omitempty"`

type Trigger struct {
    ID        uint64   // from DescIDGenKey: its pg_trigger OID
    Name      string
    FuncID    uint64   // bound by ID: CREATE OR REPLACE FUNCTION keeps it
    Args      []string // TG_ARGV
    Timing    string   // before | after
    Events    []string // insert | update | delete | truncate
    Columns   []ColumnID // UPDATE OF
    ForEach   string   // row | statement
    When      string   // SQL text; may reference OLD. and NEW.
    WhenBindings map[string]uint64 // function IDs the WHEN text calls
    Enabled   string   // O | D | R | A, pg_trigger.tgenabled
}
```

A trigger change is a table-descriptor change, so it bumps `Version`,
goes through the lease cache, and is drained after commit (`ddlTableName`
/ `extraDDL`). That is exactly the guarantee that "every gateway now
plans with it" needs. Trigger names are unique per table (`42710 trigger
"x" for relation "t" already exists`).

## Statements

Each statement lists its PostgreSQL behaviour first, then datax's
specifics.

* `CREATE [OR REPLACE] FUNCTION name ( [ [IN] [argname] argtype [ { DEFAULT | = } expr ] [, ...] ] ) { RETURNS rettype | RETURNS TABLE (col type, ...) } { LANGUAGE lang | volatility | [NOT] LEAKPROOF | { CALLED | RETURNS NULL } ON NULL INPUT | STRICT | [EXTERNAL] SECURITY { INVOKER | DEFINER } | PARALLEL x | COST n | ROWS n | SET param { TO value | = value | FROM CURRENT } | AS 'body' } ...`
  * **Privileges.** `CREATE` on the target schema (`42501 permission
    denied for schema s`); the creator becomes the owner.
  * **Where it is created.** As for tables: the first existing schema of
    the path, or `3F000`.
  * **`OR REPLACE`.** It keeps the ID, owner and privileges. It refuses a
    changed return type (`42P13 cannot change return type of existing
    function`, `HINT: Use DROP FUNCTION f(integer) first.`), a renamed
    input parameter, or a removed default (`42P13`), and it requires
    ownership (`42501 must be owner of function f`). A replaced
    function's version bumps, so cached parses stop being used.
  * **Validation with `check_function_bodies = on`.** The body is parsed,
    `$n` must be in range, a referenced argument name must exist, and the
    final statement's columns must match the return type
    (`42P13 return type mismatch in function declared to return integer`,
    `DETAIL: Actual return type is text.`). Tables named in the body are
    not resolved.
  * **With `off`**, which pg_dump sets, the body is stored unparsed and
    errors surface at the first call, as in PostgreSQL.
  * **Default expressions** are validated like column defaults
    (`validateDefaultExpr`): no column references, no subqueries
    (`0A000`).
* `DROP FUNCTION [IF EXISTS] name [ ( [ [argmode] [argname] argtype [, ...] ] ) ] [, ...] [CASCADE | RESTRICT]`.
  * Without an argument list the name must be unique in its schema;
    otherwise `42725 function name "f" is not unique`,
    `HINT: Specify the argument list to select the function unambiguously.`
  * A missing function is `42883 function f(integer) does not exist`.
    With `IF EXISTS` it is a notice.
* `ALTER FUNCTION f(args) ...`. Owner or admin; `OWNER TO` also needs
  membership in the new role, as for tables.
* `GRANT / REVOKE { EXECUTE | ALL [PRIVILEGES] } ON { FUNCTION | ROUTINE } f(args) [, ...] | ALL FUNCTIONS IN SCHEMA s TO ...`
  with grant options. Also `ALTER DEFAULT PRIVILEGES [FOR ROLE r] { GRANT | REVOKE } EXECUTE ON FUNCTIONS ...`
  (a new object kind beside `TABLES` and `SEQUENCES` in
  `DatabaseDescriptor.DefaultPrivileges`).
* `DO [LANGUAGE plpgsql] 'code'` (Phase 2). It runs as an anonymous
  `void` function, with no privilege needed beyond those of what it does.
* `CREATE [OR REPLACE] TRIGGER name { BEFORE | AFTER } event [OR ...] ON table [FOR [EACH] { ROW | STATEMENT }] [WHEN (cond)] EXECUTE { FUNCTION | PROCEDURE } f(args)` (Phase 3).
  * **Privileges.** `TRIGGER` on the table, or ownership, and `EXECUTE`
    on the function (`sql-createtrigger`).
  * **Function.** It must return `trigger`
    (`42P17 function f must return type trigger`).
  * **Target.** A view is `42809 "v" is a view` with
    `DETAIL: Views cannot have row-level BEFORE or AFTER triggers.`
  * **WHEN.** Statement-level `WHEN` may not reference columns
    (`0A000`). `OLD` in an `INSERT` trigger's `WHEN` is `42P17`.
* `ALTER TABLE t { ENABLE [REPLICA | ALWAYS] | DISABLE } TRIGGER { name | ALL | USER }`:
  see "Enabling and disabling".

## Resolution

### Parser

* `name(`, `s.name(` and `db.s.name(` are calls. `parser.Expr` gains
  `FuncSchema` and `FuncDB` (omitempty), and `ArgNames []string` for
  named notation. `pg_catalog.f(` keeps working; it now sets `FuncSchema`
  instead of being dropped.
* **The builtin arity check moves to resolution.** A call no candidate
  accepts is `42883 function lower(text, text) does not exist` with
  PostgreSQL's `HINT: No function matches the given name and argument
  types. You might need to add explicit type casts.` That is the code
  PostgreSQL gives. Today it is `42601`; no test asserts the old text.
* `FROM name(...)`, `FROM s.name(...)` and `LATERAL` forms parse as table
  functions for any name. `isTableFunc` is no longer consulted. Whether
  the function returns a set is decided at resolution. A scalar function
  in `FROM` is a one-row table, as in PostgreSQL and as datax already
  treats scalar builtins there.
* Aggregate and window names (`aggNames`, `windowNames`) keep their
  meaning. A user function named `count` is reachable only qualified,
  until `CREATE AGGREGATE` exists (PG-D-027).

### Candidates and precedence

PostgreSQL rules (`runtime-config-client`, `search_path`; `typeconv-func`):

* `pg_catalog` is searched first unless the path names it.
* `pg_temp` is never searched for functions.
* Among same-signature functions the earliest in the path wins.
* Functions with different signatures in different schemas compete on
  an equal footing.

datax's builtins are the `pg_catalog` candidates. Each is one candidate
with its declared families. For an unqualified call `f(args)`:

1. **Builtin first.** If `f` is a builtin, `pg_catalog` comes before the
   path's other schemas, and the builtin accepts the arguments (arity,
   and the families coerce), the builtin is chosen **without reading the
   catalog**. This is the hot path, and it costs what it costs today.
2. **Otherwise** the user functions named `f` are read from each schema
   of the path that the actor may `USAGE` (`catalog.Resolution.CanUse`).
   This is one prefix scan of `/system/funcns/<db>/<schema>/f/` per
   schema, sent as one batch and memoized per statement. They join the
   builtin, if any, and the best match is chosen (below).
3. **Qualified** `s.f(...)` reads only schema `s`. `pg_catalog.f` means
   the builtin.

**Deliberate difference.** Suppose a user function has a builtin's name
and arity and the builtin also accepts the call: `public.lower(integer)`
called as `lower(5)`. PostgreSQL may choose the user function, because
an exact type match beats the builtin's. datax chooses the builtin
(lenient coercion makes it accept `5`). Making this exact would need
every builtin call to read the catalog, or a gateway-cached set of
user-function names kept current by leases (Phase 4). The case is rare,
and the user function stays reachable qualified (`public.lower(5)`).
This goes in `docs/user/postgres-differences.md`.

### Choosing an overload

This is PostgreSQL's algorithm (`typeconv-func`, steps 1–4) applied to
families. An argument's family is static: `exprFamily` (`typing.go`) with
the scope's column types. Untyped literals, `NULL` and parameters are
**unknown**. Resolution therefore depends on the call site, never on a
row's values.

1. Keep candidates whose arity matches after defaults and named
   arguments. Every named argument must name a parameter, and positional
   arguments must come first (`42601`).
2. **Exact.** A candidate whose every known argument family equals its
   parameter's family wins if it is the only one.
3. **Implicit coercion.** Keep candidates every known argument coerces
   to: `Int → Decimal → Float`, `Date → Timestamp`, an unknown to
   anything, `text` to an enum only from an unknown literal. Prefer the
   most exact matches, then PostgreSQL's preferred types (`Float` among
   numerics, `String` for unknowns).
4. One candidate left: it. None: `42883` with the hint. Several:
   `42725 function f(unknown) is not unique`,
   `HINT: Could not choose a best candidate function. You might need to add explicit type casts.`

For prepared statements, a parameter's type is inferred from the chosen
overload (`paramtypes.go`). When the choice depends on an unknown
parameter, describing the statement fails `42725`, as in PostgreSQL.

### Stored expressions bind by ID

A view's query, a default, a CHECK, a policy or a trigger `WHEN` that
calls a user function records the function it resolved to. This mirrors
`ViewBindings` for relations: a `FuncBindings` map from the call as
written (name and arity) to the function ID. Each also writes a `/system/dep`
record. When the expression is used again, the call binds to exactly that
ID; it does not re-resolve through the reader's or inserter's path.

Without this, a role able to create a function earlier on someone's path
could capture `DEFAULT gen_id()` for every inserter, or capture a view's
call, which runs with the owner's privileges. Builtins are bound by name,
since their OIDs are not stable. Expressions stored before v23 bind no
user function, because none existed.

## Execution

### A call in an expression

The resolve pass (`resolveValueNode`) rewrites a resolved user-function
call:

* **Inlinable** (below): to the body's expression with the arguments
  substituted.
* **`IMMUTABLE` or `STABLE`, every argument constant:** evaluated once,
  then spliced as a literal. The access planner then sees
  `col = <value>` and can use an index, as it does for `now()`.
* **Otherwise:** to a per-row closure (`registerRowFunc`, hardened as
  described above). `evalFunc` calls the closure with the evaluated
  arguments.

The closure runs the body as follows:

1. **Strict and NULL.** If the function is strict and an argument is
   NULL, the result is NULL (or no rows) and the body never runs.
2. **Arguments.** Coerce each to its parameter's type
   (`coerceColumn`); a failure is `22P02` and the like, as an assignment
   would give.
3. **Session state.** Save the session's nested-execution state:
   `s.rels`, `stmtLookups`, `viewPath`, `viewBindings`, `privAs`, the
   effective `current_user`, and the explain state. Clear `s.rels`.
   * `SECURITY DEFINER`: set `privAs` and the effective `current_user`
     to the owner.
   * `SET search_path`: set `viewPath` to it. As inside a view, this
     skips the temporary tables unless the path names `pg_temp`, and
     then they resolve at `pg_temp`'s position.
   * Other `SET` clauses: snapshot `s.vars`, apply them, and restore on
     exit, as `SET LOCAL` restores (`vars.go`).
4. **Statements.** Run each statement of the cached parse with
   `s.execStmt(ctx, txn, stmt, args)`. Arguments are `$n`, and argument
   names were rewritten to `$n` when the body was parsed, except where a
   column of the statement's tables has that name: the column wins, as in
   PostgreSQL (`xfunc-sql`). `f.arg` qualifies an argument.
5. **Result.** The last statement's first row (scalar; no row is NULL)
   or all its rows (set), coerced to the return type (`42804` on
   mismatch). Restore the state.

A set-returning function in `FROM` runs the same way once, or once per
outer row under `LATERAL`, through `execTableFunc`. It yields
`ResultColumn`s from `returns`.

The function's `EXECUTE` privilege, and `USAGE` on its schema, are
checked once per statement at resolution: `42501 permission denied for
function f`.

### Inlining

These are PostgreSQL's conditions (wiki "Inlining of SQL functions";
`xfunc-optimization`), and datax applies them at the resolve pass.

**Scalar.** All of these must hold:

* `LANGUAGE sql`, not `SECURITY DEFINER`, no `SET` clauses;
* the body is one `SELECT expr` with no `FROM`, aggregate, window,
  subquery or set-returning call;
* the body's volatility is no greater than the declared one;
* if `STRICT`, the expression must be strict in every parameter;
* a parameter used more than once must not have a volatile argument.

The call becomes the expression, with the argument *expressions*
substituted for `$n`. This matters more in datax than in PostgreSQL:

* an inlined call costs no `execStmt`;
* the planner sees through it (`WHERE f(col) = 1` with an inlinable `f`
  can match an expression index on the inlined expression);
* it is legal wherever an expression is, index expressions included.
  That is the one route by which Phase 1 allows user functions in an
  index: the index stores the inlined text and records no dependency on
  the function. A later `OR REPLACE` therefore does not change such an
  index. PostgreSQL gives the same caveat for any immutable function in
  an index: its existing entries are not recomputed when the function
  is replaced.

**Table functions in `FROM`.** All of these must hold:

* `STABLE` or `IMMUTABLE`, not strict, not definer, no `SET`;
* the body is one `SELECT`;
* the arguments contain no volatile functions.

The call becomes a derived table: the `SELECT` with the arguments
substituted. It then gets the derived-table and lateral machinery, with
its current limits (PG-Q-004). When the substituted query falls outside
those limits, the call is executed instead of inlined.

**Plan cache.** Every statement that calls a user function, inlined or
not, bypasses the plan cache (`plancache.go`). The cache invalidates by
descriptor-pointer identity, and functions are not cached descriptors.

### Volatility

These are PostgreSQL's categories (`xfunc-volatility`). Datax applies
each as follows:

| | Folded once per statement with constant arguments | Per row otherwise | May write | Allowed in an index expression, partial predicate or CHECK |
|---|---|---|---|---|
| `IMMUTABLE` | yes, and at `CREATE INDEX` time when inlined | yes | no (`0A000 INSERT is not allowed in a non-volatile function`, when the statement runs) | index: only if inlinable (Phase 1). CHECK: yes |
| `STABLE` | yes | yes | no (same error) | CHECK only. PostgreSQL does not check CHECKs for volatility; neither does datax |
| `VOLATILE` | **no**, never the `spliceVolatile` once-per-statement rule | yes | yes | CHECK only |

In PostgreSQL, `STABLE` and `IMMUTABLE` functions see the snapshot of the
calling statement, while `VOLATILE` ones see each of their statements'
own changes. datax reads its own transaction's writes once they are
flushed. Statements inside a function run in order, and each flushes its
batch before the next, so a volatile function sees its earlier
statements' effects. The calling statement's own buffered writes are not
flushed until it ends. This matches the behaviour, given the next rule.

**Index expressions** with user functions require `IMMUTABLE`:
`42P17 functions in index expression must be marked IMMUTABLE`, the
message the builtins already use.

### The caller's transaction

A function always runs in the caller's transaction: there are no
autonomous transactions. The caller's context carries over:

* **Retries.** An implicit statement that fails with `40001` is re-run
  whole, functions included. Notices raised during the failed attempt are
  discarded with it (`discardNotify`), so a retried statement does not
  repeat them.
* **Read-only.** In a read-only transaction, or `AS OF SYSTEM TIME`,
  writes inside functions are `25006` (once the check moves into
  `execStmt`).
* **Streaming.** A statement that calls a user function that is not
  inlined is not streamed (`executeStreamingSelect`). Its rows are
  materialized, so a retry stays transparent and no write happens while
  the client pulls.
* **Timeouts.** `statement_timeout` and cancellation cover the whole top
  level statement, including nested work, through the context.
* **Commit deadline.** Descriptor leases give the transaction a commit
  deadline. A long-running function can pass it and fail with `40001`,
  which a retry then fixes (the deadline mechanism is in `docs/sql.md`).

### Recursion and runaway work

The executor is single-goroutine and recursive. A Go stack overflow is
fatal and `recover` cannot catch it (the reason for the parser's
`maxParseDepth`, issue #135). So nesting is counted explicitly.

* **Depth.** The session counts function and trigger nesting. Above
  the limit the call fails with
  `54001 stack depth limit exceeded` and
  `HINT: datax limits nested function and trigger calls to N.`
  (PostgreSQL's code; its limit is a stack size, `max_stack_depth`). N
  defaults to 256 and is raised only as far as a test shows it fits
  comfortably within Go's 1 GB default goroutine stack. Recursive SQL and
  PL/pgSQL functions, and triggers that fire triggers, all count.
* **Loops.** `statement_timeout`, cancellation (`57014`) and
  `idle_in_transaction_session_timeout` apply as to any statement.
  PL/pgSQL checks the context at each loop iteration and each statement.
* **Memory.** `RETURN NEXT` / `RETURN QUERY` and `FOR ... IN query` are
  capped at the set-operation row cap (`setOpRowCap`, 1,000,000 rows):
  `54000 ... exceeds the 1000000-row limit`. Foreign-key cascades keep
  `foreign_key_cascade_limit`.

### Errors and SQLSTATEs

These follow PostgreSQL's `errcodes-appendix`; the wording is
PostgreSQL's.

| Situation | SQLSTATE |
|---|---|
| No such function or signature; wrong arity for a builtin | `42883` + hint |
| Ambiguous call or unqualified `DROP` | `42725` |
| Duplicate signature | `42723` |
| Same-family overload (datax) | `0A000` |
| Bad definition: return mismatch, `OR REPLACE` changes, `RETURNS trigger` in SQL | `42P13` |
| Non-immutable function in an index expression; trigger function not returning `trigger` | `42P17` |
| Dependents on `DROP` | `2BP01` |
| No `EXECUTE`, no `TRIGGER`, not owner | `42501` |
| Write in a non-volatile function | `0A000` |
| Write in a read-only transaction | `25006` |
| Nesting limit | `54001` |
| Return value cannot be coerced | `42804` |
| PL/pgSQL: `RAISE EXCEPTION` (default), `no_data_found`, `too_many_rows`, `ASSERT` | `P0001`, `P0002`, `P0003`, `P0004` |
| PL/pgSQL: end of function without `RETURN` | `2F005` |
| PL/pgSQL: NULL into a `NOT NULL` variable | `22004` |
| Trigger: undefined / duplicate | `42704` / `42710` |
| Trigger: returned row does not match the table | `42804` |

An error raised inside a function carries a CONTEXT line (the new `Where`
field). Examples: `SQL function "f" statement 1` and
`PL/pgSQL function f(integer) line 3 at RAISE`.

## PL/pgSQL (Phase 2)

### Architecture

The interpreter is a new package, `pkg/sql/plpgsql`. It does not import
`pkg/sql`, which imports it, and it reaches the session through an
interface:

```go
type Host interface {
    Exec(ctx context.Context, stmt parser.Statement, params []types.Datum) (*Result, error) // execStmt, nested
    Eval(ctx context.Context, e parser.Expr, params []types.Datum) (types.Datum, error)     // resolve pass + evalExprEnv
    Savepoint(ctx context.Context) (release, rollback func(context.Context) error, err error)
    Notice(level string, n Notice)
    Check(ctx context.Context) error // cancellation, timeout, depth
}
```

* **Parse.** At `CREATE` (with `check_function_bodies`) or at first call,
  the block structure is parsed by its own small grammar. Each embedded
  SQL statement or expression is handed to `pkg/sql/parser`, which
  exports a tokenizer, so the PL grammar can find statement ends and
  `INTO` targets on the same tokens. This is how PostgreSQL's
  `pl_gram.y` works.
* **Variable substitution.**
  * A reference to a variable becomes a `Param` node into a per-call
    parameter vector. Record fields (`r.x`, `NEW.x`) become one parameter
    each.
  * Block labels and the function name qualify variables.
  * A name that is both a variable and a column of the statement's
    tables is `42702 column reference "x" is ambiguous`, with PostgreSQL's
    detail. That is `plpgsql.variable_conflict = error`, the default
    (`plpgsql-implementation`). It is checked when a statement first runs
    against its tables' descriptors.
* **Compile.** A tree of statement nodes. It is cached per gateway by
  `(function id, version)`, and per call the only state is the variable
  vector.
* **Simple expressions.** An expression with no table, subquery or
  set-returning call (`x := y + 1`, `NEW.updated_at := now()`,
  `IF NEW.a IS DISTINCT FROM OLD.a`) is evaluated with `Host.Eval`
  directly, never through `execStmt`. PostgreSQL has the same fast path
  (`exec_eval_simple_expr`). It is what makes a per-row `updated_at`
  trigger cheap.
* **Records.** `RECORD`, `%ROWTYPE`, `NEW` and `OLD` are interpreter
  values: column names, families and datums. They are never SQL values,
  since datax has no composite type, so a record cannot be passed to a
  SQL function or returned as a scalar (`0A000`).

### The subset

| In Phase 2 | Out |
|---|---|
| `DECLARE` with types, `%TYPE`, `%ROWTYPE`, `RECORD`, `CONSTANT`, `NOT NULL`, `DEFAULT` / `:=`; `ALIAS FOR $n` | `refcursor`, cursor declarations |
| `:=` / `=` assignment, including to record fields | Assignment to array elements or slices (`a[1] := x`) |
| `IF / ELSIF / ELSE`, `CASE` (both forms, `20000 case not found`), `LOOP`, `WHILE`, `FOR i IN [REVERSE] a..b [BY n]`, `FOR r IN query`, `FOR r IN EXECUTE ...`, `FOREACH x IN ARRAY`, `EXIT` / `CONTINUE [label] [WHEN]`, labels, nested blocks | `FOREACH ... SLICE` |
| `RETURN`, `RETURN NEXT`, `RETURN QUERY`, `RETURN QUERY EXECUTE` | — |
| `RAISE level 'fmt', args USING ERRCODE / MESSAGE / DETAIL / HINT`; `RAISE` (rethrow); `ASSERT` | `RAISE ... USING COLUMN / CONSTRAINT / ...` (accepted, not sent) |
| `PERFORM`; `SELECT / INSERT / UPDATE / DELETE ... INTO [STRICT] target`; `GET [CURRENT] DIAGNOSTICS x = ROW_COUNT`; `FOUND` | `GET DIAGNOSTICS ... PG_CONTEXT` |
| `EXECUTE 'sql' [INTO [STRICT] t] [USING ...]`, with `format()` / `quote_*` | — |
| `BEGIN ... EXCEPTION WHEN cond [OR cond] THEN ...`, condition names and `SQLSTATE 'xxxxx'`, `SQLSTATE` / `SQLERRM`, `GET STACKED DIAGNOSTICS` (`RETURNED_SQLSTATE`, `MESSAGE_TEXT`, `PG_EXCEPTION_DETAIL`, `PG_EXCEPTION_HINT`) | `PG_EXCEPTION_CONTEXT` (empty) |
| — | `COMMIT` / `ROLLBACK` (`2D000 invalid transaction termination`), `OPEN` / `FETCH` / `MOVE` / `CLOSE`, `#variable_conflict` options |

**`FOR r IN query` materializes the query** (up to the row cap) before the
first iteration. PostgreSQL reads through a cursor whose snapshot is fixed
when the loop opens, so the loop body's writes are invisible to the loop.
A lazy datax reader would see its own transaction's new writes. So
materializing is what reproduces PostgreSQL's result.

**`EXECUTE` and DDL.** `EXECUTE` and static statements may run DDL. It
runs under the rules of a transaction block (`docs/sql.md`):

* descriptor-only DDL runs, and the tables it touches join `extraDDL`
  and are drained after commit;
* multi-transaction DDL is `25001`, as inside `BEGIN`: `CREATE INDEX` on
  an existing table, online `ADD CONSTRAINT`, `SET NOT NULL`, and an
  `ADD COLUMN` that backfills.

`DO` blocks that wrap such DDL fail the same way. They run as written
only once those statements can run in a block (PG-D-031).

### Exceptions and savepoints

PostgreSQL runs a block with an `EXCEPTION` clause in a subtransaction
(`plpgsql-control-structures`, "Trapping Errors"). datax uses the
transaction's savepoints. On entry to such a block, `Host.Savepoint`
establishes an internal savepoint (a name no SQL can spell).

* **The block ends normally.** The savepoint is released.
* **The block raises an error a handler matches.**
  * The transaction rolls back to the savepoint.
  * The session undoes its side effects since then:
    * notifications, advisory locks and large objects, using the hooks
      `execRollbackToSavepoint` already calls (factored into one
      function);
    * pending drains: `extraDDL`, `extraDrops` and `pendingWipes` are
      truncated to their length at entry;
    * the block's created-tables list.
  * The handler runs. Variables keep the values they had when the error
    was raised, as in PostgreSQL.
* **No handler matches.** The error propagates, and the outer blocks get
  the same treatment in turn.

**What is never caught.** Retryable errors (`40001`, and a refresh or
deadline failure) are never caught, not even by `WHEN OTHERS` or
`WHEN serialization_failure`. Once the KV layer has aborted or restarted
the transaction, nothing after the savepoint is valid
(`execRollbackToSavepoint` already refuses to rescue such a transaction).
This differs from PostgreSQL, where SSI errors can be caught. Here they
always reach the client, or the implicit-statement retry loop.
`query_canceled` (`57014`) and `assert_failure` (`P0004`) are not caught
by `OTHERS`, as in PostgreSQL.

Savepoints must be cheap first (see "Prerequisites"). Otherwise a loop
whose body has an `EXCEPTION` clause, the common "insert, on
unique_violation update" idiom, costs O(n²) in the transaction's writes.

### Serializable retries

Every datax transaction is serializable, so a PL/pgSQL function that
reads and then writes is more exposed to `40001` than under PostgreSQL's
default READ COMMITTED.

* **Implicit statements** (`SELECT f()`, a `DO` block, a trigger fired by
  an implicit `INSERT`) retry transparently, whole.
* **Explicit transactions** see `40001` and retry in the client, which
  is today's rule (PG-X-001).
* **Side effects that do not roll back** happen again on each retry:
  `nextval` (as in PostgreSQL), and session-level advisory locks taken
  with `pg_advisory_lock`, which are re-entrant counts. The user doc says
  so.

## Triggers (Phase 3)

### Firing model

This follows PostgreSQL's `trigger-definition` and `trigger-datachanges`,
for one statement on table `t`:

1. **BEFORE STATEMENT** triggers run, in name order. They fire even if
   no row is affected.
2. **The target rows are found**, as today (fetch, `FROM` / `USING`
   joins, conflict probes).
3. **For each row, in the statement's existing compute loop:**
   1. Defaults and the `SET` expressions are applied.
   2. Each enabled **BEFORE ROW** trigger whose `WHEN` holds runs, in
      name order, each seeing the previous one's `NEW`.
      * `NULL` skips the row, and the remaining triggers do not run for
        it.
      * A returned row replaces `NEW`. It is coerced to the table's
        columns; a mismatch is `42804`.
      * For `DELETE`, any non-NULL return proceeds.
   3. **Only then** come NOT NULL, typmods, CHECKs, row-security
      `WITH CHECK`, keys and FKs.
      * NOT NULL and typmods move out of today's compute loop, so that a
        trigger can fill a NOT NULL column.
      * Key probes are batched after the loop, as now, so a key a
        trigger changed is the one probed.
   4. `RETURNING` sees the row as the triggers left it.
4. **The batch is written** (`RunBatch`).
5. **AFTER ROW** events were queued in row order, with `OLD` and `NEW`
   and the `WHEN` evaluated at queue time. They now run in that order,
   triggers in name order within each event. All of the statement's
   changes are visible to them, as in PostgreSQL, where AFTER ROW
   triggers fire at the end of the statement.
6. **AFTER STATEMENT** triggers run.

**Nesting.** A trigger's own statements fire their tables' triggers, and
each nested statement runs this whole model. `pg_trigger_depth()`
reports the depth. The depth limit above bounds recursion; PostgreSQL
leaves that to the trigger author.

**BEFORE ROW triggers and earlier rows.** PostgreSQL lets a BEFORE ROW
trigger's queries see the earlier rows of the same statement
(`trigger-datachanges`). In datax those rows sit in the unwritten batch.
So when a table has a BEFORE ROW trigger whose function is not a
*row-local* function, the batch is flushed before each call.

* A *row-local* PL/pgSQL function contains only simple expressions,
  assignments, `IF` / `CASE` and `RETURN`. The classification is made
  when the function is compiled.
* The common `updated_at` and normalisation triggers are row-local, so
  they keep the single batch.

### DML paths

| Path (code) | Row triggers | Statement triggers |
|---|---|---|
| `INSERT ... VALUES / SELECT` (`execInsert`) | `INSERT` per row | `INSERT` once |
| `INSERT ... ON CONFLICT DO NOTHING` (`insertOnConflict`) | BEFORE INSERT fires for **each proposed row, before** the conflict check (the trigger may change the key). A skipped row fires nothing more | `INSERT` |
| `INSERT ... ON CONFLICT DO UPDATE`, `UPSERT` | BEFORE INSERT first. On a conflict, BEFORE UPDATE (`OLD` = existing, `NEW` = merged) then AFTER UPDATE; else AFTER INSERT | PostgreSQL's order: BEFORE INSERT, BEFORE UPDATE, AFTER UPDATE, AFTER INSERT (`trigger-definition`) |
| `UPDATE` (`applyUpdate`), including a primary-key change (`update_pk.go`) | `UPDATE` per row (a moved row is an update, not a delete plus insert). `UPDATE OF cols` fires when a listed column is a `SET` target or a BEFORE trigger changed it | `UPDATE` |
| `UPDATE ... FROM` (`execUpdateFrom`) | Once per target row (datax takes the first joined row, PG-Q-032) | once |
| `DELETE`, `DELETE ... USING` (`applyDelete`, `execDeleteUsing`) | `DELETE` per row | once |
| FK `ON DELETE / UPDATE CASCADE / SET NULL` (`enforceReferencing`) | Each child row fires its table's BEFORE ROW triggers inline (a `NULL` return skips that child row, as PostgreSQL's RI does, which can leave an orphan: PostgreSQL's documented hazard). AFTER ROW events join the outer statement's queue | The child's statement triggers fire once **per parent row** cascaded from: PostgreSQL's RI runs one statement per parent row |
| `COPY FROM` (`copy.go`) | Per row, inside each chunk's transaction | BEFORE in the first chunk's transaction, AFTER in the last's. A failed COPY keeps earlier chunks and their trigger effects (PG-Q-020) |
| `TRUNCATE` | none (as in PostgreSQL) | `TRUNCATE` BEFORE and AFTER |
| Retention, row expiry (`ttl`), `PURGE`, online backfills, `ALTER COLUMN TYPE` rewrites, seal jobs, re-shard, restore | **never** | never: these are maintenance, as table rewrites are in PostgreSQL |

A COPY into a table with row triggers takes the ordinary insert path for
each row. The trigger may change the key, so the chunk's up-front key
probes cannot be used.

### Where a trigger runs

datax evaluates SQL only on the **gateway** that runs the statement:

* there is no expression pushdown into the KV layer (`pkg/kvserver` does
  not import the parser);
* execution is single-goroutine (`pkg/sql` starts goroutines only for
  re-sharding).

So a trigger runs on that gateway, in the statement's goroutine and
`kvclient.Txn`. Its reads and writes go to whichever leaseholders hold
the keys, like any statement's. Nothing runs on other nodes:
background jobs on other nodes fire no triggers (table above).

**Cost.** A trigger costs one round trip per statement it runs, per row.
An audit trigger on a 10,000-row `UPDATE` makes 10,000 writes, buffered
in one batch when the trigger is AFTER ROW. Transition tables (Phase 4)
are the cheap form for bulk audit.

### Creating a trigger online

`CREATE TRIGGER` changes the table descriptor in the statement's
transaction. It therefore works inside `BEGIN`, which migrations need
(they create a function and its trigger in one transaction).

There is a window. After that transaction commits, a write that a gateway
planned against the previous version can still commit until the drain
ends, without firing the trigger. Its deadline is that version's lease
expiration (`docs/sql.md`, descriptor leases). PostgreSQL closes this
window with a `SHARE ROW EXCLUSIVE` lock.

datax closes it differently:

* **Outside a block,** `CREATE TRIGGER` is two steps, as an online
  `ADD CONSTRAINT` is:
  1. Publish the trigger in state *enabling*. Gateways on the new version
     fire it; `pg_trigger` does not list it yet.
  2. Drain. Then mark it enabled.

  Writes that commit after the statement returns therefore always fire
  it. Writes in the window may fire it early, but none miss it.
* **Inside a block** it is one step. When the block created the table,
  there is no window at all, because no other gateway can hold it.
  Otherwise the window is documented.

`DROP TRIGGER` and `DISABLE` have the converse window: the trigger may
fire for a short time after the change commits. That is harmless.

### Enabling and disabling

`tgenabled` follows PostgreSQL (`sql-altertable`, `runtime-config-client`):

| `tgenabled` | Fires under `session_replication_role` |
|---|---|
| `O` (default, `ENABLE`) | `origin`, `local` |
| `R` (`ENABLE REPLICA`) | `replica` |
| `A` (`ENABLE ALWAYS`) | always |
| `D` (`DISABLE`) | never |

**`session_replication_role`.** Today anyone may set it and it does
nothing (PG-O-001). With triggers it would let any role switch off audit
triggers. So setting it to anything but its current value needs an admin,
as PostgreSQL requires a superuser: `42501 permission denied to set
parameter "session_replication_role"`.

**`DISABLE TRIGGER ALL` and foreign keys.** In PostgreSQL, foreign keys
are internal triggers, and `DISABLE TRIGGER ALL` (superuser) stops their
checks. Rails' fixture loading and `pg_restore --disable-triggers` depend
on this. `replica` mode also skips them. datax's foreign keys are not
triggers. To match:

* `ALTER TABLE t DISABLE TRIGGER ALL` by an admin also sets a descriptor
  flag, `fk_checks_disabled`, which suspends the table's FK checks and
  actions until `ENABLE TRIGGER ALL`.
* A non-admin gets PostgreSQL's
  `42501 permission denied: "RI_ConstraintTrigger_..." is a system trigger`
  in datax's wording.
* `DISABLE TRIGGER USER` disables only user triggers.
* `session_replication_role = replica` suspends FK checks for the
  session, as in PostgreSQL.

This closes part of PG-D-021. `REPLICA IDENTITY` and the other forms
there stay open.

## Security

* **`EXECUTE`.** A new function grants `EXECUTE` to PUBLIC, as in
  PostgreSQL (`ddl-priv`). The grant is stored explicitly in the new
  descriptor's `privileges`, so `proacl` renders
  `{=X/owner,owner=X/owner}`. That equals `acldefault('f', owner)`, which
  `vtable.DefaultACL` gains, so pg_dump emits no `GRANT`.
  * `REVOKE EXECUTE ... FROM PUBLIC` removes the grant.
  * `ALTER DEFAULT PRIVILEGES ... REVOKE EXECUTE ON FUNCTIONS FROM PUBLIC`
    applies to later functions.
  * The owner and admins always hold `EXECUTE`. Membership in
    `read_all` or `write_all` does not grant it.
* **`SECURITY DEFINER`.**
  * **Identity inside the body.** The body runs with the owner's
    privileges (`privAs`), and `current_user` returns the owner;
    `session_user` does not change. The session gains an effective
    current-user field that `current_user`, `current_role`, `user` and
    the privilege functions read: today they read `s.user`. Row-level
    security policies evaluate as the owner, as in PostgreSQL.
  * **Hijacking.** A definer function that resolves names through the
    caller's `search_path` can be hijacked. The caller can create a table
    or function in a schema earlier on its path, or a temporary table,
    which datax always searches first. PostgreSQL's advice
    (`sql-createfunction`, "Writing SECURITY DEFINER Functions Safely")
    applies, and the user doc repeats it:
    * pin `SET search_path = trusted_schema, pg_temp`;
    * revoke `EXECUTE` from PUBLIC in the same transaction as the
      `CREATE`.
  * **Making the pin work in datax.** A function with a pinned path
    resolves as a view's query does: temporary tables are skipped unless
    the path names `pg_temp`, and then they come at its position. That
    is what makes `pg_temp`-last effective. A definer function **without**
    `SET search_path` is allowed, as in PostgreSQL. The difference from a
    view is deliberate: a view records its creator's path, a function
    does not, which is PostgreSQL's behaviour.
  * **Stored expressions** that call functions bind by ID (see
    "Resolution"), so no path can redirect them.
* **Triggers.**
  * **Who may create.** `CREATE TRIGGER` needs `TRIGGER` on the table, or
    ownership, plus `EXECUTE` on the function, checked at creation only.
    `TRIGGER` is appended to the table privileges (`tablePrivOrder`;
    append-only storage order; ACL letter `t`). This closes that part of
    PG-S-007.
  * **Whose rights it runs with.** A trigger function runs with the
    privileges of the role whose statement fired it, unless it is a
    definer function. A role holding `TRIGGER` on a table can therefore
    make every writer of that table, admins included, run its code with
    the writer's privileges. That is PostgreSQL's model too. The user doc
    says to grant `TRIGGER` as carefully as ownership.
* **Denial of service.**
  * The nesting limit (`54001`) prevents a stack overflow, which in Go
    would kill the node, not just the session.
  * `statement_timeout`, cancellation and the row caps bound loops and
    sets.
  * A function cannot outlive its statement: there are no background
    workers, `pg_sleep` honours cancellation, and there is no `dblink`.
  * `CREATE FUNCTION` needs `CREATE` on a schema, which `public` does not
    grant to PUBLIC.
* **Leak-proofness** is not modelled. `LEAKPROOF` is stored for pg_dump
  only and has no effect. What PostgreSQL uses it for still has to hold:
  a row a policy hides must never reach a user-written predicate that
  calls a function, which could leak it, for example through `RAISE`.
  So the row-security filter keeps running before the statement's own
  conjuncts, and the Phase 1 tests check this with a function that
  raises on every row it sees.

## Mixed versions, upgrade, backup and restore

* **The gate.** `version.V23` introduces function descriptors and names
  (`/system/funcdesc`, `/system/funcns`), the `/system/dep` records,
  `TableDescriptor.triggers` and `fk_checks_disabled`, and the `TRIGGER`
  privilege. One function (`requireFunctions`) refuses
  `CREATE FUNCTION`, `DO`, `CREATE TRIGGER`, `GRANT ... TRIGGER` and
  `DISABLE TRIGGER ALL` with
  `0A000 functions need cluster version v23: finalize the upgrade with
  datax debug upgrade first`, until finalize.
  * Reads need no gate: with no function descriptor, every name resolves
    as before.
  * Phases 2–4 add no new persisted format of their own, beyond
    `language`, which is a string, and the new trigger fields. So they
    ship under v23 if it has not been released by then. Otherwise each
    takes the next version, as `docs/design/schemas.md` did for v22.
* **What an old binary would do.** A v22 node never reads the new system
  keys. It would ignore `triggers`, so writes would not fire them. It
  would ignore `/system/dep`, so drops would not be blocked. It would
  answer `42883` to a call. Nothing creates any of these before finalize,
  and after finalize no v22 binary can join or restart against the
  cluster (`pkg/version`). Rule 5: golden decode tests freeze
  `FunctionDescriptor` and the trigger fields.
* **Backup.**
  * Function descriptors, names and `/system/dep` records ride in the
    manifest's `databases` group (`manifestKeyGroups`), as schemas do.
  * A binary from before v23 refuses a manifest carrying them, because
    they fall outside the spans it accepts. A backup holding triggers
    always holds their functions, so older binaries refuse those too.
  * Triggers ride inside the verbatim table descriptors.
* **Cluster restore** writes everything back and keeps the
  descriptor-ID counter above the functions' IDs. Restoring into a
  cluster below v23 is refused, as schemas are.
* **`RESTORE DATABASE`** carries the database's functions (filtered by
  `database_id`).
* **`RESTORE TABLE`** of a table with triggers, or a default, CHECK or
  policy that binds a function, is refused (`0A000`) unless every bound
  function exists in the target with the same identity. In that case the
  bindings are re-pointed. A `skip_triggers` option drops the triggers
  instead.
* **Downgrade** past finalize is refused at startup, as today.

## Catalogs and tools

| Object | Content |
|---|---|
| `pg_proc` | Builtins as now, plus one row per user function. Columns: `oid` = ID, `pronamespace` = `SchemaOID`, `proowner`, `prolang`, `prokind 'f'`, `prosecdef`, `proleakproof`, `proisstrict`, `proretset`, `provolatile`, `proparallel`, `pronargs`, `pronargdefaults`, `prorettype`, `proargtypes`, `proallargtypes` / `proargmodes` (`RETURNS TABLE`), `proargnames`, `prosrc`, `prosqlbody` NULL, `proconfig`, `procost`, `prorows`, `proacl`; `probin`, `prosupport`, `protrftypes` NULL / 0 |
| `pg_language` | `internal` (12), `c` (13), `sql` (14), `plpgsql` (the extension's language OID), with `lanpltrusted` |
| `pg_trigger` | One row per trigger: `oid` = trigger ID, `tgrelid`, `tgname`, `tgfoid`, `tgtype` (PostgreSQL's bit layout), `tgenabled`, `tgisinternal false`, `tgnargs`, `tgargs`, `tgattr`, `tgqual`; no internal FK rows |
| `pg_depend` | The `/system/dep` records, plus `n` / `a` rows for function→schema and trigger→table (part of PG-C-005) |
| `information_schema.routines`, `.parameters` | User functions (builtins stay out, PG-C-015) |
| `information_schema.triggers` | One row per trigger and event: `action_order`, `action_condition`, `action_statement` (`EXECUTE FUNCTION f()`), `action_orientation`, `action_timing` |
| Functions | `pg_get_functiondef(oid)` (the full `CREATE OR REPLACE FUNCTION`, as `\sf` and `\ef` expect), `pg_get_function_arguments` (with defaults), `pg_get_function_identity_arguments`, `pg_get_function_result` (`TABLE(...)`, `SETOF ...`), `pg_get_triggerdef(oid [, pretty])`, `pg_function_is_visible` (real), `has_function_privilege`, `to_regproc` / `to_regprocedure`, casts to `regproc` / `regprocedure`, `pg_trigger_depth()` |
| `SHOW FUNCTIONS` | Unchanged: builtins only. `\df` is the way to list user functions |

**psql 16.**

* `\df[+]` lists user functions with name, result and argument types,
  type, volatility, owner, security, ACL and language. It hides
  `pg_catalog`, so builtins appear only with `\dfS`.
* `\sf f` prints the definition.
* `\d[+] t` prints `Triggers:` and `Disabled user triggers:`. psql runs
  that query only when `relhastriggers` is true, which today it is only
  for FK parents; it becomes true for any table with a trigger.
* `\dp` shows the `t` privilege.

**pg_dump 16**, the client CI pins (`docs/testing.md`):

* it sets `check_function_bodies = false`, which datax honours;
* it emits functions before tables, by its type priority;
* it emits `ALTER FUNCTION ... OWNER TO`, `COMMENT ON FUNCTION` and
  `REVOKE / GRANT ... ON FUNCTION`;
* it emits triggers in post-data, after the rows, so a restore fires none;
* it emits `ALTER TABLE ... DISABLE TRIGGER` for disabled ones.

The round-trip test (below) holds the output to identity on a second
dump.

## Testing plan

* **Parser** (`pkg/sql/parser`):
  * every accepted form, round-tripped through `format.go`;
  * the forms refused per phase;
  * qualified and named calls, and `FROM f()`;
  * PL/pgSQL grammar fixtures taken from PostgreSQL's `plpgsql` regression
    suite (statement shapes only).
* **Catalog** (`pkg/sql/catalog`):
  * golden decode tests for `FunctionDescriptor`, `Trigger` and the
    `/system/dep` value (rule 5);
  * key-ordering tests for the new spans (`keys_test.go`).
* **Resolution** (unit):
  * the candidate algorithm against a table of PostgreSQL's own outcomes,
    taken from `typeconv-func` examples and checked on PostgreSQL 16;
  * the builtin-first shortcut;
  * the same-family refusal.
* **Cluster tests** (`pkg/testutils/testcluster`, new files `functions_*`,
  `plpgsql_*`, `triggers_*`), on a multi-node cluster where routing
  matters:
  * create, call, overload, defaults, named arguments, `OR REPLACE`
    rules, `DROP` with and without signature, `CASCADE` / `RESTRICT` over
    every dependency row of the table above;
  * inlining, checked through `EXPLAIN` showing an index used through an
    inlined call;
  * volatility: a once-per-statement versus per-row counter using a
    sequence;
  * definer rights: `current_user`, a hijack attempt through a temporary
    table and through an earlier schema, both with and without
    `SET search_path`;
  * `EXECUTE` grants, default privileges, and a stored-expression binding
    that cannot be captured;
  * cross-session calls of the row-function hook are refused (regression
    test for the prerequisite);
  * read-only and `AS OF` writes are `25006`;
  * the depth limit returns `54001` and the node survives;
  * `statement_timeout` stops a `LOOP`;
  * retries: `TestingBeforeImplicitCommit` injects `40001` and the test
    asserts that triggers and notices take effect once;
  * exception blocks roll back writes, notifications and advisory locks,
    and `40001` passes through `WHEN OTHERS`;
  * triggers:
    * firing order and `WHEN`;
    * `UPDATE OF`;
    * each DML path in the table above, including `ON CONFLICT` order,
      cascades, COPY chunks, and `TRUNCATE`;
    * maintenance jobs fire nothing;
    * a BEFORE trigger filling a NOT NULL column;
    * a non-row-local BEFORE trigger seeing earlier rows;
    * `ENABLE` / `DISABLE` / `session_replication_role` (and its
      privilege);
    * `DISABLE TRIGGER ALL` suspending FKs;
    * a `CREATE TRIGGER` racing concurrent writers on another gateway:
      after the statement returns, no write misses the trigger.
* **Upgrade** (`upgrade_test.go`): every statement is refused below v23
  and works after finalize. Backup: round trip; a pre-v23 manifest
  restores; a pre-v23 binary refuses a v23 manifest with functions
  (`backup_test.go`).
* **Tools:**
  * `pgdump_test.go`: pg_dump 16 of a schema with overloaded functions,
    defaults and CHECKs calling them, a view, a definer function with
    `SET search_path`, triggers (one disabled) and comments. The test
    restores it with psql into a fresh database, dumps again, and diffs.
  * psql `\df+`, `\sf`, `\d t` golden output.
* **Corpus.** PostgreSQL's `create_function_sql`, `plpgsql` and
  `triggers` regression files run against a node, with an
  expected-differences list kept in the repository. A shrinking list is
  the progress measure.
* **Race:** `-race` over the new packages and the trigger and function
  cluster tests, including two sessions calling the same function
  concurrently.

## Effort and risks

| Phase | Effort | Main work |
|---|---|---|
| Prerequisites | M (1–2 weeks) | Row-function registry, nested read-only check, cheap savepoints, CONTEXT field |
| 1. SQL functions | XL (7–9 engineer-weeks) | Parser (qualified, named, `FROM`), descriptor and keys, resolver, call path and inlining, dependencies and `CASCADE`, privileges, catalogs and pg_dump |
| 2. PL/pgSQL and `DO` | XL (6–8) | Grammar and tokenizer export, compiler and interpreter, exceptions, `EXECUTE`, corpus |
| 3. Triggers | L–XL (6–8) | Hooks in every DML path, the AFTER queue, cascades and COPY, online create, enable/disable and FK suspension, catalogs |
| 4. Later | L (4–6) | `INSTEAD OF`, transition tables, index expressions, signatures |

**Risks**

1. **Re-entrancy of the session.** The session was written for one
   statement at a time. Nested DML inside a SELECT's row loop is new.
   Missing a piece of state in the save and restore would be a
   correctness bug: `s.rels`, the memo, `viewPath`, `privAs`, the explain
   state, `extraDDL`, `stmtNow`. *Mitigation:* one `nestedState` struct
   that is saved and restored as a whole, and a test that runs every
   statement kind nested.
2. **Performance.** A non-inlined call runs a whole statement per row,
   and function descriptors are read per statement (two KV round trips,
   uncached).
   *Mitigation:* inlining; the PL/pgSQL simple-expression path; the
   builtin-first rule; and Phase 4's gateway cache. The benchmark
   workloads in `bench/` gain a trigger case and a function case.
3. **Write visibility around BEFORE triggers.** The flush per row that a
   non-row-local trigger needs makes such tables' writes much slower.
   *Mitigation:* the row-local classification covers the common
   triggers. Others are documented.
4. **Serializable-only isolation.** More statements in a transaction
   means more `40001`s for applications that never saw them under
   PostgreSQL. This is an existing gap (PG-X-001), made more visible.
5. **The `CREATE TRIGGER` window** inside a block, on an existing table.
   It is documented. Closing it would need table locks (PG-X-005).
6. **Same-family overloads and the builtin shortcut.** These are
   deliberate differences, and an application that relies on either fails
   loudly (`0A000`) or calls the builtin. They are listed in the user doc.
7. **DDL inside `DO`.** Online DDL refused in a `DO` block will surprise
   migration authors; it is the PG-D-031 limitation in a new place.

## Open questions for the owner

| # | Question | Recommendation |
|---|---|---|
| 1 | Same-family overloads (`f(int4)` beside `f(int8)`): refuse, or accept and resolve by declared type where a call's types are known? | **Refuse** (`0A000`). Literals and `int4` expressions now have their own widths (PG-F-016 is closed), but other same-family types do not, and a function's integer result or an untyped operand is still a `bigint` (PG-F-030), so accepting them would still resolve unlike PostgreSQL in places. |
| 2 | Builtin-first shortcut versus exact PostgreSQL resolution for user functions that share a builtin's name and arity. | **Ship the shortcut** in Phase 1, documented. Revisit with the leased name set in Phase 4 only if an application needs it. |
| 3 | Do function descriptors get gateway caching and leases in Phase 1? | **No.** Read them in the statement's transaction (always correct). Generalise the lease cache beyond `*TableDescriptor` in Phase 4, once there is a measured need. |
| 4 | Should `CREATE TRIGGER` outside a block be the two-step online form? | **Yes.** It costs a drain, which every DDL already pays, and it removes the missed-write window where it can be removed. |
| 5 | Should `DISABLE TRIGGER ALL` (admin) and `session_replication_role = replica` suspend foreign-key checks, as they do in PostgreSQL? | **Yes.** Rails fixtures and `pg_restore --disable-triggers` depend on it. Restrict both to admins in the same change. |
| 6 | Nesting limit: fixed, or a setting? | **Fixed at 256** to start, raised after a stack measurement. No new setting until someone needs a different value. |
| 7 | Should PL/pgSQL handlers be able to catch `40001`? | **No.** The transaction is unusable after a KV restart. Document it. |
| 8 | Should `SECURITY DEFINER` without `SET search_path` be refused or warned about? | **No.** That is PostgreSQL's behaviour, and pg_dump output of real schemas must load. Document the advice prominently. |
| 9 | `RESTORE TABLE` of a table whose triggers or defaults bind functions. | **Refuse unless the functions exist** in the target with the same identity, and offer `skip_triggers`. |
| 10 | Are transition tables and `INSTEAD OF` in Phase 3? | **No, Phase 4.** They are not needed by the triggers that block migrations today (`updated_at`, audit), and transition tables interact with COPY chunking. |
| 11 | Should `DO` ship in Phase 2, or wait for transaction-block DDL (PG-D-031) so that typical migration blocks fully work? | **Ship it in Phase 2.** Most `DO` blocks guard descriptor-only DDL (`ADD COLUMN` without a backfill, `CREATE TYPE`), which works. |
| 12 | Should the stale "Today" text of PG-P-001 in the gap register be corrected now? | **Yes**, by the lead, to `42601 expected TABLE, found "function"`. |

## References (PostgreSQL 17 documentation pages)

* `sql-createfunction`, including "Overloading" and "Writing SECURITY
  DEFINER Functions Safely"
* `sql-dropfunction`, `sql-alterfunction`
* `xfunc-sql`, `xfunc-volatility`, `xfunc-optimization`
* `typeconv-func`, `runtime-config-client` (`search_path`,
  `check_function_bodies`, `session_replication_role`)
* `ddl-priv`, `ddl-depend`
* `plpgsql-declarations`, `plpgsql-statements`,
  `plpgsql-control-structures` ("Trapping Errors"),
  `plpgsql-errors-and-messages`, `plpgsql-trigger`,
  `plpgsql-implementation`
* `trigger-definition`, `trigger-datachanges`, `sql-createtrigger`,
  `sql-altertable` (`ENABLE` / `DISABLE TRIGGER`)
* `catalog-pg-proc`, `catalog-pg-trigger`, `catalog-pg-depend`,
  `infoschema-routines`, `infoschema-triggers`, `functions-info`
  (`pg_get_functiondef` ...)
* `errcodes-appendix`
* PostgreSQL wiki: "Inlining of SQL functions"
