package builtins

import "github.com/sthorne/datax/pkg/sql/types"

// The functions the session evaluates itself (they need the clock, the
// catalog or the transaction) are registered without an
// implementation, so the reference, SHOW FUNCTIONS and pg_proc list
// them and the parser checks their arity; the evaluator never calls
// them here (pkg/sql splices them before the row loop).

const catSession = "Session and system"

// catCatalog is the system information and privilege functions'
// section (issue #357).
const catCatalog = "System information and privileges"

// catLargeObject is the large-object functions' section (issue #303).
const catLargeObject = "Large objects"

func init() {
	for _, b := range []*Builtin{
		{Name: "now", Ret: types.Timestamp, Vol: Stable, Doc: "The statement's start time, the same for every row (also current_timestamp, localtimestamp, statement_timestamp(), transaction_timestamp()).",
			Aliases: []string{"current_timestamp", "localtimestamp", "statement_timestamp", "transaction_timestamp"}},
		{Name: "current_date", Ret: types.Date, Vol: Stable, Doc: "Today's date, from the statement's start time."},
		{Name: "current_time", Ret: types.Time, Vol: Stable, Doc: "The time of day of the statement's start time, in the session's time zone. current_time, current_timestamp, localtime and localtimestamp are written without parentheses, as in PostgreSQL, and take an optional fractional-seconds precision: current_timestamp(3).",
			Aliases: []string{"localtime"}},
		{Name: "current_user", Ret: types.String, Vol: Stable, Doc: "The current role: the session user, or the role SET ROLE selected. Also written current_role and user; like session_user, all three are keywords written without parentheses."},
		{Name: "session_user", Ret: types.String, Vol: Stable, Doc: "The authenticated session user (unchanged by SET ROLE)."},
		{Name: "current_database", Ret: types.String, Vol: Stable, Doc: "The session's database (also the keyword current_catalog, without parentheses)."},
		{Name: "current_schema", Ret: types.String, Vol: Stable, Doc: "public: the only schema."},
		{Name: "current_setting", Args: []types.Family{types.String, types.Bool}, MinArgs: 1, Ret: types.String, Vol: Stable, Doc: "The value of a session setting, built in or custom (app.tenant). An unknown or never-set name is an error (42704) unless missing_ok is true, which returns NULL. Its arguments may name columns: the name is looked up per row."},
		{Name: "set_config", Args: []types.Family{types.String, types.String, types.Bool}, MinArgs: 3, Ret: types.String, Vol: Volatile, Doc: "Sets a session setting, as SET does (SET LOCAL when is_local is true), and returns the value: SET with a value that can be a parameter. Evaluated once per statement: its arguments cannot name columns."},
		{Name: "version", Ret: types.String, Vol: Stable, Doc: "The server version string (PostgreSQL 14.0 datax <release>)."},
		{Name: "nextval", Args: []types.Family{types.String}, MinArgs: 1, Ret: types.Int, Vol: Volatile, Doc: "Advances the sequence and returns its next value; never rolled back."},
		{Name: "currval", Args: []types.Family{types.String}, MinArgs: 1, Ret: types.Int, Vol: Stable, Doc: "The value nextval last returned for the sequence in this session (55000 before any)."},
		{Name: "lastval", Ret: types.Int, Vol: Stable, Doc: "The value nextval last returned in this session, whatever the sequence."},
		{Name: "setval", Args: []types.Family{types.String, types.Int, types.Bool}, MinArgs: 2, Ret: types.Int, Vol: Volatile, Doc: "Sets the sequence's counter; with is_called false the value itself is the next one handed out."},
		{Name: "unique_rowid", Ret: types.Int, Vol: Volatile, Doc: "A node-local monotonic 64-bit id (48 bits of microsecond time above the node ID): unique across nodes with no coordination, spread across ranges unlike a sequence."},
		{Name: "pg_notify", Args: []types.Family{types.String, types.String}, MinArgs: 2, Ret: types.String, Vol: Volatile, Doc: "Sends a notification to the sessions listening on the channel (NOTIFY with a computed channel or payload); delivered when the transaction commits. Evaluated once per statement: its arguments cannot name columns."},
		{Name: "gen_random_uuid", Ret: types.Uuid, Vol: Volatile, Doc: "A random (version 4) UUID.", Aliases: []string{"uuid_generate_v4"}},
	} {
		b.Category = catSession
		b.Session = true
		register(b)
	}
	a, t := Any, types.String
	for _, f := range []*Builtin{
		{Name: "pg_get_serial_sequence", Args: []types.Family{t, t}, MinArgs: 2, Ret: t, Doc: "The sequence a SERIAL, identity or OWNED BY column draws from, as public.name (quoted where needed), or NULL when it has none. The table name is read as SQL reads one ('\"Mixed\"', 'public.t'); the column name is taken as written. 42P01 for no such table, 42703 for no such column."},
		{Name: "pg_is_in_recovery", Ret: types.Bool, Doc: "false: every node serves reads and writes; there is no standby mode."},
		{Name: "current_schemas", Args: []types.Family{types.Bool}, MinArgs: 1, Ret: types.ArrayOf(t), Doc: "The schemas of the search path: {pg_catalog,public} when the argument (include implicit schemas) is true, {public} when false."},
		{Name: "to_regclass", Args: []types.Family{t}, MinArgs: 1, Ret: types.Int, Doc: "The OID of the table, view, sequence or catalog so named (read as SQL reads a name), or NULL when there is none — 'name'::regclass without the error."},
		{Name: "to_regproc", Args: []types.Family{t}, MinArgs: 1, Ret: types.Int, Doc: "The pg_proc OID of the builtin function so named, or NULL."},
		{Name: "to_regprocedure", Args: []types.Family{t}, MinArgs: 1, Ret: types.Int, Doc: "The pg_proc OID of the builtin function named with its argument list ('lower(text)'), or NULL when there is none or it takes another number of arguments."},
		{Name: "to_regnamespace", Args: []types.Family{t}, MinArgs: 1, Ret: types.Int, Doc: "The OID of the schema (public, pg_catalog or information_schema), or NULL."},
		{Name: "to_regrole", Args: []types.Family{t}, MinArgs: 1, Ret: types.Int, Doc: "The OID of the role, or NULL when there is none."},
		{Name: "pg_has_role", Args: []types.Family{a, a, t}, MinArgs: 2, Ret: types.Bool, Doc: "pg_has_role([user,] role, privilege): whether the user (the current role when omitted; a name or an OID) is a MEMBER of the role (directly or not), has its privileges (USAGE: membership through INHERIT), or may SET ROLE to it (SET); WITH ADMIN OPTION asks whether it may grant the role. A comma-separated list is true when any holds. Root and admins hold every role. 42704 for an unknown role name, 22023 for an unknown privilege."},
		{Name: "has_table_privilege", Args: []types.Family{a, a, t}, MinArgs: 2, Ret: types.Bool, Doc: "has_table_privilege([user,] table, privilege): whether the user (the current role when omitted; a name, an OID or public) holds the privilege on the table, view or sequence (a name or an OID) — SELECT, INSERT, UPDATE, DELETE and TRUNCATE as GRANT, ownership, admin, read_all and write_all confer them; REFERENCES, TRIGGER and MAINTAIN are the owner's and admins'. A comma-separated list is true when any holds; WITH GRANT OPTION asks for the grant option. NULL for an OID that names nothing, 42P01 for an unknown name, 22023 for an unknown privilege. The schema's USAGE is not consulted, as in PostgreSQL."},
		{Name: "has_any_column_privilege", Args: []types.Family{a, a, t}, MinArgs: 2, Ret: types.Bool, Doc: "has_any_column_privilege([user,] table, privilege): SELECT, INSERT, UPDATE or REFERENCES on any column of the table. datax grants on tables, so this is has_table_privilege's answer."},
		{Name: "has_column_privilege", Args: []types.Family{a, a, a, t}, MinArgs: 3, Ret: types.Bool, Doc: "has_column_privilege([user,] table, column, privilege): SELECT, INSERT, UPDATE or REFERENCES on the column (a name, or an attribute number). datax grants on tables, so this is the table's answer. 42703 for an unknown column name, NULL for an unknown number."},
		{Name: "has_sequence_privilege", Args: []types.Family{a, a, t}, MinArgs: 2, Ret: types.Bool, Doc: "has_sequence_privilege([user,] sequence, privilege): USAGE, SELECT or UPDATE on the sequence, as nextval, currval and setval check them. 42809 when the relation is not a sequence."},
		{Name: "has_schema_privilege", Args: []types.Family{a, a, t}, MinArgs: 2, Ret: types.Bool, Doc: "has_schema_privilege([user,] schema, privilege): USAGE or CREATE. On public, USAGE is everyone's unless revoked from PUBLIC and CREATE is what creating a table in the database takes; pg_catalog and information_schema are everyone's to use and admins' to create in. 3F000 for an unknown schema."},
		{Name: "has_database_privilege", Args: []types.Family{a, a, t}, MinArgs: 2, Ret: types.Bool, Doc: "has_database_privilege([user,] database, privilege): CREATE, CONNECT, or TEMPORARY (TEMP), which follows CONNECT. 3D000 for an unknown database."},
		{Name: "has_function_privilege", Args: []types.Family{a, a, t}, MinArgs: 2, Ret: types.Bool, Doc: "has_function_privilege([user,] function, privilege): EXECUTE, which every role holds on every function. The function is a pg_proc OID or name(argument types); 42883 when none matches."},
		{Name: "has_type_privilege", Args: []types.Family{a, a, t}, MinArgs: 2, Ret: types.Bool, Doc: "has_type_privilege([user,] type, privilege): USAGE, which every role holds on every type. 42704 for an unknown type."},
		{Name: "has_tablespace_privilege", Args: []types.Family{a, a, t}, MinArgs: 2, Ret: types.Bool, Doc: "has_tablespace_privilege([user,] tablespace, privilege): CREATE on pg_default or pg_global, which only admins hold. 42704 for any other tablespace."},
		{Name: "has_foreign_data_wrapper_privilege", Args: []types.Family{a, a, t}, MinArgs: 2, Ret: types.Bool, Doc: "There are no foreign-data wrappers: 42704 for a name, NULL for an OID."},
		{Name: "has_server_privilege", Args: []types.Family{a, a, t}, MinArgs: 2, Ret: types.Bool, Doc: "There are no foreign servers: 42704 for a name, NULL for an OID."},
	} {
		f.Category = catCatalog
		f.Vol = Stable
		f.Session = true
		register(f)
	}
	i, b := types.Int, types.Bytes
	for _, f := range []*Builtin{
		{Name: "lo_create", Args: []types.Family{i}, MinArgs: 1, Ret: i, Doc: "Creates an empty large object with the given id (0 picks a free one) and returns the id. Created inside a transaction block, the object can then be written as a stream of any length."},
		{Name: "lo_creat", Args: []types.Family{i}, MinArgs: 1, Ret: i, Doc: "Creates an empty large object with a free id (the argument is ignored, as in PostgreSQL) and returns the id."},
		{Name: "lo_from_bytea", Args: []types.Family{i, b}, MinArgs: 2, Ret: i, Doc: "Creates a large object holding the given bytes (id 0 picks a free one) and returns its id."},
		{Name: "lo_open", Args: []types.Family{i, i}, MinArgs: 2, Ret: i, Doc: "Opens a large object and returns a descriptor valid until the transaction ends; mode 131072 (INV_WRITE) allows writing, 262144 (INV_READ) is read-only."},
		{Name: "lo_close", Args: []types.Family{i}, MinArgs: 1, Ret: i, Doc: "Closes a large-object descriptor (the end of the transaction closes those left open)."},
		{Name: "loread", Args: []types.Family{i, i}, MinArgs: 2, Ret: b, Doc: "Reads up to the given number of bytes at the descriptor's position and advances it; fewer bytes mean the end of the object."},
		{Name: "lowrite", Args: []types.Family{i, b}, MinArgs: 2, Ret: i, Doc: "Writes the bytes at the descriptor's position, advances it, and returns the number written."},
		{Name: "lo_lseek64", Args: []types.Family{i, i, i}, MinArgs: 3, Ret: i, Doc: "Moves the descriptor's position to an offset from the start (whence 0), the current position (1) or the end (2) and returns it.", Aliases: []string{"lo_lseek"}},
		{Name: "lo_tell64", Args: []types.Family{i}, MinArgs: 1, Ret: i, Doc: "The descriptor's position.", Aliases: []string{"lo_tell"}},
		{Name: "lo_truncate64", Args: []types.Family{i, i}, MinArgs: 2, Ret: i, Doc: "Cuts the large object to the given length, or extends it with zeros.", Aliases: []string{"lo_truncate"}},
		{Name: "lo_unlink", Args: []types.Family{i}, MinArgs: 1, Ret: i, Doc: "Removes a large object; its storage is reclaimed in the background."},
		{Name: "lo_get", Args: []types.Family{i, i, i}, MinArgs: 1, Ret: b, Doc: "The object's bytes, or the given number of them from an offset, as one value (bounded by statement_memory_limit; read large objects with loread)."},
		{Name: "lo_put", Args: []types.Family{i, i, b}, MinArgs: 3, Ret: types.String, Doc: "Writes the bytes into the object at an offset."},
	} {
		f.Category = catLargeObject
		f.Vol = Volatile
		f.Session = true
		register(f)
	}
}
