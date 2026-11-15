package builtins

import "github.com/sthorne/datax/pkg/sql/types"

// The functions the session evaluates itself (they need the clock, the
// catalog or the transaction) are registered without an
// implementation, so the reference, SHOW FUNCTIONS and pg_proc list
// them and the parser checks their arity; the evaluator never calls
// them here (pkg/sql splices them before the row loop).

const catSession = "Session and system"

// catLargeObject is the large-object functions' section (issue #303).
const catLargeObject = "Large objects"

func init() {
	for _, b := range []*Builtin{
		{Name: "now", Ret: types.Timestamp, Vol: Stable, Doc: "The statement's start time, the same for every row (also current_timestamp, localtimestamp, statement_timestamp(), transaction_timestamp()).",
			Aliases: []string{"current_timestamp", "localtimestamp", "statement_timestamp", "transaction_timestamp"}},
		{Name: "current_date", Ret: types.Date, Vol: Stable, Doc: "Today's date, from the statement's start time."},
		{Name: "current_user", Ret: types.String, Vol: Stable, Doc: "The current role: the session user, or the role SET ROLE selected."},
		{Name: "session_user", Ret: types.String, Vol: Stable, Doc: "The authenticated session user (unchanged by SET ROLE)."},
		{Name: "current_database", Ret: types.String, Vol: Stable, Doc: "The session's database."},
		{Name: "current_schema", Ret: types.String, Vol: Stable, Doc: "public: the only schema."},
		{Name: "current_setting", Args: []types.Family{types.String, types.Bool}, MinArgs: 1, Ret: types.String, Vol: Stable, Doc: "The value of a session setting, built in or custom (app.tenant). An unknown or never-set name is an error (42704) unless missing_ok is true, which returns NULL. Evaluated once per statement."},
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
