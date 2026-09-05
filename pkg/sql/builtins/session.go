package builtins

import "github.com/sthorne/datax/pkg/sql/types"

// The functions the session evaluates itself (they need the clock, the
// catalog or the transaction) are registered without an
// implementation, so the reference, SHOW FUNCTIONS and pg_proc list
// them and the parser checks their arity; the evaluator never calls
// them here (pkg/sql splices them before the row loop).

const catSession = "Session and system"

func init() {
	for _, b := range []*Builtin{
		{Name: "now", Ret: types.Timestamp, Vol: Stable, Doc: "The statement's start time, the same for every row (also current_timestamp, localtimestamp, statement_timestamp(), transaction_timestamp()).",
			Aliases: []string{"current_timestamp", "localtimestamp", "statement_timestamp", "transaction_timestamp"}},
		{Name: "current_date", Ret: types.Date, Vol: Stable, Doc: "Today's date, from the statement's start time."},
		{Name: "current_user", Ret: types.String, Vol: Stable, Doc: "The current role: the session user, or the role SET ROLE selected."},
		{Name: "session_user", Ret: types.String, Vol: Stable, Doc: "The authenticated session user (unchanged by SET ROLE)."},
		{Name: "current_database", Ret: types.String, Vol: Stable, Doc: "The session's database."},
		{Name: "current_schema", Ret: types.String, Vol: Stable, Doc: "public: the only schema."},
		{Name: "version", Ret: types.String, Vol: Stable, Doc: "The server version string (PostgreSQL 14.0 datax <release>)."},
		{Name: "nextval", Args: []types.Family{types.String}, MinArgs: 1, Ret: types.Int, Vol: Volatile, Doc: "Advances the sequence and returns its next value; never rolled back."},
		{Name: "currval", Args: []types.Family{types.String}, MinArgs: 1, Ret: types.Int, Vol: Stable, Doc: "The value nextval last returned for the sequence in this session (55000 before any)."},
		{Name: "lastval", Ret: types.Int, Vol: Stable, Doc: "The value nextval last returned in this session, whatever the sequence."},
		{Name: "setval", Args: []types.Family{types.String, types.Int, types.Bool}, MinArgs: 2, Ret: types.Int, Vol: Volatile, Doc: "Sets the sequence's counter; with is_called false the value itself is the next one handed out."},
		{Name: "unique_rowid", Ret: types.Int, Vol: Volatile, Doc: "A node-local monotonic 64-bit id (48 bits of microsecond time above the node ID): unique across nodes with no coordination, spread across ranges unlike a sequence."},
		{Name: "gen_random_uuid", Ret: types.Uuid, Vol: Volatile, Doc: "A random (version 4) UUID.", Aliases: []string{"uuid_generate_v4"}},
	} {
		b.Category = catSession
		b.Session = true
		register(b)
	}
}
