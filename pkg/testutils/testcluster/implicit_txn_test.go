package testcluster

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgproto3"
)

// A multi-statement simple Query, and an extended-protocol pipeline up to
// its Sync, run outside a transaction block as one implicit transaction,
// as in PostgreSQL: committed at the end, rolled back when a statement
// fails. Explicit BEGIN / COMMIT among the statements follow PostgreSQL's
// rules.

// pipeOutcome is what the server answered to one cycle: the error codes
// it sent, the rows' first values, and the ReadyForQuery status.
type pipeOutcome struct {
	errs   []string
	rows   []string
	status byte
}

// cycle sends msgs and collects the answers up to the ReadyForQuery that
// ends the cycle (the caller ends msgs with a Sync, or sends one Query).
func (r *rawFrontend) cycle(msgs ...pgproto3.FrontendMessage) pipeOutcome {
	r.t.Helper()
	r.send(msgs...)
	var out pipeOutcome
	for {
		switch m := r.recv().(type) {
		case *pgproto3.ErrorResponse:
			out.errs = append(out.errs, m.Code)
		case *pgproto3.DataRow:
			out.rows = append(out.rows, string(m.Values[0]))
		case *pgproto3.ReadyForQuery:
			out.status = m.TxStatus
			return out
		}
	}
}

// then is two cycles' outcome: both cycles' errors, the second's status.
func then(a, b pipeOutcome) pipeOutcome {
	b.errs = append(a.errs, b.errs...)
	return b
}

// stmt is Parse/Bind/Execute of an unnamed statement.
func stmt(q string) []pgproto3.FrontendMessage {
	return []pgproto3.FrontendMessage{&pgproto3.Parse{Query: q}, &pgproto3.Bind{}, &pgproto3.Execute{}}
}

func pipeline(qs ...string) []pgproto3.FrontendMessage {
	var msgs []pgproto3.FrontendMessage
	for _, q := range qs {
		msgs = append(msgs, stmt(q)...)
	}
	return append(msgs, &pgproto3.Sync{})
}

func TestImplicitTransactionBoundaries(t *testing.T) {
	tc := Start(t, 1)
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()
	conn, err := pgx.Connect(ctx, pgURL(tc, 0))
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close(ctx)
	if _, err := conn.Exec(ctx, `CREATE TABLE pipe (id INT8 PRIMARY KEY)`); err != nil {
		t.Fatal(err)
	}
	if _, err := conn.Exec(ctx, `CREATE TABLE mig (id INT8 PRIMARY KEY, v INT8)`); err != nil {
		t.Fatal(err)
	}
	// count is the single value a query returns.
	count := func(q string) int64 {
		t.Helper()
		var n int64
		if err := conn.QueryRow(ctx, q).Scan(&n); err != nil {
			t.Fatalf("%s: %v", q, err)
		}
		return n
	}
	indexes := func(name string) int64 {
		return count(`SELECT count(*) FROM pg_indexes WHERE indexname = '` + name + `'`)
	}
	ids := func() string {
		t.Helper()
		var s string
		if err := conn.QueryRow(ctx, `SELECT coalesce(string_agg(id::text, ',' ORDER BY id), '') FROM pipe`).Scan(&s); err != nil {
			t.Fatal(err)
		}
		return s
	}
	reset := func() {
		t.Helper()
		if _, err := conn.Exec(ctx, `DELETE FROM pipe`); err != nil {
			t.Fatal(err)
		}
	}
	check := func(what string, got pipeOutcome, errs string, status byte, want string) {
		t.Helper()
		if g := strings.Join(got.errs, ","); g != errs {
			t.Errorf("%s: errors %q, want %q", what, g, errs)
		}
		if got.status != status {
			t.Errorf("%s: ReadyForQuery %q, want %q", what, got.status, status)
		}
		if g := ids(); g != want {
			t.Errorf("%s: rows %q, want %q", what, g, want)
		}
		reset()
	}
	r := dialRaw(t, tc)
	query := func(q string) pipeOutcome { return r.cycle(&pgproto3.Query{String: q}) }

	// Extended protocol.
	check("pipeline, error after an insert",
		r.cycle(pipeline("INSERT INTO pipe VALUES (1)", "SELECT 1/0")...), "22012", 'I', "")
	check("pipeline, failing Parse after an insert",
		r.cycle(append(stmt("INSERT INTO pipe VALUES (1)"), &pgproto3.Parse{Query: "SELEC"}, &pgproto3.Sync{})...), "42601", 'I', "")
	check("pipeline, all succeed",
		r.cycle(pipeline("INSERT INTO pipe VALUES (1)", "INSERT INTO pipe VALUES (2)")...), "", 'I', "1,2")
	check("pipeline, single statement",
		r.cycle(pipeline("INSERT INTO pipe VALUES (1)")...), "", 'I', "1")
	check("pipeline, BEGIN ... COMMIT then a failing implicit part",
		r.cycle(pipeline("BEGIN", "INSERT INTO pipe VALUES (1)", "COMMIT", "INSERT INTO pipe VALUES (2)", "SELECT 1/0")...), "22012", 'I', "1")
	check("pipeline, BEGIN turns the implicit part explicit",
		then(r.cycle(pipeline("INSERT INTO pipe VALUES (1)", "BEGIN", "INSERT INTO pipe VALUES (2)")...), r.cycle(pipeline("ROLLBACK")...)), "", 'I', "")
	sel := r.cycle(pipeline("SELECT 1", "SELECT 2")...)
	if strings.Join(sel.rows, ",") != "1,2" || len(sel.errs) != 0 || sel.status != 'I' {
		t.Errorf("SELECT-only pipeline: %+v", sel)
	}
	// A row-limited portal suspended inside the implicit transaction is
	// closed by the Sync that commits it.
	if _, err := conn.Exec(ctx, `INSERT INTO pipe VALUES (10), (11), (12)`); err != nil {
		t.Fatal(err)
	}
	got := r.cycle(&pgproto3.Parse{Query: "SELECT id FROM pipe ORDER BY id"}, &pgproto3.Bind{}, &pgproto3.Execute{MaxRows: 1},
		&pgproto3.Parse{Query: "INSERT INTO pipe VALUES (13)"}, &pgproto3.Bind{}, &pgproto3.Execute{}, &pgproto3.Sync{})
	check("pipeline, suspended portal then insert", got, "", 'I', "10,11,12,13")

	// Simple protocol.
	check("simple, error after an insert", query("INSERT INTO pipe VALUES (2); SELECT 1/0;"), "22012", 'I', "")
	check("simple, all succeed", query("INSERT INTO pipe VALUES (1); INSERT INTO pipe VALUES (2)"), "", 'I', "1,2")
	check("simple, COMMIT ends the implicit transaction",
		query("INSERT INTO pipe VALUES (1); COMMIT; INSERT INTO pipe VALUES (2); SELECT 1/0"), "22012", 'I', "1")
	check("simple, BEGIN inside",
		then(query("INSERT INTO pipe VALUES (1); BEGIN; INSERT INTO pipe VALUES (2)"), query("ROLLBACK")), "", 'I', "")
	open := query("INSERT INTO pipe VALUES (1); BEGIN; INSERT INTO pipe VALUES (2)")
	if open.status != 'T' {
		t.Errorf("simple, BEGIN inside: ReadyForQuery %q, want T", open.status)
	}
	check("simple, BEGIN inside, committed later", query("COMMIT"), "", 'I', "1,2")
	check("simple, savepoint in the implicit transaction",
		query("INSERT INTO pipe VALUES (1); SAVEPOINT a"), "25P01", 'I', "")
	// A statement PostgreSQL refuses in any transaction block (VACUUM)
	// runs alone, and is refused in a multi-statement implicit
	// transaction, which then rolls back. As the first Execute of a
	// pipeline it runs on its own, as in PostgreSQL, and the rest of the
	// pipeline is one implicit transaction.
	check("simple, VACUUM alone", query("VACUUM"), "", 'I', "")
	check("simple, VACUUM after an insert", query("INSERT INTO pipe VALUES (1); VACUUM"), "25001", 'I', "")
	check("pipeline, VACUUM alone", r.cycle(pipeline("VACUUM")...), "", 'I', "")
	check("pipeline, VACUUM after an insert", r.cycle(pipeline("INSERT INTO pipe VALUES (1)", "VACUUM")...), "25001", 'I', "")
	check("pipeline, VACUUM first",
		r.cycle(pipeline("VACUUM", "INSERT INTO pipe VALUES (1)", "INSERT INTO pipe VALUES (2)", "SELECT 1/0")...), "22012", 'I', "")

	// datax's online statements (CREATE INDEX on an existing table,
	// ALTER TABLE ... ADD CONSTRAINT, ANALYZE, ...) run in transactions
	// of their own: in an implicit transaction one commits what came
	// before it and runs alone; the statements after it are another
	// implicit transaction.
	check("simple, ANALYZE after an insert", query("INSERT INTO pipe VALUES (1); ANALYZE pipe"), "", 'I', "1")
	check("simple, CREATE INDEX between inserts",
		query("INSERT INTO pipe VALUES (1); CREATE INDEX mig_v_simple ON mig (v); INSERT INTO pipe VALUES (2); SELECT 1/0"), "22012", 'I', "1")
	if n := indexes("mig_v_simple"); n != 1 {
		t.Errorf("simple, CREATE INDEX between inserts: %d indexes, want 1", n)
	}
	check("pipeline, CREATE INDEX between inserts",
		r.cycle(pipeline("INSERT INTO pipe VALUES (1)", "CREATE INDEX mig_v_pipe ON mig (v)", "INSERT INTO pipe VALUES (2)", "SELECT 1/0")...), "22012", 'I', "1")
	if n := indexes("mig_v_pipe"); n != 1 {
		t.Errorf("pipeline, CREATE INDEX between inserts: %d indexes, want 1", n)
	}
	check("pipeline, CREATE INDEX first",
		r.cycle(pipeline("CREATE INDEX mig_v_first ON mig (v)", "INSERT INTO pipe VALUES (1)", "SELECT 1/0")...), "22012", 'I', "")
	if n := indexes("mig_v_first"); n != 1 {
		t.Errorf("pipeline, CREATE INDEX first: %d indexes, want 1", n)
	}
	// A migration file sent as one Exec, as golang-migrate does.
	migration := func(sfx string) string {
		return `CREATE TABLE mig_` + sfx + ` (id INT8 PRIMARY KEY, ref INT8);
			INSERT INTO mig_` + sfx + ` VALUES (1, 1);
			CREATE INDEX mig_v_` + sfx + ` ON mig (v);
			ALTER TABLE mig_` + sfx + ` ADD CONSTRAINT mig_` + sfx + `_ref_pos CHECK (ref > 0);
			INSERT INTO pipe VALUES (7)`
	}
	if _, err := conn.Exec(ctx, migration("script")); err != nil {
		t.Errorf("migration script: %v", err)
	}
	if g := ids(); g != "7" {
		t.Errorf("migration script: rows %q, want 7", g)
	}
	reset()
	var stmts []string
	for _, q := range strings.Split(migration("batch"), ";") {
		stmts = append(stmts, strings.TrimSpace(q))
	}
	check("pipeline, migration", r.cycle(pipeline(stmts...)...), "", 'I', "7")
	for _, sfx := range []string{"script", "batch"} {
		if n := count(`SELECT count(*) FROM mig_` + sfx); n != 1 {
			t.Errorf("migration %s: %d rows, want 1", sfx, n)
		}
		if n := indexes("mig_v_" + sfx); n != 1 {
			t.Errorf("migration %s: %d indexes, want 1", sfx, n)
		}
		if _, err := conn.Exec(ctx, `INSERT INTO mig_`+sfx+` VALUES (2, -1)`); err == nil {
			t.Errorf("migration %s: the CHECK constraint was not added", sfx)
		}
	}
	// In an explicit block, the one BEGIN made of an implicit
	// transaction included, they are still refused.
	check("simple, CREATE INDEX in a block",
		then(query("BEGIN; INSERT INTO pipe VALUES (1); CREATE INDEX mig_v_block ON mig (v)"), query("ROLLBACK")), "25001", 'I', "")
	check("simple, CREATE INDEX after BEGIN in an implicit transaction",
		then(query("INSERT INTO pipe VALUES (1); BEGIN; CREATE INDEX mig_v_block ON mig (v)"), query("ROLLBACK")), "25001", 'I', "")
	if n := indexes("mig_v_block"); n != 0 {
		t.Errorf("CREATE INDEX in a block: %d indexes, want 0", n)
	}

	// pgx's SendBatch is one pipeline: all or nothing.
	b := &pgx.Batch{}
	b.Queue(`INSERT INTO pipe VALUES ($1)`, int64(1))
	b.Queue(`INSERT INTO pipe VALUES ($1)`, int64(2))
	b.Queue(`SELECT 1/0`)
	if err := conn.SendBatch(ctx, b).Close(); err == nil {
		t.Error("batch with a division by zero succeeded")
	} else if pe := (*pgconn.PgError)(nil); !errors.As(err, &pe) || pe.Code != "22012" {
		t.Errorf("batch: %v", err)
	}
	if g := ids(); g != "" {
		t.Errorf("failed batch left rows %q", g)
	}
	b = &pgx.Batch{}
	b.Queue(`INSERT INTO pipe VALUES ($1)`, int64(1))
	b.Queue(`INSERT INTO pipe VALUES ($1)`, int64(2))
	if err := conn.SendBatch(ctx, b).Close(); err != nil {
		t.Fatal(err)
	}
	if g := ids(); g != "1,2" {
		t.Errorf("batch committed %q, want 1,2", g)
	}
}
