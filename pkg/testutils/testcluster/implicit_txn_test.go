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
	// A statement that cannot run inside a transaction block runs alone,
	// and is refused in a multi-statement implicit transaction.
	check("simple, ANALYZE alone", query("ANALYZE pipe"), "", 'I', "")
	check("simple, ANALYZE after an insert", query("INSERT INTO pipe VALUES (1); ANALYZE pipe"), "25001", 'I', "")
	check("pipeline, ANALYZE alone", r.cycle(pipeline("ANALYZE pipe")...), "", 'I', "")

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
