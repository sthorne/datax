package testcluster

import (
	"context"
	"testing"
	"time"

	"github.com/sthorne/datax/pkg/sql"
	"github.com/sthorne/datax/pkg/sql/catalog"
	"github.com/sthorne/datax/pkg/sql/parser"
	"github.com/sthorne/datax/pkg/sql/types"
)

// execSQL parses and executes statements on a session, returning the last
// result.
func execSQL(t *testing.T, ctx context.Context, s *sql.Session, src string, params ...types.Datum) *sql.Result {
	t.Helper()
	res, err := trySQL(ctx, s, src, params...)
	if err != nil {
		t.Fatalf("sql %q: [%s] %s", src, err.Code, err.Msg)
	}
	return res
}

func trySQL(ctx context.Context, s *sql.Session, src string, params ...types.Datum) (*sql.Result, *sql.Error) {
	stmts, err := parser.Parse(src)
	if err != nil {
		return nil, sql.ToSQLError(err)
	}
	var last *sql.Result
	for _, stmt := range stmts {
		res, serr := s.Execute(ctx, stmt, params)
		if serr != nil {
			return nil, serr
		}
		last = res
	}
	return last, nil
}

func TestSQLEndToEnd(t *testing.T) {
	tc := Start(t, 3)
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()
	cat := catalog.NewAccessor()
	s := sql.NewSession(tc.Nodes[0].DB(), cat)

	execSQL(t, ctx, s, `CREATE TABLE accounts (id INT8 PRIMARY KEY, balance INT8 NOT NULL, owner TEXT)`)
	res := execSQL(t, ctx, s, `INSERT INTO accounts VALUES (1, 100, 'ann'), (2, 200, 'bob'), (3, 50, NULL)`)
	if res.Tag != "INSERT 0 3" {
		t.Fatalf("tag %q", res.Tag)
	}

	// Point select on PK.
	res = execSQL(t, ctx, s, `SELECT balance, owner FROM accounts WHERE id = 2`)
	if len(res.Rows) != 1 || res.Rows[0][0].I != 200 || res.Rows[0][1].S != "bob" {
		t.Fatalf("%+v", res.Rows)
	}
	// Scan with filter.
	res = execSQL(t, ctx, s, `SELECT id FROM accounts WHERE balance >= 100`)
	if len(res.Rows) != 2 {
		t.Fatalf("%+v", res.Rows)
	}
	// NULL never matches.
	res = execSQL(t, ctx, s, `SELECT id FROM accounts WHERE owner = 'ann'`)
	if len(res.Rows) != 1 || res.Rows[0][0].I != 1 {
		t.Fatalf("%+v", res.Rows)
	}
	// LIMIT.
	res = execSQL(t, ctx, s, `SELECT * FROM accounts LIMIT 2`)
	if len(res.Rows) != 2 || len(res.Columns) != 3 {
		t.Fatalf("%d rows %d cols", len(res.Rows), len(res.Columns))
	}

	// Explicit transaction with column arithmetic.
	execSQL(t, ctx, s, `BEGIN`)
	execSQL(t, ctx, s, `UPDATE accounts SET balance = balance - 10 WHERE id = 1`)
	execSQL(t, ctx, s, `UPDATE accounts SET balance = balance + 10 WHERE id = 2`)
	if s.State() != sql.StateOpen {
		t.Fatal("not in txn")
	}
	execSQL(t, ctx, s, `COMMIT`)
	res = execSQL(t, ctx, s, `SELECT balance FROM accounts WHERE id = 1`)
	if res.Rows[0][0].I != 90 {
		t.Fatalf("balance after txn: %+v", res.Rows)
	}

	// Rollback undoes changes.
	execSQL(t, ctx, s, `BEGIN; DELETE FROM accounts WHERE id = 3; ROLLBACK`)
	res = execSQL(t, ctx, s, `SELECT id FROM accounts`)
	if len(res.Rows) != 3 {
		t.Fatalf("rollback failed: %d rows", len(res.Rows))
	}

	// Parameters.
	res = execSQL(t, ctx, s, `SELECT owner FROM accounts WHERE id = $1`, types.NewString("2"))
	if len(res.Rows) != 1 || res.Rows[0][0].S != "bob" {
		t.Fatalf("param select: %+v", res.Rows)
	}

	// DELETE and duplicate key.
	res = execSQL(t, ctx, s, `DELETE FROM accounts WHERE balance < 60`)
	if res.Tag != "DELETE 1" {
		t.Fatalf("tag %q", res.Tag)
	}
	if _, err := trySQL(ctx, s, `INSERT INTO accounts VALUES (1, 0, 'dup')`); err == nil || err.Code != sql.CodeUniqueViolation {
		t.Fatalf("duplicate insert: %v", err)
	}

	// SHOW TABLES and catalog from a different node/session.
	s2 := sql.NewSession(tc.Nodes[2].DB(), catalog.NewAccessor())
	res = execSQL(t, ctx, s2, `SELECT balance FROM accounts WHERE id = 2`)
	if res.Rows[0][0].I != 210 {
		t.Fatalf("cross-node read: %+v", res.Rows)
	}
	res = execSQL(t, ctx, s2, `SHOW TABLES`)
	if len(res.Rows) != 1 || res.Rows[0][0].S != "accounts" {
		t.Fatalf("%+v", res.Rows)
	}

	// Error semantics: failed txn blocks until rollback.
	execSQL(t, ctx, s, `BEGIN`)
	if _, err := trySQL(ctx, s, `SELECT * FROM missing`); err == nil || err.Code != sql.CodeUndefinedTable {
		t.Fatalf("undefined table: %v", err)
	}
	if s.State() != sql.StateFailed {
		t.Fatal("txn should be failed")
	}
	if _, err := trySQL(ctx, s, `SELECT 1`); err == nil || err.Code != sql.CodeInFailedTransaction {
		t.Fatalf("in failed txn: %v", err)
	}
	execSQL(t, ctx, s, `ROLLBACK`)
	if s.State() != sql.StateIdle {
		t.Fatal("not idle after rollback")
	}

	// NOT NULL violation.
	if _, err := trySQL(ctx, s, `INSERT INTO accounts (id) VALUES (9)`); err == nil || err.Code != sql.CodeNotNullViolation {
		t.Fatalf("not null: %v", err)
	}

	// DROP TABLE.
	execSQL(t, ctx, s, `DROP TABLE accounts`)
	if _, err := trySQL(ctx, s, `SELECT * FROM accounts`); err == nil || err.Code != sql.CodeUndefinedTable {
		t.Fatalf("after drop: %v", err)
	}
}

func TestSQLSerializationConflict(t *testing.T) {
	tc := Start(t, 1)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	cat := catalog.NewAccessor()
	s1 := sql.NewSession(tc.Nodes[0].DB(), cat)
	s2 := sql.NewSession(tc.Nodes[0].DB(), catalog.NewAccessor())

	execSQL(t, ctx, s1, `CREATE TABLE kv (k INT8 PRIMARY KEY, v INT8)`)
	execSQL(t, ctx, s1, `INSERT INTO kv VALUES (1, 0)`)

	// s1 opens a txn and writes; s2 conflicts and must get 40001 (or block
	// until s1 finishes — with s1 holding the intent, s2's budget expires).
	execSQL(t, ctx, s1, `BEGIN`)
	execSQL(t, ctx, s1, `UPDATE kv SET v = 1 WHERE k = 1`)

	_, serr := trySQL(ctx, s2, `BEGIN; UPDATE kv SET v = 2 WHERE k = 1`)
	if serr == nil {
		// s2 won the push (aborted s1 by priority): s1 must now fail.
		execSQL(t, ctx, s2, `COMMIT`)
		if _, err := trySQL(ctx, s1, `COMMIT`); err == nil {
			t.Fatal("both conflicting txns committed")
		}
	} else {
		if serr.Code != sql.CodeSerializationFailure {
			t.Fatalf("expected 40001, got [%s] %s", serr.Code, serr.Msg)
		}
		execSQL(t, ctx, s2, `ROLLBACK`)
		execSQL(t, ctx, s1, `COMMIT`)
	}

	// Whatever happened, the table is consistent and writable.
	res := execSQL(t, ctx, s1, `SELECT v FROM kv WHERE k = 1`)
	if len(res.Rows) != 1 {
		t.Fatalf("%+v", res.Rows)
	}
}
