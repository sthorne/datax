package testcluster

import (
	"context"
	gosql "database/sql"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	_ "github.com/jackc/pgx/v5/stdlib"
)

// TestBeginTransactionModes (issue #346): BEGIN and START TRANSACTION
// take SET TRANSACTION's modes, as pgx's BeginTx and database/sql's
// read-only transactions send them, over the simple and the extended
// protocol. READ ONLY refuses writes (25006) for the block only; the
// isolation level is accepted and the session stays serializable.
func TestBeginTransactionModes(t *testing.T) {
	tc := Start(t, 1)
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	conn, err := pgx.Connect(ctx, pgURL(tc, 0))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = conn.Close(context.Background()) }()
	mustExec := func(q string) {
		t.Helper()
		if _, err := conn.Exec(ctx, q); err != nil {
			t.Fatalf("%s: %v", q, err)
		}
	}
	refused := func(q, code string) {
		t.Helper()
		if _, err := conn.Exec(ctx, q); pgErrCode(err) != code {
			t.Fatalf("%s: %v, want %s", q, err, code)
		}
	}
	show := func(name string) string {
		t.Helper()
		var v string
		if err := conn.QueryRow(ctx, "SHOW "+name).Scan(&v); err != nil {
			t.Fatalf("SHOW %s: %v", name, err)
		}
		return v
	}
	mustExec(`CREATE TABLE kv (k INT8 PRIMARY KEY, v TEXT)`)
	mustExec(`CREATE SEQUENCE seq`)

	// pgx BeginTx: begin isolation level serializable read only.
	tx, err := conn.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.Serializable, AccessMode: pgx.ReadOnly})
	if err != nil {
		t.Fatalf("BeginTx read only: %v", err)
	}
	var ro, iso string
	if err := tx.QueryRow(ctx, `SELECT current_setting('transaction_read_only'), current_setting('transaction_isolation')`).Scan(&ro, &iso); err != nil || ro != "on" || iso != "serializable" {
		t.Fatalf("in the read-only block: %q %q %v", ro, iso, err)
	}
	if _, err := tx.Exec(ctx, `INSERT INTO kv VALUES ($1, $2)`, 1, "x"); pgErrCode(err) != "25006" || err.Error() != "ERROR: cannot execute INSERT in a read-only transaction (SQLSTATE 25006)" {
		t.Fatalf("INSERT in BeginTx(ReadOnly): %v", err)
	}
	_ = tx.Rollback(ctx)
	if show("transaction_read_only") != "off" {
		t.Fatal("the block's end lifts READ ONLY")
	}

	// The other pgx options: every isolation level, READ WRITE, DEFERRABLE.
	for _, opts := range []pgx.TxOptions{
		{IsoLevel: pgx.ReadCommitted},
		{IsoLevel: pgx.RepeatableRead, AccessMode: pgx.ReadWrite},
		{IsoLevel: pgx.ReadUncommitted, DeferrableMode: pgx.NotDeferrable},
		{IsoLevel: pgx.Serializable, AccessMode: pgx.ReadOnly, DeferrableMode: pgx.Deferrable},
	} {
		tx, err := conn.BeginTx(ctx, opts)
		if err != nil {
			t.Fatalf("BeginTx %+v: %v", opts, err)
		}
		if err := tx.QueryRow(ctx, `SHOW transaction_isolation`).Scan(&iso); err != nil || iso != "serializable" {
			t.Fatalf("BeginTx %+v: isolation %q %v", opts, iso, err)
		}
		_ = tx.Rollback(ctx)
	}

	// Simple protocol: each statement form, the refusals, and the block's end.
	for _, begin := range []string{`BEGIN READ ONLY`, `START TRANSACTION READ ONLY`, `BEGIN WORK ISOLATION LEVEL READ COMMITTED, READ ONLY`, `BEGIN TRANSACTION READ ONLY NOT DEFERRABLE`} {
		mustExec(begin)
		refused(`UPDATE kv SET v = 'y'`, "25006")
		mustExec(`ROLLBACK`)
		mustExec(begin)
		refused(`DELETE FROM kv`, "25006")
		mustExec(`ROLLBACK`)
		mustExec(begin)
		refused(`CREATE TABLE nope (id INT8 PRIMARY KEY)`, "25006")
		mustExec(`ROLLBACK`)
		mustExec(begin)
		refused(`SELECT nextval('seq')`, "25006")
		mustExec(`ROLLBACK`)
		mustExec(begin)
		refused(`SELECT setval('seq', 10)`, "25006")
		mustExec(`ROLLBACK`)
	}
	mustExec(`BEGIN ISOLATION LEVEL REPEATABLE READ`)
	mustExec(`INSERT INTO kv VALUES (1, 'a')`)
	mustExec(`SELECT nextval('seq')`)
	mustExec(`COMMIT`)
	refused(`BEGIN ISOLATION LEVEL CHAOTIC`, "42601")
	refused(`BEGIN READ`, "42601")
	mustExec(`INSERT INTO kv VALUES (2, 'b')`) // nothing left open

	// READ WRITE lifts default_transaction_read_only for its block only.
	mustExec(`SET default_transaction_read_only = on`)
	mustExec(`BEGIN READ WRITE`)
	mustExec(`INSERT INTO kv VALUES (3, 'c')`)
	mustExec(`COMMIT`)
	refused(`INSERT INTO kv VALUES (4, 'd')`, "25006")
	mustExec(`SET default_transaction_read_only = off`)

	// Extended protocol: BEGIN with modes as a prepared statement.
	pc := conn.PgConn()
	for _, q := range []string{`BEGIN ISOLATION LEVEL SERIALIZABLE READ ONLY`, `START TRANSACTION READ ONLY`} {
		if _, err := pc.ExecParams(ctx, q, nil, nil, nil, nil).Close(); err != nil {
			t.Fatalf("extended %s: %v", q, err)
		}
		if _, err := pc.ExecParams(ctx, `INSERT INTO kv VALUES ($1, 'e')`, [][]byte{[]byte("5")}, nil, nil, nil).Close(); pgErrCode(err) != "25006" {
			t.Fatalf("extended INSERT after %s: %v", q, err)
		}
		if _, err := pc.ExecParams(ctx, `ROLLBACK`, nil, nil, nil, nil).Close(); err != nil {
			t.Fatal(err)
		}
	}

	// database/sql: sql.TxOptions{ReadOnly: true}.
	db, err := gosql.Open("pgx", pgURL(tc, 0))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()
	stx, err := db.BeginTx(ctx, &gosql.TxOptions{ReadOnly: true})
	if err != nil {
		t.Fatalf("database/sql BeginTx read only: %v", err)
	}
	var n int
	if err := stx.QueryRowContext(ctx, `SELECT count(*) FROM kv`).Scan(&n); err != nil || n != 3 {
		t.Fatalf("read in the read-only block: %d %v", n, err)
	}
	if _, err := stx.ExecContext(ctx, `INSERT INTO kv VALUES ($1, $2)`, 6, "f"); pgErrCode(err) != "25006" {
		t.Fatalf("database/sql INSERT in a read-only tx: %v", err)
	}
	_ = stx.Rollback()
	stx, err = db.BeginTx(ctx, &gosql.TxOptions{Isolation: gosql.LevelSerializable})
	if err != nil {
		t.Fatalf("database/sql BeginTx serializable: %v", err)
	}
	if _, err := stx.ExecContext(ctx, `INSERT INTO kv VALUES ($1, $2)`, 6, "f"); err != nil {
		t.Fatalf("database/sql INSERT in a serializable tx: %v", err)
	}
	if err := stx.Commit(); err != nil {
		t.Fatal(err)
	}
}
