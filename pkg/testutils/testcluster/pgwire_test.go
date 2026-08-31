package testcluster

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	_ "github.com/jackc/pgx/v5/stdlib"
)

func pgURL(tc *TestCluster, node int) string {
	return fmt.Sprintf("postgres://root@%s/datax?sslmode=disable", tc.Nodes[node].SQLAddr())
}

// TestPGWireWithPgx is the Phase 6 checkpoint: a stock pgx client (default
// extended-protocol mode) runs the demo workload over the wire.
func TestPGWireWithPgx(t *testing.T) {
	tc := Start(t, 3)
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	conn, err := pgx.Connect(ctx, pgURL(tc, 0))
	if err != nil {
		t.Fatalf("pgx connect: %v", err)
	}
	defer func() { _ = conn.Close(ctx) }()

	// Health check.
	var one int
	if err := conn.QueryRow(ctx, "SELECT 1").Scan(&one); err != nil || one != 1 {
		t.Fatalf("SELECT 1: %v (%d)", err, one)
	}

	if _, err := conn.Exec(ctx, `CREATE TABLE accounts (id INT8 PRIMARY KEY, balance INT8 NOT NULL, owner TEXT)`); err != nil {
		t.Fatalf("create: %v", err)
	}
	tag, err := conn.Exec(ctx, `INSERT INTO accounts VALUES (1, 100, 'ann'), (2, 100, 'bob')`)
	if err != nil || tag.RowsAffected() != 2 {
		t.Fatalf("insert: %v (%v)", err, tag)
	}

	// Extended protocol with parameters and binary results.
	var balance int64
	var owner *string
	if err := conn.QueryRow(ctx, `SELECT balance, owner FROM accounts WHERE id = $1`, 2).Scan(&balance, &owner); err != nil {
		t.Fatalf("param select: %v", err)
	}
	if balance != 100 || owner == nil || *owner != "bob" {
		t.Fatalf("got %d, %v", balance, owner)
	}

	// Explicit transaction: the README money transfer.
	tx, err := conn.Begin(ctx)
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	if _, err := tx.Exec(ctx, `UPDATE accounts SET balance = balance - 10 WHERE id = 1`); err != nil {
		t.Fatalf("update1: %v", err)
	}
	if _, err := tx.Exec(ctx, `UPDATE accounts SET balance = balance + 10 WHERE id = 2`); err != nil {
		t.Fatalf("update2: %v", err)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatalf("commit: %v", err)
	}

	// Read from a different node's SQL server.
	conn3, err := pgx.Connect(ctx, pgURL(tc, 2))
	if err != nil {
		t.Fatalf("connect n3: %v", err)
	}
	defer func() { _ = conn3.Close(ctx) }()
	rows, err := conn3.Query(ctx, `SELECT id, balance FROM accounts WHERE balance >= $1`, 90)
	if err != nil {
		t.Fatalf("query n3: %v", err)
	}
	total := int64(0)
	count := 0
	for rows.Next() {
		var id, bal int64
		if err := rows.Scan(&id, &bal); err != nil {
			t.Fatalf("scan: %v", err)
		}
		total += bal
		count++
	}
	rows.Close()
	if count != 2 || total != 200 {
		t.Fatalf("cross-node query: %d rows, total %d", count, total)
	}

	// NULL handling over the wire.
	if _, err := conn.Exec(ctx, `INSERT INTO accounts (id, balance) VALUES (3, 5)`); err != nil {
		t.Fatalf("insert null: %v", err)
	}
	var o *string
	if err := conn.QueryRow(ctx, `SELECT owner FROM accounts WHERE id = 3`).Scan(&o); err != nil {
		t.Fatalf("select null: %v", err)
	}
	if o != nil {
		t.Fatalf("expected NULL owner, got %q", *o)
	}

	// SQLSTATE surfaces: duplicate key.
	_, err = conn.Exec(ctx, `INSERT INTO accounts VALUES (1, 0, 'dup')`)
	var pgErr *pgconn.PgError
	if !errors.As(err, &pgErr) || pgErr.Code != "23505" {
		t.Fatalf("duplicate: %v", err)
	}
	// Failed-transaction gating: 25P02 until rollback.
	tx2, _ := conn.Begin(ctx)
	if _, err := tx2.Exec(ctx, `SELECT * FROM missing`); err == nil {
		t.Fatal("expected undefined table")
	}
	if _, err := tx2.Exec(ctx, `SELECT 1`); !errors.As(err, &pgErr) || pgErr.Code != "25P02" {
		t.Fatalf("failed txn gating: %v", err)
	}
	if err := tx2.Rollback(ctx); err != nil {
		t.Fatalf("rollback: %v", err)
	}
}

// TestPGWireSimpleProtocol exercises the simple query protocol.
func TestPGWireSimpleProtocol(t *testing.T) {
	tc := Start(t, 1)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	cfg, err := pgx.ParseConfig(pgURL(tc, 0))
	if err != nil {
		t.Fatal(err)
	}
	cfg.DefaultQueryExecMode = pgx.QueryExecModeSimpleProtocol
	conn, err := pgx.ConnectConfig(ctx, cfg)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer func() { _ = conn.Close(ctx) }()

	if _, err := conn.Exec(ctx, `CREATE TABLE t (k TEXT PRIMARY KEY, v FLOAT8, ok BOOL)`); err != nil {
		t.Fatalf("create: %v", err)
	}
	if _, err := conn.Exec(ctx, `INSERT INTO t VALUES ('a', 1.5, TRUE), ('b', -2.25, FALSE)`); err != nil {
		t.Fatalf("insert: %v", err)
	}
	var k string
	var v float64
	var ok bool
	if err := conn.QueryRow(ctx, `SELECT k, v, ok FROM t WHERE k = 'b'`).Scan(&k, &v, &ok); err != nil {
		t.Fatalf("select: %v", err)
	}
	if k != "b" || v != -2.25 || ok {
		t.Fatalf("got %q %v %v", k, v, ok)
	}

	// Multi-statement simple query.
	if _, err := conn.Exec(ctx, `BEGIN; UPDATE t SET v = 0 WHERE k = 'a'; COMMIT`); err != nil {
		t.Fatalf("multi: %v", err)
	}
	if err := conn.QueryRow(ctx, `SELECT v FROM t WHERE k = 'a'`).Scan(&v); err != nil || v != 0 {
		t.Fatalf("after multi: %v %v", v, err)
	}
}

// TestPGWireDatabaseSQL exercises the database/sql adapter (pgx stdlib).
func TestPGWireDatabaseSQL(t *testing.T) {
	tc := Start(t, 1)

	db, err := sql.Open("pgx", pgURL(tc, 0))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()
	db.SetMaxOpenConns(2)

	if err := db.Ping(); err != nil {
		t.Fatalf("ping: %v", err)
	}
	if _, err := db.Exec(`CREATE TABLE kv (k INT8 PRIMARY KEY, v TEXT)`); err != nil {
		t.Fatalf("create: %v", err)
	}
	if _, err := db.Exec(`INSERT INTO kv VALUES ($1, $2)`, 1, "hello"); err != nil {
		t.Fatalf("insert: %v", err)
	}
	tx, err := db.Begin()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec(`UPDATE kv SET v = $1 WHERE k = $2`, "world", 1); err != nil {
		t.Fatalf("update: %v", err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("commit: %v", err)
	}
	var v string
	if err := db.QueryRow(`SELECT v FROM kv WHERE k = $1`, 1).Scan(&v); err != nil || v != "world" {
		t.Fatalf("select: %q %v", v, err)
	}
}

// TestPGWireSerializationError verifies 40001 reaches clients under
// contention on explicit transactions.
func TestPGWireSerializationError(t *testing.T) {
	tc := Start(t, 1)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	c1, err := pgx.Connect(ctx, pgURL(tc, 0))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = c1.Close(ctx) }()
	c2, err := pgx.Connect(ctx, pgURL(tc, 0))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = c2.Close(ctx) }()

	if _, err := c1.Exec(ctx, `CREATE TABLE c (k INT8 PRIMARY KEY, v INT8)`); err != nil {
		t.Fatal(err)
	}
	if _, err := c1.Exec(ctx, `INSERT INTO c VALUES (1, 0)`); err != nil {
		t.Fatal(err)
	}

	tx1, err := c1.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := tx1.Exec(ctx, `UPDATE c SET v = 1 WHERE k = 1`); err != nil {
		t.Fatalf("tx1 update: %v", err)
	}
	tx2, err := c2.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	_, err2 := tx2.Exec(ctx, `UPDATE c SET v = 2 WHERE k = 1`)
	if err2 == nil {
		// tx2 won the conflict; tx1 must fail.
		if err := tx2.Commit(ctx); err != nil {
			t.Fatalf("tx2 commit: %v", err)
		}
		if err := tx1.Commit(ctx); err == nil {
			t.Fatal("both conflicting transactions committed")
		}
	} else {
		var pgErr *pgconn.PgError
		if !errors.As(err2, &pgErr) || pgErr.Code != "40001" {
			t.Fatalf("expected 40001, got %v", err2)
		}
		_ = tx2.Rollback(ctx)
		if err := tx1.Commit(ctx); err != nil {
			t.Fatalf("tx1 commit: %v", err)
		}
	}
}
