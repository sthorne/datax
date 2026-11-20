package testcluster

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"
)

// TestImplicitDDLNeverSerializationFailure (issue #392): several
// connections on one node run autocommit DDL at once — CREATE TABLE,
// CREATE INDEX, DROP TABLE, and the other catalog writers — all of them
// writing the namespace and the descriptor-ID counter. A conflict
// between two of them is retried inside the server, as an implicit DML
// statement's is: PostgreSQL never fails a CREATE TABLE with 40001, and
// the migration tools that run autocommit DDL do not retry it, so no
// client may see one.
func TestImplicitDDLNeverSerializationFailure(t *testing.T) {
	tc := Start(t, 1)
	ctx, cancel := context.WithTimeout(context.Background(), 6*time.Minute)
	defer cancel()

	const workers = 8
	iters := 25
	if testing.Short() {
		iters = 8
	}
	var wg sync.WaitGroup
	var mu sync.Mutex
	var failures []string
	statements := 0
	for w := 0; w < workers; w++ {
		conn := pgConn(t, ctx, tc, 0)
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for i := 0; i < iters && ctx.Err() == nil; i++ {
				tbl := fmt.Sprintf("w%d_t%d", w, i)
				stmts := []string{
					fmt.Sprintf(`CREATE TABLE %s (id INT PRIMARY KEY, v INT, s TEXT)`, tbl),
					fmt.Sprintf(`CREATE INDEX %s_v ON %s (v)`, tbl, tbl),
					fmt.Sprintf(`ALTER TABLE %s ADD COLUMN extra INT`, tbl),
					fmt.Sprintf(`CREATE SEQUENCE %s_seq`, tbl),
					fmt.Sprintf(`CREATE VIEW %s_view AS SELECT id, v FROM %s`, tbl, tbl),
					fmt.Sprintf(`CREATE SCHEMA %s_sch`, tbl),
					fmt.Sprintf(`CREATE TYPE %s_mood AS ENUM ('a', 'b')`, tbl),
					fmt.Sprintf(`DROP VIEW %s_view`, tbl),
					fmt.Sprintf(`DROP SEQUENCE %s_seq`, tbl),
					fmt.Sprintf(`DROP SCHEMA %s_sch`, tbl),
					fmt.Sprintf(`DROP TYPE %s_mood`, tbl),
				}
				if i%2 == 0 {
					stmts = append(stmts, fmt.Sprintf(`DROP TABLE %s`, tbl))
				}
				if i%5 == 0 {
					stmts = append(stmts, fmt.Sprintf(`CREATE DATABASE %s_db`, tbl), fmt.Sprintf(`DROP DATABASE %s_db`, tbl))
				}
				for _, q := range stmts {
					_, err := conn.Exec(ctx, q)
					mu.Lock()
					statements++
					if err != nil {
						failures = append(failures, fmt.Sprintf("%s: [%s] %v", q, sqlState(err), err))
					}
					mu.Unlock()
					if err != nil && sqlState(err) != "40001" {
						return // a different failure: the rest of this worker's run would only cascade
					}
				}
			}
		}(w)
	}
	wg.Wait()
	if ctx.Err() != nil {
		t.Fatalf("the workers ran out of time")
	}
	if len(failures) > 0 {
		n := len(failures)
		if n > 10 {
			failures = failures[:10]
		}
		t.Fatalf("%d of %d autocommit DDL statements failed:\n%s", n, statements, strings.Join(failures, "\n"))
	}
}

// TestExplicitDDLConflictStillSerializationFailure: inside an explicit
// transaction block the server cannot re-run the client's earlier
// statements, so a block whose DDL loses a conflict still fails with
// 40001, and the client retries the block.
func TestExplicitDDLConflictStillSerializationFailure(t *testing.T) {
	tc := Start(t, 1)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	a, b := pgConn(t, ctx, tc, 0), pgConn(t, ctx, tc, 0)
	mustExec(t, ctx, a, `CREATE TABLE ex (id INT PRIMARY KEY, v INT)`)
	mustExec(t, ctx, a, `INSERT INTO ex VALUES (1, 0)`)

	// The block reads ex's row; b then changes that row and creates a
	// table (writing the descriptor-ID counter), and commits; the
	// block's CREATE TABLE writes the counter too, above b's write, so
	// its read can no longer be moved up to where it would commit.
	tx, err := a.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = tx.Rollback(context.Background()) }()
	var v int
	if err := tx.QueryRow(ctx, `SELECT v FROM ex WHERE id = 1`).Scan(&v); err != nil {
		t.Fatal(err)
	}
	mustExec(t, ctx, b, `UPDATE ex SET v = 1 WHERE id = 1`)
	mustExec(t, ctx, b, `CREATE TABLE ex_b (id INT PRIMARY KEY)`)
	_, err = tx.Exec(ctx, `CREATE TABLE ex_a (id INT PRIMARY KEY)`)
	if err == nil {
		err = tx.Commit(ctx)
	}
	if code := sqlState(err); code != "40001" {
		t.Fatalf("an explicit block whose DDL lost a conflict: got %v (SQLSTATE %q), want 40001", err, code)
	}
	// Nothing of the block landed.
	var n int
	if err := b.QueryRow(ctx, `SELECT count(*) FROM pg_class WHERE relname = 'ex_a'`).Scan(&n); err != nil || n != 0 {
		t.Fatalf("ex_a listed %d times after its block failed: %v", n, err)
	}
}
