package testcluster

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/sthorne/datax/pkg/sql"
	"github.com/sthorne/datax/pkg/sql/catalog"
)

func explainPlan(t *testing.T, ctx context.Context, s *sql.Session, query string) string {
	t.Helper()
	res := execSQL(t, ctx, s, "EXPLAIN "+query)
	if len(res.Rows) != 1 || len(res.Rows[0]) != 1 {
		t.Fatalf("EXPLAIN returned %+v", res.Rows)
	}
	return res.Rows[0][0].S
}

func TestSecondaryIndexes(t *testing.T) {
	tc := Start(t, 3)
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()
	s := sql.NewSession(tc.Nodes[0].DB(), catalog.NewAccessor())

	execSQL(t, ctx, s, `CREATE TABLE users (id INT PRIMARY KEY, email TEXT, city TEXT, age INT)`)
	execSQL(t, ctx, s, `INSERT INTO users VALUES
		(1, 'ann@x.com', 'oslo', 30),
		(2, 'bob@x.com', 'bergen', 40),
		(3, 'cat@x.com', 'oslo', 25),
		(4, NULL, 'oslo', 35)`)

	// Backfill covers existing rows; NULL rows are simply absent from the
	// (non-unique) index.
	execSQL(t, ctx, s, `CREATE INDEX by_city ON users (city)`)
	if _, serr := trySQL(ctx, s, `CREATE INDEX by_city ON users (age)`); serr == nil {
		t.Fatal("duplicate index name accepted")
	}
	// A row with NULL in the indexed column makes a UNIQUE backfill fail.
	if _, serr := trySQL(ctx, s, `CREATE UNIQUE INDEX by_email ON users (email)`); serr == nil {
		t.Fatal("unique backfill over a NULL row succeeded")
	}

	// Plan selection.
	if p := explainPlan(t, ctx, s, `SELECT id FROM users WHERE id = 1`); p != "point lookup on primary key" {
		t.Fatalf("plan: %s", p)
	}
	if p := explainPlan(t, ctx, s, `SELECT id FROM users WHERE city = 'oslo'`); p != `scan of index "by_city" (1 column prefix) + primary key join` {
		t.Fatalf("plan: %s", p)
	}
	if p := explainPlan(t, ctx, s, `SELECT id FROM users WHERE age > 30`); p != "full table scan" {
		t.Fatalf("plan: %s", p)
	}

	// Index scan returns exactly the matching rows.
	res := execSQL(t, ctx, s, `SELECT id FROM users WHERE city = 'oslo'`)
	if len(res.Rows) != 3 {
		t.Fatalf("oslo rows: %+v", res.Rows)
	}

	// Index maintenance across UPDATE and DELETE.
	execSQL(t, ctx, s, `UPDATE users SET city = 'tromso' WHERE id = 3`)
	if res := execSQL(t, ctx, s, `SELECT id FROM users WHERE city = 'oslo'`); len(res.Rows) != 2 {
		t.Fatalf("after update: %+v", res.Rows)
	}
	if res := execSQL(t, ctx, s, `SELECT id FROM users WHERE city = 'tromso'`); len(res.Rows) != 1 || res.Rows[0][0].I != 3 {
		t.Fatalf("after update: %+v", res.Rows)
	}
	execSQL(t, ctx, s, `DELETE FROM users WHERE id = 1`)
	if res := execSQL(t, ctx, s, `SELECT id FROM users WHERE city = 'oslo'`); len(res.Rows) != 1 || res.Rows[0][0].I != 4 {
		t.Fatalf("after delete: %+v", res.Rows)
	}
}

func TestUniqueIndex(t *testing.T) {
	tc := Start(t, 3)
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()
	s := sql.NewSession(tc.Nodes[0].DB(), catalog.NewAccessor())

	execSQL(t, ctx, s, `CREATE TABLE users (id INT PRIMARY KEY, email TEXT NOT NULL)`)
	execSQL(t, ctx, s, `INSERT INTO users VALUES (1, 'ann@x.com'), (2, 'bob@x.com')`)
	execSQL(t, ctx, s, `CREATE UNIQUE INDEX by_email ON users (email)`)

	// Unique-point plan and lookup.
	if p := explainPlan(t, ctx, s, `SELECT id FROM users WHERE email = 'ann@x.com'`); p != `point lookup via unique index "by_email"` {
		t.Fatalf("plan: %s", p)
	}
	if res := execSQL(t, ctx, s, `SELECT id FROM users WHERE email = 'bob@x.com'`); len(res.Rows) != 1 || res.Rows[0][0].I != 2 {
		t.Fatalf("unique lookup: %+v", res.Rows)
	}

	// Violations: INSERT, multi-row INSERT, UPDATE.
	if _, serr := trySQL(ctx, s, `INSERT INTO users VALUES (3, 'ann@x.com')`); serr == nil || serr.Code != sql.CodeUniqueViolation {
		t.Fatalf("insert violation: %v", serr)
	}
	if _, serr := trySQL(ctx, s, `INSERT INTO users VALUES (4, 'x@x.com'), (5, 'x@x.com')`); serr == nil || serr.Code != sql.CodeUniqueViolation {
		t.Fatalf("same-statement violation: %v", serr)
	}
	if _, serr := trySQL(ctx, s, `UPDATE users SET email = 'ann@x.com' WHERE id = 2`); serr == nil || serr.Code != sql.CodeUniqueViolation {
		t.Fatalf("update violation: %v", serr)
	}
	// Non-conflicting update moves the entry.
	execSQL(t, ctx, s, `UPDATE users SET email = 'bob@y.com' WHERE id = 2`)
	if res := execSQL(t, ctx, s, `SELECT id FROM users WHERE email = 'bob@y.com'`); len(res.Rows) != 1 || res.Rows[0][0].I != 2 {
		t.Fatalf("moved entry: %+v", res.Rows)
	}
	if res := execSQL(t, ctx, s, `SELECT id FROM users WHERE email = 'bob@x.com'`); len(res.Rows) != 0 {
		t.Fatalf("stale entry: %+v", res.Rows)
	}
	// NULL into a unique-indexed column is rejected (documented divergence).
	if _, serr := trySQL(ctx, s, `INSERT INTO users (id, email) VALUES (9, NULL)`); serr == nil {
		t.Fatal("NULL accepted into unique index")
	}

	// Unique backfill rejects duplicates.
	execSQL(t, ctx, s, `CREATE TABLE dups (id INT PRIMARY KEY, v TEXT)`)
	execSQL(t, ctx, s, `INSERT INTO dups VALUES (1, 'a'), (2, 'a')`)
	if _, serr := trySQL(ctx, s, `CREATE UNIQUE INDEX by_v ON dups (v)`); serr == nil || serr.Code != sql.CodeUniqueViolation {
		t.Fatalf("backfill duplicate: %v", serr)
	}
}

// TestUniqueIndexRacingTxns: two transactions inserting the same unique
// value cannot both commit — intents make the conflict visible.
func TestUniqueIndexRacingTxns(t *testing.T) {
	tc := Start(t, 3)
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()
	setup := sql.NewSession(tc.Nodes[0].DB(), catalog.NewAccessor())
	execSQL(t, ctx, setup, `CREATE TABLE u (id INT PRIMARY KEY, v TEXT)`)
	execSQL(t, ctx, setup, `CREATE UNIQUE INDEX by_v ON u (v)`)

	var wg sync.WaitGroup
	errs := make([]error, 8)
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			s := sql.NewSession(tc.Nodes[i%3].DB(), catalog.NewAccessor())
			_, serr := trySQL(ctx, s, fmt.Sprintf(`INSERT INTO u VALUES (%d, 'contested')`, i+1))
			if serr != nil {
				errs[i] = fmt.Errorf("[%s] %s", serr.Code, serr.Msg)
			}
		}(i)
	}
	wg.Wait()

	succeeded := 0
	for _, err := range errs {
		if err == nil {
			succeeded++
		}
	}
	if succeeded != 1 {
		t.Fatalf("%d racing inserts of the same unique value succeeded (want exactly 1): %v", succeeded, errs)
	}
	if res := execSQL(t, ctx, setup, `SELECT id FROM u WHERE v = 'contested'`); len(res.Rows) != 1 {
		t.Fatalf("final rows: %+v", res.Rows)
	}
}
