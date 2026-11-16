package testcluster

import (
	"context"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

// TestAddConstraintIfNotExists (issue #322): ALTER TABLE ... ADD
// CONSTRAINT IF NOT EXISTS name ..., a datax extension, does nothing but
// say so when the table has a constraint of that name — whatever its
// definition — and adds the constraint otherwise. The IF NOT EXISTS
// forms PostgreSQL has (ADD COLUMN, CREATE INDEX) work alongside.
func TestAddConstraintIfNotExists(t *testing.T) {
	tc := Start(t, 1)
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()
	var mu sync.Mutex
	var notices []string
	cfg, err := pgx.ParseConfig(pgURL(tc, 0))
	if err != nil {
		t.Fatal(err)
	}
	cfg.OnNotice = func(_ *pgconn.PgConn, n *pgconn.Notice) {
		mu.Lock()
		notices = append(notices, n.Message)
		mu.Unlock()
	}
	conn, err := pgx.ConnectConfig(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close(context.Background())
	took := func() []string {
		mu.Lock()
		defer mu.Unlock()
		out := notices
		notices = nil
		return out
	}

	mustExec(t, ctx, conn, `CREATE TABLE p (id INT PRIMARY KEY)`)
	mustExec(t, ctx, conn, `CREATE TABLE t (id INT PRIMARY KEY, a INT, b INT, c INT, CONSTRAINT a_pos CHECK (a > 0))`)
	mustExec(t, ctx, conn, `CREATE UNIQUE INDEX c_idx ON t (c)`)
	took()

	// An existing name: nothing changes, whatever the new definition.
	mustExec(t, ctx, conn, `ALTER TABLE t ADD CONSTRAINT IF NOT EXISTS a_pos CHECK (a > 100)`)
	if got := took(); len(got) != 1 || got[0] != `constraint "a_pos" for relation "t" already exists, skipping` {
		t.Fatalf("notices: %q", got)
	}
	wantRows(t, ctx, conn, `SELECT pg_get_constraintdef(oid) FROM pg_constraint WHERE conname = 'a_pos'`, "CHECK (a > 0)")
	mustExec(t, ctx, conn, `INSERT INTO t VALUES (1, 5, 1, 1)`)
	mustExec(t, ctx, conn, `ALTER TABLE t ADD CONSTRAINT IF NOT EXISTS t_pkey CHECK (id > 0)`)
	if got := took(); len(got) != 1 || !strings.Contains(got[0], `"t_pkey"`) {
		t.Fatalf("notices: %q", got)
	}
	wantSQLState(t, ctx, conn, "42710", `ALTER TABLE t ADD CONSTRAINT a_pos CHECK (a > 100)`)

	// A new name: the constraint is added, existing rows validated.
	mustExec(t, ctx, conn, `ALTER TABLE t ADD CONSTRAINT IF NOT EXISTS b_key UNIQUE (b)`)
	mustExec(t, ctx, conn, `ALTER TABLE t ADD CONSTRAINT IF NOT EXISTS t_fk FOREIGN KEY (b) REFERENCES p (id) NOT VALID`)
	if got := took(); len(got) != 0 {
		t.Fatalf("notices: %q", got)
	}
	wantSQLState(t, ctx, conn, "23505", `INSERT INTO t VALUES (2, 5, 1, 2)`)
	wantSQLState(t, ctx, conn, "23503", `INSERT INTO t VALUES (2, 5, 2, 2)`)
	wantSQLState(t, ctx, conn, "23514", `ALTER TABLE t ADD CONSTRAINT IF NOT EXISTS a_big CHECK (a > 100)`)
	mustExec(t, ctx, conn, `ALTER TABLE t ADD CONSTRAINT IF NOT EXISTS b_key UNIQUE (b)`)
	if got := took(); len(got) != 1 {
		t.Fatalf("notices: %q", got)
	}
	// An index made by CREATE UNIQUE INDEX is not a constraint: its name
	// is taken all the same.
	wantSQLState(t, ctx, conn, "42710", `ALTER TABLE t ADD CONSTRAINT IF NOT EXISTS c_idx UNIQUE (c)`)
	// A missing table under ALTER TABLE IF EXISTS.
	mustExec(t, ctx, conn, `ALTER TABLE IF EXISTS nope ADD CONSTRAINT IF NOT EXISTS x CHECK (true)`)
	wantSQLState(t, ctx, conn, "42P01", `ALTER TABLE nope ADD CONSTRAINT IF NOT EXISTS x CHECK (true)`)
	// The forms PostgreSQL has.
	mustExec(t, ctx, conn, `ALTER TABLE t ADD COLUMN IF NOT EXISTS a INT`)
	mustExec(t, ctx, conn, `ALTER TABLE t ADD COLUMN IF NOT EXISTS d TEXT`)
	mustExec(t, ctx, conn, `CREATE INDEX IF NOT EXISTS c_idx ON t (a)`)
	mustExec(t, ctx, conn, `CREATE UNIQUE INDEX IF NOT EXISTS d_idx ON t (d)`)
	wantRows(t, ctx, conn, `SELECT indexdef FROM pg_indexes WHERE indexname = 'c_idx'`, "CREATE UNIQUE INDEX c_idx ON public.t USING btree (c)")
}
