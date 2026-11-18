package testcluster

import (
	"context"
	"fmt"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/sthorne/datax/pkg/keys"
	"github.com/sthorne/datax/pkg/kvclient"
	"github.com/sthorne/datax/pkg/server"
	"github.com/sthorne/datax/pkg/sql"
	"github.com/sthorne/datax/pkg/sql/catalog"
)

// dropDatabaseSchema fills database db with the shapes a real schema
// has: tables with secondary and unique indexes, foreign keys between
// them, serial columns (and so sequences), standalone sequences, views
// over the tables and an enum-typed column.
func dropDatabaseSchema(ctx context.Context, conn *pgx.Conn, db string, tables int) error {
	stmts := []string{
		`CREATE DATABASE ` + db,
		`USE ` + db,
		`CREATE TYPE IF NOT EXISTS dd_mood AS ENUM ('sad', 'ok', 'happy')`,
	}
	for i := 0; i < tables; i++ {
		stmts = append(stmts,
			fmt.Sprintf(`CREATE TABLE p%d (id SERIAL PRIMARY KEY, name TEXT NOT NULL, mood dd_mood, code TEXT UNIQUE)`, i),
			fmt.Sprintf(`CREATE TABLE c%d (id INT PRIMARY KEY, p INT REFERENCES p%d (id) ON DELETE CASCADE, note TEXT)`, i, i),
			fmt.Sprintf(`CREATE INDEX c%d_note ON c%d (note)`, i, i),
			fmt.Sprintf(`CREATE SEQUENCE s%d`, i),
			fmt.Sprintf(`CREATE VIEW v%d AS SELECT p%d.id, p%d.name, c%d.note FROM p%d JOIN c%d ON c%d.p = p%d.id`, i, i, i, i, i, i, i, i),
			fmt.Sprintf(`INSERT INTO p%d (name, mood, code) VALUES ('a', 'ok', 'x'), ('b', 'sad', NULL)`, i),
			fmt.Sprintf(`INSERT INTO c%d VALUES (1, 1, 'n1'), (2, 2, 'n2')`, i),
		)
	}
	stmts = append(stmts, `USE datax`)
	for _, s := range stmts {
		if _, err := conn.Exec(ctx, s); err != nil {
			return fmt.Errorf("%s: %w", s, err)
		}
	}
	return nil
}

// databaseGone checks that nothing of database db is left: it is not
// listed, cannot be used, and no name is left under its id.
func databaseGone(t *testing.T, ctx context.Context, tc *TestCluster, conn *pgx.Conn, db string, id uint64) {
	t.Helper()
	var n int
	if err := conn.QueryRow(ctx, `SELECT count(*) FROM pg_database WHERE datname = $1`, db).Scan(&n); err != nil {
		t.Fatal(err)
	}
	if n != 0 {
		t.Fatalf("database %s still listed after the drop", db)
	}
	if _, err := conn.Exec(ctx, `USE `+db); err == nil || !strings.Contains(err.Error(), "3D000") {
		t.Fatalf("USE %s after the drop: %v", db, err)
	}
	lo, hi := keys.TableNamespaceSpan(id)
	var left int
	if err := tc.Nodes[0].DB().RunTxn(ctx, "test-namespace", func(ctx context.Context, txn *kvclient.Txn) error {
		kvs, err := txn.Scan(ctx, lo, hi, 0)
		left = len(kvs)
		return err
	}); err != nil {
		t.Fatal(err)
	}
	if left != 0 {
		t.Fatalf("%d table names left under dropped database %s", left, db)
	}
}

// databaseID reads a database's id.
func databaseID(t *testing.T, ctx context.Context, tc *TestCluster, name string) uint64 {
	t.Helper()
	var id uint64
	if err := tc.Nodes[0].DB().RunTxn(ctx, "test-database-id", func(ctx context.Context, txn *kvclient.Txn) error {
		d, err := catalog.LookupDatabase(ctx, txn, name)
		if err == nil {
			id = d.ID
		}
		return err
	}); err != nil {
		t.Fatal(err)
	}
	return id
}

// TestDropDatabaseCascadeLoop (issue #322): one session, nothing else
// running, creates a database of tables, views, sequences and a type and
// drops it with CASCADE, over and over; nothing of it is left after each
// drop. DATAX_DROP_DB_SOAK=<duration> runs it for that long, with a
// larger schema.
func TestDropDatabaseCascadeLoop(t *testing.T) {
	rounds, tables := 3, 8
	var soak time.Duration
	if v := os.Getenv("DATAX_DROP_DB_SOAK"); v != "" {
		d, err := time.ParseDuration(v)
		if err != nil {
			t.Fatalf("DATAX_DROP_DB_SOAK: %v", err)
		}
		soak, rounds, tables = d, 1<<30, 40
	}
	tc := Start(t, 3)
	timeout := 240 * time.Second
	if soak > 0 {
		timeout = soak + 5*time.Minute
	}
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	conn := pgConn(t, ctx, tc, 0)
	start := time.Now()
	for round := 0; round < rounds; round++ {
		if soak > 0 && time.Since(start) > soak {
			break
		}
		db := fmt.Sprintf("dd%d", round)
		if err := dropDatabaseSchema(ctx, conn, db, tables); err != nil {
			t.Fatalf("round %d: %v", round, err)
		}
		id := databaseID(t, ctx, tc, db)
		t0 := time.Now()
		if _, err := conn.Exec(ctx, `DROP DATABASE `+db+` CASCADE`); err != nil {
			t.Fatalf("round %d: DROP DATABASE CASCADE after %s: %v", round, time.Since(t0), err)
		}
		if soak > 0 {
			t.Logf("round %d: dropped in %s", round, time.Since(t0))
		}
		databaseGone(t, ctx, tc, conn, db, id)
	}
}

// bigDatabase creates database db holding n tables (and a view and a
// sequence per ten) through one multi-statement query.
func bigDatabase(t *testing.T, ctx context.Context, conn *pgx.Conn, db string, n int) {
	t.Helper()
	var b strings.Builder
	fmt.Fprintf(&b, "CREATE DATABASE %s;", db)
	for i := 0; i < n; i++ {
		fmt.Fprintf(&b, "CREATE TABLE %s.t%d (id INT PRIMARY KEY, v TEXT);", db, i)
		if i%10 == 0 {
			fmt.Fprintf(&b, "CREATE VIEW %s.v%d AS SELECT id FROM %s.t%d;", db, i, db, i)
			fmt.Fprintf(&b, "CREATE SEQUENCE %s.s%d;", db, i)
		}
	}
	mustExec(t, ctx, conn, b.String())
}

// TestDropDatabaseCascadeSlowCommit (issue #322): DROP DATABASE of a
// database of hundreds of tables reads more keys than a transaction
// tracked for its refresh (512 spans), and gave its read set up. A drop
// that then outlived the closed-timestamp lag (3 s by default; long
// enough on a large database, or under the race detector) had its commit
// pushed above it, could not refresh, and restarted — every attempt
// alike, until the statement gave up after about a minute with "write
// timestamp diverged from read timestamp" (40001). Here the lag is
// short and the commit is held past it.
func TestDropDatabaseCascadeSlowCommit(t *testing.T) {
	tc := StartWithOptions(t, 3, func(c *server.Config) { c.ClosedTimestampLag = 100 * time.Millisecond })
	ctx, cancel := context.WithTimeout(context.Background(), 180*time.Second)
	defer cancel()
	conn := pgConn(t, ctx, tc, 0)
	bigDatabase(t, ctx, conn, "big", 600)
	id := databaseID(t, ctx, tc, "big")

	var hold atomic.Bool
	var attempts atomic.Int32
	sql.TestingBeforeImplicitCommit = func() {
		if hold.Load() {
			attempts.Add(1)
			time.Sleep(400 * time.Millisecond)
		}
	}
	defer func() { sql.TestingBeforeImplicitCommit = nil }()
	hold.Store(true)
	t0 := time.Now()
	_, err := conn.Exec(ctx, `DROP DATABASE big CASCADE`)
	hold.Store(false)
	if err != nil {
		t.Fatalf("DROP DATABASE CASCADE after %s and %d attempts: %v", time.Since(t0), attempts.Load(), err)
	}
	if n := attempts.Load(); n > 2 {
		t.Fatalf("the drop took %d attempts", n)
	}
	databaseGone(t, ctx, tc, conn, "big", id)
}

// TestDropDatabaseCascadeContended (issue #322): DROP DATABASE CASCADE
// racing sessions that create tables in the same database either drops
// the database with every table in it, or fails with 40001 or because a
// table appeared and leaves the database whole — never half dropped.
func TestDropDatabaseCascadeContended(t *testing.T) {
	tc := Start(t, 3)
	ctx, cancel := context.WithTimeout(context.Background(), 180*time.Second)
	defer cancel()
	// The contended phase has its own budget, and the drop after it its
	// own: a drop that keeps losing to the writers runs for tens of
	// seconds per try before its 40001 (on a slow runner under -race,
	// long enough to spend the whole test's deadline), and the drop that
	// must succeed once they stop would then start with none left.
	contended, cancelContended := context.WithTimeout(ctx, 150*time.Second)
	defer cancelContended()
	conn := pgConn(t, ctx, tc, 0)
	bigDatabase(t, ctx, conn, "busy", 50)
	id := databaseID(t, ctx, tc, "busy")

	var wg sync.WaitGroup
	stop := make(chan struct{})
	var created atomic.Int32
	for w := 1; w <= 2; w++ {
		wc := pgConn(t, ctx, tc, w)
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for i := 0; ; i++ {
				select {
				case <-stop:
					return
				default:
				}
				if _, err := wc.Exec(ctx, fmt.Sprintf(`CREATE TABLE busy.w%d_%d (id INT PRIMARY KEY)`, w, i)); err == nil {
					created.Add(1)
				}
				time.Sleep(20 * time.Millisecond)
			}
		}(w)
	}
	time.Sleep(200 * time.Millisecond)
	// While the writers run the drop may lose to them: then it fails
	// with 40001, having changed nothing.
	dropped := false
	for try := 0; try < 3 && !dropped && contended.Err() == nil; try++ {
		_, err := conn.Exec(contended, `DROP DATABASE busy CASCADE`)
		if err == nil {
			dropped = true
			break
		}
		if contended.Err() != nil {
			break // out of budget for the contended tries; the drop below must succeed
		}
		if code := sqlState(err); code != "40001" {
			t.Fatalf("DROP DATABASE under contention: %v", err)
		}
		var n int
		if err := conn.QueryRow(ctx, `SELECT count(*) FROM pg_database WHERE datname = 'busy'`).Scan(&n); err != nil || n != 1 {
			t.Fatalf("after a refused drop the database is listed %d times: %v", n, err)
		}
		mustExec(t, ctx, conn, `SELECT count(*) FROM busy.t49`)
	}
	close(stop)
	wg.Wait()
	// A cancelled statement closes its pgx connection, so the rest runs on
	// a fresh one under its own deadline.
	final, cancelFinal := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancelFinal()
	fconn := pgConn(t, final, tc, 0)
	if !dropped {
		mustExec(t, final, fconn, `DROP DATABASE busy CASCADE`)
	}
	t.Logf("%d tables created alongside", created.Load())
	databaseGone(t, final, tc, fconn, "busy", id)
}
