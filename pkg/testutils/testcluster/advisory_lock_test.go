package testcluster

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"hash/crc32"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	_ "github.com/jackc/pgx/v5/stdlib"

	"github.com/sthorne/datax/pkg/server"
)

// The advisory-lock tests (issue #353) run with a short lease, so that
// a holder's node that stops or is cut off frees its locks quickly.
const advisoryTestTTL = 2 * time.Second

func advisoryCluster(t *testing.T, nodes int) *TestCluster {
	return StartWithOptions(t, nodes, func(c *server.Config) { c.AdvisoryLockTTL = advisoryTestTTL })
}

// noticeConn is a connection that records the notices and warnings it
// is sent.
type noticeConn struct {
	*pgx.Conn
	mu      sync.Mutex
	notices []*pgconn.Notice
}

func (c *noticeConn) take() []*pgconn.Notice {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := c.notices
	c.notices = nil
	return out
}

func advisoryConn(t *testing.T, ctx context.Context, tc *TestCluster, node int) *noticeConn {
	t.Helper()
	cfg, err := pgx.ParseConfig(pgURL(tc, node))
	if err != nil {
		t.Fatal(err)
	}
	nc := &noticeConn{}
	cfg.OnNotice = func(_ *pgconn.PgConn, n *pgconn.Notice) {
		nc.mu.Lock()
		nc.notices = append(nc.notices, n)
		nc.mu.Unlock()
	}
	conn, err := pgx.ConnectConfig(ctx, cfg)
	if err != nil {
		t.Fatalf("connect to node %d: %v", node, err)
	}
	t.Cleanup(func() { _ = conn.Close(context.Background()) })
	nc.Conn = conn
	return nc
}

func queryBool(t *testing.T, ctx context.Context, conn *pgx.Conn, q string, args ...any) bool {
	t.Helper()
	var b bool
	if err := conn.QueryRow(ctx, q, args...).Scan(&b); err != nil {
		t.Fatalf("%s: %v", q, err)
	}
	return b
}

func wantTry(t *testing.T, ctx context.Context, conn *pgx.Conn, q string, want bool) {
	t.Helper()
	if got := queryBool(t, ctx, conn, q); got != want {
		t.Fatalf("%s = %v, want %v", q, got, want)
	}
}

// lockInBackground runs a blocking lock statement and reports its
// outcome on the returned channel.
func lockInBackground(ctx context.Context, conn *pgx.Conn, q string, args ...any) <-chan error {
	done := make(chan error, 1)
	go func() {
		_, err := conn.Exec(ctx, q, args...)
		done <- err
	}()
	return done
}

func stillWaiting(t *testing.T, done <-chan error, d time.Duration) {
	t.Helper()
	select {
	case err := <-done:
		t.Fatalf("the lock was granted (err %v) while another session held it", err)
	case <-time.After(d):
	}
}

func granted(t *testing.T, done <-chan error, within time.Duration) {
	t.Helper()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("waiting for the lock: %v", err)
		}
	case <-time.After(within):
		t.Fatalf("the lock was not granted within %s of its release", within)
	}
}

func backendPID(t *testing.T, ctx context.Context, conn *pgx.Conn) int32 {
	t.Helper()
	var pid int32
	if err := conn.QueryRow(ctx, `SELECT pg_backend_pid()`).Scan(&pid); err != nil {
		t.Fatal(err)
	}
	return pid
}

// TestAdvisoryLocks (issue #353): pg_advisory_lock and its relatives
// through a stock driver, with PostgreSQL's semantics, on one node and
// across the cluster.
func TestAdvisoryLocks(t *testing.T) {
	tc := advisoryCluster(t, 3)
	ctx, cancel := context.WithTimeout(context.Background(), 240*time.Second)
	defer cancel()

	for _, tt := range []struct {
		name         string
		holder, peer int
	}{{"same node", 0, 0}, {"across nodes", 0, 2}} {
		t.Run(tt.name, func(t *testing.T) {
			a, b := advisoryConn(t, ctx, tc, tt.holder), advisoryConn(t, ctx, tc, tt.peer)
			key := 100 + tt.peer
			mustExec(t, ctx, a.Conn, fmt.Sprintf(`SELECT pg_advisory_lock(%d)`, key))
			wantTry(t, ctx, b.Conn, fmt.Sprintf(`SELECT pg_try_advisory_lock(%d)`, key), false)
			done := lockInBackground(ctx, b.Conn, fmt.Sprintf(`SELECT pg_advisory_lock(%d)`, key))
			stillWaiting(t, done, 300*time.Millisecond)
			wantTry(t, ctx, a.Conn, fmt.Sprintf(`SELECT pg_advisory_unlock(%d)`, key), true)
			granted(t, done, 5*time.Second)
			wantTry(t, ctx, a.Conn, fmt.Sprintf(`SELECT pg_try_advisory_lock(%d)`, key), false)
			wantTry(t, ctx, b.Conn, fmt.Sprintf(`SELECT pg_advisory_unlock(%d)`, key), true)
			wantTry(t, ctx, a.Conn, fmt.Sprintf(`SELECT pg_try_advisory_lock(%d)`, key), true)
			mustExec(t, ctx, a.Conn, `SELECT pg_advisory_unlock_all()`)
		})
	}

	t.Run("mutual exclusion", func(t *testing.T) {
		// Sessions on every node take one lock in turn, blocking and
		// trying: never two inside at once.
		var inside, entered atomic.Int32
		var wg sync.WaitGroup
		errs := make(chan error, 6)
		for w := range 6 {
			conn := advisoryConn(t, ctx, tc, w%3)
			wg.Add(1)
			go func() {
				defer wg.Done()
				for i := range 10 {
					q := `SELECT pg_advisory_lock(77)`
					if (w+i)%2 == 1 {
						var ok bool
						if err := conn.QueryRow(ctx, `SELECT pg_try_advisory_lock(77)`).Scan(&ok); err != nil {
							errs <- err
							return
						}
						if !ok {
							continue
						}
						q = ""
					}
					if q != "" {
						if _, err := conn.Exec(ctx, q); err != nil {
							errs <- err
							return
						}
					}
					if n := inside.Add(1); n != 1 {
						errs <- fmt.Errorf("%d sessions hold the lock at once", n)
					}
					entered.Add(1)
					time.Sleep(2 * time.Millisecond)
					inside.Add(-1)
					var ok bool
					if err := conn.QueryRow(ctx, `SELECT pg_advisory_unlock(77)`).Scan(&ok); err != nil || !ok {
						errs <- fmt.Errorf("unlock: %v %v", ok, err)
						return
					}
				}
			}()
		}
		wg.Wait()
		close(errs)
		for err := range errs {
			t.Fatal(err)
		}
		if entered.Load() < 30 {
			t.Fatalf("only %d entries", entered.Load())
		}
	})

	t.Run("key forms", func(t *testing.T) {
		a, b := advisoryConn(t, ctx, tc, 0), advisoryConn(t, ctx, tc, 1)
		// (1, 2) as two int4 keys and 1<<32|2 as one bigint are different
		// locks, as in PostgreSQL.
		wantTry(t, ctx, a.Conn, `SELECT pg_try_advisory_lock(1, 2)`, true)
		wantTry(t, ctx, b.Conn, `SELECT pg_try_advisory_lock(1, 2)`, false)
		wantTry(t, ctx, b.Conn, `SELECT pg_try_advisory_lock(4294967298)`, true)
		wantTry(t, ctx, a.Conn, `SELECT pg_try_advisory_lock(4294967298)`, false)
		// A NULL key takes nothing (the functions are strict).
		var v *bool
		if err := a.QueryRow(ctx, `SELECT pg_try_advisory_lock(NULL)`).Scan(&v); err != nil || v != nil {
			t.Fatalf("pg_try_advisory_lock(NULL) = %v, %v; want NULL", v, err)
		}
		// An int4 key out of range.
		if _, err := a.Exec(ctx, `SELECT pg_advisory_lock(1, 3000000000)`); sqlState(err) != "22003" {
			t.Fatalf("an out-of-range int4 key: %v, want 22003", err)
		}
		// Arguments read once per statement: a column is refused.
		mustExec(t, ctx, a.Conn, `CREATE TABLE IF NOT EXISTS adv_jobs (id INT PRIMARY KEY)`)
		mustExec(t, ctx, a.Conn, `UPSERT INTO adv_jobs VALUES (1)`)
		if _, err := a.Exec(ctx, `SELECT pg_try_advisory_lock(id) FROM adv_jobs`); sqlState(err) != "0A000" {
			t.Fatalf("a column argument: %v, want 0A000", err)
		}
		mustExec(t, ctx, a.Conn, `SELECT pg_advisory_unlock_all()`)
		mustExec(t, ctx, b.Conn, `SELECT pg_advisory_unlock_all()`)
	})

	t.Run("shared and exclusive", func(t *testing.T) {
		a, b, c := advisoryConn(t, ctx, tc, 0), advisoryConn(t, ctx, tc, 1), advisoryConn(t, ctx, tc, 2)
		mustExec(t, ctx, a.Conn, `SELECT pg_advisory_lock_shared(7)`)
		wantTry(t, ctx, b.Conn, `SELECT pg_try_advisory_lock_shared(7)`, true)
		wantTry(t, ctx, c.Conn, `SELECT pg_try_advisory_lock(7)`, false)
		done := lockInBackground(ctx, c.Conn, `SELECT pg_advisory_lock(7)`)
		wantTry(t, ctx, a.Conn, `SELECT pg_advisory_unlock_shared(7)`, true)
		stillWaiting(t, done, 300*time.Millisecond) // b still shares it
		wantTry(t, ctx, b.Conn, `SELECT pg_advisory_unlock_shared(7)`, true)
		granted(t, done, 5*time.Second)
		// Now exclusive: a shared request waits too.
		wantTry(t, ctx, a.Conn, `SELECT pg_try_advisory_lock_shared(7)`, false)
		// A session's own holds never conflict with each other.
		wantTry(t, ctx, c.Conn, `SELECT pg_try_advisory_lock_shared(7)`, true)
		mustExec(t, ctx, c.Conn, `SELECT pg_advisory_unlock_all()`)
		wantTry(t, ctx, a.Conn, `SELECT pg_try_advisory_lock_shared(7)`, true)
		mustExec(t, ctx, a.Conn, `SELECT pg_advisory_unlock_all()`)
	})

	t.Run("reentrant", func(t *testing.T) {
		a, b := advisoryConn(t, ctx, tc, 0), advisoryConn(t, ctx, tc, 1)
		for range 3 {
			mustExec(t, ctx, a.Conn, `SELECT pg_advisory_lock(9)`)
		}
		wantTry(t, ctx, a.Conn, `SELECT pg_try_advisory_lock(9)`, true) // a fourth
		for i := range 4 {
			wantTry(t, ctx, b.Conn, `SELECT pg_try_advisory_lock(9)`, false)
			wantTry(t, ctx, a.Conn, `SELECT pg_advisory_unlock(9)`, true)
			if n := a.take(); len(n) != 0 {
				t.Fatalf("unlock %d warned: %v", i, n[0].Message)
			}
		}
		// Not held any more: false, with PostgreSQL's warning.
		wantTry(t, ctx, a.Conn, `SELECT pg_advisory_unlock(9)`, false)
		n := a.take()
		if len(n) != 1 || n[0].Severity != "WARNING" || n[0].Code != "01000" || n[0].Message != "you don't own a lock of type ExclusiveLock" {
			t.Fatalf("unlock of a lock not held: notices %+v", n)
		}
		wantTry(t, ctx, b.Conn, `SELECT pg_try_advisory_lock(9)`, true)
		wantTry(t, ctx, b.Conn, `SELECT pg_advisory_unlock_shared(9)`, false)
		if n := b.take(); len(n) != 1 || n[0].Message != "you don't own a lock of type ShareLock" {
			t.Fatalf("unlock_shared of an exclusive hold: notices %+v", n)
		}
		mustExec(t, ctx, b.Conn, `SET client_min_messages = error`)
		wantTry(t, ctx, b.Conn, `SELECT pg_advisory_unlock(12345)`, false)
		if n := b.take(); len(n) != 0 {
			t.Fatalf("client_min_messages = error still sent %+v", n)
		}
		mustExec(t, ctx, b.Conn, `SELECT pg_advisory_unlock_all()`)
	})

	t.Run("transaction scope", func(t *testing.T) {
		a, b := advisoryConn(t, ctx, tc, 0), advisoryConn(t, ctx, tc, 2)
		// Released at COMMIT.
		mustExec(t, ctx, a.Conn, `BEGIN`)
		mustExec(t, ctx, a.Conn, `SELECT pg_advisory_xact_lock(11)`)
		wantTry(t, ctx, a.Conn, `SELECT pg_try_advisory_xact_lock(11)`, true) // reentrant
		wantTry(t, ctx, b.Conn, `SELECT pg_try_advisory_lock(11)`, false)
		// pg_advisory_unlock does not release a transaction-level hold.
		wantTry(t, ctx, a.Conn, `SELECT pg_advisory_unlock(11)`, false)
		done := lockInBackground(ctx, b.Conn, `SELECT pg_advisory_lock(11)`)
		stillWaiting(t, done, 300*time.Millisecond)
		mustExec(t, ctx, a.Conn, `COMMIT`)
		granted(t, done, 5*time.Second)
		mustExec(t, ctx, b.Conn, `SELECT pg_advisory_unlock(11)`)

		// Released at ROLLBACK, and by a failed transaction's end.
		mustExec(t, ctx, a.Conn, `BEGIN`)
		wantTry(t, ctx, a.Conn, `SELECT pg_try_advisory_xact_lock_shared(12)`, true)
		wantTry(t, ctx, b.Conn, `SELECT pg_try_advisory_lock(12)`, false)
		_, _ = a.Exec(ctx, `SELECT 1/0`)
		mustExec(t, ctx, a.Conn, `ROLLBACK`)
		wantTry(t, ctx, b.Conn, `SELECT pg_try_advisory_lock(12)`, true)
		mustExec(t, ctx, b.Conn, `SELECT pg_advisory_unlock(12)`)

		// ROLLBACK TO SAVEPOINT gives back what was taken after it.
		mustExec(t, ctx, a.Conn, `BEGIN`)
		mustExec(t, ctx, a.Conn, `SELECT pg_advisory_xact_lock(13)`)
		mustExec(t, ctx, a.Conn, `SAVEPOINT s`)
		mustExec(t, ctx, a.Conn, `SELECT pg_advisory_xact_lock(14)`)
		mustExec(t, ctx, a.Conn, `SELECT pg_advisory_xact_lock(13)`)
		mustExec(t, ctx, a.Conn, `ROLLBACK TO SAVEPOINT s`)
		wantTry(t, ctx, b.Conn, `SELECT pg_try_advisory_lock(14)`, true)
		wantTry(t, ctx, b.Conn, `SELECT pg_try_advisory_lock(13)`, false)
		mustExec(t, ctx, a.Conn, `COMMIT`)
		wantTry(t, ctx, b.Conn, `SELECT pg_try_advisory_lock(13)`, true)
		mustExec(t, ctx, b.Conn, `SELECT pg_advisory_unlock_all()`)

		// A session-level lock taken in a transaction outlives its
		// rollback.
		mustExec(t, ctx, a.Conn, `BEGIN`)
		mustExec(t, ctx, a.Conn, `SELECT pg_advisory_lock(15)`)
		mustExec(t, ctx, a.Conn, `ROLLBACK`)
		wantTry(t, ctx, b.Conn, `SELECT pg_try_advisory_lock(15)`, false)
		wantTry(t, ctx, a.Conn, `SELECT pg_advisory_unlock(15)`, true)

		// Outside a block the statement is the transaction.
		mustExec(t, ctx, a.Conn, `SELECT pg_advisory_xact_lock(16)`)
		wantTry(t, ctx, b.Conn, `SELECT pg_try_advisory_lock(16)`, true)
		mustExec(t, ctx, b.Conn, `SELECT pg_advisory_unlock_all()`)

		// pg_advisory_unlock_all leaves the transaction's holds.
		mustExec(t, ctx, a.Conn, `SELECT pg_advisory_lock(17)`)
		mustExec(t, ctx, a.Conn, `BEGIN`)
		mustExec(t, ctx, a.Conn, `SELECT pg_advisory_xact_lock(18)`)
		mustExec(t, ctx, a.Conn, `SELECT pg_advisory_unlock_all()`)
		wantTry(t, ctx, b.Conn, `SELECT pg_try_advisory_lock(17)`, true)
		wantTry(t, ctx, b.Conn, `SELECT pg_try_advisory_lock(18)`, false)
		mustExec(t, ctx, a.Conn, `COMMIT`)
		wantTry(t, ctx, b.Conn, `SELECT pg_try_advisory_lock(18)`, true)
		mustExec(t, ctx, b.Conn, `SELECT pg_advisory_unlock_all()`)
	})

	t.Run("disconnect", func(t *testing.T) {
		a, b := advisoryConn(t, ctx, tc, 1), advisoryConn(t, ctx, tc, 0)
		mustExec(t, ctx, a.Conn, `SELECT pg_advisory_lock(30)`)
		mustExec(t, ctx, a.Conn, `SELECT pg_advisory_lock_shared(31)`)
		mustExec(t, ctx, a.Conn, `BEGIN`)
		mustExec(t, ctx, a.Conn, `SELECT pg_advisory_xact_lock(32)`)
		done := lockInBackground(ctx, b.Conn, `SELECT pg_advisory_lock(30)`)
		stillWaiting(t, done, 300*time.Millisecond)
		// The client vanishes: no Terminate, the socket just closes.
		_ = a.PgConn().Conn().Close()
		granted(t, done, 5*time.Second)
		wantTry(t, ctx, b.Conn, `SELECT pg_try_advisory_lock(31)`, true)
		wantTry(t, ctx, b.Conn, `SELECT pg_try_advisory_lock(32)`, true)
		mustExec(t, ctx, b.Conn, `SELECT pg_advisory_unlock_all()`)
	})

	t.Run("timeouts and cancel", func(t *testing.T) {
		a, b, admin := advisoryConn(t, ctx, tc, 0), advisoryConn(t, ctx, tc, 1), advisoryConn(t, ctx, tc, 2)
		mustExec(t, ctx, a.Conn, `SELECT pg_advisory_lock(40)`)

		mustExec(t, ctx, b.Conn, `SET lock_timeout = '300ms'`)
		t0 := time.Now()
		_, err := b.Exec(ctx, `SELECT pg_advisory_lock(40)`)
		var pe *pgconn.PgError
		if !errors.As(err, &pe) || pe.Code != "55P03" || pe.Message != "canceling statement due to lock timeout" {
			t.Fatalf("lock_timeout: %v, want 55P03 canceling statement due to lock timeout", err)
		}
		if took := time.Since(t0); took < 300*time.Millisecond || took > 10*time.Second {
			t.Fatalf("lock_timeout of 300ms fired after %s", took)
		}
		// Inside a block the timeout fails the block; ROLLBACK ends it.
		mustExec(t, ctx, b.Conn, `BEGIN`)
		if _, err := b.Exec(ctx, `SELECT pg_advisory_xact_lock(40)`); sqlState(err) != "55P03" {
			t.Fatalf("lock_timeout in a block: %v", err)
		}
		mustExec(t, ctx, b.Conn, `ROLLBACK`)
		mustExec(t, ctx, b.Conn, `RESET lock_timeout`)

		mustExec(t, ctx, b.Conn, `SET statement_timeout = '300ms'`)
		if _, err := b.Exec(ctx, `SELECT pg_advisory_lock_shared(40)`); !errors.As(err, &pe) || pe.Code != "57014" || !strings.Contains(pe.Message, "statement timeout") {
			t.Fatalf("statement_timeout: %v, want 57014", err)
		}
		mustExec(t, ctx, b.Conn, `RESET statement_timeout`)

		// A cancel ends the wait.
		pid := backendPID(t, ctx, b.Conn)
		done := lockInBackground(ctx, b.Conn, `SELECT pg_advisory_lock(40)`)
		stillWaiting(t, done, 300*time.Millisecond)
		wantTry(t, ctx, admin.Conn, fmt.Sprintf(`SELECT pg_cancel_backend(%d)`, pid), true)
		select {
		case err := <-done:
			if sqlState(err) != "57014" {
				t.Fatalf("cancelled wait: %v, want 57014", err)
			}
		case <-time.After(10 * time.Second):
			t.Fatal("the cancel did not end the wait")
		}
		// None of the failed waits left a hold behind.
		mustExec(t, ctx, a.Conn, `SELECT pg_advisory_unlock(40)`)
		wantTry(t, ctx, b.Conn, `SELECT pg_try_advisory_lock(40)`, true)
		mustExec(t, ctx, b.Conn, `SELECT pg_advisory_unlock_all()`)
		wantTry(t, ctx, a.Conn, `SELECT pg_try_advisory_lock(40)`, true)
		mustExec(t, ctx, a.Conn, `SELECT pg_advisory_unlock_all()`)
	})

	t.Run("row locks", func(t *testing.T) {
		a, b := advisoryConn(t, ctx, tc, 0), advisoryConn(t, ctx, tc, 1)
		mustExec(t, ctx, a.Conn, `CREATE TABLE IF NOT EXISTS adv_rows (id INT PRIMARY KEY, v INT)`)
		mustExec(t, ctx, a.Conn, `UPSERT INTO adv_rows VALUES (1, 0)`)
		// A row lock does not hold up an advisory lock of the same number...
		mustExec(t, ctx, a.Conn, `BEGIN`)
		mustExec(t, ctx, a.Conn, `SELECT * FROM adv_rows WHERE id = 1 FOR UPDATE`)
		wantTry(t, ctx, b.Conn, `SELECT pg_try_advisory_lock(1)`, true)
		mustExec(t, ctx, a.Conn, `SELECT pg_advisory_xact_lock(2)`)
		mustExec(t, ctx, a.Conn, `COMMIT`)
		// ... nor an advisory lock a row write.
		mustExec(t, ctx, a.Conn, `UPDATE adv_rows SET v = v + 1 WHERE id = 1`)
		mustExec(t, ctx, b.Conn, `SELECT pg_advisory_unlock_all()`)
	})

	t.Run("pg_locks", func(t *testing.T) {
		a, b := advisoryConn(t, ctx, tc, 0), advisoryConn(t, ctx, tc, 2)
		mustExec(t, ctx, a.Conn, `SELECT pg_advisory_lock(50)`)
		mustExec(t, ctx, a.Conn, `SELECT pg_advisory_lock(50)`) // one row however often held
		mustExec(t, ctx, a.Conn, `SELECT pg_advisory_lock_shared(-1)`)
		mustExec(t, ctx, a.Conn, `SELECT pg_advisory_lock(3, 4)`)
		pid := backendPID(t, ctx, a.Conn)
		rows, err := b.Query(ctx, `SELECT l.locktype, d.datname, l.classid, l.objid, l.objsubid, l.pid, l.mode, l.granted
			FROM pg_locks l JOIN pg_database d ON d.oid = l.database
			WHERE l.locktype = 'advisory' AND l.pid = $1 ORDER BY l.objsubid, l.objid`, pid)
		if err != nil {
			t.Fatal(err)
		}
		var got []string
		for rows.Next() {
			var lt, db, mode string
			var classid, objid, subid int64
			var p int32
			var g bool
			if err := rows.Scan(&lt, &db, &classid, &objid, &subid, &p, &mode, &g); err != nil {
				t.Fatal(err)
			}
			got = append(got, fmt.Sprintf("%s %s %d/%d/%d %d %s %v", lt, db, classid, objid, subid, p, mode, g))
		}
		if err := rows.Err(); err != nil {
			t.Fatal(err)
		}
		want := []string{
			fmt.Sprintf("advisory datax 0/50/1 %d ExclusiveLock true", pid),
			fmt.Sprintf("advisory datax 4294967295/4294967295/1 %d ShareLock true", pid),
			fmt.Sprintf("advisory datax 3/4/2 %d ExclusiveLock true", pid),
		}
		if strings.Join(got, "\n") != strings.Join(want, "\n") {
			t.Fatalf("pg_locks:\n%s\nwant:\n%s", strings.Join(got, "\n"), strings.Join(want, "\n"))
		}
		mustExec(t, ctx, a.Conn, `SELECT pg_advisory_unlock_all()`)
		var n int
		if err := b.QueryRow(ctx, `SELECT count(*) FROM pg_locks WHERE pid = $1`, pid).Scan(&n); err != nil || n != 0 {
			t.Fatalf("pg_locks after unlock_all: %d rows, %v", n, err)
		}
	})

	t.Run("golang-migrate", func(t *testing.T) {
		// golang-migrate's postgres driver, verbatim: the lock ID is a
		// decimal string bound to $1, on a database/sql connection.
		aid := fmt.Sprint(uint64(crc32.ChecksumIEEE([]byte("datax\x00public\x00schema_migrations"))) * 1486364155)
		db1, err := sql.Open("pgx", pgURL(tc, 0))
		if err != nil {
			t.Fatal(err)
		}
		defer db1.Close()
		db2, err := sql.Open("pgx", pgURL(tc, 1))
		if err != nil {
			t.Fatal(err)
		}
		defer db2.Close()
		c1, err := db1.Conn(ctx)
		if err != nil {
			t.Fatal(err)
		}
		defer c1.Close()
		c2, err := db2.Conn(ctx)
		if err != nil {
			t.Fatal(err)
		}
		defer c2.Close()
		if _, err := c1.ExecContext(ctx, `SELECT pg_advisory_lock($1)`, aid); err != nil {
			t.Fatalf("migrate Lock: %v", err)
		}
		done := make(chan error, 1)
		go func() {
			_, err := c2.ExecContext(ctx, `SELECT pg_advisory_lock($1)`, aid)
			done <- err
		}()
		stillWaiting(t, done, 300*time.Millisecond)
		if _, err := c1.ExecContext(ctx, `SELECT pg_advisory_unlock($1)`, aid); err != nil {
			t.Fatalf("migrate Unlock: %v", err)
		}
		granted(t, done, 5*time.Second)
		var ok bool
		if err := c1.QueryRowContext(ctx, `SELECT pg_try_advisory_lock($1)`, int64(1)<<40).Scan(&ok); err != nil || !ok {
			t.Fatalf("an int64 parameter: %v, %v", ok, err)
		}
		if _, err := c2.ExecContext(ctx, `SELECT pg_advisory_unlock($1)`, aid); err != nil {
			t.Fatal(err)
		}
		if _, err := c1.ExecContext(ctx, `SELECT pg_advisory_unlock_all()`); err != nil {
			t.Fatal(err)
		}
	})
}

// TestAdvisoryLockHolderNodeStops (issue #353): a lock held by a
// session on a node that stops is released within a bounded time — the
// lease's TTL plus the clock offset, once the cluster has recovered —
// and taken by a session on another node.
func TestAdvisoryLockHolderNodeStops(t *testing.T) {
	tc := advisoryCluster(t, 3)
	ctx, cancel := context.WithTimeout(context.Background(), 180*time.Second)
	defer cancel()
	holder, other := advisoryConn(t, ctx, tc, 2), advisoryConn(t, ctx, tc, 0)
	mustExec(t, ctx, holder.Conn, `SELECT pg_advisory_lock(1)`)
	mustExec(t, ctx, holder.Conn, `SELECT pg_advisory_lock_shared(2)`)
	wantTry(t, ctx, other.Conn, `SELECT pg_try_advisory_lock(1)`, false)

	tc.StopNode(2)
	t0 := time.Now()
	if _, err := other.Exec(ctx, `SELECT pg_advisory_lock(1)`); err != nil {
		t.Fatalf("taking the stopped node's lock: %v", err)
	}
	if took := time.Since(t0); took > 60*time.Second {
		t.Fatalf("the stopped node's lock was released after %s", took)
	}
	t.Logf("the stopped node's lock was taken %s after the node went away", time.Since(t0))
	wantTry(t, ctx, other.Conn, `SELECT pg_try_advisory_lock(2)`, true)
	mustExec(t, ctx, other.Conn, `SELECT pg_advisory_unlock_all()`)
}

// TestAdvisoryLockHolderPartitioned (issue #353): a holder whose node is
// cut off from the cluster cannot renew its lease. Its locks are free
// for the rest of the cluster after the lease's TTL — and its session is
// ended with FATAL, since what it believed it held is gone.
func TestAdvisoryLockHolderPartitioned(t *testing.T) {
	tc := advisoryCluster(t, 3)
	ctx, cancel := context.WithTimeout(context.Background(), 180*time.Second)
	defer cancel()
	holder, other := advisoryConn(t, ctx, tc, 2), advisoryConn(t, ctx, tc, 0)
	mustExec(t, ctx, holder.Conn, `SELECT pg_advisory_lock(1)`)
	wantTry(t, ctx, other.Conn, `SELECT pg_try_advisory_lock(1)`, false)

	tc.Isolate(2)
	defer tc.Heal()
	t0 := time.Now()
	if _, err := other.Exec(ctx, `SELECT pg_advisory_lock(1)`); err != nil {
		t.Fatalf("taking the partitioned node's lock: %v", err)
	}
	if took := time.Since(t0); took > 60*time.Second {
		t.Fatalf("the partitioned node's lock was released after %s", took)
	}
	t.Logf("the partitioned node's lock was taken %s after the node went away", time.Since(t0))
	// The holder's node has let its lease lapse by now, and ended the
	// session.
	_, err := holder.Exec(ctx, `SELECT 1`)
	var pe *pgconn.PgError
	if err == nil || errors.As(err, &pe) && pe.Code != "57P01" {
		t.Fatalf("the partitioned holder's next statement: %v, want its connection ended (57P01)", err)
	}
	mustExec(t, ctx, other.Conn, `SELECT pg_advisory_unlock_all()`)
}
