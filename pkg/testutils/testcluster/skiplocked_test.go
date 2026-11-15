package testcluster

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/sthorne/datax/pkg/keys"
	"github.com/sthorne/datax/pkg/kvclient"
	"github.com/sthorne/datax/pkg/sql"
)

// queryIDs runs a query returning one INT8 column and collects it.
func queryIDs(ctx context.Context, q interface {
	Query(context.Context, string, ...any) (pgx.Rows, error)
}, query string, args ...any) ([]int64, error) {
	rows, err := q.Query(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, pgx.RowTo[int64])
}

// TestSelectForUpdateSkipLockedAndNoWait (issue #296): while one
// transaction holds rows locked, another's FOR UPDATE SKIP LOCKED returns
// the remaining rows without waiting — through a primary scan, a sort,
// an index join and a point lookup, and with LIMIT counting only rows it
// could lock — and its FOR UPDATE NOWAIT fails at once with 55P03. The
// lock holder is not disturbed, and once it commits both see every row.
func TestSelectForUpdateSkipLockedAndNoWait(t *testing.T) {
	tc := Start(t, 3)
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()
	connect := func(node int) *pgx.Conn {
		t.Helper()
		c, err := pgx.Connect(ctx, pgURL(tc, node))
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = c.Close(context.Background()) })
		return c
	}
	a, b := connect(0), connect(1)
	for _, stmt := range []string{
		`CREATE TABLE jobs (id INT8 PRIMARY KEY, payload TEXT, prio INT8)`,
		`CREATE INDEX jobs_payload ON jobs (payload)`,
		`INSERT INTO jobs VALUES (1, 'p1', 50), (2, 'p2', 40), (3, 'p3', 30), (4, 'p4', 20), (5, 'p5', 10)`,
	} {
		if _, err := a.Exec(ctx, stmt); err != nil {
			t.Fatalf("%s: %v", stmt, err)
		}
	}

	// A locks rows 1 and 2 and keeps its transaction open.
	atx, err := a.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if ids, err := queryIDs(ctx, atx, `SELECT id FROM jobs WHERE id <= 2 ORDER BY id FOR UPDATE`); err != nil || fmt.Sprint(ids) != "[1 2]" {
		t.Fatalf("A's lock: %v, %v", ids, err)
	}

	// Nothing B does below may wait for A: every statement runs under a
	// deadline far shorter than a lock wait would last (A never lets go
	// until told to), and is timed.
	prompt := func(what string, f func(ctx context.Context) error) {
		t.Helper()
		sctx, scancel := context.WithTimeout(ctx, 5*time.Second)
		defer scancel()
		start := time.Now()
		if err := f(sctx); err != nil {
			t.Fatalf("%s: %v (after %s)", what, err, time.Since(start))
		}
	}
	// Each SKIP LOCKED statement runs in its own transaction that is then
	// rolled back, so the rows it locked are free for the next one.
	skipLocked := func(query, want string) {
		t.Helper()
		prompt(query, func(sctx context.Context) error {
			tx, err := b.Begin(sctx)
			if err != nil {
				return err
			}
			defer func() { _ = tx.Rollback(ctx) }()
			ids, err := queryIDs(sctx, tx, query)
			if err != nil {
				return err
			}
			if got := fmt.Sprint(ids); got != want {
				return fmt.Errorf("returned %s, want %s", got, want)
			}
			return nil
		})
	}
	whileLocked := []struct{ query, want string }{
		{`SELECT id FROM jobs ORDER BY id FOR UPDATE SKIP LOCKED`, "[3 4 5]"},
		{`SELECT id FROM jobs ORDER BY id LIMIT 1 FOR UPDATE SKIP LOCKED`, "[3]"},
		{`SELECT id FROM jobs ORDER BY id LIMIT 2 FOR UPDATE SKIP LOCKED`, "[3 4]"},
		{`SELECT id FROM jobs ORDER BY id LIMIT 2 OFFSET 1 FOR UPDATE SKIP LOCKED`, "[4 5]"},
		{`SELECT id FROM jobs ORDER BY id DESC LIMIT 4 FOR UPDATE SKIP LOCKED`, "[5 4 3]"},
		{`SELECT id FROM jobs ORDER BY prio DESC LIMIT 1 FOR UPDATE SKIP LOCKED`, "[3]"}, // sorted in memory
		{`SELECT id FROM jobs WHERE payload = 'p2' FOR UPDATE SKIP LOCKED`, "[]"},        // index join onto a locked row
		{`SELECT id FROM jobs WHERE payload = 'p3' FOR UPDATE SKIP LOCKED`, "[3]"},
		{`SELECT id FROM jobs WHERE id = 1 FOR UPDATE SKIP LOCKED`, "[]"}, // point lookup of a locked row
		{`SELECT id FROM jobs WHERE id = 4 FOR UPDATE SKIP LOCKED`, "[4]"},
		{`SELECT id FROM jobs ORDER BY id LIMIT 0 FOR UPDATE SKIP LOCKED`, "[]"},
	}
	for _, q := range whileLocked {
		skipLocked(q.query, q.want)
	}

	// NOWAIT: a locked row fails the statement with 55P03; a free one is
	// locked as usual.
	for _, query := range []string{
		`SELECT id FROM jobs WHERE id = 2 FOR UPDATE NOWAIT`,
		`SELECT id FROM jobs ORDER BY id FOR UPDATE NOWAIT`,
		`SELECT id FROM jobs WHERE payload = 'p1' FOR UPDATE NOWAIT`,
	} {
		prompt(query, func(sctx context.Context) error {
			_, err := queryIDs(sctx, b, query)
			if pgErrCode(err) != sql.CodeLockNotAvailable {
				return fmt.Errorf("got %v, want SQLSTATE %s", err, sql.CodeLockNotAvailable)
			}
			return nil
		})
	}
	prompt("NOWAIT on a free row", func(sctx context.Context) error {
		ids, err := queryIDs(sctx, b, `SELECT id FROM jobs WHERE id = 3 FOR UPDATE NOWAIT`)
		if err != nil || fmt.Sprint(ids) != "[3]" {
			return fmt.Errorf("got %v, %v", ids, err)
		}
		return nil
	})
	// A NOWAIT failure inside a transaction block is an ordinary statement
	// error: a savepoint recovers from it and the block goes on to commit.
	prompt("NOWAIT under a savepoint", func(sctx context.Context) error {
		tx, err := b.Begin(sctx)
		if err != nil {
			return err
		}
		defer func() { _ = tx.Rollback(ctx) }()
		if _, err := tx.Exec(sctx, `SAVEPOINT try`); err != nil {
			return err
		}
		if _, err := queryIDs(sctx, tx, `SELECT id FROM jobs WHERE id = 1 FOR UPDATE NOWAIT`); pgErrCode(err) != sql.CodeLockNotAvailable {
			return fmt.Errorf("first statement of the block: got %v, want %s", err, sql.CodeLockNotAvailable)
		}
		if _, err := tx.Exec(sctx, `ROLLBACK TO SAVEPOINT try`); err != nil {
			return err
		}
		if _, err := tx.Exec(sctx, `UPDATE jobs SET prio = 21 WHERE id = 4`); err != nil {
			return err
		}
		return tx.Commit(sctx)
	})

	// A was never pushed out of the way: it still owns its locks, writes
	// under them and commits.
	if _, err := atx.Exec(ctx, `UPDATE jobs SET payload = 'done' WHERE id = 1`); err != nil {
		t.Fatalf("A's write under its lock: %v", err)
	}
	if err := atx.Commit(ctx); err != nil {
		t.Fatalf("A's commit: %v", err)
	}

	skipLocked(`SELECT id FROM jobs ORDER BY id FOR UPDATE SKIP LOCKED`, "[1 2 3 4 5]")
	skipLocked(`SELECT id FROM jobs ORDER BY id LIMIT 1 FOR UPDATE SKIP LOCKED`, "[1]")
	prompt("NOWAIT after the lock holder committed", func(sctx context.Context) error {
		ids, err := queryIDs(sctx, b, `SELECT id FROM jobs ORDER BY id FOR UPDATE NOWAIT`)
		if err != nil || fmt.Sprint(ids) != "[1 2 3 4 5]" {
			return fmt.Errorf("got %v, %v", ids, err)
		}
		return nil
	})
	var payload string
	if err := b.QueryRow(ctx, `SELECT payload FROM jobs WHERE id = 1`).Scan(&payload); err != nil || payload != "done" {
		t.Fatalf("A's write: %q, %v", payload, err)
	}
}

// TestSkipLockedRowsStayOutOfTheReadSet (issue #296): the rows a SKIP
// LOCKED statement passed over are not reads of its transaction. B skips
// the rows A holds; A then changes one and commits; a third session
// reads the table at a later timestamp, which pushes B's next write
// above its read timestamp and so forces B to refresh its reads at
// commit. Were A's row in B's read set, that refresh would find A's
// write in it and B would fail with 40001 over a row it never returned.
func TestSkipLockedRowsStayOutOfTheReadSet(t *testing.T) {
	tc := Start(t, 3)
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()
	connect := func(node int) *pgx.Conn {
		t.Helper()
		c, err := pgx.Connect(ctx, pgURL(tc, node))
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = c.Close(context.Background()) })
		return c
	}
	a, b, c := connect(0), connect(1), connect(2)
	for _, stmt := range []string{
		`CREATE TABLE jobs (id INT8 PRIMARY KEY, payload TEXT)`,
		`INSERT INTO jobs VALUES (1, 'p1'), (2, 'p2'), (3, 'p3'), (4, 'p4'), (5, 'p5')`,
	} {
		if _, err := a.Exec(ctx, stmt); err != nil {
			t.Fatalf("%s: %v", stmt, err)
		}
	}

	atx, err := a.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if ids, err := queryIDs(ctx, atx, `SELECT id FROM jobs WHERE id <= 2 ORDER BY id FOR UPDATE`); err != nil || fmt.Sprint(ids) != "[1 2]" {
		t.Fatalf("A's lock: %v, %v", ids, err)
	}
	btx, err := b.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = btx.Rollback(ctx) }()
	if ids, err := queryIDs(ctx, btx, `SELECT id FROM jobs ORDER BY id FOR UPDATE SKIP LOCKED`); err != nil || fmt.Sprint(ids) != "[3 4 5]" {
		t.Fatalf("B's skip-locked read: %v, %v", ids, err)
	}

	// A changes a row B skipped, deletes the other, and commits.
	if _, err := atx.Exec(ctx, `UPDATE jobs SET payload = 'done' WHERE id = 1`); err != nil {
		t.Fatal(err)
	}
	if _, err := atx.Exec(ctx, `DELETE FROM jobs WHERE id = 2`); err != nil {
		t.Fatal(err)
	}
	if err := atx.Commit(ctx); err != nil {
		t.Fatalf("A's commit: %v", err)
	}

	// C reads the whole table above B's timestamp (skipping B's rows, so
	// it does not wait): B's next write is pushed over that read.
	if ids, err := queryIDs(ctx, c, `SELECT id FROM jobs ORDER BY id FOR UPDATE SKIP LOCKED`); err != nil || fmt.Sprint(ids) != "[1]" {
		t.Fatalf("C's read: %v, %v", ids, err)
	}

	if _, err := btx.Exec(ctx, `UPDATE jobs SET payload = 'mine' WHERE id = 3`); err != nil {
		t.Fatalf("B's write: %v", err)
	}
	if err := btx.Commit(ctx); err != nil {
		t.Fatalf("B's commit (a 40001 here means a skipped row was in its read set): %v", err)
	}
}

// TestSkipLockedQueue (issue #296): the job-queue pattern. Workers drain
// a table with SELECT ... ORDER BY id LIMIT 1 FOR UPDATE SKIP LOCKED then
// DELETE, one job per transaction, retrying on 40001 as any client of a
// serializable database does. Every job is taken exactly once, and the
// workers stay out of each other's way: serialization failures are the
// exception, not the cost of every contended dequeue.
func TestSkipLockedQueue(t *testing.T) {
	tc := Start(t, 3)
	ctx, cancel := context.WithTimeout(context.Background(), 240*time.Second)
	defer cancel()
	const workers, jobs = 8, 200

	setup, err := pgx.Connect(ctx, pgURL(tc, 0))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = setup.Close(context.Background()) }()
	if _, err := setup.Exec(ctx, `CREATE TABLE jobs (id INT8 PRIMARY KEY, payload TEXT)`); err != nil {
		t.Fatal(err)
	}
	var values []string
	for id := 1; id <= jobs; id++ {
		values = append(values, fmt.Sprintf("(%d, 'job')", id))
	}
	if _, err := setup.Exec(ctx, `INSERT INTO jobs VALUES `+strings.Join(values, ", ")); err != nil {
		t.Fatal(err)
	}

	var (
		mu       sync.Mutex
		taken    = map[int64]int{} // job -> times a transaction that dequeued it committed
		retries  atomic.Int64      // 40001s, at any statement or at commit
		empties  atomic.Int64      // dequeues that found every remaining job locked
		wg       sync.WaitGroup
		failOnce sync.Once
		failure  error
	)
	fail := func(err error) {
		failOnce.Do(func() { failure = err; cancel() })
	}
	// dequeue runs one transaction; done reports an empty table.
	dequeue := func(conn *pgx.Conn) (id int64, done bool, err error) {
		tx, err := conn.Begin(ctx)
		if err != nil {
			return 0, false, err
		}
		defer func() { _ = tx.Rollback(context.Background()) }()
		ids, err := queryIDs(ctx, tx, `SELECT id FROM jobs ORDER BY id LIMIT 1 FOR UPDATE SKIP LOCKED`)
		if err != nil {
			return 0, false, err
		}
		if len(ids) == 0 {
			// Nothing free: the table is empty, or every job left is in
			// another worker's hands (and comes back if that worker fails).
			var left int64
			if err := tx.QueryRow(ctx, `SELECT count(*) FROM jobs`).Scan(&left); err != nil {
				return 0, false, err
			}
			return 0, left == 0, tx.Commit(ctx)
		}
		tag, err := tx.Exec(ctx, `DELETE FROM jobs WHERE id = $1`, ids[0])
		if err != nil {
			return 0, false, err
		}
		if tag.RowsAffected() != 1 {
			return 0, false, fmt.Errorf("job %d was locked by this transaction but its DELETE removed %d rows", ids[0], tag.RowsAffected())
		}
		return ids[0], false, tx.Commit(ctx)
	}
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			conn, err := pgx.Connect(ctx, pgURL(tc, w%3))
			if err != nil {
				fail(err)
				return
			}
			defer func() { _ = conn.Close(context.Background()) }()
			for {
				id, done, err := dequeue(conn)
				switch {
				case pgErrCode(err) == sql.CodeSerializationFailure:
					retries.Add(1)
				case err != nil:
					if !errors.Is(err, context.Canceled) {
						fail(fmt.Errorf("worker %d: %w", w, err))
					}
					return
				case done:
					return
				case id == 0:
					empties.Add(1)
				default:
					mu.Lock()
					taken[id]++
					mu.Unlock()
				}
			}
		}(w)
	}
	wg.Wait()
	if failure != nil {
		t.Fatal(failure)
	}
	for id := int64(1); id <= jobs; id++ {
		if taken[id] != 1 {
			t.Errorf("job %d was taken %d times", id, taken[id])
		}
	}
	if len(taken) != jobs {
		t.Errorf("%d distinct jobs taken, want %d", len(taken), jobs)
	}
	t.Logf("%d workers drained %d jobs with %d serialization failures (40001) and %d dequeues that found every remaining job locked", workers, jobs, retries.Load(), empties.Load())
	// Not zero by construction — a worker whose snapshot predates a
	// neighbor's commit can still be told to retry — but SKIP LOCKED
	// exists so that this is rare. Without it (plain FOR UPDATE on the
	// same query) the workers queue on one row and most dequeues restart.
	if n := retries.Load(); n > jobs/4 {
		t.Errorf("%d serialization failures draining %d jobs: workers are not staying out of each other's way", n, jobs)
	}
}

// TestSkipLockedAtTheKVLayer (issue #296): the two halves of the
// mechanism below SQL. A read under LockWaitSkip steps around a held
// lock, and GetBatchForUpdateNoWait locks the keys that are free — or
// fails with a lock-not-available error — without waiting and without
// aborting the holder, even when it outranks the holder (an ordinary
// conflicting write would push the holder out of the way). And a lock
// whose holder died does not hide its row for good: nothing conflicts
// with it under SKIP LOCKED, so the skipping reader itself pushes the
// expired transaction and the row comes back.
func TestSkipLockedAtTheKVLayer(t *testing.T) {
	tc := Start(t, 3)
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()
	db := tc.Nodes[0].DB()
	prefix := keys.TableDataPrefix(903)
	key := func(i int) keys.Key { return append(prefix.Clone(), fmt.Sprintf("k%d", i)...) }
	for i := 0; i < 4; i++ {
		if err := db.Put(ctx, key(i), []byte("v")); err != nil {
			t.Fatal(err)
		}
	}

	holder := db.NewTxn("holder")
	holder.SetPriorityForTesting(1)
	if _, err := holder.GetBatchForUpdate(ctx, []keys.Key{key(1), key(2)}); err != nil {
		t.Fatal(err)
	}

	skipCtx := kvclient.WithLockWait(ctx, kvclient.LockWaitSkip)
	scanKeys := func(txn *kvclient.Txn) string {
		t.Helper()
		sctx, scancel := context.WithTimeout(skipCtx, 5*time.Second)
		defer scancel()
		rows, err := txn.Scan(sctx, prefix, prefix.PrefixEnd(), 0)
		if err != nil {
			t.Fatalf("skip-locked scan: %v", err)
		}
		var out string
		for _, kv := range rows {
			out += string(kv.Key[len(prefix):]) + " "
		}
		return out
	}

	other := db.NewTxn("other")
	other.SetPriorityForTesting(1 << 20) // would win any push it was allowed to make
	if got := scanKeys(other); got != "k0 k3 " {
		t.Fatalf("scan around the holder's locks returned %q, want k0 k3", got)
	}
	if v, err := other.Get(skipCtx, key(1)); err != nil || v != nil {
		t.Fatalf("skip-locked get of a locked key: %q, %v", v, err)
	}
	all := []keys.Key{key(0), key(1), key(2), key(3)}
	lctx, lcancel := context.WithTimeout(ctx, 5*time.Second)
	defer lcancel()
	locked, err := other.GetBatchForUpdateNoWait(lctx, all, true)
	if err != nil || fmt.Sprint(locked) != "[true false false true]" {
		t.Fatalf("skip-locked lock: %v, %v", locked, err)
	}
	if _, err := other.GetBatchForUpdateNoWait(lctx, all, false); !kvclient.IsLockNotAvailable(err) {
		t.Fatalf("no-wait lock over held keys: %v, want lock not available", err)
	}
	if err := other.Commit(ctx); err != nil {
		t.Fatalf("the skipping transaction's commit: %v", err)
	}
	// The holder was looked at, never pushed aside.
	if err := holder.Put(ctx, key(1), []byte("held")); err != nil {
		t.Fatalf("holder's write under its lock: %v", err)
	}

	// The holder's coordinator dies with its locks in place.
	holder.AbandonForTesting()
	deadline := time.Now().Add(60 * time.Second)
	for {
		reader := db.NewTxn("reader")
		got := scanKeys(reader)
		_ = reader.Rollback(ctx)
		if got == "k0 k1 k2 k3 " {
			break
		}
		if got != "k0 k3 " {
			t.Fatalf("scan while the dead holder's locks stand returned %q", got)
		}
		if time.Now().After(deadline) {
			t.Fatal("the dead holder's locks were never cleared by the readers skipping them")
		}
		time.Sleep(250 * time.Millisecond)
	}
	// Aborted, so its provisional write is gone.
	if v, err := db.Get(ctx, key(1)); err != nil || string(v) != "v" {
		t.Fatalf("value under the dead holder's lock: %q, %v", v, err)
	}
}
