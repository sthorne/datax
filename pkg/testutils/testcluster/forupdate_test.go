package testcluster

import (
	"context"
	"fmt"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/sthorne/datax/pkg/keys"
	"github.com/sthorne/datax/pkg/kvclient"
	"github.com/sthorne/datax/pkg/sql"
	"github.com/sthorne/datax/pkg/sql/catalog"
)

// TestGetForUpdateSerializesRMW: concurrent read-modify-write transactions
// using locking reads serialize on the lock instead of doomed-racing to
// restarts — every increment lands. Regression test for issue #15's thrash
// (with plain reads, symmetric conflicts restart each other repeatedly).
func TestGetForUpdateSerializesRMW(t *testing.T) {
	tc := Start(t, 3)
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	prefix := keys.TableDataPrefix(900)
	key := append(prefix.Clone(), "counter"...)
	if err := tc.Nodes[0].DB().Put(ctx, key, []byte("0")); err != nil {
		t.Fatal(err)
	}

	const workers, perWorker = 4, 10
	var wg sync.WaitGroup
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(n int) {
			defer wg.Done()
			db := tc.Nodes[n%3].DB()
			for i := 0; i < perWorker; i++ {
				for {
					txn := db.NewTxn("rmw")
					v, err := txn.GetForUpdate(ctx, key)
					if err == nil {
						cur, _ := strconv.Atoi(string(v))
						err = txn.Put(ctx, key, []byte(strconv.Itoa(cur+1)))
					}
					if err == nil {
						err = txn.Commit(ctx)
					}
					if err == nil {
						break
					}
					_ = txn.Rollback(ctx)
				}
			}
		}(w)
	}
	wg.Wait()

	v, err := tc.Nodes[0].DB().Get(ctx, key)
	if err != nil {
		t.Fatal(err)
	}
	if got, _ := strconv.Atoi(string(v)); got != workers*perWorker {
		t.Fatalf("lost updates: counter = %d, want %d", got, workers*perWorker)
	}
}

// TestGetForUpdateBlocksWriters: a held lock makes a conflicting writer
// wait; it proceeds once the locker finishes, observing the locker's
// write. A pusher with a higher random priority may legitimately ABORT
// the pending locker instead (standard push semantics), so the scenario
// retries until the locker survives the coin flip.
func TestGetForUpdateBlocksWriters(t *testing.T) {
	tc := Start(t, 3)
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	prefix := keys.TableDataPrefix(901)
	key := append(prefix.Clone(), "k"...)
	db := tc.Nodes[0].DB()

	// The locker must win the ~50/50 priority flip once; retry on a
	// deadline, not a fixed budget — under heavy load a run of losses is
	// slow, not wrong (issue #61).
	deadline := time.Now().Add(60 * time.Second)
	for {
		if time.Now().After(deadline) {
			t.Fatal("locker kept losing the priority flip until the deadline")
		}
		if err := db.Put(ctx, key, []byte("orig")); err != nil {
			t.Fatal(err)
		}
		locker := db.NewTxn("locker")
		if v, err := locker.GetForUpdate(ctx, key); err != nil || string(v) != "orig" {
			t.Fatalf("lock: %q, %v", v, err)
		}

		blocked := make(chan error, 1)
		go func() {
			w := db.NewTxn("writer")
			err := w.Put(ctx, key, []byte("writer"))
			if err == nil {
				err = w.Commit(ctx)
			}
			if err != nil {
				_ = w.Rollback(ctx)
			}
			blocked <- err
		}()
		aborted := false
		select {
		case err := <-blocked:
			// The writer won the priority flip and aborted the locker —
			// the locker must now FAIL, not silently lose its lock.
			if err != nil {
				t.Fatalf("writer neither blocked nor committed: %v", err)
			}
			aborted = true
		case <-time.After(300 * time.Millisecond):
		}
		if aborted {
			if err := locker.Put(ctx, key, []byte("locked-write")); err == nil {
				if err := locker.Commit(ctx); err == nil {
					t.Fatal("aborted locker committed successfully")
				}
			}
			_ = locker.Rollback(ctx)
			continue // retry the scenario
		}

		// The writer is queued behind the lock: finish the locker and the
		// writer proceeds, landing after the locker's write. A failure here
		// is the OTHER legitimate outcome surfacing late — the writer won
		// the flip but its abort landed after our 300ms classification
		// window (common on a loaded box) — so it retries the scenario
		// rather than failing the test.
		if err := locker.Put(ctx, key, []byte("locked-write")); err != nil {
			if !kvclient.IsRetryable(err) {
				<-blocked
				t.Fatalf("locker write failed with a non-retryable error: %v", err)
			}
			_ = locker.Rollback(ctx)
			<-blocked
			continue
		}
		if err := locker.Commit(ctx); err != nil {
			if !kvclient.IsRetryable(err) {
				<-blocked
				t.Fatalf("locker commit failed with a non-retryable error: %v", err)
			}
			_ = locker.Rollback(ctx)
			<-blocked
			continue
		}
		if err := <-blocked; err != nil {
			t.Fatalf("writer failed after lock release: %v", err)
		}
		v, err := db.Get(ctx, key)
		if err != nil || string(v) != "writer" {
			t.Fatalf("final value %q, %v", v, err)
		}
		return
	}
}

// TestScanForUpdateLocksRows: a locking scan pins every returned row.
func TestScanForUpdateLocksRows(t *testing.T) {
	tc := Start(t, 3)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	prefix := keys.TableDataPrefix(902)
	db := tc.Nodes[0].DB()
	for i := 0; i < 3; i++ {
		if err := db.Put(ctx, append(prefix.Clone(), fmt.Sprintf("k%d", i)...), []byte("v")); err != nil {
			t.Fatal(err)
		}
	}

	// Deadline-based flip retries, as in TestGetForUpdateBlocksWriters.
	deadline := time.Now().Add(45 * time.Second)
	for {
		if time.Now().After(deadline) {
			t.Fatal("locker kept losing the priority flip until the deadline")
		}
		locker := db.NewTxn("locker")
		rows, err := locker.ScanForUpdate(ctx, prefix, prefix.PrefixEnd(), 0)
		if err != nil || len(rows) != 3 {
			t.Fatalf("scan-for-update: %d rows, %v", len(rows), err)
		}

		// Writing any scanned row from another transaction blocks — unless
		// the writer wins the priority flip and aborts the locker, in which
		// case retry the scenario.
		wctx, wcancel := context.WithTimeout(ctx, 500*time.Millisecond)
		w := db.NewTxn("writer")
		err = w.Put(wctx, rows[1].Key, []byte("x"))
		wcancel()
		if err == nil {
			_ = w.Rollback(ctx)
			_ = locker.Rollback(ctx)
			continue
		}
		_ = w.Rollback(ctx)
		if err := locker.Commit(ctx); err != nil {
			// The writer's winning push surfaced after its 500ms window
			// timed out — a late flip loss, so retry (issue #61). Only a
			// conflict qualifies; anything else is a real failure.
			if !kvclient.IsRetryable(err) {
				t.Fatalf("locker commit failed with a non-retryable error: %v", err)
			}
			_ = locker.Rollback(ctx)
			continue
		}
		return
	}
}

// TestSQLSelectForUpdate: the SQL surface end to end — the classic
// read-modify-write pattern with FOR UPDATE loses no updates and the
// statement is rejected where PG semantics demand.
func TestSQLSelectForUpdate(t *testing.T) {
	tc := Start(t, 3)
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()
	cat := catalog.NewAccessor()
	s := sql.NewSession(tc.Nodes[0].DB(), cat)
	execSQL(t, ctx, s, `CREATE TABLE acct (id INT8 PRIMARY KEY, bal INT8)`)
	execSQL(t, ctx, s, `INSERT INTO acct VALUES (1, 0)`)

	const workers, perWorker = 4, 5
	var wg sync.WaitGroup
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(n int) {
			defer wg.Done()
			sess := sql.NewSession(tc.Nodes[n%3].DB(), catalog.NewAccessor())
			for i := 0; i < perWorker; i++ {
				for {
					if _, serr := trySQL(ctx, sess, `BEGIN`); serr != nil {
						continue
					}
					res, serr := trySQL(ctx, sess, `SELECT bal FROM acct WHERE id = 1 FOR UPDATE`)
					if serr == nil {
						bal := res.Rows[0][0].I
						_, serr = trySQL(ctx, sess, fmt.Sprintf(`UPDATE acct SET bal = %d WHERE id = 1`, bal+1))
					}
					if serr == nil {
						_, serr = trySQL(ctx, sess, `COMMIT`)
					}
					if serr == nil {
						break
					}
					_, _ = trySQL(ctx, sess, `ROLLBACK`)
				}
			}
		}(w)
	}
	wg.Wait()

	res := execSQL(t, ctx, s, `SELECT bal FROM acct WHERE id = 1`)
	if res.Rows[0][0].I != workers*perWorker {
		t.Fatalf("lost updates: bal = %d, want %d", res.Rows[0][0].I, workers*perWorker)
	}

	// Rejections: aggregates and AS OF SYSTEM TIME.
	if _, serr := trySQL(ctx, s, `SELECT COUNT(*) FROM acct FOR UPDATE`); serr == nil {
		t.Fatal("FOR UPDATE with aggregate accepted")
	}
	if _, serr := trySQL(ctx, s, `SELECT bal FROM acct AS OF SYSTEM TIME '-1s' FOR UPDATE`); serr == nil {
		t.Fatal("FOR UPDATE with AS OF SYSTEM TIME accepted")
	}
}
