package testcluster

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/sthorne/datax/pkg/sql"
)

// TestSkipLockedContendedReturnsFreeRow (issue #320, from the same
// report): workers loop BEGIN; SELECT id FROM work WHERE runnable ORDER
// BY id LIMIT 1 FOR UPDATE SKIP LOCKED; UPDATE the row; COMMIT — once
// leaving every row runnable, once taking each row out of the queue.
// Each worker holds at most one row, so while at least as many rows are
// runnable as there are workers a free one exists and the SELECT must
// return a row. And the workers stay out of each other's way: the
// residual filter (runnable) made the fetch read the whole table, all
// of which stayed in the transaction's read set, and about a third of
// the commits failed their refresh with 40001 ("write timestamp
// diverged from read timestamp") over rows they never returned.
//
// The report also saw the SELECT come back empty with 100 to 160 rows
// runnable. That was the locks of transactions whose COMMIT had failed:
// before #315 their intents stayed until their records expired (5s), and
// SKIP LOCKED passed over every such row as held. With that release and
// the read-set fix both taken out again, the draining case here finds 4
// to 18 such empty dequeues a run; with either in place, none (the
// read-set fix leaves next to no failed commits to hold rows).
func TestSkipLockedContendedReturnsFreeRow(t *testing.T) {
	tc := Start(t, 1)
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()
	const workers, rows = 4, 200
	iters := 150
	if raceEnabled || testing.Short() {
		iters = 40
	}
	setup := pgConn(t, ctx, tc, 0)
	for _, c := range []struct{ name, update string }{
		{"rows stay runnable", `UPDATE %s SET n = n + 1 WHERE id = $1`},
		{"queue drains", `UPDATE %s SET runnable = false, n = n + 1 WHERE id = $1`},
	} {
		t.Run(c.name, func(t *testing.T) {
			table := "work_" + strings.ReplaceAll(c.name, " ", "_")
			if _, err := setup.Exec(ctx, fmt.Sprintf(`CREATE TABLE %s (id INT8 PRIMARY KEY, runnable BOOL, n INT8)`, table)); err != nil {
				t.Fatal(err)
			}
			var values []string
			for id := 1; id <= rows; id++ {
				values = append(values, fmt.Sprintf("(%d, true, 0)", id))
			}
			if _, err := setup.Exec(ctx, fmt.Sprintf(`INSERT INTO %s VALUES %s`, table, strings.Join(values, ", "))); err != nil {
				t.Fatal(err)
			}
			dequeue := fmt.Sprintf(`SELECT id FROM %s WHERE runnable ORDER BY id LIMIT 1 FOR UPDATE SKIP LOCKED`, table)
			count := fmt.Sprintf(`SELECT count(*) FROM %s WHERE runnable`, table)
			update := fmt.Sprintf(c.update, table)
			var (
				empties, commits, retries atomic.Int64
				wg                        sync.WaitGroup
			)
			for w := 0; w < workers; w++ {
				wg.Add(1)
				go func(w int) {
					defer wg.Done()
					conn, err := pgx.Connect(ctx, pgURL(tc, 0))
					if err != nil {
						t.Error(err)
						return
					}
					defer func() { _ = conn.Close(context.Background()) }()
					for i := 0; i < iters && ctx.Err() == nil; i++ {
						drained := false
						err := func() error {
							tx, err := conn.Begin(ctx)
							if err != nil {
								return err
							}
							defer func() { _ = tx.Rollback(context.Background()) }()
							ids, err := queryIDs(ctx, tx, dequeue)
							if err != nil {
								return err
							}
							if len(ids) == 0 {
								var runnable int64
								if err := tx.QueryRow(ctx, count).Scan(&runnable); err != nil {
									return err
								}
								drained = runnable == 0
								if runnable >= workers {
									empties.Add(1)
									t.Errorf("worker %d: SKIP LOCKED returned no row while %d rows are runnable and the other workers hold at most %d", w, runnable, workers-1)
								}
								return nil
							}
							if _, err := tx.Exec(ctx, update, ids[0]); err != nil {
								return err
							}
							if err := tx.Commit(ctx); err != nil {
								return err
							}
							commits.Add(1)
							return nil
						}()
						switch {
						case err == nil:
						case pgErrCode(err) == sql.CodeSerializationFailure:
							retries.Add(1)
						default:
							t.Errorf("worker %d: %v", w, err)
							return
						}
						if drained {
							return
						}
					}
				}(w)
			}
			wg.Wait()
			t.Logf("%d workers, up to %d iterations each: %d commits, %d serialization failures, %d empty dequeues with free rows",
				workers, iters, commits.Load(), retries.Load(), empties.Load())
			// Not zero by construction (a refresh can still meet a real
			// conflict), but rare: a third of the attempts failed when
			// the fetch's whole span stayed in the read set.
			if n, total := retries.Load(), retries.Load()+commits.Load(); n*10 > total {
				t.Errorf("%d of %d attempts failed with 40001", n, total)
			}
		})
	}
}
