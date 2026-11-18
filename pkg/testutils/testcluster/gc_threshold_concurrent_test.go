package testcluster

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/sthorne/datax/pkg/sql"
)

// TestPurgeUnderConcurrentTxnsSQLState (issue #374, its repro): ordinary
// transactions — read, pause, write, read, commit — run alongside
// ALTER TABLE ... PURGE. A purge can move the threshold above a
// transaction's timestamp at any of its reads; each such refusal must
// reach the client as 40001 (a retry at a new timestamp succeeds), never
// 72000 (which says the client chose the timestamp) nor XX000.
func TestPurgeUnderConcurrentTxnsSQLState(t *testing.T) {
	const (
		rows    = 2000
		workers = 16
		runFor  = 6 * time.Second
	)
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()
	tc := Start(t, 1)
	admin := pgConn(t, ctx, tc, 0)
	if _, err := admin.Exec(ctx, `CREATE TABLE pt (id INT8 PRIMARY KEY, v INT8, pad TEXT)`); err != nil {
		t.Fatal(err)
	}
	if _, err := admin.Exec(ctx, `INSERT INTO pt SELECT i, 0, repeat('x', 64) FROM generate_series(1, `+strconv.Itoa(rows)+`) AS g(i)`); err != nil {
		t.Fatal(err)
	}

	runCtx, stop := context.WithTimeout(ctx, runFor)
	defer stop()
	var (
		committed, retried atomic.Int64
		mu                 sync.Mutex
		bad                []string
	)
	var wg sync.WaitGroup
	for w := 0; w < workers; w++ {
		conn := pgConn(t, ctx, tc, 0)
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for i := 0; runCtx.Err() == nil; i++ {
				id := (w*7919+i*31)%rows + 1
				step := ""
				err := pgx.BeginFunc(runCtx, conn, func(tx pgx.Tx) error {
					var s, c int64
					step = "SELECT sum(v)"
					if err := tx.QueryRow(runCtx, `SELECT sum(v) FROM pt`).Scan(&s); err != nil {
						return err
					}
					select {
					case <-runCtx.Done():
						return runCtx.Err()
					case <-time.After(20 * time.Millisecond):
					}
					step = "UPDATE"
					if _, err := tx.Exec(runCtx, `UPDATE pt SET v = v + 1 WHERE id = $1`, id); err != nil {
						return err
					}
					step = "SELECT count(*)"
					if err := tx.QueryRow(runCtx, `SELECT count(*) FROM pt WHERE v >= 0`).Scan(&c); err != nil {
						return err
					}
					step = "COMMIT"
					return nil
				})
				switch code := sqlState(err); {
				case err == nil:
					committed.Add(1)
				case code == sql.CodeSerializationFailure:
					retried.Add(1)
				case runCtx.Err() != nil:
					return
				default:
					mu.Lock()
					bad = append(bad, fmt.Sprintf("%s: %s: %v", step, code, err))
					mu.Unlock()
				}
			}
		}(w)
	}
	purges := 0
	for runCtx.Err() == nil {
		if _, err := admin.Exec(runCtx, `ALTER TABLE pt PURGE`); err != nil && runCtx.Err() == nil && !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("purge: %v", err)
		}
		purges++
		select {
		case <-runCtx.Done():
		case <-time.After(200 * time.Millisecond):
		}
	}
	wg.Wait()
	t.Logf("%d purges, %d committed, %d restarted (40001)", purges, committed.Load(), retried.Load())
	if len(bad) > 0 {
		if len(bad) > 10 {
			bad = append(bad[:10], fmt.Sprintf("... and %d more", len(bad)-10))
		}
		t.Fatalf("%d transactions failed other than with 40001:\n%s", len(bad), joinLines(bad))
	}
	if committed.Load() == 0 {
		t.Fatal("no transaction committed")
	}
}

func joinLines(ss []string) string {
	out := ""
	for _, s := range ss {
		out += "  " + s + "\n"
	}
	return out
}
