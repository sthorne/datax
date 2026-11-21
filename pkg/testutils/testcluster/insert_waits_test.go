package testcluster

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
)

// TestInsertWaitsForUncommittedRow: an INSERT whose key a live
// transaction holds as an uncommitted row waits for that transaction, as
// the uniqueness read it carries in its write (a conditional put) would
// have, and never aborts it — whatever the two transactions' priorities.
// Before, the conditional put pushed like a write and won about half
// the time, so the holder failed its COMMIT with 40001.
func TestInsertWaitsForUncommittedRow(t *testing.T) {
	tc := Start(t, 2)
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()
	conn, err := pgx.Connect(ctx, pgURL(tc, 0))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = conn.Close(ctx) }()
	if _, err := conn.Exec(ctx, `CREATE TABLE held (k INT8 PRIMARY KEY, v TEXT)`); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 10; i++ {
		holder, err := conn.Begin(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := holder.Exec(ctx, fmt.Sprintf(`INSERT INTO held VALUES (%d, 'holder')`, i)); err != nil {
			t.Fatal(err)
		}
		// A context deadline makes pgx close the connection: a fresh one
		// per attempt.
		w, err := pgx.Connect(ctx, pgURL(tc, 1))
		if err != nil {
			t.Fatal(err)
		}
		wctx, wcancel := context.WithTimeout(ctx, 300*time.Millisecond)
		_, err = w.Exec(wctx, fmt.Sprintf(`INSERT INTO held VALUES (%d, 'other')`, i))
		wcancel()
		_ = w.Close(ctx)
		if err == nil || !errors.Is(err, context.DeadlineExceeded) && wctx.Err() == nil {
			t.Fatalf("attempt %d: the INSERT did not wait for the holder: %v", i, err)
		}
		if err := holder.Commit(ctx); err != nil {
			t.Fatalf("attempt %d: the holder was aborted by the waiting INSERT: %v", i, err)
		}
	}
}
