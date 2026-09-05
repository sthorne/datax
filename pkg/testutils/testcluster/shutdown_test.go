package testcluster

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

	"github.com/sthorne/datax/pkg/kvpb"
	"github.com/sthorne/datax/pkg/kvserver"
)

// TestGracefulShutdown: draining a node hands every lease it holds to a
// peer and ends its SQL connections cleanly — an idle connection and a
// busy one get 57P01 (admin_shutdown) at their next idle point, one
// holding a transaction open is cut at the deadline with the same
// error, the listener refuses new connections — and a client that
// reconnects to another node sees no other error. After the stop the
// cluster answers at once through the survivors: no lease had to expire.
func TestGracefulShutdown(t *testing.T) {
	tc := Start(t, 3)
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()
	if err := tc.waitForReplication(ctx, 3, ""); err != nil {
		t.Fatal(err)
	}
	setup, err := pgx.Connect(ctx, pgURL(tc, 0))
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	if _, err := setup.Exec(ctx, `CREATE TABLE gs (id INT8 PRIMARY KEY, v INT8)`); err != nil {
		t.Fatalf("create: %v", err)
	}
	_ = setup.Close(ctx)

	idle, err := pgx.Connect(ctx, pgURL(tc, 0))
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	txc, err := pgx.Connect(ctx, pgURL(tc, 0))
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	tx, err := txc.Begin(ctx)
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	if _, err := tx.Exec(ctx, `INSERT INTO gs VALUES (-1, 0)`); err != nil {
		t.Fatalf("insert in txn: %v", err)
	}

	// A client writing through n1 the whole time: on 57P01 it moves to
	// n2 and carries on; anything else is a failure.
	var (
		mu            sync.Mutex
		unexpected    []error
		before, after int
		retried       int
	)
	stopWorker := make(chan struct{})
	workerDone := make(chan struct{})
	go func() {
		defer close(workerDone)
		node := 0
		c, err := pgx.Connect(ctx, pgURL(tc, node))
		if err != nil {
			mu.Lock()
			unexpected = append(unexpected, err)
			mu.Unlock()
			return
		}
		defer func() { _ = c.Close(ctx) }()
		for i := 1; ; i++ {
			select {
			case <-stopWorker:
				return
			default:
			}
			_, err := c.Exec(ctx, `UPSERT INTO gs VALUES ($1, $2)`, i, i)
			if err == nil {
				mu.Lock()
				if node == 0 {
					before++
				} else {
					after++
				}
				mu.Unlock()
				continue
			}
			var pgErr *pgconn.PgError
			switch {
			case node == 0 && errors.As(err, &pgErr) && pgErr.Code == "57P01":
				_ = c.Close(ctx)
				node = 1
				if c, err = pgx.Connect(ctx, pgURL(tc, node)); err != nil {
					mu.Lock()
					unexpected = append(unexpected, err)
					mu.Unlock()
					return
				}
			case errors.As(err, &pgErr) && pgErr.Code == "40001":
				// A write caught by a lease handoff: retryable, as any
				// serializable client expects.
				mu.Lock()
				retried++
				mu.Unlock()
				i--
			default:
				mu.Lock()
				unexpected = append(unexpected, err)
				mu.Unlock()
				return
			}
		}
	}()
	time.Sleep(500 * time.Millisecond)

	// Rebalancing may have moved every lease off n1 by now; give it two
	// back so the drain has something to hand over.
	var descs []kvpb.RangeDescriptor
	tc.Nodes[0].Store().VisitReplicas(func(r *kvserver.Replica) bool {
		descs = append(descs, r.Desc())
		return len(descs) < 2
	})
	for _, d := range descs {
		if err := tc.Nodes[1].DB().AdminTransferLease(ctx, d.StartKey, tc.Nodes[0].NodeID()); err != nil {
			t.Fatalf("lease of %s to n1: %v", d.RangeID, err)
		}
	}

	// The budget is generous: the test is about what the drain does with
	// each connection and lease, not how fast — under a loaded machine
	// the worker's statement in flight can take seconds to reach its
	// boundary, and a lapsed budget cuts it instead of closing it.
	dctx, dcancel := context.WithTimeout(ctx, 15*time.Second)
	rep := tc.Nodes[0].Drain(dctx)
	dcancel()
	t.Logf("n1: %s", rep)
	if rep.LeasesTransferred == 0 || rep.LeasesKept != 0 {
		t.Fatalf("leases: %+v", rep)
	}
	if rep.ConnsClosed < 2 || rep.ConnsCut != 1 {
		t.Fatalf("connections: %+v (want the idle and the worker closed, the open transaction cut)", rep)
	}
	leaders := 0
	tc.Nodes[0].Store().VisitReplicas(func(r *kvserver.Replica) bool {
		if r.IsLeader() {
			leaders++
		}
		return true
	})
	if leaders != 0 {
		t.Fatalf("n1 still leads %d ranges after the drain", leaders)
	}

	var pgErr *pgconn.PgError
	var one int
	if err := idle.QueryRow(ctx, `SELECT 1`).Scan(&one); !errors.As(err, &pgErr) || pgErr.Code != "57P01" {
		t.Fatalf("idle connection after drain: %v, want 57P01", err)
	}
	if _, err := tx.Exec(ctx, `INSERT INTO gs VALUES (-2, 0)`); !errors.As(err, &pgErr) || pgErr.Code != "57P01" {
		t.Fatalf("open transaction after drain: %v, want 57P01", err)
	}
	if c, err := pgx.Connect(ctx, pgURL(tc, 0)); err == nil {
		_ = c.Close(ctx)
		t.Fatal("a drained node accepted a new SQL connection")
	}

	close(stopWorker)
	<-workerDone
	mu.Lock()
	defer mu.Unlock()
	if len(unexpected) > 0 {
		t.Fatalf("client errors other than 57P01: %v", unexpected)
	}
	if before == 0 || after == 0 {
		t.Fatalf("worker wrote %d rows through n1 and %d through n2", before, after)
	}
	t.Logf("worker: %d writes via n1, %d via n2, %d retried", before, after, retried)

	// Stop n1: the survivors answer without waiting for anything to expire.
	tc.StopNode(0)
	c2, err := pgx.Connect(ctx, pgURL(tc, 1))
	if err != nil {
		t.Fatalf("connect n2: %v", err)
	}
	defer func() { _ = c2.Close(ctx) }()
	start := time.Now()
	var count int
	if err := c2.QueryRow(ctx, `SELECT count(*) FROM gs`).Scan(&count); err != nil {
		t.Fatalf("count after stop: %v", err)
	}
	if took := time.Since(start); took > 2*time.Second {
		t.Fatalf("first read after the stop took %s", took)
	}
	if count != before+after {
		var negative, maxID int
		_ = c2.QueryRow(ctx, `SELECT count(*) FROM gs WHERE id < 0`).Scan(&negative)
		_ = c2.QueryRow(ctx, `SELECT max(id) FROM gs`).Scan(&maxID)
		t.Fatalf("count = %d, want %d (the open transaction's row rolled back): %d negative ids, max id %d", count, before+after, negative, maxID)
	}
}
