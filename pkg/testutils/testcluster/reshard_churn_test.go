package testcluster

import (
	"context"
	"fmt"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/sthorne/datax/pkg/server"
	"github.com/sthorne/datax/pkg/sql/types"
)

// TestReshardUnderMergeChurn: an online re-shard under live ingest while
// housekeeping runs every second, so the re-shard's pre-split ranges are
// merged back underneath it and range leadership moves constantly. Issue
// #74's stall: a leadership interruption that completed entirely while a
// replica's raft loop was busy (lost and regained, or transferred away
// and back) reached the loop as a Ready with a higher term but no
// SoftState change, so proposals from the old term — the backfill's
// chunk writes — were never answered and the ALTER waited out its
// deadline, its intents blocking the ingest. The ALTER must finish well
// inside the deadline and the ingest must never fail.
func TestReshardUnderMergeChurn(t *testing.T) {
	tc := StartWithOptions(t, 3, func(cfg *server.Config) { cfg.GCInterval = time.Second })
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()
	const ttl = 2 * time.Second
	sA := leasedSession(t, tc, 0, ttl)
	sB := leasedSession(t, tc, 1, ttl)
	execSQL(t, ctx, sA, `CREATE TABLE mc (series INT8, ts TIMESTAMPTZ, tag INT8 NOT NULL, v INT8 NOT NULL,
		PRIMARY KEY (series, ts)) WITH (timeseries = true, shards = 2)`)
	execSQL(t, ctx, sA, `CREATE UNIQUE INDEX by_tag ON mc (tag)`)
	base := time.Date(2026, 8, 30, 0, 0, 0, 0, time.UTC).UnixNano()
	at := func(i int) types.Datum { return types.NewTimestamp(base + int64(i)*int64(time.Second)) }
	for i := 0; i < 50; i++ {
		execSQL(t, ctx, sA, `INSERT INTO mc VALUES ($1, $2, $3, $4)`,
			types.NewInt(int64(i%5)), at(i), types.NewInt(int64(i)), types.NewInt(int64(i%5)))
	}

	var inserted atomic.Int64
	stop := make(chan struct{})
	done := make(chan error, 1)
	go func() {
		for i := 0; ; i++ {
			select {
			case <-stop:
				done <- nil
				return
			default:
			}
			if i >= 300 {
				<-stop
				done <- nil
				return
			}
			time.Sleep(2 * time.Millisecond)
			if _, serr := trySQL(ctx, sB, `INSERT INTO mc VALUES ($1, $2, $3, 7)`,
				types.NewInt(int64(100+i%7)), at(1000+i), types.NewInt(int64(1000+i))); serr != nil {
				done <- fmt.Errorf("concurrent insert %d: [%s] %s", i, serr.Code, serr.Msg)
				return
			}
			inserted.Add(1)
		}
	}()
	for inserted.Load() < 3 {
		time.Sleep(10 * time.Millisecond)
	}

	// The ALTER must land promptly: a stalled proposal would hold it to
	// the context deadline, and the ingest with it.
	start := time.Now()
	alterDone := make(chan error, 1)
	go func() {
		_, serr := trySQL(ctx, sA, `ALTER TABLE mc SET (shards = 8)`)
		if serr != nil {
			alterDone <- fmt.Errorf("[%s] %s", serr.Code, serr.Msg)
			return
		}
		alterDone <- nil
	}()
	select {
	case err := <-alterDone:
		if err != nil {
			t.Fatalf("re-shard under merge churn: %v", err)
		}
	case <-time.After(60 * time.Second):
		t.Fatal("re-shard under merge churn stalled for 60s (issue #74)")
	}
	t.Logf("re-shard took %s", time.Since(start).Truncate(time.Millisecond))
	close(stop)
	if err := <-done; err != nil {
		if strings.Contains(err.Error(), "proposal abandoned") {
			t.Fatalf("ingest hit an orphaned proposal (issue #74): %v", err)
		}
		t.Fatal(err)
	}

	// Everything readable afterwards, on both gateways.
	want := 50 + int(inserted.Load())
	res := execSQL(t, ctx, sA, `SELECT COUNT(*) AS n FROM mc`)
	if got := int(res.Rows[0][0].I); got != want {
		t.Fatalf("gateway A: %d rows, want %d", got, want)
	}
	res = execSQL(t, ctx, sB, `SELECT COUNT(*) AS n FROM mc`)
	if got := int(res.Rows[0][0].I); got != want {
		t.Fatalf("gateway B: %d rows, want %d", got, want)
	}
}
