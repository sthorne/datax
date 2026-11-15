package testcluster

import (
	"context"
	"fmt"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"

	"github.com/sthorne/datax/pkg/keys"
	"github.com/sthorne/datax/pkg/kvclient"
	"github.com/sthorne/datax/pkg/kvpb"
	"github.com/sthorne/datax/pkg/kvserver"
	"github.com/sthorne/datax/pkg/metrics"
	"github.com/sthorne/datax/pkg/server"
	"github.com/sthorne/datax/pkg/sql"
	"github.com/sthorne/datax/pkg/sql/catalog"
	"github.com/sthorne/datax/pkg/storage"
	"github.com/sthorne/datax/pkg/util/hlc"
	"github.com/sthorne/datax/pkg/version"
)

// The replicated range clear in a cluster (issue #316): the dropped-table
// reclaim empties a dropped table's ranges with one raft command each
// once the cluster is at v22, and every replica ends up the same.

// tableIDOf resolves a table in the default database.
func tableIDOf(t *testing.T, ctx context.Context, n *server.Node, name string) uint64 {
	t.Helper()
	var id uint64
	if err := n.DB().RunTxn(ctx, "lookup", func(ctx context.Context, txn *kvclient.Txn) error {
		d, err := catalog.NewAccessor().LookupIn(ctx, txn, catalog.DefaultDatabase, name)
		if err == nil {
			id = d.ID
		}
		return err
	}); err != nil {
		t.Fatal(err)
	}
	return id
}

// rangeAt returns the descriptor of the range holding k, as node i holds it.
func rangeAt(n *server.Node, k keys.Key) (kvpb.RangeDescriptor, bool) {
	var d kvpb.RangeDescriptor
	found := false
	n.Store().VisitReplicas(func(r *kvserver.Replica) bool {
		if desc := r.Desc(); desc.ContainsKey(k) {
			d, found = desc, true
			return false
		}
		return true
	})
	return d, found
}

// insertDroppedRows fills table name (id INT8, v TEXT) with rows [from, to).
func insertDroppedRows(t *testing.T, ctx context.Context, s *sql.Session, name string, from, to int) {
	t.Helper()
	for lo := from; lo < to; lo += 500 {
		var b strings.Builder
		fmt.Fprintf(&b, "INSERT INTO %s VALUES ", name)
		for i := lo; i < min(lo+500, to); i++ {
			if i > lo {
				b.WriteString(", ")
			}
			fmt.Fprintf(&b, "(%d, 'row-%d-payload-of-some-length')", i, i)
		}
		execSQL(t, ctx, s, b.String())
	}
}

// reclaimWhenDue runs the reclaim job until it has reclaimed one table.
func reclaimWhenDue(t *testing.T, ctx context.Context, n *server.Node) {
	t.Helper()
	deadline := time.Now().Add(60 * time.Second)
	for n.RunDroppedTableReclaimOnce(ctx) == 0 {
		if time.Now().After(deadline) {
			t.Fatal("the dropped table was never reclaimed")
		}
		time.Sleep(200 * time.Millisecond)
	}
}

// TestClearRangeReplicasAgree: on three nodes, the reclaim clears a
// dropped table's range with a range clear while consistency probes run
// on it; afterwards every replica holds nothing of the span, has a size
// of zero and the clear's threshold, and the probes never disagreed. A
// read below the clear is refused — AS OF SYSTEM TIME with
// snapshot_too_old, and a follower read on a replica that served the same
// read before the clear — and a follower read above it sees nothing.
func TestClearRangeReplicasAgree(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 180*time.Second)
	defer cancel()
	tc, engines := StartWithEngines(t, 3, fastClosedTS, func(cfg *server.Config) { cfg.DroppedTableReclaimInterval = -1 })
	n := tc.Nodes[0]
	s := sql.NewSession(n.DB(), catalog.NewAccessor())
	waitForDatabases(t, ctx, s)
	execSQL(t, ctx, s, `CREATE TABLE gone (id INT8 PRIMARY KEY, v TEXT) WITH (gc_ttl = '1s')`)
	rowCount := 600
	if raceEnabled {
		rowCount = 150
	}
	insertDroppedRows(t, ctx, s, "gone", 0, rowCount)
	execSQL(t, ctx, s, `UPDATE gone SET v = 'second version' WHERE id < 50`)
	id := tableIDOf(t, ctx, n, "gone")
	start, end := keys.TableDataSpan(id)
	before := n.Clock().Now()
	beforeSQL := strconv.FormatInt(before.WallTime, 10)

	// A follower serves a read of the table below the drop before the
	// clear.
	desc, ok := rangeAt(n, start)
	if !ok || len(desc.Replicas) != 3 {
		t.Fatalf("the table's range: %+v", desc)
	}
	leader := tc.LeaderIndex(desc.RangeID)
	follower := (leader + 1) % 3
	frep, ok := tc.Nodes[follower].Store().GetReplica(desc.RangeID)
	if !ok {
		t.Fatal("no follower replica")
	}
	staleScan := func(ts hlc.Timestamp) (*kvpb.BatchResponse, *kvpb.Error) {
		ba := &kvpb.BatchRequest{Header: kvpb.BatchHeader{RangeID: desc.RangeID, Timestamp: ts, StaleRead: true}}
		ba.Add(&kvpb.ScanRequest{RequestHeader: kvpb.RequestHeader{Key: start, EndKey: end}})
		return frep.Execute(ctx, ba)
	}
	waitClosed(t, frep, before)
	if br, kerr := staleScan(before); kerr != nil || len(br.Responses[0].Scan.Rows) != rowCount {
		t.Fatalf("a follower read below the drop before the clear: %v", kerr)
	}
	execSQL(t, ctx, s, `DROP TABLE gone`)

	// Consistency probes on the table's range, from whichever node leads
	// it, until the reclaim is over.
	var probes, mismatches atomic.Int64
	stop := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-stop:
				return
			case <-time.After(20 * time.Millisecond):
			}
			for _, node := range tc.Nodes {
				d, ok := rangeAt(node, start)
				if !ok {
					continue
				}
				if r, ok := node.Store().GetReplica(d.RangeID); !ok || !r.IsLeader() {
					continue
				}
				mismatch, err := node.CheckRangeConsistency(ctx, d.RangeID)
				if err == nil {
					probes.Add(1)
				}
				if mismatch {
					mismatches.Add(1)
				}
			}
		}
	}()
	clears := testutil.ToFloat64(metrics.RangeClears)
	reclaimWhenDue(t, ctx, n)
	close(stop)
	wg.Wait()
	if got := testutil.ToFloat64(metrics.RangeClears) - clears; got < 1 {
		t.Fatalf("the reclaim did not use a range clear (%v clears)", got)
	}
	if mismatches.Load() != 0 || probes.Load() == 0 {
		t.Fatalf("consistency probes during the reclaim: %d of %d disagreed", mismatches.Load(), probes.Load())
	}

	// After it: the same empty state everywhere.
	desc, _ = rangeAt(n, start)
	leader = tc.LeaderIndex(desc.RangeID)
	if mismatch, err := tc.Nodes[leader].CheckRangeConsistency(ctx, desc.RangeID); err != nil || mismatch {
		t.Fatalf("consistency after the reclaim: mismatch=%v %v", mismatch, err)
	}
	var thr hlc.Timestamp
	for i, node := range tc.Nodes {
		r, ok := node.Store().GetReplica(desc.RangeID)
		if !ok {
			t.Fatalf("n%d holds no replica of %s", i+1, desc.RangeID)
		}
		if r.SizeBytes() != 0 || !before.Less(r.GCThreshold()) {
			t.Fatalf("n%d after the clear: size %d, threshold %s (drop read at %s)", i+1, r.SizeBytes(), r.GCThreshold(), before)
		}
		if i > 0 && r.GCThreshold() != thr {
			t.Fatalf("n%d threshold %s, n1 %s", i+1, r.GCThreshold(), thr)
		}
		thr = r.GCThreshold()
		census, err := storage.MVCCSpanCensus(engines[i], start, end)
		if err != nil {
			t.Fatal(err)
		}
		if census != (storage.SpanCensus{}) {
			t.Fatalf("n%d still holds %+v of the span", i+1, census)
		}
	}

	// Below the clear: refused.
	if _, serr := trySQL(ctx, s, `SELECT count(*) FROM gone AS OF SYSTEM TIME '`+beforeSQL+`'`); serr == nil || serr.Code != sql.CodeSnapshotTooOld {
		t.Fatalf("AS OF SYSTEM TIME below the clear: %v, want %s", serr, sql.CodeSnapshotTooOld)
	}
	frep, ok = tc.Nodes[follower].Store().GetReplica(desc.RangeID)
	if !ok || frep.IsLeader() {
		follower = (tc.LeaderIndex(desc.RangeID) + 1) % 3
		frep, _ = tc.Nodes[follower].Store().GetReplica(desc.RangeID)
	}
	if _, kerr := staleScan(before); kerr == nil || !kerr.IsBelowGCThreshold() {
		t.Fatalf("a follower read below the clear: %v", kerr)
	}
	// Above it, once closed: nothing.
	above := thr.Next()
	waitClosed(t, frep, above)
	if br, kerr := staleScan(above); kerr != nil || len(br.Responses[0].Scan.Rows) != 0 {
		t.Fatalf("a follower read above the clear: %v", kerr)
	}
}

// TestClearRangeReplicaCaughtUpBySnapshot: a replica down during the
// reclaim misses the clear, and the purge after it truncates the log
// beneath it, so it comes back by a snapshot taken after the clear; it
// then holds what the others hold.
func TestClearRangeReplicaCaughtUpBySnapshot(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 180*time.Second)
	defer cancel()
	opts := func(cfg *server.Config) { cfg.DroppedTableReclaimInterval = -1 }
	tc, engines := StartWithEngines(t, 3, opts)
	n := tc.Nodes[0]
	s := sql.NewSession(n.DB(), catalog.NewAccessor())
	waitForDatabases(t, ctx, s)
	execSQL(t, ctx, s, `CREATE TABLE gone (id INT8 PRIMARY KEY, v TEXT) WITH (gc_ttl = '1s')`)
	insertDroppedRows(t, ctx, s, "gone", 0, 200)
	id := tableIDOf(t, ctx, n, "gone")
	start, end := keys.TableDataSpan(id)
	desc, ok := rangeAt(n, start)
	if !ok || len(desc.Replicas) != 3 {
		t.Fatalf("the table's range: %+v", desc)
	}
	// Stop a node that is neither the table range's leader nor n1.
	down := 1
	if tc.LeaderIndex(desc.RangeID) == down {
		down = 2
	}
	execSQL(t, ctx, s, `DROP TABLE gone`)
	tc.StopNode(down)
	reclaimWhenDue(t, ctx, n)
	snapshots := testutil.ToFloat64(metrics.CatchupSnapshots)
	tc.RestartNode(down, engines[down], opts)

	desc, _ = rangeAt(n, start)
	var leaderIdx int
	var target uint64
	deadline := time.Now().Add(60 * time.Second)
	for {
		leaderIdx = tc.LeaderIndex(desc.RangeID)
		if r, ok := tc.Nodes[leaderIdx].Store().GetReplica(desc.RangeID); ok {
			target = r.AppliedIndex()
		}
		if r, ok := tc.Nodes[down].Store().GetReplica(desc.RangeID); ok && target > 0 && r.AppliedIndex() >= target {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("n%d never caught up on %s", down+1, desc.RangeID)
		}
		time.Sleep(100 * time.Millisecond)
	}
	if testutil.ToFloat64(metrics.CatchupSnapshots) == snapshots {
		t.Fatalf("n%d caught up without a snapshot", down+1)
	}
	if mismatch, err := tc.Nodes[leaderIdx].CheckRangeConsistency(ctx, desc.RangeID); err != nil || mismatch {
		t.Fatalf("consistency after the catch-up: mismatch=%v %v", mismatch, err)
	}
	r, _ := tc.Nodes[down].Store().GetReplica(desc.RangeID)
	lr, _ := tc.Nodes[leaderIdx].Store().GetReplica(desc.RangeID)
	if r.SizeBytes() != 0 || r.GCThreshold() != lr.GCThreshold() {
		t.Fatalf("the returned replica: size %d, threshold %s; leader's %s", r.SizeBytes(), r.GCThreshold(), lr.GCThreshold())
	}
	census, err := storage.MVCCSpanCensus(engines[down], start, end)
	if err != nil {
		t.Fatal(err)
	}
	if census.LiveKeys+census.Garbage+census.Tombstones+census.Intents != 0 {
		t.Fatalf("the returned replica holds %+v of the span", census)
	}
}

// TestDropTableReclaimBelowV22DeletesRows: on a cluster that has not
// finalized v22 a replica may not know the range clear, so the reclaim
// deletes the rows one by one as before, and never proposes a clear.
func TestDropTableReclaimBelowV22DeletesRows(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()
	tc := StartWithOptions(t, 1, func(cfg *server.Config) {
		cfg.DroppedTableReclaimInterval = -1
		cfg.BinaryVersionOverride = version.V21
	})
	n := tc.Nodes[0]
	if cv := n.ClusterVersion(); cv >= version.V22 {
		t.Fatalf("cluster at %s", cv)
	}
	s := sql.NewSession(n.DB(), catalog.NewAccessor())
	waitForDatabases(t, ctx, s)
	execSQL(t, ctx, s, `CREATE TABLE gone (id INT8 PRIMARY KEY, v TEXT) WITH (gc_ttl = '1s')`)
	insertDroppedRows(t, ctx, s, "gone", 0, 50)
	id := tableIDOf(t, ctx, n, "gone")
	start, end := keys.TableDataSpan(id)
	execSQL(t, ctx, s, `DROP TABLE gone`)
	clears := testutil.ToFloat64(metrics.RangeClears)
	entries := testutil.ToFloat64(metrics.RaftEntriesAppended)
	reclaimWhenDue(t, ctx, n)
	if got := testutil.ToFloat64(metrics.RangeClears) - clears; got != 0 {
		t.Fatalf("a v21 cluster proposed %v range clear(s)", got)
	}
	if kvs, err := n.DB().Scan(ctx, start, end, 0); err != nil || len(kvs) != 0 {
		t.Fatalf("%d rows left: %v", len(kvs), err)
	}
	if got := testutil.ToFloat64(metrics.RaftEntriesAppended) - entries; got < 2 {
		t.Fatalf("the row-by-row reclaim wrote %v raft entries", got)
	}
	if _, err := n.ClearSpan(ctx, start, end); err == nil || !strings.Contains(err.Error(), "cluster version") {
		t.Fatalf("a clear below v22: %v", err)
	}
}

// TestDropTableReclaimMeasure measures reclaiming a dropped table on three
// nodes, row by row (a v21 cluster) against a range clear (v22): raft
// entries and their payload bytes summed over the three stores, and the
// job's wall time, the purge after it included in both. Opt-in:
// DATAX_RECLAIM_MEASURE=1 (rows: DATAX_RECLAIM_MEASURE_ROWS, default
// 100000).
func TestDropTableReclaimMeasure(t *testing.T) {
	if os.Getenv("DATAX_RECLAIM_MEASURE") == "" {
		t.Skip("set DATAX_RECLAIM_MEASURE=1 to measure the dropped-table reclaim")
	}
	rowCount := 100000
	if v := os.Getenv("DATAX_RECLAIM_MEASURE_ROWS"); v != "" {
		var err error
		if rowCount, err = strconv.Atoi(v); err != nil {
			t.Fatal(err)
		}
	}
	for _, mode := range []struct {
		name string
		cv   version.Version
	}{{"row-by-row (v21)", version.V21}, {"range clear (v22)", version.V22}} {
		t.Run(mode.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 20*time.Minute)
			defer cancel()
			tc := StartWithOptions(t, 3, func(cfg *server.Config) {
				cfg.DroppedTableReclaimInterval = -1
				cfg.BinaryVersionOverride = mode.cv
			})
			n := tc.Nodes[0]
			s := sql.NewSession(n.DB(), catalog.NewAccessor())
			waitForDatabases(t, ctx, s)
			execSQL(t, ctx, s, `CREATE TABLE gone (id INT8 PRIMARY KEY, v TEXT) WITH (gc_ttl = '1s')`)
			insertDroppedRows(t, ctx, s, "gone", 0, rowCount)
			start, end := keys.TableDataSpan(tableIDOf(t, ctx, n, "gone"))
			execSQL(t, ctx, s, `DROP TABLE gone`)
			time.Sleep(8 * time.Second) // the window and the job's slack
			entries0 := testutil.ToFloat64(metrics.RaftEntriesAppended)
			bytes0 := testutil.ToFloat64(metrics.RaftEntryBytesAppended)
			clears0 := testutil.ToFloat64(metrics.RangeClears)
			began := time.Now()
			if got := n.RunDroppedTableReclaimOnce(ctx); got != 1 {
				t.Fatalf("reclaimed %d tables", got)
			}
			took := time.Since(began)
			entries := testutil.ToFloat64(metrics.RaftEntriesAppended) - entries0
			bytes := testutil.ToFloat64(metrics.RaftEntryBytesAppended) - bytes0
			if kvs, err := n.DB().Scan(ctx, start, end, 0); err != nil || len(kvs) != 0 {
				t.Fatalf("%d rows left: %v", len(kvs), err)
			}
			t.Logf("%s, %d rows: %.0f raft entries, %.0f payload bytes (three stores), %v wall, %.0f range clear(s)",
				mode.name, rowCount, entries, bytes, took.Round(time.Millisecond), testutil.ToFloat64(metrics.RangeClears)-clears0)
		})
	}
}
