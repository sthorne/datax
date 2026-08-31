package testcluster

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"

	"github.com/sthorne/datax/pkg/keys"
	"github.com/sthorne/datax/pkg/kvclient"
	"github.com/sthorne/datax/pkg/kvserver"
	"github.com/sthorne/datax/pkg/metrics"
	"github.com/sthorne/datax/pkg/server"
	"github.com/sthorne/datax/pkg/sql"
	"github.com/sthorne/datax/pkg/sql/catalog"
	"github.com/sthorne/datax/pkg/sql/rowenc"
	"github.com/sthorne/datax/pkg/sql/types"
	"github.com/sthorne/datax/pkg/util/hlc"
)

// The bounded-staleness suite (issue #55): with_max_staleness('d') picks
// one statement timestamp — the freshest the gateway's local replicas can
// serve — and degrades per range to the leader, observable through the
// served-locally / fell-back counter pair.

// waitAllClosed waits until every replica on the node has a closed
// timestamp covering ts.
func waitAllClosed(t *testing.T, n *server.Node, ts hlc.Timestamp) {
	t.Helper()
	deadline := time.Now().Add(15 * time.Second)
	for {
		ok := true
		n.Store().VisitReplicas(func(r *kvserver.Replica) bool {
			if r.ClosedTimestamp().Less(ts) {
				ok = false
			}
			return true
		})
		if ok {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("closed timestamps never covered %s", ts)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// tablePKKey encodes the primary-key row key for a single-INT8-PK table —
// for splitting SQL tables at a chosen row.
func tablePKKey(t *testing.T, ctx context.Context, tc *TestCluster, table string, id int64) keys.Key {
	t.Helper()
	acc := catalog.NewAccessor()
	var key keys.Key
	err := tc.Nodes[0].DB().RunTxn(ctx, "test-pk-key", func(ctx context.Context, txn *kvclient.Txn) error {
		desc, err := acc.Lookup(ctx, txn, table)
		if err != nil {
			return err
		}
		key, err = rowenc.EncodePK(desc, []types.Datum{types.NewInt(id)})
		return err
	})
	if err != nil {
		t.Fatalf("encoding split key: %v", err)
	}
	return key
}

// TestBoundedStalenessServesLocallyUnderPartition is the headline: with
// the leader partitioned away, a bounded-staleness read on a follower
// gateway serves locally within the bound while a current-time read
// cannot proceed.
func TestBoundedStalenessServesLocallyUnderPartition(t *testing.T) {
	tc, _ := StartWithEngines(t, 3, fastClosedTS)
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	s0 := sql.NewSession(tc.Nodes[0].DB(), catalog.NewAccessor())
	execSQL(t, ctx, s0, "CREATE TABLE bs (id INT8 PRIMARY KEY, v TEXT)")
	execSQL(t, ctx, s0, "INSERT INTO bs VALUES (1, 'one'), (2, 'two')")
	writeTS := tc.Nodes[0].Clock().Now()

	leader := tc.LeaderIndex(1)
	gateway := (leader + 1) % 3
	sg := sql.NewSession(tc.Nodes[gateway].DB(), catalog.NewAccessor())
	// Warm the gateway's descriptor lease cache and routing with a plain
	// read BEFORE the partition (the reshard guard does a current-time
	// lookup; warm caches keep it off the wire).
	execSQL(t, ctx, sg, "SELECT count(*) FROM bs")
	// Every replica the gateway holds must be servable past the insert.
	waitAllClosed(t, tc.Nodes[gateway], writeTS)

	// Partition the GATEWAY off from the whole cluster: its outbound RPC
	// all drops, so only its own replicas can answer. (Isolating just the
	// leader would let the remaining pair elect a new one and current-time
	// reads would keep working; consecutive Isolate calls overwrite each
	// other's drop hooks.)
	beforeLocal := testutil.ToFloat64(metrics.FollowerReads)
	tc.Isolate(gateway)
	defer tc.Heal()

	res, serr := trySQL(ctx, sg, "SELECT v FROM bs AS OF SYSTEM TIME with_max_staleness('10s') WHERE id = 2")
	if serr != nil {
		t.Fatalf("bounded read under partition: [%s] %s", serr.Code, serr.Msg)
	}
	if len(res.Rows) != 1 || res.Rows[0][0].S != "two" {
		t.Fatalf("bounded read rows: %+v", res.Rows)
	}
	if after := testutil.ToFloat64(metrics.FollowerReads); after <= beforeLocal {
		t.Fatalf("follower-read counter did not move (%v -> %v)", beforeLocal, after)
	}

	// A current-time read needs the partitioned leader and cannot finish.
	cctx, ccancel := context.WithTimeout(ctx, 2*time.Second)
	_, serr = trySQL(cctx, sg, "SELECT count(*) FROM bs")
	ccancel()
	if serr == nil {
		t.Fatal("current-time read succeeded with the leader partitioned")
	}
}

// TestBoundedStalenessMultiRangeConsistency: one statement timestamp holds
// across ranges — concurrent bounded reads over a split table always see
// a consistent snapshot of transactionally-updated pairs.
func TestBoundedStalenessMultiRangeConsistency(t *testing.T) {
	tc, _ := StartWithEngines(t, 3, fastClosedTS)
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	s0 := sql.NewSession(tc.Nodes[0].DB(), catalog.NewAccessor())
	execSQL(t, ctx, s0, "CREATE TABLE pairs (id INT8 PRIMARY KEY, v INT8)")
	// Pairs (i, i+500) sum to 1000; a transaction moves value between the
	// halves. Split between the halves so every pair straddles two ranges.
	for i := 0; i < 8; i++ {
		execSQL(t, ctx, s0, fmt.Sprintf("INSERT INTO pairs VALUES (%d, 500), (%d, 500)", i, i+500))
	}
	if _, err := tc.Nodes[0].DB().AdminSplit(ctx, tablePKKey(t, ctx, tc, "pairs", 500)); err != nil {
		t.Fatalf("split: %v", err)
	}
	// Bounded reads resolve to the freshest locally-closed timestamp; only
	// once that passes the seed inserts do the rows become visible to
	// them (before that, an empty result is correct staleness semantics).
	setupTS := tc.Nodes[0].Clock().Now()
	waitAllClosed(t, tc.Nodes[2], setupTS)

	var stop atomic.Bool
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		sw := sql.NewSession(tc.Nodes[1].DB(), catalog.NewAccessor())
		for i := 0; !stop.Load(); i++ {
			id := i % 8
			amt := 1 + i%7
			if _, err := trySQL(ctx, sw, "BEGIN"); err != nil {
				continue
			}
			_, e1 := trySQL(ctx, sw, fmt.Sprintf("UPDATE pairs SET v = v - %d WHERE id = %d", amt, id))
			var e2 *sql.Error
			if e1 == nil {
				_, e2 = trySQL(ctx, sw, fmt.Sprintf("UPDATE pairs SET v = v + %d WHERE id = %d", amt, id+500))
			}
			if e1 == nil && e2 == nil {
				_, _ = trySQL(ctx, sw, "COMMIT")
			} else {
				_, _ = trySQL(ctx, sw, "ROLLBACK")
			}
		}
	}()

	sr := sql.NewSession(tc.Nodes[2].DB(), catalog.NewAccessor())
	deadline := time.Now().Add(8 * time.Second)
	reads := 0
	for time.Now().Before(deadline) {
		res, serr := trySQL(ctx, sr, "SELECT sum(v) FROM pairs AS OF SYSTEM TIME with_max_staleness('5s')")
		if serr != nil {
			// Below the GC threshold / before the first closed timestamp
			// early in the cluster's life: retry.
			time.Sleep(50 * time.Millisecond)
			continue
		}
		if len(res.Rows) != 1 || res.Rows[0][0].I != 16*500 {
			stop.Store(true)
			wg.Wait()
			t.Fatalf("inconsistent snapshot: %+v", res.Rows)
		}
		reads++
	}
	stop.Store(true)
	wg.Wait()
	if reads < 10 {
		t.Fatalf("only %d bounded reads completed", reads)
	}
}

// TestBoundedStalenessFallbackMetric: both fallback shapes count — a
// gateway with NO local replica of the range, and a local replica whose
// closed timestamp cannot cover the chosen (very fresh) timestamp.
func TestBoundedStalenessFallbackMetric(t *testing.T) {
	// Disable rebalancing so the joined 4th node stays replica-free.
	tc, _ := StartWithEngines(t, 3, fastClosedTS, func(c *server.Config) {
		c.RebalanceThreshold = -1
	})
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	s0 := sql.NewSession(tc.Nodes[0].DB(), catalog.NewAccessor())
	execSQL(t, ctx, s0, "CREATE TABLE fb (id INT8 PRIMARY KEY, v TEXT)")
	execSQL(t, ctx, s0, "INSERT INTO fb VALUES (1, 'x')")
	writeTS := tc.Nodes[0].Clock().Now()
	waitAllClosed(t, tc.Nodes[0], writeTS)

	// Shape 1: a tiny bound picks a timestamp fresher than any closed
	// timestamp, so the gateway's own replica answers NotLeader and the
	// leader serves — counted as a fallback, result still correct.
	leader := tc.LeaderIndex(1)
	gateway := (leader + 1) % 3
	sg := sql.NewSession(tc.Nodes[gateway].DB(), catalog.NewAccessor())
	execSQL(t, ctx, sg, "SELECT count(*) FROM fb") // warm caches
	before := testutil.ToFloat64(metrics.FollowerReadFallbacks)
	res, serr := trySQL(ctx, sg, "SELECT v FROM fb AS OF SYSTEM TIME with_max_staleness('1ms') WHERE id = 1")
	if serr != nil {
		t.Fatalf("tiny-bound read: [%s] %s", serr.Code, serr.Msg)
	}
	if len(res.Rows) != 1 || res.Rows[0][0].S != "x" {
		t.Fatalf("tiny-bound rows: %+v", res.Rows)
	}
	if after := testutil.ToFloat64(metrics.FollowerReadFallbacks); after <= before {
		t.Fatalf("local-lag fallback not counted (%v -> %v)", before, after)
	}

	// Shape 2: a fresh node with no replicas at all — every stale
	// sub-batch it routes is a fallback. Its bound resolves to now-1s
	// (no local closed timestamps), so the write must be older than the
	// bound to be visible.
	n4, err := tc.AddNodeErr(func(c *server.Config) { fastClosedTS(c) })
	if err != nil {
		t.Fatalf("joining node 4: %v", err)
	}
	s4 := sql.NewSession(n4.DB(), catalog.NewAccessor())
	execSQL(t, ctx, s4, "SELECT count(*) FROM fb") // warm caches
	time.Sleep(1500 * time.Millisecond)            // age the write past the 1s bound
	before = testutil.ToFloat64(metrics.FollowerReadFallbacks)
	res, serr = trySQL(ctx, s4, "SELECT v FROM fb AS OF SYSTEM TIME with_max_staleness('1s') WHERE id = 1")
	if serr != nil {
		t.Fatalf("replica-free gateway read: [%s] %s", serr.Code, serr.Msg)
	}
	if len(res.Rows) != 1 || res.Rows[0][0].S != "x" {
		t.Fatalf("replica-free gateway rows: %+v", res.Rows)
	}
	if after := testutil.ToFloat64(metrics.FollowerReadFallbacks); after <= before {
		t.Fatalf("no-local-replica fallback not counted (%v -> %v)", before, after)
	}
}

// TestBoundedStalenessSQLSurface: syntax and restriction errors.
func TestBoundedStalenessSQLSurface(t *testing.T) {
	tc, _ := StartWithEngines(t, 3, fastClosedTS)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	s := sql.NewSession(tc.Nodes[0].DB(), catalog.NewAccessor())
	execSQL(t, ctx, s, "CREATE TABLE sf (id INT8 PRIMARY KEY)")

	for _, c := range []struct{ q, code string }{
		{"SELECT * FROM sf AS OF SYSTEM TIME with_max_staleness('0s')", "22023"},
		{"SELECT * FROM sf AS OF SYSTEM TIME with_max_staleness('-5s')", "22023"},
		{"SELECT * FROM sf AS OF SYSTEM TIME with_max_staleness('bogus')", "22023"},
		{"SELECT * FROM sf AS OF SYSTEM TIME '5s'", "42601"}, // bare positive still rejected
		{"SELECT * FROM sf AS OF SYSTEM TIME with_max_staleness('1s') FOR UPDATE", "0A000"},
	} {
		if _, serr := trySQL(ctx, s, c.q); serr == nil || serr.Code != c.code {
			t.Fatalf("%s: got %+v, want code %s", c.q, serr, c.code)
		}
	}
	// Inside a transaction block: 25001.
	execSQL(t, ctx, s, "BEGIN")
	if _, serr := trySQL(ctx, s, "SELECT * FROM sf AS OF SYSTEM TIME with_max_staleness('1s')"); serr == nil || serr.Code != "25001" {
		t.Fatalf("in txn: %+v", serr)
	}
	execSQL(t, ctx, s, "ROLLBACK")
}
