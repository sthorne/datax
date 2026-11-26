package testcluster

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"

	"github.com/sthorne/datax/pkg/keys"
	"github.com/sthorne/datax/pkg/metrics"
	"github.com/sthorne/datax/pkg/server"
	"github.com/sthorne/datax/pkg/sql"
	"github.com/sthorne/datax/pkg/sql/catalog"
	"github.com/sthorne/datax/pkg/sql/types"
)

// rangeIDCounter is the replicated range ID counter: every split draws
// one ID, so its growth bounds the splits the cluster made.
func rangeIDCounter(t *testing.T, ctx context.Context, n *server.Node) int64 {
	t.Helper()
	raw, err := n.DB().Get(ctx, keys.RangeIDGenKey())
	if err != nil {
		t.Fatal(err)
	}
	v, err := strconv.ParseInt(string(raw), 10, 64)
	if err != nil {
		t.Fatalf("range ID counter %q: %v", raw, err)
	}
	return v
}

// TestGCWindowChurnSplitsNothing is issue #395: a workload that creates
// a database, gives every third of its tables a GC window and a row
// expiry, and drops the database, over and over, used to split two range
// boundaries per such table at the ALTER and merge them away after the
// DROP — dozens of splits and merges a second, and with each merge
// flushing the state engine, Pebble write stalls that shed foreground
// writes in other databases. Tables that live for seconds have nothing
// for an hour's window to collect, so the GC pass leaves them on shared
// ranges: the cycles split nothing and merge nothing, the store never
// stalls, and a transaction in another database keeps a bounded latency
// throughout.
func TestGCWindowChurnSplitsNothing(t *testing.T) {
	tc := StartWithOptions(t, 1, noRowTTLLoop)
	n := tc.Nodes[0]
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()
	s := sql.NewSession(n.DB(), catalog.NewAccessor())
	probe := sql.NewSession(n.DB(), catalog.NewAccessor())
	execSQL(t, ctx, probe, `CREATE DATABASE probe`)
	execSQL(t, ctx, probe, `CREATE TABLE probe.public.p (id INT8 PRIMARY KEY, v INT8)`)
	execSQL(t, ctx, probe, `CREATE TABLE probe.public.log (id INT8 PRIMARY KEY, at TIMESTAMPTZ)`)

	var worst time.Duration
	probes := 0
	runProbe := func() {
		t.Helper()
		start := time.Now()
		k := types.NewInt(int64(probes % 10))
		execSQL(t, ctx, probe, `BEGIN`)
		execSQL(t, ctx, probe, `INSERT INTO probe.public.p VALUES ($1, 0) ON CONFLICT DO NOTHING`, k)
		execSQL(t, ctx, probe, `SELECT v FROM probe.public.p WHERE id = $1`, k)
		execSQL(t, ctx, probe, `UPDATE probe.public.p SET v = v + 1 WHERE id = $1`, k)
		execSQL(t, ctx, probe, `INSERT INTO probe.public.log VALUES ($1, now())`, types.NewInt(int64(probes)))
		execSQL(t, ctx, probe, `COMMIT`)
		probes++
		if d := time.Since(start); d > worst {
			worst = d
		}
	}

	ids0 := rangeIDCounter(t, ctx, n)
	merges0 := testutil.ToFloat64(metrics.RangeMerges)
	stalls0 := n.Store().Engine().StorageMetrics().WriteStalls
	const cycles, tables = 4, 12
	windowed := 0
	for c := 0; c < cycles; c++ {
		db := fmt.Sprintf("churn%d", c)
		execSQL(t, ctx, s, `CREATE DATABASE `+db)
		for i := 0; i < tables; i++ {
			tn := fmt.Sprintf("%s.public.t%d", db, i)
			execSQL(t, ctx, s, `CREATE TABLE `+tn+` (id INT8 PRIMARY KEY, a INT8, b TEXT, expires_at TIMESTAMPTZ DEFAULT now() + INTERVAL '1 hour')`)
			execSQL(t, ctx, s, `CREATE INDEX ON `+tn+` (a)`)
			execSQL(t, ctx, s, `CREATE INDEX ON `+tn+` (b)`)
			var vals []string
			for r := 0; r < 20; r++ {
				vals = append(vals, fmt.Sprintf("(%d, %d, 'v%d')", r, r*7, r))
			}
			execSQL(t, ctx, s, `INSERT INTO `+tn+` (id, a, b) VALUES `+strings.Join(vals, ", "))
			if i%3 == 0 {
				execSQL(t, ctx, s, `ALTER TABLE `+tn+` SET (gc_ttl = '1h')`)
				execSQL(t, ctx, s, `ALTER TABLE `+tn+` SET (ttl_expiration = expires_at)`)
				windowed++
			}
			runProbe()
		}
		if c == cycles-1 {
			// A GC pass and a merge pass while the windowed tables are
			// live: an hour's window on tables seconds old is not due.
			n.TestingRefreshRetention()
			n.Store().RunGCOnce(ctx, 25*time.Hour)
			n.Store().RunRangeMergeOnce(ctx)
		}
		execSQL(t, ctx, s, `DROP DATABASE `+db+` CASCADE`)
		runProbe()
	}
	n.Store().RunRangeMergeOnce(ctx)

	if splits := rangeIDCounter(t, ctx, n) - ids0; splits != 0 {
		t.Errorf("%d cycles with %d windowed tables split %d times, want 0", cycles, windowed, splits)
	}
	if merges := testutil.ToFloat64(metrics.RangeMerges) - merges0; merges != 0 {
		t.Errorf("the cycles merged %v times, want 0", merges)
	}
	if stalls := n.Store().Engine().StorageMetrics().WriteStalls - stalls0; stalls != 0 {
		t.Errorf("the state engine stalled %d times", stalls)
	}
	if worst > 5*time.Second {
		t.Errorf("a transaction in another database took %s (of %d)", worst, probes)
	}
	t.Logf("%d cycles, %d windowed tables, %d probes, worst %s", cycles, windowed, probes, worst)
}
