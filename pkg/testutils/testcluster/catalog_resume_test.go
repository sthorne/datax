package testcluster

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/sthorne/datax/pkg/keys"
	"github.com/sthorne/datax/pkg/kvpb"
	"github.com/sthorne/datax/pkg/server"
	"github.com/sthorne/datax/pkg/sql"
	"github.com/sthorne/datax/pkg/sql/catalog"
)

// catalogRows counts the rows node n reads out of the table descriptor
// and statistics spans, by scans, exports and gets alike, while op runs.
func catalogRows(n *server.Node, op func()) int64 {
	var rows atomic.Int64
	dlo, dhi := keys.TableDescSpan()
	slo, shi := keys.TableStatsSpan()
	in := func(k, lo, hi keys.Key) bool { return k.Compare(lo) >= 0 && k.Compare(hi) < 0 }
	n.DB().TestingSetSendObserver(func(_ context.Context, ba *kvpb.BatchRequest, br *kvpb.BatchResponse) {
		for i, ru := range ba.Requests {
			k := ru.GetInner().Header().Key
			if !(in(k, dlo, dhi) || in(k, slo, shi)) || br == nil || i >= len(br.Responses) {
				continue
			}
			switch r := br.Responses[i]; {
			case r.Scan != nil:
				rows.Add(int64(len(r.Scan.Rows)))
			case r.Export != nil:
				rows.Add(int64(len(r.Export.Records)))
			case r.Get != nil && r.Get.Value != nil:
				rows.Add(1)
			}
		}
	})
	defer n.DB().TestingSetSendObserver(nil)
	op()
	return rows.Load()
}

// catalogJobs runs one pass of each background job that follows the
// catalog on node n and returns the descriptor and statistics rows each
// read, by job.
func catalogJobs(ctx context.Context, n *server.Node) map[string]int64 {
	return map[string]int64{
		"retention": catalogRows(n, func() {
			n.TestingRefreshRetention()
			n.TestingRetentionSpans()
		}),
		"reshard": catalogRows(n, func() { n.RunReshardJanitorOnce(ctx) }),
		"stats":   catalogRows(n, func() { n.RunStatsRefreshOnce(ctx) }),
	}
}

// checkCatalogJobs fails unless each job's state on node n equals what a
// full read derives, and, for the jobs in resumed, unless it came from a
// resume record (true) or a full read (false).
func checkCatalogJobs(t *testing.T, ctx context.Context, n *server.Node, resumed map[string]bool) {
	t.Helper()
	for _, job := range []string{"retention", "reshard", "stats"} {
		got, err := n.TestingCheckCatalogFollower(ctx, job)
		if err != nil {
			t.Fatal(err)
		}
		if want, ok := resumed[job]; ok && got != want {
			t.Fatalf("%s: resumed %v, want %v", job, got, want)
		}
	}
}

func catalogResumeCfg(c *server.Config) {
	c.StatsRefreshInterval = -1 // passes run by hand
	c.StatsStaleness = time.Hour
	c.DroppedTableReclaimInterval = -1
	c.CatalogResumeEvery = time.Nanosecond // a record after every pass
}

// collectAll runs the statistics sampler on n until a pass collects
// nothing, so that what later passes read is what changed.
func collectAll(t *testing.T, ctx context.Context, n *server.Node, tables int) {
	t.Helper()
	for i := 0; i < tables+5; i++ {
		if catalogRows(n, func() { n.RunStatsRefreshOnce(ctx) }) == 0 {
			return
		}
	}
	t.Fatal("the statistics sampler kept collecting")
}

// TestCatalogFollowersResumeAfterRestart (issue #316): the retention
// provider, the re-shard janitor and the statistics sampler resume after
// a node restart from the state they persisted — the first pass reads
// what changed, including what changed while the node was down, not
// every descriptor — and a node that never ran the leader-only jobs,
// as a new leader of range 1, resumes from the record the last one
// wrote. Each state equals what a full read derives.
func TestCatalogFollowersResumeAfterRestart(t *testing.T) {
	tc, engines := StartWithEngines(t, 3, catalogResumeCfg)
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()
	n0 := tc.Nodes[0]
	s := sql.NewSession(n0.DB(), catalog.NewAccessor())
	waitForDatabases(t, ctx, s)
	const tables = 30
	for i := 0; i < tables; i++ {
		execSQL(t, ctx, s, fmt.Sprintf(`CREATE TABLE r%02d (id INT8 PRIMARY KEY)`, i))
	}
	execSQL(t, ctx, s, `CREATE TABLE r_ttl (id INT8 PRIMARY KEY) WITH (gc_ttl = '1h')`)
	execSQL(t, ctx, s, `CREATE VIEW r_v AS SELECT id FROM r00`)
	execSQL(t, ctx, s, `CREATE TABLE r_ts (series INT8, ts TIMESTAMPTZ, v INT8, PRIMARY KEY (series, ts))
		WITH (timeseries = true, shards = 2)`)
	execSQL(t, ctx, s, `ALTER TABLE r_ts SET (shards = 4)`) // a retired layout the janitor keeps

	collectAll(t, ctx, n0, tables+2)
	first := catalogJobs(ctx, n0)
	t.Logf("first passes: %v", first)
	checkCatalogJobs(t, ctx, n0, map[string]bool{"retention": false, "stats": false})
	if got := n0.TestingRetentionSpans(); got != 1 {
		t.Fatalf("retention windows %d, want 1", got)
	}

	// Down, the catalog changes: a table with a window arrives, another
	// gets one, one is dropped, another re-sharded.
	tc.StopNode(0)
	s1 := sql.NewSession(tc.Nodes[1].DB(), catalog.NewAccessor())
	for _, q := range []string{
		`CREATE TABLE r_late (id INT8 PRIMARY KEY) WITH (gc_ttl = '2h')`,
		`ALTER TABLE r01 SET (gc_ttl = '3h')`,
		`DROP TABLE r02`,
		`CREATE TABLE r_ts2 (series INT8, ts TIMESTAMPTZ, v INT8, PRIMARY KEY (series, ts))
			WITH (timeseries = true, shards = 2)`,
		`ALTER TABLE r_ts2 SET (shards = 4)`,
	} {
		execSQL(t, ctx, s1, q)
	}
	n0 = tc.RestartNode(0, engines[0], catalogResumeCfg)
	waitForDatabases(t, ctx, sql.NewSession(n0.DB(), catalog.NewAccessor()))

	// Each first pass reads what changed: the new and changed
	// descriptors and the dropped one, not the ~35 there are; the
	// sampler then collects r_late (two more rows: the descriptor it
	// collects and, from the next pass, the record it wrote).
	after := catalogJobs(ctx, n0)
	t.Logf("first passes after a restart: %v", after)
	for job, rows := range after {
		if rows > 12 {
			t.Fatalf("%s read %d descriptor/statistics rows on its first pass after a restart, want what changed (the first pass read %d)",
				job, rows, first[job])
		}
	}
	checkCatalogJobs(t, ctx, n0, map[string]bool{"retention": true, "reshard": true, "stats": true})
	if got := n0.TestingRetentionSpans(); got != 3 {
		t.Fatalf("retention windows after the restart %d, want 3 (r_ttl, r_late, r01)", got)
	}

	// Another node takes the leader-only jobs over from the records.
	n2 := tc.Nodes[2]
	execSQL(t, ctx, s1, `CREATE TABLE r_later (id INT8 PRIMARY KEY)`)
	for job, rows := range map[string]int64{
		"reshard": catalogRows(n2, func() { n2.RunReshardJanitorOnce(ctx) }),
		"stats":   catalogRows(n2, func() { n2.RunStatsRefreshOnce(ctx) }),
	} {
		if rows > 8 {
			t.Fatalf("%s read %d rows on another node's first pass, want what changed", job, rows)
		}
	}
	for _, job := range []string{"reshard", "stats"} {
		resumed, err := n2.TestingCheckCatalogFollower(ctx, job)
		if err != nil {
			t.Fatal(err)
		}
		// The janitor's own loop may have run here first, if this node
		// led range 1 for a tick; the sampler's is off.
		if job == "stats" && !resumed {
			t.Fatalf("%s on another node did not resume from the record", job)
		}
	}
}

// TestCatalogFollowersFallBack (issue #316): a resume record whose
// interval has fallen below the GC threshold of the spans, or whose
// version is not the job's, is not used: the first pass reads
// everything, as it did before there were records, and the state equals
// what a full read derives.
func TestCatalogFollowersFallBack(t *testing.T) {
	tc, engines := StartWithEngines(t, 1, catalogResumeCfg)
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()
	n := tc.Nodes[0]
	s := sql.NewSession(n.DB(), catalog.NewAccessor())
	waitForDatabases(t, ctx, s)
	const tables = 20
	for i := 0; i < tables; i++ {
		execSQL(t, ctx, s, fmt.Sprintf(`CREATE TABLE f%02d (id INT8 PRIMARY KEY)`, i))
	}
	execSQL(t, ctx, s, `CREATE TABLE f_ttl (id INT8 PRIMARY KEY) WITH (gc_ttl = '1h')`)
	collectAll(t, ctx, n, tables+1)
	catalogJobs(ctx, n)

	// A superseded descriptor version for GC to collect, then GC with a
	// window that ends after the records' timestamps.
	execSQL(t, ctx, s, `ALTER TABLE f00 SET (gc_ttl = '2h')`)
	time.Sleep(20 * time.Millisecond)
	n.Store().RunGCOnce(ctx, 10*time.Millisecond)
	tc.StopNode(0)
	n = tc.RestartNode(0, engines[0], catalogResumeCfg)
	waitForDatabases(t, ctx, sql.NewSession(n.DB(), catalog.NewAccessor()))
	rows := catalogJobs(ctx, n)
	t.Logf("first passes after GC passed the records: %v", rows)
	for _, job := range []string{"retention", "reshard"} {
		if rows[job] < tables {
			t.Fatalf("%s read %d rows past the GC threshold, want a full read (at least %d)", job, rows[job], tables)
		}
	}
	checkCatalogJobs(t, ctx, n, map[string]bool{"retention": false, "reshard": false, "stats": false})
	if got := n.TestingRetentionSpans(); got != 2 {
		t.Fatalf("retention windows %d, want 2", got)
	}

	// Records of another version — written by a binary whose state the
	// one resuming does not know — are not used either.
	stats, err := n.DB().Get(ctx, keys.CatalogResumeKey("stats"))
	if err != nil || len(stats) == 0 {
		t.Fatalf("the sampler's record: %d bytes, %v", len(stats), err)
	}
	tc.StopNode(0)
	stats[0] = 0x7f // the format version, a uvarint
	retention, err := engines[0].Get(keys.StoreCatalogResumeKey("retention"))
	if err != nil || len(retention) == 0 {
		t.Fatalf("the retention provider's record: %d bytes, %v", len(retention), err)
	}
	retention[0] = 0x7f
	if err := engines[0].Put(keys.StoreCatalogResumeKey("retention"), retention); err != nil {
		t.Fatal(err)
	}
	n = tc.RestartNode(0, engines[0], catalogResumeCfg)
	waitForDatabases(t, ctx, sql.NewSession(n.DB(), catalog.NewAccessor()))
	if err := n.DB().Put(ctx, keys.CatalogResumeKey("stats"), stats); err != nil {
		t.Fatal(err)
	}
	rows = catalogJobs(ctx, n)
	t.Logf("first passes from records of another version: %v", rows)
	if rows["retention"] < tables || rows["stats"] < tables || rows["reshard"] > 4 {
		t.Fatalf("rows read %v: want full reads for the retention provider and the sampler, what changed for the janitor", rows)
	}
	checkCatalogJobs(t, ctx, n, map[string]bool{"retention": false, "reshard": true, "stats": false})
}
