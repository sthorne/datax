package testcluster

import (
	"context"
	"fmt"
	"sort"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/sthorne/datax/pkg/kvserver"
	"github.com/sthorne/datax/pkg/server"
	"github.com/sthorne/datax/pkg/sql/catalog"
)

// ddlCycle runs one create-migrate-drop cycle of a test suite: a
// database, tables with an index and a few rows each, a migrations
// table, then DROP DATABASE.
func ddlCycle(ctx context.Context, conn *pgx.Conn, db string, tables int) error {
	stmts := []string{`CREATE DATABASE ` + db, `USE ` + db,
		`CREATE TABLE schema_migrations (version INT8 PRIMARY KEY)`}
	for i := 0; i < tables; i++ {
		stmts = append(stmts,
			fmt.Sprintf(`CREATE TABLE t%d (id INT8 PRIMARY KEY, v TEXT)`, i),
			fmt.Sprintf(`CREATE INDEX t%d_v ON t%d (v)`, i, i),
			fmt.Sprintf(`INSERT INTO t%d VALUES (1, 'a'), (2, 'b')`, i),
			fmt.Sprintf(`INSERT INTO schema_migrations VALUES (%d)`, i))
	}
	stmts = append(stmts, `USE datax`, `DROP DATABASE `+db+` CASCADE`)
	for _, s := range stmts {
		if _, err := conn.Exec(ctx, s); err != nil {
			return fmt.Errorf("%s: %w", s, err)
		}
	}
	return nil
}

// TestDDLCycleReclaim (issue #391): a test suite's create-migrate-drop
// cycle, run over and over, leaves the node where it started once the
// dropped tables' windows have passed: their spans are cleared, the
// ranges the reclaim split out are merged away, and the per-range and
// per-table state the node keeps in memory shrinks back with them.
// Before, the node's routing cache kept the descriptor and leader hint
// of every range the reclaim split out and the merge queue folded back,
// since the merge queue merges without going through it.
func TestDDLCycleReclaim(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 240*time.Second)
	defer cancel()
	const gcTTL = time.Second
	tc := StartWithOptions(t, 1, func(cfg *server.Config) {
		cfg.DroppedTableReclaimInterval = -1 // passes run by hand
		cfg.GCTTL = gcTTL
	})
	n := tc.Nodes[0]
	st := n.Store()
	conn, err := pgx.Connect(ctx, pgURL(tc, 0))
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close(context.Background())

	ranges := func() int {
		c := 0
		st.VisitReplicas(func(*kvserver.Replica) bool { c++; return true })
		return c
	}
	// settle runs the reclaim, GC and merge passes until the dropped
	// tables are reclaimed and the store is back to want ranges.
	settle := func(want int) {
		t.Helper()
		deadline := time.Now().Add(90 * time.Second)
		for {
			n.RunDroppedTableReclaimOnce(ctx)
			st.RunGCOnce(ctx, gcTTL)
			st.RunRangeMergeOnce(ctx)
			if ranges() <= want {
				return
			}
			if time.Now().After(deadline) {
				t.Fatalf("%d ranges left after the reclaim, want %d", ranges(), want)
			}
			time.Sleep(200 * time.Millisecond)
		}
	}

	// A first cycle warms what is created once (the routing entries of
	// the system ranges, the catalog's own descriptors) into the
	// baseline.
	const cycles, tables = 4, 20
	if err := ddlCycle(ctx, conn, "cyc0", tables); err != nil {
		t.Fatal(err)
	}
	time.Sleep(gcTTL + 6*time.Second) // the window and the reclaim's slack
	baseRanges := ranges()
	settle(baseRanges)
	base := n.TestingMemoryStats()

	for c := 1; c <= cycles; c++ {
		if err := ddlCycle(ctx, conn, fmt.Sprintf("cyc%d", c), tables); err != nil {
			t.Fatalf("cycle %d: %v", c, err)
		}
	}
	time.Sleep(gcTTL + 6*time.Second)
	settle(baseRanges)
	after := n.TestingMemoryStats()

	// node.tabledescs is the dashboard's name cache: it answers for
	// dropped ids too, bounded (tableDescCacheMax) and aged out by use.
	const slack = 2
	names := make([]string, 0, len(after))
	for k := range after {
		names = append(names, k)
	}
	sort.Strings(names)
	for _, k := range names {
		if k == "node.tabledescs" {
			continue
		}
		if after[k] > base[k]+slack {
			t.Errorf("%s: %d after %d cycles of %d tables, %d at the baseline", k, after[k], cycles, tables, base[k])
		}
	}
	if after["kv.rangecache.descs"] > ranges()+slack || after["kv.rangecache.hints"] > ranges()+slack {
		t.Errorf("routing cache holds %d descriptors and %d hints for %d ranges",
			after["kv.rangecache.descs"], after["kv.rangecache.hints"], ranges())
	}
	if t.Failed() {
		t.Logf("baseline %v\nafter    %v", base, after)
	}
}

// TestDroppedTableReclaimWaitsOutOverload (issue #391): the reclaim of a
// dropped table splits its span out, clears it and purges it, which
// forces a memtable flush and a compaction — small L0 files for an
// engine whose L0 is what is overloaded. While the store sheds writes,
// the reclaim waits; once it recovers, the reclaim goes ahead.
func TestDroppedTableReclaimWaitsOutOverload(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()
	var overloaded atomic.Bool
	const gcTTL = time.Second
	tc := StartWithOptions(t, 1, func(cfg *server.Config) {
		cfg.DroppedTableReclaimInterval = -1
		cfg.GCTTL = gcTTL
		cfg.TestingKnobs = kvserver.TestingKnobs{OverrideOverloaded: func() (bool, string) {
			if overloaded.Load() {
				return true, "test overload"
			}
			return false, ""
		}}
	})
	n := tc.Nodes[0]
	conn, err := pgx.Connect(ctx, pgURL(tc, 0))
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close(context.Background())
	if err := ddlCycle(ctx, conn, "ovl", 3); err != nil {
		t.Fatal(err)
	}
	time.Sleep(gcTTL + 6*time.Second)

	// Passes for longer than the gateways' descriptor leases (which also
	// hold a reclaim back) and the window: none reclaims anything.
	overloaded.Store(true)
	for until := time.Now().Add(catalog.DefaultDescLeaseTTL + 4*time.Second); time.Now().Before(until); {
		if got := n.RunDroppedTableReclaimOnce(ctx); got != 0 {
			t.Fatalf("reclaimed %d dropped table(s) while the store was overloaded", got)
		}
		time.Sleep(250 * time.Millisecond)
	}
	overloaded.Store(false)
	deadline := time.Now().Add(30 * time.Second)
	total := 0
	for total < 4 { // three tables and schema_migrations
		total += n.RunDroppedTableReclaimOnce(ctx)
		if time.Now().After(deadline) {
			t.Fatalf("reclaimed %d of 4 dropped tables after the overload cleared", total)
		}
	}
}
