package testcluster

import (
	"context"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"

	"github.com/sthorne/datax/pkg/keys"
	"github.com/sthorne/datax/pkg/metrics"
	"github.com/sthorne/datax/pkg/server"
	"github.com/sthorne/datax/pkg/sql"
	"github.com/sthorne/datax/pkg/sql/catalog"
)

// TestBackgroundStatsSampler: the paced sampler populates statistics for
// every table without ANALYZE — one table per tick, on the range-1 leader
// only — respects the staleness threshold once collected, and deletes
// orphaned statistics blobs. Issue #56 (SA2).
func TestBackgroundStatsSampler(t *testing.T) {
	tc, _ := StartWithEngines(t, 3, func(c *server.Config) {
		c.StatsRefreshInterval = 100 * time.Millisecond
		c.StatsStaleness = time.Hour // collect once, then rest
	})
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	s := sql.NewSession(tc.Nodes[0].DB(), catalog.NewAccessor())
	for _, q := range []string{
		`CREATE TABLE s1 (id INT8 PRIMARY KEY, v TEXT)`,
		`CREATE TABLE s2 (id INT8 PRIMARY KEY, v TEXT)`,
		`CREATE TABLE s3 (id INT8 PRIMARY KEY, v TEXT)`,
		`INSERT INTO s1 VALUES (1, 'a'), (2, 'b')`,
		`INSERT INTO s2 VALUES (1, 'a')`,
	} {
		execSQL(t, ctx, s, q)
	}
	// A fast-ticking sampler may have collected a table between its
	// CREATE and the inserts above — and the 1h staleness would freeze
	// that empty count for the whole test. Drop anything collected so
	// far: missing statistics are maximally stale, so the sampler
	// re-collects post-insert state on its next ticks.
	{
		lo, hi := keys.TableStatsSpan()
		kvs, err := tc.Nodes[0].DB().Scan(ctx, lo, hi, 0)
		if err != nil {
			t.Fatalf("pre-seed stats scan: %v", err)
		}
		for _, kv := range kvs {
			if err := tc.Nodes[0].DB().Delete(ctx, kv.Key); err != nil {
				t.Fatalf("pre-seed stats delete: %v", err)
			}
		}
	}
	// Plant an orphan stats blob under an ID no table owns.
	orphan := keys.TableStatsKey(999999)
	if err := tc.Nodes[0].DB().Put(ctx, orphan, []byte(`{"table_id":999999,"row_count":1,"collected_at":1}`)); err != nil {
		t.Fatalf("plant orphan: %v", err)
	}

	// The sampler (one table per tick) covers all three tables and reaps
	// the orphan. System tables don't exist, so exactly 3 collections.
	statsFor := func(table string) *sql.Result {
		t.Helper()
		res, serr := trySQL(ctx, s, "SHOW STATS FOR "+table)
		if serr != nil {
			t.Fatalf("show stats %s: %v", table, serr)
		}
		return res
	}
	// Wait until the sampler has collected CORRECT counts for all three
	// tables (a tick racing the seed inserts can capture a pre-insert
	// count that the 1h staleness would then freeze — deleting that blob
	// makes the table maximally stale again, so the sampler self-heals).
	wantRows := map[string]int64{"s1": 2, "s2": 1, "s3": 0}
	deadline := time.Now().Add(30 * time.Second)
	for {
		done := true
		for tab, want := range wantRows {
			r := statsFor(tab).Rows
			if len(r) == 0 {
				done = false
				continue
			}
			if r[0][1].I != want {
				done = false
				// Stale pre-insert collection: drop it so the sampler
				// re-collects.
				id := tableID(t, ctx, tc.Nodes[0], tab)
				if err := tc.Nodes[0].DB().Delete(ctx, keys.TableStatsKey(id)); err != nil {
					t.Fatalf("drop stale stats for %s: %v", tab, err)
				}
			}
		}
		raw, err := tc.Nodes[0].DB().Get(ctx, orphan)
		if err != nil {
			t.Fatalf("orphan get: %v", err)
		}
		if done && raw == nil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("sampler incomplete: done=%v orphan=%v", done, raw != nil)
		}
		time.Sleep(50 * time.Millisecond)
	}

	// Staleness respected: with fresh stats and a 1h threshold, several
	// more ticks must not re-collect.
	before := testutil.ToFloat64(metrics.StatsRefreshes)
	time.Sleep(600 * time.Millisecond)
	if after := testutil.ToFloat64(metrics.StatsRefreshes); after != before {
		t.Fatalf("sampler re-collected fresh stats (%v -> %v)", before, after)
	}
}
