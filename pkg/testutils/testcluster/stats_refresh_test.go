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
	deadline := time.Now().Add(30 * time.Second)
	for {
		done := true
		for _, tab := range []string{"s1", "s2", "s3"} {
			if len(statsFor(tab).Rows) == 0 {
				done = false
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

	// Row counts arrived through the sampler, not ANALYZE.
	if r := statsFor("s1").Rows; r[0][1].I != 2 {
		t.Fatalf("s1 row count: %+v", r[0])
	}
	if r := statsFor("s3").Rows; r[0][1].I != 0 {
		t.Fatalf("s3 row count: %+v", r[0])
	}

	// Staleness respected: with fresh stats and a 1h threshold, several
	// more ticks must not re-collect.
	before := testutil.ToFloat64(metrics.StatsRefreshes)
	time.Sleep(600 * time.Millisecond)
	if after := testutil.ToFloat64(metrics.StatsRefreshes); after != before {
		t.Fatalf("sampler re-collected fresh stats (%v -> %v)", before, after)
	}
}
