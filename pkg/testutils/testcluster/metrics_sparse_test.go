package testcluster

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/sthorne/datax/pkg/server"
	"github.com/sthorne/datax/pkg/sql"
	"github.com/sthorne/datax/pkg/sql/catalog"
	"github.com/sthorne/datax/pkg/sql/types"
)

// TestSparseMetricsReadAsBefore (#316): the recorder writes the per-table
// and per-index series only when they change (and on a keyframe), and
// /api/metrics still serves an idle table's series as it did when every
// tick wrote them — its value at every tick, not a gap — serves a busy
// one's changes, and ends a dropped table's series where it ended.
func TestSparseMetricsReadAsBefore(t *testing.T) {
	tc := startMetricsCluster(t, 1, func(c *server.Config) {
		c.MetricsRecordInterval = -1 // ticks run by hand, stamped in the past
		c.StatsRefreshInterval = -1
	})
	ctx, cancel := context.WithTimeout(context.Background(), 180*time.Second)
	defer cancel()
	n := tc.Nodes[0]
	s := sql.NewSession(n.DB(), catalog.NewAccessor())
	waitForDatabases(t, ctx, s)
	for _, q := range []string{
		`CREATE TABLE idle_t (id INT8 PRIMARY KEY)`,
		`CREATE TABLE busy_t (id INT8 PRIMARY KEY)`,
		`CREATE TABLE gone_t (id INT8 PRIMARY KEY)`,
		`INSERT INTO idle_t VALUES (1)`,
		`SELECT * FROM idle_t`,
		`SELECT * FROM busy_t`,
		`SELECT * FROM gone_t`,
	} {
		execSQL(t, ctx, s, q)
	}

	const interval = 10 * time.Second // the reader's default
	ticks := 130                      // over two keyframe periods
	if raceEnabled {
		ticks = 75
	}
	base := time.Now().Add(-time.Duration(ticks+10) * interval).Truncate(interval)
	stamp := func(i int) time.Time { return base.Add(time.Duration(i) * interval) }
	count := func(name string) int64 {
		r := execSQL(t, ctx, s, `SELECT count(*) FROM datax_metrics WHERE name = $1`, types.NewString(name))
		return r.Rows[0][0].I
	}
	idleRanges := `table.ranges{table=idle_t}`
	idleScans := `index.scans{index=idle_t.primary}`
	busyScans := `index.scans{index=busy_t.primary}`
	goneRanges := `table.ranges{table=gone_t}`

	// The table gauges are read in the background: the first ticks may
	// not carry them yet. Start the series once they do.
	first := 0
	for {
		if first > 40 {
			t.Fatal("the table gauges never reached the recorder")
		}
		if n.RecordMetricsAt(ctx, interval, stamp(first)) < 0 {
			t.Fatalf("tick %d wrote nothing", first)
		}
		first++
		if count(idleRanges) > 0 {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	first-- // the first tick that carried them
	dropAt := first + ticks/2
	for i := first + 1; i < ticks; i++ {
		if i%7 == 0 {
			execSQL(t, ctx, s, `SELECT * FROM busy_t`)
		}
		if i == dropAt {
			execSQL(t, ctx, s, `DROP TABLE gone_t`)
			n.TestingRefreshTableGauges()
		}
		if n.RecordMetricsAt(ctx, interval, stamp(i)) < 0 {
			t.Fatalf("tick %d wrote nothing", i)
		}
	}
	r := execSQL(t, ctx, s, `SELECT count(*) FROM datax_metrics WHERE name = $1 AND value IS NULL AND at = $2`,
		types.NewString(goneRanges), types.NewTimestamp(stamp(dropAt).UnixNano()))
	if r.Rows[0][0].I != 1 {
		t.Fatalf("the dropped table's series has no NULL row at the tick after the drop")
	}
	recorded := ticks - first
	if got := count(idleRanges); got*5 > int64(recorded) {
		t.Errorf("the idle table's ranges were written %d times in %d ticks: not sparse", got, recorded)
	}
	if got := count(idleScans); got*5 > int64(recorded) {
		t.Errorf("the idle table's index was written %d times in %d ticks: not sparse", got, recorded)
	}

	q := fmt.Sprintf("series=%s,%s,%s,%s&from=%d&to=%d", idleRanges, idleScans, busyScans, goneRanges,
		stamp(first).UnixMilli(), stamp(ticks).UnixMilli())
	code, doc := metricsQuery(t, tc, 0, q)
	if code != 200 || len(doc.Series) != 4 {
		t.Fatalf("query: %d %+v", code, doc)
	}
	nodeOf := map[string]string{idleRanges: "0", goneRanges: "0", idleScans: "1", busyScans: "1"}
	for _, sr := range doc.Series {
		pts := sr.Nodes[nodeOf[sr.Name]]
		switch sr.Name {
		case idleRanges, idleScans:
			// What a recorder writing every tick gave: a point at every
			// tick, the same value throughout.
			if len(pts) != recorded {
				t.Errorf("%s: %d points for %d ticks", sr.Name, len(pts), recorded)
				continue
			}
			for i, p := range pts {
				if int64(p[0]) != stamp(first+i).UnixMilli() || p[1] != pts[0][1] {
					t.Errorf("%s: point %d is %v, want [%d %v]", sr.Name, i, p, stamp(first+i).UnixMilli(), pts[0][1])
					break
				}
			}
		case busyScans:
			if len(pts) != recorded {
				t.Errorf("%s: %d points for %d ticks", sr.Name, len(pts), recorded)
				continue
			}
			if pts[len(pts)-1][1] <= pts[0][1] {
				t.Errorf("%s: no rise from %v to %v", sr.Name, pts[0], pts[len(pts)-1])
			}
			for i := 1; i < len(pts); i++ {
				if pts[i][1] < pts[i-1][1] {
					t.Errorf("%s: fell at point %d: %v", sr.Name, i, pts)
					break
				}
			}
		case goneRanges:
			// Every tick before the drop, none from it on.
			if len(pts) != dropAt-first || int64(pts[len(pts)-1][0]) != stamp(dropAt-1).UnixMilli() {
				t.Errorf("%s: %d points, last %v; want %d, the last at tick %d", sr.Name, len(pts), pts[len(pts)-1], dropAt-first, dropAt-1-first)
			}
		}
	}
}
