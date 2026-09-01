package testcluster

import (
	"context"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"

	"github.com/sthorne/datax/pkg/keys"
	"github.com/sthorne/datax/pkg/kvserver"
	"github.com/sthorne/datax/pkg/metrics"
	"github.com/sthorne/datax/pkg/sql"
	"github.com/sthorne/datax/pkg/sql/catalog"
	"github.com/sthorne/datax/pkg/sql/types"
)

// TestRowLevelRetentionOnMixedRange: a range deliberately holding both a
// retention table and foreign data never expires whole-range, but rows
// whose timestamp column AND write age are past the retention still age
// out individually — foreign rows and young rows survive, and the GC
// threshold keeps the conservative default. Issue #53 (TS5).
func TestRowLevelRetentionOnMixedRange(t *testing.T) {
	n, _ := startGCNode(t)
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()
	s := sql.NewSession(n.DB(), catalog.NewAccessor())

	execSQL(t, ctx, s, `CREATE TABLE keeper (id INT8 PRIMARY KEY, v TEXT)`)
	for i := 0; i < 5; i++ {
		execSQL(t, ctx, s, `INSERT INTO keeper VALUES ($1, 'k')`, types.NewInt(int64(i)))
	}
	execSQL(t, ctx, s, `CREATE TABLE ev (id INT8, at TIMESTAMPTZ, PRIMARY KEY (id, at))
		WITH (timeseries = true, retention = '1s')`)

	// Merge every range back into one, so ev's rows share a range with
	// keeper's (and the system keyspace) — the mixed shape retention GC
	// could never expire before.
	for merged := true; merged; {
		merged = false
		var starts []keys.Key
		n.Store().VisitReplicas(func(r *kvserver.Replica) bool {
			starts = append(starts, r.Desc().StartKey.Clone())
			return true
		})
		// AdminMerge makes the range containing the key absorb its RIGHT
		// neighbor; sweep every range until only one remains.
		for _, b := range starts {
			if _, err := n.DB().AdminMerge(ctx, b); err == nil {
				merged = true
			}
		}
	}
	ranges := 0
	n.Store().VisitReplicas(func(*kvserver.Replica) bool { ranges++; return true })
	if ranges != 1 {
		t.Fatalf("still %d ranges; the test needs one mixed range", ranges)
	}

	// 5 old rows (timestamp column far in the past) and 3 young ones
	// (timestamp in the future — never past retention).
	old := time.Date(2026, 8, 30, 0, 0, 0, 0, time.UTC).UnixNano()
	young := time.Now().Add(time.Hour).UnixNano()
	for i := 0; i < 5; i++ {
		execSQL(t, ctx, s, `INSERT INTO ev VALUES ($1, $2)`,
			types.NewInt(int64(i)), types.NewTimestamp(old+int64(i)*int64(time.Second)))
	}
	for i := 0; i < 3; i++ {
		execSQL(t, ctx, s, `INSERT INTO ev VALUES ($1, $2)`,
			types.NewInt(int64(100+i)), types.NewTimestamp(young+int64(i)*int64(time.Second)))
	}

	// Age the writes past the 1s retention, then run one GC pass at the
	// conservative default TTL (24h): whole-range rules delete nothing,
	// the row predicate deletes exactly the old ev rows.
	time.Sleep(1200 * time.Millisecond)
	before := testutil.ToFloat64(metrics.RetentionRowsExpired)
	n.Store().RunGCOnce(ctx, 24*time.Hour)
	expired := testutil.ToFloat64(metrics.RetentionRowsExpired) - before

	res := execSQL(t, ctx, s, `SELECT COUNT(*) AS n FROM ev`)
	if res.Rows[0][0].I != 3 {
		t.Fatalf("ev rows after GC: %+v", res.Rows)
	}
	res = execSQL(t, ctx, s, `SELECT COUNT(*) AS n FROM keeper`)
	if res.Rows[0][0].I != 5 {
		t.Fatalf("keeper rows after GC: %+v", res.Rows)
	}
	if expired < 5 {
		t.Fatalf("datax_retention_rows_expired_total advanced by %v, want >= 5", expired)
	}

	// Repeat passes are stable (no re-counting, nothing else expires).
	before = testutil.ToFloat64(metrics.RetentionRowsExpired)
	n.Store().RunGCOnce(ctx, 24*time.Hour)
	if d := testutil.ToFloat64(metrics.RetentionRowsExpired) - before; d != 0 {
		t.Fatalf("second pass expired %v more versions", d)
	}
	res = execSQL(t, ctx, s, `SELECT COUNT(*) AS n FROM ev`)
	if res.Rows[0][0].I != 3 {
		t.Fatalf("ev rows after second GC: %+v", res.Rows)
	}
}
