package testcluster

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"

	"github.com/sthorne/datax/pkg/metrics"
	"github.com/sthorne/datax/pkg/sql"
	"github.com/sthorne/datax/pkg/sql/catalog"
	"github.com/sthorne/datax/pkg/sql/types"
	"github.com/sthorne/datax/pkg/version"
)

// TestFanOutMergeEarlyStop: ORDER BY <ts> DESC LIMIT n on a sharded
// timeseries table pushes the limit into each bucket's (reverse) scan —
// the dashboard query reads at most buckets*n rows instead of the whole
// table — and falls back to the in-memory sort when the version gate is
// closed. Issue #53 (TS4).
func TestFanOutMergeEarlyStop(t *testing.T) {
	tc := Start(t, 1)
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()
	s := sql.NewSession(tc.Nodes[0].DB(), catalog.NewAccessor())

	execSQL(t, ctx, s, `CREATE TABLE mm (series TEXT, at TIMESTAMPTZ, v INT8, PRIMARY KEY (series, at))
		WITH (timeseries = true, shards = 8)`)
	base := time.Date(2026, 8, 30, 0, 0, 0, 0, time.UTC).UnixNano()
	for i := 0; i < 400; i++ {
		execSQL(t, ctx, s, `INSERT INTO mm VALUES ('cpu', $1, $2)`,
			types.NewTimestamp(base+int64(i)*int64(time.Second)), types.NewInt(int64(i)))
	}

	q := `SELECT at, v FROM mm WHERE series = 'cpu' ORDER BY at DESC LIMIT 10`
	p := explainPlan(t, ctx, s, q)
	if !strings.Contains(p, "K-way merge across shard buckets (reverse scans)") ||
		!strings.Contains(p, "limit pushed into scan") {
		t.Fatalf("desc-limit plan: %q", p)
	}

	before := testutil.ToFloat64(metrics.SQLRowsScanned)
	res := execSQL(t, ctx, s, q)
	scanned := testutil.ToFloat64(metrics.SQLRowsScanned) - before
	if len(res.Rows) != 10 {
		t.Fatalf("rows: %d", len(res.Rows))
	}
	for i := 0; i < 10; i++ {
		if res.Rows[i][1].I != int64(399-i) {
			t.Fatalf("row %d: %+v", i, res.Rows[i])
		}
	}
	// Early stop: each of the 8 buckets scans at most LIMIT rows.
	if scanned > 8*10 {
		t.Fatalf("scanned %v rows, want <= 80 (early stop)", scanned)
	}

	// Ascending early stop too.
	aq := `SELECT v FROM mm WHERE series = 'cpu' ORDER BY at LIMIT 5`
	before = testutil.ToFloat64(metrics.SQLRowsScanned)
	res = execSQL(t, ctx, s, aq)
	scanned = testutil.ToFloat64(metrics.SQLRowsScanned) - before
	if len(res.Rows) != 5 || res.Rows[0][0].I != 0 || res.Rows[4][0].I != 4 {
		t.Fatalf("asc rows: %+v", res.Rows)
	}
	if scanned > 8*5 {
		t.Fatalf("asc scanned %v rows, want <= 40", scanned)
	}

	// Gate closed (a not-yet-finalized v3 upgrade): descending falls back
	// to the in-memory sort — correct, just unpushed.
	tc.Nodes[0].DB().SetVersionGate(func() version.Version { return version.V2 })
	p = explainPlan(t, ctx, s, q)
	if !strings.Contains(p, "in-memory sort") || strings.Contains(p, "reverse") {
		t.Fatalf("gated plan: %q", p)
	}
	res = execSQL(t, ctx, s, q)
	if len(res.Rows) != 10 || res.Rows[0][1].I != 399 {
		t.Fatalf("gated rows: %+v", res.Rows)
	}
	tc.Nodes[0].DB().SetVersionGate(func() version.Version { return version.V3 })

	// DESC on a plain (unsharded) table rides a single reverse scan with
	// the limit pushed.
	execSQL(t, ctx, s, `CREATE TABLE plainpk (id INT8 PRIMARY KEY, v INT8)`)
	for i := 0; i < 100; i++ {
		execSQL(t, ctx, s, `INSERT INTO plainpk VALUES ($1, $2)`, types.NewInt(int64(i)), types.NewInt(int64(i)))
	}
	pq := `SELECT id FROM plainpk ORDER BY id DESC LIMIT 3`
	p = explainPlan(t, ctx, s, pq)
	if !strings.Contains(p, "order satisfied by access path (reverse scan)") ||
		!strings.Contains(p, "limit pushed into scan") {
		t.Fatalf("plain desc plan: %q", p)
	}
	before = testutil.ToFloat64(metrics.SQLRowsScanned)
	res = execSQL(t, ctx, s, pq)
	scanned = testutil.ToFloat64(metrics.SQLRowsScanned) - before
	if len(res.Rows) != 3 || res.Rows[0][0].I != 99 || res.Rows[2][0].I != 97 {
		t.Fatalf("plain desc rows: %+v", res.Rows)
	}
	if scanned != 3 {
		t.Fatalf("plain desc scanned %v rows, want 3", scanned)
	}
}

// benchDescLimit measures the dashboard query — ORDER BY at DESC LIMIT
// 10 over a sharded table — with the version gate open (K-way merge of
// per-bucket reverse scans, early stop) or closed (fetch everything,
// in-memory sort): the same binary, the same data, only the gate differs.
func benchDescLimit(b *testing.B, gated version.Version) {
	tc := Start(b, 1)
	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Second)
	defer cancel()
	s := sql.NewSession(tc.Nodes[0].DB(), catalog.NewAccessor())
	if _, serr := trySQL(ctx, s, `CREATE TABLE bm (series TEXT, at TIMESTAMPTZ, v INT8, PRIMARY KEY (series, at))
		WITH (timeseries = true, shards = 8)`); serr != nil {
		b.Fatalf("create: %v", serr)
	}
	base := time.Date(2026, 8, 30, 0, 0, 0, 0, time.UTC).UnixNano()
	for i := 0; i < 5000; i++ {
		if _, serr := trySQL(ctx, s, `INSERT INTO bm VALUES ('cpu', $1, $2)`,
			types.NewTimestamp(base+int64(i)*int64(time.Second)), types.NewInt(int64(i))); serr != nil {
			b.Fatalf("insert: %v", serr)
		}
	}
	tc.Nodes[0].DB().SetVersionGate(func() version.Version { return gated })
	q := `SELECT at, v FROM bm WHERE series = 'cpu' ORDER BY at DESC LIMIT 10`
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		res, serr := trySQL(ctx, s, q)
		if serr != nil {
			b.Fatal(serr)
		}
		if len(res.Rows) != 10 {
			b.Fatalf("rows: %d", len(res.Rows))
		}
	}
}

func BenchmarkDescLimitMerge(b *testing.B)        { benchDescLimit(b, version.V3) }
func BenchmarkDescLimitInMemorySort(b *testing.B) { benchDescLimit(b, version.V2) }
