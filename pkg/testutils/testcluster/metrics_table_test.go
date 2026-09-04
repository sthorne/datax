package testcluster

import (
	"context"
	"fmt"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/sthorne/datax/pkg/server"
	"github.com/sthorne/datax/pkg/sql"
	"github.com/sthorne/datax/pkg/sql/catalog"
	"github.com/sthorne/datax/pkg/sql/types"
	"github.com/sthorne/datax/pkg/version"
)

// startMetricsCluster: HTTP on every node, the recorder ticking every
// second, plus any extra config hook.
func startMetricsCluster(t *testing.T, numNodes int, extra func(*server.Config)) *TestCluster {
	t.Helper()
	listeners := make([]net.Listener, numNodes)
	for i := range listeners {
		lis, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		listeners[i] = lis
	}
	i := 0
	tc, _ := StartWithEngines(t, numNodes, func(c *server.Config) {
		c.HTTPListener = listeners[i]
		c.MetricsRecordInterval = 2 * time.Second
		if extra != nil {
			extra(c)
		}
		i++
	})
	return tc
}

func metricsQuery(t *testing.T, tc *TestCluster, i int, query string) (int, server.MetricsResult) {
	t.Helper()
	code, _, body := httpGet(t, "http://"+tc.Nodes[i].HTTPAddr()+"/api/metrics?"+query)
	var doc server.MetricsResult
	if code == 200 {
		if err := jsonUnmarshal([]byte(body), &doc); err != nil {
			t.Fatalf("%s: %v: %s", query, err, body)
		}
	}
	return code, doc
}

// TestMetricsTable (issue #115): every node records its series into the
// datax_metrics table at aligned timestamps; /api/metrics lists the
// catalog and serves aligned, downsampled arrays per node, differentiates
// counters, refuses unknown series; the table is reserved (create, drop
// and column DDL refused; retention and shards settable; non-admins
// read with a grant and never write); backups exclude it unless asked;
// a live re-shard does not interrupt the recorder.
func TestMetricsTable(t *testing.T) {
	tc := startMetricsCluster(t, 3, nil)
	ctx, cancel := context.WithTimeout(context.Background(), 180*time.Second)
	defer cancel()
	root := sql.NewSession(tc.Nodes[0].DB(), catalog.NewAccessor())
	execSQL(t, ctx, root, `CREATE TABLE t1 (series TEXT, at TIMESTAMPTZ, v FLOAT8, PRIMARY KEY (series, at)) WITH (timeseries = true, retention = '1d', shards = 2)`)

	// Catalog: ready once the table exists, the series list, the peers.
	var cat server.MetricsCatalog
	deadline := time.Now().Add(40 * time.Second)
	for {
		code, _, body := httpGet(t, "http://"+tc.Nodes[0].HTTPAddr()+"/api/metrics")
		if code != 200 {
			t.Fatalf("/api/metrics: %d %s", code, body)
		}
		if err := jsonUnmarshal([]byte(body), &cat); err != nil {
			t.Fatal(err)
		}
		if cat.Ready {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("recorder never created the table: %s", body)
		}
		time.Sleep(300 * time.Millisecond)
	}
	if !cat.Enabled || cat.IntervalSeconds != 2 || len(cat.Series) < 30 || cat.Table != "datax_metrics" {
		t.Fatalf("catalog: %+v", cat)
	}
	if len(cat.Nodes) != 3 || len(cat.Labels["peer"]) != 2 {
		t.Fatalf("catalog nodes %v peers %v", cat.Nodes, cat.Labels["peer"])
	}

	// Every node's samples land, aligned to the interval, within a few
	// ticks; the cluster series come from range 1's leader as node 0.
	deadline = time.Now().Add(40 * time.Second)
	for {
		code, doc := metricsQuery(t, tc, 1, "series=node.ranges,go.goroutines,table.ranges{table=t1}&since=2m")
		if code != 200 {
			t.Fatalf("query: %d", code)
		}
		ok := len(doc.Series) == 3
		for _, s := range doc.Series[:2] {
			for _, id := range []string{"1", "2", "3"} {
				if len(s.Nodes[id]) < 2 {
					ok = false
				}
				for _, p := range s.Nodes[id] {
					if int64(p[0])%2000 != 0 {
						t.Fatalf("%s n%s: point at %v is not aligned to the interval", s.Name, id, p[0])
					}
					if s.Name == "node.ranges" && p[1] < 1 {
						t.Fatalf("n%s holds %v ranges", id, p[1])
					}
				}
			}
		}
		if len(doc.Series) == 3 && len(doc.Series[2].Nodes["0"]) == 0 {
			ok = false
		}
		if ok {
			if doc.StepMs != 2000 {
				t.Fatalf("step %d ms for a 2m window at a 2s interval", doc.StepMs)
			}
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("samples never arrived: %+v", doc)
		}
		time.Sleep(500 * time.Millisecond)
	}

	// Warm-up is over once every node's samples have arrived; from here
	// on every tick's write must succeed (the counter is process-wide, so
	// this covers all three nodes).
	errorsAfterWarmup := recorderErrors(t, tc)

	// Plain SQL reads it.
	res := execSQL(t, ctx, root, `SELECT at, value FROM datax_metrics WHERE node = 1 AND name = 'node.leaders' ORDER BY at`)
	if len(res.Rows) == 0 {
		t.Fatal("no node.leaders rows for n1 via SQL")
	}
	for i := 1; i < len(res.Rows); i++ {
		if res.Rows[i][0].I <= res.Rows[i-1][0].I {
			t.Fatal("rows not in time order")
		}
	}

	// Counters differentiate into rates (the recorder's own commits keep
	// txn.commits moving); a counter without rate is the raw cumulative.
	if code, doc := metricsQuery(t, tc, 0, "series=txn.commits&node=1&since=2m&rate=1"); code != 200 || !doc.Series[0].Rate || len(doc.Series[0].Nodes["1"]) == 0 {
		t.Fatalf("rate query: %d %+v", code, doc)
	} else {
		for _, p := range doc.Series[0].Nodes["1"] {
			if p[1] < 0 {
				t.Fatalf("negative rate %v", p)
			}
		}
	}
	if code, doc := metricsQuery(t, tc, 0, "series=txn.commits&node=1&since=2m"); code != 200 || doc.Series[0].Rate {
		t.Fatalf("cumulative query: %d %+v", code, doc)
	}

	// Validation.
	if code, _ := metricsQuery(t, tc, 0, "series=node.nope&since=1m"); code != 404 {
		t.Fatalf("unknown series: %d, want 404", code)
	}
	if code, _ := metricsQuery(t, tc, 0, "series=node.ranges&since=yesterday"); code != 400 {
		t.Fatalf("bad since: %d, want 400", code)
	}

	// Downsampling: two hours of 10-second samples for a synthetic node
	// fold into at most 500 points, and a coarse step averages them.
	now := time.Now().Truncate(time.Hour)
	var b strings.Builder
	var params []types.Datum
	b.WriteString(`INSERT INTO datax_metrics (node, name, at, value) VALUES `)
	for i := 0; i < 720; i++ {
		if i > 0 {
			b.WriteString(", ")
		}
		fmt.Fprintf(&b, "(9, 'node.cpu_percent', $%d, $%d)", 2*i+1, 2*i+2)
		params = append(params, types.NewTimestamp(now.Add(-2*time.Hour+time.Duration(i)*10*time.Second).UnixNano()), types.NewFloat(float64(i%10)))
	}
	execSQL(t, ctx, root, b.String(), params...)
	code, doc := metricsQuery(t, tc, 2, fmt.Sprintf("series=node.cpu_percent&node=9&from=%d&to=%d", now.Add(-2*time.Hour).UnixMilli(), now.UnixMilli()))
	if code != 200 || len(doc.Series[0].Nodes["9"]) > 500 || len(doc.Series[0].Nodes["9"]) < 400 {
		t.Fatalf("downsample: %d, %d points", code, len(doc.Series[0].Nodes["9"]))
	}
	code, doc = metricsQuery(t, tc, 2, fmt.Sprintf("series=node.cpu_percent&node=9&from=%d&to=%d&step=1h", now.Add(-2*time.Hour).UnixMilli(), now.UnixMilli()))
	if code != 200 || len(doc.Series[0].Nodes["9"]) != 2 {
		t.Fatalf("hourly step: %d, %+v", code, doc.Series[0].Nodes["9"])
	}
	for _, p := range doc.Series[0].Nodes["9"] {
		if p[1] < 4.4 || p[1] > 4.6 {
			t.Fatalf("hourly average %v, want 4.5", p[1])
		}
	}

	// Reserved: the cluster owns the table.
	for _, stmt := range []string{
		`CREATE TABLE datax_metrics (id INT8 PRIMARY KEY)`,
		`DROP TABLE datax_metrics`,
		`ALTER TABLE datax_metrics ADD COLUMN x INT8`,
		`ALTER TABLE datax_metrics DROP COLUMN value`,
	} {
		if _, serr := trySQL(ctx, root, stmt); serr == nil || serr.Code != sql.CodeInsufficientPriv {
			t.Fatalf("%s: got %v, want 42501", stmt, serr)
		}
	}
	execSQL(t, ctx, root, `ALTER TABLE datax_metrics SET (retention = '2d')`)
	// The schema document is cached for a few seconds; poll for the change.
	deadline = time.Now().Add(20 * time.Second)
	for {
		_, _, body := httpGet(t, "http://"+tc.Nodes[0].HTTPAddr()+"/api/schema")
		var sd server.SchemaStatus
		if err := jsonUnmarshal([]byte(body), &sd); err != nil {
			t.Fatal(err)
		}
		var got *server.SchemaTable
		for i := range sd.Tables {
			if sd.Tables[i].Name == "datax_metrics" {
				got = &sd.Tables[i]
			}
		}
		if got == nil {
			t.Fatal("datax_metrics missing from /api/schema")
		}
		if got.RetentionSeconds == 2*86400 && got.Shards == 8 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("retention/shards after ALTER: %+v", *got)
		}
		time.Sleep(300 * time.Millisecond)
	}

	// A reporting user reads with a grant and never writes.
	execSQL(t, ctx, root, `CREATE USER reader PASSWORD 'pw'`)
	execSQL(t, ctx, root, `GRANT SELECT ON datax_metrics TO reader`)
	reader := sql.NewSessionForUser(tc.Nodes[0].DB(), catalog.NewAccessor(), "reader")
	if res := execSQL(t, ctx, reader, `SELECT at, value FROM datax_metrics WHERE node = 9 AND name = 'node.cpu_percent' ORDER BY at LIMIT 3`); len(res.Rows) != 3 {
		t.Fatalf("reader SELECT: %d rows", len(res.Rows))
	}
	execSQL(t, ctx, root, `GRANT INSERT ON datax_metrics TO reader`)
	if _, serr := trySQL(ctx, reader, `INSERT INTO datax_metrics VALUES (9, 'x', $1, 1)`, types.NewTimestamp(now.UnixNano())); serr == nil || serr.Code != sql.CodeInsufficientPriv {
		t.Fatalf("reader INSERT: got %v, want 42501", serr)
	}
	if _, serr := trySQL(ctx, reader, `DELETE FROM datax_metrics WHERE node = 9`); serr == nil || serr.Code != sql.CodeInsufficientPriv {
		t.Fatalf("reader DELETE: got %v, want 42501", serr)
	}

	// Backups exclude the table unless asked.
	sum, err := tc.Nodes[0].RunBackup(ctx, t.TempDir(), "", false, false)
	if err != nil {
		t.Fatal(err)
	}
	for _, bt := range sum.Tables {
		if bt.Name == "datax_metrics" {
			t.Fatal("default backup carried datax_metrics")
		}
	}
	sum, err = tc.Nodes[0].RunBackup(ctx, t.TempDir(), "", false, true)
	if err != nil {
		t.Fatal(err)
	}
	found := false
	for _, bt := range sum.Tables {
		if bt.Name == "datax_metrics" {
			found = true
		}
	}
	if !found {
		t.Fatal("--include-metrics backup lacks datax_metrics")
	}

	if e := recorderErrors(t, tc); e != errorsAfterWarmup {
		t.Fatalf("recorder write errors in steady state: %s -> %s", errorsAfterWarmup, e)
	}

	// A live re-shard while the recorder keeps writing: rows keep arriving
	// under the new layout (a tick's write that loses to the backfill is
	// retried next tick, so the error counter may move during the window).
	before := len(execSQL(t, ctx, root, `SELECT at FROM datax_metrics WHERE node = 1 AND name = 'node.ranges' ORDER BY at`).Rows)
	execSQL(t, ctx, root, `ALTER TABLE datax_metrics SET (shards = 16)`)
	deadline = time.Now().Add(30 * time.Second)
	for {
		after := len(execSQL(t, ctx, root, `SELECT at FROM datax_metrics WHERE node = 1 AND name = 'node.ranges' ORDER BY at`).Rows)
		if after >= before+3 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("recorder stalled across the re-shard: %d -> %d rows", before, after)
		}
		time.Sleep(500 * time.Millisecond)
	}
}

// TestMetricsTableVersionGate: a cluster that has not finalized v5 never
// creates the table (a v4 node would treat it as a user table), and the
// endpoint says so.
func TestMetricsTableVersionGate(t *testing.T) {
	tc := startMetricsCluster(t, 1, func(c *server.Config) { c.BinaryVersionOverride = version.V4 })
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	time.Sleep(4 * time.Second)
	root := sql.NewSession(tc.Nodes[0].DB(), catalog.NewAccessor())
	if _, serr := trySQL(ctx, root, `SELECT at FROM datax_metrics WHERE node = 1 AND name = 'node.ranges'`); serr == nil {
		t.Fatal("datax_metrics exists before v5 finalized")
	}
	code, _, body := httpGet(t, "http://"+tc.Nodes[0].HTTPAddr()+"/api/metrics?series=node.ranges&since=1m")
	if code != 503 {
		t.Fatalf("query before v5: %d %s", code, body)
	}
	if _, _, body := httpGet(t, "http://"+tc.Nodes[0].HTTPAddr()+"/api/metrics"); !strings.Contains(body, `"ready": false`) {
		t.Fatalf("catalog before v5: %s", body)
	}
}

// recorderErrors reads datax_metrics_record_errors_total from /metrics.
func recorderErrors(t *testing.T, tc *TestCluster) string {
	t.Helper()
	_, _, body := httpGet(t, "http://"+tc.Nodes[0].HTTPAddr()+"/metrics")
	for _, l := range strings.Split(body, "\n") {
		if strings.HasPrefix(l, "datax_metrics_record_errors_total ") {
			return strings.TrimPrefix(l, "datax_metrics_record_errors_total ")
		}
	}
	t.Fatalf("/metrics lacks datax_metrics_record_errors_total:\n%s", grepLines(body, "datax_metrics_record"))
	return ""
}
