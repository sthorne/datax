package testcluster

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/sthorne/datax/pkg/server"
	"github.com/sthorne/datax/pkg/sql"
	"github.com/sthorne/datax/pkg/sql/catalog"
	"github.com/sthorne/datax/pkg/version"
)

// shapeFingerprint finds, in /api/statements on node i, the fingerprint
// of the shape whose text contains needle.
func shapeFingerprint(t *testing.T, tc *TestCluster, i int, needle string) (string, string) {
	t.Helper()
	code, _, body := httpGet(t, "http://"+tc.Nodes[i].HTTPAddr()+"/api/statements")
	if code != 200 {
		t.Fatalf("/api/statements: %d %s", code, body)
	}
	var doc server.StatementsStatus
	if err := json.Unmarshal([]byte(body), &doc); err != nil {
		t.Fatal(err)
	}
	for _, s := range doc.Statements {
		if strings.Contains(s.Shape, needle) {
			return s.Fingerprint, s.Shape
		}
	}
	t.Fatalf("no shape containing %q in %s", needle, body)
	return "", ""
}

// TestStatementShapesAreRecorded (issue #209): a node writes its
// busiest shapes' counters into datax_metrics_shapes every tick, keyed
// by fingerprint, so /api/metrics charts a shape's executions and time
// over a window like any counter; the shape's text is kept in
// datax_statement_shapes, which the catalog resolves for the picker
// and the query returns for the chart's title.
func TestStatementShapesAreRecorded(t *testing.T) {
	pglis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	// Shapes are counted at the wire, so the workload goes through pgx.
	tc := startMetricsCluster(t, 1, func(c *server.Config) { c.PGListener = pglis })
	ctx, cancel := context.WithTimeout(context.Background(), 180*time.Second)
	defer cancel()
	conn, err := pgx.Connect(ctx, pgURL(tc, 0))
	if err != nil {
		t.Fatalf("pgx connect: %v", err)
	}
	defer func() { _ = conn.Close(ctx) }()
	if _, err := conn.Exec(ctx, `CREATE TABLE ledger (id INT8 PRIMARY KEY, amount INT8 NOT NULL)`); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 20; i++ {
		if _, err := conn.Exec(ctx, fmt.Sprintf(`INSERT INTO ledger VALUES (%d, %d)`, i, i*10)); err != nil {
			t.Fatal(err)
		}
	}
	read := func() {
		for i := 0; i < 20; i++ {
			var amount int64
			if err := conn.QueryRow(ctx, fmt.Sprintf(`SELECT amount FROM ledger WHERE id = %d`, i)).Scan(&amount); err != nil {
				t.Fatalf("select: %v", err)
			}
		}
	}
	read()
	fp, shape := shapeFingerprint(t, tc, 0, "SELECT amount FROM ledger")
	series := "stmt.count{fp=" + fp + "}"

	// The counter needs two ticks to make a point of; keep the shape
	// busy so it stays in the top-N and every tick has a rise.
	deadline := time.Now().Add(90 * time.Second)
	var res server.MetricsResult
	for {
		code, doc := metricsQuery(t, tc, 0, "series="+series+",stmt.total_us{fp="+fp+"}&since=5m")
		if code == 200 && len(doc.Series) == 2 && len(doc.Series[0].Nodes["1"]) >= 2 {
			res = doc
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("%s never recorded: %d %+v", series, code, doc)
		}
		read()
		time.Sleep(time.Second)
	}
	s := res.Series[0]
	if s.Name != series || s.Kind != "counter" || s.Group != "statements" || s.Label != shape {
		t.Errorf("series: %+v (want label %q)", s, shape)
	}
	pts := s.Nodes["1"]
	if last := pts[len(pts)-1][1]; last < 20 {
		t.Errorf("stmt.count{fp=%s} = %v after at least twenty executions", fp, last)
	}
	if last := res.Series[1].Nodes["1"]; len(last) == 0 || last[len(last)-1][1] <= 0 {
		t.Errorf("stmt.total_us: %+v", res.Series[1])
	}
	// Rates are what the chart draws: a counter differentiated.
	if code, doc := metricsQuery(t, tc, 0, "series="+series+"&since=5m&rate=1"); code != 200 || len(doc.Series) != 1 || !doc.Series[0].Rate {
		t.Errorf("rate query: %d %+v", code, doc)
	}

	// The catalog knows the shape's fingerprint as an fp label value and
	// its text, and says where the series come from.
	_, _, body := httpGet(t, "http://"+tc.Nodes[0].HTTPAddr()+"/api/metrics")
	var cat server.MetricsCatalog
	if err := json.Unmarshal([]byte(body), &cat); err != nil {
		t.Fatal(err)
	}
	if cat.ShapesTable != catalog.ShapesMetricsTableName || !cat.ShapesReady || cat.ShapesTopN != 50 || cat.ShapesRetentionSeconds != 24*3600 {
		t.Errorf("catalog: %+v", cat)
	}
	found := false
	for _, v := range cat.Labels["fp"] {
		found = found || v == fp
	}
	if !found || cat.LabelText["fp"][fp] != shape {
		t.Errorf("catalog fp labels %v, text %q", cat.Labels["fp"], cat.LabelText["fp"][fp])
	}

	// The shape's text is in the fingerprint table, by SQL, with its
	// kind and the table it touches.
	root := sql.NewSession(tc.Nodes[0].DB(), catalog.NewAccessor())
	r := execSQL(t, ctx, root, `SELECT shape, kind, relations FROM datax_statement_shapes WHERE fingerprint = '`+fp+`'`)
	if len(r.Rows) != 1 || r.Rows[0][0].Text() != shape || r.Rows[0][1].Text() != "select" || r.Rows[0][2].Text() != "ledger" {
		t.Errorf("datax_statement_shapes: %+v", r.Rows)
	}
	// And the series rows are in the shapes table, not datax_metrics.
	r = execSQL(t, ctx, root, `SELECT count(*) FROM datax_metrics_shapes WHERE node = 1 AND name = '`+series+`'`)
	if len(r.Rows) != 1 || r.Rows[0][0].Text() == "0" {
		t.Errorf("datax_metrics_shapes rows: %+v", r.Rows)
	}
	r = execSQL(t, ctx, root, `SELECT count(*) FROM datax_metrics WHERE node = 1 AND name = '`+series+`'`)
	if len(r.Rows) != 1 || r.Rows[0][0].Text() != "0" {
		t.Errorf("datax_metrics has shape rows: %+v", r.Rows)
	}

	// Both shape tables are regenerable history, like datax_metrics: a
	// backup leaves them out unless asked (InfoSec on the PR: the
	// fingerprint table is a workload-and-schema disclosure in a backup
	// shared with a third party).
	backedUp := func(includeMetrics bool) map[string]bool {
		t.Helper()
		sum, err := tc.Nodes[0].RunBackup(ctx, t.TempDir(), "", false, includeMetrics)
		if err != nil {
			t.Fatal(err)
		}
		names := map[string]bool{}
		for _, bt := range sum.Tables {
			names[bt.Name] = true
		}
		return names
	}
	for _, tbl := range []string{catalog.ShapesMetricsTableName, catalog.StatementShapesTableName} {
		if backedUp(false)[tbl] {
			t.Errorf("a default backup carries %s", tbl)
		}
		if !backedUp(true)[tbl] {
			t.Errorf("an --include-metrics backup lacks %s", tbl)
		}
	}
}

// TestStatementShapesVersionGate: a cluster that has not finalized v19
// never creates the shape tables, and the endpoint says so.
func TestStatementShapesVersionGate(t *testing.T) {
	// The node serves SQL, so the recorder reaches the version clause
	// of its gate rather than returning for want of a SQL server (QA on
	// the PR: without the listener the test held at any version).
	pglis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	tc := startMetricsCluster(t, 1, func(c *server.Config) {
		c.BinaryVersionOverride = version.V18
		c.PGListener = pglis
	})
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	time.Sleep(4 * time.Second)
	root := sql.NewSession(tc.Nodes[0].DB(), catalog.NewAccessor())
	for _, tbl := range []string{catalog.ShapesMetricsTableName, catalog.StatementShapesTableName} {
		if _, serr := trySQL(ctx, root, `SELECT count(*) FROM `+tbl); serr == nil {
			t.Errorf("%s exists before v19 finalized", tbl)
		}
	}
	code, _, body := httpGet(t, "http://"+tc.Nodes[0].HTTPAddr()+"/api/metrics?series=stmt.count{fp=abc}&since=1m")
	if code != 503 || !strings.Contains(body, "v19") {
		t.Fatalf("shape query before v19: %d %s", code, body)
	}
	if _, _, body := httpGet(t, "http://"+tc.Nodes[0].HTTPAddr()+"/api/metrics"); !strings.Contains(body, `"shapes_ready": false`) {
		t.Fatalf("catalog before v19: %s", body)
	}
}

// TestShapeTextIsForAdmins: the fp label values and the shape text
// behind them are the shapes view's content, which is admin-gated; a
// reader gets the counters by name and nothing that says what a
// fingerprint is.
func TestShapeTextIsForAdmins(t *testing.T) {
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	tc, certsDir := startSecureCluster(t, "topsecret", func(i int, cfg *server.Config) {
		if i == 0 {
			cfg.HTTPListener = lis
		}
		cfg.MetricsRecordInterval = 2 * time.Second
	})
	ctx, cancel := context.WithTimeout(context.Background(), 180*time.Second)
	defer cancel()
	root := waitForRoot(t, ctx, tc, certsDir)
	defer root.Close(ctx)
	for _, stmt := range []string{
		`CREATE TABLE secrets (id INT8 PRIMARY KEY, v TEXT)`,
		`CREATE USER reader PASSWORD 'reader-pw'`,
	} {
		if _, err := root.Exec(ctx, stmt); err != nil {
			t.Fatalf("%s: %v", stmt, err)
		}
	}
	client := httpsClient(t, certsDir, "")
	base := "https://" + tc.Nodes[0].HTTPAddr()
	var cat server.MetricsCatalog
	deadline := time.Now().Add(90 * time.Second)
	for {
		for i := 0; i < 5; i++ {
			if _, err := root.Exec(ctx, fmt.Sprintf(`SELECT v FROM secrets WHERE id = %d`, i)); err != nil {
				t.Fatal(err)
			}
		}
		code, body, _ := authedGet(t, client, base+"/api/metrics", "root", "topsecret")
		if code != http.StatusOK {
			t.Fatalf("catalog as root: %d %s", code, body)
		}
		if err := json.Unmarshal([]byte(body), &cat); err != nil {
			t.Fatal(err)
		}
		if len(cat.LabelText["fp"]) > 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("root never saw a shape text: %s", body)
		}
		time.Sleep(time.Second)
	}
	var fp string
	for k, text := range cat.LabelText["fp"] {
		if strings.Contains(text, "SELECT v FROM secrets") {
			fp = k
		}
	}
	if fp == "" {
		t.Fatalf("no shape for the select: %+v", cat.LabelText)
	}
	code, body, _ := authedGet(t, client, base+"/api/metrics", "reader", "reader-pw")
	if code != http.StatusOK {
		t.Fatalf("catalog as reader: %d %s", code, body)
	}
	if strings.Contains(body, fp) || strings.Contains(body, "label_text") || strings.Contains(body, "SELECT v FROM secrets") {
		t.Errorf("reader's catalog names a shape: %s", body)
	}
	// Nor a table, or an index, the reader has no grant on: the table
	// and index label values follow the same rule as /api/schema
	// (InfoSec on the PR; issue #197's rule). The labels come from the
	// schema cache, which learns of the table on its own refresh, so
	// first wait until the admin's catalog offers the table's index —
	// a reader's catalog that lacks the name only because the cache
	// does would prove nothing.
	deadline = time.Now().Add(60 * time.Second)
	for {
		if _, rootBody, _ := authedGet(t, client, base+"/api/metrics", "root", "topsecret"); strings.Contains(rootBody, `"secrets.primary"`) {
			break
		}
		if time.Now().After(deadline) {
			_, rootBody, _ := authedGet(t, client, base+"/api/metrics", "root", "topsecret")
			t.Fatalf("root's catalog never offered the index label secrets.primary: %s", rootBody)
		}
		time.Sleep(200 * time.Millisecond)
	}
	if _, body, _ := authedGet(t, client, base+"/api/metrics", "reader", "reader-pw"); strings.Contains(body, "secrets") {
		t.Errorf("reader's catalog names a table or index it has no grant on: %s", body)
	}
	code, body, _ = authedGet(t, client, base+"/api/metrics?series=stmt.count{fp="+fp+"}&since=5m", "reader", "reader-pw")
	if code != http.StatusOK || strings.Contains(body, `"label"`) || strings.Contains(body, "secrets") {
		t.Errorf("reader's query: %d %s", code, body)
	}
	code, body, _ = authedGet(t, client, base+"/api/metrics?series=stmt.count{fp="+fp+"}&since=5m", "root", "topsecret")
	if code != http.StatusOK || !strings.Contains(body, "SELECT v FROM secrets") {
		t.Errorf("root's query: %d %s", code, body)
	}
}
