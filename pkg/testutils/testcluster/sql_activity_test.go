package testcluster

import (
	"context"
	"encoding/json"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/sthorne/datax/pkg/server"
)

// TestSQLActivity (issue #84): the SQL server accounts for connections by
// state (an idle transaction and its age), statements by kind, latency
// percentiles and COPY rows; the summary rides the heartbeat into
// /api/cluster; /api/activity lists the connections and the statements
// past the slow threshold; /metrics carries the series.
func TestSQLActivity(t *testing.T) {
	httpLis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	first := true
	tc := StartWithOptions(t, 3, func(cfg *server.Config) {
		if first {
			cfg.HTTPListener = httpLis
			first = false
		}
		cfg.SlowStatementThreshold = time.Nanosecond // record everything
	})
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()
	url := "postgres://root@" + tc.Nodes[0].SQLAddr() + "/datax?sslmode=disable"
	connect := func() *pgx.Conn {
		t.Helper()
		cfg, err := pgx.ParseConfig(url)
		if err != nil {
			t.Fatal(err)
		}
		cfg.DefaultQueryExecMode = pgx.QueryExecModeSimpleProtocol
		c, err := pgx.ConnectConfig(ctx, cfg)
		if err != nil {
			t.Fatal(err)
		}
		return c
	}
	a, b := connect(), connect()
	defer func() { _ = a.Close(ctx); _ = b.Close(ctx) }()

	if _, err := a.Exec(ctx, "CREATE TABLE kv (id INT8 PRIMARY KEY, v INT8)"); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 5; i++ {
		if _, err := a.Exec(ctx, "INSERT INTO kv VALUES ($1, $2)", i, i); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := a.Exec(ctx, "SELECT * FROM kv"); err != nil {
		t.Fatal(err)
	}
	// b opens a transaction and goes idle inside it.
	if _, err := b.Exec(ctx, "BEGIN"); err != nil {
		t.Fatal(err)
	}
	if _, err := b.Exec(ctx, "UPDATE kv SET v = 100 WHERE id = 1"); err != nil {
		t.Fatal(err)
	}
	time.Sleep(300 * time.Millisecond)

	base := "http://" + tc.Nodes[0].HTTPAddr()
	// The serving node's summary is live in /api/cluster.
	_, _, body := httpGet(t, base+"/api/cluster")
	var doc server.ClusterStatus
	if err := json.Unmarshal([]byte(body), &doc); err != nil {
		t.Fatal(err)
	}
	var n1 *server.ClusterNode
	for i := range doc.Nodes {
		if doc.Nodes[i].NodeID == 1 {
			n1 = &doc.Nodes[i]
		}
	}
	if n1 == nil || n1.SQL == nil {
		t.Fatalf("n1 has no SQL summary: %+v", doc.Nodes)
	}
	q := n1.SQL
	if q.Open < 2 || q.IdleInTxn != 1 || q.OldestIdleTxnMillis < 200 {
		t.Fatalf("connections: %+v", q)
	}
	if q.Statements["ddl"] < 1 || q.Statements["insert"] < 5 || q.Statements["select"] < 1 || q.Statements["txn"] < 1 || q.Statements["update"] < 1 {
		t.Fatalf("statement counts: %+v", q.Statements)
	}
	if q.P50Micros <= 0 || q.P99Micros < q.P50Micros {
		t.Fatalf("latency percentiles: p50=%d p99=%d", q.P50Micros, q.P99Micros)
	}
	if q.ByUser["root"] < 2 {
		t.Fatalf("by user: %v", q.ByUser)
	}

	// The admin view lists both connections with their states and, with
	// the threshold at zero, every statement in the slow ring.
	_, _, body = httpGet(t, base+"/api/activity")
	var act server.ActivityStatus
	if err := json.Unmarshal([]byte(body), &act); err != nil {
		t.Fatal(err)
	}
	states := map[string]int{}
	for _, c := range act.Connections {
		states[c.State]++
	}
	if states["idle_in_txn"] != 1 || states["idle"] < 1 {
		t.Fatalf("connection states: %v", states)
	}
	sawUpdate := false
	for _, s := range act.Slow {
		if s.Kind == "update" && strings.Contains(s.Text, "UPDATE kv") && s.User == "root" && s.Duration > 0 {
			sawUpdate = true
		}
	}
	if !sawUpdate {
		t.Fatalf("slow ring lacks the update: %+v", act.Slow)
	}
	if act.SlowThresholdMillis != 0 {
		t.Fatalf("slow threshold: %d ms", act.SlowThresholdMillis)
	}

	// Commit b's transaction: the idle-in-txn count drops.
	if _, err := b.Exec(ctx, "COMMIT"); err != nil {
		t.Fatal(err)
	}
	_, _, body = httpGet(t, base+"/api/activity")
	if err := json.Unmarshal([]byte(body), &act); err != nil {
		t.Fatal(err)
	}
	if act.Summary.IdleInTxn != 0 || act.Summary.Open < 2 {
		t.Fatalf("after COMMIT: %+v", act.Summary)
	}

	_, _, body = httpGet(t, base+"/metrics")
	for _, want := range []string{`datax_sql_connections{state="open"}`, `datax_sql_statements_total{kind="insert"}`, "datax_sql_statement_latency_seconds_count", "datax_sql_serialization_failures_total"} {
		if !strings.Contains(body, want) {
			t.Fatalf("/metrics lacks %s", want)
		}
	}

	// Other nodes' rows arrive through the heartbeat: n2 has served
	// nothing, and says so rather than being absent.
	deadline := time.Now().Add(15 * time.Second)
	for {
		_, _, body = httpGet(t, base+"/api/cluster")
		if err := json.Unmarshal([]byte(body), &doc); err != nil {
			t.Fatal(err)
		}
		ok := false
		for _, cn := range doc.Nodes {
			if cn.NodeID == 2 && cn.SQL != nil && cn.SQL.Open == 0 {
				ok = true
			}
		}
		if ok {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("n2 never advertised a SQL summary: %+v", doc.Nodes)
		}
		time.Sleep(300 * time.Millisecond)
	}
}
