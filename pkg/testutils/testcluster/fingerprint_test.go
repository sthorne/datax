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
)

// TestStatementFingerprints (issue #157): the question that drives
// optimisation work is which statement shape costs the cluster the most,
// and it cannot be answered by a slow-statement ring — a statement that
// is fast and frequent never enters one. This runs exactly that shape
// and asserts it is visible, grouped, and charged its own rows.
func TestStatementFingerprints(t *testing.T) {
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
	})
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	conn, cerr := pgx.Connect(ctx, pgURL(tc, 0))
	if cerr != nil {
		t.Fatalf("pgx connect: %v", cerr)
	}
	defer func() { _ = conn.Close(ctx) }()
	exec := func(q string, args ...any) {
		t.Helper()
		if _, err := conn.Exec(ctx, q, args...); err != nil {
			t.Fatalf("%s: %v", q, err)
		}
	}
	exec(`CREATE TABLE accounts (id INT8 PRIMARY KEY, balance INT8 NOT NULL)`)
	for i := 0; i < 50; i++ {
		exec(`INSERT INTO accounts VALUES ($1, $2)`, i, 100)
	}

	// The frequent-and-fast shape: fifty executions, differing only in
	// their literal. One row, or the premise fails.
	for i := 0; i < 50; i++ {
		var bal int64
		if err := conn.QueryRow(ctx, fmt.Sprintf(`SELECT balance FROM accounts WHERE id = %d`, i)).Scan(&bal); err != nil {
			t.Fatalf("select: %v", err)
		}
	}
	// A scan, so rows scanned can be told from rows returned.
	rows, err := conn.Query(ctx, `SELECT id FROM accounts WHERE balance > 1000000`)
	if err != nil {
		t.Fatalf("scan: %v", err)
	}
	for rows.Next() {
	}
	rows.Close()

	addr := tc.Nodes[0].HTTPAddr()
	var doc server.StatementsStatus
	code, _, body := httpGet(t, "http://"+addr+"/api/statements")
	if code != 200 {
		t.Fatalf("/api/statements: %d %s", code, body)
	}
	if err := json.Unmarshal([]byte(body), &doc); err != nil {
		t.Fatal(err)
	}
	if len(doc.Statements) == 0 {
		t.Fatalf("no statement shapes recorded after 100+ executions: %s", body)
	}

	find := func(want string) *server.ClusterStatement {
		for i, s := range doc.Statements {
			if strings.Contains(s.Shape, want) {
				return &doc.Statements[i]
			}
		}
		return nil
	}

	// Fifty executions differing only in a literal are one row with a
	// count of fifty — the grouping the whole facility rests on.
	point := find("SELECT balance FROM accounts WHERE id = ?")
	if point == nil {
		t.Fatalf("the point lookup is not among the shapes: %+v", shapeList(doc))
	}
	if point.Count != 50 {
		t.Errorf("the point lookup ran 50 times, counted %d", point.Count)
	}
	if point.TotalMicros <= 0 || point.MeanMicros <= 0 {
		t.Errorf("no time charged to a shape that ran 50 times: %+v", point)
	}
	if point.MeanMicros*int64(point.Count) > point.TotalMicros*11/10 ||
		point.MeanMicros*int64(point.Count) < point.TotalMicros*9/10 {
		t.Errorf("mean * count should be about the total: %d * %d vs %d",
			point.MeanMicros, point.Count, point.TotalMicros)
	}
	if len(point.PerNode) == 0 {
		t.Errorf("no per-node detail: %+v", point)
	}
	// Percentiles belong to the node that measured them.
	if point.PerNode[0].P50Micros <= 0 {
		t.Errorf("no p50 on the per-node row: %+v", point.PerNode[0])
	}
	if len(point.Tables) != 1 || point.Tables[0] != "accounts" {
		t.Errorf("tables %v, want [accounts]", point.Tables)
	}
	// The literal must not have survived into the shape, or every
	// execution would be its own row.
	if strings.Contains(point.Shape, "= 4") || strings.Contains(point.Shape, "= 7") {
		t.Errorf("a literal survived into the shape: %q", point.Shape)
	}

	// The scan read many rows and returned none: the signature this view
	// exists to expose, and invisible to a slow-statement ring.
	scan := find("SELECT id FROM accounts WHERE balance > ?")
	if scan == nil {
		t.Fatalf("the scan is not among the shapes: %+v", shapeList(doc))
	}
	if scan.RowsScanned == 0 {
		t.Errorf("a full scan of 50 rows is charged no rows scanned: %+v", scan)
	}
	if scan.RowsScanned <= scan.RowsReturned {
		t.Errorf("the scan read more than it returned; got scanned %d returned %d",
			scan.RowsScanned, scan.RowsReturned)
	}

	// The INSERT ran fifty times as one shape too, despite fifty
	// different parameter values.
	ins := find("INSERT INTO accounts")
	if ins == nil || ins.Count != 50 {
		t.Errorf("the insert should be one shape run 50 times: %+v", ins)
	}

	// Every node was asked, and the document says how many answered.
	if doc.NodesAsked != 3 {
		t.Errorf("asked %d nodes, want 3", doc.NodesAsked)
	}
	if doc.Nodes < 1 {
		t.Errorf("no node answered: %+v", doc.Errors)
	}
}

// EXPLAIN closes the loop from "this shape is expensive" to "here is
// why", and plans the server's own stored representative rather than
// anything the caller sends.
func TestStatementExplain(t *testing.T) {
	httpLis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	first := true
	tc := StartWithOptions(t, 1, func(cfg *server.Config) {
		if first {
			cfg.HTTPListener = httpLis
			first = false
		}
	})
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	conn, cerr := pgx.Connect(ctx, pgURL(tc, 0))
	if cerr != nil {
		t.Fatalf("pgx connect: %v", cerr)
	}
	defer func() { _ = conn.Close(ctx) }()
	for _, q := range []string{
		`CREATE TABLE t (id INT8 PRIMARY KEY, v INT8)`,
		`INSERT INTO t VALUES (1, 1), (2, 2)`,
	} {
		if _, err := conn.Exec(ctx, q); err != nil {
			t.Fatalf("%s: %v", q, err)
		}
	}
	var v int64
	if err := conn.QueryRow(ctx, `SELECT v FROM t WHERE id = 1`).Scan(&v); err != nil {
		t.Fatal(err)
	}

	addr := tc.Nodes[0].HTTPAddr()
	code, _, body := httpGet(t, "http://"+addr+"/api/statements")
	if code != 200 {
		t.Fatalf("/api/statements: %d %s", code, body)
	}
	var doc server.StatementsStatus
	if err := json.Unmarshal([]byte(body), &doc); err != nil {
		t.Fatal(err)
	}
	var fp string
	for _, s := range doc.Statements {
		if strings.Contains(s.Shape, "SELECT v FROM t WHERE id = ?") {
			fp = s.Fingerprint
		}
	}
	if fp == "" {
		t.Fatalf("the select is not among the shapes: %+v", shapeList(doc))
	}

	code, _, body = httpGet(t, "http://"+addr+"/api/explain?fingerprint="+fp)
	if code != 200 {
		t.Fatalf("/api/explain: %d %s", code, body)
	}
	var ex server.ExplainStatus
	if err := json.Unmarshal([]byte(body), &ex); err != nil {
		t.Fatal(err)
	}
	if ex.Error != "" {
		t.Fatalf("explain: %s", ex.Error)
	}
	if len(ex.Plan) == 0 {
		t.Fatalf("no plan returned: %s", body)
	}
	// The document shows what was explained, so the reader need not
	// trust that it was the right statement.
	if !strings.Contains(ex.Statement, "SELECT v FROM t") {
		t.Errorf("the explained statement is not reported: %q", ex.Statement)
	}
	// An unknown fingerprint is a 404 that says why, not a plan for
	// something invented.
	code, _, body = httpGet(t, "http://"+addr+"/api/explain?fingerprint=deadbeefdeadbeef")
	if code != http.StatusNotFound {
		t.Fatalf("explain of an unknown shape: %d %s, want 404", code, body)
	}
	if !strings.Contains(body, "bounded") {
		t.Errorf("the refusal should say why the shape is unknown: %s", body)
	}
	// And no fingerprint at all is a 400 rather than a plan of nothing.
	if code, _, _ := httpGet(t, "http://"+addr+"/api/explain"); code != http.StatusBadRequest {
		t.Fatalf("explain without a fingerprint: %d, want 400", code)
	}
}

// TestStatementGating (issue #157): a representative statement can carry
// data, so the list and the plans stay behind the admin gate.
func TestStatementGating(t *testing.T) {
	httpLis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	tc, certsDir := startSecureCluster(t, "topsecret", func(i int, cfg *server.Config) {
		if i == 0 {
			cfg.HTTPListener = httpLis
		}
	})
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	base := "https://" + tc.Nodes[0].HTTPAddr()
	client := httpsClient(t, certsDir, "")

	deadline := time.Now().Add(30 * time.Second)
	for {
		if code, _, _ := authedGet(t, client, base+"/status", "root", "topsecret"); code == http.StatusOK {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("root basic auth never succeeded")
		}
		time.Sleep(200 * time.Millisecond)
	}
	rootConn, err := connectSecure(ctx, secureURL(tc, certsDir, "root", "topsecret"))
	if err != nil {
		t.Fatalf("connect as root: %v", err)
	}
	defer func() { _ = rootConn.Close(ctx) }()
	if _, err := rootConn.Exec(ctx, `CREATE TABLE secrets (id INT8 PRIMARY KEY, v TEXT)`); err != nil {
		t.Fatal(err)
	}
	if _, err := rootConn.Exec(ctx, `CREATE USER scraper PASSWORD 'metrics-pw'`); err != nil {
		t.Fatal(err)
	}
	for {
		if code, _, _ := authedGet(t, client, base+"/status", "scraper", "metrics-pw"); code == http.StatusOK {
			break
		}
		if time.Now().After(deadline.Add(30 * time.Second)) {
			t.Fatal("scraper basic auth never succeeded")
		}
		time.Sleep(200 * time.Millisecond)
	}

	for _, path := range []string{"/api/statements", "/api/explain?fingerprint=abc"} {
		if code, body, _ := authedGet(t, client, base+path, "scraper", "metrics-pw"); code != http.StatusForbidden {
			t.Errorf("%s as a non-admin: %d %s, want 403", path, code, body)
		}
	}
	if code, _, _ := authedGet(t, client, base+"/api/statements", "root", "topsecret"); code != http.StatusOK {
		t.Errorf("/api/statements as root: want 200")
	}
}

func shapeList(doc server.StatementsStatus) []string {
	out := make([]string, 0, len(doc.Statements))
	for _, s := range doc.Statements {
		out = append(out, s.Shape)
	}
	return out
}
