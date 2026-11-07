package testcluster

import (
	"context"
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
)

// TestIndexUsageIsCounted (issue #208): the executor charges a scan and
// its rows to the index the plan read through, where the scan opens —
// a secondary-index lookup to that index and not the primary, a
// primary-key point and a full scan to the primary and not the index,
// and an index created and never read to nothing at all.
func TestIndexUsageIsCounted(t *testing.T) {
	tc := Start(t, 1)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	s := sql.NewSession(tc.Nodes[0].DB(), catalog.NewAccessor())
	execSQL(t, ctx, s, `CREATE TABLE usage_t (id INT8 PRIMARY KEY, c INT8, d INT8)`)
	execSQL(t, ctx, s, `CREATE INDEX by_c ON usage_t (c)`)
	execSQL(t, ctx, s, `CREATE INDEX by_d ON usage_t (d)`)
	execSQL(t, ctx, s, `INSERT INTO usage_t VALUES (1, 10, 100), (2, 20, 200), (3, 20, 300)`)
	sql.ResetIndexUsage()
	use := func() map[uint64]sql.IndexUse {
		out := map[uint64]sql.IndexUse{}
		for _, u := range sql.IndexUsageSnapshot() {
			out[u.IndexID] = u
		}
		return out
	}
	// by_c is index 2, by_d index 3, the primary index 1.
	if res := execSQL(t, ctx, s, `SELECT id FROM usage_t WHERE c = 20`); len(res.Rows) != 2 {
		t.Fatalf("lookup by c: %d rows", len(res.Rows))
	}
	u := use()
	if u[2].Scans != 1 || u[2].Rows != 2 {
		t.Errorf("a lookup through by_c charged it %d scans and %d rows, want 1 and 2", u[2].Scans, u[2].Rows)
	}
	if u[1].Scans != 0 {
		t.Errorf("a lookup through by_c charged the primary %d scans: the rows behind the entries are read through the index", u[1].Scans)
	}
	if _, ok := u[3]; ok {
		t.Error("by_d, never read, has a usage entry")
	}
	if u[2].LastUsed.IsZero() || time.Since(u[2].LastUsed) > time.Minute {
		t.Errorf("by_c's last use is %v", u[2].LastUsed)
	}

	// The same lookup and a full scan over the wire, where a plain
	// top-level SELECT streams (QA on the PR): the stream iterator's
	// charges are the ordinary client's path, and the in-process session
	// above never takes it.
	conn, err := pgx.Connect(ctx, pgURL(tc, 0))
	if err != nil {
		t.Fatalf("pgx connect: %v", err)
	}
	defer func() { _ = conn.Close(ctx) }()
	drain := func(q string) int {
		t.Helper()
		rows, err := conn.Query(ctx, q)
		if err != nil {
			t.Fatalf("%s: %v", q, err)
		}
		n := 0
		for rows.Next() {
			n++
		}
		rows.Close()
		if rows.Err() != nil {
			t.Fatalf("%s: %v", q, rows.Err())
		}
		return n
	}
	sql.ResetIndexUsage()
	if n := drain(`SELECT id FROM usage_t WHERE c = 20`); n != 2 {
		t.Fatalf("streamed lookup by c: %d rows", n)
	}
	if n := drain(`SELECT id FROM usage_t`); n != 3 {
		t.Fatalf("streamed full scan: %d rows", n)
	}
	u = use()
	if u[2].Scans != 1 || u[2].Rows != 2 {
		t.Errorf("a streamed lookup through by_c charged it %d scans and %d rows, want 1 and 2", u[2].Scans, u[2].Rows)
	}
	if u[1].Scans != 1 || u[1].Rows != 3 {
		t.Errorf("a streamed full scan charged the primary %d scans and %d rows, want 1 and 3", u[1].Scans, u[1].Rows)
	}

	sql.ResetIndexUsage()
	if res := execSQL(t, ctx, s, `SELECT c FROM usage_t WHERE id = 1`); len(res.Rows) != 1 {
		t.Fatalf("point read: %d rows", len(res.Rows))
	}
	if res := execSQL(t, ctx, s, `SELECT count(*) FROM usage_t`); len(res.Rows) != 1 {
		t.Fatalf("full scan: %d rows", len(res.Rows))
	}
	u = use()
	if u[1].Scans != 2 || u[1].Rows != 1+3 {
		t.Errorf("a point read and a full scan charged the primary %d scans and %d rows, want 2 and 4", u[1].Scans, u[1].Rows)
	}
	if _, ok := u[2]; ok {
		t.Error("a point read and a full scan charged by_c")
	}
}

// TestIndexUsageIsSummedFromTheMetricsTable (issue #208): every node's
// recorder writes its counts as index.scans{index=table.index} and
// index.rows{…}; /api/index-usage sums them over the metrics table's
// retention across the nodes that recorded, the primary first, an index
// never read at zero rather than absent.
func TestIndexUsageIsSummedFromTheMetricsTable(t *testing.T) {
	tc := startMetricsCluster(t, 2, nil)
	ctx, cancel := context.WithTimeout(context.Background(), 180*time.Second)
	defer cancel()
	s0 := sql.NewSession(tc.Nodes[0].DB(), catalog.NewAccessor())
	s1 := sql.NewSession(tc.Nodes[1].DB(), catalog.NewAccessor())
	execSQL(t, ctx, s0, `CREATE TABLE orders (id INT8 PRIMARY KEY, customer INT8, sku INT8)`)
	execSQL(t, ctx, s0, `CREATE INDEX by_customer ON orders (customer)`)
	execSQL(t, ctx, s0, `CREATE INDEX by_sku ON orders (sku)`)
	execSQL(t, ctx, s0, `INSERT INTO orders VALUES (1, 7, 1), (2, 7, 2), (3, 8, 3)`)
	// Three lookups through by_customer on n1, two on n2; one primary
	// point read on each; by_sku never.
	for i := 0; i < 3; i++ {
		execSQL(t, ctx, s0, `SELECT id FROM orders WHERE customer = 7`)
	}
	for i := 0; i < 2; i++ {
		execSQL(t, ctx, s1, `SELECT id FROM orders WHERE customer = 8`)
	}
	execSQL(t, ctx, s0, `SELECT sku FROM orders WHERE id = 1`)
	execSQL(t, ctx, s1, `SELECT sku FROM orders WHERE id = 2`)

	// The first sample of a counter is history, so the sums need a tick
	// after the reads landed, and the recorder's second tick after that
	// — hence the wait.
	var doc server.IndexUsageStatus
	deadline := time.Now().Add(90 * time.Second)
	for {
		code, _, body := httpGet(t, "http://"+tc.Nodes[0].HTTPAddr()+"/api/index-usage?table=orders")
		if code == 200 {
			if err := jsonUnmarshal([]byte(body), &doc); err != nil {
				t.Fatalf("%v: %s", err, body)
			}
			byName := map[string]server.IndexUsageRow{}
			for _, r := range doc.Indexes {
				byName[r.Name] = r
			}
			if len(doc.Nodes) == 2 && byName["by_customer"].Scans >= 5 && byName["primary"].Scans >= 2 {
				break
			}
		}
		if time.Now().After(deadline) {
			t.Fatalf("index usage never summed both nodes' reads: %d %s", code, body)
		}
		// Keep reading so a counter that was first sampled before the
		// window-start problem has a rise to record on the next tick.
		execSQL(t, ctx, s0, `SELECT id FROM orders WHERE customer = 7`)
		execSQL(t, ctx, s1, `SELECT id FROM orders WHERE customer = 8`)
		execSQL(t, ctx, s0, `SELECT sku FROM orders WHERE id = 1`)
		execSQL(t, ctx, s1, `SELECT sku FROM orders WHERE id = 2`)
		time.Sleep(time.Second)
	}
	if doc.Table != "orders" || doc.WindowSeconds != 7*24*3600 || doc.NodesAsked != 2 || !doc.Recording {
		t.Errorf("document: %+v", doc)
	}
	if len(doc.Indexes) != 3 || doc.Indexes[0].Name != "primary" || !doc.Indexes[0].Primary {
		t.Fatalf("indexes: %+v", doc.Indexes)
	}
	for _, r := range doc.Indexes {
		switch r.Name {
		case "by_sku":
			if r.Scans != 0 || r.Rows != 0 || r.LastUsedUnixMs != 0 {
				t.Errorf("by_sku, never read, reports %+v", r)
			}
		case "by_customer", "primary":
			if r.Rows == 0 || r.LastUsedUnixMs == 0 || time.Since(time.UnixMilli(r.LastUsedUnixMs)) > 5*time.Minute {
				t.Errorf("%s: %+v", r.Name, r)
			}
		}
	}
	// A table the schema does not know is refused, not zeroed.
	if code, _, body := httpGet(t, "http://"+tc.Nodes[0].HTTPAddr()+"/api/index-usage?table=nope"); code != http.StatusNotFound || !strings.Contains(body, "no table called") {
		t.Errorf("unknown table: %d %s", code, body)
	}
	// The catalog offers the index label values, the primary included.
	code, _, body := httpGet(t, "http://"+tc.Nodes[0].HTTPAddr()+"/api/metrics")
	if code != 200 || !strings.Contains(body, `"orders.primary"`) || !strings.Contains(body, `"orders.by_sku"`) {
		t.Errorf("/api/metrics catalog lacks the index labels: %d %s", code, body)
	}
}

// TestIndexUsageNeedsTheAdminRole (issue #208): which indexes a
// workload uses is a fact about the workload, gated like the statements.
func TestIndexUsageNeedsTheAdminRole(t *testing.T) {
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	tc, certsDir := startSecureCluster(t, "topsecret", func(i int, cfg *server.Config) {
		if i == 0 {
			cfg.HTTPListener = lis
		}
	})
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()
	root := waitForRoot(t, ctx, tc, certsDir)
	defer root.Close(ctx)
	for _, stmt := range []string{
		`CREATE TABLE openbook (id TEXT PRIMARY KEY, v INT8)`,
		`CREATE USER reader PASSWORD 'reader-pw'`,
		`GRANT SELECT ON openbook TO reader`,
	} {
		if _, err := root.Exec(ctx, stmt); err != nil {
			t.Fatalf("%s: %v", stmt, err)
		}
	}
	client := httpsClient(t, certsDir, "")
	url := "https://" + tc.Nodes[0].HTTPAddr() + "/api/index-usage?table=openbook"
	if code, body, _ := authedGet(t, client, url, "reader", "reader-pw"); code != http.StatusForbidden {
		t.Errorf("reader: %d %s", code, body)
	}
	if code, body, _ := authedGet(t, client, url, "root", "topsecret"); code != http.StatusOK || !strings.Contains(body, `"primary"`) {
		t.Errorf("root: %d %s", code, fmt.Sprint(body))
	}
}
