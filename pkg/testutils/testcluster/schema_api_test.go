package testcluster

import (
	"context"
	"encoding/json"
	"net"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/sthorne/datax/pkg/base"
	"github.com/sthorne/datax/pkg/security"
	"github.com/sthorne/datax/pkg/server"
	"github.com/sthorne/datax/pkg/sql"
	"github.com/sthorne/datax/pkg/sql/catalog"
	"github.com/sthorne/datax/pkg/sql/types"
)

// TestSchemaAPI (issue #83): /api/schema lists every table with its
// columns, primary key, indexes, time-series options, grants, statistics
// and range footprint; ranges in /api/cluster and /status are labeled
// with their table; and /metrics carries the table gauges.
func TestSchemaAPI(t *testing.T) {
	tc := startWithHTTP(t, 3)
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()
	cat := catalog.NewAccessor()
	cat.EnableStats(tc.Nodes[0].DB())
	s := sql.NewSession(tc.Nodes[0].DB(), cat)

	execSQL(t, ctx, s, `CREATE TABLE accounts (id INT8 PRIMARY KEY, owner TEXT NOT NULL, balance DECIMAL(12,2))`)
	execSQL(t, ctx, s, `CREATE UNIQUE INDEX by_owner ON accounts (owner)`)
	execSQL(t, ctx, s, `CREATE TABLE metrics (series TEXT, at TIMESTAMPTZ, v FLOAT8, PRIMARY KEY (series, at)) WITH (timeseries = true, retention = '7d', shards = 4)`)
	for i := 0; i < 50; i++ {
		execSQL(t, ctx, s, `INSERT INTO accounts VALUES ($1, $2, 1.50)`, types.NewInt(int64(i)), types.NewString("user"+itoa(i)))
	}
	execSQL(t, ctx, s, `ANALYZE accounts`)
	execSQL(t, ctx, s, `CREATE USER analyst PASSWORD 'pw'`)
	execSQL(t, ctx, s, `GRANT SELECT, INSERT ON accounts TO analyst`)

	base := "http://" + tc.Nodes[0].HTTPAddr()
	code, ctype, body := httpGet(t, base+"/api/schema")
	if code != 200 || !strings.Contains(ctype, "application/json") {
		t.Fatalf("/api/schema: %d %s", code, ctype)
	}
	var doc server.SchemaStatus
	if err := json.Unmarshal([]byte(body), &doc); err != nil {
		t.Fatal(err)
	}
	if doc.Error != "" {
		t.Fatalf("schema document: %s", doc.Error)
	}
	byName := map[string]server.SchemaTable{}
	for _, tb := range doc.Tables {
		byName[tb.Name] = tb
	}
	acc, ok := byName["accounts"]
	if !ok {
		t.Fatalf("accounts missing from %+v", doc.Tables)
	}
	if len(acc.Columns) != 3 || acc.Columns[1].Name != "owner" || !acc.Columns[1].NotNull || acc.Columns[2].Precision != 12 || acc.Columns[2].Scale != 2 {
		t.Fatalf("accounts columns: %+v", acc.Columns)
	}
	if len(acc.PrimaryKey) != 1 || acc.PrimaryKey[0] != "id" {
		t.Fatalf("accounts primary key: %v", acc.PrimaryKey)
	}
	if len(acc.Indexes) != 1 || acc.Indexes[0].Name != "by_owner" || !acc.Indexes[0].Unique || acc.Indexes[0].Columns[0] != "owner" {
		t.Fatalf("accounts indexes: %+v", acc.Indexes)
	}
	if acc.Stats == nil || acc.Stats.RowCount != 50 || acc.Stats.Stale || acc.Stats.AgeSeconds < 0 {
		t.Fatalf("accounts stats: %+v", acc.Stats)
	}
	if acc.Ranges < 1 || acc.LocalReplicas < 1 {
		t.Fatalf("accounts footprint: ranges=%d local=%d", acc.Ranges, acc.LocalReplicas)
	}
	if got := acc.Privileges["analyst"]; len(got) != 2 {
		t.Fatalf("accounts grants: %v", acc.Privileges)
	}
	mt, ok := byName["metrics"]
	if !ok {
		t.Fatal("metrics missing")
	}
	if !mt.Timeseries || mt.RetentionSeconds != 7*86400 || mt.Shards != 4 {
		t.Fatalf("metrics timeseries options: %+v", mt)
	}
	if mt.Stats != nil {
		t.Fatalf("metrics was never analyzed, got stats %+v", mt.Stats)
	}
	hidden := 0
	for _, c := range mt.Columns {
		if c.Hidden {
			hidden++
		}
	}
	if hidden != 1 {
		t.Fatalf("the shard column should be the one hidden column, got %d in %+v", hidden, mt.Columns)
	}
	// Insecure mode: every viewer is admin and sees the user list.
	if !doc.Principal.Admin || len(doc.Users) < 2 {
		t.Fatalf("users for an admin viewer: principal=%+v users=%+v", doc.Principal, doc.Users)
	}

	// Ranges are labeled with the table their start key belongs to once
	// the schema cache has seen the catalog (the call above primed it).
	// The sharded metrics table owns its own ranges; a small table that
	// still shares range 1 with the system keys carries no label, since
	// r1 starts in the system space.
	_, _, body = httpGet(t, base+"/api/cluster")
	var cdoc server.ClusterStatus
	if err := json.Unmarshal([]byte(body), &cdoc); err != nil {
		t.Fatal(err)
	}
	labeled := map[string]bool{}
	for _, r := range cdoc.Ranges {
		if r.Table != "" {
			labeled[r.Table] = true
		}
	}
	if !labeled["metrics"] {
		t.Fatalf("no cluster range labeled metrics: %+v", cdoc.Ranges)
	}
	for _, r := range cdoc.Ranges {
		if r.RangeID == 1 && r.Table != "" {
			t.Fatalf("range 1 starts in the system space and should carry no table label, got %q", r.Table)
		}
	}

	_, _, body = httpGet(t, base+"/metrics")
	for _, want := range []string{`datax_table_ranges{table="accounts"}`, `datax_table_rows{table="accounts"} 50`, `datax_table_stats_age_seconds{table="accounts"}`} {
		if !strings.Contains(body, want) {
			t.Fatalf("/metrics lacks %s", want)
		}
	}
}

// TestSchemaAPISecureFiltering: in secure mode a user sees the tables it
// holds a grant on and no user list; root sees everything.
func TestSchemaAPISecureFiltering(t *testing.T) {
	httpLis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	tc, certsDir := startSecureCluster(t, "topsecret", func(i int, cfg *server.Config) {
		if i == 0 {
			cfg.HTTPListener = httpLis
		}
	})
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	var root *pgx.Conn
	deadline := time.Now().Add(30 * time.Second)
	for {
		root, err = connectSecure(ctx, secureURL(tc, certsDir, "root", "topsecret"))
		if err == nil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("secure cluster never served SQL: %v", err)
		}
		time.Sleep(200 * time.Millisecond)
	}
	defer func() { _ = root.Close(ctx) }()
	for _, stmt := range []string{
		`CREATE TABLE visible (id INT8 PRIMARY KEY)`,
		`CREATE TABLE hidden (id INT8 PRIMARY KEY)`,
		`CREATE USER reader PASSWORD 'reader-pw'`,
		`GRANT SELECT ON visible TO reader`,
	} {
		if _, err := root.Exec(ctx, stmt); err != nil {
			t.Fatalf("%s: %v", stmt, err)
		}
	}
	if err := security.CreateClientCert(certsDir, "root"); err != nil {
		t.Fatal(err)
	}
	client := httpsClient(t, certsDir, "")
	base := "https://" + tc.Nodes[0].HTTPAddr()
	fetch := func(user, pass string) server.SchemaStatus {
		t.Helper()
		code, body, _ := authedGet(t, client, base+"/api/schema", user, pass)
		if code != http.StatusOK {
			t.Fatalf("/api/schema as %s: %d (%s)", user, code, body)
		}
		var doc server.SchemaStatus
		if err := json.Unmarshal([]byte(body), &doc); err != nil {
			t.Fatal(err)
		}
		return doc
	}
	names := func(doc server.SchemaStatus) []string {
		var out []string
		for _, tb := range doc.Tables {
			out = append(out, tb.Name)
		}
		return out
	}
	asRoot := fetch("root", "topsecret")
	if !asRoot.Principal.Admin || len(asRoot.Tables) != 2 || len(asRoot.Users) != 2 {
		t.Fatalf("root's view: principal=%+v tables=%v users=%+v", asRoot.Principal, names(asRoot), asRoot.Users)
	}
	asReader := fetch("reader", "reader-pw")
	if asReader.Principal.Admin || asReader.Principal.User != "reader" {
		t.Fatalf("reader's principal: %+v", asReader.Principal)
	}
	if got := names(asReader); len(got) != 1 || got[0] != "visible" {
		t.Fatalf("reader should see only visible, got %v", got)
	}
	if len(asReader.Users) != 0 {
		t.Fatalf("reader should not see the user list, got %+v", asReader.Users)
	}
	if code, _, _ := authedGet(t, client, base+"/api/schema", "", ""); code != http.StatusUnauthorized {
		t.Fatalf("/api/schema without credentials: %d, want 401", code)
	}
}

// TestClusterAPIUnderPartition: the range-label refresh must never block
// the cluster API. A node cut off from range 1's leader cannot scan the
// catalog, so /api/cluster and /status keep answering from the names
// they already know (refreshing in the background) and /api/schema
// reports the catalog unavailable within its bounded build time rather
// than hanging the request.
func TestClusterAPIUnderPartition(t *testing.T) {
	tc := startWithHTTP(t, 3)
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()
	sess := sql.NewSession(tc.Nodes[0].DB(), catalog.NewAccessor())
	// Sharded, so the table owns ranges of its own (a small plain table
	// shares range 1 and labels nothing).
	execSQL(t, ctx, sess, `CREATE TABLE cut (ts TIMESTAMP, id INT, PRIMARY KEY (id, ts)) WITH (timeseries=true, retention='1d', shards=2)`)

	leader := tc.LeaderIndex(1)
	victim := (leader + 1) % 3
	addr := tc.Nodes[victim].HTTPAddr()
	labeledRange := func(doc server.ClusterStatus) bool {
		for _, r := range doc.Ranges {
			if r.Table == "cut" {
				return true
			}
		}
		return false
	}
	// Warm the cache so the labels exist, then let it go stale.
	deadline := time.Now().Add(30 * time.Second)
	for {
		code, _, body := httpGet(t, "http://"+addr+"/api/cluster")
		if code != 200 {
			t.Fatalf("/api/cluster before the partition: %d %s", code, body)
		}
		var doc server.ClusterStatus
		if err := jsonUnmarshal([]byte(body), &doc); err != nil {
			t.Fatal(err)
		}
		if labeledRange(doc) {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("no range labeled cut before the partition: %s", body)
		}
		time.Sleep(500 * time.Millisecond)
	}
	leaderID := base.NodeID(leader + 1)
	tc.Nodes[victim].InjectRPCDrop(func(to base.NodeID) bool { return to == leaderID })
	defer tc.Nodes[victim].InjectRPCDrop(nil)
	time.Sleep(6 * time.Second) // past schemaCacheFor

	start := time.Now()
	code, _, body := httpGet(t, "http://"+addr+"/api/cluster")
	if code != 200 {
		t.Fatalf("/api/cluster on the partitioned node: %d %s", code, body)
	}
	if took := time.Since(start); took > 3*time.Second {
		t.Fatalf("/api/cluster on the partitioned node took %s", took)
	}
	var doc server.ClusterStatus
	if err := jsonUnmarshal([]byte(body), &doc); err != nil {
		t.Fatal(err)
	}
	if !labeledRange(doc) {
		t.Fatalf("stale labels were dropped during the partition: %s", body)
	}
	if code, _, _ := httpGet(t, "http://"+addr+"/status"); code != 200 {
		t.Fatalf("/status on the partitioned node: %d", code)
	}
	// The browser's own document is bounded: it comes back (with the
	// catalog error, or from a build that beat the partition) rather than
	// hanging until the client gives up.
	start = time.Now()
	client := &http.Client{Timeout: 20 * time.Second}
	resp, err := client.Get("http://" + addr + "/api/schema")
	if err != nil {
		t.Fatalf("/api/schema on the partitioned node after %s: %v", time.Since(start), err)
	}
	_ = resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Fatalf("/api/schema on the partitioned node: %d", resp.StatusCode)
	}
}
