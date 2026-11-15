package testcluster

import (
	"context"
	"encoding/json"
	"net"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/sthorne/datax/pkg/server"
)

// TestRowSecurityGatesTheConsole (issue #294): the console answers as
// the signed-in role, and must not be a way around a policy. A range
// boundary is a row key, and table statistics count every row; a role
// that may SELECT from a table sees both — until the table's row
// security confines it, after which the console decodes none of the
// table's boundaries for it and reports no row count, while the admin's
// view is unchanged. Disabling row security restores both.
func TestRowSecurityGatesTheConsole(t *testing.T) {
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
	// The boundary is another tenant's key.
	const row = "tenant-b-boundary-key"
	exec := func(stmt string) {
		t.Helper()
		if _, err := root.Exec(ctx, stmt); err != nil {
			t.Fatalf("%s: %v", stmt, err)
		}
	}
	exec(`CREATE TABLE ledger (tenant TEXT, k TEXT, v INT8, PRIMARY KEY (tenant, k))`)
	exec(`INSERT INTO ledger VALUES ('tenant-a', 'k1', 1), ('` + row + `', 'k1', 2), ('tenant-c', 'k1', 3)`)
	exec(`ALTER TABLE ledger SPLIT AT VALUES ('` + row + `')`)
	exec(`ANALYZE ledger`)
	exec(`CREATE USER tenant_a PASSWORD 'tenant-a-pw'`)
	exec(`GRANT SELECT ON ledger TO tenant_a`)

	client := httpsClient(t, certsDir, "")
	base := "https://" + tc.Nodes[0].HTTPAddr()
	get := func(path, user, pass string) string {
		t.Helper()
		code, body, _ := authedGet(t, client, base+path, user, pass)
		if code != http.StatusOK {
			t.Fatalf("%s as %s: %d (%s)", path, user, code, body)
		}
		return body
	}
	// ledgerIn reads the caller's /api/schema: whether it lists ledger,
	// and the row count it reports for it (-1 when none).
	ledgerIn := func(user, pass string) (listed bool, rows int64) {
		t.Helper()
		var doc struct {
			Tables []struct {
				Name  string `json:"name"`
				Stats *struct {
					RowCount int64 `json:"row_count"`
				} `json:"stats"`
			} `json:"tables"`
		}
		if err := json.Unmarshal([]byte(get("/api/schema", user, pass)), &doc); err != nil {
			t.Fatalf("/api/schema as %s: %v", user, err)
		}
		for _, tbl := range doc.Tables {
			if tbl.Name != "ledger" {
				continue
			}
			if tbl.Stats == nil {
				return true, -1
			}
			return true, tbl.Stats.RowCount
		}
		return false, -1
	}
	rowCount := func(user, pass string) int64 {
		t.Helper()
		_, rows := ledgerIn(user, pass)
		return rows
	}
	await := func(what string, ok func() bool) {
		t.Helper()
		deadline := time.Now().Add(30 * time.Second)
		for !ok() {
			if time.Now().After(deadline) {
				t.Fatal(what)
			}
			time.Sleep(250 * time.Millisecond)
		}
	}
	paths := []string{"/api/cluster", "/status", "/api/overview", "/api/node"}
	shown := func(user, pass string) bool {
		for _, path := range paths {
			if strings.Contains(get(path, user, pass), row) {
				return true
			}
		}
		return false
	}

	// Before row security: a SELECT grantee sees the boundary and the
	// row count, as it could read them over SQL.
	await("the grantee never saw the boundary and the row count before row security", func() bool {
		return strings.Contains(get("/api/cluster", "tenant_a", "tenant-a-pw"), row) && rowCount("tenant_a", "tenant-a-pw") == 3
	})

	exec(`ALTER TABLE ledger ENABLE ROW LEVEL SECURITY`)
	exec(`CREATE POLICY own ON ledger USING (tenant = 'tenant-a')`)
	tenant, err := connectSecure(ctx, secureURL(tc, certsDir, "tenant_a", "tenant-a-pw"))
	if err != nil {
		t.Fatal(err)
	}
	defer tenant.Close(ctx)
	var n int64
	if err := tenant.QueryRow(ctx, `SELECT count(*) FROM ledger`).Scan(&n); err != nil || n != 1 {
		t.Fatalf("the tenant role reads %d rows over SQL (%v), want its 1", n, err)
	}
	// The console follows within the few seconds it reuses its copy of
	// the schema for, and then holds on every page that renders a key.
	await("the console still shows the confined role another tenant's key or the table's row count", func() bool {
		return !shown("tenant_a", "tenant-a-pw") && rowCount("tenant_a", "tenant-a-pw") == -1
	})
	if listed, _ := ledgerIn("tenant_a", "tenant-a-pw"); !listed {
		t.Fatal("the confined role no longer sees that the table exists: it should, as SHOW TABLES names it")
	}
	if body := get("/api/cluster", "tenant_a", "tenant-a-pw"); !strings.Contains(strings.ReplaceAll(body, " ", ""), `"keys_redacted":true`) {
		t.Fatal("the confined role's document does not say that keys were withheld")
	}
	// The admin's view is the whole of it.
	if !strings.Contains(get("/api/cluster", "root", "topsecret"), row) || rowCount("root", "topsecret") != 3 {
		t.Fatal("the admin lost the boundary or the row count")
	}

	exec(`ALTER TABLE ledger DISABLE ROW LEVEL SECURITY`)
	await("the console still withholds from the grantee what SQL lets it read again", func() bool {
		return strings.Contains(get("/api/cluster", "tenant_a", "tenant-a-pw"), row) && rowCount("tenant_a", "tenant-a-pw") == 3
	})
}
