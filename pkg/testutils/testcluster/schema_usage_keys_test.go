package testcluster

import (
	"context"
	"net"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/sthorne/datax/pkg/server"
)

// TestSchemaUsageGatesTheKeyViewer (issue #259): SQL authorizes a table
// read as the table's own privilege AND usage of its schema, and REVOKE
// USAGE ON SCHEMA public FROM PUBLIC turns the second on. After it, a
// grantee holding SELECT on a table and no USAGE on its schema is
// refused the row by SQL, so the console decodes no boundary of the
// table for it and /api/schema does not list the table; a grant of
// USAGE restores both, and the admin is never affected.
func TestSchemaUsageGatesTheKeyViewer(t *testing.T) {
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
	const row = "sealed-row-value"
	exec := func(stmt string) {
		t.Helper()
		if _, err := root.Exec(ctx, stmt); err != nil {
			t.Fatalf("%s: %v", stmt, err)
		}
	}
	exec(`CREATE TABLE sealedbook (id TEXT PRIMARY KEY, v INT8)`)
	exec(`INSERT INTO sealedbook VALUES ('` + row + `', 1), ('zz', 2)`)
	exec(`ALTER TABLE sealedbook SPLIT AT VALUES ('` + row + `')`)
	exec(`CREATE USER blocked PASSWORD 'blocked-pw'`)
	exec(`GRANT SELECT ON sealedbook TO blocked`)

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
	// Wait for the split boundary and the table name to be in the
	// admin's documents, then for the grantee's.
	deadline := time.Now().Add(30 * time.Second)
	for {
		if strings.Contains(get("/api/cluster", "root", "topsecret"), row) && strings.Contains(get("/api/schema", "blocked", "blocked-pw"), `"sealedbook"`) &&
			strings.Contains(get("/api/cluster", "blocked", "blocked-pw"), row) {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("the grantee never saw the boundary row value before the revoke")
		}
		time.Sleep(200 * time.Millisecond)
	}

	exec(`REVOKE USAGE ON SCHEMA public FROM PUBLIC`)
	// SQL refuses the row now.
	blocked, err := connectSecure(ctx, secureURL(tc, certsDir, "blocked", "blocked-pw"))
	if err != nil {
		t.Fatal(err)
	}
	defer blocked.Close(ctx)
	if _, err := blocked.Exec(ctx, `SELECT v FROM sealedbook`); err == nil || !strings.Contains(err.Error(), "permission denied") {
		t.Fatalf("SELECT as blocked after the revoke: %v, want permission denied", err)
	}
	// So the console shows neither the row value nor the table — within
	// the few seconds the viewer reuses its database list for (the
	// cache InfoSec asked for on the PR), after which it must hold.
	consoleShows := func(want bool, when string) {
		t.Helper()
		paths := []string{"/api/cluster", "/status", "/api/overview", "/api/node"}
		deadline := time.Now().Add(20 * time.Second)
		for {
			var wrong []string
			for _, path := range paths {
				if body := get(path, "blocked", "blocked-pw"); strings.Contains(body, row) != want {
					wrong = append(wrong, path+": "+body)
				}
			}
			if body := get("/api/schema", "blocked", "blocked-pw"); strings.Contains(body, `"sealedbook"`) != want {
				wrong = append(wrong, "/api/schema: "+body)
			}
			if len(wrong) == 0 {
				return
			}
			if time.Now().After(deadline) {
				if want {
					t.Fatalf("%s, the console still withholds from blocked what SQL lets it read:\n%s", when, strings.Join(wrong, "\n"))
				}
				t.Fatalf("%s, the console still shows blocked the row value or the table SQL refuses it:\n%s", when, strings.Join(wrong, "\n"))
			}
			time.Sleep(250 * time.Millisecond)
		}
	}
	consoleShows(false, "after REVOKE USAGE")
	// The admin is unaffected.
	if body := get("/api/cluster", "root", "topsecret"); !strings.Contains(body, row) {
		t.Errorf("root lost the boundary row value to the revoke")
	}
	// And a grant of USAGE restores the grantee.
	exec(`GRANT USAGE ON SCHEMA public TO blocked`)
	if _, err := blocked.Exec(ctx, `SELECT v FROM sealedbook`); err != nil {
		t.Fatalf("SELECT as blocked after GRANT USAGE: %v", err)
	}
	consoleShows(true, "after GRANT USAGE")
}
