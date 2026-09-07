package testcluster

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"io"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/sthorne/datax/pkg/security"
	"github.com/sthorne/datax/pkg/server"
)

// httpsClient builds a client that trusts the test CA; a non-empty
// certUser also loads that user's client certificate (from
// security.CreateClientCert).
func httpsClient(t *testing.T, certsDir, certUser string) *http.Client {
	t.Helper()
	caPEM, err := os.ReadFile(filepath.Join(certsDir, "ca.crt"))
	if err != nil {
		t.Fatal(err)
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(caPEM) {
		t.Fatal("bad CA pem")
	}
	tlsCfg := &tls.Config{RootCAs: pool}
	if certUser != "" {
		cert, err := tls.LoadX509KeyPair(
			filepath.Join(certsDir, "client."+certUser+".crt"),
			filepath.Join(certsDir, "client."+certUser+".key"))
		if err != nil {
			t.Fatal(err)
		}
		tlsCfg.Certificates = []tls.Certificate{cert}
	}
	return &http.Client{
		Timeout:   10 * time.Second,
		Transport: &http.Transport{TLSClientConfig: tlsCfg},
	}
}

func authedGet(t *testing.T, client *http.Client, url, user, pass string) (int, string, http.Header) {
	t.Helper()
	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		t.Fatal(err)
	}
	if user != "" {
		req.SetBasicAuth(user, pass)
	}
	resp, err := client.Do(req)
	if err != nil {
		t.Fatalf("GET %s: %v", url, err)
	}
	defer func() { _ = resp.Body.Close() }()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	return resp.StatusCode, string(body), resp.Header
}

// TestHTTPAuthSecure: in secure mode every observability route requires
// HTTP Basic credentials of any valid user (verified against the stored
// SCRAM verifier) or a CA-verified client certificate; unknown users and
// wrong passwords fail identically; insecure mode stays open.
func TestHTTPAuthSecure(t *testing.T) {
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
	base := "https://" + tc.Nodes[0].HTTPAddr()
	client := httpsClient(t, certsDir, "")

	// Root's credential is seeded asynchronously; wait for auth to work.
	deadline := time.Now().Add(30 * time.Second)
	for {
		code, _, _ := authedGet(t, client, base+"/status", "root", "topsecret")
		if code == http.StatusOK {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("root basic auth never succeeded (last code %d)", code)
		}
		time.Sleep(200 * time.Millisecond)
	}

	// Every route: 401 without credentials (with a challenge), 200 with.
	for _, path := range []string{"/", "/metrics", "/status", "/api/cluster"} {
		code, _, hdr := authedGet(t, client, base+path, "", "")
		if code != http.StatusUnauthorized {
			t.Fatalf("%s without creds: %d, want 401", path, code)
		}
		if hdr.Get("WWW-Authenticate") == "" {
			t.Fatalf("%s: missing WWW-Authenticate challenge", path)
		}
		if code, _, _ := authedGet(t, client, base+path, "root", "topsecret"); code != http.StatusOK {
			t.Fatalf("%s with root creds: %d, want 200", path, code)
		}
	}

	// Wrong password and unknown user fail identically.
	codeW, bodyW, _ := authedGet(t, client, base+"/status", "root", "wrong")
	codeU, bodyU, _ := authedGet(t, client, base+"/status", "nobody", "topsecret")
	if codeW != http.StatusUnauthorized || codeU != http.StatusUnauthorized {
		t.Fatalf("bad-credential codes: %d / %d, want 401 / 401", codeW, codeU)
	}
	if bodyW != bodyU {
		t.Fatalf("unknown-user response differs from wrong-password: %q vs %q", bodyU, bodyW)
	}

	// Any valid user reaches the read-only endpoints; /metrics takes the
	// metrics role (or admin), so a scrape account needs no table grants.
	conn, err := connectSecure(ctx, secureURL(tc, certsDir, "root", "topsecret"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := conn.Exec(ctx, `CREATE USER scraper PASSWORD 'metrics-pw'`); err != nil {
		t.Fatal(err)
	}
	deadline = time.Now().Add(30 * time.Second)
	for {
		code, _, _ := authedGet(t, client, base+"/status", "scraper", "metrics-pw")
		if code == http.StatusOK {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("non-admin user never authenticated (last code %d)", code)
		}
		time.Sleep(200 * time.Millisecond)
	}
	if code, _, _ := authedGet(t, client, base+"/metrics", "scraper", "metrics-pw"); code != http.StatusForbidden {
		t.Fatalf("/metrics without the metrics role: %d, want 403", code)
	}
	if _, err := conn.Exec(ctx, `GRANT metrics TO scraper`); err != nil {
		t.Fatal(err)
	}
	_ = conn.Close(ctx)
	if code, _, _ := authedGet(t, client, base+"/metrics", "scraper", "metrics-pw"); code != http.StatusOK {
		t.Fatalf("/metrics with the metrics role: %d, want 200", code)
	}
	if code, _, _ := authedGet(t, client, base+"/metrics", "scraper", "wrong"); code != http.StatusUnauthorized {
		t.Fatal("scraper with wrong password accepted")
	}
	// Profiles are admin-only (issue #100): they expose statement text.
	if code, _, _ := authedGet(t, client, base+"/debug/pprof/goroutine", "scraper", "metrics-pw"); code != http.StatusForbidden {
		t.Fatalf("/debug/pprof/ as a non-admin: %d, want 403", code)
	}
	if code, body, _ := authedGet(t, client, base+"/debug/pprof/goroutine?debug=1", "root", "topsecret"); code != http.StatusOK || !strings.Contains(body, "goroutine") {
		t.Fatalf("/debug/pprof/ as root: %d", code)
	}

	// /api/cluster tells the caller who it is signed in as and whether it
	// holds the admin role, so the dashboard can show it and explain a
	// drill-down refusal in terms of it.
	principalOf := func(user, pass string) server.ClusterPrincipal {
		t.Helper()
		code, body, _ := authedGet(t, client, base+"/api/cluster", user, pass)
		if code != http.StatusOK {
			t.Fatalf("/api/cluster as %s: %d (%s)", user, code, body)
		}
		var doc server.ClusterStatus
		if err := json.Unmarshal([]byte(body), &doc); err != nil {
			t.Fatalf("undecodable /api/cluster body: %v", err)
		}
		return doc.Principal
	}
	if p := principalOf("scraper", "metrics-pw"); !p.Secure || p.User != "scraper" || p.Via != "basic" || p.Admin {
		t.Fatalf("principal as scraper: %+v", p)
	}
	if p := principalOf("root", "topsecret"); !p.Secure || p.User != "root" || p.Via != "basic" || !p.Admin {
		t.Fatalf("principal as root: %+v", p)
	}

	// The cross-node drill-down is admin-gated: a non-admin gets 403 (not
	// 401 — they authenticated fine), an admin gets every replica's view.
	if code, _, _ := authedGet(t, client, base+"/api/range?id=1", "scraper", "metrics-pw"); code != http.StatusForbidden {
		t.Fatalf("/api/range as non-admin: want 403")
	}
	// The node detail page follows the same rule: the serving node's own
	// document for anyone, another node's only for admins.
	if code, _, _ := authedGet(t, client, base+"/api/node?id=2", "scraper", "metrics-pw"); code != http.StatusForbidden {
		t.Fatalf("/api/node?id=2 as non-admin: want 403")
	}
	if code, body, _ := authedGet(t, client, base+"/api/node", "scraper", "metrics-pw"); code != http.StatusOK || !strings.Contains(body, `"node_id": 1`) {
		t.Fatalf("/api/node (self) as non-admin: %d %s", code, body)
	}
	// Statement text is admin-only too; the refusal names the reason so
	// the console can explain it (issue #146).
	if code, body, _ := authedGet(t, client, base+"/api/activity", "scraper", "metrics-pw"); code != http.StatusForbidden || !strings.Contains(body, "admin role required") {
		t.Fatalf("/api/activity as non-admin: %d %q, want 403 naming the admin role", code, body)
	}
	if code, body, _ := authedGet(t, client, base+"/api/node?id=2", "root", "topsecret"); code != http.StatusOK || !strings.Contains(body, `"node_id": 2`) {
		t.Fatalf("/api/node?id=2 as root: %d %s", code, body)
	}
	code, body, _ := authedGet(t, client, base+"/api/range?id=1", "root", "topsecret")
	if code != http.StatusOK {
		t.Fatalf("/api/range as root: %d, want 200 (%s)", code, body)
	}
	var detail server.RangeDetail
	if err := json.Unmarshal([]byte(body), &detail); err != nil {
		t.Fatalf("undecodable /api/range body: %v", err)
	}
	if detail.RangeID != 1 || len(detail.Replicas) != 3 {
		t.Fatalf("range detail: id=%d replicas=%d, want 1/3", detail.RangeID, len(detail.Replicas))
	}
	leaders := 0
	for _, rep := range detail.Replicas {
		if rep.Error != "" {
			t.Fatalf("replica n%d view errored: %s", rep.NodeID, rep.Error)
		}
		if rep.Status == nil {
			t.Fatalf("replica n%d has no status", rep.NodeID)
		}
		if rep.Status.Leader {
			leaders++
		}
	}
	if leaders != 1 {
		t.Fatalf("range detail shows %d leaders, want exactly 1", leaders)
	}

	// A CA-verified client certificate authenticates without Basic creds.
	if err := security.CreateClientCert(certsDir, "root"); err != nil {
		t.Fatal(err)
	}
	certClient := httpsClient(t, certsDir, "root")
	code, body, _ = authedGet(t, certClient, base+"/api/cluster", "", "")
	if code != http.StatusOK {
		t.Fatal("client-cert auth failed")
	}
	var certDoc server.ClusterStatus
	if err := json.Unmarshal([]byte(body), &certDoc); err != nil {
		t.Fatal(err)
	}
	if p := certDoc.Principal; !p.Secure || p.User != "root" || p.Via != "cert" || !p.Admin {
		t.Fatalf("principal via client certificate: %+v", p)
	}
}

// TestHTTPAuthInsecure: without TLS the endpoints stay open — trust-mode
// parity with pgwire.
func TestHTTPAuthInsecure(t *testing.T) {
	tc := startWithHTTP(t, 1)
	base := "http://" + tc.Nodes[0].HTTPAddr()
	for _, path := range []string{"/", "/metrics", "/status", "/api/cluster", "/api/range?id=1"} {
		if code, _, _ := httpGet(t, base+path); code != http.StatusOK {
			t.Fatalf("insecure %s: %d, want 200", path, code)
		}
	}
	// No identity to report, and every viewer may drill down.
	_, _, body := httpGet(t, base+"/api/cluster")
	var doc server.ClusterStatus
	if err := json.Unmarshal([]byte(body), &doc); err != nil {
		t.Fatal(err)
	}
	if p := doc.Principal; p.Secure || p.User != "" || !p.Admin {
		t.Fatalf("insecure principal: %+v", p)
	}
}

// TestHTTPCertAuthChecksLogin (issue #138): a client certificate opens
// the HTTP endpoints only while its CommonName is a role that may log
// in — NOLOGIN closes the door, LOGIN reopens it, DROP ROLE closes it
// for good — the way pgwire's certificate path already behaved; the
// node's own certificate is admitted throughout.
func TestHTTPCertAuthChecksLogin(t *testing.T) {
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
	base := "https://" + tc.Nodes[0].HTTPAddr()
	var root *pgx.Conn
	deadline := time.Now().Add(30 * time.Second)
	for {
		root, err = connectSecure(ctx, secureURL(tc, certsDir, "root", "topsecret"))
		if err == nil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("root could never authenticate: %v", err)
		}
		time.Sleep(200 * time.Millisecond)
	}
	defer func() { _ = root.Close(ctx) }()
	if _, err := root.Exec(ctx, `CREATE USER certy`); err != nil {
		t.Fatal(err)
	}
	if err := security.CreateClientCert(certsDir, "certy"); err != nil {
		t.Fatal(err)
	}
	certy := httpsClient(t, certsDir, "certy")
	get := func(client *http.Client) int {
		t.Helper()
		code, _, _ := authedGet(t, client, base+"/status", "", "")
		return code
	}
	if code := get(certy); code != http.StatusOK {
		t.Fatalf("certificate of a LOGIN role: %d, want 200", code)
	}
	if _, err := root.Exec(ctx, `ALTER ROLE certy NOLOGIN`); err != nil {
		t.Fatal(err)
	}
	if code := get(certy); code != http.StatusUnauthorized {
		t.Fatalf("certificate of a NOLOGIN role: %d, want 401", code)
	}
	if _, err := root.Exec(ctx, `ALTER ROLE certy LOGIN`); err != nil {
		t.Fatal(err)
	}
	if code := get(certy); code != http.StatusOK {
		t.Fatalf("certificate after LOGIN was restored: %d, want 200", code)
	}
	if _, err := root.Exec(ctx, `DROP ROLE certy`); err != nil {
		t.Fatal(err)
	}
	if code := get(certy); code != http.StatusUnauthorized {
		t.Fatalf("certificate of a dropped role: %d, want 401", code)
	}
	// A refused certificate does not take Basic credentials down with it.
	if code, _, _ := authedGet(t, certy, base+"/status", "root", "topsecret"); code != http.StatusOK {
		t.Fatalf("Basic credentials alongside a refused certificate: %d, want 200", code)
	}
	// A certificate whose CommonName never was a role.
	if err := security.CreateClientCert(certsDir, "stranger"); err != nil {
		t.Fatal(err)
	}
	if code := get(httpsClient(t, certsDir, "stranger")); code != http.StatusUnauthorized {
		t.Fatalf("certificate of a name that is no role: %d, want 401", code)
	}

	// The node certificate (the cluster's own identity, no role
	// descriptor) still reaches the endpoints internode calls need.
	nodeClient := httpsClient(t, certsDir, "")
	nodeCert, err := tls.LoadX509KeyPair(filepath.Join(certsDir, security.NodeCert), filepath.Join(certsDir, security.NodeKeyFile))
	if err != nil {
		t.Fatal(err)
	}
	nodeClient.Transport.(*http.Transport).TLSClientConfig.Certificates = []tls.Certificate{nodeCert}
	if code, _, _ := authedGet(t, nodeClient, base+"/api/range?id=1", "", ""); code != http.StatusOK {
		t.Fatalf("node certificate on an admin endpoint: %d, want 200", code)
	}
}

// TestSchemaAPIResolvesRoles (issue #197): /api/schema must show a
// non-admin the tables it can actually read, not only the ones granted
// to it by name.
//
// The filter was a direct-grant lookup keyed by the principal's own
// username, which is a narrower test than privilege resolution: it
// missed a grant to `public`, a grant to a role the user is a member of,
// and ownership, which is not a row in the privilege map at all. It
// failed closed, so this was never a disclosure — but in the common
// deployment where access is granted through roles rather than to
// individuals, a non-admin opened the schema view and saw nothing it
// could not equally well query from psql.
func TestSchemaAPIResolvesRoles(t *testing.T) {
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

	db, err := connectSecure(ctx, secureURL(tc, certsDir, "root", "topsecret"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close(ctx)
	for _, stmt := range []string{
		// One table per way a user can reach a table without a grant in
		// its own name, plus one it must not see.
		`CREATE TABLE via_role (id INT PRIMARY KEY)`,
		`CREATE TABLE via_public (id INT PRIMARY KEY)`,
		`CREATE TABLE via_none (id INT PRIMARY KEY)`,
		`CREATE ROLE readers`,
		`GRANT SELECT ON via_role TO readers`,
		`GRANT SELECT ON via_public TO public`,
		`CREATE USER alice WITH PASSWORD 'alicepw'`,
		`GRANT readers TO alice`,
	} {
		if _, err := db.Exec(ctx, stmt); err != nil {
			t.Fatalf("%s: %v", stmt, err)
		}
	}

	// The console's answer must match what alice can actually read. The
	// document is rebuilt at most once every schemaCacheFor, so poll for
	// the tables to appear rather than racing the rebuild.
	var doc server.SchemaStatus
	// Filled by each poll below before it is read.
	var seen map[string]bool
	deadline = time.Now().Add(30 * time.Second)
	for {
		code, body, _ := authedGet(t, client, base+"/api/schema", "alice", "alicepw")
		if code != http.StatusOK {
			t.Fatalf("/api/schema as alice: %d", code)
		}
		doc = server.SchemaStatus{}
		if err := jsonUnmarshal([]byte(body), &doc); err != nil {
			t.Fatal(err)
		}
		seen = map[string]bool{}
		for _, tbl := range doc.Tables {
			seen[tbl.Name] = true
		}
		if seen["via_role"] && seen["via_public"] {
			break
		}
		if time.Now().After(deadline) {
			if !seen["via_role"] {
				t.Error("via_role: granted to a role alice is a member of, and not shown")
			}
			if !seen["via_public"] {
				t.Error("via_public: granted to public, which every role holds, and not shown")
			}
			t.Fatalf("alice's schema after 30s: %v", seen)
		}
		time.Sleep(250 * time.Millisecond)
	}
	if seen["via_none"] {
		t.Error("via_none: alice holds nothing on it and it was shown")
	}
	if doc.Users != nil {
		t.Error("a non-admin was shown the user list")
	}

	// The table is visible; the names of everyone else granted on it are
	// not — the same call /api/security makes.
	if _, err := db.Exec(ctx, `CREATE ROLE auditors`); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(ctx, `GRANT SELECT ON via_role TO auditors`); err != nil {
		t.Fatal(err)
	}
	deadline = time.Now().Add(30 * time.Second)
	for {
		code, body, _ := authedGet(t, client, base+"/api/schema", "alice", "alicepw")
		doc = server.SchemaStatus{}
		if code == http.StatusOK && jsonUnmarshal([]byte(body), &doc) == nil {
			for _, tbl := range doc.Tables {
				if tbl.Name != "via_role" {
					continue
				}
				if _, leaked := tbl.Privileges["auditors"]; leaked {
					t.Fatal("via_role names auditors to alice, who is not a member of it")
				}
				if _, own := tbl.Privileges["readers"]; own {
					return // alice sees the grant she holds, and not the other
				}
			}
		}
		if time.Now().After(deadline) {
			t.Fatalf("alice never saw her own grant on via_role: %+v", doc.Tables)
		}
		time.Sleep(250 * time.Millisecond)
	}
}

// TestExplainPlansAsTheCaller (issue #193): /api/explain produced its
// plan through the node's own system session, so the plan a console
// button returned was not the one the operator's query would produce —
// different privileges, and a session that bypasses grants and may
// create system tables. Every other view filters what it shows by what
// the caller may see; this one did not, and said nothing about it.
func TestExplainPlansAsTheCaller(t *testing.T) {
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

	db, err := connectSecure(ctx, secureURL(tc, certsDir, "root", "topsecret"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close(ctx)
	for _, stmt := range []string{
		`CREATE TABLE plans (id INT PRIMARY KEY, v INT)`,
		`INSERT INTO plans VALUES (1, 1)`,
		`CREATE USER carol WITH PASSWORD 'carolpw'`,
		`GRANT admin TO carol`, // /api/explain is admin-gated
	} {
		if _, err := db.Exec(ctx, stmt); err != nil {
			t.Fatalf("%s: %v", stmt, err)
		}
	}
	// Run a statement so the shape is in this node's accounting: the
	// endpoint explains what it recorded, never text from the request.
	if _, err := db.Exec(ctx, `SELECT id FROM plans WHERE v = 1`); err != nil {
		t.Fatal(err)
	}

	// Find the shape, then explain it as carol.
	var fp string
	deadline = time.Now().Add(30 * time.Second)
	for fp == "" {
		code, body, _ := authedGet(t, client, base+"/api/statements", "carol", "carolpw")
		if code == http.StatusOK {
			var doc server.StatementsStatus
			if jsonUnmarshal([]byte(body), &doc) == nil {
				for _, sh := range doc.Statements {
					// The SELECT specifically: EXPLAIN describes a query
					// plan, and the other shapes on this table are DDL.
					if sh.Kind == "select" && strings.Contains(sh.Shape, "plans") {
						fp = sh.Fingerprint
					}
				}
			}
		}
		if fp == "" && time.Now().After(deadline) {
			t.Fatalf("no statement shape for the test query after 30s; last body: %.400s", body)
		}
		if fp == "" {
			time.Sleep(250 * time.Millisecond)
		}
	}

	code, body, _ := authedGet(t, client, base+"/api/explain?fingerprint="+fp, "carol", "carolpw")
	if code != http.StatusOK {
		t.Fatalf("/api/explain as carol: %d %s", code, body)
	}
	var ex server.ExplainStatus
	if err := jsonUnmarshal([]byte(body), &ex); err != nil {
		t.Fatal(err)
	}
	if ex.Error != "" {
		t.Fatalf("explain error: %s", ex.Error)
	}
	if ex.PlannedAs != "carol" {
		t.Errorf("plan produced as %q, want carol: the console must not have to assume whose privileges a plan reflects", ex.PlannedAs)
	}
	if len(ex.Plan) == 0 {
		t.Error("no plan returned")
	}
}

// TestLoginIsRateLimited (issue #195): /api/login is reachable before
// any credential is validated and each attempt re-derives PBKDF2 —
// measured at 0.72 ms of CPU. Unbounded, that is a pre-authentication
// amplification: a request an attacker sends for nothing costs the node
// a slice of a core, so a handful of connections pins it, against a
// listener that must stay reachable for the console to work.
//
// The refusal has to come before the derivation, so this asserts on
// latency as well as on the status code: a 429 issued after the work has
// already run would satisfy a status-code test and none of the point.
func TestLoginIsRateLimited(t *testing.T) {
	httpLis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	tc, certsDir := startSecureCluster(t, "topsecret", func(i int, cfg *server.Config) {
		if i == 0 {
			cfg.HTTPListener = httpLis
		}
	})
	_ = tc
	base := "https://" + tc.Nodes[0].HTTPAddr()
	client := httpsClient(t, certsDir, "")

	post := func(user, pass string) (int, time.Duration) {
		t.Helper()
		body := `{"user":` + jsonQuote(user) + `,"password":` + jsonQuote(pass) + `}`
		req, err := http.NewRequest(http.MethodPost, base+"/api/login", strings.NewReader(body))
		if err != nil {
			t.Fatal(err)
		}
		req.Header.Set("Content-Type", "application/json")
		start := time.Now()
		resp, err := client.Do(req)
		took := time.Since(start)
		if err != nil {
			t.Fatal(err)
		}
		io.Copy(io.Discard, resp.Body)
		resp.Body.Close()
		return resp.StatusCode, took
	}

	// Wait for auth to be answering at all.
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

	// Guess until the limiter refuses, then confirm the refusal is
	// cheap — it must not have run the derivation.
	var throttled int
	var throttledTime time.Duration
	for i := 0; i < 60 && throttled < 3; i++ {
		code, took := post("root", "wrong-password")
		if code == http.StatusTooManyRequests {
			throttled++
			throttledTime += took
			continue
		}
		if code != http.StatusUnauthorized {
			t.Fatalf("attempt %d: %d, want 401 or 429", i, code)
		}
	}
	if throttled < 3 {
		t.Fatal("60 wrong passwords from one source were never refused: the endpoint is unbounded")
	}

	// A refused attempt is answered without the PBKDF2 derivation. The
	// derivation is milliseconds; a refusal is a map lookup, so an
	// average well under a millisecond of server time is the signal.
	// Network and TLS dominate what is measured here, so the bar is
	// deliberately loose: it is checking that the work was skipped, not
	// how fast the machine is.
	if avg := throttledTime / time.Duration(throttled); avg > 50*time.Millisecond {
		t.Errorf("refused attempts averaged %s; a refusal should not be doing the work it refuses", avg)
	}

	// A legitimate sign-in still gets through. Everything in this test
	// shares one source address, so the guessing above drained the
	// source's budget — the documented cost of keying on address, and
	// why the budget refills: within a couple of seconds root is in, and
	// succeeding restores the budget rather than spending it.
	deadline = time.Now().Add(10 * time.Second)
	for {
		code, _, _ := authedGet(t, client, base+"/status", "root", "topsecret")
		if code == http.StatusOK {
			break
		}
		if code != http.StatusTooManyRequests {
			t.Fatalf("legitimate sign-in: %d, want 200 (or 429 while the budget refills)", code)
		}
		if time.Now().After(deadline) {
			t.Fatal("a legitimate sign-in never got through: the budget does not refill")
		}
		time.Sleep(250 * time.Millisecond)
	}
	// Having succeeded, root is not left one attempt from being throttled
	// again.
	for i := 0; i < 3; i++ {
		if code, _, _ := authedGet(t, client, base+"/status", "root", "topsecret"); code != http.StatusOK {
			t.Fatalf("sign-in %d after a success: %d — proving a credential must not spend the guessing budget", i, code)
		}
	}

	// The refusals reach the console (issue #203). A counter the node
	// keeps and never shows is the gap this asserts is closed.
	code, body, _ := authedGet(t, client, base+"/api/security", "root", "topsecret")
	if code != http.StatusOK {
		t.Fatalf("/api/security: %d", code)
	}
	var sec server.SecurityStatus
	if err := jsonUnmarshal([]byte(body), &sec); err != nil {
		t.Fatal(err)
	}
	if sec.AuthThrottled < float64(throttled) {
		t.Errorf("/api/security reports %v refusals, but %d were observed: the counter is not reaching the document",
			sec.AuthThrottled, throttled)
	}
	if sec.ThrottledRateLimit == 0 {
		t.Error("the rate-limit cause is zero after the limiter refused: the two causes are not being counted apart")
	}
	if sec.AuthThrottled != sec.ThrottledRateLimit+sec.ThrottledVerifyFull {
		t.Errorf("total %v is not the sum of its causes (%v + %v)",
			sec.AuthThrottled, sec.ThrottledRateLimit, sec.ThrottledVerifyFull)
	}
}

// TestAuthThrottlingIsVisibleAndRaisesAProblem (issue #203): the refusals
// are counted for everyone who may read the security document — they
// name nobody, so gating them on the admin role would hide a node's own
// state from the person signed in to it — and sustained throttling
// reaches the problems panel as a rate rather than as a total that would
// stay amber forever after one bad afternoon.
func TestAuthThrottlingIsVisibleAndRaisesAProblem(t *testing.T) {
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

	db, err := connectSecure(ctx, secureURL(tc, certsDir, "root", "topsecret"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close(ctx)
	if _, err := db.Exec(ctx, `CREATE USER watcher WITH PASSWORD 'watcherpw'`); err != nil {
		t.Fatal(err)
	}

	// Seed the health window before any throttling, so the burst below
	// falls inside a window the rate is measured over. Without this the
	// first sample would already include the burst and the rate would
	// be zero — which is what the check is meant to avoid reporting.
	if code, _, _ := authedGet(t, client, base+"/api/health", "root", "topsecret"); code != http.StatusOK {
		t.Fatalf("/api/health seed: %d", code)
	}

	// Hammer hard enough that the rate is unambiguous: the threshold is
	// one refusal a second over the window, and this produces hundreds
	// in a few seconds.
	post := func() int {
		body := `{"user":"watcher","password":"wrong"}`
		req, err := http.NewRequest(http.MethodPost, base+"/api/login", strings.NewReader(body))
		if err != nil {
			t.Fatal(err)
		}
		req.Header.Set("Content-Type", "application/json")
		resp, err := client.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		io.Copy(io.Discard, resp.Body)
		resp.Body.Close()
		return resp.StatusCode
	}
	refused := 0
	for stop := time.Now().Add(4 * time.Second); time.Now().Before(stop); {
		if post() == http.StatusTooManyRequests {
			refused++
		}
	}
	if refused < 10 {
		t.Fatalf("only %d refusals in four seconds of guessing: the limiter is not engaging, so this test proves nothing", refused)
	}

	// A non-admin sees the figure. It is this node's own count of its
	// own refusals and names nobody.
	code, body, _ := authedGet(t, client, base+"/api/security", "watcher", "watcherpw")
	if code != http.StatusOK {
		t.Fatalf("/api/security as a non-admin: %d", code)
	}
	var sec server.SecurityStatus
	if err := jsonUnmarshal([]byte(body), &sec); err != nil {
		t.Fatal(err)
	}
	if sec.Principal.Admin {
		t.Fatal("watcher should not hold the admin role; this test is not proving what it claims")
	}
	if sec.AuthThrottled == 0 {
		t.Error("a non-admin sees no throttle count: the figure names nobody and should not be gated")
	}

	// And it reaches the problems panel, as a rate.
	deadline = time.Now().Add(30 * time.Second)
	for {
		code, body, _ := authedGet(t, client, base+"/api/health", "root", "topsecret")
		if code != http.StatusOK {
			t.Fatalf("/api/health: %d", code)
		}
		var h server.HealthStatus
		if err := jsonUnmarshal([]byte(body), &h); err != nil {
			t.Fatal(err)
		}
		found := false
		for _, p := range h.Problems {
			if p.Check == "auth-throttled" {
				found = true
				if p.Section != "events" {
					t.Errorf("auth-throttled points at section %q, want events", p.Section)
				}
			}
		}
		if found {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("sustained throttling (%d refusals) never raised an auth-throttled problem", refused)
		}
		time.Sleep(500 * time.Millisecond)
	}
}

func jsonQuote(s string) string {
	b, _ := json.Marshal(s)
	return string(b)
}
