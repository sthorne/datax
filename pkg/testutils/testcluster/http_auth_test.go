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
