package testcluster

import (
	"context"
	"encoding/json"
	"net"
	"net/http"
	"regexp"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/sthorne/datax/pkg/security"
	"github.com/sthorne/datax/pkg/server"
)

// TestCertExpiryMetric (issue #156): silently expiring certificates are
// a well-known way to lose a cluster. The expiry is published as a gauge
// so Prometheus can alert on it whether or not anyone opens the console
// — and it is absent, rather than zero, on a cluster that has no
// certificates, because a series asserting "nothing expires" would be a
// lie an alert could not tell from the truth.
func TestCertExpiryMetric(t *testing.T) {
	httpLis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	tc, certsDir := startSecureCluster(t, "topsecret", func(i int, cfg *server.Config) {
		if i == 0 {
			cfg.HTTPListener = httpLis
		}
	})
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

	code, body, _ := authedGet(t, client, base+"/metrics", "root", "topsecret")
	if code != http.StatusOK {
		t.Fatalf("/metrics: %d %s", code, body)
	}
	// Both the node's own certificate and the CA every node trusts.
	line := regexp.MustCompile(`datax_cert_expiry_seconds\{kind="([a-z]+)",subject="([^"]*)"\} ([0-9.e+-]+)`)
	seen := map[string]float64{}
	for _, m := range line.FindAllStringSubmatch(body, -1) {
		v, perr := strconv.ParseFloat(m[3], 64)
		if perr != nil {
			t.Fatalf("unparseable value %q", m[3])
		}
		seen[m[1]] = v
	}
	for _, kind := range []string{"ca", "node"} {
		v, ok := seen[kind]
		if !ok {
			t.Fatalf("no datax_cert_expiry_seconds for kind=%q; got %v", kind, seen)
		}
		if v <= 0 {
			t.Fatalf("kind=%q reports %v seconds left: a freshly created certificate has not expired", kind, v)
		}
	}
	// The CA outlives the node certificate it signed, which is what the
	// tooling issues; if that ever inverts, the alert thresholds are
	// pointing at the wrong one.
	if seen["ca"] <= seen["node"] {
		t.Errorf("the CA (%v) should outlive the node certificate (%v)", seen["ca"], seen["node"])
	}

	// A fresh cluster's certificates are years out, so the health check
	// runs and finds nothing: the check is wired without crying wolf.
	code, body, _ = authedGet(t, client, base+"/api/health", "root", "topsecret")
	if code != http.StatusOK {
		t.Fatalf("/api/health: %d %s", code, body)
	}
	var hd server.HealthStatus
	if err := json.Unmarshal([]byte(body), &hd); err != nil {
		t.Fatal(err)
	}
	if hd.Checks == 0 {
		t.Fatal("no checks ran")
	}
	for _, p := range hd.Problems {
		if strings.HasPrefix(p.Check, "cert-") {
			t.Errorf("a certificate valid for years produced %s: %s", p.Check, p.Summary)
		}
	}
}

// The same series must be absent from an insecure cluster.
func TestCertExpiryMetricAbsentInsecure(t *testing.T) {
	tc := startWithHTTP(t, 1)
	code, _, body := httpGet(t, "http://"+tc.Nodes[0].HTTPAddr()+"/metrics")
	if code != 200 {
		t.Fatalf("/metrics: %d", code)
	}
	if strings.Contains(body, "datax_cert_expiry_seconds") {
		t.Fatal("an insecure cluster has no certificates and must publish no expiry series")
	}
	// And /api/security says so rather than showing an empty table with
	// no explanation.
	code, _, body = httpGet(t, "http://"+tc.Nodes[0].HTTPAddr()+"/api/security")
	if code != 200 {
		t.Fatalf("/api/security: %d %s", code, body)
	}
	var doc server.SecurityStatus
	if err := json.Unmarshal([]byte(body), &doc); err != nil {
		t.Fatal(err)
	}
	if doc.Secure {
		t.Fatal("an insecure cluster reported itself secure")
	}
	if len(doc.Certificates) != 0 {
		t.Fatalf("insecure mode has no certificates: %+v", doc.Certificates)
	}
	// Roles still exist, and the built-ins are always among them.
	if len(doc.Roles) == 0 {
		t.Fatal("no roles reported")
	}
}

// TestSecurityViewGating (issue #156): what a certificate expires is
// operational and belongs to any authenticated user; who has been
// connecting names people and travels with the admin surface. This is
// the line, checked from both sides.
func TestSecurityViewGating(t *testing.T) {
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
	if _, err := rootConn.Exec(ctx, `CREATE USER scraper PASSWORD 'metrics-pw'`); err != nil {
		t.Fatalf("create scraper: %v", err)
	}
	// A role that is a group, and a member of it, so the roles table has
	// membership to resolve rather than a flat list.
	if _, err := rootConn.Exec(ctx, `CREATE ROLE readers`); err != nil {
		t.Fatalf("create readers: %v", err)
	}
	if _, err := rootConn.Exec(ctx, `GRANT readers TO scraper`); err != nil {
		t.Fatalf("grant readers: %v", err)
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

	fetch := func(user, pass string) server.SecurityStatus {
		t.Helper()
		code, body, _ := authedGet(t, client, base+"/api/security", user, pass)
		if code != http.StatusOK {
			t.Fatalf("/api/security as %s: %d %s", user, code, body)
		}
		var doc server.SecurityStatus
		if err := json.Unmarshal([]byte(body), &doc); err != nil {
			t.Fatal(err)
		}
		return doc
	}

	admin := fetch("root", "topsecret")
	other := fetch("scraper", "metrics-pw")

	// Open to both: the cluster is secure, and the certificates that
	// will one day stop it are named with their dates.
	for name, doc := range map[string]server.SecurityStatus{"root": admin, "scraper": other} {
		if !doc.Secure {
			t.Errorf("%s: a secure cluster reported itself insecure", name)
		}
		kinds := map[string]bool{}
		for _, c := range doc.Certificates {
			kinds[c.Kind] = true
			if c.NotAfter.IsZero() {
				t.Errorf("%s: certificate %q carries no expiry", name, c.Subject)
			}
		}
		if !kinds["ca"] || !kinds["node"] {
			t.Errorf("%s: expected the CA and the node certificate, got %+v", name, doc.Certificates)
		}
		// Roles are not secret to an authenticated user, and membership
		// resolves rather than being left to be walked by hand.
		var readersSeen, scraperSeen bool
		for _, r := range doc.Roles {
			if r.Name == "readers" {
				readersSeen = true
			}
			if r.Name == "scraper" {
				scraperSeen = true
				found := false
				for _, m := range r.MemberOf {
					if m == "readers" {
						found = true
					}
				}
				if !found {
					t.Errorf("%s: scraper's membership of readers is not reported: %+v", name, r)
				}
			}
		}
		if !readersSeen || !scraperSeen {
			t.Errorf("%s: roles missing from the document: %+v", name, doc.Roles)
		}
		builtin := 0
		for _, r := range doc.Roles {
			if r.Builtin {
				builtin++
			}
		}
		if builtin == 0 {
			t.Errorf("%s: the built-in roles are not listed", name)
		}
	}

	// Admin only: who is connected and by what.
	if len(admin.Connections) == 0 {
		t.Errorf("root is connected over SQL and the breakdown is empty: %+v", admin)
	}
	viaSeen := false
	for _, c := range admin.Connections {
		for via := range c.Via {
			if via == "scram" || via == "cert" {
				viaSeen = true
			}
		}
	}
	if !viaSeen {
		t.Errorf("no connection reports how it authenticated: %+v", admin.Connections)
	}
	if len(other.Connections) != 0 {
		t.Errorf("a non-admin was given the per-user connection breakdown: %+v", other.Connections)
	}
	if len(other.ClientCerts) != 0 {
		t.Errorf("a non-admin was given the observed client certificates: %+v", other.ClientCerts)
	}

	// A client certificate presented to the HTTP listener is reported to
	// an admin — what has been connecting, from the material the node
	// already verified.
	if err := security.CreateClientCert(certsDir, "scraper"); err != nil {
		t.Fatalf("issuing a client certificate: %v", err)
	}
	certClient := httpsClient(t, certsDir, "scraper")
	if code, _, _ := getWith(t, certClient, base+"/api/security", nil); code != http.StatusOK {
		t.Fatalf("client-certificate request refused: %d", code)
	}
	admin = fetch("root", "topsecret")
	found := false
	for _, c := range admin.ClientCerts {
		if c.Kind == "client" && c.Subject == "scraper" {
			found = true
			if c.NotAfter.IsZero() {
				t.Errorf("the observed client certificate carries no expiry: %+v", c)
			}
		}
	}
	if !found {
		t.Errorf("a client certificate was presented and is not reported: %+v", admin.ClientCerts)
	}
	if len(other.ClientCerts) != 0 {
		t.Errorf("a non-admin still must not see them: %+v", other.ClientCerts)
	}
}
