package testcluster

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"io"
	"net"
	"net/http"
	"os"
	"path/filepath"
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

	// Any valid user works — endpoints are read-only, admin not required.
	conn, err := connectSecure(ctx, secureURL(tc, certsDir, "root", "topsecret"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := conn.Exec(ctx, `CREATE USER scraper PASSWORD 'metrics-pw'`); err != nil {
		t.Fatal(err)
	}
	_ = conn.Close(ctx)
	deadline = time.Now().Add(30 * time.Second)
	for {
		code, _, _ := authedGet(t, client, base+"/metrics", "scraper", "metrics-pw")
		if code == http.StatusOK {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("non-admin user never authenticated (last code %d)", code)
		}
		time.Sleep(200 * time.Millisecond)
	}
	if code, _, _ := authedGet(t, client, base+"/metrics", "scraper", "wrong"); code != http.StatusUnauthorized {
		t.Fatal("scraper with wrong password accepted")
	}

	// A CA-verified client certificate authenticates without Basic creds.
	if err := security.CreateClientCert(certsDir, "root"); err != nil {
		t.Fatal(err)
	}
	certClient := httpsClient(t, certsDir, "root")
	if code, _, _ := authedGet(t, certClient, base+"/api/cluster", "", ""); code != http.StatusOK {
		t.Fatal("client-cert auth failed")
	}
}

// TestHTTPAuthInsecure: without TLS the endpoints stay open — trust-mode
// parity with pgwire.
func TestHTTPAuthInsecure(t *testing.T) {
	tc := startWithHTTP(t, 1)
	base := "http://" + tc.Nodes[0].HTTPAddr()
	for _, path := range []string{"/", "/metrics", "/status", "/api/cluster"} {
		if code, _, _ := httpGet(t, base+path); code != http.StatusOK {
			t.Fatalf("insecure %s: %d, want 200", path, code)
		}
	}
}
