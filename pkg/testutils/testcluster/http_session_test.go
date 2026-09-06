package testcluster

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net"
	"net/http"
	"net/http/cookiejar"
	"net/url"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/sthorne/datax/pkg/security"
	"github.com/sthorne/datax/pkg/server"
)

// postJSON sends a JSON body and returns the status, body and response.
func postJSON(t *testing.T, client *http.Client, url string, doc any) (int, string, *http.Response) {
	t.Helper()
	var body []byte
	if doc != nil {
		var err error
		if body, err = json.Marshal(doc); err != nil {
			t.Fatal(err)
		}
	}
	req, err := http.NewRequest("POST", url, bytes.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := client.Do(req)
	if err != nil {
		t.Fatalf("POST %s: %v", url, err)
	}
	defer func() { _ = resp.Body.Close() }()
	raw, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	return resp.StatusCode, string(raw), resp
}

// getWith performs a GET with the given headers.
func getWith(t *testing.T, client *http.Client, url string, headers map[string]string) (int, string, http.Header) {
	t.Helper()
	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		t.Fatal(err)
	}
	for k, v := range headers {
		req.Header.Set(k, v)
	}
	resp, err := client.Do(req)
	if err != nil {
		t.Fatalf("GET %s: %v", url, err)
	}
	defer func() { _ = resp.Body.Close() }()
	raw, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	return resp.StatusCode, string(raw), resp.Header
}

// sessionCookie returns the datax_session cookie the jar holds for base,
// or "" when it holds none.
func sessionCookie(t *testing.T, client *http.Client, base string) string {
	t.Helper()
	u, err := url.Parse(base)
	if err != nil {
		t.Fatal(err)
	}
	for _, c := range client.Jar.Cookies(u) {
		if c.Name == "datax_session" {
			return c.Value
		}
	}
	return ""
}

// TestHTTPSession (issue #158): the console's front door. A password
// exchanged at /api/login authenticates later requests by cookie on any
// node of the cluster; signing out ends it; a tampered or foreign token
// is refused; and the two doors machines use — HTTP Basic and client
// certificates — are untouched.
func TestHTTPSession(t *testing.T) {
	httpLis := make([]net.Listener, 2)
	for i := range httpLis {
		lis, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		httpLis[i] = lis
	}
	tc, certsDir := startSecureCluster(t, "topsecret", func(i int, cfg *server.Config) {
		if i < len(httpLis) {
			cfg.HTTPListener = httpLis[i]
		}
	})
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()
	base := "https://" + tc.Nodes[0].HTTPAddr()
	other := "https://" + tc.Nodes[1].HTTPAddr()

	// A browser: trusts the CA, presents no client certificate, keeps
	// cookies.
	jar, err := cookiejar.New(nil)
	if err != nil {
		t.Fatal(err)
	}
	client := httpsClient(t, certsDir, "")
	client.Jar = jar

	// Root's credential is seeded asynchronously.
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

	// An unauthenticated browser navigation gets the login page — 401,
	// but no challenge, so no native credential dialog — while anything
	// scripted still gets the challenge it always got.
	code, body, hdr := getWith(t, client, base+"/", map[string]string{"Accept": "text/html,application/xhtml+xml,*/*;q=0.8"})
	if code != http.StatusUnauthorized || hdr.Get("WWW-Authenticate") != "" {
		t.Fatalf("unauthenticated browser navigation: %d, challenge %q; want 401 with none", code, hdr.Get("WWW-Authenticate"))
	}
	if !strings.Contains(hdr.Get("Content-Type"), "text/html") || !strings.Contains(body, `id="form"`) || !strings.Contains(body, "/api/login") {
		t.Fatalf("unauthenticated browser navigation did not get the login page: %s", firstLine(body))
	}
	if strings.Contains(body, "__LOGIN_CONTEXT__") {
		t.Fatal("the login page still carries its context placeholder")
	}
	// It names the cluster and node the operator actually reached.
	if !strings.Contains(body, tc.Nodes[0].Addr()) {
		t.Fatalf("the login page does not name the node it was served by: %s", firstLine(body))
	}
	// Airgap: the sign-in page is as self-contained as the console, and
	// it is the page an operator sees before anything else works.
	if re := regexp.MustCompile(`(https?:)?//[a-zA-Z0-9.-]+\.[a-z]{2,}`); re.MatchString(body) {
		t.Fatalf("the login page references an external host: %q", re.FindString(body))
	}
	for _, tag := range []string{"<script src=", "<link ", "@import", "url("} {
		if strings.Contains(body, tag) {
			t.Fatalf("the login page loads an external asset via %q", tag)
		}
	}
	// It never leaks cluster state to an unauthenticated caller: only
	// what identifies the endpoint.
	for _, leak := range []string{"leader_qps", "replica_bytes", "\"ranges\"", "\"nodes\""} {
		if strings.Contains(body, leak) {
			t.Fatalf("the login page carries cluster state (%s)", leak)
		}
	}
	for _, path := range []string{"/api/cluster", "/status", "/metrics"} {
		code, _, hdr := getWith(t, client, base+path, map[string]string{"Accept": "*/*"})
		if code != http.StatusUnauthorized || hdr.Get("WWW-Authenticate") == "" {
			t.Fatalf("unauthenticated %s: %d, challenge %q; want 401 with a challenge", path, code, hdr.Get("WWW-Authenticate"))
		}
	}

	// Signing in: a wrong password, an unknown user and a user that
	// exists but cannot sign in are one indistinguishable refusal.
	conn, err := connectSecure(ctx, secureURL(tc, certsDir, "root", "topsecret"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := conn.Exec(ctx, `CREATE USER ops PASSWORD 'ops-pw'`); err != nil {
		t.Fatal(err)
	}
	if _, err := conn.Exec(ctx, `CREATE USER certonly NOLOGIN`); err != nil {
		t.Fatal(err)
	}
	var refusals []string
	for _, tc := range []struct{ user, pass string }{
		{"root", "wrong"},
		{"nobody", "topsecret"},
		{"certonly", "topsecret"},
	} {
		code, body, _ := postJSON(t, client, base+"/api/login", map[string]string{"user": tc.user, "password": tc.pass})
		if code != http.StatusUnauthorized {
			t.Fatalf("sign-in as %s/%s: %d, want 401", tc.user, tc.pass, code)
		}
		refusals = append(refusals, body)
		if c := sessionCookie(t, client, base); c != "" {
			t.Fatalf("a refused sign-in set a cookie: %q", c)
		}
	}
	for _, r := range refusals[1:] {
		if r != refusals[0] {
			t.Fatalf("sign-in refusals differ and so enumerate users:\n%s\n%s", refusals[0], r)
		}
	}

	// Sign-in is POST-only and requires a JSON content type: with
	// SameSite=Strict that is what keeps another origin from driving it.
	if code, _, _ := getWith(t, client, base+"/api/login", nil); code != http.StatusMethodNotAllowed {
		t.Fatalf("GET /api/login: %d, want 405", code)
	}
	formReq, err := http.NewRequest("POST", base+"/api/login", strings.NewReader("user=root&password=topsecret"))
	if err != nil {
		t.Fatal(err)
	}
	formReq.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	formResp, err := client.Do(formReq)
	if err != nil {
		t.Fatal(err)
	}
	_ = formResp.Body.Close()
	if formResp.StatusCode != http.StatusUnsupportedMediaType {
		t.Fatalf("form-encoded sign-in: %d, want 415", formResp.StatusCode)
	}

	// A real sign-in: the cookie is set with the attributes a session
	// cookie must carry, and it authenticates the endpoints afterwards
	// with no password on the wire.
	code, body, resp := postJSON(t, client, base+"/api/login", map[string]string{"user": "ops", "password": "ops-pw"})
	if code != http.StatusOK {
		t.Fatalf("sign-in as ops: %d (%s)", code, body)
	}
	var set *http.Cookie
	for _, c := range resp.Cookies() {
		if c.Name == "datax_session" {
			set = c
		}
	}
	if set == nil {
		t.Fatal("sign-in set no session cookie")
	}
	if !set.HttpOnly || !set.Secure || set.SameSite != http.SameSiteStrictMode || set.Path != "/" {
		t.Fatalf("session cookie attributes: HttpOnly=%v Secure=%v SameSite=%v Path=%q", set.HttpOnly, set.Secure, set.SameSite, set.Path)
	}
	if set.MaxAge <= 0 {
		t.Fatalf("session cookie has no bounded lifetime: MaxAge=%d", set.MaxAge)
	}
	if strings.Contains(set.Value, "ops-pw") {
		t.Fatal("the session token carries the password")
	}
	var lr struct {
		User      string `json:"user"`
		Admin     bool   `json:"admin"`
		ExpiresAt int64  `json:"expires_at_unix_ms"`
	}
	if err := json.Unmarshal([]byte(body), &lr); err != nil {
		t.Fatal(err)
	}
	if lr.User != "ops" || lr.Admin || lr.ExpiresAt <= time.Now().UnixMilli() {
		t.Fatalf("sign-in response: %+v", lr)
	}

	// The cookie authenticates, and says so: via "session", with the
	// expiry the console prompts against.
	principal := func(client *http.Client, base string) server.ClusterPrincipal {
		t.Helper()
		code, body, _ := getWith(t, client, base+"/api/cluster", nil)
		if code != http.StatusOK {
			t.Fatalf("/api/cluster: %d (%s)", code, firstLine(body))
		}
		var doc server.ClusterStatus
		if err := json.Unmarshal([]byte(body), &doc); err != nil {
			t.Fatal(err)
		}
		return doc.Principal
	}
	if p := principal(client, base); !p.Secure || p.User != "ops" || p.Via != "session" || p.Admin || p.SessionExpiresAt <= time.Now().UnixMilli() {
		t.Fatalf("principal from the cookie: %+v", p)
	}
	// A signed-in browser navigation gets the console, not the login page.
	if code, body, _ := getWith(t, client, base+"/", map[string]string{"Accept": "text/html"}); code != http.StatusOK || !strings.Contains(body, "/api/overview") {
		t.Fatalf("signed-in navigation: %d, %s", code, firstLine(body))
	}
	// Authorization is still the catalog's business, not the token's.
	if code, _, _ := getWith(t, client, base+"/api/range?id=1", nil); code != http.StatusForbidden {
		t.Fatalf("/api/range as a non-admin session: %d, want 403", code)
	}

	// Any node accepts it: the signing key is derived from cluster state,
	// not minted per process.
	if p := principal(client, other); p.User != "ops" || p.Via != "session" {
		t.Fatalf("the token was not accepted by another node: %+v", p)
	}

	// Signing out ends it, on both nodes.
	if code, body, _ := postJSON(t, client, base+"/api/logout", nil); code != http.StatusOK {
		t.Fatalf("sign-out: %d (%s)", code, body)
	}
	if c := sessionCookie(t, client, base); c != "" {
		t.Fatalf("sign-out left a cookie: %q", c)
	}
	if code, _, hdr := getWith(t, client, base+"/api/cluster", nil); code != http.StatusUnauthorized || hdr.Get("WWW-Authenticate") == "" {
		t.Fatalf("after sign-out: %d, challenge %q; want 401 with a challenge", code, hdr.Get("WWW-Authenticate"))
	}

	// A forged or foreign token authenticates nothing, and the response
	// clears it so a browser stops sending it.
	stolen := set.Value
	for _, tc := range []struct{ name, token string }{
		{"tampered user", strings.Replace(stolen, "b3Bz", "cm9vdA", 1)},
		{"garbage", "not-a-token"},
		{"empty payload", "."},
		{"resigned by nobody", stolen[:len(stolen)-4] + "AAAA"},
	} {
		req, err := http.NewRequest("GET", base+"/api/cluster", nil)
		if err != nil {
			t.Fatal(err)
		}
		req.AddCookie(&http.Cookie{Name: "datax_session", Value: tc.token})
		resp, err := client.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		_, _ = io.Copy(io.Discard, resp.Body)
		_ = resp.Body.Close()
		if resp.StatusCode != http.StatusUnauthorized {
			t.Fatalf("%s: %d, want 401", tc.name, resp.StatusCode)
		}
		cleared := false
		for _, c := range resp.Cookies() {
			if c.Name == "datax_session" && c.MaxAge < 0 {
				cleared = true
			}
		}
		if !cleared {
			t.Fatalf("%s: the stale cookie was not cleared", tc.name)
		}
	}

	// The untouched doors: Basic still works everywhere, a client
	// certificate still authenticates, and /metrics still takes a
	// metrics-role scrape account with no cookie in sight.
	if _, err := conn.Exec(ctx, `GRANT metrics TO ops`); err != nil {
		t.Fatal(err)
	}
	_ = conn.Close(ctx)
	plain := httpsClient(t, certsDir, "")
	for _, path := range []string{"/", "/status", "/api/cluster", "/metrics"} {
		if code, _, _ := authedGet(t, plain, base+path, "ops", "ops-pw"); code != http.StatusOK {
			t.Fatalf("HTTP Basic on %s after sessions exist: %d", path, code)
		}
	}
	if p := principalOfClient(t, plain, base, "ops", "ops-pw"); p.Via != "basic" {
		t.Fatalf("Basic credentials now report via %q", p.Via)
	}
	if err := security.CreateClientCert(certsDir, "ops"); err != nil {
		t.Fatal(err)
	}
	certClient := httpsClient(t, certsDir, "ops")
	if code, _, _ := getWith(t, certClient, base+"/api/cluster", nil); code != http.StatusOK {
		t.Fatalf("client certificate after sessions exist: %d", code)
	}
}

// principalOfClient reads /api/cluster's principal with Basic credentials.
func principalOfClient(t *testing.T, client *http.Client, base, user, pass string) server.ClusterPrincipal {
	t.Helper()
	code, body, _ := authedGet(t, client, base+"/api/cluster", user, pass)
	if code != http.StatusOK {
		t.Fatalf("/api/cluster as %s: %d", user, code)
	}
	var doc server.ClusterStatus
	if err := json.Unmarshal([]byte(body), &doc); err != nil {
		t.Fatal(err)
	}
	return doc.Principal
}

// TestHTTPSessionInsecureMode: with no certificates there is no identity
// to establish, so signing in is refused with an explanation rather than
// minting a token that would mean nothing.
func TestHTTPSessionInsecureMode(t *testing.T) {
	tc := startWithHTTP(t, 1)
	base := "http://" + tc.Nodes[0].HTTPAddr()
	client := &http.Client{Timeout: 10 * time.Second}
	code, body, _ := postJSON(t, client, base+"/api/login", map[string]string{"user": "root", "password": ""})
	if code != http.StatusBadRequest || !strings.Contains(body, "insecure mode") {
		t.Fatalf("sign-in in insecure mode: %d (%s)", code, body)
	}
	// Everything is still open, and the console still loads.
	if code, _, _ := getWith(t, client, base+"/api/cluster", nil); code != http.StatusOK {
		t.Fatalf("/api/cluster in insecure mode: %d", code)
	}
	if code, _, _ := getWith(t, client, base+"/", map[string]string{"Accept": "text/html"}); code != http.StatusOK {
		t.Fatalf("/ in insecure mode: %d", code)
	}
}

func firstLine(s string) string {
	if i := strings.IndexByte(s, '\n'); i >= 0 {
		return s[:i]
	}
	if len(s) > 200 {
		return s[:200]
	}
	return s
}
