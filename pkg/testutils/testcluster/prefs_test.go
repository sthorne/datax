package testcluster

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/sthorne/datax/pkg/keys"
	"github.com/sthorne/datax/pkg/server"
	"github.com/sthorne/datax/pkg/sql"
	"github.com/sthorne/datax/pkg/sql/catalog"
	"github.com/sthorne/datax/pkg/version"
)

// httpSend performs one request with a body and returns status and body.
func httpSend(t *testing.T, method, url, contentType, body string) (int, string) {
	t.Helper()
	req, err := http.NewRequest(method, url, bytes.NewReader([]byte(body)))
	if err != nil {
		t.Fatal(err)
	}
	if contentType != "" {
		req.Header.Set("Content-Type", contentType)
	}
	client := &http.Client{Timeout: 10 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		t.Fatalf("%s %s: %v", method, url, err)
	}
	defer func() { _ = resp.Body.Close() }()
	out, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	return resp.StatusCode, string(out)
}

func getPrefs(t *testing.T, tc *TestCluster, i int) server.PrefsStatus {
	t.Helper()
	code, _, body := httpGet(t, "http://"+tc.Nodes[i].HTTPAddr()+"/api/prefs")
	if code != 200 {
		t.Fatalf("node %d GET /api/prefs: %d %s", i+1, code, body)
	}
	var doc server.PrefsStatus
	if err := json.Unmarshal([]byte(body), &doc); err != nil {
		t.Fatalf("node %d: %v: %s", i+1, err, body)
	}
	return doc
}

func setPref(t *testing.T, tc *TestCluster, i int, name, value string) (int, string) {
	t.Helper()
	body, err := json.Marshal(map[string]string{"name": name, "value": value})
	if err != nil {
		t.Fatal(err)
	}
	return httpSend(t, "POST", "http://"+tc.Nodes[i].HTTPAddr()+"/api/prefs", "application/json", string(body))
}

// TestConsolePrefs (issue #204): the console's viewer preferences live in
// the cluster, not in one browser. A preference set through any node's
// /api/prefs is read back from every other node, survives in the
// datax_ui_prefs system table, is refused unless both its name and its
// value are ones the console ships, and cannot be written by a SQL user.
func TestConsolePrefs(t *testing.T) {
	tc := startWithHTTP(t, 3)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	// Nothing set yet: an empty document, not an error, and no table has
	// been created for a viewer who has never expressed a preference.
	doc := getPrefs(t, tc, 0)
	if len(doc.Prefs) != 0 || !doc.Persistent {
		t.Fatalf("fresh cluster: %+v", doc)
	}
	if raw, err := tc.Nodes[0].DB().Get(ctx, keys.TableDescKey(catalog.PrefsTableID)); err != nil || raw != nil {
		t.Fatalf("the preferences table was created before any preference was set (%v)", err)
	}

	// Set on node 1, read from every node: the point of storing this in
	// the cluster rather than in the browser.
	if code, body := setPref(t, tc, 0, "theme", "dark"); code != 200 {
		t.Fatalf("set theme: %d %s", code, body)
	}
	for i := range tc.Nodes {
		if got := getPrefs(t, tc, i).Prefs["theme"]; got != "dark" {
			t.Fatalf("node %d reads theme %q, want dark", i+1, got)
		}
	}

	// A second preference joins the first rather than replacing it, and
	// re-setting one overwrites in place (one row per user per name).
	if code, body := setPref(t, tc, 1, "timestamps", "absolute"); code != 200 {
		t.Fatalf("set timestamps: %d %s", code, body)
	}
	if code, body := setPref(t, tc, 2, "theme", "light"); code != 200 {
		t.Fatalf("reset theme: %d %s", code, body)
	}
	doc = getPrefs(t, tc, 0)
	if doc.Prefs["theme"] != "light" || doc.Prefs["timestamps"] != "absolute" || len(doc.Prefs) != 2 {
		t.Fatalf("after two preferences: %+v", doc)
	}
	root := sql.NewSession(tc.Nodes[0].DB(), catalog.NewAccessor())
	res := execSQL(t, ctx, root, `SELECT username, name, value FROM `+catalog.PrefsTableName+` ORDER BY name`)
	if len(res.Rows) != 2 {
		t.Fatalf("table holds %d rows, want 2 (a re-set must overwrite, not append): %v", len(res.Rows), res.Rows)
	}
	// Insecure mode authenticates nobody, so every viewer shares the one
	// row keyed by the empty name.
	if res.Rows[0][0].S != "" {
		t.Fatalf("insecure mode wrote username %q, want the empty shared key", res.Rows[0][0].S)
	}

	// The table was created at its OWN reserved descriptor ID. Before
	// #204 the CREATE path assigned catalog.MetricsTableID to any system
	// table by name, so a second system table landed on the first one's
	// ID; this read is what that bug fails.
	raw, err := tc.Nodes[0].DB().Get(ctx, keys.TableDescKey(catalog.PrefsTableID))
	if err != nil || raw == nil {
		t.Fatalf("no table descriptor at the reserved ID %d: %v", catalog.PrefsTableID, err)
	}
	var desc catalog.TableDescriptor
	if err := json.Unmarshal(raw, &desc); err != nil {
		t.Fatal(err)
	}
	if desc.Name != catalog.PrefsTableName {
		t.Fatalf("descriptor %d is %q, want %s", catalog.PrefsTableID, desc.Name, catalog.PrefsTableName)
	}

	// The allowlist is enforced on the server, for both halves.
	for _, bad := range []struct{ name, value, want string }{
		{"theme", "rgb(0,0,0)", "is not a value of"},
		{"theme", "", "is not a value of"},
		{"font", "comic", "is not a console preference"},
		{"../../etc/passwd", "x", "is not a console preference"},
	} {
		code, body := setPref(t, tc, 0, bad.name, bad.value)
		if code != 400 || !strings.Contains(body, bad.want) {
			t.Errorf("set %q=%q: %d %s, want 400 containing %q", bad.name, bad.value, code, body, bad.want)
		}
	}
	// Nothing the allowlist refused reached the table.
	if got := len(execSQL(t, ctx, root, `SELECT name FROM `+catalog.PrefsTableName).Rows); got != 2 {
		t.Fatalf("a refused preference was stored: %d rows", got)
	}

	// The write is shaped like /api/login's: POST only, JSON only. With
	// SameSite=Strict on the session cookie that pair is what keeps
	// another origin from driving it.
	url := "http://" + tc.Nodes[0].HTTPAddr() + "/api/prefs"
	if code, body := httpSend(t, "POST", url, "application/x-www-form-urlencoded", `{"name":"theme","value":"dark"}`); code != 415 {
		t.Errorf("form-encoded POST: %d %s, want 415", code, body)
	}
	if code, body := httpSend(t, "PUT", url, "application/json", `{"name":"theme","value":"dark"}`); code != 405 {
		t.Errorf("PUT: %d %s, want 405", code, body)
	}
	if code, body := httpSend(t, "POST", url, "application/json", `not json`); code != 400 {
		t.Errorf("malformed body: %d %s, want 400", code, body)
	}

	// Reserved like the metrics table: readable, never writable, never
	// dropped, by a SQL user. (The system session above is the only
	// writer, and it is reached exclusively through /api/prefs.)
	user := sql.NewSessionForUser(tc.Nodes[0].DB(), catalog.NewAccessor(), "alice")
	for _, stmt := range []string{
		`INSERT INTO ` + catalog.PrefsTableName + ` (username, name, value) VALUES ('bob', 'theme', 'dark')`,
		`UPDATE ` + catalog.PrefsTableName + ` SET value = 'dark'`,
		`DELETE FROM ` + catalog.PrefsTableName,
		`DROP TABLE ` + catalog.PrefsTableName,
		`ALTER TABLE ` + catalog.PrefsTableName + ` ADD COLUMN x INT8`,
	} {
		if _, serr := trySQL(ctx, user, stmt); serr == nil {
			t.Errorf("a SQL user was allowed to run %q", stmt)
		}
	}
}

// TestConsolePrefsBeforeFinalize (issue #204, compatibility rule 4): a
// cluster that has not finalized v17 knows nothing of the reservation,
// so nothing creates the table. The console still honours a preference
// for the tab; the node says plainly that it will not be kept.
func TestConsolePrefsBeforeFinalize(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	tc, _ := StartWithEngines(t, 1, func(c *server.Config) {
		c.HTTPListener = listener
		c.BinaryVersionOverride = version.V16
	})

	doc := getPrefs(t, tc, 0)
	if doc.Persistent || len(doc.Prefs) != 0 {
		t.Fatalf("a v16 cluster reports %+v, want no preferences and Persistent=false", doc)
	}
	if !strings.Contains(doc.Why, "v17") {
		t.Errorf("Why does not name the version that would store preferences: %q", doc.Why)
	}
	code, body := setPref(t, tc, 0, "theme", "dark")
	if code != http.StatusServiceUnavailable || !strings.Contains(body, "v17") {
		t.Fatalf("set on a v16 cluster: %d %s, want 503 naming v17", code, body)
	}
}

// TestConsolePrefsWriteIsBounded (issue #204, raised in review): the
// write runs through the node's internal system session, which bypasses
// privilege checks by construction — so the principals this endpoint
// admits include ones that cannot write a byte over pgwire (a
// SELECT-only user, a metrics scrape account, a read-only certificate
// identity). Two things keep that from being an unbounded write path
// into replicated state, and both are observable from outside.
func TestConsolePrefsWriteIsBounded(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	tc, _ := StartWithEngines(t, 1, func(c *server.Config) { c.HTTPListener = listener })

	if code, body := setPref(t, tc, 0, "theme", "dark"); code != 200 {
		t.Fatalf("set theme: %d %s", code, body)
	}
	// Storing what is already stored is not a write. It still answers
	// 200 — the caller asked for a state and got it.
	if code, body := setPref(t, tc, 0, "theme", "dark"); code != 200 {
		t.Fatalf("re-setting the same value: %d %s, want 200", code, body)
	}

	// A caller that actually changes the value meets the bound. Values
	// alternate so that every request is a real write.
	limited := false
	for i := 0; i < 40; i++ {
		value := "dark"
		if i%2 == 0 {
			value = "light"
		}
		code, body := setPref(t, tc, 0, "theme", value)
		if code == http.StatusTooManyRequests {
			limited = true
			break
		}
		if code != 200 {
			t.Fatalf("write %d: %d %s", i, code, body)
		}
	}
	if !limited {
		t.Fatal("40 back-to-back preference changes were all accepted: the write is unbounded, and the " +
			"principals reaching it include ones with no write privilege anywhere in the database")
	}

	// And the no-op is genuinely suppressed rather than merely cheap:
	// with the budget now spent, a request that changes nothing still
	// succeeds, because it never reaches the write or the bucket.
	last := setPrefValue(t, tc, 0)
	if code, body := setPref(t, tc, 0, "theme", last); code != 200 {
		t.Fatalf("a no-op write was refused by the rate limit (%d %s): it should never reach it, or a "+
			"console re-sending the value it already holds would be told to slow down", code, body)
	}
}

// setPrefValue reads back the stored theme.
func setPrefValue(t *testing.T, tc *TestCluster, i int) string {
	t.Helper()
	return getPrefs(t, tc, i).Prefs["theme"]
}
