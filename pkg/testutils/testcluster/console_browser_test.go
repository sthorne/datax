//go:build unix

package testcluster

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"net"
	"os"
	"os/exec"
	"regexp"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/sthorne/datax/pkg/server"
)

// The console's other tests read its JavaScript as text. They are
// thorough about what text can show and blind to what a page renders,
// and two faults shipped through that gap (issue #276): every "when"
// column of an event read "29910444m ago" for two months (#272), and a
// node, schema or metrics page opened from a bookmark never loaded its
// header or the insecure-mode banner (#273). Both are one glance at a
// rendered page.
//
// So this opens every route in a real browser, cold — a new tab per
// route, the way a bookmark or a shared link arrives — at a desktop and
// a phone width, and reads the page back.
//
// It drives Chrome over the DevTools pipe (--remote-debugging-pipe:
// JSON messages, NUL-terminated, on file descriptors 3 and 4), which
// needs nothing beyond the standard library. The binary comes from
// DATAX_CHROME or the usual names on PATH. With none, the test skips —
// unless DATAX_REQUIRE_BROWSER is set, as it is in CI, where a skip
// would be this test quietly ceasing to exist.

// consoleLongTable is long enough that its span makes the ranges table
// wider than a phone, which is what it takes to see a cell escape its
// scroll box (#274).
const consoleLongTable = "a_table_whose_name_is_long_enough_to_scroll_sideways"

func TestConsoleRendersEveryRouteOpenedCold(t *testing.T) {
	bin := findChrome()
	if bin == "" {
		if os.Getenv("DATAX_REQUIRE_BROWSER") != "" {
			t.Fatal("DATAX_REQUIRE_BROWSER is set and no Chrome or Chromium was found: set DATAX_CHROME to the binary")
		}
		t.Skip("no Chrome or Chromium on PATH and DATAX_CHROME is unset")
	}
	httpLis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	first := true
	tc := StartWithOptions(t, 3, func(cfg *server.Config) {
		if first {
			cfg.HTTPListener = httpLis
			first = false
		}
	})
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()

	conn, cerr := pgx.Connect(ctx, pgURL(tc, 0))
	if cerr != nil {
		t.Fatalf("pgx connect: %v", cerr)
	}
	defer func() { _ = conn.Close(ctx) }()
	for _, q := range []string{
		`CREATE TABLE ` + consoleLongTable + ` (id INT8 PRIMARY KEY, a_long_secondary_column_name TEXT)`,
		`INSERT INTO ` + consoleLongTable + ` VALUES (1, 'x'), (2, 'y')`,
		`SELECT * FROM ` + consoleLongTable + ` WHERE id = 1`,
		// A split is an event, and the views that list events are where
		// a wrong "when" shows.
		`ALTER TABLE ` + consoleLongTable + ` SPLIT AT VALUES (2)`,
	} {
		if _, err := conn.Exec(ctx, q); err != nil {
			t.Fatalf("%s: %v", q, err)
		}
	}
	// The timeline a page shows is the serving node's own ring, so the
	// checks below need an event in n1's.
	if _, _, body := httpGet(t, "http://"+tc.Nodes[0].HTTPAddr()+"/api/events"); !strings.Contains(body, `"kind"`) {
		t.Fatalf("n1's event ring is empty after a split, so no view has a moment to render: %s", body)
	}

	br := startChrome(t, bin)
	base := "http://" + tc.Nodes[0].HTTPAddr() + "/#/"
	routes := []string{"", "nodes", "node/1", "node/2", "data", "sql", "schema", "schema/" + consoleLongTable, "metrics", "ops", "security"}
	// A route whose page lists events must show their "when": a check
	// for wrong ages proves nothing on a page that rendered none.
	wantsWhen := map[string]bool{"": true, "node/1": true, "ops": true}
	for _, vp := range []struct {
		name          string
		width, height int
		mobile        bool
	}{{"desktop", 1440, 900, false}, {"phone", 390, 844, true}} {
		for _, route := range routes {
			t.Run(vp.name+"/"+strings.ReplaceAll(route, "/", "_"), func(t *testing.T) {
				page := br.open(t, base+route, vp.width, vp.height, vp.mobile)
				defer page.close()
				var got consoleFacts
				deadline := time.Now().Add(30 * time.Second)
				for {
					got = page.facts(t)
					if got.ready(wantsWhen[route]) || time.Now().After(deadline) {
						break
					}
					time.Sleep(200 * time.Millisecond)
				}
				if !strings.Contains(got.Header, "served by") {
					t.Errorf("the header reads %q: a page opened directly never learned which node served it (issue #273)", got.Header)
				}
				// The test cluster authenticates nobody, and every view has
				// to say so.
				if got.Banner != "block" {
					t.Errorf("the insecure-mode banner is not shown (display: %s) on an insecure cluster (issue #273)", got.Banner)
				}
				if len(got.Scope) < 4 {
					t.Errorf("the scope picker offers %v, want the whole cluster and its three nodes (issue #273)", got.Scope)
				}
				if got.View == "" {
					t.Error("no view is showing")
				}
				if wantsWhen[route] && len(got.Whens) == 0 {
					t.Errorf(`no "when" cell rendered on a view that lists events; the view reads: %.300q`, got.Text)
				}
				for _, w := range got.Whens {
					if !saneWhen.MatchString(w) {
						t.Errorf(`a "when" reads %q on a cluster that is seconds old (issue #272)`, w)
						break
					}
				}
				if m := absurdAge.FindString(got.Text); m != "" {
					t.Errorf("the page says %q on a cluster that is seconds old (issue #272)", m)
				}
				if got.ScrollWidth > got.ClientWidth {
					t.Errorf("the page is %dpx wide in a %dpx viewport, so it scrolls sideways; the furthest element is %s (issue #274)",
						got.ScrollWidth, got.ClientWidth, got.Widest)
				}
				for _, e := range page.exceptions() {
					t.Errorf("script error: %s", e)
				}
			})
		}
	}
	// The formatters themselves, called in the page: an age is said in
	// the unit a person would use (issue #272), and a moment handed to
	// fmtWhen as an instant reads as the age it is.
	t.Run("ages", func(t *testing.T) {
		page := br.open(t, base, 1440, 900, false)
		defer page.close()
		deadline := time.Now().Add(30 * time.Second)
		for page.eval(t, `typeof fmtAgo === 'function' && typeof fmtWhen === 'function'`) != "true" {
			if time.Now().After(deadline) {
				t.Fatal("the console's scripts never loaded")
			}
			time.Sleep(100 * time.Millisecond)
		}
		const m, h, d = 60000, 3600000, 86400000
		for _, c := range []struct {
			expr string
			want string
		}{
			{fmt.Sprintf("fmtAgo(%d)", 900), "now"},
			{fmt.Sprintf("fmtAgo(%d)", 42000), "42s ago"},
			{fmt.Sprintf("fmtAgo(%d)", 59600), "59s ago"},
			{fmt.Sprintf("fmtAgo(%d)", 90000), "1m ago"},
			{fmt.Sprintf("fmtAgo(%d)", 59*m+40000), "59m ago"},
			{fmt.Sprintf("fmtAgo(%d)", h), "1h ago"},
			{fmt.Sprintf("fmtAgo(%d)", h+25*m), "1h 25m ago"},
			{fmt.Sprintf("fmtAgo(%d)", 17*h+30*m), "17h ago"},
			{fmt.Sprintf("fmtAgo(%d)", d), "24h ago"},
			{fmt.Sprintf("fmtAgo(%d)", 3*d+h), "3d ago"},
			{"fmtWhen(Date.now() - 5000)", "5s ago"},
			{"fmtWhen(Date.parse(new Date(Date.now() - 180000).toISOString()))", "3m ago"},
			{"fmtWhen(NaN)", "—"},
		} {
			if got := page.eval(t, c.expr); got != c.want {
				t.Errorf("%s = %q, want %q", c.expr, got, c.want)
			}
		}
	})
}

var (
	// What a moment may read as on a cluster started a minute ago.
	saneWhen = regexp.MustCompile(`^(now|\d{1,2}s ago|\d{1,2}m ago|\d\d:\d\d:\d\d [+-]\d\d:\d\d)$`)
	// An age no cluster in a test has: thousands of any unit.
	absurdAge = regexp.MustCompile(`\b\d{4,}[smhd] ago\b`)
)

// consoleFacts is what one look at a rendered page reports.
type consoleFacts struct {
	View        string   `json:"view"`
	Header      string   `json:"header"`
	Banner      string   `json:"banner"`
	Scope       []string `json:"scope"`
	Whens       []string `json:"whens"`
	Text        string   `json:"text"`
	ScrollWidth int      `json:"scrollWidth"`
	ClientWidth int      `json:"clientWidth"`
	Widest      string   `json:"widest"`
}

// ready reports whether the page has finished its first render: the
// header is filled, and the events are in where the view lists them.
func (f consoleFacts) ready(wantWhen bool) bool {
	return strings.Contains(f.Header, "served by") && f.View != "" && f.Banner == "block" && (!wantWhen || len(f.Whens) > 0)
}

const consoleFactsJS = `(() => {
  const main = document.querySelector('#main-views > main:not([hidden])');
  const hdr = document.getElementById('hdr-node');
  const banner = document.getElementById('insecure-banner');
  const scope = document.getElementById('scope-picker');
  let widest = '', right = 0;
  for (const el of document.querySelectorAll('body *')) {
    const r = el.getBoundingClientRect();
    if (r.width === 0 || r.right <= right) continue;
    // A closed <details> keeps boxes for what it is not showing.
    const shut = el.closest('details:not([open])');
    if (shut && !el.closest('summary')) continue;
    // What a scroll box clips does not widen the page. An absolutely
    // positioned element is clipped only by a positioned ancestor.
    let clipped = false, abs = getComputedStyle(el).position === 'absolute';
    for (let p = el.parentElement; p && p !== document.body; p = p.parentElement) {
      const s = getComputedStyle(p);
      if (s.overflowX !== 'visible' && (!abs || s.position !== 'static')) { clipped = true; break; }
      if (s.position === 'absolute') abs = true;
    }
    if (clipped) continue;
    right = r.right;
    widest = el.tagName.toLowerCase() + (el.id ? '#' + el.id : '') + (el.className && el.className.baseVal === undefined ? '.' + String(el.className).trim().replace(/\s+/g, '.') : '') +
      ' "' + (el.textContent || '').trim().slice(0, 40) + '" (right edge ' + Math.round(r.right) + 'px)';
  }
  return JSON.stringify({
    view: main ? main.id : '',
    header: hdr ? hdr.textContent.trim() : '',
    banner: banner ? getComputedStyle(banner).display : 'missing',
    scope: scope ? [...scope.options].map(o => o.value) : [],
    whens: main ? [...main.querySelectorAll('td.when')].map(td => td.textContent.trim()) : [],
    text: main ? main.innerText : '',
    scrollWidth: document.documentElement.scrollWidth,
    clientWidth: document.documentElement.clientWidth,
    widest,
  });
})()`

func findChrome() string {
	if p := os.Getenv("DATAX_CHROME"); p != "" {
		return p
	}
	for _, name := range []string{"google-chrome", "google-chrome-stable", "chromium", "chromium-browser", "chrome"} {
		if p, err := exec.LookPath(name); err == nil {
			return p
		}
	}
	return ""
}

// chrome is one headless browser driven over the DevTools pipe.
type chrome struct {
	w  *os.File
	mu sync.Mutex
	// next numbers requests; waiting holds the reply channel of each one
	// in flight; thrown collects uncaught exceptions by session.
	next    int
	waiting map[int]chan cdpMessage
	thrown  map[string][]string
}

type cdpMessage struct {
	ID        int             `json:"id,omitempty"`
	Method    string          `json:"method,omitempty"`
	Params    json.RawMessage `json:"params,omitempty"`
	Result    json.RawMessage `json:"result,omitempty"`
	Error     *cdpError       `json:"error,omitempty"`
	SessionID string          `json:"sessionId,omitempty"`
}

type cdpError struct {
	Message string `json:"message"`
}

func startChrome(t *testing.T, bin string) *chrome {
	t.Helper()
	toChromeR, toChromeW, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	fromChromeR, fromChromeW, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	// Chrome keeps its sandbox unless it cannot have one: as root it
	// refuses to start with it, and DATAX_CHROME_NO_SANDBOX is for the
	// other place it cannot, an ordinary user in a container without
	// user namespaces. Either way the only pages this browser ever loads
	// are the console of the cluster this test started, on loopback, and
	// the DevTools pipe below opens no port another process could reach.
	// Not t.TempDir(): Chrome's helper processes can still be writing
	// their profile for a moment after the browser exits, and a test
	// must not fail on tidying up after it.
	profile, err := os.MkdirTemp("", "datax-console-test-")
	if err != nil {
		t.Fatal(err)
	}
	args := []string{"--headless", "--disable-gpu", "--disable-dev-shm-usage",
		"--no-first-run", "--no-default-browser-check", "--user-data-dir=" + profile, "--remote-debugging-pipe"}
	if os.Geteuid() == 0 || os.Getenv("DATAX_CHROME_NO_SANDBOX") != "" {
		args = append(args, "--no-sandbox")
	}
	cmd := exec.Command(bin, args...)
	cmd.ExtraFiles = []*os.File{toChromeR, fromChromeW} // the child's fd 3 and fd 4
	if err := cmd.Start(); err != nil {
		t.Fatalf("start %s: %v", bin, err)
	}
	_ = toChromeR.Close()
	_ = fromChromeW.Close()
	c := &chrome{w: toChromeW, waiting: map[int]chan cdpMessage{}, thrown: map[string][]string{}}
	exited := make(chan struct{})
	go func() { _ = cmd.Wait(); close(exited) }()
	t.Cleanup(func() {
		// Ask it to leave, so its helpers go with it; insist if it won't.
		if out, err := json.Marshal(cdpMessage{ID: 1 << 30, Method: "Browser.close"}); err == nil {
			_, _ = toChromeW.Write(append(out, 0))
		}
		select {
		case <-exited:
		case <-time.After(5 * time.Second):
			_ = cmd.Process.Kill()
			<-exited
		}
		_ = toChromeW.Close()
		_ = fromChromeR.Close()
		for i := 0; i < 10 && os.RemoveAll(profile) != nil; i++ {
			time.Sleep(100 * time.Millisecond)
		}
	})
	go c.read(fromChromeR)
	return c
}

// read dispatches what Chrome writes: a reply to the request waiting on
// its id, an uncaught exception to its session's list.
func (c *chrome) read(r *os.File) {
	br := bufio.NewReaderSize(r, 1<<20)
	for {
		raw, err := br.ReadBytes(0)
		if err != nil {
			c.mu.Lock()
			for id, ch := range c.waiting {
				close(ch)
				delete(c.waiting, id)
			}
			c.mu.Unlock()
			return
		}
		var m cdpMessage
		if json.Unmarshal(raw[:len(raw)-1], &m) != nil {
			continue
		}
		c.mu.Lock()
		if m.ID != 0 {
			if ch, ok := c.waiting[m.ID]; ok {
				ch <- m
				delete(c.waiting, m.ID)
			}
		} else if m.Method == "Runtime.exceptionThrown" {
			var p struct {
				Details struct {
					Text      string `json:"text"`
					Exception struct {
						Description string `json:"description"`
					} `json:"exception"`
				} `json:"exceptionDetails"`
			}
			_ = json.Unmarshal(m.Params, &p)
			c.thrown[m.SessionID] = append(c.thrown[m.SessionID], strings.TrimSpace(p.Details.Text+" "+p.Details.Exception.Description))
		}
		c.mu.Unlock()
	}
}

func (c *chrome) call(t *testing.T, session, method string, params any) json.RawMessage {
	t.Helper()
	raw, err := json.Marshal(params)
	if err != nil {
		t.Fatal(err)
	}
	ch := make(chan cdpMessage, 1)
	c.mu.Lock()
	c.next++
	id := c.next
	c.waiting[id] = ch
	c.mu.Unlock()
	out, _ := json.Marshal(cdpMessage{ID: id, Method: method, Params: raw, SessionID: session})
	if _, err := c.w.Write(append(out, 0)); err != nil {
		t.Fatalf("%s: writing to the browser: %v", method, err)
	}
	select {
	case m, ok := <-ch:
		if !ok {
			t.Fatalf("%s: the browser went away", method)
		}
		if m.Error != nil {
			t.Fatalf("%s: %s", method, m.Error.Message)
		}
		return m.Result
	case <-time.After(30 * time.Second):
		t.Fatalf("%s: no reply from the browser in 30s (if this is the first call, Chrome did not come up: "+
			"where it cannot start its sandbox, set DATAX_CHROME_NO_SANDBOX=1)", method)
	}
	return nil
}

// browserPage is one tab.
type browserPage struct {
	c       *chrome
	t       *testing.T
	target  string
	session string
}

// open loads url in a new tab of the given size. A new tab, because the
// point is a page opened cold: following a link inside the console
// keeps whatever the previous view had already loaded. mobile asks for
// a phone's overlay scrollbars, so the viewport is the width given.
func (c *chrome) open(t *testing.T, url string, width, height int, mobile bool) *browserPage {
	t.Helper()
	var created struct {
		TargetID string `json:"targetId"`
	}
	mustUnmarshal(t, c.call(t, "", "Target.createTarget", map[string]any{"url": "about:blank"}), &created)
	var attached struct {
		SessionID string `json:"sessionId"`
	}
	mustUnmarshal(t, c.call(t, "", "Target.attachToTarget", map[string]any{"targetId": created.TargetID, "flatten": true}), &attached)
	p := &browserPage{c: c, t: t, target: created.TargetID, session: attached.SessionID}
	c.call(t, p.session, "Runtime.enable", map[string]any{})
	c.call(t, p.session, "Emulation.setDeviceMetricsOverride", map[string]any{"width": width, "height": height, "deviceScaleFactor": 1, "mobile": mobile})
	c.call(t, p.session, "Page.navigate", map[string]any{"url": url})
	return p
}

func (p *browserPage) close() {
	p.c.call(p.t, "", "Target.closeTarget", map[string]any{"targetId": p.target})
}

// eval runs an expression in the page and returns its string value.
func (p *browserPage) eval(t *testing.T, expr string) string {
	t.Helper()
	var res struct {
		Result struct {
			Value any `json:"value"`
		} `json:"result"`
		Exception *struct {
			Text string `json:"text"`
		} `json:"exceptionDetails"`
	}
	mustUnmarshal(t, p.c.call(t, p.session, "Runtime.evaluate", map[string]any{"expression": expr, "returnByValue": true}), &res)
	if res.Exception != nil {
		t.Fatalf("evaluating in the page: %s", res.Exception.Text)
	}
	return fmt.Sprint(res.Result.Value)
}

func (p *browserPage) facts(t *testing.T) consoleFacts {
	t.Helper()
	var f consoleFacts
	// The page may still be about:blank, or mid-navigation, on the first
	// looks: an unparseable answer is "not ready", not a failure.
	_ = json.Unmarshal([]byte(p.eval(t, `document.getElementById('hdr-node') ? `+consoleFactsJS+` : '{}'`)), &f)
	return f
}

func (p *browserPage) exceptions() []string {
	p.c.mu.Lock()
	defer p.c.mu.Unlock()
	return append([]string(nil), p.c.thrown[p.session]...)
}

func mustUnmarshal(t *testing.T, raw json.RawMessage, v any) {
	t.Helper()
	if err := json.Unmarshal(raw, v); err != nil {
		t.Fatalf("decoding the browser's reply %s: %v", raw, err)
	}
}
