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
	"strconv"
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
		// The charts need samples to draw, and soon.
		cfg.MetricsRecordInterval = 2 * time.Second
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
	// The health panel, with problems open. A young test cluster usually
	// has none, so the routes above see the panel empty — and when one
	// did happen to be open, under a loaded suite, the overview was 439px
	// wide on the phone and this test failed one run in several (found
	// by QA on #284). Whether the panel fits must not depend on the
	// cluster being unwell at the right moment, so the problems are
	// rendered on purpose, through the console's own renderHealth, and
	// the page is measured in the same breath, before the next poll
	// redraws it.
	for _, vp := range []struct {
		name          string
		width, height int
		mobile        bool
	}{{"desktop", 1440, 900, false}, {"phone", 390, 844, true}} {
		t.Run("problems/"+vp.name, func(t *testing.T) {
			page := br.open(t, base, vp.width, vp.height, vp.mobile)
			defer page.close()
			deadline := time.Now().Add(30 * time.Second)
			for !page.facts(t).ready(false) {
				if time.Now().After(deadline) {
					t.Fatal("the overview never finished its first render")
				}
				time.Sleep(200 * time.Millisecond)
			}
			var got consoleFacts
			if err := json.Unmarshal([]byte(page.eval(t, `(() => {
  const since = Date.now() - 90000;
  renderHealth({ checks: 11, problems: [
    { severity: "critical", check: "node-down", section: "nodes", since_unix_ms: since, summary: "n3 has not heartbeated for 42s" },
    { severity: "warning", check: "under-replicated", section: "cluster-ranges", since_unix_ms: since, summary: "4 ranges have fewer replicas than their target" },
    { severity: "warning", check: "placement-unsatisfiable", section: "nodes", since_unix_ms: since, summary: "database shop asks for 3 replicas across regions and only 2 regions have a live node" },
    { severity: "info", check: "stale-statistics", section: "schema", since_unix_ms: since, summary: "a_table_whose_name_is_long_enough_to_scroll_sideways has not been analyzed since it grew tenfold" },
    // One unbroken token wider than a phone: nothing but the summary's
    // own overflow-wrap can fit it (QA on #284: the rule was otherwise
    // unexercised).
    { severity: "info", check: "stale-statistics", section: "schema", since_unix_ms: since, summary: "an_identifier_with_no_break_in_it_that_is_wider_than_any_phone_screen_this_console_will_ever_be_read_on" },
  ] });
  const f = JSON.parse(`+consoleFactsJS+`);
  f.problems = document.querySelectorAll('#problems .problem').length;
  // Every row ends in one tail that holds its age and its links (issue
  // #278): apart, the age stayed beside a short summary while the links
  // wrapped, and sat in a different place from row to row.
  f.tails = [...document.querySelectorAll('#problems .problem')].filter(r => {
    const t = r.querySelector(':scope > .tail');
    return t && t.querySelector('.since') && t.querySelector('.links') && !r.querySelector(':scope > .since, :scope > .links');
  }).length;
  return JSON.stringify(f);
})()`)), &got); err != nil {
				t.Fatal(err)
			}
			if got.Problems != 5 {
				t.Fatalf("the panel shows %d problems, want the 5 rendered into it", got.Problems)
			}
			if got.Tails != got.Problems {
				t.Errorf("%d of %d problem rows keep their age and links together in one tail (issue #278)", got.Tails, got.Problems)
			}
			if got.ScrollWidth > got.ClientWidth {
				t.Errorf("with problems open the overview is %dpx wide in a %dpx viewport, so it scrolls sideways; the furthest element is %s (issue #274)",
					got.ScrollWidth, got.ClientWidth, got.Widest)
			}
			if m := absurdAge.FindString(got.Text); m != "" {
				t.Errorf("the panel says %q of a problem found 90s ago (issue #272)", m)
			}
			if !strings.Contains(got.Text, "1m ago") {
				t.Errorf("no problem says when it was found; the view reads: %.300q", got.Text)
			}
		})
	}

	// The polish of #278 to #281, each read off a rendered page.
	t.Run("polish", func(t *testing.T) {
		// wait polls until expr is true. A page that is still about:blank,
		// or mid-navigation, has none of the elements expr asks about, and
		// asking throws: that is "not yet", not a failure — under a loaded
		// suite the first look regularly lands there.
		wait := func(page *browserPage, what, expr string) {
			t.Helper()
			deadline := time.Now().Add(30 * time.Second)
			guarded := `(() => { try { return String(!!(` + expr + `)); } catch (e) { return "false"; } })()`
			for page.eval(t, guarded) != "true" {
				if time.Now().After(deadline) {
					t.Fatalf("%s: never true: %s", what, expr)
				}
				time.Sleep(200 * time.Millisecond)
			}
		}
		check := func(page *browserPage, what, expr, want string) {
			t.Helper()
			if got := page.eval(t, expr); got != want {
				t.Errorf("%s: %s = %q, want %q", what, expr, got, want)
			}
		}

		// #281: on a phone the header leaves the screen to the content.
		// It was 300px of 844 before any of it; with the insecure banner
		// showing it is now about 200.
		phone := br.open(t, base, 390, 844, true)
		defer phone.close()
		wait(phone, "the overview on a phone", `document.getElementById('hdr-node').textContent.includes('served by')`)
		if h, err := strconv.Atoi(phone.eval(t, `String(Math.round(document.querySelector('header').getBoundingClientRect().bottom))`)); err != nil || h > 235 {
			t.Errorf("on a phone the banner and header end %dpx down the screen (%v), want 235 at most (issue #281)", h, err)
		}
		check(phone, "every view is still one tap away on a phone (#281)",
			`String([...document.querySelectorAll('#nav a')].filter(a => { const r = a.getBoundingClientRect(); return r.width > 0 && r.left >= 0 && r.right <= innerWidth; }).length)`, "8")
		check(phone, "the banner still says who has access (#281)",
			`String(document.getElementById('insecure-banner').innerText.includes('authenticates nobody') && document.getElementById('insecure-banner').innerText.includes('full access'))`, "true")

		desk := br.open(t, base, 1440, 900, false)
		defer desk.close()
		wait(desk, "the overview", `document.getElementById('hdr-node').textContent.includes('served by')`)
		// #278: the strip is named for what it holds; the hint names a key
		// this keyboard has (the test's browser is not on a Mac).
		check(desk, "the overview's strip (#278)", `document.getElementById('recent-ops-title').textContent`, "Recent operations")
		// What the ellipsis may cut, the title keeps.
		check(desk, "the header label's title", `String(/^served by n\d+( · \S+)*$/.test(document.getElementById('hdr-node').title))`, "true")
		check(desk, "the jump hint (#278)", `String(['⌘K', 'Ctrl K'].includes(document.getElementById('jump-key').textContent))`, "true")
		// #278: a length of time says no "ago".
		check(desk, "a duration (#278)", `fmtElapsed(5100000)`, "1h 25m")
		check(desk, "a short duration (#278)", `fmtElapsed(14000)`, "14s")
		// #280: an event's text links what it names, escapes what it is
		// given, and leaves an id inside a key alone.
		check(desk, "event links (#280)",
			`(() => { const d = document.createElement('div'); d.innerHTML = eventText('r23 split at /table/n1/primary/3; shedding lease of r1 from n1 to n3 (r9)'); return [...d.querySelectorAll('a')].map(a => a.textContent).join(' '); })()`,
			"r23 r1 n1 n3 r9")
		check(desk, "event text is escaped before it is linked (#280)",
			`(() => { const d = document.createElement('div'); d.innerHTML = eventText('<img src=x onerror=window.pwned=1> r2 "quoted"'); return String(d.querySelectorAll('img').length) + ' ' + String(window.pwned === undefined) + ' ' + d.querySelectorAll('a').length; })()`,
			"0 true 1")
		// A row value is not linked, whatever it says: a quoted string is
		// passed over whole, escaped quotes inside it included, and what
		// follows it is linked as before.
		check(desk, "a quoted key is left alone (#280)",
			`(() => { const d = document.createElement('div'); d.innerHTML = eventText('r5 split at /table/t/primary/"from n1 to n3 \\"r7\\" x"; r6 takes [/table/t/primary/"to n2", /Max) on n4'); return [...d.querySelectorAll('a')].map(a => a.textContent).join(' ') + '|' + d.textContent.includes('"from n1 to n3 \\"r7\\" x"'); })()`,
			"r5 r6 n4|true")
		// An unclosed quote hides nothing after it, and a run of
		// backslashes behind one is read in one way, at once (sixty of
		// them took minutes while a backslash could be read two ways;
		// the static test holds the pattern, this the behaviour).
		check(desk, "an unclosed quote (#280)",
			`(() => { const d = document.createElement('div'); d.innerHTML = eventText('restore failed: "' + '\\'.repeat(60) + ' on n4 for r2'); return [...d.querySelectorAll('a')].map(a => a.textContent).join(' '); })()`,
			"n4 r2")
		// #280: a burst is one row; a lone event, a run of three, a slow
		// drip and audit records are not folded.
		check(desk, "folding (#280)", `(() => {
  const at = s => new Date(Date.UTC(2026, 0, 1, 0, 0, s)).toISOString();
  const evs = [];
  for (let i = 0; i < 6; i++) evs.push({ seq: 100 - i, kind: "lease-shed", at: at(60 - i), summary: "shedding lease of r" + i });
  evs.push({ seq: 90, kind: "split", at: at(50), summary: "r1 split" });
  for (let i = 0; i < 5; i++) evs.push({ seq: 80 - i, kind: "merge", at: at(40 - i), summary: "m", audit: true });
  for (let i = 0; i < 3; i++) evs.push({ seq: 75 - i, kind: "rebalance", at: at(30 - i), summary: "b" });
  for (let i = 0; i < 4; i++) evs.push({ seq: 72 - i, kind: "upreplicate", at: at(20 - i), summary: "u" });
  for (let i = 0; i < 5; i++) evs.push({ seq: 68 - i, kind: "snapshot", at: new Date(Date.UTC(2025, 0, 1 + i)).toISOString(), summary: "s" });
  return foldEvents(evs).map(it => it.fold ? it.fold[0].kind + "x" + it.fold.length : it.ev.kind).join(",");
})()`, "lease-shedx6,split,merge,merge,merge,merge,merge,rebalance,rebalance,rebalance,upreplicatex4,snapshot,snapshot,snapshot,snapshot,snapshot")
		// The kinds where each record is its own finding are never one
		// row, however fast they come: an incident is a burst of them.
		check(desk, "findings are not folded (#280)", `(() => {
  const at = s => new Date(Date.UTC(2026, 0, 1, 0, 0, s)).toISOString();
  return ["health", "consistency-failure", "security", "auth-failure", "rebalance"].map(kind => {
    const evs = [];
    for (let i = 0; i < 5; i++) evs.push({ seq: 50 - i, kind, at: at(30 - i), summary: kind + " " + i });
    return foldEvents(evs).length;
  }).join(",");
})()`, "5,5,5,5,1")
		// A fold's row, shut and open. Shut it says how many, how long
		// ago and the newest; open it is the header of the rows under it
		// and says only how many — the first row beneath has the rest,
		// live. The caret is drawn from the state and is not part of the
		// button's name. An open fold stays open while its burst grows:
		// it is known by its oldest event, and the newest is the one that
		// changes — a reader opens a burst while it is happening, and it
		// must not shut on the next poll.
		check(desk, "a fold opens in place (#280)", `(() => {
  const at = s => new Date(Date.now() - s * 1000).toISOString();
  const evs = [];
  for (let i = 0; i < 6; i++) evs.push({ seq: 100 - i, kind: "lease-shed", at: at(i), summary: "shedding lease of r" + (i + 1) });
  const t = document.createElement("table");
  t.innerHTML = "<tbody>" + foldRows(foldEvents(evs)).map(r => r.html).join("") + "</tbody>";
  document.querySelector("main").appendChild(t);
  const row = t.querySelector("tr.fold"), b = row.querySelector("button[data-fold]");
  const shown = () => [...row.querySelectorAll(".shut")].filter(s => getComputedStyle(s).display !== "none").length;
  const out = [t.querySelectorAll("tr").length, shown(), b.getAttribute("aria-expanded"), b.textContent];
  b.click();
  out.push(shown(), b.getAttribute("aria-expanded"), row.hasAttribute("data-open"));
  out.push(foldRows(foldEvents(evs)).length);
  evs.unshift({ seq: 101, kind: "lease-shed", at: new Date().toISOString(), summary: "shedding lease of r9" });
  const grown = foldRows(foldEvents(evs));
  out.push(grown.length, grown[0].html.includes('aria-expanded="true"'), grown[0].html.includes("7 in a row"));
  b.click();
  out.push(shown(), b.getAttribute("aria-expanded"), row.hasAttribute("data-open"));
  out.push(/\/\s*""$/.test(getComputedStyle(b, "::before").content));
  t.remove();
  return out.join("|");
})()`, "1|2|false|6 in a row|0|true|true|7|8|true|true|2|false|false|true")
		// A locality never costs the header a line: one too long for the
		// room that is left ends in an ellipsis instead of putting the
		// sign-in on a second line, at every width above a phone. The test
		// cluster has no localities, so a long one is put where the header
		// puts it — and the header is measured against itself, before and
		// after, since how many lines it is to begin with depends on this
		// machine's fonts.
		for _, c := range []struct {
			width int
			want  string
		}{{700, "inline 0 ellipsis"}, {1100, "inline 0 ellipsis"}, {1280, "inline 0 ellipsis"}, {1320, "inline 0 ellipsis"}, {1920, "inline 0 ellipsis"}} {
			p := br.open(t, base, c.width, 900, false)
			wait(p, "the overview", `document.getElementById('hdr-node').textContent.includes('served by')`)
			check(p, fmt.Sprintf("the header's locality, the height it adds and how it is cut at %dpx", c.width), `(() => {
  const h = () => Math.round(document.querySelector("header").getBoundingClientRect().height);
  const n = document.getElementById("hdr-node"), before = h();
  const s = document.createElement("span"); s.className = "loc"; s.textContent = " · region=us-east-1,zone=us-east-1b,rack=12";
  n.querySelector("a").after(s);
  return [getComputedStyle(s).display, h() - before, getComputedStyle(n).textOverflow].join(" ");
})()`, c.want)
			p.close()
		}

		// #279: the cell that names a row that opens reads as a link.
		schema := br.open(t, base+"schema", 1440, 900, false)
		defer schema.close()
		wait(schema, "the schema list", `!!document.querySelector('#schema tr.clickable .opens')`)
		check(schema, "a table's name in the schema list (#279)",
			`getComputedStyle(document.querySelector('#schema tr.clickable .opens')).textDecorationLine`, "underline")
		data := br.open(t, base+"data", 1440, 900, false)
		defer data.close()
		wait(data, "the ranges table", `!!document.querySelector('#cluster-ranges tr.clickable .opens')`)
		check(data, "a range's id in the ranges table (#279)",
			`getComputedStyle(document.querySelector('#cluster-ranges tr.clickable .opens')).textDecorationLine`, "underline")

		// #278: the admin badge is a badge beside a role's name too.
		sec := br.open(t, base+"security", 1440, 900, false)
		defer sec.close()
		wait(sec, "the roles table", `!!document.querySelector('#view-security td .role')`)
		check(sec, "the admin badge in the roles table (#278)",
			`String(parseFloat(getComputedStyle(document.querySelector('#view-security td .role')).borderTopWidth) > 0 && parseFloat(getComputedStyle(document.querySelector('#view-security td .role')).marginLeft) > 0)`, "true")

		// #278: uptime once on a node's page; the version breaks at a space.
		node := br.open(t, base+"node/1", 390, 844, true)
		defer node.close()
		wait(node, "the node page", `document.querySelectorAll('#view-node .tile').length > 6`)
		check(node, "uptime tiles on the node page (#278)",
			`String([...document.querySelectorAll('#view-node .tile .label')].filter(l => l.textContent.trim() === 'uptime').length)`, "1")
		check(node, "the version tile does not break inside a word (#278)",
			`getComputedStyle(document.querySelector('#node-ident .value')).wordBreak`, "normal")

		// #280: the timeline says what a split cut by the table's name.
		ops := br.open(t, base+"ops", 1440, 900, false)
		defer ops.close()
		wait(ops, "the timeline", `!!document.querySelector('#events td.when')`)
		// Whichever split is on screen — a burst folds to its newest —
		// names its table, and none is left with a number for one.
		check(ops, "a split names its table (#280)",
			`(() => { const rows = [...document.querySelectorAll('#events tr')].map(r => r.textContent).filter(t => /split at \/table\//.test(t)); return String(rows.length > 0 && rows.every(t => /split at \/table\/[a-z_]+/.test(t))); })()`, "true")
		check(ops, "an event links the range it names (#280)", `String([...document.querySelectorAll('#events td a')].some(a => /[?&]r=\d+/.test(a.getAttribute('href'))))`, "true")

		// A link to a range keeps both the range and the time window. The
		// two shared the name "range", and under any window but the
		// default the link an event built opened the data view with no
		// range at all.
		win := br.open(t, base+"ops?range=6h", 1440, 900, false)
		defer win.close()
		wait(win, "the timeline under a 6h window", `ui.range === '6h' && !!document.querySelector('#events td.when')`)
		check(win, "a range's link under a 6h window", `(() => {
  const d = document.createElement('div'); d.innerHTML = eventText('r1 split');
  const q = new URLSearchParams(d.querySelector('a').getAttribute('href').split('?')[1]);
  return q.get('r') + ' ' + q.get('range');
})()`, "1 6h")
		for _, link := range []string{"data?r=1&range=6h", "data?range=1"} {
			p := br.open(t, base+link, 1440, 900, false)
			wait(p, "the range opened by "+link, `!!document.querySelector('#cluster-ranges tr[data-range="1"][aria-expanded="true"]')`)
			if strings.Contains(link, "6h") {
				check(p, "the window beside the range in "+link, `ui.range + ' ' + document.getElementById('range-picker-global').value`, "6h 6h")
			}
			// Closing the range takes it out of the address and leaves
			// the window: a reload, a bookmark or Back must not reopen
			// what the reader just closed.
			want := "#/data"
			if strings.Contains(link, "6h") {
				want = "#/data?range=6h"
			}
			check(p, "the address after closing the range opened by "+link, `(() => { toggleRangeDetail(1); return location.hash; })()`, want)
			check(p, "the range after closing it", `String(!!document.querySelector('#cluster-ranges tr[data-range="1"][aria-expanded="true"]'))`, "false")
			p.close()
		}
	})

	// Sorting, the one ranges table, and what a chart says about itself
	// (#282, #283, #277).
	t.Run("tables and charts", func(t *testing.T) {
		wait := func(page *browserPage, what, expr string) {
			t.Helper()
			deadline := time.Now().Add(30 * time.Second)
			guarded := `(() => { try { return String(!!(` + expr + `)); } catch (e) { return "false"; } })()`
			for page.eval(t, guarded) != "true" {
				if time.Now().After(deadline) {
					t.Fatalf("%s: never true: %s", what, expr)
				}
				time.Sleep(200 * time.Millisecond)
			}
		}
		check := func(page *browserPage, what, expr, want string) {
			t.Helper()
			if got := page.eval(t, expr); got != want {
				t.Errorf("%s: %s = %q, want %q", what, expr, got, want)
			}
		}

		// #283: one table of ranges, with the load figures as columns.
		data := br.open(t, base+"data", 1440, 900, false)
		defer data.close()
		wait(data, "the ranges table", `document.querySelectorAll('#cluster-ranges tr[data-range]').length > 1`)
		check(data, "the ranges table's columns (#283)",
			`[...document.querySelectorAll('table[data-sorts="ranges"] thead th')].map(th => th.childNodes[0].textContent.trim()).join(",")`,
			"range,span,replicas,leader,qps,size")
		check(data, "the hotspots table is gone (#283)", `String(!document.getElementById('repl-hot') && !document.getElementById('sec-hot-ranges'))`, "true")
		check(data, "every range row has the six cells (#283)",
			`String([...document.querySelectorAll('#cluster-ranges tr[data-range]')].every(r => r.children.length === 6))`, "true")

		// #282: a header's button sorts, reverses, and gives the table its
		// own order back; the header says which, and a poll keeps it.
		ids := `[...document.querySelectorAll('#cluster-ranges tr[data-range]')].map(r => Number(r.dataset.range)).join(",")`
		click := `(document.querySelector('table[data-sorts="ranges"] th[data-sort="range"] button.sort').click(), "ok")`
		data.eval(t, click)
		asc := data.eval(t, ids)
		check(data, "sorted ascending (#282)", `document.querySelector('table[data-sorts="ranges"] th[data-sort="range"]').getAttribute("aria-sort")`, "ascending")
		check(data, "ascending is ascending (#282)", `String((s => s.join(",") === s.slice().sort((a, b) => a - b).join(","))(`+"`"+`${`+ids+`}`+"`"+`.split(",").map(Number)))`, "true")
		data.eval(t, click)
		check(data, "sorted descending (#282)", `document.querySelector('table[data-sorts="ranges"] th[data-sort="range"]').getAttribute("aria-sort")`, "descending")
		check(data, "descending is the reverse (#282)", ids, reverseCSV(asc))
		time.Sleep(3500 * time.Millisecond) // a poll redraws the table
		check(data, "a poll keeps the reader's order (#282)", ids, reverseCSV(asc))
		data.eval(t, click)
		check(data, "the third press clears the sort (#282)", `document.querySelector('table[data-sorts="ranges"] th[data-sort="range"]').getAttribute("aria-sort")`, "none")
		for _, table := range []string{"nodes", "domains", "schema", "ranges"} {
			check(data, "the "+table+" table's headers sort (#282)",
				`String(document.querySelectorAll('table[data-sorts="`+table+`"] th[data-sort] button.sort').length >= 4)`, "true")
		}
		// The rule itself: no figure sorts last in either direction, and
		// equal rows keep the order they came in.
		check(data, "sortRows (#282)", `(() => {
  sortBy("t", { v: r => r.v, n: r => r.n });
  const rows = [{ k: "a", v: 3, n: "n10" }, { k: "b", v: null, n: "n2" }, { k: "c", v: 1, n: "n2" }, { k: "d", v: 3, n: "" }, { k: "e", v: undefined, n: "n1" }];
  const run = (col, dir) => { sortChoice.t = { col, dir }; return sortRows("t", rows).map(r => r.k).join(""); };
  const out = [run("v", 1), run("v", -1), run("n", 1), run("n", -1)].join(" ");
  delete sortChoice.t;
  return out + " " + sortRows("t", rows).map(r => r.k).join("");
})()`, "cadbe adcbe ebcad abced abcde")

		// #277: round gridlines, a flat series that does not invent
		// detail, words for a title and a unit.
		check(data, "niceTicks (#277)", `[niceTicks(0, 1566, "/s"), niceTicks(0, 0, "bytes"), niceTicks(0, 87, "percent"), niceTicks(0, 400 * 1048576, "bytes").map(v => v / 1048576), niceTicks(0, 0.0004, "")].map(a => a.join(",")).join(" | ")`,
			"0,500,1000,1500,2000 | 0,1 | 0,25,50,75,100 | 0,128,256,384,512 | 0,0.0001,0.0002,0.00030000000000000003,0.0004")
		check(data, "a chart's title and unit (#277)",
			`[seriesTitle({ name: "kv.batch_p99_us", help: "p99 KV batch latency" }), seriesTitle({ name: "x.y" }), unitWord({}, "us"), unitWord({ rate: true }, "/s"), unitWord({}, "percent")].join(" | ")`,
			"P99 KV batch latency | x.y | duration | per second | percent")
		// A window whose data begins late starts where the data does, for
		// every chart alike; one that is mostly full is left alone.
		check(data, "chartWindow (#277)", `(() => {
  const doc = first => ({ from_ms: 0, to_ms: 3600000, step_ms: 10000, series: [{ nodes: { 1: [[first, 1], [3590000, 2]] } }, { nodes: { 2: [[first + 20000, 1]] } }] });
  const late = chartWindow(doc(3000000)), early = chartWindow(doc(600000));
  return [late.win.from_ms, late.since, early.win.from_ms, early.since].join(",");
})()`, "2980000,3000000,0,0")

		metrics := br.open(t, base+"metrics", 1440, 900, false)
		defer metrics.close()
		wait(metrics, "the charts", `document.querySelectorAll('#charts .chart svg').length > 0`)
		check(metrics, "the event note is said once (#277)", `String(document.querySelectorAll('#charts .annnote').length <= 1 && document.querySelectorAll('#charts .chart .annnote').length === 0)`, "true")
		check(metrics, "a chart is titled in words beside its metric's name (#277)",
			`String([...document.querySelectorAll('#charts .chart')].every(c => c.querySelector('.title code.metric') && c.querySelector('.title b').textContent !== c.querySelector('.title code.metric').textContent))`, "true")
		check(metrics, "no axis repeats a label (#277)",
			`String([...document.querySelectorAll('#charts .chart svg')].every(svg => { const l = [...svg.querySelectorAll('g.axis text[text-anchor="end"]')].map(t => t.textContent); return new Set(l).size === l.length; }))`, "true")
		check(metrics, "line-end labels stay inside the plot (#277)",
			`String([...document.querySelectorAll('#charts .endlabel')].every(l => Number(l.getAttribute("y")) <= 182))`, "true")
	})

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
	Problems    int      `json:"problems"`
	Tails       int      `json:"tails"`
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

// reverseCSV is a comma-separated list, back to front.
func reverseCSV(s string) string {
	parts := strings.Split(s, ",")
	for i, j := 0, len(parts)-1; i < j; i, j = i+1, j-1 {
		parts[i], parts[j] = parts[j], parts[i]
	}
	return strings.Join(parts, ",")
}

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
