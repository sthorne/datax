package ui

import (
	"bytes"
	"errors"
	"html"
	"os"
	"regexp"
	"strings"
	"testing"
)

// TestScriptFilesAssemble (issue #151): the console is several files
// assembled into one page, so the build must fail rather than the
// console blanking when a file goes missing, is emptied, or lands out
// of order.
func TestScriptFilesAssemble(t *testing.T) {
	names, err := ScriptFiles()
	if err != nil {
		t.Fatal(err)
	}
	if len(names) < 2 {
		t.Fatalf("console script files: %v", names)
	}
	// Name order is the load order, and it has to put the shared core
	// and the router before the views and the boot file last: the boot
	// file starts the polling and calls into everything above it.
	if !strings.HasSuffix(names[0], "10-core.js") {
		t.Fatalf("the first script is %s, want the shared core", names[0])
	}
	if !strings.HasSuffix(names[len(names)-1], "95-boot.js") {
		t.Fatalf("the last script is %s, want the boot file", names[len(names)-1])
	}
	for i := 1; i < len(names); i++ {
		if names[i-1] >= names[i] {
			t.Fatalf("script files are not in name order: %s then %s", names[i-1], names[i])
		}
	}
	for _, name := range names {
		body, err := FS.ReadFile(name)
		if err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		if len(bytes.TrimSpace(body)) == 0 {
			t.Fatalf("%s is empty", name)
		}
	}
	// The shell has the seam the scripts are spliced into, and the pages
	// carry the placeholders the node fills in.
	shell, err := FS.ReadFile("index.html")
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Contains(shell, []byte("__CONSOLE_SCRIPTS__")) {
		t.Fatal("the console shell lost its script placeholder")
	}
	// The version placeholder lives in the core script, which is part of
	// the assembled page the node stamps and digests.
	core, err := FS.ReadFile(names[0])
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Contains(core, []byte("__CONSOLE_VERSION__")) {
		t.Fatalf("%s lost the console version placeholder", names[0])
	}
	login, err := FS.ReadFile("login.html")
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Contains(login, []byte("__LOGIN_CONTEXT__")) {
		t.Fatal("the sign-in page lost its context placeholder")
	}
	// Every view container the router switches between must exist, or a
	// route resolves to a blank page.
	for _, view := range []string{"overview", "nodes", "node", "data", "sql", "schema", "metrics", "ops", "security"} {
		if !bytes.Contains(shell, []byte(`<main id="view-`+view+`"`)) {
			t.Fatalf("the console shell has no container for the %s view", view)
		}
	}
	// One container per view: two with the same id is a view that never
	// hides, which is how the node page's sections leaked onto every
	// other view in 0.44.0.
	for _, view := range []string{"overview", "nodes", "node", "data", "sql", "schema", "metrics", "ops", "security"} {
		if n := bytes.Count(shell, []byte(`<main id="view-`+view+`"`)); n != 1 {
			t.Fatalf("the %s view has %d containers, want exactly 1", view, n)
		}
	}
}

// TestHiddenAttributeWins: an element that ships with the hidden
// attribute must actually be hidden.
//
// The jump dialog shipped covering the whole page from first paint. Its
// markup carried `hidden` and every script that shows and hides it reads
// `.hidden`, but the stylesheet said `#jump { display: flex }` — and an
// id selector (1,0,0) outranks the user agent's `[hidden] { display:
// none }` (0,1,0). The overlay is `position: fixed; inset: 0`, so it
// swallowed every click on the console while the code believed it was
// gone.
//
// Nothing caught it: the server serves the same bytes either way, and a
// DOM test that reads an element's text passes whether or not the
// element is on top of the page. So this asserts the invariant in the
// stylesheet itself.
func TestHiddenAttributeWins(t *testing.T) {
	page, err := FS.ReadFile("index.html")
	if err != nil {
		t.Fatal(err)
	}
	css := string(page)

	// The override that makes the attribute authoritative regardless of
	// what any other rule sets.
	if !hiddenOverride.MatchString(css) {
		t.Fatal("index.html has no `[hidden] { display: none !important }` rule: any id or class rule " +
			"that sets display will leave a `hidden` element on screen while the scripts believe it is hidden")
	}

	// And every element that ships hidden is named, so a reviewer can
	// see which elements depend on the rule above.
	ids := hiddenElement.FindAllStringSubmatch(css, -1)
	if len(ids) == 0 {
		t.Fatal("no element ships with the hidden attribute; this test is watching nothing")
	}
	for _, m := range ids {
		id := m[1]
		// A rule that sets display on this element's id is exactly the
		// shape of the original bug. It is legal now that the override
		// exists, but it is worth failing on so the next person adds
		// the element to the list deliberately rather than by accident.
		rule := regexp.MustCompile(`(?s)#` + regexp.QuoteMeta(id) + `\s*\{[^}]*\bdisplay\s*:`)
		if rule.MatchString(css) && !allowedDisplayOnHidden[id] {
			t.Errorf("#%s ships with the hidden attribute and has a rule setting display on its id. "+
				"That outranks [hidden] on its own; it works only because of the !important override. "+
				"If that is intended, add %q to allowedDisplayOnHidden with a reason", id, id)
		}
	}
}

// allowedDisplayOnHidden lists elements that ship hidden AND carry an id
// rule setting display, each because its shown state needs a layout the
// element cannot get any other way.
var allowedDisplayOnHidden = map[string]bool{
	// The jump dialog centres its box with flex when shown.
	"jump": true,
	// So does the help panel, for the same reason — and safely for the
	// same reason: the [hidden] override outranks both.
	"help": true,
}

var (
	hiddenOverride = regexp.MustCompile(`\[hidden\]\s*\{[^}]*display\s*:\s*none\s*!important`)
	hiddenElement  = regexp.MustCompile(`<[a-z]+[^>]*\bid="([a-zA-Z0-9_-]+)"[^>]*\bhidden\b`)
)

// TestScriptsOnlyTouchElementsThatExist: a script that reads an element
// the markup does not have throws, and in a poll that stops the console
// updating.
//
// #hdr-reload was referenced by the overview poll and was not in the
// page. It fires only when the node serves a newer console than the tab
// is running — a rolling upgrade — so the console froze at "last updated
// never" exactly while the cluster was changing under it.
func TestScriptsOnlyTouchElementsThatExist(t *testing.T) {
	page, err := FS.ReadFile("index.html")
	if err != nil {
		t.Fatal(err)
	}
	markup := string(page)
	names, err := ScriptFiles()
	if err != nil {
		t.Fatal(err)
	}
	// An element may also be created by a script and then read back, so
	// the scripts' own generated markup counts as a definition.
	var all strings.Builder
	all.WriteString(markup)
	sources := map[string]string{}
	for _, name := range names {
		src, err := FS.ReadFile(name)
		if err != nil {
			t.Fatal(err)
		}
		sources[name] = string(src)
		all.WriteString(string(src))
	}
	defined := all.String()
	for _, name := range names {
		src := sources[name]
		for _, m := range getByID.FindAllStringSubmatch(src, -1) {
			id := m[1]
			// Optional chaining says the caller expects it to be absent.
			if strings.Contains(src, `getElementById("`+id+`")?.`) {
				continue
			}
			if !strings.Contains(defined, `id="`+id+`"`) && !strings.Contains(defined, `id=\"`+id+`\"`) {
				t.Errorf("%s reads getElementById(%q), which neither index.html nor any script creates: "+
					"the call returns null and the next property access throws", name, id)
			}
		}
	}
}

// getByID matches a literal getElementById("...") — the calls whose
// target can be checked against the markup. Computed ids are skipped.
var getByID = regexp.MustCompile(`getElementById\("([a-zA-Z0-9_-]+)"\)`)

// TestNodeViewFetchesTheNodeInTheRoute: the node page asked the server
// for node 0 on every visit.
//
// The view's cache object declared `id: 0` and nothing ever assigned the
// route's node to it, so the poll built "/api/node?id=0" — which the
// server is right to reject with a 400 — and the page rendered "Node n0"
// above the error. No server-side test could see it: the node serves the
// same bytes either way, and the wrong id is chosen in the browser.
func TestNodeViewFetchesTheNodeInTheRoute(t *testing.T) {
	src, err := FS.ReadFile("js/90-node.js")
	if err != nil {
		t.Fatal(err)
	}
	body := funcBody(string(src), "async function pollNode()")
	if body == "" {
		t.Fatal("js/90-node.js has no pollNode")
	}
	// The call, not the comment above it that quotes the same path.
	fetchAt := strings.Index(body, `fetch("/api/node?id=`)
	if fetchAt < 0 {
		t.Fatal("pollNode does not fetch /api/node?id=")
	}
	if !strings.Contains(body[:fetchAt], "ui.node") {
		t.Error("pollNode reaches /api/node?id= without reading ui.node: " +
			"the route is what says which node the page is showing, so a cached " +
			"id that nothing assigns fetches node 0 and the server returns 400")
	}
}

// funcBody returns the text between the braces of the declaration
// starting with header, or "" if it is not there. It counts braces, so a
// nested block does not end the function early; strings and comments
// holding an unbalanced brace would, and none of the console's do.
func funcBody(src, header string) string {
	i := strings.Index(src, header)
	if i < 0 {
		return ""
	}
	open := strings.Index(src[i:], "{")
	if open < 0 {
		return ""
	}
	depth := 0
	for j := i + open; j < len(src); j++ {
		switch src[j] {
		case '{':
			depth++
		case '}':
			if depth--; depth == 0 {
				return src[i+open+1 : j]
			}
		}
	}
	return ""
}

// TestTileQualifiersAreNotHeadlines: a tile is a label, one figure and
// sometimes a qualifier, and the qualifier must not be set at the
// figure's size.
//
// It used to run on after the figure inside the same 22px line, so
// "connections" read "0 (0 active, 0 idle in txn) (0 of 2 live nodes)"
// as four lines of headline type — taller than its card, spilling over
// what came below it, and stretching every other tile in its grid row to
// match.
func TestTileQualifiersAreNotHeadlines(t *testing.T) {
	page, err := FS.ReadFile("index.html")
	if err != nil {
		t.Fatal(err)
	}
	css := string(page)
	if !tileQualRule.MatchString(css) {
		t.Error("index.html has no rule setting a tile value's qualifier " +
			"(.tile .value .muted) on its own line at a smaller size")
	}
	if !tileLongRule.MatchString(css) {
		t.Error("index.html has no .tile .value.long rule: tile() marks a value " +
			"that is a phrase rather than a figure, and without the rule it is " +
			"still set at the figure's size")
	}
	core, err := FS.ReadFile("js/10-core.js")
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(core), `class="value${cls}"`) || !strings.Contains(string(core), "valueClass(text)") {
		t.Error("js/10-core.js: tile() no longer classifies long values, so the " +
			"CSS rule above can never apply")
	}
}

var (
	tileQualRule = regexp.MustCompile(`\.tile\s+\.value\s+\.muted\s*\{[^}]*display\s*:\s*block`)
	tileLongRule = regexp.MustCompile(`\.tile\s+\.value\.long\s*\{[^}]*font-size`)
)

// ---- Help: every term on the page explains itself ----
//
// The console is full of precise, opaque words — "compaction debt",
// "bare majority", "40001/s", "stats age". Each is explained by an entry
// in the glossary, keyed by the term as it is written on screen, so a
// column that says "leases" is explained without anyone wiring it up.
//
// These tests are what makes "everything on the page" true rather than
// aspirational: a new column or section whose word is not in the
// glossary fails the build, and an entry that stops matching anything on
// the page is reported as dead rather than left to rot.

// TestEveryTermOnThePageIsExplained walks the terms the page actually
// shows — table headings, section titles and tile labels — and requires
// each to resolve.
func TestEveryTermOnThePageIsExplained(t *testing.T) {
	g, err := loadGlossary()
	if err != nil {
		t.Fatal(err)
	}
	terms, err := pageTerms()
	if err != nil {
		t.Fatal(err)
	}
	if len(terms) < 100 {
		t.Fatalf("found only %d terms on the page; the extractor is broken, not the glossary", len(terms))
	}
	for _, term := range terms {
		if g.lookup(term.view, term.text) == "" {
			t.Errorf("%s: nothing explains %q (from %s). Add it to HELP in js/12-help.js — "+
				"key it %q if it means something different here than elsewhere",
				term.where, term.text, term.where, term.view+"/"+term.text)
		}
	}
}

// TestHelpEntriesSaySomething: an entry that only restates its label
// costs a click to learn nothing, which is worse than no entry at all.
func TestHelpEntriesSaySomething(t *testing.T) {
	g, err := loadGlossary()
	if err != nil {
		t.Fatal(err)
	}
	if len(g.entries) < 100 {
		t.Fatalf("the glossary has %d entries; the parser is broken", len(g.entries))
	}
	for key, text := range g.entries {
		if len(text) < 30 {
			t.Errorf("HELP[%q] is %d characters (%q): too short to add anything the label does not already say",
				key, len(text), text)
		}
		if !strings.HasSuffix(strings.TrimSpace(text), ".") {
			t.Errorf("HELP[%q] does not end in a full stop: %q", key, text)
		}
	}
}

// TestHelpGlossaryHasNoDeadEntries: an entry matching nothing on the
// page is either a term that was renamed or one that never existed, and
// either way the reader will never see it.
func TestHelpGlossaryHasNoDeadEntries(t *testing.T) {
	g, err := loadGlossary()
	if err != nil {
		t.Fatal(err)
	}
	terms, err := pageTerms()
	if err != nil {
		t.Fatal(err)
	}
	used := map[string]bool{}
	for _, term := range terms {
		if key := g.matched(term.view, term.text); key != "" {
			used[key] = true
		}
	}
	for key := range g.entries {
		if used[key] || helpKeysNotOnAPage[key] {
			continue
		}
		t.Errorf("HELP[%q] matches nothing on the page: either the term was renamed and the "+
			"entry needs its new key, or the entry is for something that is no longer shown", key)
	}
}

// helpKeysNotOnAPage are entries that explain a control in the header or
// a term the page writes in prose rather than as a heading, so the term
// extractor below will never see them.
var helpKeysNotOnAPage = map[string]bool{
	"scope": true, "range": true, "jump to": true,
	"compare": true, "annotate": true, "filter": true,
}

type glossary struct{ entries map[string]string }

// lookup mirrors helpFor in js/12-help.js: the view's own reading of the
// word, then the word, then the word without the parenthetical that
// qualifies it, then the generated-term patterns.
func (g glossary) lookup(view, term string) string {
	if key := g.matched(view, term); key != "" {
		return g.entries[key]
	}
	if generatedTerm.MatchString(term) {
		return "pattern"
	}
	return ""
}

// matched returns the glossary key a term resolves to, or "".
func (g glossary) matched(view, term string) string {
	bare := strings.TrimSpace(parenthetical.ReplaceAllString(term, ""))
	for _, key := range []string{view + "/" + term, term, view + "/" + bare, bare} {
		if _, ok := g.entries[key]; ok {
			return key
		}
	}
	return ""
}

var (
	// The glossary is a flat object of "key": "text" pairs, one per line.
	helpEntry = regexp.MustCompile(`(?m)^\s{2}"([^"]+)":\s*"((?:[^"\\]|\\.)*)",\s*$`)
	// Terms the page generates rather than writes: one column per node,
	// one row per range.
	generatedTerm = regexp.MustCompile(`^[nr]\d+$`)
	parenthetical = regexp.MustCompile(`\s*\([^)]*\)\s*$`)
)

func loadGlossary() (glossary, error) {
	src, err := FS.ReadFile("js/12-help.js")
	if err != nil {
		return glossary{}, err
	}
	// Only the object literal, so a "key": "value" pair inside a comment
	// or a later function is not read as an entry.
	text := string(src)
	start := strings.Index(text, "const HELP = {")
	if start < 0 {
		return glossary{}, errors.New("js/12-help.js has no HELP glossary")
	}
	end := strings.Index(text[start:], "\n};")
	if end < 0 {
		return glossary{}, errors.New("js/12-help.js: the HELP glossary is not closed")
	}
	entries := map[string]string{}
	for _, m := range helpEntry.FindAllStringSubmatch(text[start:start+end], -1) {
		entries[m[1]] = strings.ReplaceAll(m[2], `\"`, `"`)
	}
	return glossary{entries: entries}, nil
}

type pageTerm struct{ view, text, where string }

// pageTerms collects what the reader sees a label for: every table
// heading and section title in the markup, every heading the scripts
// generate, and every tile label.
func pageTerms() ([]pageTerm, error) {
	page, err := FS.ReadFile("index.html")
	if err != nil {
		return nil, err
	}
	var terms []pageTerm
	add := func(view, raw, where string) {
		if t := normalizeTerm(raw); t != "" {
			terms = append(terms, pageTerm{view: view, text: t, where: where})
		}
	}
	// The markup, view by view: a term is read the way its view means it.
	for _, part := range viewSplit.FindAllStringSubmatch(string(page), -1) {
		view, body := part[1], part[2]
		for _, m := range headingRE.FindAllStringSubmatch(body, -1) {
			add(view, m[2], "index.html #/"+view)
		}
	}
	names, err := ScriptFiles()
	if err != nil {
		return nil, err
	}
	for _, name := range names {
		src, err := FS.ReadFile(name)
		if err != nil {
			return nil, err
		}
		// A heading the script writes. Its view is not knowable here, so
		// it has to resolve without one.
		for _, m := range headingRE.FindAllStringSubmatch(string(src), -1) {
			add("", m[2], name)
		}
		for _, m := range tileLabel.FindAllStringSubmatch(string(src), -1) {
			add("", m[1], name)
		}
	}
	return terms, nil
}

var (
	viewSplit = regexp.MustCompile(`(?s)<main id="view-([a-z]+)"(.*?)</main>`)
	headingRE = regexp.MustCompile(`(?s)<(th|h2)\b[^>]*>(.*?)</(?:th|h2)>`)
	tileLabel = regexp.MustCompile(`\btile(?:HTML)?\("([^"]+)"`)
	// Controls and screen-reader text live inside a heading without being
	// part of the term: "Statement shapes" is the heading and the sort
	// <select> beside it is not.
	inlineControl = regexp.MustCompile(`(?s)<(select|button|input|label)\b.*?</(?:select|button|input|label)>`)
	anyTag        = regexp.MustCompile(`<[^>]*>`)
	spaces        = regexp.MustCompile(`\s+`)
)

// normalizeTerm reduces a heading to its glossary key the same way
// normTerm and labelText do in the browser.
func normalizeTerm(raw string) string {
	raw = inlineControl.ReplaceAllString(raw, " ")
	raw = anyTag.ReplaceAllString(raw, " ")
	raw = html.UnescapeString(raw)
	// A template hole is a value, not a term.
	if strings.Contains(raw, "${") {
		return ""
	}
	return strings.TrimSpace(spaces.ReplaceAllString(strings.ToLower(raw), " "))
}

// TestTileValuesAreEscaped (issue #191): tile() escapes its value, and a
// caller that needs to compose markup says so by name.
//
// tile() used to escape, then stopped so that callers could pass a
// qualifier span — but the label beside the value is still escaped, so
// nothing signalled that the second argument had become a raw-HTML slot.
// Seven call sites were passing strings the page had not formatted: a
// node's address and locality, an overload reason, and the signed-in
// role name, which is a database identifier and can be quoted into
// anything. The exposure was small (a tile shows the viewer their own
// name) but the contract was inverted, which is the part that does not
// stay small.
func TestTileValuesAreEscaped(t *testing.T) {
	core, err := FS.ReadFile("js/10-core.js")
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(core), "return renderTile(label, esc(text)") {
		t.Error("js/10-core.js: tile() no longer escapes its value; the short name must be the safe one")
	}
	names, err := ScriptFiles()
	if err != nil {
		t.Fatal(err)
	}
	for _, name := range names {
		src, err := FS.ReadFile(name)
		if err != nil {
			t.Fatal(err)
		}
		for _, call := range callsTo(string(src), "tile(") {
			for _, markup := range []string{"qual(", "contrib(", "<span", "<b>", "<a "} {
				if strings.Contains(call, markup) {
					t.Errorf("%s: tile(...) is passed composed markup (%s), which it now escapes and would render as text:\n\t%s\n"+
						"use tileHTML for a value the page composes itself", name, markup, oneLine(call))
				}
			}
		}
	}
}

// callsTo returns the text of every call to fn in src, from the opening
// parenthesis to the one that closes it. It counts parentheses, so a
// nested call does not end the outer one early; `tileHTML(` is not a
// match for `tile(` because the search starts at a word boundary.
func callsTo(src, fn string) []string {
	var out []string
	for i := 0; i+len(fn) <= len(src); i++ {
		if !strings.HasPrefix(src[i:], fn) {
			continue
		}
		if i > 0 && (isWordByte(src[i-1]) || src[i-1] == '.') {
			continue // tileHTML(, renderTile(, a.tile(
		}
		depth := 0
		for j := i + len(fn) - 1; j < len(src); j++ {
			switch src[j] {
			case '(':
				depth++
			case ')':
				if depth--; depth == 0 {
					out = append(out, src[i:j+1])
					i = j
					goto next
				}
			}
		}
	next:
	}
	return out
}

func isWordByte(b byte) bool {
	return b == '_' || (b >= 'a' && b <= 'z') || (b >= 'A' && b <= 'Z') || (b >= '0' && b <= '9')
}

func oneLine(s string) string {
	s = strings.Join(strings.Fields(s), " ")
	if len(s) > 160 {
		return s[:160] + "…"
	}
	return s
}

// TestTableDetailIsReachable (issue #194): the schema list was the only
// level there was, and everything below it lived in `title` attributes —
// which do not appear on touch, are awkward to reach from a keyboard,
// are unreadable for a forty-column table and cannot be copied.
//
// The level below it is a route, and a row leads to it by click and by
// keyboard alike, the way #149 established for node and range rows.
func TestTableDetailIsReachable(t *testing.T) {
	router, err := FS.ReadFile("js/15-router.js")
	if err != nil {
		t.Fatal(err)
	}
	parse := funcBody(string(router), "function parseRoute()")
	if parse == "" {
		t.Fatal("js/15-router.js has no parseRoute")
	}
	if !strings.Contains(parse, `schema\/`) {
		t.Error("parseRoute does not match #/schema/<table>: the detail route is unreachable and falls through to the overview")
	}
	if !strings.Contains(parse, "safeDecode") {
		t.Error("parseRoute does not decode the table segment: a name with a space or a dot arrives percent-encoded")
	}
	route := funcBody(string(router), "function route()")
	if !strings.Contains(route, "ui.table = r.table") {
		t.Error("route() never assigns ui.table, so the view cannot know which table the route names")
	}
	if !strings.Contains(route, `renderSchema(lastSchema)`) {
		t.Error("route() does not redraw the schema view: moving between the list and a table would wait for the next poll")
	}

	list, err := FS.ReadFile("js/40-schema.js")
	if err != nil {
		t.Fatal(err)
	}
	row := funcBody(string(list), "function renderSchema(d)")
	for _, want := range []string{`data-table=`, `tabindex="0"`, `role="link"`} {
		if !strings.Contains(row, want) {
			t.Errorf("a schema row carries no %s: the detail is reachable by neither pointer nor keyboard", want)
		}
	}

	detail, err := FS.ReadFile("js/45-table.js")
	if err != nil {
		t.Fatal(err)
	}
	wire := funcBody(string(detail), "function wireTableDetail()")
	if wire == "" {
		t.Fatal("js/45-table.js has no wireTableDetail")
	}
	for _, want := range []string{`addEventListener("click"`, `addEventListener("keydown"`} {
		if !strings.Contains(wire, want) {
			t.Errorf("wireTableDetail binds no %s on the list: a row reachable one way only is not reachable", want)
		}
	}
	boot, err := FS.ReadFile("js/95-boot.js")
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(boot), "wireTableDetail()") {
		t.Error("nothing calls wireTableDetail(), so the handlers it binds are never bound")
	}
}

// TestTableDetailShowsDetailAsContent (issue #194): the point of the
// view is that what was in a tooltip is now on the page. A detail put
// back into a `title` would be the same defect in a new place.
func TestTableDetailShowsDetailAsContent(t *testing.T) {
	src, err := FS.ReadFile("js/45-table.js")
	if err != nil {
		t.Fatal(err)
	}
	body := string(src)
	if strings.Contains(body, `title="`) {
		t.Error("js/45-table.js puts something in a title attribute: this view exists because the detail was in tooltips")
	}
	// Each field the issue names, and the element it lands in.
	for field, where := range map[string]string{
		"c.type":                   "renderTableColumns",
		"c.not_null":               "renderTableColumns",
		"c.default":                "renderTableColumns",
		"c.hidden":                 "renderTableColumns",
		"i.unique":                 "renderTableIndexes",
		`i.state === "write-only"`: "renderTableIndexes",
		"t.constraints":            "renderTableConstraints",
		"st.row_count":             "renderTableStats",
		"st.stale":                 "renderTableStats",
		"ANALYZE":                  "renderTableStats",
		"t.ranges":                 "renderTableRanges",
		"t.privileges":             "renderTableGrants",
		"s.tables":                 "renderTableShapes",
	} {
		fn := funcBody(body, "function "+where+"(t)")
		if fn == "" {
			t.Fatalf("js/45-table.js has no %s", where)
		}
		if !strings.Contains(fn, field) {
			t.Errorf("%s does not read %s, which the detail view is meant to show", where, field)
		}
	}
	// The DDL is the single most-requested thing from a schema browser,
	// and it is only useful if it can be copied and if it parses.
	ddl := funcBody(body, "function tableDDL(t)")
	if ddl == "" {
		t.Fatal("js/45-table.js has no tableDDL")
	}
	for _, want := range []string{"CREATE TABLE", "CREATE VIEW", "PRIMARY KEY", "CREATE ", "COMMENT ON TABLE"} {
		if !strings.Contains(ddl, want) {
			t.Errorf("tableDDL never writes %q", want)
		}
	}
	if !strings.Contains(ddl, "constraintDDL") {
		t.Error("tableDDL omits the table's constraints: DDL that silently drops a foreign key is worse than none")
	}
	if !strings.Contains(body, "SQL_RESERVED") {
		t.Error("quoteIdent does not know the reserved words, so a column called `when` produces DDL that will not parse")
	}
	if !strings.Contains(funcBody(body, "function wireTableDetail()"), "clipboard") {
		t.Error("the DDL has no copy button")
	}
}

// TestTableDetailTermsAreInTheGlossary (issue #194): the glossary covers
// every term on the page and must not regress when a view adds some. The
// two tests above it check coverage generally; this one names the terms
// this view introduced, so removing an entry fails here with the reason
// rather than as an anonymous count.
func TestTableDetailTermsAreInTheGlossary(t *testing.T) {
	g, err := loadGlossary()
	if err != nil {
		t.Fatal(err)
	}
	for _, term := range []string{
		"column", "type", "nullability", "default", "hidden",
		"index", "unique", "build state",
		"constraints", "constraint", "definition", "validated",
		"statistics", "privileges", "reconstructed ddl",
	} {
		if g.lookup("schema", term) == "" {
			t.Errorf("the table detail shows %q and nothing explains it", term)
		}
	}
}

// TestSecurityFiguresReadTheDocumentThatCarriesThem (issue #203 review):
// the Security view is fed by two documents — the cluster poll, which
// carries who you are signed in as, and /api/security, which carries
// this node's own counters — and a figure read from the wrong one is
// silently zero rather than visibly broken.
//
// That is not hypothetical: the "refused before verification" tile was
// merged reading auth_throttled_rate_limit off the cluster document,
// which has no such field, so it rendered "none" while the node refused
// hundreds a second. Nothing caught it because every test asserted on
// the API documents and none on the page.
//
// What this proves, exactly: for every helper parameter whose body
// reads a field unique to one of the two documents, the call sites this
// can trace pass that document — either the global itself, or a
// parameter its own callers only ever pass that global in. It checks
// the argument rather than the reading function's body because the bad
// read was one call away from the renderer that made it.
//
// What it does NOT prove, so a passing run is not read as more than it
// is:
//
//   - Nothing about fields both documents carry (principal, node_id).
//   - Nothing about whether a document has arrived. The follow-on
//     defect, renderAuthTiles(null) claiming "insecure" on a secure
//     cluster, was a nullness bug that no document-identity check sees.
//   - Coverage is per parameter, not per call site. jsCalls reads the
//     bodies of top-level function declarations, so a call made from an
//     arrow function or a bare module-level block — renderReplication
//     (lastCluster) inside a click listener, say — is invisible to it.
//     A parameter with one traceable call site and four untraceable
//     ones is reported exactly like one that is fully checked, and only
//     a parameter with no traceable call site at all reaches the "not
//     covered" log. So the honest reading of a pass is that some call
//     site passes the right document, not that every one does.
func TestSecurityFiguresReadTheDocumentThatCarriesThem(t *testing.T) {
	wantDoc, err := documentFields()
	if err != nil {
		t.Fatal(err)
	}
	if len(wantDoc) < 4 {
		t.Fatalf("only %d fields are unique to one document; the extractor is broken, not the console", len(wantDoc))
	}
	// Every script, concatenated the way the page assembles them: the
	// call graph crosses files (renderSecurity is defined in 92-ops.js
	// and called from 30-overview.js and 95-boot.js), and reading one
	// file would leave those call sites invisible — which silently
	// stopped an earlier version of this test from catching the bug it
	// was written for.
	names, err := ScriptFiles()
	if err != nil {
		t.Fatal(err)
	}
	var all strings.Builder
	for _, name := range names {
		src, err := FS.ReadFile(name)
		if err != nil {
			t.Fatal(err)
		}
		all.Write(src)
		all.WriteString("\n")
	}
	body := all.String()
	funcs := jsFunctions(body)
	if len(funcs) < 40 {
		t.Fatalf("parsed %d functions out of the console scripts; the parser is broken", len(funcs))
	}

	// Which document each parameter must be, from the fields its body
	// reads off it. A parameter reading fields unique to both documents
	// is the bug this test is about, stated directly.
	want := map[paramRef]string{}
	for _, f := range funcs {
		for i, param := range f.params {
			for field, doc := range wantDoc {
				if !readsField(f.body, param, field) {
					continue
				}
				ref := paramRef{fn: f.name, param: param, idx: i}
				if prev, ok := want[ref]; ok && prev != doc {
					t.Errorf("%s(%s) reads fields unique to both documents (%s and %s): "+
						"one parameter cannot be both", f.name, param, prev, doc)
				}
				want[ref] = doc
			}
		}
	}
	if len(want) == 0 {
		t.Fatal("no helper reads a document-specific field off a parameter; this test is watching nothing")
	}

	// What each parameter is actually handed, resolved to a global where
	// it can be: a global passed straight in, or one passed through a
	// caller's own parameter. Iterated to a fixed point so one level of
	// indirection (renderAuthTiles(lastCluster, secDoc) -> authThrottleText(sec))
	// resolves.
	origins := map[paramRef]map[string]bool{}
	calls := jsCalls(body, funcs)
	for round := 0; round < len(funcs)+2; round++ {
		changed := false
		for _, c := range calls {
			target, ok := funcs.byName(c.callee)
			if !ok {
				continue
			}
			for i, arg := range c.args {
				if i >= len(target.params) {
					break
				}
				ref := paramRef{fn: target.name, param: target.params[i], idx: i}
				for _, origin := range resolveArg(arg, c.caller, funcs, origins) {
					if origins[ref] == nil {
						origins[ref] = map[string]bool{}
					}
					if !origins[ref][origin] {
						origins[ref][origin] = true
						changed = true
					}
				}
			}
		}
		if !changed {
			break
		}
	}

	// A parameter whose arguments this test cannot resolve — passed from
	// a callback, or computed — is outside its reach, not a defect. It is
	// reported so the limit is visible, and the covered count is asserted
	// below so the check cannot quietly decay into covering nothing.
	covered := 0
	for ref, doc := range want {
		got := origins[ref]
		if len(got) == 0 {
			t.Logf("not covered: %s(%s) reads a field only %s carries, but every call passes it "+
				"something this test cannot trace to a document", ref.fn, ref.param, doc)
			continue
		}
		covered++
		for origin := range got {
			if origin == doc {
				continue
			}
			t.Errorf("%s(%s) reads a field only %s carries, but is handed %s. "+
				"That document has no such field, so the figure renders as zero under every condition.",
				ref.fn, ref.param, doc, origin)
		}
	}
	if covered == 0 {
		t.Error("no document-specific parameter had a traceable call site: this test is no longer checking anything")
	}

	// A non-zero count is too low a floor on its own. readsField sees
	// `name.field` and `name["field"]` and nothing else, so destructuring
	// the document in a signature or a body takes that parameter out of
	// the extractor's sight — the check quietly stops watching the
	// function it was written for, while coverage elsewhere keeps the
	// count non-zero and the run green. That is a silent coverage loss
	// rather than a false positive, which makes it the worse of the two.
	//
	// So the figures that actually broke are pinned by name. A refactor
	// that hides one of these from the extractor fails here, saying which
	// function stopped being watched, instead of passing quietly.
	// renderAuthTiles is deliberately not here: the only field it reads
	// off the cluster document is principal, which /api/security
	// carries too, so it is legitimately outside what this test can
	// speak to — the same limit the doc comment names above.
	for _, fn := range []string{"authThrottleText"} {
		found := false
		for ref := range want {
			if ref.fn == fn && len(origins[ref]) > 0 {
				found = true
				break
			}
		}
		if !found {
			t.Errorf("%s no longer has a document-specific parameter this test can both read and trace, "+
				"so it is no longer covered. It reads a field only one of the two documents carries, and it is "+
				"the figure this test exists for. If it was refactored to destructure the document, or to take it "+
				"from a global rather than a parameter, extend readsField or the argument tracer to follow it — "+
				"do not leave the check silently watching nothing", fn)
		}
	}
}

// documentFields maps each JSON field unique to one of the two documents
// to the global that carries it. Fields both structs declare are absent:
// reading those off either document is legitimate.
func documentFields() (map[string]string, error) {
	sec, err := structJSONFields("../security_api.go", "SecurityStatus")
	if err != nil {
		return nil, err
	}
	cluster, err := structJSONFields("../cluster_api.go", "ClusterStatus")
	if err != nil {
		return nil, err
	}
	want := map[string]string{}
	for f := range sec {
		if !cluster[f] {
			want[f] = "secDoc"
		}
	}
	for f := range cluster {
		if !sec[f] {
			want[f] = "lastCluster"
		}
	}
	return want, nil
}

func structJSONFields(path, name string) (map[string]bool, error) {
	src, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	text := string(src)
	start := strings.Index(text, "type "+name+" struct {")
	if start < 0 {
		return nil, errors.New(path + ": no type " + name)
	}
	end := strings.Index(text[start:], "\n}")
	if end < 0 {
		return nil, errors.New(path + ": type " + name + " is not closed")
	}
	fields := map[string]bool{}
	for _, m := range jsonTag.FindAllStringSubmatch(text[start:start+end], -1) {
		fields[m[1]] = true
	}
	return fields, nil
}

// readsField reports whether body reads field off name, by property or
// by bracket. Destructuring is not recognised; the console does not use
// it on these documents, and a test that quietly half-covers is worse
// than one whose limits are written down.
func readsField(body, name, field string) bool {
	return strings.Contains(body, name+"."+field) ||
		strings.Contains(body, name+`["`+field+`"]`)
}

// paramRef identifies one parameter of one function.
type paramRef struct {
	fn, param string
	idx       int
}

type jsFunc struct {
	name   string
	params []string
	body   string
}

type jsFuncs []jsFunc

func (fs jsFuncs) byName(n string) (jsFunc, bool) {
	for _, f := range fs {
		if f.name == n {
			return f, true
		}
	}
	return jsFunc{}, false
}

func jsFunctions(body string) jsFuncs {
	var out jsFuncs
	for _, m := range jsFuncDecl.FindAllStringSubmatch(body, -1) {
		f := jsFunc{name: m[1], body: funcBody(body, m[0])}
		for _, p := range strings.Split(m[2], ",") {
			if p = strings.TrimSpace(p); p != "" {
				f.params = append(f.params, p)
			}
		}
		out = append(out, f)
	}
	return out
}

type jsCall struct {
	caller string
	callee string
	args   []string
}

// jsCalls finds calls with plain identifier or member arguments, which
// is all this check can resolve. A call whose argument is an expression
// is skipped, and the "handed nothing" branch above reports a parameter
// left with no resolvable call site rather than passing it silently.
func jsCalls(body string, funcs jsFuncs) []jsCall {
	var out []jsCall
	for _, f := range funcs {
		for _, m := range jsCallSite.FindAllStringSubmatch(f.body, -1) {
			c := jsCall{caller: f.name, callee: m[1]}
			for _, a := range strings.Split(m[2], ",") {
				c.args = append(c.args, strings.TrimSpace(a))
			}
			out = append(out, c)
		}
	}
	return out
}

// resolveArg maps one argument to the globals it can be: the global
// itself, or whatever its caller's own parameter is handed.
func resolveArg(arg, caller string, funcs jsFuncs, origins map[paramRef]map[string]bool) []string {
	switch arg {
	case "secDoc", "lastCluster":
		return []string{arg}
	}
	f, ok := funcs.byName(caller)
	if !ok {
		return nil
	}
	for i, p := range f.params {
		if p != arg {
			continue
		}
		var out []string
		for o := range origins[paramRef{fn: caller, param: p, idx: i}] {
			out = append(out, o)
		}
		return out
	}
	return nil
}

var (
	// A top-level function declaration, its name and its parameter list.
	jsFuncDecl = regexp.MustCompile(`(?m)^function ([A-Za-z_$][A-Za-z0-9_$]*)\(([^)]*)\)`)
	// A call with identifier-or-member arguments only.
	jsCallSite = regexp.MustCompile(`([A-Za-z_$][A-Za-z0-9_$]*)\(([A-Za-z_$][A-Za-z0-9_$.]*(?:,\s*[A-Za-z_$][A-Za-z0-9_$.]*)*)\)`)
	jsonTag    = regexp.MustCompile("`json:\"([a-z0-9_]+)")
)
