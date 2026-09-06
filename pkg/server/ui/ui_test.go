package ui

import (
	"bytes"
	"errors"
	"html"
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
	if !strings.Contains(string(core), `class="value${valueClass(value)}"`) {
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
	tileLabel = regexp.MustCompile(`\btile\("([^"]+)"`)
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
