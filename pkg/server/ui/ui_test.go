package ui

import (
	"bytes"
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
