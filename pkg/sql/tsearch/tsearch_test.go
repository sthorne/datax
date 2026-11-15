package tsearch

import (
	"fmt"
	"math"
	"math/rand"
	"strings"
	"testing"
)

func mustConfig(t testing.TB, name string) *Config {
	t.Helper()
	cfg, err := LookupConfig(name)
	if err != nil {
		t.Fatal(err)
	}
	return cfg
}

func TestTokenize(t *testing.T) {
	for _, c := range []struct{ in, want string }{
		{"The quick-brown fox's 2nd jump", "the quick brown fox s 2nd jump"},
		{"  ", ""},
		{"snake_case and camelCase", "snake case and camelcase"},
		{"Pi is 3.14, not 3 or 42.", "pi is 3.14 not 3 or 42"},
		{"version 1.2.3", "version 1.2 3"},
		{"mail Bob.Smith+tag@Example.COM now", "mail bob.smith+tag@example.com now"},
		{"see https://www.example.com/a/b.html?q=1", "see https www.example.com a b.html q 1"},
		{"open notes.txt please.", "open notes.txt please"},
		{"e.g. U.S.A. and i.e.", "e g u s a and i e"},
		{"Ünïcödé ÉCOLE straße 東京 Москва", "ünïcödé école straße 東京 москва"},
		{"abc123 x9 9x", "abc123 x9 9x"},
		{"a@b not-an-email@ @c", "a b not an email c"},
		{"semi;colon,comma.period!bang", "semi colon comma.period bang"},
		{"bad \xff utf8", "bad utf8"},
	} {
		var got []string
		for _, tok := range Tokenize(c.in) {
			got = append(got, tok.Text)
			if strings.ToLower(c.in[tok.Start:tok.End]) != tok.Text {
				t.Errorf("%q: token %q does not span %q", c.in, tok.Text, c.in[tok.Start:tok.End])
			}
		}
		if s := strings.Join(got, " "); s != c.want {
			t.Errorf("Tokenize(%q) = %q, want %q", c.in, s, c.want)
		}
	}
	// Look-ahead is bounded: a long run of what could start an address
	// or a host name is tokenized in time linear in its length.
	long := strings.Repeat("a.", 200000)
	if toks := Tokenize(long); len(toks) != 200000 {
		t.Errorf("%d tokens in the long dotted run", len(toks))
	}
	if toks := Tokenize(strings.Repeat("x", 100) + "@example.com"); len(toks) != 2 || toks[1].Kind != TokHost {
		t.Errorf("an over-long local part: %+v", toks)
	}
	kinds := map[string]TokenKind{"word": TokWord, "42": TokNumber, "3.5": TokNumber, "a@b.org": TokEmail, "b.org": TokHost}
	for text, want := range kinds {
		toks := Tokenize(text)
		if len(toks) != 1 || toks[0].Kind != want {
			t.Errorf("Tokenize(%q) = %+v, want one token of kind %d", text, toks, want)
		}
	}
}

func TestConfigs(t *testing.T) {
	for _, name := range []string{"english", "English", "pg_catalog.english", " simple "} {
		if _, err := LookupConfig(name); err != nil {
			t.Errorf("LookupConfig(%q): %v", name, err)
		}
	}
	if _, err := LookupConfig("klingon"); err == nil {
		t.Error("an unknown configuration was accepted")
	}
	english := mustConfig(t, "english")
	if len(english.stop) != 127 {
		t.Errorf("english has %d stop words, the Snowball list has 127", len(english.stop))
	}
	// A token with a digit, an address and a host name are not English
	// words: neither stopped nor stemmed.
	for in, want := range map[string]string{
		"The Running of the 2nd Bulls":      "'2nd':5 'bull':6 'run':2",
		"is was the":                        "",
		"cities3 Cities":                    "'citi':2 'cities3':1",
		"ask info@lists.example.org please": "'ask':1 'info@lists.example.org':2 'pleas':3",
		"running.example.com is running":    "'run':3 'running.example.com':1",
	} {
		v, err := ToVector(english, in)
		if err != nil || v.String() != want {
			t.Errorf("english %q = %s (%v), want %s", in, v, err, want)
		}
	}
	v, _ := ToVector(mustConfig(t, "simple"), "The Running of the Bulls")
	if v.String() != "'bulls':5 'of':3 'running':2 'the':1,4" {
		t.Errorf("simple: %s", v)
	}
}

// Results PostgreSQL's documentation gives for the same inputs
// (chapters "Text Search Types" and "Controlling Text Search").
func TestToVectorDocumented(t *testing.T) {
	english := mustConfig(t, "english")
	for in, want := range map[string]string{
		"a fat  cat sat on a mat - it ate a fat rats": "'ate':9 'cat':3 'fat':2,11 'mat':7 'rat':12 'sat':4",
		"The Fat Rats": "'fat':2 'rat':3",
		"The quick brown foxes jumped over the lazy dogs": "'brown':3 'dog':9 'fox':4 'jump':5 'lazi':8 'quick':2",
	} {
		v, err := ToVector(english, in)
		if err != nil || v.String() != want {
			t.Errorf("to_tsvector(%q) = %s (%v), want %s", in, v, err, want)
		}
	}
}

func TestVectorText(t *testing.T) {
	for in, want := range map[string]string{
		"a fat cat sat on a mat and ate a fat rat":                            "'a' 'and' 'ate' 'cat' 'fat' 'mat' 'on' 'rat' 'sat'",
		"the lexeme '    ' contains spaces":                                   "'    ' 'contains' 'lexeme' 'spaces' 'the'",
		"'Joe''s' contains a quote":                                           "'Joe''s' 'a' 'contains' 'quote'",
		"a:1 fat:2 cat:3 sat:4 on:5 a:6 mat:7 and:8 ate:9 a:10 fat:11 rat:12": "'a':1,6,10 'and':8 'ate':9 'cat':3 'fat':2,11 'mat':7 'on':5 'rat':12 'sat':4",
		"a:1A fat:2B,4C cat:5D":                                               "'a':1A 'cat':5 'fat':2B,4C",
		"The Fat Rats":                                                        "'Fat' 'Rats' 'The'",
		"b:3,1,3 b:2a":                                                        "'b':1,2A,3",
		"x:99999":                                                             "'x':16383",
		`back\ slash 'q\'uote'`:                                               `'back slash' 'q''uote'`,
		"":                                                                    "",
	} {
		v, err := ParseVector(in)
		if err != nil {
			t.Errorf("%q: %v", in, err)
			continue
		}
		if v.String() != want {
			t.Errorf("%q::tsvector = %s, want %s", in, v, want)
		}
		// The text form reads back as the same vector.
		again, err := ParseVector(v.String())
		if err != nil || again.String() != v.String() {
			t.Errorf("%s does not read back: %s (%v)", v, again, err)
		}
	}
	for _, bad := range []string{"a:", "a:0", "a:x", "a:1,", "'open", "a:1b2", "''", strings.Repeat("x", MaxLexemeLen+1)} {
		if v, err := ParseVector(bad); err == nil {
			t.Errorf("%.20q was accepted as %s", bad, v)
		}
	}
	// Positions past the limit are dropped; a word too long to index is
	// skipped rather than refused.
	many := strings.Repeat("w ", MaxPositions+50)
	v, err := ToVector(mustConfig(t, "simple"), many+strings.Repeat("y", MaxLexemeLen+1)+" z")
	if err != nil || len(v) != 2 || len(v[0].Pos) != MaxPositions || v[1].Lexeme != "z" {
		t.Errorf("limits: %d entries, %v", len(v), err)
	}
}

func TestVectorOps(t *testing.T) {
	v, _ := ParseVector("fat:2,4 cat:3 rat:5A")
	if got := v.SetWeight(WeightB).String(); got != "'cat':3B 'fat':2B,4B 'rat':5B" {
		t.Errorf("setweight: %s", got)
	}
	if got := v.Strip().String(); got != "'cat' 'fat' 'rat'" {
		t.Errorf("strip: %s", got)
	}
	// PostgreSQL's example: 'fat:2,4 cat:3 rat:5A' || 'fat:1 dog:2'.
	o, _ := ParseVector("fat:1 dog:2")
	c, err := v.Concat(o)
	if err != nil || c.String() != "'cat':3 'dog':7 'fat':2,4,6 'rat':5A" {
		t.Errorf("concat: %s (%v)", c, err)
	}
	if fmt.Sprint(v.Lexemes()) != "[cat fat rat]" {
		t.Errorf("lexemes: %v", v.Lexemes())
	}
}

func TestQueryText(t *testing.T) {
	for in, want := range map[string]string{
		"fat & rat":                 "'fat' & 'rat'",
		"fat & (rat | cat)":         "'fat' & ( 'rat' | 'cat' )",
		"fat & rat & ! cat":         "'fat' & 'rat' & !'cat'",
		"fat:ab & cat":              "'fat':AB & 'cat'",
		"super:*":                   "'super':*",
		"supern:*A & star:A*B":      "'supern':*A & 'star':*AB",
		"fat <-> rat":               "'fat' <-> 'rat'",
		"fat<2>rat":                 "'fat' <2> 'rat'",
		"a | b & c":                 "'a' | 'b' & 'c'",
		"(a | b) & c":               "( 'a' | 'b' ) & 'c'",
		"a & b <-> c":               "'a' & 'b' <-> 'c'",
		"(a & b) <-> c":             "( 'a' & 'b' ) <-> 'c'",
		"!a <-> b":                  "!'a' <-> 'b'",
		"!(a <-> b)":                "!( 'a' <-> 'b' )",
		"!(a | b)":                  "!( 'a' | 'b' )",
		"!!a":                       "!!'a'",
		"a <-> (b <-> c)":           "'a' <-> ( 'b' <-> 'c' )",
		"a <-> b <-> c":             "'a' <-> 'b' <-> 'c'",
		"'two words' & 'Joe''s'":    "'two words' & 'Joe''s'",
		"Fat":                       "'Fat'",
		"  ":                        "",
		"a <0> b":                   "'a' <0> 'b'",
		`'a\\b'`:                    `'a\\b'`,
		"((a))":                     "'a'",
		"a|b|c":                     "'a' | 'b' | 'c'",
		"a & (b & c)":               "'a' & 'b' & 'c'",
		"a <-> b | c <3> d & !e:*c": "'a' <-> 'b' | 'c' <3> 'd' & !'e':*C",
	} {
		q, err := ParseQuery(in)
		if err != nil {
			t.Errorf("%q: %v", in, err)
			continue
		}
		if q.String() != want {
			t.Errorf("%q::tsquery = %s, want %s", in, q, want)
		}
		again, err := ParseQuery(q.String())
		if err != nil || again.String() != q.String() {
			t.Errorf("%s does not read back: %s (%v)", q, again, err)
		}
	}
	for _, bad := range []string{"a &", "& a", "a b", "(a", "a)", "a <x> b", "a <-> ", "a:", "a:z", "!", "a | | b", "a <99999> b", "a <-1> b", strings.Repeat("(", 300) + "a" + strings.Repeat(")", 300)} {
		if q, err := ParseQuery(bad); err == nil {
			t.Errorf("%.20q was accepted as %s", bad, q)
		}
	}
}

// The query functions against the results PostgreSQL's documentation
// gives ("Parsing Queries"), and the stop-word handling it describes.
func TestQueryFunctionsDocumented(t *testing.T) {
	english := mustConfig(t, "english")
	toQuery := func(s string) string {
		q, err := ToQuery(english, s)
		if err != nil {
			return "error: " + err.Error()
		}
		return q.String()
	}
	for _, c := range []struct{ fn, in, want string }{
		{"to", "The & Fat & Rats", "'fat' & 'rat'"},
		{"to", "Fat | Rats:AB", "'fat' | 'rat':AB"},
		{"to", "supern:*A & star:A*B", "'supern':*A & 'star':*AB"},
		{"to", "'supernovae stars' & !crab", "'supernova' <-> 'star' & !'crab'"},
		{"to", "fat & (rats | cats) & !dog", "'fat' & ( 'rat' | 'cat' ) & !'dog'"},
		{"to", "fatal <-> error", "'fatal' <-> 'error'"},
		{"to", "postgres:*", "'postgr':*"},
		// Stop words vanish; a phrase keeps its width.
		{"to", "the", ""},
		{"to", "!the", ""},
		{"to", "the | a", ""},
		{"to", "cat & the", "'cat'"},
		{"to", "the | cat", "'cat'"},
		{"to", "cat <-> the <-> dog", "'cat' <2> 'dog'"},
		{"to", "the <-> cat <-> dog", "'cat' <-> 'dog'"},
		{"to", "cat <-> dog <-> the", "'cat' <-> 'dog'"},
		{"to", "cat <-> (the <-> dog)", "'cat' <2> 'dog'"},
		{"to", "(cat <-> a) <2> (the <-> dog)", "'cat' <4> 'dog'"},
		{"to", "well-known", "'well' <-> 'known'"},
		{"to", "fat & & rat", "error: syntax error in tsquery: \"fat & & rat\""},
		{"plain", "The Fat Rats", "'fat' & 'rat'"},
		{"plain", "The Fat & Rats:C", "'fat' & 'rat' & 'c'"},
		{"plain", "the of", ""},
		{"phrase", "The Fat Rats", "'fat' <-> 'rat'"},
		{"phrase", "The Fat & Rats:C", "'fat' <-> 'rat' <-> 'c'"},
		{"phrase", "the cats ate the rats", "'cat' <-> 'ate' <2> 'rat'"},
		{"phrase", "cats ate rats", "'cat' <-> 'ate' <-> 'rat'"},
		{"web", "The fat rats", "'fat' & 'rat'"},
		{"web", `"supernovae stars" -crab`, "'supernova' <-> 'star' & !'crab'"},
		{"web", `"sad cat" or "fat rat"`, "'sad' <-> 'cat' | 'fat' <-> 'rat'"},
		{"web", `signal -"segmentation fault"`, "'signal' & !( 'segment' <-> 'fault' )"},
		{"web", `""" )( dummy \\ query <->`, "'dummi' & 'queri'"},
		{"web", "cats OR dogs birds", "'cat' | 'dog' & 'bird'"},
		{"web", `"unterminated phrase`, "'untermin' & 'phrase'"},
		{"web", "-the", ""},
		{"web", "or", ""},
	} {
		var got string
		switch c.fn {
		case "to":
			got = toQuery(c.in)
		case "plain":
			got = PlainToQuery(english, c.in).String()
		case "phrase":
			got = PhraseToQuery(english, c.in).String()
		case "web":
			got = WebsearchToQuery(english, c.in).String()
		}
		if got != c.want {
			t.Errorf("%s(%q) = %s, want %s", c.fn, c.in, got, c.want)
		}
	}
	if q, _ := ToQuery(mustConfig(t, "simple"), "The & Fat"); q.String() != "'the' & 'fat'" {
		t.Errorf("simple: %s", q)
	}
}

func match(t *testing.T, vector, query string) bool {
	t.Helper()
	v, err := ParseVector(vector)
	if err != nil {
		t.Fatalf("%q: %v", vector, err)
	}
	q, err := ParseQuery(query)
	if err != nil {
		t.Fatalf("%q: %v", query, err)
	}
	return Match(v, q)
}

func TestMatch(t *testing.T) {
	const doc = "a:1 fat:2 cat:3 sat:4 on:5 a:6 mat:7 and:8 ate:9 a:10 fat:11 rat:12"
	for _, c := range []struct {
		vector, query string
		want          bool
	}{
		// PostgreSQL's documented examples.
		{"a fat cat sat on a mat and ate a fat rat", "cat & rat", true},
		{"a fat cat sat on a mat and ate a fat rat", "fat & cow", false},
		{"fatal:1 error:2", "fatal <-> error", true},
		{"error:1 is:2 not:3 fatal:4", "fatal <-> error", false},
		{"postgradu:1", "postgr:*", true},
		// Boolean logic.
		{doc, "cat | cow", true},
		{doc, "cow | dog", false},
		{doc, "!cow", true},
		{doc, "!cat", false},
		{doc, "cat & !cow", true},
		{doc, "cat & !rat", false},
		{doc, "!(cow | dog) & mat", true},
		{doc, "", false},
		{"", "!cat", true},
		{"", "cat", false},
		// Prefixes.
		{doc, "ca:*", true},
		{doc, "c:* & r:* & !z:*", true},
		{doc, "cab:*", false},
		{doc, "cat:*", true},
		// Phrases: exact distances, in order.
		{doc, "fat <-> cat", true},
		{doc, "cat <-> fat", false},
		{doc, "fat <-> rat", true},
		{doc, "fat <-> cat <-> sat", true},
		{doc, "fat <-> cat <-> on", false},
		{doc, "fat <2> sat", true},
		{doc, "fat <3> sat", false},
		{doc, "cat <-> (sat <-> on)", true},
		{doc, "(fat <-> cat) <-> (sat <-> on)", true},
		{doc, "(fat <-> cat) <2> (sat <-> on)", false},
		{doc, "cat <0> cat", true},
		{doc, "cat <0> fat", false},
		{doc, "a <-> fat <-> (cat | rat)", true},
		{doc, "a <-> fat <-> (cow | rat)", true},
		{doc, "a <-> fat <-> (cow | dog)", false},
		{doc, "fa:* <-> ra:*", true},
		{doc, "fa:* <-> ma:*", false},
		// Negation inside a phrase: "a fat that is not followed by cat".
		{doc, "fat <-> !cat", true},
		{"fat:1 cat:2", "fat <-> !cat", false},
		{doc, "!fat <-> cat", false},
		{doc, "!sat <-> on", false},
		{doc, "!cat <-> on", true},
		{doc, "!(fat <-> cat)", false},
		{doc, "!(cat <-> fat)", true},
		{doc, "!cow <-> cat", true},
		// Weights.
		{"cat:3A fat:2", "cat:A", true},
		{"cat:3A fat:2", "cat:B", false},
		{"cat:3A fat:2", "cat:AB", true},
		{"cat:3A fat:2", "fat:D", true},
		{"cat:3A fat:2", "fat:A", false},
		{"cat:3A,7 fat:2", "fat <-> cat:A", true},
		{"cat:3,7A fat:2", "fat <-> cat:A", false},
		{"cat:3A catalog:9B", "cat:*B", true},
		// Without positions: words match whatever the weights, phrases
		// never do.
		{"cat fat", "cat & fat", true},
		{"cat fat", "cat:A", true},
		{"cat fat", "fat <-> cat", false},
		{"cat fat", "!(fat <-> cat)", true},
	} {
		if got := match(t, c.vector, c.query); got != c.want {
			t.Errorf("%q @@ %q = %v, want %v", c.vector, c.query, got, c.want)
		}
	}
	english := mustConfig(t, "english")
	for _, c := range []struct {
		text, query string
		want        bool
	}{
		{"fat cats ate fat rats", "fat & rat", true},
		{"fatal error", "fatal <-> error", true},
		{"error is not fatal", "fatal <-> error", false},
		{"postgraduate", "postgres:*", true},
		{"The cat in the hat", "cat <-> hat", false},
		{"The cat in the hat", "cat <3> hat", true},
		{"The cat in the hat", "cat <-> in <-> the <-> hat", true}, // stop words widen the phrase
	} {
		v, _ := ToVector(english, c.text)
		q, err := ToQuery(english, c.query)
		if err != nil {
			t.Fatal(err)
		}
		if got := Match(v, q); got != c.want {
			t.Errorf("to_tsvector(%q) @@ to_tsquery(%q) [%s @@ %s] = %v, want %v", c.text, c.query, v, q, got, c.want)
		}
	}
}

func rank(t *testing.T, vector, query string, norm int) float64 {
	t.Helper()
	v, _ := ParseVector(vector)
	q, err := ParseQuery(query)
	if err != nil {
		t.Fatal(err)
	}
	return Rank(DefaultWeights, v, q, norm)
}

func near(a, b float64) bool { return math.Abs(a-b) <= 1e-6*math.Max(1, math.Abs(b)) }

func TestRank(t *testing.T) {
	// One occurrence at the default weight: 0.1 / (pi^2/6).
	if r := rank(t, "cat:1", "cat", 0); !near(r, 0.0607927) {
		t.Errorf("one occurrence: %v", r)
	}
	// Further occurrences add 0.1/4, 0.1/9, ...
	if r := rank(t, "cat:1,5,9", "cat", 0); !near(r, (0.1+0.1/4+0.1/9)/1.64493406685) {
		t.Errorf("three occurrences: %v", r)
	}
	// The heaviest occurrence counts in full wherever it is.
	if r := rank(t, "cat:1,5A", "cat", 0); !near(r, (1.0+(0.1+1.0/4)-1.0/4)/1.64493406685) {
		t.Errorf("weighted: %v", r)
	}
	// An OR averages over its terms; a missing one adds nothing.
	if a, b := rank(t, "cat:1", "cat | dog", 0), rank(t, "cat:1", "cat", 0); !near(a, b/2) {
		t.Errorf("or: %v, want half of %v", a, b)
	}
	if r := rank(t, "cow:1", "cat", 0); r != 0 {
		t.Errorf("no occurrence: %v", r)
	}
	// An AND scores pairs by distance: adjacent beats distant.
	adjacent, distant := rank(t, "cat:1 rat:2", "cat & rat", 0), rank(t, "cat:1 rat:40", "cat & rat", 0)
	if !(adjacent > distant && distant > 0) {
		t.Errorf("and: adjacent %v, distant %v", adjacent, distant)
	}
	if want := math.Sqrt(0.1 * 0.1 / (1.005 + 0.05*math.Exp(1/1.5-2))); !near(adjacent, want) {
		t.Errorf("and, adjacent: %v, want %v", adjacent, want)
	}
	// An AND one of whose words is absent still matches nothing but
	// ranks as a tiny positive number, as in PostgreSQL.
	if r := rank(t, "cat:1", "cat & rat", 0); r != 1e-20 {
		t.Errorf("and with a missing word: %v", r)
	}
	// Weights and normalization.
	v, _ := ParseVector("cat:1A")
	q, _ := ParseQuery("cat")
	if r := Rank([4]float64{0.1, 0.2, 0.4, 0.5}, v, q, 0); !near(r, 0.5/1.64493406685) {
		t.Errorf("custom weights: %v", r)
	}
	base := rank(t, "cat:1 dog:2 emu:3", "cat", 0)
	for norm, want := range map[int]float64{
		NormLogLength:         base / math.Log2(4),
		NormLength:            base / 3,
		NormUnique:            base / 3,
		NormLogUnique:         base / math.Log2(4),
		NormSelf:              base / (base + 1),
		NormLength | NormSelf: (base / 3) / (base/3 + 1),
	} {
		if r := rank(t, "cat:1 dog:2 emu:3", "cat", norm); !near(r, want) {
			t.Errorf("normalization %d: %v, want %v", norm, r, want)
		}
	}
	if r := Rank(DefaultWeights, nil, q, 0); r != 0 {
		t.Errorf("empty vector: %v", r)
	}
}

func TestRankCD(t *testing.T) {
	cd := func(vector, query string, norm int) float64 {
		v, _ := ParseVector(vector)
		q, err := ParseQuery(query)
		if err != nil {
			t.Fatal(err)
		}
		return RankCD(DefaultWeights, v, q, norm)
	}
	// Two adjacent words: one cover of two words, no noise.
	if r := cd("cat:1 rat:2", "cat & rat", 0); !near(r, 0.1) {
		t.Errorf("adjacent: %v", r)
	}
	// Three other words inside the cover divide it by four.
	if r := cd("cat:1 rat:5", "cat & rat", 0); !near(r, 0.1/4) {
		t.Errorf("apart: %v", r)
	}
	// Each cover adds: two separate pairs.
	if r := cd("cat:1,10 rat:2,11", "cat & rat", 0); !(r > 0.2-1e-9) {
		t.Errorf("two covers: %v", r)
	}
	// A single word covers itself; heavier words weigh more.
	if a, b := cd("cat:1,2,3", "cat", 0), cd("cat:1A", "cat", 0); !near(a, 0.3) || !near(b, 1.0) {
		t.Errorf("single word: %v, weighted: %v", a, b)
	}
	if r := cd("cow:1", "cat", 0); r != 0 {
		t.Errorf("no match: %v", r)
	}
	// A phrase is covered only where it stands as a phrase.
	if r := cd("cat:1,7 rat:5,8", "cat <-> rat", 0); !near(r, 0.1) {
		t.Errorf("phrase: %v", r)
	}
	if a, b := cd("cat:1 rat:2 x:3 y:4", "cat & rat", NormLength), cd("cat:1 rat:2 x:3 y:4", "cat & rat", 0); !near(a, b/4) {
		t.Errorf("normalized by length: %v, want a quarter of %v", a, b)
	}
}

func TestHeadline(t *testing.T) {
	english := mustConfig(t, "english")
	const doc = "The most common type of search is to find all documents containing given query terms and return them in order of their similarity to the query."
	q, _ := ToQuery(english, "query & similarity")
	o := DefaultHeadlineOptions()
	got := Headline(english, doc, q, o)
	for _, want := range []string{"<b>query</b> terms", "<b>similarity</b> to the <b>query</b>"} {
		if !strings.Contains(got, want) {
			t.Errorf("headline %q lacks %q", got, want)
		}
	}
	// Custom markers, a short window that starts at the first hit.
	o, err := ParseHeadlineOptions(`StartSel = "[", StopSel=], MaxWords=6, MinWords=3`)
	if err != nil {
		t.Fatal(err)
	}
	if got := Headline(english, doc, q, o); got != "[similarity] to the [query]" {
		t.Errorf("short headline: %q", got)
	}
	// Prefix operands mark every word they match; stop words in the
	// document are never marked.
	p, _ := ToQuery(english, "docu:* | the")
	if got := Headline(english, "The documents and the documentation", p, o); got != "[documents] and the [documentation]" {
		t.Errorf("prefix headline: %q", got)
	}
	// No hit: the start of the document.
	none, _ := ToQuery(english, "zebra")
	if got := Headline(english, doc, none, o); got != "The most common" {
		t.Errorf("no hit: %q", got)
	}
	all, _ := ParseHeadlineOptions("HighlightAll=true")
	if got := Headline(english, "  cats, and dogs!  ", mustQuery(t, english, "dog"), all); got != "  cats, and <b>dogs</b>!  " {
		t.Errorf("highlight all: %q", got)
	}
	if got := Headline(english, "", q, DefaultHeadlineOptions()); got != "" {
		t.Errorf("empty document: %q", got)
	}
	for _, bad := range []string{"Bogus=1", "MaxWords=x", "MinWords=50", "StartSel", "HighlightAll=maybe"} {
		if _, err := ParseHeadlineOptions(bad); err == nil {
			t.Errorf("options %q were accepted", bad)
		}
	}
	if _, err := ParseHeadlineOptions("ShortWord=3, MaxFragments=2, FragmentDelimiter=\" ... \""); err != nil {
		t.Errorf("ignored options: %v", err)
	}
}

func mustQuery(t *testing.T, cfg *Config, s string) Query {
	t.Helper()
	q, err := ToQuery(cfg, s)
	if err != nil {
		t.Fatal(err)
	}
	return q
}

func TestBinary(t *testing.T) {
	v, _ := ParseVector("a:1A,16383 'two words':2B c")
	want := []byte{0, 0, 0, 3,
		'a', 0, 0, 2, 0xc0, 0x01, 0x3f, 0xff,
		'c', 0, 0, 0,
		't', 'w', 'o', ' ', 'w', 'o', 'r', 'd', 's', 0, 0, 1, 0x80, 0x02}
	if got := v.AppendBinary(nil); string(got) != string(want) {
		t.Errorf("binary tsvector:\n got %v\nwant %v", got, want)
	}
	q, _ := ParseQuery("a & !b:*A <2> c")
	// & , right = (!b <2> c), left = a; the phrase's right is c, its
	// left the negation.
	wantQ := []byte{0, 0, 0, 6,
		2, 2,
		2, 4, 0, 2,
		1, 0, 0, 'c', 0,
		2, 1,
		1, 8, 1, 'b', 0,
		1, 0, 0, 'a', 0}
	if got := q.AppendBinary(nil); string(got) != string(wantQ) {
		t.Errorf("binary tsquery:\n got %v\nwant %v", got, wantQ)
	}
	if e := (Query{}).AppendBinary(nil); string(e) != "\x00\x00\x00\x00" {
		t.Errorf("empty query: %v", e)
	}
	rng := rand.New(rand.NewSource(1))
	for i := 0; i < 500; i++ {
		v := randomVector(rng)
		back, err := ParseVectorBinary(v.AppendBinary(nil))
		if err != nil || back.String() != v.String() {
			t.Fatalf("vector %s came back as %s (%v)", v, back, err)
		}
		q := Query{Root: randomNode(rng, 4)}
		qb, err := ParseQueryBinary(q.AppendBinary(nil))
		if err != nil || qb.String() != q.String() {
			t.Fatalf("query %s came back as %s (%v)", q, qb, err)
		}
	}
	for _, bad := range [][]byte{nil, {0, 0, 0, 1}, {0, 0, 0, 1, 'a', 0, 0, 1}, {0, 0, 0, 1, 'a', 0, 0, 1, 0, 0}, {0, 0, 0, 0, 1}} {
		if _, err := ParseVectorBinary(bad); err == nil {
			t.Errorf("binary tsvector %v was accepted", bad)
		}
	}
	for _, bad := range [][]byte{nil, {0, 0, 0, 1}, {0, 0, 0, 1, 2, 2}, {0, 0, 0, 2, 1, 0, 0, 'a', 0}, {0, 0, 0, 1, 1, 0, 0, 'a', 0, 9}, {0, 0, 0, 1, 3, 0}} {
		if _, err := ParseQueryBinary(bad); err == nil {
			t.Errorf("binary tsquery %v was accepted", bad)
		}
	}
}

var testWords = []string{"cat", "cats", "catalog", "car", "dog", "do", "rat", "fat", "mat", "a", "ab", "abc", "emu"}

func randomVector(rng *rand.Rand) Vector {
	b := newBuilder()
	for i, n := 0, rng.Intn(8); i < n; i++ {
		var pos []Pos
		for j, m := 0, rng.Intn(4); j < m; j++ {
			pos = append(pos, Pos{P: uint16(1 + rng.Intn(12)), W: Weight(rng.Intn(4))})
		}
		_ = b.add(testWords[rng.Intn(len(testWords))], pos...)
	}
	return b.vector()
}

func randomVal(rng *rand.Rand) *Node {
	n := &Node{Kind: NodeVal, Lexeme: testWords[rng.Intn(len(testWords))], Prefix: rng.Intn(5) == 0}
	if rng.Intn(6) == 0 {
		n.Weights = uint8(1 + rng.Intn(15))
	}
	return n
}

// randomNode builds a random query. Inside a phrase, & and | join only
// single words (and their negations): operands of different widths
// there follow a PostgreSQL oddity (see match.go) that the brute-force
// definition below does not model.
func randomNode(rng *rand.Rand, depth int) *Node {
	return randomNodeIn(rng, depth, false, false)
}

func randomNodeIn(rng *rand.Rand, depth int, inPhrase, wordsOnly bool) *Node {
	if depth == 0 || rng.Intn(3) == 0 {
		return randomVal(rng)
	}
	kid := func() *Node { return randomNodeIn(rng, depth-1, inPhrase, wordsOnly) }
	switch rng.Intn(7) {
	case 0:
		if inPhrase && !wordsOnly {
			// A negated phrase inside a phrase has no fixed width.
			return &Node{Kind: NodeNot, L: randomNodeIn(rng, depth-1, true, true)}
		}
		return &Node{Kind: NodeNot, L: kid()}
	case 1, 2:
		if inPhrase {
			return &Node{Kind: NodeAnd, L: randomNodeIn(rng, depth-1, true, true), R: randomNodeIn(rng, depth-1, true, true)}
		}
		return &Node{Kind: NodeAnd, L: kid(), R: kid()}
	case 3, 4:
		if inPhrase {
			return &Node{Kind: NodeOr, L: randomNodeIn(rng, depth-1, true, true), R: randomNodeIn(rng, depth-1, true, true)}
		}
		return &Node{Kind: NodeOr, L: kid(), R: kid()}
	}
	if wordsOnly {
		return randomVal(rng)
	}
	return &Node{Kind: NodePhrase, Distance: rng.Intn(4), L: randomNodeIn(rng, depth-1, true, false), R: randomNodeIn(rng, depth-1, true, false)}
}

// naiveMatch decides a query by brute force, sharing no code with
// Match: a phrase is tried at every position, by asking of each operand
// whether it "holds at" a position.
func naiveMatch(v Vector, n *Node) bool {
	switch n.Kind {
	case NodeNot:
		return !naiveMatch(v, n.L)
	case NodeAnd:
		return naiveMatch(v, n.L) && naiveMatch(v, n.R)
	case NodeOr:
		return naiveMatch(v, n.L) || naiveMatch(v, n.R)
	case NodePhrase:
		for p := -40; p <= 60; p++ {
			if naiveAt(v, n, p) {
				return true
			}
		}
		return false
	}
	for _, e := range v {
		if e.Lexeme == n.Lexeme || (n.Prefix && strings.HasPrefix(e.Lexeme, n.Lexeme)) {
			if n.Weights == 0 || len(e.Pos) == 0 {
				return true
			}
			for _, p := range e.Pos {
				if n.Weights&(1<<p.W) != 0 {
					return true
				}
			}
		}
	}
	return false
}

// naiveWidth is how far before its last position an operand starts.
func naiveWidth(n *Node) int {
	switch n.Kind {
	case NodePhrase:
		return naiveWidth(n.L) + n.Distance + naiveWidth(n.R)
	case NodeAnd, NodeOr:
		return max(naiveWidth(n.L), naiveWidth(n.R))
	case NodeNot:
		return naiveWidth(n.L)
	}
	return 0
}

// naiveAt: the operand ends at position p.
func naiveAt(v Vector, n *Node, p int) bool {
	switch n.Kind {
	case NodeNot:
		return !naiveAt(v, n.L, p)
	case NodeAnd:
		return naiveAt(v, n.L, p) && naiveAt(v, n.R, p)
	case NodeOr:
		return naiveAt(v, n.L, p) || naiveAt(v, n.R, p)
	case NodePhrase:
		return naiveAt(v, n.R, p) && naiveAt(v, n.L, p-n.Distance-naiveWidth(n.R))
	}
	for _, e := range v {
		if e.Lexeme == n.Lexeme || (n.Prefix && strings.HasPrefix(e.Lexeme, n.Lexeme)) {
			for _, pos := range e.Pos {
				if int(pos.P) == p && (n.Weights == 0 || n.Weights&(1<<pos.W) != 0) {
					return true
				}
			}
		}
	}
	return false
}

// TestMatchRandom compares Match with the brute-force definition on
// random vectors and queries, phrases with negations and alternatives
// inside them included, and checks that the text forms read back.
func TestMatchRandom(t *testing.T) {
	rng := rand.New(rand.NewSource(299))
	matched := 0
	for i := 0; i < 20000; i++ {
		v := randomVector(rng)
		q := Query{Root: randomNode(rng, 3)}
		got, want := Match(v, q), naiveMatch(v, q.Root)
		if got != want {
			t.Fatalf("%s @@ %s = %v, the definition says %v", v, q, got, want)
		}
		if got {
			matched++
		}
		back, err := ParseQuery(q.String())
		if err != nil || back.String() != q.String() || Match(v, back) != got {
			t.Fatalf("query %s read back as %s (%v)", q, back, err)
		}
		vb, err := ParseVector(v.String())
		if err != nil || vb.String() != v.String() {
			t.Fatalf("vector %s read back as %s (%v)", v, vb, err)
		}
	}
	if matched < 2000 || matched > 18000 {
		t.Fatalf("%d of 20000 random pairs matched: the test is not exercising both outcomes", matched)
	}
}

func BenchmarkToVectorEnglish(b *testing.B) {
	english := mustConfig(b, "english")
	text := strings.Repeat("The most common type of search is to find all documents containing given query terms and return them in order of their similarity to the query. ", 4)
	b.SetBytes(int64(len(text)))
	for i := 0; i < b.N; i++ {
		if _, err := ToVector(english, text); err != nil {
			b.Fatal(err)
		}
	}
}
