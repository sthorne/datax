package inverted

import (
	"fmt"
	"math/rand"
	"strings"
	"testing"

	"github.com/sthorne/datax/pkg/sql/tsearch"
	"github.com/sthorne/datax/pkg/sql/types"
)

func tsKind(t testing.TB, name string) Kind {
	t.Helper()
	k, ok := Lookup(name)
	if !ok {
		t.Fatalf("kind %q is not registered", name)
	}
	return k
}

func TestTextSearchKinds(t *testing.T) {
	vec := tsKind(t, TSVectorOps)
	english := tsKind(t, TextSearchKindName("english"))
	simple := tsKind(t, TextSearchKindName("simple"))
	if vec.Family() != types.Tsvector || english.Family() != types.String || vec.Method() != "gin" {
		t.Fatalf("families: %v %v, method %s", vec.Family(), english.Family(), vec.Method())
	}
	if fn, arg := english.(Computed).Computes(); fn != "to_tsvector" || arg != "english" {
		t.Fatalf("english computes %s(%s)", fn, arg)
	}
	if fn, _ := vec.(Computed).Computes(); fn != "" {
		t.Fatalf("the tsvector kind computes %s", fn)
	}
	elements := func(k Kind, d types.Datum) string {
		els, err := k.Elements(d)
		if err != nil {
			t.Fatal(err)
		}
		out := make([]string, len(els))
		for i, e := range els {
			out[i] = string(e)
		}
		return strings.Join(out, " ")
	}
	v, _ := types.ParseTsvector("fat:2,11A cat:3 'two words' rat")
	// Lexemes only: positions and weights are not in the index.
	if got := elements(vec, v); got != "cat fat rat two words" {
		t.Errorf("tsvector elements: %q", got)
	}
	text := types.NewString("The Fat cats, the fat RATS")
	if got := elements(english, text); got != "cat fat rat" {
		t.Errorf("english elements: %q", got)
	}
	if got := elements(simple, text); got != "cats fat rats the" {
		t.Errorf("simple elements: %q", got)
	}
	for _, k := range []Kind{vec, english} {
		if els, err := k.Elements(types.DNull); err != nil || els != nil {
			t.Errorf("%s: NULL has elements %v (%v)", k.Name(), els, err)
		}
	}
	if els, _ := english.Elements(types.NewString("the of and")); len(els) != 0 {
		t.Errorf("stop words alone have elements: %q", els)
	}
	if got := vec.Describe([]byte("cat")); got != `"cat"` {
		t.Errorf("describe: %s", got)
	}

	// A long lexeme is cut, not digested, so its prefixes still lead
	// to it.
	long := strings.Repeat("x", MaxElementLen+40)
	lv, _ := types.ParseTsvector(long + " " + long + "y")
	els, _ := vec.Elements(lv)
	if len(els) != 1 || len(els[0]) != MaxElementLen || IsDigest(els[0]) {
		t.Fatalf("long lexemes: %d elements of %d bytes", len(els), len(els[0]))
	}
	for _, q := range []string{long, long[:10] + ":*", long + ":*", long + "zzz:*"} {
		e, err := vec.Query("@@", types.NewString(q))
		if err != nil || !admits(e, els) {
			t.Errorf("a lookup for %.12s... does not find the long lexeme (%v)", q, err)
		}
	}

	// More distinct lexemes than a row may have entries.
	var b strings.Builder
	for i := 0; i <= MaxElementsPerValue; i++ {
		fmt.Fprintf(&b, "w%d ", i)
	}
	if _, err := simple.Elements(types.NewString(b.String())); err == nil {
		t.Error("a document with too many lexemes was accepted")
	}
}

func TestTextSearchQueryShapes(t *testing.T) {
	vec := tsKind(t, TSVectorOps)
	for query, want := range map[string]string{
		"cat":                       `"cat"`,
		"cat & rat":                 `"cat" AND "rat"`,
		"cat | rat":                 `"cat" OR "rat"`,
		"cat & (rat | dog)":         `"cat" AND ("rat" OR "dog")`,
		"ca:*":                      `"ca":*`,
		"ca:* & dog:A":              `"ca":* AND "dog"`,
		"cat <-> rat":               `"cat" AND "rat"`,
		"cat <3> (rat <-> dog)":     `"cat" AND "rat" AND "dog"`,
		"cat & !rat":                `"cat"`,
		"cat <-> !rat":              `"cat"`,
		"!rat":                      "all",
		"cat | !rat":                "all",
		"(cat | !rat) & dog":        `"dog"`,
		"!(cat & rat)":              "all",
		"cat & cat":                 `"cat"`,
		"(cat & rat) | (cat & emu)": `("cat" AND "rat") OR ("cat" AND "emu")`,
	} {
		e, err := vec.Query("@@", types.NewString(query))
		if err != nil {
			t.Errorf("%s: %v", query, err)
			continue
		}
		if got := e.Format(vec.Describe); got != want {
			t.Errorf("%s: lookups %s, want %s", query, got, want)
		}
	}
	// The empty query and NULL match no row; other operators and what
	// is not a query say nothing.
	q, _ := types.ParseTsquery("")
	for _, rhs := range []types.Datum{q, types.DNull} {
		if e, _ := vec.Query("@@", rhs); e == nil || e.Format(vec.Describe) != "none" {
			t.Errorf("%+v: %v", rhs, e)
		}
	}
	for _, c := range []struct {
		op  string
		rhs types.Datum
	}{{"NOT @@", types.NewString("cat")}, {"=", types.NewString("cat")}, {"@@", types.NewString("cat &")}, {"@@", types.NewInt(1)}} {
		if e, err := vec.Query(c.op, c.rhs); e != nil || err != nil {
			t.Errorf("%s %+v: %v, %v", c.op, c.rhs, e, err)
		}
	}
}

var tsWords = []string{"cat", "cats", "catalog", "car", "dog", "do", "rat", "fat", "mat", "a", "ab", "abc", "emu"}

func randTSQuery(r *rand.Rand, depth int) string {
	if depth == 0 || r.Intn(3) == 0 {
		w := tsWords[r.Intn(len(tsWords))]
		switch r.Intn(8) {
		case 0:
			return w + ":*"
		case 1:
			return w + ":A"
		}
		return w
	}
	l, rr := randTSQuery(r, depth-1), randTSQuery(r, depth-1)
	switch r.Intn(8) {
	case 0:
		return "!(" + l + ")"
	case 1, 2, 3:
		return "(" + l + " & " + rr + ")"
	case 4, 5:
		return "(" + l + " | " + rr + ")"
	}
	return fmt.Sprintf("(%s <%d> %s)", l, 1+r.Intn(2), rr)
}

// TestTextSearchQueryNeverMisses is the contract of a kind, on random
// documents and random queries: every document the predicate is true
// for satisfies the lookups.
func TestTextSearchQueryNeverMisses(t *testing.T) {
	vec := tsKind(t, TSVectorOps)
	simple := tsKind(t, TextSearchKindName("simple"))
	cfg, _ := tsearch.LookupConfig("simple")
	r := rand.New(rand.NewSource(299))
	matches, constrained, admitted := 0, 0, 0
	for i := 0; i < 20000; i++ {
		words := make([]string, r.Intn(8))
		for j := range words {
			words[j] = tsWords[r.Intn(len(tsWords))]
		}
		text := strings.Join(words, " ")
		v, err := tsearch.ToVector(cfg, text)
		if err != nil {
			t.Fatal(err)
		}
		query := randTSQuery(r, 3)
		q, err := tsearch.ParseQuery(query)
		if err != nil {
			t.Fatalf("%s: %v", query, err)
		}
		// The same elements whether the column holds the vector or the
		// text it was made from.
		fromVector, _ := vec.Elements(types.NewTsvector(v))
		fromText, _ := simple.Elements(types.NewString(text))
		if fmt.Sprintf("%q", fromVector) != fmt.Sprintf("%q", fromText) {
			t.Fatalf("%q: elements %q from the vector, %q from the text", text, fromVector, fromText)
		}
		e, err := vec.Query("@@", types.NewTsquery(q))
		if err != nil {
			t.Fatal(err)
		}
		match := tsearch.Match(v, q)
		if e != nil {
			constrained++
			if admits(e, fromVector) {
				admitted++
			}
		}
		if match {
			matches++
			if !admits(e, fromVector) {
				t.Fatalf("%q matches %s, but its elements %q do not satisfy %s", text, q, fromVector, e.Format(vec.Describe))
			}
		}
	}
	if matches < 1000 || constrained < 5000 || admitted == constrained {
		t.Fatalf("%d matches, %d constrained queries, %d admitted: the test is not exercising the contract", matches, constrained, admitted)
	}
}
