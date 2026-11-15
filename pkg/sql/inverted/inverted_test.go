package inverted

import (
	"bytes"
	"encoding/json"
	"fmt"
	"math/rand"
	"sort"
	"strings"
	"testing"

	"github.com/sthorne/datax/pkg/sql/builtins"
	"github.com/sthorne/datax/pkg/sql/types"
)

func jsonb(t testing.TB, s string) types.Datum {
	t.Helper()
	d, err := types.ParseJSONB(s)
	if err != nil {
		t.Fatalf("parse %q: %v", s, err)
	}
	return d
}

func elementsOf(t testing.TB, doc string) [][]byte {
	t.Helper()
	k, _ := Lookup(JSONBOps)
	els, err := k.Elements(jsonb(t, doc))
	if err != nil {
		t.Fatalf("elements of %s: %v", doc, err)
	}
	return els
}

func describeAll(t testing.TB, doc string) []string {
	k, _ := Lookup(JSONBOps)
	var out []string
	for _, e := range elementsOf(t, doc) {
		out = append(out, k.Describe(e))
	}
	return out
}

// TestJSONBElements pins what a document's elements are: one per scalar
// with its path, positions dropped, plus the top-level keys.
func TestJSONBElements(t *testing.T) {
	for _, c := range []struct {
		doc  string
		want []string
	}{
		{`{"a":1,"b":{"c":"x","d":[true,null]}}`, []string{
			`? "a"`, `? "b"`,
			`"a" = 1`, `"b"."c" = "x"`, `"b"."d".[] = null`, `"b"."d".[] = true`,
		}},
		// Duplicates and positions collapse.
		{`[1,2,1,2,1]`, []string{`[] = 1`, `[] = 2`}},
		// A top-level array's strings are keys too (the ? operator).
		{`["a","b",3]`, []string{`? "a"`, `? "b"`, `[] = 3`, `[] = "a"`, `[] = "b"`}},
		// Scalars at the top level.
		{`"x"`, []string{`? "x"`, `$ = "x"`}},
		{`7`, []string{`$ = 7`}},
		{`null`, []string{`$ = null`}},
		// Empty containers are not elements; the key that holds one is.
		{`{}`, nil},
		{`[]`, nil},
		{`{"a":{},"b":[]}`, []string{`? "a"`, `? "b"`}},
		{`{"a":{"b":{}}}`, []string{`? "a"`}},
		// Nested arrays keep their depth (a deeper path sorts first: step
		// tags are below value tags).
		{`[[1],[2,[3]]]`, []string{`[].[].[] = 3`, `[].[] = 1`, `[].[] = 2`}},
		// Numbers that are equal are one element.
		{`[1,1.0,1e0,100,1e2]`, []string{`[] = 1`, `[] = 100`}},
	} {
		got := describeAll(t, c.doc)
		if fmt.Sprint(got) != fmt.Sprint(c.want) {
			t.Errorf("%s:\n got %q\nwant %q", c.doc, got, c.want)
		}
	}
}

// TestJSONBElementTypeSeparation: values of different JSON types under
// one path never share an element, whatever their text.
func TestJSONBElementTypeSeparation(t *testing.T) {
	docs := []string{`{"a":1}`, `{"a":"1"}`, `{"a":true}`, `{"a":"true"}`, `{"a":null}`, `{"a":"null"}`, `{"a":false}`,
		`{"a":[1]}`, `{"a":{"1":1}}`, `{"a":""}`, `{"a":0}`, `{"a1":1}`, `{"a":{"":1}}`, `{"":{"a":1}}`}
	seen := map[string]string{}
	for _, doc := range docs {
		for _, e := range elementsOf(t, doc) {
			if e[0] == jsonKeyTag {
				continue
			}
			if prev, dup := seen[string(e)]; dup {
				t.Errorf("%s and %s share the element %x", prev, doc, e)
			}
			seen[string(e)] = doc
		}
	}
	// A key and a path element never collide either.
	if bytes.Equal(jsonKeyElement("a"), elementsOf(t, `"a"`)[1]) {
		t.Fatal("key element equals path element")
	}
}

// TestJSONBElementOrder: under one path, elements sort by type and then
// by value, numbers numerically.
func TestJSONBElementOrder(t *testing.T) {
	values := []string{`null`, `false`, `true`, `-1e30`, `-2.5`, `-2`, `0`, `0.001`, `1`, `1.5`, `2`, `10`, `1e30`, `""`, `"a"`, `"ab"`, `"b"`}
	var prev []byte
	for i, v := range values {
		els := elementsOf(t, `{"k":[`+v+`]}`)
		e := els[len(els)-1] // the path element sorts after the key element
		if i > 0 && bytes.Compare(prev, e) >= 0 {
			t.Errorf("%s does not sort after %s", v, values[i-1])
		}
		prev = e
	}
	// Paths order by key bytes before any value.
	a := elementsOf(t, `{"a":"zzz"}`)[1]
	b := elementsOf(t, `{"b":null}`)[1]
	if bytes.Compare(a, b) >= 0 {
		t.Error("path a does not sort before path b")
	}
}

// TestJSONBElementRoundTrip: an undigested path element reads back as
// the path and value it was made from.
func TestJSONBElementRoundTrip(t *testing.T) {
	for _, c := range []struct{ doc, steps, value string }{
		{`{"a":{"b":[{"c":"hello"}]}}`, `"a"/"b"/[]/"c"`, `"hello"`},
		{`{"k\u0000ey":-12.50}`, `"k\u0000ey"`, `-12.5`},
		{`[[true]]`, `[]/[]`, `true`},
		{`{"a":"with \"quotes\" and \\"}`, `"a"`, `"with \"quotes\" and \\"`},
		{`{"":""}`, `""`, `""`},
	} {
		els := elementsOf(t, c.doc)
		steps, value, err := DecodeJSONPath(els[len(els)-1])
		if err != nil {
			t.Fatalf("%s: %v", c.doc, err)
		}
		if got := strings.Join(steps, "/"); got != c.steps || value != c.value {
			t.Errorf("%s: read back %s = %s, want %s = %s", c.doc, got, value, c.steps, c.value)
		}
	}
}

// TestJSONBLongElements: a value too long for a key is stored as a
// digest of fixed size, the same on the write and the query side.
func TestJSONBLongElements(t *testing.T) {
	long := strings.Repeat("x", 5000)
	els := elementsOf(t, `{"a":"`+long+`","`+long+`":1}`)
	for _, e := range els {
		if len(e) > MaxElementLen {
			t.Fatalf("element of %d bytes stored undigested", len(e))
		}
	}
	k, _ := Lookup(JSONBOps)
	q, err := k.Query("@>", jsonb(t, `{"a":"`+long+`"}`))
	if err != nil || q == nil || q.Op != OpLeaf || !IsDigest(q.Element) {
		t.Fatalf("query for a long value: %+v %v", q, err)
	}
	if !has(els, q.Element) {
		t.Fatal("the digest a query asks for is not the one the document stored")
	}
	// A different long value has a different digest.
	q2, _ := k.Query("@>", jsonb(t, `{"a":"`+long+`y"}`))
	if has(els, q2.Element) {
		t.Fatal("two long values share a digest")
	}
	// Deep nesting digests too, rather than growing the key.
	deep := strings.Repeat(`{"level":`, 200) + `1` + strings.Repeat(`}`, 200)
	for _, e := range elementsOf(t, deep) {
		if len(e) > MaxElementLen {
			t.Fatalf("deep path stored in %d bytes", len(e))
		}
	}
}

func TestJSONBElementLimit(t *testing.T) {
	var b strings.Builder
	b.WriteString("[")
	for i := 0; i <= MaxElementsPerValue; i++ {
		if i > 0 {
			b.WriteString(",")
		}
		fmt.Fprintf(&b, "%d", i)
	}
	b.WriteString("]")
	k, _ := Lookup(JSONBOps)
	if _, err := k.Elements(jsonb(t, b.String())); err == nil {
		t.Fatal("a document over the element limit was indexed")
	}
	// The limit counts distinct elements: a long array of one value is
	// one element, and exactly the limit is allowed.
	repeated := "[" + strings.Repeat("7,", 3*MaxElementsPerValue) + "7]"
	if els, err := k.Elements(jsonb(t, repeated)); err != nil || len(els) != 1 {
		t.Fatalf("repeated value: %d elements, %v", len(els), err)
	}
	atLimit := strings.Replace(b.String(), "[0,", "[", 1)
	if els, err := k.Elements(jsonb(t, atLimit)); err != nil || len(els) != MaxElementsPerValue {
		t.Fatalf("at the limit: %d elements, %v", len(els), err)
	}
}

func has(els [][]byte, e []byte) bool {
	i := sort.Search(len(els), func(i int) bool { return bytes.Compare(els[i], e) >= 0 })
	return i < len(els) && bytes.Equal(els[i], e)
}

// admits evaluates an expression against one document's element set.
func admits(e *Expr, els [][]byte) bool {
	if e == nil {
		return true
	}
	switch e.Op {
	case OpLeaf:
		return has(els, e.Element)
	case OpAnd:
		for _, k := range e.Kids {
			if !admits(k, els) {
				return false
			}
		}
		return true
	}
	for _, k := range e.Kids {
		if admits(k, els) {
			return true
		}
	}
	return false
}

// TestJSONBQueryShapes pins the lookups each operator turns into.
func TestJSONBQueryShapes(t *testing.T) {
	k, _ := Lookup(JSONBOps)
	for _, c := range []struct{ op, rhs, want string }{
		{"@>", `{"a":{"b":1}}`, `"a"."b" = 1`},
		{"@>", `{"a":1,"b":"x"}`, `"a" = 1 AND "b" = "x"`},
		{"@>", `{"tags":["go","db"]}`, `"tags".[] = "db" AND "tags".[] = "go"`},
		{"@>", `[1]`, `[] = 1`},
		// A top-level scalar is contained by itself and by an array of it.
		{"@>", `1`, `$ = 1 OR [] = 1`},
		// Empty containers constrain nothing — except that their key exists.
		{"@>", `{}`, `all`},
		{"@>", `[]`, `all`},
		{"@>", `{"a":{}}`, `? "a"`},
		{"@>", `{"a":{"b":[]},"c":2}`, `? "a" AND "c" = 2`},
		{"@>", `{"a":[{}]}`, `? "a"`},
		{"@>", `[{}]`, `all`},
		// Equal numbers ask for one element.
		{"@>", `{"n":1.0}`, `"n" = 1`},
		{"?", `a`, `? "a"`},
		{"?|", `{a,b}`, `? "a" OR ? "b"`},
		{"?&", `{a,b,a}`, `? "a" AND ? "b"`},
		{"?|", `{}`, `none`},
		{"?&", `{}`, `all`},
		{"<@", `{"a":1}`, `all`},
		{"=", `{"a":1}`, `all`},
	} {
		rhs := types.NewString(c.rhs)
		q, err := k.Query(c.op, rhs)
		if err != nil {
			t.Fatalf("%s %s: %v", c.op, c.rhs, err)
		}
		got := q.Format(k.Describe)
		// Object keys iterate in map order; compare as sorted terms when
		// the expression is a flat conjunction or disjunction.
		if normalize(got) != normalize(c.want) {
			t.Errorf("%s %s: got %s, want %s", c.op, c.rhs, got, c.want)
		}
	}
	if q, _ := k.Query("@>", types.DNull); q == nil || q.Op != OpOr || len(q.Kids) != 0 {
		t.Errorf("@> NULL should admit no row, got %+v", q)
	}
}

func normalize(s string) string {
	for _, sep := range []string{" AND ", " OR "} {
		if strings.Contains(s, sep) && !strings.Contains(s, "(") {
			parts := strings.Split(s, sep)
			sort.Strings(parts)
			return strings.Join(parts, sep)
		}
	}
	return s
}

// randJSON builds a random document over a small vocabulary, so that
// random queries hit and miss in comparable numbers.
func randJSON(r *rand.Rand, depth int) any {
	scalars := []any{nil, true, false, 0, 1, 2, 1.5, "a", "b", "1", "", "true"}
	if depth <= 0 || r.Intn(3) == 0 {
		return scalars[r.Intn(len(scalars))]
	}
	if r.Intn(2) == 0 {
		n := r.Intn(4)
		out := make([]any, n)
		for i := range out {
			out[i] = randJSON(r, depth-1)
		}
		return out
	}
	keys := []string{"a", "b", "c", "1", ""}
	out := map[string]any{}
	for i, n := 0, r.Intn(4); i < n; i++ {
		out[keys[r.Intn(len(keys))]] = randJSON(r, depth-1)
	}
	return out
}

// TestJSONBQueryNeverMisses is the kind's contract, checked against the
// operators' own implementation over random documents and queries: when
// the predicate is true of a document, the document's elements satisfy
// the query's expression. (The converse need not hold; the recheck is
// for that.)
func TestJSONBQueryNeverMisses(t *testing.T) {
	r := rand.New(rand.NewSource(1))
	k, _ := Lookup(JSONBOps)
	render := func(v any) string {
		d, err := json.Marshal(v)
		if err != nil {
			t.Fatal(err)
		}
		return string(d)
	}
	trueCount, lossy := 0, 0
	for i := 0; i < 20000; i++ {
		doc := jsonb(t, render(randJSON(r, 3)))
		els, err := k.Elements(doc)
		if err != nil {
			t.Fatal(err)
		}
		dv, _ := decodeJSON(doc.S)

		var op string
		var rhs types.Datum
		var truth bool
		switch r.Intn(4) {
		case 0, 1:
			op = "@>"
			rhs = jsonb(t, render(randJSON(r, 2)))
			rv, _ := decodeJSON(rhs.S)
			truth = builtins.JSONContains(dv, rv, true)
		case 2:
			op = "?"
			rhs = types.NewString([]string{"a", "b", "1", "", "zz"}[r.Intn(5)])
			truth = builtins.JSONHasKey(dv, rhs.S)
		default:
			op = []string{"?|", "?&"}[r.Intn(2)]
			keys := []string{"a", "b", "c", "zz"}
			r.Shuffle(len(keys), func(i, j int) { keys[i], keys[j] = keys[j], keys[i] })
			keys = keys[:1+r.Intn(3)]
			rhs = types.NewString("{" + strings.Join(keys, ",") + "}")
			any, all := false, true
			for _, key := range keys {
				h := builtins.JSONHasKey(dv, key)
				any, all = any || h, all && h
			}
			truth = any
			if op == "?&" {
				truth = all
			}
		}
		q, err := k.Query(op, rhs)
		if err != nil {
			t.Fatal(err)
		}
		got := admits(q, els)
		if truth {
			trueCount++
			if !got {
				t.Fatalf("%s %s %s is true, but the index expression %s does not admit the document",
					doc.S, op, rhs.Text(), q.Format(k.Describe))
			}
		} else if got && q != nil {
			lossy++
		}
	}
	if trueCount < 500 {
		t.Fatalf("only %d of the random cases were true: the test is not exercising matches", trueCount)
	}
	t.Logf("%d true cases admitted; %d false cases admitted too (left to the recheck)", trueCount, lossy)
}

func TestExprAlgebra(t *testing.T) {
	a, b := Leaf([]byte("a")), Leaf([]byte("b"))
	if And() != nil || And(nil, nil) != nil {
		t.Error("an empty conjunction should be unconstrained")
	}
	if And(a, nil) != a {
		t.Error("AND with an unconstrained term should be the other term")
	}
	if Or(a, nil) != nil {
		t.Error("OR with an unconstrained branch should be unconstrained")
	}
	if e := And(a, None()); e.Op != OpOr || len(e.Kids) != 0 {
		t.Error("AND with an empty branch should be empty")
	}
	if e := Or(a, None()); e != a {
		t.Error("OR with an empty branch should be the other branch")
	}
	if e := And(a, And(b, a)); e.Op != OpAnd || len(e.Kids) != 2 {
		t.Errorf("nested AND should flatten and drop repeats: %+v", e)
	}
	if e := Or(a, Or(b, a)); e.Op != OpOr || len(e.Kids) != 2 {
		t.Errorf("nested OR should flatten and drop repeats: %+v", e)
	}
	if n := And(a, Or(b, Leaf([]byte("c")))).Leaves(); n != 3 {
		t.Errorf("leaves = %d", n)
	}
}

func TestDiff(t *testing.T) {
	bs := func(ss ...string) [][]byte {
		out := make([][]byte, len(ss))
		for i, s := range ss {
			out[i] = []byte(s)
		}
		return out
	}
	rem, add := Diff(bs("a", "b", "d"), bs("b", "c", "d", "e"))
	if fmt.Sprintf("%s %s", rem, add) != "[a] [c e]" {
		t.Fatalf("diff: -%s +%s", rem, add)
	}
	if got := SortDedup(bs("b", "a", "b", "a", "c")); fmt.Sprintf("%s", got) != "[a b c]" {
		t.Fatalf("sort/dedup: %s", got)
	}
}

// fakeIndex is an in-memory inverted index for evaluating expressions.
type fakeIndex struct {
	postings map[string][][]byte
	scans    []string // element and limit of each scan, in order
}

func (f *fakeIndex) scan(element []byte, limit int) ([][]byte, error) {
	f.scans = append(f.scans, fmt.Sprintf("%s/%d", element, limit))
	refs := f.postings[string(element)]
	if limit > 0 && len(refs) > limit {
		refs = refs[:limit]
	}
	return refs, nil
}

func refsRange(lo, hi, step int) [][]byte {
	var out [][]byte
	for i := lo; i < hi; i += step {
		out = append(out, []byte(fmt.Sprintf("%08d", i)))
	}
	return out
}

func TestEval(t *testing.T) {
	f := &fakeIndex{postings: map[string][][]byte{
		"even":   refsRange(0, 100, 2),
		"third":  refsRange(0, 100, 3),
		"common": refsRange(0, 5000, 1),
		"other":  refsRange(0, 5000, 2),
		"rare":   refsRange(4000, 4010, 1),
	}}
	count := func(e *Expr) int {
		t.Helper()
		refs, err := e.Eval(f.scan)
		if err != nil {
			t.Fatal(err)
		}
		if !sort.SliceIsSorted(refs, func(i, j int) bool { return bytes.Compare(refs[i], refs[j]) < 0 }) {
			t.Fatal("result is not sorted")
		}
		return len(refs)
	}
	leaf := func(s string) *Expr { return Leaf([]byte(s)) }

	if n := count(And(leaf("even"), leaf("third"))); n != 17 { // multiples of 6 below 100
		t.Errorf("even AND third = %d", n)
	}
	if n := count(Or(leaf("even"), leaf("third"))); n != 67 {
		t.Errorf("even OR third = %d", n)
	}
	if n := count(And(leaf("even"), leaf("absent"))); n != 0 {
		t.Errorf("AND with an absent element = %d", n)
	}
	// A conjunction with a selective element does not read the common
	// one in full: the result is the selective element's rows, a
	// superset the recheck narrows.
	f.scans = nil
	if n := count(And(leaf("common"), leaf("rare"))); n != 10 {
		t.Errorf("common AND rare = %d", n)
	}
	for _, s := range f.scans {
		if s == "common/0" {
			t.Errorf("the common element was read in full: %v", f.scans)
		}
	}
	// With no selective element, every term is read and intersected.
	if n := count(And(leaf("common"), leaf("other"))); n != 2500 {
		t.Errorf("common AND other = %d", n)
	}
	// A disjunction under a conjunction is skipped when a leaf is
	// selective, evaluated when none is.
	if n := count(And(leaf("rare"), Or(leaf("even"), leaf("third")))); n != 10 {
		t.Errorf("rare AND (even OR third) = %d", n)
	}
	if n := count(And(leaf("common"), Or(leaf("even"), leaf("third")))); n != 67 {
		t.Errorf("common AND (even OR third) = %d", n)
	}
	if n := count(None()); n != 0 {
		t.Errorf("none = %d", n)
	}
}
