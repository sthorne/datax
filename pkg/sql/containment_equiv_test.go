package sql

import (
	"context"
	"encoding/json"
	"fmt"
	"math/rand"
	"strings"
	"testing"

	"github.com/sthorne/datax/pkg/sql/catalog"
	"github.com/sthorne/datax/pkg/sql/jsonbcontain"
	"github.com/sthorne/datax/pkg/sql/parser"
	"github.com/sthorne/datax/pkg/sql/rowenc"
	"github.com/sthorne/datax/pkg/sql/types"
)

// The pieces random documents are built from: few enough that keys,
// strings and numbers collide often, with the spellings of one number
// (1, 1.0, 10e-1, 0.1e1), signed zeros, numbers past float64 and past the
// decimal type's exponent range, escapes and U+FFFD.
var (
	fuzzKeys    = []string{"a", "b", "c", "tenant", "é", `q"t`, " ", "�", ""}
	fuzzStrings = []string{"x", "y", "", "1", "é", `back\slash`, "tab\t", "�", "a"}
	fuzzNumbers = []string{
		"0", "-0", "0.0", "0e5", "1", "1.0", "10e-1", "0.1e1", "1E0", "-1", "-1.00",
		"2", "1.5", "1.50", "15e-1", "100", "1e2", "1E+2", "9007199254740993",
		"9007199254740992", "123456789012345678901234567890", "1e5000", "1.0e5000", "0e3000",
	}
)

// fuzzValue is a random JSON value as text, sometimes with a duplicate
// key or white space (the jsonb parse removes both).
func fuzzValue(r *rand.Rand, depth int) string {
	n := 6
	if depth > 0 {
		n = 8
	}
	switch r.Intn(n) {
	case 0:
		return []string{"null", "true", "false"}[r.Intn(3)]
	case 1, 2:
		return fuzzNumbers[r.Intn(len(fuzzNumbers))]
	case 3, 4:
		return quoteJSON(fuzzStrings[r.Intn(len(fuzzStrings))])
	case 5:
		return quoteJSON(fuzzKeys[r.Intn(len(fuzzKeys))])
	case 6:
		var b strings.Builder
		b.WriteByte('[')
		for i, k := 0, r.Intn(5); i < k; i++ {
			if i > 0 {
				b.WriteString(", ")
			}
			b.WriteString(fuzzValue(r, depth-1))
		}
		b.WriteByte(']')
		return b.String()
	default:
		var b strings.Builder
		b.WriteByte('{')
		for i, k := 0, r.Intn(5); i < k; i++ {
			if i > 0 {
				b.WriteByte(',')
			}
			b.WriteString(quoteJSON(fuzzKeys[r.Intn(len(fuzzKeys))]))
			b.WriteString(": ")
			b.WriteString(fuzzValue(r, depth-1))
		}
		b.WriteByte('}')
		return b.String()
	}
}

func quoteJSON(s string) string {
	d, err := types.ParseJSONB(fmt.Sprintf("%q", s))
	if err != nil {
		panic(err)
	}
	return d.S
}

// fuzzSub derives a pattern a document is likely to contain: a part of
// it (some keys, some elements, a top-level element), sometimes changed.
func fuzzSub(r *rand.Rand, v any) any {
	switch t := v.(type) {
	case map[string]any:
		out := map[string]any{}
		for k, e := range t {
			if r.Intn(3) > 0 {
				out[k] = fuzzSub(r, e)
			}
		}
		if r.Intn(8) == 0 {
			out[fuzzKeys[r.Intn(len(fuzzKeys))]] = "zz"
		}
		return out
	case []any:
		var out []any
		for _, e := range t {
			if r.Intn(2) == 0 {
				out = append(out, fuzzSub(r, e))
			}
		}
		if len(out) > 1 && r.Intn(3) == 0 {
			out[0], out[len(out)-1] = out[len(out)-1], out[0]
		}
		if len(out) > 0 && r.Intn(4) == 0 {
			out = append(out, out[0]) // a repeated element
		}
		if out == nil {
			out = []any{}
		}
		return out
	}
	if r.Intn(10) == 0 {
		return "zz"
	}
	return v
}

func fuzzDoc(t testing.TB, r *rand.Rand) types.Datum {
	d, err := types.ParseJSONB(fuzzValue(r, 3))
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	return d
}

// checkContainsEquiv compares the text walk with the decoding
// implementation, both ways round (@> and <@).
func checkContainsEquiv(t testing.TB, l, rt types.Datum) {
	t.Helper()
	for _, pair := range [][2]types.Datum{{l, rt}, {rt, l}} {
		want, werr := jsonbContainsTree(pair[0], pair[1])
		got, gerr := jsonbContains(pair[0], pair[1])
		if (werr != nil) != (gerr != nil) || got != want {
			t.Fatalf("%s @> %s: walk %v (%v), tree %v (%v)", pair[0].S, pair[1].S, got, gerr, want, werr)
		}
	}
}

// TestJSONBContainsMatchesTree: over random documents and patterns, the
// text walk gives the decoding implementation's answer (the reference
// TestJSONBContains pins to PostgreSQL's).
func TestJSONBContainsMatchesTree(t *testing.T) {
	r := rand.New(rand.NewSource(1))
	n := 40000
	if testing.Short() {
		n = 4000
	}
	trues := 0
	for i := 0; i < n; i++ {
		l := fuzzDoc(t, r)
		var rt types.Datum
		if i%2 == 0 {
			rt = fuzzDoc(t, r)
		} else {
			var v any
			if err := decodeJSONNumber(l.S, &v); err != nil {
				t.Fatal(err)
			}
			b, err := jsonMarshalNumber(fuzzSub(r, v))
			if err != nil {
				t.Fatal(err)
			}
			if rt, err = types.ParseJSONB(b); err != nil {
				t.Fatal(err)
			}
		}
		checkContainsEquiv(t, l, rt)
		if ok, _ := jsonbContainsTree(l, rt); ok {
			trues++
		}
	}
	// The derived patterns must exercise the true answers too.
	if trues < n/5 {
		t.Fatalf("only %d of %d pairs were contained", trues, n)
	}
}

func jsonMarshalNumber(v any) (string, error) {
	b, err := json.Marshal(v)
	return string(b), err
}

// FuzzJSONBContains: any two jsonb values the text walk and the
// decoding implementation agree on.
func FuzzJSONBContains(f *testing.F) {
	for _, s := range [][2]string{
		{`{"a":1,"b":[1,2,{"c":"x"}]}`, `{"b":[{"c":"x"}]}`},
		{`[1,2,[1,3]]`, `[[1,3]]`},
		{`[1,2]`, `1`},
		{`1`, `[1]`},
		{`{"n":100}`, `{"n":1e2}`},
		{`{"a":"q\"t"}`, `{"a":"q\"t"}`},
		{`{"a":1,"a":2}`, `{"a":2}`},
	} {
		f.Add(s[0], s[1])
	}
	f.Fuzz(func(t *testing.T, a, b string) {
		l, err := types.ParseJSONB(a)
		if err != nil {
			return
		}
		r, err := types.ParseJSONB(b)
		if err != nil {
			return
		}
		checkContainsEquiv(t, l, r)
	})
}

// TestJSONBContainsMalformed: a stored text the walk cannot read falls
// back to decoding, which reports it. (The walk reads no further than
// the answer needs, so damage past that point goes unnoticed: stored
// jsonb text is always well-formed.)
func TestJSONBContainsMalformed(t *testing.T) {
	pat, err := types.ParseJSONB(`{"a":1}`)
	if err != nil {
		t.Fatal(err)
	}
	for _, doc := range []string{`{"a":`, `{"a" 1}`, `{"b":"x,"a":1}`, `{"a":tru}`, ``, `{"a":}`, `{"b":[1,"a":1}`} {
		if _, err := jsonbContains(types.Datum{Fam: types.Jsonb, S: doc}, pat); err == nil {
			t.Errorf("%q: no error", doc)
		}
	}
	// White space the walk steps over (stored text has none).
	ok, err := jsonbContains(types.Datum{Fam: types.Jsonb, S: " { \"b\" : [ 1 , 2 ] ,\n\"a\" : 1.0 } "}, pat)
	if err != nil || !ok {
		t.Fatalf("spaced document: %v, %v", ok, err)
	}
}

// TestJSONBFilterCompilesOnce: a scan's filter compiles a constant @>
// operand once, not per row, and the walk allocates nothing per row.
func TestJSONBFilterCompilesOnce(t *testing.T) {
	desc, where, row := benchJSONBFilter(t)
	var memo whereMemo
	if ok, err := matchesWhereMemo(where, desc, row, nil, &memo); err != nil || !ok {
		t.Fatalf("first row: %v, %v", ok, err)
	}
	if len(memo.jsonb) != 1 {
		t.Fatalf("memo holds %d patterns, want 1", len(memo.jsonb))
	}
	allocs := testing.AllocsPerRun(200, func() {
		if ok, err := matchesWhereMemo(where, desc, row, nil, &memo); err != nil || !ok {
			t.Fatalf("row: %v, %v", ok, err)
		}
	})
	if allocs != 0 {
		t.Fatalf("filter allocated %v times per row, want 0", allocs)
	}
	if len(memo.jsonb) != 1 {
		t.Fatalf("memo holds %d patterns, want 1", len(memo.jsonb))
	}
}

// benchJSONBFilter is the bench jsonb-contain workload's table, filter
// and one of its documents.
func benchJSONBFilter(t testing.TB) (*catalog.TableDescriptor, []parser.Comparison, map[catalog.ColumnID]types.Datum) {
	desc := &catalog.TableDescriptor{
		Name:       "docs",
		Columns:    []catalog.Column{{ID: 1, Name: "k", Type: types.Int}, {ID: 2, Name: "doc", Type: types.Jsonb}},
		PrimaryKey: []catalog.ColumnID{1},
	}
	stmts, err := parser.Parse(`SELECT k FROM docs WHERE doc @> '{"tenant": 417}'`)
	if err != nil {
		t.Fatal(err)
	}
	doc, err := types.ParseJSONB(benchJSONBDoc(417417))
	if err != nil {
		t.Fatal(err)
	}
	row := map[catalog.ColumnID]types.Datum{1: types.NewInt(417417), 2: doc}
	return desc, stmts[0].(*parser.Select).Where, row
}

// benchJSONBDoc is document j of the bench workload (1,000 tenants, a
// 64-byte pad).
func benchJSONBDoc(j int) string {
	return fmt.Sprintf(`{"tenant": %d, "kind": "k%d", "tags": ["t%d", "t%d"], "attrs": {"seq": %d, "ok": %t}, "pad": "%s"}`,
		j%1000, j%7, j%50, (j/50)%50, j, j%2 == 0, strings.Repeat("x", 64))
}

// BenchmarkJSONBContains is one document tested against doc @>
// '{"tenant": n}', per implementation: tree decodes both sides (as
// before), walk is the text walk per call (pattern compiled each time),
// compiled is the walk with the pattern compiled once, and filter is a
// scan's whole WHERE evaluation of one row. The match and miss variants
// are a document of the tenant and of another one.
func BenchmarkJSONBContains(b *testing.B) {
	desc, where, row := benchJSONBFilter(b)
	pat, err := types.ParseJSONB(`{"tenant": 417}`)
	if err != nil {
		b.Fatal(err)
	}
	compiled, err := jsonbcontain.Compile(pat.S)
	if err != nil {
		b.Fatal(err)
	}
	for _, c := range []struct {
		name string
		j    int
	}{{"match", 417417}, {"miss", 417418}} {
		doc, err := types.ParseJSONB(benchJSONBDoc(c.j))
		if err != nil {
			b.Fatal(err)
		}
		want := c.name == "match"
		b.Run(c.name+"/tree", func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				if ok, _ := jsonbContainsTree(doc, pat); ok != want {
					b.Fatal("wrong answer")
				}
			}
		})
		b.Run(c.name+"/walk", func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				if ok, _ := jsonbContains(doc, pat); ok != want {
					b.Fatal("wrong answer")
				}
			}
		})
		b.Run(c.name+"/compiled", func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				if ok, _ := compiled.ContainedIn(doc.S); ok != want {
					b.Fatal("wrong answer")
				}
			}
		})
		r := map[catalog.ColumnID]types.Datum{1: row[1], 2: doc}
		b.Run(c.name+"/filter", func(b *testing.B) {
			b.ReportAllocs()
			var memo whereMemo
			for i := 0; i < b.N; i++ {
				if ok, _ := matchesWhereMemo(where, desc, r, nil, &memo); ok != want {
					b.Fatal("wrong answer")
				}
			}
		})
		b.Run(c.name+"/filter-before", func(b *testing.B) {
			// The filter as it ran before: the operand re-coerced and
			// both sides decoded per row.
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				rhs, err := types.NewString(`{"tenant": 417}`).Coerce(types.Jsonb)
				if err != nil {
					b.Fatal(err)
				}
				if ok, _ := jsonbContainsTree(doc, rhs); ok != want {
					b.Fatal("wrong answer")
				}
			}
		})
	}
}

// BenchmarkJSONBContainsScanRow is one stored row of the bench workload
// through a full scan's filter, decode included: before decodes every
// row into a new map and the filter decodes both operands; after
// recycles a rejected row's map and walks the document. The miss case
// is the common one (999 of 1,000 rows).
func BenchmarkJSONBContainsScanRow(b *testing.B) {
	desc, where, _ := benchJSONBFilter(b)
	desc.ID = 104
	ctx := context.Background()
	for _, c := range []struct {
		name string
		j    int
	}{{"match", 417417}, {"miss", 417418}} {
		doc, err := types.ParseJSONB(benchJSONBDoc(c.j))
		if err != nil {
			b.Fatal(err)
		}
		key, err := rowenc.EncodePK(desc, []types.Datum{types.NewInt(int64(c.j))})
		if err != nil {
			b.Fatal(err)
		}
		val, err := rowenc.EncodeValue(desc, map[catalog.ColumnID]types.Datum{2: doc})
		if err != nil {
			b.Fatal(err)
		}
		want := c.name == "match"
		b.Run(c.name+"/before", func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				row, err := decodeFullRow(ctx, nil, desc, key, val)
				if err != nil {
					b.Fatal(err)
				}
				rhs, err := types.NewString(`{"tenant": 417}`).Coerce(types.Jsonb)
				if err != nil {
					b.Fatal(err)
				}
				if ok, _ := jsonbContainsTree(row[2], rhs); ok != want {
					b.Fatal("wrong answer")
				}
			}
		})
		b.Run(c.name+"/after", func(b *testing.B) {
			b.ReportAllocs()
			it := &scanIter{desc: desc, where: where}
			row := map[catalog.ColumnID]types.Datum{}
			for i := 0; i < b.N; i++ {
				clear(row)
				row, err := decodeRowInto(ctx, nil, desc, key, val, nil, row)
				if err != nil {
					b.Fatal(err)
				}
				ok, err := matchesWhereMemo(where, desc, row, nil, &it.memo)
				if err != nil || ok != want {
					b.Fatal("wrong answer")
				}
			}
		})
	}
}
