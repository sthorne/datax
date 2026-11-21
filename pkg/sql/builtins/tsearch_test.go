package builtins

import (
	"fmt"
	"strings"
	"sync"
	"testing"

	"github.com/sthorne/datax/pkg/sql/types"
)

func call(t *testing.T, name string, args ...types.Datum) (types.Datum, error) {
	t.Helper()
	b, ok := Lookup(name)
	if !ok {
		t.Fatalf("no builtin %s", name)
	}
	return b.Call(args)
}

func TestTextSearchBuiltins(t *testing.T) {
	for _, c := range []struct {
		name string
		args []types.Datum
		want string
	}{
		{"to_tsvector", []types.Datum{str("english"), str("The Fat Rats")}, "'fat':2 'rat':3"},
		{"to_tsvector", []types.Datum{str("simple"), str("The Fat Rats")}, "'fat':2 'rats':3 'the':1"},
		{"to_tsvector", []types.Datum{str("The Fat Rats")}, "'fat':2 'rat':3"},
		{"to_tsvector", []types.Datum{str("english"), types.NewInt(42)}, "'42':1"},
		{"to_tsquery", []types.Datum{str("english"), str("Fat | Rats:AB")}, "'fat' | 'rat':AB"},
		{"plainto_tsquery", []types.Datum{str("english"), str("The Fat Rats")}, "'fat' & 'rat'"},
		{"phraseto_tsquery", []types.Datum{str("english"), str("The Fat Rats")}, "'fat' <-> 'rat'"},
		{"websearch_to_tsquery", []types.Datum{str("english"), str(`"fat rats" or cats -dogs`)}, "'fat' <-> 'rat' | 'cat' & !'dog'"},
		{"setweight", []types.Datum{str("fat:2 cat"), str("a")}, "'cat' 'fat':2A"},
		{"strip", []types.Datum{str("fat:2A cat:3")}, "'cat' 'fat'"},
		{"numnode", []types.Datum{str("fat & !rat")}, "4"},
		{"numnode", []types.Datum{str("")}, "0"},
		{"tsvector_to_array", []types.Datum{str("fat:2 cat 'a b'")}, "{\"a b\",cat,fat}"},
		{"ts_rank", []types.Datum{str("cat:1"), str("cat")}, "0.06079271"},
		{"ts_rank", []types.Datum{str("cat:1 dog:2"), str("cat"), types.NewInt(2)}, "0.030396355"},
		{"ts_rank_cd", []types.Datum{str("cat:1 rat:2"), str("cat & rat")}, "0.1"},
		{"ts_headline", []types.Datum{str("english"), str("Fat cats ate rats"), str("cat")}, "Fat <b>cats</b> ate rats"},
		{"ts_headline", []types.Datum{str("simple"), str("Fat cats ate rats"), str("cats"), str("StartSel=*, StopSel=*")}, "Fat *cats* ate rats"},
	} {
		got, err := call(t, c.name, c.args...)
		if err != nil {
			t.Errorf("%s: %v", c.name, err)
			continue
		}
		if got.Text() != c.want {
			t.Errorf("%s(%v) = %s, want %s", c.name, c.args, got.Text(), c.want)
		}
	}
	// Weights: {D, C, B, A}.
	weights := types.NewArray(types.Float, []types.Datum{types.NewFloat(0.1), types.NewFloat(0.2), types.NewFloat(0.4), types.NewFloat(0.5)})
	if got, err := call(t, "ts_rank", weights, str("cat:1A"), str("cat")); err != nil || got.Text() != "0.30396354" {
		t.Errorf("ts_rank with weights: %s, %v", got.Text(), err)
	}
	// A typed tsquery argument tells ts_headline which form it was
	// called in: (document, query, options) or (config, document, query).
	q, _ := types.ParseTsquery("cats")
	if got, err := call(t, HeadlineWithDefault, str("simple"), str("Fat cats"), q, str("StartSel=[, StopSel=]")); err != nil || got.S != "Fat [cats]" {
		t.Errorf("headline, default configuration: %q, %v", got.S, err)
	}
	if got, err := call(t, HeadlineWithDefault, str("simple"), str("english"), str("Fat cats"), types.Datum{Fam: types.Tsquery, S: "'cat'"}); err != nil || got.S != "Fat <b>cats</b>" {
		t.Errorf("headline, named configuration: %q, %v", got.S, err)
	}
	// Three untyped texts: (config, document, query).
	if got, err := call(t, HeadlineWithDefault, str("simple"), str("english"), str("Fat cats"), str("cat")); err != nil || got.S != "Fat <b>cats</b>" {
		t.Errorf("headline, three texts: %q, %v", got.S, err)
	}
	if got, err := call(t, "to_tsvector", str("english"), types.DNull); err != nil || !got.Null {
		t.Errorf("NULL text: %+v, %v", got, err)
	}
	for _, c := range []struct {
		name string
		args []types.Datum
		code string
	}{
		{"to_tsvector", []types.Datum{str("klingon"), str("x")}, "42704"},
		{"to_tsquery", []types.Datum{str("english"), str("a &")}, "42601"},
		{"setweight", []types.Datum{str("a:1"), str("E")}, CodeInvalidArgument},
		{"ts_rank", []types.Datum{str("a:1"), str("a |")}, "42601"},
		{"ts_rank", []types.Datum{types.NewArray(types.Float, []types.Datum{types.NewFloat(1)}), str("a:1"), str("a")}, "2202E"},
		{"ts_headline", []types.Datum{str("english"), str("doc"), str("a"), str("Bogus=1")}, CodeInvalidArgument},
		{"strip", []types.Datum{str("a:0")}, CodeUndefined},
	} {
		_, err := call(t, c.name, c.args...)
		be, ok := err.(*Error)
		if !ok || be.Code != c.code {
			t.Errorf("%s(%v): %v, want %s", c.name, c.args, err, c.code)
		}
	}
	if as, ok := TextSearchDefaultArg("to_tsvector", 1); !ok || as != "to_tsvector" {
		t.Error("to_tsvector(text) takes the default configuration")
	}
	if _, ok := TextSearchDefaultArg("to_tsvector", 2); ok {
		t.Error("to_tsvector(config, text) does not")
	}
	if as, ok := TextSearchDefaultArg("ts_headline", 3); !ok || as != HeadlineWithDefault {
		t.Error("ts_headline carries the default configuration")
	}
}

func TestTSMatch(t *testing.T) {
	v, _ := types.ParseTsvector("fat:1 cat:2")
	q, _ := types.ParseTsquery("fat <-> cat")
	for _, c := range []struct {
		l, r types.Datum
		want bool
	}{
		{v, q, true},
		{q, v, true},
		{v, str("cat & !dog"), true},
		{v, str("dog"), false},
	} {
		if got, err := TSMatch(c.l, c.r); err != nil || got != c.want {
			t.Errorf("%s @@ %s = %v, %v", c.l.Text(), c.r.Text(), got, err)
		}
	}
	// Text on the left of a query is not guessed at.
	if _, err := TSMatch(str("fat cat"), q); err == nil || !strings.Contains(err.Error(), "to_tsvector") {
		t.Errorf("text @@ tsquery: %v", err)
	}
	for _, c := range [][2]types.Datum{{str("a"), str("b")}, {v, v}, {q, q}, {v, types.NewInt(1)}} {
		if _, err := TSMatch(c[0], c[1]); err == nil {
			t.Errorf("%s @@ %s was accepted", c[0].Fam, c[1].Fam)
		}
	}
	if _, err := TSMatch(v, str("a &")); err == nil {
		t.Error("a malformed query was accepted")
	}
	cat, err := ConcatTSVectors(v, str("dog:1"))
	if err != nil || cat.Text() != "'cat':2 'dog':3 'fat':1" {
		t.Errorf("concat: %s, %v", cat.Text(), err)
	}
	if d, err := Cast(str("b a:1"), "tsvector"); err != nil || d.Fam != types.Tsvector || d.S != "'a':1 'b'" {
		t.Errorf("cast to tsvector: %+v, %v", d, err)
	}
	if d, err := Cast(str("a & b:*"), "pg_catalog.tsquery"); err != nil || d.Fam != types.Tsquery || d.S != "'a' & 'b':*" {
		t.Errorf("cast to tsquery: %+v, %v", d, err)
	}
	if d, err := Cast(v, "text"); err != nil || d.Fam != types.String || d.S != v.S {
		t.Errorf("cast to text: %+v, %v", d, err)
	}
	if _, err := Cast(str("a:x"), "tsvector"); err == nil {
		t.Error("a malformed tsvector was cast")
	}
	if CastFamily("tsvector", types.String) != types.Tsvector || CastFamily("tsquery", types.String) != types.Tsquery {
		t.Error("cast families")
	}
}

// TestTSMatcherCache: queries parsed once and shared stay correct when
// many goroutines match at once, past the cache's limit (it starts
// over), and a query that does not parse is an error every time.
func TestTSMatcherCache(t *testing.T) {
	v := types.Datum{Fam: types.Tsvector, S: "'cat':2 'fat':1"}
	var wg sync.WaitGroup
	errs := make(chan string, 8)
	for g := 0; g < 8; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			for i := 0; i < 2*tsMatcherLimit; i++ {
				q := types.Datum{Fam: types.Tsquery, S: fmt.Sprintf("'cat' & !'w%d'", (i+g)%(tsMatcherLimit+50))}
				if ok, err := TSMatch(v, q); err != nil || !ok {
					errs <- fmt.Sprintf("%s: %v, %v", q.S, ok, err)
					return
				}
				if _, err := TSMatch(v, types.NewString("bad &")); err == nil {
					errs <- "a query that does not parse matched"
					return
				}
			}
		}(g)
	}
	wg.Wait()
	close(errs)
	for e := range errs {
		t.Error(e)
	}
}
