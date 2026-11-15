package sql

import (
	"bytes"
	"sort"
	"testing"

	"github.com/sthorne/datax/pkg/sql/catalog"
	"github.com/sthorne/datax/pkg/sql/inverted"
	"github.com/sthorne/datax/pkg/sql/parser"
	"github.com/sthorne/datax/pkg/sql/rowenc"
	"github.com/sthorne/datax/pkg/sql/types"
)

// textSearchTestDesc: PRIMARY KEY (id); v TSVECTOR with a GIN index;
// body TEXT indexed by its English lexemes; title TEXT unindexed.
func textSearchTestDesc() *catalog.TableDescriptor {
	return &catalog.TableDescriptor{
		ID: 44, Name: "articles",
		Columns: []catalog.Column{
			{ID: 1, Name: "id", Type: types.Int, NotNull: true},
			{ID: 2, Name: "v", Type: types.Tsvector},
			{ID: 3, Name: "body", Type: types.String},
			{ID: 4, Name: "title", Type: types.String},
		},
		PrimaryKey: []catalog.ColumnID{1},
		Indexes: []catalog.IndexDescriptor{
			{ID: 2, Name: "articles_v", ColumnIDs: []catalog.ColumnID{2}, Inverted: inverted.TSVectorOps},
			{ID: 3, Name: "articles_body", ColumnIDs: []catalog.ColumnID{3}, Inverted: inverted.TextSearchKindName("english")},
		},
	}
}

func whereOf(t *testing.T, cond string) []parser.Comparison {
	t.Helper()
	stmts, err := parser.Parse("SELECT id FROM articles WHERE " + cond)
	if err != nil {
		t.Fatalf("%s: %v", cond, err)
	}
	return stmts[0].(*parser.Select).Where
}

func TestPickPlanTextSearch(t *testing.T) {
	desc := textSearchTestDesc()
	for _, c := range []struct {
		where string
		want  string // the plan; "" = a full scan
	}{
		{`v @@ to_tsquery('english', 'cats & fat')`, `inverted scan of index "articles_v" ("cat" AND "fat") + primary key join, rows rechecked`},
		{`v @@ 'cat | rat'`, `inverted scan of index "articles_v" ("cat" OR "rat") + primary key join, rows rechecked`},
		{`v @@ 'cat'::tsquery AND title = 'x'`, `inverted scan of index "articles_v" ("cat") + primary key join, rows rechecked`},
		{`v @@ plainto_tsquery('english', 'The fat cats')`, `inverted scan of index "articles_v" ("fat" AND "cat") + primary key join, rows rechecked`},
		{`v @@ phraseto_tsquery('english', 'fat cats')`, `inverted scan of index "articles_v" ("fat" AND "cat") + primary key join, rows rechecked`},
		{`v @@ websearch_to_tsquery('english', '"fat cats" -dog')`, `inverted scan of index "articles_v" ("fat" AND "cat") + primary key join, rows rechecked`},
		{`v @@ to_tsquery('simple', 'cat:* & !dog')`, `inverted scan of index "articles_v" ("cat":*) + primary key join, rows rechecked`},
		{`v @@ 'a' OR v @@ 'b'`, `inverted scan of index "articles_v" ("a" OR "b") + primary key join, rows rechecked`},
		// The query on the left.
		{`to_tsquery('english', 'cat') @@ v`, ``},
		// Nothing for the index to look up.
		{`v @@ '!cat'`, ``},
		{`v @@ 'cat | !dog'`, ``},
		{`NOT (v @@ 'cat')`, ``},
		{`v @@ 'cat' OR title = 'x'`, ``},
		// A query of stop words matches nothing, and the index knows it.
		{`v @@ to_tsquery('english', 'the')`, `inverted scan of index "articles_v" (none) + primary key join, rows rechecked`},
		// The text column, through the expression its index was built on.
		{`to_tsvector('english', body) @@ to_tsquery('english', 'cats <-> fat')`, `inverted scan of index "articles_body" ("cat" AND "fat") + primary key join, rows rechecked`},
		{`to_tsvector('pg_catalog.english', body) @@ 'cat'`, `inverted scan of index "articles_body" ("cat") + primary key join, rows rechecked`},
		{`to_tsvector('English'::regconfig, articles.body) @@ 'cat'`, `inverted scan of index "articles_body" ("cat") + primary key join, rows rechecked`},
		// Another configuration, another column, another expression: the
		// index holds different lexemes, or none of that column's.
		{`to_tsvector('simple', body) @@ 'cat'`, ``},
		{`to_tsvector('english', title) @@ 'cat'`, ``},
		{`to_tsvector('english', lower(body)) @@ 'cat'`, ``},
		{`to_tsvector('english', body || title) @@ 'cat'`, ``},
		{`setweight(to_tsvector('english', body), 'A') @@ 'cat'`, ``},
		{`body = 'cat'`, ``},
	} {
		plan, err := pickPlan(desc, whereOf(t, c.where), nil)
		if err != nil {
			t.Errorf("%s: %v", c.where, err)
			continue
		}
		want := c.want
		if want == "" {
			want = "full table scan"
		}
		if plan.String() != want {
			t.Errorf("%s:\n got %s\nwant %s", c.where, plan.String(), want)
		}
	}
	// A parameter is planned once its value is known.
	where := whereOf(t, `v @@ to_tsquery('english', $1)`)
	plan, err := pickPlan(desc, where, []types.Datum{types.NewString("rats | mice")})
	if err != nil || plan.String() != `inverted scan of index "articles_v" ("rat" OR "mice") + primary key join, rows rechecked` {
		t.Errorf("parameter: %s (%v)", plan.String(), err)
	}
}

// TestInvertedPrefixSpan: the span of a prefix holds exactly the
// entries of the elements that start with it, whatever bytes they hold.
func TestInvertedPrefixSpan(t *testing.T) {
	desc := textSearchTestDesc()
	idx := &desc.Indexes[0]
	lexemes := []string{"a", "ab", "ab\x00c", "abc", "abd", "b", "ac", "ab\xff", "\x00", "a\x00", "abcd"}
	type entry struct {
		lexeme string
		key    []byte
	}
	var entries []entry
	for i, lex := range lexemes {
		v := types.Datum{Fam: types.Tsvector, S: (&quoted{lex}).String()}
		es, err := rowenc.IndexEntries(desc, idx, map[catalog.ColumnID]types.Datum{1: types.NewInt(int64(i)), 2: v})
		if err != nil || len(es) != 1 {
			t.Fatalf("%q: %d entries (%v)", lex, len(es), err)
		}
		entries = append(entries, entry{lex, es[0].Key})
	}
	for _, prefix := range []string{"", "a", "ab", "abc", "ab\x00", "b", "c", "\x00", "ab\xff"} {
		start, end := rowenc.InvertedPrefixSpan(desc, idx, []byte(prefix))
		var got, want []string
		for _, e := range entries {
			if bytes.Compare(e.key, start) >= 0 && bytes.Compare(e.key, end) < 0 {
				got = append(got, e.lexeme)
			}
			if bytes.HasPrefix([]byte(e.lexeme), []byte(prefix)) {
				want = append(want, e.lexeme)
			}
		}
		sort.Strings(got)
		sort.Strings(want)
		if len(got) != len(want) {
			t.Errorf("prefix %q: span holds %q, want %q", prefix, got, want)
			continue
		}
		for i := range got {
			if got[i] != want[i] {
				t.Errorf("prefix %q: span holds %q, want %q", prefix, got, want)
				break
			}
		}
	}
}

// quoted renders one lexeme as a tsvector literal.
type quoted struct{ lexeme string }

func (q *quoted) String() string {
	out := []byte{'\''}
	for i := 0; i < len(q.lexeme); i++ {
		switch c := q.lexeme[i]; c {
		case '\'':
			out = append(out, '\'', '\'')
		case '\\':
			out = append(out, '\\', '\\')
		default:
			out = append(out, c)
		}
	}
	return string(append(out, '\''))
}
