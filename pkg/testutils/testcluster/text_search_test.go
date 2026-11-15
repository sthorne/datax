package testcluster

import (
	"context"
	"fmt"
	"math/rand"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"

	"github.com/sthorne/datax/pkg/keys"
	"github.com/sthorne/datax/pkg/server"
	"github.com/sthorne/datax/pkg/sql"
	"github.com/sthorne/datax/pkg/sql/catalog"
	"github.com/sthorne/datax/pkg/sql/tsearch"
	"github.com/sthorne/datax/pkg/sql/types"
	"github.com/sthorne/datax/pkg/version"
)

// Full-text search (issue #299): the tsvector and tsquery types, the @@
// operator, and GIN indexes over a tsvector column and over
// to_tsvector('config', text_column).

func TestTextSearch(t *testing.T) {
	tc := Start(t, 3)
	ctx, cancel := context.WithTimeout(context.Background(), 240*time.Second)
	defer cancel()
	s := sql.NewSession(tc.Nodes[0].DB(), catalog.NewAccessor())
	db := tc.Nodes[0].DB()

	execSQL(t, ctx, s, `CREATE TABLE articles (id INT8 PRIMARY KEY, body TEXT, v TSVECTOR, note TEXT)`)
	execSQL(t, ctx, s, `INSERT INTO articles (id, body, note) VALUES
		(1, 'The quick brown fox jumps over the lazy dog', 'x'),
		(2, 'A fat cat sat on a mat and ate a fat rat', 'x'),
		(3, 'Fatal error: the system crashed', 'y'),
		(4, 'The error was not fatal', 'y'),
		(5, 'PostgreSQL full text search with ranking; searching and searches', 'y'),
		(6, NULL, 'z'),
		(7, '', 'z'),
		(8, 'the of and', 'z'),
		(9, 'Cats and dogs: a catalog of categories', 'z'),
		(10, 'cat cat cat dog', 'z')`)
	execSQL(t, ctx, s, `UPDATE articles SET v = to_tsvector('english', body)`)
	res := execSQL(t, ctx, s, `SELECT v FROM articles WHERE id = 2`)
	if got := res.Rows[0][0]; got.Fam != types.Tsvector || got.Text() != `'ate':9 'cat':3 'fat':2,11 'mat':7 'rat':12 'sat':4` {
		t.Fatalf("stored vector: %s (%s)", got.Text(), got.Fam)
	}

	// What is refused.
	for q, code := range map[string]string{
		`CREATE INDEX u ON articles USING GIN (body)`:                               sql.CodeFeatureNotSupported,
		`CREATE INDEX u ON articles (v)`:                                            sql.CodeFeatureNotSupported,
		`CREATE UNIQUE INDEX u ON articles USING GIN (v)`:                           sql.CodeFeatureNotSupported,
		`CREATE INDEX u ON articles USING GIN (to_tsvector('klingon', body))`:       sql.CodeUndefinedObject,
		`CREATE INDEX u ON articles USING GIN (to_tsvector('english', v))`:          sql.CodeFeatureNotSupported,
		`CREATE INDEX u ON articles USING GIN (lower('english', body))`:             sql.CodeFeatureNotSupported,
		`CREATE INDEX u ON articles (to_tsvector('english', body))`:                 sql.CodeFeatureNotSupported,
		`CREATE INDEX u ON articles USING GIN (to_tsvector(body))`:                  sql.CodeSyntaxError,
		`CREATE INDEX u ON articles USING GIN (to_tsvector('english', body), note)`: sql.CodeSyntaxError,
		`CREATE INDEX u ON articles USING GIN (body to_tsvector_english)`:           sql.CodeUndefinedObject,
		`CREATE INDEX u ON articles USING GIN (v jsonb_ops)`:                        sql.CodeFeatureNotSupported,
		`CREATE TABLE bad (v TSVECTOR PRIMARY KEY)`:                                 sql.CodeFeatureNotSupported,
		`SELECT id FROM articles WHERE body @@ to_tsquery('english', 'cat')`:        sql.CodeFeatureNotSupported,
		`SELECT to_tsvector('klingon', 'x')`:                                        sql.CodeUndefinedObject,
		`SELECT to_tsquery('english', 'cat &')`:                                     sql.CodeSyntaxError,
		`SELECT 'a:0'::tsvector`:                                                    sql.CodeSyntaxError,
		`SET default_text_search_config = 'klingon'`:                                sql.CodeInvalidParameterValue,
	} {
		if _, serr := trySQL(ctx, s, q); serr == nil || serr.Code != code {
			t.Fatalf("%s: got %v, want %s", q, serr, code)
		}
	}

	// Both index forms, backfilled over the rows already there.
	execSQL(t, ctx, s, `CREATE INDEX articles_v ON articles USING GIN (v)`)
	execSQL(t, ctx, s, `CREATE INDEX articles_body ON articles USING GIN (to_tsvector('english', body))`)
	checkInvertedIndex(t, ctx, db, "articles", "articles_v")
	checkInvertedIndex(t, ctx, db, "articles", "articles_body")

	// EXPLAIN names the index and the lexemes it looks up.
	for q, want := range map[string]string{
		`SELECT id FROM articles WHERE v @@ to_tsquery('english', 'cats & dogs')`:                                                                `inverted scan of index "articles_v" ("cat" AND "dog") + primary key join, rows rechecked`,
		`SELECT id FROM articles WHERE v @@ to_tsquery('english', 'fatal <-> error')`:                                                            `inverted scan of index "articles_v" ("fatal" AND "error") + primary key join, rows rechecked`,
		`SELECT id FROM articles WHERE v @@ to_tsquery('english', 'cat:* | fox')`:                                                                `inverted scan of index "articles_v" ("cat":* OR "fox") + primary key join, rows rechecked`,
		`SELECT id FROM articles WHERE v @@ to_tsquery('english', 'cat & !dog')`:                                                                 `inverted scan of index "articles_v" ("cat") + primary key join, rows rechecked`,
		`SELECT id FROM articles WHERE to_tsvector('english', body) @@ to_tsquery('english', 'cats')`:                                            `inverted scan of index "articles_body" ("cat") + primary key join, rows rechecked`,
		`SELECT id FROM articles WHERE to_tsvector(body) @@ plainto_tsquery('fat rats')`:                                                         `inverted scan of index "articles_body" ("fat" AND "rat") + primary key join, rows rechecked`,
		`SELECT id FROM articles WHERE to_tsvector('simple', body) @@ to_tsquery('simple', 'cats')`:                                              `full table scan`,
		`SELECT id FROM articles WHERE v @@ to_tsquery('english', '!cat')`:                                                                       `full table scan`,
		`SELECT id FROM articles WHERE NOT (v @@ to_tsquery('english', 'cat'))`:                                                                  `full table scan`,
		`SELECT id FROM articles WHERE v @@ to_tsquery('english', 'cat') OR note = 'x'`:                                                          `full table scan`,
		`SELECT id FROM articles WHERE id = 2 AND v @@ to_tsquery('english', 'cat')`:                                                             `point lookup on primary key`,
		`SELECT id FROM articles WHERE v @@ to_tsquery('english', 'cat') ORDER BY id`:                                                            `inverted scan of index "articles_v" ("cat") + primary key join, rows rechecked; order satisfied by access path`,
		`SELECT id, ts_rank(v, to_tsquery('english', 'cat')) AS r FROM articles WHERE v @@ to_tsquery('english', 'cat') ORDER BY r DESC LIMIT 2`: ``,
	} {
		p := explainPlan(t, ctx, s, q)
		if want == "" {
			if !strings.HasPrefix(p, `inverted scan of index "articles_v" ("cat")`) {
				t.Fatalf("%s: plan %s", q, p)
			}
			continue
		}
		if p != want {
			t.Fatalf("%s:\n plan %s\n want %s", q, p, want)
		}
	}

	// Results, the same through each index as through a filter of the
	// whole table.
	cases := []struct {
		query string
		want  []int64
	}{
		{`cat`, []int64{2, 9, 10}},
		{`cats & dogs`, []int64{9, 10}},
		{`cat & !dog`, []int64{2}},
		{`fox | rat`, []int64{1, 2}},
		{`error & fatal`, []int64{3, 4}},
		{`fatal <-> error`, []int64{3}},
		{`error <-> fatal`, nil},
		{`error <3> fatal`, []int64{4}},
		{`quick <-> brown <-> fox`, []int64{1}},
		{`fat <-> cat`, []int64{2}},
		{`fat <-> rat`, []int64{2}},
		{`cat <-> fat`, nil},
		{`fat <-> !cat`, []int64{2}}, // the second "fat" is followed by "rat"
		{`cat:*`, []int64{2, 9, 10}}, // cat, catalog, categori
		{`catal:*`, []int64{9}},
		{`fa:* & err:*`, []int64{3, 4}},
		{`search`, []int64{5}},
		{`searching <-> searches`, nil}, // "and" stands between them
		{`searching <2> searches`, []int64{5}},
		{`lazy`, []int64{1}},
		{`(cat | fox) & (dog | rat)`, []int64{1, 2, 9, 10}},
		{`the`, nil},
		{`zebra`, nil},
		{`cat & zebra`, nil},
	}
	for _, c := range cases {
		for _, lhs := range []string{`v`, `to_tsvector('english', body)`} {
			q := `SELECT id FROM articles WHERE ` + lhs + ` @@ to_tsquery('english', '` + c.query + `')`
			got := sameBothWays(t, ctx, s, q)
			if fmt.Sprint(got) != fmt.Sprint(append([]int64{}, c.want...)) {
				t.Fatalf("%s: got %v, want %v", q, got, c.want)
			}
		}
	}
	// The other ways of writing a query, and a conjunct beside it.
	for q, want := range map[string]string{
		`SELECT id FROM articles WHERE v @@ plainto_tsquery('english', 'The Fat Rats')`:                          "[2]",
		`SELECT id FROM articles WHERE v @@ phraseto_tsquery('english', 'the lazy dogs')`:                        "[1]",
		`SELECT id FROM articles WHERE v @@ phraseto_tsquery('english', 'sat on a mat')`:                         "[2]",
		`SELECT id FROM articles WHERE v @@ websearch_to_tsquery('english', '"fat cat" -dog')`:                   "[2]",
		`SELECT id FROM articles WHERE v @@ websearch_to_tsquery('english', 'fox or "system crashed"')`:          "[1 3]",
		`SELECT id FROM articles WHERE v @@ 'cat & dog'`:                                                         "[9 10]",
		`SELECT id FROM articles WHERE v @@ 'cat'::tsquery AND note = 'x'`:                                       "[2]",
		`SELECT id FROM articles WHERE v @@ to_tsquery('english', 'cat') AND v @@ to_tsquery('english', 'mat')`:  "[2]",
		`SELECT id FROM articles WHERE v @@ to_tsquery('english', 'fox') OR v @@ to_tsquery('english', 'crash')`: "[1 3]",
	} {
		if got := sameBothWays(t, ctx, s, q); fmt.Sprint(got) != want {
			t.Fatalf("%s: got %v, want %s", q, got, want)
		}
	}
	// A parameter on the right-hand side is planned too.
	res = execSQL(t, ctx, s, `SELECT id FROM articles WHERE v @@ to_tsquery('english', $1) ORDER BY id`, types.NewString("cats & dogs"))
	if len(res.Rows) != 2 || res.Rows[0][0].I != 9 || res.Rows[1][0].I != 10 {
		t.Fatalf("parameterized search: %+v", res.Rows)
	}
	res = execSQL(t, ctx, s, `SELECT id FROM articles WHERE v @@ $1 ORDER BY id`, types.NewString("'cat' & 'mat'"))
	if len(res.Rows) != 1 || res.Rows[0][0].I != 2 {
		t.Fatalf("a tsquery parameter: %+v", res.Rows)
	}

	// Ranking: computed on the rows the index found, sorted, limited.
	res = execSQL(t, ctx, s, `SELECT id, ts_rank(v, to_tsquery('english', 'cat')) AS r, ts_rank_cd(v, to_tsquery('english', 'cat')) AS cd
		FROM articles WHERE v @@ to_tsquery('english', 'cat') ORDER BY r DESC, id LIMIT 2`)
	if len(res.Rows) != 2 || res.Rows[0][0].I != 10 || res.Rows[1][0].I != 2 {
		t.Fatalf("ranked: %+v", res.Rows)
	}
	if r, cd := res.Rows[0][1].F, res.Rows[0][2].F; r <= res.Rows[1][1].F || r < 0.08 || r > 0.09 || cd < 0.29 || cd > 0.31 {
		t.Fatalf("ranks of three occurrences: ts_rank %v, ts_rank_cd %v; of one: %v", r, cd, res.Rows[1][1].F)
	}
	res = execSQL(t, ctx, s, `SELECT ts_headline('english', body, to_tsquery('english', 'cat & rat'), 'StartSel=<<, StopSel=>>') FROM articles WHERE id = 2`)
	if got := res.Rows[0][0].S; got != `A fat <<cat>> sat on a mat and ate a fat <<rat>>` {
		t.Fatalf("headline: %q", got)
	}

	// The session's default configuration: a call that names none uses
	// it, and the expression index serves the call only while the two
	// agree.
	const oneArg = `SELECT id FROM articles WHERE to_tsvector(body) @@ to_tsquery('cats')`
	if got := sameBothWays(t, ctx, s, oneArg); fmt.Sprint(got) != "[2 9 10]" {
		t.Fatalf("default configuration: %v", got)
	}
	execSQL(t, ctx, s, `SET default_text_search_config = 'simple'`)
	if p := explainPlan(t, ctx, s, oneArg); p != "full table scan" {
		t.Fatalf("under simple, the english index serves %s", p)
	}
	if got := idsOf(t, ctx, s, oneArg); fmt.Sprint(got) != "[9]" { // only the literal word "cats"
		t.Fatalf("simple configuration: %v", got)
	}
	res = execSQL(t, ctx, s, `SELECT get_current_ts_config(), to_tsvector('The Cats'), current_setting('default_text_search_config')`)
	if r := res.Rows[0]; r[0].S != "simple" || r[1].Text() != `'cats':2 'the':1` || r[2].S != "pg_catalog.simple" {
		t.Fatalf("simple: %+v", r)
	}
	execSQL(t, ctx, s, `SET default_text_search_config TO 'pg_catalog.english'`)
	if got := sameBothWays(t, ctx, s, oneArg); fmt.Sprint(got) != "[2 9 10]" {
		t.Fatalf("back to english: %v", got)
	}

	// Maintenance. One changed word moves one entry out and one in, in
	// each index; the other column's update touches neither.
	before, _ := invertedEntries(t, ctx, db, "articles", "articles_body")
	execSQL(t, ctx, s, `UPDATE articles SET body = 'The quick brown fox jumps over the lazy cat' WHERE id = 1`)
	after, _ := invertedEntries(t, ctx, db, "articles", "articles_body")
	if removed, added := diffStrings(before, after); len(removed) != 1 || len(added) != 1 {
		t.Fatalf("one changed word moved %d entries out and %d in", len(removed), len(added))
	}
	vBefore, _ := invertedEntries(t, ctx, db, "articles", "articles_v")
	execSQL(t, ctx, s, `UPDATE articles SET note = 'q' WHERE id = 1`)
	if now, _ := invertedEntries(t, ctx, db, "articles", "articles_v"); len(now) != len(vBefore) {
		t.Fatalf("an unrelated update changed the index")
	}
	// The vector column is the application's to keep: until it is set
	// again it still says "dog".
	if got := sameBothWays(t, ctx, s, `SELECT id FROM articles WHERE v @@ 'dog'`); fmt.Sprint(got) != "[1 9 10]" {
		t.Fatalf("vector column before its update: %v", got)
	}
	if got := sameBothWays(t, ctx, s, `SELECT id FROM articles WHERE to_tsvector('english', body) @@ 'dog'`); fmt.Sprint(got) != "[9 10]" {
		t.Fatalf("expression index after the body changed: %v", got)
	}
	execSQL(t, ctx, s, `UPDATE articles SET v = to_tsvector('english', body) WHERE id = 1`)
	execSQL(t, ctx, s, `UPDATE articles SET body = NULL, v = NULL WHERE id = 3`)
	execSQL(t, ctx, s, `UPDATE articles SET body = 'rats', v = 'rat:1' WHERE id = 6`)
	execSQL(t, ctx, s, `DELETE FROM articles WHERE id = 4`)
	execSQL(t, ctx, s, `DELETE FROM articles WHERE v @@ 'search'`) // found through the index it then edits
	checkInvertedIndex(t, ctx, db, "articles", "articles_v")
	checkInvertedIndex(t, ctx, db, "articles", "articles_body")
	for q, want := range map[string]string{
		`SELECT id FROM articles WHERE v @@ 'cat'`:                               "[1 2 9 10]",
		`SELECT id FROM articles WHERE v @@ 'rat'`:                               "[2 6]",
		`SELECT id FROM articles WHERE to_tsvector('english', body) @@ 'rat'`:    "[2 6]",
		`SELECT id FROM articles WHERE to_tsvector('english', body) @@ 'error'`:  "[]",
		`SELECT id FROM articles WHERE to_tsvector('english', body) @@ 'search'`: "[]",
	} {
		if got := sameBothWays(t, ctx, s, q); fmt.Sprint(got) != want {
			t.Fatalf("after maintenance, %s: got %v, want %s", q, got, want)
		}
	}
	// A rolled-back write leaves the indexes as they were; inside the
	// transaction its own entries are seen.
	n := checkInvertedIndex(t, ctx, db, "articles", "articles_body")
	execSQL(t, ctx, s, `BEGIN`)
	execSQL(t, ctx, s, `INSERT INTO articles VALUES (50, 'a zebra crossing', to_tsvector('english', 'a zebra crossing'), 'r')`)
	execSQL(t, ctx, s, `SET inverted_index_scan = on`)
	if got := idsOf(t, ctx, s, `SELECT id FROM articles WHERE to_tsvector('english', body) @@ 'zebra'`); fmt.Sprint(got) != "[50]" {
		t.Fatalf("a transaction does not see its own entries: %v", got)
	}
	execSQL(t, ctx, s, `ROLLBACK`)
	execSQL(t, ctx, s, `RESET inverted_index_scan`)
	if m := checkInvertedIndex(t, ctx, db, "articles", "articles_body"); m != n {
		t.Fatalf("rollback left %d entries, was %d", m, n)
	}
	// A document with more distinct lexemes than one row may have
	// entries is refused, not half indexed.
	var big strings.Builder
	for i := 0; i <= 8192; i++ {
		fmt.Fprintf(&big, "w%d ", i)
	}
	if _, serr := trySQL(ctx, s, `INSERT INTO articles (id, body) VALUES (60, '`+big.String()+`')`); serr == nil || serr.Code != sql.CodeProgramLimitExceeded {
		t.Fatalf("oversized document: %v", serr)
	}

	// The catalogs.
	res = execSQL(t, ctx, s, `SELECT indexdef FROM pg_indexes WHERE tablename = 'articles' AND indexname <> 'articles_pkey' ORDER BY indexname`)
	if len(res.Rows) != 2 || res.Rows[0][0].S != `CREATE INDEX articles_body ON public.articles USING gin (to_tsvector('english'::regconfig, body))` ||
		res.Rows[1][0].S != `CREATE INDEX articles_v ON public.articles USING gin (v)` {
		t.Fatalf("pg_indexes: %+v", res.Rows)
	}
	res = execSQL(t, ctx, s, `SELECT typname, oid FROM pg_type WHERE typname IN ('tsvector', 'tsquery') ORDER BY typname`)
	if len(res.Rows) != 2 || res.Rows[0][1].I != 3615 || res.Rows[1][1].I != 3614 {
		t.Fatalf("pg_type: %+v", res.Rows)
	}
	res = execSQL(t, ctx, s, `SELECT a.amname FROM pg_class c JOIN pg_am a ON a.oid = c.relam WHERE c.relname = 'articles_body'`)
	if len(res.Rows) != 1 || res.Rows[0][0].S != "gin" {
		t.Fatalf("pg_class.relam: %+v", res.Rows)
	}
	// The definition pg_indexes prints is accepted back.
	execSQL(t, ctx, s, `CREATE INDEX articles_body2 ON public.articles USING gin (to_tsvector('english'::regconfig, body))`)
	execSQL(t, ctx, s, `CREATE INDEX IF NOT EXISTS articles_v2 ON articles USING gin (v tsvector_ops)`)
	execSQL(t, ctx, s, `CREATE INDEX articles_simple ON articles USING GIN ((to_tsvector('simple', body)))`)
	checkInvertedIndex(t, ctx, db, "articles", "articles_body2")
	checkInvertedIndex(t, ctx, db, "articles", "articles_v2")
	checkInvertedIndex(t, ctx, db, "articles", "articles_simple")
	if p := explainPlan(t, ctx, s, `SELECT id FROM articles WHERE to_tsvector('simple', body) @@ 'cats'`); !strings.HasPrefix(p, `inverted scan of index "articles_simple" ("cats")`) {
		t.Fatalf("simple index: %s", p)
	}
	execSQL(t, ctx, s, `DROP INDEX articles_body2`)
	execSQL(t, ctx, s, `DROP INDEX articles_v2`)
	execSQL(t, ctx, s, `DROP INDEX articles_simple`)

	// Every node plans and reads through both.
	for i := 1; i < 3; i++ {
		si := sql.NewSession(tc.Nodes[i].DB(), catalog.NewAccessor())
		if got := sameBothWays(t, ctx, si, `SELECT id FROM articles WHERE v @@ to_tsquery('english', 'cat <-> sat')`); fmt.Sprint(got) != "[2]" {
			t.Fatalf("node %d: %v", i+1, got)
		}
		if got := sameBothWays(t, ctx, si, `SELECT id FROM articles WHERE to_tsvector('english', body) @@ to_tsquery('english', 'ca:* & dog')`); fmt.Sprint(got) != "[9 10]" {
			t.Fatalf("node %d: %v", i+1, got)
		}
	}

	// LIKE ... INCLUDING ALL copies both kinds; TRUNCATE empties them.
	execSQL(t, ctx, s, `CREATE TABLE articles2 (LIKE articles INCLUDING ALL)`)
	execSQL(t, ctx, s, `INSERT INTO articles2 SELECT id, body, v, note FROM articles`)
	checkInvertedIndex(t, ctx, db, "articles2", "articles_v")
	checkInvertedIndex(t, ctx, db, "articles2", "articles_body")
	if got := sameBothWays(t, ctx, s, `SELECT id FROM articles2 WHERE to_tsvector('english', body) @@ 'cat'`); fmt.Sprint(got) != "[1 2 9 10]" {
		t.Fatalf("copied table: %v", got)
	}
	execSQL(t, ctx, s, `TRUNCATE articles2`)
	if m := checkInvertedIndex(t, ctx, db, "articles2", "articles_body"); m != 0 {
		t.Fatalf("truncate left %d entries", m)
	}
}

// textSearchVocabulary is small, so random queries match some rows and
// miss others; it has words that share stems and prefixes, and stop
// words.
var textSearchVocabulary = strings.Fields(`cat cats catalog category dog dogs dogged run running runner fat fatal
	error errors rat rats mat the a of and search searching index indexes indexed quick brown fox lazy system
	crash crashed data database databases query queries rank ranking 42 3.14 node1 user@example.com example.com`)

func randText(r *rand.Rand) string {
	words := make([]string, 1+r.Intn(14))
	for i := range words {
		words[i] = textSearchVocabulary[r.Intn(len(textSearchVocabulary))]
	}
	return strings.Join(words, []string{" ", ", ", " - ", ". "}[r.Intn(4)])
}

// randTSQuery builds a random tsquery text over the vocabulary.
func randTSQuery(r *rand.Rand, depth int) string {
	if depth == 0 || r.Intn(3) == 0 {
		w := textSearchVocabulary[r.Intn(len(textSearchVocabulary))]
		if strings.ContainsAny(w, "@.") {
			w = "'" + w + "'"
		}
		if r.Intn(7) == 0 {
			if len(w) > 3 && !strings.HasPrefix(w, "'") {
				w = w[:2+r.Intn(2)]
			}
			return w + ":*"
		}
		return w
	}
	a, b := randTSQuery(r, depth-1), randTSQuery(r, depth-1)
	switch r.Intn(9) {
	case 0:
		return "!(" + a + ")"
	case 1, 2, 3:
		return "(" + a + " & " + b + ")"
	case 4, 5:
		return "(" + a + " | " + b + ")"
	case 6:
		return fmt.Sprintf("(%s <%d> %s)", a, 1+r.Intn(3), b)
	}
	return "(" + a + " <-> " + b + ")"
}

// TestTextSearchDifferential: over random documents, random queries —
// &, |, !, phrases and prefixes — return the same rows through either
// index as through a filter of the whole table, and the rows the
// matcher itself picks out; before and after random updates and
// deletes. And the indexes hold exactly what the rows imply.
func TestTextSearchDifferential(t *testing.T) {
	tc := Start(t, 1)
	ctx, cancel := context.WithTimeout(context.Background(), 240*time.Second)
	defer cancel()
	s := sql.NewSession(tc.Nodes[0].DB(), catalog.NewAccessor())
	db := tc.Nodes[0].DB()
	r := rand.New(rand.NewSource(299))
	english, _ := tsearch.LookupConfig("english")

	execSQL(t, ctx, s, `CREATE TABLE rnd (id INT8 PRIMARY KEY, body TEXT, v TSVECTOR)`)
	const rows = 400
	bodies := map[int]*string{}
	set := func(id int, body *string) string {
		bodies[id] = body
		if body == nil {
			return "NULL, NULL"
		}
		return fmt.Sprintf("'%s', to_tsvector('english', '%s')", *body, *body)
	}
	insert := func(lo, hi int) {
		var vals []string
		for i := lo; i < hi; i++ {
			var body *string
			if r.Intn(25) != 0 {
				text := randText(r)
				body = &text
			}
			vals = append(vals, fmt.Sprintf("(%d, %s)", i, set(i, body)))
		}
		execSQL(t, ctx, s, `INSERT INTO rnd VALUES `+strings.Join(vals, ", "))
	}
	// Half before the indexes (backfilled), half after (maintained).
	insert(0, rows/2)
	execSQL(t, ctx, s, `CREATE INDEX rnd_v ON rnd USING GIN (v)`)
	execSQL(t, ctx, s, `CREATE INDEX rnd_body ON rnd USING GIN (to_tsvector('english', body))`)
	insert(rows/2, rows)

	served, matched := 0, 0
	round := func(queries int) {
		for i := 0; i < queries; i++ {
			text := randTSQuery(r, 3)
			tsq, err := tsearch.ToQuery(english, text)
			if err != nil {
				t.Fatalf("%s: %v", text, err)
			}
			// What the matcher says, row by row.
			var want []int64
			for id, body := range bodies {
				if body == nil {
					continue
				}
				v, err := tsearch.ToVector(english, *body)
				if err != nil {
					t.Fatal(err)
				}
				if tsearch.Match(v, tsq) {
					want = append(want, int64(id))
				}
			}
			sort.Slice(want, func(i, j int) bool { return want[i] < want[j] })
			for _, lhs := range []string{"v", "to_tsvector('english', body)"} {
				q := `SELECT id FROM rnd WHERE ` + lhs + ` @@ to_tsquery('english', '` + strings.ReplaceAll(text, "'", "''") + `')`
				execSQL(t, ctx, s, `SET inverted_index_scan = on`)
				plan := explainPlan(t, ctx, s, q)
				indexed := idsOf(t, ctx, s, q)
				execSQL(t, ctx, s, `SET inverted_index_scan = off`)
				filtered := idsOf(t, ctx, s, q)
				if fmt.Sprint(indexed) != fmt.Sprint(filtered) || fmt.Sprint(indexed) != fmt.Sprint(append([]int64{}, want...)) {
					t.Fatalf("%s\n query: %s\n plan: %s\n through the index: %v\n as a filter:       %v\n the matcher:       %v", q, tsq, plan, indexed, filtered, want)
				}
				if strings.HasPrefix(plan, "inverted scan") {
					served++
					if len(indexed) > 0 {
						matched++
					}
				}
			}
		}
	}
	round(200)
	checkInvertedIndex(t, ctx, db, "rnd", "rnd_v")
	checkInvertedIndex(t, ctx, db, "rnd", "rnd_body")

	for i := 0; i < 150; i++ {
		id := r.Intn(rows)
		switch r.Intn(4) {
		case 0:
			execSQL(t, ctx, s, fmt.Sprintf(`DELETE FROM rnd WHERE id = %d`, id))
			delete(bodies, id)
		case 1:
			if _, ok := bodies[id]; ok {
				execSQL(t, ctx, s, fmt.Sprintf(`UPDATE rnd SET body = NULL, v = NULL WHERE id = %d`, id))
				bodies[id] = nil
			}
		default:
			if _, ok := bodies[id]; ok {
				text := randText(r)
				bodies[id] = &text
				execSQL(t, ctx, s, fmt.Sprintf(`UPDATE rnd SET body = '%s', v = to_tsvector('english', '%s') WHERE id = %d`, text, text, id))
			}
		}
	}
	checkInvertedIndex(t, ctx, db, "rnd", "rnd_v")
	checkInvertedIndex(t, ctx, db, "rnd", "rnd_body")
	round(200)
	if served < 300 || matched < 100 {
		t.Fatalf("%d random queries were served by an index and %d of them matched rows: the test is not exercising it", served, matched)
	}
}

// TestTextSearchBackfillUnderWrites: both kinds of index are built on a
// populated table while other gateways insert, update and delete. When
// each CREATE INDEX returns, the index holds exactly what the rows
// imply.
func TestTextSearchBackfillUnderWrites(t *testing.T) {
	tc := Start(t, 3)
	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Second)
	defer cancel()
	const ttl = time.Second
	s := leasedSession(t, tc, 0, ttl)

	doc := func(i, w int) string {
		return fmt.Sprintf("group%d report about topic%d and topic%d written by writer%d", i%10, i%7, i%11, w)
	}
	execSQL(t, ctx, s, `CREATE TABLE live (id INT8 PRIMARY KEY, body TEXT, v TSVECTOR)`)
	const seedRows = 600
	for lo := 0; lo < seedRows; lo += 100 {
		var vals []string
		for i := lo; i < lo+100; i++ {
			vals = append(vals, fmt.Sprintf(`(%d, '%s', to_tsvector('english', '%s'))`, i, doc(i, 0), doc(i, 0)))
		}
		execSQL(t, ctx, s, `INSERT INTO live VALUES `+strings.Join(vals, ", "))
	}

	var stop atomic.Bool
	var wg sync.WaitGroup
	var writes, failures atomic.Int64
	for w := 0; w < 3; w++ {
		ws := leasedSession(t, tc, w%3, ttl)
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			r := rand.New(rand.NewSource(int64(w) + 1))
			next := seedRows + w*100000
			for !stop.Load() {
				var q string
				switch r.Intn(4) {
				case 0:
					text := doc(r.Intn(1000), w+1) + " fresh"
					q = fmt.Sprintf(`INSERT INTO live VALUES (%d, '%s', to_tsvector('english', '%s'))`, next, text, text)
					next++
				case 1:
					// Deletes take the lower half of the seeded rows and
					// updates the upper half, so both kinds of row are
					// left to query however long the build runs.
					q = fmt.Sprintf(`DELETE FROM live WHERE id = %d`, r.Intn(seedRows/2))
				default:
					text := doc(r.Intn(1000), w+1) + " moved"
					q = fmt.Sprintf(`UPDATE live SET body = '%s', v = to_tsvector('english', '%s') WHERE id = %d`, text, text, seedRows/2+r.Intn(seedRows/2))
				}
				if _, serr := trySQL(ctx, ws, q); serr != nil {
					failures.Add(1)
				} else {
					writes.Add(1)
				}
			}
		}(w)
	}
	time.Sleep(300 * time.Millisecond)
	execSQL(t, ctx, s, `CREATE INDEX live_v ON live USING GIN (v)`)
	first := writes.Load()
	execSQL(t, ctx, s, `CREATE INDEX live_body ON live USING GIN (to_tsvector('english', body))`)
	during := writes.Load()
	time.Sleep(300 * time.Millisecond)
	stop.Store(true)
	wg.Wait()
	if first == 0 || during == first {
		t.Fatalf("no write committed while an index was being built (%d, %d)", first, during)
	}
	t.Logf("%d writes committed during the builds, %d after, %d failed", during, writes.Load()-during, failures.Load())

	checkInvertedIndex(t, ctx, tc.Nodes[0].DB(), "live", "live_v")
	checkInvertedIndex(t, ctx, tc.Nodes[0].DB(), "live", "live_body")
	for _, lhs := range []string{"v", "to_tsvector('english', body)"} {
		for _, q := range []string{`group3`, `moved`, `fresh`, `moved & writer2`, `report <-> about`, `writ:* & topic3`, `topic1 <2> topic1`} {
			if got := sameBothWays(t, ctx, s, `SELECT id FROM live WHERE `+lhs+` @@ to_tsquery('english', '`+q+`')`); len(got) == 0 {
				t.Fatalf("%s @@ %s matched nothing", lhs, q)
			}
		}
		for grp := 0; grp < 10; grp++ {
			for topic := 0; topic < 7; topic++ {
				sameBothWays(t, ctx, s, fmt.Sprintf(`SELECT id FROM live WHERE %s @@ to_tsquery('english', 'group%d & topic%d')`, lhs, grp, topic))
			}
		}
	}
}

// TestCreateIndexDiscardsEntriesWrittenDuringItsDrain: while CREATE
// INDEX waits for every gateway to adopt the write-only index, a
// gateway that has adopted it writes a row's entries and a gateway that
// has not then changes the row, or deletes it. Those entries describe
// nothing that exists; the build must not keep them. It is true of
// every kind of index — an ordinary one reported "entry points at a
// missing row" — and was found by the text-search backfill test above,
// which builds a second index while the first is being written.
func TestCreateIndexDiscardsEntriesWrittenDuringItsDrain(t *testing.T) {
	for name, ddl := range map[string]string{
		"ordinary":   `CREATE INDEX mixed_idx ON mixed (note)`,
		"jsonb":      `CREATE INDEX mixed_idx ON mixed USING GIN (doc)`,
		"tsvector":   `CREATE INDEX mixed_idx ON mixed USING GIN (v)`,
		"expression": `CREATE INDEX mixed_idx ON mixed USING GIN (to_tsvector('english', body))`,
	} {
		t.Run(name, func(t *testing.T) {
			tc := Start(t, 1)
			ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
			defer cancel()
			const ttl = 3 * time.Second
			builder := leasedSession(t, tc, 0, ttl)
			a, catA := leasedSessionWithAccessor(t, tc, 0, ttl)
			b, catB := leasedSessionWithAccessor(t, tc, 0, ttl)
			execSQL(t, ctx, builder, `CREATE TABLE mixed (id INT8 PRIMARY KEY, note TEXT, body TEXT, v TSVECTOR, doc JSONB)`)
			execSQL(t, ctx, builder, `INSERT INTO mixed VALUES (1, 'n0', 'alpha', 'alpha', '{"k": "alpha"}'), (2, 'n0', 'alpha', 'alpha', '{"k": "alpha"}'), (3, 'n0', 'alpha', 'alpha', '{"k": "alpha"}')`)
			// Both gateways hold the descriptor without the index, and
			// renew only when told to.
			execSQL(t, ctx, a, `SELECT id FROM mixed`)
			execSQL(t, ctx, b, `SELECT id FROM mixed`)
			catA.TestingPauseRenewal(true)
			catB.TestingPauseRenewal(true)
			for _, cat := range []*catalog.Accessor{catA, catB} {
				if err := cat.TestingRenewNow(ctx); err != nil {
					t.Fatal(err)
				}
			}
			done := make(chan *sql.Error, 1)
			go func() {
				_, serr := trySQL(ctx, builder, ddl)
				done <- serr
			}()
			// Wait for the write-only index to be published; the build
			// then waits for a and b.
			deadline := time.Now().Add(20 * time.Second)
			for len(lookupDescriptor(t, ctx, tc.Nodes[0].DB(), "mixed").Indexes) == 0 {
				if time.Now().After(deadline) {
					t.Fatal("the index was never published")
				}
				time.Sleep(20 * time.Millisecond)
			}
			// a adopts the index and writes entries for rows 1 and 2; b,
			// which has not, changes row 1 and deletes row 2.
			if err := catA.TestingRenewNow(ctx); err != nil {
				t.Fatal(err)
			}
			const beta = `note = 'n1', body = 'beta', v = 'beta', doc = '{"k": "beta"}'`
			execSQL(t, ctx, a, `UPDATE mixed SET `+beta+` WHERE id = 1`)
			execSQL(t, ctx, a, `UPDATE mixed SET `+beta+` WHERE id = 2`)
			execSQL(t, ctx, a, `UPDATE mixed SET `+beta+` WHERE id = 3`)
			execSQL(t, ctx, b, `UPDATE mixed SET note = 'n2', body = 'gamma', v = 'gamma', doc = '{"k": "gamma"}' WHERE id = 1`)
			execSQL(t, ctx, b, `DELETE FROM mixed WHERE id = 2`)
			catA.TestingPauseRenewal(false)
			catB.TestingPauseRenewal(false)
			if serr := <-done; serr != nil {
				t.Fatalf("%s: %v", ddl, serr)
			}
			checkInvertedIndex(t, ctx, tc.Nodes[0].DB(), "mixed", "mixed_idx")
			// Row 3, which only a wrote, is found; rows 1 and 2 are not
			// found under what a wrote for them.
			q := map[string]string{
				"ordinary":   `SELECT id FROM mixed WHERE note = 'n1'`,
				"jsonb":      `SELECT id FROM mixed WHERE doc @> '{"k": "beta"}'`,
				"tsvector":   `SELECT id FROM mixed WHERE v @@ 'beta'`,
				"expression": `SELECT id FROM mixed WHERE to_tsvector('english', body) @@ 'beta'`,
			}[name]
			if p := explainPlan(t, ctx, builder, q); !strings.Contains(p, "mixed_idx") {
				t.Fatalf("%s: plan %s", q, p)
			}
			if got := idsOf(t, ctx, builder, q); fmt.Sprint(got) != "[3]" {
				t.Fatalf("%s: %v", q, got)
			}
		})
	}
}

// TestTextSearchSplitsMergesAndRestart: the indexes span several ranges
// (split inside their own keyspace, so one lexeme's entries and one
// prefix's lexemes fall in different ranges), the ranges are merged
// back, and every node restarts in turn; lookups, the recheck and
// maintenance keep working throughout, on each node.
func TestTextSearchSplitsMergesAndRestart(t *testing.T) {
	tc, engines := StartWithEngines(t, 3)
	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Second)
	defer cancel()
	s := sql.NewSession(tc.Nodes[0].DB(), catalog.NewAccessor())
	db := tc.Nodes[0].DB()

	execSQL(t, ctx, s, `CREATE TABLE kept (id INT8 PRIMARY KEY, body TEXT, v TSVECTOR)`)
	execSQL(t, ctx, s, `CREATE INDEX kept_v ON kept USING GIN (v)`)
	execSQL(t, ctx, s, `CREATE INDEX kept_body ON kept USING GIN (to_tsvector('english', body))`)
	for lo := 0; lo < 300; lo += 100 {
		var vals []string
		for i := lo; i < lo+100; i++ {
			text := fmt.Sprintf("group%d named name%d tagged tag%d and tagging", i%5, i, i%3)
			vals = append(vals, fmt.Sprintf(`(%d, '%s', to_tsvector('english', '%s'))`, i, text, text))
		}
		execSQL(t, ctx, s, `INSERT INTO kept VALUES `+strings.Join(vals, ", "))
	}
	want := map[string]int{
		`group2`:             60,
		`group2 & tag0`:      20,
		`name17`:             1,
		`name1:*`:            111, // name1, name10..19, name100..199
		`named <-> name5`:    1,
		`tag:* & !group0`:    240,
		`tagged <3> tagging`: 300,
		`tagged <-> tagging`: 0,
		`group1 | group4`:    120,
	}
	check := func(s *sql.Session, where string) {
		t.Helper()
		for q, n := range want {
			for _, lhs := range []string{"v", "to_tsvector('english', body)"} {
				sel := `SELECT id FROM kept WHERE ` + lhs + ` @@ to_tsquery('english', '` + q + `')`
				if got := sameBothWays(t, ctx, s, sel); len(got) != n {
					t.Fatalf("%s: %s returned %d rows, want %d", where, sel, len(got), n)
				}
			}
		}
	}
	check(s, "before any split")

	// Each index is cut in four at its own entries; lefts[i] is a key
	// of the range to the left of splits[i].
	var splits, lefts []keys.Key
	for _, index := range []string{"kept_v", "kept_body"} {
		got, _ := invertedEntries(t, ctx, db, "kept", index)
		left := keys.Key(got[0])
		for _, at := range []int{len(got) / 4, len(got) / 2, 3 * len(got) / 4} {
			key := keys.Key(got[at])
			if _, err := db.AdminSplit(ctx, key); err != nil {
				t.Fatalf("split inside %s: %v", index, err)
			}
			splits, lefts = append(splits, key), append(lefts, left)
			left = key
		}
	}
	check(s, "after the splits")
	execSQL(t, ctx, s, `UPDATE kept SET body = 'group2 named moved tagged tag0 and tagging', v = to_tsvector('english', 'group2 named moved tagged tag0 and tagging') WHERE id = 1`)
	want[`group2`], want[`group2 & tag0`], want[`group1 | group4`], want[`name1:*`] = 61, 21, 119, 110
	check(s, "after a write across the splits")
	checkInvertedIndex(t, ctx, db, "kept", "kept_v")
	checkInvertedIndex(t, ctx, db, "kept", "kept_body")

	// Merge the ranges back together, right to left: the range that
	// starts at each split point joins the one to its left. (The merge
	// queue may get to a pair first; either way the boundary goes.)
	for i := len(splits) - 1; i >= 0; i-- {
		deadline := time.Now().Add(60 * time.Second)
		for {
			desc, ok, err := db.LookupMetaRecord(ctx, splits[i])
			if err != nil {
				t.Fatal(err)
			}
			if ok && !desc.StartKey.Equal(splits[i]) {
				break
			}
			_, merr := db.AdminMerge(ctx, lefts[i])
			if merr != nil && time.Now().After(deadline) {
				t.Fatalf("the range at %s was never merged into its left neighbour: %v", keys.Pretty(splits[i]), merr)
			}
			if merr != nil {
				time.Sleep(200 * time.Millisecond)
			}
		}
	}
	check(s, "after the merges")
	execSQL(t, ctx, s, `DELETE FROM kept WHERE id = 2`)
	want[`group2`], want[`tag:* & !group0`], want[`tagged <3> tagging`] = 60, 239, 299
	check(s, "after a write to the merged ranges")
	checkInvertedIndex(t, ctx, db, "kept", "kept_v")
	checkInvertedIndex(t, ctx, db, "kept", "kept_body")

	for i := 0; i < 3; i++ {
		tc.StopNode(i)
		n := tc.RestartNode(i, engines[i])
		si := sql.NewSession(n.DB(), catalog.NewAccessor())
		check(si, fmt.Sprintf("through node %d after its restart", i+1))
		text := "group2 named late tagged tag0 and tagging"
		execSQL(t, ctx, si, fmt.Sprintf(`INSERT INTO kept VALUES (%d, '%s', to_tsvector('english', '%s'))`, 1000+i, text, text))
		want[`group2`]++
		want[`group2 & tag0`]++
		want[`tagged <3> tagging`]++
		want[`tag:* & !group0`]++
	}
	sn := sql.NewSession(tc.Node(0).DB(), catalog.NewAccessor())
	check(sn, "after every node restarted")
	checkInvertedIndex(t, ctx, tc.Node(0).DB(), "kept", "kept_v")
	checkInvertedIndex(t, ctx, tc.Node(0).DB(), "kept", "kept_body")
}

// TestTextSearchBackupRestore: a table with both kinds of text-search
// index restores with the indexes usable and exact — full and
// incremental into a new cluster, and as one table into the running
// cluster, where the index entries are re-keyed under the new table.
func TestTextSearchBackupRestore(t *testing.T) {
	tc := Start(t, 3)
	ctx, cancel := context.WithTimeout(context.Background(), 240*time.Second)
	defer cancel()
	s := sql.NewSession(tc.Nodes[0].DB(), catalog.NewAccessor())
	waitForDatabases(t, ctx, s)

	execSQL(t, ctx, s, `CREATE TABLE saved (id INT8 PRIMARY KEY, body TEXT, v TSVECTOR, q TSQUERY)`)
	execSQL(t, ctx, s, `CREATE INDEX saved_v ON saved USING GIN (v)`)
	execSQL(t, ctx, s, `CREATE INDEX saved_body ON saved USING GIN (to_tsvector('english', body))`)
	put := func(s *sql.Session, id int, text string) {
		execSQL(t, ctx, s, fmt.Sprintf(`INSERT INTO saved VALUES (%d, '%s', setweight(to_tsvector('english', '%s'), 'B'), to_tsquery('english', 'group%d & !late'))`, id, text, text, id%5))
	}
	for i := 0; i < 50; i++ {
		put(s, i, fmt.Sprintf("group%d report tagged tag%d", i%5, i%3))
	}
	dirFull := t.TempDir()
	if _, err := tc.Nodes[0].RunBackup(ctx, dirFull, "", false, false); err != nil {
		t.Fatalf("full backup: %v", err)
	}
	// The incremental carries removed entries as well as new ones.
	execSQL(t, ctx, s, `UPDATE saved SET body = 'rewritten report', v = to_tsvector('english', 'rewritten report') WHERE id < 10`)
	execSQL(t, ctx, s, `DELETE FROM saved WHERE id >= 40`)
	put(s, 100, "group9 late report")
	dirInc := t.TempDir()
	if _, err := tc.Nodes[0].RunBackup(ctx, dirInc, dirFull, false, false); err != nil {
		t.Fatalf("incremental backup: %v", err)
	}
	queries := []string{
		`SELECT id FROM saved WHERE v @@ to_tsquery('english', 'rewritten')`,
		`SELECT id FROM saved WHERE v @@ to_tsquery('english', 'group1 & tag:*')`,
		`SELECT id FROM saved WHERE v @@ to_tsquery('english', 'report:B')`,
		`SELECT id FROM saved WHERE to_tsvector('english', body) @@ to_tsquery('english', 'late <-> report')`,
		`SELECT id FROM saved WHERE to_tsvector('english', body) @@ to_tsquery('english', 'group2 | group3')`,
	}

	// Everything, into a new cluster.
	tc2 := Start(t, 1)
	if _, err := tc2.Nodes[0].RunRestore(ctx, []string{dirFull, dirInc}); err != nil {
		t.Fatalf("restore: %v", err)
	}
	s2 := sql.NewSession(tc2.Nodes[0].DB(), catalog.NewAccessor())
	checkInvertedIndex(t, ctx, tc2.Nodes[0].DB(), "saved", "saved_v")
	checkInvertedIndex(t, ctx, tc2.Nodes[0].DB(), "saved", "saved_body")
	for _, q := range queries {
		src := sameBothWays(t, ctx, s, q)
		dst := sameBothWays(t, ctx, s2, q)
		if len(src) == 0 || fmt.Sprint(src) != fmt.Sprint(dst) {
			t.Fatalf("%s: source %v, restored %v", q, src, dst)
		}
	}
	// The values themselves, weights and stored queries included.
	const dump = `SELECT id, body, v, q FROM saved ORDER BY id`
	requireSameRows(t, "the restored rows", rowsText(t, ctx, s2, dump), rowsText(t, ctx, s, dump))
	put(s2, 200, "rewritten again")
	if got := sameBothWays(t, ctx, s2, queries[0]); len(got) != 11 {
		t.Fatalf("after a write to the restored table: %v", got)
	}

	// One table, into the running cluster under a new name.
	if _, err := tc.Nodes[1].RunRestoreOpts(ctx, server.RestoreOptions{
		Srcs: []string{dirFull, dirInc}, Tables: []server.RestoreTable{{Name: "saved", Into: "saved_back"}},
	}); err != nil {
		t.Fatalf("table restore: %v", err)
	}
	rs := sql.NewSession(tc.Nodes[2].DB(), catalog.NewAccessor())
	checkInvertedIndex(t, ctx, tc.Nodes[2].DB(), "saved_back", "saved_v")
	checkInvertedIndex(t, ctx, tc.Nodes[2].DB(), "saved_back", "saved_body")
	for _, q := range queries {
		src := sameBothWays(t, ctx, s, q)
		dst := sameBothWays(t, ctx, rs, strings.Replace(q, "FROM saved", "FROM saved_back", 1))
		if fmt.Sprint(src) != fmt.Sprint(dst) {
			t.Fatalf("%s: source %v, restored table %v", q, src, dst)
		}
	}
	requireSameRows(t, "the restored table's rows", rowsText(t, ctx, rs, strings.Replace(dump, "FROM saved", "FROM saved_back", 1)), rowsText(t, ctx, s, dump))
	// Both indexes are maintained on the restored table, and keep their
	// definitions.
	execSQL(t, ctx, rs, `INSERT INTO saved_back VALUES (300, 'rewritten once more', to_tsvector('english', 'rewritten once more'), NULL)`)
	execSQL(t, ctx, rs, `DELETE FROM saved_back WHERE id = 0`)
	checkInvertedIndex(t, ctx, tc.Nodes[2].DB(), "saved_back", "saved_v")
	checkInvertedIndex(t, ctx, tc.Nodes[2].DB(), "saved_back", "saved_body")
	for _, lhs := range []string{"v", "to_tsvector('english', body)"} {
		if got := sameBothWays(t, ctx, rs, `SELECT id FROM saved_back WHERE `+lhs+` @@ to_tsquery('english', 'rewritten')`); len(got) != 10 {
			t.Fatalf("after writes to the restored table, %s: %v", lhs, got)
		}
	}
	def := strings.Join(rowsText(t, ctx, rs, `SELECT indexdef FROM pg_indexes WHERE tablename = 'saved_back' ORDER BY indexname`), "\n")
	if !strings.Contains(def, "USING gin (to_tsvector('english'::regconfig, body))") || !strings.Contains(def, "USING gin (v)") {
		t.Fatalf("the restored table's indexes:\n%s", def)
	}
}

// TestTextSearchNeedsV21: until the cluster has finalized v21 a node may
// be running a binary that cannot decode a tsvector, so no table gets a
// column of either type; the functions and the operator, which store
// nothing, work.
func TestTextSearchNeedsV21(t *testing.T) {
	tc := StartWithOptions(t, 1, func(c *server.Config) { c.BinaryVersionOverride = version.V20 })
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	s := sql.NewSession(tc.Nodes[0].DB(), catalog.NewAccessor())
	waitForDatabases(t, ctx, s)
	execSQL(t, ctx, s, `CREATE TABLE old (id INT8 PRIMARY KEY, body TEXT)`)
	for _, q := range []string{
		`CREATE TABLE old2 (id INT8 PRIMARY KEY, v TSVECTOR)`,
		`CREATE TABLE old2 (id INT8 PRIMARY KEY, q TSQUERY)`,
		`ALTER TABLE old ADD COLUMN v TSVECTOR`,
		`ALTER TABLE old ALTER COLUMN body TYPE TSVECTOR`,
		`CREATE TABLE old2 AS SELECT id, to_tsvector('english', body) AS v FROM old`,
		`CREATE INDEX old_body ON old USING GIN (to_tsvector('english', body))`,
	} {
		_, serr := trySQL(ctx, s, q)
		if serr == nil || serr.Code != sql.CodeFeatureNotSupported || !strings.Contains(serr.Msg, "v21") {
			t.Fatalf("%s at v20: %v", q, serr)
		}
	}
	execSQL(t, ctx, s, `INSERT INTO old VALUES (1, 'fat cats'), (2, 'thin dogs')`)
	if got := idsOf(t, ctx, s, `SELECT id FROM old WHERE to_tsvector('english', body) @@ to_tsquery('english', 'cat')`); fmt.Sprint(got) != "[1]" {
		t.Fatalf("search as a filter at v20: %v", got)
	}
}

// TestTextSearchOverPgwire: a stock pgx client reads and writes both
// types, as text on the simple protocol and in PostgreSQL's binary
// formats on the extended one, and runs an indexed search with a
// parameter.
func TestTextSearchOverPgwire(t *testing.T) {
	tc := Start(t, 1)
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()
	conn, err := pgx.Connect(ctx, pgURL(tc, 0))
	if err != nil {
		t.Fatalf("pgx connect: %v", err)
	}
	defer func() { _ = conn.Close(ctx) }()
	exec := func(q string, args ...any) {
		t.Helper()
		if _, err := conn.Exec(ctx, q, args...); err != nil {
			t.Fatalf("%s: %v", q, err)
		}
	}
	exec(`CREATE TABLE notes (id INT8 PRIMARY KEY, body TEXT, v TSVECTOR, q TSQUERY)`)
	exec(`CREATE INDEX notes_v ON notes USING GIN (v)`)
	exec(`INSERT INTO notes VALUES (1, 'A fat cat', setweight(to_tsvector('english', 'A fat cat'), 'A'), to_tsquery('english', 'fat & !rats:*'))`)
	// Parameters: text for the functions, and a tsvector sent as the
	// driver encodes one.
	exec(`INSERT INTO notes VALUES ($1, $2, to_tsvector('english', $2), plainto_tsquery('english', $3))`, int64(2), "Two thin dogs", "thin dog")
	var sent pgtype.TSVector
	if err := sent.Scan("'bird':1A 'cage':2,5 'two words'"); err != nil {
		t.Fatal(err)
	}
	exec(`INSERT INTO notes (id, v) VALUES ($1, $2)`, int64(3), sent)

	// The extended protocol: the column types are described by their
	// PostgreSQL OIDs, and the driver decodes the binary tsvector.
	rows, err := conn.Query(ctx, `SELECT id, v, q::text FROM notes ORDER BY id`)
	if err != nil {
		t.Fatal(err)
	}
	fds := rows.FieldDescriptions()
	if fds[1].DataTypeOID != pgtype.TSVectorOID {
		t.Fatalf("tsvector column described as OID %d", fds[1].DataTypeOID)
	}
	var got []string
	for rows.Next() {
		var id int64
		var v pgtype.TSVector
		var q *string
		if err := rows.Scan(&id, &v, &q); err != nil {
			t.Fatal(err)
		}
		text, err := v.Value()
		if err != nil {
			t.Fatal(err)
		}
		qs := "NULL"
		if q != nil {
			qs = *q
		}
		got = append(got, fmt.Sprintf("%d|%v|%s", id, text, qs))
	}
	rows.Close()
	want := []string{
		`1|'cat':3A 'fat':2A|'fat' & !'rat':*`,
		`2|'dog':3 'thin':2 'two':1|'thin' & 'dog'`,
		`3|'bird':1A 'cage':2,5 'two words'|NULL`,
	}
	if fmt.Sprint(got) != fmt.Sprint(want) {
		t.Fatalf("binary results:\n got %q\nwant %q", got, want)
	}
	// A tsquery column on the extended protocol: OID 3615, binary.
	rows, err = conn.Query(ctx, `SELECT q FROM notes WHERE id = 1`, pgx.QueryResultFormats{pgx.BinaryFormatCode})
	if err != nil {
		t.Fatal(err)
	}
	if oid := rows.FieldDescriptions()[0].DataTypeOID; oid != 3615 {
		t.Fatalf("tsquery column described as OID %d", oid)
	}
	if !rows.Next() {
		t.Fatal("no row")
	}
	raw := append([]byte(nil), rows.RawValues()[0]...)
	rows.Close()
	back, err := tsearch.ParseQueryBinary(raw)
	if err != nil || back.String() != `'fat' & !'rat':*` {
		t.Fatalf("binary tsquery: %s (%v)", back, err)
	}

	// The simple protocol: text.
	var vText, qText string
	if err := conn.QueryRow(ctx, `SELECT v, q FROM notes WHERE id = 1`, pgx.QueryExecModeSimpleProtocol).Scan(&vText, &qText); err != nil {
		t.Fatal(err)
	}
	if vText != `'cat':3A 'fat':2A` || qText != `'fat' & !'rat':*` {
		t.Fatalf("text results: %s | %s", vText, qText)
	}

	// An indexed search with the query text as a parameter, ranked.
	var id int64
	var rank float64
	if err := conn.QueryRow(ctx, `SELECT id, ts_rank(v, to_tsquery('english', $1)) AS r FROM notes WHERE v @@ to_tsquery('english', $1) ORDER BY r DESC LIMIT 1`, "cats | dogs").Scan(&id, &rank); err != nil {
		t.Fatal(err)
	}
	if id != 1 || rank <= 0 {
		t.Fatalf("ranked search: id %d, rank %v", id, rank)
	}
	// A tsquery parameter: described as a tsquery, sent as its text.
	if err := conn.QueryRow(ctx, `SELECT id FROM notes WHERE v @@ $1`, "bird & cage").Scan(&id); err != nil || id != 3 {
		t.Fatalf("a tsquery parameter: id %d, %v", id, err)
	}
	var plan string
	if err := conn.QueryRow(ctx, `EXPLAIN SELECT id FROM notes WHERE v @@ to_tsquery('english', 'cats | dogs')`).Scan(&plan); err != nil {
		t.Fatal(err)
	}
	if !strings.HasPrefix(plan, `inverted scan of index "notes_v" ("cat" OR "dog")`) {
		t.Fatalf("plan: %s", plan)
	}
}

// BenchmarkTextSearch compares searches through a GIN index with the
// same searches as filters, over 50,000 documents of about 40 words:
//
//	go test ./pkg/testutils/testcluster -run - -bench TextSearch -benchtime 20x
func BenchmarkTextSearch(b *testing.B) {
	const docs = 50000
	tc := Start(b, 1)
	ctx := context.Background()
	s := sql.NewSession(tc.Nodes[0].DB(), catalog.NewAccessor())
	exec := func(q string) *sql.Result {
		b.Helper()
		res, serr := trySQL(ctx, s, q)
		if serr != nil {
			b.Fatalf("%.200s: %v", q, serr)
		}
		return res
	}
	exec(`CREATE TABLE bench_text (id INT8 PRIMARY KEY, body TEXT)`)
	start := time.Now()
	for lo := 0; lo < docs; lo += 250 {
		var vals []string
		for i := lo; i < lo+250; i++ {
			vals = append(vals, fmt.Sprintf(`(%d, '%s')`, i, BenchDocument(i)))
		}
		exec(`INSERT INTO bench_text VALUES ` + strings.Join(vals, ", "))
	}
	b.Logf("load of %d documents: %s", docs, time.Since(start).Round(time.Millisecond))
	start = time.Now()
	exec(`CREATE INDEX bench_text_gin ON bench_text USING GIN (to_tsvector('english', body))`)
	b.Logf("backfill of %d documents: %s", docs, time.Since(start).Round(time.Millisecond))

	for _, c := range []struct {
		name, query string
		first       int // the query's number runs over first..first+99
	}{
		{"rare-word", `'topic%d'`, 100},           // 50 rows of 50,000
		{"and", `'topic%d & common'`, 100},        // a rare word and one every row has
		{"phrase", `'quarterly <-> report%d'`, 0}, // two words side by side, 500 rows
		{"prefix", `'serial%d:*'`, 100},           // 111 lexemes, 111 rows
	} {
		for _, mode := range []string{"on", "off"} {
			name := c.name + "/indexed"
			if mode == "off" {
				name = c.name + "/filtered"
			}
			b.Run(name, func(b *testing.B) {
				exec(`SET inverted_index_scan = ` + mode)
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					q := fmt.Sprintf(c.query, c.first+i%100)
					res := exec(`SELECT id FROM bench_text WHERE to_tsvector('english', body) @@ to_tsquery('english', ` + q + `)`)
					if len(res.Rows) == 0 {
						b.Fatalf("%s matched nothing", q)
					}
				}
			})
		}
	}
	b.Run("ranked-top10", func(b *testing.B) {
		exec(`SET inverted_index_scan = on`)
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			q := fmt.Sprintf(`to_tsquery('english', 'report%d | topic%d')`, i%100, i%1000)
			res := exec(`SELECT id, ts_rank(to_tsvector('english', body), ` + q + `) AS r FROM bench_text
				WHERE to_tsvector('english', body) @@ ` + q + ` ORDER BY r DESC LIMIT 10`)
			if len(res.Rows) != 10 {
				b.Fatalf("top 10 returned %d rows", len(res.Rows))
			}
		}
	})
}

// BenchDocument is document i of the benchmark corpus: about 40 words,
// of which "topic<i mod 1000>" selects 1 in 1000 documents,
// "report<i mod 100>" 1 in 100, "serial<i>" one, and "common" all.
func BenchDocument(i int) string {
	filler := []string{"the", "quarterly", "figures", "were", "reviewed", "by", "committee", "and", "found", "consistent",
		"with", "earlier", "estimates", "although", "several", "regions", "reported", "delays", "in", "shipping"}
	var b strings.Builder
	fmt.Fprintf(&b, "common topic%d serial%d quarterly report%d", i%1000, i, i%100)
	for j := 0; j < 30; j++ {
		b.WriteByte(' ')
		b.WriteString(filler[(i*7+j*3)%len(filler)])
	}
	return b.String()
}
