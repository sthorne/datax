package testcluster

import (
	"context"
	"fmt"
	"slices"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
)

// TestTextSearchPreparedFold: a tsquery made from a parameter is made
// once per execution, never carried over from an earlier one. A
// prepared statement `... @@ to_tsquery('english', $1)` run again on
// the same connection with another $1 answers for the new value, for
// each of the four tsquery constructors, filtering a whole table and
// through a GIN index (where the value also picks the index lookups).
//
// Why it cannot be carried over: the call is folded by the resolve
// pass, which works on a copy of the statement's WHERE, and a statement
// the pass rewrote is never looked up in the plan cache (execSelectBody
// consults it only while the statement is the one prepared, t == orig),
// whose entries hold no WHERE anyway — the descriptor, statistics,
// projection and point-lookup shape. So these executions miss the
// cache, every one, as they did before the fold (the tsquery functions
// were already resolved, for the default configuration). The test
// asserts that, so that a change letting such a statement into the
// cache has to face this test. The parameterized tsquery that does run
// from the cache — `v @@ $1` on a tsvector column, nothing to resolve —
// is checked too: it hits, and answers for each value.
func TestTextSearchPreparedFold(t *testing.T) {
	tc := Start(t, 1)
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()
	// pgx's default mode: each statement text prepared once on the
	// connection, then Bind/Execute with the new parameters.
	conn, err := pgx.Connect(ctx, pgURL(tc, 0))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = conn.Close(ctx) }()
	exec := func(q string) {
		t.Helper()
		if _, err := conn.Exec(ctx, q); err != nil {
			t.Fatalf("%s: %v", q, err)
		}
	}
	for _, table := range []string{"docs", "docs_gin"} {
		exec(`CREATE TABLE ` + table + ` (id INT8 PRIMARY KEY, body TEXT, v TSVECTOR)`)
		exec(`INSERT INTO ` + table + ` (id, body) VALUES (1, 'apple pie recipe'), (2, 'banana split dessert'),
			(3, 'apple and banana smoothie'), (4, 'cherry tart')`)
		exec(`UPDATE ` + table + ` SET v = to_tsvector('english', body)`)
	}
	exec(`CREATE INDEX docs_gin_body ON docs_gin USING GIN (to_tsvector('english', body))`)
	exec(`CREATE INDEX docs_gin_v ON docs_gin USING GIN (v)`)

	ids := func(q string, arg string) []int64 {
		t.Helper()
		rows, err := conn.Query(ctx, q, arg)
		if err != nil {
			t.Fatalf("%s [%q]: %v", q, arg, err)
		}
		got, err := pgx.CollectRows(rows, pgx.RowTo[int64])
		if err != nil {
			t.Fatalf("%s [%q]: %v", q, arg, err)
		}
		slices.Sort(got)
		return got
	}
	type run struct {
		arg  string
		want []int64
	}
	for _, c := range []struct {
		fn   string
		runs []run
	}{
		{"to_tsquery", []run{{"apple", []int64{1, 3}}, {"banana", []int64{2, 3}}, {"cherry", []int64{4}}, {"apple", []int64{1, 3}}}},
		{"plainto_tsquery", []run{{"apple pie", []int64{1}}, {"banana smoothie", []int64{3}}, {"apple pie", []int64{1}}}},
		{"phraseto_tsquery", []run{{"apple pie", []int64{1}}, {"banana split", []int64{2}}, {"apple pie", []int64{1}}}},
		{"websearch_to_tsquery", []run{{"apple -banana", []int64{1}}, {"banana -apple", []int64{2}}, {"apple -banana", []int64{1}}}},
	} {
		for _, table := range []string{"docs", "docs_gin"} {
			q := fmt.Sprintf(`SELECT id FROM %s WHERE to_tsvector('english', body) @@ %s('english', $1)`, table, c.fn)
			for i, r := range c.runs {
				h0, _ := planCacheCounts()
				got := ids(q, r.arg)
				h1, _ := planCacheCounts()
				if !slices.Equal(got, r.want) {
					t.Errorf("%s, execution %d with %q: ids %v, want %v", q, i+1, r.arg, got, r.want)
				}
				if h1 != h0 {
					t.Errorf("%s, execution %d: a plan cache hit for a statement the resolve pass rewrote", q, i+1)
				}
			}
		}
	}

	// A tsquery parameter matched from the plan cache: `v @@ $1` is not
	// rewritten, so its executions after the first hit the cache, and
	// the query each one parses (once per text, builtins.tsMatcher) is
	// its own. The text is a tsquery literal, taken as written (no
	// stemming), so it names the stems: appl, cherri.
	for _, table := range []string{"docs", "docs_gin"} {
		q := `SELECT id FROM ` + table + ` WHERE v @@ $1`
		for i, r := range []run{{"appl", []int64{1, 3}}, {"banana & !appl", []int64{2}}, {"cherri | split", []int64{2, 4}}, {"appl", []int64{1, 3}}} {
			h0, _ := planCacheCounts()
			got := ids(q, r.arg)
			h1, _ := planCacheCounts()
			if !slices.Equal(got, r.want) {
				t.Errorf("%s, execution %d with %q: ids %v, want %v", q, i+1, r.arg, got, r.want)
			}
			if i > 0 && h1-h0 < 1 {
				t.Errorf("%s, execution %d: no plan cache hit", q, i+1)
			}
		}
	}
}
