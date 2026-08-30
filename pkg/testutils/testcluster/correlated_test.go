package testcluster

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/sthorne/datax/pkg/sql"
	"github.com/sthorne/datax/pkg/sql/catalog"
	"github.com/sthorne/datax/pkg/sql/types"
)

// TestCorrelatedSubqueries: the v1 correlated matrix — EXISTS/NOT EXISTS,
// correlated scalar comparisons and IN in WHERE, UPDATE/DELETE, NULL
// outer values, parameters, LIMIT, EXPLAIN's nested-loop note — plus the
// error contract: genuine typos are 42703 (not the old blanket 0A000),
// and uncovered shapes keep a clear 0A000.
func TestCorrelatedSubqueries(t *testing.T) {
	n, _ := startGCNode(t)
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()
	s := sql.NewSession(n.DB(), catalog.NewAccessor())

	execSQL(t, ctx, s, `CREATE TABLE depts (id INT8 PRIMARY KEY, region TEXT)`)
	execSQL(t, ctx, s, `CREATE TABLE emp (id INT8 PRIMARY KEY, dept_id INT8, salary INT8, name TEXT)`)
	execSQL(t, ctx, s, `INSERT INTO depts VALUES (1, 'north'), (2, 'west'), (3, 'west'), (4, 'east')`)
	execSQL(t, ctx, s, `INSERT INTO emp VALUES
		(1, 1, 100, 'ann'), (2, 1, 200, 'bob'), (3, 2, 300, 'cat'),
		(4, 3, 150, 'dan'), (5, NULL, 50, 'eve')`)

	col := func(q string, params ...types.Datum) []string {
		t.Helper()
		res := execSQL(t, ctx, s, q, params...)
		out := make([]string, 0, len(res.Rows))
		for _, r := range res.Rows {
			out = append(out, r[0].Text())
		}
		return out
	}
	eq := func(got, want []string) {
		t.Helper()
		if len(got) != len(want) {
			t.Fatalf("got %v, want %v", got, want)
		}
		for i := range want {
			if got[i] != want[i] {
				t.Fatalf("got %v, want %v", got, want)
			}
		}
	}

	// Correlated EXISTS: depts that have employees (eve's NULL dept_id
	// matches nothing).
	eq(col(`SELECT region FROM depts d WHERE EXISTS (SELECT 1 FROM emp WHERE dept_id = d.id) ORDER BY id`),
		[]string{"north", "west", "west"})
	// ... and NOT EXISTS: the empty dept.
	eq(col(`SELECT region FROM depts d WHERE NOT EXISTS (SELECT 1 FROM emp WHERE dept_id = d.id)`),
		[]string{"east"})

	// The classic correlated scalar: each department's top earner. The
	// alias disambiguates emp-vs-emp; bare dept_id resolves inner (inner
	// shadows outer); eve's NULL dept yields a NULL max that matches
	// nothing.
	eq(col(`SELECT name FROM emp e WHERE salary = (SELECT MAX(salary) FROM emp WHERE dept_id = e.dept_id) ORDER BY id`),
		[]string{"bob", "cat", "dan"})

	// Correlated scalar with an inequality.
	eq(col(`SELECT name FROM emp e WHERE salary < (SELECT MAX(salary) FROM emp WHERE dept_id = e.dept_id) ORDER BY id`),
		[]string{"ann"})

	// Correlated IN.
	eq(col(`SELECT name FROM emp e WHERE dept_id IN (SELECT id FROM depts WHERE region = 'west' AND id = e.dept_id) ORDER BY id`),
		[]string{"cat", "dan"})

	// Parameters flow through, and prepared-style re-execution sees fresh
	// bindings.
	eq(col(`SELECT region FROM depts WHERE EXISTS (SELECT 1 FROM emp WHERE dept_id = depts.id AND salary > $1) ORDER BY id`,
		types.NewInt(100)), []string{"north", "west", "west"})
	eq(col(`SELECT region FROM depts WHERE EXISTS (SELECT 1 FROM emp WHERE dept_id = depts.id AND salary > $1) ORDER BY id`,
		types.NewInt(250)), []string{"west"})

	// LIMIT re-applies after the correlated filter.
	if got := col(`SELECT region FROM depts WHERE EXISTS (SELECT 1 FROM emp WHERE dept_id = depts.id) LIMIT 2`); len(got) != 2 {
		t.Fatalf("limit over correlated filter: %v", got)
	}

	// EXPLAIN states the nested loop.
	if p := explainPlan(t, ctx, s, `SELECT region FROM depts d WHERE EXISTS (SELECT 1 FROM emp WHERE dept_id = d.id)`); !strings.Contains(p, "correlated filter: nested loop") {
		t.Fatalf("explain: %q", p)
	}

	// UPDATE and DELETE with correlated WHERE (the outer qualifier is the
	// table name).
	execSQL(t, ctx, s, `UPDATE depts SET region = 'empty' WHERE NOT EXISTS (SELECT 1 FROM emp WHERE dept_id = depts.id)`)
	eq(col(`SELECT region FROM depts WHERE id = 4`), []string{"empty"})
	res := execSQL(t, ctx, s, `DELETE FROM emp WHERE NOT EXISTS (SELECT 1 FROM depts WHERE id = emp.dept_id)`)
	if res.Tag != "DELETE 1" {
		t.Fatalf("delete tag: %s", res.Tag) // exactly eve (NULL dept_id)
	}

	// Error contract.
	if _, serr := trySQL(ctx, s, `SELECT id FROM depts WHERE EXISTS (SELECT 1 FROM emp WHERE nosuch = 5)`); serr == nil || serr.Code != sql.CodeUndefinedColumn {
		t.Fatalf("typo inside subquery: %+v", serr)
	}
	if _, serr := trySQL(ctx, s, `SELECT id FROM depts WHERE EXISTS (SELECT 1 FROM emp WHERE bogus.x = 5)`); serr == nil || serr.Code != sql.CodeUndefinedTable {
		t.Fatalf("unknown qualifier: %+v", serr)
	}
	// Depth-2 correlation: the innermost reference to the outermost scope
	// is out of reach (one correlation level), reported as a missing FROM
	// entry with the scoping rule spelled out.
	if _, serr := trySQL(ctx, s, `SELECT id FROM depts WHERE EXISTS (SELECT 1 FROM emp WHERE EXISTS (SELECT 1 FROM emp WHERE dept_id = depts.id))`); serr == nil ||
		serr.Code != sql.CodeUndefinedTable || !strings.Contains(serr.Msg, "immediately enclosing") {
		t.Fatalf("two-level correlation: %+v", serr)
	}
	// A bare outer column in a position that cannot carry correlation
	// (GROUP BY) is rejected up front.
	if _, serr := trySQL(ctx, s, `SELECT id FROM depts d WHERE EXISTS (SELECT 1 FROM emp GROUP BY region)`); serr == nil || serr.Code != sql.CodeFeatureNotSupported {
		t.Fatalf("correlated GROUP BY: %+v", serr)
	}
}
