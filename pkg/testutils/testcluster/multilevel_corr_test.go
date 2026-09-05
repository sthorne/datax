package testcluster

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/sthorne/datax/pkg/sql"
	"github.com/sthorne/datax/pkg/sql/catalog"
)

// TestMultiLevelCorrelated: correlated subqueries reaching through more
// than one enclosing scope — the innermost query referencing the middle
// level, the outermost, or both — with shadowing, NULL bindings, and the
// depth cap.
func TestMultiLevelCorrelated(t *testing.T) {
	n, _ := startGCNode(t)
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()
	s := sql.NewSession(n.DB(), catalog.NewAccessor())

	// regions ⇐ depts ⇐ emp, small on purpose (the loop is O(product)).
	execSQL(t, ctx, s, `CREATE TABLE regions (id INT8 PRIMARY KEY, zone TEXT, min_pay INT8)`)
	execSQL(t, ctx, s, `CREATE TABLE depts (id INT8 PRIMARY KEY, region_id INT8, name TEXT)`)
	execSQL(t, ctx, s, `CREATE TABLE emp (id INT8 PRIMARY KEY, dept_id INT8, salary INT8)`)
	execSQL(t, ctx, s, `INSERT INTO regions VALUES (1, 'w', 100), (2, 'e', 400), (3, 'n', 50)`)
	execSQL(t, ctx, s, `INSERT INTO depts VALUES (10, 1, 'eng'), (11, 1, 'ops'), (12, 2, 'sales'), (13, NULL, 'lab')`)
	execSQL(t, ctx, s, `INSERT INTO emp VALUES
		(100, 10, 150), (101, 10, 90), (102, 11, 120), (103, 12, 300), (104, NULL, 500)`)

	col := func(q string) []string {
		t.Helper()
		res := execSQL(t, ctx, s, q)
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

	// Innermost references BOTH enclosing levels: regions that have a
	// dept with an employee paid at least the REGION's min_pay.
	// west (min 100): eng has 150, ops has 120 → yes. east (min 400):
	// sales' best is 300 → no. north: no depts → no.
	eq(col(`SELECT zone FROM regions r WHERE EXISTS (
			SELECT 1 FROM depts d WHERE d.region_id = r.id AND EXISTS (
				SELECT 1 FROM emp e WHERE e.dept_id = d.id AND e.salary >= r.min_pay))
		ORDER BY zone`),
		[]string{"w"})

	// Innermost references ONLY the outermost (skips the middle level):
	// regions that have a dept, where some employee ANYWHERE is under the
	// region's min_pay. west (100): 90 qualifies → yes. east (400): 300
	// qualifies → yes. north (50): no depts → no.
	eq(col(`SELECT zone FROM regions r WHERE EXISTS (
			SELECT 1 FROM depts d WHERE d.region_id = r.id AND EXISTS (
				SELECT 1 FROM emp e WHERE e.salary < r.min_pay))
		ORDER BY zone`),
		[]string{"e", "w"})

	// NOT EXISTS at depth 2: regions with no dept employing anyone under
	// the region's min_pay. west: eng has 90 < 100 → excluded. east:
	// sales has 300 < 400 → excluded. north: no depts → vacuously true.
	eq(col(`SELECT zone FROM regions r WHERE NOT EXISTS (
			SELECT 1 FROM depts d WHERE d.region_id = r.id AND EXISTS (
				SELECT 1 FROM emp e WHERE e.dept_id = d.id AND e.salary < r.min_pay))
		ORDER BY zone`),
		[]string{"n"})

	// The middle level as the outer: depts whose own employees clear
	// their region's min_pay (correlation flows depts→regions and
	// depts→emp through the nested levels).
	eq(col(`SELECT name FROM depts d WHERE EXISTS (
			SELECT 1 FROM regions r WHERE r.id = d.region_id AND EXISTS (
				SELECT 1 FROM emp e WHERE e.dept_id = d.id AND e.salary >= r.min_pay))
		ORDER BY name`),
		[]string{"eng", "ops"})

	// Depth cap: eight levels of correlated nesting are the limit; the
	// ninth rejects with a clear error.
	nest := func(levels int) string {
		q := "SELECT id FROM regions r1 WHERE "
		for i := 2; i <= levels; i++ {
			q += fmt.Sprintf("EXISTS (SELECT 1 FROM regions r%d WHERE r%d.id = r1.id", i, i)
			if i < levels {
				q += " AND "
			}
		}
		return q + strings.Repeat(")", levels-1) + " ORDER BY id"
	}
	eq(col(nest(9)), []string{"1", "2", "3"})
	if _, serr := trySQL(ctx, s, nest(10)); serr == nil || serr.Code != sql.CodeFeatureNotSupported ||
		!strings.Contains(serr.Msg, "nest deeper") {
		t.Fatalf("depth cap: %+v", serr)
	}

	// Shadowing: the same table at two levels — bare columns bind to the
	// NEAREST scope, qualifiers reach outward. Every emp has a colleague
	// in the same dept paid less (strictly): true only for the top earner
	// per multi-employee dept... assert via count of emps for whom a
	// lower-paid same-dept emp exists: eng(150>90) → emp 100; others no.
	eq(col(`SELECT id FROM emp e1 WHERE EXISTS (
			SELECT 1 FROM emp e2 WHERE e2.dept_id = e1.dept_id AND e2.salary < e1.salary)
		ORDER BY id`),
		[]string{"100"})

	// Typos at any depth are 42703; unknown qualifiers 42P01.
	if _, serr := trySQL(ctx, s, `SELECT id FROM regions r WHERE EXISTS (
			SELECT 1 FROM depts d WHERE EXISTS (
				SELECT 1 FROM emp WHERE nosuch = r.id))`); serr == nil || serr.Code != sql.CodeUndefinedColumn {
		t.Fatalf("deep typo: %+v", serr)
	}
	if _, serr := trySQL(ctx, s, `SELECT id FROM regions r WHERE EXISTS (
			SELECT 1 FROM depts d WHERE EXISTS (
				SELECT 1 FROM emp WHERE dept_id = bogus.id))`); serr == nil || serr.Code != sql.CodeUndefinedTable {
		t.Fatalf("deep unknown qualifier: %+v", serr)
	}

	// Position gates hold at every level: an outer ref in a deep GROUP BY
	// is still rejected.
	if _, serr := trySQL(ctx, s, `SELECT id FROM regions r WHERE EXISTS (
			SELECT 1 FROM depts d WHERE EXISTS (
				SELECT COUNT(*) FROM emp GROUP BY r.zone))`); serr == nil || serr.Code != sql.CodeFeatureNotSupported {
		t.Fatalf("deep correlated GROUP BY: %+v", serr)
	}
}
