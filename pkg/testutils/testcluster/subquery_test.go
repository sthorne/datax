package testcluster

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

	"github.com/sthorne/datax/pkg/sql"
	"github.com/sthorne/datax/pkg/sql/catalog"
)

// TestSubqueriesOverPgwire: uncorrelated scalar, IN, EXISTS, and
// FROM-subquery forms through a stock pgx client, matching PostgreSQL
// semantics (zero-row scalar = NULL, NOT IN over a NULL-bearing set is
// never true, derived tables filter on aggregate outputs).
func TestSubqueriesOverPgwire(t *testing.T) {
	tc := Start(t, 3)
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	conn, err := pgx.Connect(ctx, pgURL(tc, 0))
	if err != nil {
		t.Fatalf("pgx connect: %v", err)
	}
	defer func() { _ = conn.Close(ctx) }()
	mustExec := func(q string) {
		t.Helper()
		if _, err := conn.Exec(ctx, q); err != nil {
			t.Fatalf("%s: %v", q, err)
		}
	}

	mustExec(`CREATE TABLE depts (id INT8 PRIMARY KEY, region TEXT)`)
	mustExec(`CREATE TABLE emp (id INT8 PRIMARY KEY, dept_id INT8, salary INT8, name TEXT)`)
	mustExec(`INSERT INTO depts VALUES (1, 'north'), (2, 'west'), (3, 'west'), (4, 'east')`)
	mustExec(`INSERT INTO emp VALUES
		(1, 1, 100, 'ann'), (2, 1, 200, 'bob'), (3, 2, 300, 'cat'),
		(4, 3, 150, 'dan'), (5, NULL, 50, 'eve')`)

	names := func(q string, args ...any) []string {
		t.Helper()
		rows, err := conn.Query(ctx, q, args...)
		if err != nil {
			t.Fatalf("%s: %v", q, err)
		}
		defer rows.Close()
		var out []string
		for rows.Next() {
			var s string
			if err := rows.Scan(&s); err != nil {
				t.Fatalf("scan: %v", err)
			}
			out = append(out, s)
		}
		if rows.Err() != nil {
			t.Fatalf("%s: %v", q, rows.Err())
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

	// Scalar subquery in WHERE — spliced before planning.
	eq(names(`SELECT name FROM emp WHERE salary = (SELECT MAX(salary) FROM emp)`), []string{"cat"})

	// Zero-row scalar subquery is NULL: comparison never matches.
	eq(names(`SELECT name FROM emp WHERE salary = (SELECT salary FROM emp WHERE id = 999)`), nil)

	// Scalar subquery in the select list (FROM-less outer).
	var cnt int64
	if err := conn.QueryRow(ctx, `SELECT (SELECT COUNT(*) FROM emp)`).Scan(&cnt); err != nil || cnt != 5 {
		t.Fatalf("select-list scalar: %v (%d)", err, cnt)
	}

	// IN over a subquery.
	eq(names(`SELECT name FROM emp WHERE dept_id IN (SELECT id FROM depts WHERE region = 'west') ORDER BY name`),
		[]string{"cat", "dan"})

	// NOT IN with a NULL in the inner set is never true (PostgreSQL
	// three-valued logic) — even dept 4, which no employee references.
	eq(names(`SELECT region FROM depts WHERE id NOT IN (SELECT dept_id FROM emp)`), nil)
	// Filtering the NULLs restores the expected row.
	eq(names(`SELECT region FROM depts WHERE id NOT IN (SELECT dept_id FROM emp WHERE dept_id IS NOT NULL)`),
		[]string{"east"})

	// Literal IN lists.
	eq(names(`SELECT name FROM emp WHERE id IN (1, 3) ORDER BY name`), []string{"ann", "cat"})
	eq(names(`SELECT name FROM emp WHERE id NOT IN (1, 2, 3, 5) ORDER BY name`), []string{"dan"})

	// EXISTS / NOT EXISTS (uncorrelated).
	eq(names(`SELECT name FROM emp WHERE EXISTS (SELECT 1 FROM depts WHERE region = 'east') AND id = 1`),
		[]string{"ann"})
	eq(names(`SELECT name FROM emp WHERE NOT EXISTS (SELECT 1 FROM depts WHERE region = 'south') AND id = 1`),
		[]string{"ann"})
	eq(names(`SELECT name FROM emp WHERE EXISTS (SELECT 1 FROM depts WHERE region = 'south')`), nil)

	// Derived table: filter on an aggregate output, order, project.
	rows, err := conn.Query(ctx, `SELECT d, total FROM
		(SELECT dept_id AS d, SUM(salary) AS total FROM emp WHERE dept_id IS NOT NULL GROUP BY dept_id) sums
		WHERE total >= 300 ORDER BY total DESC, d`)
	if err != nil {
		t.Fatalf("derived: %v", err)
	}
	var ds, totals []int64
	for rows.Next() {
		var d, tot int64
		if err := rows.Scan(&d, &tot); err != nil {
			t.Fatalf("scan: %v", err)
		}
		ds, totals = append(ds, d), append(totals, tot)
	}
	rows.Close()
	if len(ds) != 2 || ds[0] != 1 || ds[1] != 2 || totals[0] != 300 || totals[1] != 300 {
		t.Fatalf("derived rows: %v %v", ds, totals)
	}

	// Aggregate over a derived table; qualified alias references.
	if err := conn.QueryRow(ctx, `SELECT COUNT(*) FROM (SELECT DISTINCT dept_id FROM emp WHERE dept_id IS NOT NULL) t`).Scan(&cnt); err != nil || cnt != 3 {
		t.Fatalf("count over derived: %v (%d)", err, cnt)
	}
	eq(names(`SELECT t.name FROM (SELECT name, salary FROM emp) t WHERE t.salary > 250`), []string{"cat"})

	// Subqueries in DML.
	mustExec(`UPDATE emp SET salary = (SELECT MAX(salary) FROM emp) WHERE id = 5`)
	eq(names(`SELECT name FROM emp WHERE salary = 300 AND id = 5`), []string{"eve"})
	mustExec(`INSERT INTO depts VALUES (99, (SELECT region FROM depts WHERE id = 2))`)
	eq(names(`SELECT region FROM depts WHERE id = 99`), []string{"west"})
	mustExec(`DELETE FROM depts WHERE id IN (SELECT id FROM depts WHERE id = 99)`)
	eq(names(`SELECT region FROM depts WHERE id = 99`), nil)

	// Errors: multi-row scalar (21000), multi-column subquery (42601),
	// correlated reference (0A000), missing FROM alias (42601).
	var pgErr *pgconn.PgError
	if _, err := conn.Exec(ctx, `SELECT name FROM emp WHERE salary = (SELECT salary FROM emp)`); err == nil {
		t.Fatal("multi-row scalar accepted")
	} else if !errors.As(err, &pgErr) || pgErr.Code != "21000" {
		t.Fatalf("multi-row scalar error: %v", err)
	}
	if _, err := conn.Exec(ctx, `SELECT name FROM emp WHERE id IN (SELECT id, dept_id FROM emp)`); err == nil {
		t.Fatal("multi-column IN subquery accepted")
	} else if !errors.As(err, &pgErr) || pgErr.Code != "42601" {
		t.Fatalf("multi-column IN error: %v", err)
	}
	// Correlated EXISTS works now (full matrix in TestCorrelatedSubqueries):
	// 'region' resolves in the OUTER scope, so each 'west' dept probes the
	// non-empty emp table.
	eq(names(`SELECT region FROM depts WHERE EXISTS (SELECT 1 FROM emp WHERE region = 'west') ORDER BY id`),
		[]string{"west", "west"})
	if _, err := conn.Exec(ctx, `SELECT * FROM (SELECT id FROM emp)`); err == nil {
		t.Fatal("FROM subquery without alias accepted")
	} else if !errors.As(err, &pgErr) || pgErr.Code != "42601" {
		t.Fatalf("missing alias error: %v", err)
	}
}

// TestSubquerySession: spliced scalars reach the planner (EXPLAIN shows a
// point lookup), derived-table EXPLAIN, and prepared statements re-evaluate
// subqueries per execution.
func TestSubquerySession(t *testing.T) {
	tc := Start(t, 3)
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()
	s := sql.NewSession(tc.Nodes[0].DB(), catalog.NewAccessor())

	execSQL(t, ctx, s, `CREATE TABLE kv (k INT PRIMARY KEY, v INT)`)
	execSQL(t, ctx, s, `INSERT INTO kv VALUES (1, 10), (2, 20), (3, 30)`)

	// The spliced scalar is a literal by planning time: full point lookup.
	if p := explainPlan(t, ctx, s, `SELECT v FROM kv WHERE k = (SELECT MAX(k) FROM kv)`); p != "point lookup on primary key" {
		t.Fatalf("plan: %s", p)
	}
	// And a spliced range bound.
	if p := explainPlan(t, ctx, s, `SELECT v FROM kv WHERE k > (SELECT MIN(k) FROM kv)`); p != "range scan of primary key (k > 1)" {
		t.Fatalf("plan: %s", p)
	}
	if p := explainPlan(t, ctx, s, `SELECT x FROM (SELECT k AS x FROM kv) t`); p != `materialized subquery (derived table "t")` {
		t.Fatalf("plan: %s", p)
	}

	// The same statement re-evaluates its subquery every execution (the
	// AST is never mutated by splicing).
	q := `SELECT v FROM kv WHERE k = (SELECT MAX(k) FROM kv)`
	res := execSQL(t, ctx, s, q)
	if len(res.Rows) != 1 || res.Rows[0][0].I != 30 {
		t.Fatalf("rows: %+v", res.Rows)
	}
	execSQL(t, ctx, s, `INSERT INTO kv VALUES (9, 90)`)
	res = execSQL(t, ctx, s, q)
	if len(res.Rows) != 1 || res.Rows[0][0].I != 90 {
		t.Fatalf("re-executed rows: %+v", res.Rows)
	}

	// IN + parallel range predicate still plans the bounded scan (IN stays
	// residual).
	res = execSQL(t, ctx, s, `SELECT v FROM kv WHERE k > 1 AND k IN (SELECT k FROM kv WHERE v >= 30) ORDER BY v`)
	if len(res.Rows) != 2 || res.Rows[0][0].I != 30 || res.Rows[1][0].I != 90 {
		t.Fatalf("in+range rows: %+v", res.Rows)
	}
}
