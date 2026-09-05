package testcluster

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/sthorne/datax/pkg/sql"
	"github.com/sthorne/datax/pkg/sql/catalog"
	"github.com/sthorne/datax/pkg/sql/parser"
)

// TestCTEs covers WITH: plain members (referenced as a base table, a join
// side, a subquery source, a set-operation member, an INSERT source),
// column lists, members that see earlier members, WITH RECURSIVE (an
// org chart and a counter, UNION vs UNION ALL, the caps), data-modifying
// members, WITH on INSERT / UPDATE / DELETE, WITH inside subqueries,
// shadowing of a real table, INSERT ... SELECT, joined derived tables,
// Describe through pgx, and EXPLAIN.
func TestCTEs(t *testing.T) {
	tc := Start(t, 1)
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()
	s := sql.NewSession(tc.Nodes[0].DB(), catalog.NewAccessor())
	waitForDatabases(t, ctx, s)
	texts := func(r *sql.Result) []string {
		var out []string
		for _, row := range r.Rows {
			var cells []string
			for _, d := range row {
				if d.Null {
					cells = append(cells, "NULL")
				} else {
					cells = append(cells, d.Text())
				}
			}
			out = append(out, strings.Join(cells, "|"))
		}
		return out
	}
	expect := func(q string, rows ...string) {
		t.Helper()
		r := execSQL(t, ctx, s, q)
		if strings.Join(texts(r), ";") != strings.Join(rows, ";") {
			t.Fatalf("%s: got %v, want %v", q, texts(r), rows)
		}
	}
	code := func(q, want string) {
		t.Helper()
		_, serr := trySQL(ctx, s, q)
		if serr == nil || serr.Code != want {
			t.Fatalf("%s: %v, want %s", q, serr, want)
		}
	}

	execSQL(t, ctx, s, `CREATE TABLE emp (id INT8 PRIMARY KEY, name TEXT, mgr INT8, dept TEXT)`)
	execSQL(t, ctx, s, `INSERT INTO emp VALUES (1, 'ann', NULL, 'eng'), (2, 'bob', 1, 'eng'), (3, 'cy', 2, 'eng'), (4, 'di', 1, 'ops'), (5, 'ed', 4, 'ops')`)

	expect(`WITH eng AS (SELECT id, name FROM emp WHERE dept = 'eng') SELECT count(*) FROM eng`, "3")
	expect(`WITH eng AS (SELECT id, name FROM emp WHERE dept = 'eng'), top AS (SELECT id FROM eng ORDER BY id LIMIT 1) SELECT e.name FROM eng e JOIN top t ON t.id = e.id`, "ann")
	expect(`WITH d (n, cnt) AS (SELECT dept, count(*) FROM emp GROUP BY dept) SELECT n, cnt FROM d ORDER BY cnt DESC, n`, "eng|3", "ops|2")
	expect(`WITH a AS (SELECT 1 AS v) SELECT v FROM a UNION SELECT 2 ORDER BY 1`, "1", "2")
	expect(`WITH s AS (SELECT id FROM emp WHERE dept = 'ops') SELECT name FROM emp WHERE id IN (SELECT id FROM s) ORDER BY id`, "di", "ed")
	expect(`SELECT id FROM emp WHERE id IN (WITH q AS (SELECT id FROM emp WHERE id > 3) SELECT id FROM q) ORDER BY id`, "4", "5")
	expect(`SELECT name FROM emp e WHERE EXISTS (WITH q AS (SELECT id FROM emp WHERE id > 4) SELECT 1 FROM q WHERE q.id = e.id)`, "ed")
	expect(`SELECT (WITH q AS (SELECT max(id) AS m FROM emp) SELECT m FROM q) AS top`, "5")
	expect(`WITH eng AS (SELECT id FROM emp WHERE dept = 'eng') SELECT e.name FROM emp e JOIN eng ON eng.id = e.id WHERE e.id > 1 ORDER BY e.id`, "bob", "cy")
	// A member shadows a table of its name for the statement only.
	expect(`WITH emp AS (SELECT 42 AS id) SELECT id FROM emp`, "42")
	expect(`SELECT count(*) FROM emp`, "5")

	// WITH RECURSIVE: an org chart and a counter.
	expect(`WITH RECURSIVE chain AS (SELECT id, name, mgr, 1 AS depth FROM emp WHERE id = 1 UNION ALL SELECT e.id, e.name, e.mgr, c.depth + 1 FROM emp e JOIN chain c ON e.mgr = c.id) SELECT id, name, depth FROM chain ORDER BY depth, id`,
		"1|ann|1", "2|bob|2", "4|di|2", "3|cy|3", "5|ed|3")
	expect(`WITH RECURSIVE n AS (SELECT 1 AS x UNION SELECT x + 1 FROM n WHERE x < 5) SELECT x FROM n ORDER BY x`, "1", "2", "3", "4", "5")
	expect(`WITH RECURSIVE n AS (SELECT 1 AS x UNION SELECT x FROM n) SELECT count(*) FROM n`, "1")
	code(`WITH RECURSIVE bad AS (SELECT 1 AS x UNION ALL SELECT x FROM bad) SELECT count(*) FROM bad`, sql.CodeProgramLimitExceeded)
	code(`WITH RECURSIVE bad AS (SELECT x FROM bad UNION ALL SELECT 1) SELECT 1`, sql.CodeSyntaxError)

	// Data-modifying members, and WITH on INSERT / UPDATE / DELETE.
	expect(`WITH moved AS (DELETE FROM emp WHERE id = 5 RETURNING id, name) SELECT * FROM moved`, "5|ed")
	expect(`SELECT count(*) FROM emp`, "4")
	code(`WITH x AS (DELETE FROM emp WHERE id = 99) SELECT 1`, sql.CodeFeatureNotSupported)
	execSQL(t, ctx, s, `WITH s AS (SELECT id FROM emp WHERE dept = 'ops') UPDATE emp SET dept = 'ops2' WHERE id IN (SELECT id FROM s)`)
	expect(`SELECT id FROM emp WHERE dept = 'ops2'`, "4")
	execSQL(t, ctx, s, `WITH s AS (SELECT id FROM emp WHERE dept = 'ops2') DELETE FROM emp WHERE id IN (SELECT id FROM s)`)
	expect(`SELECT count(*) FROM emp`, "3")

	// INSERT ... SELECT, plain and from a member.
	execSQL(t, ctx, s, `CREATE TABLE emp2 (id INT8 PRIMARY KEY, name TEXT)`)
	if r := execSQL(t, ctx, s, `INSERT INTO emp2 SELECT id, name FROM emp WHERE dept = 'eng'`); r.Tag != "INSERT 0 3" {
		t.Fatalf("insert select tag: %s", r.Tag)
	}
	execSQL(t, ctx, s, `WITH x AS (SELECT 100 AS id, 'hundred' AS name) INSERT INTO emp2 (id, name) SELECT id, name FROM x`)
	expect(`INSERT INTO emp2 SELECT id + 200, name FROM emp WHERE id = 1 RETURNING id`, "201")
	expect(`INSERT INTO emp2 SELECT id FROM emp WHERE false RETURNING id`)
	expect(`SELECT id FROM emp2 ORDER BY id`, "1", "2", "3", "100", "201")
	code(`INSERT INTO emp2 SELECT id, name, dept FROM emp`, sql.CodeSyntaxError)

	// A derived table joins like a table.
	expect(`SELECT d.dept, e.name FROM (SELECT dept FROM emp GROUP BY dept) AS d JOIN emp e ON e.dept = d.dept ORDER BY d.dept, e.name`, "eng|ann", "eng|bob", "eng|cy")
	expect(`SELECT d.n, e.name FROM (SELECT 9 AS n) AS d LEFT JOIN emp e ON e.id = d.n`, "9|NULL")

	// Describe and EXPLAIN.
	stmts, perr := parser.Parse(`WITH d (n, cnt) AS (SELECT dept, count(*) FROM emp GROUP BY dept) SELECT n, cnt FROM d`)
	if perr != nil {
		t.Fatal(perr)
	}
	cols, serr := s.PlanColumns(ctx, stmts[0])
	if serr != nil || len(cols) != 2 || cols[0].Name != "n" || cols[1].Type.String() != "INT8" {
		t.Fatalf("describe with: %v %v", cols, serr)
	}
	expect(`EXPLAIN WITH e AS (SELECT id FROM emp) SELECT * FROM e JOIN emp ON emp.id = e.id`,
		"nested loop inner join; outer (e): full table scan; inner (emp) per outer row: point lookup on primary key")

	conn, err := pgx.Connect(ctx, pgURL(tc, 0))
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer func() { _ = conn.Close(ctx) }()
	rows, err := conn.Query(ctx, `WITH RECURSIVE n AS (SELECT 1 AS x UNION ALL SELECT x + 1 FROM n WHERE x < $1) SELECT x FROM n ORDER BY x DESC LIMIT 1`, 7)
	if err != nil {
		t.Fatalf("pgx recursive: %v", err)
	}
	var got int64
	for rows.Next() {
		if err := rows.Scan(&got); err != nil {
			t.Fatal(err)
		}
	}
	rows.Close()
	if got != 7 {
		t.Fatalf("pgx recursive with a parameter: %d", got)
	}
}
