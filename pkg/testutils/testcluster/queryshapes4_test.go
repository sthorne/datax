package testcluster

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/sthorne/datax/pkg/sql"
	"github.com/sthorne/datax/pkg/sql/catalog"
	"github.com/sthorne/datax/pkg/sql/parser"
)

// TestQueryShapes4 covers the last query-shape set (#94): IN / EXISTS
// subqueries inside OR (uncorrelated and correlated, in SELECT, UPDATE
// and DELETE, over joins and grouped queries), scalar subqueries in
// ORDER BY, correlated subqueries nested past four levels, and EXPLAIN
// ANALYZE's stage report.
func TestQueryShapes4(t *testing.T) {
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

	execSQL(t, ctx, s, `CREATE TABLE a (id INT8 PRIMARY KEY, g TEXT, x INT8)`)
	execSQL(t, ctx, s, `CREATE TABLE b (id INT8 PRIMARY KEY, aid INT8, y INT8)`)
	execSQL(t, ctx, s, `INSERT INTO a VALUES (1, 'p', 1), (2, 'q', 2), (3, 'p', 3), (4, 'r', 4)`)
	execSQL(t, ctx, s, `INSERT INTO b VALUES (1, 1, 10), (2, 3, 30), (3, 9, 90)`)

	// IN / EXISTS inside OR.
	expect(`SELECT id FROM a WHERE id IN (SELECT aid FROM b) OR g = 'r' ORDER BY id`, "1", "3", "4")
	expect(`SELECT id FROM a WHERE g = 'q' OR EXISTS (SELECT 1 FROM b WHERE b.aid = a.id) ORDER BY id`, "1", "2", "3")
	expect(`SELECT id FROM a WHERE (id IN (SELECT aid FROM b) AND x > 1) OR (NOT EXISTS (SELECT 1 FROM b WHERE b.aid = a.id) AND g = 'q') ORDER BY id`, "2", "3")
	expect(`SELECT id FROM a WHERE id NOT IN (SELECT aid FROM b) OR id = 1 ORDER BY id`, "1", "2", "4")
	expect(`SELECT a.id FROM a JOIN b ON b.aid = a.id WHERE a.g = 'x' OR a.id IN (SELECT aid FROM b WHERE y > 20) ORDER BY a.id`, "3")
	expect(`SELECT g, count(*) FROM a WHERE g = 'r' OR id IN (SELECT aid FROM b) GROUP BY g ORDER BY g`, "p|2", "r|1")
	expect(`SELECT id FROM a WHERE x > 3 OR (SELECT count(*) FROM b WHERE b.aid = a.id) > 0 ORDER BY id`, "1", "3", "4")
	expect(`UPDATE a SET x = x + 100 WHERE g = 'r' OR id IN (SELECT aid FROM b WHERE y = 10) RETURNING id`, "1", "4")
	expect(`DELETE FROM a WHERE g = 'zz' OR EXISTS (SELECT 1 FROM b WHERE b.aid = a.id AND y = 30) RETURNING id`, "3")
	expect(`SELECT id, x FROM a ORDER BY id`, "1|101", "2|2", "4|104")

	// A scalar subquery in ORDER BY.
	expect(`SELECT id FROM a ORDER BY x - (SELECT avg(x) FROM a) DESC, id`, "4", "1", "2")

	// Correlation six levels deep.
	expect(`SELECT id FROM a a1 WHERE EXISTS (SELECT 1 FROM a a2 WHERE a2.id = a1.id AND EXISTS (SELECT 1 FROM a a3 WHERE a3.id = a2.id AND EXISTS (SELECT 1 FROM a a4 WHERE a4.id = a3.id AND EXISTS (SELECT 1 FROM a a5 WHERE a5.id = a4.id AND EXISTS (SELECT 1 FROM a a6 WHERE a6.id = a5.id AND a6.x > 100))))) ORDER BY id`, "1", "4")

	// EXPLAIN ANALYZE: the plan line, a line per stage, the totals.
	r := execSQL(t, ctx, s, `EXPLAIN ANALYZE SELECT a.id, b.y FROM a JOIN b ON b.aid = a.id WHERE a.g = 'x' OR a.id IN (SELECT aid FROM b WHERE y > 5) ORDER BY b.y DESC`)
	lines := texts(r)
	if len(lines) < 5 || !strings.HasPrefix(lines[0], "plan: nested loop inner join") || !strings.HasPrefix(lines[len(lines)-1], "output: 1 rows; total ") {
		t.Fatalf("explain analyze: %q", lines)
	}
	joined := strings.Join(lines, "\n")
	for _, want := range []string{"scan b: full table scan; ", "scan a: full table scan; ", "join level 1 (b, inner): 1 rows", "sort: 1 joined rows in memory", " rows in "} {
		if !strings.Contains(joined, want) {
			t.Fatalf("explain analyze lacks %q:\n%s", want, joined)
		}
	}
	r = execSQL(t, ctx, s, `EXPLAIN ANALYZE SELECT g, count(*) FROM a GROUP BY g ORDER BY 2 DESC`)
	if j := strings.Join(texts(r), "\n"); !strings.Contains(j, "group/aggregate: 3 groups from 3 rows") || !strings.Contains(j, "output: 3 rows") {
		t.Fatalf("explain analyze grouped: %s", j)
	}
	r = execSQL(t, ctx, s, `EXPLAIN ANALYZE SELECT id, row_number() OVER (ORDER BY x) FROM a`)
	if j := strings.Join(texts(r), "\n"); !strings.Contains(j, "window: 1 function(s) over 3 rows") {
		t.Fatalf("explain analyze window: %s", j)
	}
	if r := execSQL(t, ctx, s, `EXPLAIN SELECT id FROM a`); len(r.Rows) != 1 || r.Rows[0][0].Text() != "full table scan" {
		t.Fatalf("plain explain unchanged: %v", texts(r))
	}
	stmts, perr := parser.Parse(`EXPLAIN ANALYZE SELECT id FROM a`)
	if perr != nil {
		t.Fatal(perr)
	}
	cols, serr := s.PlanColumns(ctx, stmts[0])
	if serr != nil || len(cols) != 1 || cols[0].Name != "plan" {
		t.Fatalf("describe explain analyze: %v %v", cols, serr)
	}
}
