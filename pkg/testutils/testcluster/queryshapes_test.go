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

// TestQueryShapes covers the first query-shape set (#94): OFFSET / FETCH
// / LIMIT ALL / LIMIT 0 on every path, NULLS FIRST | LAST, ORDER BY
// positions, expressions and aggregates over grouped and set-operation
// output, INTERSECT / EXCEPT [ALL] with PostgreSQL's precedence and type
// unification, parenthesized members and VALUES, and the RIGHT / FULL /
// NATURAL / USING join forms.
func TestQueryShapes(t *testing.T) {
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

	execSQL(t, ctx, s, `CREATE TABLE a (id INT8 PRIMARY KEY, x INT8, g TEXT)`)
	execSQL(t, ctx, s, `CREATE TABLE b (id INT8 PRIMARY KEY, aid INT8, y INT8)`)
	execSQL(t, ctx, s, `CREATE INDEX b_aid ON b (aid)`)
	execSQL(t, ctx, s, `INSERT INTO a VALUES (1, 10, 'p'), (2, 20, 'q'), (3, NULL, 'p'), (4, 40, 'r')`)
	execSQL(t, ctx, s, `INSERT INTO b VALUES (1, 1, 100), (2, 1, 101), (3, 3, 300), (4, 9, 900)`)

	// ---- OFFSET / FETCH / LIMIT ALL / LIMIT 0 ------------------------
	expect(`SELECT id FROM a ORDER BY id OFFSET 1 LIMIT 2`, "2", "3")
	expect(`SELECT id FROM a ORDER BY id OFFSET 3 ROWS FETCH FIRST 5 ROWS ONLY`, "4")
	expect(`SELECT id FROM a ORDER BY id LIMIT ALL OFFSET 2`, "3", "4")
	expect(`SELECT id FROM a ORDER BY id FETCH FIRST ROW ONLY`, "1")
	expect(`SELECT id FROM a ORDER BY id LIMIT 0`)
	expect(`SELECT id FROM a ORDER BY id OFFSET 10`)
	expect(`SELECT 1 OFFSET 1`)
	expect(`SELECT id FROM a WHERE x IS NOT NULL ORDER BY x DESC LIMIT 1 OFFSET 1`, "2")
	expect(`SELECT g, count(*) FROM a GROUP BY g ORDER BY g OFFSET 1 LIMIT 1`, "q|1")
	expect(`SELECT a.id FROM a JOIN b ON b.aid = a.id ORDER BY a.id, b.id OFFSET 1 LIMIT 1`, "1")
	expect(`SELECT id FROM (SELECT id FROM a ORDER BY id OFFSET 2) AS d ORDER BY id LIMIT 1`, "3")
	expect(`SELECT DISTINCT g FROM a ORDER BY g OFFSET 1`, "q", "r")
	// The pushed-down fetch keeps LIMIT + OFFSET rows; EXPLAIN says so.
	expect(`EXPLAIN SELECT id FROM a ORDER BY id LIMIT 2 OFFSET 3`, "full table scan; order satisfied by access path; limit pushed into scan; offset 3 applied after fetch")

	// ---- NULLS FIRST | LAST -------------------------------------------
	expect(`SELECT id FROM a ORDER BY x ASC NULLS FIRST`, "3", "1", "2", "4")
	expect(`SELECT id FROM a ORDER BY x DESC NULLS LAST`, "4", "2", "1", "3")
	expect(`SELECT id FROM a ORDER BY x DESC`, "3", "4", "2", "1")
	expect(`SELECT g, max(x) AS m FROM a GROUP BY g ORDER BY m DESC NULLS FIRST, g`, "r|40", "q|20", "p|10")
	expect(`SELECT a.id, b.y FROM a LEFT JOIN b ON b.aid = a.id ORDER BY b.y NULLS FIRST, a.id`, "2|NULL", "4|NULL", "1|100", "1|101", "3|300")
	// An index delivers the default NULL placement only.
	if pl := execSQL(t, ctx, s, `EXPLAIN SELECT id FROM b ORDER BY aid NULLS FIRST`); !strings.Contains(texts(pl)[0], "in-memory sort") {
		t.Fatalf("NULLS FIRST ascending must sort in memory: %v", texts(pl))
	}

	// ---- ORDER BY over grouped output ----------------------------------
	expect(`SELECT g, count(*) FROM a GROUP BY g ORDER BY count(*) DESC, g`, "p|2", "q|1", "r|1")
	expect(`SELECT g FROM a GROUP BY g ORDER BY max(x) DESC`, "r", "q", "p")
	expect(`SELECT g, sum(x) FROM a GROUP BY g ORDER BY 2 DESC NULLS LAST`, "r|40", "q|20", "p|10")
	expect(`SELECT g, count(*) AS n FROM a GROUP BY g ORDER BY n * -1, g`, "p|2", "q|1", "r|1")
	expect(`SELECT count(*) FROM a GROUP BY g ORDER BY g DESC`, "1", "1", "2")
	expect(`SELECT a.g, count(*) FROM a JOIN b ON b.aid = a.id GROUP BY a.g ORDER BY count(*) DESC, a.g`, "p|3")
	code(`SELECT id FROM a ORDER BY count(*)`, sql.CodeGrouping)

	// ---- set operations ----------------------------------------------
	expect(`SELECT id FROM a UNION SELECT aid FROM b ORDER BY 1`, "1", "2", "3", "4", "9")
	expect(`SELECT id FROM a INTERSECT SELECT aid FROM b ORDER BY id`, "1", "3")
	expect(`SELECT aid FROM b INTERSECT ALL SELECT aid FROM b ORDER BY 1`, "1", "1", "3", "9")
	expect(`SELECT id FROM a EXCEPT SELECT aid FROM b ORDER BY 1 DESC`, "4", "2")
	expect(`SELECT aid FROM b EXCEPT ALL SELECT id FROM a WHERE id = 1 ORDER BY 1`, "1", "3", "9")
	// INTERSECT binds tighter: a UNION ALL (b INTERSECT c).
	expect(`SELECT id FROM a UNION ALL SELECT aid FROM b INTERSECT SELECT id FROM b WHERE id < 3 ORDER BY 1`, "1", "1", "2", "3", "4")
	// EXCEPT and UNION associate left to right: (a EXCEPT b) UNION c.
	expect(`SELECT id FROM a EXCEPT SELECT aid FROM b UNION SELECT 9 ORDER BY 1`, "2", "4", "9")
	expect(`(SELECT id FROM a ORDER BY id DESC LIMIT 1) UNION ALL (SELECT id FROM a ORDER BY id LIMIT 1) ORDER BY 1`, "1", "4")
	expect(`VALUES (1), (2) UNION SELECT 3 ORDER BY 1 DESC`, "3", "2", "1")
	expect(`VALUES (1, 'a'), (2, 'b') ORDER BY 1 DESC`, "2|b", "1|a")
	expect(`SELECT id FROM a UNION SELECT aid FROM b ORDER BY 1 LIMIT 2 OFFSET 1`, "2", "3")
	expect(`SELECT 1 UNION SELECT 2.5 ORDER BY 1`, "1", "2.5")
	expect(`SELECT 1 UNION SELECT 'x' ORDER BY 1`, "1", "x")
	if r := execSQL(t, ctx, s, `SELECT 1 UNION SELECT 2.5`); r.Columns[0].Type.String() != "DECIMAL" {
		t.Fatalf("union typing: %v", r.Columns[0].Type)
	}
	stmts, perr := parser.Parse(`SELECT 1 UNION SELECT 'x'`)
	if perr != nil {
		t.Fatal(perr)
	}
	cols, serr := s.PlanColumns(ctx, stmts[0])
	if serr != nil || cols[0].Type.String() != "TEXT" {
		t.Fatalf("describe union: %v %v", cols, serr)
	}
	code(`SELECT id, x FROM a UNION SELECT id FROM b`, sql.CodeSyntaxError)

	// ---- RIGHT / FULL / USING / NATURAL ---------------------------------
	expect(`SELECT a.id, b.id FROM a RIGHT JOIN b ON b.aid = a.id ORDER BY b.id`, "1|1", "1|2", "3|3", "NULL|4")
	expect(`SELECT a.id, b.id FROM a FULL OUTER JOIN b ON b.aid = a.id ORDER BY a.id NULLS LAST, b.id`, "1|1", "1|2", "2|NULL", "3|3", "4|NULL", "NULL|4")
	expect(`SELECT count(*) FROM a FULL JOIN b ON b.aid = a.id WHERE a.id IS NULL`, "1")
	expect(`SELECT b.id FROM a RIGHT JOIN b ON b.aid = a.id WHERE a.id IS NULL`, "4")
	expect(`SELECT * FROM a JOIN b USING (id) ORDER BY id`, "1|10|p|1|100", "2|20|q|1|101", "3|NULL|p|3|300", "4|40|r|9|900")
	expect(`SELECT id, x, y FROM a NATURAL JOIN b ORDER BY id`, "1|10|100", "2|20|101", "3|NULL|300", "4|40|900")
	expect(`SELECT id, y FROM a NATURAL LEFT JOIN b WHERE id > 2 ORDER BY id`, "3|300", "4|900")
	// USING columns read as COALESCE across an outer join.
	execSQL(t, ctx, s, `CREATE TABLE c (id INT8 PRIMARY KEY, z INT8)`)
	execSQL(t, ctx, s, `INSERT INTO c VALUES (4, 4), (7, 7)`)
	expect(`SELECT id, x, z FROM a FULL JOIN c USING (id) ORDER BY id`, "1|10|NULL", "2|20|NULL", "3|NULL|NULL", "4|40|4", "7|NULL|7")
	expect(`SELECT a.id, b.id, c.id FROM a JOIN b ON b.aid = a.id RIGHT JOIN c ON c.id = a.id ORDER BY c.id`, "NULL|NULL|4", "NULL|NULL|7")
	expect(`EXPLAIN SELECT * FROM a RIGHT JOIN b ON b.aid = a.id`, "nested loop right join; outer (a): full table scan; inner (b) per outer row: scan of index \"b_aid\" (1 column prefix) + primary key join + unmatched b rows appended")
	code(`SELECT * FROM a JOIN b USING (nosuch)`, sql.CodeUndefinedColumn)
	code(`SELECT id FROM a JOIN c ON c.id = a.id`, sql.CodeAmbiguousColumn)

	// Over pgwire, the same answers.
	conn, err := pgx.Connect(ctx, pgURL(tc, 0))
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer func() { _ = conn.Close(ctx) }()
	rows, err := conn.Query(ctx, `SELECT id FROM a EXCEPT SELECT aid FROM b ORDER BY 1 OFFSET $1`, 1)
	if err != nil {
		t.Fatalf("pgx except: %v", err)
	}
	var got []int64
	for rows.Next() {
		var v int64
		if err := rows.Scan(&v); err != nil {
			t.Fatal(err)
		}
		got = append(got, v)
	}
	rows.Close()
	if len(got) != 1 || got[0] != 4 {
		t.Fatalf("pgx except offset: %v", got)
	}
}
