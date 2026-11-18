package testcluster

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/sthorne/datax/pkg/sql"
	"github.com/sthorne/datax/pkg/sql/catalog"
)

// TestCorrelatedScoping (#361): PostgreSQL's scoping rule — an aliased
// FROM item answers to its alias only, so a reference by the table's own
// name climbs to an enclosing query — in every place a correlated
// reference can stand: select list, WHERE, HAVING and ORDER BY, grouped
// and ungrouped, EXISTS / IN / scalar subqueries, LATERAL, nested two
// levels deep, UPDATE (plain and ... FROM) and DELETE. A grouped select's
// subqueries run per group, bound by the group's keys. Every expected
// value was checked against PostgreSQL 16; before the fix, the grouped
// forms counted the whole table (the outer "ev." bound to the inner ev)
// or were refused, and a table name hidden by its alias was read as the
// aliased table's column.
func TestCorrelatedScoping(t *testing.T) {
	n, _ := startGCNode(t)
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()
	s := sql.NewSession(n.DB(), catalog.NewAccessor())

	setup := func() {
		t.Helper()
		execSQL(t, ctx, s, `DROP TABLE IF EXISTS ev`)
		execSQL(t, ctx, s, `DROP TABLE IF EXISTS g`)
		execSQL(t, ctx, s, `CREATE TABLE ev (id INT8 PRIMARY KEY, grp INT8, v INT8)`)
		execSQL(t, ctx, s, `INSERT INTO ev VALUES (1,1,10),(2,1,20),(3,1,30),(4,2,40),(5,2,50),(6,2,60)`)
		execSQL(t, ctx, s, `CREATE TABLE g (grp INT8 PRIMARY KEY, name TEXT, tot INT8)`)
		execSQL(t, ctx, s, `INSERT INTO g VALUES (1,'a',0),(2,'b',0),(3,'c',0)`)
	}
	setup()

	// rows renders a result as "a,b c,d" (NULL as empty).
	rows := func(q string) string {
		t.Helper()
		res := execSQL(t, ctx, s, q)
		out := make([]string, 0, len(res.Rows))
		for _, r := range res.Rows {
			cells := make([]string, len(r))
			for i, d := range r {
				if !d.Null {
					cells[i] = d.Text()
				}
			}
			out = append(out, strings.Join(cells, ","))
		}
		return strings.Join(out, " ")
	}
	check := func(q, want string) {
		t.Helper()
		if got := rows(q); got != want {
			t.Fatalf("%s\n got: %q\nwant: %q", q, got, want)
		}
	}
	fails := func(q, code, msg string) *sql.Error {
		t.Helper()
		_, serr := trySQL(ctx, s, q)
		if serr == nil || serr.Code != code || !strings.Contains(serr.Msg, msg) {
			t.Fatalf("%s\n got: %+v\nwant: %s %q", q, serr, code, msg)
		}
		return serr
	}

	// The issue: grouped, the outer table under its own name, the inner
	// aliased. Three rows per group: 3 each (it was 6, the whole table).
	check(`SELECT grp, (SELECT count(*) FROM ev e2 WHERE e2.grp = ev.grp) FROM ev GROUP BY grp ORDER BY grp`, "1,3 2,3")
	// ... with any other outer alias (refused before).
	check(`SELECT grp, (SELECT count(*) FROM ev e2 WHERE e2.grp = o.grp) FROM ev o GROUP BY grp ORDER BY grp`, "1,3 2,3")
	check(`SELECT o.grp, (SELECT max(v) FROM ev e2 WHERE e2.grp = o.grp) AS m FROM ev o GROUP BY o.grp ORDER BY 1`, "1,30 2,60")
	// ... the inner unaliased: its own name is its own.
	check(`SELECT e.grp, (SELECT count(*) FROM ev WHERE ev.grp = e.grp AND ev.v > 15) FROM ev e GROUP BY e.grp ORDER BY 1`, "1,2 2,3")
	// Next to aggregates, with a filtered input.
	check(`SELECT grp, count(*) AS n, (SELECT name FROM g WHERE g.grp = ev.grp) FROM ev WHERE v > 10 GROUP BY grp ORDER BY 1`, "1,2,a 2,3,b")
	// A grouping set's rolled-up key reads as NULL in the subquery too.
	check(`SELECT grp, (SELECT count(*) FROM g WHERE g.grp = ev.grp) FROM ev GROUP BY ROLLUP (grp) ORDER BY grp`, "1,1 2,1 ,0")
	// Grouped over a join.
	check(`SELECT a.grp, count(*), (SELECT count(*) FROM ev e2 WHERE e2.grp = a.grp AND e2.v > 15) FROM ev a JOIN g ON g.grp = a.grp GROUP BY a.grp ORDER BY 1`, "1,3,2 2,3,3")
	check(`SELECT g.name, (SELECT sum(v) FROM ev e WHERE e.grp = g.grp) FROM g GROUP BY g.name, g.grp ORDER BY 1`, "a,60 b,150 c,")

	// HAVING: scalar, EXISTS, against an aggregate.
	check(`SELECT grp FROM ev GROUP BY grp HAVING (SELECT count(*) FROM ev e2 WHERE e2.grp = ev.grp AND e2.v > 15) = 2`, "1")
	check(`SELECT grp FROM ev GROUP BY grp HAVING EXISTS (SELECT 1 FROM ev e2 WHERE e2.grp = ev.grp AND e2.v > 55)`, "2")
	check(`SELECT grp, sum(v) FROM ev o GROUP BY grp HAVING sum(v) > (SELECT min(e2.v) * 4 FROM ev e2 WHERE e2.grp = o.grp)`, "1,60")
	check(`SELECT grp FROM ev GROUP BY grp HAVING (SELECT count(*) FROM ev e2 WHERE e2.grp = ev.grp) = count(*) ORDER BY 1`, "1 2")

	// Grouped ORDER BY: by a correlated subquery, and by the output name
	// of one.
	check(`SELECT grp FROM ev GROUP BY grp ORDER BY (SELECT max(v) FROM ev e2 WHERE e2.grp <> ev.grp)`, "2 1")
	check(`SELECT grp, (SELECT name FROM g WHERE g.grp = ev.grp) AS n FROM ev GROUP BY grp ORDER BY n DESC`, "2,b 1,a")

	// Nested two levels deep, grouped: the innermost reads both the
	// middle query's row and the grouped row.
	check(`SELECT grp, (SELECT count(*) FROM ev e2 WHERE e2.grp = ev.grp AND EXISTS (SELECT 1 FROM g WHERE g.grp = e2.grp AND g.grp = ev.grp AND e2.v > 10 * ev.grp)) FROM ev GROUP BY grp ORDER BY 1`, "1,2 2,3")

	// Ungrouped: select list, WHERE scalar / EXISTS / IN, ORDER BY.
	check(`SELECT id, (SELECT count(*) FROM ev e2 WHERE e2.grp = ev.grp AND e2.v < ev.v) FROM ev ORDER BY id`, "1,0 2,1 3,2 4,0 5,1 6,2")
	check(`SELECT id FROM ev WHERE (SELECT count(*) FROM ev e2 WHERE e2.grp = ev.grp AND e2.v > ev.v) = 1 ORDER BY id`, "2 5")
	check(`SELECT id FROM ev WHERE EXISTS (SELECT 1 FROM ev e2 WHERE e2.grp = ev.grp AND e2.v > ev.v) ORDER BY id`, "1 2 4 5")
	check(`SELECT id FROM ev WHERE v IN (SELECT e2.v - 10 FROM ev e2 WHERE e2.grp = ev.grp) ORDER BY id`, "1 2 4 5")
	check(`SELECT id FROM ev ORDER BY (SELECT count(*) FROM ev e2 WHERE e2.v > ev.v), id`, "6 5 4 3 2 1")
	check(`SELECT id FROM ev o ORDER BY (SELECT count(*) FROM ev e2 WHERE e2.v > o.v) LIMIT 2`, "6 5")
	check(`SELECT id, (SELECT count(*) FROM ev e2 WHERE e2.v > o.v) AS c FROM ev o ORDER BY c DESC LIMIT 2`, "1,5 2,4")
	check(`SELECT id, (SELECT count(*) FROM ev e2 WHERE e2.v > o.v) FROM ev o ORDER BY 2 LIMIT 2`, "6,0 5,1")
	check(`SELECT * FROM ev o ORDER BY (SELECT count(*) FROM ev e2 WHERE e2.grp = o.grp AND e2.v > o.v) DESC, id LIMIT 3`, "1,1,10 4,2,40 2,1,20")
	check(`SELECT DISTINCT grp, (SELECT count(*) FROM ev e2 WHERE e2.grp <> o.grp AND e2.v > 15) AS c FROM ev o ORDER BY c`, "2,2 1,3")

	// Nested two levels deep, ungrouped.
	check(`SELECT id FROM ev WHERE EXISTS (SELECT 1 FROM ev e2 WHERE EXISTS (SELECT 1 FROM ev e3 WHERE e3.id = e2.id AND e3.grp = ev.grp AND e3.v > ev.v)) ORDER BY id`, "1 2 4 5")
	check(`SELECT id FROM ev WHERE id IN (SELECT e2.id FROM ev e2 WHERE e2.v = (SELECT max(e3.v) FROM ev e3 WHERE e3.grp = ev.grp)) ORDER BY 1`, "3 6")

	// LATERAL.
	check(`SELECT ev.id, x.c FROM ev, LATERAL (SELECT count(*) AS c FROM ev e2 WHERE e2.grp = ev.grp AND e2.v < ev.v) x ORDER BY ev.id`, "1,0 2,1 3,2 4,0 5,1 6,2")
	check(`SELECT o.id, x.n FROM ev o, LATERAL (SELECT (SELECT count(*) FROM ev e3 WHERE e3.grp = e2.grp AND e3.v < o.v) AS n FROM ev e2 WHERE e2.id = o.id) x ORDER BY 1`, "1,0 2,1 3,2 4,0 5,1 6,2")

	// A FROM-less select honours its WHERE (it returned its row whatever
	// the WHERE said, so a FROM-less EXISTS was always true).
	check(`SELECT 1 WHERE 1 = 2`, "")
	check(`SELECT 1, 'x' WHERE 2 > 1`, "1,x")
	check(`SELECT id FROM ev o WHERE EXISTS (SELECT 1 WHERE o.grp = 2) ORDER BY 1`, "4 5 6")
	fails(`SELECT 1 WHERE nosuch = 2`, sql.CodeUndefinedColumn, `column "nosuch" does not exist`)

	// An outer join's USING column, bare, belongs to the merged pair.
	check(`SELECT o.id FROM ev o JOIN g USING (grp) WHERE EXISTS (SELECT 1 WHERE grp = 2) ORDER BY 1`, "4 5 6")

	// UPDATE ... FROM: the FROM item under its own name, the target
	// under an alias.
	execSQL(t, ctx, s, `UPDATE g SET tot = (SELECT sum(v) FROM ev e2 WHERE e2.grp = ev.grp) FROM ev WHERE ev.grp = g.grp AND ev.v IN (10, 40)`)
	check(`SELECT grp, tot FROM g ORDER BY grp`, "1,60 2,150 3,0")
	check(`UPDATE g x SET tot = (SELECT count(*) FROM ev e2 WHERE e2.grp = x.grp AND e2.v > 15) FROM ev WHERE ev.grp = x.grp AND ev.v = 10 RETURNING x.grp, x.tot`, "1,2")
	check(`UPDATE g SET tot = -1 FROM ev e WHERE e.grp = g.grp AND e.v = 40 AND EXISTS (SELECT 1 FROM ev WHERE ev.grp = e.grp AND ev.v > 55) RETURNING g.grp`, "2")
	check(`SELECT grp, tot FROM g ORDER BY grp`, "1,2 2,-1 3,0")
	// A plain UPDATE's SET reading the row being updated (it read the
	// inner g's own row: 3,3,3).
	execSQL(t, ctx, s, `UPDATE g SET tot = (SELECT count(*) FROM g g2 WHERE g2.grp <= g.grp)`)
	check(`SELECT grp, tot FROM g ORDER BY grp`, "1,1 2,2 3,3")
	execSQL(t, ctx, s, `UPDATE g x SET tot = (SELECT sum(v) FROM ev WHERE ev.grp = x.grp) + x.tot WHERE grp < 3`)
	check(`SELECT grp, tot FROM g ORDER BY grp`, "1,61 2,152 3,3")
	// The table's own name qualifies when it has no alias.
	if res := execSQL(t, ctx, s, `UPDATE ev SET v = v WHERE ev.id = 1`); res.Tag != "UPDATE 1" {
		t.Fatalf("UPDATE with a table-qualified WHERE: %s", res.Tag)
	}
	if res := execSQL(t, ctx, s, `DELETE FROM g WHERE NOT EXISTS (SELECT 1 FROM ev WHERE ev.grp = g.grp) AND g.grp = 3`); res.Tag != "DELETE 1" {
		t.Fatalf("DELETE: %s", res.Tag)
	}

	// Errors. A table name hidden by its alias, with nothing enclosing to
	// bind to: 42P01, worded and hinted as PostgreSQL does.
	hint := `Perhaps you meant to reference the table alias "e2".`
	for _, q := range []string{
		`SELECT count(*) FROM ev e2 WHERE e2.grp = ev.grp`,
		`SELECT (SELECT count(*) FROM ev e2 WHERE e2.grp = ev.grp) FROM ev e1`,
		`SELECT grp, (SELECT count(*) FROM ev e2 WHERE e2.grp = ev.grp) FROM ev e1 GROUP BY grp`,
		`SELECT grp FROM ev e1 GROUP BY grp HAVING (SELECT count(*) FROM ev e2 WHERE e2.grp = ev.grp) > 1`,
		`SELECT id FROM ev e1 ORDER BY (SELECT count(*) FROM ev e2 WHERE e2.v > ev.v)`,
		`SELECT o.id, x.c FROM ev o, LATERAL (SELECT count(*) AS c FROM ev e2 WHERE e2.grp = ev.grp) x`,
		`UPDATE g SET tot = 0 FROM ev e WHERE EXISTS (SELECT 1 FROM ev e2 WHERE e2.grp = ev.grp) AND e.grp = g.grp`,
	} {
		if serr := fails(q, sql.CodeUndefinedTable, `invalid reference to FROM-clause entry for table "ev"`); serr.Hint != hint {
			t.Fatalf("%s: hint %q", q, serr.Hint)
		}
	}
	fails(`SELECT ev.id FROM ev e`, sql.CodeUndefinedTable, `invalid reference to FROM-clause entry for table "ev"`)
	fails(`SELECT id FROM ev e ORDER BY ev.v`, sql.CodeUndefinedTable, `invalid reference to FROM-clause entry for table "ev"`)
	fails(`SELECT a.id FROM ev a JOIN g ON g.grp = a.grp WHERE ev.id = 1`, sql.CodeUndefinedTable, `invalid reference to FROM-clause entry for table "ev"`)
	fails(`SELECT a.id FROM ev a JOIN g b ON g.grp = a.grp`, sql.CodeUndefinedTable, `invalid reference to FROM-clause entry for table "g"`)
	fails(`UPDATE ev e SET v = 1 WHERE ev.id = 1`, sql.CodeUndefinedTable, `invalid reference to FROM-clause entry for table "ev"`)
	fails(`DELETE FROM ev e WHERE ev.id = 1`, sql.CodeUndefinedTable, `invalid reference to FROM-clause entry for table "ev"`)
	if serr := fails(`UPDATE g x SET tot = 2 FROM ev WHERE ev.grp = g.grp`, sql.CodeUndefinedTable, `invalid reference to FROM-clause entry for table "g"`); serr.Hint != `Perhaps you meant to reference the table alias "x".` {
		t.Fatalf("UPDATE ... FROM hint: %q", serr.Hint)
	}
	// A name no FROM item has.
	fails(`SELECT id FROM ev WHERE bogus.id = 1`, sql.CodeUndefinedTable, `missing FROM-clause entry for table "bogus"`)
	fails(`SELECT grp, (SELECT count(*) FROM ev e2 WHERE e2.grp = bogus.grp) FROM ev GROUP BY grp`, sql.CodeUndefinedTable, `missing FROM-clause entry for table "bogus"`)
	// Ambiguous within one scope: the enclosing join's, and the
	// subquery's own.
	fails(`SELECT 1 FROM ev a JOIN ev b ON a.id = b.id WHERE EXISTS (SELECT 1 FROM g WHERE g.grp = v)`, sql.CodeAmbiguousColumn, `column reference "v" is ambiguous`)
	fails(`SELECT id FROM ev WHERE EXISTS (SELECT 1 FROM ev a JOIN g ON a.grp = g.grp WHERE a.id = ev.id AND grp = 1)`, sql.CodeAmbiguousColumn, `column reference "grp" is ambiguous`)
	// A grouped select's subquery may only read grouping columns.
	fails(`SELECT grp, (SELECT count(*) FROM ev e2 WHERE e2.v > ev.v) FROM ev GROUP BY grp`, sql.CodeGrouping, `subquery uses ungrouped column "ev.v" from outer query`)
	fails(`SELECT count(*), (SELECT count(*) FROM g WHERE g.grp = ev.grp) FROM ev`, sql.CodeGrouping, `subquery uses ungrouped column "ev.grp" from outer query`)
	fails(`SELECT grp, (SELECT count(*) FROM g WHERE g.tot < v) FROM ev o GROUP BY grp`, sql.CodeGrouping, `subquery uses ungrouped column "o.v" from outer query`)
	// DISTINCT sorts only by what it shows.
	fails(`SELECT DISTINCT grp FROM ev o ORDER BY (SELECT 1 FROM g WHERE g.grp = o.grp)`, sql.CodeInvalidColumnReference, `for SELECT DISTINCT, ORDER BY expressions must appear in select list`)

	// EXPLAIN describes the shape (it ran the subquery eagerly before).
	if p := explainPlan(t, ctx, s, `SELECT grp, (SELECT count(*) FROM ev e2 WHERE e2.grp = ev.grp) FROM ev GROUP BY grp`); !strings.Contains(p, "correlated subquery") {
		t.Fatalf("explain: %q", p)
	}
	if p := explainPlan(t, ctx, s, `SELECT id FROM ev o ORDER BY (SELECT count(*) FROM ev e2 WHERE e2.v > o.v) LIMIT 2`); strings.Contains(p, "top-k") || !strings.Contains(p, "sort") {
		t.Fatalf("explain: %q", p)
	}
}
