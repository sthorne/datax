package testcluster

import (
	"context"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/sthorne/datax/pkg/sql"
	"github.com/sthorne/datax/pkg/sql/catalog"
)

// TestJoinOnSubquery covers issue #379: a subquery in a join's ON
// condition decides matching like any other ON conjunct. An uncorrelated
// one ([NOT] IN, scalar, EXISTS) is evaluated once; one reading the
// joined sides is evaluated per candidate row. Inner, LEFT, RIGHT and
// FULL joins, comma joins beside explicit ones, three-way joins whose
// subquery reads the second or third member, LATERAL, keyed (index
// lookup) and keyless (nested loop) levels, a join nested in an outer
// query's subquery, EXPLAIN, and both wire protocols. Every expected
// result was taken from PostgreSQL 16.
func TestJoinOnSubquery(t *testing.T) {
	tc := Start(t, 1)
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()
	s := sql.NewSession(tc.Nodes[0].DB(), catalog.NewAccessor())

	execSQL(t, ctx, s, `CREATE TABLE a (id INT PRIMARY KEY)`)
	execSQL(t, ctx, s, `CREATE TABLE b (id INT PRIMARY KEY)`)
	execSQL(t, ctx, s, `CREATE TABLE c (x INT PRIMARY KEY, y INT)`)
	execSQL(t, ctx, s, `CREATE TABLE d (id INT PRIMARY KEY, v INT)`)
	execSQL(t, ctx, s, `INSERT INTO a VALUES (1), (2)`)
	execSQL(t, ctx, s, `INSERT INTO b VALUES (1), (2)`)
	execSQL(t, ctx, s, `INSERT INTO c VALUES (1, 1), (3, 2)`)
	execSQL(t, ctx, s, `INSERT INTO d VALUES (1, 10), (2, 20)`)

	runGroupedCases(t, ctx, s, []groupedCase{
		// The issue's table.
		{sql: `SELECT a.id, b.id FROM a JOIN b ON a.id = b.id AND b.id IN (SELECT 1)`, want: "1|1"},
		{sql: `SELECT a.id, b.id FROM a JOIN b ON b.id IN (SELECT 1) ORDER BY 1, 2`, want: "1|1; 2|1"},
		{sql: `SELECT a.id, b.id FROM a LEFT JOIN b ON a.id = b.id AND b.id IN (SELECT 1) ORDER BY 1`, want: "1|1; 2|NULL"},
		{sql: `SELECT a.id, b.id FROM a JOIN b ON a.id = b.id AND b.id NOT IN (SELECT 1)`, want: "2|2"},
		{sql: `SELECT a.id, b.id FROM a JOIN b ON a.id = b.id AND b.id = (SELECT 1)`, want: "1|1"},
		{sql: `SELECT a.id, b.id FROM a JOIN b ON a.id = b.id AND EXISTS (SELECT 1 WHERE b.id = 1)`, want: "1|1"},
		// Uncorrelated EXISTS / NOT EXISTS, an empty IN and a NULL
		// scalar, an OR group, a subquery over a table.
		{sql: `SELECT a.id, b.id FROM a JOIN b ON a.id = b.id AND EXISTS (SELECT 1 FROM c WHERE c.x = 3) ORDER BY 1`, want: "1|1; 2|2"},
		{sql: `SELECT a.id, b.id FROM a LEFT JOIN b ON a.id = b.id AND NOT EXISTS (SELECT 1 FROM c) ORDER BY 1`, want: "1|NULL; 2|NULL"},
		{sql: `SELECT a.id, b.id FROM a LEFT JOIN b ON a.id = b.id AND b.id IN (SELECT x FROM c WHERE x > 5) ORDER BY 1`, want: "1|NULL; 2|NULL"},
		{sql: `SELECT a.id, b.id FROM a LEFT JOIN b ON a.id = b.id AND b.id = (SELECT y FROM c WHERE x = 9) ORDER BY 1`, want: "1|NULL; 2|NULL"},
		{sql: `SELECT a.id, b.id FROM a JOIN b ON a.id = b.id AND (b.id = 2 OR b.id IN (SELECT y FROM c WHERE x = 1)) ORDER BY 1`, want: "1|1; 2|2"},
		{sql: `SELECT a.id, b.id FROM a JOIN b ON a.id = b.id AND b.id IN (SELECT y FROM c) ORDER BY 1`, want: "1|1; 2|2"},
		{sql: `SELECT a.id, b.id FROM a JOIN b ON a.id = b.id AND b.id NOT IN (SELECT y FROM c WHERE x = 3) ORDER BY 1`, want: "1|1"},
		// NOT IN over a list holding NULL is never true.
		{sql: `SELECT a.id, b.id FROM a LEFT JOIN b ON a.id = b.id AND b.id NOT IN (SELECT NULL::INT) ORDER BY 1`, want: "1|NULL; 2|NULL"},

		// Correlated: per candidate row.
		{sql: `SELECT a.id, b.id FROM a JOIN b ON a.id = b.id AND EXISTS (SELECT 1 FROM c WHERE c.x = b.id)`, want: "1|1"},
		{sql: `SELECT a.id, b.id FROM a JOIN b ON a.id = b.id AND NOT EXISTS (SELECT 1 FROM c WHERE c.x = b.id)`, want: "2|2"},
		{sql: `SELECT a.id, b.id FROM a JOIN b ON b.id IN (SELECT c.y FROM c WHERE c.x = a.id) ORDER BY 1`, want: "1|1"},
		{sql: `SELECT a.id, b.id FROM a LEFT JOIN b ON b.id IN (SELECT c.y FROM c WHERE c.x = a.id) ORDER BY 1`, want: "1|1; 2|NULL"},
		{sql: `SELECT a.id, b.id FROM a JOIN b ON b.id NOT IN (SELECT c.y FROM c WHERE c.x = a.id) ORDER BY 1, 2`, want: "1|2; 2|1; 2|2"},
		{sql: `SELECT a.id, b.id FROM a JOIN b ON b.id = (SELECT c.y FROM c WHERE c.x = a.id) ORDER BY 1`, want: "1|1"},
		{sql: `SELECT a.id, b.id FROM a JOIN b ON a.id = b.id AND (SELECT count(*) FROM c WHERE c.y <= b.id) = 2 ORDER BY 1`, want: "2|2"},
		{sql: `SELECT a.id, b.id FROM a JOIN b ON a.id + 1 IN (SELECT c.x + b.id FROM c) ORDER BY 1, 2`, want: "1|1; 2|2"},

		// RIGHT and FULL: ON decides matching, the unmatched side still
		// comes out NULL-extended.
		{sql: `SELECT a.id, b.id FROM a RIGHT JOIN b ON a.id = b.id AND b.id IN (SELECT 1) ORDER BY 2`, want: "1|1; NULL|2"},
		{sql: `SELECT a.id, b.id FROM a FULL JOIN b ON a.id = b.id AND b.id IN (SELECT 1) ORDER BY 1, 2`, want: "1|1; 2|NULL; NULL|2"},
		{sql: `SELECT a.id, b.id FROM a RIGHT JOIN b ON a.id = b.id AND EXISTS (SELECT 1 FROM c WHERE c.x = a.id) ORDER BY 2`, want: "1|1; NULL|2"},
		{sql: `SELECT a.id, b.id FROM a FULL JOIN b ON a.id = b.id AND EXISTS (SELECT 1 FROM c WHERE c.x = b.id) ORDER BY 1, 2`, want: "1|1; 2|NULL; NULL|2"},
		{sql: `SELECT a.id, b.id FROM a FULL JOIN b ON a.id = b.id AND b.id IN (SELECT c.y FROM c WHERE c.x = a.id) ORDER BY 1, 2`, want: "1|1; 2|NULL; NULL|2"},
		// The WHERE then filters the joined rows.
		{sql: `SELECT a.id, b.id FROM a LEFT JOIN b ON a.id = b.id AND b.id IN (SELECT 1) WHERE b.id IS NULL`, want: "2|NULL"},

		// Three tables: the subquery reads the second or the third
		// member, or both ends.
		{sql: `SELECT a.id, b.id, d.id FROM a JOIN b ON a.id = b.id JOIN d ON d.id = b.id AND b.id IN (SELECT 1)`, want: "1|1|1"},
		{sql: `SELECT a.id, b.id, d.id FROM a JOIN b ON a.id = b.id LEFT JOIN d ON d.id = a.id AND EXISTS (SELECT 1 FROM c WHERE c.x = b.id) ORDER BY 1`, want: "1|1|1; 2|2|NULL"},
		{sql: `SELECT a.id, b.id, d.id FROM a JOIN b ON a.id = b.id JOIN d ON d.id = a.id AND d.id NOT IN (SELECT c.x FROM c WHERE c.y = d.id) ORDER BY 1`, want: "2|2|2"},
		{sql: `SELECT a.id, b.id, d.id FROM a JOIN b ON a.id = b.id JOIN d ON EXISTS (SELECT 1 FROM c WHERE c.x = a.id AND c.y = d.id) ORDER BY 1`, want: "1|1|1"},
		{sql: `SELECT a.id, b.id, d.id FROM a JOIN b ON a.id = b.id AND b.id IN (SELECT 2) RIGHT JOIN d ON d.id = a.id ORDER BY 3`, want: "NULL|NULL|1; 2|2|2"},
		{sql: `SELECT a.id, d.v FROM a JOIN d ON d.v = (SELECT max(d2.v) FROM d d2 WHERE d2.id <= a.id) ORDER BY 1`, want: "1|10; 2|20"},

		// Comma joins beside explicit ones.
		{sql: `SELECT a.id, b.id, d.id FROM a, b JOIN d ON d.id = b.id AND b.id IN (SELECT 1) ORDER BY 1`, want: "1|1|1; 2|1|1"},
		{sql: `SELECT a.id, b.id, d.id FROM a JOIN b ON a.id = b.id AND b.id IN (SELECT 1), d WHERE d.id = 2`, want: "1|1|2"},
		{sql: `SELECT a.id, b.id, d.id FROM a, b JOIN d ON d.id = b.id AND EXISTS (SELECT 1 FROM c WHERE c.x = d.id) WHERE a.id = b.id`, want: "1|1|1"},

		// LATERAL.
		{sql: `SELECT a.id, l.v FROM a JOIN LATERAL (SELECT b.id AS v FROM b WHERE b.id >= a.id) l ON l.v IN (SELECT 2) ORDER BY 1`, want: "1|2; 2|2"},
		{sql: `SELECT a.id, l.v FROM a LEFT JOIN LATERAL (SELECT b.id AS v FROM b WHERE b.id >= a.id) l ON l.v IN (SELECT 1) ORDER BY 1`, want: "1|1; 2|NULL"},
		{sql: `SELECT a.id, l.v FROM a JOIN LATERAL (SELECT b.id AS v FROM b WHERE b.id >= a.id) l ON EXISTS (SELECT 1 FROM c WHERE c.x = l.v) ORDER BY 1`, want: "1|1"},
		{sql: `SELECT a.id, l.v FROM a LEFT JOIN LATERAL (SELECT b.id AS v FROM b) l ON l.v IN (SELECT c.y FROM c WHERE c.x = a.id) ORDER BY 1`, want: "1|1; 2|NULL"},

		// Grouped, a derived table, and a join inside an outer query's
		// subquery whose ON reads the outer row.
		{sql: `SELECT count(*) FROM a JOIN b ON b.id IN (SELECT 1)`, want: "2"},
		{sql: `SELECT s.x FROM (SELECT a.id AS x FROM a JOIN b ON a.id = b.id AND b.id NOT IN (SELECT 1)) s`, want: "2"},
		{sql: `SELECT d.id FROM d WHERE EXISTS (SELECT 1 FROM a JOIN b ON a.id = b.id AND b.id IN (SELECT d.id)) ORDER BY 1`, want: "1; 2"},
		{sql: `SELECT d.id FROM d WHERE EXISTS (SELECT 1 FROM a JOIN b ON a.id = b.id AND EXISTS (SELECT 1 FROM c WHERE c.x = b.id AND c.y = d.id)) ORDER BY 1`, want: "1"},
		{sql: `SELECT a.id FROM a WHERE a.id IN (SELECT b.id FROM b JOIN d ON d.id = b.id AND d.v IN (SELECT 20))`, want: "2"},

		// An ON clause sees only the members joined so far.
		{sql: `SELECT 1 FROM a JOIN b ON EXISTS (SELECT 1 FROM c WHERE c.x = d.id) JOIN d ON true`, code: "42P01"},
	})

	if _, serr := trySQL(ctx, s, `SELECT 1 FROM a JOIN b ON EXISTS (SELECT 1 FROM c WHERE c.x = d.id) JOIN d ON true`); serr == nil || serr.Msg != `missing FROM-clause entry for table "d"` {
		t.Errorf("later member in an ON subquery: %v", serr)
	}

	// The keyed level is still a lookup; the subquery joins the filter.
	if p := explainPlan(t, ctx, s, `SELECT a.id FROM a JOIN b ON a.id = b.id AND EXISTS (SELECT 1 FROM c WHERE c.x = b.id)`); !strings.Contains(p, "point lookup on primary key + join filter") {
		t.Errorf("plan: %s", p)
	}
	if p := explainPlan(t, ctx, s, `SELECT a.id FROM a JOIN b ON a.id = b.id AND b.id IN (SELECT 1)`); !strings.Contains(p, "+ join filter") {
		t.Errorf("plan: %s", p)
	}

	// UPDATE ... FROM and DELETE ... USING with a join whose ON holds
	// a subquery.
	execSQL(t, ctx, s, `UPDATE d SET v = v + 1 FROM a JOIN b ON a.id = b.id AND b.id IN (SELECT 1) WHERE d.id = a.id`)
	execSQL(t, ctx, s, `DELETE FROM c USING a JOIN b ON a.id = b.id AND EXISTS (SELECT 1 WHERE b.id = 2) WHERE c.y = a.id`)
	runGroupedCases(t, ctx, s, []groupedCase{
		{sql: `SELECT id, v FROM d ORDER BY id`, want: "1|11; 2|20"},
		{sql: `SELECT x, y FROM c ORDER BY x`, want: "1|1"},
	})

	// Both wire protocols: simple, and extended (Parse, Describe, Bind)
	// with a parameter in and beside the subquery.
	conn, err := pgx.Connect(ctx, pgURL(tc, 0))
	if err != nil {
		t.Fatalf("pgx connect: %v", err)
	}
	defer func() { _ = conn.Close(ctx) }()
	collect := func(q string, args ...any) string {
		t.Helper()
		rows, err := conn.Query(ctx, q, args...)
		if err != nil {
			t.Fatalf("%s: %v", q, err)
		}
		var out []string
		for rows.Next() {
			var x, y *int64
			if err := rows.Scan(&x, &y); err != nil {
				t.Fatalf("scan: %v", err)
			}
			cell := func(p *int64) string {
				if p == nil {
					return "NULL"
				}
				return strconv.FormatInt(*p, 10)
			}
			out = append(out, cell(x)+"|"+cell(y))
		}
		rows.Close()
		if rows.Err() != nil {
			t.Fatalf("%s: %v", q, rows.Err())
		}
		return strings.Join(out, "; ")
	}
	for _, c := range []struct {
		q    string
		args []any
		want string
	}{
		{`SELECT a.id, b.id FROM a JOIN b ON a.id = b.id AND b.id IN (SELECT 1)`, nil, "1|1"},
		{`SELECT a.id, b.id FROM a LEFT JOIN b ON a.id = b.id AND b.id IN (SELECT $1::INT) ORDER BY 1`, []any{1}, "1|1; 2|NULL"},
		{`SELECT a.id, b.id FROM a JOIN b ON a.id = b.id AND b.id NOT IN (SELECT $1::INT)`, []any{1}, "2|2"},
		{`SELECT a.id, b.id FROM a JOIN b ON a.id = b.id AND b.id = (SELECT $1::INT)`, []any{2}, "2|2"},
		{`SELECT a.id, b.id FROM a JOIN b ON a.id = b.id AND EXISTS (SELECT 1 FROM c WHERE c.x = b.id AND c.y = $1)`, []any{1}, "1|1"},
		{`SELECT a.id, b.id FROM a FULL JOIN b ON a.id = b.id AND b.id IN (SELECT c.y FROM c WHERE c.x = a.id AND c.y >= $1) ORDER BY 1, 2`, []any{0}, "1|1; 2|NULL; NULL|2"},
	} {
		if got := collect(c.q, append([]any{pgx.QueryExecModeSimpleProtocol}, c.args...)...); got != c.want {
			t.Errorf("simple %s:\n got  %s\n want %s", c.q, got, c.want)
		}
		if got := collect(c.q, append([]any{pgx.QueryExecModeDescribeExec}, c.args...)...); got != c.want {
			t.Errorf("extended %s:\n got  %s\n want %s", c.q, got, c.want)
		}
	}
	if got := describeOIDs(t, ctx, conn, "on_sub", `SELECT a.id, b.id FROM a JOIN b ON a.id = b.id AND EXISTS (SELECT 1 FROM c WHERE c.x = b.id)`); !sameOIDs(got, []uint32{23, 23}) {
		t.Errorf("described: %v", got)
	}
}
