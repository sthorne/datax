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
	"github.com/sthorne/datax/pkg/sql/parser"
)

// TestWindowFunctions covers OVER: the ranking functions with PARTITION
// BY and ORDER BY (ties, NULLS placement), the offset functions (lag /
// lead with offsets and defaults), the value functions over default and
// explicit frames, aggregates as windows (running, sliding, partition
// totals, count(*) with peers), named windows, window calls inside
// expressions and predicates, windows over grouped and joined queries
// (a derived table as a join member), DISTINCT / ORDER BY / LIMIT after
// the window stage, Describe, EXPLAIN and the refused forms.
func TestWindowFunctions(t *testing.T) {
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

	execSQL(t, ctx, s, `CREATE TABLE sales (id INT8 PRIMARY KEY, region TEXT, amount INT8, note TEXT)`)
	execSQL(t, ctx, s, `INSERT INTO sales VALUES (1, 'n', 10, 'a'), (2, 'n', 30, NULL), (3, 'n', 20, 'c'), (4, 's', 5, 'd'), (5, 's', 5, NULL), (6, 's', 50, 'f')`)

	// Ranking.
	expect(`SELECT id, row_number() OVER (PARTITION BY region ORDER BY amount DESC) AS rn, rank() OVER (PARTITION BY region ORDER BY amount DESC) AS rk, dense_rank() OVER (PARTITION BY region ORDER BY amount DESC) AS dr FROM sales ORDER BY region, rn`,
		"2|1|1|1", "3|2|2|2", "1|3|3|3", "6|1|1|1", "4|2|2|2", "5|3|2|2")
	expect(`SELECT id, percent_rank() OVER (ORDER BY amount) AS pr, cume_dist() OVER (ORDER BY amount) AS cd, ntile(4) OVER (ORDER BY id) AS q FROM sales ORDER BY id`,
		"1|0.4|0.5|1", "2|0.8|0.8333333333333334|1", "3|0.6|0.6666666666666666|2", "4|0|0.3333333333333333|2", "5|0|0.3333333333333333|3", "6|1|1|4")
	expect(`SELECT id, row_number() OVER (ORDER BY note NULLS FIRST, id) FROM sales ORDER BY id`, "1|3", "2|1", "3|4", "4|5", "5|2", "6|6")
	expect(`SELECT row_number() OVER ()`, "1")

	// Offsets and values.
	expect(`SELECT id, lag(amount) OVER (ORDER BY id), lead(amount, 1, 0) OVER (ORDER BY id), lag(amount, 2, -1) OVER (ORDER BY id) FROM sales WHERE region = 'n' ORDER BY id`,
		"1|NULL|30|-1", "2|10|20|-1", "3|30|0|10")
	expect(`SELECT id, first_value(amount) OVER (ORDER BY id), last_value(amount) OVER (ORDER BY id), last_value(amount) OVER (ORDER BY id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING), nth_value(amount, 2) OVER (ORDER BY id) FROM sales WHERE region = 's' ORDER BY id`,
		"4|5|5|50|NULL", "5|5|5|50|5", "6|5|50|50|5")

	// Aggregates as windows.
	expect(`SELECT id, sum(amount) OVER (PARTITION BY region ORDER BY id) AS running, sum(amount) OVER (PARTITION BY region) AS total, count(*) OVER () AS n FROM sales ORDER BY id`,
		"1|10|60|6", "2|40|60|6", "3|60|60|6", "4|5|60|6", "5|10|60|6", "6|60|60|6")
	expect(`SELECT id, sum(amount) OVER (ORDER BY id ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING) AS sliding, avg(amount) OVER (ORDER BY id ROWS 2 PRECEDING) AS trailing FROM sales ORDER BY id`,
		"1|40|10", "2|60|20", "3|55|20", "4|30|18.333333", "5|60|10", "6|55|20")
	expect(`SELECT id, count(*) OVER (ORDER BY amount) AS peers_incl FROM sales ORDER BY id`, "1|3", "2|5", "3|4", "4|2", "5|2", "6|6")
	expect(`SELECT id, string_agg(note, ',') OVER (PARTITION BY region ORDER BY id) FROM sales ORDER BY id`, "1|a", "2|a", "3|a,c", "4|d", "5|d", "6|d,f")
	// WHERE filters before the window stage, as in PostgreSQL.
	expect(`SELECT id, max(amount) OVER (PARTITION BY region ORDER BY id RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) FROM sales WHERE id IN (1, 2, 4, 6) ORDER BY id`, "1|30", "2|30", "4|50", "6|50")

	// Named windows, windows inside expressions and predicates.
	expect(`SELECT id, amount - lag(amount) OVER w AS delta, round(100.0 * amount / sum(amount) OVER (), 1) AS pct FROM sales WINDOW w AS (PARTITION BY region ORDER BY id) ORDER BY id`,
		"1|NULL|8.3", "2|20|25", "3|-10|16.7", "4|NULL|4.2", "5|0|4.2", "6|45|41.7")
	expect(`SELECT id, count(*) OVER (ORDER BY id) > 3 AS late, CASE WHEN row_number() OVER (ORDER BY id) = 1 THEN 'first' ELSE 'rest' END AS pos, coalesce(lag(note) OVER (ORDER BY id), '-') AS prev FROM sales ORDER BY id LIMIT 3`,
		"1|f|first|-", "2|f|rest|a", "3|f|rest|-")
	expect(`SELECT id, first_value(amount) OVER (w ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) FROM sales WINDOW w AS (PARTITION BY region ORDER BY amount DESC) ORDER BY id`,
		"1|30", "2|30", "3|30", "4|50", "5|50", "6|50")

	// Over grouped and joined queries, and a derived table as a join member.
	expect(`SELECT region, sum(amount) AS total, rank() OVER (ORDER BY max(amount) DESC) AS rk FROM sales GROUP BY region ORDER BY rk`, "s|60|1", "n|60|2")
	expect(`SELECT s.id, r.n, row_number() OVER (PARTITION BY s.region ORDER BY s.id) AS rn FROM sales s JOIN (SELECT region, count(*) AS n FROM sales GROUP BY region) AS r ON r.region = s.region ORDER BY s.id`,
		"1|3|1", "2|3|2", "3|3|3", "4|3|1", "5|3|2", "6|3|3")
	expect(`SELECT s.id, d.total FROM sales s LEFT JOIN (SELECT region, sum(amount) AS total FROM sales GROUP BY region) AS d ON d.region = s.region WHERE s.id IN (1, 6) ORDER BY s.id`, "1|60", "6|60")
	expect(`SELECT id FROM sales s WHERE EXISTS (SELECT 1 FROM sales x JOIN (SELECT region FROM sales WHERE amount > 40) AS big ON big.region = x.region WHERE x.id = s.id) ORDER BY id`, "4", "5", "6")

	// DISTINCT, ORDER BY, OFFSET after the window stage; SELECT * with a window.
	expect(`SELECT DISTINCT region, count(*) OVER (PARTITION BY region) FROM sales ORDER BY region`, "n|3", "s|3")
	expect(`SELECT id, row_number() OVER (ORDER BY amount, id) AS rn FROM sales ORDER BY rn DESC LIMIT 2 OFFSET 1`, "2|5", "3|4")
	expect(`SELECT *, row_number() OVER (ORDER BY id) AS rn FROM sales WHERE id < 3 ORDER BY id`, "1|n|10|a|1", "2|n|30|NULL|2")

	// Describe and EXPLAIN.
	stmts, perr := parser.Parse(`SELECT id, amount - lag(amount) OVER (ORDER BY id) AS delta, rank() OVER (ORDER BY id) AS rk, avg(amount) OVER () AS a, percent_rank() OVER () AS p FROM sales`)
	if perr != nil {
		t.Fatal(perr)
	}
	cols, serr := s.PlanColumns(ctx, stmts[0])
	if serr != nil || len(cols) != 5 || cols[1].Name != "delta" || cols[1].Type.String() != "INT8" || cols[2].Type.String() != "INT8" || cols[3].Type.String() != "DECIMAL" || cols[4].Type.String() != "FLOAT8" {
		t.Fatalf("describe windows: %v %v", cols, serr)
	}
	expect(`EXPLAIN SELECT id, sum(amount) OVER (PARTITION BY region) FROM sales`, "full table scan; then 1 window function(s) over the fetched rows")

	code(`SELECT rank() FROM sales`, sql.CodeSyntaxError)
	code(`SELECT sum(amount) OVER (ORDER BY id RANGE BETWEEN 1 PRECEDING AND CURRENT ROW) FROM sales`, sql.CodeFeatureNotSupported)
	code(`SELECT sum(DISTINCT amount) OVER () FROM sales`, sql.CodeFeatureNotSupported)
	code(`SELECT row_number() OVER nosuch FROM sales`, sql.CodeUndefinedObject)
	code(`SELECT ntile(0) OVER (ORDER BY id) FROM sales`, sql.CodeInvalidParameterValue)

	conn, err := pgx.Connect(ctx, pgURL(tc, 0))
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer func() { _ = conn.Close(ctx) }()
	rows, err := conn.Query(ctx, `SELECT id, amount - lag(amount) OVER (PARTITION BY region ORDER BY id) FROM sales WHERE amount > $1 ORDER BY id`, 5)
	if err != nil {
		t.Fatalf("pgx window: %v", err)
	}
	var got []string
	for rows.Next() {
		var id int64
		var delta *int64
		if err := rows.Scan(&id, &delta); err != nil {
			t.Fatal(err)
		}
		if delta == nil {
			got = append(got, "NULL")
		} else {
			got = append(got, strconv.FormatInt(*delta, 10))
		}
	}
	rows.Close()
	if strings.Join(got, ",") != "NULL,20,-10,NULL" {
		t.Fatalf("pgx window deltas: %v", got)
	}
}
