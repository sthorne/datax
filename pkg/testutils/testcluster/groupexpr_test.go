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

// groupedText renders a result as "a|b; c|d" (NULL for NULL), for
// comparing whole results.
func groupedText(res *sql.Result) string {
	var rows []string
	for _, r := range res.Rows {
		cells := make([]string, len(r))
		for i, d := range r {
			if d.Null {
				cells[i] = "NULL"
			} else {
				cells[i] = d.Text()
			}
		}
		rows = append(rows, strings.Join(cells, "|"))
	}
	return strings.Join(rows, "; ")
}

// groupedCase is one query and the result it must render to, or the
// SQLSTATE it must be refused with.
type groupedCase struct {
	sql  string
	want string
	code string
}

func runGroupedCases(t *testing.T, ctx context.Context, s *sql.Session, cases []groupedCase) {
	t.Helper()
	for _, c := range cases {
		res, serr := trySQL(ctx, s, c.sql)
		if c.code != "" {
			if serr == nil || string(serr.Code) != c.code {
				t.Errorf("%s: want SQLSTATE %s, got %v", c.sql, c.code, serr)
			}
			continue
		}
		if serr != nil {
			t.Errorf("%s: [%s] %s", c.sql, serr.Code, serr.Msg)
			continue
		}
		if got := groupedText(res); got != c.want {
			t.Errorf("%s:\n got  %s\n want %s", c.sql, got, c.want)
		}
	}
}

// describeOIDs prepares a statement through pgx (Parse + Describe) and
// returns its result columns' type OIDs.
func describeOIDs(t *testing.T, ctx context.Context, conn *pgx.Conn, name, query string) []uint32 {
	t.Helper()
	sd, err := conn.Prepare(ctx, name, query)
	if err != nil {
		t.Fatalf("prepare %s: %v", query, err)
	}
	oids := make([]uint32, len(sd.Fields))
	for i, f := range sd.Fields {
		oids[i] = f.DataTypeOID
	}
	return oids
}

func sameOIDs(a, b []uint32) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

const (
	oidBool    = 16
	oidInt8    = 20
	oidText    = 25
	oidFloat8  = 701
	oidNumeric = 1700
)

// TestAggregateExpressions covers issue #335: scalar expressions over
// aggregate calls and grouping columns, in the select list, HAVING and
// ORDER BY, with and without GROUP BY and over a join; a column neither
// grouped nor aggregated is refused with 42803.
func TestAggregateExpressions(t *testing.T) {
	tc := Start(t, 1)
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()
	s := sql.NewSession(tc.Nodes[0].DB(), catalog.NewAccessor())

	execSQL(t, ctx, s, `CREATE TABLE sales (id INT8 PRIMARY KEY, city TEXT, amount INT8, region TEXT)`)
	execSQL(t, ctx, s, `INSERT INTO sales VALUES
		(1, 'oslo', 10, 'north'), (2, 'oslo', 20, 'north'), (3, 'bergen', 30, 'west'),
		(4, NULL, 40, 'west'), (5, NULL, NULL, 'west'), (6, 'tromso', 5, 'north')`)
	execSQL(t, ctx, s, `CREATE TABLE regions (name TEXT PRIMARY KEY, target INT8)`)
	execSQL(t, ctx, s, `INSERT INTO regions VALUES ('north', 30), ('west', 100)`)

	runGroupedCases(t, ctx, s, []groupedCase{
		// Without GROUP BY: one row.
		{sql: `SELECT SUM(amount) / COUNT(amount) FROM sales`, want: "21"},
		{sql: `SELECT COUNT(*) * 2, MAX(amount) - MIN(amount) AS spread FROM sales`, want: "12|35"},
		{sql: `SELECT COALESCE(SUM(amount), 0) + 1 FROM sales WHERE id > 100`, want: "1"},
		{sql: `SELECT ROUND(AVG(amount) / 4, 2) FROM sales`, want: "5.25"},
		{sql: `SELECT (COUNT(*)) FROM sales`, want: "6"},
		{sql: `SELECT COUNT(*)::text || ' rows' FROM sales`, want: "6 rows"},
		// The aggregate's own modifiers survive inside an expression.
		{sql: `SELECT COUNT(DISTINCT city) + 0, COUNT(*) FILTER (WHERE amount > 15) * 10 FROM sales`, want: "3|30"},
		// Grouped: arithmetic, function calls, CASE, casts, COALESCE,
		// a scalar subquery, and the grouping column inside expressions.
		{sql: `SELECT region, COUNT(*) * 100 / (SELECT COUNT(*) FROM sales) AS pct, ROUND(AVG(amount) * 1.5, 1) AS mean,
				COALESCE(MAX(city), 'none') AS top, CASE WHEN SUM(amount) > 50 THEN 'big' ELSE 'small' END AS size,
				SUM(amount)::text AS total_text, upper(region) || ':' || COUNT(*) AS label
			FROM sales GROUP BY region ORDER BY region`,
			want: "north|50|17.5|tromso|small|35|NORTH:3; west|50|52.5|bergen|big|70|WEST:3"},
		{sql: `SELECT city, COUNT(*) FILTER (WHERE amount IS NOT NULL) - COUNT(*) AS missing FROM sales GROUP BY city ORDER BY city NULLS FIRST`,
			want: "NULL|-1; bergen|0; oslo|0; tromso|0"},
		// HAVING: any condition over aggregates and grouping columns.
		{sql: `SELECT region FROM sales GROUP BY region HAVING SUM(amount) / COUNT(amount) > 15 ORDER BY region`, want: "west"},
		{sql: `SELECT region FROM sales GROUP BY region HAVING COUNT(*) > 5 OR MIN(amount) < 10`, want: "north"},
		{sql: `SELECT region FROM sales GROUP BY region HAVING 60 < SUM(amount) AND upper(region) LIKE 'W%'`, want: "west"},
		{sql: `SELECT region, COUNT(*) AS n FROM sales GROUP BY region HAVING n = 3 AND region = 'north'`, want: "north|3"},
		// ORDER BY an expression over aggregates, or a position naming one.
		{sql: `SELECT region, SUM(amount) FROM sales GROUP BY region ORDER BY SUM(amount) * -1`, want: "west|70; north|35"},
		{sql: `SELECT region, SUM(amount) / COUNT(*) AS per FROM sales GROUP BY region ORDER BY 2 DESC`, want: "west|23; north|11"},
		{sql: `SELECT region FROM sales GROUP BY region ORDER BY MAX(amount) - MIN(amount)`, want: "west; north"},
		// A grouped join.
		{sql: `SELECT r.name, SUM(s.amount) * 100 / MAX(r.target) AS pct_of_target FROM sales s JOIN regions r ON r.name = s.region
			GROUP BY r.name HAVING SUM(s.amount) * 100 / MAX(r.target) > 50 ORDER BY SUM(s.amount) DESC`,
			want: "west|70; north|116"},
		// Neither grouped nor aggregated: 42803.
		{sql: `SELECT city, SUM(amount) / COUNT(*) FROM sales GROUP BY region`, code: "42803"},
		{sql: `SELECT amount + SUM(amount) FROM sales`, code: "42803"},
		{sql: `SELECT region FROM sales GROUP BY region HAVING amount > 1`, code: "42803"},
		{sql: `SELECT region FROM sales GROUP BY region ORDER BY amount`, code: "42803"},
		{sql: `SELECT region, COUNT(*) FROM sales s JOIN regions r ON r.name = s.region GROUP BY region ORDER BY r.target`, code: "42803"},
		// Aggregates where they cannot be.
		{sql: `SELECT id FROM sales WHERE COUNT(*) > 1`, code: "42803"},
		{sql: `SELECT nope + COUNT(*) FROM sales`, code: "42703"},
	})

	// EXPLAIN plans the grouped select like any other.
	if p := explainPlan(t, ctx, s, `SELECT region, SUM(amount) / COUNT(*) FROM sales GROUP BY region HAVING COUNT(*) > 1`); p != "full table scan" {
		t.Fatalf("plan: %s", p)
	}

	// Through pgx: the extended protocol describes the computed columns
	// with the types the rows carry, and parameters inside them bind.
	conn, err := pgx.Connect(ctx, pgURL(tc, 0))
	if err != nil {
		t.Fatalf("pgx connect: %v", err)
	}
	defer func() { _ = conn.Close(ctx) }()
	if got := describeOIDs(t, ctx, conn, "agg_shapes", `SELECT region, COUNT(*) * 2, ROUND(AVG(amount), 1), AVG(amount)::float8 / 2,
		CASE WHEN SUM(amount) > 50 THEN 'big' ELSE 'small' END, COUNT(*) > 2 FROM sales GROUP BY region`); !sameOIDs(got, []uint32{oidText, oidInt8, oidNumeric, oidFloat8, oidText, oidBool}) {
		t.Fatalf("described: %v", got)
	}
	rows, err := conn.Query(ctx, `SELECT region, SUM(amount) * $1::int8 AS scaled FROM sales GROUP BY region HAVING COUNT(*) > $2 AND SUM(amount) > $3 ORDER BY region`, 2, 2, 40)
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	var got []string
	for rows.Next() {
		var region string
		var scaled int64
		if err := rows.Scan(&region, &scaled); err != nil {
			t.Fatalf("scan: %v", err)
		}
		got = append(got, region+"="+strconv.FormatInt(scaled, 10))
	}
	rows.Close()
	if rows.Err() != nil || strings.Join(got, ",") != "west=140" {
		t.Fatalf("parameters: %v %v", got, rows.Err())
	}
	var ratio float64
	if err := conn.QueryRow(ctx, `SELECT SUM(amount)::float8 / COUNT(*) FROM sales`).Scan(&ratio); err != nil || ratio != 17.5 {
		t.Fatalf("ratio: %v %v", ratio, err)
	}
}
