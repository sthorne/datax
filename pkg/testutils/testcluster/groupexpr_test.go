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

// TestGroupByExpressions covers issue #336: GROUP BY expressions
// (positions and output names of expression items too), select items
// over grouped expressions, and ROLLUP, CUBE and GROUPING SETS with
// GROUPING().
func TestGroupByExpressions(t *testing.T) {
	tc := Start(t, 1)
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()
	s := sql.NewSession(tc.Nodes[0].DB(), catalog.NewAccessor())

	execSQL(t, ctx, s, `CREATE TABLE events (id INT8 PRIMARY KEY, ts TIMESTAMP, kind TEXT, n INT8)`)
	execSQL(t, ctx, s, `INSERT INTO events VALUES
		(1, '2024-01-01 10:00', 'a', 1), (2, '2024-01-01 12:00', 'b', 2), (3, '2024-01-02 09:00', 'a', 3),
		(4, '2024-01-02 18:00', 'a', 4), (5, '2024-01-03 01:00', NULL, 5)`)
	execSQL(t, ctx, s, `CREATE TABLE kinds (kind TEXT PRIMARY KEY, label TEXT)`)
	execSQL(t, ctx, s, `INSERT INTO kinds VALUES ('a', 'alpha'), ('b', 'beta')`)

	runGroupedCases(t, ctx, s, []groupedCase{
		// Expressions as grouping keys, by expression, position or name.
		{sql: `SELECT date_trunc('day', ts)::date AS day, COUNT(*) FROM events GROUP BY date_trunc('day', ts) ORDER BY day`,
			want: "2024-01-01|2; 2024-01-02|2; 2024-01-03|1"},
		{sql: `SELECT n % 2 AS parity, SUM(n) FROM events GROUP BY n % 2 ORDER BY 1`, want: "0|6; 1|9"},
		{sql: `SELECT n % 2, COUNT(*) FROM events GROUP BY 1 ORDER BY 1`, want: "0|2; 1|3"},
		{sql: `SELECT n % 2 AS parity, COUNT(*) FROM events GROUP BY parity ORDER BY parity DESC`, want: "1|3; 0|2"},
		{sql: `SELECT (n % 2) * 10 + COUNT(*) FROM events GROUP BY n % 2 ORDER BY 1`, want: "2; 13"},
		{sql: `SELECT upper(kind) FROM events GROUP BY upper(kind) HAVING upper(kind) <> 'B' ORDER BY upper(kind) NULLS FIRST`, want: "A"},
		// An input column of the alias's name wins in GROUP BY.
		{sql: `SELECT n % 2 AS n, COUNT(*) FROM events GROUP BY n ORDER BY 2, 1 LIMIT 1`, want: "0|1"},
		// Grouped expressions over a join.
		{sql: `SELECT lower(k.label), COUNT(*) FROM events e JOIN kinds k ON k.kind = e.kind GROUP BY lower(k.label) ORDER BY 1`,
			want: "alpha|3; beta|1"},
		// Not grouped: the column alone is refused (42803).
		{sql: `SELECT n FROM events GROUP BY n % 2`, code: "42803"},
		{sql: `SELECT n % 3 FROM events GROUP BY n % 2`, code: "42803"},
		{sql: `SELECT COUNT(*) FROM events GROUP BY COUNT(*)`, code: "42803"},
		{sql: `SELECT COUNT(*) FROM events GROUP BY 1`, code: "42803"},
		// ROLLUP: the union of (kind, n % 2), (kind) and (); rolled-up
		// keys are NULL, which GROUPING() tells from a NULL key.
		{sql: `SELECT kind, n % 2 AS p, SUM(n), GROUPING(kind, n % 2) AS g FROM events GROUP BY ROLLUP (kind, n % 2)
			ORDER BY g, kind NULLS LAST, p`,
			want: "a|0|4|0; a|1|4|0; b|0|2|0; NULL|1|5|0; a|NULL|8|1; b|NULL|2|1; NULL|NULL|5|1; NULL|NULL|15|3"},
		{sql: `SELECT COUNT(*) FROM events GROUP BY CUBE (kind, n % 2) ORDER BY 1`, want: "1; 1; 1; 1; 1; 2; 2; 3; 3; 5"},
		{sql: `SELECT kind, n % 2, COUNT(*), GROUPING(kind), GROUPING(n % 2) FROM events
			GROUP BY GROUPING SETS ((kind), (n % 2), ()) ORDER BY 4, 5, 1, 2`,
			want: "a|NULL|3|0|1; b|NULL|1|0|1; NULL|NULL|1|0|1; NULL|0|2|1|0; NULL|1|3|1|0; NULL|NULL|5|1|1"},
		{sql: `SELECT kind, n % 2, SUM(n) FROM events WHERE kind IS NOT NULL GROUP BY kind, ROLLUP (n % 2) ORDER BY 1, 2`,
			want: "a|0|4; a|1|4; a|NULL|8; b|0|2; b|NULL|2"},
		{sql: `SELECT kind, SUM(n) FROM events GROUP BY ROLLUP (kind) HAVING GROUPING(kind) = 1`, want: "NULL|15"},
		{sql: `SELECT kind, COUNT(*) FROM events GROUP BY GROUPING SETS (kind, ()) ORDER BY GROUPING(kind), kind`,
			want: "a|3; b|1; NULL|1; NULL|5"},
		// The empty grouping set gives its row over no input, as the
		// grand total of no rows.
		{sql: `SELECT kind, COUNT(*) FROM events WHERE id > 100 GROUP BY ROLLUP (kind)`, want: "NULL|0"},
		{sql: `SELECT COUNT(*) FROM events WHERE id > 100 GROUP BY kind`, want: ""},
		// Over a join, and GROUPING() of a key that is not one: 42803.
		{sql: `SELECT k.label, SUM(e.n) FROM events e JOIN kinds k ON k.kind = e.kind GROUP BY ROLLUP (k.label) ORDER BY 1 NULLS LAST`,
			want: "alpha|8; beta|2; NULL|10"},
		{sql: `SELECT GROUPING(n) FROM events GROUP BY kind`, code: "42803"},
		// Over a derived table.
		{sql: `SELECT d.kind, COUNT(*) FROM (SELECT * FROM events) d GROUP BY ROLLUP (d.kind) ORDER BY 1 NULLS LAST, 2`,
			want: "a|3; b|1; NULL|1; NULL|5"},
		{sql: `SELECT n % 2 AS p, COUNT(*) FROM (SELECT * FROM events) d GROUP BY p ORDER BY p`, want: "0|2; 1|3"},
	})

	if p := explainPlan(t, ctx, s, `SELECT kind, n % 2, COUNT(*) FROM events GROUP BY ROLLUP (kind, n % 2)`); p != "full table scan" {
		t.Fatalf("plan: %s", p)
	}

	// Describe: a grouped expression describes as the same expression
	// does ungrouped; GROUPING() is an integer.
	conn, err := pgx.Connect(ctx, pgURL(tc, 0))
	if err != nil {
		t.Fatalf("pgx connect: %v", err)
	}
	defer func() { _ = conn.Close(ctx) }()
	plain := describeOIDs(t, ctx, conn, "plain_ts", `SELECT date_trunc('day', ts) FROM events`)
	if got := describeOIDs(t, ctx, conn, "trunc_ts", `SELECT date_trunc('day', ts), n % 2, GROUPING(kind), COUNT(*)
		FROM events GROUP BY ROLLUP (date_trunc('day', ts), n % 2, kind)`); !sameOIDs(got, []uint32{plain[0], oidInt8, oidInt8, oidInt8}) {
		t.Fatalf("described: %v (ts is %v)", got, plain)
	}
	var total int64
	if err := conn.QueryRow(ctx, `SELECT SUM(n) FROM events GROUP BY CUBE (kind) HAVING GROUPING(kind) = $1::int8`, 1).Scan(&total); err != nil || total != 15 {
		t.Fatalf("cube total: %d %v", total, err)
	}
}

// TestDistinctOn covers issue #337: SELECT DISTINCT ON (exprs) keeps the
// first row of each group in ORDER BY order; the leading ORDER BY terms
// must be the DISTINCT ON expressions (42P10); over joins, grouped
// queries, derived tables and with LIMIT / OFFSET.
func TestDistinctOn(t *testing.T) {
	tc := Start(t, 1)
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()
	s := sql.NewSession(tc.Nodes[0].DB(), catalog.NewAccessor())

	execSQL(t, ctx, s, `CREATE TABLE scores (id INT8 PRIMARY KEY, player TEXT, game TEXT, score INT8)`)
	execSQL(t, ctx, s, `INSERT INTO scores VALUES (1, 'ann', 'g1', 10), (2, 'ann', 'g2', 30), (3, 'bob', 'g1', 20),
		(4, 'bob', 'g2', 5), (5, 'cy', 'g1', 7)`)
	execSQL(t, ctx, s, `CREATE TABLE players (name TEXT PRIMARY KEY, team TEXT)`)
	execSQL(t, ctx, s, `INSERT INTO players VALUES ('ann', 'red'), ('bob', 'blue'), ('cy', 'red')`)

	runGroupedCases(t, ctx, s, []groupedCase{
		{sql: `SELECT DISTINCT ON (player) player, game, score FROM scores ORDER BY player, score DESC`,
			want: "ann|g2|30; bob|g1|20; cy|g1|7"},
		{sql: `SELECT DISTINCT ON (player) player, score FROM scores ORDER BY player, score DESC LIMIT 2`, want: "ann|30; bob|20"},
		{sql: `SELECT DISTINCT ON (player) player, score FROM scores ORDER BY player, score LIMIT 2 OFFSET 1`, want: "bob|5; cy|7"},
		{sql: `SELECT DISTINCT ON (player) player FROM scores`, want: "ann; bob; cy"},
		// Expressions, positions and output names.
		{sql: `SELECT DISTINCT ON (score % 2) score % 2 AS parity, id FROM scores ORDER BY score % 2, id`, want: "0|1; 1|4"},
		{sql: `SELECT DISTINCT ON (1) score % 2, id FROM scores ORDER BY 1, id DESC`, want: "0|3; 1|5"},
		{sql: `SELECT DISTINCT ON (p) player AS p, score FROM scores ORDER BY p, score`, want: "ann|10; bob|5; cy|7"},
		{sql: `SELECT DISTINCT ON (game, player) game, player, score FROM scores ORDER BY player, game, score`,
			want: "g1|ann|10; g2|ann|30; g1|bob|20; g2|bob|5; g1|cy|7"},
		// Over a join, a grouped query and a derived table.
		{sql: `SELECT DISTINCT ON (p.team) p.team, s.player, s.score FROM scores s JOIN players p ON p.name = s.player
			ORDER BY p.team, s.score DESC`, want: "blue|bob|20; red|ann|30"},
		{sql: `SELECT DISTINCT ON (player) player, game, SUM(score) FROM scores GROUP BY player, game ORDER BY player, SUM(score)`,
			want: "ann|g1|10; bob|g2|5; cy|g1|7"},
		{sql: `SELECT DISTINCT ON (team) team, COUNT(*) FROM scores s JOIN players p ON p.name = s.player GROUP BY team, s.game ORDER BY team, COUNT(*) DESC, s.game`,
			want: "blue|1; red|2"},
		{sql: `SELECT DISTINCT ON (player) player, score FROM (SELECT * FROM scores WHERE score > 5) d ORDER BY player, score`,
			want: "ann|10; bob|20; cy|7"},
		// The leading ORDER BY terms must be the DISTINCT ON expressions.
		{sql: `SELECT DISTINCT ON (player) player, score FROM scores ORDER BY score`, code: "42P10"},
		{sql: `SELECT DISTINCT ON (player, game) player FROM scores ORDER BY player, score, game`, code: "42P10"},
		{sql: `SELECT DISTINCT ON (s.player) s.player FROM scores s JOIN players p ON p.name = s.player ORDER BY p.team`, code: "42P10"},
		{sql: `SELECT DISTINCT ON (player) player FROM scores ORDER BY player FOR UPDATE`, code: "0A000"},
	})

	if p := explainPlan(t, ctx, s, `SELECT DISTINCT ON (player) player, score FROM scores ORDER BY player, score LIMIT 1`); p == "" {
		t.Fatal("empty plan")
	}

	conn, err := pgx.Connect(ctx, pgURL(tc, 0))
	if err != nil {
		t.Fatalf("pgx connect: %v", err)
	}
	defer func() { _ = conn.Close(ctx) }()
	if got := describeOIDs(t, ctx, conn, "distinct_on", `SELECT DISTINCT ON (player) player, score * 2, score > 9 FROM scores ORDER BY player, score DESC`); !sameOIDs(got, []uint32{oidText, oidInt8, oidBool}) {
		t.Fatalf("described: %v", got)
	}
	var player string
	var score int64
	if err := conn.QueryRow(ctx, `SELECT DISTINCT ON (player) player, score FROM scores WHERE player <> $1 ORDER BY player, score DESC`, "ann").Scan(&player, &score); err != nil || player != "bob" || score != 20 {
		t.Fatalf("prepared: %s %d %v", player, score, err)
	}
}
