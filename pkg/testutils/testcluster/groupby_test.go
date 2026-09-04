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
	"github.com/sthorne/datax/pkg/sql/types"
)

// TestGroupByOverPgwire runs grouped queries through a stock pgx client and
// asserts PostgreSQL semantics: NULL group keys form one group, COUNT(col)
// skips NULLs, HAVING filters after aggregation, DISTINCT dedupes, ORDER BY
// applies to the grouped output.
func TestGroupByOverPgwire(t *testing.T) {
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
	mustExec(`CREATE TABLE sales (id INT8 PRIMARY KEY, city TEXT, amount INT8, region TEXT)`)
	mustExec(`INSERT INTO sales VALUES
		(1, 'oslo',   10, 'north'),
		(2, 'oslo',   20, 'north'),
		(3, 'bergen', 30, 'west'),
		(4, NULL,     40, 'west'),
		(5, NULL,     NULL, 'west'),
		(6, 'tromso', 5,  'north')`)

	// GROUP BY with NULL keys: PostgreSQL groups NULLs together.
	// city=NULL → COUNT(*)=2 but COUNT(amount)=1 (NULL amounts skipped).
	rows, err := conn.Query(ctx, `SELECT city, COUNT(*) AS n, COUNT(amount) AS na, SUM(amount) AS total
		FROM sales GROUP BY city ORDER BY n DESC, city`)
	if err != nil {
		t.Fatalf("group query: %v", err)
	}
	type grow struct {
		city  *string
		n, na int64
		total *int64
	}
	var got []grow
	for rows.Next() {
		var g grow
		if err := rows.Scan(&g.city, &g.n, &g.na, &g.total); err != nil {
			t.Fatalf("scan: %v", err)
		}
		got = append(got, g)
	}
	rows.Close()
	if rows.Err() != nil {
		t.Fatal(rows.Err())
	}
	// Expected (ORDER BY n DESC, city ASC NULLS LAST):
	//   oslo   n=2 na=2 total=30
	//   NULL   n=2 na=1 total=40
	//   bergen n=1 na=1 total=30
	//   tromso n=1 na=1 total=5
	if len(got) != 4 {
		t.Fatalf("groups: %+v", got)
	}
	check := func(i int, city string, isNull bool, n, na int64, total int64) {
		t.Helper()
		g := got[i]
		if isNull != (g.city == nil) || (!isNull && *g.city != city) {
			t.Fatalf("row %d city = %v", i, g.city)
		}
		if g.n != n || g.na != na || g.total == nil || *g.total != total {
			t.Fatalf("row %d = %+v", i, g)
		}
	}
	check(0, "oslo", false, 2, 2, 30)
	check(1, "", true, 2, 1, 40)
	check(2, "bergen", false, 1, 1, 30)
	check(3, "tromso", false, 1, 1, 5)

	// HAVING filters post-aggregation; aggregate-call form.
	rows, err = conn.Query(ctx, `SELECT region, SUM(amount) AS total FROM sales
		GROUP BY region HAVING COUNT(*) > 2 AND SUM(amount) >= 70 ORDER BY region`)
	if err != nil {
		t.Fatalf("having query: %v", err)
	}
	var regions []string
	var totals []int64
	for rows.Next() {
		var r string
		var tot int64
		if err := rows.Scan(&r, &tot); err != nil {
			t.Fatalf("scan: %v", err)
		}
		regions, totals = append(regions, r), append(totals, tot)
	}
	rows.Close()
	// north: 3 rows, sum 35 (fails sum); west: 3 rows, sum 70 (passes).
	if len(regions) != 1 || regions[0] != "west" || totals[0] != 70 {
		t.Fatalf("having result: %v %v", regions, totals)
	}

	// HAVING by output alias and group column.
	var city string
	var n int64
	err = conn.QueryRow(ctx, `SELECT city, COUNT(*) AS n FROM sales
		GROUP BY city HAVING n = 2 AND city = 'oslo'`).Scan(&city, &n)
	if err != nil || city != "oslo" || n != 2 {
		t.Fatalf("alias having: %v (%s, %d)", err, city, n)
	}

	// Aggregates without GROUP BY still work (one group), now with ORDER BY
	// rejected only when unknown names are used.
	var cnt, mx int64
	var avg float64
	if err := conn.QueryRow(ctx, `SELECT COUNT(*), MAX(amount), AVG(amount) FROM sales`).Scan(&cnt, &mx, &avg); err != nil {
		t.Fatalf("whole-table agg: %v", err)
	}
	if cnt != 6 || mx != 40 || avg != 21 { // (10+20+30+40+5)/5
		t.Fatalf("agg = %d %d %v", cnt, mx, avg)
	}

	// Aggregates over an empty group set: no GROUP BY → one row; with
	// GROUP BY → zero rows.
	var zc int64
	var zs *int64
	if err := conn.QueryRow(ctx, `SELECT COUNT(*), SUM(amount) FROM sales WHERE id > 100`).Scan(&zc, &zs); err != nil {
		t.Fatalf("empty agg: %v", err)
	}
	if zc != 0 || zs != nil {
		t.Fatalf("empty agg = %d %v", zc, zs)
	}
	rows, err = conn.Query(ctx, `SELECT city, COUNT(*) FROM sales WHERE id > 100 GROUP BY city`)
	if err != nil {
		t.Fatalf("empty grouped: %v", err)
	}
	for rows.Next() {
		t.Fatal("empty grouped query returned a row")
	}
	rows.Close()

	// DISTINCT: degenerate grouping, NULLs collapse to one row.
	rows, err = conn.Query(ctx, `SELECT DISTINCT city FROM sales ORDER BY city`)
	if err != nil {
		t.Fatalf("distinct: %v", err)
	}
	var cities []*string
	for rows.Next() {
		var c *string
		if err := rows.Scan(&c); err != nil {
			t.Fatalf("scan: %v", err)
		}
		cities = append(cities, c)
	}
	rows.Close()
	// bergen, oslo, tromso, NULL (ASC → NULLS LAST).
	if len(cities) != 4 || *cities[0] != "bergen" || *cities[1] != "oslo" || *cities[2] != "tromso" || cities[3] != nil {
		t.Fatalf("distinct cities: %+v", cities)
	}

	// DISTINCT over multiple columns + LIMIT after dedupe.
	rows, err = conn.Query(ctx, `SELECT DISTINCT region, city FROM sales ORDER BY region, city LIMIT 3`)
	if err != nil {
		t.Fatalf("distinct multi: %v", err)
	}
	count := 0
	for rows.Next() {
		count++
	}
	rows.Close()
	if count != 3 {
		t.Fatalf("distinct limit rows = %d", count)
	}

	// Error cases with PostgreSQL codes.
	var pgErr *pgconn.PgError
	if _, err := conn.Exec(ctx, `SELECT city, amount FROM sales GROUP BY city`); err == nil {
		t.Fatal("non-grouped column accepted")
	} else if !errors.As(err, &pgErr) || pgErr.Code != "42803" {
		t.Fatalf("non-grouped column error: %v", err)
	}
	if _, err := conn.Exec(ctx, `SELECT id FROM sales HAVING id > 1`); err == nil {
		t.Fatal("HAVING without grouping accepted")
	} else if !errors.As(err, &pgErr) || pgErr.Code != "42803" {
		t.Fatalf("ungrouped HAVING error: %v", err)
	}
	if _, err := conn.Exec(ctx, `SELECT * FROM sales GROUP BY city`); err == nil {
		t.Fatal("SELECT * with GROUP BY accepted")
	}
}

// TestGroupBySession exercises the session-level corners: grouped output
// ordering, aliasing, and interaction with WHERE-planned scans.
func TestGroupBySession(t *testing.T) {
	tc := Start(t, 3)
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()
	s := sql.NewSession(tc.Nodes[0].DB(), catalog.NewAccessor())

	execSQL(t, ctx, s, `CREATE TABLE kv (k INT PRIMARY KEY, g INT, v INT)`)
	execSQL(t, ctx, s, `INSERT INTO kv VALUES (1,1,10), (2,1,20), (3,2,30), (4,2,40), (5,3,50)`)

	// WHERE narrows the input before grouping (bounded PK scan from SQL1).
	res := execSQL(t, ctx, s, `SELECT g, SUM(v) AS s FROM kv WHERE k > 2 GROUP BY g ORDER BY g`)
	if len(res.Rows) != 2 {
		t.Fatalf("rows: %+v", res.Rows)
	}
	if res.Rows[0][0].I != 2 || res.Rows[0][1].I != 70 || res.Rows[1][0].I != 3 || res.Rows[1][1].I != 50 {
		t.Fatalf("rows: %+v", res.Rows)
	}
	if res.Columns[1].Name != "s" {
		t.Fatalf("columns: %+v", res.Columns)
	}

	// ORDER BY aggregate output, descending, with LIMIT.
	res = execSQL(t, ctx, s, `SELECT g, SUM(v) AS s FROM kv GROUP BY g ORDER BY s DESC LIMIT 2`)
	if len(res.Rows) != 2 || res.Rows[0][0].I != 2 || res.Rows[1][0].I != 3 {
		t.Fatalf("rows: %+v", res.Rows)
	}

	// MIN/MAX/AVG per group.
	res = execSQL(t, ctx, s, `SELECT g, MIN(v) AS lo, MAX(v) AS hi, AVG(v) AS mean FROM kv GROUP BY g ORDER BY g`)
	if len(res.Rows) != 3 {
		t.Fatalf("rows: %+v", res.Rows)
	}
	if res.Rows[0][1].I != 10 || res.Rows[0][2].I != 20 || res.Rows[0][3].Fam != types.Decimal || res.Rows[0][3].Text() != "15" {
		t.Fatalf("group 1: %+v", res.Rows[0]) // AVG of integers is DECIMAL, as in PostgreSQL
	}

	// HAVING that references a hidden aggregate (not projected).
	res = execSQL(t, ctx, s, `SELECT g FROM kv GROUP BY g HAVING COUNT(*) = 2 ORDER BY g`)
	if len(res.Rows) != 2 || res.Rows[0][0].I != 1 || res.Rows[1][0].I != 2 {
		t.Fatalf("hidden having: %+v", res.Rows)
	}

	// GROUP BY column not in select list is fine.
	res = execSQL(t, ctx, s, `SELECT COUNT(*) AS n FROM kv GROUP BY g ORDER BY n`)
	if len(res.Rows) != 3 {
		t.Fatalf("rows: %+v", res.Rows)
	}

	// EXPLAIN is unchanged by grouping (post-fetch) and never claims limit
	// pushdown for grouped/distinct queries.
	if p := explainPlan(t, ctx, s, `SELECT g, COUNT(*) AS n FROM kv GROUP BY g LIMIT 1`); p != "full table scan" {
		t.Fatalf("plan: %s", p)
	}
	if p := explainPlan(t, ctx, s, `SELECT DISTINCT g FROM kv LIMIT 1`); p != "full table scan" {
		t.Fatalf("plan: %s", p)
	}

	// Error codes.
	if _, serr := trySQL(ctx, s, `SELECT v FROM kv GROUP BY g`); serr == nil || serr.Code != sql.CodeGrouping {
		t.Fatalf("non-grouped column: %v", serr)
	}
	if _, serr := trySQL(ctx, s, `SELECT g FROM kv GROUP BY g FOR UPDATE`); serr == nil || serr.Code != sql.CodeFeatureNotSupported {
		t.Fatalf("grouped FOR UPDATE: %v", serr)
	}
	if _, serr := trySQL(ctx, s, `SELECT DISTINCT g FROM kv FOR UPDATE`); serr == nil || serr.Code != sql.CodeFeatureNotSupported {
		t.Fatalf("distinct FOR UPDATE: %v", serr)
	}
	if _, serr := trySQL(ctx, s, `SELECT g, COUNT(*) FROM kv GROUP BY g ORDER BY nope`); serr == nil || serr.Code != sql.CodeUndefinedColumn {
		t.Fatalf("unknown order column: %v", serr)
	}
}
