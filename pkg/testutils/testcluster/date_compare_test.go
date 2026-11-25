package testcluster

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"

	"github.com/sthorne/datax/pkg/sql"
	"github.com/sthorne/datax/pkg/sql/catalog"
)

// TestDateTimestampComparison: a date compares with a timestamp with
// or without time zone as in PostgreSQL, the date promoted to its
// midnight — in the session's TimeZone against a timestamp with time
// zone — wherever the comparison is made: a WHERE clause (with a span
// on an index or the primary key that agrees), BETWEEN, IN, a select
// item, CASE, a join, a subquery, a prepared statement. A date outside
// the timestamp range is before or after every timestamp, not an error
// (it used to be XX000 "cannot compare DATE with TIMESTAMPTZ").
func TestDateTimestampComparison(t *testing.T) {
	tc := Start(t, 1)
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()
	s := sql.NewSession(tc.Nodes[0].DB(), catalog.NewAccessor())
	waitForDatabases(t, ctx, s)

	execSQL(t, ctx, s, `CREATE TABLE dt (id INT8 PRIMARY KEY, d DATE, ts TIMESTAMPTZ, tn TIMESTAMP)`)
	execSQL(t, ctx, s, `CREATE INDEX dt_ts ON dt (ts)`)
	execSQL(t, ctx, s, `CREATE INDEX dt_d ON dt (d)`)
	execSQL(t, ctx, s, `INSERT INTO dt VALUES
		(3, '2024-01-01', '2024-01-01 00:00:00+00', '2024-01-01 00:00:00'),
		(4, '2024-01-02', '2024-01-01 05:00:00+00', '2024-01-01 05:00:00'),
		(5, '1500-01-01', '2024-01-02 00:00:00+00', '2024-01-02 00:00:00'),
		(6, '3000-01-01', '2024-01-02 00:00:00+00', '2024-01-02 00:00:00')`)
	execSQL(t, ctx, s, `CREATE TABLE dd (id INT8 PRIMARY KEY, d DATE)`)
	execSQL(t, ctx, s, `INSERT INTO dd VALUES (10, '2024-01-01'), (11, '2024-01-02'), (12, '1500-01-01')`)
	execSQL(t, ctx, s, `CREATE TABLE pk (ts TIMESTAMPTZ PRIMARY KEY, v INT8)`)
	execSQL(t, ctx, s, `INSERT INTO pk VALUES ('2024-01-01 00:00:00+00', 1), ('2024-01-01 05:00:00+00', 2)`)

	rows := func(q string) string {
		t.Helper()
		r, serr := trySQL(ctx, s, q)
		if serr != nil {
			t.Errorf("%s: %v", q, serr)
			return ""
		}
		var out []string
		for _, row := range r.Rows {
			var cells []string
			for _, d := range row {
				cells = append(cells, d.Text())
			}
			out = append(out, strings.Join(cells, "|"))
		}
		return strings.Join(out, " ")
	}
	type cmpCase struct{ q, utc, ny string }
	cases := []cmpCase{
		// Scalar comparisons, out-of-range dates included.
		{`SELECT '2024-01-01'::date = '2024-01-01 00:00:00+00'::timestamptz`, "t", "f"},
		{`SELECT '2024-01-01'::date < '2024-01-01 00:00:01'::timestamp`, "t", "t"},
		{`SELECT '1500-01-01'::date < '1700-01-01 00:00:00'::timestamp`, "t", "t"},
		{`SELECT '3000-01-01'::date > '2261-01-01 00:00:00+00'::timestamptz`, "t", "t"},
		{`SELECT '1500-01-01'::date <> '2024-01-01 00:00:00+00'::timestamptz`, "t", "t"},
		{`SELECT '2024-01-01'::date >= '2024-01-01 05:00:00+00'::timestamptz`, "f", "t"},
		{`SELECT '2024-01-01'::date BETWEEN '2023-12-31 23:00:00+00'::timestamptz AND '2024-01-01 00:00:00+00'::timestamptz`, "t", "f"},
		{`SELECT '2024-01-01'::date IN ('2024-01-01 05:00:00+00'::timestamptz, '2024-01-02 00:00:00+00'::timestamptz)`, "f", "t"},
		// WHERE on a timestamp column, through its index.
		{`SELECT id FROM dt WHERE ts >= '2024-01-01'::date ORDER BY id`, "3 4 5 6", "4 5 6"},
		{`SELECT id FROM dt WHERE ts = '2024-01-01'::date ORDER BY id`, "3", "4"},
		{`SELECT id FROM dt WHERE ts > '1500-01-01'::date ORDER BY id`, "3 4 5 6", "3 4 5 6"},
		{`SELECT id FROM dt WHERE ts < '3000-01-01'::date ORDER BY id`, "3 4 5 6", "3 4 5 6"},
		{`SELECT id FROM dt WHERE ts < '1500-01-01'::date ORDER BY id`, "", ""},
		{`SELECT id FROM dt WHERE ts BETWEEN '2024-01-01'::date AND '2024-01-02'::date ORDER BY id`, "3 4 5 6", "4 5 6"},
		{`SELECT id FROM dt WHERE ts IN ('2024-01-01'::date, '2024-01-02'::date) ORDER BY id`, "3 5 6", "4"},
		{`SELECT id FROM dt WHERE ts >= '2024-01-01'::date OR id = 1 ORDER BY id`, "3 4 5 6", "4 5 6"},
		{`SELECT id FROM dt WHERE tn >= '2024-01-01'::date ORDER BY id`, "3 4 5 6", "3 4 5 6"},
		{`SELECT id FROM dt WHERE tn = '2024-01-01'::date ORDER BY id`, "3", "3"},
		// WHERE on a date column, through its index.
		{`SELECT id FROM dt WHERE d <= '2024-01-01 05:00:00'::timestamptz ORDER BY id`, "3 5", "3 5"},
		{`SELECT id FROM dt WHERE d < '2024-01-01 05:00:00'::timestamp ORDER BY id`, "3 5", "3 5"},
		{`SELECT id FROM dt WHERE d IN ('2024-01-01 00:00:00'::timestamp) ORDER BY id`, "3", "3"},
		// Column against column, and computed comparisons.
		{`SELECT id FROM dt WHERE d < ts ORDER BY id`, "5", "5"},
		{`SELECT id FROM dt WHERE d = ts ORDER BY id`, "3", ""},
		{`SELECT id, d = ts, d < ts FROM dt ORDER BY id`, "3|t|f 4|f|f 5|f|t 6|f|f", "3|f|f 4|f|f 5|f|t 6|f|f"},
		{`SELECT CASE WHEN d = ts THEN 'eq' ELSE 'ne' END FROM dt WHERE id = 3`, "eq", "ne"},
		{`SELECT id FROM dt WHERE d IS DISTINCT FROM tn ORDER BY id`, "4 5 6", "4 5 6"},
		// The primary key.
		{`SELECT v FROM pk WHERE ts = '2024-01-01'::date`, "1", "2"},
		{`SELECT v FROM pk WHERE ts IN ('2024-01-01'::date, '1500-01-01'::date)`, "1", "2"},
		{`SELECT v FROM pk WHERE ts >= '2024-01-01'::date ORDER BY v`, "1 2", "2"},
		// Joins, either side looked up, and subqueries.
		{`SELECT dd.id, dt.id FROM dd JOIN dt ON dd.d = dt.ts ORDER BY 1, 2`, "10|3 11|5 11|6", "10|4"},
		{`SELECT dd.id, dt.id FROM dt JOIN dd ON dd.d = dt.ts ORDER BY 1, 2`, "10|3 11|5 11|6", "10|4"},
		{`SELECT dd.id, dt.id FROM dd, dt WHERE dd.d = dt.ts ORDER BY 1, 2`, "10|3 11|5 11|6", "10|4"},
		{`SELECT dd.id, dt.id FROM dd, dt WHERE dd.d = dt.tn ORDER BY 1, 2`, "10|3 11|5 11|6", "10|3 11|5 11|6"},
		{`SELECT dd.id, dt.id FROM dd JOIN dt ON dd.d < dt.ts ORDER BY 1, 2`, "10|4 10|5 10|6 12|3 12|4 12|5 12|6", "10|5 10|6 12|3 12|4 12|5 12|6"},
		{`SELECT id FROM dd WHERE d IN (SELECT ts FROM dt) ORDER BY 1`, "10 11", "10"},
		{`SELECT id FROM dt WHERE ts = (SELECT d FROM dd WHERE id = 10) ORDER BY 1`, "3", "4"},
		{`SELECT id FROM dd WHERE EXISTS (SELECT 1 FROM dt WHERE dt.ts = dd.d) ORDER BY 1`, "10 11", "10"},
		// A write's WHERE.
		{`UPDATE dt SET id = id WHERE ts = '2024-01-01'::date RETURNING id`, "3", "4"},
	}
	for _, zone := range []string{"UTC", "America/New_York"} {
		execSQL(t, ctx, s, `SET TIME ZONE '`+zone+`'`)
		for _, c := range cases {
			want := c.utc
			if zone != "UTC" {
				want = c.ny
			}
			if got := rows(c.q); got != want {
				t.Errorf("%s, in %s: %q, want %q", c.q, zone, got, want)
			}
		}
	}
	// The span a date bounds on a timestamptz index is its midnight in
	// the session's zone; a date out of range bounds nothing.
	for _, c := range []struct{ zone, q, want string }{
		{"UTC", `EXPLAIN SELECT id FROM dt WHERE ts >= '2024-01-01'::date`, "(ts >= 2024-01-01 00:00:00+00)"},
		{"America/New_York", `EXPLAIN SELECT id FROM dt WHERE ts >= '2024-01-01'::date`, "(ts >= 2024-01-01 05:00:00+00)"},
		{"America/New_York", `EXPLAIN SELECT id FROM dt WHERE ts > '1500-01-01'::date`, "full table scan"},
	} {
		execSQL(t, ctx, s, `SET TIME ZONE '`+c.zone+`'`)
		if got := rows(c.q); !strings.Contains(got, c.want) {
			t.Errorf("%s, in %s: %q, want it to show %q", c.q, c.zone, got, c.want)
		}
	}
	execSQL(t, ctx, s, `SET TIME ZONE 'UTC'`)

	// A prepared statement, its plan cached, with the date a parameter.
	conn, err := pgx.Connect(ctx, pgURL(tc, 0))
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close(ctx)
	if _, err := conn.Exec(ctx, `SET TIME ZONE 'America/New_York'`); err != nil {
		t.Fatal(err)
	}
	day := pgtype.Date{Time: time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC), Valid: true}
	old := pgtype.Date{Time: time.Date(1500, 1, 1, 0, 0, 0, 0, time.UTC), Valid: true}
	for i := 0; i < 3; i++ {
		var v int64
		if err := conn.QueryRow(ctx, `SELECT v FROM pk WHERE ts = $1::date`, day).Scan(&v); err != nil || v != 2 {
			t.Errorf("prepared, run %d: %d %v, want 2", i, v, err)
		}
		var n int64
		if err := conn.QueryRow(ctx, `SELECT count(*) FROM pk WHERE ts > $1::date`, old).Scan(&n); err != nil || n != 2 {
			t.Errorf("prepared with a date out of range, run %d: %d %v, want 2", i, n, err)
		}
	}
}
