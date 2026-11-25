package testcluster

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgtype"

	"github.com/sthorne/datax/pkg/sql"
	"github.com/sthorne/datax/pkg/sql/catalog"
)

// TestDateToTimestampOutOfRange (PG-T-005): a date, or a computed
// timestamp, outside the int64-nanosecond range (1678 to 2261) is
// refused with 22008 wherever it would become a timestamp — a cast, an
// assignment, date arithmetic, a date/time function — instead of
// wrapping into a wrong value ('1500-01-01'::date::timestamptz used to
// be 2084-07-20 23:34:33.709552+00).
func TestDateToTimestampOutOfRange(t *testing.T) {
	tc := Start(t, 1)
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	s := sql.NewSession(tc.Nodes[0].DB(), catalog.NewAccessor())
	waitForDatabases(t, ctx, s)

	cases := []exprCase{
		{expr: `'1678-01-01'::date::timestamp`, want: "1678-01-01 00:00:00"},
		{expr: `'2261-12-31'::date::timestamptz`, want: "2261-12-31 00:00:00+00"},
		{expr: `'1500-01-01'::date::timestamptz`, code: "22008"},
		{expr: `'1500-01-01'::date::timestamp`, code: "22008"},
		{expr: `'3000-01-01'::date::timestamp`, code: "22008"},
		{expr: `CAST('2262-01-01'::date AS timestamptz)`, code: "22008"},
		{expr: `'1500-01-01'::date + '01:00'::time`, code: "22008"},
		{expr: `'1500-01-01'::date + '1 day'::interval`, code: "22008"},
		{expr: `'1500-01-01'::date - '2000-01-01'::timestamp`, code: "22008"},
		{expr: `'2200-01-01'::timestamptz + '100 years'::interval`, code: "22008"},
		{expr: `'1700-01-01'::timestamptz - '100 years'::interval`, code: "22008"},
		{expr: `'2261-12-31'::timestamp + '1 day'::interval`, code: "22008"},
		{expr: `to_timestamp(1e12)`, code: "22008"},
		{expr: `to_timestamp(-1e300)`, code: "22008"},
		{expr: `to_timestamp('3000-01-01', 'YYYY-MM-DD')`, code: "22008"},
		{expr: `make_timestamp(3000, 1, 1, 0, 0, 0)`, code: "22008"},
		{expr: `date_trunc('day', '1500-01-01'::date)`, code: "22008"},
		{expr: `timezone('UTC', '1500-01-01'::date)`, code: "22008"},
		{expr: `generate_series('1500-01-01'::date, '1500-01-03'::date, '1 day'::interval)`, code: "22008"},
	}
	runExprCases(t, ctx, s, cases)
	// The same in a session zone, where a date becomes a timestamp with
	// time zone at its midnight there.
	execSQL(t, ctx, s, `SET TIME ZONE 'America/New_York'`)
	runExprCases(t, ctx, s, []exprCase{
		{expr: `'2024-01-01'::date::timestamptz`, want: "2024-01-01 05:00:00+00"},
		{expr: `'1500-01-01'::date::timestamptz`, code: "22008"},
		{expr: `'2200-01-01'::timestamptz + '100 years'::interval`, code: "22008"},
		{expr: `generate_series('1500-01-01'::date, '1500-01-03'::date, '1 day'::interval)`, code: "22008"},
	})
	execSQL(t, ctx, s, `SET TIME ZONE 'UTC'`)

	// Assignment: a date stored into a timestamp column.
	execSQL(t, ctx, s, `CREATE TABLE dts (id INT8 PRIMARY KEY, ts TIMESTAMPTZ, tn TIMESTAMP)`)
	for _, q := range []string{
		`INSERT INTO dts (id, ts) VALUES (1, '1500-01-01'::date)`,
		`INSERT INTO dts (id, tn) VALUES (2, '3000-01-01'::date)`,
		`INSERT INTO dts (id, ts) SELECT 3, d FROM (VALUES ('1500-01-01'::date)) v(d)`,
	} {
		if _, serr := trySQL(ctx, s, q); serr == nil || serr.Code != "22008" {
			t.Errorf("%s: %v, want 22008", q, serr)
		}
	}
	execSQL(t, ctx, s, `INSERT INTO dts VALUES (4, '2024-01-01'::date, '2024-01-01'::date)`)
	execSQL(t, ctx, s, `UPDATE dts SET ts = '2024-01-02'::date WHERE id = 4`)
	if _, serr := trySQL(ctx, s, `UPDATE dts SET ts = '1500-01-01'::date WHERE id = 4`); serr == nil || serr.Code != "22008" {
		t.Errorf("UPDATE to a date out of range: %v, want 22008", serr)
	}
	for _, q := range []string{
		`ALTER TABLE dts SPLIT AT VALUES (10) WITH EXPIRATION '3000-01-01'::date`,
		`ALTER TABLE dts SPLIT AT VALUES (10) WITH EXPIRATION INTERVAL '1000 years'`,
	} {
		if _, serr := trySQL(ctx, s, q); serr == nil || serr.Code != "22008" {
			t.Errorf("%s: %v, want 22008", q, serr)
		}
	}
	if r := execSQL(t, ctx, s, `SELECT ts, tn FROM dts`); len(r.Rows) != 1 || r.Rows[0][0].Text() != "2024-01-02 00:00:00+00" || r.Rows[0][1].Text() != "2024-01-01 00:00:00" {
		t.Errorf("rows: %v", r.Rows)
	}

	// A date sent in binary (pgx's default) to a cast and to an
	// assignment.
	conn, err := pgx.Connect(ctx, pgURL(tc, 0))
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close(ctx)
	old := pgtype.Date{Time: time.Date(1500, 1, 1, 0, 0, 0, 0, time.UTC), Valid: true}
	for _, q := range []string{`SELECT $1::date::timestamptz`, `INSERT INTO dts (id, ts) VALUES (5, $1::date)`} {
		_, err := conn.Exec(ctx, q, old)
		var pe *pgconn.PgError
		if !errors.As(err, &pe) || pe.Code != "22008" {
			t.Errorf("%s with a binary date of 1500: %v, want 22008", q, err)
		}
	}
}
