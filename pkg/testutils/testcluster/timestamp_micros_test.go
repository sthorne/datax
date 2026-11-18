package testcluster

import (
	"context"
	"regexp"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
)

// TestTimestampMicrosecondPrecision (PG-F-019): now(),
// clock_timestamp() and the values written from them keep
// PostgreSQL's microsecond precision: their text has at most six
// fractional digits, jsonb renders them as PostgreSQL does, a stored
// value equals its own text read back, and the binary form agrees
// with the text.
func TestTimestampMicrosecondPrecision(t *testing.T) {
	tc := Start(t, 1)
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()
	cfg, err := pgx.ParseConfig(pgURL(tc, 0))
	if err != nil {
		t.Fatal(err)
	}
	cfg.DefaultQueryExecMode = pgx.QueryExecModeSimpleProtocol // text results
	text, err := pgx.ConnectConfig(ctx, cfg)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer text.Close(ctx)
	bin, err := pgx.Connect(ctx, pgURL(tc, 0)) // binary results
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer bin.Close(ctx)

	ts := regexp.MustCompile(`^\d{4}-\d\d-\d\d \d\d:\d\d:\d\d(\.\d{1,6})?\+00$`)
	js := regexp.MustCompile(`^\{"a":"\d{4}-\d\d-\d\dT\d\d:\d\d:\d\d(\.\d{1,6})?\+00:00"\}$`)
	if _, err := text.Exec(ctx, `CREATE TABLE tsm (id INT PRIMARY KEY, at TIMESTAMPTZ, wall TIMESTAMP)`); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 20; i++ {
		got, err := queryCells(t, ctx, text, `SELECT now(), now()::text, clock_timestamp(), current_timestamp, statement_timestamp()`)
		if err != nil {
			t.Fatal(err)
		}
		for _, c := range regexp.MustCompile(`\|`).Split(got, -1) {
			if !ts.MatchString(c) {
				t.Fatalf("timestamp text %q has more than microseconds", c)
			}
		}
		if got, err = queryCells(t, ctx, text, `SELECT jsonb_build_object('a', clock_timestamp())`); err != nil || !js.MatchString(got) {
			t.Fatalf("jsonb of clock_timestamp() = %q, %v", got, err)
		}
		if _, err := text.Exec(ctx, `INSERT INTO tsm VALUES ($1, clock_timestamp(), clock_timestamp())`, i); err != nil {
			t.Fatal(err)
		}
	}
	// Each stored value equals its text read back; the binary value is
	// the text's instant, to the microsecond.
	rows, err := queryCells(t, ctx, text, `SELECT id, at::text, wall::text FROM tsm ORDER BY id`)
	if err != nil {
		t.Fatal(err)
	}
	for _, row := range regexp.MustCompile(`;`).Split(rows, -1) {
		cells := regexp.MustCompile(`\|`).Split(row, -1)
		if !ts.MatchString(cells[1]) {
			t.Fatalf("stored timestamptz renders %q", cells[1])
		}
		n, err := queryCells(t, ctx, text, `SELECT count(*) FROM tsm WHERE id = $1 AND at = $2::timestamptz AND wall = $3::timestamp`, cells[0], cells[1], cells[2])
		if err != nil || n != "1" {
			t.Fatalf("row %s: stored value != its text %q / %q (%q, %v)", cells[0], cells[1], cells[2], n, err)
		}
		var at time.Time
		if err := bin.QueryRow(ctx, `SELECT at FROM tsm WHERE id = $1`, cells[0]).Scan(&at); err != nil {
			t.Fatal(err)
		}
		want, err := time.Parse("2006-01-02 15:04:05.999999-07", cells[1])
		if err != nil || !at.Equal(want) || at.Nanosecond()%1000 != 0 {
			t.Fatalf("row %s: binary %v, text %q (%v)", cells[0], at, cells[1], err)
		}
	}
	// Input rounds to microseconds, as PostgreSQL's does.
	for q, want := range map[string]string{
		`SELECT '2024-01-01 00:00:00.1234567+00'::timestamptz`:                            "2024-01-01 00:00:00.123457+00",
		`SELECT '2024-01-01 23:59:59.9999996'::timestamp`:                                 "2024-01-02 00:00:00",
		`SELECT to_timestamp(1.0000004)`:                                                  "1970-01-01 00:00:01+00",
		`SELECT jsonb_build_object('a', '2024-01-01 00:00:00.5+00'::timestamptz)`:         `{"a":"2024-01-01T00:00:00.5+00:00"}`,
		`SELECT to_jsonb('2024-01-01 00:00:00'::timestamp), to_jsonb('2024-01-01'::date)`: `"2024-01-01T00:00:00"|"2024-01-01"`,
	} {
		if got, err := queryCells(t, ctx, text, q); err != nil || got != want {
			t.Errorf("%s = %q, %v; want %q", q, got, err, want)
		}
	}
}
