package testcluster

import (
	"context"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
)

// TestTimeZoneLeftovers (PG-T-001): what #340 left — a literal without
// an offset in a join's ON condition (an OR group included) reads in
// the session's TimeZone; to_jsonb, jsonb_build_object, array_to_string,
// format and concat render a timestamptz in it; the abbreviations of
// PostgreSQL's default set resolve; and a TimeZone given at startup is
// the session's, reported in ParameterStatus.
func TestTimeZoneLeftovers(t *testing.T) {
	tc := Start(t, 1)
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()
	cfg, err := pgx.ParseConfig(pgURL(tc, 0))
	if err != nil {
		t.Fatal(err)
	}
	cfg.DefaultQueryExecMode = pgx.QueryExecModeSimpleProtocol // text results
	cfg.RuntimeParams["timezone"] = "America/New_York"
	conn, err := pgx.ConnectConfig(ctx, cfg)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer conn.Close(ctx)
	if got := conn.PgConn().ParameterStatus("TimeZone"); got != "America/New_York" {
		t.Fatalf("ParameterStatus TimeZone = %q at startup", got)
	}
	for _, q := range []string{
		`CREATE TABLE tzt (id INT PRIMARY KEY, ts TIMESTAMPTZ)`, `CREATE TABLE tzu (id INT PRIMARY KEY, k INT)`,
		`INSERT INTO tzt VALUES (1, '2024-01-01 05:00:00+00')`, `INSERT INTO tzu VALUES (1, 1)`,
	} {
		if _, err := conn.Exec(ctx, q); err != nil {
			t.Fatal(err)
		}
	}
	for q, want := range map[string]string{
		`SHOW TimeZone`: "America/New_York",
		`SELECT tzu.id, tzt.id FROM tzu LEFT JOIN tzt ON tzu.k = tzt.id AND (tzt.ts = '2024-01-01 00:00:00' OR tzt.id = 99)`: "1|1",
		`SELECT tzu.id, tzt.id FROM tzu JOIN tzt ON tzu.k = tzt.id AND (tzt.ts = '2024-01-01 00:00:00' OR tzt.id = 99)`:      "1|1",
		`SELECT tzu.id, tzt.id FROM tzu LEFT JOIN tzt ON tzu.k = tzt.id AND tzt.ts = '2024-01-01 00:00:00'`:                  "1|1",
		`SELECT to_jsonb(ts), jsonb_build_object('a', ts), jsonb_build_array(ts) FROM tzt`:                                   `"2024-01-01T00:00:00-05:00"|{"a":"2024-01-01T00:00:00-05:00"}|["2024-01-01T00:00:00-05:00"]`,
		`SELECT array_to_string(ARRAY[ts], ','), format('%s|%L', ts, ts), concat(ts, '!') FROM tzt`:                          "2024-01-01 00:00:00-05|2024-01-01 00:00:00-05|'2024-01-01 00:00:00-05'|2024-01-01 00:00:00-05!",
		// Abbreviations of PostgreSQL's default set (fixed offsets).
		`SELECT '2024-01-01 00:00:00 SGT'::timestamptz, '2024-01-01 00:00:00 NST'::timestamptz, '2024-01-01 00:00:00 SAST'::timestamptz`: "2023-12-31 11:00:00-05|2023-12-31 22:30:00-05|2023-12-31 17:00:00-05",
		`SELECT '2024-01-01 00:00:00'::timestamp AT TIME ZONE 'ACDT', '2024-01-01 00:00:00'::timestamp AT TIME ZONE 'BRT'`:               "2023-12-31 08:30:00-05|2023-12-31 22:00:00-05",
	} {
		if got, err := queryCells(t, ctx, conn, q); err != nil || got != want {
			t.Errorf("%s = %q, %v; want %q", q, got, err, want)
		}
	}
	// options' -c TimeZone=... is the session's too.
	cfg2, _ := pgx.ParseConfig(pgURL(tc, 0))
	cfg2.RuntimeParams["options"] = "-c TimeZone=Asia/Kolkata"
	conn2, err := pgx.ConnectConfig(ctx, cfg2)
	if err != nil {
		t.Fatalf("connect with options: %v", err)
	}
	defer conn2.Close(ctx)
	if got := conn2.PgConn().ParameterStatus("TimeZone"); got != "Asia/Kolkata" {
		t.Fatalf("ParameterStatus TimeZone = %q with options", got)
	}
}
