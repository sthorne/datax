package testcluster

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/sthorne/datax/pkg/sql"
	"github.com/sthorne/datax/pkg/sql/catalog"
)

// TestTimeZones (issue #340): the session's TimeZone governs what
// PostgreSQL has it govern — a timestamp with time zone read from text
// without an offset (literals, text parameters, INSERT values), its
// rendering as text (the wire, ::text, ||, to_char), its calendar
// fields (::date, ::time, ::timestamp, date_trunc, extract,
// current_date) and day and month arithmetic across DST — while the
// stored instant and the binary format stay UTC. AT TIME ZONE and
// timezone() convert both ways with IANA names, abbreviations, POSIX
// offsets (whose sign is inverted) and intervals.
func TestTimeZones(t *testing.T) {
	tc := Start(t, 1)
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()
	s := sql.NewSession(tc.Nodes[0].DB(), catalog.NewAccessor())
	waitForDatabases(t, ctx, s)
	execSQL(t, ctx, s, `CREATE TABLE tz (id INT8 PRIMARY KEY, at TIMESTAMPTZ, wall TIMESTAMP)`)

	conn, err := pgx.Connect(ctx, pgURL(tc, 0))
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close(ctx)
	exec := func(q string, args ...any) {
		t.Helper()
		if _, err := conn.Exec(ctx, q, args...); err != nil {
			t.Fatalf("%s: %v", q, err)
		}
	}
	check := func(q, want string) {
		t.Helper()
		for _, mode := range []pgx.QueryExecMode{pgx.QueryExecModeSimpleProtocol, pgx.QueryExecModeExec} {
			got, err := queryCells(t, ctx, conn, q, mode)
			if err != nil || got != want {
				t.Errorf("%v %s = %q %v, want %q", mode, q, got, err, want)
			}
		}
	}
	instant := func(q string, want time.Time) {
		t.Helper()
		var got time.Time
		if err := conn.QueryRow(ctx, q).Scan(&got); err != nil || !got.Equal(want) {
			t.Errorf("%s (binary) = %v %v, want %v", q, got, err, want)
		}
	}
	utc := func(v string) time.Time {
		tm, err := time.Parse("2006-01-02 15:04:05", v)
		if err != nil {
			t.Fatal(err)
		}
		return tm
	}

	// Conversions name their zone: the same under any TimeZone.
	for _, zone := range []string{"UTC", "Asia/Kolkata"} {
		exec(`SET TimeZone = '` + zone + `'`)
		check(`SELECT '2024-07-01 12:00:00+00'::timestamptz AT TIME ZONE 'America/New_York'`, "2024-07-01 08:00:00")
		check(`SELECT timezone('Europe/Paris', '2024-01-15 12:00:00+00'::timestamptz)`, "2024-01-15 13:00:00")
		check(`SELECT ('2024-07-01 12:00:00'::timestamp AT TIME ZONE 'America/New_York') = '2024-07-01 16:00:00+00'::timestamptz`, "t")
		check(`SELECT '2024-07-01 12:00:00+00'::timestamptz AT TIME ZONE 'UTC+5'`, "2024-07-01 07:00:00")
		check(`SELECT '2024-07-01 12:00:00+00'::timestamptz AT TIME ZONE INTERVAL '-08:00'`, "2024-07-01 04:00:00")
		check(`SELECT '2024-07-01 12:00:00+00'::timestamptz AT TIME ZONE 'pst'`, "2024-07-01 04:00:00")
	}
	exec(`SET TimeZone = 'UTC'`)
	// The spring-forward gap and the fall-back overlap, as PostgreSQL
	// resolves them.
	check(`SELECT '2024-03-10 02:30:00'::timestamp AT TIME ZONE 'America/New_York'`, "2024-03-10 07:30:00+00")
	check(`SELECT '2024-11-03 01:30:00'::timestamp AT TIME ZONE 'America/New_York'`, "2024-11-03 06:30:00+00")
	check(`SELECT '2024-11-03 05:30:00+00'::timestamptz AT TIME ZONE 'America/New_York', '2024-11-03 06:30:00+00'::timestamptz AT TIME ZONE 'America/New_York'`, "2024-11-03 01:30:00|2024-11-03 01:30:00")
	if _, err := conn.Exec(ctx, `SELECT now() AT TIME ZONE 'Mars/Olympus'`); pgCode(err) != "22023" {
		t.Errorf("an unknown zone: %v, want 22023", err)
	}
	// The result types: timestamptz AT TIME ZONE is a timestamp (1114),
	// the reverse a timestamptz (1184), each decoding in binary.
	sd, err := conn.Prepare(ctx, "atz", `SELECT now() AT TIME ZONE 'UTC', '2024-01-01'::timestamp AT TIME ZONE 'UTC'`)
	if err != nil || sd.Fields[0].DataTypeOID != 1114 || sd.Fields[1].DataTypeOID != 1184 {
		t.Fatalf("AT TIME ZONE result types: %+v %v", sd, err)
	}
	instant(`SELECT '2024-07-01 12:00:00+00'::timestamptz AT TIME ZONE 'America/New_York'`, utc("2024-07-01 08:00:00"))
	instant(`SELECT '2024-07-01 12:00:00'::timestamp AT TIME ZONE 'America/New_York'`, utc("2024-07-01 16:00:00"))

	// Under America/New_York.
	exec(`SET TimeZone = 'America/New_York'`)
	// Input without an offset is New York time; with one, or with a
	// zone name, it is what it says.
	check(`SELECT '2024-07-01 12:00:00'::timestamptz`, "2024-07-01 12:00:00-04")
	check(`SELECT '2024-01-15 12:00:00'::timestamptz`, "2024-01-15 12:00:00-05")
	check(`SELECT '2024-07-01 12:00:00+00'::timestamptz`, "2024-07-01 08:00:00-04")
	check(`SELECT '2024-07-01 12:00:00 Europe/Paris'::timestamptz`, "2024-07-01 06:00:00-04")
	check(`SELECT '2024-03-10 02:30:00'::timestamptz, '2024-11-03 01:30:00'::timestamptz`, "2024-03-10 03:30:00-04|2024-11-03 01:30:00-05")
	instant(`SELECT '2024-07-01 12:00:00'::timestamptz`, utc("2024-07-01 16:00:00"))
	// Stored values: literals and text parameters without an offset.
	exec(`INSERT INTO tz VALUES (1, '2024-07-01 22:30:00', '2024-07-01 22:30:00')`)
	exec(`INSERT INTO tz VALUES ($1, $2, $3)`, int64(2), "2024-01-15 09:00:00", "2024-01-15 09:00:00")
	exec(`INSERT INTO tz VALUES (3, $1, NULL)`, time.Date(2024, 3, 10, 12, 0, 0, 0, time.UTC))
	instant(`SELECT at FROM tz WHERE id = 1`, utc("2024-07-02 02:30:00"))
	instant(`SELECT at FROM tz WHERE id = 2`, utc("2024-01-15 14:00:00"))
	check(`SELECT id, at, wall FROM tz ORDER BY id`, "1|2024-07-01 22:30:00-04|2024-07-01 22:30:00;2|2024-01-15 09:00:00-05|2024-01-15 09:00:00;3|2024-03-10 08:00:00-04|NULL")
	if _, err := conn.PgConn().CopyFrom(ctx, strings.NewReader("4\t2024-07-01 12:00:00\t\\N\n"), "COPY tz FROM STDIN"); err != nil {
		t.Fatalf("COPY: %v", err)
	}
	instant(`SELECT at FROM tz WHERE id = 4`, utc("2024-07-01 16:00:00"))
	check(`SELECT id FROM tz WHERE at = '2024-07-01 22:30:00'`, "1")
	check(`SELECT id FROM tz WHERE at >= '2024-07-01' AND at < '2024-07-02' ORDER BY id`, "1;4")
	check(`SELECT id FROM tz WHERE at IN ('2024-01-15 09:00:00', '2000-01-01')`, "2")
	// Rendering as text and calendar fields are New York's.
	check(`SELECT at::text, at::date, at::time, at::timestamp FROM tz WHERE id = 1`, "2024-07-01 22:30:00-04|2024-07-01|22:30:00|2024-07-01 22:30:00")
	check(`SELECT 'at ' || at FROM tz WHERE id = 2`, "at 2024-01-15 09:00:00-05")
	check(`SELECT to_char(at, 'YYYY-MM-DD HH24:MI TZ OF') FROM tz WHERE id = 1`, "2024-07-01 22:30 EDT -04")
	check(`SELECT date_trunc('day', at), date_trunc('hour', at), date_trunc('month', at) FROM tz WHERE id = 1`, "2024-07-01 00:00:00-04|2024-07-01 22:00:00-04|2024-07-01 00:00:00-04")
	check(`SELECT extract(hour FROM at), extract(day FROM at), date_part('dow', at), extract(timezone FROM at), extract(epoch FROM at) FROM tz WHERE id = 1`, "22|1|1|-14400|1719887400")
	check(`SELECT date_trunc('day', wall), extract(hour FROM wall) FROM tz WHERE id = 1`, "2024-07-01 00:00:00|22")
	check(`SELECT current_date = (now() AT TIME ZONE 'America/New_York')::date`, "t")
	check(`SELECT '2024-07-01'::date::timestamptz`, "2024-07-01 00:00:00-04")
	// A day across DST is 23 or 25 hours; the clock part elapses.
	check(`SELECT '2024-03-09 12:00:00'::timestamptz + interval '1 day', '2024-03-09 12:00:00'::timestamptz + interval '24 hours'`, "2024-03-10 12:00:00-04|2024-03-10 13:00:00-04")
	check(`SELECT '2024-11-03 12:00:00'::timestamptz - interval '1 day'`, "2024-11-02 12:00:00-04")
	check(`SELECT * FROM generate_series('2024-11-02'::timestamptz, '2024-11-04', interval '1 day')`, "2024-11-02 00:00:00-04;2024-11-03 00:00:00-04;2024-11-04 00:00:00-05")
	// The binary format is the UTC instant whatever the zone.
	instant(`SELECT date_trunc('day', at) FROM tz WHERE id = 1`, utc("2024-07-01 04:00:00"))
	var wall time.Time
	if err := conn.QueryRow(ctx, `SELECT wall FROM tz WHERE id = 1`).Scan(&wall); err != nil || !wall.Equal(utc("2024-07-01 22:30:00")) {
		t.Errorf("TIMESTAMP (binary) is unaffected: %v %v", wall, err)
	}

	// A half-hour zone, and a POSIX offset (UTC+5 is five hours west).
	exec(`SET TimeZone = 'Asia/Kolkata'`)
	check(`SELECT at, date_trunc('hour', at) FROM tz WHERE id = 1`, "2024-07-02 08:00:00+05:30|2024-07-02 08:00:00+05:30")
	exec(`SET TIME ZONE 'UTC+5'`)
	check(`SELECT at FROM tz WHERE id = 1`, "2024-07-01 21:30:00-05")
	exec(`SET TimeZone = '-03'`)
	check(`SELECT at FROM tz WHERE id = 1`, "2024-07-02 05:30:00+03")
	if _, err := conn.Exec(ctx, `SET TimeZone = 'PST'`); pgCode(err) != "22023" {
		t.Errorf("SET TimeZone to an abbreviation: %v, want 22023", err)
	}
	exec(`RESET TimeZone`)
	check(`SELECT at FROM tz WHERE id = 1`, "2024-07-02 02:30:00+00")
}
