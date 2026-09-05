package testcluster

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"

	"github.com/sthorne/datax/pkg/server"
	"github.com/sthorne/datax/pkg/sql"
	"github.com/sthorne/datax/pkg/sql/catalog"
	"github.com/sthorne/datax/pkg/sql/types"
	"github.com/sthorne/datax/pkg/version"
)

// TestIntervalTime (issue #96, part two): INTERVAL and TIME columns —
// every input syntax, PostgreSQL's rendering, arithmetic with
// timestamps, dates and each other, extract / justify_* / make_*,
// ordering and aggregation, indexes and primary keys, the catalogs, the
// wire through pgx in both protocols, ALTER COLUMN TYPE from text, and
// the timeseries options accepting interval text.
func TestIntervalTime(t *testing.T) {
	tc := Start(t, 1)
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()
	s := sql.NewSession(tc.Nodes[0].DB(), catalog.NewAccessor())
	waitForDatabases(t, ctx, s)

	text := func(r *sql.Result) string {
		var b strings.Builder
		for _, row := range r.Rows {
			for i, d := range row {
				if i > 0 {
					b.WriteByte('|')
				}
				b.WriteString(d.Text())
			}
			b.WriteByte('\n')
		}
		return b.String()
	}
	one := func(q string) string {
		t.Helper()
		return strings.TrimSuffix(text(execSQL(t, ctx, s, q)), "\n")
	}
	refused := func(stmt, code string) {
		t.Helper()
		if _, serr := trySQL(ctx, s, stmt); serr == nil || serr.Code != code {
			t.Fatalf("%s: %v, want %s", stmt, serr, code)
		}
	}

	execSQL(t, ctx, s, `CREATE TABLE ev (id INT8 PRIMARY KEY, dur INTERVAL, at TIME, t3 TIME(3), ts TIMESTAMPTZ)`)
	execSQL(t, ctx, s, `INSERT INTO ev VALUES
		(1, '1 day 02:00:00', '10:30', '10:30:00.1239', '2024-01-31 10:00:00Z'),
		(2, '1 year 2 mons', '4:05 PM', '23:59:59.9996', '2024-02-29 00:00:00Z'),
		(3, 'P1DT2H', TIME '00:00', NULL, '2024-03-01 12:00:00Z'),
		(4, INTERVAL '3 hours', '2024-01-02 07:08:09', '07:08:09', NULL),
		(5, NULL, NULL, NULL, '2024-01-01 00:00:00Z')`)
	execSQL(t, ctx, s, `INSERT INTO ev (id, dur, at) VALUES ($1, $2, $3)`, types.NewInt(6), types.NewString("-1 day 12:00:00 ago"), types.NewString("12:00:00+05"))
	refused(`INSERT INTO ev (id, dur) VALUES (9, '3 fortnights')`, sql.CodeInvalidTextRepresentation)
	refused(`INSERT INTO ev (id, at) VALUES (9, '25:00')`, sql.CodeInvalidTextRepresentation)

	got := text(execSQL(t, ctx, s, `SELECT id, dur, at, t3, ts FROM ev ORDER BY id`))
	want := "1|1 day 02:00:00|10:30:00|10:30:00.124|2024-01-31 10:00:00+00\n" +
		"2|1 year 2 mons|16:05:00|24:00:00|2024-02-29 00:00:00+00\n" +
		"3|1 day 02:00:00|00:00:00||2024-03-01 12:00:00+00\n" +
		"4|03:00:00|07:08:09|07:08:09|\n" +
		"5||||2024-01-01 00:00:00+00\n" +
		"6|1 day -12:00:00|12:00:00||\n"
	if got != want {
		t.Fatalf("rows:\n%s\nwant:\n%s", got, want)
	}

	// Arithmetic: timestamp ± interval on the calendar, timestamp −
	// timestamp, interval ± interval, interval × / number, time ±
	// interval wrapping at midnight, time − time, date + time.
	for _, c := range []struct{ q, want string }{
		{`SELECT ts + dur FROM ev WHERE id = 2`, "2025-04-29 00:00:00+00"},
		{`SELECT ts + INTERVAL '1 month' FROM ev WHERE id = 1`, "2024-02-29 10:00:00+00"},
		{`SELECT ts - dur FROM ev WHERE id = 1`, "2024-01-30 08:00:00+00"},
		{`SELECT dur + INTERVAL '2 hours 30 minutes' FROM ev WHERE id = 1`, "1 day 04:30:00"},
		{`SELECT dur - dur FROM ev WHERE id = 1`, "00:00:00"},
		{`SELECT dur * 2, dur / 2, 3 * dur FROM ev WHERE id = 4`, "06:00:00|01:30:00|09:00:00"},
		{`SELECT INTERVAL '1 month' * 1.5`, "1 mon 15 days"},
		{`SELECT (SELECT ts FROM ev WHERE id = 3) - (SELECT ts FROM ev WHERE id = 1)`, "30 days 02:00:00"},
		{`SELECT at + INTERVAL '2 hours', at - INTERVAL '11 hours' FROM ev WHERE id = 1`, "12:30:00|23:30:00"},
		{`SELECT at + dur FROM ev WHERE id = 2`, "16:05:00"},
		{`SELECT (SELECT at FROM ev WHERE id = 2) - (SELECT at FROM ev WHERE id = 1)`, "05:35:00"},
		{`SELECT DATE '2024-05-06' + at FROM ev WHERE id = 1`, "2024-05-06 10:30:00+00"},
		{`SELECT ts::time, ts::date + INTERVAL '1 day' FROM ev WHERE id = 1`, "10:00:00|2024-02-01 00:00:00+00"},
		{`SELECT dur > INTERVAL '1 day', dur = '26 hours', INTERVAL '30 days' = INTERVAL '1 month' FROM ev WHERE id = 1`, "t|t|t"},
		{`SELECT extract(hour FROM dur), extract(day FROM dur), extract(epoch FROM dur) FROM ev WHERE id = 1`, "2|1|93600"},
		{`SELECT extract(month FROM dur), extract(year FROM dur) FROM ev WHERE id = 2`, "2|1"},
		{`SELECT extract(hour FROM at), extract(minute FROM at), extract(second FROM t3) FROM ev WHERE id = 1`, "10|30|0.124"},
		{`SELECT justify_hours(INTERVAL '27 hours'), justify_days(INTERVAL '35 days'), justify_interval(INTERVAL '1 month -1 day')`, "1 day 03:00:00|1 mon 5 days|29 days"},
		{`SELECT make_interval(1, 2, 0, 3, 4, 5, 6.5), make_time(8, 9, 10.5)`, "1 year 2 mons 3 days 04:05:06.5|08:09:10.5"},
		{`SELECT age('2024-03-01'::timestamptz, '2023-01-15'::timestamptz)`, "1 year 1 mon 15 days"},
		{`SELECT INTERVAL '1 hour'::text, TIME '10:00'::text, '1 day'::interval, '10:00'::time, INTERVAL '90 minutes'::time`, "01:00:00|10:00:00|1 day|10:00:00|01:30:00"},
		{`SELECT sum(dur), avg(dur), min(dur), max(dur), count(dur) FROM ev`, "1 year 2 mons 3 days -05:00:00|2 mons 24 days 13:24:00|03:00:00|1 year 2 mons|5"},
		{`SELECT min(at), max(at) FROM ev`, "00:00:00|16:05:00"},
		{`SELECT id FROM ev WHERE dur IS NOT NULL ORDER BY dur DESC, id`, "2\n1\n3\n6\n4"},
		{`SELECT dur, count(*) FROM ev WHERE dur IS NOT NULL GROUP BY dur ORDER BY dur`, "03:00:00|1\n1 day -12:00:00|1\n1 day 02:00:00|2\n1 year 2 mons|1"},
		{`SELECT id FROM ev WHERE at BETWEEN TIME '07:00' AND TIME '12:00' ORDER BY id`, "1\n4\n6"},
		{`SELECT now() - INTERVAL '1 day' < now(), clock_timestamp() + INTERVAL '1 hour' > now()`, "t|t"},
	} {
		if got := one(c.q); got != c.want {
			t.Errorf("%s: %q, want %q", c.q, got, c.want)
		}
	}
	refused(`SELECT dur + 1 FROM ev`, sql.CodeUndefinedFunction)
	refused(`SELECT INTERVAL '1 day' / 0`, sql.CodeDivisionByZero)

	// Indexes and primary keys over the new families.
	execSQL(t, ctx, s, `CREATE INDEX ev_dur ON ev (dur)`)
	execSQL(t, ctx, s, `CREATE INDEX ev_at ON ev (at)`)
	if got := one(`SELECT id FROM ev WHERE dur > INTERVAL '1 day' AND dur < INTERVAL '1 year' ORDER BY id`); got != "1\n3" {
		t.Fatalf("index range over intervals: %q", got)
	}
	if got := one(`EXPLAIN SELECT id FROM ev WHERE dur = '1 day 02:00:00'`); !strings.Contains(got, "ev_dur") {
		t.Fatalf("plan should use ev_dur: %s", got)
	}
	if got := one(`SELECT id FROM ev WHERE at = '16:05' OR at > TIME '11:00' ORDER BY id`); got != "2\n6" {
		t.Fatalf("index over times: %q", got)
	}
	execSQL(t, ctx, s, `CREATE TABLE sched (slot TIME PRIMARY KEY, every INTERVAL NOT NULL, name TEXT, UNIQUE (every))`)
	execSQL(t, ctx, s, `INSERT INTO sched VALUES ('09:00', '1 week', 'standup'), ('17:30', '1 day', 'wrap'), ('12:00', '1 month', 'lunch')`)
	refused(`INSERT INTO sched VALUES ('09:00', '2 weeks', 'dup')`, sql.CodeUniqueViolation)
	refused(`INSERT INTO sched VALUES ('10:00', '7 days', 'dup')`, sql.CodeUniqueViolation)
	if got := one(`SELECT slot, every, name FROM sched ORDER BY slot`); got != "09:00:00|7 days|standup\n12:00:00|1 mon|lunch\n17:30:00|1 day|wrap" {
		t.Fatalf("primary key over TIME: %q", got)
	}
	if got := one(`SELECT name FROM sched WHERE slot = TIME '12:00'`); got != "lunch" {
		t.Fatalf("point read over a TIME key: %q", got)
	}
	execSQL(t, ctx, s, `UPDATE sched SET every = every * 2 WHERE name = 'wrap'`)
	if got := one(`SELECT every FROM sched WHERE name = 'wrap'`); got != "2 days" {
		t.Fatalf("update: %q", got)
	}

	// Catalogs.
	create := execSQL(t, ctx, s, `SHOW CREATE TABLE ev`).Rows[0][1].S
	for _, w := range []string{"dur INTERVAL", "at TIME,", "t3 TIME(3)"} {
		if !strings.Contains(create, w) {
			t.Fatalf("SHOW CREATE TABLE lacks %q:\n%s", w, create)
		}
	}
	if got := one(`SELECT column_name, data_type, udt_name FROM information_schema.columns WHERE table_name = 'ev' AND column_name IN ('dur', 'at', 't3') ORDER BY ordinal_position`); got !=
		"dur|interval|interval\nat|time without time zone|time\nt3|time(3) without time zone|time" {
		t.Fatalf("information_schema.columns: %q", got)
	}
	if got := one(`SELECT attname, atttypid, atttypmod, attlen FROM pg_attribute WHERE attrelid = 'ev'::regclass AND attname IN ('dur', 'at', 't3') ORDER BY attnum`); got != "dur|1186|-1|16\nat|1083|-1|8\nt3|1083|3|8" {
		t.Fatalf("pg_attribute: %q", got)
	}
	if got := one(`SELECT oid, typname, typcategory FROM pg_type WHERE oid IN (1083, 1186) ORDER BY oid`); got != "1083|time|D\n1186|interval|T" {
		t.Fatalf("pg_type: %q", got)
	}

	// ALTER COLUMN TYPE from text, and CREATE TABLE AS keeping the family.
	execSQL(t, ctx, s, `CREATE TABLE txt (id INT8 PRIMARY KEY, d TEXT, c TEXT)`)
	execSQL(t, ctx, s, `INSERT INTO txt VALUES (1, '1 day', '10:00'), (2, 'P2W', '4 PM')`)
	execSQL(t, ctx, s, `ALTER TABLE txt ALTER COLUMN d TYPE INTERVAL`)
	execSQL(t, ctx, s, `ALTER TABLE txt ALTER COLUMN c TYPE TIME`)
	if got := one(`SELECT d, c, d + INTERVAL '1 hour' FROM txt ORDER BY id`); got != "1 day|10:00:00|1 day 01:00:00\n14 days|16:00:00|14 days 01:00:00" {
		t.Fatalf("after ALTER COLUMN TYPE: %q", got)
	}
	execSQL(t, ctx, s, `CREATE TABLE ev2 AS SELECT id, dur, at, dur * 2 AS twice FROM ev`)
	if c := execSQL(t, ctx, s, `SHOW CREATE TABLE ev2`).Rows[0][1].S; !strings.Contains(c, "dur INTERVAL") || !strings.Contains(c, "at TIME") || !strings.Contains(c, "twice INTERVAL") {
		t.Fatalf("CREATE TABLE AS:\n%s", c)
	}

	// Timeseries options take interval text.
	execSQL(t, ctx, s, `CREATE TABLE m (series INT8, ts TIMESTAMPTZ, v FLOAT8, PRIMARY KEY (series, ts)) WITH (timeseries = true, retention = '7 days')`)
	execSQL(t, ctx, s, `INSERT INTO m VALUES (1, now(), 1.5)`)
	// The stale read lands before the table's creation until the closed
	// timestamp passes it.
	for deadline := time.Now().Add(20 * time.Second); ; {
		_, serr := trySQL(ctx, s, `SELECT count(*) FROM m AS OF SYSTEM TIME with_max_staleness('2 seconds')`)
		if serr == nil {
			break
		}
		if serr.Code != sql.CodeUndefinedTable || time.Now().After(deadline) {
			t.Fatalf("with_max_staleness('2 seconds'): %v", serr)
		}
		time.Sleep(100 * time.Millisecond)
	}
	refused(`SELECT count(*) FROM m AS OF SYSTEM TIME with_max_staleness('2 fortnights')`, sql.CodeInvalidParameterValue)
	refused(`CREATE TABLE m2 (ts TIMESTAMPTZ PRIMARY KEY) WITH (timeseries = true, retention = '1 fortnight')`, sql.CodeSyntaxError)

	// The wire: Describe (1186 / 1083), binary results into pgtype
	// values, binary parameters, and the text protocol.
	conn, err := pgx.Connect(ctx, pgURL(tc, 0))
	if err != nil {
		t.Fatalf("pgx connect: %v", err)
	}
	defer func() { _ = conn.Close(ctx) }()
	sd, err := conn.Prepare(ctx, "d", `SELECT dur, at, t3, ts - dur, at + INTERVAL '1 hour', dur * 2 FROM ev`)
	if err != nil {
		t.Fatalf("prepare: %v", err)
	}
	wantOIDs := []uint32{1186, 1083, 1083, 1184, 1083, 1186}
	wantMods := []int32{-1, -1, 3, -1, -1, -1}
	for i, f := range sd.Fields {
		if f.DataTypeOID != wantOIDs[i] || f.TypeModifier != wantMods[i] {
			t.Errorf("column %s describes as %d/%d, want %d/%d", f.Name, f.DataTypeOID, f.TypeModifier, wantOIDs[i], wantMods[i])
		}
	}
	var dur pgtype.Interval
	var at pgtype.Time
	if err := conn.QueryRow(ctx, `SELECT dur, at FROM ev WHERE id = 2`).Scan(&dur, &at); err != nil {
		t.Fatalf("binary scan: %v", err)
	}
	if dur != (pgtype.Interval{Months: 14, Valid: true}) || at != (pgtype.Time{Microseconds: (16*3600 + 5*60) * 1e6, Valid: true}) {
		t.Fatalf("binary values: %+v %+v", dur, at)
	}
	var gap time.Duration
	if err := conn.QueryRow(ctx, `SELECT dur FROM ev WHERE id = 4`).Scan(&gap); err != nil || gap != 3*time.Hour {
		t.Fatalf("interval into time.Duration: %v %v", gap, err)
	}
	if _, err := conn.Exec(ctx, `INSERT INTO ev (id, dur, at) VALUES ($1, $2, $3)`, int64(10),
		pgtype.Interval{Months: 1, Days: 2, Microseconds: 3_500_000, Valid: true}, pgtype.Time{Microseconds: 45_296_000_000, Valid: true}); err != nil {
		t.Fatalf("binary params: %v", err)
	}
	if _, err := conn.Exec(ctx, `INSERT INTO ev (id, dur, at) VALUES ($1, $2, $3)`, int64(11), 90*time.Minute, "23:59:59"); err != nil {
		t.Fatalf("duration / text params: %v", err)
	}
	if got := one(`SELECT id, dur, at FROM ev WHERE id >= 10 ORDER BY id`); got != "10|1 mon 2 days 00:00:03.5|12:34:56\n11|01:30:00|23:59:59" {
		t.Fatalf("rows from binary params: %q", got)
	}
	scfg, _ := pgx.ParseConfig(pgURL(tc, 0))
	scfg.DefaultQueryExecMode = pgx.QueryExecModeSimpleProtocol
	sconn, err := pgx.ConnectConfig(ctx, scfg)
	if err != nil {
		t.Fatalf("simple connect: %v", err)
	}
	defer func() { _ = sconn.Close(ctx) }()
	var tdur pgtype.Interval
	var tat pgtype.Time
	if err := sconn.QueryRow(ctx, `SELECT dur, at FROM ev WHERE id = 10`).Scan(&tdur, &tat); err != nil {
		t.Fatalf("text scan: %v", err)
	}
	if tdur != (pgtype.Interval{Months: 1, Days: 2, Microseconds: 3_500_000, Valid: true}) || tat != (pgtype.Time{Microseconds: 45_296_000_000, Valid: true}) {
		t.Fatalf("text values: %+v %+v", tdur, tat)
	}
}

// TestIntervalTimeNeedV10: a column of the new families is refused
// until the cluster version is finalized (an older node cannot decode
// its rows); expressions over them work regardless.
func TestIntervalTimeNeedV10(t *testing.T) {
	tc := StartWithOptions(t, 1, func(c *server.Config) { c.BinaryVersionOverride = version.V9 })
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	s := sql.NewSession(tc.Nodes[0].DB(), catalog.NewAccessor())
	waitForDatabases(t, ctx, s)
	execSQL(t, ctx, s, `CREATE TABLE plain (id INT8 PRIMARY KEY, ts TIMESTAMPTZ, d TEXT)`)
	for _, stmt := range []string{
		`CREATE TABLE iv (id INT8 PRIMARY KEY, d INTERVAL)`,
		`CREATE TABLE tm (id INT8 PRIMARY KEY, t TIME)`,
		`ALTER TABLE plain ADD COLUMN d2 INTERVAL`,
		`ALTER TABLE plain ALTER COLUMN d TYPE INTERVAL`,
		`CREATE TABLE ct AS SELECT id, INTERVAL '1 day' AS d FROM plain`,
	} {
		if _, serr := trySQL(ctx, s, stmt); serr == nil || serr.Code != sql.CodeFeatureNotSupported {
			t.Fatalf("%s at v9: %v, want 0A000", stmt, serr)
		}
	}
	if r := execSQL(t, ctx, s, `SELECT INTERVAL '1 day' + INTERVAL '2 hours', TIME '10:00' + INTERVAL '1 hour'`); r.Rows[0][0].Text() != "1 day 02:00:00" || r.Rows[0][1].Text() != "11:00:00" {
		t.Fatalf("expressions at v9: %v", r.Rows)
	}
}
