package testcluster

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/sthorne/datax/pkg/server"
	"github.com/sthorne/datax/pkg/sql"
	"github.com/sthorne/datax/pkg/sql/catalog"
	"github.com/sthorne/datax/pkg/sql/types"
	"github.com/sthorne/datax/pkg/version"
)

// TestColumnTypeModifiers (issue #96): integer widths (INT2 / INT4 /
// INT8), VARCHAR(n) / CHAR(n), TIMESTAMP without time zone and
// TIMESTAMP(p) are enforced on every write path, spelled by the
// catalogs and SHOW CREATE TABLE, described on the wire with
// PostgreSQL's OIDs and typmods so pgx scans into int16 / int32 /
// time.Time, copied by LIKE and CREATE TABLE AS, and changed by ALTER
// COLUMN TYPE — in place when widening, through the rewrite otherwise.
func TestColumnTypeModifiers(t *testing.T) {
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
	refused := func(stmt, code string) {
		t.Helper()
		if _, serr := trySQL(ctx, s, stmt); serr == nil || serr.Code != code {
			t.Fatalf("%s: %v, want %s", stmt, serr, code)
		}
	}

	execSQL(t, ctx, s, `CREATE TABLE ty (
		id INT4 PRIMARY KEY, sm SMALLINT, big BIGINT, n INT,
		v VARCHAR(5), c CHAR(3),
		ts TIMESTAMP, tz TIMESTAMPTZ(3), t0 TIMESTAMP(0) WITHOUT TIME ZONE)`)
	execSQL(t, ctx, s, `INSERT INTO ty VALUES
		(1, 32767, 5000000000, 2147483647, 'abcde', 'ab', '2024-01-02 03:04:05.5+05:00', '2024-01-02 03:04:05.1234567', '2024-01-02 03:04:05.6')`)
	// Excess spaces truncate silently (PostgreSQL's rule); CHAR trims.
	execSQL(t, ctx, s, `INSERT INTO ty (id, v, c) VALUES (2, 'abcde   ', 'xy ')`)

	// Refusals: range (22003), length (22001), on INSERT, UPDATE and a
	// DEFAULT.
	refused(`INSERT INTO ty (id, sm) VALUES (3, 32768)`, sql.CodeNumericValueOutOfRange)
	refused(`INSERT INTO ty (id, n) VALUES (3, 2147483648)`, sql.CodeNumericValueOutOfRange)
	refused(`INSERT INTO ty (id) VALUES (3000000000)`, sql.CodeNumericValueOutOfRange)
	refused(`INSERT INTO ty (id, v) VALUES (3, 'abcdef')`, "22001")
	refused(`INSERT INTO ty (id, c) VALUES (3, 'abcd')`, "22001")
	refused(`UPDATE ty SET sm = -32769 WHERE id = 1`, sql.CodeNumericValueOutOfRange)
	refused(`UPDATE ty SET v = 'toolong' WHERE id = 1`, "22001")
	refused(`ALTER TABLE ty ADD COLUMN w VARCHAR(2) DEFAULT 'xyz'`, "22001")
	refused(`INSERT INTO ty (id, ts) VALUES (3, 'not a time')`, sql.CodeInvalidTextRepresentation)
	execSQL(t, ctx, s, `INSERT INTO ty (id, sm, ts) VALUES ($1, $2, $3)`, types.NewInt(3), types.NewInt(-32768), types.NewString("2024-06-01T12:00:00-07:00"))

	// Rendering: the without-time-zone column ignored its offset and
	// renders without one; TIMESTAMPTZ(3) rounded to milliseconds and
	// TIMESTAMP(0) to seconds; CHAR(3) blank-pads on output but trims
	// for length and concatenation.
	got := text(execSQL(t, ctx, s, `SELECT id, sm, big, n, v, c, ts, tz, t0 FROM ty ORDER BY id`))
	want := "1|32767|5000000000|2147483647|abcde|ab |2024-01-02 03:04:05.5|2024-01-02 03:04:05.123+00|2024-01-02 03:04:06\n" +
		"2||||abcde|xy |||\n" +
		"3|-32768|||||2024-06-01 12:00:00||\n"
	if got != want {
		t.Fatalf("rows:\n%s\nwant:\n%s", got, want)
	}
	if got := text(execSQL(t, ctx, s, `SELECT c || '!', length(c), v = 'abcde' FROM ty WHERE id = 1`)); got != "ab!|2|t\n" {
		t.Fatalf("CHAR semantics: %q", got)
	}
	if got := text(execSQL(t, ctx, s, `SELECT ts FROM ty WHERE ts = '2024-01-02 03:04:05.5' AND id = 1`)); got != "2024-01-02 03:04:05.5\n" {
		t.Fatalf("TIMESTAMP comparison with a literal: %q", got)
	}

	// SHOW CREATE TABLE and the catalogs spell the declared types.
	create := execSQL(t, ctx, s, `SHOW CREATE TABLE ty`).Rows[0][1].S
	for _, want := range []string{"id INT4 NOT NULL", "sm INT2", "big INT8", "n INT4", "v VARCHAR(5)", "c CHAR(3)", "ts TIMESTAMP,", "tz TIMESTAMPTZ(3)", "t0 TIMESTAMP(0)"} {
		if !strings.Contains(create, want) {
			t.Fatalf("SHOW CREATE TABLE lacks %q:\n%s", want, create)
		}
	}
	got = text(execSQL(t, ctx, s, `SELECT column_name, data_type, character_maximum_length, numeric_precision, udt_name
		FROM information_schema.columns WHERE table_name = 'ty' ORDER BY ordinal_position`))
	want = "id|integer||32|int4\nsm|smallint||16|int2\nbig|bigint||64|int8\nn|integer||32|int4\n" +
		"v|character varying(5)|5||varchar\nc|character(3)|3||bpchar\n" +
		"ts|timestamp without time zone|||timestamp\ntz|timestamp(3) with time zone|||timestamptz\nt0|timestamp(0) without time zone|||timestamp\n"
	if got != want {
		t.Fatalf("information_schema.columns:\n%s\nwant:\n%s", got, want)
	}
	got = text(execSQL(t, ctx, s, `SELECT attname, atttypid, atttypmod, attlen, format_type(atttypid, atttypmod)
		FROM pg_attribute WHERE attrelid = 'ty'::regclass ORDER BY attnum`))
	want = "id|23|-1|4|integer\nsm|21|-1|2|smallint\nbig|20|-1|8|bigint\nn|23|-1|4|integer\n" +
		"v|1043|9|-1|character varying(5)\nc|1042|7|-1|character(3)\n" +
		"ts|1114|-1|8|timestamp without time zone\ntz|1184|3|8|timestamp(3) with time zone\nt0|1114|-1|8|timestamp(0) without time zone\n"
	if got != want {
		t.Fatalf("pg_attribute:\n%s\nwant:\n%s", got, want)
	}
	if got := text(execSQL(t, ctx, s, `SELECT oid, typname FROM pg_type WHERE oid IN (20, 21, 23, 1042, 1043, 1114, 1184) ORDER BY oid`)); got !=
		"20|int8\n21|int2\n23|int4\n1042|bpchar\n1043|varchar\n1114|timestamp\n1184|timestamptz\n" {
		t.Fatalf("pg_type: %q", got)
	}

	// LIKE and CREATE TABLE AS keep the modifiers of table columns; a
	// computed column is bare.
	execSQL(t, ctx, s, `CREATE TABLE ty_like (LIKE ty)`)
	if c := execSQL(t, ctx, s, `SHOW CREATE TABLE ty_like`).Rows[0][1].S; !strings.Contains(c, "sm INT2") || !strings.Contains(c, "c CHAR(3)") || !strings.Contains(c, "tz TIMESTAMPTZ(3)") {
		t.Fatalf("LIKE:\n%s", c)
	}
	execSQL(t, ctx, s, `CREATE TABLE ty_as AS SELECT id, v, ts, sm + 1 AS sm1 FROM ty`)
	if c := execSQL(t, ctx, s, `SHOW CREATE TABLE ty_as`).Rows[0][1].S; !strings.Contains(c, "id INT4") || !strings.Contains(c, "v VARCHAR(5)") || !strings.Contains(c, "ts TIMESTAMP,") || !strings.Contains(c, "sm1 INT8") {
		t.Fatalf("CREATE TABLE AS:\n%s", c)
	}
	refused(`INSERT INTO ty_like (id, v) VALUES (9, 'toolong')`, "22001")

	// The wire: Describe reports the refined OIDs and typmods; pgx scans
	// binary results into the matching Go widths and binds parameters
	// of those widths; the text protocol renders per the column.
	conn, err := pgx.Connect(ctx, pgURL(tc, 0))
	if err != nil {
		t.Fatalf("pgx connect: %v", err)
	}
	defer func() { _ = conn.Close(ctx) }()
	sd, err := conn.Prepare(ctx, "d", `SELECT id, sm, big, v, c, ts, tz, sm + 1, v || 'x' FROM ty`)
	if err != nil {
		t.Fatalf("prepare: %v", err)
	}
	wantOIDs := []uint32{23, 21, 20, 1043, 1042, 1114, 1184, 20, 25}
	wantMods := []int32{-1, -1, -1, 9, 7, -1, 3, -1, -1}
	for i, f := range sd.Fields {
		if f.DataTypeOID != wantOIDs[i] || f.TypeModifier != wantMods[i] {
			t.Errorf("column %s describes as %d/%d, want %d/%d", f.Name, f.DataTypeOID, f.TypeModifier, wantOIDs[i], wantMods[i])
		}
	}
	var id int32
	var sm int16
	var big int64
	var v, c string
	var ts, tz time.Time
	if err := conn.QueryRow(ctx, `SELECT id, sm, big, v, c, ts, tz FROM ty WHERE id = 1`).Scan(&id, &sm, &big, &v, &c, &ts, &tz); err != nil {
		t.Fatalf("binary scan: %v", err)
	}
	wantTS := time.Date(2024, 1, 2, 3, 4, 5, 500000000, time.UTC)
	wantTZ := time.Date(2024, 1, 2, 3, 4, 5, 123000000, time.UTC)
	if id != 1 || sm != 32767 || big != 5000000000 || v != "abcde" || c != "ab " || !ts.Equal(wantTS) || !tz.Equal(wantTZ) {
		t.Fatalf("binary values: %d %d %d %q %q %v %v", id, sm, big, v, c, ts, tz)
	}
	if _, err := conn.Exec(ctx, `INSERT INTO ty (id, sm, big, v, c, ts, tz) VALUES ($1, $2, $3, $4, $5, $6, $7)`,
		int32(10), int16(-7), int64(1<<40), "wire", "w", wantTS, wantTZ); err != nil {
		t.Fatalf("binary params: %v", err)
	}
	if _, err := conn.Exec(ctx, `INSERT INTO ty (id, sm) VALUES ($1, $2)`, int32(11), int32(70000)); err == nil || !strings.Contains(err.Error(), "22003") {
		t.Fatalf("out-of-range binary param: %v", err)
	}
	scfg, _ := pgx.ParseConfig(pgURL(tc, 0))
	scfg.DefaultQueryExecMode = pgx.QueryExecModeSimpleProtocol
	sconn, err := pgx.ConnectConfig(ctx, scfg)
	if err != nil {
		t.Fatalf("simple connect: %v", err)
	}
	defer func() { _ = sconn.Close(ctx) }()
	var tid int32
	var tsm int16
	var tc3 string
	var tts time.Time
	if err := sconn.QueryRow(ctx, `SELECT id, sm, c, ts FROM ty WHERE id = 10`).Scan(&tid, &tsm, &tc3, &tts); err != nil {
		t.Fatalf("text scan: %v", err)
	}
	if tid != 10 || tsm != -7 || tc3 != "w  " || !tts.Equal(wantTS) {
		t.Fatalf("text values: %d %d %q %v", tid, tsm, tc3, tts)
	}

	// ALTER COLUMN TYPE: widening is a descriptor write (a value that was
	// refused fits afterwards); narrowing rewrites and refuses a stored
	// value that does not fit, leaving the column as it was.
	execSQL(t, ctx, s, `ALTER TABLE ty ALTER COLUMN sm TYPE INT4`)
	execSQL(t, ctx, s, `INSERT INTO ty (id, sm) VALUES (20, 40000)`)
	execSQL(t, ctx, s, `ALTER TABLE ty ALTER COLUMN v TYPE TEXT`)
	execSQL(t, ctx, s, `INSERT INTO ty (id, v) VALUES (21, 'no longer bounded')`)
	if _, serr := trySQL(ctx, s, `ALTER TABLE ty ALTER COLUMN big TYPE INT4`); serr == nil || serr.Code != sql.CodeNumericValueOutOfRange || !strings.Contains(serr.Msg, `column "big"`) {
		t.Fatalf("narrowing over a stored 5000000000: %v", serr)
	}
	refused(`ALTER TABLE ty ALTER COLUMN v TYPE VARCHAR(4)`, "22001")
	if c := execSQL(t, ctx, s, `SHOW CREATE TABLE ty`).Rows[0][1].S; !strings.Contains(c, "big INT8") || !strings.Contains(c, "v TEXT") {
		t.Fatalf("a refused narrowing must leave the column as it was:\n%s", c)
	}
	execSQL(t, ctx, s, `DELETE FROM ty WHERE id IN (1, 2, 21)`)
	execSQL(t, ctx, s, `UPDATE ty SET big = 7 WHERE id = 10`)
	execSQL(t, ctx, s, `ALTER TABLE ty ALTER COLUMN big TYPE INT4`)
	execSQL(t, ctx, s, `ALTER TABLE ty ALTER COLUMN v TYPE VARCHAR(4)`)
	execSQL(t, ctx, s, `ALTER TABLE ty ALTER COLUMN tz TYPE TIMESTAMP`)
	refused(`INSERT INTO ty (id, big) VALUES (22, 5000000000)`, sql.CodeNumericValueOutOfRange)
	refused(`INSERT INTO ty (id, v) VALUES (22, 'abcde')`, "22001")
	create = execSQL(t, ctx, s, `SHOW CREATE TABLE ty`).Rows[0][1].S
	for _, want := range []string{"sm INT4", "big INT4", "v VARCHAR(4)", "tz TIMESTAMP,"} {
		if !strings.Contains(create, want) {
			t.Fatalf("after ALTER COLUMN TYPE, SHOW CREATE TABLE lacks %q:\n%s", want, create)
		}
	}
	if got := text(execSQL(t, ctx, s, `SELECT id, sm, big, v, tz FROM ty WHERE id = 10`)); got != "10|-7|7|wire|2024-01-02 03:04:05.123\n" {
		t.Fatalf("after ALTER COLUMN TYPE: %q", got)
	}
}

// TestColumnTypeModifiersNeedV9: below cluster version v9 a new column
// keeps the earlier meaning of its declaration — INT is 64-bit,
// VARCHAR(n) is unbounded, TIMESTAMP is TIMESTAMPTZ — so a mixed-
// version cluster never carries a modifier an older binary would
// ignore.
func TestColumnTypeModifiersNeedV9(t *testing.T) {
	tc := StartWithOptions(t, 1, func(c *server.Config) { c.BinaryVersionOverride = version.V8 })
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	s := sql.NewSession(tc.Nodes[0].DB(), catalog.NewAccessor())
	waitForDatabases(t, ctx, s)
	execSQL(t, ctx, s, `CREATE TABLE old (id INT PRIMARY KEY, v VARCHAR(2), ts TIMESTAMP)`)
	execSQL(t, ctx, s, `INSERT INTO old VALUES (3000000000, 'unbounded', '2024-01-02 03:04:05')`)
	create := execSQL(t, ctx, s, `SHOW CREATE TABLE old`).Rows[0][1].S
	if !strings.Contains(create, "id INT8") || !strings.Contains(create, "v TEXT") || !strings.Contains(create, "ts TIMESTAMPTZ") {
		t.Fatalf("v8 CREATE TABLE:\n%s", create)
	}
	if r := execSQL(t, ctx, s, `SELECT ts FROM old`); r.Rows[0][0].Text() != "2024-01-02 03:04:05+00" {
		t.Fatalf("v8 TIMESTAMP: %s", r.Rows[0][0].Text())
	}
}
