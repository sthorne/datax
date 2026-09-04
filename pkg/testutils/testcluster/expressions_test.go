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

// exprCase is one expression with its expected text rendering (or an
// expected SQLSTATE).
type exprCase struct {
	expr string
	want string // the rendered value; "NULL" for NULL
	code string // when set, the expected error code instead
}

// runExprCases evaluates each expression in a FROM-less SELECT and
// checks its text, table-driven.
func runExprCases(t *testing.T, ctx context.Context, s *sql.Session, cases []exprCase) {
	t.Helper()
	for _, c := range cases {
		r, serr := trySQL(ctx, s, "SELECT "+c.expr)
		if c.code != "" {
			if serr == nil || serr.Code != c.code {
				t.Errorf("%s: %v, want %s", c.expr, serr, c.code)
			}
			continue
		}
		if serr != nil {
			t.Errorf("%s: %v", c.expr, serr)
			continue
		}
		got := "NULL"
		if d := r.Rows[0][0]; !d.Null {
			got = d.Text()
		}
		if got != c.want {
			t.Errorf("%s = %q, want %q", c.expr, got, c.want)
		}
	}
}

// TestExpressionResultTypes: computed output columns describe on the
// wire as their real types — an integer sum as int8, a cast as its
// target, a builtin by its declared result — so drivers scan them
// without a text round trip.
func TestExpressionResultTypes(t *testing.T) {
	tc := Start(t, 1)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	s := sql.NewSession(tc.Nodes[0].DB(), catalog.NewAccessor())
	waitForDatabases(t, ctx, s)
	execSQL(t, ctx, s, `CREATE TABLE r (id INT8 PRIMARY KEY, price DECIMAL, name TEXT, at TIMESTAMPTZ, ok BOOL)`)
	execSQL(t, ctx, s, `INSERT INTO r VALUES (1, 9.5, 'x', '2024-01-02 03:04:05', true)`)
	conn, err := pgx.Connect(ctx, "postgres://root@"+tc.Nodes[0].SQLAddr()+"/datax?sslmode=disable")
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer conn.Close(ctx)
	const (
		oidBool        = 16
		oidInt8        = 20
		oidText        = 25
		oidFloat8      = 701
		oidNumeric     = 1700
		oidTimestamptz = 1184
		oidDate        = 1082
	)
	cases := []struct {
		q    string
		oids []uint32
	}{
		{`SELECT 1 + 1, 2 * 3.5, 7 / 2.0, 'a' || 'b', 1 < 2`, []uint32{oidInt8, oidNumeric, oidNumeric, oidText, oidBool}},
		{`SELECT id * 2, price * 2, length(name), upper(name), round(price), at::date, ok IS TRUE FROM r`, []uint32{oidInt8, oidNumeric, oidInt8, oidText, oidNumeric, oidDate, oidBool}},
		{`SELECT '5'::int8, 1::float8, 2 ^ 3, sqrt(4), coalesce(price, 0), greatest(id, 3), CASE WHEN ok THEN 1 ELSE 0 END FROM r`, []uint32{oidInt8, oidFloat8, oidInt8, oidFloat8, oidNumeric, oidInt8, oidInt8}},
		{`SELECT now() > at, at + '1 day', extract(year FROM at) FROM r`, []uint32{oidBool, oidTimestamptz, oidNumeric}},
	}
	for i, c := range cases {
		sd, err := conn.Prepare(ctx, "q"+itoa(i), c.q)
		if err != nil {
			t.Fatalf("%s: %v", c.q, err)
		}
		if len(sd.Fields) != len(c.oids) {
			t.Fatalf("%s: %d fields, want %d", c.q, len(sd.Fields), len(c.oids))
		}
		for j, f := range sd.Fields {
			if f.DataTypeOID != c.oids[j] {
				t.Errorf("%s: column %d (%s) describes as OID %d, want %d", c.q, j+1, f.Name, f.DataTypeOID, c.oids[j])
			}
		}
		// And the values arrive typed: scanning into Go types works.
		rows, err := conn.Query(ctx, c.q)
		if err != nil {
			t.Fatalf("%s: %v", c.q, err)
		}
		for rows.Next() {
			if _, err := rows.Values(); err != nil {
				t.Fatalf("%s: values: %v", c.q, err)
			}
		}
		rows.Close()
	}
}

// TestExpressionsScalar: the builtin string, math and conditional
// functions, the arithmetic operators, and the predicates (BETWEEN,
// SIMILAR TO, LIKE ... ESCAPE, IS DISTINCT FROM, IS TRUE), with NULL
// propagation and the error codes.
func TestExpressionsScalar(t *testing.T) {
	tc := Start(t, 1)
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	s := sql.NewSession(tc.Nodes[0].DB(), catalog.NewAccessor())
	waitForDatabases(t, ctx, s)

	runExprCases(t, ctx, s, []exprCase{
		// Strings.
		{expr: `concat('a', 1, NULL, 'b')`, want: "a1b"},
		{expr: `concat_ws(', ', 'a', NULL, 'b')`, want: "a, b"},
		{expr: `concat_ws(NULL, 'a', 'b')`, want: "NULL"},
		{expr: `substring('hello world', 7)`, want: "world"},
		{expr: `substring('hello world', 1, 5)`, want: "hello"},
		{expr: `substring('hello world' FROM 7 FOR 3)`, want: "wor"},
		{expr: `substr('héllo', 2, 2)`, want: "él"},
		{expr: `substring('abc', -1, 3)`, want: "a"},
		{expr: `left('hello', 2)`, want: "he"},
		{expr: `left('hello', -2)`, want: "hel"},
		{expr: `right('hello', 3)`, want: "llo"},
		{expr: `position('lo' IN 'hello')`, want: "4"},
		{expr: `strpos('hello', 'zz')`, want: "0"},
		{expr: `replace('a-b-c', '-', '+')`, want: "a+b+c"},
		{expr: `trim('  x  ')`, want: "x"},
		{expr: `trim(BOTH 'x' FROM 'xxhixx')`, want: "hi"},
		{expr: `trim(LEADING 'x' FROM 'xxhixx')`, want: "hixx"},
		{expr: `trim(TRAILING FROM 'hi  ')`, want: "hi"},
		{expr: `ltrim('  x')`, want: "x"},
		{expr: `rtrim('x  ') || '|'`, want: "x|"},
		{expr: `btrim('--x--', '-')`, want: "x"},
		{expr: `lpad('7', 3, '0')`, want: "007"},
		{expr: `rpad('ab', 5, 'xy')`, want: "abxyx"},
		{expr: `lpad('hello', 3)`, want: "hel"},
		{expr: `repeat('ab', 3)`, want: "ababab"},
		{expr: `reverse('abc')`, want: "cba"},
		{expr: `split_part('a.b.c', '.', 2)`, want: "b"},
		{expr: `split_part('a.b.c', '.', -1)`, want: "c"},
		{expr: `split_part('a.b.c', '.', 9)`, want: ""},
		{expr: `split_part('a.b.c', '.', 0)`, code: "22023"},
		{expr: `starts_with('hello', 'he')`, want: "t"},
		{expr: `initcap('hello wORLD-x')`, want: "Hello World-X"},
		{expr: `md5('abc')`, want: "900150983cd24fb0d6963f7d28e17f72"},
		{expr: `encode(sha256('abc'), 'hex')`, want: "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"},
		{expr: `encode('hi', 'base64')`, want: "aGk="},
		{expr: `encode(decode('aGk=', 'base64'), 'escape')`, want: "hi"},
		{expr: `decode('zz', 'hex')`, code: "22P02"},
		{expr: `to_hex(255)`, want: "ff"},
		{expr: `format('%s is %L or %I', 'x', 'it''s', 'Col')`, want: `x is 'it''s' or "Col"`},
		{expr: `format('%2$s %1$s', 'a', 'b')`, want: "b a"},
		{expr: `format('%s')`, code: "22023"},
		{expr: `char_length('héllo')`, want: "5"},
		{expr: `octet_length('héllo')`, want: "6"},
		{expr: `ascii('A')`, want: "65"},
		{expr: `chr(65)`, want: "A"},
		{expr: `chr(0)`, code: "22023"},
		{expr: `upper(NULL)`, want: "NULL"},
		{expr: `length(123)`, want: "3"},
		{expr: `translate('12345', '143', 'ax')`, want: "a2x5"},
		// Math.
		{expr: `round(2.5)`, want: "3"},
		{expr: `round(-2.5)`, want: "-3"},
		{expr: `round(2.345, 2)`, want: "2.35"},
		{expr: `round(42.4)`, want: "42"},
		{expr: `round(1234, -2)`, want: "1200"},
		{expr: `trunc(2.789, 1)`, want: "2.7"},
		{expr: `trunc(-2.7)`, want: "-2"},
		{expr: `floor(-2.5)`, want: "-3"},
		{expr: `ceil(2.1)`, want: "3"},
		{expr: `ceiling(-2.1)`, want: "-2"},
		{expr: `mod(7, 3)`, want: "1"},
		{expr: `mod(-7, 3)`, want: "-1"},
		{expr: `7 % 3`, want: "1"},
		{expr: `7.5 % 2`, want: "1.5"},
		{expr: `2 ^ 10`, want: "1024"},
		{expr: `2 ^ 3 ^ 2`, want: "64"},
		{expr: `2 * 3 ^ 2`, want: "18"},
		{expr: `power(2, -1)`, want: "0.5"},
		{expr: `sqrt(16)`, want: "4"},
		{expr: `sqrt(-1)`, code: "22023"},
		{expr: `cbrt(27)`, want: "3"},
		{expr: `abs(-3.5)`, want: "3.5"},
		{expr: `sign(-9)`, want: "-1"},
		{expr: `exp(0)`, want: "1"},
		{expr: `ln(1)`, want: "0"},
		{expr: `log(100)`, want: "2"},
		{expr: `log(2, 8)`, want: "3"},
		{expr: `ln(0)`, code: "22023"},
		{expr: `div(7, 2)`, want: "3"},
		{expr: `div(1, 0)`, code: "22012"},
		{expr: `1 % 0`, code: "22012"},
		{expr: `width_bucket(5.35, 0, 10, 5)`, want: "3"},
		{expr: `width_bucket(11, 0, 10, 5)`, want: "6"},
		{expr: `gcd(12, 18)`, want: "6"},
		{expr: `pi() > 3.14`, want: "t"},
		{expr: `random() >= 0 AND random() < 1`, want: "t"},
		{expr: `9223372036854775807 + 1`, code: "22003"},
		{expr: `-9223372036854775807 - 2`, code: "22003"},
		{expr: `4611686018427387904 * 2`, code: "22003"},
		{expr: `3 * 4`, want: "12"},
		// Conditionals.
		{expr: `coalesce(NULL, NULL, 3)`, want: "3"},
		{expr: `nullif(1, 1)`, want: "NULL"},
		{expr: `nullif(1, 2)`, want: "1"},
		{expr: `nullif('a', NULL)`, want: "a"},
		{expr: `greatest(1, 5, NULL, 3)`, want: "5"},
		{expr: `least(1, 5, NULL, 3)`, want: "1"},
		{expr: `least(NULL, NULL)`, want: "NULL"},
		{expr: `greatest('b', 'a')`, want: "b"},
		// Predicates.
		{expr: `5 BETWEEN 1 AND 10`, want: "t"},
		{expr: `5 NOT BETWEEN 1 AND 10`, want: "f"},
		{expr: `5 BETWEEN 10 AND 1`, want: "f"},
		{expr: `5 BETWEEN SYMMETRIC 10 AND 1`, want: "t"},
		{expr: `NULL BETWEEN 1 AND 2`, want: "NULL"},
		{expr: `NULL = 1`, want: "NULL"},
		{expr: `1 IN (2, NULL)`, want: "NULL"},
		{expr: `1 IN (1, NULL)`, want: "t"},
		{expr: `NULL AND FALSE`, want: "f"},
		{expr: `NULL OR TRUE`, want: "t"},
		{expr: `NULL OR FALSE`, want: "NULL"},
		{expr: `NOT (NULL = 1)`, want: "NULL"},
		{expr: `'abc' LIKE 'a%'`, want: "t"},
		{expr: `'a%c' LIKE 'a#%c' ESCAPE '#'`, want: "t"},
		{expr: `'abc' LIKE 'a#%c' ESCAPE '#'`, want: "f"},
		{expr: `'a\c' LIKE 'a\c' ESCAPE ''`, want: "t"},
		{expr: `'abc' SIMILAR TO '(a|b)%'`, want: "t"},
		{expr: `'abc' SIMILAR TO 'a_'`, want: "f"},
		{expr: `'abc' NOT SIMILAR TO 'ab[cd]'`, want: "f"},
		{expr: `'abc' SIMILAR TO '%(b|d)%'`, want: "t"},
		{expr: `NULL IS DISTINCT FROM NULL`, want: "f"},
		{expr: `1 IS DISTINCT FROM NULL`, want: "t"},
		{expr: `1 IS NOT DISTINCT FROM 1`, want: "t"},
		{expr: `1 IS DISTINCT FROM 2`, want: "t"},
		{expr: `NULL IS TRUE`, want: "f"},
		{expr: `NULL IS NOT TRUE`, want: "t"},
		{expr: `(1 = 1) IS TRUE`, want: "t"},
		{expr: `(1 = 2) IS FALSE`, want: "t"},
		{expr: `(1 = 2) IS NOT FALSE`, want: "f"},
		{expr: `nosuch(1)`, code: "42883"},
		{expr: `length(1, 2)`, code: "42601"},
		// Casts.
		{expr: `'42'::int`, want: "42"},
		{expr: `' 42 '::int8 + 1`, want: "43"},
		{expr: `'x'::int`, code: "22P02"},
		{expr: `2.5::int`, want: "3"},
		{expr: `(-2.5)::int`, want: "-3"},
		{expr: `2.5::float8::int`, want: "2"},
		{expr: `3000000000::int4`, code: "22003"},
		{expr: `40000::int2`, code: "22003"},
		{expr: `'99999999999999999999'::int8`, code: "22003"},
		{expr: `CAST('1.5' AS float8) * 2`, want: "3"},
		{expr: `1::text || 'a'`, want: "1a"},
		{expr: `(1 + 2)::text || 'x'`, want: "3x"},
		{expr: `'t'::bool`, want: "t"},
		{expr: `'off'::boolean`, want: "f"},
		{expr: `'maybe'::bool`, code: "22P02"},
		{expr: `1::bool`, want: "t"},
		{expr: `true::int`, want: "1"},
		{expr: `123.456::numeric(5,2)`, want: "123.46"},
		{expr: `12345.6::numeric(5,2)`, code: "22003"},
		{expr: `'abcdef'::varchar(2)`, want: "ab"},
		{expr: `'2024-01-02'::date`, want: "2024-01-02"},
		{expr: `'2024-01-02 03:04:05'::timestamp`, want: "2024-01-02 03:04:05+00"},
		{expr: `'2024-01-02 03:04:05'::timestamptz::date`, want: "2024-01-02"},
		{expr: `'not a date'::date`, code: "22007"},
		{expr: `'1.25'::numeric + 1`, want: "2.25"},
		{expr: `0.1::float8::numeric`, want: "0.1"},
		{expr: `'{"a": 1}'::jsonb`, want: `{"a":1}`},
		{expr: `'{'::jsonb`, code: "22P02"},
		{expr: `'a'::jsonb`, code: "22P02"},
		{expr: `'x'::text::name::oid`, want: "x"},
		{expr: `'2261-12-31 23:59:59Z'::timestamptz`, want: "2261-12-31 23:59:59+00"},
		{expr: `'2999-01-01 00:00:00Z'::timestamptz`, code: "22008"}, // beyond the int64-nanosecond range: refused, never wrapped
		{expr: `'1600-01-01 00:00:00Z'::timestamptz`, code: "22008"},
		{expr: `date_trunc('day', '2999-01-01 00:00:00Z')`, code: "22008"},
		{expr: `'0f0a'::bytea`, want: `\x30663061`}, // escape format: the characters themselves
		{expr: `'\x0f0a'::bytea`, want: `\x0f0a`},
		{expr: `'8c9f3d2e-0a1b-4c5d-8e7f-1a2b3c4d5e6f'::uuid`, want: "8c9f3d2e-0a1b-4c5d-8e7f-1a2b3c4d5e6f"},
		{expr: `'nope'::uuid`, code: "22P02"},
		{expr: `'5'::int BETWEEN 1 AND 10`, want: "t"},
		// Date and time.
		{expr: `extract(year FROM '2024-03-15 13:45:30'::timestamptz)`, want: "2024"},
		{expr: `extract(month FROM '2024-03-15'::date)`, want: "3"},
		{expr: `extract(dow FROM '2024-03-15'::date)`, want: "5"},
		{expr: `extract(isodow FROM '2024-03-17'::date)`, want: "7"},
		{expr: `extract(doy FROM '2024-03-15'::date)`, want: "75"},
		{expr: `extract(epoch FROM '1970-01-02 00:00:00'::timestamptz)`, want: "86400"},
		{expr: `extract(second FROM '2024-03-15 13:45:30.25'::timestamptz)`, want: "30.25"},
		{expr: `date_part('hour', '2024-03-15 13:45:30'::timestamptz)`, want: "13"},
		{expr: `extract(week FROM '2024-01-01'::date)`, want: "1"},
		{expr: `extract(quarter FROM '2024-11-01'::date)`, want: "4"},
		{expr: `extract(century FROM '2024-11-01'::date)`, want: "21"},
		{expr: `extract(fortnight FROM '2024-11-01'::date)`, code: "22023"},
		{expr: `date_trunc('month', '2024-03-15 13:45:30'::timestamptz)`, want: "2024-03-01 00:00:00+00"},
		{expr: `date_trunc('hour', '2024-03-15 13:45:30'::timestamptz)`, want: "2024-03-15 13:00:00+00"},
		{expr: `date_trunc('week', '2024-03-15'::date)`, want: "2024-03-11 00:00:00+00"},
		{expr: `date_trunc('quarter', '2024-05-15'::date)`, want: "2024-04-01 00:00:00+00"},
		{expr: `'2024-03-15 13:45:30'::timestamptz + '1 day'`, want: "2024-03-16 13:45:30+00"},
		{expr: `'2024-01-31'::timestamptz + '1 month'`, want: "2024-02-29 00:00:00+00"},
		{expr: `'2024-03-15 13:45:30'::timestamptz - '2 hours 30 minutes'`, want: "2024-03-15 11:15:30+00"},
		{expr: `'2024-03-15'::timestamptz + '1 week 1 day 01:30:00'`, want: "2024-03-23 01:30:00+00"},
		{expr: `'2024-03-15'::timestamptz + 'P1DT2H'`, want: "2024-03-16 02:00:00+00"},
		{expr: `'2024-03-15'::date + 3`, want: "2024-03-18"},
		{expr: `'2024-03-15'::date - '2024-03-01'::date`, want: "14"},
		{expr: `'2024-03-15'::date + '2 days'`, want: "2024-03-17 00:00:00+00"}, // a timestamp, as in PostgreSQL
		{expr: `'2024-03-16 12:00:00'::timestamptz - '2024-03-15 10:30:00'::timestamptz`, want: "1 day 01:30:00"},
		{expr: `'2024-03-15'::timestamptz + 'bogus'`, code: "22007"},
		{expr: `age('2024-03-15'::timestamptz, '2022-01-20'::timestamptz)`, want: "2 years 1 mon 24 days"},
		{expr: `age('2024-03-15 10:00'::timestamptz, '2024-03-15 08:30'::timestamptz)`, want: "01:30:00"},
		{expr: `to_timestamp(86400)`, want: "1970-01-02 00:00:00+00"},
		{expr: `to_timestamp('15/03/2024 13:45', 'DD/MM/YYYY HH24:MI')`, want: "2024-03-15 13:45:00+00"},
		{expr: `to_date('March 5, 2024', 'Month DD, YYYY')`, want: "2024-03-05"},
		{expr: `to_char('2024-03-05 13:07:09'::timestamptz, 'YYYY-MM-DD HH12:MI:SS AM')`, want: "2024-03-05 01:07:09 PM"},
		{expr: `to_char('2024-03-05'::date, 'Day, DD Month YYYY')`, want: "Tuesday  , 05 March     2024"},
		{expr: `to_char('2024-03-05'::date, 'FMDay, DD FMMonth YYYY')`, want: "Tuesday, 05 March 2024"},
		{expr: `to_char('2024-03-05'::date, 'Dy Mon DD "Q"Q')`, want: "Tue Mar 05 Q1"},
		{expr: `to_char(1234.5, '9,999.99')`, want: " 1,234.50"},
		{expr: `to_char(-12.345, 'FM999.00')`, want: "-12.35"},
		{expr: `to_char(7, '000')`, want: " 007"},
		{expr: `make_date(2024, 2, 29)`, want: "2024-02-29"},
		{expr: `make_date(2023, 2, 29)`, code: "22008"},
		{expr: `make_timestamp(2024, 3, 15, 13, 45, 30.5)`, want: "2024-03-15 13:45:30.5+00"},
		{expr: `make_interval(1, 2, 0, 3, 4, 5, 6)`, want: "1 year 2 mons 3 days 04:05:06"},
		{expr: `justify_hours('30 hours')`, want: "1 day 06:00:00"},
		{expr: `current_date = now()::date`, want: "t"},
		{expr: `current_timestamp = now()`, want: "t"},
		{expr: `localtimestamp <= clock_timestamp()`, want: "t"},
		{expr: `now() - '1 day' < now()`, want: "t"},
	})

	// Over table rows: predicates as filters, BETWEEN as index bounds,
	// an escape in a filter, and NULL semantics of NOT BETWEEN.
	execSQL(t, ctx, s, `CREATE TABLE t (id INT8 PRIMARY KEY, name TEXT, n INT8, flag BOOL)`)
	execSQL(t, ctx, s, `INSERT INTO t VALUES (1, 'alpha', 10, true), (2, 'a_b', 20, false), (3, 'gamma', NULL, NULL), (4, 'delta', 40, true)`)
	execSQL(t, ctx, s, `CREATE INDEX t_n ON t (n)`)
	rows := func(q string) string {
		r := execSQL(t, ctx, s, q)
		var out []string
		for _, row := range r.Rows {
			out = append(out, row[0].Text())
		}
		return strings.Join(out, ",")
	}
	if got := rows(`SELECT id FROM t WHERE n BETWEEN 15 AND 45 ORDER BY id`); got != "2,4" {
		t.Fatalf("BETWEEN: %s", got)
	}
	if pl := explainPlan(t, ctx, s, `SELECT id FROM t WHERE n BETWEEN 15 AND 45`); !strings.Contains(pl, `"t_n"`) {
		t.Fatalf("BETWEEN not planned as index bounds: %s", pl)
	}
	if got := rows(`SELECT id FROM t WHERE n NOT BETWEEN 15 AND 45 ORDER BY id`); got != "1" {
		t.Fatalf("NOT BETWEEN (NULL excluded): %s", got)
	}
	if got := rows(`SELECT id FROM t WHERE name LIKE 'a$_%' ESCAPE '$'`); got != "2" {
		t.Fatalf("LIKE ESCAPE: %s", got)
	}
	if got := rows(`SELECT id FROM t WHERE flag IS NOT TRUE ORDER BY id`); got != "2,3" {
		t.Fatalf("IS NOT TRUE: %s", got)
	}
	if got := rows(`SELECT id FROM t WHERE n IS DISTINCT FROM 10 ORDER BY id`); got != "2,3,4" {
		t.Fatalf("IS DISTINCT FROM: %s", got)
	}
	if got := rows(`SELECT id FROM t WHERE name SIMILAR TO '(alpha|delta)' ORDER BY id`); got != "1,4" {
		t.Fatalf("SIMILAR TO: %s", got)
	}
	if got := rows(`SELECT upper(left(name, 1)) || substring(name FROM 2) FROM t WHERE id = 1`); got != "Alpha" {
		t.Fatalf("string functions over a row: %s", got)
	}
	if got := rows(`SELECT n % 15 FROM t WHERE id = 4`); got != "10" {
		t.Fatalf("modulo over a row: %s", got)
	}
	if got := rows(`SELECT coalesce(n, 0) + 1 FROM t WHERE id = 3`); got != "1" {
		t.Fatalf("coalesce over a row: %s", got)
	}
	// A LIKE prefix becomes index bounds (the LIKE still filters).
	execSQL(t, ctx, s, `CREATE INDEX t_name ON t (name)`)
	if pl := explainPlan(t, ctx, s, `SELECT id FROM t WHERE name LIKE 'al%'`); !strings.Contains(pl, `"t_name"`) {
		t.Fatalf("LIKE prefix not planned as index bounds: %s", pl)
	}
	if got := rows(`SELECT id FROM t WHERE name LIKE 'a%' ORDER BY id`); got != "1,2" {
		t.Fatalf("LIKE prefix rows: %s", got)
	}
	if got := rows(`SELECT id FROM t WHERE name LIKE 'a_pha'`); got != "1" {
		t.Fatalf("LIKE with a later wildcard: %s", got)
	}
	if pl := explainPlan(t, ctx, s, `SELECT id FROM t WHERE name LIKE '%a'`); strings.Contains(pl, `"t_name"`) {
		t.Fatalf("a leading wildcard must not use the index: %s", pl)
	}
	// SHOW FUNCTIONS and pg_proc list the registry.
	if got := rows(`SELECT proname FROM pg_proc WHERE proname IN ('substring', 'substr', 'now', 'nextval') ORDER BY 1`); got != "nextval,now,substr,substring" {
		t.Fatalf("pg_proc: %s", got)
	}
	if got := rows(`SELECT count(*) FROM pg_proc WHERE provolatile = 'v'`); got == "0" {
		t.Fatalf("pg_proc volatility: %s", got)
	}
	if got := rows(`SELECT id FROM t WHERE greatest(n, 25) = 25 ORDER BY id`); got != "1,2,3" { // greatest ignores the NULL
		t.Fatalf("greatest in WHERE: %s", got)
	}
	r := execSQL(t, ctx, s, `SHOW FUNCTIONS`)
	if len(r.Rows) < 80 || r.Columns[0].Name != "name" {
		t.Fatalf("SHOW FUNCTIONS: %d rows", len(r.Rows))
	}
	found := false
	for _, row := range r.Rows {
		if row[0].S == "split_part" && strings.Contains(row[1].S, "split_part(text, text, int8) → text") && row[3].S == "immutable" {
			found = true
		}
	}
	if !found {
		t.Fatalf("SHOW FUNCTIONS lacks split_part")
	}
}

// TestExpressionsJSONAndAggregates: the jsonb functions and operators
// (paths, containment, key existence, building, setting), and the
// aggregates over expressions — DISTINCT, FILTER, string_agg, array_agg,
// bool_and/or, statistics, percentiles, the json aggregates — over a
// table, with GROUP BY, HAVING, and over a join.
func TestExpressionsJSONAndAggregates(t *testing.T) {
	tc := Start(t, 1)
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	s := sql.NewSession(tc.Nodes[0].DB(), catalog.NewAccessor())
	waitForDatabases(t, ctx, s)

	runExprCases(t, ctx, s, []exprCase{
		{expr: `'{"a": {"b": [10, 20, 30]}}'::jsonb #> '{a,b,1}'`, want: "20"},
		{expr: `'{"a": {"b": [10, 20, 30]}}'::jsonb #>> '{a,b,-1}'`, want: "30"},
		{expr: `'{"a": {"b": "x"}}'::jsonb #>> '{a,b}'`, want: "x"},
		{expr: `'{"a": 1}'::jsonb #> '{z}'`, want: "NULL"},
		{expr: `'[1, 2, 3]'::jsonb -> 0`, want: "1"},
		{expr: `'[1, 2, 3]'::jsonb ->> -1`, want: "3"},
		{expr: `'{"a": {"b": 2}}'::jsonb -> 'a' ->> 'b'`, want: "2"},
		{expr: `'{"a": 1, "b": 2}'::jsonb @> '{"a": 1}'`, want: "t"},
		{expr: `'{"a": 1}'::jsonb <@ '{"a": 1, "b": 2}'`, want: "t"},
		{expr: `'{"a": 1}'::jsonb ? 'a'`, want: "t"},
		{expr: `'{"a": 1}'::jsonb ? 'z'`, want: "f"},
		{expr: `'["x", "y"]'::jsonb ? 'y'`, want: "t"},
		{expr: `'{"a": 1, "b": 2}'::jsonb ?| '{z,b}'`, want: "t"},
		{expr: `'{"a": 1, "b": 2}'::jsonb ?& '{a,b}'`, want: "t"},
		{expr: `'{"a": 1, "b": 2}'::jsonb ?& '{a,z}'`, want: "f"},
		{expr: `jsonb_build_object('a', 1, 'b', 'x', 'c', NULL, 'd', true)`, want: `{"a":1,"b":"x","c":null,"d":true}`},
		{expr: `jsonb_build_array(1, 'x', NULL, '{"k": 1}'::jsonb)`, want: `[1,"x",null,{"k":1}]`},
		{expr: `jsonb_build_object('a')`, code: "22023"},
		{expr: `jsonb_array_length('[1, 2, 3]'::jsonb)`, want: "3"},
		{expr: `jsonb_array_length('{}'::jsonb)`, code: "22023"},
		{expr: `jsonb_typeof('{}'::jsonb) || ',' || jsonb_typeof('[]'::jsonb) || ',' || jsonb_typeof('1'::jsonb) || ',' || jsonb_typeof('"x"'::jsonb) || ',' || jsonb_typeof('null'::jsonb) || ',' || jsonb_typeof('true'::jsonb)`, want: "object,array,number,string,null,boolean"},
		{expr: `jsonb_extract_path('{"a": {"b": 2}}'::jsonb, 'a', 'b')`, want: "2"},
		{expr: `jsonb_extract_path_text('{"a": {"b": "q"}}'::jsonb, 'a', 'b')`, want: "q"},
		{expr: `jsonb_set('{"a": 1, "b": {"c": 2}}'::jsonb, '{b,c}', '9'::jsonb)`, want: `{"a":1,"b":{"c":9}}`},
		{expr: `jsonb_set('{"a": 1}'::jsonb, '{z}', '"new"'::jsonb)`, want: `{"a":1,"z":"new"}`},
		{expr: `jsonb_set('{"a": 1}'::jsonb, '{z}', '"new"'::jsonb, false)`, want: `{"a":1}`},
		{expr: `jsonb_set('[1, 2, 3]'::jsonb, '{1}', '"two"'::jsonb)`, want: `[1,"two",3]`},
		{expr: `to_jsonb('x')`, want: `"x"`},
		{expr: `to_jsonb(1.5)`, want: "1.5"},
		{expr: `to_jsonb(true)`, want: "true"},
		{expr: `jsonb_strip_nulls('{"a": null, "b": {"c": null, "d": 1}}'::jsonb)`, want: `{"b":{"d":1}}`},
		{expr: `jsonb_pretty('{"a": [1]}'::jsonb)`, want: "{\n    \"a\": [\n        1\n    ]\n}"},
		{expr: `gen_random_uuid() = gen_random_uuid()`, want: "f"},
		{expr: `length(uuid_generate_v4()::text)`, want: "36"},
	})

	execSQL(t, ctx, s, `CREATE TABLE o (id INT8 PRIMARY KEY, city TEXT, amount DECIMAL, qty INT8, ok BOOL, tags JSONB)`)
	execSQL(t, ctx, s, `INSERT INTO o VALUES
		(1, 'oslo', 10.5, 1, true, '{"vip": true, "n": 1}'),
		(2, 'oslo', 20, 2, false, '{"vip": false, "n": 2}'),
		(3, 'oslo', NULL, 3, true, '{"n": 3}'),
		(4, 'rome', 5, 2, true, '{"vip": true, "n": 4}'),
		(5, 'rome', 15, NULL, NULL, NULL)`)
	execSQL(t, ctx, s, `CREATE TABLE c (city TEXT PRIMARY KEY, country TEXT)`)
	execSQL(t, ctx, s, `INSERT INTO c VALUES ('oslo', 'NO'), ('rome', 'IT')`)
	rows := func(q string) string {
		r := execSQL(t, ctx, s, q)
		var out []string
		for _, row := range r.Rows {
			var cells []string
			for _, d := range row {
				if d.Null {
					cells = append(cells, "NULL")
				} else {
					cells = append(cells, d.Text())
				}
			}
			out = append(out, strings.Join(cells, "|"))
		}
		return strings.Join(out, ";")
	}
	check := func(q, want string) {
		t.Helper()
		if got := rows(q); got != want {
			t.Errorf("%s\n  got  %q\n  want %q", q, got, want)
		}
	}
	check(`SELECT count(*), count(qty), count(DISTINCT qty), sum(DISTINCT qty), sum(qty * 2), avg(amount) FROM o`, "5|4|3|6|16|12.625")
	check(`SELECT min(amount + 1), max(upper(city)), max(length(city) * qty) FROM o`, "6|ROME|12")
	check(`SELECT string_agg(city, ',') FROM o`, "oslo,oslo,oslo,rome,rome")
	check(`SELECT string_agg(DISTINCT city, ', ') FROM o`, "oslo, rome")
	check(`SELECT array_agg(qty) FROM o`, "{1,2,3,2,NULL}")
	check(`SELECT bool_and(ok), bool_or(ok), every(ok) FROM o`, "f|t|f")
	check(`SELECT count(*) FILTER (WHERE ok), sum(amount) FILTER (WHERE city = 'rome'), avg(qty) FILTER (WHERE qty > 1) FROM o`, "3|20|2.333333")
	check(`SELECT stddev(qty), var_pop(qty), stddev_pop(amount) FROM o`, "0.816496580927726|0.5|5.53821947921893")
	check(`SELECT percentile_cont(0.5) WITHIN GROUP (ORDER BY qty), percentile_disc(0.5) WITHIN GROUP (ORDER BY amount), percentile_cont(0.25) WITHIN GROUP (ORDER BY qty DESC) FROM o`, "2|10.5|2.25")
	check(`SELECT jsonb_agg(qty), jsonb_object_agg(id, city) FROM o WHERE id <= 2`, `[1,2]|{"1":"oslo","2":"oslo"}`)
	check(`SELECT json_agg(tags -> 'n') FROM o WHERE city = 'rome'`, "[4,null]")
	check(`SELECT city, count(*), sum(qty), string_agg(id::text, '+'), bool_and(tags @> '{"vip": true}') FROM o GROUP BY city ORDER BY city`, "oslo|3|6|1+2+3|f;rome|2|2|4+5|t") // the NULL tags row is UNKNOWN, skipped
	check(`SELECT city, count(*) FILTER (WHERE tags ? 'vip') FROM o GROUP BY city HAVING count(*) > 2 ORDER BY city`, "oslo|2")
	check(`SELECT city FROM o GROUP BY city HAVING sum(qty * 2) >= 12 ORDER BY city`, "oslo")
	check(`SELECT id FROM o WHERE tags @> '{"vip": true}' ORDER BY id`, "1;4")
	check(`SELECT id FROM o WHERE tags ? 'vip' AND NOT (tags @> '{"vip": false}') ORDER BY id`, "1;4")
	check(`SELECT id FROM o WHERE tags #>> '{n}' = '3'`, "3")
	check(`SELECT id, tags -> 'n' FROM o WHERE (tags ->> 'n')::int > 2 ORDER BY id`, "3|3;4|4")
	check(`SELECT c.country, count(*), sum(o.qty * 10), string_agg(o.city || ':' || o.id::text, ' ') FILTER (WHERE o.ok) FROM o JOIN c ON o.city = c.city GROUP BY c.country ORDER BY c.country`, "IT|2|20|rome:4;NO|3|60|oslo:1 oslo:3")
	check(`SELECT c.country FROM o JOIN c ON o.city = c.city GROUP BY c.country HAVING bool_or(o.tags @> '{"vip": true}') ORDER BY c.country`, "IT;NO")
	if _, serr := trySQL(ctx, s, `SELECT percentile_cont(0.5) FROM o`); serr == nil || serr.Code != sql.CodeSyntaxError {
		t.Errorf("percentile without WITHIN GROUP: %v", serr)
	}
	if _, serr := trySQL(ctx, s, `SELECT sum(city) FROM o`); serr == nil || serr.Code != sql.CodeFeatureNotSupported {
		t.Errorf("sum over text: %v", serr)
	}
}
