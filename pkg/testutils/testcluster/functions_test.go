package testcluster

import (
	"context"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/sthorne/datax/pkg/sql"
	"github.com/sthorne/datax/pkg/sql/catalog"
)

// queryCells runs a query and renders every cell of every row as
// text ("NULL" for NULL), cells joined by "|", rows by ";".
func queryCells(t *testing.T, ctx context.Context, conn *pgx.Conn, q string, args ...any) (string, error) {
	t.Helper()
	rows, err := conn.Query(ctx, q, args...)
	if err != nil {
		return "", err
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		raw := rows.RawValues()
		cells := make([]string, len(raw))
		for i, v := range raw {
			cells[i] = "NULL"
			if v != nil {
				cells[i] = string(v)
			}
		}
		out = append(out, strings.Join(cells, "|"))
	}
	return strings.Join(out, ";"), rows.Err()
}

// TestCommonFunctions (issue #339): the commonly used PostgreSQL
// function families — the regular-expression functions, the
// set-returning generate_series, jsonb_each, jsonb_array_elements,
// jsonb_object_keys and regexp_split_to_table / regexp_matches (in
// FROM and in a FROM-less select list), jsonb_insert, and the string,
// math, date and array functions an application or ORM reaches for —
// with NULL handling and error codes, in text and binary results.
func TestCommonFunctions(t *testing.T) {
	tc := Start(t, 1)
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()
	s := sql.NewSession(tc.Nodes[0].DB(), catalog.NewAccessor())
	waitForDatabases(t, ctx, s)

	runExprCases(t, ctx, s, []exprCase{
		// Regular expressions.
		{expr: `regexp_replace('foobarbaz', 'b(..)', 'X\1Y', 'g')`, want: "fooXarYXazY"},
		{expr: `regexp_replace('Hello World', '[aeiou]', '_')`, want: "H_llo World"},
		{expr: `regexp_replace('Hello World', '[AEIOU]', '_', 'gi')`, want: "H_ll_ W_rld"},
		{expr: `regexp_replace('  a   b ', '\s+', ' ', 'g')`, want: " a b "},
		{expr: `regexp_replace('a', '(', 'b')`, code: "2201B"},
		{expr: `regexp_replace(NULL, 'a', 'b')`, want: "NULL"},
		{expr: `regexp_match('foobarbequebaz', '(bar)(beque)')`, want: "{bar,beque}"},
		{expr: `regexp_match('abc', 'z')`, want: "NULL"},
		{expr: `regexp_match('abc', 'b', 'g')`, code: "22023"},
		{expr: `regexp_split_to_array('hello world', '\s+')`, want: "{hello,world}"},
		{expr: `regexp_count('ABCABCAXYaxy', 'A.', 1, 'i')`, want: "4"},
		{expr: `regexp_like('Hello', '^h', 'i')`, want: "t"},
		{expr: `regexp_substr('number of your street, town zip, FR', '[^,]+', 1, 2)`, want: " town zip"},
		{expr: `'abc' ~ 'b'`, want: "t"},
		// Strings (already present; the audit's coverage).
		{expr: `split_part('a,b,c', ',', 2)`, want: "b"},
		{expr: `lpad('7', 3, '0') || rpad('x', 3, '.')`, want: "007x.."},
		{expr: `initcap('hello wORLD')`, want: "Hello World"},
		{expr: `translate('12345', '143', 'ax')`, want: "a2x5"},
		{expr: `concat_ws(', ', 'a', NULL, 'b')`, want: "a, b"},
		{expr: `left('abcde', 2) || right('abcde', 2)`, want: "abde"},
		{expr: `repeat('ab', 2) || reverse('xyz')`, want: "ababzyx"},
		{expr: `starts_with('alphabet', 'alph')`, want: "t"},
		{expr: `md5('abc')`, want: "900150983cd24fb0d6963f7d28e17f72"},
		{expr: `encode(decode('68656c6c6f', 'hex'), 'base64')`, want: "aGVsbG8="},
		// Math.
		{expr: `round(42.4382, 2)`, want: "42.44"},
		{expr: `trunc(42.4382, 1)`, want: "42.4"},
		{expr: `ceil(-42.8) + floor(-42.8)`, want: "-85"},
		{expr: `power(2, 10)`, want: "1024"},
		{expr: `sqrt(16)`, want: "4"},
		{expr: `ln(exp(1))`, want: "1"},
		{expr: `log(100)`, want: "2"},
		{expr: `mod(-7, 3)`, want: "-1"},
		{expr: `sign(-0.5)`, want: "-1"},
		{expr: `width_bucket(5.35, 0.024, 10.06, 5)`, want: "3"},
		{expr: `greatest(1, 5, NULL, 3) + least(4, 2)`, want: "7"},
		// Date and time.
		{expr: `date_trunc('month', '2024-02-15 10:00:00+00'::timestamptz)`, want: "2024-02-01 00:00:00+00"},
		{expr: `date_part('dow', '2024-02-15'::date)`, want: "4"},
		{expr: `make_date(2024, 2, 29)`, want: "2024-02-29"},
		{expr: `age('2024-03-01'::timestamptz, '2023-01-01'::timestamptz)`, want: "1 year 2 mons"},
		{expr: `to_char('2024-07-04 13:05:00+00'::timestamptz, 'YYYY-MM-DD HH24:MI')`, want: "2024-07-04 13:05"},
		{expr: `justify_hours('27 hours'::interval)`, want: "1 day 03:00:00"},
		// JSON.
		{expr: `jsonb_insert('{"a":[0,1,2]}', '{a,1}', '"new"')`, want: `{"a":[0,"new",1,2]}`},
		{expr: `jsonb_insert('{"a":[0,1,2]}', '{a,1}', '"new"', true)`, want: `{"a":[0,1,"new",2]}`},
		{expr: `jsonb_insert('{"a":1}', '{b}', '2')`, want: `{"a":1,"b":2}`},
		{expr: `jsonb_insert('{"a":1}', '{a}', '2')`, code: "22023"},
		{expr: `jsonb_set('{"a":1}', '{b}', '2')`, want: `{"a":1,"b":2}`},
		{expr: `jsonb_typeof(jsonb_build_object('a', 1) -> 'a')`, want: "number"},
		// Arrays.
		{expr: `array_to_string(array_remove(array_append(ARRAY[1, 2], 3), 2), ',')`, want: "1,3"},
		{expr: `cardinality(string_to_array('a,b,c', ','))`, want: "3"},
		// Set-returning functions refuse to be a per-row value.
		{expr: `1 + generate_series(1, 2)`, code: "0A000"},
	})

	execSQL(t, ctx, s, `CREATE TABLE fx (id INT8 PRIMARY KEY, name TEXT, doc JSONB)`)
	execSQL(t, ctx, s, `INSERT INTO fx VALUES (1, 'Ann  Lee', '{"tags":["a","b"]}'), (2, 'Bo', '{"tags":[]}'), (3, NULL, NULL)`)

	conn, err := pgx.Connect(ctx, pgURL(tc, 0))
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close(ctx)

	// Each query in both protocols (text results; the binary decoding
	// is checked by the typed scans after).
	for _, mode := range []pgx.QueryExecMode{pgx.QueryExecModeExec, pgx.QueryExecModeSimpleProtocol} {
		for _, c := range []struct {
			q    string
			args []any
			want string
		}{
			{q: `SELECT * FROM generate_series(1, 10, 4) AS g(n)`, want: "1;5;9"},
			{q: `SELECT n FROM generate_series(5, 1, -2) n`, want: "5;3;1"},
			{q: `SELECT generate_series(1, 3)`, want: "1;2;3"},
			{q: `SELECT generate_series(1, 2) * 10, 'x'`, want: "10|x;20|x"},
			{q: `SELECT generate_series(1, 2), generate_series(1, 3)`, want: "1|1;2|2;NULL|3"},
			{q: `SELECT generate_series(1, 0)`, want: ""},
			{q: `SELECT * FROM generate_series(0.5, 2, 0.5)`, want: "0.5;1.0;1.5;2.0"},
			{q: `SELECT count(*), sum(n) FROM generate_series(1, 100) AS g(n)`, want: "100|5050"},
			{q: `SELECT n FROM generate_series(1, 10) n WHERE n % 3 = 0 ORDER BY n DESC LIMIT 2`, want: "9;6"},
			{q: `SELECT g.n, f.name FROM generate_series(1, 3) AS g(n) JOIN fx f ON f.id = g.n ORDER BY g.n`, want: "1|Ann  Lee;2|Bo;3|NULL"},
			{q: `WITH d AS (SELECT * FROM generate_series(1, 3) AS g(n)) SELECT sum(n) FROM d`, want: "6"},
			{q: `SELECT key, value FROM jsonb_each('{"b":[1,2],"a":"x"}') ORDER BY key`, want: `a|"x";b|[1,2]`},
			{q: `SELECT key, value FROM jsonb_each_text('{"b":2,"a":"x","n":null}')`, want: `a|x;b|2;n|NULL`},
			{q: `SELECT * FROM jsonb_array_elements('[1,"a",null]')`, want: `1;"a";null`},
			{q: `SELECT value FROM jsonb_array_elements_text('["a","b"]') AS e WHERE value <> 'a'`, want: `b`},
			{q: `SELECT jsonb_array_elements_text('["x","y"]')`, want: `x;y`},
			{q: `SELECT * FROM jsonb_object_keys('{"k2":1,"k1":2}')`, want: `k1;k2`},
			{q: `SELECT * FROM json_array_elements_text('[1,2]') AS x(v)`, want: `1;2`},
			{q: `SELECT * FROM regexp_split_to_table('the quick  fox', '\s+') AS w`, want: "the;quick;fox"},
			{q: `SELECT regexp_matches('foobarbequebazilbarfbonk', '(b[^b]+)(b[^b]+)', 'g')`, want: "{bar,beque};{bazil,barf}"},
			{q: `SELECT regexp_replace(name, '\s+', ' ', 'g') FROM fx ORDER BY id`, want: "Ann Lee;Bo;NULL"},
			{q: `SELECT id FROM fx WHERE regexp_like(name, '^a', 'i')`, want: "1"},
			{q: `SELECT n FROM generate_series($1::int8, $2::int8) n`, args: []any{int64(2), int64(4)}, want: "2;3;4"},
		} {
			got, err := queryCells(t, ctx, conn, c.q, append([]any{mode}, c.args...)...)
			if err != nil {
				t.Errorf("%v %s: %v", mode, c.q, err)
				continue
			}
			if got != c.want {
				t.Errorf("%v %s = %q, want %q", mode, c.q, got, c.want)
			}
		}
	}

	// Typed scans: the series' column types describe and decode.
	var ns []int64
	rows, _ := conn.Query(ctx, `SELECT * FROM generate_series(1, 3)`)
	ns, err = pgx.CollectRows(rows, pgx.RowTo[int64])
	if err != nil || !reflect.DeepEqual(ns, []int64{1, 2, 3}) {
		t.Fatalf("generate_series as int8: %v %v", ns, err)
	}
	rows, _ = conn.Query(ctx, `SELECT g FROM generate_series('2024-03-09 00:00:00+00'::timestamptz, '2024-03-11', interval '1 day') g`)
	days, err := pgx.CollectRows(rows, pgx.RowTo[time.Time])
	if err != nil || len(days) != 3 || !days[2].Equal(time.Date(2024, 3, 11, 0, 0, 0, 0, time.UTC)) {
		t.Fatalf("generate_series over timestamps: %v %v", days, err)
	}
	var doc map[string]any
	var key string
	if err := conn.QueryRow(ctx, `SELECT key, value FROM jsonb_each('{"a":{"b":true}}')`).Scan(&key, &doc); err != nil || key != "a" || doc["b"] != true {
		t.Fatalf("jsonb_each binary: %q %v %v", key, doc, err)
	}
	var m []string
	if err := conn.QueryRow(ctx, `SELECT regexp_match('2024-07-04', '(\d+)-(\d+)-(\d+)')`).Scan(&m); err != nil || !reflect.DeepEqual(m, []string{"2024", "07", "04"}) {
		t.Fatalf("regexp_match as text[]: %v %v", m, err)
	}
	sd, err := conn.Prepare(ctx, "each", `SELECT * FROM jsonb_each('{"a":1}')`)
	if err != nil || len(sd.Fields) != 2 || sd.Fields[0].Name != "key" || sd.Fields[0].DataTypeOID != 25 || sd.Fields[1].DataTypeOID != 3802 {
		t.Fatalf("jsonb_each describes as (key text, value jsonb): %+v %v", sd, err)
	}
	if sd, err = conn.Prepare(ctx, "gs", `SELECT generate_series(1, 2)`); err != nil || sd.Fields[0].Name != "generate_series" || sd.Fields[0].DataTypeOID != 20 {
		t.Fatalf("generate_series in the select list describes as generate_series int8: %+v %v", sd, err)
	}

	// INSERT ... SELECT from a series.
	execSQL(t, ctx, s, `CREATE TABLE nums (n INT8 PRIMARY KEY, label TEXT)`)
	if _, err := conn.Exec(ctx, `INSERT INTO nums SELECT n, 'n' || n FROM generate_series(1, 50) AS g(n)`); err != nil {
		t.Fatal(err)
	}
	var count int
	if err := conn.QueryRow(ctx, `SELECT count(*) FROM nums WHERE label LIKE 'n%'`).Scan(&count); err != nil || count != 50 {
		t.Fatalf("INSERT ... SELECT FROM generate_series: %d %v", count, err)
	}

	// Refusals, with PostgreSQL's codes.
	for q, want := range map[string]string{
		`SELECT * FROM generate_series(1, 3, 0)`:                   "22023",
		`SELECT * FROM generate_series(now(), now() + '1 day')`:    "42883",
		`SELECT * FROM jsonb_each('[1]')`:                          "22023",
		`SELECT * FROM jsonb_array_elements('{"a":1}')`:            "22023",
		`SELECT * FROM jsonb_object_keys('3')`:                     "22023",
		`SELECT jsonb_each('{"a":1}')`:                             "0A000",
		`SELECT generate_series(1, id) FROM fx`:                    "0A000",
		`SELECT * FROM regexp_split_to_table('a', 'a', 'g')`:       "22023",
		`SELECT * FROM regexp_matches('a', '(?=a)')`:               "2201B",
		`SELECT * FROM generate_series(1, 10000000000) LIMIT 1`:    "53200",
		`SELECT * FROM generate_series(1, 2) AS g(a, b)`:           "42P10",
		`SELECT * FROM jsonb_each('{"a":1}') AS e(k, v, extra)`:    "42P10",
		`SELECT * FROM generate_series('a'::text, 'b'::text) AS g`: "22P02",
	} {
		_, err := queryCells(t, ctx, conn, q, pgx.QueryExecModeSimpleProtocol)
		if pgCode(err) != want {
			t.Errorf("%s: %v, want %s", q, err, want)
		}
	}

	// The registry lists them: SHOW FUNCTIONS, and pg_proc with
	// proretset.
	var retset bool
	if err := conn.QueryRow(ctx, `SELECT proretset FROM pg_proc WHERE proname = 'generate_series'`).Scan(&retset); err != nil || !retset {
		t.Fatalf("pg_proc generate_series proretset: %v %v", retset, err)
	}
	if err := conn.QueryRow(ctx, `SELECT proretset FROM pg_proc WHERE proname = 'regexp_replace'`).Scan(&retset); err != nil || retset {
		t.Fatalf("pg_proc regexp_replace proretset: %v %v", retset, err)
	}
	r := execSQL(t, ctx, s, `SHOW FUNCTIONS`)
	found := map[string]bool{}
	for _, row := range r.Rows {
		found[row[0].Text()] = true
	}
	for _, fn := range []string{"generate_series", "regexp_replace", "regexp_match", "regexp_split_to_table", "jsonb_each", "jsonb_array_elements", "jsonb_insert", "timezone"} {
		if !found[fn] {
			t.Errorf("SHOW FUNCTIONS lacks %s", fn)
		}
	}
}
