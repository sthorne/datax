package testcluster

import (
	"context"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/sthorne/datax/pkg/sql"
	"github.com/sthorne/datax/pkg/sql/catalog"
	"github.com/sthorne/datax/pkg/sql/types"
)

// TestPGOperatorsAndFunctions closes PG-F-001, -005, -009, -010 and
// -011: the regex leftovers, the jsonb operators and subscripting, the
// string and math functions, and the integer bitwise and prefix
// operators, each checked against PostgreSQL's documented results and
// error codes.
func TestPGOperatorsAndFunctions(t *testing.T) {
	tc := Start(t, 1)
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()
	s := sql.NewSession(tc.Nodes[0].DB(), catalog.NewAccessor())
	waitForDatabases(t, ctx, s)

	runExprCases(t, ctx, s, []exprCase{
		// PG-F-011: the bitwise operators, with PostgreSQL's
		// precedence (all "other" operators bind alike, looser than
		// + and -, left to right) and its wrapping shifts.
		{expr: `91 & 15`, want: "11"},
		{expr: `32 | 3`, want: "35"},
		{expr: `17 # 5`, want: "20"},
		{expr: `~1`, want: "-2"},
		{expr: `1 << 4`, want: "16"},
		{expr: `8 >> 2`, want: "2"},
		{expr: `-8 >> 1`, want: "-4"},
		{expr: `1::int8 << 63`, want: "-9223372036854775808"},
		{expr: `1::int8 << 64`, want: "1"},
		{expr: `1 + 2 & 3`, want: "3"},
		{expr: `5 & 3 | 8`, want: "9"},
		{expr: `1 | 2 # 3`, want: "0"},
		{expr: `2 * 3 << 1`, want: "12"},
		{expr: `~1 & 3`, want: "2"},
		{expr: `~ 1 + 2`, want: "-4"},
		{expr: `1 << 2 < 5`, want: "t"},
		{expr: `(5 & 4) = 4`, want: "t"},
		{expr: `'12' & 4`, want: "4"},
		{expr: `NULL & 4`, want: "NULL"},
		{expr: `1.5 & 1`, code: "42883"},
		{expr: `'x' & 1`, code: "22P02"},
		{expr: `@ -5`, want: "5"},
		{expr: `@ -5.5`, want: "5.5"},
		{expr: `|/ 25.0`, want: "5"},
		{expr: `||/ 27.0`, want: "3"},
		{expr: `'a'||'b'`, want: "ab"},

		// PG-F-010: math.
		{expr: `sind(30) + cosd(60) + tand(45) + cotd(45)`, want: "3"},
		{expr: `asind(0.5) + acosd(0.5) + atand(1) + atan2d(1, 0)`, want: "225"},
		{expr: `degrees(pi())`, want: "180"},
		{expr: `radians(180) = pi()`, want: "t"},
		{expr: `acos(2)`, code: "22003"},
		{expr: `acosh(0.5)`, code: "22003"},
		{expr: `cot(0)`, want: "Infinity"},
		{expr: `factorial(20)`, want: "2432902008176640000"},
		{expr: `factorial(-1)`, code: "2201F"},
		{expr: `lcm(12, 18)`, want: "36"},
		{expr: `scale(8.41)`, want: "2"},
		{expr: `trim_scale(8.4100)`, want: "8.41"},
		{expr: `random(1, 1)`, want: "1"},
		{expr: `random(3, 1)`, code: "22023"},
		{expr: `random_normal(5, 0)`, want: "5"},
		{expr: `erf(0)`, want: "0"},
		{expr: `ln(0)`, code: "2201E"},

		// PG-F-009: strings.
		{expr: `overlay('Txxxxas' placing 'hom' from 2 for 4)`, want: "Thomas"},
		{expr: `overlay('Txxxxas' placing 'hom' from 2)`, want: "Thomxas"},
		{expr: `overlay('\x1234567890'::bytea placing '\x0203'::bytea from 2 for 3)`, want: `\x12020390`},
		{expr: `bit_length('jose')`, want: "32"},
		{expr: `to_ascii('Karel', 'LATIN1')`, want: "Karel"},
		{expr: `to_ascii('Karel')`, code: "0A000"},
		{expr: `convert_from(convert_to('café', 'LATIN1'), 'LATIN1')`, want: "café"},
		{expr: `convert_to('€', 'LATIN1')`, code: "22P05"},
		{expr: `unistr('d\0061t\+000061')`, want: "data"},
		{expr: `normalize('é', NFD) = 'e' || chr(769)`, want: "t"},
		{expr: `normalize('e' || chr(769)) = 'é'`, want: "t"},
		{expr: `('e' || chr(769)) IS NORMALIZED`, want: "f"},
		{expr: `('e' || chr(769)) IS NFD NORMALIZED`, want: "t"},
		{expr: `('e' || chr(769)) IS NOT NFC NORMALIZED`, want: "t"},
		{expr: `NULL::text IS NORMALIZED`, want: "NULL"},
		{expr: `parse_ident('"SomeSchema".someTable')`, want: "{SomeSchema,sometable}"},
		{expr: `encode(sha512('abc'), 'hex') = encode(sha512('abc'::bytea), 'hex')`, want: "t"},
		{expr: `crc32('The quick brown fox jumps over the lazy dog')`, want: "1095738169"},
		{expr: `get_byte('\x1234567890'::bytea, 4)`, want: "144"},
		{expr: `set_bit('\x1234567890'::bytea, 30, 0)`, want: `\x1234563890`},
		{expr: `format('|%10s|%-10s|', 'foo', 'bar')`, want: "|       foo|bar       |"},
		{expr: `format('|%*s|', 6, 'x')`, want: "|     x|"},
		{expr: `format('%5d', 1)`, code: "22023"},

		// PG-F-001: regex leftovers.
		{expr: `regexp_instr('number of your street, town zip, FR', '[^,]+', 1, 2)`, want: "23"},
		{expr: `regexp_instr('ABCDEFGHI', '(c..)(...)', 1, 1, 0, 'i', 2)`, want: "6"},
		{expr: `substring('Thomas' from '...$')`, want: "mas"},
		{expr: `substring('foobar' from 'o(.)b')`, want: "o"},
		{expr: `substring('Thomas' from 2 for 3)`, want: "hom"},
		{expr: `substring('Thomas' similar '%#"o_a#"_' escape '#')`, want: "oma"},
		{expr: `substring('Thomas' from '%#"o_a#"_' for '#')`, want: "oma"},
		{expr: `substring('foobar' similar '#"o_b#"%' escape '#')`, want: "NULL"},
		{expr: `'a' || chr(10) || 'b' ~ 'a.b'`, want: "t"},
		{expr: `'xyz' ~ '(x)\1'`, code: "0A000"},
		{expr: `'xyz' ~ 'x(?=y)'`, code: "0A000"},
		{expr: `'xyz' ~ '('`, code: "2201B"},
		{expr: `'Hello World' ~* '\mworld\M'`, want: "t"},

		// PG-F-005: jsonb operators.
		{expr: `'{"a": "b", "c": "d"}'::jsonb - 'a'`, want: `{"c":"d"}`},
		{expr: `'["a", "b", "a"]'::jsonb - 'a'`, want: `["b"]`},
		{expr: `'{"a": "b", "c": "d"}'::jsonb - '{a,c}'::text[]`, want: `{}`},
		{expr: `'["a", "b"]'::jsonb - 1`, want: `["a"]`},
		{expr: `'["a", "b"]'::jsonb - -1`, want: `["a"]`},
		{expr: `'["a", "b"]'::jsonb - 5`, want: `["a","b"]`},
		{expr: `'{"a": 1}'::jsonb - 0`, code: "22023"},
		{expr: `'3'::jsonb - 'a'`, code: "22023"},
		{expr: `'{"a": 1}'::jsonb - 1.5`, code: "42883"},
		{expr: `'["a", {"b": 1}]'::jsonb #- '{1,b}'`, want: `["a",{}]`},
		{expr: `'{"a": {"b": [1, 2]}}'::jsonb #- '{a,b,-1}'`, want: `{"a":{"b":[1]}}`},
		{expr: `'{"a": 1}'::jsonb #- '{x,y}'`, want: `{"a":1}`},
		{expr: `'[1]'::jsonb #- '{x}'`, code: "22P02"},
		{expr: `'1'::jsonb #- '{a}'`, code: "22023"},
		{expr: `('{"a": 1}'::jsonb #- '{a}') - 'b'`, want: `{}`},
		{expr: `('{"a": {"b": [10, 20]}}'::jsonb)['a']`, want: `{"b":[10,20]}`},
		{expr: `('{"a": {"b": [10, 20]}}'::jsonb)['a']['b'][1]`, want: `20`},
		{expr: `('{"a": {"b": [10, 20]}}'::jsonb)['a']['b'][-1]`, want: `20`},
		{expr: `('{"a": {"b": [10, 20]}}'::jsonb)['a']['b']['1']`, want: `20`},
		{expr: `('{"a": {"b": [10, 20]}}'::jsonb)['a']['b']['x']`, want: `NULL`},
		{expr: `('{"a": 1}'::jsonb)['z']`, want: `NULL`},
		{expr: `('{"1": "one"}'::jsonb)[1]`, want: `"one"`},
		{expr: `('{"a": {"b": "c"}}'::jsonb)['a'] ->> 'b'`, want: `c`},
		{expr: `('{"a": 1}'::jsonb)[1.5]`, code: "42804"},
		{expr: `'{"a": 1}'::jsonb < '{"a": 2}'::jsonb`, want: "t"},
		{expr: `'[1, 2]'::jsonb > '{"a": 1}'::jsonb`, want: "f"},
		{expr: `'[]'::jsonb < 'null'::jsonb`, want: "t"},
	})

	// Jsonb subscripts in a table, ORDER BY over jsonb, and the
	// subscripted UPDATE.
	execSQL(t, ctx, s, `CREATE TABLE pgj (id INT8 PRIMARY KEY, j JSONB, n INT4)`)
	execSQL(t, ctx, s, `INSERT INTO pgj VALUES
		(1, '{"a": {"b": 1}, "tags": ["x"]}', 6),
		(2, '[1, 2, 3]', 3),
		(3, '"str"', 1),
		(4, 'true', 0),
		(5, 'null', 12),
		(6, NULL, 5),
		(7, '{"a": 1, "b": 2}', 9),
		(8, '[]', 10),
		(9, '42', 2)`)
	conn, err := pgx.Connect(ctx, "postgres://root@"+tc.Nodes[0].SQLAddr()+"/datax?sslmode=disable")
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer conn.Close(ctx)
	check := func(q, want string, args ...any) {
		t.Helper()
		got, err := queryCells(t, ctx, conn, q, append([]any{pgx.QueryExecModeSimpleProtocol}, args...)...)
		if err != nil {
			t.Errorf("%s: %v", q, err)
			return
		}
		if got != want {
			t.Errorf("%s:\n got %s\nwant %s", q, got, want)
		}
	}
	// PostgreSQL's jsonb order: [] < null < "str" < 42 < true <
	// non-empty arrays < objects (fewer pairs first); NULL last.
	check(`SELECT id FROM pgj ORDER BY j, id`, "8;5;3;9;4;2;7;1;6")
	check(`SELECT id FROM pgj ORDER BY j DESC NULLS LAST, id`, "1;7;2;4;9;3;5;8;6")
	check(`SELECT id FROM pgj WHERE j > '42' ORDER BY id`, "1;2;4;7")
	check(`SELECT id, j['a']['b'], j['tags'][0], j[1] FROM pgj WHERE id IN (1, 2) ORDER BY id`, `1|1|"x"|NULL;2|NULL|NULL|2`)
	check(`SELECT n & 3, n | 1, n # 1, n << 2, n >> 1, ~n FROM pgj WHERE id = 1`, "2|7|7|24|3|-7")
	check(`SELECT id FROM pgj WHERE n & 1 = 1 ORDER BY id`, "2;3;6;7")
	// A parameter beside a bitwise operator binds as the other side's
	// integer type (extended protocol, binary).
	var masked int64
	if err := conn.QueryRow(ctx, `SELECT n & $1 FROM pgj WHERE id = 1`, int64(4)).Scan(&masked); err != nil || masked != 4 {
		t.Errorf("n & $1: %d %v", masked, err)
	}
	check(`SELECT j - 'a' FROM pgj WHERE id = 7`, `{"b":2}`)
	check(`SELECT j #- '{a,b}' FROM pgj WHERE id = 1`, `{"a":{},"tags":["x"]}`)

	execSQL(t, ctx, s, `UPDATE pgj SET j['a']['b'] = '"new"', j['tags'][2] = '"z"' WHERE id = 1`)
	check(`SELECT j FROM pgj WHERE id = 1`, `{"a":{"b":"new"},"tags":["x",null,"z"]}`)
	execSQL(t, ctx, s, `UPDATE pgj SET j['x']['y'][1] = '7' WHERE id = 7`)
	check(`SELECT j FROM pgj WHERE id = 7`, `{"a":1,"b":2,"x":{"y":[null,7]}}`)
	execSQL(t, ctx, s, `UPDATE pgj SET j[0] = '1' WHERE id = 6`)
	check(`SELECT j FROM pgj WHERE id = 6`, `[1]`)
	execSQL(t, ctx, s, `UPDATE pgj SET j[-1] = '30' WHERE id = 2`)
	check(`SELECT j FROM pgj WHERE id = 2`, `[1,2,30]`)
	execSQL(t, ctx, s, `UPDATE pgj SET j['k'] = $1 WHERE id = 7`, types.DNull)
	check(`SELECT j['k'] FROM pgj WHERE id = 7`, `null`)
	for q, want := range map[string]string{
		`UPDATE pgj SET j['a']['b']['c'] = '1' WHERE id = 7`: "22023", // a is a scalar
		`UPDATE pgj SET j[-9] = '1' WHERE id = 2`:            "22023",
		`UPDATE pgj SET j['a'] = '1' WHERE id = 3`:           "22023",
		`UPDATE pgj SET j[NULL] = '1' WHERE id = 7`:          "22004",
		`UPDATE pgj SET j['a'] = 5 WHERE id = 7`:             "42804",
		`UPDATE pgj SET j['a'] = '{' WHERE id = 7`:           "22P02",
		`UPDATE pgj SET j['k'] = NULL WHERE id = 8`:          "22P02", // text subscript on an array
		`SELECT 5!`: "42601",
	} {
		_, err := queryCells(t, ctx, conn, q, pgx.QueryExecModeSimpleProtocol)
		if pgCode(err) != want {
			t.Errorf("%s: %v, want %s", q, err, want)
		}
	}
	check(`SELECT j FROM pgj WHERE id = 7`, `{"a":1,"b":2,"k":null,"x":{"y":[null,7]}}`)
}
