package testcluster

import (
	"context"
	"testing"
	"time"

	"github.com/sthorne/datax/pkg/sql"
	"github.com/sthorne/datax/pkg/sql/catalog"
)

// TestByteaExpressions (PG-T-003): bytea in the remaining
// expressions — || joins bytes, btrim / ltrim / rtrim / reverse take
// a bytea's bytes, get_byte, set_byte, get_bit, set_bit, bit_length,
// convert_from, convert_to and convert exist, input takes the escape
// format, and the text functions with no bytea form refuse one
// (42883) instead of working on its hex text.
func TestByteaExpressions(t *testing.T) {
	tc := Start(t, 1)
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()
	s := sql.NewSession(tc.Nodes[0].DB(), catalog.NewAccessor())
	waitForDatabases(t, ctx, s)
	execSQL(t, ctx, s, `CREATE TABLE bx (id INT PRIMARY KEY, b BYTEA)`)
	execSQL(t, ctx, s, `INSERT INTO bx VALUES (1, '\x0102'), (2, 'abc\000'), (3, '\x00ff00')`)

	runExprCases(t, ctx, s, []exprCase{
		{expr: `'\x0102'::bytea || '\x03'::bytea`, want: `\x010203`},
		{expr: `'\x01'::bytea || '\x02'`, want: `\x0102`},
		{expr: `pg_typeof('\x01'::bytea || '\x02'::bytea)`, want: "bytea"},
		{expr: `'abc\000'::bytea`, want: `\x61626300`},
		{expr: `octet_length('abc\000'::bytea)`, want: "4"},
		{expr: `'a\\b'::bytea`, want: `\x615c62`},
		{expr: `'\101\102'::bytea`, want: `\x4142`},
		{expr: `'a\q'::bytea`, code: "22P02"},
		{expr: `btrim('\x0001020300'::bytea, '\x00'::bytea)`, want: `\x010203`},
		{expr: `trim(both '\x00'::bytea from '\x000100'::bytea)`, want: `\x01`},
		{expr: `ltrim('\x000100'::bytea, '\x00'::bytea)`, want: `\x0100`},
		{expr: `rtrim('\x000100'::bytea, '\x00'::bytea)`, want: `\x0001`},
		{expr: `btrim('\xff01ff'::bytea, '\xff'::bytea)`, want: `\x01`},
		{expr: `reverse('\x010203'::bytea)`, want: `\x030201`},
		{expr: `get_byte('\x0102ff'::bytea, 2)`, want: "255"},
		{expr: `get_byte('\x01'::bytea, 1)`, code: "2202E"},
		{expr: `set_byte('\x0102'::bytea, 0, 255)`, want: `\xff02`},
		{expr: `get_bit('\x01'::bytea, 0)`, want: "1"},
		{expr: `get_bit('\x80'::bytea, 7)`, want: "1"},
		{expr: `set_bit('\x00'::bytea, 7, 1)`, want: `\x80`},
		{expr: `set_bit('\x00'::bytea, 0, 2)`, code: "22023"},
		{expr: `get_bit('\x00'::bytea, 8)`, code: "2202E"},
		{expr: `bit_length('\x0102'::bytea)`, want: "16"},
		{expr: `bit_length('abc')`, want: "24"},
		{expr: `convert_from('\x68c3a9'::bytea, 'UTF8')`, want: "hé"},
		{expr: `convert_from('\xe9'::bytea, 'LATIN1')`, want: "é"},
		{expr: `convert_from('\xff'::bytea, 'UTF8')`, code: "22021"},
		{expr: `convert_from('\x41'::bytea, 'NOPE')`, code: "22023"},
		{expr: `convert_to('hé', 'LATIN1')`, want: `\x68e9`},
		{expr: `convert_to('hé', 'utf8')`, want: `\x68c3a9`},
		{expr: `convert('\xe9'::bytea, 'LATIN1', 'UTF8')`, want: `\xc3a9`},
		{expr: `decode('a\\b\101', 'escape')`, want: `\x615c6241`},
		{expr: `'\x01'::bytea < '\x02'::bytea`, want: "t"},
		{expr: `'\x0102'::bytea > '\x01'::bytea`, want: "t"},
		{expr: `length('\x0102'::bytea)`, want: "2"},
		{expr: `lower('\x41'::bytea)`, code: "42883"},
		{expr: `left('\x4142'::bytea, 1)`, code: "42883"},
		{expr: `repeat('\x41'::bytea, 2)`, code: "42883"},
		{expr: `split_part('\x41'::bytea, 'x', 1)`, code: "42883"},
		{expr: `char_length('\x41'::bytea)`, code: "42883"},
		{expr: `btrim('\x41'::bytea)`, code: "42883"},
	})
	for q, want := range map[string]string{
		`SELECT b || '\xee'::bytea FROM bx ORDER BY id`:                      `\x0102ee;\x61626300ee;\x00ff00ee`,
		`SELECT id FROM bx WHERE b > '\x01' ORDER BY id`:                     "1;2",
		`SELECT btrim(b, '\x00'::bytea), get_byte(b, 0) FROM bx ORDER BY id`: `\x0102|1;\x616263|97;\xff|0`,
		`SELECT max(b), min(b) FROM bx`:                                      `\x61626300|\x00ff00`,
	} {
		if got, err := queryCellsSession(ctx, s, q); err != nil || got != want {
			t.Errorf("%s = %q, %v; want %q", q, got, err, want)
		}
	}
}
