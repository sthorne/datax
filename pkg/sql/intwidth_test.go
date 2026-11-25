package sql

import (
	"strings"
	"testing"

	"github.com/sthorne/datax/pkg/sql/catalog"
	"github.com/sthorne/datax/pkg/sql/parser"
	"github.com/sthorne/datax/pkg/sql/types"
)

// intWidthTable is a table with an int2 (a), an int4 (b) and an int8 (c)
// column, and one row of it.
func intWidthTable() (*catalog.TableDescriptor, map[catalog.ColumnID]types.Datum) {
	desc := &catalog.TableDescriptor{Name: "t", Columns: []catalog.Column{
		{ID: 1, Name: "a", Type: types.Int, Width: 2},
		{ID: 2, Name: "b", Type: types.Int, Width: 4},
		{ID: 3, Name: "c", Type: types.Int},
		{ID: 4, Name: "s", Type: types.String},
	}}
	row := map[catalog.ColumnID]types.Datum{
		1: types.NewInt(32767), 2: types.NewInt(2147483647), 3: types.NewInt(9223372036854775807), 4: types.NewString("x"),
	}
	return desc, row
}

func selectExpr(t *testing.T, e string) parser.Expr {
	t.Helper()
	stmts, err := parser.Parse("SELECT " + e + " FROM t")
	if err != nil {
		t.Fatalf("%s: %v", e, err)
	}
	return stmts[0].(*parser.Select).Exprs[0].Expr
}

// TestIntegerOperatorWidths: integer arithmetic is of its wider
// operand's type, as in PostgreSQL (int2 + int2 is int2, int2 + int4
// int4, anything with an int8 int8; an integer literal is an int4), and
// a result out of that type's range is 22003 with PostgreSQL's message.
// A cast checks its target's range; a shift keeps its left operand's
// type and wraps at it; CASE and COALESCE are of their arms' common type
// and any other function's integer result is a bigint. pg_typeof names
// the width. Every value's width is the one its column is described
// with (exprWidth), so the wire never carries a value wider than its
// column.
func TestIntegerOperatorWidths(t *testing.T) {
	desc, row := intWidthTable()
	for _, tc := range []struct {
		expr  string
		want  string // the value, or the error message
		width int32  // the value's width (and the described one)
	}{
		{"1", "1", 4},
		{"2147483648", "2147483648", 8},
		{"-2147483648", "-2147483648", 4},
		{"a + a", "smallint out of range", 0},
		{"a - 1::int2", "32766", 2},
		{"a * 1::int2", "32767", 2},
		{"a + 1", "32768", 4},
		{"a / -1::int2", "-32767", 2},
		{"(-32768)::int2 / -1::int2", "smallint out of range", 0},
		{"b + 1", "integer out of range", 0},
		{"b - 1", "2147483646", 4},
		{"b * 2", "integer out of range", 0},
		{"b + 1::int8", "2147483648", 8},
		{"b::int8 + 1", "2147483648", 8},
		{"2147483647 + 1", "integer out of range", 0},
		{"2147483647::int4 + 1", "integer out of range", 0},
		{"50000 * 50000", "integer out of range", 0},
		{"(-2147483648) / -1", "integer out of range", 0},
		{"c + 1", "bigint out of range", 0},
		{"(-9223372036854775807 - 1) / -1", "bigint out of range", 0},
		{"c / -1", "-9223372036854775807", 8},
		{"b % 7", "1", 4},
		{"a % 7", "0", 4},
		{"a % 7::int2", "0", 2},
		{"a & 255", "255", 4},
		{"a & 255::int2", "255", 2},
		{"1 << 31", "-2147483648", 4},
		{"1::int2 << 15", "-32768", 2},
		{"1 << 32", "1", 4},
		{"1::int8 << 31", "2147483648", 8},
		{"b >> 30", "1", 4},
		{"70000::int2", "smallint out of range", 0},
		{"3000000000::int4", "integer out of range", 0},
		{"a::int8", "32767", 8},
		{"b::int2", "smallint out of range", 0},
		{"coalesce(a, 1)", "32767", 4},
		{"coalesce(a, a)", "32767", 2},
		{"coalesce(a, c)", "32767", 8},
		{"CASE WHEN true THEN a ELSE b END", "32767", 4},
		{"CASE WHEN true THEN a ELSE NULL END", "32767", 2},
		{"CASE WHEN true THEN a ELSE b END + 2147483647", "integer out of range", 0},
		{"greatest(a, 1) * 2", "65534", 4},
		{"length(s)", "1", 8},
		{"length(s) + 1", "2", 8},
		{"abs(a) + 2147483647", "2147516414", 8},
		{"(a)", "32767", 2},
	} {
		e := selectExpr(t, tc.expr)
		got, err := evalExpr(e, desc, row, nil)
		if tc.width == 0 {
			serr := ToSQLError(err)
			if serr == nil || serr.Code != CodeNumericValueOutOfRange || serr.Msg != tc.want {
				t.Errorf("%s: %v %v, want 22003 %q", tc.expr, got, err, tc.want)
			}
			continue
		}
		if err != nil || got.Text() != tc.want || got.IntWidth() != tc.width {
			t.Errorf("%s = %s (width %d), %v; want %s (width %d)", tc.expr, got.Text(), got.IntWidth(), err, tc.want, tc.width)
			continue
		}
		described := exprWidth(types.Int, e, descColWidth(desc))
		if described == 0 {
			described = 8
		}
		if described != got.IntWidth() {
			t.Errorf("%s: described width %d, value's %d", tc.expr, described, got.IntWidth())
		}
		name, err := evalExpr(parser.Expr{Func: "pg_typeof", Args: []parser.Expr{e}}, desc, row, nil)
		if want := types.IntWidthName(tc.width); err != nil || name.S != want {
			t.Errorf("pg_typeof(%s) = %q, %v; want %q", tc.expr, name.S, err, want)
		}
	}
}

// TestIntegerParamIsBigint: a parameter is a bigint in an expression
// (as exprWidth describes it), whatever produced its value: an EXECUTE
// argument's int4 literal does not make $1 * $1 overflow at int4.
func TestIntegerParamIsBigint(t *testing.T) {
	params := []types.Datum{types.NewInt(100000).WithIntWidth(4)}
	e := parser.Expr{Param: 1, BinOp: "*", Right: &parser.Expr{Param: 1}}
	got, err := evalExpr(e, nil, nil, params)
	if err != nil || got.I != 10000000000 || got.IntWidth() != 8 {
		t.Fatalf("$1 * $1 = %v (width %d), %v; want 10000000000 (width 8)", got.I, got.IntWidth(), err)
	}
}

// TestToRegtypeIntegerWidths: to_regtype names int2 and int4 by their
// own OIDs (21, 23; their arrays 1005, 1007), as PostgreSQL does, not
// int8's.
func TestToRegtypeIntegerWidths(t *testing.T) {
	for name, want := range map[string]int64{
		"int2": 21, "smallint": 21, "int4": 23, "int": 23, "integer": 23, "pg_catalog.int4": 23,
		"int8": 20, "bigint": 20, "int4[]": 1007, "int2[]": 1005, "int8[]": 1016,
	} {
		arg := types.NewString(name)
		got, err := evalExpr(parser.Expr{Func: "to_regtype", Args: []parser.Expr{{Lit: &arg}}}, nil, nil, nil)
		if err != nil || got.I != want {
			t.Errorf("to_regtype(%q) = %v, %v; want %d", name, got.I, err, want)
		}
	}
}

// TestParamIntegerWidths: a parameter compared with, stored into or
// cast to an int2 or int4 column is of that width (described as int2 /
// int4), one beside an int4 expression in arithmetic is suggested int4,
// and the first column that fixes it decides.
func TestParamIntegerWidths(t *testing.T) {
	desc, _ := intWidthTable()
	lookup := func(name string) (*catalog.TableDescriptor, error) {
		if name == "t" {
			return desc, nil
		}
		return nil, &catalog.ErrTableNotFound{Name: name}
	}
	for q, want := range map[string]string{
		`SELECT c FROM t WHERE a = $1 AND b = $2 AND c = $3`:       "2,4,8",
		`SELECT c FROM t WHERE $1 = b`:                             "4",
		`SELECT c FROM t WHERE b + 1 = $1`:                         "4",
		`INSERT INTO t (a, b, c) VALUES ($1, $2, $3)`:              "2,4,8",
		`UPDATE t SET b = $1 WHERE a = $2`:                         "4,2",
		`SELECT $1::int4, $2::smallint, $3::int8`:                  "4,2,8",
		`SELECT b + $1 FROM t`:                                     "4",
		`SELECT c FROM t WHERE b = $1 AND c = $1`:                  "4",
		`SELECT c FROM t WHERE b IN ($1, $2)`:                      "4,4",
		`SELECT c FROM t WHERE c = $1 LIMIT $2`:                    "8,8",
		`SELECT c FROM t WHERE b + $1 > 0 AND c = $1`:              "8",
		`INSERT INTO t (a) VALUES ($1) ON CONFLICT (a) DO NOTHING`: "2",
	} {
		stmts, err := parser.Parse(q)
		if err != nil {
			t.Fatalf("%s: %v", q, err)
		}
		pt := newParamTyper(parser.CountParams(stmts[0]), lookup)
		if err := pt.statement(stmts[0], nil, true); err != nil {
			t.Errorf("%s: %v", q, err)
			continue
		}
		var got []string
		for i, f := range pt.fams {
			w := pt.widths[i]
			if f != types.Int {
				got = append(got, f.String())
				continue
			}
			if w == 0 {
				w = 8
			}
			got = append(got, string(rune('0'+w)))
		}
		if strings.Join(got, ",") != want {
			t.Errorf("%s: widths %v, want %s", q, got, want)
		}
	}
}
