package parser

import (
	"math"
	"testing"

	"github.com/sthorne/datax/pkg/sql/types"
)

// literal parses SELECT <expr> and returns the expression's innermost
// literal and the expression itself.
func literal(t *testing.T, q string) (*types.Datum, Expr) {
	t.Helper()
	stmts, err := Parse(q)
	if err != nil {
		t.Fatalf("%s: %v", q, err)
	}
	e := stmts[0].(*Select).Exprs[0].Expr
	inner := e
	for inner.Left != nil {
		inner = *inner.Left
	}
	if inner.Lit == nil {
		t.Fatalf("%s: no literal in %#v", q, e)
	}
	return inner.Lit, e
}

// TestIntegerLiteralBeyondInt8: an integer literal outside int8 is
// numeric, as in PostgreSQL, not a syntax error; the int8 extremes
// (the most negative one included) stay int8.
func TestIntegerLiteralBeyondInt8(t *testing.T) {
	for q, want := range map[string]string{
		"SELECT 99999999999999999999":  "99999999999999999999",
		"SELECT -99999999999999999999": "-99999999999999999999",
		"SELECT 9223372036854775808":   "9223372036854775808",
	} {
		d, _ := literal(t, q)
		if d.Fam != types.Decimal || d.Text() != want {
			t.Errorf("%s: got %s %q, want DECIMAL %q", q, d.Fam, d.Text(), want)
		}
	}
	for q, want := range map[string]int64{
		"SELECT 9223372036854775807":  math.MaxInt64,
		"SELECT -9223372036854775808": math.MinInt64,
		"SELECT -9223372036854775807": -math.MaxInt64,
		"SELECT 2147483648":           2147483648,
	} {
		d, _ := literal(t, q)
		if d.Fam != types.Int || d.I != want {
			t.Errorf("%s: got %s %v, want INT8 %d", q, d.Fam, d.Text(), want)
		}
	}
}

// TestNegativeZeroFloatLiteral: -0.0::float8 is -(0.0::float8), which
// is -0 in PostgreSQL; the minus folded into the literal must not lose
// the sign. Without a float cast a negated zero stays an exact zero.
func TestNegativeZeroFloatLiteral(t *testing.T) {
	for _, q := range []string{"SELECT -0.0::float8", "SELECT -0::float8", "SELECT -0.0::float4", "SELECT -0.0::double precision", "SELECT -0.0::float8::text"} {
		d, _ := literal(t, q)
		if d.Fam != types.Float || d.F != 0 || !math.Signbit(d.F) {
			t.Errorf("%s: got %s %v, want FLOAT8 -0", q, d.Fam, d.Text())
		}
	}
	for _, q := range []string{"SELECT -0.0", "SELECT -0", "SELECT -0.0::numeric", "SELECT -0::int4", "SELECT -1.5::float8"} {
		d, _ := literal(t, q)
		if d.Fam == types.Float {
			t.Errorf("%s: got FLOAT8 %v, want the literal's own type", q, d.Text())
		}
	}
}

// TestIntegerLiteralWidth: an integer literal is an integer (int4,
// width 4) when it fits, else a bigint (width 0), as PostgreSQL types
// it; a negative literal is folded first, so -2147483648 is an integer
// and -2147483649 a bigint.
func TestIntegerLiteralWidth(t *testing.T) {
	for q, want := range map[string]int32{
		"SELECT 1":           4,
		"SELECT 0":           4,
		"SELECT 2147483647":  4,
		"SELECT -2147483648": 4,
		"SELECT 2147483648":  8,
		"SELECT -2147483649": 8,
		"SELECT 1::int8":     4, // the literal; the cast types the expression
	} {
		d, _ := literal(t, q)
		if d.Fam != types.Int || d.IntWidth() != want {
			t.Errorf("%s: %s width %d, want INT8 width %d", q, d.Fam, d.IntWidth(), want)
		}
	}
}
