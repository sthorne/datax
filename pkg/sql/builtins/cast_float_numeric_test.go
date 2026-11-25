package builtins

import (
	"math"
	"testing"

	"github.com/sthorne/datax/pkg/sql/types"
)

// TestCastFloat8ToNumeric: float8::numeric is PostgreSQL's
// float8_numeric, which renders the value with 15 significant digits
// (%.15g, DBL_DIG) and parses that, so float noise below the 15th digit
// is gone: (0.1 + 0.2)::numeric is 0.3, not 0.30000000000000004. NaN
// and the infinities become numeric's own (PostgreSQL 14 and later).
// The expected values are PostgreSQL 16's.
func TestCastFloat8ToNumeric(t *testing.T) {
	tenth, fifth := 0.1, 0.2 // at run time: a constant 0.1 + 0.2 folds exactly
	for _, c := range []struct {
		in   float64
		typ  string
		want string
	}{
		{tenth + fifth, "numeric", "0.3"},
		{1.0 / 3, "numeric", "0.333333333333333"},
		{2.5, "numeric", "2.5"},
		{-1.25, "numeric", "-1.25"},
		{1e20, "numeric", "100000000000000000000"},
		{123456789012345678, "numeric", "123456789012346000"},
		{1e-5, "numeric", "0.00001"},
		{math.Copysign(0, -1), "numeric", "0"},
		{math.NaN(), "numeric", "NaN"},
		{math.Inf(1), "numeric", "Infinity"},
		{math.Inf(-1), "numeric", "-Infinity"},
		{tenth + fifth, "numeric(5,3)", "0.300"},
		{1.005, "numeric(5,2)", "1.01"},
	} {
		got, err := Cast(types.NewFloat(c.in), c.typ)
		if err != nil {
			t.Errorf("%v::%s: %v", c.in, c.typ, err)
			continue
		}
		if got.Fam != types.Decimal || got.Text() != c.want {
			t.Errorf("%v::%s = %s %q, want %q", c.in, c.typ, got.Fam, got.Text(), c.want)
		}
	}
}
