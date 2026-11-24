package builtins

import (
	"math"
	"testing"

	"github.com/sthorne/datax/pkg/sql/types"
)

// TestRoundFloatHalfEven: round(double precision) is PostgreSQL's rint,
// which rounds halves to even and keeps the sign of a zero result;
// round(x, n) and numeric round halves away from zero.
func TestRoundFloatHalfEven(t *testing.T) {
	rd, _ := Lookup("round")
	for _, c := range []struct {
		in, want float64
	}{
		{2.5, 2}, {3.5, 4}, {-2.5, -2}, {-0.5, math.Copysign(0, -1)}, {0.5, 0}, {1.4999, 1}, {-1.6, -2},
	} {
		d, err := rd.Call([]types.Datum{types.NewFloat(c.in)})
		if err != nil {
			t.Fatal(err)
		}
		if d.Fam != types.Float || d.F != c.want || math.Signbit(d.F) != math.Signbit(c.want) {
			t.Errorf("round(%v::float8) = %v (%s), want %v", c.in, d.F, d.Fam, c.want)
		}
	}
	if d, err := rd.Call([]types.Datum{types.NewFloat(2.5), types.NewInt(0)}); err != nil || d.F != 3 {
		t.Errorf("round(2.5::float8, 0) = %v %v, want 3", d.F, err)
	}
	if d, err := rd.Call([]types.Datum{types.NewDecimal("2.5")}); err != nil || d.Text() != "3" {
		t.Errorf("round(2.5::numeric) = %v %v, want 3", d.Text(), err)
	}
}
