package sql

import (
	"math"
	"testing"

	"github.com/sthorne/datax/pkg/sql/catalog"
	"github.com/sthorne/datax/pkg/sql/types"
)

func TestEnforceTypmod(t *testing.T) {
	col := catalog.Column{Name: "amt", Type: types.Decimal, Precision: 4, Scale: 2}

	// Rescale with halves away from zero, as PostgreSQL does; canonical
	// S, declared Dscale.
	for _, c := range []struct{ in, s string }{
		{"9.9", "9.9"},
		{"1.005", "1.01"}, // half away from zero, not to even (1.00)
		{"1.015", "1.02"},
		{"1.025", "1.03"},
		{"1.0049", "1"},
		{"-1.005", "-1.01"},
		{"-1.0049", "-1"},
		{"-1.006", "-1.01"},
		{"99.994", "99.99"},
	} {
		d, err := enforceTypmod(col, types.NewDecimal(c.in))
		if err != nil {
			t.Fatalf("%q: %v", c.in, err)
		}
		if d.S != c.s || d.Dscale != 2 {
			t.Fatalf("%q: got S=%q Dscale=%d", c.in, d.S, d.Dscale)
		}
	}

	// Scale 0: 2.5 → 3, -2.5 → -3, 3.5 → 4 (half-even gave 2, -2, 4).
	int5 := catalog.Column{Name: "n", Type: types.Decimal, Precision: 5, Scale: 0}
	for _, c := range []struct{ in, s string }{{"2.5", "3"}, {"-2.5", "-3"}, {"3.5", "4"}, {"0.5", "1"}, {"-0.4", "0"}} {
		d, err := enforceTypmod(int5, types.NewDecimal(c.in))
		if err != nil || d.S != c.s {
			t.Fatalf("%q into DECIMAL(5,0): got %q, %v; want %q", c.in, d.S, err, c.s)
		}
	}

	// Overflow: integer digits exceed p−s — including only-after-rounding.
	for _, in := range []string{"100", "-100", "99.995", "-99.995", "123.4"} {
		if _, err := enforceTypmod(col, types.NewDecimal(in)); err == nil {
			t.Fatalf("%q: accepted", in)
		} else if e, ok := err.(*Error); !ok || e.Code != CodeNumericValueOutOfRange {
			t.Fatalf("%q: wrong error %v", in, err)
		}
	}

	// Boundary: 99.99 is the largest DECIMAL(4,2).
	if _, err := enforceTypmod(col, types.NewDecimal("99.99")); err != nil {
		t.Fatalf("99.99 rejected: %v", err)
	}

	// Bare DECIMAL and NULL pass through untouched.
	bare := catalog.Column{Name: "d", Type: types.Decimal}
	d, err := enforceTypmod(bare, types.NewDecimal("123.456"))
	if err != nil || d.S != "123.456" || d.Dscale != 0 {
		t.Fatalf("bare: %+v, %v", d, err)
	}
	if d, err := enforceTypmod(col, types.DNull); err != nil || !d.Null {
		t.Fatalf("null: %+v, %v", d, err)
	}
}

// TestAssignColumn: the assignment casts the write path applies —
// numeric into an integer column rounds half away from zero, float8
// half to even (rint), each range-checked against the column's width
// with PostgreSQL's message; anything else (float8 into numeric too)
// is coerceColumn's.
func TestAssignColumn(t *testing.T) {
	int2 := catalog.Column{Name: "a", Type: types.Int, Width: 2}
	int4 := catalog.Column{Name: "b", Type: types.Int, Width: 4}
	int8 := catalog.Column{Name: "c", Type: types.Int}
	num := catalog.Column{Name: "n", Type: types.Decimal}
	tenth, fifth := 0.1, 0.2 // at run time: a constant 0.1 + 0.2 folds exactly
	for _, c := range []struct {
		col  catalog.Column
		in   types.Datum
		want string
		code string
	}{
		{int8, types.NewDecimal("2.5"), "3", ""},
		{int8, types.NewDecimal("-2.5"), "-3", ""},
		{int8, types.NewDecimal("2.49"), "2", ""},
		{int4, types.NewFloat(2.5), "2", ""},
		{int4, types.NewFloat(3.5), "4", ""},
		{int2, types.NewFloat(-2.5), "-2", ""},
		{int2, types.NewDecimal("32767.4"), "32767", ""},
		{int2, types.NewDecimal("32767.5"), "smallint out of range", CodeNumericValueOutOfRange},
		{int2, types.NewDecimal("1e30"), "smallint out of range", CodeNumericValueOutOfRange},
		{int4, types.NewFloat(-2147483648.4), "-2147483648", ""},
		{int4, types.NewFloat(2147483647.5), "integer out of range", CodeNumericValueOutOfRange},
		{int4, types.NewFloat(math.NaN()), "integer out of range", CodeNumericValueOutOfRange},
		{int8, types.NewFloat(math.Inf(1)), "bigint out of range", CodeNumericValueOutOfRange},
		{int8, types.NewDecimal("9223372036854775807.5"), "bigint out of range", CodeNumericValueOutOfRange},
		{int4, types.NewDecimal("NaN"), "cannot convert NaN to integer", CodeFeatureNotSupported},
		{num, types.NewFloat(1.5), "1.5", ""},
		{num, types.NewFloat(tenth + fifth), "0.3", ""},
		{num, types.NewFloat(math.NaN()), "NaN", ""},
		{num, types.NewFloat(math.Inf(-1)), "-Infinity", ""},
		{num, types.NewInt(7), "7", ""},
		{int8, types.NewString("12"), "12", ""},
		{int8, types.DNull, "NULL", ""},
	} {
		got, err := assignColumn(c.col, c.in, nil)
		if c.code != "" {
			if err == nil || err.Code != c.code || err.Msg != c.want {
				t.Errorf("%s ← %s: %v, want %s %q", c.col.TypeSQL(), c.in.Text(), err, c.code, c.want)
			}
			continue
		}
		if err != nil {
			t.Errorf("%s ← %s: %v", c.col.TypeSQL(), c.in.Text(), err)
			continue
		}
		text := got.Text()
		if got.Null {
			text = "NULL"
		}
		if text != c.want || (!got.Null && got.Fam != c.col.Type) {
			t.Errorf("%s ← %s: %s %q, want %q", c.col.TypeSQL(), c.in.Text(), got.Fam, text, c.want)
		}
	}
}

// TestAssignColumnArrays: a numeric[] or float8[] value stored into an
// int2[] / int4[] / int8[] column converts element by element with the
// scalar assignment casts (numeric half away from zero, float8 rint,
// 22003 out of the element type's range), and a float8[] into a
// numeric[] column with float8_numeric's 15 digits; NULL elements stay
// NULL. Before, each was 22P02 "cannot use DECIMAL[] value as INT8[]".
func TestAssignColumnArrays(t *testing.T) {
	int2s := catalog.Column{Name: "a", Type: types.ArrayOf(types.Int), Width: 2}
	int4s := catalog.Column{Name: "b", Type: types.ArrayOf(types.Int), Width: 4}
	int8s := catalog.Column{Name: "c", Type: types.ArrayOf(types.Int)}
	nums := catalog.Column{Name: "n", Type: types.ArrayOf(types.Decimal)}
	dec := func(s ...string) types.Datum {
		out := make([]types.Datum, len(s))
		for i, v := range s {
			if v == "NULL" {
				out[i] = types.DNull
				continue
			}
			out[i] = types.NewDecimal(v)
		}
		return types.NewArray(types.Decimal, out)
	}
	flt := func(f ...float64) types.Datum {
		out := make([]types.Datum, len(f))
		for i, v := range f {
			out[i] = types.NewFloat(v)
		}
		return types.NewArray(types.Float, out)
	}
	tenth, fifth := 0.1, 0.2
	for _, c := range []struct {
		col  catalog.Column
		in   types.Datum
		want string
		code string
	}{
		{int4s, dec("1.5", "2.4", "-2.5"), "{2,2,-3}", ""},
		{int8s, dec("1.5", "NULL"), "{2,NULL}", ""},
		{int4s, flt(1.5, 2.5, -0.5), "{2,2,0}", ""},
		{int2s, dec("32767.5"), "smallint out of range", CodeNumericValueOutOfRange},
		{int4s, flt(3e9), "integer out of range", CodeNumericValueOutOfRange},
		{int8s, flt(math.NaN()), "bigint out of range", CodeNumericValueOutOfRange},
		{int4s, dec("NaN"), "cannot convert NaN to integer", CodeFeatureNotSupported},
		{nums, flt(tenth+fifth, 2.5), "{0.3,2.5}", ""},
		{int8s, types.NewArray(types.Int, []types.Datum{types.NewInt(1)}), "{1}", ""},
	} {
		got, err := assignColumn(c.col, c.in, nil)
		if c.code != "" {
			if err == nil || err.Code != c.code || err.Msg != c.want {
				t.Errorf("%s ← %s: %v, want %s %q", c.col.TypeSQL(), c.in.Text(), err, c.code, c.want)
			}
			continue
		}
		if err != nil {
			t.Errorf("%s ← %s: %v", c.col.TypeSQL(), c.in.Text(), err)
			continue
		}
		if got.Text() != c.want || got.Fam != c.col.Type {
			t.Errorf("%s ← %s: %s %q, want %q", c.col.TypeSQL(), c.in.Text(), got.Fam, got.Text(), c.want)
		}
	}
}
