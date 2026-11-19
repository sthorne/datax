package types

import (
	"math"
	"testing"
)

// TestNumericSpecialValues (PG-T-013): numeric's NaN and ±Infinity
// read in every spelling PostgreSQL takes, render by name, order
// -Infinity < finite < Infinity < NaN (NaN equal to itself, also
// against a float8), and convert to and from float8.
func TestNumericSpecialValues(t *testing.T) {
	for in, want := range map[string]string{
		"NaN": "NaN", "nan": "NaN", " NAN ": "NaN", "Infinity": "Infinity", "+infinity": "Infinity", "inf": "Infinity",
		"-Infinity": "-Infinity", "-INF": "-Infinity",
	} {
		d, err := ParseDecimal(in)
		if err != nil || d.Text() != want {
			t.Errorf("ParseDecimal(%q) = %q, %v; want %q", in, d.Text(), err, want)
		}
	}
	if _, err := ParseDecimal("nanx"); err == nil {
		t.Error("nanx accepted")
	}
	nan, pinf, ninf := NewDecimal(DecimalNaN), NewDecimal(DecimalPosInf), NewDecimal(DecimalNegInf)
	big, _ := ParseDecimal("1e100")
	order := []Datum{ninf, NewInt(-5), NewDecimal("0.5"), big, pinf, nan}
	for i := range order {
		for j := range order {
			c, err := order[i].Compare(order[j])
			want := 0
			switch {
			case i < j:
				want = -1
			case i > j:
				want = 1
			}
			if err != nil || c != want {
				t.Errorf("Compare(%s, %s) = %d, %v; want %d", order[i].Text(), order[j].Text(), c, err, want)
			}
		}
	}
	if c, err := nan.Compare(NewFloat(math.NaN())); err != nil || c != 0 {
		t.Errorf("numeric NaN vs float8 NaN: %d, %v", c, err)
	}
	if c, err := pinf.Compare(NewFloat(math.Inf(1))); err != nil || c != 0 {
		t.Errorf("numeric Infinity vs float8 Infinity: %d, %v", c, err)
	}
	if f, err := ninf.Coerce(Float); err != nil || !math.IsInf(f.F, -1) {
		t.Errorf("-Infinity to float8: %v, %v", f.F, err)
	}
	padded := nan
	padded.Dscale = 2
	if padded.Text() != "NaN" {
		t.Errorf("NaN in a NUMERIC(p,2) column renders %q", padded.Text())
	}
}

// TestNumericSpecialArith (PG-T-013): PostgreSQL's arithmetic rules
// for NaN and the infinities.
func TestNumericSpecialArith(t *testing.T) {
	nan, pinf, ninf := NewDecimal(DecimalNaN), NewDecimal(DecimalPosInf), NewDecimal(DecimalNegInf)
	one, zero := NewDecimal("1"), NewDecimal("0")
	cases := []struct {
		l    Datum
		op   string
		r    Datum
		want string // "" with err
		err  bool
	}{
		{pinf, "+", one, "Infinity", false},
		{pinf, "+", pinf, "Infinity", false},
		{pinf, "-", pinf, "NaN", false},
		{pinf, "+", ninf, "NaN", false},
		{ninf, "-", one, "-Infinity", false},
		{pinf, "*", NewDecimal("-2"), "-Infinity", false},
		{pinf, "*", zero, "NaN", false},
		{NewInt(0), "*", ninf, "NaN", false},
		{pinf, "/", pinf, "NaN", false},
		{one, "/", pinf, "0", false},
		{pinf, "/", NewInt(-3), "-Infinity", false},
		{pinf, "/", zero, "", true},
		{nan, "/", zero, "NaN", false},
		{nan, "+", one, "NaN", false},
		{one, "-", nan, "NaN", false},
		{NewDecimal("5"), "%", pinf, "5", false},
		{pinf, "%", NewDecimal("5"), "NaN", false},
		{pinf, "%", zero, "", true},
	}
	for _, c := range cases {
		d, ok, err := DecimalSpecialArith(c.l, c.op, c.r)
		if !ok {
			t.Errorf("%s %s %s: not handled", c.l.Text(), c.op, c.r.Text())
			continue
		}
		if c.err != (err != nil) || !c.err && d.Text() != c.want {
			t.Errorf("%s %s %s = %q, %v; want %q (error %v)", c.l.Text(), c.op, c.r.Text(), d.Text(), err, c.want, c.err)
		}
	}
	if _, ok, _ := DecimalSpecialArith(one, "+", one); ok {
		t.Error("finite operands taken as special")
	}
}
