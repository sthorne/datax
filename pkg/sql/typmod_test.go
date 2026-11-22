package sql

import (
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
