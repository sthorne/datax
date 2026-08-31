package sql

import (
	"testing"

	"github.com/sthorne/datax/pkg/sql/catalog"
	"github.com/sthorne/datax/pkg/sql/types"
)

func TestEnforceTypmod(t *testing.T) {
	col := catalog.Column{Name: "amt", Type: types.Decimal, Precision: 4, Scale: 2}

	// Rescale round-half-even; canonical S, declared Dscale.
	for _, c := range []struct{ in, s string }{
		{"9.9", "9.9"},
		{"1.005", "1"}, // ties to even
		{"1.015", "1.02"},
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

	// Overflow: integer digits exceed p−s — including only-after-rounding.
	for _, in := range []string{"100", "-100", "99.995", "123.4"} {
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
