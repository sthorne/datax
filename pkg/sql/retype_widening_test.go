package sql

import (
	"testing"

	"github.com/sthorne/datax/pkg/sql/catalog"
	"github.com/sthorne/datax/pkg/sql/types"
)

// TestPureWideningArrays: an array column's modifiers are its
// elements', so narrowing them (a DECIMAL scale, a VARCHAR length) is a
// rewrite that converts every element, not a descriptor-only change.
func TestPureWideningArrays(t *testing.T) {
	arr := func(c catalog.Column) catalog.Column { c.Type = types.ArrayOf(c.Type); return c }
	dec := func(p, s int32) catalog.Column { return catalog.Column{Type: types.Decimal, Precision: p, Scale: s} }
	vc := func(n int32) catalog.Column { return catalog.Column{Type: types.String, MaxLen: n} }
	for _, c := range []struct {
		old, new catalog.Column
		want     bool
	}{
		{dec(10, 3), dec(10, 2), false},
		{arr(dec(10, 3)), arr(dec(10, 2)), false},
		{arr(dec(10, 2)), arr(dec(10, 2)), true},
		{arr(vc(10)), arr(vc(5)), false},
		{arr(vc(5)), arr(vc(10)), true},
		{arr(catalog.Column{Type: types.Int, Width: 4}), arr(catalog.Column{Type: types.Int}), true},
		{arr(catalog.Column{Type: types.Int}), arr(catalog.Column{Type: types.Int, Width: 2}), false},
	} {
		if got := pureWidening(c.old, c.new); got != c.want {
			t.Errorf("%s to %s: got %v, want %v", c.old.TypeSQL(), c.new.TypeSQL(), got, c.want)
		}
	}
}
