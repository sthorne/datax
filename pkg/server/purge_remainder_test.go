package server

import (
	"testing"

	"github.com/sthorne/datax/pkg/keys"
	"github.com/sthorne/datax/pkg/kvpb"
)

// TestPurgeRemainder: the part of a listed range a purge must still
// reach is what a split took away between the listing and the
// collection, clipped to the purged span; a range that kept its bounds,
// or grew by a merge, leaves nothing.
func TestPurgeRemainder(t *testing.T) {
	k := func(s string) keys.Key { return keys.Key(s) }
	d := func(a, b string) kvpb.RangeDescriptor { return kvpb.RangeDescriptor{StartKey: k(a), EndKey: k(b)} }
	for _, c := range []struct {
		name              string
		listed, collected kvpb.RangeDescriptor
		end               string
		lo, hi            string
		ok                bool
	}{
		{"unchanged", d("a", "f"), d("a", "f"), "z", "", "", false},
		{"split", d("a", "f"), d("a", "c"), "z", "c", "f", true},
		{"split, clipped to the span", d("a", "f"), d("a", "c"), "e", "c", "e", true},
		{"split past the span's end", d("a", "f"), d("a", "e"), "e", "", "", false},
		{"merged", d("a", "f"), d("a", "k"), "z", "", "", false},
	} {
		lo, hi, ok := purgeRemainder(c.listed, c.collected, k(c.end))
		if ok != c.ok || (ok && (!lo.Equal(k(c.lo)) || !hi.Equal(k(c.hi)))) {
			t.Errorf("%s: got [%s, %s) %v, want [%s, %s) %v", c.name, lo, hi, ok, c.lo, c.hi, c.ok)
		}
	}
}
