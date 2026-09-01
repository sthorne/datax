package sql

import (
	"fmt"
	"math"
	"testing"

	"github.com/sthorne/datax/pkg/sql/types"
)

func TestKMVSketch(t *testing.T) {
	// Exact below capacity.
	s := newKMV()
	for i := 0; i < 100; i++ {
		s.add(statsHashDatum(types.NewInt(int64(i % 40))))
	}
	if got := s.distinct(); got != 40 {
		t.Fatalf("exact distinct = %d, want 40", got)
	}
	// Estimated beyond capacity: within ±15% at 100k distinct values.
	s = newKMV()
	for i := 0; i < 100000; i++ {
		s.add(statsHashDatum(types.NewString(fmt.Sprintf("value-%d", i))))
	}
	got := float64(s.distinct())
	if math.Abs(got-100000)/100000 > 0.15 {
		t.Fatalf("estimate %v too far from 100000", got)
	}
	// Duplicates don't inflate the estimate.
	s = newKMV()
	for round := 0; round < 5; round++ {
		for i := 0; i < 1000; i++ {
			s.add(statsHashDatum(types.NewInt(int64(i))))
		}
	}
	got = float64(s.distinct())
	if math.Abs(got-1000)/1000 > 0.15 {
		t.Fatalf("duplicate-heavy estimate %v too far from 1000", got)
	}
}

func TestStatsHashDatumStability(t *testing.T) {
	// Decimal display scale must not affect identity: a value read from a
	// DECIMAL(p,s) column carries Dscale, the same value written fresh
	// does not.
	a := types.NewDecimal("1.5")
	b := types.NewDecimal("1.5")
	b.Dscale = 3
	if statsHashDatum(a) != statsHashDatum(b) {
		t.Fatal("Dscale changed the hash")
	}
	// Different families never collide via shared representations.
	if statsHashDatum(types.NewInt(0)) == statsHashDatum(types.NewString("")) {
		t.Fatal("cross-family collision")
	}
	// Distinct values hash distinctly (sanity).
	if statsHashDatum(types.NewInt(1)) == statsHashDatum(types.NewInt(2)) {
		t.Fatal("1 and 2 collide")
	}
}
