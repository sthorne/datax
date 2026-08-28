package base

import "testing"

func TestParseLocality(t *testing.T) {
	l, err := ParseLocality("region=us-east,zone=b,rack=12")
	if err != nil {
		t.Fatal(err)
	}
	if got := l.String(); got != "region=us-east,zone=b,rack=12" {
		t.Fatalf("round trip: got %q", got)
	}
	if _, err := ParseLocality("region"); err == nil {
		t.Fatal("expected error for missing value")
	}
	if _, err := ParseLocality("a=1,a=2"); err == nil {
		t.Fatal("expected error for duplicate tier")
	}
	if l, err := ParseLocality(""); err != nil || len(l.Tiers) != 0 {
		t.Fatalf("empty locality: %v %v", l, err)
	}
}

func TestDiversity(t *testing.T) {
	a, _ := ParseLocality("region=r1,rack=a")
	b, _ := ParseLocality("region=r1,rack=b")
	c, _ := ParseLocality("region=r2,rack=a")

	if d := a.Diversity(a); d != 0 {
		t.Fatalf("self diversity = %v, want 0", d)
	}
	// Same region, different rack: shares 1 of 2 tiers.
	if d := a.Diversity(b); d != 0.5 {
		t.Fatalf("a-b diversity = %v, want 0.5", d)
	}
	// Different region: shares nothing.
	if d := a.Diversity(c); d != 1 {
		t.Fatalf("a-c diversity = %v, want 1", d)
	}
	if a.Diversity(c) <= a.Diversity(b) {
		t.Fatal("cross-region should be more diverse than cross-rack")
	}
}
