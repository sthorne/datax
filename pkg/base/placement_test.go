package base

import "testing"

func loc(t *testing.T, s string) Locality {
	t.Helper()
	l, err := ParseLocality(s)
	if err != nil {
		t.Fatal(err)
	}
	return l
}

// TestPlacementSatisfies: a constraint is a locality tier a node must
// carry, and the set is a disjunction — a database pinned to two regions
// admits nodes in either.
func TestPlacementSatisfies(t *testing.T) {
	eu := PlacementPolicy{Constraints: []Constraint{{Key: "region", Value: "eu-west-1"}}}
	twoRegions := PlacementPolicy{Constraints: []Constraint{
		{Key: "region", Value: "eu-west-1"},
		{Key: "region", Value: "eu-central-1"},
	}}
	for _, tc := range []struct {
		name     string
		policy   PlacementPolicy
		locality string
		want     bool
	}{
		{"no policy admits anything", PlacementPolicy{}, "region=us-east-1,rack=a", true},
		{"no policy admits a node with no locality", PlacementPolicy{}, "", true},
		{"matching region", eu, "region=eu-west-1,rack=a", true},
		{"matching region, tier order irrelevant", eu, "rack=a,region=eu-west-1", true},
		{"another region", eu, "region=us-east-1,rack=a", false},
		{"no locality at all", eu, "", false},
		{"right key, wrong value", eu, "region=eu-west-2", false},
		{"value under another key does not count", eu, "zone=eu-west-1", false},
		{"either of two regions", twoRegions, "region=eu-central-1,rack=b", true},
		{"neither of two regions", twoRegions, "region=ap-south-1", false},
		{"replicas alone constrains nothing", PlacementPolicy{Replicas: 5}, "region=us-east-1", true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := tc.policy.Satisfies(loc(t, tc.locality)); got != tc.want {
				t.Fatalf("Satisfies(%q) = %v, want %v", tc.locality, got, tc.want)
			}
		})
	}
}

func TestPlacementReplicasOr(t *testing.T) {
	if got := (PlacementPolicy{}).ReplicasOr(3); got != 3 {
		t.Fatalf("no policy: %d, want the default 3", got)
	}
	if got := (PlacementPolicy{Replicas: 5}).ReplicasOr(3); got != 5 {
		t.Fatalf("policy: %d, want 5", got)
	}
}

// TestPlacementValidate: what a policy may not say.
func TestPlacementValidate(t *testing.T) {
	for _, tc := range []struct {
		name   string
		policy PlacementPolicy
		ok     bool
	}{
		{"empty", PlacementPolicy{}, true},
		{"three replicas", PlacementPolicy{Replicas: 3}, true},
		{"one replica", PlacementPolicy{Replicas: 1}, true},
		{"the ceiling", PlacementPolicy{Replicas: MaxReplicationFactor}, true},
		{"past the ceiling", PlacementPolicy{Replicas: MaxReplicationFactor + 2}, false},
		{"negative", PlacementPolicy{Replicas: -1}, false},
		{"even", PlacementPolicy{Replicas: 4}, false},
		{"good constraint", PlacementPolicy{Constraints: []Constraint{{Key: "region", Value: "eu"}}}, true},
		{"empty key", PlacementPolicy{Constraints: []Constraint{{Value: "eu"}}}, false},
		{"empty value", PlacementPolicy{Constraints: []Constraint{{Key: "region"}}}, false},
		{"duplicate", PlacementPolicy{Constraints: []Constraint{{Key: "region", Value: "eu"}, {Key: "region", Value: "eu"}}}, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if err := tc.policy.Validate(); (err == nil) != tc.ok {
				t.Fatalf("Validate() = %v, want ok=%v", err, tc.ok)
			}
		})
	}
}

func TestParseConstraint(t *testing.T) {
	for _, tc := range []struct {
		in   string
		want Constraint
		ok   bool
	}{
		{"region=eu-west-1", Constraint{"region", "eu-west-1"}, true},
		{"  region = eu-west-1  ", Constraint{"region", "eu-west-1"}, true},
		{"rack=b", Constraint{"rack", "b"}, true},
		{"region", Constraint{}, false},
		{"=eu", Constraint{}, false},
		{"region=", Constraint{}, false},
		{"", Constraint{}, false},
		{"region=a,b", Constraint{}, false},
	} {
		got, err := ParseConstraint(tc.in)
		if (err == nil) != tc.ok {
			t.Fatalf("ParseConstraint(%q) = %v, %v", tc.in, got, err)
		}
		if tc.ok && got != tc.want {
			t.Fatalf("ParseConstraint(%q) = %v, want %v", tc.in, got, tc.want)
		}
	}
}

// TestPlacementNormalizeAndEqual: two policies that mean the same thing
// compare equal however they were written, and a clone shares nothing.
func TestPlacementNormalizeAndEqual(t *testing.T) {
	a := PlacementPolicy{Replicas: 3, Constraints: []Constraint{
		{Key: "region", Value: "eu-west-1"},
		{Key: "region", Value: "eu-central-1"},
		{Key: "region", Value: "eu-west-1"},
	}}
	b := PlacementPolicy{Replicas: 3, Constraints: []Constraint{
		{Key: "region", Value: "eu-central-1"},
		{Key: "region", Value: "eu-west-1"},
	}}
	if !a.Equal(b) {
		t.Fatalf("%v and %v should be equal", a, b)
	}
	if a.Equal(PlacementPolicy{Replicas: 5, Constraints: b.Constraints}) {
		t.Fatal("policies with different replica counts compared equal")
	}
	if n := a.Normalize(); len(n.Constraints) != 2 || n.Constraints[0].Value != "eu-central-1" {
		t.Fatalf("Normalize gave %v", n.Constraints)
	}
	c := a.Clone()
	c.Constraints[0] = Constraint{Key: "region", Value: "changed"}
	if a.Constraints[0].Value == "changed" {
		t.Fatal("Clone shares its constraint slice")
	}
	if got := b.String(); got != "replicas = 3, constraints = ('region=eu-central-1', 'region=eu-west-1')" {
		t.Fatalf("String() = %q", got)
	}
	if got := (PlacementPolicy{}).String(); got != "none" {
		t.Fatalf("empty String() = %q", got)
	}
}
