package sql

import (
	"testing"

	"github.com/sthorne/datax/pkg/base"
	"github.com/sthorne/datax/pkg/sql/parser"
)

// placementFromOptions folds an ALTER's option list onto the policy a
// database already carries (issue #176).
func TestPlacementFromOptions(t *testing.T) {
	cur := base.PlacementPolicy{
		Replicas:    5,
		Constraints: []base.Constraint{{Key: "region", Value: "eu"}},
	}

	// Naming only the replicas leaves the constraints alone.
	got, err := placementFromOptions(cur, &parser.PlacementOptions{Replicas: 3, SetReplicas: true})
	if err != nil {
		t.Fatal(err)
	}
	if got.Replicas != 3 || len(got.Constraints) != 1 || got.Constraints[0].Value != "eu" {
		t.Fatalf("replicas only: %+v", got)
	}

	// Naming only the constraints leaves the replica count alone.
	got, err = placementFromOptions(cur, &parser.PlacementOptions{
		Constraints: []string{"region=us", "region = ap"}, SetConstraints: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	if got.Replicas != 5 || len(got.Constraints) != 2 {
		t.Fatalf("constraints only: %+v", got)
	}
	// Normalized: sorted, and the spaces around the '=' are not part of
	// the value.
	if got.Constraints[0].Value != "ap" || got.Constraints[1].Value != "us" {
		t.Fatalf("normalize: %+v", got.Constraints)
	}

	// An empty list clears them; the count survives.
	got, err = placementFromOptions(cur, &parser.PlacementOptions{SetConstraints: true})
	if err != nil {
		t.Fatal(err)
	}
	if got.Replicas != 5 || len(got.Constraints) != 0 {
		t.Fatalf("clear: %+v", got)
	}

	// The folded result is validated, not just the new part: an even
	// count has no well-defined majority.
	if _, err := placementFromOptions(cur, &parser.PlacementOptions{Replicas: 4, SetReplicas: true}); err == nil {
		t.Fatal("an even replica count was accepted")
	}
	if _, err := placementFromOptions(cur, &parser.PlacementOptions{Replicas: 99, SetReplicas: true}); err == nil {
		t.Fatal("a replica count above the maximum was accepted")
	}
	if _, err := placementFromOptions(cur, &parser.PlacementOptions{
		Constraints: []string{"region"}, SetConstraints: true,
	}); err == nil {
		t.Fatal("a constraint without a value was accepted")
	}

	// Setting the same thing twice is a no-op the executor can skip.
	got, err = placementFromOptions(cur, &parser.PlacementOptions{
		Constraints: []string{"region=eu"}, SetConstraints: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	if !got.Equal(cur) {
		t.Fatalf("idempotent set changed the policy: %+v", got)
	}
}
