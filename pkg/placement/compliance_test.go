package placement

import (
	"testing"

	"github.com/sthorne/datax/pkg/base"
)

// Compliance helpers (issue #405).
func TestComplianceHelpers(t *testing.T) {
	loc := func(s string) base.Locality {
		l, err := base.ParseLocality(s)
		if err != nil {
			t.Fatal(err)
		}
		return l
	}
	nodes := map[base.NodeID]NodeState{
		1: {Locality: loc("region=us"), Usable: true},
		2: {Locality: loc("region=us"), Usable: true},
		3: {Locality: loc("region=us"), Usable: false}, // dead or draining
		4: {Locality: loc("region=eu"), Usable: true},
	}
	us := base.PlacementPolicy{Constraints: []base.Constraint{{Key: "region", Value: "us"}}}
	if got := Eligible(us, nodes); got != 2 {
		t.Fatalf("Eligible(us) = %d, want 2 (n3 is not usable)", got)
	}
	// Two usable US nodes for the default three replicas: unsatisfiable,
	// though nodes match.
	if !Unsatisfiable(us, 2) {
		t.Fatal("two matching nodes for three replicas is satisfiable")
	}
	if us2 := (base.PlacementPolicy{Replicas: 2, Constraints: us.Constraints}); Unsatisfiable(us2, 2) {
		t.Fatal("two matching nodes for two replicas is unsatisfiable")
	}
	// No constraints: too few nodes is under-replication, not this.
	if Unsatisfiable(base.PlacementPolicy{}, 0) {
		t.Fatal("an unconstrained policy is unsatisfiable")
	}
	if got := UnknownLocality([]base.NodeID{1, 9, 4, 7}, nodes); len(got) != 2 || got[0] != 9 || got[1] != 7 {
		t.Fatalf("UnknownLocality = %v, want [9 7]", got)
	}
	// Misplaced skips the unknown node; the compliance check counts it
	// separately.
	if got := Misplaced(us, []base.NodeID{1, 4, 9}, Localities(nodes)); len(got) != 1 || got[0] != 4 {
		t.Fatalf("Misplaced = %v, want [4]", got)
	}
}
