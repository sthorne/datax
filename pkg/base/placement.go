package base

import (
	"fmt"
	"sort"
	"strings"
)

// Replica placement policy (issue #176). Replication is otherwise one
// global constant and an allocator that spreads a range as widely as it
// can; a policy says that a particular database's data must live on
// nodes matching a locality, and how many copies of it there should be.
//
// The type lives here, beside Locality, so both the allocator and the
// catalog can use it without either depending on the other.

// Constraint is a required locality tier: a node satisfies it when its
// locality carries Key with exactly Value. "region=eu-west-1" is the
// shape an operator writes.
type Constraint struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}

func (c Constraint) String() string { return c.Key + "=" + c.Value }

// ParseConstraint reads "key=value".
func ParseConstraint(s string) (Constraint, error) {
	k, v, ok := strings.Cut(strings.TrimSpace(s), "=")
	k, v = strings.TrimSpace(k), strings.TrimSpace(v)
	if !ok || k == "" || v == "" {
		return Constraint{}, fmt.Errorf("invalid placement constraint %q: expected key=value, e.g. region=eu-west-1", s)
	}
	if strings.ContainsAny(k, ", ") || strings.ContainsAny(v, ",") {
		return Constraint{}, fmt.Errorf("invalid placement constraint %q: keys and values may not contain a comma", s)
	}
	return Constraint{Key: k, Value: v}, nil
}

// PlacementPolicy is where a database's replicas may live and how many
// there are. The zero value means "no policy": the cluster default
// replication factor, and any node.
type PlacementPolicy struct {
	// Replicas is the target replica count; 0 means the cluster default.
	Replicas int `json:"replicas,omitempty"`
	// Constraints is a disjunction: a replica may live on any node
	// satisfying ANY of them. Empty means any node, which is the
	// behaviour every range had before this existed.
	Constraints []Constraint `json:"constraints,omitempty"`
}

// IsZero reports whether the policy asks for nothing.
func (p PlacementPolicy) IsZero() bool { return p.Replicas == 0 && len(p.Constraints) == 0 }

// ReplicasOr is the policy's replica count, or def when it names none.
func (p PlacementPolicy) ReplicasOr(def int) int {
	if p.Replicas <= 0 {
		return def
	}
	return p.Replicas
}

// Satisfies reports whether a node with this locality may hold a replica.
// A policy with no constraints admits every node.
func (p PlacementPolicy) Satisfies(l Locality) bool {
	if len(p.Constraints) == 0 {
		return true
	}
	for _, c := range p.Constraints {
		for _, t := range l.Tiers {
			if t.Key == c.Key && t.Value == c.Value {
				return true
			}
		}
	}
	return false
}

// Validate reports why the policy cannot be stored, if it cannot.
func (p PlacementPolicy) Validate() error {
	if p.Replicas < 0 {
		return fmt.Errorf("replicas must be positive, got %d", p.Replicas)
	}
	if p.Replicas > MaxReplicationFactor {
		return fmt.Errorf("replicas must be at most %d, got %d", MaxReplicationFactor, p.Replicas)
	}
	if p.Replicas > 0 && p.Replicas%2 == 0 {
		// An even replica count buys no extra failure tolerance and
		// costs a round trip: 4 replicas tolerate the same single loss
		// as 3, because a quorum of 4 is 3.
		return fmt.Errorf("replicas must be odd so a majority is well defined, got %d", p.Replicas)
	}
	seen := map[Constraint]bool{}
	for _, c := range p.Constraints {
		if c.Key == "" || c.Value == "" {
			return fmt.Errorf("invalid placement constraint %q", c)
		}
		if seen[c] {
			return fmt.Errorf("duplicate placement constraint %q", c)
		}
		seen[c] = true
	}
	return nil
}

// String renders the policy the way an operator wrote it.
func (p PlacementPolicy) String() string {
	var parts []string
	if p.Replicas > 0 {
		parts = append(parts, fmt.Sprintf("replicas = %d", p.Replicas))
	}
	if len(p.Constraints) > 0 {
		cs := make([]string, len(p.Constraints))
		for i, c := range p.Constraints {
			cs[i] = "'" + c.String() + "'"
		}
		parts = append(parts, "constraints = ("+strings.Join(cs, ", ")+")")
	}
	if len(parts) == 0 {
		return "none"
	}
	return strings.Join(parts, ", ")
}

// ConstraintStrings renders the constraints alone, for a status document.
func (p PlacementPolicy) ConstraintStrings() []string {
	out := make([]string, len(p.Constraints))
	for i, c := range p.Constraints {
		out[i] = c.String()
	}
	return out
}

// Normalize sorts and de-duplicates the constraints so two policies that
// mean the same thing compare and render the same.
func (p PlacementPolicy) Normalize() PlacementPolicy {
	if len(p.Constraints) == 0 {
		return p
	}
	seen := map[Constraint]bool{}
	out := make([]Constraint, 0, len(p.Constraints))
	for _, c := range p.Constraints {
		if !seen[c] {
			seen[c] = true
			out = append(out, c)
		}
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Key != out[j].Key {
			return out[i].Key < out[j].Key
		}
		return out[i].Value < out[j].Value
	})
	p.Constraints = out
	return p
}

// Clone returns a deep copy: a descriptor's policy must not share its
// slice with the copy a caller holds.
func (p PlacementPolicy) Clone() PlacementPolicy {
	if len(p.Constraints) == 0 {
		return PlacementPolicy{Replicas: p.Replicas}
	}
	return PlacementPolicy{Replicas: p.Replicas, Constraints: append([]Constraint(nil), p.Constraints...)}
}

// Equal reports whether two policies ask for the same thing.
func (p PlacementPolicy) Equal(q PlacementPolicy) bool {
	a, b := p.Normalize(), q.Normalize()
	if a.Replicas != b.Replicas || len(a.Constraints) != len(b.Constraints) {
		return false
	}
	for i := range a.Constraints {
		if a.Constraints[i] != b.Constraints[i] {
			return false
		}
	}
	return true
}
