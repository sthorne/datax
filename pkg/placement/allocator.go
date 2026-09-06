// Package placement decides where replicas live: the rack-aware allocator
// maximizes failure-domain diversity so losing one rack (or zone, or
// region) never costs a range more than one replica. See
// docs/replication-and-placement.md.
package placement

import (
	"sort"

	"github.com/sthorne/datax/pkg/base"
	"github.com/sthorne/datax/pkg/kvpb"
)

// Candidate is a node considered for a new replica.
type Candidate struct {
	Node kvpb.NodeDescriptor
	// RangeCount is how many ranges the node already hosts (tie-break:
	// prefer emptier nodes).
	RangeCount int
}

// AllocateTarget picks the best node for a new replica of a range whose
// current replicas live on `existing`. Candidates already holding a replica
// must be filtered out by the caller (or are skipped here defensively).
// Returns false when no candidate is eligible.
func AllocateTarget(existing []kvpb.NodeDescriptor, candidates []Candidate) (base.NodeID, bool) {
	return AllocateTargetFor(base.PlacementPolicy{}, existing, candidates)
}

// AllocateTargetFor is AllocateTarget under a placement policy (issue
// #176): candidates the policy does not admit are dropped, and the
// diversity score is then maximized WITHIN what is left — so a database
// pinned to one region still spreads its replicas across that region's
// racks rather than piling them onto one.
//
// A policy no candidate satisfies returns false, exactly as too few
// nodes does. The caller reports that as an unmet policy rather than
// quietly widening it: placing a replica outside a region the operator
// named is the one thing this must never do.
func AllocateTargetFor(policy base.PlacementPolicy, existing []kvpb.NodeDescriptor, candidates []Candidate) (base.NodeID, bool) {
	held := make(map[base.NodeID]bool, len(existing))
	for _, nd := range existing {
		held[nd.NodeID] = true
	}
	type scored struct {
		id     base.NodeID
		score  float64
		ranges int
	}
	var best *scored
	for _, c := range candidates {
		if held[c.Node.NodeID] {
			continue
		}
		if !policy.Satisfies(c.Node.Locality) {
			continue
		}
		s := scored{id: c.Node.NodeID, score: diversityScore(c.Node, existing), ranges: c.RangeCount}
		if best == nil ||
			s.score > best.score ||
			(s.score == best.score && s.ranges < best.ranges) ||
			(s.score == best.score && s.ranges == best.ranges && s.id < best.id) {
			b := s
			best = &b
		}
	}
	if best == nil {
		return 0, false
	}
	return best.id, true
}

// diversityScore sums the candidate's locality distance to every existing
// replica: higher means the candidate sits in broader failure domains
// relative to the current placement.
func diversityScore(cand kvpb.NodeDescriptor, existing []kvpb.NodeDescriptor) float64 {
	var sum float64
	for _, nd := range existing {
		sum += cand.Locality.Diversity(nd.Locality)
	}
	return sum
}

// RemoveTarget picks the replica whose removal keeps the remaining set most
// diverse (used when a rebalance's source is not specified).
func RemoveTarget(existing []kvpb.NodeDescriptor) (base.NodeID, bool) {
	if len(existing) == 0 {
		return 0, false
	}
	bestID := base.NodeID(0)
	bestRemaining := -1.0
	ids := make([]base.NodeID, 0, len(existing))
	for _, nd := range existing {
		ids = append(ids, nd.NodeID)
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	for _, id := range ids {
		var remaining []kvpb.NodeDescriptor
		for _, nd := range existing {
			if nd.NodeID != id {
				remaining = append(remaining, nd)
			}
		}
		score := SetDiversity(remaining)
		if score > bestRemaining {
			bestRemaining = score
			bestID = id
		}
	}
	return bestID, true
}

// SetDiversity is the pairwise locality diversity of a replica set — the
// quantity placement maximizes.
func SetDiversity(set []kvpb.NodeDescriptor) float64 {
	var sum float64
	for i := range set {
		for j := i + 1; j < len(set); j++ {
			sum += set[i].Locality.Diversity(set[j].Locality)
		}
	}
	return sum
}

// RebalanceKeepsDiversity reports whether replacing the replica on `remove`
// with one on `add` leaves the set at least as diverse as before. Load
// rebalancing uses it as a non-regression filter: a move driven by range
// counts must never trade away failure-domain spread.
func RebalanceKeepsDiversity(existing []kvpb.NodeDescriptor, remove base.NodeID, add kvpb.NodeDescriptor) bool {
	after := make([]kvpb.NodeDescriptor, 0, len(existing))
	for _, nd := range existing {
		if nd.NodeID != remove {
			after = append(after, nd)
		}
	}
	if len(after) == len(existing) {
		return false // remove is not in the set
	}
	after = append(after, add)
	return SetDiversity(after) >= SetDiversity(existing)
}

// SatisfyingNodes returns the members of nodes a policy admits — what a
// caller needs to tell "this range is under-replicated" from "this
// policy cannot be met by the cluster as it stands".
func SatisfyingNodes(policy base.PlacementPolicy, nodes []kvpb.NodeDescriptor) []kvpb.NodeDescriptor {
	if len(policy.Constraints) == 0 {
		return nodes
	}
	out := make([]kvpb.NodeDescriptor, 0, len(nodes))
	for _, nd := range nodes {
		if policy.Satisfies(nd.Locality) {
			out = append(out, nd)
		}
	}
	return out
}

// Misplaced returns the replicas of a range sitting on nodes the policy
// does not admit: the ones a constrained database needs moved. localities
// maps a node to its locality; a node the caller has never heard from is
// left alone rather than assumed to be in violation.
func Misplaced(policy base.PlacementPolicy, replicas []base.NodeID, localities map[base.NodeID]base.Locality) []base.NodeID {
	if len(policy.Constraints) == 0 {
		return nil
	}
	var out []base.NodeID
	for _, id := range replicas {
		l, known := localities[id]
		if !known {
			continue
		}
		if !policy.Satisfies(l) {
			out = append(out, id)
		}
	}
	return out
}
