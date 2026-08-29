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
