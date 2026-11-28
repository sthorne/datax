package placement

import "github.com/sthorne/datax/pkg/base"

// Compliance (issue #405): whether the replicas a policy governs are
// where it says. The allocator and the health check ask what to move;
// a residency check asks whether anything could be outside the policy,
// so the questions here answer conservatively. A replica on a node of
// unknown locality is not assumed to be in place, and a policy is
// unsatisfiable as soon as it wants more replicas than there are live
// nodes to hold them, not only when no node matches at all.

// NodeState is what compliance needs to know about one node.
type NodeState struct {
	Locality base.Locality
	// Usable: the node is live and not being drained, so the allocator
	// may place a replica on it.
	Usable bool
}

// Eligible counts the usable nodes whose locality satisfies policy.
func Eligible(policy base.PlacementPolicy, nodes map[base.NodeID]NodeState) int {
	n := 0
	for _, st := range nodes {
		if st.Usable && policy.Satisfies(st.Locality) {
			n++
		}
	}
	return n
}

// Unsatisfiable reports whether a policy with constraints wants more
// replicas than eligible nodes can hold. Such a range cannot be brought
// into line: the allocator adds a replica before it removes one, so a
// misplaced replica stays where it is until enough matching nodes join,
// and a lost one is never replaced. A policy without constraints is
// never unsatisfiable here: too few nodes is under-replication, which
// the health check reports on its own.
func Unsatisfiable(policy base.PlacementPolicy, eligible int) bool {
	return len(policy.Constraints) > 0 && eligible < policy.ReplicasOr(base.DefaultReplicationFactor)
}

// UnknownLocality returns the replicas whose node's locality is not
// known: a node missing from the registry (gone, or not yet heard from).
// Misplaced skips them; a compliance check must not.
func UnknownLocality(replicas []base.NodeID, nodes map[base.NodeID]NodeState) []base.NodeID {
	var out []base.NodeID
	for _, id := range replicas {
		if _, ok := nodes[id]; !ok {
			out = append(out, id)
		}
	}
	return out
}

// Localities projects nodes onto their localities, the shape Misplaced
// takes.
func Localities(nodes map[base.NodeID]NodeState) map[base.NodeID]base.Locality {
	out := make(map[base.NodeID]base.Locality, len(nodes))
	for id, st := range nodes {
		out[id] = st.Locality
	}
	return out
}
