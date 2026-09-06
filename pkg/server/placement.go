package server

import (
	"context"
	"sort"

	"github.com/sthorne/datax/pkg/base"
	"github.com/sthorne/datax/pkg/keys"
	"github.com/sthorne/datax/pkg/kvpb"
	"github.com/sthorne/datax/pkg/metrics"
	"github.com/sthorne/datax/pkg/placement"
	"github.com/sthorne/datax/pkg/util/log"
)

// Per-range replica placement (issue #176). A database carries a policy;
// its tables inherit it; a range belongs to the table its start key
// names. Resolution is therefore a map lookup on the table ID, off the
// same catalog scan that builds the schema cache — the allocator runs on
// every tick of the replication loop and must not read the catalog to
// decide where one replica goes.
//
// A range that resolves to nothing — a system range, a meta range, a
// table whose database has no policy, or any range at all before the
// first catalog scan lands — gets the zero policy, which is exactly the
// behaviour every range had before this existed: the cluster default
// replication factor, and any node.

// placementFor returns the policy governing the span [start, end). A
// range must lie WHOLLY inside one table's key space to inherit that
// table's policy: a range straddling two tables could belong to two
// databases with different policies, and there is no honest answer for
// it, so it gets none. That is why a policy splits its tables out into
// their own ranges (pkg/sql: presplitPlacement) and why two ranges under
// different policies never merge (Node.placementBarrier).
func (n *Node) placementFor(start, end keys.Key) base.PlacementPolicy {
	id, ok := tableIDOfKey(start)
	if !ok {
		return base.PlacementPolicy{}
	}
	if lo, hi := keys.TableDataSpan(id); start.Compare(lo) < 0 || (len(end) > 0 && end.Compare(hi) > 0) {
		return base.PlacementPolicy{}
	}
	n.schema.mu.Lock()
	defer n.schema.mu.Unlock()
	return n.schema.placement[id]
}

// placementOf is placementFor for a range descriptor.
func (n *Node) placementOf(desc kvpb.RangeDescriptor) base.PlacementPolicy {
	return n.placementFor(desc.StartKey, desc.EndKey)
}

// placementBarrier backs kvserver's MergeBarrier: adjacent ranges merge
// only when they render the same policy. Merging a policied table's
// range into its neighbour would produce a range no policy governs, and
// the data would drift back out of its region on the next rebalance.
func (n *Node) placementBarrier(start, end keys.Key) string {
	p := n.placementFor(start, end)
	if p.IsZero() {
		return ""
	}
	return p.Normalize().String()
}

// anyPlacementPolicy reports whether any database in the cluster carries
// a policy. The allocator's extra passes — moving a misplaced replica
// home, and the health finding for a policy nothing satisfies — cost a
// scan per tick, so they are skipped entirely on a cluster that has
// never used placement, which is every cluster until someone writes a
// policy.
func (n *Node) anyPlacementPolicy() bool {
	n.schema.mu.Lock()
	defer n.schema.mu.Unlock()
	return len(n.schema.placement) > 0
}

// localitiesOf maps node ID to locality for a set of registry entries.
func localitiesOf(nodes map[base.NodeID]kvpb.NodeDescriptor) map[base.NodeID]base.Locality {
	out := make(map[base.NodeID]base.Locality, len(nodes))
	for id, nd := range nodes {
		out[id] = nd.Locality
	}
	return out
}

// enforcePlacementOnce moves one replica that a policy does not admit
// onto a node that does. It is what makes ALTER DATABASE ... SET take
// effect on data that already exists: the other passes only act on a
// range that is short a replica, has a dead one, or is on an overloaded
// node, and a range whose replicas are merely in the wrong region is
// none of those.
//
// Add-then-remove, one range per tick, and only while the range is
// otherwise healthy — every replica live and the membership at its full
// count. A range that cannot be repaired is not a range to relocate, and
// a policy that no live node satisfies leaves the data exactly where it
// is: the health API reports it (see placementFindings) rather than the
// allocator quietly widening the policy.
//
// Reports whether it acted, so the caller can hold off the load passes
// for this tick.
func (n *Node) enforcePlacementOnce(ctx context.Context) bool {
	if !n.anyPlacementPolicy() {
		return false
	}
	live, ok := n.balanceLiveSet()
	if !ok || len(live) < 2 {
		return false
	}
	descs, err := n.listRanges(ctx)
	if err != nil {
		log.Debugf("placement: listing ranges: %v", err)
		return false
	}
	sort.Slice(descs, func(i, j int) bool { return descs[i].RangeID < descs[j].RangeID })
	localities := localitiesOf(live)
	rangeCount := map[base.NodeID]int{}
	for _, d := range descs {
		for _, r := range d.Replicas {
			rangeCount[r.NodeID]++
		}
	}

	for _, desc := range descs {
		policy := n.placementOf(desc)
		if len(policy.Constraints) == 0 || !allReplicasLive(desc, live) {
			continue
		}
		var ids []base.NodeID
		for _, r := range desc.Replicas {
			ids = append(ids, r.NodeID)
		}
		bad := placement.Misplaced(policy, ids, localities)
		if len(bad) == 0 {
			continue
		}
		if len(desc.Replicas) < policy.ReplicasOr(base.DefaultReplicationFactor) {
			continue // up-replication has priority; it runs earlier this tick
		}
		from := bad[0]
		var survivors []kvpb.NodeDescriptor
		for _, r := range desc.Replicas {
			if r.NodeID != from {
				survivors = append(survivors, live[r.NodeID])
			}
		}
		var candidates []placement.Candidate
		for _, nd := range live {
			if nd.Leaving() {
				continue
			}
			if _, holds := desc.GetReplica(nd.NodeID); !holds {
				candidates = append(candidates, placement.Candidate{Node: nd, RangeCount: rangeCount[nd.NodeID]})
			}
		}
		target, ok := placement.AllocateTargetFor(policy, survivors, candidates)
		if !ok {
			log.Warnf("%s: replica on n%d is outside placement %s and no live node satisfies it; leaving it in place",
				desc.RangeID, from, policy)
			continue
		}
		log.Infof("placement: moving %s off n%d to n%d to satisfy %s", desc.RangeID, from, target, policy)
		n.events.Record("placement", "%s: moving the replica on n%d to n%d to satisfy %s", desc.RangeID, from, target, policy)
		if _, err := n.moveReplica(ctx, desc, target, from); err != nil {
			log.Warnf("placement: moving %s off n%d: %v", desc.RangeID, from, err)
			return true // attempted: don't stack another op this tick
		}
		metrics.PlacementMoves.Inc()
		return true // one move per tick
	}
	return false
}
