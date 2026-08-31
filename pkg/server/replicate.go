package server

import (
	"context"
	"sort"
	"strings"
	"time"

	"github.com/sthorne/datax/pkg/base"
	"github.com/sthorne/datax/pkg/kvpb"
	"github.com/sthorne/datax/pkg/metrics"
	"github.com/sthorne/datax/pkg/placement"
	"github.com/sthorne/datax/pkg/util/log"
)

// defaultLivenessGrace is how stale a node's registry heartbeat may be
// before the allocator stops considering it live.
const defaultLivenessGrace = 15 * time.Second

// defaultDeadNodeThreshold is how stale a node's heartbeat must be before
// its replicas are repaired away. Deliberately larger than the liveness
// grace: the gap is the window in which a briefly-restarting node causes no
// replica churn.
const defaultDeadNodeThreshold = 30 * time.Second

func (n *Node) livenessGrace() time.Duration {
	if n.cfg.LivenessGrace > 0 {
		return n.cfg.LivenessGrace
	}
	return defaultLivenessGrace
}

func (n *Node) deadNodeThreshold() time.Duration {
	if n.cfg.DeadNodeThreshold > 0 {
		return n.cfg.DeadNodeThreshold
	}
	return defaultDeadNodeThreshold
}

// upreplicationLoop raises under-replicated ranges to the replication
// factor. Only the node currently leading range 1 acts (a cheap way to have
// exactly one allocator without a separate election); every node runs the
// loop and checks.
func (n *Node) upreplicationLoop(ctx context.Context) {
	interval := n.cfg.UpreplicationInterval
	if interval == 0 {
		interval = 3 * time.Second
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
		r1, ok := n.store.GetReplica(1)
		if !ok || !r1.IsLeader() {
			continue
		}
		wctx, cancel := context.WithTimeout(ctx, interval*4)
		n.upreplicateOnce(wctx)
		n.repairDeadOnce(wctx)
		n.drainOnce(wctx)
		// The balancing passes run in strict priority and perform at most
		// ONE op per tick between them: acting on load statistics that a
		// just-performed move has already invalidated only causes churn.
		if !n.rebalanceOnce(wctx) {
			if !n.shedLeaseOnce(wctx) {
				n.rebalanceBytesOnce(wctx)
			}
		}
		cancel()
	}
}

// liveNodes returns the registry entries currently considered live: this
// node itself, plus every peer whose heartbeat is within the liveness
// grace window.
func (n *Node) liveNodes() map[base.NodeID]kvpb.NodeDescriptor {
	now := n.clock.Now().WallTime
	live := map[base.NodeID]kvpb.NodeDescriptor{}
	for _, nd := range n.registry.All() {
		if nd.NodeID == n.ident.NodeID || now-nd.LivenessTime < int64(n.livenessGrace()) {
			live[nd.NodeID] = nd
		}
	}
	return live
}

func (n *Node) upreplicateOnce(ctx context.Context) {
	descs, err := n.listRanges(ctx)
	if err != nil {
		log.Debugf("upreplication: listing ranges: %v", err)
		return
	}

	liveNodes := n.liveNodes()
	rangeCount := map[base.NodeID]int{}
	for _, d := range descs {
		for _, r := range d.Replicas {
			rangeCount[r.NodeID]++
		}
	}

	for _, desc := range descs {
		if len(desc.Replicas) >= base.DefaultReplicationFactor {
			continue
		}
		var existing []kvpb.NodeDescriptor
		for _, r := range desc.Replicas {
			if nd, ok := liveNodes[r.NodeID]; ok {
				existing = append(existing, nd)
			} else if nd, ok := n.registry.Get(r.NodeID); ok {
				existing = append(existing, nd)
			}
		}
		var candidates []placement.Candidate
		for _, nd := range liveNodes {
			if nd.Draining {
				continue // never place new replicas on a draining node
			}
			if _, holds := desc.GetReplica(nd.NodeID); !holds {
				candidates = append(candidates, placement.Candidate{Node: nd, RangeCount: rangeCount[nd.NodeID]})
			}
		}
		target, ok := placement.AllocateTarget(existing, candidates)
		if !ok {
			continue // not enough distinct live nodes yet
		}
		log.Infof("upreplicating %s (%d/%d replicas) to n%d", desc.RangeID, len(desc.Replicas), base.DefaultReplicationFactor, target)
		if _, err := n.db.AdminChangeReplicas(ctx, desc.StartKey, target, 0); err != nil {
			log.Warnf("upreplicating %s to n%d: %v", desc.RangeID, target, err)
			continue
		}
		rangeCount[target]++
	}
}

// repairDeadOnce replaces replicas living on dead nodes (heartbeat staler
// than DeadNodeThreshold): for each affected range, add an allocator-picked
// live target first, then remove the dead replica — the membership never
// dips below its starting size mid-repair. At most one repair per range per
// tick. Skipped when the range's live replicas are not a strict majority
// (the ConfChange could not commit, and removing members would only make a
// bad situation worse) or when no diversity-valid spare exists.
func (n *Node) repairDeadOnce(ctx context.Context) {
	now := n.clock.Now().WallTime
	dead := map[base.NodeID]bool{}
	live := map[base.NodeID]kvpb.NodeDescriptor{}
	for _, nd := range n.registry.All() {
		switch {
		case nd.NodeID == n.ident.NodeID:
			live[nd.NodeID] = nd
		case now-nd.LivenessTime > int64(n.deadNodeThreshold()):
			dead[nd.NodeID] = true
		case now-nd.LivenessTime < int64(n.livenessGrace()):
			live[nd.NodeID] = nd
		}
	}
	if len(dead) == 0 {
		return
	}
	descs, err := n.listRanges(ctx)
	if err != nil {
		log.Debugf("dead-node repair: listing ranges: %v", err)
		return
	}
	rangeCount := map[base.NodeID]int{}
	for _, d := range descs {
		for _, r := range d.Replicas {
			rangeCount[r.NodeID]++
		}
	}

	for _, desc := range descs {
		var deadNode base.NodeID
		liveCount := 0
		var survivors []kvpb.NodeDescriptor
		for _, r := range desc.Replicas {
			if dead[r.NodeID] {
				if deadNode == 0 {
					deadNode = r.NodeID
				}
				continue
			}
			if nd, ok := live[r.NodeID]; ok {
				liveCount++
				survivors = append(survivors, nd)
			}
		}
		if deadNode == 0 {
			continue
		}
		if liveCount <= len(desc.Replicas)/2 {
			log.Warnf("%s: %d/%d replicas live; cannot repair without quorum", desc.RangeID, liveCount, len(desc.Replicas))
			continue
		}
		var candidates []placement.Candidate
		for _, nd := range live {
			if nd.Draining {
				continue // never place new replicas on a draining node
			}
			if _, holds := desc.GetReplica(nd.NodeID); !holds {
				candidates = append(candidates, placement.Candidate{Node: nd, RangeCount: rangeCount[nd.NodeID]})
			}
		}
		target, ok := placement.AllocateTarget(survivors, candidates)
		if !ok {
			continue // no spare live node to repair onto
		}
		log.Infof("repairing %s: replacing dead n%d with n%d", desc.RangeID, deadNode, target)
		if _, err := n.db.AdminChangeReplicas(ctx, desc.StartKey, target, 0); err != nil {
			log.Warnf("repairing %s: adding replica on n%d: %v", desc.RangeID, target, err)
			continue
		}
		if _, err := n.db.AdminChangeReplicas(ctx, desc.StartKey, 0, deadNode); err != nil {
			log.Warnf("repairing %s: removing dead replica on n%d: %v", desc.RangeID, deadNode, err)
			continue
		}
		metrics.DeadNodeRepairs.Inc()
		rangeCount[target]++
	}
}

// defaultRebalanceThreshold is the range-count spread that triggers a
// rebalance move. 2 is the minimum with hysteresis: one move narrows the
// spread by 2, so a cluster balanced to a spread of 1 never moves anything
// and oscillation is impossible.
const defaultRebalanceThreshold = 2

func (n *Node) rebalanceThreshold() int {
	if n.cfg.RebalanceThreshold != 0 {
		return n.cfg.RebalanceThreshold
	}
	return defaultRebalanceThreshold
}

// rebalanceOnce evens out range counts across live nodes: when the spread
// between the most- and least-loaded node reaches the threshold, move one
// replica (per tick, total) from the fullest node to the emptiest, never
// trading away failure-domain diversity. Skipped entirely while any node is
// dead — repair has priority, and rebalancing against a shrinking live set
// only causes churn. Reports whether it performed (or attempted) an op, so
// the lower-priority load passes can hold off this tick.
func (n *Node) rebalanceOnce(ctx context.Context) bool {
	threshold := n.rebalanceThreshold()
	if threshold < 0 {
		return false
	}
	live, ok := n.balanceLiveSet()
	if !ok || len(live) < 2 {
		return false
	}
	descs, err := n.listRanges(ctx)
	if err != nil {
		log.Debugf("rebalance: listing ranges: %v", err)
		return false
	}
	sort.Slice(descs, func(i, j int) bool { return descs[i].RangeID < descs[j].RangeID })

	// A range left over-replicated by a crashed add-then-remove is repaired
	// first (one per tick), before load is considered.
	for _, desc := range descs {
		if len(desc.Replicas) <= base.DefaultReplicationFactor || !allReplicasLive(desc, live) {
			continue
		}
		var existing []kvpb.NodeDescriptor
		for _, r := range desc.Replicas {
			existing = append(existing, live[r.NodeID])
		}
		from, ok := placement.RemoveTarget(existing)
		if !ok {
			continue
		}
		log.Infof("removing surplus replica of %s from n%d", desc.RangeID, from)
		if err := n.removeReplicaFrom(ctx, desc, from); err != nil {
			log.Warnf("removing surplus replica of %s from n%d: %v", desc.RangeID, from, err)
		}
		return true
	}

	// Spread check across every live node, spares included.
	rangeCount := map[base.NodeID]int{}
	for id := range live {
		rangeCount[id] = 0
	}
	for _, d := range descs {
		for _, r := range d.Replicas {
			if _, ok := live[r.NodeID]; ok {
				rangeCount[r.NodeID]++
			}
		}
	}
	var max, dst base.NodeID
	for id, c := range rangeCount {
		if max == 0 || c > rangeCount[max] || (c == rangeCount[max] && id < max) {
			max = id
		}
		if dst == 0 || c < rangeCount[dst] || (c == rangeCount[dst] && id < dst) {
			dst = id
		}
	}
	if rangeCount[max]-rangeCount[dst] < threshold {
		return false
	}

	// Any node at the maximum count may donate — if the fullest node's
	// replicas are all diversity-pinned, an equally full peer can still move.
	for _, desc := range descs {
		if len(desc.Replicas) < base.DefaultReplicationFactor || !allReplicasLive(desc, live) {
			continue
		}
		if _, onDst := desc.GetReplica(dst); onDst {
			continue
		}
		var existing []kvpb.NodeDescriptor
		for _, r := range desc.Replicas {
			existing = append(existing, live[r.NodeID])
		}
		for _, r := range desc.Replicas {
			src := r.NodeID
			if rangeCount[src] != rangeCount[max] || src == dst {
				continue
			}
			if !placement.RebalanceKeepsDiversity(existing, src, live[dst]) {
				continue
			}
			log.Infof("rebalancing %s: n%d (%d ranges) -> n%d (%d ranges)", desc.RangeID, src, rangeCount[src], dst, rangeCount[dst])
			if _, err := n.moveReplica(ctx, desc, dst, src); err != nil {
				log.Warnf("rebalancing %s: %v", desc.RangeID, err)
				return true // attempted: don't stack another op this tick
			}
			metrics.Rebalances.Inc()
			return true // one move per tick
		}
	}
	return false
}

func allReplicasLive(desc kvpb.RangeDescriptor, live map[base.NodeID]kvpb.NodeDescriptor) bool {
	for _, r := range desc.Replicas {
		if _, ok := live[r.NodeID]; !ok {
			return false
		}
	}
	return true
}

// removeReplicaFrom removes one replica, transferring leadership to another
// member first when the source leads the range.
func (n *Node) removeReplicaFrom(ctx context.Context, desc kvpb.RangeDescriptor, from base.NodeID) error {
	_, err := n.db.AdminChangeReplicas(ctx, desc.StartKey, 0, from)
	if err != nil && strings.Contains(err.Error(), "refusing to remove the leader's own replica") {
		for _, r := range desc.Replicas {
			if r.NodeID != from {
				if terr := n.db.AdminTransferLease(ctx, desc.StartKey, r.NodeID); terr == nil {
					break
				}
			}
		}
		_, err = n.db.AdminChangeReplicas(ctx, desc.StartKey, 0, from)
	}
	return err
}
