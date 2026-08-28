package server

import (
	"context"
	"time"

	"github.com/sthorne/datax/pkg/base"
	"github.com/sthorne/datax/pkg/kvpb"
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
		cancel()
	}
}

func (n *Node) upreplicateOnce(ctx context.Context) {
	descs, err := n.listRanges(ctx)
	if err != nil {
		log.Debugf("upreplication: listing ranges: %v", err)
		return
	}

	now := n.clock.Now().WallTime
	liveNodes := map[base.NodeID]kvpb.NodeDescriptor{}
	for _, nd := range n.registry.All() {
		if nd.NodeID == n.ident.NodeID || now-nd.LivenessTime < int64(n.livenessGrace()) {
			liveNodes[nd.NodeID] = nd
		}
	}
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
		rangeCount[target]++
	}
}
