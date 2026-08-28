package server

import (
	"context"
	"time"

	"github.com/sthorne/datax/pkg/base"
	"github.com/sthorne/datax/pkg/kvpb"
	"github.com/sthorne/datax/pkg/placement"
	"github.com/sthorne/datax/pkg/util/log"
)

// livenessGrace is how stale a node's registry heartbeat may be before the
// allocator stops considering it live.
const livenessGrace = 15 * time.Second

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
		if nd.NodeID == n.ident.NodeID || now-nd.LivenessTime < int64(livenessGrace) {
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
