package server

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/sthorne/datax/pkg/base"
	"github.com/sthorne/datax/pkg/cluster"
	"github.com/sthorne/datax/pkg/keys"
	"github.com/sthorne/datax/pkg/kvpb"
	"github.com/sthorne/datax/pkg/metrics"
	"github.com/sthorne/datax/pkg/placement"
	"github.com/sthorne/datax/pkg/util/log"
)

// Decommission marks a node draining. The flag's authority is the node's own
// registry row, but that row is overwritten by the node's heartbeat every few
// seconds — so the op is forwarded to the target node itself when reachable
// (it flips its in-memory flag and re-asserts it on every beat, immune to
// the overwrite race), and only falls back to writing the row directly for
// an unreachable node, which adopts the flag from its row if it ever
// returns. The allocator then drains the node's replicas away while it is
// still alive to serve and vote; once empty it can be stopped with zero
// repair churn.

func (n *Node) serveDecommission(ctx context.Context, req cluster.AdminRequest) cluster.AdminResponse {
	if req.NodeID == 0 {
		return cluster.AdminResponse{Error: "decommission requires --node"}
	}
	if req.NodeID == n.ident.NodeID {
		n.draining.Store(!req.Cancel)
		// Publish immediately rather than waiting a heartbeat.
		nd := kvpb.NodeDescriptor{
			NodeID:       n.ident.NodeID,
			Address:      n.addr,
			Locality:     n.cfg.Locality,
			LivenessTime: n.clock.Now().WallTime,
			Draining:     n.draining.Load(),
		}
		raw, _ := json.Marshal(nd)
		if err := n.db.Put(ctx, keys.NodeRegistryKey(n.ident.NodeID), raw); err != nil {
			return cluster.AdminResponse{Error: fmt.Sprintf("publishing draining state: %v", err)}
		}
		n.registry.Upsert(nd)
		return n.decommissionStatus(ctx, req.NodeID, nd.Draining)
	}

	// Forward to the target so its own process holds the flag.
	if addr, err := n.registry.Resolve(req.NodeID); err == nil {
		fctx, cancel := context.WithTimeout(ctx, 10*time.Second)
		var resp cluster.AdminResponse
		err := n.trans.Call(fctx, addr, "admin", req, &resp)
		cancel()
		if err == nil {
			return resp
		}
		log.Warnf("decommission: forwarding to n%d at %s failed (%v); writing its registry row directly", req.NodeID, addr, err)
	}

	// Unreachable target: write its row. A dead node cannot clobber the
	// flag, and one that returns adopts it from the row.
	nd, ok := n.registry.Get(req.NodeID)
	if !ok {
		return cluster.AdminResponse{Error: fmt.Sprintf("unknown node n%d", req.NodeID)}
	}
	nd.Draining = !req.Cancel
	raw, _ := json.Marshal(nd)
	if err := n.db.Put(ctx, keys.NodeRegistryKey(req.NodeID), raw); err != nil {
		return cluster.AdminResponse{Error: fmt.Sprintf("writing draining state: %v", err)}
	}
	n.registry.Upsert(nd)
	return n.decommissionStatus(ctx, req.NodeID, nd.Draining)
}

// decommissionStatus reports drain progress: the flag and how many replicas
// still live on the node.
func (n *Node) decommissionStatus(ctx context.Context, id base.NodeID, draining bool) cluster.AdminResponse {
	descs, err := n.listRanges(ctx)
	if err != nil {
		return cluster.AdminResponse{Error: err.Error()}
	}
	remaining := 0
	for _, d := range descs {
		if _, ok := d.GetReplica(id); ok {
			remaining++
		}
	}
	return cluster.AdminResponse{Draining: draining, RemainingReplicas: remaining}
}

// drainOnce moves replicas off draining nodes, one per draining node per
// tick, while they are still alive to vote — planned decommission instead of
// waiting for dead-node repair. A range is skipped (with a warning) when its
// live replicas would not be a strict majority or when no non-draining
// target exists; it keeps its replica and the drain stalls safely.
func (n *Node) drainOnce(ctx context.Context) {
	now := n.clock.Now().WallTime
	live := map[base.NodeID]kvpb.NodeDescriptor{}
	var draining []kvpb.NodeDescriptor
	for _, nd := range n.registry.All() {
		if nd.NodeID == n.ident.NodeID || now-nd.LivenessTime < int64(n.livenessGrace()) {
			live[nd.NodeID] = nd
			if nd.Draining {
				draining = append(draining, nd)
			}
		}
	}
	if len(draining) == 0 {
		return
	}
	descs, err := n.listRanges(ctx)
	if err != nil {
		log.Debugf("drain: listing ranges: %v", err)
		return
	}
	rangeCount := map[base.NodeID]int{}
	for _, d := range descs {
		for _, r := range d.Replicas {
			rangeCount[r.NodeID]++
		}
	}

	for _, dn := range draining {
		for _, desc := range descs {
			if _, holds := desc.GetReplica(dn.NodeID); !holds {
				continue
			}
			liveCount := 0
			var survivors []kvpb.NodeDescriptor
			for _, r := range desc.Replicas {
				if nd, ok := live[r.NodeID]; ok {
					liveCount++
					if r.NodeID != dn.NodeID {
						survivors = append(survivors, nd)
					}
				}
			}
			if liveCount <= len(desc.Replicas)/2 {
				log.Warnf("%s: %d/%d replicas live; not draining n%d below quorum", desc.RangeID, liveCount, len(desc.Replicas), dn.NodeID)
				continue
			}
			var candidates []placement.Candidate
			for _, nd := range live {
				if nd.Draining {
					continue
				}
				if _, holds := desc.GetReplica(nd.NodeID); !holds {
					candidates = append(candidates, placement.Candidate{Node: nd, RangeCount: rangeCount[nd.NodeID]})
				}
			}
			target, ok := placement.AllocateTarget(survivors, candidates)
			if !ok {
				log.Warnf("%s: no target to drain n%d onto; replica stays until a node joins", desc.RangeID, dn.NodeID)
				continue
			}
			log.Infof("draining %s off n%d to n%d", desc.RangeID, dn.NodeID, target)
			if _, err := n.moveReplica(ctx, desc, target, dn.NodeID); err != nil {
				log.Warnf("draining %s off n%d: %v", desc.RangeID, dn.NodeID, err)
				continue
			}
			metrics.DecommissionMoves.Inc()
			rangeCount[target]++
			break // one move per draining node per tick
		}
	}
}
