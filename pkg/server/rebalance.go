package server

import (
	"context"
	"fmt"

	"github.com/sthorne/datax/pkg/cluster"
	"github.com/sthorne/datax/pkg/kvpb"
	"github.com/sthorne/datax/pkg/placement"
)

// serveRebalance moves a replica: add the target, then remove the source
// (explicit --from, or the most redundant replica if unspecified).
func (n *Node) serveRebalance(ctx context.Context, req cluster.AdminRequest) cluster.AdminResponse {
	if req.RangeID == 0 || req.ToNode == 0 {
		return cluster.AdminResponse{Error: "rebalance requires --range and --to"}
	}
	descs, err := n.listRanges(ctx)
	if err != nil {
		return cluster.AdminResponse{Error: err.Error()}
	}
	var desc *kvpb.RangeDescriptor
	for i := range descs {
		if descs[i].RangeID == req.RangeID {
			desc = &descs[i]
			break
		}
	}
	if desc == nil {
		return cluster.AdminResponse{Error: fmt.Sprintf("range %d not found", req.RangeID)}
	}
	from := req.FromNode
	if from == 0 {
		// Choose the replica whose removal keeps the set most diverse,
		// never the one we are about to add.
		var existing []kvpb.NodeDescriptor
		for _, r := range desc.Replicas {
			if nd, ok := n.registry.Get(r.NodeID); ok && r.NodeID != req.ToNode {
				existing = append(existing, nd)
			}
		}
		var ok bool
		from, ok = placement.RemoveTarget(existing)
		if !ok {
			return cluster.AdminResponse{Error: "could not choose a source replica; specify --from"}
		}
	}
	resp, err := n.db.AdminChangeReplicas(ctx, desc.StartKey, req.ToNode, 0)
	if err != nil {
		return cluster.AdminResponse{Error: fmt.Sprintf("adding replica on n%d: %v", req.ToNode, err)}
	}
	resp, err = n.db.AdminChangeReplicas(ctx, desc.StartKey, 0, from)
	if err != nil {
		return cluster.AdminResponse{Error: fmt.Sprintf("removing replica on n%d: %v", from, err)}
	}
	return cluster.AdminResponse{Ranges: []kvpb.RangeDescriptor{resp.Desc}}
}
