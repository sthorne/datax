package server

import (
	"context"
	"fmt"
	"strings"

	"github.com/sthorne/datax/pkg/base"
	"github.com/sthorne/datax/pkg/cluster"
	"github.com/sthorne/datax/pkg/kvpb"
	"github.com/sthorne/datax/pkg/placement"
)

// moveReplica relocates one replica of desc from one node to another: add
// the target first (membership never dips), then remove the source. When
// the source currently leads the range, the removal is refused server-side;
// transfer leadership to the freshly added target and retry once. Returns
// the final descriptor.
func (n *Node) moveReplica(ctx context.Context, desc kvpb.RangeDescriptor, to, from base.NodeID) (kvpb.RangeDescriptor, error) {
	added, err := n.db.AdminChangeReplicas(ctx, desc.StartKey, to, 0)
	if err != nil {
		return desc, fmt.Errorf("adding replica on n%d: %w", to, err)
	}
	cur := added.Desc
	removed, err := n.db.AdminChangeReplicas(ctx, desc.StartKey, 0, from)
	if err != nil && strings.Contains(err.Error(), "refusing to remove the leader's own replica") {
		if terr := n.db.AdminTransferLease(ctx, desc.StartKey, to); terr != nil {
			return cur, fmt.Errorf("transferring lease off n%d: %w", from, terr)
		}
		removed, err = n.db.AdminChangeReplicas(ctx, desc.StartKey, 0, from)
	}
	if err != nil {
		return cur, fmt.Errorf("removing replica on n%d: %w", from, err)
	}
	return removed.Desc, nil
}

// findRange resolves a range ID against the /meta records.
func (n *Node) findRange(ctx context.Context, id base.RangeID) (kvpb.RangeDescriptor, error) {
	descs, err := n.listRanges(ctx)
	if err != nil {
		return kvpb.RangeDescriptor{}, err
	}
	for _, d := range descs {
		if d.RangeID == id {
			return d, nil
		}
	}
	return kvpb.RangeDescriptor{}, fmt.Errorf("range %d not found", id)
}

// serveRebalance moves a replica: add the target, then remove the source
// (explicit --from, or the most redundant replica if unspecified).
func (n *Node) serveRebalance(ctx context.Context, req cluster.AdminRequest) cluster.AdminResponse {
	if req.RangeID == 0 || req.ToNode == 0 {
		return cluster.AdminResponse{Error: "rebalance requires --range and --to"}
	}
	desc, err := n.findRange(ctx, req.RangeID)
	if err != nil {
		return cluster.AdminResponse{Error: err.Error()}
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
	final, err := n.moveReplica(ctx, desc, req.ToNode, from)
	if err != nil {
		return cluster.AdminResponse{Error: err.Error()}
	}
	return cluster.AdminResponse{Ranges: []kvpb.RangeDescriptor{final}}
}

// serveTransferLease moves a range's leadership (= its lease) to --to.
func (n *Node) serveTransferLease(ctx context.Context, req cluster.AdminRequest) cluster.AdminResponse {
	if req.RangeID == 0 || req.ToNode == 0 {
		return cluster.AdminResponse{Error: "transfer-lease requires --range and --to"}
	}
	desc, err := n.findRange(ctx, req.RangeID)
	if err != nil {
		return cluster.AdminResponse{Error: err.Error()}
	}
	if err := n.db.AdminTransferLease(ctx, desc.StartKey, req.ToNode); err != nil {
		return cluster.AdminResponse{Error: err.Error()}
	}
	return cluster.AdminResponse{Ranges: []kvpb.RangeDescriptor{desc}}
}
