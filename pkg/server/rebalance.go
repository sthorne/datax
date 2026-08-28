package server

import (
	"context"

	"github.com/sthorne/datax/pkg/cluster"
)

// serveRebalance moves a replica (Phase 7: placement & elasticity).
func (n *Node) serveRebalance(ctx context.Context, req cluster.AdminRequest) cluster.AdminResponse {
	return cluster.AdminResponse{Error: "rebalance not implemented yet"}
}
