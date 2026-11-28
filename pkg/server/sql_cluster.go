package server

import (
	"context"
	"time"

	"github.com/sthorne/datax/pkg/kvpb"
	"github.com/sthorne/datax/pkg/sql"
)

// sqlCluster is the node's view of the cluster as SQL sees it (issue
// #405): SHOW NODES, SHOW RANGES and SHOW PLACEMENT's compliance read
// it, with the HTTP status API on or off.
type sqlCluster struct{ n *Node }

// Nodes lists the registry with each node's liveness judged as the
// health check judges it.
func (c sqlCluster) Nodes(ctx context.Context) ([]sql.ClusterNode, error) {
	n := c.n
	now := n.clock.Now().WallTime
	grace := n.livenessGrace().Nanoseconds()
	dead := n.deadNodeThreshold().Nanoseconds()
	var out []sql.ClusterNode
	for _, nd := range n.registry.All() {
		state := sql.NodeLive
		switch age := now - nd.LivenessTime; {
		case age > dead:
			state = sql.NodeDead
		case age > grace:
			state = sql.NodeUnresponsive
		}
		var hb time.Time
		if nd.LivenessTime > 0 {
			hb = time.Unix(0, nd.LivenessTime).UTC()
		}
		out = append(out, sql.ClusterNode{
			NodeID: nd.NodeID, Address: nd.Address, Locality: nd.Locality, State: state,
			Draining: nd.Draining, ShuttingDown: nd.ShuttingDown, LastHeartbeat: hb,
		})
	}
	return out, nil
}

// Ranges lists every range descriptor from /meta. Unlike the status
// endpoints it never falls back to an older listing: a compliance
// answer read from a stale one could call a database satisfied after a
// replica moved out of its region.
func (c sqlCluster) Ranges(ctx context.Context) ([]kvpb.RangeDescriptor, error) {
	ctx, cancel := context.WithTimeout(ctx, rangeListTimeout)
	defer cancel()
	return c.n.listRanges(ctx)
}
