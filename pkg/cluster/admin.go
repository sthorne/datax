package cluster

import (
	"github.com/sthorne/datax/pkg/base"
	"github.com/sthorne/datax/pkg/kvpb"
)

// AdminRequest is the JSON body of admin RPCs (datax debug ...).
type AdminRequest struct {
	// Op: "split", "ranges", "nodes", "rebalance", "transfer-lease".
	Op string `json:"op"`
	// Key for split (raw key bytes).
	Key []byte `json:"key,omitempty"`
	// RangeID and target/source nodes for rebalance.
	RangeID  base.RangeID `json:"range_id,omitempty"`
	ToNode   base.NodeID  `json:"to_node,omitempty"`
	FromNode base.NodeID  `json:"from_node,omitempty"`
}

// AdminResponse is the JSON reply.
type AdminResponse struct {
	Error  string                 `json:"error,omitempty"`
	Ranges []kvpb.RangeDescriptor `json:"ranges,omitempty"`
	Nodes  []kvpb.NodeDescriptor  `json:"nodes,omitempty"`
	Left   *kvpb.RangeDescriptor  `json:"left,omitempty"`
	Right  *kvpb.RangeDescriptor  `json:"right,omitempty"`
}
