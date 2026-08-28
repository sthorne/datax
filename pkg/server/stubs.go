package server

import (
	"context"
	"encoding/json"

	"github.com/sthorne/datax/pkg/base"
)

func kvNodeID(v int64) base.NodeID { return base.NodeID(v) }

// handleAdmin serves cluster admin RPCs (implemented with the multi-range
// phase: split, ranges, nodes, rebalance).
func (n *Node) handleAdmin(ctx context.Context, data []byte) ([]byte, error) {
	return json.Marshal(map[string]string{"error": "admin operations not implemented yet"})
}

// startSQL brings up the SQL/pgwire front end (Phase 6).
func (n *Node) startSQL() error { return nil }
