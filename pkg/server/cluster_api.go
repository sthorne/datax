package server

import (
	"encoding/json"
	"net/http"
	"time"

	"github.com/sthorne/datax/pkg/storage"
)

// The read-only JSON document behind the web UI (/api/cluster). Assembled
// entirely node-locally: the node registry gives every peer's address,
// locality, liveness and draining state; the /meta scan gives the whole
// cluster's range descriptors; the local store contributes per-replica
// detail and storage health. Nothing here mutates state.

// ClusterNode is one row of the cluster's node table.
type ClusterNode struct {
	NodeID   int    `json:"node_id"`
	Address  string `json:"address"`
	Locality string `json:"locality,omitempty"`
	// Live is derived from the last heartbeat vs the liveness grace
	// window (the same rule the allocator uses).
	Live     bool  `json:"live"`
	AgoMs    int64 `json:"heartbeat_ago_ms"`
	Draining bool  `json:"draining,omitempty"`
}

// ClusterRange is one cluster-wide range descriptor (from /meta — every
// range, not just this store's).
type ClusterRange struct {
	RangeID    int64  `json:"range_id"`
	StartKey   string `json:"start_key"`
	EndKey     string `json:"end_key"`
	Replicas   []int  `json:"replicas"`
	Generation int64  `json:"generation"`
}

// ClusterStatus is the /api/cluster document.
type ClusterStatus struct {
	Now     int64                   `json:"now_unix_ms"`
	NodeID  int                     `json:"node_id"`
	Nodes   []ClusterNode           `json:"nodes"`
	Ranges  []ClusterRange          `json:"ranges"`
	Local   NodeStatus              `json:"local"`
	Storage *storage.StorageMetrics `json:"storage,omitempty"`
	// Error carries a partial-data note (e.g. the meta scan failed during
	// startup or a partition); the rest of the document is still valid.
	Error string `json:"error,omitempty"`
}

func (n *Node) serveClusterAPI(w http.ResponseWriter, req *http.Request) {
	now := n.clock.Now().WallTime
	doc := ClusterStatus{
		Now:    now / int64(time.Millisecond),
		NodeID: int(n.ident.NodeID),
		Local:  n.statusSummary(),
	}
	grace := n.livenessGrace().Nanoseconds()
	for _, nd := range n.registry.All() {
		doc.Nodes = append(doc.Nodes, ClusterNode{
			NodeID:   int(nd.NodeID),
			Address:  nd.Address,
			Locality: nd.Locality.String(),
			Live:     now-nd.LivenessTime <= grace,
			AgoMs:    (now - nd.LivenessTime) / int64(time.Millisecond),
			Draining: nd.Draining,
		})
	}
	if descs, err := n.listRanges(req.Context()); err != nil {
		doc.Error = "cluster range listing unavailable: " + err.Error()
	} else {
		for _, d := range descs {
			cr := ClusterRange{
				RangeID:    int64(d.RangeID),
				StartKey:   d.StartKey.String(),
				EndKey:     d.EndKey.String(),
				Generation: d.Generation,
			}
			for _, rep := range d.Replicas {
				cr.Replicas = append(cr.Replicas, int(rep.NodeID))
			}
			doc.Ranges = append(doc.Ranges, cr)
		}
	}
	if n.engine != nil {
		m := n.engine.StorageMetrics()
		doc.Storage = &m
	}
	w.Header().Set("Content-Type", "application/json")
	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ")
	_ = enc.Encode(doc)
}
