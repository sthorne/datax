package server

import (
	"context"
	"encoding/json"
	"net/http"
	"sort"
	"strconv"
	"sync"
	"time"

	"github.com/sthorne/datax/pkg/base"
	"github.com/sthorne/datax/pkg/cluster"
)

// /api/range?id=N — the cross-node drill-down behind the dashboard's range
// detail view. The serving node reads the range's descriptor, then asks
// every replica-holding node (itself included) for its view of that range
// over the internode admin RPC ("node-status" with a range filter), so any
// node's dashboard can show any range's replicas, leader, and health.
// Admin-gated: it triggers cluster-wide fan-out and exposes per-replica
// internals.

// ReplicaDetail is one node's view of the range (plus registry identity).
type ReplicaDetail struct {
	NodeID   int    `json:"node_id"`
	Address  string `json:"address,omitempty"`
	Locality string `json:"locality,omitempty"`
	Live     bool   `json:"live"`
	// Error notes why this replica's view is missing (node unreachable,
	// replica not yet applied there, or an old binary without the op).
	Error  string       `json:"error,omitempty"`
	Status *RangeStatus `json:"status,omitempty"`
}

// RangeDetail is the /api/range document.
type RangeDetail struct {
	RangeID    int64           `json:"range_id"`
	StartKey   string          `json:"start_key"`
	EndKey     string          `json:"end_key"`
	Generation int64           `json:"generation"`
	Replicas   []ReplicaDetail `json:"replicas"`
}

func (n *Node) serveRangeAPI(w http.ResponseWriter, req *http.Request) {
	id, err := strconv.ParseInt(req.URL.Query().Get("id"), 10, 64)
	if err != nil || id <= 0 {
		http.Error(w, "range id required: /api/range?id=N", http.StatusBadRequest)
		return
	}
	desc, err := n.findRange(req.Context(), base.RangeID(id))
	if err != nil {
		http.Error(w, "range not found: "+err.Error(), http.StatusNotFound)
		return
	}
	doc := RangeDetail{
		RangeID:    int64(desc.RangeID),
		StartKey:   desc.StartKey.String(),
		EndKey:     desc.EndKey.String(),
		Generation: desc.Generation,
	}

	now := n.clock.Now().WallTime
	grace := n.livenessGrace().Nanoseconds()
	doc.Replicas = make([]ReplicaDetail, len(desc.Replicas))
	var wg sync.WaitGroup
	for i, rep := range desc.Replicas {
		rd := &doc.Replicas[i]
		rd.NodeID = int(rep.NodeID)
		if nd, ok := n.registry.Get(rep.NodeID); ok {
			rd.Address = nd.Address
			rd.Locality = nd.Locality.String()
			rd.Live = now-nd.LivenessTime <= grace
		}
		if rep.NodeID == n.ident.NodeID {
			rd.Live = true
			for _, rs := range n.rangeStatuses() {
				if rs.RangeID == doc.RangeID {
					rs := rs
					rd.Status = &rs
					break
				}
			}
			if rd.Status == nil {
				rd.Error = "replica not present on this node"
			}
			continue
		}
		addr, err := n.registry.Resolve(rep.NodeID)
		if err != nil {
			rd.Error = err.Error()
			continue
		}
		wg.Add(1)
		go func(addr string, rd *ReplicaDetail) {
			defer wg.Done()
			ctx, cancel := context.WithTimeout(req.Context(), 3*time.Second)
			defer cancel()
			var resp cluster.AdminResponse
			if err := n.trans.Call(ctx, addr, "admin", cluster.AdminRequest{
				Op: "node-status", RangeID: desc.RangeID,
			}, &resp); err != nil {
				rd.Error = err.Error()
				return
			}
			if resp.Error != "" {
				rd.Error = resp.Error
				return
			}
			var st NodeStatus
			if err := json.Unmarshal(resp.Status, &st); err != nil {
				rd.Error = "undecodable status: " + err.Error()
				return
			}
			if len(st.Ranges) == 0 {
				rd.Error = "replica not present on that node"
				return
			}
			rd.Status = &st.Ranges[0]
		}(addr, rd)
	}
	wg.Wait()
	sort.Slice(doc.Replicas, func(i, j int) bool { return doc.Replicas[i].NodeID < doc.Replicas[j].NodeID })

	w.Header().Set("Content-Type", "application/json")
	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ")
	_ = enc.Encode(doc)
}
