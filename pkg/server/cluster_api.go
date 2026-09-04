package server

import (
	"encoding/json"
	"net/http"
	"time"

	"github.com/sthorne/datax/pkg/kvpb"
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
	// Load aggregates from the node's own heartbeat (leader-local QPS;
	// see kvpb.NodeDescriptor).
	LeaderQPS    float64 `json:"leader_qps,omitempty"`
	LeaderCount  int     `json:"leader_count,omitempty"`
	ReplicaBytes int64   `json:"replica_bytes,omitempty"`
	// Machine is the node's host summary from its heartbeat (nil for a
	// node on a binary that does not advertise one).
	Machine *kvpb.MachineSummary `json:"machine,omitempty"`
	// Latency is the node's row of the network matrix: its round trip and
	// clock offset to each peer, from its heartbeat (fresh from the pinger
	// for the serving node itself).
	Latency []kvpb.PeerLatency `json:"latency,omitempty"`
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

// ClusterPrincipal tells the dashboard who it is signed in as, so the page
// can show the identity and explain an admin-only refusal in terms of it
// (the browser caches Basic credentials per realm, so "which user am I"
// is not otherwise visible to the person at the keyboard).
type ClusterPrincipal struct {
	// Secure is false in insecure mode, where there is no identity: User
	// and Via are empty and Admin is true (trust, like everything else).
	Secure bool `json:"secure"`
	// User is the authenticated database user; Via is how it
	// authenticated: "basic" (HTTP Basic credentials) or "cert" (a
	// CA-verified client certificate).
	User string `json:"user,omitempty"`
	Via  string `json:"via,omitempty"`
	// Admin reports whether User holds the admin role (root, an admin-role
	// member, or the node identity), which the range drill-down requires.
	Admin bool `json:"admin"`
}

// ClusterStatus is the /api/cluster document.
type ClusterStatus struct {
	Now    int64 `json:"now_unix_ms"`
	NodeID int   `json:"node_id"`
	// MaxOffsetMs is the clock skew the cluster tolerates (--max-offset);
	// measured offsets are judged against it.
	MaxOffsetMs int64 `json:"max_offset_ms"`
	// Principal is per request: the caller's identity, not cluster state.
	Principal ClusterPrincipal        `json:"principal"`
	Nodes     []ClusterNode           `json:"nodes"`
	Ranges    []ClusterRange          `json:"ranges"`
	Local     NodeStatus              `json:"local"`
	Storage   *storage.StorageMetrics `json:"storage,omitempty"`
	// Error carries a partial-data note (e.g. the meta scan failed during
	// startup or a partition); the rest of the document is still valid.
	Error string `json:"error,omitempty"`
}

func (n *Node) serveClusterAPI(w http.ResponseWriter, req *http.Request) {
	now := n.clock.Now().WallTime
	doc := ClusterStatus{
		Now:       now / int64(time.Millisecond),
		NodeID:    int(n.ident.NodeID),
		Principal: n.clusterPrincipal(req),
		Local:     n.statusSummary(),
	}
	doc.MaxOffsetMs = n.clock.MaxOffset().Milliseconds()
	grace := n.livenessGrace().Nanoseconds()
	for _, nd := range n.registry.All() {
		doc.Nodes = append(doc.Nodes, ClusterNode{
			NodeID:       int(nd.NodeID),
			Address:      nd.Address,
			Locality:     nd.Locality.String(),
			Live:         now-nd.LivenessTime <= grace,
			AgoMs:        (now - nd.LivenessTime) / int64(time.Millisecond),
			Draining:     nd.Draining,
			LeaderQPS:    nd.LeaderQPS,
			LeaderCount:  nd.LeaderCount,
			ReplicaBytes: nd.ReplicaBytes,
			Machine:      nd.Machine,
			Latency:      nd.Latency,
		})
		if nd.NodeID == n.ident.NodeID && n.pinger != nil {
			doc.Nodes[len(doc.Nodes)-1].Latency = n.pinger.Snapshot()
		}
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

// clusterPrincipal describes the request's authenticated caller for the
// dashboard (see ClusterPrincipal).
func (n *Node) clusterPrincipal(req *http.Request) ClusterPrincipal {
	if n.tlsCfgs == nil {
		return ClusterPrincipal{Secure: false, Admin: true}
	}
	p := principalFrom(req)
	return ClusterPrincipal{
		Secure: true,
		User:   p.User,
		Via:    p.Via,
		Admin:  n.isAdminPrincipal(req.Context(), p.User),
	}
}
