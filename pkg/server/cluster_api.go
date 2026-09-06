package server

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"sync"
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
	// ShuttingDown: the node is draining for a stop (kvpb.NodeDescriptor).
	ShuttingDown bool `json:"shutting_down,omitempty"`
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
	// SQL is the node's SQL activity summary from its heartbeat (live for
	// the serving node).
	SQL *kvpb.SQLSummary `json:"sql,omitempty"`
	// HotRanges are the node's heaviest mature leaseholders by QPS and
	// BigRanges its largest replicas by bytes, top-K each, straight from
	// the heartbeat the allocator already reads (issue #152). QPS is
	// leader-local, so these are the LEADER's own measurement of ranges
	// it leads — not a cluster total, and the console says so.
	HotRanges []kvpb.HotRange `json:"hot_ranges,omitempty"`
	BigRanges []kvpb.HotRange `json:"big_ranges,omitempty"`
}

// ClusterRange is one cluster-wide range descriptor (from /meta — every
// range, not just this store's).
type ClusterRange struct {
	RangeID    int64  `json:"range_id"`
	StartKey   string `json:"start_key"`
	EndKey     string `json:"end_key"`
	Replicas   []int  `json:"replicas"`
	Generation int64  `json:"generation"`
	// Table names the table the range's start key belongs to ("" for
	// system and meta ranges, or before the first catalog scan).
	Table string `json:"table,omitempty"`
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
	// authenticated: "basic" (HTTP Basic credentials), "cert" (a
	// CA-verified client certificate), or "session" (the console's
	// sign-in cookie, issue #158).
	User string `json:"user,omitempty"`
	Via  string `json:"via,omitempty"`
	// SessionExpiresAt is when the sign-in cookie lapses, for a console
	// that wants to prompt before it does; absent for the other doors.
	SessionExpiresAt int64 `json:"session_expires_at_unix_ms,omitempty"`
	// Admin reports whether User holds the admin role (root, an admin-role
	// member, or the node identity), which the range drill-down requires.
	Admin bool `json:"admin"`
}

// ClusterRollup is the cluster-level summary of the /api/cluster
// document (issue #145): the load figures every node's heartbeat carries
// (kvpb.NodeDescriptor), summed over the LIVE nodes so that the same
// numbers come out whichever node serves the page — the serving node is
// provenance, not the subject. Contributing says how many nodes a sum
// covers, so a node that is down (or on a binary that advertises no
// figures) shows as a smaller count beside the figure rather than as a
// smaller figure with no explanation.
type ClusterRollup struct {
	Nodes     int `json:"nodes"`
	LiveNodes int `json:"live_nodes"`
	// Contributing is the number of live nodes whose figures the load
	// sums below cover; SQLContributing the number whose heartbeat
	// carried a SQL summary (a node on an older binary carries none).
	Contributing    int `json:"contributing"`
	SQLContributing int `json:"sql_contributing"`
	// QPS sums the nodes' leader-local request rates; DataBytes their
	// replica bytes; Leases the ranges they lead.
	QPS       float64 `json:"qps"`
	DataBytes int64   `json:"data_bytes"`
	Leases    int     `json:"leases"`
	// Ranges and Replicas come from the cluster's range descriptors
	// (every range, not just the serving node's).
	Ranges   int `json:"ranges"`
	Replicas int `json:"replicas"`
	// SQL: connections by state and by user, cumulative statements and
	// serialization failures (the page differences consecutive
	// documents for rates), the worst p99 and the node that owns it, and
	// the longest an open transaction has been idle anywhere.
	Connections           int               `json:"connections"`
	Active                int               `json:"active"`
	IdleInTxn             int               `json:"idle_in_txn"`
	ConnectionsByUser     map[string]int    `json:"connections_by_user,omitempty"`
	Statements            uint64            `json:"statements"`
	StatementsByKind      map[string]uint64 `json:"statements_by_kind,omitempty"`
	SerializationFailures uint64            `json:"serialization_failures"`
	WorstP99Micros        int64             `json:"worst_p99_us"`
	WorstP99Node          int               `json:"worst_p99_node,omitempty"`
	OldestIdleTxnMillis   int64             `json:"oldest_idle_txn_ms,omitempty"`
}

// rollup sums the live nodes' heartbeat figures and the range listing.
func rollup(nodes []ClusterNode, ranges []ClusterRange) ClusterRollup {
	r := ClusterRollup{Nodes: len(nodes), Ranges: len(ranges)}
	for _, cr := range ranges {
		r.Replicas += len(cr.Replicas)
	}
	for _, n := range nodes {
		if !n.Live {
			continue
		}
		r.LiveNodes++
		r.Contributing++
		r.QPS += n.LeaderQPS
		r.DataBytes += n.ReplicaBytes
		r.Leases += n.LeaderCount
		q := n.SQL
		if q == nil {
			continue
		}
		r.SQLContributing++
		r.Connections += q.Open
		r.Active += q.Active
		r.IdleInTxn += q.IdleInTxn
		for user, c := range q.ByUser {
			if r.ConnectionsByUser == nil {
				r.ConnectionsByUser = map[string]int{}
			}
			r.ConnectionsByUser[user] += c
		}
		for kind, c := range q.Statements {
			if r.StatementsByKind == nil {
				r.StatementsByKind = map[string]uint64{}
			}
			r.StatementsByKind[kind] += c
			r.Statements += c
		}
		r.SerializationFailures += q.SerializationFailures
		if q.P99Micros > r.WorstP99Micros {
			r.WorstP99Micros, r.WorstP99Node = q.P99Micros, n.NodeID
		}
		if q.OldestIdleTxnMillis > r.OldestIdleTxnMillis {
			r.OldestIdleTxnMillis = q.OldestIdleTxnMillis
		}
	}
	return r
}

// ClusterStatus is the /api/cluster document.
type ClusterStatus struct {
	Now    int64 `json:"now_unix_ms"`
	NodeID int   `json:"node_id"`
	// MaxOffsetMs is the clock skew the cluster tolerates (--max-offset);
	// measured offsets are judged against it.
	MaxOffsetMs int64 `json:"max_offset_ms"`
	// ConsoleVersion is the digest of the console page this node serves;
	// the page compares it with its own and offers a reload when they
	// differ (issue #146).
	ConsoleVersion string `json:"console_version,omitempty"`
	// Principal is per request: the caller's identity, not cluster state.
	Principal ClusterPrincipal `json:"principal"`
	Nodes     []ClusterNode    `json:"nodes"`
	Ranges    []ClusterRange   `json:"ranges"`
	// Rollup sums the live nodes' figures (issue #145).
	Rollup ClusterRollup `json:"rollup"`
	// Replication buckets the ranges by replication state and projects
	// what the loss of each failure domain would cost (issue #152).
	Replication ClusterReplication `json:"replication"`
	// Capacity is the per-store disk-fill forecast (issue #156), fitted
	// over the recorded free-space window and refreshed in the
	// background. A store with no meaningful trend is present with
	// filling=false and a reason, so the console can say why there is no
	// number instead of showing an empty cell.
	Capacity []Forecast              `json:"capacity"`
	Local    NodeStatus              `json:"local"`
	Storage  *storage.StorageMetrics `json:"storage,omitempty"`
	// Error carries a partial-data note (e.g. the meta scan failed during
	// startup or a partition); the rest of the document is still valid.
	Error string `json:"error,omitempty"`
}

func (n *Node) serveClusterAPI(w http.ResponseWriter, req *http.Request) {
	doc := n.clusterDoc(req)
	w.Header().Set("Content-Type", "application/json")
	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ")
	_ = enc.Encode(doc)
}

// clusterDoc assembles the /api/cluster document (also the cluster
// section of /api/overview).
func (n *Node) clusterDoc(req *http.Request) ClusterStatus {
	now := n.clock.Now().WallTime
	n.refreshSchema() // keep the table-name map fresh for range labels, without waiting on it
	forecasts := n.capacityForecasts()
	if forecasts == nil {
		forecasts = []Forecast{}
	}
	doc := ClusterStatus{
		Capacity:       forecasts,
		Now:            now / int64(time.Millisecond),
		NodeID:         int(n.ident.NodeID),
		ConsoleVersion: n.consoleVersion,
		Principal:      n.clusterPrincipal(req),
		Local:          n.statusSummary(),
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
			ShuttingDown: nd.ShuttingDown,
			LeaderQPS:    nd.LeaderQPS,
			LeaderCount:  nd.LeaderCount,
			ReplicaBytes: nd.ReplicaBytes,
			Machine:      nd.Machine,
			Latency:      nd.Latency,
			SQL:          nd.SQL,
			HotRanges:    nd.HotRanges,
			BigRanges:    nd.BigRanges,
		})
		if nd.NodeID == n.ident.NodeID {
			last := &doc.Nodes[len(doc.Nodes)-1]
			if n.pinger != nil {
				last.Latency = n.pinger.Snapshot()
			}
			if n.sqlServer() != nil {
				last.SQL = n.sqlServer().Activity().Summary()
			}
		}
	}
	descs, age, err := n.clusterRanges(req.Context())
	if err != nil {
		doc.Error = "cluster range listing unavailable: " + err.Error()
		if descs != nil {
			doc.Error += fmt.Sprintf("; showing the list from %s ago", age.Truncate(time.Second))
		}
	}
	{
		for _, d := range descs {
			cr := ClusterRange{
				RangeID:    int64(d.RangeID),
				StartKey:   n.prettyKey(d.StartKey),
				EndKey:     n.prettyKey(d.EndKey),
				Generation: d.Generation,
				Table:      n.tableNameOf(d.StartKey),
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
	doc.Rollup = rollup(doc.Nodes, doc.Ranges)
	doc.Replication = n.replicationStatus(descs, doc.Nodes)
	return doc
}

// clusterPrincipal describes the request's authenticated caller for the
// dashboard (see ClusterPrincipal).
func (n *Node) clusterPrincipal(req *http.Request) ClusterPrincipal {
	if n.tlsCfgs == nil {
		return ClusterPrincipal{Secure: false, Admin: true}
	}
	p := principalFrom(req)
	cp := ClusterPrincipal{
		Secure: true,
		User:   p.User,
		Via:    p.Via,
		Admin:  n.isAdminPrincipal(req.Context(), p.User),
	}
	if p.Via == "session" {
		if exp := sessionExpiry(req); !exp.IsZero() {
			cp.SessionExpiresAt = exp.UnixMilli()
		}
	}
	return cp
}

// rangeListTimeout bounds the /meta scan behind the dashboard's range
// list. The scan is routed like any read, so a node cut off from the
// meta range's leader would otherwise retry until the client gave up —
// and a partitioned node's dashboard is exactly when an operator looks.
const rangeListTimeout = 2 * time.Second

type rangeListCache struct {
	mu    sync.Mutex
	at    time.Time
	descs []kvpb.RangeDescriptor
}

// clusterRanges returns the cluster's range descriptors for the
// observability endpoints: the /meta scan bounded by rangeListTimeout,
// falling back to the last list this node fetched (with its age) and the
// error when the scan fails, so the picture stays up while it is stale
// rather than going blank.
func (n *Node) clusterRanges(ctx context.Context) ([]kvpb.RangeDescriptor, time.Duration, error) {
	ctx, cancel := context.WithTimeout(ctx, rangeListTimeout)
	defer cancel()
	descs, err := n.listRanges(ctx)
	n.rangeList.mu.Lock()
	defer n.rangeList.mu.Unlock()
	if err == nil {
		n.rangeList.descs, n.rangeList.at = descs, time.Now()
		return descs, 0, nil
	}
	if n.rangeList.descs == nil {
		return nil, 0, err
	}
	return n.rangeList.descs, time.Since(n.rangeList.at), err
}
