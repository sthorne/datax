package server

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"time"

	"github.com/sthorne/datax/pkg/base"
	"github.com/sthorne/datax/pkg/cluster"
	"github.com/sthorne/datax/pkg/kvpb"
	"github.com/sthorne/datax/pkg/pgwire"
	"github.com/sthorne/datax/pkg/storage"
	"github.com/sthorne/datax/pkg/util/events"
	"github.com/sthorne/datax/pkg/version"
)

// /api/node?id=N — the node detail page (issue #86): one node's identity,
// machine sample, storage, ranges, SQL activity, network row and recent
// events. The serving node answers for itself directly; for another
// node it asks that node over the internode admin RPC ("node-detail"),
// so any node's dashboard can inspect any node. The fan-out is
// admin-gated like /api/range; non-admins get the serving node only.

// NodeDetail is the /api/node document.
type NodeDetail struct {
	NodeID        int    `json:"node_id"`
	Address       string `json:"address,omitempty"`
	SQLAddr       string `json:"sql_address,omitempty"`
	Locality      string `json:"locality,omitempty"`
	Live          bool   `json:"live"`
	Draining      bool   `json:"draining,omitempty"`
	ShuttingDown  bool   `json:"shutting_down,omitempty"`
	HeartbeatAgo  int64  `json:"heartbeat_ago_ms"`
	BinaryVersion int    `json:"binary_version,omitempty"`
	// ClusterVersion is the version this node has mirrored locally.
	ClusterVersion int    `json:"cluster_version,omitempty"`
	Release        string `json:"release,omitempty"`
	UptimeSeconds  int64  `json:"uptime_seconds,omitempty"`
	// Error notes why the detail is missing (unreachable, old binary).
	Error string `json:"error,omitempty"`

	// Settings are the node's operator-facing knobs.
	Settings map[string]string `json:"settings,omitempty"`
	Status   *NodeStatus       `json:"status,omitempty"`

	Storage *storage.StorageMetrics `json:"storage,omitempty"`
	// EngineMode is "split" when the raft log has its own engine and the
	// state engine runs without a WAL (issue #105), "single" otherwise;
	// RaftStorage is the raft engine's snapshot when split.
	EngineMode      string                      `json:"engine_mode,omitempty"`
	RaftStorage     *storage.StorageMetrics     `json:"raft_storage,omitempty"`
	DebtGated       bool                        `json:"debt_gated,omitempty"`
	DebtGateEntries int64                       `json:"debt_gate_entries,omitempty"`
	Overloaded      bool                        `json:"overloaded,omitempty"`
	OverloadReason  string                      `json:"overload_reason,omitempty"`
	Encrypted       bool                        `json:"encrypted,omitempty"`
	Reencryption    *cluster.ReencryptionStatus `json:"reencryption,omitempty"`

	Latency  []kvpb.PeerLatency `json:"latency,omitempty"`
	SQL      *kvpb.SQLSummary   `json:"sql,omitempty"`
	Activity *ActivityStatus    `json:"activity,omitempty"`
	Events   []events.Event     `json:"events"`
}

// localNodeDetail assembles this node's document. Statement text and
// audit records are admin-only material (the same rules as
// /api/activity and /api/events).
func (n *Node) localNodeDetail(ctx context.Context, admin bool) NodeDetail {
	st := n.statusSummary()
	d := NodeDetail{
		NodeID:         int(n.ident.NodeID),
		Address:        n.addr,
		SQLAddr:        n.SQLAddr(),
		Locality:       n.cfg.Locality.String(),
		Live:           true,
		Draining:       n.draining.Load(),
		ShuttingDown:   n.shuttingDown.Load(),
		BinaryVersion:  int(n.binaryVersion()),
		ClusterVersion: int(n.readClusterVersion(ctx)),
		Release:        version.Release,
		Status:         &st,
		Events:         []events.Event{},
	}
	if st.Machine != nil {
		d.UptimeSeconds = st.Machine.ProcessUp
	}
	d.Settings = map[string]string{
		"data directory":           orDefault(n.cfg.Dir, "in-memory"),
		"storage profile":          orDefault(string(n.cfg.StorageProfile), "balanced"),
		"max clock offset":         n.clock.MaxOffset().String(),
		"metrics record interval":  offOr(n.metricsRecordInterval()),
		"slow statement threshold": offOr(n.cfg.SlowStatementThreshold),
		"consistency interval":     offOr(n.cfg.ConsistencyInterval),
		"secure":                   strconv.FormatBool(n.tlsCfgs != nil),
	}
	if n.engine != nil {
		sm := n.engine.StorageMetrics()
		d.Storage = &sm
		d.EngineMode = n.engineMode()
		if n.raftEngine != nil {
			rm := n.raftEngine.StorageMetrics()
			d.RaftStorage = &rm
		}
		d.DebtGated = n.engine.DebtGated()
		d.DebtGateEntries = n.engine.DebtGateEntries()
		d.Overloaded, d.OverloadReason = n.engine.Overloaded()
		d.Encrypted = n.engine.Encrypted()
		if d.Encrypted {
			d.Reencryption = n.reencryptionStatus()
		}
	}
	if n.pinger != nil {
		d.Latency = n.pinger.Snapshot()
	}
	if n.pgServer != nil {
		d.SQL = n.pgServer.Activity().Summary()
		if admin {
			a := n.activityStatus()
			d.Activity = &a
		}
	}
	if n.events != nil {
		d.Events = n.events.Recent(0, 50, admin)
		if d.Events == nil {
			d.Events = []events.Event{}
		}
	}
	return d
}

func orDefault(v, def string) string {
	if v == "" {
		return def
	}
	return v
}

func offOr(d time.Duration) string {
	if d <= 0 {
		return "off"
	}
	return d.String()
}

func (n *Node) serveNodeAPI(w http.ResponseWriter, req *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ")
	p := n.clusterPrincipal(req)
	idStr := req.URL.Query().Get("id")
	id := n.ident.NodeID
	if idStr != "" {
		v, err := strconv.Atoi(idStr)
		if err != nil || v <= 0 {
			w.WriteHeader(http.StatusBadRequest)
			_ = enc.Encode(map[string]string{"error": "node id required: /api/node?id=N"})
			return
		}
		id = base.NodeID(v)
	}
	if id == n.ident.NodeID {
		_ = enc.Encode(n.localNodeDetail(req.Context(), p.Admin))
		return
	}
	if !p.Admin {
		w.WriteHeader(http.StatusForbidden)
		_ = enc.Encode(map[string]string{"error": "the admin role is required to inspect another node"})
		return
	}
	doc := NodeDetail{NodeID: int(id), Events: []events.Event{}}
	nd, ok := n.registry.Get(id)
	if !ok {
		w.WriteHeader(http.StatusNotFound)
		_ = enc.Encode(map[string]string{"error": fmt.Sprintf("n%d is not a member of this cluster", id)})
		return
	}
	now := n.clock.Now().WallTime
	doc.Address, doc.Locality, doc.Draining, doc.ShuttingDown = nd.Address, nd.Locality.String(), nd.Draining, nd.ShuttingDown
	doc.Live = now-nd.LivenessTime <= n.livenessGrace().Nanoseconds()
	doc.HeartbeatAgo = (now - nd.LivenessTime) / int64(time.Millisecond)
	doc.BinaryVersion = nd.BinaryVersion
	doc.Latency, doc.SQL = nd.Latency, nd.SQL
	if nd.Machine != nil {
		doc.UptimeSeconds = nd.Machine.UptimeSeconds
	}
	addr, err := n.registry.Resolve(id)
	if err != nil {
		doc.Error = err.Error()
		_ = enc.Encode(doc)
		return
	}
	ctx, cancel := context.WithTimeout(req.Context(), 3*time.Second)
	defer cancel()
	var resp cluster.AdminResponse
	if err := n.trans.Call(ctx, addr, "admin", cluster.AdminRequest{Op: "node-detail"}, &resp); err != nil {
		doc.Error = err.Error()
	} else if resp.Error != "" {
		doc.Error = resp.Error
	} else {
		var remote NodeDetail
		if err := json.Unmarshal(resp.Status, &remote); err != nil {
			doc.Error = "undecodable detail: " + err.Error()
		} else {
			remote.Live, remote.HeartbeatAgo, remote.Draining = doc.Live, doc.HeartbeatAgo, doc.Draining || remote.Draining
			remote.ShuttingDown = doc.ShuttingDown || remote.ShuttingDown
			doc = remote
		}
	}
	_ = enc.Encode(doc)
}

// activityStatus builds the /api/activity document.
func (n *Node) activityStatus() ActivityStatus {
	doc := ActivityStatus{NodeID: int(n.ident.NodeID), Connections: []pgwire.ConnectionInfo{}, Active: []pgwire.ActiveStatement{}, Slow: []pgwire.SlowStatement{}}
	if n.pgServer != nil {
		act := n.pgServer.Activity()
		doc.Summary = act.Summary()
		doc.Connections = act.Connections()
		doc.Active = act.Active()
		doc.Slow = act.Slow()
		doc.SlowThresholdMillis = act.SlowThreshold().Milliseconds()
	}
	return doc
}
