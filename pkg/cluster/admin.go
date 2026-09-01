package cluster

import (
	"encoding/json"

	"github.com/sthorne/datax/pkg/base"
	"github.com/sthorne/datax/pkg/kvpb"
)

// AdminRequest is the JSON body of admin RPCs (datax debug ...).
type AdminRequest struct {
	// Op: "split", "ranges", "nodes", "rebalance", "transfer-lease",
	// "decommission", "merge".
	Op string `json:"op"`
	// Key for split (raw key bytes).
	Key []byte `json:"key,omitempty"`
	// RangeID and target/source nodes for rebalance.
	RangeID  base.RangeID `json:"range_id,omitempty"`
	ToNode   base.NodeID  `json:"to_node,omitempty"`
	FromNode base.NodeID  `json:"from_node,omitempty"`
	// NodeID and Cancel for decommission.
	NodeID base.NodeID `json:"node_id,omitempty"`
	Cancel bool        `json:"cancel,omitempty"`
	// Backup/restore: filesystem paths ON THE SERVING NODE. Backup writes
	// to Path (BasePath names a prior backup for an incremental); restore
	// applies the chain in Paths order (full first).
	Path           string   `json:"path,omitempty"`
	BasePath       string   `json:"base_path,omitempty"`
	Paths          []string `json:"paths,omitempty"`
	AllowPlaintext bool     `json:"allow_plaintext,omitempty"`
	// CheckID identifies a consistency probe for collect-checksum.
	CheckID string `json:"check_id,omitempty"`
	// Version is the target for upgrade-cluster (0 = the serving node's
	// binary version).
	Version int `json:"version,omitempty"`
}

// AdminResponse is the JSON reply.
type AdminResponse struct {
	Error  string                 `json:"error,omitempty"`
	Ranges []kvpb.RangeDescriptor `json:"ranges,omitempty"`
	Nodes  []kvpb.NodeDescriptor  `json:"nodes,omitempty"`
	Left   *kvpb.RangeDescriptor  `json:"left,omitempty"`
	Right  *kvpb.RangeDescriptor  `json:"right,omitempty"`
	// Decommission progress.
	Draining          bool `json:"draining,omitempty"`
	RemainingReplicas int  `json:"remaining_replicas,omitempty"`
	// Backup/restore summary.
	Backup *BackupSummary `json:"backup,omitempty"`
	// Checksum answers collect-checksum: this node's digest for the probe,
	// and the applied index it was computed at.
	Checksum     []byte `json:"checksum,omitempty"`
	AppliedIndex uint64 `json:"applied_index,omitempty"`
	// ClusterVersion reports the finalized cluster version (nodes,
	// upgrade-cluster).
	ClusterVersion int `json:"cluster_version,omitempty"`
	// Status answers node-status: the serving node's /status document
	// (server.NodeStatus), optionally filtered to one range. Raw JSON so
	// this package does not depend on the server package's types.
	Status json.RawMessage `json:"status,omitempty"`
}

// BackupTableSummary reports one table of a backup or restore.
type BackupTableSummary struct {
	ID      uint64 `json:"id"`
	Name    string `json:"name"`
	Records int64  `json:"records"`
	Bytes   int64  `json:"bytes"`
	// SHA256 is the hex digest over the table's LIVE (key, value) records
	// in key order — comparable between a backup and a fresh export of the
	// restored cluster.
	SHA256 string `json:"sha256"`
}

// BackupSummary is the admin RPC's report for a backup or restore.
type BackupSummary struct {
	Path        string               `json:"path,omitempty"`
	ClusterID   string               `json:"cluster_id,omitempty"`
	EndTSNanos  int64                `json:"end_ts_nanos,omitempty"`
	Incremental bool                 `json:"incremental,omitempty"`
	Tables      []BackupTableSummary `json:"tables,omitempty"`
	Users       int                  `json:"users,omitempty"`
}
