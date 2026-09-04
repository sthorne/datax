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
	// IncludeMetrics backs up the datax_metrics system table too (bulky
	// and regenerable, so excluded by default).
	IncludeMetrics bool `json:"include_metrics,omitempty"`
	// CheckID identifies a consistency probe for collect-checksum.
	CheckID string `json:"check_id,omitempty"`
	// Index is the applied index wait-applied blocks for (with RangeID).
	Index uint64 `json:"index,omitempty"`
	// Version is the target for upgrade-cluster (0 = the serving node's
	// binary version).
	Version int `json:"version,omitempty"`
	// Store-key rotation (rotate-store-key): the node's current store key
	// (verified against the on-disk registry) and its replacement. Carried
	// over mutual TLS in secure mode; never logged.
	OldStoreKey []byte `json:"old_store_key,omitempty"`
	NewStoreKey []byte `json:"new_store_key,omitempty"`
}

// ReencryptionStatus reports the background re-encryption pass on one
// node: whether a pass is running, live sstable bytes/files still under
// retired data keys, and total bytes rewritten by passes so far.
// RemainingBytes == 0 with Active == false is the attestation that no
// live sstable remains under a retired key.
type ReencryptionStatus struct {
	Active         bool  `json:"active"`
	RemainingBytes int64 `json:"remaining_bytes"`
	RemainingFiles int   `json:"remaining_files"`
	RewrittenBytes int64 `json:"rewritten_bytes_total"`
	// SweepError is set when the stale-file sweep behind RemainingBytes
	// failed; the counts are then the last good reading (or zero if there
	// never was one) and attest nothing.
	SweepError string `json:"sweep_error,omitempty"`
}

// AdminResponse is the JSON reply.
type AdminResponse struct {
	Error  string                 `json:"error,omitempty"`
	Ranges []kvpb.RangeDescriptor `json:"ranges,omitempty"`
	// TableNames (ranges, split, merge) maps table IDs to names, so a
	// client can print spans as /table/orders/... (keys.SetTableNamer).
	TableNames map[uint64]string     `json:"table_names,omitempty"`
	Nodes      []kvpb.NodeDescriptor `json:"nodes,omitempty"`
	Left       *kvpb.RangeDescriptor `json:"left,omitempty"`
	Right      *kvpb.RangeDescriptor `json:"right,omitempty"`
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
	// Reencryption answers reencrypt / reencrypt-status.
	Reencryption *ReencryptionStatus `json:"reencryption,omitempty"`
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
