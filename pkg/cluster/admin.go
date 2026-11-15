package cluster

import (
	"encoding/json"

	"github.com/sthorne/datax/pkg/base"
	"github.com/sthorne/datax/pkg/kvpb"
	"github.com/sthorne/datax/pkg/sql/notify"
	"github.com/sthorne/datax/pkg/storage/enc"
)

// AdminRequest is the JSON body of admin RPCs (datax debug ...).
type AdminRequest struct {
	// Op: "split", "ranges", "nodes", "rebalance", "transfer-lease",
	// "decommission", "merge", "unsplit", "purge".
	Op string `json:"op"`
	// Key for split and unsplit (raw key bytes). Key and EndKey are the
	// span of a purge; Table names it instead, as [database.]table.
	Key    []byte `json:"key,omitempty"`
	EndKey []byte `json:"end_key,omitempty"`
	Table  string `json:"table,omitempty"`
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
	// Backup keys (issue #291). BackupKey seals the backup being written
	// and BaseBackupKey opens a sealed base; BackupKeys opens a restore's
	// chain — one key for all of Paths, or one per path in the same
	// order, an empty entry for a plaintext backup. Key material in
	// transit: accepted over mutual TLS, or without TLS only from the
	// node's own machine; never logged, audited or echoed.
	BackupKey     []byte   `json:"backup_key,omitempty"`
	BaseBackupKey []byte   `json:"base_backup_key,omitempty"`
	BackupKeys    [][]byte `json:"backup_keys,omitempty"`
	// IncludeMetrics backs up the datax_metrics system table too (bulky
	// and regenerable, so excluded by default).
	IncludeMetrics bool `json:"include_metrics,omitempty"`
	// A request carrying BackupKey travels as "backup-sealed", and a
	// restore of everything carrying BackupKeys as "restore-sealed"
	// (issue #313), for the reason given next: a node from before sealed
	// backups would otherwise ignore the keys. "backup" and "restore"
	// still take them, from commands that predate the names.
	//
	// What "backup-history" and "restore-select" add to backup and
	// restore (issues #301, #302). They travel under op names of their
	// own because a node from before them would read "backup" or
	// "restore", ignore the fields it does not know, and do the old thing
	// — a plain incremental, or a restore of everything to the chain's
	// end — and report success.
	//
	// AsOf is the moment to restore to, as BackupSummary.EndTS writes
	// one; empty means the end of the chain. Tables names the tables to
	// restore ("db.table", or "table" in the default database) and Into,
	// parallel to it, the names to restore them under (an empty entry
	// keeps the name). Database restores every table of one database into
	// a new database, IntoDatabase its name there (empty keeps the name).
	// DropForeignKeys restores a table without its foreign keys to tables
	// that are not being restored with it, rather than refusing.
	AsOf            string   `json:"as_of,omitempty"`
	Tables          []string `json:"tables,omitempty"`
	Into            []string `json:"into,omitempty"`
	Database        string   `json:"database,omitempty"`
	IntoDatabase    string   `json:"into_database,omitempty"`
	DropForeignKeys bool     `json:"drop_foreign_keys,omitempty"`
	// CheckID identifies a consistency probe for collect-checksum.
	CheckID string `json:"check_id,omitempty"`
	// Index is the applied index wait-applied blocks for (with RangeID).
	Index uint64 `json:"index,omitempty"`
	// Version is the target for upgrade-cluster (0 = the serving node's
	// binary version).
	Version int `json:"version,omitempty"`
	// PID, Secret and Terminate route a query cancel (cancel-query) to
	// the node whose SQL connection the process ID names. Secret is the
	// wire client's authority and authorizes a plain cancel of that one
	// connection; zero means the caller holds none, and then — as for
	// Terminate always — the admin role authorizes it instead, checked
	// on the node that acts (issue #211).
	PID       int32  `json:"pid,omitempty"`
	Secret    uint32 `json:"secret,omitempty"`
	Terminate bool   `json:"terminate,omitempty"`
	// Store-key rotation (rotate-store-key): the node's current store key
	// (verified against the on-disk registry) and its replacement. Carried
	// over mutual TLS in secure mode; never logged.
	// A node whose key is under a root (--enc-root) makes the new key
	// itself, and for it both fields must be empty.
	OldStoreKey []byte `json:"old_store_key,omitempty"`
	NewStoreKey []byte `json:"new_store_key,omitempty"`
	// Root names the root rewrap-store-key wraps under: the 1-based
	// position among the node's own --enc-root values, 0 for the root in
	// use. A position, not a spec — an exec root is a command line, and
	// no request may name a command for a node to run.
	Root int `json:"root,omitempty"`
	// Share (unseal) is one share of a root key, as its holder keeps it.
	// It is a secret in transit: never logged, audited or echoed.
	Share string `json:"share,omitempty"`
	// Peer (peer-unseal) is a sealed node's request for the root key.
	Peer *PeerUnsealRequest `json:"peer,omitempty"`
	// Enable (peer-unseal-setting) turns peer unseal on or off; nil
	// only reports it.
	Enable *bool `json:"enable,omitempty"`
	// Notify (notify) is a run of committed NOTIFYs from another node,
	// for this node's listening sessions (issue #297).
	Notify *notify.Batch `json:"notify,omitempty"`
	// ClusterKey carries the arguments of the cluster-key-* ops (issue
	// #310).
	ClusterKey *ClusterKeyRequest `json:"cluster_key,omitempty"`
}

// ClusterKeyRequest is what the cluster-key-* ops carry (issue #310;
// pkg/server/clusterkey.go).
type ClusterKeyRequest struct {
	// ClusterID and NodeID say who asks (cluster-key-handover) or whom
	// an offer is for (cluster-key-offer).
	ClusterID string      `json:"cluster_id,omitempty"`
	NodeID    base.NodeID `json:"node_id,omitempty"`
	// Exchange is the asker's public half of a key exchange
	// (cluster-key-handover, cluster-key-export).
	Exchange []byte `json:"exchange,omitempty"`
	// Nonce names the offer an install answers; Giver and Sealed are the
	// giver's public half and the keys sealed for that offer.
	Nonce  string `json:"nonce,omitempty"`
	Giver  []byte `json:"giver,omitempty"`
	Sealed []byte `json:"sealed,omitempty"`
	// Keys (cluster-key-import) are another cluster's keys, unwrapped by
	// the CLI from an export: key material in transit, accepted over
	// mutual TLS only, never logged, audited or echoed.
	Keys []enc.TransportKey `json:"keys,omitempty"`
}

// ClusterKeyAnswer is what the cluster-key-* ops answer.
type ClusterKeyAnswer struct {
	Status *ClusterKeyStatus `json:"status,omitempty"`
	// Exchange and Nonce answer an offer; Exchange and Sealed a
	// handover or an export.
	Exchange []byte `json:"exchange,omitempty"`
	Nonce    string `json:"nonce,omitempty"`
	Sealed   []byte `json:"sealed,omitempty"`
	// Notes say what an op did, for the operator.
	Notes []string `json:"notes,omitempty"`
}

// ClusterKeyStatus is the cluster key as the cluster records it, and who
// holds it. Nothing in it is secret.
type ClusterKeyStatus struct {
	ClusterID string `json:"cluster_id"`
	Enabled   bool   `json:"enabled"`
	// ID and Version name the current cluster key.
	ID      string `json:"id,omitempty"`
	Version int    `json:"version,omitempty"`
	// Previous are versions a rotation has not finished leaving.
	Previous []int `json:"previous,omitempty"`
	// Imported are other clusters' keys held for their backups, as
	// id/vN.
	Imported []string `json:"imported,omitempty"`
	// Pending describes an enabling, rotation or import that began and
	// has not finished: running the same command again finishes it.
	Pending string `json:"pending,omitempty"`
	// Nodes is every member and what it reports.
	Nodes []ClusterKeyNode `json:"nodes,omitempty"`
}

// ClusterKeyNode is one member as the cluster key sees it.
type ClusterKeyNode struct {
	NodeID          base.NodeID `json:"node_id"`
	Live            bool        `json:"live"`
	Decommissioning bool        `json:"decommissioning,omitempty"`
	// Reported: the node has written its holder record (it does once the
	// cluster is at v22).
	Reported bool `json:"reported"`
	// Root is the kind of root its store key is under ("" for a key file
	// or a plaintext store).
	Root string `json:"root,omitempty"`
	// Holds lists the cluster keys it holds, as id/vN; HoldsCurrent says
	// whether the current one is among them.
	Holds        []string `json:"holds,omitempty"`
	HoldsCurrent bool     `json:"holds_current"`
}

// PeerUnsealRequest is what a sealed node sends a running peer: who it
// is, which root key it waits for, and the public half of the key
// exchange the answer is sealed under.
type PeerUnsealRequest struct {
	ClusterID string      `json:"cluster_id"`
	NodeID    base.NodeID `json:"node_id"`
	KeyID     string      `json:"key_id"`
	Exchange  []byte      `json:"exchange"`
}

// PeerUnsealAnswer carries the root key, sealed for the one exchange.
type PeerUnsealAnswer struct {
	Exchange []byte `json:"exchange"`
	Sealed   []byte `json:"sealed"`
}

// SealStatus (seal-status, unseal) says where a node stands with a root
// key that is rebuilt from operators' shares.
type SealStatus struct {
	// Sealed: the node's store is closed, waiting for shares.
	Sealed bool `json:"sealed"`
	// Root is the kind of root the store key is under ("" for a key
	// file or a plaintext store) and KeyID that root's key.
	Root  string `json:"root,omitempty"`
	KeyID string `json:"key_id,omitempty"`
	// Have and Need count the distinct shares submitted and the
	// threshold (0 until a first share has said what it is); Indexes are
	// the shares in, which are not secret.
	Have    int   `json:"have"`
	Need    int   `json:"need"`
	Indexes []int `json:"indexes,omitempty"`
	// Staged describes a second root key being assembled on a running
	// node, to move the store key under (rotating the root key): its
	// progress, and once complete its position for rewrap --to-root.
	Staged *SealStatus `json:"staged,omitempty"`
	// Position is a staged root's place among the node's roots.
	Position int `json:"position,omitempty"`
	// PeerUnseal reports the cluster's setting as this node holds it:
	// "on", "off", or "" where it does not apply.
	PeerUnseal string `json:"peer_unseal,omitempty"`
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
	Seal   *SealStatus            `json:"seal,omitempty"`
	Peer   *PeerUnsealAnswer      `json:"peer,omitempty"`
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
	// its replicated statistics at the same point (empty from a node that
	// predates them, issue #316), and the applied index it was computed at.
	Checksum      []byte `json:"checksum,omitempty"`
	ChecksumStats string `json:"checksum_stats,omitempty"`
	AppliedIndex  uint64 `json:"applied_index,omitempty"`
	// ClusterVersion reports the finalized cluster version (nodes,
	// upgrade-cluster).
	ClusterVersion int `json:"cluster_version,omitempty"`
	// Reencryption answers reencrypt / reencrypt-status.
	Reencryption *ReencryptionStatus `json:"reencryption,omitempty"`
	// Purge answers purge: the per-replica report. PurgeRange and
	// PurgeReplica answer the steps the purging node asks of the others
	// (purge-range of a range's leader, purge-replica of each replica).
	Purge        *kvpb.PurgeReport        `json:"purge,omitempty"`
	PurgeRange   *PurgeRangeAnswer        `json:"purge_range,omitempty"`
	PurgeReplica *kvpb.PurgeReplicaReport `json:"purge_replica,omitempty"`
	// Status answers node-status: the serving node's /status document
	// (server.NodeStatus), optionally filtered to one range. Raw JSON so
	// this package does not depend on the server package's types.
	Status json.RawMessage `json:"status,omitempty"`
	// ClusterKey answers the cluster-key-* ops (issue #310).
	ClusterKey *ClusterKeyAnswer `json:"cluster_key,omitempty"`
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
	// Versions is the number of versions the table's data file holds, in
	// a backup with revision history.
	Versions int64 `json:"versions,omitempty"`
	// From is the table's name and id in the backup, where a restore
	// brought it back under another (a table restore).
	From   string `json:"from,omitempty"`
	FromID uint64 `json:"from_id,omitempty"`
}

// BackupSummary is the admin RPC's report for a backup or restore.
type BackupSummary struct {
	Path        string `json:"path,omitempty"`
	ClusterID   string `json:"cluster_id,omitempty"`
	EndTSNanos  int64  `json:"end_ts_nanos,omitempty"`
	Incremental bool   `json:"incremental,omitempty"`
	// Sealed: the backup's files were written sealed under a backup key.
	Sealed bool                 `json:"sealed,omitempty"`
	Tables []BackupTableSummary `json:"tables,omitempty"`
	Users  int                  `json:"users,omitempty"`
	// EndTS and BaseTS are the backup's timestamps as restore's --as-of
	// takes them: Unix nanoseconds, with ",<logical>" appended when the
	// logical part is not zero. For a restore, EndTS is the moment the
	// restored data is as of.
	EndTS  string `json:"end_ts,omitempty"`
	BaseTS string `json:"base_ts,omitempty"`
	// RevisionHistory: the backup holds every version written since its
	// base, so a chain through it restores to any moment in between.
	RevisionHistory bool `json:"revision_history,omitempty"`
	// Notes are what a table restore left out or changed on the way in:
	// foreign keys dropped, grants to roles the cluster does not have.
	Notes []string `json:"notes,omitempty"`
}

// PurgeRangeAnswer is a leader's answer to purge-range: the range as it
// stands, the threshold it collected at, how many versions that deleted,
// and the applied index a replica must reach to hold the collection.
type PurgeRangeAnswer struct {
	Desc           kvpb.RangeDescriptor `json:"desc"`
	ThresholdNanos int64                `json:"threshold_nanos"`
	Removed        int64                `json:"removed"`
	AppliedIndex   uint64               `json:"applied_index"`
}
