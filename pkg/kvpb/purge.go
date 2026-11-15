package kvpb

import "github.com/sthorne/datax/pkg/base"

// PurgeReport is the result of a purge of one key span (issue #295): what
// the forced garbage collection removed and, replica by replica, what a
// scan of the engine below the MVCC layer found afterwards. It travels as
// JSON on the admin channel and is recorded whole as an audit event.
type PurgeReport struct {
	StartKey []byte `json:"start_key"`
	EndKey   []byte `json:"end_key"`
	// PurgeTSNanos is the wall time of the purge timestamp: every range of
	// the span collected garbage at this threshold, and reads at or below
	// it are refused from then on.
	PurgeTSNanos int64 `json:"purge_ts_nanos"`
	// Ranges and VersionsRemoved count the ranges visited and the MVCC
	// versions (tombstones included) the forced collection deleted.
	Ranges          int   `json:"ranges"`
	VersionsRemoved int64 `json:"versions_removed"`
	// Replicas holds one entry per replica of every range of the span.
	Replicas []PurgeReplicaReport `json:"replicas"`
	// Complete is the attestation: every replica answered, and none found
	// a superseded version, a tombstone or an unresolved intent in the
	// span. When false, Incomplete says why, one line per shortfall.
	Complete   bool     `json:"complete"`
	Incomplete []string `json:"incomplete,omitempty"`
	// NotCovered names what a purge does not reach, so the report cannot
	// be read as saying more than it does.
	NotCovered []string `json:"not_covered"`
}

// PurgeReplicaReport is one replica's part of a PurgeReport.
type PurgeReplicaReport struct {
	RangeID base.RangeID `json:"range_id"`
	NodeID  base.NodeID  `json:"node_id"`
	// Error is set when the replica did not answer or could not finish;
	// the counts below then attest nothing.
	Error string `json:"error,omitempty"`
	// LiveKeys is how many keys of the span still hold a value (the rows
	// that were not deleted). Garbage is how many versions lie beneath a
	// newer one, Tombstones how many deletion markers remain and Intents
	// how many keys hold an unresolved write: all three are zero on a
	// replica the purge has finished with.
	LiveKeys   int64 `json:"live_keys"`
	Garbage    int64 `json:"garbage_versions"`
	Tombstones int64 `json:"tombstones"`
	Intents    int64 `json:"intents"`
	// Compacted says the replica's engines rewrote the span's files after
	// the collection, so the removed bytes are no longer in a live file.
	Compacted bool `json:"compacted"`
}
