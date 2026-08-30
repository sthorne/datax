// Package kvpb defines datax's KV API: the batch request/response types
// exchanged between the KV client (DistSender/TxnCoordinator) and the range
// replicas, plus the shared cluster data types (descriptors, transactions).
// Hot-path serialization (the Batch RPC body and the raft command payload)
// is protobuf via the converters in proto.go; cold paths (join, admin,
// descriptors at rest, registry rows) stay JSON for debuggability.
package kvpb

import (
	"fmt"

	"github.com/google/uuid"
	"github.com/sthorne/datax/pkg/base"
	"github.com/sthorne/datax/pkg/keys"
	"github.com/sthorne/datax/pkg/storage/enginepb"
	"github.com/sthorne/datax/pkg/util/hlc"
)

// ReplicaDescriptor identifies one replica of a range.
type ReplicaDescriptor struct {
	NodeID    base.NodeID    `json:"node_id"`
	StoreID   base.StoreID   `json:"store_id"`
	ReplicaID base.ReplicaID `json:"replica_id"`
}

// RangeDescriptor describes a range: its bounds and replica set. Stored in
// /meta records and in each replica's local descriptor key.
type RangeDescriptor struct {
	RangeID       base.RangeID        `json:"range_id"`
	StartKey      keys.Key            `json:"start_key"`
	EndKey        keys.Key            `json:"end_key"`
	Replicas      []ReplicaDescriptor `json:"replicas"`
	NextReplicaID base.ReplicaID      `json:"next_replica_id"`
	// Generation increments on every split or replica change; used to
	// discard stale cached descriptors.
	Generation int64 `json:"generation"`
}

// ContainsKey returns whether the range's span covers the (global) key.
func (d *RangeDescriptor) ContainsKey(k keys.Key) bool {
	return d.StartKey.Compare(k) <= 0 && k.Compare(d.EndKey) < 0
}

// GetReplica returns the descriptor of the replica on the given node.
func (d *RangeDescriptor) GetReplica(nodeID base.NodeID) (ReplicaDescriptor, bool) {
	for _, r := range d.Replicas {
		if r.NodeID == nodeID {
			return r, true
		}
	}
	return ReplicaDescriptor{}, false
}

func (d *RangeDescriptor) String() string {
	return fmt.Sprintf("%s [%s, %s) gen=%d replicas=%v", d.RangeID, d.StartKey, d.EndKey, d.Generation, d.Replicas)
}

// NodeDescriptor describes a node in the registry (range 1). The liveness
// timestamp is updated by periodic heartbeats.
type NodeDescriptor struct {
	NodeID   base.NodeID   `json:"node_id"`
	Address  string        `json:"address"`
	Locality base.Locality `json:"locality"`
	// LivenessTime is the HLC wall time of the node's last heartbeat.
	LivenessTime int64 `json:"liveness_time"`
	// Draining marks a node being decommissioned: the allocator moves its
	// replicas away and never places new ones on it. The node itself
	// adopts and re-asserts the flag through its heartbeats.
	Draining bool `json:"draining,omitempty"`

	// Load aggregates, refreshed on every heartbeat so the allocator (the
	// range-1 leader) can weigh load it cannot observe locally. QPS is
	// leader-local and resets on leadership changes, so these are
	// best-effort signals gated by thresholds, never exact accounting.
	//
	// LeaderQPS sums the measured request rate over this node's MATURE
	// leaseholder replicas (immature trackers — mid-window after a
	// transfer — contribute nothing rather than a misleading zero-ish
	// partial rate).
	LeaderQPS float64 `json:"leader_qps,omitempty"`
	// LeaderCount is how many ranges this node currently leads.
	LeaderCount int `json:"leader_count,omitempty"`
	// ReplicaBytes sums SizeBytes over all replicas this node hosts.
	ReplicaBytes int64 `json:"replica_bytes,omitempty"`
	// HotRanges are the node's heaviest mature leaseholders by QPS, and
	// BigRanges its largest replicas by bytes (top-K each) — the concrete
	// candidates a lease-shedding or byte-rebalancing pass acts on.
	HotRanges []HotRange `json:"hot_ranges,omitempty"`
	BigRanges []HotRange `json:"big_ranges,omitempty"`
}

// HotRange is one entry of a node's advertised hot/big range lists.
type HotRange struct {
	RangeID base.RangeID `json:"range_id"`
	QPS     float64      `json:"qps,omitempty"`
	Bytes   int64        `json:"bytes,omitempty"`
}

// Transaction is the full transaction state, as tracked by the coordinator
// and stored in the transaction record.
type Transaction struct {
	enginepb.TxnMeta
	Name   string             `json:"name,omitempty"`
	Status enginepb.TxnStatus `json:"status"`
	// ReadTimestamp is the snapshot all reads observe. In datax's retry-only
	// design it must equal TxnMeta.WriteTimestamp for a commit to succeed.
	ReadTimestamp hlc.Timestamp `json:"read_ts"`
	// LastHeartbeat is used to detect abandoned transactions.
	LastHeartbeat hlc.Timestamp `json:"last_heartbeat"`
	// IntentKeys is the committed transaction's write set, recorded at
	// commit so GC resolves every intent before reclaiming the record.
	IntentKeys []keys.Key `json:"intent_keys,omitempty"`
	// WaitingFor advertises the transaction this one is currently blocked
	// on (uuid.Nil = not waiting), with WaitingForKey the blocker's anchor
	// key so its record can be found. Published by the coordinator while
	// it waits in a push loop; pushers walk these edges to detect
	// deadlock cycles. Advisory and possibly stale — used only to pick an
	// abort victim, never for correctness of data.
	WaitingFor    uuid.UUID `json:"waiting_for,omitempty"`
	WaitingForKey keys.Key  `json:"waiting_for_key,omitempty"`
	// InFlightKeys, on a STAGING record, names the writes pipelined with
	// the parallel commit: the transaction is implicitly committed iff all
	// of them are present at or below the record's timestamp.
	InFlightKeys []keys.Key `json:"in_flight_keys,omitempty"`
}

// NewTransaction creates a transaction starting at now.
func NewTransaction(name string, priority int32, now hlc.Timestamp) *Transaction {
	return &Transaction{
		TxnMeta: enginepb.TxnMeta{
			ID:             uuid.New(),
			Epoch:          0,
			WriteTimestamp: now,
			MinTimestamp:   now,
			Priority:       priority,
		},
		Name:          name,
		Status:        enginepb.PENDING,
		ReadTimestamp: now,
		LastHeartbeat: now,
	}
}

// Clone returns a deep-enough copy (slices shared where immutable).
func (t *Transaction) Clone() *Transaction {
	c := *t
	return &c
}

// Restart begins a new epoch at the given timestamp.
func (t *Transaction) Restart(now hlc.Timestamp) {
	t.Epoch++
	t.WriteTimestamp = now
	t.ReadTimestamp = now
	if t.Priority < 1<<30 {
		t.Priority++ // starvation avoidance: retries push harder
	}
}

// KeyValue is a key with its value, as returned by scans.
type KeyValue struct {
	Key   keys.Key `json:"key"`
	Value []byte   `json:"value"`
}
