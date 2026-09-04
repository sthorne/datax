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
	// BinaryVersion is the protocol version of the binary the node runs
	// (see pkg/version), re-asserted on every heartbeat. 0 (absent, or an
	// entry synthesized from raft traffic before the node's first
	// heartbeat lands) conservatively reads as version 1.
	BinaryVersion int `json:"binary_version,omitempty"`

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
	// Machine is the node's host summary (CPU, memory, store disk, load,
	// file descriptors), sampled by the node itself and re-asserted on
	// every heartbeat so any node can show every node's row without a
	// fan-out. Absent from nodes on binaries that predate it.
	Machine *MachineSummary `json:"machine,omitempty"`
	// Latency is this node's measured round trip and clock offset to each
	// peer (see PeerLatency), re-asserted on every heartbeat so any node
	// can show the whole matrix without a fan-out.
	Latency []PeerLatency `json:"latency,omitempty"`
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

// MachineSummary is the compact host picture a node advertises on its
// heartbeat (see pkg/util/sysstats for the full sample behind it).
type MachineSummary struct {
	// CPUPercent is host CPU busy, percent of all cores, over the last
	// sampling interval; Load1 the one-minute load average; Cores the
	// logical CPU count.
	CPUPercent float64 `json:"cpu_percent"`
	Load1      float64 `json:"load1"`
	Cores      int     `json:"cores"`
	// Host memory in bytes: total, and available as the kernel defines
	// it; RSS is the datax process's resident set.
	MemTotal     uint64 `json:"mem_total"`
	MemAvailable uint64 `json:"mem_available"`
	RSS          uint64 `json:"rss"`
	// The store directory's filesystem in bytes (zero for in-memory
	// stores or platforms without the figure).
	DiskTotal uint64 `json:"disk_total"`
	DiskFree  uint64 `json:"disk_free"`
	// File descriptors held and the soft limit.
	OpenFDs int `json:"open_fds"`
	FDLimit int `json:"fd_limit"`
	// UptimeSeconds is how long the process has been up.
	UptimeSeconds int64 `json:"uptime_seconds"`
}

// PeerLatency is one node's view of the network to one peer, from a
// periodic ping (the NTP exchange, see pkg/rpc).
type PeerLatency struct {
	Peer base.NodeID `json:"peer"`
	// RTTMicros is the smoothed round-trip time and P99Micros the 99th
	// percentile over the recent ring; OffsetMicros is the peer's physical
	// clock minus this node's (positive: the peer runs ahead).
	RTTMicros    int64 `json:"rtt_us"`
	P99Micros    int64 `json:"p99_us"`
	OffsetMicros int64 `json:"offset_us"`
	// Reachable is false once a ping has timed out or failed; AgeMillis is
	// how long ago the last successful ping was.
	Reachable bool  `json:"reachable"`
	AgeMillis int64 `json:"age_ms"`
}
