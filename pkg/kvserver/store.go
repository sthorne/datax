package kvserver

import (
	"context"
	"sync"
	"time"

	"go.etcd.io/raft/v3/raftpb"

	"github.com/sthorne/datax/pkg/base"
	"github.com/sthorne/datax/pkg/keys"
	"github.com/sthorne/datax/pkg/kvpb"
	"github.com/sthorne/datax/pkg/storage"
	"github.com/sthorne/datax/pkg/util/encoding"
	"github.com/sthorne/datax/pkg/util/hlc"
	"github.com/sthorne/datax/pkg/util/log"
	"github.com/sthorne/datax/pkg/util/stop"
)

// RaftTransport is the store's outbound Raft message channel, implemented
// by pkg/rpc.
type RaftTransport interface {
	SendRaftMessage(ctx context.Context, to base.NodeID, rangeID base.RangeID, m raftpb.Message) error
}

// StoreConfig collects a store's dependencies.
type StoreConfig struct {
	NodeID           base.NodeID
	StoreID          base.StoreID
	Engine           *storage.Engine
	Clock            *hlc.Clock
	Transport        RaftTransport
	SnapshotSender   SnapshotSender
	Stopper          *stop.Stopper
	RaftTickInterval time.Duration
	// DisableLeaseReads reverts ReadIndex to full quorum round trips
	// (raft's ReadOnlySafe) instead of leader leases.
	DisableLeaseReads bool
	// MergeSizeThreshold: a range and its right neighbor both below it (and
	// colocated) are merged by the housekeeping loop. 0 = a quarter of
	// SplitSizeThreshold; negative disables merging.
	MergeSizeThreshold int64
	// SplitSizeThreshold is the range size that triggers an automatic split
	// (default 64 MiB; negative disables auto-splitting).
	SplitSizeThreshold int64
	// ClosedTimestampLag is how far behind now() each published closed
	// timestamp sits (0 = default 3s; negative disables publication and
	// with it follower reads). ClosedTimestampInterval is the publication
	// cadence (0 = default 1s).
	ClosedTimestampLag      time.Duration
	ClosedTimestampInterval time.Duration
	// LoadSplitThreshold is the sustained per-range QPS above which the
	// housekeeping loop splits a range by load (0 = default 500; negative
	// disables load-based splitting). LoadSettleWindow is how long a
	// fresh load-split half is protected from re-merging and how long a
	// rate must be observed before it is trusted (0 = twice the rate
	// window).
	LoadSplitThreshold float64
	LoadSettleWindow   time.Duration
	// RetentionOverride, when set, maps a key span to a GC TTL override
	// (per-table retention for timeseries tables). Called per replica from
	// the housekeeping loop with the range's [start, end); returning
	// ok=true replaces the store-wide TTL for that range with ttl.
	// expire=true additionally switches that range's GC to row expiry:
	// every version at or below the threshold is collected, INCLUDING the
	// newest one — rows older than the retention disappear. The provider
	// must only set expire for a range that lies entirely inside one
	// retention table's span, and must never return a TTL shorter than
	// the default for a span that also holds non-retention data (never
	// delete early from a mixed range).
	RetentionOverride func(start, end keys.Key) (ttl time.Duration, expire, ok bool)
	// RowExpiry, when set, provides ROW-LEVEL retention for ranges that
	// only PARTIALLY overlap retention tables (where RetentionOverride
	// must keep the conservative max TTL and can never expire whole
	// ranges). Called with the range's [start, end); when ok, the
	// returned predicate reports — for a user key and one version's
	// commit timestamp — that the row is past its table's retention,
	// keyed on the row's own timestamp column AND the version's write
	// age. GC then collects such versions (survivors and tombstones
	// included) WITHOUT ratcheting the range's GC threshold, so the
	// range's other tenants keep their history. Keys with a live intent
	// are never offered to the predicate.
	RowExpiry    func(start, end keys.Key) (func(key keys.Key, vts hlc.Timestamp) bool, bool)
	TestingKnobs TestingKnobs
}

// TestingKnobs are test-only hooks; all nil in production.
type TestingKnobs struct {
	// AfterLatch runs after a batch acquires its latches, before it is
	// evaluated — lets tests hold a latch open deliberately.
	AfterLatch func(ba *kvpb.BatchRequest)
	// BeforeReadReturn runs after a read-only batch evaluates, before the
	// final lease re-check — lets tests expire lease contact mid-read.
	BeforeReadReturn func(ba *kvpb.BatchRequest)
	// OverrideOverloaded replaces the engine's backpressure signal — lets
	// tests trip the write-shedding gate without an overloaded Pebble.
	OverrideOverloaded func() (bool, string)
	// LoadNowNanos replaces the load-tracking clock, so tests advance
	// rate windows without sleeping.
	LoadNowNanos func() int64
	// OverrideReplicaQPS injects a per-range request rate (trusted as
	// mature) — lets tests drive load splits without real traffic.
	OverrideReplicaQPS func(base.RangeID) (float64, bool)
}

// Sender executes routed KV batches (implemented by kvclient.DB). The store
// needs one for admin operations that touch other ranges (ID allocation,
// meta record updates).
type Sender interface {
	Send(ctx context.Context, ba *kvpb.BatchRequest) (*kvpb.BatchResponse, *kvpb.Error)
}

// Store owns all replicas hosted by this node's engine.
type Store struct {
	cfg StoreConfig

	senderMu sync.Mutex
	sender   Sender

	mu struct {
		sync.Mutex
		replicas map[base.RangeID]*Replica
	}

	// consistencyMu round-robins the consistency sweep over led ranges.
	consistencyMu struct {
		sync.Mutex
		cursor uint64
	}

	// nodeHealth holds peers' storage-health verdicts (see node_health.go).
	nodeHealth nodeHealthMap
}

// SetSender injects the routed KV client (once, at node startup).
func (s *Store) SetSender(sender Sender) {
	s.senderMu.Lock()
	s.sender = sender
	s.senderMu.Unlock()
}

func (s *Store) getSender() Sender {
	s.senderMu.Lock()
	defer s.senderMu.Unlock()
	return s.sender
}

func NewStore(cfg StoreConfig) *Store {
	if cfg.RaftTickInterval == 0 {
		cfg.RaftTickInterval = 100 * time.Millisecond
	}
	if cfg.SplitSizeThreshold == 0 {
		cfg.SplitSizeThreshold = 64 << 20
	}
	s := &Store{cfg: cfg}
	s.mu.replicas = make(map[base.RangeID]*Replica)
	return s
}

func (s *Store) NodeID() base.NodeID   { return s.cfg.NodeID }
func (s *Store) StoreID() base.StoreID { return s.cfg.StoreID }
func (s *Store) Clock() *hlc.Clock     { return s.cfg.Clock }

// LoadLocalRangeDescriptors scans a store for every persisted range
// descriptor. Exported for offline recovery tooling as well as startup.
func LoadLocalRangeDescriptors(eng *storage.Engine) ([]kvpb.RangeDescriptor, error) {
	// Every replica persists its descriptor at a well-known local key;
	// scan the replica-local keyspace for them.
	lower := keys.Key{0x01, 'u', 'r'}
	upper := lower.PrefixEnd()
	it := eng.NewIter(lower, upper)
	var rangeIDs []base.RangeID
	suffix := []byte("desc")
	for ok := it.SeekGE(lower); ok; ok = it.Next() {
		k := it.Key()
		if len(k) < len(lower)+8+len(suffix) {
			continue
		}
		if string(k[len(k)-len(suffix):]) != string(suffix) {
			continue
		}
		_, rid, err := encoding.DecodeUint64(k[len(lower):])
		if err != nil {
			continue
		}
		rangeIDs = append(rangeIDs, base.RangeID(rid))
	}
	if err := it.Close(); err != nil {
		return nil, err
	}
	var out []kvpb.RangeDescriptor
	for _, rid := range rangeIDs {
		desc, ok, err := loadRangeDescriptor(eng, rid)
		if err != nil {
			return nil, err
		}
		if ok {
			out = append(out, desc)
		}
	}
	return out, nil
}

// LoadReplicas restarts the Raft groups for every replica found on disk
// (called once at node startup).
func (s *Store) LoadReplicas() error {
	descs, err := LoadLocalRangeDescriptors(s.cfg.Engine)
	if err != nil {
		return err
	}
	for _, desc := range descs {
		rep, ok := desc.GetReplica(s.cfg.NodeID)
		if !ok {
			log.Warnf("%s: descriptor on disk but this node is not a member; skipping", desc.RangeID)
			continue
		}
		if _, err := s.startReplica(desc, rep.ReplicaID, false /* bootstrap */); err != nil {
			return err
		}
		log.Infof("%s: restarted replica %d [%s, %s)", desc.RangeID, rep.ReplicaID, desc.StartKey, desc.EndKey)
	}
	return nil
}

// CreateReplica creates (and boots) a brand-new replica of a range whose
// initial membership includes this store. Used at cluster bootstrap and by
// the split/upreplication paths.
func (s *Store) CreateReplica(desc kvpb.RangeDescriptor, bootstrap bool) (*Replica, error) {
	rep, ok := desc.GetReplica(s.cfg.NodeID)
	if !ok {
		return nil, kvpb.NewErrorf("node %s is not a member of %s", s.cfg.NodeID, desc.RangeID)
	}
	b := s.cfg.Engine.NewBatch()
	if err := PutRangeDescriptor(b, desc); err != nil {
		_ = b.Close()
		return nil, err
	}
	if err := b.Commit(true); err != nil {
		return nil, err
	}
	return s.startReplica(desc, rep.ReplicaID, bootstrap)
}

func (s *Store) startReplica(desc kvpb.RangeDescriptor, replicaID base.ReplicaID, bootstrap bool) (*Replica, error) {
	s.mu.Lock()
	if existing, ok := s.mu.replicas[desc.RangeID]; ok {
		s.mu.Unlock()
		return existing, nil
	}
	s.mu.Unlock()

	r, err := newReplica(s, desc, replicaID, bootstrap)
	if err != nil {
		return nil, err
	}
	s.mu.Lock()
	s.mu.replicas[desc.RangeID] = r
	s.mu.Unlock()
	return r, nil
}

// GetReplica returns the replica of the given range, if this store has one.
func (s *Store) GetReplica(rangeID base.RangeID) (*Replica, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	r, ok := s.mu.replicas[rangeID]
	return r, ok
}

// MinClosedTimestamp is the oldest closed timestamp across this store's
// replicas that have published one — the freshest timestamp EVERY such
// local replica can serve a stale read at. Replicas with no closed
// timestamp yet are skipped: they can serve nothing locally at any
// timestamp, so counting them as zero would pin bounded-staleness reads
// to their staleness bound without making those ranges servable. Zero
// when no replica qualifies.
func (s *Store) MinClosedTimestamp() hlc.Timestamp {
	var min hlc.Timestamp
	s.VisitReplicas(func(r *Replica) bool {
		if ct := r.ClosedTimestamp(); !ct.IsEmpty() && (min.IsEmpty() || ct.Less(min)) {
			min = ct
		}
		return true
	})
	return min
}

// LocalDescriptor returns the current descriptor of this store's replica
// of the range, if it holds one — the authoritative local-membership
// answer for gateway routing (kvclient.LocalRanges).
func (s *Store) LocalDescriptor(id base.RangeID) (kvpb.RangeDescriptor, bool) {
	r, ok := s.GetReplica(id)
	if !ok {
		return kvpb.RangeDescriptor{}, false
	}
	return r.Desc(), true
}

// VisitReplicas calls f for each replica until it returns false.
func (s *Store) VisitReplicas(f func(*Replica) bool) {
	s.mu.Lock()
	reps := make([]*Replica, 0, len(s.mu.replicas))
	for _, r := range s.mu.replicas {
		reps = append(reps, r)
	}
	s.mu.Unlock()
	for _, r := range reps {
		if !f(r) {
			return
		}
	}
}

// replicaForKey returns the replica whose range covers the (addressed) key.
func (s *Store) replicaForKey(k keys.Key) (*Replica, bool) {
	var found *Replica
	s.VisitReplicas(func(r *Replica) bool {
		if d := r.Desc(); d.ContainsKey(k) {
			found = r
			return false
		}
		return true
	})
	return found, found != nil
}

// ExecuteBatch routes a batch to the right local replica and executes it.
// After a split, a client's routing may be stale; if another local replica
// covers the key, reroute to it transparently, and always decorate
// RangeKeyMismatch errors with the fresh local descriptors so clients can
// repair their caches.
func (s *Store) ExecuteBatch(ctx context.Context, ba *kvpb.BatchRequest) (*kvpb.BatchResponse, *kvpb.Error) {
	if len(ba.Requests) == 0 {
		return nil, kvpb.NewErrorf("empty batch")
	}
	// A request union with no populated member is what an unknown request
	// type decodes to under the JSON fallback encoding (a newer peer's
	// request): degrade to an error, never a nil dereference.
	for i := range ba.Requests {
		if ba.Requests[i].GetInner() == nil {
			return nil, kvpb.NewErrorf("unsupported request at index %d in batch (version skew?)", i)
		}
	}
	addr, err := keys.Addr(ba.Requests[0].GetInner().Header().Key)
	if err != nil {
		return nil, kvpb.NewError(err)
	}

	var r *Replica
	if ba.Header.RangeID != 0 {
		var ok bool
		r, ok = s.GetReplica(ba.Header.RangeID)
		if !ok {
			e := kvpb.NewErrorf("%s: no replica on node %s", ba.Header.RangeID, s.cfg.NodeID)
			e.RangeNotFound = &kvpb.RangeNotFoundError{RangeID: ba.Header.RangeID}
			return nil, e
		}
	}
	for attempt := 0; ; attempt++ {
		if r == nil || !containsKey(r.Desc(), addr) {
			r2, ok := s.replicaForKey(addr)
			if !ok || (r != nil && r2 == r) || attempt >= 2 {
				e := kvpb.NewErrorf("no replica covering key %s on node %s", addr, s.cfg.NodeID)
				e.RangeKeyMismatch = &kvpb.RangeKeyMismatchError{RequestKey: addr, ActualDescriptors: s.localDescriptors()}
				return nil, e
			}
			r = r2
			ba.Header.RangeID = r.Desc().RangeID
		}
		br, kerr := r.Execute(ctx, ba)
		if kerr != nil && kerr.RangeKeyMismatch != nil {
			kerr.RangeKeyMismatch.ActualDescriptors = s.localDescriptors()
			if attempt < 2 {
				r = nil // re-route: our own replica map may have fresher bounds
				continue
			}
		}
		return br, kerr
	}
}

func (s *Store) localDescriptors() []kvpb.RangeDescriptor {
	var out []kvpb.RangeDescriptor
	s.VisitReplicas(func(r *Replica) bool {
		out = append(out, r.Desc())
		return true
	})
	return out
}

// HandleRaftMessage delivers an incoming Raft message to its replica.
func (s *Store) HandleRaftMessage(ctx context.Context, rangeID base.RangeID, m raftpb.Message) {
	r, ok := s.GetReplica(rangeID)
	if !ok {
		// No replica here (yet). Phase 7 creates replicas on preseed;
		// until then, stray messages are dropped.
		log.Debugf("dropping raft message for unknown %s", rangeID)
		return
	}
	if err := r.stepRaftMessage(ctx, m); err != nil {
		log.Warnf("%s: raft step failed: %v", rangeID, err)
	}
}

func containsKey(d kvpb.RangeDescriptor, k keys.Key) bool { return d.ContainsKey(k) }
