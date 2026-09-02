package kvserver

import (
	"sync"
	"time"

	"github.com/sthorne/datax/pkg/base"
)

// Peer storage health, learned from the snapshots peers piggyback on their
// raft envelopes (see rpcpb.StorageHealth). Leaders fold it into the
// backpressure gate: a range sheds table-data writes when ANY member of
// its replica set is overloaded, not just when the leader's own engine is
// — an overloaded follower otherwise lags raft silently until it needs a
// catch-up snapshot, or the range quietly rides one node from quorum loss.

// nodeHealthFreshFor bounds how long a peer's verdict is trusted. A peer
// that stops sending raft traffic reads as healthy: going silent is the
// liveness system's problem, and stale shedding would wedge writes after
// the peer recovered or left.
const nodeHealthFreshFor = 5 * time.Second

type nodeHealth struct {
	overloaded bool
	reason     string
	receivedAt time.Time
}

type nodeHealthMap struct {
	mu sync.Mutex
	m  map[base.NodeID]nodeHealth
}

// UpdateNodeHealth records a peer's latest storage-health verdict.
func (s *Store) UpdateNodeHealth(id base.NodeID, overloaded bool, reason string) {
	s.nodeHealth.mu.Lock()
	defer s.nodeHealth.mu.Unlock()
	if s.nodeHealth.m == nil {
		s.nodeHealth.m = make(map[base.NodeID]nodeHealth)
	}
	s.nodeHealth.m[id] = nodeHealth{overloaded: overloaded, reason: reason, receivedAt: time.Now()}
}

// NodeOverloaded reports a peer's freshest storage-health verdict. Absent
// or stale snapshots read as healthy (see nodeHealthFreshFor).
func (s *Store) NodeOverloaded(id base.NodeID) (bool, string) {
	s.nodeHealth.mu.Lock()
	defer s.nodeHealth.mu.Unlock()
	h, ok := s.nodeHealth.m[id]
	if !ok || !h.overloaded || time.Since(h.receivedAt) > nodeHealthFreshFor {
		return false, ""
	}
	return true, h.reason
}
