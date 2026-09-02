package kvserver

import (
	"fmt"
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
//
// An overloaded verdict is STICKY: it holds until the same peer reports
// healthy again. Verdicts only ride envelopes the peer itself sends, and
// the state the gate exists to keep a follower out of — a Pebble hard
// stall, its raft loop blocked in Batch.Commit — is exactly the state in
// which it sends nothing. Aging the verdict out on silence (the original
// 5s window) therefore released the gate for the one member it was
// protecting, and the leader resumed writing to it (issue #65). Silence
// after "overloaded" is read as continued overload; silence after
// "healthy" is a liveness matter, not a storage one, and reads as healthy
// as before. A peer that dies while overloaded keeps its ranges shed until
// it comes back (healthy) or repair moves its replicas elsewhere —
// membership changes are /system writes and never gated — which is the
// safe direction to fail in: the range was one node from quorum loss
// either way.

// nodeHealthQuietAfter is how long a sticky verdict's peer may be silent
// before the shed reason says so, so an operator can tell a stalled node
// from a chatty one.
const nodeHealthQuietAfter = 5 * time.Second

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

// NodeOverloaded reports a peer's latest storage-health verdict. A peer
// that never reported, or last reported healthy, reads as healthy; an
// overloaded verdict holds until the peer reports healthy again, however
// long it stays silent (see the package comment above).
func (s *Store) NodeOverloaded(id base.NodeID) (bool, string) {
	s.nodeHealth.mu.Lock()
	defer s.nodeHealth.mu.Unlock()
	h, ok := s.nodeHealth.m[id]
	if !ok || !h.overloaded {
		return false, ""
	}
	if quiet := time.Since(h.receivedAt); quiet > nodeHealthQuietAfter {
		return true, fmt.Sprintf("%s (no raft traffic from it for %s)", h.reason, quiet.Truncate(time.Second))
	}
	return true, h.reason
}
