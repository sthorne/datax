package kvserver

import (
	"testing"

	"github.com/sthorne/datax/pkg/base"
	"github.com/sthorne/datax/pkg/kvpb"
)

// TestFromNonMember (issue #245): a raft message is dropped when its
// sender's replica id is one the descriptor has moved past — not listed
// and below NextReplicaID — and stepped from a listed member, from an id
// the descriptor has not allocated yet (a member this replica has not
// applied the addition of), when there is no sender, and on a
// descriptor that never allocated ids.
func TestFromNonMember(t *testing.T) {
	r := &Replica{replicaID: 2}
	r.mu.desc = kvpb.RangeDescriptor{RangeID: 7, NextReplicaID: 5,
		Replicas: []kvpb.ReplicaDescriptor{{NodeID: 1, ReplicaID: 1}, {NodeID: 2, ReplicaID: 2}, {NodeID: 3, ReplicaID: 3}}}
	for from, want := range map[uint64]bool{0: false, 1: false, 2: false, 3: false, 4: true, 5: false, 9: false} {
		if got := r.fromNonMember(from); got != want {
			t.Errorf("from replica %d: dropped=%v, want %v", from, got, want)
		}
	}
	r.mu.desc.NextReplicaID = 0
	if r.fromNonMember(4) {
		t.Error("a descriptor that never allocated ids drops a message")
	}
}

// TestLeftoverReason: what the /meta record has to say for a quiet
// replica to be a leftover.
func TestLeftoverReason(t *testing.T) {
	s := &Store{cfg: StoreConfig{NodeID: 4, StoreID: 4}}
	r := &Replica{rangeID: 7, replicaID: 5}
	desc := kvpb.RangeDescriptor{RangeID: 7, Generation: 3}
	members := []kvpb.ReplicaDescriptor{{NodeID: 1, StoreID: 1, ReplicaID: 1}, {NodeID: 2, StoreID: 2, ReplicaID: 2}, {NodeID: 3, StoreID: 3, ReplicaID: 3}}
	with := func(id base.ReplicaID) []kvpb.ReplicaDescriptor {
		return append(append([]kvpb.ReplicaDescriptor(nil), members...), kvpb.ReplicaDescriptor{NodeID: 4, StoreID: 4, ReplicaID: id})
	}
	cases := []struct {
		name string
		rec  kvpb.RangeDescriptor
		gone bool
	}{
		{"another range owns the span", kvpb.RangeDescriptor{RangeID: 8, Replicas: with(5)}, true},
		{"this range without this store", kvpb.RangeDescriptor{RangeID: 7, Replicas: members}, true},
		{"this store with a newer replica", kvpb.RangeDescriptor{RangeID: 7, Replicas: with(6)}, true},
		{"this store at this replica", kvpb.RangeDescriptor{RangeID: 7, Replicas: with(5)}, false},
		{"this store at an older replica (stale record)", kvpb.RangeDescriptor{RangeID: 7, Replicas: with(4)}, false},
		// A sibling store of this node, or this store's id on another
		// node, is not this store: the match needs both ids.
		{"another store of this node holds the newer replica", kvpb.RangeDescriptor{RangeID: 7, Replicas: append(append([]kvpb.ReplicaDescriptor(nil), members...), kvpb.ReplicaDescriptor{NodeID: 4, StoreID: 9, ReplicaID: 6})}, true},
		{"this store's id on another node", kvpb.RangeDescriptor{RangeID: 7, Replicas: append(append([]kvpb.ReplicaDescriptor(nil), members...), kvpb.ReplicaDescriptor{NodeID: 8, StoreID: 4, ReplicaID: 5})}, true},
	}
	for _, c := range cases {
		if got := s.leftoverReason(r, desc, c.rec); (got != "") != c.gone {
			t.Errorf("%s: %q, want gone=%v", c.name, got, c.gone)
		}
		if got := s.leftoverByMeta(r, desc, c.rec, true); (got != "") != c.gone {
			t.Errorf("%s (through leftoverByMeta): %q, want gone=%v", c.name, got, c.gone)
		}
	}
	// No record, or one that does not cover the key, means nothing,
	// whatever the zero or stale descriptor beside it would have said
	// (QA on the PR: the most destructive branch, pinned on its own).
	for _, rec := range []kvpb.RangeDescriptor{{}, {RangeID: 8, Replicas: with(5)}, {RangeID: 7, Replicas: members}} {
		if got := s.leftoverByMeta(r, desc, rec, false); got != "" {
			t.Errorf("with no record found, the pass would destroy the replica: %q", got)
		}
	}
}
