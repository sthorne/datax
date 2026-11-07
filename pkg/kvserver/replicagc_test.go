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
	}
	for _, c := range cases {
		if got := s.leftoverReason(r, desc, c.rec); (got != "") != c.gone {
			t.Errorf("%s: %q, want gone=%v", c.name, got, c.gone)
		}
	}
}
