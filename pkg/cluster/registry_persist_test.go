package cluster

import (
	"reflect"
	"testing"

	"github.com/sthorne/datax/pkg/base"
	"github.com/sthorne/datax/pkg/kvpb"
	"github.com/sthorne/datax/pkg/storage"
)

// TestPersistRegistryDurableOnChange: on a state engine with no WAL, the
// registry copy a restart dials its peers from is flushed when a node
// joins or moves, so a crash cannot lose it; a beat that changes only
// liveness or load updates the copy without a flush, and an identical
// registry writes nothing.
func TestPersistRegistryDurableOnChange(t *testing.T) {
	eng, err := storage.Open(t.TempDir(), storage.Options{DisableWAL: true})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = eng.Close() }()
	baseSeq := eng.FlushedSeqNum()

	nodes := []kvpb.NodeDescriptor{
		{NodeID: 1, Address: "10.0.0.1:26257", LivenessTime: 100},
		{NodeID: 2, Address: "10.0.0.2:26257", LivenessTime: 100},
	}
	if err := PersistRegistry(eng, nodes); err != nil {
		t.Fatal(err)
	}
	joined := eng.FlushedSeqNum()
	if joined <= baseSeq {
		t.Fatalf("a new registry was not flushed (flushed seq %d, was %d): a crash loses it", joined, baseSeq)
	}

	// A heartbeat: liveness and load move, membership does not.
	nodes[0].LivenessTime, nodes[1].LivenessTime, nodes[1].LeaderQPS = 200, 200, 7
	if err := PersistRegistry(eng, nodes); err != nil {
		t.Fatal(err)
	}
	if got := eng.FlushedSeqNum(); got != joined {
		t.Fatalf("a liveness-only beat flushed the state engine (seq %d -> %d)", joined, got)
	}
	got, err := LoadPersistedRegistry(eng)
	if err != nil || len(got) != 2 || got[0].LivenessTime != 200 {
		t.Fatalf("the beat's rows were not stored: %+v %v", got, err)
	}

	// n2 comes back on a new address: a restart must dial that one.
	nodes[1].Address = "10.0.0.9:26257"
	if err := PersistRegistry(eng, nodes); err != nil {
		t.Fatal(err)
	}
	if got := eng.FlushedSeqNum(); got <= joined {
		t.Fatalf("a changed address was not flushed (seq %d, was %d)", got, joined)
	}
}

// TestRegistryIdentityIgnoresBeats: the identity of the registry leaves
// out what every heartbeat rewrites and keeps what membership changes.
func TestRegistryIdentityIgnoresBeats(t *testing.T) {
	a := []kvpb.NodeDescriptor{{NodeID: 2, Address: "b"}, {NodeID: 1, Address: "a", LivenessTime: 1, LeaderCount: 3,
		FailedReplicas: []base.RangeID{4}, Machine: &kvpb.MachineSummary{}}}
	b := []kvpb.NodeDescriptor{{NodeID: 1, Address: "a", LivenessTime: 9}, {NodeID: 2, Address: "b", ReplicaBytes: 5}}
	ia, ib := RegistryIdentity(a), RegistryIdentity(b)
	if !reflect.DeepEqual(ia, ib) || ia[0].NodeID != 1 {
		t.Fatalf("identities differ: %+v vs %+v", ia, ib)
	}
	b[1].Draining = true
	if RegistryIdentity(b)[1].Draining != true {
		t.Fatal("draining is part of the identity")
	}
}
