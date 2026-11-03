package kvserver

import (
	"testing"

	"github.com/sthorne/datax/pkg/base"
	"github.com/sthorne/datax/pkg/kvpb"
)

// TestLeftoverByPrior: the rule under which a preseed may replace a replica
// already on the store (issue #200). The sender's pre-change descriptor
// must omit this node, the preseed must be exactly one change on from it,
// and the local replica must be no newer than it; a header that fails any
// of these destroys nothing.
func TestLeftoverByPrior(t *testing.T) {
	const self base.NodeID = 4
	members := []kvpb.ReplicaDescriptor{{NodeID: 1, ReplicaID: 1}, {NodeID: 2, ReplicaID: 2}, {NodeID: 3, ReplicaID: 3}}
	withSelf := append(append([]kvpb.ReplicaDescriptor(nil), members...), kvpb.ReplicaDescriptor{NodeID: self, ReplicaID: 5})
	prior := &kvpb.RangeDescriptor{RangeID: 1, Generation: 6, Replicas: members}
	hdr := func(descGen int64, p *kvpb.RangeDescriptor) snapshotHeader {
		return snapshotHeader{Desc: kvpb.RangeDescriptor{RangeID: 1, Generation: descGen, Replicas: withSelf}, Prior: p}
	}
	cases := []struct {
		name     string
		localGen int64
		h        snapshotHeader
		want     bool
	}{
		{"a leftover behind the change, omitted from prior", 5, hdr(7, prior), true},
		{"a leftover at prior's own generation", 6, hdr(7, prior), true},
		{"no prior: a catch-up snapshot", 5, hdr(7, nil), false},
		{"this node is a member of prior", 5, hdr(7, &kvpb.RangeDescriptor{RangeID: 1, Generation: 6, Replicas: withSelf}), false},
		{"the preseed is not one change on from prior", 5, hdr(9, prior), false},
		{"the preseed is behind prior", 5, hdr(6, prior), false},
		{"the local replica is newer than prior (a live member)", 7, hdr(7, prior), false},
	}
	for _, c := range cases {
		if got := leftoverByPrior(c.localGen, c.h, self); got != c.want {
			t.Errorf("%s: got %v, want %v", c.name, got, c.want)
		}
	}
}
