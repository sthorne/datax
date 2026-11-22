package server

import (
	"reflect"
	"testing"

	"github.com/sthorne/datax/pkg/base"
	"github.com/sthorne/datax/pkg/kvpb"
	"github.com/sthorne/datax/pkg/version"
)

// TestFinalizeBlockers covers which nodes keep a finalize from going
// through: every node that may hold replicas, live or not, unless it is
// fully decommissioned (draining with no replica left).
func TestFinalizeBlockers(t *testing.T) {
	up := func(id base.NodeID, bv version.Version) kvpb.NodeDescriptor {
		return kvpb.NodeDescriptor{NodeID: id, BinaryVersion: int(bv), LivenessTime: 100}
	}
	down := func(id base.NodeID, bv version.Version) kvpb.NodeDescriptor {
		return kvpb.NodeDescriptor{NodeID: id, BinaryVersion: int(bv), LivenessTime: 1}
	}
	draining := func(nd kvpb.NodeDescriptor) kvpb.NodeDescriptor { nd.Draining = true; return nd }
	rng := func(ids ...base.NodeID) kvpb.RangeDescriptor {
		var d kvpb.RangeDescriptor
		for _, id := range ids {
			d.Replicas = append(d.Replicas, kvpb.ReplicaDescriptor{NodeID: id})
		}
		return d
	}
	live := func(nd kvpb.NodeDescriptor) bool { return nd.LivenessTime >= 100 }
	target := version.V24

	for _, tc := range []struct {
		name  string
		nodes []kvpb.NodeDescriptor
		descs []kvpb.RangeDescriptor
		want  []string
	}{
		{
			name:  "single node upgraded",
			nodes: []kvpb.NodeDescriptor{up(1, version.V23)}, // own row lags: self's binary counts
			descs: []kvpb.RangeDescriptor{rng(1)},
		},
		{
			name:  "every node upgraded",
			nodes: []kvpb.NodeDescriptor{up(1, target), up(2, target), up(3, target)},
			descs: []kvpb.RangeDescriptor{rng(1, 2, 3)},
		},
		{
			name:  "live straggler",
			nodes: []kvpb.NodeDescriptor{up(1, target), up(2, target), up(3, version.V23)},
			descs: []kvpb.RangeDescriptor{rng(1, 2, 3)},
			want:  []string{"n3 (v23)"},
		},
		{
			name:  "down straggler holding replicas",
			nodes: []kvpb.NodeDescriptor{up(1, target), up(2, target), down(3, version.V23)},
			descs: []kvpb.RangeDescriptor{rng(1, 2, 3)},
			want:  []string{"n3 (v23, down)"},
		},
		{
			name:  "down straggler with its replicas repaired away may still return",
			nodes: []kvpb.NodeDescriptor{up(1, target), up(2, target), up(4, target), down(3, version.V23)},
			descs: []kvpb.RangeDescriptor{rng(1, 2, 4)},
			want:  []string{"n3 (v23, down)"},
		},
		{
			name:  "down node of unknown binary",
			nodes: []kvpb.NodeDescriptor{up(1, target), up(2, target), down(3, 0)},
			descs: []kvpb.RangeDescriptor{rng(1, 2, 3)},
			want:  []string{"n3 (v1, down)"},
		},
		{
			name:  "draining straggler still holding replicas",
			nodes: []kvpb.NodeDescriptor{up(1, target), up(2, target), draining(up(3, version.V23))},
			descs: []kvpb.RangeDescriptor{rng(1, 2, 3), rng(1, 3)},
			want:  []string{"n3 (v23, draining, 2 replica(s) left)"},
		},
		{
			name:  "dead draining straggler still holding replicas",
			nodes: []kvpb.NodeDescriptor{up(1, target), up(2, target), draining(down(3, version.V23))},
			descs: []kvpb.RangeDescriptor{rng(1, 2, 3)},
			want:  []string{"n3 (v23, down, draining, 1 replica(s) left)"},
		},
		{
			name:  "decommissioned straggler does not block",
			nodes: []kvpb.NodeDescriptor{up(1, target), up(2, target), up(4, target), draining(down(3, version.V23))},
			descs: []kvpb.RangeDescriptor{rng(1, 2, 4)},
		},
		{
			name:  "replica on a node missing from the registry",
			nodes: []kvpb.NodeDescriptor{up(1, target), up(2, target)},
			descs: []kvpb.RangeDescriptor{rng(1, 2, 7)},
			want:  []string{"n7 (binary unknown: not in the node registry, holds 1 replica(s))"},
		},
		{
			name:  "ordered by node id",
			nodes: []kvpb.NodeDescriptor{up(1, target), down(10, version.V23), up(2, version.V23)},
			descs: []kvpb.RangeDescriptor{rng(1, 2, 10)},
			want:  []string{"n2 (v23)", "n10 (v23, down)"},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got := finalizeBlockers(target, 1, target, tc.nodes, tc.descs, live)
			if !reflect.DeepEqual(got, tc.want) {
				t.Fatalf("blockers = %q, want %q", got, tc.want)
			}
		})
	}
}
