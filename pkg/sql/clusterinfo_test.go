package sql

import (
	"testing"

	"github.com/sthorne/datax/pkg/base"
	"github.com/sthorne/datax/pkg/keys"
	"github.com/sthorne/datax/pkg/kvpb"
	"github.com/sthorne/datax/pkg/sql/catalog"
)

// Placement compliance (issue #405): the judgement behind SHOW
// PLACEMENT's compliance columns and SHOW RANGES's status.

func complianceNodes(t *testing.T, specs ...string) []ClusterNode {
	t.Helper()
	var out []ClusterNode
	for i, spec := range specs {
		state := NodeLive
		if spec[0] == '!' {
			state, spec = NodeDead, spec[1:]
		}
		l, err := base.ParseLocality(spec)
		if err != nil {
			t.Fatal(err)
		}
		out = append(out, ClusterNode{NodeID: base.NodeID(i + 1), Locality: l, State: state})
	}
	return out
}

func complianceRange(id int, lo, hi keys.Key, nodes ...int) kvpb.RangeDescriptor {
	d := kvpb.RangeDescriptor{RangeID: base.RangeID(id), StartKey: lo, EndKey: hi}
	for _, n := range nodes {
		d.Replicas = append(d.Replicas, kvpb.ReplicaDescriptor{NodeID: base.NodeID(n)})
	}
	return d
}

func TestJudgePlacement(t *testing.T) {
	us := base.PlacementPolicy{Constraints: []base.Constraint{{Key: "region", Value: "us-east-1"}, {Key: "region", Value: "us-east-2"}}}
	// n1..n3 in the US, n4 in the EU.
	nodes := complianceNodes(t, "region=us-east-1", "region=us-east-2", "region=us-east-1", "region=eu-west-1")
	t1 := &catalog.TableDescriptor{ID: 100, Name: "a"}
	t2 := &catalog.TableDescriptor{ID: 101, Name: "b"}
	span := func(td *catalog.TableDescriptor) dataSpan {
		lo, hi := keys.TableDataSpan(td.ID)
		return dataSpan{lo: lo, hi: hi, table: td}
	}
	spans := []dataSpan{span(t1), span(t2)}
	lo1, hi1 := keys.TableDataSpan(t1.ID)
	lo2, hi2 := keys.TableDataSpan(t2.ID)
	other, otherEnd := keys.TableDataSpan(500)

	type want struct {
		satisfied                                       bool
		ranges, misplaced, unsat, unattributed, unknown int
		statuses                                        []string
	}
	cases := []struct {
		name   string
		policy base.PlacementPolicy
		nodes  []ClusterNode
		descs  []kvpb.RangeDescriptor
		want   want
	}{
		{
			name: "settled", policy: us, nodes: nodes,
			descs: []kvpb.RangeDescriptor{complianceRange(1, lo1, hi1, 1, 2, 3), complianceRange(2, lo2, hi2, 1, 2, 3), complianceRange(3, other, otherEnd, 4)},
			want:  want{satisfied: true, ranges: 2, statuses: []string{rangeOK, rangeOK}},
		},
		{
			name: "a replica still in the EU", policy: us, nodes: nodes,
			descs: []kvpb.RangeDescriptor{complianceRange(1, lo1, hi1, 1, 2, 4), complianceRange(2, lo2, hi2, 1, 2, 3)},
			want:  want{ranges: 2, misplaced: 1, statuses: []string{rangeMisplaced, rangeOK}},
		},
		{
			// Two tables in one range: no policy governs it, so its
			// replicas could go anywhere. Its replicas are all in the US
			// today, and that is still not compliance.
			name: "two tables share a range", policy: us, nodes: nodes,
			descs: []kvpb.RangeDescriptor{complianceRange(1, lo1, hi2, 1, 2, 3)},
			want:  want{ranges: 1, unattributed: 1, statuses: []string{rangeUnattributed}},
		},
		{
			// The table shares a range with system data before it.
			name: "range starts before the table", policy: us, nodes: nodes,
			descs: []kvpb.RangeDescriptor{complianceRange(1, keys.Key("\x00"), hi1, 1, 2, 3), complianceRange(2, lo2, hi2, 1, 2, 3)},
			want:  want{ranges: 2, unattributed: 1, statuses: []string{rangeUnattributed, rangeOK}},
		},
		{
			// n9 is in no registry: gone, or never heard from.
			name: "replica on an unknown node", policy: us, nodes: nodes,
			descs: []kvpb.RangeDescriptor{complianceRange(1, lo1, hi1, 1, 2, 9), complianceRange(2, lo2, hi2, 1, 2, 3)},
			want:  want{ranges: 2, unknown: 1, statuses: []string{rangeUnknownLocality, rangeOK}},
		},
		{
			// Only two US nodes for three replicas: the EU replica can
			// never leave, since the allocator adds before it removes.
			name: "fewer matching nodes than replicas", policy: us,
			nodes: complianceNodes(t, "region=us-east-1", "region=us-east-2", "region=eu-west-1"),
			descs: []kvpb.RangeDescriptor{complianceRange(1, lo1, hi1, 1, 2, 3), complianceRange(2, lo2, hi2, 1, 2, 3)},
			want:  want{ranges: 2, misplaced: 2, unsat: 2, statuses: []string{rangeUnsatisfiable, rangeUnsatisfiable}},
		},
		{
			// A dead US node does not count towards the policy.
			name: "a matching node is dead", policy: us,
			nodes: complianceNodes(t, "region=us-east-1", "region=us-east-2", "!region=us-east-1", "region=eu-west-1"),
			descs: []kvpb.RangeDescriptor{complianceRange(1, lo1, hi1, 1, 2, 3)},
			want:  want{ranges: 1, unsat: 1, statuses: []string{rangeUnsatisfiable}},
		},
		{
			name: "no node matches", policy: base.PlacementPolicy{Constraints: []base.Constraint{{Key: "region", Value: "ap-south-1"}}}, nodes: nodes,
			descs: []kvpb.RangeDescriptor{complianceRange(1, lo1, hi1, 1, 2, 3)},
			want:  want{ranges: 1, misplaced: 1, unsat: 1, statuses: []string{rangeUnsatisfiable}},
		},
		{
			// A policy that cannot be met is not met, data or no data.
			name: "unsatisfiable before any range", policy: base.PlacementPolicy{Constraints: []base.Constraint{{Key: "region", Value: "eu-west-1"}}}, nodes: nodes,
			want: want{},
		},
		{
			name: "replica count of one", policy: base.PlacementPolicy{Replicas: 1, Constraints: []base.Constraint{{Key: "region", Value: "eu-west-1"}}}, nodes: nodes,
			descs: []kvpb.RangeDescriptor{complianceRange(1, lo1, hi1, 4)},
			want:  want{satisfied: true, ranges: 1, statuses: []string{rangeOK}},
		},
		{
			// No constraints: nothing can be out of place. A shared range
			// is still shown for what it is.
			name: "no policy", policy: base.PlacementPolicy{}, nodes: nodes,
			descs: []kvpb.RangeDescriptor{complianceRange(1, lo1, hi2, 1, 4, 9)},
			want:  want{satisfied: true, ranges: 1, unattributed: 1, statuses: []string{rangeUnattributed}},
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			rep := judgePlacement(c.policy, spans, c.descs, c.nodes)
			got := want{satisfied: rep.satisfied, ranges: len(rep.ranges), misplaced: rep.misplaced, unsat: rep.unsatisfiable,
				unattributed: rep.unattributed, unknown: rep.unknownReplicas}
			for _, r := range rep.ranges {
				got.statuses = append(got.statuses, r.status)
			}
			if got.satisfied != c.want.satisfied || got.ranges != c.want.ranges || got.misplaced != c.want.misplaced ||
				got.unsat != c.want.unsat || got.unattributed != c.want.unattributed || got.unknown != c.want.unknown ||
				len(got.statuses) != len(c.want.statuses) {
				t.Fatalf("got %+v, want %+v", got, c.want)
			}
			for i := range got.statuses {
				if got.statuses[i] != c.want.statuses[i] {
					t.Fatalf("statuses %v, want %v", got.statuses, c.want.statuses)
				}
			}
		})
	}
}
