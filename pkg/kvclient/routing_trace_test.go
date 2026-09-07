package kvclient

import (
	"strings"
	"testing"

	"github.com/sthorne/datax/pkg/base"
	"github.com/sthorne/datax/pkg/keys"
	"github.com/sthorne/datax/pkg/kvpb"
)

// A batch that gives up on routing says what it tried (issue #200): the
// descriptor each regroup sent on, with its generation and replicas,
// the node's answer, and the descriptors that answer carried; bounded to
// the last few, oldest dropped first.
func TestRoutingTraceSaysWhatWasTried(t *testing.T) {
	desc := func(id base.RangeID, gen int64, nodes ...base.NodeID) kvpb.RangeDescriptor {
		d := kvpb.RangeDescriptor{RangeID: id, Generation: gen, StartKey: keys.Key("a"), EndKey: keys.Key("z")}
		for _, n := range nodes {
			d.Replicas = append(d.Replicas, kvpb.ReplicaDescriptor{NodeID: n})
		}
		return d
	}
	var tr routingTrace
	mismatch := kvpb.NewErrorf("no replica covering key /table/mc/3 on node n3")
	mismatch.RangeKeyMismatch = &kvpb.RangeKeyMismatchError{ActualDescriptors: []kvpb.RangeDescriptor{desc(12, 7, 1, 2), desc(13, 2, 1, 2, 3)}}
	tr.note(desc(12, 6, 1, 2, 3), mismatch)
	got := tr.String()
	for _, want := range []string{"r12@6[n1 n2 n3]: ", "no replica covering key /table/mc/3 on node n3", "answer carried 2 descriptor(s): r12@7 r13@2"} {
		if !strings.Contains(got, want) {
			t.Errorf("trace %q lacks %q", got, want)
		}
	}
	// An answer without descriptors says nothing about carrying any.
	tr.note(desc(12, 7, 1, 2), kvpb.NewErrorf("r12: no replica on node n3"))
	if got := tr.String(); strings.Count(got, "answer carried") != 1 || !strings.HasSuffix(got, "r12@7[n1 n2]: r12: no replica on node n3") {
		t.Errorf("trace %q", got)
	}
	// Bounded: the oldest entries go first, the newest is always last.
	for i := int64(0); i < 20; i++ {
		tr.note(desc(20, i, 1), kvpb.NewErrorf("attempt %d", i))
	}
	if len(tr) != routingTraceLen {
		t.Fatalf("trace holds %d entries, want %d", len(tr), routingTraceLen)
	}
	if !strings.HasPrefix(tr[0], "r20@12[n1]") || !strings.HasSuffix(tr[len(tr)-1], "attempt 19") {
		t.Fatalf("trace kept the wrong entries: first %q, last %q", tr[0], tr[len(tr)-1])
	}
}
