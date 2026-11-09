package testcluster

import (
	"context"
	"fmt"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/sthorne/datax/pkg/base"
	"github.com/sthorne/datax/pkg/keys"
	"github.com/sthorne/datax/pkg/kvpb"
	"github.com/sthorne/datax/pkg/server"
)

// Issue #200's reshard half. A split, a merge and a replica change each
// build the descriptor they will propose from the one the leader holds at
// that moment and propose it afterwards; two of them on the same range
// can be built from the same descriptor, and raft orders their application.
// The merge apply refused a stale trigger; the split and the membership
// change adopted theirs unconditionally, so a split or a replica change
// built before a merge and applied after it rewrote the range's bounds to
// the pre-merge ones at the merged generation — the absorbed span then
// belonged to no range, /meta still named the merged descriptor at the
// same generation, and every batch for a key in it retried forever
// against the same descriptor ("routing did not converge ... answer
// carried r125@106"). These tests hold each operation between building
// and proposing, change the range underneath it, and check the stale
// operation is refused and the range is whole.

// descriptorHold arms the BeforeProposeDescriptorChange knob so that the
// first `op` on the armed range blocks until release is closed.
type descriptorHold struct {
	rangeID atomic.Int64
	op      atomic.Value
	fired   atomic.Bool
	arrived chan struct{}
	release chan struct{}
}

func newDescriptorHold() *descriptorHold {
	h := &descriptorHold{arrived: make(chan struct{}, 1), release: make(chan struct{})}
	h.op.Store("")
	return h
}

func (h *descriptorHold) knob(id base.RangeID, op string) {
	if int64(id) != h.rangeID.Load() || op != h.op.Load().(string) {
		return
	}
	if !h.fired.CompareAndSwap(false, true) {
		return
	}
	h.arrived <- struct{}{}
	<-h.release
}

func (h *descriptorHold) arm(id base.RangeID, op string) {
	h.op.Store(op)
	h.rangeID.Store(int64(id))
}

func (h *descriptorHold) waitArrived(t *testing.T, what string) {
	t.Helper()
	select {
	case <-h.arrived:
	case <-time.After(60 * time.Second):
		t.Fatalf("%s never reached the propose point", what)
	}
}

// raceFixture: a table prefix split in two, five keys on each side.
type raceFixture struct {
	tc       *TestCluster
	prefix   keys.Key
	keys     []keys.Key
	lhs, rhs kvpb.RangeDescriptor
}

func newRaceFixture(t *testing.T, ctx context.Context, tc *TestCluster, tableID uint64) *raceFixture {
	t.Helper()
	f := &raceFixture{tc: tc, prefix: keys.TableDataPrefix(tableID)}
	for i := 0; i < 10; i++ {
		side := "a"
		if i >= 5 {
			side = "z"
		}
		k := append(f.prefix.Clone(), fmt.Sprintf("%s%02d", side, i)...)
		f.keys = append(f.keys, k)
		if err := tc.Nodes[0].DB().Put(ctx, k, []byte(fmt.Sprintf("v%d", i))); err != nil {
			t.Fatal(err)
		}
	}
	sr, err := tc.Nodes[0].DB().AdminSplit(ctx, append(f.prefix.Clone(), "m"...))
	if err != nil {
		t.Fatalf("split: %v", err)
	}
	f.lhs, f.rhs = sr.Left, sr.Right
	return f
}

// everyKeyReadable reads every fixture key through every node: a span that
// belongs to no range fails here with "routing did not converge".
func (f *raceFixture) everyKeyReadable(t *testing.T, ctx context.Context, when string) {
	t.Helper()
	for i, k := range f.keys {
		for n, node := range f.tc.Nodes {
			v, err := node.DB().Get(ctx, k)
			if err != nil {
				t.Fatalf("%s: key %d via node %d: %v", when, i, n, err)
			}
			if want := fmt.Sprintf("v%d", i); string(v) != want {
				t.Fatalf("%s: key %d via node %d: %q, want %q", when, i, n, v, want)
			}
		}
	}
}

// metaRecord returns the /meta descriptor of a range.
func (f *raceFixture) metaRecord(t *testing.T, ctx context.Context, id base.RangeID) kvpb.RangeDescriptor {
	t.Helper()
	descs, err := f.tc.ranges(ctx)
	if err != nil {
		t.Fatal(err)
	}
	for _, d := range descs {
		if d.RangeID == id {
			return d
		}
	}
	t.Fatalf("range %d has no /meta record", id)
	return kvpb.RangeDescriptor{}
}

// localDescriptors returns each node's own descriptor of the range (the
// one its store routes by), node index → descriptor.
func (f *raceFixture) localDescriptors(id base.RangeID) map[int]kvpb.RangeDescriptor {
	out := map[int]kvpb.RangeDescriptor{}
	for i, n := range f.tc.Nodes {
		if d, ok := n.Store().LocalDescriptor(id); ok {
			out[i] = d
		}
	}
	return out
}

func sameRangeDescriptor(a, b kvpb.RangeDescriptor) bool {
	if a.RangeID != b.RangeID || a.Generation != b.Generation || !a.StartKey.Equal(b.StartKey) || !a.EndKey.Equal(b.EndKey) || len(a.Replicas) != len(b.Replicas) {
		return false
	}
	for _, x := range a.Replicas {
		if y, ok := b.GetReplica(x.NodeID); !ok || y != x {
			return false
		}
	}
	return true
}

// shedReplica removes one node's replica from both fixture ranges (a merge
// needs equal replica sets) and returns that node's id: a bootstrap puts a
// replica on every node, so a spare for a later addition has to be made.
func (f *raceFixture) shedReplica(t *testing.T, ctx context.Context) base.NodeID {
	t.Helper()
	lhsLead, rhsLead := f.tc.LeaderIndex(f.lhs.RangeID), f.tc.LeaderIndex(f.rhs.RangeID)
	victim := -1
	for i := range f.tc.Nodes {
		if i != lhsLead && i != rhsLead {
			victim = i
			break
		}
	}
	if victim < 0 {
		t.Fatal("no node that leads neither range")
	}
	id := f.tc.Nodes[victim].NodeID()
	for _, d := range []*kvpb.RangeDescriptor{&f.lhs, &f.rhs} {
		resp, err := f.tc.Nodes[f.tc.LeaderIndex(d.RangeID)].DB().AdminChangeReplicas(ctx, d.StartKey, 0, id)
		if err != nil {
			t.Fatalf("shedding n%d from %s: %v", id, d.RangeID, err)
		}
		*d = resp.Desc
	}
	return id
}

// TestSplitBuiltBeforeAMergeIsRefused: a split of the LHS built before the
// LHS absorbs its neighbour, applied after. The split must be refused; the
// merged range must keep its full span on every replica and in /meta, every
// key on both sides must stay reachable, and a split built afresh must go
// through.
func TestSplitBuiltBeforeAMergeIsRefused(t *testing.T) {
	hold := newDescriptorHold()
	tc := StartWithOptions(t, 3, func(c *server.Config) { c.TestingKnobs.BeforeProposeDescriptorChange = hold.knob })
	ctx, cancel := context.WithTimeout(context.Background(), 180*time.Second)
	defer cancel()
	f := newRaceFixture(t, ctx, tc, 861)

	hold.arm(f.lhs.RangeID, "split")
	lead := tc.LeaderIndex(f.lhs.RangeID)
	splitErr := make(chan error, 1)
	go func() {
		_, err := tc.Nodes[lead].DB().AdminSplit(ctx, append(f.prefix.Clone(), "a03"...))
		splitErr <- err
	}()
	hold.waitArrived(t, "the split")

	merged := mergeRanges(t, ctx, tc, f.lhs.RangeID)
	if !merged.EndKey.Equal(f.rhs.EndKey) {
		t.Fatalf("merge produced %v, want end %s", merged, f.rhs.EndKey)
	}
	close(hold.release)

	select {
	case err := <-splitErr:
		if err == nil || !strings.Contains(err.Error(), "split against generation") {
			t.Fatalf("stale split: got %v, want refusal", err)
		}
	case <-time.After(60 * time.Second):
		t.Fatal("stale split never returned")
	}

	// Nothing of the stale split landed: every replica and /meta hold the
	// merged span at the merged generation, and both sides are readable.
	for i, d := range f.localDescriptors(f.lhs.RangeID) {
		if d.Generation != merged.Generation || !d.EndKey.Equal(merged.EndKey) {
			t.Fatalf("node %d holds %v after the refused split, want %v", i, d, merged)
		}
	}
	if m := f.metaRecord(t, ctx, f.lhs.RangeID); m.Generation != merged.Generation || !m.EndKey.Equal(merged.EndKey) {
		t.Fatalf("/meta holds %v after the refused split, want %v", m, merged)
	}
	f.everyKeyReadable(t, ctx, "after the refused split")

	// A split built from the current descriptor goes through, and both
	// sides together cover the merged span.
	sr, err := tc.Nodes[tc.LeaderIndex(f.lhs.RangeID)].DB().AdminSplit(ctx, append(f.prefix.Clone(), "a03"...))
	if err != nil {
		t.Fatalf("fresh split: %v", err)
	}
	if sr.Left.Generation != merged.Generation+1 || !sr.Right.EndKey.Equal(merged.EndKey) {
		t.Fatalf("fresh split produced %v / %v, want generation %d ending at %s", sr.Left, sr.Right, merged.Generation+1, merged.EndKey)
	}
	f.everyKeyReadable(t, ctx, "after the fresh split")
}

// TestReplicaChangeBuiltBeforeAMergeIsRefused: a replica addition on the
// LHS built before the LHS absorbs its neighbour, applied after. The
// membership change must be refused (and reported refused, not applied),
// the merged range must keep its span and its membership, and the same
// addition built afresh must go through with the merged bounds.
func TestReplicaChangeBuiltBeforeAMergeIsRefused(t *testing.T) {
	hold := newDescriptorHold()
	tc := StartWithOptions(t, 4, func(c *server.Config) {
		c.TestingKnobs.BeforeProposeDescriptorChange = hold.knob
		c.RebalanceThreshold = -1
	})
	ctx, cancel := context.WithTimeout(context.Background(), 180*time.Second)
	defer cancel()
	f := newRaceFixture(t, ctx, tc, 862)
	spare := f.shedReplica(t, ctx)

	hold.arm(f.lhs.RangeID, "replica-change")
	lead := tc.LeaderIndex(f.lhs.RangeID)
	changeErr := make(chan error, 1)
	go func() {
		_, err := tc.Nodes[lead].DB().AdminChangeReplicas(ctx, f.lhs.StartKey, spare, 0)
		changeErr <- err
	}()
	hold.waitArrived(t, "the replica change")

	merged := mergeRanges(t, ctx, tc, f.lhs.RangeID)
	close(hold.release)

	select {
	case err := <-changeErr:
		if err == nil || !strings.Contains(err.Error(), "replica change against generation") {
			t.Fatalf("stale replica change: got %v, want refusal", err)
		}
	case <-time.After(60 * time.Second):
		t.Fatal("stale replica change never returned")
	}

	// The members hold the merged descriptor untouched. The spare holds
	// the preseeded replica the refused change left behind — the stale
	// descriptor it was seeded with, a member of nothing — which the next
	// preseed replaces (below).
	spareIdx := -1
	for i, d := range f.localDescriptors(f.lhs.RangeID) {
		if tc.Nodes[i].NodeID() == spare {
			spareIdx = i
			if d.Generation != merged.Generation || !d.EndKey.Equal(f.lhs.EndKey) {
				t.Fatalf("spare n%d holds %v, want the preseeded leftover at generation %d ending at %s", spare, d, merged.Generation, f.lhs.EndKey)
			}
			continue
		}
		if d.Generation != merged.Generation || !d.EndKey.Equal(merged.EndKey) || len(d.Replicas) != len(merged.Replicas) {
			t.Fatalf("node %d holds %v after the refused change, want %v", i, d, merged)
		}
	}
	if spareIdx < 0 {
		t.Fatalf("spare n%d holds no leftover replica; the preseed did not happen", spare)
	}
	if m := f.metaRecord(t, ctx, f.lhs.RangeID); m.Generation != merged.Generation || !m.EndKey.Equal(merged.EndKey) || len(m.Replicas) != len(merged.Replicas) {
		t.Fatalf("/meta holds %v after the refused change, want %v", m, merged)
	}
	f.everyKeyReadable(t, ctx, "after the refused replica change")

	// Built afresh, the addition lands on the merged descriptor.
	resp, err := tc.Nodes[tc.LeaderIndex(f.lhs.RangeID)].DB().AdminChangeReplicas(ctx, f.lhs.StartKey, spare, 0)
	if err != nil {
		t.Fatalf("fresh replica change: %v", err)
	}
	if _, has := resp.Desc.GetReplica(spare); !has || resp.Desc.Generation != merged.Generation+1 || !resp.Desc.EndKey.Equal(merged.EndKey) {
		t.Fatalf("fresh replica change produced %v, want n%d at generation %d ending at %s", resp.Desc, spare, merged.Generation+1, merged.EndKey)
	}
	// The leftover is gone: the spare now holds the range as added.
	if d, ok := tc.Nodes[spareIdx].Store().LocalDescriptor(f.lhs.RangeID); !ok || !sameRangeDescriptor(d, resp.Desc) {
		t.Fatalf("spare n%d holds %v after the fresh change, want %v", spare, d, resp.Desc)
	}
	f.everyKeyReadable(t, ctx, "after the fresh replica change")
}

// TestSplitBuiltBeforeAReplicaChangeIsRefused: a split built before a
// replica is added to the range, applied after. Unrefused, both halves
// would carry the old membership, and the new member would vanish from the
// descriptor while staying in the raft group. The split must be refused,
// the four-replica descriptor must survive, and a split built afresh must
// give both halves all four replicas.
func TestSplitBuiltBeforeAReplicaChangeIsRefused(t *testing.T) {
	hold := newDescriptorHold()
	tc := StartWithOptions(t, 4, func(c *server.Config) {
		c.TestingKnobs.BeforeProposeDescriptorChange = hold.knob
		c.RebalanceThreshold = -1
	})
	ctx, cancel := context.WithTimeout(context.Background(), 180*time.Second)
	defer cancel()
	f := newRaceFixture(t, ctx, tc, 863)
	spare := f.shedReplica(t, ctx)

	hold.arm(f.lhs.RangeID, "split")
	lead := tc.LeaderIndex(f.lhs.RangeID)
	splitErr := make(chan error, 1)
	go func() {
		_, err := tc.Nodes[lead].DB().AdminSplit(ctx, append(f.prefix.Clone(), "a03"...))
		splitErr <- err
	}()
	hold.waitArrived(t, "the split")

	added, err := tc.Nodes[lead].DB().AdminChangeReplicas(ctx, f.lhs.StartKey, spare, 0)
	if err != nil {
		t.Fatalf("replica change under the built split: %v", err)
	}
	if len(added.Desc.Replicas) != 4 {
		t.Fatalf("replica change produced %v, want four replicas", added.Desc)
	}
	close(hold.release)

	select {
	case err := <-splitErr:
		if err == nil || !strings.Contains(err.Error(), "split against generation") {
			t.Fatalf("stale split: got %v, want refusal", err)
		}
	case <-time.After(60 * time.Second):
		t.Fatal("stale split never returned")
	}

	for i, d := range f.localDescriptors(f.lhs.RangeID) {
		if d.Generation != added.Desc.Generation || len(d.Replicas) != 4 || !d.EndKey.Equal(f.lhs.EndKey) {
			t.Fatalf("node %d holds %v after the refused split, want %v", i, d, added.Desc)
		}
	}
	f.everyKeyReadable(t, ctx, "after the refused split")

	sr, err := tc.Nodes[tc.LeaderIndex(f.lhs.RangeID)].DB().AdminSplit(ctx, append(f.prefix.Clone(), "a03"...))
	if err != nil {
		t.Fatalf("fresh split: %v", err)
	}
	if len(sr.Left.Replicas) != 4 || len(sr.Right.Replicas) != 4 || sr.Left.Generation != added.Desc.Generation+1 {
		t.Fatalf("fresh split produced %v / %v, want four replicas each at generation %d", sr.Left, sr.Right, added.Desc.Generation+1)
	}
	f.everyKeyReadable(t, ctx, "after the fresh split")
}

// TestLeftoverOverlappingALiveReplicaIsNotReplaced: the one irreversible
// step a preseed can take — wiping a leftover replica — is refused when the
// leftover's span overlaps a replica the store still serves. A leftover of
// the LHS from before a split covers the new RHS's span; once the node
// holds a live replica of that RHS, a preseed of the LHS must leave the
// leftover alone rather than wipe the RHS's keys with it.
func TestLeftoverOverlappingALiveReplicaIsNotReplaced(t *testing.T) {
	hold := newDescriptorHold()
	tc := StartWithOptions(t, 4, func(c *server.Config) {
		c.TestingKnobs.BeforeProposeDescriptorChange = hold.knob
		c.RebalanceThreshold = -1
	})
	ctx, cancel := context.WithTimeout(context.Background(), 180*time.Second)
	defer cancel()
	f := newRaceFixture(t, ctx, tc, 864)
	spare := f.shedReplica(t, ctx)
	spareIdx := -1
	for i, n := range tc.Nodes {
		if n.NodeID() == spare {
			spareIdx = i
		}
	}

	// An addition of the spare to the LHS, preseeded (with the LHS's
	// whole span) and held; the LHS splits underneath it; the addition is
	// refused and the preseed stays behind as a leftover covering both
	// halves.
	hold.arm(f.lhs.RangeID, "replica-change")
	lead := tc.LeaderIndex(f.lhs.RangeID)
	changeErr := make(chan error, 1)
	go func() {
		_, err := tc.Nodes[lead].DB().AdminChangeReplicas(ctx, f.lhs.StartKey, spare, 0)
		changeErr <- err
	}()
	hold.waitArrived(t, "the replica change")
	sr, err := tc.Nodes[lead].DB().AdminSplit(ctx, append(f.prefix.Clone(), "a03"...))
	if err != nil {
		t.Fatalf("split under the held change: %v", err)
	}
	close(hold.release)
	select {
	case err := <-changeErr:
		if err == nil || !strings.Contains(err.Error(), "replica change against generation") {
			t.Fatalf("stale replica change: got %v, want refusal", err)
		}
	case <-time.After(60 * time.Second):
		t.Fatal("stale replica change never returned")
	}
	leftover, ok := tc.Nodes[spareIdx].Store().LocalDescriptor(f.lhs.RangeID)
	if !ok || !leftover.EndKey.Equal(f.lhs.EndKey) {
		t.Fatalf("spare n%d holds %v (%v), want the preseeded leftover ending at %s", spare, leftover, ok, f.lhs.EndKey)
	}

	// The spare now gains a live replica of the new RHS, whose span the
	// leftover covers.
	if _, err := tc.Nodes[tc.LeaderIndex(sr.Right.RangeID)].DB().AdminChangeReplicas(ctx, sr.Right.StartKey, spare, 0); err != nil {
		t.Fatalf("adding the spare to the RHS: %v", err)
	}
	rhsOnSpare, ok := tc.Nodes[spareIdx].Store().LocalDescriptor(sr.Right.RangeID)
	if !ok || !rhsOnSpare.StartKey.Equal(sr.Right.StartKey) {
		t.Fatalf("spare n%d does not hold the RHS: %v %v", spare, rhsOnSpare, ok)
	}

	// Adding the spare to the LHS now finds the leftover, and the leftover
	// overlaps the live RHS replica: the preseed is refused, the leftover
	// stays, and the RHS's keys still read everywhere.
	_, err = tc.Nodes[tc.LeaderIndex(f.lhs.RangeID)].DB().AdminChangeReplicas(ctx, f.lhs.StartKey, spare, 0)
	if err == nil || !strings.Contains(err.Error(), "overlapping live replica") {
		t.Fatalf("preseed over a leftover overlapping a live replica: got %v, want refusal naming the overlap", err)
	}
	if d, ok := tc.Nodes[spareIdx].Store().LocalDescriptor(f.lhs.RangeID); !ok || !d.EndKey.Equal(f.lhs.EndKey) {
		t.Fatalf("leftover replaced or wiped despite the overlap: %v %v", d, ok)
	}
	if d, ok := tc.Nodes[spareIdx].Store().LocalDescriptor(sr.Right.RangeID); !ok || !sameRangeDescriptor(d, rhsOnSpare) {
		t.Fatalf("the live RHS replica on the spare changed: %v %v", d, ok)
	}
	f.everyKeyReadable(t, ctx, "after the refused preseed")
}

// TestSplitProposedBeforeAFreezeIsRefused: a split of the RHS built and
// about to be proposed when a Subsume freezes the range — adminSplit's
// own frozen check ran before the freeze applied and let it through. At
// apply the split finds the range frozen and must be refused: the merge
// driver has read the frozen descriptor as final, and a split landing
// after it would hand half the span to a new range the merge then
// absorbs as well (seen once in CI on #264's branch under merge churn).
// Unfrozen, the same split goes through.
func TestSplitProposedBeforeAFreezeIsRefused(t *testing.T) {
	hold := newDescriptorHold()
	tc := StartWithOptions(t, 3, func(c *server.Config) { c.TestingKnobs.BeforeProposeDescriptorChange = hold.knob })
	ctx, cancel := context.WithTimeout(context.Background(), 180*time.Second)
	defer cancel()
	f := newRaceFixture(t, ctx, tc, 867)

	hold.arm(f.rhs.RangeID, "split")
	lead := tc.LeaderIndex(f.rhs.RangeID)
	splitKey := append(f.prefix.Clone(), "z07"...)
	splitErr := make(chan error, 1)
	go func() {
		_, err := tc.Nodes[lead].DB().AdminSplit(ctx, splitKey)
		splitErr <- err
	}()
	hold.waitArrived(t, "the split")

	// The freeze lands while the split waits to be proposed.
	rhs, ok := tc.Nodes[lead].Store().GetReplica(f.rhs.RangeID)
	if !ok {
		t.Fatalf("n%d holds no replica of %s", tc.Nodes[lead].NodeID(), f.rhs.RangeID)
	}
	sub := &kvpb.BatchRequest{Header: kvpb.BatchHeader{RangeID: f.rhs.RangeID, Timestamp: tc.Nodes[lead].Clock().Now()}}
	sub.Add(&kvpb.SubsumeRequest{RequestHeader: kvpb.RequestHeader{Key: f.rhs.StartKey, EndKey: f.rhs.EndKey}, MergeInto: f.lhs.RangeID})
	if _, kerr := rhs.Execute(ctx, sub); kerr != nil {
		t.Fatalf("freezing the RHS: %v", kerr)
	}
	close(hold.release)

	select {
	case err := <-splitErr:
		if err == nil || !strings.Contains(err.Error(), "frozen for a merge") {
			t.Fatalf("split applied on a frozen range: got %v, want the frozen refusal", err)
		}
	case <-time.After(60 * time.Second):
		t.Fatal("the held split never returned")
	}
	// Nothing of it landed: every replica holds the frozen descriptor as
	// the merge driver read it.
	for i, d := range f.localDescriptors(f.rhs.RangeID) {
		if d.Generation != f.rhs.Generation || !d.EndKey.Equal(f.rhs.EndKey) {
			t.Fatalf("node %d holds %v after the refused split, want %v", i, d, f.rhs)
		}
	}

	// Unfrozen (an abandoned merge), the same split goes through.
	unf := &kvpb.BatchRequest{Header: kvpb.BatchHeader{RangeID: f.rhs.RangeID, Timestamp: tc.Nodes[lead].Clock().Now()}}
	unf.Add(&kvpb.UnfreezeRequest{RequestHeader: kvpb.RequestHeader{Key: f.rhs.StartKey, EndKey: f.rhs.EndKey}})
	if _, kerr := rhs.Execute(ctx, unf); kerr != nil {
		t.Fatalf("unfreezing the RHS: %v", kerr)
	}
	sr, err := tc.Nodes[lead].DB().AdminSplit(ctx, splitKey)
	if err != nil {
		t.Fatalf("split after the unfreeze: %v", err)
	}
	if sr.Left.Generation != f.rhs.Generation+1 || !sr.Right.EndKey.Equal(f.rhs.EndKey) {
		t.Fatalf("split produced %v / %v, want generation %d ending at %s", sr.Left, sr.Right, f.rhs.Generation+1, f.rhs.EndKey)
	}
	f.everyKeyReadable(t, ctx, "after the split")
}
