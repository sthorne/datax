package kvserver

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/sthorne/datax/pkg/base"
	"github.com/sthorne/datax/pkg/keys"
	"github.com/sthorne/datax/pkg/util/hlc"
)

// TestPurgeReportsRangeAfterSplit: a split that applies between a
// purge's bounds check and its collection takes the right-hand side away
// with the threshold it had then. The leader reports the range as the
// collection left it, so the purging node can see the part it did not
// reach (pkg/server purgeSpan purges that part too).
func TestPurgeReportsRangeAfterSplit(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	start, end := keys.TableDataSpan(1)
	mid := append(start.Clone(), 'm')
	var env *routerEnv
	var armed atomic.Bool
	var splitErr atomic.Value
	env = startRouterStore(t, TestingKnobs{
		BeforePurgeCollect: func(base.RangeID) {
			if armed.CompareAndSwap(true, false) {
				if _, kerr := env.split(ctx, mid); kerr != nil {
					splitErr.Store(kerr.Error())
				}
			}
		},
	})
	sr, kerr := env.split(ctx, start)
	if kerr != nil {
		t.Fatal(kerr)
	}
	if _, kerr := env.split(ctx, end); kerr != nil {
		t.Fatal(kerr)
	}
	r, ok := env.store.GetReplica(sr.Right.RangeID)
	if !ok {
		t.Fatal("no replica for the table range")
	}
	if d := r.Desc(); !d.StartKey.Equal(start) || !d.EndKey.Equal(end) {
		t.Fatalf("table range spans %s to %s", keys.Pretty(d.StartKey), keys.Pretty(d.EndKey))
	}
	env.put(t, append(start.Clone(), 'a'), "a")
	env.put(t, append(start.Clone(), 'z'), "z")

	armed.Store(true)
	res, err := env.store.PurgeRangeBelow(ctx, r.rangeID, start, end, hlc.Timestamp{})
	if err != nil {
		t.Fatal(err)
	}
	if v := splitErr.Load(); v != nil {
		t.Fatalf("split during the purge: %v", v)
	}
	if !res.Desc.EndKey.Equal(mid) {
		t.Fatalf("purge reported the range ending at %s, want %s (the range after the split)", keys.Pretty(res.Desc.EndKey), keys.Pretty(mid))
	}
	// Before the fix the leader reported the range as it checked it, and
	// the right-hand side, holding the pre-purge threshold, went unseen.
	var right *Replica
	env.store.VisitReplicas(func(x *Replica) bool {
		if x.Desc().StartKey.Equal(mid) {
			right = x
			return false
		}
		return true
	})
	if right == nil {
		t.Fatal("the split did not make a right-hand range")
	}
	if thr := right.GCThreshold(); res.Threshold.LessEq(thr) {
		t.Fatalf("right-hand threshold %s is at the collection's %s: the split applied after the collection", thr, res.Threshold)
	}
}
