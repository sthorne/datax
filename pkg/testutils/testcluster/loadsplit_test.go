package testcluster

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/sthorne/datax/pkg/base"
	"github.com/sthorne/datax/pkg/keys"
	"github.com/sthorne/datax/pkg/server"
)

// TestLoadBasedSplitAndMergeGuard: a range sustaining QPS above the load
// threshold splits even though it is far below the size threshold; the
// merge pass refuses to undo the split while the range is hot or freshly
// split; once the load subsides and the settle window passes, the halves
// merge back.
func TestLoadBasedSplitAndMergeGuard(t *testing.T) {
	var fakeNow atomic.Int64
	fakeNow.Store(time.Now().UnixNano())

	var hotMu sync.Mutex
	hot := map[base.RangeID]float64{}
	setHot := func(id base.RangeID, qps float64) {
		hotMu.Lock()
		defer hotMu.Unlock()
		if qps == 0 {
			delete(hot, id)
		} else {
			hot[id] = qps
		}
	}

	tc, _ := StartWithEngines(t, 3, func(c *server.Config) {
		c.SplitSizeThreshold = 1 << 30 // size never triggers
		c.LoadSplitThreshold = 100
		c.TestingKnobs.LoadNowNanos = fakeNow.Load
		c.TestingKnobs.OverrideReplicaQPS = func(id base.RangeID) (float64, bool) {
			hotMu.Lock()
			defer hotMu.Unlock()
			q, ok := hot[id]
			return q, ok
		}
	})
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()
	db := tc.Nodes[0].DB()

	// Real rows so the midpoint fallback (and post-split reads) have data.
	prefix := keys.TableDataPrefix(810)
	for i := 0; i < 60; i++ {
		k := append(prefix.Clone(), fmt.Sprintf("row-%02d", i)...)
		if err := db.Put(ctx, k, []byte("v")); err != nil {
			t.Fatal(err)
		}
	}

	rangeCount := func() int {
		descs, err := tc.ranges(ctx)
		if err != nil {
			return -1
		}
		return len(descs)
	}
	tableRange := func() base.RangeID {
		descs, err := tc.ranges(ctx)
		if err != nil {
			t.Fatal(err)
		}
		for _, d := range descs {
			if d.StartKey.Compare(prefix) <= 0 && prefix.Compare(d.EndKey) < 0 {
				return d.RangeID
			}
		}
		t.Fatal("no range contains the table prefix")
		return 0
	}

	before := rangeCount()
	target := tableRange()

	// Below threshold: no split.
	setHot(target, 50)
	for _, n := range tc.Nodes {
		n.Store().RunAutoSplitOnce(ctx)
	}
	if got := rangeCount(); got != before {
		t.Fatalf("sub-threshold load split a range: %d -> %d", before, got)
	}

	// Above threshold: the leader's pass splits it.
	setHot(target, 500)
	deadline := time.Now().Add(30 * time.Second)
	for rangeCount() <= before {
		if time.Now().After(deadline) {
			t.Fatal("hot range never load-split")
		}
		for _, n := range tc.Nodes {
			n.Store().RunAutoSplitOnce(ctx)
		}
		time.Sleep(100 * time.Millisecond)
	}

	// The new boundary lies inside the table span, and all rows survive.
	descs, err := tc.ranges(ctx)
	if err != nil {
		t.Fatal(err)
	}
	boundaryInside := false
	for _, d := range descs {
		if d.StartKey.Compare(prefix) > 0 && d.StartKey.Compare(prefix.PrefixEnd()) < 0 {
			boundaryInside = true
		}
	}
	if !boundaryInside {
		t.Fatalf("no split boundary inside the table span: %+v", descs)
	}
	rows, err := db.Scan(ctx, prefix, prefix.PrefixEnd(), 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 60 {
		t.Fatalf("rows after load split: %d, want 60", len(rows))
	}
	split := rangeCount()

	// Merge guard, case 1: still hot — the pair must not re-merge.
	for i := 0; i < 5; i++ {
		for _, n := range tc.Nodes {
			n.Store().RunRangeMergeOnce(ctx)
		}
	}
	if got := rangeCount(); got < split {
		t.Fatalf("hot pair was re-merged: %d -> %d", split, got)
	}

	// Merge guard, case 2: load gone but inside the settle window — the
	// fresh-split stamp still protects the halves.
	setHot(target, 0)
	for i := 0; i < 5; i++ {
		for _, n := range tc.Nodes {
			n.Store().RunRangeMergeOnce(ctx)
		}
	}
	if got := rangeCount(); got < split {
		t.Fatalf("freshly-split pair was re-merged inside the settle window: %d -> %d", split, got)
	}

	// Cold and settled: the halves merge back (the pass may need several
	// rounds to pull RHS leadership to the LHS's node first).
	fakeNow.Add((5 * time.Minute).Nanoseconds())
	deadline = time.Now().Add(45 * time.Second)
	for rangeCount() > before {
		if time.Now().After(deadline) {
			t.Fatalf("cold halves never merged back: %d ranges, want %d", rangeCount(), before)
		}
		for _, n := range tc.Nodes {
			n.Store().RunRangeMergeOnce(ctx)
		}
		time.Sleep(100 * time.Millisecond)
	}

	// Data intact after the round trip.
	rows, err = db.Scan(ctx, prefix, prefix.PrefixEnd(), 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 60 {
		t.Fatalf("rows after re-merge: %d, want 60", len(rows))
	}
}
