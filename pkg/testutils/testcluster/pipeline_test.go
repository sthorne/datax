package testcluster

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"

	"github.com/sthorne/datax/pkg/base"
	"github.com/sthorne/datax/pkg/keys"
	"github.com/sthorne/datax/pkg/metrics"
	"github.com/sthorne/datax/pkg/server"
)

// TestPipelinedApplyBacklog (issue #106): committed entries apply off
// the raft pass, in log order, and a replica whose apply queue is over
// its bound is given no pass until the queue drains — the raft side
// keeps committing meanwhile, nothing is lost or reordered, and every
// proposer is answered once its entry applies.
func TestPipelinedApplyBacklog(t *testing.T) {
	var (
		holdRange atomic.Int64 // entries of this range block at apply while held
		release   = make(chan struct{})
		held      atomic.Int64
	)
	tc, _ := StartWithEngines(t, 1, func(c *server.Config) {
		c.TestingKnobs.ApplyQueueMaxBytes = 1 // any queued entry is over the bound
		c.TestingKnobs.FailApply = func(id base.RangeID, _ uint64) error {
			if int64(id) == holdRange.Load() {
				held.Add(1)
				<-release
			}
			return nil
		}
	})
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()
	db := tc.Nodes[0].DB()
	prefix := keys.TableDataPrefix(907)
	key := func(i int) keys.Key { return append(prefix.Clone(), fmt.Sprintf("k%03d", i)...) }
	if err := db.Put(ctx, key(0), []byte("v0")); err != nil {
		t.Fatal(err)
	}
	sr, err := db.AdminSplit(ctx, prefix)
	if err != nil {
		t.Fatal(err)
	}
	rangeID := sr.Right.RangeID
	store := tc.Nodes[0].Store()

	// Hold the range's applies: the first write commits in the raft log
	// and blocks at apply; the writes after it find the replica over its
	// bound, so their passes are deferred and their proposers wait.
	holdRange.Store(int64(rangeID))
	deferredBefore := testutil.ToFloat64(metrics.RaftApplyBackpressure)
	const n = 20
	var wg sync.WaitGroup
	errs := make([]error, n)
	put := func(i int) {
		defer wg.Done()
		errs[i-1] = db.Put(ctx, key(i), []byte(fmt.Sprintf("v%d", i)))
	}
	wg.Add(1)
	go put(1)
	deadline := time.Now().Add(30 * time.Second)
	for held.Load() == 0 {
		if time.Now().After(deadline) {
			t.Fatal("the first write never reached apply")
		}
		time.Sleep(5 * time.Millisecond)
	}
	for i := 2; i <= n; i++ {
		wg.Add(1)
		go put(i)
	}
	for testutil.ToFloat64(metrics.RaftApplyBackpressure) == deferredBefore {
		if time.Now().After(deadline) {
			t.Fatal("no pass was deferred while the apply was held")
		}
		time.Sleep(5 * time.Millisecond)
	}
	r, ok := store.GetReplica(rangeID)
	if !ok {
		t.Fatal("range gone")
	}
	applied := r.AppliedIndex()
	if r.LastIndex() <= applied {
		t.Fatalf("the log did not run ahead of the apply: last %d, applied %d", r.LastIndex(), applied)
	}
	t.Logf("held at applied index %d with the log at %d, %d entries queued, %0.f passes deferred",
		applied, r.LastIndex(), store.TestingApplyQueueLen(rangeID),
		testutil.ToFloat64(metrics.RaftApplyBackpressure)-deferredBefore)

	// Release: everything applies in order and every proposer is answered.
	holdRange.Store(0)
	close(release)
	wg.Wait()
	for i, err := range errs {
		if err != nil {
			t.Fatalf("write %d: %v", i+1, err)
		}
	}
	for i := 0; i <= n; i++ {
		v, err := db.Get(ctx, key(i))
		if err != nil || string(v) != fmt.Sprintf("v%d", i) {
			t.Fatalf("key %d after the backlog drained: %q, %v", i, v, err)
		}
	}
	if r.AppliedIndex() != r.LastIndex() {
		t.Fatalf("applied %d, last %d after the drain", r.AppliedIndex(), r.LastIndex())
	}
	if q := store.TestingApplyQueueLen(rangeID); q != 0 {
		t.Fatalf("%d entries still queued", q)
	}
}
