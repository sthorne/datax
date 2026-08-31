package testcluster

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/sthorne/datax/pkg/base"
	"github.com/sthorne/datax/pkg/keys"
	"github.com/sthorne/datax/pkg/server"
	"github.com/sthorne/datax/pkg/storage"
	"github.com/sthorne/datax/pkg/util/hlc"
)

// TestConsistencyChecker: identical replicas pass the checksum probe; a
// rogue key planted directly in one follower's engine — exactly the
// corruption the checker exists to catch — fails it.
func TestConsistencyChecker(t *testing.T) {
	tc, engines := StartWithEngines(t, 3)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	db := tc.Nodes[0].DB()

	prefix := keys.TableDataPrefix(910)
	for i := 0; i < 50; i++ {
		k := append(prefix.Clone(), fmt.Sprintf("row-%03d", i)...)
		if err := db.Put(ctx, k, []byte("payload")); err != nil {
			t.Fatal(err)
		}
	}

	leader := tc.LeaderIndex(1)
	mismatch, err := tc.Nodes[leader].CheckRangeConsistency(ctx, 1)
	if err != nil {
		t.Fatalf("consistency check: %v", err)
	}
	if mismatch {
		t.Fatal("healthy replicas reported divergent")
	}

	// Plant corruption: a raw engine write on a follower, inside range 1's
	// MVCC data span, bypassing raft entirely.
	follower := (leader + 1) % 3
	rogue := storage.EncodeMVCCKey(append(prefix.Clone(), "rogue"...), hlc.Timestamp{WallTime: 12345})
	b := engines[follower].NewBatch()
	if err := b.Put(rogue, []byte("bitrot")); err != nil {
		t.Fatal(err)
	}
	if err := b.Commit(true); err != nil {
		t.Fatal(err)
	}

	mismatch, err = tc.Nodes[leader].CheckRangeConsistency(ctx, 1)
	if err != nil {
		t.Fatalf("consistency check after corruption: %v", err)
	}
	if !mismatch {
		t.Fatal("planted corruption went undetected")
	}
}

// TestLoadHandoffOnLeaseTransfer: a warm, mature load tracker's rate rides
// the lease transfer and seeds the new leader — its LoadSummary reports
// the range's real QPS immediately, without waiting out a maturity window.
func TestLoadHandoffOnLeaseTransfer(t *testing.T) {
	var fakeNow atomic.Int64
	fakeNow.Store(time.Now().UnixNano())
	tc, _ := StartWithEngines(t, 3, func(c *server.Config) {
		c.TestingKnobs.LoadNowNanos = fakeNow.Load
	})
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	leader := tc.LeaderIndex(1)
	db := tc.Nodes[leader].DB()
	prefix := keys.TableDataPrefix(911)

	// Warm the tracker: real requests spread over (fake) time, then a full
	// window rotation to make it mature.
	warm := func(n int) {
		for i := 0; i < n; i++ {
			k := append(prefix.Clone(), fmt.Sprintf("k-%03d", i)...)
			if err := db.Put(ctx, k, []byte("v")); err != nil {
				t.Fatal(err)
			}
			if i%50 == 0 {
				fakeNow.Add((500 * time.Millisecond).Nanoseconds())
			}
		}
	}
	warm(500)
	fakeNow.Add((11 * time.Second).Nanoseconds()) // rotate: previous window full
	warm(200)                                     // current window traffic keeps the rate visible

	pre := tc.Nodes[leader].Store().LoadSummary(8)
	if pre.LeaderQPS <= 0 {
		t.Fatalf("source tracker not warm: %+v", pre)
	}

	// Transfer range 1's lease to another node.
	target := (leader + 1) % 3
	if err := db.AdminTransferLease(ctx, keys.MinKey, base.NodeID(target+1)); err != nil {
		t.Fatalf("transfer: %v", err)
	}
	deadline := time.Now().Add(15 * time.Second)
	for tc.LeaderIndex(1) != target {
		if time.Now().After(deadline) {
			t.Fatal("lease transfer did not land")
		}
	}

	// WITHOUT advancing the fake clock through another 10s window, the new
	// leader must already report a mature, non-trivial rate — the handoff.
	post := tc.Nodes[target].Store().LoadSummary(8)
	if post.LeaderQPS <= 0 {
		t.Fatalf("new leader is load-amnesiac after transfer: %+v (pre-transfer source had %.1f QPS)",
			post, pre.LeaderQPS)
	}
}
