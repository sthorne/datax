package testcluster

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/sthorne/datax/pkg/base"
	"github.com/sthorne/datax/pkg/keys"
	"github.com/sthorne/datax/pkg/kvpb"
	"github.com/sthorne/datax/pkg/server"
)

// TestTransferLease: leadership of a range moves to the requested follower
// and the range keeps serving through any gateway. Regression test for
// issue #6.
func TestTransferLease(t *testing.T) {
	tc := Start(t, 3)
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	lead := tc.LeaderIndex(1)
	target := (lead + 1) % 3
	targetID := tc.Nodes[target].NodeID()

	key := append(keys.TableDataPrefix(800), "k"...)
	if err := tc.Nodes[lead].DB().Put(ctx, key, []byte("before")); err != nil {
		t.Fatal(err)
	}

	if err := tc.Nodes[lead].DB().AdminTransferLease(ctx, keys.MinKey, targetID); err != nil {
		t.Fatalf("transfer: %v", err)
	}
	if got := tc.LeaderIndex(1); got != target {
		t.Fatalf("leader is node %d, want %d", got, target)
	}

	// Serving continues through every gateway, and a transfer back works.
	for i, n := range tc.Nodes {
		if v, err := n.DB().Get(ctx, key); err != nil || string(v) != "before" {
			t.Fatalf("read via node %d: %q, %v", i, v, err)
		}
	}
	if err := tc.Nodes[target].DB().Put(ctx, key, []byte("after")); err != nil {
		t.Fatal(err)
	}
	if err := tc.Nodes[0].DB().AdminTransferLease(ctx, keys.MinKey, tc.Nodes[lead].NodeID()); err != nil {
		t.Fatalf("transfer back: %v", err)
	}
	if got := tc.LeaderIndex(1); got != lead {
		t.Fatalf("leader is node %d, want %d back", got, lead)
	}
	if v, err := tc.Nodes[0].DB().Get(ctx, key); err != nil || string(v) != "after" {
		t.Fatalf("read after transfer back: %q, %v", v, err)
	}
}

// TestTransferLeaseThenRemoveOldLeader: the previously impossible operation —
// moving a replica off the range's own leader — now works via transfer.
func TestTransferLeaseThenRemoveOldLeader(t *testing.T) {
	// 4 static nodes: range 1 starts with a replica on every node, so
	// removing one still leaves a healthy RF-3 range.
	//
	// That fourth replica is a surplus the allocator repairs on its own:
	// every tick, the range-1 leader removes one replica of any range
	// over its factor, choosing the lowest node id when nothing else
	// tells them apart — n1, which is also where range 1's leader
	// usually is — and it does so by exactly the sequence this test
	// performs, a transfer and then a remove (issue #200). The two
	// raced: on a quiet machine the test won inside the first one-second
	// tick, and under load the allocator did, so the test's own remove
	// found "n1 has no replica to remove". The balancing passes are off
	// here; the test is about the transfer-then-remove sequence, which
	// it now performs alone.
	tc := StartWithOptions(t, 4, func(cfg *server.Config) { cfg.RebalanceThreshold = -1 })
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	lead := tc.LeaderIndex(1)
	leadID := tc.Nodes[lead].NodeID()
	next := tc.Nodes[(lead+1)%4].NodeID()
	db := tc.Nodes[(lead+1)%4].DB()

	// Direct remove of the leader's replica is refused; after a transfer it
	// works — the moveReplica sequence.
	if _, err := db.AdminChangeReplicas(ctx, keys.MinKey, 0, leadID); err == nil {
		t.Fatal("removing the leader's own replica succeeded without a transfer")
	}
	if err := db.AdminTransferLease(ctx, keys.MinKey, next); err != nil {
		t.Fatalf("transfer: %v", err)
	}
	if _, err := db.AdminChangeReplicas(ctx, keys.MinKey, 0, leadID); err != nil {
		t.Fatalf("removing old leader after transfer: %v", err)
	}

	// The range still serves and the old leader no longer holds a replica.
	if err := db.Put(ctx, append(keys.TableDataPrefix(801), "x"...), []byte("v")); err != nil {
		t.Fatal(err)
	}
	if _, ok := tc.Nodes[lead].Store().GetReplica(1); ok {
		t.Fatal("old leader still has a range-1 replica")
	}
}

// TestTransferLeaseInvalidTarget: a target without a replica errors; the
// current leader as target is a no-op.
func TestTransferLeaseInvalidTarget(t *testing.T) {
	tc := Start(t, 3)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	lead := tc.LeaderIndex(1)
	db := tc.Nodes[lead].DB()
	if err := db.AdminTransferLease(ctx, keys.MinKey, base.NodeID(99)); err == nil {
		t.Fatal("transfer to a node without a replica succeeded")
	}
	if err := db.AdminTransferLease(ctx, keys.MinKey, tc.Nodes[lead].NodeID()); err != nil {
		t.Fatalf("self-transfer no-op: %v", err)
	}
	if got := tc.LeaderIndex(1); got != lead {
		t.Fatalf("leader moved on a no-op transfer: %d", got)
	}
}

// TestTransferLeaseUnderLoad: transfers race a continuous writer; writes
// acknowledged as successful are never lost.
func TestTransferLeaseUnderLoad(t *testing.T) {
	tc := Start(t, 3)
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	db := tc.Nodes[0].DB()
	prefix := keys.TableDataPrefix(802)
	acked := make(map[int]bool)
	stop := make(chan struct{})
	done := make(chan struct{})
	go func() {
		defer close(done)
		for i := 0; ; i++ {
			select {
			case <-stop:
				return
			default:
			}
			k := append(prefix.Clone(), fmt.Sprintf("k%04d", i)...)
			if err := db.Put(ctx, k, []byte("v")); err == nil {
				acked[i] = true
			}
		}
	}()

	for round := 0; round < 4; round++ {
		lead := tc.LeaderIndex(1)
		target := tc.Nodes[(lead+1)%3].NodeID()
		if err := tc.Nodes[lead].DB().AdminTransferLease(ctx, keys.MinKey, target); err != nil {
			t.Logf("transfer round %d: %v (retryable)", round, err)
		}
		time.Sleep(200 * time.Millisecond)
	}
	close(stop)
	<-done

	for i := range acked {
		k := append(prefix.Clone(), fmt.Sprintf("k%04d", i)...)
		if v, err := db.Get(ctx, k); err != nil || string(v) != "v" {
			t.Fatalf("acknowledged write k%04d lost: %q, %v", i, v, err)
		}
	}
	if len(acked) == 0 {
		t.Fatal("no writes succeeded during the transfer storm")
	}
}

// TestWritesInFlightAcrossAPlannedTransferAreSettled (issue #271): a
// write proposed just before a lease transfer is still waiting for its
// apply when the old leader steps down. For a transfer this replica
// asked for, that entry is in the new leader's log — raft hands over
// only to a follower that holds the whole log — so its proposer is
// answered from the entry's own apply a moment later, not told at once
// that its outcome is unknown. Writers hammer the leader's replica
// directly while the lease goes round the cluster; none may see
// "leadership lost with proposal in flight".
func TestWritesInFlightAcrossAPlannedTransferAreSettled(t *testing.T) {
	tc := Start(t, 3)
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()
	prefix := keys.TableDataPrefix(803)
	var (
		mu        sync.Mutex
		ambiguous []string
		applied   int
	)
	stop := make(chan struct{})
	var wg sync.WaitGroup
	for w := 0; w < 8; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for i := 0; ; i++ {
				select {
				case <-stop:
					return
				default:
				}
				lead := tc.LeaderIndex(1)
				rep, ok := tc.Nodes[lead].Store().GetReplica(1)
				if !ok {
					continue
				}
				ba := &kvpb.BatchRequest{Header: kvpb.BatchHeader{RangeID: 1, Timestamp: tc.Nodes[lead].Clock().Now()}}
				ba.Add(&kvpb.PutRequest{RequestHeader: kvpb.RequestHeader{Key: append(prefix.Clone(), fmt.Sprintf("w%d-%06d", w, i)...)}, Value: []byte("v")})
				_, kerr := rep.Execute(ctx, ba)
				mu.Lock()
				switch {
				case kerr == nil:
					applied++
				case kerr.Ambiguous != nil:
					ambiguous = append(ambiguous, kerr.Message)
				}
				mu.Unlock()
			}
		}(w)
	}
	transfers := 0
	for round := 0; round < 30; round++ {
		lead := tc.LeaderIndex(1)
		target := tc.Nodes[(lead+1)%3].NodeID()
		if err := tc.Nodes[lead].DB().AdminTransferLease(ctx, keys.MinKey, target); err == nil {
			transfers++
		}
		time.Sleep(50 * time.Millisecond)
	}
	close(stop)
	wg.Wait()
	if transfers < 10 || applied == 0 {
		t.Fatalf("%d transfers and %d writes: the test did not exercise what it is about", transfers, applied)
	}
	if len(ambiguous) > 0 {
		t.Fatalf("%d of %d writes were answered with an unknown outcome across %d planned transfers; first: %s", len(ambiguous), len(ambiguous)+applied, transfers, ambiguous[0])
	}
}
