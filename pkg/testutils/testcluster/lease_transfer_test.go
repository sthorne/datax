package testcluster

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/sthorne/datax/pkg/base"
	"github.com/sthorne/datax/pkg/keys"
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
	tc := Start(t, 4)
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
