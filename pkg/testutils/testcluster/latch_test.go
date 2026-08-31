package testcluster

import (
	"context"
	"net"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/sthorne/datax/pkg/base"
	"github.com/sthorne/datax/pkg/cluster"
	"github.com/sthorne/datax/pkg/keys"
	"github.com/sthorne/datax/pkg/kvpb"
	"github.com/sthorne/datax/pkg/kvserver"
	"github.com/sthorne/datax/pkg/server"
)

// TestLatchDisjointParallelism proves per-key latches: a write that stalls
// while HOLDING its latch (via the AfterLatch knob) blocks an overlapping
// read but not a disjoint one.
func TestLatchDisjointParallelism(t *testing.T) {
	hot := append(keys.TableDataPrefix(600), "hot"...)
	cold := append(keys.TableDataPrefix(600), "cold"...)

	var stall atomic.Bool
	release := make(chan struct{})
	knobs := kvserver.TestingKnobs{
		AfterLatch: func(ba *kvpb.BatchRequest) {
			if !stall.Load() || ba.IsReadOnly() || len(ba.Requests) != 1 {
				return
			}
			if put := ba.Requests[0].Put; put != nil && put.Key.Equal(hot) {
				<-release
			}
		},
	}

	// Single node with knobs (build it directly; testcluster.Start has no
	// knob plumbing and one node suffices).
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	clusterID := uuid.New()
	n, err := server.Start(server.Config{
		Listener:     lis,
		TestingKnobs: knobs,
		StaticBootstrap: &server.StaticBootstrap{
			ClusterID: clusterID,
			NodeID:    1,
			Range1:    cluster.Range1Descriptor([]base.NodeID{1}),
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer n.Stop()
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	db := n.DB()

	if err := db.Put(ctx, hot, []byte("v0")); err != nil {
		t.Fatal(err)
	}
	if err := db.Put(ctx, cold, []byte("c0")); err != nil {
		t.Fatal(err)
	}

	stall.Store(true)
	writeDone := make(chan error, 1)
	go func() { writeDone <- db.Put(ctx, hot, []byte("v1")) }()

	// Wait until the stalled write holds its latch.
	time.Sleep(200 * time.Millisecond)

	// Disjoint read proceeds while the write's latch is held.
	fast := make(chan error, 1)
	go func() {
		_, err := db.Get(ctx, cold)
		fast <- err
	}()
	select {
	case err := <-fast:
		if err != nil {
			t.Fatalf("disjoint read failed: %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("disjoint read blocked behind an unrelated write latch")
	}

	// Overlapping read blocks until the write releases.
	slow := make(chan error, 1)
	go func() {
		_, err := db.Get(ctx, hot)
		slow <- err
	}()
	select {
	case <-slow:
		t.Fatal("overlapping read did not block behind the write latch")
	case <-time.After(300 * time.Millisecond):
	}

	stall.Store(false)
	close(release)
	if err := <-writeDone; err != nil {
		t.Fatalf("stalled write: %v", err)
	}
	select {
	case err := <-slow:
		if err != nil {
			t.Fatalf("overlapping read after release: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("overlapping read never completed after release")
	}
	v, err := db.Get(ctx, hot)
	if err != nil || string(v) != "v1" {
		t.Fatalf("final read: %q %v", v, err)
	}
}
