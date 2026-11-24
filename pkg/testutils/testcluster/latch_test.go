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

// startLatchNode starts one node with the given knobs and closed-timestamp
// settings.
func startLatchNode(t *testing.T, knobs kvserver.TestingKnobs, lag, interval time.Duration) *server.Node {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	n, err := server.Start(server.Config{
		Listener:                lis,
		TestingKnobs:            knobs,
		ClosedTimestampLag:      lag,
		ClosedTimestampInterval: interval,
		StaticBootstrap: &server.StaticBootstrap{
			ClusterID: uuid.New(),
			NodeID:    1,
			Range1:    cluster.Range1Descriptor([]base.NodeID{1}),
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(n.Stop)
	return n
}

// stallKeys holds, in AfterLatch, every single-Put batch on a key in the
// set, until the key's channel is closed.
type stallKeys struct {
	mu   chan struct{} // 1-slot lock
	keys map[string]chan struct{}
}

func newStallKeys() *stallKeys {
	return &stallKeys{mu: make(chan struct{}, 1), keys: map[string]chan struct{}{}}
}

func (s *stallKeys) hold(k keys.Key) (release func()) {
	ch := make(chan struct{})
	s.mu <- struct{}{}
	s.keys[string(k)] = ch
	<-s.mu
	return func() {
		s.mu <- struct{}{}
		delete(s.keys, string(k))
		<-s.mu
		close(ch)
	}
}

func (s *stallKeys) afterLatch(ba *kvpb.BatchRequest) {
	if ba.IsReadOnly() || len(ba.Requests) != 1 || ba.Requests[0].Put == nil {
		return
	}
	s.mu <- struct{}{}
	ch := s.keys[string(ba.Requests[0].Put.Key)]
	<-s.mu
	if ch != nil {
		<-ch
	}
}

func replicaFor(t *testing.T, n *server.Node, k keys.Key) *kvserver.Replica {
	t.Helper()
	var found *kvserver.Replica
	n.Store().VisitReplicas(func(r *kvserver.Replica) bool {
		if d := r.Desc(); d.ContainsKey(k) {
			found = r
			return false
		}
		return true
	})
	if found == nil {
		t.Fatalf("no replica holds %s", k)
	}
	return found
}

// TestLatchClosedTimestampPassesCurrentWrites: a closed-timestamp promise
// at T drains only the writes that could land at or below T. A write in
// flight at a current timestamp (above T, which trails now by the lag)
// does not hold it up: before, the promise waited for every write on the
// range, so a range with a write always in flight published nothing, each
// attempt waiting out its five-second deadline. A write in flight at or
// below T still holds the promise back until it has applied.
func TestLatchClosedTimestampPassesCurrentWrites(t *testing.T) {
	stall := newStallKeys()
	n := startLatchNode(t, kvserver.TestingKnobs{AfterLatch: stall.afterLatch}, 2*time.Second, 50*time.Millisecond)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	db := n.DB()
	k := append(keys.TableDataPrefix(601), "w"...)
	if err := db.Put(ctx, k, []byte("v0")); err != nil {
		t.Fatal(err)
	}
	r := replicaFor(t, n, k)
	eventually(t, 10*time.Second, "a first closed timestamp", func() bool { return !r.ClosedTimestamp().IsEmpty() })

	// A write at now, held while latched for a second: the closed
	// timestamp keeps advancing meanwhile (it stays 2s behind the write).
	release := stall.hold(k)
	done := make(chan error, 1)
	go func() { done <- db.Put(ctx, k, []byte("v1")) }()
	time.Sleep(200 * time.Millisecond)
	before := r.ClosedTimestamp()
	time.Sleep(time.Second)
	after := r.ClosedTimestamp()
	release()
	if err := <-done; err != nil {
		t.Fatalf("held write: %v", err)
	}
	if !before.Less(after) || after.WallTime-before.WallTime < int64(500*time.Millisecond) {
		t.Fatalf("closed timestamp moved from %s to %s in 1s with a current write in flight; it should keep publishing", before, after)
	}

	// A write at a timestamp the promise is about to pass, held: the
	// closed timestamp must stay below it until the write has applied.
	wts := n.Clock().Now()
	release = stall.hold(k)
	go func() {
		ba := &kvpb.BatchRequest{Header: kvpb.BatchHeader{Timestamp: wts}}
		ba.Add(&kvpb.PutRequest{RequestHeader: kvpb.RequestHeader{Key: k}, Value: []byte("old")})
		_, kerr := db.Send(ctx, ba)
		if kerr != nil {
			done <- kerr
			return
		}
		done <- nil
	}()
	deadline := time.Now().Add(3 * time.Second) // the lag, and a second past it
	for time.Now().Before(deadline) {
		if c := r.ClosedTimestamp(); !c.Less(wts) {
			release()
			t.Fatalf("closed timestamp %s reached a write at %s still in flight", c, wts)
		}
		time.Sleep(20 * time.Millisecond)
	}
	release()
	<-done // its deadline may have passed while it was held: no matter
	eventually(t, 5*time.Second, "closed timestamp passes the write once it applied", func() bool { return wts.Less(r.ClosedTimestamp()) })
}

// TestLatchSplitLeavesLeftSideRunning: a split latches only its right-hand
// side. A write held in flight on the left does not delay the split (and
// a left-hand write issued while the split waits is not delayed either);
// a write held on the right does, and the split lands once it applies,
// with the write on the side it was sent to.
func TestLatchSplitLeavesLeftSideRunning(t *testing.T) {
	stall := newStallKeys()
	n := startLatchNode(t, kvserver.TestingKnobs{AfterLatch: stall.afterLatch}, 0, 0)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	db := n.DB()
	pfx := keys.TableDataPrefix(602)
	k := func(s string) keys.Key { return append(append(keys.Key(nil), pfx...), s...) }
	if _, err := db.AdminSplit(ctx, pfx); err != nil {
		t.Fatal(err)
	}
	for _, s := range []string{"c", "q"} {
		if err := db.Put(ctx, k(s), []byte("v0")); err != nil {
			t.Fatal(err)
		}
	}

	// Left side held: the split at "m" lands while the write is in flight.
	release := stall.hold(k("c"))
	done := make(chan error, 1)
	go func() { done <- db.Put(ctx, k("c"), []byte("v1")) }()
	time.Sleep(200 * time.Millisecond)
	split := make(chan error, 1)
	go func() {
		_, err := db.AdminSplit(ctx, k("m"))
		split <- err
	}()
	select {
	case err := <-split:
		if err != nil {
			t.Fatalf("split: %v", err)
		}
	case <-time.After(5 * time.Second):
		release()
		t.Fatal("a split waited for a write on its left-hand side")
	}
	release()
	if err := <-done; err != nil {
		t.Fatalf("held left write: %v", err)
	}

	// Right side held: the split at "p" waits for the write at "q".
	release = stall.hold(k("q"))
	go func() { done <- db.Put(ctx, k("q"), []byte("v1")) }()
	time.Sleep(200 * time.Millisecond)
	go func() {
		_, err := db.AdminSplit(ctx, k("p"))
		split <- err
	}()
	select {
	case err := <-split:
		release()
		t.Fatalf("a split landed with a write in flight on its right-hand side (err %v)", err)
	case <-time.After(500 * time.Millisecond):
	}
	// The left-hand side keeps serving meanwhile.
	if err := db.Put(ctx, k("n"), []byte("v")); err != nil {
		t.Fatal(err)
	}
	release()
	if err := <-done; err != nil {
		t.Fatalf("held right write: %v", err)
	}
	if err := <-split; err != nil {
		t.Fatalf("split after the write applied: %v", err)
	}
	for s, want := range map[string]string{"c": "v1", "q": "v1", "n": "v"} {
		v, err := db.Get(ctx, k(s))
		if err != nil || string(v) != want {
			t.Fatalf("%s after the splits: %q %v, want %q", s, v, err, want)
		}
	}
	if r := replicaFor(t, n, k("q")); !r.Desc().StartKey.Equal(k("p")) {
		t.Fatalf("q is on [%s, %s), want the range split at p", r.Desc().StartKey, r.Desc().EndKey)
	}
}
