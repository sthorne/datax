package testcluster

import (
	"context"
	"testing"
	"time"

	"github.com/sthorne/datax/pkg/keys"
	"github.com/sthorne/datax/pkg/kvpb"
	"github.com/sthorne/datax/pkg/server"
)

// Issue #269: the nightly bench's cluster run failed a statement with
// "r22: read index abandoned: context deadline exceeded". A read confirms
// leadership before it serves; the balancer moved the range's lease away
// with that confirmation round in flight, raft dropped the round, and the
// read waited out the round's timeout — which is as long as the gateway's
// per-attempt deadline, so what came back was the attempt running out of
// time rather than "not the leader, try the new one". These tests make a
// round's timeout far longer than the test, so a round that is released
// by the leadership change is told apart from one that merely expires.

// TestReadRoundIsReleasedWhenLeadershipIsLost: a read waiting on a
// confirmation round when its replica stops being the leader is answered
// NotLeader at once.
func TestReadRoundIsReleasedWhenLeadershipIsLost(t *testing.T) {
	tc := StartWithOptions(t, 3, func(c *server.Config) {
		// Every read takes the quorum round, so one can be caught in flight.
		c.DisableLeaseReads = true
		c.TestingKnobs.ReadIndexTimeout = 60 * time.Second
	})
	ctx, cancel := context.WithTimeout(context.Background(), 180*time.Second)
	defer cancel()
	key := append(keys.TableDataPrefix(870).Clone(), "k"...)
	if err := tc.Nodes[0].DB().Put(ctx, key, []byte("v")); err != nil {
		t.Fatal(err)
	}

	for try := 0; try < 3; try++ {
		lead := tc.LeaderIndex(1)
		// Cut the leader off: its round can gather no quorum, and after an
		// election timeout without one it steps down.
		tc.Isolate(lead)
		get := &kvpb.BatchRequest{Header: kvpb.BatchHeader{RangeID: 1, Timestamp: tc.Nodes[lead].Clock().Now()}}
		get.Add(&kvpb.GetRequest{RequestHeader: kvpb.RequestHeader{Key: key}})
		rctx, rcancel := context.WithTimeout(ctx, 30*time.Second)
		start := time.Now()
		_, kerr := tc.Nodes[lead].Store().ExecuteBatch(rctx, get)
		waited := time.Since(start)
		rcancel()
		tc.Heal()
		if waited < 50*time.Millisecond {
			continue // it had already stepped down: nothing was in flight
		}
		if kerr == nil || kerr.NotLeader == nil {
			t.Fatalf("read caught by a leadership loss came back after %s with %v, want NotLeader", waited.Truncate(time.Millisecond), kerr)
		}
		if waited > 20*time.Second {
			t.Fatalf("read was released only after %s", waited.Truncate(time.Millisecond))
		}
		// The cluster serves the read once it has a leader again.
		if v, err := tc.Nodes[(lead+1)%3].DB().Get(ctx, key); err != nil || string(v) != "v" {
			t.Fatalf("read after the leadership change: %q, %v", v, err)
		}
		return
	}
	t.Fatal("never caught a read waiting on a confirmation round")
}

// TestFollowerDoesNotAskForAReadIndex: a replica that is not the leader
// refuses the round itself. Handed to raft, a follower's request is
// forwarded to the leader it knows — answering a read this replica must
// not serve — or dropped without a word when it knows none, which is the
// state a replica is in just after stepping down.
func TestFollowerDoesNotAskForAReadIndex(t *testing.T) {
	tc := StartWithOptions(t, 3, func(c *server.Config) {
		c.TestingKnobs.ReadIndexTimeout = 60 * time.Second
	})
	lead := tc.LeaderIndex(1)
	follower := (lead + 1) % 3
	r, ok := tc.Nodes[follower].Store().GetReplica(1)
	if !ok {
		t.Fatalf("n%d holds no replica of r1", follower+1)
	}
	done := make(chan *kvpb.Error, 1)
	go func() {
		_, kerr := r.TestingIssueReadIndex()
		done <- kerr
	}()
	select {
	case kerr := <-done:
		if kerr == nil || kerr.NotLeader == nil {
			t.Fatalf("a follower's read-index round came back with %v, want NotLeader", kerr)
		}
	case <-time.After(20 * time.Second):
		t.Fatal("a follower's read-index round was neither answered nor refused")
	}
}
