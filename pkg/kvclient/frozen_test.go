package kvclient

import (
	"context"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/sthorne/datax/pkg/base"
	"github.com/sthorne/datax/pkg/keys"
	"github.com/sthorne/datax/pkg/kvpb"
	"github.com/sthorne/datax/pkg/util/hlc"
)

// frozenStore is a LocalStore whose one range answers "frozen for a
// merge" a set number of times before serving. The refusal carries no
// descriptors, as an answer need not: a gateway that evicted its
// descriptor on it would have nothing to route by (meta lookups are off
// here) and fail, so a write that goes through shows the descriptor was
// held.
type frozenStore struct {
	desc          kvpb.RangeDescriptor
	frozenAnswers atomic.Int32
	calls         atomic.Int32
}

func (s *frozenStore) NodeID() base.NodeID { return 1 }

func (s *frozenStore) ExecuteBatch(_ context.Context, ba *kvpb.BatchRequest) (*kvpb.BatchResponse, *kvpb.Error) {
	s.calls.Add(1)
	if s.frozenAnswers.Add(-1) >= 0 {
		e := kvpb.NewErrorf("%s: range is frozen for a merge", s.desc.RangeID)
		e.RangeKeyMismatch = &kvpb.RangeKeyMismatchError{
			RequestKey: ba.Requests[0].GetInner().Header().Key,
			Frozen:     true,
		}
		return nil, e
	}
	return &kvpb.BatchResponse{Responses: []kvpb.ResponseUnion{{Put: &kvpb.PutResponse{}}}}, nil
}

func newFrozenStoreDB() (*DB, *frozenStore, keys.Key) {
	prefix := keys.TableDataPrefix(900)
	desc := kvpb.RangeDescriptor{
		RangeID: 5, Generation: 3, StartKey: prefix, EndKey: prefix.PrefixEnd(),
		Replicas: []kvpb.ReplicaDescriptor{{NodeID: 1, StoreID: 1, ReplicaID: 1}},
	}
	store := &frozenStore{desc: desc}
	db := NewDB(store, nil, hlc.NewClock(nil, 0))
	db.SeedDescriptor(desc)
	return db, store, append(prefix.Clone(), "k"...)
}

// A range frozen for a merge answers with the right descriptor and a
// refusal; the gateway holds the descriptor and waits the freeze out
// instead of re-resolving the same range until the routing budget is
// spent (issue #263).
func TestGatewayWaitsOutAFrozenRange(t *testing.T) {
	db, store, key := newFrozenStoreDB()
	store.frozenAnswers.Store(3)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	if err := db.Put(ctx, key, []byte("v")); err != nil {
		t.Fatalf("write through a briefly frozen range: %v", err)
	}
	if got := store.calls.Load(); got != 4 {
		t.Fatalf("store answered %d times, want 3 frozen refusals and the write", got)
	}
}

// A freeze that never lifts is still an error, and a terminal one: after
// frozenRangeWait the batch fails saying how long the range was frozen,
// and the routing loop does not re-resolve and wait again — one batch
// waits the bound once, not thirty times.
func TestFrozenRangeWaitIsBounded(t *testing.T) {
	db, store, key := newFrozenStoreDB()
	store.frozenAnswers.Store(1 << 30)
	saved := frozenRangeWait
	frozenRangeWait = 300 * time.Millisecond
	defer func() { frozenRangeWait = saved }()

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	header := kvpb.BatchHeader{Timestamp: db.clock.Now()}
	reqs := []kvpb.RequestUnion{{Put: &kvpb.PutRequest{RequestHeader: kvpb.RequestHeader{Key: key}, Value: []byte("v")}}}
	start := time.Now()
	_, regroup, kerr := db.sendPartial(ctx, &header, reqs, store.desc)
	if regroup || kerr == nil || kerr.RangeKeyMismatch != nil {
		t.Fatalf("got regroup=%v err=%v, want a terminal error", regroup, kerr)
	}
	if !strings.Contains(kerr.Error(), "frozen for a merge") || !strings.Contains(kerr.Error(), "frozen for 300ms") {
		t.Fatalf("error %q does not say the range stayed frozen for the bound", kerr.Error())
	}
	if el := time.Since(start); el < frozenRangeWait || el > 10*time.Second {
		t.Fatalf("waited %s, want about %s", el, frozenRangeWait)
	}
	if store.calls.Load() < 2 {
		t.Fatalf("store answered %d times, want retries while frozen", store.calls.Load())
	}
	if _, ok := db.cache.Lookup(key); !ok {
		t.Fatal("the descriptor was evicted: the routing was right and must be kept")
	}

	// Through the whole batch path the bound is paid once.
	calls := store.calls.Load()
	start = time.Now()
	err := db.Put(ctx, key, []byte("v"))
	if err == nil || !strings.Contains(err.Error(), "frozen for 300ms") {
		t.Fatalf("Put on a range nothing unfreezes: %v, want the frozen error", err)
	}
	if el := time.Since(start); el > 3*frozenRangeWait {
		t.Fatalf("Put took %s against a %s bound: the routing loop waited again", el, frozenRangeWait)
	}
	if n := store.calls.Load() - calls; n > 40 {
		t.Fatalf("Put sent %d batches to a frozen range, want one bounded wait's worth", n)
	}
}
