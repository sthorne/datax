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

// stallingStore is a LocalStore whose one range sits on a batch until the
// attempt's deadline passes, a set number of times, and then answers with
// what a replica says when it gives up — the shape of a leader that is
// seconds behind under load (issue #269). After that it serves.
type stallingStore struct {
	stalls atomic.Int32
	calls  atomic.Int32
}

func (s *stallingStore) NodeID() base.NodeID { return 1 }

func (s *stallingStore) ExecuteBatch(ctx context.Context, ba *kvpb.BatchRequest) (*kvpb.BatchResponse, *kvpb.Error) {
	s.calls.Add(1)
	if s.stalls.Add(-1) >= 0 {
		<-ctx.Done()
		return nil, kvpb.NewErrorf("r5: read index abandoned: %v", ctx.Err())
	}
	br := &kvpb.BatchResponse{}
	for _, u := range ba.Requests {
		switch {
		case u.Get != nil:
			br.Responses = append(br.Responses, kvpb.ResponseUnion{Get: &kvpb.GetResponse{Value: []byte("v")}})
		case u.Put != nil:
			br.Responses = append(br.Responses, kvpb.ResponseUnion{Put: &kvpb.PutResponse{}})
		}
	}
	return br, nil
}

func newStallingStoreDB() (*DB, *stallingStore, keys.Key) {
	prefix := keys.TableDataPrefix(901)
	desc := kvpb.RangeDescriptor{
		RangeID: 5, Generation: 3, StartKey: prefix, EndKey: prefix.PrefixEnd(),
		Replicas: []kvpb.ReplicaDescriptor{{NodeID: 1, StoreID: 1, ReplicaID: 1}},
	}
	store := &stallingStore{}
	db := NewDB(store, nil, hlc.NewClock(nil, 0))
	db.attemptTimeout = 150 * time.Millisecond
	db.SeedDescriptor(desc)
	return db, store, append(prefix.Clone(), "k"...)
}

// A read whose attempt at a replica runs out its per-attempt deadline is
// sent again, under the caller's own deadline: the replica's "I gave up"
// is the attempt timing out, not the statement's result. The nightly
// bench's cluster run failed on exactly this once the gateway's own node
// led the range: "ERROR: r22: read index abandoned: context deadline
// exceeded" (issue #269).
func TestReadAttemptTimeoutIsRetried(t *testing.T) {
	db, store, key := newStallingStoreDB()
	store.stalls.Store(2)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	start := time.Now()
	v, err := db.Get(ctx, key)
	if err != nil {
		t.Fatalf("read through two timed-out attempts: %v", err)
	}
	if string(v) != "v" {
		t.Fatalf("read %q, want %q", v, "v")
	}
	if got := store.calls.Load(); got != 3 {
		t.Fatalf("store was asked %d times, want two timed-out attempts and the read", got)
	}
	if el := time.Since(start); el < 2*db.attemptTimeout {
		t.Fatalf("returned after %s, before two attempts of %s could have timed out", el, db.attemptTimeout)
	}
}

// The retry runs under the caller's deadline, not past it: a replica that
// never answers costs the caller its own budget and then the error says
// what the attempts ran into.
func TestReadAttemptTimeoutRespectsTheCallersDeadline(t *testing.T) {
	db, store, key := newStallingStoreDB()
	store.stalls.Store(1 << 30)
	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()
	start := time.Now()
	_, err := db.Get(ctx, key)
	if err == nil {
		t.Fatal("read from a replica that never answers succeeded")
	}
	if el := time.Since(start); el > 5*time.Second {
		t.Fatalf("read took %s against a 500ms budget", el)
	}
	if !strings.Contains(err.Error(), "read index abandoned") && !strings.Contains(err.Error(), "deadline") {
		t.Fatalf("error %q does not say what the attempts ran into", err.Error())
	}
	if store.calls.Load() < 2 {
		t.Fatalf("store was asked %d times, want more than one attempt inside the budget", store.calls.Load())
	}
}

// A write is not sent again on a timed-out attempt: it may have been
// proposed, and resending it blind is not the gateway's call.
func TestWriteAttemptTimeoutIsNotRetriedByTheGateway(t *testing.T) {
	db, store, key := newStallingStoreDB()
	store.stalls.Store(1)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	err := db.Put(ctx, key, []byte("v"))
	if err == nil || !strings.Contains(err.Error(), "abandoned") {
		t.Fatalf("write whose attempt timed out: got %v, want the replica's error", err)
	}
	if got := store.calls.Load(); got != 1 {
		t.Fatalf("store was asked %d times, want the one attempt", got)
	}
}
