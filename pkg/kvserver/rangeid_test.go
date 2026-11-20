package kvserver

import (
	"bytes"
	"context"
	"fmt"
	"sort"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"

	"github.com/sthorne/datax/pkg/base"
	"github.com/sthorne/datax/pkg/keys"
	"github.com/sthorne/datax/pkg/kvpb"
	"github.com/sthorne/datax/pkg/metrics"
	"github.com/sthorne/datax/pkg/storage"
	"github.com/sthorne/datax/pkg/util/hlc"
	"github.com/sthorne/datax/pkg/util/stop"
)

// Range ID allocation (issue #389). Every split draws its new range's ID
// from one replicated counter with an Increment. Under DDL load two
// splits 4 ms apart drew the same ID, the second split overwrote the
// first range's descriptor, and a span — the one holding the database
// catalog — was left with no replica serving it.

// storeRouter is a Sender over one store: it routes each batch by key
// and resends what the store answers with a routing or timestamp error,
// the way kvclient.DB does across a cluster.
type storeRouter struct {
	s     *Store
	clock *hlc.Clock
}

func (sr *storeRouter) Send(ctx context.Context, ba *kvpb.BatchRequest) (*kvpb.BatchResponse, *kvpb.Error) {
	for attempt := 0; ; attempt++ {
		c := *ba
		c.Header.RangeID = 0
		br, kerr := sr.s.ExecuteBatch(ctx, &c)
		if kerr == nil {
			return br, nil
		}
		retry := kerr.RangeKeyMismatch != nil || kerr.NotLeader != nil || kerr.DescriptorChanged != nil ||
			(kerr.TxnRetry != nil && ba.Header.Txn == nil)
		if !retry || attempt >= 500 || ctx.Err() != nil {
			return nil, kerr
		}
		if kerr.TxnRetry != nil {
			ba.Header.Timestamp = sr.clock.Now()
		}
		time.Sleep(time.Millisecond)
	}
}

type routerEnv struct {
	store  *Store
	r1     *Replica
	router *storeRouter
	clock  *hlc.Clock
}

// startRouterStore starts a one-store, one-replica cluster whose range 1
// spans the whole keyspace, its ID counter at 1 (as bootstrap leaves it).
func startRouterStore(t *testing.T, knobs TestingKnobs) *routerEnv {
	t.Helper()
	eng, err := storage.Open("", storage.Options{})
	if err != nil {
		t.Fatal(err)
	}
	stopper := stop.NewStopper()
	t.Cleanup(func() {
		stopper.Stop()
		_ = eng.Close()
	})
	env := &routerEnv{clock: hlc.NewClock(nil, 250*time.Millisecond)}
	env.store = NewStore(StoreConfig{
		NodeID: 1, StoreID: 1, Engine: eng, Clock: env.clock, Stopper: stopper,
		Transport: nopRaftTransport{}, RaftTickInterval: 10 * time.Millisecond,
		TestingKnobs: knobs,
	})
	env.router = &storeRouter{s: env.store, clock: env.clock}
	env.store.SetSender(env.router)
	desc := kvpb.RangeDescriptor{
		RangeID: 1, StartKey: keys.MinKey.Clone(), EndKey: keys.MaxKey.Clone(), NextReplicaID: 2,
		Replicas: []kvpb.ReplicaDescriptor{{NodeID: 1, StoreID: 1, ReplicaID: 1}},
	}
	env.r1, err = env.store.CreateReplica(desc, true)
	if err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(10 * time.Second)
	for !env.r1.IsLeader() {
		if time.Now().After(deadline) {
			t.Fatal("range 1 never got a leader")
		}
		time.Sleep(10 * time.Millisecond)
	}
	env.put(t, keys.RangeIDGenKey(), "1")
	return env
}

func (e *routerEnv) send(ctx context.Context, req kvpb.Request) (*kvpb.BatchResponse, *kvpb.Error) {
	ba := &kvpb.BatchRequest{Header: kvpb.BatchHeader{Timestamp: e.clock.Now()}}
	ba.Add(req)
	return e.router.Send(ctx, ba)
}

func (e *routerEnv) put(t *testing.T, k keys.Key, v string) {
	t.Helper()
	if _, kerr := e.send(context.Background(), &kvpb.PutRequest{RequestHeader: kvpb.RequestHeader{Key: k}, Value: []byte(v)}); kerr != nil {
		t.Fatal(kerr)
	}
}

func (e *routerEnv) counter(t *testing.T) int64 {
	t.Helper()
	br, kerr := e.send(context.Background(), &kvpb.GetRequest{RequestHeader: kvpb.RequestHeader{Key: keys.RangeIDGenKey()}})
	if kerr != nil {
		t.Fatal(kerr)
	}
	v, err := strconv.ParseInt(string(br.Responses[0].Get.Value), 10, 64)
	if err != nil {
		t.Fatal(err)
	}
	return v
}

func (e *routerEnv) split(ctx context.Context, k keys.Key) (*kvpb.AdminSplitResponse, *kvpb.Error) {
	br, kerr := e.send(ctx, &kvpb.AdminSplitRequest{RequestHeader: kvpb.RequestHeader{Key: k}})
	if kerr != nil {
		return nil, kerr
	}
	return br.Responses[0].AdminSplit, nil
}

func (e *routerEnv) merge(ctx context.Context, k keys.Key) *kvpb.Error {
	_, kerr := e.send(ctx, &kvpb.AdminMergeRequest{RequestHeader: kvpb.RequestHeader{Key: k}})
	return kerr
}

// checkStoreTiling verifies that the store's replicas tile the keyspace:
// distinct IDs, no gap and no overlap, every key covered by exactly one
// replica, and each replica's persisted descriptor the one it serves.
func checkStoreTiling(t *testing.T, s *Store) {
	t.Helper()
	var descs []kvpb.RangeDescriptor
	s.VisitReplicas(func(r *Replica) bool {
		descs = append(descs, r.Desc())
		return true
	})
	sort.Slice(descs, func(i, j int) bool { return descs[i].StartKey.Compare(descs[j].StartKey) < 0 })
	ids := map[base.RangeID]bool{}
	for i, d := range descs {
		if ids[d.RangeID] {
			t.Errorf("range ID %s serves two replicas", d.RangeID)
		}
		ids[d.RangeID] = true
		if i == 0 && !d.StartKey.Equal(keys.MinKey) {
			t.Errorf("no replica covers [%s, %s)", keys.MinKey, d.StartKey)
		}
		if i > 0 && !descs[i-1].EndKey.Equal(d.StartKey) {
			t.Errorf("%s ends at %s but the next range %s starts at %s", descs[i-1].RangeID, descs[i-1].EndKey, d.RangeID, d.StartKey)
		}
		stored, ok, err := loadRangeDescriptor(s.cfg.Engine, d.RangeID)
		if err != nil || !ok || !stored.StartKey.Equal(d.StartKey) || !stored.EndKey.Equal(d.EndKey) {
			t.Errorf("%s serves [%s, %s) but its stored descriptor is %v (found %v, %v)", d.RangeID, d.StartKey, d.EndKey, stored, ok, err)
		}
		for _, k := range []keys.Key{d.StartKey, d.StartKey.Next()} {
			n := 0
			s.VisitReplicas(func(r *Replica) bool {
				if rd := r.Desc(); rd.ContainsKey(k) {
					n++
				}
				return true
			})
			if n != 1 {
				t.Errorf("key %s is covered by %d replicas", k, n)
			}
		}
	}
	if len(descs) > 0 && !descs[len(descs)-1].EndKey.Equal(keys.MaxKey) {
		t.Errorf("no replica covers [%s, %s)", descs[len(descs)-1].EndKey, keys.MaxKey)
	}
}

// TestRangeIDCounterUniqueWhileItsRangeSplits: increments of the range ID
// counter, and splits elsewhere drawing from it, run while the range
// holding the counter is split and merged back over and over — the
// counter changing hands between two ranges. Every value handed out is
// distinct, and the store's ranges still tile the keyspace.
//
// Before the fix an Increment that passed the key-bounds check on the
// counter's range, then waited out a split's latch, was served by the
// left half after the split had handed the key to the right half — and
// applied through the left half's Raft group in parallel with an
// Increment applied through the right half's, both reading the same
// value.
func TestRangeIDCounterUniqueWhileItsRangeSplits(t *testing.T) {
	env := startRouterStore(t, TestingKnobs{})
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	gen := keys.RangeIDGenKey()

	var mu sync.Mutex
	handed := map[int64]string{}
	record := func(v int64, by string) {
		mu.Lock()
		defer mu.Unlock()
		if prev, ok := handed[v]; ok {
			t.Errorf("counter value %d handed out twice: to %s and to %s", v, prev, by)
		}
		handed[v] = by
	}

	stopChurn := make(chan struct{})
	var churn sync.WaitGroup
	churn.Add(1)
	go func() {
		defer churn.Done()
		for {
			select {
			case <-stopChurn:
				return
			default:
			}
			if sr, kerr := env.split(ctx, gen); kerr == nil && sr.Left.RangeID != 0 {
				record(int64(sr.Right.RangeID), fmt.Sprintf("the split at the counter (%s)", sr.Right.StartKey))
			}
			_ = env.merge(ctx, keys.SystemPrefix)
		}
	}()

	var work sync.WaitGroup
	for g := 0; g < 8; g++ {
		work.Add(2)
		go func(g int) {
			defer work.Done()
			for i := 0; i < 300; i++ {
				br, kerr := env.send(ctx, &kvpb.IncrementRequest{RequestHeader: kvpb.RequestHeader{Key: gen}, By: 1})
				if kerr != nil {
					continue
				}
				record(br.Responses[0].Increment.NewValue, fmt.Sprintf("increment %d/%d", g, i))
			}
		}(g)
		go func(g int) {
			defer work.Done()
			for i := 0; i < 20; i++ {
				k := append(keys.TableDataPrefix(uint64(500+g)), fmt.Sprintf("k%03d", i)...)
				if sr, kerr := env.split(ctx, k); kerr == nil && sr.Left.RangeID != 0 {
					record(int64(sr.Right.RangeID), fmt.Sprintf("the split at %s", k))
				}
			}
		}(g)
	}
	work.Wait()
	close(stopChurn)
	churn.Wait()
	if ctx.Err() != nil {
		t.Fatal("timed out")
	}
	if len(handed) < 8*300/2 {
		t.Fatalf("only %d values handed out; the load did not run", len(handed))
	}
	if got := env.counter(t); got < int64(len(handed)) {
		t.Errorf("counter at %d after handing out %d distinct values", got, len(handed))
	}
	checkStoreTiling(t, env.store)
}

// TestConcurrentSplitsAndMergesKeepRangesTiled: a burst of concurrent
// splits on many ranges of one store, then splits and merges together;
// every split's range gets its own ID and every key still resolves to
// exactly one range afterwards.
func TestConcurrentSplitsAndMergesKeepRangesTiled(t *testing.T) {
	env := startRouterStore(t, TestingKnobs{})
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	const tables, perTable = 12, 20
	key := func(tbl, i int) keys.Key {
		return append(keys.TableDataPrefix(uint64(700+tbl)), fmt.Sprintf("k%03d", i)...)
	}
	var mu sync.Mutex
	ids := map[base.RangeID]string{}
	record := func(sr *kvpb.AdminSplitResponse) {
		if sr.Left.RangeID == 0 {
			return
		}
		mu.Lock()
		defer mu.Unlock()
		if prev, ok := ids[sr.Right.RangeID]; ok {
			t.Errorf("range ID %s given to the splits at %s and at %s", sr.Right.RangeID, prev, sr.Right.StartKey)
		}
		ids[sr.Right.RangeID] = sr.Right.StartKey.String()
	}

	// One table boundary first, so the bursts below split many ranges.
	for tbl := 0; tbl < tables; tbl++ {
		sr, kerr := env.split(ctx, keys.TableDataPrefix(uint64(700+tbl)))
		if kerr != nil {
			t.Fatal(kerr)
		}
		record(sr)
	}
	var wg sync.WaitGroup
	for tbl := 0; tbl < tables; tbl++ {
		wg.Add(1)
		go func(tbl int) {
			defer wg.Done()
			for i := 0; i < perTable; i++ {
				if sr, kerr := env.split(ctx, key(tbl, i)); kerr == nil {
					record(sr)
				}
			}
		}(tbl)
	}
	// The counter's range keeps changing hands meanwhile, as range 1's
	// does under DDL load.
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 60; i++ {
			if sr, kerr := env.split(ctx, keys.RangeIDGenKey()); kerr == nil {
				record(sr)
			}
			_ = env.merge(ctx, keys.SystemPrefix)
		}
	}()
	wg.Wait()
	checkStoreTiling(t, env.store)

	// Splits and merges together: odd keys merge away while new keys
	// split.
	for tbl := 0; tbl < tables; tbl++ {
		wg.Add(2)
		go func(tbl int) {
			defer wg.Done()
			for i := 0; i < perTable; i += 2 {
				_ = env.merge(ctx, key(tbl, i))
			}
		}(tbl)
		go func(tbl int) {
			defer wg.Done()
			for i := 0; i < perTable; i++ {
				if sr, kerr := env.split(ctx, append(key(tbl, i), 'x')); kerr == nil {
					record(sr)
				}
			}
		}(tbl)
	}
	wg.Wait()
	if ctx.Err() != nil {
		t.Fatal("timed out")
	}
	checkStoreTiling(t, env.store)
	// Every key written before is still readable through its range.
	for tbl := 0; tbl < tables; tbl++ {
		k := append(key(tbl, perTable/2), 'v')
		env.put(t, k, "v")
		br, kerr := env.send(ctx, &kvpb.GetRequest{RequestHeader: kvpb.RequestHeader{Key: k}})
		if kerr != nil || !bytes.Equal(br.Responses[0].Get.Value, []byte("v")) {
			t.Fatalf("read of %s after the burst: %v", k, kerr)
		}
	}
}

// TestWriteQueuedBehindSplitIsReRouted: an Increment that passed the
// key-bounds check on the counter's range and then waited for its
// latches while a split handed the counter to a new range is refused by
// the old range once it has the latches, before anything is proposed;
// the counter moves only through its owner.
func TestWriteQueuedBehindSplitIsReRouted(t *testing.T) {
	gen := keys.RangeIDGenKey()
	var armed atomic.Bool
	atLatch, proceed := make(chan struct{}), make(chan struct{})
	env := startRouterStore(t, TestingKnobs{
		BeforeLatch: func(ba *kvpb.BatchRequest) {
			if ba.Requests[0].Increment != nil && armed.CompareAndSwap(true, false) {
				close(atLatch)
				<-proceed
			}
		},
	})
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	env.put(t, gen, "10")
	if _, kerr := env.split(ctx, keys.TableDataPrefix(1)); kerr != nil {
		t.Fatal(kerr) // the counter's range ends below the table data
	}
	before := env.counter(t)

	armed.Store(true)
	type result struct {
		br   *kvpb.BatchResponse
		kerr *kvpb.Error
	}
	done := make(chan result, 1)
	go func() {
		ba := &kvpb.BatchRequest{Header: kvpb.BatchHeader{RangeID: env.r1.rangeID, Timestamp: env.clock.Now()}}
		ba.Add(&kvpb.IncrementRequest{RequestHeader: kvpb.RequestHeader{Key: gen}, By: 1})
		br, kerr := env.r1.Execute(ctx, ba)
		done <- result{br, kerr}
	}()
	<-atLatch
	sr, kerr := env.split(ctx, gen)
	if kerr != nil {
		t.Fatal(kerr)
	}
	if sr.Left.RangeID != env.r1.rangeID || sr.Right.ContainsKey(gen) != true {
		t.Fatalf("the split did not hand the counter to a new range: %v / %v", sr.Left, sr.Right)
	}
	afterSplit := env.counter(t)
	if afterSplit != before+1 {
		t.Fatalf("counter at %d after the split, want %d", afterSplit, before+1)
	}
	refusedBefore := testutil.ToFloat64(metrics.DescriptorChangeRefusals.WithLabelValues("write"))
	close(proceed)
	res := <-done
	if res.kerr == nil {
		t.Fatalf("%s served an Increment of %s after the split gave it to %s: new value %d",
			env.r1.rangeID, gen, sr.Right.RangeID, res.br.Responses[0].Increment.NewValue)
	}
	if res.kerr.RangeKeyMismatch == nil {
		t.Fatalf("refusal is not a RangeKeyMismatch: %v", res.kerr)
	}
	if got := testutil.ToFloat64(metrics.DescriptorChangeRefusals.WithLabelValues("write")) - refusedBefore; got != 0 {
		t.Errorf("the write was proposed and refused at apply (%v); it should be refused once it holds its latches", got)
	}
	if got := env.counter(t); got != afterSplit {
		t.Fatalf("counter at %d, want %d: the refused Increment moved it", got, afterSplit)
	}
}

// TestStaleWriteRefusedAtApply: a write proposed for a key the range no
// longer owns — or while it is frozen for a merge — is refused when it
// applies (deterministically, on every replica), with a RangeKeyMismatch
// the client follows, and writes nothing. This is what keeps a proposal
// ordered after a split by a leader change from applying through the
// wrong Raft group.
func TestStaleWriteRefusedAtApply(t *testing.T) {
	env := startClearTestStore(t)
	ctx := context.Background()
	outside, _ := keys.TableDataSpan(clearTestTable + 1)
	outside = append(outside.Clone(), 'k')
	propose := func(req kvpb.Request) *kvpb.Error {
		ba := &kvpb.BatchRequest{Header: kvpb.BatchHeader{RangeID: env.r.rangeID, Timestamp: env.clock.Now()}}
		ba.Add(req)
		_, kerr := env.r.proposeCmd(ctx, ba, cmdTriggers{})
		return kerr
	}
	for _, req := range []kvpb.Request{
		&kvpb.IncrementRequest{RequestHeader: kvpb.RequestHeader{Key: outside}, By: 1},
		&kvpb.PutRequest{RequestHeader: kvpb.RequestHeader{Key: outside}, Value: []byte("v")},
		&kvpb.DeleteRequest{RequestHeader: kvpb.RequestHeader{Key: outside}},
	} {
		kerr := propose(req)
		if kerr == nil || kerr.RangeKeyMismatch == nil {
			t.Fatalf("%s outside the range applied (%v)", req.Method(), kerr)
		}
	}
	if v, err := storage.MVCCGet(env.store.cfg.Engine, outside, maxTimestamp, storage.MVCCGetOptions{}); err != nil || v != nil {
		t.Fatalf("the refused writes left %q (%v)", v, err)
	}
	// Inside the range, the same write applies.
	if kerr := propose(&kvpb.IncrementRequest{RequestHeader: kvpb.RequestHeader{Key: env.key("c")}, By: 1}); kerr != nil {
		t.Fatal(kerr)
	}
	// Frozen: refused even inside the range.
	env.r.mu.Lock()
	env.r.mu.frozen = true
	env.r.mu.Unlock()
	kerr := propose(&kvpb.IncrementRequest{RequestHeader: kvpb.RequestHeader{Key: env.key("c")}, By: 1})
	env.r.mu.Lock()
	env.r.mu.frozen = false
	env.r.mu.Unlock()
	if kerr == nil || kerr.RangeKeyMismatch == nil || !kerr.RangeKeyMismatch.Frozen {
		t.Fatalf("a write on a frozen range applied (%v)", kerr)
	}
	if v, err := storage.MVCCGet(env.store.cfg.Engine, env.key("c"), maxTimestamp, storage.MVCCGetOptions{}); err != nil || string(v) != "1" {
		t.Fatalf("counter is %q (%v), want 1", v, err)
	}
}

// TestSplitRefusesRangeIDInUse: a split whose new range would take an ID
// another range on the store already has is refused at apply, leaving
// both ranges as they were; with a free ID the same split applies.
func TestSplitRefusesRangeIDInUse(t *testing.T) {
	env := startClearTestStore(t)
	rStart, rEnd := keys.TableDataSpan(clearTestTable + 1)
	other, err := env.store.CreateReplica(kvpb.RangeDescriptor{
		RangeID: 11, StartKey: rStart, EndKey: rEnd, NextReplicaID: 2, Generation: 1,
		Replicas: env.r.Desc().Replicas,
	}, true)
	if err != nil {
		t.Fatal(err)
	}
	desc := env.r.Desc()
	left, right := desc, desc
	left.EndKey = env.key("m")
	left.Generation = desc.Generation + 1
	right.RangeID = other.rangeID
	right.StartKey = env.key("m")
	right.Generation = desc.Generation + 1
	b := env.store.cfg.Engine.NewBatch()
	defer func() { _ = b.Close() }()
	collisions := testutil.ToFloat64(metrics.RangeIDCollisions)
	if err := env.r.stageSplit(b, &splitTrigger{Left: left, Right: right}); err == nil {
		t.Fatalf("a split giving the new range %s's ID applied", other.rangeID)
	}
	if testutil.ToFloat64(metrics.RangeIDCollisions) != collisions+1 {
		t.Error("the refusal was not counted")
	}
	if d := other.Desc(); !d.StartKey.Equal(rStart) || !d.EndKey.Equal(rEnd) {
		t.Fatalf("%s is now %v", other.rangeID, d)
	}
	right.RangeID = 12
	if err := env.r.stageSplit(b, &splitTrigger{Left: left, Right: right}); err != nil {
		t.Fatalf("the split with a free ID: %v", err)
	}
	// A replica of the right-hand range itself (seeded before this
	// replica applied the split) is not a collision.
	if why := env.store.rangeIDInUse(other.rangeID, rStart); why != "" {
		t.Fatalf("the range's own replica counted as a collision: %s", why)
	}
}

// TestSplitDrawsAnotherRangeIDWhenTaken: when the counter hands a split
// an ID that already names a range on the store, the split draws another
// one and both ranges survive.
func TestSplitDrawsAnotherRangeIDWhenTaken(t *testing.T) {
	env := startRouterStore(t, TestingKnobs{})
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	first, kerr := env.split(ctx, keys.TableDataPrefix(800))
	if kerr != nil {
		t.Fatal(kerr)
	}
	// Wind the counter back so the next draw is the ID just used — and
	// below it, range 1's.
	env.put(t, keys.RangeIDGenKey(), strconv.Itoa(int(first.Right.RangeID)-1))
	second, kerr := env.split(ctx, keys.TableDataPrefix(801))
	if kerr != nil {
		t.Fatal(kerr)
	}
	if second.Right.RangeID == first.Right.RangeID || second.Right.RangeID == 1 {
		t.Fatalf("the second split reused %s", second.Right.RangeID)
	}
	checkStoreTiling(t, env.store)
	// A taken ID drawn before range 1's first split is refused the same
	// way: range 1 itself.
	env.put(t, keys.RangeIDGenKey(), "0")
	third, kerr := env.split(ctx, keys.TableDataPrefix(802))
	if kerr != nil {
		t.Fatal(kerr)
	}
	for _, used := range []base.RangeID{1, first.Right.RangeID, second.Right.RangeID} {
		if third.Right.RangeID == used {
			t.Fatalf("the third split reused %s", used)
		}
	}
	checkStoreTiling(t, env.store)
}

// TestMergeRefusesRightRangeWithWrongStart: a merge trigger naming a
// right-hand range whose local replica starts somewhere other than the
// left range's end — another range under the same ID — is refused, and
// that replica is left in place.
func TestMergeRefusesRightRangeWithWrongStart(t *testing.T) {
	env := startClearTestStore(t)
	ctx := context.Background()
	// A range NOT adjacent to env.r, under the ID the trigger names, at
	// the generation and with the end key the trigger claims — only its
	// start differs.
	rStart, rEnd := keys.TableDataSpan(clearTestTable + 1)
	far, err := env.store.CreateReplica(kvpb.RangeDescriptor{
		RangeID: 11, StartKey: append(rStart.Clone(), 'm'), EndKey: rEnd, NextReplicaID: 2, Generation: 1,
		Replicas: env.r.Desc().Replicas,
	}, true)
	if err != nil {
		t.Fatal(err)
	}
	claimed := kvpb.RangeDescriptor{RangeID: far.rangeID, StartKey: rStart, EndKey: rEnd, Generation: far.Desc().Generation, Replicas: env.r.Desc().Replicas}
	merged := env.r.Desc()
	merged.EndKey = rEnd
	merged.Generation++
	b := env.store.cfg.Engine.NewBatch()
	defer func() { _ = b.Close() }()
	trig := &mergeTrigger{Left: env.r.Desc(), Right: claimed, Merged: merged, RightAppliedIndex: far.AppliedIndex()}
	if err := env.r.stageMerge(ctx, b, trig); err == nil {
		t.Fatal("a merge absorbed a range that does not start where this one ends")
	}
	if rep, ok := env.store.GetReplica(far.rangeID); !ok || rep != far {
		t.Fatalf("%s was detached", far.rangeID)
	}
}
