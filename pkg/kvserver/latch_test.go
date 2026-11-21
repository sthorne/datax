package kvserver

import (
	"context"
	"fmt"
	"math/rand"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/sthorne/datax/pkg/keys"
	"github.com/sthorne/datax/pkg/kvpb"
	"github.com/sthorne/datax/pkg/storage/enginepb"
	"github.com/sthorne/datax/pkg/util/hlc"
)

func span(s, e string) latchSpan {
	sp := latchSpan{Start: keys.Key(s)}
	if e != "" {
		sp.End = keys.Key(e)
	}
	return sp
}

func mustAcquire(t *testing.T, m *latchManager, spans []latchSpan, mode latchMode) *latchGuard {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	g, err := m.Acquire(ctx, spans, mode)
	if err != nil {
		t.Fatalf("acquire: %v", err)
	}
	return g
}

// acquireBlocks asserts the acquisition does NOT complete within a short
// window, then returns a channel that yields the guard once it does.
func acquireBlocks(t *testing.T, m *latchManager, spans []latchSpan, mode latchMode) <-chan *latchGuard {
	t.Helper()
	ch := make(chan *latchGuard, 1)
	go func() {
		g, err := m.Acquire(context.Background(), spans, mode)
		if err == nil {
			ch <- g
		}
	}()
	select {
	case <-ch:
		t.Fatal("acquisition should have blocked")
	case <-time.After(50 * time.Millisecond):
	}
	return ch
}

func TestLatchOverlapMatrix(t *testing.T) {
	cases := []struct {
		name     string
		a, b     latchSpan
		aM, bM   latchMode
		conflict bool
	}{
		{"point-point same key, wr-wr", span("k", ""), span("k", ""), latchExclusive, latchExclusive, true},
		{"point-point same key, rd-rd", span("k", ""), span("k", ""), latchShared, latchShared, false},
		{"point-point same key, rd-wr", span("k", ""), span("k", ""), latchShared, latchExclusive, true},
		{"disjoint points, wr-wr", span("a", ""), span("b", ""), latchExclusive, latchExclusive, false},
		{"span covers point, wr-rd", span("a", "z"), span("m", ""), latchExclusive, latchShared, true},
		{"adjacent spans, wr-wr", span("a", "m"), span("m", "z"), latchExclusive, latchExclusive, false},
		{"point at span end, wr-wr", span("a", "m"), span("m", ""), latchExclusive, latchExclusive, false},
		{"point at span start, wr-wr", span("a", "m"), span("a", ""), latchExclusive, latchExclusive, true},
		{"overlapping spans, rd-wr", span("a", "n"), span("m", "z"), latchShared, latchExclusive, true},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			m := newLatchManager()
			g1 := mustAcquire(t, m, []latchSpan{c.a}, c.aM)
			if c.conflict {
				ch := acquireBlocks(t, m, []latchSpan{c.b}, c.bM)
				g1.Release()
				select {
				case g2 := <-ch:
					g2.Release()
				case <-time.After(2 * time.Second):
					t.Fatal("blocked acquisition never woke after release")
				}
			} else {
				g2 := mustAcquire(t, m, []latchSpan{c.b}, c.bM)
				g2.Release()
				g1.Release()
			}
		})
	}
}

func TestLatchWholeRange(t *testing.T) {
	m := newLatchManager()
	g := mustAcquire(t, m, []latchSpan{wholeRangeSpan}, latchExclusive)
	ch := acquireBlocks(t, m, []latchSpan{span(string(keys.TableDataPrefix(5)), "")}, latchShared)
	g.Release()
	(<-ch).Release()
}

func TestLatchAcquireCancel(t *testing.T) {
	m := newLatchManager()
	g := mustAcquire(t, m, []latchSpan{span("k", "")}, latchExclusive)
	defer g.Release()
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	if _, err := m.Acquire(ctx, []latchSpan{span("k", "")}, latchExclusive); err == nil {
		t.Fatal("expected ctx error")
	}
	// The canceled waiter must not have left residue blocking others.
	g.Release()
	g2 := mustAcquire(t, m, []latchSpan{span("k", "")}, latchExclusive)
	g2.Release()
}

func TestLatchSpansFromBatch(t *testing.T) {
	ba := &kvpb.BatchRequest{}
	ba.Add(&kvpb.GetRequest{RequestHeader: kvpb.RequestHeader{Key: keys.Key("a")}})
	ba.Add(&kvpb.ScanRequest{RequestHeader: kvpb.RequestHeader{Key: keys.Key("b"), EndKey: keys.Key("c")}})
	spans, mode, err := latchSpans(ba)
	if err != nil || mode != latchShared || len(spans) != 2 {
		t.Fatalf("%v %v %v", spans, mode, err)
	}
	if spans[1].End == nil {
		t.Fatal("scan span lost its end key")
	}

	wb := &kvpb.BatchRequest{}
	wb.Add(&kvpb.PutRequest{RequestHeader: kvpb.RequestHeader{Key: keys.Key("a")}})
	_, mode, err = latchSpans(wb)
	if err != nil || mode != latchExclusive {
		t.Fatalf("%v %v", mode, err)
	}
}

// naiveConflict is the reference linear overlap scan the point index
// replaced; the randomized test below checks the index agrees with it.
func naiveConflict(held map[*latch]struct{}, l *latch) bool {
	for h := range held {
		if l.mode == latchShared && h.mode == latchShared {
			continue
		}
		// A barrier at T and a write known to land above T never conflict.
		if w, b := l, h; w.mode == latchExclusive && b.mode == latchShared && !b.closedBelow.IsEmpty() && !w.ts.IsEmpty() && b.closedBelow.Less(w.ts) {
			continue
		}
		if w, b := h, l; w.mode == latchExclusive && b.mode == latchShared && !b.closedBelow.IsEmpty() && !w.ts.IsEmpty() && b.closedBelow.Less(w.ts) {
			continue
		}
		for _, hs := range h.spans {
			for _, ls := range l.spans {
				sEnd, oEnd := hs.End, ls.End
				if sEnd == nil {
					sEnd = hs.Start.Next()
				}
				if oEnd == nil {
					oEnd = ls.Start.Next()
				}
				if hs.Start.Compare(oEnd) < 0 && ls.Start.Compare(sEnd) < 0 {
					return true
				}
			}
		}
	}
	return false
}

func TestLatchIndexMatchesNaiveScan(t *testing.T) {
	rng := rand.New(rand.NewSource(1))
	randSpan := func() latchSpan {
		a := keys.Key{byte('a' + rng.Intn(8))}
		if rng.Intn(4) != 0 {
			return latchSpan{Start: a}
		}
		b := keys.Key{byte('a' + rng.Intn(8))}
		if b.Compare(a) <= 0 {
			b = a.Next()
		}
		return latchSpan{Start: a, End: b}
	}
	randLatch := func() *latch {
		l := &latch{mode: latchMode(rng.Intn(2)), done: make(chan struct{})}
		// Some writes carry a timestamp and some shared latches are
		// closed-timestamp barriers, at timestamps 1..4.
		if rng.Intn(2) == 0 {
			ts := hlc.Timestamp{WallTime: int64(rng.Intn(4) + 1)}
			if l.mode == latchExclusive {
				l.ts = ts
			} else {
				l.closedBelow = ts
			}
		}
		for i := rng.Intn(4) + 1; i > 0; i-- {
			sp := randSpan()
			l.spans = append(l.spans, sp)
			if sp.End != nil {
				l.ranged = true
			}
		}
		return l
	}
	m := newLatchManager()
	var holders []*latch
	for i := 0; i < 5000; i++ {
		if len(holders) > 0 && rng.Intn(3) == 0 {
			j := rng.Intn(len(holders))
			m.remove(holders[j])
			holders = append(holders[:j], holders[j+1:]...)
		}
		l := randLatch()
		got := m.findConflict(l) != nil
		want := naiveConflict(m.held, l)
		if got != want {
			t.Fatalf("step %d: index says conflict=%v, naive says %v for %+v", i, got, want, l.spans)
		}
		if !got {
			m.insert(l)
			holders = append(holders, l)
		}
	}
	for _, l := range holders {
		m.remove(l)
	}
	if len(m.held) != 0 || len(m.points) != 0 || len(m.ranged) != 0 {
		t.Fatalf("index not empty after releasing everything: %d held, %d point keys, %d ranged", len(m.held), len(m.points), len(m.ranged))
	}
}

func TestLatchPointUnderRangedHolder(t *testing.T) {
	m := newLatchManager()
	g := mustAcquire(t, m, []latchSpan{span("b", "d")}, latchExclusive)
	ch := acquireBlocks(t, m, []latchSpan{span("c", "")}, latchShared)
	// A point outside the range and a point at the exclusive end are free.
	mustAcquire(t, m, []latchSpan{span("a", "")}, latchExclusive).Release()
	mustAcquire(t, m, []latchSpan{span("d", "")}, latchExclusive).Release()
	g.Release()
	(<-ch).Release()
}

// BenchmarkLatchAcquirePoints models a 100-key write batch contending with
// many disjoint holders — the shape the probe batches of INSERT produce.
func BenchmarkLatchAcquirePoints(b *testing.B) {
	m := newLatchManager()
	ctx := context.Background()
	for i := 0; i < 64; i++ {
		spans := make([]latchSpan, 100)
		for j := range spans {
			spans[j] = latchSpan{Start: keys.Key(fmt.Sprintf("h%03d-%03d", i, j))}
		}
		if _, err := m.Acquire(ctx, spans, latchExclusive); err != nil {
			b.Fatal(err)
		}
	}
	spans := make([]latchSpan, 100)
	for j := range spans {
		spans[j] = latchSpan{Start: keys.Key(fmt.Sprintf("x-%03d", j))}
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		g, err := m.Acquire(ctx, spans, latchExclusive)
		if err != nil {
			b.Fatal(err)
		}
		g.Release()
	}
}

// TestLatchWideBatchesInterleaved: a pushed bulk INSERT refreshes its
// uniqueness probes as one batch of point Refresh requests; that batch,
// and the wide write batches of other workers whose keys fall between its
// keys, latch only their own keys, so neither waits for the other.
func TestLatchWideBatchesInterleaved(t *testing.T) {
	key := func(i int) keys.Key { return append(keys.TableDataPrefix(433), fmt.Sprintf("%06d", i)...) }
	refresh, write := &kvpb.BatchRequest{}, &kvpb.BatchRequest{}
	for i := 0; i < 1000; i++ {
		refresh.Add(&kvpb.RefreshRequest{RequestHeader: kvpb.RequestHeader{Key: key(2 * i)}})
		write.Add(&kvpb.PutRequest{RequestHeader: kvpb.RequestHeader{Key: key(2*i + 1)}, Value: []byte("v")})
	}
	m := newLatchManager()
	rs, rm, err := latchSpans(refresh)
	if err != nil {
		t.Fatal(err)
	}
	for _, sp := range rs {
		if sp.End != nil {
			t.Fatalf("a point refresh latched the span %s-%s", sp.Start, sp.End)
		}
	}
	g1 := mustAcquire(t, m, rs, rm)
	defer g1.Release()
	ws, wm, err := latchSpans(write)
	if err != nil {
		t.Fatal(err)
	}
	g2 := mustAcquire(t, m, ws, wm)
	defer g2.Release()
	// One of the refreshed keys written: that write waits.
	ch := acquireBlocks(t, m, []latchSpan{{Start: key(500)}}, latchExclusive)
	g1.Release()
	(<-ch).Release()
}

func mustAcquireOpts(t *testing.T, m *latchManager, spans []latchSpan, mode latchMode, opts latchOpts) *latchGuard {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	g, err := m.AcquireOpts(ctx, spans, mode, opts)
	if err != nil {
		t.Fatalf("acquire: %v", err)
	}
	return g
}

// acquireOptsBlocks is acquireBlocks for AcquireOpts.
func acquireOptsBlocks(t *testing.T, m *latchManager, spans []latchSpan, mode latchMode, opts latchOpts) <-chan *latchGuard {
	t.Helper()
	ch := make(chan *latchGuard, 1)
	go func() {
		g, err := m.AcquireOpts(context.Background(), spans, mode, opts)
		if err == nil {
			ch <- g
		}
	}()
	select {
	case <-ch:
		t.Fatal("acquisition should have blocked")
	case <-time.After(50 * time.Millisecond):
	}
	return ch
}

func wake(t *testing.T, ch <-chan *latchGuard) *latchGuard {
	t.Helper()
	select {
	case g := <-ch:
		return g
	case <-time.After(2 * time.Second):
		t.Fatal("blocked acquisition never woke")
		return nil
	}
}

// TestLatchClosedTSBarrier: a closed-timestamp barrier at T neither waits
// for nor blocks a write whose latch carries a timestamp above T, and
// still drains — and blocks — every write that could land at or below T:
// one at T, one below, and one with no timestamp (transaction-record
// operations, intent resolution, GC).
func TestLatchClosedTSBarrier(t *testing.T) {
	ts := func(w int64) hlc.Timestamp { return hlc.Timestamp{WallTime: w} }
	k := span(string(keys.TableDataPrefix(7))+"k", "")
	barrier := closedTSBarrier(ts(10))

	// Above T: passes both ways.
	m := newLatchManager()
	w := mustAcquireOpts(t, m, []latchSpan{k}, latchExclusive, latchOpts{ts: ts(11)})
	mustAcquireOpts(t, m, []latchSpan{wholeRangeSpan}, latchShared, barrier).Release()
	w.Release()
	b := mustAcquireOpts(t, m, []latchSpan{wholeRangeSpan}, latchShared, barrier)
	mustAcquireOpts(t, m, []latchSpan{k}, latchExclusive, latchOpts{ts: ts(11)}).Release()
	b.Release()

	// At, below, or without a timestamp: the barrier waits for the write,
	// and a write that arrives while the barrier is held waits for it.
	for _, wts := range []hlc.Timestamp{ts(10), ts(9), {}} {
		t.Run(fmt.Sprintf("write@%s", wts), func(t *testing.T) {
			m := newLatchManager()
			w := mustAcquireOpts(t, m, []latchSpan{k}, latchExclusive, latchOpts{ts: wts})
			ch := acquireOptsBlocks(t, m, []latchSpan{wholeRangeSpan}, latchShared, barrier)
			w.Release()
			b := wake(t, ch)
			ch = acquireOptsBlocks(t, m, []latchSpan{k}, latchExclusive, latchOpts{ts: wts})
			b.Release()
			wake(t, ch).Release()
		})
	}

	// A plain shared latch (a read) still conflicts with a write at any
	// timestamp.
	m = newLatchManager()
	w = mustAcquireOpts(t, m, []latchSpan{k}, latchExclusive, latchOpts{ts: ts(11)})
	ch := acquireBlocks(t, m, []latchSpan{k}, latchShared)
	w.Release()
	wake(t, ch).Release()
}

// TestLatchRangeWideFair: a range-wide latch that waits is honored by
// every later conflicting acquirer, so a stream of point writes — each
// overlapping the next only through the range-wide waiter — cannot starve
// it. Before, the second write below acquired at once and the split kept
// waiting for as long as some write was in flight.
func TestLatchRangeWideFair(t *testing.T) {
	m := newLatchManager()
	w1 := mustAcquire(t, m, []latchSpan{span("m", "")}, latchExclusive)
	split := acquireOptsBlocks(t, m, []latchSpan{splitLatchSpan(keys.Key("k"))}, latchExclusive, latchOpts{fair: true, op: "split"})
	// A later write on the right-hand side queues behind the split.
	w2 := acquireBlocks(t, m, []latchSpan{span("p", "")}, latchExclusive)
	// One on the left-hand side does not conflict with it at all.
	mustAcquire(t, m, []latchSpan{span("c", "")}, latchExclusive).Release()
	w1.Release()
	g := wake(t, split)
	select {
	case <-w2:
		t.Fatal("the later write acquired before the split it queued behind released")
	case <-time.After(50 * time.Millisecond):
	}
	g.Release()
	wake(t, w2).Release()

	// A closed-timestamp barrier waiting for a write at T does not hold
	// up writes above T, which it does not conflict with.
	ts := func(w int64) hlc.Timestamp { return hlc.Timestamp{WallTime: w} }
	w1 = mustAcquireOpts(t, m, []latchSpan{span("m", "")}, latchExclusive, latchOpts{ts: ts(10)})
	barrier := acquireOptsBlocks(t, m, []latchSpan{wholeRangeSpan}, latchShared, closedTSBarrier(ts(10)))
	mustAcquireOpts(t, m, []latchSpan{span("p", "")}, latchExclusive, latchOpts{ts: ts(11)}).Release()
	// A read never conflicts with the barrier.
	mustAcquire(t, m, []latchSpan{span("n", "z")}, latchShared).Release()
	// A write at T does, and queues.
	w3 := acquireOptsBlocks(t, m, []latchSpan{span("p", "")}, latchExclusive, latchOpts{ts: ts(10)})
	w1.Release()
	b := wake(t, barrier)
	b.Release()
	wake(t, w3).Release()
}

// TestLatchFairWaiterAbandoned: a fair waiter that gives up wakes the
// acquirers queued behind it and leaves nothing behind.
func TestLatchFairWaiterAbandoned(t *testing.T) {
	m := newLatchManager()
	w1 := mustAcquire(t, m, []latchSpan{span("m", "")}, latchExclusive)
	ctx, cancel := context.WithCancel(context.Background())
	gaveUp := make(chan error, 1)
	go func() {
		_, err := m.AcquireOpts(ctx, []latchSpan{wholeRangeSpan}, latchExclusive, latchOpts{fair: true, op: "merge"})
		gaveUp <- err
	}()
	time.Sleep(50 * time.Millisecond)
	w2 := acquireBlocks(t, m, []latchSpan{span("p", "")}, latchExclusive)
	cancel()
	if err := <-gaveUp; err == nil {
		t.Fatal("a canceled fair acquirer acquired")
	}
	wake(t, w2).Release()
	w1.Release()
	m.mu.Lock()
	defer m.mu.Unlock()
	if len(m.held) != 0 || len(m.waiting) != 0 {
		t.Fatalf("residue: %d held, %d waiting", len(m.held), len(m.waiting))
	}
}

// TestLatchSplitRightSideOnly: a split's latch covers [split key, Max):
// it waits for, and blocks, requests on the right-hand side and those
// straddling the split key, and leaves the left-hand side alone.
func TestLatchSplitRightSideOnly(t *testing.T) {
	sk := string(keys.TableDataPrefix(9)) + "m"
	left, right := span(string(keys.TableDataPrefix(9))+"c", ""), span(sk, "")
	straddle := span(string(keys.TableDataPrefix(9))+"a", string(keys.TableDataPrefix(9))+"z")
	splitOpts := latchOpts{fair: true, op: "split"}
	for _, mode := range []latchMode{latchShared, latchExclusive} {
		for _, sp := range []latchSpan{right, straddle, span(string(keys.TableDataPrefix(10)), "")} {
			m := newLatchManager()
			g := mustAcquire(t, m, []latchSpan{sp}, mode)
			ch := acquireOptsBlocks(t, m, []latchSpan{splitLatchSpan(keys.Key(sk))}, latchExclusive, splitOpts)
			ch2 := acquireBlocks(t, m, []latchSpan{sp}, mode)
			g.Release()
			s := wake(t, ch)
			select {
			case <-ch2:
				t.Fatalf("%v (mode %d) acquired while the split held its latch", sp, mode)
			case <-time.After(20 * time.Millisecond):
			}
			s.Release()
			wake(t, ch2).Release()
		}
		m := newLatchManager()
		g := mustAcquire(t, m, []latchSpan{left}, mode)
		s := mustAcquireOpts(t, m, []latchSpan{splitLatchSpan(keys.Key(sk))}, latchExclusive, splitOpts)
		mustAcquire(t, m, []latchSpan{span(string(keys.TableDataPrefix(9))+"d", "")}, latchExclusive).Release()
		s.Release()
		g.Release()
	}
}

// TestLatchBatchOpts: the timestamp a batch's latch carries for barriers
// is the lowest one it can write an MVCC version at, and unknown (empty,
// conflicting with every barrier) whenever that cannot be told.
func TestLatchBatchOpts(t *testing.T) {
	ts := func(w int64) hlc.Timestamp { return hlc.Timestamp{WallTime: w} }
	k := keys.Key(string(keys.TableDataPrefix(8)) + "k")
	hdr := kvpb.RequestHeader{Key: k}
	txn := func(read, write int64) *kvpb.Transaction {
		return &kvpb.Transaction{ReadTimestamp: ts(read), TxnMeta: enginepb.TxnMeta{ID: uuid.New(), Key: k, WriteTimestamp: ts(write)}}
	}
	batch := func(h kvpb.BatchHeader, reqs ...kvpb.Request) *kvpb.BatchRequest {
		ba := &kvpb.BatchRequest{Header: h}
		for _, r := range reqs {
			ba.Add(r)
		}
		return ba
	}
	cases := []struct {
		name string
		ba   *kvpb.BatchRequest
		want latchOpts
	}{
		{"non-transactional put", batch(kvpb.BatchHeader{Timestamp: ts(5)}, &kvpb.PutRequest{RequestHeader: hdr}), latchOpts{ts: ts(5)}},
		{"pushed transaction's put: its read timestamp", batch(kvpb.BatchHeader{Txn: txn(4, 6)}, &kvpb.PutRequest{RequestHeader: hdr}), latchOpts{ts: ts(4)}},
		{"one-phase commit", batch(kvpb.BatchHeader{Txn: txn(7, 7)}, &kvpb.CPutRequest{RequestHeader: hdr}, &kvpb.EndTxnRequest{RequestHeader: hdr, Commit: true, All: true}), latchOpts{ts: ts(7)}},
		{"locking read", batch(kvpb.BatchHeader{Txn: txn(3, 9)}, &kvpb.GetRequest{RequestHeader: hdr, ForUpdate: true}), latchOpts{ts: ts(3)}},
		{"commit", batch(kvpb.BatchHeader{Txn: txn(3, 9)}, &kvpb.EndTxnRequest{RequestHeader: hdr, Commit: true}), latchOpts{noMVCC: true}},
		{"heartbeat", batch(kvpb.BatchHeader{Txn: txn(3, 9)}, &kvpb.HeartbeatTxnRequest{RequestHeader: hdr}), latchOpts{noMVCC: true}},
		{"push", batch(kvpb.BatchHeader{Timestamp: ts(9)}, &kvpb.PushTxnRequest{RequestHeader: hdr}), latchOpts{noMVCC: true}},
		{"committing resolution: its commit timestamp", batch(kvpb.BatchHeader{Timestamp: ts(9)}, &kvpb.ResolveIntentRequest{RequestHeader: hdr, Status: enginepb.COMMITTED, CommitTS: ts(2)}), latchOpts{ts: ts(2)}},
		{"aborting resolution", batch(kvpb.BatchHeader{Timestamp: ts(9)}, &kvpb.ResolveIntentRequest{RequestHeader: hdr, Status: enginepb.ABORTED}), latchOpts{}},
		{"resolution beside a put", batch(kvpb.BatchHeader{Timestamp: ts(9)}, &kvpb.PutRequest{RequestHeader: hdr}, &kvpb.ResolveIntentRequest{RequestHeader: hdr, Status: enginepb.ABORTED}), latchOpts{}},
		{"savepoint rollback", batch(kvpb.BatchHeader{Txn: txn(3, 9)}, &kvpb.RollbackIntentRequest{RequestHeader: hdr}), latchOpts{}},
		{"GC", batch(kvpb.BatchHeader{Timestamp: ts(9)}, &kvpb.GCRequest{RequestHeader: hdr}), latchOpts{}},
		{"no timestamp", batch(kvpb.BatchHeader{}, &kvpb.PutRequest{RequestHeader: hdr}), latchOpts{}},
	}
	for _, c := range cases {
		_, mode, err := latchSpans(c.ba)
		if err != nil {
			t.Fatal(err)
		}
		if got := batchLatchOpts(c.ba, mode); got != c.want {
			t.Errorf("%s: %+v, want %+v", c.name, got, c.want)
		}
	}
	// A read-only batch is a plain shared latch.
	ro := batch(kvpb.BatchHeader{Timestamp: ts(5)}, &kvpb.GetRequest{RequestHeader: hdr})
	if _, mode, _ := latchSpans(ro); mode != latchShared || batchLatchOpts(ro, mode) != (latchOpts{}) {
		t.Errorf("read-only batch: mode %d, opts %+v", mode, batchLatchOpts(ro, mode))
	}

	// A transaction-record operation in flight neither holds up a barrier
	// at any timestamp nor waits for one.
	m := newLatchManager()
	w := mustAcquireOpts(t, m, []latchSpan{{Start: k}}, latchExclusive, latchOpts{noMVCC: true})
	b := mustAcquireOpts(t, m, []latchSpan{wholeRangeSpan}, latchShared, closedTSBarrier(ts(100)))
	mustAcquireOpts(t, m, []latchSpan{{Start: k.Next()}}, latchExclusive, latchOpts{noMVCC: true}).Release()
	b.Release()
	w.Release()
}
