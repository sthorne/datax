package kvserver

import (
	"context"
	"testing"
	"time"

	"github.com/sthorne/datax/pkg/keys"
	"github.com/sthorne/datax/pkg/kvpb"
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
