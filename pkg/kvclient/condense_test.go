package kvclient

import (
	"fmt"
	"testing"

	"github.com/sthorne/datax/pkg/keys"
)

// TestReadSpansCondense (issue #322): a transaction that reads more than
// maxReadSpans spans keeps a read set it can refresh — the overflow is
// condensed into a few spans covering every key it read — instead of
// giving the read set up, after which every push of its timestamp was a
// restart.
func TestReadSpansCondense(t *testing.T) {
	txn := &Txn{}
	key := func(i int) keys.Key { return keys.Key(fmt.Sprintf("k%06d", i)) }
	const n = 3*maxReadSpans + 7
	for i := 0; i < n; i++ {
		if i%10 == 0 {
			txn.recordRead(key(i), key(i+3)) // a scan now and then
			continue
		}
		txn.recordRead(key(i), nil)
	}
	if txn.mu.refreshUnusable {
		t.Fatal("the read set was given up")
	}
	if len(txn.mu.condensed) == 0 || len(txn.mu.condensed) > maxCondensedSpans || len(txn.mu.readSpans) > maxReadSpans {
		t.Fatalf("%d condensed spans, %d read spans", len(txn.mu.condensed), len(txn.mu.readSpans))
	}
	for i := 0; i < n; i++ {
		if !readSpansCover(txn.mu.condensed, key(i)) && !readSpansCover(txn.mu.readSpans, key(i)) {
			t.Fatalf("key %s read but not covered", key(i))
		}
	}
	// A point read at the end is covered, and the next key is not.
	got := condenseSpans([]readSpan{{start: keys.Key("b")}, {start: keys.Key("a"), end: keys.Key("b")}, {start: keys.Key("x")}}, 8)
	if len(got) != 2 || string(got[0].start) != "a" || !got[0].end.Equal(keys.Key("b").Next()) || !got[1].end.Equal(keys.Key("x").Next()) {
		t.Fatalf("condensed %+v", got)
	}
	if readSpansCover(got, keys.Key("c")) || !readSpansCover(got, keys.Key("x")) {
		t.Fatalf("cover of %+v", got)
	}
	// Merging closes the gaps between the closest keys first: the reads
	// of one keyspace merge before a span reaches across another.
	var spans []readSpan
	for i := 0; i < 10; i++ {
		spans = append(spans, readSpan{start: keys.Key(fmt.Sprintf("desc/%03d", i*7))}, readSpan{start: keys.Key(fmt.Sprintf("nsdb/%03d", i*7))})
	}
	got = condenseSpans(spans, 2)
	if len(got) != 2 || readSpansCover(got, keys.Key("lease/001")) || !readSpansCover(got, keys.Key("desc/003")) || !readSpansCover(got, keys.Key("nsdb/063")) {
		t.Fatalf("condensed to %d spans: %+v", len(got), got)
	}
}
