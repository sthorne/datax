package kvclient

import (
	"fmt"
	"strings"
	"testing"

	"github.com/sthorne/datax/pkg/keys"
)

func spansOf(t *Txn) string {
	var out []string
	for _, sp := range t.mu.readSpans {
		if len(sp.end) == 0 {
			out = append(out, string(sp.start))
			continue
		}
		out = append(out, fmt.Sprintf("[%s,%s)", string(sp.start), string(sp.end)))
	}
	return strings.Join(out, " ")
}

// TestForgetSpan: forgetting a span drops the point reads inside it and
// cuts the scans that overlap it back to their parts outside it; reads
// recorded before the mark are untouched.
func TestForgetSpan(t *testing.T) {
	k := func(s string) keys.Key { return keys.Key(s) }
	txn := &Txn{}
	txn.recordRead(k("d"), nil)    // an earlier statement's point read
	txn.recordRead(k("a"), k("z")) // and its scan
	mark := txn.ReadMark()
	txn.recordRead(k("a"), k("z")) // straddles the span: both ends kept
	txn.recordRead(k("a"), k("e")) // ends inside it
	txn.recordRead(k("f"), k("g")) // wholly inside it
	txn.recordRead(k("g"), k("p")) // starts inside it
	txn.recordRead(k("q"), k("r")) // beyond it
	txn.recordRead(k("d"), nil)    // a point inside it
	txn.recordRead(k("c"), nil)    // a point before it
	txn.recordRead(k("m"), nil)    // a point at its end: kept
	txn.ForgetSpan(mark, k("d"), k("m"))
	want := "d [a,z) [a,d) [m,z) [a,d) [m,p) [q,r) c m"
	if got := spansOf(txn); got != want {
		t.Fatalf("read spans after forgetting [d, m):\n got %s\nwant %s", got, want)
	}
	txn.ForgetSpan(mark, k("m"), k("d")) // empty: nothing changes
	if got := spansOf(txn); got != want {
		t.Fatalf("an empty span changed the reads: %s", got)
	}
}

// TestForgetReads: forgetting a key drops its point read and splits the
// scans that cover it, leaving the gaps on either side read; reads
// recorded before the mark are untouched.
func TestForgetReads(t *testing.T) {
	k := func(s string) keys.Key { return keys.Key(s) }
	next := func(s string) string { return string(k(s).Next()) }

	txn := &Txn{}
	txn.recordRead(k("b"), nil)    // an earlier statement's point read
	txn.recordRead(k("a"), k("z")) // and its scan
	mark := txn.ReadMark()
	txn.recordRead(k("a"), k("m"))
	txn.recordRead(k("b"), nil)
	txn.recordRead(k("c"), nil)
	txn.recordRead(k("m"), k("p"))

	txn.ForgetReads(mark, []keys.Key{k("f"), k("b"), k("a"), k("o"), k("x")})
	want := fmt.Sprintf("b [a,z) [%s,b) [%s,f) [%s,m) c [m,o) [%s,p)", next("a"), next("b"), next("f"), next("o"))
	if got := spansOf(txn); got != want {
		t.Fatalf("read spans after forgetting a, b, f, o:\n got %s\nwant %s", got, want)
	}

	// Forgetting the only key of a span leaves nothing of it behind.
	txn = &Txn{}
	txn.recordRead(k("a"), k("a").Next())
	txn.ForgetReads(0, []keys.Key{k("a")})
	if got := spansOf(txn); got != "" {
		t.Fatalf("a span of one forgotten key survived: %s", got)
	}

	// More holes than the read set can hold: refresh is given up, as it
	// is when plain reads overflow it.
	txn = &Txn{}
	txn.recordRead(k("k"), k("l"))
	var holes []keys.Key
	for i := 0; i < maxReadSpans+1; i++ {
		holes = append(holes, k(fmt.Sprintf("k%04d", i)))
	}
	txn.ForgetReads(0, holes)
	if !txn.mu.refreshUnusable || len(txn.mu.readSpans) != 0 {
		t.Fatalf("overflowing read set: refreshUnusable=%v, %d spans", txn.mu.refreshUnusable, len(txn.mu.readSpans))
	}
}
