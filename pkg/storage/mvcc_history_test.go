package storage

import (
	"fmt"
	"testing"

	"github.com/google/uuid"

	"github.com/sthorne/datax/pkg/keys"
	"github.com/sthorne/datax/pkg/storage/enginepb"
	"github.com/sthorne/datax/pkg/util/hlc"
)

// intentMeta reads key's intent metadata from the batch.
func intentMeta(t *testing.T, b *Batch, key keys.Key) enginepb.MVCCMetadata {
	t.Helper()
	metaKey, _ := mvccKeyBounds(key)
	raw, err := b.Get(metaKey)
	if err != nil || raw == nil {
		t.Fatalf("intent metadata for %s: %q, %v", key, raw, err)
	}
	meta, err := decodeMeta(raw)
	if err != nil {
		t.Fatal(err)
	}
	return meta
}

// TestIntentHistoryBounded (issue #162): a same-epoch rewrite of an intent
// keeps only the superseded values a savepoint rollback could restore.
// No live savepoint: none. The oldest live savepoint at sequence F: the
// newest entry at or below F and everything above it. Two entries at one
// sequence collapse to the later. A coordinator that says nothing (floor
// 0) keeps everything, as before.
func TestIntentHistoryBounded(t *testing.T) {
	eng := openTestEngine(t)
	key := keys.Key("hot")
	ts := hlc.Timestamp{WallTime: 100}
	write := func(b *Batch, txn *enginepb.TxnMeta, seq, floor int32, val string) {
		t.Helper()
		txn.Sequence, txn.HistoryFloor = seq, floor
		if err := MVCCPut(b, key, ts, []byte(val), txn); err != nil {
			t.Fatal(err)
		}
	}
	seqs := func(h []enginepb.IntentValue) []int32 {
		out := make([]int32, len(h))
		for i, e := range h {
			out[i] = e.Sequence
		}
		return out
	}

	// No savepoints: the history stays empty however often the key is
	// rewritten, and the metadata does not grow.
	b := eng.NewBatch()
	txn := &enginepb.TxnMeta{ID: uuid.New(), WriteTimestamp: ts}
	for i := int32(1); i <= 10; i++ {
		write(b, txn, i, -1, fmt.Sprintf("v%d", i))
	}
	size10 := len(encodeMeta(intentMeta(t, b, key))) // two-digit sequence, like 64's
	for i := int32(11); i <= 64; i++ {
		write(b, txn, i, -1, fmt.Sprintf("v%d", i))
	}
	meta := intentMeta(t, b, key)
	if len(meta.History) != 0 || len(encodeMeta(meta)) != size10 {
		t.Fatalf("no savepoints: %d history entries, %d bytes (was %d)", len(meta.History), len(encodeMeta(meta)), size10)
	}
	_ = b.Close()

	// A savepoint at F=5 taken after writes at 3 and 5, then writes at 7,
	// 9, 11: the entry for 3 is unreachable (5 is the newest at or below
	// F), 5 restores the savepoint's state, 7 and 9 may serve a later
	// savepoint the server does not know about.
	b = eng.NewBatch()
	txn = &enginepb.TxnMeta{ID: uuid.New(), WriteTimestamp: ts}
	write(b, txn, 3, -1, "a")
	write(b, txn, 5, -1, "b")
	write(b, txn, 7, 6, "c")
	write(b, txn, 9, 6, "d")
	write(b, txn, 11, 6, "e")
	meta = intentMeta(t, b, key)
	if got := seqs(meta.History); fmt.Sprint(got) != "[5 7 9]" {
		t.Fatalf("history with a savepoint at 5: %v, want [5 7 9]", got)
	}
	// A rollback to the savepoint restores b.
	if err := MVCCRollbackIntent(b, key, txn.ID, 5); err != nil {
		t.Fatal(err)
	}
	if v, err := MVCCGet(b, key, ts, MVCCGetOptions{Txn: txn}); err != nil || string(v) != "b" {
		t.Fatalf("after rollback to 5: %q, %v", v, err)
	}
	// The savepoint released: the next rewrite drops what is left.
	write(b, txn, 13, -1, "f")
	if meta = intentMeta(t, b, key); len(meta.History) != 0 {
		t.Fatalf("after the savepoint's release: %v", seqs(meta.History))
	}
	_ = b.Close()

	// The savepoint taken before any write (F=0): the first write's value
	// is what a rollback restores — the intent itself goes — so nothing
	// at or below 0 exists to keep, and every later entry is kept.
	b = eng.NewBatch()
	txn = &enginepb.TxnMeta{ID: uuid.New(), WriteTimestamp: ts}
	write(b, txn, 1, 1, "a")
	write(b, txn, 2, 1, "b")
	write(b, txn, 3, 1, "c")
	if got := seqs(intentMeta(t, b, key).History); fmt.Sprint(got) != "[1 2]" {
		t.Fatalf("savepoint at 0: %v, want [1 2]", got)
	}
	if err := MVCCRollbackIntent(b, key, txn.ID, 0); err != nil {
		t.Fatal(err)
	}
	if v, err := MVCCGet(b, key, ts, MVCCGetOptions{Txn: txn}); err != nil || v != nil {
		t.Fatalf("after rollback to 0 the key should be unwritten: %q, %v", v, err)
	}
	_ = b.Close()

	// Two writes at one sequence: the later replaces the earlier.
	b = eng.NewBatch()
	txn = &enginepb.TxnMeta{ID: uuid.New(), WriteTimestamp: ts}
	write(b, txn, 2, 1, "a")
	write(b, txn, 2, 1, "b")
	write(b, txn, 4, 1, "c")
	meta = intentMeta(t, b, key)
	if got := seqs(meta.History); fmt.Sprint(got) != "[2]" || string(meta.History[0].Value) != "b" {
		t.Fatalf("duplicate sequence: %v / %q, want [2] / b", got, meta.History[0].Value)
	}
	_ = b.Close()

	// Floor 0: an old coordinator; everything is kept.
	b = eng.NewBatch()
	txn = &enginepb.TxnMeta{ID: uuid.New(), WriteTimestamp: ts}
	for i := int32(1); i <= 8; i++ {
		write(b, txn, i, 0, "x")
	}
	if n := len(intentMeta(t, b, key).History); n != 7 {
		t.Fatalf("floor 0: %d entries, want 7", n)
	}
	_ = b.Close()
}

// BenchmarkIntentRewriteDepth: the cost of one more write to a key a
// transaction has already written depth times (issue #162), without a
// savepoint (history dropped) and under a savepoint taken before the
// first write (history kept, as before the change).
func BenchmarkIntentRewriteDepth(b *testing.B) {
	eng, err := Open("", Options{})
	if err != nil {
		b.Fatal(err)
	}
	defer func() { _ = eng.Close() }()
	ts := hlc.Timestamp{WallTime: 100}
	val := make([]byte, 128)
	for _, floor := range []int32{-1, 1} {
		for _, depth := range []int{1, 16, 64} {
			b.Run(fmt.Sprintf("floor=%d/depth=%d", floor, depth), func(b *testing.B) {
				b.ReportAllocs()
				for i := 0; i < b.N; i++ {
					b.StopTimer()
					bt := eng.NewBatch()
					key := keys.Key(fmt.Sprintf("k%d", i))
					txn := &enginepb.TxnMeta{ID: uuid.New(), WriteTimestamp: ts, HistoryFloor: floor}
					for d := 0; d < depth; d++ {
						txn.Sequence++
						if err := MVCCPut(bt, key, ts, val, txn); err != nil {
							b.Fatal(err)
						}
					}
					txn.Sequence++
					b.StartTimer()
					if err := MVCCPut(bt, key, ts, val, txn); err != nil {
						b.Fatal(err)
					}
					b.StopTimer()
					_ = bt.Close()
					b.StartTimer()
				}
			})
		}
	}
}
