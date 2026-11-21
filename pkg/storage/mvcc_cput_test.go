package storage

import (
	"errors"
	"testing"

	"github.com/google/uuid"
	"github.com/sthorne/datax/pkg/keys"
	"github.com/sthorne/datax/pkg/storage/enginepb"
	"github.com/sthorne/datax/pkg/util/hlc"
)

// The conditional put: a write that succeeds only on a key with no live
// value at the read timestamp, reading and writing as a Get followed by
// a Put of the same transaction would — with a locking read's
// stale-snapshot rule.

func cputTS(w int64) hlc.Timestamp { return hlc.Timestamp{WallTime: w} }

func cputEngine(t *testing.T) *Engine {
	t.Helper()
	eng, err := Open("", Options{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = eng.Close() })
	return eng
}

func cputCommit(t *testing.T, eng *Engine, f func(b *Batch) error) {
	t.Helper()
	b := eng.NewBatch()
	if err := f(b); err != nil {
		_ = b.Close()
		t.Fatal(err)
	}
	if err := b.Commit(false); err != nil {
		t.Fatal(err)
	}
}

func cputTxn(k keys.Key, read, write int64) *enginepb.TxnMeta {
	return &enginepb.TxnMeta{ID: uuid.New(), Key: k, WriteTimestamp: cputTS(write), MinTimestamp: cputTS(read)}
}

// try runs one conditional put in a batch of its own, keeping its writes
// only if it succeeded.
func tryCPut(t *testing.T, eng *Engine, k keys.Key, readTS int64, v string, txn *enginepb.TxnMeta) error {
	t.Helper()
	b := eng.NewBatch()
	g := NewGetter(b)
	err := MVCCConditionalPut(b, g, k, cputTS(readTS), []byte(v), txn)
	g.Close()
	if err != nil {
		_ = b.Close()
		return err
	}
	if cerr := b.Commit(false); cerr != nil {
		t.Fatal(cerr)
	}
	return nil
}

func wantConditionFailed(t *testing.T, err error, k keys.Key) {
	t.Helper()
	var cfe *ConditionFailedError
	if !errors.As(err, &cfe) {
		t.Fatalf("want ConditionFailedError, got %v", err)
	}
	if !cfe.Key.Equal(k) {
		t.Fatalf("ConditionFailedError names %s, want %s", cfe.Key, k)
	}
}

func TestMVCCConditionalPutAbsentKey(t *testing.T) {
	eng := cputEngine(t)
	k := keys.Key("a")
	txn := cputTxn(k, 10, 10)
	if err := tryCPut(t, eng, k, 10, "v", txn); err != nil {
		t.Fatal(err)
	}
	// It wrote an intent of the transaction at its write timestamp,
	// visible to the transaction and a conflict to everyone else.
	if v, err := MVCCGet(eng, k, cputTS(10), MVCCGetOptions{Txn: txn}); err != nil || string(v) != "v" {
		t.Fatalf("own read: %q, %v", v, err)
	}
	var wie *WriteIntentError
	if _, err := MVCCGet(eng, k, cputTS(20), MVCCGetOptions{}); !errors.As(err, &wie) || wie.Intents[0].Txn.ID != txn.ID {
		t.Fatalf("foreign read: %v", err)
	}
}

func TestMVCCConditionalPutExistingValue(t *testing.T) {
	eng := cputEngine(t)
	k := keys.Key("a")
	cputCommit(t, eng, func(b *Batch) error { return MVCCPutCommitted(b, k, cputTS(5), []byte("old")) })

	err := tryCPut(t, eng, k, 10, "new", cputTxn(k, 10, 10))
	wantConditionFailed(t, err, k)
	// Nothing written: no intent, the old value stands.
	if raw, err := eng.Get(EncodeMVCCKey(k, hlc.Timestamp{})); err != nil || raw != nil {
		t.Fatalf("intent left behind: %q, %v", raw, err)
	}
	if v, err := MVCCGet(eng, k, cputTS(30), MVCCGetOptions{}); err != nil || string(v) != "old" {
		t.Fatalf("value after failed put: %q, %v", v, err)
	}

	// A version exactly at the read timestamp is seen, as a Get sees it.
	k2 := keys.Key("b")
	cputCommit(t, eng, func(b *Batch) error { return MVCCPutCommitted(b, k2, cputTS(10), []byte("old")) })
	wantConditionFailed(t, tryCPut(t, eng, k2, 10, "new", cputTxn(k2, 10, 10)), k2)
}

func TestMVCCConditionalPutTombstoneIsAbsent(t *testing.T) {
	eng := cputEngine(t)
	k := keys.Key("a")
	cputCommit(t, eng, func(b *Batch) error { return MVCCPutCommitted(b, k, cputTS(5), []byte("old")) })
	cputCommit(t, eng, func(b *Batch) error { return MVCCDeleteCommitted(b, k, cputTS(7)) })
	txn := cputTxn(k, 10, 10)
	if err := tryCPut(t, eng, k, 10, "new", txn); err != nil {
		t.Fatalf("a deleted key is absent: %v", err)
	}
	if v, err := MVCCGet(eng, k, cputTS(10), MVCCGetOptions{Txn: txn}); err != nil || string(v) != "new" {
		t.Fatalf("own read: %q, %v", v, err)
	}
	// Read below the tombstone, the key is live: the condition fails.
	k2 := keys.Key("b")
	cputCommit(t, eng, func(b *Batch) error { return MVCCPutCommitted(b, k2, cputTS(5), []byte("old")) })
	cputCommit(t, eng, func(b *Batch) error { return MVCCDeleteCommitted(b, k2, cputTS(7)) })
	wantConditionFailed(t, tryCPut(t, eng, k2, 6, "new", cputTxn(k2, 6, 6)), k2)
}

// A committed version above the read timestamp — the snapshot is stale,
// whether or not the version is inside the uncertainty interval — fails
// the put with WriteTooOld, as it fails a locking read: the transaction
// moves past it and puts again, then seeing the value.
func TestMVCCConditionalPutVersionAboveRead(t *testing.T) {
	eng := cputEngine(t)
	k := keys.Key("a")
	cputCommit(t, eng, func(b *Batch) error { return MVCCPutCommitted(b, k, cputTS(15), []byte("newer")) })
	err := tryCPut(t, eng, k, 10, "v", cputTxn(k, 10, 10))
	var wto *WriteTooOldError
	if !errors.As(err, &wto) {
		t.Fatalf("want WriteTooOld, got %v", err)
	}
	if !cputTS(15).Less(wto.ActualTimestamp) {
		t.Fatalf("retry hint %s not above the version at 15", wto.ActualTimestamp)
	}
	// Even a pushed write timestamp above the version does not help: the
	// read at 10 missed it.
	if err := tryCPut(t, eng, k, 10, "v", cputTxn(k, 10, 20)); !errors.As(err, &wto) {
		t.Fatalf("pushed write: want WriteTooOld, got %v", err)
	}
	// At a read timestamp above it, the condition fails.
	wantConditionFailed(t, tryCPut(t, eng, k, 16, "v", cputTxn(k, 16, 16)), k)

	// A tombstone above the read timestamp is a stale snapshot too.
	k2 := keys.Key("b")
	cputCommit(t, eng, func(b *Batch) error { return MVCCDeleteCommitted(b, k2, cputTS(15)) })
	if err := tryCPut(t, eng, k2, 10, "v", cputTxn(k2, 10, 10)); !errors.As(err, &wto) {
		t.Fatalf("tombstone above read: want WriteTooOld, got %v", err)
	}
}

func TestMVCCConditionalPutForeignIntent(t *testing.T) {
	eng := cputEngine(t)
	for _, tc := range []struct {
		name     string
		intentTS int64
		value    string // "" = a deletion intent
	}{
		{"below the read", 5, "v"},
		{"above the read", 15, "v"},
		{"deletion below the read", 5, ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			k := keys.Key("k-" + tc.name)
			other := cputTxn(k, tc.intentTS, tc.intentTS)
			cputCommit(t, eng, func(b *Batch) error {
				if tc.value == "" {
					return MVCCDelete(b, k, other.WriteTimestamp, other)
				}
				return MVCCPut(b, k, other.WriteTimestamp, []byte(tc.value), other)
			})
			err := tryCPut(t, eng, k, 10, "mine", cputTxn(k, 10, 10))
			var wie *WriteIntentError
			if !errors.As(err, &wie) || wie.Intents[0].Txn.ID != other.ID {
				t.Fatalf("want the other transaction's intent, got %v", err)
			}
		})
	}
	// Above the read, over a committed value: the value is what the
	// transaction's snapshot holds, so the condition fails without
	// waiting for the intent (which can only resolve above the read).
	k := keys.Key("over-value")
	cputCommit(t, eng, func(b *Batch) error { return MVCCPutCommitted(b, k, cputTS(5), []byte("old")) })
	other := cputTxn(k, 15, 15)
	cputCommit(t, eng, func(b *Batch) error { return MVCCDelete(b, k, other.WriteTimestamp, other) })
	wantConditionFailed(t, tryCPut(t, eng, k, 10, "mine", cputTxn(k, 10, 10)), k)
}

func TestMVCCConditionalPutOwnWrites(t *testing.T) {
	eng := cputEngine(t)
	k := keys.Key("a")
	txn := cputTxn(k, 10, 10)

	// The transaction's own earlier write is a live value.
	cputCommit(t, eng, func(b *Batch) error { return MVCCPut(b, k, txn.WriteTimestamp, []byte("first"), txn) })
	wantConditionFailed(t, tryCPut(t, eng, k, 10, "second", txn), k)

	// Its own earlier deletion is not: the put rewrites the intent.
	k2 := keys.Key("b")
	cputCommit(t, eng, func(b *Batch) error { return MVCCPutCommitted(b, k2, cputTS(5), []byte("old")) })
	cputCommit(t, eng, func(b *Batch) error { return MVCCDelete(b, k2, txn.WriteTimestamp, txn) })
	if err := tryCPut(t, eng, k2, 10, "again", txn); err != nil {
		t.Fatalf("after own delete: %v", err)
	}
	if v, err := MVCCGet(eng, k2, cputTS(10), MVCCGetOptions{Txn: txn}); err != nil || string(v) != "again" {
		t.Fatalf("own read: %q, %v", v, err)
	}

	// A write of an earlier epoch is not the transaction's any more: the
	// key is absent to the new epoch, and its put replaces the intent.
	k3 := keys.Key("c")
	cputCommit(t, eng, func(b *Batch) error { return MVCCPut(b, k3, txn.WriteTimestamp, []byte("epoch0"), txn) })
	next := *txn
	next.Epoch = 1
	next.WriteTimestamp = cputTS(12)
	if err := tryCPut(t, eng, k3, 12, "epoch1", &next); err != nil {
		t.Fatalf("over an older epoch's intent: %v", err)
	}
	if v, err := MVCCGet(eng, k3, cputTS(12), MVCCGetOptions{Txn: &next}); err != nil || string(v) != "epoch1" {
		t.Fatalf("new epoch's read: %q, %v", v, err)
	}
}

// Within one batch the put sees the batch's earlier writes, as a Get
// through the batch's getter does.
func TestMVCCConditionalPutSeesItsBatch(t *testing.T) {
	eng := cputEngine(t)
	k := keys.Key("a")
	txn := cputTxn(k, 10, 10)
	b := eng.NewBatch()
	defer func() { _ = b.Close() }()
	g := NewGetter(b)
	defer g.Close()
	if err := MVCCConditionalPut(b, g, k, cputTS(10), []byte("one"), txn); err != nil {
		t.Fatal(err)
	}
	wantConditionFailed(t, MVCCConditionalPut(b, g, k, cputTS(10), []byte("two"), txn), k)
}

func TestMVCCConditionalPutNonTransactional(t *testing.T) {
	eng := cputEngine(t)
	k := keys.Key("a")
	// Outside a transaction the latest value counts, whatever its
	// timestamp.
	cputCommit(t, eng, func(b *Batch) error { return MVCCPutCommitted(b, k, cputTS(50), []byte("later")) })
	wantConditionFailed(t, tryCPut(t, eng, k, 10, "v", nil), k)
	k2 := keys.Key("b")
	cputCommit(t, eng, func(b *Batch) error { return MVCCDeleteCommitted(b, k2, cputTS(50)) })
	if err := tryCPut(t, eng, k2, 10, "v", nil); err != nil {
		t.Fatal(err)
	}
	if v, err := MVCCGet(eng, k2, cputTS(100), MVCCGetOptions{}); err != nil || string(v) != "v" {
		t.Fatalf("read back: %q, %v", v, err)
	}
}

func TestMVCCConditionalPutCommitted(t *testing.T) {
	eng := cputEngine(t)
	try := func(k keys.Key, ts int64, v string) error {
		b := eng.NewBatch()
		err := MVCCConditionalPutCommitted(b, nil, k, cputTS(ts), []byte(v))
		if err != nil {
			_ = b.Close()
			return err
		}
		if cerr := b.Commit(false); cerr != nil {
			t.Fatal(cerr)
		}
		return nil
	}
	k := keys.Key("a")
	if err := try(k, 10, "v"); err != nil {
		t.Fatal(err)
	}
	if raw, err := eng.Get(EncodeMVCCKey(k, hlc.Timestamp{})); err != nil || raw != nil {
		t.Fatalf("one-phase put left intent metadata: %q, %v", raw, err)
	}
	wantConditionFailed(t, try(k, 20, "w"), k)

	var wto *WriteTooOldError
	k2 := keys.Key("b")
	cputCommit(t, eng, func(b *Batch) error { return MVCCDeleteCommitted(b, k2, cputTS(30)) })
	if err := try(k2, 20, "v"); !errors.As(err, &wto) {
		t.Fatalf("version above: want WriteTooOld, got %v", err)
	}
	if err := try(k2, 40, "v"); err != nil {
		t.Fatalf("over a tombstone: %v", err)
	}

	k3 := keys.Key("c")
	other := cputTxn(k3, 50, 50)
	cputCommit(t, eng, func(b *Batch) error { return MVCCPut(b, k3, other.WriteTimestamp, []byte("x"), other) })
	var wie *WriteIntentError
	if err := try(k3, 20, "v"); !errors.As(err, &wie) {
		t.Fatalf("intent above: want WriteIntentError, got %v", err)
	}
	if err := try(k3, 60, "v"); !errors.As(err, &wie) {
		t.Fatalf("intent below: want WriteIntentError, got %v", err)
	}
}
