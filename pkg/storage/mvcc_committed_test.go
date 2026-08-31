package storage

import (
	"errors"
	"testing"

	"github.com/google/uuid"
	"github.com/sthorne/datax/pkg/keys"
	"github.com/sthorne/datax/pkg/storage/enginepb"
	"github.com/sthorne/datax/pkg/util/hlc"
)

// The one-phase-commit write primitives: committed values at exactly the
// given timestamp, transactional conflict semantics (WriteTooOld on a
// newer version, WriteIntentError on any intent), never the nil-txn
// timestamp ratchet.

func TestMVCCPutCommitted(t *testing.T) {
	eng, err := Open("", Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = eng.Close() }()
	ts := func(w int64) hlc.Timestamp { return hlc.Timestamp{WallTime: w} }
	k := keys.Key("a")

	b := eng.NewBatch()
	if err := MVCCPutCommitted(b, k, ts(10), []byte("v1")); err != nil {
		t.Fatal(err)
	}
	if err := b.Commit(false); err != nil {
		t.Fatal(err)
	}
	if v, err := MVCCGet(eng, k, ts(11), MVCCGetOptions{}); err != nil || string(v) != "v1" {
		t.Fatalf("read back: %q, %v", v, err)
	}
	// No intent metadata was written.
	if raw, err := eng.Get(EncodeMVCCKey(k, hlc.Timestamp{})); err != nil || raw != nil {
		t.Fatalf("unexpected intent metadata: %q, %v", raw, err)
	}

	// Writing at or below the existing version is WriteTooOld — never a
	// silent timestamp bump.
	b = eng.NewBatch()
	err = MVCCPutCommitted(b, k, ts(10), []byte("v2"))
	var wto *WriteTooOldError
	if !errors.As(err, &wto) {
		t.Fatalf("write at existing version: %v", err)
	}
	if wto.ActualTimestamp != ts(10).Next() {
		t.Fatalf("retry hint %v", wto.ActualTimestamp)
	}
	_ = b.Close()

	// A newer committed write goes through; a tombstone deletes.
	b = eng.NewBatch()
	if err := MVCCDeleteCommitted(b, k, ts(20)); err != nil {
		t.Fatal(err)
	}
	if err := b.Commit(false); err != nil {
		t.Fatal(err)
	}
	if v, err := MVCCGet(eng, k, ts(21), MVCCGetOptions{}); err != nil || v != nil {
		t.Fatalf("post-delete read: %q, %v", v, err)
	}
}

func TestMVCCPutCommittedIntentConflict(t *testing.T) {
	eng, err := Open("", Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = eng.Close() }()
	k := keys.Key("b")
	other := &enginepb.TxnMeta{ID: uuid.New(), Key: k, WriteTimestamp: hlc.Timestamp{WallTime: 5}}

	b := eng.NewBatch()
	if err := MVCCPut(b, k, other.WriteTimestamp, []byte("intent"), other); err != nil {
		t.Fatal(err)
	}
	if err := b.Commit(false); err != nil {
		t.Fatal(err)
	}

	b = eng.NewBatch()
	err = MVCCPutCommitted(b, k, hlc.Timestamp{WallTime: 10}, []byte("mine"))
	var wie *WriteIntentError
	if !errors.As(err, &wie) || len(wie.Intents) != 1 || wie.Intents[0].Txn.ID != other.ID {
		t.Fatalf("intent conflict: %v", err)
	}
	_ = b.Close()
}
