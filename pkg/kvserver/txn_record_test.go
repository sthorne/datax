package kvserver

import (
	"fmt"
	"reflect"
	"testing"

	"github.com/google/uuid"

	"github.com/sthorne/datax/pkg/keys"
	"github.com/sthorne/datax/pkg/kvpb"
	"github.com/sthorne/datax/pkg/storage"
	"github.com/sthorne/datax/pkg/storage/enginepb"
	"github.com/sthorne/datax/pkg/util/hlc"
)

// TestTxnRecordBothEncodings (issue #141): a transaction record is stored
// as JSON for a transaction from before cluster version v14 and as
// protobuf for one flagged BinaryMeta; both read back whole, and the
// first byte tells them apart.
func TestTxnRecordBothEncodings(t *testing.T) {
	eng, err := storage.Open("", storage.Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = eng.Close() }()
	for _, binary := range []bool{false, true} {
		txn := kvpb.NewTransaction("t", 5, hlc.Timestamp{WallTime: 10})
		txn.Key = []byte("anchor")
		txn.BinaryMeta = binary
		txn.Status = enginepb.STAGING
		txn.LastHeartbeat = hlc.Timestamp{WallTime: 12, Logical: 1}
		txn.IntentKeys = []keys.Key{keys.Key("a"), keys.Key("b\x00c")}
		txn.InFlightKeys = []keys.Key{keys.Key("d")}
		txn.WaitingFor = uuid.New()
		txn.WaitingForKey = keys.Key("w")
		b := eng.NewBatch()
		key := txnRecordKey(&txn.TxnMeta)
		if err := putTxnRecord(b, key, txn); err != nil {
			t.Fatal(err)
		}
		raw, err := b.Get(key)
		if err != nil {
			t.Fatal(err)
		}
		if (raw[0] == '{') == binary {
			t.Fatalf("binary=%v: record opens with %q", binary, raw[:1])
		}
		got, err := loadTxnRecord(b, key)
		if err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(got, txn) {
			t.Fatalf("binary=%v round trip:\n got %+v\nwant %+v", binary, got, txn)
		}
		_ = b.Close()
	}
}

// TestGCEnumeratesBothRecordEncodings (issue #141): the GC pass finds
// finalized transaction records and live intents whichever encoding they
// were stored in, and leaves other range-local keys alone.
func TestGCEnumeratesBothRecordEncodings(t *testing.T) {
	eng, err := storage.Open("", storage.Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = eng.Close() }()
	desc := kvpb.RangeDescriptor{StartKey: keys.MinKey, EndKey: keys.MaxKey}
	b := eng.NewBatch()
	want := map[uuid.UUID]bool{}
	for i, binary := range []bool{false, true} {
		txn := kvpb.NewTransaction("t", 5, hlc.Timestamp{WallTime: 10})
		txn.Key = keys.Key(fmt.Sprintf("anchor%d", i))
		txn.BinaryMeta = binary
		txn.Status = enginepb.COMMITTED
		txn.LastHeartbeat = hlc.Timestamp{WallTime: 12}
		if err := putTxnRecord(b, txnRecordKey(&txn.TxnMeta), txn); err != nil {
			t.Fatal(err)
		}
		want[txn.ID] = false
		// A live intent of a third transaction in the same encoding: the
		// version enumerator must attribute it to its owner.
		intent := kvpb.NewTransaction("i", 5, hlc.Timestamp{WallTime: 11})
		intent.BinaryMeta = binary
		if err := storage.MVCCPut(b, keys.Key(fmt.Sprintf("k%d", i)), intent.WriteTimestamp, []byte("v"), &intent.TxnMeta); err != nil {
			t.Fatal(err)
		}
		want[intent.ID] = true
	}
	// A JSON range-local record that is not a transaction record.
	if err := b.Put(keys.RangeDescriptorKey(7), []byte(`{"id":7}`)); err != nil {
		t.Fatal(err)
	}
	if err := b.Commit(true); err != nil {
		t.Fatal(err)
	}
	_ = b.Close()

	snap := eng.NewSnapshot()
	defer func() { _ = snap.Close() }()
	threshold := hlc.Timestamp{WallTime: 100}
	_, _, live, err := enumerateGarbageVersions(snap, desc, threshold, false, nil)
	if err != nil {
		t.Fatal(err)
	}
	recs, err := enumerateGarbageTxnRecords(snap, desc, threshold, live)
	if err != nil {
		t.Fatal(err)
	}
	if len(recs) != 2 || len(live) != 2 {
		t.Fatalf("got %d reclaimable records, %d live intent owners; want 2, 2", len(recs), len(live))
	}
	for _, r := range recs {
		if isIntent, ok := want[r.txnID]; !ok || isIntent {
			t.Fatalf("unexpected reclaimable record for %s", r.txnID)
		}
	}
	for id := range live {
		if isIntent, ok := want[id]; !ok || !isIntent {
			t.Fatalf("unexpected live intent owner %s", id)
		}
	}
}

// TestRecoverTxnOnlyFinalizesVerifiedRecord: a push of a STAGING record
// reports the staged epoch and sequence status recovery needs, and a
// RecoverTxn naming the staged record it verified finalizes only that
// record. One naming another epoch, sequence or timestamp (the
// transaction re-staged since) leaves the record STAGING; one naming
// nothing (a recoverer from before cluster version v25) finalizes it as
// before.
func TestRecoverTxnOnlyFinalizesVerifiedRecord(t *testing.T) {
	eng, err := storage.Open("", storage.Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = eng.Close() }()
	r := &Replica{}
	for _, tt := range []struct {
		name     string
		edit     func(req *kvpb.RecoverTxnRequest)
		finalize bool
	}{
		{"verified", func(*kvpb.RecoverTxnRequest) {}, true},
		{"unguarded", func(req *kvpb.RecoverTxnRequest) { req.StagedTS = hlc.Timestamp{} }, true},
		{"other-epoch", func(req *kvpb.RecoverTxnRequest) { req.StagedEpoch = 0 }, false},
		{"other-sequence", func(req *kvpb.RecoverTxnRequest) { req.StagedSequence = 3 }, false},
		{"other-timestamp", func(req *kvpb.RecoverTxnRequest) { req.StagedTS = hlc.Timestamp{WallTime: 9} }, false},
	} {
		for _, commit := range []bool{true, false} {
			t.Run(fmt.Sprintf("%s/commit=%v", tt.name, commit), func(t *testing.T) {
				b := eng.NewBatch()
				defer func() { _ = b.Close() }()
				txn := kvpb.NewTransaction("t", 5, hlc.Timestamp{WallTime: 10})
				txn.Key = keys.Key("anchor")
				txn.Epoch, txn.Sequence = 1, 4
				txn.Status = enginepb.STAGING
				txn.InFlightKeys = []keys.Key{keys.Key("a")}
				key := txnRecordKey(&txn.TxnMeta)
				if err := putTxnRecord(b, key, txn); err != nil {
					t.Fatal(err)
				}
				push, perr := r.evalPushTxn(b, &kvpb.PushTxnRequest{PusheeTxn: txn.TxnMeta, Now: hlc.Timestamp{WallTime: 11}})
				if perr != nil {
					t.Fatal(perr)
				}
				if push.Status != enginepb.STAGING || push.StagedEpoch != 1 || push.StagedSequence != 4 ||
					!push.CommitTS.Equal(txn.WriteTimestamp) {
					t.Fatalf("push of a staged record = %+v", push)
				}
				req := &kvpb.RecoverTxnRequest{
					RequestHeader:  kvpb.RequestHeader{Key: txn.Key},
					TxnID:          txn.ID,
					Commit:         commit,
					StagedTS:       push.CommitTS,
					StagedEpoch:    push.StagedEpoch,
					StagedSequence: push.StagedSequence,
				}
				tt.edit(req)
				resp, rerr := r.evalRecoverTxn(b, req)
				if rerr != nil {
					t.Fatal(rerr)
				}
				want := enginepb.STAGING
				switch {
				case tt.finalize && commit:
					want = enginepb.COMMITTED
				case tt.finalize:
					want = enginepb.ABORTED
				}
				rec, err := loadTxnRecord(b, key)
				if err != nil {
					t.Fatal(err)
				}
				if resp.Status != want || rec.Status != want {
					t.Fatalf("recover answered %s, record %s; want %s", resp.Status, rec.Status, want)
				}
			})
		}
	}
}
