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
