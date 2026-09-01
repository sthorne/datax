package kvpb

import (
	"reflect"
	"testing"

	"github.com/google/uuid"

	"github.com/sthorne/datax/pkg/base"
	"github.com/sthorne/datax/pkg/keys"
	"github.com/sthorne/datax/pkg/storage"
	"github.com/sthorne/datax/pkg/storage/enginepb"
	"github.com/sthorne/datax/pkg/util/hlc"
)

func mkts(w int64) hlc.Timestamp { return hlc.Timestamp{WallTime: w, Logical: 2} }

func testTxn() *Transaction {
	return &Transaction{
		TxnMeta: enginepb.TxnMeta{
			ID:             uuid.New(),
			Key:            []byte("anchor"),
			Epoch:          3,
			WriteTimestamp: mkts(100),
			MinTimestamp:   mkts(90),
			Priority:       7,
		},
		Name:          "test",
		Status:        enginepb.PENDING,
		ReadTimestamp: mkts(95),
		LastHeartbeat: mkts(99),
		IntentKeys:    []keys.Key{keys.Key("a"), keys.Key("b")},
	}
}

func testDesc() RangeDescriptor {
	return RangeDescriptor{
		RangeID:  7,
		StartKey: keys.Key("a"),
		EndKey:   keys.Key("z"),
		Replicas: []ReplicaDescriptor{
			{NodeID: 1, StoreID: 1, ReplicaID: 1},
			{NodeID: 2, StoreID: 2, ReplicaID: 2},
		},
		NextReplicaID: 3,
		Generation:    9,
	}
}

// TestBatchRequestProtoRoundTrip: every request type survives the proto
// round trip byte-for-byte at the Go struct level.
func TestBatchRequestProtoRoundTrip(t *testing.T) {
	h := func(k string) RequestHeader { return RequestHeader{Key: keys.Key(k)} }
	hr := func(k, e string) RequestHeader { return RequestHeader{Key: keys.Key(k), EndKey: keys.Key(e)} }
	ba := &BatchRequest{Header: BatchHeader{
		Timestamp:       mkts(50),
		Txn:             testTxn(),
		RangeID:         4,
		CreateTxnRecord: true,
		StaleRead:       true,
	}}
	ba.Add(&GetRequest{RequestHeader: h("g")})
	ba.Add(&PutRequest{RequestHeader: h("p"), Value: []byte("v")})
	ba.Add(&DeleteRequest{RequestHeader: h("d")})
	ba.Add(&IncrementRequest{RequestHeader: h("i"), By: -4})
	ba.Add(&ScanRequest{RequestHeader: hr("a", "b"), MaxRows: 10})
	ba.Add(&ScanRequest{RequestHeader: hr("a", "b"), MaxRows: 10, Reverse: true})
	ba.Add(&EndTxnRequest{RequestHeader: h("e"), Commit: true, IntentKeys: []keys.Key{keys.Key("x")}})
	ba.Add(&HeartbeatTxnRequest{RequestHeader: h("h"), Now: mkts(60)})
	ba.Add(&PushTxnRequest{RequestHeader: h("q"), PusherTxn: testTxn(), PusheeTxn: testTxn().TxnMeta, PushAbort: true, Now: mkts(61)})
	ba.Add(&ResolveIntentRequest{RequestHeader: h("r"), TxnID: uuid.New(), Status: enginepb.COMMITTED, CommitTS: mkts(62)})
	ba.Add(&RefreshRequest{RequestHeader: hr("c", "d"), FromTS: mkts(40)})
	ba.Add(&GCRequest{RequestHeader: hr("a", "z"), Threshold: mkts(10),
		Versions:      []GCVersion{{Key: keys.Key("k"), TS: mkts(5), Bytes: 33}},
		TxnRecordKeys: []keys.Key{keys.Key("t")}})
	ba.Add(&TruncateLogRequest{RequestHeader: h("a"), Index: 100, Term: 3})
	ba.Add(&AdminSplitRequest{RequestHeader: h("m")})
	ba.Add(&AdminChangeReplicasRequest{RequestHeader: h("n"), AddNode: 4, RemoveNode: 2})
	ba.Add(&AdminTransferLeaseRequest{RequestHeader: h("o"), Target: 3})
	ba.Add(&AdminMergeRequest{RequestHeader: h("u")})
	ba.Add(&SubsumeRequest{RequestHeader: hr("s", "t"), MergeInto: base.RangeID(2)})
	ba.Add(&UnfreezeRequest{RequestHeader: hr("s", "t")})

	data, err := MarshalBatchRequest(ba)
	if err != nil {
		t.Fatal(err)
	}
	got, err := UnmarshalBatchRequest(data)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(ba, got) {
		t.Fatalf("round trip mismatch:\n got %+v\nwant %+v", got, ba)
	}
}

// TestBatchEnvelopeProtoRoundTrip covers responses (every type) and each
// error detail.
func TestBatchEnvelopeProtoRoundTrip(t *testing.T) {
	br := &BatchResponse{Txn: testTxn(), Timestamp: mkts(70)}
	br.Responses = []ResponseUnion{
		{Get: &GetResponse{Value: []byte("v")}},
		{Get: &GetResponse{}}, // not found: nil Value must survive
		{Put: &PutResponse{}},
		{Delete: &DeleteResponse{}},
		{Increment: &IncrementResponse{NewValue: 9}},
		{Scan: &ScanResponse{Rows: []KeyValue{{Key: keys.Key("k"), Value: []byte("v")}}, Resume: keys.Key("r")}},
		{EndTxn: &EndTxnResponse{CommitTimestamp: mkts(71)}},
		{HeartbeatTxn: &HeartbeatTxnResponse{Status: enginepb.ABORTED}},
		{PushTxn: &PushTxnResponse{Status: enginepb.COMMITTED, CommitTS: mkts(72)}},
		{ResolveIntent: &ResolveIntentResponse{}},
		{Refresh: &RefreshResponse{}},
		{GC: &GCResponse{}},
		{TruncateLog: &TruncateLogResponse{}},
		{AdminSplit: &AdminSplitResponse{Left: testDesc(), Right: testDesc()}},
		{AdminChangeReplicas: &AdminChangeReplicasResponse{Desc: testDesc()}},
		{AdminTransferLease: &AdminTransferLeaseResponse{Desc: testDesc()}},
		{AdminMerge: &AdminMergeResponse{Desc: testDesc()}},
		{Subsume: &SubsumeResponse{}},
		{Unfreeze: &UnfreezeResponse{}},
	}
	errs := []*Error{
		nil,
		{Message: "plain"},
		{Message: "nl", NotLeader: &NotLeaderError{RangeID: 3, LeaderHint: 2}},
		{Message: "rnf", RangeNotFound: &RangeNotFoundError{RangeID: 5}},
		{Message: "rkm", RangeKeyMismatch: &RangeKeyMismatchError{RequestKey: keys.Key("k"), ActualDescriptors: []RangeDescriptor{testDesc()}}},
		{Message: "wi", WriteIntent: &WriteIntentError{Intents: []storage.Intent{{Key: keys.Key("k"), Txn: testTxn().TxnMeta}}}},
		{Message: "wto", WriteTooOld: &WriteTooOldError{Timestamp: mkts(1), ActualTimestamp: mkts(2)}},
		{Message: "unc", Uncertainty: &UncertaintyError{ReadTimestamp: mkts(1), ExistingTimestamp: mkts(2)}},
		{Message: "ab", TxnAborted: &TxnAbortedError{}},
		{Message: "rt", TxnRetry: &TxnRetryError{RetryTimestamp: mkts(3)}},
		{Message: "nf", TxnNotFound: &TxnNotFoundError{}},
		{Message: "amb", Ambiguous: &AmbiguousResultError{}},
	}
	for _, kerr := range errs {
		data, err := MarshalBatchEnvelope(br, kerr)
		if err != nil {
			t.Fatal(err)
		}
		gotBr, gotErr, err := UnmarshalBatchEnvelope(data)
		if err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(br, gotBr) {
			t.Fatalf("response mismatch:\n got %+v\nwant %+v", gotBr, br)
		}
		if !reflect.DeepEqual(kerr, gotErr) {
			t.Fatalf("error mismatch:\n got %+v\nwant %+v", gotErr, kerr)
		}
	}
}
