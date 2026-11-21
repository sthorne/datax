package kvserver

import (
	"encoding/hex"
	"testing"
	"time"

	"github.com/sthorne/datax/pkg/keys"
	"github.com/sthorne/datax/pkg/kvpb"
	"github.com/sthorne/datax/pkg/storage"
	"github.com/sthorne/datax/pkg/storage/enginepb"
	"github.com/sthorne/datax/pkg/util/hlc"
)

// The conditional put on a replica (kvpb.CPutRequest), on the range
// clear's one-replica store (startClearTestStore), which runs at cluster
// version v22: a replica evaluates a conditional put whatever version it
// has heard of — the gate is the sender's (pkg/version rule 4).

// goldenRaftCommandCPut is a conditional put's command (cluster version
// v24): range 9 at 300.1, a CPut of a = v.
const goldenRaftCommandCPut = "010a08636d642d6370757412180a090a0508ac0210011809120bca01080a030a0161120176"

func TestRaftCommandDecodesGoldenCPut(t *testing.T) {
	data, err := hex.DecodeString(goldenRaftCommandCPut)
	if err != nil {
		t.Fatal(err)
	}
	cmd, err := decodeRaftCommand(data)
	if err != nil {
		t.Fatalf("golden conditional-put command no longer decodes: %v", err)
	}
	if cmd.ID != "cmd-cput" || cmd.Batch.Header.RangeID != 9 || len(cmd.Batch.Requests) != 1 {
		t.Fatalf("decoded %+v", cmd)
	}
	if cp := cmd.Batch.Requests[0].CPut; cp == nil || string(cp.Key) != "a" || string(cp.Value) != "v" {
		t.Fatalf("conditional put: %+v", cp)
	}
	raw, err := encodeRaftCommand(cmd)
	if err != nil {
		t.Fatal(err)
	}
	if hex.EncodeToString(raw) != goldenRaftCommandCPut {
		t.Fatalf("encoding changed:\n got %x\nwant %s", raw, goldenRaftCommandCPut)
	}
}

func cputReq(k keys.Key, v string) *kvpb.CPutRequest {
	return &kvpb.CPutRequest{RequestHeader: kvpb.RequestHeader{Key: k}, Value: []byte(v)}
}

// cputTxn is a transaction at the store's clock, anchored on key.
func (e *clearTestEnv) cputTxn(key keys.Key) *kvpb.Transaction {
	txn := kvpb.NewTransaction("cput", 1, e.clock.Now())
	txn.Key = key
	txn.Sequence = 1
	return txn
}

// send runs reqs as one batch of txn, the first creating its record.
func (e *clearTestEnv) send(t *testing.T, txn *kvpb.Transaction, reqs ...kvpb.Request) (*kvpb.BatchResponse, *kvpb.Error) {
	t.Helper()
	ba := &kvpb.BatchRequest{Header: kvpb.BatchHeader{Txn: txn.Clone(), CreateTxnRecord: true}}
	for _, r := range reqs {
		ba.Add(r)
	}
	return e.exec(t, ba)
}

// send1PC runs reqs and a committing EndTxn{All} as one batch: a
// one-phase commit.
func (e *clearTestEnv) send1PC(t *testing.T, txn *kvpb.Transaction, reqs ...kvpb.Request) (*kvpb.BatchResponse, *kvpb.Error) {
	t.Helper()
	reqs = append(reqs, &kvpb.EndTxnRequest{RequestHeader: kvpb.RequestHeader{Key: txn.Key}, Commit: true, All: true})
	return e.send(t, txn, reqs...)
}

func wantCondFailed(t *testing.T, kerr *kvpb.Error, k keys.Key) {
	t.Helper()
	if kerr == nil || kerr.ConditionFailed == nil {
		t.Fatalf("want a ConditionFailedError naming %s, got %v", k, kerr)
	}
	if !kerr.ConditionFailed.Key.Equal(k) {
		t.Fatalf("ConditionFailedError names %s, want %s", kerr.ConditionFailed.Key, k)
	}
	if kerr.IsRetryableTxnError() || kerr.WriteIntent != nil {
		t.Fatalf("a failed condition is final, not a conflict: %+v", kerr)
	}
}

// committedAt reads key's newest committed value beneath any intent.
func (e *clearTestEnv) committedAt(t *testing.T, key keys.Key) []byte {
	t.Helper()
	v, err := storageGet(t, e, key)
	if err != nil {
		t.Fatal(err)
	}
	return v
}

func (e *clearTestEnv) intentOf(t *testing.T, key keys.Key) *enginepb.MVCCMetadata {
	t.Helper()
	metaKey := storage.EncodeMVCCKey(key, hlc.Timestamp{})
	raw, err := e.store.cfg.Engine.Get(metaKey)
	if err != nil {
		t.Fatal(err)
	}
	if raw == nil {
		return nil
	}
	meta, err := storage.DecodeMVCCMetadata(raw)
	if err != nil {
		t.Fatal(err)
	}
	return &meta
}

func TestCPutWritesAnAbsentKey(t *testing.T) {
	env := startClearTestStore(t)
	k := env.key("k")
	txn := env.cputTxn(k)
	br, kerr := env.send(t, txn, cputReq(k, "v"))
	if kerr != nil {
		t.Fatal(kerr)
	}
	if br.Responses[0].CPut == nil {
		t.Fatalf("response %+v", br.Responses[0])
	}
	if in := env.intentOf(t, k); in == nil || in.Txn.ID != txn.ID {
		t.Fatalf("no intent of the transaction: %+v", in)
	}

	// One phase: the value lands committed, with no intent.
	k2 := env.key("k2")
	br, kerr = env.send1PC(t, env.cputTxn(k2), cputReq(k2, "w"))
	if kerr != nil {
		t.Fatal(kerr)
	}
	if et := br.Responses[1].EndTxn; et == nil || !et.OnePhase {
		t.Fatalf("not a one-phase commit: %+v", br.Responses[1])
	}
	if in := env.intentOf(t, k2); in != nil {
		t.Fatalf("a one-phase put left an intent: %+v", in)
	}
	if v := env.committedAt(t, k2); string(v) != "w" {
		t.Fatalf("committed %q", v)
	}
}

func TestCPutFailsOnACommittedValue(t *testing.T) {
	env := startClearTestStore(t)
	k := env.key("k")
	env.put(t, "k", "old")
	for _, onePhase := range []bool{false, true} {
		txn := env.cputTxn(k)
		var kerr *kvpb.Error
		if onePhase {
			_, kerr = env.send1PC(t, txn, cputReq(k, "new"))
		} else {
			_, kerr = env.send(t, txn, cputReq(k, "new"))
		}
		wantCondFailed(t, kerr, k)
		if in := env.intentOf(t, k); in != nil {
			t.Fatalf("one phase %v: a failed put left an intent", onePhase)
		}
		if v := env.committedAt(t, k); string(v) != "old" {
			t.Fatalf("one phase %v: value %q after a failed put", onePhase, v)
		}
		// Nothing else of the batch applied: not the record either.
		rec, err := loadTxnRecord(env.store.cfg.Engine, txnRecordKey(&txn.TxnMeta))
		if err != nil || rec != nil {
			t.Fatalf("one phase %v: record %+v, %v", onePhase, rec, err)
		}
	}
}

func TestCPutTombstoneIsAbsent(t *testing.T) {
	env := startClearTestStore(t)
	k := env.key("k")
	env.put(t, "k", "old")
	env.del(t, "k")
	if _, kerr := env.send1PC(t, env.cputTxn(k), cputReq(k, "again")); kerr != nil {
		t.Fatalf("a deleted key is absent: %v", kerr)
	}
	if v := env.committedAt(t, k); string(v) != "again" {
		t.Fatalf("committed %q", v)
	}
}

// A version committed above the transaction's read timestamp — one
// inside its uncertainty interval (the clock's maximum offset above the
// read) or beyond it — is a stale snapshot: the put fails with a
// retryable WriteTooOld, as a Get then Put would have failed (with an
// UncertaintyError or a WriteTooOld), never with a write beside it.
func TestCPutVersionAboveTheRead(t *testing.T) {
	env := startClearTestStore(t)
	for _, c := range []struct {
		name  string
		above time.Duration
		value bool
	}{
		{"value inside the uncertainty interval", time.Millisecond, true},
		{"value beyond it", time.Minute, true},
		{"tombstone inside it", time.Millisecond, false},
	} {
		t.Run(c.name, func(t *testing.T) {
			k := env.key(c.name)
			txn := env.cputTxn(k)
			ba := &kvpb.BatchRequest{Header: kvpb.BatchHeader{Timestamp: txn.ReadTimestamp.AddNanos(int64(c.above))}}
			if c.value {
				ba.Add(&kvpb.PutRequest{RequestHeader: kvpb.RequestHeader{Key: k}, Value: []byte("later")})
			} else {
				ba.Add(&kvpb.DeleteRequest{RequestHeader: kvpb.RequestHeader{Key: k}})
			}
			if _, kerr := env.exec(t, ba); kerr != nil {
				t.Fatal(kerr)
			}
			for _, onePhase := range []bool{false, true} {
				var kerr *kvpb.Error
				if onePhase {
					_, kerr = env.send1PC(t, txn, cputReq(k, "mine"))
				} else {
					_, kerr = env.send(t, txn, cputReq(k, "mine"))
				}
				if kerr == nil || kerr.WriteTooOld == nil || !kerr.IsRetryableTxnError() {
					t.Fatalf("one phase %v: want a retryable WriteTooOld, got %v", onePhase, kerr)
				}
			}
			// Restarted above it, the transaction sees the value (or the
			// tombstone).
			later := env.cputTxn(k)
			later.ReadTimestamp = txn.ReadTimestamp.AddNanos(int64(c.above)).Next()
			later.WriteTimestamp = later.ReadTimestamp
			_, kerr := env.send1PC(t, later, cputReq(k, "mine"))
			if c.value {
				wantCondFailed(t, kerr, k)
			} else if kerr != nil {
				t.Fatal(kerr)
			}
		})
	}
}

func TestCPutForeignIntent(t *testing.T) {
	env := startClearTestStore(t)
	k := env.key("k")
	other := env.cputTxn(k)
	if _, kerr := env.send(t, other, &kvpb.PutRequest{RequestHeader: kvpb.RequestHeader{Key: k}, Value: []byte("theirs")}); kerr != nil {
		t.Fatal(kerr)
	}
	for _, onePhase := range []bool{false, true} {
		txn := env.cputTxn(env.key("anchor"))
		var kerr *kvpb.Error
		if onePhase {
			_, kerr = env.send1PC(t, txn, cputReq(k, "mine"))
		} else {
			_, kerr = env.send(t, txn, cputReq(k, "mine"))
		}
		if kerr == nil || kerr.WriteIntent == nil || kerr.WriteIntent.Intents[0].Txn.ID != other.ID {
			t.Fatalf("one phase %v: want the other transaction's intent, got %v", onePhase, kerr)
		}
	}
	// Resolved as committed, it is a value the next put fails on.
	env.resolve(t, k, other, enginepb.COMMITTED, false, env.clock.Now())
	wantCondFailed(t, func() *kvpb.Error { _, e := env.send1PC(t, env.cputTxn(k), cputReq(k, "mine")); return e }(), k)
}

func TestCPutOwnEarlierWrite(t *testing.T) {
	env := startClearTestStore(t)
	k := env.key("k")
	txn := env.cputTxn(k)
	if _, kerr := env.send(t, txn, &kvpb.PutRequest{RequestHeader: kvpb.RequestHeader{Key: k}, Value: []byte("first")}); kerr != nil {
		t.Fatal(kerr)
	}
	txn.Sequence++
	_, kerr := env.send(t, txn, cputReq(k, "second"))
	wantCondFailed(t, kerr, k)

	// Its own earlier deletion of a value leaves the key absent to it.
	k2 := env.key("k2")
	env.put(t, "k2", "old")
	txn2 := env.cputTxn(k2)
	if _, kerr := env.send(t, txn2, &kvpb.DeleteRequest{RequestHeader: kvpb.RequestHeader{Key: k2}}); kerr != nil {
		t.Fatal(kerr)
	}
	txn2.Sequence++
	if _, kerr := env.send(t, txn2, cputReq(k2, "again")); kerr != nil {
		t.Fatalf("after its own delete: %v", kerr)
	}
}

// When several conditional puts of a batch fail, the batch reports the
// first; a conflict on a key before it wins (that key may yet hold a
// value), one after it does not.
func TestCPutReportsTheEarliestFailure(t *testing.T) {
	env := startClearTestStore(t)
	a, b, c := env.key("a"), env.key("b"), env.key("c")
	env.put(t, "a", "x")
	env.put(t, "b", "x")
	other := env.cputTxn(c)
	if _, kerr := env.send(t, other, &kvpb.PutRequest{RequestHeader: kvpb.RequestHeader{Key: c}, Value: []byte("theirs")}); kerr != nil {
		t.Fatal(kerr)
	}
	for _, onePhase := range []bool{false, true} {
		run := func(reqs ...kvpb.Request) *kvpb.Error {
			txn := env.cputTxn(env.key("anchor"))
			if onePhase {
				_, kerr := env.send1PC(t, txn, reqs...)
				return kerr
			}
			_, kerr := env.send(t, txn, reqs...)
			return kerr
		}
		wantCondFailed(t, run(cputReq(env.key("free"), "v"), cputReq(b, "v"), cputReq(a, "v")), b)
		wantCondFailed(t, run(cputReq(a, "v"), cputReq(c, "v")), a)
		if kerr := run(cputReq(c, "v"), cputReq(a, "v")); kerr == nil || kerr.WriteIntent == nil {
			t.Fatalf("one phase %v: an intent before the failed key: got %v, want the intent", onePhase, kerr)
		}
	}
}

// A failed conditional put is a read of its key at the transaction's read
// timestamp: no other writer may then land on the key at or below it (a
// delete beneath the read would falsify what the transaction saw). A
// transaction's write is pushed above the read, and a non-transactional
// one refused.
func TestCPutFailureUpdatesTheTimestampCache(t *testing.T) {
	env := startClearTestStore(t)
	for _, onePhase := range []bool{false, true} {
		k, control := env.key("k"), env.key("control")
		if onePhase {
			k, control = env.key("k1pc"), env.key("control1pc")
		}
		env.put(t, string(k[len(env.start):]), "old")
		env.put(t, string(control[len(env.start):]), "old")

		writer := env.cputTxn(k) // timestamped before the reader
		reader := env.cputTxn(k)
		var kerr *kvpb.Error
		if onePhase {
			_, kerr = env.send1PC(t, reader, cputReq(k, "new"))
		} else {
			_, kerr = env.send(t, reader, cputReq(k, "new"))
		}
		wantCondFailed(t, kerr, k)

		// The control key was not read: the writer deletes it in place.
		br, kerr := env.send(t, writer, &kvpb.DeleteRequest{RequestHeader: kvpb.RequestHeader{Key: control}})
		if kerr != nil {
			t.Fatal(kerr)
		}
		if br.Txn.WriteTimestamp != writer.WriteTimestamp {
			t.Fatalf("one phase %v: an unread key's delete was pushed to %s", onePhase, br.Txn.WriteTimestamp)
		}
		// The read key: pushed above the failed put's read.
		w2 := writer.Clone()
		w2.Sequence++
		br, kerr = env.send(t, w2, &kvpb.DeleteRequest{RequestHeader: kvpb.RequestHeader{Key: k}})
		if kerr != nil {
			t.Fatal(kerr)
		}
		if !reader.ReadTimestamp.Less(br.Txn.WriteTimestamp) {
			t.Fatalf("one phase %v: a delete of the read key at %s was not pushed above the read at %s", onePhase, br.Txn.WriteTimestamp, reader.ReadTimestamp)
		}
		// Without a transaction: refused.
		ba := &kvpb.BatchRequest{Header: kvpb.BatchHeader{Timestamp: reader.ReadTimestamp}}
		ba.Add(&kvpb.DeleteRequest{RequestHeader: kvpb.RequestHeader{Key: k}})
		if _, kerr := env.exec(t, ba); kerr == nil || kerr.TxnRetry == nil {
			t.Fatalf("one phase %v: a non-transactional delete beneath the read: got %v", onePhase, kerr)
		}
		env.resolve(t, k, w2, enginepb.ABORTED, false, env.clock.Now())
		env.resolve(t, control, writer, enginepb.ABORTED, false, env.clock.Now())
	}
}

// A conditional put reads history at the transaction's timestamp: one at
// or below the range's GC threshold is refused, like a Get.
func TestCPutBelowTheGCThreshold(t *testing.T) {
	env := startClearTestStore(t)
	k := env.key("k")
	txn := env.cputTxn(k)
	env.r.mu.Lock()
	env.r.mu.gcThreshold = txn.ReadTimestamp
	env.r.mu.Unlock()
	if _, kerr := env.send1PC(t, txn, cputReq(k, "v")); kerr == nil || kerr.BelowGCThreshold == nil {
		t.Fatalf("want a GC-threshold refusal, got %v", kerr)
	}
}
