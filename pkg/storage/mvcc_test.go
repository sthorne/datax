package storage

import (
	"bytes"
	"errors"
	"testing"

	"github.com/google/uuid"
	"github.com/sthorne/datax/pkg/keys"
	"github.com/sthorne/datax/pkg/storage/enginepb"
	"github.com/sthorne/datax/pkg/util/hlc"
)

func openTestEngine(t *testing.T) *Engine {
	t.Helper()
	eng, err := Open("") // in-memory
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = eng.Close() })
	return eng
}

func ts(w int64, l int32) hlc.Timestamp { return hlc.Timestamp{WallTime: w, Logical: l} }

func mustPut(t *testing.T, eng *Engine, key string, at hlc.Timestamp, value string, txn *enginepb.TxnMeta) {
	t.Helper()
	b := eng.NewBatch()
	if err := MVCCPut(b, keys.Key(key), at, []byte(value), txn); err != nil {
		t.Fatalf("put %s@%s: %v", key, at, err)
	}
	if err := b.Commit(false); err != nil {
		t.Fatal(err)
	}
}

func mustDelete(t *testing.T, eng *Engine, key string, at hlc.Timestamp, txn *enginepb.TxnMeta) {
	t.Helper()
	b := eng.NewBatch()
	if err := MVCCDelete(b, keys.Key(key), at, txn); err != nil {
		t.Fatalf("delete %s@%s: %v", key, at, err)
	}
	if err := b.Commit(false); err != nil {
		t.Fatal(err)
	}
}

func mustGet(t *testing.T, eng *Engine, key string, at hlc.Timestamp, opts MVCCGetOptions) []byte {
	t.Helper()
	v, err := MVCCGet(eng, keys.Key(key), at, opts)
	if err != nil {
		t.Fatalf("get %s@%s: %v", key, at, err)
	}
	return v
}

func newTxn(at hlc.Timestamp) *enginepb.TxnMeta {
	return &enginepb.TxnMeta{
		ID:             uuid.New(),
		Epoch:          0,
		WriteTimestamp: at,
		MinTimestamp:   at,
		Priority:       1,
	}
}

func resolve(t *testing.T, eng *Engine, key string, txn *enginepb.TxnMeta, status enginepb.TxnStatus, commitTS hlc.Timestamp) {
	t.Helper()
	b := eng.NewBatch()
	if err := MVCCResolveIntent(b, keys.Key(key), txn.ID, status, commitTS); err != nil {
		t.Fatal(err)
	}
	if err := b.Commit(false); err != nil {
		t.Fatal(err)
	}
}

func TestMVCCVersionedReads(t *testing.T) {
	eng := openTestEngine(t)
	mustPut(t, eng, "k", ts(10, 0), "v10", nil)
	mustPut(t, eng, "k", ts(20, 0), "v20", nil)

	if v := mustGet(t, eng, "k", ts(5, 0), MVCCGetOptions{}); v != nil {
		t.Fatalf("read below first version: got %q", v)
	}
	if v := mustGet(t, eng, "k", ts(10, 0), MVCCGetOptions{}); string(v) != "v10" {
		t.Fatalf("read at 10: got %q", v)
	}
	if v := mustGet(t, eng, "k", ts(15, 0), MVCCGetOptions{}); string(v) != "v10" {
		t.Fatalf("read at 15: got %q", v)
	}
	if v := mustGet(t, eng, "k", ts(25, 0), MVCCGetOptions{}); string(v) != "v20" {
		t.Fatalf("read at 25: got %q", v)
	}
}

func TestMVCCTombstones(t *testing.T) {
	eng := openTestEngine(t)
	mustPut(t, eng, "k", ts(10, 0), "alive", nil)
	mustDelete(t, eng, "k", ts(20, 0), nil)

	if v := mustGet(t, eng, "k", ts(15, 0), MVCCGetOptions{}); string(v) != "alive" {
		t.Fatalf("read below tombstone: got %q", v)
	}
	if v := mustGet(t, eng, "k", ts(25, 0), MVCCGetOptions{}); v != nil {
		t.Fatalf("read above tombstone: got %q", v)
	}
}

func TestMVCCWriteTooOld(t *testing.T) {
	eng := openTestEngine(t)
	mustPut(t, eng, "k", ts(20, 0), "newer", nil)

	// Transactional write beneath a committed version fails.
	txn := newTxn(ts(10, 0))
	b := eng.NewBatch()
	err := MVCCPut(b, keys.Key("k"), txn.WriteTimestamp, []byte("x"), txn)
	var wto *WriteTooOldError
	if !errors.As(err, &wto) {
		t.Fatalf("expected WriteTooOldError, got %v", err)
	}
	if !wto.ActualTimestamp.Equal(ts(20, 1)) {
		t.Fatalf("suggested restart timestamp: %s", wto.ActualTimestamp)
	}
	_ = b.Close()

	// Non-transactional write just serializes above.
	mustPut(t, eng, "k", ts(10, 0), "bumped", nil)
	if v := mustGet(t, eng, "k", ts(20, 1), MVCCGetOptions{}); string(v) != "bumped" {
		t.Fatalf("bumped write not visible: %q", v)
	}
	if v := mustGet(t, eng, "k", ts(20, 0), MVCCGetOptions{}); string(v) != "newer" {
		t.Fatalf("original overwritten: %q", v)
	}
}

func TestMVCCIntentVisibility(t *testing.T) {
	eng := openTestEngine(t)
	mustPut(t, eng, "k", ts(5, 0), "committed", nil)

	txn := newTxn(ts(10, 0))
	mustPut(t, eng, "k", txn.WriteTimestamp, "provisional", txn)

	// The writing transaction reads its own intent (read-your-writes).
	if v := mustGet(t, eng, "k", ts(10, 0), MVCCGetOptions{Txn: txn}); string(v) != "provisional" {
		t.Fatalf("own read: got %q", v)
	}

	// Other readers hit a WriteIntentError, even below the intent timestamp
	// (v1 pushes on any intent).
	_, err := MVCCGet(eng, keys.Key("k"), ts(20, 0), MVCCGetOptions{})
	var wie *WriteIntentError
	if !errors.As(err, &wie) {
		t.Fatalf("expected WriteIntentError, got %v", err)
	}
	if wie.Intents[0].Txn.ID != txn.ID {
		t.Fatal("intent attributed to wrong txn")
	}

	// A conflicting writer also fails.
	other := newTxn(ts(30, 0))
	b := eng.NewBatch()
	if err := MVCCPut(b, keys.Key("k"), other.WriteTimestamp, []byte("x"), other); !errors.As(err, &wie) {
		t.Fatalf("conflicting write: expected WriteIntentError, got %v", err)
	}
	_ = b.Close()
}

func TestMVCCIntentRewriteSameTxn(t *testing.T) {
	eng := openTestEngine(t)
	txn := newTxn(ts(10, 0))
	mustPut(t, eng, "k", txn.WriteTimestamp, "first", txn)
	mustPut(t, eng, "k", txn.WriteTimestamp, "second", txn)

	if v := mustGet(t, eng, "k", ts(10, 0), MVCCGetOptions{Txn: txn}); string(v) != "second" {
		t.Fatalf("own read after rewrite: got %q", v)
	}

	resolve(t, eng, "k", txn, enginepb.COMMITTED, txn.WriteTimestamp)
	if v := mustGet(t, eng, "k", ts(10, 0), MVCCGetOptions{}); string(v) != "second" {
		t.Fatalf("after commit: got %q", v)
	}
	// Exactly one version must exist.
	res, err := MVCCScan(eng, keys.Key("k"), keys.Key("k").Next(), ts(100, 0), 0, MVCCGetOptions{})
	if err != nil || len(res.KVs) != 1 {
		t.Fatalf("scan after commit: %v, %d rows", err, len(res.KVs))
	}
}

func TestMVCCResolveCommitAndAbort(t *testing.T) {
	eng := openTestEngine(t)
	mustPut(t, eng, "k", ts(5, 0), "old", nil)

	txn := newTxn(ts(10, 0))
	mustPut(t, eng, "k", txn.WriteTimestamp, "new", txn)

	// Abort: intent and provisional value vanish; old value remains.
	resolve(t, eng, "k", txn, enginepb.ABORTED, hlc.Timestamp{})
	if v := mustGet(t, eng, "k", ts(20, 0), MVCCGetOptions{}); string(v) != "old" {
		t.Fatalf("after abort: got %q", v)
	}

	// Commit at a moved timestamp: version migrates.
	txn2 := newTxn(ts(30, 0))
	mustPut(t, eng, "k", txn2.WriteTimestamp, "committed", txn2)
	resolve(t, eng, "k", txn2, enginepb.COMMITTED, ts(35, 0))
	if v := mustGet(t, eng, "k", ts(34, 0), MVCCGetOptions{}); string(v) != "old" {
		t.Fatalf("below moved commit ts: got %q", v)
	}
	if v := mustGet(t, eng, "k", ts(35, 0), MVCCGetOptions{}); string(v) != "committed" {
		t.Fatalf("at moved commit ts: got %q", v)
	}

	// Resolving twice is a no-op.
	resolve(t, eng, "k", txn2, enginepb.COMMITTED, ts(35, 0))
}

func TestMVCCUncertainty(t *testing.T) {
	eng := openTestEngine(t)
	mustPut(t, eng, "k", ts(15, 0), "future", nil)

	// Read at 10 with uncertainty limit 20: the value at 15 is uncertain.
	_, err := MVCCGet(eng, keys.Key("k"), ts(10, 0), MVCCGetOptions{UncertaintyLimit: ts(20, 0)})
	var ue *UncertaintyError
	if !errors.As(err, &ue) {
		t.Fatalf("expected UncertaintyError, got %v", err)
	}
	if !ue.ExistingTimestamp.Equal(ts(15, 0)) {
		t.Fatalf("uncertain value timestamp: %s", ue.ExistingTimestamp)
	}

	// Limit below the value: no uncertainty, value simply invisible.
	if v := mustGet(t, eng, "k", ts(10, 0), MVCCGetOptions{UncertaintyLimit: ts(14, 0)}); v != nil {
		t.Fatalf("got %q", v)
	}

	// Value beyond the limit: not uncertain either.
	mustPut(t, eng, "k2", ts(50, 0), "far", nil)
	if v := mustGet(t, eng, "k2", ts(10, 0), MVCCGetOptions{UncertaintyLimit: ts(20, 0)}); v != nil {
		t.Fatalf("got %q", v)
	}

	// Scan hits uncertainty too.
	_, err = MVCCScan(eng, keys.Key("a"), keys.Key("z"), ts(10, 0), 0, MVCCGetOptions{UncertaintyLimit: ts(20, 0)})
	if !errors.As(err, &ue) {
		t.Fatalf("scan: expected UncertaintyError, got %v", err)
	}
}

func TestMVCCEpochHandling(t *testing.T) {
	eng := openTestEngine(t)
	mustPut(t, eng, "k", ts(5, 0), "committed", nil)

	txn := newTxn(ts(10, 0))
	mustPut(t, eng, "k", txn.WriteTimestamp, "epoch0", txn)

	// The transaction restarts: epoch bumps, timestamp moves.
	txn2 := *txn
	txn2.Epoch = 1
	txn2.WriteTimestamp = ts(20, 0)

	// Reads in the new epoch ignore the old epoch's provisional value.
	if v := mustGet(t, eng, "k", ts(20, 0), MVCCGetOptions{Txn: &txn2}); string(v) != "committed" {
		t.Fatalf("new-epoch read: got %q", v)
	}

	// A new-epoch write replaces the old intent entirely.
	mustPut(t, eng, "k", txn2.WriteTimestamp, "epoch1", &txn2)
	if v := mustGet(t, eng, "k", ts(20, 0), MVCCGetOptions{Txn: &txn2}); string(v) != "epoch1" {
		t.Fatalf("new-epoch read after write: got %q", v)
	}
	resolve(t, eng, "k", txn, enginepb.COMMITTED, ts(20, 0))
	if v := mustGet(t, eng, "k", ts(20, 0), MVCCGetOptions{}); string(v) != "epoch1" {
		t.Fatalf("after commit: got %q", v)
	}
}

func TestMVCCScanBasics(t *testing.T) {
	eng := openTestEngine(t)
	mustPut(t, eng, "a", ts(10, 0), "va", nil)
	mustPut(t, eng, "b", ts(20, 0), "vb", nil)
	mustPut(t, eng, "c", ts(10, 0), "vc", nil)
	mustDelete(t, eng, "c", ts(15, 0), nil)
	mustPut(t, eng, "d", ts(10, 0), "vd", nil)

	// At ts 12: a and d visible ("b" not yet written, "c" written but then
	// deleted at 15 — still visible at 12).
	res, err := MVCCScan(eng, keys.Key("a"), keys.Key("z"), ts(12, 0), 0, MVCCGetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	want := map[string]string{"a": "va", "c": "vc", "d": "vd"}
	if len(res.KVs) != len(want) {
		t.Fatalf("scan@12: got %d rows", len(res.KVs))
	}
	for _, kv := range res.KVs {
		if want[string(kv.Key)] != string(kv.Value) {
			t.Fatalf("scan@12: %s=%q", kv.Key, kv.Value)
		}
	}

	// At ts 25: c's tombstone hides it; b appears.
	res, err = MVCCScan(eng, keys.Key("a"), keys.Key("z"), ts(25, 0), 0, MVCCGetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if len(res.KVs) != 3 || string(res.KVs[0].Key) != "a" || string(res.KVs[1].Key) != "b" || string(res.KVs[2].Key) != "d" {
		t.Fatalf("scan@25: %v", res.KVs)
	}

	// Bounded scan returns a resume key.
	res, err = MVCCScan(eng, keys.Key("a"), keys.Key("z"), ts(25, 0), 2, MVCCGetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if len(res.KVs) != 2 || res.Resume == nil {
		t.Fatalf("bounded scan: %d rows, resume %v", len(res.KVs), res.Resume)
	}
	res2, err := MVCCScan(eng, res.Resume, keys.Key("z"), ts(25, 0), 0, MVCCGetOptions{})
	if err != nil || len(res2.KVs) != 1 || string(res2.KVs[0].Key) != "d" {
		t.Fatalf("resumed scan: %v %v", res2.KVs, err)
	}
}

func TestMVCCScanIntents(t *testing.T) {
	eng := openTestEngine(t)
	mustPut(t, eng, "a", ts(10, 0), "va", nil)
	txn := newTxn(ts(20, 0))
	mustPut(t, eng, "b", txn.WriteTimestamp, "vb-prov", txn)
	mustPut(t, eng, "c", ts(10, 0), "vc", nil)

	// Foreign scan: aggregated WriteIntentError.
	_, err := MVCCScan(eng, keys.Key("a"), keys.Key("z"), ts(30, 0), 0, MVCCGetOptions{})
	var wie *WriteIntentError
	if !errors.As(err, &wie) || len(wie.Intents) != 1 || !bytes.Equal(wie.Intents[0].Key, []byte("b")) {
		t.Fatalf("expected intent on b, got %v", err)
	}

	// The owning transaction sees its own provisional value.
	res, err := MVCCScan(eng, keys.Key("a"), keys.Key("z"), ts(20, 0), 0, MVCCGetOptions{Txn: txn})
	if err != nil {
		t.Fatal(err)
	}
	if len(res.KVs) != 3 || string(res.KVs[1].Value) != "vb-prov" {
		t.Fatalf("own scan: %v", res.KVs)
	}

	// After commit, everyone sees it.
	resolve(t, eng, "b", txn, enginepb.COMMITTED, txn.WriteTimestamp)
	res, err = MVCCScan(eng, keys.Key("a"), keys.Key("z"), ts(30, 0), 0, MVCCGetOptions{})
	if err != nil || len(res.KVs) != 3 {
		t.Fatalf("after commit: %v %v", res.KVs, err)
	}
}

func TestMVCCTxnDeleteOwnWrite(t *testing.T) {
	eng := openTestEngine(t)
	txn := newTxn(ts(10, 0))
	mustPut(t, eng, "k", txn.WriteTimestamp, "v", txn)
	mustDelete(t, eng, "k", txn.WriteTimestamp, txn)

	// Own read sees the delete.
	if v := mustGet(t, eng, "k", ts(10, 0), MVCCGetOptions{Txn: txn}); v != nil {
		t.Fatalf("own read after own delete: got %q", v)
	}
	resolve(t, eng, "k", txn, enginepb.COMMITTED, txn.WriteTimestamp)
	if v := mustGet(t, eng, "k", ts(20, 0), MVCCGetOptions{}); v != nil {
		t.Fatalf("after commit of delete: got %q", v)
	}
}

func TestMVCCCheckForWrites(t *testing.T) {
	eng := openTestEngine(t)
	mustPut(t, eng, "a", ts(10, 0), "v10", nil)
	mustPut(t, eng, "b", ts(20, 0), "v20", nil)
	mustDelete(t, eng, "c", ts(25, 0), nil)

	own := newTxn(ts(30, 0))
	mustPut(t, eng, "d", own.WriteTimestamp, "own-intent", own)

	check := func(start, end string, from, to hlc.Timestamp) error {
		return MVCCCheckForWrites(eng, keys.Key(start), keys.Key(end), from, to, own.ID)
	}

	// No writes in (10, 15]: refresh safe.
	if err := check("a", "b", ts(10, 0), ts(15, 0)); err != nil {
		t.Fatalf("clean window: %v", err)
	}
	// b@20 falls in (15, 25]: refresh fails.
	if err := check("a", "z", ts(15, 0), ts(25, 0)); err == nil {
		t.Fatal("missed committed write in window")
	}
	// Boundary: from is exclusive — a@10 not in (10, 30]... but b@20 is.
	if err := check("a", "b", ts(9, 0), ts(30, 0)); err == nil {
		t.Fatal("missed write at from boundary+")
	}
	// to is inclusive: b@20 in (19, 20].
	if err := check("b", "c", ts(19, 0), ts(20, 0)); err == nil {
		t.Fatal("missed write at to boundary")
	}
	// Tombstones count as writes: c@25 in (24, 26].
	if err := check("c", "d", ts(24, 0), ts(26, 0)); err == nil {
		t.Fatal("missed tombstone in window")
	}
	// Own intent and provisional value are ignored.
	if err := check("d", "e", ts(29, 0), ts(31, 0)); err != nil {
		t.Fatalf("own intent should not fail refresh: %v", err)
	}
	// Foreign intent fails refresh.
	other := newTxn(ts(40, 0))
	mustPut(t, eng, "e", other.WriteTimestamp, "foreign", other)
	if err := check("e", "f", ts(39, 0), ts(41, 0)); err == nil {
		t.Fatal("missed foreign intent")
	}
	var wie *WriteIntentError
	if err := check("e", "f", ts(1, 0), ts(2, 0)); !errors.As(err, &wie) {
		// A foreign intent fails refresh regardless of window (it could
		// commit anywhere).
		t.Fatalf("foreign intent outside window: got %v", err)
	}
}
