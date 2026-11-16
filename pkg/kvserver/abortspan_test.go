package kvserver

import (
	"bytes"
	"context"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/sthorne/datax/pkg/keys"
	"github.com/sthorne/datax/pkg/kvpb"
	"github.com/sthorne/datax/pkg/storage"
	"github.com/sthorne/datax/pkg/storage/enginepb"
	"github.com/sthorne/datax/pkg/util/hlc"
	"github.com/sthorne/datax/pkg/util/stop"
	"github.com/sthorne/datax/pkg/version"
)

// The abort span's unit tests (issue #320) run on the range clear's
// one-replica store (startClearTestStore): range 9 over one table's span.

// abortTestTxn starts a transaction anchored on key: its first write
// lays an intent there and creates its record.
func (e *clearTestEnv) abortTestTxn(t *testing.T, key keys.Key) *kvpb.Transaction {
	t.Helper()
	txn := kvpb.NewTransaction("victim", 1, e.clock.Now())
	txn.Key = key
	ba := &kvpb.BatchRequest{Header: kvpb.BatchHeader{Txn: txn, CreateTxnRecord: true}}
	ba.Add(&kvpb.PutRequest{RequestHeader: kvpb.RequestHeader{Key: key}, Value: []byte("mine")})
	if _, kerr := e.exec(t, ba); kerr != nil {
		t.Fatal(kerr)
	}
	return txn
}

// resolve resolves key's intent of txn with status, as a pusher (poison)
// or as the transaction's own coordinator, at ts.
func (e *clearTestEnv) resolve(t *testing.T, key keys.Key, txn *kvpb.Transaction, status enginepb.TxnStatus, poison bool, ts hlc.Timestamp) {
	t.Helper()
	ba := &kvpb.BatchRequest{Header: kvpb.BatchHeader{Timestamp: ts}}
	ba.Add(&kvpb.ResolveIntentRequest{RequestHeader: kvpb.RequestHeader{Key: key}, TxnID: txn.ID, Status: status, CommitTS: txn.WriteTimestamp, Poison: poison})
	if _, kerr := e.exec(t, ba); kerr != nil {
		t.Fatal(kerr)
	}
}

// txnRequest sends one request of txn and returns the error.
func (e *clearTestEnv) txnRequest(t *testing.T, txn *kvpb.Transaction, req kvpb.Request) *kvpb.Error {
	t.Helper()
	ba := &kvpb.BatchRequest{Header: kvpb.BatchHeader{Txn: txn}}
	ba.Add(req)
	_, kerr := e.exec(t, ba)
	return kerr
}

func (e *clearTestEnv) hasAbortEntry(t *testing.T, id uuid.UUID) bool {
	t.Helper()
	_, ok, err := loadAbortSpanEntry(e.store.cfg.Engine, e.r.rangeID, id)
	if err != nil {
		t.Fatal(err)
	}
	return ok
}

// TestAbortSpanRefusesTheAbortedTransaction: a poisoning resolution that
// removes a transaction's intent leaves an entry, and from then on every
// data request of the transaction on the range — a read of the key it
// wrote, a read of another key, a scan, a write, a locking read — fails
// with a TxnAbortedError; its heartbeat still answers (with the record's
// verdict), and other transactions are untouched. The coordinator's own
// aborted resolution removes the entry.
func TestAbortSpanRefusesTheAbortedTransaction(t *testing.T) {
	env := startClearTestStore(t)
	k, other := env.key("k"), env.key("other")
	victim := env.abortTestTxn(t, k)
	if env.hasAbortEntry(t, victim.ID) {
		t.Fatal("an entry before any abort")
	}
	env.resolve(t, k, victim, enginepb.ABORTED, true, env.clock.Now())
	if !env.hasAbortEntry(t, victim.ID) {
		t.Fatal("a poisoning resolution that removed an intent left no entry")
	}
	for _, c := range []struct {
		name string
		req  kvpb.Request
	}{
		{"get of the removed write", &kvpb.GetRequest{RequestHeader: kvpb.RequestHeader{Key: k}}},
		{"get of another key", &kvpb.GetRequest{RequestHeader: kvpb.RequestHeader{Key: other}}},
		{"scan", &kvpb.ScanRequest{RequestHeader: kvpb.RequestHeader{Key: env.start, EndKey: env.end}}},
		{"put", &kvpb.PutRequest{RequestHeader: kvpb.RequestHeader{Key: other}, Value: []byte("x")}},
		{"delete", &kvpb.DeleteRequest{RequestHeader: kvpb.RequestHeader{Key: k}}},
		{"locking get", &kvpb.GetRequest{RequestHeader: kvpb.RequestHeader{Key: k}, ForUpdate: true}},
	} {
		if kerr := env.txnRequest(t, victim, c.req); kerr == nil || kerr.TxnAborted == nil {
			t.Errorf("%s: got %v, want a TxnAbortedError", c.name, kerr)
		}
	}
	if v, _ := storageGet(t, env, other); v != nil {
		t.Fatalf("a refused put wrote %q", v)
	}
	hb := &kvpb.HeartbeatTxnRequest{RequestHeader: kvpb.RequestHeader{Key: k}, Now: env.clock.Now()}
	if kerr := env.txnRequest(t, victim, hb); kerr != nil {
		t.Fatalf("heartbeat refused: %v", kerr)
	}
	bystander := kvpb.NewTransaction("bystander", 1, env.clock.Now())
	if kerr := env.txnRequest(t, bystander, &kvpb.GetRequest{RequestHeader: kvpb.RequestHeader{Key: k}}); kerr != nil {
		t.Fatalf("another transaction refused: %v", kerr)
	}

	// The coordinator gives up and resolves its writes (none is left):
	// the entry goes.
	env.resolve(t, k, victim, enginepb.ABORTED, false, env.clock.Now())
	if env.hasAbortEntry(t, victim.ID) {
		t.Fatal("the coordinator's own resolution left the entry")
	}
}

// storageGet reads key's newest committed value straight from the engine.
func storageGet(t *testing.T, env *clearTestEnv, key keys.Key) ([]byte, error) {
	t.Helper()
	return storage.MVCCGet(env.store.cfg.Engine, key, maxTimestamp, storage.MVCCGetOptions{Inconsistent: true})
}

// TestAbortSpanOnlyWhenAWriteWasRemoved: no entry is written by a
// resolution that does not poison, that is not ABORTED, or that finds no
// intent of the transaction (nothing of it was taken away there).
func TestAbortSpanOnlyWhenAWriteWasRemoved(t *testing.T) {
	env := startClearTestStore(t)
	a := env.abortTestTxn(t, env.key("a"))
	env.resolve(t, env.key("a"), a, enginepb.ABORTED, false, env.clock.Now())
	if env.hasAbortEntry(t, a.ID) {
		t.Fatal("an unpoisoned resolution left an entry")
	}
	b := env.abortTestTxn(t, env.key("b"))
	env.resolve(t, env.key("b"), b, enginepb.COMMITTED, true, env.clock.Now())
	if env.hasAbortEntry(t, b.ID) {
		t.Fatal("a committing resolution left an entry")
	}
	c := kvpb.NewTransaction("never-wrote-here", 1, env.clock.Now())
	env.resolve(t, env.key("c"), c, enginepb.ABORTED, true, env.clock.Now())
	if env.hasAbortEntry(t, c.ID) {
		t.Fatal("a resolution that removed nothing left an entry")
	}
}

// TestAbortSpanGC: GC removes the entries written at or below its cutoff
// and keeps the rest, with a command that moves no GC threshold; below
// cluster version v22 the GC pass leaves the abort span alone.
func TestAbortSpanGC(t *testing.T) {
	env := startClearTestStore(t)
	old := env.abortTestTxn(t, env.key("old"))
	young := env.abortTestTxn(t, env.key("young"))
	oldTS := env.clock.Now()
	env.resolve(t, env.key("old"), old, enginepb.ABORTED, true, oldTS)
	youngTS := env.clock.Now()
	env.resolve(t, env.key("young"), young, enginepb.ABORTED, true, youngTS)
	ctx := context.Background()

	// Below v22 the pass proposes nothing for the abort span.
	env.cv.Store(int64(version.V21))
	if err := env.r.runGC(ctx, env.clock.Now().AddNanos(-int64(time.Hour)), false, nil); err != nil {
		t.Fatal(err)
	}
	if !env.hasAbortEntry(t, old.ID) || !env.hasAbortEntry(t, young.ID) {
		t.Fatal("a GC pass below v22 removed abort-span entries")
	}
	env.cv.Store(int64(version.V22))

	thr := env.r.GCThreshold()
	n, err := env.r.collectAbortSpan(ctx, oldTS)
	if err != nil {
		t.Fatal(err)
	}
	if n != 1 || env.hasAbortEntry(t, old.ID) || !env.hasAbortEntry(t, young.ID) {
		t.Fatalf("collected %d; old entry left %v, young entry left %v (want 1, false, true)", n, env.hasAbortEntry(t, old.ID), env.hasAbortEntry(t, young.ID))
	}
	if env.r.GCThreshold() != thr {
		t.Fatalf("abort-span GC moved the GC threshold from %s to %s", thr, env.r.GCThreshold())
	}
	if n, err := env.r.collectAbortSpan(ctx, oldTS); err != nil || n != 0 {
		t.Fatalf("a second pass collected %d (%v)", n, err)
	}
	// The ordinary pass uses AbortSpanTTL: the young entry is not due.
	if err := env.r.runGC(ctx, env.clock.Now().AddNanos(-int64(time.Hour)), false, nil); err != nil {
		t.Fatal(err)
	}
	if !env.hasAbortEntry(t, young.ID) {
		t.Fatal("an entry younger than AbortSpanTTL was collected")
	}
}

// TestAbortSpanSplit: a split gives both halves the parent's entries.
func TestAbortSpanSplit(t *testing.T) {
	env := startClearTestStore(t)
	victim := env.abortTestTxn(t, env.key("k"))
	env.resolve(t, env.key("k"), victim, enginepb.ABORTED, true, env.clock.Now())
	desc := env.r.Desc()
	left, right := desc, desc
	left.EndKey = env.key("m")
	left.Generation = desc.Generation + 1
	right.RangeID = 10
	right.StartKey = env.key("m")
	right.Generation = desc.Generation + 1
	b := env.store.cfg.Engine.NewBatch()
	defer func() { _ = b.Close() }()
	if err := env.r.stageSplit(b, &splitTrigger{Left: left, Right: right}); err != nil {
		t.Fatal(err)
	}
	for _, id := range []struct {
		side string
		r    kvpb.RangeDescriptor
	}{{"left", left}, {"right", right}} {
		if _, ok, err := loadAbortSpanEntry(b, id.r.RangeID, victim.ID); err != nil || !ok {
			t.Fatalf("the %s half has no entry (%v)", id.side, err)
		}
	}
}

// TestAbortSpanMerge: a merge carries the right-hand range's entries into
// the merged range, keeping the later of two entries for one
// transaction, and removes the right-hand range's.
func TestAbortSpanMerge(t *testing.T) {
	env := startClearTestStore(t)
	ctx := context.Background()
	rStart, rEnd := keys.TableDataSpan(clearTestTable + 1)
	rhsDesc := kvpb.RangeDescriptor{
		RangeID: 11, StartKey: env.end, EndKey: rEnd, NextReplicaID: 2, Generation: 1,
		Replicas: env.r.Desc().Replicas,
	}
	if !rhsDesc.StartKey.Equal(rStart) {
		t.Fatalf("the table spans are not adjacent: %s, %s", env.end, rStart)
	}
	rhs, err := env.store.CreateReplica(rhsDesc, true)
	if err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(10 * time.Second)
	for !rhs.IsLeader() {
		if time.Now().After(deadline) {
			t.Fatal("the right-hand replica never led its range")
		}
		time.Sleep(20 * time.Millisecond)
	}
	rexec := func(ba *kvpb.BatchRequest) {
		t.Helper()
		ba.Header.RangeID = rhs.rangeID
		if _, kerr := rhs.Execute(ctx, ba); kerr != nil {
			t.Fatal(kerr)
		}
	}
	// One transaction aborted on the right only, and one on both sides
	// (later on the right).
	rightOnly := kvpb.NewTransaction("right-only", 1, env.clock.Now())
	rightOnly.Key = append(rStart.Clone(), 'r')
	both := env.abortTestTxn(t, env.key("both"))
	bothLeft, _, err := func() (abortSpanEntry, bool, error) {
		env.resolve(t, env.key("both"), both, enginepb.ABORTED, true, env.clock.Now())
		return loadAbortSpanEntry(env.store.cfg.Engine, env.r.rangeID, both.ID)
	}()
	if err != nil {
		t.Fatal(err)
	}
	for _, c := range []struct {
		txn *kvpb.Transaction
		key keys.Key
	}{{rightOnly, rightOnly.Key}, {both, append(rStart.Clone(), 'b')}} {
		w := &kvpb.BatchRequest{Header: kvpb.BatchHeader{Txn: c.txn, CreateTxnRecord: c.txn == rightOnly}}
		w.Add(&kvpb.PutRequest{RequestHeader: kvpb.RequestHeader{Key: c.key}, Value: []byte("v")})
		rexec(w)
		res := &kvpb.BatchRequest{Header: kvpb.BatchHeader{Timestamp: env.clock.Now()}}
		res.Add(&kvpb.ResolveIntentRequest{RequestHeader: kvpb.RequestHeader{Key: c.key}, TxnID: c.txn.ID, Status: enginepb.ABORTED, Poison: true})
		rexec(res)
	}
	bothRight, ok, err := loadAbortSpanEntry(env.store.cfg.Engine, rhs.rangeID, both.ID)
	if err != nil || !ok || !bothLeft.Timestamp.Less(bothRight.Timestamp) {
		t.Fatalf("right-hand entry %+v (%v, %v), left-hand %+v", bothRight, ok, err, bothLeft)
	}

	merged := env.r.Desc()
	merged.EndKey = rEnd
	merged.Generation++
	trig := &mergeTrigger{Left: env.r.Desc(), Right: rhs.Desc(), Merged: merged, RightAppliedIndex: rhs.AppliedIndex()}
	b := env.store.cfg.Engine.NewBatch()
	defer func() { _ = b.Close() }()
	if err := env.r.stageMerge(ctx, b, trig); err != nil {
		t.Fatal(err)
	}
	for _, c := range []struct {
		name string
		id   uuid.UUID
		want hlc.Timestamp
	}{{"right-only", rightOnly.ID, hlc.Timestamp{}}, {"both", both.ID, bothRight.Timestamp}} {
		e, ok, err := loadAbortSpanEntry(b, env.r.rangeID, c.id)
		if err != nil || !ok {
			t.Fatalf("%s: the merged range has no entry (%v)", c.name, err)
		}
		if !c.want.IsEmpty() && e.Timestamp != c.want {
			t.Fatalf("%s: merged entry at %s, want the later %s", c.name, e.Timestamp, c.want)
		}
	}
	if total, _, err := countAbortSpan(b, rhs.rangeID, hlc.Timestamp{}); err != nil || total != 0 {
		t.Fatalf("the right-hand range keeps %d entries after the merge (%v)", total, err)
	}
}

// TestAbortSpanInSnapshot: a snapshot streams the range's entries; a
// replica seeded from it holds those and no others and refuses the
// aborted transaction as the leader does; and the consistency checksum
// covers the abort span.
func TestAbortSpanInSnapshot(t *testing.T) {
	env := startClearTestStore(t)
	ctx := context.Background()
	victim := env.abortTestTxn(t, env.key("k"))
	before := env.checksum(t)
	env.resolve(t, env.key("k"), victim, enginepb.ABORTED, true, env.clock.Now())
	// The resolution changed the data too; the abort span's part of the
	// checksum is checked on its own below.
	if bytes.Equal(before, env.checksum(t)) {
		t.Fatal("the checksum did not change")
	}

	snaps := &capturingSnapshotSender{}
	env.store.cfg.SnapshotSender = snaps
	desc := env.r.Desc()
	desc.Replicas = append(desc.Replicas, kvpb.ReplicaDescriptor{NodeID: 2, StoreID: 2, ReplicaID: 2})
	desc.NextReplicaID = 3
	if err := env.r.sendSnapshotTo(ctx, 2, desc, 2, nil); err != nil {
		t.Fatal(err)
	}
	streamed := false
	for _, kv := range snaps.kvs {
		if bytes.Equal(kv.Key, keys.AbortSpanKey(env.r.rangeID, victim.ID)) {
			streamed = true
		}
	}
	if !streamed {
		t.Fatal("the snapshot did not stream the abort-span entry")
	}

	eng2, err := storage.Open("", storage.Options{})
	if err != nil {
		t.Fatal(err)
	}
	stopper2 := stop.NewStopper()
	defer func() {
		stopper2.Stop()
		_ = eng2.Close()
	}()
	s2 := NewStore(StoreConfig{NodeID: 2, StoreID: 2, Engine: eng2, Clock: env.clock, Stopper: stopper2, Transport: nopRaftTransport{}, RaftTickInterval: time.Hour})
	// A stale entry of an earlier replica of the range must not survive.
	stale := uuid.New()
	if err := putAbortSpanEntry(eng2, env.r.rangeID, stale, abortSpanEntry{Timestamp: env.clock.Now()}); err != nil {
		t.Fatal(err)
	}
	i := 0
	if err := s2.ApplySnapshotStream(snaps.header, func() ([]SnapshotKV, error) {
		if i >= len(snaps.kvs) {
			return nil, nil
		}
		out := snaps.kvs[i:min(i+snapshotChunkSize, len(snaps.kvs))]
		i += len(out)
		return out, nil
	}); err != nil {
		t.Fatal(err)
	}
	r2, ok := s2.GetReplica(env.r.rangeID)
	if !ok {
		t.Fatal("no seeded replica")
	}
	if _, ok, _ := loadAbortSpanEntry(eng2, r2.rangeID, victim.ID); !ok {
		t.Fatal("the seeded replica has no abort-span entry")
	}
	if _, ok, _ := loadAbortSpanEntry(eng2, r2.rangeID, stale); ok {
		t.Fatal("the seeded replica kept a stale entry the stream did not carry")
	}
	ba := &kvpb.BatchRequest{Header: kvpb.BatchHeader{Txn: victim}}
	ba.Add(&kvpb.GetRequest{RequestHeader: kvpb.RequestHeader{Key: env.key("k")}})
	if _, kerr := r2.evalReadOnly(ba); kerr == nil || kerr.TxnAborted == nil {
		t.Fatalf("the seeded replica served the aborted transaction: %v", kerr)
	}
	snap2 := eng2.NewSnapshot()
	sum2, err := checksumRangeState(snap2, r2.Desc())
	_ = snap2.Close()
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(sum2, env.checksum(t)) {
		t.Fatal("the seeded replica's checksum differs from the leader's")
	}

	// The checksum sees the abort span: an extra entry on the copy alone
	// makes the two differ.
	if err := putAbortSpanEntry(eng2, r2.rangeID, uuid.New(), abortSpanEntry{Timestamp: env.clock.Now()}); err != nil {
		t.Fatal(err)
	}
	snap2 = eng2.NewSnapshot()
	sum3, err := checksumRangeState(snap2, r2.Desc())
	_ = snap2.Close()
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Equal(sum3, sum2) {
		t.Fatal("an abort-span entry does not change the consistency checksum")
	}
}
