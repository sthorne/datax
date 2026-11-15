package kvserver

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"go.etcd.io/raft/v3/raftpb"

	"github.com/sthorne/datax/pkg/base"
	"github.com/sthorne/datax/pkg/keys"
	"github.com/sthorne/datax/pkg/kvpb"
	"github.com/sthorne/datax/pkg/storage"
	"github.com/sthorne/datax/pkg/util/hlc"
	"github.com/sthorne/datax/pkg/util/stop"
	"github.com/sthorne/datax/pkg/version"
)

// The range clear's unit tests (issue #316) run one replica of a range
// that is exactly one table's data span, on a store of its own, with the
// catalog the leader consults stood in for by catalogSender.

const clearTestTable = 104

type nopRaftTransport struct{}

func (nopRaftTransport) SendRaftMessage(context.Context, base.NodeID, base.RangeID, raftpb.Message) error {
	return nil
}

func (nopRaftTransport) SendRaftHeartbeats(context.Context, base.NodeID, []RaftHeartbeat, []RaftHeartbeat, []RaftHeartbeat) error {
	return nil
}

// catalogSender answers the leader's catalog reads from a map.
type catalogSender struct {
	mu   sync.Mutex
	vals map[string][]byte
}

func (c *catalogSender) set(k keys.Key, v []byte) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if v == nil {
		delete(c.vals, string(k))
		return
	}
	c.vals[string(k)] = v
}

func (c *catalogSender) Send(_ context.Context, ba *kvpb.BatchRequest) (*kvpb.BatchResponse, *kvpb.Error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	br := &kvpb.BatchResponse{}
	for _, u := range ba.Requests {
		if u.Get == nil {
			return nil, kvpb.NewErrorf("catalogSender: %s", u.GetInner().Method())
		}
		br.Responses = append(br.Responses, kvpb.ResponseUnion{Get: &kvpb.GetResponse{Value: c.vals[string(u.Get.Key)]}})
	}
	return br, nil
}

type clearTestEnv struct {
	store      *Store
	r          *Replica
	cat        *catalogSender
	start, end keys.Key
	clock      *hlc.Clock
	cv         *version.Version
}

func startClearTestStore(t *testing.T) *clearTestEnv {
	t.Helper()
	eng, err := storage.Open("", storage.Options{})
	if err != nil {
		t.Fatal(err)
	}
	stopper := stop.NewStopper()
	t.Cleanup(func() {
		stopper.Stop()
		_ = eng.Close()
	})
	env := &clearTestEnv{cat: &catalogSender{vals: map[string][]byte{}}, clock: hlc.NewClock(nil, 250*time.Millisecond)}
	cv := version.V22
	env.cv = &cv
	env.store = NewStore(StoreConfig{
		NodeID: 1, StoreID: 1, Engine: eng, Clock: env.clock, Stopper: stopper,
		Transport: nopRaftTransport{}, RaftTickInterval: 10 * time.Millisecond,
		ClusterVersion: func() version.Version { return *env.cv },
	})
	env.store.SetSender(env.cat)
	env.start, env.end = keys.TableDataSpan(clearTestTable)
	env.cat.set(keys.DroppedTableKey(clearTestTable), []byte(`{"id":104}`))
	desc := kvpb.RangeDescriptor{
		RangeID: 9, StartKey: env.start, EndKey: env.end, NextReplicaID: 2, Generation: 3,
		Replicas: []kvpb.ReplicaDescriptor{{NodeID: 1, StoreID: 1, ReplicaID: 1}},
	}
	env.r, err = env.store.CreateReplica(desc, true)
	if err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(10 * time.Second)
	for !env.r.IsLeader() {
		if time.Now().After(deadline) {
			t.Fatal("the replica never led its range")
		}
		time.Sleep(20 * time.Millisecond)
	}
	return env
}

func (e *clearTestEnv) key(s string) keys.Key {
	return append(e.start.Clone(), s...)
}

func (e *clearTestEnv) exec(t *testing.T, ba *kvpb.BatchRequest) (*kvpb.BatchResponse, *kvpb.Error) {
	t.Helper()
	ba.Header.RangeID = e.r.rangeID
	if ba.Header.Timestamp.IsEmpty() && ba.Header.Txn == nil {
		ba.Header.Timestamp = e.clock.Now()
	}
	return e.r.Execute(context.Background(), ba)
}

func (e *clearTestEnv) put(t *testing.T, k, v string) {
	t.Helper()
	ba := &kvpb.BatchRequest{}
	ba.Add(&kvpb.PutRequest{RequestHeader: kvpb.RequestHeader{Key: e.key(k)}, Value: []byte(v)})
	if _, kerr := e.exec(t, ba); kerr != nil {
		t.Fatal(kerr)
	}
}

func (e *clearTestEnv) del(t *testing.T, k string) {
	t.Helper()
	ba := &kvpb.BatchRequest{}
	ba.Add(&kvpb.DeleteRequest{RequestHeader: kvpb.RequestHeader{Key: e.key(k)}})
	if _, kerr := e.exec(t, ba); kerr != nil {
		t.Fatal(kerr)
	}
}

func (e *clearTestEnv) scanAt(t *testing.T, ts hlc.Timestamp) ([]kvpb.KeyValue, *kvpb.Error) {
	t.Helper()
	ba := &kvpb.BatchRequest{Header: kvpb.BatchHeader{Timestamp: ts}}
	ba.Add(&kvpb.ScanRequest{RequestHeader: kvpb.RequestHeader{Key: e.start, EndKey: e.end}})
	br, kerr := e.exec(t, ba)
	if kerr != nil {
		return nil, kerr
	}
	return br.Responses[0].Scan.Rows, nil
}

func (e *clearTestEnv) fill(t *testing.T, n int) {
	t.Helper()
	for i := 0; i < n; i++ {
		e.put(t, fmt.Sprintf("row-%03d", i), strings.Repeat("v", 10+i%7))
	}
	for i := 0; i < n; i += 3 {
		e.put(t, fmt.Sprintf("row-%03d", i), "overwritten") // a second version
	}
	for i := 1; i < n; i += 5 {
		e.del(t, fmt.Sprintf("row-%03d", i)) // a tombstone over a version
	}
}

func (e *clearTestEnv) checksum(t *testing.T) []byte {
	t.Helper()
	snap := e.store.cfg.Engine.NewSnapshot()
	defer func() { _ = snap.Close() }()
	sum, err := checksumRangeState(snap, e.r.Desc())
	if err != nil {
		t.Fatal(err)
	}
	return sum
}

// TestClearRangeEmptiesTheRange: one command removes every version and
// tombstone of the range; the size becomes exactly zero (persisted, not
// only in memory) and the GC threshold the clear's time, at least the
// newest version's; a read or write at or below it is refused and a read
// above it sees nothing.
func TestClearRangeEmptiesTheRange(t *testing.T) {
	env := startClearTestStore(t)
	env.fill(t, 60)
	eng := env.store.cfg.Engine
	want, err := countSpanForClear(eng, env.start, env.end)
	if err != nil {
		t.Fatal(err)
	}
	exact, err := spanSizeBytes(eng, env.start, env.end)
	if err != nil {
		t.Fatal(err)
	}
	if want.keys < 60 || exact != want.bytes || env.r.SizeBytes() <= 0 {
		t.Fatalf("before the clear: %+v, %d bytes, size %d", want, exact, env.r.SizeBytes())
	}
	before := env.clock.Now()
	lastIndex := env.r.LastIndex()
	res, err := env.store.ClearRange(context.Background(), env.r.rangeID, env.start, env.end)
	if err != nil {
		t.Fatal(err)
	}
	if n := env.r.LastIndex() - lastIndex; n != 1 {
		t.Errorf("the clear took %d raft entries, want 1", n)
	}
	if res.Keys != want.keys || res.Bytes != exact {
		t.Errorf("reported %d keys and %d bytes, the span held %d and %d", res.Keys, res.Bytes, want.keys, exact)
	}
	if !before.Less(res.Timestamp) || res.Timestamp.Less(want.newest) {
		t.Errorf("clear at %s: before it %s, newest version %s", res.Timestamp, before, want.newest)
	}
	if after, err := countSpanForClear(eng, env.start, env.end); err != nil || after.keys != 0 {
		t.Fatalf("after the clear the span holds %+v (%v)", after, err)
	}
	if got := env.r.SizeBytes(); got != 0 {
		t.Errorf("size after the clear: %d, want 0", got)
	}
	if got := env.r.GCThreshold(); got != res.Timestamp {
		t.Errorf("GC threshold %s, want the clear's time %s", got, res.Timestamp)
	}
	st, err := loadReplicaState(eng, env.r.rangeID)
	if err != nil {
		t.Fatal(err)
	}
	if st.SizeBytes != 0 || st.GCThreshold != res.Timestamp || st.AppliedIndex != res.AppliedIndex {
		t.Errorf("persisted state %+v", st)
	}

	// At or below the clear: refused, never answered from nothing.
	for _, ts := range []hlc.Timestamp{before, res.Timestamp} {
		if _, kerr := env.scanAt(t, ts); kerr == nil || !kerr.IsBelowGCThreshold() {
			t.Errorf("a read at %s after a clear at %s: %v", ts, res.Timestamp, kerr)
		}
		ba := &kvpb.BatchRequest{Header: kvpb.BatchHeader{Timestamp: ts}}
		ba.Add(&kvpb.PutRequest{RequestHeader: kvpb.RequestHeader{Key: env.key("late")}, Value: []byte("x")})
		if _, kerr := env.exec(t, ba); kerr == nil {
			t.Errorf("a write at %s landed beneath a clear at %s", ts, res.Timestamp)
		}
	}
	// Above it: empty, and the range takes writes as before.
	if rows, kerr := env.scanAt(t, res.Timestamp.Next()); kerr != nil || len(rows) != 0 {
		t.Fatalf("a read above the clear: %d rows, %v", len(rows), kerr)
	}
	env.put(t, "after", "v")
	if rows, kerr := env.scanAt(t, env.clock.Now()); kerr != nil || len(rows) != 1 {
		t.Fatalf("a write after the clear: %d rows, %v", len(rows), kerr)
	}
	if env.r.SizeBytes() <= 0 {
		t.Error("the size did not grow again after the clear")
	}
}

// TestClearRangeRefusesIntents: an intent in the span — a transaction
// that may yet commit — refuses the clear and leaves everything as it
// was.
func TestClearRangeRefusesIntents(t *testing.T) {
	env := startClearTestStore(t)
	env.fill(t, 10)
	txn := kvpb.NewTransaction("writer", 1, env.clock.Now())
	ba := &kvpb.BatchRequest{Header: kvpb.BatchHeader{Txn: txn}}
	ba.Add(&kvpb.PutRequest{RequestHeader: kvpb.RequestHeader{Key: env.key("row-005")}, Value: []byte("pending")})
	if _, kerr := env.exec(t, ba); kerr != nil {
		t.Fatal(kerr)
	}
	sum, size, thr := env.checksum(t), env.r.SizeBytes(), env.r.GCThreshold()
	_, err := env.store.ClearRange(context.Background(), env.r.rangeID, env.start, env.end)
	if err == nil || !strings.Contains(err.Error(), "intent") {
		t.Fatalf("a clear over an intent: %v", err)
	}
	if !bytes.Equal(env.checksum(t), sum) || env.r.SizeBytes() != size || env.r.GCThreshold() != thr {
		t.Fatal("a refused clear changed the range")
	}
}

// TestClearRangeRefusesWhatIsNotADroppedTable: the leader clears only one
// table's whole data span, of a table with a dropped-table record and no
// descriptor under its id, on a cluster at v22.
func TestClearRangeRefusesWhatIsNotADroppedTable(t *testing.T) {
	env := startClearTestStore(t)
	env.fill(t, 5)
	ctx := context.Background()
	sum := env.checksum(t)
	expectRefusal := func(what, contains string, start, end keys.Key) {
		t.Helper()
		_, err := env.store.ClearRange(ctx, env.r.rangeID, start, end)
		if err == nil || !strings.Contains(err.Error(), contains) {
			t.Fatalf("%s: %v (want %q)", what, err, contains)
		}
		if !bytes.Equal(env.checksum(t), sum) {
			t.Fatalf("%s: the range changed", what)
		}
	}
	env.cat.set(keys.TableDescKey(clearTestTable), []byte(`{}`))
	expectRefusal("a live table", "live descriptor", env.start, env.end)
	env.cat.set(keys.TableDescKey(clearTestTable), nil)
	env.cat.set(keys.SequenceDescKey(clearTestTable), []byte(`{}`))
	expectRefusal("a live sequence", "live descriptor", env.start, env.end)
	env.cat.set(keys.SequenceDescKey(clearTestTable), nil)
	env.cat.set(keys.DroppedTableKey(clearTestTable), nil)
	expectRefusal("no dropped-table record", "no dropped-table record", env.start, env.end)
	env.cat.set(keys.DroppedTableKey(clearTestTable), []byte(`{"id":104}`))
	expectRefusal("part of a table's span", "whole data span", env.start, env.key("row-003"))
	wider := keys.TableDataPrefix(clearTestTable + 1).PrefixEnd()
	expectRefusal("more than one table's span", "whole data span", env.start, wider)
	*env.cv = version.V21
	expectRefusal("below v22", "cluster version", env.start, env.end)
	*env.cv = version.V22
	if _, err := env.store.ClearRange(ctx, env.r.rangeID, env.start, env.end); err != nil {
		t.Fatalf("the dropped table's span: %v", err)
	}
}

// TestClearRangeAboveServedReads: the clear's time is above every read the
// range has served and its closed timestamp, so no read answered before
// the clear can be answered differently after it — a read above the
// clear's time would see the rows gone.
func TestClearRangeAboveServedReads(t *testing.T) {
	env := startClearTestStore(t)
	env.fill(t, 5)
	future := env.clock.Now().AddNanos(int64(time.Hour))
	if rows, kerr := env.scanAt(t, future); kerr != nil || len(rows) == 0 {
		t.Fatalf("a read in the future: %d rows, %v", len(rows), kerr)
	}
	if err := env.r.publishClosedTimestamp(context.Background(), env.clock.Now()); err != nil {
		t.Fatal(err)
	}
	res, err := env.store.ClearRange(context.Background(), env.r.rangeID, env.start, env.end)
	if err != nil {
		t.Fatal(err)
	}
	if res.Timestamp.Less(future) {
		t.Fatalf("clear at %s, below a read served at %s", res.Timestamp, future)
	}
	if res.Timestamp.Less(env.r.ClosedTimestamp()) {
		t.Fatalf("clear at %s, below the closed timestamp %s", res.Timestamp, env.r.ClosedTimestamp())
	}
	if _, kerr := env.scanAt(t, future); kerr == nil || !kerr.IsBelowGCThreshold() {
		t.Fatalf("the future read again after the clear: %v", kerr)
	}
}

// TestClearRangeAppliesOnce: a clear command applied a second time — a
// replayed or re-proposed entry — finds the threshold at its time and
// applies nothing, and neither does one built against another generation
// of the range: the rows written since stay, and so does the size.
func TestClearRangeAppliesOnce(t *testing.T) {
	env := startClearTestStore(t)
	env.fill(t, 10)
	ctx := context.Background()
	res, err := env.store.ClearRange(ctx, env.r.rangeID, env.start, env.end)
	if err != nil {
		t.Fatal(err)
	}
	env.put(t, "after-1", "v")
	env.put(t, "after-2", "v")
	sum, size := env.checksum(t), env.r.SizeBytes()
	desc := env.r.Desc()
	propose := func(ts hlc.Timestamp, gen int64) bool {
		t.Helper()
		ba := &kvpb.BatchRequest{Header: kvpb.BatchHeader{RangeID: env.r.rangeID, Timestamp: ts}}
		ba.Add(&kvpb.ClearRangeRequest{RequestHeader: kvpb.RequestHeader{Key: desc.StartKey, EndKey: desc.EndKey}, Timestamp: ts, Generation: gen})
		br, kerr := env.r.propose(ctx, ba)
		if kerr != nil {
			t.Fatal(kerr)
		}
		return br.Responses[0].ClearRange.Cleared
	}
	if propose(res.Timestamp, desc.Generation) {
		t.Fatal("the same clear applied twice")
	}
	if propose(env.clock.Now(), desc.Generation-1) {
		t.Fatal("a clear built against an older generation applied")
	}
	if !bytes.Equal(env.checksum(t), sum) || env.r.SizeBytes() != size || env.r.GCThreshold() != res.Timestamp {
		t.Fatal("a clear that did not apply changed the range")
	}
	// A clear in a batch with anything else is refused whole.
	ba := &kvpb.BatchRequest{Header: kvpb.BatchHeader{RangeID: env.r.rangeID, Timestamp: env.clock.Now()}}
	ba.Add(&kvpb.ClearRangeRequest{RequestHeader: kvpb.RequestHeader{Key: desc.StartKey, EndKey: desc.EndKey}, Timestamp: ba.Header.Timestamp, Generation: desc.Generation})
	ba.Add(&kvpb.PutRequest{RequestHeader: kvpb.RequestHeader{Key: env.key("x")}, Value: []byte("v")})
	if _, kerr := env.r.propose(ctx, ba); kerr == nil {
		t.Fatal("a clear batched with a put applied")
	}
	if !bytes.Equal(env.checksum(t), sum) {
		t.Fatal("a refused batch changed the range")
	}
}

// capturingSnapshotSender keeps the stream a replica sends.
type capturingSnapshotSender struct {
	header []byte
	kvs    []SnapshotKV
}

func (c *capturingSnapshotSender) SendSnapshot(_ context.Context, _ base.NodeID, header []byte, next func() ([]SnapshotKV, error)) error {
	c.header = header
	for {
		kvs, err := next()
		if err != nil {
			return err
		}
		if len(kvs) == 0 {
			return nil
		}
		c.kvs = append(c.kvs, kvs...)
	}
}

// TestSnapshotAfterClearRange: a snapshot taken after a clear streams no
// key of the span and carries the threshold and the zero size, and a
// replica seeded from it holds the same state as the leader.
func TestSnapshotAfterClearRange(t *testing.T) {
	env := startClearTestStore(t)
	env.fill(t, 20)
	ctx := context.Background()
	res, err := env.store.ClearRange(ctx, env.r.rangeID, env.start, env.end)
	if err != nil {
		t.Fatal(err)
	}
	snaps := &capturingSnapshotSender{}
	env.store.cfg.SnapshotSender = snaps
	desc := env.r.Desc()
	desc.Replicas = append(desc.Replicas, kvpb.ReplicaDescriptor{NodeID: 2, StoreID: 2, ReplicaID: 2})
	desc.NextReplicaID = 3
	if err := env.r.sendSnapshotTo(ctx, 2, desc, 2, nil); err != nil {
		t.Fatal(err)
	}
	var h snapshotHeader
	if err := json.Unmarshal(snaps.header, &h); err != nil {
		t.Fatal(err)
	}
	if h.GCThreshold != res.Timestamp || h.SizeBytes != 0 || h.AppliedIndex < res.AppliedIndex {
		t.Fatalf("snapshot header %+v after a clear at %s (index %d)", h, res.Timestamp, res.AppliedIndex)
	}
	lower := storage.EncodeMVCCKey(env.start, hlc.Timestamp{})
	upper := storage.EncodeMVCCKey(env.end, hlc.Timestamp{})
	for _, kv := range snaps.kvs {
		if bytes.Compare(kv.Key, lower) >= 0 && bytes.Compare(kv.Key, upper) < 0 {
			t.Fatalf("the snapshot streamed a cleared key %x", kv.Key)
		}
	}

	// Seed a second store from it and compare.
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
	if r2.GCThreshold() != res.Timestamp || r2.SizeBytes() != 0 {
		t.Fatalf("seeded replica: threshold %s, size %d", r2.GCThreshold(), r2.SizeBytes())
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
}
