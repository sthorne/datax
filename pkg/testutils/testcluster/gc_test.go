package testcluster

import (
	"bytes"
	"context"
	"crypto/sha256"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/sthorne/datax/pkg/base"
	"github.com/sthorne/datax/pkg/cluster"
	"github.com/sthorne/datax/pkg/keys"
	"github.com/sthorne/datax/pkg/kvclient"
	"github.com/sthorne/datax/pkg/kvpb"
	"github.com/sthorne/datax/pkg/server"
	"github.com/sthorne/datax/pkg/storage"
	"github.com/sthorne/datax/pkg/util/hlc"
)

// startGCNode starts a single-node cluster with an injected engine (so the
// test can inspect raw storage) and the background GC loop disabled (tests
// drive GC explicitly via RunGCOnce).
func startGCNode(t *testing.T) (*server.Node, *storage.Engine) {
	return startGCNodeCfg(t)
}

// startGCNodeCfg is startGCNode with config overrides applied on top of
// the fixture defaults.
func startGCNodeCfg(t *testing.T, opts ...func(*server.Config)) (*server.Node, *storage.Engine) {
	t.Helper()
	eng, err := storage.Open("", storage.Options{})
	if err != nil {
		t.Fatal(err)
	}
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	cfg := server.Config{
		Listener: lis,
		Engine:   eng,
		GCTTL:    -1, // disable the background loop
		StaticBootstrap: &server.StaticBootstrap{
			ClusterID: uuid.New(),
			NodeID:    1,
			Range1:    cluster.Range1Descriptor([]base.NodeID{1}),
		},
	}
	for _, opt := range opts {
		opt(&cfg)
	}
	n, err := server.Start(cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		n.Stop()
		_ = eng.Close()
	})
	return n, eng
}

// mvccEntryCounts returns how many intent-metadata records and how many
// versions the engine holds for user key k.
func mvccEntryCounts(t *testing.T, eng *storage.Engine, k keys.Key) (metas, versions int) {
	t.Helper()
	lower := storage.EncodeMVCCKey(k, hlc.Timestamp{})
	upper := storage.EncodeMVCCKey(k.Next(), hlc.Timestamp{})
	it := eng.NewIter(lower, upper)
	defer func() { _ = it.Close() }()
	for ok := it.SeekGE(lower); ok; ok = it.Next() {
		_, ts, err := storage.DecodeMVCCKey(it.Key())
		if err != nil {
			t.Fatalf("decoding %x: %v", it.Key(), err)
		}
		if ts.IsEmpty() {
			metas++
		} else {
			versions++
		}
	}
	return metas, versions
}

// txnRecordsAnchoredAt counts transaction records whose anchor key is k.
func txnRecordsAnchoredAt(t *testing.T, eng *storage.Engine, k keys.Key) int {
	t.Helper()
	lo, hi := keys.RangeLocalAddressedSpan(keys.MinKey, keys.MaxKey)
	it := eng.NewIter(lo, hi)
	defer func() { _ = it.Close() }()
	count := 0
	for ok := it.SeekGE(lo); ok; ok = it.Next() {
		var txn kvpb.Transaction
		if err := jsonUnmarshal(it.Value(), &txn); err != nil {
			continue
		}
		if bytes.Equal(txn.Key, k) {
			count++
		}
	}
	return count
}

// TestGCReclaimsOldVersions drives one GC pass and asserts, by raw engine
// iteration, exactly which versions survive: the newest at-or-below-threshold
// version per key, deleted keys reclaimed entirely, keys under intents
// untouched, and reads below the raised threshold rejected.
func TestGCReclaimsOldVersions(t *testing.T) {
	n, eng := startGCNode(t)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	db := n.DB()

	prefix := keys.TableDataPrefix(700)
	kMulti := append(prefix.Clone(), "multi"...)
	kDel := append(prefix.Clone(), "victim"...)
	kSingle := append(prefix.Clone(), "single"...)
	kIntent := append(prefix.Clone(), "intent"...)

	for _, v := range []string{"v1", "v2", "v3"} {
		if err := db.Put(ctx, kMulti, []byte(v)); err != nil {
			t.Fatal(err)
		}
	}
	if err := db.Put(ctx, kDel, []byte("doomed")); err != nil {
		t.Fatal(err)
	}
	if err := db.RunTxn(ctx, "del", func(ctx context.Context, txn *kvclient.Txn) error {
		return txn.Delete(ctx, kDel)
	}); err != nil {
		t.Fatal(err)
	}
	if err := db.Put(ctx, kSingle, []byte("keep")); err != nil {
		t.Fatal(err)
	}
	for _, v := range []string{"old1", "old2"} {
		if err := db.Put(ctx, kIntent, []byte(v)); err != nil {
			t.Fatal(err)
		}
	}
	tsOld := n.Clock().Now()

	// Age everything beyond the (time-compressed) TTL, then lay a fresh
	// intent over kIntent's old versions: the intent must shield them.
	time.Sleep(500 * time.Millisecond)
	tx := db.NewTxn("open")
	if err := tx.Put(ctx, kIntent, []byte("prov")); err != nil {
		t.Fatal(err)
	}

	n.Store().RunGCOnce(ctx, 200*time.Millisecond)

	if metas, versions := mvccEntryCounts(t, eng, kMulti); metas != 0 || versions != 1 {
		t.Fatalf("kMulti after GC: %d metas, %d versions; want 0, 1", metas, versions)
	}
	if metas, versions := mvccEntryCounts(t, eng, kDel); metas != 0 || versions != 0 {
		t.Fatalf("kDel after GC: %d metas, %d versions; want 0, 0 (tombstone and history reclaimed)", metas, versions)
	}
	if metas, versions := mvccEntryCounts(t, eng, kSingle); metas != 0 || versions != 1 {
		t.Fatalf("kSingle after GC: %d metas, %d versions; want 0, 1", metas, versions)
	}
	if metas, versions := mvccEntryCounts(t, eng, kIntent); metas != 1 || versions != 3 {
		t.Fatalf("kIntent after GC: %d metas, %d versions; want 1, 3 (intent shields the key)", metas, versions)
	}

	// Reads at or below the threshold are rejected, non-retryably.
	rep, ok := n.Store().GetReplica(1)
	if !ok {
		t.Fatal("no replica of range 1")
	}
	oldBa := &kvpb.BatchRequest{Header: kvpb.BatchHeader{RangeID: 1, Timestamp: tsOld}}
	oldBa.Add(&kvpb.GetRequest{RequestHeader: kvpb.RequestHeader{Key: kMulti}})
	if _, kerr := rep.Execute(ctx, oldBa); kerr == nil || !strings.Contains(kerr.Message, "GC threshold") {
		t.Fatalf("read at %s below threshold: got %v; want GC threshold error", tsOld, kerr)
	}

	// Current reads see the survivors.
	if v, err := db.Get(ctx, kMulti); err != nil || string(v) != "v3" {
		t.Fatalf("kMulti: %q, %v; want v3", v, err)
	}
	if v, err := db.Get(ctx, kDel); err != nil || v != nil {
		t.Fatalf("kDel: %q, %v; want absent", v, err)
	}
	if v, err := db.Get(ctx, kSingle); err != nil || string(v) != "keep" {
		t.Fatalf("kSingle: %q, %v; want keep", v, err)
	}

	// The open transaction (younger than the threshold) commits fine, and a
	// second GC pass then reclaims the versions its intent was shielding.
	if err := tx.Commit(ctx); err != nil {
		t.Fatalf("committing the open transaction after GC: %v", err)
	}
	time.Sleep(500 * time.Millisecond)
	n.Store().RunGCOnce(ctx, 200*time.Millisecond)
	if metas, versions := mvccEntryCounts(t, eng, kIntent); metas != 0 || versions != 1 {
		t.Fatalf("kIntent after commit+GC: %d metas, %d versions; want 0, 1", metas, versions)
	}
	if v, err := db.Get(ctx, kIntent); err != nil || string(v) != "prov" {
		t.Fatalf("kIntent: %q, %v; want prov", v, err)
	}
}

// TestGCTxnRecordsAndResurrectionGuard: finalized transaction records older
// than the TTL are reclaimed, and a zombie transaction born below the
// threshold cannot recreate its record.
func TestGCTxnRecordsAndResurrectionGuard(t *testing.T) {
	n, eng := startGCNode(t)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	db := n.DB()

	prefix := keys.TableDataPrefix(701)
	kCommitted := append(prefix.Clone(), "committed"...)
	kAborted := append(prefix.Clone(), "aborted"...)

	if err := db.RunTxn(ctx, "c", func(ctx context.Context, txn *kvclient.Txn) error {
		return txn.Put(ctx, kCommitted, []byte("x"))
	}); err != nil {
		t.Fatal(err)
	}
	txAbort := db.NewTxn("a")
	if err := txAbort.Put(ctx, kAborted, []byte("y")); err != nil {
		t.Fatal(err)
	}
	if err := txAbort.Rollback(ctx); err != nil {
		t.Fatal(err)
	}
	if c := txnRecordsAnchoredAt(t, eng, kCommitted); c != 1 {
		t.Fatalf("committed txn record count before GC: %d; want 1", c)
	}
	if c := txnRecordsAnchoredAt(t, eng, kAborted); c != 1 {
		t.Fatalf("aborted txn record count before GC: %d; want 1", c)
	}
	tsOld := n.Clock().Now()

	time.Sleep(500 * time.Millisecond)
	n.Store().RunGCOnce(ctx, 200*time.Millisecond)

	if c := txnRecordsAnchoredAt(t, eng, kCommitted); c != 0 {
		t.Fatalf("committed txn record count after GC: %d; want 0", c)
	}
	if c := txnRecordsAnchoredAt(t, eng, kAborted); c != 0 {
		t.Fatalf("aborted txn record count after GC: %d; want 0", c)
	}

	// Resurrection guard: a transaction BORN below the threshold (but whose
	// read/write timestamps have moved above it, so it passes the batch-level
	// threshold check) must not recreate its possibly-reclaimed record.
	zombie := kvpb.NewTransaction("zombie", 0, n.Clock().Now())
	zombie.MinTimestamp = tsOld
	zombie.Key = kAborted
	rep, ok := n.Store().GetReplica(1)
	if !ok {
		t.Fatal("no replica of range 1")
	}
	ba := &kvpb.BatchRequest{Header: kvpb.BatchHeader{RangeID: 1, Txn: zombie, CreateTxnRecord: true}}
	ba.Add(&kvpb.PutRequest{RequestHeader: kvpb.RequestHeader{Key: kAborted}, Value: []byte("z")})
	_, kerr := rep.Execute(ctx, ba)
	if kerr == nil || kerr.TxnAborted == nil {
		t.Fatalf("zombie record creation: got %v; want TxnAborted (resurrection guard)", kerr)
	}
}

// TestGCReplicaConsistency runs GC on a 3-node range and asserts every
// replica's raw engine content over the data span converges to the same
// bytes with old versions gone — the replicated-GC divergence tripwire.
func TestGCReplicaConsistency(t *testing.T) {
	tc, engines := StartWithEngines(t, 3)
	nodes := tc.Nodes
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	prefix := keys.TableDataPrefix(710)
	db := nodes[0].DB()
	const numKeys = 20
	for round := 0; round < 3; round++ {
		for i := 0; i < numKeys; i++ {
			k := append(prefix.Clone(), byte('a'+i))
			if err := db.Put(ctx, k, []byte{byte('0' + round)}); err != nil {
				t.Fatal(err)
			}
		}
	}
	time.Sleep(500 * time.Millisecond)

	// Only the leader's store acts; running the pass everywhere is what the
	// real housekeeping loop does too.
	for _, n := range nodes {
		n.Store().RunGCOnce(ctx, 200*time.Millisecond)
	}

	// Every replica converges to identical bytes with exactly one surviving
	// version per key.
	spanLo := storage.EncodeMVCCKey(prefix, hlc.Timestamp{})
	spanHi := storage.EncodeMVCCKey(prefix.PrefixEnd(), hlc.Timestamp{})
	deadline := time.Now().Add(15 * time.Second)
	for {
		sums := make([][32]byte, len(engines))
		counts := make([]int, len(engines))
		for i, eng := range engines {
			h := sha256.New()
			it := eng.NewIter(spanLo, spanHi)
			for ok := it.SeekGE(spanLo); ok; ok = it.Next() {
				h.Write(it.Key())
				h.Write(it.Value())
				counts[i]++
			}
			if err := it.Close(); err != nil {
				t.Fatal(err)
			}
			copy(sums[i][:], h.Sum(nil))
		}
		if counts[0] == numKeys && sums[0] == sums[1] && sums[1] == sums[2] {
			return // converged: one version per key, byte-identical replicas
		}
		if time.Now().After(deadline) {
			t.Fatalf("replicas did not converge after GC: entry counts %v, checksums equal: %v/%v",
				counts, sums[0] == sums[1], sums[1] == sums[2])
		}
		time.Sleep(50 * time.Millisecond)
	}
}

// TestGCResolvesOrphanedCommittedIntents: a coordinator that crashes after
// the commit flip but before resolving its intents must NOT lose the
// committed writes when GC later reclaims the record. GC resolves the
// record's write set (across ranges) before collecting it. Regression test
// for issue #16.
func TestGCResolvesOrphanedCommittedIntents(t *testing.T) {
	tc, engines := StartWithEngines(t, 3)
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()
	db := tc.Nodes[0].DB()

	prefix := keys.TableDataPrefix(780)
	if _, err := db.AdminSplit(ctx, append(prefix.Clone(), 'm')); err != nil {
		t.Fatal(err)
	}
	kLeft := append(prefix.Clone(), "a-left"...)
	kRight := append(prefix.Clone(), "z-right"...)

	// Drive the transaction manually so we can crash it at the worst moment:
	// intents on two ranges, record committed, nothing resolved.
	txn := kvpb.NewTransaction("orphan", 0, tc.Nodes[0].Clock().Now())
	txn.Key = kLeft
	send := func(ba *kvpb.BatchRequest) {
		t.Helper()
		if _, kerr := db.Send(ctx, ba); kerr != nil {
			t.Fatal(kerr)
		}
	}
	ba := &kvpb.BatchRequest{Header: kvpb.BatchHeader{Txn: txn, CreateTxnRecord: true}}
	ba.Add(&kvpb.PutRequest{RequestHeader: kvpb.RequestHeader{Key: kLeft}, Value: []byte("left")})
	send(ba)
	ba = &kvpb.BatchRequest{Header: kvpb.BatchHeader{Txn: txn}}
	ba.Add(&kvpb.PutRequest{RequestHeader: kvpb.RequestHeader{Key: kRight}, Value: []byte("right")})
	send(ba)
	ba = &kvpb.BatchRequest{Header: kvpb.BatchHeader{Txn: txn}}
	ba.Add(&kvpb.EndTxnRequest{
		RequestHeader: kvpb.RequestHeader{Key: kLeft},
		Commit:        true,
		IntentKeys:    []keys.Key{kLeft.Clone(), kRight.Clone()},
	})
	send(ba)
	// "Crash": no resolution, no heartbeats. Both intents are orphaned.

	// Age past the TTL and run GC everywhere (each range's leader acts).
	time.Sleep(500 * time.Millisecond)
	for round := 0; round < 2; round++ {
		for _, n := range tc.Nodes {
			n.Store().RunGCOnce(ctx, 200*time.Millisecond)
		}
		time.Sleep(100 * time.Millisecond)
	}

	// The committed writes survive — without pre-collection resolution the
	// next reader would push, find no record, and wrongly abort the intents.
	if v, err := db.Get(ctx, kLeft); err != nil || string(v) != "left" {
		t.Fatalf("committed write on the anchor range lost: %q, %v", v, err)
	}
	if v, err := db.Get(ctx, kRight); err != nil || string(v) != "right" {
		t.Fatalf("committed write on the other range lost: %q, %v", v, err)
	}
	// The intents were resolved (no metadata left on any replica) and the
	// record was reclaimed.
	for i, eng := range engines {
		if metas, _ := mvccEntryCounts(t, eng, kLeft); metas != 0 {
			t.Fatalf("node %d: intent metadata survives on kLeft", i+1)
		}
		if metas, _ := mvccEntryCounts(t, eng, kRight); metas != 0 {
			t.Fatalf("node %d: intent metadata survives on kRight", i+1)
		}
	}
	if c := txnRecordsAnchoredAt(t, engines[0], kLeft); c != 0 {
		t.Fatalf("committed record not reclaimed: %d records", c)
	}
}
