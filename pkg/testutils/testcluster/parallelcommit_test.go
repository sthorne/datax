package testcluster

import (
	"bytes"
	"context"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"

	"github.com/sthorne/datax/pkg/keys"
	"github.com/sthorne/datax/pkg/kvclient"
	"github.com/sthorne/datax/pkg/kvpb"
	"github.com/sthorne/datax/pkg/metrics"
	"github.com/sthorne/datax/pkg/storage"
	"github.com/sthorne/datax/pkg/util/hlc"
)

// TestParallelCommitFastPath: a pipelined transaction commits via the
// STAGING fast path (write batch and staged EndTxn in parallel), the data
// is durably visible, and the fast-path counter moves.
func TestParallelCommitFastPath(t *testing.T) {
	tc := Start(t, 3)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	db := tc.Nodes[0].DB()
	prefix := keys.TableDataPrefix(930)
	k1 := append(prefix.Clone(), "a"...)
	k2 := append(prefix.Clone(), "b"...)
	// Settle elections: a leadership change bumps the timestamp-cache
	// floor, which would forward the pipelined writes and (correctly)
	// push the commit off the fast path.
	tc.LeaderIndex(1)
	if err := db.Put(ctx, append(prefix.Clone(), "warmup"...), []byte("w")); err != nil {
		t.Fatal(err)
	}

	before := testutil.ToFloat64(metrics.ParallelCommits)
	err := db.RunTxn(ctx, "pc", func(ctx context.Context, txn *kvclient.Txn) error {
		var wb kvclient.WriteBatch
		wb.Put(k1, []byte("v1"))
		wb.Put(k2, []byte("v2"))
		return txn.RunBatch(ctx, &wb)
	})
	if err != nil {
		t.Fatal(err)
	}
	if after := testutil.ToFloat64(metrics.ParallelCommits); after <= before {
		t.Fatalf("parallel-commit counter did not move (%v -> %v)", before, after)
	}
	// Values durable and readable (recovery or finalize resolves intents).
	deadline := time.Now().Add(10 * time.Second)
	for {
		v1, err1 := db.Get(ctx, k1)
		v2, err2 := db.Get(ctx, k2)
		if err1 == nil && err2 == nil && bytes.Equal(v1, []byte("v1")) && bytes.Equal(v2, []byte("v2")) {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("committed values not readable: %q/%v %q/%v", v1, err1, v2, err2)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// stageAbandonedTxn manually drives the parallel-commit protocol up to the
// STAGING record and then abandons it (a coordinator crash at the worst
// moment). writeSecond controls whether the second in-flight write is
// actually sent.
func stageAbandonedTxn(t *testing.T, db *kvclient.DB, k1, k2 keys.Key, writeSecond bool) *kvpb.Transaction {
	t.Helper()
	ctx := context.Background()
	txn := kvpb.NewTransaction("crashed", 0, db.Clock().Now())
	txn.Key = k1.Clone()
	txn.Sequence = 1

	wba := &kvpb.BatchRequest{Header: kvpb.BatchHeader{Txn: txn.Clone(), CreateTxnRecord: true}}
	wba.Add(&kvpb.PutRequest{RequestHeader: kvpb.RequestHeader{Key: k1.Clone()}, Value: []byte("staged-1")})
	if writeSecond {
		wba.Add(&kvpb.PutRequest{RequestHeader: kvpb.RequestHeader{Key: k2.Clone()}, Value: []byte("staged-2")})
	}
	br, kerr := db.Send(ctx, wba)
	if kerr != nil {
		t.Fatalf("staged writes: %v", kerr)
	}
	if br.Txn != nil && txn.WriteTimestamp.Less(br.Txn.WriteTimestamp) {
		// A leadership-change floor bump forwarded the writes: the staged
		// commit we are constructing would be genuinely invalid. The
		// scenario needs unforwarded writes — fail loudly (the callers
		// settle elections first, so this should not happen).
		t.Fatalf("staged writes forwarded from %s to %s", txn.WriteTimestamp, br.Txn.WriteTimestamp)
	}
	eba := &kvpb.BatchRequest{Header: kvpb.BatchHeader{Txn: txn.Clone()}}
	eba.Add(&kvpb.EndTxnRequest{
		RequestHeader: kvpb.RequestHeader{Key: k1.Clone()},
		Commit:        true,
		IntentKeys:    []keys.Key{k1.Clone(), k2.Clone()},
		InFlight:      []keys.Key{k1.Clone(), k2.Clone()},
	})
	if _, kerr := db.Send(ctx, eba); kerr != nil {
		t.Fatalf("staging EndTxn: %v", kerr)
	}
	return txn
}

// TestStagingRecoveryCommits: coordinator dies after staging with ALL
// in-flight writes applied — the transaction is implicitly committed, and
// the first reader's status recovery finalizes it as COMMITTED.
func TestStagingRecoveryCommits(t *testing.T) {
	tc := Start(t, 3)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	db := tc.Nodes[0].DB()
	prefix := keys.TableDataPrefix(931)
	k1 := append(prefix.Clone(), "a"...)
	k2 := append(prefix.Clone(), "b"...)
	tc.LeaderIndex(1)
	if err := db.Put(ctx, append(prefix.Clone(), "warmup"...), []byte("w")); err != nil {
		t.Fatal(err)
	}
	stageAbandonedTxn(t, db, k1, k2, true)

	reader := db.NewTxn("reader")
	v, err := reader.Get(ctx, k1)
	if err != nil || !bytes.Equal(v, []byte("staged-1")) {
		t.Fatalf("recovered read k1 = %q, %v; want staged-1", v, err)
	}
	v, err = reader.Get(ctx, k2)
	if err != nil || !bytes.Equal(v, []byte("staged-2")) {
		t.Fatalf("recovered read k2 = %q, %v; want staged-2", v, err)
	}
	_ = reader.Rollback(ctx)
}

// TestStagingRecoveryAborts: coordinator dies after staging with an
// in-flight write MISSING — recovery must abort, and the prevention read
// guarantees the straggler can never make the staged commit true later.
func TestStagingRecoveryAborts(t *testing.T) {
	tc := Start(t, 3)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	db := tc.Nodes[0].DB()
	prefix := keys.TableDataPrefix(932)
	k1 := append(prefix.Clone(), "a"...)
	k2 := append(prefix.Clone(), "b"...)
	tc.LeaderIndex(1)
	if err := db.Put(ctx, append(prefix.Clone(), "warmup"...), []byte("w")); err != nil {
		t.Fatal(err)
	}
	txn := stageAbandonedTxn(t, db, k1, k2, false /* k2 write never sent */)

	reader := db.NewTxn("reader")
	if v, err := reader.Get(ctx, k1); err != nil || v != nil {
		t.Fatalf("read of aborted staged write = %q, %v; want absent", v, err)
	}
	_ = reader.Rollback(ctx)

	// The straggler arrives late: it must not resurrect the staged commit.
	// The prevention read forces it above the staged timestamp, and the
	// record is already ABORTED, so the write is refused outright.
	lba := &kvpb.BatchRequest{Header: kvpb.BatchHeader{Txn: txn.Clone()}}
	lba.Add(&kvpb.PutRequest{RequestHeader: kvpb.RequestHeader{Key: k2.Clone()}, Value: []byte("late")})
	br, kerr := db.Send(ctx, lba)
	if kerr == nil && br.Txn != nil && !txn.WriteTimestamp.Less(br.Txn.WriteTimestamp) {
		t.Fatalf("straggler landed at or below the staged timestamp %s", txn.WriteTimestamp)
	}

	// Whatever the straggler did, no committed value may ever appear.
	reader2 := db.NewTxn("reader2")
	if v, err := reader2.Get(ctx, k2); err != nil || v != nil {
		t.Fatalf("straggler produced a visible value: %q, %v", v, err)
	}
	_ = reader2.Rollback(ctx)
	if v, err := reader2.Get(ctx, k1); err != nil || v != nil {
		t.Fatalf("aborted staged write resurfaced: %q, %v", v, err)
	}
}

// TestMultiRangePushCommitsAbovePushedWrite: a transaction's write batch
// spans two ranges, and a read bumps only the FIRST range's timestamp
// cache above the transaction's timestamp — so only that sub-batch's
// write is pushed. The batch response must report the MAXIMUM write
// timestamp across sub-batches: if the un-pushed second group's response
// overwrote it, the parallel commit would stage, commit, and resolve at
// the original timestamp, physically moving the pushed intent's version
// back DOWN beneath the timestamp already served to the reader — a
// serializability violation (regression: the re-shard dual-write phantom).
func TestMultiRangePushCommitsAbovePushedWrite(t *testing.T) {
	n, eng := startGCNode(t)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	db := n.DB()
	prefix := keys.TableDataPrefix(933)
	k1 := append(prefix.Clone(), "a"...)
	k2 := append(prefix.Clone(), "m"...)
	if _, err := db.AdminSplit(ctx, k2); err != nil {
		t.Fatal(err)
	}
	if err := db.Put(ctx, append(prefix.Clone(), "a-warm"...), []byte("w")); err != nil {
		t.Fatal(err)
	}
	if err := db.Put(ctx, append(prefix.Clone(), "m-warm"...), []byte("w")); err != nil {
		t.Fatal(err)
	}

	var scanTS hlc.Timestamp
	err := db.RunTxn(ctx, "mr-push", func(ctx context.Context, txn *kvclient.Txn) error {
		var wb kvclient.WriteBatch
		wb.Put(k1, []byte("v1"))
		wb.Put(k2, []byte("v2"))
		if err := txn.RunBatch(ctx, &wb); err != nil { // deferred: no intents yet
			return err
		}
		// Bump k1's range's timestamp cache above the transaction's
		// timestamp; k2's range stays untouched. At commit, the k1 write is
		// pushed and the k2 write is not.
		scanTS = db.Clock().Now()
		_, err := db.Scan(ctx, k1, k1.Next(), 0)
		return err
	})
	if err != nil {
		t.Fatal(err)
	}

	// Both committed versions must land at ONE timestamp, above the served
	// read. Wait out intent resolution first (the buggy path finalized
	// asynchronously).
	deadline := time.Now().Add(10 * time.Second)
	for {
		m1, _ := mvccEntryCounts(t, eng, k1)
		m2, _ := mvccEntryCounts(t, eng, k2)
		if m1 == 0 && m2 == 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("intents not resolved: %d/%d metas remain", m1, m2)
		}
		time.Sleep(20 * time.Millisecond)
	}
	ts1 := newestVersionTS(t, eng, k1)
	ts2 := newestVersionTS(t, eng, k2)
	if !ts1.Equal(ts2) {
		t.Fatalf("atomic commit split across timestamps: k1 at %s, k2 at %s", ts1, ts2)
	}
	if !scanTS.Less(ts1) {
		t.Fatalf("commit at %s did not clear the served read at %s (write slid beneath a reader)", ts1, scanTS)
	}
}

// newestVersionTS returns the timestamp of the newest committed version of
// user key k, by raw engine iteration.
func newestVersionTS(t *testing.T, eng *storage.Engine, k keys.Key) hlc.Timestamp {
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
		if !ts.IsEmpty() {
			return ts // versions sort newest-first
		}
	}
	t.Fatalf("no committed version of %x", k)
	return hlc.Timestamp{}
}
