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
