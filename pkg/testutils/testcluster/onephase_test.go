package testcluster

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/prometheus/client_golang/prometheus/testutil"

	"github.com/sthorne/datax/pkg/keys"
	"github.com/sthorne/datax/pkg/kvclient"
	"github.com/sthorne/datax/pkg/metrics"
	"github.com/sthorne/datax/pkg/storage"
	"github.com/sthorne/datax/pkg/util/hlc"
)

// The one-phase commit suite (issue #50): a pipelined transaction whose
// entire write set lands on one range commits in a single raft proposal —
// committed values, no record, no intents — with clean fallbacks whenever
// the preconditions do not hold.

// txnRecordSpan is the engine span holding any transaction record anchored
// at key.
func txnRecordSpan(anchor keys.Key) (keys.Key, keys.Key) {
	p := keys.TransactionKey(anchor, uuid.UUID{})
	p = p[:len(p)-16] // trim the uuid: all records anchored at this key
	return p, p.PrefixEnd()
}

func assertNoTxnState(t *testing.T, eng *storage.Engine, ks ...keys.Key) {
	t.Helper()
	for _, k := range ks {
		if raw, err := eng.Get(storage.EncodeMVCCKey(k, hlc.Timestamp{})); err != nil || raw != nil {
			t.Fatalf("intent metadata present at %s: %q, %v", k, raw, err)
		}
		lo, hi := txnRecordSpan(k)
		it := eng.NewIter(lo, hi)
		if it.SeekGE(lo) {
			t.Fatalf("transaction record present under %s: %q", k, it.Key())
		}
		if err := it.Close(); err != nil {
			t.Fatal(err)
		}
	}
}

func TestOnePhaseCommit(t *testing.T) {
	tc, engines := StartWithEngines(t, 3)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	db := tc.Nodes[0].DB()
	key := func(s string) keys.Key { return append(keys.TableDataPrefix(940), s...) }
	tc.LeaderIndex(1)
	if err := db.Put(ctx, key("warmup"), []byte("w")); err != nil {
		t.Fatal(err)
	}

	before := testutil.ToFloat64(metrics.OnePhaseCommits)
	err := db.RunTxn(ctx, "1pc", func(ctx context.Context, txn *kvclient.Txn) error {
		var wb kvclient.WriteBatch
		wb.Put(key("a"), []byte("v1"))
		wb.Put(key("b"), []byte("v2"))
		wb.Delete(key("warmup"))
		return txn.RunBatch(ctx, &wb)
	})
	if err != nil {
		t.Fatal(err)
	}
	if after := testutil.ToFloat64(metrics.OnePhaseCommits); after <= before {
		t.Fatalf("one-phase counter did not move (%v -> %v)", before, after)
	}
	if v, err := db.Get(ctx, key("a")); err != nil || string(v) != "v1" {
		t.Fatalf("a: %q, %v", v, err)
	}
	if v, err := db.Get(ctx, key("b")); err != nil || string(v) != "v2" {
		t.Fatalf("b: %q, %v", v, err)
	}
	if v, err := db.Get(ctx, key("warmup")); err != nil || v != nil {
		t.Fatalf("deleted warmup visible: %q, %v", v, err)
	}
	// The raw engine holds committed versions ONLY: no intent metadata, no
	// transaction record (checked on a replica that applied the proposal).
	li := tc.LeaderIndex(1)
	assertNoTxnState(t, engines[li], key("a"), key("b"), key("warmup"))
}

func TestOnePhaseCommitPushedFallsBackToRetry(t *testing.T) {
	tc := Start(t, 3)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	db := tc.Nodes[0].DB()
	key := func(s string) keys.Key { return append(keys.TableDataPrefix(941), s...) }
	tc.LeaderIndex(1)
	if err := db.Put(ctx, key("warmup"), []byte("w")); err != nil {
		t.Fatal(err)
	}

	before := testutil.ToFloat64(metrics.OnePhaseCommits)
	err := db.RunTxn(ctx, "1pc-pushed", func(ctx context.Context, txn *kvclient.Txn) error {
		var wb kvclient.WriteBatch
		wb.Put(key("p"), []byte("mine"))
		if err := txn.RunBatch(ctx, &wb); err != nil {
			return err
		}
		// A read AFTER the transaction's timestamp bumps the timestamp
		// cache above it: the server must reject the 1PC pre-proposal with
		// a retryable error, and the client's refresh (no read spans)
		// re-stamps and resends.
		if _, err := db.Get(ctx, key("p")); err != nil {
			return err
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if after := testutil.ToFloat64(metrics.OnePhaseCommits); after <= before {
		t.Fatalf("pushed 1PC did not retry into the fast path (%v -> %v)", before, after)
	}
	if v, err := db.Get(ctx, key("p")); err != nil || string(v) != "mine" {
		t.Fatalf("p: %q, %v", v, err)
	}
}

// TestOnePhaseCommitReadProbeSerializable: the uniqueness-probe shape —
// read a key, then write it in the same transaction. A conflicting write
// landing between probe and commit must fail the commit (refresh finds the
// read span invalidated), never commit above the unvalidated read.
func TestOnePhaseCommitReadProbeSerializable(t *testing.T) {
	tc := Start(t, 3)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	db := tc.Nodes[0].DB()
	key := func(s string) keys.Key { return append(keys.TableDataPrefix(942), s...) }
	tc.LeaderIndex(1)

	txn := db.NewTxn("probe")
	txn.EnablePipelining()
	if v, err := txn.Get(ctx, key("u")); err != nil || v != nil {
		t.Fatalf("probe: %q, %v", v, err)
	}
	// Conflicting committed write after the probe.
	if err := db.Put(ctx, key("u"), []byte("theirs")); err != nil {
		t.Fatal(err)
	}
	var wb kvclient.WriteBatch
	wb.Put(key("u"), []byte("mine"))
	if err := txn.RunBatch(ctx, &wb); err != nil {
		t.Fatal(err)
	}
	err := txn.Commit(ctx)
	var re *kvclient.RetryableError
	if !errors.As(err, &re) {
		t.Fatalf("probe-invalidated 1PC commit: %v", err)
	}
	if v, gerr := db.Get(ctx, key("u")); gerr != nil || string(v) != "theirs" {
		t.Fatalf("u after failed commit: %q, %v", v, gerr)
	}
}

func TestOnePhaseCommitMultiRangeFallsBack(t *testing.T) {
	tc := Start(t, 3)
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()
	db := tc.Nodes[0].DB()
	key := func(s string) keys.Key { return append(keys.TableDataPrefix(943), s...) }
	if _, err := db.AdminSplit(ctx, key("m")); err != nil {
		t.Fatal(err)
	}
	tc.LeaderIndex(1)

	beforeOne := testutil.ToFloat64(metrics.OnePhaseCommits)
	beforePar := testutil.ToFloat64(metrics.ParallelCommits)
	err := db.RunTxn(ctx, "multi", func(ctx context.Context, txn *kvclient.Txn) error {
		var wb kvclient.WriteBatch
		wb.Put(key("a"), []byte("left"))
		wb.Put(key("z"), []byte("right"))
		return txn.RunBatch(ctx, &wb)
	})
	if err != nil {
		t.Fatal(err)
	}
	if after := testutil.ToFloat64(metrics.OnePhaseCommits); after != beforeOne {
		t.Fatalf("multi-range commit took the 1PC path (%v -> %v)", beforeOne, after)
	}
	if after := testutil.ToFloat64(metrics.ParallelCommits); after <= beforePar {
		t.Fatalf("multi-range commit missed the parallel path (%v -> %v)", beforePar, after)
	}
	// The parallel commit resolves its intents asynchronously.
	deadline := time.Now().Add(10 * time.Second)
	for _, kv := range []struct{ k, want string }{{"a", "left"}, {"z", "right"}} {
		for {
			v, err := db.Get(ctx, key(kv.k))
			if err == nil && string(v) == kv.want {
				break
			}
			if time.Now().After(deadline) {
				t.Fatalf("%s: %q, %v", kv.k, v, err)
			}
			time.Sleep(20 * time.Millisecond)
		}
	}
}

// TestOnePhaseCommitSplitRaceFallsBack: a split lands after the client's
// cache warmed, so the pinned-descriptor send bounces (RangeKeyMismatch)
// and the commit falls back cleanly — nothing applied twice, nothing lost.
func TestOnePhaseCommitSplitRaceFallsBack(t *testing.T) {
	tc := Start(t, 3)
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()
	db := tc.Nodes[0].DB()
	key := func(s string) keys.Key { return append(keys.TableDataPrefix(944), s...) }
	tc.LeaderIndex(1)
	if err := db.Put(ctx, key("warmup"), []byte("w")); err != nil {
		t.Fatal(err)
	}

	txn := db.NewTxn("split-race")
	txn.EnablePipelining()
	var wb kvclient.WriteBatch
	wb.Put(key("a"), []byte("v1"))
	wb.Put(key("z"), []byte("v2"))
	if err := txn.RunBatch(ctx, &wb); err != nil {
		t.Fatal(err)
	}
	// Split between the keys through ANOTHER gateway, so the committing
	// client's cache stays stale.
	if _, err := tc.Nodes[1].DB().AdminSplit(ctx, key("m")); err != nil {
		t.Fatal(err)
	}
	if err := txn.Commit(ctx); err != nil {
		t.Fatalf("commit across surprise split: %v", err)
	}
	// The fallback parallel commit resolves its intents asynchronously.
	deadline := time.Now().Add(10 * time.Second)
	for _, kv := range []struct{ k, want string }{{"a", "v1"}, {"z", "v2"}} {
		for {
			v, err := db.Get(ctx, key(kv.k))
			if err == nil && string(v) == kv.want {
				break
			}
			if time.Now().After(deadline) {
				t.Fatalf("%s: %q, %v", kv.k, v, err)
			}
			time.Sleep(20 * time.Millisecond)
		}
	}
}

// TestOnePhaseCommitForeignIntent: a 1PC batch tripping over an abandoned
// transaction's intent pushes it (expiry) and then commits.
func TestOnePhaseCommitForeignIntent(t *testing.T) {
	tc := Start(t, 3)
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()
	db := tc.Nodes[0].DB()
	key := func(s string) keys.Key { return append(keys.TableDataPrefix(945), s...) }
	tc.LeaderIndex(1)

	blocker := db.NewTxn("blocker")
	if err := blocker.Put(ctx, key("c"), []byte("stale")); err != nil {
		t.Fatal(err)
	}
	blocker.AbandonForTesting() // heartbeats stop; expires after ~5s

	before := testutil.ToFloat64(metrics.OnePhaseCommits)
	err := db.RunTxn(ctx, "1pc-push", func(ctx context.Context, txn *kvclient.Txn) error {
		var wb kvclient.WriteBatch
		wb.Put(key("c"), []byte("mine"))
		return txn.RunBatch(ctx, &wb)
	})
	if err != nil {
		t.Fatal(err)
	}
	if after := testutil.ToFloat64(metrics.OnePhaseCommits); after <= before {
		t.Fatalf("intent-pushing 1PC missed the fast path (%v -> %v)", before, after)
	}
	if v, err := db.Get(ctx, key("c")); err != nil || string(v) != "mine" {
		t.Fatalf("c: %q, %v", v, err)
	}
}

// TestOnePhaseCommitAbandonedClientLeavesNothing: an abandoned/failed 1PC
// attempt strands no state — no record to recover, no intents to push.
func TestOnePhaseCommitAbandonedClientLeavesNothing(t *testing.T) {
	tc, engines := StartWithEngines(t, 3)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	db := tc.Nodes[0].DB()
	key := func(s string) keys.Key { return append(keys.TableDataPrefix(946), s...) }
	tc.LeaderIndex(1)
	if err := db.Put(ctx, key("warmup"), []byte("w")); err != nil {
		t.Fatal(err)
	}

	txn := db.NewTxn("doomed")
	txn.EnablePipelining()
	var wb kvclient.WriteBatch
	wb.Put(key("d"), []byte("never"))
	if err := txn.RunBatch(ctx, &wb); err != nil {
		t.Fatal(err)
	}
	dead, deadCancel := context.WithCancel(ctx)
	deadCancel() // the client "dies" before its commit can be delivered
	if err := txn.Commit(dead); err == nil {
		t.Fatal("commit on a canceled context succeeded")
	}
	// Whatever raced: either the value committed atomically (the proposal
	// made it) or nothing exists — never an intent or record to clean up.
	time.Sleep(200 * time.Millisecond)
	li := tc.LeaderIndex(1)
	v, err := db.Get(ctx, key("d"))
	if err != nil {
		t.Fatalf("post-abandon read: %v", err)
	}
	if v != nil && string(v) != "never" {
		t.Fatalf("torn value: %q", v)
	}
	assertNoTxnState(t, engines[li], key("d"))
}
