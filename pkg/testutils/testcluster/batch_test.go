package testcluster

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/sthorne/datax/pkg/keys"
	"github.com/sthorne/datax/pkg/kvclient"
)

// TestTxnRunBatchAcrossRanges: one buffered write batch spanning a split
// commits atomically — the record-creation flag reaches only the anchor
// range, intents land on both sides, and all-or-nothing holds.
func TestTxnRunBatchAcrossRanges(t *testing.T) {
	tc := Start(t, 3)
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()
	db := tc.Nodes[0].DB()

	key := func(s string) keys.Key { return append(keys.TableDataPrefix(750), s...) }
	if _, err := db.AdminSplit(ctx, key("m")); err != nil {
		t.Fatalf("split: %v", err)
	}

	// Batch with keys on both sides of the split; the first key anchors.
	tx := db.NewTxn("batch")
	var wb kvclient.WriteBatch
	for i := 0; i < 10; i++ {
		wb.Put(key(fmt.Sprintf("a%02d", i)), []byte("left"))
		wb.Put(key(fmt.Sprintf("z%02d", i)), []byte("right"))
	}
	if err := tx.RunBatch(ctx, &wb); err != nil {
		t.Fatalf("RunBatch: %v", err)
	}
	// Uncommitted: invisible to others (an independent read pushes the txn
	// and, finding it live, must not see the values).
	if err := tx.Commit(ctx); err != nil {
		t.Fatalf("commit: %v", err)
	}
	for i := 0; i < 10; i++ {
		if v, err := db.Get(ctx, key(fmt.Sprintf("a%02d", i))); err != nil || string(v) != "left" {
			t.Fatalf("left key %d: %q, %v", i, v, err)
		}
		if v, err := db.Get(ctx, key(fmt.Sprintf("z%02d", i))); err != nil || string(v) != "right" {
			t.Fatalf("right key %d: %q, %v", i, v, err)
		}
	}

	// A batch that rolls back leaves nothing.
	tx2 := db.NewTxn("rollback")
	var wb2 kvclient.WriteBatch
	wb2.Put(key("r-left"), []byte("x"))
	wb2.Put(key("zz-right"), []byte("x"))
	if err := tx2.RunBatch(ctx, &wb2); err != nil {
		t.Fatal(err)
	}
	if err := tx2.Rollback(ctx); err != nil {
		t.Fatal(err)
	}
	if v, err := db.Get(ctx, key("r-left")); err != nil || v != nil {
		t.Fatalf("rolled-back left key visible: %q, %v", v, err)
	}
	if v, err := db.Get(ctx, key("zz-right")); err != nil || v != nil {
		t.Fatalf("rolled-back right key visible: %q, %v", v, err)
	}
}

// TestTxnBatchInterleavedRangesParallel: a batch whose keys interleave
// across four ranges (r0,r1,r2,r3,r0,...) goes through the parallel
// per-range fan-out; every response and value must land at its original
// batch position — the positional-reassembly guarantee — including
// deletes, and including under pipelined (parallel) commit.
func TestTxnBatchInterleavedRangesParallel(t *testing.T) {
	tc := Start(t, 3)
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()
	db := tc.Nodes[0].DB()

	key := func(s string) keys.Key { return append(keys.TableDataPrefix(751), s...) }
	for _, s := range []string{"g", "n", "t"} { // 4 ranges: [..g) [g,n) [n,t) [t..)
		if _, err := db.AdminSplit(ctx, key(s)); err != nil {
			t.Fatalf("split at %s: %v", s, err)
		}
	}
	shards := []string{"a", "h", "p", "w"} // one prefix per range

	// Pre-existing keys the batch will delete, one per range.
	for _, s := range shards {
		if err := db.Put(ctx, key(s+"-dead"), []byte("old")); err != nil {
			t.Fatal(err)
		}
	}

	// Non-pipelined: RunBatch flushes through the fan-out synchronously.
	tx := db.NewTxn("interleaved")
	var wb kvclient.WriteBatch
	for i := 0; i < 16; i++ {
		s := shards[i%4]
		wb.Put(key(fmt.Sprintf("%s-k%02d", s, i)), []byte(fmt.Sprintf("v%02d", i)))
	}
	for _, s := range shards {
		wb.Delete(key(s + "-dead"))
	}
	if err := tx.RunBatch(ctx, &wb); err != nil {
		t.Fatalf("RunBatch: %v", err)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatalf("commit: %v", err)
	}
	for i := 0; i < 16; i++ {
		s := shards[i%4]
		want := fmt.Sprintf("v%02d", i)
		if v, err := db.Get(ctx, key(fmt.Sprintf("%s-k%02d", s, i))); err != nil || string(v) != want {
			t.Fatalf("key %d: got %q, %v; want %q", i, v, err, want)
		}
	}
	for _, s := range shards {
		if v, err := db.Get(ctx, key(s+"-dead")); err != nil || v != nil {
			t.Fatalf("deleted key %s-dead still visible: %q, %v", s, v, err)
		}
	}

	// Pipelined: the same shape deferred to a parallel commit.
	err := db.RunTxn(ctx, "interleaved-pc", func(ctx context.Context, txn *kvclient.Txn) error {
		var wb kvclient.WriteBatch
		for i := 0; i < 16; i++ {
			s := shards[i%4]
			wb.Put(key(fmt.Sprintf("%s-p%02d", s, i)), []byte(fmt.Sprintf("w%02d", i)))
		}
		return txn.RunBatch(ctx, &wb)
	})
	if err != nil {
		t.Fatalf("pipelined commit: %v", err)
	}
	deadline := time.Now().Add(10 * time.Second)
	for i := 0; i < 16; i++ {
		s := shards[i%4]
		want := fmt.Sprintf("w%02d", i)
		for {
			v, err := db.Get(ctx, key(fmt.Sprintf("%s-p%02d", s, i)))
			if err == nil && string(v) == want {
				break
			}
			if time.Now().After(deadline) {
				t.Fatalf("pipelined key %d: got %q, %v; want %q", i, v, err, want)
			}
			time.Sleep(20 * time.Millisecond)
		}
	}
}

// TestStaleTxnParallelCommitNotPoisoned: a pipelined transaction older
// than the poison-guard age commits through the sequential
// record-then-writes ordering, so a pusher can never observe an intent
// before the record exists and poison it ABORTED (the rec==nil push
// path). With concurrent readers hammering the keys through commit, the
// transaction must remain atomic — and absent contention it must commit.
func TestStaleTxnParallelCommitNotPoisoned(t *testing.T) {
	tc := Start(t, 3)
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()
	db := tc.Nodes[0].DB()
	key := func(s string) keys.Key { return append(keys.TableDataPrefix(752), s...) }
	tc.LeaderIndex(1)
	if err := db.Put(ctx, key("warmup"), []byte("w")); err != nil {
		t.Fatal(err)
	}

	// Age a pipelined transaction past TxnExpiration (5s) with its writes
	// still deferred, then commit with readers racing.
	tx := db.NewTxn("stale")
	tx.EnablePipelining()
	var wb kvclient.WriteBatch
	wb.Put(key("s1"), []byte("v1"))
	wb.Put(key("s2"), []byte("v2"))
	if err := tx.RunBatch(ctx, &wb); err != nil {
		t.Fatal(err)
	}
	time.Sleep(6 * time.Second)

	var stop atomic.Bool
	var wg sync.WaitGroup
	for w := 0; w < 4; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for !stop.Load() {
				_, _ = db.Get(ctx, key("s1"))
				_, _ = db.Get(ctx, key("s2"))
			}
		}()
	}
	err := tx.Commit(ctx)
	stop.Store(true)
	wg.Wait()

	// Atomicity: both values or neither, never a torn write.
	deadline := time.Now().Add(10 * time.Second)
	for {
		v1, e1 := db.Get(ctx, key("s1"))
		v2, e2 := db.Get(ctx, key("s2"))
		if e1 == nil && e2 == nil {
			committed := string(v1) == "v1" && string(v2) == "v2"
			aborted := v1 == nil && v2 == nil
			if committed || aborted {
				if err == nil && !committed {
					t.Fatalf("commit reported success but values absent")
				}
				if err != nil && committed {
					// Ambiguity the protocol never allows on this path.
					t.Fatalf("commit reported %v but values visible", err)
				}
				break
			}
		}
		if time.Now().After(deadline) {
			t.Fatalf("torn state: s1=%q(%v) s2=%q(%v), commit err %v", v1, e1, v2, e2, err)
		}
		time.Sleep(20 * time.Millisecond)
	}

	// Absent contention, a stale transaction's guarded commit succeeds.
	tx2 := db.NewTxn("stale-quiet")
	tx2.EnablePipelining()
	var wb2 kvclient.WriteBatch
	wb2.Put(key("q1"), []byte("q1"))
	wb2.Put(key("q2"), []byte("q2"))
	if err := tx2.RunBatch(ctx, &wb2); err != nil {
		t.Fatal(err)
	}
	time.Sleep(6 * time.Second)
	if err := tx2.Commit(ctx); err != nil {
		t.Fatalf("quiet stale commit: %v", err)
	}
}
