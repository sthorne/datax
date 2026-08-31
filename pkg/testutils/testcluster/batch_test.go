package testcluster

import (
	"context"
	"fmt"
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
