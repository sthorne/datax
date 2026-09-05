package testcluster

import (
	"context"
	"testing"
	"time"

	"github.com/sthorne/datax/pkg/keys"
	"github.com/sthorne/datax/pkg/kvpb"
	"github.com/sthorne/datax/pkg/util/hlc"
)

// TestSplitKeepsServedReadsProtected (issue #134): a read the parent range
// served at T on the right-hand span, inside the closed-timestamp lag, must
// still refuse a write at T on the fresh RHS — the parent's in-memory
// timestamp cache does not travel with the split, so the RHS's floor is
// bumped to now() as the split applies.
func TestSplitKeepsServedReadsProtected(t *testing.T) {
	tc := Start(t, 1)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	n := tc.Nodes[0]
	db, store := n.DB(), n.Store()
	prefix := keys.TableDataPrefix(909)
	k := append(prefix.Clone(), "k"...)
	if err := db.Put(ctx, append(prefix.Clone(), "a"...), []byte("before")); err != nil {
		t.Fatal(err)
	}

	// Read k at T on the parent range.
	T := n.Clock().Now()
	read := &kvpb.BatchRequest{Header: kvpb.BatchHeader{Timestamp: T}}
	read.Add(&kvpb.GetRequest{RequestHeader: kvpb.RequestHeader{Key: k}})
	if _, kerr := store.ExecuteBatch(ctx, read); kerr != nil {
		t.Fatalf("read at T: %v", kerr)
	}

	// Split so k lands on a fresh right-hand range, and write k at T there
	// well inside the closed-timestamp lag.
	sr, err := db.AdminSplit(ctx, prefix)
	if err != nil {
		t.Fatal(err)
	}
	if !sr.Right.ContainsKey(k) {
		t.Fatalf("k not on the RHS %v", sr.Right)
	}
	write := &kvpb.BatchRequest{Header: kvpb.BatchHeader{Timestamp: T}}
	write.Add(&kvpb.PutRequest{RequestHeader: kvpb.RequestHeader{Key: k}, Value: []byte("after-split")})
	_, kerr := store.ExecuteBatch(ctx, write)
	if kerr == nil {
		t.Fatalf("the RHS admitted a write at %s beneath a read the parent served there", T)
	}
	if kerr.TxnRetry == nil && kerr.WriteTooOld == nil {
		t.Fatalf("write at T: %v, want a timestamp push", kerr)
	}

	// Nothing is visible at T: a reader at T sees what it saw before.
	again := &kvpb.BatchRequest{Header: kvpb.BatchHeader{Timestamp: T}}
	again.Add(&kvpb.GetRequest{RequestHeader: kvpb.RequestHeader{Key: k}})
	br, kerr := store.ExecuteBatch(ctx, again)
	if kerr != nil {
		t.Fatal(kerr)
	}
	if v := br.Responses[0].Get.Value; v != nil {
		t.Fatalf("value %q visible at the already-served read timestamp", v)
	}
	// A write above the served read lands.
	if err := db.Put(ctx, k, []byte("later")); err != nil {
		t.Fatal(err)
	}
	if v, err := db.Get(ctx, k); err != nil || string(v) != "later" {
		t.Fatalf("later write: %q, %v", v, err)
	}
	_ = hlc.Timestamp{}
}
