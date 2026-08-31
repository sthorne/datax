package testcluster

import (
	"context"
	"testing"
	"time"

	"github.com/sthorne/datax/pkg/keys"
	"github.com/sthorne/datax/pkg/kvpb"
)

// TestIntervalTSCacheDisjointWrite: the interval timestamp cache pushes only
// writers that overlap a served read. A write at an old timestamp to a key
// nobody read succeeds; the same write to the read key is pushed. Regression
// test for issue #9 (v1's single high-water mark rejected both).
func TestIntervalTSCacheDisjointWrite(t *testing.T) {
	tc, _ := StartWithEngines(t, 1)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	prefix := keys.TableDataPrefix(860)
	readKey := append(prefix.Clone(), "hot"...)
	coldKey := append(prefix.Clone(), "cold"...)
	if err := tc.Nodes[0].DB().Put(ctx, readKey, []byte("v")); err != nil {
		t.Fatal(err)
	}

	oldTS := tc.Nodes[0].Clock().Now()
	// Serve a read of readKey at a newer timestamp: only its span is marked.
	if _, err := tc.Nodes[0].DB().Get(ctx, readKey); err != nil {
		t.Fatal(err)
	}

	rep, ok := tc.Nodes[0].Store().GetReplica(1)
	if !ok {
		t.Fatal("no replica of range 1")
	}

	// A non-transactional write at the pre-read timestamp to an UNREAD key:
	// allowed (v1 rejected it — any read pushed every writer on the range).
	ba := &kvpb.BatchRequest{Header: kvpb.BatchHeader{RangeID: 1, Timestamp: oldTS}}
	ba.Add(&kvpb.PutRequest{RequestHeader: kvpb.RequestHeader{Key: coldKey}, Value: []byte("w")})
	if _, kerr := rep.Execute(ctx, ba); kerr != nil {
		t.Fatalf("disjoint write at old timestamp pushed: %v", kerr)
	}

	// The same write to the READ key must be bounced above the read.
	ba = &kvpb.BatchRequest{Header: kvpb.BatchHeader{RangeID: 1, Timestamp: oldTS}}
	ba.Add(&kvpb.PutRequest{RequestHeader: kvpb.RequestHeader{Key: readKey}, Value: []byte("w")})
	if _, kerr := rep.Execute(ctx, ba); kerr == nil || kerr.TxnRetry == nil {
		t.Fatalf("write beneath the served read not pushed: %v", kerr)
	}
}
