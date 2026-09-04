package testcluster

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/sthorne/datax/pkg/keys"
)

// TestMetaLookupWaitsForAddressingRepair: a /meta scan that lands in the
// window of a split or merge repair — the record covering a key gone, the
// neighbour's record not covering it — is retried until the repair lands,
// instead of failing the batch with "stale addressing" (issue #111). A
// repair that never comes still fails, within the lookup's bound and with
// the reason.
func TestMetaLookupWaitsForAddressingRepair(t *testing.T) {
	tc := Start(t, 3)
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	db := tc.Nodes[0].DB()
	prefix := keys.TableDataPrefix(859)
	keyA := append(prefix.Clone(), "a"...)
	if err := db.Put(ctx, keyA, []byte("v")); err != nil {
		t.Fatal(err)
	}
	// Split the table off range 1 first: the cache keeps a fallback entry
	// for the meta range, so a key inside it would never reach /meta.
	if _, err := db.AdminSplit(ctx, prefix); err != nil {
		t.Fatal(err)
	}
	sr, err := db.AdminSplit(ctx, append(prefix.Clone(), "m"...))
	if err != nil {
		t.Fatal(err)
	}
	leftMeta := keys.RangeMetaKey(sr.Left.EndKey)
	record, err := db.Get(ctx, leftMeta)
	if err != nil || record == nil {
		t.Fatalf("left record after split: %v (%v)", record, err)
	}

	// Open the window: the LHS record is gone (as after a merge's delete,
	// before the merged record is written) and this gateway's routing
	// entry for it is evicted, so the next request must consult /meta —
	// where the first record beyond keyA is the RHS's, which does not
	// cover it.
	if err := db.Delete(ctx, leftMeta); err != nil {
		t.Fatal(err)
	}
	db.EvictDescriptor(sr.Left.RangeID)
	result := make(chan error, 1)
	go func() {
		_, err := db.Get(ctx, keyA)
		result <- err
	}()
	select {
	case err := <-result:
		t.Fatalf("lookup returned during the repair window: %v", err)
	case <-time.After(400 * time.Millisecond):
	}
	// The repair lands; the waiting lookup completes.
	if err := db.Put(ctx, leftMeta, record); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-result:
		if err != nil {
			t.Fatalf("lookup after the repair: %v", err)
		}
	case <-time.After(15 * time.Second):
		t.Fatal("lookup did not complete after the repair landed")
	}

	// No repair: the lookup gives up within its bound and says why.
	if err := db.Delete(ctx, leftMeta); err != nil {
		t.Fatal(err)
	}
	db.EvictDescriptor(sr.Left.RangeID)
	start := time.Now()
	_, err = db.Get(ctx, keyA)
	elapsed := time.Since(start)
	if err == nil || !strings.Contains(err.Error(), "stale addressing") || !strings.Contains(err.Error(), "did not converge") {
		t.Fatalf("expected a bounded stale-addressing failure, got %v", err)
	}
	if elapsed < 2*time.Second || elapsed > 20*time.Second {
		t.Fatalf("lookup gave up after %s; expected about the 3s bound", elapsed)
	}
	if err := db.Put(ctx, leftMeta, record); err != nil {
		t.Fatal(err)
	}
}
