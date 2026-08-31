package testcluster

import (
	"context"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"

	"github.com/sthorne/datax/pkg/keys"
	"github.com/sthorne/datax/pkg/kvclient"
	"github.com/sthorne/datax/pkg/kvserver"
	"github.com/sthorne/datax/pkg/metrics"
	"github.com/sthorne/datax/pkg/server"
)

// TestBackpressureShedsTableWrites: while the engine reports overload,
// table-data writes are shed with a retryable storage-overloaded error and
// the client retries with backoff until the pressure clears; /system
// writes are never gated.
func TestBackpressureShedsTableWrites(t *testing.T) {
	var overloaded atomic.Bool
	var rejections atomic.Int64
	knobs := kvserver.TestingKnobs{
		OverrideOverloaded: func() (bool, string) {
			if overloaded.Load() {
				rejections.Add(1)
				return true, "test overload"
			}
			return false, ""
		},
	}
	tc, _ := StartWithEngines(t, 3, func(c *server.Config) { c.TestingKnobs = knobs })
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	db := tc.Nodes[0].DB()
	tableKey := append(keys.TableDataPrefix(940), "k"...)

	// Healthy: writes flow.
	if err := db.Put(ctx, tableKey, []byte("v0")); err != nil {
		t.Fatal(err)
	}

	// Permanently overloaded: a table write cannot land and times out with
	// the overload error, while a /system write sails through.
	overloaded.Store(true)
	shortCtx, shortCancel := context.WithTimeout(ctx, 2*time.Second)
	err := db.Put(shortCtx, tableKey, []byte("v1"))
	shortCancel()
	if err == nil || !strings.Contains(err.Error(), "storage overloaded") {
		t.Fatalf("gated write error = %v, want storage overloaded", err)
	}
	if err := db.Put(ctx, keys.UserKey("bp-test"), []byte("x")); err != nil {
		t.Fatalf("/system write gated under overload: %v", err)
	}
	if rejections.Load() == 0 {
		t.Fatal("knob never consulted")
	}
	if testutil.ToFloat64(metrics.StorageBackpressure) == 0 {
		t.Fatal("backpressure counter did not move")
	}

	// Transient overload: the client backs off and the SAME write (and a
	// full transaction) succeed once pressure clears, without a txn restart
	// surfacing to the caller.
	overloaded.Store(false)
	rejections.Store(0)
	overloaded.Store(true)
	go func() {
		time.Sleep(300 * time.Millisecond)
		overloaded.Store(false)
	}()
	start := time.Now()
	if err := db.Put(ctx, tableKey, []byte("v2")); err != nil {
		t.Fatalf("write after pressure cleared: %v", err)
	}
	if elapsed := time.Since(start); elapsed < 250*time.Millisecond {
		t.Fatalf("write returned in %s — backoff did not wait out the overload", elapsed)
	}
	if v, err := db.Get(ctx, tableKey); err != nil || string(v) != "v2" {
		t.Fatalf("read back: %q %v", v, err)
	}

	overloaded.Store(true)
	go func() {
		time.Sleep(300 * time.Millisecond)
		overloaded.Store(false)
	}()
	err = db.RunTxn(ctx, "bp", func(ctx context.Context, txn *kvclient.Txn) error {
		return txn.Put(ctx, append(keys.TableDataPrefix(940), "t"...), []byte("txn"))
	})
	if err != nil {
		t.Fatalf("transactional write under transient overload: %v", err)
	}
}
