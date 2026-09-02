package testcluster

import (
	"context"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"

	"github.com/sthorne/datax/pkg/keys"
	"github.com/sthorne/datax/pkg/metrics"
	"github.com/sthorne/datax/pkg/server"
)

// TestFollowerBackpressureSheds: when a FOLLOWER's storage reports
// overload, its verdict rides raft envelopes to its peers and leaders shed
// table-data writes for ranges the sick node holds a replica of — with
// cause "follower" — instead of letting it lag raft without bound.
// /system writes keep flowing, and the gate reopens promptly once the
// follower advertises healthy again. Issue #47.
func TestFollowerBackpressureSheds(t *testing.T) {
	var n3overloaded atomic.Bool
	tc, _ := StartWithEngines(t, 3, func(c *server.Config) {
		if c.StaticBootstrap.NodeID == 3 {
			c.TestingKnobs.OverrideOverloaded = func() (bool, string) {
				if n3overloaded.Load() {
					return true, "test follower overload"
				}
				return false, ""
			}
		}
	})
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()
	db := tc.Nodes[0].DB()
	tableKey := append(keys.TableDataPrefix(941), "k"...)

	// Healthy cluster: table writes flow.
	if err := db.Put(ctx, tableKey, []byte("v0")); err != nil {
		t.Fatal(err)
	}

	// Pin range leadership away from n3, so the shed exercises the
	// FOLLOWER path (n3 leading would trip its own leader-local gate).
	if err := db.AdminTransferLease(ctx, tableKey, tc.Nodes[0].NodeID()); err != nil {
		t.Fatal(err)
	}

	// Sicken n3 and wait for its piggybacked verdict to reach a peer's
	// store (raft heartbeat traffic carries it within a tick or two).
	n3overloaded.Store(true)
	deadline := time.Now().Add(30 * time.Second)
	for {
		seen := false
		for i := 0; i < 2; i++ {
			if over, why := tc.Nodes[i].Store().NodeOverloaded(3); over && strings.Contains(why, "test follower overload") {
				seen = true
				break
			}
		}
		if seen {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("n3's overload verdict never reached a peer store")
		}
		time.Sleep(50 * time.Millisecond)
	}

	// The leader (whichever node it is) sheds table-data writes with the
	// follower cause; the client's retry loop absorbs retries until the
	// short deadline expires with the shed error.
	before := testutil.ToFloat64(metrics.StorageBackpressureCause.WithLabelValues("follower"))
	deadline = time.Now().Add(30 * time.Second)
	for {
		shortCtx, shortCancel := context.WithTimeout(ctx, 2*time.Second)
		err := db.Put(shortCtx, tableKey, []byte("v1"))
		shortCancel()
		if err != nil && strings.Contains(err.Error(), "follower n3 overloaded") {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("write never shed with the follower cause (last err: %v)", err)
		}
	}
	if d := testutil.ToFloat64(metrics.StorageBackpressureCause.WithLabelValues("follower")) - before; d < 1 {
		t.Fatalf("follower-cause counter advanced by %v, want >= 1", d)
	}

	// /system writes are never gated — liveness and metadata must keep
	// flowing exactly when a store struggles.
	if err := db.Put(ctx, keys.UserKey("fbp-test"), []byte("x")); err != nil {
		t.Fatalf("/system write gated under follower overload: %v", err)
	}

	// Recovery: the next healthy piggyback overwrites the verdict and the
	// same write lands (the client's backoff rides out the transition).
	n3overloaded.Store(false)
	if err := db.Put(ctx, tableKey, []byte("v2")); err != nil {
		t.Fatalf("write after follower recovered: %v", err)
	}
	if v, err := db.Get(ctx, tableKey); err != nil || string(v) != "v2" {
		t.Fatalf("read back: %q %v", v, err)
	}
}
