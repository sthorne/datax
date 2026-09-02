package testcluster

import (
	"context"
	"crypto/rand"
	"fmt"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/sthorne/datax/pkg/base"
	"github.com/sthorne/datax/pkg/cluster"
	"github.com/sthorne/datax/pkg/keys"
	"github.com/sthorne/datax/pkg/rpc"
	"github.com/sthorne/datax/pkg/security"
	"github.com/sthorne/datax/pkg/server"
	"github.com/sthorne/datax/pkg/storage"
	"github.com/sthorne/datax/pkg/storage/enc"
	"github.com/sthorne/datax/pkg/util/hlc"
)

// TestOnlineRotationAndReencryptRPC: the rotate-store-key admin op
// re-wraps a LIVE node's key registry under write load with zero write
// errors (wrong old key refused; a second rotation needs the new key),
// and the reencrypt/reencrypt-status ops report the pass. The op carries
// the store keys, so it is served only over mutual TLS: the cluster is
// secure and the caller presents root's client certificate. Issue #48.
func TestOnlineRotationAndReencryptRPC(t *testing.T) {
	newKey := func() []byte {
		k := make([]byte, enc.KeyLen)
		if _, err := rand.Read(k); err != nil {
			t.Fatal(err)
		}
		return k
	}
	key1, key2, key3 := newKey(), newKey(), newKey()
	engines := make([]*storage.Engine, 3)
	for i := range engines {
		eng, err := storage.Open("", storage.Options{EncryptionKey: key1})
		if err != nil {
			t.Fatal(err)
		}
		engines[i] = eng
	}
	t.Cleanup(func() {
		for _, eng := range engines {
			_ = eng.Close()
		}
	})
	tc, certsDir := startSecureCluster(t, "topsecret", func(i int, cfg *server.Config) {
		cfg.Engine = engines[i]
		cfg.GCInterval = -1 // no background housekeeping
	})
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	if err := security.CreateClientCert(certsDir, "root"); err != nil {
		t.Fatal(err)
	}
	rootTLS, err := security.LoadClientTLS(certsDir, "root")
	if err != nil {
		t.Fatal(err)
	}

	// Continuous table-data writes for the whole rotation sequence.
	var writeErrs atomic.Int64
	stop := make(chan struct{})
	done := make(chan struct{})
	go func() {
		defer close(done)
		db := tc.Nodes[1].DB()
		for i := 0; ; i++ {
			select {
			case <-stop:
				return
			default:
			}
			k := append(keys.TableDataPrefix(950), fmt.Sprintf("w-%06d", i)...)
			if err := db.Put(ctx, k, []byte("v")); err != nil {
				writeErrs.Add(1)
				return
			}
		}
	}()

	trans := rpc.NewTransport(hlc.NewClock(nil, base.DefaultMaxClockOffset), nil, nil)
	trans.SetTLS(rootTLS)
	call := func(req cluster.AdminRequest) cluster.AdminResponse {
		t.Helper()
		cctx, ccancel := context.WithTimeout(ctx, 20*time.Second)
		defer ccancel()
		var resp cluster.AdminResponse
		if err := trans.Call(cctx, tc.Nodes[0].Addr(), "admin", req, &resp); err != nil {
			t.Fatalf("admin %s: %v", req.Op, err)
		}
		return resp
	}

	// Wrong old key: refused by GCM authentication.
	if resp := call(cluster.AdminRequest{Op: "rotate-store-key", OldStoreKey: key2, NewStoreKey: key3}); !strings.Contains(resp.Error, "does not match") {
		t.Fatalf("wrong old key: %q", resp.Error)
	}
	// Correct rotation.
	if resp := call(cluster.AdminRequest{Op: "rotate-store-key", OldStoreKey: key1, NewStoreKey: key2}); resp.Error != "" {
		t.Fatalf("live rotation: %s", resp.Error)
	}
	// The reseal landed: the retired key no longer rotates, the new one does.
	if resp := call(cluster.AdminRequest{Op: "rotate-store-key", OldStoreKey: key1, NewStoreKey: key3}); resp.Error == "" {
		t.Fatal("retired store key still accepted")
	}
	if resp := call(cluster.AdminRequest{Op: "rotate-store-key", OldStoreKey: key2, NewStoreKey: key3}); resp.Error != "" {
		t.Fatalf("rotation under the new key: %s", resp.Error)
	}

	// Re-encryption: a fresh store has nothing stale; the op starts (or
	// no-ops) and the status op reports a terminal, attestable state.
	if resp := call(cluster.AdminRequest{Op: "reencrypt"}); resp.Error != "" || resp.Reencryption == nil {
		t.Fatalf("reencrypt: %q (%+v)", resp.Error, resp.Reencryption)
	}
	deadline := time.Now().Add(30 * time.Second)
	for {
		resp := call(cluster.AdminRequest{Op: "reencrypt-status"})
		if resp.Error != "" {
			t.Fatalf("reencrypt-status: %s", resp.Error)
		}
		if st := resp.Reencryption; st != nil && !st.Active && st.RemainingBytes == 0 && st.SweepError == "" {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("re-encryption never reached the attestable terminal state")
		}
		time.Sleep(200 * time.Millisecond)
	}

	close(stop)
	<-done
	if n := writeErrs.Load(); n != 0 {
		t.Fatalf("%d write errors during online rotation", n)
	}
	// And the cluster still serves reads.
	if v, err := tc.Nodes[2].DB().Get(ctx, append(keys.TableDataPrefix(950), "w-000000"...)); err != nil || string(v) != "v" {
		t.Fatalf("read after rotations: %q %v", v, err)
	}
}
