package testcluster

import (
	"context"
	"crypto/rand"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/sthorne/datax/pkg/base"
	"github.com/sthorne/datax/pkg/cluster"
	"github.com/sthorne/datax/pkg/keys"
	"github.com/sthorne/datax/pkg/rpc"
	"github.com/sthorne/datax/pkg/security"
	"github.com/sthorne/datax/pkg/server"
	"github.com/sthorne/datax/pkg/storage/enc"
	"github.com/sthorne/datax/pkg/util/hlc"
)

// TestOnlineRotationStagedKeyRestart: a real-directory node started from
// --enc-key key files rotates its store key online through the admin RPC
// — the branch that swaps the node's own key and re-seals the metadata
// backup — and then restarts. With the new key staged beside the old one
// (`--enc-key old.key,new.key`) the restart opens the store on either
// side of the rotation; with only the old key it is refused; with only
// the new key it opens. The metadata backup comes out sealed under the
// new key. Issues #67 and #68.
func TestOnlineRotationStagedKeyRestart(t *testing.T) {
	newKey := func() []byte {
		k := make([]byte, enc.KeyLen)
		if _, err := rand.Read(k); err != nil {
			t.Fatal(err)
		}
		return k
	}
	key1, key2 := newKey(), newKey()
	keyDir := t.TempDir()
	oldPath, newPath := filepath.Join(keyDir, "old.key"), filepath.Join(keyDir, "new.key")
	for p, k := range map[string][]byte{oldPath: key1, newPath: key2} {
		if err := os.WriteFile(p, k, 0o600); err != nil {
			t.Fatal(err)
		}
	}
	dirs := make([]string, 3)
	for i := range dirs {
		dirs[i] = t.TempDir()
	}
	// Both keys staged from the start: the node must pick the one that
	// matches (the registry is initialized under the first).
	staged := oldPath + "," + newPath
	tc, certsDir := startSecureCluster(t, "topsecret", func(i int, cfg *server.Config) {
		cfg.Dir = dirs[i]
		cfg.EncKeyPath = staged
		cfg.GCInterval = -1
	})
	for _, n := range tc.Nodes {
		tc.addrs = append(tc.addrs, n.Addr())
	}
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()
	if err := security.CreateClientCert(certsDir, "root"); err != nil {
		t.Fatal(err)
	}
	rootTLS, err := security.LoadClientTLS(certsDir, "root")
	if err != nil {
		t.Fatal(err)
	}
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

	prefix := keys.TableDataPrefix(951)
	for i := 0; i < 10; i++ {
		if err := tc.Nodes[0].DB().Put(ctx, append(prefix.Clone(), byte('0'+i)), []byte("v")); err != nil {
			t.Fatal(err)
		}
	}
	// The metadata backup is written each heartbeat; wait for the first.
	bakPath := filepath.Join(dirs[0], server.MetadataBackupFile)
	waitBackup := func(key []byte, wantOK bool) {
		t.Helper()
		deadline := time.Now().Add(30 * time.Second)
		for {
			if raw, err := os.ReadFile(bakPath); err == nil {
				_, uerr := enc.Unseal(server.MetadataBackupMagic, key, raw)
				if (uerr == nil) == wantOK {
					return
				}
			}
			if time.Now().After(deadline) {
				t.Fatalf("metadata backup never reached the expected seal state (want opens with key: %v)", wantOK)
			}
			time.Sleep(200 * time.Millisecond)
		}
	}
	waitBackup(key1, true)

	// Rotate n1 online: registry resealed, node key swapped, backup
	// resealed under the new key right away.
	if resp := call(cluster.AdminRequest{Op: "rotate-store-key", OldStoreKey: key1, NewStoreKey: key2}); resp.Error != "" {
		t.Fatalf("online rotation: %s", resp.Error)
	}
	if raw, err := os.ReadFile(bakPath); err != nil {
		t.Fatal(err)
	} else if _, err := enc.Unseal(server.MetadataBackupMagic, key2, raw); err != nil {
		t.Fatalf("metadata backup not resealed under the new key right after rotation: %v", err)
	} else if _, err := enc.Unseal(server.MetadataBackupMagic, key1, raw); err == nil {
		t.Fatal("metadata backup still opens with the retired key")
	}
	if resp := call(cluster.AdminRequest{Op: "rotate-store-key", OldStoreKey: key1, NewStoreKey: key2}); resp.Error == "" {
		t.Fatal("retired store key still rotates the registry")
	}

	// Restart n1 with the staged list: the new key matches now.
	restart := func(encKeyPath string) (*server.Node, error) {
		t.Helper()
		return tc.RestartNodeErr(0, nil, func(cfg *server.Config) {
			cfg.Dir = dirs[0]
			cfg.CertsDir = certsDir
			cfg.EncKeyPath = encKeyPath
		})
	}
	tc.StopNode(0)
	if _, err := restart(oldPath); err == nil || !strings.Contains(err.Error(), "does not match") {
		t.Fatalf("restart with only the retired key: %v, want a key mismatch", err)
	}
	if _, err := restart(oldPath + "," + "/nonexistent.key"); err == nil || !strings.Contains(err.Error(), "loading encryption key") {
		t.Fatalf("restart with an unreadable staged key: %v, want a load error", err)
	}
	if _, err := restart(staged); err != nil {
		t.Fatalf("restart with the staged key list: %v", err)
	}
	rows, err := tc.Nodes[0].DB().Scan(ctx, prefix, prefix.PrefixEnd(), 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 10 {
		t.Fatalf("after restart: %d rows, want 10", len(rows))
	}
	waitBackup(key2, true)

	// And with the old file retired, the new key alone opens the store.
	tc.StopNode(0)
	if _, err := restart(newPath); err != nil {
		t.Fatalf("restart with the new key only: %v", err)
	}
	if rows, err := tc.Nodes[0].DB().Scan(ctx, prefix, prefix.PrefixEnd(), 0); err != nil || len(rows) != 10 {
		t.Fatalf("after the final restart: %d rows, %v", len(rows), err)
	}
}
