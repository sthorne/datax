package testcluster

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/cockroachdb/pebble/v2/vfs"
	"github.com/sthorne/datax/pkg/base"
	"github.com/sthorne/datax/pkg/cluster"
	"github.com/sthorne/datax/pkg/keys"
	"github.com/sthorne/datax/pkg/rpc"
	"github.com/sthorne/datax/pkg/security"
	"github.com/sthorne/datax/pkg/server"
	"github.com/sthorne/datax/pkg/storage/enc"
	"github.com/sthorne/datax/pkg/util/hlc"
)

// TestStoreKeyUnderARoot (issue #305): a node started with --enc-root
// makes its own store key and keeps it in the data directory only
// wrapped. It restarts through the root, rotates the key online in a
// request that carries no key, moves to another root it was started
// with, and refuses to open with a root that did not wrap its key, with
// a key file, or with both flags at once. The store key appears in no
// file of the data directory at any point.
func TestStoreKeyUnderARoot(t *testing.T) {
	keyDir := t.TempDir()
	writeKey := func(name string) string {
		k := make([]byte, enc.KeyLen)
		if _, err := rand.Read(k); err != nil {
			t.Fatal(err)
		}
		p := filepath.Join(keyDir, name)
		if err := os.WriteFile(p, k, 0o600); err != nil {
			t.Fatal(err)
		}
		return "file:" + p
	}
	rootA, rootB, stranger := writeKey("a.key"), writeKey("b.key"), writeKey("c.key")
	dirs := make([]string, 3)
	for i := range dirs {
		dirs[i] = t.TempDir()
	}
	tc, certsDir := startSecureCluster(t, "topsecret", func(i int, cfg *server.Config) {
		cfg.Dir = dirs[i]
		cfg.EncRoot = rootA
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
	// storeKey is n1's store key as its root unwraps it, read the way an
	// offline tool would.
	storeKey := func(spec string) []byte {
		t.Helper()
		roots, err := enc.ParseRoots(spec)
		if err != nil {
			t.Fatal(err)
		}
		got, err := enc.UnwrapStoreKeys(ctx, vfs.Default, dirs[0], roots)
		if err != nil {
			t.Fatal(err)
		}
		if len(got) != 1 {
			t.Fatalf("the wrapped-key file holds %d keys, want one", len(got))
		}
		return got[0].Key
	}
	// keyOnDisk looks for key, raw and as hex, in every file of n1's
	// data directory.
	keyOnDisk := func(key []byte) string {
		t.Helper()
		var found string
		_ = filepath.Walk(dirs[0], func(p string, info os.FileInfo, err error) error {
			if err != nil || info.IsDir() || found != "" {
				return nil
			}
			raw, rerr := os.ReadFile(p)
			if rerr != nil {
				return nil
			}
			if bytes.Contains(raw, key) || bytes.Contains(raw, []byte(hex.EncodeToString(key))) {
				found = p
			}
			return nil
		})
		return found
	}

	prefix := keys.TableDataPrefix(952)
	for i := 0; i < 10; i++ {
		if err := tc.Nodes[0].DB().Put(ctx, append(prefix.Clone(), byte('0'+i)), []byte("v")); err != nil {
			t.Fatal(err)
		}
	}
	rows := func(what string) {
		t.Helper()
		got, err := tc.Nodes[0].DB().Scan(ctx, prefix, prefix.PrefixEnd(), 0)
		if err != nil || len(got) != 10 {
			t.Fatalf("%s: %d rows, %v; want 10", what, len(got), err)
		}
	}
	key1 := storeKey(rootA)
	if p := keyOnDisk(key1); p != "" {
		t.Fatalf("the store key is in the clear in %s", p)
	}
	if !enc.RegistryExists(vfs.Default, dirs[0]) {
		t.Fatal("the store under a root has no key registry: it is not encrypted")
	}
	// The security document names the root and nothing secret.
	if kind, id := tc.Nodes[0].EncryptionRoot(); kind != "file" || id == "" || strings.Contains(hex.EncodeToString(key1), id) {
		t.Fatalf("the node names its root as %q, key id %q", kind, id)
	}

	// Rotate online. The request is what an operator's tool sends: an op
	// and nothing else.
	req := cluster.AdminRequest{Op: "rotate-store-key"}
	if raw, _ := json.Marshal(req); strings.Contains(string(raw), "key") && strings.Contains(string(raw), "store_key") {
		t.Fatalf("the rotation request carries key material: %s", raw)
	}
	if resp := call(req); resp.Error != "" {
		t.Fatalf("online rotation under a root: %s", resp.Error)
	}
	key2 := storeKey(rootA)
	if bytes.Equal(key1, key2) {
		t.Fatal("the rotation left the store under the same key")
	}
	if _, err := enc.MatchStoreKey(vfs.Default, dirs[0], [][]byte{key1}); err == nil {
		t.Fatal("the retired store key still opens the registry")
	}
	if raw, err := os.ReadFile(filepath.Join(dirs[0], server.MetadataBackupFile)); err == nil {
		if _, err := enc.Unseal(server.MetadataBackupMagic, key2, raw); err != nil {
			t.Fatalf("the metadata backup was not resealed under the new key: %v", err)
		}
	}
	for _, k := range [][]byte{key1, key2} {
		if p := keyOnDisk(k); p != "" {
			t.Fatalf("a store key is in the clear in %s after a rotation", p)
		}
	}
	// A rooted node takes no key from a request, and a rewrap cannot
	// name a root it was not started with.
	if resp := call(cluster.AdminRequest{Op: "rotate-store-key", OldStoreKey: key2, NewStoreKey: key1}); !strings.Contains(resp.Error, "must not carry one") {
		t.Fatalf("a rotation carrying keys to a rooted node: %q", resp.Error)
	}
	if resp := call(cluster.AdminRequest{Op: "rewrap-store-key", Root: 2}); !strings.Contains(resp.Error, "there is no root 2") {
		t.Fatalf("a rewrap under a root the node does not have: %q", resp.Error)
	}
	if resp := call(cluster.AdminRequest{Op: "rewrap-store-key"}); resp.Error != "" {
		t.Fatalf("rewrap under the root in use: %s", resp.Error)
	}
	rows("after rotation and rewrap")

	restart := func(mutate func(*server.Config)) (*server.Node, error) {
		t.Helper()
		return tc.RestartNodeErr(0, nil, func(cfg *server.Config) {
			cfg.Dir = dirs[0]
			cfg.CertsDir = certsDir
			mutate(cfg)
		})
	}
	tc.StopNode(0)
	if _, err := restart(func(cfg *server.Config) { cfg.EncRoot = stranger }); err == nil || !strings.Contains(err.Error(), "no root given matches") {
		t.Fatalf("restart with a root that did not wrap the key: %v", err)
	}
	if _, err := restart(func(cfg *server.Config) {}); err == nil || !strings.Contains(err.Error(), "encrypted") {
		t.Fatalf("restart with no key and no root: %v", err)
	}
	if _, err := restart(func(cfg *server.Config) {
		cfg.EncRoot, cfg.EncKeyPath = rootA, filepath.Join(keyDir, "a.key")
	}); err == nil || !strings.Contains(err.Error(), "alternatives") {
		t.Fatalf("restart with both --enc-key and --enc-root: %v", err)
	}
	// The wrapping key is not the store key: a.key as --enc-key opens
	// nothing.
	if _, err := restart(func(cfg *server.Config) { cfg.EncKeyPath = filepath.Join(keyDir, "a.key") }); err == nil {
		t.Fatal("the root's wrapping key opened the store as a store key")
	}
	// With the root the store is moving to staged beside the one it is
	// under, it opens; then it moves, and opens with the new root alone.
	if _, err := restart(func(cfg *server.Config) { cfg.EncRoot = rootA + "," + rootB }); err != nil {
		t.Fatalf("restart with the root and a staged one: %v", err)
	}
	rows("after a restart through the root")
	if resp := call(cluster.AdminRequest{Op: "rewrap-store-key", Root: 2}); resp.Error != "" {
		t.Fatalf("moving the store to the staged root: %s", resp.Error)
	}
	if got := storeKey(rootB); !bytes.Equal(got, key2) {
		t.Fatal("moving roots changed the store key")
	}
	// A rotation after the move is wrapped by the root moved to.
	if resp := call(cluster.AdminRequest{Op: "rotate-store-key"}); resp.Error != "" {
		t.Fatalf("rotation after moving roots: %s", resp.Error)
	}
	key3 := storeKey(rootB)
	tc.StopNode(0)
	if _, err := restart(func(cfg *server.Config) { cfg.EncRoot = rootA }); err == nil {
		t.Fatal("the root the store moved away from still opens it")
	}
	if _, err := restart(func(cfg *server.Config) { cfg.EncRoot = rootB }); err != nil {
		t.Fatalf("restart with the root moved to: %v", err)
	}
	rows("after moving roots")
	for _, k := range [][]byte{key1, key2, key3} {
		if p := keyOnDisk(k); p != "" {
			t.Fatalf("a store key is in the clear in %s", p)
		}
	}
}

// TestRootRefusesAStoreThatIsNotItsOwn: --enc-root does not silently
// take over a store it did not make — one opened with a key file, or a
// plaintext one — and the offline adoption is what moves a key-file
// store under a root, after which the key file is no longer how it
// opens once the key is rotated.
func TestRootRefusesAStoreThatIsNotItsOwn(t *testing.T) {
	keyDir := t.TempDir()
	keyPath, rootPath := filepath.Join(keyDir, "store.key"), filepath.Join(keyDir, "root.key")
	for _, p := range []string{keyPath, rootPath} {
		k := make([]byte, enc.KeyLen)
		if _, err := rand.Read(k); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(p, k, 0o600); err != nil {
			t.Fatal(err)
		}
	}
	root := "file:" + rootPath
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	for name, encKey := range map[string]string{"key-file store": keyPath, "plaintext store": ""} {
		dir := t.TempDir()
		tc := StartWithOptions(t, 1, func(cfg *server.Config) {
			cfg.Dir = dir
			cfg.EncKeyPath = encKey
			cfg.GCInterval = -1
		})
		tc.addrs = append(tc.addrs, tc.Nodes[0].Addr())
		prefix := keys.TableDataPrefix(953)
		if err := tc.Nodes[0].DB().Put(ctx, append(prefix.Clone(), 'a'), []byte("v")); err != nil {
			t.Fatal(err)
		}
		tc.StopNode(0)
		restart := func(mutate func(*server.Config)) error {
			_, err := tc.RestartNodeErr(0, nil, func(cfg *server.Config) {
				cfg.Dir = dir
				mutate(cfg)
			})
			return err
		}
		err := restart(func(cfg *server.Config) { cfg.EncRoot, cfg.EncKeyPath = root, "" })
		if err == nil {
			t.Fatalf("%s: a root opened a store it did not make", name)
		}
		if enc.WrappedKeyExists(vfs.Default, dir) {
			t.Fatalf("%s: the refused start left a wrapped key behind", name)
		}
		if encKey == "" {
			if !strings.Contains(err.Error(), "unencrypted") {
				t.Fatalf("%s: %v", name, err)
			}
			continue
		}
		if !strings.Contains(err.Error(), "enc-root adopt") {
			t.Fatalf("%s: the refusal does not say how to adopt the root: %v", name, err)
		}
		// Adopt, as `datax debug enc-root adopt` does.
		r, err := enc.ParseRoot(root)
		if err != nil {
			t.Fatal(err)
		}
		key, err := enc.LoadKeyFile(keyPath)
		if err != nil {
			t.Fatal(err)
		}
		if err := enc.RewrapStoreKey(ctx, vfs.Default, dir, r, key); err != nil {
			t.Fatal(err)
		}
		if err := restart(func(cfg *server.Config) { cfg.EncRoot, cfg.EncKeyPath = root, "" }); err != nil {
			t.Fatalf("%s: start under the adopted root: %v", name, err)
		}
		if got, err := tc.Nodes[0].DB().Scan(ctx, prefix, prefix.PrefixEnd(), 0); err != nil || len(got) != 1 {
			t.Fatalf("%s: after adoption: %d rows, %v", name, len(got), err)
		}
	}
}
