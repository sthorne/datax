package testcluster

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/cockroachdb/pebble/v2/vfs"
	"github.com/jackc/pgx/v5"
	"github.com/sthorne/datax/pkg/base"
	"github.com/sthorne/datax/pkg/cluster"
	"github.com/sthorne/datax/pkg/rpc"
	"github.com/sthorne/datax/pkg/security"
	"github.com/sthorne/datax/pkg/server"
	"github.com/sthorne/datax/pkg/storage/enc"
	"github.com/sthorne/datax/pkg/util/hlc"
)

// TestSealedStartOpensOnAQuorumOfShares (issue #306): a cluster whose
// store keys are under a root key split 2-of-3 among operators. Every
// node starts sealed — a new one and a restarted one alike — and opens
// only when two distinct shares have reached it; every pair of shares
// works. One share, a repeated share, a share of another key and a
// damaged share are each refused, as is anyone but root, and a sealed
// node serves nothing else. Then the root key itself is replaced on a
// running node, and the shares of the old one no longer open it.
func TestSealedStartOpensOnAQuorumOfShares(t *testing.T) {
	rootKey, err := enc.NewStoreKey()
	if err != nil {
		t.Fatal(err)
	}
	shares, err := enc.SplitRootKey(nil, rootKey, 2, 3)
	if err != nil {
		t.Fatal(err)
	}
	otherKey, _ := enc.NewStoreKey()
	foreign, err := enc.SplitRootKey(nil, otherKey, 2, 3)
	if err != nil {
		t.Fatal(err)
	}
	pairs := [][2]int{{0, 1}, {0, 2}, {1, 2}}

	ctx, cancel := context.WithTimeout(context.Background(), 180*time.Second)
	defer cancel()
	var (
		transMu           sync.Mutex
		rootTrans, alices *rpc.Transport
	)
	// call sends one admin request to addr as root (or as alice, an
	// ordinary user with a certificate).
	call := func(addr string, asAlice bool, req cluster.AdminRequest) cluster.AdminResponse {
		transMu.Lock()
		tr := rootTrans
		if asAlice {
			tr = alices
		}
		transMu.Unlock()
		cctx, ccancel := context.WithTimeout(ctx, 20*time.Second)
		defer ccancel()
		var resp cluster.AdminResponse
		if err := tr.Call(cctx, addr, "admin", req, &resp); err != nil {
			resp.Error = "transport: " + err.Error()
		}
		return resp
	}
	unseal := func(addr, share string) cluster.AdminResponse {
		return call(addr, false, cluster.AdminRequest{Op: "unseal", Share: share})
	}
	// problems collects what the goroutines that unseal find wrong; they
	// cannot call t.Fatal.
	var (
		probMu   sync.Mutex
		problems []string
	)
	problem := func(format string, args ...any) {
		probMu.Lock()
		defer probMu.Unlock()
		problems = append(problems, fmt.Sprintf(format, args...))
		t.Logf(format, args...)
	}
	checkProblems := func(when string) {
		t.Helper()
		probMu.Lock()
		defer probMu.Unlock()
		if len(problems) > 0 {
			t.Fatalf("%s: %d problem(s) while sealed (see the log above)", when, len(problems))
		}
	}
	expectErr := func(what string, resp cluster.AdminResponse, want string) {
		if !strings.Contains(resp.Error, want) {
			problem("%s: error %q, want it to contain %q", what, resp.Error, want)
		}
	}
	// openWith submits the pair's two shares to a sealed node, with the
	// refusals in between when thorough.
	openWith := func(pair [2]int, set []enc.Share, thorough bool, wantKeyID string) func(addr string) {
		return func(addr string) {
			go func() {
				st := call(addr, true, cluster.AdminRequest{Op: "seal-status"})
				if st.Error != "" || st.Seal == nil || !st.Seal.Sealed || st.Seal.Have != 0 || st.Seal.KeyID != wantKeyID {
					problem("seal-status of a sealed node: %+v %q", st.Seal, st.Error)
				}
				if thorough {
					expectErr("ranges on a sealed node", call(addr, false, cluster.AdminRequest{Op: "ranges"}), "sealed")
					expectErr("a share from a user who is not root", call(addr, true, cluster.AdminRequest{Op: "unseal", Share: set[pair[0]].String()}), "only the root user's certificate")
					if wantKeyID != "" {
						expectErr("a share of another cluster's key", unseal(addr, foreign[0].String()), "share of root key")
					}
					expectErr("something that is not a share", unseal(addr, "hunter2"), "not a share")
				}
				first := unseal(addr, set[pair[0]].String())
				if first.Error != "" || first.Seal == nil || !first.Seal.Sealed || first.Seal.Have != 1 || first.Seal.Need != set[0].Threshold {
					problem("first share: %+v %q", first.Seal, first.Error)
				}
				if thorough {
					expectErr("the same share again", unseal(addr, set[pair[0]].String()), "already been submitted")
					damaged := []byte(set[pair[1]].String())
					at := len(enc.SharePrefix) + 20
					if damaged[at] == 'a' {
						damaged[at] = 'c'
					} else {
						damaged[at] = 'a'
					}
					expectErr("a damaged share", unseal(addr, string(damaged)), "mistyped or damaged")
					if wantKeyID == "" {
						expectErr("a share of a second key", unseal(addr, foreign[0].String()), "share of root key")
					}
					st := call(addr, false, cluster.AdminRequest{Op: "seal-status"})
					if st.Seal == nil || !st.Seal.Sealed || st.Seal.Have != 1 || len(st.Seal.Indexes) != 1 || st.Seal.Indexes[0] != pair[0]+1 {
						problem("after one share and four refusals: %+v", st.Seal)
					}
					// One share is not enough, however long it is left.
					time.Sleep(300 * time.Millisecond)
					if st := call(addr, false, cluster.AdminRequest{Op: "seal-status"}); st.Seal == nil || !st.Seal.Sealed {
						problem("the node opened on one share: %+v %q", st.Seal, st.Error)
					}
				}
				second := unseal(addr, set[pair[1]].String())
				if second.Error != "" || second.Seal == nil || second.Seal.Sealed {
					problem("second share: %+v %q", second.Seal, second.Error)
				}
			}()
		}
	}

	dirs := make([]string, 3)
	for i := range dirs {
		dirs[i] = t.TempDir()
	}
	var certsDirEarly string
	tc, certsDir := startSecureCluster(t, "topsecret", func(i int, cfg *server.Config) {
		// The client certificates have to exist before the first node
		// waits to be unsealed: nothing can be created while it blocks.
		if i == 0 {
			certsDirEarly = cfg.CertsDir
			for _, user := range []string{"root", "alice"} {
				if err := security.CreateClientCert(certsDirEarly, user); err != nil {
					t.Fatal(err)
				}
			}
			mk := func(user string) *rpc.Transport {
				tlsCfg, err := security.LoadClientTLS(certsDirEarly, user)
				if err != nil {
					t.Fatal(err)
				}
				tr := rpc.NewTransport(hlc.NewClock(nil, base.DefaultMaxClockOffset), nil, nil)
				tr.SetTLS(tlsCfg)
				return tr
			}
			transMu.Lock()
			rootTrans, alices = mk("root"), mk("alice")
			transMu.Unlock()
		}
		cfg.Dir = dirs[i]
		cfg.EncRoot = "shares"
		cfg.GCInterval = -1
		// A new store has no wrapped key to say which root key it waits
		// for: the first share says.
		cfg.OnSealed = openWith(pairs[i], shares, i == 0, "")
	})
	checkProblems("first start")
	for _, n := range tc.Nodes {
		tc.addrs = append(tc.addrs, n.Addr())
	}
	keyID := enc.RootKeyID(rootKey)
	for i, n := range tc.Nodes {
		if kind, id := n.EncryptionRoot(); kind != "shares" || id != keyID {
			t.Fatalf("n%d: root %q %q, want shares %q", i+1, kind, id, keyID)
		}
	}
	// The root key and the three store keys are in memory kept for keys
	// (issue #309), not on the heap.
	if km := enc.KeyMemory(); km.Held < 4 {
		t.Fatalf("key memory holds %d keys with three nodes open under one root key", km.Held)
	}
	// A running node says it is not sealed, and to anyone.
	if st := call(tc.addrs[0], true, cluster.AdminRequest{Op: "seal-status"}); st.Error != "" || st.Seal == nil || st.Seal.Sealed || st.Seal.Root != "shares" || st.Seal.KeyID != keyID {
		t.Fatalf("seal-status of a running node: %+v %q", st.Seal, st.Error)
	}

	// Root's credential is seeded asynchronously; retry until auth works.
	var conn *pgx.Conn
	for deadline := time.Now().Add(30 * time.Second); ; {
		if conn, err = connectSecure(ctx, secureURL(tc, certsDir, "root", "topsecret")); err == nil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("root could never authenticate: %v", err)
		}
		time.Sleep(200 * time.Millisecond)
	}
	if _, err := conn.Exec(ctx, "CREATE TABLE sealed (id INT PRIMARY KEY, v STRING)"); err != nil {
		t.Fatal(err)
	}
	if _, err := conn.Exec(ctx, "INSERT INTO sealed VALUES (1, 'kept under shares'), (2, 'and again')"); err != nil {
		t.Fatal(err)
	}
	_ = conn.Close(ctx)

	// Neither the root key nor a share is anywhere in a data directory;
	// the wrapped-key file names the root and holds a blob.
	scan := func(when string, secrets ...[]byte) {
		t.Helper()
		for _, dir := range dirs {
			_ = filepath.WalkDir(dir, func(path string, d os.DirEntry, err error) error {
				if err != nil || d.IsDir() {
					return nil
				}
				raw, err := os.ReadFile(path)
				if err != nil {
					return nil
				}
				for _, s := range secrets {
					if bytes.Contains(raw, s) {
						t.Fatalf("%s: %s contains key material", when, path)
					}
				}
				return nil
			})
		}
	}
	secrets := [][]byte{rootKey}
	for _, s := range shares {
		secrets = append(secrets, []byte(strings.TrimPrefix(s.String(), enc.SharePrefix)))
	}
	scan("running", secrets...)
	for _, dir := range dirs {
		entries, err := enc.ReadWrappedKeys(vfs.Default, dir)
		if err != nil || len(entries) != 1 || entries[0].Root != "shares" || entries[0].KeyID != keyID {
			t.Fatalf("%s: wrapped key %+v, %v", dir, entries, err)
		}
	}

	// A cold start: everything down, and each node waits for two shares
	// again — this time knowing, from its own disk, which key's.
	tc.StopAll()
	restartAll := func(onSealed func(i int) func(string), root string) {
		t.Helper()
		var wg sync.WaitGroup
		errs := make([]error, 3)
		for i := range dirs {
			wg.Add(1)
			go func(i int) {
				defer wg.Done()
				_, errs[i] = tc.RestartNodeErr(i, nil, func(cfg *server.Config) {
					cfg.Dir = dirs[i]
					cfg.CertsDir = certsDir
					cfg.PGListen = "127.0.0.1:0"
					cfg.EncRoot = root
					cfg.OnSealed = onSealed(i)
				})
			}(i)
		}
		wg.Wait()
		for i, err := range errs {
			if err != nil {
				t.Fatalf("restarting n%d: %v", i+1, err)
			}
		}
	}
	restartAll(func(i int) func(string) { return openWith(pairs[(i+1)%3], shares, i == 0, keyID) }, "shares")
	checkProblems("cold start")
	rows := func(when string) {
		t.Helper()
		var n int
		deadline := time.Now().Add(60 * time.Second)
		for {
			c, err := connectSecure(ctx, secureURL(tc, certsDir, "root", "topsecret"))
			if err == nil {
				err = c.QueryRow(ctx, "SELECT count(*) FROM sealed").Scan(&n)
				_ = c.Close(ctx)
			}
			if err == nil && n == 2 {
				return
			}
			if time.Now().After(deadline) {
				t.Fatalf("%s: rows %d, %v", when, n, err)
			}
			time.Sleep(200 * time.Millisecond)
		}
	}
	rows("after a cold start")
	scan("after a cold start", secrets...)

	// Replacing the root key. A new key is split; its shares go to the
	// RUNNING node, which holds the new key beside the one in use until
	// the store key is moved under it.
	newKey, _ := enc.NewStoreKey()
	newShares, err := enc.SplitRootKey(nil, newKey, 2, 2)
	if err != nil {
		t.Fatal(err)
	}
	addr := tc.addrs[0]
	expectNow := func(what string, resp cluster.AdminResponse, want string) {
		t.Helper()
		if !strings.Contains(resp.Error, want) {
			t.Fatalf("%s: error %q, want it to contain %q", what, resp.Error, want)
		}
	}
	expectNow("a share of the key in use, to a running node", unseal(addr, shares[0].String()), "already running under")
	expectNow("staging a share as a user without the admin role", call(addr, true, cluster.AdminRequest{Op: "unseal", Share: newShares[0].String()}), "permission denied")
	if resp := unseal(addr, newShares[0].String()); resp.Error != "" || resp.Seal == nil || resp.Seal.Sealed || resp.Seal.Staged == nil || !resp.Seal.Staged.Sealed || resp.Seal.Staged.Have != 1 {
		t.Fatalf("staging the first share of a new root key: %+v %q", resp.Seal, resp.Error)
	}
	expectNow("rewrapping under a root key that is not whole yet", call(addr, false, cluster.AdminRequest{Op: "rewrap-store-key", Root: 2}), "sealed")
	staged := unseal(addr, newShares[1].String())
	if staged.Error != "" || staged.Seal == nil || staged.Seal.Staged == nil || staged.Seal.Staged.Sealed || staged.Seal.Staged.Position != 2 || staged.Seal.Staged.KeyID != enc.RootKeyID(newKey) {
		t.Fatalf("staging the second share: %+v %q", staged.Seal, staged.Error)
	}
	if resp := call(addr, false, cluster.AdminRequest{Op: "rewrap-store-key", Root: 2}); resp.Error != "" {
		t.Fatalf("moving the store key under the new root key: %s", resp.Error)
	}
	if _, id := tc.Nodes[0].EncryptionRoot(); id != enc.RootKeyID(newKey) {
		t.Fatalf("after the move the node reports root key %q", id)
	}
	// The node restarts under the new key's shares, and the old key's
	// are refused as they are offered.
	tc.StopNode(0)
	if _, err := tc.RestartNodeErr(0, nil, func(cfg *server.Config) {
		cfg.Dir = dirs[0]
		cfg.CertsDir = certsDir
		cfg.PGListen = "127.0.0.1:0"
		cfg.EncRoot = "shares"
		cfg.OnSealed = func(addr string) {
			go func() {
				expectErr("a share of the root key the store moved away from", unseal(addr, shares[0].String()), "share of root key")
				if r := unseal(addr, newShares[1].String()); r.Error != "" {
					problem("new share 2: %q", r.Error)
				}
				if r := unseal(addr, newShares[0].String()); r.Error != "" || r.Seal == nil || r.Seal.Sealed {
					problem("new share 1: %+v %q", r.Seal, r.Error)
				}
			}()
		}
	}); err != nil {
		t.Fatalf("restart under the new root key: %v", err)
	}
	checkProblems("restart under the new root key")
	rows("after replacing the root key")
	scan("after replacing the root key", append(secrets, newKey)...)
}
