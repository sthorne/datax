package server

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"

	"github.com/cockroachdb/pebble/v2/vfs"
	"github.com/sthorne/datax/pkg/cluster"
	"github.com/sthorne/datax/pkg/storage/enc"
	"github.com/sthorne/datax/pkg/util/log"
)

// A node whose store key is kept under a root (issues #292, #305; the
// model is in pkg/storage/enc/root.go). --enc-root is the alternative to
// --enc-key: with it the store key is made on this node, lives in the
// data directory only wrapped, and is rotated here without ever being
// put in a request.

// loadRootedStoreKey resolves the store key of a node started with
// --enc-root, before its engines open: it unwraps what the data
// directory holds, or — for a directory with no store in it — makes a
// key and wraps it. On return n.encKey opens the store and n.encRoot is
// the root that key is under.
func (n *Node) loadRootedStoreKey(ctx context.Context) error {
	if n.cfg.EncKeyPath != "" {
		return errors.New("--enc-key and --enc-root are alternatives: a store's key is either the file --enc-key names or kept under the root --enc-root names (to move a store from one to the other, see `datax debug enc-root`)")
	}
	if n.cfg.Engine != nil || n.cfg.Dir == "" {
		return errors.New("--enc-root needs a data directory: the wrapped store key is kept there")
	}
	roots, err := enc.ParseRoots(n.cfg.EncRoot)
	if err != nil {
		return fmt.Errorf("--enc-root: %w", err)
	}
	dir := n.cfg.Dir
	// A root key held as shares is in nobody's hands yet: wait, sealed,
	// for the people who hold them.
	if waitFor, err := sealedRoot(dir, roots); err != nil {
		return fmt.Errorf("reading the wrapped store key: %w", err)
	} else if waitFor != nil {
		if err := n.awaitUnseal(waitFor); err != nil {
			return err
		}
	}
	if !enc.WrappedKeyExists(vfs.Default, dir) {
		// No wrapped key. That is a new store, or one that is not this
		// root's to open.
		if enc.RegistryExists(vfs.Default, dir) {
			return fmt.Errorf("the store in %s is encrypted under a key file, not a root: start it with --enc-key, or put its key under the root first with `datax debug enc-root adopt`", dir)
		}
		if storeDirHasFiles(dir) {
			return fmt.Errorf("the store in %s exists unencrypted; refusing to open it with a root (encrypting in place is not supported)", dir)
		}
		if err := os.MkdirAll(dir, 0o755); err != nil {
			return err
		}
		key, err := enc.InitWrappedStoreKey(ctx, vfs.Default, dir, roots[0])
		if err != nil {
			return fmt.Errorf("making the store key: %w", err)
		}
		n.encKey, n.encRoots, n.encRoot, n.encEntryKeyID = key, roots, roots[0], roots[0].KeyID()
		log.Infof("store key made and wrapped by the %s root (%s)", roots[0].Kind(), rootKeyID(roots[0]))
		log.Audit("store-key-wrap", "root", roots[0].Kind(), "key_id", roots[0].KeyID(), "reason", "new store")
		return nil
	}
	got, err := enc.UnwrapStoreKeys(ctx, vfs.Default, dir, roots)
	if err != nil {
		return fmt.Errorf("unwrapping the store key: %w", err)
	}
	candidates := make([][]byte, len(got))
	for i := range got {
		candidates[i] = got[i].Key
	}
	// More than one key only after a rotation that did not finish; the
	// registry says which half it reached.
	idx, err := enc.MatchStoreKey(vfs.Default, dir, candidates)
	if err != nil {
		return fmt.Errorf("selecting the store key: %w", err)
	}
	n.encKey, n.encRoots, n.encRoot = candidates[idx], roots, got[idx].Root
	n.encBinding, n.encEntryKeyID = got[idx].Binding, got[idx].KeyID
	log.Audit("store-key-unwrap", "root", n.encRoot.Kind(), "key_id", n.encRoot.KeyID())
	// A split store's second registry is sealed under the same key. A
	// rotation interrupted between the two leaves it under the other
	// candidate; this node holds both, so it finishes the job here rather
	// than carry two keys (which the next rotation could not reseal).
	if raftDir := n.raftDir(); raftDir != "" && enc.RegistryExists(vfs.Default, raftDir) {
		ridx, err := enc.MatchStoreKey(vfs.Default, raftDir, candidates)
		if err != nil {
			return fmt.Errorf("selecting the raft engine's store key: %w", err)
		}
		if ridx != idx {
			if err := enc.RotateStoreKey(vfs.Default, raftDir, candidates[ridx], candidates[idx]); err != nil {
				return fmt.Errorf("finishing an interrupted store-key rotation on the raft engine: %w", err)
			}
			log.Infof("finished an interrupted store-key rotation: the raft engine's registry is under the store's key again")
		}
	}
	if len(got) > 1 || got[idx].Index != 0 {
		if err := enc.PruneWrappedKeys(vfs.Default, dir, got[idx].Index); err != nil {
			return fmt.Errorf("retiring the other half of an interrupted store-key rotation: %w", err)
		}
		log.Infof("an interrupted store-key rotation was settled: the store is under the key the registry reached")
	}
	log.Infof("store key unwrapped by the %s root (%s)", n.encRoot.Kind(), rootKeyID(n.encRoot))
	return nil
}

func rootKeyID(r enc.Root) string {
	if id := r.KeyID(); id != "" {
		return "key id " + id
	}
	return "no key id"
}

// storeDirHasFiles reports whether dir already holds a Pebble store.
func storeDirHasFiles(dir string) bool {
	names, err := os.ReadDir(dir)
	if err != nil {
		return false
	}
	for _, e := range names {
		if n := e.Name(); n == "CURRENT" || len(n) >= 8 && n[:8] == "MANIFEST" {
			return true
		}
	}
	return false
}

// resealStoreKeyLive moves the live engines' registries from oldKey to
// newKey: the state engine's, and a split store's raft engine's.
func (n *Node) resealStoreKeyLive(oldKey, newKey []byte) error {
	if err := n.engine.RotateStoreKeyLive(oldKey, newKey); err != nil {
		return err
	}
	if n.raftEngine != nil {
		// The raft engine's registry is sealed under the same store key.
		// On disk it lives under the state engine's directory and the
		// reseal above already covered it (enc.RotateStoreKey rotates a
		// store's raft subdirectory with it); an engine elsewhere (an
		// in-memory pair) is rotated here. A crash between the two
		// reseals is repaired at the next start by matching each
		// engine's registry against the candidate keys.
		if err := n.raftEngine.RotateStoreKeyLive(oldKey, newKey); err != nil {
			if _, merr := n.raftEngine.MatchStoreKey([][]byte{newKey}); merr != nil {
				return fmt.Errorf("state engine rotated but the raft engine did not: %w; rerun the rotation", err)
			}
		}
	}
	return nil
}

// adoptStoreKey makes newKey the key this node seals its own artifacts
// with, and reseals the metadata backup now rather than a heartbeat
// later, so nothing sealed under the old key outlives the rotation.
func (n *Node) adoptStoreKey(ctx context.Context, newKey []byte) {
	n.encKeyMu.Lock()
	hadKey := n.encKey != nil
	if hadKey {
		// The key it replaces stays held, not released: a reader that
		// took it before the swap may still be using it.
		n.encKey = enc.HoldKey(newKey)
	}
	n.encKeyMu.Unlock()
	if hadKey {
		n.exportMetadata(ctx)
	}
}

// serveRotateRootedStoreKey is rotate-store-key on a node whose key is
// under a root. The request carries no key: the new one is made here,
// wrapped by the root, and the registry resealed under it — so unlike
// the key-file form it has nothing to protect in transit and needs no
// particular transport.
func (n *Node) serveRotateRootedStoreKey(ctx context.Context, req cluster.AdminRequest) cluster.AdminResponse {
	if len(req.OldStoreKey) != 0 || len(req.NewStoreKey) != 0 {
		return cluster.AdminResponse{Error: "this node's store key is kept under a root: it makes the new key itself, and a rotation request must not carry one (run rotate-enc-key without --old-key and --new-key)"}
	}
	n.rotateMu.Lock()
	defer n.rotateMu.Unlock()
	n.encKeyMu.Lock()
	current := append([]byte(nil), n.encKey...)
	root := n.encRoot
	bound := !n.encBinding.IsZero()
	n.encKeyMu.Unlock()
	defer enc.Zero(current)
	// The new key's entry is bound as the old one's was: a rotation does
	// not change what a store's key is tied to.
	wctx := ctx
	if _, takes := root.(enc.ContextRoot); bound || takes {
		wctx = enc.WithBinding(ctx, n.storeKeyBinding())
	}
	next, err := enc.RotateWrappedStoreKey(wctx, vfs.Default, n.cfg.Dir, root, current, n.resealStoreKeyLive)
	if err != nil {
		return cluster.AdminResponse{Error: err.Error()}
	}
	n.noteWrapped(root, enc.BindingFrom(wctx))
	n.adoptStoreKey(ctx, next)
	msg := fmt.Sprintf("store key rotated online under the %s root (%s); registry and metadata backup sealed under the new key", root.Kind(), rootKeyID(root))
	log.Infof("%s", msg)
	n.events.Record("key-rotation", "%s", msg)
	return cluster.AdminResponse{}
}

// serveRewrapStoreKey wraps the store key again: under the root in use
// (after that root's own key has rotated), or under another of the roots
// this node was started with (moving the store to it). The store key and
// everything under it are untouched.
//
// The target is named by its position among the node's own --enc-root
// values, never by a spec in the request: an exec root is a command
// line, and a request that could name one would be a way to run a
// command on the node.
func (n *Node) serveRewrapStoreKey(ctx context.Context, req cluster.AdminRequest) cluster.AdminResponse {
	if n.engine == nil || !n.engine.Encrypted() {
		return cluster.AdminResponse{Error: "store is not encrypted"}
	}
	if n.encRoot == nil {
		return cluster.AdminResponse{Error: "this node's store key is a key file (--enc-key), not kept under a root; there is nothing to rewrap"}
	}
	n.rotateMu.Lock()
	defer n.rotateMu.Unlock()
	n.encKeyMu.Lock()
	key := append([]byte(nil), n.encKey...)
	from := n.encRoot
	roots := append([]enc.Root(nil), n.encRoots...)
	n.encKeyMu.Unlock()
	to := from
	if req.Root != 0 {
		if req.Root < 1 || req.Root > len(roots) {
			return cluster.AdminResponse{Error: fmt.Sprintf("this node has %d root(s); there is no root %d to rewrap under", len(roots), req.Root)}
		}
		to = roots[req.Root-1]
	}
	defer enc.Zero(key)
	// A rewrap binds the key to this node, whatever the root: it is the
	// operator's own act, and the one that puts an unbound key right.
	binding := n.storeKeyBinding()
	if err := enc.RewrapStoreKey(enc.WithBinding(ctx, binding), vfs.Default, n.cfg.Dir, to, key); err != nil {
		return cluster.AdminResponse{Error: err.Error()}
	}
	n.encKeyMu.Lock()
	n.encRoot = to
	n.encKeyMu.Unlock()
	n.noteWrapped(to, binding)
	if _, ok := to.(enc.HealthChecker); ok {
		n.rootHealth.usedJustNow(to.KeyID())
	}
	msg := fmt.Sprintf("store key rewrapped under the %s root (%s)", to.Kind(), rootKeyID(to))
	if to != from {
		msg = fmt.Sprintf("store key moved from the %s root (%s) to the %s root (%s); a restart needs --enc-root to name the new root", from.Kind(), rootKeyID(from), to.Kind(), rootKeyID(to))
	}
	log.Infof("%s", msg)
	n.events.Record("key-rotation", "%s", msg)
	return cluster.AdminResponse{}
}

// EncryptionRoot is the kind and key id of the root this node's store
// key is kept under ("" and "" for a key-file or plaintext store).
func (n *Node) EncryptionRoot() (kind, keyID string) {
	if r := n.securityRoot(); r != nil {
		return r.Kind, r.KeyID
	}
	return "", ""
}

// ResealMetadataBackup moves a stopped store's sealed metadata backup
// from oldKey to newKey, so one key opens everything in the directory.
// A backup that is absent or unsealed is left alone.
func ResealMetadataBackup(dir string, oldKey, newKey []byte) error {
	bakPath := dir + string(os.PathSeparator) + MetadataBackupFile
	raw, err := os.ReadFile(bakPath)
	if err != nil || !bytes.HasPrefix(raw, []byte(MetadataBackupMagic)) {
		return nil
	}
	plain, err := enc.Unseal(MetadataBackupMagic, oldKey, raw)
	if err != nil {
		return fmt.Errorf("resealing metadata backup: %w", err)
	}
	sealed, err := enc.Seal(MetadataBackupMagic, newKey, plain)
	if err != nil {
		return err
	}
	if err := os.WriteFile(bakPath+".tmp", sealed, 0o600); err != nil {
		return err
	}
	return os.Rename(bakPath+".tmp", bakPath)
}

// holdStoreKey moves the store key this start resolved into memory kept
// for keys (enc.HoldKey, issue #309) and says, once, how well that
// memory is protected. With RequireLockedKeys a shortfall refuses the
// start; otherwise it is a warning that carries its remedy.
func (n *Node) holdStoreKey() error {
	if n.encKey == nil {
		return nil
	}
	loaded := n.encKey
	n.encKey = enc.HoldKey(loaded)
	// The raft engine's key is the same key unless a rotation was
	// interrupted between the two engines; either way it moves too,
	// before the loaded copy it may share is wiped.
	if n.raftEncKey != nil {
		if bytes.Equal(n.raftEncKey, loaded) {
			n.raftEncKey = n.encKey
		} else {
			other := n.raftEncKey
			n.raftEncKey = enc.HoldKey(other)
			enc.Zero(other)
		}
	}
	enc.Zero(loaded)
	st := enc.KeyMemory()
	switch {
	case st.Locked && st.NoDump:
		log.Infof("key memory: locked (never swapped) and left out of core dumps; core dumps of the process: %s", st.CoreDumps)
	case n.cfg.RequireLockedKeys:
		return fmt.Errorf("--require-locked-keys: key memory is not fully protected (locked=%t, out of core dumps=%t): %s", st.Locked, st.NoDump, st.Reason)
	default:
		log.Warnf("key memory is not fully protected (locked=%t, out of core dumps=%t): %s. The node runs; its keys may reach swap or a core dump. --require-locked-keys refuses to start in this state", st.Locked, st.NoDump, st.Reason)
	}
	return nil
}
