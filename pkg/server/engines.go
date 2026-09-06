package server

import (
	"fmt"
	"path/filepath"

	"github.com/cockroachdb/pebble/v2/vfs"

	"github.com/sthorne/datax/pkg/cluster"
	"github.com/sthorne/datax/pkg/keys"
	"github.com/sthorne/datax/pkg/kvserver"
	"github.com/sthorne/datax/pkg/storage"
	"github.com/sthorne/datax/pkg/storage/enc"
	"github.com/sthorne/datax/pkg/util/log"
	"github.com/sthorne/datax/pkg/version"
)

// The store's engines (issue #105). A node's store is one Pebble engine
// under --dir until the cluster finalizes v13; from then on it is two:
// the state machine under --dir with no write-ahead log, and the raft
// log under --dir/raft with one (pkg/kvserver/raftengine.go explains the
// durability model). A store migrates on its first start after the
// finalize — its raft state moves to the raft engine, bounded by the log
// size that truncation keeps small — and records that it did
// (keys.StoreRaftEngineKey) alongside a store cluster version of at least
// v13, so a v12 binary refuses to open it. A fresh store bootstrapped by
// a v13 binary, or joining a v13 cluster, is split from the start.

// stateOptions are the state engine's options; disableWAL for a split
// store, prefixBloom for a store at cluster version v15 (issue #161).
func (n *Node) stateOptions(disableWAL, prefixBloom bool) storage.Options {
	return storage.Options{
		Profile: n.cfg.StorageProfile, EncryptionKey: n.encKey,
		MemTableSize: n.cfg.StorageMemTableSize, CacheSize: n.cfg.StorageCacheSize,
		DisableWAL: disableWAL, PrefixBloom: prefixBloom,
	}
}

// prefixBloomFor reports whether a store at cluster version cv runs in
// prefix mode (issue #161): from v15 on. The comparer is fixed at open,
// so the decision is made where the state engine is (re)opened — at
// start from the store's own version copy, and for a fresh store from
// the version it is born at — and a running node that observes the
// finalize switches at its next restart (ratchetStoreFormat says so).
func prefixBloomFor(cv version.Version) bool { return cv >= version.V15 }

// raftOptions are the raft engine's options: the store's profile and
// encryption, a small memtable (Options.Raft).
func (n *Node) raftOptions() storage.Options {
	key := n.encKey
	if n.raftEncKey != nil {
		key = n.raftEncKey
	}
	return storage.Options{Profile: n.cfg.StorageProfile, EncryptionKey: key, CacheSize: n.cfg.StorageCacheSize, Raft: true}
}

// raftDir is the raft engine's directory ("" for an in-memory store).
func (n *Node) raftDir() string {
	if n.cfg.Dir == "" {
		return ""
	}
	return filepath.Join(n.cfg.Dir, enc.RaftSubdir)
}

// openEngines opens the store: single-engine, or split when the store
// says so, migrating it first when the cluster version calls for it.
func (n *Node) openEngines() error {
	state, err := storage.Open(n.cfg.Dir, n.stateOptions(false, false))
	if err != nil {
		return err
	}
	split, err := kvserver.IsSplitStore(state)
	if err != nil {
		_ = state.Close()
		return err
	}
	_, initialized, err := cluster.ReadStoreIdent(state)
	if err != nil {
		_ = state.Close()
		return err
	}
	stored, err := readStoreClusterVersion(state)
	if err != nil {
		_ = state.Close()
		return err
	}
	fresh := !initialized && (n.cfg.BootstrapSelf || n.cfg.StaticBootstrap != nil)
	if !split && ((initialized && stored >= version.V13) || (fresh && n.binaryVersion() >= version.V13)) {
		if err := n.migrateToSplitStore(state, stored); err != nil {
			_ = state.Close()
			return err
		}
		split = true
	}
	if !split {
		n.engine = state
		return nil
	}
	// The version the store runs at: its own copy, or for a store this
	// binary is about to bootstrap the binary's (a joiner learns its
	// cluster's version after the join, see activateSplitStore).
	cv := stored
	if fresh {
		cv = n.binaryVersion()
	}
	// Reopen the state engine without its WAL — and in prefix mode when
	// the version calls for it — and open the raft engine.
	if err := state.Close(); err != nil {
		return err
	}
	if n.engine, err = storage.Open(n.cfg.Dir, n.stateOptions(true, prefixBloomFor(cv))); err != nil {
		return err
	}
	if n.raftEngine, err = storage.Open(n.raftDir(), n.raftOptions()); err != nil {
		_ = n.engine.Close()
		n.engine = nil
		return fmt.Errorf("opening the raft engine: %w", err)
	}
	return nil
}

// migrateToSplitStore moves the raft state of the (open, WAL-backed)
// state engine onto a fresh raft engine and marks the store split; the
// store's cluster version is raised to v13 so an older binary refuses
// the store afterwards. The raft engine is closed again; the caller
// closes and reopens the state engine without its WAL.
func (n *Node) migrateToSplitStore(state *storage.Engine, stored version.Version) error {
	raft, err := storage.Open(n.raftDir(), n.raftOptions())
	if err != nil {
		return fmt.Errorf("opening the raft engine for migration: %w", err)
	}
	moved, err := kvserver.MigrateRaftState(state, raft)
	if err != nil {
		_ = raft.Close()
		return fmt.Errorf("moving raft state to its own engine: %w", err)
	}
	if stored < version.V13 {
		if err := state.Put(keys.StoreClusterVersionKey(), []byte(fmt.Sprintf("%d", int(version.V13)))); err != nil {
			_ = raft.Close()
			return err
		}
	}
	if err := state.Flush(); err != nil {
		_ = raft.Close()
		return err
	}
	if err := raft.Close(); err != nil {
		return err
	}
	if moved > 0 {
		log.Infof("store migrated: %d raft keys moved to %s; the state engine now runs without a WAL", moved, n.raftDir())
	} else {
		log.Infof("store opened split: raft log under %s, state engine without a WAL", n.raftDir())
	}
	return nil
}

// activateSplitStore migrates a running single-engine store (a fresh
// joiner that learned the cluster is at v13) before any replica exists
// on it: the engines are swapped in place.
func (n *Node) activateSplitStore() error {
	if n.raftEngine != nil || n.cfg.Engine != nil {
		return nil // already split, or injected engines (tests)
	}
	stored, err := readStoreClusterVersion(n.engine)
	if err != nil {
		return err
	}
	if err := n.migrateToSplitStore(n.engine, stored); err != nil {
		return err
	}
	if err := n.engine.Close(); err != nil {
		return err
	}
	cv := version.Version(n.clusterVersion.Load())
	if n.engine, err = storage.Open(n.cfg.Dir, n.stateOptions(true, prefixBloomFor(cv))); err != nil {
		return err
	}
	if n.raftEngine, err = storage.Open(n.raftDir(), n.raftOptions()); err != nil {
		return fmt.Errorf("opening the raft engine: %w", err)
	}
	return nil
}

// matchRaftEncKey picks the candidate key that seals the raft engine's
// registry (a rotation interrupted between the two engines leaves them
// under different keys until the next rotation completes).
func (n *Node) matchRaftEncKey(candidates [][]byte) {
	dir := n.raftDir()
	if dir == "" || len(candidates) < 2 || !enc.RegistryExists(vfs.Default, dir) {
		return
	}
	idx, err := enc.MatchStoreKey(vfs.Default, dir, candidates)
	if err != nil {
		log.Warnf("selecting the raft engine's encryption key: %v", err)
		return
	}
	n.raftEncKey = candidates[idx]
}

// persistStoreClusterVersion records the cluster version the store
// observed (the downgrade gate), durably on a WAL-less state engine.
func (n *Node) persistStoreClusterVersion(raw []byte) error {
	if err := n.engine.Put(keys.StoreClusterVersionKey(), raw); err != nil {
		return err
	}
	if n.engine.WALDisabled() {
		return n.engine.Flush()
	}
	return nil
}

// ratchetStoreFormat moves both engines to the Pebble format the cluster
// version calls for (issue #166): FormatColumnarBlocks from v14 on. Run
// wherever the node learns its cluster version — at start from the
// store's copy, on join, after a bootstrap, and when the running node
// observes a finalize — so the ratchet is online and needs no restart.
// Idempotent; an error is logged, not fatal: the store keeps serving at
// its current format and the next observation retries.
func (n *Node) ratchetStoreFormat() {
	if version.Version(n.clusterVersion.Load()) < version.V14 || n.engine == nil {
		return
	}
	for _, e := range []*storage.Engine{n.engine, n.raftEngine} {
		if e == nil {
			continue
		}
		was := e.Format()
		if err := e.RatchetFormat(storage.FormatColumnarBlocks); err != nil {
			log.Warnf("ratcheting the store's Pebble format to %d: %v", storage.FormatColumnarBlocks, err)
			continue
		}
		if was != e.Format() {
			log.Infof("store Pebble format %d → %d (cluster version %s: columnar blocks)", was, e.Format(), version.Version(n.clusterVersion.Load()))
		}
	}
	// Prefix mode (v15) is a property of the open, not of the store:
	// say so once when the version arrives while the engine runs
	// without it.
	if prefixBloomFor(version.Version(n.clusterVersion.Load())) && !n.engine.PrefixBloom() && n.prefixBloomNoticed.CompareAndSwap(false, true) {
		log.Infof("cluster version %s: the store switches to prefix bloom filters at this node's next restart (issue #161)", version.Version(n.clusterVersion.Load()))
		n.events.Record("upgrade", "the store switches to prefix bloom filters at this node's next restart")
	}
}

// adoptBootstrapVersion records, on a store this binary just bootstrapped,
// the cluster version it was born at — in memory and in the store's own
// copy (what the heartbeat mirror would otherwise write on its first
// pass, and what the downgrade gate reads before quorum) — and applies
// what that version asks of the store (the format ratchet).
func (n *Node) adoptBootstrapVersion() error {
	cv := n.binaryVersion()
	n.clusterVersion.Store(int64(cv))
	if err := n.persistStoreClusterVersion([]byte(fmt.Sprintf("%d", int(cv)))); err != nil {
		return err
	}
	n.ratchetStoreFormat()
	return nil
}

// storePrefixBloom reports whether the state engine runs in prefix mode.
func (n *Node) storePrefixBloom() bool { return n.engine != nil && n.engine.PrefixBloom() }

// storeFormat is the state engine's Pebble format for status output.
func (n *Node) storeFormat() int {
	if n.engine == nil {
		return 0
	}
	return n.engine.Format()
}

// engineMode names the store layout for status output.
func (n *Node) engineMode() string {
	if n.raftEngine != nil {
		return "split"
	}
	return "single"
}
