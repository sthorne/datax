package server

import (
	"bytes"
	"context"
	"strings"
	"time"

	"github.com/sthorne/datax/pkg/cluster"
	"github.com/sthorne/datax/pkg/metrics"
	"github.com/sthorne/datax/pkg/storage"
	"github.com/sthorne/datax/pkg/storage/enc"
	"github.com/sthorne/datax/pkg/util/log"
)

// Online encryption maintenance behind the admin RPCs:
//
//   - rotate-store-key re-wraps the data-key registry under a new store
//     key on the live engine (atomic reseal), swaps the key this node
//     seals artifacts with, and immediately re-seals the metadata backup
//     — the same end state the offline `datax debug rotate-enc-key`
//     produces, with the node up the whole time.
//   - reencrypt starts a paced background pass compacting live sstables
//     still under retired data keys; reencrypt-status reports progress.
//     RemainingBytes reaching 0 attests nothing under a retired key
//     remains.

const (
	// reencryptPassBytes bounds one pass's compaction work; the pause
	// between passes lets foreground compactions and flushes breathe.
	reencryptPassBytes = 64 << 20
	reencryptPause     = 500 * time.Millisecond
	// reencryptMaxPasses caps a runaway loop (a file Pebble keeps moving
	// without rewriting); the gauge stays nonzero and the operator re-runs.
	reencryptMaxPasses = 100
)

func (n *Node) serveRotateStoreKey(ctx context.Context, req cluster.AdminRequest) cluster.AdminResponse {
	if n.engine == nil || !n.engine.Encrypted() {
		return cluster.AdminResponse{Error: "store is not encrypted"}
	}
	if len(req.OldStoreKey) != enc.KeyLen || len(req.NewStoreKey) != enc.KeyLen {
		return cluster.AdminResponse{Error: "rotate-store-key requires --old-key and --new-key (32 bytes each)"}
	}
	// The request carries both store keys. Over the insecure listener
	// they would cross the network in cleartext — and anyone who can
	// reach the port could rotate the store — which defeats encryption
	// at rest; the offline path (`--dir` on a stopped node) covers
	// insecure clusters.
	if n.tlsCfgs == nil {
		return cluster.AdminResponse{Error: "rotate-store-key carries the store key and is only served over mutual TLS (secure cluster); rotate a stopped node offline with --dir"}
	}
	// One rotation at a time, end to end. The registry reseal serializes
	// on the engine's own lock, but the key swap and backup reseal below
	// do not — two overlapping rotations k1→k2 and k2→k3 could otherwise
	// interleave so that the registry ends under k3 while encKey, and the
	// metadata backup it seals, end under k2: the next restart with k3
	// opens the store but cannot read its backup (issue #66).
	n.rotateMu.Lock()
	defer n.rotateMu.Unlock()
	if err := n.engine.RotateStoreKeyLive(req.OldStoreKey, req.NewStoreKey); err != nil {
		return cluster.AdminResponse{Error: err.Error()}
	}
	if n.raftEngine != nil {
		// The raft engine's registry is sealed under the same store key.
		// On disk it lives under the state engine's directory and the
		// reseal above already covered it (enc.RotateStoreKey rotates a
		// store's raft subdirectory with it); an engine elsewhere (an
		// in-memory pair) is rotated here. A crash between the two
		// reseals is repaired at the next start by matching each
		// engine's registry against the candidate keys.
		if err := n.raftEngine.RotateStoreKeyLive(req.OldStoreKey, req.NewStoreKey); err != nil {
			if _, merr := n.raftEngine.MatchStoreKey([][]byte{req.NewStoreKey}); merr != nil {
				return cluster.AdminResponse{Error: "state engine rotated but the raft engine did not: " + err.Error() + "; rerun the rotation with the same keys"}
			}
		}
	}
	// Future artifact seals (metadata backup) must use the new key; swap it
	// and re-seal the backup now rather than waiting a heartbeat, so no
	// old-key artifact outlives the rotation.
	n.encKeyMu.Lock()
	hadKey := n.encKey != nil
	if hadKey {
		n.encKey = append([]byte(nil), req.NewStoreKey...)
	}
	n.encKeyMu.Unlock()
	if hadKey {
		n.exportMetadata(ctx)
	}
	log.Infof("store key rotated online; registry and metadata backup sealed under the new key")
	n.events.Record("key-rotation", "store key rotated online; registry and metadata backup sealed under the new key")
	n.reportKeyFilesAfterRotation(req.NewStoreKey)
	return cluster.AdminResponse{}
}

// reportKeyFilesAfterRotation tells the operator whether a restart would
// find the new store key: the node opens with whichever of its --enc-key
// files seals the registry, so a staged file (--enc-key old.key,new.key)
// carries it through; otherwise the retired key is all a restart has.
func (n *Node) reportKeyFilesAfterRotation(newKey []byte) {
	if len(n.encKeyPaths) == 0 {
		return
	}
	for _, p := range n.encKeyPaths {
		if k, err := enc.LoadKeyFile(p); err == nil && bytes.Equal(k, newKey) {
			log.Infof("key file %s holds the new store key; a restart opens the store with it", p)
			return
		}
	}
	log.Warnf("no key file given to --enc-key (%s) holds the new store key; a restart before it is added fails to open the store — write it to a file and restart with --enc-key <old>,<new> or replace the old file",
		strings.Join(n.encKeyPaths, ","))
}

func (n *Node) reencryptionStatus() *cluster.ReencryptionStatus {
	remaining, files, sweepErr := n.engine.ReencryptionStatus()
	if n.raftEngine != nil {
		r2, f2, err2 := n.raftEngine.ReencryptionStatus()
		remaining, files = remaining+r2, files+f2
		if sweepErr == nil {
			sweepErr = err2
		}
	}
	n.reencMu.Lock()
	defer n.reencMu.Unlock()
	st := &cluster.ReencryptionStatus{
		Active:         n.reencActive,
		RemainingBytes: remaining,
		RemainingFiles: files,
		RewrittenBytes: n.reencRewritten,
	}
	if sweepErr != nil {
		st.SweepError = sweepErr.Error()
	}
	return st
}

// serveReencrypt starts the background pass (idempotent while one runs)
// and returns the current status.
func (n *Node) serveReencrypt(cluster.AdminRequest) cluster.AdminResponse {
	if n.engine == nil || !n.engine.Encrypted() {
		return cluster.AdminResponse{Error: "store is not encrypted"}
	}
	n.reencMu.Lock()
	start := !n.reencActive
	if start {
		n.reencActive = true
	}
	n.reencMu.Unlock()
	if start {
		if err := n.stopper.RunWorker(n.reencryptWorker); err != nil {
			n.reencMu.Lock()
			n.reencActive = false
			n.reencMu.Unlock()
			return cluster.AdminResponse{Error: err.Error()}
		}
	}
	return cluster.AdminResponse{Reencryption: n.reencryptionStatus()}
}

func (n *Node) reencryptWorker(ctx context.Context) {
	defer func() {
		n.reencMu.Lock()
		n.reencActive = false
		n.reencMu.Unlock()
	}()
	// attempted spans the run: a file a compaction left in place is not
	// retried, so the files behind it get their turn (see ReencryptPass).
	attempted := map[uint64]bool{}
	engines := []*storage.Engine{n.engine}
	if n.raftEngine != nil {
		engines = append(engines, n.raftEngine)
	}
	for pass := 0; pass < reencryptMaxPasses; pass++ {
		var targeted, remaining int64
		var files int
		var err error
		for _, eng := range engines {
			t, r, f, perr := eng.ReencryptPass(ctx, reencryptPassBytes, attempted)
			targeted, remaining, files = targeted+t, remaining+r, files+f
			if perr != nil && err == nil {
				err = perr
			}
		}
		if targeted > 0 {
			metrics.ReencryptionRewritten.Add(float64(targeted))
			n.reencMu.Lock()
			n.reencRewritten += targeted
			n.reencMu.Unlock()
		}
		if err != nil {
			log.Warnf("re-encryption pass: %v", err)
			return
		}
		if remaining == 0 {
			log.Infof("re-encryption complete: no live sstable under a retired data key")
			n.events.Record("re-encryption", "re-encryption complete: no live sstable under a retired data key")
			return
		}
		if targeted == 0 {
			log.Warnf("re-encryption stopped: %d bytes in %d files under retired keys cannot be rewritten by manual compaction (single-key or bottom-level files); they retire with natural churn — re-run `datax debug reencrypt` later", remaining, files)
			return
		}
		log.Debugf("re-encryption: %d bytes in %d files remain under retired keys", remaining, files)
		select {
		case <-ctx.Done():
			return
		case <-time.After(reencryptPause):
		}
	}
	log.Warnf("re-encryption stopped after %d passes with stale files remaining; re-run `datax debug reencrypt`", reencryptMaxPasses)
}
