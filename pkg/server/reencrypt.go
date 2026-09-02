package server

import (
	"context"
	"time"

	"github.com/sthorne/datax/pkg/cluster"
	"github.com/sthorne/datax/pkg/metrics"
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
	if err := n.engine.RotateStoreKeyLive(req.OldStoreKey, req.NewStoreKey); err != nil {
		return cluster.AdminResponse{Error: err.Error()}
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
	return cluster.AdminResponse{}
}

func (n *Node) reencryptionStatus() *cluster.ReencryptionStatus {
	remaining, files := n.engine.ReencryptionStatus()
	n.reencMu.Lock()
	defer n.reencMu.Unlock()
	return &cluster.ReencryptionStatus{
		Active:         n.reencActive,
		RemainingBytes: remaining,
		RemainingFiles: files,
		RewrittenBytes: n.reencRewritten,
	}
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
	for pass := 0; pass < reencryptMaxPasses; pass++ {
		targeted, remaining, files, err := n.engine.ReencryptPass(ctx, reencryptPassBytes)
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
