package server

import (
	"context"
	"time"

	"github.com/sthorne/datax/pkg/cluster"
	"github.com/sthorne/datax/pkg/metrics"
	"github.com/sthorne/datax/pkg/util/log"
)

// The background rewrite of the sstables written before prefix mode
// (cluster version v15, issue #161; pkg/storage/rewrite.go). A store's
// first prefix-mode open leaves its existing tables with whole-key bloom
// filters that a prefix read cannot consult — correct, just no skip —
// and its cold bulk rests in L6 where natural compaction may never
// reach it. The pass runs once per start while such tables remain,
// paced like re-encryption (the same budget and pause), and reports in
// the node document (prefix_bloom_rewrite) and the
// datax_prefix_bloom_remaining_bytes gauge.

// maybeStartFilterRewrite starts the pass if the state engine runs in
// prefix mode and still holds tables with whole-key filters.
func (n *Node) maybeStartFilterRewrite() {
	if n.engine == nil || !n.engine.PrefixBloom() {
		return
	}
	_, files, err := n.engine.FilterRewriteStatus()
	if err != nil {
		log.Warnf("sweeping for sstables with whole-key filters: %v", err)
		return
	}
	if files == 0 {
		return
	}
	n.rewriteMu.Lock()
	start := !n.rewriteActive
	if start {
		n.rewriteActive = true
	}
	n.rewriteMu.Unlock()
	if !start {
		return
	}
	log.Infof("prefix bloom filters: %d live sstables carry whole-key filters; rewriting them in the background", files)
	if err := n.stopper.RunWorker(n.filterRewriteWorker); err != nil {
		n.rewriteMu.Lock()
		n.rewriteActive = false
		n.rewriteMu.Unlock()
	}
}

func (n *Node) filterRewriteStatus() *cluster.ReencryptionStatus {
	remaining, files, sweepErr := n.engine.FilterRewriteStatus()
	n.rewriteMu.Lock()
	defer n.rewriteMu.Unlock()
	st := &cluster.ReencryptionStatus{
		Active:         n.rewriteActive,
		RemainingBytes: remaining,
		RemainingFiles: files,
		RewrittenBytes: n.rewriteRewritten,
	}
	if sweepErr != nil {
		st.SweepError = sweepErr.Error()
	}
	return st
}

func (n *Node) filterRewriteWorker(ctx context.Context) {
	defer func() {
		n.rewriteMu.Lock()
		n.rewriteActive = false
		n.rewriteMu.Unlock()
	}()
	attempted := map[uint64]bool{}
	for pass := 0; pass < reencryptMaxPasses; pass++ {
		targeted, remaining, files, err := n.engine.FilterRewritePass(ctx, reencryptPassBytes, attempted)
		if targeted > 0 {
			metrics.PrefixBloomRewritten.Add(float64(targeted))
			n.rewriteMu.Lock()
			n.rewriteRewritten += targeted
			n.rewriteMu.Unlock()
		}
		if err != nil {
			if ctx.Err() == nil {
				log.Warnf("prefix bloom rewrite pass: %v", err)
			}
			return
		}
		if remaining == 0 {
			log.Infof("prefix bloom filters: every live sstable carries them")
			n.events.Record("upgrade", "prefix bloom filters: every live sstable carries them")
			return
		}
		if targeted == 0 {
			log.Infof("prefix bloom rewrite stopped: %d bytes in %d files cannot be rewritten by manual compaction (single-key or bottom-level files); they retire with natural churn", remaining, files)
			return
		}
		log.Debugf("prefix bloom rewrite: %d bytes in %d files remain with whole-key filters", remaining, files)
		select {
		case <-ctx.Done():
			return
		case <-time.After(reencryptPause):
		}
	}
	log.Warnf("prefix bloom rewrite stopped after %d passes with %d files remaining; the next start resumes it", reencryptMaxPasses, 0)
}
