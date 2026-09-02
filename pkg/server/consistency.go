package server

import (
	"bytes"
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/sthorne/datax/pkg/base"
	"github.com/sthorne/datax/pkg/cluster"
	"github.com/sthorne/datax/pkg/kvserver"
	"github.com/sthorne/datax/pkg/metrics"
	"github.com/sthorne/datax/pkg/util/log"
)

// The consistency sweep (issue #54): the store proposes a checksum probe
// on one led range (round-robin), then this node collects the other
// replicas' checksums over the admin channel and compares. A mismatch is
// replicated-state corruption — it increments
// datax_consistency_failures_total and logs every replica's digest.
//
// Off by default (Config.ConsistencyInterval == 0): hashing a range reads
// all of it, so operators opt in and pick the pace.

// CheckRangeConsistency probes one range this node's store leads and
// compares every replica's checksum. Returns mismatch=true when any
// replica diverged.
func (n *Node) CheckRangeConsistency(ctx context.Context, rangeID base.RangeID) (mismatch bool, err error) {
	probe, perr := n.store.ProposeChecksumOn(ctx, rangeID)
	if perr != nil {
		return false, perr
	}
	return n.collectAndCompare(ctx, probe)
}

// runConsistencyOnce probes the next led range, if any.
func (n *Node) runConsistencyOnce(ctx context.Context) {
	probe, err := n.store.ProposeChecksum(ctx)
	if err != nil {
		log.Debugf("consistency probe: %v", err)
		return
	}
	if probe == nil {
		return
	}
	if _, err := n.collectAndCompare(ctx, probe); err != nil {
		log.Debugf("consistency collection for %s: %v", probe.RangeID, err)
	}
}

func (n *Node) collectAndCompare(ctx context.Context, probe *kvserver.ConsistencyProbe) (bool, error) {
	mismatch := false
	for _, rep := range probe.Desc.Replicas {
		if rep.NodeID == n.ident.NodeID {
			continue
		}
		nd, ok := n.registry.Get(rep.NodeID)
		if !ok {
			log.Warnf("consistency check %s on %s: node n%d has no registry entry", probe.CheckID, probe.RangeID, rep.NodeID)
			continue
		}
		req := cluster.AdminRequest{Op: "collect-checksum", RangeID: probe.RangeID, CheckID: probe.CheckID}
		var resp cluster.AdminResponse
		cctx, cancel := context.WithTimeout(ctx, 15*time.Second)
		err := n.trans.Call(cctx, nd.Address, "admin", req, &resp)
		cancel()
		if err != nil || resp.Error != "" {
			// A lagging or dead replica is a liveness problem, not proof of
			// corruption: log and move on (the next sweep retries).
			log.Warnf("consistency check %s on %s: n%d unreachable or incomplete: %v %s",
				probe.CheckID, probe.RangeID, rep.NodeID, err, resp.Error)
			continue
		}
		if !bytes.Equal(resp.Checksum, probe.LocalSum) {
			mismatch = true
			metrics.ConsistencyFailures.Inc()
			log.Errorf("CONSISTENCY FAILURE on %s (check %s at applied index %d): leader n%d has %s, replica n%d has %s",
				probe.RangeID, probe.CheckID, probe.Index, n.ident.NodeID,
				hex.EncodeToString(probe.LocalSum), rep.NodeID, hex.EncodeToString(resp.Checksum))
		}
	}
	return mismatch, nil
}

// waitForApplication asks nodeID to block until its replica of rangeID has
// applied index (kvserver.StoreConfig.WaitForApplication, over the
// wait-applied admin op). A node running a binary that predates the op
// answers "unknown admin op"; the merge then proceeds as it did before
// the check existed rather than stalling merges for the whole roll.
func (n *Node) waitForApplication(ctx context.Context, nodeID base.NodeID, rangeID base.RangeID, index uint64) error {
	nd, ok := n.registry.Get(nodeID)
	if !ok {
		return fmt.Errorf("n%d has no registry entry", nodeID)
	}
	req := cluster.AdminRequest{Op: "wait-applied", RangeID: rangeID, Index: index}
	var resp cluster.AdminResponse
	if err := n.trans.Call(ctx, nd.Address, "admin", req, &resp); err != nil {
		return err
	}
	if resp.Error != "" {
		if strings.Contains(resp.Error, "unknown admin op") {
			log.Warnf("n%d runs a binary without wait-applied; merging %s without confirming its subsume there", nodeID, rangeID)
			return nil
		}
		return errors.New(resp.Error)
	}
	return nil
}

// startConsistencyLoop runs the paced sweep when enabled.
func (n *Node) startConsistencyLoop() {
	if n.cfg.ConsistencyInterval <= 0 {
		return
	}
	n.stopper.RunWorker(func(ctx context.Context) {
		t := time.NewTicker(n.cfg.ConsistencyInterval)
		defer t.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-t.C:
				tctx, cancel := context.WithTimeout(ctx, n.cfg.ConsistencyInterval)
				n.runConsistencyOnce(tctx)
				cancel()
			}
		}
	})
}
