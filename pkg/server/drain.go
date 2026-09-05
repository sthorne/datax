package server

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/sthorne/datax/pkg/base"
	"github.com/sthorne/datax/pkg/kvpb"
	"github.com/sthorne/datax/pkg/kvserver"
	"github.com/sthorne/datax/pkg/util/log"
)

// DefaultDrainTimeout bounds a stop's drain when the configuration does
// not.
const DefaultDrainTimeout = 10 * time.Second

// leaseTransferTimeout bounds one lease transfer inside a drain: raft
// gives up a transfer after one election timeout (1s), so a lagging
// target is skipped for the next candidate well within the budget.
const leaseTransferTimeout = 3 * time.Second

// DrainReport says what a Drain achieved.
type DrainReport struct {
	// LeasesTransferred counts the ranges this node led that another
	// replica now leads; LeasesKept the ones it still leads (no live
	// peer to take them, a transfer that did not complete in time, or
	// the deadline).
	LeasesTransferred, LeasesKept int
	// ConnsClosed counts SQL connections that ended cleanly (an
	// admin_shutdown error at their next idle point); ConnsCut the ones
	// still open at the deadline, closed under them.
	ConnsClosed, ConnsCut int
	// Incomplete names why the drain stopped early ("" when it ran to
	// completion).
	Incomplete string
}

func (r DrainReport) String() string {
	s := fmt.Sprintf("drained: %d leases transferred (%d kept), %d SQL connections closed (%d cut)",
		r.LeasesTransferred, r.LeasesKept, r.ConnsClosed, r.ConnsCut)
	if r.Incomplete != "" {
		s += " — incomplete: " + r.Incomplete
	}
	return s
}

// DrainTimeout is the configured drain budget (DefaultDrainTimeout when
// unset).
func (n *Node) DrainTimeout() time.Duration {
	if n.cfg.DrainTimeout > 0 {
		return n.cfg.DrainTimeout
	}
	return DefaultDrainTimeout
}

// Drain prepares the node to stop without a service blip: it announces
// itself as shutting down (peers stop handing it leases and placing
// replicas on it), transfers every lease it holds to a live peer, and
// drains the SQL listener — no new connections, idle connections told
// to go (SQLSTATE 57P01), busy ones allowed to finish their statement
// or transaction. Everything is bounded by ctx: at its deadline the
// leases still held stay held (the cluster re-elects after the stop)
// and the remaining connections are closed. Call Stop afterwards; a
// node that has been drained keeps serving what still reaches it, so a
// cancelled stop only costs the transferred leases.
func (n *Node) Drain(ctx context.Context) DrainReport {
	n.shuttingDown.Store(true)
	n.events.Record("shutdown", "draining: shedding leases and SQL connections")
	pctx, cancel := context.WithTimeout(ctx, 2*time.Second)
	n.publishLiveness(pctx)
	cancel()

	var rep DrainReport
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		if n.sqlServer() != nil {
			rep.ConnsClosed, rep.ConnsCut = n.sqlServer().Drain(ctx)
		}
	}()
	rep.LeasesTransferred, rep.LeasesKept = n.shedLeases(ctx)
	wg.Wait()
	if err := ctx.Err(); err != nil {
		rep.Incomplete = err.Error()
	}
	log.Infof("node %s %s", n.ident.NodeID, rep)
	n.events.Record("shutdown", "%s", rep)
	return rep
}

// shedLeases transfers every lease this node holds to a live peer that
// is not itself leaving, a few ranges at a time, trying each replica
// in turn until one transfer completes.
func (n *Node) shedLeases(ctx context.Context) (moved, kept int) {
	now := n.clock.Now().WallTime
	live := map[base.NodeID]bool{}
	for _, nd := range n.registry.All() {
		if nd.NodeID != n.ident.NodeID && !nd.Leaving() && now-nd.LivenessTime < int64(n.livenessGrace()) {
			live[nd.NodeID] = true
		}
	}
	var led []kvpb.RangeDescriptor
	n.store.VisitReplicas(func(r *kvserver.Replica) bool {
		if r.IsLeader() {
			led = append(led, r.Desc())
		}
		return true
	})
	var (
		mu  sync.Mutex
		wg  sync.WaitGroup
		sem = make(chan struct{}, 4)
	)
	for _, desc := range led {
		if ctx.Err() != nil {
			kept++
			continue
		}
		wg.Add(1)
		sem <- struct{}{}
		go func(desc kvpb.RangeDescriptor) {
			defer wg.Done()
			defer func() { <-sem }()
			ok := false
			for _, r := range desc.Replicas {
				if r.NodeID == n.ident.NodeID || !live[r.NodeID] {
					continue
				}
				tctx, cancel := context.WithTimeout(ctx, leaseTransferTimeout)
				err := n.db.AdminTransferLease(tctx, desc.StartKey, r.NodeID)
				cancel()
				if err == nil {
					ok = true
					break
				}
				log.Debugf("drain: %s lease to n%d: %v", desc.RangeID, r.NodeID, err)
				if ctx.Err() != nil {
					break
				}
			}
			mu.Lock()
			if ok {
				moved++
			} else {
				kept++
			}
			mu.Unlock()
		}(desc)
	}
	wg.Wait()
	return moved, kept
}
