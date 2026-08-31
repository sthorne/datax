package kvserver

import (
	"context"
	"time"

	"github.com/sthorne/datax/pkg/kvpb"
	"github.com/sthorne/datax/pkg/metrics"
)

// Lease (leadership) transfer. Leaseholder = raft leader in datax, so
// transferring the lease is raft's TransferLeadership: the leader stops
// accepting proposals, brings the transferee fully up to date, and tells it
// to campaign. Everything downstream is already handled by existing paths —
// the new leader's timestamp cache is bumped on leadership acquisition
// (a transfer always starts a new term), and the old leader's in-flight
// proposals fail with ambiguous "leadership lost" errors.

// adminTransferLease executes an AdminTransferLeaseRequest on the leader.
func (r *Replica) adminTransferLease(ctx context.Context, req *kvpb.AdminTransferLeaseRequest) (*kvpb.AdminTransferLeaseResponse, *kvpb.Error) {
	desc := r.Desc()
	if req.Target == r.store.cfg.NodeID {
		return &kvpb.AdminTransferLeaseResponse{Desc: desc}, nil // already the leader
	}
	rep, ok := desc.GetReplica(req.Target)
	if !ok {
		return nil, kvpb.NewErrorf("%s: node %s has no replica to transfer the lease to", r.rangeID, req.Target)
	}

	// Hand the measured load to the incoming leaseholder first (riding the
	// log, so it is applied before the target can lead anything new): the
	// new leader seeds its tracker from it instead of starting amnesiac,
	// and rebalancing keeps seeing the range's real load across the
	// transfer. Best-effort — a failed proposal just means a cold start.
	if q, mature := r.load.qps(); mature && q > 0 {
		_, _ = r.proposeCmd(ctx, &kvpb.BatchRequest{Header: kvpb.BatchHeader{RangeID: r.rangeID}},
			cmdTriggers{load: &loadHandoff{QPS: q, AtNanos: r.store.loadNow()}})
	}

	r.node.TransferLeadership(ctx, uint64(r.replicaID), uint64(rep.ReplicaID))

	// Raft aborts a transfer that cannot complete within one election
	// timeout (1s at the default 100ms tick), so a bounded poll either
	// observes the new leader or this leader resuming.
	for i := 0; i < 25; i++ {
		if r.leaderHint() == req.Target {
			metrics.LeaseTransfers.Inc()
			return &kvpb.AdminTransferLeaseResponse{Desc: desc}, nil
		}
		select {
		case <-ctx.Done():
			return nil, kvpb.NewErrorf("%s: lease transfer to n%d: %v", r.rangeID, req.Target, ctx.Err())
		case <-time.After(100 * time.Millisecond):
		}
	}
	return nil, kvpb.NewErrorf("%s: lease transfer to n%d did not complete (target may be lagging); retry", r.rangeID, req.Target)
}
