package server

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/sthorne/datax/pkg/base"
	"github.com/sthorne/datax/pkg/cluster"
	"github.com/sthorne/datax/pkg/keys"
	"github.com/sthorne/datax/pkg/kvclient"
	"github.com/sthorne/datax/pkg/kvpb"
	"github.com/sthorne/datax/pkg/kvserver"
	"github.com/sthorne/datax/pkg/metrics"
	"github.com/sthorne/datax/pkg/util/hlc"
	"github.com/sthorne/datax/pkg/util/log"
	"github.com/sthorne/datax/pkg/version"
)

// Purge (issue #295): ALTER TABLE ... PURGE and `datax debug purge`.
//
// A deleted row is gone from every read at once and from the disk a GC
// window later, and then only from the reach of reads: the bytes wait in
// their sstables for a compaction. An application that has promised to
// destroy an account's data needs the second kind of gone, on a
// schedule, and a record that it happened. A purge is that, for one key
// span, coordinated from the node that was asked:
//
//  1. Split at the span's two ends, so the ranges that follow lie wholly
//     inside it. Collection raises a range's GC threshold, and with it
//     refuses every older read of that range; the split keeps that to
//     the span that was named.
//  2. On each range's leader, collect garbage at a threshold of now — or
//     just below the oldest backup in progress, which a purge does not
//     cut off (backup_protect.go, issue #376) — and truncate the raft
//     log (kvserver.Store.PurgeRangeBelow).
//  3. On every replica of each range, wait for the leader's applied
//     index, compact the span on the state engine and the range's log on
//     the raft engine, and count what is left (kvserver.Store.
//     PurgeReplica).
//  4. On every node that holds one of those replicas, retire the
//     write-ahead logs written before the purge (kvserver.Store.
//     ScrubLogs).
//
// The result is the per-replica count. It is complete when every replica
// answered and none holds a superseded version, a tombstone or an intent
// in the span; anything else is reported as what it is, with the reason,
// and the same purge can be run again: a replica that was down applies
// the collection when it rejoins (and serves no stale read until it has,
// see kvserver.Replica.heardSinceStart), and a second purge then compacts
// it and counts it. The report is written to the audit log whole.
//
// What a purge does not reach is in the report too (purgeNotCovered), so
// that "complete" is not read as more than it says.

// purgeNotCovered is PurgeReport.NotCovered: where a purged value can
// still be, as far as this database knows.
var purgeNotCovered = []string{
	"backups taken before the purge still hold the data; destroy or rewrite them separately",
	"a replica that did not answer still holds the data until it rejoins and the purge is run again; if its node is running but cut off, it can serve follower reads from before the purge until it reconnects",
	"a raft snapshot that was being sent during the purge carries what its range held when it began",
	"the raft log of a range the rows were written to before a split moved them is truncated on that range's own schedule",
	"blocks freed by deleting or rewriting a file are the file system's and the device's to reuse or discard",
}

const (
	// purgeStepTimeout bounds one step on one node: a compaction of a
	// large span is slow, an unreachable node should not be waited on
	// for long.
	purgeStepTimeout = 2 * time.Minute
	// purgeLeaderRounds is how many times the replicas of a range are
	// asked in turn for its leader before the range is given up on (an
	// election in flight settles within a few).
	purgeLeaderRounds = 20
)

// PurgeSpan purges [start, end), which must lie in the table key space.
func (n *Node) PurgeSpan(ctx context.Context, start, end keys.Key) (*kvpb.PurgeReport, error) {
	if n.ClusterVersion() < version.V21 {
		return nil, fmt.Errorf("purge needs cluster version %s (finalize the upgrade first)", version.V21)
	}
	if start.Compare(end) >= 0 {
		return nil, errors.New("purge: the span is empty")
	}
	if !start.HasPrefix(keys.TablePrefix) || len(start) <= len(keys.TablePrefix) ||
		end.Compare(keys.TablePrefix.PrefixEnd()) > 0 {
		// The system and meta spans hold the cluster's own history —
		// descriptors as of a past time, range addressing — and a
		// threshold of now on those refuses work the cluster does to
		// itself.
		return nil, errors.New("purge: the span must lie inside table data")
	}
	opID := newOpID()
	n.events.RecordStart("purge", opID, "purging %s to %s", keys.Pretty(start), keys.Pretty(end))
	rep, err := n.purgeSpan(ctx, start, end)
	if err != nil {
		n.events.RecordEnd("purge", opID, "error", "purge of %s to %s failed: %v", keys.Pretty(start), keys.Pretty(end), err)
		log.Audit("purge", "start", keys.Pretty(start), "end", keys.Pretty(end), "outcome", "error", "error", err.Error())
		return nil, err
	}
	outcome := "ok"
	if !rep.Complete {
		outcome = "incomplete"
	}
	n.events.RecordEnd("purge", opID, outcome, "purged %s to %s: %d version(s) removed over %d range(s), %d replica(s) counted%s",
		keys.Pretty(start), keys.Pretty(end), rep.VersionsRemoved, rep.Ranges, len(rep.Replicas), incompleteSuffix(rep))
	raw, _ := json.Marshal(rep)
	log.Audit("purge", "start", keys.Pretty(start), "end", keys.Pretty(end), "outcome", outcome,
		"purge_ts_nanos", rep.PurgeTSNanos, "versions_removed", rep.VersionsRemoved, "report", string(raw))
	metrics.PurgeVersionsRemoved.Add(float64(rep.VersionsRemoved))
	return rep, nil
}

func incompleteSuffix(rep *kvpb.PurgeReport) string {
	if rep.Complete {
		return ""
	}
	return "; incomplete: " + strings.Join(rep.Incomplete, "; ")
}

func (n *Node) purgeSpan(ctx context.Context, start, end keys.Key) (*kvpb.PurgeReport, error) {
	rep := &kvpb.PurgeReport{StartKey: start, EndKey: end, NotCovered: purgeNotCovered, Replicas: []kvpb.PurgeReplicaReport{}}
	for _, k := range []keys.Key{start, end} {
		if k.Compare(keys.TablePrefix.PrefixEnd()) >= 0 {
			continue // the end of the key space is a boundary already
		}
		if _, err := n.db.AdminSplit(ctx, k); err != nil {
			return nil, fmt.Errorf("splitting at %s so the purge stays inside its span: %w", keys.Pretty(k), err)
		}
	}
	ranges, err := n.rangesTiling(ctx, start, end)
	if err != nil {
		return nil, err
	}
	rep.Ranges = len(ranges)
	// The backups in progress hold the collection below the oldest of
	// them (backup_protect.go).
	limit, held, err := n.purgeHold(ctx)
	if err != nil {
		return nil, err
	}
	nodes := map[base.NodeID]bool{}
	for i := 0; i < len(ranges); i++ {
		desc := ranges[i]
		ans, err := n.purgeRangeOnLeader(ctx, desc, start, end, limit, len(held) > 0)
		if err != nil {
			rep.Incomplete = append(rep.Incomplete, fmt.Sprintf("%s was not collected: %v", desc.RangeID, err))
			rep.Replicas = append(rep.Replicas, kvpb.PurgeReplicaReport{RangeID: desc.RangeID, Error: "range not collected: " + err.Error()})
			continue
		}
		if lo, hi, ok := purgeRemainder(desc, ans.Desc, end); ok {
			// A split applied after the listing and before the collection
			// (a load-based split): the right-hand side left with the
			// threshold it had then, and is purged on its own.
			more, err := n.rangesTiling(ctx, lo, hi)
			switch {
			case err != nil:
				rep.Incomplete = append(rep.Incomplete, fmt.Sprintf("%s split during the purge and %s to %s was not collected: %v", desc.RangeID, keys.Pretty(lo), keys.Pretty(hi), err))
			case len(ranges)+len(more) > purgeMaxRanges:
				rep.Incomplete = append(rep.Incomplete, fmt.Sprintf("%s to %s kept splitting during the purge and was not collected", keys.Pretty(lo), keys.Pretty(hi)))
			default:
				ranges = append(ranges, more...)
				rep.Ranges += len(more)
			}
		}
		rep.VersionsRemoved += ans.Removed
		if ans.ThresholdNanos > rep.PurgeTSNanos {
			rep.PurgeTSNanos = ans.ThresholdNanos
		}
		for _, replica := range ans.Desc.Replicas {
			nodes[replica.NodeID] = true
			rr := kvpb.PurgeReplicaReport{RangeID: desc.RangeID, NodeID: replica.NodeID}
			resp, err := n.purgeCall(ctx, replica.NodeID, cluster.AdminRequest{Op: "purge-replica", RangeID: desc.RangeID, Index: ans.AppliedIndex})
			switch {
			case err != nil:
				rr.Error = err.Error()
			case resp.PurgeReplica == nil:
				rr.Error = "no report in the answer"
			default:
				rr = *resp.PurgeReplica
			}
			switch {
			case rr.Error != "":
				rep.Incomplete = append(rep.Incomplete, fmt.Sprintf("%s on n%d is unconfirmed: %s", desc.RangeID, replica.NodeID, rr.Error))
			case rr.Garbage+rr.Tombstones+rr.Intents > 0:
				rep.Incomplete = append(rep.Incomplete, fmt.Sprintf("%s on n%d still holds %d superseded version(s), %d tombstone(s) and %d intent(s)",
					desc.RangeID, replica.NodeID, rr.Garbage, rr.Tombstones, rr.Intents))
			}
			rep.Replicas = append(rep.Replicas, rr)
		}
	}
	ids := make([]base.NodeID, 0, len(nodes))
	for id := range nodes {
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	for _, id := range ids {
		if _, err := n.purgeCall(ctx, id, cluster.AdminRequest{Op: "purge-scrub"}); err != nil {
			rep.Incomplete = append(rep.Incomplete, fmt.Sprintf("n%d did not retire its write-ahead logs: %v", id, err))
		}
	}
	if len(held) > 0 {
		for _, p := range held {
			rep.HeldBy = append(rep.HeldBy, p.String())
		}
		rep.HeldBelowNanos = held[0].TS.WallTime
		rep.Rerun = n.schedulePurgeRerun(start, end, held)
		line := fmt.Sprintf("held back by %d backup(s) in progress: versions deleted before %s are gone, and those deleted since are still on disk until they end",
			len(held), describeBackupTS(held[0].TS))
		if rep.Rerun {
			line += "; this node purges the span again then"
		} else {
			line += "; purge again then"
		}
		rep.Incomplete = append([]string{line}, rep.Incomplete...)
	}
	rep.Complete = len(rep.Incomplete) == 0
	return rep, nil
}

// purgeMaxRanges bounds how many ranges one purge collects, the ranges
// split off during it included, so a span that never stops splitting
// ends the purge (reported incomplete) rather than holding it forever.
const purgeMaxRanges = 100000

// purgeRemainder is the part of listed, clipped to end, that the range
// as its collection left it (collected) no longer covers: a split that
// applied in between keeps the left-hand side under the range's ID and
// hands [collected.EndKey, listed.EndKey) to a new range.
func purgeRemainder(listed, collected kvpb.RangeDescriptor, end keys.Key) (lo, hi keys.Key, ok bool) {
	hi = listed.EndKey
	if end.Compare(hi) < 0 {
		hi = end
	}
	if collected.EndKey.Compare(hi) >= 0 {
		return nil, nil, false
	}
	return collected.EndKey, hi, true
}

// rangesTiling returns the ranges that cover [start, end), in key order,
// and fails unless they cover it exactly: a range missing from a stale
// listing would be a part of the span the purge silently skipped.
func (n *Node) rangesTiling(ctx context.Context, start, end keys.Key) ([]kvpb.RangeDescriptor, error) {
	var lastErr error
	for attempt := 0; attempt < 20; attempt++ {
		if attempt > 0 {
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-time.After(100 * time.Millisecond):
			}
		}
		all, err := n.listRanges(ctx)
		if err != nil {
			lastErr = err
			continue
		}
		var in []kvpb.RangeDescriptor
		for _, d := range all {
			if d.StartKey.Compare(end) < 0 && start.Compare(d.EndKey) < 0 {
				in = append(in, d)
			}
		}
		sort.Slice(in, func(i, j int) bool { return in[i].StartKey.Compare(in[j].StartKey) < 0 })
		lastErr = nil
		at := start
		for _, d := range in {
			if !d.StartKey.Equal(at) {
				lastErr = fmt.Errorf("the range listing does not line up at %s", keys.Pretty(at))
				break
			}
			at = d.EndKey
		}
		if lastErr == nil && !at.Equal(end) {
			lastErr = fmt.Errorf("the range listing ends at %s, short of %s", keys.Pretty(at), keys.Pretty(end))
		}
		if lastErr == nil {
			return in, nil
		}
	}
	return nil, fmt.Errorf("purge: could not list the span's ranges: %w", lastErr)
}

// purgeRangeOnLeader finds the range's leader among its replicas and has
// it run its step, collecting no higher than limit. held says a backup
// in progress set limit: then a leader from before backup protection,
// which would collect at its own now, is not asked at all. Otherwise
// such a leader is asked as it was before (purge-range), and only the
// race with a backup starting at that moment is uncovered while the
// cluster is mixed.
func (n *Node) purgeRangeOnLeader(ctx context.Context, desc kvpb.RangeDescriptor, start, end keys.Key, limit hlc.Timestamp, held bool) (*cluster.PurgeRangeAnswer, error) {
	answered := func(resp cluster.AdminResponse) bool { return resp.PurgeRange != nil }
	req := cluster.AdminRequest{Op: "purge-range-below", RangeID: desc.RangeID, Key: start, EndKey: end, PurgeBelow: &limit}
	resp, err := n.leaderCall(ctx, desc, req, answered)
	if err != nil && strings.Contains(err.Error(), "unknown admin op") {
		if held {
			return nil, fmt.Errorf("a node of the range runs a version from before backup protection and would purge through the backup in progress: %w", err)
		}
		req.Op, req.PurgeBelow = "purge-range", nil
		resp, err = n.leaderCall(ctx, desc, req, answered)
	}
	if err != nil {
		return nil, err
	}
	return resp.PurgeRange, nil
}

// leaderCall asks the range's replicas in turn to run a leader's step (a
// purge's collection, a range clear) until the leader answers with a
// result (answered reports whether resp carries one).
func (n *Node) leaderCall(ctx context.Context, desc kvpb.RangeDescriptor, req cluster.AdminRequest, answered func(cluster.AdminResponse) bool) (cluster.AdminResponse, error) {
	var lastErr error
	for round := 0; round < purgeLeaderRounds; round++ {
		for _, replica := range desc.Replicas {
			resp, err := n.purgeCallRaw(ctx, replica.NodeID, req)
			if err != nil {
				// The node is unreachable: a replica that is down is not
				// the leader; ask the others.
				lastErr = err
				continue
			}
			if resp.Error == "" && answered(resp) {
				return resp, nil
			}
			err = errors.New(resp.Error)
			if resp.Error == "" {
				err = errors.New("no result in the answer")
			}
			if !strings.Contains(err.Error(), kvserver.ErrPurgeNotLeader.Error()) {
				return cluster.AdminResponse{}, err
			}
			lastErr = err
		}
		select {
		case <-ctx.Done():
			return cluster.AdminResponse{}, ctx.Err()
		case <-time.After(250 * time.Millisecond):
		}
	}
	return cluster.AdminResponse{}, fmt.Errorf("no replica answered as leader: %w", lastErr)
}

// purgeCall runs one purge step on a node: here directly, elsewhere over
// the admin channel.
func (n *Node) purgeCall(ctx context.Context, id base.NodeID, req cluster.AdminRequest) (cluster.AdminResponse, error) {
	resp, err := n.purgeCallRaw(ctx, id, req)
	if err != nil {
		return resp, err
	}
	if resp.Error != "" {
		return resp, errors.New(resp.Error)
	}
	return resp, nil
}

// purgeCallRaw is purgeCall with the step's own error left in the answer:
// the error it returns is the node's being out of reach.
func (n *Node) purgeCallRaw(ctx context.Context, id base.NodeID, req cluster.AdminRequest) (cluster.AdminResponse, error) {
	ctx, cancel := context.WithTimeout(ctx, purgeStepTimeout)
	defer cancel()
	var resp cluster.AdminResponse
	if id == n.ident.NodeID {
		return n.serveAdminOp(ctx, req), nil
	}
	addr, err := n.resolvePeer(id)
	if err != nil {
		return resp, err
	}
	err = n.trans.Call(ctx, addr, "admin", req, &resp)
	return resp, err
}

// servePurgeStep answers the three steps a purge asks of a node.
func (n *Node) servePurgeStep(ctx context.Context, req cluster.AdminRequest) cluster.AdminResponse {
	switch req.Op {
	case "purge-range", "purge-range-below":
		var limit hlc.Timestamp
		if req.Op == "purge-range-below" {
			if req.PurgeBelow == nil || req.PurgeBelow.IsEmpty() {
				return cluster.AdminResponse{Error: "purge-range-below names no limit"}
			}
			limit = *req.PurgeBelow
		}
		res, err := n.store.PurgeRangeBelow(ctx, req.RangeID, keys.Key(req.Key), keys.Key(req.EndKey), limit)
		if err != nil {
			return cluster.AdminResponse{Error: err.Error()}
		}
		return cluster.AdminResponse{PurgeRange: &cluster.PurgeRangeAnswer{
			Desc: res.Desc, ThresholdNanos: res.Threshold.WallTime, Removed: int64(res.Removed), AppliedIndex: res.AppliedIndex,
		}}
	case "purge-replica":
		rr, err := n.store.PurgeReplica(ctx, req.RangeID, req.Index)
		if err != nil {
			return cluster.AdminResponse{Error: err.Error()}
		}
		return cluster.AdminResponse{PurgeReplica: &rr}
	default: // purge-scrub
		if err := n.store.ScrubLogs(ctx); err != nil {
			return cluster.AdminResponse{Error: err.Error()}
		}
		return cluster.AdminResponse{}
	}
}

// tableIDByName resolves [database.]table to its descriptor ID.
func (n *Node) tableIDByName(ctx context.Context, name string) (uint64, error) {
	cat, err := n.catalogAccessor()
	if err != nil {
		return 0, err
	}
	var id uint64
	err = n.db.RunTxn(ctx, "purge-lookup", func(ctx context.Context, txn *kvclient.Txn) error {
		desc, err := cat.Lookup(ctx, txn, name)
		if err != nil {
			return err
		}
		if desc.IsView() {
			return fmt.Errorf("%q is a view", name)
		}
		id = desc.ID
		return nil
	})
	return id, err
}
