package server

import (
	"context"
	"errors"
	"fmt"

	"github.com/sthorne/datax/pkg/cluster"
	"github.com/sthorne/datax/pkg/keys"
	"github.com/sthorne/datax/pkg/util/hlc"
	"github.com/sthorne/datax/pkg/util/log"
	"github.com/sthorne/datax/pkg/version"
)

// A dropped table's span cleared with one replicated command per range
// (issue #316; kvserver.Store.ClearRange, docs/transactions.md, "A
// dropped table's span"). The reclaim job uses it in place of deleting
// the rows one by one once the cluster has finalized v22: below that, a
// node that does not know the command could be a replica of the range.

// ClearSpanResult is what a clear of a span did.
type ClearSpanResult struct {
	Ranges int
	// Keys and Bytes count the stored MVCC keys (versions and tombstones)
	// removed and their bytes.
	Keys, Bytes int64
	// Timestamp is the latest of the ranges' clear times.
	Timestamp hlc.Timestamp
}

// ClearSpan empties [start, end), which must be one dropped table's whole
// data span: split at the span's ends, so that the ranges that follow lie
// inside it (a clear raises a range's GC threshold, as a purge does), then
// a range clear on each range's leader. The leader refuses a span that is
// not a table's whole data span, whose table has no dropped-table record
// or has a descriptor under its id, and a range holding an intent; the
// caller runs it again later. Clearing an empty range again is harmless.
func (n *Node) ClearSpan(ctx context.Context, start, end keys.Key) (ClearSpanResult, error) {
	var res ClearSpanResult
	if n.ClusterVersion() < version.V22 {
		return res, fmt.Errorf("a range clear needs cluster version %s (finalize the upgrade first)", version.V22)
	}
	id, ok := keys.TableIDOf(start)
	if s, e := keys.TableDataSpan(id); !ok || !s.Equal(start) || !e.Equal(end) {
		return res, errors.New("clear: the span must be one table's whole data span")
	}
	for _, k := range []keys.Key{start, end} {
		if k.Compare(keys.TablePrefix.PrefixEnd()) >= 0 {
			continue
		}
		if _, err := n.db.AdminSplit(ctx, k); err != nil {
			return res, fmt.Errorf("splitting at %s so the clear stays inside its span: %w", keys.Pretty(k), err)
		}
	}
	ranges, err := n.rangesTiling(ctx, start, end)
	if err != nil {
		return res, err
	}
	for _, desc := range ranges {
		req := cluster.AdminRequest{Op: "clear-range", RangeID: desc.RangeID, Key: start, EndKey: end}
		resp, err := n.leaderCall(ctx, desc, req, func(resp cluster.AdminResponse) bool { return resp.ClearRange != nil })
		if err != nil {
			return res, fmt.Errorf("%s: %w", desc.RangeID, err)
		}
		a := resp.ClearRange
		res.Ranges++
		res.Keys += a.Keys
		res.Bytes += a.Bytes
		res.Timestamp = res.Timestamp.Forward(a.Timestamp)
	}
	log.Audit("clear-range", "start", keys.Pretty(start), "end", keys.Pretty(end), "table_id", id,
		"ranges", res.Ranges, "keys", res.Keys, "bytes", res.Bytes, "clear_ts", res.Timestamp.String())
	return res, nil
}

// serveClearRange answers a clear's step on a range's leader.
func (n *Node) serveClearRange(ctx context.Context, req cluster.AdminRequest) cluster.AdminResponse {
	res, err := n.store.ClearRange(ctx, req.RangeID, keys.Key(req.Key), keys.Key(req.EndKey))
	if err != nil {
		return cluster.AdminResponse{Error: err.Error()}
	}
	return cluster.AdminResponse{ClearRange: &cluster.ClearRangeAnswer{
		Desc: res.Desc, Timestamp: res.Timestamp, Keys: res.Keys, Bytes: res.Bytes, AppliedIndex: res.AppliedIndex,
	}}
}
