package server

import (
	"context"
	"encoding/json"
	"sync"
	"time"

	"github.com/sthorne/datax/pkg/keys"
	"github.com/sthorne/datax/pkg/kvclient"
	"github.com/sthorne/datax/pkg/metrics"
	"github.com/sthorne/datax/pkg/sql"
	"github.com/sthorne/datax/pkg/sql/catalog"
	"github.com/sthorne/datax/pkg/sql/rowseal"
	"github.com/sthorne/datax/pkg/util/log"
)

const (
	defaultStatsRefreshInterval = 60 * time.Second
	defaultStatsStaleness       = 10 * time.Minute
)

// statsRefreshLoop keeps table statistics fresh without operator action.
// Every node runs the loop; only the node currently leading range 1 acts
// (the cluster-singleton idiom the allocator uses). Pacing is deliberate:
// at most ONE table is re-collected per tick — the stalest of those whose
// statistics are missing or older than StatsStaleness — so a large
// cluster refreshes steadily instead of stampeding; and a tick reads
// only the descriptors and statistics records written since the last
// one (#316), so an idle tick reads nothing however many tables there
// are. The same tick deletes statistics blobs whose
// table no longer exists (the DROP TABLE transaction already deletes
// them; this is the backstop).
func (n *Node) statsRefreshLoop(ctx context.Context) {
	interval := n.cfg.StatsRefreshInterval
	if interval < 0 {
		return
	}
	if interval == 0 {
		interval = defaultStatsRefreshInterval
	}
	staleness := n.cfg.StatsStaleness
	if staleness == 0 {
		staleness = defaultStatsStaleness
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
		r1, ok := n.store.GetReplica(1)
		if !ok || !r1.IsLeader() {
			continue
		}
		wctx, cancel := context.WithTimeout(ctx, interval*4)
		n.statsRefreshOnce(wctx, staleness)
		cancel()
	}
}

// statsRefreshState is what the sampler keeps between passes (#316): the
// tables it may collect and when each was last collected, updated from
// the descriptors and statistics records written since the last pass
// (descWatcher, following both spans at one timestamp) instead of read
// in full every tick.
type statsRefreshState struct {
	mu    sync.Mutex
	watch *descWatcher
	// tables are the live tables that hold rows (views excluded).
	tables map[uint64]bool
	// collected is CollectedAt by table id, for every statistics record;
	// 0 for one that does not decode (maximally stale).
	collected map[uint64]int64
}

// statsRefreshOnce refreshes the single stalest-eligible table and sweeps
// orphaned statistics keys. Errors are logged, never fatal: statistics
// are advisory.
func (n *Node) statsRefreshOnce(ctx context.Context, staleness time.Duration) {
	sr := &n.statsRefresh
	sr.mu.Lock()
	defer sr.mu.Unlock()
	if sr.watch == nil {
		dlo, dhi := keys.TableDescSpan()
		slo, shi := keys.TableStatsSpan()
		sr.watch = &descWatcher{n: n, spans: []watchSpan{{dlo, dhi}, {slo, shi}}}
	}
	// What changed since the last pass: an export waits out (pushes) an
	// intent a concurrent DDL or catalog migration left, as the
	// transactional scan this replaced did.
	full, recs, err := sr.watch.next(ctx)
	if err != nil {
		log.Debugf("stats refresh: catalog read: %v", err)
		return
	}
	if full || sr.tables == nil {
		sr.tables = map[uint64]bool{}
		sr.collected = map[uint64]int64{}
	}
	for _, rec := range recs {
		if id, ok := descIDOfKey(rec.Key); ok {
			delete(sr.tables, id)
			var d catalog.TableDescriptor
			if !rec.Deleted && json.Unmarshal(rec.Value, &d) == nil && d.ID != 0 && !d.IsView() {
				sr.tables[d.ID] = true
			}
			continue
		}
		if id, ok := keys.TableStatsID(rec.Key); ok {
			delete(sr.collected, id)
			if rec.Deleted {
				continue
			}
			var st catalog.TableStatistics
			if json.Unmarshal(rec.Value, &st) == nil {
				sr.collected[id] = st.CollectedAt
			} else {
				sr.collected[id] = 0 // corrupt: treat as maximally stale
			}
		}
	}

	now := n.clock.Now().WallTime
	var pickID uint64
	var pickAge int64 = -1
	for id := range sr.tables {
		// Missing stats count as CollectedAt 0, which makes age ≈ now —
		// maximally stale, so uncollected tables are always picked first;
		// among equals, the lowest id, as the scan in id order did.
		age := now - sr.collected[id]
		if age < staleness.Nanoseconds() {
			continue
		}
		if age > pickAge || (age == pickAge && id < pickID) {
			pickID, pickAge = id, age
		}
	}

	// Orphan backstop: stats blobs for vanished tables. Both spans were
	// read as of one timestamp, so a record here and no table means the
	// table was gone at that timestamp.
	for id := range sr.collected {
		if !sr.tables[id] {
			if err := n.db.Delete(ctx, keys.TableStatsKey(id)); err != nil {
				log.Debugf("stats refresh: orphan delete %d: %v", id, err)
				continue
			}
			delete(sr.collected, id)
		}
	}

	if pickID == 0 {
		return
	}
	// The one descriptor the pass collects is read in a transaction, for
	// the same reason.
	var pick *catalog.TableDescriptor
	if err := n.db.RunTxn(ctx, "stats-refresh-desc", func(ctx context.Context, txn *kvclient.Txn) error {
		pick = nil
		raw, err := txn.Get(ctx, keys.TableDescKey(pickID))
		if err != nil || raw == nil {
			return err
		}
		var d catalog.TableDescriptor
		if json.Unmarshal(raw, &d) == nil && d.ID == pickID && !d.IsView() {
			pick = &d
		}
		return nil
	}); err != nil {
		log.Debugf("stats refresh: descriptor %d: %v", pickID, err)
		return
	}
	if pick == nil {
		delete(sr.tables, pickID) // gone since: the next pass's read says so too
		return
	}
	st, err := sql.CollectTableStats(ctx, n.db, rowseal.For(n.cat), pick)
	if err != nil {
		log.Debugf("stats refresh: collect %q: %v", pick.Name, err)
		return
	}
	raw, err := json.Marshal(st)
	if err != nil {
		return
	}
	if err := n.db.Put(ctx, keys.TableStatsKey(pick.ID), raw); err != nil {
		log.Debugf("stats refresh: store %q: %v", pick.Name, err)
		return
	}
	// The next pass reads this write back from the export too; noting it
	// now keeps a pass that fails meanwhile from picking it again.
	sr.collected[pick.ID] = st.CollectedAt
	metrics.StatsRefreshes.Inc()
	metrics.StatsRowsScanned.Add(float64(st.RowCount))
	log.Debugf("stats refresh: table %q: %d rows", pick.Name, st.RowCount)
}

// RunStatsRefreshOnce runs one pass of the statistics sampler with the
// configured (or default) staleness, whether or not this node leads
// range 1: for tests and measurements.
func (n *Node) RunStatsRefreshOnce(ctx context.Context) {
	staleness := n.cfg.StatsStaleness
	if staleness == 0 {
		staleness = defaultStatsStaleness
	}
	n.statsRefreshOnce(ctx, staleness)
}
