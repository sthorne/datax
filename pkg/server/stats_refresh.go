package server

import (
	"context"
	"encoding/json"
	"time"

	"github.com/sthorne/datax/pkg/keys"
	"github.com/sthorne/datax/pkg/kvclient"
	"github.com/sthorne/datax/pkg/kvpb"
	"github.com/sthorne/datax/pkg/metrics"
	"github.com/sthorne/datax/pkg/sql"
	"github.com/sthorne/datax/pkg/sql/catalog"
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
// statistics are missing or older than StatsStaleness — and the sweep
// itself reads in bounded chunks, so a large cluster refreshes steadily
// instead of stampeding. The same tick deletes statistics blobs whose
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

// statsRefreshOnce refreshes the single stalest-eligible table and sweeps
// orphaned statistics keys. Errors are logged, never fatal: statistics
// are advisory.
func (n *Node) statsRefreshOnce(ctx context.Context, staleness time.Duration) {
	// Load descriptors and existing stats blobs, transactionally: a
	// transactional read pushes any intent a concurrent DDL or catalog
	// migration left on a descriptor instead of failing on it.
	var descKVs, statKVs []kvpb.KeyValue
	if err := n.db.RunTxn(ctx, "stats-refresh-scan", func(ctx context.Context, txn *kvclient.Txn) error {
		dlo, dhi := keys.TableDescSpan()
		var err error
		if descKVs, err = txn.Scan(ctx, dlo, dhi, 0); err != nil {
			return err
		}
		slo, shi := keys.TableStatsSpan()
		statKVs, err = txn.Scan(ctx, slo, shi, 0)
		return err
	}); err != nil {
		log.Debugf("stats refresh: catalog scan: %v", err)
		return
	}
	collected := make(map[uint64]int64, len(statKVs)) // tableID → CollectedAt
	for _, kv := range statKVs {
		id, ok := keys.TableStatsID(kv.Key)
		if !ok {
			continue
		}
		var st catalog.TableStatistics
		if json.Unmarshal(kv.Value, &st) == nil {
			collected[id] = st.CollectedAt
		} else {
			collected[id] = 0 // corrupt: treat as maximally stale
		}
	}

	now := n.clock.Now().WallTime
	live := make(map[uint64]bool, len(descKVs))
	var pick *catalog.TableDescriptor
	var pickAge int64 = -1
	for _, kv := range descKVs {
		var d catalog.TableDescriptor
		if json.Unmarshal(kv.Value, &d) != nil || d.ID == 0 {
			continue
		}
		live[d.ID] = true
		// Missing stats decode as CollectedAt 0, which makes age ≈ now —
		// maximally stale, so uncollected tables are always picked first.
		age := now - collected[d.ID]
		if age < staleness.Nanoseconds() {
			continue
		}
		if age > pickAge {
			d := d
			pick, pickAge = &d, age
		}
	}

	// Orphan backstop: stats blobs for vanished tables.
	for id := range collected {
		if !live[id] {
			if err := n.db.Delete(ctx, keys.TableStatsKey(id)); err != nil {
				log.Debugf("stats refresh: orphan delete %d: %v", id, err)
			}
		}
	}

	if pick == nil {
		return
	}
	st, err := sql.CollectTableStats(ctx, n.db, pick)
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
	metrics.StatsRefreshes.Inc()
	metrics.StatsRowsScanned.Add(float64(st.RowCount))
	log.Debugf("stats refresh: table %q: %d rows", pick.Name, st.RowCount)
}
