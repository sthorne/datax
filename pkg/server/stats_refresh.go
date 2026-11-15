package server

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/sthorne/datax/pkg/keys"
	"github.com/sthorne/datax/pkg/kvclient"
	"github.com/sthorne/datax/pkg/kvpb"
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
			// What the sampler keeps belongs to the leader: should this
			// node lead again, it resumes from the last leader's record.
			n.statsRefresh.mu.Lock()
			n.statsRefresh.follow = nil
			n.statsRefresh.mu.Unlock()
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
// in full every tick, and persisted in the replicated keyspace
// (catalogFollower) so that the first pass of a node that starts or
// takes over range 1's leadership reads what changed since the last
// leader's last record rather than both spans whole.
type statsRefreshState struct {
	mu     sync.Mutex
	follow *catalogFollower
}

// statsState is the sampler's followedState.
type statsState struct {
	// tables are the live tables that hold rows (views excluded).
	tables map[uint64]bool
	// collected is CollectedAt by table id, for every statistics record;
	// 0 for one that does not decode (maximally stale).
	collected map[uint64]int64
}

// statsResumeVersion is the format of the sampler's resume record
// (keys.CatalogResumeKey("stats")); bump it when what the sampler keeps
// of a descriptor or a statistics record changes.
const statsResumeVersion = 1

// statsResumeEvery is how often the sampler writes its resume record.
// The record holds an entry per table and per statistics record — about
// 60 KB at 5,000 tables — and the sampler, collecting a table a tick,
// changes it every tick, so it is written seldom: the first pass after a
// start reads back the at most ten records collected since.
const statsResumeEvery = 10 * time.Minute

func newStatsFollower(n *Node) *catalogFollower {
	dlo, dhi := keys.TableDescSpan()
	slo, shi := keys.TableStatsSpan()
	fresh := func() followedState {
		s := &statsState{}
		s.reset()
		return s
	}
	return newCatalogFollower(n, []watchSpan{{dlo, dhi}, {slo, shi}}, fresh,
		clusterResume{n, keys.CatalogResumeKey("stats")}, statsResumeVersion, statsResumeEvery)
}

func (s *statsState) reset() {
	s.tables = map[uint64]bool{}
	s.collected = map[uint64]int64{}
}

func (s *statsState) apply(rec kvpb.ExportRecord) bool {
	if id, ok := descIDOfKey(rec.Key); ok {
		delete(s.tables, id)
		var d catalog.TableDescriptor
		if !rec.Deleted && json.Unmarshal(rec.Value, &d) == nil && d.ID != 0 && !d.IsView() {
			s.tables[d.ID] = true
		}
		return true
	}
	if id, ok := keys.TableStatsID(rec.Key); ok {
		delete(s.collected, id)
		if rec.Deleted {
			return true
		}
		var st catalog.TableStatistics
		if json.Unmarshal(rec.Value, &st) == nil {
			s.collected[id] = st.CollectedAt
		} else {
			s.collected[id] = 0 // corrupt: treat as maximally stale
		}
		return true
	}
	return false
}

// encode: the tables, by id, as the count and then the gaps between
// ids; the statistics records the same way, each id followed by how
// long before the newest CollectedAt (written first) it was collected.
func (s *statsState) encode() []byte {
	ids := sortedIDs(s.tables)
	b := binary.AppendUvarint(nil, uint64(len(ids)))
	var prev uint64
	for _, id := range ids {
		b = binary.AppendUvarint(b, id-prev)
		prev = id
	}
	ids = sortedIDs(s.collected)
	var newest int64
	for _, at := range s.collected {
		newest = max(newest, at)
	}
	b = binary.AppendUvarint(b, uint64(len(ids)))
	b = binary.AppendVarint(b, newest)
	prev = 0
	for _, id := range ids {
		b = binary.AppendUvarint(b, id-prev)
		b = binary.AppendUvarint(b, uint64(newest-s.collected[id]))
		prev = id
	}
	return b
}

func (s *statsState) decode(raw []byte) error {
	r := byteReader{b: raw}
	s.reset()
	n := r.uvarint()
	if n > uint64(len(raw)) {
		return fmt.Errorf("resume record: %d tables in %d bytes", n, len(raw))
	}
	var id uint64
	for i := uint64(0); i < n && !r.bad; i++ {
		id += r.uvarint()
		s.tables[id] = true
	}
	n = r.uvarint()
	if n > uint64(len(raw)) {
		return fmt.Errorf("resume record: %d statistics records in %d bytes", n, len(raw))
	}
	newest := r.varint()
	id = 0
	for i := uint64(0); i < n && !r.bad; i++ {
		id += r.uvarint()
		s.collected[id] = newest - int64(r.uvarint())
	}
	if r.bad || len(r.b) != 0 {
		return fmt.Errorf("resume record: malformed statistics state")
	}
	return nil
}

// statsRefreshOnce refreshes the single stalest-eligible table and sweeps
// orphaned statistics keys. Errors are logged, never fatal: statistics
// are advisory.
func (n *Node) statsRefreshOnce(ctx context.Context, staleness time.Duration) {
	sr := &n.statsRefresh
	sr.mu.Lock()
	defer sr.mu.Unlock()
	if sr.follow == nil {
		sr.follow = newStatsFollower(n)
	}
	// What changed since the last pass: an export waits out (pushes) an
	// intent a concurrent DDL or catalog migration left, as the
	// transactional scan this replaced did.
	if _, _, err := sr.follow.pass(ctx); err != nil {
		log.Debugf("stats refresh: catalog read: %v", err)
		return
	}
	state := sr.follow.state.(*statsState)

	now := n.clock.Now().WallTime
	var pickID uint64
	var pickAge int64 = -1
	for id := range state.tables {
		// Missing stats count as CollectedAt 0, which makes age ≈ now —
		// maximally stale, so uncollected tables are always picked first;
		// among equals, the lowest id, as the scan in id order did.
		age := now - state.collected[id]
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
	for id := range state.collected {
		if !state.tables[id] {
			if err := n.db.Delete(ctx, keys.TableStatsKey(id)); err != nil {
				log.Debugf("stats refresh: orphan delete %d: %v", id, err)
				continue
			}
			delete(state.collected, id)
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
		delete(state.tables, pickID) // gone since: the next pass's read says so too
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
	state.collected[pick.ID] = st.CollectedAt
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
