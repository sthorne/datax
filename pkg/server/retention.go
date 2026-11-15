package server

import (
	"context"
	"encoding/json"
	"sort"
	"sync"
	"time"

	"github.com/sthorne/datax/pkg/keys"
	"github.com/sthorne/datax/pkg/sql/catalog"
	"github.com/sthorne/datax/pkg/sql/rowenc"
	"github.com/sthorne/datax/pkg/sql/types"
	"github.com/sthorne/datax/pkg/util/hlc"
	"github.com/sthorne/datax/pkg/util/log"
)

// retentionProvider backs kvserver's RetentionOverride: it maps a range's
// key span to the GC TTL implied by timeseries tables' retention options
// and by the gc_ttl option any table may carry (issue #295).
// The table set is refreshed from the descriptors at most every
// retentionRefreshInterval — the first time by reading them all, after
// that by reading what changed (descWatcher) — and served stale on errors — GC policy may
// lag a CREATE/DROP by one interval, which is harmless (retention is
// best-effort aging, not a deadline).
type retentionProvider struct {
	node       *Node // node.db is set after the store exists; resolve lazily
	defaultTTL time.Duration

	mu        sync.Mutex
	fetchedAt time.Time
	spans     []retentionSpan
	// byID is spans by table id, kept up to date from watch.
	byID  map[uint64]retentionSpan
	watch *descWatcher
	// edgeSplitAt is when the edges of a table with a GC window were
	// last asked for, by the table's first key (see splitEdges).
	edgeSplitAt map[string]time.Time
}

// edgeSplitInterval is how often a table's edge splits are asked for
// again while one of its ranges still straddles an edge.
const edgeSplitInterval = time.Minute

type retentionSpan struct {
	start, end keys.Key
	ttl        time.Duration
	// expire: the window is a retention (rows older than it go, newest
	// version included). False for a gc_ttl, which is an ordinary GC
	// window of a different length: the newest version of a row stays.
	expire bool
	// Row-level expiry inputs (nil prefixes disables it for this table —
	// see rowExpiry for the eligibility rules).
	prefixes []keys.Key     // primary-index prefixes, every retained generation
	pkFams   []types.Family // PK column families in physical order (last: the timestamp)
}

const retentionRefreshInterval = 30 * time.Second

// override implements kvserver.StoreConfig.RetentionOverride.
//
// Semantics (the conservative direction is always "keep longer"):
//   - the range lies fully inside one retention table's span → that
//     table's TTL with expire=true: rows past retention are collected
//     outright (survivors included);
//   - the range lies fully inside one gc_ttl table's span → that table's
//     window, shorter or longer than the default, with ordinary
//     survivor-keeping GC;
//   - the range partially overlaps any such span (it also holds other
//     data) → max(default TTL, every overlapping window) with ordinary
//     survivor-keeping GC — a mixed range is never GC'd earlier than any
//     of its tenants allows, and never expires rows. This is what keeps
//     one table's short window from reaching another table's history;
//     the statement that sets a gc_ttl splits the table's edges so that
//     its ranges are its own;
//   - no overlap → no override.
func (p *retentionProvider) override(start, end keys.Key) (time.Duration, bool, bool) {
	spans := p.get()
	var maxTTL time.Duration
	overlaps := false
	for _, ts := range spans {
		if start.Compare(ts.end) >= 0 || ts.start.Compare(end) >= 0 {
			continue
		}
		if ts.start.Compare(start) <= 0 && end.Compare(ts.end) <= 0 {
			// Table spans are disjoint: fully inside one means no other
			// span can overlap.
			return ts.ttl, ts.expire, true
		}
		overlaps = true
		if ts.ttl > maxTTL {
			maxTTL = ts.ttl
		}
		if !ts.expire {
			p.splitEdges(ts)
		}
	}
	if !overlaps {
		return 0, false, false
	}
	if p.defaultTTL > maxTTL {
		maxTTL = p.defaultTTL
	}
	return maxTTL, false, true
}

// splitEdges asks, in the background and at most once per
// edgeSplitInterval, for range boundaries at the two ends of a table
// with a GC window, when a range has been found straddling one. The DDL
// that sets a window splits the edges itself, but best-effort, and a
// table that arrives by restore was never split at all; without this its
// window would be set and, on a shared range, never applied.
func (p *retentionProvider) splitEdges(ts retentionSpan) {
	p.mu.Lock()
	if p.edgeSplitAt == nil {
		p.edgeSplitAt = map[string]time.Time{}
	}
	last := p.edgeSplitAt[string(ts.start)]
	due := p.node != nil && p.node.db != nil && time.Since(last) >= edgeSplitInterval
	if due {
		p.edgeSplitAt[string(ts.start)] = time.Now()
	}
	p.mu.Unlock()
	if !due {
		return
	}
	_ = p.node.stopper.RunWorker(func(ctx context.Context) {
		ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
		defer cancel()
		for _, k := range []keys.Key{ts.start, ts.end} {
			if _, err := p.node.db.AdminSplit(ctx, k); err != nil {
				log.Debugf("splitting at %s for a table's GC window: %v", k, err)
			}
		}
	})
}

func (p *retentionProvider) get() []retentionSpan {
	p.mu.Lock()
	defer p.mu.Unlock()
	if time.Since(p.fetchedAt) < retentionRefreshInterval && !p.fetchedAt.IsZero() {
		return p.spans
	}
	if p.node.db == nil {
		return p.spans
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	// What changed since the last refresh, not every descriptor (issue
	// #313): see descWatcher.
	if p.watch == nil {
		p.watch = &descWatcher{n: p.node}
	}
	full, recs, err := p.watch.next(ctx)
	if err != nil {
		log.Debugf("retention refresh: descriptors: %v", err)
		return p.spans // stale-serve
	}
	if full || p.byID == nil {
		p.byID = map[uint64]retentionSpan{}
	}
	changed := full
	for _, rec := range recs {
		id, ok := descIDOfKey(rec.Key)
		if !ok {
			continue
		}
		_, had := p.byID[id]
		delete(p.byID, id)
		if !rec.Deleted {
			var d catalog.TableDescriptor
			if json.Unmarshal(rec.Value, &d) == nil && d.ID != 0 {
				if rs, ok := retentionSpanFor(&d); ok {
					p.byID[id] = rs
					had = true
				}
			}
		}
		changed = changed || had
	}
	if changed {
		spans := make([]retentionSpan, 0, len(p.byID))
		for _, rs := range p.byID {
			spans = append(spans, rs)
		}
		sort.Slice(spans, func(i, j int) bool { return spans[i].start.Compare(spans[j].start) < 0 })
		p.spans = spans
	}
	p.fetchedAt = time.Now()
	return p.spans
}

// retentionSpanFor is what the provider keeps for table d: its window, if
// it has one of its own.
func retentionSpanFor(d *catalog.TableDescriptor) (retentionSpan, bool) {
	start, end := keys.TableDataSpan(d.ID)
	if !d.Timeseries || d.RetentionSeconds <= 0 {
		// The DDL refuses a gc_ttl beside a retention, so the two
		// never compete for one table.
		if d.GCTTLSeconds > 0 {
			return retentionSpan{start: start, end: end, ttl: time.Duration(d.GCTTLSeconds) * time.Second}, true
		}
		return retentionSpan{}, false
	}
	rs := retentionSpan{start: start, end: end, ttl: time.Duration(d.RetentionSeconds) * time.Second, expire: true}
	// Row-level expiry eligibility: the timestamp must be decodable
	// from the key alone (it always is — the last PK column), and the
	// table must carry no secondary indexes (their entries hold no
	// timestamp; expiring rows but not entries would leave dangling
	// entries that index reads treat as corruption). Every retained
	// primary generation — live, mid-reshard shadow, retired — decodes
	// with the same column families, so all of them are eligible.
	if len(d.Indexes) == 0 && len(d.PrimaryKey) > 0 {
		fams := make([]types.Family, 0, len(d.PrimaryKey))
		okFams := true
		for _, colID := range d.PrimaryKey {
			col, ok := d.ColByID(colID)
			if !ok {
				okFams = false
				break
			}
			fams = append(fams, col.Type)
		}
		if okFams && fams[len(fams)-1] == types.Timestamp {
			rs.pkFams = fams
			rs.prefixes = append(rs.prefixes, keys.TableIndexPrefix(d.ID, d.LivePrimaryIndex()))
			if d.Reshard != nil {
				rs.prefixes = append(rs.prefixes, keys.TableIndexPrefix(d.ID, d.Reshard.NewIndexID))
			}
			for _, rl := range d.RetiredLayouts {
				rs.prefixes = append(rs.prefixes, keys.TableIndexPrefix(d.ID, rl.PrimaryIndexID))
			}
		}
	}
	return rs, true
}

// rowExpiry implements kvserver.StoreConfig.RowExpiry: row-level
// retention for ranges that only PARTIALLY overlap retention tables. The
// predicate ages a version out when BOTH the row's own timestamp column
// (decoded from its key) and the version's commit timestamp are older
// than the table's retention — for normal timeseries ingest the two move
// together; a freshly-rewritten old row waits out its write age, and a
// backfilled ancient row expires promptly. Keys that match no eligible
// retention table (foreign tenants of the mixed range, secondary-index
// entries) are never touched.
func (p *retentionProvider) rowExpiry(start, end keys.Key) (func(keys.Key, hlc.Timestamp) bool, bool) {
	spans := p.get()
	var overlapping []retentionSpan
	for _, ts := range spans {
		if start.Compare(ts.end) >= 0 || ts.start.Compare(end) >= 0 {
			continue
		}
		if ts.start.Compare(start) <= 0 && end.Compare(ts.end) <= 0 {
			return nil, false // fully contained: expire-mode GC handles it
		}
		if len(ts.prefixes) > 0 {
			overlapping = append(overlapping, ts)
		}
	}
	if len(overlapping) == 0 {
		return nil, false
	}
	nowNanos := time.Now().UnixNano()
	return func(key keys.Key, vts hlc.Timestamp) bool {
		for _, ts := range overlapping {
			if key.Compare(ts.start) < 0 || key.Compare(ts.end) >= 0 {
				continue
			}
			cutoff := nowNanos - ts.ttl.Nanoseconds()
			if vts.WallTime > cutoff {
				return false // written too recently, whatever the row says
			}
			for _, prefix := range ts.prefixes {
				if !key.HasPrefix(prefix) {
					continue
				}
				rowTS, ok := rowenc.DecodeTrailingTimestamp(key[len(prefix):], ts.pkFams)
				return ok && rowTS <= cutoff
			}
			return false // an index entry or unknown generation: keep
		}
		return false
	}, true
}

// TestingRefreshRetention makes the next GC pass read the table
// descriptors again instead of serving the cached windows, so a test
// need not wait out the refresh interval after changing one.
func (n *Node) TestingRefreshRetention() {
	n.retention.mu.Lock()
	n.retention.fetchedAt = time.Time{}
	n.retention.mu.Unlock()
}

// TestingRetentionSpans makes the provider answer as GC would, and
// returns how many tables carry a window: what a refresh found.
func (n *Node) TestingRetentionSpans() int {
	return len(n.retention.get())
}
