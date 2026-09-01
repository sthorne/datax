package server

import (
	"context"
	"encoding/json"
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
// key span to the GC TTL implied by timeseries tables' retention options.
// The table set is refreshed from the descriptors (a KV scan) at most every
// retentionRefreshInterval, and served stale on scan errors — GC policy may
// lag a CREATE/DROP by one interval, which is harmless (retention is
// best-effort aging, not a deadline).
type retentionProvider struct {
	node       *Node // node.db is set after the store exists; resolve lazily
	defaultTTL time.Duration

	mu        sync.Mutex
	fetchedAt time.Time
	spans     []retentionSpan
}

type retentionSpan struct {
	start, end keys.Key
	ttl        time.Duration
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
//   - the range partially overlaps any retention span (it also holds
//     other data) → max(default TTL, every overlapping retention) with
//     ordinary survivor-keeping GC — a mixed range is never GC'd earlier
//     than any of its tenants allows, and never expires rows;
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
			return ts.ttl, true, true
		}
		overlaps = true
		if ts.ttl > maxTTL {
			maxTTL = ts.ttl
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
	lo, hi := keys.TableDescSpan()
	rows, err := p.node.db.Scan(ctx, lo, hi, 0)
	if err != nil {
		log.Debugf("retention refresh: descriptor scan: %v", err)
		return p.spans // stale-serve
	}
	var spans []retentionSpan
	for _, kv := range rows {
		var d catalog.TableDescriptor
		if json.Unmarshal(kv.Value, &d) != nil || !d.Timeseries || d.RetentionSeconds <= 0 || d.ID == 0 {
			continue
		}
		start, end := keys.TableDataSpan(d.ID)
		rs := retentionSpan{start: start, end: end, ttl: time.Duration(d.RetentionSeconds) * time.Second}
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
		spans = append(spans, rs)
	}
	p.spans, p.fetchedAt = spans, time.Now()
	return p.spans
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
