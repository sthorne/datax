package server

import (
	"context"
	"encoding/json"
	"sync"
	"time"

	"github.com/sthorne/datax/pkg/keys"
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
		var d struct {
			ID               uint64 `json:"id"`
			Timeseries       bool   `json:"timeseries"`
			RetentionSeconds int64  `json:"retention_seconds"`
		}
		if json.Unmarshal(kv.Value, &d) != nil || !d.Timeseries || d.RetentionSeconds <= 0 || d.ID == 0 {
			continue
		}
		start, end := keys.TableDataSpan(d.ID)
		spans = append(spans, retentionSpan{start: start, end: end, ttl: time.Duration(d.RetentionSeconds) * time.Second})
	}
	p.spans, p.fetchedAt = spans, time.Now()
	return p.spans
}
