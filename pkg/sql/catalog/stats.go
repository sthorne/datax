package catalog

import (
	"context"
	"encoding/json"
	"time"

	"github.com/sthorne/datax/pkg/keys"
	"github.com/sthorne/datax/pkg/kvclient"
	"github.com/sthorne/datax/pkg/util/log"
)

// TableStatistics is the persisted planning statistics for one table,
// stored as JSON at keys.TableStatsKey(TableID) — deliberately OUTSIDE
// the descriptor, whose key is lease-hot. Like every persisted JSON
// payload the shape is append-only: new fields must be omitempty with a
// safe zero value (pkg/version rule 1), frozen by a golden decode test.
type TableStatistics struct {
	TableID uint64 `json:"table_id"`
	// RowCount is exact as of the collection sweep's frozen timestamp.
	RowCount int64 `json:"row_count"`
	// CollectedAt is the wall-nanos of that frozen timestamp (staleness).
	CollectedAt int64 `json:"collected_at"`
	// Columns carries per-column estimates, keyed by the stable ColumnID
	// (Name is informational only).
	Columns []ColumnStatistics `json:"columns,omitempty"`
}

// ColumnStatistics is one column's estimates from the collection sweep.
type ColumnStatistics struct {
	ID   ColumnID `json:"id"`
	Name string   `json:"name,omitempty"`
	// Distinct is the estimated number of distinct non-NULL values (exact
	// below the sketch capacity).
	Distinct int64 `json:"distinct,omitempty"`
	// Nulls is the exact count of NULL values seen.
	Nulls int64 `json:"nulls,omitempty"`
}

// Column returns the statistics for a column ID, if collected.
func (s *TableStatistics) Column(id ColumnID) (ColumnStatistics, bool) {
	for _, c := range s.Columns {
		if c.ID == id {
			return c, true
		}
	}
	return ColumnStatistics{}, false
}

// statsRefreshInterval bounds how often a gateway re-reads a table's
// statistics key. Statistics inform planning only, so bounded staleness
// is fine — the cache absorbs the background sampler's writes without a
// per-query KV read.
const statsRefreshInterval = 30 * time.Second

type cachedStats struct {
	stats     *TableStatistics // nil = known-absent
	fetchedAt time.Time
}

// EnableStats wires the KV client the statistics cache reads through.
// Without it (bare accessors in unit tests), Stats always reports no
// statistics and the planner uses its structural fallback.
func (a *Accessor) EnableStats(db *kvclient.DB) {
	a.statsMu.Lock()
	a.statsDB = db
	a.statsCache = make(map[uint64]*cachedStats)
	a.statsMu.Unlock()
}

// Stats returns the cached statistics for a table, refreshing from the
// stats key at most every statsRefreshInterval, serving stale on error.
// ok=false means no statistics exist (or the accessor has no KV client):
// callers fall back to structural planning.
func (a *Accessor) Stats(ctx context.Context, tableID uint64) (*TableStatistics, bool) {
	a.statsMu.Lock()
	defer a.statsMu.Unlock()
	if a.statsDB == nil {
		return nil, false
	}
	if c, ok := a.statsCache[tableID]; ok && time.Since(c.fetchedAt) < statsRefreshInterval {
		return c.stats, c.stats != nil
	}
	rctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	raw, err := a.statsDB.Get(rctx, keys.TableStatsKey(tableID))
	cancel()
	if err != nil {
		// Stale-serve: planning quality degrades gracefully; never fail a
		// query over a statistics read.
		if c, ok := a.statsCache[tableID]; ok {
			return c.stats, c.stats != nil
		}
		return nil, false
	}
	entry := &cachedStats{fetchedAt: time.Now()}
	if raw != nil {
		var st TableStatistics
		if jerr := json.Unmarshal(raw, &st); jerr != nil {
			log.Warnf("table %d statistics: corrupt blob ignored: %v", tableID, jerr)
		} else {
			entry.stats = &st
		}
	}
	a.statsCache[tableID] = entry
	return entry.stats, entry.stats != nil
}

// InvalidateStats drops the cached entry so the next Stats call re-reads
// (ANALYZE calls it on its own gateway for read-your-writes; other
// gateways converge within statsRefreshInterval).
func (a *Accessor) InvalidateStats(tableID uint64) {
	a.statsMu.Lock()
	delete(a.statsCache, tableID)
	a.statsMu.Unlock()
}
