package sql

import (
	"sort"
	"sync"
	"time"

	"github.com/sthorne/datax/pkg/sql/catalog"
)

// Index usage (issue #208).
//
// The table detail view lists every index on a table with its columns,
// uniqueness and build state, and stops exactly short of the question
// anyone reading that list is asking: is anything using it? Every
// secondary index is a second copy of its columns that every write to
// the table maintains, so an unused one is a permanent write tax with
// no reader — and the only way to find one was to reason about the
// workload by hand, and the usual outcome was that nobody dared drop
// anything.
//
// So the executor counts, per (table, index), the scans it opens and
// the rows it reads through each, with the last time it did. The count
// is taken where the scan is opened rather than where the plan is
// chosen, so a cached plan that never runs is not use; and the primary
// index is counted like the rest, so the figures read as a ratio ("3 of
// 4 million reads went through the primary") rather than as a lone
// zero. A unique-index point lookup reads the primary row through the
// index, and is counted against the index only: the question is which
// access path the planner chose, and that one was the index.
//
// The counters are per node and in memory, like the statement shapes
// (#157): the metrics recorder writes them into datax_metrics every
// interval as index.scans{index=table.name} and index.rows{…}, which is
// what survives a restart and what the console sums over its window.

// IndexUse is one index's live accounting on this node.
type IndexUse struct {
	TableID  uint64
	IndexID  uint64
	Scans    uint64
	Rows     uint64
	LastUsed time.Time
}

type indexUsage struct {
	mu sync.Mutex
	m  map[[2]uint64]*IndexUse
}

var indexUsageRegistry = &indexUsage{m: map[[2]uint64]*IndexUse{}}

// noteIndexScan charges one scan opened through index of table.
func noteIndexScan(desc *catalog.TableDescriptor, index uint64) {
	u := indexUsageRegistry
	u.mu.Lock()
	e := u.entry(desc.ID, index)
	e.Scans++
	e.LastUsed = time.Now()
	u.mu.Unlock()
}

// noteIndexRows charges n rows read through index of table.
func noteIndexRows(desc *catalog.TableDescriptor, index uint64, n int) {
	if n <= 0 {
		return
	}
	u := indexUsageRegistry
	u.mu.Lock()
	u.entry(desc.ID, index).Rows += uint64(n)
	u.mu.Unlock()
}

func (u *indexUsage) entry(table, index uint64) *IndexUse {
	k := [2]uint64{table, index}
	e := u.m[k]
	if e == nil {
		e = &IndexUse{TableID: table, IndexID: index}
		u.m[k] = e
	}
	return e
}

// IndexUsageSnapshot returns every index this node has read through
// since it started, in (table, index) order. An index never read is
// absent: the caller knows the catalog and reports the zero.
func IndexUsageSnapshot() []IndexUse {
	u := indexUsageRegistry
	u.mu.Lock()
	out := make([]IndexUse, 0, len(u.m))
	for _, e := range u.m {
		out = append(out, *e)
	}
	u.mu.Unlock()
	sort.Slice(out, func(i, j int) bool {
		if out[i].TableID != out[j].TableID {
			return out[i].TableID < out[j].TableID
		}
		return out[i].IndexID < out[j].IndexID
	})
	return out
}

// ResetIndexUsage forgets every count; for tests, which share the
// process-wide registry.
func ResetIndexUsage() {
	u := indexUsageRegistry
	u.mu.Lock()
	u.m = map[[2]uint64]*IndexUse{}
	u.mu.Unlock()
}
