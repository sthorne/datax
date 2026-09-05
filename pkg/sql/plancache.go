package sql

import (
	"container/list"

	"github.com/sthorne/datax/pkg/metrics"
	"github.com/sthorne/datax/pkg/sql/catalog"
	"github.com/sthorne/datax/pkg/sql/parser"
	"github.com/sthorne/datax/pkg/sql/rowenc"
	"github.com/sthorne/datax/pkg/sql/types"
)

// The plan cache (issue #107). A prepared statement is parsed once, but
// everything after parsing ran on every execution: the view check over
// its table names, the alias strip, the projection, the statistics
// lookup and the access-path choice. Measured with the harness (kv
// point lookups through pgx's extended protocol, one gateway on two
// cores) those were about 8 % of the gateway's CPU — the executor's
// own share is small next to the KV round trip — so the cache is scoped
// to what that measurement supports: the single-table SELECT, UPDATE
// and DELETE paths, which are what an ORM's prepared statements are.
//
// Key: the statement's parsed form (its pointer: a prepared statement
// keeps one for its life, and the wire layer's parse cache hands the
// simple protocol the same one for the same text) and the session's
// current database. Value: the table descriptor the statement resolved
// to, the statistics it planned with, the projection, and the shape of
// a primary-key point lookup (which conjuncts pin which key column) so
// the bounds are bound from the parameters without re-planning.
//
// Invalidation is by identity. The catalog's descriptor cache hands out
// one *TableDescriptor per version (a lease refresh, a DDL in this
// transaction or a dropped-and-recreated table each yield a new one),
// and the statistics cache one *TableStatistics per refresh (ANALYZE
// invalidates; the sampler refreshes every ten minutes): an entry is
// used only while the lookups it would have skipped return the very
// same pointers, so a plan built on a stale descriptor or stale
// statistics is a miss, not a wrong answer. The cache is per session
// (no locking) and bounded (planCacheSize, LRU).
type planCache struct {
	entries map[planKey]*list.Element
	lru     *list.List // front = most recently used
}

type planKey struct {
	stmt parser.Statement
	db   string
}

// planEntry is one cached plan.
type planEntry struct {
	key   planKey
	desc  *catalog.TableDescriptor
	stats *catalog.TableStatistics
	// proj is the resolved projection (selects); stripped records that
	// stripTableAlias ran on the statement.
	proj     []projCol
	stripped bool
	// point is the primary-key point shape: for each primary-key column
	// (the logical key of a sharded table), the index of the WHERE
	// conjunct whose equality pins it; nil when the statement is not a
	// point lookup by shape.
	point []int
}

const planCacheSize = 128

func newPlanCache() *planCache {
	return &planCache{entries: make(map[planKey]*list.Element), lru: list.New()}
}

// get returns the entry for stmt in db, or nil, and counts the outcome.
func (c *planCache) get(stmt parser.Statement, db string) *planEntry {
	if c == nil {
		return nil
	}
	el, ok := c.entries[planKey{stmt: stmt, db: db}]
	if !ok {
		metrics.SQLPlanCacheMisses.Inc()
		return nil
	}
	c.lru.MoveToFront(el)
	return el.Value.(*planEntry)
}

// peek returns the entry for stmt in db without counting (EXPLAIN).
func (c *planCache) peek(stmt parser.Statement, db string) *planEntry {
	if c == nil {
		return nil
	}
	if el, ok := c.entries[planKey{stmt: stmt, db: db}]; ok {
		return el.Value.(*planEntry)
	}
	return nil
}

// valid reports whether the entry still describes what the statement
// would plan against.
func (e *planEntry) valid(desc *catalog.TableDescriptor, stats *catalog.TableStatistics) bool {
	if e.desc == desc && e.stats == stats {
		metrics.SQLPlanCacheHits.Inc()
		return true
	}
	metrics.SQLPlanCacheMisses.Inc()
	return false
}

// put stores (or replaces) the entry, evicting the least recently used
// one past the bound.
func (c *planCache) put(e *planEntry) {
	if c == nil {
		return
	}
	if el, ok := c.entries[e.key]; ok {
		el.Value = e
		c.lru.MoveToFront(el)
		return
	}
	c.entries[e.key] = c.lru.PushFront(e)
	for c.lru.Len() > planCacheSize {
		old := c.lru.Back()
		c.lru.Remove(old)
		delete(c.entries, old.Value.(*planEntry).key)
		metrics.SQLPlanCacheEvictions.Inc()
	}
}

// len is the number of cached plans (tests).
func (c *planCache) len() int {
	if c == nil {
		return 0
	}
	return c.lru.Len()
}

// pkPointShape finds the WHERE conjuncts that pin every primary-key
// column by equality — the shape of a point lookup — independent of the
// parameter values. ok=false: not a point lookup by shape.
func pkPointShape(desc *catalog.TableDescriptor, where []parser.Comparison) ([]int, bool, error) {
	pkCols := desc.PrimaryKey
	if desc.ShardBuckets > 0 && len(pkCols) > 1 {
		pkCols = pkCols[1:]
	}
	shape := make([]int, len(pkCols))
	for i := range shape {
		shape[i] = -1
	}
	for ci, cmp := range where {
		if cmp.Op != "=" || len(cmp.Path) > 0 || cmp.Column == "" {
			continue
		}
		col, ok := desc.Col(cmp.Column)
		if !ok {
			return nil, false, newErrf(CodeUndefinedColumn, "column %q does not exist", cmp.Column)
		}
		if !desc.IsPKCol(col.ID) || col.Hidden {
			continue
		}
		for i, id := range pkCols {
			if id == col.ID && shape[i] < 0 {
				shape[i] = ci
			}
		}
	}
	for _, ci := range shape {
		if ci < 0 {
			return nil, false, nil
		}
	}
	return shape, true, nil
}

// bindPKPoint evaluates a point shape's conjuncts against params into a
// point plan. ok=false when a value cannot be bound (row-dependent,
// un-coercible or NULL): the caller plans the statement in full, as
// pkPointValues would have decided the same way.
func bindPKPoint(desc *catalog.TableDescriptor, shape []int, where []parser.Comparison, params []types.Datum) (accessPlan, bool, error) {
	pkCols := desc.PrimaryKey
	if desc.ShardBuckets > 0 && len(pkCols) > 1 {
		pkCols = pkCols[1:]
	}
	out := make([]types.Datum, len(pkCols))
	for i, ci := range shape {
		col, _ := desc.ColByID(pkCols[i])
		d, err := evalExpr(where[ci].Value, nil, nil, params)
		if err != nil {
			return accessPlan{}, false, nil
		}
		d, cerr := coerceColumn(col, d)
		if cerr != nil || d.Null {
			return accessPlan{}, false, nil
		}
		out[i] = d
	}
	if desc.ShardBuckets > 0 {
		bucket, err := rowenc.ShardBucket(desc, out)
		if err != nil {
			return accessPlan{}, false, err
		}
		out = append([]types.Datum{bucket}, out...)
	}
	return accessPlan{kind: planPKPoint, pkVals: out}, true, nil
}
