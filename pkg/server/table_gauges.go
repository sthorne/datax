package server

import (
	"context"
	"encoding/json"
	"sort"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/sthorne/datax/pkg/keys"
	"github.com/sthorne/datax/pkg/kvclient"
	"github.com/sthorne/datax/pkg/kvpb"
	"github.com/sthorne/datax/pkg/sql/catalog"
)

// The table gauges: ranges, rows and statistics age per table, as the
// recorder's table.* series (written once per cluster, by the leader of
// range 1) and as datax_table_* on /metrics. They used to be set as a
// side effect of building the schema document, which is why every node
// built it on a timer. They are now their own reading of the catalog —
// the descriptors, the statistics and the range list, three scans and no
// document — taken only by a node that has a use for it: the leader of
// range 1 on its recorder tick, and any node when /metrics is scraped
// (issue #311). The cost is still proportional to the number of tables,
// as a series per table must be; it is paid by one node per tick rather
// than by every node several times over.

// tableGauge is one table's reading.
type tableGauge struct {
	Name   string
	Ranges int
	// Stats is nil for a table that has never been analyzed.
	Stats *SchemaStats
}

type tableGaugeCache struct {
	mu     sync.Mutex
	at     time.Time
	gauges []tableGauge
	// refreshing is non-nil while a reading is being taken, and closed
	// when it has been installed.
	refreshing chan struct{}
}

// refreshTableGauges starts a background reading when the last one is
// older than schemaCacheFor, and returns the channel that closes when it
// lands (nil when the cache is fresh or the node is stopping).
func (n *Node) refreshTableGauges() <-chan struct{} {
	c := &n.gauges
	c.mu.Lock()
	if c.refreshing != nil {
		ch := c.refreshing
		c.mu.Unlock()
		return ch
	}
	if !c.at.IsZero() && time.Since(c.at) < schemaCacheFor {
		c.mu.Unlock()
		return nil
	}
	ch := make(chan struct{})
	c.refreshing = ch
	c.mu.Unlock()
	finish := func(gauges []tableGauge, ok bool) {
		c.mu.Lock()
		if ok {
			c.gauges, c.at = gauges, time.Now()
		}
		c.refreshing = nil
		c.mu.Unlock()
		close(ch)
	}
	if err := n.stopper.RunWorker(func(ctx context.Context) {
		gauges, err := n.readTableGauges(ctx)
		// A reading that failed keeps the last one: the series hold
		// their value through a partition rather than vanish.
		finish(gauges, err == nil)
	}); err != nil {
		finish(nil, false)
		return nil
	}
	return ch
}

// cachedTableGauges is the last reading (nil before the first).
func (n *Node) cachedTableGauges() []tableGauge {
	n.gauges.mu.Lock()
	defer n.gauges.mu.Unlock()
	return n.gauges.gauges
}

// readTableGauges takes one reading: every table that owns rows, by
// name.
func (n *Node) readTableGauges(ctx context.Context) ([]tableGauge, error) {
	ctx, cancel := context.WithTimeout(ctx, schemaBuildTimeout)
	defer cancel()
	var descs []*catalog.TableDescriptor
	var stats map[uint64]*catalog.TableStatistics
	err := n.db.RunTxn(ctx, "table-gauges", func(ctx context.Context, txn *kvclient.Txn) error {
		var err error
		if descs, err = catalog.NewAccessor().List(ctx, txn); err != nil {
			return err
		}
		stats, err = readAllTableStats(ctx, txn)
		return err
	})
	if err != nil {
		return nil, err
	}
	ranges, _, err := n.clusterRanges(ctx)
	if err != nil {
		return nil, err
	}
	idx := newRangeIndex(ranges)
	now := n.clock.Now().WallTime
	out := make([]tableGauge, 0, len(descs))
	for _, d := range descs {
		if d.IsView() {
			continue // no rows, no ranges
		}
		g := tableGauge{Name: d.Name, Stats: schemaStats(stats[d.ID], now)}
		start, end := keys.TableDataSpan(d.ID)
		idx.overlapping(start, end, func(kvpb.RangeDescriptor) { g.Ranges++ })
		out = append(out, g)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out, nil
}

// readAllTableStats reads every table's statistics in one scan, keyed by
// table ID.
func readAllTableStats(ctx context.Context, txn *kvclient.Txn) (map[uint64]*catalog.TableStatistics, error) {
	lo, hi := keys.TableStatsSpan()
	rows, err := txn.Scan(ctx, lo, hi, 0)
	if err != nil {
		return nil, err
	}
	out := make(map[uint64]*catalog.TableStatistics, len(rows))
	for _, kv := range rows {
		id, ok := keys.TableStatsID(keys.Key(kv.Key))
		if !ok {
			continue
		}
		var st catalog.TableStatistics
		if json.Unmarshal(kv.Value, &st) == nil {
			out[id] = &st
		}
	}
	return out, nil
}

// schemaStats renders stored statistics as the browser and the gauges
// show them (nil for none).
func schemaStats(st *catalog.TableStatistics, nowWall int64) *SchemaStats {
	if st == nil {
		return nil
	}
	return &SchemaStats{
		RowCount: st.RowCount, CollectedAt: st.CollectedAt / int64(time.Millisecond),
		AgeSeconds: (nowWall - st.CollectedAt) / int64(time.Second),
		Stale:      time.Duration(nowWall-st.CollectedAt) > statsStaleAfter,
	}
}

// rangeIndex answers which ranges overlap a span without comparing the
// span against every range: with a table per span and a range list that
// grows with the tables, the comparison of each against all was the
// quadratic part of a schema document build.
type rangeIndex struct {
	// descs are sorted by start key; maxEnd[i] is the greatest end key
	// among descs[:i+1], so a walk back from a span's end can stop as
	// soon as nothing earlier reaches the span. The /meta listing may
	// briefly hold overlapping records across a split or a merge, which
	// is why "the one range before" is not assumed.
	descs  []kvpb.RangeDescriptor
	maxEnd []keys.Key
}

func newRangeIndex(descs []kvpb.RangeDescriptor) *rangeIndex {
	ri := &rangeIndex{descs: append([]kvpb.RangeDescriptor(nil), descs...)}
	sort.SliceStable(ri.descs, func(i, j int) bool { return ri.descs[i].StartKey.Compare(ri.descs[j].StartKey) < 0 })
	ri.maxEnd = make([]keys.Key, len(ri.descs))
	for i, d := range ri.descs {
		ri.maxEnd[i] = d.EndKey
		if i > 0 && ri.maxEnd[i-1].Compare(d.EndKey) > 0 {
			ri.maxEnd[i] = ri.maxEnd[i-1]
		}
	}
	return ri
}

// overlapping calls f for every range r with r.StartKey < end and
// r.EndKey > start.
func (ri *rangeIndex) overlapping(start, end keys.Key, f func(kvpb.RangeDescriptor)) {
	i := sort.Search(len(ri.descs), func(i int) bool { return ri.descs[i].StartKey.Compare(end) >= 0 })
	for i--; i >= 0 && ri.maxEnd[i].Compare(start) > 0; i-- {
		if ri.descs[i].EndKey.Compare(start) > 0 {
			f(ri.descs[i])
		}
	}
}

// tableGaugeCollector serves datax_table_* from this node's reading of
// the table gauges, taken when /metrics is scraped rather than on a
// timer: a node nobody scrapes reads nothing for it.
type tableGaugeCollector struct {
	node                 *Node
	ranges, rows, age    *prometheus.Desc
	waitForFreshReadings time.Duration
}

func newTableGaugeCollector(n *Node) *tableGaugeCollector {
	return &tableGaugeCollector{
		node: n,
		ranges: prometheus.NewDesc("datax_table_ranges",
			"Ranges covering each table's key space (read when scraped, at most every few seconds).", []string{"table"}, nil),
		rows: prometheus.NewDesc("datax_table_rows",
			"Row count from each table's statistics, as of their collection.", []string{"table"}, nil),
		age: prometheus.NewDesc("datax_table_stats_age_seconds",
			"Age of each table's statistics; the planner falls back to structural estimates without them.", []string{"table"}, nil),
		// A scrape waits this long for a reading in flight before it
		// serves the previous one: long enough for a healthy catalog,
		// short enough that a node cut off from it still answers.
		waitForFreshReadings: 2 * time.Second,
	}
}

func (c *tableGaugeCollector) Describe(ch chan<- *prometheus.Desc) {
	ch <- c.ranges
	ch <- c.rows
	ch <- c.age
}

func (c *tableGaugeCollector) Collect(ch chan<- prometheus.Metric) {
	if landed := c.node.refreshTableGauges(); landed != nil {
		select {
		case <-landed:
		case <-time.After(c.waitForFreshReadings):
		}
	}
	// One series per name: tables of different databases may share one,
	// and a collector may not emit the same label set twice.
	seen := map[string]bool{}
	for _, g := range c.node.cachedTableGauges() {
		if seen[g.Name] {
			continue
		}
		seen[g.Name] = true
		ch <- prometheus.MustNewConstMetric(c.ranges, prometheus.GaugeValue, float64(g.Ranges), g.Name)
		if g.Stats != nil {
			ch <- prometheus.MustNewConstMetric(c.rows, prometheus.GaugeValue, float64(g.Stats.RowCount), g.Name)
			ch <- prometheus.MustNewConstMetric(c.age, prometheus.GaugeValue, float64(g.Stats.AgeSeconds), g.Name)
		}
	}
}
