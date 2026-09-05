package server

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"time"

	dto "github.com/prometheus/client_model/go"

	"github.com/sthorne/datax/pkg/kvserver"
	"github.com/sthorne/datax/pkg/metrics"
	"github.com/sthorne/datax/pkg/sql"
	"github.com/sthorne/datax/pkg/sql/catalog"
	"github.com/sthorne/datax/pkg/sql/parser"
	"github.com/sthorne/datax/pkg/sql/types"
	"github.com/sthorne/datax/pkg/util/log"
	"github.com/sthorne/datax/pkg/version"
)

// The metrics recorder (issue #115): every node writes its own metrics
// into the datax_metrics system table every MetricsRecordInterval, one
// INSERT of a few dozen rows, stamped with the node's HLC wall time
// truncated to the interval so every node's samples align. The table is
// a sharded time-series table with a 7-day retention, created by the
// first node that finds it missing once the cluster has finalized v5.
// History therefore survives restarts, is queryable with plain SQL from
// any client, and the dashboard's Metrics view charts it from one place
// (/api/metrics, metrics_api.go).

const defaultMetricsRecordInterval = 10 * time.Second

// MetricsTableDDL creates the metrics table. IF NOT EXISTS makes the
// nodes' concurrent attempts idempotent.
const MetricsTableDDL = `CREATE TABLE IF NOT EXISTS ` + catalog.MetricsTableName + ` (
  node INT8 NOT NULL,
  name TEXT NOT NULL,
  at TIMESTAMPTZ NOT NULL,
  value FLOAT8,
  PRIMARY KEY (node, name, at)
) WITH (timeseries = true, retention = '7d', shards = 8)`

// Series kinds: a gauge is charted as is; a counter is cumulative and
// the query side differentiates it into a rate.
const (
	SeriesGauge   = "gauge"
	SeriesCounter = "counter"
)

// MetricSeries describes one recorded series for the picker.
type MetricSeries struct {
	// Name is the base name; a labelled series records as name{label=value}.
	Name  string `json:"name"`
	Kind  string `json:"kind"`
	Unit  string `json:"unit"`
	Group string `json:"group"`
	Help  string `json:"help"`
	// Label names the label a labelled series carries (peer, table).
	Label string `json:"label,omitempty"`
	// Cluster marks a series recorded once per cluster (as node 0, by
	// range 1's leader) rather than once per node.
	Cluster bool `json:"cluster,omitempty"`
}

// metricSeriesDefs is the fixed list of series the recorder writes.
var metricSeriesDefs = []MetricSeries{
	{Name: "node.cpu_percent", Kind: SeriesGauge, Unit: "percent", Group: "host", Help: "host CPU busy, all cores"},
	{Name: "node.process_cpu_percent", Kind: SeriesGauge, Unit: "percent", Group: "host", Help: "this process's CPU, of one core"},
	{Name: "node.load1", Kind: SeriesGauge, Unit: "", Group: "host", Help: "one-minute load average"},
	{Name: "node.mem_available", Kind: SeriesGauge, Unit: "bytes", Group: "host", Help: "host memory available"},
	{Name: "node.rss", Kind: SeriesGauge, Unit: "bytes", Group: "host", Help: "this process's resident set"},
	{Name: "store.disk_free", Kind: SeriesGauge, Unit: "bytes", Group: "host", Help: "free space on the store's filesystem"},
	{Name: "store.disk_read_bps", Kind: SeriesGauge, Unit: "bytes/s", Group: "host", Help: "store device read throughput"},
	{Name: "store.disk_write_bps", Kind: SeriesGauge, Unit: "bytes/s", Group: "host", Help: "store device write throughput"},
	{Name: "store.disk_busy_percent", Kind: SeriesGauge, Unit: "percent", Group: "host", Help: "store device utilization"},
	{Name: "node.net_rx_bps", Kind: SeriesGauge, Unit: "bytes/s", Group: "host", Help: "network receive throughput"},
	{Name: "node.net_tx_bps", Kind: SeriesGauge, Unit: "bytes/s", Group: "host", Help: "network transmit throughput"},
	{Name: "node.open_fds", Kind: SeriesGauge, Unit: "", Group: "host", Help: "open file descriptors"},
	{Name: "go.goroutines", Kind: SeriesGauge, Unit: "", Group: "host", Help: "goroutines"},
	{Name: "go.heap_in_use", Kind: SeriesGauge, Unit: "bytes", Group: "host", Help: "Go heap in use"},

	{Name: "store.l0_files", Kind: SeriesGauge, Unit: "", Group: "storage", Help: "L0 sstables"},
	{Name: "store.l0_sublevels", Kind: SeriesGauge, Unit: "", Group: "storage", Help: "L0 sublevels"},
	{Name: "store.compaction_debt", Kind: SeriesGauge, Unit: "bytes", Group: "storage", Help: "estimated compaction debt"},
	{Name: "store.memtable_bytes", Kind: SeriesGauge, Unit: "bytes", Group: "storage", Help: "memtables in memory"},
	{Name: "store.write_stalls", Kind: SeriesCounter, Unit: "", Group: "storage", Help: "Pebble write stalls"},
	{Name: "store.block_cache_bytes", Kind: SeriesGauge, Unit: "bytes", Group: "storage", Help: "block cache in use"},
	{Name: "store.block_cache_hits", Kind: SeriesCounter, Unit: "", Group: "storage", Help: "block cache hits"},
	{Name: "store.block_cache_misses", Kind: SeriesCounter, Unit: "", Group: "storage", Help: "block cache misses"},
	{Name: "store.bloom_hits", Kind: SeriesCounter, Unit: "", Group: "storage", Help: "point reads a bloom filter skipped"},
	{Name: "store.bloom_misses", Kind: SeriesCounter, Unit: "", Group: "storage", Help: "point reads the bloom filters passed through"},
	{Name: "store.debt_gated", Kind: SeriesGauge, Unit: "", Group: "storage", Help: "1 while the compaction-debt gate is latched"},
	{Name: "storage.backpressure", Kind: SeriesCounter, Unit: "", Group: "storage", Help: "writes shed under backpressure"},

	{Name: "node.ranges", Kind: SeriesGauge, Unit: "", Group: "ranges", Help: "replicas held"},
	{Name: "node.leaders", Kind: SeriesGauge, Unit: "", Group: "ranges", Help: "ranges led"},
	{Name: "node.leader_qps", Kind: SeriesGauge, Unit: "/s", Group: "ranges", Help: "requests per second on led ranges"},
	{Name: "node.replica_bytes", Kind: SeriesGauge, Unit: "bytes", Group: "ranges", Help: "bytes held by replicas"},

	{Name: "rpc.rtt_us", Kind: SeriesGauge, Unit: "us", Group: "network", Label: "peer", Help: "smoothed round trip to the peer"},
	{Name: "rpc.clock_offset_us", Kind: SeriesGauge, Unit: "us", Group: "network", Label: "peer", Help: "the peer's clock minus this node's"},
	{Name: "rpc.reachable", Kind: SeriesGauge, Unit: "", Group: "network", Label: "peer", Help: "1 while pings to the peer succeed"},

	{Name: "txn.commits", Kind: SeriesCounter, Unit: "", Group: "transactions", Help: "transactions committed"},
	{Name: "txn.aborts", Kind: SeriesCounter, Unit: "", Group: "transactions", Help: "transactions aborted"},
	{Name: "txn.retries", Kind: SeriesCounter, Unit: "", Group: "transactions", Help: "transaction retries"},
	{Name: "kv.batch_p99_us", Kind: SeriesGauge, Unit: "us", Group: "transactions", Help: "KV batch latency p99 (cumulative histogram)"},

	{Name: "sql.connections", Kind: SeriesGauge, Unit: "", Group: "sql", Help: "open SQL connections"},
	{Name: "sql.idle_in_txn", Kind: SeriesGauge, Unit: "", Group: "sql", Help: "connections idle inside an open transaction"},
	{Name: "sql.statements", Kind: SeriesCounter, Unit: "", Group: "sql", Help: "statements executed"},
	{Name: "sql.serialization_failures", Kind: SeriesCounter, Unit: "", Group: "sql", Help: "statements that ended in 40001"},
	{Name: "sql.rows_scanned", Kind: SeriesCounter, Unit: "", Group: "sql", Help: "rows scanned by SQL"},
	{Name: "sql.plan_cache_hits", Kind: SeriesCounter, Unit: "", Group: "sql", Help: "statement executions that reused a cached plan"},
	{Name: "sql.plan_cache_misses", Kind: SeriesCounter, Unit: "", Group: "sql", Help: "statement executions planned in full"},
	{Name: "sql.p99_us", Kind: SeriesGauge, Unit: "us", Group: "sql", Help: "statement latency p99 over the recent ring"},

	{Name: "table.rows", Kind: SeriesGauge, Unit: "", Group: "tables", Label: "table", Cluster: true, Help: "rows per table (from statistics)"},
	{Name: "table.ranges", Kind: SeriesGauge, Unit: "", Group: "tables", Label: "table", Cluster: true, Help: "ranges per table"},
}

// metricSeriesByName indexes the definitions by base name.
var metricSeriesByName = func() map[string]MetricSeries {
	m := make(map[string]MetricSeries, len(metricSeriesDefs))
	for _, d := range metricSeriesDefs {
		m[d.Name] = d
	}
	return m
}()

// splitSeriesName splits name{label=value} into its base and label value.
func splitSeriesName(name string) (base, label string) {
	if i := strings.IndexByte(name, '{'); i > 0 && strings.HasSuffix(name, "}") {
		lv := name[i+1 : len(name)-1]
		if j := strings.IndexByte(lv, '='); j > 0 {
			return name[:i], lv[j+1:]
		}
		return name[:i], lv
	}
	return name, ""
}

func labelledSeries(base, label, value string) string {
	return fmt.Sprintf("%s{%s=%s}", base, label, value)
}

func (n *Node) metricsRecordInterval() time.Duration {
	if n.cfg.MetricsRecordInterval < 0 {
		return 0
	}
	if n.cfg.MetricsRecordInterval == 0 {
		return defaultMetricsRecordInterval
	}
	return n.cfg.MetricsRecordInterval
}

// metricsRecorderLoop runs the recorder for the node's lifetime.
func (n *Node) metricsRecorderLoop(ctx context.Context) {
	interval := n.metricsRecordInterval()
	if interval == 0 {
		return
	}
	// First tick soon after start so a fresh cluster's charts fill in;
	// aligned ticks thereafter.
	timer := time.NewTimer(interval / 2)
	defer timer.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-timer.C:
		}
		n.recordMetricsOnce(ctx, interval)
		next := interval - time.Duration(time.Now().UnixNano()%int64(interval))
		if next < interval/4 {
			next += interval
		}
		timer.Reset(next)
	}
}

// recordMetricsOnce writes one tick of samples (or skips it, counted).
func (n *Node) recordMetricsOnce(ctx context.Context, interval time.Duration) {
	if n.metricsPaused.Load() {
		return
	}
	// One tick's budget: a multi-range INSERT under a re-shard or a busy
	// store can outlast a short interval; ticks are sequential, so a slow
	// write only delays the next one.
	budget := 2 * interval
	if budget < 5*time.Second {
		budget = 5 * time.Second
	}
	if budget > 30*time.Second {
		budget = 30 * time.Second
	}
	wctx, cancel := context.WithTimeout(ctx, budget)
	defer cancel()
	if n.readClusterVersion(wctx) < version.V5 {
		return // rule 4: nothing creates the table before finalize
	}
	if !n.metricsReady.Load() {
		if err := n.ensureMetricsTable(wctx); err != nil {
			log.Debugf("metrics recorder: creating %s: %v", catalog.MetricsTableName, err)
			return
		}
		n.metricsReady.Store(true)
	}
	if n.engine != nil {
		if overloaded, _ := n.engine.Overloaded(); overloaded {
			metrics.MetricsRecordSkipped.Inc()
			return
		}
	}
	r1, _ := n.store.GetReplica(1)
	leader := r1 != nil && r1.IsLeader()
	at := n.clock.Now().WallTime
	at -= at % interval.Nanoseconds()
	rows := n.sampleMetrics(leader)
	if len(rows) == 0 {
		return
	}
	if err := n.writeMetricRows(wctx, at, rows); err != nil {
		metrics.MetricsRecordErrors.Inc()
		if now := time.Now(); now.Sub(n.metricsLastWarn) > time.Minute {
			n.metricsLastWarn = now
			log.Warnf("metrics recorder: write failed (retried next tick): %v", err)
		}
		if strings.Contains(err.Error(), "does not exist") {
			n.metricsReady.Store(false) // dropped or restored away: recreate next tick
		}
		return
	}
}

// ensureMetricsTable creates the table when it is missing.
func (n *Node) ensureMetricsTable(ctx context.Context) error {
	sess, err := n.systemSession()
	if err != nil {
		return err
	}
	stmts, err := parser.Parse(MetricsTableDDL)
	if err != nil {
		return err
	}
	if _, serr := sess.Execute(ctx, stmts[0], nil); serr != nil {
		return serr
	}
	return nil
}

// systemSession returns a fresh internal root session (sessions are not
// safe for concurrent use; they are cheap).
func (n *Node) systemSession() (*sql.Session, error) {
	cat, err := n.catalogAccessor()
	if err != nil {
		return nil, err
	}
	return sql.NewSystemSession(n.db, cat), nil
}

// metricRow is one sample bound for the table.
type metricRow struct {
	node  int64
	name  string
	value float64
}

// writeMetricRows inserts the tick's rows in one statement (one
// transaction), through the SQL layer so re-shards and retention apply
// exactly as they do for user writes.
func (n *Node) writeMetricRows(ctx context.Context, at int64, rows []metricRow) error {
	sess, err := n.systemSession()
	if err != nil {
		return err
	}
	var b strings.Builder
	b.WriteString("INSERT INTO " + catalog.MetricsTableName + " (node, name, at, value) VALUES ")
	params := make([]types.Datum, 0, 4*len(rows))
	for i, r := range rows {
		if i > 0 {
			b.WriteString(", ")
		}
		p := len(params)
		fmt.Fprintf(&b, "($%d, $%d, $%d, $%d)", p+1, p+2, p+3, p+4)
		params = append(params, types.NewInt(r.node), types.NewString(r.name), types.NewTimestamp(at), types.NewFloat(r.value))
	}
	stmts, err := parser.Parse(b.String())
	if err != nil {
		return err
	}
	if _, serr := sess.Execute(ctx, stmts[0], params); serr != nil {
		return serr
	}
	metrics.MetricsRecordRows.Add(float64(len(rows)))
	return nil
}

// sampleMetrics gathers this tick's rows: every per-node series for this
// node and, when it leads range 1, the cluster-level series as node 0.
func (n *Node) sampleMetrics(leader bool) []metricRow {
	self := int64(n.ident.NodeID)
	var rows []metricRow
	add := func(name string, v float64) { rows = append(rows, metricRow{node: self, name: name, value: v}) }

	if n.sys != nil {
		m := n.sys.Latest()
		if !m.At.IsZero() {
			add("node.cpu_percent", m.CPUPercent)
			add("node.process_cpu_percent", m.ProcessCPUPercent)
			add("node.load1", m.Load1)
			add("node.mem_available", float64(m.MemAvailable))
			add("node.rss", float64(m.RSS))
			add("store.disk_free", float64(m.DiskFree))
			add("store.disk_read_bps", m.DiskReadBytesPS)
			add("store.disk_write_bps", m.DiskWriteBytesPS)
			add("store.disk_busy_percent", m.DiskBusyPercent)
			add("node.net_rx_bps", m.NetRxBytesPS)
			add("node.net_tx_bps", m.NetTxBytesPS)
			add("node.open_fds", float64(m.OpenFDs))
			add("go.goroutines", float64(m.Goroutines))
			add("go.heap_in_use", float64(m.HeapInUse))
		}
	}
	if n.engine != nil {
		sm := n.engine.StorageMetrics()
		add("store.l0_files", float64(sm.L0Files))
		add("store.l0_sublevels", float64(sm.L0Sublevels))
		add("store.compaction_debt", float64(sm.CompactionDebtBytes))
		add("store.memtable_bytes", float64(sm.MemtableBytes))
		add("store.block_cache_bytes", float64(sm.BlockCacheBytes))
		add("store.block_cache_hits", float64(sm.BlockCacheHits))
		add("store.block_cache_misses", float64(sm.BlockCacheMisses))
		add("store.bloom_hits", float64(sm.FilterHits))
		add("store.bloom_misses", float64(sm.FilterMisses))
		add("store.write_stalls", float64(sm.WriteStalls))
		gated := 0.0
		if n.engine.DebtGated() {
			gated = 1
		}
		add("store.debt_gated", gated)
	}
	add("storage.backpressure", counterValue(metrics.StorageBackpressure))

	var ranges, leaders int
	var qps float64
	var bytes int64
	n.store.VisitReplicas(func(r *kvserver.Replica) bool {
		ranges++
		bytes += r.SizeBytes()
		if r.IsLeader() {
			leaders++
			qps += r.QPS()
		}
		return true
	})
	add("node.ranges", float64(ranges))
	add("node.leaders", float64(leaders))
	add("node.leader_qps", qps)
	add("node.replica_bytes", float64(bytes))

	if n.pinger != nil {
		for _, pl := range n.pinger.Snapshot() {
			peer := pl.Peer.String()
			reachable := 0.0
			if pl.Reachable {
				reachable = 1
				add(labelledSeries("rpc.rtt_us", "peer", peer), float64(pl.RTTMicros))
				add(labelledSeries("rpc.clock_offset_us", "peer", peer), float64(pl.OffsetMicros))
			}
			add(labelledSeries("rpc.reachable", "peer", peer), reachable)
		}
	}

	add("txn.commits", counterValue(metrics.TxnCommits))
	add("txn.aborts", counterValue(metrics.TxnAborts))
	add("txn.retries", counterValue(metrics.TxnRetries))
	add("kv.batch_p99_us", histogramQuantile(metrics.KVBatchLatency, 0.99)*1e6)
	add("sql.rows_scanned", counterValue(metrics.SQLRowsScanned))

	if n.pgServer != nil {
		if s := n.pgServer.Activity().Summary(); s != nil {
			var stmts uint64
			for _, c := range s.Statements {
				stmts += c
			}
			add("sql.connections", float64(s.Open))
			add("sql.idle_in_txn", float64(s.IdleInTxn))
			add("sql.statements", float64(stmts))
			add("sql.serialization_failures", float64(s.SerializationFailures))
			add("sql.plan_cache_hits", float64(s.PlanCacheHits))
			add("sql.plan_cache_misses", float64(s.PlanCacheMisses))
			add("sql.p99_us", float64(s.P99Micros))
		}
	}

	if leader {
		n.refreshSchema()
		if doc := n.cachedSchemaDoc(); doc != nil {
			for _, t := range doc.Tables {
				if catalog.IsSystemTable(t.Name) {
					continue
				}
				rows = append(rows, metricRow{node: 0, name: labelledSeries("table.ranges", "table", t.Name), value: float64(t.Ranges)})
				if t.Stats != nil {
					rows = append(rows, metricRow{node: 0, name: labelledSeries("table.rows", "table", t.Name), value: float64(t.Stats.RowCount)})
				}
			}
		}
	}
	return rows
}

// histogramQuantile estimates a quantile from a cumulative Prometheus
// histogram by linear interpolation within the bucket that crosses it
// (the same estimate a `histogram_quantile` over the raw buckets gives).
func histogramQuantile(h interface{ Write(*dto.Metric) error }, q float64) float64 {
	var m dto.Metric
	if err := h.Write(&m); err != nil || m.Histogram == nil {
		return 0
	}
	hist := m.Histogram
	total := float64(hist.GetSampleCount())
	if total == 0 {
		return 0
	}
	rank := q * total
	buckets := hist.GetBucket()
	sort.Slice(buckets, func(i, j int) bool { return buckets[i].GetUpperBound() < buckets[j].GetUpperBound() })
	prevCount, prevBound := 0.0, 0.0
	for _, b := range buckets {
		count, bound := float64(b.GetCumulativeCount()), b.GetUpperBound()
		if count >= rank {
			if count == prevCount || bound == prevBound {
				return bound
			}
			return prevBound + (bound-prevBound)*(rank-prevCount)/(count-prevCount)
		}
		prevCount, prevBound = count, bound
	}
	return prevBound
}
