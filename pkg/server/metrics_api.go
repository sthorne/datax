package server

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/sthorne/datax/pkg/sql/catalog"
	"github.com/sthorne/datax/pkg/sql/parser"
	"github.com/sthorne/datax/pkg/sql/types"
)

// /api/metrics — the Metrics view's query endpoint over the datax_metrics
// table. Without `series` it describes what can be asked for; with it,
// it returns aligned [t, v] arrays per node, downsampled server-side to
// at most metricsMaxPoints per series (the average for gauges, the rate
// for counters with &rate=1), served by one index scan per (node,
// series) on the table's (node, name, at) key. Any database user may
// read it, the same rule as /metrics. The same query as SQL:
//
//	SELECT at, value FROM datax_metrics
//	WHERE node = 1 AND name = 'node.cpu_percent' AND at >= '2026-09-04 15:00:00Z'
//	ORDER BY at;

const (
	metricsMaxPoints = 500
	metricsMaxWindow = 30 * 24 * time.Hour
	// metricsScanLimit bounds one (node, series) scan: 30 days of
	// 10-second samples is 259k rows; the LIMIT keeps a misconfigured
	// interval from turning a chart into a table scan.
	metricsScanLimit = 300000
)

// MetricsCatalog is the /api/metrics document without a query: what the
// recorder writes and the label values this node knows.
type MetricsCatalog struct {
	NodeID          int                 `json:"node_id"`
	Enabled         bool                `json:"enabled"`
	IntervalSeconds float64             `json:"interval_seconds"`
	Table           string              `json:"table"`
	Ready           bool                `json:"ready"`
	Series          []MetricSeries      `json:"series"`
	Labels          map[string][]string `json:"labels"`
	Nodes           []int               `json:"nodes"`
}

// MetricsResult is the /api/metrics document for a query.
type MetricsResult struct {
	NodeID int            `json:"node_id"`
	FromMs int64          `json:"from_ms"`
	ToMs   int64          `json:"to_ms"`
	StepMs int64          `json:"step_ms"`
	Series []MetricResult `json:"series"`
}

// MetricResult is one series' points per node: [t_ms, value] pairs in
// time order; buckets with no sample are absent (a restart shows as a
// gap, not as a line to zero).
type MetricResult struct {
	Name  string                  `json:"name"`
	Kind  string                  `json:"kind"`
	Unit  string                  `json:"unit"`
	Group string                  `json:"group"`
	Rate  bool                    `json:"rate,omitempty"`
	Nodes map[string][][2]float64 `json:"nodes"`
}

func (n *Node) serveMetricsAPI(w http.ResponseWriter, req *http.Request) {
	q := req.URL.Query()
	w.Header().Set("Content-Type", "application/json")
	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ")
	if q.Get("series") == "" {
		_ = enc.Encode(n.metricsCatalog())
		return
	}
	res, code, err := n.queryMetrics(req.Context(), q)
	if err != nil {
		w.WriteHeader(code)
		_ = enc.Encode(map[string]string{"error": err.Error()})
		return
	}
	_ = enc.Encode(res)
}

func (n *Node) metricsCatalog() MetricsCatalog {
	doc := MetricsCatalog{
		NodeID:          int(n.ident.NodeID),
		Enabled:         n.metricsRecordInterval() > 0,
		IntervalSeconds: n.metricsRecordInterval().Seconds(),
		Table:           catalog.MetricsTableName,
		Ready:           n.metricsReady.Load(),
		Series:          metricSeriesDefs,
		Labels:          map[string][]string{"peer": {}, "table": {}},
	}
	for _, nd := range n.registry.All() {
		doc.Nodes = append(doc.Nodes, int(nd.NodeID))
		if nd.NodeID != n.ident.NodeID {
			doc.Labels["peer"] = append(doc.Labels["peer"], nd.NodeID.String())
		}
	}
	sort.Ints(doc.Nodes)
	sort.Strings(doc.Labels["peer"])
	if sd := n.cachedSchemaDoc(); sd != nil {
		for _, t := range sd.Tables {
			if !catalog.IsSystemTable(t.Name) {
				doc.Labels["table"] = append(doc.Labels["table"], t.Name)
			}
		}
	}
	return doc
}

// parseWindow reads since/from/to/step into a [from, to) window and a
// step, defaulting to the last hour at whatever step yields at most
// metricsMaxPoints points (never below the recording interval).
func (n *Node) parseWindow(q map[string][]string) (from, to, step time.Duration, err error) {
	get := func(k string) string {
		if v, ok := q[k]; ok && len(v) > 0 {
			return v[0]
		}
		return ""
	}
	now := time.Duration(n.clock.Now().WallTime)
	to = now
	if v := get("to"); v != "" {
		ms, perr := strconv.ParseInt(v, 10, 64)
		if perr != nil {
			return 0, 0, 0, fmt.Errorf("to: want unix milliseconds")
		}
		to = time.Duration(ms) * time.Millisecond
	}
	window := time.Hour
	if v := get("since"); v != "" {
		if window, err = time.ParseDuration(v); err != nil || window <= 0 {
			return 0, 0, 0, fmt.Errorf("since: want a duration like 15m, 1h, 6h, 24h, 168h")
		}
	}
	from = to - window
	if v := get("from"); v != "" {
		ms, perr := strconv.ParseInt(v, 10, 64)
		if perr != nil {
			return 0, 0, 0, fmt.Errorf("from: want unix milliseconds")
		}
		from = time.Duration(ms) * time.Millisecond
	}
	if to <= from {
		return 0, 0, 0, fmt.Errorf("empty window")
	}
	if to-from > metricsMaxWindow {
		from = to - metricsMaxWindow
	}
	interval := n.metricsRecordInterval()
	if interval <= 0 {
		interval = defaultMetricsRecordInterval
	}
	step = (to - from) / metricsMaxPoints
	if v := get("step"); v != "" {
		if step, err = time.ParseDuration(v); err != nil || step <= 0 {
			return 0, 0, 0, fmt.Errorf("step: want a duration like 30s")
		}
	}
	if step < interval {
		step = interval
	}
	// Buckets are whole seconds starting on step boundaries, so every
	// node's points line up with each other and with the recorder's
	// interval-aligned samples; the cap rounds the step up, never down.
	step = step.Truncate(time.Second)
	if step < time.Second {
		step = time.Second
	}
	if (to-from)/step > metricsMaxPoints {
		step = ((to-from)/metricsMaxPoints + time.Second - 1).Truncate(time.Second)
	}
	from = from.Truncate(step)
	return from, to, step, nil
}

func (n *Node) queryMetrics(ctx context.Context, q map[string][]string) (*MetricsResult, int, error) {
	from, to, step, err := n.parseWindow(q)
	if err != nil {
		return nil, http.StatusBadRequest, err
	}
	names := strings.Split(q["series"][0], ",")
	rate := false
	if v, ok := q["rate"]; ok && len(v) > 0 && (v[0] == "1" || v[0] == "true") {
		rate = true
	}
	var nodes []int64
	if v, ok := q["node"]; ok && len(v) > 0 && v[0] != "" {
		for _, s := range strings.Split(v[0], ",") {
			id, perr := strconv.ParseInt(strings.TrimPrefix(strings.TrimSpace(s), "n"), 10, 64)
			if perr != nil {
				return nil, http.StatusBadRequest, fmt.Errorf("node: want node IDs like 1,2,3")
			}
			nodes = append(nodes, id)
		}
	} else {
		for _, nd := range n.registry.All() {
			nodes = append(nodes, int64(nd.NodeID))
		}
		sort.Slice(nodes, func(i, j int) bool { return nodes[i] < nodes[j] })
	}

	res := &MetricsResult{
		NodeID: int(n.ident.NodeID),
		FromMs: from.Milliseconds(), ToMs: to.Milliseconds(), StepMs: step.Milliseconds(),
	}
	sess, err := n.systemSession()
	if err != nil {
		return nil, http.StatusInternalServerError, err
	}
	stmts, err := parser.Parse("SELECT at, value FROM " + catalog.MetricsTableName +
		" WHERE node = $1 AND name = $2 AND at >= $3 AND at < $4 ORDER BY at LIMIT " + strconv.Itoa(metricsScanLimit))
	if err != nil {
		return nil, http.StatusInternalServerError, err
	}
	interval := n.metricsRecordInterval()
	if interval <= 0 {
		interval = defaultMetricsRecordInterval
	}
	for _, name := range names {
		name = strings.TrimSpace(name)
		base, _ := splitSeriesName(name)
		def, ok := metricSeriesByName[base]
		if !ok {
			return nil, http.StatusNotFound, fmt.Errorf("unknown series %q", name)
		}
		mr := MetricResult{Name: name, Kind: def.Kind, Unit: def.Unit, Group: def.Group, Nodes: map[string][][2]float64{}}
		if rate && def.Kind == SeriesCounter {
			mr.Rate = true
			mr.Unit = strings.TrimSuffix(def.Unit, "/s") + "/s"
		}
		targets := nodes
		if def.Cluster {
			targets = []int64{0}
		}
		for _, id := range targets {
			params := []types.Datum{types.NewInt(id), types.NewString(name), types.NewTimestamp(int64(from)), types.NewTimestamp(int64(to))}
			r, serr := sess.Execute(ctx, stmts[0], params)
			if serr != nil {
				if strings.Contains(serr.Error(), "does not exist") {
					return nil, http.StatusServiceUnavailable, fmt.Errorf("the %s table does not exist yet (recording starts once the cluster has finalized v5)", catalog.MetricsTableName)
				}
				return nil, http.StatusInternalServerError, serr
			}
			pts := make([]tsample, 0, len(r.Rows))
			for _, row := range r.Rows {
				if len(row) != 2 || row[0].Null || row[1].Null {
					continue
				}
				pts = append(pts, tsample{t: row[0].I, v: row[1].F})
			}
			out := downsample(pts, int64(from), int64(step), mr.Rate, 3*interval.Nanoseconds())
			if len(out) > 0 {
				mr.Nodes[strconv.FormatInt(id, 10)] = out
			}
		}
		res.Series = append(res.Series, mr)
	}
	return res, http.StatusOK, nil
}

type tsample struct {
	t int64 // unix nanoseconds
	v float64
}

// downsample folds raw samples into step-wide buckets from `from`: the
// average of the values (gauges), or the average of the per-interval
// rates (counters), where a rate is the delta between consecutive
// samples over their spacing; a drop (counter reset: the node restarted)
// or a gap wider than maxGap ends a run instead of yielding a spike.
// Points carry the bucket's start in milliseconds.
func downsample(pts []tsample, from, step int64, rate bool, maxGap int64) [][2]float64 {
	if step <= 0 || len(pts) == 0 {
		return nil
	}
	type acc struct {
		sum float64
		n   int
	}
	buckets := map[int64]*acc{}
	put := func(t int64, v float64) {
		b := (t - from) / step
		a := buckets[b]
		if a == nil {
			a = &acc{}
			buckets[b] = a
		}
		a.sum += v
		a.n++
	}
	if !rate {
		for _, p := range pts {
			put(p.t, p.v)
		}
	} else {
		for i := 1; i < len(pts); i++ {
			dt := pts[i].t - pts[i-1].t
			dv := pts[i].v - pts[i-1].v
			if dt <= 0 || dt > maxGap || dv < 0 {
				continue
			}
			put(pts[i].t, dv/(float64(dt)/1e9))
		}
	}
	keys := make([]int64, 0, len(buckets))
	for b := range buckets {
		keys = append(keys, b)
	}
	sort.Slice(keys, func(i, j int) bool { return keys[i] < keys[j] })
	out := make([][2]float64, 0, len(keys))
	for _, b := range keys {
		a := buckets[b]
		v := a.sum / float64(a.n)
		if math.IsNaN(v) || math.IsInf(v, 0) {
			continue
		}
		out = append(out, [2]float64{float64((from + b*step) / 1e6), v})
	}
	return out
}
