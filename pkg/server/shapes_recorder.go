package server

import (
	"context"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
	"unicode/utf8"

	"github.com/sthorne/datax/pkg/metrics"
	"github.com/sthorne/datax/pkg/pgwire"
	"github.com/sthorne/datax/pkg/sql/catalog"
	"github.com/sthorne/datax/pkg/sql/parser"
	"github.com/sthorne/datax/pkg/sql/types"
	"github.com/sthorne/datax/pkg/util/log"
	"github.com/sthorne/datax/pkg/version"
)

// Statement shapes over time (issue #209).
//
// The fingerprint accounting (#157) is a bounded in-memory table per
// node: cumulative counters with no time dimension, cleared by a
// restart. It answers "which shape costs the most since this node
// started" and nothing about when. "Was this slow yesterday" and "what
// did the workload look like during the incident" need a time series,
// and the mechanism for one already exists: the metrics recorder writes
// series into a time-series table every interval, /api/metrics serves
// them over a window, and the charts handle per-node lines, gaps and
// counters-as-rates.
//
// So every tick, after the node's own series, the recorder writes the
// node's shapeSeriesTopN busiest shapes by total time — count, total
// time, rows scanned, retries, each a cumulative counter keyed by
// fingerprint — into datax_metrics_shapes, a second time-series table
// with a day's retention: the rows are numerous and go stale fast, and
// retention is per table. A shape that falls out of the top-N stops
// being written, never written as zero, so the chart shows the gap it
// is; a restart shows as the counter's reset.
//
// A fingerprint is not a chart label. What makes a chart readable is
// the shape — the normalised text with every literal replaced by ?,
// which is what the shapes view already shows and which carries no
// literal value by construction. It keeps identifiers, deliberately:
// they are what makes it readable, and a schema whose identifiers are
// themselves data (a table per customer, a column named for a person)
// puts those names in the shape, which is why the text is admin-gated
// on the console and left out of backups unless asked. It is kept in
// datax_statement_shapes, written when a fingerprint is first recorded
// and touched now and then after, cut at shapeTextMax so a very wide
// statement bounds its row and the catalog that reads it. The
// representative statement text, which can carry data, stays in memory,
// admin-gated, and a restart still clears it: persisting it would be a
// different security posture, decided here rather than arrived at.

const (
	// shapeSeriesTopN is how many shapes a node writes per tick: the
	// in-memory table's point is that the full set is unbounded, and
	// persisting all of it would move that problem, not solve it. The
	// in-memory table's own cap (pgwire's stmtShapeMax) does not carry
	// over: the fingerprint table is bounded by this many new rows per
	// tick per node and by the sweep, which a client cycling distinct
	// shapes can fill at that rate and no faster.
	shapeSeriesTopN = 50
	// shapeTextMax is the longest shape text persisted: a statement's
	// width is unbounded where its depth is not, and the text is read
	// shapeTextLimit rows at a time into one document.
	shapeTextMax = 1024
	// shapeSeenFor is how long a fingerprint's row in the shapes table
	// goes before this node touches its last_seen again.
	shapeSeenFor = 10 * time.Minute
	// shapeSweepEvery is how often a node removes shapes not seen for
	// shapeKeepFor from the fingerprint table: the series they key have
	// aged out of the time series long before.
	shapeSweepEvery = time.Hour
	shapeKeepFor    = 48 * time.Hour
)

// ShapesMetricsTableDDL creates the statement-shape time series: the
// metrics table's shape, with a day's retention.
const ShapesMetricsTableDDL = `CREATE TABLE IF NOT EXISTS ` + catalog.ShapesMetricsTableName + ` (
  node INT8 NOT NULL,
  name TEXT NOT NULL,
  at TIMESTAMPTZ NOT NULL,
  value FLOAT8,
  PRIMARY KEY (node, name, at)
) WITH (timeseries = true, retention = '24h', shards = 8)`

// StatementShapesTableDDL creates the fingerprint-to-shape table.
const StatementShapesTableDDL = `CREATE TABLE IF NOT EXISTS ` + catalog.StatementShapesTableName + ` (
  fingerprint TEXT PRIMARY KEY,
  shape TEXT NOT NULL,
  kind TEXT,
  relations TEXT,
  first_seen TIMESTAMPTZ NOT NULL,
  last_seen TIMESTAMPTZ NOT NULL
)`

// shapesRecorder is the per-node state of the shape recorder; the
// recorder loop is one goroutine, and the lock covers the readers of
// ready from the API.
type shapesRecorder struct {
	mu       sync.Mutex
	ready    bool
	seen     map[string]time.Time // fingerprint -> when its row was last touched
	sweptAt  time.Time
	lastWarn time.Time
}

// shapesReady reports whether the shape tables are known to exist.
func (n *Node) shapesReady() bool {
	n.shapes.mu.Lock()
	defer n.shapes.mu.Unlock()
	return n.shapes.ready
}

// recordShapesOnce writes this tick's shape series, once the cluster is
// at the version that knows the tables (rule 4) and the node serves SQL.
func (n *Node) recordShapesOnce(ctx context.Context, at int64) {
	if n.sqlServer() == nil || n.readClusterVersion(ctx) < version.V19 {
		return
	}
	if !n.shapesReady() {
		if err := n.ensureShapesTables(ctx); err != nil {
			log.Debugf("shape recorder: creating the shape tables: %v", err)
			return
		}
	}
	stats, _ := n.sqlServer().Activity().Statements(shapeSeriesTopN)
	rows := shapeSeriesRows(int64(n.ident.NodeID), stats)
	if len(rows) == 0 {
		return
	}
	if err := n.writeMetricRows(ctx, catalog.ShapesMetricsTableName, at, rows); err != nil {
		n.shapesWriteFailed(err)
		return
	}
	if err := n.touchShapes(ctx, stats, time.Unix(0, at)); err != nil {
		n.shapesWriteFailed(err)
		return
	}
	n.sweepShapes(ctx, time.Unix(0, at))
}

func (n *Node) shapesWriteFailed(err error) {
	metrics.MetricsRecordErrors.Inc()
	n.shapes.mu.Lock()
	defer n.shapes.mu.Unlock()
	if now := time.Now(); now.Sub(n.shapes.lastWarn) > time.Minute {
		n.shapes.lastWarn = now
		log.Warnf("shape recorder: write failed (retried next tick): %v", err)
	}
	if strings.Contains(err.Error(), "does not exist") {
		n.shapes.ready = false // dropped or restored away: recreate next tick
	}
}

// shapeSeriesRows is the tick's rows for the given shapes: four
// cumulative counters per shape, keyed by fingerprint. Only the shapes
// given are written — the caller passes the top-N — so a shape that
// fell out is a gap in its series, not a zero.
func shapeSeriesRows(node int64, stats []pgwire.StatementStat) []metricRow {
	rows := make([]metricRow, 0, 4*len(stats))
	for _, st := range stats {
		if st.Fingerprint == "" {
			continue
		}
		rows = append(rows,
			metricRow{node: node, name: labelledSeries("stmt.count", "fp", st.Fingerprint), value: float64(st.Count)},
			metricRow{node: node, name: labelledSeries("stmt.total_us", "fp", st.Fingerprint), value: float64(st.TotalMicros)},
			metricRow{node: node, name: labelledSeries("stmt.rows_scanned", "fp", st.Fingerprint), value: float64(st.RowsScanned)},
			metricRow{node: node, name: labelledSeries("stmt.retries", "fp", st.Fingerprint), value: float64(st.Retries)},
		)
	}
	return rows
}

// ensureShapesTables creates both tables when they are missing.
func (n *Node) ensureShapesTables(ctx context.Context) error {
	sess, err := n.systemSession()
	if err != nil {
		return err
	}
	for _, ddl := range []string{ShapesMetricsTableDDL, StatementShapesTableDDL} {
		stmts, err := parser.Parse(ddl)
		if err != nil {
			return err
		}
		if _, serr := sess.Execute(ctx, stmts[0], nil); serr != nil {
			return serr
		}
	}
	n.shapes.mu.Lock()
	n.shapes.ready = true
	if n.shapes.seen == nil {
		n.shapes.seen = map[string]time.Time{}
	}
	n.shapes.mu.Unlock()
	return nil
}

// touchShapes writes the shape text for every fingerprint this node has
// not touched in shapeSeenFor: an insert for a new one, last_seen for a
// known one. Bounded by the top-N and the interval, so at most a few
// rows a tick.
func (n *Node) touchShapes(ctx context.Context, stats []pgwire.StatementStat, at time.Time) error {
	var due []pgwire.StatementStat
	n.shapes.mu.Lock()
	for _, st := range stats {
		if st.Fingerprint == "" {
			continue
		}
		if last, ok := n.shapes.seen[st.Fingerprint]; !ok || at.Sub(last) >= shapeSeenFor {
			due = append(due, st)
		}
	}
	n.shapes.mu.Unlock()
	if len(due) == 0 {
		return nil
	}
	sess, err := n.systemSession()
	if err != nil {
		return err
	}
	stmts, err := parser.Parse("INSERT INTO " + catalog.StatementShapesTableName +
		" (fingerprint, shape, kind, relations, first_seen, last_seen) VALUES ($1, $2, $3, $4, $5, $5)" +
		" ON CONFLICT (fingerprint) DO UPDATE SET shape = $2, kind = $3, relations = $4, last_seen = $5")
	if err != nil {
		return err
	}
	for _, st := range due {
		tables := append([]string(nil), st.Tables...)
		sort.Strings(tables)
		params := []types.Datum{types.NewString(st.Fingerprint), types.NewString(truncateShape(st.Shape)), types.NewString(st.Kind),
			types.NewString(strings.Join(tables, ",")), types.NewTimestamp(at.UnixNano())}
		if _, serr := sess.Execute(ctx, stmts[0], params); serr != nil {
			return serr
		}
		n.shapes.mu.Lock()
		n.shapes.seen[st.Fingerprint] = at
		n.shapes.mu.Unlock()
	}
	return nil
}

// sweepShapes removes fingerprints no node has seen for shapeKeepFor,
// once an hour: their series left the time series a day earlier.
func (n *Node) sweepShapes(ctx context.Context, at time.Time) {
	n.shapes.mu.Lock()
	due := at.Sub(n.shapes.sweptAt) >= shapeSweepEvery
	if due {
		n.shapes.sweptAt = at
		for fp, last := range n.shapes.seen {
			if at.Sub(last) >= shapeKeepFor {
				delete(n.shapes.seen, fp)
			}
		}
	}
	n.shapes.mu.Unlock()
	if !due {
		return
	}
	sess, err := n.systemSession()
	if err != nil {
		return
	}
	stmts, err := parser.Parse("DELETE FROM " + catalog.StatementShapesTableName + " WHERE last_seen < $1")
	if err != nil {
		return
	}
	if _, serr := sess.Execute(ctx, stmts[0], []types.Datum{types.NewTimestamp(at.Add(-shapeKeepFor).UnixNano())}); serr != nil {
		log.Debugf("shape recorder: sweep: %v", serr)
	}
}

// shapeTexts reads the fingerprint-to-shape map for the catalog and the
// chart titles: the newest shapeTextLimit by last_seen.
const shapeTextLimit = 500

func (n *Node) shapeTexts(ctx context.Context) (map[string]string, error) {
	sess, err := n.systemSession()
	if err != nil {
		return nil, err
	}
	stmts, err := parser.Parse("SELECT fingerprint, shape FROM " + catalog.StatementShapesTableName + " ORDER BY last_seen DESC LIMIT " + strconv.Itoa(shapeTextLimit))
	if err != nil {
		return nil, err
	}
	res, serr := sess.Execute(ctx, stmts[0], nil)
	if serr != nil {
		return nil, serr
	}
	out := make(map[string]string, len(res.Rows))
	for _, row := range res.Rows {
		if len(row) == 2 && !row[0].Null && !row[1].Null {
			out[row[0].S] = row[1].S
		}
	}
	return out, nil
}

// seriesTable is the table a series is recorded in: the shape series
// in datax_metrics_shapes, everything else in datax_metrics.
func seriesTable(base string) string {
	if strings.HasPrefix(base, "stmt.") {
		return catalog.ShapesMetricsTableName
	}
	return catalog.MetricsTableName
}

// truncateShape cuts a shape text at shapeTextMax bytes, on a rune
// boundary, marking the cut.
func truncateShape(text string) string {
	if len(text) <= shapeTextMax {
		return text
	}
	cut := shapeTextMax
	for cut > 0 && !utf8.RuneStart(text[cut]) {
		cut--
	}
	return text[:cut] + "…"
}
