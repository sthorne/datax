// ---- Metrics view: /#/metrics?series=a,b&range=1h&nodes=1,2&rate=1&compare=1 ----
// Charts the datax_metrics table through /api/metrics: one chart per
// series with one line per node (categorical slots 1-8 by node ID; a
// ninth node or beyond draws in gray), y from zero for gauges, a
// crosshair readout with every node's exact value at that time, and a
// gap where a node recorded nothing (a restart is a gap, not a fall to
// zero). Counters are charted as rates.
const RANGES = ["15m", "1h", "6h", "24h", "7d"];
const RANGE_SECONDS = { "15m": 900, "1h": 3600, "6h": 21600, "24h": 86400, "7d": 604800 };
const DEFAULT_SERIES = ["node.leader_qps", "node.cpu_percent", "sql.statements", "store.compaction_debt"];
// The time range is the header's now (ui.range, issue #151), shared with
// the node page's charts and with every rate on the page; this view keeps
// only what is its own.
const mv = { series: [], nodes: null, rate: true, compare: false, catalog: null, data: null };

// applyMetricsParams reads this view's parameters off the route.
function applyMetricsParams(p) {
  if (p.get("series")) mv.series = p.get("series").split(",").filter(Boolean);
  if (mv.series.length === 0) mv.series = DEFAULT_SERIES.slice();
  mv.nodes = p.get("nodes") ? p.get("nodes").split(",").filter(Boolean) : null;
  mv.compare = p.get("compare") === "1";
  mv.rate = p.get("rate") !== "0";
  document.getElementById("compare-toggle").checked = mv.compare;
  loadCatalog().then(renderPickers);
}
// metricsParams is what this view contributes to the URL; the router
// adds the cross-cutting scope and range.
function metricsParams() {
  const p = { series: mv.series.join(","), };
  if (mv.nodes) p.nodes = mv.nodes.join(",");
  if (mv.compare) p.compare = "1";
  if (!mv.rate) p.rate = "0";
  return p;
}
// nodeColor follows the node, not its row: filtering a node out never
// repaints the survivors. Slots 1-8 are the categorical ramp; past that
// there is no distinguishable hue to hand out, and a chart never plots
// two such nodes in one frame (see MAX_LINES).
function nodeColor(id) { id = Number(id); return id >= 1 && id <= 8 ? `var(--series-${id})` : "var(--text-3)"; }
// MAX_LINES is how many lines share one frame before the view facets
// into one chart per node (issue #216). Eight categorical hues that
// hold up across every pair on both themes is not a palette that exists
// to be found — three replacement ramps were validated and all failed
// all-pairs, a four-slot set passes — and the console plots any subset
// of node IDs together (ids are never reused, so a three-node cluster
// can be n2, n6 and n8). So past four lines, and whenever a node past
// n8 would share a frame, the remedy is a frame per node, not a hue.
const MAX_LINES = 4;
function mustFacet(ids) {
  return ids.length > MAX_LINES || (ids.length > 1 && ids.some(id => Number(id) > 8));
}
