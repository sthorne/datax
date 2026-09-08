// ---- Metrics view: /#/metrics?series=a,b&range=1h&nodes=1,2&rate=1&compare=1 ----
// Charts the datax_metrics table through /api/metrics: one chart per
// series with one line per node (categorical slots 1-8 by node ID; a
// ninth node or beyond draws in gray), y from zero for gauges, a
// crosshair readout with every node's exact value at that time, and a
// gap where a node recorded nothing (a restart is a gap, not a fall to
// zero). Counters are charted as rates.
const RANGES = ["15m", "1h", "6h", "24h", "7d"];
const RANGE_SECONDS = { "15m": 900, "1h": 3600, "6h": 21600, "24h": 86400, "7d": 604800 };
// A custom window (issue #206): a drag across any chart selects one, and
// it rides in the route as range=custom&from=<ms>&to=<ms>, so a chart
// narrowed to the eleven minutes around an incident is a link someone
// can paste. The picker shows the window as its own entry while it is
// in force, and choosing a preset returns to one that ends at now. The
// range is still the header's one value shared by every chart on the
// page; only what it can be has grown.
ui.window = null;
// windowQuery is the /api/metrics window every chart fetch uses: the
// custom window's edges, or the preset's length ending at now.
function windowQuery() {
  if (ui.range === "custom" && ui.window) return `from=${ui.window.from}&to=${ui.window.to}`;
  return `since=${RANGE_SECONDS[ui.range] || RANGE_SECONDS["1h"]}s`;
}
// windowStart is where the current window begins, for what draws
// against it before the server has said.
function windowStart() {
  return ui.range === "custom" && ui.window ? ui.window.from : Date.now() - (RANGE_SECONDS[ui.range] || RANGE_SECONDS["1h"]) * 1000;
}
// parseWindowParams reads a custom window off the route, or null when
// the route does not carry a usable one: both edges, in order, no wider
// than the widest preset (the server clamps wider ones, and a picker
// entry that lies about its width would be worse than a preset).
function parseWindowParams(p) {
  const from = Number(p.get("from")), to = Number(p.get("to"));
  if (!Number.isFinite(from) || !Number.isFinite(to) || to <= from || to - from > RANGE_SECONDS["7d"] * 1000) return null;
  return { from, to };
}
function windowLabel(w) { return `${fmtTime(w.from, true)} – ${fmtTime(w.to, true)}`; }
// setWindow applies a selected window to every chart on the page.
function setWindow(from, to) {
  ui.window = { from: Math.floor(from), to: Math.ceil(to) };
  ui.range = "custom";
  pushRoute();
  renderRangePicker();
  refetchCharts();
}
// refetchCharts re-runs whichever charts the current view draws — the
// same dispatch the header's range picker uses.
function refetchCharts() {
  if (ui.view === "metrics") { runNow("metrics"); }
  else if (ui.view === "node") { nv.chartsAt = 0; runNow("node"); }
  else if (ui.view === "sql") { runNow("txnCharts"); }
}
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
