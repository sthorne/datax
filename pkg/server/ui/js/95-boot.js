// ---- Polling: one scheduler owns every periodic fetch (issue #147) ----
// A task names the views that need it and its interval; route() hands
// the scheduler the current view and it starts what that view needs (a
// first fetch at once) and stops the rest, so the Metrics view fetches
// no schema and no statements. A hidden tab polls nothing and fetches
// everything once the moment it is shown. A failing fetch backs off
// exponentially (capped at a minute) and the header says so: how long
// since the last good update, and when the next try is.
const sched = { tasks: new Map(), view: "overview", visible: document.visibilityState !== "hidden" };
function task(name, fn, every, views) { sched.tasks.set(name, { name, fn, every, views: new Set(views), timer: null, failures: 0, running: false, next: 0, lastOK: 0, lastErr: "" }); }
function armTask(t) {
  clearTimeout(t.timer); t.timer = null;
  if (!sched.visible || !t.views.has(sched.view)) return;
  const delay = t.failures ? Math.min(t.every * 2 ** t.failures, 60000) : t.every;
  t.next = Date.now() + delay;
  t.timer = setTimeout(() => runTask(t), delay);
}
async function runTask(t) {
  if (t.running) return;
  clearTimeout(t.timer); t.timer = null;
  t.running = true;
  try { await t.fn(); t.failures = 0; t.lastOK = Date.now(); t.lastErr = ""; }
  catch (err) { t.failures++; t.lastErr = err && err.message || String(err); }
  finally { t.running = false; armTask(t); renderStaleness(); }
}
function runNow(name) { const t = sched.tasks.get(name); if (t) runTask(t); }
function applySchedule(view) {
  sched.view = view;
  for (const t of sched.tasks.values()) {
    if (!t.views.has(view) || !sched.visible) { clearTimeout(t.timer); t.timer = null; continue; }
    if (!t.timer && !t.running) runTask(t);
  }
}
document.addEventListener("visibilitychange", () => {
  sched.visible = document.visibilityState !== "hidden";
  applySchedule(sched.view);
});
// The header's staleness pill: the view's primary task decides.
// The view's primary task decides what the header's staleness pill
// reports: every view has one fetch it is mainly waiting on.
const PRIMARY_TASK = {
  overview: "overview", nodes: "overview", data: "overview", sql: "overview",
  ops: "overview", security: "overview", schema: "schema", metrics: "metrics", node: "node",
};
function renderStaleness() {
  const t = sched.tasks.get(PRIMARY_TASK[sched.view]);
  const el = document.getElementById("hdr-stale");
  if (!t || !t.failures) { el.style.display = "none"; return; }
  const since = t.lastOK ? fmtAgo(Date.now() - t.lastOK) + " ago" : "never";
  const retry = Math.max(0, Math.round((t.next - Date.now()) / 1000));
  el.textContent = `● stale — last updated ${since} · retrying in ${retry}s` + (t.lastErr ? ` (${t.lastErr})` : "");
  el.style.display = "inline";
}
setInterval(renderStaleness, 1000);
// The overview: one document — the cluster, the health problems and the
// event tail — instead of a request per section.
async function pollOverview() {
  const resp = await fetch("/api/overview", { cache: "no-store" });
  // The session lapsed (or the role lost LOGIN) while the page was
  // open: the server has already cleared the cookie, so a reload lands
  // on the sign-in page rather than polling 401s forever (issue #158).
  if (resp.status === 401) { location.reload(); return; }
  if (!resp.ok) throw new Error("HTTP " + resp.status);
  const d = await resp.json();
  lastCluster = d.cluster;
  render(d.cluster);
  if (d.health) renderHealth(d.health); else healthUnavailable((d.errors || {}).health || "no health section");
  // The serving node's event ring rides with the cluster document; the
  // ops, security and overview views all read it (a scoped node other
  // than this one is fetched separately, see pollScopedEvents).
  if (d.events && scopeIsServing()) ingestEvents(d.events.node_id, d.events.events, d.events.latest_seq);
  if (ui.view === "ops") renderOps();
  if (ui.view === "overview") renderRecentOps();
  if (ui.view === "security") renderSecurity(d.cluster);
  // The node serves a newer console than this tab runs (a rolling
  // upgrade under a long-lived tab): offer a reload, once.
  if (d.cluster.console_version && d.cluster.console_version !== CONSOLE_VERSION)
    document.getElementById("hdr-reload").style.display = "inline";
}
// Which views need which fetch. A view starts exactly these and stops
// everything else (issue #147), so the Metrics view fetches no schema
// and the Schema view polls no cluster document.
task("overview", pollOverview, 3000, ["overview", "nodes", "data", "sql", "ops", "security"]);
task("schema", pollSchema, 10000, ["schema", "data", "security"]);
task("activity", pollActivity, 3000, ["sql"]);
task("scopedEvents", pollScopedEvents, 5000, ["ops", "security", "overview"]);
task("tileHistory", pollTileHistory, 30000, ["overview"]);
task("node", pollNode, 5000, ["node"]);
task("metrics", fetchMetrics, 15000, ["metrics"]);

window.addEventListener("hashchange", route);
document.getElementById("compare-toggle").addEventListener("change", ev => {
  mv.compare = ev.target.checked; pushRoute(metricsParams()); renderCharts();
});
// The two view-local filters. Each narrows its own view and writes
// itself into the route, so a filtered view is a link one can share.
document.getElementById("schema-filter").addEventListener("input", ev => {
  setSchemaFilter(ev.target.value);
  pushRoute(schemaFilter ? { q: schemaFilter } : {});
  if (lastSchema) renderSchema(lastSchema);
});
document.getElementById("data-filter").addEventListener("input", ev => {
  setDataFilter(ev.target.value);
  pushRoute(dataFilter ? { q: dataFilter } : {});
  if (lastCluster) renderClusterRanges(lastCluster.ranges || []);
});
document.getElementById("node-filter").addEventListener("input", ev => {
  setNodeFilter(ev.target.value);
  pushRoute(nodeFilter ? { locality: nodeFilter } : {});
  if (lastCluster) renderNodesTable(lastCluster);
});
document.getElementById("events-filter").addEventListener("change", renderOps);

a11yTables();
wireControls();
route();