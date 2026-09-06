// ---- Routes and the cross-cutting controls (issue #151) ----
// Every view is a real route behind one persistent nav, and the three
// things that used to be trapped inside one view — which node a
// node-scoped panel describes, the time range charts and rates use, and
// the search that is the only practical navigation past a few hundred
// ranges — live in the header where every view reads them.
//
// A route is #/<path>[?params]. The section a health finding points at
// rides in the params (#/nodes?sec=network) rather than in the path:
// the old #sec-storage anchors parsed as an unknown path, fell through
// to the overview, and stopped the other views' polls on the way.
const VIEWS = ["overview", "nodes", "node", "data", "sql", "schema", "metrics", "ops", "security"];
const NAV = [
  ["overview", ""], ["nodes", "nodes"], ["data", "data"], ["sql", "sql"],
  ["schema", "schema"], ["metrics", "metrics"], ["ops", "ops"], ["security", "security"],
];
// The health checks name a section; each maps to the route that now
// shows the figure. "self" resolves to the node that served the page.
const SECTION_ROUTE = {
  nodes: ["nodes", ""], network: ["nodes", "network"], schema: ["schema", ""],
  "cluster-ranges": ["data", ""], storage: ["self", "storage"], events: ["ops", ""],
};
const SECTION_NAMES = {
  nodes: "nodes", network: "network", schema: "schema",
  "cluster-ranges": "ranges", storage: "the node's storage", events: "operations",
};

// ui is the cross-view state: the current route, the node scope every
// node-scoped panel reads, and the time range every chart and rate uses.
const ui = { view: "overview", node: 0, scope: "cluster", range: "1h", params: new URLSearchParams(), scroll: {} };

function parseRoute() {
  const h = location.hash.replace(/^#\/?/, "");
  const q = h.indexOf("?");
  const path = q < 0 ? h : h.slice(0, q);
  const params = new URLSearchParams(q < 0 ? "" : h.slice(q + 1));
  const node = path.match(/^node\/(\d+)$/);
  let view = path === "" ? "overview" : node ? "node" : path;
  if (!VIEWS.includes(view)) view = "overview";
  return { view, node: node ? Number(node[1]) : 0, params };
}

// routeTo builds a hash, carrying the cross-cutting controls so they
// survive a view change and a shared link.
function routeTo(path, params) {
  const p = new URLSearchParams(params || {});
  if (ui.scope !== "cluster") p.set("scope", ui.scope);
  if (ui.range !== "1h") p.set("range", ui.range);
  const q = p.toString();
  return "#/" + path + (q ? "?" + q : "");
}
function go(path, params) { location.hash = routeTo(path, params); }
// pushRoute rewrites the current URL in place (a control changed, not a
// navigation), so Back still steps between views rather than settings.
function pushRoute(params) {
  const p = new URLSearchParams(params || ui.params);
  p.delete("scope"); p.delete("range");
  const path = ui.view === "node" ? "node/" + ui.node : ui.view === "overview" ? "" : ui.view;
  history.replaceState(null, "", routeTo(path, p));
}

// scopeNode is the node a node-scoped panel describes: the id when the
// scope names one, 0 for cluster scope.
function scopeNode() { return ui.scope.startsWith("n") ? Number(ui.scope.slice(1)) : 0; }
// scopeIsServing reports whether the scoped node is the one serving this
// page — the only node whose activity and events need no fan-out.
function scopeIsServing() {
  const id = scopeNode();
  return id === 0 || (lastCluster && id === lastCluster.node_id);
}
function scopeLabel() {
  const id = scopeNode();
  return id ? "n" + id : "the whole cluster";
}

function renderNav() {
  const nav = document.getElementById("nav");
  renderKeyed(nav, NAV.map(([name, path]) => ({
    key: name,
    html: `<a data-key="${name}" href="${routeTo(path)}"${name === ui.view || (name === "nodes" && ui.view === "node") ? ' class="current" aria-current="page"' : ""}>${name}</a>`,
  })));
}

function renderScopePicker() {
  const sel = document.getElementById("scope-picker");
  const nodes = lastCluster ? (lastCluster.nodes || []).map(n => n.node_id).sort((a, b) => a - b) : [];
  const opts = [`<option value="cluster">the whole cluster</option>`].concat(
    nodes.map(id => `<option value="n${id}">n${id}${lastCluster && id === lastCluster.node_id ? " (this node)" : ""}</option>`));
  const html = opts.join("");
  if (sel.dataset.html !== html) { sel.innerHTML = html; sel.dataset.html = html; }
  if (sel.value !== ui.scope) sel.value = ui.scope;
}
function renderRangePicker() {
  const sel = document.getElementById("range-picker-global");
  if (!sel.options.length) sel.innerHTML = RANGES.map(r => `<option value="${r}">${r}</option>`).join("");
  if (sel.value !== ui.range) sel.value = ui.range;
}

// ---- Jump to (⌘K) ----
// n3, r128, a table name, or a locality like rack=b. Past a few hundred
// ranges search is the navigation, so the box resolves the identifiers
// an operator already has in hand rather than making them find a view
// first and filter inside it.
function jumpCandidates(q) {
  q = q.trim();
  if (!q) return [];
  const out = [];
  const node = q.match(/^n?(\d+)$/i);
  if (node) out.push({ label: "node n" + node[1], hint: "node detail", path: "node/" + node[1] });
  const range = q.match(/^r(\d+)$/i);
  if (range) out.push({ label: "range r" + range[1], hint: "replica placement", path: "data", params: { range: range[1] } });
  if (/^\w+=/.test(q)) out.push({ label: "locality " + q, hint: "nodes in it", path: "nodes", params: { locality: q } });
  const lower = q.toLowerCase();
  for (const n of (lastCluster && lastCluster.nodes) || []) {
    if ((n.locality || "").toLowerCase().includes(lower) && !/^\w+=/.test(q)) {
      out.push({ label: "n" + n.node_id, hint: n.locality, path: "node/" + n.node_id });
    }
  }
  for (const t of (lastSchema && lastSchema.tables) || []) {
    if (t.name.toLowerCase().includes(lower)) out.push({ label: t.name, hint: "table", path: "schema", params: { q: t.name } });
  }
  if (!out.length) out.push({ label: q, hint: "search tables and ranges", path: "schema", params: { q } });
  return out.slice(0, 8);
}
let jumpSelected = 0;
function renderJump() {
  const q = document.getElementById("jump-input").value;
  const items = jumpCandidates(q);
  jumpSelected = Math.min(jumpSelected, Math.max(0, items.length - 1));
  renderKeyed(document.getElementById("jump-results"), items.map((it, i) => ({
    key: it.path + JSON.stringify(it.params || {}),
    html: `<li data-key="${esc(it.path + JSON.stringify(it.params || {}))}" class="${i === jumpSelected ? "on" : ""}" role="option" aria-selected="${i === jumpSelected}" data-i="${i}">
      <b>${esc(it.label)}</b> <span class="muted">${esc(it.hint)}</span></li>`,
  })));
  return items;
}
function openJump() {
  const box = document.getElementById("jump");
  box.hidden = false;
  jumpSelected = 0;
  const input = document.getElementById("jump-input");
  input.value = "";
  renderJump();
  input.focus();
}
function closeJump() {
  document.getElementById("jump").hidden = true;
  document.getElementById("jump-open").focus();
}
function wireJump() {
  const input = document.getElementById("jump-input");
  document.getElementById("jump-open").addEventListener("click", openJump);
  document.getElementById("jump").addEventListener("click", ev => { if (ev.target.id === "jump") closeJump(); });
  input.addEventListener("input", () => { jumpSelected = 0; renderJump(); });
  input.addEventListener("keydown", ev => {
    const items = jumpCandidates(input.value);
    if (ev.key === "Escape") { ev.preventDefault(); closeJump(); return; }
    if (ev.key === "ArrowDown") { ev.preventDefault(); jumpSelected = Math.min(jumpSelected + 1, items.length - 1); renderJump(); return; }
    if (ev.key === "ArrowUp") { ev.preventDefault(); jumpSelected = Math.max(jumpSelected - 1, 0); renderJump(); return; }
    if (ev.key === "Enter") {
      ev.preventDefault();
      const it = items[jumpSelected];
      if (it) { closeJump(); go(it.path, it.params); }
    }
  });
  document.getElementById("jump-results").addEventListener("click", ev => {
    const li = ev.target.closest("li");
    if (!li) return;
    const it = jumpCandidates(input.value)[Number(li.dataset.i)];
    if (it) { closeJump(); go(it.path, it.params); }
  });
  document.addEventListener("keydown", ev => {
    if ((ev.metaKey || ev.ctrlKey) && ev.key.toLowerCase() === "k") { ev.preventDefault(); openJump(); }
  });
}

// route applies the hash: it shows one view, remembers where each was
// scrolled to, starts exactly the polls that view needs (#147) and
// scrolls to a named section when a health finding sent the reader here.
let routedOnce = false;
function route() {
  const r = parseRoute();
  if (routedOnce) ui.scroll[ui.view + "/" + ui.node] = window.scrollY;
  const scope = r.params.get("scope");
  if (scope && (scope === "cluster" || /^n\d+$/.test(scope))) ui.scope = scope;
  const range = r.params.get("range");
  if (range && RANGE_SECONDS[range]) ui.range = range;
  ui.view = r.view; ui.node = r.node; ui.params = r.params;
  for (const v of VIEWS) {
    const el = document.getElementById("view-" + v);
    if (el) el.hidden = v !== r.view;
  }
  renderNav();
  renderScopePicker();
  renderRangePicker();
  if (r.view === "metrics") applyMetricsParams(r.params);
  if (r.view === "data" && r.params.get("range")) openRangeDetail(Number(r.params.get("range")));
  if (r.view === "schema" && r.params.get("q") !== null) setSchemaFilter(r.params.get("q"));
  if (r.view === "nodes" && r.params.get("locality")) setNodeFilter(r.params.get("locality"));
  applySchedule(r.view);
  // Restore where this view was, or take the reader to the section a
  // health finding named.
  const sec = r.params.get("sec");
  const target = sec && document.getElementById("sec-" + sec);
  if (target) target.scrollIntoView({ block: "start" });
  else window.scrollTo(0, ui.scroll[r.view + "/" + r.node] || 0);
  routedOnce = true;
}

// wireControls binds the header's cross-cutting controls. Changing one
// rewrites the URL in place and re-renders the current view rather than
// navigating, so Back steps between views and not between settings.
function wireControls() {
  document.getElementById("scope-picker").addEventListener("change", ev => {
    ui.scope = ev.target.value;
    pushRoute();
    rerenderCurrentView();
  });
  document.getElementById("range-picker-global").addEventListener("change", ev => {
    ui.range = ev.target.value;
    pushRoute();
    if (ui.view === "metrics") { runNow("metrics"); } else if (ui.view === "node") { nv.chartsAt = 0; runNow("node"); }
  });
  wireJump();
}
// rerenderCurrentView redraws from the last document each view holds,
// without waiting for the next poll.
function rerenderCurrentView() {
  if (lastCluster) render(lastCluster);
  if (lastSchema) renderSchema(lastSchema);
  if (ui.view === "sql") runNow("activity");
  if (ui.view === "ops" || ui.view === "security") runNow("overview");
}
