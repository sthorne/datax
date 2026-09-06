// ---- Node detail: /#/node/N ----
// /api/node?id=N is the node's own document when N is the serving node,
// else fetched from N over the internode RPC (admin role). Its recent
// history is the last 15 minutes from the datax_metrics table.
const nv = { id: 0, chartsAt: 0 };
const NODE_CHART_SERIES = ["node.cpu_percent", "node.leader_qps", "sql.statements", "kv.batch_p99_us"];
async function pollNode() {
  // The node to show is the one in the route. nv.id was never assigned
  // from it, so every visit asked for /api/node?id=0 — a 400 — and the
  // page read "Node n0" above an error. The route is the single source
  // of truth for which node this view is; nv only caches across polls.
  const id = ui.node;
  if (id !== nv.id) {
    nv.id = id;
    nv.chartsAt = 0; // a different node's history, so refetch it now
  }
  if (!id) {
    document.getElementById("node-err").style.display = "block";
    document.getElementById("node-err").textContent =
      "no node in the address: open a node from the nodes table, or use #/node/1";
    return;
  }
  const errBox = document.getElementById("node-err");
  document.getElementById("node-title").textContent = `Node n${id}`;
  document.getElementById("node-metrics-link").href = routeTo("metrics", { nodes: String(id) });
  try {
    const resp = await fetch("/api/node?id=" + id, { cache: "no-store" });
    const d = await resp.json();
    if (!resp.ok) throw new Error(d.error || ("HTTP " + resp.status));
    renderNode(d);
    if (Date.now() - nv.chartsAt > 30000) { nv.chartsAt = Date.now(); pollNodeCharts(id); }
  } catch (err) {
    errBox.style.display = "block";
    errBox.innerHTML = /admin role/.test(err.message) ? drillDownRefusal() : esc(err.message || err);
    if (!/admin role/.test(err.message)) throw err;
  }
}
function renderNode(d) {
  const errBox = document.getElementById("node-err");
  if (d.error) { errBox.style.display = "block"; errBox.textContent = "partial data — " + d.error; }
  else errBox.style.display = "none";
  const st = d.status || {};
  renderTiles(document.getElementById("node-ident"),
    tile("status", (d.live ? (d.draining ? "draining" : "live") : "down") + (d.heartbeat_ago_ms ? " · heartbeat " + fmtAgo(d.heartbeat_ago_ms) : "")) +
    tile("address", d.address || "—") +
    tile("sql", d.sql_address || "—") +
    tile("locality", d.locality || "—") +
    tile("version", (d.release ? "v" + d.release + " · " : "") + "protocol v" + (d.binary_version || 1) + (d.cluster_version ? " · cluster v" + d.cluster_version : "")) +
    tile("uptime", d.uptime_seconds ? fmtDuration(d.uptime_seconds) : "—") +
    tile("replicas", (st.ranges || []).length + (st.leader_of !== undefined ? " · leads " + st.leader_of : "")));
  const lm = st.machine;
  // The forecast rides the cluster document, so this page shows it for
  // whichever node it is (issue #156). It is a tile next to the point
  // reading it explains, not a separate section.
  const cap = lastCluster ? capacityFor(lastCluster, d.node_id) : null;
  const capTile = cap && cap.filling
    ? tile("fills in", fmtFillDays(cap.days_to_full) + ` <span class="muted">at ${fmtBytes(Math.max(0, cap.growth_bytes_per_day))}/day</span>`)
    : tile("fills in", `<span class="muted">${esc(cap ? (cap.reason || "not filling") : "no forecast yet")}</span>`);
  if (lm) renderTiles(document.getElementById("node-machine"), machineTiles(lm) + capTile);
  else setHTML(document.getElementById("node-machine"), `<span class="muted">no machine sample</span>`);
  document.getElementById("node-machine-note").textContent = lm && (lm.unavailable || []).length ? "not available on this platform: " + lm.unavailable.join(", ") : "";
  renderTiles(document.getElementById("node-storage"), storageTiles(d.storage || {}) +
    tile("debt gate", d.debt_gated ? "latched" : "open" + (d.debt_gate_entries ? " · " + d.debt_gate_entries + " entries" : "")) +
    tile("overload", d.overloaded ? "shedding: " + (d.overload_reason || "") : "no") +
    tile("encryption", d.encrypted ? (d.reencryption && d.reencryption.active ? "re-encrypting: " + fmtBytes(d.reencryption.remaining_bytes) + " left" : "encrypted at rest") : "plaintext") +
    (d.store_prefix_bloom ? tile("prefix bloom filters", d.prefix_bloom_rewrite && d.prefix_bloom_rewrite.remaining_files ? `rewriting ${d.prefix_bloom_rewrite.remaining_files} legacy tables` : "on") : ""));
  document.getElementById("node-storage-note").textContent = d.reencryption && d.reencryption.sweep_error ? "re-encryption sweep error: " + d.reencryption.sweep_error : "";
  const ranges = (st.ranges || []).slice().sort((a, b) => a.range_id - b.range_id);
  renderKeyed(document.getElementById("node-ranges"), ranges.length
    ? ranges.map(r => ({ key: "r" + r.range_id, html: rangeRow(r, "this node", x => String(Math.max(0, (x.applied_index || 0) - (x.truncated_index || 0)))) }))
    : [{ key: "none", html: `<tr data-key="none"><td colspan="8" class="muted">no replicas</td></tr>` }]);
  const q = d.sql;
  renderTiles(document.getElementById("node-sql"), q
    ? tile("connections", `${q.open}` + qual(`${q.active} active · ${q.idle_in_txn} idle in txn`)) +
      tile("statements", Object.values(q.statements || {}).reduce((a, b) => a + b, 0) + qual("since this node started")) +
      tile("40001", q.serialization_failures + qual("since this node started")) +
      tile("plan cache", (q.plan_cache_hits + q.plan_cache_misses)
        ? `${(100 * q.plan_cache_hits / (q.plan_cache_hits + q.plan_cache_misses)).toFixed(0)}%` + qual(`${q.plan_cache_hits} of ${q.plan_cache_hits + q.plan_cache_misses} planned statements hit`)
        : "no statements planned") +
      tile("p50 / p99", `${(q.p50_us / 1000).toFixed(1)} / ${(q.p99_us / 1000).toFixed(1)} ms`)
    : `<span class="muted">no SQL listener</span>`);
  const act = d.activity, box = document.getElementById("node-activity");
  if (act) {
    box.hidden = false;
    const rows = [];
    for (const s of act.active || []) rows.push(`<tr><td><span class="st live">running</span></td><td>${esc(s.user)}</td><td>${esc(s.kind)}</td><td class="num">${(s.elapsed_us / 1000).toFixed(0)} ms</td><td class="num">—</td><td class="key">${esc(s.text)}</td></tr>`);
    for (const s of act.slow || []) rows.push(`<tr><td>${fmtAgo(Date.now() - Date.parse(s.at))}</td><td>${esc(s.user)}</td><td>${esc(s.kind)}</td><td class="num">${(s.duration_us / 1000).toFixed(0)} ms</td><td class="num">${s.rows}</td><td class="key">${esc(s.text)}</td></tr>`);
    setHTML(document.getElementById("node-statements"), rows.join("") || `<tr><td colspan="6" class="muted">nothing in flight and nothing over ${act.slow_threshold_ms} ms recently</td></tr>`);
    document.getElementById("node-sql-note").textContent = "";
  } else {
    box.hidden = true;
    document.getElementById("node-sql-note").textContent = q ? "statements in flight and slow statements need the admin role" : "";
  }
  renderKeyed(document.getElementById("node-latency"), (d.latency || []).slice().sort((a, b) => a.peer - b.peer).map(l => ({ key: "n" + l.peer, html: `<tr data-key="n${l.peer}">
    <td>n${l.peer}</td>
    <td class="num">${l.reachable ? fmtRTT(l.rtt_us) : "—"}</td>
    <td class="num">${l.reachable ? fmtRTT(l.p99_us) : "—"}</td>
    <td class="num">${l.reachable ? fmtOffset(l.offset_us) : "—"}</td>
    <td>${l.reachable ? `<span class="st live"><span class="dot"></span>yes</span>` : `<span class="st down"><span class="dot"></span>no</span>`}</td>
    <td class="num">${l.age_ms >= 0 ? fmtAgo(l.age_ms) : "never"}</td>
  </tr>` })).concat((d.latency || []).length ? [] : [{ key: "none", html: `<tr data-key="none"><td colspan="6" class="muted">no peers measured</td></tr>` }]));
  renderKeyed(document.getElementById("node-settings"), Object.entries(d.settings || {}).sort().map(([k, v]) => ({ key: k, html: `<tr data-key="${esc(k)}"><td>${esc(k)}</td><td class="key">${esc(v)}</td></tr>` })));
  const evs = (d.events || []).slice().reverse();
  renderKeyed(document.getElementById("node-events"), evs.length
    ? evs.map(e => ({ key: String(e.seq), html: `<tr data-key="${e.seq}">
    <td class="when" title="${esc(e.at)}">${fmtAgo(Date.now() - Date.parse(e.at))}</td>
    <td><span class="kind${e.audit ? " audit" : ""}">${esc(e.kind)}</span></td>
    <td class="key" style="max-width:none;white-space:normal">${esc(e.summary)}</td>
  </tr>` }))
    : [{ key: "none", html: `<tr data-key="none"><td colspan="3" class="muted">nothing recorded yet</td></tr>` }]);
}
async function pollNodeCharts(id) {
  const box = document.getElementById("node-charts");
  try {
    const resp = await fetch(`/api/metrics?series=${NODE_CHART_SERIES.join(",")}&node=${id}&since=${RANGE_SECONDS[ui.range]}s&rate=1`, { cache: "no-store" });
    if (!resp.ok) { const e = await resp.json().catch(() => ({})); box.innerHTML = `<span class="muted">${esc(e.error || "history unavailable")}</span>`; return; }
    const d = await resp.json();
    box.innerHTML = "";
    for (const s of d.series || []) box.appendChild(chart(s, s.nodes || {}, "", d));
  } catch (err) { box.innerHTML = `<span class="muted">history unavailable: ${esc(err.message || err)}</span>`; }
}
