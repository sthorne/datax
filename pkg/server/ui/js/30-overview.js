// The overview reports the cluster (issue #145): the rollup the node
// computed over every live node's heartbeat, and one card per node.
// Which node served the page is provenance in the header, not the
// subject; its own sections live on its node page.
let rollupPrev = null; // {at, statements, retries} for statement rates
function contrib(r, n) { return n < r.live_nodes ? ` <span class="muted">(${n} of ${r.live_nodes} live nodes)</span>` : ""; }
// render draws the current view from the cluster document. Every view
// reads the same document, so the poll is shared (#147); which sections
// are drawn is the view's business (#151).
function render(d) {
  lastCluster = d;
  // Every view: who served the page, who is signed in, what the scope
  // picker can offer, and a partial-data note if the document carries one.
  const loc = d.local && d.local.locality;
  setHTML(document.getElementById("hdr-node"),
    `served by <a href="${routeTo("node/" + d.node_id)}" title="this node's page">n${d.node_id}</a>` + (loc ? " · " + esc(loc) : "") + (d.local.address ? ` <span class="muted">${esc(d.local.address)}</span>` : ""));
  renderPrincipal(d.principal);
  renderScopePicker();
  const errBox = document.getElementById("cluster-err");
  if (d.error) { errBox.style.display = "block"; errBox.textContent = "partial data — " + d.error; }
  else errBox.style.display = "none";
  switch (ui.view) {
    case "overview": renderOverview(d); break;
    case "nodes": renderNodesTable(d); renderNetwork(d); break;
    case "sql": renderSQL(d); break;
    case "data": renderReplication(d); renderClusterRanges(d.ranges || []); break;
    case "security": renderSecurity(d); break;
  }
}
function renderOverview(d) {
  const r = d.rollup || {};
  const now = Date.now();
  let stmtRate = null, retryRate = null;
  if (rollupPrev && now > rollupPrev.at && r.statements >= rollupPrev.statements) {
    const dt = (now - rollupPrev.at) / 1000;
    stmtRate = (r.statements - rollupPrev.statements) / dt;
    retryRate = (r.serialization_failures - rollupPrev.retries) / dt;
  }
  rollupPrev = { at: now, statements: r.statements || 0, retries: r.serialization_failures || 0 };
  const users = Object.entries(r.connections_by_user || {}).sort((a, b) => b[1] - a[1]).slice(0, 3).map(([u, c]) => `${esc(u)} ${c}`).join(", ");
  renderTiles(document.getElementById("tiles"),
    tile("live nodes", (r.live_nodes ?? 0) + " / " + (r.nodes ?? (d.nodes || []).length)) +
    tile("cluster QPS", Math.round(r.qps || 0) + contrib(r, r.contributing), "qps", r.qps || 0) +
    tile("cluster data", fmtBytes(r.data_bytes || 0) + contrib(r, r.contributing), "bytes", r.data_bytes || 0) +
    tile("ranges", (r.ranges ?? 0) + ` <span class="muted">· ${r.replicas ?? 0} replicas</span>`) +
    tile("leases", (r.leases ?? 0) + contrib(r, r.contributing)) +
    tile("connections", `${r.connections ?? 0} <span class="muted">(${r.active ?? 0} active, ${r.idle_in_txn ?? 0} idle in txn${users ? "; " + users : ""})</span>` + contrib(r, r.sql_contributing)) +
    tile("statements/s", stmtRate === null ? "…" : stmtRate.toFixed(1) + contrib(r, r.sql_contributing), "stmt", stmtRate || 0) +
    tile("40001/s", retryRate === null ? "…" : retryRate.toFixed(2), "retry", retryRate || 0) +
    tile("worst p99", r.worst_p99_node ? `${(r.worst_p99_us / 1000).toFixed(1)} ms <span class="muted">(n${r.worst_p99_node})</span>` : "—", "p99", (r.worst_p99_us || 0) / 1000));

  const nodes = (d.nodes || []).slice().sort((a, b) => a.node_id - b.node_id);
  renderKeyed(document.getElementById("nodes"), nodes.map(n => {
    const m = n.machine;
    return { key: "n" + n.node_id, html: `<a class="card" data-key="n${n.node_id}" href="#/node/${n.node_id}" title="open n${n.node_id}'s page">
    <div class="head"><b>n${n.node_id}</b> ${statusCell(n)} <span class="muted">${esc(n.locality || "no locality")}</span></div>
    <div class="row">${m ? `cpu <b>${pct(m.cpu_percent)}</b> · load <b>${m.cores ? loadCell(m.load1, m.cores) : "—"}</b> · memory <b>${m.mem_total ? memCell(m) : "—"}</b>` : `<span class="muted">no machine summary (older binary?)</span>`}</div>
    <div class="row">${m ? `disk free <b>${m.disk_total ? diskCell(m) : "in-memory store"}</b> · fds <b>${m.fd_limit ? fdCell(m) : "—"}</b>` : ""}</div>
    <div class="row">leads <b>${n.leader_count || 0}</b> · <b>${fmtBytes(n.replica_bytes || 0)}</b> · <b>${Math.round(n.leader_qps || 0)}</b> qps${n.sql ? ` · <b>${n.sql.open}</b> connections` : ""} · heartbeat ${fmtAgo(n.heartbeat_ago_ms)}</div>
    <div class="row muted key">${esc(n.address)}</div>
  </a>` }; }));
  const dead = nodes.filter(n => !n.live).length;
  document.getElementById("nodes-note").innerHTML = "figures from each node's heartbeat" + (dead ? `; ${dead} node${dead > 1 ? "s" : ""} down — excluded from the cluster totals above` : "") +
    ` · <a href="${routeTo("nodes")}">every node's detail →</a>`;
}

// The nodes view: the same figures as the overview's cards, as a table
// that sorts and filters, plus the network matrix beside it.
let nodeFilter = "";
function setNodeFilter(v) {
  nodeFilter = (v || "").trim().toLowerCase();
  const box = document.getElementById("node-filter");
  if (box.value !== v) box.value = v || "";
}
function matchesNode(n) {
  if (!nodeFilter) return true;
  return ("n" + n.node_id).includes(nodeFilter) ||
    (n.locality || "").toLowerCase().includes(nodeFilter) ||
    (n.address || "").toLowerCase().includes(nodeFilter);
}
function renderNodesTable(d) {
  const nodes = (d.nodes || []).slice().sort((a, b) => a.node_id - b.node_id).filter(matchesNode);
  renderKeyed(document.getElementById("nodes-table"), nodes.length ? nodes.map(n => {
    const m = n.machine;
    return { key: "n" + n.node_id, html: `<tr data-key="n${n.node_id}">
      <td data-label="node"><a href="${routeTo("node/" + n.node_id)}">n${n.node_id}</a></td>
      <td data-label="status">${statusCell(n)}</td>
      <td class="key" data-label="address">${esc(n.address)}</td>
      <td data-label="locality">${esc(n.locality || "—")}</td>
      <td class="num" data-label="cpu">${m ? pct(m.cpu_percent) : "—"}</td>
      <td class="num" data-label="load">${m && m.cores ? loadCell(m.load1, m.cores) : "—"}</td>
      <td class="num" data-label="memory">${m && m.mem_total ? memCell(m) : "—"}</td>
      <td class="num" data-label="disk free">${m && m.disk_total ? diskCell(m) : "—"}</td>
      <td class="num" data-label="fds">${m && m.fd_limit ? fdCell(m) : "—"}</td>
      <td class="num" data-label="leases">${n.leader_count || 0}</td>
      <td class="num" data-label="heartbeat">${fmtAgo(n.heartbeat_ago_ms)}</td>
    </tr>` };
  }) : [{ key: "none", html: `<tr data-key="none"><td colspan="11" class="muted">no node matches ${esc(nodeFilter)}</td></tr>` }]);
  document.getElementById("nodes-table-note").textContent =
    nodeFilter ? `filtered to ${nodes.length} of ${(d.nodes || []).length} nodes` : "";
}
function rangeRow(r, leaderLabel, extra) {
  return `<tr data-key="r${r.range_id}">
    <td>r${r.range_id}${r.leader ? " ★" : ""}</td>
    <td class="key">${spanText(r)}</td>
    <td>${(r.replicas || []).map(x => "n" + x).join(" ")}</td>
    <td>${r.leader ? leaderLabel : ""}</td>
    <td class="num">${fmtBytes(r.size_bytes)}</td>
    <td class="num">${r.qps ? r.qps.toFixed(0) : "0"}</td>
    <td class="num">${r.applied_index}</td>${extra ? `<td class="num">${esc(extra(r))}</td>` : ""}
  </tr>`;
}
function machineTiles(lm) {
  return tile("host cpu", pct(lm.cpu_percent) + (lm.iowait_percent >= 1 ? " (" + pct(lm.iowait_percent) + " iowait)" : ""), "cpu", lm.cpu_percent) +
      tile("load", lm.cores ? (lm.load1 ?? 0).toFixed(2) + " / " + lm.cores + " cores" : "—") +
      tile("memory", lm.mem_total ? fmtBytes(lm.mem_total - lm.mem_available) + " / " + fmtBytes(lm.mem_total) : "—") +
      tile("process", fmtBytes(lm.rss || 0) + " rss · " + pct(lm.process_cpu_percent) + " cpu") +
      tile("disk free", lm.disk_total ? fmtBytes(lm.disk_free) + " / " + fmtBytes(lm.disk_total) : "in-memory store", "diskfree", lm.disk_free || 0) +
      tile("disk i/o", lm.disk_total ? fmtBytes(lm.disk_read_bytes_ps || 0) + "/s r · " + fmtBytes(lm.disk_write_bytes_ps || 0) + "/s w · " + pct(lm.disk_busy_percent) + " busy" : "—", "diskw", lm.disk_write_bytes_ps || 0) +
      tile("network", fmtBytes(lm.net_rx_bytes_ps || 0) + "/s in · " + fmtBytes(lm.net_tx_bytes_ps || 0) + "/s out", "net", (lm.net_rx_bytes_ps || 0) + (lm.net_tx_bytes_ps || 0)) +
      tile("file descriptors", lm.fd_limit ? lm.open_fds + " / " + lm.fd_limit : "—") +
      tile("go runtime", lm.goroutines + " goroutines · " + fmtBytes(lm.heap_in_use || 0) + " heap · gc p99 " + (lm.gc_pause_p99_ns / 1e6).toFixed(1) + " ms") +
      tile("uptime", fmtDuration(lm.process_uptime_seconds ?? lm.uptime_seconds ?? 0));
}
function storageTiles(s) {
  return tile("L0 files", s.l0_files ?? 0) +
    tile("L0 sublevels", s.l0_sublevels ?? 0) +
    tile("compaction debt", fmtBytes(s.compaction_debt_bytes || 0), "debt", s.compaction_debt_bytes || 0) +
    tile("memtables", (s.memtable_count ?? 0) + " (" + fmtBytes(s.memtable_bytes || 0) + ")") +
    tile("write stalls", s.write_stalls ?? 0) +
    tile("disk slow events", s.disk_slow_events ?? 0) +
    tile("block cache", fmtBytes(s.block_cache_bytes || 0) + " · " + pctOf(s.block_cache_hits, s.block_cache_misses) + " hit") +
    tile("bloom filters", pctOf(s.filter_hits, s.filter_misses) + " of point reads skipped");
}
// pctOf renders hits / (hits + misses) as a percentage ("–" before any read).
function pctOf(hits, misses) {
  const h = hits || 0, m = misses || 0;
  if (h + m === 0) return "–";
  return (100 * h / (h + m)).toFixed(1) + "%";
}
// Schema browser: /api/schema is fetched on the same 3s cadence as the
// cluster document; the filter box narrows the table list and both range
// tables (ranges carry the table their start key belongs to).
