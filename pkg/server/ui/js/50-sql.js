const sqlPrev = new Map(); // node_id → {at, total, retries}
function sumStatements(st) { return Object.values(st || {}).reduce((a, b) => a + b, 0); }
function renderSQL(d) {
  const nodes = (d.nodes || []).slice().sort((a, b) => a.node_id - b.node_id);
  const now = Date.now();
  let open = 0, active = 0, idleTxn = 0, stmtRate = 0, retryRate = 0, p99 = 0, p99Node = null, oldest = 0;
  const rows = nodes.map(n => {
    const q = n.sql;
    if (!q) return { key: "n" + n.node_id, html: `<tr data-key="n${n.node_id}"><td>n${n.node_id}</td><td colspan="6" class="muted">no SQL summary (older binary?)</td></tr>` };
    open += q.open; active += q.active; idleTxn += q.idle_in_txn;
    if ((q.oldest_idle_txn_ms || 0) > oldest) oldest = q.oldest_idle_txn_ms;
    if (q.p99_us > p99) { p99 = q.p99_us; p99Node = n.node_id; }
    const total = sumStatements(q.statements), prev = sqlPrev.get(n.node_id);
    let rate = null, rrate = null;
    if (prev && now > prev.at && total >= prev.total) {
      const dt = (now - prev.at) / 1000;
      rate = (total - prev.total) / dt;
      rrate = (q.serialization_failures - prev.retries) / dt;
      stmtRate += rate; retryRate += rrate;
    }
    sqlPrev.set(n.node_id, { at: now, total, retries: q.serialization_failures });
    const mix = Object.entries(q.statements || {}).filter(([, v]) => v > 0).sort((a, b) => b[1] - a[1]).slice(0, 4)
      .map(([k, v]) => `${esc(k)} ${Math.round(100 * v / Math.max(total, 1))}%`).join(" · ");
    const idle = q.oldest_idle_txn_ms ? warn(q.oldest_idle_txn_ms > 60000 ? "down" : q.oldest_idle_txn_ms > 10000 ? "draining" : "", fmtAgo(q.oldest_idle_txn_ms)) : "—";
    return { key: "n" + n.node_id, html: `<tr data-key="n${n.node_id}">
      <td data-label="node">n${n.node_id}</td>
      <td class="num" data-label="connections">${q.open} <span class="muted">(${q.active} / ${q.idle_in_txn})</span></td>
      <td class="num" data-label="oldest idle txn">${idle}</td>
      <td class="num" data-label="stmt/s">${rate === null ? "…" : rate.toFixed(1)}</td>
      <td data-label="mix">${mix || "—"}</td>
      <td class="num" data-label="40001/s">${rrate === null ? "…" : rrate.toFixed(2)}</td>
      <td class="num" data-label="p50 / p99">${(q.p50_us / 1000).toFixed(1)} / ${(q.p99_us / 1000).toFixed(1)} ms</td>
    </tr>` };
  });
  renderKeyed(document.getElementById("sql-nodes"), rows);
  renderTiles(document.getElementById("sql-tiles"),
    tile("connections", `${open} (${active} active, ${idleTxn} idle in txn)`) +
    tile("statements/s", stmtRate.toFixed(1), "stmt", stmtRate) +
    tile("40001/s", retryRate.toFixed(2), "retry", retryRate) +
    tile("worst p99", p99Node ? `${(p99 / 1000).toFixed(1)} ms (n${p99Node})` : "—", "p99", p99 / 1000) +
    tile("oldest idle txn", oldest ? fmtAgo(oldest) : "none"));
  document.getElementById("sql-note").textContent = "rates are differences between polls; kinds: select, insert, update, delete, copy, txn (BEGIN/COMMIT/ROLLBACK/savepoints), ddl";
}
// The statements panel is node-scoped (issue #151): it shows the
// statements of whichever node the header's scope names, which is the
// serving node unless the reader chose another — and says so, because
// "in flight" means something different per node.
async function pollActivity() {
  const box = document.getElementById("sql-detail");
  const scope = document.getElementById("sql-scope");
  const refuse = () => { box.hidden = false; setHTML(document.getElementById("sql-statements"), `<tr><td colspan="6" class="note">statements need the admin role — ${drillDownRefusal()}</td></tr>`); };
  const id = scopeNode();
  const serving = lastCluster ? lastCluster.node_id : 0;
  scope.textContent = id && id !== serving
    ? `statements on n${id}, fetched from it over the internode RPC (admin role)`
    : `statements on n${serving || "?"}, the node serving this page — each node keeps its own`;
  if (!canDrillDown()) { refuse(); return; }
  if (id && id !== serving) { await pollNodeStatements(id, box); return; }
  const resp = await fetch("/api/activity", { cache: "no-store" });
  if (resp.status === 403) { refuse(); return; }
  if (!resp.ok) { box.hidden = true; throw new Error("HTTP " + resp.status); }
  {
    const a = await resp.json();
    box.hidden = false;
    const rows = [];
    for (const st of a.active || []) rows.push(`<tr><td><span class="st live">running</span></td><td>${esc(st.user)}</td><td>${esc(st.kind)}</td>
      <td class="num">${(st.elapsed_us / 1000).toFixed(0)} ms</td><td class="num">—</td><td class="key">${esc(st.text)}</td></tr>`);
    for (const st of a.slow || []) rows.push(`<tr><td>${fmtAgo(Date.now() - Date.parse(st.at))}</td><td>${esc(st.user)}</td><td>${esc(st.kind)}${st.retry ? ' <span class="st draining">40001</span>' : ""}</td>
      <td class="num">${(st.duration_us / 1000).toFixed(0)} ms</td><td class="num">${st.rows}</td><td class="key">${esc(st.text)}${st.error ? `<div class="muted">${esc(st.error)}</div>` : ""}</td></tr>`);
    setHTML(document.getElementById("sql-statements"), rows.join("") ||
      `<tr><td colspan="6" class="muted">nothing in flight and nothing over ${a.slow_threshold_ms} ms recently</td></tr>`);
  }
}
// The network matrix: each row is one node's measured round trip to
// every other node (from its heartbeat; the serving node's row is live),
// colored by RTT, with that node's clock offset to each peer on hover
// and the worst offset in the last column, judged against --max-offset.

// pollNodeStatements shows another node's statements, from its node
// document (the same fan-out the node page uses, admin-gated).
async function pollNodeStatements(id, box) {
  const resp = await fetch("/api/node?id=" + id, { cache: "no-store" });
  const d = await resp.json().catch(() => ({}));
  if (resp.status === 403) {
    box.hidden = false;
    setHTML(document.getElementById("sql-statements"), `<tr><td colspan="6" class="note">${drillDownRefusal()}</td></tr>`);
    return;
  }
  if (!resp.ok) { box.hidden = true; throw new Error(d.error || ("HTTP " + resp.status)); }
  const act = d.activity;
  box.hidden = false;
  if (!act) {
    setHTML(document.getElementById("sql-statements"), `<tr><td colspan="6" class="muted">n${id} reported no statement activity</td></tr>`);
    return;
  }
  const rows = [];
  for (const st of act.active || []) rows.push(`<tr><td><span class="st live">running</span></td><td>${esc(st.user)}</td><td>${esc(st.kind)}</td>
    <td class="num">${(st.elapsed_us / 1000).toFixed(0)} ms</td><td class="num">—</td><td class="key">${esc(st.text)}</td></tr>`);
  for (const st of act.slow || []) rows.push(`<tr><td>${fmtAgo(Date.now() - Date.parse(st.at))}</td><td>${esc(st.user)}</td><td>${esc(st.kind)}${st.retry ? ' <span class="st draining">40001</span>' : ""}</td>
    <td class="num">${(st.duration_us / 1000).toFixed(0)} ms</td><td class="num">${st.rows}</td><td class="key">${esc(st.text)}${st.error ? `<div class="muted">${esc(st.error)}</div>` : ""}</td></tr>`);
  setHTML(document.getElementById("sql-statements"), rows.join("") ||
    `<tr><td colspan="6" class="muted">nothing in flight and nothing over ${act.slow_threshold_ms} ms recently on n${id}</td></tr>`);
}
