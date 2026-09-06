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
  renderTxnUsers(d, lastContention && lastContention.retriesByUser, lastContention && lastContention.node);
}
// The statements panel is node-scoped (issue #151): it shows the
// statements of whichever node the header's scope names, which is the
// serving node unless the reader chose another — and says so, because
// "in flight" means something different per node.
async function pollActivity() {
  const box = document.getElementById("sql-detail");
  const scope = document.getElementById("sql-scope");
  const refuse = () => {
    box.hidden = false;
    setHTML(document.getElementById("sql-statements"), `<tr><td colspan="6" class="note">statements need the admin role — ${drillDownRefusal()}</td></tr>`);
    refuseContention();
  };
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
    renderContention(a, serving);
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
    refuseContention();
    return;
  }
  if (!resp.ok) { box.hidden = true; throw new Error(d.error || ("HTTP " + resp.status)); }
  const act = d.activity;
  box.hidden = false;
  if (!act) {
    setHTML(document.getElementById("sql-statements"), `<tr><td colspan="6" class="muted">n${id} reported no statement activity</td></tr>`);
    setHTML(document.getElementById("retry-shapes"), `<tr><td colspan="4" class="muted">n${id} reported no statement activity</td></tr>`);
    setHTML(document.getElementById("idle-txns"), `<tr><td colspan="7" class="muted">n${id} reported no statement activity</td></tr>`);
    return;
  }
  renderContention(act, id);
  const rows = [];
  for (const st of act.active || []) rows.push(`<tr><td><span class="st live">running</span></td><td>${esc(st.user)}</td><td>${esc(st.kind)}</td>
    <td class="num">${(st.elapsed_us / 1000).toFixed(0)} ms</td><td class="num">—</td><td class="key">${esc(st.text)}</td></tr>`);
  for (const st of act.slow || []) rows.push(`<tr><td>${fmtAgo(Date.now() - Date.parse(st.at))}</td><td>${esc(st.user)}</td><td>${esc(st.kind)}${st.retry ? ' <span class="st draining">40001</span>' : ""}</td>
    <td class="num">${(st.duration_us / 1000).toFixed(0)} ms</td><td class="num">${st.rows}</td><td class="key">${esc(st.text)}${st.error ? `<div class="muted">${esc(st.error)}</div>` : ""}</td></tr>`);
  setHTML(document.getElementById("sql-statements"), rows.join("") ||
    `<tr><td colspan="6" class="muted">nothing in flight and nothing over ${act.slow_threshold_ms} ms recently on n${id}</td></tr>`);
}

// ---- Transactions and contention (issue #154) --------------------
//
// A serializable database lives and dies on retries, and until now the
// console's only transaction-shaped figure was a 40001/s column. Four
// series were recorded every interval and reachable only by knowing to
// pick them out of the metrics picker.
//
// The rates are charted from the metrics table over the header's range,
// which is not sensitive and needs no admin role. What a rate cannot
// tell you — WHICH statements and WHICH sessions — comes from
// /api/activity, which is admin-gated because it carries statement text.
const TXN_SERIES = ["txn.commits", "txn.aborts", "txn.retries", "kv.batch_p99_us", "sql.p99_us"];
// The last contention document and whose it was. The by-user table is
// drawn from the cluster poll (3s) and this (also 3s, but a separate
// request that can be refused), so it is kept rather than re-fetched.
let lastContention = null;

// renderContention draws the three admin panels from one node's
// activity document, and remembers the by-user counts for the table
// that mixes them with the cluster-wide connection counts.
function renderContention(a, id) {
  lastContention = { node: id, retriesByUser: a.retries_by_user || {} };
  renderRetryShapes(a, id);
  renderIdleTxns(a, id);
  if (lastCluster) renderTxnUsers(lastCluster, lastContention.retriesByUser, id);
}
// The last metrics document the charts drew, kept so a redraw needs no
// refetch.
let txnData = null;

async function pollTxnCharts() {
  const box = document.getElementById("txn-charts");
  const note = document.getElementById("txn-note");
  try {
    const resp = await fetch(`/api/metrics?series=${TXN_SERIES.join(",")}&since=${RANGE_SECONDS[ui.range]}s&rate=1`, { cache: "no-store" });
    if (!resp.ok) {
      const e = await resp.json().catch(() => ({}));
      box.innerHTML = "";
      note.textContent = e.error || `history unavailable (HTTP ${resp.status})`;
      txnData = null;
      return;
    }
    txnData = await resp.json();
    renderTxnCharts();
  } catch (err) {
    box.innerHTML = "";
    note.textContent = "history unavailable: " + (err.message || err);
    txnData = null;
  }
}

function renderTxnCharts() {
  const box = document.getElementById("txn-charts");
  const note = document.getElementById("txn-note");
  if (!txnData) return;
  box.innerHTML = "";
  for (const s of txnData.series || []) box.appendChild(chart(s, s.nodes || {}, "", txnData, { annotate: false }));
  if (!box.children.length) box.innerHTML = `<div class="muted">no samples in this range</div>`;
  // Commits, aborts and retries are per-node counters of independent
  // work, so a cluster total is a sum. Latency percentiles are not:
  // there is no p99 of two p99s, so the tiles report the worst node and
  // name it rather than averaging figures that do not average.
  const latest = name => {
    const s = (txnData.series || []).find(x => x.name === name);
    if (!s) return null;
    const out = {};
    for (const [id, pts] of Object.entries(s.nodes || {})) if (pts.length) out[id] = pts[pts.length - 1][1];
    return out;
  };
  const sum = m => m ? Object.values(m).reduce((a, b) => a + b, 0) : null;
  const worst = m => {
    if (!m) return null;
    let id = null, v = -Infinity;
    for (const [k, x] of Object.entries(m)) if (x > v) { v = x; id = k; }
    return id === null ? null : { id, v };
  };
  const commits = sum(latest("txn.commits")), aborts = sum(latest("txn.aborts")), retries = sum(latest("txn.retries"));
  const kv = worst(latest("kv.batch_p99_us")), stmt = worst(latest("sql.p99_us"));
  const rate = v => v === null ? "—" : v.toFixed(v < 10 ? 2 : 0) + "/s";
  const ms = w => w ? `${(w.v / 1000).toFixed(1)} ms (n${w.id})` : "—";
  renderTiles(document.getElementById("txn-tiles"),
    tile("commits/s", rate(commits)) +
    tile("aborts/s", rate(aborts)) +
    tile("retries/s", rate(retries)) +
    // A retry rate on its own says nothing; against the commit rate it
    // says how much of the work is being redone.
    tile("retried share", commits === null || retries === null || commits + retries <= 0
      ? "—" : pct(100 * retries / (commits + retries))) +
    tile("kv batch p99", ms(kv)) +
    tile("statement p99", ms(stmt)));
  note.textContent = "from the datax_metrics table over the header's range, one line per node. "
    + "Commits, aborts and retries are counted per node and summed in the tiles; the two latency tiles are the worst node, named, "
    + "because there is no p99 of two p99s. KV batch latency beside statement latency is what tells a slow statement from a slow cluster.";
}

// renderTxnUsers: who holds the connections, and whose statements are
// hitting 40001s. Connections come from every node's heartbeat and add
// up; retries are the scoped node's own count and do not, so the table
// says which column is which rather than presenting one total.
function renderTxnUsers(d, retriesByUser, retryNode) {
  const byUser = new Map();
  for (const n of d.nodes || []) {
    for (const [u, c] of Object.entries((n.sql || {}).by_user || {})) byUser.set(u, (byUser.get(u) || 0) + c);
  }
  const retries = retriesByUser || {};
  for (const u of Object.keys(retries)) if (!byUser.has(u)) byUser.set(u, 0);
  const rows = [...byUser.entries()].sort((a, b) => (retries[b[0]] || 0) - (retries[a[0]] || 0) || b[1] - a[1])
    .map(([u, c]) => ({
      key: u || "(none)", html: `<tr data-key="${esc(u || "(none)")}">
      <td data-label="user">${esc(u || "—")}</td>
      <td class="num" data-label="connections">${c}</td>
      <td class="num" data-label="40001s">${retriesByUser ? (retries[u] || 0)
        : `<span class="muted" title="the 40001 breakdown needs the admin role">—</span>`}</td>
    </tr>` }));
  renderKeyed(document.getElementById("txn-users"), rows.length ? rows
    : [{ key: "none", html: `<tr data-key="none"><td colspan="3" class="muted">no connections</td></tr>` }]);
  document.getElementById("txn-users-note").textContent = retriesByUser
    ? `connections are every node's open connections added up; 40001s are n${retryNode || "?"}'s own count since it started, not a cluster total — each node counts its own`
    : "connections are every node's open connections added up; the 40001 breakdown needs the admin role";
}

// renderRetryShapes: which statement shapes are producing the failures.
// Cumulative since the scoped node started, so it answers "what should
// change" rather than "what happened in the last three seconds".
function renderRetryShapes(a, id) {
  const shapes = a.retry_shapes || [];
  const rows = shapes.map(s => {
    const users = Object.entries(s.users || {}).sort((x, y) => y[1] - x[1]).map(([u, c]) => `${esc(u || "—")} ${c}`).join(" · ");
    return {
      key: s.shape, html: `<tr data-key="${esc(s.shape)}">
      <td class="num" data-label="40001s">${s.count}</td>
      <td data-label="users">${users || "—"}</td>
      <td class="when" data-label="last" title="${esc(s.last_at)}">${fmtAgo(Date.now() - Date.parse(s.last_at))}</td>
      <td class="key" style="max-width:none;white-space:normal">${esc(s.shape)}</td>
    </tr>` };
  });
  renderKeyed(document.getElementById("retry-shapes"), rows.length ? rows
    : [{ key: "none", html: `<tr data-key="none"><td colspan="4" class="muted">n${id} has recorded no serialization failures since it started</td></tr>` }]);
  const other = a.retry_shapes_other || 0;
  document.getElementById("retry-note").textContent =
    `statement shapes with their literals replaced, counted on n${id} since it started — cumulative, not a rate. `
    + (other ? `${other} further failure${other === 1 ? "" : "s"} fell outside the shapes listed and are counted only in this total. ` : "")
    + "Each node counts its own, so this is not a cluster figure.";
}

// renderIdleTxns: an idle transaction blocks other writers, and a
// duration alone does not say who to talk to. This says who, from
// where, as what, and what they last ran.
function renderIdleTxns(a, id) {
  const rows = (a.idle_txns || []).map(t => ({
    key: String(t.pid), html: `<tr data-key="${t.pid}">
      <td class="num" data-label="pid">${t.pid}</td>
      <td data-label="user">${esc(t.user || "—")}</td>
      <td data-label="client">${esc(t.remote || "—")}</td>
      <td data-label="application">${esc(t.application || "—")}${t.database ? ` <span class="muted">${esc(t.database)}</span>` : ""}</td>
      <td class="num" data-label="idle">${warn(t.idle_ms > 60000 ? "down" : t.idle_ms > 10000 ? "draining" : "", fmtAgo(t.idle_ms))}</td>
      <td class="num" data-label="txn open">${t.txn_ms ? fmtAgo(t.txn_ms) : "—"}</td>
      <td class="key" style="max-width:none;white-space:normal">${esc(t.last || "—")}</td>
    </tr>` }));
  renderKeyed(document.getElementById("idle-txns"), rows.length ? rows
    : [{ key: "none", html: `<tr data-key="none"><td colspan="7" class="muted">no session on n${id} is idle inside an open transaction</td></tr>` }]);
  document.getElementById("idle-note").textContent =
    `sessions on n${id} holding an open transaction: their write intents block every other writer to those keys, `
    + "so the oldest is the one to ask about. \"idle\" is how long it has been sitting in this state; \"txn open\" is how long the whole block has been open.";
}

// Non-admins see the rates and the connection counts and are told, once,
// what the gate is for, rather than being shown empty tables.
function refuseContention() {
  lastContention = null;
  if (lastCluster) renderTxnUsers(lastCluster, null, 0);
  const note = `<tr><td colspan="7" class="note">statement shapes and sessions need the admin role — ${drillDownRefusal()}</td></tr>`;
  setHTML(document.getElementById("retry-shapes"), note);
  setHTML(document.getElementById("idle-txns"), note);
  document.getElementById("retry-note").textContent = "the retry rate above needs no role; the statements behind it carry data, so they are gated";
  document.getElementById("idle-note").textContent = "";
}
