function fmtRTT(us) { return us < 1000 ? (us / 1000).toFixed(2) + " ms" : us < 100000 ? (us / 1000).toFixed(1) + " ms" : Math.round(us / 1000) + " ms"; }
function fmtOffset(us) { const ms = us / 1000; return (ms >= 0 ? "+" : "") + (Math.abs(ms) < 10 ? ms.toFixed(2) : ms.toFixed(0)) + " ms"; }
function renderNetwork(d) {
  const nodes = (d.nodes || []).slice().sort((a, b) => a.node_id - b.node_id);
  const table = document.getElementById("latency");
  const note = document.getElementById("latency-note");
  if (nodes.length < 2) { setHTML(table, ""); setHTML(document.getElementById("latency-worst"), ""); note.textContent = "one node: nothing to measure"; return; }
  const maxOff = (d.max_offset_ms || 0) * 1000; // µs
  // The worst pairs — unreachable first, then the slowest round trips
  // and the largest clock offsets — for narrow screens, where an n×n
  // matrix does not fit (and the better view past ~10 nodes anywhere).
  const pairs = [];
  for (const from of nodes) for (const l of from.latency || []) pairs.push({ from: from.node_id, to: l.peer, l });
  pairs.sort((a, b) => (b.l.reachable ? 0 : 1) - (a.l.reachable ? 0 : 1) || (b.l.rtt_us || 0) - (a.l.rtt_us || 0));
  const worstOff = pairs.filter(p => p.l.reachable).sort((a, b) => Math.abs(b.l.offset_us) - Math.abs(a.l.offset_us)).slice(0, 3);
  const shown = [...new Set([...pairs.slice(0, 5), ...worstOff])];
  setHTML(document.getElementById("latency-worst"), shown.map(p => `<tr data-key="${p.from}-${p.to}">
    <td data-label="pair">n${p.from} → n${p.to}</td>
    <td class="num" data-label="round trip">${p.l.reachable ? fmtRTT(p.l.rtt_us) : `<span class="st down">✕ unreachable</span>`}</td>
    <td class="num" data-label="clock offset">${p.l.reachable ? fmtOffset(p.l.offset_us) : "—"}</td>
  </tr>`).join("") || `<tr><td colspan="3" class="muted">no measurements yet</td></tr>`);
  let html = `<thead><tr><th>from \\ to</th>` + nodes.map(n => `<th class="num">n${n.node_id}</th>`).join("") +
    `<th class="num" title="largest clock offset this node measured to any peer, vs the tolerated --max-offset">clock offset</th></tr></thead><tbody>`;
  for (const from of nodes) {
    const row = new Map((from.latency || []).map(l => [l.peer, l]));
    let worst = null;
    html += `<tr><td>n${from.node_id}</td>`;
    for (const to of nodes) {
      if (to.node_id === from.node_id) { html += `<td class="num muted">—</td>`; continue; }
      const l = row.get(to.node_id);
      if (!l) { html += `<td class="num muted" title="no measurement yet">·</td>`; continue; }
      if (!l.reachable) { html += `<td class="num"><span class="st down">✕ unreachable</span></td>`; continue; }
      if (worst === null || Math.abs(l.offset_us) > Math.abs(worst)) worst = l.offset_us;
      const lvl = l.rtt_us < 2000 ? "live" : l.rtt_us < 20000 ? "draining" : "down";
      html += `<td class="num" title="p99 ${fmtRTT(l.p99_us)} · clock offset ${fmtOffset(l.offset_us)} · measured ${fmtAgo(l.age_ms)}"><span class="st ${lvl}">${fmtRTT(l.rtt_us)}</span></td>`;
    }
    if (worst === null) html += `<td class="num muted">—</td>`;
    else {
      const a = Math.abs(worst);
      const lvl = maxOff && a >= maxOff ? "down" : maxOff && a >= maxOff / 2 ? "draining" : "";
      html += `<td class="num">${lvl ? `<span class="st ${lvl}">${fmtOffset(worst)}</span>` : fmtOffset(worst)}</td>`;
    }
    html += `</tr>`;
  }
  if (setHTML(table, html + "</tbody>")) a11yTables(table.parentElement);
  note.textContent = "round trip from row node to column node (smoothed; hover for p99 and clock offset); pinged every 2s" +
    (d.max_offset_ms ? `; clock offsets are judged against the tolerated ${d.max_offset_ms} ms` : "");
}
// Cross-node drill-down: clicking a cluster range fetches /api/range?id=N
// (admin role) — every holding node's view of that range over internode
// RPC. The open detail row survives the 3s re-render (cached HTML) and is
// refreshed only on click.
// spanText renders a range's span: keys are already human-readable
// (/table/orders/primary/42); inside a labeled table the table part is
// dropped so the row reads "orders · /primary/42 → /primary/99".
