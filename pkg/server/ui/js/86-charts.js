async function loadCatalog() {
  if (mv.catalog) return;
  try {
    const resp = await fetch("/api/metrics", { cache: "no-store" });
    if (resp.ok) mv.catalog = await resp.json();
  } catch (err) { /* rendered as unavailable below */ }
}
// The picker lists every series the recorder writes, grouped; a
// labelled series expands to the label values this node knows.
function seriesChoices() {
  const out = [];
  if (!mv.catalog) return out;
  for (const def of mv.catalog.series || []) {
    if (!def.label) { out.push({ name: def.name, def }); continue; }
    for (const v of (mv.catalog.labels || {})[def.label] || []) out.push({ name: `${def.name}{${def.label}=${v}}`, def });
  }
  return out;
}
function renderPickers() {
  const sp = document.getElementById("series-picker");
  const choices = seriesChoices();
  const groups = new Map();
  for (const c of choices) { if (!groups.has(c.def.group)) groups.set(c.def.group, []); groups.get(c.def.group).push(c); }
  let html = "";
  for (const [g, items] of groups) {
    html += `<h4>${esc(g)}</h4>` + items.map(c => `<label><input type="checkbox" value="${esc(c.name)}" ${mv.series.includes(c.name) ? "checked" : ""}> ${esc(c.name)} <span class="h">${esc(c.def.kind === "counter" ? "rate" : c.def.unit || "")}</span></label>`).join("");
  }
  sp.innerHTML = html || `<span class="muted">${mv.catalog ? "no series" : "the metrics catalog is unavailable"}</span>`;
  document.getElementById("series-summary").textContent = `choose series (${mv.series.length} of ${choices.length} selected)`;
  document.getElementById("series-details").open = mv.series.length === 0;
  sp.querySelectorAll("input").forEach(cb => cb.addEventListener("change", () => {
    mv.series = [...sp.querySelectorAll("input:checked")].map(x => x.value);
    pushRoute(metricsParams()); fetchMetrics();
  }));
  const note = document.getElementById("metrics-note");
  if (mv.catalog && !mv.catalog.enabled) note.textContent = "recording is disabled on this node (--metrics-record-interval 0)";
  else if (mv.catalog && !mv.catalog.ready) note.textContent = "the datax_metrics table does not exist yet: recording starts once the cluster has finalized v5";
  else if (mv.catalog) note.textContent = `from the datax_metrics table, recorded every ${mv.catalog.interval_seconds}s per node; the same data by SQL: SELECT at, value FROM datax_metrics WHERE node = 1 AND name = '…' ORDER BY at`;
}
async function fetchMetrics() {
  const box = document.getElementById("charts");
  if (mv.series.length === 0) { box.innerHTML = `<div class="muted">pick a series above</div>`; return; }
  try {
    const p = new URLSearchParams({ series: mv.series.join(","), since: RANGE_SECONDS[ui.range] + "s" });
    if (mv.rate) p.set("rate", "1");
    if (mv.nodes) p.set("node", mv.nodes.join(","));
    const resp = await fetch("/api/metrics?" + p.toString(), { cache: "no-store" });
    if (!resp.ok) {
      const e = await resp.json().catch(() => ({}));
      box.innerHTML = `<div class="err" style="display:block">${esc(e.error || ("HTTP " + resp.status))}</div>`;
      return;
    }
    mv.data = await resp.json();
    // The marks come from the same window the charts draw, fetched
    // alongside rather than after, so a chart never renders once bare
    // and then again annotated (issue #155).
    await fetchAnnotations(mv.data.from_ms || (Date.now() - RANGE_SECONDS[ui.range] * 1000));
    renderCharts();
  } catch (err) {
    box.innerHTML = `<div class="err" style="display:block">metrics unavailable: ${esc(err.message || err)}</div>`;
  }
}
function fmtUnit(v, unit) {
  if (v === null || v === undefined) return "—";
  switch (unit) {
    case "bytes": return fmtBytes(Math.max(0, v));
    case "bytes/s": return fmtBytes(Math.max(0, v)) + "/s";
    case "percent": return pct(v);
    case "us": return Math.abs(v) >= 1000 ? (v / 1000).toFixed(1) + " ms" : v.toFixed(0) + " µs";
    case "us/s": return (v / 1000).toFixed(1) + " ms/s";
    case "/s": return v.toFixed(v < 10 ? 2 : 0) + "/s";
    default: return Math.abs(v) >= 100 ? v.toFixed(0) : Math.abs(v) >= 10 ? v.toFixed(1) : v.toFixed(2);
  }
}
function fmtTime(ms, long) {
  const d = new Date(ms), hh = String(d.getHours()).padStart(2, "0"), mm = String(d.getMinutes()).padStart(2, "0");
  return long ? `${d.getMonth() + 1}/${d.getDate()} ${hh}:${mm}` : `${hh}:${mm}`;
}
// ---- Chart annotations (issue #155) ----
// "Why did p99 jump at 14:20" was a two-window investigation: read the
// chart here, scroll the event log there, match timestamps by eye. Both
// data sets are on the same page and the same clock; these are the marks
// that put them on the same axis.
//
// Structural kinds by default. An annotation layer that draws 300 marks
// is noise, so the ones that explain a step change lead and the rest are
// opt-in.
const STRUCTURAL_KINDS = new Set(["split", "auto-split", "load-split", "merge", "rebalance",
  "byte-rebalance", "dead-node-repair", "decommission", "shutdown", "upgrade", "placement"]);
// annotations: {events, oldestMs, node} for the window last fetched.
let ann = { events: [], oldestMs: 0, node: 0 };

// annotationKindsSelected: the structural default, every kind (null, so
// a kind the console has never heard of is still marked rather than
// silently dropped), or none when the layer is switched off.
function annotationKindsSelected() {
  const on = document.getElementById("annotate-toggle");
  if (on && !on.checked) return new Set();
  return document.getElementById("annotate-kinds")?.value === "*" ? null : STRUCTURAL_KINDS;
}

// fetchAnnotations pulls the event window behind the charts.
//
// An event ring is per node, and this asks the SERVING node's — the one
// with the window form and with how far back it reaches. The charts
// themselves are cluster-wide, so the marks are one node's account of
// what the cluster did, and the note under each chart says whose rather
// than leaving it to be assumed.
async function fetchAnnotations(fromMs) {
  try {
    const resp = await fetch(`/api/events?from=${Math.floor(fromMs)}&limit=500`, { cache: "no-store" });
    if (!resp.ok) { ann.events = []; return; }
    const d = await resp.json();
    ann.events = (d.events || []).filter(e => !e.audit);
    ann.oldestMs = d.oldest_unix_ms || 0;
    ann.node = d.node_id || 0;
  } catch (err) { ann.events = []; }
}

// annotationLayer draws the marks for one chart's window, plus the
// hover targets. Returns SVG.
function annotationMarks(from, to, x, T, PH, colour) {
  const kinds = annotationKindsSelected();
  const marks = ann.events.filter(e => {
    const t = Date.parse(e.at);
    return (kinds === null || kinds.has(e.kind)) && t >= from && t <= to;
  });
  if (!marks.length) return { svg: "", count: 0 };
  let svg = `<g class="annotations">`;
  for (const e of marks) {
    const t = Date.parse(e.at);
    const xx = x(t).toFixed(1);
    const title = `${e.kind} · ${fmtTime(t, true)}\n${e.summary}`;
    svg += `<line class="ann" x1="${xx}" x2="${xx}" y1="${T}" y2="${T + PH}" stroke="${colour}"/>`;
    svg += `<rect class="annhit" x="${(Number(xx) - 4).toFixed(1)}" y="${T}" width="8" height="${PH}" fill="transparent"><title>${esc(title)}</title></rect>`;
  }
  return { svg: svg + `</g>`, count: marks.length };
}

function renderCharts() {
  const box = document.getElementById("charts");
  box.classList.toggle("compare", mv.compare);
  box.innerHTML = "";
  const d = mv.data;
  for (const s of d.series || []) {
    const ids = Object.keys(s.nodes || {}).sort((a, b) => a - b);
    if (mv.compare && ids.length > 1) {
      for (const id of ids) box.appendChild(chart(s, { [id]: s.nodes[id] }, ` · n${id}`, d, { annotate: true }));
    } else {
      box.appendChild(chart(s, s.nodes || {}, "", d, { annotate: true }));
    }
  }
  if (!box.children.length) box.innerHTML = `<div class="muted">nothing to chart</div>`;
}
// chart builds one panel: inline SVG, recessive grid, 2px lines, a
// legend of node line-keys, the crosshair tooltip, and a table view.
//
// opts.annotate marks the events in the window (issue #155). It is off
// unless asked for, because the marks come from a fetch the metrics
// view makes: a chart drawn elsewhere would otherwise carry whichever
// window that view last looked at, which is worse than no marks.
function chart(s, nodes, suffix, win, opts) {
  const annotate = !!(opts && opts.annotate);
  const el = document.createElement("div");
  el.className = "chart";
  const unit = s.rate ? (s.unit || "/s") : s.unit;
  const ids = Object.keys(nodes).sort((a, b) => a - b);
  const W = 800, H = 200, L = 58, R = 12, T = 10, B = 22, PW = W - L - R, PH = H - T - B;
  const from = win.from_ms, to = win.to_ms, step = win.step_ms;
  let min = 0, max = 0, any = false;
  for (const id of ids) for (const p of nodes[id]) { any = true; if (p[1] > max) max = p[1]; if (p[1] < min) min = p[1]; }
  if (max === min) max = min + 1;
  max = max + (max - min) * 0.05;
  const x = t => L + (t - from) / (to - from) * PW;
  const y = v => T + (max - v) / (max - min) * PH;
  const label = (id) => id === "0" ? "cluster" : "n" + id;
  let title = `<div class="title"><b>${esc(s.name)}${esc(suffix)}</b><span>${esc(s.rate ? "per second" : unit || "")}${s.kind === "counter" && !s.rate ? " · cumulative" : ""}</span></div>`;
  if (!any) { el.innerHTML = title + `<div class="empty">no samples in this range</div>`; return el; }
  let svg = `<svg viewBox="0 0 ${W} ${H}" role="img" aria-label="${esc(s.name)} over time"><g class="grid">`;
  const ticks = 4;
  for (let i = 0; i <= ticks; i++) {
    const v = min + (max - min) * i / ticks, yy = y(v).toFixed(1);
    svg += `<line x1="${L}" x2="${W - R}" y1="${yy}" y2="${yy}"/>`;
  }
  svg += `</g><g class="axis">`;
  for (let i = 0; i <= ticks; i++) {
    const v = min + (max - min) * i / ticks;
    svg += `<text x="${L - 6}" y="${(y(v) + 4).toFixed(1)}" text-anchor="end">${esc(fmtUnit(v, unit))}</text>`;
  }
  const long = to - from > 36 * 3600 * 1000;
  for (let i = 0; i <= 4; i++) {
    const t = from + (to - from) * i / 4;
    svg += `<text x="${x(t).toFixed(1)}" y="${H - 6}" text-anchor="${i === 0 ? "start" : i === 4 ? "end" : "middle"}">${esc(fmtTime(t, long))}</text>`;
  }
  svg += `</g>`;
  // Annotations go in before the data: a mark explains a line, so it
  // must never be drawn over one.
  const marks = annotate
    ? annotationMarks(from, to, x, T, PH, ann.node ? nodeColor(String(ann.node)) : "var(--text-3)")
    : { svg: "", count: 0 };
  svg += marks.svg;
  for (const id of ids) {
    let dpath = "", prev = null;
    for (const p of nodes[id]) {
      const gap = prev !== null && p[0] - prev > 2 * step;
      dpath += (prev === null || gap ? "M" : "L") + x(p[0]).toFixed(1) + "," + y(p[1]).toFixed(1);
      prev = p[0];
    }
    svg += `<path class="line" stroke="${nodeColor(id)}" d="${dpath}"/>`;
  }
  svg += `<line class="xhair" y1="${T}" y2="${T + PH}"/>`;
  for (const id of ids) svg += `<circle class="dot" data-node="${id}" r="4" fill="${nodeColor(id)}"/>`;
  svg += `<rect class="hit" x="${L}" y="${T}" width="${PW}" height="${PH}" fill="transparent"/></svg>`;
  let legend = ids.length > 1 ? `<div class="legend">` + ids.map(id => `<span><i style="background:${nodeColor(id)}"></i>${label(id)}</span>`).join("") + `</div>` : "";
  let table = `<details><summary>as a table</summary><div class="tablewrap"><table><thead><tr><th>time</th>` + ids.map(id => `<th class="num">${label(id)}</th>`).join("") + `</tr></thead><tbody>`;
  const times = [...new Set(ids.flatMap(id => nodes[id].map(p => p[0])))].sort((a, b) => b - a).slice(0, 30);
  const byT = {}; for (const id of ids) { byT[id] = new Map(nodes[id].map(p => [p[0], p[1]])); }
  for (const t of times) table += `<tr><td>${esc(fmtTime(t, true))}</td>` + ids.map(id => `<td class="num">${esc(byT[id].has(t) ? fmtUnit(byT[id].get(t), unit) : "—")}</td>`).join("") + `</tr>`;
  table += `</tbody></table></div></details>`;
  let annNote = "";
  if (annotate && marks.count) {
    annNote = `<div class="muted annnote">${marks.count} event${marks.count === 1 ? "" : "s"} marked` +
      (ann.node ? ` from n${ann.node}'s ring` : "") +
      (ann.oldestMs && ann.oldestMs > from
        ? ` — the ring reaches back only to ${esc(fmtTime(ann.oldestMs, true))}, so anything earlier in this window is unmarked rather than absent`
        : "") + `</div>`;
  } else if (annotate && ann.oldestMs && ann.oldestMs > from) {
    annNote = `<div class="muted annnote">the event ring reaches back only to ${esc(fmtTime(ann.oldestMs, true))}; earlier in this window is unmarked rather than quiet</div>`;
  }
  el.innerHTML = title + svg + legend + `<div class="tip"></div>` + annNote + table;
  // Crosshair: snap to the nearest bucket; the readout lists every node.
  const svgEl = el.querySelector("svg"), hit = el.querySelector(".hit"), xh = el.querySelector(".xhair"), tip = el.querySelector(".tip");
  const dots = {}; el.querySelectorAll(".dot").forEach(c => dots[c.dataset.node] = c);
  hit.addEventListener("pointermove", ev => {
    const r = svgEl.getBoundingClientRect();
    const px = (ev.clientX - r.left) / r.width * W;
    let t = from + Math.round((px - L) / PW * (to - from) / step) * step;
    t = Math.max(from, Math.min(to, t));
    xh.setAttribute("x1", x(t)); xh.setAttribute("x2", x(t)); xh.style.display = "block";
    tip.replaceChildren();
    const tt = document.createElement("div"); tt.className = "t"; tt.textContent = fmtTime(t, true); tip.appendChild(tt);
    for (const id of ids) {
      const v = byT[id].get(t);
      const dot = dots[id];
      if (v === undefined) { dot.style.display = "none"; continue; }
      dot.setAttribute("cx", x(t)); dot.setAttribute("cy", y(v)); dot.style.display = "block";
      const row = document.createElement("div"); row.className = "r";
      const k = document.createElement("span"); const key = document.createElement("i"); key.style.cssText = `display:inline-block;width:12px;height:2px;margin-right:5px;vertical-align:middle;background:${nodeColor(id)}`;
      k.appendChild(key); k.appendChild(document.createTextNode(label(id)));
      const b = document.createElement("b"); b.textContent = fmtUnit(v, unit);
      row.appendChild(b); row.appendChild(k); tip.appendChild(row);
    }
    tip.style.display = "block";
    const left = (ev.clientX - r.left) + 14, flip = left + 160 > r.width;
    tip.style.left = flip ? (ev.clientX - r.left - 14 - tip.offsetWidth) + "px" : left + "px";
    tip.style.top = (ev.clientY - r.top + 20) + "px";
  });
  hit.addEventListener("pointerleave", () => { xh.style.display = "none"; tip.style.display = "none"; for (const id of ids) dots[id].style.display = "none"; });
  return el;
}
