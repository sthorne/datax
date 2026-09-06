"use strict";
// The console is assembled from the files in pkg/server/ui/js, in name
// order, and served as one page (issue #151): several files to read and
// edit, one self-contained page to serve, no build step and no second
// request that an airgapped node could not answer.
//
// CONSOLE_VERSION is stamped by the node when it serves the page.
const CONSOLE_VERSION = "__CONSOLE_VERSION__";
// Read-only dashboard: polls /api/cluster every 3s and renders in place.
// Sparkline history lives only in this tab.
// The last document each poll produced, so a view can redraw when a
// control changes without waiting for the next fetch.
let lastCluster = null, lastSchema = null;
const HISTORY = 40;
const hist = {}; // name -> number[]
function pushHist(name, v) {
  const h = hist[name] || (hist[name] = []);
  h.push(v);
  if (h.length > HISTORY) h.shift();
  return h;
}
function esc(s) {
  return String(s).replace(/[&<>"]/g, c => ({"&":"&amp;","<":"&lt;",">":"&gt;",'"':"&quot;"}[c]));
}
// setHTML replaces an element's markup only when it changed, and never
// under an interaction (issue #148): while the element holds the text
// selection, the focused element or an open disclosure, the new markup
// waits and lands when the interaction ends.
function interacting(el) {
  if (el.contains(document.activeElement) && document.activeElement !== document.body) return true;
  if (el.querySelector("details[open]")) return true;
  const sel = window.getSelection();
  return !!(sel && !sel.isCollapsed && sel.anchorNode && el.contains(sel.anchorNode));
}
const pendingHTML = new Map();
function setHTML(el, html) {
  if (el.__html === html) { pendingHTML.delete(el); return false; }
  if (interacting(el)) { pendingHTML.set(el, html); return false; }
  el.innerHTML = html; el.__html = html; pendingHTML.delete(el);
  return true;
}
function flushPendingHTML() { for (const [el, html] of [...pendingHTML]) setHTML(el, html); }
// renderKeyed reconciles a container's children by identity: items are
// {key, html} with html the child's full markup carrying data-key; a
// child whose markup is unchanged is left alone (its selection, focus
// and open disclosures with it), a changed one is replaced unless it is
// under an interaction (then it waits for the next pass), a new one is
// created, a vanished one removed, and the order follows items. An item
// with html null keeps whatever element already exists under its key
// (the range drill-down's detail row, filled in by its own fetch).
const keyedTpl = document.createElement("template");
function renderKeyed(container, items) {
  const existing = new Map();
  for (const el of container.children) if (el.dataset.key !== undefined) existing.set(el.dataset.key, el);
  const kept = new Set();
  let i = 0;
  for (const it of items) {
    let el = existing.get(it.key);
    if (it.html === null) { if (!el) continue; }
    else if (!el) { keyedTpl.innerHTML = it.html; el = keyedTpl.content.firstElementChild; el.__html = it.html; existing.set(it.key, el); }
    else if (el.__html !== it.html && !interacting(el)) { keyedTpl.innerHTML = it.html; const fresh = keyedTpl.content.firstElementChild; fresh.__html = it.html; el.replaceWith(fresh); el = fresh; existing.set(it.key, el); }
    kept.add(it.key);
    const at = container.children[i];
    if (at !== el) container.insertBefore(el, at || null);
    i++;
  }
  for (const [k, el] of existing) if (!kept.has(k) && !interacting(el)) el.remove();
}
function renderTiles(el, html) {
  keyedTpl.innerHTML = html;
  renderKeyed(el, [...keyedTpl.content.children].map(c => ({ key: c.dataset.key, html: c.outerHTML })));
}
document.addEventListener("selectionchange", () => { const sel = window.getSelection(); if (!sel || sel.isCollapsed) setTimeout(flushPendingHTML, 0); });
document.addEventListener("focusout", () => setTimeout(flushPendingHTML, 0));
document.addEventListener("toggle", () => setTimeout(flushPendingHTML, 0), true);
// a11yTables gives every table a caption (its section's heading, for
// screen readers) and column scope on its headers (issue #149).
function a11yTables(root) {
  for (const table of (root || document).querySelectorAll("table")) {
    if (!table.caption) {
      const h2 = table.closest("section")?.querySelector("h2");
      if (h2) { const c = table.createCaption(); c.className = "sr-only"; c.textContent = h2.textContent.trim(); }
    }
    for (const th of table.querySelectorAll("thead th")) if (!th.hasAttribute("scope")) th.setAttribute("scope", "col");
  }
}
function pct(v) { return (v === undefined || v === null) ? "—" : (v < 10 ? v.toFixed(1) : Math.round(v)) + "%"; }
function fmtDuration(sec) {
  const d = Math.floor(sec / 86400), h = Math.floor(sec % 86400 / 3600), m = Math.floor(sec % 3600 / 60);
  return d ? `${d}d ${h}h` : h ? `${h}h ${m}m` : `${m}m ${Math.floor(sec % 60)}s`;
}
// Nodes-table cells: colored when a figure is worth a look. Disk free
// goes amber under 15% and red under 5%; load above the core count and
// fds past 80% of the limit likewise.
// warn colours a figure worth a look and adds a text cue (colour is
// never the only carrier): "!" for a warning, "!!" for a critical one.
function warn(level, text) { return level ? `<span class="st ${level}">${text}<span class="cue" aria-label="${level === "down" ? "critical" : "warning"}">${level === "down" ? " !!" : " !"}</span></span>` : text; }
function loadCell(load1, cores) {
  const ratio = load1 / cores;
  return warn(ratio > 2 ? "down" : ratio > 1 ? "draining" : "", (load1 ?? 0).toFixed(2));
}
function memCell(m) {
  const used = m.mem_total - m.mem_available, frac = used / m.mem_total;
  return warn(frac > 0.95 ? "down" : frac > 0.85 ? "draining" : "", fmtBytes(used) + " / " + fmtBytes(m.mem_total));
}
function diskCell(m) {
  const frac = m.disk_free / m.disk_total;
  return warn(frac < 0.05 ? "down" : frac < 0.15 ? "draining" : "", fmtBytes(m.disk_free) + " (" + Math.round(frac * 100) + "%)");
}
function fdCell(m) {
  const frac = m.open_fds / m.fd_limit;
  return warn(frac > 0.95 ? "down" : frac > 0.8 ? "draining" : "", m.open_fds + " / " + m.fd_limit);
}
function fmtBytes(b) {
  if (b >= 1 << 30) return (b / (1 << 30)).toFixed(1) + " GiB";
  if (b >= 1 << 20) return (b / (1 << 20)).toFixed(1) + " MiB";
  if (b >= 1 << 10) return (b / (1 << 10)).toFixed(1) + " KiB";
  return Math.round(b) + " B";
}
function fmtAgo(ms) {
  if (ms < 1500) return "now";
  if (ms < 60000) return Math.round(ms / 1000) + "s ago";
  return Math.round(ms / 60000) + "m ago";
}
function spark(h) {
  if (h.length < 2) return "";
  const max = Math.max(...h, 1e-9), w = 100, ht = 26;
  const pts = h.map((v, i) =>
    (i * w / (h.length - 1)).toFixed(1) + "," + (ht - 2 - v / max * (ht - 4)).toFixed(1)
  ).join(" ");
  return `<svg viewBox="0 0 ${w} ${ht}" preserveAspectRatio="none" role="img" aria-label="recent trend"><polyline points="${pts}"/></svg>`;
}
// Tiles link to the same figure charted over time (the Metrics view);
// their sparkline is the last 15 minutes from the datax_metrics table
// when the recorder has data, else this tab's own memory of the polls.
const TILE_SERIES = {
  "this node QPS": "node.leader_qps", "this node data": "node.replica_bytes",
  "host cpu": "node.cpu_percent", "load": "node.load1", "memory": "node.mem_available", "process": "node.rss",
  "disk free": "store.disk_free", "disk i/o": "store.disk_write_bps", "network": "node.net_rx_bps",
  "file descriptors": "node.open_fds", "go runtime": "go.goroutines",
  "L0 files": "store.l0_files", "L0 sublevels": "store.l0_sublevels", "compaction debt": "store.compaction_debt",
  "memtables": "store.memtable_bytes", "write stalls": "store.write_stalls",
  "block cache": "store.block_cache_bytes", "cache hits": "store.block_cache_hits", "bloom hits": "store.bloom_hits",
  "connections": "sql.connections", "statements/s": "sql.statements", "40001/s": "sql.serialization_failures", "worst p99": "sql.p99_us",
};
const TILE_RATE = { "write stalls": 1, "statements/s": 1, "40001/s": 1, "cache hits": 1, "bloom hits": 1 };
let tileHist = {}; // series -> number[] from the table (this node, last 15 minutes)
// A tile's value is normally one figure, set large. Some are a phrase —
// "v0.53.1 · protocol v2 · cluster v16", "no statements planned" — and
// at the figure's size those wrapped into three and four lines of
// headline type that overran the card. The tile classifies by the
// length of the figure itself (a qualifier is already its own small
// line, so it does not count) rather than each caller remembering to,
// which also covers a value that only grows long in the field.
const LONG_VALUE = 12;
function valueClass(value) {
  const figure = String(value).replace(/<span class="muted">[\s\S]*?<\/span>/g, "").replace(/<[^>]*>/g, "").trim();
  return figure.length > LONG_VALUE ? " long" : "";
}
function tile(label, value, histName, histVal) {
  let sp = "";
  const series = TILE_SERIES[label];
  if (series && tileHist[series] && tileHist[series].length > 1) sp = spark(tileHist[series]);
  else if (histName !== undefined) sp = spark(pushHist(histName, histVal));
  const body = `<div class="label">${esc(label)}</div><div class="value${valueClass(value)}">${value}</div>${sp}`;
  if (!series) return `<div class="tile" data-key="${esc(label)}">${body}</div>`;
  return `<a class="tile" data-key="${esc(label)}" href="#/metrics?series=${encodeURIComponent(series)}${TILE_RATE[label] ? "&rate=1" : ""}" title="chart ${esc(series)} over time">${body}</a>`;
}
async function pollTileHistory() {
  try {
    const names = [...new Set(Object.values(TILE_SERIES))];
    const me = lastCluster ? lastCluster.node_id : null;
    if (!me) return;
    const resp = await fetch(`/api/metrics?series=${names.join(",")}&node=${me}&since=15m&rate=1`, { cache: "no-store" });
    if (!resp.ok) { tileHist = {}; return; }
    const d = await resp.json();
    const h = {};
    for (const s of d.series || []) { const pts = (s.nodes || {})[String(me)]; if (pts && pts.length > 1) h[s.name] = pts.map(p => p[1]); }
    tileHist = h;
  } catch (err) { tileHist = {}; }
}
function statusCell(n) {
  if (!n.live) return `<span class="st down"><span class="dot"></span>down</span>`;
  if (n.shutting_down) return `<span class="st draining"><span class="dot"></span>stopping</span>`;
  if (n.draining) return `<span class="st draining"><span class="dot"></span>draining</span>`;
  return `<span class="st live"><span class="dot"></span>live</span>`;
}
// who is the signed-in principal from the last /api/cluster document
// (null until the first poll). In insecure mode there is no identity and
// every viewer is treated as admin.
