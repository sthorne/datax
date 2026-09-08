// healthLink turns a check's section into the route that now shows the
// figure (issue #151): the old #sec-* anchors parsed as an unknown path
// and fell through to the overview, stopping the other views' polls.
function healthLink(section) {
  const entry = SECTION_ROUTE[section];
  if (!entry) return "";
  let [path, sec] = entry;
  if (path === "self") path = lastCluster ? "node/" + lastCluster.node_id : "nodes";
  return routeTo(path, sec ? { sec } : {});
}
// renderHealth draws the problems panel from a /api/health document (the
// overview poll carries one), and puts the cluster's severity where an
// operator with thirty tabs can see it (issue #150): the favicon takes
// the colour of the worst open problem — steady, never animated — and
// the title carries the count: "(2) datax — n1".
//
// Each row says when this node's checks first found it (issue #207) —
// a moment, so it reads the viewer's way — and links to its history:
// the operations view filtered to the health records that name the
// check, which is where "has this been flapping" is answered.
function problemSince(p) {
  if (!p.since_unix_ms) return "";
  return `<span class="since" title="${esc("first found open by this node's checks at " + new Date(p.since_unix_ms).toISOString())}">${fmtWhen(p.since_unix_ms)}</span>`;
}
function problemHistory(p) {
  return `<a class="history" href="${routeTo("ops", { kind: "health", q: p.check })}" title="when this check appeared and cleared, from this node's event ring">history →</a>`;
}
const SEVERITY_RANK = { critical: 3, warning: 2, info: 1 };
let lastSeverity = null;
function renderHealth(h) {
  const box = document.getElementById("problems");
  const probs = h.problems || [];
  if (!probs.length) {
    renderKeyed(box, [{ key: "ok", html: `<span class="ok" data-key="ok">✓ no problems found (${h.checks} checks)</span>` }]);
  } else {
    renderKeyed(box, probs.map(p => ({ key: p.check + ":" + p.summary, html: `<div class="problem ${esc(p.severity)}" data-key="${esc(p.check + ":" + p.summary)}">
      <span class="sev">${esc(p.severity)}</span>
      <span class="check">${esc(p.check)}</span>
      <span>${esc(p.summary)}</span>
      ${problemSince(p)}
      <span class="links">${p.section && healthLink(p.section) ? `<a href="${healthLink(p.section)}">${esc(SECTION_NAMES[p.section] || p.section)} →</a>` : ""}${problemHistory(p)}</span>
    </div>` })));
  }
  const worst = probs.reduce((w, p) => (SEVERITY_RANK[p.severity] || 0) > (SEVERITY_RANK[w] || 0) ? p.severity : w, "");
  const open = probs.filter(p => p.severity !== "info").length;
  const node = lastCluster ? "n" + lastCluster.node_id : "";
  document.title = (open ? `(${open}) ` : "") + "datax" + (node ? " — " + node : "");
  if (worst !== lastSeverity) {
    lastSeverity = worst;
    const css = getComputedStyle(document.documentElement);
    drawFavicon((worst === "critical" ? css.getPropertyValue("--critical") : worst === "warning" ? css.getPropertyValue("--warning") : css.getPropertyValue("--good")).trim() || "#8a897f");
  }
}
// drawFavicon paints a disc of the given colour into the tab icon — on a
// canvas, so the page embeds no asset and references no URL. A
// half-opacity outline reads on light and dark browser chrome alike.
function drawFavicon(fill) {
  const c = document.createElement("canvas");
  c.width = c.height = 32;
  const g = c.getContext("2d");
  if (!g) return;
  g.beginPath(); g.arc(16, 16, 13, 0, 2 * Math.PI);
  g.fillStyle = fill; g.fill();
  g.lineWidth = 2; g.strokeStyle = "rgba(0,0,0,.35)"; g.stroke();
  try { document.getElementById("favicon").href = c.toDataURL("image/png"); } catch (err) { /* no canvas: keep the default icon */ }
}
drawFavicon("#8a897f");
function healthUnavailable(err) {
  setHTML(document.getElementById("problems"), `<span class="muted">health checks unavailable: ${esc(err)}</span>`);
}
