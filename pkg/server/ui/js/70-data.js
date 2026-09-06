function spanText(r) {
  let s = r.start_key || "", e = r.end_key || "";
  if (r.table) {
    const p = "/table/" + r.table;
    const trim = k => k === p ? "/" : (k.startsWith(p + "/") ? k.slice(p.length) : k);
    s = trim(s); e = trim(e);
    return `<b>${esc(r.table)}</b> · ${esc(s)} → ${esc(e)}`;
  }
  return `${esc(s)} → ${esc(e)}`;
}
let openRange = null;
let dataFilter = "";
function setDataFilter(v) {
  dataFilter = (v || "").trim().toLowerCase();
  const box = document.getElementById("data-filter");
  if (box.value !== v) box.value = v || "";
}
function matchesFilter(r) {
  if (!dataFilter) return true;
  return (r.table || "").toLowerCase().includes(dataFilter) || ("r" + r.range_id).includes(dataFilter);
}
// openRangeDetail is how a route (or jump-to) opens a range: it lands
// on the view with that range's per-replica detail already expanded.
function openRangeDetail(id) {
  if (openRange === id) return;
  openRange = id;
  if (lastCluster) renderClusterRanges(lastCluster.ranges || []);
  fetchRangeDetail(id);
}
function renderClusterRanges(ranges) {
  const admin = canDrillDown();
  const hint = document.getElementById("cluster-range-hint");
  setHTML(hint, admin
    ? "click a range, or focus it and press Enter, for its per-replica detail from every holding node (admin role)"
    : `per-replica detail needs the admin role — ${drillDownRefusal()}`);
  const items = [];
  for (const r of ranges.filter(matchesFilter).slice().sort((a, b) => a.range_id - b.range_id)) {
    items.push({ key: "r" + r.range_id, html: `<tr class="${admin ? "clickable" : ""}" data-key="r${r.range_id}" data-range="${r.range_id}"${admin ? ` tabindex="0" role="button" aria-expanded="${r.range_id === openRange}" title="Enter or click: per-replica detail from every holding node"` : ""}>
      <td>r${r.range_id}</td>
      <td class="key">${spanText(r)}</td>
      <td>${(r.replicas || []).map(x => "n" + x).join(" ")}</td>
    </tr>` });
    // The open detail row is its own element, filled by its fetch and
    // left alone by re-renders (its identity survives them).
    if (r.range_id === openRange) items.push({ key: "d" + r.range_id, html: null });
  }
  renderKeyed(document.getElementById("cluster-ranges"), items);
}
{
  const tbody = document.getElementById("cluster-ranges");
  const rangeOf = ev => { const tr = ev.target.closest("tr.clickable"); return tr && tbody.contains(tr) ? Number(tr.dataset.range) : null; };
  tbody.addEventListener("click", ev => { const id = rangeOf(ev); if (id !== null && !ev.target.closest("tr.detailrow")) toggleRangeDetail(id); });
  tbody.addEventListener("keydown", ev => { if (ev.key !== "Enter" && ev.key !== " ") return; const id = rangeOf(ev); if (id !== null) { ev.preventDefault(); toggleRangeDetail(id); } });
}
function detailRowFor(id) {
  const tbody = document.getElementById("cluster-ranges");
  let el = tbody.querySelector(`tr[data-key="d${id}"]`);
  if (!el) {
    el = document.createElement("tr");
    el.className = "detailrow"; el.dataset.key = "d" + id;
    el.innerHTML = `<td class="detail" colspan="3"></td>`;
    const row = tbody.querySelector(`tr[data-key="r${id}"]`);
    if (row) row.after(el); else tbody.appendChild(el);
  }
  return el.firstElementChild;
}
function toggleRangeDetail(id) {
  if (openRange === id) {
    openRange = null;
    document.getElementById("cluster-ranges").querySelector(`tr[data-key="d${id}"]`)?.remove();
    if (lastCluster) renderClusterRanges(lastCluster.ranges || []);
    pushRoute();
    return;
  }
  if (openRange !== null) document.getElementById("cluster-ranges").querySelector(`tr[data-key="d${openRange}"]`)?.remove();
  openRange = id;
  if (lastCluster) renderClusterRanges(lastCluster.ranges || []);
  pushRoute({ range: String(id) });
  fetchRangeDetail(id);
}
// fetchRangeDetail fills the open row from every holding node's view of
// the range (admin role; the refusal names the user and the GRANT).
async function fetchRangeDetail(id) {
  const cell = detailRowFor(id);
  cell.innerHTML = `<div class="note">loading r${id}…</div>`;
  try {
    const resp = await fetch("/api/range?id=" + id, { cache: "no-store" });
    if (resp.status === 403) {
      cell.innerHTML = `<div class="note">${drillDownRefusal()}</div>`;
    } else if (!resp.ok) {
      cell.innerHTML = `<div class="note">error: HTTP ${resp.status}</div>`;
    } else {
      const d = await resp.json();
      cell.innerHTML = `<table>
        <caption class="sr-only">Range r${id}: per-replica detail</caption>
        <thead><tr><th scope="col">node</th><th scope="col">status</th><th scope="col">locality</th><th scope="col">leader</th>
          <th scope="col" class="num">applied</th><th scope="col" class="num">size</th><th scope="col" class="num">qps</th>
          <th scope="col">closed ts</th></tr></thead>
        <tbody>` + (d.replicas || []).map(rep => {
          if (rep.error) return `<tr><td>n${rep.node_id}</td>
            <td>${statusCell({ live: rep.live })}</td>
            <td>${esc(rep.locality || "—")}</td>
            <td colspan="5" class="key">${esc(rep.error)}</td></tr>`;
          const s = rep.status || {};
          return `<tr>
            <td>n${rep.node_id}</td>
            <td>${statusCell({ live: rep.live })}</td>
            <td>${esc(rep.locality || "—")}</td>
            <td>${s.leader ? "★ leader" : ""}</td>
            <td class="num">${s.applied_index ?? ""}</td>
            <td class="num">${fmtBytes(s.size_bytes || 0)}</td>
            <td class="num">${s.qps ? s.qps.toFixed(0) : "0"}</td>
            <td class="key">${esc(s.closed_timestamp || "—")}</td>
          </tr>`;
        }).join("") + `</tbody></table>`;
    }
  } catch (err) {
    cell.innerHTML = `<div class="note">error: ${esc(err.message || err)}</div>`;
  }
}
// Problems panel: from the overview's health section. Rows link to the
// dashboard section that shows the underlying figure.
