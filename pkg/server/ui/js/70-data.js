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
// A key the server rendered at its prefix is copied as the shown form
// (issue #213) — not one a command takes — and the copy control says so
// rather than hand it over under a key's name. The label is the row's:
// the control copies the start key, and the server says whether that
// key was redacted (start_key_redacted, issue #252), so a caller who may
// read some tables is not told its role does not see a key it is
// looking at in full.
function rangeKeyLabel(r) {
  return r && r.start_key_redacted ? "start key as shown (your role does not see the full key)" : "start key";
}
// shownKeys names which of a row's keys are the shown form, for the CSV.
function shownKeys(r) {
  return r.start_key_redacted && r.end_key_redacted ? "both" : r.start_key_redacted ? "start" : r.end_key_redacted ? "end" : "none";
}
// RANGE_TABLE_CAP bounds the rows the ranges table renders at once
// (issue #227): a real cluster has hundreds, each row carries a copy
// control that is a tab stop, and the reader least able to use the
// drill-down — a non-admin, whose rows are not focusable — paid one stop
// per range to get past the table. The rest are one click away, and the
// CSV always carries every row the filter left.
const RANGE_TABLE_CAP = 200;
let rangesShowAll = false;
sortBy("ranges", {
  range: r => r.range_id, span: r => (r.table || "") + " " + (r.start_key || ""), replicas: r => (r.replicas || []).length,
  leader: r => r.load.leader, qps: r => r.load.qps, size: r => r.load.bytes,
});
function renderClusterRanges(ranges) {
  const admin = canDrillDown();
  // The document's any-of flag: a CSV has one header for every row.
  const shownForm = !!(lastCluster && lastCluster.keys_redacted);
  const hint = document.getElementById("cluster-range-hint");
  setHTML(hint, admin
    ? "click a range, or focus it and press Enter, for its per-replica detail from every holding node (admin role)"
    : `per-replica detail needs the admin role — ${drillDownRefusal()}`);
  const items = [];
  // The table's own order is heaviest first (issue #283): by the rate
  // its leader serves, then by size, then by id — so the first rows, and
  // the rows the cap keeps, are the ones a reader came to find. A range
  // with no advertised figure follows those that have one.
  const load = lastCluster ? rangeLoad(lastCluster) : new Map();
  const of = r => load.get(r.range_id) || {};
  const all = sortRows("ranges", ranges.filter(matchesFilter).map(r => Object.assign({ load: of(r) }, r))
    .sort((a, b) => (b.load.qps || 0) - (a.load.qps || 0) || (b.load.bytes || 0) - (a.load.bytes || 0) || a.range_id - b.range_id));
  const shown = rangesShowAll ? all : all.slice(0, RANGE_TABLE_CAP);
  for (const r of shown) {
    items.push({ key: "r" + r.range_id, html: `<tr class="${admin ? "clickable" : ""}" data-key="r${r.range_id}" data-range="${r.range_id}"${admin ? ` tabindex="0" role="button" aria-expanded="${r.range_id === openRange}" title="Enter or click: per-replica detail from every holding node"` : ""}>
      <td><span${admin ? ' class="opens"' : ""}>r${r.range_id}</span></td>
      <td class="key">${spanText(r)}${copyBtn("r" + r.range_id + "'s " + rangeKeyLabel(r), r.start_key || "")}</td>
      <td>${(r.replicas || []).map(x => "n" + x).join(" ")}</td>
      <td>${r.load.leader ? "n" + r.load.leader : "—"}</td>
      <td class="num">${r.load.qps ? r.load.qps.toFixed(0) : "—"}</td>
      <td class="num">${r.load.bytes ? fmtBytes(r.load.bytes) : "—"}</td>
    </tr>` });
    // The open detail row is its own element, filled by its fetch and
    // left alone by re-renders (its identity survives them).
    if (r.range_id === openRange) items.push({ key: "d" + r.range_id, html: null });
  }
  renderKeyed(document.getElementById("cluster-ranges"), items);
  // The cap line holds a button, and this runs on every poll: through
  // setHTML, so unchanged markup is left alone and a change waits while
  // the reader is on the button rather than replacing it under them
  // (the UI and security reviews on the PR; the #148/#149 class). The
  // empty case goes the same way, so the helper's record of what the
  // element holds stays true.
  const cap = document.getElementById("cluster-range-cap");
  if (all.length > shown.length) {
    setHTML(cap, `showing the first ${esc(String(shown.length))} of ${esc(String(all.length))} ranges · <button type="button" class="linkish" data-ranges="all">show all ${esc(String(all.length))}</button> · the CSV carries every row`);
  } else if (rangesShowAll && all.length > RANGE_TABLE_CAP) {
    setHTML(cap, `showing all ${esc(String(all.length))} ranges · <button type="button" class="linkish" data-ranges="first">show the first ${RANGE_TABLE_CAP}</button>`);
  } else {
    setHTML(cap, "");
  }
  // The rows the filter left, all of them, with the keys untrimmed: the
  // cell shortens a span to what fits a column, and a key that has been
  // shortened is not one a command will take (issue #205). With the
  // any-of header, a column says which rows the shown form applies to.
  setCSV("ranges", ["range", "table", shownForm ? "start key (as shown)" : "start key", shownForm ? "end key (as shown)" : "end key", "replicas", "leader", "qps", "size bytes"].concat(shownForm ? ["keys as shown"] : []),
    all.map(r => ["r" + r.range_id, r.table || "", r.start_key || "", r.end_key || "",
      (r.replicas || []).map(x => "n" + x).join(" "), r.load.leader ? "n" + r.load.leader : "", r.load.qps ? Math.round(r.load.qps) : "", r.load.bytes || ""].concat(shownForm ? [shownKeys(r)] : [])));
  setHTML(document.getElementById("cluster-range-load-note"),
    "heaviest first unless a column is sorted. Leader, qps and size are what each node advertises in its heartbeat for its heaviest and largest ranges, so a quiet, small range shows a dash. QPS is the leader's own rate over a range it leads — it is not summed across nodes, and a range whose leader is down reports none.");
}
{
  const cap = document.getElementById("cluster-range-cap");
  cap.addEventListener("click", ev => {
    const b = ev.target.closest("button[data-ranges]");
    if (!b) return;
    rangesShowAll = b.dataset.ranges === "all";
    if (lastCluster) renderClusterRanges(lastCluster.ranges || []);
    // Keep the reader where the control was: the replacement control
    // takes the focus the pressed one had.
    const next = cap.querySelector("button[data-ranges]");
    if (next) next.focus();
  });
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
    el.innerHTML = `<td class="detail" colspan="6"></td>`;
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
    // The closed range leaves the URL, whichever name it came under.
    const p = new URLSearchParams(ui.params);
    p.delete("r");
    if (/^\d+$/.test(p.get("range") || "")) p.delete("range");
    ui.params = p;
    pushRoute(p);
    return;
  }
  if (openRange !== null) document.getElementById("cluster-ranges").querySelector(`tr[data-key="d${openRange}"]`)?.remove();
  openRange = id;
  if (lastCluster) renderClusterRanges(lastCluster.ranges || []);
  pushRoute({ r: String(id) });
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
