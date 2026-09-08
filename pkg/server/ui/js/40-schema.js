let schemaFilter = "";
function setSchemaFilter(v) {
  schemaFilter = (v || "").trim().toLowerCase();
  const box = document.getElementById("schema-filter");
  if (box.value !== v) box.value = v || "";
}
function fmtRetention(sec) {
  if (sec % 86400 === 0) return (sec / 86400) + "d";
  if (sec % 3600 === 0) return (sec / 3600) + "h";
  return sec + "s";
}
function renderSchema(d) {
  lastSchema = d;
  // #/schema is the list; #/schema/<table> is the level below it
  // (issue #194). One document serves both, so the route decides which
  // section is on screen rather than a second poll.
  const detail = ui.view === "schema" && !!ui.table;
  document.getElementById("sec-schema").hidden = detail;
  document.getElementById("sec-table").hidden = !detail;
  if (detail) { renderTableDetail(d); return; }
  const tbody = document.getElementById("schema");
  const tables = (d.tables || []).filter(t => !schemaFilter || t.name.toLowerCase().includes(schemaFilter));
  renderKeyed(tbody, tables.map(t => {
    const cols = (t.columns || []).filter(c => !c.hidden);
    const hiddenNames = new Set((t.columns || []).filter(c => c.hidden).map(c => c.name));
    const pk = (t.primary_key || []).filter(n => !hiddenNames.has(n));
    const colList = cols.map(c => `${c.name} ${c.type}${c.precision ? `(${c.precision},${c.scale})` : ""}${c.not_null ? " not null" : ""}`).join("\n");
    const idx = (t.indexes || []).map(i => `${i.unique ? "unique " : ""}${esc(i.name)} (${(i.columns || []).map(esc).join(", ")})${i.state === "write-only" ? " " + tok("draining", "building") : ""}`).join("<br>");
    const ts = t.view ? `<div class="muted" style="font-size:12px" title="${esc(t.definition || "")}">view</div>` : t.timeseries ? `<div class="muted" style="font-size:12px">timeseries${t.retention_seconds ? " · retention " + fmtRetention(t.retention_seconds) : ""}${t.shards ? " · " + t.shards + " shards" : ""}</div>` : "";
    const st = t.stats;
    const rows = t.view ? "—" : st ? st.row_count.toLocaleString() : `<span class="muted">not analyzed</span>`;
    const age = st ? (st.stale ? warn("draining", fmtWhen(Date.now() - st.age_seconds * 1000)) : fmtWhen(Date.now() - st.age_seconds * 1000)) : "—";
    const grants = Object.entries(t.privileges || {}).map(([u, p]) => `${esc(u)}: ${p.map(esc).join(",").toLowerCase()}`).join("<br>") || `<span class="muted">admins only</span>`;
    const key = (t.database || "") + "." + t.name;
    const to = tableKey(t);
    return { key, html: `<tr data-key="${esc(key)}" class="clickable" data-table="${esc(to)}" tabindex="0" role="link" title="Enter or click: this table in full">
      <td data-label="table"><b>${esc(to)}</b>${ts}</td>
      <td data-label="columns" title="${esc(colList)}">${cols.length}</td>
      <td class="key" data-label="primary key" title="${hiddenNames.size ? "led by the hidden shard column" : ""}">${pk.map(esc).join(", ")}</td>
      <td data-label="indexes">${idx || "—"}</td>
      <td class="num" data-label="rows">${rows}</td>
      <td class="num" data-label="stats age">${age}</td>
      <td class="num" data-label="ranges">${t.ranges}</td>
      <td class="num" data-label="local size" title="${t.local_replicas} local replicas, ${t.leaders_here} led here">${fmtBytes(t.local_bytes || 0)}</td>
      <td data-label="grants">${grants}</td>
    </tr>` };
  }));
  const note = document.getElementById("schema-note");
  note.textContent = (d.tables || []).length
    ? `${(d.tables || []).length} tables in the one namespace (the connection URL's database name is accepted and ignored); local size counts this node's replicas only. Click a table, or focus it and press Enter, for its columns, indexes, statistics, grants and DDL`
    : (d.principal && d.principal.secure && !d.principal.admin ? "no tables granted to " + d.principal.user : "no tables yet");
  // Users used to be a small table appended here. #/security owns roles
  // now (issue #156) and draws them from /api/security with their
  // membership resolved, so this no longer renders them: the wrapper it
  // reached for no longer exists — which made this line throw and take
  // the whole schema poll with it for any admin — and #users is that
  // view's table, which this would have overwritten with a two-column
  // list every time the schema refreshed.
}
async function pollSchema() {
  const resp = await fetch("/api/schema", { cache: "no-store" });
  if (!resp.ok) throw new Error("HTTP " + resp.status);
  renderSchema(await resp.json());
}
// SQL activity: connection states and statement counters from every
// node's heartbeat (live for the serving node). Counters are cumulative,
// so rates are differences between consecutive polls.
