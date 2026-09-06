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
  const tbody = document.getElementById("schema");
  const tables = (d.tables || []).filter(t => !schemaFilter || t.name.toLowerCase().includes(schemaFilter));
  renderKeyed(tbody, tables.map(t => {
    const cols = (t.columns || []).filter(c => !c.hidden);
    const hiddenNames = new Set((t.columns || []).filter(c => c.hidden).map(c => c.name));
    const pk = (t.primary_key || []).filter(n => !hiddenNames.has(n));
    const colList = cols.map(c => `${c.name} ${c.type}${c.precision ? `(${c.precision},${c.scale})` : ""}${c.not_null ? " not null" : ""}`).join("\n");
    const idx = (t.indexes || []).map(i => `${i.unique ? "unique " : ""}${esc(i.name)} (${(i.columns || []).map(esc).join(", ")})${i.state === "write-only" ? ' <span class="st draining">building</span>' : ""}`).join("<br>");
    const ts = t.view ? `<div class="muted" style="font-size:12px" title="${esc(t.definition || "")}">view</div>` : t.timeseries ? `<div class="muted" style="font-size:12px">timeseries${t.retention_seconds ? " · retention " + fmtRetention(t.retention_seconds) : ""}${t.shards ? " · " + t.shards + " shards" : ""}</div>` : "";
    const st = t.stats;
    const rows = t.view ? "—" : st ? st.row_count.toLocaleString() : `<span class="muted">not analyzed</span>`;
    const age = st ? (st.stale ? `<span class="st draining">${fmtAgo(st.age_seconds * 1000)}</span>` : fmtAgo(st.age_seconds * 1000)) : "—";
    const grants = Object.entries(t.privileges || {}).map(([u, p]) => `${esc(u)}: ${p.map(esc).join(",").toLowerCase()}`).join("<br>") || `<span class="muted">admins only</span>`;
    const key = (t.database || "") + "." + t.name;
    return { key, html: `<tr data-key="${esc(key)}">
      <td data-label="table"><b>${esc(t.database && t.database !== "datax" ? t.database + "." + t.name : t.name)}</b>${ts}</td>
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
    ? `${(d.tables || []).length} tables in the one namespace (the connection URL's database name is accepted and ignored); local size counts this node's replicas only`
    : (d.principal && d.principal.secure && !d.principal.admin ? "no tables granted to " + d.principal.user : "no tables yet");
  const uw = document.getElementById("users-wrap");
  if (d.users && d.users.length) {
    uw.hidden = false;
    renderKeyed(document.getElementById("users"), d.users.map(u => ({ key: u.name, html: `<tr data-key="${esc(u.name)}"><td>${esc(u.name)}</td><td>${u.admin ? '<span class="role">admin</span>' : "user"}</td></tr>` })));
  } else uw.hidden = true;
}
async function pollSchema() {
  const resp = await fetch("/api/schema", { cache: "no-store" });
  if (!resp.ok) throw new Error("HTTP " + resp.status);
  renderSchema(await resp.json());
}
// SQL activity: connection states and statement counters from every
// node's heartbeat (live for the serving node). Counters are cumulative,
// so rates are differences between consecutive polls.
