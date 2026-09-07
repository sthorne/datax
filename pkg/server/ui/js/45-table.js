// ---- The table detail (#/schema/<table>, issue #194) ----
//
// The list above is a good index; it is not a description. Everything
// the reader needs to know about one table was in a `title` attribute —
// unreachable on touch, awkward from a keyboard, unreadable at forty
// columns and impossible to copy. This is the level below it, and its
// audience is not the operator: it is the person who wants to know
// whether their index finished building, whether the statistics are
// fresh enough for the planner to pick it, who has been granted what,
// and what the DDL actually is.

// tableKey names a table the way the page writes it and the way the
// route carries it: qualified only when the database is not the default
// one, so #/schema/items and the row that leads to it agree.
function tableKey(t) {
  return t.database && t.database !== "datax" ? t.database + "." + t.name : t.name;
}
function findTable(d, key) {
  return (d.tables || []).find(t => tableKey(t) === key);
}

// ---- The reconstructed DDL ----
//
// Every field this needs was already in /api/schema except the column
// defaults and the constraints, which is exactly the signal the issue
// named: the view asked for something the API should have been
// returning, so the API returns it now rather than the view guessing.
// Quoting errs wide on purpose: quoting a word that did not need it
// changes nothing, and failing to quote one that did produces DDL that
// will not parse. So anything that is not a plain lower-case identifier
// is quoted, and so is every word SQL reserves.
const SQL_RESERVED = new Set(("abort add all alter analyze and any array as asc begin between by case cast check " +
  "collate column commit constraint create cross current_date current_time current_timestamp default deferrable " +
  "delete desc distinct do drop else end except exists explain false fetch for foreign from grant group having " +
  "if ilike in index initially inner insert intersect interval into is join key left like limit local natural " +
  "not null offset on only or order outer password primary references returning revoke right rollback select " +
  "session set show start table tables then to transaction true truncate union unique update user using values " +
  "view when where window with").split(" "));
function quoteIdent(name) {
  const s = String(name);
  return /^[a-z_][a-z0-9_]*$/.test(s) && !SQL_RESERVED.has(s)
    ? s : `"${s.replace(/"/g, '""')}"`;
}
// qualifiedIdent is the name the DDL has to name: qualified when the
// table is not in the default database, so the statement can be run as
// it stands.
function qualifiedIdent(t) {
  return t.database && t.database !== "datax"
    ? quoteIdent(t.database) + "." + quoteIdent(t.name) : quoteIdent(t.name);
}
function columnDDL(c) {
  let s = "  " + quoteIdent(c.name) + " " + c.type;
  if (c.not_null) s += " NOT NULL";
  if (c.default) s += " DEFAULT " + c.default;
  return s;
}
function constraintDDL(c) {
  const cols = (c.columns || []).map(quoteIdent).join(", ");
  let body;
  if (c.kind === "check") body = `CHECK (${c.expr})`;
  else if (c.kind === "foreign") {
    body = `FOREIGN KEY (${cols}) REFERENCES ${quoteIdent(c.ref_table || "?")} (${(c.ref_columns || []).map(quoteIdent).join(", ")})`;
    if (c.on_delete && c.on_delete !== "restrict") body += " ON DELETE " + c.on_delete.toUpperCase();
    if (c.on_update && c.on_update !== "restrict") body += " ON UPDATE " + c.on_update.toUpperCase();
  } else body = `UNIQUE (${cols})`;
  return `CONSTRAINT ${quoteIdent(c.name)} ${body}${c.validated ? "" : " NOT VALID"}`;
}
function tableDDL(t) {
  const name = qualifiedIdent(t);
  if (t.view) return `CREATE VIEW ${name} AS\n${(t.definition || "").trim() || "  -- definition unavailable"};`;
  const parts = (t.columns || []).map(columnDDL);
  if ((t.primary_key || []).length) parts.push("  PRIMARY KEY (" + t.primary_key.map(quoteIdent).join(", ") + ")");
  for (const c of t.constraints || []) parts.push("  " + constraintDDL(c));
  let opts = "";
  if (t.timeseries) {
    const o = ["timeseries"];
    if (t.retention_seconds) o.push("retention = '" + fmtRetention(t.retention_seconds) + "'");
    if (t.shards) o.push("shards = " + t.shards);
    opts = " WITH (" + o.join(", ") + ")";
  }
  let sql = `CREATE TABLE ${name} (\n${parts.join(",\n")}\n)${opts};`;
  for (const i of t.indexes || []) {
    sql += `\nCREATE ${i.unique ? "UNIQUE " : ""}INDEX ${quoteIdent(i.name)} ON ${name} (${(i.columns || []).map(quoteIdent).join(", ")});`;
  }
  if (t.comment) sql += `\nCOMMENT ON TABLE ${name} IS '${t.comment.replace(/'/g, "''")}';`;
  return sql;
}

function renderTableDetail(d) {
  const t = findTable(d, ui.table);
  const title = document.getElementById("table-title");
  const summary = document.getElementById("table-summary");
  const back = `<a href="${routeTo("schema")}">back to the schema</a>`;
  if (!t) {
    title.textContent = ui.table || "Table";
    // Not found and not permitted are the same document to a non-admin,
    // so the note says both rather than asserting the table is gone.
    setHTML(summary, (d.tables || []).length || (d.principal && d.principal.admin)
      ? `No table called <b>${esc(ui.table)}</b> is visible to you — it may not exist, or you may hold no privilege on it. ${back}`
      : `Waiting for the schema. ${back}`);
    for (const id of ["table-columns", "table-indexes", "table-constraints", "table-grants", "table-shapes"]) {
      setHTML(document.getElementById(id), "");
    }
    for (const id of ["table-stats", "table-ranges", "table-columns-note", "table-indexes-note", "table-shapes-note", "table-ddl-note"]) {
      setHTML(document.getElementById(id), "");
    }
    setHTML(document.getElementById("table-ddl"), "");
    return;
  }
  title.textContent = tableKey(t);
  const what = t.view ? "view" : t.timeseries ? "time-series table" : "table";
  const bits = [`${what} #${t.id}`, `descriptor version ${t.version}`];
  if (t.timeseries) {
    bits.push(t.retention_seconds ? "retention " + fmtRetention(t.retention_seconds) : "no retention set");
    bits.push((t.shards || 1) + " shard" + ((t.shards || 1) === 1 ? "" : "s"));
  }
  if (t.comment) bits.push(esc(t.comment));
  setHTML(summary, bits.join(" · ") + " · " + back);

  renderTableColumns(t);
  renderTableIndexes(t);
  renderTableConstraints(t);
  renderTableStats(t);
  renderTableRanges(t);
  renderTableGrants(t);
  renderTableShapes(t);
  setHTML(document.getElementById("table-ddl"), esc(tableDDL(t)));
  setHTML(document.getElementById("table-ddl-note"),
    t.view ? "rebuilt from the descriptor, not the text you typed" :
      "rebuilt from the descriptor, not the text you typed: identifiers are quoted where they need it and the clauses are in the order the catalog stores them");
}

function renderTableColumns(t) {
  const pk = new Set(t.primary_key || []);
  const cols = t.columns || [];
  renderKeyed(document.getElementById("table-columns"), cols.length ? cols.map(c => ({
    key: c.name,
    html: `<tr data-key="${esc(c.name)}">
      <td data-label="column"><b>${esc(c.name)}</b>${pk.has(c.name) ? ' <span class="muted">pk</span>' : ""}</td>
      <td data-label="type" class="key">${esc(c.type)}</td>
      <td data-label="nullability">${c.not_null ? "not null" : `<span class="muted">nullable</span>`}</td>
      <td data-label="default" class="key">${c.default ? esc(c.default) : "—"}</td>
      <td data-label="hidden">${c.hidden ? `<span class="st draining">hidden</span>` : "—"}</td>
    </tr>`,
  })) : [{ key: "none", html: `<tr data-key="none"><td colspan="5" class="muted">no columns</td></tr>` }]);
  const hidden = cols.filter(c => c.hidden);
  setHTML(document.getElementById("table-columns-note"), hidden.length
    ? `${hidden.map(c => esc(c.name)).join(", ")} ${hidden.length === 1 ? "is" : "are"} hidden: the system maintains ${hidden.length === 1 ? "it" : "them"} and SELECT * never returns ${hidden.length === 1 ? "it" : "them"}. A time-series table's shard column leads the primary key, which is why the key here is wider than the one you declared.`
    : `columns in descriptor order, which is the order the primary key and SELECT * use.`);
}

function renderTableIndexes(t) {
  const idx = t.indexes || [];
  renderKeyed(document.getElementById("table-indexes"), idx.length ? idx.map(i => ({
    key: i.name,
    html: `<tr data-key="${esc(i.name)}">
      <td data-label="index"><b>${esc(i.name)}</b></td>
      <td data-label="columns" class="key">${(i.columns || []).map(esc).join(", ")}</td>
      <td data-label="unique">${i.unique ? "unique" : "—"}</td>
      <td data-label="build state">${i.state === "write-only"
        ? `<span class="st draining">building</span>`
        : `<span class="muted">ready</span>`}</td>
    </tr>`,
  })) : [{ key: "none", html: `<tr data-key="none"><td colspan="4" class="muted">no secondary indexes — every read is a scan of the primary key or a lookup by it</td></tr>` }]);
  const building = idx.filter(i => i.state === "write-only");
  setHTML(document.getElementById("table-indexes-note"), building.length
    ? `${building.map(i => esc(i.name)).join(", ")} ${building.length === 1 ? "is" : "are"} still being built: writers maintain the index already, but the planner will not use it until the backfill finishes and the CREATE INDEX returns. How far along the backfill is is not published, so this says building, not a percentage.`
    : `the primary key is the table itself and is not listed here.`);
}

function renderTableConstraints(t) {
  const cs = t.constraints || [];
  renderKeyed(document.getElementById("table-constraints"), cs.length ? cs.map(c => ({
    key: c.name,
    html: `<tr data-key="${esc(c.name)}">
      <td data-label="constraint"><b>${esc(c.name)}</b> <span class="kind">${esc(c.kind)}</span></td>
      <td data-label="definition" class="key" style="max-width:none;white-space:normal">${esc(constraintDDL(c).replace(/^CONSTRAINT \S+ /, ""))}</td>
      <td data-label="validated">${c.validated ? "yes" : `<span class="st draining">not valid</span>`}</td>
    </tr>`,
  })) : [{ key: "none", html: `<tr data-key="none"><td colspan="3" class="muted">no CHECK, FOREIGN KEY or named UNIQUE constraints</td></tr>` }]);
}

function renderTableStats(t) {
  const el = document.getElementById("table-stats");
  if (t.view) { setHTML(el, `<span class="muted">a view owns no rows, so it has no statistics: the planner uses the statistics of the tables its query reads.</span>`); return; }
  const st = t.stats;
  if (!st) {
    setHTML(el, `<span class="st draining">never analyzed</span> — the planner is estimating this table's size from nothing, which is the usual reason it picks a scan over an index. Run <code class="key">ANALYZE ${esc(qualifiedIdent(t))}</code>.`);
    return;
  }
  const age = fmtAgo(st.age_seconds * 1000);
  const when = st.collected_at_unix_ms ? new Date(st.collected_at_unix_ms).toLocaleString() : "at an unrecorded time";
  setHTML(el, `<b>${st.row_count.toLocaleString()}</b> rows estimated, collected ${esc(when)}, ${st.stale ? warn("draining", age) : age}. `
    + (st.stale
      ? `Past the point where the background sampler refreshes them, so the planner may be choosing from figures the table has outgrown. <code class="key">ANALYZE ${esc(qualifiedIdent(t))}</code> collects them now.`
      : `Fresh enough for the planner to use. It is still an estimate: a bulk change can leave it far behind before it goes stale.`));
}

function renderTableRanges(t) {
  const el = document.getElementById("table-ranges");
  if (t.view) { setHTML(el, `<span class="muted">a view stores nothing, so it covers no ranges.</span>`); return; }
  const mine = ((lastCluster && lastCluster.ranges) || []).filter(r => r.table === t.name);
  const ids = mine.slice(0, 12).map(r => `r${r.range_id}`).join(", ");
  setHTML(el, `<b>${t.ranges}</b> range${t.ranges === 1 ? "" : "s"} cluster-wide`
    + (ids ? ` (${esc(ids)}${mine.length > 12 ? ", …" : ""})` : "")
    + `. This node holds ${t.local_replicas} replica${t.local_replicas === 1 ? "" : "s"} of them, leads ${t.leaders_here}, and those replicas take ${fmtBytes(t.local_bytes || 0)} — the only bytes a node can measure without asking every other one. `
    + `<a href="${routeTo("data", { q: t.name })}">Show them under Ranges</a>.`);
}

function renderTableGrants(t) {
  const rows = Object.entries(t.privileges || {}).sort((a, b) => a[0].localeCompare(b[0]));
  renderKeyed(document.getElementById("table-grants"), rows.length ? rows.map(([role, privs]) => ({
    key: role,
    html: `<tr data-key="${esc(role)}">
      <td data-label="role"><b>${esc(role)}</b></td>
      <td data-label="privileges">${(privs || []).map(p => `<span class="kind">${esc(String(p).toLowerCase())}</span>`).join(" ")}</td>
    </tr>`,
  })) : [{ key: "none", html: `<tr data-key="none"><td colspan="2" class="muted">no grants — the owner and the admin role reach it, nobody else</td></tr>` }]);
}

// Which statement shapes touch this table: a filter over the document
// #/sql already fetches, and the most useful thing this page can say.
function renderTableShapes(t) {
  const body = document.getElementById("table-shapes");
  const note = document.getElementById("table-shapes-note");
  // Every state of this table goes through renderKeyed, refusal
  // included: a row the reconciler does not own is a row it will not
  // take away again when the answer changes.
  if (!stmtDoc) {
    renderKeyed(body, [{ key: "refused", html: `<tr data-key="refused"><td colspan="5" class="note">statement shapes need the admin role — ${drillDownRefusal()}</td></tr>` }]);
    setHTML(note, "a shape's representative statement can carry data, so the list is gated with the rest of the statement surface");
    return;
  }
  const name = t.name.toLowerCase(), qual = tableKey(t).toLowerCase();
  const shapes = (stmtDoc.statements || [])
    .filter(s => (s.tables || []).some(x => { const l = String(x).toLowerCase(); return l === name || l === qual; }))
    .sort((a, b) => (b.total_us || 0) - (a.total_us || 0));
  renderKeyed(body, shapes.length ? shapes.map(s => ({
    key: s.fingerprint,
    html: `<tr data-key="${esc(s.fingerprint)}">
      <td class="key" style="max-width:none;white-space:normal"><a href="${routeTo("sql")}" class="table-shape" data-fp="${esc(s.fingerprint)}">${esc(s.shape)}</a></td>
      <td class="num" data-label="executions">${fmtCount(s.count)}</td>
      <td class="num" data-label="mean">${fmtMicros(s.mean_us)}</td>
      <td class="num" data-label="rows scanned">${fmtCount(s.rows_scanned)}</td>
      <td class="num" data-label="total time">${fmtMicros(s.total_us)}</td>
    </tr>`,
  })) : [{ key: "none", html: `<tr data-key="none"><td colspan="5" class="muted">no recorded shape names this table</td></tr>` }]);
  setHTML(note, shapes.length
    ? `the shapes each node has recorded that name this table, heaviest first. Each node keeps a bounded table of them, so this is a window over the busiest, not every statement that ever ran.`
    : `no shape in the recorded window names this table. That is not proof nothing reads it: the tables a shape reports come from the parsed statement, and each node keeps a bounded number of shapes.`);
}

// renderTableShapesIfOpen redraws just the shapes when the statements
// document changed (or was refused) while the detail is on screen.
function renderTableShapesIfOpen() {
  if (ui.view !== "schema" || !ui.table || !lastSchema) return;
  const t = findTable(lastSchema, ui.table);
  if (t) renderTableShapes(t);
}

// wireTableDetail: the list's rows lead here, and the DDL copies.
function wireTableDetail() {
  const tbody = document.getElementById("schema");
  const nameOf = ev => { const tr = ev.target.closest("tr[data-table]"); return tr && tbody.contains(tr) ? tr.dataset.table : null; };
  tbody.addEventListener("click", ev => {
    if (ev.target.closest("a, button")) return;
    const name = nameOf(ev);
    if (name) go("schema/" + encodeURIComponent(name));
  });
  tbody.addEventListener("keydown", ev => {
    if (ev.key !== "Enter" && ev.key !== " ") return;
    const name = nameOf(ev);
    if (name) { ev.preventDefault(); go("schema/" + encodeURIComponent(name)); }
  });
  document.getElementById("table-ddl-copy").addEventListener("click", async () => {
    const btn = document.getElementById("table-ddl-copy");
    const text = document.getElementById("table-ddl").textContent;
    try {
      // The clipboard API needs a secure context; over plain HTTP it
      // rejects, and saying so beats a button that silently does nothing.
      await navigator.clipboard.writeText(text);
      btn.textContent = "copied";
    } catch {
      btn.textContent = "select it and copy";
      const r = document.createRange();
      r.selectNodeContents(document.getElementById("table-ddl"));
      const sel = window.getSelection();
      sel.removeAllRanges(); sel.addRange(r);
    }
    setTimeout(() => { btn.textContent = "copy"; }, 2000);
  });
  // A shape here opens it where its detail lives, rather than growing a
  // second copy of the statement panel on this page.
  document.getElementById("table-shapes").addEventListener("click", ev => {
    const a = ev.target.closest("a.table-shape");
    if (!a) return;
    ev.preventDefault();
    stmtOpen = a.dataset.fp;
    go("sql");
  });
}
