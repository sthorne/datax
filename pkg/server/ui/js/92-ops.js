// ---- Operations and Security (issue #151) ----
// The event ring the overview poll already carries, given its own view
// with the kind filter, and the security view: how this cluster
// authenticates, who its users are, and the audit stream separated from
// the operational one rather than interleaved with it.
//
// Both are node-scoped: an event ring is per node. The serving node's
// ring rides in /api/overview; another node's comes from its node
// document, which is admin-gated like every other fan-out.
const EVENTS_MAX = 300;
let eventsAll = [], eventsKinds = new Set(), eventsSeen = 0, eventsNode = 0;

// ingestEvents merges a document's events, keeping the ring bounded. A
// change of scoped node starts a fresh ring rather than mixing two.
function ingestEvents(nodeID, events, latest) {
  if (nodeID !== eventsNode) { eventsAll = []; eventsKinds = new Set(); eventsSeen = 0; eventsNode = nodeID; }
  for (const e of events || []) {
    if (e.seq && e.seq <= eventsSeen) continue;
    eventsAll.push(e);
    eventsKinds.add(e.kind);
    if (e.seq) eventsSeen = Math.max(eventsSeen, e.seq);
  }
  if (latest) eventsSeen = Math.max(eventsSeen, latest);
  if (eventsAll.length > EVENTS_MAX) eventsAll = eventsAll.slice(eventsAll.length - EVENTS_MAX);
}

function eventRow(e) {
  return { key: String(e.seq || (e.at + e.summary)), html: `<tr data-key="${esc(String(e.seq || (e.at + e.summary)))}">
    <td class="when" title="${esc(e.at)}">${fmtAgo(Date.now() - Date.parse(e.at))}</td>
    <td><span class="kind${e.audit ? " audit" : ""}">${esc(e.kind)}</span></td>
    <td class="key" style="max-width:none;white-space:normal">${esc(e.summary)}</td>
  </tr>` };
}

// renderOps draws the operations view: what the cluster is doing to
// itself, newest first, with the audit stream left to the security view.
function renderOps() {
  const sel = document.getElementById("events-filter");
  const want = sel.value;
  const kinds = [...eventsKinds].filter(k => !AUDIT_KINDS.has(k)).sort();
  const opts = [`<option value="">all kinds</option>`].concat(kinds.map(k => `<option value="${esc(k)}"${k === want ? " selected" : ""}>${esc(k)}</option>`));
  if (sel.options.length !== opts.length) sel.innerHTML = opts.join("");
  const rows = eventsAll.filter(e => !e.audit && (!want || e.kind === want)).slice().reverse();
  renderKeyed(document.getElementById("events"), rows.length ? rows.map(eventRow)
    : [{ key: "none", html: `<tr data-key="none"><td colspan="3" class="muted">${want ? "no " + esc(want) + " events" : "nothing recorded yet"}</td></tr>` }]);
  document.getElementById("ops-scope").textContent =
    `the event ring of n${eventsNode || "?"}${lastCluster && eventsNode === lastCluster.node_id ? ", the node serving this page" : ""} — each node keeps its own`;
  document.getElementById("events-note").textContent =
    `${rows.length} of the last ${eventsAll.length} events (splits, merges, rebalances, repairs, snapshots, backups, upgrades, key rotations)`;
}

// AUDIT_KINDS are the security-relevant records; they are shown on the
// security view instead of mixed into the operational timeline.
const AUDIT_KINDS = new Set(["key-rotation"]);

// renderSecurity: how the cluster authenticates, who may sign in, and
// the audit stream. Certificate expiry and per-node encryption state
// arrive with #156; what is here is what the endpoints already carry.
function renderSecurity(d) {
  const p = d.principal || {};
  renderTiles(document.getElementById("sec-auth"),
    tile("mode", p.secure ? "secure" : "insecure — no authentication") +
    tile("signed in as", p.secure ? (p.user || "?") : "everyone is root") +
    tile("signed in by", p.secure ? (VIA_NAMES[p.via] || p.via || "?") : "—") +
    tile("admin role", p.admin ? "held" : "not held"));
  document.getElementById("sec-auth-note").textContent = p.secure
    ? "every HTTP route takes a session cookie, HTTP Basic credentials, or a client certificate; all three need a role that exists and holds LOGIN"
    : "this cluster authenticates nobody: start the nodes with a certificate directory to change that";

  const users = (lastSchema && lastSchema.users) || [];
  renderKeyed(document.getElementById("users"), users.length
    ? users.map(u => ({ key: u.name, html: `<tr data-key="${esc(u.name)}"><td>${esc(u.name)}</td><td>${u.admin ? '<span class="role">admin</span>' : "user"}</td></tr>` }))
    : [{ key: "none", html: `<tr data-key="none"><td colspan="2" class="muted">${p.secure && !p.admin ? "the user list needs the admin role" : "no users yet"}</td></tr>` }]);
  document.getElementById("users-note").textContent = users.length
    ? "roles and grants are managed with GRANT and REVOKE on the SQL port; the console never mutates"
    : "";

  const audit = eventsAll.filter(e => e.audit || AUDIT_KINDS.has(e.kind)).slice().reverse();
  renderKeyed(document.getElementById("audit"), audit.length ? audit.map(eventRow)
    : [{ key: "none", html: `<tr data-key="none"><td colspan="3" class="muted">${canDrillDown() ? "nothing audited yet" : "audit records need the admin role"}</td></tr>` }]);
  document.getElementById("audit-scope").textContent =
    `the audit records in n${eventsNode || "?"}'s event ring — authentication failures, sign-ins, denied admin operations and privilege DDL`;
  document.getElementById("audit-note").textContent = canDrillDown()
    ? "the node log carries the full record; this is the in-memory tail"
    : "signed in without the admin role: audit records are filtered out by the server";
}

// The overview's compact activity strip: the newest few operations, so
// the front page shows what the cluster is doing to itself without
// becoming the operations view.
function renderRecentOps() {
  const rows = eventsAll.filter(e => !e.audit).slice(-6).reverse();
  renderKeyed(document.getElementById("recent-ops"), rows.length ? rows.map(eventRow)
    : [{ key: "none", html: `<tr data-key="none"><td colspan="3" class="muted">nothing recorded yet</td></tr>` }]);
  document.getElementById("recent-ops-note").innerHTML =
    `the newest operations on n${eventsNode || "?"} · <a href="${routeTo("ops")}">the whole timeline →</a>`;
}

// pollScopedEvents fetches the scoped node's ring when it is not the
// node serving the page (the serving node's rides in /api/overview).
async function pollScopedEvents() {
  const id = scopeNode();
  if (!id || scopeIsServing()) return;
  const resp = await fetch("/api/node?id=" + id, { cache: "no-store" });
  const d = await resp.json().catch(() => ({}));
  if (resp.status === 403) {
    eventsNode = id; eventsAll = []; eventsKinds = new Set();
    setHTML(document.getElementById("events"), `<tr><td colspan="3" class="note">${drillDownRefusal()}</td></tr>`);
    return;
  }
  if (!resp.ok) throw new Error(d.error || ("HTTP " + resp.status));
  ingestEvents(id, d.events, 0);
  if (ui.view === "ops") renderOps();
  if (ui.view === "security" && lastCluster) renderSecurity(lastCluster);
}
