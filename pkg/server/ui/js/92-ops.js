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

// Operations (issue #153): the long-running things the cluster is doing
// to itself, paired from the ring's start/end records by the server. The
// flat log below stays as the audit trail it is; this is the reading of
// it that says what is RUNNING, which a list of instants cannot.
let opsAll = [];
function ingestOperations(ops) { opsAll = ops || []; }

// fmtElapsed renders a duration in ms at a useful resolution: seconds
// while an operation is young, minutes and hours once it is not.
function fmtElapsed(ms) {
  if (!ms || ms < 0) return "—";
  const sec = Math.round(ms / 1000);
  if (sec < 60) return sec + "s";
  return fmtDuration(sec);
}

function opRow(o, running) {
  const key = o.kind + "/" + o.op;
  // Progress that cannot be known is elapsed time, never a bar with a
  // number nobody measured.
  const when = running
    ? `<td class="when" title="${esc(new Date(o.started_unix_ms).toISOString())}">${fmtAgo(Date.now() - o.started_unix_ms)}</td>`
    : `<td class="when" title="${esc(new Date(o.ended_unix_ms).toISOString())}">${fmtAgo(Date.now() - o.ended_unix_ms)}</td>`;
  const outcome = running ? "" :
    `<td>${o.outcome === "ok"
      ? `<span class="st live"><span class="dot"></span>ok</span>`
      : `<span class="st draining"><span class="dot"></span>${esc(o.outcome || "ended")}</span>`}</td>`;
  const took = running
    ? `<td class="num">${fmtElapsed(o.elapsed_ms)}</td>`
    : `<td class="num">${o.started_unix_ms ? fmtElapsed(o.elapsed_ms) : `<span class="muted" title="this operation's start is older than the event ring, so how long it took is not known">—</span>`}</td>`;
  return { key, html: `<tr data-key="${esc(key)}">
    <td><span class="kind">${esc(o.kind)}</span></td>
    <td class="key" style="max-width:none;white-space:normal">${esc(o.summary)}</td>
    ${running ? took + when : outcome + took + when}
  </tr>` };
}

function renderOperations() {
  const who = "n" + (eventsNode || "?");
  const running = opsAll.filter(o => o.running);
  const done = opsAll.filter(o => !o.running);
  renderKeyed(document.getElementById("ops-running"), running.length
    ? running.map(o => opRow(o, true))
    : [{ key: "none", html: `<tr data-key="none"><td colspan="4" class="muted">nothing long-running on ${esc(who)} right now</td></tr>` }]);
  setHTML(document.getElementById("ops-running-note"),
    `backups, restores, re-encryption sweeps and decommission drains record both of their ends, so one still going shows here with how long it has been going, as reported by ${esc(who)}. Progress that cannot be measured is shown as elapsed time rather than as a bar with a number nobody measured.`);
  renderKeyed(document.getElementById("ops-done"), done.length
    ? done.map(o => opRow(o, false))
    : [{ key: "none", html: `<tr data-key="none"><td colspan="5" class="muted">none finished within ${esc(who)}'s event ring</td></tr>` }]);
  setHTML(document.getElementById("ops-done-note"),
    "derived from the event ring below, which stays the audit trail it is: an operation whose start has already aged out of the ring is still listed, with no duration claimed for it.");
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
  renderOperations();
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

// secDoc is the last /api/security document (issue #156). It is fetched
// on its own cadence, so the parts of the view drawn from the cluster
// poll and the parts drawn from it redraw independently.
let secDoc = null;

// pollSecurity fetches the security document. It is not admin-gated as a
// whole: what a certificate expires belongs to any authenticated user,
// and the server filters out what names people.
async function pollSecurity() {
  const resp = await fetch("/api/security", { cache: "no-store" });
  if (!resp.ok) { secDoc = null; renderSecurityDoc(); throw new Error("HTTP " + resp.status); }
  secDoc = await resp.json();
  renderSecurityDoc();
}

// certRow: the expiry, and what its lapse would cost. A certificate is
// only interesting for when it stops working, so that is the column the
// eye lands on and the one that is coloured.
// SQL_VIA_NAMES names the SQL wire's authentication methods, which are
// not the HTTP ones (VIA_NAMES): a SQL client presents a certificate or
// runs SCRAM, or, in insecure mode, is simply believed.
const SQL_VIA_NAMES = {
  cert: "client certificate", scram: "SCRAM password", trust: "trusted (insecure mode)",
  unknown: "not recorded (connected before this node started tracking)",
};
const CERT_MEANS = {
  ca: "every node authenticates against this CA; its lapse stops the cluster",
  node: "this node cannot reach its peers or serve SQL without it",
  client: "this identity can no longer authenticate by certificate",
};

function certRow(c) {
  const left = Date.parse(c.not_after) - Date.now();
  const days = left / 86400000;
  const level = left <= 0 ? "down" : days <= 7 ? "down" : days <= 30 ? "draining" : "";
  const when = left <= 0 ? "expired" : days < 1 ? Math.round(days * 24) + "h" : Math.round(days) + "d";
  return {
    key: c.kind + "/" + c.subject, html: `<tr data-key="${esc(c.kind + "/" + c.subject)}">
    <td data-label="kind"><span class="kind">${esc(c.kind)}</span></td>
    <td class="key" data-label="subject">${esc(c.subject || "—")}</td>
    <td class="key" data-label="issuer">${esc(c.issuer || "—")}</td>
    <td class="num" data-label="expires" title="${esc(CERT_MEANS[c.kind] || "")}">${warn(level, when)}</td>
    <td class="when" data-label="on" title="${esc(c.not_after)}">${esc(new Date(c.not_after).toLocaleDateString())}</td>
  </tr>` };
}

// renderSecurityDoc draws the parts that come from /api/security.
function renderSecurityDoc() {
  const d = secDoc;
  // Soonest expiry first across both lists: the server sorts each, but
  // the one to act on is the nearest whichever list it came from.
  const certs = d ? (d.certificates || []).concat(d.client_certs || [])
    .sort((a, b) => Date.parse(a.not_after) - Date.parse(b.not_after)) : [];
  renderKeyed(document.getElementById("certs"), certs.length ? certs.map(certRow)
    : [{ key: "none", html: `<tr data-key="none"><td colspan="5" class="muted">${d && !d.secure
        ? "this cluster runs without certificates: start the nodes with a certificate directory to change that"
        : "no certificates reported"}</td></tr>` }]);
  document.getElementById("certs-note").textContent = !d ? "" : (d.secure
    ? `the material n${d.node_id} loaded, soonest expiry first — the same dates /metrics publishes as datax_cert_expiry_seconds, so an alert fires without anyone opening this page. `
      + `Health warns 30 days out and turns critical at 7.`
      + (d.client_certs ? ` Client rows are the identities that have presented a certificate to n${d.node_id}: what has been connecting, not a directory of what could${d.client_certs_full ? ", and the list is full, so it is a sample" : ""}.`
        : " Client certificates presented to this node need the admin role.")
    : "insecure mode: nothing to expire, and nothing authenticating either");

  const conns = d && d.connections;
  renderKeyed(document.getElementById("sec-conns"), conns && conns.length
    ? conns.map(c => ({ key: c.user || "(none)", html: `<tr data-key="${esc(c.user || "(none)")}">
      <td data-label="user">${esc(c.user || "—")}</td>
      <td data-label="how they authenticated">${Object.entries(c.via || {}).sort((a, b) => b[1] - a[1])
        .map(([v, n]) => `${esc(SQL_VIA_NAMES[v] || v)} ${n}`).join(" · ") || "—"}</td>
      <td class="num" data-label="connections">${c.total}</td>
    </tr>` }))
    : [{ key: "none", html: `<tr data-key="none"><td colspan="3" class="muted">${d && d.principal && !d.principal.admin
        ? "the per-user breakdown needs the admin role" : "nobody is connected to this node"}</td></tr>` }]);
  document.getElementById("sec-conns-note").textContent = !d ? "" :
    `n${d.node_id}'s own connections — each node accepts its own, so this is not a cluster total. `
    + `${Math.round(d.auth_failures)} authentication failure(s) and ${Math.round(d.admin_denied)} denied admin operation(s) on this node since it started.`;

  const stores = d ? (d.stores || []) : [];
  renderKeyed(document.getElementById("sec-stores"), stores.length
    ? stores.map(st => {
      const re = st.reencryption;
      const progress = !st.encrypted ? "—"
        : re && re.remaining_bytes > 0 ? warn("draining", fmtBytes(re.remaining_bytes) + " still on a retired key")
          : re && re.sweep_error ? warn("down", esc(re.sweep_error))
            : "every live sstable on the active key";
      return {
        key: "n" + st.node_id, html: `<tr data-key="n${st.node_id}">
        <td data-label="store">n${st.node_id}</td>
        <td data-label="state">${st.encrypted ? `<span class="st live"><span class="dot"></span>encrypted</span>`
            : `<span class="st draining"><span class="dot"></span>plaintext</span>`}</td>
        <td class="num" data-label="re-encryption">${progress}</td>
      </tr>` };
    })
    : [{ key: "none", html: `<tr data-key="none"><td colspan="3" class="muted">no store reported</td></tr>` }]);
  document.getElementById("sec-stores-note").textContent = !d ? "" :
    `n${d.node_id}'s own store — each node's is on its own page, because asking every node would be a fan-out this view does not make`;

  const roles = d ? (d.roles || []) : [];
  renderKeyed(document.getElementById("users"), roles.length
    ? roles.map(r => ({ key: r.name, html: `<tr data-key="${esc(r.name)}">
      <td data-label="role">${esc(r.name)}${r.builtin ? ' <span class="muted">built-in</span>' : ""}${r.admin ? ' <span class="role">admin</span>' : ""}</td>
      <td data-label="may log in">${r.builtin ? '<span class="muted">no — built-in roles are groups</span>' : r.login ? "yes" : "no"}</td>
      <td data-label="member of">${(r.member_of || []).map(esc).join(" · ") || "—"}</td>
      <td data-label="effectively holds">${(r.effective || []).map(esc).join(" · ") || "—"}${r.no_inherit ? ' <span class="muted">(NOINHERIT: reachable with SET ROLE, not held)</span>' : ""}</td>
    </tr>` }))
    : [{ key: "none", html: `<tr data-key="none"><td colspan="4" class="muted">no roles reported</td></tr>` }]);
  document.getElementById("users-note").textContent = !d ? "" :
    "privileges flow along membership: a role holds what it was granted plus, unless it is NOINHERIT, what the roles it belongs to hold. "
    + "Managed with GRANT and REVOKE on the SQL port; the console never mutates."
    + (d.error ? " " + d.error : "");
}

// renderSecurity: how the cluster authenticates, who may sign in, and
// the audit stream. The certificate, connection, encryption and role
// panels are drawn by renderSecurityDoc from /api/security (issue #156);
// this draws what rides the cluster poll.
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
    eventsNode = id; eventsAll = []; eventsKinds = new Set(); opsAll = [];
    setHTML(document.getElementById("events"), `<tr><td colspan="3" class="note">${drillDownRefusal()}</td></tr>`);
    return;
  }
  if (!resp.ok) throw new Error(d.error || ("HTTP " + resp.status));
  ingestEvents(id, d.events, 0);
  ingestOperations(d.operations);
  if (ui.view === "ops") renderOps();
  if (ui.view === "security" && lastCluster) renderSecurity(lastCluster);
}
