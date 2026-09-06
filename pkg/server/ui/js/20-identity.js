let who = null;
// The identity block (issue #158): who this page is signed in as, how,
// and — for a session — the way out. A cluster that authenticates nobody
// says so across the top of the page rather than in a grey aside.
const VIA_NAMES = { session: "signed in with a password", basic: "HTTP Basic credentials", cert: "client certificate" };
function renderPrincipal(p) {
  who = p || null;
  const el = document.getElementById("hdr-user");
  const banner = document.getElementById("insecure-banner");
  if (!who) { el.hidden = true; return; }
  el.hidden = false;
  if (!who.secure) {
    banner.style.display = "block";
    setHTML(banner, `<b>Insecure mode.</b> This cluster authenticates nobody: anyone who can reach this port has full access, and so does anyone who can reach the SQL port. Start the nodes with a certificate directory to change that.`);
    setHTML(document.getElementById("hdr-user-summary"), `<span class="muted">no sign-in</span>`);
    setHTML(document.getElementById("hdr-user-menu"), `<div class="muted">This cluster runs in insecure mode; there is no identity to show and nothing to sign out of.</div>`);
    return;
  }
  banner.style.display = "none";
  setHTML(document.getElementById("hdr-user-summary"),
    `${esc(who.user || "?")}${who.admin ? `<span class="role">admin</span>` : ""} ▾`);
  const lapses = who.session_expires_at_unix_ms
    ? `<dt>session</dt><dd>ends ${fmtDuration(Math.max(0, (who.session_expires_at_unix_ms - Date.now()) / 1000))} from now</dd>` : "";
  setHTML(document.getElementById("hdr-user-menu"),
    `<dl><dt>user</dt><dd>${esc(who.user || "?")}</dd>
      <dt>signed in by</dt><dd>${esc(VIA_NAMES[who.via] || who.via || "?")}</dd>
      <dt>roles</dt><dd>${who.admin ? "admin" : "no admin role"}</dd>${lapses}</dl>` +
    (who.via === "session"
      ? `<button type="button" id="btn-signout">Sign out</button><button type="button" id="btn-switch">Sign in as someone else</button>`
      : `<div class="muted">This page authenticates by ${esc(VIA_NAMES[who.via] || "credentials")}, which the browser holds rather than this page. To use a different identity, sign in with a password from the <a href="/api/logout-then-login" id="lnk-login">sign-in page</a>.</div>`));
  const signOut = async (andBack) => {
    try { await fetch("/api/logout", { method: "POST" }); } catch (err) { /* the reload tells the story */ }
    location.href = andBack ? "/?next=" + encodeURIComponent(location.pathname + location.hash) : "/";
  };
  document.getElementById("btn-signout")?.addEventListener("click", () => signOut(false));
  document.getElementById("btn-switch")?.addEventListener("click", () => signOut(true));
  document.getElementById("lnk-login")?.addEventListener("click", ev => { ev.preventDefault(); signOut(false); });
}
function canDrillDown() { return !who || who.admin; }
function drillDownRefusal() {
  if (!who || !who.secure || !who.user) return "admin role required for cross-node drill-down";
  const otherUser = who.via === "session"
    ? `, or sign out and sign in as <code>root</code> from the user menu`
    : `, or sign in as <code>root</code>`;
  return `signed in as ${esc(who.user)}: the admin role is required for cross-node drill-down. ` +
    `Grant it with <code>GRANT ADMIN TO ${esc(who.user)}</code>${otherUser}.`;
}
