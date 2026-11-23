package server

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/sthorne/datax/pkg/keys"
	"github.com/sthorne/datax/pkg/metrics"
	"github.com/sthorne/datax/pkg/rpc"
	"github.com/sthorne/datax/pkg/security"
	"github.com/sthorne/datax/pkg/server/ui"
	"github.com/sthorne/datax/pkg/sql/catalog"
	"github.com/sthorne/datax/pkg/util/log"
	"github.com/sthorne/datax/pkg/version"
)

// The console's front door (issue #158). HTTP Basic and client
// certificates are how machines authenticate and are untouched; this
// adds the door a person uses. POST /api/login exchanges a password for
// a signed session cookie, POST /api/logout clears it, and httpAuth
// accepts the cookie as a third way to establish the same principal
// every downstream check already reads.
//
// The token is stateless and signed with a key derived from a secret
// the cluster shares (sessionKey; pkg/security/session.go), so any node
// accepts a token any other node minted and no session state is
// replicated. What the token asserts is identity only: roles are
// resolved per request from the catalog, so NOLOGIN, DROP ROLE and a
// revoked admin membership take effect at once rather than at expiry.

const (
	// sessionCookieName is the cookie the console signs in with.
	sessionCookieName = "datax_session"
	// sessionTTL bounds a token's life. It is what limits the damage of
	// a stolen cookie that nobody notices; an operator who needs every
	// outstanding session dead runs `datax debug rotate-session-secret`
	// (rotateSessionSecret).
	sessionTTL = 12 * time.Hour
	// sessionSecretTTL is how long a node trusts its cached copy of the
	// session secret before reading it again: a rotation stops tokens
	// signed under the old secret validating on every node within this
	// long. A node that cannot read the secret once its copy is stale
	// accepts no token until it can.
	sessionSecretTTL = 10 * time.Second
	// loginBodyLimit bounds the credential document.
	loginBodyLimit = 4 << 10
)

// sessionKey is the key this node signs and verifies session tokens
// with: derived from the session secret once a rotation has written one,
// and from the cluster's shared authentication secret before that (as
// every node before cluster version v26 derives it, so the tokens are
// the same). nil when a secret cannot be read — sessions are then
// unavailable and the other two doors still work.
func (n *Node) sessionKey(ctx context.Context) []byte {
	secret, ok := n.currentSessionSecret(ctx)
	if !ok {
		return nil
	}
	if len(secret) == 0 {
		secret = n.mockSecret(ctx)
		if len(secret) == 0 {
			return nil
		}
	}
	return security.DeriveKey(secret, security.SessionKeyPurpose)
}

// currentSessionSecret returns the session secret (nil when no rotation
// has written one), from the cache while it is younger than
// sessionSecretTTL and from KV otherwise. ok is false when the cache is
// stale and the read fails: the caller must then accept no token, or a
// node cut off from KV would honour revoked ones indefinitely.
func (n *Node) currentSessionSecret(ctx context.Context) (secret []byte, ok bool) {
	n.sessionSecretMu.Lock()
	defer n.sessionSecretMu.Unlock()
	if !n.sessionSecretRead.IsZero() && time.Since(n.sessionSecretRead) < sessionSecretTTL {
		return n.sessionSecret, true
	}
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	raw, err := n.db.Get(ctx, keys.SessionSecretKey())
	if err != nil {
		log.Warnf("reading the console's session secret: %v (console sessions are refused on this node until it can be read)", err)
		return nil, false
	}
	n.sessionSecret, n.sessionSecretRead = raw, time.Now()
	return raw, true
}

// rotateSessionSecret writes a new session secret, which revokes every
// outstanding console session: a token signed under the old secret (or,
// before the first rotation, under the authentication secret) fails
// verification on this node at once and on every other node within
// sessionSecretTTL, whose wait it returns after. Root only — the one
// principal whose session cannot otherwise be cut off, since root cannot
// be made NOLOGIN — and only from cluster version v26, below which a
// node never reads the secret. The authentication secret itself is left
// alone: it also keys the stand-in SCRAM salts of unknown users, and
// changing those while real users' salts stay put would tell an observer
// which names exist.
func (n *Node) rotateSessionSecret(ctx context.Context) error {
	if n.tlsCfgs != nil {
		if cn := rpc.PeerCN(ctx); cn != catalog.RootRole {
			return fmt.Errorf("permission denied: rotate-session-secret may only be run as root (connected as %q)", cn)
		}
	}
	if cv := n.db.ClusterVersion(); cv < version.V26 {
		return fmt.Errorf("rotate-session-secret needs cluster version %s (the cluster is at %s): finalize the upgrade with `datax debug upgrade` first", version.V26, cv)
	}
	secret := make([]byte, 32)
	if _, err := rand.Read(secret); err != nil {
		return err
	}
	if err := n.db.Put(ctx, keys.SessionSecretKey(), secret); err != nil {
		return fmt.Errorf("writing the new session secret: %w", err)
	}
	n.sessionSecretMu.Lock()
	n.sessionSecret, n.sessionSecretRead = secret, time.Now()
	n.sessionSecretMu.Unlock()
	// Every other node's copy is at most sessionSecretTTL old; once that
	// has passed (with a margin for a read in flight) none accepts the
	// revoked tokens, which is what the caller is told.
	select {
	case <-time.After(sessionSecretTTL + time.Second):
		return nil
	case <-ctx.Done():
		return fmt.Errorf("the session secret was rotated, but the wait for every node to adopt it was cut short (they do within %s): %w", sessionSecretTTL, ctx.Err())
	}
}

// loginRequest is the POST /api/login body.
type loginRequest struct {
	User     string `json:"user"`
	Password string `json:"password"`
}

// loginResponse is what a successful sign-in returns; the console shows
// the identity it got and prompts again as the expiry approaches.
type loginResponse struct {
	User      string `json:"user"`
	Admin     bool   `json:"admin"`
	ExpiresAt int64  `json:"expires_at_unix_ms"`
}

// loginRefusal is the ONE message every failed sign-in gets. The issue
// asked to distinguish "this user exists but has no password, the
// cluster authenticates it by certificate" from "unknown user or wrong
// password"; that distinction is a user-enumeration oracle, and it would
// undo the constant-time dummy verifier the same issue asks to keep. The
// hint is therefore given unconditionally — the login page says a
// cluster may authenticate some users by certificate only — and the
// refusal itself confirms nothing about any particular name.
const loginRefusal = "unknown user, wrong password, or a user this cluster authenticates by client certificate only"

// serveLogin verifies a password through the same path httpAuth uses for
// HTTP Basic and, on success, sets the session cookie.
//
// State-changing, so it is POST-only and requires a JSON content type:
// with SameSite=Strict on the cookie, that pair is what keeps another
// origin from driving it (a form post cannot set that content type, and
// a cross-site fetch that could is blocked from carrying the cookie).
// This is deliberate, not incidental.
func (n *Node) serveLogin(w http.ResponseWriter, req *http.Request) {
	if req.Method != http.MethodPost {
		w.Header().Set("Allow", http.MethodPost)
		writeLoginError(w, http.StatusMethodNotAllowed, "sign-in is a POST")
		return
	}
	if ct := req.Header.Get("Content-Type"); !strings.HasPrefix(ct, "application/json") {
		writeLoginError(w, http.StatusUnsupportedMediaType, "sign-in requires a JSON body")
		return
	}
	if n.tlsCfgs == nil {
		// Insecure mode has no identities to establish; the console says
		// so with a banner rather than offering a form.
		writeLoginError(w, http.StatusBadRequest, "this cluster runs in insecure mode: there is no authentication to sign in to")
		return
	}
	var lr loginRequest
	if err := json.NewDecoder(io.LimitReader(req.Body, loginBodyLimit)).Decode(&lr); err != nil {
		writeLoginError(w, http.StatusBadRequest, "malformed sign-in request")
		return
	}
	ctx := req.Context()
	key := n.sessionKey(ctx)
	if key == nil {
		writeLoginError(w, http.StatusServiceUnavailable, "the cluster's authentication secret is unavailable on this node; try another node, or use HTTP Basic credentials")
		return
	}
	// Ahead of the expensive work, not after it: verifying a password
	// costs this node real CPU, and /api/login is reachable before any
	// credential is validated (issue #195). A refusal here answers
	// exactly as a wrong password does.
	if !n.authLimit.allow(req.RemoteAddr, lr.User) {
		n.authThrottled(req.RemoteAddr, lr.User, req.URL.Path, throttleRateLimit)
		writeLoginError(w, http.StatusTooManyRequests, loginRefusal)
		return
	}
	// And the verification budget, which a success does not refund: a
	// caller holding a valid credential costs this node a derivation per
	// sign-in like anyone else (issue #221).
	if !n.authLimit.allowVerify(req.RemoteAddr) {
		n.authLimit.unspend(req.RemoteAddr, lr.User)
		n.authThrottled(req.RemoteAddr, lr.User, req.URL.Path, throttleVerifyRate)
		writeLoginError(w, http.StatusTooManyRequests, loginRefusal)
		return
	}
	if !n.authLimit.acquireVerify() {
		n.authThrottled(req.RemoteAddr, lr.User, req.URL.Path, throttleVerifyFull)
		writeLoginError(w, http.StatusTooManyRequests, loginRefusal)
		return
	}
	defer n.authLimit.releaseVerify()
	// The same work for every outcome: an unknown user is verified
	// against the dummy verifier so timing does not separate it from a
	// wrong password (security.DummyVerifier).
	verifier, err := n.lookupVerifier(ctx, lr.User)
	if err != nil || verifier == nil {
		verifier = security.DummyVerifier()
	}
	ok := security.VerifyPassword(verifier, lr.Password)
	if ok {
		// Proving a credential is not what the limiter is bounding.
		n.authLimit.succeeded(req.RemoteAddr, lr.User)
		// A correct password still does not sign in a role that may not:
		// NOLOGIN and DROP ROLE close this door as they close the others
		// (issue #138 for the certificate path).
		if allowed, lerr := n.canLogin(ctx, lr.User); lerr != nil || !allowed {
			ok = false
		}
	}
	if !ok {
		metrics.AuthFailures.Inc()
		log.Audit("http-auth-failure", "principal", lr.User, "via", "login", "remote", req.RemoteAddr, "path", req.URL.Path)
		writeLoginError(w, http.StatusUnauthorized, loginRefusal)
		return
	}
	now := time.Now()
	expires := now.Add(sessionTTL)
	http.SetCookie(w, &http.Cookie{
		Name:     sessionCookieName,
		Value:    security.SignSession(key, lr.User, now, expires),
		Path:     "/",
		Expires:  expires,
		MaxAge:   int(sessionTTL / time.Second),
		HttpOnly: true,
		Secure:   true,
		SameSite: http.SameSiteStrictMode,
	})
	log.Audit("http-login", "principal", lr.User, "remote", req.RemoteAddr)
	writeJSON(w, http.StatusOK, loginResponse{
		User:      lr.User,
		Admin:     n.isAdminPrincipal(ctx, lr.User),
		ExpiresAt: expires.UnixMilli(),
	})
}

// serveLogout clears the session cookie. POST-only for the same reason
// sign-in is, though SameSite=Strict already keeps another origin from
// carrying the cookie here at all.
func (n *Node) serveLogout(w http.ResponseWriter, req *http.Request) {
	if req.Method != http.MethodPost {
		w.Header().Set("Allow", http.MethodPost)
		writeLoginError(w, http.StatusMethodNotAllowed, "sign-out is a POST")
		return
	}
	if p := principalFrom(req); p.Via == "session" {
		log.Audit("http-logout", "principal", p.User, "remote", req.RemoteAddr)
	}
	clearSessionCookie(w)
	writeJSON(w, http.StatusOK, map[string]string{"status": "signed out"})
}

// clearSessionCookie expires the cookie in the browser.
func clearSessionCookie(w http.ResponseWriter) {
	http.SetCookie(w, &http.Cookie{
		Name:     sessionCookieName,
		Value:    "",
		Path:     "/",
		Expires:  time.Unix(0, 0),
		MaxAge:   -1,
		HttpOnly: true,
		Secure:   true,
		SameSite: http.SameSiteStrictMode,
	})
}

// sessionPrincipal resolves a request's session cookie to a principal.
// The bool reports whether a cookie was present at all, so httpAuth can
// tell "no session" from "a session that did not hold" and clear the
// stale cookie in the second case.
func (n *Node) sessionPrincipal(req *http.Request) (httpPrincipal, bool) {
	c, err := req.Cookie(sessionCookieName)
	if err != nil || c.Value == "" {
		return httpPrincipal{}, false
	}
	ctx := req.Context()
	key := n.sessionKey(ctx)
	if key == nil {
		return httpPrincipal{}, true
	}
	user, err := security.ParseSession(key, c.Value, time.Now())
	if err != nil {
		reason := "invalid"
		if errors.Is(err, security.ErrSessionExpired) {
			reason = "expired"
		}
		metrics.AuthFailures.Inc()
		log.Audit("http-auth-failure", "principal", "", "via", "session", "reason", reason, "remote", req.RemoteAddr, "path", req.URL.Path)
		return httpPrincipal{}, true
	}
	// The token proves who signed in, not that they may still sign in:
	// the role must still exist and hold LOGIN.
	if allowed, lerr := n.canLogin(ctx, user); lerr != nil || !allowed {
		metrics.AuthFailures.Inc()
		log.Audit("http-auth-failure", "principal", user, "via", "session", "reason", "no-login", "remote", req.RemoteAddr, "path", req.URL.Path)
		return httpPrincipal{}, true
	}
	return httpPrincipal{User: user, Via: "session"}, true
}

// sessionExpiry is the expiry the request's session cookie asserts, for
// the console to prompt before it lapses; zero when there is none. The
// value is not trusted for anything else — the signature check above is
// what authenticates.
func sessionExpiry(req *http.Request) time.Time {
	c, err := req.Cookie(sessionCookieName)
	if err != nil || c.Value == "" {
		return time.Time{}
	}
	exp, ok := security.SessionExpiry(c.Value)
	if !ok {
		return time.Time{}
	}
	return exp
}

// wantsHTML reports whether the request is a browser navigation rather
// than a programmatic call — the test that decides whether an
// unauthenticated request gets the login page or the Basic challenge
// that every existing client expects. It is the Accept header, never
// the user agent: curl and Prometheus send */*, browsers name text/html.
func wantsHTML(req *http.Request) bool {
	for _, part := range strings.Split(req.Header.Get("Accept"), ",") {
		if mediaType, _, _ := strings.Cut(strings.TrimSpace(part), ";"); mediaType == "text/html" {
			return true
		}
	}
	return false
}

func writeJSON(w http.ResponseWriter, code int, doc any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(doc)
}

func writeLoginError(w http.ResponseWriter, code int, msg string) {
	writeJSON(w, code, map[string]string{"error": msg})
}

// loginContext is what the login page shows about the node it reached:
// an operator behind a load balancer should see which cluster and node
// they are about to hand a password to. It is served before
// authentication, so it carries only what an operator needs to identify
// the endpoint — no cluster state, no user names, no counts.
type loginContext struct {
	ClusterID string `json:"cluster_id,omitempty"`
	NodeID    int    `json:"node_id,omitempty"`
	Locality  string `json:"locality,omitempty"`
	Address   string `json:"address,omitempty"`
	Release   string `json:"release,omitempty"`
	Secure    bool   `json:"secure"`
}

// loginContextPlaceholder is the token in login.html the served page has
// replaced by the JSON above.
const loginContextPlaceholder = "__LOGIN_CONTEXT__"

// renderLoginPage builds the served login page once, at startup.
func (n *Node) renderLoginPage() error {
	raw, err := ui.FS.ReadFile("login.html")
	if err != nil {
		return err
	}
	doc, err := json.Marshal(loginContext{
		ClusterID: n.ident.ClusterID.String(),
		NodeID:    int(n.ident.NodeID),
		Locality:  n.cfg.Locality.String(),
		Address:   n.addr,
		Release:   version.Release,
		Secure:    n.tlsCfgs != nil,
	})
	if err != nil {
		return err
	}
	n.loginPage = bytes.Replace(raw, []byte(loginContextPlaceholder), doc, 1)
	return nil
}

// serveLoginPage answers an unauthenticated browser navigation. The
// status is still 401 — the request was not authenticated — but no
// WWW-Authenticate header rides with it, so the browser renders the page
// instead of raising its own credential dialog.
func (n *Node) serveLoginPage(w http.ResponseWriter, req *http.Request) {
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.Header().Set("Cache-Control", "no-store")
	w.WriteHeader(http.StatusUnauthorized)
	_, _ = w.Write(n.loginPage)
}
