package server

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/sthorne/datax/pkg/metrics"
	"github.com/sthorne/datax/pkg/security"
	"github.com/sthorne/datax/pkg/server/ui"
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
// The token is stateless and signed with a key derived from the
// cluster's authentication secret (pkg/security/session.go), so any node
// accepts a token any other node minted and no session state is
// replicated. What the token asserts is identity only: roles are
// resolved per request from the catalog, so NOLOGIN, DROP ROLE and a
// revoked admin membership take effect at once rather than at expiry.

const (
	// sessionCookieName is the cookie the console signs in with.
	sessionCookieName = "datax_session"
	// sessionTTL bounds a token's life. It is what limits the damage of
	// a stolen cookie, since a stateless token cannot be revoked; an
	// operator who needs every outstanding session dead rotates the
	// cluster's authentication secret.
	sessionTTL = 12 * time.Hour
	// loginBodyLimit bounds the credential document.
	loginBodyLimit = 4 << 10
)

// sessionKey is the key this node signs and verifies session tokens
// with, derived from the cluster's shared authentication secret. nil
// when the secret cannot be read — sessions are then unavailable and
// the other two doors still work.
func (n *Node) sessionKey(ctx context.Context) []byte {
	secret := n.mockSecret(ctx)
	if len(secret) == 0 {
		return nil
	}
	return security.DeriveKey(secret, security.SessionKeyPurpose)
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
	// The same work for every outcome: an unknown user is verified
	// against the dummy verifier so timing does not separate it from a
	// wrong password (security.DummyVerifier).
	verifier, err := n.lookupVerifier(ctx, lr.User)
	if err != nil || verifier == nil {
		verifier = security.DummyVerifier()
	}
	ok := security.VerifyPassword(verifier, lr.Password)
	if ok {
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
