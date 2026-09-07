package server

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"

	"github.com/sthorne/datax/pkg/sql/catalog"
	"github.com/sthorne/datax/pkg/sql/parser"
	"github.com/sthorne/datax/pkg/sql/types"
	"github.com/sthorne/datax/pkg/version"
)

// The console's viewer preferences (issue #204). A display choice —
// which theme, whether a timestamp reads "4m ago" or as a clock time —
// belongs to the person making it, not to the browser they happen to be
// sitting at. We are a database, so the cluster stores them: the choice
// follows an operator to another browser, another machine, and to any
// node's console, and it survives clearing site data.
//
// The table is a system table like datax_metrics: a reserved descriptor
// ID, created by whichever node first finds it missing, written only by
// the internal system session. Users may read it (that is what
// catalog.IsSystemTable grants) but not write it, so the only path that
// sets a preference is the one below, which takes the user from the
// authenticated principal and never from the request body.

// PrefsTableDDL creates the preferences table. IF NOT EXISTS makes the
// nodes' concurrent attempts idempotent.
//
// One row per preference rather than one blob per user: a new preference
// is then an INSERT, not a read-modify-write of a document two nodes can
// race on, and an operator can read the table with plain SQL.
const PrefsTableDDL = `CREATE TABLE IF NOT EXISTS ` + catalog.PrefsTableName + ` (
  username TEXT NOT NULL,
  name TEXT NOT NULL,
  value TEXT NOT NULL,
  PRIMARY KEY (username, name)
)`

// prefsBodyLimit bounds the preference document. A preference is two
// short allowlisted tokens; the limit is generous for the JSON around
// them and still refuses a body worth buffering.
const prefsBodyLimit = 4 << 10

// prefValues is the allowlist: every preference the console may store,
// and every value it may hold. Both halves are checked server-side, so
// the table can only ever contain tokens from this map — whatever a
// client sends. A preference the console has not shipped yet is
// refused rather than stored for a console that will never read it.
//
// There is deliberately no default here. A preference the viewer has
// never set is absent from the document, and the console renders its
// natural state for an absent value (no data-theme attribute, so the
// operating system decides; relative timestamps). Defaults live in one
// place — the page — instead of drifting between the page and the node.
var prefValues = map[string][]string{
	"theme":      {"system", "light", "dark"},
	"timestamps": {"relative", "absolute"},
}

func prefValueAllowed(name, value string) bool {
	for _, v := range prefValues[name] {
		if v == value {
			return true
		}
	}
	return false
}

// PrefsStatus is the /api/prefs document.
type PrefsStatus struct {
	// Prefs holds only what this viewer has actually set.
	Prefs map[string]string `json:"prefs"`
	// Persistent reports whether a change made now would be stored. It
	// is false until the cluster has finalized v17, which is when the
	// table may exist at all; the console honours a change either way
	// and says so when it will not outlive the tab.
	Persistent bool `json:"persistent"`
	// Why explains a false Persistent in the console's own words.
	Why string `json:"why,omitempty"`
}

// prefsUser is the row key for the requesting principal.
//
// Insecure mode authenticates nobody and has exactly one identity, so
// it gets one shared row under the empty name — which no real user can
// hold. In secure mode httpAuth has already established a principal for
// every path but /api/login and /api/logout, so an empty name here means
// something has gone wrong upstream, and we refuse rather than write a
// secure cluster's preferences into the insecure-mode row.
func (n *Node) prefsUser(req *http.Request) (string, error) {
	if n.tlsCfgs == nil {
		return "", nil
	}
	user := principalFrom(req).User
	if user == "" {
		return "", fmt.Errorf("no authenticated user")
	}
	return user, nil
}

// prefsPersistent reports whether preferences can be stored yet, and why
// not when they cannot.
func (n *Node) prefsPersistent(ctx context.Context) (bool, string) {
	if cv := n.readClusterVersion(ctx); cv < version.V17 {
		return false, fmt.Sprintf("this cluster is at %s: preferences are stored from v%d, so this choice lasts for the tab only. Finalize the upgrade to keep it.", cv, int(version.V17))
	}
	return true, ""
}

// writePrefsError is the shape every refusal takes.
func writePrefsError(w http.ResponseWriter, code int, msg string) {
	writeJSON(w, code, map[string]string{"error": msg})
}

// servePrefsAPI reads (GET) and sets (POST) the requesting viewer's
// console preferences.
//
// The write is the console's first state-changing endpoint after
// sign-in, and it is guarded exactly as /api/login is: POST only, a JSON
// content type required, and the session cookie is SameSite=Strict. A
// cross-origin form post cannot set that content type, and a
// cross-origin fetch that could set it cannot carry the cookie. The
// user is taken from the authenticated principal, so the worst a
// confused browser could achieve is changing its own theme.
func (n *Node) servePrefsAPI(w http.ResponseWriter, req *http.Request) {
	switch req.Method {
	case http.MethodGet:
		n.servePrefsGet(w, req)
	case http.MethodPost:
		n.servePrefsPost(w, req)
	default:
		w.Header().Set("Allow", "GET, POST")
		writePrefsError(w, http.StatusMethodNotAllowed, "preferences are read with GET and set with POST")
	}
}

func (n *Node) servePrefsGet(w http.ResponseWriter, req *http.Request) {
	ctx := req.Context()
	user, err := n.prefsUser(req)
	if err != nil {
		writePrefsError(w, http.StatusUnauthorized, err.Error())
		return
	}
	doc := PrefsStatus{Prefs: map[string]string{}}
	doc.Persistent, doc.Why = n.prefsPersistent(ctx)
	if !doc.Persistent {
		// Nothing has created the table yet, by rule 4. An empty
		// document is the truth, not an error: the console renders its
		// defaults and reports that a change will not be kept.
		writeJSON(w, http.StatusOK, doc)
		return
	}
	rows, err := n.readPrefs(ctx, user)
	if err != nil {
		if isMissingTable(err) {
			// Finalized, but no node has created the table yet (or a
			// restore removed it): still not an error to the reader.
			writeJSON(w, http.StatusOK, doc)
			return
		}
		writePrefsError(w, http.StatusServiceUnavailable, "preferences are unavailable on this node: "+err.Error())
		return
	}
	for name, value := range rows {
		// A value that is no longer in the allowlist — written by a
		// newer console, or by hand — is dropped rather than handed to
		// a page that has no rendering for it.
		if prefValueAllowed(name, value) {
			doc.Prefs[name] = value
		}
	}
	writeJSON(w, http.StatusOK, doc)
}

// prefsRequest is one preference being set.
type prefsRequest struct {
	Name  string `json:"name"`
	Value string `json:"value"`
}

func (n *Node) servePrefsPost(w http.ResponseWriter, req *http.Request) {
	ctx := req.Context()
	if ct := req.Header.Get("Content-Type"); !strings.HasPrefix(ct, "application/json") {
		writePrefsError(w, http.StatusUnsupportedMediaType, "setting a preference requires a JSON body")
		return
	}
	user, err := n.prefsUser(req)
	if err != nil {
		writePrefsError(w, http.StatusUnauthorized, err.Error())
		return
	}
	var pr prefsRequest
	if err := json.NewDecoder(io.LimitReader(req.Body, prefsBodyLimit)).Decode(&pr); err != nil {
		writePrefsError(w, http.StatusBadRequest, "malformed preference")
		return
	}
	if _, ok := prefValues[pr.Name]; !ok {
		writePrefsError(w, http.StatusBadRequest, fmt.Sprintf("%q is not a console preference", pr.Name))
		return
	}
	if !prefValueAllowed(pr.Name, pr.Value) {
		writePrefsError(w, http.StatusBadRequest,
			fmt.Sprintf("%q is not a value of %q (%s)", pr.Value, pr.Name, strings.Join(prefValues[pr.Name], ", ")))
		return
	}
	if ok, why := n.prefsPersistent(ctx); !ok {
		writePrefsError(w, http.StatusServiceUnavailable, why)
		return
	}
	if err := n.writePref(ctx, user, pr.Name, pr.Value); err != nil {
		writePrefsError(w, http.StatusServiceUnavailable, "the preference could not be stored: "+err.Error())
		return
	}
	writeJSON(w, http.StatusOK, PrefsStatus{Prefs: map[string]string{pr.Name: pr.Value}, Persistent: true})
}

// isMissingTable reports the one error the prefs path treats as "not
// created yet" rather than as a failure.
func isMissingTable(err error) bool {
	return err != nil && strings.Contains(err.Error(), "does not exist")
}

// ensurePrefsTable creates the table when it is missing. Unlike the
// metrics recorder there is no tick to do this on, so it runs on the
// first write; a read that finds nothing needs no table at all.
func (n *Node) ensurePrefsTable(ctx context.Context) error {
	if n.prefsReady.Load() {
		return nil
	}
	sess, err := n.systemSession()
	if err != nil {
		return err
	}
	stmts, err := parser.Parse(PrefsTableDDL)
	if err != nil {
		return err
	}
	if _, serr := sess.Execute(ctx, stmts[0], nil); serr != nil {
		return serr
	}
	n.prefsReady.Store(true)
	return nil
}

// readPrefs returns every preference stored for a user.
func (n *Node) readPrefs(ctx context.Context, user string) (map[string]string, error) {
	sess, err := n.systemSession()
	if err != nil {
		return nil, err
	}
	stmts, err := parser.Parse("SELECT name, value FROM " + catalog.PrefsTableName + " WHERE username = $1")
	if err != nil {
		return nil, err
	}
	res, serr := sess.Execute(ctx, stmts[0], []types.Datum{types.NewString(user)})
	if serr != nil {
		return nil, serr
	}
	out := map[string]string{}
	for _, row := range res.Rows {
		if len(row) != 2 || row[0].Null || row[1].Null {
			continue
		}
		out[row[0].S] = row[1].S
	}
	return out, nil
}

// writePref stores one preference, creating the table if this is the
// first one the cluster has ever been asked to keep.
func (n *Node) writePref(ctx context.Context, user, name, value string) error {
	if err := n.ensurePrefsTable(ctx); err != nil {
		return err
	}
	sess, err := n.systemSession()
	if err != nil {
		return err
	}
	stmts, err := parser.Parse("INSERT INTO " + catalog.PrefsTableName +
		" (username, name, value) VALUES ($1, $2, $3) ON CONFLICT (username, name) DO UPDATE SET value = $3")
	if err != nil {
		return err
	}
	params := []types.Datum{types.NewString(user), types.NewString(name), types.NewString(value)}
	if _, serr := sess.Execute(ctx, stmts[0], params); serr != nil {
		if isMissingTable(serr) {
			n.prefsReady.Store(false) // dropped or restored away: recreate next time
		}
		return serr
	}
	return nil
}
