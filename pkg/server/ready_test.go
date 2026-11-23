package server

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/sthorne/datax/pkg/pgwire"
	"github.com/sthorne/datax/pkg/security"
)

// TestReadiness: /ready answers 503 until the node has joined (a
// heartbeat landed) and its SQL listener serves, 200 then, and 503 again
// once it drains for a stop; a node configured without SQL needs only
// to have joined.
func TestReadiness(t *testing.T) {
	get := func(n *Node) (int, Readiness) {
		t.Helper()
		rec := httptest.NewRecorder()
		n.serveReady(rec, httptest.NewRequest("GET", "/ready", nil))
		var r Readiness
		if err := json.Unmarshal(rec.Body.Bytes(), &r); err != nil {
			t.Fatalf("body %q: %v", rec.Body.String(), err)
		}
		return rec.Code, r
	}
	n := &Node{cfg: Config{PGListen: "127.0.0.1:0"}}
	if code, r := get(n); code != http.StatusServiceUnavailable || r.Ready {
		t.Fatalf("before joining: %d %+v", code, r)
	}
	n.joined.Store(true)
	if code, r := get(n); code != http.StatusServiceUnavailable || r.Reason == "" {
		t.Fatalf("joined, SQL not serving: %d %+v", code, r)
	}
	n.pgServer.Store(&pgwire.Server{})
	if code, r := get(n); code != http.StatusOK || !r.Ready {
		t.Fatalf("ready: %d %+v", code, r)
	}
	n.shuttingDown.Store(true)
	if code, r := get(n); code != http.StatusServiceUnavailable || r.Ready {
		t.Fatalf("draining: %d %+v", code, r)
	}

	noSQL := &Node{}
	noSQL.joined.Store(true)
	if code, _ := get(noSQL); code != http.StatusOK {
		t.Fatalf("joined node without SQL: %d", code)
	}
}

// TestReadyNeedsNoCredentials: in secure mode /ready is answered without
// credentials, as an orchestrator's probe asks it; /status still is not.
func TestReadyNeedsNoCredentials(t *testing.T) {
	n := &Node{tlsCfgs: &security.TLSConfigs{}}
	n.joined.Store(true)
	mux := http.NewServeMux()
	mux.HandleFunc("/ready", n.serveReady)
	mux.HandleFunc("/status", func(w http.ResponseWriter, _ *http.Request) {})
	h := n.httpAuth(mux)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest("GET", "/ready", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("/ready without credentials: %d %s", rec.Code, rec.Body.String())
	}
	rec = httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest("GET", "/status", nil))
	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("/status without credentials: %d, want 401", rec.Code)
	}
}
