package server

import (
	"encoding/json"
	"net/http"
)

// /status answers 200 as long as the HTTP port does: it is a liveness
// check and a document, not a statement that the node can take work.
// /ready is the readiness check a load balancer or an orchestrator's
// probe asks before sending this node SQL clients: 200 once the node
// has joined (its first heartbeat reached the registry in range 1, so
// the cluster's KV layer answers it), its store is open and its SQL
// listener (when one is configured) is serving; 503 before that, and
// again from the moment it begins draining for a stop. It is answered
// without authentication, in secure mode too, and says nothing but
// whether the node is ready and, if not, why.

// Readiness is the /ready document.
type Readiness struct {
	Ready  bool   `json:"ready"`
	Reason string `json:"reason,omitempty"`
}

// readiness reports whether this node is ready to serve and, if not,
// why.
func (n *Node) readiness() Readiness {
	switch {
	case n.shuttingDown.Load():
		return Readiness{Reason: "draining: the node is stopping"}
	case n.versionRefused.Load():
		return Readiness{Reason: "stopping: the cluster runs a version this binary does not support"}
	case !n.joined.Load():
		return Readiness{Reason: "not joined: the node's first heartbeat has not reached the cluster"}
	case (n.cfg.PGListen != "" || n.cfg.PGListener != nil) && n.sqlServer() == nil:
		return Readiness{Reason: "SQL is not serving yet"}
	}
	return Readiness{Ready: true}
}

func (n *Node) serveReady(w http.ResponseWriter, req *http.Request) {
	r := n.readiness()
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-store")
	if !r.Ready {
		w.WriteHeader(http.StatusServiceUnavailable)
	}
	_ = json.NewEncoder(w).Encode(r)
}
