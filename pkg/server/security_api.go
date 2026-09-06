package server

import (
	"context"
	"encoding/json"
	"net/http"
	"sort"
	"time"

	"github.com/sthorne/datax/pkg/cluster"
	"github.com/sthorne/datax/pkg/metrics"
	"github.com/sthorne/datax/pkg/security"
	"github.com/sthorne/datax/pkg/sql/catalog"
)

// /api/security (issue #156). Security state was scattered: audit
// records rode the general event ring behind an amber outline, the
// signed-in principal was in the header, users were a small table
// appended to the schema section, and certificate expiry was nowhere at
// all. Nothing answered "who can reach this cluster and how".
//
// This is that answer, assembled from what the node already holds. The
// gate follows the data: what a certificate expires is operational and
// open to any authenticated user, while who has been connecting — the
// per-user breakdown and the client certificates presented to this node
// — names people, and travels with the admin surface.

// SecurityRole is one role and how it is reached.
type SecurityRole struct {
	Name string `json:"name"`
	// Builtin roles always exist, cannot log in, and carry their meaning
	// in the authorization code rather than in a descriptor.
	Builtin bool `json:"builtin,omitempty"`
	Login   bool `json:"login,omitempty"`
	// NoInherit: the role does not automatically hold what the roles it
	// belongs to hold.
	NoInherit bool `json:"no_inherit,omitempty"`
	// MemberOf is the membership granted directly; Effective is what
	// that resolves to through the graph, so "why is this an admin" is
	// answerable without walking it by hand.
	MemberOf  []string `json:"member_of,omitempty"`
	Effective []string `json:"effective,omitempty"`
	Admin     bool     `json:"admin,omitempty"`
}

// SecurityUserAuth is one user's current connections by how they got in.
type SecurityUserAuth struct {
	User string `json:"user"`
	// Via counts connections by method: "cert", "scram", or "trust" in
	// insecure mode.
	Via   map[string]int `json:"via"`
	Total int            `json:"total"`
}

// SecurityStore is one store's encryption state.
type SecurityStore struct {
	NodeID    int  `json:"node_id"`
	Encrypted bool `json:"encrypted"`
	// Reencryption is the sweep rewriting sstables onto the active key
	// after a rotation; nil when the store is not encrypted or no sweep
	// has run.
	Reencryption *cluster.ReencryptionStatus `json:"reencryption,omitempty"`
}

// SecurityStatus is the /api/security document.
type SecurityStatus struct {
	Now       int64            `json:"now_unix_ms"`
	NodeID    int              `json:"node_id"`
	Principal ClusterPrincipal `json:"principal"`
	// Secure is whether this cluster authenticates at all.
	Secure bool `json:"secure"`
	// Certificates is the TLS material this node loaded, soonest expiry
	// first. Empty in insecure mode, which is the honest answer rather
	// than an absent section.
	Certificates []security.CertInfo `json:"certificates"`
	// ClientCerts is the client certificates actually presented to this
	// node's HTTP listener (admin only): what has been connecting, not a
	// directory of what could. ClientCertsFull says the bounded table is
	// full, so the list is a sample rather than the whole of it.
	ClientCerts     []security.CertInfo `json:"client_certs,omitempty"`
	ClientCertsFull bool                `json:"client_certs_full,omitempty"`
	// Roles is every role and its membership; built-in roles are marked.
	Roles []SecurityRole `json:"roles"`
	// Connections breaks this node's SQL connections down by user and
	// authentication method (admin only).
	Connections []SecurityUserAuth `json:"connections,omitempty"`
	// AuthFailures and AdminDenied are cumulative on this node since it
	// started — each node counts its own, so neither is a cluster total.
	AuthFailures float64 `json:"auth_failures"`
	AdminDenied  float64 `json:"admin_denied"`
	// Stores is the serving node's own store encryption state. Each
	// node's is on its own page; a cluster-wide sweep would be a fan-out
	// this view does not make.
	Stores []SecurityStore `json:"stores"`
	// Error notes a partial document; what is present is still valid.
	Error string `json:"error,omitempty"`
}

func (n *Node) serveSecurityAPI(w http.ResponseWriter, req *http.Request) {
	doc := n.securityDoc(req)
	w.Header().Set("Content-Type", "application/json")
	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ")
	_ = enc.Encode(doc)
}

func (n *Node) securityDoc(req *http.Request) SecurityStatus {
	p := n.clusterPrincipal(req)
	doc := SecurityStatus{
		Now:          n.clock.Now().WallTime / int64(time.Millisecond),
		NodeID:       int(n.ident.NodeID),
		Principal:    p,
		Secure:       n.tlsCfgs != nil,
		Certificates: []security.CertInfo{},
		Roles:        []SecurityRole{},
		Stores:       []SecurityStore{},
		AuthFailures: counterValue(metrics.AuthFailures),
		AdminDenied:  counterValue(metrics.AdminDenied),
	}

	// Certificates. The loaded material is operational and open; the
	// clients observed connecting name people, so they are not.
	certs, full := n.certs.all()
	for _, ci := range certs {
		if ci.Kind == "client" {
			if p.Admin {
				doc.ClientCerts = append(doc.ClientCerts, ci)
			}
			continue
		}
		doc.Certificates = append(doc.Certificates, ci)
	}
	doc.ClientCertsFull = p.Admin && full

	// Roles: membership is not secret to an authenticated user.
	ctx := req.Context()
	doc.Roles = n.securityRoles(ctx, &doc)

	// Who is connected and how (admin only).
	if p.Admin && n.sqlServer() != nil {
		byAuth := n.sqlServer().Activity().ByAuth()
		for user, via := range byAuth {
			total := 0
			for _, c := range via {
				total += c
			}
			doc.Connections = append(doc.Connections, SecurityUserAuth{User: user, Via: via, Total: total})
		}
		sort.Slice(doc.Connections, func(i, j int) bool {
			if doc.Connections[i].Total != doc.Connections[j].Total {
				return doc.Connections[i].Total > doc.Connections[j].Total
			}
			return doc.Connections[i].User < doc.Connections[j].User
		})
	}

	// The serving node's store.
	if n.engine != nil {
		st := SecurityStore{NodeID: int(n.ident.NodeID), Encrypted: n.engine.Encrypted()}
		if st.Encrypted {
			st.Reencryption = n.reencryptionStatus()
		}
		doc.Stores = append(doc.Stores, st)
	}
	return doc
}

// securityRoles lists every role with its membership resolved.
func (n *Node) securityRoles(ctx context.Context, doc *SecurityStatus) []SecurityRole {
	out := make([]SecurityRole, 0, 8)
	for _, b := range catalog.BuiltinRoles {
		out = append(out, SecurityRole{Name: b, Builtin: true})
	}
	roles, err := catalog.ListRoles(ctx, n.db)
	if err != nil {
		doc.Error = "the role list is unavailable: " + err.Error()
		return out
	}
	graph := catalog.NewRoleGraph(roles)
	for _, r := range roles {
		sr := SecurityRole{Name: r.Name, Login: r.Login, NoInherit: r.NoInherit}
		for _, m := range r.MemberOf {
			sr.MemberOf = append(sr.MemberOf, m.Role)
		}
		if set, gerr := graph.Effective(r.Name); gerr == nil {
			for name := range set {
				if name != r.Name {
					sr.Effective = append(sr.Effective, name)
				}
			}
			sort.Strings(sr.Effective)
			_, sr.Admin = set[catalog.AdminRole]
		}
		if r.Name == catalog.RootRole {
			sr.Admin = true
		}
		out = append(out, sr)
	}
	return out
}
