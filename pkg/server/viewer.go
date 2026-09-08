package server

import (
	"context"
	"net/http"

	"github.com/sthorne/datax/pkg/keys"
	"github.com/sthorne/datax/pkg/sql/catalog"
)

// keyViewer is one request's answer to which tables the caller may see
// rendered: their names, and their boundary keys decoded into the row
// values a split was cut at (issue #213).
//
// A range boundary is a real row key. Rendered, it reads
// /table/users/primary/"alice@example.com" — one exact value from a row,
// and the table it came from — and /status, /api/cluster, /api/overview,
// /api/node and the split events on /api/events carried one for every
// range in the cluster to any authenticated user, grant or no grant.
// The rule the rest of the console follows (#197's /api/schema,
// /api/security) is to keep the document and filter the data-bearing
// fields to what the caller may read; this is that rule applied to
// keys. A table the caller may see renders as before. One it may not
// renders as its table-and-index prefix by id (keys.PrettyPrefix), and
// its name is left out — so /api/cluster's table list agrees with the
// same caller's /api/schema. The range id, replica set, size and QPS
// carry the operational meaning and none of the data. System and meta
// keys carry no row and render for everyone.
type keyViewer struct {
	// all: the caller sees everything — the admin role, or insecure
	// mode, where there is no identity.
	all   bool
	set   catalog.RoleSet
	descs map[uint64]*catalog.TableDescriptor
	// err is why the caller's roles could not be resolved. The viewer
	// then sees no table at all: fail closed, and say so where the
	// document has a place for it.
	err error
}

// keyViewerFor resolves the caller's effective roles, once per request.
// "May see" is privilege resolution, as /api/schema's is (issue #197):
// a grant to public, a grant through a role, ownership and the
// read_all / write_all roles all count.
func (n *Node) keyViewerFor(ctx context.Context, p ClusterPrincipal) keyViewer {
	if p.Admin {
		return keyViewer{all: true}
	}
	set, err := catalog.LazyRoleGraph(ctx, n.db).Effective(p.User)
	if err != nil {
		return keyViewer{err: err}
	}
	return keyViewer{set: set, descs: n.tableDescs()}
}

func (n *Node) keyViewer(req *http.Request) keyViewer {
	return n.keyViewerFor(req.Context(), n.clusterPrincipal(req))
}

// sees reports whether the caller may see table id: its name, and its
// keys decoded. A table the schema cache does not know yet is not seen:
// the predicate cannot run, so it fails closed.
func (v keyViewer) sees(id uint64) bool {
	if v.all {
		return true
	}
	if v.err != nil {
		return false
	}
	d := v.descs[id]
	return d != nil && catalog.CanSeeTable(v.set, d)
}

// viewKey renders k for the caller: decoded when it belongs to a table
// the caller may see, at its prefix when not, and as for anyone when it
// belongs to no table.
func (n *Node) viewKey(v keyViewer, k keys.Key) string {
	if id, ok := keys.TableIDOf(k); ok && !v.sees(id) {
		return keys.PrettyPrefix(k)
	}
	return n.prettyKey(k)
}

// viewTable labels a range with its table's name when the caller may
// see it, and with nothing otherwise — as a system range is labelled.
func (n *Node) viewTable(v keyViewer, start keys.Key) string {
	if id, ok := keys.TableIDOf(start); ok && !v.sees(id) {
		return ""
	}
	return n.tableNameOf(start)
}
