package server

import (
	"context"
	"net/http"
	"sync/atomic"

	"github.com/sthorne/datax/pkg/keys"
	"github.com/sthorne/datax/pkg/sql/catalog"
)

// keyViewer is one request's answer to which tables the caller may see
// rendered: their names (sees), and their boundary keys decoded into the
// row values a split was cut at (reads) (issues #213, #253).
//
// A range boundary is a real row key. Rendered, it reads
// /table/users/primary/"alice@example.com" — one exact value from a row,
// and the table it came from — and /status, /api/cluster, /api/overview,
// /api/node and the split events on /api/events carried one for every
// range in the cluster to any authenticated user, grant or no grant.
// The rule the rest of the console follows (#197's /api/schema,
// /api/security) is to keep the document and filter the data-bearing
// fields to what the caller may read; this is that rule applied to
// keys. A table the caller may read renders as before. One it may not
// renders as its table-and-index prefix by id (keys.PrettyPrefix), and
// one it may not even see has its name left out too — so /api/cluster's
// table list agrees with the same caller's /api/schema, and a grantee
// who may write a table and not read it gets its name and none of its
// rows. The range id, replica set, size and QPS
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
	// redacted is set the first time viewKey renders a key at its prefix
	// for this viewer: a document whose keys all decoded for the caller
	// has nothing to warn about, whatever the caller's role (issue
	// #241). A pointer, so every copy of this value-copied struct shares
	// the one flag; atomic, so a fan-out that one day renders a
	// document's keys on several goroutines cannot race it. Build a
	// viewer with keyViewerFor or allKeysViewer, which allocate it: a
	// restricted viewer built without it reports every key as redacted
	// (redactedAny), the direction a forgotten field should fail.
	redacted *atomic.Bool
}

// allKeysViewer is the viewer that sees everything: the node's own
// use, the admin-gated paths, and the admin role.
func allKeysViewer() keyViewer {
	return keyViewer{all: true, redacted: new(atomic.Bool)}
}

// keyViewerFor resolves the caller's effective roles, once per request.
// "May see" is privilege resolution, as /api/schema's is (issue #197):
// a grant to public, a grant through a role, ownership and the
// read_all / write_all roles all count.
func (n *Node) keyViewerFor(ctx context.Context, p ClusterPrincipal) keyViewer {
	if p.Admin {
		return allKeysViewer()
	}
	set, err := catalog.LazyRoleGraph(ctx, n.db).Effective(p.User)
	if err != nil {
		return keyViewer{err: err, redacted: new(atomic.Bool)}
	}
	return keyViewer{set: set, descs: n.tableDescs(), redacted: new(atomic.Bool)}
}

// redactedAny reports whether a key was rendered at its prefix for this
// viewer so far — the fact a document states as keys_redacted. A
// restricted viewer with nowhere to record that fails closed: it may
// have redacted, so it says it did.
func (v keyViewer) redactedAny() bool {
	if v.redacted == nil {
		return !v.all
	}
	return v.redacted.Load()
}

func (n *Node) keyViewer(req *http.Request) keyViewer {
	return n.keyViewerFor(req.Context(), n.clusterPrincipal(req))
}

// sees reports whether the caller may see table id named: the listing
// question, which any grant answers, as /api/schema's does. A table the
// schema cache does not know yet is not seen: the predicate cannot run,
// so it fails closed.
func (v keyViewer) sees(id uint64) bool {
	return v.may(id, catalog.CanSeeTable)
}

// reads reports whether the caller may read table id's rows: the
// question to ask before decoding one of its keys, since a range
// boundary is a row key and the decoded form is a row value. Any grant
// names the table; only a read privilege shows a row of it, so an
// INSERT-only grantee gets the table's name and its boundaries at their
// prefix, as the SQL authorizer would refuse it the row (issue #253).
func (v keyViewer) reads(id uint64) bool {
	return v.may(id, catalog.CanReadTable)
}

func (v keyViewer) may(id uint64, pred func(catalog.RoleSet, *catalog.TableDescriptor) bool) bool {
	if v.all {
		return true
	}
	if v.err != nil {
		return false
	}
	d := v.descs[id]
	return d != nil && pred(v.set, d)
}

// viewKey renders k for the caller: decoded when it belongs to a table
// the caller may read, at its prefix when not, and as for anyone when it
// belongs to no table. It also reports whether the key was rendered at
// its prefix, for a document that says so where the key is (issue
// #252): the document's keys_redacted is the any-of over every key it
// rendered, and a row labelled from it told a caller who may read some
// tables that its role did not see a key it was looking at in full.
func (n *Node) viewKey(v keyViewer, k keys.Key) (string, bool) {
	if id, ok := keys.TableIDOf(k); ok && !v.reads(id) {
		if v.redacted != nil {
			v.redacted.Store(true)
		}
		return keys.PrettyPrefix(k), true
	}
	return n.prettyKey(k), false
}

// viewTable labels a range with its table's name when the caller may
// see it, and with nothing otherwise — as a system range is labelled.
func (n *Node) viewTable(v keyViewer, start keys.Key) string {
	if id, ok := keys.TableIDOf(start); ok && !v.sees(id) {
		return ""
	}
	return n.tableNameOf(start)
}
