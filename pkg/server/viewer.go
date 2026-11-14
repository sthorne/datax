package server

import (
	"context"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/sthorne/datax/pkg/keys"
	"github.com/sthorne/datax/pkg/kvclient"
	"github.com/sthorne/datax/pkg/sql/catalog"
	"github.com/sthorne/datax/pkg/util/events"
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
	// denied is the databases whose schema the caller may not use
	// (schemaUsageDenied): a table in one is neither seen nor read,
	// whatever its own grants say (issue #259).
	denied map[uint64]bool
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
	denied, err := n.schemaUsageDenied(ctx, set)
	if err != nil {
		return keyViewer{err: err, redacted: new(atomic.Bool)}
	}
	return keyViewer{set: set, descs: n.tableDescs(), denied: denied, redacted: new(atomic.Bool)}
}

// schemaUsageDenied is the set of databases whose schema the role set may
// not use: the clause of the SQL authorizer's read question that the
// table predicates do not carry (issue #259). SQL authorizes a table read
// as the table's own privilege AND usage of its schema, and REVOKE USAGE
// ON SCHEMA public FROM PUBLIC turns the second on: after it, a grantee
// holding SELECT on a table and no USAGE on its schema is refused the
// row, so the console must not decode the table's boundaries for it,
// nor list the table. Resolved once per request, as the roles are, for
// every database rather than per key: the viewer answers for many keys
// and the databases are few. An admin may use every schema; a database
// that does not restrict usage, or whose owner the set holds, is usable;
// otherwise USAGE must be held, directly or through a role. A database
// the read does not find is usable, as the authorizer treats it; a read
// that fails is an error, and the viewer fails closed on it.
func (n *Node) schemaUsageDenied(ctx context.Context, set catalog.RoleSet) (map[uint64]bool, error) {
	if set.IsAdmin() {
		return nil, nil
	}
	dbs, err := n.databaseDescs(ctx)
	if err != nil {
		return nil, err
	}
	return catalog.SchemaUsageDenied(dbs, set), nil
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
	return d != nil && !v.denied[d.DatabaseID] && pred(v.set, d)
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

// viewEvents serves evs to the caller. Who is served which form of a
// keyed event is events.Redact's decision and is unchanged: a reader
// who may not read every table an event's keys belong to gets the
// summary with each key at its prefix (issue #213).
//
// What changes is how the full form is printed for a reader who may
// (issue #280). The storage layer that records a split has no catalog,
// so its summary says "/table/5/1/3" where every range table on the
// same console says "bench_ts_s4 · /primary/3". An event that carries
// its keys is printed here with prettyKey, the printer the range tables
// use, so the two read the same. A reader who is refused is never
// handed that form: the check below is the same predicate over the same
// tables that Redact applies, made first.
func (n *Node) viewEvents(v keyViewer, evs []events.Event) []events.Event {
	for i := range evs {
		ev := &evs[i]
		if ev.Template == "" || strings.Count(ev.Template, events.KeyMark) != len(ev.Keys) {
			continue
		}
		readable := true
		for _, t := range ev.Tables {
			if !v.reads(t) {
				readable = false
				break
			}
		}
		if !readable {
			continue // Redact serves the prefix form
		}
		s := ev.Template
		for _, k := range ev.Keys {
			s = strings.Replace(s, events.KeyMark, n.prettyKey(keys.Key(k)), 1)
		}
		ev.Summary = s
	}
	return events.Redact(evs, v.reads)
}

// viewTable labels a range with its table's name when the caller may
// see it, and with nothing otherwise — as a system range is labelled.
func (n *Node) viewTable(v keyViewer, start keys.Key) string {
	if id, ok := keys.TableIDOf(start); ok && !v.sees(id) {
		return ""
	}
	return n.tableNameOf(start)
}

// databaseDescsFor is how long the viewer's database list is reused
// before it is read again: UsageRestricted changes about as often as a
// role grant, and the console polls every few seconds, so the list is
// read once per few seconds rather than once per document (InfoSec on
// the PR for #259). A failed read is not cached: the next request reads
// again, and fails closed until one succeeds.
const databaseDescsFor = 5 * time.Second

type databaseDescsCache struct {
	mu   sync.Mutex
	dbs  []*catalog.DatabaseDescriptor
	at   time.Time
	read bool
}

func (n *Node) databaseDescs(ctx context.Context) ([]*catalog.DatabaseDescriptor, error) {
	n.dbDescs.mu.Lock()
	if n.dbDescs.read && time.Since(n.dbDescs.at) < databaseDescsFor {
		dbs := n.dbDescs.dbs
		n.dbDescs.mu.Unlock()
		return dbs, nil
	}
	n.dbDescs.mu.Unlock()
	var dbs []*catalog.DatabaseDescriptor
	err := n.db.RunTxn(ctx, "schema-usage", func(ctx context.Context, txn *kvclient.Txn) error {
		var err error
		dbs, err = catalog.ListDatabases(ctx, txn)
		return err
	})
	if err != nil {
		return nil, err
	}
	n.dbDescs.mu.Lock()
	n.dbDescs.dbs, n.dbDescs.at, n.dbDescs.read = dbs, time.Now(), true
	n.dbDescs.mu.Unlock()
	return dbs, nil
}
