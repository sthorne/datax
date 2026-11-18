package sql

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"time"

	"github.com/sthorne/datax/pkg/keys"
	"github.com/sthorne/datax/pkg/kvclient"
	"github.com/sthorne/datax/pkg/kvpb"
	"github.com/sthorne/datax/pkg/metrics"
	"github.com/sthorne/datax/pkg/sql/catalog"
	"github.com/sthorne/datax/pkg/sql/parser"
	"github.com/sthorne/datax/pkg/sql/rowenc"
	"github.com/sthorne/datax/pkg/sql/types"
	"github.com/sthorne/datax/pkg/sql/vtable"
	"github.com/sthorne/datax/pkg/util/log"
	"github.com/sthorne/datax/pkg/version"
)

// Large objects (issue #303): values too big to be a row value, written
// and read as a stream through PostgreSQL's large-object functions
// (lo_create, lo_open, lowrite, loread, lo_lseek64, lo_truncate64,
// lo_close, lo_unlink), which drivers already speak.
//
// An ordinary value is one KV write: it crosses the wire in one
// message, sits whole in the gateway's memory several times over and is
// one Raft proposal. A large object is instead cut into pages of
// LargeObjectPageSize bytes, each an ordinary row of a system table, so
// replication, range splits, backup and encryption at rest treat it as
// they treat any table. Three tables hold them:
//
//	datax_largeobject_metadata (loid, cid, owner, created_at)
//	    one row per object: the id clients name, the content it
//	    currently stands for, and who owns it. An object exists exactly
//	    when this row does.
//	datax_largeobject (cid, pageno, data)
//	    the pages of a content. A page that is absent, or shorter than
//	    the page size before the last page, reads as zeros (a sparse
//	    object, as in PostgreSQL); the object's size is where its last
//	    page ends.
//	datax_largeobject_pending (cid, loid, kind, node, at)
//	    contents that no object may be standing for: one being uploaded
//	    (kind upload), or one whose object was unlinked (kind unlink).
//	    The sweeper deletes their pages (SweepLargeObjects).
//
// Pages are keyed by a content id rather than by the object id because
// an object id can be chosen by the client and so used again: the pages
// of an unlinked object are deleted later, by the sweeper, and must not
// be mistaken for the pages of a new object of the same id.
//
// # Writing a new object
//
// A multi-gigabyte upload cannot be one transaction: its write set would
// be the object. So the pages of an object created in the open
// transaction block are written outside that transaction, each as its
// own small write, as they arrive; only the metadata row is written in
// the transaction. Nobody else can see the object until the block
// commits, so nobody can see its pages half written, and the commit of
// that one row is what makes the object exist.
//
// So that the upload holds no lock while it runs — a transaction that
// kept an intent on the metadata row for the minutes a large upload
// takes would hold up every backup and every scan of that table for as
// long — the metadata row itself is written only when the block
// commits.
//
// What a rollback or a dead gateway leaves behind is pages nobody
// refers to. The pending row accounts for them: it is written, outside
// the transaction, before the first page, and the committing
// transaction deletes it in the same breath as it writes the metadata
// row — so it disappears exactly when the object becomes visible and
// survives every other ending. An upload that ends without committing
// marks its row for the sweeper on the way out; one whose gateway died
// cannot, so a live upload also refreshes its row's timestamp as it
// goes (a heartbeat, every largeObjectHeartbeat of activity), and the
// sweeper takes an upload row nobody has refreshed for the grace period
// for abandoned. Every change to the row is conditional on what the row
// says: the sweeper claims an upload by rewriting its row before it
// deletes a page, and the heartbeat and the commit both stop when they
// find the row claimed or gone. An upload its client left idle for
// longer than the grace period therefore fails cleanly (55000) rather
// than committing an object whose pages have been deleted.
//
// One more thing has to hold for readers: a transaction that can see the
// metadata row must be able to see every page. Pages are written at the
// times they are written, which are later than the creating
// transaction's own timestamp, so before committing the transaction is
// moved forward past the last of them (Txn.AdvanceTo).
//
// # Changing an existing object
//
// Writes to an object that was already committed go through the
// session's transaction like any row write — other transactions may be
// reading the object — and are therefore bounded: at most
// largeObjectTxnWriteLimit bytes per transaction. More than that is a
// new object.

const (
	// LargeObjectPageSize is the size of a large object's pages. A page
	// is one KV value and one Raft proposal, so it wants to be far below
	// the sizes at which those hurt (a range's target size, the
	// internode message limit) and far above the size at which the
	// per-key overhead shows; PostgreSQL's 2 kB would make a 1 GiB object
	// half a million keys.
	LargeObjectPageSize = 256 << 10
	// LargeObjectMaxSize is the largest object accepted (54000 beyond).
	// Nothing in the layout sets it — page numbers are 64-bit — it is a
	// stated bound rather than a discovered one.
	LargeObjectMaxSize = int64(1) << 40
	// largeObjectTxnWriteLimit bounds the bytes one transaction may
	// write into objects through itself — objects that existed before
	// it, and anything written by a statement outside a transaction
	// block: those writes are intents the transaction holds until it
	// commits. (TestingLargeObjectTxnWriteLimit replaces it in a test.)
	largeObjectTxnWriteLimit = 64 << 20

	// PostgreSQL's lo_open mode bits.
	loModeWrite = 0x20000
	loModeRead  = 0x40000

	loPendingUpload = "upload"
	loPendingUnlink = "unlink"

	// minLargeObjectID keeps generated ids clear of the small numbers a
	// person picks by hand; maxLargeObjectID is the largest an OID holds.
	minLargeObjectID = 1 << 16
	maxLargeObjectID = 1<<32 - 1
)

// largeObjectDDL creates the three tables; IF NOT EXISTS makes the
// gateways' concurrent first uses idempotent.
var largeObjectDDL = []string{
	`CREATE TABLE IF NOT EXISTS ` + catalog.LargeObjectMetadataTableName + ` (
  loid INT8 NOT NULL,
  cid INT8 NOT NULL,
  owner TEXT NOT NULL,
  created_at TIMESTAMPTZ NOT NULL,
  PRIMARY KEY (loid)
)`,
	`CREATE TABLE IF NOT EXISTS ` + catalog.LargeObjectTableName + ` (
  cid INT8 NOT NULL,
  pageno INT8 NOT NULL,
  data BYTEA NOT NULL,
  PRIMARY KEY (cid, pageno)
)`,
	`CREATE TABLE IF NOT EXISTS ` + catalog.LargeObjectPendingTableName + ` (
  cid INT8 NOT NULL,
  loid INT8 NOT NULL,
  kind TEXT NOT NULL,
  node INT8 NOT NULL,
  at TIMESTAMPTZ NOT NULL,
  PRIMARY KEY (cid)
)`,
}

// Column IDs of the tables above (the position in the declaration).
const (
	loMetaColCID     catalog.ColumnID = 2
	loMetaColOwner   catalog.ColumnID = 3
	loMetaColCreated catalog.ColumnID = 4

	loPageColData catalog.ColumnID = 3

	loPendingColLoid catalog.ColumnID = 2
	loPendingColKind catalog.ColumnID = 3
	loPendingColNode catalog.ColumnID = 4
	loPendingColAt   catalog.ColumnID = 5
)

// loTables are the three tables' descriptors.
type loTables struct {
	meta, pages, pending *catalog.TableDescriptor
}

// lookupLargeObjectTables resolves the tables, or nil when they have
// not been created. The lookup runs in a transaction of its own: a
// session transaction that began before the tables existed would not
// find them.
func lookupLargeObjectTables(ctx context.Context, db *kvclient.DB, cat *catalog.Accessor) (*loTables, error) {
	var t *loTables
	err := db.RunTxn(ctx, "lo-tables", func(ctx context.Context, txn *kvclient.Txn) error {
		t = nil
		var got loTables
		for name, dst := range map[string]**catalog.TableDescriptor{
			catalog.LargeObjectMetadataTableName: &got.meta,
			catalog.LargeObjectTableName:         &got.pages,
			catalog.LargeObjectPendingTableName:  &got.pending,
		} {
			d, err := cat.LookupIn(ctx, txn, catalog.DefaultDatabase, name)
			if err != nil {
				var nf *catalog.ErrTableNotFound
				if errors.As(err, &nf) {
					return nil
				}
				return err
			}
			*dst = d
		}
		t = &got
		return nil
	})
	return t, err
}

// EnsureLargeObjectTables creates the three tables if they are not
// there and returns their descriptors. The nodes call it once the
// cluster is at v21, so that the tables exist before any session wants
// them: created on first use instead, they would be created underneath
// the open transaction of that first use, and a transaction that had
// read the catalog — libpq looks up the functions' OIDs in pg_proc
// inside it — could then not commit.
//
// The node's own session creates them: the tables are the cluster's,
// whoever stores the first object.
func EnsureLargeObjectTables(ctx context.Context, db *kvclient.DB, cat *catalog.Accessor) (*loTables, error) {
	if db.ClusterVersion() < version.V21 {
		return nil, newErrf(CodeFeatureNotSupported, "large objects need cluster version v21: finalize the upgrade with `datax debug upgrade` first")
	}
	t, err := lookupLargeObjectTables(ctx, db, cat)
	if err != nil || t != nil {
		return t, err
	}
	sys := NewSystemSession(db, cat)
	for _, ddl := range largeObjectDDL {
		stmts, err := parser.Parse(ddl)
		if err != nil {
			return nil, err
		}
		if _, serr := sys.Execute(ctx, stmts[0], nil); serr != nil {
			return nil, serr
		}
	}
	if t, err = lookupLargeObjectTables(ctx, db, cat); err != nil {
		return nil, err
	}
	if t == nil {
		return nil, newErrf(CodeInternal, "the large-object tables could not be created")
	}
	return t, nil
}

// loState is a session's large-object state: the descriptors open in
// its transaction and what it knows of the objects behind them.
type loState struct {
	tables *loTables
	// txn is the transaction the descriptors belong to. Descriptors do
	// not outlive it, as in PostgreSQL.
	txn    *kvclient.Txn
	fds    map[int32]*loDesc
	nextFD int32
	objs   map[int64]*loObject // by content id
	// txnBytes counts what this transaction has written into objects
	// through itself (largeObjectTxnWriteLimit).
	txnBytes int64
	// created are the objects made in this transaction block, by object
	// id: their metadata rows are written when the block commits.
	created map[int64]*loObject
	// broken is set when the transaction rolled back to a savepoint
	// after writing pages outside itself: those writes were not undone,
	// so the objects it created can no longer be committed.
	broken bool
}

// loObject is one content as this transaction sees it.
type loObject struct {
	loid, cid int64
	// staged: created in this transaction block, pages written outside
	// the transaction. owner and createdAt are what its metadata row
	// will say, and lastBeat when its pending row was last refreshed.
	staged    bool
	owner     string
	createdAt int64
	lastBeat  time.Time
	// size is where the last page ends, once known.
	size      int64
	sizeKnown bool
}

// loDesc is an open descriptor: a position in an object and at most one
// page of it held in memory, so a stream of small writes costs one page
// write per page rather than one per call.
type loDesc struct {
	obj   *loObject
	write bool
	pos   int64
	page  int64 // the buffered page's number; -1 when none
	buf   []byte
	dirty bool
}

// largeObjectFuncs are the large-object functions; the session
// evaluates them once per statement, with constant arguments.
var largeObjectFuncs = map[string]bool{
	"lo_create": true, "lo_creat": true, "lo_open": true, "lo_close": true, "loread": true, "lowrite": true,
	"lo_lseek": true, "lo_lseek64": true, "lo_tell": true, "lo_tell64": true, "lo_truncate": true, "lo_truncate64": true,
	"lo_unlink": true, "lo_get": true, "lo_put": true, "lo_from_bytea": true,
}

// largeObjectWrites are the ones that change something. (lo_open is
// not among them: opened for writing in a read-only transaction, the
// descriptor's first lowrite is what is refused.)
var largeObjectWrites = map[string]bool{
	"lo_create": true, "lo_creat": true, "lowrite": true, "lo_truncate": true, "lo_truncate64": true,
	"lo_unlink": true, "lo_put": true, "lo_from_bytea": true,
}

func init() {
	for name := range largeObjectFuncs {
		volatileFuncs[name] = true
		splicedFuncs[name] = true
	}
}

func loErrNoDescriptor(fd int64) *Error {
	return newErrf(CodeUndefinedObject, "invalid large-object descriptor: %d", fd)
}

func loErrNoObject(loid int64) *Error {
	return newErrf(CodeUndefinedObject, "large object %d does not exist", loid)
}

// loBegin returns the state for a call running in txn, dropping what an
// earlier transaction left, and makes sure the tables exist.
func (s *Session) loBegin(ctx context.Context, txn *kvclient.Txn) (*loState, error) {
	if s.db.ClusterVersion() < version.V21 {
		return nil, newErrf(CodeFeatureNotSupported, "large objects need cluster version v21: finalize the upgrade with `datax debug upgrade` first")
	}
	if s.lo == nil {
		s.lo = &loState{}
	}
	st := s.lo
	if st.txn != txn {
		st.txn, st.fds, st.objs, st.created = txn, map[int32]*loDesc{}, map[int64]*loObject{}, map[int64]*loObject{}
		st.nextFD, st.txnBytes, st.broken = 0, 0, false
	}
	if st.broken {
		return nil, newErrf(CodeInFailedTransaction, "this transaction rolled back to a savepoint after streaming a new large object, which a savepoint cannot undo: roll the transaction back")
	}
	if st.tables == nil {
		// Normally there already: the nodes create the tables when the
		// cluster reaches v21 (EnsureLargeObjectTables). A first object
		// stored before they have is what this covers.
		t, err := EnsureLargeObjectTables(ctx, s.db, s.cat)
		if err != nil {
			return nil, err
		}
		st.tables = t
	}
	return st, nil
}

// loExplicit reports whether txn is the session's open transaction
// block — the only place a descriptor lives past its statement, and the
// only place a new object's pages are written outside the transaction.
func (s *Session) loExplicit(txn *kvclient.Txn) bool {
	return s.state == StateOpen && s.txn == txn
}

func (t *loTables) metaKey(loid int64) (keys.Key, error) {
	return rowenc.EncodePK(t.meta, []types.Datum{types.NewInt(loid)})
}

func (t *loTables) pageKey(cid, page int64) (keys.Key, error) {
	return rowenc.EncodePK(t.pages, []types.Datum{types.NewInt(cid), types.NewInt(page)})
}

func (t *loTables) pendingKey(cid int64) (keys.Key, error) {
	return rowenc.EncodePK(t.pending, []types.Datum{types.NewInt(cid)})
}

func (t *loTables) pendingValue(loid int64, kind string, node int64, at int64) ([]byte, error) {
	return rowenc.EncodeValue(t.pending, map[catalog.ColumnID]types.Datum{
		loPendingColLoid: types.NewInt(loid), loPendingColKind: types.NewString(kind),
		loPendingColNode: types.NewInt(node), loPendingColAt: types.NewTimestamp(at),
	})
}

// loMeta is a decoded metadata row.
type loMeta struct {
	cid   int64
	owner string
}

// loReadMeta reads an object's metadata row through the transaction
// (locking it when the caller is about to change the object); nil when
// there is no such object.
func (s *Session) loReadMeta(ctx context.Context, txn *kvclient.Txn, st *loState, loid int64, lock bool) (*loMeta, error) {
	key, err := st.tables.metaKey(loid)
	if err != nil {
		return nil, err
	}
	var raw []byte
	if lock {
		raw, err = txn.GetForUpdate(ctx, key)
	} else {
		raw, err = txn.Get(ctx, key)
	}
	if err != nil || raw == nil {
		return nil, err
	}
	row, err := rowenc.DecodeValue(st.tables.meta, raw)
	if err != nil {
		return nil, err
	}
	return &loMeta{cid: row[loMetaColCID].I, owner: row[loMetaColOwner].S}, nil
}

// loCheckAccess: an object is its owner's (and the admins'). There are
// no per-object grants.
func (s *Session) loCheckAccess(ctx context.Context, txn *kvclient.Txn, loid int64, m *loMeta) error {
	return s.checkOwner(ctx, txn, "large object", fmt.Sprint(loid), m.owner)
}

// largeObjectHeartbeat is how often a running upload refreshes its
// pending row. TestingLargeObjectHeartbeat, when set before any session
// exists, replaces it (a test's grace period is seconds, not minutes).
const largeObjectHeartbeat = 15 * time.Second

var TestingLargeObjectHeartbeat time.Duration

// TestingLargeObjectTxnWriteLimit, when set before any session exists,
// replaces largeObjectTxnWriteLimit.
var TestingLargeObjectTxnWriteLimit int64

func loErrReclaimed(loid int64) *Error {
	return newErrf(CodeObjectNotInState, "the upload of large object %d sat idle for too long and its pages have been reclaimed: roll the transaction back and upload again", loid)
}

// loPendingKind reads a content's pending row through txn: its kind, or
// "" when there is none.
func loPendingKind(ctx context.Context, txn *kvclient.Txn, t *loTables, cid int64, lock bool) (string, error) {
	pk, err := t.pendingKey(cid)
	if err != nil {
		return "", err
	}
	var raw []byte
	if lock {
		raw, err = txn.GetForUpdate(ctx, pk)
	} else {
		raw, err = txn.Get(ctx, pk)
	}
	if err != nil || raw == nil {
		return "", err
	}
	row, err := rowenc.DecodeValue(t.pending, raw)
	if err != nil {
		return "", err
	}
	return row[loPendingColKind].S, nil
}

// loBeat refreshes an upload's pending row when it is due, so the
// sweeper can tell the upload from an abandoned one. The refresh is
// conditional: a row the sweeper has claimed is not taken back.
func (s *Session) loBeat(ctx context.Context, st *loState, obj *loObject) error {
	every := largeObjectHeartbeat
	if TestingLargeObjectHeartbeat > 0 {
		every = TestingLargeObjectHeartbeat
	}
	if !obj.staged || time.Since(obj.lastBeat) < every {
		return nil
	}
	reclaimed := false
	err := s.db.RunTxn(ctx, "lo-heartbeat", func(ctx context.Context, txn *kvclient.Txn) error {
		kind, err := loPendingKind(ctx, txn, st.tables, obj.cid, false)
		if err != nil {
			return err
		}
		if reclaimed = kind != loPendingUpload; reclaimed {
			return nil
		}
		pk, err := st.tables.pendingKey(obj.cid)
		if err != nil {
			return err
		}
		pv, err := st.tables.pendingValue(obj.loid, loPendingUpload, int64(s.db.LocalNodeID()), s.db.Clock().Now().WallTime)
		if err != nil {
			return err
		}
		return txn.Put(ctx, pk, pv)
	})
	if err != nil {
		return err
	}
	if reclaimed {
		return loErrReclaimed(obj.loid)
	}
	obj.lastBeat = time.Now()
	return nil
}

// loAbandon hands an upload that will not commit to the sweeper, if it
// is still an upload: a transaction whose commit did go through has
// deleted the row, and then there is nothing to hand over.
func (s *Session) loAbandon(ctx context.Context, t *loTables, obj *loObject) error {
	return s.db.RunTxn(ctx, "lo-abandon", func(ctx context.Context, txn *kvclient.Txn) error {
		kind, err := loPendingKind(ctx, txn, t, obj.cid, false)
		if err != nil || kind != loPendingUpload {
			return err
		}
		pk, err := t.pendingKey(obj.cid)
		if err != nil {
			return err
		}
		pv, err := t.pendingValue(obj.loid, loPendingUnlink, int64(s.db.LocalNodeID()), s.db.Clock().Now().WallTime)
		if err != nil {
			return err
		}
		return txn.Put(ctx, pk, pv)
	})
}

func (t *loTables) metaValue(obj *loObject) ([]byte, error) {
	return rowenc.EncodeValue(t.meta, map[catalog.ColumnID]types.Datum{
		loMetaColCID: types.NewInt(obj.cid), loMetaColOwner: types.NewString(obj.owner), loMetaColCreated: types.NewTimestamp(obj.createdAt),
	})
}

// loCreate makes a new, empty object: loid 0 asks for an id.
func (s *Session) loCreate(ctx context.Context, txn *kvclient.Txn, st *loState, loid int64) (*loObject, error) {
	if loid < 0 || loid > maxLargeObjectID {
		return nil, newErrf(CodeInvalidParameterValue, "large object id %d is out of range (0 to %d)", loid, int64(maxLargeObjectID))
	}
	taken := func(id int64) (bool, error) {
		if _, mine := st.created[id]; mine {
			return true, nil
		}
		m, err := s.loReadMeta(ctx, txn, st, id, false)
		return m != nil, err
	}
	if loid == 0 {
		// A random id, checked through the transaction: two sessions that
		// draw the same one conflict on the metadata row.
		for try := 0; ; try++ {
			cand := minLargeObjectID + rand.Int63n(maxLargeObjectID-minLargeObjectID)
			used, err := taken(cand)
			if err != nil {
				return nil, err
			}
			if !used {
				loid = cand
				break
			}
			if try == 20 {
				return nil, newErrf(CodeInternal, "could not find a free large object id")
			}
		}
	} else if used, err := taken(loid); err != nil {
		return nil, err
	} else if used {
		return nil, newErrf(CodeDuplicateObject, "large object %d already exists", loid)
	}
	obj := &loObject{loid: loid, cid: s.uniqueRowID(), sizeKnown: true, owner: s.user, createdAt: s.db.Clock().Now().WallTime}
	metrics.LargeObjectsCreated.Inc()
	if s.loExplicit(txn) {
		// The pending row goes in first and outside the transaction, so
		// that whatever becomes of the transaction the pages written from
		// here on are accounted for. The metadata row waits for COMMIT
		// (loBeforeCommit).
		pk, err := st.tables.pendingKey(obj.cid)
		if err != nil {
			return nil, err
		}
		pv, err := st.tables.pendingValue(loid, loPendingUpload, int64(s.db.LocalNodeID()), obj.createdAt)
		if err != nil {
			return nil, err
		}
		if err := s.db.Put(ctx, pk, pv); err != nil {
			return nil, err
		}
		obj.staged, obj.lastBeat = true, time.Now()
		st.created[loid] = obj
		st.objs[obj.cid] = obj
		return obj, nil
	}
	mk, err := st.tables.metaKey(loid)
	if err != nil {
		return nil, err
	}
	mv, err := st.tables.metaValue(obj)
	if err != nil {
		return nil, err
	}
	if err := txn.Put(ctx, mk, mv); err != nil {
		return nil, err
	}
	st.objs[obj.cid] = obj
	return obj, nil
}

// loLookup resolves an object for reading or writing.
func (s *Session) loLookup(ctx context.Context, txn *kvclient.Txn, st *loState, loid int64, write bool) (*loObject, error) {
	if obj, mine := st.created[loid]; mine {
		return obj, nil
	}
	m, err := s.loReadMeta(ctx, txn, st, loid, false)
	if err != nil {
		return nil, err
	}
	if m == nil {
		return nil, loErrNoObject(loid)
	}
	if err := s.loCheckAccess(ctx, txn, loid, m); err != nil {
		return nil, err
	}
	obj := st.objs[m.cid]
	if obj == nil {
		obj = &loObject{loid: loid, cid: m.cid}
		st.objs[m.cid] = obj
	}
	if write {
		// A writer holds the metadata row: an unlink of the object waits
		// for it, so the sweeper never runs under a write in flight.
		if m, err = s.loReadMeta(ctx, txn, st, loid, true); err != nil {
			return nil, err
		} else if m == nil || m.cid != obj.cid {
			return nil, loErrNoObject(loid)
		}
	}
	return obj, nil
}

func (s *Session) loOpen(st *loState, obj *loObject, write bool) int32 {
	fd := st.nextFD
	st.nextFD++
	st.fds[fd] = &loDesc{obj: obj, write: write, page: -1}
	return fd
}

// The three ways a page is reached: through the transaction for a
// committed object, directly for one this transaction is creating.

func (s *Session) loPutPage(ctx context.Context, st *loState, obj *loObject, page int64, data []byte) error {
	key, err := st.tables.pageKey(obj.cid, page)
	if err != nil {
		return err
	}
	val, err := rowenc.EncodeValue(st.tables.pages, map[catalog.ColumnID]types.Datum{loPageColData: types.NewBytes(data)})
	if err != nil {
		return err
	}
	metrics.LargeObjectBytesWritten.Add(float64(len(data)))
	if obj.staged {
		if err := s.loBeat(ctx, st, obj); err != nil {
			return err
		}
		return s.db.Put(ctx, key, val)
	}
	limit := int64(largeObjectTxnWriteLimit)
	if TestingLargeObjectTxnWriteLimit > 0 {
		limit = TestingLargeObjectTxnWriteLimit
	}
	if st.txnBytes += int64(len(data)); st.txnBytes > limit {
		return newErrf(CodeProgramLimitExceeded, "one transaction may write at most %s into large objects through itself; a large object of any size is written by creating it inside a transaction block (BEGIN, lo_create) and writing it there, before COMMIT", formatMemory(limit))
	}
	return st.txn.Put(ctx, key, val)
}

func (s *Session) loDeleteKeys(ctx context.Context, st *loState, obj *loObject, ks []keys.Key) error {
	if len(ks) == 0 {
		return nil
	}
	if obj.staged {
		return deleteKeysNow(ctx, s.db, ks)
	}
	var wb kvclient.WriteBatch
	for _, k := range ks {
		wb.Delete(k)
	}
	return st.txn.RunBatch(ctx, &wb)
}

// loScanPages reads the pages numbered [from, to] that exist, at most
// max of them (0 = all).
func (s *Session) loScanPages(ctx context.Context, st *loState, obj *loObject, from, to, max int64) ([]loPage, error) {
	start, err := st.tables.pageKey(obj.cid, from)
	if err != nil {
		return nil, err
	}
	end, err := st.tables.pageKey(obj.cid, to)
	if err != nil {
		return nil, err
	}
	end = end.PrefixEnd()
	var kvs []kvpb.KeyValue
	if obj.staged {
		kvs, err = s.db.Scan(ctx, start, end, max)
	} else {
		kvs, err = st.txn.Scan(ctx, start, end, max)
	}
	if err != nil {
		return nil, err
	}
	return decodeLOPages(st.tables, kvs)
}

// loPage is one stored page.
type loPage struct {
	key  keys.Key
	no   int64
	data string
}

func decodeLOPages(t *loTables, kvs []kvpb.KeyValue) ([]loPage, error) {
	out := make([]loPage, 0, len(kvs))
	for _, kv := range kvs {
		pk, err := rowenc.DecodePK(t.pages, kv.Key)
		if err != nil {
			return nil, err
		}
		row, err := rowenc.DecodeValue(t.pages, kv.Value)
		if err != nil {
			return nil, err
		}
		out = append(out, loPage{key: kv.Key, no: pk[1].I, data: row[loPageColData].S})
	}
	return out, nil
}

// loSize is where the object's last page ends.
func (s *Session) loSize(ctx context.Context, st *loState, obj *loObject) (int64, error) {
	if obj.sizeKnown {
		return obj.size, nil
	}
	prefix := pagePrefix(st.tables, obj.cid)
	kvs, err := st.txn.ReverseScan(ctx, prefix, prefix.PrefixEnd(), 1)
	if err != nil {
		return 0, err
	}
	pages, err := decodeLOPages(st.tables, kvs)
	if err != nil {
		return 0, err
	}
	obj.size, obj.sizeKnown = 0, true
	if len(pages) == 1 {
		obj.size = pages[0].no*LargeObjectPageSize + int64(len(pages[0].data))
	}
	return obj.size, nil
}

// pagePrefix is the key prefix of every page of one content.
func pagePrefix(t *loTables, cid int64) keys.Key {
	k, _ := rowenc.AppendKeyDatum(rowenc.PrimaryKeyPrefixFor(t.pages), types.Int, types.NewInt(cid))
	return k
}

// loFlush writes the descriptor's buffered page if it has changed.
func (s *Session) loFlush(ctx context.Context, st *loState, d *loDesc) error {
	if !d.dirty {
		return nil
	}
	if err := s.loPutPage(ctx, st, d.obj, d.page, d.buf); err != nil {
		return err
	}
	d.dirty = false
	return nil
}

// loSettle writes out every buffered page of d's object and makes the
// other descriptors on it forget theirs, so that what d is about to
// read or change is what is stored.
func (s *Session) loSettle(ctx context.Context, st *loState, d *loDesc) error {
	for _, o := range st.fds {
		if o.obj != d.obj {
			continue
		}
		if err := s.loFlush(ctx, st, o); err != nil {
			return err
		}
		if o != d {
			o.page, o.buf = -1, o.buf[:0]
		}
	}
	return nil
}

// loLoad makes page the descriptor's buffered page. whole says the
// caller is about to overwrite all of it, so what is stored need not be
// read.
func (s *Session) loLoad(ctx context.Context, st *loState, d *loDesc, page int64, whole bool) error {
	if d.page == page {
		return nil
	}
	if err := s.loFlush(ctx, st, d); err != nil {
		return err
	}
	d.page, d.buf = page, d.buf[:0]
	if whole {
		return nil
	}
	size, err := s.loSize(ctx, st, d.obj)
	if err != nil {
		return err
	}
	if page*LargeObjectPageSize >= size {
		return nil // past the end: nothing stored
	}
	pages, err := s.loScanPages(ctx, st, d.obj, page, page, 1)
	if err != nil {
		return err
	}
	if len(pages) == 1 {
		d.buf = append(d.buf, pages[0].data...)
	}
	return nil
}

func (s *Session) loWrite(ctx context.Context, st *loState, d *loDesc, data []byte) error {
	if !d.write {
		return newErrf(CodeObjectNotInState, "large object descriptor was not opened for writing")
	}
	if d.pos+int64(len(data)) > LargeObjectMaxSize {
		return newErrf(CodeProgramLimitExceeded, "a large object may hold at most %s", formatMemory(LargeObjectMaxSize))
	}
	if err := s.loSettleOthers(ctx, st, d); err != nil {
		return err
	}
	for len(data) > 0 {
		page, off := d.pos/LargeObjectPageSize, int(d.pos%LargeObjectPageSize)
		n := LargeObjectPageSize - off
		if n > len(data) {
			n = len(data)
		}
		if err := s.loLoad(ctx, st, d, page, off == 0 && n == LargeObjectPageSize); err != nil {
			return err
		}
		if len(d.buf) < off+n {
			// Anything between the stored end of the page and the write
			// is a hole, which reads as zeros.
			d.buf = append(d.buf, make([]byte, off+n-len(d.buf))...)
		}
		copy(d.buf[off:], data[:n])
		d.dirty = true
		d.pos += int64(n)
		data = data[n:]
		if d.obj.sizeKnown && d.pos > d.obj.size {
			d.obj.size = d.pos
		}
	}
	return nil
}

// loSettleOthers is loSettle for a write through d: d keeps its page.
func (s *Session) loSettleOthers(ctx context.Context, st *loState, d *loDesc) error {
	for _, o := range st.fds {
		if o == d || o.obj != d.obj {
			continue
		}
		if err := s.loFlush(ctx, st, o); err != nil {
			return err
		}
		o.page, o.buf = -1, o.buf[:0]
	}
	// The size must be known before the first buffered write moves it.
	_, err := s.loSize(ctx, st, d.obj)
	return err
}

func (s *Session) loRead(ctx context.Context, st *loState, d *loDesc, n int64) ([]byte, error) {
	if n < 0 {
		return nil, newErrf(CodeInvalidParameterValue, "requested length cannot be negative")
	}
	if err := s.loSettle(ctx, st, d); err != nil {
		return nil, err
	}
	size, err := s.loSize(ctx, st, d.obj)
	if err != nil {
		return nil, err
	}
	if d.pos >= size || n == 0 {
		return []byte{}, nil
	}
	if n > size-d.pos {
		n = size - d.pos
	}
	// The bytes are held twice while they are assembled: as stored pages
	// and as the result.
	if err := s.charge(2 * n); err != nil {
		return nil, err
	}
	out := make([]byte, n)
	first, last := d.pos/LargeObjectPageSize, (d.pos+n-1)/LargeObjectPageSize
	pages, err := s.loScanPages(ctx, st, d.obj, first, last, 0)
	if err != nil {
		return nil, err
	}
	for _, p := range pages {
		// The page covers [pstart, pstart+len); the result covers
		// [d.pos, d.pos+n). Copy the overlap; the rest stays zero.
		pstart := p.no * LargeObjectPageSize
		from, to := d.pos, d.pos+n
		if pstart > from {
			from = pstart
		}
		if pend := pstart + int64(len(p.data)); pend < to {
			to = pend
		}
		if from < to {
			copy(out[from-d.pos:], p.data[from-pstart:to-pstart])
		}
	}
	d.pos += n
	metrics.LargeObjectBytesRead.Add(float64(n))
	return out, nil
}

// loTruncate makes the object exactly size bytes long: pages past the
// end are deleted and the page holding the last byte is cut, or padded
// with zeros, to end there.
func (s *Session) loTruncate(ctx context.Context, st *loState, d *loDesc, size int64) error {
	if !d.write {
		return newErrf(CodeObjectNotInState, "large object descriptor was not opened for writing")
	}
	if size < 0 {
		return newErrf(CodeInvalidParameterValue, "a large object cannot be truncated to a negative length")
	}
	if size > LargeObjectMaxSize {
		return newErrf(CodeProgramLimitExceeded, "a large object may hold at most %s", formatMemory(LargeObjectMaxSize))
	}
	if err := s.loSettle(ctx, st, d); err != nil {
		return err
	}
	cur, err := s.loSize(ctx, st, d.obj)
	if err != nil {
		return err
	}
	d.page, d.buf = -1, d.buf[:0]
	lastPage := int64(-1)
	if size > 0 {
		lastPage = (size - 1) / LargeObjectPageSize
	}
	if curLast := (cur - 1) / LargeObjectPageSize; cur > 0 && curLast > lastPage {
		// A few pages a round: a scan returns the pages themselves.
		for from := lastPage + 1; ; {
			pages, err := s.loScanPages(ctx, st, d.obj, from, curLast, 8)
			if err != nil {
				return err
			}
			if len(pages) == 0 {
				break
			}
			ks := make([]keys.Key, len(pages))
			for i, p := range pages {
				ks[i] = p.key
			}
			if err := s.loDeleteKeys(ctx, st, d.obj, ks); err != nil {
				return err
			}
			from = pages[len(pages)-1].no + 1
		}
	}
	d.obj.size = size
	if size > 0 {
		want := int((size-1)%LargeObjectPageSize) + 1
		var have []byte
		if lastPage*LargeObjectPageSize < cur {
			pages, err := s.loScanPages(ctx, st, d.obj, lastPage, lastPage, 1)
			if err != nil {
				return err
			}
			if len(pages) == 1 {
				have = []byte(pages[0].data)
			}
		}
		if len(have) != want || lastPage*LargeObjectPageSize >= cur {
			if len(have) > want {
				have = have[:want]
			} else {
				have = append(have, make([]byte, want-len(have))...)
			}
			if err := s.loPutPage(ctx, st, d.obj, lastPage, have); err != nil {
				return err
			}
		}
	}
	return nil
}

// loUnlink removes the object: the metadata row goes with the
// transaction, and the content is handed to the sweeper by a pending
// row written with it.
func (s *Session) loUnlink(ctx context.Context, txn *kvclient.Txn, st *loState, loid int64) error {
	if obj, mine := st.created[loid]; mine {
		// Never visible to anyone: it simply stops being an upload.
		for fd, d := range st.fds {
			if d.obj == obj {
				delete(st.fds, fd)
			}
		}
		delete(st.created, loid)
		delete(st.objs, obj.cid)
		metrics.LargeObjectsUnlinked.Inc()
		return s.loAbandon(ctx, st.tables, obj)
	}
	m, err := s.loReadMeta(ctx, txn, st, loid, false)
	if err != nil {
		return err
	}
	if m == nil {
		return loErrNoObject(loid)
	}
	if err := s.loCheckAccess(ctx, txn, loid, m); err != nil {
		return err
	}
	for fd, d := range st.fds {
		if d.obj.cid == m.cid {
			delete(st.fds, fd)
		}
	}
	mk, err := st.tables.metaKey(loid)
	if err != nil {
		return err
	}
	pk, err := st.tables.pendingKey(m.cid)
	if err != nil {
		return err
	}
	pv, err := st.tables.pendingValue(loid, loPendingUnlink, int64(s.db.LocalNodeID()), s.db.Clock().Now().WallTime)
	if err != nil {
		return err
	}
	var wb kvclient.WriteBatch
	wb.Delete(mk)
	wb.Put(pk, pv)
	if err := txn.RunBatch(ctx, &wb); err != nil {
		return err
	}
	metrics.LargeObjectsUnlinked.Inc()
	return nil
}

// listLargeObjects lists the objects the session's role may open, for
// pg_largeobject_metadata. A cluster that has never stored one has no
// tables to read, and none are made for the asking.
func (s *Session) listLargeObjects(ctx context.Context, txn *kvclient.Txn, set catalog.RoleSet) ([]vtable.LargeObjectInfo, error) {
	if s.db.ClusterVersion() < version.V21 {
		return nil, nil
	}
	t, err := lookupLargeObjectTables(ctx, s.db, s.cat)
	if err != nil || t == nil {
		return nil, err
	}
	lo, hi := rowenc.PrimarySpanFor(t.meta)
	kvs, err := txn.Scan(ctx, lo, hi, 0)
	if err != nil {
		return nil, err
	}
	all := s.actor() == catalog.RootRole || set.IsAdmin()
	var out []vtable.LargeObjectInfo
	for _, kv := range kvs {
		pk, err := rowenc.DecodePK(t.meta, kv.Key)
		if err != nil {
			return nil, err
		}
		row, err := rowenc.DecodeValue(t.meta, kv.Value)
		if err != nil {
			return nil, err
		}
		if owner := row[loMetaColOwner].S; all || set.Has(catalog.OwnerOf(owner)) {
			out = append(out, vtable.LargeObjectInfo{OID: pk[0].I, Owner: owner})
		}
	}
	return out, nil
}

// evalLargeObjectFunc evaluates one large-object function call.
func (s *Session) evalLargeObjectFunc(ctx context.Context, txn *kvclient.Txn, name string, args []types.Datum) (types.Datum, error) {
	for _, a := range args {
		if a.Null {
			return types.DNull, nil // the functions are strict
		}
	}
	intArg := func(i int) (int64, error) {
		d, err := args[i].Coerce(types.Int)
		if err != nil {
			return 0, newErrf(CodeInvalidTextRepresentation, "%s: argument %d: %v", name, i+1, err)
		}
		return d.I, nil
	}
	bytesArg := func(i int) ([]byte, error) {
		d, err := args[i].Coerce(types.Bytes)
		if err != nil {
			return nil, newErrf(CodeInvalidTextRepresentation, "%s: argument %d: %v", name, i+1, err)
		}
		return []byte(d.S), nil
	}
	want := func(min, max int) error {
		if len(args) < min || len(args) > max {
			return newErrf(CodeSyntaxError, "%s() takes %d to %d arguments", name, min, max)
		}
		return nil
	}
	if largeObjectWrites[name] {
		// The statement is a SELECT, which a read-only transaction lets
		// through; what it calls is not a read.
		if s.readOnly() {
			return types.Datum{}, newErrf(CodeReadOnlyTransaction, "cannot execute %s() in a read-only transaction", name)
		}
	}
	st, err := s.loBegin(ctx, txn)
	if err != nil {
		return types.Datum{}, err
	}
	explicit := s.loExplicit(txn)
	for _, obj := range st.created {
		// Any call in the block shows the uploads in it are alive.
		if err := s.loBeat(ctx, st, obj); err != nil {
			return types.Datum{}, err
		}
	}
	if !explicit {
		// A statement that is its own transaction: nothing of it outlives
		// the call, so whatever it buffered is written before it returns.
		defer func() { st.txn = nil }()
	}
	desc := func(i int) (*loDesc, error) {
		fd, err := intArg(i)
		if err != nil {
			return nil, err
		}
		d, ok := st.fds[int32(fd)]
		if !ok || fd != int64(int32(fd)) {
			return nil, loErrNoDescriptor(fd)
		}
		return d, nil
	}
	// oneShot opens a descriptor for a function that works on an object
	// id directly, and writes out what it buffered.
	oneShot := func(loid int64, write bool, fn func(d *loDesc) error) error {
		obj, err := s.loLookup(ctx, txn, st, loid, write)
		if err != nil {
			return err
		}
		d := &loDesc{obj: obj, write: write, page: -1}
		st.fds[-1] = d
		defer delete(st.fds, -1)
		if err := fn(d); err != nil {
			return err
		}
		return s.loFlush(ctx, st, d)
	}

	switch name {
	case "lo_create", "lo_creat":
		if err := want(1, 1); err != nil {
			return types.Datum{}, err
		}
		loid := int64(0)
		if name == "lo_create" {
			if loid, err = intArg(0); err != nil {
				return types.Datum{}, err
			}
		}
		obj, err := s.loCreate(ctx, txn, st, loid)
		if err != nil {
			return types.Datum{}, err
		}
		return types.NewInt(obj.loid), nil

	case "lo_from_bytea":
		if err := want(2, 2); err != nil {
			return types.Datum{}, err
		}
		loid, err := intArg(0)
		if err != nil {
			return types.Datum{}, err
		}
		data, err := bytesArg(1)
		if err != nil {
			return types.Datum{}, err
		}
		obj, err := s.loCreate(ctx, txn, st, loid)
		if err != nil {
			return types.Datum{}, err
		}
		d := &loDesc{obj: obj, write: true, page: -1}
		if err := s.loWrite(ctx, st, d, data); err != nil {
			return types.Datum{}, err
		}
		if err := s.loFlush(ctx, st, d); err != nil {
			return types.Datum{}, err
		}
		return types.NewInt(obj.loid), nil

	case "lo_open":
		if err := want(2, 2); err != nil {
			return types.Datum{}, err
		}
		loid, err := intArg(0)
		if err != nil {
			return types.Datum{}, err
		}
		mode, err := intArg(1)
		if err != nil {
			return types.Datum{}, err
		}
		write := mode&loModeWrite != 0
		obj, err := s.loLookup(ctx, txn, st, loid, write)
		if err != nil {
			return types.Datum{}, err
		}
		return types.NewInt(int64(s.loOpen(st, obj, write))), nil

	case "lo_close":
		if err := want(1, 1); err != nil {
			return types.Datum{}, err
		}
		d, err := desc(0)
		if err != nil {
			return types.Datum{}, err
		}
		if err := s.loFlush(ctx, st, d); err != nil {
			return types.Datum{}, err
		}
		fd, _ := intArg(0)
		delete(st.fds, int32(fd))
		return types.NewInt(0), nil

	case "lowrite":
		if err := want(2, 2); err != nil {
			return types.Datum{}, err
		}
		d, err := desc(0)
		if err != nil {
			return types.Datum{}, err
		}
		data, err := bytesArg(1)
		if err != nil {
			return types.Datum{}, err
		}
		if err := s.loWrite(ctx, st, d, data); err != nil {
			return types.Datum{}, err
		}
		return types.NewInt(int64(len(data))), nil

	case "loread":
		if err := want(2, 2); err != nil {
			return types.Datum{}, err
		}
		d, err := desc(0)
		if err != nil {
			return types.Datum{}, err
		}
		n, err := intArg(1)
		if err != nil {
			return types.Datum{}, err
		}
		out, err := s.loRead(ctx, st, d, n)
		if err != nil {
			return types.Datum{}, err
		}
		return types.NewBytes(out), nil

	case "lo_lseek", "lo_lseek64":
		if err := want(3, 3); err != nil {
			return types.Datum{}, err
		}
		d, err := desc(0)
		if err != nil {
			return types.Datum{}, err
		}
		off, err := intArg(1)
		if err != nil {
			return types.Datum{}, err
		}
		whence, err := intArg(2)
		if err != nil {
			return types.Datum{}, err
		}
		pos := off
		switch whence {
		case 0: // SEEK_SET
		case 1: // SEEK_CUR
			pos += d.pos
		case 2: // SEEK_END
			if err := s.loSettle(ctx, st, d); err != nil {
				return types.Datum{}, err
			}
			size, err := s.loSize(ctx, st, d.obj)
			if err != nil {
				return types.Datum{}, err
			}
			pos += size
		default:
			return types.Datum{}, newErrf(CodeInvalidParameterValue, "invalid whence setting: %d", whence)
		}
		if pos < 0 || pos > LargeObjectMaxSize {
			return types.Datum{}, newErrf(CodeInvalidParameterValue, "invalid large object seek target: %d", pos)
		}
		d.pos = pos
		return types.NewInt(pos), nil

	case "lo_tell", "lo_tell64":
		if err := want(1, 1); err != nil {
			return types.Datum{}, err
		}
		d, err := desc(0)
		if err != nil {
			return types.Datum{}, err
		}
		return types.NewInt(d.pos), nil

	case "lo_truncate", "lo_truncate64":
		if err := want(2, 2); err != nil {
			return types.Datum{}, err
		}
		d, err := desc(0)
		if err != nil {
			return types.Datum{}, err
		}
		size, err := intArg(1)
		if err != nil {
			return types.Datum{}, err
		}
		if err := s.loTruncate(ctx, st, d, size); err != nil {
			return types.Datum{}, err
		}
		return types.NewInt(0), nil

	case "lo_unlink":
		if err := want(1, 1); err != nil {
			return types.Datum{}, err
		}
		loid, err := intArg(0)
		if err != nil {
			return types.Datum{}, err
		}
		if err := s.loUnlink(ctx, txn, st, loid); err != nil {
			return types.Datum{}, err
		}
		return types.NewInt(1), nil

	case "lo_get":
		if len(args) != 1 && len(args) != 3 {
			return types.Datum{}, newErrf(CodeSyntaxError, "lo_get() takes an object id, or an object id, an offset and a length")
		}
		loid, err := intArg(0)
		if err != nil {
			return types.Datum{}, err
		}
		off, n := int64(0), LargeObjectMaxSize
		if len(args) == 3 {
			if off, err = intArg(1); err != nil {
				return types.Datum{}, err
			}
			if n, err = intArg(2); err != nil {
				return types.Datum{}, err
			}
			if off < 0 {
				return types.Datum{}, newErrf(CodeInvalidParameterValue, "offset cannot be negative")
			}
		}
		var out []byte
		if err := oneShot(loid, false, func(d *loDesc) error {
			d.pos = off
			var err error
			out, err = s.loRead(ctx, st, d, n)
			return err
		}); err != nil {
			return types.Datum{}, err
		}
		return types.NewBytes(out), nil

	case "lo_put":
		if err := want(3, 3); err != nil {
			return types.Datum{}, err
		}
		loid, err := intArg(0)
		if err != nil {
			return types.Datum{}, err
		}
		off, err := intArg(1)
		if err != nil {
			return types.Datum{}, err
		}
		data, err := bytesArg(2)
		if err != nil {
			return types.Datum{}, err
		}
		if off < 0 {
			return types.Datum{}, newErrf(CodeInvalidParameterValue, "offset cannot be negative")
		}
		if err := oneShot(loid, true, func(d *loDesc) error {
			d.pos = off
			return s.loWrite(ctx, st, d, data)
		}); err != nil {
			return types.Datum{}, err
		}
		return types.NewString(""), nil
	}
	return types.Datum{}, newErrf(CodeUndefinedFunction, "function %s does not exist", name)
}

// loBeforeCommit runs before the session's transaction block commits.
// Every buffered page is written. Then, for the objects the block
// created: the transaction is moved past the last page written outside
// it, so that whoever can see an object's metadata row can see all of
// its pages; and it writes each metadata row and deletes the pending
// row beside it, having checked — under a lock, so the sweeper cannot
// be halfway through the opposite decision — that the upload is still
// the row's.
func (s *Session) loBeforeCommit(ctx context.Context) error {
	st := s.lo
	if st == nil || st.txn != s.txn || s.txn == nil {
		return nil
	}
	if st.broken {
		return newErrf(CodeInFailedTransaction, "this transaction rolled back to a savepoint after streaming a new large object, which a savepoint cannot undo: it cannot commit")
	}
	for _, d := range st.fds {
		if err := s.loFlush(ctx, st, d); err != nil {
			return err
		}
	}
	if len(st.created) == 0 {
		return nil
	}
	if err := s.txn.AdvanceTo(ctx, s.db.Clock().Now()); err != nil {
		return err
	}
	var wb kvclient.WriteBatch
	for loid, obj := range st.created {
		kind, err := loPendingKind(ctx, s.txn, st.tables, obj.cid, true)
		if err != nil {
			return err
		}
		if kind != loPendingUpload {
			return loErrReclaimed(loid)
		}
		// The id was free when the object was created; it must still be.
		if m, err := s.loReadMeta(ctx, s.txn, st, loid, false); err != nil {
			return err
		} else if m != nil {
			return newErrf(CodeDuplicateObject, "large object %d already exists", loid)
		}
		pk, err := st.tables.pendingKey(obj.cid)
		if err != nil {
			return err
		}
		mk, err := st.tables.metaKey(loid)
		if err != nil {
			return err
		}
		mv, err := st.tables.metaValue(obj)
		if err != nil {
			return err
		}
		wb.Delete(pk)
		wb.Put(mk, mv)
	}
	return s.txn.RunBatch(ctx, &wb)
}

// loEndTxn drops the transaction's descriptors. After anything but a
// commit, the contents it was uploading are orphans, and their pending
// rows are marked so the sweeper need not wait out the grace period.
func (s *Session) loEndTxn(ctx context.Context, committed bool) {
	st := s.lo
	if st == nil || st.txn == nil {
		return
	}
	created := st.created
	st.txn, st.fds, st.objs, st.created = nil, nil, nil, nil
	if committed || len(created) == 0 {
		return
	}
	// The session may be going away with its context; this is a few
	// small writes, worth a moment of their own.
	ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
	defer cancel()
	for _, obj := range created {
		if err := s.loAbandon(ctx, st.tables, obj); err != nil {
			log.Debugf("large object %d: abandoned upload not marked (the sweeper reclaims it after its grace period): %v", obj.loid, err)
		}
	}
}

// loRollbackToSavepoint: a savepoint restores the transaction's own
// writes, which covers every change to an existing object; descriptors
// are simply closed. Pages written outside the transaction are not
// restored, so a transaction that has any can only roll back.
func (s *Session) loRollbackToSavepoint() {
	st := s.lo
	if st == nil || st.txn != s.txn || s.txn == nil {
		return
	}
	if len(st.created) > 0 {
		st.broken = true
	}
	st.fds = map[int32]*loDesc{}
	for cid, obj := range st.objs {
		if !obj.staged {
			delete(st.objs, cid)
		}
	}
}

// deleteKeysNow deletes keys outside any transaction, in one batch.
func deleteKeysNow(ctx context.Context, db *kvclient.DB, ks []keys.Key) error {
	ba := &kvpb.BatchRequest{Header: kvpb.BatchHeader{Timestamp: db.Clock().Now()}}
	for _, k := range ks {
		ba.Add(&kvpb.DeleteRequest{RequestHeader: kvpb.RequestHeader{Key: k}})
	}
	if _, kerr := db.Send(ctx, ba); kerr != nil {
		return kerr
	}
	return nil
}

// DefaultLargeObjectUploadGrace is how long an upload's pending row
// may go without a heartbeat before the sweeper takes the upload for
// abandoned — in effect, how long a client may leave an upload idle.
const DefaultLargeObjectUploadGrace = 10 * time.Minute

// LargeObjectSweep is what one pass of the sweeper did.
type LargeObjectSweep struct {
	// Contents is the number of orphaned contents reclaimed, Pages the
	// pages deleted for them.
	Contents, Pages int
}

// SweepLargeObjects reclaims the pages of contents no object stands
// for: the contents of unlinked objects, uploads that ended without
// committing, and uploads that have shown no sign of life for
// uploadGrace (their gateway died, or their client went away without
// closing its connection).
//
// A pending row of kind unlink is an orphan by construction. One of
// kind upload becomes one when the sweeper rewrites it as unlink, which
// it does in a transaction that reads the row first: the upload's
// commit reads and deletes the same row under a lock, so exactly one of
// the two wins, and an upload that lost finds out before it commits.
func SweepLargeObjects(ctx context.Context, db *kvclient.DB, cat *catalog.Accessor, uploadGrace time.Duration) (LargeObjectSweep, error) {
	var res LargeObjectSweep
	if db.ClusterVersion() < version.V21 {
		return res, nil
	}
	t, err := lookupLargeObjectTables(ctx, db, cat)
	if err != nil || t == nil {
		return res, err
	}
	lo, hi := rowenc.PrimarySpanFor(t.pending)
	var rows []kvpb.KeyValue
	if err := db.RunTxn(ctx, "lo-sweep-scan", func(ctx context.Context, txn *kvclient.Txn) error {
		// A row locked right now belongs to a commit or an unlink in
		// progress: the next pass sees how that came out.
		var err error
		rows, err = txn.Scan(kvclient.WithLockWait(ctx, kvclient.LockWaitSkip), lo, hi, 1000)
		return err
	}); err != nil {
		return res, err
	}
	now := db.Clock().Now().WallTime
	for _, kv := range rows {
		pk, err := rowenc.DecodePK(t.pending, kv.Key)
		if err != nil {
			return res, err
		}
		row, err := rowenc.DecodeValue(t.pending, kv.Value)
		if err != nil {
			return res, err
		}
		cid := pk[0].I
		if row[loPendingColKind].S == loPendingUpload {
			if now-row[loPendingColAt].I < int64(uploadGrace) {
				continue
			}
			claimed := false
			if err := db.RunTxn(ctx, "lo-sweep-claim", func(ctx context.Context, txn *kvclient.Txn) error {
				claimed = false
				raw, err := txn.Get(ctx, kv.Key)
				if err != nil || raw == nil {
					return err
				}
				cur, err := rowenc.DecodeValue(t.pending, raw)
				if err != nil {
					return err
				}
				if cur[loPendingColKind].S != loPendingUpload || cur[loPendingColAt].I != row[loPendingColAt].I {
					return nil // unlinked, or alive after all
				}
				pv, err := t.pendingValue(cur[loPendingColLoid].I, loPendingUnlink, cur[loPendingColNode].I, now)
				if err != nil {
					return err
				}
				claimed = true
				return txn.Put(ctx, kv.Key, pv)
			}); err != nil {
				return res, err
			}
			if !claimed {
				continue
			}
		}
		start := pagePrefix(t, cid)
		end := start.PrefixEnd()
		for {
			// The scan returns the pages themselves, so a few at a time.
			kvs, err := db.Scan(ctx, start, end, 16)
			if err != nil {
				return res, err
			}
			if len(kvs) == 0 {
				break
			}
			ks := make([]keys.Key, len(kvs))
			for i, page := range kvs {
				ks[i] = page.Key
			}
			if err := deleteKeysNow(ctx, db, ks); err != nil {
				return res, err
			}
			res.Pages += len(ks)
		}
		// Pages first, the row last: a pass that stops halfway is
		// finished by the next.
		if err := db.Delete(ctx, kv.Key); err != nil {
			return res, err
		}
		res.Contents++
	}
	metrics.LargeObjectContentsReclaimed.Add(float64(res.Contents))
	metrics.LargeObjectPagesReclaimed.Add(float64(res.Pages))
	return res, nil
}

// loSetOwner is ALTER LARGE OBJECT loid OWNER TO owner (what pg_dump
// writes after each lo_create): the object's owner, or an admin, gives
// it to another role.
func (s *Session) loSetOwner(ctx context.Context, txn *kvclient.Txn, loid int64, owner string) error {
	st, err := s.loBegin(ctx, txn)
	if err != nil {
		return err
	}
	if obj, mine := st.created[loid]; mine {
		// Created in this transaction block: its metadata row is written
		// at commit, with the owner it has then.
		if err := s.checkOwner(ctx, txn, "large object", fmt.Sprint(loid), obj.owner); err != nil {
			return err
		}
		obj.owner = owner
		return nil
	}
	key, err := st.tables.metaKey(loid)
	if err != nil {
		return err
	}
	raw, err := txn.GetForUpdate(ctx, key)
	if err != nil {
		return err
	}
	if raw == nil {
		return loErrNoObject(loid)
	}
	row, err := rowenc.DecodeValue(st.tables.meta, raw)
	if err != nil {
		return err
	}
	if err := s.checkOwner(ctx, txn, "large object", fmt.Sprint(loid), row[loMetaColOwner].S); err != nil {
		return err
	}
	if obj := st.objs[row[loMetaColCID].I]; obj != nil {
		obj.owner = owner
	}
	row[loMetaColOwner] = types.NewString(owner)
	val, err := rowenc.EncodeValue(st.tables.meta, row)
	if err != nil {
		return err
	}
	return txn.Put(ctx, key, val)
}
