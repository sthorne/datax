package sql

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/sthorne/datax/pkg/keys"
	"github.com/sthorne/datax/pkg/kvclient"
	"github.com/sthorne/datax/pkg/sql/catalog"
	"github.com/sthorne/datax/pkg/sql/parser"
	"github.com/sthorne/datax/pkg/util/log"
)

// Temporary tables (gap PG-D-009): CREATE TEMP TABLE, ON COMMIT
// PRESERVE ROWS | DELETE ROWS | DROP, pg_temp.t, and their end with the
// session that made them.
//
// A temporary table is an ordinary table descriptor marked with the id
// of the session that owns it (catalog/temp.go). Its name entry is one
// that only that session can reach (catalog.LookupTemp), so another
// session, whatever name it writes, resolves its own table or none: it
// cannot read the rows. The owning session resolves an unqualified name
// to its temporary table first, then to an ordinary table, as
// PostgreSQL searches pg_temp before the rest of search_path; pg_temp.t
// names the temporary table only.
//
// The table goes when the session does: Close drops the session's
// temporary tables, and DISCARD TEMP and DISCARD ALL drop them too. A
// session that cannot drop them — its node stopped or crashed, or the
// drop failed — leaves them to the orphan sweep (SweepOrphanTempTables,
// run by every node, pkg/server/temp_sweep.go), which drops a temporary
// table whose session is no longer live. A session id names its node
// and the node process's instance: the node that made it knows whether
// the session is live, and a node that restarted, being a new instance,
// knows that none of its earlier sessions is; any node takes the tables
// of a node whose liveness record has lapsed for orphans.
//
// Listings (pg_class, information_schema, SHOW TABLES, regclass names,
// GRANT ... ON ALL TABLES) show a session its own temporary tables and
// no other session's.

// TempRegistry hands out the ids of a node's sessions that may own
// temporary tables and knows which of them are still live: the orphan
// sweep's view of this node. One per node (pgwire.ServerOptions); a
// session without one uses a process-wide registry.
type TempRegistry struct {
	node     int32
	instance string
	mu       sync.Mutex
	seq      uint64
	live     map[string]bool
}

// NewTempRegistry returns the registry of node's sessions, under a new
// instance id (a restarted node's earlier sessions are not its own).
func NewTempRegistry(node int32) *TempRegistry {
	var b [6]byte
	_, _ = rand.Read(b[:])
	return &TempRegistry{node: node, instance: hex.EncodeToString(b[:]), live: map[string]bool{}}
}

var defaultTempRegistry = NewTempRegistry(0)

// Node is the node the registry's sessions run on.
func (r *TempRegistry) Node() int32 { return r.node }

// Instance is the registry's instance id.
func (r *TempRegistry) Instance() string { return r.instance }

// acquire hands out a new live session id: node.instance.seq.
func (r *TempRegistry) acquire() string {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.seq++
	sid := fmt.Sprintf("%d.%s.%d", r.node, r.instance, r.seq)
	r.live[sid] = true
	return sid
}

// release marks a session id ended.
func (r *TempRegistry) release(sid string) {
	r.mu.Lock()
	delete(r.live, sid)
	r.mu.Unlock()
}

// Live reports whether sid is a session of this registry that has not
// ended.
func (r *TempRegistry) Live(sid string) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.live[sid]
}

// ParseTempSession splits a temporary table's session id into its node
// and instance.
func ParseTempSession(sid string) (node int32, instance string, ok bool) {
	parts := strings.Split(sid, ".")
	if len(parts) != 3 {
		return 0, "", false
	}
	n, err := strconv.ParseInt(parts[0], 10, 32)
	if err != nil {
		return 0, "", false
	}
	return int32(n), parts[1], true
}

// tempState is a session's temporary tables.
type tempState struct {
	reg *TempRegistry
	// sid is the session's id, taken when it first creates a temporary
	// table ("" until then).
	sid string
	// names are the names the session's temporary tables may have: a
	// superset (a rolled-back CREATE or a DROP leaves its name), which
	// spares a session the lookup of a temporary table it cannot have.
	names map[string]bool
	// ids are the temporary tables the session may have created, dropped
	// when it ends (each is checked to be the session's before).
	ids map[uint64]bool
	// touched are the temporary tables the current transaction resolved,
	// dropAtCommit those it created ON COMMIT DROP.
	touched      map[uint64]bool
	dropAtCommit map[uint64]bool
}

// SetTempRegistry gives the session its node's registry of session ids.
func (s *Session) SetTempRegistry(r *TempRegistry) { s.temp.reg = r }

// tempSession is the session's id, taken on first use.
func (s *Session) tempSession() string {
	if s.temp.sid == "" {
		if s.temp.reg == nil {
			s.temp.reg = defaultTempRegistry
		}
		s.temp.sid = s.temp.reg.acquire()
	}
	return s.temp.sid
}

// tempVisible reports whether a listing shows d to the session: every
// table but another session's temporary one.
func (s *Session) tempVisible(d *catalog.TableDescriptor) bool {
	return d.Temp == nil || d.TempOf(s.temp.sid)
}

// visibleTables drops the other sessions' temporary tables from a
// listing (in place).
func (s *Session) visibleTables(all []*catalog.TableDescriptor) []*catalog.TableDescriptor {
	out := all[:0]
	for _, d := range all {
		if s.tempVisible(d) {
			out = append(out, d)
		}
	}
	return out
}

// listIn is the tables of database db the session can see.
func (s *Session) listIn(ctx context.Context, txn *kvclient.Txn, db *catalog.DatabaseDescriptor) ([]*catalog.TableDescriptor, error) {
	all, err := s.cat.ListIn(ctx, txn, db)
	if err != nil {
		return nil, err
	}
	return s.visibleTables(all), nil
}

// listAll is the tables of every database the session can see.
func (s *Session) listAll(ctx context.Context, txn *kvclient.Txn) ([]*catalog.TableDescriptor, error) {
	all, err := s.cat.List(ctx, txn)
	if err != nil {
		return nil, err
	}
	return s.visibleTables(all), nil
}

// lookupTemp resolves name to one of the session's temporary tables: a
// pg_temp.x name always (ok, with a not-found error when there is no
// such table), a bare name when the session has a temporary table of
// that name. ok is false when name is for the ordinary lookup.
func (s *Session) lookupTemp(ctx context.Context, txn *kvclient.Txn, name string) (d *catalog.TableDescriptor, ok bool, err error) {
	db, bare := catalog.SplitTableName(name)
	must := db == catalog.TempSchema
	if !must && (db != "" || !s.temp.names[bare]) {
		return nil, false, nil
	}
	if s.temp.sid == "" {
		return nil, true, &catalog.ErrTableNotFound{Name: name}
	}
	d, err = s.cat.LookupTemp(ctx, txn, s.database, s.temp.sid, bare)
	if err != nil {
		var nf *catalog.ErrTableNotFound
		if errors.As(err, &nf) {
			if must {
				return nil, true, &catalog.ErrTableNotFound{Name: name}
			}
			return nil, false, nil
		}
		return nil, true, err
	}
	if s.temp.touched == nil {
		s.temp.touched = map[uint64]bool{}
	}
	s.temp.touched[d.ID] = true
	return d, true, nil
}

// namesTempTable reports whether name resolves to one of the session's
// temporary tables.
func (s *Session) namesTempTable(ctx context.Context, name string) bool {
	if s.temp.sid == "" {
		return false
	}
	found := false
	_ = s.db.RunTxn(ctx, "temp-check", func(ctx context.Context, txn *kvclient.Txn) error {
		d, ok, err := s.lookupTemp(ctx, txn, name)
		found = ok && err == nil && d != nil
		return nil
	})
	return found
}

// prepareTempCreate checks a CREATE TABLE's TEMP, UNLOGGED and ON
// COMMIT clauses and returns the statement with a pg_temp qualifier
// taken as TEMP (and removed), as PostgreSQL creates pg_temp.t as a
// temporary table.
func prepareTempCreate(t *parser.CreateTable) (*parser.CreateTable, error) {
	db, bare := catalog.SplitTableName(t.Name)
	if db == catalog.TempSchema {
		c := *t
		c.Name, c.Temp = bare, true
		if c.Unlogged {
			return nil, newErrf(CodeInvalidTableDefinition, "only temporary relations may be created in temporary schemas")
		}
		t = &c
	} else if t.Temp && db != "" {
		return nil, newErrf(CodeInvalidTableDefinition, "cannot create temporary relation in non-temporary schema")
	}
	if t.OnCommit != "" && !t.Temp {
		return nil, newErrf(CodeInvalidTableDefinition, "ON COMMIT can only be used on temporary tables")
	}
	if t.Temp && len(t.Options) > 0 {
		return nil, newErrf(CodeFeatureNotSupported, "WITH (...) storage options are not supported on temporary tables")
	}
	if t.Temp {
		for _, cd := range t.Columns {
			if cd.Serial || cd.Identity != "" {
				return nil, newErrf(CodeFeatureNotSupported, "SERIAL and identity columns are not supported on temporary tables (column %q)", cd.Name)
			}
		}
	}
	return t, nil
}

// tempOnCommit is the catalog's on-commit action for a CREATE TABLE.
func tempOnCommit(t *parser.CreateTable) string {
	switch t.OnCommit {
	case "delete":
		return catalog.OnCommitDelete
	case "drop":
		return catalog.OnCommitDrop
	}
	return catalog.OnCommitPreserve
}

// noteTempCreated records a temporary table the session just created.
func (s *Session) noteTempCreated(d *catalog.TableDescriptor) {
	if s.temp.names == nil {
		s.temp.names = map[string]bool{}
	}
	if s.temp.ids == nil {
		s.temp.ids = map[uint64]bool{}
	}
	s.temp.names[d.Name] = true
	s.temp.ids[d.ID] = true
	if d.Temp.OnCommit == catalog.OnCommitDrop {
		if s.temp.dropAtCommit == nil {
			s.temp.dropAtCommit = map[uint64]bool{}
		}
		s.temp.dropAtCommit[d.ID] = true
	}
}

// noteTempRenamed records a temporary table's new name.
func (s *Session) noteTempRenamed(d *catalog.TableDescriptor) {
	if d.Temp != nil && s.temp.names != nil {
		s.temp.names[d.Name] = true
	}
}

// tempEndTxn forgets the transaction's on-commit bookkeeping (the
// transaction committed, rolled back, or is starting over).
func (s *Session) tempEndTxn() {
	s.temp.touched, s.temp.dropAtCommit = nil, nil
}

// tempPreCommit runs the transaction's ON COMMIT actions in txn, just
// before it commits: the tables created ON COMMIT DROP are dropped, and
// the ON COMMIT DELETE ROWS tables it used are emptied.
func (s *Session) tempPreCommit(ctx context.Context, txn *kvclient.Txn) error {
	if len(s.temp.dropAtCommit) == 0 && len(s.temp.touched) == 0 {
		return nil
	}
	drops, touched := s.temp.dropAtCommit, s.temp.touched
	for id := range drops {
		d, err := catalog.ReadTable(ctx, txn, id)
		if err != nil {
			return err
		}
		if !d.TempOf(s.temp.sid) {
			continue // dropped already, or never committed
		}
		if err := s.dropTempTable(ctx, txn, d); err != nil {
			return err
		}
	}
	for id := range touched {
		if drops[id] {
			continue
		}
		d, err := catalog.ReadTable(ctx, txn, id)
		if err != nil {
			return err
		}
		if !d.TempOf(s.temp.sid) || d.Temp.OnCommit != catalog.OnCommitDelete {
			continue
		}
		if err := s.deleteTempRows(ctx, txn, d); err != nil {
			return err
		}
	}
	return nil
}

// deleteTempRows empties temporary table d in txn (ON COMMIT DELETE
// ROWS), as its owner.
func (s *Session) deleteTempRows(ctx context.Context, txn *kvclient.Txn, d *catalog.TableDescriptor) error {
	saved, savedDDL, savedDrops := s.privAs, s.extraDDL, s.extraDrops
	s.privAs = catalog.OwnerOf(d.Owner)
	defer func() { s.privAs, s.extraDDL, s.extraDrops = saved, savedDDL, savedDrops }()
	_, err := s.execStmt(ctx, txn, &parser.Delete{Table: catalog.TempSchema + "." + d.Name}, nil)
	return err
}

// dropTempTable drops temporary table d in txn, with the foreign keys of
// the session's other temporary tables that reference it: what DROP
// TABLE ... CASCADE does, without its privilege checks (the session
// owns its temporary tables, whatever role it has set since).
func (s *Session) dropTempTable(ctx context.Context, txn *kvclient.Txn, d *catalog.TableDescriptor) error {
	if err := s.dropTableConstraints(ctx, txn, d, true); err != nil {
		return err
	}
	var wb kvclient.WriteBatch
	if err := s.cat.DropDescriptor(txn, d, &wb); err != nil {
		return err
	}
	if err := txn.RunBatch(ctx, &wb); err != nil {
		return err
	}
	return s.finishTableDrop(ctx, txn, d)
}

// dropTempTables drops every temporary table of the session's, in its
// open transaction block if it is in one, or in a transaction of its
// own (DISCARD TEMP, DISCARD ALL, the end of the session).
func (s *Session) dropTempTables(ctx context.Context) error {
	if len(s.temp.ids) == 0 {
		return nil
	}
	drop := func(ctx context.Context, txn *kvclient.Txn) error {
		for id := range s.temp.ids {
			d, err := catalog.ReadTable(ctx, txn, id)
			if err != nil {
				return err
			}
			if !d.TempOf(s.temp.sid) {
				continue
			}
			if err := s.dropTempTable(ctx, txn, d); err != nil {
				return err
			}
		}
		return nil
	}
	if s.state == StateOpen {
		return drop(ctx, s.txn)
	}
	saved := s.extraDrops
	err := s.db.RunTxn(ctx, "temp-drop", func(ctx context.Context, txn *kvclient.Txn) error {
		s.extraDrops, s.pendingWipes = nil, nil
		return drop(ctx, txn)
	})
	s.extraDrops, s.pendingWipes = saved, nil
	if err != nil {
		return err
	}
	s.temp.ids = nil
	return nil
}

// TestingKeepTempTablesOnClose, when set, makes an ending session leave
// its temporary tables behind, as a session whose node stops does: a
// test's way to leave them to the orphan sweep.
var TestingKeepTempTablesOnClose atomic.Bool

// closeTemp drops the session's temporary tables as it ends and retires
// its id. A table the drop misses is the orphan sweep's.
func (s *Session) closeTemp() {
	if s.temp.sid == "" {
		return
	}
	if len(s.temp.ids) > 0 && !TestingKeepTempTablesOnClose.Load() {
		ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		if err := s.dropTempTables(ctx); err != nil {
			log.Warnf("dropping the temporary tables of an ending session: %v (the orphan sweep will)", err)
		}
		cancel()
	}
	s.temp.reg.release(s.temp.sid)
	s.temp = tempState{reg: s.temp.reg}
}

// SweepOrphanTempTables drops every temporary table whose session
// orphaned reports ended, each in a transaction of its own, and returns
// how many it dropped.
func SweepOrphanTempTables(ctx context.Context, db *kvclient.DB, cat *catalog.Accessor, orphaned func(sid string) bool) (int, error) {
	var orphans []uint64
	if err := db.RunTxn(ctx, "temp-sweep-list", func(ctx context.Context, txn *kvclient.Txn) error {
		orphans = orphans[:0]
		start, end := keys.TableDescSpan()
		rows, err := txn.Scan(ctx, start, end, 0)
		if err != nil {
			return err
		}
		for _, kv := range rows {
			// Only descriptors that mention a session can be temporary:
			// the rest are not decoded.
			if !strings.Contains(string(kv.Value), `"temp":{`) {
				continue
			}
			var d catalog.TableDescriptor
			if err := json.Unmarshal(kv.Value, &d); err != nil || d.Temp == nil || !orphaned(d.Temp.Session) {
				continue
			}
			orphans = append(orphans, d.ID)
		}
		return nil
	}); err != nil {
		return 0, err
	}
	dropped := 0
	for _, id := range orphans {
		s := NewSystemSession(db, cat)
		done := false
		err := db.RunTxn(ctx, "temp-sweep-drop", func(ctx context.Context, txn *kvclient.Txn) error {
			done = false
			s.extraDrops, s.pendingWipes = nil, nil
			d, err := catalog.ReadTable(ctx, txn, id)
			if err != nil || d == nil || d.Temp == nil || !orphaned(d.Temp.Session) {
				return err
			}
			s.temp.sid = d.Temp.Session
			done = true
			return s.dropTempTable(ctx, txn, d)
		})
		if err != nil {
			return dropped, err
		}
		if done {
			dropped++
			log.Infof("dropped orphaned temporary table %d of ended session %s", id, s.temp.sid)
		}
	}
	return dropped, nil
}
