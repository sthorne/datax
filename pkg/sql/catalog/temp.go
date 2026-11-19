package catalog

import (
	"context"
	"errors"
	"strings"

	"github.com/sthorne/datax/pkg/kvclient"
)

// Temporary tables (gap PG-D-009).
//
// A temporary table is an ordinary table descriptor, with rows, indexes
// and constraints like any other, that carries a TempTable naming the
// session it belongs to. What makes it private is its name entry: it is
// not filed under its name, where every session resolves tables, but
// under TempNamespaceName(session, name), a name that contains NUL
// bytes. No SQL text can spell such a name, and LookupIn, LookupFreshIn,
// DropIn, Create and RenameTable refuse any name holding one
// (IsReservedTableName), so the entry is reached only through
// LookupTemp, which the owning session calls with its own session id.
// The descriptor's Name stays the table's plain name, so messages,
// pg_class and the names derived from it (t_pkey) read as in PostgreSQL.
//
// Temporary descriptors are never cached or leased: LookupTemp reads
// the descriptor in the caller's transaction every time. Nothing but
// their own session uses them, so there is no other gateway to drain.

// TempSchema is the qualifier that names the session's temporary
// tables (pg_temp.t), as PostgreSQL's alias for the session's temporary
// schema does.
const TempSchema = "pg_temp"

// On-commit actions of a temporary table (TempTable.OnCommit).
const (
	OnCommitPreserve = ""       // ON COMMIT PRESERVE ROWS, the default
	OnCommitDelete   = "delete" // ON COMMIT DELETE ROWS
	OnCommitDrop     = "drop"   // ON COMMIT DROP
)

// TempTable is what makes a table temporary.
type TempTable struct {
	// Session is the owning session's id (pkg/sql/temptable.go): the
	// node, the node process's instance and a sequence number, which is
	// how the orphan sweep tells a session that has ended.
	Session string `json:"session"`
	// OnCommit is the action at the end of each transaction.
	OnCommit string `json:"on_commit,omitempty"`
}

// tempMarker begins every temporary table's name entry.
const tempMarker = "\x00temp\x00"

// TempNamespaceName is the name entry of session's temporary table name.
func TempNamespaceName(session, name string) string {
	return tempMarker + session + "\x00" + name
}

// IsReservedTableName reports whether name holds a NUL byte, as only a
// temporary table's name entry does: no table may be created, renamed
// or resolved under such a name by SQL.
func IsReservedTableName(name string) bool { return strings.IndexByte(name, 0) >= 0 }

// namespaceName is where d's name entry lives within its database.
func (d *TableDescriptor) namespaceName() string {
	if d.Temp != nil {
		return TempNamespaceName(d.Temp.Session, d.Name)
	}
	return SchemaNamespaceName(d.SchemaID, d.Name)
}

// IsTemp reports whether d is a temporary table.
func (d *TableDescriptor) IsTemp() bool { return d != nil && d.Temp != nil }

// TempOf reports whether d is a temporary table of session.
func (d *TableDescriptor) TempOf(session string) bool {
	return d != nil && d.Temp != nil && session != "" && d.Temp.Session == session
}

// Persistence is pg_class.relpersistence: 't' for a temporary table, 'u'
// for an unlogged one, 'p' otherwise.
func (d *TableDescriptor) Persistence() string {
	switch {
	case d.Temp != nil:
		return "t"
	case d.Unlogged:
		return "u"
	}
	return "p"
}

// LookupTemp resolves session's temporary table name in database db,
// reading the name entry and the descriptor in txn (never cached).
func (a *Accessor) LookupTemp(ctx context.Context, txn *kvclient.Txn, db, session, name string) (*TableDescriptor, error) {
	if session == "" || IsReservedTableName(name) {
		return nil, &ErrTableNotFound{Name: name}
	}
	dbID, err := a.databaseID(ctx, txn, db)
	if err != nil {
		return nil, err
	}
	d, err := lookupUncached(ctx, txn, dbID, TempNamespaceName(session, name), false)
	if err != nil {
		var nf *ErrTableNotFound
		if errors.As(err, &nf) {
			return nil, &ErrTableNotFound{Name: name}
		}
		return nil, err
	}
	if !d.TempOf(session) {
		return nil, &ErrTableNotFound{Name: name}
	}
	return d, nil
}
