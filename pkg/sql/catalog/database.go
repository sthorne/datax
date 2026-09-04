package catalog

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"

	"github.com/sthorne/datax/pkg/keys"
	"github.com/sthorne/datax/pkg/kvclient"
)

// Databases (issue #88). A database descriptor lives at
// /system/db/<dbID>, its name at /system/dbns/<name> → dbID, and its
// tables' names at /system/nsdb/<dbID>/<name> → tableID. Table IDs stay
// global and row keys stay /t/<tableID>/..., so a table's database is a
// catalog fact only; nothing moves. A cluster bootstrapped, or finalized,
// at v6 carries the default database "datax" (the URL every client has
// been using) and a reserved, empty "system" database. Before v6 there
// are no database descriptors: every table lives in the flat namespace
// (keys.NamespaceKey) and reads as the default database's, so a v5 node
// and a v6 node agree on every name until finalize migrates the layout.

const (
	// DefaultDatabase is the database a session starts in and the one
	// pre-v6 tables belong to.
	DefaultDatabase = "datax"
	// SystemDatabase is reserved for the cluster (catalog views later);
	// it cannot be dropped, renamed, or used as a home for tables.
	SystemDatabase = "system"
	// PublicSchema is the only schema; qualified names may spell it.
	PublicSchema = "public"
)

// Database privileges.
const (
	PrivCreate  = "CREATE"
	PrivConnect = "CONNECT"
)

// DatabaseDescriptor describes a database.
type DatabaseDescriptor struct {
	ID    uint64 `json:"id"`
	Name  string `json:"name"`
	Owner string `json:"owner,omitempty"`
	// Privileges maps a user to its database privileges (CREATE, CONNECT).
	Privileges map[string][]string `json:"privileges,omitempty"`
	// ConnectRestricted is set by REVOKE CONNECT ... FROM PUBLIC: then only
	// admins and users holding CONNECT may open a session on the database.
	// PostgreSQL grants CONNECT to PUBLIC on every new database; so does
	// this.
	ConnectRestricted bool `json:"connect_restricted,omitempty"`
}

// HasPrivilege reports whether user holds priv on the database.
func (d *DatabaseDescriptor) HasPrivilege(user, priv string) bool {
	for _, p := range d.Privileges[user] {
		if p == priv {
			return true
		}
	}
	return false
}

// ErrDatabaseNotFound is returned for an unknown database (SQLSTATE 3D000).
type ErrDatabaseNotFound struct{ Name string }

func (e *ErrDatabaseNotFound) Error() string {
	return fmt.Sprintf("database %q does not exist", e.Name)
}

// ErrDatabaseExists is returned when a database name is taken (42P04).
type ErrDatabaseExists struct{ Name string }

func (e *ErrDatabaseExists) Error() string { return fmt.Sprintf("database %q already exists", e.Name) }

// SplitTableName splits "db.name" into its parts; a bare name yields an
// empty database (the session's current one).
func SplitTableName(qualified string) (db, name string) {
	if i := strings.LastIndexByte(qualified, '.'); i > 0 {
		return qualified[:i], qualified[i+1:]
	}
	return "", qualified
}

// LookupDatabase resolves a database by name within txn. A missing
// descriptor for the default database before the v6 migration is not an
// error: it resolves to ID 0, the flat namespace.
func LookupDatabase(ctx context.Context, txn *kvclient.Txn, name string) (*DatabaseDescriptor, error) {
	idRaw, err := txn.Get(ctx, keys.DatabaseNamespaceKey(name))
	if err != nil {
		return nil, err
	}
	if idRaw == nil {
		if name == DefaultDatabase {
			return &DatabaseDescriptor{ID: 0, Name: DefaultDatabase}, nil
		}
		return nil, &ErrDatabaseNotFound{Name: name}
	}
	id, err := strconv.ParseUint(string(idRaw), 10, 64)
	if err != nil {
		return nil, fmt.Errorf("corrupt database namespace entry for %q", name)
	}
	return readDatabase(ctx, txn, id, name)
}

func readDatabase(ctx context.Context, txn *kvclient.Txn, id uint64, name string) (*DatabaseDescriptor, error) {
	raw, err := txn.Get(ctx, keys.DatabaseDescKey(id))
	if err != nil {
		return nil, err
	}
	if raw == nil {
		return nil, fmt.Errorf("database namespace entry for %q points at missing descriptor %d", name, id)
	}
	var d DatabaseDescriptor
	if err := json.Unmarshal(raw, &d); err != nil {
		return nil, fmt.Errorf("corrupt database descriptor %d: %w", id, err)
	}
	return &d, nil
}

// ListDatabases returns every database descriptor, by name. Before the
// migration there are none; the default database still exists logically,
// so it is synthesized (ID 0) when absent.
func ListDatabases(ctx context.Context, txn *kvclient.Txn) ([]*DatabaseDescriptor, error) {
	start, end := keys.DatabaseDescSpan()
	rows, err := txn.Scan(ctx, start, end, 0)
	if err != nil {
		return nil, err
	}
	var out []*DatabaseDescriptor
	haveDefault := false
	for _, kv := range rows {
		var d DatabaseDescriptor
		if err := json.Unmarshal(kv.Value, &d); err != nil {
			continue
		}
		if d.Name == DefaultDatabase {
			haveDefault = true
		}
		out = append(out, &d)
	}
	if !haveDefault {
		out = append(out, &DatabaseDescriptor{ID: 0, Name: DefaultDatabase})
	}
	sortDatabases(out)
	return out, nil
}

func sortDatabases(ds []*DatabaseDescriptor) {
	for i := 1; i < len(ds); i++ {
		for j := i; j > 0 && ds[j].Name < ds[j-1].Name; j-- {
			ds[j], ds[j-1] = ds[j-1], ds[j]
		}
	}
}

// CreateDatabase writes a new database within txn (the caller has
// checked that the cluster is at v6 and the name is allowed).
func (a *Accessor) CreateDatabase(ctx context.Context, txn *kvclient.Txn, d *DatabaseDescriptor) error {
	existing, err := txn.Get(ctx, keys.DatabaseNamespaceKey(d.Name))
	if err != nil {
		return err
	}
	if existing != nil {
		return &ErrDatabaseExists{Name: d.Name}
	}
	id, err := txn.Increment(ctx, keys.DescIDGenKey(), 1)
	if err != nil {
		return err
	}
	d.ID = uint64(id)
	if err := a.putDatabase(ctx, txn, d); err != nil {
		return err
	}
	return txn.Put(ctx, keys.DatabaseNamespaceKey(d.Name), []byte(strconv.FormatUint(d.ID, 10)))
}

// UpdateDatabase rewrites a database descriptor (privileges).
func (a *Accessor) UpdateDatabase(ctx context.Context, txn *kvclient.Txn, d *DatabaseDescriptor) error {
	return a.putDatabase(ctx, txn, d)
}

func (a *Accessor) putDatabase(ctx context.Context, txn *kvclient.Txn, d *DatabaseDescriptor) error {
	raw, err := json.Marshal(d)
	if err != nil {
		return err
	}
	if err := txn.Put(ctx, keys.DatabaseDescKey(d.ID), raw); err != nil {
		return err
	}
	a.invalidateDatabase(d.Name)
	return nil
}

// RenameDatabase moves the name entry; the descriptor keeps its ID, so
// its tables' namespace entries are untouched.
func (a *Accessor) RenameDatabase(ctx context.Context, txn *kvclient.Txn, d *DatabaseDescriptor, newName string) error {
	existing, err := txn.Get(ctx, keys.DatabaseNamespaceKey(newName))
	if err != nil {
		return err
	}
	if existing != nil {
		return &ErrDatabaseExists{Name: newName}
	}
	oldName := d.Name
	d.Name = newName
	if err := a.putDatabase(ctx, txn, d); err != nil {
		return err
	}
	if err := txn.Delete(ctx, keys.DatabaseNamespaceKey(oldName)); err != nil {
		return err
	}
	a.invalidateDatabase(oldName)
	a.InvalidateAll()
	return txn.Put(ctx, keys.DatabaseNamespaceKey(newName), []byte(strconv.FormatUint(d.ID, 10)))
}

// DropDatabase removes an empty database's descriptor and name.
func (a *Accessor) DropDatabase(ctx context.Context, txn *kvclient.Txn, d *DatabaseDescriptor) error {
	if err := txn.Delete(ctx, keys.DatabaseNamespaceKey(d.Name)); err != nil {
		return err
	}
	if err := txn.Delete(ctx, keys.DatabaseDescKey(d.ID)); err != nil {
		return err
	}
	a.invalidateDatabase(d.Name)
	return nil
}

// ListIn returns the table descriptors of one database (ID 0 = the
// default database's pre-migration tables, which also belong to the
// default database once it has an ID).
func (a *Accessor) ListIn(ctx context.Context, txn *kvclient.Txn, db *DatabaseDescriptor) ([]*TableDescriptor, error) {
	all, err := a.List(ctx, txn)
	if err != nil {
		return nil, err
	}
	out := all[:0]
	for _, d := range all {
		if d.DatabaseID == db.ID || (d.DatabaseID == 0 && db.Name == DefaultDatabase) {
			out = append(out, d)
		}
	}
	return out, nil
}

// MigrateNamespace is the v6 catalog migration, idempotent and run in
// one transaction: create the default and system databases if missing,
// move every flat namespace entry under the default database, and stamp
// DatabaseID on the descriptors it points at. Safe to repeat: a second
// run finds nothing to move.
func MigrateNamespace(ctx context.Context, db *kvclient.DB) (moved int, err error) {
	err = db.RunTxn(ctx, "catalog-migrate-v6", func(ctx context.Context, txn *kvclient.Txn) error {
		moved = 0
		a := &Accessor{}
		def, err := ensureDatabase(ctx, txn, a, DefaultDatabase)
		if err != nil {
			return err
		}
		if _, err := ensureDatabase(ctx, txn, a, SystemDatabase); err != nil {
			return err
		}
		lo, hi := keys.NamespaceSpan()
		rows, err := txn.Scan(ctx, lo, hi, 0)
		if err != nil {
			return err
		}
		for _, kv := range rows {
			id, err := strconv.ParseUint(string(kv.Value), 10, 64)
			if err != nil {
				continue
			}
			raw, err := txn.Get(ctx, keys.TableDescKey(id))
			if err != nil {
				return err
			}
			if raw != nil {
				var d TableDescriptor
				if json.Unmarshal(raw, &d) == nil && d.DatabaseID == 0 {
					d.DatabaseID = def.ID
					out, err := json.Marshal(&d)
					if err != nil {
						return err
					}
					if err := txn.Put(ctx, keys.TableDescKey(id), out); err != nil {
						return err
					}
				}
				// The name lives in the flat key's suffix; the descriptor
				// carries it too, which is what we trust.
				if err := txn.Put(ctx, keys.TableNamespaceKey(def.ID, tableNameOf(raw)), kv.Value); err != nil {
					return err
				}
			}
			if err := txn.Delete(ctx, keys.Key(kv.Key)); err != nil {
				return err
			}
			moved++
		}
		return nil
	})
	return moved, err
}

func tableNameOf(raw []byte) string {
	var d TableDescriptor
	_ = json.Unmarshal(raw, &d)
	return d.Name
}

func ensureDatabase(ctx context.Context, txn *kvclient.Txn, a *Accessor, name string) (*DatabaseDescriptor, error) {
	idRaw, err := txn.Get(ctx, keys.DatabaseNamespaceKey(name))
	if err != nil {
		return nil, err
	}
	if idRaw != nil {
		id, err := strconv.ParseUint(string(idRaw), 10, 64)
		if err != nil {
			return nil, fmt.Errorf("corrupt database namespace entry for %q", name)
		}
		return readDatabase(ctx, txn, id, name)
	}
	d := &DatabaseDescriptor{Name: name, Owner: "root"}
	if err := a.CreateDatabase(ctx, txn, d); err != nil {
		return nil, err
	}
	return d, nil
}

// BootstrapDatabases seeds the database catalog of a brand-new cluster
// born at v6 or later: the default and system databases (IDs 1 and 2,
// exactly what MigrateNamespace would allocate) and the descriptor ID
// counter past them. Seeding at bootstrap means no table is ever created
// in the flat namespace only to be moved moments later by the leader's
// migration — a move that would surprise a transaction reading the
// table's descriptor at the same time (an uncertainty restart). put
// writes one key of the pre-Raft seed state.
func BootstrapDatabases(put func(key keys.Key, value []byte) error) error {
	for i, name := range []string{DefaultDatabase, SystemDatabase} {
		d := &DatabaseDescriptor{ID: uint64(i + 1), Name: name, Owner: "root"}
		raw, err := json.Marshal(d)
		if err != nil {
			return err
		}
		if err := put(keys.DatabaseDescKey(d.ID), raw); err != nil {
			return err
		}
		if err := put(keys.DatabaseNamespaceKey(name), []byte(strconv.FormatUint(d.ID, 10))); err != nil {
			return err
		}
	}
	return put(keys.DescIDGenKey(), []byte("2"))
}
