package sql

import (
	"context"
	"fmt"
	"sort"

	"github.com/sthorne/datax/pkg/keys"
	"github.com/sthorne/datax/pkg/kvclient"
	"github.com/sthorne/datax/pkg/sql/catalog"
	"github.com/sthorne/datax/pkg/sql/parser"
	"github.com/sthorne/datax/pkg/sql/types"
	"github.com/sthorne/datax/pkg/util/log"
)

// Database statements (issue #88): CREATE / DROP / ALTER ... RENAME /
// SHOW DATABASES, USE and SET database, and GRANT ... ON DATABASE. The
// admin gate has run for the DDL; USE is any user's, subject to CONNECT.

// errBeforeV6 is what database DDL returns until the cluster has
// finalized v6 (the migration creates the default database's descriptor,
// which is the tell).
func errBeforeV6() *Error {
	return newErrf(CodeFeatureNotSupported, "databases need cluster version v6: finalize the upgrade with `datax debug upgrade` first")
}

func (s *Session) requireV6(ctx context.Context, txn *kvclient.Txn) error {
	def, err := catalog.LookupDatabase(ctx, txn, catalog.DefaultDatabase)
	if err != nil {
		return err
	}
	if def.ID == 0 {
		return errBeforeV6()
	}
	return nil
}

func reservedDatabase(name string) bool {
	return name == catalog.DefaultDatabase || name == catalog.SystemDatabase
}

func (s *Session) execCreateDatabase(ctx context.Context, txn *kvclient.Txn, t *parser.CreateDatabase) (*Result, error) {
	if err := s.requireV6(ctx, txn); err != nil {
		return nil, err
	}
	if t.Name == catalog.PublicSchema || t.Name == "" {
		return nil, newErrf(CodeSyntaxError, "%q is not a valid database name", t.Name)
	}
	d := &catalog.DatabaseDescriptor{Name: t.Name, Owner: s.user}
	if err := s.cat.CreateDatabase(ctx, txn, d); err != nil {
		var ex *catalog.ErrDatabaseExists
		if t.IfNotExists && asErr(err, &ex) {
			return &Result{Tag: "CREATE DATABASE"}, nil
		}
		return nil, err
	}
	log.Audit("database-ddl", "stmt", "CREATE DATABASE", "target", t.Name, "principal", s.user)
	return &Result{Tag: "CREATE DATABASE"}, nil
}

// execDropDatabase drops a database: refused for the reserved ones, for
// the session's own current database (PostgreSQL's rule), and for a
// database that still holds tables unless CASCADE drops them too (their
// descriptors, names and statistics; row data is left for GC like any
// DROP TABLE).
func (s *Session) execDropDatabase(ctx context.Context, txn *kvclient.Txn, t *parser.DropDatabase) (*Result, error) {
	if err := s.requireV6(ctx, txn); err != nil {
		return nil, err
	}
	if reservedDatabase(t.Name) {
		return nil, newErrf(CodeInsufficientPriv, "database %q is reserved and cannot be dropped", t.Name)
	}
	if t.Name == s.database {
		return nil, newErrf(CodeObjectInUse, "cannot drop the currently open database %q", t.Name)
	}
	db, err := catalog.LookupDatabase(ctx, txn, t.Name)
	if err != nil {
		var nf *catalog.ErrDatabaseNotFound
		if t.IfExists && asErr(err, &nf) {
			return &Result{Tag: "DROP DATABASE"}, nil
		}
		return nil, ToSQLError(err)
	}
	tables, err := s.cat.ListIn(ctx, txn, db)
	if err != nil {
		return nil, err
	}
	if len(tables) > 0 && !t.Cascade {
		return nil, newErrf(CodeDependentObjectsExist, "database %q holds %d table(s); DROP DATABASE ... CASCADE drops them too", t.Name, len(tables))
	}
	for _, d := range tables {
		if _, err := s.cat.DropIn(ctx, txn, t.Name, d.Name); err != nil {
			return nil, err
		}
		if err := txn.Delete(ctx, keys.TableStatsKey(d.ID)); err != nil {
			return nil, err
		}
	}
	if err := s.cat.DropDatabase(ctx, txn, db); err != nil {
		return nil, err
	}
	log.Audit("database-ddl", "stmt", "DROP DATABASE", "target", t.Name, "tables", len(tables), "principal", s.user)
	return &Result{Tag: "DROP DATABASE"}, nil
}

func (s *Session) execAlterDatabase(ctx context.Context, txn *kvclient.Txn, t *parser.AlterDatabase) (*Result, error) {
	if err := s.requireV6(ctx, txn); err != nil {
		return nil, err
	}
	if reservedDatabase(t.Name) || reservedDatabase(t.NewName) || t.NewName == catalog.PublicSchema {
		return nil, newErrf(CodeInsufficientPriv, "database names %q and %q are reserved", catalog.DefaultDatabase, catalog.SystemDatabase)
	}
	db, err := catalog.LookupDatabase(ctx, txn, t.Name)
	if err != nil {
		return nil, ToSQLError(err)
	}
	if err := s.cat.RenameDatabase(ctx, txn, db, t.NewName); err != nil {
		return nil, err
	}
	if s.database == t.Name {
		s.database = t.NewName
	}
	log.Audit("database-ddl", "stmt", "ALTER DATABASE RENAME", "target", t.Name, "to", t.NewName, "principal", s.user)
	return &Result{Tag: "ALTER DATABASE"}, nil
}

func (s *Session) execShowDatabases(ctx context.Context, txn *kvclient.Txn) (*Result, error) {
	dbs, err := catalog.ListDatabases(ctx, txn)
	if err != nil {
		return nil, err
	}
	res := &Result{Columns: []ResultColumn{{Name: "database_name", Type: types.String}, {Name: "owner", Type: types.String}}}
	for _, d := range dbs {
		owner := d.Owner
		if owner == "" {
			owner = "root"
		}
		res.Rows = append(res.Rows, []types.Datum{types.NewString(d.Name), types.NewString(owner)})
	}
	res.Tag = fmt.Sprintf("SHOW DATABASES %d", len(res.Rows))
	return res, nil
}

// UseDatabase switches the session's current database after checking
// that it exists (3D000) and that the user may connect to it (42501):
// PostgreSQL's CONNECT privilege, granted to PUBLIC unless revoked.
func (s *Session) UseDatabase(ctx context.Context, name string) *Error {
	if name == "" {
		return newErrf(CodeInvalidCatalogName, "database name is empty")
	}
	var db *catalog.DatabaseDescriptor
	err := s.db.RunTxn(ctx, "use-database", func(ctx context.Context, txn *kvclient.Txn) error {
		d, err := catalog.LookupDatabase(ctx, txn, name)
		if err != nil {
			return err
		}
		db = d
		if s.user == "root" || !d.ConnectRestricted || d.HasPrivilege(s.user, catalog.PrivConnect) {
			return nil
		}
		ok, err := s.isAdmin(ctx, txn)
		if err != nil {
			return err
		}
		if !ok {
			return newErrf(CodeInsufficientPriv, "permission denied for database %q: CONNECT was revoked from PUBLIC and %q holds no grant", name, s.user)
		}
		return nil
	})
	if err != nil {
		var nf *catalog.ErrDatabaseNotFound
		if asErr(err, &nf) {
			return newErrf(CodeInvalidCatalogName, "database %q does not exist", name)
		}
		return ToSQLError(err)
	}
	s.database = db.Name
	return nil
}

// execGrantRevokeDatabase applies GRANT/REVOKE CREATE | CONNECT | ALL ON
// DATABASE db TO user | PUBLIC. Only CONNECT has a PUBLIC form (the
// default everyone has, revocable); CREATE is per user.
func (s *Session) execGrantRevokeDatabase(ctx context.Context, txn *kvclient.Txn, t *parser.GrantRevoke, tag string) (*Result, error) {
	if err := s.requireV6(ctx, txn); err != nil {
		return nil, err
	}
	set := map[string]bool{}
	for _, p := range t.Privileges {
		switch p {
		case "ALL":
			set[catalog.PrivCreate], set[catalog.PrivConnect] = true, true
		case catalog.PrivCreate, catalog.PrivConnect:
			set[p] = true
		default:
			return nil, newErrf(CodeSyntaxError, "%s is not a database privilege (database privileges: CREATE, CONNECT, ALL)", p)
		}
	}
	db, err := catalog.LookupDatabase(ctx, txn, t.Database)
	if err != nil {
		return nil, ToSQLError(err)
	}
	if db.ID == 0 {
		return nil, errBeforeV6()
	}
	if t.User == "public" {
		if set[catalog.PrivCreate] {
			return nil, newErrf(CodeFeatureNotSupported, "CREATE cannot be granted to PUBLIC; grant it to a user")
		}
		db.ConnectRestricted = t.Revoke
	} else {
		if db.Privileges == nil {
			db.Privileges = map[string][]string{}
		}
		cur := map[string]bool{}
		for _, p := range db.Privileges[t.User] {
			cur[p] = true
		}
		for p := range set {
			cur[p] = !t.Revoke
		}
		var next []string
		for p, on := range cur {
			if on {
				next = append(next, p)
			}
		}
		sort.Strings(next)
		if len(next) == 0 {
			delete(db.Privileges, t.User)
		} else {
			db.Privileges[t.User] = next
		}
	}
	if err := s.cat.UpdateDatabase(ctx, txn, db); err != nil {
		return nil, err
	}
	log.Audit("privilege-ddl", "stmt", tag, "privileges", fmt.Sprint(t.Privileges), "database", t.Database, "target", t.User, "principal", s.user)
	return &Result{Tag: tag}, nil
}
