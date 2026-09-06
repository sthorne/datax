package sql

import (
	"context"
	"fmt"
	"strings"

	"github.com/sthorne/datax/pkg/base"
	"github.com/sthorne/datax/pkg/keys"
	"github.com/sthorne/datax/pkg/kvclient"
	"github.com/sthorne/datax/pkg/sql/catalog"
	"github.com/sthorne/datax/pkg/sql/parser"
	"github.com/sthorne/datax/pkg/sql/types"
	"github.com/sthorne/datax/pkg/util/log"
	"github.com/sthorne/datax/pkg/version"
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
	if t.Placement != nil {
		if err := s.requireV16(); err != nil {
			return nil, err
		}
		policy, err := placementFromOptions(base.PlacementPolicy{}, t.Placement)
		if err != nil {
			return nil, err
		}
		d.Placement = policy
	}
	if err := s.cat.CreateDatabase(ctx, txn, d); err != nil {
		var ex *catalog.ErrDatabaseExists
		if t.IfNotExists && asErr(err, &ex) {
			return &Result{Tag: "CREATE DATABASE"}, nil
		}
		return nil, err
	}
	log.Audit("database-ddl", "stmt", "CREATE DATABASE", "target", t.Name, "principal", s.sessionUser, "role", s.user)
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
	if err := s.checkOwner(ctx, txn, "database", db.Name, db.Owner); err != nil {
		return nil, err
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
	log.Audit("database-ddl", "stmt", "DROP DATABASE", "target", t.Name, "tables", len(tables), "principal", s.sessionUser, "role", s.user)
	return &Result{Tag: "DROP DATABASE"}, nil
}

func (s *Session) execAlterDatabase(ctx context.Context, txn *kvclient.Txn, t *parser.AlterDatabase) (*Result, error) {
	if t.Placement != nil {
		return s.execAlterDatabasePlacement(ctx, txn, t)
	}
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
	if err := s.checkOwner(ctx, txn, "database", db.Name, db.Owner); err != nil {
		return nil, err
	}
	if err := s.cat.RenameDatabase(ctx, txn, db, t.NewName); err != nil {
		return nil, err
	}
	if s.database == t.Name {
		s.database = t.NewName
	}
	log.Audit("database-ddl", "stmt", "ALTER DATABASE RENAME", "target", t.Name, "to", t.NewName, "principal", s.sessionUser, "role", s.user)
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
		ok, err := s.databasePrivAllowed(ctx, txn, d, catalog.PrivConnect)
		if err != nil {
			return err
		}
		if !ok {
			return newErrf(CodeInsufficientPriv, "permission denied for database %q: CONNECT was revoked from PUBLIC and %q holds no grant", name, s.actor())
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

// Replica placement (issue #176). A database may carry a policy — a
// replica count and a set of locality constraints — that the allocator
// honours for every range of its tables. The policy is a catalog fact
// like a privilege: nothing moves when it is written; the replication
// queue notices at its next pass and converges the ranges onto nodes
// the policy admits.

// requireV16 gates the DDL that writes a policy. A v15 node reads the
// descriptor but not the policy field, so it would keep allocating
// replicas anywhere and undo the placement a v16 node just made.
func (s *Session) requireV16() error {
	if s.db.ClusterVersion() < version.V16 {
		return newErrf(CodeFeatureNotSupported, "replica placement needs cluster version v16: finalize the upgrade with `datax debug upgrade` first")
	}
	return nil
}

// placementFromOptions folds an option list onto the policy a database
// already carries: an option the operator did not name is left alone,
// which is what lets ALTER change a replica count without restating the
// constraints. An empty constraint list clears them.
func placementFromOptions(cur base.PlacementPolicy, o *parser.PlacementOptions) (base.PlacementPolicy, error) {
	out := cur.Clone()
	if o.SetReplicas {
		out.Replicas = o.Replicas
	}
	if o.SetConstraints {
		out.Constraints = nil
		for _, raw := range o.Constraints {
			c, err := base.ParseConstraint(raw)
			if err != nil {
				return base.PlacementPolicy{}, newErrf(CodeInvalidParameter, "%s", err)
			}
			out.Constraints = append(out.Constraints, c)
		}
	}
	out = out.Normalize()
	if err := out.Validate(); err != nil {
		return base.PlacementPolicy{}, newErrf(CodeInvalidParameter, "%s", err)
	}
	return out, nil
}

// execAlterDatabasePlacement is ALTER DATABASE name SET (...).
func (s *Session) execAlterDatabasePlacement(ctx context.Context, txn *kvclient.Txn, t *parser.AlterDatabase) (*Result, error) {
	if err := s.requireV6(ctx, txn); err != nil {
		return nil, err
	}
	if err := s.requireV16(); err != nil {
		return nil, err
	}
	db, err := catalog.LookupDatabase(ctx, txn, t.Name)
	if err != nil {
		return nil, ToSQLError(err)
	}
	if err := s.checkOwner(ctx, txn, "database", db.Name, db.Owner); err != nil {
		return nil, err
	}
	policy, err := placementFromOptions(db.Placement, t.Placement)
	if err != nil {
		return nil, err
	}
	if policy.Equal(db.Placement) {
		return &Result{Tag: "ALTER DATABASE"}, nil
	}
	d := db.Clone()
	d.Placement = policy
	if err := s.cat.UpdateDatabase(ctx, txn, d); err != nil {
		return nil, err
	}
	// Carve the database's existing tables into their own ranges, so the
	// policy has ranges to apply to (see presplitPlacement). Lifting a
	// policy needs no splits: the boundaries simply stop being barriers
	// and the merge pass folds the ranges back together on its own.
	if !policy.IsZero() {
		tables, err := s.cat.ListIn(ctx, txn, d)
		if err != nil {
			return nil, err
		}
		for _, t := range tables {
			s.splitTableOut(ctx, t.ID)
		}
	}
	log.Audit("database-ddl", "stmt", "ALTER DATABASE SET PLACEMENT", "target", t.Name, "placement", policy.String(), "principal", s.sessionUser, "role", s.user)
	return &Result{Tag: "ALTER DATABASE"}, nil
}

// presplitPlacement carves a table into its own ranges when its database
// carries a placement policy. A range inherits a policy only when it
// lies wholly inside one table's key space — a range straddling two
// tables could belong to two databases asking for different things — so
// without these boundaries a small database would share one range with
// its neighbours and no policy would apply to it at all.
//
// The splits are best effort and idempotent: an existing boundary is the
// state asked for, and a failure costs a placement that applies at the
// next size split rather than a failed statement.
func (s *Session) presplitPlacement(ctx context.Context, txn *kvclient.Txn, dbName string, desc *catalog.TableDescriptor) {
	if desc == nil || desc.ID == 0 || dbName == "" {
		return
	}
	db, err := catalog.LookupDatabase(ctx, txn, dbName)
	if err != nil || db.Placement.IsZero() {
		return
	}
	s.splitTableOut(ctx, desc.ID)
}

func (s *Session) splitTableOut(ctx context.Context, tableID uint64) {
	lo, hi := keys.TableDataSpan(tableID)
	for _, k := range []keys.Key{lo, hi} {
		if _, err := s.db.AdminSplit(ctx, k); err != nil {
			log.Debugf("placement pre-split at %s: %v", k, err)
		}
	}
}

// execShowPlacement renders one database's policy, or the session's own
// database when the statement names none. The replica count shown is
// the one the allocator will use, so a database with no policy of its
// own reports the cluster default rather than a blank.
func (s *Session) execShowPlacement(ctx context.Context, txn *kvclient.Txn, t *parser.ShowPlacement) (*Result, error) {
	name := t.Database
	if name == "" {
		name = s.database
	}
	if name == "" {
		name = catalog.DefaultDatabase
	}
	db, err := catalog.LookupDatabase(ctx, txn, name)
	if err != nil {
		return nil, ToSQLError(err)
	}
	res := &Result{Columns: []ResultColumn{
		{Name: "database_name", Type: types.String},
		{Name: "replicas", Type: types.Int},
		{Name: "constraints", Type: types.String},
		{Name: "source", Type: types.String},
	}}
	source := "cluster default"
	if !db.Placement.IsZero() {
		source = "database policy"
	}
	constraints := "any node"
	if cs := db.Placement.ConstraintStrings(); len(cs) > 0 {
		constraints = strings.Join(cs, ", ")
	}
	res.Rows = append(res.Rows, []types.Datum{
		types.NewString(db.Name),
		types.NewInt(int64(db.Placement.ReplicasOr(base.DefaultReplicationFactor))),
		types.NewString(constraints),
		types.NewString(source),
	})
	res.Tag = "SHOW PLACEMENT 1"
	return res, nil
}
