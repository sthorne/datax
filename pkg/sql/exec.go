package sql

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"runtime/debug"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/sthorne/datax/pkg/audit"
	"github.com/sthorne/datax/pkg/keys"
	"github.com/sthorne/datax/pkg/kvclient"
	"github.com/sthorne/datax/pkg/kvpb"
	"github.com/sthorne/datax/pkg/metrics"
	"github.com/sthorne/datax/pkg/sql/catalog"
	"github.com/sthorne/datax/pkg/sql/parser"
	"github.com/sthorne/datax/pkg/sql/rowenc"
	"github.com/sthorne/datax/pkg/sql/rowseal"
	"github.com/sthorne/datax/pkg/sql/types"
	"github.com/sthorne/datax/pkg/sql/vtable"
	"github.com/sthorne/datax/pkg/util/log"
	"github.com/sthorne/datax/pkg/version"
)

// execStmt runs one statement in txn. It is the panic barrier of the
// statement path (issue #136): a panic in the planner, the executor, a
// builtin or an encoder becomes an internal error (XX000) for this
// statement — logged with its stack and counted — and the session goes
// on, the transaction failed as by any other error, instead of the
// process dying with every connection on it. What Go cannot recover
// (a stack overflow, out of memory) is not caught, and not papered over.
func (s *Session) execStmt(ctx context.Context, txn *kvclient.Txn, stmt parser.Statement, params []types.Datum) (res *Result, err error) {
	defer func() {
		if r := recover(); r != nil {
			res, err = nil, s.recoveredPanic(r, stmt)
		}
	}()
	if TestingPanicBeforeExec != nil {
		TestingPanicBeforeExec(stmt)
	}
	switch stmt.(type) {
	case *parser.Select, *parser.Insert, *parser.Update, *parser.Delete:
		// A data statement resolves each table once (the view check and
		// the plan both ask); the memo lives for this attempt.
		if s.stmtLookups == nil || s.stmtLookups.txn != txn {
			s.stmtLookups = &lookupMemo{txn: txn, m: make(map[string]*catalog.TableDescriptor, 2)}
			defer func() { s.stmtLookups = nil }()
		}
	}
	if ct, ok := stmt.(*parser.CreateTable); ok {
		// CREATE TABLE: admins, or a user granted CREATE on the database.
		if err := s.checkCreateInDatabase(ctx, txn, ct.Name); err != nil {
			return nil, err
		}
	} else if cv, ok := stmt.(*parser.CreateView); ok {
		if err := s.checkCreateInDatabase(ctx, txn, cv.Name); err != nil {
			return nil, err
		}
	} else if requiresAdmin(stmt) {
		if err := s.checkAdmin(ctx, txn); err != nil {
			return nil, err
		}
	}
	// Ownership, grants and role membership are checked by each
	// statement's executor (privileges.go, roles.go).
	// Views the statement names bind as leading WITH members.
	expanded, err := s.expandViews(ctx, txn, stmt)
	if err != nil {
		return nil, err
	}
	stmt = expanded
	switch t := stmt.(type) {
	case *parser.GrantRevoke:
		return s.execGrantRevoke(ctx, txn, t)
	case *parser.CreateDatabase:
		return s.execCreateDatabase(ctx, txn, t)
	case *parser.DropDatabase:
		return s.execDropDatabase(ctx, txn, t)
	case *parser.AlterDatabase:
		return s.execAlterDatabase(ctx, txn, t)
	case *parser.ShowDatabases:
		return s.execShowDatabases(ctx, txn)
	case *parser.ShowPlacement:
		return s.execShowPlacement(ctx, txn, t)
	case *parser.CreateTable:
		return s.execCreateTable(ctx, txn, t)
	case *parser.Explain:
		return s.execExplain(ctx, txn, t, params)
	case *parser.AlterTable:
		return s.execAlterTable(ctx, txn, t)
	case *parser.DropIndex:
		return s.execDropIndex(ctx, txn, t)
	case *parser.CreateIndex:
		// Reached only inside a transaction block (executeData runs the
		// online build outside one).
		return s.execCreateIndexInTxn(ctx, txn, t)
	case *parser.CreateExtension:
		// The extensions datax accepts are built in (vtable.Extensions:
		// plpgsql, pgcrypto's gen_random_uuid, uuid-ossp's
		// uuid_generate_v4, pgvector), so the statement that would
		// install one has nothing to do; any other extension is
		// something datax does not have.
		if _, ok := vtable.LookupExtension(t.Name); !ok {
			return nil, newErrf(CodeFeatureNotSupported, "extension %q is not available (datax has no installable extensions; %s are accepted because what they provide is built in)", t.Name, vtable.ExtensionNames())
		}
		return &Result{Tag: "CREATE EXTENSION"}, nil
	case *parser.DropExtension:
		// A built-in extension cannot be removed; dropping one does
		// nothing. Any other name is not installed, as in PostgreSQL.
		var notices []string
		for _, name := range t.Names {
			if _, ok := vtable.LookupExtension(name); ok {
				continue
			}
			if !t.IfExists {
				return nil, newErrf(CodeUndefinedObject, "extension %q does not exist", name)
			}
			notices = append(notices, fmt.Sprintf("extension %q does not exist, skipping", name))
		}
		return &Result{Tag: "DROP EXTENSION", Notices: notices}, nil
	case *parser.CreateView:
		return s.execCreateView(ctx, txn, t)
	case *parser.CommentOn:
		return s.execCommentOn(ctx, txn, t)
	case *parser.DropView:
		return s.execDropView(ctx, txn, t)
	case *parser.AlterIndex:
		return s.execAlterIndex(ctx, txn, t)
	case *parser.Truncate:
		return s.execTruncate(ctx, txn, t)
	case *parser.CreateRole:
		return s.execCreateRole(ctx, txn, t)
	case *parser.DropRole:
		return s.execDropRole(ctx, txn, t)
	case *parser.AlterOwner:
		return s.execAlterOwner(ctx, txn, t)
	case *parser.ReassignOwned:
		return s.execReassignOwned(ctx, txn, t)
	case *parser.DropOwned:
		return s.execDropOwned(ctx, txn, t)
	case *parser.AlterDefaultPrivileges:
		return s.execAlterDefaultPrivileges(ctx, txn, t)
	case *parser.CreatePolicy:
		return s.execCreatePolicy(ctx, txn, t)
	case *parser.AlterPolicy:
		return s.execAlterPolicy(ctx, txn, t)
	case *parser.DropPolicy:
		return s.execDropPolicy(ctx, txn, t)
	case *parser.DropTable:
		return s.execDropTable(ctx, txn, t)
	case *parser.Insert:
		return s.execInsert(ctx, txn, t, params)
	case *parser.CopyFrom:
		// COPY's data arrives through the pgwire copy-in sub-protocol, so
		// the statement cannot execute through the ordinary path.
		return nil, newErrf(CodeFeatureNotSupported, "COPY FROM STDIN can only run over the wire protocol")
	case *parser.Select:
		return s.execSelect(ctx, txn, t, params)
	case *parser.Update:
		return s.execUpdate(ctx, txn, t, params)
	case *parser.Delete:
		return s.execDelete(ctx, txn, t, params)
	case *parser.ShowTables:
		return s.execShowTables(ctx, txn, t.Database)
	case *parser.CreateType:
		return s.execCreateType(ctx, txn, t)
	case *parser.AlterType:
		return s.execAlterType(ctx, txn, t)
	case *parser.DropType:
		return s.execDropType(ctx, txn, t)
	case *parser.CreateSequence:
		return s.execCreateSequence(ctx, txn, t)
	case *parser.AlterSequence:
		return s.execAlterSequence(ctx, txn, t)
	case *parser.DropSequence:
		return s.execDropSequence(ctx, txn, t)
	case *parser.ShowSequences:
		return s.execShowSequences(ctx, txn)
	case *parser.ShowFunctions:
		return execShowFunctions(), nil
	case *parser.Show:
		return s.execShow(ctx, txn, t)
	case *parser.ShowStats:
		return s.execShowStats(ctx, txn, t)
	case *parser.LockTable:
		return s.execLockTable(ctx, txn, t)
	case *parser.Analyze, *parser.Vacuum:
		// ANALYZE (and VACUUM, which may run it) sweeps in transactions
		// of its own and is intercepted before execStmt; reaching here
		// means an unsupported calling context.
		if _, ok := stmt.(*parser.Vacuum); ok {
			return nil, newErrf(CodeFeatureNotSupported, "VACUUM cannot run in this context")
		}
		return nil, newErrf(CodeFeatureNotSupported, "ANALYZE cannot run in this context")
	default:
		return nil, newErrf(CodeFeatureNotSupported, "unsupported statement %T", stmt)
	}
}

func (s *Session) execCreateTable(ctx context.Context, txn *kvclient.Txn, t *parser.CreateTable) (*Result, error) {
	if t.As != nil {
		return nil, newErrf(CodeActiveTransaction, "CREATE TABLE ... AS cannot run inside a transaction block")
	}
	t, likeIndexes, err := s.expandLike(ctx, txn, t)
	if err != nil {
		return nil, err
	}
	if t, err = withRowidKey(t); err != nil {
		return nil, err
	}
	dbName, bare := catalog.SplitTableName(t.Name)
	if dbName == "" {
		dbName = s.database
	}
	if catalog.IsSystemTable(bare) && !s.system {
		return nil, newErrf(CodeInsufficientPriv, "table name %q is reserved for the cluster's own use", bare)
	}
	if dbName == catalog.SystemDatabase && !s.system {
		return nil, newErrf(CodeInsufficientPriv, "database %q is reserved for the cluster", dbName)
	}
	dbID, err := s.cat.DatabaseID(ctx, txn, dbName)
	if err != nil {
		return nil, ToSQLError(err)
	}
	desc := &catalog.TableDescriptor{Name: bare, DatabaseID: dbID, Owner: s.user}
	if id, ok := catalog.SystemTableID(bare); ok {
		desc.ID = id
	}
	seen := map[string]bool{}
	var colPK []string
	for i, cd := range t.Columns {
		if seen[cd.Name] {
			return nil, newErrf(CodeSyntaxError, "duplicate column %q", cd.Name)
		}
		seen[cd.Name] = true
		col := catalog.Column{
			ID: catalog.ColumnID(i + 1), Name: cd.Name, Type: cd.Type, NotNull: cd.NotNull,
			Precision: cd.Precision, Scale: cd.Scale,
			Width: cd.Width, MaxLen: cd.MaxLen, Char: cd.Char, NoTZ: cd.NoTZ, TimePrecision: cd.TimePrecision,
		}
		col.Hidden = cd.Hidden
		s.stripTypeAttrs(&col)
		if err := s.requireV10(col.Type); err != nil {
			return nil, ToSQLError(err)
		}
		if err := s.resolveEnumColumn(ctx, txn, &col, cd.TypeName); err != nil {
			return nil, ToSQLError(err)
		}
		if cd.Default != nil && !cd.Default.Null {
			d, cerr := coerceColumn(col, *cd.Default)
			if cerr != nil {
				return nil, newErrf(CodeSyntaxError, "DEFAULT for column %q: %v", cd.Name, cerr.Msg)
			}
			d, terr := enforceTypmod(col, d)
			if terr != nil {
				return nil, terr
			}
			col.Default = &d
		}
		if cd.DefaultExpr != nil {
			text, err := s.validateDefaultExpr(ctx, txn, cd.Name, cd.Type, *cd.DefaultExpr)
			if err != nil {
				return nil, err
			}
			col.DefaultExpr = text
		}
		if cd.Serial || cd.Identity != "" {
			if err := s.requireV7("SERIAL and identity columns"); err != nil {
				return nil, err
			}
			if cd.Type != types.Int {
				return nil, newErrf(CodeInvalidParameter, "identity column %q must be an integer type", cd.Name)
			}
			if cd.Default != nil || cd.DefaultExpr != nil {
				return nil, newErrf(CodeSyntaxError, "column %q: both a DEFAULT and an identity are specified", cd.Name)
			}
			col.Identity = cd.Identity
			if cd.Serial {
				col.Identity = "" // SERIAL is a plain default, not an identity: explicit values are fine
			}
		}
		desc.Columns = append(desc.Columns, col)
		if cd.PrimaryKey {
			colPK = append(colPK, cd.Name)
		}
	}
	pkNames := t.PrimaryKey
	if len(pkNames) == 0 {
		pkNames = colPK
	} else if len(colPK) > 0 {
		return nil, newErrf(CodeSyntaxError, "multiple primary key definitions")
	}
	if err := applyTableOptions(desc, t.Options, pkNames); err != nil {
		return nil, err
	}
	sealed, tenantCol, err := s.checkSealOptions(ctx, desc, t.Options, pkNames)
	if err != nil {
		return nil, err
	}
	general := false
	for name := range t.Options {
		general = general || generalTableOption(name)
	}
	if general {
		if err := s.requireV21("the gc_ttl and row expiry options"); err != nil {
			return nil, err
		}
		if _, err := applyGeneralOptions(desc, t.Options, false); err != nil {
			return nil, err
		}
	}
	if desc.ShardBuckets > 0 {
		// The hidden bucket column leads the primary key; the executor
		// fills it from the logical PK on every insert.
		shard := catalog.Column{
			ID:      catalog.ColumnID(len(desc.Columns) + 1),
			Name:    rowenc.ShardColumnName,
			Type:    types.Int,
			NotNull: true,
			Hidden:  true,
		}
		desc.Columns = append(desc.Columns, shard)
		pkNames = append([]string{rowenc.ShardColumnName}, pkNames...)
	}
	for _, name := range pkNames {
		col, ok := desc.Col(name)
		if !ok {
			return nil, newErrf(CodeUndefinedColumn, "primary key column %q does not exist", name)
		}
		if !types.IsIndexable(col.Type) {
			return nil, newErrf(CodeFeatureNotSupported, "column %q of type %s cannot be part of a primary key (no ordered key encoding)", name, col.Type)
		}
		desc.PrimaryKey = append(desc.PrimaryKey, col.ID)
		// PK columns are implicitly NOT NULL.
		for i := range desc.Columns {
			if desc.Columns[i].ID == col.ID {
				desc.Columns[i].NotNull = true
			}
		}
	}
	desc.NextColumnID = catalog.ColumnID(len(desc.Columns) + 1)
	if catalog.TakesDefaultPrivileges(desc.Name) {
		if err := s.applyDefaultPrivileges(ctx, txn, dbID, desc.Owner, "TABLES", &desc.Privileges, &desc.GrantOptions); err != nil {
			return nil, err
		}
	}
	for _, c := range desc.Columns {
		if err := s.requireTextSearchTypes(c.Type); err != nil {
			return nil, err
		}
	}
	if err := s.cat.Create(ctx, txn, desc); err != nil {
		var ex *catalog.ErrTableExists
		if t.IfNotExists {
			if ok := asErr(err, &ex); ok {
				return &Result{Tag: "CREATE TABLE"}, nil
			}
		}
		return nil, err
	}
	s.noteCreatedTable(txn, desc.ID)
	// SERIAL and identity columns own a sequence named <table>_<col>_seq,
	// created now that the table has its ID; the column defaults to
	// nextval of it.
	owned := false
	for i, cd := range t.Columns {
		if !cd.Serial && cd.Identity == "" {
			continue
		}
		col := &desc.Columns[i]
		sd := catalog.NewSequenceDescriptor(desc.Name+"_"+col.Name+"_seq", desc.DatabaseID)
		sd.OwnerTable, sd.OwnerColumn = desc.ID, col.ID
		sd.Owner = desc.Owner
		if err := s.applyDefaultPrivileges(ctx, txn, desc.DatabaseID, sd.Owner, "SEQUENCES", &sd.Privileges, &sd.GrantOptions); err != nil {
			return nil, err
		}
		if cd.IdentitySeq != nil {
			if err := applySequenceOptions(sd, cd.IdentitySeq, true); err != nil {
				return nil, err
			}
		}
		if err := catalog.CreateSequence(ctx, txn, sd); err != nil {
			return nil, ToSQLError(err)
		}
		col.SequenceID = sd.ID
		col.DefaultExpr = nextvalDefault(sd.Name)
		owned = true
	}
	constrained, err := s.createTableConstraints(ctx, txn, desc, t)
	if err != nil {
		return nil, err
	}
	if len(likeIndexes) > 0 {
		if err := addLikeIndexes(desc, likeIndexes); err != nil {
			return nil, err
		}
	}
	ttlIndexed := false
	if desc.RowTTL != nil {
		before := len(desc.Indexes)
		addTTLIndex(desc)
		ttlIndexed = len(desc.Indexes) != before
	}
	if sealed {
		if err := s.sealNewTable(ctx, txn, desc, tenantCol, "CREATE TABLE"); err != nil {
			return nil, err
		}
	}
	if owned || constrained || len(likeIndexes) > 0 || ttlIndexed || sealed {
		if err := s.cat.Update(ctx, txn, desc); err != nil {
			return nil, err
		}
	}
	s.presplitTimeseries(ctx, desc)
	if desc.GCTTLSeconds > 0 {
		s.splitAtTableEdges(ctx, desc)
	}
	s.presplitPlacement(ctx, txn, dbName, desc)
	res := &Result{Tag: "CREATE TABLE"}
	if sealed {
		res.Notices = append(res.Notices, sealedTableNotice(desc))
	}
	return res, nil
}

// validateDefaultExpr checks an expression default at DDL time — the v7
// gate, no column references or subqueries, the value coercible to the
// column's type — and returns its stored text.
func (s *Session) validateDefaultExpr(ctx context.Context, txn *kvclient.Txn, colName string, typ types.Family, e parser.Expr) (string, error) {
	if err := s.requireV7("expression DEFAULTs"); err != nil {
		return "", err
	}
	if exprHasSubquery(e) || e.Case != nil || e.Cmp != nil {
		return "", newErrf(CodeFeatureNotSupported, "DEFAULT for column %q: subqueries, CASE and comparisons are not supported in defaults", colName)
	}
	text := parser.FormatExpr(e)
	if _, err := parser.ParseExpr(text); err != nil {
		return "", newErrf(CodeSyntaxError, "DEFAULT for column %q: %v", colName, err)
	}
	// Try it once: an unknown function or a type mismatch fails now, not
	// at the first INSERT. nextval is not exercised (the sequence may be
	// created after the table).
	if !exprIsVolatile(e) {
		r, err := s.resolveValueExpr(ctx, txn, e, nil)
		if err != nil {
			return "", newErrf(CodeSyntaxError, "DEFAULT for column %q: %v", colName, err)
		}
		d, err := evalExpr(r, nil, nil, nil)
		if err != nil {
			return "", newErrf(CodeSyntaxError, "DEFAULT for column %q: %v", colName, err)
		}
		if _, err := d.Coerce(typ); err != nil {
			return "", newErrf(CodeInvalidTextRepresentation, "DEFAULT for column %q: %v", colName, err)
		}
	}
	return text, nil
}

// applyTableOptions validates the WITH (...) options and stamps the
// timeseries fields onto desc (before the hidden shard column is added).
func applyTableOptions(desc *catalog.TableDescriptor, options map[string]string, pkNames []string) error {
	timeseries := false
	for name := range options {
		switch name {
		case "timeseries", "retention", "shards":
			timeseries = true
		default:
			if !generalTableOption(name) && !sealOption(name) {
				return newErrf(CodeSyntaxError, "unknown table option %q", name)
			}
		}
	}
	if !timeseries {
		return nil // only options any table takes (applyGeneralOptions)
	}
	switch options["timeseries"] {
	case "true":
	case "", "false":
		return newErrf(CodeSyntaxError, "the retention and shards options require timeseries=true")
	default:
		return newErrf(CodeSyntaxError, "timeseries must be true or false")
	}
	desc.Timeseries = true

	// The timestamp must close the (logical) primary key so each series'
	// rows are stored in time order and retention trims a key suffix.
	last, ok := desc.Col(pkNames[len(pkNames)-1])
	if ok && last.Hidden {
		// The generated rowid of a table that declares no key (#354).
		return newErrf(CodeFeatureNotSupported, "a timeseries table must declare a PRIMARY KEY ending in its TIMESTAMPTZ column")
	}
	if !ok || last.Type != types.Timestamp {
		return newErrf(CodeFeatureNotSupported, "a timeseries table's last primary key column must be TIMESTAMPTZ")
	}

	if v, ok := options["retention"]; ok {
		secs, err := parseRetention(v)
		if err != nil {
			return newErrf(CodeSyntaxError, "retention: %v", err)
		}
		desc.RetentionSeconds = secs
	}
	if v, ok := options["shards"]; ok {
		n, err := strconv.Atoi(v)
		if err != nil || n < 2 || n > 256 {
			return newErrf(CodeSyntaxError, "shards must be an integer in [2, 256]")
		}
		if _, exists := desc.Col(rowenc.ShardColumnName); exists {
			return newErrf(CodeSyntaxError, "column name %q is reserved for sharded timeseries tables", rowenc.ShardColumnName)
		}
		desc.ShardBuckets = int32(n)
	}
	return nil
}

// parseRetention parses a duration like 90d, 36h, 30m, or 45s — or any
// interval text ('7 days', '1 month 12 hours'; a month counts 30 days)
// — into seconds.
func parseRetention(s string) (int64, error) {
	if len(s) < 2 {
		return 0, fmt.Errorf("want <number><d|h|m|s> or an interval, got %q", s)
	}
	n, err := strconv.ParseInt(s[:len(s)-1], 10, 64)
	if err != nil {
		iv, ierr := types.ParseInterval(s)
		if ierr != nil {
			return 0, fmt.Errorf("want <number><d|h|m|s> or an interval, got %q", s)
		}
		secs := iv.CmpValue() / int64(time.Second)
		if secs <= 0 {
			return 0, fmt.Errorf("retention %q must be positive", s)
		}
		return secs, nil
	}
	if n <= 0 {
		return 0, fmt.Errorf("want a positive number before the unit in %q", s)
	}
	var mult int64
	switch s[len(s)-1] {
	case 'd':
		mult = 24 * 3600
	case 'h':
		mult = 3600
	case 'm':
		mult = 60
	case 's':
		mult = 1
	default:
		return 0, fmt.Errorf("unit must be d, h, m, or s in %q", s)
	}
	if n > (1<<62)/mult {
		return 0, fmt.Errorf("retention %q overflows", s)
	}
	return n * mult, nil
}

// execSetRetention is ALTER TABLE ... SET (retention = '<n><d|h|m|s>'):
// one descriptor write; the retention provider picks the new TTL up on
// its next refresh (the session has already checked admin).
func (s *Session) execSetRetention(ctx context.Context, t *parser.AlterTable) (*Result, *Error) {
	secs, perr := parseRetention(t.SetOptions["retention"])
	if perr != nil {
		return nil, newErrf(CodeSyntaxError, "retention: %v", perr)
	}
	err := s.db.RunTxn(ctx, "set-retention", func(ctx context.Context, txn *kvclient.Txn) error {
		shared, err := s.lookup(ctx, txn, t.Table)
		if err != nil {
			return err
		}
		if !shared.Timeseries {
			return newErrf(CodeFeatureNotSupported, "retention applies to timeseries tables (created WITH (timeseries = true))")
		}
		if shared.GCTTLSeconds > 0 {
			return newErrf(CodeFeatureNotSupported, "table %q has a gc_ttl; a retention is itself the table's GC window (ALTER TABLE ... RESET (gc_ttl) first)", shared.Name)
		}
		desc := shared.Clone()
		desc.RetentionSeconds = secs
		return s.cat.Update(ctx, txn, desc)
	})
	if err != nil {
		return nil, ToSQLError(err)
	}
	return &Result{Tag: "ALTER TABLE"}, nil
}

// MinAuditRetentionSeconds is the shortest retention the audit table
// accepts. Retention is the one way records leave the table, so a
// period of seconds would be a DELETE by another name; a day leaves the
// change, which is itself recorded, time to be seen before anything
// goes.
const MinAuditRetentionSeconds = 24 * 3600

// execSetAuditRetention is ALTER TABLE datax_audit SET (retention =
// ...), the single change SQL may make to the audit table (issue #293):
// admins only, nothing but the retention, and the change is an audit
// record of its own. The table is not a timeseries table and the
// storage layer's retention does not apply to it — the nodes age their
// own chains out, each saying in its chain where it was cut — so the
// period is only stored here for them to read.
func (s *Session) execSetAuditRetention(ctx context.Context, t *parser.AlterTable) (*Result, *Error) {
	if len(t.SetOptions) != 1 || t.SetOptions["retention"] == "" {
		return nil, newErrf(CodeInsufficientPriv, "table %q belongs to the cluster: only ALTER TABLE ... SET (retention = ...) is allowed", t.Table)
	}
	secs, perr := parseRetention(t.SetOptions["retention"])
	if perr != nil {
		return nil, newErrf(CodeSyntaxError, "retention: %v", perr)
	}
	if secs < MinAuditRetentionSeconds {
		return nil, newErrf(CodeInvalidParameterValue, "retention: audit records are kept for at least one day")
	}
	var before int64
	err := s.db.RunTxn(ctx, "set-audit-retention", func(ctx context.Context, txn *kvclient.Txn) error {
		if err := s.checkAdmin(ctx, txn); err != nil {
			return err
		}
		shared, err := s.lookup(ctx, txn, t.Table)
		if err != nil {
			return err
		}
		desc := shared.Clone()
		before = desc.RetentionSeconds
		desc.RetentionSeconds = secs
		return s.cat.Update(ctx, txn, desc)
	})
	if err != nil {
		return nil, ToSQLError(err)
	}
	log.Audit(audit.EventRetentionSet, "target", catalog.AuditTableName, "seconds", secs, "was_seconds", before, "principal", s.sessionUser, "role", s.user)
	return &Result{Tag: "ALTER TABLE"}, nil
}

// presplitTimeseries pre-carves a fresh timeseries table's key space:
// splits at the table's span boundaries (so ranges rarely straddle the
// table edge — the common case for per-table retention GC is then "range
// fully inside the table") and, for sharded tables, at each bucket
// prefix, so ingest parallelizes across ranges immediately instead of
// after autosplit catches up. Best-effort: splits are non-transactional
// and outlive an aborted enclosing txn, which is harmless — an aborted
// CREATE leaves empty ranges that the size-based merger re-absorbs.
func (s *Session) presplitTimeseries(ctx context.Context, desc *catalog.TableDescriptor) {
	if !desc.Timeseries || desc.ID == 0 {
		return
	}
	splitAt := []keys.Key{
		keys.TableDataPrefix(desc.ID),
		keys.TableDataPrefix(desc.ID).PrefixEnd(),
	}
	for b := int32(1); b < desc.ShardBuckets; b++ {
		k, err := rowenc.AppendKeyDatum(rowenc.PrimaryKeyPrefixFor(desc), types.Int, types.NewInt(int64(b)))
		if err != nil {
			return
		}
		splitAt = append(splitAt, k)
	}
	for _, k := range splitAt {
		if _, err := s.db.AdminSplit(ctx, k); err != nil {
			log.Debugf("timeseries pre-split at %s: %v", k, err)
		}
	}
}

// execSplitAt is ALTER TABLE t SPLIT AT VALUES (k, ...), ... [WITH
// EXPIRATION e]: carve a range boundary at each primary-key tuple (a
// prefix of the key is allowed), so a bulk load or a benchmark spreads
// over many ranges from the start instead of waiting for size- and
// load-based splits. Rows are the boundaries as key paths. A sharded
// timeseries table's key begins with its shard bucket, so it is split by
// shard (SET (shards = N)), not here.
//
// The boundaries are sticky (issue #265): the halves of a pre-split are
// empty, which is exactly what the merge queue folds back together, so
// each is recorded on its range as made on purpose and stays until
// UNSPLIT or its expiration. Before the cluster has finalized v20 the
// record cannot be written; the split is then an ordinary one, as it was
// before, and an expiration — which would promise something the cluster
// cannot keep — is refused.
func (s *Session) execSplitAt(ctx context.Context, t *parser.AlterTable, params []types.Datum) (*Result, error) {
	desc, err := s.splitTable(ctx, t)
	if err != nil {
		return nil, err
	}
	if desc == nil {
		return &Result{Tag: "ALTER TABLE"}, nil
	}
	stickyUntil := int64(kvpb.StickyForever)
	if t.SplitExpiration != nil {
		if !s.db.StickyBitsOK() {
			return nil, newErrf(CodeFeatureNotSupported, "SPLIT AT ... WITH EXPIRATION needs cluster version %s (finalize the upgrade first)", version.V20)
		}
		if stickyUntil, err = s.splitExpiration(*t.SplitExpiration, params); err != nil {
			return nil, err
		}
	}
	res := &Result{Tag: "ALTER TABLE", Columns: splitColumns()}
	for _, tuple := range t.SplitAt {
		key, err := splitKey(desc, "SPLIT AT", tuple, params)
		if err != nil {
			return nil, err
		}
		// An existing boundary is the state asked for (a re-run of a
		// load script or a benchmark's set-up) and the KV split treats
		// it so, marking it sticky. The KV layer also carries the two
		// waits this statement used to fail on or spin for: a range
		// frozen for a merge in flight is waited out by the client, and
		// a range that another split or merge changed first is re-read
		// and split again by the leader (issue #290).
		if _, err := s.db.AdminSplitSticky(ctx, key, stickyUntil); err != nil {
			return nil, err
		}
		res.Rows = append(res.Rows, []types.Datum{types.NewString(keys.PrettyForTable(key, desc.ID, desc.Name))})
	}
	res.Tag = fmt.Sprintf("ALTER TABLE %d", len(res.Rows))
	return res, nil
}

// execUnsplit is ALTER TABLE t UNSPLIT AT VALUES (k, ...), ... and
// UNSPLIT ALL: stop holding manual range boundaries against merging.
// Nothing merges here; the merge queue folds the ranges back when they
// are small enough, as it does any others. UNSPLIT AT names boundaries
// the way SPLIT AT does and fails on a key that is not one; UNSPLIT ALL
// releases every sticky boundary inside the table. Rows are the
// boundaries released.
func (s *Session) execUnsplit(ctx context.Context, t *parser.AlterTable, params []types.Datum) (*Result, error) {
	desc, err := s.splitTable(ctx, t)
	if err != nil {
		return nil, err
	}
	if desc == nil {
		return &Result{Tag: "ALTER TABLE"}, nil
	}
	if !s.db.StickyBitsOK() {
		return nil, newErrf(CodeFeatureNotSupported, "UNSPLIT needs cluster version %s (finalize the upgrade first)", version.V20)
	}
	res := &Result{Tag: "ALTER TABLE", Columns: splitColumns()}
	var unsplit []keys.Key
	if t.UnsplitAll {
		// Every range that starts strictly inside the table's primary
		// index: the table's own edges are not SPLIT AT boundaries.
		lo := rowenc.PrimaryKeyPrefixFor(desc)
		if unsplit, err = s.stickyBoundaries(ctx, lo, lo.PrefixEnd()); err != nil {
			return nil, err
		}
	}
	for _, tuple := range t.UnsplitAt {
		key, err := splitKey(desc, "UNSPLIT AT", tuple, params)
		if err != nil {
			return nil, err
		}
		unsplit = append(unsplit, key)
	}
	for _, key := range unsplit {
		if _, err := s.db.AdminUnsplit(ctx, key); err != nil {
			return nil, err
		}
		res.Rows = append(res.Rows, []types.Datum{types.NewString(keys.PrettyForTable(key, desc.ID, desc.Name))})
	}
	res.Tag = fmt.Sprintf("ALTER TABLE %d", len(res.Rows))
	return res, nil
}

// stickyBoundaries lists, from the range addressing records, the start
// keys of the ranges that begin strictly inside (lo, hi) and carry a
// sticky bit (expired or not: an expired bit is as well cleared).
func (s *Session) stickyBoundaries(ctx context.Context, lo, hi keys.Key) ([]keys.Key, error) {
	const page = 256
	_, metaEnd := keys.MetaSpan()
	var out []keys.Key
	// A range's record is keyed by its end key, so the first record past
	// lo's own is the range holding lo.
	for cursor := keys.Key(keys.RangeMetaKey(lo)).Next(); ; {
		rows, err := s.db.Scan(ctx, cursor, metaEnd, page)
		if err != nil {
			return nil, err
		}
		for _, kv := range rows {
			var d kvpb.RangeDescriptor
			if err := json.Unmarshal(kv.Value, &d); err != nil {
				return nil, fmt.Errorf("corrupt range addressing record at %s: %w", kv.Key, err)
			}
			if d.StartKey.Compare(hi) >= 0 {
				return out, nil
			}
			if d.StartKey.Compare(lo) > 0 && d.StickyUntil != 0 {
				out = append(out, d.StartKey)
			}
		}
		if len(rows) < page {
			return out, nil
		}
		cursor = rows[len(rows)-1].Key.Next()
	}
}

// releaseManualSplits stops holding every manual range boundary inside a
// table's data, for DROP TABLE and TRUNCATE: both leave the table's old
// key space empty for good (a truncated table continues under new index
// IDs), and a sticky boundary there would keep an empty range alive with
// no table left to name it in an UNSPLIT. Best-effort and outside the
// statement's transaction, like the splits themselves: a failure is
// logged and the boundary stays (datax debug unsplit can release it),
// and a statement that then rolls back has given up boundaries it could
// have kept — they merge only if their ranges are small, and SPLIT AT
// makes them again.
func (s *Session) releaseManualSplits(ctx context.Context, tableID uint64) {
	if tableID == 0 || !s.db.StickyBitsOK() {
		return
	}
	lo, hi := keys.TableDataSpan(tableID)
	sticky, err := s.stickyBoundaries(ctx, lo, hi)
	if err != nil {
		log.Debugf("releasing manual splits of table %d: %v", tableID, err)
		return
	}
	for _, key := range sticky {
		if _, err := s.db.AdminUnsplit(ctx, key); err != nil {
			log.Debugf("releasing manual split at %s: %v", keys.Pretty(key), err)
		}
	}
}

// splitTable resolves the table of a SPLIT AT or UNSPLIT and checks that
// its key space is one those statements carve. A nil descriptor with a
// nil error is IF EXISTS on a table that does not exist.
func (s *Session) splitTable(ctx context.Context, t *parser.AlterTable) (*catalog.TableDescriptor, error) {
	var desc *catalog.TableDescriptor
	if err := s.db.RunTxn(ctx, "split-at", func(ctx context.Context, txn *kvclient.Txn) error {
		shared, err := s.lookup(ctx, txn, t.Table)
		if err != nil {
			return err
		}
		desc = shared
		return nil
	}); err != nil {
		var nf *catalog.ErrTableNotFound
		if t.IfExists && asErr(err, &nf) {
			return nil, nil
		}
		return nil, err
	}
	if err := mustBeReal(desc); err != nil {
		return nil, err
	}
	if desc.ShardBuckets > 1 {
		return nil, newErrf(CodeFeatureNotSupported, "%q is sharded: its key space is carved by shard (ALTER TABLE ... SET (shards = N)), not by primary key", desc.Name)
	}
	return desc, nil
}

// splitKey encodes one SPLIT AT / UNSPLIT AT tuple as a key of desc's
// primary index; what names the statement in errors.
func splitKey(desc *catalog.TableDescriptor, what string, tuple []parser.Expr, params []types.Datum) (keys.Key, error) {
	if len(tuple) == 0 || len(tuple) > len(desc.PrimaryKey) {
		return nil, newErrf(CodeSyntaxError, "%s: a tuple names 1 to %d primary key values (%q has %d)", what, len(desc.PrimaryKey), desc.Name, len(desc.PrimaryKey))
	}
	key := rowenc.PrimaryKeyPrefixFor(desc)
	for i, e := range tuple {
		col, _ := desc.ColByID(desc.PrimaryKey[i])
		d, err := evalExpr(e, nil, nil, params)
		if err != nil {
			return nil, err
		}
		if d, err = col.Conform(d); err != nil {
			return nil, newErrf(CodeInvalidParameterValue, "%s: column %q: %v", what, col.Name, err)
		}
		if d.Null {
			return nil, newErrf(CodeSyntaxError, "%s: column %q: a split key cannot be NULL", what, col.Name)
		}
		if key, err = rowenc.AppendKeyDatum(key, col.Type, d); err != nil {
			return nil, newErrf(CodeInvalidParameterValue, "%s: column %q: %v", what, col.Name, err)
		}
	}
	return key, nil
}

// splitExpiration evaluates SPLIT AT ... WITH EXPIRATION e to the
// wall-clock time, in nanoseconds, at which the boundaries stop being
// held: a timestamp (or text that reads as one) is that moment, an
// interval is that long from now. It must lie in the future — a boundary
// that is already expired is a plain split, and saying so is kinder than
// making one silently.
func (s *Session) splitExpiration(e parser.Expr, params []types.Datum) (int64, error) {
	d, err := evalExpr(e, nil, nil, params)
	if err != nil {
		return 0, err
	}
	now := s.db.Clock().Now().WallTime
	var until int64
	switch {
	case d.Null:
		return 0, newErrf(CodeInvalidParameterValue, "SPLIT AT: the expiration cannot be NULL (leave WITH EXPIRATION out for a boundary that does not expire)")
	case d.Fam == types.Timestamp:
		until = d.I
	case d.Fam == types.Date:
		until = d.I * int64(24*time.Hour)
	case d.Fam == types.IntervalFam:
		until = d.IntervalVal().AddTo(time.Unix(0, now).UTC(), 1).UnixNano()
	case d.Fam == types.String:
		if until, err = types.ParseTimestamp(d.S); err != nil {
			iv, ierr := types.ParseInterval(d.S)
			if ierr != nil {
				return 0, newErrf(CodeInvalidParameterValue, "SPLIT AT: the expiration %q is neither a timestamp nor an interval", d.S)
			}
			until = iv.AddTo(time.Unix(0, now).UTC(), 1).UnixNano()
		}
	default:
		return 0, newErrf(CodeInvalidParameterValue, "SPLIT AT: the expiration must be a timestamp or an interval, not %s", d.Fam)
	}
	if until <= now {
		return 0, newErrf(CodeInvalidParameterValue, "SPLIT AT: the expiration %s is not in the future", time.Unix(0, until).UTC().Format(time.RFC3339))
	}
	return until, nil
}

func (s *Session) execDropTable(ctx context.Context, txn *kvclient.Txn, t *parser.DropTable) (*Result, error) {
	if _, bare := catalog.SplitTableName(t.Name); catalog.IsSealedSystemTable(bare) && !s.system {
		return nil, sealedTableError(bare)
	} else if catalog.IsSystemTable(bare) && !s.system {
		return nil, newErrf(CodeInsufficientPriv, "table %q belongs to the cluster and cannot be dropped (DELETE FROM it, or ALTER TABLE ... SET (retention = ...))", bare)
	}
	if existing, err := s.cat.LookupIn(ctx, txn, s.database, t.Name); err == nil && existing != nil {
		if existing.IsView() {
			return nil, newErrf(CodeWrongObjectType, "%q is a view (use DROP VIEW)", t.Name)
		}
		if err := s.checkTableOwner(ctx, txn, existing); err != nil {
			return nil, err
		}
		if err := s.dropDependentViews(ctx, txn, existing.ID, "table "+existing.Name, t.Cascade, "DROP TABLE ..."); err != nil {
			return nil, err
		}
		if err := s.dropTableConstraints(ctx, txn, existing, t.Cascade); err != nil {
			return nil, err
		}
	}
	desc, err := s.cat.DropIn(ctx, txn, s.database, t.Name)
	if err != nil {
		var nf *catalog.ErrTableNotFound
		if t.IfExists && asErr(err, &nf) {
			return &Result{Tag: "DROP TABLE"}, nil
		}
		return nil, err
	}
	// Statistics die with the table (same DDL txn; the background
	// sampler's orphan sweep is the backstop for anything missed), and so
	// do the sequences its columns own.
	if desc != nil {
		s.noteDrop(desc)
		if err := s.queueKeyDestroy(ctx, txn, desc); err != nil {
			return nil, err
		}
		s.releaseManualSplits(ctx, desc.ID)
		if err := txn.Delete(ctx, keys.TableStatsKey(desc.ID)); err != nil {
			return nil, err
		}
		seqs, err := catalog.ListSequences(ctx, txn, desc.DatabaseID)
		if err != nil {
			return nil, err
		}
		for _, sd := range seqs {
			if sd.OwnerTable == desc.ID {
				if err := catalog.DropSequence(ctx, txn, sd); err != nil {
					return nil, err
				}
			}
		}
	}
	return &Result{Tag: "DROP TABLE"}, nil
}

func (s *Session) execInsert(ctx context.Context, txn *kvclient.Txn, t *parser.Insert, params []types.Datum) (*Result, error) {
	if len(t.With) > 0 {
		restore, err := s.bindWith(ctx, txn, t.With, params, false, nil)
		if err != nil {
			return nil, err
		}
		defer restore()
		c := *t
		c.With = nil
		t = &c
	}
	if t.Select != nil {
		// INSERT ... SELECT: the source's rows insert as literal VALUES.
		src, err := s.execSelect(ctx, txn, t.Select, params)
		if err != nil {
			return nil, err
		}
		c := *t
		c.Select = nil
		c.Rows = make([][]parser.Expr, 0, len(src.Rows))
		for _, row := range src.Rows {
			exprs := make([]parser.Expr, len(row))
			for i := range row {
				d := row[i]
				exprs[i] = parser.Expr{Lit: &d}
			}
			c.Rows = append(c.Rows, exprs)
		}
		if len(c.Rows) == 0 {
			desc, err := s.lookup(ctx, txn, t.Table)
			if err != nil {
				return nil, err
			}
			if err := s.checkTablePriv(ctx, txn, desc, "INSERT"); err != nil {
				return nil, err
			}
			res := &Result{Tag: "INSERT 0 0"}
			if t.Returning != nil {
				ret, err := s.returningProjection(desc, t.Table, t.Returning)
				if err != nil {
					return nil, err
				}
				for _, p := range ret.proj {
					res.Columns = append(res.Columns, colResult(p.name, p.col))
				}
				res.Rows = [][]types.Datum{}
			}
			return res, nil
		}
		t = &c
	}
	t, err := s.resolveInsertSubs(ctx, txn, t, params)
	if err != nil {
		return nil, err
	}
	desc, err := s.lookup(ctx, txn, t.Table)
	if err != nil {
		return nil, err
	}
	if err := mustBeReal(desc); err != nil {
		return nil, err
	}
	if err := s.checkTablePriv(ctx, txn, desc, "INSERT"); err != nil {
		return nil, err
	}
	target, err := resolveInsertTargets(desc, t.Columns)
	if err != nil {
		return nil, err
	}
	// GENERATED ALWAYS AS IDENTITY refuses an explicit value unless the
	// statement says OVERRIDING SYSTEM VALUE (a DEFAULT value is fine).
	rows := t.Rows
	if t.DefaultValues {
		rows = [][]parser.Expr{{}}
		target = nil
	}
	if t.Overriding != "system" {
		for i, c := range target {
			if c.Identity != "always" {
				continue
			}
			for _, r := range rows {
				if i < len(r) && !r[i].IsDefault {
					return nil, newErrf(CodeGeneratedAlways, "cannot insert a non-DEFAULT value into column %q: it is an identity column defined as GENERATED ALWAYS (use OVERRIDING SYSTEM VALUE to override)", c.Name)
				}
			}
		}
	}
	ret, err := s.returningProjection(desc, t.Table, t.Returning)
	if err != nil {
		return nil, err
	}
	conflict, err := resolveConflict(desc, t, target)
	if err != nil {
		return nil, err
	}
	defaults, err := s.prepareDefaults(ctx, txn, desc, params)
	if err != nil {
		return nil, err
	}
	guard, err := s.guard(ctx, txn, desc)
	if err != nil {
		return nil, err
	}
	// Row security: every proposed row must pass the INSERT policies'
	// WITH CHECK, whether it ends up inserted or — through ON CONFLICT DO
	// UPDATE — only names the row to update; that path checks the UPDATE
	// policies too. With RETURNING, or a DO UPDATE (which reads the
	// existing row), a written row must also be one the role may see.
	doUpdate := conflict != nil && !conflict.oc.DoNothing
	rowSec, err := s.rowCheckFor(ctx, txn, desc, true, doUpdate, t.Returning != nil || doUpdate)
	if err != nil {
		return nil, err
	}
	var conflictVisible []parser.Comparison
	if rowSec != nil && doUpdate {
		if conflictVisible, _, err = s.rowFilter(ctx, txn, desc, catalog.PolicyUpdate, catalog.PolicySelect); err != nil {
			return nil, err
		}
	}
	count := 0
	var wb kvclient.WriteBatch
	inserted := map[string]bool{} // duplicates within this statement
	res := &Result{}
	// Every row is built first, so the statement's point reads — the
	// primary-key and unique-index uniqueness probes and the foreign-key
	// parent lookups — go out as one batch before any row is written
	// (issue #103); the per-row pipeline then answers from that batch.
	type builtRow struct {
		target []catalog.Column
		vals   []types.Datum
		row    map[catalog.ColumnID]types.Datum
		key    keys.Key
	}
	built := make([]builtRow, 0, len(rows))
	for _, exprRow := range rows {
		if len(exprRow) != len(target) {
			return nil, newErrf(CodeSyntaxError, "INSERT has %d values but %d target columns", len(exprRow), len(target))
		}
		vals := make([]types.Datum, len(exprRow))
		for i, e := range exprRow {
			var d types.Datum
			if e.IsDefault {
				if d, err = s.defaultValue(ctx, txn, defaults, &target[i], params); err != nil {
					return nil, err
				}
			} else {
				if exprIsVolatile(e) {
					if e, err = s.spliceVolatile(ctx, txn, e, params); err != nil {
						return nil, err
					}
				}
				if d, err = evalExpr(e, nil, nil, params); err != nil {
					return nil, err
				}
			}
			d, cerr := coerceColumn(target[i], d)
			if cerr != nil {
				return nil, cerr
			}
			vals[i] = d
		}
		rowTarget, rowVals, err := s.expandDefaults(ctx, txn, desc, defaults, target, vals, params)
		if err != nil {
			return nil, err
		}
		row, key, err := buildInsertRow(desc, rowTarget, rowVals)
		if err != nil {
			return nil, err
		}
		built = append(built, builtRow{target: rowTarget, vals: rowVals, row: row, key: key})
	}
	var ps *probeSet
	if len(built) > 1 {
		probeKeys := make([]keys.Key, 0, len(built)*2)
		for _, b := range built {
			probeKeys = append(probeKeys, b.key)
			probeKeys = appendUniqueIndexKeys(probeKeys, desc, b.row)
			probeKeys = guard.appendParentKeys(ctx, txn, probeKeys, nil, b.row)
		}
		if ps, err = newProbeSet(ctx, txn, probeKeys); err != nil {
			return nil, err
		}
		guard.setProbe(ps)
	}
	for _, b := range built {
		rowTarget, rowVals := b.target, b.vals
		var row map[catalog.ColumnID]types.Datum
		// The policy check comes before any probe of the table: a row the
		// role may not write is refused as such, whether or not a row
		// with its key exists.
		if err := rowSec.checkInsert(b.row); err != nil {
			return nil, err
		}
		if conflict == nil {
			if row, err = insertRowReturning(ctx, s.rowSealer(), txn, desc, rowTarget, rowVals, &wb, inserted, ps); err != nil {
				return nil, err
			}
			if err := guard.checkInsert(ctx, txn, row, inserted); err != nil {
				return nil, err
			}
		} else {
			written, wrow, err := s.insertOnConflict(ctx, txn, desc, rowTarget, rowVals, conflict, params, &wb, inserted, guard, ps, rowSec, conflictVisible)
			if err != nil {
				return nil, err
			}
			if !written {
				continue
			}
			row = wrow
		}
		count++
		if ret != nil {
			out, err := ret.project(desc, row, params)
			if err != nil {
				return nil, err
			}
			res.Rows = append(res.Rows, out)
		}
	}
	if err := txn.RunBatch(ctx, &wb); err != nil {
		return nil, err
	}
	if ret != nil {
		res.Columns = ret.columns()
	}
	res.Tag = fmt.Sprintf("INSERT 0 %d", count)
	return res, nil
}

// conflictPlan is a resolved ON CONFLICT clause: which unique key
// arbitrates (the primary key, or one unique index), and what to do.
type conflictPlan struct {
	pk    bool                     // the arbiter is the primary key
	index *catalog.IndexDescriptor // else this unique index
	any   bool                     // DO NOTHING with no target: any unique key
	oc    *parser.OnConflict
}

// resolveConflict validates the ON CONFLICT clause (or UPSERT) against the
// table's unique keys: the target must name the primary key's columns
// or a unique index's (42P10 otherwise), or a constraint by name.
func resolveConflict(desc *catalog.TableDescriptor, t *parser.Insert, target []catalog.Column) (*conflictPlan, error) {
	oc := t.OnConflict
	if t.Upsert {
		// UPSERT: ON CONFLICT (primary key) DO UPDATE SET every target
		// column = excluded.column.
		oc = &parser.OnConflict{}
		for _, c := range target {
			if desc.IsPKCol(c.ID) {
				continue
			}
			oc.Set = append(oc.Set, parser.SetClause{Column: c.Name, Value: parser.Expr{Column: "excluded." + c.Name}})
		}
		return &conflictPlan{pk: true, oc: oc}, nil
	}
	if oc == nil {
		return nil, nil
	}
	for _, set := range oc.Set {
		col, ok := desc.Col(set.Column)
		if !ok {
			return nil, newErrf(CodeUndefinedColumn, "column %q does not exist", set.Column)
		}
		if desc.IsPKCol(col.ID) {
			return nil, newErrf(CodeFeatureNotSupported, "ON CONFLICT DO UPDATE cannot change primary key column %q", set.Column)
		}
	}
	plan := &conflictPlan{oc: oc}
	if oc.Constraint != "" {
		if oc.Constraint == desc.Name+"_pkey" {
			plan.pk = true
			return plan, nil
		}
		for i := range desc.Indexes {
			if desc.Indexes[i].Name == oc.Constraint && desc.Indexes[i].Unique {
				plan.index = &desc.Indexes[i]
				return plan, nil
			}
		}
		return nil, newErrf(CodeUndefinedObject, "constraint %q for table %q does not exist", oc.Constraint, desc.Name)
	}
	if oc.Columns == nil {
		plan.any = true // DO NOTHING without a target
		return plan, nil
	}
	ids := map[catalog.ColumnID]bool{}
	for _, name := range oc.Columns {
		col, ok := desc.Col(name)
		if !ok {
			return nil, newErrf(CodeUndefinedColumn, "column %q does not exist", name)
		}
		ids[col.ID] = true
	}
	same := func(cols []catalog.ColumnID) bool {
		n := 0
		for _, id := range cols {
			if c, ok := desc.ColByID(id); ok && c.Hidden {
				continue // the shard column is not a user-facing key column
			}
			if !ids[id] {
				return false
			}
			n++
		}
		return n == len(ids)
	}
	if same(desc.PrimaryKey) {
		plan.pk = true
		return plan, nil
	}
	for i := range desc.Indexes {
		// A partial unique index is inferred only from a conflict
		// target that repeats its predicate, which ON CONFLICT here
		// cannot write: as in PostgreSQL, such a target matches none.
		if desc.Indexes[i].Unique && !desc.Indexes[i].Computed() && same(desc.Indexes[i].ColumnIDs) {
			plan.index = &desc.Indexes[i]
			return plan, nil
		}
	}
	return nil, newErrf(CodeInvalidColumnReference, "there is no unique or exclusion constraint matching the ON CONFLICT specification")
}

// insertRowReturning is insertRow, handing back the row it wrote.
func insertRowReturning(ctx context.Context, b *rowseal.Boundary, txn *kvclient.Txn, desc *catalog.TableDescriptor, target []catalog.Column, vals []types.Datum, wb *kvclient.WriteBatch, inserted map[string]bool, ps *probeSet) (map[catalog.ColumnID]types.Datum, error) {
	row, _, err := buildInsertRow(desc, target, vals)
	if err != nil {
		return nil, err
	}
	if err := insertRow(ctx, b, txn, desc, target, vals, wb, inserted, false, ps); err != nil {
		return nil, err
	}
	return row, nil
}

// insertOnConflict inserts one row, or — when its primary key or a
// unique index entry already exists — applies the ON CONFLICT action.
// It reports whether a row was written (inserted or updated) and that
// row. A conflict on a unique key other than the arbiter is the usual
// unique violation, as in PostgreSQL.
func (s *Session) insertOnConflict(ctx context.Context, txn *kvclient.Txn, desc *catalog.TableDescriptor, target []catalog.Column, vals []types.Datum, plan *conflictPlan, params []types.Datum, wb *kvclient.WriteBatch, inserted map[string]bool, guard *rowGuard, ps *probeSet, rowSec *rowCheck, conflictVisible []parser.Comparison) (bool, map[catalog.ColumnID]types.Datum, error) {
	row, key, err := buildInsertRow(desc, target, vals)
	if err != nil {
		return false, nil, err
	}
	if inserted[string(key)] {
		if plan.oc.DoNothing {
			return false, nil, nil
		}
		return false, nil, newErrf(CodeCardinality, "ON CONFLICT DO UPDATE command cannot affect row a second time")
	}
	// Find the existing row the new one collides with: by primary key,
	// then by each unique index (whose entry value is the row's PK).
	var oldKey keys.Key
	var oldRow map[catalog.ColumnID]types.Datum
	arbiter := ""
	if existing, err := ps.get(ctx, txn, key); err != nil {
		return false, nil, err
	} else if existing != nil {
		oldKey = key
		if oldRow, err = decodeFullRow(ctx, s.rowSealer(), desc, key, existing); err != nil {
			return false, nil, err
		}
		arbiter = "pk"
	}
	if oldRow == nil {
		for i := range desc.Indexes {
			idx := &desc.Indexes[i]
			if !idx.Unique {
				continue
			}
			ekey, _, skip, err := rowenc.EncodeIndexEntry(desc, idx, row)
			if err != nil {
				return false, nil, invertedEntryErr(idx, err)
			}
			if skip {
				continue
			}
			existing, err := ps.get(ctx, txn, ekey)
			if err != nil {
				return false, nil, err
			}
			if existing == nil {
				continue
			}
			pk := append(rowenc.PrimaryKeyPrefixFor(desc), existing...)
			raw, err := txn.Get(ctx, pk)
			if err != nil {
				return false, nil, err
			}
			if raw == nil {
				continue // a dangling entry: let the insert path report it
			}
			oldKey = pk
			if oldRow, err = decodeFullRow(ctx, s.rowSealer(), desc, pk, raw); err != nil {
				return false, nil, err
			}
			arbiter = idx.Name
			break
		}
	}
	if oldRow == nil {
		// No conflict: a plain insert (the PK read above already ran).
		if err := insertRow(ctx, s.rowSealer(), txn, desc, target, vals, wb, inserted, true, ps); err != nil {
			return false, nil, err
		}
		if err := guard.checkInsert(ctx, txn, row, inserted); err != nil {
			return false, nil, err
		}
		return true, row, nil
	}
	matches := plan.any || (plan.pk && arbiter == "pk") || (plan.index != nil && arbiter == plan.index.Name)
	if !matches {
		if arbiter == "pk" {
			return false, nil, newErrf(CodeUniqueViolation, "duplicate key value violates unique constraint on %q", desc.Name)
		}
		return false, nil, newErrf(CodeUniqueViolation, "duplicate key value violates unique constraint %q", arbiter)
	}
	if plan.oc.DoNothing {
		return false, nil, nil
	}
	// DO UPDATE under row security: the existing row must be one the
	// role's UPDATE and SELECT policies admit. It is an error rather than
	// a skipped row, as in PostgreSQL — the statement asked to change a
	// row it may not touch — and it is checked before SET or WHERE can
	// compute anything from that row.
	if rowSec != nil {
		ok, err := matchesWhere(conflictVisible, desc, oldRow, nil)
		if err != nil {
			return false, nil, err
		}
		if !ok {
			return false, nil, rowSecurityError(desc)
		}
	}
	// DO UPDATE: SET and WHERE see the existing row by its columns and
	// the proposed one as excluded.*.
	env := conflictEnv{desc: desc, table: desc.Name, old: oldRow, excluded: row}
	if len(plan.oc.Where) > 0 {
		ok, err := condsHold(plan.oc.Where, env, params)
		if err != nil {
			return false, nil, err
		}
		if !ok {
			return false, nil, nil
		}
	}
	newRow := copyRow(oldRow)
	for _, set := range plan.oc.Set {
		col, _ := desc.Col(set.Column)
		d, err := evalExprEnv(set.Value, env, params)
		if err != nil {
			return false, nil, err
		}
		d, cerr := coerceColumn(col, d)
		if cerr != nil {
			return false, nil, cerr
		}
		if d.Null && col.NotNull {
			return false, nil, newErrf(CodeNotNullViolation, "null value in column %q violates not-null constraint", col.Name)
		}
		if d, err = enforceTypmod(col, d); err != nil {
			return false, nil, err
		}
		newRow[col.ID] = d
	}
	if err := rowSec.checkUpdate(newRow); err != nil {
		return false, nil, err
	}
	if err := guard.checkUpdate(ctx, txn, oldRow, newRow, wb); err != nil {
		return false, nil, err
	}
	if err := putRowValue(ctx, s.rowSealer(), desc, wb, oldKey, newRow); err != nil {
		return false, nil, err
	}
	if err := updateIndexEntries(ctx, txn, desc, oldRow, newRow, wb, inserted, ps); err != nil {
		return false, nil, err
	}
	inserted[string(oldKey)] = true
	return true, newRow, nil
}

// conflictEnv resolves names inside ON CONFLICT DO UPDATE: "excluded.c"
// is the proposed row, anything else the existing row.
type conflictEnv struct {
	desc          *catalog.TableDescriptor
	table         string
	old, excluded map[catalog.ColumnID]types.Datum
}

func (e conflictEnv) col(name string) (types.Datum, error) {
	q, bare := splitQualified(name)
	row := e.old
	switch q {
	case "excluded":
		row = e.excluded
	case "", e.table:
	default:
		return types.Datum{}, newErrf(CodeUndefinedTable, "missing FROM-clause entry for table %q", q)
	}
	col, ok := e.desc.Col(bare)
	if !ok {
		return types.Datum{}, newErrf(CodeUndefinedColumn, "column %q does not exist", name)
	}
	d, ok := row[col.ID]
	if !ok {
		d = types.DNull
	}
	return d, nil
}

// returning is a resolved RETURNING list: the projection over the rows a
// write statement has in hand.
type returning struct {
	proj []projCol
}

// returningProjection resolves RETURNING against the table (nil when
// the statement has no clause). Qualifiers naming the table are
// stripped, as for a single-table select.
func (s *Session) returningProjection(desc *catalog.TableDescriptor, table string, exprs []parser.SelectExpr) (*returning, error) {
	if exprs == nil {
		return nil, nil
	}
	sel := &parser.Select{Table: table, Exprs: append([]parser.SelectExpr(nil), exprs...), Limit: -1}
	if err := stripTableAlias(sel); err != nil {
		return nil, err
	}
	for _, se := range sel.Exprs {
		if se.Agg != "" || !se.Star && exprHasAgg(se.Expr) {
			return nil, newErrf(CodeGrouping, "aggregate functions are not allowed in RETURNING")
		}
		if !se.Star && exprHasSubquery(se.Expr) {
			return nil, newErrf(CodeFeatureNotSupported, "subqueries are not supported in RETURNING")
		}
	}
	proj, err := resolveProjection(desc, sel.Exprs)
	if err != nil {
		return nil, err
	}
	return &returning{proj: proj}, nil
}

func (r *returning) columns() []ResultColumn {
	cols := make([]ResultColumn, len(r.proj))
	for i, p := range r.proj {
		cols[i] = colResult(p.name, p.col)
	}
	return cols
}

func (r *returning) project(desc *catalog.TableDescriptor, row map[catalog.ColumnID]types.Datum, params []types.Datum) ([]types.Datum, error) {
	out := make([]types.Datum, len(r.proj))
	for i, p := range r.proj {
		if p.expr != nil {
			d, err := evalExpr(*p.expr, desc, row, params)
			if err != nil {
				return nil, err
			}
			out[i] = conformTo(d, p.col.Type)
			continue
		}
		d, ok := row[p.col.ID]
		if !ok {
			d = types.DNull
		}
		out[i] = d
	}
	return out, nil
}

// resolveInsertTargets resolves an INSERT/COPY target-column list: empty
// means every visible column in order. Hidden columns (the _shard bucket)
// are never targets: implicitly they are skipped, explicitly they error —
// the executor computes them.
func resolveInsertTargets(desc *catalog.TableDescriptor, cols []string) ([]catalog.Column, error) {
	if len(cols) == 0 {
		return desc.VisibleColumns(), nil
	}
	target := make([]catalog.Column, 0, len(cols))
	for _, name := range cols {
		col, ok := desc.Col(name)
		if !ok {
			return nil, newErrf(CodeUndefinedColumn, "column %q does not exist", name)
		}
		if col.Hidden {
			return nil, newErrf(CodeSyntaxError, "column %q is system-managed and cannot be inserted", name)
		}
		target = append(target, col)
	}
	return target, nil
}

// buildInsertRow completes a positional value row into the full column
// map — defaults, the hidden shard bucket, NOT NULL — and returns it with
// its primary-key encoding. Shared by INSERT's per-row path and COPY's
// batched-probe path.
func buildInsertRow(desc *catalog.TableDescriptor, target []catalog.Column, vals []types.Datum) (map[catalog.ColumnID]types.Datum, keys.Key, error) {
	row := make(map[catalog.ColumnID]types.Datum, len(desc.Columns))
	for i := range target {
		row[target[i].ID] = vals[i]
	}
	for _, col := range desc.Columns {
		d, ok := row[col.ID]
		if !ok {
			if col.Hidden && col.Name == rowenc.ShardColumnName && desc.ShardBuckets > 0 {
				// The shard bucket hashes the logical PK. It is the last
				// column in desc.Columns, so every logical PK datum is
				// already filled (and NOT-NULL-checked) by this point.
				logical := make([]types.Datum, len(desc.PrimaryKey)-1)
				for i, id := range desc.PrimaryKey[1:] {
					logical[i] = row[id]
				}
				sd, serr := rowenc.ShardBucket(desc, logical)
				if serr != nil {
					return nil, nil, serr
				}
				row[col.ID] = sd
				continue
			}
			// An omitted column takes its DEFAULT (NULL when none).
			d = types.DNull
			if col.Default != nil {
				d = *col.Default
			}
			row[col.ID] = d
		}
		if d.Null && col.NotNull {
			return nil, nil, newErrf(CodeNotNullViolation, "null value in column %q violates not-null constraint", columnDisplayName(col))
		}
		// Type modifiers (DECIMAL(p,s), widths, lengths, TIMESTAMP forms):
		// apply before the key encoding below — PK columns precede the
		// (last-positioned) hidden shard column, so the shard hash and
		// pkKey both see the stored form.
		if col.HasTypmod() {
			ed, eerr := enforceTypmod(col, row[col.ID])
			if eerr != nil {
				return nil, nil, eerr
			}
			row[col.ID] = ed
		}
	}
	key, verr := pkKey(desc, row)
	if verr != nil {
		return nil, nil, verr
	}
	return row, key, nil
}

// insertRow runs the per-row insert pipeline shared by INSERT and COPY on
// already-coerced datums: defaults, the hidden shard bucket, NOT NULL,
// primary-key uniqueness (intra-statement map + a committed read — skipped
// when the caller already probed the key, pkKnownAbsent), value encoding,
// the reshard shadow write, and secondary index entries — all buffered
// into wb. vals must be positionally parallel to target.
func insertRow(ctx context.Context, b *rowseal.Boundary, txn *kvclient.Txn, desc *catalog.TableDescriptor, target []catalog.Column, vals []types.Datum, wb *kvclient.WriteBatch, inserted map[string]bool, pkKnownAbsent bool, ps *probeSet) error {
	row, key, err := buildInsertRow(desc, target, vals)
	if err != nil {
		return err
	}
	if inserted[string(key)] {
		return newErrf(CodeUniqueViolation, "duplicate key value violates unique constraint on %q", desc.Name)
	}
	if !pkKnownAbsent {
		if existing, err := ps.get(ctx, txn, key); err != nil {
			return err
		} else if existing != nil {
			return newErrf(CodeUniqueViolation, "duplicate key value violates unique constraint on %q", desc.Name)
		}
	}
	// Writes are buffered; the caller flushes the batch (one routed batch —
	// one Raft proposal per touched range — per statement or chunk).
	if err := putRowValue(ctx, b, desc, wb, key, row); err != nil {
		return err
	}
	if err := addIndexEntries(ctx, txn, desc, row, wb, inserted, ps); err != nil {
		return err
	}
	inserted[string(key)] = true
	return nil
}

func pkKey(desc *catalog.TableDescriptor, row map[catalog.ColumnID]types.Datum) (keys.Key, error) {
	pkVals := make([]types.Datum, len(desc.PrimaryKey))
	for i, id := range desc.PrimaryKey {
		pkVals[i] = row[id]
	}
	return rowenc.EncodePK(desc, pkVals)
}

// fetchRows returns the full rows (colID → datum, including PK columns)
// matching the WHERE clause, along with their keys, in PK order.
type fetchedRow struct {
	key keys.Key
	row map[catalog.ColumnID]types.Datum
}

// fetchRows reads the rows of desc that match where, as a query would:
// when row security applies to the actor, only the rows its SELECT
// policies admit (rls.go). Every read made on behalf of a query — a
// join's sides, an aggregate's input, an index lookup — comes through
// here, which is what applies the policy to each table reference.
func (s *Session) fetchRows(ctx context.Context, txn *kvclient.Txn, desc *catalog.TableDescriptor, where []parser.Comparison, params []types.Datum, limit int64) ([]fetchedRow, accessPlan, error) {
	where, _, err := s.securedWhere(ctx, txn, desc, where, catalog.PolicySelect)
	if err != nil {
		return nil, accessPlan{}, err
	}
	return s.fetchRowsRaw(ctx, txn, desc, where, params, limit, nil)
}

// fetchRowsRaw reads the rows of desc that match where and nothing else:
// no row-security policy is added. Its callers either have put the
// policy in where themselves (UPDATE and DELETE, whose filter is their
// own command's) or must not apply one (the foreign-key checks and
// cascades, which see the whole of the other table). stmt is the
// statement whose plan may be cached (the UPDATE or DELETE as prepared);
// nil plans in full.
func (s *Session) fetchRowsRaw(ctx context.Context, txn *kvclient.Txn, desc *catalog.TableDescriptor, where []parser.Comparison, params []types.Datum, limit int64, stmt parser.Statement) ([]fetchedRow, accessPlan, error) {
	st, _ := s.cat.Stats(ctx, desc.ID)
	var pe *planEntry
	if stmt != nil {
		if e := s.plansFor().get(stmt, s.database); e != nil && e.valid(desc, st) {
			pe = e
		}
	}
	plan, err := s.planAccess(desc, st, where, params, pe)
	if err != nil {
		return nil, plan, err
	}
	if stmt != nil && pe == nil {
		if err := s.cachePlan(stmt, desc, st, nil, where); err != nil {
			return nil, plan, err
		}
	}
	start := time.Now()
	rows, err := s.executePlan(ctx, txn, desc, plan, where, params, limit)
	if err == nil && s.explain != nil {
		s.note("scan %s: %s; %d rows in %s", desc.Name, plan.String(), len(rows), explainDuration(time.Since(start)))
	}
	return rows, plan, err
}

func (s *Session) executePlan(ctx context.Context, txn *kvclient.Txn, desc *catalog.TableDescriptor, plan accessPlan, where []parser.Comparison, params []types.Datum, limit int64) ([]fetchedRow, error) {
	if desc.Virtual != "" {
		// A virtual table has no keys: whatever the plan, generate and
		// filter (its "primary key" is the hidden ordinal).
		return s.fetchVirtual(ctx, txn, desc, where, params, limit)
	}
	if top := plan.top; top != nil {
		if top.vec != nil {
			return s.executeVectorScan(ctx, txn, desc, top.vec, where, params, top.keep)
		}
		return s.scanTopK(ctx, txn, desc, plan, where, top.order, params, top.keep)
	}
	switch plan.kind {
	case planPKPoint:
		key, err := rowenc.EncodePK(desc, plan.pkVals)
		if err != nil {
			return nil, err
		}
		noteIndexScan(desc, desc.LivePrimaryIndex())
		rows, err := s.fetchByPrimaryKey(ctx, txn, desc, key, where, params)
		noteIndexRows(desc, desc.LivePrimaryIndex(), len(rows))
		return rows, err

	case planPKPoints:
		return s.fetchPKPoints(ctx, txn, desc, plan, where, params, limit)

	case planUniquePoint:
		key, err := rowenc.EncodeIndexPrefix(desc, plan.idx, plan.idxVals)
		if err != nil {
			return nil, err
		}
		noteIndexScan(desc, plan.idx.ID)
		pkEnc, err := txn.Get(ctx, key)
		if err != nil || pkEnc == nil {
			return nil, err
		}
		noteIndexRows(desc, plan.idx.ID, 1)
		pk := append(rowenc.PrimaryKeyPrefixFor(desc), pkEnc...)
		rows, err := s.fetchByPrimaryKey(ctx, txn, desc, pk, where, params)
		if err != nil {
			return nil, err
		}
		if rows == nil && kvclient.LockWaitFrom(ctx) != kvclient.LockWaitSkip {
			// The entry and the row commit atomically; a dangling entry is
			// corruption, not an empty result — unless the row was filtered.
			// (Under SKIP LOCKED a locked row reads as absent, so absence
			// proves nothing there.)
			if raw, gerr := txn.Get(ctx, pk); gerr == nil && raw == nil {
				return nil, newErrf(CodeInternal, "index %q entry points at a missing row", plan.idx.Name)
			}
		}
		return rows, nil

	case planInvertedScan:
		return s.executeInvertedScan(ctx, txn, desc, plan, where, params, limit)

	case planIndexScan:
		prefix, err := rowenc.EncodeIndexPrefix(desc, plan.idx, plan.idxVals)
		if err != nil {
			return nil, err
		}
		var fam types.Family
		if plan.hasBounds() {
			fam, _ = plan.idx.KeyFamily(desc, len(plan.idxVals))
		}
		start, end, err := plan.spanBounds(prefix, fam)
		if err != nil {
			return nil, err
		}
		// Index entries are 1:1 with rows, so with no residual filter every
		// scanned entry yields a result row: the limit pushes into the scan.
		// Under SKIP LOCKED it does not quite: the lock is on the primary
		// row, not on the entry, so the join passes over the row behind an
		// entry it scanned (issue #322). The scan then goes on past its
		// last entry for the rows still wanted, until the limit is met or
		// the span runs out — the limit counts rows returned, and a short
		// result means there was no more to return (fetchSkipLocked
		// relies on that to stop).
		var scanLimit int64
		if len(plan.residual) == 0 {
			scanLimit = limit
		}
		skip := kvclient.LockWaitFrom(ctx) == kvclient.LockWaitSkip
		noteIndexScan(desc, plan.idx.ID)
		mark := txn.ReadMark()
		var (
			out              []fetchedRow
			passed           []keys.Key // entries of rows passed over as locked
			fetched, batches int
			met              bool
		)
		lo, hi := start, end
		for {
			kvs, err := spanScan(ctx, txn, lo, hi, scanLimit, plan.reverse)
			if err != nil {
				return nil, err
			}
			metrics.SQLRowsScanned.Add(float64(len(kvs)))
			s.AddScanned(int64(len(kvs)))
			noteIndexRows(desc, plan.idx.ID, len(kvs))
			// The primary rows behind the entries come back in pages of
			// primaryFetchPage, each page one routed batch, in the index's
			// order (issue #103): a lookup matching 1,000 entries is four
			// round trips instead of 1,000.
			pks := make([]keys.Key, len(kvs))
			for i, kv := range kvs {
				pk, err := rowenc.IndexEntryPrimaryKey(desc, plan.idx, kv.Key, kv.Value)
				if err != nil {
					return nil, newErrf(CodeInternal, "%v", err)
				}
				pks[i] = pk
			}
			fetched += len(pks)
			err = fetchPrimaryRows(ctx, txn, pks, func(first int, raws [][]byte) (bool, error) {
				batches++
				for i, raw := range raws {
					pk := pks[first+i]
					if raw == nil {
						if skip {
							// Locked by another transaction: passed over.
							passed = append(passed, kvs[first+i].Key)
							continue
						}
						return false, newErrf(CodeInternal, "index %q entry points at a missing row", plan.idx.Name)
					}
					row, err := decodeFullRow(ctx, s.rowSealer(), desc, pk, raw)
					if err != nil {
						return false, err
					}
					match, err := matchesWhere(where, desc, row, params)
					if err != nil {
						return false, err
					}
					if !match {
						continue
					}
					if err := s.chargeRow(row); err != nil {
						return false, err
					}
					out = append(out, fetchedRow{key: pk, row: row})
					if limit > 0 && int64(len(out)) == limit {
						// The entries after this one, and the primary rows
						// of this page behind them, cannot change the
						// result (see forgetScanTail).
						met = true
						if at := first + i; at+1 < len(kvs) {
							forgetScanTail(txn, mark, start, end, kvs[at].Key, plan.reverse)
							txn.ForgetReads(mark, pks[at+1:first+len(raws)])
						}
						return false, nil
					}
				}
				return true, nil
			})
			if err != nil {
				return nil, err
			}
			if met || !skip || scanLimit == 0 || int64(len(kvs)) < scanLimit {
				break
			}
			// A full page, short of the limit by the rows passed over:
			// the next page starts past its last entry, sized for the
			// rows still wanted, and grows while rows keep being passed
			// over so a long locked run costs few round trips.
			last := kvs[len(kvs)-1].Key
			if plan.reverse {
				hi = last
			} else {
				lo = last.Next()
			}
			want := limit - int64(len(out))
			if scanLimit *= skipLockedGrowth; scanLimit < want {
				scanLimit = want
			}
			if scanLimit > primaryFetchPage {
				scanLimit = max(want, primaryFetchPage)
			}
		}
		// A row passed over is no part of the result, and the transaction
		// holding it is likely to change it, moving or deleting this
		// entry: its entry leaves the read set as the row itself did
		// (see Txn.ForgetReads), or that change would fail this
		// transaction's next refresh over a row it never returned.
		txn.ForgetReads(mark, passed)
		if s.explain != nil && fetched > 0 {
			s.note("index join: %d primary rows fetched in %d batches of up to %d", fetched, batches, primaryFetchPage)
		}
		return out, nil

	case planPKScan:
		// The plan's pinned prefix and bounds constrain the logical PK; on
		// a sharded table (fanBuckets > 0) that is PrimaryKey[1:], and the
		// scan runs once per bucket with the bucket value prepended.
		spans, err := pkScanSpans(desc, plan)
		if err != nil {
			return nil, err
		}
		if plan.fanBuckets == 0 {
			return s.scanPrimarySpan(ctx, txn, desc, plan, spans[0].start, spans[0].end, where, params, limit)
		}
		runs := make([][]fetchedRow, 0, plan.fanBuckets)
		for _, sp := range spans {
			// The limit is only an upper bound per span (each span alone
			// cannot yield more result rows than the global limit); the
			// global limit re-applies below — and, without mergeFan, the
			// caller re-applies it after sorting when there is an ORDER
			// BY, in which case it passes limit 0 here.
			rows, err := s.scanPrimarySpan(ctx, txn, desc, plan, sp.start, sp.end, where, params, limit)
			if err != nil {
				return nil, err
			}
			runs = append(runs, rows)
		}
		if plan.mergeFan {
			// Every fanned key shares a constant-length prefix (table +
			// index + 8-byte bucket); the suffix is the order-preserving
			// logical-PK encoding, so the merge compares raw suffix bytes.
			suffixAt := len(rowenc.PrimaryKeyPrefixFor(desc)) + 8
			return mergeFannedRows(runs, suffixAt, plan.reverse, limit), nil
		}
		var out []fetchedRow
		for _, rows := range runs {
			out = append(out, rows...)
		}
		if limit > 0 && int64(len(out)) > limit {
			out = out[:limit]
		}
		return out, nil
	}

	start, end := rowenc.PrimarySpanFor(desc)
	return s.scanPrimarySpan(ctx, txn, desc, plan, start, end, where, params, limit)
}

// fetchPKPoints runs a planPKPoints plan: the rows at its primary keys,
// looked up in key order (descending for a reverse plan) in pages of
// primaryFetchPage, filtered by the WHERE clause, up to limit. A key
// with no row — or, under SKIP LOCKED, a row another transaction holds
// — yields nothing. Once the limit is met the keys after the last row
// kept leave the read set, as the tail of a limited scan does.
func (s *Session) fetchPKPoints(ctx context.Context, txn *kvclient.Txn, desc *catalog.TableDescriptor, plan accessPlan, where []parser.Comparison, params []types.Datum, limit int64) ([]fetchedRow, error) {
	if len(plan.pkPoints) == 0 {
		return nil, nil
	}
	pks := make([]keys.Key, 0, len(plan.pkPoints))
	for _, vals := range plan.pkPoints {
		key, err := rowenc.EncodePK(desc, vals)
		if err != nil {
			return nil, err
		}
		pks = append(pks, key)
	}
	slices.SortFunc(pks, func(a, b keys.Key) int {
		if plan.reverse {
			return bytes.Compare(b, a)
		}
		return bytes.Compare(a, b)
	})
	pks = slices.CompactFunc(pks, func(a, b keys.Key) bool { return a.Equal(b) })
	noteIndexScan(desc, desc.LivePrimaryIndex())
	mark := txn.ReadMark()
	var out []fetchedRow
	err := fetchPrimaryRows(ctx, txn, pks, func(first int, raws [][]byte) (bool, error) {
		for i, raw := range raws {
			if raw == nil {
				continue // no such row, or one passed over as locked
			}
			pk := pks[first+i]
			row, err := decodeFullRow(ctx, s.rowSealer(), desc, pk, raw)
			if err != nil {
				return false, err
			}
			match, err := matchesWhere(where, desc, row, params)
			if err != nil {
				return false, err
			}
			if !match {
				continue
			}
			if err := s.chargeRow(row); err != nil {
				return false, err
			}
			out = append(out, fetchedRow{key: pk, row: row})
			if limit > 0 && int64(len(out)) == limit {
				txn.ForgetReads(mark, pks[first+i+1:first+len(raws)])
				return false, nil
			}
		}
		return true, nil
	})
	if err != nil {
		return nil, err
	}
	metrics.SQLRowsScanned.Add(float64(len(out)))
	s.AddScanned(int64(len(out)))
	noteIndexRows(desc, desc.LivePrimaryIndex(), len(out))
	return out, nil
}

// spanScan runs one KV scan in the plan's direction.
func spanScan(ctx context.Context, txn *kvclient.Txn, start, end keys.Key, limit int64, reverse bool) ([]kvpb.KeyValue, error) {
	if reverse {
		return txn.ReverseScan(ctx, start, end, limit)
	}
	return txn.Scan(ctx, start, end, limit)
}

// mergeFannedRows K-way-merges per-bucket runs that are each already in
// logical-PK order (descending when reverse), comparing the key bytes
// beyond the constant prefix, and stops at limit. Linear best-pick per
// output row: bucket counts are small (≤ 256).
func mergeFannedRows(runs [][]fetchedRow, suffixAt int, reverse bool, limit int64) []fetchedRow {
	idx := make([]int, len(runs))
	var out []fetchedRow
	for {
		best := -1
		for r := range runs {
			if idx[r] >= len(runs[r]) {
				continue
			}
			if best < 0 {
				best = r
				continue
			}
			c := bytes.Compare(runs[r][idx[r]].key[suffixAt:], runs[best][idx[best]].key[suffixAt:])
			if (!reverse && c < 0) || (reverse && c > 0) {
				best = r
			}
		}
		if best < 0 {
			return out
		}
		out = append(out, runs[best][idx[best]])
		idx[best]++
		if limit > 0 && int64(len(out)) == limit {
			return out
		}
	}
}

// scanPrimarySpan scans [start, end) of the primary index, filters with the
// full WHERE clause, and stops at limit. The limit is pushed into the KV
// scan itself when the plan has no residual filter (every scanned row is a
// result row).
func (s *Session) scanPrimarySpan(ctx context.Context, txn *kvclient.Txn, desc *catalog.TableDescriptor, plan accessPlan, start, end keys.Key, where []parser.Comparison, params []types.Datum, limit int64) ([]fetchedRow, error) {
	var scanLimit int64
	if len(plan.residual) == 0 {
		scanLimit = limit
	}
	noteIndexScan(desc, desc.LivePrimaryIndex())
	mark := txn.ReadMark()
	kvs, err := spanScan(ctx, txn, start, end, scanLimit, plan.reverse)
	if err != nil {
		return nil, err
	}
	metrics.SQLRowsScanned.Add(float64(len(kvs)))
	s.AddScanned(int64(len(kvs)))
	noteIndexRows(desc, desc.LivePrimaryIndex(), len(kvs))
	var out []fetchedRow
	for i, kv := range kvs {
		row, err := decodeFullRow(ctx, s.rowSealer(), desc, kv.Key, kv.Value)
		if err != nil {
			return nil, err
		}
		match, err := matchesWhere(where, desc, row, params)
		if err != nil {
			return nil, err
		}
		if !match {
			continue
		}
		if err := s.chargeRow(row); err != nil {
			return nil, err
		}
		out = append(out, fetchedRow{key: kv.Key, row: row})
		if limit > 0 && int64(len(out)) == limit {
			if i+1 < len(kvs) {
				forgetScanTail(txn, mark, start, end, kv.Key, plan.reverse)
			}
			break
		}
	}
	return out, nil
}

// forgetScanTail takes the part of a scan of [start, end) beyond last —
// the last row a limited statement kept — out of the transaction's read
// set (issue #320). With a residual filter the limit cannot be pushed
// into the KV scan, which reads the whole span; but the rows after the
// limit-th match cannot change the statement's result, any more than
// they would have had the scan stopped there, and left in the read set
// every change another transaction commits among them fails this one's
// next refresh. A SKIP LOCKED queue (WHERE runnable ORDER BY id LIMIT 1)
// read every row of the queue that way and failed two thirds of its
// commits.
func forgetScanTail(txn *kvclient.Txn, mark int, start, end, last keys.Key, reverse bool) {
	if reverse {
		txn.ForgetSpan(mark, start, last)
		return
	}
	txn.ForgetSpan(mark, last.Next(), end)
}

// pkPointValues extracts PK datums when the WHERE clause pins every PK
// column with '='.
func pkPointValues(desc *catalog.TableDescriptor, where []parser.Comparison, params []types.Datum) ([]types.Datum, bool, error) {
	byCol := map[catalog.ColumnID]types.Datum{}
	for _, cmp := range where {
		if cmp.Op != "=" || len(cmp.Path) > 0 || cmp.Column == "" {
			continue
		}
		col, ok := desc.Col(cmp.Column)
		if !ok {
			return nil, false, newErrf(CodeUndefinedColumn, "column %q does not exist", cmp.Column)
		}
		if !desc.IsPKCol(col.ID) || col.Hidden {
			continue
		}
		d, err := evalExpr(cmp.Value, nil, nil, params)
		if err != nil {
			continue // row-dependent value (column RHS): not a point bound
		}
		d, cerr := coerceColumn(col, d)
		if cerr != nil {
			return nil, false, nil // un-coercible: cannot match anything via point path; fall to scan
		}
		if d.Null {
			continue // = NULL never matches; the scan+filter path returns nothing
		}
		byCol[col.ID] = d
	}
	// Sharded timeseries: the query pins the LOGICAL primary key; the
	// hidden leading _shard value is recomputed from it, so a fully-pinned
	// lookup stays a single point read.
	pkCols := desc.PrimaryKey
	if desc.ShardBuckets > 0 && len(pkCols) > 1 {
		pkCols = pkCols[1:]
	}
	if len(byCol) != len(pkCols) {
		return nil, false, nil
	}
	out := make([]types.Datum, len(pkCols))
	for i, id := range pkCols {
		out[i] = byCol[id]
	}
	if desc.ShardBuckets > 0 {
		bucket, err := rowenc.ShardBucket(desc, out)
		if err != nil {
			return nil, false, err
		}
		out = append([]types.Datum{bucket}, out...)
	}
	return out, true, nil
}

// TestingPanicBeforeExec, when set, runs before every statement executes
// — a test's way to inject a panic into the statement path.
var TestingPanicBeforeExec func(stmt parser.Statement)

// recoveredPanic turns a panic caught on the statement path into the
// statement's error: logged at error level with the goroutine's stack
// (the only evidence of the bug underneath), counted in
// datax_sql_statement_panics_total, reported as XX000.
func (s *Session) recoveredPanic(r any, stmt parser.Statement) *Error {
	metrics.SQLStatementPanics.Inc()
	log.Errorf("panic executing %T statement (recovered; the statement fails with XX000): %v\n%s", stmt, r, debug.Stack())
	return newErrf(CodeInternal, "internal error: %v", r)
}

// planAccess chooses the access path: a cached point shape bound to the
// parameters when the entry has one and the values bind, else the
// planner in full.
func (s *Session) planAccess(desc *catalog.TableDescriptor, st *catalog.TableStatistics, where []parser.Comparison, params []types.Datum, pe *planEntry) (accessPlan, error) {
	if pe != nil && pe.point != nil {
		plan, ok, err := bindPKPoint(desc, pe.point, where, params)
		if err != nil {
			return accessPlan{}, err
		}
		if ok {
			return plan, nil
		}
	}
	return pickPlanMode(desc, st, where, params, s.vars.invertedScan)
}

// cachePlan records what a statement planned against.
func (s *Session) cachePlan(stmt parser.Statement, desc *catalog.TableDescriptor, st *catalog.TableStatistics, proj []projCol, where []parser.Comparison) error {
	e := &planEntry{key: planKey{stmt: stmt, db: s.database}, desc: desc, stats: st, proj: proj, stripped: true}
	shape, ok, err := pkPointShape(desc, where)
	if err != nil {
		return err
	}
	if ok {
		e.point = shape
	}
	s.plansFor().put(e)
	return nil
}

// putRowValue buffers row's value at key, and at the in-flight
// re-shard's shadow key when there is one, each sealed for its own key
// when the table is sealed (pkg/sql/rowseal). Every write of a row value
// goes through here.
func putRowValue(ctx context.Context, b *rowseal.Boundary, desc *catalog.TableDescriptor, wb batchWriter, key keys.Key, row map[catalog.ColumnID]types.Datum) error {
	value, err := rowenc.EncodeValue(desc, row)
	if err != nil {
		return err
	}
	stored, err := b.SealValue(ctx, desc, key, value)
	if err != nil {
		return err
	}
	wb.Put(key, stored)
	if desc.Reshard != nil {
		shadow, err := reshardShadowKey(desc, row)
		if err != nil {
			return err
		}
		if stored, err = b.SealValue(ctx, desc, shadow, value); err != nil {
			return err
		}
		wb.Put(shadow, stored)
	}
	return nil
}

// rowSealer is the gateway's sealed-table boundary (nil without a key
// store, where a sealed table can be neither read nor written).
func (s *Session) rowSealer() *rowseal.Boundary { return rowseal.For(s.cat) }

// decodeFullRow decodes a primary row — its value opened first when the
// table is sealed — into every column, PK columns included. Every read
// of a row value goes through here (or through openRowValue).
func decodeFullRow(ctx context.Context, b *rowseal.Boundary, desc *catalog.TableDescriptor, key keys.Key, value []byte) (map[catalog.ColumnID]types.Datum, error) {
	value, err := b.OpenValue(ctx, desc, key, value)
	if err != nil {
		return nil, err
	}
	row, err := rowenc.DecodeValue(desc, value)
	if err != nil {
		return nil, err
	}
	pkVals, err := rowenc.DecodePK(desc, key)
	if err != nil {
		return nil, err
	}
	for i, id := range desc.PrimaryKey {
		row[id] = pkVals[i]
	}
	// Stamp DECIMAL(p,s) columns with their declared display scale so
	// projections render fixed-scale ("9.90"), CHAR(n) columns with their
	// padding and TIMESTAMP (without time zone) columns with the no-
	// offset rendering. After the PK overwrite, so key-decoded PK values
	// are stamped too. Display-only: canonical text in S / the nanos in I
	// stay the comparison/storage identity.
	for i := range desc.Columns {
		col := &desc.Columns[i]
		if col.Precision > 0 || col.Char || col.NoTZ || col.Type.IsArray() {
			if d, ok := row[col.ID]; ok && !d.Null {
				row[col.ID] = stampDisplay(col, d)
			}
		}
	}
	return row, nil
}

func (s *Session) execSelect(ctx context.Context, txn *kvclient.Txn, t *parser.Select, params []types.Datum) (*Result, error) {
	// Only the statement's own select may stream; every select run
	// underneath (WITH members, derived tables, set members, subqueries)
	// sees the flag cleared and materializes.
	top := s.streamTop
	s.streamTop = false
	// The plan cache keys on the statement as prepared: a select that is
	// rewritten below (WITH, derived tables, spliced subqueries, LIMIT
	// parameters) plans in full.
	orig := t
	if len(t.With) > 0 {
		restore, err := s.bindWith(ctx, txn, t.With, params, false, nil)
		if err != nil {
			return nil, err
		}
		defer restore()
		c := *t
		c.With = nil
		t = &c
	}
	if t.Derived != nil && len(t.Joins) > 0 {
		// A joined derived table runs as a relation named by its alias.
		inner, err := s.execSubSelect(ctx, txn, t.Derived, params)
		if err != nil {
			return nil, s.fromSubErr(ctx, txn, t.Derived, err)
		}
		name := strings.ToLower(t.Alias)
		desc, err := relationDesc(name, inner.Columns, nil)
		if err != nil {
			return nil, err
		}
		prev, had := s.bindRelation(name, &relation{desc: desc, rows: inner.Rows})
		defer s.restoreRelations(map[string]*relation{name: func() *relation {
			if had {
				return prev
			}
			return nil
		}()})
		c := *t
		c.Derived, c.Table = nil, name
		t = &c
	}
	if hasDerivedJoin(t) {
		bound, restore, err := s.bindJoinedDerived(ctx, txn, t, params, false)
		if err != nil {
			return nil, err
		}
		defer restore()
		t = bound
	}
	if t.LimitParam > 0 || t.OffsetParam > 0 {
		var err error
		if t, err = resolveLimitParams(t, params); err != nil {
			return nil, err
		}
	}
	// A single table's columns are addressed by bare name from here on,
	// whatever the select's shape (grouped, windowed, plain): t.c and
	// alias.c become c.
	if err := stripTableAlias(t); err != nil {
		return nil, err
	}
	if len(t.DistinctOn) > 0 {
		if err := checkDistinctOn(t); err != nil {
			return nil, err
		}
		if hasWindows(t.Exprs) {
			return nil, newErrf(CodeFeatureNotSupported, "SELECT DISTINCT ON with window functions is not supported")
		}
	}
	if hasWindows(t.Exprs) {
		return s.execWindowed(ctx, txn, t, params)
	}
	if res, ok := s.emptyCatalogSelect(ctx, txn, t); ok {
		return res, nil
	}
	for _, jc := range t.Joins {
		if jc.FuncTable != nil {
			return nil, newErrf(CodeFeatureNotSupported, "table function %s cannot be joined", jc.FuncTable.Func)
		}
	}
	if t.Union != nil {
		return s.execSetOps(ctx, txn, t, params)
	}
	// Correlated conjuncts leave the WHERE clause before eager subquery
	// resolution and access planning ever see them; they come back as a
	// per-row filter after the fetch. Only the plain single-table path
	// supports them.
	var corr []correlatedConjunct
	var corrProjs []corrProj
	var grouped *groupedCorr
	var post *postSort
	var jc *joinCorr
	if t.Table != "" && t.Derived == nil {
		if outer, rowOf, ok := s.selectScope(ctx, txn, t); ok {
			plannable, cc, serr := s.splitCorrelatedWhereScope(ctx, txn, t.Where, outer)
			if serr != nil {
				return nil, serr
			}
			if len(cc) > 0 {
				c := *t
				c.Where = plannable
				t, corr = &c, cc
			}
			if !hasAggregates(t.Exprs) && len(t.GroupBy) == 0 {
				cp, c, perr := s.splitCorrelatedProj(ctx, txn, t, outer)
				if perr != nil {
					return nil, perr
				}
				t, corrProjs = c, cp
				if len(t.Joins) == 0 {
					corrIdx := map[int]bool{}
					for _, p := range corrProjs {
						corrIdx[p.idx] = true
					}
					c, ps, oerr := s.splitCorrelatedOrder(ctx, txn, t, outer, corrIdx)
					if oerr != nil {
						return nil, oerr
					}
					if ps != nil {
						// The hidden sort items correlate too.
						more, c2, perr := s.splitCorrelatedProj(ctx, txn, c, outer)
						if perr != nil {
							return nil, perr
						}
						t, post = c2, ps
						corrProjs = append(corrProjs, more...)
					}
				}
			} else {
				g, c, gerr := s.splitCorrelatedGrouped(ctx, txn, t, outer, params)
				if gerr != nil {
					return nil, gerr
				}
				t, grouped = c, g
			}
			if len(t.Joins) > 0 && (len(corr) > 0 || len(corrProjs) > 0 || grouped != nil) {
				jc = &joinCorr{conds: corr, projs: corrProjs, grouped: grouped, desc: outer.desc, rowOf: rowOf}
			}
		}
	}
	t, err := s.resolveSelectSubs(ctx, txn, t, params)
	if err != nil {
		return nil, err
	}
	if t.Derived != nil {
		return s.execDerivedSelect(ctx, txn, t, params)
	}
	if t.FuncTable != nil {
		return s.execFuncTableSelect(ctx, txn, t, params)
	}
	if t.Table == "" {
		// FROM-less SELECT: one row of evaluated expressions — or, with
		// an unnest(array) among them, one row per element (the other
		// expressions repeated; SELECT unnest($1::int8[]) expands a
		// parameter into rows). A WHERE clause decides whether the row is
		// there at all; when it is not, nothing is evaluated.
		keep, werr := matchesWhere(t.Where, &catalog.TableDescriptor{}, nil, params)
		if werr != nil {
			return nil, werr
		}
		if len(t.Having) > 0 {
			return nil, newErrf(CodeFeatureNotSupported, "HAVING without a FROM clause is not supported")
		}
		res := &Result{}
		var row []types.Datum
		var sets []selectSet
		for i, se := range t.Exprs {
			if se.Star {
				return nil, newErrf(CodeSyntaxError, "SELECT * requires a FROM clause")
			}
			name := se.Alias
			if name == "" {
				name = "?column?"
			}
			if fn, ok := selectSRF(se.Expr); ok && keep {
				if name == "?column?" {
					name = fn
				}
				set, col, err := s.selectSetValues(ctx, txn, se.Expr, name, params)
				if err != nil {
					return nil, err
				}
				set.at = i
				res.Columns = append(res.Columns, col)
				sets = append(sets, set)
				row = append(row, types.DNull)
				continue
			}
			d := types.DNull
			if keep {
				var err error
				if d, err = evalExpr(se.Expr, nil, nil, params); err != nil {
					return nil, err
				}
			}
			// Typed as described (Describe sees the expression, not the
			// value); the value is conformed to it.
			fam := exprFamily(se.Expr, nil)
			if fam == types.Unknown {
				fam = d.Fam
				if d.Null {
					fam = types.String
				}
			}
			res.Columns = append(res.Columns, ResultColumn{Name: name, Type: fam, NoTZ: exprNoTZ(se.Expr, nil)})
			row = append(row, conformTo(d, fam))
		}
		rows := [][]types.Datum{row}
		if !keep {
			rows = nil
		}
		if len(sets) > 0 {
			// Set-returning calls in the list run in lockstep, the
			// shorter ones padded with NULLs (PostgreSQL 10's rule); the
			// other items repeat on every row.
			n := 0
			for _, st := range sets {
				n = max(n, len(st.vals))
			}
			rows = make([][]types.Datum, 0, n)
			for k := 0; k < n; k++ {
				r := append([]types.Datum(nil), row...)
				for _, st := range sets {
					if k < len(st.vals) {
						r[st.at] = st.vals[k]
					}
				}
				rows = append(rows, r)
			}
		}
		res.Rows = trimRows(rows, t)
		res.Tag = fmt.Sprintf("SELECT %d", len(res.Rows))
		return res, nil
	}

	desc, err := s.lookup(ctx, txn, t.Table)
	if err != nil {
		return nil, err
	}
	if t.DMLTarget == "" || t.Alias != t.DMLTarget {
		// (An UPDATE's or DELETE's target heading its join with the
		// FROM list needs that statement's privilege only.)
		if err := s.checkTablePriv(ctx, txn, desc, "SELECT"); err != nil {
			return nil, err
		}
	}
	if len(t.Joins) > 0 {
		return s.execJoinSelect(ctx, txn, desc, t, params, jc)
	}
	if hasAggregates(t.Exprs) || len(t.GroupBy) > 0 {
		if t.ForUpdate {
			return nil, newErrf(CodeFeatureNotSupported, "FOR UPDATE is not allowed with GROUP BY or aggregate functions")
		}
		return s.execGroupedSelect(ctx, txn, desc, t, params, corr, grouped)
	}
	if len(t.Having) > 0 {
		return nil, newErrf(CodeGrouping, "HAVING requires GROUP BY or aggregate functions")
	}
	if t.Distinct && t.ForUpdate {
		return nil, newErrf(CodeFeatureNotSupported, "FOR UPDATE is not allowed with DISTINCT")
	}
	// Row security: the policy's conjuncts lead the WHERE clause, so the
	// access path is chosen with them and every row read below — by key,
	// through an index, streamed, or passed over by SKIP LOCKED — is
	// filtered by them. A locking read must pass the UPDATE policies as
	// well. The filtered statement is a copy, which also keeps it out of
	// the plan cache (keyed on the statement as prepared).
	rlsCmds := []string{catalog.PolicySelect}
	if t.ForUpdate {
		rlsCmds = append(rlsCmds, catalog.PolicyUpdate)
	}
	if where, secured, serr := s.securedWhere(ctx, txn, desc, t.Where, rlsCmds...); serr != nil {
		return nil, serr
	} else if secured {
		c := *t
		c.Where = where
		t = &c
	}
	st, _ := s.cat.Stats(ctx, desc.ID)
	var pe *planEntry
	if t == orig {
		if e := s.plansFor().get(t, s.database); e != nil && e.valid(desc, st) {
			pe = e
		}
	}
	var proj []projCol
	if pe != nil {
		proj = pe.proj
	} else {
		if err := stripTableAlias(t); err != nil {
			return nil, err
		}
		var perr error
		if proj, perr = resolveProjection(desc, t.Exprs); perr != nil {
			return nil, perr
		}
	}

	// With ORDER BY the limit applies only after sorting (unless the access
	// path already delivers the requested order); with DISTINCT only after
	// deduplication.
	fetchLimit := keepCount(t)
	if t.Distinct || len(corr) > 0 || post != nil {
		// A correlated filter runs after the fetch, so the fetch itself
		// must not stop early; the limit re-applies to the survivors.
		fetchLimit = 0
	}
	plan, err := s.planAccess(desc, st, t.Where, params, pe)
	if err != nil {
		return nil, err
	}
	if pe == nil && t == orig {
		if err := s.cachePlan(t, desc, st, proj, t.Where); err != nil {
			return nil, err
		}
	}
	needSort := false
	order := foldDistanceConstants(desc, resolveOrderAliases(t), params)
	if len(order) > 0 {
		dec := orderPlan(desc, plan, order, s.db.ReverseScansOK())
		needSort = !dec.satisfied
		if needSort {
			fetchLimit = 0
		} else {
			// The access path delivers the order itself: reversed scans
			// for descending, and a K-way merge across shard buckets on a
			// fanned scan.
			plan.reverse, plan.mergeFan = dec.reverse, dec.mergeFan
		}
	}
	if s.streamable(top, desc, plan, t, needSort, corr, corrProjs) {
		return s.newScanStream(txn, desc, plan, t, proj, t.Where, params, fetchLimit)
	}
	var rows []fetchedRow
	if topKEligible(desc, plan, t, needSort, corr) {
		// ORDER BY ... LIMIT over a scan: only the rows that can still
		// make the limit are kept (topk.go) — read out of a vector index
		// when the order is a distance one serves, else out of the scan
		// itself. It is a directive on the plan, like the scan direction,
		// so the fetch below goes through executePlan as every fetch
		// does; the rows it returns then take the ordinary sort, over at
		// most the limit's worth of them.
		plan.top = &topKSpec{order: order, keep: keepCount(t), vec: s.pickVectorIndex(desc, plan, order, params)}
	}
	if t.ForUpdate && t.LockWait == parser.LockWaitSkipLocked {
		rows, err = s.fetchSkipLocked(ctx, txn, desc, plan, t, params, fetchLimit, corr, needSort, order)
		if err != nil {
			return nil, err
		}
	} else {
		nowait := t.ForUpdate && t.LockWait == parser.LockWaitNoWait
		fetchCtx := ctx
		if nowait {
			// The fetch must not wait either: a plain read queues behind
			// an intent at or below its timestamp.
			fetchCtx = kvclient.WithLockWait(ctx, kvclient.LockWaitError)
		}
		rows, err = s.executePlan(fetchCtx, txn, desc, plan, t.Where, params, fetchLimit)
		if err != nil {
			return nil, rowLockErr(desc, err)
		}
		if len(corr) > 0 {
			rows, err = s.filterCorrelated(ctx, txn, corr, desc, rows, params)
			if err != nil {
				return nil, err
			}
			if !needSort && !t.Distinct && post == nil {
				rows = cutRows(rows, keepCount(t))
			}
		}
		if t.ForUpdate {
			// Lock each selected row: the locking read re-verifies the row at
			// the transaction's read timestamp server-side (any newer committed
			// version surfaces as a retryable conflict), so the fetch-then-lock
			// gap cannot admit a stale read.
			lockKeys := make([]keys.Key, len(rows))
			for i, fr := range rows {
				lockKeys[i] = fr.key
			}
			var lerr error
			if nowait {
				_, lerr = txn.GetBatchForUpdateNoWait(ctx, lockKeys, false)
			} else {
				_, lerr = txn.GetBatchForUpdate(ctx, lockKeys)
			}
			if lerr != nil {
				return nil, rowLockErr(desc, lerr)
			}
		}
		if needSort {
			if err := sortRows(desc, rows, order, params); err != nil {
				return nil, err
			}
			s.note("sort: %d rows in memory", len(rows))
			if !t.Distinct {
				rows = cutRows(rows, keepCount(t))
			}
		}
	}
	if len(t.DistinctOn) > 0 {
		// The rows are in ORDER BY order: keep each DISTINCT ON group's
		// first (LIMIT applies after).
		if rows, err = keepFirstDistinct(rows, func(fr fetchedRow) ([]types.Datum, error) {
			return evalExprList(t.DistinctOn, tableEnv{desc: desc, row: fr.row}, params)
		}); err != nil {
			return nil, err
		}
	}
	res := &Result{}
	for _, p := range proj {
		res.Columns = append(res.Columns, colResult(p.name, p.col))
	}
	// Output positions of the correlated subqueries (SELECT * expands to
	// several projection columns ahead of them).
	projMemo := corrMemo{}
	projAt := make([]int, len(corrProjs))
	if len(corrProjs) > 0 {
		pos := make([]int, len(t.Exprs))
		n := 0
		for i, se := range t.Exprs {
			pos[i] = n
			if se.Star {
				n += len(desc.VisibleColumns())
			} else {
				n++
			}
		}
		for i, cp := range corrProjs {
			projAt[i] = pos[cp.idx]
		}
	}
	for _, fr := range rows {
		out := make([]types.Datum, len(proj))
		for i, p := range proj {
			if p.expr != nil {
				d, err := evalExpr(*p.expr, desc, fr.row, params)
				if err != nil {
					return nil, err
				}
				out[i] = conformTo(d, p.col.Type)
				continue
			}
			d, ok := fr.row[p.col.ID]
			if !ok {
				d = types.DNull
			}
			out[i] = d
		}
		for i := range corrProjs {
			cp := &corrProjs[i]
			d, err := s.evalCorrProj(ctx, txn, cp, desc, fr.row, params, projMemo)
			if err != nil {
				return nil, err
			}
			out[projAt[i]] = d
			if !d.Null {
				res.Columns[projAt[i]].Type = d.Fam
			}
		}
		if err := s.chargeDatums(out); err != nil {
			return nil, err
		}
		res.Rows = append(res.Rows, out)
	}
	if post != nil {
		// ORDER BY a correlated value: sorted now that it is computed.
		if err := sortResultRows(res.Columns, res.Rows, post.order, params); err != nil {
			return nil, err
		}
		s.note("sort: %d rows in memory", len(res.Rows))
		if post.hidden > 0 {
			visible := len(res.Columns) - post.hidden
			res.Columns = res.Columns[:visible]
			for i := range res.Rows {
				res.Rows[i] = res.Rows[i][:visible]
			}
		}
	}
	if t.Distinct && len(t.DistinctOn) == 0 {
		// Degenerate grouping over the projection: keep first occurrences
		// (rows are already in the requested order).
		res.Rows = dedupeRows(res.Rows)
	}
	res.Rows = trimRows(res.Rows, t)
	res.Tag = fmt.Sprintf("SELECT %d", len(res.Rows))
	return res, nil
}

func (s *Session) execUpdate(ctx context.Context, txn *kvclient.Txn, t *parser.Update, params []types.Datum) (*Result, error) {
	orig := t
	if len(t.With) > 0 {
		restore, err := s.bindWith(ctx, txn, t.With, params, false, nil)
		if err != nil {
			return nil, err
		}
		defer restore()
		c := *t
		c.With = nil
		t = &c
	}
	if len(t.From) > 0 {
		return s.execUpdateFrom(ctx, txn, t, params)
	}
	var err error
	if t, err = stripUpdateAlias(t); err != nil {
		return nil, err
	}
	corr, t2, err := s.splitCorrelatedUpdate(ctx, txn, t)
	if err != nil {
		return nil, err
	}
	t = t2
	// SET values whose subqueries read the row being updated are
	// evaluated per row (the rest are resolved once, below).
	setCorr, t, err := s.splitCorrelatedSet(ctx, txn, t)
	if err != nil {
		return nil, err
	}
	t, err = s.resolveUpdateSubs(ctx, txn, t, params)
	if err != nil {
		return nil, err
	}
	desc, err := s.lookup(ctx, txn, t.Table)
	if err != nil {
		return nil, err
	}
	if err := mustBeReal(desc); err != nil {
		return nil, err
	}
	if err := s.checkTablePriv(ctx, txn, desc, "UPDATE"); err != nil {
		return nil, err
	}
	pkSet, err := updateSetsPK(desc, t.Set)
	if err != nil {
		return nil, err
	}

	var planKeyStmt parser.Statement
	if t == orig {
		planKeyStmt = t
	}
	// Row security: the UPDATE policies' USING filters the target rows,
	// and — when the statement had to read the table to find or compute
	// them — so does the SELECT policies'. The new version of each row
	// must pass WITH CHECK below. A statement so filtered plans in full:
	// the plan cache cannot tell roles or settings apart.
	reads := updateReadsTable(t, corr) || len(setCorr) > 0
	cmds := []string{catalog.PolicyUpdate}
	if reads {
		cmds = append(cmds, catalog.PolicySelect)
	}
	where, secured, err := s.securedWhere(ctx, txn, desc, t.Where, cmds...)
	if err != nil {
		return nil, err
	}
	if secured {
		planKeyStmt = nil
	}
	rows, _, err := s.fetchRowsRaw(ctx, txn, desc, where, params, 0, planKeyStmt)
	if err != nil {
		return nil, err
	}
	if len(corr) > 0 {
		memo := corrMemo{}
		kept := rows[:0]
		for _, fr := range rows {
			match, cerr := s.evalCorrelated(ctx, txn, corr, desc, fr.row, params, memo)
			if cerr != nil {
				return nil, cerr
			}
			if match {
				kept = append(kept, fr)
			}
		}
		rows = kept
	}
	ret, err := s.returningProjection(desc, t.Table, t.Returning)
	if err != nil {
		return nil, err
	}
	setMemo := corrMemo{}
	u := updateApply{
		desc: desc, set: t.Set, rows: rows, pkSet: pkSet, reads: reads,
		eval: func(i, j int) (types.Datum, error) {
			if cp := setCorr[j]; cp != nil {
				return s.evalCorrProj(ctx, txn, cp, desc, rows[i].row, params, setMemo)
			}
			value := t.Set[j].Value
			if exprIsVolatile(value) {
				var err error
				if value, err = s.spliceVolatile(ctx, txn, value, params); err != nil {
					return types.Datum{}, err
				}
			}
			return evalExpr(value, desc, rows[i].row, params)
		},
	}
	if ret != nil {
		u.retCols = ret.columns()
		u.ret = func(_ int, row map[catalog.ColumnID]types.Datum) ([]types.Datum, error) {
			return ret.project(desc, row, params)
		}
	}
	return s.applyUpdate(ctx, txn, &u, params)
}

// updateSetsPK checks an UPDATE's SET columns against the target and
// reports whether one is a primary-key column (rows may move).
func updateSetsPK(desc *catalog.TableDescriptor, set []parser.SetClause) (bool, error) {
	pkSet := false
	for _, sc := range set {
		col, ok := desc.Col(sc.Column)
		if !ok {
			return false, newErrf(CodeUndefinedColumn, "column %q of relation %q does not exist", sc.Column, desc.Name)
		}
		if desc.IsPKCol(col.ID) {
			if col.Hidden {
				return false, newErrf(CodeFeatureNotSupported, "column %q is maintained by the system and cannot be updated", sc.Column)
			}
			pkSet = true
		}
	}
	return pkSet, nil
}

// updateApply is an UPDATE's target rows and how to compute what it
// writes: eval gives SET clause j's value for row i (DEFAULT is handled
// by applyUpdate), ret the RETURNING row for row i once updated.
type updateApply struct {
	desc  *catalog.TableDescriptor
	set   []parser.SetClause
	rows  []fetchedRow
	pkSet bool
	// reads: the statement read the table to find or compute its rows
	// (the new rows must then pass the SELECT policies too).
	reads   bool
	eval    func(i, j int) (types.Datum, error)
	ret     func(i int, row map[catalog.ColumnID]types.Datum) ([]types.Datum, error)
	retCols []ResultColumn
}

// applyUpdate writes an UPDATE's new rows: SET values, NOT NULL and type
// modifiers, moved primary keys, row security's WITH CHECK, unique keys
// and foreign keys checked for the statement as a whole, then the
// writes and RETURNING.
func (s *Session) applyUpdate(ctx context.Context, txn *kvclient.Txn, u *updateApply, params []types.Datum) (*Result, error) {
	desc, rows, pkSet := u.desc, u.rows, u.pkSet
	rowSec, err := s.rowCheckFor(ctx, txn, desc, false, true, u.reads)
	if err != nil {
		return nil, err
	}
	res := &Result{}
	var wb kvclient.WriteBatch
	defaults, err := s.prepareDefaults(ctx, txn, desc, params)
	if err != nil {
		return nil, err
	}
	guard, err := s.guard(ctx, txn, desc)
	if err != nil {
		return nil, err
	}
	// Every row's new values are computed first, so the statement's
	// point reads — the moved unique-index entries and the foreign-key
	// parents of changed columns — go out as one batch (issue #103).
	oldRows := make([]map[catalog.ColumnID]types.Datum, len(rows))
	for i, fr := range rows {
		oldRow := copyRow(fr.row)
		oldRows[i] = oldRow
		// Every SET is evaluated against the row as it was, and the
		// values are written after, so SET a = b, b = a swaps as in
		// PostgreSQL.
		pending := make([]types.Datum, len(u.set))
		for j, set := range u.set {
			col, _ := desc.Col(set.Column)
			var d types.Datum
			switch {
			case set.Value.IsDefault:
				if d, err = s.defaultValue(ctx, txn, defaults, &col, params); err != nil {
					return nil, err
				}
			default:
				if d, err = u.eval(i, j); err != nil {
					return nil, err
				}
			}
			d, cerr := coerceColumn(col, d)
			if cerr != nil {
				return nil, cerr
			}
			if d.Null && col.NotNull {
				return nil, newErrf(CodeNotNullViolation, "null value in column %q violates not-null constraint", col.Name)
			}
			d, cerr2 := enforceTypmod(col, d)
			if cerr2 != nil {
				return nil, cerr2
			}
			pending[j] = d
		}
		for j, set := range u.set {
			col, _ := desc.Col(set.Column)
			fr.row[col.ID] = pending[j]
		}
	}
	// The rows' keys after the update: a row whose primary key changes
	// moves (update_pk.go).
	oldKeys := make([]keys.Key, len(rows))
	newKeys := make([]keys.Key, len(rows))
	newRows := make([]map[catalog.ColumnID]types.Datum, len(rows))
	for i, fr := range rows {
		oldKeys[i], newKeys[i], newRows[i] = fr.key, fr.key, fr.row
		if !pkSet {
			continue
		}
		for _, id := range desc.PrimaryKey {
			if col, _ := desc.ColByID(id); rowVal(fr.row, id).Null {
				return nil, newErrf(CodeNotNullViolation, "null value in column %q violates not-null constraint", col.Name)
			}
		}
		if _, err := withShardBucket(desc, fr.row); err != nil {
			return nil, newErrf(CodeInternal, "shard bucket: %v", err)
		}
		if newKeys[i], err = pkKey(desc, fr.row); err != nil {
			return nil, err
		}
	}
	var ps *probeSet
	if len(rows) > 1 {
		var probeKeys []keys.Key
		for i, fr := range rows {
			if !newKeys[i].Equal(oldKeys[i]) {
				probeKeys = append(probeKeys, newKeys[i])
			}
			probeKeys = appendMovedUniqueIndexKeys(probeKeys, desc, oldRows[i], fr.row)
			probeKeys = guard.appendParentKeys(ctx, txn, probeKeys, oldRows[i], fr.row)
		}
		if len(probeKeys) > 0 {
			if ps, err = newProbeSet(ctx, txn, probeKeys); err != nil {
				return nil, err
			}
			guard.setProbe(ps)
		}
	}
	// The policy check comes before any probe of the table: a row the
	// role may not write is refused as such, whether or not its new key
	// is taken.
	for _, fr := range rows {
		if err := rowSec.checkUpdate(fr.row); err != nil {
			return nil, err
		}
	}
	// The statement's unique keys are checked together, so a row may
	// take a key another row of the statement gives up.
	if err := checkUpdateKeys(ctx, txn, desc, oldRows, newRows, oldKeys, newKeys, ps); err != nil {
		return nil, err
	}
	writes := newUpdateWrites()
	for i, fr := range rows {
		oldRow := oldRows[i]
		if err := guard.checkUpdate(ctx, txn, oldRow, fr.row, &wb); err != nil {
			return nil, err
		}
		if newKeys[i].Equal(fr.key) {
			if err := putRowValue(ctx, s.rowSealer(), desc, writes, fr.key, fr.row); err != nil {
				return nil, err
			}
			if err := updateIndexEntries(ctx, txn, desc, oldRow, fr.row, writes, nil, ps); err != nil {
				return nil, err
			}
		} else {
			// The row moves: its old version and entries go (a re-shard's
			// shadow copy too), the new one is written at its new key —
			// sealed for that key on a sealed table.
			if err := deleteRow(desc, fetchedRow{key: fr.key, row: oldRow}, writes); err != nil {
				return nil, err
			}
			if err := putRowValue(ctx, s.rowSealer(), desc, writes, newKeys[i], fr.row); err != nil {
				return nil, err
			}
			if err := addIndexEntries(ctx, txn, desc, fr.row, writes, nil, ps); err != nil {
				return nil, err
			}
		}
		if u.ret != nil {
			out, err := u.ret(i, fr.row)
			if err != nil {
				return nil, err
			}
			res.Rows = append(res.Rows, out)
		}
	}
	if err := guard.cascadeOverlap(desc, oldKeys, newKeys); err != nil {
		return nil, err
	}
	writes.flush(&wb)
	if err := txn.RunBatch(ctx, &wb); err != nil {
		return nil, err
	}
	if u.ret != nil {
		res.Columns = u.retCols
	}
	res.Tag = fmt.Sprintf("UPDATE %d", len(rows))
	return res, nil
}

func (s *Session) execDelete(ctx context.Context, txn *kvclient.Txn, t *parser.Delete, params []types.Datum) (*Result, error) {
	orig := t
	if len(t.With) > 0 {
		restore, err := s.bindWith(ctx, txn, t.With, params, false, nil)
		if err != nil {
			return nil, err
		}
		defer restore()
		c := *t
		c.With = nil
		t = &c
	}
	if len(t.Using) > 0 {
		return s.execDeleteUsing(ctx, txn, t, params)
	}
	var err error
	if t, err = stripDeleteAlias(t); err != nil {
		return nil, err
	}
	corr, t2, err := s.splitCorrelatedDelete(ctx, txn, t)
	if err != nil {
		return nil, err
	}
	t = t2
	t, err = s.resolveDeleteSubs(ctx, txn, t, params)
	if err != nil {
		return nil, err
	}
	desc, err := s.lookup(ctx, txn, t.Table)
	if err != nil {
		return nil, err
	}
	if err := mustBeReal(desc); err != nil {
		return nil, err
	}
	if err := s.checkTablePriv(ctx, txn, desc, "DELETE"); err != nil {
		return nil, err
	}
	var planKeyStmt parser.Statement
	if t == orig {
		planKeyStmt = t
	}
	// Row security: the DELETE policies' USING filters the target rows,
	// with the SELECT policies' when the statement reads the table (a
	// WHERE clause or RETURNING).
	cmds := []string{catalog.PolicyDelete}
	if len(t.Where) > 0 || len(corr) > 0 || t.Returning != nil {
		cmds = append(cmds, catalog.PolicySelect)
	}
	where, secured, err := s.securedWhere(ctx, txn, desc, t.Where, cmds...)
	if err != nil {
		return nil, err
	}
	if secured {
		planKeyStmt = nil
	}
	rows, _, err := s.fetchRowsRaw(ctx, txn, desc, where, params, 0, planKeyStmt)
	if err != nil {
		return nil, err
	}
	if len(corr) > 0 {
		memo := corrMemo{}
		kept := rows[:0]
		for _, fr := range rows {
			match, cerr := s.evalCorrelated(ctx, txn, corr, desc, fr.row, params, memo)
			if cerr != nil {
				return nil, cerr
			}
			if match {
				kept = append(kept, fr)
			}
		}
		rows = kept
	}
	ret, err := s.returningProjection(desc, t.Table, t.Returning)
	if err != nil {
		return nil, err
	}
	var project func(int, map[catalog.ColumnID]types.Datum) ([]types.Datum, error)
	var retCols []ResultColumn
	if ret != nil {
		retCols = ret.columns()
		project = func(_ int, row map[catalog.ColumnID]types.Datum) ([]types.Datum, error) {
			return ret.project(desc, row, params)
		}
	}
	return s.applyDelete(ctx, txn, desc, rows, project, retCols)
}

// applyDelete deletes rows of desc — their foreign-key actions, index
// entries and a re-shard's shadow copies with them — and projects
// RETURNING (ret, nil without one) from each deleted row.
func (s *Session) applyDelete(ctx context.Context, txn *kvclient.Txn, desc *catalog.TableDescriptor, rows []fetchedRow, ret func(int, map[catalog.ColumnID]types.Datum) ([]types.Datum, error), retCols []ResultColumn) (*Result, error) {
	guard, err := s.guard(ctx, txn, desc)
	if err != nil {
		return nil, err
	}
	res := &Result{}
	var wb kvclient.WriteBatch
	for i, fr := range rows {
		if err := guard.checkDelete(ctx, txn, fr.row, &wb); err != nil {
			return nil, err
		}
		wb.Delete(fr.key)
		if desc.Reshard != nil {
			shadow, serr := reshardShadowKey(desc, fr.row)
			if serr != nil {
				return nil, serr
			}
			wb.Delete(shadow)
		}
		if err := dropIndexEntries(desc, fr.row, &wb); err != nil {
			return nil, err
		}
		if ret != nil {
			out, err := ret(i, fr.row)
			if err != nil {
				return nil, err
			}
			res.Rows = append(res.Rows, out)
		}
	}
	if err := txn.RunBatch(ctx, &wb); err != nil {
		return nil, err
	}
	if ret != nil {
		res.Columns = retCols
	}
	res.Tag = fmt.Sprintf("DELETE %d", len(rows))
	return res, nil
}

// mustBeReal refuses a write or DDL against a virtual catalog table.
func mustBeReal(desc *catalog.TableDescriptor) error {
	if desc.Virtual != "" {
		return newErrf(CodeInsufficientPriv, "%s is a system catalog and cannot be modified", desc.Virtual)
	}
	if desc.IsView() {
		return newErrf(CodeWrongObjectType, "%q is a view", desc.Name)
	}
	return nil
}

func (s *Session) execAlterTable(ctx context.Context, txn *kvclient.Txn, t *parser.AlterTable) (*Result, error) {
	if _, bare := catalog.SplitTableName(t.Table); catalog.IsSystemTable(bare) && !s.system && t.SetOptions == nil {
		return nil, newErrf(CodeInsufficientPriv, "table %q belongs to the cluster: only ALTER TABLE ... SET (retention | shards) is allowed", t.Table)
	}
	if t.ResetOptions != nil || t.Purge || t.RotateKey {
		return nil, newErrf(CodeActiveTransaction, "ALTER TABLE ... RESET, PURGE and ROTATE ENCRYPTION KEY cannot run inside a transaction block")
	}
	shared, err := s.lookup(ctx, txn, t.Table)
	if err != nil {
		var nf *catalog.ErrTableNotFound
		if t.IfExists && asErr(err, &nf) {
			return &Result{Tag: "ALTER TABLE"}, nil
		}
		return nil, err
	}
	if err := mustBeReal(shared); err != nil {
		return nil, err
	}
	if err := s.checkTableOwner(ctx, txn, shared); err != nil {
		return nil, err
	}
	desc := shared.Clone()
	normalizeNextColumnID(desc)
	switch {
	case t.RenameTo != "":
		return s.execRenameTable(ctx, txn, desc, t)
	case t.RenameCol != nil:
		return s.execRenameColumn(ctx, txn, desc, t.RenameCol)
	case t.RenameConstraint != nil:
		return s.execRenameConstraint(ctx, txn, desc, t.RenameConstraint)
	case t.Cmds == nil && (t.AddConstraint != nil || t.ValidateConstraint != "" || t.SetNotNull != ""):
		return nil, newErrf(CodeActiveTransaction, "ALTER TABLE ... ADD CONSTRAINT, VALIDATE CONSTRAINT and SET NOT NULL cannot run inside a transaction block")
	}
	// The actions, in written order, applied to one copy of the
	// descriptor and written once (alter_multi.go). One that needs the
	// table's rows runs online, outside a transaction block: reaching it
	// here means the statement is in one.
	cmds := alterCmds(t)
	if err := checkAlterCombination(cmds); err != nil {
		return nil, err
	}
	for _, c := range cmds {
		if alterActionNeedsRows(c) {
			return nil, alterInBlockError(cmds)
		}
	}
	eff := newAlterEffects()
	for _, c := range cmds {
		if err := s.applyAlterAction(ctx, txn, desc, c, eff); err != nil {
			return nil, err
		}
	}
	if err := s.commitAlterEffects(ctx, txn, desc, eff); err != nil {
		return nil, err
	}
	return &Result{Tag: "ALTER TABLE", Notices: eff.notices}, nil
}

// execExplain describes the access plan of a SELECT without executing it.
func (s *Session) execExplain(ctx context.Context, txn *kvclient.Txn, t *parser.Explain, params []types.Datum) (*Result, error) {
	sel, ok := t.Stmt.(*parser.Select)
	if ok && sel.Table == "" && sel.Derived == nil && sel.FuncTable == nil && sel.Union == nil && len(sel.Joins) == 0 && len(sel.With) == 0 {
		// SELECT without FROM (SELECT $1::text, SELECT now()): one row
		// computed from its expressions, no table read. ANALYZE runs it
		// with the bound parameters.
		if t.Analyze {
			return s.execExplainAnalyze(ctx, txn, sel, params)
		}
		return &Result{
			Columns: []ResultColumn{{Name: "plan", Type: types.String}},
			Rows:    [][]types.Datum{{types.NewString("result: one row computed without reading a table")}},
			Tag:     "EXPLAIN",
		}, nil
	}
	if !ok || (sel.Table == "" && sel.Derived == nil) {
		return nil, newErrf(CodeFeatureNotSupported, "EXPLAIN supports table SELECT statements only")
	}
	if t.Analyze {
		return s.execExplainAnalyze(ctx, txn, sel, params)
	}
	// A plan shows a table's shape, its indexes and a row estimate drawn
	// from its statistics: it takes the privilege the statement would,
	// as in PostgreSQL. EXPLAIN ANALYZE runs the statement, which checks.
	if err := s.checkSelectReads(ctx, txn, sel, map[uint64]bool{}); err != nil {
		return nil, err
	}
	if len(sel.With) > 0 {
		// The members bind by shape only: EXPLAIN runs nothing.
		restore, err := s.bindWith(ctx, txn, sel.With, params, true, nil)
		if err != nil {
			return nil, err
		}
		defer restore()
		c := *sel
		c.With = nil
		sel = &c
	}
	if hasDerivedJoin(sel) {
		bound, restore, err := s.bindJoinedDerived(ctx, txn, sel, params, true)
		if err != nil {
			return nil, err
		}
		defer restore()
		sel = bound
	}
	// The plan reads a single table's columns by bare name, as
	// execution does.
	if err := stripTableAlias(sel); err != nil {
		return nil, err
	}
	windowed := ""
	if hasWindows(sel.Exprs) {
		wp, err := windowPlanFor(sel)
		if err != nil {
			return nil, err
		}
		sel = wp.inner
		windowed = fmt.Sprintf("; then %d window function(s) over the fetched rows", len(wp.items))
	}
	// Correlated conjuncts are stripped exactly as execution strips them,
	// so the plan below describes the plannable remainder.
	var corr []correlatedConjunct
	if sel.Table != "" && len(sel.Joins) == 0 && sel.Derived == nil {
		if cdesc, derr := s.lookup(ctx, txn, sel.Table); derr == nil {
			plannable, cc, serr := s.splitCorrelatedWhere(ctx, txn, sel.Where, cdesc, sel.Table, sel.Alias)
			if serr != nil {
				return nil, serr
			}
			if len(cc) > 0 {
				c := *sel
				c.Where = plannable
				sel, corr = &c, cc
			}
		}
	}
	// Correlated subqueries elsewhere (select list, HAVING, ORDER BY)
	// run per row or per group: the plan shows the statement's shape.
	corrNote := ""
	if sel.Table != "" && sel.Derived == nil {
		if outer, _, ok := s.selectScope(ctx, txn, sel); ok {
			cs, cerr := s.findCorrelatedSubs(ctx, txn, sel, outer)
			if cerr != nil {
				return nil, cerr
			}
			if cs != nil {
				shape, serr := cs.shape()
				if serr != nil {
					return nil, serr
				}
				corrNote = fmt.Sprintf("; %d correlated subquery(ies) outside WHERE: run per row or group", len(cs.nodes))
				if !hasAggregates(sel.Exprs) && len(sel.GroupBy) == 0 && cs.ordersBy() {
					// Sorted once the values are computed: no ordered or
					// top-k fetch.
					shape.OrderBy = nil
					corrNote += "; then a sort by them in memory"
				}
				sel = shape
			}
		}
	}
	// Subqueries are evaluated (in this transaction) so the plan reflects
	// the spliced values, exactly as execution will see them.
	sel, err := s.resolveSelectSubs(ctx, txn, sel, params)
	if err != nil {
		return nil, err
	}
	if sel.Derived != nil {
		return &Result{
			Columns: []ResultColumn{{Name: "plan", Type: types.String}},
			Rows:    [][]types.Datum{{types.NewString(fmt.Sprintf("materialized subquery (derived table %q)", sel.Alias))}},
			Tag:     "EXPLAIN",
		}, nil
	}
	desc, err := s.lookup(ctx, txn, sel.Table)
	if err != nil {
		return nil, err
	}
	if len(sel.Joins) > 0 {
		text, jerr := s.explainJoin(ctx, txn, desc, sel, params)
		if jerr != nil {
			return nil, jerr
		}
		text += corrNote
		return &Result{
			Columns: []ResultColumn{{Name: "plan", Type: types.String}},
			Rows:    [][]types.Datum{{types.NewString(text)}},
			Tag:     "EXPLAIN",
		}, nil
	}
	// Row security shows in the plan as it runs: the policy's conjuncts
	// bound the scan. The row estimate is left out for a role the policy
	// confines — it is drawn from statistics over every tenant's rows.
	rlsCmds := []string{catalog.PolicySelect}
	if sel.ForUpdate {
		rlsCmds = append(rlsCmds, catalog.PolicyUpdate)
	}
	where, secured, err := s.securedWhere(ctx, txn, desc, sel.Where, rlsCmds...)
	if err != nil {
		return nil, err
	}
	if secured {
		c := *sel
		c.Where = where
		sel = &c
	}
	st, _ := s.cat.Stats(ctx, desc.ID)
	plan, err := pickPlanMode(desc, st, sel.Where, params, s.vars.invertedScan)
	if err != nil {
		return nil, err
	}
	text := plan.String()
	if plan.estRows > 0 && !secured {
		text += fmt.Sprintf(" [~%.0f rows]", plan.estRows)
	}
	if secured {
		text += "; row-level security policy applied"
	} else if e := s.plans.peek(sel, s.database); e != nil && e.desc == desc && e.stats == st {
		text += " (cached plan)"
	} else if sel == t.Stmt {
		// EXPLAIN plans exactly as execution would: the next execution of
		// this statement (or of this EXPLAIN) reuses it.
		if err := s.cachePlan(sel, desc, st, nil, sel.Where); err != nil {
			return nil, err
		}
	}
	if len(corr) > 0 {
		text += fmt.Sprintf("; correlated filter: nested loop over %d conjunct(s) (O(outer rows x inner query), memoized per correlation key)", len(corr))
	}
	text += corrNote
	grouped := hasAggregates(sel.Exprs) || len(sel.GroupBy) > 0 || sel.Distinct
	dec := orderPlan(desc, plan, sel.OrderBy, s.db.ReverseScansOK())
	// The top-k paths, decided as execSelect decides them.
	topK := !grouped && topKEligible(desc, plan, sel, len(sel.OrderBy) > 0 && !dec.satisfied, corr)
	viaVector := false
	if topK {
		if vp := s.pickVectorIndex(desc, plan, resolveOrderAliases(sel), params); vp != nil {
			text = vp.String() + strings.TrimPrefix(text, plan.String())
			viaVector = true
		}
	}
	if len(sel.OrderBy) > 0 && !grouped {
		switch {
		case viaVector:
			text += fmt.Sprintf("; top-k: the nearest %d candidates that pass the filter kept, in index distance order", keepCount(sel))
		case topK:
			text += fmt.Sprintf("; top-k heap: the first %d rows in order kept while scanning, no full sort", keepCount(sel))
		case dec.satisfied && dec.mergeFan:
			text += "; order satisfied by K-way merge across shard buckets"
			if dec.reverse {
				text += " (reverse scans)"
			}
		case dec.satisfied && dec.reverse:
			text += "; order satisfied by access path (reverse scan)"
		case dec.satisfied:
			text += "; order satisfied by access path"
		default:
			text += "; in-memory sort"
		}
	}
	if sel.Limit > 0 && !grouped && len(plan.residual) == 0 &&
		(len(sel.OrderBy) == 0 || dec.satisfied) {
		switch plan.kind {
		case planFullScan, planPKScan, planIndexScan:
			text += "; limit pushed into scan"
		}
	}
	if sel.Offset > 0 {
		text += fmt.Sprintf("; offset %d applied after fetch", sel.Offset)
	}
	text += windowed
	return &Result{
		Columns: []ResultColumn{{Name: "plan", Type: types.String}},
		Rows:    [][]types.Datum{{types.NewString(text)}},
		Tag:     "EXPLAIN",
	}, nil
}

func (s *Session) execShowTables(ctx context.Context, txn *kvclient.Txn, dbName string) (*Result, error) {
	if dbName == "" {
		dbName = s.database
	}
	db, err := s.cat.Database(ctx, txn, dbName)
	if err != nil {
		return nil, ToSQLError(err)
	}
	descs, err := s.cat.ListIn(ctx, txn, db)
	if err != nil {
		return nil, err
	}
	// The same listing question information_schema, pg_catalog and the
	// console ask (issues #197, #259): a table the session holds no
	// privilege on, or whose schema it may not use, is not named. SHOW
	// TABLES listed every table of the database before, which was the
	// one listing that answered differently.
	set, err := s.roleSet(ctx, txn)
	if err != nil {
		return nil, err
	}
	denied := catalog.SchemaUsageDenied([]*catalog.DatabaseDescriptor{db}, set)
	res := &Result{Columns: showTablesColumns()}
	for _, d := range descs {
		if d.IsView() {
			continue // SHOW VIEWS
		}
		if !canSeeTable(set, d) || denied[d.DatabaseID] {
			continue
		}
		res.Rows = append(res.Rows, []types.Datum{types.NewString(d.Name)})
	}
	res.Tag = fmt.Sprintf("SHOW TABLES %d", len(res.Rows))
	return res, nil
}

// asErr is errors.As with less ceremony at call sites.
func asErr[T error](err error, target *T) bool {
	return errors.As(err, target)
}

// stripTableAlias rewrites alias-qualified column references (r.rolname
// in SELECT r.rolname FROM pg_roles r) to bare names for a single-table
// select, where the executor addresses columns by name alone. Joins keep
// their qualifiers; they resolve them per side.
//
// PostgreSQL scoping: an aliased table answers to its alias only (FROM
// ev e hides "ev"), so only the alias is stripped then; unaliased, the
// bare and the written table name are. A qualified name left over names
// no FROM item of this select and is refused with PostgreSQL's 42P01
// (never read as this table's column, which bound a correlated
// subquery's outer reference to its own table). The subqueries' names
// are theirs to resolve.
func stripTableAlias(t *parser.Select) error {
	if len(t.Joins) > 0 || t.Table == "" {
		return nil
	}
	_, bare := catalog.SplitTableName(t.Table)
	prefixes := map[string]bool{}
	var hidden scopeLevel
	if t.Alias != "" && t.Alias != bare && t.Alias != t.Table {
		prefixes[t.Alias+"."] = true
		hidden.hidden = []hiddenName{{table: bare, alias: t.Alias}, {table: t.Table, alias: t.Alias}}
	} else {
		prefixes[bare+"."], prefixes[t.Table+"."] = true, true
	}
	var err error
	// strip rewrites a name in place only when it changes: a statement
	// already stripped (a prepared one, run again) is only read.
	strip := func(name *string) {
		for pre := range prefixes {
			if strings.HasPrefix(*name, pre) && !strings.Contains((*name)[len(pre):], ".") {
				*name = (*name)[len(pre):]
				return
			}
		}
		if q, _ := splitQualified(*name); q != "" && err == nil {
			err = fromEntryErr(q, hidden)
		}
	}
	// Every expression position of this select (select list, aggregate
	// arguments and filters, WHERE, HAVING, ORDER BY, windows); not its
	// subqueries, whose qualified names may be correlated references
	// to this very alias, nor its WITH members or set members.
	self := *t
	self.With, self.Union, self.Derived = nil, nil, nil
	w := parser.Walker{
		OnSelect: func(s *parser.Select) bool { return s == &self },
		OnExpr: func(e *parser.Expr) bool {
			if e.Column != "" {
				strip(&e.Column)
			}
			return true
		},
		OnCond: func(c *parser.Comparison) bool {
			if c.Column != "" {
				strip(&c.Column)
			}
			return true
		},
		OnSelectExpr: func(se *parser.SelectExpr) {
			if se.AggCol != "" {
				strip(&se.AggCol)
			}
		},
		OnOrderCol: func(oc *parser.OrderCol) {
			if oc.Column != "" {
				strip(&oc.Column)
			}
		},
	}
	w.SelectStmt(&self)
	return err
}

// emptyCatalogSelect answers a select whose base table is an
// always-empty catalog (pg_trigger, pg_policy, ...) without planning it:
// the tools' queries over those take shapes (correlated table
// functions, WITH ORDINALITY) the executor does not run, and the answer
// is empty either way. Aggregates, grouping and LEFT JOINs from the
// empty side are left to the planner; unions too.
func (s *Session) emptyCatalogSelect(ctx context.Context, txn *kvclient.Txn, t *parser.Select) (*Result, bool) {
	if t.Table == "" || t.Union != nil || t.Derived != nil || hasAggregates(t.Exprs) || len(t.GroupBy) > 0 || len(t.Having) > 0 {
		return nil, false
	}
	// lookup resolves a real table of the same name first.
	desc, err := s.lookup(ctx, txn, t.Table)
	if err != nil || desc.Virtual == "" || !vtable.IsAlwaysEmpty(desc.Virtual) {
		return nil, false
	}
	vt, ok := vtable.Lookup(desc.Virtual)
	if !ok {
		return nil, false
	}
	for _, jc := range t.Joins {
		// A RIGHT or FULL join keeps the joined member's rows even
		// when the catalog has none.
		if jc.Left || jc.Right || jc.Full {
			return nil, false
		}
	}
	res := &Result{Tag: "SELECT 0"}
	for _, se := range t.Exprs {
		switch {
		case se.Star:
			for _, c := range vt.Columns {
				if !c.Hidden {
					res.Columns = append(res.Columns, ResultColumn{Name: c.Name, Type: c.Type})
				}
			}
		case se.Alias != "":
			res.Columns = append(res.Columns, ResultColumn{Name: se.Alias, Type: types.String})
		case se.Expr.Column != "":
			name := se.Expr.Column
			if i := strings.LastIndexByte(name, '.'); i >= 0 {
				name = name[i+1:]
			}
			res.Columns = append(res.Columns, ResultColumn{Name: name, Type: types.String})
		default:
			res.Columns = append(res.Columns, ResultColumn{Name: "?column?", Type: types.String})
		}
	}
	return res, true
}

// execUnion runs each member of a UNION [ALL] chain and concatenates the
// results (UNION removes duplicate rows). Column names come from the
// head; ORDER BY names resolve against the head's output columns, then
// resolveOrderAliases maps ORDER BY names that name an output column
// (SELECT x AS nsp ... ORDER BY nsp) onto what that output computes: the
// underlying column, or the expression itself. Output names take
// precedence over input columns, as in PostgreSQL.
func resolveOrderAliases(t *parser.Select) []parser.OrderCol {
	if len(t.OrderBy) == 0 {
		return nil
	}
	out := make([]parser.OrderCol, len(t.OrderBy))
	for i, oc := range t.OrderBy {
		out[i] = oc
		if oc.Expr != nil || oc.Column == "" {
			continue
		}
		for _, se := range t.Exprs {
			if se.Star || se.Agg != "" || se.Alias != oc.Column {
				continue
			}
			e := se.Expr
			if plainColumn(e) {
				out[i].Column = e.Column
			} else {
				out[i].Expr = &e
			}
			break
		}
	}
	return out
}

// plainColumn reports whether e is a bare column reference.
func plainColumn(e parser.Expr) bool {
	return e.Column != "" && len(e.Path) == 0 && e.BinOp == "" && e.Func == "" && e.Left == nil &&
		e.Case == nil && e.Cmp == nil && e.Lit == nil && e.Sub == nil
}

// selectScope builds the correlation scope a select's subqueries bind
// against: its table, or the merged sides of its joins. Lookups that
// fail leave the ordinary path to report them.
func (s *Session) selectScope(ctx context.Context, txn *kvclient.Txn, t *parser.Select) (scopeLevel, func(joinedRow) map[catalog.ColumnID]types.Datum, bool) {
	desc, err := s.lookup(ctx, txn, t.Table)
	if err != nil {
		return scopeLevel{}, nil, false
	}
	if len(t.Joins) == 0 {
		return tableLevel(desc, t.Table, t.Alias), nil, true
	}
	inner := make([]*catalog.TableDescriptor, len(t.Joins))
	for i := range t.Joins {
		if inner[i], err = s.lookup(ctx, txn, t.Joins[i].Table); err != nil {
			return scopeLevel{}, nil, false
		}
	}
	sides, err := makeJoinSides(desc, inner, t)
	if err != nil {
		return scopeLevel{}, nil, false
	}
	// USING / NATURAL merges decide which side owns a bare name.
	if _, err := expandUsing(sides, t); err != nil {
		return scopeLevel{}, nil, false
	}
	level, rowOf := joinScope(sides)
	return level, rowOf, true
}

// execShow runs the introspection statements (SHOW COLUMNS / INDEXES /
// CREATE TABLE / USERS / GRANTS / ALL).
func (s *Session) execShow(ctx context.Context, txn *kvclient.Txn, t *parser.Show) (*Result, error) {
	str := types.NewString
	// The shape is showColumns', which Describe reports too.
	res := &Result{Tag: "SHOW", Columns: showColumns(t)}
	switch t.Kind {
	case "columns":
		desc, err := s.lookup(ctx, txn, t.Table)
		if err != nil {
			return nil, err
		}
		if err := s.checkTablePriv(ctx, txn, desc, "SELECT"); err != nil {
			return nil, err
		}
		for i := range desc.Columns {
			c := &desc.Columns[i]
			if c.Hidden {
				continue
			}
			def := types.DNull
			if c.Default != nil {
				def = str(c.Default.Text())
			}
			var in []string
			if desc.IsPKCol(c.ID) {
				in = append(in, desc.Name+"_pkey")
			}
			for _, idx := range desc.Indexes {
				if indexColumnUses(&idx, *c) {
					in = append(in, idx.Name)
				}
			}
			res.Rows = append(res.Rows, []types.Datum{str(c.Name), str(vtable.FormatType(c)), types.NewBool(!c.NotNull && !desc.IsPKCol(c.ID)), def, str("{" + strings.Join(in, ",") + "}")})
		}
	case "indexes":
		desc, err := s.lookup(ctx, txn, t.Table)
		if err != nil {
			return nil, err
		}
		if err := s.checkTablePriv(ctx, txn, desc, "SELECT"); err != nil {
			return nil, err
		}
		seq := int64(0)
		for _, id := range desc.PrimaryKey {
			if c, ok := desc.ColByID(id); ok && !c.Hidden {
				seq++
				res.Rows = append(res.Rows, []types.Datum{str(desc.Name), str(desc.Name + "_pkey"), types.NewBool(false), types.NewInt(seq), str(c.Name)})
			}
		}
		for _, idx := range desc.Indexes {
			for j, id := range idx.ColumnIDs {
				if idx.IsExprKey(j) {
					res.Rows = append(res.Rows, []types.Datum{str(desc.Name), str(idx.Name), types.NewBool(!idx.Unique), types.NewInt(int64(j + 1)), str(idx.Exprs[j])})
				} else if c, ok := desc.ColByID(id); ok {
					res.Rows = append(res.Rows, []types.Datum{str(desc.Name), str(idx.Name), types.NewBool(!idx.Unique), types.NewInt(int64(j + 1)), str(c.Name)})
				}
			}
		}
	case "views":
		db, err := s.cat.Database(ctx, txn, s.database)
		if err != nil {
			return nil, ToSQLError(err)
		}
		descs, err := s.cat.ListIn(ctx, txn, db)
		if err != nil {
			return nil, err
		}
		for _, d := range descs {
			if d.IsView() {
				res.Rows = append(res.Rows, []types.Datum{str(d.Name), str(d.ViewQuery)})
			}
		}
	case "create":
		desc, err := s.lookup(ctx, txn, t.Table)
		if err != nil {
			return nil, err
		}
		if err := s.checkTablePriv(ctx, txn, desc, "SELECT"); err != nil {
			return nil, err
		}
		if desc.IsView() {
			res.Rows = append(res.Rows, []types.Datum{str(desc.Name), str(CreateViewDef(desc))})
			break
		}
		if err := mustBeReal(desc); err != nil {
			return nil, err
		}
		byID := func(id uint64) *catalog.TableDescriptor {
			d, err := catalog.ReadTable(ctx, txn, id)
			if err != nil {
				return nil
			}
			return d
		}
		def := vtable.CreateTableDefWith(desc, byID)
		if rls := vtable.RowSecurityDefs(desc); len(rls) > 0 {
			def += ";\n" + strings.Join(rls, ";\n")
		}
		res.Rows = append(res.Rows, []types.Datum{str(desc.Name), str(def)})
	case "users", "roles":
		// SHOW USERS: the login roles; SHOW ROLES: every role, the built-in
		// ones included.
		env, err := s.virtualEnv(ctx, txn, envNoTables, true)
		if err != nil {
			return nil, err
		}
		graph := catalog.NewRoleGraph(env.Roles)
		for _, r := range env.Roles {
			if t.Kind == "users" && !r.Login {
				continue
			}
			set, err := graph.Effective(r.Name)
			if err != nil {
				return nil, err
			}
			var members []string
			for _, m := range r.MemberOf {
				members = append(members, m.Role)
			}
			memberOf := strings.Join(members, ",")
			if t.Kind == "users" {
				res.Rows = append(res.Rows, []types.Datum{str(r.Name), types.NewBool(set.IsAdmin()), str(memberOf)})
			} else {
				res.Rows = append(res.Rows, []types.Datum{str(r.Name), types.NewBool(r.Login), types.NewBool(set.IsAdmin()), str(memberOf)})
			}
		}
	case "grants":
		if t.OnRole {
			rows, err := s.roleGrantRows(ctx, txn, t)
			if err != nil {
				return nil, err
			}
			for _, r := range rows {
				res.Rows = append(res.Rows, []types.Datum{str(r[0]), str(r[1]), str(r[2])})
			}
			break
		}
		rows, err := s.grantRows(ctx, txn, t)
		if err != nil {
			return nil, err
		}
		for _, r := range rows {
			row := make([]types.Datum, len(r))
			for i, v := range r {
				if v == "" && (i == 1 || i == 2) {
					row[i] = types.DNull
				} else {
					row[i] = str(v)
				}
			}
			res.Rows = append(res.Rows, row)
		}
	case "all":
		for _, kv := range s.settings() {
			res.Rows = append(res.Rows, []types.Datum{str(kv[0]), str(kv[1])})
		}
	case "sessions":
		// This node's sessions (each node keeps its own registry).
		admin, err := s.isAdmin(ctx, txn)
		if err != nil {
			return nil, err
		}
		res.Rows = s.sessionRows(admin)
	default:
		return nil, newErrf(CodeInternal, "unknown SHOW form %q", t.Kind)
	}
	res.Tag = fmt.Sprintf("SHOW %d", len(res.Rows))
	return res, nil
}

// resolveLimitParams returns t with LIMIT $n / OFFSET $n resolved from
// the parameters (on a copy; the statement may be cached): NULL is no
// limit / no offset, as in PostgreSQL, and a negative count is refused.
func resolveLimitParams(t *parser.Select, params []types.Datum) (*parser.Select, error) {
	c := *t
	count := func(idx int, clause, code string) (int64, bool, error) {
		if idx > len(params) {
			return 0, false, newErrf(CodeSyntaxError, "%s: parameter $%d was not supplied", clause, idx)
		}
		d := params[idx-1]
		if d.Null {
			return 0, false, nil
		}
		v, err := d.Coerce(types.Int)
		if err != nil {
			return 0, false, newErrf(CodeInvalidTextRepresentation, "%s count %q is not an integer", clause, d.Text())
		}
		if v.I < 0 {
			return 0, false, newErrf(code, "%s must not be negative", clause)
		}
		return v.I, true, nil
	}
	if t.LimitParam > 0 {
		v, set, err := count(t.LimitParam, "LIMIT", "2201W")
		if err != nil {
			return nil, err
		}
		c.Limit, c.LimitParam = -1, 0
		if set {
			c.Limit = v
		}
	}
	if t.OffsetParam > 0 {
		v, set, err := count(t.OffsetParam, "OFFSET", "2201X")
		if err != nil {
			return nil, err
		}
		c.Offset, c.OffsetParam = 0, 0
		if set {
			c.Offset = v
		}
	}
	return &c, nil
}

// filterCorrelated keeps the rows the statement's correlated subquery
// filters accept.
func (s *Session) filterCorrelated(ctx context.Context, txn *kvclient.Txn, corr []correlatedConjunct, desc *catalog.TableDescriptor, rows []fetchedRow, params []types.Datum) ([]fetchedRow, error) {
	memo := corrMemo{}
	kept := rows[:0]
	for _, fr := range rows {
		match, err := s.evalCorrelated(ctx, txn, corr, desc, fr.row, params, memo)
		if err != nil {
			return nil, err
		}
		if match {
			kept = append(kept, fr)
		}
	}
	return kept, nil
}

// rowLockErr gives a NOWAIT failure PostgreSQL's wording, naming the
// table; any other error passes through.
func rowLockErr(desc *catalog.TableDescriptor, err error) error {
	if kvclient.IsLockNotAvailable(err) {
		return newErrf(CodeLockNotAvailable, "could not obtain lock on row in relation %q", desc.Name)
	}
	return err
}

// skipLockedGrowth and skipLockedBoundedRounds shape the refetch of a
// SKIP LOCKED statement whose limited fetch came back full and still
// left the limit unmet: each round asks for four times as many rows,
// and after the bounded rounds the fetch is unlimited.
const (
	skipLockedGrowth        = 4
	skipLockedBoundedRounds = 3
)

// fetchSkipLocked fetches and locks the rows of a SELECT ... FOR UPDATE
// SKIP LOCKED, leaving out every row it cannot lock at once, and returns
// them in the statement's order, cut to LIMIT+OFFSET.
//
// The fetch reads under kvclient.LockWaitSkip, so rows carrying another
// transaction's intent never come back, and a pushed-down limit counts
// only rows that were free when the scan passed them. The rows are then
// locked in order, only as many as the statement returns — a row locked
// and then cut by LIMIT would be hidden from every other SKIP LOCKED
// reader for nothing. A row that cannot be locked after all (taken
// between the fetch and the lock, or changed and committed since this
// transaction's snapshot) is passed over like one locked from the start,
// and the next candidate takes its place. When a limited fetch runs out
// of candidates that way, it is repeated with a larger limit: the rows
// already locked come back (this transaction reads its own intents), the
// ones passed over are not tried again.
//
// Every row passed over is then taken out of the transaction's read set
// (Txn.ForgetReads): another transaction is about to change it, and it
// is no part of this statement's result. So is a row that was fetched
// but lay beyond the limit in an ordered scan — a fetch that could have
// stopped exactly at the limit would never have read it.
func (s *Session) fetchSkipLocked(ctx context.Context, txn *kvclient.Txn, desc *catalog.TableDescriptor, plan accessPlan, t *parser.Select, params []types.Datum, fetchLimit int64, corr []correlatedConjunct, needSort bool, order []parser.OrderCol) ([]fetchedRow, error) {
	if t.Limit == 0 {
		return nil, nil // nothing is returned, so nothing is locked
	}
	want := keepCount(t)
	skipCtx := kvclient.WithLockWait(ctx, kvclient.LockWaitSkip)
	mark := txn.ReadMark()
	held := map[string]bool{}   // locked by this statement
	passed := map[string]bool{} // could not be locked: not tried again
	limit := fetchLimit
	for round := 1; ; round++ {
		rows, err := s.executePlan(skipCtx, txn, desc, plan, t.Where, params, limit)
		if err != nil {
			return nil, err
		}
		exhausted := limit == 0 || int64(len(rows)) < limit
		if len(corr) > 0 {
			if rows, err = s.filterCorrelated(ctx, txn, corr, desc, rows, params); err != nil {
				return nil, err
			}
		}
		if needSort {
			if err := sortRows(desc, rows, order, params); err != nil {
				return nil, err
			}
			s.note("sort: %d rows in memory", len(rows))
		}
		var kept []fetchedRow
		next := 0
		for next < len(rows) && (want == 0 || int64(len(kept)) < want) {
			// Lock as many candidates as are still wanted, in one batch.
			n := len(rows) - next
			if want > 0 && want-int64(len(kept)) < int64(n) {
				n = int(want - int64(len(kept)))
			}
			chunk := rows[next : next+n]
			next += n
			var lockKeys []keys.Key
			for _, fr := range chunk {
				if k := string(fr.key); !held[k] && !passed[k] {
					lockKeys = append(lockKeys, fr.key)
				}
			}
			got, err := txn.GetBatchForUpdateNoWait(ctx, lockKeys, true)
			if err != nil {
				return nil, err
			}
			var refused []keys.Key
			for i, k := range lockKeys {
				if got[i] {
					held[string(k)] = true
				} else {
					passed[string(k)] = true
					refused = append(refused, k)
				}
			}
			// At once, not at the end: the very next read of this
			// statement may have to refresh, and the transaction that
			// took the row has likely committed by then.
			txn.ForgetReads(mark, refused)
			for _, fr := range chunk {
				if held[string(fr.key)] {
					kept = append(kept, fr)
				}
			}
		}
		if want > 0 && int64(len(kept)) < want && !exhausted {
			if round >= skipLockedBoundedRounds {
				limit = 0
			} else {
				limit *= skipLockedGrowth
			}
			continue
		}
		var forget []keys.Key
		for k := range passed {
			forget = append(forget, keys.Key(k))
		}
		if !needSort {
			for _, fr := range rows[next:] {
				if !held[string(fr.key)] {
					forget = append(forget, fr.key)
				}
			}
		}
		txn.ForgetReads(mark, forget)
		return kept, nil
	}
}

// keepCount is how many leading rows a stage must keep for the query's
// LIMIT and OFFSET to apply after it (0 = every row).
func keepCount(t *parser.Select) int64 {
	if t.Limit < 0 {
		return 0
	}
	return t.Limit + t.Offset
}

// cutRows keeps the first keep rows (all when keep is 0).
func cutRows[T any](rows []T, keep int64) []T {
	if keep > 0 && int64(len(rows)) > keep {
		return rows[:keep]
	}
	return rows
}

// trimRows applies the query's OFFSET, then its LIMIT (LIMIT 0 keeps
// nothing; -1 is no limit).
func trimRows[T any](rows []T, t *parser.Select) []T {
	if t.Offset > 0 {
		if int64(len(rows)) <= t.Offset {
			rows = rows[:0]
		} else {
			rows = rows[t.Offset:]
		}
	}
	if t.Limit >= 0 && int64(len(rows)) > t.Limit {
		rows = rows[:t.Limit]
	}
	return rows
}

// explainStats is what EXPLAIN ANALYZE gathers while the statement runs:
// one line per stage (a scan with its path, rows and time; a join level;
// the group, window, set-operation and sort stages), in execution order.
type explainStats struct {
	lines []string
}

// note records a stage line when EXPLAIN ANALYZE is running.
func (s *Session) note(format string, args ...any) {
	if s.explain != nil {
		s.explain.lines = append(s.explain.lines, fmt.Sprintf(format, args...))
	}
}

// explainDuration renders a stage duration for EXPLAIN ANALYZE.
func explainDuration(d time.Duration) string {
	return fmt.Sprintf("%.3f ms", float64(d.Microseconds())/1000)
}

// execExplainAnalyze runs the select with stage accounting on and
// reports the plan line followed by one line per stage with its actual
// rows and time, then the output row count and the total time.
func (s *Session) execExplainAnalyze(ctx context.Context, txn *kvclient.Txn, sel *parser.Select, params []types.Datum) (*Result, error) {
	plan, err := s.execExplain(ctx, txn, &parser.Explain{Stmt: sel}, params)
	if err != nil {
		return nil, err
	}
	stats := &explainStats{}
	s.explain = stats
	start := time.Now()
	res, err := s.execSelect(ctx, txn, sel, params)
	elapsed := time.Since(start)
	s.explain = nil
	if err != nil {
		return nil, err
	}
	out := &Result{Columns: []ResultColumn{{Name: "plan", Type: types.String}}}
	out.Rows = append(out.Rows, []types.Datum{types.NewString("plan: " + plan.Rows[0][0].Text())})
	for _, line := range stats.lines {
		out.Rows = append(out.Rows, []types.Datum{types.NewString("  " + line)})
	}
	out.Rows = append(out.Rows, []types.Datum{types.NewString(fmt.Sprintf("output: %d rows; total %s", len(res.Rows), explainDuration(elapsed)))})
	out.Tag = fmt.Sprintf("EXPLAIN %d", len(out.Rows))
	return out, nil
}
