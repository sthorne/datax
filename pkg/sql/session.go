package sql

import (
	"context"
	"errors"
	"fmt"
	"github.com/sthorne/datax/pkg/sql/builtins"
	"math"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/sthorne/datax/pkg/keys"
	"github.com/sthorne/datax/pkg/kvclient"
	"github.com/sthorne/datax/pkg/sql/catalog"
	"github.com/sthorne/datax/pkg/sql/parser"
	"github.com/sthorne/datax/pkg/sql/types"
	"github.com/sthorne/datax/pkg/sql/vtable"
	"github.com/sthorne/datax/pkg/util/decimal"
	"github.com/sthorne/datax/pkg/util/encoding"
	"github.com/sthorne/datax/pkg/util/hlc"
	"github.com/sthorne/datax/pkg/version"
)

// resolveAsOf turns an AS OF SYSTEM TIME operand into a fixed timestamp:
// a negative duration ('-5s', relative to now), an RFC 3339 timestamp
// ('2026-08-29T12:00:00Z'), or a Unix-nanoseconds integer.
func resolveAsOf(operand string, now hlc.Timestamp) (hlc.Timestamp, error) {
	if nanos, err := strconv.ParseInt(operand, 10, 64); err == nil {
		if nanos <= 0 || nanos >= now.WallTime {
			return hlc.Timestamp{}, fmt.Errorf("timestamp %d is not in the past", nanos)
		}
		return hlc.Timestamp{WallTime: nanos}, nil
	}
	if d, err := time.ParseDuration(operand); err == nil {
		if d >= 0 {
			return hlc.Timestamp{}, fmt.Errorf("duration %q must be negative (a time in the past)", operand)
		}
		return now.AddNanos(d.Nanoseconds()), nil
	}
	if t, err := time.Parse(time.RFC3339Nano, operand); err == nil {
		ts := hlc.Timestamp{WallTime: t.UnixNano()}
		if !ts.Less(now) {
			return hlc.Timestamp{}, fmt.Errorf("timestamp %q is not in the past", operand)
		}
		return ts, nil
	}
	return hlc.Timestamp{}, fmt.Errorf("cannot interpret %q as a duration, RFC 3339 timestamp, or Unix nanoseconds", operand)
}

// resolveMaxStaleness turns a with_max_staleness operand into the
// statement timestamp: the freshest the local store can serve
// (localClosed), but never older than now minus the bound. A local store
// that has closed past now-bound simply gives a fresher read; one
// lagging past the bound (or absent — a pure gateway) yields
// now-bound, and ranges that cannot serve it locally fall back to their
// leaders through ordinary stale-read routing.
func resolveMaxStaleness(operand string, now, localClosed hlc.Timestamp) (hlc.Timestamp, error) {
	d, err := time.ParseDuration(operand)
	if err != nil {
		return hlc.Timestamp{}, fmt.Errorf("cannot interpret %q as a duration", operand)
	}
	if d <= 0 {
		return hlc.Timestamp{}, fmt.Errorf("max staleness %q must be a positive duration", operand)
	}
	ts := now.AddNanos(-d.Nanoseconds())
	if ts.Less(localClosed) {
		ts = localClosed
	}
	return ts, nil
}

// ResultColumn describes one output column.
type ResultColumn struct {
	Name string
	Type types.Family
	// Typmod is PostgreSQL's atttypmod for the column (0 = none, emitted
	// as -1 on the wire). Set only when a real DECIMAL(p,s) column backs
	// the output: ((p<<16) | (s+4)).
	Typmod int32
}

// DecimalTypmod encodes a DECIMAL(p,s) declaration as PostgreSQL's
// atttypmod.
func DecimalTypmod(precision, scale int32) int32 {
	return precision<<16 | (scale + 4)
}

// Result is the outcome of one statement.
type Result struct {
	Columns []ResultColumn
	Rows    [][]types.Datum
	// Tag is the PostgreSQL command tag ("SELECT 3", "INSERT 0 2", ...).
	Tag string
}

// TxnState is the session's transaction status, mirrored into the wire
// protocol's ReadyForQuery byte.
type TxnState int

const (
	StateIdle   TxnState = iota // 'I'
	StateOpen                   // 'T'
	StateFailed                 // 'E' — only ROLLBACK escapes
)

// Session executes statements for one client connection.
type Session struct {
	db  *kvclient.DB
	cat *catalog.Accessor
	// user is the authenticated SQL user ("root" for internal sessions).
	// In insecure/trust mode it is the client-claimed name: privileges are
	// enforced against it, but nothing verified the identity.
	user string
	// database is the session's current database (the startup parameter,
	// USE, or SET database); unqualified table names resolve in it.
	database string
	// system marks the node's own internal session (see NewSystemSession):
	// the only one allowed to create or drop a system table.
	system bool

	// cascadeLimit is SET foreign_key_cascade_limit (0 = the default);
	// pendingWipes are indexes dropped by statements of this session
	// whose entries are reclaimed once the statement commits.
	cascadeLimit int
	pendingWipes []indexWipe
	extraDDL     []string
	// stmtNow is the statement timestamp (now(), current_timestamp):
	// taken from the clock on first use in each statement.
	stmtNow int64
	// seqs is the session's sequence state (value blocks, currval,
	// lastval), created on first use.
	seqs *seqState
	// rels are the statement-scoped relations (WITH members, joined
	// derived tables) bound by name; see cte.go.
	rels map[string]*relation
	// explain collects each stage's actual rows and time while EXPLAIN
	// ANALYZE runs a statement (nil otherwise).
	explain *explainStats

	txn   *kvclient.Txn
	state TxnState
	// pendingDDL names tables changed inside the open explicit transaction;
	// lease adoption for them is drained at COMMIT.
	pendingDDL []string
}

// NewSession creates a root session (internal components, tests, tools).
// The catalog accessor is shared per node.
func NewSession(db *kvclient.DB, cat *catalog.Accessor) *Session {
	return &Session{db: db, cat: cat, user: "root", database: catalog.DefaultDatabase}
}

// NewSystemSession creates the node's own root session, the one that may
// create and drop system tables (the metrics recorder's).
func NewSystemSession(db *kvclient.DB, cat *catalog.Accessor) *Session {
	return &Session{db: db, cat: cat, user: "root", database: catalog.DefaultDatabase, system: true}
}

// NewSessionForUser creates a session for an authenticated user.
func NewSessionForUser(db *kvclient.DB, cat *catalog.Accessor, user string) *Session {
	if user == "" {
		user = "root"
	}
	return &Session{db: db, cat: cat, user: user, database: catalog.DefaultDatabase}
}

// SetUser rebinds the session's user (pgwire sets it after startup).
func (s *Session) SetUser(user string) {
	if user == "" {
		user = "root"
	}
	s.user = user
}

// User returns the session's SQL user.
func (s *Session) User() string { return s.user }

// Database returns the session's current database.
func (s *Session) Database() string { return s.database }

// ClusterVersion is the cluster version the session's gateway observes
// (its version-gated DDL keys off this mirror, which starts at the floor
// and catches up with the first heartbeat).
func (s *Session) ClusterVersion() version.Version { return s.db.ClusterVersion() }

// FinalizedClusterVersion reads the replicated cluster version the
// upgrade finalized (the value the gateway's mirror converges to).
func (s *Session) FinalizedClusterVersion(ctx context.Context) (version.Version, error) {
	raw, err := s.db.Get(ctx, keys.ClusterVersionKey())
	if err != nil {
		return 0, err
	}
	if raw == nil {
		return version.V1, nil
	}
	v, err := strconv.Atoi(string(raw))
	if err != nil {
		return 0, err
	}
	return version.Version(v), nil
}

// settings lists the session variables SHOW ALL and pg_settings report:
// the wire's startup parameters plus the ones the session honors.
func (s *Session) settings() [][2]string {
	return [][2]string{
		{"application_name", ""},
		{"client_encoding", "UTF8"},
		{"database", s.database},
		{"DateStyle", "ISO"},
		{"foreign_key_cascade_limit", strconv.Itoa(s.fkCascadeLimit())},
		{"integer_datetimes", "on"},
		{"search_path", catalog.PublicSchema},
		{"server_encoding", "UTF8"},
		{"server_version", "14.0 datax"},
		{"standard_conforming_strings", "on"},
		{"TimeZone", "UTC"},
		{"transaction_isolation", "serializable"},
	}
}

// setting resolves a SHOW name (case-insensitively; SHOW TIME ZONE and
// SHOW TRANSACTION ISOLATION LEVEL by their spelled-out forms) to the
// setting's canonical name and value.
func (s *Session) setting(name string) (string, string, bool) {
	switch strings.ToLower(name) {
	case "time_zone":
		name = "TimeZone"
	case "transaction_isolation_level":
		name = "transaction_isolation"
	}
	for _, kv := range s.settings() {
		if strings.EqualFold(kv[0], name) {
			return kv[0], kv[1], true
		}
	}
	return "", "", false
}

// lookup resolves a table name: a virtual catalog table (pg_catalog.x,
// information_schema.x, or a bare pg_catalog name that no real table in
// the current database shadows) or a table in the session's current
// database (or the name's own qualifier).
func (s *Session) lookup(ctx context.Context, txn *kvclient.Txn, name string) (*catalog.TableDescriptor, error) {
	db, bare := catalog.SplitTableName(name)
	if len(s.rels) > 0 {
		if db == "" {
			if r, ok := s.rels[strings.ToLower(bare)]; ok {
				return r.desc, nil // a WITH member shadows a table of its name
			}
		} else {
			for _, r := range s.rels {
				if r.alias != "" && r.alias == strings.ToLower(name) {
					return r.desc, nil // a view referenced by its qualified name
				}
			}
		}
	}
	if vtable.IsSchema(db) {
		if vt, ok := vtable.Lookup(name); ok {
			return vt.Descriptor(), nil
		}
		return nil, &catalog.ErrTableNotFound{Name: name}
	}
	// A descriptor this transaction's DDL wrote is read through the
	// transaction and never cached: the cache is shared with other
	// sessions on the gateway and must not carry uncommitted state, and
	// a rollback must not leave the phantom behind.
	var d *catalog.TableDescriptor
	var err error
	if s.state == StateOpen && s.ddlTouched(name) {
		d, err = s.cat.LookupFreshIn(ctx, txn, s.database, name)
	} else {
		d, err = s.cat.LookupIn(ctx, txn, s.database, name)
	}
	if err != nil {
		var nf *catalog.ErrTableNotFound
		if errors.As(err, &nf) {
			if db == "" {
				if vt, ok := vtable.Lookup(bare); ok {
					return vt.Descriptor(), nil
				}
			}
			// A sequence reads as a one-row relation (last_value, log_cnt,
			// is_called), as in PostgreSQL — psql's \d of a sequence needs it.
			if sd, serr := s.lookupSequence(ctx, txn, name); serr == nil {
				return sequenceRelation(sd), nil
			}
		}
	}
	return d, err
}

// sequenceRelation is the virtual one-row descriptor a sequence reads as.
func sequenceRelation(sd *catalog.SequenceDescriptor) *catalog.TableDescriptor {
	return &catalog.TableDescriptor{
		ID: sd.ID, Name: sd.Name, DatabaseID: sd.DatabaseID, Virtual: "sequence",
		Columns: []catalog.Column{
			{ID: 1, Name: "last_value", Type: types.Int, NotNull: true},
			{ID: 2, Name: "log_cnt", Type: types.Int, NotNull: true},
			{ID: 3, Name: "is_called", Type: types.Bool, NotNull: true},
			{ID: 4, Name: "_ord", Type: types.Int, NotNull: true, Hidden: true},
		},
		PrimaryKey: []catalog.ColumnID{4},
	}
}

// virtualEnv gathers what the virtual tables render: every database,
// the tables the session may see (all of them for admins; for others,
// the tables they hold a privilege on), statistics, users and admins.
func (s *Session) virtualEnv(ctx context.Context, txn *kvclient.Txn) (*vtable.Env, error) {
	env := &vtable.Env{User: s.user, Database: s.database, Stats: map[uint64]*catalog.TableStatistics{}, Admins: map[string]bool{}}
	dbs, err := catalog.ListDatabases(ctx, txn)
	if err != nil {
		return nil, err
	}
	env.Databases = dbs
	if env.Sequences, err = catalog.ListSequences(ctx, txn, 0); err != nil {
		return nil, err
	}
	env.SequenceValue = func(sd *catalog.SequenceDescriptor) (int64, bool, error) { return s.sequenceValue(ctx, sd) }
	admin, err := s.isAdmin(ctx, txn)
	if err != nil {
		return nil, err
	}
	env.IsAdmin = admin
	all, err := s.cat.List(ctx, txn)
	if err != nil {
		return nil, err
	}
	for _, d := range all {
		if !admin && len(d.Privileges[s.user]) == 0 {
			continue
		}
		env.Tables = append(env.Tables, d)
		if st, _ := s.cat.Stats(ctx, d.ID); st != nil {
			env.Stats[d.ID] = st
		}
	}
	lo, hi := keys.UserSpan()
	users, err := txn.Scan(ctx, lo, hi, 0)
	if err != nil {
		return nil, err
	}
	env.Users = []string{"root"}
	for _, kv := range users {
		if _, name, derr := encoding.DecodeString(kv.Key[len(lo):]); derr == nil && name != "root" {
			env.Users = append(env.Users, name)
		}
	}
	alo, ahi := keys.AdminUserSpan()
	admins, err := txn.Scan(ctx, alo, ahi, 0)
	if err != nil {
		return nil, err
	}
	for _, kv := range admins {
		if _, name, derr := encoding.DecodeString(kv.Key[len(alo):]); derr == nil {
			env.Admins[name] = true
		}
	}
	env.Settings = s.settings()
	return env, nil
}

// fetchVirtual generates a virtual table's rows and applies the WHERE
// conjuncts (no index ever applies).
func (s *Session) fetchVirtual(ctx context.Context, txn *kvclient.Txn, desc *catalog.TableDescriptor, where []parser.Comparison, params []types.Datum, limit int64) ([]fetchedRow, error) {
	if strings.HasPrefix(desc.Virtual, relationPrefix) {
		return s.relationRows(desc, where, params, limit)
	}
	if desc.Virtual == "sequence" {
		sd, err := catalog.ReadSequence(ctx, txn, desc.ID)
		if err != nil {
			return nil, ToSQLError(err)
		}
		v, called, err := s.sequenceValue(ctx, sd)
		if err != nil {
			return nil, err
		}
		if !called {
			v = sd.Start
		}
		row := map[catalog.ColumnID]types.Datum{1: types.NewInt(v), 2: types.NewInt(0), 3: types.NewBool(called), 4: types.NewInt(0)}
		ok, err := matchesWhere(where, desc, row, params)
		if err != nil || !ok {
			return nil, err
		}
		return []fetchedRow{{row: row}}, nil
	}
	vt, ok := vtable.Lookup(desc.Virtual)
	if !ok {
		return nil, newErrf(CodeInternal, "virtual table %q vanished", desc.Virtual)
	}
	env, err := s.virtualEnv(ctx, txn)
	if err != nil {
		return nil, err
	}
	rows, err := vt.Rows(ctx, env)
	if err != nil {
		return nil, err
	}
	ord := desc.PrimaryKey[0]
	var out []fetchedRow
	for i, r := range rows {
		row := make(map[catalog.ColumnID]types.Datum, len(r)+1)
		for j, d := range r {
			row[catalog.ColumnID(j+1)] = d
		}
		row[ord] = types.NewInt(int64(i))
		ok, err := matchesWhere(where, desc, row, params)
		if err != nil {
			return nil, err
		}
		if !ok {
			continue
		}
		out = append(out, fetchedRow{row: row})
		if limit > 0 && int64(len(out)) >= limit {
			break
		}
	}
	return out, nil
}

func (s *Session) State() TxnState { return s.state }

// statementTime is now() for the statement in flight: one reading of
// the clock, so now(), current_timestamp and current_date agree within
// a statement.
func (s *Session) statementTime() int64 {
	if s.stmtNow == 0 {
		s.stmtNow = s.db.Clock().Now().WallTime
	}
	return s.stmtNow
}

// Close rolls back any open transaction (connection teardown).
func (s *Session) Close(ctx context.Context) {
	if s.txn != nil {
		_ = s.txn.Rollback(ctx)
		s.txn = nil
	}
	s.state = StateIdle
}

// Execute runs one parsed statement with the given parameter values.
func (s *Session) Execute(ctx context.Context, stmt parser.Statement, params []types.Datum) (*Result, *Error) {
	s.stmtNow = 0
	// A failed transaction accepts only ROLLBACK (or COMMIT, which also
	// rolls back) — and ROLLBACK TO SAVEPOINT, which escapes the failed
	// state by restoring the savepoint (PostgreSQL semantics).
	if s.state == StateFailed {
		switch t := stmt.(type) {
		case *parser.Rollback, *parser.Commit:
			s.rollback(ctx)
			return &Result{Tag: "ROLLBACK"}, nil
		case *parser.RollbackToSavepoint:
			return s.execRollbackToSavepoint(ctx, t.Name)
		default:
			return nil, newErrf(CodeInFailedTransaction,
				"current transaction is aborted, commands ignored until end of transaction block")
		}
	}

	switch t := stmt.(type) {
	case *parser.Begin:
		if s.state == StateOpen {
			return nil, newErrf(CodeActiveTransaction, "there is already a transaction in progress")
		}
		s.txn = s.db.NewTxn("sql")
		s.state = StateOpen
		return &Result{Tag: "BEGIN"}, nil

	case *parser.Commit:
		if s.state != StateOpen {
			return &Result{Tag: "COMMIT"}, nil // PG: warning, not error
		}
		err := s.txn.Commit(ctx)
		s.txn = nil
		s.state = StateIdle
		pending := s.pendingDDL
		if err != nil {
			s.forgetDDL()
			s.pendingDDL, s.extraDDL, s.pendingWipes = nil, nil, nil
			return nil, ToSQLError(err)
		}
		s.pendingDDL = nil
		// Schema changes are visible everywhere once COMMIT returns: drain
		// lease adoption for every table this transaction altered.
		for _, name := range pending {
			if derr := s.cat.FinishDDLIn(ctx, s.database, name); derr != nil {
				return nil, ToSQLError(derr)
			}
		}
		s.runPendingWipes(ctx)
		return &Result{Tag: "COMMIT"}, nil

	case *parser.Rollback:
		s.rollback(ctx)
		return &Result{Tag: "ROLLBACK"}, nil

	case *parser.Savepoint:
		if s.state != StateOpen {
			return nil, newErrf(CodeNoActiveTransaction, "SAVEPOINT can only be used in transaction blocks")
		}
		if err := s.txn.Savepoint(t.Name); err != nil {
			return nil, ToSQLError(err)
		}
		return &Result{Tag: "SAVEPOINT"}, nil

	case *parser.ReleaseSavepoint:
		if s.state != StateOpen {
			return nil, newErrf(CodeNoActiveTransaction, "RELEASE SAVEPOINT can only be used in transaction blocks")
		}
		if err := s.txn.ReleaseSavepoint(t.Name); err != nil {
			return nil, newErrf(CodeInvalidSavepoint, "%v", err)
		}
		return &Result{Tag: "RELEASE"}, nil

	case *parser.RollbackToSavepoint:
		if s.state != StateOpen {
			return nil, newErrf(CodeNoActiveTransaction, "ROLLBACK TO SAVEPOINT can only be used in transaction blocks")
		}
		return s.execRollbackToSavepoint(ctx, t.Name)

	case *parser.SetVar:
		if len(t.Name) > 5 && t.Name[:5] == "show:" {
			name, value, ok := s.setting(t.Name[5:])
			if !ok {
				return nil, newErrf(CodeUndefinedObject, "unrecognized configuration parameter %q", t.Name[5:])
			}
			return &Result{Columns: []ResultColumn{{Name: name, Type: types.String}}, Rows: [][]types.Datum{{types.NewString(value)}}, Tag: "SHOW"}, nil
		}
		if t.Name == "database" {
			if serr := s.UseDatabase(ctx, t.Value); serr != nil {
				return nil, serr
			}
		}
		if t.Name == "foreign_key_cascade_limit" {
			n, err := strconv.Atoi(strings.Trim(t.Value, "'"))
			if err != nil || n < 1 {
				return nil, newErrf(CodeInvalidParameterValue, "foreign_key_cascade_limit must be a positive integer, not %q", t.Value)
			}
			s.cascadeLimit = n
		}
		return &Result{Tag: "SET"}, nil
	case *parser.Use:
		if serr := s.UseDatabase(ctx, t.Name); serr != nil {
			return nil, serr
		}
		return &Result{Tag: "SET"}, nil

	default:
		return s.executeData(ctx, stmt, params)
	}
}

// executeData runs a data statement in the session transaction (explicit)
// or an auto-retrying implicit one.
func (s *Session) executeData(ctx context.Context, stmt parser.Statement, params []types.Datum) (*Result, *Error) {
	// CREATE INDEX is a multi-transaction state machine (publish
	// write-only → drain → backfill+publish → drain), so — like
	// PostgreSQL's CREATE INDEX CONCURRENTLY — it cannot run inside an
	// explicit transaction block.
	// ALTER TABLE ... SET (shards = N) is the online re-shard: the same
	// multi-transaction state-machine shape, with the same restrictions.
	// ADD CONSTRAINT, VALIDATE CONSTRAINT and SET NOT NULL publish, drain
	// and then sweep the existing rows: multi-transaction, like CREATE
	// INDEX.
	if at, ok := stmt.(*parser.AlterTable); ok && at.IfExists && (at.AddConstraint != nil || at.ValidateConstraint != "" || at.SetNotNull != "" || at.SetOptions != nil || at.SetType != nil) && !s.tableExists(ctx, at.Table) {
		return &Result{Tag: "ALTER TABLE"}, nil
	}
	// CREATE TABLE ... AS and ALTER COLUMN TYPE are multi-transaction
	// statements too: refused in a block, admin-only.
	if ct, ok := stmt.(*parser.CreateTable); ok && ct.As != nil {
		if s.state == StateOpen {
			return nil, newErrf(CodeActiveTransaction, "CREATE TABLE ... AS cannot run inside a transaction block")
		}
		return s.execCreateTableAs(ctx, ct, params)
	}
	if at, ok := stmt.(*parser.AlterTable); ok && at.SetType != nil {
		if s.state == StateOpen {
			return nil, newErrf(CodeActiveTransaction, "ALTER TABLE ... ALTER COLUMN TYPE cannot run inside a transaction block")
		}
		var aerr error
		if err := s.db.RunTxn(ctx, "admin-check", func(ctx context.Context, txn *kvclient.Txn) error {
			aerr = s.checkAdmin(ctx, txn)
			return nil
		}); err != nil {
			return nil, ToSQLError(err)
		}
		if aerr != nil {
			return nil, ToSQLError(aerr)
		}
		return s.execRetypeOnline(ctx, at)
	}
	if at, ok := stmt.(*parser.AlterTable); ok && (at.AddConstraint != nil || at.ValidateConstraint != "" || at.SetNotNull != "") {
		if s.state == StateOpen {
			return nil, newErrf(CodeActiveTransaction, "ALTER TABLE ... ADD CONSTRAINT, VALIDATE CONSTRAINT and SET NOT NULL cannot run inside a transaction block")
		}
		var aerr error
		if err := s.db.RunTxn(ctx, "admin-check", func(ctx context.Context, txn *kvclient.Txn) error {
			aerr = s.checkAdmin(ctx, txn)
			return nil
		}); err != nil {
			return nil, ToSQLError(err)
		}
		if aerr != nil {
			return nil, ToSQLError(aerr)
		}
		switch {
		case at.AddConstraint != nil:
			return s.execAddConstraintOnline(ctx, at)
		case at.ValidateConstraint != "":
			return s.execValidateConstraintOnline(ctx, at)
		default:
			return s.execSetNotNullOnline(ctx, at)
		}
	}

	if at, ok := stmt.(*parser.AlterTable); ok && at.SetOptions != nil {
		if s.state == StateOpen {
			return nil, newErrf(CodeActiveTransaction, "ALTER TABLE ... SET (shards) cannot run inside a transaction block")
		}
		var aerr error
		if err := s.db.RunTxn(ctx, "admin-check", func(ctx context.Context, txn *kvclient.Txn) error {
			aerr = s.checkAdmin(ctx, txn)
			return nil
		}); err != nil {
			return nil, ToSQLError(err)
		}
		if aerr != nil {
			return nil, ToSQLError(aerr)
		}
		if _, ok := at.SetOptions["retention"]; ok {
			res, rerr := s.execSetRetention(ctx, at)
			if rerr != nil {
				return nil, rerr
			}
			if _, reshard := at.SetOptions["shards"]; !reshard {
				return res, nil
			}
			rest := map[string]string{}
			for k, v := range at.SetOptions {
				if k != "retention" {
					rest[k] = v
				}
			}
			at = &parser.AlterTable{Table: at.Table, SetOptions: rest}
		}
		return s.execReshardOnline(ctx, at)
	}

	if ci, ok := stmt.(*parser.CreateIndex); ok {
		if s.state == StateOpen {
			return nil, newErrf(CodeActiveTransaction, "CREATE INDEX cannot run inside a transaction block")
		}
		var aerr error
		if err := s.db.RunTxn(ctx, "admin-check", func(ctx context.Context, txn *kvclient.Txn) error {
			aerr = s.checkAdmin(ctx, txn)
			return nil
		}); err != nil {
			return nil, ToSQLError(err)
		}
		if aerr != nil {
			return nil, ToSQLError(aerr)
		}
		return s.execCreateIndexOnline(ctx, ci)
	}

	// ANALYZE sweeps tables at a frozen timestamp in paced chunks outside
	// any transaction, so — like the other multi-transaction statements —
	// it cannot run inside an explicit transaction block.
	if an, ok := stmt.(*parser.Analyze); ok {
		if s.state == StateOpen {
			return nil, newErrf(CodeActiveTransaction, "ANALYZE cannot run inside a transaction block")
		}
		var aerr error
		if err := s.db.RunTxn(ctx, "admin-check", func(ctx context.Context, txn *kvclient.Txn) error {
			aerr = s.checkAdmin(ctx, txn)
			return nil
		}); err != nil {
			return nil, ToSQLError(err)
		}
		if aerr != nil {
			return nil, ToSQLError(aerr)
		}
		return s.execAnalyze(ctx, an)
	}

	// AS OF SYSTEM TIME pins a SELECT to a fixed past timestamp: it runs
	// in its own read-only historical transaction (servable by follower
	// replicas), so it cannot join an explicit transaction's timestamp.
	if sel, ok := stmt.(*parser.Select); ok && (sel.AsOf != "" || sel.AsOfMaxStaleness != "") {
		if s.state == StateOpen {
			return nil, newErrf(CodeActiveTransaction, "AS OF SYSTEM TIME cannot run inside a transaction block")
		}
		if sel.ForUpdate {
			return nil, newErrf(CodeFeatureNotSupported, "FOR UPDATE is not allowed with AS OF SYSTEM TIME")
		}
		var ts hlc.Timestamp
		if sel.AsOfMaxStaleness != "" {
			var err error
			ts, err = resolveMaxStaleness(sel.AsOfMaxStaleness, s.db.Clock().Now(), s.db.LocalClosedTimestamp())
			if err != nil {
				return nil, newErrf(CodeInvalidParameterValue, "with_max_staleness: %v", err)
			}
		} else {
			var err error
			ts, err = resolveAsOf(sel.AsOf, s.db.Clock().Now())
			if err != nil {
				return nil, newErrf(CodeSyntaxError, "AS OF SYSTEM TIME: %v", err)
			}
		}
		// Historical reads plan against the descriptor current AT their
		// timestamp (the catalog reads it through the historical txn), so
		// a read below a re-shard routes to the retired pre-swap layout.
		// The guard refuses only when that layout has already been wiped
		// by the janitor — its rows are gone, and the read would silently
		// come back short. Checked per referenced table; best-effort with
		// a short timeout on the current-descriptor read (a partitioned
		// leader must not hang a bounded-staleness read whose whole point
		// is serving locally).
		txn := s.db.NewHistoricalTxn("sql-asof", ts)
		if sel.Table != "" {
			if serr := s.checkHistoricalLayout(ctx, txn, sel.Table); serr != nil {
				return nil, serr
			}
		}
		for i := range sel.Joins {
			if serr := s.checkHistoricalLayout(ctx, txn, sel.Joins[i].Table); serr != nil {
				return nil, serr
			}
		}
		res, serr := s.execSelect(ctx, txn, sel, params)
		_ = txn.Commit(ctx) // read-only: releases nothing, marks finished
		if serr != nil {
			return nil, ToSQLError(serr)
		}
		return res, nil
	}

	if s.state == StateOpen {
		s.extraDDL = nil
		res, err := s.execStmt(ctx, s.txn, stmt, params)
		if err != nil {
			// Any error fails the explicit transaction (PG semantics).
			s.state = StateFailed
			s.extraDDL, s.pendingWipes = nil, nil
			return nil, ToSQLError(err)
		}
		if name := ddlTableName(stmt); name != "" {
			s.pendingDDL = append(s.pendingDDL, name)
		}
		s.pendingDDL = append(s.pendingDDL, s.extraDDL...)
		s.extraDDL = nil
		return res, nil
	}
	var res *Result
	err := s.db.RunTxn(ctx, "sql-implicit", func(ctx context.Context, txn *kvclient.Txn) error {
		var err error
		s.extraDDL, s.pendingWipes = nil, nil
		res, err = s.execStmt(ctx, txn, stmt, params)
		return err
	})
	if err != nil {
		s.extraDDL, s.pendingWipes = nil, nil
		return nil, ToSQLError(err)
	}
	names := s.extraDDL
	s.extraDDL = nil
	if name := ddlTableName(stmt); name != "" {
		names = append([]string{name}, names...)
	}
	for _, name := range names {
		if derr := s.cat.FinishDDLIn(ctx, s.database, name); derr != nil {
			return nil, ToSQLError(derr)
		}
	}
	s.runPendingWipes(ctx)
	return res, nil
}

// noteDDL records another table a statement's DDL changed (a foreign
// key's parent, a dropped table's children), for the post-commit drain.
func (s *Session) noteDDL(name string) {
	s.extraDDL = append(s.extraDDL, name)
}

func (s *Session) rollback(ctx context.Context) {
	if s.txn != nil {
		_ = s.txn.Rollback(ctx)
		s.txn = nil
	}
	s.forgetDDL()
	s.pendingDDL, s.extraDDL, s.pendingWipes = nil, nil, nil
	s.state = StateIdle
}

// ddlTouched reports whether the open transaction's DDL changed name.
func (s *Session) ddlTouched(name string) bool {
	_, bare := catalog.SplitTableName(name)
	for _, lists := range [][]string{s.pendingDDL, s.extraDDL} {
		for _, n := range lists {
			if _, b := catalog.SplitTableName(n); strings.EqualFold(b, bare) {
				return true
			}
		}
	}
	return false
}

// forgetDDL drops the cache entries of the tables the transaction's DDL
// touched (a rollback, or a failed commit): what the cache holds for
// them may be the transaction's own uncommitted view.
func (s *Session) forgetDDL() {
	for _, lists := range [][]string{s.pendingDDL, s.extraDDL} {
		for _, n := range lists {
			s.cat.Invalidate(n)
		}
	}
}

// evalExpr evaluates an expression given an optional row context.
// exprEnv resolves column references during expression evaluation: a
// single table's descriptor+row, or a joined row with side-qualified
// names. Everything else about evaluation (literals, params, functions,
// paths, arithmetic) is environment-independent.
type exprEnv interface {
	col(name string) (types.Datum, error)
}

// tableEnv is the single-table environment: descriptor + row map. A nil
// descriptor or row means column references are simply not available
// (constant-only contexts like access planning).
type tableEnv struct {
	desc *catalog.TableDescriptor
	row  map[catalog.ColumnID]types.Datum
}

func (t tableEnv) col(name string) (types.Datum, error) {
	if t.desc == nil || t.row == nil {
		return types.Datum{}, newErrf(CodeUndefinedColumn, "column %q is not available in this context", name)
	}
	col, ok := t.desc.Col(name)
	if !ok {
		return types.Datum{}, newErrf(CodeUndefinedColumn, "column %q does not exist", name)
	}
	d, ok := t.row[col.ID]
	if !ok {
		d = types.DNull
	}
	return d, nil
}

// evalExpr evaluates an expression against a single table's row — the
// historical signature, kept for its many call sites; join evaluation
// passes a joinEnv to evalExprEnv directly.
func evalExpr(e parser.Expr, desc *catalog.TableDescriptor, row map[catalog.ColumnID]types.Datum, params []types.Datum) (types.Datum, error) {
	return evalExprEnv(e, tableEnv{desc: desc, row: row}, params)
}

func evalExprEnv(e parser.Expr, env exprEnv, params []types.Datum) (types.Datum, error) {
	var base types.Datum
	switch {
	case e.Left != nil:
		d, err := evalExprEnv(*e.Left, env, params)
		if err != nil {
			return types.Datum{}, err
		}
		base = d
	case e.Case != nil:
		d, err := evalCase(e.Case, env, params)
		if err != nil {
			return types.Datum{}, err
		}
		base = d
	case e.Func != "":
		d, err := evalFunc(e, env, params)
		if err != nil {
			return types.Datum{}, err
		}
		base = d
	case e.Lit != nil:
		base = *e.Lit
	case e.Param > 0:
		if e.Param > len(params) {
			return types.Datum{}, newErrf(CodeInvalidParameter, "missing value for parameter $%d", e.Param)
		}
		base = params[e.Param-1]
	case e.Column != "":
		d, err := env.col(e.Column)
		if err != nil {
			return types.Datum{}, err
		}
		base = d
	case e.Cmp != nil:
		// A predicate as a value is three-valued: a comparison with a
		// NULL operand is NULL, not false (WHERE's rule).
		ok, null, err := cond3(*e.Cmp, env, params)
		if err != nil {
			return types.Datum{}, err
		}
		if null {
			base = types.DNull
		} else {
			base = types.NewBool(ok)
		}
	case e.Sub != nil:
		// Scalar subqueries are evaluated and spliced before execution.
		return types.Datum{}, newErrf(CodeInternal, "unresolved scalar subquery")
	default:
		return types.Datum{}, newErrf(CodeInternal, "empty expression")
	}
	if len(e.Path) > 0 {
		var err error
		base, err = applyPath(base, e.Path)
		if err != nil {
			return types.Datum{}, err
		}
	}
	if e.BinOp != "" {
		rhs, err := evalExprEnv(*e.Right, env, params)
		if err != nil {
			return types.Datum{}, err
		}
		if base, err = applyArith(base, e.BinOp, rhs); err != nil {
			return types.Datum{}, err
		}
	}
	if e.Cast != "" {
		d, err := builtins.Cast(base, e.Cast)
		return d, builtinErr(err)
	}
	return base, nil
}

func applyArith(l types.Datum, op string, r types.Datum) (types.Datum, error) {
	if l.Null || r.Null {
		return types.DNull, nil
	}
	if d, ok, err := builtins.DateArith(l, op, r); ok {
		return d, builtinErr(err)
	}
	switch op {
	case "||":
		return types.NewString(l.Text() + r.Text()), nil
	case "%":
		d, err := builtins.Mod(l, r)
		return d, builtinErr(err)
	case "^":
		d, err := builtins.Power(l, r)
		return d, builtinErr(err)
	}
	if l.Fam == types.Int && r.Fam == types.Int {
		// Integer arithmetic overflows to an error, never wraps.
		switch op {
		case "+":
			s := l.I + r.I
			if (s > l.I) != (r.I > 0) && r.I != 0 {
				return types.Datum{}, newErrf(CodeNumericValueOutOfRange, "integer out of range")
			}
			return types.NewInt(s), nil
		case "-":
			s := l.I - r.I
			if (s < l.I) != (r.I > 0) && r.I != 0 {
				return types.Datum{}, newErrf(CodeNumericValueOutOfRange, "integer out of range")
			}
			return types.NewInt(s), nil
		case "*":
			if l.I != 0 && r.I != 0 {
				p := l.I * r.I
				if p/r.I != l.I || (l.I == -1 && r.I == math.MinInt64) || (r.I == -1 && l.I == math.MinInt64) {
					return types.Datum{}, newErrf(CodeNumericValueOutOfRange, "integer out of range")
				}
				return types.NewInt(p), nil
			}
			return types.NewInt(0), nil
		case "/":
			// Integer division truncates (PostgreSQL semantics).
			if r.I == 0 {
				return types.Datum{}, newErrf(CodeDivisionByZero, "division by zero")
			}
			return types.NewInt(l.I / r.I), nil
		}
	}
	// Exact decimal arithmetic when either side is DECIMAL and the other
	// lifts exactly (DECIMAL or INT). A FLOAT operand falls through to
	// float arithmetic — mixing binary rounding in and calling the result
	// exact would be a lie.
	if (l.Fam == types.Decimal || r.Fam == types.Decimal) &&
		(l.Fam == types.Decimal || l.Fam == types.Int) &&
		(r.Fam == types.Decimal || r.Fam == types.Int) {
		ld, err := l.Coerce(types.Decimal)
		if err != nil {
			return types.Datum{}, newErrf(CodeInternal, "%v", err)
		}
		rd, err := r.Coerce(types.Decimal)
		if err != nil {
			return types.Datum{}, newErrf(CodeInternal, "%v", err)
		}
		lv, err := ld.DecimalVal()
		if err != nil {
			return types.Datum{}, newErrf(CodeInternal, "%v", err)
		}
		rv, err := rd.DecimalVal()
		if err != nil {
			return types.Datum{}, newErrf(CodeInternal, "%v", err)
		}
		switch op {
		case "+":
			return types.NewDecimal(decimal.Add(lv, rv).String()), nil
		case "-":
			return types.NewDecimal(decimal.Sub(lv, rv).String()), nil
		case "*":
			return types.NewDecimal(decimal.Mul(lv, rv).String()), nil
		case "/":
			q, err := decimal.DivQuantize(lv, rv, 6)
			if err != nil {
				return types.Datum{}, newErrf(CodeDivisionByZero, "division by zero")
			}
			return types.NewDecimal(q.String()), nil
		}
	}
	lf, err := l.Coerce(types.Float)
	if err != nil {
		return types.Datum{}, newErrf(CodeFeatureNotSupported, "unsupported operand for %s: %s", op, l.Fam)
	}
	rf, err := r.Coerce(types.Float)
	if err != nil {
		return types.Datum{}, newErrf(CodeFeatureNotSupported, "unsupported operand for %s: %s", op, r.Fam)
	}
	switch op {
	case "+":
		return types.NewFloat(lf.F + rf.F), nil
	case "-":
		return types.NewFloat(lf.F - rf.F), nil
	case "*":
		return types.NewFloat(lf.F * rf.F), nil
	case "/":
		if rf.F == 0 {
			return types.Datum{}, newErrf(CodeDivisionByZero, "division by zero")
		}
		return types.NewFloat(lf.F / rf.F), nil
	}
	return types.Datum{}, newErrf(CodeFeatureNotSupported, "unsupported operator %q", op)
}

// evalFunc evaluates a builtin scalar call ("now" never reaches here —
// the session splices it before execution).
func evalFunc(e parser.Expr, env exprEnv, params []types.Datum) (types.Datum, error) {
	args := make([]types.Datum, len(e.Args))
	for i, a := range e.Args {
		d, err := evalExprEnv(a, env, params)
		if err != nil {
			return types.Datum{}, err
		}
		args[i] = d
	}
	if b, ok := builtins.Lookup(e.Func); ok && !b.Session {
		d, err := b.Call(args)
		if err != nil {
			var be *builtins.Error
			if errors.As(err, &be) {
				return types.Datum{}, newErrf(be.Code, "%s", be.Msg)
			}
			return types.Datum{}, err
		}
		return d, nil
	}
	switch e.Func {
	case "quote_ident":
		if args[0].Null {
			return types.DNull, nil
		}
		return types.NewString(`"` + strings.ReplaceAll(args[0].Text(), `"`, `""`) + `"`), nil
	case "pg_typeof":
		return types.NewString(vtable.TypeName(args[0].Fam)), nil
	case "format_type":
		if args[0].Null {
			return types.DNull, nil
		}
		return types.NewString(vtable.FormatTypeOID(args[0].I)), nil
	case "now":
		return types.Datum{}, newErrf(CodeInternal, "now() was not resolved before execution")
	case "array_to_string":
		if args[0].Null || args[1].Null {
			return types.DNull, nil
		}
		return types.NewString(strings.Join(arrayElems(args[0].Text()), args[1].Text())), nil
	case "pg_size_pretty":
		if args[0].Null {
			return types.DNull, nil
		}
		n, err := args[0].Coerce(types.Int)
		if err != nil {
			return types.DNull, nil
		}
		return types.NewString(sizePretty(n.I)), nil
	case "pg_table_size", "pg_total_relation_size", "pg_relation_size", "pg_database_size",
		"pg_get_function_result", "pg_get_function_arguments", "pg_get_function_identity_arguments",
		"pg_get_functiondef", "pg_get_triggerdef", "pg_get_ruledef":
		return types.DNull, nil
	case "pg_char_to_encoding":
		return types.NewInt(6), nil // UTF8
	case "getdatabaseencoding":
		return types.NewString("UTF8"), nil
	case "pg_tablespace_location":
		return types.NewString(""), nil
	case "has_database_privilege", "has_table_privilege", "has_schema_privilege", "pg_type_is_visible", "pg_function_is_visible":
		return types.NewBool(true), nil
	}
	return types.Datum{}, newErrf(CodeUndefinedFunction, "unknown function %q", e.Func)
}

// sizePretty renders bytes the way pg_size_pretty does.
func sizePretty(n int64) string {
	units := []string{"bytes", "kB", "MB", "GB", "TB", "PB"}
	v := float64(n)
	i := 0
	for i < len(units)-1 && (v >= 10240 || v <= -10240) {
		v /= 1024
		i++
	}
	if i == 0 {
		return fmt.Sprintf("%d bytes", n)
	}
	return fmt.Sprintf("%.0f %s", v, units[i])
}

// matchesWhere evaluates the conjunction against a full row.
func matchesWhere(where []parser.Comparison, desc *catalog.TableDescriptor, row map[catalog.ColumnID]types.Datum, params []types.Datum) (bool, error) {
	for _, cmp := range where {
		// Constant conjuncts (evaluated EXISTS subqueries) bind no column.
		if cmp.Op == "TRUE" {
			continue
		}
		if cmp.Op == "FALSE" {
			return false, nil
		}
		if cmp.Expr != nil {
			// Computed left-hand side: evaluate both sides and compare raw
			// (Compare lifts numerics; no column type to coerce toward).
			ok, err := condsHold([]parser.Comparison{cmp}, tableEnv{desc: desc, row: row}, params)
			if err != nil {
				return false, err
			}
			if !ok {
				return false, nil
			}
			continue
		}
		if cmp.Op == "OR" {
			matched := false
			for _, disjunct := range cmp.Or {
				for _, inner := range disjunct {
					if inner.Sub != nil {
						return false, newErrf(CodeInternal, "an IN or EXISTS subquery inside OR was not resolved before execution")
					}
				}
				ok, err := matchesWhere(disjunct, desc, row, params)
				if err != nil {
					return false, err
				}
				if ok {
					matched = true
					break
				}
			}
			if !matched {
				return false, nil
			}
			continue
		}
		col, ok := desc.Col(cmp.Column)
		if !ok {
			return false, newErrf(CodeUndefinedColumn, "column %q does not exist", cmp.Column)
		}
		lhs, ok := row[col.ID]
		if !ok {
			lhs = types.DNull
		}
		// A ->/->> path replaces the column value and its comparison type.
		cmpType := col.Type
		if len(cmp.Path) > 0 {
			var perr error
			lhs, perr = applyPath(lhs, cmp.Path)
			if perr != nil {
				return false, perr
			}
			cmpType = pathResultType(cmp.Path)
		}
		if cmp.Op == "IS NULL" || cmp.Op == "IS NOT NULL" {
			if lhs.Null != (cmp.Op == "IS NULL") {
				return false, nil
			}
			continue
		}
		if nullAwareOp(cmp.Op) {
			var rhs types.Datum
			if !isBoolTestOp(cmp.Op) {
				v, err := evalExpr(cmp.Value, desc, row, params)
				if err != nil {
					return false, err
				}
				rhs = v
			}
			ok, err := applyNullAware(cmp.Op, lhs, rhs)
			if err != nil {
				return false, err
			}
			if !ok {
				return false, nil
			}
			continue
		}
		if cmp.Op == "IN" || cmp.Op == "NOT IN" {
			match, err := matchesIn(cmp, cmpType, lhs, desc, row, params)
			if err != nil || !match {
				return false, err
			}
			continue
		}
		if cmp.Op == "@>" || cmp.Op == "NOT @>" {
			// JSONB containment. The left side (post-path) must be jsonb —
			// a terminal ->> produces text and is refused.
			if cmpType != types.Jsonb {
				return false, newErrf(CodeFeatureNotSupported, "@> requires jsonb operands (%s is %s)", cmp.Column, cmpType)
			}
			rhs, err := evalExpr(cmp.Value, desc, row, params)
			if err != nil {
				return false, err
			}
			if lhs.Null || rhs.Null {
				return false, nil // NULL operands: UNKNOWN either way
			}
			rhs, err = rhs.Coerce(types.Jsonb)
			if err != nil {
				return false, newErrf(CodeInvalidTextRepresentation, "WHERE %s @>: %v", cmp.Column, err)
			}
			ok, cerr := jsonbContains(lhs, rhs)
			if cerr != nil {
				return false, cerr
			}
			if ok != (cmp.Op == "@>") {
				return false, nil
			}
			continue
		}
		rhs, err := evalExpr(cmp.Value, desc, row, params)
		if err != nil {
			return false, err
		}
		if lhs.Null || rhs.Null {
			return false, nil // NULL comparisons never match
		}
		if !plainCmpOp(cmp.Op) {
			ok, err := applyCmpOpEsc(cmp.Op, lhs, rhs, cmp.Escape)
			if err != nil {
				return false, err
			}
			if !ok {
				return false, nil
			}
			continue
		}
		rhs, err = rhs.Coerce(cmpType)
		if err != nil {
			return false, newErrf(CodeInternal, "WHERE %s: %v", cmp.Column, err)
		}
		c, err := lhs.Compare(rhs)
		if err != nil {
			return false, nil
		}
		if !cmpHolds(cmp.Op, c) {
			return false, nil
		}
	}
	return true, nil
}

// evalCase evaluates a CASE expression: the simple form compares the
// operand with each WHEN value, the searched form tests each condition.
func evalCase(ce *parser.CaseExpr, env exprEnv, params []types.Datum) (types.Datum, error) {
	var operand types.Datum
	if ce.Operand != nil {
		d, err := evalExprEnv(*ce.Operand, env, params)
		if err != nil {
			return types.Datum{}, err
		}
		operand = d
	}
	for _, w := range ce.Whens {
		hit := false
		if ce.Operand != nil {
			v, err := evalExprEnv(*w.Value, env, params)
			if err != nil {
				return types.Datum{}, err
			}
			if !operand.Null && !v.Null {
				if c, err := operand.Compare(v); err == nil && c == 0 {
					hit = true
				}
			}
		} else {
			ok, err := condsHold(w.Cond, env, params)
			if err != nil {
				return types.Datum{}, err
			}
			hit = ok
		}
		if hit {
			return evalExprEnv(w.Result, env, params)
		}
	}
	if ce.Else != nil {
		return evalExprEnv(*ce.Else, env, params)
	}
	return types.DNull, nil
}

// condsHold evaluates WHERE-shaped conjuncts against an expression
// environment (CASE WHEN conditions): comparisons, IS NULL, IN lists, OR.
func condsHold(conds []parser.Comparison, env exprEnv, params []types.Datum) (bool, error) {
	for _, cmp := range conds {
		switch cmp.Op {
		case "TRUE":
			continue
		case "FALSE":
			return false, nil
		case "OR":
			any := false
			for _, alt := range cmp.Or {
				ok, err := condsHold(alt, env, params)
				if err != nil {
					return false, err
				}
				if ok {
					any = true
					break
				}
			}
			if !any {
				return false, nil
			}
			continue
		}
		var lhs types.Datum
		var err error
		if cmp.Expr != nil {
			lhs, err = evalExprEnv(*cmp.Expr, env, params)
		} else {
			lhs, err = env.col(cmp.Column)
		}
		if err != nil {
			return false, err
		}
		switch cmp.Op {
		case "IS NULL":
			if !lhs.Null {
				return false, nil
			}
			continue
		case "IS NOT NULL":
			if lhs.Null {
				return false, nil
			}
			continue
		case "IN", "NOT IN":
			found := false
			for _, ve := range cmp.Values {
				v, err := evalExprEnv(ve, env, params)
				if err != nil {
					return false, err
				}
				if !lhs.Null && !v.Null {
					if c, err := lhs.Compare(v); err == nil && c == 0 {
						found = true
						break
					}
				}
			}
			if found != (cmp.Op == "IN") {
				return false, nil
			}
			continue
		}
		var rhs types.Datum
		if !isBoolTestOp(cmp.Op) {
			if rhs, err = evalExprEnv(cmp.Value, env, params); err != nil {
				return false, err
			}
		}
		if nullAwareOp(cmp.Op) {
			ok, err := applyNullAware(cmp.Op, lhs, rhs)
			if err != nil {
				return false, err
			}
			if !ok {
				return false, nil
			}
			continue
		}
		if lhs.Null || rhs.Null {
			return false, nil
		}
		ok, err := applyCmpOpEsc(cmp.Op, lhs, rhs, cmp.Escape)
		if err != nil || !ok {
			return false, nil
		}
	}
	return true, nil
}

// cond3 evaluates one conjunct under three-valued logic: (value, false)
// when it is TRUE or FALSE, (_, true) when it is UNKNOWN.
func cond3(cmp parser.Comparison, env exprEnv, params []types.Datum) (bool, bool, error) {
	switch cmp.Op {
	case "TRUE":
		return true, false, nil
	case "FALSE":
		return false, false, nil
	case "OR":
		// OR of ANDs: TRUE if any disjunct is TRUE, else UNKNOWN if any
		// is UNKNOWN, else FALSE.
		unknown := false
		for _, alt := range cmp.Or {
			ok, null, err := conds3(alt, env, params)
			if err != nil {
				return false, false, err
			}
			if ok {
				return true, false, nil
			}
			unknown = unknown || null
		}
		return false, unknown, nil
	}
	var lhs types.Datum
	var err error
	if cmp.Expr != nil {
		lhs, err = evalExprEnv(*cmp.Expr, env, params)
	} else {
		lhs, err = env.col(cmp.Column)
		if err == nil && len(cmp.Path) > 0 {
			lhs, err = applyPath(lhs, cmp.Path)
		}
	}
	if err != nil {
		return false, false, err
	}
	switch cmp.Op {
	case "IS NULL":
		return lhs.Null, false, nil
	case "IS NOT NULL":
		return !lhs.Null, false, nil
	case "IN", "NOT IN":
		if lhs.Null {
			return false, true, nil
		}
		sawNull := false
		for _, ve := range cmp.Values {
			v, err := evalExprEnv(ve, env, params)
			if err != nil {
				return false, false, err
			}
			if v.Null {
				sawNull = true
				continue
			}
			if c, err := lhs.Compare(v); err == nil && c == 0 {
				return cmp.Op == "IN", false, nil
			}
		}
		if sawNull {
			return false, true, nil
		}
		return cmp.Op != "IN", false, nil
	}
	var rhs types.Datum
	if !isBoolTestOp(cmp.Op) {
		if rhs, err = evalExprEnv(cmp.Value, env, params); err != nil {
			return false, false, err
		}
	}
	if nullAwareOp(cmp.Op) {
		ok, err := applyNullAware(cmp.Op, lhs, rhs)
		return ok, false, err
	}
	if lhs.Null || rhs.Null {
		return false, true, nil
	}
	ok, err := applyCmpOpEsc(cmp.Op, lhs, rhs, cmp.Escape)
	if err != nil {
		return false, false, err
	}
	return ok, false, nil
}

// conds3 is the three-valued AND of a conjunction.
func conds3(conds []parser.Comparison, env exprEnv, params []types.Datum) (bool, bool, error) {
	unknown := false
	for _, c := range conds {
		ok, null, err := cond3(c, env, params)
		if err != nil {
			return false, false, err
		}
		if !ok && !null {
			return false, false, nil
		}
		unknown = unknown || null
	}
	return !unknown, unknown, nil
}

// jsonOp evaluates the jsonb operators: @> and <@ (containment), ?
// (the key or string element exists), ?| and ?& (any / all of a text
// array of keys exist).
func jsonOp(op string, lhs, rhs types.Datum) (bool, error) {
	l, err := lhs.Coerce(types.Jsonb)
	if err != nil {
		return false, newErrf(CodeFeatureNotSupported, "%s requires a jsonb left operand (got %s)", op, lhs.Fam)
	}
	switch op {
	case "@>", "<@":
		r, err := rhs.Coerce(types.Jsonb)
		if err != nil {
			return false, newErrf(CodeInvalidTextRepresentation, "%s: %v", op, err)
		}
		if op == "<@" {
			l, r = r, l
		}
		return jsonbContains(l, r)
	case "?":
		v, err := decodeJSONValue(l.S)
		if err != nil {
			return false, err
		}
		return builtins.JSONHasKey(v, rhs.Text()), nil
	case "?|", "?&":
		v, err := decodeJSONValue(l.S)
		if err != nil {
			return false, err
		}
		keys := builtins.TextArrayElems(rhs.Text())
		for _, k := range keys {
			has := builtins.JSONHasKey(v, k)
			if has && op == "?|" {
				return true, nil
			}
			if !has && op == "?&" {
				return false, nil
			}
		}
		return op == "?&" && len(keys) > 0, nil
	}
	return false, newErrf(CodeInternal, "unknown jsonb operator %q", op)
}

func decodeJSONValue(s string) (any, error) {
	var v any
	if err := decodeJSONNumber(s, &v); err != nil {
		return nil, newErrf(CodeInternal, "malformed stored jsonb: %v", err)
	}
	return v, nil
}

// builtinErr converts a builtins error into a SQL error with its
// SQLSTATE (nil stays nil).
func builtinErr(err error) error {
	if err == nil {
		return nil
	}
	var be *builtins.Error
	if errors.As(err, &be) {
		return newErrf(be.Code, "%s", be.Msg)
	}
	return err
}

// boolTest evaluates x IS [NOT] TRUE | FALSE: a NULL is neither.
func boolTest(op string, d types.Datum) (bool, error) {
	want := strings.HasSuffix(op, "TRUE")
	not := strings.Contains(op, "NOT")
	if d.Null {
		return not, nil
	}
	b, err := d.Coerce(types.Bool)
	if err != nil {
		return false, newErrf(CodeInvalidTextRepresentation, "%s: %v", op, err)
	}
	return (b.B == want) != not, nil
}

// distinctFrom evaluates x IS [NOT] DISTINCT FROM y: NULLs compare as a
// value, equal to each other and different from everything else.
func distinctFrom(op string, lhs, rhs types.Datum) (bool, error) {
	distinct := lhs.Null != rhs.Null
	if !lhs.Null && !rhs.Null {
		if rhs.Fam != lhs.Fam {
			if c, err := rhs.Coerce(lhs.Fam); err == nil {
				rhs = c
			}
		}
		c, err := lhs.Compare(rhs)
		distinct = err != nil || c != 0
	}
	return distinct != strings.Contains(op, "NOT"), nil
}

// isBoolTestOp reports the one-sided IS [NOT] TRUE | FALSE tests.
func isBoolTestOp(op string) bool {
	return strings.HasSuffix(op, "TRUE") || strings.HasSuffix(op, "FALSE")
}

// nullAwareOp reports whether op decides on NULL operands itself
// (instead of the NULL-never-matches rule).
func nullAwareOp(op string) bool {
	switch op {
	case "IS TRUE", "IS NOT TRUE", "IS FALSE", "IS NOT FALSE", "IS DISTINCT FROM", "IS NOT DISTINCT FROM":
		return true
	}
	return false
}

// applyNullAware evaluates the null-aware operators.
func applyNullAware(op string, lhs, rhs types.Datum) (bool, error) {
	if strings.Contains(op, "DISTINCT") {
		return distinctFrom(op, lhs, rhs)
	}
	return boolTest(op, lhs)
}

// applyCmpOp applies an operator to two values: ordering operators
// through Compare, the regular-expression operators on text.
func applyCmpOp(op string, lhs, rhs types.Datum) (bool, error) {
	return applyCmpOpEsc(op, lhs, rhs, "")
}

// applyCmpOpEsc is applyCmpOp with a pattern's ESCAPE character ("" =
// backslash, parser.NoEscape = none).
func applyCmpOpEsc(op string, lhs, rhs types.Datum, escape string) (bool, error) {
	switch op {
	case "~", "!~", "~*", "!~*":
		re, err := regexFor(rhs.Text(), op == "~*" || op == "!~*")
		if err != nil {
			return false, err
		}
		m := re.MatchString(lhs.Text())
		return m == (op == "~" || op == "~*"), nil
	}
	switch op {
	case "LIKE", "NOT LIKE", "ILIKE", "NOT ILIKE":
		re, err := regexFor(likeToRegexpEsc(rhs.Text(), escape), strings.HasSuffix(op, "ILIKE"))
		if err != nil {
			return false, err
		}
		m := re.MatchString(lhs.Text())
		return m == !strings.HasPrefix(op, "NOT"), nil
	case "SIMILAR TO", "NOT SIMILAR TO":
		re, err := regexFor(similarToRegexp(rhs.Text(), escape), false)
		if err != nil {
			return false, newErrf(CodeInvalidRegexp, "invalid SIMILAR TO pattern: %v", err)
		}
		m := re.MatchString(lhs.Text())
		return m == !strings.HasPrefix(op, "NOT"), nil
	case "@>", "NOT @>", "<@", "NOT <@", "?", "NOT ?", "?|", "NOT ?|", "?&", "NOT ?&":
		ok, err := jsonOp(strings.TrimPrefix(op, "NOT "), lhs, rhs)
		if err != nil {
			return false, err
		}
		return ok == !strings.HasPrefix(op, "NOT "), nil
	}
	if i := strings.IndexByte(op, ' '); i >= 0 {
		// Quantified comparison over a text array value ('{a,b}'): ANY
		// holds when some element does, ALL when every element does.
		base, quant := op[:i], op[i+1:]
		for _, el := range arrayElems(rhs.Text()) {
			ed := types.NewString(el)
			if c, err := ed.Coerce(lhs.Fam); err == nil {
				ed = c
			}
			ok, err := applyCmpOp(base, lhs, ed)
			if err != nil {
				return false, err
			}
			if ok && quant == "ANY" {
				return true, nil
			}
			if !ok && quant == "ALL" {
				return false, nil
			}
		}
		return quant == "ALL", nil
	}
	if lhs.Fam != rhs.Fam {
		if c, err := rhs.Coerce(lhs.Fam); err == nil {
			rhs = c
		}
	}
	c, err := lhs.Compare(rhs)
	if err != nil {
		return false, err
	}
	return cmpHolds(op, c), nil
}

// likeToRegexp translates a LIKE pattern (% and _ wildcards, backslash
// escapes) into an anchored regular expression.
func likeToRegexp(pattern string) string {
	return likeToRegexpEsc(pattern, "")
}

// escapeRune resolves a pattern's ESCAPE character: backslash by
// default, none for parser.NoEscape.
func escapeRune(escape string) (rune, bool) {
	switch escape {
	case "":
		return '\\', true
	case parser.NoEscape:
		return 0, false
	}
	r := []rune(escape)
	return r[0], true
}

func likeToRegexpEsc(pattern, escape string) string {
	esc, hasEsc := escapeRune(escape)
	var b strings.Builder
	b.WriteString("^")
	escaped := false
	for _, r := range pattern {
		switch {
		case escaped:
			b.WriteString(regexp.QuoteMeta(string(r)))
			escaped = false
		case hasEsc && r == esc:
			escaped = true
		case r == '%':
			b.WriteString(".*")
		case r == '_':
			b.WriteString(".")
		default:
			b.WriteString(regexp.QuoteMeta(string(r)))
		}
	}
	b.WriteString("$")
	return b.String()
}

// plainCmpOp reports whether op is an ordering comparison the typed
// Coerce-and-Compare path handles; the rest go through applyCmpOp.
func plainCmpOp(op string) bool {
	switch op {
	case "=", "!=", "<", "<=", ">", ">=":
		return true
	}
	return false
}

// arrayElems splits a PostgreSQL text array literal ('{a,"b c",d}') into
// its elements. NULL elements come back as the text "NULL".
func arrayElems(text string) []string {
	text = strings.TrimSpace(text)
	if len(text) >= 2 && text[0] == '{' && text[len(text)-1] == '}' {
		text = text[1 : len(text)-1]
	} else {
		// An int2vector (pg_index.indkey): space-separated, no braces.
		return strings.Fields(text)
	}
	if text == "" {
		return nil
	}
	var out []string
	var cur strings.Builder
	quoted, escaped := false, false
	for _, r := range text {
		switch {
		case escaped:
			cur.WriteRune(r)
			escaped = false
		case r == '\\':
			escaped = true
		case r == '"':
			quoted = !quoted
		case r == ',' && !quoted:
			out = append(out, cur.String())
			cur.Reset()
		default:
			cur.WriteRune(r)
		}
	}
	out = append(out, cur.String())
	return out
}

var (
	regexMu    sync.Mutex
	regexCache = map[string]*regexp.Regexp{}
)

// regexFor compiles a POSIX-style pattern (Go's RE2 covers what psql and
// the tools send), cached.
// similarToRegexp translates a SIMILAR TO pattern — SQL's % and _
// wildcards over a regular expression using | * + ? {m,n} ( ) [ ] —
// into an anchored RE2 expression.
func similarToRegexp(pattern, escape string) string {
	esc, hasEsc := escapeRune(escape)
	var b strings.Builder
	b.WriteString("^(?:")
	escaped := false
	for _, r := range pattern {
		switch {
		case escaped:
			b.WriteString(regexp.QuoteMeta(string(r)))
			escaped = false
		case hasEsc && r == esc:
			escaped = true
		case r == '%':
			b.WriteString(".*")
		case r == '_':
			b.WriteString(".")
		case strings.ContainsRune("|*+?{}()[]", r):
			b.WriteRune(r)
		default:
			b.WriteString(regexp.QuoteMeta(string(r)))
		}
	}
	b.WriteString(")$")
	return b.String()
}

func regexFor(pattern string, fold bool) (*regexp.Regexp, error) {
	key := pattern
	if fold {
		key = "(?i)" + pattern
	}
	regexMu.Lock()
	defer regexMu.Unlock()
	if re, ok := regexCache[key]; ok {
		return re, nil
	}
	re, err := regexp.Compile(key)
	if err != nil {
		return nil, newErrf(CodeInvalidParameterValue, "invalid regular expression %q: %v", pattern, err)
	}
	if len(regexCache) > 256 {
		regexCache = map[string]*regexp.Regexp{}
	}
	regexCache[key] = re
	return re, nil
}

// cmpHolds applies a comparison operator to a Compare result.
func cmpHolds(op string, c int) bool {
	switch op {
	case "=":
		return c == 0
	case "!=":
		return c != 0
	case "<":
		return c < 0
	case "<=":
		return c <= 0
	case ">":
		return c > 0
	case ">=":
		return c >= 0
	}
	return false
}

// matchesIn evaluates col [NOT] IN (values) with SQL three-valued
// semantics collapsed to WHERE's boolean: a NULL left-hand side never
// matches; a NULL element can never prove NOT IN (x NOT IN (..., NULL) is
// UNKNOWN unless x matches a non-NULL element).
func matchesIn(cmp parser.Comparison, cmpType types.Family, lhs types.Datum, desc *catalog.TableDescriptor, row map[catalog.ColumnID]types.Datum, params []types.Datum) (bool, error) {
	if lhs.Null {
		return false, nil
	}
	matched, hasNull := false, false
	for _, ve := range cmp.Values {
		d, err := evalExpr(ve, desc, row, params)
		if err != nil {
			return false, err
		}
		if d.Null {
			hasNull = true
			continue
		}
		d, cerr := d.Coerce(cmpType)
		if cerr != nil {
			return false, newErrf(CodeInternal, "IN %s: %v", cmp.Column, cerr)
		}
		if c, err := lhs.Compare(d); err == nil && c == 0 {
			matched = true
			break
		}
	}
	if cmp.Op == "IN" {
		return matched, nil
	}
	return !matched && !hasNull, nil
}

// execRollbackToSavepoint restores the named savepoint, escaping the
// in-failed-transaction state (25P02) per PostgreSQL semantics. A
// transaction whose coordinator already finished (aborted by a conflict)
// cannot be rescued — the retryable error stands.
func (s *Session) execRollbackToSavepoint(ctx context.Context, name string) (*Result, *Error) {
	if s.txn == nil {
		return nil, newErrf(CodeNoActiveTransaction, "ROLLBACK TO SAVEPOINT can only be used in transaction blocks")
	}
	if err := s.txn.RollbackToSavepoint(ctx, name); err != nil {
		if kvclient.IsRetryable(err) {
			s.state = StateFailed
			return nil, ToSQLError(err)
		}
		return nil, newErrf(CodeInvalidSavepoint, "%v", err)
	}
	s.state = StateOpen
	return &Result{Tag: "ROLLBACK"}, nil
}
