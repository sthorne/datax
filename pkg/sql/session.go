package sql

import (
	"context"
	"errors"
	"fmt"
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

// settings lists the session variables SHOW ALL and pg_settings report:
// the wire's startup parameters plus the ones the session honors.
func (s *Session) settings() [][2]string {
	return [][2]string{
		{"application_name", ""},
		{"client_encoding", "UTF8"},
		{"database", s.database},
		{"DateStyle", "ISO"},
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
	if vtable.IsSchema(db) {
		if vt, ok := vtable.Lookup(name); ok {
			return vt.Descriptor(), nil
		}
		return nil, &catalog.ErrTableNotFound{Name: name}
	}
	d, err := s.cat.LookupIn(ctx, txn, s.database, name)
	if err != nil && db == "" {
		var nf *catalog.ErrTableNotFound
		if errors.As(err, &nf) {
			if vt, ok := vtable.Lookup(bare); ok {
				return vt.Descriptor(), nil
			}
		}
	}
	return d, err
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
		s.pendingDDL = nil
		if err != nil {
			return nil, ToSQLError(err)
		}
		// Schema changes are visible everywhere once COMMIT returns: drain
		// lease adoption for every table this transaction altered.
		for _, name := range pending {
			if derr := s.cat.FinishDDLIn(ctx, s.database, name); derr != nil {
				return nil, ToSQLError(derr)
			}
		}
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
		res, err := s.execStmt(ctx, s.txn, stmt, params)
		if err != nil {
			// Any error fails the explicit transaction (PG semantics).
			s.state = StateFailed
			return nil, ToSQLError(err)
		}
		if name := ddlTableName(stmt); name != "" {
			s.pendingDDL = append(s.pendingDDL, name)
		}
		return res, nil
	}
	var res *Result
	err := s.db.RunTxn(ctx, "sql-implicit", func(ctx context.Context, txn *kvclient.Txn) error {
		var err error
		res, err = s.execStmt(ctx, txn, stmt, params)
		return err
	})
	if err != nil {
		return nil, ToSQLError(err)
	}
	if name := ddlTableName(stmt); name != "" {
		if derr := s.cat.FinishDDLIn(ctx, s.database, name); derr != nil {
			return nil, ToSQLError(derr)
		}
	}
	return res, nil
}

func (s *Session) rollback(ctx context.Context) {
	if s.txn != nil {
		_ = s.txn.Rollback(ctx)
		s.txn = nil
	}
	s.pendingDDL = nil
	s.state = StateIdle
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
		ok, err := condsHold([]parser.Comparison{*e.Cmp}, env, params)
		if err != nil {
			return types.Datum{}, err
		}
		base = types.NewBool(ok)
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
	if e.BinOp == "" {
		return base, nil
	}
	rhs, err := evalExprEnv(*e.Right, env, params)
	if err != nil {
		return types.Datum{}, err
	}
	return applyArith(base, e.BinOp, rhs)
}

func applyArith(l types.Datum, op string, r types.Datum) (types.Datum, error) {
	if l.Null || r.Null {
		return types.DNull, nil
	}
	if op == "||" {
		return types.NewString(l.Text() + r.Text()), nil
	}
	if l.Fam == types.Int && r.Fam == types.Int {
		switch op {
		case "+":
			return types.NewInt(l.I + r.I), nil
		case "-":
			return types.NewInt(l.I - r.I), nil
		case "*":
			return types.NewInt(l.I * r.I), nil
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
	switch e.Func {
	case "coalesce":
		for _, d := range args {
			if !d.Null {
				return d, nil
			}
		}
		return types.DNull, nil
	case "length":
		if args[0].Null {
			return types.DNull, nil
		}
		if args[0].Fam != types.String {
			return types.Datum{}, newErrf(CodeFeatureNotSupported, "length() requires a text argument, got %s", args[0].Fam)
		}
		return types.NewInt(int64(len([]rune(args[0].S)))), nil
	case "lower", "upper":
		if args[0].Null {
			return types.DNull, nil
		}
		if args[0].Fam != types.String {
			return types.Datum{}, newErrf(CodeFeatureNotSupported, "%s() requires a text argument, got %s", e.Func, args[0].Fam)
		}
		if e.Func == "lower" {
			return types.NewString(strings.ToLower(args[0].S)), nil
		}
		return types.NewString(strings.ToUpper(args[0].S)), nil
	case "abs":
		d := args[0]
		if d.Null {
			return types.DNull, nil
		}
		switch d.Fam {
		case types.Int:
			if d.I < 0 {
				return types.NewInt(-d.I), nil
			}
			return d, nil
		case types.Float:
			return types.NewFloat(math.Abs(d.F)), nil
		case types.Decimal:
			v, err := d.DecimalVal()
			if err != nil {
				return types.Datum{}, newErrf(CodeInternal, "%v", err)
			}
			if v.Sign() < 0 {
				return types.NewDecimal(decimal.Neg(v).String()), nil
			}
			return d, nil
		}
		return types.Datum{}, newErrf(CodeFeatureNotSupported, "abs() requires a numeric argument, got %s", d.Fam)
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
			lhs, err := evalExpr(*cmp.Expr, desc, row, params)
			if err != nil {
				return false, err
			}
			rhs, err := evalExpr(cmp.Value, desc, row, params)
			if err != nil {
				return false, err
			}
			if lhs.Null || rhs.Null {
				return false, nil
			}
			ok, err := applyCmpOp(cmp.Op, lhs, rhs)
			if err != nil || !ok {
				return false, nil
			}
			continue
		}
		if cmp.Op == "OR" {
			matched := false
			for _, disjunct := range cmp.Or {
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
			ok, err := applyCmpOp(cmp.Op, lhs, rhs)
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
		rhs, err := evalExprEnv(cmp.Value, env, params)
		if err != nil {
			return false, err
		}
		if lhs.Null || rhs.Null {
			return false, nil
		}
		ok, err := applyCmpOp(cmp.Op, lhs, rhs)
		if err != nil || !ok {
			return false, nil
		}
	}
	return true, nil
}

// applyCmpOp applies an operator to two values: ordering operators
// through Compare, the regular-expression operators on text.
func applyCmpOp(op string, lhs, rhs types.Datum) (bool, error) {
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
		re, err := regexFor(likeToRegexp(rhs.Text()), strings.HasSuffix(op, "ILIKE"))
		if err != nil {
			return false, err
		}
		m := re.MatchString(lhs.Text())
		return m == !strings.HasPrefix(op, "NOT"), nil
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
	var b strings.Builder
	b.WriteString("^")
	escaped := false
	for _, r := range pattern {
		switch {
		case escaped:
			b.WriteString(regexp.QuoteMeta(string(r)))
			escaped = false
		case r == '\\':
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
