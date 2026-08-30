package sql

import (
	"context"
	"fmt"
	"strconv"
	"time"

	"github.com/sthorne/datax/pkg/kvclient"
	"github.com/sthorne/datax/pkg/sql/catalog"
	"github.com/sthorne/datax/pkg/sql/parser"
	"github.com/sthorne/datax/pkg/sql/types"
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

// ResultColumn describes one output column.
type ResultColumn struct {
	Name string
	Type types.Family
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

	txn   *kvclient.Txn
	state TxnState
	// pendingDDL names tables changed inside the open explicit transaction;
	// lease adoption for them is drained at COMMIT.
	pendingDDL []string
}

// NewSession creates a session. The catalog accessor is shared per node.
func NewSession(db *kvclient.DB, cat *catalog.Accessor) *Session {
	return &Session{db: db, cat: cat}
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
			if derr := s.cat.FinishDDL(ctx, name); derr != nil {
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
			return &Result{Columns: []ResultColumn{{Name: t.Name[5:], Type: types.String}}, Tag: "SHOW"}, nil
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
	if ci, ok := stmt.(*parser.CreateIndex); ok {
		if s.state == StateOpen {
			return nil, newErrf(CodeActiveTransaction, "CREATE INDEX cannot run inside a transaction block")
		}
		return s.execCreateIndexOnline(ctx, ci)
	}

	// AS OF SYSTEM TIME pins a SELECT to a fixed past timestamp: it runs
	// in its own read-only historical transaction (servable by follower
	// replicas), so it cannot join an explicit transaction's timestamp.
	if sel, ok := stmt.(*parser.Select); ok && sel.AsOf != "" {
		if s.state == StateOpen {
			return nil, newErrf(CodeActiveTransaction, "AS OF SYSTEM TIME cannot run inside a transaction block")
		}
		if sel.ForUpdate {
			return nil, newErrf(CodeFeatureNotSupported, "FOR UPDATE is not allowed with AS OF SYSTEM TIME")
		}
		ts, err := resolveAsOf(sel.AsOf, s.db.Clock().Now())
		if err != nil {
			return nil, newErrf(CodeSyntaxError, "AS OF SYSTEM TIME: %v", err)
		}
		txn := s.db.NewHistoricalTxn("sql-asof", ts)
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
		if derr := s.cat.FinishDDL(ctx, name); derr != nil {
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
func evalExpr(e parser.Expr, desc *catalog.TableDescriptor, row map[catalog.ColumnID]types.Datum, params []types.Datum) (types.Datum, error) {
	var base types.Datum
	switch {
	case e.Lit != nil:
		base = *e.Lit
	case e.Param > 0:
		if e.Param > len(params) {
			return types.Datum{}, newErrf(CodeInvalidParameter, "missing value for parameter $%d", e.Param)
		}
		base = params[e.Param-1]
	case e.Column != "":
		if desc == nil || row == nil {
			return types.Datum{}, newErrf(CodeUndefinedColumn, "column %q is not available in this context", e.Column)
		}
		col, ok := desc.Col(e.Column)
		if !ok {
			return types.Datum{}, newErrf(CodeUndefinedColumn, "column %q does not exist", e.Column)
		}
		d, ok := row[col.ID]
		if !ok {
			d = types.DNull
		}
		base = d
	case e.Sub != nil:
		// Scalar subqueries are evaluated and spliced before execution.
		return types.Datum{}, newErrf(CodeInternal, "unresolved scalar subquery")
	default:
		return types.Datum{}, newErrf(CodeInternal, "empty expression")
	}
	if e.BinOp == "" {
		return base, nil
	}
	rhs, err := evalExpr(*e.Right, desc, row, params)
	if err != nil {
		return types.Datum{}, err
	}
	return applyArith(base, e.BinOp, rhs)
}

func applyArith(l types.Datum, op string, r types.Datum) (types.Datum, error) {
	if l.Null || r.Null {
		return types.DNull, nil
	}
	if l.Fam == types.Int && r.Fam == types.Int {
		switch op {
		case "+":
			return types.NewInt(l.I + r.I), nil
		case "-":
			return types.NewInt(l.I - r.I), nil
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
	}
	return types.Datum{}, newErrf(CodeFeatureNotSupported, "unsupported operator %q", op)
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
		col, ok := desc.Col(cmp.Column)
		if !ok {
			return false, newErrf(CodeUndefinedColumn, "column %q does not exist", cmp.Column)
		}
		lhs, ok := row[col.ID]
		if !ok {
			lhs = types.DNull
		}
		if cmp.Op == "IS NULL" || cmp.Op == "IS NOT NULL" {
			if lhs.Null != (cmp.Op == "IS NULL") {
				return false, nil
			}
			continue
		}
		if cmp.Op == "IN" || cmp.Op == "NOT IN" {
			match, err := matchesIn(cmp, col, lhs, desc, row, params)
			if err != nil || !match {
				return false, err
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
		rhs, err = rhs.Coerce(col.Type)
		if err != nil {
			return false, newErrf(CodeInternal, "WHERE %s: %v", cmp.Column, err)
		}
		c, err := lhs.Compare(rhs)
		if err != nil {
			return false, nil
		}
		ok = false
		switch cmp.Op {
		case "=":
			ok = c == 0
		case "!=":
			ok = c != 0
		case "<":
			ok = c < 0
		case "<=":
			ok = c <= 0
		case ">":
			ok = c > 0
		case ">=":
			ok = c >= 0
		}
		if !ok {
			return false, nil
		}
	}
	return true, nil
}

// matchesIn evaluates col [NOT] IN (values) with SQL three-valued
// semantics collapsed to WHERE's boolean: a NULL left-hand side never
// matches; a NULL element can never prove NOT IN (x NOT IN (..., NULL) is
// UNKNOWN unless x matches a non-NULL element).
func matchesIn(cmp parser.Comparison, col catalog.Column, lhs types.Datum, desc *catalog.TableDescriptor, row map[catalog.ColumnID]types.Datum, params []types.Datum) (bool, error) {
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
		d, cerr := d.Coerce(col.Type)
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
