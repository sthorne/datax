package sql

import (
	"context"

	"github.com/sthorne/datax/pkg/kvclient"
	"github.com/sthorne/datax/pkg/sql/catalog"
	"github.com/sthorne/datax/pkg/sql/parser"
	"github.com/sthorne/datax/pkg/sql/types"
)

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
	// rolls back, per PostgreSQL semantics).
	if s.state == StateFailed {
		switch stmt.(type) {
		case *parser.Rollback, *parser.Commit:
			s.rollback(ctx)
			return &Result{Tag: "ROLLBACK"}, nil
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
		if err != nil {
			return nil, ToSQLError(err)
		}
		return &Result{Tag: "COMMIT"}, nil

	case *parser.Rollback:
		s.rollback(ctx)
		return &Result{Tag: "ROLLBACK"}, nil

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
	if s.state == StateOpen {
		res, err := s.execStmt(ctx, s.txn, stmt, params)
		if err != nil {
			// Any error fails the explicit transaction (PG semantics).
			s.state = StateFailed
			return nil, ToSQLError(err)
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
	return res, nil
}

func (s *Session) rollback(ctx context.Context) {
	if s.txn != nil {
		_ = s.txn.Rollback(ctx)
		s.txn = nil
	}
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
		col, ok := desc.Col(cmp.Column)
		if !ok {
			return false, newErrf(CodeUndefinedColumn, "column %q does not exist", cmp.Column)
		}
		lhs, ok := row[col.ID]
		if !ok {
			lhs = types.DNull
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
