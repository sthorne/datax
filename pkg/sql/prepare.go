package sql

import (
	"context"
	"strings"

	"github.com/sthorne/datax/pkg/sql/parser"
	"github.com/sthorne/datax/pkg/sql/types"
)

// SQL-level prepared statements: PREPARE, EXECUTE and DEALLOCATE (what
// pg_dump uses for the queries it repeats per object). They live in
// the session, apart from the wire protocol's named statements.

const (
	// CodeDuplicatePreparedStatement: PREPARE of a name in use.
	CodeDuplicatePreparedStatement = "42P05"
	// CodeInvalidSQLStatementName: EXECUTE or DEALLOCATE of a name
	// that is not prepared.
	CodeInvalidSQLStatementName = "26000"
)

// preparedSQL is one PREPARE'd statement.
type preparedSQL struct {
	stmt  parser.Statement
	types []string
}

// execPrepare runs PREPARE.
func (s *Session) execPrepare(t *parser.Prepare) (*Result, *Error) {
	if _, ok := s.preparedSQL[t.Name]; ok {
		return nil, newErrf(CodeDuplicatePreparedStatement, "prepared statement %q already exists", t.Name)
	}
	switch t.Stmt.(type) {
	case *parser.Prepare, *parser.ExecutePrepared, *parser.Deallocate:
		return nil, newErrf(CodeSyntaxError, "syntax error at or near %q", "PREPARE")
	}
	if s.preparedSQL == nil {
		s.preparedSQL = map[string]*preparedSQL{}
	}
	s.preparedSQL[t.Name] = &preparedSQL{stmt: t.Stmt, types: t.Types}
	return &Result{Tag: "PREPARE"}, nil
}

// execDeallocate runs DEALLOCATE.
func (s *Session) execDeallocate(t *parser.Deallocate) (*Result, *Error) {
	if t.All {
		s.preparedSQL = nil
		return &Result{Tag: "DEALLOCATE ALL"}, nil
	}
	if _, ok := s.preparedSQL[t.Name]; !ok {
		return nil, newErrf(CodeInvalidSQLStatementName, "prepared statement %q does not exist", t.Name)
	}
	delete(s.preparedSQL, t.Name)
	return &Result{Tag: "DEALLOCATE"}, nil
}

// executePrepared resolves EXECUTE's statement and parameter values:
// each argument a constant expression, cast to its declared type.
func (s *Session) executePrepared(t *parser.ExecutePrepared, params []types.Datum) (parser.Statement, []types.Datum, *Error) {
	ps, ok := s.preparedSQL[t.Name]
	if !ok {
		return nil, nil, newErrf(CodeInvalidSQLStatementName, "prepared statement %q does not exist", t.Name)
	}
	if len(ps.types) != len(t.Args) {
		return nil, nil, newErrf(CodeSyntaxError, "wrong number of parameters for prepared statement %q", t.Name)
	}
	vals := make([]types.Datum, len(t.Args))
	for i, a := range t.Args {
		d, err := evalExpr(a, nil, nil, params)
		if err != nil {
			return nil, nil, ToSQLError(err)
		}
		typ := strings.TrimPrefix(ps.types[i], "pg_catalog.")
		if fam, perr := types.ParseType(typ); perr == nil && !d.Null && d.Fam != fam {
			c, cerr := d.Coerce(fam)
			if cerr != nil {
				return nil, nil, newErrf(CodeInvalidTextRepresentation, "parameter $%d of prepared statement %q: %v", i+1, t.Name, cerr)
			}
			d = c
		}
		vals[i] = d
	}
	return ps.stmt, vals, nil
}

// execExecutePrepared runs EXECUTE: the prepared statement, as itself.
func (s *Session) execExecutePrepared(ctx context.Context, t *parser.ExecutePrepared, params []types.Datum) (*Result, *Error) {
	stmt, vals, serr := s.executePrepared(t, params)
	if serr != nil {
		return nil, serr
	}
	return s.Execute(ctx, stmt, vals)
}
