package sql

import (
	"context"

	"github.com/sthorne/datax/pkg/kvclient"
	"github.com/sthorne/datax/pkg/sql/builtins"
	"github.com/sthorne/datax/pkg/sql/parser"
)

// castTypeNames lists, in order of appearance, the type names stmt's
// casts (x::t, CAST(x AS t)) write that are not built-in types: each
// must name a user-defined type (an enum) of the session's database.
// Every expression position the parser.Walker visits is covered, plus
// the column defaults and CHECK constraints of CREATE TABLE and ALTER
// TABLE.
func castTypeNames(stmt parser.Statement) []string {
	var names []string
	seen := map[string]bool{}
	w := parser.Walker{OnExpr: func(e *parser.Expr) bool {
		if e.Cast != "" && !builtins.KnownCastType(e.Cast) && !seen[e.Cast] {
			seen[e.Cast] = true
			names = append(names, e.Cast)
		}
		return true
	}}
	w.Statement(stmt)
	column := func(c *parser.ColumnDef) {
		if c.DefaultExpr != nil {
			w.WalkExpr(c.DefaultExpr)
		}
		for i := range c.Constraints {
			w.Conds(c.Constraints[i].CheckFails)
		}
	}
	switch t := stmt.(type) {
	case *parser.CreateTable:
		for i := range t.Columns {
			column(&t.Columns[i])
		}
		for i := range t.Constraints {
			w.Conds(t.Constraints[i].CheckFails)
		}
	case *parser.AlterTable:
		cmds := t.Cmds
		if cmds == nil {
			cmds = []*parser.AlterTable{t}
		}
		for _, t := range cmds {
			if t.AddCol != nil {
				column(t.AddCol)
			}
			if t.SetDefault != nil && t.SetDefault.Expr != nil {
				w.WalkExpr(t.SetDefault.Expr)
			}
			if t.AddConstraint != nil {
				w.Conds(t.AddConstraint.CheckFails)
			}
		}
	}
	return names
}

// checkCastTypes refuses a statement that casts to a type that does not
// exist, as PostgreSQL does when it analyzes the statement (42704 type
// "t" does not exist), before any row is read: a cast to a type datax
// does not have (citext, money, inet, ...) would otherwise pass its
// value through unchanged, as text. A name that is not built in is
// looked up as a user-defined type; txn nil reads the catalog in a
// transaction of its own.
func (s *Session) checkCastTypes(ctx context.Context, txn *kvclient.Txn, stmt parser.Statement) *Error {
	names := castTypeNames(stmt)
	if len(names) == 0 {
		return nil
	}
	check := func(ctx context.Context, txn *kvclient.Txn) error {
		for _, name := range names {
			base := builtins.CastBaseType(name)
			_, err := s.lookupType(ctx, txn, base)
			if serr, ok := err.(*Error); ok && serr.Code == CodeUndefinedObject && len(base) > 1 && base[0] == '_' {
				// _mood: the array type of the enum mood.
				_, err = s.lookupType(ctx, txn, base[1:])
			}
			if err != nil {
				if serr, ok := err.(*Error); ok && serr.Code == CodeUndefinedObject {
					return newErrf(CodeUndefinedObject, "type %q does not exist", name)
				}
				return err
			}
		}
		return nil
	}
	var err error
	if txn != nil {
		err = check(ctx, txn)
	} else {
		err = s.db.RunTxn(ctx, "sql-cast-types", check)
	}
	if err != nil {
		return ToSQLError(err)
	}
	return nil
}
