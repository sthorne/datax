package sql

import (
	"github.com/sthorne/datax/pkg/sql/builtins"
	"github.com/sthorne/datax/pkg/sql/parser"
	"github.com/sthorne/datax/pkg/sql/types"
)

// exprFamily infers the type an expression produces, for describing
// output columns: literals, columns (through colType), casts, builtin
// calls (the registry's result family), arithmetic (numeric
// promotion), || (text), predicates (bool), CASE (the first typed
// arm). types.Unknown when it cannot be told (callers describe as
// text).
func exprFamily(e parser.Expr, colType func(name string) (types.Family, bool)) types.Family {
	fam := types.Unknown
	switch {
	case e.Left != nil:
		fam = exprFamily(*e.Left, colType)
	case e.Case != nil:
		for _, w := range e.Case.Whens {
			if f := exprFamily(w.Result, colType); f != types.Unknown {
				fam = f
				break
			}
		}
		if fam == types.Unknown && e.Case.Else != nil {
			fam = exprFamily(*e.Case.Else, colType)
		}
	case e.Func != "":
		if b, ok := builtins.Lookup(e.Func); ok {
			args := make([]types.Family, len(e.Args))
			for i, a := range e.Args {
				args[i] = exprFamily(a, colType)
			}
			fam = b.ResultFamily(args)
		} else {
			fam = sessionFuncFamily(e.Func)
		}
	case e.Lit != nil:
		if !e.Lit.Null {
			fam = e.Lit.Fam
		}
	case e.Column != "":
		if colType != nil {
			if f, ok := colType(e.Column); ok {
				fam = f
			}
		}
	case e.Cmp != nil:
		fam = types.Bool
	}
	if len(e.Path) > 0 {
		fam = pathResultType(e.Path)
	}
	if e.BinOp != "" && e.Right != nil {
		rhs := exprFamily(*e.Right, colType)
		fam = arithFamily(e.BinOp, fam, rhs)
	}
	if e.Cast != "" {
		fam = builtins.CastFamily(e.Cast, fam)
	}
	return fam
}

// arithFamily is the result family of l op r.
func arithFamily(op string, l, r types.Family) types.Family {
	switch op {
	case "||":
		return types.String
	case "^":
		if l == types.Int && r == types.Int {
			return types.Int
		}
		return types.Float
	}
	numeric := func(f types.Family) int {
		switch f {
		case types.Int:
			return 1
		case types.Decimal:
			return 2
		case types.Float:
			return 3
		}
		return 0
	}
	ln, rn := numeric(l), numeric(r)
	switch {
	case l == types.Timestamp && (r == types.Timestamp || r == types.Date) && op == "-",
		l == types.Date && r == types.Timestamp && op == "-":
		return types.String // an interval, as text
	case l == types.Timestamp || r == types.Timestamp:
		return types.Timestamp
	case l == types.Date && r == types.Date && op == "-":
		return types.Int
	case l == types.Date && r == types.Int, l == types.Int && r == types.Date:
		return types.Date
	case l == types.Date || r == types.Date:
		return types.Timestamp
	case ln == 0 || rn == 0:
		return types.Unknown
	case ln == 3 || rn == 3:
		return types.Float
	case ln == 2 || rn == 2:
		return types.Decimal
	}
	if op == "/" && l == types.Int && r == types.Int {
		return types.Int
	}
	return types.Int
}

// sessionFuncFamily types the functions the session evaluates itself.
func sessionFuncFamily(name string) types.Family {
	switch name {
	case "now", "current_timestamp", "localtimestamp", "clock_timestamp", "statement_timestamp", "transaction_timestamp":
		return types.Timestamp
	case "current_date":
		return types.Date
	case "nextval", "currval", "lastval", "setval", "unique_rowid", "pg_backend_pid":
		return types.Int
	case "gen_random_uuid", "uuid_generate_v4":
		return types.Uuid
	case "pg_table_is_visible", "has_database_privilege", "has_table_privilege", "has_schema_privilege", "pg_type_is_visible", "pg_function_is_visible", "pg_relation_is_publishable":
		return types.Bool
	}
	return types.String
}

// conformTo brings a computed value to its column's described family
// (a coalesce whose first non-NULL argument was an integer literal in
// a decimal column, say), so the wire carries what Describe promised.
// A value that cannot convert is left as it is.
func conformTo(d types.Datum, fam types.Family) types.Datum {
	if d.Null || fam == types.Unknown || d.Fam == fam {
		return d
	}
	if c, err := builtins.Cast(d, fam.String()); err == nil && c.Fam == fam {
		return c
	}
	return d
}
