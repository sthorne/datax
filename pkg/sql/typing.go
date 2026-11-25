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
// arm; a date arm and a timestamp arm meet at timestamp, as in
// PostgreSQL). types.Unknown when it cannot be told (callers describe as
// text).
func exprFamily(e parser.Expr, colType func(name string) (types.Family, bool)) types.Family {
	fam := types.Unknown
	switch {
	case e.Left != nil:
		fam = exprFamily(*e.Left, colType)
	case e.Case != nil:
		fam = armsFamily(exprArms(e), colType)
	case isArmsFunc(e.Func):
		fam = armsFamily(e.Args, colType)
	case e.Func == "array_construct":
		elem := types.String
		for _, a := range e.Args {
			if f := exprFamily(a, colType); f != types.Unknown {
				elem = f
				break
			}
		}
		fam = types.ArrayOf(elem)
	case e.Func == "array_subscript" && len(e.Args) == 2:
		fam = exprFamily(e.Args[0], colType)
		if fam != types.Jsonb {
			fam = fam.Elem()
		}
	case e.Func == "array" && len(e.Args) == 1 && e.Args[0].Sub != nil:
		fam = types.ArrayOf(types.String)
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
		// A NULL carrying a family (a column's value substituted into
		// a correlated or lateral query) is typed by it.
		fam = e.Lit.Fam
	case e.Column != "":
		if colType != nil {
			if f, ok := colType(e.Column); ok {
				fam = f
			}
		}
	case e.Cmp != nil:
		fam = types.Bool
	case e.Agg != nil:
		fam = aggFamily(*e.Agg, colType)
	}
	if len(e.Path) > 0 {
		fam = pathResultType(e.Path)
	}
	if e.BinOp != "" && e.Right != nil {
		rhs := exprFamily(*e.Right, colType)
		if f, ok := jsonbConcatFamily(e, fam, rhs); ok {
			fam = f
		} else {
			fam = arithFamily(e.BinOp, fam, rhs)
		}
	}
	if e.Cast != "" {
		fam = builtins.CastFamily(e.Cast, fam)
	}
	return fam
}

// isArmsFunc reports the functions whose result is their arguments'
// common type, as a CASE's is its arms'.
func isArmsFunc(name string) bool {
	switch name {
	case "coalesce", "greatest", "least", "nullif":
		return true
	}
	return false
}

// exprArms are the expressions whose common type e yields: a CASE's
// results, an arms function's arguments (isArmsFunc); nil otherwise.
func exprArms(e parser.Expr) []parser.Expr {
	switch {
	case e.Case != nil:
		arms := make([]parser.Expr, 0, len(e.Case.Whens)+1)
		for _, w := range e.Case.Whens {
			arms = append(arms, w.Result)
		}
		if e.Case.Else != nil {
			arms = append(arms, *e.Case.Else)
		}
		return arms
	case isArmsFunc(e.Func):
		return e.Args
	}
	return nil
}

// armsFamily is the family arms meet at: the first typed arm's, except
// that a date and a timestamp meet at timestamp, as in PostgreSQL's
// select_common_type (the dates become timestamps: dateArmsKind).
func armsFamily(arms []parser.Expr, colType func(name string) (types.Family, bool)) types.Family {
	fam := types.Unknown
	for _, a := range arms {
		f := exprFamily(a, colType)
		switch {
		case fam == types.Unknown:
			fam = f
		case fam == types.Date && f == types.Timestamp:
			fam = types.Timestamp
		}
		if fam != types.Unknown && fam != types.Date {
			break
		}
	}
	return fam
}

// armsNoTZ reports arms that meet at a timestamp without time zone:
// some arm is a timestamp and every timestamp arm is without time zone
// (a date arm becomes one at its wall-clock midnight). A timestamp
// with time zone among them makes the common type one.
func armsNoTZ(arms []parser.Expr, colType func(name string) (types.Family, bool), colNoTZ func(name string) bool) bool {
	ts := false
	for _, a := range arms {
		if exprFamily(a, colType) != types.Timestamp {
			continue
		}
		if !exprNoTZ(a, colType, colNoTZ) {
			return false
		}
		ts = true
	}
	return ts
}

// dateArmsKind reports whether arms (exprArms) mix a date with a
// timestamp, so a date among them becomes the timestamp they meet at,
// and whether that is a timestamp without time zone. The arms are typed
// through env's columns when it can type them (kindEnv).
func dateArmsKind(arms []parser.Expr, env exprEnv) (mixed, noTZ bool) {
	var colType func(string) (types.Family, bool)
	var colNoTZ func(string) bool
	if ke, ok := env.(kindEnv); ok && env != nil {
		colType = func(n string) (types.Family, bool) {
			c, ok := ke.colKind(n)
			return c.Type, ok
		}
		colNoTZ = func(n string) bool {
			c, ok := ke.colKind(n)
			return ok && c.NoTZ
		}
	}
	date, ts := false, false
	for _, a := range arms {
		switch exprFamily(a, colType) {
		case types.Date:
			date = true
		case types.Timestamp:
			ts = true
		}
	}
	if !date || !ts {
		return false, false
	}
	return true, armsNoTZ(arms, colType, colNoTZ)
}

// aggFamily is the result family of an aggregate call: its argument's
// family through the aggregate's result rule (aggSpec.resultType).
func aggFamily(se parser.SelectExpr, colType func(name string) (types.Family, bool)) types.Family {
	sp := aggSpec{fn: se.Agg}
	switch {
	case se.AggWithinGroup && len(se.AggOrder) == 1 && se.AggOrder[0].Expr != nil:
		sp.argType = exprFamily(*se.AggOrder[0].Expr, colType)
	case se.AggWithinGroup && len(se.AggOrder) == 1:
		sp.argType = exprFamily(parser.Expr{Column: se.AggOrder[0].Column}, colType)
	case se.AggCol != "":
		sp.argType = exprFamily(parser.Expr{Column: se.AggCol}, colType)
	case se.AggArg != nil:
		sp.argType = exprFamily(*se.AggArg, colType)
		// Columns' widths are not known here: an integer column counts
		// as int8 (resolveAggSpec, which has them, settles the rest).
		sp.intWidth = intExprWidth(*se.AggArg, func(string) int32 { return 8 })
	}
	return sp.resultType()
}

// arithFamily is the result family of l op r.
func arithFamily(op string, l, r types.Family) types.Family {
	switch op {
	case "<->", "<=>", "<#>":
		return types.Float // a distance between two vectors
	case "&", "|", "#", "<<", ">>":
		return types.Int
	case "#-":
		return types.Jsonb
	case "-":
		if l == types.Jsonb {
			return types.Jsonb
		}
	case "||":
		if l == types.Tsvector || r == types.Tsvector {
			return types.Tsvector
		}
		if l.IsArray() {
			return l
		}
		if r.IsArray() {
			return r
		}
		if l == types.Bytes && r == types.Bytes {
			return types.Bytes
		}
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
	case l == types.Vector || r == types.Vector:
		return types.Vector // element-wise + and -
	case l == types.Timestamp && (r == types.Timestamp || r == types.Date) && op == "-",
		l == types.Date && r == types.Timestamp && op == "-":
		return types.IntervalFam
	case l == types.IntervalFam && r == types.IntervalFam,
		l == types.IntervalFam && rn > 0 && (op == "*" || op == "/"),
		ln > 0 && r == types.IntervalFam && op == "*":
		return types.IntervalFam
	case l == types.Time && r == types.Time && op == "-":
		return types.IntervalFam
	case l == types.Time && (r == types.IntervalFam || r == types.String), r == types.Time && l == types.IntervalFam:
		return types.Time
	case l == types.Date && r == types.Time, l == types.Time && r == types.Date:
		return types.Timestamp
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
	case "current_time", "localtime":
		return types.Time
	case "nextval", "currval", "lastval", "setval", "unique_rowid", "pg_backend_pid", "to_regtype",
		destroyTenantKeyFunc, rotateTenantKeyFunc, destroyOldKeyVersionsFunc, removeTenantRowsFunc:
		return types.Int
	case "gen_random_uuid", "uuid_generate_v4":
		return types.Uuid
	case "datax_key_wrap", "datax_key_unwrap", "datax_encrypt", "datax_decrypt":
		return types.Bytes
	case "pg_table_is_visible", "has_database_privilege", "has_table_privilege", "has_schema_privilege", "pg_type_is_visible", "pg_function_is_visible", "pg_relation_is_publishable",
		"pg_cancel_backend", "pg_terminate_backend", "pg_is_in_recovery", "pg_has_role", "has_column_privilege", "has_any_column_privilege",
		"has_sequence_privilege", "has_function_privilege", "has_type_privilege", "has_tablespace_privilege",
		"has_foreign_data_wrapper_privilege", "has_server_privilege":
		return types.Bool
	case "to_regclass", "to_regproc", "to_regprocedure", "to_regnamespace", "to_regrole":
		return types.Int
	case "current_schemas":
		return types.ArrayOf(types.String)
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
