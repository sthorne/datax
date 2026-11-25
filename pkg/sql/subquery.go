package sql

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/sthorne/datax/pkg/kvclient"
	"github.com/sthorne/datax/pkg/sql/builtins"
	"github.com/sthorne/datax/pkg/sql/catalog"
	"github.com/sthorne/datax/pkg/sql/parser"
	"github.com/sthorne/datax/pkg/sql/types"
	"github.com/sthorne/datax/pkg/sql/vtable"
	"github.com/sthorne/datax/pkg/version"
)

// Uncorrelated subqueries: the inner SELECT is evaluated first, within the
// same transaction, and its result is spliced in as a literal (scalar), a
// value list ([NOT] IN), or a constant conjunct ([NOT] EXISTS). The parsed
// AST is never mutated — resolution builds modified copies — so a prepared
// statement re-evaluates its subqueries on every execution.

// execSubSelect executes a subquery. A column or qualifier that fails to
// resolve inside the subquery's own scope is (almost always) a
// correlated reference of a shape the correlation analysis does not
// cover, which is not supported. (Covered shapes resolve every name
// before they run: a genuine typo there is reported as such.)
func (s *Session) execSubSelect(ctx context.Context, txn *kvclient.Txn, sub *parser.Select, params []types.Datum) (*Result, error) {
	res, err := s.execSelect(ctx, txn, sub, params)
	if err != nil {
		if serr, ok := err.(*Error); ok && (serr.Code == CodeUndefinedColumn || serr.Code == CodeUndefinedTable && strings.Contains(serr.Msg, "FROM-clause entry")) {
			return nil, newErrf(CodeFeatureNotSupported, "correlated subqueries are not supported (%s)", serr.Msg)
		}
		return nil, err
	}
	return res, nil
}

// execScalarSub evaluates (SELECT ...) used as a value: exactly one output
// column; zero rows is NULL; more than one row is an error.
// A row subquery (e.SubCol set: SET (a, b) = (SELECT x, y ...)) must
// return e.SubCols columns, and yields column e.SubCol.
func (s *Session) execScalarSub(ctx context.Context, txn *kvclient.Txn, e parser.Expr, params []types.Datum) (types.Datum, error) {
	res, err := s.execSubSelect(ctx, txn, e.Sub, params)
	if err != nil {
		return types.Datum{}, err
	}
	col := 0
	switch {
	case e.SubCol > 0 && len(res.Columns) != e.SubCols:
		return types.Datum{}, newErrf(CodeSyntaxError, "number of columns does not match number of values")
	case e.SubCol > 0:
		col = e.SubCol - 1
	case len(res.Columns) != 1:
		return types.Datum{}, newErrf(CodeSyntaxError, "subquery must return only one column")
	}
	// The value carries the subquery's column type, NULL included, so the
	// expression around it types (and describes) as planning did.
	fam, width := res.Columns[col].Type, res.Columns[col].Width
	switch len(res.Rows) {
	case 0:
		return types.Datum{Null: true, Fam: fam}.WithIntWidth(width), nil
	case 1:
		d := conformTo(res.Rows[0][col], fam)
		if d.Fam == types.Int {
			d = d.WithIntWidth(width)
		}
		return d, nil
	}
	return types.Datum{}, newErrf(CodeCardinality, "more than one row returned by a subquery used as an expression")
}

// scalarSubColumn is the column a scalar subquery's value is described
// as: the subquery's one output column, as PlanColumns describes it
// (PostgreSQL types a scalar sublink by its select item). ok is false
// when the subquery does not describe on its own as one typed column.
func (s *Session) scalarSubColumn(ctx context.Context, sub *parser.Select) (ResultColumn, bool) {
	cols, serr := s.PlanColumns(ctx, sub)
	if serr != nil {
		// A correlated subquery names the enclosing row in its WHERE
		// (or HAVING, ORDER BY), which does not describe on its own;
		// those clauses never change the output's type.
		cols, serr = s.PlanColumns(ctx, outputShape(sub))
	}
	if serr != nil || len(cols) != 1 || cols[0].Type == types.Unknown {
		return ResultColumn{}, false
	}
	return cols[0], true
}

// outputShape is a copy of sel (and of each set-operation member)
// without the clauses that filter or order its rows, which are all a
// correlated subquery's references to an enclosing row usually sit in:
// it describes the same columns.
func outputShape(sel *parser.Select) *parser.Select {
	c := *sel
	c.Where, c.Having, c.OrderBy, c.DistinctOn = nil, nil, nil, nil
	if c.Union != nil {
		c.Union = outputShape(c.Union)
	}
	return &c
}

// bareScalarSub reports whether e is a scalar subquery and nothing else
// ((SELECT ...) as a select item, not an operand, cast or argument).
func bareScalarSub(e parser.Expr) bool {
	return e.Sub != nil && e.SubCol == 0 && e.Lit == nil && e.Param == 0 && e.Column == "" &&
		e.Func == "" && e.BinOp == "" && e.Left == nil && e.Right == nil && e.Case == nil &&
		e.Cmp == nil && e.Cast == "" && len(e.Path) == 0 && e.Window == nil && e.Agg == nil
}

// typeScalarSubItems gives every select item of t that is a bare scalar
// subquery its subquery's column type (width, modifier and the like
// included), keeping the item's name, and conforms rows' values to it.
// Describe (planColumns) and execution (execSelect) both run it, so the
// RowDescription and the values agree whichever path produced them. A
// select list with * (whose expansion moves positions) or a set
// operation (typed by unifying its members) is left as it is.
func (s *Session) typeScalarSubItems(ctx context.Context, t *parser.Select, cols []ResultColumn, rows [][]types.Datum) {
	if t == nil || t.Union != nil || len(cols) != len(t.Exprs) {
		return
	}
	for _, se := range t.Exprs {
		if se.Star {
			return
		}
	}
	for i, se := range t.Exprs {
		if se.Agg != "" || se.Window != nil || !bareScalarSub(se.Expr) {
			continue
		}
		col, ok := s.scalarSubColumn(ctx, se.Expr.Sub)
		if !ok {
			continue
		}
		col.Name = cols[i].Name
		cols[i] = col
		for _, r := range rows {
			if i < len(r) {
				r[i] = conformTo(r[i], col.Type)
			}
		}
	}
}

// typeSubqueryOperands returns e with every scalar subquery that
// describes (scalarSubColumn) replaced by a NULL of its type, for
// typing the expression around it without evaluating anything; a
// condition carrying one ([NOT] EXISTS, IN (SELECT ...)) stands as a
// boolean NULL. ok is false when a subquery remains (one that does not
// describe, or one inside a CASE or ARRAY(SELECT ...)).
func (s *Session) typeSubqueryOperands(ctx context.Context, e parser.Expr) (parser.Expr, bool) {
	ok := true
	var walk func(e parser.Expr) parser.Expr
	walk = func(e parser.Expr) parser.Expr {
		out := e
		if e.Cmp != nil && e.Lit == nil && e.Sub == nil && e.Column == "" && e.Func == "" && e.Left == nil {
			// A condition ([NOT] EXISTS, IN (SELECT ...)) is a boolean,
			// whatever its subqueries read.
			null := types.Datum{Null: true, Fam: types.Bool}
			out.Cmp, out.Lit = nil, &null
			if e.Right != nil {
				r := walk(*e.Right)
				out.Right = &r
			}
			return out
		}
		if e.Cmp != nil || e.Case != nil || e.Func == "array" {
			// CASE arms and ARRAY(SELECT ...) keep their subqueries.
			if exprHasSubquery(e) {
				ok = false
			}
			return e
		}
		if e.Sub != nil {
			if e.SubCol != 0 {
				ok = false
				return e
			}
			col, cok := s.scalarSubColumn(ctx, e.Sub)
			if !cok {
				ok = false
				return e
			}
			null := types.Datum{Null: true, Fam: col.Type}.WithIntWidth(col.Width)
			out.Sub, out.Lit = nil, &null
		}
		if e.Left != nil {
			l := walk(*e.Left)
			out.Left = &l
		}
		if e.Right != nil {
			r := walk(*e.Right)
			out.Right = &r
		}
		if len(e.Args) > 0 {
			out.Args = make([]parser.Expr, len(e.Args))
			for i, a := range e.Args {
				out.Args[i] = walk(a)
			}
		}
		return out
	}
	out := walk(e)
	return out, ok
}

// resolveValueExpr splices the parts of a value expression that must be
// evaluated once, before row-by-row execution: scalar subqueries and
// now() (the statement timestamp).
func (s *Session) resolveValueExpr(ctx context.Context, txn *kvclient.Txn, e parser.Expr, params []types.Datum) (parser.Expr, error) {
	return s.resolveValueExprOpts(ctx, txn, e, params, false)
}

// resolveValueExprOpts is resolveValueExpr; keepVolatile leaves the
// volatile row functions (nextval, ...) in place for a per-row caller.
func (s *Session) resolveValueExprOpts(ctx context.Context, txn *kvclient.Txn, e parser.Expr, params []types.Datum, keepVolatile bool) (parser.Expr, error) {
	out, err := s.resolveValueNode(ctx, txn, e, params, keepVolatile)
	if err != nil {
		return out, err
	}
	return s.zoneNode(out), nil
}

// resolveValueNode is resolveValueExprOpts before the time-zone
// annotation (zoneNode).
func (s *Session) resolveValueNode(ctx context.Context, txn *kvclient.Txn, e parser.Expr, params []types.Datum, keepVolatile bool) (parser.Expr, error) {
	if constArgFuncs[e.Func] && argsNeedSplice(e.Args) {
		// A session function that evaluates its arguments as constants
		// here, given another session function or a subquery as one
		// (pg_cancel_backend(pg_backend_pid()), set_config('a.b',
		// current_user, false)): the arguments resolve first, innermost
		// out, as PostgreSQL evaluates a nested call.
		args := make([]parser.Expr, len(e.Args))
		for i, a := range e.Args {
			ra, err := s.resolveValueExprOpts(ctx, txn, a, params, keepVolatile)
			if err != nil {
				return e, err
			}
			args[i] = ra
		}
		e.Args = args
	}
	out := e
	if e.Func != "" && volatileFuncs[e.Func] && !keepVolatile {
		// nextval() and friends outside a write's per-row path (SELECT
		// nextval('s')): once per statement.
		return s.spliceVolatile(ctx, txn, e, params)
	}
	if e.Sub != nil {
		d, err := s.execScalarSub(ctx, txn, e, params)
		if err != nil {
			return e, err
		}
		out.Sub, out.Lit, out.SubCol, out.SubCols = nil, &d, 0, 0
	}
	if callAs, ok := builtins.TextSearchDefaultArg(e.Func, len(e.Args)); ok {
		// A text-search call that names no configuration gets the
		// session's default as its first argument, once, here: the
		// function itself is then a pure function of its arguments, and
		// the planner sees the same to_tsvector('english', col) whether
		// or not the statement spelled the configuration out.
		d := types.NewString(s.textSearchConfig())
		out.Func = callAs
		out.Args = append([]parser.Expr{{Lit: &d}}, e.Args...)
	}
	switch e.Func {
	case "get_current_ts_config":
		d := types.NewString(s.textSearchConfig())
		out.Func, out.Args, out.Lit = "", nil, &d
	case "now", "current_timestamp", "localtimestamp", "statement_timestamp", "transaction_timestamp":
		d := types.NewTimestamp(roundClock(s.statementTime(), e.Args))
		out.Func, out.Args, out.Lit = "", nil, &d
	case "current_time", "localtime":
		// The time of day of the statement's start, in the session's
		// time zone.
		loc := s.TimeZone()
		if loc == nil {
			loc = time.UTC
		}
		at := time.Unix(0, roundClock(s.statementTime(), e.Args)).In(loc)
		midnight := time.Date(at.Year(), at.Month(), at.Day(), 0, 0, 0, 0, loc)
		d := types.NewTime(int64(at.Sub(midnight)))
		out.Func, out.Args, out.Lit = "", nil, &d
	case "current_date":
		// The statement's date in the session's time zone.
		d := types.NewDate(floorDivInt(types.InZone(s.statementTime(), s.TimeZone()), types.NanosPerDay))
		out.Func, out.Args, out.Lit = "", nil, &d
	case "current_database":
		d := types.NewString(s.database)
		out.Func, out.Lit = "", &d
	case "current_schema":
		// The first schema of the search path that exists; NULL when
		// none does.
		schemas, err := s.pathSchemasAnyTxn(ctx, txn)
		if err != nil {
			return out, err
		}
		d := types.DNull
		if len(schemas) > 0 {
			d = types.NewString(schemas[0].Name)
		}
		out.Func, out.Lit = "", &d
	case "current_user", "current_role", "user":
		d := types.NewString(s.user)
		out.Func, out.Lit = "", &d
	case "current_catalog":
		d := types.NewString(s.database)
		out.Func, out.Lit = "", &d
	case "session_user":
		d := types.NewString(s.sessionUser)
		out.Func, out.Lit = "", &d
	case "version":
		d := types.NewString("PostgreSQL 14.0 datax " + version.Release)
		out.Func, out.Lit = "", &d
	case "pg_backend_pid":
		d := types.NewInt(int64(s.BackendPID))
		out.Func, out.Lit = "", &d
	case "current_setting":
		// current_setting(name [, missing_ok]): the variable's value; an
		// unknown name is 42704 unless missing_ok, then NULL.
		if len(e.Args) < 1 || len(e.Args) > 2 {
			return e, newErrf(CodeSyntaxError, "current_setting() takes a name and an optional missing_ok")
		}
		if exprHasColumn(e.Args[0]) || len(e.Args) == 2 && exprHasColumn(e.Args[1]) {
			// A name read from the row (current_setting(s.name) over
			// pg_settings, psql's \dconfig): a CASE over every setting
			// the session has, custom ones included, matched
			// case-insensitively as SHOW matches. A name that is none
			// of them falls to unrecognizedSettingFunc, which is 42704
			// for that row unless missing_ok, then NULL — what the
			// constant form below does.
			args := make([]parser.Expr, len(e.Args))
			for i, a := range e.Args {
				r, err := s.resolveValueExprOpts(ctx, txn, a, params, keepVolatile)
				if err != nil {
					return e, err
				}
				args[i] = r
			}
			ce := &parser.CaseExpr{Operand: &parser.Expr{Func: "lower", Args: []parser.Expr{args[0]}}}
			for _, kv := range s.settings() {
				n, v := types.NewString(strings.ToLower(kv[0])), types.NewString(kv[1])
				ce.Whens = append(ce.Whens, parser.CaseWhen{Value: &parser.Expr{Lit: &n}, Result: parser.Expr{Lit: &v}})
			}
			ce.Else = &parser.Expr{Func: unrecognizedSettingFunc, Args: args}
			return parser.Expr{Case: ce}, nil
		}
		nameArg, err := evalExpr(e.Args[0], nil, nil, params)
		if err != nil {
			return e, err
		}
		missingOK := false
		if len(e.Args) == 2 {
			m, err := evalExpr(e.Args[1], nil, nil, params)
			if err != nil {
				return e, err
			}
			missingOK = !m.Null && m.B
		}
		_, value, ok := s.setting(nameArg.Text())
		var d types.Datum
		switch {
		case nameArg.Null:
			d = types.DNull // strict, as in PostgreSQL
		case ok:
			d = types.NewString(value)
		case missingOK:
			d = types.DNull
		default:
			return e, newErrf(CodeUndefinedObject, "unrecognized configuration parameter %q", nameArg.Text())
		}
		out.Func, out.Args, out.Lit = "", nil, &d
	case "set_config":
		// set_config(name, value, is_local): SET as a function, so the
		// value can be bound as a parameter — what a connection pool does
		// to name the tenant a request runs for. Returns the value.
		if len(e.Args) != 3 {
			return e, newErrf(CodeSyntaxError, "set_config() takes a name, a value and is_local")
		}
		if exprHasColumn(e.Args[0]) || exprHasColumn(e.Args[1]) || exprHasColumn(e.Args[2]) {
			// Arguments read from the row (pg_dump's set_config(name,
			// ...) FROM pg_settings WHERE name = ...): SET once per
			// row, through a closure evalFunc calls.
			args := make([]parser.Expr, len(e.Args))
			for i, a := range e.Args {
				r, err := s.resolveValueExprOpts(ctx, txn, a, params, keepVolatile)
				if err != nil {
					return e, err
				}
				args[i] = r
			}
			return s.registerRowFunc(func(vals []types.Datum) (types.Datum, error) {
				return s.setConfig(ctx, vals)
			}, args), nil
		}
		var vals [3]types.Datum
		for i := range vals {
			d, err := evalExpr(e.Args[i], nil, nil, params)
			if err != nil {
				return e, err
			}
			vals[i] = d
		}
		d, err := s.setConfig(ctx, vals[:])
		if err != nil {
			return e, err
		}
		out.Func, out.Args, out.Lit = "", nil, &d
	case "pg_sleep":
		// pg_sleep(seconds): waits, honoring cancellation and the
		// statement timeout.
		if len(e.Args) != 1 {
			return e, newErrf(CodeSyntaxError, "pg_sleep() takes one argument")
		}
		arg, err := evalExpr(e.Args[0], nil, nil, params)
		if err != nil {
			return e, err
		}
		secs, err := arg.Coerce(types.Float)
		if err != nil {
			return e, newErrf(CodeInvalidTextRepresentation, "pg_sleep: %v", err)
		}
		select {
		case <-ctx.Done():
			return e, ctx.Err()
		case <-time.After(time.Duration(secs.F * float64(time.Second))):
		}
		d := types.NewString("")
		out.Func, out.Args, out.Lit = "", nil, &d
	case "pg_notify":
		// pg_notify(channel, payload): NOTIFY as a function, so both can
		// be computed or bound. Like every function resolved here it runs
		// once per statement, not once per row, and its arguments cannot
		// name columns.
		if len(e.Args) != 2 {
			return e, newErrf(CodeSyntaxError, "pg_notify() takes a channel and a payload")
		}
		channel, err := evalExpr(e.Args[0], nil, nil, params)
		if err != nil {
			return e, err
		}
		payload, err := evalExpr(e.Args[1], nil, nil, params)
		if err != nil {
			return e, err
		}
		var ch, pl string
		if !channel.Null {
			ch = channel.Text()
		}
		if !payload.Null {
			pl = payload.Text()
		}
		if serr := s.queueNotify(ch, pl); serr != nil {
			return e, serr
		}
		d := types.NewString("")
		out.Func, out.Args, out.Lit = "", nil, &d
	case "pg_cancel_backend", "pg_terminate_backend":
		// Admin-only: cancel (or terminate) another session of this
		// cluster by its wire PID.
		if len(e.Args) != 1 {
			return e, newErrf(CodeSyntaxError, "%s() takes one argument", e.Func)
		}
		admin, err := s.isAdmin(ctx, txn)
		if err != nil {
			return e, err
		}
		if !admin {
			return e, newErrf(CodeInsufficientPriv, "permission denied: %s() needs the admin role", e.Func)
		}
		arg, err := evalExpr(e.Args[0], nil, nil, params)
		if err != nil {
			return e, err
		}
		pid, err := arg.Coerce(types.Int)
		if err != nil {
			return e, newErrf(CodeInvalidTextRepresentation, "%s: %v", e.Func, err)
		}
		ok := false
		if s.BackendControl != nil {
			if ok, err = s.BackendControl(int32(pid.I), e.Func == "pg_terminate_backend"); err != nil {
				return e, newErrf(CodeInternal, "%s: %v", e.Func, err)
			}
		}
		d := types.NewBool(ok)
		out.Func, out.Args, out.Lit = "", nil, &d
	case "pg_get_serial_sequence", "pg_is_in_recovery", "current_schemas", "to_regclass", "to_regproc", "to_regprocedure",
		"to_regnamespace", "to_regrole", "pg_has_role", "has_table_privilege", "has_any_column_privilege", "has_column_privilege",
		"has_sequence_privilege", "has_schema_privilege", "has_database_privilege", "has_function_privilege", "has_type_privilege",
		"has_tablespace_privilege", "has_foreign_data_wrapper_privilege", "has_server_privilege":
		// The system information and privilege functions (sysfuncs.go):
		// a value, or a CASE over the objects a column argument names.
		r, err := s.spliceSysFunc(ctx, txn, e, params, keepVolatile)
		if err != nil {
			return e, err
		}
		out.Func, out.Args, out.Lit, out.Case = "", nil, r.Lit, r.Case
	case "pg_export_snapshot":
		id, err := s.evalExportSnapshot(txn)
		if err != nil {
			return e, err
		}
		d := types.NewString(id)
		out.Func, out.Args, out.Lit = "", nil, &d
	case "acldefault":
		return s.spliceACLDefault(ctx, txn, e, params, keepVolatile)
	case "pg_get_userbyid":
		// The role of that OID (vtable.RoleOID): owners differ now that
		// schemas, tables and the rest change hands (\dn's Owner).
		return s.spliceUserByID(ctx, txn, e)
	case "pg_table_is_visible":
		// Every relation is on the search path but information_schema's
		// views, which PostgreSQL's search path leaves out, and the
		// relations of the schemas the session's path does not reach.
		hidden, err := s.invisibleClassOIDs(ctx, txn)
		if err != nil {
			return out, err
		}
		if len(e.Args) == 1 && e.Args[0].Column != "" && len(e.Args[0].Path) == 0 && e.Args[0].BinOp == "" {
			vals := make([]parser.Expr, len(hidden))
			for i, oid := range hidden {
				d := types.NewInt(oid)
				vals[i] = parser.Expr{Lit: &d}
			}
			out = parser.Expr{Cmp: &parser.Comparison{Column: e.Args[0].Column, Op: "NOT IN", Values: vals}}
			break
		}
		visible := true
		if len(e.Args) == 1 && e.Args[0].Lit != nil && !e.Args[0].Lit.Null {
			if oid, err := s.regclassOID(ctx, txn, e.Args[0].Lit.Text()); err == nil {
				for _, h := range hidden {
					visible = visible && oid != h
				}
			}
		}
		d := types.NewBool(visible)
		out.Func, out.Args, out.Lit = "", nil, &d
	case "pg_type_is_visible":
		// Every type is on the search path but the user-defined types of
		// the schemas the session's path does not reach.
		hidden, err := s.invisibleTypeOIDs(ctx, txn)
		if err != nil {
			return out, err
		}
		if len(e.Args) != 1 {
			break
		}
		if e.Args[0].Lit == nil {
			if len(hidden) == 0 {
				d := types.NewBool(true)
				out.Func, out.Args, out.Lit = "", nil, &d
				break
			}
			vals := make([]parser.Expr, len(hidden))
			for i, oid := range hidden {
				d := types.NewInt(oid)
				vals[i] = parser.Expr{Lit: &d}
			}
			if e.Args[0].Column != "" && len(e.Args[0].Path) == 0 && e.Args[0].BinOp == "" {
				out = parser.Expr{Cmp: &parser.Comparison{Column: e.Args[0].Column, Op: "NOT IN", Values: vals}}
				break
			}
			// Any other argument: a CASE over the hidden OIDs.
			f, t := types.NewBool(false), types.NewBool(true)
			ce := &parser.CaseExpr{Operand: &e.Args[0], Else: &parser.Expr{Lit: &t}}
			for _, v := range vals {
				ce.Whens = append(ce.Whens, parser.CaseWhen{Value: &parser.Expr{Lit: v.Lit}, Result: parser.Expr{Lit: &f}})
			}
			out = parser.Expr{Case: ce}
			break
		}
		visible := true
		if !e.Args[0].Lit.Null {
			oid, perr := strconv.ParseInt(e.Args[0].Lit.Text(), 10, 64)
			if perr != nil && isRegtypeCast(e.Args[0].Cast) {
				// 'app.mood'::regtype: the type it names.
				var td *catalog.TypeDescriptor
				var ok bool
				var err error
				lookup := func(ctx context.Context, txn *kvclient.Txn) error {
					td, ok, err = s.enumTypeByText(ctx, txn, e.Args[0].Lit.Text())
					return nil
				}
				if txn != nil {
					_ = lookup(ctx, txn)
				} else if rerr := s.db.RunTxn(ctx, "type-is-visible", lookup); rerr != nil {
					return out, rerr
				}
				if err != nil {
					return out, err
				}
				if ok {
					oid, perr = catalog.EnumOID(td.ID), nil
				}
			}
			if perr == nil {
				for _, h := range hidden {
					visible = visible && oid != h
				}
			}
		}
		d := types.NewBool(visible)
		out.Func, out.Args, out.Lit = "", nil, &d
	case "pg_partition_ancestors":
		// No partitions: a relation's only ancestor is itself. The
		// argument is an OID, as a number or a regclass literal.
		if len(e.Args) == 1 && e.Args[0].Lit != nil && !e.Args[0].Lit.Null {
			oid, err := s.regclassOID(ctx, txn, e.Args[0].Lit.Text())
			if err != nil {
				return e, err
			}
			d := types.NewInt(oid)
			out.Func, out.Args, out.Lit = "", nil, &d
		}
	case "pg_encoding_to_char":
		d := types.NewString("UTF8")
		out.Func, out.Args, out.Lit = "", nil, &d
	case "shobj_description", "pg_get_statisticsobjdef_columns", "pg_get_triggerdef":
		d := types.DNull
		out.Func, out.Args, out.Lit = "", nil, &d
	case "pg_relation_is_publishable":
		d := types.NewBool(true)
		out.Func, out.Args, out.Lit = "", nil, &d
	case "array":
		// array(SELECT ...): the subquery's single column rendered as a
		// text array literal.
		if len(e.Args) == 1 && e.Args[0].Sub != nil {
			res, err := s.execSubSelect(ctx, txn, e.Args[0].Sub, params)
			if err != nil {
				return e, err
			}
			if len(res.Columns) != 1 {
				return e, newErrf(CodeSyntaxError, "subquery must return only one column")
			}
			elem := res.Columns[0].Type
			if elem == types.Unknown || elem.IsArray() {
				elem = types.String
			}
			elems := make([]types.Datum, len(res.Rows))
			for i, r := range res.Rows {
				elems[i] = r[0]
				if !r[0].Null && r[0].Fam != elem {
					if c, cerr := r[0].Coerce(elem); cerr == nil {
						elems[i] = c
					} else {
						elems[i] = types.NewString(r[0].Text())
					}
				}
			}
			d := types.NewArray(elem, elems)
			out.Func, out.Args, out.Lit = "", nil, &d
		}
	case "format_type", "pg_get_indexdef", "pg_get_constraintdef", "pg_get_expr", "pg_get_viewdef", "obj_description", "col_description":
		// pg_get_viewdef(oid [, pretty]), pg_get_indexdef(oid [, col,
		// pretty]), pg_get_constraintdef(oid [, pretty]),
		// obj_description(oid, class) and col_description(oid, attnum) on
		// a LITERAL OID (psql's \d+, a tool's per-object call) resolve
		// from the descriptor here.
		if literal := e.Func != "format_type" && e.Func != "pg_get_expr"; literal && len(e.Args) > 0 && e.Args[0].Lit != nil && !e.Args[0].Lit.Null {
			oid, err := s.regclassOID(ctx, txn, e.Args[0].Lit.Text())
			if err != nil {
				return e, err
			}
			d := types.DNull
			switch e.Func {
			case "pg_get_viewdef":
				if desc, rerr := catalog.ReadTable(ctx, txn, vtable.TableIDOfOID(oid)); rerr == nil && desc != nil && desc.IsView() && s.tempVisible(desc) {
					d = types.NewString(vtable.ViewDef(desc.ViewQuery))
				}
			case "pg_get_indexdef", "pg_get_constraintdef":
				if text, ok := s.indexOrConstraintDef(ctx, txn, e, oid); ok {
					d = types.NewString(text)
				}
			default:
				if text := s.descriptionOf(ctx, txn, oid, e); text != "" {
					d = types.NewString(text)
				}
			}
			out.Func, out.Args, out.Lit = "", nil, &d
			break
		}
		// Row-dependent catalog renderings: the virtual tables carry them
		// as hidden columns beside the OID the function takes, so the
		// call becomes a column reference on the same row.
		if len(e.Args) > 0 && e.Args[0].Column != "" && (e.Func != "format_type" || len(e.Args) == 2 && e.Args[1].Column != "") {
			prefix := ""
			if i := strings.LastIndexByte(e.Args[0].Column, '.'); i >= 0 {
				prefix = e.Args[0].Column[:i+1]
			}
			hidden := vtable.HiddenColumnFor(e.Func)
			switch _, bare := splitQualified(e.Args[0].Column); {
			case e.Func == "format_type":
				// format_type(t.oid, a.atttypmod) (pg_dump's, over pg_type
				// joined to pg_attribute on the type): the typmod's row
				// renders the type with it.
				if i := strings.LastIndexByte(e.Args[1].Column, '.'); i >= 0 {
					prefix = e.Args[1].Column[:i+1]
				} else {
					prefix = ""
				}
			case e.Func == "pg_get_expr" && (bare == "polqual" || bare == "polwithcheck"):
				// A policy's expressions are kept as their SQL text: the
				// column is its own rendering (two per row, so no one
				// hidden column can stand for both).
				hidden = bare
			}
			out.Func, out.Args, out.Column = "", nil, prefix+hidden
		}
		if out.Func == "format_type" && len(out.Args) > 0 {
			// format_type of an enum's OID (pg_type's oid, a literal):
			// the type's name, quoted as PostgreSQL quotes it ("Role");
			// the builtin types render at evaluation.
			fe, err := s.formatEnumTypes(ctx, txn, out)
			if err != nil {
				return e, err
			}
			out = fe
		}
	}
	if e.Left != nil {
		l, err := s.resolveValueExprOpts(ctx, txn, *e.Left, params, keepVolatile)
		if err != nil {
			return e, err
		}
		out.Left = &l
	}
	if e.Right != nil {
		r, err := s.resolveValueExprOpts(ctx, txn, *e.Right, params, keepVolatile)
		if err != nil {
			return e, err
		}
		out.Right = &r
	}
	if len(out.Args) > 0 {
		args := make([]parser.Expr, len(out.Args))
		for i, a := range out.Args {
			ra, err := s.resolveValueExprOpts(ctx, txn, a, params, keepVolatile)
			if err != nil {
				return e, err
			}
			args[i] = ra
		}
		out.Args = args
	}
	if folded, ok := foldTSQueryCall(out, params); ok {
		out = folded
	}
	if e.Agg != nil {
		// An aggregate call inside an expression: its arguments and
		// FILTER resolve like a select item's.
		agg := *e.Agg
		if agg.AggArg != nil {
			a, err := s.resolveValueExprOpts(ctx, txn, *agg.AggArg, params, keepVolatile)
			if err != nil {
				return e, err
			}
			agg.AggArg = &a
		}
		if len(agg.AggArgs) > 0 {
			args := make([]parser.Expr, len(agg.AggArgs))
			for i, a := range agg.AggArgs {
				ra, err := s.resolveValueExprOpts(ctx, txn, a, params, keepVolatile)
				if err != nil {
					return e, err
				}
				args[i] = ra
			}
			agg.AggArgs = args
		}
		if len(agg.AggFilter) > 0 {
			f, err := s.resolveWhereSubs(ctx, txn, agg.AggFilter, params)
			if err != nil {
				return e, err
			}
			agg.AggFilter = f
		}
		if len(agg.AggOrder) > 0 {
			order := make([]parser.OrderCol, len(agg.AggOrder))
			for i, oc := range agg.AggOrder {
				order[i] = oc
				if oc.Expr != nil {
					ro, err := s.resolveValueExprOpts(ctx, txn, *oc.Expr, params, keepVolatile)
					if err != nil {
						return e, err
					}
					order[i].Expr = &ro
				}
			}
			agg.AggOrder = order
		}
		out.Agg = &agg
	}
	if e.Cmp != nil {
		// A boolean value: its EXISTS / IN / scalar subqueries resolve
		// like a WHERE conjunct's.
		resolved, err := s.resolveWhereSubs(ctx, txn, []parser.Comparison{*e.Cmp}, params)
		if err != nil {
			return e, err
		}
		out.Cmp = &resolved[0]
	}
	if out.Cast != "" && out.Cast != "regclass" && out.Lit != nil && out.Lit.Fam == types.String {
		// 'label'::mood — a cast to an enum type of the database.
		if v, ok, err := s.enumLiteral(ctx, txn, out.Cast, *out.Lit); err != nil {
			return e, err
		} else if ok {
			out.Cast, out.Lit = "", &v
			return out, nil
		}
	}
	if isRegtypeCast(out.Cast) && out.Lit != nil && !out.Lit.Null && out.Lit.Fam == types.String {
		// 'name'::regtype naming an enum: the name as PostgreSQL prints
		// it ('"Role"'::regtype is "Role", 'role'::regtype 42704 when
		// only "Role" exists). A builtin's name passes through.
		if td, ok, err := s.enumTypeByText(ctx, txn, out.Lit.Text()); err != nil {
			return e, err
		} else if ok {
			// Its OID, printing as format_type prints the type
			// (qualified when the search path does not reach it).
			names, err := s.typeDisplayNames(ctx, txn)
			if err != nil {
				return e, err
			}
			d := types.NewInt(catalog.EnumOID(td.ID))
			d.Reg = names[td.ID]
			out.Lit, out.Cast = &d, ""
		}
	}
	if (out.Cast == "oid" || out.Cast == "pg_catalog.oid") && e.Left != nil && isRegtypeCast(e.Left.Cast) && e.Left.Lit != nil && !e.Left.Lit.Null && e.Left.Lit.Fam == types.String && e.Left.BinOp == "" {
		// 'name'::regtype::oid naming an enum: its OID.
		if td, ok, err := s.enumTypeByText(ctx, txn, e.Left.Lit.Text()); err != nil {
			return e, err
		} else if ok {
			d := types.NewInt(catalog.EnumOID(td.ID))
			return parser.Expr{Lit: &d}, nil
		}
	}
	if out.Func == "to_regtype" && len(out.Args) == 1 && out.Args[0].Lit != nil && !out.Args[0].Lit.Null && out.Args[0].Lit.Fam == types.String && out.Args[0].Cast == "" {
		// to_regtype('"Role"'): an enum's OID (NULL, at evaluation, for
		// a name that is no type).
		if td, ok, err := s.enumTypeByText(ctx, txn, out.Args[0].Lit.Text()); err != nil {
			return e, err
		} else if ok {
			d := types.NewInt(catalog.EnumOID(td.ID))
			out.Func, out.Args, out.Lit = "", nil, &d
		}
	}
	if kind := regKind(out.Cast); kind != "" && out.Lit != nil && !out.Lit.Null && (out.Lit.Fam == types.String || out.Lit.Fam == types.Int) && out.BinOp == "" {
		// 'name'::regclass (a table's, an index's or a catalog view's
		// OID, 42P01 when nothing is so named), 'lower'::regproc,
		// 'int4'::regtype, ...: the OID, rendering as the name;
		// '1259'::regclass is the OID itself.
		d, err := s.resolveRegLiteral(ctx, txn, kind, *out.Lit)
		if err != nil {
			return e, err
		}
		out.Lit, out.Cast = &d, ""
	} else if out.Cast == "regclass" && out.Lit == nil {
		// oid::regclass on a column or expression (conrelid::regclass in
		// psql's constraint queries): the table's name per row, as a
		// CASE over every OID the session can see.
		names, err := s.regclassNames(ctx, txn)
		if err != nil {
			return e, err
		}
		inner := out
		inner.Cast = ""
		ce := &parser.CaseExpr{Operand: &inner, Else: &inner}
		for _, on := range names {
			oid, name := types.NewInt(on.oid), types.NewString(on.name)
			ce.Whens = append(ce.Whens, parser.CaseWhen{Value: &parser.Expr{Lit: &oid}, Result: parser.Expr{Lit: &name}})
		}
		out = parser.Expr{Case: ce}
	}
	if e.Case != nil {
		ce := *e.Case
		if ce.Operand != nil {
			op, err := s.resolveValueExprOpts(ctx, txn, *ce.Operand, params, keepVolatile)
			if err != nil {
				return e, err
			}
			ce.Operand = &op
		}
		whens := make([]parser.CaseWhen, len(ce.Whens))
		for i, w := range ce.Whens {
			nw := w
			if w.Value != nil {
				v, err := s.resolveValueExprOpts(ctx, txn, *w.Value, params, keepVolatile)
				if err != nil {
					return e, err
				}
				nw.Value = &v
			}
			if len(w.Cond) > 0 {
				c, err := s.resolveWhereSubs(ctx, txn, w.Cond, params)
				if err != nil {
					return e, err
				}
				nw.Cond = c
			}
			r, err := s.resolveValueExprOpts(ctx, txn, w.Result, params, keepVolatile)
			if err != nil {
				return e, err
			}
			nw.Result = r
			whens[i] = nw
		}
		ce.Whens = whens
		if ce.Else != nil {
			el, err := s.resolveValueExprOpts(ctx, txn, *ce.Else, params, keepVolatile)
			if err != nil {
				return e, err
			}
			ce.Else = &el
		}
		out.Case = &ce
	}
	return out, nil
}

// roundClock rounds a clock reading (nanoseconds) to the fractional-
// seconds precision of current_timestamp(p) and its kin, when args
// carries one (the parser admits only a non-negative integer literal;
// above 6, microseconds, it changes nothing).
func roundClock(nanos int64, args []parser.Expr) int64 {
	if len(args) != 1 || args[0].Lit == nil || args[0].Lit.I >= 6 {
		// PostgreSQL's clock has microsecond precision.
		return types.TruncMicros(nanos)
	}
	unit := int64(1000)
	for p := args[0].Lit.I; p < 6; p++ {
		unit *= 10
	}
	return (nanos + unit/2) / unit * unit
}

// exprHasSub reports whether an expression needs the pre-execution
// resolve pass: a scalar subquery or a now() call anywhere inside it.
func exprHasSub(e parser.Expr) bool {
	return exprHas(e, func(x parser.Expr) bool {
		return x.Sub != nil || x.Cast != "" || (x.Func != "" && splicedFuncs[x.Func])
	})
}

// needsResolve reports whether an expression holds anything the
// resolve pass must handle: a subquery or spliced call (exprHasSub),
// or — under a TimeZone that is not UTC — what reads or renders in the
// zone (zoneSensitive), which the pass marks with it.
func (s *Session) needsResolve(e parser.Expr) bool {
	return exprHasSub(e) || s.zone() != nil && exprHas(e, zoneMarked)
}

// zoneMarked reports a node the session marks with its TimeZone: a
// zone-sensitive one (zoneSensitive), or a condition (a comparison, a
// searched CASE) whose comparisons take the zone (Comparison.Zone).
func zoneMarked(x parser.Expr) bool {
	return zoneSensitive(x) || x.Cmp != nil || x.Case != nil
}

// zone is the session's TimeZone when it is not UTC (nil when it is).
func (s *Session) zone() *time.Location {
	if loc := s.vars.timeZone; loc != nil && loc != time.UTC {
		return loc
	}
	return nil
}

// zoneSensitive reports whether a node reads or renders a timestamp
// with time zone in the session's zone: a text literal (which may be
// read as one), a cast, an operator over one, a zone-aware builtin.
func zoneSensitive(x parser.Expr) bool {
	if x.Lit != nil && !x.Lit.Null && x.Lit.Fam == types.String || x.Cast != "" {
		return true
	}
	switch x.BinOp {
	case "+", "-", "||":
		return true
	}
	b, ok := builtins.Lookup(x.Func)
	return ok && b.ZoneSensitive()
}

// zoneNode marks a resolved node with the session's TimeZone when it is
// not UTC: a text literal reads in it as a timestamp with time zone,
// and a cast, operator or zone-aware call evaluates in it.
func (s *Session) zoneNode(out parser.Expr) parser.Expr {
	loc := s.zone()
	if loc == nil || !zoneSensitive(out) {
		return out
	}
	if out.Lit != nil && !out.Lit.Null && out.Lit.Fam == types.String && out.Lit.Loc == nil {
		d := *out.Lit
		d.Loc = loc
		out.Lit = &d
	}
	out.Zone = loc
	return out
}

// zoneParams marks a statement's text parameters with the session's
// TimeZone, in which they read as timestamps with time zone.
func (s *Session) zoneParams(params []types.Datum) []types.Datum {
	loc := s.zone()
	if loc == nil {
		return params
	}
	var out []types.Datum
	for i, p := range params {
		if p.Null || p.Fam != types.String || p.Loc != nil {
			continue
		}
		if out == nil {
			out = append([]types.Datum(nil), params...)
		}
		out[i].Loc = loc
	}
	if out == nil {
		return params
	}
	return out
}

// exprHasSubquery reports whether an actual (SELECT ...) appears
// anywhere inside the expression (spliced functions do not count).
func exprHasSubquery(e parser.Expr) bool {
	return exprHas(e, func(x parser.Expr) bool { return x.Sub != nil })
}

// condHas is exprHas over a conjunct: its subquery, its expressions,
// and (recursively) an OR group's disjuncts.
func condHas(c parser.Comparison, pred func(parser.Expr) bool) bool {
	if c.Sub != nil && pred(parser.Expr{Sub: c.Sub}) || exprHas(c.Value, pred) || c.Expr != nil && exprHas(*c.Expr, pred) {
		return true
	}
	for _, v := range c.Values {
		if exprHas(v, pred) {
			return true
		}
	}
	for _, d := range c.Or {
		for _, inner := range d {
			if condHas(inner, pred) {
				return true
			}
		}
	}
	return false
}

func exprHas(e parser.Expr, pred func(parser.Expr) bool) bool {
	if pred(e) {
		return true
	}
	if e.Cmp != nil && condHas(*e.Cmp, pred) {
		return true
	}
	if e.Case != nil {
		if e.Case.Operand != nil && exprHas(*e.Case.Operand, pred) {
			return true
		}
		for _, w := range e.Case.Whens {
			if w.Value != nil && exprHas(*w.Value, pred) || exprHas(w.Result, pred) {
				return true
			}
			for _, c := range w.Cond {
				if condHas(c, pred) {
					return true
				}
			}
		}
		if e.Case.Else != nil && exprHas(*e.Case.Else, pred) {
			return true
		}
	}
	if e.Left != nil && exprHas(*e.Left, pred) {
		return true
	}
	if e.Right != nil && exprHas(*e.Right, pred) {
		return true
	}
	for _, a := range e.Args {
		if exprHas(a, pred) {
			return true
		}
	}
	if e.Agg != nil {
		// An aggregate call's arguments and FILTER (inside an expression).
		if e.Agg.AggArg != nil && exprHas(*e.Agg.AggArg, pred) {
			return true
		}
		for _, a := range e.Agg.AggArgs {
			if exprHas(a, pred) {
				return true
			}
		}
		for _, c := range e.Agg.AggFilter {
			if condHas(c, pred) {
				return true
			}
		}
		for _, oc := range e.Agg.AggOrder {
			if oc.Expr != nil && exprHas(*oc.Expr, pred) {
				return true
			}
		}
	}
	return false
}

// cmpNeedsResolve reports whether a conjunct (or, recursively, an OR
// group) contains anything the pre-execution resolve pass must handle.
func (s *Session) cmpNeedsResolve(cmp parser.Comparison) bool {
	if cmp.Sub != nil || s.needsResolve(cmp.Value) {
		return true
	}
	if loc := s.zone(); loc != nil && cmp.Zone != loc && cmp.Op != "TRUE" && cmp.Op != "FALSE" {
		return true // to be marked with the zone
	}
	if cmp.Expr != nil && s.needsResolve(*cmp.Expr) {
		return true
	}
	for _, ve := range cmp.Values {
		if s.needsResolve(ve) {
			return true
		}
	}
	for _, d := range cmp.Or {
		for _, inner := range d {
			if s.cmpNeedsResolve(inner) {
				return true
			}
		}
	}
	return false
}

// resolveWhereSubs evaluates every subquery in a WHERE conjunction,
// returning a spliced copy (or the original slice when nothing changed).
func (s *Session) resolveWhereSubs(ctx context.Context, txn *kvclient.Txn, where []parser.Comparison, params []types.Datum) ([]parser.Comparison, error) {
	changed := false
	for _, cmp := range where {
		if s.cmpNeedsResolve(cmp) {
			changed = true
			break
		}
	}
	if !changed {
		return where, nil
	}
	out := make([]parser.Comparison, len(where))
	copy(out, where)
	loc := s.zone()
	for i := range out {
		cmp := &out[i]
		if loc != nil {
			// A date compares with a timestamp with time zone at its
			// midnight in the session's zone.
			cmp.Zone = loc
		}
		if len(cmp.Or) > 0 {
			// OR groups may hold scalar subqueries and now() calls:
			// resolve each disjunct recursively.
			or := make([][]parser.Comparison, len(cmp.Or))
			for j, d := range cmp.Or {
				rd, err := s.resolveWhereSubs(ctx, txn, d, params)
				if err != nil {
					return nil, err
				}
				or[j] = rd
			}
			cmp.Or = or
			continue
		}
		if s.needsResolve(cmp.Value) {
			v, err := s.resolveValueExpr(ctx, txn, cmp.Value, params)
			if err != nil {
				return nil, err
			}
			cmp.Value = v
		}
		if cmp.Expr != nil && s.needsResolve(*cmp.Expr) {
			v, err := s.resolveValueExpr(ctx, txn, *cmp.Expr, params)
			if err != nil {
				return nil, err
			}
			cmp.Expr = &v
		}
		switch cmp.Op {
		case "IN", "NOT IN":
			for j, ve := range cmp.Values {
				if !s.needsResolve(ve) {
					continue
				}
				if &cmp.Values[0] == &where[i].Values[0] {
					cmp.Values = append([]parser.Expr(nil), where[i].Values...)
				}
				v, err := s.resolveValueExpr(ctx, txn, ve, params)
				if err != nil {
					return nil, err
				}
				cmp.Values[j] = v
			}
			if cmp.Sub != nil {
				res, err := s.execSubSelect(ctx, txn, cmp.Sub, params)
				if err != nil {
					return nil, err
				}
				if len(res.Columns) != 1 {
					return nil, newErrf(CodeSyntaxError, "subquery must return only one column")
				}
				vals := make([]parser.Expr, len(res.Rows))
				for j, r := range res.Rows {
					d := r[0]
					vals[j] = parser.Expr{Lit: &d}
				}
				cmp.Values, cmp.Sub = vals, nil
			}
		case "EXISTS", "NOT EXISTS":
			probe := *cmp.Sub
			probe.Limit = 1
			res, err := s.execSubSelect(ctx, txn, &probe, params)
			if err != nil {
				return nil, err
			}
			exists := len(res.Rows) > 0
			if cmp.Op == "NOT EXISTS" {
				exists = !exists
			}
			op := "FALSE"
			if exists {
				op = "TRUE"
			}
			*cmp = parser.Comparison{Op: op}
		}
	}
	return out, nil
}

// resolveSelectSubs evaluates the uncorrelated subqueries of a SELECT
// (select list, WHERE, HAVING values) into a spliced copy. The derived
// table (FROM subquery) is left alone — execDerivedSelect materializes it.
func (s *Session) resolveSelectSubs(ctx context.Context, txn *kvclient.Txn, t *parser.Select, params []types.Datum) (*parser.Select, error) {
	out := t
	cloned := func() *parser.Select {
		if out == t {
			c := *t
			out = &c
		}
		return out
	}

	where, err := s.resolveWhereSubs(ctx, txn, t.Where, params)
	if err != nil {
		return nil, err
	}
	if len(where) > 0 && &where[0] != &t.Where[0] {
		cloned().Where = where
	}
	// A join's ON conjuncts beyond its key equalities resolve as WHERE's
	// do (pg_dump's d.classid = 'pg_class'::regclass in a LEFT JOIN),
	// OR groups included (so a literal in one reads in the session's
	// TimeZone), and so do their uncorrelated subqueries: evaluated once,
	// IN becomes a value list, EXISTS a constant, a scalar subquery a
	// literal. The correlated ones were split off before
	// (splitCorrelatedOn) for the join to evaluate per candidate row.
	for i, j := range t.Joins {
		var f []parser.Comparison
		for k, cmp := range j.Filter {
			if !s.cmpNeedsResolve(cmp) {
				continue
			}
			r, err := s.resolveWhereSubs(ctx, txn, []parser.Comparison{cmp}, params)
			if err != nil {
				return nil, err
			}
			if f == nil {
				f = append([]parser.Comparison(nil), j.Filter...)
			}
			f[k] = r[0]
		}
		if f == nil {
			continue
		}
		c := cloned()
		if len(c.Joins) > 0 && &c.Joins[0] == &t.Joins[0] {
			c.Joins = append([]parser.JoinClause(nil), t.Joins...)
		}
		c.Joins[i].Filter = f
	}

	for i, se := range t.Exprs {
		if se.Agg != "" && !se.Star && se.Window == nil {
			// An aggregate select item: a subquery among its arguments,
			// FILTER or ORDER BY (array_agg(x ORDER BY (SELECT ...))).
			call := se
			wrapped := parser.Expr{Agg: &call}
			if !exprHasSubquery(wrapped) {
				continue
			}
			v, err := s.resolveValueExpr(ctx, txn, wrapped, params)
			if err != nil {
				return nil, err
			}
			c := cloned()
			if &c.Exprs[0] == &t.Exprs[0] {
				c.Exprs = append([]parser.SelectExpr(nil), t.Exprs...)
			}
			c.Exprs[i] = *v.Agg
			continue
		}
		if !s.needsResolve(se.Expr) {
			continue
		}
		v, err := s.resolveValueExpr(ctx, txn, se.Expr, params)
		if err != nil {
			return nil, err
		}
		c := cloned()
		if &c.Exprs[0] == &t.Exprs[0] {
			c.Exprs = append([]parser.SelectExpr(nil), t.Exprs...)
		}
		c.Exprs[i].Expr = v
		if se.Alias == "" && catalogRenderingFuncs[se.Expr.Func] && v.Func == "" {
			// A catalog function that became its row's hidden rendering
			// or a constant keeps its name as the output column's, as
			// PostgreSQL names a call (pg_get_indexdef, not __indexdef).
			c.Exprs[i].Alias = se.Expr.Func
		}
	}

	having, err := s.resolveWhereSubs(ctx, txn, t.Having, params)
	if err != nil {
		return nil, err
	}
	if len(having) > 0 && &having[0] != &t.Having[0] {
		cloned().Having = having
	}
	for i, oc := range t.OrderBy {
		if oc.Expr == nil || !s.needsResolve(*oc.Expr) {
			continue
		}
		v, err := s.resolveValueExpr(ctx, txn, *oc.Expr, params)
		if err != nil {
			return nil, err
		}
		c := cloned()
		if &c.OrderBy[0] == &t.OrderBy[0] {
			c.OrderBy = append([]parser.OrderCol(nil), t.OrderBy...)
		}
		c.OrderBy[i].Expr = &v
	}
	// GROUP BY and DISTINCT ON expressions are resolved like the select
	// list's, so a grouped expression (date_trunc over a timestamp under
	// a session TimeZone) evaluates, and matches its select item, the same.
	for i, g := range t.GroupBy {
		if !s.needsResolve(g) {
			continue
		}
		v, err := s.resolveValueExpr(ctx, txn, g, params)
		if err != nil {
			return nil, err
		}
		c := cloned()
		if &c.GroupBy[0] == &t.GroupBy[0] {
			c.GroupBy = append([]parser.Expr(nil), t.GroupBy...)
		}
		c.GroupBy[i] = v
	}
	for i, d := range t.DistinctOn {
		if !s.needsResolve(d) {
			continue
		}
		v, err := s.resolveValueExpr(ctx, txn, d, params)
		if err != nil {
			return nil, err
		}
		c := cloned()
		if &c.DistinctOn[0] == &t.DistinctOn[0] {
			c.DistinctOn = append([]parser.Expr(nil), t.DistinctOn...)
		}
		c.DistinctOn[i] = v
	}
	return out, nil
}

// resolveUpdateSubs evaluates subqueries in an UPDATE's SET values and
// WHERE clause into a spliced copy.
func (s *Session) resolveUpdateSubs(ctx context.Context, txn *kvclient.Txn, t *parser.Update, params []types.Datum) (*parser.Update, error) {
	out := t
	where, err := s.resolveWhereSubs(ctx, txn, t.Where, params)
	if err != nil {
		return nil, err
	}
	if len(where) > 0 && &where[0] != &t.Where[0] {
		c := *t
		c.Where = where
		out = &c
	}
	for i, set := range t.Set {
		if !s.needsResolve(set.Value) {
			continue
		}
		v, err := s.resolveValueExpr(ctx, txn, set.Value, params)
		if err != nil {
			return nil, err
		}
		if out == t {
			c := *t
			out = &c
		}
		if &out.Set[0] == &t.Set[0] {
			out.Set = append([]parser.SetClause(nil), t.Set...)
		}
		out.Set[i].Value = v
	}
	ret, err := s.resolveReturning(ctx, txn, t.Returning, params)
	if err != nil {
		return nil, err
	}
	if ret != nil {
		if out == t {
			c := *t
			out = &c
		}
		out.Returning = ret
	}
	return out, nil
}

// resolveReturning splices the once-per-statement calls of a RETURNING
// list (now(), current_timestamp, current_user, ...) as the SET and
// VALUES positions have them spliced, returning nil when nothing
// changed. An item holding a subquery is left for returningProjection
// to refuse; per-row volatile calls are left in place.
func (s *Session) resolveReturning(ctx context.Context, txn *kvclient.Txn, exprs []parser.SelectExpr, params []types.Datum) ([]parser.SelectExpr, error) {
	var out []parser.SelectExpr
	for i, se := range exprs {
		if se.Star || !s.needsResolve(se.Expr) || exprHasSubquery(se.Expr) {
			continue
		}
		v, err := s.resolveValueExprOpts(ctx, txn, se.Expr, params, true)
		if err != nil {
			return nil, err
		}
		if out == nil {
			out = append([]parser.SelectExpr(nil), exprs...)
		}
		out[i].Expr = v
	}
	return out, nil
}

// resolveDeleteSubs evaluates subqueries in a DELETE's WHERE clause.
func (s *Session) resolveDeleteSubs(ctx context.Context, txn *kvclient.Txn, t *parser.Delete, params []types.Datum) (*parser.Delete, error) {
	out := t
	where, err := s.resolveWhereSubs(ctx, txn, t.Where, params)
	if err != nil {
		return nil, err
	}
	if len(where) > 0 && &where[0] != &t.Where[0] {
		c := *t
		c.Where = where
		out = &c
	}
	ret, err := s.resolveReturning(ctx, txn, t.Returning, params)
	if err != nil {
		return nil, err
	}
	if ret != nil {
		if out == t {
			c := *t
			out = &c
		}
		out.Returning = ret
	}
	return out, nil
}

// resolveInsertSubs evaluates scalar subqueries in INSERT values.
func (s *Session) resolveInsertSubs(ctx context.Context, txn *kvclient.Txn, t *parser.Insert, params []types.Datum) (*parser.Insert, error) {
	out := t
	for ri, row := range t.Rows {
		for ci, e := range row {
			if !s.needsResolve(e) {
				continue
			}
			v, err := s.resolveValueExpr(ctx, txn, e, params)
			if err != nil {
				return nil, err
			}
			if out == t {
				c := *t
				c.Rows = make([][]parser.Expr, len(t.Rows))
				copy(c.Rows, t.Rows)
				out = &c
			}
			if &out.Rows[ri][0] == &t.Rows[ri][0] {
				out.Rows[ri] = append([]parser.Expr(nil), t.Rows[ri]...)
			}
			out.Rows[ri][ci] = v
		}
	}
	clone := func() *parser.Insert {
		if out == t {
			c := *t
			out = &c
		}
		return out
	}
	if oc := t.OnConflict; oc != nil {
		// ON CONFLICT ... DO UPDATE SET v = now() WHERE t.at < now():
		// spliced as an UPDATE's SET and WHERE are.
		var noc *parser.OnConflict
		for i, set := range oc.Set {
			if !s.needsResolve(set.Value) {
				continue
			}
			v, err := s.resolveValueExpr(ctx, txn, set.Value, params)
			if err != nil {
				return nil, err
			}
			if noc == nil {
				c := *oc
				c.Set = append([]parser.SetClause(nil), oc.Set...)
				noc = &c
			}
			noc.Set[i].Value = v
		}
		where, err := s.resolveWhereSubs(ctx, txn, oc.Where, params)
		if err != nil {
			return nil, err
		}
		if len(where) > 0 && &where[0] != &oc.Where[0] {
			if noc == nil {
				c := *oc
				noc = &c
			}
			noc.Where = where
		}
		if noc != nil {
			clone().OnConflict = noc
		}
	}
	ret, err := s.resolveReturning(ctx, txn, t.Returning, params)
	if err != nil {
		return nil, err
	}
	if ret != nil {
		clone().Returning = ret
	}
	return out, nil
}

// ---------------------------------------------------------------------------
// Derived tables: FROM (SELECT ...) AS alias.

// stripAliasQualifier rewrites "alias.c" to "c" for the derived table's
// alias in a possibly-qualified column name.
func stripAliasQualifier(name, alias string) string {
	if q, col := splitQualified(name); q == alias {
		return col
	}
	return name
}

// derivedSelect returns a copy of the outer select with the derived
// table's alias stripped from every column reference, so the synthetic
// descriptor's bare column names resolve.
func derivedSelect(t *parser.Select) *parser.Select {
	strip := func(n string) string { return stripAliasQualifier(n, t.Alias) }
	c := *t
	c.Exprs = append([]parser.SelectExpr(nil), t.Exprs...)
	for i := range c.Exprs {
		if !c.Exprs[i].Star {
			c.Exprs[i] = renameSelectExpr(c.Exprs[i], strip)
		}
	}
	c.Where = renameCondColumns(t.Where, strip)
	c.GroupBy = make([]parser.Expr, len(t.GroupBy))
	for i, g := range t.GroupBy {
		c.GroupBy[i] = renameExprColumns(g, strip)
	}
	c.Having = renameCondColumns(t.Having, strip)
	if len(t.DistinctOn) > 0 {
		c.DistinctOn = make([]parser.Expr, len(t.DistinctOn))
		for i, e := range t.DistinctOn {
			c.DistinctOn[i] = renameExprColumns(e, strip)
		}
	}
	c.OrderBy = append([]parser.OrderCol(nil), t.OrderBy...)
	for i := range c.OrderBy {
		oc := &c.OrderBy[i]
		oc.Column = strip(oc.Column)
		if oc.Expr != nil {
			e := renameExprColumns(*oc.Expr, strip)
			oc.Expr = &e
		}
		if oc.Agg != nil {
			a := renameSelectExpr(*oc.Agg, strip)
			oc.Agg = &a
		}
	}
	return &c
}

// derivedDesc builds the synthetic single-table descriptor for a
// materialized subquery result.
func derivedDesc(alias string, cols []ResultColumn) *catalog.TableDescriptor {
	desc := &catalog.TableDescriptor{Name: alias}
	for i, rc := range cols {
		desc.Columns = append(desc.Columns, catalog.Column{
			ID: catalog.ColumnID(i + 1), Name: strings.ToLower(rc.Name), Type: rc.Type,
			// An integer column keeps its width (int2 / int4 / int8),
			// as PostgreSQL's subquery output columns do.
			Width: rc.Width,
		})
	}
	return desc
}

// execDerivedSelect materializes the FROM subquery and runs the outer
// select's pipeline (WHERE filter, grouping or projection, ORDER BY,
// DISTINCT, LIMIT) over the in-memory rows.
func (s *Session) execDerivedSelect(ctx context.Context, txn *kvclient.Txn, t *parser.Select, params []types.Datum) (*Result, error) {
	if t.ForUpdate {
		return nil, newErrf(CodeFeatureNotSupported, "FOR UPDATE is not allowed on a subquery")
	}
	inner, err := s.execSubSelect(ctx, txn, t.Derived, params)
	if err != nil {
		return nil, err
	}
	return s.execMaterialized(ctx, txn, derivedDesc(t.Alias, inner.Columns), inner.Rows, t, params)
}

// tableFuncColumns are the columns of a FROM-clause table function
// call named alias, the leading ones renamed by rename. A function
// returning a single scalar column (unnest) names it by the alias, as
// in PostgreSQL.
func tableFuncColumns(e parser.Expr, alias string, rename []string) ([]ResultColumn, error) {
	var cols []ResultColumn
	switch e.Func {
	case "unnest":
		cols = []ResultColumn{{Name: alias, Type: funcTableElem(e)}}
	case "pg_partition_ancestors":
		cols = []ResultColumn{{Name: alias, Type: types.Int}}
	case "pg_available_extensions":
		cols = []ResultColumn{{Name: "name", Type: types.String}, {Name: "default_version", Type: types.String}, {Name: "comment", Type: types.String}}
	default:
		b, ok := builtins.Lookup(e.Func)
		if !ok || !b.IsSetReturning() {
			return nil, newErrf(CodeFeatureNotSupported, "table function %s is not supported", e.Func)
		}
		cols = srfColumns(b, e, alias)
	}
	if len(rename) > len(cols) {
		return nil, newErrf(CodeInvalidColumnReference, "table \"%s\" has %d columns available but %d columns specified", alias, len(cols), len(rename))
	}
	for i, n := range rename {
		cols[i].Name = n
	}
	for i := range cols {
		cols[i].Name = strings.ToLower(cols[i].Name)
	}
	return cols, nil
}

// funcTableDesc is the descriptor of a select's FROM table function.
func funcTableDesc(t *parser.Select) (*catalog.TableDescriptor, error) {
	cols, err := tableFuncColumns(*t.FuncTable, t.Alias, t.FuncCols)
	if err != nil {
		return nil, err
	}
	return derivedDesc(t.Alias, cols), nil
}

// isLateralFunc reports whether a table function call's arguments read
// a column, which can only be an earlier FROM member's (implicitly
// LATERAL).
func isLateralFunc(e parser.Expr) bool {
	lateral := false
	exprColumns(e, func(string) { lateral = true })
	return lateral
}

// execTableFunc runs a FROM-clause table function call: its columns
// (named as tableFuncColumns says) and rows.
func (s *Session) execTableFunc(ctx context.Context, txn *kvclient.Txn, fe parser.Expr, alias string, rename []string, params []types.Datum) ([]ResultColumn, [][]types.Datum, error) {
	cols, err := tableFuncColumns(fe, alias, rename)
	if err != nil {
		return nil, nil, err
	}
	if isLateralFunc(fe) {
		// A lateral call joined to the members to its left runs per row
		// with their values substituted (pkg/sql/lateral.go); one still
		// reading a column has nothing to its left that has it.
		return nil, nil, fromRefErr(fe)
	}
	var rows [][]types.Datum
	switch fe.Func {
	case "unnest":
		e, err := s.resolveValueExpr(ctx, txn, fe, params)
		if err != nil {
			return nil, nil, err
		}
		if len(e.Args) != 1 {
			return nil, nil, newErrf(CodeUndefinedFunction, "unnest takes one array argument")
		}
		arg, err := evalExpr(e.Args[0], nil, nil, params)
		if err != nil {
			return nil, nil, err
		}
		switch {
		case arg.Null:
		case arg.Fam.IsArray():
			for _, d := range arg.A {
				rows = append(rows, []types.Datum{d})
			}
		default:
			for _, el := range arrayElems(arg.Text()) {
				d := types.NewString(el)
				if el == "NULL" {
					d = types.DNull
				}
				rows = append(rows, []types.Datum{d})
			}
		}
	case "pg_partition_ancestors":
		// No partitions: a relation's only ancestor is itself (the
		// scalar rewrite resolves it to the relation's OID).
		e, err := s.resolveValueExpr(ctx, txn, fe, params)
		if err != nil {
			return nil, nil, err
		}
		if e.Lit == nil {
			return nil, nil, newErrf(CodeFeatureNotSupported, "pg_partition_ancestors takes a relation OID or name")
		}
		rows = append(rows, []types.Datum{*e.Lit})
	case "pg_available_extensions":
		// The extensions datax has built in (vtable.Extensions), as the
		// pg_available_extensions view and pg_extension list them.
		if len(fe.Args) != 0 {
			return nil, nil, newErrf(CodeUndefinedFunction, "pg_available_extensions takes no arguments")
		}
		for _, e := range vtable.Extensions {
			rows = append(rows, []types.Datum{types.NewString(e.Name), types.NewString(e.Version), types.NewString(e.Comment)})
		}
	default:
		// A set-returning builtin (generate_series, jsonb_each, ...)
		// over constant arguments.
		e, err := s.resolveValueExpr(ctx, txn, fe, params)
		if err != nil {
			return nil, nil, err
		}
		if rows, err = s.srfRows(e, params, cols); err != nil {
			return nil, nil, err
		}
	}
	return cols, rows, nil
}

// srfColumns are the columns of a set-returning builtin's call: its
// declared names (the alias for an unnamed one, as for a function
// returning a scalar) typed from the arguments.
func srfColumns(b *builtins.Builtin, e parser.Expr, alias string) []ResultColumn {
	argFams := make([]types.Family, len(e.Args))
	for i, a := range e.Args {
		argFams[i] = exprFamily(a, nil)
	}
	fams := b.ColumnFamilies(argFams)
	cols := make([]ResultColumn, len(b.Columns))
	for i, n := range b.Columns {
		if n == "" {
			n = alias
		}
		f := fams[i]
		if f == types.Unknown {
			f = types.String
		}
		cols[i] = ResultColumn{Name: n, Type: f}
	}
	// generate_series over timestamps without time zone yields them.
	cols[0].NoTZ = cols[0].Type == types.Timestamp && len(e.Args) > 0 && exprNoTZ(e.Args[0], nil)
	return cols
}

// srfRows evaluates a set-returning builtin call's arguments (constants
// and parameters; the session's own functions already spliced) and
// collects its rows, each accounted against the statement's memory
// limit as it is made and conformed to the described column types.
func (s *Session) srfRows(e parser.Expr, params []types.Datum, cols []ResultColumn) ([][]types.Datum, error) {
	b, ok := builtins.Lookup(e.Func)
	if !ok || !b.IsSetReturning() {
		return nil, newErrf(CodeUndefinedFunction, "function %s does not return a set", e.Func)
	}
	args := make([]types.Datum, len(e.Args))
	for i, a := range e.Args {
		d, err := evalExpr(a, nil, nil, params)
		if err != nil {
			return nil, err
		}
		args[i] = d
	}
	next, err := b.CallRowsIn(args, e.Zone)
	if err != nil {
		return nil, builtinErr(err)
	}
	var rows [][]types.Datum
	for {
		row, ok, err := next()
		if err != nil {
			return nil, builtinErr(err)
		}
		if !ok {
			return rows, nil
		}
		if err := s.chargeDatums(row); err != nil {
			return nil, err
		}
		for i := range row {
			if i < len(cols) {
				row[i] = conformTo(row[i], cols[i].Type)
			}
		}
		rows = append(rows, row)
	}
}

// funcTableElem is the element family FROM unnest(array) produces, from
// the argument's inferred type (text when it cannot be told).
func funcTableElem(e parser.Expr) types.Family {
	if e.Func == "unnest" && len(e.Args) == 1 {
		if f := exprFamily(e.Args[0], nil); f.IsArray() {
			return f.Elem()
		}
	}
	return types.String
}

// execFuncTableSelect materializes FROM f(...) — unnest(array): one row
// per array element (none when the array is NULL) — then runs the
// select's pipeline over the rows.
func (s *Session) execFuncTableSelect(ctx context.Context, txn *kvclient.Txn, t *parser.Select, params []types.Datum) (*Result, error) {
	cols, rows, err := s.execTableFunc(ctx, txn, *t.FuncTable, t.Alias, t.FuncCols, params)
	if err != nil {
		return nil, err
	}
	return s.execMaterialized(ctx, txn, derivedDesc(t.Alias, cols), rows, t, params)
}

// execMaterialized runs a select's pipeline (WHERE filter, grouping or
// projection, ORDER BY, DISTINCT, LIMIT) over in-memory rows shaped by
// desc, for derived tables and table functions.
func (s *Session) execMaterialized(ctx context.Context, txn *kvclient.Txn, desc *catalog.TableDescriptor, inRows [][]types.Datum, t *parser.Select, params []types.Datum) (*Result, error) {
	t = derivedSelect(t)
	rows := make([]fetchedRow, 0, len(inRows))
	for _, r := range inRows {
		row := make(map[catalog.ColumnID]types.Datum, len(r))
		for i, d := range r {
			row[catalog.ColumnID(i+1)] = d
		}
		match, err := matchesWhere(t.Where, desc, row, params)
		if err != nil {
			return nil, err
		}
		if match {
			if err := s.chargeRow(row); err != nil {
				return nil, err
			}
			rows = append(rows, fetchedRow{row: row})
		}
	}

	if hasAggregates(t.Exprs) || len(t.GroupBy) > 0 {
		return s.execGroupedOver(desc, rows, t, params, nil)
	}
	if len(t.Having) > 0 {
		return nil, newErrf(CodeGrouping, "HAVING requires GROUP BY or aggregate functions")
	}

	if len(t.OrderBy) > 0 {
		if err := sortRows(desc, rows, t.OrderBy, params); err != nil {
			return nil, err
		}
	}
	if len(t.DistinctOn) > 0 {
		var derr error
		if rows, derr = keepFirstDistinct(rows, func(fr fetchedRow) ([]types.Datum, error) {
			return evalExprList(t.DistinctOn, tableEnv{desc: desc, row: fr.row}, params)
		}); derr != nil {
			return nil, derr
		}
	}
	proj, perr := resolveProjection(desc, t.Exprs)
	if perr != nil {
		return nil, perr
	}
	res := &Result{}
	for _, p := range proj {
		res.Columns = append(res.Columns, colResult(p.name, p.col))
	}
	for _, fr := range rows {
		out := make([]types.Datum, len(proj))
		for i, p := range proj {
			if p.expr != nil {
				d, err := evalExpr(*p.expr, desc, fr.row, params)
				if err != nil {
					return nil, err
				}
				out[i] = d
				continue
			}
			d, ok := fr.row[p.col.ID]
			if !ok {
				d = types.DNull
			}
			out[i] = d
		}
		if err := s.chargeDatums(out); err != nil {
			return nil, err
		}
		res.Rows = append(res.Rows, out)
	}
	if t.Distinct && len(t.DistinctOn) == 0 {
		res.Rows = dedupeRows(res.Rows)
	}
	res.Rows = trimRows(res.Rows, t)
	res.Tag = fmt.Sprintf("SELECT %d", len(res.Rows))
	return res, nil
}

// splicedFuncs are resolved by the session before execution: they
// depend on the session, the clock, or the catalog rather than the row.
var splicedFuncs = map[string]bool{
	"now": true, "current_timestamp": true, "localtimestamp": true, "statement_timestamp": true, "transaction_timestamp": true, "current_date": true,
	"current_time": true, "localtime": true,
	"current_database": true, "current_schema": true, "current_user": true, "session_user": true,
	"current_role": true, "user": true, "current_catalog": true,
	"version": true, "pg_backend_pid": true, "pg_sleep": true, "pg_notify": true, "pg_cancel_backend": true, "pg_terminate_backend": true, "pg_get_userbyid": true, "pg_table_is_visible": true, "pg_type_is_visible": true, "pg_partition_ancestors": true,
	"pg_encoding_to_char": true, "obj_description": true, "col_description": true, "shobj_description": true,
	"array_to_string": true, "pg_get_viewdef": true, "current_schemas": true, "current_setting": true, "set_config": true, "pg_get_triggerdef": true,
	"to_regtype": true, "format_type": true, "pg_get_indexdef": true, "pg_get_constraintdef": true, "pg_get_expr": true,
	"pg_get_statisticsobjdef_columns": true, "pg_relation_is_publishable": true, "array": true, "acldefault": true, "pg_export_snapshot": true,
	"nextval": true, "currval": true, "lastval": true, "setval": true, "unique_rowid": true, "gen_random_uuid": true, "uuid_generate_v4": true,
	// The text-search functions take the session's default
	// configuration when the call names none.
	"to_tsvector": true, "to_tsquery": true, "plainto_tsquery": true, "phraseto_tsquery": true, "websearch_to_tsquery": true,
	"ts_headline": true, "get_current_ts_config": true,
}

// foldTSQueryCall evaluates a call that makes a tsquery from constants
// (to_tsquery('english', 'cat & hat'), with the session's default
// configuration already supplied), once for the statement: the call is
// a pure function of its arguments, and a filter would otherwise make
// the same query again for every row. A call that fails is left to fail
// where it always has, when a row evaluates it.
func foldTSQueryCall(e parser.Expr, params []types.Datum) (parser.Expr, bool) {
	switch e.Func {
	case "to_tsquery", "plainto_tsquery", "phraseto_tsquery", "websearch_to_tsquery":
	default:
		return e, false
	}
	if len(e.Args) != 2 || e.Left != nil || e.Case != nil {
		return e, false
	}
	for _, a := range e.Args {
		constant := a.Lit != nil || a.Param > 0
		if !constant || a.Column != "" || a.Func != "" || a.BinOp != "" || a.Left != nil || a.Cast != "" ||
			len(a.Path) > 0 || a.Case != nil || a.Cmp != nil || a.Sub != nil || a.Agg != nil || a.Window != nil {
			return e, false
		}
	}
	call := e
	call.BinOp, call.Left, call.Right, call.Cast, call.Path = "", nil, nil, "", nil
	d, err := evalExpr(call, nil, nil, params)
	if err != nil {
		return e, false
	}
	e.Func, e.Args, e.Lit = "", nil, &d
	return e, true
}

// regclassOID resolves a regclass literal: a number is the OID itself,
// a name the table's OID (42P01 when nothing is so named).
func (s *Session) regclassOID(ctx context.Context, txn *kvclient.Txn, text string) (int64, error) {
	if n, err := strconv.ParseInt(text, 10, 64); err == nil {
		return n, nil
	}
	// Read as SQL reads a name: '"Mixed"', 'public.t' and
	// 'pg_catalog.pg_class' (PostgreSQL's own OID) as well as 't'.
	r, err := s.lookupRelationName(ctx, txn, text)
	if err != nil {
		return 0, err
	}
	if r == nil {
		// An index is a relation too ('t_pkey'::regclass).
		if oid, ok, err := s.indexOIDByName(ctx, txn, text); err != nil {
			return 0, err
		} else if ok {
			return oid, nil
		}
		return 0, newErrf(CodeUndefinedTable, "relation %q does not exist", text)
	}
	// Reaching a relation of a schema by name takes USAGE on it (42501),
	// as PostgreSQL's namespace lookup does.
	schemaID := r.desc.SchemaID
	if r.seq != nil {
		schemaID = r.seq.SchemaID
	} else if r.desc.Virtual != "" || r.desc.Temp != nil {
		schemaID = 0
	}
	if err := s.checkObjectSchemaUsage(ctx, txn, schemaID); err != nil {
		return 0, err
	}
	return r.oid, nil
}

// catalogRenderingFuncs are the catalog functions a select item's call
// resolves to a rendering before execution.
var catalogRenderingFuncs = map[string]bool{
	"format_type": true, "pg_get_indexdef": true, "pg_get_constraintdef": true, "pg_get_expr": true,
	"pg_get_viewdef": true, "obj_description": true, "col_description": true,
}

// indexOIDByName resolves an index of the current database's public
// schema by name to its pg_class OID.
func (s *Session) indexOIDByName(ctx context.Context, txn *kvclient.Txn, text string) (int64, bool, error) {
	parts, ok := parseQualifiedName(text)
	if !ok || len(parts) > 2 || len(parts) == 2 && parts[0] != catalog.PublicSchema {
		return 0, false, nil
	}
	name := parts[len(parts)-1]
	db, err := s.cat.Database(ctx, txn, s.database)
	if err != nil || db == nil {
		return 0, false, nil
	}
	tables, err := s.listIn(ctx, txn, db)
	if err != nil {
		return 0, false, ToSQLError(err)
	}
	for _, t := range tables {
		if oid, ok := vtable.IndexOIDByName(t, name); ok {
			return oid, true, nil
		}
	}
	return 0, false, nil
}

// indexOrConstraintDef is pg_get_indexdef(oid [, col, pretty]) or
// pg_get_constraintdef(oid [, pretty]) for a literal OID (false: no
// index or constraint has it, and the call is NULL).
func (s *Session) indexOrConstraintDef(ctx context.Context, txn *kvclient.Txn, e parser.Expr, oid int64) (string, bool) {
	if oid>>16 == 0 {
		return "", false
	}
	desc, err := catalog.ReadTable(ctx, txn, vtable.TableIDOfOID(oid>>16))
	if err != nil || desc == nil || !s.tempVisible(desc) {
		return "", false
	}
	byID := func(id uint64) *catalog.TableDescriptor {
		d, err := catalog.ReadTable(ctx, txn, id)
		if err != nil || d == nil || !s.tempVisible(d) {
			return nil
		}
		return d
	}
	if e.Func == "pg_get_constraintdef" {
		return vtable.ConstraintDefByOID(desc, oid, byID)
	}
	col := int64(0)
	if len(e.Args) > 1 && e.Args[1].Lit != nil && e.Args[1].Lit.Fam == types.Int {
		col = e.Args[1].Lit.I
	}
	return vtable.IndexDefByOID(desc, oid, int(col))
}

// descriptionOf resolves obj_description(oid, class) / col_description
// (oid, attnum) for a literal OID: a table's, an index's or a column's
// COMMENT ("" when none).
func (s *Session) descriptionOf(ctx context.Context, txn *kvclient.Txn, oid int64, e parser.Expr) string {
	tableID, indexID := vtable.TableIDOfOID(oid), uint64(0)
	if oid>>16 != 0 {
		tableID, indexID = vtable.TableIDOfOID(oid>>16), uint64(oid&0xffff)
	}
	desc, err := catalog.ReadTable(ctx, txn, tableID)
	if err != nil || desc == nil || !s.tempVisible(desc) {
		return ""
	}
	if e.Func == "obj_description" && len(e.Args) > 1 && e.Args[1].Lit != nil && e.Args[1].Lit.Text() == "pg_constraint" {
		// A constraint's comment: its primary key, a UNIQUE index, or a
		// CHECK / FOREIGN KEY constraint (pg_constraint's OIDs).
		switch {
		case indexID == 1:
			return desc.PKComment
		case indexID&(1<<15) != 0:
			for i := range desc.Constraints {
				if c := &desc.Constraints[i]; vtable.ConstraintOID(desc, c) == oid {
					return c.Comment
				}
			}
		default:
			for _, idx := range desc.Indexes {
				if idx.ID == indexID && idx.Unique {
					return idx.ConstraintComment
				}
			}
		}
		return ""
	}
	if indexID != 0 {
		for _, idx := range desc.Indexes {
			if idx.ID == indexID {
				return idx.Comment
			}
		}
		return ""
	}
	if e.Func == "col_description" && len(e.Args) > 1 && e.Args[1].Lit != nil {
		n, ok := e.Args[1].Lit.I, e.Args[1].Lit.Fam == types.Int
		if !ok {
			return ""
		}
		pos := int64(0)
		for _, c := range desc.Columns {
			if c.Hidden {
				continue
			}
			pos++
			if pos == n {
				return c.Comment
			}
		}
		return ""
	}
	return desc.Comment
}

// regclassNames lists the (OID, name) of every table the session can
// see — real tables of every database, then the catalogs.
func (s *Session) regclassNames(ctx context.Context, txn *kvclient.Txn) ([]oidName, error) {
	all, err := s.listAll(ctx, txn)
	if err != nil {
		return nil, err
	}
	// A table of a schema the search path does not reach prints
	// qualified, as PostgreSQL's regclass output does.
	visible := map[uint64]bool{0: true}
	schemaNames := map[uint64]string{}
	if path, err := s.lookupPathSchemas(ctx, txn); err == nil {
		for _, sd := range path {
			visible[sd.ID] = true
		}
	}
	if schemas, err := catalog.ListAllSchemas(ctx, txn); err == nil {
		for _, sd := range schemas {
			schemaNames[sd.ID] = sd.Name
		}
	}
	out := make([]oidName, 0, len(all))
	// Names print quoted where they must be, as regclass output does
	// ("Big"."Q" reads back).
	for _, d := range all {
		name := quoteIdentifier(d.Name)
		if d.SchemaID != 0 && !visible[d.SchemaID] {
			name = quoteIdentifier(schemaNames[d.SchemaID]) + "." + name
		}
		out = append(out, oidName{oid: vtable.TableOID(d), name: name})
		if d.IsView() || d.Virtual != "" {
			continue
		}
		// Its indexes, as pg_class lists them and regclassOID resolves
		// them (the primary key's as <table>_pkey when it is visible).
		if oid, ok := vtable.IndexOIDByName(d, d.Name+"_pkey"); ok {
			out = append(out, oidName{oid: oid, name: quoteIdentifier(d.Name + "_pkey")})
		}
		for i := range d.Indexes {
			out = append(out, oidName{oid: vtable.IndexOID(d, d.Indexes[i].ID), name: quoteIdentifier(d.Indexes[i].Name)})
		}
	}
	// Sequences: bare when the name resolves to the sequence through
	// the path (the first path schema holding one of the name), else
	// qualified.
	if dbID, err := s.currentDatabaseID(ctx, txn); err == nil {
		seqs, err := catalog.ListSequences(ctx, txn, dbID)
		if err != nil {
			return nil, err
		}
		rank := map[uint64]int{}
		if path, err := s.lookupPathSchemas(ctx, txn); err == nil {
			for i, sd := range path {
				if _, ok := rank[sd.ID]; !ok {
					rank[sd.ID] = i + 1
				}
			}
		}
		first := map[string]*catalog.SequenceDescriptor{}
		for _, sq := range seqs {
			r := rank[sq.SchemaID]
			if r == 0 {
				continue
			}
			if f := first[sq.Name]; f == nil || r < rank[f.SchemaID] {
				first[sq.Name] = sq
			}
		}
		for _, sq := range seqs {
			name := quoteIdentifier(sq.Name)
			if f := first[sq.Name]; f == nil || f.ID != sq.ID {
				schema := catalog.PublicSchema
				if sq.SchemaID != 0 {
					schema = schemaNames[sq.SchemaID]
				}
				name = quoteIdentifier(schema) + "." + name
			}
			out = append(out, oidName{oid: int64(sq.ID), name: name})
		}
	}
	vtable.EachClassOID(func(oid int64, name string) {
		out = append(out, oidName{oid: oid, name: name})
	})
	return out, nil
}

type oidName struct {
	oid  int64
	name string
}

// floorDivInt is a / b rounded toward negative infinity.
func floorDivInt(a, b int64) int64 {
	q := a / b
	if a%b != 0 && (a < 0) != (b < 0) {
		q--
	}
	return q
}

// typeDisplayNames maps each user-defined type of the current database
// to its name as format_type and regtype print it: quoted, and
// qualified by its schema unless the search path resolves the name to
// it (PostgreSQL's TypeIsVisible).
func (s *Session) typeDisplayNames(ctx context.Context, txn *kvclient.Txn) (map[uint64]string, error) {
	dbID, err := s.currentDatabaseID(ctx, txn)
	if err != nil {
		return nil, err
	}
	tds, err := catalog.ListTypes(ctx, txn, dbID)
	if err != nil || len(tds) == 0 {
		return nil, err
	}
	hidden, err := s.invisibleTypeOIDs(ctx, txn)
	if err != nil {
		return nil, err
	}
	invisible := map[int64]bool{}
	for _, h := range hidden {
		invisible[h] = true
	}
	var schemaNames map[uint64]string
	names := make(map[uint64]string, len(tds))
	for _, td := range tds {
		name := vtable.QuoteIdent(td.Name)
		if invisible[catalog.EnumOID(td.ID)] {
			if schemaNames == nil {
				schemaNames = map[uint64]string{}
				schemas, err := catalog.ListSchemas(ctx, txn, dbID)
				if err != nil {
					return nil, err
				}
				for _, sd := range schemas {
					schemaNames[sd.ID] = sd.Name
				}
			}
			schema := catalog.PublicSchema
			if td.SchemaID != 0 {
				schema = schemaNames[td.SchemaID]
			}
			name = vtable.QuoteIdent(schema) + "." + name
		}
		names[td.ID] = name
	}
	return names, nil
}

// formatEnumTypes resolves format_type(oid, ...) for the database's
// enum types: a literal OID becomes the name, any other argument a
// CASE over the enums' OIDs that falls back to the call.
func (s *Session) formatEnumTypes(ctx context.Context, txn *kvclient.Txn, e parser.Expr) (parser.Expr, error) {
	dbID, err := s.currentDatabaseID(ctx, txn)
	if err != nil {
		return e, err
	}
	tds, err := catalog.ListTypes(ctx, txn, dbID)
	if err != nil || len(tds) == 0 {
		return e, err
	}
	// A type of a schema the search path does not reach prints
	// qualified, as format_type does in PostgreSQL.
	names, err := s.typeDisplayNames(ctx, txn)
	if err != nil {
		return e, err
	}
	if lit := e.Args[0].Lit; lit != nil && lit.Fam == types.Int && e.Args[0].BinOp == "" && e.Args[0].Cast == "" {
		for _, td := range tds {
			if catalog.EnumOID(td.ID) == lit.I {
				d := types.NewString(names[td.ID])
				return parser.Expr{Lit: &d}, nil
			}
		}
		return e, nil
	}
	if e.Args[0].Lit != nil {
		return e, nil
	}
	inner := e
	ce := &parser.CaseExpr{Operand: &e.Args[0], Else: &inner}
	for _, td := range tds {
		oid, name := types.NewInt(catalog.EnumOID(td.ID)), types.NewString(names[td.ID])
		ce.Whens = append(ce.Whens, parser.CaseWhen{Value: &parser.Expr{Lit: &oid}, Result: parser.Expr{Lit: &name}})
	}
	return parser.Expr{Case: ce}, nil
}

// isRegtypeCast reports whether a cast names regtype.
func isRegtypeCast(c string) bool { return c == "regtype" || c == "pg_catalog.regtype" }

// enumTypeByText finds the enum type a type name written as text
// names, read as SQL reads a name (stringToQualifiedNameList): an
// unquoted part folded to lower case, a quoted one kept, resolved
// through the search path or its schema qualifier. A builtin's name is
// no enum (ok false).
func (s *Session) enumTypeByText(ctx context.Context, txn *kvclient.Txn, text string) (*catalog.TypeDescriptor, bool, error) {
	parts, ok := parseQualifiedName(text)
	if !ok {
		return nil, false, nil
	}
	if len(parts) == 2 && parts[0] == vtable.PgCatalog {
		parts = parts[1:]
	}
	if len(parts) == 1 && builtins.KnownCastType(parts[0]) {
		return nil, false, nil
	}
	name, ok := relationLookupName(parts)
	if !ok {
		return nil, false, nil
	}
	td, err := s.cat.LookupTypeIn(s.rctx(ctx), txn, s.database, name)
	if err != nil || td == nil {
		return nil, false, nil
	}
	// A type of a schema without USAGE is not to be reached by name
	// (42501, as PostgreSQL's namespace lookup).
	if err := s.checkObjectSchemaUsage(ctx, txn, td.SchemaID); err != nil {
		return nil, false, err
	}
	return td, true, nil
}

// constArgFuncs are the spliced functions whose splice evaluates their
// arguments as constants (the others take their arguments as columns,
// subqueries or sets themselves).
var constArgFuncs = map[string]bool{
	"pg_cancel_backend": true, "pg_terminate_backend": true, "pg_sleep": true, "pg_notify": true,
	"set_config": true, "current_setting": true, "nextval": true, "currval": true, "setval": true,
}

// argsNeedSplice reports whether a call's arguments hold a spliced
// call or a subquery, which must resolve before a spliced function
// evaluates them.
func argsNeedSplice(args []parser.Expr) bool {
	for _, a := range args {
		if exprHas(a, func(x parser.Expr) bool { return x.Sub != nil || (x.Func != "" && splicedFuncs[x.Func]) }) {
			return true
		}
	}
	return false
}

// spliceUserByID resolves pg_get_userbyid(oid): a literal OID becomes
// the role's name, any other argument a CASE over the roles' OIDs
// (NULL for an OID no role has).
func (s *Session) spliceUserByID(ctx context.Context, txn *kvclient.Txn, e parser.Expr) (parser.Expr, error) {
	if len(e.Args) != 1 {
		return e, newErrf(CodeUndefinedFunction, "function pg_get_userbyid takes one argument")
	}
	var roles []*catalog.RoleDescriptor
	read := func(ctx context.Context, txn *kvclient.Txn) error {
		var err error
		roles, err = catalog.ListRoles(ctx, txn)
		return err
	}
	var err error
	if txn != nil {
		err = read(ctx, txn)
	} else {
		err = s.db.RunTxn(ctx, "pg-get-userbyid", read)
	}
	if err != nil {
		return e, err
	}
	if lit := e.Args[0].Lit; lit != nil && e.Args[0].BinOp == "" {
		if lit.Null {
			d := types.DNull
			return parser.Expr{Lit: &d}, nil
		}
		oid, cerr := lit.Coerce(types.Int)
		if cerr == nil {
			for _, r := range roles {
				if vtable.RoleOID(r.Name) == oid.I {
					d := types.NewString(r.Name)
					return parser.Expr{Lit: &d}, nil
				}
			}
		}
		d := types.DNull
		return parser.Expr{Lit: &d}, nil
	}
	null := types.DNull
	ce := &parser.CaseExpr{Operand: &e.Args[0], Else: &parser.Expr{Lit: &null}}
	for _, r := range roles {
		oid, name := types.NewInt(vtable.RoleOID(r.Name)), types.NewString(r.Name)
		ce.Whens = append(ce.Whens, parser.CaseWhen{Value: &parser.Expr{Lit: &oid}, Result: parser.Expr{Lit: &name}})
	}
	return parser.Expr{Case: ce}, nil
}

// invisibleTypeOIDs are the pg_type OIDs pg_type_is_visible is false
// for: the user-defined types of the current database that the search
// path does not reach, or reaches only after another of the name.
func (s *Session) invisibleTypeOIDs(ctx context.Context, txn *kvclient.Txn) ([]int64, error) {
	var hidden []int64
	read := func(ctx context.Context, txn *kvclient.Txn) error {
		db, err := catalog.LookupDatabase(ctx, txn, s.database)
		if err != nil || db.ID == 0 {
			return nil
		}
		tds, err := catalog.ListTypes(ctx, txn, db.ID)
		if err != nil || len(tds) == 0 {
			return err
		}
		// Visible: the first type of its name along the path, as
		// PostgreSQL's TypeIsVisible (an empty path reaches none, so
		// pg_dump's prints every type qualified).
		path, err := s.pathSchemas(ctx, txn)
		if err != nil {
			return err
		}
		rank := map[uint64]int{}
		for i, sd := range path {
			if _, ok := rank[sd.ID]; !ok {
				rank[sd.ID] = i + 1
			}
		}
		first := map[string]*catalog.TypeDescriptor{}
		for _, td := range tds {
			r := rank[td.SchemaID]
			if r == 0 {
				continue
			}
			if f := first[td.Name]; f == nil || r < rank[f.SchemaID] {
				first[td.Name] = td
			}
		}
		for _, td := range tds {
			if f := first[td.Name]; f == nil || f.ID != td.ID {
				hidden = append(hidden, catalog.EnumOID(td.ID))
			}
		}
		return nil
	}
	var err error
	if txn != nil {
		err = read(ctx, txn)
	} else {
		err = s.db.RunTxn(ctx, "type-is-visible", read)
	}
	return hidden, err
}

// invisibleClassOIDs are the pg_class OIDs pg_table_is_visible is false
// for: information_schema's views, and the tables, views, indexes and
// sequences of the current database's schemas that the search path
// does not reach.
func (s *Session) invisibleClassOIDs(ctx context.Context, txn *kvclient.Txn) ([]int64, error) {
	hidden := vtable.InformationSchemaClassOIDs()
	read := func(ctx context.Context, txn *kvclient.Txn) error {
		path, err := s.lookupPathSchemas(ctx, txn)
		if err != nil {
			return err
		}
		visible := map[uint64]bool{}
		for _, sd := range path {
			visible[sd.ID] = true
		}
		db, err := catalog.LookupDatabase(ctx, txn, s.database)
		if err != nil || db.ID == 0 {
			return nil
		}
		schemas, err := catalog.ListSchemas(ctx, txn, db.ID)
		if err != nil || len(schemas) == 0 {
			return err
		}
		all, err := s.listIn(ctx, txn, db)
		if err != nil {
			return err
		}
		seqs, err := catalog.ListSequences(ctx, txn, db.ID)
		if err != nil {
			return err
		}
		for _, sq := range seqs {
			if !visible[sq.SchemaID] {
				hidden = append(hidden, int64(sq.ID))
			}
		}
		for _, d := range all {
			if d.Temp != nil || visible[d.SchemaID] {
				continue
			}
			hidden = append(hidden, vtable.TableOID(d))
			if d.IsView() {
				continue
			}
			if oid, ok := vtable.IndexOIDByName(d, d.Name+"_pkey"); ok {
				hidden = append(hidden, oid)
			}
			for i := range d.Indexes {
				hidden = append(hidden, vtable.IndexOID(d, d.Indexes[i].ID))
			}
		}
		return nil
	}
	var err error
	if txn != nil {
		err = read(ctx, txn)
	} else {
		err = s.db.RunTxn(ctx, "table-is-visible", read)
	}
	return hidden, err
}
