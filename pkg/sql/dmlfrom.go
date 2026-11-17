package sql

import (
	"context"

	"github.com/sthorne/datax/pkg/kvclient"
	"github.com/sthorne/datax/pkg/sql/catalog"
	"github.com/sthorne/datax/pkg/sql/parser"
	"github.com/sthorne/datax/pkg/sql/rowenc"
	"github.com/sthorne/datax/pkg/sql/types"
)

// UPDATE ... FROM and DELETE ... USING (issue #355).
//
// The statement's target is joined with its FROM (USING) list by the
// select executor itself: a select over the target and the list's
// members, with the statement's WHERE, runs through execSelect, so
// everything a SELECT's FROM list can hold — tables, aliases, derived
// tables, VALUES lists, WITH members, views, table functions, explicit
// JOINs — and everything its WHERE can say works the same here. The
// select carries a marker (Select.DMLTarget) on which execJoinSelect
// hands back its joined rows; each target row the join matched is then
// read again by primary key, under the statement's own row-security
// policies, and written through the same path as a plain UPDATE or
// DELETE (unique keys, foreign keys and cascades, WITH CHECK, moved
// primary keys).
//
// A target row joining several rows is updated or deleted once. An
// UPDATE takes its new values (and RETURNING its FROM columns) from the
// first of its joined rows in the order the join produced them: the
// FROM list's rows in the order they are read — by primary key for a
// table read whole, by the index for one looked up through an index, as
// written for a VALUES list — member by member. PostgreSQL leaves the
// choice unspecified.

// dmlJoin receives the joined rows of the select an UPDATE ... FROM or
// DELETE ... USING runs: the sides in execution order, the joined rows
// after WHERE, and the select-list values computed for each.
type dmlJoin struct {
	sides  []joinSide
	joined []joinedRow
	out    [][]types.Datum
}

// dmlTargetAlias is the name the target answers to in the join: its
// alias, else its bare table name.
func dmlTargetAlias(table, alias string) string {
	if alias != "" {
		return alias
	}
	_, bare := catalog.SplitTableName(table)
	return bare
}

// dmlFromSelect builds the select joining the target with the FROM list.
// With targetFirst the target heads it (it is read once, the list's
// members looked up per target row); otherwise it is joined last, looked
// up per row of the list — through its primary key or an index when the
// WHERE equates one with the list. Either way the target is a CROSS
// member, so the list's own joins (RIGHT, FULL, LATERAL, USING) see only
// the list, as in PostgreSQL.
func dmlFromSelect(table, alias string, from []parser.JoinClause, where []parser.Comparison, exprs []parser.SelectExpr, targetFirst bool) *parser.Select {
	ta := dmlTargetAlias(table, alias)
	sel := &parser.Select{Limit: -1, Exprs: exprs, Where: where, DMLTarget: ta}
	if targetFirst {
		sel.Table, sel.Alias = table, ta
		sel.Joins = append([]parser.JoinClause(nil), from...)
		return sel
	}
	head := from[0]
	sel.Alias, sel.Derived, sel.FuncTable, sel.FuncCols = head.Alias, head.Derived, head.FuncTable, head.FuncCols
	if head.Derived == nil {
		sel.Table = head.Table
	}
	sel.Joins = append(append([]parser.JoinClause(nil), from[1:]...), parser.JoinClause{Cross: true, Table: table, Alias: ta})
	return sel
}

// dmlTargetFirst picks the target's place in the join. The target is
// joined last — looked up per row of the FROM list — when the WHERE
// equates the leading column of its primary key or of one of its
// indexes with a column of the list (UPDATE orders o ... FROM staging s
// WHERE o.id = s.id reads staging once and orders by key), or when the
// list has joins of its own. Otherwise it is read once, ahead of the
// list (DELETE FROM t USING u WHERE t.uid = u.id with t.uid unindexed
// looks u up by its key per row of t, instead of scanning t per row of
// u).
func dmlTargetFirst(desc *catalog.TableDescriptor, alias string, from []parser.JoinClause, where []parser.Comparison) bool {
	for _, j := range from {
		if !j.Cross || j.Lateral || j.Natural {
			return false
		}
	}
	lead := map[catalog.ColumnID]bool{}
	for _, id := range desc.PrimaryKey {
		if col, ok := desc.ColByID(id); ok && !col.Hidden {
			lead[id] = true
			break
		}
	}
	for i := range desc.Indexes {
		idx := &desc.Indexes[i]
		if idx.Public() && !idx.IsInverted() && len(idx.ColumnIDs) > 0 {
			lead[idx.ColumnIDs[0]] = true
		}
	}
	ta := dmlTargetAlias("", alias)
	// target classifies a column reference: 2 names a target column, 1
	// is a bare name the target has (it may be another side's), 0 is
	// some other side's.
	target := func(name string) (catalog.Column, int) {
		q, c := splitQualified(name)
		col, ok := desc.Col(c)
		switch {
		case !ok:
			return col, 0
		case q == "":
			return col, 1
		case q == ta || q == desc.Name:
			return col, 2
		}
		return col, 0
	}
	for _, cmp := range where {
		if cmp.Op != "=" || cmp.Column == "" || len(cmp.Path) > 0 || cmp.Expr != nil || !plainExpr(cmp.Value) || cmp.Value.Column == "" {
			continue
		}
		lc, lk := target(cmp.Column)
		rc, rk := target(cmp.Value.Column)
		if lk > 0 && rk == 0 && lead[lc.ID] || rk > 0 && lk == 0 && lead[rc.ID] {
			return false
		}
	}
	return true
}

// runDMLJoin runs the select joining the target desc with the FROM list
// and returns its joined rows and the target's side among them.
func (s *Session) runDMLJoin(ctx context.Context, txn *kvclient.Txn, desc *catalog.TableDescriptor, table, alias string, from []parser.JoinClause, where []parser.Comparison, exprs []parser.SelectExpr, params []types.Datum) (*dmlJoin, int, error) {
	targetFirst := dmlTargetFirst(desc, alias, from, where)
	if len(exprs) == 0 {
		one := types.NewInt(1)
		exprs = []parser.SelectExpr{{Expr: parser.Expr{Lit: &one}}}
	}
	sel := dmlFromSelect(table, alias, from, where, exprs, targetFirst)
	dj := &dmlJoin{}
	prev := s.dmlJoin
	s.dmlJoin = dj
	defer func() { s.dmlJoin = prev }()
	res, err := s.execSelect(ctx, txn, sel, params)
	if err != nil {
		return nil, 0, err
	}
	if dj.sides == nil {
		if len(res.Rows) == 0 {
			// Answered without joining (a FROM list headed by an
			// always-empty catalog): nothing matched.
			return dj, 0, nil
		}
		return nil, 0, newErrf(CodeInternal, "the join of %s with its FROM list did not report its rows", desc.Name)
	}
	ti := 0
	if !targetFirst {
		ti = len(dj.sides) - 1
	}
	if dj.sides[ti].desc.ID != desc.ID {
		return nil, 0, newErrf(CodeInternal, "the join of %s with its FROM list lost its target", desc.Name)
	}
	return dj, ti, nil
}

// dmlTargetRows reads the target rows the join matched, each once, by
// primary key and filtered by the policies of cmds (the statement's own
// command: the join read the target under its SELECT policies already),
// returning with each the index of its first joined row.
func (s *Session) dmlTargetRows(ctx context.Context, txn *kvclient.Txn, desc *catalog.TableDescriptor, dj *dmlJoin, ti int, params []types.Datum, cmds ...string) ([]fetchedRow, []int, error) {
	first := map[string]int{}
	var points [][]types.Datum
	for i, jr := range dj.joined {
		row := jr.rows[ti]
		if row == nil {
			continue
		}
		vals := make([]types.Datum, len(desc.PrimaryKey))
		for k, id := range desc.PrimaryKey {
			vals[k] = rowVal(row, id)
		}
		key, err := rowenc.EncodePK(desc, vals)
		if err != nil {
			return nil, nil, err
		}
		if _, seen := first[string(key)]; seen {
			continue
		}
		first[string(key)] = i
		points = append(points, vals)
	}
	if len(points) == 0 {
		return nil, nil, nil
	}
	where, _, err := s.securedWhere(ctx, txn, desc, nil, cmds...)
	if err != nil {
		return nil, nil, err
	}
	rows, err := s.executePlan(ctx, txn, desc, accessPlan{kind: planPKPoints, pkPoints: points}, where, params, 0)
	if err != nil {
		return nil, nil, err
	}
	at := make([]int, len(rows))
	for i, fr := range rows {
		j, ok := first[string(fr.key)]
		if !ok {
			return nil, nil, newErrf(CodeInternal, "a row of %s read back under a key the join did not match", desc.Name)
		}
		at[i] = j
	}
	return rows, at, nil
}

// withRow returns jr with side i's row replaced.
func withRow(jr joinedRow, i int, row map[catalog.ColumnID]types.Datum) joinedRow {
	out := make([]map[catalog.ColumnID]types.Datum, len(jr.rows))
	copy(out, jr.rows)
	out[i] = row
	return joinedRow{rows: out}
}

// dmlReturning is a RETURNING list over the target and the FROM list:
// the sides in written order (the target first, so RETURNING * is the
// target's columns and then each member's, as in PostgreSQL), and the
// projection resolved against them.
type dmlReturning struct {
	sides []joinSide
	proj  []joinProj
	ti    int // the target's side in execution order
}

// checkReturningExprs refuses what RETURNING cannot hold.
func checkReturningExprs(exprs []parser.SelectExpr) error {
	for _, se := range exprs {
		if se.Agg != "" || !se.Star && exprHasAgg(se.Expr) {
			return newErrf(CodeGrouping, "aggregate functions are not allowed in RETURNING")
		}
		if !se.Star && exprHasSubquery(se.Expr) {
			return newErrf(CodeFeatureNotSupported, "subqueries are not supported in RETURNING")
		}
	}
	return nil
}

// dmlReturningFor resolves a RETURNING list against the join's sides
// (in execution order, the target at ti).
func dmlReturningFor(sides []joinSide, ti int, exprs []parser.SelectExpr) (*dmlReturning, error) {
	if exprs == nil {
		return nil, nil
	}
	if err := checkReturningExprs(exprs); err != nil {
		return nil, err
	}
	written := sides
	if ti != 0 {
		// The target was joined last: put it first again, and renumber
		// the USING merges between the list's members.
		written = make([]joinSide, 0, len(sides))
		written = append(written, sides[ti])
		for _, js := range sides[:ti] {
			if js.merged != nil {
				m := make(map[string]int, len(js.merged))
				for k, v := range js.merged {
					m[k] = v + 1
				}
				js.merged = m
			}
			written = append(written, js)
		}
	}
	proj, err := resolveJoinProjection(written, exprs)
	if err != nil {
		return nil, err
	}
	return &dmlReturning{sides: written, proj: proj, ti: ti}, nil
}

func (r *dmlReturning) columns() []ResultColumn {
	cols := make([]ResultColumn, len(r.proj))
	for i, p := range r.proj {
		cols[i] = exprResult(p.name, p.typ, p.ref.col)
	}
	return cols
}

// project evaluates the list over a joined row (execution order) whose
// target is row.
func (r *dmlReturning) project(jr joinedRow, row map[catalog.ColumnID]types.Datum, params []types.Datum) ([]types.Datum, error) {
	rows := make([]map[catalog.ColumnID]types.Datum, 0, len(jr.rows))
	rows = append(rows, row)
	for i, sr := range jr.rows {
		if i != r.ti {
			rows = append(rows, sr)
		}
	}
	wr := joinedRow{rows: rows}
	out := make([]types.Datum, len(r.proj))
	for i, p := range r.proj {
		if p.expr != nil {
			d, err := evalExprEnv(*p.expr, joinEnv{r.sides, wr}, params)
			if err != nil {
				return nil, err
			}
			out[i] = conformTo(d, p.typ)
			continue
		}
		out[i] = wr.datum(p.ref)
	}
	return out, nil
}

// dmlReturningColumns describes the RETURNING list of an UPDATE ... FROM
// or DELETE ... USING without running it, by describing the select over
// the target and the list (the target first).
func (s *Session) dmlReturningColumns(ctx context.Context, table, alias string, from []parser.JoinClause, exprs []parser.SelectExpr) ([]ResultColumn, *Error) {
	if exprs == nil {
		return nil, nil
	}
	if err := checkReturningExprs(exprs); err != nil {
		return nil, ToSQLError(err)
	}
	sel := dmlFromSelect(table, alias, from, nil, exprs, true)
	return s.PlanColumns(ctx, sel)
}

// execUpdateFrom runs UPDATE t [AS a] SET ... FROM list [WHERE ...]
// [RETURNING ...] (its WITH members already bound).
func (s *Session) execUpdateFrom(ctx context.Context, txn *kvclient.Txn, t *parser.Update, params []types.Datum) (*Result, error) {
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
	// A SET value holding a subquery — which may read the target and
	// the FROM list's columns — is computed by the join's select list;
	// every other value is evaluated here, on the joined row.
	set := append([]parser.SetClause(nil), t.Set...)
	var exprs []parser.SelectExpr
	subAt := make([]int, len(set))
	for j := range set {
		subAt[j] = -1
		v := set[j].Value
		switch {
		case v.IsDefault:
		case exprHasSubquery(v):
			subAt[j] = len(exprs)
			exprs = append(exprs, parser.SelectExpr{Expr: v})
		case s.needsResolve(v):
			if set[j].Value, err = s.resolveValueExpr(ctx, txn, v, params); err != nil {
				return nil, err
			}
		}
	}
	returning := t.Returning
	if r, err := s.resolveReturning(ctx, txn, returning, params); err != nil {
		return nil, err
	} else if r != nil {
		returning = r
	}
	if err := checkReturningExprs(returning); err != nil {
		return nil, err
	}
	dj, ti, err := s.runDMLJoin(ctx, txn, desc, t.Table, t.Alias, t.From, t.Where, exprs, params)
	if err != nil {
		return nil, err
	}
	if dj.sides != nil {
		// Bad or ambiguous names are refused whether or not a row
		// matched.
		for j, sc := range set {
			if subAt[j] < 0 && !sc.Value.IsDefault {
				if err := walkExprRefs(dj.sides, sc.Value); err != nil {
					return nil, err
				}
			}
		}
	}
	// Row security: the join read the target under its SELECT policies;
	// the UPDATE policies' USING picks the rows that may change, and
	// each new row must pass both kinds' WITH CHECK.
	rows, at, err := s.dmlTargetRows(ctx, txn, desc, dj, ti, params, catalog.PolicyUpdate)
	if err != nil {
		return nil, err
	}
	// The SET values read the rows as they were before the statement.
	old := make([]joinedRow, len(rows))
	for i, fr := range rows {
		old[i] = withRow(dj.joined[at[i]], ti, copyRow(fr.row))
	}
	u := updateApply{
		desc: desc, set: set, rows: rows, pkSet: pkSet, reads: true,
		eval: func(i, j int) (types.Datum, error) {
			if subAt[j] >= 0 {
				return dj.out[at[i]][subAt[j]], nil
			}
			value := set[j].Value
			if exprIsVolatile(value) {
				var err error
				if value, err = s.spliceVolatile(ctx, txn, value, params); err != nil {
					return types.Datum{}, err
				}
			}
			return evalExprEnv(value, joinEnv{dj.sides, old[i]}, params)
		},
	}
	if returning != nil {
		if dj.sides == nil {
			cols, serr := s.dmlReturningColumns(ctx, t.Table, t.Alias, t.From, returning)
			if serr != nil {
				return nil, serr
			}
			return &Result{Columns: cols, Tag: "UPDATE 0"}, nil
		}
		ret, err := dmlReturningFor(dj.sides, ti, returning)
		if err != nil {
			return nil, err
		}
		u.retCols = ret.columns()
		u.ret = func(i int, row map[catalog.ColumnID]types.Datum) ([]types.Datum, error) {
			return ret.project(old[i], row, params)
		}
	}
	return s.applyUpdate(ctx, txn, &u, params)
}

// execDeleteUsing runs DELETE FROM t [AS a] USING list [WHERE ...]
// [RETURNING ...] (its WITH members already bound).
func (s *Session) execDeleteUsing(ctx context.Context, txn *kvclient.Txn, t *parser.Delete, params []types.Datum) (*Result, error) {
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
	returning := t.Returning
	if r, err := s.resolveReturning(ctx, txn, returning, params); err != nil {
		return nil, err
	} else if r != nil {
		returning = r
	}
	if err := checkReturningExprs(returning); err != nil {
		return nil, err
	}
	dj, ti, err := s.runDMLJoin(ctx, txn, desc, t.Table, t.Alias, t.Using, t.Where, nil, params)
	if err != nil {
		return nil, err
	}
	// Row security: SELECT policies applied in the join, the DELETE
	// policies' USING here.
	rows, at, err := s.dmlTargetRows(ctx, txn, desc, dj, ti, params, catalog.PolicyDelete)
	if err != nil {
		return nil, err
	}
	var project func(int, map[catalog.ColumnID]types.Datum) ([]types.Datum, error)
	var retCols []ResultColumn
	if returning != nil {
		if dj.sides == nil {
			cols, serr := s.dmlReturningColumns(ctx, t.Table, t.Alias, t.Using, returning)
			if serr != nil {
				return nil, serr
			}
			return &Result{Columns: cols, Tag: "DELETE 0"}, nil
		}
		ret, err := dmlReturningFor(dj.sides, ti, returning)
		if err != nil {
			return nil, err
		}
		retCols = ret.columns()
		project = func(i int, row map[catalog.ColumnID]types.Datum) ([]types.Datum, error) {
			return ret.project(dj.joined[at[i]], row, params)
		}
	}
	return s.applyDelete(ctx, txn, desc, rows, project, retCols)
}

// stripUpdateAlias addresses the target's columns by bare name in an
// aliased single-table UPDATE (a.c becomes c in SET, WHERE and
// RETURNING; subqueries keep theirs, which correlate through the alias).
func stripUpdateAlias(t *parser.Update) *parser.Update {
	c := *t
	sel := &parser.Select{Table: t.Table, Alias: t.Alias, Where: append([]parser.Comparison(nil), t.Where...)}
	for _, sc := range t.Set {
		sel.Exprs = append(sel.Exprs, parser.SelectExpr{Expr: sc.Value})
	}
	sel.Exprs = append(sel.Exprs, t.Returning...)
	stripTableAlias(sel)
	c.Set = append([]parser.SetClause(nil), t.Set...)
	for i := range c.Set {
		c.Set[i].Value = sel.Exprs[i].Expr
	}
	if t.Returning != nil {
		c.Returning = sel.Exprs[len(t.Set):]
	}
	c.Where = sel.Where
	return &c
}

// stripDeleteAlias is stripUpdateAlias for DELETE.
func stripDeleteAlias(t *parser.Delete) *parser.Delete {
	c := *t
	sel := &parser.Select{Table: t.Table, Alias: t.Alias, Where: append([]parser.Comparison(nil), t.Where...), Exprs: append([]parser.SelectExpr(nil), t.Returning...)}
	stripTableAlias(sel)
	if t.Returning != nil {
		c.Returning = sel.Exprs
	}
	c.Where = sel.Where
	return &c
}
