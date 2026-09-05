package sql

import (
	"context"
	"fmt"
	"strings"

	"github.com/sthorne/datax/pkg/kvclient"
	"github.com/sthorne/datax/pkg/sql/catalog"
	"github.com/sthorne/datax/pkg/sql/parser"
	"github.com/sthorne/datax/pkg/sql/types"
	"github.com/sthorne/datax/pkg/sql/vtable"
)

// Statement-scoped relations: the rows a WITH member (or a derived table
// that is joined) produced, bound under a name for the rest of the
// statement. A relation reads like a virtual table — lookup resolves the
// name to a descriptor whose Virtual is "cte:<name>", and fetchVirtual
// serves its rows through the ordinary access path — so it works
// wherever a table does: as the base of a select, a join side, a
// subquery source, a set-operation member, an INSERT source. Every
// member is materialized once, in order (later members see earlier
// ones); a WITH RECURSIVE member iterates. Bindings are restored when
// the statement that made them finishes, so nested WITHs shadow.

// relation is one bound relation.
type relation struct {
	desc *catalog.TableDescriptor
	rows [][]types.Datum
	// alias is a database-qualified name the relation also answers to
	// (a view referenced as db.v).
	alias string
}

// relationPrefix marks a relation's descriptor.
const relationPrefix = "cte:"

// recursiveIterationCap bounds a WITH RECURSIVE evaluation.
const recursiveIterationCap = 10000

// relationDesc renders bound columns as a descriptor: a table with a
// primary key on a hidden ordinal column, like a virtual table's.
func relationDesc(name string, cols []ResultColumn, rename []string) (*catalog.TableDescriptor, error) {
	if len(rename) > len(cols) {
		return nil, newErrf(CodeSyntaxError, "WITH %s has %d columns available but %d columns specified", name, len(cols), len(rename))
	}
	out := make([]catalog.Column, 0, len(cols)+1)
	for i, rc := range cols {
		cname := strings.ToLower(rc.Name)
		if i < len(rename) {
			cname = strings.ToLower(rename[i])
		}
		typ := rc.Type
		if typ == types.Unknown {
			typ = types.String
		}
		out = append(out, catalog.Column{ID: catalog.ColumnID(i + 1), Name: cname, Type: typ})
	}
	ord := catalog.Column{ID: catalog.ColumnID(len(out) + 1), Name: "_ord", Type: types.Int, NotNull: true, Hidden: true}
	out = append(out, ord)
	return &catalog.TableDescriptor{
		ID: vtable.VirtualTableID, Name: name, Columns: out,
		PrimaryKey: []catalog.ColumnID{ord.ID}, Virtual: relationPrefix + name,
	}, nil
}

// bindRelation binds rows under name, returning what it replaced.
func (s *Session) bindRelation(name string, r *relation) (prev *relation, had bool) {
	if s.rels == nil {
		s.rels = map[string]*relation{}
	}
	prev, had = s.rels[name]
	s.rels[name] = r
	return prev, had
}

// restoreRelations puts the shadowed bindings back.
func (s *Session) restoreRelations(saved map[string]*relation) {
	for name, prev := range saved {
		if prev == nil {
			delete(s.rels, name)
		} else {
			s.rels[name] = prev
		}
	}
}

// relationRows serves a bound relation's rows as fetched rows (with the
// hidden ordinal), filtered by where.
func (s *Session) relationRows(desc *catalog.TableDescriptor, where []parser.Comparison, params []types.Datum, limit int64) ([]fetchedRow, error) {
	name := strings.TrimPrefix(desc.Virtual, relationPrefix)
	r, ok := s.rels[name]
	if !ok {
		return nil, newErrf(CodeInternal, "relation %q is not bound", name)
	}
	ord := desc.PrimaryKey[0]
	var out []fetchedRow
	for i, vals := range r.rows {
		row := make(map[catalog.ColumnID]types.Datum, len(vals)+1)
		for j, d := range vals {
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
		if err := s.chargeRow(row); err != nil {
			return nil, err
		}
		out = append(out, fetchedRow{row: row})
		if limit > 0 && int64(len(out)) >= limit {
			break
		}
	}
	return out, nil
}

// bindWith materializes a statement's WITH members in order and binds
// them; the returned function restores the bindings they shadowed. With
// describeOnly the members are bound with their columns and no rows,
// for planning without execution (a recursive member's columns are its
// seed's), and onMember, if given, runs after each member is bound —
// with the member itself visible, as its recursive step needs.
func (s *Session) bindWith(ctx context.Context, txn *kvclient.Txn, with []parser.CTE, params []types.Datum, describeOnly bool, onMember func(parser.CTE) error) (func(), error) {
	saved := map[string]*relation{}
	restore := func() { s.restoreRelations(saved) }
	for _, cte := range with {
		name := strings.ToLower(cte.Name)
		var (
			res *Result
			err error
		)
		switch {
		case describeOnly:
			shape := cte.Query
			if sel, ok := cte.Query.(*parser.Select); ok && cte.Recursive && sel.Union != nil {
				seed := *sel
				seed.Union, seed.SetOp, seed.UnionAll = nil, "", false
				shape = &seed
			}
			cols, serr := s.PlanColumns(ctx, shape)
			if serr != nil {
				restore()
				return nil, serr
			}
			res = &Result{Columns: cols}
		case cte.Recursive && stmtReferences(cte.Query, name):
			res, err = s.execRecursiveCTE(ctx, txn, cte, name, params)
		default:
			// A view's query runs with its owner's privileges (definer
			// semantics): the tables it reads are checked as the owner,
			// the view itself was checked as the reader when it was bound.
			savedAs := s.privAs
			if cte.Definer != "" {
				s.privAs = cte.Definer
			}
			res, err = s.execStmt(ctx, txn, cte.Query, params)
			s.privAs = savedAs
		}
		if err != nil {
			restore()
			return nil, err
		}
		if res == nil || res.Columns == nil {
			restore()
			return nil, newErrf(CodeFeatureNotSupported, "WITH %s: a data-modifying statement in WITH must have RETURNING", cte.Name)
		}
		desc, err := relationDesc(name, res.Columns, cte.Columns)
		if err != nil {
			restore()
			return nil, err
		}
		if prev, had := s.bindRelation(name, &relation{desc: desc, rows: res.Rows, alias: cte.Qualified}); had {
			if _, seen := saved[name]; !seen {
				saved[name] = prev
			}
		} else if _, seen := saved[name]; !seen {
			saved[name] = nil
		}
		if onMember != nil {
			if err := onMember(cte); err != nil {
				restore()
				return nil, err
			}
		}
	}
	return restore, nil
}

// execRecursiveCTE evaluates WITH RECURSIVE name AS (seed UNION [ALL]
// step): the seed's rows start the working table; the step runs with
// name bound to the working table, its new rows (all of them under
// UNION ALL; the not-yet-seen ones under UNION) become the next working
// table, until a step produces nothing. Bounded by an iteration cap and
// the set-operation row cap.
func (s *Session) execRecursiveCTE(ctx context.Context, txn *kvclient.Txn, cte parser.CTE, name string, params []types.Datum) (*Result, error) {
	sel, ok := cte.Query.(*parser.Select)
	if !ok || sel.Union == nil {
		return nil, newErrf(CodeSyntaxError, "recursive query %q must have the form non-recursive-term UNION [ALL] recursive-term", cte.Name)
	}
	seed := *sel
	seed.Union, seed.SetOp, seed.UnionAll = nil, "", false
	if stmtReferences(&seed, name) {
		return nil, newErrf(CodeSyntaxError, "recursive reference to query %q must not appear within its non-recursive term", cte.Name)
	}
	all := sel.UnionAll
	step := sel.Union
	seedRes, err := s.execSelect(ctx, txn, &seed, params)
	if err != nil {
		return nil, err
	}
	// Rename the columns before the step runs, so the step sees them.
	desc, err := relationDesc(name, seedRes.Columns, cte.Columns)
	if err != nil {
		return nil, err
	}
	seen := map[string]bool{}
	var result [][]types.Datum
	working := seedRes.Rows
	if !all {
		working = nil
		for _, r := range seedRes.Rows {
			if k := encodeGroupKey(r); !seen[k] {
				seen[k] = true
				working = append(working, r)
			}
		}
	}
	prev, had := s.bindRelation(name, &relation{desc: desc})
	defer func() {
		if had {
			s.rels[name] = prev
		} else {
			delete(s.rels, name)
		}
	}()
	for iter := 0; len(working) > 0; iter++ {
		result = append(result, working...)
		if len(result) > setOpRowCap {
			return nil, newErrf(CodeProgramLimitExceeded, "recursive query %q materializes more than %d rows", cte.Name, setOpRowCap)
		}
		if iter >= recursiveIterationCap {
			return nil, newErrf(CodeProgramLimitExceeded, "recursive query %q did not finish within %d iterations", cte.Name, recursiveIterationCap)
		}
		s.rels[name] = &relation{desc: desc, rows: working}
		stepRes, err := s.execSelect(ctx, txn, step, params)
		if err != nil {
			return nil, err
		}
		if len(stepRes.Columns) != len(seedRes.Columns) {
			return nil, newErrf(CodeSyntaxError, "recursive query %q: each UNION query must have the same number of columns", cte.Name)
		}
		var next [][]types.Datum
		for _, r := range stepRes.Rows {
			for i := range r {
				if !r[i].Null && r[i].Fam != desc.Columns[i].Type {
					r[i] = conformTo(r[i], desc.Columns[i].Type)
				}
			}
			if !all {
				k := encodeGroupKey(r)
				if seen[k] {
					continue
				}
				seen[k] = true
			}
			next = append(next, r)
		}
		working = next
	}
	return &Result{Columns: seedRes.Columns, Rows: result, Tag: fmt.Sprintf("SELECT %d", len(result))}, nil
}

// stmtReferences reports whether a statement reads the relation name:
// as a table (base or joined), inside a derived table or a set-operation
// member, or in a subquery of its select list or WHERE.
func stmtReferences(stmt parser.Statement, name string) bool {
	var sel func(t *parser.Select) bool
	var expr func(e parser.Expr) bool
	var conds func(cs []parser.Comparison) bool
	expr = func(e parser.Expr) bool {
		if e.Sub != nil && sel(e.Sub) {
			return true
		}
		if e.Left != nil && expr(*e.Left) || e.Right != nil && expr(*e.Right) {
			return true
		}
		for _, a := range e.Args {
			if expr(a) {
				return true
			}
		}
		if e.Case != nil {
			if e.Case.Operand != nil && expr(*e.Case.Operand) {
				return true
			}
			for _, w := range e.Case.Whens {
				if w.Value != nil && expr(*w.Value) || conds(w.Cond) || expr(w.Result) {
					return true
				}
			}
			if e.Case.Else != nil && expr(*e.Case.Else) {
				return true
			}
		}
		return false
	}
	conds = func(cs []parser.Comparison) bool {
		for _, c := range cs {
			if c.Sub != nil && sel(c.Sub) || expr(c.Value) || c.Expr != nil && expr(*c.Expr) {
				return true
			}
			for _, v := range c.Values {
				if expr(v) {
					return true
				}
			}
			for _, d := range c.Or {
				if conds(d) {
					return true
				}
			}
		}
		return false
	}
	sel = func(t *parser.Select) bool {
		for m := t; m != nil; m = m.Union {
			if strings.EqualFold(m.Table, name) {
				return true
			}
			for _, j := range m.Joins {
				if strings.EqualFold(j.Table, name) || (j.Derived != nil && sel(j.Derived)) {
					return true
				}
			}
			if m.Derived != nil && sel(m.Derived) {
				return true
			}
			for _, se := range m.Exprs {
				if !se.Star && expr(se.Expr) {
					return true
				}
			}
			if conds(m.Where) {
				return true
			}
		}
		return false
	}
	switch t := stmt.(type) {
	case *parser.Select:
		return sel(t)
	case *parser.Insert:
		return strings.EqualFold(t.Table, name) || (t.Select != nil && sel(t.Select))
	case *parser.Update:
		return strings.EqualFold(t.Table, name) || conds(t.Where)
	case *parser.Delete:
		return strings.EqualFold(t.Table, name) || conds(t.Where)
	}
	return false
}

// bindJoinedDerived binds every JOIN (SELECT ...) AS d member of t as a
// relation named by its alias (rows when executing, columns only when
// describing), returning t with the members referring to the relations
// by name and a function restoring the shadowed bindings.
func (s *Session) bindJoinedDerived(ctx context.Context, txn *kvclient.Txn, t *parser.Select, params []types.Datum, describeOnly bool) (*parser.Select, func(), error) {
	saved := map[string]*relation{}
	restore := func() { s.restoreRelations(saved) }
	var out *parser.Select
	for i := range t.Joins {
		jc := t.Joins[i]
		if jc.Derived == nil {
			continue
		}
		if out == nil {
			c := *t
			c.Joins = append([]parser.JoinClause(nil), t.Joins...)
			out = &c
		}
		name := strings.ToLower(jc.Alias)
		var cols []ResultColumn
		var rows [][]types.Datum
		if describeOnly {
			c, serr := s.PlanColumns(ctx, jc.Derived)
			if serr != nil {
				restore()
				return nil, nil, serr
			}
			cols = c
		} else {
			res, err := s.execSubSelect(ctx, txn, jc.Derived, params)
			if err != nil {
				restore()
				return nil, nil, err
			}
			cols, rows = res.Columns, res.Rows
		}
		desc, err := relationDesc(name, cols, nil)
		if err != nil {
			restore()
			return nil, nil, err
		}
		if prev, had := s.bindRelation(name, &relation{desc: desc, rows: rows}); had {
			if _, seen := saved[name]; !seen {
				saved[name] = prev
			}
		} else if _, seen := saved[name]; !seen {
			saved[name] = nil
		}
		out.Joins[i].Derived, out.Joins[i].Table = nil, name
	}
	if out == nil {
		return t, restore, nil
	}
	return out, restore, nil
}

// hasDerivedJoin reports whether any join member is a subquery.
func hasDerivedJoin(t *parser.Select) bool {
	for _, jc := range t.Joins {
		if jc.Derived != nil {
			return true
		}
	}
	return false
}
