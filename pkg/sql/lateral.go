package sql

import (
	"context"
	"fmt"
	"strings"

	"github.com/sthorne/datax/pkg/kvclient"
	"github.com/sthorne/datax/pkg/sql/catalog"
	"github.com/sthorne/datax/pkg/sql/parser"
	"github.com/sthorne/datax/pkg/sql/types"
)

// LATERAL members of a FROM list: a subquery written LATERAL (SELECT
// ...), or a table function call (implicitly lateral, as in PostgreSQL),
// that may read the columns of the members to its left.
//
// bindJoinedDerived binds such a member as a relation named by its alias
// that has columns and no rows, and remembers the query it runs. The
// join then sees an ordinary side: projection, WHERE, ORDER BY,
// grouping and Describe resolve against its columns. Execution is a
// correlated nested loop: at the member's join level, every partial row
// of the sides to its left substitutes its values into the member's
// query (the correlated-subquery machinery of correlated.go: analyzeSub
// finds the references, substituteSub splices the values in as
// literals) and runs it; the rows it returns are that partial row's
// candidates, which the ON clause then filters, a miss NULL-extending
// under LEFT JOIN. Runs are memoized on the referenced values, so
// partial rows that agree on them run the query once. A lateral member
// nested inside a lateral subquery composes: the outer substitution
// reaches into it, and it runs per row when the substituted subquery
// does.

// isLateralMember reports whether a join member runs per row of the
// members to its left: a table function call reading a column, or a
// member written LATERAL. A LATERAL subquery outside the shapes the
// correlation analysis covers (a derived table or a set operation of
// its own) runs once, as a plain derived table.
func isLateralMember(jc parser.JoinClause) bool {
	if jc.FuncTable != nil {
		return jc.Lateral || isLateralFunc(*jc.FuncTable)
	}
	return jc.Lateral && jc.Derived != nil && innerSubShape(jc.Derived)
}

// hasLateral reports whether any join member of t is lateral.
func hasLateral(t *parser.Select) bool {
	for _, jc := range t.Joins {
		if isLateralMember(jc) {
			return true
		}
	}
	return false
}

// lateralSub is the query a lateral member runs per row: the subquery
// itself, or SELECT * over the table function call.
func lateralSub(jc parser.JoinClause) *parser.Select {
	if jc.Derived != nil {
		return jc.Derived
	}
	return &parser.Select{Exprs: []parser.SelectExpr{{Star: true}}, FuncTable: jc.FuncTable, Alias: jc.Alias, FuncCols: jc.FuncCols, Limit: -1}
}

// lookupMaybePlan resolves a relation name in txn, or, without one (a
// statement being described), as planning does.
func (s *Session) lookupMaybePlan(ctx context.Context, txn *kvclient.Txn, name string) (*catalog.TableDescriptor, error) {
	if txn == nil {
		return s.lookupForPlan(ctx, name)
	}
	return s.lookup(ctx, txn, name)
}

// lateralScope is the scope a lateral member at join index i reads: the
// merged sides of the FROM members to its left (already bound, so each
// is a table or a relation by name).
func (s *Session) lateralScope(ctx context.Context, txn *kvclient.Txn, t *parser.Select, i int) (scopeLevel, error) {
	base, err := s.lookupMaybePlan(ctx, txn, t.Table)
	if err != nil {
		return scopeLevel{}, err
	}
	inner := make([]*catalog.TableDescriptor, i)
	for j := 0; j < i; j++ {
		if inner[j], err = s.lookupMaybePlan(ctx, txn, t.Joins[j].Table); err != nil {
			return scopeLevel{}, err
		}
	}
	left := *t
	left.Joins = t.Joins[:i]
	sides, err := makeJoinSides(base, inner, &left)
	if err != nil {
		return scopeLevel{}, err
	}
	scope, _ := joinScope(sides)
	return scope, nil
}

// typedNullRow is a row of scope desc whose every column is a NULL of
// the column's type, substituted to describe a lateral query.
func typedNullRow(desc *catalog.TableDescriptor) map[catalog.ColumnID]types.Datum {
	row := make(map[catalog.ColumnID]types.Datum, len(desc.Columns))
	for _, c := range desc.Columns {
		row[c.ID] = types.Datum{Fam: c.Type, Null: true}
	}
	return row
}

// describeLateral returns the columns a lateral query produces, reading
// the scopes outers (nearest first): each scope, outermost first, is
// substituted by a row of typed NULLs, and the closed query is
// described.
func (s *Session) describeLateral(ctx context.Context, txn *kvclient.Txn, sub *parser.Select, outers []scopeLevel) ([]ResultColumn, error) {
	cur := sub
	for lv := outers; len(lv) > 0; lv = lv[:len(lv)-1] {
		_, node, err := s.analyzeSub(ctx, txn, cur, lv)
		if err != nil {
			return nil, err
		}
		if cur, err = substituteSub(cur, node, typedNullRow(lv[len(lv)-1].desc), nil); err != nil {
			return nil, err
		}
	}
	cols, serr := s.PlanColumns(ctx, cur)
	if serr != nil {
		return nil, serr
	}
	return cols, nil
}

// lateralErr checks that a lateral member may be joined the way it is:
// PostgreSQL refuses a RIGHT or FULL join of one (its rows depend on
// the rows to its left, which such a join would have to produce
// unmatched).
func lateralErr(jc parser.JoinClause) error {
	if jc.Right || jc.Full {
		return newErrf(CodeInvalidColumnReference, "invalid reference to FROM-clause entry for table %q: the combining JOIN type must be INNER or LEFT for a LATERAL reference", jc.Alias)
	}
	return nil
}

// lateralAt returns the query of the lateral relation desc names, if it
// is one.
func (s *Session) lateralAt(desc *catalog.TableDescriptor) *parser.Select {
	if !strings.HasPrefix(desc.Virtual, relationPrefix) {
		return nil
	}
	if r, ok := s.rels[strings.TrimPrefix(desc.Virtual, relationPrefix)]; ok && r.desc == desc {
		return r.lateral
	}
	return nil
}

// anyLateralSide reports whether a join side is a lateral relation.
func (s *Session) anyLateralSide(sides []joinSide) bool {
	for _, js := range sides {
		if s.lateralAt(js.desc) != nil {
			return true
		}
	}
	return false
}

// joinLateral runs join level k when side k is a lateral relation: for
// every partial row, the lateral query with the row's values
// substituted (memoized on them), its rows filtered by the level's ON
// equalities and conditions; a partial row with no match NULL-extends
// under LEFT JOIN and drops otherwise.
func (s *Session) joinLateral(ctx context.Context, txn *kvclient.Txn, sides []joinSide, k int, sub *parser.Select, on []onPair, filter []parser.Comparison, joined []joinedRow, params []types.Datum) ([]joinedRow, error) {
	scope, rowOf := joinScope(sides[:k])
	refs, node, err := s.analyzeSub(ctx, txn, sub, []scopeLevel{scope})
	if err != nil {
		return nil, err
	}
	desc := sides[k].desc
	ord := desc.PrimaryKey[0]
	memo := map[string][]map[catalog.ColumnID]types.Datum{}
	runs := 0
	var next []joinedRow
	for _, jr := range joined {
		row := rowOf(jr)
		var kb strings.Builder
		for _, name := range refs {
			d := node.scope.bindDatum(name, row)
			fmt.Fprintf(&kb, "|%t,%d,%d,%g,%q,%t,%v", d.Null, d.Fam, d.I, d.F, d.S, d.B, d.A)
		}
		key := kb.String()
		cands, ok := memo[key]
		if !ok {
			clone, err := substituteSub(sub, node, row, params)
			if err != nil {
				return nil, err
			}
			res, err := s.execSubSelect(ctx, txn, clone, params)
			if err != nil {
				return nil, err
			}
			runs++
			cands = make([]map[catalog.ColumnID]types.Datum, 0, len(res.Rows))
			for i, vals := range res.Rows {
				if len(vals) != len(desc.Columns)-1 {
					return nil, newErrf(CodeInternal, "lateral %s returned %d columns, described %d", sides[k].alias, len(vals), len(desc.Columns)-1)
				}
				r := make(map[catalog.ColumnID]types.Datum, len(vals)+1)
				for j, d := range vals {
					c := desc.Columns[j]
					if !d.Null && d.Fam != c.Type {
						d = conformTo(d, c.Type)
					}
					r[c.ID] = d
				}
				r[ord] = types.NewInt(int64(i))
				if err := s.chargeRow(r); err != nil {
					return nil, err
				}
				cands = append(cands, r)
			}
			memo[key] = cands
		}
		matched := 0
		for _, r := range cands {
			if !onPairsMatch(jr, on, r) {
				continue
			}
			ext := jr.extend(r)
			if len(filter) > 0 {
				ok, err := evalJoinWhere(sides[:k+1], filter, ext, params)
				if err != nil {
					return nil, err
				}
				if !ok {
					continue
				}
			}
			next = append(next, ext)
			matched++
		}
		if matched == 0 && sides[k].left {
			next = append(next, jr.extend(nil))
		}
	}
	s.note("lateral %s: %d run(s) for %d outer row(s)", sides[k].alias, runs, len(joined))
	return next, nil
}

// onPairsMatch reports whether a candidate row of the joined side
// satisfies the level's ON equalities against the partial row (SQL
// equality: NULL matches nothing).
func onPairsMatch(jr joinedRow, on []onPair, r map[catalog.ColumnID]types.Datum) bool {
	for _, p := range on {
		l := jr.datum(joinRef{side: p.leftSide, col: p.leftCol})
		rv, ok := r[p.rightCol.ID]
		if !ok || l.Null || rv.Null {
			return false
		}
		if match, err := compareDatums(l, "=", rv); err != nil || !match {
			return false
		}
	}
	return true
}

// fromRefErr is the error for a table function call heading the FROM
// list that reads a column: nothing is to its left, so the reference is
// to a member further right or to nothing (42P01 for a qualified name,
// 42703 for a bare one, as in PostgreSQL).
func fromRefErr(e parser.Expr) error {
	var err error
	exprColumns(e, func(name string) {
		if err != nil {
			return
		}
		if q, _ := splitQualified(name); q != "" {
			err = newErrf(CodeUndefinedTable, "invalid reference to FROM-clause entry for table %q", q)
			return
		}
		err = newErrf(CodeUndefinedColumn, "column %q does not exist", name)
	})
	return err
}

// fromSubErr refines the error a FROM-list subquery failed with: a
// reference to a table it cannot see (a member to its right, or one
// that is not there) is 42P01, as in PostgreSQL, rather than the
// generic column error.
func (s *Session) fromSubErr(ctx context.Context, txn *kvclient.Txn, sub *parser.Select, err error) error {
	serr, ok := err.(*Error)
	if !ok || serr.Code != CodeFeatureNotSupported && serr.Code != CodeUndefinedColumn || !innerSubShape(sub) {
		return err
	}
	if _, _, aerr := s.analyzeSub(ctx, txn, sub, nil); aerr != nil {
		if ae, ok := aerr.(*Error); ok && ae.Code == CodeUndefinedTable {
			return aerr
		}
	}
	return err
}
