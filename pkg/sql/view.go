package sql

import (
	"context"
	"fmt"
	"strings"

	"github.com/sthorne/datax/pkg/kvclient"
	"github.com/sthorne/datax/pkg/sql/catalog"
	"github.com/sthorne/datax/pkg/sql/parser"
	"github.com/sthorne/datax/pkg/sql/types"
	"github.com/sthorne/datax/pkg/util/log"
	"github.com/sthorne/datax/pkg/version"
)

// Views (issue #95, cluster version v9). A view is a table descriptor
// that carries its query's SQL text and no rows. A statement that names
// a view has the view bound as a statement-scoped relation before it
// runs — the query becomes an implicit leading WITH member (pkg/sql/cte.go),
// so the view reads anywhere a table does and a view over a view expands
// as the member executes. The catalogs list views (pg_class relkind 'v',
// pg_views, information_schema.views); DML on a view is refused; a
// table or view another view depends on cannot be dropped, renamed or
// have a column dropped while the view exists.

// requireV9 gates the DDL that writes v9 descriptor fields (views).
func (s *Session) requireV9(what string) error {
	if s.db.ClusterVersion() < version.V9 {
		return newErrf(CodeFeatureNotSupported, "%s need cluster version v9: finalize the upgrade with `datax debug upgrade` first", what)
	}
	return nil
}

// expandViews returns stmt with every view it references bound as a
// leading WITH member (a copy; stmt itself is unchanged). Names an
// explicit WITH member or an enclosing binding already covers are left
// alone; a DML statement targeting a view is refused. Without a
// transaction (Describe), descriptors resolve through the planning
// lookup.
func (s *Session) expandViews(ctx context.Context, txn *kvclient.Txn, stmt parser.Statement) (parser.Statement, error) {
	if ex, ok := stmt.(*parser.Explain); ok {
		inner, err := s.expandViews(ctx, txn, ex.Stmt)
		if err != nil {
			return nil, err
		}
		c := *ex
		c.Stmt = inner
		return &c, nil
	}
	target := ""
	switch t := stmt.(type) {
	case *parser.Select:
	case *parser.Insert:
		target = t.Table
	case *parser.Update:
		target = t.Table
	case *parser.Delete:
		target = t.Table
	default:
		return stmt, nil
	}
	resolve := func(name string) (*catalog.TableDescriptor, error) {
		if txn != nil {
			return s.lookup(ctx, txn, name)
		}
		return s.lookupForPlan(ctx, name)
	}
	if target != "" {
		if d, err := resolve(target); err == nil && d.IsView() {
			return nil, newErrf(CodeWrongObjectType, "cannot modify view %q: views are read-only", d.Name)
		}
	}
	shadowed := map[string]bool{}
	for _, cte := range stmtWith(stmt) {
		shadowed[strings.ToLower(cte.Name)] = true
	}
	var members []parser.CTE
	seen := map[string]bool{}
	for _, name := range stmtTables(stmt) {
		lname := strings.ToLower(name)
		db, bare := catalog.SplitTableName(lname)
		if seen[bare] || shadowed[bare] {
			continue
		}
		if r, bound := s.rels[bare]; bound && (db == "" || r.alias == lname) {
			continue // an enclosing binding (a view being expanded, a CTE)
		}
		d, err := resolve(name)
		if err != nil || d == nil || d.Virtual != "" || !d.IsView() {
			continue // the statement reports a missing table itself
		}
		if txn != nil {
			if err := s.checkTablePriv(ctx, txn, d, "SELECT"); err != nil {
				return nil, err
			}
		}
		stmts, perr := parser.Parse(d.ViewQuery)
		if perr != nil || len(stmts) != 1 {
			return nil, newErrf(CodeInternal, "view %q: stored query does not parse: %v", d.Name, perr)
		}
		sel, ok := stmts[0].(*parser.Select)
		if !ok {
			return nil, newErrf(CodeInternal, "view %q: stored query is not a SELECT", d.Name)
		}
		cols := make([]string, 0, len(d.Columns))
		for _, c := range d.Columns {
			cols = append(cols, c.Name)
		}
		m := parser.CTE{Name: bare, Columns: cols, Query: sel}
		if db != "" {
			m.Qualified = lname
		}
		members = append(members, m)
		seen[bare] = true
	}
	if len(members) == 0 {
		return stmt, nil
	}
	with := append(members, stmtWith(stmt)...)
	switch t := stmt.(type) {
	case *parser.Select:
		c := *t
		c.With = with
		return &c, nil
	case *parser.Insert:
		c := *t
		c.With = with
		return &c, nil
	case *parser.Update:
		c := *t
		c.With = with
		return &c, nil
	case *parser.Delete:
		c := *t
		c.With = with
		return &c, nil
	}
	return stmt, nil
}

// stmtTables lists every table name a statement references, in first
// appearance order, without duplicates: the base and joined tables of
// every select, set-operation member, derived table, subquery and WITH
// member, and a DML statement's target.
func stmtTables(stmt parser.Statement) []string {
	var out []string
	seen := map[string]bool{}
	add := func(name string) {
		if name == "" || seen[strings.ToLower(name)] {
			return
		}
		seen[strings.ToLower(name)] = true
		out = append(out, name)
	}
	var sel func(t *parser.Select)
	var expr func(e parser.Expr)
	var conds func(cs []parser.Comparison)
	var exprs func(es []parser.SelectExpr)
	var stmtf func(st parser.Statement)
	expr = func(e parser.Expr) {
		if e.Sub != nil {
			sel(e.Sub)
		}
		if e.Left != nil {
			expr(*e.Left)
		}
		if e.Right != nil {
			expr(*e.Right)
		}
		for _, a := range e.Args {
			expr(a)
		}
		if e.Case != nil {
			if e.Case.Operand != nil {
				expr(*e.Case.Operand)
			}
			for _, w := range e.Case.Whens {
				if w.Value != nil {
					expr(*w.Value)
				}
				conds(w.Cond)
				expr(w.Result)
			}
			if e.Case.Else != nil {
				expr(*e.Case.Else)
			}
		}
		if e.Window != nil {
			exprs([]parser.SelectExpr{*e.Window})
		}
	}
	conds = func(cs []parser.Comparison) {
		for _, c := range cs {
			if c.Sub != nil {
				sel(c.Sub)
			}
			expr(c.Value)
			if c.Expr != nil {
				expr(*c.Expr)
			}
			for _, v := range c.Values {
				expr(v)
			}
			for _, d := range c.Or {
				conds(d)
			}
		}
	}
	exprs = func(es []parser.SelectExpr) {
		for _, se := range es {
			if !se.Star {
				expr(se.Expr)
			}
		}
	}
	sel = func(t *parser.Select) {
		for m := t; m != nil; m = m.Union {
			for _, cte := range m.With {
				stmtf(cte.Query)
			}
			add(m.Table)
			if m.Derived != nil {
				sel(m.Derived)
			}
			if m.FuncTable != nil {
				expr(*m.FuncTable)
			}
			for _, j := range m.Joins {
				add(j.Table)
				if j.Derived != nil {
					sel(j.Derived)
				}
			}
			exprs(m.Exprs)
			conds(m.Where)
			for _, h := range m.Having {
				expr(h.Value)
			}
			for _, oc := range m.OrderBy {
				if oc.Expr != nil {
					expr(*oc.Expr)
				}
			}
		}
	}
	stmtf = func(st parser.Statement) {
		switch t := st.(type) {
		case *parser.Select:
			sel(t)
		case *parser.Insert:
			for _, cte := range t.With {
				stmtf(cte.Query)
			}
			add(t.Table)
			if t.Select != nil {
				sel(t.Select)
			}
			for _, row := range t.Rows {
				for _, e := range row {
					expr(e)
				}
			}
			exprs(t.Returning)
		case *parser.Update:
			for _, cte := range t.With {
				stmtf(cte.Query)
			}
			add(t.Table)
			for _, sc := range t.Set {
				expr(sc.Value)
			}
			conds(t.Where)
			exprs(t.Returning)
		case *parser.Delete:
			for _, cte := range t.With {
				stmtf(cte.Query)
			}
			add(t.Table)
			conds(t.Where)
			exprs(t.Returning)
		case *parser.Explain:
			stmtf(t.Stmt)
		}
	}
	stmtf(stmt)
	return out
}

// viewDependencies resolves the tables and views a view's query reads
// (every one must exist now; the catalogs count for nothing).
func (s *Session) viewDependencies(ctx context.Context, txn *kvclient.Txn, q *parser.Select) ([]uint64, error) {
	shadowed := map[string]bool{}
	for _, cte := range q.With {
		shadowed[strings.ToLower(cte.Name)] = true
	}
	var deps []uint64
	seen := map[uint64]bool{}
	for _, name := range stmtTables(q) {
		if _, bare := catalog.SplitTableName(strings.ToLower(name)); shadowed[bare] {
			continue
		}
		d, err := s.lookup(ctx, txn, name)
		if err != nil {
			var nf *catalog.ErrTableNotFound
			if asErr(err, &nf) {
				return nil, newErrf(CodeUndefinedTable, "relation %q does not exist", name)
			}
			return nil, err
		}
		if d.Virtual != "" || seen[d.ID] {
			continue
		}
		seen[d.ID] = true
		deps = append(deps, d.ID)
	}
	return deps, nil
}

// execCreateView is CREATE [OR REPLACE] VIEW name [(cols)] AS query.
func (s *Session) execCreateView(ctx context.Context, txn *kvclient.Txn, t *parser.CreateView) (*Result, error) {
	if err := s.requireV9("views"); err != nil {
		return nil, err
	}
	dbName, bare := catalog.SplitTableName(t.Name)
	if dbName == "" {
		dbName = s.database
	}
	if catalog.IsSystemTable(bare) || vtableName(bare) {
		return nil, newErrf(CodeInsufficientPriv, "relation name %q is reserved", bare)
	}
	if dbName == catalog.SystemDatabase && !s.system {
		return nil, newErrf(CodeInsufficientPriv, "database %q is reserved for the cluster", dbName)
	}
	if parser.CountParams(t.Query) > 0 {
		return nil, newErrf(CodeSyntaxError, "a view's query cannot use parameters")
	}
	if t.Query.AsOf != "" || t.Query.AsOfMaxStaleness != "" || t.Query.ForUpdate {
		return nil, newErrf(CodeFeatureNotSupported, "a view's query cannot use AS OF SYSTEM TIME or FOR UPDATE")
	}
	deps, err := s.viewDependencies(ctx, txn, t.Query)
	if err != nil {
		return nil, err
	}
	expanded, err := s.expandViews(ctx, txn, t.Query)
	if err != nil {
		return nil, err
	}
	cols, serr := s.PlanColumns(ctx, expanded)
	if serr != nil {
		return nil, serr
	}
	if len(cols) == 0 {
		return nil, newErrf(CodeSyntaxError, "a view's query must return columns")
	}
	if len(t.Columns) > 0 && len(t.Columns) != len(cols) {
		return nil, newErrf(CodeSyntaxError, "view %q has %d columns available but %d column names specified", bare, len(cols), len(t.Columns))
	}
	columns := make([]catalog.Column, 0, len(cols))
	names := map[string]bool{}
	for i, rc := range cols {
		name := strings.ToLower(rc.Name)
		if i < len(t.Columns) {
			name = strings.ToLower(t.Columns[i])
		}
		if names[name] {
			return nil, newErrf(CodeDuplicateObject, "column %q specified more than once", name)
		}
		names[name] = true
		typ := rc.Type
		if typ == types.Unknown {
			typ = types.String
		}
		columns = append(columns, catalog.Column{ID: catalog.ColumnID(i + 1), Name: name, Type: typ})
	}
	dbID, err := s.cat.DatabaseID(ctx, txn, dbName)
	if err != nil {
		return nil, ToSQLError(err)
	}
	if t.OrReplace {
		existing, err := s.cat.LookupIn(ctx, txn, dbName, bare)
		var nf *catalog.ErrTableNotFound
		if err != nil && !asErr(err, &nf) {
			return nil, err
		}
		if existing != nil {
			if !existing.IsView() {
				return nil, newErrf(CodeWrongObjectType, "%q is not a view", bare)
			}
			if cyc, err := s.viewDependsOn(ctx, txn, deps, existing.ID); err != nil {
				return nil, err
			} else if cyc {
				return nil, newErrf(CodeInvalidObjectDefinition, "view %q would depend on itself", bare)
			}
			nd := existing.Clone()
			nd.Columns, nd.NextColumnID = columns, catalog.ColumnID(len(columns)+1)
			nd.ViewQuery, nd.ViewDepends = t.Text, deps
			if err := s.cat.Update(ctx, txn, nd); err != nil {
				return nil, err
			}
			log.Audit("view-ddl", "stmt", "CREATE OR REPLACE VIEW", "target", bare, "principal", s.user)
			return &Result{Tag: "CREATE VIEW"}, nil
		}
	}
	desc := &catalog.TableDescriptor{
		Name: bare, DatabaseID: dbID, Columns: columns, NextColumnID: catalog.ColumnID(len(columns) + 1),
		ViewQuery: t.Text, ViewDepends: deps,
	}
	if err := s.cat.Create(ctx, txn, desc); err != nil {
		return nil, err
	}
	log.Audit("view-ddl", "stmt", "CREATE VIEW", "target", bare, "principal", s.user)
	return &Result{Tag: "CREATE VIEW"}, nil
}

// viewDependsOn reports whether following deps through views reaches
// target.
func (s *Session) viewDependsOn(ctx context.Context, txn *kvclient.Txn, deps []uint64, target uint64) (bool, error) {
	seen := map[uint64]bool{}
	queue := append([]uint64(nil), deps...)
	for len(queue) > 0 {
		id := queue[0]
		queue = queue[1:]
		if id == target {
			return true, nil
		}
		if seen[id] {
			continue
		}
		seen[id] = true
		d, err := catalog.ReadTable(ctx, txn, id)
		if err != nil {
			return false, err
		}
		if d != nil && d.IsView() {
			queue = append(queue, d.ViewDepends...)
		}
	}
	return false, nil
}

// viewsDependingOn lists the views whose query reads relation id.
func (s *Session) viewsDependingOn(ctx context.Context, txn *kvclient.Txn, id uint64) ([]*catalog.TableDescriptor, error) {
	all, err := s.cat.List(ctx, txn)
	if err != nil {
		return nil, err
	}
	var out []*catalog.TableDescriptor
	for _, d := range all {
		if !d.IsView() {
			continue
		}
		for _, dep := range d.ViewDepends {
			if dep == id {
				out = append(out, d)
				break
			}
		}
	}
	return out, nil
}

// dropDependentViews handles the views that depend on relation name
// (id) when it is dropped: refused (2BP01) unless cascade, which drops
// them and, transitively, the views depending on those.
func (s *Session) dropDependentViews(ctx context.Context, txn *kvclient.Txn, id uint64, name string, cascade bool, hint string) error {
	deps, err := s.viewsDependingOn(ctx, txn, id)
	if err != nil {
		return err
	}
	if len(deps) == 0 {
		return nil
	}
	if !cascade {
		return newErrf(CodeDependentObjectsExist, "cannot drop %s because view %q depends on it (use %s CASCADE)", name, deps[0].Name, hint)
	}
	dbs, err := s.databaseNames(ctx, txn)
	if err != nil {
		return err
	}
	dropped := map[uint64]bool{}
	for len(deps) > 0 {
		v := deps[0]
		deps = deps[1:]
		if dropped[v.ID] {
			continue
		}
		more, err := s.viewsDependingOn(ctx, txn, v.ID)
		if err != nil {
			return err
		}
		deps = append(deps, more...)
		if _, err := s.cat.DropIn(ctx, txn, dbs[v.DatabaseID], v.Name); err != nil {
			return err
		}
		dropped[v.ID] = true
		s.noteDDL(v.Name)
		log.Audit("view-ddl", "stmt", "DROP VIEW (cascade)", "target", v.Name, "principal", s.user)
	}
	return nil
}

// databaseNames maps database IDs to names (0 = the default database).
func (s *Session) databaseNames(ctx context.Context, txn *kvclient.Txn) (map[uint64]string, error) {
	dbs, err := catalog.ListDatabases(ctx, txn)
	if err != nil {
		return nil, err
	}
	out := map[uint64]string{0: catalog.DefaultDatabase}
	for _, d := range dbs {
		out[d.ID] = d.Name
	}
	return out, nil
}

// execDropView is DROP VIEW [IF EXISTS] name [, ...] [CASCADE].
func (s *Session) execDropView(ctx context.Context, txn *kvclient.Txn, t *parser.DropView) (*Result, error) {
	dbs, err := s.databaseNames(ctx, txn)
	if err != nil {
		return nil, err
	}
	var targets []*catalog.TableDescriptor
	in := map[uint64]bool{}
	for _, name := range t.Names {
		d, err := s.lookup(ctx, txn, name)
		if err != nil {
			var nf *catalog.ErrTableNotFound
			if t.IfExists && asErr(err, &nf) {
				continue
			}
			return nil, err
		}
		if d.Virtual != "" || !d.IsView() {
			return nil, newErrf(CodeWrongObjectType, "%q is not a view (use DROP TABLE)", name)
		}
		if in[d.ID] {
			continue
		}
		in[d.ID] = true
		targets = append(targets, d)
	}
	for _, v := range targets {
		deps, err := s.viewsDependingOn(ctx, txn, v.ID)
		if err != nil {
			return nil, err
		}
		for _, d := range deps {
			if !in[d.ID] && !t.Cascade {
				return nil, newErrf(CodeDependentObjectsExist, "cannot drop view %q because view %q depends on it (use DROP VIEW ... CASCADE)", v.Name, d.Name)
			}
		}
	}
	for _, v := range targets {
		if err := s.dropDependentViews(ctx, txn, v.ID, "view "+v.Name, true, "DROP VIEW ..."); err != nil {
			return nil, err
		}
		if _, err := s.cat.DropIn(ctx, txn, dbs[v.DatabaseID], v.Name); err != nil {
			var nf *catalog.ErrTableNotFound
			if asErr(err, &nf) {
				continue // dropped along with a target it depended on
			}
			return nil, err
		}
		s.noteDDL(v.Name)
		log.Audit("view-ddl", "stmt", "DROP VIEW", "target", v.Name, "principal", s.user)
	}
	return &Result{Tag: "DROP VIEW"}, nil
}

// refuseIfViewed refuses a change to a relation that a view depends on
// (renames and column drops would break the stored query).
func (s *Session) refuseIfViewed(ctx context.Context, txn *kvclient.Txn, desc *catalog.TableDescriptor, what string) error {
	deps, err := s.viewsDependingOn(ctx, txn, desc.ID)
	if err != nil {
		return err
	}
	if len(deps) > 0 {
		return newErrf(CodeDependentObjectsExist, "cannot %s: view %q depends on table %q (drop or replace the view first)", what, deps[0].Name, desc.Name)
	}
	return nil
}

// CreateViewDef renders the statement that recreates a view.
func CreateViewDef(d *catalog.TableDescriptor) string {
	names := make([]string, 0, len(d.Columns))
	for _, c := range d.Columns {
		names = append(names, c.Name)
	}
	return fmt.Sprintf("CREATE VIEW %s (%s) AS %s", d.Name, strings.Join(names, ", "), d.ViewQuery)
}
