package sql

import (
	"context"
	"strings"

	"github.com/sthorne/datax/pkg/kvclient"
	"github.com/sthorne/datax/pkg/sql/builtins"
	"github.com/sthorne/datax/pkg/sql/catalog"
	"github.com/sthorne/datax/pkg/sql/parser"
	"github.com/sthorne/datax/pkg/sql/types"
)

// Sequence bindings of column defaults (gap PG-D-035).
//
// PostgreSQL stores DEFAULT nextval('x') with the regclass resolved:
// the default draws from the sequence 'x' named when the default was
// set, whatever the inserting session's search path, and whatever is
// created under the name later. datax stores a default as text, so a
// column records, beside it, the sequence each name resolved to
// (Column.SeqBindings), and the default is evaluated with those
// sequences, by ID. A default set before bindings existed resolves its
// names through public alone: sequences could live nowhere else then.

// seqArgFuncs are the functions whose first argument names a sequence.
var seqArgFuncs = map[string]bool{"nextval": true, "currval": true, "setval": true}

// seqNameArg reports whether a is a literal sequence name: a string,
// or a string cast to regclass; name is its text.
func seqNameArg(a parser.Expr) (name string, ok bool) {
	if a.Lit == nil || a.Lit.Null || a.Lit.Fam != types.String || a.BinOp != "" || a.Left != nil || a.Right != nil || a.Func != "" {
		return "", false
	}
	if a.Cast != "" && !strings.EqualFold(a.Cast, "regclass") && !strings.EqualFold(a.Cast, "pg_catalog.regclass") {
		return "", false
	}
	return a.Lit.Text(), true
}

// mapSeqArgs returns a copy of e in which f has seen, and may have
// replaced, the literal sequence argument of every nextval, currval
// and setval call.
func mapSeqArgs(e parser.Expr, f func(arg parser.Expr, name string) (parser.Expr, bool)) parser.Expr {
	out := e
	if e.Left != nil {
		l := mapSeqArgs(*e.Left, f)
		out.Left = &l
	}
	if e.Right != nil {
		r := mapSeqArgs(*e.Right, f)
		out.Right = &r
	}
	if len(e.Args) > 0 {
		out.Args = make([]parser.Expr, len(e.Args))
		for i, a := range e.Args {
			if i == 0 && seqArgFuncs[e.Func] {
				if name, ok := seqNameArg(a); ok {
					if r, ok := f(a, name); ok {
						out.Args[i] = r
						continue
					}
					out.Args[i] = a
					continue
				}
			}
			out.Args[i] = mapSeqArgs(a, f)
		}
	}
	return out
}

// seqArgNames lists the literal sequence names e's calls name.
func seqArgNames(e parser.Expr) []string {
	var out []string
	mapSeqArgs(e, func(_ parser.Expr, name string) (parser.Expr, bool) {
		out = append(out, name)
		return parser.Expr{}, false
	})
	return out
}

// bindDefaultSequences resolves, for a default being set, the sequence
// each literal name of its text names (through the session's search
// path, as the default is written): the column's SeqBindings. A name
// that names no sequence is refused (42P01), as PostgreSQL refuses it
// when it casts the name to regclass: left to resolve when used, it
// would follow the inserting session's path. nil when the default
// names no sequence.
func (s *Session) bindDefaultSequences(ctx context.Context, txn *kvclient.Txn, text string) (map[string]uint64, error) {
	if text == "" || text == "NULL" {
		return nil, nil
	}
	e, err := parser.ParseExpr(text)
	if err != nil {
		return nil, nil
	}
	var out map[string]uint64
	for _, name := range seqArgNames(e) {
		if out == nil {
			out = map[string]uint64{}
		}
		if _, done := out[name]; done {
			continue
		}
		d, err := s.lookupSequence(ctx, txn, name)
		if err != nil {
			return nil, err
		}
		out[name] = d.ID
	}
	return out, nil
}

// setDefaultExpr sets col's expression default and binds its sequence
// names.
func (s *Session) setDefaultExpr(ctx context.Context, txn *kvclient.Txn, col *catalog.Column, text string) error {
	b, err := s.bindDefaultSequences(ctx, txn, text)
	if err != nil {
		return err
	}
	col.DefaultExpr, col.SeqBindings = text, b
	return nil
}

// legacySeqName is a name a default set before bindings existed wrote,
// qualified so that it resolves as it did then, through public alone:
// "x" is public.x; "q.x" (q could only be a database then) is
// q.public.x.
func legacySeqName(name string) string {
	var parts []string
	if strings.Contains(name, `"`) {
		p, ok := parseQualifiedName(name)
		if !ok {
			return name
		}
		parts = p
	} else {
		parts = catalog.SplitName(name)
	}
	q := make([]string, len(parts))
	for i, p := range parts {
		q[i] = quoteIdentifier(p)
	}
	switch len(parts) {
	case 1:
		return catalog.PublicSchema + "." + q[0]
	case 2:
		if parts[0] == catalog.PublicSchema {
			return name
		}
		return q[0] + "." + catalog.PublicSchema + "." + q[1]
	}
	return name
}

// unboundSeqName is how a name a default does not bind to a sequence
// (a binding of 0, which a table restore leaves for a sequence it does
// not bring back) resolves: never through the inserting session's
// search path, so an unqualified name is public's; a qualified one
// resolves as written.
func unboundSeqName(name string) string {
	var parts []string
	if strings.Contains(name, `"`) {
		p, ok := parseQualifiedName(name)
		if !ok {
			return name
		}
		parts = p
	} else {
		parts = catalog.SplitName(name)
	}
	if len(parts) == 1 {
		return catalog.PublicSchema + "." + quoteIdentifier(parts[0])
	}
	return name
}

// fixedSeqName is the name a default's sequence name resolves by when
// it is not bound: public-qualified for a default from before bindings
// (legacySeqName), or for a binding of 0 (unboundSeqName).
func fixedSeqName(c *catalog.Column, name string) string {
	if c.SeqBindings == nil {
		return legacySeqName(name)
	}
	return unboundSeqName(name)
}

// boundDefaultExpr is a column's parsed default with its sequence
// names replaced by what they are bound to: the sequence's OID for a
// bound name (nextval reads it as a regclass, and asks no USAGE on its
// schema, as PostgreSQL does not for a bound regclass), else the name
// as fixedSeqName qualifies it — never one the inserting session's
// search path resolves.
func boundDefaultExpr(c *catalog.Column, e parser.Expr) parser.Expr {
	return mapSeqArgs(e, func(_ parser.Expr, name string) (parser.Expr, bool) {
		if id := c.SeqBindings[name]; id != 0 {
			d := types.NewInt(int64(id))
			return parser.Expr{Lit: &d}, true
		}
		d := types.NewString(fixedSeqName(c, name))
		return parser.Expr{Lit: &d}, true
	})
}

// currentSeqBinding is the sequence a default's name resolves to now,
// as the default is evaluated (0 for none).
func (s *Session) currentSeqBinding(ctx context.Context, txn *kvclient.Txn, c *catalog.Column, name string) (uint64, error) {
	if id := c.SeqBindings[name]; id != 0 {
		return id, nil
	}
	d, err := s.lookupSequence(ctx, txn, fixedSeqName(c, name))
	if err != nil {
		if serr, ok := err.(*Error); ok && serr.Code == CodeUndefinedTable {
			return 0, nil
		}
		return 0, err
	}
	return d.ID, nil
}

// seqArgText is how a default names sequence d: schema-qualified in a
// schema other than public; in public by its name when the session's
// search path resolves that to d, else public-qualified.
func (s *Session) seqArgText(ctx context.Context, txn *kvclient.Txn, d *catalog.SequenceDescriptor) (string, error) {
	bare := quoteIdentifier(d.Name)
	schema := catalog.PublicSchema
	if d.SchemaID != 0 {
		sd, err := catalog.ReadSchema(ctx, txn, d.SchemaID)
		if err != nil {
			return "", err
		}
		if sd == nil {
			return "", newErrf(CodeInvalidSchemaName, "schema of sequence %q does not exist", d.Name)
		}
		schema = sd.Name
	} else if got, err := s.lookupSequence(ctx, txn, bare); err == nil && got.ID == d.ID {
		return bare, nil
	}
	return quoteIdentifier(schema) + "." + bare, nil
}

// seqDefaultRef is one column default that names a sequence a DDL
// statement moves or drops.
type seqDefaultRef struct {
	table *catalog.TableDescriptor
	col   catalog.ColumnID
	expr  parser.Expr
	ids   map[string]uint64 // each name → the sequence it resolves to
	// legacy: the default predates bindings (its names resolve through
	// public).
	legacy bool
}

// seqDefaultRefs lists the column defaults of database db's tables
// that resolve a name to a sequence hit reports.
func (s *Session) seqDefaultRefs(ctx context.Context, txn *kvclient.Txn, dbID uint64, hit func(seqID uint64) bool) ([]seqDefaultRef, error) {
	db, err := catalog.ReadDatabase(ctx, txn, dbID)
	if err != nil || db == nil {
		return nil, err
	}
	tables, err := s.cat.ListIn(ctx, txn, db)
	if err != nil {
		return nil, err
	}
	var out []seqDefaultRef
	for _, t := range tables {
		// Every session's temporary tables count: a default bound to
		// the sequence is one whoever's table it is.
		if t.IsView() || t.Virtual != "" {
			continue
		}
		for i := range t.Columns {
			c := &t.Columns[i]
			if c.DefaultExpr == "" || c.DefaultExpr == "NULL" || !strings.Contains(c.DefaultExpr, "(") {
				continue
			}
			e, err := parser.ParseExpr(c.DefaultExpr)
			if err != nil {
				continue
			}
			names := seqArgNames(e)
			if len(names) == 0 {
				continue
			}
			ids := map[string]uint64{}
			found := false
			for _, n := range names {
				id, err := s.currentSeqBinding(ctx, txn, c, n)
				if err != nil {
					return nil, err
				}
				ids[n] = id
				found = found || (id != 0 && hit(id))
			}
			if found {
				out = append(out, seqDefaultRef{table: t, col: c.ID, expr: e, ids: ids, legacy: c.SeqBindings == nil})
			}
		}
	}
	return out, nil
}

// rewriteSeqDefaults rewrites the defaults refs lists (found before a
// sequence moved) to name each sequence as it is now named, bound by
// ID, and drains the tables.
func (s *Session) rewriteSeqDefaults(ctx context.Context, txn *kvclient.Txn, refs []seqDefaultRef) error {
	byTable := map[uint64]*catalog.TableDescriptor{}
	var order []uint64
	for _, r := range refs {
		if r.table.Temp != nil && !s.tempVisible(r.table) {
			// Another session's: its binding, by ID, holds; only the
			// text would be stale, which that session alone shows.
			continue
		}
		desc := byTable[r.table.ID]
		if desc == nil {
			cur, err := catalog.ReadTable(ctx, txn, r.table.ID)
			if err != nil {
				return err
			}
			if cur == nil {
				continue
			}
			desc = cur.Clone()
			byTable[desc.ID] = desc
			order = append(order, desc.ID)
		}
		bindings := map[string]uint64{}
		var ferr error
		e := mapSeqArgs(r.expr, func(arg parser.Expr, name string) (parser.Expr, bool) {
			id := r.ids[name]
			if id == 0 {
				// Resolving to nothing now: still resolved as it was
				// (fixedSeqName), never through a session's path.
				name = unboundSeqName(fixedName(r, name))
				d := types.NewString(name)
				arg.Lit = &d
				bindings[name] = 0
				return arg, true
			}
			sd, err := catalog.ReadSequence(ctx, txn, id)
			if err != nil {
				ferr = err
				return parser.Expr{}, false
			}
			text, err := s.seqArgText(ctx, txn, sd)
			if err != nil {
				ferr = err
				return parser.Expr{}, false
			}
			bindings[text] = id
			d := types.NewString(text)
			arg.Lit = &d
			return arg, true
		})
		if ferr != nil {
			return ferr
		}
		for i := range desc.Columns {
			if desc.Columns[i].ID == r.col {
				desc.Columns[i].DefaultExpr = parser.FormatExpr(e)
				desc.Columns[i].SeqBindings = bindings
			}
		}
	}
	for _, id := range order {
		desc := byTable[id]
		if err := s.cat.Update(ctx, txn, desc); err != nil {
			return err
		}
		s.noteDDL(catalog.RelationName(desc))
	}
	return nil
}

// serialDefault is the default of a column that owns sequence d
// (SERIAL, identity): nextval of it, named as seqArgText names it, and
// the binding.
func (s *Session) serialDefault(ctx context.Context, txn *kvclient.Txn, d *catalog.SequenceDescriptor) (string, map[string]uint64, error) {
	name := quoteIdentifier(d.Name)
	if d.SchemaID != 0 {
		var err error
		if name, err = s.seqArgText(ctx, txn, d); err != nil {
			return "", nil, err
		}
	}
	return "nextval('" + strings.ReplaceAll(name, "'", "''") + "')", map[string]uint64{name: d.ID}, nil
}

// pinnedDefaultExpr is column c's parsed default e with each sequence
// name rewritten to name, from the session's search path, the
// sequence it is bound to (CREATE TABLE ... LIKE ... INCLUDING
// DEFAULTS copies the default, and PostgreSQL its bound regclass).
func (s *Session) pinnedDefaultExpr(ctx context.Context, txn *kvclient.Txn, c *catalog.Column, e parser.Expr) (parser.Expr, error) {
	var ferr error
	out := mapSeqArgs(e, func(arg parser.Expr, name string) (parser.Expr, bool) {
		id, err := s.currentSeqBinding(ctx, txn, c, name)
		if err != nil {
			ferr = err
			return parser.Expr{}, false
		}
		if id == 0 {
			return parser.Expr{}, false
		}
		sd, err := catalog.ReadSequence(ctx, txn, id)
		if err != nil {
			ferr = err
			return parser.Expr{}, false
		}
		text, err := s.seqArgText(ctx, txn, sd)
		if err != nil {
			ferr = err
			return parser.Expr{}, false
		}
		d := types.NewString(text)
		arg.Lit = &d
		return arg, true
	})
	return out, ferr
}

// dropOwnedSequenceDefaults handles the column defaults of other tables
// bound to a sequence table d owns, which goes with d: RESTRICT refuses
// the drop (2BP01), CASCADE drops those defaults, as in PostgreSQL.
func (s *Session) dropOwnedSequenceDefaults(ctx context.Context, txn *kvclient.Txn, d *catalog.TableDescriptor, cascade bool) error {
	owned, err := s.ownedSequences(ctx, txn, d)
	if err != nil || len(owned) == 0 {
		return err
	}
	names := map[uint64]string{}
	for _, sq := range owned {
		names[sq.ID] = sq.Name
	}
	refs, err := s.seqDefaultRefs(ctx, txn, d.DatabaseID, func(id uint64) bool { return names[id] != "" })
	if err != nil {
		return err
	}
	for _, r := range refs {
		if r.legacy || r.table.ID == d.ID {
			continue
		}
		col, _ := r.table.ColByID(r.col)
		if !cascade {
			seq := ""
			for _, id := range r.ids {
				if names[id] != "" {
					seq = names[id]
				}
			}
			return &Error{Code: CodeDependentObjectsExist,
				Msg:    "cannot drop table " + d.Name + " because other objects depend on it",
				Detail: "default value for column " + col.Name + " of table " + r.table.Name + " depends on sequence " + seq,
				Hint:   "Use DROP ... CASCADE to drop the dependent objects too."}
		}
		if err := s.dropBoundDefault(ctx, txn, r.table.ID, r.col); err != nil {
			return err
		}
	}
	return nil
}

// fixedName is fixedSeqName for a seqDefaultRef's column.
func fixedName(r seqDefaultRef, name string) string {
	if r.legacy {
		return legacySeqName(name)
	}
	return name
}

// viewNamedObjects lists the sequences and user-defined types view v's
// query names by text (nextval('s.q'), 's.q'::regclass, 'a'::s.mood),
// resolved as the view resolves them, through its own search path. A
// view stores its query as text: moving or renaming one of them would
// change, or break, what the view reads.
func (s *Session) viewNamedObjects(ctx context.Context, txn *kvclient.Txn, v *catalog.TableDescriptor) (seqs, typs map[uint64]bool, err error) {
	stmts, perr := parser.Parse(v.ViewQuery)
	if perr != nil || len(stmts) != 1 {
		return nil, nil, nil
	}
	var seqNames, typeNames []string
	w := parser.Walker{OnExpr: func(e *parser.Expr) bool {
		if seqArgFuncs[e.Func] && len(e.Args) > 0 {
			if name, ok := seqNameArg(e.Args[0]); ok {
				seqNames = append(seqNames, name)
			}
		}
		if e.Cast != "" && e.Lit != nil && !e.Lit.Null && e.Lit.Fam == types.String {
			if strings.EqualFold(e.Cast, "regclass") || strings.EqualFold(e.Cast, "pg_catalog.regclass") {
				seqNames = append(seqNames, e.Lit.Text())
			}
		}
		if e.Cast != "" && !builtins.KnownCastType(e.Cast) {
			typeNames = append(typeNames, castLookupName(e.Cast))
		}
		return true
	}}
	w.Statement(stmts[0])
	if len(seqNames) == 0 && len(typeNames) == 0 {
		return nil, nil, nil
	}
	saved, savedBindings := s.viewPath, s.viewBindings
	s.viewPath, s.viewBindings = viewResolutionPath(v.ViewSearchPath), nil
	defer func() { s.viewPath, s.viewBindings = saved, savedBindings }()
	seqs, typs = map[uint64]bool{}, map[uint64]bool{}
	for _, n := range seqNames {
		if d, err := s.lookupSequence(ctx, txn, n); err == nil {
			seqs[d.ID] = true
		}
	}
	for _, n := range typeNames {
		if td, err := s.cat.LookupTypeIn(s.rctx(ctx), txn, s.database, n); err == nil && td != nil {
			typs[td.ID] = true
		}
	}
	return seqs, typs, nil
}

// refuseIfObjectViewed refuses (2BP01) a change to the sequences or
// types hit reports while a view of database dbID names one of them.
func (s *Session) refuseIfObjectViewed(ctx context.Context, txn *kvclient.Txn, dbID uint64, what string, seqHit, typeHit func(id uint64) bool) error {
	db, err := catalog.ReadDatabase(ctx, txn, dbID)
	if err != nil || db == nil {
		return err
	}
	all, err := s.cat.ListIn(ctx, txn, db)
	if err != nil {
		return err
	}
	for _, v := range all {
		if !v.IsView() || v.Virtual != "" {
			continue
		}
		seqs, typs, err := s.viewNamedObjects(ctx, txn, v)
		if err != nil {
			return err
		}
		for id := range seqs {
			if seqHit != nil && seqHit(id) {
				return newErrf(CodeDependentObjectsExist, "cannot %s: view %q names a sequence it affects (drop or replace the view first)", what, v.Name)
			}
		}
		for id := range typs {
			if typeHit != nil && typeHit(id) {
				return newErrf(CodeDependentObjectsExist, "cannot %s: view %q names a type it affects (drop or replace the view first)", what, v.Name)
			}
		}
	}
	return nil
}

// refuseIfTypeCastByText refuses (2BP01) moving type td, or renaming
// its schema, while a column default or CHECK constraint of database
// dbID casts to a type by a name that could be it: those are stored as
// text and resolved when evaluated. qualifier, when set, is the
// schema's name, and any cast it qualifies counts.
func (s *Session) refuseIfTypeCastByText(ctx context.Context, txn *kvclient.Txn, dbID uint64, typeNames []string, qualifier, what string) error {
	if len(typeNames) == 0 && qualifier == "" {
		return nil
	}
	db, err := catalog.ReadDatabase(ctx, txn, dbID)
	if err != nil || db == nil {
		return err
	}
	all, err := s.cat.ListIn(ctx, txn, db)
	if err != nil {
		return err
	}
	hits := func(text string) bool {
		if text == "" || !strings.Contains(text, "::") && !strings.Contains(strings.ToUpper(text), "CAST") {
			return false
		}
		e, err := parser.ParseExpr(text)
		if err != nil {
			return castTextNames(text, typeNames, qualifier)
		}
		found := false
		w := parser.Walker{OnExpr: func(x *parser.Expr) bool {
			if x.Cast == "" || builtins.KnownCastType(x.Cast) {
				return true
			}
			q, bare := catalog.SplitTableName(castLookupName(x.Cast))
			if qualifier != "" && q == qualifier {
				found = true
			}
			for _, n := range typeNames {
				if bare == n {
					found = true
				}
			}
			return true
		}}
		w.WalkExpr(&e)
		return found
	}
	for _, t := range all {
		if t.IsView() || t.Virtual != "" {
			continue
		}
		for _, c := range t.Columns {
			if hits(c.DefaultExpr) {
				return newErrf(CodeDependentObjectsExist, "cannot %s: the default of column %s.%s casts to a type it affects (change the default first)", what, t.Name, c.Name)
			}
		}
		for _, k := range t.Constraints {
			if hits(k.Expr) {
				return newErrf(CodeDependentObjectsExist, "cannot %s: constraint %q of table %q casts to a type it affects (drop the constraint first)", what, k.Name, t.Name)
			}
		}
	}
	return nil
}

// castTextNames is refuseIfTypeCastByText's test for a text that does
// not parse as an expression (a CHECK's condition): a cast to one of
// names, or to a type the qualifier qualifies, anywhere in it.
func castTextNames(text string, names []string, qualifier string) bool {
	lower := strings.ToLower(text)
	for _, n := range names {
		if strings.Contains(lower, "::"+strings.ToLower(n)) || strings.Contains(lower, "::"+strings.ToLower(quoteIdentifier(n))) ||
			strings.Contains(lower, "."+strings.ToLower(n)) || strings.Contains(lower, " as "+strings.ToLower(n)) {
			return true
		}
	}
	if qualifier != "" {
		q := strings.ToLower(qualifier)
		if strings.Contains(lower, "::"+q+".") || strings.Contains(lower, "::"+strings.ToLower(quoteIdentifier(qualifier))+".") ||
			strings.Contains(lower, " as "+q+".") {
			return true
		}
	}
	return false
}
