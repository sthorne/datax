package sql

import (
	"context"
	"strings"

	"github.com/sthorne/datax/pkg/kvclient"
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
// that names no sequence yet is bound to 0 and resolves when used. nil
// when the default names no sequence.
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
			if serr, ok := err.(*Error); ok && serr.Code == CodeUndefinedTable {
				out[name] = 0
				continue
			}
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

// boundDefaultExpr is a column's parsed default with its sequence
// names replaced by what they are bound to: the sequence's OID for a
// bound name (nextval reads it as a regclass), the name qualified to
// public for a default from before bindings, the name itself for one
// bound to nothing.
func boundDefaultExpr(c *catalog.Column, e parser.Expr) parser.Expr {
	return mapSeqArgs(e, func(_ parser.Expr, name string) (parser.Expr, bool) {
		if c.SeqBindings != nil {
			id := c.SeqBindings[name]
			if id == 0 {
				return parser.Expr{}, false
			}
			d := types.NewInt(int64(id))
			return parser.Expr{Lit: &d}, true
		}
		d := types.NewString(legacySeqName(name))
		return parser.Expr{Lit: &d}, true
	})
}

// currentSeqBinding is the sequence a default's name resolves to now,
// as the default is evaluated (0 for none).
func (s *Session) currentSeqBinding(ctx context.Context, txn *kvclient.Txn, c *catalog.Column, name string) (uint64, error) {
	if c.SeqBindings != nil {
		return c.SeqBindings[name], nil
	}
	d, err := s.lookupSequence(ctx, txn, legacySeqName(name))
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
		if t.IsView() || t.Virtual != "" || (t.Temp != nil && !s.tempVisible(t)) {
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
				if r.legacy {
					// Still resolved through public alone, now bound.
					name = legacySeqName(name)
					d := types.NewString(name)
					arg.Lit = &d
					bindings[name] = 0
					return arg, true
				}
				bindings[name] = 0
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
