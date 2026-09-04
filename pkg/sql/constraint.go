package sql

import (
	"context"
	"fmt"
	"strings"

	"github.com/sthorne/datax/pkg/keys"
	"github.com/sthorne/datax/pkg/kvclient"
	"github.com/sthorne/datax/pkg/sql/catalog"
	"github.com/sthorne/datax/pkg/sql/parser"
	"github.com/sthorne/datax/pkg/sql/rowenc"
	"github.com/sthorne/datax/pkg/sql/types"
	"github.com/sthorne/datax/pkg/util/log"
	"github.com/sthorne/datax/pkg/version"
)

// Table constraints: CHECK, FOREIGN KEY and named UNIQUE (cluster
// version v8).
//
// A CHECK is stored as its expression's text and evaluated per written
// row as the lowered negation: the row violates the constraint when NOT
// expr is TRUE, so a NULL result passes, as in PostgreSQL. A UNIQUE
// constraint is a unique index carrying the constraint's name. A
// FOREIGN KEY is checked on the child side by a point read of the parent
// (its primary key or a unique index) inside the writing transaction —
// serializable isolation already refuses a concurrent parent delete, so
// no extra locking — and on the parent side, when a referenced row is
// deleted or its key changes, by looking up the children through the
// index the constraint created (or found) on the referencing columns
// and applying the action: refuse (RESTRICT / NO ACTION), delete or
// re-key them (CASCADE), or null the reference (SET NULL). A cascade is
// bounded per statement (foreign_key_cascade_limit): an unbounded
// cascade is an unbounded transaction.
//
// ALTER TABLE ... ADD CONSTRAINT publishes the constraint first, so new
// writes honor it from the moment every gateway has adopted the
// descriptor, then validates the existing rows in bounded chunks (NOT
// VALID skips that; VALIDATE CONSTRAINT runs it later) — the shape of
// the online index build, and like it not allowed inside a transaction
// block.

// DefaultFKCascadeLimit is the default per-statement cap on rows a
// foreign-key cascade may touch (SET foreign_key_cascade_limit).
const DefaultFKCascadeLimit = 10000

// requireV8 gates the DDL that writes v8 descriptor fields.
func (s *Session) requireV8(what string) error {
	if s.db.ClusterVersion() < version.V8 {
		return newErrf(CodeFeatureNotSupported, "%s needs cluster version v8: finalize the upgrade with `datax debug upgrade` first", what)
	}
	return nil
}

// constraintName picks a constraint's name: the given one (refused when
// taken), else PostgreSQL's <table>_<col>_<suffix>, numbered when taken.
func constraintName(desc *catalog.TableDescriptor, given string, cols []string, suffix string) (string, error) {
	taken := func(n string) bool {
		if n == desc.Name+"_pkey" {
			return true
		}
		if _, ok := desc.Constraint(n); ok {
			return true
		}
		if _, ok := desc.Index(n); ok {
			return true
		}
		return false
	}
	if given != "" {
		if taken(given) {
			return "", newErrf(CodeDuplicateObject, "constraint %q for relation %q already exists", given, desc.Name)
		}
		return given, nil
	}
	base := desc.Name
	if len(cols) > 0 {
		base += "_" + strings.Join(cols, "_")
	}
	base += "_" + suffix
	if !taken(base) {
		return base, nil
	}
	for i := 1; ; i++ {
		n := fmt.Sprintf("%s%d", base, i)
		if !taken(n) {
			return n, nil
		}
	}
}

// resolveColumns maps column names to descriptor columns, refusing
// unknown, hidden and repeated ones.
func resolveColumns(desc *catalog.TableDescriptor, names []string, what string) ([]catalog.Column, error) {
	seen := map[catalog.ColumnID]bool{}
	out := make([]catalog.Column, 0, len(names))
	for _, n := range names {
		c, ok := desc.Col(n)
		if !ok || c.Hidden {
			return nil, newErrf(CodeUndefinedColumn, "column %q named in %s does not exist", n, what)
		}
		if seen[c.ID] {
			return nil, newErrf(CodeSyntaxError, "column %q appears twice in %s", n, what)
		}
		seen[c.ID] = true
		out = append(out, c)
	}
	return out, nil
}

// addConstraint resolves one parsed constraint against desc (already
// carrying its ID) and appends it, creating the index a UNIQUE
// constraint is or a FOREIGN KEY needs. Indexes are created in state
// indexState: public on a new, empty table; write-only when the table
// may hold rows (the caller backfills them). A foreign key to another
// table adds the inbound reference to that table's descriptor — a
// private clone kept in parents (by ID) across the statement's
// constraints, returned for the caller to write. validated marks the
// constraint validated.
func (s *Session) addConstraint(ctx context.Context, txn *kvclient.Txn, desc *catalog.TableDescriptor, cd *parser.ConstraintDef, indexState string, validated bool, parents map[uint64]*catalog.TableDescriptor) (*catalog.Constraint, *catalog.TableDescriptor, error) {
	if err := s.requireV8("table constraints"); err != nil {
		return nil, nil, err
	}
	if desc.NextConstraintID == 0 {
		var max uint64
		for _, c := range desc.Constraints {
			if c.ID > max {
				max = c.ID
			}
		}
		desc.NextConstraintID = max + 1
	}
	if desc.NextIndexID < rowenc.PrimaryIndexID+1 {
		desc.NextIndexID = rowenc.PrimaryIndexID + 1
	}
	c := catalog.Constraint{ID: desc.NextConstraintID, Kind: cd.Kind, Validated: validated}
	var parent *catalog.TableDescriptor
	switch cd.Kind {
	case "unique":
		cols, err := resolveColumns(desc, cd.Columns, "the UNIQUE constraint")
		if err != nil {
			return nil, nil, err
		}
		name, err := constraintName(desc, cd.Name, cd.Columns, "key")
		if err != nil {
			return nil, nil, err
		}
		for _, col := range cols {
			if !types.IsIndexable(col.Type) {
				return nil, nil, newErrf(CodeFeatureNotSupported, "column %q of type %s cannot be part of a unique constraint (no ordered key encoding)", col.Name, col.Type)
			}
			c.Columns = append(c.Columns, col.ID)
		}
		idx := catalog.IndexDescriptor{ID: desc.NextIndexID, Name: name, Unique: true, ColumnIDs: c.Columns, State: indexState}
		desc.NextIndexID++
		desc.Indexes = append(desc.Indexes, idx)
		c.Name, c.IndexID, c.Validated = name, idx.ID, true
	case "check":
		if exprHasSub := condsHave(cd.CheckFails, func(e parser.Expr) bool { return e.Sub != nil || e.Param > 0 }); exprHasSub {
			return nil, nil, newErrf(CodeFeatureNotSupported, "CHECK constraints cannot use subqueries or parameters")
		}
		if condsHave(cd.CheckFails, func(e parser.Expr) bool { return e.Func != "" && volatileFuncs[e.Func] }) {
			return nil, nil, newErrf(CodeFeatureNotSupported, "CHECK constraints cannot use volatile functions")
		}
		var names []string
		seen := map[string]bool{}
		for _, cmp := range cd.CheckFails {
			condColumns(cmp, func(n string) {
				if !seen[n] {
					seen[n] = true
					names = append(names, n)
				}
			})
		}
		cols, err := resolveColumns(desc, names, "the CHECK constraint")
		if err != nil {
			return nil, nil, err
		}
		for _, col := range cols {
			c.Columns = append(c.Columns, col.ID)
		}
		// Try it once so an unknown function or a type mismatch fails now,
		// not at the first INSERT: against a NULL row and a sample row.
		for _, probe := range []map[catalog.ColumnID]types.Datum{nullRow(desc), sampleRow(desc)} {
			if _, err := matchesWhere(cd.CheckFails, desc, probe, nil); err != nil {
				return nil, nil, newErrf(CodeSyntaxError, "CHECK constraint: %v", err)
			}
		}
		name, err := constraintName(desc, cd.Name, names[:min(len(names), 1)], "check")
		if err != nil {
			return nil, nil, err
		}
		c.Name, c.Expr = name, cd.Check
	case "foreign":
		cols, err := resolveColumns(desc, cd.Columns, "the foreign key")
		if err != nil {
			return nil, nil, err
		}
		name, err := constraintName(desc, cd.Name, cd.Columns, "fkey")
		if err != nil {
			return nil, nil, err
		}
		shared, err := s.lookup(ctx, txn, cd.RefTable)
		if err != nil {
			return nil, nil, err
		}
		if err := mustBeReal(shared); err != nil {
			return nil, nil, err
		}
		ref := desc // self-reference: one descriptor, written once
		if shared.ID != desc.ID {
			if shared.DatabaseID != desc.DatabaseID {
				return nil, nil, newErrf(CodeFeatureNotSupported, "foreign key %q: referenced table %q is in another database", name, shared.Name)
			}
			// Never write through the cached descriptor: mutate a clone.
			ref = parents[shared.ID]
			if ref == nil {
				ref = shared.Clone()
				parents[shared.ID] = ref
			}
		}
		refNames := cd.RefColumns
		if len(refNames) == 0 {
			for _, id := range visiblePKIDs(ref) {
				col, _ := ref.ColByID(id)
				refNames = append(refNames, col.Name)
			}
		}
		refCols, err := resolveColumns(ref, refNames, "the referenced table")
		if err != nil {
			return nil, nil, err
		}
		if len(refCols) != len(cols) {
			return nil, nil, newErrf(CodeInvalidColumnReference, "foreign key %q: %d referencing columns but %d referenced columns", name, len(cols), len(refCols))
		}
		for i := range cols {
			if cols[i].Type != refCols[i].Type {
				return nil, nil, newErrf(CodeInvalidColumnReference, "foreign key %q: column %q is %s but referenced column %q is %s", name, cols[i].Name, cols[i].Type, refCols[i].Name, refCols[i].Type)
			}
			c.Columns = append(c.Columns, cols[i].ID)
			c.RefColumns = append(c.RefColumns, refCols[i].ID)
		}
		if !uniqueKeyOf(ref, c.RefColumns) {
			return nil, nil, newErrf(CodeInvalidColumnReference, "there is no unique constraint matching given keys for referenced table %q", ref.Name)
		}
		c.OnDelete, c.OnUpdate = cd.OnDelete, cd.OnUpdate
		if c.OnDelete == "" {
			c.OnDelete = catalog.FKRestrict
		}
		if c.OnUpdate == "" {
			c.OnUpdate = catalog.FKRestrict
		}
		// The referencing side gets an index when none covers the columns,
		// so a parent delete is a point lookup, never a scan of the child.
		if !indexCovers(desc, c.Columns) {
			idx := catalog.IndexDescriptor{ID: desc.NextIndexID, Name: name + "_idx", ColumnIDs: c.Columns, State: indexState}
			desc.NextIndexID++
			desc.Indexes = append(desc.Indexes, idx)
			c.IndexID, c.AutoIndex = idx.ID, true
		}
		c.Name, c.RefTable = name, ref.ID
		ref.InboundFKs = append(ref.InboundFKs, catalog.ForeignKeyRef{TableID: desc.ID, ConstraintID: c.ID})
		if ref != desc {
			parent = ref
		}
	default:
		return nil, nil, newErrf(CodeInternal, "unknown constraint kind %q", cd.Kind)
	}
	desc.NextConstraintID++
	desc.Constraints = append(desc.Constraints, c)
	return &desc.Constraints[len(desc.Constraints)-1], parent, nil
}

// condsHave reports whether any expression inside conds satisfies pred.
func condsHave(conds []parser.Comparison, pred func(parser.Expr) bool) bool {
	for _, c := range conds {
		if condHas(c, pred) {
			return true
		}
	}
	return false
}

func visiblePKIDs(d *catalog.TableDescriptor) []catalog.ColumnID {
	var out []catalog.ColumnID
	for _, id := range d.PrimaryKey {
		if c, ok := d.ColByID(id); ok && c.Hidden {
			continue
		}
		out = append(out, id)
	}
	return out
}

// uniqueKeyOf reports whether cols (as a set) are the table's primary
// key or a public unique index.
func uniqueKeyOf(d *catalog.TableDescriptor, cols []catalog.ColumnID) bool {
	same := func(key []catalog.ColumnID) bool {
		if len(key) != len(cols) {
			return false
		}
		set := map[catalog.ColumnID]bool{}
		for _, id := range cols {
			set[id] = true
		}
		for _, id := range key {
			if !set[id] {
				return false
			}
		}
		return true
	}
	if same(visiblePKIDs(d)) {
		return true
	}
	for i := range d.Indexes {
		if d.Indexes[i].Unique && d.Indexes[i].Public() && same(d.Indexes[i].ColumnIDs) {
			return true
		}
	}
	return false
}

// indexCovers reports whether the primary key or an index starts with
// cols (as a set), so a lookup by them is a key-bounded scan.
func indexCovers(d *catalog.TableDescriptor, cols []catalog.ColumnID) bool {
	prefix := func(key []catalog.ColumnID) bool {
		if len(key) < len(cols) {
			return false
		}
		set := map[catalog.ColumnID]bool{}
		for _, id := range cols {
			set[id] = true
		}
		for _, id := range key[:len(cols)] {
			if !set[id] {
				return false
			}
		}
		return true
	}
	if prefix(visiblePKIDs(d)) {
		return true
	}
	for i := range d.Indexes {
		if prefix(d.Indexes[i].ColumnIDs) {
			return true
		}
	}
	return false
}

func nullRow(d *catalog.TableDescriptor) map[catalog.ColumnID]types.Datum {
	row := map[catalog.ColumnID]types.Datum{}
	for _, c := range d.Columns {
		row[c.ID] = types.DNull
	}
	return row
}

// sampleRow holds one plausible value per column, for trying a CHECK
// expression at DDL time.
func sampleRow(d *catalog.TableDescriptor) map[catalog.ColumnID]types.Datum {
	row := map[catalog.ColumnID]types.Datum{}
	for _, c := range d.Columns {
		var v types.Datum
		switch c.Type {
		case types.Int, types.Float, types.Decimal:
			v, _ = types.NewInt(1).Coerce(c.Type)
		case types.String, types.Bytes:
			v = types.NewString("x")
		case types.Bool:
			v = types.NewBool(true)
		case types.Timestamp:
			v = types.NewTimestamp(0)
		case types.Date:
			v = types.NewDate(0)
		case types.Jsonb:
			v = types.NewJsonb("{}")
		default:
			v = types.DNull
		}
		row[c.ID] = v
	}
	return row
}

// ---- enforcement ----------------------------------------------------

// rowGuard enforces one table's constraints for one statement: its
// CHECKs, its foreign keys (child side), and the foreign keys that
// reference it (parent side). nil when the table has none.
type rowGuard struct {
	s      *Session
	desc   *catalog.TableDescriptor
	checks []guardCheck
	fks    []*catalog.Constraint
	// parents caches referenced tables' descriptors by ID.
	parents map[uint64]*catalog.TableDescriptor
	// st is the statement's cascade state, shared by every guard the
	// statement creates.
	st *cascadeState
}

// cascadeState is one statement's cascade bookkeeping: the remaining
// row allowance, and the rows its cascades have already deleted or
// rewritten (by primary key), so a row reached through two keys is
// deleted once and rewritten on top of its in-flight version rather
// than written twice.
type cascadeState struct {
	budget int
	rows   map[string]*inflightRow
}

type inflightRow struct {
	deleted bool
	row     map[catalog.ColumnID]types.Datum
}

func newCascadeState(budget int) *cascadeState {
	return &cascadeState{budget: budget, rows: map[string]*inflightRow{}}
}

type guardCheck struct {
	c     *catalog.Constraint
	fails []parser.Comparison
}

// guard builds the table's rowGuard (nil when nothing to enforce).
func (s *Session) guard(ctx context.Context, txn *kvclient.Txn, desc *catalog.TableDescriptor) (*rowGuard, error) {
	return s.guardWithState(ctx, txn, desc, newCascadeState(s.fkCascadeLimit()))
}

func (s *Session) guardWithState(ctx context.Context, txn *kvclient.Txn, desc *catalog.TableDescriptor, st *cascadeState) (*rowGuard, error) {
	if len(desc.Constraints) == 0 && len(desc.InboundFKs) == 0 {
		return nil, nil
	}
	g := &rowGuard{s: s, desc: desc, parents: map[uint64]*catalog.TableDescriptor{}, st: st}
	for i := range desc.Constraints {
		c := &desc.Constraints[i]
		switch c.Kind {
		case catalog.ConstraintCheck:
			fails, err := parser.ParseCheck(c.Expr)
			if err != nil {
				return nil, newErrf(CodeInternal, "constraint %q: stored expression %q: %v", c.Name, c.Expr, err)
			}
			g.checks = append(g.checks, guardCheck{c: c, fails: fails})
		case catalog.ConstraintForeign:
			g.fks = append(g.fks, c)
		}
	}
	return g, nil
}

func (s *Session) fkCascadeLimit() int {
	if s.cascadeLimit > 0 {
		return s.cascadeLimit
	}
	return DefaultFKCascadeLimit
}

// table resolves a descriptor by ID (the guard's own, or a cached one).
func (g *rowGuard) table(ctx context.Context, txn *kvclient.Txn, id uint64) (*catalog.TableDescriptor, error) {
	if id == g.desc.ID {
		return g.desc, nil
	}
	if d, ok := g.parents[id]; ok {
		return d, nil
	}
	d, err := catalog.ReadTable(ctx, txn, id)
	if err != nil {
		return nil, err
	}
	if d == nil {
		return nil, newErrf(CodeInternal, "table %d referenced by a constraint of %q does not exist", id, g.desc.Name)
	}
	g.parents[id] = d
	return d, nil
}

// checkInsert enforces the constraints an inserted row must satisfy.
// inserted is the statement's own primary keys so far, for a
// self-referencing key written earlier in the same statement.
func (g *rowGuard) checkInsert(ctx context.Context, txn *kvclient.Txn, row map[catalog.ColumnID]types.Datum, inserted map[string]bool) error {
	if g == nil {
		return nil
	}
	if err := g.checkChecks(row, "new row for relation"); err != nil {
		return err
	}
	return g.checkForeignKeys(ctx, txn, nil, row, inserted, "insert or update on table", 0)
}

// checkUpdate enforces the constraints on a row changing from oldRow to
// newRow: its CHECKs, its foreign keys when their columns changed, and
// the referencing rows when the columns they reference changed
// (cascading into wb).
func (g *rowGuard) checkUpdate(ctx context.Context, txn *kvclient.Txn, oldRow, newRow map[catalog.ColumnID]types.Datum, wb *kvclient.WriteBatch) error {
	if g == nil {
		return nil
	}
	if err := g.checkChecks(newRow, "new row for relation"); err != nil {
		return err
	}
	if err := g.checkForeignKeys(ctx, txn, oldRow, newRow, nil, "insert or update on table", 0); err != nil {
		return err
	}
	return g.enforceReferencing(ctx, txn, oldRow, newRow, wb)
}

// checkDelete applies the referencing rows' foreign-key actions for a
// deleted row (cascading into wb).
func (g *rowGuard) checkDelete(ctx context.Context, txn *kvclient.Txn, oldRow map[catalog.ColumnID]types.Datum, wb *kvclient.WriteBatch) error {
	if g == nil {
		return nil
	}
	return g.enforceReferencing(ctx, txn, oldRow, nil, wb)
}

func (g *rowGuard) checkChecks(row map[catalog.ColumnID]types.Datum, what string) error {
	if g == nil {
		return nil
	}
	for _, ck := range g.checks {
		violated, err := matchesWhere(ck.fails, g.desc, row, nil)
		if err != nil {
			return err
		}
		if violated {
			return newErrf(CodeCheckViolation, "%s %q violates check constraint %q", what, g.desc.Name, ck.c.Name)
		}
	}
	return nil
}

// keyText renders the columns' values PostgreSQL-style: (a, b)=(1, x).
func keyText(d *catalog.TableDescriptor, ids []catalog.ColumnID, row map[catalog.ColumnID]types.Datum) string {
	names := make([]string, len(ids))
	vals := make([]string, len(ids))
	for i, id := range ids {
		c, _ := d.ColByID(id)
		names[i] = c.Name
		if v := rowVal(row, id); v.Null {
			vals[i] = "null"
		} else {
			vals[i] = v.Text()
		}
	}
	return "(" + strings.Join(names, ", ") + ")=(" + strings.Join(vals, ", ") + ")"
}

// equalityConds builds the WHERE conjuncts "cols[i] = vals[i]".
func equalityConds(d *catalog.TableDescriptor, cols []catalog.ColumnID, vals []types.Datum) []parser.Comparison {
	conds := make([]parser.Comparison, len(cols))
	for i, id := range cols {
		c, _ := d.ColByID(id)
		v := vals[i]
		conds[i] = parser.Comparison{Column: c.Name, Op: "=", Value: parser.Expr{Lit: &v}}
	}
	return conds
}

// checkForeignKeys verifies the row's references exist (MATCH SIMPLE: a
// NULL anywhere in the key exempts the row). On update only keys whose
// columns changed are looked up; exempt is the key a parent's cascade
// is rewriting (its new values are the parent's, present by
// construction though not yet flushed).
func (g *rowGuard) checkForeignKeys(ctx context.Context, txn *kvclient.Txn, oldRow, row map[catalog.ColumnID]types.Datum, inserted map[string]bool, what string, exempt uint64) error {
	if g == nil {
		return nil
	}
	for _, fk := range g.fks {
		if fk.ID == exempt {
			continue
		}
		vals := make([]types.Datum, len(fk.Columns))
		skip, changed := false, oldRow == nil
		for i, id := range fk.Columns {
			vals[i] = rowVal(row, id)
			if vals[i].Null {
				skip = true
			}
			if oldRow != nil && datumChanged(rowVal(oldRow, id), vals[i]) {
				changed = true
			}
		}
		if skip || !changed {
			continue
		}
		parent, err := g.table(ctx, txn, fk.RefTable)
		if err != nil {
			return err
		}
		// A self-referencing key may point at a row this statement wrote
		// but has not flushed yet.
		if parent == g.desc && inserted != nil && sameIDs(fk.RefColumns, visiblePKIDs(parent)) {
			probe := map[catalog.ColumnID]types.Datum{}
			for i, id := range fk.RefColumns {
				probe[id] = vals[i]
			}
			if key, err := pkKeyPartial(parent, probe); err == nil && inserted[string(key)] {
				continue
			}
		}
		rows, _, err := g.s.fetchRows(ctx, txn, parent, equalityConds(parent, fk.RefColumns, vals), nil, 1)
		if err != nil {
			return err
		}
		if len(rows) == 0 {
			return newErrf(CodeForeignKeyViolation, "%s %q violates foreign key constraint %q: key %s is not present in table %q",
				what, g.desc.Name, fk.Name, keyText(g.desc, fk.Columns, row), parent.Name)
		}
	}
	return nil
}

// rowVal reads a column's value, NULL when the row does not carry it.
func rowVal(row map[catalog.ColumnID]types.Datum, id catalog.ColumnID) types.Datum {
	if v, ok := row[id]; ok {
		return v
	}
	return types.DNull
}

// datumChanged reports whether a column's value differs (NULL and
// non-NULL differ; two NULLs do not).
func datumChanged(a, b types.Datum) bool {
	if a.Null || b.Null {
		return a.Null != b.Null
	}
	c, err := a.Compare(b)
	return err != nil || c != 0
}

func sameIDs(a, b []catalog.ColumnID) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// pkKeyPartial encodes the primary key of a row holding only its key
// columns (the shard bucket, when the table has one, is derived).
func pkKeyPartial(d *catalog.TableDescriptor, key map[catalog.ColumnID]types.Datum) (keys.Key, error) {
	row := copyRow(key)
	for _, c := range d.Columns {
		if _, ok := row[c.ID]; !ok {
			row[c.ID] = types.DNull
		}
	}
	if d.ShardBuckets > 0 {
		logical := make([]types.Datum, len(d.PrimaryKey)-1)
		for i, id := range d.PrimaryKey[1:] {
			logical[i] = row[id]
		}
		sd, err := rowenc.ShardBucket(d, logical)
		if err != nil {
			return nil, err
		}
		row[d.PrimaryKey[0]] = sd
	}
	return pkKey(d, row)
}

// enforceReferencing handles the rows of other tables that reference
// oldRow, for a delete (newRow nil) or a change of the referenced
// columns: refuse, cascade, or null out, per the foreign key's action.
func (g *rowGuard) enforceReferencing(ctx context.Context, txn *kvclient.Txn, oldRow, newRow map[catalog.ColumnID]types.Datum, wb *kvclient.WriteBatch) error {
	if g == nil {
		return nil
	}
	for _, ref := range g.desc.InboundFKs {
		child, err := g.table(ctx, txn, ref.TableID)
		if err != nil {
			return err
		}
		fk, ok := child.ConstraintByID(ref.ConstraintID)
		if !ok || fk.Kind != catalog.ConstraintForeign {
			continue // a stale reference (the constraint is gone)
		}
		vals := make([]types.Datum, len(fk.RefColumns))
		skip, changed := false, newRow == nil
		for i, id := range fk.RefColumns {
			vals[i] = rowVal(oldRow, id)
			if vals[i].Null {
				skip = true
			}
			if newRow != nil && datumChanged(vals[i], rowVal(newRow, id)) {
				changed = true
			}
		}
		if skip || !changed {
			continue
		}
		action := fk.OnDelete
		if newRow != nil {
			action = fk.OnUpdate
		}
		limit := int64(1)
		if action != catalog.FKRestrict && action != "" {
			limit = int64(g.st.budget) + 1
		}
		rows, _, err := g.s.fetchRows(ctx, txn, child, equalityConds(child, fk.Columns, vals), nil, limit)
		if err != nil {
			return err
		}
		// Rows an earlier cascade of this statement deleted are gone;
		// ones it rewrote continue from their in-flight version.
		live := rows[:0]
		for _, fr := range rows {
			if f, ok := g.st.rows[string(fr.key)]; ok {
				if f.deleted {
					continue
				}
				fr.row = f.row
			}
			live = append(live, fr)
		}
		rows = live
		if len(rows) == 0 {
			continue
		}
		verb := "delete"
		if newRow != nil {
			verb = "update"
		}
		switch action {
		case catalog.FKRestrict, "":
			return newErrf(CodeForeignKeyViolation, "%s on table %q violates foreign key constraint %q on table %q: key %s is still referenced from table %q",
				verb, g.desc.Name, fk.Name, child.Name, keyText(g.desc, fk.RefColumns, oldRow), child.Name)
		case catalog.FKCascade, catalog.FKSetNull:
			if len(rows) > g.st.budget {
				return newErrf(CodeProgramLimitExceeded, "%s on table %q cascades to more than %d rows of %q through %q (foreign_key_cascade_limit)",
					verb, g.desc.Name, g.s.fkCascadeLimit(), child.Name, fk.Name)
			}
			g.st.budget -= len(rows)
			cg, err := g.s.guardWithState(ctx, txn, child, g.st)
			if err != nil {
				return err
			}
			for _, fr := range rows {
				if action == catalog.FKCascade && newRow == nil {
					if err := cg.checkDelete(ctx, txn, fr.row, wb); err != nil {
						return err
					}
					if err := deleteRow(child, fr, wb); err != nil {
						return err
					}
					g.st.rows[string(fr.key)] = &inflightRow{deleted: true}
					continue
				}
				updated := copyRow(fr.row)
				for i, id := range fk.Columns {
					if action == catalog.FKSetNull {
						col, _ := child.ColByID(id)
						if col.NotNull {
							return newErrf(CodeNotNullViolation, "%s on table %q: foreign key %q ON %s SET NULL would null column %q of %q, which is NOT NULL",
								verb, g.desc.Name, fk.Name, strings.ToUpper(verb), col.Name, child.Name)
						}
						updated[id] = types.DNull
					} else {
						updated[id] = rowVal(newRow, fk.RefColumns[i])
					}
				}
				// The child's own CHECKs, its other keys (untouched columns
				// are skipped) and its children see the change.
				if err := cg.checkChecks(updated, "new row for relation"); err != nil {
					return err
				}
				if err := cg.checkForeignKeys(ctx, txn, fr.row, updated, nil, "update on table", fk.ID); err != nil {
					return err
				}
				if err := cg.enforceReferencing(ctx, txn, fr.row, updated, wb); err != nil {
					return err
				}
				newKey, err := rewriteRow(ctx, txn, child, fr, updated, wb)
				if err != nil {
					return err
				}
				if !newKey.Equal(fr.key) {
					g.st.rows[string(fr.key)] = &inflightRow{deleted: true}
				}
				g.st.rows[string(newKey)] = &inflightRow{row: updated}
			}
		}
	}
	return nil
}

// deleteRow buffers the deletion of a fetched row and its index entries.
func deleteRow(d *catalog.TableDescriptor, fr fetchedRow, wb *kvclient.WriteBatch) error {
	wb.Delete(fr.key)
	if d.Reshard != nil {
		shadow, err := reshardShadowKey(d, fr.row)
		if err != nil {
			return err
		}
		wb.Delete(shadow)
	}
	return dropIndexEntries(d, fr.row, wb)
}

// rewriteRow buffers a row's new value: in place when its key is
// unchanged, else as a delete plus an insert at the new key, which it
// returns.
func rewriteRow(ctx context.Context, txn *kvclient.Txn, d *catalog.TableDescriptor, fr fetchedRow, updated map[catalog.ColumnID]types.Datum, wb *kvclient.WriteBatch) (keys.Key, error) {
	newKey, err := pkKey(d, updated)
	if err != nil {
		return nil, err
	}
	value, err := rowenc.EncodeValue(d, updated)
	if err != nil {
		return nil, err
	}
	if newKey.Equal(fr.key) {
		wb.Put(fr.key, value)
		if d.Reshard != nil {
			shadow, err := reshardShadowKey(d, updated)
			if err != nil {
				return nil, err
			}
			wb.Put(shadow, value)
		}
		return newKey, updateIndexEntries(ctx, txn, d, fr.row, updated, wb, map[string]bool{})
	}
	if existing, err := txn.Get(ctx, newKey); err != nil {
		return nil, err
	} else if existing != nil {
		return nil, newErrf(CodeUniqueViolation, "duplicate key value violates unique constraint on %q", d.Name)
	}
	if err := deleteRow(d, fr, wb); err != nil {
		return nil, err
	}
	wb.Put(newKey, value)
	if d.Reshard != nil {
		shadow, err := reshardShadowKey(d, updated)
		if err != nil {
			return nil, err
		}
		wb.Put(shadow, value)
	}
	return newKey, addIndexEntries(ctx, txn, d, updated, wb, map[string]bool{})
}

// ---- DDL ------------------------------------------------------------

// createTableConstraints adds a new table's constraints (column-level
// ones first, in column order, then the table-level ones). The table
// exists and is empty, so its indexes are public at once.
func (s *Session) createTableConstraints(ctx context.Context, txn *kvclient.Txn, desc *catalog.TableDescriptor, t *parser.CreateTable) (bool, error) {
	var defs []parser.ConstraintDef
	for _, cd := range t.Columns {
		defs = append(defs, cd.Constraints...)
	}
	defs = append(defs, t.Constraints...)
	if len(defs) == 0 {
		return false, nil
	}
	parents := map[uint64]*catalog.TableDescriptor{}
	for i := range defs {
		if defs[i].NotValid {
			return false, newErrf(CodeSyntaxError, "NOT VALID applies to ALTER TABLE ... ADD CONSTRAINT only")
		}
		if _, _, err := s.addConstraint(ctx, txn, desc, &defs[i], catalog.IndexStatePublic, true, parents); err != nil {
			return false, err
		}
	}
	for _, p := range parents {
		if err := s.cat.Update(ctx, txn, p); err != nil {
			return false, err
		}
		s.noteDDL(p.Name)
	}
	return true, nil
}

// execAddConstraintOnline is ALTER TABLE ... ADD CONSTRAINT: publish
// (new writes are checked from now on; a UNIQUE constraint's index and
// a foreign key's lookup index start write-only), drain, backfill the
// index, validate the existing rows in chunks unless NOT VALID, then
// mark the constraint validated. A failure at any step removes it.
func (s *Session) execAddConstraintOnline(ctx context.Context, t *parser.AlterTable) (*Result, *Error) {
	cd := t.AddConstraint
	var cID, indexID uint64
	var parentName string
	err := s.db.RunTxn(ctx, "add-constraint-publish", func(ctx context.Context, txn *kvclient.Txn) error {
		shared, err := s.lookup(ctx, txn, t.Table)
		if err != nil {
			return err
		}
		if err := mustBeReal(shared); err != nil {
			return err
		}
		desc := shared.Clone()
		if desc.Reshard != nil {
			return newErrf(CodeActiveTransaction, "cannot add a constraint to table %q while a re-shard is in progress", t.Table)
		}
		c, parent, err := s.addConstraint(ctx, txn, desc, cd, catalog.IndexStateWriteOnly, false, map[uint64]*catalog.TableDescriptor{})
		if err != nil {
			return err
		}
		cID, indexID = c.ID, c.IndexID
		if c.Kind == catalog.ConstraintUnique {
			indexID = c.IndexID
		} else if !c.AutoIndex {
			indexID = 0
		}
		if parent != nil {
			parentName = parent.Name
			if err := s.cat.Update(ctx, txn, parent); err != nil {
				return err
			}
		}
		return s.cat.Update(ctx, txn, desc)
	})
	if err != nil {
		return nil, ToSQLError(err)
	}
	if err := s.cat.FinishDDLIn(ctx, s.database, t.Table); err != nil {
		return nil, ToSQLError(err)
	}
	if parentName != "" {
		if err := s.cat.FinishDDLIn(ctx, s.database, parentName); err != nil {
			return nil, ToSQLError(err)
		}
	}
	// The index: backfill, then flip public (a unique constraint's
	// backfill is also its uniqueness check).
	var ferr error
	if indexID != 0 {
		ferr = s.backfillIndex(ctx, t.Table, indexID)
		if ferr == nil {
			ferr = s.setIndexState(ctx, t.Table, indexID, catalog.IndexStatePublic)
		}
	}
	if ferr == nil && !cd.NotValid {
		ferr = s.validateConstraintRows(ctx, t.Table, cID)
	}
	if ferr == nil && cd.Kind != "unique" && !cd.NotValid {
		ferr = s.setConstraintValidated(ctx, t.Table, cID)
	}
	if ferr != nil {
		s.abandonConstraint(ctx, t.Table, cID)
		return nil, ToSQLError(ferr)
	}
	if err := s.cat.FinishDDLIn(ctx, s.database, t.Table); err != nil {
		return nil, ToSQLError(err)
	}
	log.Audit("constraint-ddl", "stmt", "ADD CONSTRAINT", "target", t.Table, "principal", s.user)
	return &Result{Tag: "ALTER TABLE"}, nil
}

// setIndexState flips one index's state in its own transaction.
func (s *Session) setIndexState(ctx context.Context, table string, indexID uint64, state string) error {
	return s.db.RunTxn(ctx, "constraint-index-state", func(ctx context.Context, txn *kvclient.Txn) error {
		shared, err := s.lookup(ctx, txn, table)
		if err != nil {
			return err
		}
		desc := shared.Clone()
		idx, ok := desc.IndexByID(indexID)
		if !ok {
			return newErrf(CodeInternal, "index %d vanished during constraint creation", indexID)
		}
		idx.State = state
		return s.cat.Update(ctx, txn, desc)
	})
}

func (s *Session) setConstraintValidated(ctx context.Context, table string, cID uint64) error {
	return s.db.RunTxn(ctx, "constraint-validated", func(ctx context.Context, txn *kvclient.Txn) error {
		shared, err := s.lookup(ctx, txn, table)
		if err != nil {
			return err
		}
		desc := shared.Clone()
		c, ok := desc.ConstraintByID(cID)
		if !ok {
			return newErrf(CodeInternal, "constraint %d vanished during validation", cID)
		}
		c.Validated = true
		return s.cat.Update(ctx, txn, desc)
	})
}

// abandonConstraint removes a constraint whose creation or validation
// failed, with its index and the parent's reference (best effort).
func (s *Session) abandonConstraint(ctx context.Context, table string, cID uint64) {
	var tableID, indexID uint64
	var parentName string
	_ = s.db.RunTxn(ctx, "add-constraint-abandon", func(ctx context.Context, txn *kvclient.Txn) error {
		shared, err := s.lookup(ctx, txn, table)
		if err != nil {
			return err
		}
		desc := shared.Clone()
		tableID = desc.ID
		c, ok := desc.ConstraintByID(cID)
		if !ok {
			return nil
		}
		parent, err := s.removeConstraint(ctx, txn, desc, c)
		if err != nil {
			return err
		}
		indexID = c.IndexID
		if parent != nil {
			parentName = parent.Name
			if err := s.cat.Update(ctx, txn, parent); err != nil {
				return err
			}
		}
		return s.cat.Update(ctx, txn, desc)
	})
	_ = s.cat.FinishDDLIn(ctx, s.database, table)
	if parentName != "" {
		_ = s.cat.FinishDDLIn(ctx, s.database, parentName)
	}
	if tableID != 0 && indexID != 0 {
		s.wipeIndexEntries(ctx, tableID, indexID)
	}
}

// removeConstraint takes c off desc — with its index, when the
// constraint owns one — and off the referenced table's inbound list,
// returning that table (nil when none, or when it is desc itself) for
// the caller to write.
func (s *Session) removeConstraint(ctx context.Context, txn *kvclient.Txn, desc *catalog.TableDescriptor, c *catalog.Constraint) (*catalog.TableDescriptor, error) {
	removed := *c
	kept := desc.Constraints[:0]
	for _, x := range desc.Constraints {
		if x.ID != removed.ID {
			kept = append(kept, x)
		}
	}
	desc.Constraints = kept
	if removed.IndexID != 0 && (removed.Kind == catalog.ConstraintUnique || removed.AutoIndex) {
		ki := desc.Indexes[:0]
		for _, ix := range desc.Indexes {
			if ix.ID != removed.IndexID {
				ki = append(ki, ix)
			}
		}
		desc.Indexes = ki
	}
	if removed.Kind != catalog.ConstraintForeign {
		return nil, nil
	}
	parent := desc
	if removed.RefTable != desc.ID {
		p, err := catalog.ReadTable(ctx, txn, removed.RefTable)
		if err != nil {
			return nil, err
		}
		if p == nil {
			return nil, nil
		}
		parent = p.Clone()
	}
	kr := parent.InboundFKs[:0]
	for _, r := range parent.InboundFKs {
		if !(r.TableID == desc.ID && r.ConstraintID == removed.ID) {
			kr = append(kr, r)
		}
	}
	parent.InboundFKs = kr
	if parent == desc {
		return nil, nil
	}
	return parent, nil
}

// validateConstraintRows checks every existing row of the table against
// one constraint, in bounded chunks as of a boundary the way the index
// backfill plans (rows written after it were checked by their writers).
func (s *Session) validateConstraintRows(ctx context.Context, table string, cID uint64) error {
	boundary := s.db.Clock().Now()
	var cursor, end keys.Key
	if err := s.db.RunTxn(ctx, "validate-constraint-plan", func(ctx context.Context, txn *kvclient.Txn) error {
		desc, err := s.lookup(ctx, txn, table)
		if err != nil {
			return err
		}
		cursor, end = rowenc.PrimarySpanFor(desc)
		return nil
	}); err != nil {
		return err
	}
	for {
		plan, err := s.db.ScanAt(ctx, cursor, end, backfillChunkSize, boundary)
		if err != nil {
			return err
		}
		if len(plan) == 0 {
			return nil
		}
		chunkEnd := end
		if len(plan) == backfillChunkSize {
			chunkEnd = plan[len(plan)-1].Key.Next()
		}
		if err := s.db.RunTxn(ctx, "validate-constraint-chunk", func(ctx context.Context, txn *kvclient.Txn) error {
			desc, err := s.lookup(ctx, txn, table)
			if err != nil {
				return err
			}
			c, ok := desc.ConstraintByID(cID)
			if !ok {
				return newErrf(CodeInternal, "constraint vanished during validation")
			}
			g := &rowGuard{s: s, desc: desc, parents: map[uint64]*catalog.TableDescriptor{}, st: newCascadeState(0)}
			switch c.Kind {
			case catalog.ConstraintCheck:
				fails, err := parser.ParseCheck(c.Expr)
				if err != nil {
					return err
				}
				g.checks = []guardCheck{{c: c, fails: fails}}
			case catalog.ConstraintForeign:
				g.fks = []*catalog.Constraint{c}
			default:
				return nil
			}
			kvs, err := txn.Scan(ctx, cursor, chunkEnd, 0)
			if err != nil {
				return err
			}
			for _, kv := range kvs {
				row, err := decodeFullRow(desc, kv.Key, kv.Value)
				if err != nil {
					return err
				}
				if err := g.checkChecks(row, "existing row of relation"); err != nil {
					return err
				}
				if err := g.checkForeignKeys(ctx, txn, nil, row, nil, "existing row of table", 0); err != nil {
					return err
				}
			}
			return nil
		}); err != nil {
			return err
		}
		if chunkEnd.Equal(end) {
			return nil
		}
		cursor = chunkEnd
	}
}

// execValidateConstraintOnline is ALTER TABLE ... VALIDATE CONSTRAINT.
func (s *Session) execValidateConstraintOnline(ctx context.Context, t *parser.AlterTable) (*Result, *Error) {
	var cID uint64
	already := false
	if err := s.db.RunTxn(ctx, "validate-constraint-lookup", func(ctx context.Context, txn *kvclient.Txn) error {
		desc, err := s.lookup(ctx, txn, t.Table)
		if err != nil {
			return err
		}
		c, ok := desc.Constraint(t.ValidateConstraint)
		if !ok {
			return newErrf(CodeUndefinedObject, "constraint %q for table %q does not exist", t.ValidateConstraint, desc.Name)
		}
		cID, already = c.ID, c.Validated
		return nil
	}); err != nil {
		return nil, ToSQLError(err)
	}
	if already {
		return &Result{Tag: "ALTER TABLE"}, nil
	}
	if err := s.validateConstraintRows(ctx, t.Table, cID); err != nil {
		return nil, ToSQLError(err)
	}
	if err := s.setConstraintValidated(ctx, t.Table, cID); err != nil {
		return nil, ToSQLError(err)
	}
	if err := s.cat.FinishDDLIn(ctx, s.database, t.Table); err != nil {
		return nil, ToSQLError(err)
	}
	return &Result{Tag: "ALTER TABLE"}, nil
}

// execDropConstraint is ALTER TABLE ... DROP CONSTRAINT [IF EXISTS], in
// the statement's transaction; the dropped index's entries are wiped
// after commit by the caller (dropConstraintCleanup).
func (s *Session) execDropConstraint(ctx context.Context, txn *kvclient.Txn, desc *catalog.TableDescriptor, t *parser.AlterTable) (*Result, error) {
	c, ok := desc.Constraint(t.DropConstraint)
	if !ok {
		if t.DropConstraintIfExists {
			return &Result{Tag: "ALTER TABLE"}, nil
		}
		if t.DropConstraint == desc.Name+"_pkey" {
			return nil, newErrf(CodeFeatureNotSupported, "cannot drop the primary key of table %q", desc.Name)
		}
		return nil, newErrf(CodeUndefinedObject, "constraint %q for table %q does not exist", t.DropConstraint, desc.Name)
	}
	indexID := c.IndexID
	if c.Kind == catalog.ConstraintForeign && !c.AutoIndex {
		indexID = 0
	}
	parent, err := s.removeConstraint(ctx, txn, desc, c)
	if err != nil {
		return nil, err
	}
	if parent != nil {
		if err := s.cat.Update(ctx, txn, parent); err != nil {
			return nil, err
		}
		s.noteDDL(parent.Name)
	}
	if err := s.cat.Update(ctx, txn, desc); err != nil {
		return nil, err
	}
	if indexID != 0 {
		s.pendingWipes = append(s.pendingWipes, indexWipe{tableID: desc.ID, indexID: indexID})
	}
	log.Audit("constraint-ddl", "stmt", "DROP CONSTRAINT", "target", desc.Name+"."+t.DropConstraint, "principal", s.user)
	return &Result{Tag: "ALTER TABLE"}, nil
}

// indexWipe is an index whose entries are reclaimed once the dropping
// transaction has committed.
type indexWipe struct{ tableID, indexID uint64 }

// runPendingWipes reclaims the entries of indexes dropped by committed
// statements (best effort; the entries are unreachable either way).
func (s *Session) runPendingWipes(ctx context.Context) {
	wipes := s.pendingWipes
	s.pendingWipes = nil
	for _, w := range wipes {
		s.wipeIndexEntries(ctx, w.tableID, w.indexID)
	}
}

// execSetNotNullOnline is ALTER TABLE ... ALTER COLUMN c SET NOT NULL:
// publish (new NULLs are refused from now on), drain, then sweep the
// existing rows; a NULL found reverts the change.
func (s *Session) execSetNotNullOnline(ctx context.Context, t *parser.AlterTable) (*Result, *Error) {
	var colID catalog.ColumnID
	flip := func(notNull bool) error {
		return s.db.RunTxn(ctx, "set-not-null", func(ctx context.Context, txn *kvclient.Txn) error {
			shared, err := s.lookup(ctx, txn, t.Table)
			if err != nil {
				return err
			}
			if err := mustBeReal(shared); err != nil {
				return err
			}
			desc := shared.Clone()
			col, ok := desc.Col(t.SetNotNull)
			if !ok {
				return newErrf(CodeUndefinedColumn, "column %q does not exist", t.SetNotNull)
			}
			colID = col.ID
			for i := range desc.Columns {
				if desc.Columns[i].ID == col.ID {
					desc.Columns[i].NotNull = notNull
				}
			}
			return s.cat.Update(ctx, txn, desc)
		})
	}
	if err := flip(true); err != nil {
		return nil, ToSQLError(err)
	}
	if err := s.cat.FinishDDLIn(ctx, s.database, t.Table); err != nil {
		return nil, ToSQLError(err)
	}
	if err := s.sweepRows(ctx, t.Table, func(desc *catalog.TableDescriptor, row map[catalog.ColumnID]types.Datum) error {
		if rowVal(row, colID).Null {
			return newErrf(CodeNotNullViolation, "column %q of relation %q contains null values", t.SetNotNull, desc.Name)
		}
		return nil
	}); err != nil {
		_ = flip(false)
		_ = s.cat.FinishDDLIn(ctx, s.database, t.Table)
		return nil, ToSQLError(err)
	}
	return &Result{Tag: "ALTER TABLE"}, nil
}

// sweepRows runs visit over every row of the table as of a boundary, in
// bounded chunks, each in its own transaction.
func (s *Session) sweepRows(ctx context.Context, table string, visit func(*catalog.TableDescriptor, map[catalog.ColumnID]types.Datum) error) error {
	boundary := s.db.Clock().Now()
	var cursor, end keys.Key
	if err := s.db.RunTxn(ctx, "sweep-plan", func(ctx context.Context, txn *kvclient.Txn) error {
		desc, err := s.lookup(ctx, txn, table)
		if err != nil {
			return err
		}
		cursor, end = rowenc.PrimarySpanFor(desc)
		return nil
	}); err != nil {
		return err
	}
	for {
		plan, err := s.db.ScanAt(ctx, cursor, end, backfillChunkSize, boundary)
		if err != nil {
			return err
		}
		if len(plan) == 0 {
			return nil
		}
		chunkEnd := end
		if len(plan) == backfillChunkSize {
			chunkEnd = plan[len(plan)-1].Key.Next()
		}
		if err := s.db.RunTxn(ctx, "sweep-chunk", func(ctx context.Context, txn *kvclient.Txn) error {
			desc, err := s.lookup(ctx, txn, table)
			if err != nil {
				return err
			}
			kvs, err := txn.Scan(ctx, cursor, chunkEnd, 0)
			if err != nil {
				return err
			}
			for _, kv := range kvs {
				row, err := decodeFullRow(desc, kv.Key, kv.Value)
				if err != nil {
					return err
				}
				if err := visit(desc, row); err != nil {
					return err
				}
			}
			return nil
		}); err != nil {
			return err
		}
		if chunkEnd.Equal(end) {
			return nil
		}
		cursor = chunkEnd
	}
}

// constraintUses reports the constraint names that involve column id,
// on this table or through an inbound foreign key.
func (s *Session) constraintUses(ctx context.Context, txn *kvclient.Txn, desc *catalog.TableDescriptor, id catalog.ColumnID) ([]string, error) {
	var names []string
	for _, c := range desc.Constraints {
		for _, cid := range c.Columns {
			if cid == id {
				names = append(names, c.Name)
			}
		}
	}
	for _, ref := range desc.InboundFKs {
		child, err := catalog.ReadTable(ctx, txn, ref.TableID)
		if err != nil {
			return nil, err
		}
		if child == nil {
			continue
		}
		if c, ok := child.ConstraintByID(ref.ConstraintID); ok {
			for _, cid := range c.RefColumns {
				if cid == id {
					names = append(names, child.Name+"."+c.Name)
				}
			}
		}
	}
	return names, nil
}

// dropTableConstraints detaches a dropped table from the foreign keys
// around it: its own keys leave their parents' inbound lists; keys of
// other tables that reference it are dropped with CASCADE and refuse
// the drop without.
func (s *Session) dropTableConstraints(ctx context.Context, txn *kvclient.Txn, desc *catalog.TableDescriptor, cascade bool) error {
	for _, ref := range desc.InboundFKs {
		if ref.TableID == desc.ID {
			continue
		}
		child, err := catalog.ReadTable(ctx, txn, ref.TableID)
		if err != nil {
			return err
		}
		if child == nil {
			continue
		}
		c, ok := child.ConstraintByID(ref.ConstraintID)
		if !ok {
			continue
		}
		if !cascade {
			return newErrf(CodeDependentObjectsExist, "cannot drop table %q because constraint %q on table %q depends on it (use DROP TABLE ... CASCADE)", desc.Name, c.Name, child.Name)
		}
		cd := child.Clone()
		cc, _ := cd.ConstraintByID(ref.ConstraintID)
		indexID := cc.IndexID
		if !cc.AutoIndex {
			indexID = 0
		}
		if _, err := s.removeConstraint(ctx, txn, cd, cc); err != nil {
			return err
		}
		if err := s.cat.Update(ctx, txn, cd); err != nil {
			return err
		}
		s.noteDDL(cd.Name)
		if indexID != 0 {
			s.pendingWipes = append(s.pendingWipes, indexWipe{tableID: cd.ID, indexID: indexID})
		}
	}
	parents := map[uint64]*catalog.TableDescriptor{}
	for i := range desc.Constraints {
		c := &desc.Constraints[i]
		if c.Kind != catalog.ConstraintForeign || c.RefTable == desc.ID {
			continue
		}
		p, ok := parents[c.RefTable]
		if !ok {
			raw, err := catalog.ReadTable(ctx, txn, c.RefTable)
			if err != nil {
				return err
			}
			if raw == nil {
				continue
			}
			p = raw.Clone()
			parents[c.RefTable] = p
		}
		kr := p.InboundFKs[:0]
		for _, r := range p.InboundFKs {
			if r.TableID != desc.ID {
				kr = append(kr, r)
			}
		}
		p.InboundFKs = kr
	}
	for _, p := range parents {
		if err := s.cat.Update(ctx, txn, p); err != nil {
			return err
		}
		s.noteDDL(p.Name)
	}
	return nil
}
