package sql

import (
	"context"
	"strconv"
	"strings"

	"github.com/sthorne/datax/pkg/keys"
	"github.com/sthorne/datax/pkg/kvclient"
	"github.com/sthorne/datax/pkg/sql/catalog"
	"github.com/sthorne/datax/pkg/sql/parser"
	"github.com/sthorne/datax/pkg/sql/rowenc"
	"github.com/sthorne/datax/pkg/sql/vtable"
	"github.com/sthorne/datax/pkg/util/log"
)

// The DDL of issue #95, part one: DROP INDEX, the RENAME forms, ALTER
// COLUMN SET / DROP DEFAULT and TRUNCATE. Each is one descriptor write
// in the statement's transaction (explicit or implicit), so all of them
// run inside a transaction block; whatever must follow the commit — the
// lease drain, an index wipe — rides the session's post-commit hooks
// (pendingDDL / pendingWipes), exactly like DROP CONSTRAINT.

// findIndex locates an index by name among the tables of the session's
// database (or of the qualifier in name). The descriptor comes through
// the leased lookup so the caller's write is version-safe.
func (s *Session) findIndex(ctx context.Context, txn *kvclient.Txn, name string) (*catalog.TableDescriptor, *catalog.IndexDescriptor, error) {
	dbName, bare := catalog.SplitTableName(name)
	if dbName == "" {
		dbName = s.database
	}
	db, err := s.cat.Database(ctx, txn, dbName)
	if err != nil {
		return nil, nil, ToSQLError(err)
	}
	all, err := s.cat.ListIn(ctx, txn, db)
	if err != nil {
		return nil, nil, err
	}
	for _, d := range all {
		if _, ok := d.Index(bare); !ok {
			continue
		}
		tname := d.Name
		if dbName != s.database {
			tname = dbName + "." + d.Name
		}
		shared, err := s.lookup(ctx, txn, tname)
		if err != nil {
			return nil, nil, err
		}
		desc := shared.Clone()
		for i := range desc.Indexes {
			if desc.Indexes[i].Name == bare {
				return desc, &desc.Indexes[i], nil
			}
		}
	}
	return nil, nil, newErrf(CodeUndefinedObject, "index %q does not exist", bare)
}

// execDropIndex is DROP INDEX [IF EXISTS] name: the descriptor drops the
// index now; its entries are wiped after the commit and the lease drain,
// once no gateway can still write it.
func (s *Session) execDropIndex(ctx context.Context, txn *kvclient.Txn, t *parser.DropIndex) (*Result, error) {
	desc, idx, err := s.findIndex(ctx, txn, t.Name)
	if err != nil {
		if serr, ok := err.(*Error); ok && serr.Code == CodeUndefinedObject && t.IfExists {
			return &Result{Tag: "DROP INDEX"}, nil
		}
		return nil, err
	}
	if err := mustBeReal(desc); err != nil {
		return nil, err
	}
	if catalog.IsSystemTable(desc.Name) && !s.system {
		return nil, newErrf(CodeInsufficientPriv, "index %q belongs to the cluster table %q and cannot be dropped", idx.Name, desc.Name)
	}
	if err := s.checkTableOwner(ctx, txn, desc); err != nil {
		return nil, err
	}
	for _, c := range desc.Constraints {
		if c.IndexID == idx.ID {
			return nil, newErrf(CodeDependentObjectsExist, "cannot drop index %q because constraint %q on table %q requires it (drop the constraint instead)", idx.Name, c.Name, desc.Name)
		}
	}
	if desc.Reshard != nil {
		return nil, newErrf(CodeObjectInUse, "cannot drop an index of table %q while a re-shard is in progress", desc.Name)
	}
	if !idx.Public() {
		return nil, newErrf(CodeObjectInUse, "index %q is still being built", idx.Name)
	}
	indexID, indexName := idx.ID, idx.Name
	kept := desc.Indexes[:0]
	for _, ix := range desc.Indexes {
		if ix.ID != indexID {
			kept = append(kept, ix)
		}
	}
	desc.Indexes = kept
	if err := s.cat.Update(ctx, txn, desc); err != nil {
		return nil, err
	}
	s.noteDDL(desc.Name)
	s.pendingWipes = append(s.pendingWipes, indexWipe{tableID: desc.ID, indexID: indexID})
	log.Audit("index-ddl", "stmt", "DROP INDEX", "target", desc.Name+"."+indexName, "principal", s.sessionUser, "role", s.user)
	return &Result{Tag: "DROP INDEX"}, nil
}

// execAlterIndex is ALTER INDEX [IF EXISTS] name RENAME TO new. An index
// that backs a UNIQUE constraint of the same name renames the constraint
// with it, as in PostgreSQL.
func (s *Session) execAlterIndex(ctx context.Context, txn *kvclient.Txn, t *parser.AlterIndex) (*Result, error) {
	desc, idx, err := s.findIndex(ctx, txn, t.Name)
	if err != nil {
		if serr, ok := err.(*Error); ok && serr.Code == CodeUndefinedObject && t.IfExists {
			return &Result{Tag: "ALTER INDEX"}, nil
		}
		return nil, err
	}
	if err := mustBeReal(desc); err != nil {
		return nil, err
	}
	if catalog.IsSystemTable(desc.Name) && !s.system {
		return nil, newErrf(CodeInsufficientPriv, "index %q belongs to the cluster table %q", idx.Name, desc.Name)
	}
	if err := s.checkTableOwner(ctx, txn, desc); err != nil {
		return nil, err
	}
	if err := checkNewIndexName(desc, t.NewName); err != nil {
		return nil, err
	}
	old := idx.Name
	idx.Name = t.NewName
	for i := range desc.Constraints {
		if c := &desc.Constraints[i]; c.IndexID == idx.ID && c.Kind == catalog.ConstraintUnique && c.Name == old {
			c.Name = t.NewName
		}
	}
	if err := s.cat.Update(ctx, txn, desc); err != nil {
		return nil, err
	}
	s.noteDDL(desc.Name)
	log.Audit("index-ddl", "stmt", "ALTER INDEX RENAME", "target", desc.Name+"."+old, "to", t.NewName, "principal", s.sessionUser, "role", s.user)
	return &Result{Tag: "ALTER INDEX"}, nil
}

// checkNewIndexName refuses an index name the table already uses (for an
// index or a constraint) or the reserved primary name.
func checkNewIndexName(desc *catalog.TableDescriptor, name string) error {
	if name == "primary" {
		return newErrf(CodeSyntaxError, "index name %q is reserved", name)
	}
	if _, exists := desc.Index(name); exists {
		return newErrf(CodeDuplicateObject, "index %q already exists on table %q", name, desc.Name)
	}
	if _, exists := desc.Constraint(name); exists {
		return newErrf(CodeDuplicateObject, "constraint %q already exists on table %q", name, desc.Name)
	}
	return nil
}

// execRenameTable is ALTER TABLE t RENAME TO new: the name entry moves,
// the ID stays, so foreign keys and owned sequences still resolve. The
// new name is bare (the parser takes one identifier): a table cannot
// move between databases.
func (s *Session) execRenameTable(ctx context.Context, txn *kvclient.Txn, desc *catalog.TableDescriptor, t *parser.AlterTable) (*Result, error) {
	if catalog.IsSystemTable(t.RenameTo) || vtableName(t.RenameTo) {
		return nil, newErrf(CodeInsufficientPriv, "table name %q is reserved", t.RenameTo)
	}
	if err := s.refuseIfViewed(ctx, txn, desc, "rename table "+desc.Name); err != nil {
		return nil, err
	}
	old := desc.Name
	if err := s.cat.RenameTable(ctx, txn, desc, t.RenameTo); err != nil {
		return nil, err
	}
	log.Audit("table-ddl", "stmt", "ALTER TABLE RENAME", "target", old, "to", t.RenameTo, "principal", s.sessionUser, "role", s.user)
	return &Result{Tag: "ALTER TABLE"}, nil
}

// vtableName reports whether a bare name would shadow a virtual catalog
// table that unqualified lookups fall back to.
func vtableName(name string) bool {
	_, ok := vtable.Lookup(name)
	return ok
}

// execRenameColumn is ALTER TABLE t RENAME [COLUMN] a TO b. Indexes,
// constraints and foreign keys reference columns by ID; the stored text
// of CHECK constraints is rewritten.
func (s *Session) execRenameColumn(ctx context.Context, txn *kvclient.Txn, desc *catalog.TableDescriptor, r *parser.Rename) (*Result, error) {
	col, ok := desc.Col(r.From)
	if !ok || col.Hidden {
		return nil, newErrf(CodeUndefinedColumn, "column %q does not exist", r.From)
	}
	if r.To == r.From {
		return &Result{Tag: "ALTER TABLE"}, nil
	}
	if _, exists := desc.Col(r.To); exists {
		return nil, newErrf(CodeDuplicateObject, "column %q already exists", r.To)
	}
	if err := s.refuseIfViewed(ctx, txn, desc, "rename column "+r.From); err != nil {
		return nil, err
	}
	for i := range desc.Columns {
		if desc.Columns[i].ID == col.ID {
			desc.Columns[i].Name = r.To
		}
	}
	for i := range desc.Constraints {
		c := &desc.Constraints[i]
		if c.Kind != catalog.ConstraintCheck {
			continue
		}
		uses := false
		for _, id := range c.Columns {
			if id == col.ID {
				uses = true
			}
		}
		if !uses {
			continue
		}
		text, err := parser.RenameColumnRefs(c.Expr, r.From, r.To)
		if err != nil {
			return nil, newErrf(CodeInternal, "constraint %q: %v", c.Name, err)
		}
		c.Expr = text
	}
	if err := s.cat.Update(ctx, txn, desc); err != nil {
		return nil, err
	}
	log.Audit("table-ddl", "stmt", "ALTER TABLE RENAME COLUMN", "target", desc.Name+"."+r.From, "to", r.To, "principal", s.sessionUser, "role", s.user)
	return &Result{Tag: "ALTER TABLE"}, nil
}

// execRenameConstraint is ALTER TABLE t RENAME CONSTRAINT a TO b; a
// UNIQUE constraint's index carries the constraint's name and is renamed
// with it.
func (s *Session) execRenameConstraint(ctx context.Context, txn *kvclient.Txn, desc *catalog.TableDescriptor, r *parser.Rename) (*Result, error) {
	c, ok := desc.Constraint(r.From)
	if !ok {
		if r.From == desc.Name+"_pkey" {
			return nil, newErrf(CodeFeatureNotSupported, "the primary key of table %q cannot be renamed", desc.Name)
		}
		return nil, newErrf(CodeUndefinedObject, "constraint %q for table %q does not exist", r.From, desc.Name)
	}
	if r.To == r.From {
		return &Result{Tag: "ALTER TABLE"}, nil
	}
	if err := checkNewIndexName(desc, r.To); err != nil {
		return nil, err
	}
	c.Name = r.To
	if c.Kind == catalog.ConstraintUnique {
		for i := range desc.Indexes {
			if desc.Indexes[i].ID == c.IndexID && desc.Indexes[i].Name == r.From {
				desc.Indexes[i].Name = r.To
			}
		}
	}
	if err := s.cat.Update(ctx, txn, desc); err != nil {
		return nil, err
	}
	log.Audit("constraint-ddl", "stmt", "RENAME CONSTRAINT", "target", desc.Name+"."+r.From, "to", r.To, "principal", s.sessionUser, "role", s.user)
	return &Result{Tag: "ALTER TABLE"}, nil
}

// execSetDefault is ALTER TABLE t ALTER [COLUMN] c SET DEFAULT value and
// DROP DEFAULT (sd nil). A column added with a DEFAULT fills its
// pre-existing rows from that constant on read, so its fill value must
// survive a new default: the new insert default is stored as the
// expression default (a literal's text, or "NULL" for none), which
// inserts prefer, while the constant keeps filling the old rows.
func (s *Session) execSetDefault(ctx context.Context, txn *kvclient.Txn, desc *catalog.TableDescriptor, colName string, sd *parser.SetDefault) (*Result, error) {
	col, ok := desc.Col(colName)
	if !ok || col.Hidden {
		return nil, newErrf(CodeUndefinedColumn, "column %q does not exist", colName)
	}
	if col.Identity != "" {
		return nil, newErrf(CodeSyntaxError, "column %q is an identity column; its default is its sequence", colName)
	}
	stmt := "DROP DEFAULT"
	switch {
	case sd == nil:
		if col.FillDefault {
			col.DefaultExpr = "NULL"
		} else {
			col.Default, col.DefaultExpr = nil, ""
		}
	case sd.Default != nil:
		stmt = "SET DEFAULT"
		if sd.Default.Null {
			if col.FillDefault {
				col.DefaultExpr = "NULL"
			} else {
				col.Default, col.DefaultExpr = nil, ""
			}
			break
		}
		d, cerr := coerceColumn(col, *sd.Default)
		if cerr != nil {
			return nil, newErrf(CodeSyntaxError, "DEFAULT for column %q: %v", colName, cerr.Msg)
		}
		d, terr := enforceTypmod(col, d)
		if terr != nil {
			return nil, terr
		}
		if col.FillDefault {
			text, err := s.validateDefaultExpr(ctx, txn, colName, col.Type, parser.Expr{Lit: &d})
			if err != nil {
				return nil, err
			}
			col.DefaultExpr = text
		} else {
			col.Default, col.DefaultExpr = &d, ""
		}
	default:
		stmt = "SET DEFAULT"
		text, err := s.validateDefaultExpr(ctx, txn, colName, col.Type, *sd.Expr)
		if err != nil {
			return nil, err
		}
		col.DefaultExpr = text
		if !col.FillDefault {
			col.Default = nil
		}
	}
	for i := range desc.Columns {
		if desc.Columns[i].ID == col.ID {
			desc.Columns[i] = col
		}
	}
	if err := s.cat.Update(ctx, txn, desc); err != nil {
		return nil, err
	}
	log.Audit("table-ddl", "stmt", "ALTER COLUMN "+stmt, "target", desc.Name+"."+colName, "principal", s.sessionUser, "role", s.user)
	return &Result{Tag: "ALTER TABLE"}, nil
}

// execTruncate is TRUNCATE [TABLE] t [, ...] [RESTART IDENTITY] [CASCADE]:
// each table's rows and index entries are abandoned in place by moving
// the table to fresh index IDs (one descriptor write, however many
// ranges the table spans) — the layout swap the online re-shard uses.
// The old layout stays readable for AS OF SYSTEM TIME below the
// truncation until the re-shard janitor reclaims it after the historical
// window. A table another table's foreign key references is refused
// unless every referencing table is truncated too (CASCADE adds them).
func (s *Session) execTruncate(ctx context.Context, txn *kvclient.Txn, t *parser.Truncate) (*Result, error) {
	descs := map[uint64]*catalog.TableDescriptor{}
	var order []uint64
	add := func(name string) (*catalog.TableDescriptor, error) {
		if _, bare := catalog.SplitTableName(name); catalog.IsSystemTable(bare) && !s.system {
			return nil, newErrf(CodeInsufficientPriv, "table %q belongs to the cluster and cannot be truncated (DELETE FROM it)", bare)
		}
		shared, err := s.lookup(ctx, txn, name)
		if err != nil {
			return nil, err
		}
		if err := mustBeReal(shared); err != nil {
			return nil, err
		}
		if d, ok := descs[shared.ID]; ok {
			return d, nil
		}
		if err := s.checkTablePriv(ctx, txn, shared, "TRUNCATE"); err != nil {
			return nil, err
		}
		desc := shared.Clone()
		if desc.Reshard != nil {
			return nil, newErrf(CodeObjectInUse, "cannot truncate table %q while a re-shard is in progress", desc.Name)
		}
		for _, idx := range desc.Indexes {
			if !idx.Public() {
				return nil, newErrf(CodeObjectInUse, "cannot truncate table %q while index %q is being built", desc.Name, idx.Name)
			}
		}
		descs[desc.ID] = desc
		order = append(order, desc.ID)
		return desc, nil
	}
	for _, name := range t.Tables {
		if _, err := add(name); err != nil {
			return nil, err
		}
	}
	// Referencing tables: refused, or truncated along under CASCADE
	// (transitively — a child's own children too).
	for i := 0; i < len(order); i++ {
		desc := descs[order[i]]
		for _, ref := range desc.InboundFKs {
			if _, ok := descs[ref.TableID]; ok {
				continue
			}
			child, err := catalog.ReadTable(ctx, txn, ref.TableID)
			if err != nil {
				return nil, err
			}
			if child == nil {
				continue
			}
			c, ok := child.ConstraintByID(ref.ConstraintID)
			if !ok {
				continue
			}
			if !t.Cascade {
				return nil, newErrf(CodeFeatureNotSupported, "cannot truncate table %q because table %q references it through foreign key %q (use TRUNCATE ... CASCADE, or truncate both)", desc.Name, child.Name, c.Name)
			}
			// Foreign keys never cross databases, so the child resolves
			// in the parent's.
			cname := child.Name
			if q, _ := catalog.SplitTableName(desc.Name); q != "" {
				cname = q + "." + child.Name
			}
			if _, err := add(cname); err != nil {
				return nil, err
			}
		}
	}
	now := s.db.Clock().Now().WallTime
	var names []string
	for _, id := range order {
		desc := descs[id]
		if err := truncateLayout(desc, now); err != nil {
			return nil, err
		}
		if err := s.cat.Update(ctx, txn, desc); err != nil {
			return nil, err
		}
		if err := txn.Delete(ctx, keys.TableStatsKey(desc.ID)); err != nil {
			return nil, err
		}
		if t.RestartIdentity {
			if err := s.restartOwnedSequences(ctx, txn, desc); err != nil {
				return nil, err
			}
		}
		s.noteDDL(desc.Name)
		names = append(names, desc.Name)
	}
	log.Audit("table-ddl", "stmt", "TRUNCATE", "target", strings.Join(names, ","), "principal", s.sessionUser, "role", s.user)
	return &Result{Tag: "TRUNCATE TABLE"}, nil
}

// truncateLayout moves desc to fresh primary and secondary index IDs,
// retiring the current layout for the janitor. Constraints that name an
// index follow it.
func truncateLayout(desc *catalog.TableDescriptor, now int64) error {
	next := desc.NextIndexID
	if next <= desc.LivePrimaryIndex() {
		next = desc.LivePrimaryIndex() + 1
	}
	if next < rowenc.PrimaryIndexID+1 {
		next = rowenc.PrimaryIndexID + 1
	}
	for _, idx := range desc.Indexes {
		if next <= idx.ID {
			next = idx.ID + 1
		}
	}
	retired := catalog.RetiredLayout{
		PrimaryIndexID: desc.LivePrimaryIndex(),
		Buckets:        desc.ShardBuckets,
		RetiredAt:      now,
	}
	desc.PrimaryIndex = next
	next++
	renumbered := map[uint64]uint64{}
	for i := range desc.Indexes {
		retired.IndexIDs = append(retired.IndexIDs, desc.Indexes[i].ID)
		renumbered[desc.Indexes[i].ID] = next
		desc.Indexes[i].ID = next
		next++
	}
	for i := range desc.Constraints {
		if id, ok := renumbered[desc.Constraints[i].IndexID]; ok {
			desc.Constraints[i].IndexID = id
		}
	}
	desc.NextIndexID = next
	desc.RetiredLayouts = append(desc.RetiredLayouts, retired)
	return nil
}

// restartOwnedSequences resets the sequences the table's columns own to
// their START values (TRUNCATE ... RESTART IDENTITY).
func (s *Session) restartOwnedSequences(ctx context.Context, txn *kvclient.Txn, desc *catalog.TableDescriptor) error {
	for _, c := range desc.Columns {
		if c.SequenceID == 0 {
			continue
		}
		sd, err := catalog.ReadSequence(ctx, txn, c.SequenceID)
		if err != nil || sd == nil {
			continue
		}
		if err := txn.Put(ctx, keys.SequenceValueKey(sd.ID), []byte(strconv.FormatInt(sd.Start-sd.Increment, 10))); err != nil {
			return err
		}
		s.dropSeqBlock(sd.ID)
	}
	return nil
}

// tableExists reports whether a table resolves for the session (ALTER
// TABLE IF EXISTS on the multi-transaction forms, which look the table up
// themselves).
func (s *Session) tableExists(ctx context.Context, name string) bool {
	found := false
	_ = s.db.RunTxn(ctx, "table-exists", func(ctx context.Context, txn *kvclient.Txn) error {
		d, err := s.lookup(ctx, txn, name)
		found = err == nil && d != nil && d.Virtual == ""
		return nil
	})
	return found
}
