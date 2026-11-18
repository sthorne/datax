package sql

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"github.com/sthorne/datax/pkg/kvclient"
	"github.com/sthorne/datax/pkg/sql/catalog"
	"github.com/sthorne/datax/pkg/sql/parser"
	"github.com/sthorne/datax/pkg/sql/rowenc"
	"github.com/sthorne/datax/pkg/sql/types"
	"github.com/sthorne/datax/pkg/util/log"
	"github.com/sthorne/datax/pkg/version"
)

// ALTER TABLE with several actions, and ADD COLUMN with a volatile
// default (issue #364).
//
//	ALTER TABLE t ADD COLUMN a INT, DROP COLUMN b, ALTER COLUMN c SET DEFAULT 1;
//	ALTER TABLE t ADD COLUMN id UUID NOT NULL DEFAULT gen_random_uuid() UNIQUE;
//
// The actions apply in written order, as one schema change. When every
// action is a change to the descriptor alone (ADD COLUMN with no default
// or one evaluated once, DROP COLUMN, SET / DROP DEFAULT, DROP NOT NULL,
// DROP CONSTRAINT, a widening ALTER COLUMN TYPE, row security), they
// are applied one after another to one copy of the descriptor and
// written once: one descriptor version, one drain, in the statement's
// transaction (an explicit block's too).
//
// Some actions need the table's rows: ADD CONSTRAINT validates them and
// builds an index, SET NOT NULL and VALIDATE CONSTRAINT check them,
// ALTER COLUMN TYPE rewrites them, and ADD COLUMN with a volatile
// default (or NOT NULL and no default, or SERIAL, or a constraint of
// its own) evaluates the default for each of them. A statement with any
// of these runs online, outside a transaction block, in the shape of
// CREATE INDEX:
//
//  1. check: every action is applied, in order, to a copy of the
//     descriptor that is thrown away — a name that does not exist, a
//     duplicate, a type that cannot convert fails the statement here,
//     before anything is published.
//  2. publish: in one transaction, the pieces that need the rows are
//     added in their write-only states, the way each does alone: a new
//     constraint (checked by new writes) and its index (maintained by
//     writers, unused by readers), SET NOT NULL's flag, ALTER COLUMN
//     TYPE's shadow column — and every column the statement adds, as a
//     hidden column under a placeholder name (catalog.Column.Adding),
//     which INSERT fills from its default and nothing reads. The other
//     actions wait. The descriptor records the statement and its plan
//     (catalog.PendingAlter). Drain.
//  3. work, in bounded chunks of rows each in a transaction of its own,
//     as of a boundary taken after the drain: the backfill of the new
//     columns with volatile defaults (backfillColumns), the rewrites of
//     the retyped columns, the index builds, the NOT NULL checks, the
//     constraint validations.
//  4. flip: in one transaction, the actions are applied again in
//     written order to the descriptor as it now is — the pieces of step
//     2 become public (the new columns take their names), and the
//     waiting actions are applied as written. Drain.
//
// A failure at step 3 or 4 removes everything step 2 published, so the
// statement leaves nothing applied. A combination that cannot be made
// one change this way is refused with 0A000, naming the action.
//
// A new column's existing rows read its default as PostgreSQL gives it.
// A constant, or an expression without volatile functions (now(),
// current_user, 1 + 2), is evaluated once, by the ALTER, and the rows
// that predate the column read that value (catalog.Column.FillDefault):
// no row is rewritten. A volatile expression (random(),
// gen_random_uuid(), clock_timestamp(), nextval) is evaluated for each
// existing row by the backfill, with now() inside it fixed at the
// ALTER's statement time for every row.
//
// The backfill is crash-safe and resumable. Each chunk is a serializable
// transaction that re-reads its rows and fills only those that lack the
// column, so it is idempotent; rows written after the drain have the
// column already (INSERT fills it; UPDATE keeps what the row had, and a
// row that lacks it is filled by the chunk that reads it, before or
// after the update, whichever commits second). If the gateway running
// the statement stops, the descriptor keeps the write-only pieces and
// the plan: the table works as before, its writers keep the pieces up
// to date, and running the same statement again picks the plan up,
// redoes the steps (a resumed backfill skips the rows already filled),
// and flips.

// alterEffects is what applying ALTER TABLE actions to a descriptor
// leaves for the transaction to do besides writing it: the referenced
// tables a foreign key change touched, the indexes to wipe once it has
// committed, the sequences of dropped columns, notices and audit
// records.
type alterEffects struct {
	parents  map[uint64]*catalog.TableDescriptor
	wipes    []indexWipe
	dropSeqs []uint64
	notices  []string
	audits   [][3]string
	// dry: the descriptor is a throwaway copy (the online check): no
	// sequence is created, no audit record kept.
	dry bool
}

func newAlterEffects() *alterEffects {
	return &alterEffects{parents: map[uint64]*catalog.TableDescriptor{}}
}

func (e *alterEffects) audit(kind, stmt, target string) {
	e.audits = append(e.audits, [3]string{kind, stmt, target})
}

func (e *alterEffects) notice(format string, args ...any) {
	e.notices = append(e.notices, fmt.Sprintf(format, args...))
}

// commitAlterEffects writes desc and what eff holds in txn, in the
// statement's transaction: the referenced tables are drained after it
// commits (noteDDL) and the indexes wiped (pendingWipes).
func (s *Session) commitAlterEffects(ctx context.Context, txn *kvclient.Txn, desc *catalog.TableDescriptor, eff *alterEffects) error {
	if err := s.writeAlterEffects(ctx, txn, desc, eff); err != nil {
		return err
	}
	for _, p := range eff.parents {
		if p.ID != desc.ID {
			s.noteDDL(p.Name)
		}
	}
	s.pendingWipes = append(s.pendingWipes, eff.wipes...)
	return nil
}

// writeAlterEffects writes desc, the referenced tables and the dropped
// sequences in txn, and logs the audit records.
func (s *Session) writeAlterEffects(ctx context.Context, txn *kvclient.Txn, desc *catalog.TableDescriptor, eff *alterEffects) error {
	for _, id := range eff.dropSeqs {
		if sd, err := catalog.ReadSequence(ctx, txn, id); err == nil && sd != nil {
			if err := catalog.DropSequence(ctx, txn, sd); err != nil {
				return err
			}
		}
	}
	for _, p := range eff.parents {
		if p.ID == desc.ID {
			continue
		}
		if err := s.cat.Update(ctx, txn, p); err != nil {
			return err
		}
	}
	if err := s.cat.Update(ctx, txn, desc); err != nil {
		return err
	}
	for _, a := range eff.audits {
		log.Audit(a[0], "stmt", a[1], "target", a[2], "principal", s.sessionUser, "role", s.user)
	}
	return nil
}

// alterCmds is the statement's actions in written order, a column's
// own constraints (ADD COLUMN c INT UNIQUE) following its ADD COLUMN as
// ADD CONSTRAINT actions of their own. The parsed statement is not
// changed (it may be cached).
func alterCmds(at *parser.AlterTable) []*parser.AlterTable {
	in := at.Cmds
	if in == nil {
		in = []*parser.AlterTable{at}
	}
	var out []*parser.AlterTable
	for _, c := range in {
		if c.AddCol == nil || len(c.AddCol.Constraints) == 0 {
			out = append(out, c)
			continue
		}
		add := *c
		def := *c.AddCol
		def.Constraints = nil
		add.AddCol = &def
		out = append(out, &add)
		for i := range c.AddCol.Constraints {
			cd := c.AddCol.Constraints[i]
			if len(cd.Columns) == 0 && cd.Kind != "check" {
				cd.Columns = []string{def.Name}
			}
			out = append(out, &parser.AlterTable{Table: c.Table, IfExists: c.IfExists, Only: c.Only, AddConstraint: &cd})
		}
	}
	return out
}

// alterActionName names one action for a message.
func alterActionName(t *parser.AlterTable) string {
	switch {
	case t.AddCol != nil:
		return fmt.Sprintf("ADD COLUMN %s", t.AddCol.Name)
	case t.DropCol != "":
		return fmt.Sprintf("DROP COLUMN %s", t.DropCol)
	case t.AddConstraint != nil:
		if t.AddConstraint.Name != "" {
			return fmt.Sprintf("ADD CONSTRAINT %s", t.AddConstraint.Name)
		}
		switch t.AddConstraint.Kind {
		case "foreign":
			return "ADD FOREIGN KEY"
		case "primary":
			return "ADD PRIMARY KEY"
		}
		return "ADD " + strings.ToUpper(t.AddConstraint.Kind)
	case t.DropConstraint != "":
		return fmt.Sprintf("DROP CONSTRAINT %s", t.DropConstraint)
	case t.ValidateConstraint != "":
		return fmt.Sprintf("VALIDATE CONSTRAINT %s", t.ValidateConstraint)
	case t.SetNotNull != "":
		return fmt.Sprintf("ALTER COLUMN %s SET NOT NULL", t.SetNotNull)
	case t.DropNotNull != "":
		return fmt.Sprintf("ALTER COLUMN %s DROP NOT NULL", t.DropNotNull)
	case t.AddIdentity != nil:
		return fmt.Sprintf("ALTER COLUMN %s ADD GENERATED AS IDENTITY", t.AddIdentity.Column)
	case t.SetDefault != nil:
		return fmt.Sprintf("ALTER COLUMN %s SET DEFAULT", t.SetDefault.Column)
	case t.DropDefault != "":
		return fmt.Sprintf("ALTER COLUMN %s DROP DEFAULT", t.DropDefault)
	case t.SetType != nil:
		return fmt.Sprintf("ALTER COLUMN %s TYPE", t.SetType.Column)
	case t.RowSecurity != "":
		return strings.ToUpper(t.RowSecurity) + " ROW LEVEL SECURITY"
	case t.SetOptions != nil:
		return "SET (...)"
	case t.ResetOptions != nil:
		return "RESET (...)"
	case t.Purge:
		return "PURGE"
	case t.RotateKey:
		return "ROTATE ENCRYPTION KEY"
	case t.SplitAt != nil:
		return "SPLIT AT"
	case t.UnsplitAt != nil || t.UnsplitAll:
		return "UNSPLIT"
	}
	return "an action"
}

// checkAlterCombination refuses the actions that cannot share an ALTER
// TABLE with others, before anything is read.
func checkAlterCombination(cmds []*parser.AlterTable) error {
	if len(cmds) < 2 {
		return nil
	}
	retyped := map[string]bool{}
	for _, c := range cmds {
		switch {
		case c.SetOptions != nil, c.ResetOptions != nil, c.Purge, c.RotateKey, c.SplitAt != nil, c.UnsplitAt != nil, c.UnsplitAll:
			return newErrf(CodeFeatureNotSupported, "ALTER TABLE ... %s cannot be combined with other actions: run it as a statement of its own", alterActionName(c))
		case c.SetType != nil:
			if retyped[c.SetType.Column] {
				return newErrf(CodeSyntaxError, "cannot alter type of column %q twice", c.SetType.Column)
			}
			retyped[c.SetType.Column] = true
		}
	}
	return nil
}

// addColNeedsRows reports whether ADD COLUMN def needs the table's rows:
// a volatile default to evaluate for each, NOT NULL with nothing to
// fill them with to check, a sequence to draw from, or a constraint.
func addColNeedsRows(def *parser.ColumnDef) bool {
	if len(def.Constraints) > 0 || def.Serial || def.Identity != "" {
		return true
	}
	if def.DefaultExpr != nil && exprIsVolatile(*def.DefaultExpr) {
		return true
	}
	return def.NotNull && (def.Default == nil || def.Default.Null) && def.DefaultExpr == nil
}

// alterActionNeedsRows reports whether one action needs the table's
// rows, and so a statement of its own transactions.
func alterActionNeedsRows(t *parser.AlterTable) bool {
	switch {
	case t.AddCol != nil:
		return addColNeedsRows(t.AddCol)
	case t.AddConstraint != nil, t.ValidateConstraint != "", t.SetNotNull != "", t.SetType != nil:
		return true
	}
	return false
}

// alterRunsOnline reports whether the statement goes to
// execAlterTableOnline: several actions, one of which needs the rows,
// or an ADD COLUMN alone that does. (The other actions that need the
// rows have online paths of their own when alone.)
func alterRunsOnline(at *parser.AlterTable) bool {
	if at.Cmds == nil {
		return at.AddCol != nil && addColNeedsRows(at.AddCol)
	}
	for _, c := range at.Cmds {
		if alterActionNeedsRows(c) {
			return true
		}
	}
	return false
}

// alterInBlockError is the refusal of an action that needs the rows
// inside a transaction block.
func alterInBlockError(cmds []*parser.AlterTable) *Error {
	for _, c := range cmds {
		if alterActionNeedsRows(c) {
			return newErrf(CodeActiveTransaction, "ALTER TABLE ... %s cannot run inside a transaction block: it reads the table's rows online, in transactions of its own", alterActionName(c))
		}
	}
	return newErrf(CodeActiveTransaction, "ALTER TABLE cannot run inside a transaction block")
}

// normalizeNextColumnID sets the next column ID of a descriptor written
// before it was kept.
func normalizeNextColumnID(desc *catalog.TableDescriptor) {
	if desc.NextColumnID != 0 {
		return
	}
	var max catalog.ColumnID
	for _, c := range desc.Columns {
		if c.ID > max {
			max = c.ID
		}
	}
	desc.NextColumnID = max + 1
}

// applyAlterAction applies one action to desc as it is when the
// statement is done: what the in-transaction path writes, what the
// online check runs, and what the online flip applies for the actions
// that waited.
func (s *Session) applyAlterAction(ctx context.Context, txn *kvclient.Txn, desc *catalog.TableDescriptor, t *parser.AlterTable, eff *alterEffects) error {
	switch {
	case t.RowSecurity != "":
		if eff.dry {
			return nil
		}
		return s.applyRowSecurity(desc, t.RowSecurity)
	case t.AddConstraint != nil:
		cd := t.AddConstraint
		if t.AddConstraintIfNotExists && hasConstraintNamed(desc, cd.Name) {
			eff.notice("constraint %q for relation %q already exists, skipping", cd.Name, desc.Name)
			return nil
		}
		if desc.Reshard != nil {
			return newErrf(CodeActiveTransaction, "cannot add a constraint to table %q while a re-shard is in progress", desc.Name)
		}
		if _, _, err := s.addConstraint(ctx, txn, desc, cd, catalog.IndexStatePublic, !cd.NotValid, eff.parents); err != nil {
			return err
		}
		eff.audit("constraint-ddl", "ADD CONSTRAINT", desc.Name)
	case t.ValidateConstraint != "":
		c, ok := desc.Constraint(t.ValidateConstraint)
		if !ok {
			return newErrf(CodeUndefinedObject, "constraint %q for table %q does not exist", t.ValidateConstraint, desc.Name)
		}
		c.Validated = true
	case t.SetNotNull != "":
		col, ok := desc.Col(t.SetNotNull)
		if !ok || col.Hidden {
			return newErrf(CodeUndefinedColumn, "column %q does not exist", t.SetNotNull)
		}
		setColumnNotNull(desc, col.ID, true)
	case t.DropConstraint != "":
		return s.applyDropConstraint(ctx, txn, desc, t, eff)
	case t.SetDefault != nil:
		if err := s.applySetDefault(ctx, txn, desc, t.SetDefault.Column, t.SetDefault); err != nil {
			return err
		}
		eff.audit("table-ddl", "ALTER COLUMN SET DEFAULT", desc.Name+"."+t.SetDefault.Column)
	case t.DropDefault != "":
		if err := s.applySetDefault(ctx, txn, desc, t.DropDefault, nil); err != nil {
			return err
		}
		eff.audit("table-ddl", "ALTER COLUMN DROP DEFAULT", desc.Name+"."+t.DropDefault)
	case t.DropNotNull != "":
		col, ok := desc.Col(t.DropNotNull)
		if !ok || col.Hidden {
			return newErrf(CodeUndefinedColumn, "column %q does not exist", t.DropNotNull)
		}
		if desc.IsPKCol(col.ID) {
			return newErrf(CodeInvalidColumnReference, "column %q is in the primary key and cannot be nullable", t.DropNotNull)
		}
		if col.Identity != "" {
			return newErrf(CodeSyntaxError, "column %q is an identity column and cannot be nullable", t.DropNotNull)
		}
		setColumnNotNull(desc, col.ID, false)
	case t.AddIdentity != nil:
		return s.applyAddIdentity(ctx, txn, desc, t.AddIdentity, eff)
	case t.AddCol != nil:
		_, err := s.applyAddColumn(ctx, txn, desc, t, eff, false)
		return err
	case t.DropCol != "":
		return s.applyDropColumn(ctx, txn, desc, t, eff)
	case t.SetType != nil:
		if err := s.requireV9("ALTER COLUMN TYPE"); err != nil {
			return err
		}
		if err := s.requireV10(t.SetType.Type); err != nil {
			return err
		}
		col, target, how, err := s.retypePlan(ctx, txn, desc, t.SetType)
		if err != nil {
			return err
		}
		switch how {
		case retypeWiden:
			retypeWidenInPlace(desc, col.ID, target)
		case retypeRewrite:
			retypeSetInPlace(desc, col.ID, target)
		}
		eff.audit("table-ddl", "ALTER COLUMN TYPE", desc.Name+"."+col.Name)
	case t.SetOptions != nil, t.ResetOptions != nil, t.Purge, t.RotateKey, t.SplitAt != nil, t.UnsplitAt != nil, t.UnsplitAll:
		return newErrf(CodeFeatureNotSupported, "ALTER TABLE ... %s cannot be combined with other actions: run it as a statement of its own", alterActionName(t))
	case t.RenameTo != "" || t.RenameCol != nil || t.RenameConstraint != nil:
		return newErrf(CodeSyntaxError, "RENAME cannot be combined with other ALTER TABLE actions")
	default:
		return newErrf(CodeSyntaxError, "ALTER TABLE requires an action (ADD, DROP, RENAME, ALTER COLUMN, SET)")
	}
	return nil
}

func setColumnNotNull(desc *catalog.TableDescriptor, id catalog.ColumnID, notNull bool) {
	for i := range desc.Columns {
		if desc.Columns[i].ID == id {
			desc.Columns[i].NotNull = notNull
		}
	}
}

// applyAddColumn adds ADD COLUMN's column to desc and returns its ID (0
// for ADD COLUMN IF NOT EXISTS of a column that exists). pending makes
// it the online statement's write-only column: a volatile default is
// marked for the backfill (Backfill) and a SERIAL column's sequence is
// created; the caller hides it.
//
// Outside the online statement, a default that needs evaluating for
// each existing row cannot be given (alterRunsOnline routed it), except
// to the online check (eff.dry), which only looks at the result.
func (s *Session) applyAddColumn(ctx context.Context, txn *kvclient.Txn, desc *catalog.TableDescriptor, t *parser.AlterTable, eff *alterEffects, pending bool) (catalog.ColumnID, error) {
	def := t.AddCol
	if def.PrimaryKey {
		return 0, newErrf(CodeFeatureNotSupported, "ADD COLUMN cannot add a primary key column")
	}
	if len(def.Constraints) > 0 {
		// Only the online statement takes them, as ADD CONSTRAINT
		// actions of their own (alterCmds).
		return 0, newErrf(CodeActiveTransaction, "ALTER TABLE ... ADD COLUMN %s with a constraint cannot run inside a transaction block: the constraint is checked online, in transactions of its own", def.Name)
	}
	if _, exists := desc.Col(def.Name); exists {
		if t.AddColIfNotExists {
			eff.notice("column %q of relation %q already exists, skipping", def.Name, desc.Name)
			return 0, nil
		}
		return 0, newErrf(CodeDuplicateColumn, "column %q of relation %q already exists", def.Name, desc.Name)
	}
	for _, c := range desc.Columns {
		if c.Adding == def.Name {
			return 0, newErrf(CodeObjectInUse, "column %q of table %q is being added by an ALTER TABLE in progress", def.Name, desc.Name)
		}
	}
	normalizeNextColumnID(desc)
	col := catalog.Column{
		ID: desc.NextColumnID, Name: def.Name, Type: def.Type, NotNull: def.NotNull,
		Precision: def.Precision, Scale: def.Scale,
		Width: def.Width, MaxLen: def.MaxLen, Char: def.Char, NoTZ: def.NoTZ, TimePrecision: def.TimePrecision,
	}
	s.stripTypeAttrs(&col)
	if err := s.requireV10(col.Type); err != nil {
		return 0, err
	}
	if err := s.resolveEnumColumn(ctx, txn, &col, def.TypeName); err != nil {
		return 0, err
	}
	if err := s.requireTextSearchTypes(col.Type); err != nil {
		return 0, err
	}
	rowsNeeded := addColNeedsRows(def)
	if rowsNeeded && !pending && !eff.dry {
		return 0, alterInBlockError([]*parser.AlterTable{t})
	}
	switch {
	case def.Serial || def.Identity != "":
		if err := s.requireV7("SERIAL and identity columns"); err != nil {
			return 0, err
		}
		if def.Type != types.Int {
			return 0, newErrf(CodeInvalidParameter, "identity column %q must be an integer type", def.Name)
		}
		if def.Default != nil || def.DefaultExpr != nil {
			return 0, newErrf(CodeSyntaxError, "column %q: both a DEFAULT and an identity are specified", def.Name)
		}
		col.Identity = def.Identity
		col.NotNull = col.NotNull || def.Identity != ""
		col.FillDefault, col.Backfill = true, pending
		seqName := desc.Name + "_" + col.Name + "_seq"
		col.DefaultExpr = fmt.Sprintf("nextval('%s')", seqName)
		if !eff.dry {
			sd := catalog.NewSequenceDescriptor(seqName, desc.DatabaseID)
			sd.OwnerTable, sd.OwnerColumn = desc.ID, col.ID
			sd.Owner = desc.Owner
			if err := s.applyDefaultPrivileges(ctx, txn, desc.DatabaseID, sd.Owner, "SEQUENCES", &sd.Privileges, &sd.GrantOptions); err != nil {
				return 0, err
			}
			if def.IdentitySeq != nil {
				if err := applySequenceOptions(sd, def.IdentitySeq, true); err != nil {
					return 0, err
				}
			}
			if err := catalog.CreateSequence(ctx, txn, sd); err != nil {
				return 0, ToSQLError(err)
			}
			col.SequenceID = sd.ID
		}
	case def.Default != nil && !def.Default.Null:
		d, cerr := coerceColumn(col, *def.Default)
		if cerr != nil {
			return 0, newErrf(CodeSyntaxError, "DEFAULT for column %q: %v", def.Name, cerr.Msg)
		}
		d, terr := enforceTypmod(col, d)
		if terr != nil {
			return 0, terr
		}
		// Fill-on-read: rows written before this ADD lack the column and
		// decode as the default; NULLs written afterwards are stored
		// explicitly so they stay NULL.
		col.Default, col.FillDefault = &d, true
	case def.DefaultExpr != nil:
		text, err := s.validateDefaultExpr(ctx, txn, def.Name, def.Type, *def.DefaultExpr)
		if err != nil {
			return 0, err
		}
		col.DefaultExpr, col.FillDefault = text, true
		if exprIsVolatile(*def.DefaultExpr) {
			// Each existing row gets a value of its own: the backfill.
			col.Backfill = pending
			break
		}
		// Evaluated once, now, as PostgreSQL does: the rows that predate
		// the column read this value (now() is the ALTER's time for
		// every one of them), and INSERT evaluates the expression.
		r, err := s.resolveValueExpr(ctx, txn, *def.DefaultExpr, nil)
		if err != nil {
			return 0, newErrf(CodeSyntaxError, "DEFAULT for column %q: %v", def.Name, err)
		}
		d, err := evalExpr(r, nil, nil, nil)
		if err != nil {
			return 0, newErrf(CodeSyntaxError, "DEFAULT for column %q: %v", def.Name, err)
		}
		d, cerr := coerceColumn(col, d)
		if cerr != nil {
			return 0, cerr
		}
		d, terr := enforceTypmod(col, d)
		if terr != nil {
			return 0, terr
		}
		if !d.Null {
			col.Default = &d
		} else if col.NotNull && !pending && !eff.dry {
			return 0, newErrf(CodeNotNullViolation, "column %q of relation %q contains null values", def.Name, desc.Name)
		}
	}
	desc.Columns = append(desc.Columns, col)
	desc.NextColumnID++
	eff.audit("table-ddl", "ADD COLUMN", desc.Name+"."+col.Name)
	return col.ID, nil
}

// applyDropColumn is DROP COLUMN's change to desc. Drops are lazy (the
// bytes stay in old row values and are skipped on decode) and refused
// for primary-key columns and columns a constraint, policy, view or the
// row expiry depends on; the indexes that read the column are dropped
// with it. Column IDs are never reused, so a drop-then-re-add cannot
// resurrect old values.
func (s *Session) applyDropColumn(ctx context.Context, txn *kvclient.Txn, desc *catalog.TableDescriptor, t *parser.AlterTable, eff *alterEffects) error {
	col, ok := desc.Col(t.DropCol)
	if !ok || col.Hidden {
		if t.DropColIfExists {
			eff.notice("column %q of relation %q does not exist, skipping", t.DropCol, desc.Name)
			return nil
		}
		return newErrf(CodeUndefinedColumn, "column %q does not exist", t.DropCol)
	}
	if desc.IsPKCol(col.ID) {
		return newErrf(CodeFeatureNotSupported, "cannot drop primary key column %q", t.DropCol)
	}
	if desc.RowTTL != nil && desc.RowTTL.ColumnID == col.ID {
		return newErrf(CodeDependentObjectsExist, "cannot drop column %q: the table's row expiry reads it (ALTER TABLE ... RESET (ttl) first)", t.DropCol)
	}
	if err := s.refuseIfViewed(ctx, txn, desc, "drop column "+t.DropCol); err != nil {
		return err
	}
	if uses, err := s.constraintUses(ctx, txn, desc, col.ID); err != nil {
		return err
	} else if len(uses) > 0 {
		return newErrf(CodeDependentObjectsExist, "cannot drop column %q: used by constraint %s (drop the constraint first)", t.DropCol, strings.Join(uses, ", "))
	}
	if uses := policyColumnUses(desc, col.Name); len(uses) > 0 {
		return newErrf(CodeDependentObjectsExist, "cannot drop column %q: used by policy %s (drop the policy first)", t.DropCol, strings.Join(uses, ", "))
	}
	for _, c := range desc.Columns {
		if c.RetypeFrom == col.ID {
			return newErrf(CodeObjectInUse, "cannot drop column %q: an ALTER COLUMN TYPE of it is in progress", t.DropCol)
		}
	}
	// The indexes that read the column — as a key, in an expression key
	// or in a predicate — are dropped with it, as in PostgreSQL. (One a
	// constraint owns was refused above.)
	var keptIndexes []catalog.IndexDescriptor
	var wipes []indexWipe
	for _, idx := range desc.Indexes {
		if !indexColumnUses(&idx, col) {
			keptIndexes = append(keptIndexes, idx)
			continue
		}
		if !idx.Public() {
			return newErrf(CodeObjectInUse, "cannot drop column %q: index %q, which uses it, is still being built", t.DropCol, idx.Name)
		}
		if desc.Reshard != nil {
			return newErrf(CodeObjectInUse, "cannot drop column %q of table %q while a re-shard is in progress: index %q uses it", t.DropCol, desc.Name, idx.Name)
		}
		w := indexWipe{tableID: desc.ID, indexID: idx.ID}
		if idx.IsVector() {
			w.vectorBuild = idx.Vector.Build
		}
		wipes = append(wipes, w)
	}
	desc.Indexes = keptIndexes
	eff.wipes = append(eff.wipes, wipes...)
	kept := desc.Columns[:0]
	for _, c := range desc.Columns {
		if c.ID != col.ID {
			kept = append(kept, c)
		}
	}
	desc.Columns = kept
	if col.SequenceID != 0 {
		eff.dropSeqs = append(eff.dropSeqs, col.SequenceID)
	}
	eff.audit("table-ddl", "DROP COLUMN", desc.Name+"."+col.Name)
	return nil
}

// renameColumnIn renames column col of desc to `to`, rewriting the
// stored text that names it: CHECK constraints, index expressions and
// predicates, policies.
func renameColumnIn(desc *catalog.TableDescriptor, col catalog.Column, to string) error {
	if err := renameIndexColumnRefs(desc, col, to); err != nil {
		return err
	}
	for i := range desc.Columns {
		if desc.Columns[i].ID == col.ID {
			desc.Columns[i].Name = to
		}
	}
	for i := range desc.Constraints {
		c := &desc.Constraints[i]
		if c.Kind != catalog.ConstraintCheck {
			continue
		}
		uses := false
		for _, id := range c.Columns {
			uses = uses || id == col.ID
		}
		if !uses {
			continue
		}
		text, err := parser.RenameColumnRefs(c.Expr, col.Name, to)
		if err != nil {
			return newErrf(CodeInternal, "constraint %q: %v", c.Name, err)
		}
		c.Expr = text
	}
	return renamePolicyColumn(desc, col.Name, to)
}

// ---- the online statement ------------------------------------------

// alterPlan is what an online ALTER TABLE published and has left to do,
// kept in the descriptor (catalog.PendingAlter.Plan) so a re-run of the
// statement can resume it.
type alterPlan struct {
	// StmtTime is the statement's time: now() in a volatile default is
	// this, for every existing row, however many runs the backfill takes.
	StmtTime int64       `json:"stmt_time"`
	Steps    []alterStep `json:"steps"`
}

// alterStep is one action's part of the plan, in written order.
type alterStep struct {
	// Deferred: a change to the descriptor alone, applied as written at
	// the flip. Skip: nothing to do (IF [NOT] EXISTS, a type the column
	// has already).
	Deferred bool `json:"deferred,omitempty"`
	Skip     bool `json:"skip,omitempty"`
	// Col is ADD COLUMN's write-only column, named Name once public, or
	// the column of SET NOT NULL. Backfill: its volatile default is
	// evaluated for every existing row. Sweep: every row is checked for
	// a NULL in it. NotNullSet: SET NOT NULL set the flag, which an
	// abandon clears again. Seq is a SERIAL column's sequence.
	Col        catalog.ColumnID `json:"col,omitempty"`
	Name       string           `json:"name,omitempty"`
	Backfill   bool             `json:"backfill,omitempty"`
	Sweep      bool             `json:"sweep,omitempty"`
	NotNullSet bool             `json:"not_null_set,omitempty"`
	Seq        uint64           `json:"seq,omitempty"`
	// Constraint is ADD CONSTRAINT's or VALIDATE CONSTRAINT's; Index the
	// write-only index to build; Validate: the existing rows are checked
	// against it; Parent the referenced table of a foreign key.
	Constraint uint64 `json:"constraint,omitempty"`
	Index      uint64 `json:"index,omitempty"`
	Validate   bool   `json:"validate,omitempty"`
	Parent     uint64 `json:"parent,omitempty"`
	// Added: ADD CONSTRAINT added Constraint (an abandon removes it).
	Added bool `json:"added,omitempty"`
	// Old and Shadow are ALTER COLUMN TYPE's column and its shadow.
	Old    catalog.ColumnID `json:"old,omitempty"`
	Shadow catalog.ColumnID `json:"shadow,omitempty"`
}

// alterDigest identifies a statement's actions, to recognise a re-run.
func alterDigest(table string, cmds []*parser.AlterTable) string {
	// ONLY and IF EXISTS change nothing about what the statement does.
	plain := make([]parser.AlterTable, len(cmds))
	for i, c := range cmds {
		plain[i] = *c
		plain[i].Only, plain[i].IfExists, plain[i].Table = false, false, ""
	}
	raw, err := json.Marshal(struct {
		Table string
		Cmds  []parser.AlterTable
	}{table, plain})
	if err != nil {
		raw = []byte(fmt.Sprintf("%s %p", table, cmds)) // never matches a re-run
	}
	sum := sha256.Sum256(raw)
	return hex.EncodeToString(sum[:])
}

// pendingColumnName is the placeholder name of a write-only column.
func pendingColumnName(id catalog.ColumnID) string { return fmt.Sprintf("__adding_%d", id) }

// columnDisplayName is the name a message gives a column: a write-only
// column's own name, not its placeholder.
func columnDisplayName(c catalog.Column) string {
	if c.Adding != "" {
		return c.Adding
	}
	return c.Name
}

// execAlterTableOnline runs an ALTER TABLE whose actions need the
// table's rows (see the file comment). The session has refused it
// inside a transaction block.
func (s *Session) execAlterTableOnline(ctx context.Context, at *parser.AlterTable) (*Result, *Error) {
	cmds := alterCmds(at)
	if err := checkAlterCombination(cmds); err != nil {
		return nil, ToSQLError(err)
	}
	if _, bare := catalog.SplitTableName(at.Table); catalog.IsSystemTable(bare) && !s.system {
		return nil, newErrf(CodeInsufficientPriv, "table %q belongs to the cluster: only ALTER TABLE ... SET (retention | shards) is allowed", at.Table)
	}
	for _, c := range cmds {
		if c.AddCol != nil && s.db.ClusterVersion() < version.V22 && addColNeedsRows(c.AddCol) {
			return nil, newErrf(CodeFeatureNotSupported, "ALTER TABLE ... %s needs cluster version %s: finalize the upgrade with `datax debug upgrade` first", alterActionName(c), version.V22)
		}
	}
	if serr := s.checkTableOwnerNoTxn(ctx, at.Table); serr != nil {
		return nil, serr
	}
	digest := alterDigest(at.Table, cmds)
	var plan alterPlan
	var parents []string
	var notices []string
	var wipes []indexWipe
	done, resumed := false, false
	err := s.db.RunTxn(ctx, "alter-table-publish", func(ctx context.Context, txn *kvclient.Txn) error {
		plan, parents, notices, wipes, done, resumed = alterPlan{}, nil, nil, nil, false, false
		shared, err := s.lookup(ctx, txn, at.Table)
		if err != nil {
			return err
		}
		if err := mustBeReal(shared); err != nil {
			return err
		}
		desc := shared.Clone()
		normalizeNextColumnID(desc)
		if desc.Alter != nil {
			if desc.Alter.Statement != digest {
				return newErrf(CodeObjectInUse, "table %q has another ALTER TABLE in progress, or one that was interrupted: run that statement again to finish it", desc.Name)
			}
			// A re-run of an interrupted statement (or one still
			// running elsewhere): resume its plan.
			if err := json.Unmarshal(desc.Alter.Plan, &plan); err != nil {
				return newErrf(CodeInternal, "table %q: the plan of its ALTER TABLE in progress: %v", desc.Name, err)
			}
			resumed = true
			return nil
		}
		if desc.Reshard != nil {
			return newErrf(CodeObjectInUse, "cannot alter table %q while a re-shard is in progress", desc.Name)
		}
		// 1. check: the whole statement against a throwaway copy.
		dry := desc.Clone()
		deff := newAlterEffects()
		deff.dry = true
		for _, c := range cmds {
			if err := s.applyAlterAction(ctx, txn, dry, c, deff); err != nil {
				return err
			}
		}
		// 2. publish.
		eff := newAlterEffects()
		plan = alterPlan{StmtTime: s.statementTime()}
		for i, c := range cmds {
			step, err := s.publishAlterAction(ctx, txn, desc, c, cmds[:i], eff)
			if err != nil {
				return err
			}
			plan.Steps = append(plan.Steps, step)
		}
		notices = eff.notices
		online := false
		for _, st := range plan.Steps {
			online = online || !(st.Skip || st.Deferred)
		}
		if !online {
			// Nothing needs the rows after all: the statement is the
			// changes to the descriptor, applied now in this transaction.
			desc = shared.Clone()
			normalizeNextColumnID(desc)
			eff = newAlterEffects()
			for _, c := range cmds {
				if err := s.applyAlterAction(ctx, txn, desc, c, eff); err != nil {
					return err
				}
			}
			notices, wipes, done = eff.notices, eff.wipes, true
			for _, p := range eff.parents {
				if p.ID != desc.ID {
					parents = append(parents, p.Name)
				}
			}
			return s.writeAlterEffects(ctx, txn, desc, eff)
		}
		if err := checkAlterRetypeUses(desc, cmds, plan); err != nil {
			return err
		}
		// The new columns go write-only under their placeholders.
		for _, st := range plan.Steps {
			if st.Name == "" || st.Skip {
				continue
			}
			col, _ := desc.ColByID(st.Col)
			if err := renameColumnIn(desc, col, pendingColumnName(col.ID)); err != nil {
				return err
			}
			for i := range desc.Columns {
				if desc.Columns[i].ID == col.ID {
					desc.Columns[i].Hidden, desc.Columns[i].Adding = true, st.Name
				}
			}
		}
		raw, err := json.Marshal(plan)
		if err != nil {
			return err
		}
		desc.Alter = &catalog.PendingAlter{Statement: digest, Plan: raw}
		for _, p := range eff.parents {
			if p.ID != desc.ID {
				parents = append(parents, p.Name)
			}
		}
		eff.audits = nil // logged once the statement is done
		return s.writeAlterEffects(ctx, txn, desc, eff)
	})
	if err != nil {
		return nil, ToSQLError(err)
	}
	if done {
		if err := s.drainAlter(ctx, at.Table, parents); err != nil {
			return nil, ToSQLError(err)
		}
		s.pendingWipes = append(s.pendingWipes, wipes...)
		s.runPendingWipes(ctx)
		return &Result{Tag: "ALTER TABLE", Notices: notices}, nil
	}
	// A resumed run drains too: the run it resumes may not have.
	if err := s.drainAlter(ctx, at.Table, parents); err != nil {
		s.abandonAlter(ctx, at.Table, digest)
		return nil, ToSQLError(err)
	}
	// 3. work.
	if err := s.runAlterWork(ctx, at.Table, plan); err != nil {
		if errors.Is(err, ErrTestingAlterInterrupted) {
			return nil, newErrf(CodeQueryCanceled, "%v", err)
		}
		s.abandonAlter(ctx, at.Table, digest)
		return nil, ToSQLError(err)
	}
	// 4. flip.
	var flipParents []string
	var flipWipes []indexWipe
	alreadyDone := false
	err = s.db.RunTxn(ctx, "alter-table-flip", func(ctx context.Context, txn *kvclient.Txn) error {
		flipParents, flipWipes, alreadyDone = nil, nil, false
		shared, err := s.lookup(ctx, txn, at.Table)
		if err != nil {
			return err
		}
		if shared.Alter == nil || shared.Alter.Statement != digest {
			// Another run of the same statement finished it first.
			if alterPlanApplied(shared, plan) {
				alreadyDone = true
				return nil
			}
			return newErrf(CodeObjectInUse, "the ALTER TABLE of table %q was rolled back by another session", shared.Name)
		}
		desc := shared.Clone()
		eff := newAlterEffects()
		for i, c := range cmds {
			if err := s.flipAlterAction(ctx, txn, desc, c, plan.Steps[i], eff); err != nil {
				return err
			}
		}
		desc.Alter = nil
		for _, p := range eff.parents {
			if p.ID != desc.ID {
				flipParents = append(flipParents, p.Name)
			}
		}
		flipWipes = eff.wipes
		return s.writeAlterEffects(ctx, txn, desc, eff)
	})
	if err != nil {
		s.abandonAlter(ctx, at.Table, digest)
		return nil, ToSQLError(err)
	}
	if !alreadyDone {
		for _, st := range plan.Steps {
			if st.Parent != 0 {
				if name, ok := s.tableNameByID(ctx, st.Parent); ok {
					flipParents = append(flipParents, name)
				}
			}
		}
	}
	if err := s.drainAlter(ctx, at.Table, flipParents); err != nil {
		return nil, ToSQLError(err)
	}
	s.pendingWipes = append(s.pendingWipes, flipWipes...)
	s.runPendingWipes(ctx)
	if resumed {
		notices = append(notices, fmt.Sprintf("resumed the interrupted ALTER TABLE of table %q", at.Table))
	}
	return &Result{Tag: "ALTER TABLE", Notices: notices}, nil
}

// drainAlter waits until no gateway can still use the descriptors of
// the table and the tables named before their last change.
func (s *Session) drainAlter(ctx context.Context, table string, others []string) error {
	if err := s.cat.FinishDDLIn(ctx, s.database, table); err != nil {
		return err
	}
	seen := map[string]bool{}
	for _, name := range others {
		if seen[name] {
			continue
		}
		seen[name] = true
		if err := s.cat.FinishDDLIn(ctx, s.database, name); err != nil {
			return err
		}
	}
	return nil
}

// tableNameByID names the table of descriptor id, read in a transaction
// of its own.
func (s *Session) tableNameByID(ctx context.Context, id uint64) (string, bool) {
	var name string
	_ = s.db.RunTxn(ctx, "table-name", func(ctx context.Context, txn *kvclient.Txn) error {
		d, err := catalog.ReadTable(ctx, txn, id)
		if err == nil && d != nil {
			name = d.Name
		}
		return nil
	})
	return name, name != ""
}

// publishAlterAction is one action's step 2 on desc: the pieces that
// need the rows are added write-only; the rest waits for the flip.
// earlier are the actions written before it.
func (s *Session) publishAlterAction(ctx context.Context, txn *kvclient.Txn, desc *catalog.TableDescriptor, t *parser.AlterTable, earlier []*parser.AlterTable, eff *alterEffects) (alterStep, error) {
	var step alterStep
	switch {
	case t.AddCol != nil:
		def := t.AddCol
		if _, exists := desc.Col(def.Name); exists && !t.AddColIfNotExists {
			// The check passed, so an earlier action drops the column
			// that has the name: the new one would be visible beside it.
			return step, newErrf(CodeFeatureNotSupported, "ALTER TABLE ... ADD COLUMN %s cannot be combined with dropping a column of the same name when the statement reads the table's rows: run them as separate statements", def.Name)
		}
		id, err := s.applyAddColumn(ctx, txn, desc, t, eff, true)
		if err != nil {
			return step, err
		}
		if id == 0 {
			step.Skip = true
			return step, nil
		}
		col, _ := desc.ColByID(id)
		step.Col, step.Name, step.Backfill, step.Seq = id, def.Name, col.Backfill, col.SequenceID
		step.Sweep = col.NotNull && !col.Backfill && col.Default == nil
	case t.AddConstraint != nil:
		cd := t.AddConstraint
		if t.AddConstraintIfNotExists && hasConstraintNamed(desc, cd.Name) {
			eff.notice("constraint %q for relation %q already exists, skipping", cd.Name, desc.Name)
			step.Skip = true
			return step, nil
		}
		for _, e := range earlier {
			if cd.Name != "" && e.DropConstraint == cd.Name {
				return step, newErrf(CodeFeatureNotSupported, "ALTER TABLE ... ADD CONSTRAINT %s cannot be combined with dropping a constraint of the same name: run them as separate statements", cd.Name)
			}
		}
		c, parent, err := s.addConstraint(ctx, txn, desc, cd, catalog.IndexStateWriteOnly, false, eff.parents)
		if err != nil {
			return step, err
		}
		step.Constraint, step.Added = c.ID, true
		if c.Kind == catalog.ConstraintUnique || c.AutoIndex {
			step.Index = c.IndexID
		}
		step.Validate = !cd.NotValid && c.Kind != catalog.ConstraintUnique
		if parent != nil {
			step.Parent = parent.ID
		}
	case t.ValidateConstraint != "":
		c, ok := desc.Constraint(t.ValidateConstraint)
		if !ok {
			return step, newErrf(CodeUndefinedObject, "constraint %q for table %q does not exist", t.ValidateConstraint, desc.Name)
		}
		if c.Validated {
			step.Skip = true
			return step, nil
		}
		step.Constraint, step.Validate = c.ID, true
	case t.SetNotNull != "":
		col, ok := desc.Col(t.SetNotNull)
		if !ok || col.Hidden {
			return step, newErrf(CodeUndefinedColumn, "column %q does not exist", t.SetNotNull)
		}
		if col.NotNull {
			step.Skip = true
			return step, nil
		}
		setColumnNotNull(desc, col.ID, true)
		step.Col, step.Sweep, step.NotNullSet = col.ID, true, true
	case t.SetType != nil:
		if err := s.requireV9("ALTER COLUMN TYPE"); err != nil {
			return step, err
		}
		if err := s.requireV10(t.SetType.Type); err != nil {
			return step, err
		}
		for _, e := range earlier {
			if e.AddCol != nil && e.AddCol.Name == t.SetType.Column {
				return step, newErrf(CodeFeatureNotSupported, "ALTER TABLE ... ALTER COLUMN %s TYPE cannot be combined with adding the column: give ADD COLUMN the type", t.SetType.Column)
			}
		}
		col, target, how, err := s.retypePlan(ctx, txn, desc, t.SetType)
		if err != nil {
			return step, err
		}
		switch how {
		case retypeNoop:
			step.Skip = true
		case retypeWiden:
			step.Deferred = true
		default:
			for _, c := range desc.Columns {
				if c.RetypeFrom == col.ID {
					return step, newErrf(CodeObjectInUse, "another ALTER COLUMN TYPE of column %q is in progress", col.Name)
				}
			}
			step.Old, step.Shadow = col.ID, addRetypeShadow(desc, col, target)
		}
	default:
		step.Deferred = true
	}
	return step, nil
}

// checkAlterRetypeUses refuses a constraint the statement adds over a
// column it retypes: the constraint would name the column the rewrite
// replaces.
func checkAlterRetypeUses(desc *catalog.TableDescriptor, cmds []*parser.AlterTable, plan alterPlan) error {
	retyped := map[catalog.ColumnID]bool{}
	for _, st := range plan.Steps {
		if st.Old != 0 {
			retyped[st.Old] = true
		}
	}
	if len(retyped) == 0 {
		return nil
	}
	for i, st := range plan.Steps {
		if st.Constraint == 0 || cmds[i].AddConstraint == nil {
			continue
		}
		c, ok := desc.ConstraintByID(st.Constraint)
		if !ok {
			continue
		}
		for _, id := range c.Columns {
			if retyped[id] {
				return newErrf(CodeFeatureNotSupported, "ALTER TABLE ... %s cannot be combined with ALTER COLUMN TYPE of a column it uses: run them as separate statements", alterActionName(cmds[i]))
			}
		}
	}
	return nil
}

// runAlterWork is step 3: everything that reads the rows, in an order
// where each piece sees the ones before it done.
func (s *Session) runAlterWork(ctx context.Context, table string, plan alterPlan) error {
	var fill []catalog.ColumnID
	for _, st := range plan.Steps {
		if st.Backfill && !st.Skip {
			fill = append(fill, st.Col)
		}
	}
	if len(fill) > 0 {
		if err := s.backfillColumns(ctx, table, fill, plan.StmtTime); err != nil {
			return err
		}
	}
	for _, st := range plan.Steps {
		if st.Shadow == 0 {
			continue
		}
		var tableID uint64
		if err := s.db.RunTxn(ctx, "alter-table-id", func(ctx context.Context, txn *kvclient.Txn) error {
			desc, err := s.lookup(ctx, txn, table)
			if err == nil {
				tableID = desc.ID
			}
			return err
		}); err != nil {
			return err
		}
		if err := s.retypeBackfill(ctx, table, tableID, st.Old, st.Shadow); err != nil {
			return err
		}
	}
	for _, st := range plan.Steps {
		if st.Index != 0 {
			if err := s.backfillIndex(ctx, table, st.Index); err != nil {
				return err
			}
		}
	}
	var sweep []catalog.ColumnID
	for _, st := range plan.Steps {
		if st.Sweep && !st.Skip {
			sweep = append(sweep, st.Col)
		}
	}
	if len(sweep) > 0 {
		if err := s.sweepRows(ctx, table, func(desc *catalog.TableDescriptor, row map[catalog.ColumnID]types.Datum) error {
			for _, id := range sweep {
				if rowVal(row, id).Null {
					col, _ := desc.ColByID(id)
					return newErrf(CodeNotNullViolation, "column %q of relation %q contains null values", columnDisplayName(col), desc.Name)
				}
			}
			return nil
		}); err != nil {
			return err
		}
	}
	for _, st := range plan.Steps {
		if st.Validate {
			if err := s.validateConstraintRows(ctx, table, st.Constraint); err != nil {
				return err
			}
		}
	}
	return nil
}

// flipAlterAction is one action's step 4 on desc.
func (s *Session) flipAlterAction(ctx context.Context, txn *kvclient.Txn, desc *catalog.TableDescriptor, t *parser.AlterTable, st alterStep, eff *alterEffects) error {
	if st.Skip {
		return nil
	}
	if st.Deferred {
		return s.applyAlterAction(ctx, txn, desc, t, eff)
	}
	vanished := func(what string) error {
		return newErrf(CodeInternal, "%s of the ALTER TABLE vanished before it finished", what)
	}
	switch {
	case t.AddCol != nil:
		col, ok := desc.ColByID(st.Col)
		if !ok || col.Adding == "" {
			return vanished("column " + st.Name)
		}
		if _, taken := desc.Col(st.Name); taken {
			return newErrf(CodeDuplicateColumn, "column %q of relation %q already exists", st.Name, desc.Name)
		}
		if err := renameColumnIn(desc, col, st.Name); err != nil {
			return err
		}
		for i := range desc.Columns {
			if desc.Columns[i].ID == col.ID {
				c := &desc.Columns[i]
				c.Hidden, c.Adding, c.Backfill = false, "", false
			}
		}
		eff.audit("table-ddl", "ADD COLUMN", desc.Name+"."+st.Name)
	case t.AddConstraint != nil, t.ValidateConstraint != "":
		c, ok := desc.ConstraintByID(st.Constraint)
		if !ok {
			return vanished("constraint")
		}
		if st.Index != 0 {
			idx, ok := desc.IndexByID(st.Index)
			if !ok {
				return vanished("index")
			}
			idx.State = catalog.IndexStatePublic
		}
		if st.Validate {
			c.Validated = true
		}
		if t.AddConstraint != nil {
			eff.audit("constraint-ddl", "ADD CONSTRAINT", desc.Name+"."+c.Name)
		}
	case t.SetNotNull != "":
		// By name: an ALTER COLUMN TYPE before it in the statement has
		// put its shadow in the column's place, the flag carried over.
		col, ok := desc.Col(t.SetNotNull)
		if !ok || col.Hidden || !col.NotNull {
			return vanished("NOT NULL of column " + t.SetNotNull)
		}
	case t.SetType != nil:
		if err := retypeSwapIDs(desc, st.Old, st.Shadow); err != nil {
			return err
		}
		eff.audit("table-ddl", "ALTER COLUMN TYPE", desc.Name+"."+t.SetType.Column)
	}
	return nil
}

// alterPlanApplied reports whether desc shows plan's pieces public: a
// finished run of the statement, as opposed to one rolled back.
func alterPlanApplied(desc *catalog.TableDescriptor, plan alterPlan) bool {
	for _, st := range plan.Steps {
		switch {
		case st.Skip:
		case st.Name != "":
			c, ok := desc.ColByID(st.Col)
			if !ok || c.Hidden {
				return false
			}
		case st.Constraint != 0 && st.Index != 0:
			if idx, ok := desc.IndexByID(st.Index); !ok || !idx.Public() {
				return false
			}
		case st.Constraint != 0:
			if _, ok := desc.ConstraintByID(st.Constraint); !ok {
				return false
			}
		case st.Shadow != 0:
			if _, ok := desc.ColByID(st.Shadow); !ok {
				return false
			}
		}
	}
	return true
}

// abandonAlter removes what the statement published, best effort: the
// write-only columns, constraints, indexes, shadows and sequences, and
// SET NOT NULL's flag. The rows keep the bytes of the removed columns,
// skipped on decode (column IDs are never reused), as after DROP COLUMN.
func (s *Session) abandonAlter(ctx context.Context, table, digest string) {
	ctx = context.WithoutCancel(ctx)
	var tableID uint64
	var indexes []uint64
	var parents []string
	_ = s.db.RunTxn(ctx, "alter-table-abandon", func(ctx context.Context, txn *kvclient.Txn) error {
		tableID, indexes, parents = 0, nil, nil
		shared, err := s.lookup(ctx, txn, table)
		if err != nil {
			return err
		}
		if shared.Alter == nil || shared.Alter.Statement != digest {
			return nil
		}
		var plan alterPlan
		if err := json.Unmarshal(shared.Alter.Plan, &plan); err != nil {
			return err
		}
		desc := shared.Clone()
		tableID = desc.ID
		eff := newAlterEffects()
		removeCol := func(id catalog.ColumnID) {
			kept := desc.Columns[:0]
			for _, c := range desc.Columns {
				if c.ID != id {
					kept = append(kept, c)
				}
			}
			desc.Columns = kept
		}
		for i := len(plan.Steps) - 1; i >= 0; i-- {
			st := plan.Steps[i]
			if st.Skip || st.Deferred {
				continue
			}
			if st.Added {
				if c, ok := desc.ConstraintByID(st.Constraint); ok {
					if _, err := s.removeConstraintIn(ctx, txn, desc, c, eff.parents); err != nil {
						return err
					}
				}
				if st.Index != 0 {
					indexes = append(indexes, st.Index)
				}
			}
			if st.Name != "" {
				removeCol(st.Col)
				if st.Seq != 0 {
					eff.dropSeqs = append(eff.dropSeqs, st.Seq)
				}
			}
			if st.NotNullSet {
				setColumnNotNull(desc, st.Col, false)
			}
			if st.Shadow != 0 {
				removeCol(st.Shadow)
			}
		}
		desc.Alter = nil
		for _, p := range eff.parents {
			if p.ID != desc.ID {
				parents = append(parents, p.Name)
			}
		}
		return s.writeAlterEffects(ctx, txn, desc, eff)
	})
	_ = s.drainAlter(ctx, table, parents)
	if tableID != 0 {
		for _, id := range indexes {
			s.wipeIndexEntries(ctx, tableID, id)
		}
	}
}

// TestingAlterBackfillChunk, when set, runs at the start of every
// chunk of an ADD COLUMN backfill, in its transaction; an error fails
// the chunk. ErrTestingAlterInterrupted fails it as a stopped gateway would:
// the statement returns without abandoning what it published.
var TestingAlterBackfillChunk func(table string) error

// ErrTestingAlterInterrupted, returned by TestingAlterBackfillChunk,
// stops an online ALTER TABLE where it is, leaving its write-only
// pieces and plan in the descriptor as a crash of its gateway would.
var ErrTestingAlterInterrupted = fmt.Errorf("alter table interrupted (testing)")

// backfillColumns fills the write-only columns ids of the rows that
// predate them, as of a boundary taken now, one bounded chunk per
// transaction (the shape of backfillIndex): each chunk re-reads its
// rows and evaluates the default for each row that lacks a column —
// volatile functions afresh per row, now() at stmtTime. A column that is
// NOT NULL refuses a NULL default (23502).
func (s *Session) backfillColumns(ctx context.Context, table string, ids []catalog.ColumnID, stmtTime int64) error {
	var cursor, end []byte
	if err := s.db.RunTxn(ctx, "alter-backfill-plan", func(ctx context.Context, txn *kvclient.Txn) error {
		desc, err := s.lookup(ctx, txn, table)
		if err != nil {
			return err
		}
		c, e := rowenc.PrimarySpanFor(desc)
		cursor, end = c, e
		return nil
	}); err != nil {
		return err
	}
	boundary := s.db.Clock().Now()
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
		if err := s.backfillColumnsChunk(ctx, table, ids, stmtTime, cursor, chunkEnd); err != nil {
			return err
		}
		if string(chunkEnd) == string(end) {
			return nil
		}
		cursor = chunkEnd
	}
}

// backfillColumnsChunk fills the rows in [start, end) in one
// serializable transaction.
func (s *Session) backfillColumnsChunk(ctx context.Context, table string, ids []catalog.ColumnID, stmtTime int64, start, end []byte) error {
	return s.db.RunTxn(ctx, "alter-backfill-chunk", func(ctx context.Context, txn *kvclient.Txn) error {
		desc, err := s.lookup(ctx, txn, table)
		if err != nil {
			return err
		}
		mini := &catalog.TableDescriptor{}
		for _, id := range ids {
			col, ok := desc.ColByID(id)
			if !ok || col.Adding == "" || !col.Backfill {
				return newErrf(CodeInternal, "ADD COLUMN state vanished during the backfill")
			}
			mini.Columns = append(mini.Columns, col)
		}
		// The defaults' stable parts (now(), current_user) are evaluated
		// once, as of the statement.
		saved := s.stmtNow
		s.stmtNow = stmtTime
		defaults, err := s.prepareDefaults(ctx, txn, mini, nil)
		s.stmtNow = saved
		if err != nil {
			return err
		}
		if TestingAlterBackfillChunk != nil {
			if err := TestingAlterBackfillChunk(table); err != nil {
				return err
			}
		}
		kvs, err := txn.Scan(ctx, start, end, 0)
		if err != nil {
			return err
		}
		var wb kvclient.WriteBatch
		for _, kv := range kvs {
			plain, err := s.rowSealer().OpenValue(ctx, desc, kv.Key, kv.Value)
			if err != nil {
				return err
			}
			row, err := rowenc.DecodeValue(desc, plain)
			if err != nil {
				return err
			}
			changed := false
			for i := range mini.Columns {
				col := mini.Columns[i]
				if _, has := row[col.ID]; has {
					continue // written after the publish, or filled already
				}
				d, err := s.defaultValue(ctx, txn, defaults, &col, nil)
				if err != nil {
					return err
				}
				if d.Null && col.NotNull {
					return newErrf(CodeNotNullViolation, "column %q of relation %q contains null values", col.Adding, desc.Name)
				}
				named := col
				named.Name = col.Adding
				if d, err = enforceTypmod(named, d); err != nil {
					return err
				}
				row[col.ID], changed = d, true
			}
			if !changed {
				continue
			}
			value, err := rowenc.EncodeValue(desc, row)
			if err != nil {
				return newErrf(CodeInvalidTextRepresentation, "%v", err)
			}
			stored, err := s.rowSealer().SealValue(ctx, desc, kv.Key, value)
			if err != nil {
				return err
			}
			wb.Put(kv.Key, stored)
		}
		return txn.RunBatch(ctx, &wb)
	})
}
