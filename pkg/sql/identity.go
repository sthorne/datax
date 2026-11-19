package sql

import (
	"context"

	"github.com/sthorne/datax/pkg/kvclient"
	"github.com/sthorne/datax/pkg/sql/catalog"
	"github.com/sthorne/datax/pkg/sql/parser"
	"github.com/sthorne/datax/pkg/sql/types"
	"github.com/sthorne/datax/pkg/sql/vtable"
)

// applyAddIdentity is ALTER TABLE ... ALTER COLUMN c ADD GENERATED
// {ALWAYS | BY DEFAULT} AS IDENTITY [(options)] — how pg_dump makes an
// identity column: the column, already NOT NULL and without a default,
// takes a new sequence (SEQUENCE NAME's, or <table>_<column>_seq) that
// it owns, and draws from it. The rows it has keep their values.
func (s *Session) applyAddIdentity(ctx context.Context, txn *kvclient.Txn, desc *catalog.TableDescriptor, ai *parser.AddIdentity, eff *alterEffects) error {
	if err := s.requireV7("identity columns"); err != nil {
		return err
	}
	col, ok := desc.Col(ai.Column)
	if !ok || col.Hidden {
		return newErrf(CodeUndefinedColumn, "column %q of relation %q does not exist", ai.Column, desc.Name)
	}
	if col.Type != types.Int {
		return newErrf(CodeInvalidParameter, "identity column type must be smallint, integer, or bigint")
	}
	if col.Identity != "" {
		return newErrf(CodeObjectNotInState, "column %q of relation %q is already an identity column", col.Name, desc.Name)
	}
	if ColumnHasDefault(&col) {
		return newErrf(CodeObjectNotInState, "column %q of relation %q already has a default value", col.Name, desc.Name)
	}
	if !col.NotNull {
		return newErrf(CodeObjectNotInState, "column %q of relation %q must be declared NOT NULL before identity can be added", col.Name, desc.Name)
	}
	seqName := desc.Name + "_" + col.Name + "_seq"
	if ai.Seq != nil && ai.Seq.SeqName != "" {
		// [schema.]name: the sequence lives with the table, in its
		// schema (PostgreSQL refuses another).
		seqName = catalog.BareName(ai.Seq.SeqName)
		if len(catalog.SplitName(ai.Seq.SeqName)) > 1 {
			target, err := s.createTarget(ctx, txn, ai.Seq.SeqName)
			if err != nil {
				return err
			}
			if target.DBID != desc.DatabaseID || target.Schema.ID != desc.SchemaID {
				return newErrf(CodeFeatureNotSupported, "the sequence of an identity column must be in its table's schema (%q)", ai.Seq.SeqName)
			}
		}
	}
	if eff.dry {
		return nil
	}
	sd := catalog.NewSequenceDescriptor(seqName, desc.DatabaseID)
	sd.SchemaID = desc.SchemaID
	sd.OwnerTable, sd.OwnerColumn = desc.ID, col.ID
	sd.Owner = desc.Owner
	if err := s.applyDefaultPrivileges(ctx, txn, desc.DatabaseID, sd.Owner, "SEQUENCES", &sd.Privileges, &sd.GrantOptions); err != nil {
		return err
	}
	if ai.Seq != nil {
		if err := applySequenceOptions(sd, ai.Seq, true); err != nil {
			return err
		}
	}
	if err := catalog.CreateSequence(ctx, txn, sd); err != nil {
		return ToSQLError(err)
	}
	text, bindings, err := s.serialDefault(ctx, txn, sd)
	if err != nil {
		return err
	}
	for i := range desc.Columns {
		if desc.Columns[i].ID == col.ID {
			desc.Columns[i].Identity = ai.Identity
			desc.Columns[i].SequenceID = sd.ID
			desc.Columns[i].DefaultExpr, desc.Columns[i].SeqBindings = text, bindings
		}
	}
	return nil
}

// ColumnHasDefault reports whether a column has a default expression
// or value (DROP DEFAULT's NULL marker is none).
func ColumnHasDefault(c *catalog.Column) bool {
	return vtable.ColumnDefault(c) != ""
}
