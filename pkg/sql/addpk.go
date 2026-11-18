package sql

import (
	"context"

	"github.com/sthorne/datax/pkg/kvclient"
	"github.com/sthorne/datax/pkg/sql/catalog"
	"github.com/sthorne/datax/pkg/sql/parser"
)

// rewriteAddPrimaryKey turns ALTER TABLE t ADD [CONSTRAINT n] PRIMARY
// KEY (cols) on a table keyed by its hidden rowid — every table pg_dump's
// output creates, since it adds the key after loading the rows — into
// what datax can do in place: SET NOT NULL on each column that is not,
// and a UNIQUE constraint over them marked as the primary key. The rows
// stay keyed by the rowid; the catalogs show the index as the primary
// key and a foreign key without columns references it. Any other
// statement, and a table with a declared key (refused as PostgreSQL
// refuses a second key), is returned as it is.
func (s *Session) rewriteAddPrimaryKey(ctx context.Context, at *parser.AlterTable) (*parser.AlterTable, *Error) {
	cmds := alterCmds(at)
	found := false
	for _, c := range cmds {
		if c.AddConstraint != nil && c.AddConstraint.Kind == "primary" {
			found = true
		}
	}
	if !found {
		return at, nil
	}
	var desc *catalog.TableDescriptor
	look := func(ctx context.Context, txn *kvclient.Txn) error {
		d, err := s.lookup(ctx, txn, at.Table)
		if err != nil {
			return err
		}
		desc = d
		return nil
	}
	var err error
	if s.state == StateOpen {
		err = look(ctx, s.txn)
	} else {
		err = s.db.RunTxn(ctx, "add-primary-key", look)
	}
	if err != nil {
		if at.IfExists {
			return at, nil // the routing below reports the missing table
		}
		return nil, ToSQLError(err)
	}
	if desc.Virtual != "" || desc.IsView() || len(visiblePKIDs(desc)) > 0 {
		return at, nil
	}
	var out []*parser.AlterTable
	for _, c := range cmds {
		if c.AddConstraint == nil || c.AddConstraint.Kind != "primary" {
			out = append(out, c)
			continue
		}
		for _, name := range c.AddConstraint.Columns {
			if col, ok := desc.Col(name); ok && !col.Hidden && !col.NotNull {
				out = append(out, &parser.AlterTable{Table: at.Table, IfExists: at.IfExists, Only: at.Only, SetNotNull: name})
			}
		}
		cd := *c.AddConstraint
		cd.Kind, cd.PrimaryKey = "unique", true
		uc := *c
		uc.AddConstraint = &cd
		out = append(out, &uc)
	}
	if len(out) == 1 {
		return out[0], nil
	}
	r := &parser.AlterTable{Table: at.Table, IfExists: at.IfExists, Only: at.Only, Cmds: out}
	return r, nil
}
