package sql

import (
	"context"
	"fmt"

	"github.com/sthorne/datax/pkg/keys"
	"github.com/sthorne/datax/pkg/kvclient"
	"github.com/sthorne/datax/pkg/sql/catalog"
	"github.com/sthorne/datax/pkg/sql/parser"
	"github.com/sthorne/datax/pkg/sql/rowenc"
	"github.com/sthorne/datax/pkg/sql/types"
	"github.com/sthorne/datax/pkg/util/log"
	"github.com/sthorne/datax/pkg/version"
)

// ALTER TABLE t ALTER COLUMN c TYPE new — an online rewrite in the shape
// of CREATE INDEX (issue #95; cluster version v9):
//
//  1. publish: a hidden shadow column of the new type, marked
//     RetypeFrom = the column, joins the descriptor. Every row write
//     from then on (rowenc.EncodeValue) derives the shadow's value from
//     the column's — INSERT, UPDATE, COPY, cascades, whoever writes.
//     Drain lease adoption: no gateway writes a row without the shadow.
//  2. backfill: sweep the rows as of a boundary in bounded chunks, each
//     in its own transaction, converting the column's value into the
//     shadow. A value the new type cannot hold fails the statement.
//  3. swap: the shadow takes the column's name, position, NOT NULL and
//     defaults (converted) and becomes visible; the old column is
//     dropped (its bytes are skipped on decode, as after DROP COLUMN).
//     Drain.
//
// A failure after step 1 removes the shadow again and drains, leaving
// the table as it was. The conversion is the type's cast: widening and
// text conversions only (retypeAllowed); a column an index, constraint,
// primary key, sequence or view depends on is refused.

// stripTypeAttrs drops the type modifiers this release added (integer
// widths, character lengths, CHAR padding, TIMESTAMP without time zone
// and TIMESTAMP(p)) from a column about to be created while the cluster
// version is below v9: a node still on the previous binary would ignore
// them, so until the upgrade is finalized the column keeps the earlier
// meaning (INT is 64-bit, VARCHAR(n) is unbounded, TIMESTAMP is
// TIMESTAMPTZ) — the documented behavior of every release before v9.
func (s *Session) stripTypeAttrs(col *catalog.Column) {
	if s.db.ClusterVersion() >= version.V9 {
		return
	}
	col.Width, col.MaxLen, col.Char, col.NoTZ, col.TimePrecision = 0, 0, false, false, 0
}

// retypeAllowed reports whether values of from convert to to without
// loss (the cast may still refuse a particular value, which fails the
// rewrite).
func retypeAllowed(from, to types.Family) bool {
	if from == to {
		return true
	}
	if to == types.String {
		return true
	}
	switch from {
	case types.Int:
		return to == types.Float || to == types.Decimal
	case types.Date:
		return to == types.Timestamp
	case types.Timestamp:
		return to == types.Time
	case types.Time:
		return to == types.IntervalFam
	case types.String:
		switch to {
		case types.Int, types.Float, types.Decimal, types.Bool, types.Timestamp, types.Date, types.Bytes, types.Uuid, types.Jsonb, types.IntervalFam, types.Time:
			return true
		}
	}
	return false
}

// execRetypeOnline runs the state machine. The session has already
// rejected explicit-transaction contexts and checked admin.
func (s *Session) execRetypeOnline(ctx context.Context, t *parser.AlterTable) (*Result, *Error) {
	st := t.SetType
	if err := s.requireV9("ALTER COLUMN TYPE"); err != nil {
		return nil, ToSQLError(err)
	}
	if err := s.requireV10(st.Type); err != nil {
		return nil, ToSQLError(err)
	}
	var shadowID, oldID catalog.ColumnID
	var tableID uint64
	changed, widened := false, false
	err := s.db.RunTxn(ctx, "retype-publish", func(ctx context.Context, txn *kvclient.Txn) error {
		shadowID, oldID, tableID, changed, widened = 0, 0, 0, false, false
		shared, err := s.lookup(ctx, txn, t.Table)
		if err != nil {
			return err
		}
		if err := mustBeReal(shared); err != nil {
			return err
		}
		desc := shared.Clone()
		col, ok := desc.Col(st.Column)
		if !ok || col.Hidden {
			return newErrf(CodeUndefinedColumn, "column %q does not exist", st.Column)
		}
		target := catalog.Column{
			Type: st.Type, Precision: st.Precision, Scale: st.Scale,
			Width: st.Width, MaxLen: st.MaxLen, Char: st.Char, NoTZ: st.NoTZ, TimePrecision: st.TimePrecision,
		}
		if col.Type == target.Type && col.Precision == target.Precision && col.Scale == target.Scale &&
			col.IntWidth() == target.IntWidth() && col.MaxLen == target.MaxLen && col.Char == target.Char && col.NoTZ == target.NoTZ &&
			col.TimePrecision == target.TimePrecision {
			return nil // nothing to do
		}
		if pureWidening(col, target) {
			// Every stored value stays valid: a descriptor write in this
			// transaction is the whole statement (INT4 → INT8, VARCHAR(10)
			// → VARCHAR(20) or TEXT, TIMESTAMP(3) → TIMESTAMP).
			for i := range desc.Columns {
				if desc.Columns[i].ID == col.ID {
					c := &desc.Columns[i]
					c.Width, c.MaxLen, c.Char, c.NoTZ, c.TimePrecision, c.Precision, c.Scale = target.Width, target.MaxLen, target.Char, target.NoTZ, target.TimePrecision, target.Precision, target.Scale
				}
			}
			widened = true
			return s.cat.Update(ctx, txn, desc)
		}
		if !retypeAllowed(col.Type, st.Type) {
			return newErrf(CodeFeatureNotSupported, "column %q cannot be converted from %s to %s in place: only widening and text conversions are supported (recreate the column)", col.Name, col.TypeSQL(), target.TypeSQL())
		}
		if desc.Reshard != nil {
			return newErrf(CodeObjectInUse, "cannot alter a column of table %q while a re-shard is in progress", desc.Name)
		}
		for _, c := range desc.Columns {
			if c.RetypeFrom != 0 {
				return newErrf(CodeObjectInUse, "another ALTER COLUMN TYPE on table %q is in progress", desc.Name)
			}
		}
		if desc.IsPKCol(col.ID) {
			return newErrf(CodeFeatureNotSupported, "cannot change the type of primary key column %q", col.Name)
		}
		if col.Identity != "" || col.SequenceID != 0 {
			return newErrf(CodeFeatureNotSupported, "cannot change the type of column %q: it draws from a sequence", col.Name)
		}
		for _, idx := range desc.Indexes {
			for _, id := range idx.ColumnIDs {
				if id == col.ID {
					return newErrf(CodeFeatureNotSupported, "cannot change the type of column %q: used by index %q (drop it first)", col.Name, idx.Name)
				}
			}
		}
		if uses, err := s.constraintUses(ctx, txn, desc, col.ID); err != nil {
			return err
		} else if len(uses) > 0 {
			return newErrf(CodeFeatureNotSupported, "cannot change the type of column %q: used by constraint %s (drop it first)", col.Name, uses[0])
		}
		if err := s.refuseIfViewed(ctx, txn, desc, "change the type of column "+col.Name); err != nil {
			return err
		}
		if desc.NextColumnID == 0 {
			for _, c := range desc.Columns {
				if c.ID >= desc.NextColumnID {
					desc.NextColumnID = c.ID + 1
				}
			}
		}
		shadow := target
		shadow.ID, shadow.Name, shadow.Hidden, shadow.RetypeFrom = desc.NextColumnID, fmt.Sprintf("__retype_%d", col.ID), true, col.ID
		desc.NextColumnID++
		desc.Columns = append(desc.Columns, shadow)
		shadowID, oldID, tableID, changed = shadow.ID, col.ID, desc.ID, true
		return s.cat.Update(ctx, txn, desc)
	})
	if err != nil {
		return nil, ToSQLError(err)
	}
	if !changed && !widened {
		return &Result{Tag: "ALTER TABLE"}, nil
	}
	if err := s.cat.FinishDDLIn(ctx, s.database, t.Table); err != nil {
		return nil, ToSQLError(err)
	}
	if widened {
		log.Audit("table-ddl", "stmt", "ALTER COLUMN TYPE", "target", t.Table+"."+st.Column, "type", st.Type.String(), "principal", s.user)
		return &Result{Tag: "ALTER TABLE"}, nil
	}

	abandon := func(cause error) (*Result, *Error) {
		cctx := context.WithoutCancel(ctx)
		_ = s.db.RunTxn(cctx, "retype-abandon", func(ctx context.Context, txn *kvclient.Txn) error {
			shared, err := s.lookup(ctx, txn, t.Table)
			if err != nil {
				return err
			}
			desc := shared.Clone()
			kept := desc.Columns[:0]
			for _, c := range desc.Columns {
				if c.ID != shadowID {
					kept = append(kept, c)
				}
			}
			desc.Columns = kept
			return s.cat.Update(ctx, txn, desc)
		})
		_ = s.cat.FinishDDLIn(cctx, s.database, t.Table)
		return nil, ToSQLError(cause)
	}

	// Step 2: convert the existing rows in chunks.
	if err := s.retypeBackfill(ctx, t.Table, tableID, oldID, shadowID); err != nil {
		return abandon(err)
	}

	// Step 3: swap.
	err = s.db.RunTxn(ctx, "retype-swap", func(ctx context.Context, txn *kvclient.Txn) error {
		shared, err := s.lookup(ctx, txn, t.Table)
		if err != nil {
			return err
		}
		desc := shared.Clone()
		oi, si := -1, -1
		for i, c := range desc.Columns {
			switch c.ID {
			case oldID:
				oi = i
			case shadowID:
				si = i
			}
		}
		if oi < 0 || si < 0 {
			return newErrf(CodeInternal, "column vanished during ALTER COLUMN TYPE")
		}
		old, shadow := desc.Columns[oi], desc.Columns[si]
		shadow.Name, shadow.Hidden, shadow.RetypeFrom = old.Name, false, 0
		shadow.NotNull, shadow.Comment = old.NotNull, old.Comment
		if old.Default != nil {
			d, cerr := old.Default.ConvertTo(shadow.Type)
			if cerr != nil {
				return newErrf(CodeInvalidTextRepresentation, "the DEFAULT of column %q cannot convert to %s: %v", old.Name, shadow.Type, cerr)
			}
			if d, cerr = enforceTypmod(shadow, d); cerr != nil {
				return cerr
			}
			shadow.Default, shadow.FillDefault = &d, old.FillDefault
		}
		shadow.DefaultExpr = old.DefaultExpr
		desc.Columns[oi] = shadow
		desc.Columns = append(desc.Columns[:si], desc.Columns[si+1:]...)
		return s.cat.Update(ctx, txn, desc)
	})
	if err != nil {
		return abandon(err)
	}
	if err := s.cat.FinishDDLIn(ctx, s.database, t.Table); err != nil {
		return nil, ToSQLError(err)
	}
	log.Audit("table-ddl", "stmt", "ALTER COLUMN TYPE", "target", t.Table+"."+st.Column, "type", st.Type.String(), "principal", s.user)
	return &Result{Tag: "ALTER TABLE"}, nil
}

// retypeBackfill rewrites every row as of a boundary so its shadow
// column carries the converted value (rows written after the boundary
// already do, through the write hook). Chunked like the index backfill;
// each chunk re-encodes its rows under the current descriptor.
func (s *Session) retypeBackfill(ctx context.Context, table string, tableID uint64, oldID, shadowID catalog.ColumnID) error {
	boundary := s.db.Clock().Now()
	var cursor, end keys.Key
	if err := s.db.RunTxn(ctx, "retype-plan", func(ctx context.Context, txn *kvclient.Txn) error {
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
		if err := s.db.RunTxn(ctx, "retype-chunk", func(ctx context.Context, txn *kvclient.Txn) error {
			desc, err := s.lookup(ctx, txn, table)
			if err != nil {
				return err
			}
			shadow, ok := desc.ColByID(shadowID)
			if !ok || shadow.RetypeFrom != oldID {
				return newErrf(CodeInternal, "ALTER COLUMN TYPE state vanished during the rewrite")
			}
			kvs, err := txn.Scan(ctx, cursor, chunkEnd, 0)
			if err != nil {
				return err
			}
			var wb kvclient.WriteBatch
			for _, kv := range kvs {
				row, err := rowenc.DecodeValue(desc, kv.Value)
				if err != nil {
					return err
				}
				if _, done := row[shadowID]; done {
					continue // written after publish: the hook filled it
				}
				src, has := row[oldID]
				if !has {
					continue // NULL: the shadow is NULL too
				}
				old, _ := desc.ColByID(oldID)
				conv, cerr := src.ConvertTo(shadow.Type)
				if cerr != nil {
					return newErrf(CodeInvalidTextRepresentation, "column %q: value %s cannot convert to %s: %v", old.Name, src.Text(), shadow.TypeSQL(), cerr)
				}
				named := shadow
				named.Name = old.Name
				if _, cerr := enforceTypmod(named, conv); cerr != nil {
					return cerr
				}
				value, err := rowenc.EncodeValue(desc, row) // the hook derives the shadow
				if err != nil {
					return newErrf(CodeInvalidTextRepresentation, "%v", err)
				}
				wb.Put(kv.Key, value)
			}
			return txn.RunBatch(ctx, &wb)
		}); err != nil {
			return err
		}
		if chunkEnd.Equal(end) {
			return nil
		}
		cursor = chunkEnd
	}
}
