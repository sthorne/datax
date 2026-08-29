package sql

import (
	"context"

	"github.com/sthorne/datax/pkg/kvclient"
	"github.com/sthorne/datax/pkg/sql/catalog"
	"github.com/sthorne/datax/pkg/sql/parser"
	"github.com/sthorne/datax/pkg/sql/rowenc"
)

// Online CREATE INDEX (three steps, driven by the session outside any
// explicit transaction — like PostgreSQL's CREATE INDEX CONCURRENTLY):
//
//  1. Publish the index in the write-only state and drain: once every
//     gateway's descriptor lease has adopted it, all new writes maintain
//     the index (the planner still ignores it).
//  2. Backfill from a full scan in its own transaction — every row that
//     committed before the backfill's snapshot is written by the backfill,
//     every row after it is maintained by its writer, so the union is
//     complete — then flip the index public in the same transaction.
//  3. Drain again so every gateway can plan with it.
//
// Remaining gap (issue #22): a transaction that BEGAN before step 1's
// drain, on another gateway, still writes with the descriptor it started
// with; statement-sized windows are closed, long-lived explicit
// transactions are not. Bounded, batched backfill is also future work.

// execCreateIndexOnline runs the state machine. The session has already
// rejected explicit-transaction contexts.
func (s *Session) execCreateIndexOnline(ctx context.Context, t *parser.CreateIndex) (*Result, *Error) {
	// Step 1: publish write-only.
	var indexID uint64
	err := s.db.RunTxn(ctx, "create-index-publish", func(ctx context.Context, txn *kvclient.Txn) error {
		shared, err := s.cat.Lookup(ctx, txn, t.Table)
		if err != nil {
			return err
		}
		desc := shared.Clone()
		if t.Name == "primary" {
			return newErrf(CodeSyntaxError, "index name %q is reserved", t.Name)
		}
		if _, exists := desc.Index(t.Name); exists {
			return newErrf(CodeSyntaxError, "index %q already exists", t.Name)
		}
		var colIDs []catalog.ColumnID
		seenCol := map[catalog.ColumnID]bool{}
		for _, name := range t.Columns {
			col, ok := desc.Col(name)
			if !ok {
				return newErrf(CodeUndefinedColumn, "column %q does not exist", name)
			}
			if seenCol[col.ID] {
				return newErrf(CodeSyntaxError, "duplicate column %q in index", name)
			}
			seenCol[col.ID] = true
			colIDs = append(colIDs, col.ID)
		}
		if desc.NextIndexID < rowenc.PrimaryIndexID+1 {
			desc.NextIndexID = rowenc.PrimaryIndexID + 1
		}
		idx := catalog.IndexDescriptor{
			ID: desc.NextIndexID, Name: t.Name, Unique: t.Unique,
			ColumnIDs: colIDs, State: catalog.IndexStateWriteOnly,
		}
		desc.NextIndexID++
		desc.Indexes = append(desc.Indexes, idx)
		indexID = idx.ID
		return s.cat.Update(ctx, txn, desc)
	})
	if err != nil {
		return nil, ToSQLError(err)
	}
	if err := s.cat.FinishDDL(ctx, t.Table); err != nil {
		return nil, ToSQLError(err)
	}

	// Step 2: backfill in its own transaction that touches ONLY row and
	// index data — never the descriptor. The descriptor key lives on range 1
	// and is read constantly (lease renewals), so a transaction writing it
	// is pushed and must refresh; refreshing a full-table read span fails
	// against any concurrent write, and the backfill would livelock. Left
	// alone, the backfill commits at its original timestamp: rows committed
	// before it are in its scan, rows after are maintained by their writers
	// (the drain above guarantees that), so the union is complete. A
	// concurrent update to a scanned row collides on the entry key
	// (WriteTooOld), forcing the refresh-and-retry that re-reads it.
	backfillErr := s.db.RunTxn(ctx, "create-index-backfill", func(ctx context.Context, txn *kvclient.Txn) error {
		shared, err := s.cat.Lookup(ctx, txn, t.Table)
		if err != nil {
			return err
		}
		desc := shared
		var idx *catalog.IndexDescriptor
		for i := range desc.Indexes {
			if desc.Indexes[i].ID == indexID {
				idx = &desc.Indexes[i]
			}
		}
		if idx == nil {
			return newErrf(CodeInternal, "index %q vanished during backfill", t.Name)
		}
		start, end := rowenc.PrimarySpan(desc.ID)
		kvs, err := txn.Scan(ctx, start, end, 0)
		if err != nil {
			return err
		}
		var wb kvclient.WriteBatch
		seen := map[string]bool{}
		for _, kv := range kvs {
			row, err := decodeFullRow(desc, kv.Key, kv.Value)
			if err != nil {
				return err
			}
			key, val, skip, err := rowenc.EncodeIndexEntry(desc, idx, row)
			if err != nil {
				return newErrf(CodeInternal, "index %q: %v", idx.Name, err)
			}
			if skip {
				if idx.Unique {
					return newErrf(CodeNotNullViolation, "cannot create unique index %q: a row has NULL in an indexed column", idx.Name)
				}
				continue
			}
			if idx.Unique {
				if seen[string(key)] {
					return newErrf(CodeUniqueViolation, "cannot create unique index %q: duplicate values exist", idx.Name)
				}
				seen[string(key)] = true
			}
			wb.Put(key, val)
		}
		return txn.RunBatch(ctx, &wb)
	})
	// Flip public in a separate small transaction (see above for why the
	// backfill cannot carry the descriptor write itself).
	if backfillErr == nil {
		backfillErr = s.db.RunTxn(ctx, "create-index-public", func(ctx context.Context, txn *kvclient.Txn) error {
			shared, err := s.cat.Lookup(ctx, txn, t.Table)
			if err != nil {
				return err
			}
			desc := shared.Clone()
			for i := range desc.Indexes {
				if desc.Indexes[i].ID == indexID {
					desc.Indexes[i].State = catalog.IndexStatePublic
					return s.cat.Update(ctx, txn, desc)
				}
			}
			return newErrf(CodeInternal, "index %q vanished during backfill", t.Name)
		})
	}
	if backfillErr != nil {
		// Abandon: remove the write-only index so writers stop maintaining
		// it, then surface the original failure.
		_ = s.db.RunTxn(ctx, "create-index-abandon", func(ctx context.Context, txn *kvclient.Txn) error {
			shared, err := s.cat.Lookup(ctx, txn, t.Table)
			if err != nil {
				return err
			}
			desc := shared.Clone()
			kept := desc.Indexes[:0]
			for _, ix := range desc.Indexes {
				if ix.ID != indexID {
					kept = append(kept, ix)
				}
			}
			desc.Indexes = kept
			return s.cat.Update(ctx, txn, desc)
		})
		_ = s.cat.FinishDDL(ctx, t.Table)
		return nil, ToSQLError(backfillErr)
	}

	// Step 3: drain adoption of the public index.
	if err := s.cat.FinishDDL(ctx, t.Table); err != nil {
		return nil, ToSQLError(err)
	}
	return &Result{Tag: "CREATE INDEX"}, nil
}

// ddlTableName names the table a committed DDL statement changed (empty
// for non-DDL); the session drains lease adoption for it after commit.
func ddlTableName(stmt parser.Statement) string {
	switch t := stmt.(type) {
	case *parser.CreateTable:
		return t.Name
	case *parser.AlterTable:
		return t.Table
	case *parser.DropTable:
		return t.Name
	}
	return ""
}
