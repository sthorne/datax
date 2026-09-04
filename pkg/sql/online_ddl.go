package sql

import (
	"context"

	"github.com/sthorne/datax/pkg/keys"
	"github.com/sthorne/datax/pkg/kvclient"
	"github.com/sthorne/datax/pkg/sql/catalog"
	"github.com/sthorne/datax/pkg/sql/parser"
	"github.com/sthorne/datax/pkg/sql/rowenc"
	"github.com/sthorne/datax/pkg/sql/types"
)

// Online CREATE INDEX (three steps, driven by the session outside any
// explicit transaction — like PostgreSQL's CREATE INDEX CONCURRENTLY):
//
//  1. Publish the index in the write-only state and drain: once every
//     gateway's descriptor lease has adopted it, all new writes maintain
//     the index (the planner still ignores it).
//  2. Backfill in bounded chunks planned against a frozen snapshot — see
//     backfillIndex. One whole-table transaction would restart forever
//     under concurrent writes (its refresh span is the whole table) and
//     its single giant raft entry would stall the range; a naive resume
//     cursor would chase a growing table's tail forever.
//  3. Flip the index public in its own small transaction (the descriptor
//     key is read constantly by lease renewals, so no data-carrying
//     transaction may write it), then drain again so every gateway plans
//     with it.
//
// Remaining gap: a transaction that BEGAN before step 1's drain, on
// another gateway, still writes with the descriptor it started with;
// statement-sized windows are closed, long-lived explicit transactions are
// not (documented in docs/sql.md).

// backfillChunkSize bounds each backfill chunk: rows scanned, KV batch
// size, raft entry size, and refresh-span width per transaction.
const backfillChunkSize = 64

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
		if desc.Reshard != nil {
			// The re-shard allocated shadow IDs for the indexes it saw at
			// publish time; an index appearing mid-flight would miss the
			// swap. The two state machines exclude each other.
			return newErrf(CodeActiveTransaction, "cannot create an index on table %q while a re-shard is in progress", t.Table)
		}
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
			if !types.IsIndexable(col.Type) {
				return newErrf(CodeFeatureNotSupported, "column %q of type %s cannot be indexed (no ordered key encoding)", name, col.Type)
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

	// Step 2: chunked backfill (see the file comment for the liveness and
	// correctness argument).
	backfillErr := s.backfillIndex(ctx, t.Table, indexID)
	// Step 3: flip public in its own small transaction.
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
		// it, wipe any entries committed chunks left behind (best effort —
		// index IDs are never reused, so leftovers are unreachable, just
		// wasted space), then surface the original failure.
		var tableID uint64
		_ = s.db.RunTxn(ctx, "create-index-abandon", func(ctx context.Context, txn *kvclient.Txn) error {
			shared, err := s.cat.Lookup(ctx, txn, t.Table)
			if err != nil {
				return err
			}
			desc := shared.Clone()
			tableID = desc.ID
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
		if tableID != 0 {
			s.wipeIndexEntries(ctx, tableID, indexID)
		}
		return nil, ToSQLError(backfillErr)
	}

	// Step 3: drain adoption of the public index.
	if err := s.cat.FinishDDL(ctx, t.Table); err != nil {
		return nil, ToSQLError(err)
	}
	return &Result{Tag: "CREATE INDEX"}, nil
}

// backfillIndex fills the index from the table's rows as of a fixed
// boundary timestamp, one bounded chunk per transaction.
//
// The planning sweep scans row KEYS inconsistently AT the boundary, so its
// row set is frozen — concurrent writers cannot extend it, and the sweep
// terminates no matter how fast the table grows (rows committed after the
// boundary were written by post-drain writers that maintain the index
// themselves). Each planned chunk then re-reads its own narrow key span in
// a serializable transaction and writes the entries: a concurrent delete
// or update inside the chunk invalidates its read and forces a rescan, so
// entries are always derived from rows that exist at the chunk's commit —
// no resurrection of concurrently deleted rows. Refresh spans stay one
// chunk wide, so writes elsewhere in the table never restart a chunk.
func (s *Session) backfillIndex(ctx context.Context, table string, indexID uint64) error {
	boundary := s.db.Clock().Now()

	var cursor, end keys.Key
	if err := s.db.RunTxn(ctx, "create-index-plan", func(ctx context.Context, txn *kvclient.Txn) error {
		desc, err := s.cat.Lookup(ctx, txn, table)
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
		// The chunks' serializable reads must cover the WHOLE primary span,
		// the tail beyond the last row and an empty table included: they
		// are what the timestamp cache remembers, and a writer that
		// planned under a lease the drain has since written off is pushed
		// above this backfill only for keys some chunk read (a pushed
		// commit then fails its lease deadline and re-plans with the
		// index; issue #110). A write into an unread tail would land in
		// the past, below the boundary, and never reach the index.
		chunkEnd := end
		if len(plan) == backfillChunkSize {
			chunkEnd = plan[len(plan)-1].Key.Next()
		}
		if err := s.backfillChunk(ctx, table, indexID, cursor, chunkEnd); err != nil {
			return err
		}
		if chunkEnd.Equal(end) {
			return nil
		}
		cursor = chunkEnd
	}
}

// backfillChunk writes the index entries for the rows currently in
// [start, end) in one serializable transaction.
func (s *Session) backfillChunk(ctx context.Context, table string, indexID uint64, start, end keys.Key) error {
	return s.db.RunTxn(ctx, "create-index-backfill", func(ctx context.Context, txn *kvclient.Txn) error {
		desc, err := s.cat.Lookup(ctx, txn, table)
		if err != nil {
			return err
		}
		var idx *catalog.IndexDescriptor
		for i := range desc.Indexes {
			if desc.Indexes[i].ID == indexID {
				idx = &desc.Indexes[i]
			}
		}
		if idx == nil {
			return newErrf(CodeInternal, "index vanished during backfill")
		}
		kvs, err := txn.Scan(ctx, start, end, 0)
		if err != nil {
			return err
		}
		var wb kvclient.WriteBatch
		seen := map[string]string{}
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
				// Duplicates within the chunk, then against entries earlier
				// chunks (or concurrent writers) committed: a unique entry's
				// value is the encoded primary key, so an existing entry
				// with a different value is another row with the same
				// indexed values.
				if prev, dup := seen[string(key)]; dup && prev != string(val) {
					return newErrf(CodeUniqueViolation, "cannot create unique index %q: duplicate values exist", idx.Name)
				}
				seen[string(key)] = string(val)
				existing, err := txn.Get(ctx, key)
				if err != nil {
					return err
				}
				if existing != nil && string(existing) != string(val) {
					return newErrf(CodeUniqueViolation, "cannot create unique index %q: duplicate values exist", idx.Name)
				}
			}
			wb.Put(key, val)
		}
		return txn.RunBatch(ctx, &wb)
	})
}

// wipeIndexEntries best-effort deletes an abandoned (or superseded)
// index's entries in batched, transactional chunks — a re-shard's old
// layout can hold hundreds of thousands of keys (per-key deletes would
// take minutes), and scanning inside the txn resolves any straggler
// dual-write intents instead of aborting on them. It sweeps in passes
// until a pass finds nothing (a statement in flight during an earlier
// pass may have committed behind the cursor); leftovers are unreachable
// garbage either way.
func (s *Session) wipeIndexEntries(ctx context.Context, tableID, indexID uint64) {
	WipeIndexEntries(ctx, s.db, tableID, indexID)
}

// WipeIndexEntries is the session-free form of the wipe: the re-shard
// janitor (pkg/server) reclaims retired layouts once their historical
// window lapses.
func WipeIndexEntries(ctx context.Context, db *kvclient.DB, tableID, indexID uint64) {
	const wipeChunk = 1024
	for pass := 0; pass < 5; pass++ {
		lo, hi := keys.TableIndexSpan(tableID, indexID)
		deleted := 0
		for {
			var n int
			var last keys.Key
			err := db.RunTxn(ctx, "wipe-index", func(ctx context.Context, txn *kvclient.Txn) error {
				kvs, err := txn.Scan(ctx, lo, hi, wipeChunk)
				if err != nil {
					return err
				}
				n = len(kvs)
				if n == 0 {
					return nil
				}
				last = kvs[n-1].Key.Clone()
				var wb kvclient.WriteBatch
				for _, kv := range kvs {
					wb.Delete(kv.Key)
				}
				return txn.RunBatch(ctx, &wb)
			})
			if err != nil {
				return
			}
			if n == 0 {
				break
			}
			deleted += n
			lo = last.Next()
		}
		if deleted == 0 {
			return
		}
	}
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
	case *parser.GrantRevoke:
		// Table grants ride the descriptor: drain leases like any DDL so
		// every gateway enforces the new privileges once the grant returns.
		return t.Table
	}
	return ""
}
