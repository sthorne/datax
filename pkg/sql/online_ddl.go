package sql

import (
	"context"

	"github.com/sthorne/datax/pkg/keys"
	"github.com/sthorne/datax/pkg/kvclient"
	"github.com/sthorne/datax/pkg/sql/catalog"
	"github.com/sthorne/datax/pkg/sql/inverted"
	"github.com/sthorne/datax/pkg/sql/parser"
	"github.com/sthorne/datax/pkg/sql/rowenc"
	"github.com/sthorne/datax/pkg/sql/types"
	"github.com/sthorne/datax/pkg/version"
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
// What step 1's drain has to establish is not that every gateway has
// ADOPTED the write-only index but that none can still write without it:
// a statement served from a gateway's lease cache is pinned to the
// descriptor it planned against and bounded only by that entry's
// expiration, which outlives the renewal that superseded it. The drain
// waits for that expiration too (issue #185, catalog.descLease), so a
// write that misses the index cannot commit above the boundary this
// backfill is about to take.

// invertedKindFor resolves CREATE INDEX's access method and operator
// class to an inverted index kind; nil is an ordinary index. USING GIN
// with no operator class takes the kind of the column's type.
func (s *Session) invertedKindFor(t *parser.CreateIndex, desc *catalog.TableDescriptor) (inverted.Kind, error) {
	switch t.Using {
	case "", "btree":
		for i, oc := range t.OpClasses {
			if oc != "" {
				return nil, newErrf(CodeFeatureNotSupported, "operator class %q is not supported for column %q of an ordinary index", oc, t.Columns[i])
			}
		}
		return nil, nil
	case "gin":
	default:
		return nil, newErrf(CodeFeatureNotSupported, "access method %q is not supported (btree and gin are)", t.Using)
	}
	if s.db.ClusterVersion() < version.V21 {
		return nil, newErrf(CodeFeatureNotSupported, "CREATE INDEX ... USING GIN needs cluster version %s (finalize the upgrade first)", version.V21)
	}
	if t.Unique {
		return nil, newErrf(CodeFeatureNotSupported, "a GIN index cannot be UNIQUE: its entries are the elements of a value, not the value")
	}
	if len(t.Columns) != 1 {
		return nil, newErrf(CodeFeatureNotSupported, "a GIN index takes exactly one column")
	}
	col, ok := desc.Col(t.Columns[0])
	if !ok {
		return nil, newErrf(CodeUndefinedColumn, "column %q does not exist", t.Columns[0])
	}
	opclass := ""
	if len(t.OpClasses) > 0 {
		opclass = t.OpClasses[0]
	}
	if opclass == "jsonb_path_ops" {
		// PostgreSQL's smaller class serves @> only. The one kind here
		// serves @> at least as selectively and the key operators too,
		// so a schema that names the class gets it.
		opclass = inverted.JSONBOps
	}
	if opclass != "" {
		kind, ok := inverted.Lookup(opclass)
		if !ok {
			return nil, newErrf(CodeUndefinedObject, "operator class %q does not exist for access method \"gin\"", opclass)
		}
		return kind, nil
	}
	for _, name := range inverted.Names() {
		if kind, _ := inverted.Lookup(name); kind.Family() == col.Type {
			return kind, nil
		}
	}
	return nil, newErrf(CodeFeatureNotSupported, "column %q of type %s cannot have a GIN index (JSONB columns can)", col.Name, col.Type)
}

// backfillChunkSize bounds each backfill chunk: rows scanned, KV batch
// size, raft entry size, and refresh-span width per transaction.
const backfillChunkSize = 64

// execCreateIndexOnline runs the state machine. The session has already
// rejected explicit-transaction contexts.
func (s *Session) execCreateIndexOnline(ctx context.Context, t *parser.CreateIndex) (*Result, *Error) {
	// Step 1: publish write-only.
	var indexID uint64
	exists := false
	err := s.db.RunTxn(ctx, "create-index-publish", func(ctx context.Context, txn *kvclient.Txn) error {
		exists = false
		shared, err := s.lookup(ctx, txn, t.Table)
		if err != nil {
			return err
		}
		if err := mustBeReal(shared); err != nil {
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
		if _, taken := desc.Index(t.Name); taken {
			if t.IfNotExists {
				exists = true
				return nil
			}
			return newErrf(CodeDuplicateObject, "index %q already exists", t.Name)
		}
		kind, err := s.invertedKindFor(t, desc)
		if err != nil {
			return err
		}
		var colIDs []catalog.ColumnID
		seenCol := map[catalog.ColumnID]bool{}
		for _, name := range t.Columns {
			col, ok := desc.Col(name)
			if !ok {
				return newErrf(CodeUndefinedColumn, "column %q does not exist", name)
			}
			if kind == nil && !types.IsIndexable(col.Type) {
				if col.Type == types.Jsonb {
					return newErrf(CodeFeatureNotSupported, "column %q of type %s has no ordered key encoding; index it with USING GIN", name, col.Type)
				}
				return newErrf(CodeFeatureNotSupported, "column %q of type %s cannot be indexed (no ordered key encoding)", name, col.Type)
			}
			if kind != nil && col.Type != kind.Family() {
				return newErrf(CodeFeatureNotSupported, "operator class %q does not index column %q of type %s", kind.Name(), name, col.Type)
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
		if kind != nil {
			idx.Inverted = kind.Name()
		}
		desc.NextIndexID++
		desc.Indexes = append(desc.Indexes, idx)
		indexID = idx.ID
		return s.cat.Update(ctx, txn, desc)
	})
	if err != nil {
		return nil, ToSQLError(err)
	}
	if exists {
		return &Result{Tag: "CREATE INDEX"}, nil
	}
	if err := s.cat.FinishDDLIn(ctx, s.database, t.Table); err != nil {
		return nil, ToSQLError(err)
	}

	// Step 2: chunked backfill (see the file comment for the liveness and
	// correctness argument).
	backfillErr := s.backfillIndex(ctx, t.Table, indexID)
	// Step 3: flip public in its own small transaction.
	if backfillErr == nil {
		backfillErr = s.db.RunTxn(ctx, "create-index-public", func(ctx context.Context, txn *kvclient.Txn) error {
			shared, err := s.lookup(ctx, txn, t.Table)
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
			shared, err := s.lookup(ctx, txn, t.Table)
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
		_ = s.cat.FinishDDLIn(ctx, s.database, t.Table)
		if tableID != 0 {
			s.wipeIndexEntries(ctx, tableID, indexID)
		}
		return nil, ToSQLError(backfillErr)
	}

	// Step 3: drain adoption of the public index.
	if err := s.cat.FinishDDLIn(ctx, s.database, t.Table); err != nil {
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
		desc, err := s.lookup(ctx, txn, table)
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
			entries, err := rowenc.IndexEntries(desc, idx, row)
			if err != nil {
				return invertedEntryErr(idx, err)
			}
			if len(entries) == 0 {
				if idx.Unique {
					return newErrf(CodeNotNullViolation, "cannot create unique index %q: a row has NULL in an indexed column", idx.Name)
				}
				continue
			}
			if !idx.Unique {
				// One entry, or one per element of an inverted index's
				// value. A row with many elements makes this chunk's batch
				// that many times larger; MaxElementsPerValue bounds it.
				for _, e := range entries {
					wb.Put(e.Key, e.Value)
				}
				continue
			}
			key, val := entries[0].Key, entries[0].Value
			{
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
		if t.RenameTo != "" {
			// The drain looks the table up by name: the new one.
			if q, _ := catalog.SplitTableName(t.Table); q != "" {
				return q + "." + t.RenameTo
			}
			return t.RenameTo
		}
		return t.Table
	case *parser.DropTable:
		return t.Name
	case *parser.CreateView:
		return t.Name
	case *parser.CommentOn:
		return t.Name
	case *parser.GrantRevoke:
		// Table grants ride the descriptor: drain leases like any DDL so
		// every gateway enforces the new privileges once the grant returns
		// (the executor records every table it touched in extraDDL; this
		// covers the common single-table form).
		if t.ObjectKind == "table" && len(t.Objects) == 1 && !t.AllInSchema {
			return t.Objects[0]
		}
	case *parser.AlterOwner:
		if t.Kind == "table" || t.Kind == "view" {
			return t.Name
		}
	}
	return ""
}
