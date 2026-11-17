package sql

import (
	"context"

	"github.com/sthorne/datax/pkg/keys"
	"github.com/sthorne/datax/pkg/kvclient"
	"github.com/sthorne/datax/pkg/kvpb"
	"github.com/sthorne/datax/pkg/sql/catalog"
	"github.com/sthorne/datax/pkg/sql/inverted"
	"github.com/sthorne/datax/pkg/sql/parser"
	"github.com/sthorne/datax/pkg/sql/rowenc"
	"github.com/sthorne/datax/pkg/sql/tsearch"
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
	if err := checkIndexForm(t); err != nil {
		return nil, err
	}
	switch t.Using {
	case "", "btree":
		// The operator classes the string types have are accepted
		// (newIndexDescriptor checks each against its column); a
		// column that does not exist is reported there too.
		for i, oc := range t.OpClasses {
			if col, ok := desc.Col(t.Columns[i]); ok {
				if _, err := ordinaryOpClass(oc, &col); err != nil {
					return nil, err
				}
			}
		}
		return nil, nil
	case "gin":
	default:
		return nil, newErrf(CodeFeatureNotSupported, "access method %q is not supported (btree, gin and ivfflat are)", t.Using)
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
	if t.ExprFunc != "" {
		// The one expression an index can be built on: the lexemes of a
		// text column under a named configuration. It is stored as an
		// inverted kind of its own over the column (pkg/sql/inverted),
		// and serves predicates written with the same expression.
		if t.ExprFunc != "to_tsvector" {
			return nil, newErrf(CodeFeatureNotSupported, "a GIN index on an expression takes to_tsvector('config', column); index other expressions USING btree")
		}
		cfg, err := tsearch.LookupConfig(t.ExprArg)
		if err != nil {
			return nil, newErrf(CodeUndefinedObject, "%v", err)
		}
		kind, _ := inverted.Lookup(inverted.TextSearchKindName(cfg.Name))
		if col.Type != kind.Family() {
			return nil, newErrf(CodeFeatureNotSupported, "to_tsvector(config, column) indexes a TEXT column; %q is %s", col.Name, col.Type)
		}
		return kind, nil
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
		if ok && isComputedKind(kind) {
			// Reached only through its expression, never by name.
			ok = false
		}
		if !ok {
			return nil, newErrf(CodeUndefinedObject, "operator class %q does not exist for access method \"gin\"", opclass)
		}
		return kind, nil
	}
	for _, name := range inverted.Names() {
		if kind, _ := inverted.Lookup(name); kind.Family() == col.Type && !isComputedKind(kind) {
			return kind, nil
		}
	}
	if col.Type == types.String {
		return nil, newErrf(CodeFeatureNotSupported, "column %q of type %s has no GIN index of its own: for full-text search, index to_tsvector('english', %s)", col.Name, col.Type, col.Name)
	}
	return nil, newErrf(CodeFeatureNotSupported, "column %q of type %s cannot have a GIN index (JSONB and TSVECTOR columns can)", col.Name, col.Type)
}

// isComputedKind: the kind indexes a function of its column (see
// inverted.Computed), so it is chosen by that expression and not by the
// column's type or an operator class.
func isComputedKind(kind inverted.Kind) bool {
	c, ok := kind.(inverted.Computed)
	if !ok {
		return false
	}
	fn, _ := c.Computes()
	return fn != ""
}

// backfillChunkSize bounds each backfill chunk: rows scanned, KV batch
// size, raft entry size, and refresh-span width per transaction.
const backfillChunkSize = 64

// execCreateIndexOnline runs the state machine. The session has already
// rejected explicit-transaction contexts.
func (s *Session) execCreateIndexOnline(ctx context.Context, t *parser.CreateIndex) (*Result, *Error) {
	if err := checkIndexForm(t); err != nil {
		return nil, ToSQLError(err)
	}
	if t.Name == "" {
		// CREATE INDEX ON t (cols): the name is chosen from the table as
		// it is now. One taken in the meantime fails the publish below
		// as a duplicate.
		var name string
		if err := s.db.RunTxn(ctx, "create-index-name", func(ctx context.Context, txn *kvclient.Txn) error {
			desc, err := s.lookup(ctx, txn, t.Table)
			if err != nil {
				return err
			}
			name = chooseIndexName(desc, t)
			return nil
		}); err != nil {
			return nil, ToSQLError(err)
		}
		c := *t
		c.Name = name
		t = &c
	}
	// A vector index trains its centres before anything is published
	// (see vector_index.go): vec is then the index's description, its
	// centres already committed. nil for every other kind of index, and
	// when the name is taken and there was nothing to train for.
	vspec, err := s.vectorSpecFor(t)
	if err != nil {
		return nil, ToSQLError(err)
	}
	var vec *catalog.VectorIndex
	var vecTableID uint64
	if vspec != nil {
		if vec, vecTableID, err = s.trainVectorIndex(ctx, t, vspec); err != nil {
			return nil, ToSQLError(err)
		}
	}
	// Step 1: publish write-only.
	var indexID uint64
	var notices []string
	exists := false
	err = s.db.RunTxn(ctx, "create-index-publish", func(ctx context.Context, txn *kvclient.Txn) error {
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
		var kind inverted.Kind
		if vspec == nil {
			if kind, err = s.invertedKindFor(t, desc); err != nil {
				return err
			}
		} else {
			// The column may have changed since the centres were trained
			// on it.
			col, err := vectorIndexColumn(desc, t.Columns[0])
			if err != nil {
				return err
			}
			if vec == nil || desc.ID != vecTableID || int(col.MaxLen) != vec.Dims {
				return newErrf(CodeObjectInUse, "table %q changed while index %q was being prepared; run CREATE INDEX again", t.Table, t.Name)
			}
		}
		idx, err := s.newIndexDescriptor(t, desc, kind, vspec != nil, catalog.IndexStateWriteOnly)
		if err != nil {
			return err
		}
		idx.Vector = vec
		indexID = idx.ID
		notices = nil
		if desc.Sealed() {
			notices = []string{sealedIndexNotice(desc, idx)}
		}
		return s.cat.Update(ctx, txn, desc)
	})
	if vec != nil && (err != nil || exists) {
		// The centres were trained for an index that is not to be.
		s.dropVectorCentroids(ctx, vecTableID, vec.Build)
	}
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
			if vec != nil {
				s.dropVectorCentroids(ctx, tableID, vec.Build)
			}
		}
		return nil, ToSQLError(backfillErr)
	}

	// Step 3: drain adoption of the public index.
	if err := s.cat.FinishDDLIn(ctx, s.database, t.Table); err != nil {
		return nil, ToSQLError(err)
	}
	return &Result{Tag: "CREATE INDEX", Notices: notices}, nil
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
//
// Before any of that the index is emptied (clearDrainEntries): what it
// holds when the drain ends is not to be trusted.
func (s *Session) backfillIndex(ctx context.Context, table string, indexID uint64) error {
	var tableID uint64
	var cursor, end keys.Key
	if err := s.db.RunTxn(ctx, "create-index-plan", func(ctx context.Context, txn *kvclient.Txn) error {
		desc, err := s.lookup(ctx, txn, table)
		if err != nil {
			return err
		}
		tableID = desc.ID
		cursor, end = rowenc.PrimarySpanFor(desc)
		return nil
	}); err != nil {
		return err
	}
	if err := s.clearDrainEntries(ctx, tableID, indexID); err != nil {
		return err
	}
	boundary := s.db.Clock().Now()
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

// clearDrainEntries deletes every entry the index holds when the drain
// of its write-only state has ended, in one pass over its span.
//
// While the drain waits, gateways adopt the index one by one, so for a
// while some writers maintain it and some do not. A writer that does
// writes a row's entries; a writer that does not may then change or
// delete that row and leaves those entries where they are. Nothing
// later would remove them: the backfill only adds what each row has,
// and a writer only removes what it believes the row had. They would
// stay as entries for rows that no longer hold the value — a row
// returned for a value it does not have by a covering read, or "entry
// points at a missing row" — so the index starts from nothing instead.
//
// After the drain every writer that can still commit maintains the
// index, so no such entry is made from here on, and one pass is enough:
// every stale entry already exists when the pass starts. The pass also
// deletes good entries, and may delete one a writer has just written;
// all of that is before the backfill's boundary, and the backfill then
// writes every entry of every row as it reads it.
func (s *Session) clearDrainEntries(ctx context.Context, tableID, indexID uint64) error {
	// The keys are read outside the transaction that deletes them. Read
	// inside it, they would be a span the transaction must find
	// unchanged at commit, and under a stream of writers adding entries
	// to that span it never would. Nothing needs the two to be one
	// snapshot: a key written after the read is a live writer's, and a
	// delete of a key rewritten since the read is one more deletion the
	// backfill makes good.
	const chunk = 256
	lo, hi := keys.TableIndexSpan(tableID, indexID)
	for {
		kvs, err := s.db.Scan(ctx, lo, hi, chunk)
		if err != nil {
			return err
		}
		if len(kvs) == 0 {
			return nil
		}
		if err := s.db.RunTxn(ctx, "create-index-clear", func(ctx context.Context, txn *kvclient.Txn) error {
			var wb kvclient.WriteBatch
			for _, kv := range kvs {
				wb.Delete(kv.Key)
			}
			return txn.RunBatch(ctx, &wb)
		}); err != nil {
			return err
		}
		if len(kvs) < chunk {
			return nil
		}
		lo = kvs[len(kvs)-1].Key.Next()
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
		return s.writeIndexEntries(ctx, txn, desc, idx, kvs)
	})
}

// writeIndexEntries writes idx's entries for the primary rows kvs of
// desc in txn, refusing a duplicate of a unique index's values among
// them or against an entry already in the index.
func (s *Session) writeIndexEntries(ctx context.Context, txn *kvclient.Txn, desc *catalog.TableDescriptor, idx *catalog.IndexDescriptor, kvs []kvpb.KeyValue) error {
	var wb kvclient.WriteBatch
	seen := map[string]string{}
	for _, kv := range kvs {
		row, err := decodeFullRow(ctx, s.rowSealer(), desc, kv.Key, kv.Value)
		if err != nil {
			return err
		}
		entries, err := rowenc.IndexEntries(desc, idx, row)
		if err != nil {
			return invertedEntryErr(idx, err)
		}
		if len(entries) == 0 {
			// NULLS DISTINCT: a row with a NULL has no entry and
			// duplicates nothing (refused before v22, as the write
			// path refuses it).
			if idx.Unique && !uniqueNullsDistinct(txn) {
				return newErrf(CodeNotNullViolation, "cannot create unique index %q: a row has NULL in an indexed column (NULLs in a unique index need cluster version %s: finalize the upgrade first)", idx.Name, version.V22)
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
	case *parser.CreatePolicy:
		// A policy rides the descriptor: once the statement returns,
		// every gateway filters by it.
		return t.Table
	case *parser.AlterPolicy:
		return t.Table
	case *parser.DropPolicy:
		return t.Table
	}
	return ""
}
