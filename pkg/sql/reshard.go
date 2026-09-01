package sql

import (
	"context"
	"strconv"
	"sync"
	"time"

	"github.com/sthorne/datax/pkg/keys"
	"github.com/sthorne/datax/pkg/kvclient"
	"github.com/sthorne/datax/pkg/sql/catalog"
	"github.com/sthorne/datax/pkg/sql/parser"
	"github.com/sthorne/datax/pkg/sql/rowenc"
	"github.com/sthorne/datax/pkg/sql/types"
	"github.com/sthorne/datax/pkg/util/log"
)

// Online re-sharding: ALTER TABLE t SET (shards = M) rewrites a sharded
// timeseries table's rows under a new bucket count without blocking
// writes, on the online CREATE INDEX state machine. Primary rows live at
// an index ID; the new layout is built at a freshly allocated ID (IDs are
// never reused, so the keyspaces cannot collide), then the descriptor
// swaps ShardBuckets and PrimaryIndex atomically:
//
//  1. publish Reshard{NewIndexID, NewBuckets} + lease drain — from here
//     every gateway's INSERT/UPDATE/DELETE dual-writes both layouts;
//  2. pre-split the new layout's bucket prefixes;
//  3. backfill: a frozen-timestamp planning sweep over the OLD layout;
//     each chunk re-reads its narrow span in a serializable txn and
//     re-keys the rows (recompute bucket mod M, re-encode at the new
//     index). Value bytes copy verbatim — values encode only non-PK
//     columns. A concurrent delete inside a chunk invalidates the
//     chunk's read and forces a rescan; a concurrent insert's dual-write
//     writes the same bytes to the same key, so overlap is idempotent;
//  4. swap: ShardBuckets = M, PrimaryIndex = NewIndexID, clear Reshard,
//     stamp ReshardedAt; drain. Readers, writers, and the planner's
//     fan-out all key off these together;
//  5. wipe the old layout (best effort; unreachable either way).
//
// Secondary indexes ride the same machinery: their entries embed the
// _shard value in the primary-key suffix, so each index is rebuilt at a
// shadow ID (ReshardState.NewIndexIDs) — dual-writes mirror every entry
// mutation to the shadow ID with the bucket recomputed, the backfill
// writes shadow entries from the same decoded rows, and the swap adopts
// the shadow IDs together with the primary layout. A re-shard and a
// CREATE INDEX backfill exclude each other (both are refused while the
// other is in flight).
//
// Scope guards: timeseries and already-sharded only (the PK column
// list must stay identical so DecodePK serves both layouts during
// dual-write); historical reads below ReshardedAt are refused (the new
// layout's rows carry backfill-time MVCC timestamps).

// reshardShadowKey builds the in-flight re-shard's new-layout key for a
// complete row; (nil, nil) when no re-shard is pending.
func reshardShadowKey(desc *catalog.TableDescriptor, row map[catalog.ColumnID]types.Datum) (keys.Key, error) {
	rs := desc.Reshard
	if rs == nil {
		return nil, nil
	}
	logical := make([]types.Datum, len(desc.PrimaryKey)-1)
	for i, id := range desc.PrimaryKey[1:] {
		logical[i] = row[id]
	}
	bucket, err := rowenc.ShardBucketAt(desc, logical, rs.NewBuckets)
	if err != nil {
		return nil, err
	}
	return rowenc.EncodePKAt(desc, rs.NewIndexID, append([]types.Datum{bucket}, logical...))
}

// reshardShadowRow returns a copy of row with the hidden shard column
// recomputed under the pending re-shard's bucket count — the row an index
// entry's new-layout primary-key suffix must encode. nil when no re-shard
// is in flight.
func reshardShadowRow(desc *catalog.TableDescriptor, row map[catalog.ColumnID]types.Datum) (map[catalog.ColumnID]types.Datum, error) {
	rs := desc.Reshard
	if rs == nil {
		return nil, nil
	}
	logical := make([]types.Datum, len(desc.PrimaryKey)-1)
	for i, id := range desc.PrimaryKey[1:] {
		logical[i] = row[id]
	}
	bucket, err := rowenc.ShardBucketAt(desc, logical, rs.NewBuckets)
	if err != nil {
		return nil, err
	}
	out := copyRow(row)
	out[desc.PrimaryKey[0]] = bucket
	return out, nil
}

// execReshardOnline runs the re-shard state machine. The session has
// already rejected explicit-transaction contexts and checked admin.
func (s *Session) execReshardOnline(ctx context.Context, t *parser.AlterTable) (*Result, *Error) {
	for name := range t.SetOptions {
		if name != "shards" {
			return nil, newErrf(CodeFeatureNotSupported, "ALTER TABLE ... SET supports only the shards option")
		}
	}
	m64, err := strconv.Atoi(t.SetOptions["shards"])
	if err != nil || m64 < 2 || m64 > 256 {
		return nil, newErrf(CodeSyntaxError, "shards must be an integer in [2, 256]")
	}
	newBuckets := int32(m64)

	// Step 1: publish the dual-write marker.
	var tableID, newIndexID, oldIndexID uint64
	var oldSecondaryIDs, newSecondaryIDs []uint64
	rerr := s.db.RunTxn(ctx, "reshard-publish", func(ctx context.Context, txn *kvclient.Txn) error {
		shared, err := s.cat.Lookup(ctx, txn, t.Table)
		if err != nil {
			return err
		}
		desc := shared.Clone()
		switch {
		case !desc.Timeseries || desc.ShardBuckets <= 0:
			return newErrf(CodeFeatureNotSupported, "re-sharding requires a sharded timeseries table (created WITH (timeseries = true, shards = N))")
		case newBuckets == desc.ShardBuckets:
			return newErrf(CodeSyntaxError, "table %q already has %d shards", t.Table, desc.ShardBuckets)
		case desc.Reshard != nil:
			return newErrf(CodeActiveTransaction, "a re-shard of table %q is already in progress", t.Table)
		}
		// A write-only index is mid-backfill: its own state machine races
		// the shadow-ID adoption. One at a time.
		for i := range desc.Indexes {
			if !desc.Indexes[i].Public() {
				return newErrf(CodeActiveTransaction, "cannot re-shard table %q while index %q is being built", t.Table, desc.Indexes[i].Name)
			}
		}
		if desc.NextIndexID <= desc.LivePrimaryIndex() {
			desc.NextIndexID = desc.LivePrimaryIndex() + 1
		}
		tableID, oldIndexID = desc.ID, desc.LivePrimaryIndex()
		newIndexID = desc.NextIndexID
		desc.NextIndexID++
		// Secondary-index entries embed the shard bucket in their
		// primary-key suffix, so each index is rebuilt at a shadow ID and
		// adopted at the swap together with the primary layout.
		oldSecondaryIDs, newSecondaryIDs = nil, nil
		for i := range desc.Indexes {
			oldSecondaryIDs = append(oldSecondaryIDs, desc.Indexes[i].ID)
			newSecondaryIDs = append(newSecondaryIDs, desc.NextIndexID)
			desc.NextIndexID++
		}
		desc.Reshard = &catalog.ReshardState{NewIndexID: newIndexID, NewBuckets: newBuckets, NewIndexIDs: newSecondaryIDs}
		return s.cat.Update(ctx, txn, desc)
	})
	if rerr != nil {
		return nil, ToSQLError(rerr)
	}
	if err := s.cat.FinishDDL(ctx, t.Table); err != nil {
		return nil, ToSQLError(err)
	}
	// The drain waits for every gateway's LEASE to adopt the dual-write
	// descriptor, but a statement that looked its descriptor up just
	// before adoption can still commit without the shadow write. Wait out
	// that window before the backfill starts copying (a short statement's
	// lifetime; the long-lived explicit-transaction variant of this gap is
	// issue #22's, documented in docs/timeseries.md).
	sleepGrace(ctx)

	// Step 2: pre-split the new layout so backfill and post-swap ingest
	// parallelize immediately (best effort, like CREATE's pre-splits).
	s.presplitReshard(ctx, tableID, newIndexID, newBuckets)

	// Step 3: backfill the new layout from the old.
	backfillErr := s.backfillReshard(ctx, t.Table, oldIndexID)

	// Step 4: the atomic swap.
	if backfillErr == nil {
		backfillErr = s.db.RunTxn(ctx, "reshard-swap", func(ctx context.Context, txn *kvclient.Txn) error {
			shared, err := s.cat.Lookup(ctx, txn, t.Table)
			if err != nil {
				return err
			}
			desc := shared.Clone()
			if desc.Reshard == nil || desc.Reshard.NewIndexID != newIndexID {
				return newErrf(CodeInternal, "re-shard state vanished during backfill")
			}
			if len(desc.Indexes) != len(desc.Reshard.NewIndexIDs) {
				return newErrf(CodeInternal, "re-shard shadow index set diverged from the table's indexes")
			}
			desc.ShardBuckets = newBuckets
			desc.PrimaryIndex = newIndexID
			for i := range desc.Indexes {
				desc.Indexes[i].ID = desc.Reshard.NewIndexIDs[i]
			}
			desc.Reshard = nil
			desc.ReshardedAt = s.db.Clock().Now().WallTime
			return s.cat.Update(ctx, txn, desc)
		})
	}
	if backfillErr != nil {
		// Abandon: clear the marker so writers stop dual-writing, wipe the
		// partial new layout, surface the original failure. The cleanup
		// runs on a cancel-proof context — a client that disconnected
		// mid-backfill (cancelling ctx) must not strand the dual-write
		// marker.
		cctx := context.WithoutCancel(ctx)
		_ = s.db.RunTxn(cctx, "reshard-abandon", func(ctx context.Context, txn *kvclient.Txn) error {
			shared, err := s.cat.Lookup(ctx, txn, t.Table)
			if err != nil {
				return err
			}
			desc := shared.Clone()
			if desc.Reshard == nil || desc.Reshard.NewIndexID != newIndexID {
				return nil
			}
			desc.Reshard = nil
			return s.cat.Update(ctx, txn, desc)
		})
		_ = s.cat.FinishDDL(cctx, t.Table)
		s.wipeIndexEntries(cctx, tableID, newIndexID)
		for _, id := range newSecondaryIDs {
			s.wipeIndexEntries(cctx, tableID, id)
		}
		return nil, ToSQLError(backfillErr)
	}
	if err := s.cat.FinishDDL(ctx, t.Table); err != nil {
		return nil, ToSQLError(err)
	}
	// Same grace on the way out: stale-descriptor readers may still be
	// pointed at the old layout for one statement's lifetime.
	sleepGrace(ctx)

	// Step 5: the old layout — primary rows and every secondary index's
	// old-ID entries — is unreachable; reclaim it. Emptied ranges get
	// re-absorbed by the size-based merger.
	s.wipeIndexEntries(ctx, tableID, oldIndexID)
	for _, id := range oldSecondaryIDs {
		s.wipeIndexEntries(ctx, tableID, id)
	}
	return &Result{Tag: "ALTER TABLE"}, nil
}

// presplitReshard splits the new layout's span boundary and bucket
// prefixes (mirrors presplitTimeseries for the CREATE-time layout).
func (s *Session) presplitReshard(ctx context.Context, tableID, indexID uint64, buckets int32) {
	splitAt := []keys.Key{keys.TableIndexPrefix(tableID, indexID)}
	prefix := keys.TableIndexPrefix(tableID, indexID)
	for b := int32(1); b < buckets; b++ {
		k, err := rowenc.AppendKeyDatum(prefix.Clone(), types.Int, types.NewInt(int64(b)))
		if err != nil {
			return
		}
		splitAt = append(splitAt, k)
	}
	for _, k := range splitAt {
		if _, err := s.db.AdminSplit(ctx, k); err != nil {
			log.Debugf("reshard pre-split at %s: %v", k, err)
		}
	}
}

// TestingReshardFailBackfill, when set (tests only), is called at the
// start of the re-shard backfill; a non-nil result aborts the re-shard
// and exercises the abandon path.
var TestingReshardFailBackfill func() error

// backfillReshard re-keys every old-layout row into the new layout, in
// chunks, against a frozen planning timestamp (the same shape as
// backfillIndex — see that function's correctness comment).
func (s *Session) backfillReshard(ctx context.Context, table string, oldIndexID uint64) error {
	if TestingReshardFailBackfill != nil {
		if err := TestingReshardFailBackfill(); err != nil {
			return err
		}
	}
	boundary := s.db.Clock().Now()

	var cursor, end keys.Key
	if err := s.db.RunTxn(ctx, "reshard-plan", func(ctx context.Context, txn *kvclient.Txn) error {
		desc, err := s.cat.Lookup(ctx, txn, table)
		if err != nil {
			return err
		}
		cursor, end = keys.TableIndexSpan(desc.ID, oldIndexID)
		return nil
	}); err != nil {
		return err
	}

	// Chunks are independent (disjoint spans; idempotent against
	// dual-writes), and each one pays a full commit round trip — run them
	// on parallel workers so the backfill isn't bound by serial commit
	// latency. The planning sweep stays sequential (cheap frozen reads).
	wctx, cancel := context.WithCancel(ctx)
	defer cancel()
	type chunkSpan struct{ start, end keys.Key }
	spans := make(chan chunkSpan, reshardWorkers)
	errs := make(chan error, reshardWorkers)
	var wg sync.WaitGroup
	for w := 0; w < reshardWorkers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for cs := range spans {
				if err := s.reshardChunk(wctx, table, cs.start, cs.end); err != nil {
					select {
					case errs <- err:
					default:
					}
					cancel()
					return
				}
			}
		}()
	}
	var planErr error
	for planErr == nil {
		plan, err := s.db.ScanAt(wctx, cursor, end, reshardChunkSize, boundary)
		if err != nil {
			planErr = err
			break
		}
		if len(plan) == 0 {
			break
		}
		chunkEnd := plan[len(plan)-1].Key.Next()
		select {
		case spans <- chunkSpan{start: cursor, end: chunkEnd}:
		case <-wctx.Done():
			planErr = wctx.Err()
		}
		cursor = chunkEnd
	}
	close(spans)
	wg.Wait()
	select {
	case err := <-errs:
		return err
	default:
	}
	return planErr
}

// reshardWorkers is the backfill's chunk-commit parallelism.
const reshardWorkers = 8

// reshardGraceWait bounds how long a just-published descriptor change
// takes to reach statements already past their descriptor lookup.
var reshardGraceWait = time.Second

func sleepGrace(ctx context.Context) {
	select {
	case <-time.After(reshardGraceWait):
	case <-ctx.Done():
	}
}

// reshardChunkSize is larger than the CREATE INDEX chunk: a re-key chunk
// is a verbatim KV copy (no per-row uniqueness probes), so the only costs
// a bigger chunk raises are raft entry size and refresh-span width — and
// a wider chunk means far fewer serializable txns racing the live
// append tail.
const reshardChunkSize = 512

func (s *Session) reshardChunk(ctx context.Context, table string, start, end keys.Key) error {
	return s.db.RunTxn(ctx, "reshard-backfill", func(ctx context.Context, txn *kvclient.Txn) error {
		desc, err := s.cat.Lookup(ctx, txn, table)
		if err != nil {
			return err
		}
		if desc.Reshard == nil {
			return newErrf(CodeInternal, "re-shard state vanished mid-backfill")
		}
		kvs, err := txn.Scan(ctx, start, end, 0)
		if err != nil {
			return err
		}
		var wb kvclient.WriteBatch
		for _, kv := range kvs {
			row, err := decodeFullRow(desc, kv.Key, kv.Value)
			if err != nil {
				return err
			}
			newKey, err := reshardShadowKey(desc, row)
			if err != nil {
				return err
			}
			// Value bytes carry only non-PK columns: verbatim copy.
			wb.Put(newKey, kv.Value)
			// Rebuild every secondary index's entry at its shadow ID: the
			// entry's primary-key suffix embeds the bucket, so it encodes
			// from the shadow row. Idempotent against dual-writes for the
			// same reason the primary copy is.
			if len(desc.Reshard.NewIndexIDs) > 0 {
				shadowRow, err := reshardShadowRow(desc, row)
				if err != nil {
					return err
				}
				for i := range desc.Indexes {
					if i >= len(desc.Reshard.NewIndexIDs) {
						return newErrf(CodeInternal, "re-shard shadow index set diverged from the table's indexes")
					}
					ik, iv, skip, err := rowenc.EncodeIndexEntryAt(desc, &desc.Indexes[i], desc.Reshard.NewIndexIDs[i], shadowRow)
					if err != nil {
						return newErrf(CodeInternal, "re-shard index %q: %v", desc.Indexes[i].Name, err)
					}
					if !skip {
						wb.Put(ik, iv)
					}
				}
			}
		}
		return txn.RunBatch(ctx, &wb)
	})
}
