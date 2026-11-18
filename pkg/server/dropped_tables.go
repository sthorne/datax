package server

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/sthorne/datax/pkg/base"
	"github.com/sthorne/datax/pkg/keys"
	"github.com/sthorne/datax/pkg/kvclient"
	"github.com/sthorne/datax/pkg/sql/catalog"
	"github.com/sthorne/datax/pkg/util/log"
	"github.com/sthorne/datax/pkg/version"
)

// Reclaiming a dropped table's rows (issue #313). DROP TABLE removes the
// descriptor and the name and leaves the rows, with a record
// (catalog.DroppedTable) saying when and how long the table kept its
// history. The rows stay for that window because they are still
// reachable: AS OF SYSTEM TIME below the drop resolves the table as it
// was and reads them, and so does an incremental backup's export of an
// interval that ends before the drop. After the window no admissible
// read reaches them, and this job — on the node leading range 1, like
// the re-shard janitor — clears the table's span:
//
//  1. The span is cleared. On a cluster at v22, one replicated range
//     clear per range of the span (ClearSpan, kvserver.Store.ClearRange:
//     a range deletion of every version, with the range's GC threshold
//     raised to the clear's time in the same command), whatever the
//     number of rows. Below v22 — a replica could be on a binary that
//     does not know the command — every row is deleted instead, in
//     transactions of restoreChunk deletes, resuming after the last key
//     deleted. Either is idempotent, so a pass cut short by a restart or
//     a move of range 1's leadership is simply run again; the record
//     then says the rows are gone.
//  2. A purge of the span (collection at a window of zero, then
//     compaction, log truncation and log scrub: docs/transactions.md,
//     "Purge") leaves nothing of the table in the files: after a clear,
//     the keys are still in their sstables under the range tombstone and
//     their values in the raft log; after row-by-row deletion, the
//     tombstones and the versions under them are collected too. On a
//     cluster below v21, which has no purge, ordinary GC collects the
//     deletions one window later.
//  3. The record is removed.
//
// The clear and the purge raise the span's GC threshold to their own
// time. That refuses
// only reads of the dropped table at times its history window no longer
// covers anyway; the span is split from its neighbours first, and the
// merge queue folds the empty ranges back into their neighbours once the
// neighbours' own windows have passed the purge (kvserver's merge
// refuses a pair whose thresholds are further apart than that, because
// the merged range adopts the higher one).

const (
	droppedTableReclaimInterval = time.Minute
	// droppedTableSlack is added to a table's window before its rows are
	// cleared: the record's time is the dropping transaction's read
	// timestamp, which may precede its commit, and the gateway's clock
	// may lag this node's.
	droppedTableSlack = 5 * time.Second
)

// droppedTableReclaimLoop runs RunDroppedTableReclaimOnce on the node
// leading range 1.
func (n *Node) droppedTableReclaimLoop(ctx context.Context) {
	interval := n.cfg.DroppedTableReclaimInterval
	if interval < 0 {
		return
	}
	if interval == 0 {
		interval = droppedTableReclaimInterval
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
		r1, ok := n.store.GetReplica(1)
		if !ok || !r1.IsLeader() {
			continue
		}
		// The one-time sweep for tables dropped before the records
		// (dropped_sweep.go) writes records for this job to act on.
		for began := time.Now(); time.Since(began) < droppedSweepTickBudget && n.ClusterVersion() >= version.V22; {
			done, err := n.droppedTableSweep(ctx)
			if err != nil {
				log.Debugf("dropped-table sweep: %v", err)
				break
			}
			if done {
				break
			}
		}
		n.RunDroppedTableReclaimOnce(ctx)
	}
}

// droppedTableWindow is how long rec's rows are kept after the drop.
func (n *Node) droppedTableWindow(rec *catalog.DroppedTable) time.Duration {
	if rec.WindowSeconds > 0 {
		return time.Duration(rec.WindowSeconds) * time.Second
	}
	if n.cfg.GCTTL > 0 {
		return n.cfg.GCTTL
	}
	return base.DefaultGCTTL
}

// RunDroppedTableReclaimOnce clears the span of every dropped table whose
// history window has passed, and returns how many it finished. It reads
// the dropped tables' records and nothing else: its cost follows the
// number of tables waiting to be reclaimed, not the size of the catalog.
// Errors are logged and the table is taken up again on the next pass.
func (n *Node) RunDroppedTableReclaimOnce(ctx context.Context) int {
	lo, hi := keys.DroppedTableSpan()
	kvs, err := n.db.Scan(ctx, lo, hi, 0)
	if err != nil {
		log.Debugf("dropped-table reclaim: %v", err)
		return 0
	}
	now := n.clock.Now().WallTime
	done := 0
	for _, kv := range kvs {
		rec, err := catalog.DecodeDroppedTable(kv.Value)
		if err != nil {
			log.Warnf("dropped-table reclaim: %s: %v", keys.Key(kv.Key), err)
			continue
		}
		if now < rec.DroppedAt+(n.droppedTableWindow(rec)+droppedTableSlack).Nanoseconds() {
			continue
		}
		// A statement planned against a gateway's cached descriptor
		// commits within that gateway's lease, and a gateway that saw the
		// drop keeps the lease's expiration in its acknowledgement: while
		// any lease on the table is live, a write planned before the drop
		// could still land in the span after it was cleared (issue #377).
		if !rec.Cleared {
			var leased bool
			if err := n.db.RunTxn(ctx, "drop-reclaim-leases", func(ctx context.Context, txn *kvclient.Txn) error {
				var err error
				leased, err = catalog.DescriptorLeased(ctx, txn, rec.ID, n.clock.Now().WallTime)
				return err
			}); err != nil || leased {
				if err != nil {
					log.Debugf("dropped-table reclaim: table %s (id %d): reading its leases: %v", rec.Label(), rec.ID, err)
				}
				continue
			}
		}
		opID := newOpID()
		n.events.RecordStart("drop-reclaim", opID, "reclaiming dropped table %s (id %d)", rec.Label(), rec.ID)
		if err := n.reclaimDroppedTable(ctx, rec); err != nil {
			n.events.RecordEnd("drop-reclaim", opID, "failed", "reclaiming dropped table %s (id %d): %v", rec.Label(), rec.ID, err)
			log.Warnf("dropped-table reclaim: table %s (id %d): %v", rec.Label(), rec.ID, err)
			continue
		}
		n.events.RecordEnd("drop-reclaim", opID, "ok", "reclaimed dropped table %s (id %d)", rec.Label(), rec.ID)
		log.Infof("dropped-table reclaim: table %s (id %d) dropped at %s: span cleared",
			rec.Label(), rec.ID, time.Unix(0, rec.DroppedAt).UTC().Format(time.RFC3339))
		done++
	}
	return done
}

func (n *Node) reclaimDroppedTable(ctx context.Context, rec *catalog.DroppedTable) error {
	start, end := keys.TableDataSpan(rec.ID)
	if !rec.Cleared {
		// A descriptor under the id means the id is in use again (a
		// restore of everything brings tables back under their ids, and
		// removes the records of the ids it uses; this is the guard should
		// one be left). Its rows are that table's: the record goes, the
		// rows stay.
		if raw, err := n.db.Get(ctx, keys.TableDescKey(rec.ID)); err != nil {
			return err
		} else if raw != nil {
			log.Warnf("dropped-table reclaim: id %d names a table again; its record is removed and its rows kept", rec.ID)
			return n.db.RunTxn(ctx, "drop-reclaim-record", func(ctx context.Context, txn *kvclient.Txn) error {
				return txn.Delete(ctx, keys.DroppedTableKey(rec.ID))
			})
		}
		if n.ClusterVersion() >= version.V22 {
			res, err := n.ClearSpan(ctx, start, end)
			if err != nil {
				return fmt.Errorf("clearing the span: %w", err)
			}
			log.Infof("dropped-table reclaim: table %s (id %d): cleared %d key(s), %d byte(s) over %d range(s) at %s",
				rec.Label(), rec.ID, res.Keys, res.Bytes, res.Ranges, res.Timestamp)
		} else if err := n.wipeSpan(ctx, start, end); err != nil {
			return fmt.Errorf("deleting the rows: %w", err)
		}
		if err := n.updateDroppedTable(ctx, rec.ID, func(r *catalog.DroppedTable) { r.Cleared = true }); err != nil {
			return err
		}
	}
	if n.ClusterVersion() >= version.V21 {
		rep, err := n.PurgeSpan(ctx, start, end)
		if err != nil {
			return fmt.Errorf("collecting the deleted rows: %w", err)
		}
		if !rep.Complete {
			// The collection is replicated: a replica that did not answer
			// applies it when it does. The rows are deleted either way.
			log.Infof("dropped-table reclaim: table %s (id %d): purge incomplete: %s", rec.Label(), rec.ID, incompleteSuffix(rep))
		}
	}
	return n.db.RunTxn(ctx, "drop-reclaim-record", func(ctx context.Context, txn *kvclient.Txn) error {
		// The gateways' lapsed leases on the table go with its record.
		if err := catalog.DeleteExpiredLeases(ctx, txn, rec.ID, n.clock.Now().WallTime); err != nil {
			return err
		}
		return txn.Delete(ctx, keys.DroppedTableKey(rec.ID))
	})
}

// updateDroppedTable rewrites table id's record, if it is still there.
func (n *Node) updateDroppedTable(ctx context.Context, id uint64, fn func(*catalog.DroppedTable)) error {
	return n.db.RunTxn(ctx, "drop-reclaim-record", func(ctx context.Context, txn *kvclient.Txn) error {
		raw, err := txn.Get(ctx, keys.DroppedTableKey(id))
		if err != nil || raw == nil {
			return err
		}
		rec, err := catalog.DecodeDroppedTable(raw)
		if err != nil {
			return err
		}
		fn(rec)
		out, err := json.Marshal(rec)
		if err != nil {
			return err
		}
		return txn.Put(ctx, keys.DroppedTableKey(id), out)
	})
}

// clearTableIDForRestore makes table id's span empty and its id free of
// any dropped-table record, for a restore of everything about to bring a
// table back under that id. The target holds no tables, but it may have
// held some: their rows stay until reclaimed, under ids the backup's
// tables can carry, and would otherwise mix with the restored rows — and
// a record left behind would have the reclaim job delete the restored
// table's rows when its window ran out.
func (n *Node) clearTableIDForRestore(ctx context.Context, id uint64) error {
	start, end := keys.TableDataSpan(id)
	kvs, err := n.db.Scan(ctx, start, end, 1)
	if err != nil {
		return err
	}
	if len(kvs) > 0 {
		log.Infof("restore: table id %d holds rows of a dropped table; deleting them first", id)
		if err := n.wipeSpan(ctx, start, end); err != nil {
			return err
		}
	}
	return n.db.RunTxn(ctx, "restore-dropped-record", func(ctx context.Context, txn *kvclient.Txn) error {
		return txn.Delete(ctx, keys.DroppedTableKey(id))
	})
}
