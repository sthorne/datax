package server

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"time"

	"github.com/sthorne/datax/pkg/keys"
	"github.com/sthorne/datax/pkg/kvclient"
	"github.com/sthorne/datax/pkg/sql/catalog"
	"github.com/sthorne/datax/pkg/util/log"
	"github.com/sthorne/datax/pkg/version"
)

// The sweep for tables dropped before DROP TABLE left a record (issue
// #316). Since #313 a DROP TABLE writes a catalog.DroppedTable record in
// its transaction, and the reclaim job clears the table's span once its
// history window has passed. A table dropped by a binary from before
// that left its rows and nothing that names them: no descriptor, no
// record, rows under /t/<id> for ever. This sweep finds such spans once
// per cluster and writes the record DROP TABLE would have written, and
// the reclaim job does the rest.
//
// What it records is an id that, in one transaction, has
//
//   - a live row under /t/<id> (a span holding only deletions is left
//     to GC),
//   - no table descriptor, and no sequence, type or database descriptor
//     either (none of those keeps rows under /t/, so such an id is not
//     one this sweep understands),
//   - no dropped-table record (the table is already on its way),
//   - no table restore in flight naming it (keys.RestoreProgressKey:
//     a restore loads rows under fresh ids before any descriptor names
//     them; the restore publishes them or its janitor removes them),
//
// and an id that is an ordinary one: above zero, below the reserved
// range of the cluster's own tables and the virtual tables (1<<40 and
// up: a reserved table exists as a span before its descriptor does), and
// at most the descriptor id generator's value when the sweep started. A
// table created since cannot be dropped without a record, so the sweep
// does not look at ids issued after it began.
//
// Reading the descriptor and the rows in one transaction is what keeps a
// table being created out of it: CREATE TABLE writes the descriptor and
// any rows in one transaction, and a read that meets its intents pushes
// it above the read, so in the sweep's snapshot a table has both or
// neither. The record goes in the same transaction, with the sweep's
// progress, so a sweep cut short — a restart, a move of range 1's
// leadership — resumes after the last id it recorded, and running it
// again records nothing twice.
//
// The record's time is the sweep's, not the drop's, which is unknown:
// the drop was at or before it. Its window is zero, the node's GC TTL,
// the one window that bounded the dropped table's history once its
// descriptor was gone (an AS OF SYSTEM TIME read resolves a table from
// the descriptor span's history, kept for the GC TTL), so the reclaim
// waits at least that long after the drop, and longer by however long
// before the sweep the drop was. The reclaim itself re-reads the
// descriptor before it deletes anything and keeps the rows of an id that
// names a table again.
//
// The sweep runs on the node leading range 1, once the cluster has
// finalized v22 — when no binary that drops tables without records can be
// running — beside the reclaim job, a bounded number of ids per pass.

// droppedSweepState is the sweep's progress (keys.DroppedTableSweepKey).
type droppedSweepState struct {
	// MaxID is the descriptor id generator's value when the sweep began;
	// ids above it are not swept.
	MaxID uint64 `json:"max_id"`
	// Next is the lowest id not yet examined.
	Next uint64 `json:"next"`
	// Found counts the records the sweep wrote.
	Found int `json:"found"`
	// Done is set when every id up to MaxID has been examined.
	Done bool `json:"done,omitempty"`
}

// droppedSweepBudget bounds one pass: the ids with rows it examines.
const droppedSweepBudget = 256

// droppedSweepTickBudget bounds the passes one tick of the reclaim loop
// runs before it reclaims: a large catalog is swept over a few ticks.
const droppedSweepTickBudget = 10 * time.Second

// reservedTableIDs is where the cluster's own tables and the virtual
// tables begin (catalog.MetricsTableID, vtable.VirtualTableID).
const reservedTableIDs = catalog.MetricsTableID

// RunDroppedTableSweepOnce runs one bounded pass of the sweep and reports
// whether the sweep is finished (true at once when it already was; false
// below v22 and on an error, which the next pass retries).
func (n *Node) RunDroppedTableSweepOnce(ctx context.Context) bool {
	done, err := n.droppedTableSweep(ctx)
	if err != nil {
		log.Debugf("dropped-table sweep: %v", err)
	}
	return done
}

func (n *Node) droppedTableSweep(ctx context.Context) (bool, error) {
	if n.droppedSweepDone.Load() {
		return true, nil
	}
	if n.ClusterVersion() < version.V22 {
		return false, nil
	}
	done, err := n.droppedTableSweepPass(ctx)
	if done {
		n.droppedSweepDone.Store(true)
	}
	return done, err
}

func (n *Node) readDroppedSweepState(ctx context.Context, txn *kvclient.Txn) (*droppedSweepState, error) {
	raw, err := txn.Get(ctx, keys.DroppedTableSweepKey())
	if err != nil {
		return nil, err
	}
	if raw == nil {
		return nil, nil
	}
	var st droppedSweepState
	if err := json.Unmarshal(raw, &st); err != nil {
		return nil, fmt.Errorf("corrupt sweep record: %w", err)
	}
	return &st, nil
}

func putDroppedSweepState(ctx context.Context, txn *kvclient.Txn, st *droppedSweepState) error {
	raw, err := json.Marshal(st)
	if err != nil {
		return err
	}
	return txn.Put(ctx, keys.DroppedTableSweepKey(), raw)
}

func (n *Node) droppedTableSweepPass(ctx context.Context) (bool, error) {
	var st *droppedSweepState
	if err := n.db.RunTxn(ctx, "dropped-sweep-start", func(ctx context.Context, txn *kvclient.Txn) error {
		var err error
		if st, err = n.readDroppedSweepState(ctx, txn); err != nil || st != nil {
			return err
		}
		raw, err := txn.Get(ctx, keys.DescIDGenKey())
		if err != nil {
			return err
		}
		var max uint64
		if raw != nil {
			if max, err = strconv.ParseUint(string(raw), 10, 64); err != nil {
				return fmt.Errorf("descriptor id generator: %w", err)
			}
		}
		if max >= reservedTableIDs {
			max = reservedTableIDs - 1
		}
		st = &droppedSweepState{MaxID: max, Next: 1}
		return putDroppedSweepState(ctx, txn, st)
	}); err != nil {
		return false, err
	}
	if st.Done {
		return true, nil
	}
	end := keys.TableDataPrefix(st.MaxID + 1)
	for i := 0; i < droppedSweepBudget; i++ {
		// The next id at or after st.Next with a live row.
		kvs, err := n.db.Scan(ctx, keys.TableDataPrefix(st.Next), end, 1)
		if err != nil {
			return false, err
		}
		if len(kvs) == 0 {
			err := n.db.RunTxn(ctx, "dropped-sweep-done", func(ctx context.Context, txn *kvclient.Txn) error {
				cur, err := n.readDroppedSweepState(ctx, txn)
				if err != nil || cur == nil || cur.Next != st.Next {
					return fmt.Errorf("the sweep record moved under this pass (%v)", err)
				}
				cur.Done = true
				st = cur
				return putDroppedSweepState(ctx, txn, cur)
			})
			if err != nil {
				return false, err
			}
			log.Infof("dropped-table sweep: done; %d table(s) dropped before their record existed are waiting out their window", st.Found)
			return true, nil
		}
		id, ok := keys.TableIDOf(keys.Key(kvs[0].Key))
		if !ok || id < st.Next || id > st.MaxID {
			return false, fmt.Errorf("the scan from id %d returned key %s", st.Next, keys.Key(kvs[0].Key))
		}
		// A live table, the common case, is passed over without a
		// transaction of its own.
		if raw, err := n.db.Get(ctx, keys.TableDescKey(id)); err != nil {
			return false, err
		} else if raw != nil {
			st.Next = id + 1
			continue
		}
		found := false
		err = n.db.RunTxn(ctx, "dropped-sweep-record", func(ctx context.Context, txn *kvclient.Txn) error {
			found = false
			cur, err := n.readDroppedSweepState(ctx, txn)
			if err != nil {
				return err
			}
			if cur == nil || cur.Done || cur.Next > id {
				return fmt.Errorf("the sweep record moved under this pass")
			}
			cur.Next = id + 1
			ok, err := n.droppedSweepCandidate(ctx, txn, id)
			if err != nil {
				return err
			}
			if ok {
				rec := catalog.DroppedTable{ID: id, DroppedAt: txn.ReadTimestamp().WallTime, Swept: true}
				raw, err := json.Marshal(&rec)
				if err != nil {
					return err
				}
				if err := txn.Put(ctx, keys.DroppedTableKey(id), raw); err != nil {
					return err
				}
				cur.Found++
				found = true
			}
			st = cur
			return putDroppedSweepState(ctx, txn, cur)
		})
		if err != nil {
			return false, err
		}
		if found {
			log.Infof("dropped-table sweep: id %d holds rows and names no table; recorded as dropped, to be reclaimed after the GC TTL", id)
		}
	}
	// Out of budget: note how far this pass got (a live table's id moves
	// Next without a transaction of its own).
	return false, n.db.RunTxn(ctx, "dropped-sweep-progress", func(ctx context.Context, txn *kvclient.Txn) error {
		cur, err := n.readDroppedSweepState(ctx, txn)
		if err != nil || cur == nil || cur.Done {
			return err
		}
		if st.Next > cur.Next {
			cur.Next = st.Next
		}
		return putDroppedSweepState(ctx, txn, cur)
	})
}

// droppedSweepCandidate reports, in txn, whether id holds rows of a
// table dropped without a record: see the comment at the top.
func (n *Node) droppedSweepCandidate(ctx context.Context, txn *kvclient.Txn, id uint64) (bool, error) {
	if id == 0 || id >= reservedTableIDs || catalog.IsSystemTableID(id) {
		return false, nil
	}
	for _, k := range []keys.Key{
		keys.TableDescKey(id), keys.DroppedTableKey(id),
		keys.SequenceDescKey(id), keys.TypeDescKey(id), keys.DatabaseDescKey(id),
	} {
		raw, err := txn.Get(ctx, k)
		if err != nil {
			return false, err
		}
		if raw != nil {
			return false, nil
		}
	}
	start, end := keys.TableDataSpan(id)
	rows, err := txn.Scan(ctx, start, end, 1)
	if err != nil || len(rows) == 0 {
		return false, err
	}
	lo, hi := keys.RestoreProgressSpan()
	recs, err := txn.Scan(ctx, lo, hi, 0)
	if err != nil {
		return false, err
	}
	for _, kv := range recs {
		var rp restoreProgress
		if err := json.Unmarshal(kv.Value, &rp); err != nil {
			// A record this node cannot read may name the id.
			return false, fmt.Errorf("restore record %s: %w", keys.Key(kv.Key), err)
		}
		for _, tid := range rp.TableIDs {
			if tid == id {
				return false, nil
			}
		}
	}
	return true, nil
}

// TestingForgetDroppedSweepDone drops this node's memory that the sweep
// has finished, so the next pass reads the sweep's record again.
func (n *Node) TestingForgetDroppedSweepDone() { n.droppedSweepDone.Store(false) }
