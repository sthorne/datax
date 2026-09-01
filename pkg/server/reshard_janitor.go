package server

import (
	"context"
	"encoding/json"
	"time"

	"github.com/sthorne/datax/pkg/base"
	"github.com/sthorne/datax/pkg/keys"
	"github.com/sthorne/datax/pkg/kvclient"
	"github.com/sthorne/datax/pkg/sql"
	"github.com/sthorne/datax/pkg/sql/catalog"
	"github.com/sthorne/datax/pkg/util/log"
)

const reshardJanitorInterval = time.Minute

// reshardJanitorLoop reclaims retired re-shard layouts. A completed
// re-shard leaves the superseded layout — old primary rows and old
// secondary-index generations — on disk so AS OF SYSTEM TIME below the
// swap keeps working; once a layout's RetiredAt ages past the keep
// window (ReshardRetireFor; default the GC TTL, matching the deepest
// timestamp a historical read can use anyway), it is unreachable by any
// admissible read and this loop wipes it. Every node runs the loop; only
// the range-1 leader acts (the cluster-singleton idiom the statistics
// sampler uses).
func (n *Node) reshardJanitorLoop(ctx context.Context) {
	ticker := time.NewTicker(reshardJanitorInterval)
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
		n.RunReshardJanitorOnce(ctx)
	}
}

// reshardKeepFor resolves the retired-layout keep window.
func (n *Node) reshardKeepFor() time.Duration {
	if n.cfg.ReshardRetireFor != 0 {
		return n.cfg.ReshardRetireFor
	}
	if n.cfg.GCTTL != 0 {
		return n.cfg.GCTTL
	}
	return base.DefaultGCTTL
}

// RunReshardJanitorOnce reclaims every retired layout past the keep
// window: the descriptor entry is removed FIRST (so the historical-read
// guard starts refusing before any rows disappear), then the layout's
// keyspaces are wiped. Errors are logged, never fatal — an unreclaimed
// layout is just disk.
func (n *Node) RunReshardJanitorOnce(ctx context.Context) {
	lo, hi := keys.TableDescSpan()
	descKVs, err := n.db.Scan(ctx, lo, hi, 0)
	if err != nil {
		log.Debugf("reshard janitor: descriptor scan: %v", err)
		return
	}
	now := n.clock.Now().WallTime
	keepFor := n.reshardKeepFor()
	for _, kv := range descKVs {
		var d catalog.TableDescriptor
		if json.Unmarshal(kv.Value, &d) != nil || d.ID == 0 || len(d.RetiredLayouts) == 0 {
			continue
		}
		var expired []catalog.RetiredLayout
		for _, rl := range d.RetiredLayouts {
			if now-rl.RetiredAt > keepFor.Nanoseconds() {
				expired = append(expired, rl)
			}
		}
		if len(expired) == 0 {
			continue
		}
		// Drop the entries transactionally against the current descriptor
		// (a racing re-shard may have appended more layouts).
		err := n.db.RunTxn(ctx, "reshard-janitor", func(ctx context.Context, txn *kvclient.Txn) error {
			raw, err := txn.Get(ctx, keys.TableDescKey(d.ID))
			if err != nil || raw == nil {
				return err // dropped table: its layouts are orphaned with the rest of its data
			}
			var cur catalog.TableDescriptor
			if err := json.Unmarshal(raw, &cur); err != nil {
				return err
			}
			kept := cur.RetiredLayouts[:0]
			for _, rl := range cur.RetiredLayouts {
				if now-rl.RetiredAt <= keepFor.Nanoseconds() {
					kept = append(kept, rl)
				}
			}
			if len(kept) == len(cur.RetiredLayouts) {
				return nil
			}
			cur.RetiredLayouts = kept
			if len(cur.RetiredLayouts) == 0 {
				cur.RetiredLayouts = nil
			}
			cur.Version++
			out, err := json.Marshal(&cur)
			if err != nil {
				return err
			}
			return txn.Put(ctx, keys.TableDescKey(d.ID), out)
		})
		if err != nil {
			log.Debugf("reshard janitor: descriptor %d: %v", d.ID, err)
			continue
		}
		for _, rl := range expired {
			sql.WipeIndexEntries(ctx, n.db, d.ID, rl.PrimaryIndexID)
			for _, id := range rl.IndexIDs {
				sql.WipeIndexEntries(ctx, n.db, d.ID, id)
			}
			log.Infof("reshard janitor: reclaimed retired layout (table %d, primary index %d, %d secondary generation(s))",
				d.ID, rl.PrimaryIndexID, len(rl.IndexIDs))
		}
	}
}
