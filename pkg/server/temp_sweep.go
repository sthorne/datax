package server

import (
	"context"
	"time"

	"github.com/sthorne/datax/pkg/base"
	"github.com/sthorne/datax/pkg/sql"
	"github.com/sthorne/datax/pkg/util/log"
)

// The orphan sweep of temporary tables (gap PG-D-009). A session drops
// its temporary tables when it ends (pkg/sql/temptable.go); one whose
// node stopped or crashed cannot, and leaves them behind. Every node
// sweeps, every TempTableSweepInterval, the temporary tables of sessions
// that are no longer live:
//
//   - of this node, those of an earlier instance (the node restarted)
//     and those of a session of this instance that has ended (its own
//     drop failed);
//   - of another node, those of a node whose liveness record is older
//     than the dead-node threshold (the node is gone, or cut off for so
//     long that its sessions could not have used them).
//
// A dropped table's rows go the way of any dropped table's: the reclaim
// job clears its span once its history window has passed.

// defaultTempTableSweepInterval is how often a node sweeps.
const defaultTempTableSweepInterval = time.Minute

// tempTableFirstPass is how soon after start the first sweep runs: a
// restarted node clears its earlier instance's tables quickly.
const tempTableFirstPass = 5 * time.Second

func (n *Node) tempTableSweepLoop(ctx context.Context) {
	interval := n.cfg.TempTableSweepInterval
	if interval < 0 {
		return
	}
	if interval == 0 {
		interval = defaultTempTableSweepInterval
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	soon := time.NewTimer(tempTableFirstPass)
	defer soon.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		case <-soon.C:
		}
		if _, err := n.SweepTempTablesOnce(ctx); err != nil && ctx.Err() == nil {
			log.Debugf("temporary table sweep: %v", err)
		}
	}
}

// SweepTempTablesOnce runs one pass of the sweep and returns how many
// orphaned temporary tables it dropped.
func (n *Node) SweepTempTablesOnce(ctx context.Context) (int, error) {
	srv := n.sqlServer()
	if srv == nil {
		return 0, nil
	}
	cat, err := n.catalogAccessor()
	if err != nil {
		return 0, err
	}
	reg := srv.TempTables()
	now := n.clock.Now().WallTime
	return sql.SweepOrphanTempTables(ctx, n.db, cat, func(sid string) bool {
		node, instance, ok := sql.ParseTempSession(sid)
		if !ok {
			return false
		}
		if node == reg.Node() {
			return instance != reg.Instance() || !reg.Live(sid)
		}
		nd, known := n.registry.Get(base.NodeID(node))
		return known && now-nd.LivenessTime > int64(n.deadNodeThreshold())
	})
}
