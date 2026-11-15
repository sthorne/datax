package server

import (
	"context"
	"time"

	"github.com/sthorne/datax/pkg/sql"
	"github.com/sthorne/datax/pkg/util/log"
	"github.com/sthorne/datax/pkg/version"
)

const (
	defaultLargeObjectSweepInterval = time.Minute
	// largeObjectFirstPass is how soon after start (and how often until
	// it succeeds) a node checks that the tables exist.
	largeObjectFirstPass = 5 * time.Second
)

// largeObjectSweepLoop reclaims the pages of large-object contents that
// no object stands for (issue #303; pkg/sql/largeobject.go has the
// rule): unlinked objects, and uploads whose transaction ended without
// committing — a rollback, a dropped connection, a gateway that died
// mid-stream. Every node runs the loop; only the range-1 leader acts
// (the cluster-singleton idiom the other janitors use). Two nodes
// acting at once in a leadership change is harmless: a pass only
// deletes, and deleting twice is deleting once.
//
// The same loop creates the large-object tables once the cluster is at
// v21, so they are there before the first session asks for them
// (sql.EnsureLargeObjectTables says why that matters).
func (n *Node) largeObjectSweepLoop(ctx context.Context) {
	interval := n.cfg.LargeObjectSweepInterval
	if interval < 0 {
		return
	}
	if interval == 0 {
		interval = defaultLargeObjectSweepInterval
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	// The first look comes soon after start rather than an interval in:
	// a new cluster should have its tables within seconds.
	soon := time.NewTimer(largeObjectFirstPass)
	defer soon.Stop()
	ensured := false
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		case <-soon.C:
		}
		r1, ok := n.store.GetReplica(1)
		if !ok || !r1.IsLeader() || n.ClusterVersion() < version.V21 {
			if !ensured {
				soon.Reset(largeObjectFirstPass)
			}
			continue
		}
		cat, err := n.catalogAccessor()
		if err != nil {
			continue
		}
		if !ensured {
			if _, err := sql.EnsureLargeObjectTables(ctx, n.db, cat); err != nil {
				log.Debugf("large-object tables: %v", err)
				soon.Reset(largeObjectFirstPass)
				continue
			}
			ensured = true
		}
		if _, err := n.RunLargeObjectSweepOnce(ctx); err != nil {
			log.Debugf("large-object sweep: %v", err)
		}
	}
}

// RunLargeObjectSweepOnce runs one pass of the sweeper and returns what
// it reclaimed. Exported for tests, which drive passes instead of
// waiting on the loop. An error leaves the rest for the next pass:
// unreclaimed pages are only disk.
func (n *Node) RunLargeObjectSweepOnce(ctx context.Context) (sql.LargeObjectSweep, error) {
	cat, err := n.catalogAccessor()
	if err != nil {
		return sql.LargeObjectSweep{}, err
	}
	grace := n.cfg.LargeObjectUploadGrace
	if grace <= 0 {
		grace = sql.DefaultLargeObjectUploadGrace
	}
	res, err := sql.SweepLargeObjects(ctx, n.db, cat, grace)
	if res.Contents > 0 {
		// Recorded only when there was something to reclaim: a timeline
		// that fills with empty passes says nothing (issue #192).
		log.Infof("large-object sweep: reclaimed %d content(s), %d page(s)", res.Contents, res.Pages)
	}
	return res, err
}
