package server

import (
	"context"
	"time"

	"github.com/sthorne/datax/pkg/sql/catalog"
	"github.com/sthorne/datax/pkg/util/log"
	"github.com/sthorne/datax/pkg/version"
)

const viewDependencyRecordInterval = time.Minute

// viewDependencyRecordLoop completes the records of which views read
// which relations (issue #313; catalog.RecordAllViewDependencies) once
// the cluster is at v22, on the node leading range 1, and then stops:
// from then on every view is recorded by the transaction that creates
// it.
func (n *Node) viewDependencyRecordLoop(ctx context.Context) {
	ticker := time.NewTicker(viewDependencyRecordInterval)
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
		if n.RecordViewDependenciesOnce(ctx) {
			return
		}
	}
}

// RecordViewDependenciesOnce records every view's dependencies and marks
// the records complete, if the cluster is at v22 and they are not
// already; it reports whether they are complete.
func (n *Node) RecordViewDependenciesOnce(ctx context.Context) bool {
	if n.ClusterVersion() < version.V22 {
		return false
	}
	done, err := catalog.RecordAllViewDependencies(ctx, n.db)
	if err != nil {
		log.Debugf("view dependency records: %v", err)
	}
	return done
}
