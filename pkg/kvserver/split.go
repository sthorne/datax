package kvserver

import (
	"fmt"

	"go.etcd.io/raft/v3/raftpb"

	"github.com/sthorne/datax/pkg/storage"
)

// Split and snapshot handling land in later phases; the hooks exist so the
// apply path is complete.

func (r *Replica) stageSplit(b *storage.Batch, trig *splitTrigger) error {
	return fmt.Errorf("splits not implemented yet")
}

func (r *Replica) finishSplit(trig *splitTrigger) {}

func (r *Replica) applySnapshot(snap raftpb.Snapshot) error {
	return fmt.Errorf("incoming raft snapshots not implemented yet")
}

func (r *Replica) maybeCompleteConfChange(cc raftpb.ConfChange) {}
