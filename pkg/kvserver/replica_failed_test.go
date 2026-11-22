package kvserver

import (
	"errors"
	"slices"
	"testing"

	"github.com/sthorne/datax/pkg/base"
)

// TestFailedReplicaIsReported: a replica whose group a raft-engine write
// error stopped (ENOSPC, EIO) stays in the store map, serving nothing, and
// the store lists it so the node can report it; a clean-shutdown abort
// is not a failure.
func TestFailedReplicaIsReported(t *testing.T) {
	env := startClearTestStore(t)
	if got := env.store.FailedReplicas(); len(got) != 0 {
		t.Fatalf("failed replicas before any failure: %v", got)
	}
	env.r.failReady(errApplyAborted)
	if env.r.Failed() {
		t.Fatal("an apply aborted by shutdown counted as a failed replica")
	}
	env.r.failReady(errors.New("raft engine commit: no space left on device"))
	if !env.r.Failed() {
		t.Fatal("a replica stopped by a raft engine write error is not marked failed")
	}
	if _, ok := env.store.GetReplica(env.r.rangeID); !ok {
		t.Fatal("the failed replica left the store map")
	}
	if got, want := env.store.FailedReplicas(), []base.RangeID{env.r.rangeID}; !slices.Equal(got, want) {
		t.Fatalf("FailedReplicas = %v, want %v", got, want)
	}
}
