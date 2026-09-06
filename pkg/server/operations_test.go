package server

import (
	"testing"
	"time"

	"github.com/sthorne/datax/pkg/util/events"
)

// Pairing the ring's start/end records into operations (issue #153).
func TestOperationsFromPairsStartAndEnd(t *testing.T) {
	r := events.New()
	r.RecordStart("backup", "op1", "backup to /b started")
	r.Record("split", "r7 split at k") // an instant: not an operation
	r.RecordStart("decommission", "op2", "n3 draining")
	r.RecordEnd("backup", "op1", "ok", "backup written: 4 tables")

	evs := r.Recent(0, 0, true)
	now := time.Now().UnixMilli() + 1000
	ops := operationsFrom(evs, now)

	if len(ops) != 2 {
		t.Fatalf("got %d operations, want 2 (the instant is not one): %+v", len(ops), ops)
	}
	// Running first.
	if !ops[0].Running || ops[0].Kind != "decommission" {
		t.Fatalf("running operation should sort first: %+v", ops)
	}
	if ops[0].ElapsedMs <= 0 {
		t.Fatalf("a running operation reports how long it has been running: %+v", ops[0])
	}
	done := ops[1]
	if done.Running || done.Kind != "backup" || done.Outcome != "ok" {
		t.Fatalf("completed backup: %+v", done)
	}
	// The end's summary replaces the start's, so the row reads as the
	// outcome rather than the intent.
	if done.Summary != "backup written: 4 tables" {
		t.Fatalf("completed summary %q", done.Summary)
	}
	if done.EndedMs < done.StartedMs || done.ElapsedMs < 0 {
		t.Fatalf("completed timing: %+v", done)
	}
}

// An end whose start has aged out of the ring is still reported, with no
// elapsed time invented for it.
func TestOperationsFromEndWithoutStart(t *testing.T) {
	r := events.New()
	r.RecordEnd("re-encryption", "gone", "ok", "re-encryption complete")
	ops := operationsFrom(r.Recent(0, 0, true), time.Now().UnixMilli())
	if len(ops) != 1 {
		t.Fatalf("got %+v", ops)
	}
	if ops[0].Running || ops[0].StartedMs != 0 || ops[0].ElapsedMs != 0 {
		t.Fatalf("an end without a start claims no duration: %+v", ops[0])
	}
	if ops[0].Outcome != "ok" {
		t.Fatalf("outcome %q", ops[0].Outcome)
	}
}

// Since returns a time window and how far back the ring reaches (#155).
func TestRingSinceWindow(t *testing.T) {
	r := events.New()
	r.Record("split", "old")
	time.Sleep(10 * time.Millisecond)
	cut := time.Now()
	time.Sleep(10 * time.Millisecond)
	r.Record("merge", "new")
	r.RecordAudit("auth-failure", "secret")

	evs, oldest := r.Since(cut, 0, false)
	if len(evs) != 1 || evs[0].Summary != "new" {
		t.Fatalf("window: %+v", evs)
	}
	if oldest.IsZero() {
		t.Fatal("Since reports the oldest record the ring holds, so a caller can tell a short ring from a quiet cluster")
	}
	// Audit records stay admin-only through the window form too.
	evs, _ = r.Since(cut, 0, true)
	if len(evs) != 2 {
		t.Fatalf("an admin sees the audit record in the window: %+v", evs)
	}
}
