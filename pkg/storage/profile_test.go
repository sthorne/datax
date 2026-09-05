package storage

import (
	"testing"
	"time"

	"github.com/cockroachdb/pebble/v2"
)

// TestProfileOptions: balanced leaves Pebble's options untouched (today's
// behavior by construction); ingest sets the documented tuning.
func TestProfileOptions(t *testing.T) {
	var balanced pebble.Options
	gate := ProfileBalanced.apply(&balanced)
	if balanced.MemTableSize != 0 || balanced.L0CompactionThreshold != 0 ||
		balanced.L0StopWritesThreshold != 0 || balanced.CompactionConcurrencyRange != nil {
		t.Fatalf("balanced profile touched pebble options: %+v", balanced)
	}
	if gate.l0Sublevels != 10 || gate.l0Files != 400 || gate.memtableBytes != 0 ||
		gate.debtHigh != 2<<30 || gate.debtLow != 1<<30 {
		t.Fatalf("balanced gate: %+v", gate)
	}

	var ingest pebble.Options
	gate = ProfileIngest.apply(&ingest)
	if ingest.MemTableSize != 64<<20 || ingest.MemTableStopWritesThreshold != 4 ||
		ingest.L0CompactionThreshold != 2 || ingest.L0StopWritesThreshold != 1000 ||
		ingest.LBaseMaxBytes != 256<<20 || ingest.BytesPerSync != 1<<20 {
		t.Fatalf("ingest profile options: %+v", ingest)
	}
	if ingest.CompactionConcurrencyRange == nil {
		t.Fatal("ingest profile left the compaction concurrency at Pebble's default")
	}
	if _, upper := ingest.CompactionConcurrencyRange(); upper < 2 {
		t.Fatal("ingest compaction concurrency not raised")
	}
	if gate.l0Sublevels != 20 || gate.l0Files != 1500 || gate.memtableBytes != 3*(64<<20) ||
		gate.debtHigh != 8<<30 || gate.debtLow != 4<<30 {
		t.Fatalf("ingest gate: %+v", gate)
	}

	if _, err := ParseProfile("ingest"); err != nil {
		t.Fatal(err)
	}
	if p, err := ParseProfile(""); err != nil || p != ProfileBalanced {
		t.Fatalf("empty profile: %v %v", p, err)
	}
	if _, err := ParseProfile("nope"); err == nil {
		t.Fatal("bad profile accepted")
	}
}

// TestOverloaded: the soft gate trips on each signal (against an injected
// cached snapshot) and on an in-progress Pebble stall.
func TestOverloaded(t *testing.T) {
	e := &Engine{}
	e.health.gate = softGate{l0Sublevels: 20, l0Files: 1500, memtableBytes: 3 * (64 << 20)}
	inject := func(s StorageMetrics) {
		e.health.snapshot.Store(&s)
		e.health.refreshed.Store(time.Now().UnixNano())
	}

	inject(StorageMetrics{L0Sublevels: 5, L0Files: 10, MemtableCount: 7, MemtableBytes: 1 << 20})
	if over, why := e.Overloaded(); over {
		t.Fatalf("healthy engine overloaded: %s", why)
	}
	inject(StorageMetrics{L0Sublevels: 20})
	if over, _ := e.Overloaded(); !over {
		t.Fatal("sublevel threshold did not trip")
	}
	inject(StorageMetrics{L0Files: 1500})
	if over, _ := e.Overloaded(); !over {
		t.Fatal("file threshold did not trip")
	}
	inject(StorageMetrics{MemtableBytes: 3 * (64 << 20)})
	if over, _ := e.Overloaded(); !over {
		t.Fatal("memtable-bytes threshold did not trip")
	}
	inject(StorageMetrics{})
	e.health.inStall.Store(true)
	if over, why := e.Overloaded(); !over || why != "pebble write stall in progress" {
		t.Fatalf("in-stall not reported: %v %s", over, why)
	}
	e.health.inStall.Store(false)

	// Balanced-style gate with the memtable criterion disabled never trips
	// on memtable state alone.
	e.health.gate = softGate{l0Sublevels: 10, l0Files: 400}
	inject(StorageMetrics{MemtableCount: 50, MemtableBytes: 1 << 30})
	if over, _ := e.Overloaded(); over {
		t.Fatal("disabled memtable criterion tripped")
	}
}

// TestDebtGateHysteresis: the compaction-debt gate latches at the high
// water, stays latched between the waters, releases at the low water, and
// counts each entry once. While latched (and no engine gate trips) the
// overload cause is "debt".
func TestDebtGateHysteresis(t *testing.T) {
	e := &Engine{}
	e.health.gate = softGate{l0Sublevels: 10, l0Files: 400, debtHigh: 2 << 30, debtLow: 1 << 30}
	inject := func(s StorageMetrics) {
		e.health.snapshot.Store(&s)
		e.health.refreshed.Store(time.Now().UnixNano())
		e.updateDebtGate(s.CompactionDebtBytes)
	}

	inject(StorageMetrics{CompactionDebtBytes: 1 << 30})
	if e.DebtGated() {
		t.Fatal("gate latched below high water")
	}
	inject(StorageMetrics{CompactionDebtBytes: 2 << 30})
	if !e.DebtGated() || e.DebtGateEntries() != 1 {
		t.Fatalf("gate at high water: latched=%v entries=%d", e.DebtGated(), e.DebtGateEntries())
	}
	if over, cause, _ := e.OverloadedCause(); !over || cause != CauseDebt {
		t.Fatalf("latched gate not reported: %v %s", over, cause)
	}
	// Between the waters: still latched (no flapping), no double count.
	inject(StorageMetrics{CompactionDebtBytes: (1 << 30) + (1 << 29)})
	if !e.DebtGated() || e.DebtGateEntries() != 1 {
		t.Fatalf("hysteresis window: latched=%v entries=%d", e.DebtGated(), e.DebtGateEntries())
	}
	// At/below the low water: releases.
	inject(StorageMetrics{CompactionDebtBytes: 1 << 30})
	if e.DebtGated() {
		t.Fatal("gate did not release at low water")
	}
	if over, _, _ := e.OverloadedCause(); over {
		t.Fatal("released gate still overloaded")
	}
	// Re-entry counts again.
	inject(StorageMetrics{CompactionDebtBytes: 3 << 30})
	if !e.DebtGated() || e.DebtGateEntries() != 2 {
		t.Fatalf("re-entry: latched=%v entries=%d", e.DebtGated(), e.DebtGateEntries())
	}
	// Immediate engine signals outrank the debt cause.
	inject(StorageMetrics{CompactionDebtBytes: 3 << 30, L0Sublevels: 10})
	if _, cause, _ := e.OverloadedCause(); cause != CauseEngine {
		t.Fatalf("engine signal outranked by debt: %s", cause)
	}
	// A disabled gate (no thresholds) never latches.
	e2 := &Engine{}
	e2.health.gate = softGate{l0Sublevels: 10}
	e2.updateDebtGate(100 << 30)
	if e2.DebtGated() {
		t.Fatal("disabled debt gate latched")
	}
}

// TestStorageMetricsLive: a real engine serves a snapshot and refreshes it
// after the TTL.
func TestStorageMetricsLive(t *testing.T) {
	e, err := Open("", Options{Profile: ProfileIngest})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = e.Close() }()
	for i := 0; i < 100; i++ {
		if err := e.Put([]byte{0x04, byte(i)}, make([]byte, 128)); err != nil {
			t.Fatal(err)
		}
	}
	if err := e.Flush(); err != nil {
		t.Fatal(err)
	}
	s := e.StorageMetrics()
	if s.MemtableCount < 1 {
		t.Fatalf("no memtables reported: %+v", s)
	}
	if over, why := e.Overloaded(); over {
		t.Fatalf("tiny engine overloaded: %s", why)
	}
}
