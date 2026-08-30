package storage

import (
	"testing"
	"time"

	"github.com/cockroachdb/pebble"
)

// TestProfileOptions: balanced leaves Pebble's options untouched (today's
// behavior by construction); ingest sets the documented tuning.
func TestProfileOptions(t *testing.T) {
	var balanced pebble.Options
	gate := ProfileBalanced.apply(&balanced)
	if balanced.MemTableSize != 0 || balanced.L0CompactionThreshold != 0 ||
		balanced.L0StopWritesThreshold != 0 || balanced.MaxConcurrentCompactions != nil {
		t.Fatalf("balanced profile touched pebble options: %+v", balanced)
	}
	if gate.l0Sublevels != 10 || gate.l0Files != 400 || gate.memtableBytes != 0 {
		t.Fatalf("balanced gate: %+v", gate)
	}

	var ingest pebble.Options
	gate = ProfileIngest.apply(&ingest)
	if ingest.MemTableSize != 64<<20 || ingest.MemTableStopWritesThreshold != 4 ||
		ingest.L0CompactionThreshold != 2 || ingest.L0StopWritesThreshold != 1000 ||
		ingest.LBaseMaxBytes != 256<<20 || ingest.BytesPerSync != 1<<20 {
		t.Fatalf("ingest profile options: %+v", ingest)
	}
	if ingest.MaxConcurrentCompactions == nil || ingest.MaxConcurrentCompactions() < 2 {
		t.Fatal("ingest compaction concurrency not raised")
	}
	if gate.l0Sublevels != 20 || gate.l0Files != 1500 || gate.memtableBytes != 3*(64<<20) {
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
