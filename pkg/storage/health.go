package storage

import (
	"fmt"
	"sync/atomic"
	"time"
)

// Engine health: a cheap, cached view of Pebble's write-path pressure that
// the KV layer polls on every write. pebble.DB.Metrics() takes the DB
// mutex, so the snapshot is refreshed at most once per metricsTTL — the
// cache is load-bearing, not an optimization.

const metricsTTL = time.Second

// StorageMetrics is a point-in-time snapshot of engine write-path health.
type StorageMetrics struct {
	L0Files             int
	L0Sublevels         int
	CompactionDebtBytes uint64
	MemtableCount       int64
	MemtableBytes       uint64
	WriteStalls         int64 // cumulative Pebble write-stall events
	DiskSlowEvents      int64 // cumulative slow-disk events
	BackgroundErrors    int64 // cumulative background (compaction/flush) errors
}

// health carries the engine's event counters and cached metrics snapshot.
type health struct {
	stalls    atomic.Int64
	inStall   atomic.Bool
	diskSlow  atomic.Int64
	bgErrors  atomic.Int64
	snapshot  atomic.Pointer[StorageMetrics]
	refreshed atomic.Int64 // unix nanos of the last snapshot refresh
	gate      softGate
}

// StorageMetrics returns the engine's health snapshot, refreshing it from
// Pebble at most once per second.
func (e *Engine) StorageMetrics() StorageMetrics {
	now := time.Now().UnixNano()
	last := e.health.refreshed.Load()
	if s := e.health.snapshot.Load(); s != nil && now-last < int64(metricsTTL) {
		return *s
	}
	// Best-effort single-flight: first CAS winner refreshes, losers serve
	// the previous snapshot (or refresh too on the very first call).
	if !e.health.refreshed.CompareAndSwap(last, now) {
		if s := e.health.snapshot.Load(); s != nil {
			return *s
		}
	}
	m := e.db.Metrics()
	s := StorageMetrics{
		CompactionDebtBytes: m.Compact.EstimatedDebt,
		MemtableCount:       m.MemTable.Count,
		MemtableBytes:       m.MemTable.Size,
		L0Files:             int(m.Levels[0].NumFiles),
		L0Sublevels:         int(m.Levels[0].Sublevels),
		WriteStalls:         e.health.stalls.Load(),
		DiskSlowEvents:      e.health.diskSlow.Load(),
		BackgroundErrors:    e.health.bgErrors.Load(),
	}
	e.health.snapshot.Store(&s)
	return s
}

// Overloaded reports whether the engine has crossed the profile's soft
// backpressure thresholds, with a reason naming the tripped signal. It is
// called on every KV write, so it only ever reads the cached snapshot.
func (e *Engine) Overloaded() (bool, string) {
	if e.health.inStall.Load() {
		return true, "pebble write stall in progress"
	}
	s := e.StorageMetrics()
	g := e.health.gate
	switch {
	case g.l0Sublevels > 0 && s.L0Sublevels >= g.l0Sublevels:
		return true, fmt.Sprintf("L0 sublevels %d >= %d", s.L0Sublevels, g.l0Sublevels)
	case g.l0Files > 0 && s.L0Files >= g.l0Files:
		return true, fmt.Sprintf("L0 files %d >= %d", s.L0Files, g.l0Files)
	case g.memtableBytes > 0 && s.MemtableBytes >= g.memtableBytes:
		return true, fmt.Sprintf("memtable bytes %d >= %d", s.MemtableBytes, g.memtableBytes)
	}
	return false, ""
}
