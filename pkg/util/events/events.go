// Package events keeps a node's recent operational events — splits,
// merges, rebalances, lease transfers, repairs, snapshots, backups,
// upgrades, consistency failures, and the audit stream — in a bounded
// ring, so the dashboard can show what happened without parsing logs.
// Recording is a one-liner next to the log call that already describes
// the event; the ring is per node.
package events

import (
	"fmt"
	"sync"
	"time"
)

// RingSize bounds how many events a node keeps.
const RingSize = 500

// Event is one recorded occurrence.
type Event struct {
	Seq     uint64    `json:"seq"`
	At      time.Time `json:"at"`
	Kind    string    `json:"kind"`
	Summary string    `json:"summary"`
	// Audit marks records from the audit stream (authentication failures,
	// admin operations, privilege DDL): shown to admins only.
	Audit bool `json:"audit,omitempty"`
}

// Ring is a bounded, sequence-numbered event log.
type Ring struct {
	mu    sync.Mutex
	buf   [RingSize]Event
	n     int
	next  int
	seq   uint64
	sinks []func(Event)
}

// New returns an empty ring.
func New() *Ring { return &Ring{} }

// Record adds an event; format and args build the summary.
func (r *Ring) Record(kind, format string, args ...any) {
	r.record(false, kind, fmt.Sprintf(format, args...))
}

// RecordAudit adds an audit-stream event.
func (r *Ring) RecordAudit(kind, summary string) { r.record(true, kind, summary) }

func (r *Ring) record(audit bool, kind, summary string) {
	if r == nil {
		return
	}
	r.mu.Lock()
	r.seq++
	ev := Event{Seq: r.seq, At: time.Now(), Kind: kind, Summary: summary, Audit: audit}
	r.buf[r.next] = ev
	r.next = (r.next + 1) % RingSize
	if r.n < RingSize {
		r.n++
	}
	sinks := r.sinks
	r.mu.Unlock()
	for _, fn := range sinks {
		fn(ev)
	}
}

// OnRecord registers fn to run with every event recorded.
func (r *Ring) OnRecord(fn func(Event)) {
	r.mu.Lock()
	r.sinks = append(r.sinks, fn)
	r.mu.Unlock()
}

// Recent returns events with Seq > since, oldest first, at most limit
// (0 = all), omitting audit records unless includeAudit is set.
func (r *Ring) Recent(since uint64, limit int, includeAudit bool) []Event {
	if r == nil {
		return nil
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]Event, 0, r.n)
	start := (r.next - r.n + RingSize) % RingSize
	for i := 0; i < r.n; i++ {
		ev := r.buf[(start+i)%RingSize]
		if ev.Seq <= since || (ev.Audit && !includeAudit) {
			continue
		}
		out = append(out, ev)
	}
	if limit > 0 && len(out) > limit {
		out = out[len(out)-limit:]
	}
	return out
}

// Seq returns the latest sequence number (0 when empty).
func (r *Ring) Seq() uint64 {
	if r == nil {
		return 0
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.seq
}
