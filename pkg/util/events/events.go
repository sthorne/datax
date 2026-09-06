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
	// Op and Phase pair the two ends of a long-running operation
	// (issue #153). Most events are instants and carry neither. An
	// operation records a "start" and later an "end" under one Op id, so
	// a reader can tell a decommission that began twenty minutes ago
	// from one that finished last week — which a flat log of instants
	// cannot express. Outcome is set on the end ("ok", or why not).
	//
	// The ring stays the audit trail it is: the pairing is a label on
	// the records, and the operations view is derived from it. This is
	// deliberately not a job store.
	Op      string `json:"op,omitempty"`
	Phase   string `json:"phase,omitempty"`
	Outcome string `json:"outcome,omitempty"`
}

// Phases of a paired operation.
const (
	PhaseStart = "start"
	PhaseEnd   = "end"
)

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

// RecordStart opens a long-running operation under op; RecordEnd closes
// it with an outcome. The two are matched by (kind, op).
func (r *Ring) RecordStart(kind, op, format string, args ...any) {
	r.recordOp(kind, op, PhaseStart, "", fmt.Sprintf(format, args...))
}

// RecordEnd closes the operation opened under op. outcome is "ok" or a
// short reason it was not.
func (r *Ring) RecordEnd(kind, op, outcome, format string, args ...any) {
	r.recordOp(kind, op, PhaseEnd, outcome, fmt.Sprintf(format, args...))
}

func (r *Ring) recordOp(kind, op, phase, outcome, summary string) {
	if r == nil {
		return
	}
	r.mu.Lock()
	r.seq++
	ev := Event{Seq: r.seq, At: time.Now(), Kind: kind, Summary: summary,
		Op: op, Phase: phase, Outcome: outcome}
	r.store(ev)
	sinks := r.sinks
	r.mu.Unlock()
	for _, fn := range sinks {
		fn(ev)
	}
}

func (r *Ring) record(audit bool, kind, summary string) {
	if r == nil {
		return
	}
	r.mu.Lock()
	r.seq++
	ev := Event{Seq: r.seq, At: time.Now(), Kind: kind, Summary: summary, Audit: audit}
	r.store(ev)
	sinks := r.sinks
	r.mu.Unlock()
	for _, fn := range sinks {
		fn(ev)
	}
}

// store puts ev in the ring; the caller holds the lock.
func (r *Ring) store(ev Event) {
	r.buf[r.next] = ev
	r.next = (r.next + 1) % RingSize
	if r.n < RingSize {
		r.n++
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

// Since returns the events at or after from, oldest first, at most
// limit (0 = all), omitting audit records unless includeAudit is set.
// Oldest is the timestamp of the oldest record the ring still holds —
// the annotation layer needs it to say when a window reaches back
// further than the ring does, rather than implying nothing happened
// (issue #155).
func (r *Ring) Since(from time.Time, limit int, includeAudit bool) (out []Event, oldest time.Time) {
	if r == nil {
		return nil, time.Time{}
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	start := (r.next - r.n + RingSize) % RingSize
	for i := 0; i < r.n; i++ {
		ev := r.buf[(start+i)%RingSize]
		if i == 0 {
			oldest = ev.At
		}
		if ev.Audit && !includeAudit {
			continue
		}
		if ev.At.Before(from) {
			continue
		}
		out = append(out, ev)
	}
	if limit > 0 && len(out) > limit {
		out = out[len(out)-limit:]
	}
	return out, oldest
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
