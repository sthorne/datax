package log

import (
	"sync"
	"sync/atomic"
)

// The audit stream records security-relevant actions with the acting
// principal: authentication failures (SQL and HTTP), user/privilege DDL,
// and destructive admin operations. Records ride the ordinary process log
// at Info level under the fixed message "audit" with an "event" attribute
// and structured key/value pairs, so they are grep-able (`msg=audit`) and
// machine-parsable without a second log file.

// AuditSink receives one audit record.
type AuditSink func(event string, kv []any)

var auditSink atomic.Pointer[AuditSink]

// SetAuditSink installs (or clears, with nil) an additional receiver for
// audit records. Testing hook: production audit always goes to the log.
// Independent of the subscribers added with AddAuditSink, so a test's
// sink is not displaced by the nodes it starts.
func SetAuditSink(fn func(event string, kv []any)) {
	if fn == nil {
		auditSink.Store(nil)
		return
	}
	s := AuditSink(fn)
	auditSink.Store(&s)
}

var auditSubs struct {
	mu   sync.Mutex
	next int
	fns  map[int]AuditSink
}

// AddAuditSink subscribes fn to every audit record until the returned
// function is called. Nodes use it to feed their event rings; every node
// in the process (tests, `datax demo`) receives every record.
func AddAuditSink(fn AuditSink) (remove func()) {
	auditSubs.mu.Lock()
	defer auditSubs.mu.Unlock()
	if auditSubs.fns == nil {
		auditSubs.fns = map[int]AuditSink{}
	}
	id := auditSubs.next
	auditSubs.next++
	auditSubs.fns[id] = fn
	return func() {
		auditSubs.mu.Lock()
		defer auditSubs.mu.Unlock()
		delete(auditSubs.fns, id)
	}
}

// Audit emits one audit record. kv are slog-style alternating key/value
// pairs; every record should carry a "principal" key.
func Audit(event string, kv ...any) {
	args := append([]any{"event", event}, kv...)
	get().Info("audit", args...)
	if fn := auditSink.Load(); fn != nil {
		(*fn)(event, kv)
	}
	auditSubs.mu.Lock()
	subs := make([]AuditSink, 0, len(auditSubs.fns))
	for _, fn := range auditSubs.fns {
		subs = append(subs, fn)
	}
	auditSubs.mu.Unlock()
	for _, fn := range subs {
		fn(event, kv)
	}
}
