package log

import "sync/atomic"

// The audit stream records security-relevant actions with the acting
// principal: authentication failures (SQL and HTTP), user/privilege DDL,
// and destructive admin operations. Records ride the ordinary process log
// at Info level under the fixed message "audit" with an "event" attribute
// and structured key/value pairs, so they are grep-able (`msg=audit`) and
// machine-parsable without a second log file.

var auditSink atomic.Pointer[func(event string, kv []any)]

// SetAuditSink installs (or clears, with nil) an additional receiver for
// audit records. Testing hook: production audit always goes to the log.
func SetAuditSink(fn func(event string, kv []any)) {
	if fn == nil {
		auditSink.Store(nil)
		return
	}
	auditSink.Store(&fn)
}

// Audit emits one audit record. kv are slog-style alternating key/value
// pairs; every record should carry a "principal" key.
func Audit(event string, kv ...any) {
	args := append([]any{"event", event}, kv...)
	get().Info("audit", args...)
	if fn := auditSink.Load(); fn != nil {
		(*fn)(event, kv)
	}
}
