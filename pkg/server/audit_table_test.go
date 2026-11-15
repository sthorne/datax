package server

import (
	"encoding/json"
	"strings"
	"testing"
	"time"
)

// TestAuditEntry (issue #293): how one log.Audit call becomes a record
// — who, what it acted on, what came of it, and the rest as a JSON
// object that holds no secret, no row value, and nothing unbounded.
func TestAuditEntry(t *testing.T) {
	e := newAuditEntry(42, "role-ddl", []any{"stmt", "CREATE ROLE", "target", "alice", "login", true, "principal", "root", "role", "root"})
	if e.principal != "root" || e.target != "alice" || e.outcome != "ok" || e.refusal {
		t.Fatalf("role-ddl: %+v", e)
	}
	if got := e.detail(); got != `{"stmt":"CREATE ROLE","login":true,"role":"root"}` {
		t.Fatalf("detail: %s", got)
	}

	// The outcome a record states wins; one it does not state follows
	// from the kind of event.
	for _, c := range []struct {
		event   string
		kv      []any
		outcome string
	}{
		{"admin-op", []any{"op", "split", "outcome", "error", "error", "boom"}, "error"},
		{"sql-auth-failure", []any{"principal", "mallory"}, "failed"},
		{"http-auth-failure", []any{"principal", ""}, "failed"},
		{"admin-denied", []any{"principal", "bob"}, "denied"},
		{"auth-throttled", []any{"path", "sql"}, "denied"},
		{"http-login", []any{"principal", "bob"}, "ok"},
	} {
		if got := newAuditEntry(1, c.event, c.kv).outcome; got != c.outcome {
			t.Errorf("%s: outcome %q, want %q", c.event, got, c.outcome)
		}
	}

	// An admin operation is its op; a split key is a row's primary key
	// and is left to the log.
	e = newAuditEntry(1, "admin-op", []any{"op", "split", "principal", "root", "key", "/Table/52/1/'alice@example.com'", "path", "/tmp/x", "outcome", "ok"})
	if e.target != "split" || strings.Contains(e.detail(), "alice@example.com") || !strings.Contains(e.detail(), `"path":"/tmp/x"`) {
		t.Fatalf("admin-op: target %q detail %s", e.target, e.detail())
	}

	// No call site passes a secret; one that did would not get it
	// stored.
	e = newAuditEntry(1, "role-ddl", []any{"password", "hunter2", "Auth_Token", "abc", "share", "1-deadbeef", "share_index", int64(2), "backup_key", "given", "key_id", "k1"})
	d := e.detail()
	for _, secret := range []string{"hunter2", "abc", "deadbeef"} {
		if strings.Contains(d, secret) {
			t.Fatalf("secret %q stored: %s", secret, d)
		}
	}
	for _, kept := range []string{`"share_index":2`, `"backup_key":"given"`, `"key_id":"k1"`} {
		if !strings.Contains(d, kept) {
			t.Fatalf("%s missing: %s", kept, d)
		}
	}

	// Long values are cut, and the result is still valid JSON whatever
	// the cut landed on.
	e = newAuditEntry(1, "admin-op", []any{"error", strings.Repeat("é", 2000)})
	var m map[string]any
	if err := json.Unmarshal([]byte(e.detail()), &m); err != nil || len(m["error"].(string)) > auditValueLimit+8 {
		t.Fatalf("long value: %v, %d bytes", err, len(e.detail()))
	}
}

// TestAuditBuffer: the buffer is bounded and says what it lost; the
// same refusal repeated is one record with a count; refusals past
// their rate are counted, not queued; and a batch that could not be
// written goes back in front of what arrived since.
func TestAuditBuffer(t *testing.T) {
	a := &auditTable{folds: map[string]int{}, refusalTokens: auditRefusalBurst}
	now := time.Now()

	// One caller failing to log in, from a new port each time.
	for i := 0; i < 50; i++ {
		a.push(newAuditEntry(1, "sql-auth-failure", []any{"principal", "mallory", "remote", "10.0.0.9:" + string(rune('0'+i%10)) + "000"}), now)
	}
	if len(a.queue) != 1 || a.queue[0].count != 50 || !strings.Contains(a.queue[0].detail(), `"remote":"10.0.0.9","count":50`) {
		t.Fatalf("fold: %d entries, %+v", len(a.queue), a.queue)
	}
	// A different principal each time: the burst is recorded, the rest
	// suppressed and counted.
	for i := 0; i < 1000; i++ {
		a.push(newAuditEntry(1, "sql-auth-failure", []any{"principal", strings.Repeat("u", i+1)}), now)
	}
	if len(a.queue) != auditRefusalBurst || a.suppressed != 1000-(auditRefusalBurst-1) {
		t.Fatalf("rate: %d queued, %d suppressed", len(a.queue), a.suppressed)
	}
	// The rate refills with time.
	a.push(newAuditEntry(1, "sql-auth-failure", []any{"principal", "later"}), now.Add(time.Second))
	if len(a.queue) != auditRefusalBurst+1 {
		t.Fatalf("refill: %d queued", len(a.queue))
	}

	// Ordinary events are never rate limited, only bounded.
	for i := 0; i < auditBufferSize+10; i++ {
		a.push(newAuditEntry(1, "role-ddl", []any{"principal", "root"}), now)
	}
	if len(a.queue) != auditBufferSize || a.dropped != int64(auditRefusalBurst+1+10) {
		t.Fatalf("bound: %d queued, %d dropped", len(a.queue), a.dropped)
	}

	batch, dropped, _ := a.take(auditBatchSize)
	if len(batch) != auditBatchSize || len(a.queue) != auditBufferSize-auditBatchSize || dropped != a.dropped {
		t.Fatalf("take: %d taken, %d left", len(batch), len(a.queue))
	}
	if batch[0].event != "sql-auth-failure" || batch[0].count != 50 {
		t.Fatalf("take order: %+v", batch[0])
	}
	// Events arrive while the write is failing; the batch returns to
	// the front and the newest are what no longer fits.
	for i := 0; i < auditBatchSize; i++ {
		a.push(newAuditEntry(1, "view-ddl", []any{"principal", "root"}), now)
	}
	before := a.dropped
	a.putBack(batch)
	if len(a.queue) != auditBufferSize || a.queue[0].count != 50 || a.dropped != before+auditBatchSize {
		t.Fatalf("putBack: %d queued, first %+v, dropped %d (was %d)", len(a.queue), a.queue[0], a.dropped, before)
	}
	if last := a.queue[len(a.queue)-1]; last.event != "role-ddl" {
		t.Fatalf("putBack kept the newest: %+v", last)
	}
}

// TestAuditKeyUseFolds (issue #310): a use of the key service is
// recorded every time but folded within a flush — a hot unwrap loop is
// one record with a count, not a row per call — and is never subject to
// the refusal rate; a different outcome or version is its own record.
// Its rate-limit refusals are refusals.
func TestAuditKeyUseFolds(t *testing.T) {
	a := &auditTable{folds: map[string]int{}, refusalTokens: auditRefusalBurst}
	now := time.Now()
	use := func(version int64, outcome string) []any {
		return []any{"principal", "app", "target", "billing", "role", "app", "op", "unwrap", "key_id", int64(7), "version", version, "outcome", outcome}
	}
	for i := 0; i < 5000; i++ {
		a.push(newAuditEntry(1, "key-use", use(1, "ok")), now)
	}
	a.push(newAuditEntry(1, "key-use", use(2, "ok")), now)
	a.push(newAuditEntry(1, "key-use", use(1, "failed")), now)
	if len(a.queue) != 3 || a.queue[0].count != 5000 || a.suppressed != 0 || a.dropped != 0 {
		t.Fatalf("fold: %d entries, first %+v, suppressed %d", len(a.queue), a.queue[0], a.suppressed)
	}
	if d := a.queue[0].detail(); d != `{"role":"app","op":"unwrap","key_id":7,"version":1,"count":5000}` || a.queue[0].target != "billing" {
		t.Fatalf("detail: %s target %q", d, a.queue[0].target)
	}
	_, _, _ = a.take(auditBatchSize)
	a.push(newAuditEntry(1, "key-use", use(1, "ok")), now)
	if len(a.queue) != 1 || a.queue[0].count != 1 {
		t.Fatalf("a fold outlived its flush: %+v", a.queue)
	}
	if e := newAuditEntry(1, "key-use-throttled", use(1, "throttled")); !e.refusal {
		t.Fatal("a rate-limit refusal is not a refusal")
	}
}
