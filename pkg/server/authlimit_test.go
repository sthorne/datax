package server

import (
	"testing"
	"time"
)

// The bound has to sit ahead of the expensive work, so these tests assert
// on what the limiter permits rather than on a status code: a 429 after
// the PBKDF2 derivation has already run would satisfy a status-code test
// and none of the point (issue #195).

func TestAuthLimiterBoundsOneSource(t *testing.T) {
	l := newAuthLimiter()
	now := time.Now()
	l.nowFn = func() time.Time { return now }

	// The burst is generous enough that a person mistyping never meets
	// it, and then the source has to wait.
	allowed := 0
	for i := 0; i < authBurst*4; i++ {
		if l.allow("10.0.0.1:5000", "alice") {
			allowed++
		}
	}
	if allowed > authBurst {
		t.Fatalf("%d attempts allowed from one source in a burst of %d", allowed, authBurst)
	}
	if allowed < authAccountBurst {
		t.Fatalf("only %d attempts allowed; a mistyped password must not be throttled", allowed)
	}

	// A different port on the same address shares the budget: opening a
	// new connection is what an attacker would do.
	if l.allow("10.0.0.1:9999", "alice") {
		t.Fatal("a new port from the same address got a fresh budget")
	}

	// Another source is unaffected — no lockout of the cluster by one
	// caller.
	if !l.allow("10.0.0.2:5000", "alice") {
		t.Fatal("one source exhausting its budget throttled another")
	}

	// The budget returns with time.
	now = now.Add(10 * authRefill)
	if !l.allow("10.0.0.1:5000", "alice") {
		t.Fatal("the budget never refilled")
	}
}

// A source working through passwords for one username meets a tighter
// bound than the source bound, and a legitimate sign-in for that same
// username from elsewhere is unaffected — which is why this is keyed by
// (source, account) and not by account. A limiter keyed by username
// alone would let an attacker lock a known user out.
func TestAuthLimiterDoesNotLockOutAnAccount(t *testing.T) {
	l := newAuthLimiter()
	now := time.Now()
	l.nowFn = func() time.Time { return now }

	for i := 0; i < authBurst*2; i++ {
		l.allow("10.0.0.1:5000", "alice")
	}
	if l.allow("10.0.0.1:5000", "alice") {
		t.Fatal("the attacker's source is not throttled")
	}
	if !l.allow("192.168.1.5:4000", "alice") {
		t.Fatal("alice was locked out of her own account by someone else guessing at it")
	}
}

func TestAuthLimiterCapsVerificationsInFlight(t *testing.T) {
	l := newAuthLimiter()
	for i := 0; i < authVerifyInFlight; i++ {
		if !l.acquireVerify() {
			t.Fatalf("slot %d refused below the cap", i)
		}
	}
	// Past the cap the answer is immediate: a caller made to wait is a
	// caller holding a goroutine, which is the resource being protected.
	done := make(chan bool, 1)
	go func() { done <- l.acquireVerify() }()
	select {
	case got := <-done:
		if got {
			t.Fatal("acquired a slot past the cap")
		}
	case <-time.After(time.Second):
		t.Fatal("acquireVerify blocked instead of refusing")
	}
	l.releaseVerify()
	if !l.acquireVerify() {
		t.Fatal("a released slot was not reusable")
	}
}

// The map is what an attacker rotating source addresses would grow.
func TestAuthLimiterMapIsBounded(t *testing.T) {
	l := newAuthLimiter()
	for i := 0; i < authLimiterMax*2; i++ {
		l.allow(net4(i)+":1000", "alice")
	}
	l.mu.Lock()
	n := len(l.buckets)
	l.mu.Unlock()
	// Two entries per attempt (source, and source+account), so the cap
	// is met at authLimiterMax and eviction keeps it there.
	if n > authLimiterMax+2 {
		t.Fatalf("%d buckets retained, cap is %d", n, authLimiterMax)
	}
}

func net4(i int) string {
	return "10." + itoaNode(i>>16&0xff) + "." + itoaNode(i>>8&0xff) + "." + itoaNode(i&0xff)
}

func TestSourceKeyIsTheHost(t *testing.T) {
	for addr, want := range map[string]string{
		"10.0.0.1:5000":   "10.0.0.1",
		"[::1]:5000":      "::1",
		"not-an-addr":     "not-an-addr",
		"127.0.0.1:65535": "127.0.0.1",
	} {
		if got := sourceKey(addr); got != want {
			t.Errorf("sourceKey(%q) = %q, want %q", addr, got, want)
		}
	}
}
