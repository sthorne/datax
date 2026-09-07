package server

import (
	"math"
	"testing"
	"time"

	"github.com/sthorne/datax/pkg/pgwire"
)

// The bound has to sit ahead of the expensive work, so these tests assert
// on what the limiter permits rather than on a status code: a 429 after
// the PBKDF2 derivation has already run would satisfy a status-code test
// and none of the point (issue #195).

func TestAuthLimiterBoundsOneSource(t *testing.T) {
	l := newAuthLimiter(0)
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
	l := newAuthLimiter(0)
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
	l := newAuthLimiter(0)
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
	l := newAuthLimiter(0)
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

// The SQL door asks before the exchange and charges after a failure
// (issue #212), so a burst of correct sign-ins from one address — a
// pool starting — is never refused, and a burst of wrong ones is
// charged in full even though every one of them was asked before any
// had failed.
func TestAuthLimiterBudgetIsSpentOnFailureNotOnAsking(t *testing.T) {
	l := newAuthLimiter(0)
	now := time.Now()
	l.nowFn = func() time.Time { return now }

	// Asking spends nothing: far more asks than the burst all pass.
	for i := 0; i < authBurst*3; i++ {
		if !l.budget("10.0.0.1:5000", "alice") {
			t.Fatalf("ask %d refused when nothing has failed", i)
		}
	}
	// A burst of failures that all began together is charged when they
	// land, and the source then has no budget.
	for i := 0; i < authAccountBurst; i++ {
		l.spend("10.0.0.1:5000", "alice")
	}
	if l.budget("10.0.0.1:5000", "alice") {
		t.Fatalf("%d failures at one account left budget for another attempt", authAccountBurst)
	}
	// The per-source bucket is the wider one: another account from the
	// same source still has budget until the source's own burst is spent.
	if !l.budget("10.0.0.1:5000", "bob") {
		t.Fatal("failures at one account refused another account from the same source inside the source burst")
	}
	// A different source is unaffected.
	if !l.budget("10.0.0.2:5000", "alice") {
		t.Fatal("one source's failures throttled another")
	}
	// Success refunds; the budget is back at once.
	l.succeeded("10.0.0.1:5000", "alice")
	if !l.budget("10.0.0.1:5000", "alice") {
		t.Fatal("a success did not refund the budget")
	}
}

// A wave costs what a sequence costs. Every guess that asked before the
// first failure landed runs (that is the order of charging), and every
// one is charged when it lands — so a hundred at once puts the source a
// hundred seconds into debt, not a burst's worth. A floor shallower
// than the wave would be a discount the source could take again as
// soon as it refilled; the floor there is exists only so that an
// address is never held for longer than ten minutes.
func TestAuthLimiterAWaveIsChargedInFull(t *testing.T) {
	l := newAuthLimiter(0)
	now := time.Now()
	l.nowFn = func() time.Time { return now }

	const wave = 100
	for i := 0; i < wave; i++ {
		if !l.budget("10.0.0.1:5000", "alice") {
			t.Fatalf("guess %d of a wave refused before any had landed: asking must spend nothing", i)
		}
	}
	for i := 0; i < wave; i++ {
		l.spend("10.0.0.1:5000", "alice")
	}
	// A burst's worth of waiting — what a floor at the burst would have
	// made enough — buys nothing.
	now = now.Add(time.Duration(2*authBurst+1) * authRefill)
	if l.budget("10.0.0.1:5000", "alice") {
		t.Fatal("a wave of a hundred was forgiven after a burst's worth of waiting: waves are free")
	}
	// The wave is paid for a second at a time: budget returns exactly
	// when the debt is cleared, and not before.
	start := now.Add(-time.Duration(2*authBurst+1) * authRefill)
	now = start.Add(time.Duration(wave-authAccountBurst) * authRefill)
	if l.budget("10.0.0.1:5000", "alice") {
		t.Fatal("budget returned a second early")
	}
	now = start.Add(time.Duration(wave-authAccountBurst+1) * authRefill)
	if !l.budget("10.0.0.1:5000", "alice") {
		t.Fatal("a wave of a hundred cost more than a hundred seconds")
	}

	// The floor: at the default cap an address is never held for longer
	// than authDebtMin seconds, however wide the wave.
	for i := 0; i < 10*int(authDebtMin); i++ {
		l.spend("10.0.0.2:5000", "alice")
	}
	now = now.Add(time.Duration(authDebtMin-10) * authRefill)
	if l.budget("10.0.0.2:5000", "alice") {
		t.Fatal("budget before the floor's worth of waiting")
	}
	now = now.Add(11 * authRefill)
	if !l.budget("10.0.0.2:5000", "alice") {
		t.Fatal("the floor is not holding: an address is held for longer than authDebtMin")
	}
}

// The floor is never shallower than the widest wave the node admits —
// by construction, from the pre-authentication cap in force, not by two
// constants kept in step by a comment (review of #212). At the default
// cap that is authDebtMin, with a margin; a cap raised past it deepens
// the floor with it; no cap at all means no floor, since the wave is
// then bounded only by descriptors.
func TestAuthLimiterDebtFloorCoversTheWidestWave(t *testing.T) {
	if authDebtMin <= float64(pgwire.DefaultMaxPendingAuth) {
		t.Fatalf("the default floor (%v) must be deeper than the default pre-auth cap (%d), "+
			"or a source gets a discount on every wave at the defaults", authDebtMin, pgwire.DefaultMaxPendingAuth)
	}
	for _, tc := range []struct {
		configured int
		want       float64
	}{
		{0, authDebtMin},
		{pgwire.DefaultMaxPendingAuth, authDebtMin},
		{2000, 2000},
		{-1, math.Inf(1)},
	} {
		l := newAuthLimiter(tc.configured)
		if l.debtMax != tc.want {
			t.Errorf("cap %d: floor %v, want %v", tc.configured, l.debtMax, tc.want)
		}
		if cap := pgwire.EffectiveMaxPendingAuth(tc.configured); cap > 0 && l.debtMax < float64(cap) {
			t.Errorf("cap %d: floor %v is shallower than the widest wave (%d)", tc.configured, l.debtMax, cap)
		}
	}
	// And the deeper floor is the one in force: a wave as wide as a
	// raised cap is charged in full.
	l := newAuthLimiter(2000)
	now := time.Now()
	l.nowFn = func() time.Time { return now }
	for i := 0; i < 2000; i++ {
		l.spend("10.0.0.1:5000", "alice")
	}
	now = now.Add(time.Duration(authDebtMin+authBurst) * authRefill)
	if l.budget("10.0.0.1:5000", "alice") {
		t.Fatal("a wave as wide as a raised cap was forgiven at the default floor: the floor did not follow the cap")
	}
	now = now.Add(time.Duration(2000-authDebtMin) * authRefill)
	if !l.budget("10.0.0.1:5000", "alice") {
		t.Fatal("the wave cost more than its width in seconds")
	}
}
