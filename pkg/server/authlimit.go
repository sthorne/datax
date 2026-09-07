package server

import (
	"math"
	"net"
	"sync"
	"time"

	"github.com/sthorne/datax/pkg/metrics"
	"github.com/sthorne/datax/pkg/pgwire"
	"github.com/sthorne/datax/pkg/util/log"
)

// Bounding what an unauthenticated caller can cost this node (issue
// #195).
//
// Verifying a password re-derives PBKDF2 at the SCRAM iteration count.
// That is the point of PBKDF2 and is correct as a per-login cost —
// measured at 0.72 ms of CPU on the machine the issue was written on.
// It is a problem as an unbounded one: /api/login is reachable before
// any credential is validated, so about 1,400 requests a second
// saturate a core, and a handful of connections pin the node. Raising
// the iteration count, which one otherwise would, makes it strictly
// worse — the sign that the missing bound is the real defect, not the
// cost.
//
// Two bounds, both ahead of the expensive work rather than after it:
//
//   - a token bucket per source address, and a tighter one per (source,
//     account). The first bounds what one caller can cost; the second
//     bounds guessing at one account without ever locking that account
//     out, which a per-account limiter would — and a lockout keyed by
//     username is itself a denial of service against a known username.
//   - a cap on verifications in flight at once, so many distinct sources
//     cannot together pin every core. This is the bound that does not
//     depend on the key being meaningful, which matters because behind a
//     proxy or NAT the source address is the proxy.
//
// A refusal answers identically whoever asked, so nothing here reopens
// the user enumeration that the uniform refusal closes.
//
// Deliberately not here: per-account backoff that survives across nodes.
// It needs cluster-wide state to mean anything against an attacker who
// spreads attempts over the cluster, and a design that does not turn
// into a lockout; both are more than this bound needs, and the bound is
// what stops the amplification.

const (
	// authBurst is how many attempts a source may make back-to-back
	// before it has to wait. Large enough that a person mistyping a
	// password never meets it.
	authBurst = 10
	// authRefill is how fast a source's budget returns: one attempt a
	// second sustained, which is far below what guessing needs and far
	// above what a human does.
	authRefill = time.Second
	// authAccountBurst is the same for one account from one source,
	// tighter because a source working through passwords for a single
	// username is the shape being bounded.
	authAccountBurst = 5
	// authLimiterMax bounds the map: past it the least recently used
	// entries are dropped, which forgets a limit rather than growing
	// without bound. An attacker rotating source addresses to evict
	// entries still meets the in-flight cap below.
	authLimiterMax = 4096
	// authDebtMin is the least far below zero a bucket may go. The SQL
	// door charges a failure after the exchange (spend), so a wave of
	// guesses that all began before any had failed is charged in full
	// when they land, and the source then waits a second for every one
	// of them — which is what makes a wave cost exactly what the same
	// guesses would cost one at a time (issue #212). A floor shallower
	// than the widest wave the node admits would be a discount on every
	// wave: a source could repeat one the moment the floor refilled. So
	// the floor in force is the deeper of this and the pre-authentication
	// connection cap (debtFloor), by construction rather than by keeping
	// two constants in step; what this constant sets is how long a shared
	// address can be held after the guessing stops, wherever the cap is
	// at or below it.
	authDebtMin = float64(10 * time.Minute / authRefill)
)

// debtFloor is how far below zero a bucket may go on a node whose
// pre-authentication cap is maxPending (0 = no cap): the widest wave the
// node can admit, or authDebtMin, whichever is deeper. With no cap a
// wave is bounded only by descriptors, and so is the debt — an operator
// who removes the cap removes the bound on how long a guessing address
// is held, and the flag says so.
func debtFloor(maxPending int) float64 {
	if maxPending <= 0 {
		return math.Inf(1)
	}
	return math.Max(authDebtMin, float64(maxPending))
}

// authVerifyInFlight caps concurrent password verifications. Four is
// enough that ordinary sign-ins never queue and small enough that
// verification cannot take every core.
const authVerifyInFlight = 4

// The verification budget (issue #221). The attempt buckets above are
// refunded by a success — they must be, or a legitimate user sharing an
// address with a guessing loop is locked out — and that refund made
// success a reset button: a caller holding any one valid credential was
// exempt from the bound altogether, at 470 derivations a second on the
// machine the issue was measured on, with only the in-flight cap left
// between it and the node's cores. What that bound is meant to hold is
// the cost, and the cost of a verification is the same whether or not
// the password was right. So a third bucket, per source, spent by every
// verification the HTTP doors run and refunded by nothing.
//
// Sized against how the doors are used, not against a person: HTTP
// Basic re-verifies on every request, so a scraper, a script or a
// support bundle spends it at its request rate. A hundred in a burst
// and twenty a second sustained is far above any of those and far
// below what it takes to pin a core (a derivation is under a
// millisecond). Nothing on the SQL door spends it: SCRAM's server side
// holds the stored key and runs no derivation, so there is no cost
// there to bound.
const (
	authVerifyBurst = 100
	authVerifyRate  = 20 // tokens a second
)

// authLimiter is a token bucket per key with a bounded, LRU-evicted map.
type authLimiter struct {
	mu      sync.Mutex
	buckets map[string]*authBucket
	verify  chan struct{} // the in-flight cap, as a semaphore
	nowFn   func() time.Time
	// debtMax is the floor (debtFloor), set once at construction.
	debtMax float64
}

type authBucket struct {
	tokens float64
	rate   float64 // tokens a second
	last   time.Time
	seen   time.Time // for eviction
}

// authRate is the attempt buckets' refill, in tokens a second.
var authRate = 1 / authRefill.Seconds()

// newAuthLimiter builds the limiter for a node whose pre-authentication
// cap is maxPending (as configured: 0 = the default, negative = none).
func newAuthLimiter(maxPending int) *authLimiter {
	return &authLimiter{
		buckets: map[string]*authBucket{},
		verify:  make(chan struct{}, authVerifyInFlight),
		nowFn:   time.Now,
		debtMax: debtFloor(pgwire.EffectiveMaxPendingAuth(maxPending)),
	}
}

// allow reports whether an attempt from source for user may proceed to
// the expensive verification. It consumes from both buckets, so a
// refusal by either costs the caller its budget in neither.
func (l *authLimiter) allow(source, user string) bool {
	src := sourceKey(source)
	if !l.take("s\x00"+src, authBurst) {
		return false
	}
	if !l.take("a\x00"+src+"\x00"+user, authAccountBurst) {
		return false
	}
	return true
}

func (l *authLimiter) take(key string, burst float64) bool {
	return l.takeAt(key, burst, authRate)
}

// takeAt is take for a bucket refilling at rate tokens a second.
func (l *authLimiter) takeAt(key string, burst, rate float64) bool {
	now := l.nowFn()
	l.mu.Lock()
	defer l.mu.Unlock()
	b := l.bucketLocked(key, burst, rate, now)
	if b.tokens < 1 {
		return false
	}
	b.tokens--
	return true
}

// allowVerify reports whether source may have one more password
// verified, spending one from its verification budget if so. Asked
// after allow and before the verification runs, whatever the outcome
// will be; nothing refunds it (issue #221).
func (l *authLimiter) allowVerify(source string) bool {
	return l.takeAt("v\x00"+sourceKey(source), authVerifyBurst, authVerifyRate)
}

// unspend returns the attempt allow charged when the verification it
// admitted was refused by the verification budget instead: nothing was
// checked, so nothing was attempted. Without it a credential re-sent
// too often would spend its attempt budget on refusals that never
// verified, and from the sixth on be refused as rate-limit — reported
// as guessing, which it is not, and which calls for a different
// response (issue #221).
func (l *authLimiter) unspend(source, user string) {
	src := sourceKey(source)
	l.mu.Lock()
	defer l.mu.Unlock()
	for key, burst := range map[string]float64{
		"s\x00" + src:                 authBurst,
		"a\x00" + src + "\x00" + user: authAccountBurst,
	} {
		if b, ok := l.buckets[key]; ok && b.tokens < burst {
			b.tokens = math.Min(burst, b.tokens+1)
		}
	}
}

// bucketLocked returns key's bucket, refilled for the time that has
// passed (capped at the burst) and created at the burst if new. Caller
// holds l.mu.
func (l *authLimiter) bucketLocked(key string, burst, rate float64, now time.Time) *authBucket {
	b, ok := l.buckets[key]
	if !ok {
		l.evictLocked()
		b = &authBucket{tokens: burst, rate: rate, last: now}
		l.buckets[key] = b
	}
	if d := now.Sub(b.last); d > 0 {
		b.tokens += d.Seconds() * b.rate
		if b.tokens > burst {
			b.tokens = burst
		}
		b.last = now
	}
	b.seen = now
	return b
}

// budget reports whether source has an attempt left against user,
// spending nothing; spend charges one failed attempt to both buckets.
// The pair is what the SQL door uses (issue #212): it asks before the
// SCRAM exchange and charges after a failure, rather than charging on
// the attempt as allow does, so that a pool opening many connections
// with the right password at once — none of which will fail — is not
// refused. What that order gives up is the bound on how many guesses
// can be in flight together: every one that asked before the first
// failure landed runs, up to the pre-authentication connection cap.
// What it does not give up is what they cost — each is charged when it
// lands, into debt, so the wave buys nothing over the same guesses one
// at a time (see authDebtMax).
//
// A bucket goes below zero when a wave lands (debtFloor).
func (l *authLimiter) budget(source, user string) bool {
	src := sourceKey(source)
	now := l.nowFn()
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.bucketLocked("s\x00"+src, authBurst, authRate, now).tokens >= 1 &&
		l.bucketLocked("a\x00"+src+"\x00"+user, authAccountBurst, authRate, now).tokens >= 1
}

func (l *authLimiter) spend(source, user string) {
	src := sourceKey(source)
	now := l.nowFn()
	l.mu.Lock()
	defer l.mu.Unlock()
	for key, burst := range map[string]float64{
		"s\x00" + src:                 authBurst,
		"a\x00" + src + "\x00" + user: authAccountBurst,
	} {
		b := l.bucketLocked(key, burst, authRate, now)
		if b.tokens--; b.tokens < -l.debtMax {
			b.tokens = -l.debtMax
		}
	}
}

// sqlAuthLimiter is the node's limiter as the SQL listener sees it
// (pgwire cannot import this package): the same buckets /api/login and
// Basic spend, so guessing gains nothing by changing port.
type sqlAuthLimiter struct{ l *authLimiter }

func (a sqlAuthLimiter) Budget(source, user string) bool { return a.l.budget(source, user) }
func (a sqlAuthLimiter) Failed(source, user string)      { a.l.spend(source, user) }
func (a sqlAuthLimiter) Succeeded(source, user string)   { a.l.succeeded(source, user) }

// evictLocked drops the least recently used entries when the map is
// full. Caller holds l.mu.
func (l *authLimiter) evictLocked() {
	if len(l.buckets) < authLimiterMax {
		return
	}
	var oldestKey string
	var oldest time.Time
	for k, b := range l.buckets {
		if oldest.IsZero() || b.seen.Before(oldest) {
			oldestKey, oldest = k, b.seen
		}
	}
	delete(l.buckets, oldestKey)
}

// acquireVerify takes a slot for one password verification, or reports
// false if too many are already running. It does not wait: a caller made
// to wait is a caller holding a goroutine, which is the resource being
// protected.
func (l *authLimiter) acquireVerify() bool {
	select {
	case l.verify <- struct{}{}:
		return true
	default:
		return false
	}
}

func (l *authLimiter) releaseVerify() { <-l.verify }

// succeeded restores the budget a successful attempt spent. A caller
// that proves it holds a credential is not the caller being bounded, so
// its attempts should not accumulate against it — and an attacker, who
// never succeeds, never gets a refill this way.
//
// It does not remove the trade that source keying makes: a legitimate
// user behind the same address as someone guessing shares that address's
// budget, and is slowed to the refill rate until the guessing stops.
// That is the cost of the key, and the in-flight cap is what holds when
// the key means nothing at all.
//
// What it does not refund is the verification budget (allowVerify): a
// success was a verification like any other and cost the same, and
// refunding it here is what let a credential-holder verify without
// bound (issue #221).
func (l *authLimiter) succeeded(source, user string) {
	src := sourceKey(source)
	now := l.nowFn()
	l.mu.Lock()
	defer l.mu.Unlock()
	for key, burst := range map[string]float64{
		"s\x00" + src:                 authBurst,
		"a\x00" + src + "\x00" + user: authAccountBurst,
	} {
		if b, ok := l.buckets[key]; ok {
			b.tokens = burst
			b.last, b.seen = now, now
		}
	}
}

// sourceKey is the host part of a RemoteAddr, so every port from one
// address shares a budget. Behind a proxy or NAT this is the proxy, and
// the in-flight cap is what holds in that case.
func sourceKey(remoteAddr string) string {
	if host, _, err := net.SplitHostPort(remoteAddr); err == nil {
		return host
	}
	return remoteAddr
}

// The three ways an attempt is refused before any verification runs.
// They are counted apart because they call for different actions: a
// rate-limit refusal names one source asking too often, a verify-rate
// refusal one source having passwords checked too often — a valid
// credential re-sent on every request, which is a client to change,
// not an attacker — and a verify-full refusal says this node is already
// doing as much password hashing at once as it permits — the first two
// are someone else's problem to stop, the third is this node's ceiling
// (issues #203, #221).
// These are the label values metrics.AuthThrottleCauses pre-creates at
// registration; a new cause has to be added there too, or its series
// appears only once it first fires.
const (
	throttleRateLimit  = "rate-limit"
	throttleVerifyRate = "verify-rate"
	throttleVerifyFull = "verify-full"
)

// authThrottled records a refusal. What it records is deliberately the
// same whatever was asked for: nothing here may separate a known
// username from an unknown one. The cause describes the node's own
// state, not the caller's identity, so it discloses nothing.
func (n *Node) authThrottled(source, user, path, cause string) {
	metrics.AuthThrottled.WithLabelValues(cause).Inc()
	log.Audit("auth-throttled", "remote", source, "path", path, "cause", cause)
}

// authTimeout is the handshake deadline in force (0 = none), the
// pgwire default when the configuration leaves it unset.
func (n *Node) authTimeout() time.Duration {
	switch {
	case n.cfg.AuthTimeout < 0:
		return 0
	case n.cfg.AuthTimeout == 0:
		return pgwire.DefaultAuthTimeout
	}
	return n.cfg.AuthTimeout
}
