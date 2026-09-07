package server

import (
	"net"
	"sync"
	"time"

	"github.com/sthorne/datax/pkg/metrics"
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
)

// authVerifyInFlight caps concurrent password verifications. Four is
// enough that ordinary sign-ins never queue and small enough that
// verification cannot take every core.
const authVerifyInFlight = 4

// authLimiter is a token bucket per key with a bounded, LRU-evicted map.
type authLimiter struct {
	mu      sync.Mutex
	buckets map[string]*authBucket
	verify  chan struct{} // the in-flight cap, as a semaphore
	nowFn   func() time.Time
}

type authBucket struct {
	tokens float64
	last   time.Time
	seen   time.Time // for eviction
}

func newAuthLimiter() *authLimiter {
	return &authLimiter{
		buckets: map[string]*authBucket{},
		verify:  make(chan struct{}, authVerifyInFlight),
		nowFn:   time.Now,
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
	now := l.nowFn()
	l.mu.Lock()
	defer l.mu.Unlock()
	b, ok := l.buckets[key]
	if !ok {
		l.evictLocked()
		b = &authBucket{tokens: burst, last: now}
		l.buckets[key] = b
	}
	// Refill for the time that has passed, capped at the burst.
	if d := now.Sub(b.last); d > 0 {
		b.tokens += d.Seconds() * (1 / authRefill.Seconds())
		if b.tokens > burst {
			b.tokens = burst
		}
		b.last = now
	}
	b.seen = now
	if b.tokens < 1 {
		return false
	}
	b.tokens--
	return true
}

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

// The two ways an attempt is refused before any verification runs. They
// are counted apart because they call for different actions: a
// rate-limit refusal names one source asking too often, and a
// verify-full refusal says this node is already doing as much password
// hashing at once as it permits — the first is someone else's problem
// to stop, the second is this node's ceiling (issue #203).
// These are the label values metrics.AuthThrottleCauses pre-creates at
// registration; a new cause has to be added there too, or its series
// appears only once it first fires.
const (
	throttleRateLimit  = "rate-limit"
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
