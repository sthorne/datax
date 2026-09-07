package testcluster

import (
	"context"
	"io"
	"net"
	"net/http"
	"regexp"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/sthorne/datax/pkg/server"
)

// The verification budget (issue #221), end to end: a caller holding a
// valid credential and re-sending it on every request — HTTP Basic's
// shape — is bounded like anyone else, where before it was exempt from
// the bound altogether because each success refunded its attempt
// budget. The SQL door is untouched by it: SCRAM's server side runs no
// derivation, so there is no cost there to bound, and a pool signing in
// as often as it likes is not refused.
func TestSuccessfulSignInsSpendTheVerificationBudget(t *testing.T) {
	httpLis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	tc, certsDir := startSecureCluster(t, "topsecret", func(i int, cfg *server.Config) {
		if i == 0 {
			cfg.HTTPListener = httpLis
		}
	})
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()
	base := "https://" + tc.Nodes[0].HTTPAddr()
	operator := httpsClient(t, certsDir, "")
	root := waitForRoot(t, ctx, tc, certsDir)
	defer root.Close(ctx)

	// Seed the health window before any flood, so what follows is the
	// only throttling inside it: the check reports a rate over the
	// window, from this sample on (the checks are cached for three
	// seconds).
	if code, _, _ := authedGet(t, operator, base+"/api/health", "root", "topsecret"); code != http.StatusOK {
		t.Fatalf("/api/health seed: %d", code)
	}

	// A flood of correct Basic credentials from one address. Every one
	// is a derivation; before #221 all of them ran.
	const flood = 300
	hammer := httpsClientFrom(t, certsDir, "", "127.0.0.5")
	start := time.Now()
	var ok, refused int
	for i := 0; i < flood; i++ {
		switch code, _, _ := authedGet(t, hammer, base+"/status", "root", "topsecret"); code {
		case http.StatusOK:
			ok++
		case http.StatusTooManyRequests:
			refused++
		default:
			t.Fatalf("request %d: %d, want 200 or 429", i, code)
		}
	}
	elapsed := time.Since(start)
	if refused == 0 {
		t.Fatalf("%d correct sign-ins from one address in %s, none refused: a credential-holder is exempt from the bound", flood, elapsed)
	}
	// The bound: a burst of 100 plus twenty a second (server.authVerifyBurst,
	// server.authVerifyRate), measured against the time the flood took.
	if limit := 100 + int(20*elapsed.Seconds()) + 20; ok > limit {
		t.Fatalf("%d of %d correct sign-ins verified in %s (limit %d): the verification budget is not holding", ok, flood, elapsed, limit)
	}

	// Another address is unaffected, and so is the operator's.
	if code, _, _ := authedGet(t, httpsClientFrom(t, certsDir, "", "127.0.0.6"), base+"/status", "root", "topsecret"); code != http.StatusOK {
		t.Fatalf("a sign-in from another address while one is over its verification budget: %d", code)
	}

	// The same through /api/login, from a third address: the other door
	// that verifies, and the one the issue measured — a script signing
	// in afresh on every call. Each door is its own call site, so each
	// is proved on its own.
	const logins = 200
	signer := httpsClientFrom(t, certsDir, "", "127.0.0.7")
	start = time.Now()
	var loginOK, loginRefused int
	for i := 0; i < logins; i++ {
		req, err := http.NewRequest(http.MethodPost, base+"/api/login", strings.NewReader(`{"user":"root","password":"topsecret"}`))
		if err != nil {
			t.Fatal(err)
		}
		req.Header.Set("Content-Type", "application/json")
		resp, err := signer.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		io.Copy(io.Discard, resp.Body)
		resp.Body.Close()
		switch resp.StatusCode {
		case http.StatusOK:
			loginOK++
		case http.StatusTooManyRequests:
			loginRefused++
		default:
			t.Fatalf("login %d: %d, want 200 or 429", i, resp.StatusCode)
		}
	}
	elapsed = time.Since(start)
	if loginRefused == 0 {
		t.Fatalf("%d correct sign-ins through /api/login from one address in %s, none refused: that door does not spend the verification budget", logins, elapsed)
	}
	if limit := 100 + int(20*elapsed.Seconds()) + 20; loginOK > limit {
		t.Fatalf("%d of %d correct /api/login sign-ins verified in %s (limit %d): the verification budget is not holding at that door", loginOK, logins, elapsed, limit)
	}
	refused += loginRefused

	// Counted as its own cause, and the total is still the sum.
	code, body, _ := authedGet(t, operator, base+"/api/security", "root", "topsecret")
	if code != http.StatusOK {
		t.Fatalf("/api/security: %d", code)
	}
	var sec server.SecurityStatus
	if err := jsonUnmarshal([]byte(body), &sec); err != nil {
		t.Fatal(err)
	}
	if sec.ThrottledVerifyRate < float64(refused) {
		t.Fatalf("/api/security reports %v verify-rate refusals, %d were observed", sec.ThrottledVerifyRate, refused)
	}
	if sec.AuthThrottled != sec.ThrottledRateLimit+sec.ThrottledVerifyRate+sec.ThrottledVerifyFull {
		t.Fatalf("total %v is not the sum of its causes (%v + %v + %v)",
			sec.AuthThrottled, sec.ThrottledRateLimit, sec.ThrottledVerifyRate, sec.ThrottledVerifyFull)
	}

	// And the health check counts them into its rate. Nothing in the
	// window was rate-limited — a refusal by the verification budget
	// returns the attempt charge, and every attempt here was correct —
	// and nothing reached the concurrent-verification cap from one
	// request at a time, so this problem is raised by verify-rate
	// refusals alone, or not at all; and its parenthetical names them.
	summary := regexp.MustCompile(`^(\d+) authentication attempts refused before verification in the last (\S+) on this node \((\d+) rate-limited, (\d+) over the verification budget, (\d+) over the concurrent-verification cap\)`)
	deadline := time.Now().Add(30 * time.Second)
	for {
		code, body, _ := authedGet(t, operator, base+"/api/health", "root", "topsecret")
		if code != http.StatusOK {
			t.Fatalf("/api/health: %d", code)
		}
		var h server.HealthStatus
		if err := jsonUnmarshal([]byte(body), &h); err != nil {
			t.Fatal(err)
		}
		var problem *server.Problem
		for i := range h.Problems {
			if h.Problems[i].Check == "auth-throttled" {
				problem = &h.Problems[i]
			}
		}
		if problem != nil {
			m := summary.FindStringSubmatch(problem.Summary)
			if m == nil {
				t.Fatalf("auth-throttled does not name its causes: %q", problem.Summary)
			}
			total, _ := strconv.Atoi(m[1])
			window, err := time.ParseDuration(m[2])
			if err != nil {
				t.Fatalf("auth-throttled's window %q: %v", m[2], err)
			}
			rl, _ := strconv.Atoi(m[3])
			vr, _ := strconv.Atoi(m[4])
			vf, _ := strconv.Atoi(m[5])
			if vr == 0 {
				t.Fatalf("auth-throttled was raised with no refusal attributed to the verification budget: %q", problem.Summary)
			}
			if total != rl+vr+vf {
				t.Errorf("auth-throttled's parenthetical does not sum to its total: %q", problem.Summary)
			}
			// The threshold is one refusal a second over the window; if
			// the other causes alone cross it, this problem would be
			// raised without verify-rate and proves nothing about it.
			if window > 0 && float64(rl+vf)/window.Seconds() > 1 {
				t.Fatalf("the window carries %d refusals of other causes in %s, enough to raise the problem alone: this test is not proving what it claims (%q)", rl+vf, window, problem.Summary)
			}
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("%d refusals by the verification budget never raised an auth-throttled problem: the health check is not counting that cause", refused)
		}
		time.Sleep(500 * time.Millisecond)
	}

	// The SQL door spends none of it: the same address, past its HTTP
	// verification budget, signs in over SQL as often as it likes — a
	// pool reconnecting is not a cost the node needs to bound there.
	for i := 0; i < 30; i++ {
		c, err := connectSecureFrom(ctx, secureURL(tc, certsDir, "root", "topsecret"), "127.0.0.5")
		if err != nil {
			t.Fatalf("SQL sign-in %d from an address over its HTTP verification budget: %v", i, err)
		}
		_ = c.Close(ctx)
	}
}

// TestGuessingStaysBoundedAtBothDoors (issue #221, from QA's review of
// its fix): unspend returns the attempt charge when the verification
// budget refuses — and only then. Called on every request it would
// refund every guess, and the per-account guessing bound of #195 (five,
// then one a second) would silently become the verification burst (a
// hundred), with every other test still green: they check that refusal
// arrives, not the count at which it arrives. This pins the count, at
// each door, since the two call sites are independent.
func TestGuessingStaysBoundedAtBothDoors(t *testing.T) {
	httpLis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	tc, certsDir := startSecureCluster(t, "topsecret", func(i int, cfg *server.Config) {
		if i == 0 {
			cfg.HTTPListener = httpLis
		}
	})
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	base := "https://" + tc.Nodes[0].HTTPAddr()
	root := waitForRoot(t, ctx, tc, certsDir)
	defer root.Close(ctx)

	// The bound: authAccountBurst guesses at one account from one
	// address, plus one a second for the time the loop takes — a loop
	// of thirty guesses takes well under a second, so allow one.
	const bound = 5 + 1

	// Basic, from a fresh address: wrong passwords for root until the
	// first refusal.
	guesser := httpsClientFrom(t, certsDir, "", "127.0.0.8")
	ran := 0
	for i := 0; i < 30; i++ {
		code, _, _ := authedGet(t, guesser, base+"/status", "root", "wrong")
		if code == http.StatusTooManyRequests {
			break
		}
		if code != http.StatusUnauthorized {
			t.Fatalf("Basic guess %d: %d, want 401 or 429", i, code)
		}
		ran++
	}
	if ran > bound {
		t.Fatalf("%d wrong Basic guesses at one account ran before a refusal (bound %d): the guessing bound is being refunded", ran, bound)
	}
	if ran == 0 {
		t.Fatal("the first Basic guess was refused: the bound is not what this test measures")
	}

	// /api/login, from another fresh address, the same.
	signer := httpsClientFrom(t, certsDir, "", "127.0.0.9")
	ran = 0
	for i := 0; i < 30; i++ {
		req, err := http.NewRequest(http.MethodPost, base+"/api/login", strings.NewReader(`{"user":"root","password":"wrong"}`))
		if err != nil {
			t.Fatal(err)
		}
		req.Header.Set("Content-Type", "application/json")
		resp, err := signer.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		io.Copy(io.Discard, resp.Body)
		resp.Body.Close()
		if resp.StatusCode == http.StatusTooManyRequests {
			break
		}
		if resp.StatusCode != http.StatusUnauthorized {
			t.Fatalf("/api/login guess %d: %d, want 401 or 429", i, resp.StatusCode)
		}
		ran++
	}
	if ran > bound {
		t.Fatalf("%d wrong /api/login guesses at one account ran before a refusal (bound %d): the guessing bound is being refunded", ran, bound)
	}
	if ran == 0 {
		t.Fatal("the first /api/login guess was refused: the bound is not what this test measures")
	}
}
