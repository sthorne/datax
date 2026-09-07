package testcluster

import (
	"net"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/sthorne/datax/pkg/server"
)

// The rate-based health checks (issues #223, #224): auth-throttled and
// auth-failures report a rate over a window rather than a lifetime
// total, so that a problem raised during an incident clears once the
// pressure stops — the point of the design, and the half of it nothing
// tested while the window was a five-minute constant. With the window
// shortened through Config.HealthRateWindow, each check is watched
// from quiet, through just under its threshold (still quiet), over it
// (raised), and past the window after the pressure stops (cleared).
//
// The health document is cached for three seconds (healthCacheFor), so
// every reading below is taken after a pause longer than that.

// healthRateWindow is the window these tests run the checks over.
const healthRateWindow = 6 * time.Second

// startHealthRateCluster starts a secure cluster with the shortened
// window and waits until root can read it.
func startHealthRateCluster(t *testing.T) (base string, client *http.Client, certsDir string) {
	t.Helper()
	httpLis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	tc, certsDir := startSecureCluster(t, "topsecret", func(i int, cfg *server.Config) {
		if i == 0 {
			cfg.HTTPListener = httpLis
		}
		cfg.HealthRateWindow = healthRateWindow
	})
	base = "https://" + tc.Nodes[0].HTTPAddr()
	client = httpsClient(t, certsDir, "")
	deadline := time.Now().Add(30 * time.Second)
	for {
		if code, _, _ := authedGet(t, client, base+"/status", "root", "topsecret"); code == http.StatusOK {
			return base, client, certsDir
		}
		if time.Now().After(deadline) {
			t.Fatal("root basic auth never succeeded")
		}
		time.Sleep(200 * time.Millisecond)
	}
}

// healthProblem reads /api/health as root and returns the problem for
// check, or nil.
func healthProblem(t *testing.T, client *http.Client, base, check string) *server.Problem {
	t.Helper()
	code, body, _ := authedGet(t, client, base+"/api/health", "root", "topsecret")
	if code != http.StatusOK {
		t.Fatalf("/api/health: %d", code)
	}
	var h server.HealthStatus
	if err := jsonUnmarshal([]byte(body), &h); err != nil {
		t.Fatal(err)
	}
	for i := range h.Problems {
		if h.Problems[i].Check == check {
			return &h.Problems[i]
		}
	}
	return nil
}

// pastTheCache waits out the health document's cache so the next read
// runs the checks again.
func pastTheCache() { time.Sleep(3500 * time.Millisecond) }

// guess posts one wrong password for root through /api/login from c
// and reports whether it was refused before verification (429) rather
// than verified and failed (401).
func guess(t *testing.T, c *http.Client, base string) (refused bool) {
	t.Helper()
	req, err := http.NewRequest(http.MethodPost, base+"/api/login", strings.NewReader(`{"user":"root","password":"wrong"}`))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := c.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	switch resp.StatusCode {
	case http.StatusTooManyRequests:
		return true
	case http.StatusUnauthorized:
		return false
	}
	t.Fatalf("a wrong password was answered %d", resp.StatusCode)
	return false
}

func TestAuthThrottledProblemRaisesAndClears(t *testing.T) {
	base, client, certsDir := startHealthRateCluster(t)
	attacker := httpsClientFrom(t, certsDir, "", "127.0.0.2")

	// Quiet on a node nothing has throttled; this reading also seeds the
	// window, so what follows is measured from here.
	if p := healthProblem(t, client, base, "auth-throttled"); p != nil {
		t.Fatalf("auth-throttled on a node nothing has throttled: %s", p.Summary)
	}

	// Just under the threshold (one refusal a second over the window):
	// two refusals, read more than three seconds later. The guesses
	// that run before the account's burst is spent are failures, not
	// refusals, and are not this check's.
	refuse := func(n int) {
		t.Helper()
		for got := 0; got < n; {
			if guess(t, attacker, base) {
				got++
			}
		}
	}
	refuse(2)
	pastTheCache()
	if p := healthProblem(t, client, base, "auth-throttled"); p != nil {
		t.Fatalf("two refusals in over three seconds raised auth-throttled (threshold one a second): %s", p.Summary)
	}

	// Over it: forty refusals inside the same window.
	refuse(40)
	pastTheCache()
	p := healthProblem(t, client, base, "auth-throttled")
	if p == nil {
		t.Fatal("forty refusals in a few seconds did not raise auth-throttled")
	}
	if !strings.Contains(p.Summary, "rate-limited") {
		t.Errorf("auth-throttled summary does not name the cause: %q", p.Summary)
	}

	// The pressure stops. Once the window has rolled past the last
	// refusal the problem is gone: a rate, not a total.
	deadline := time.Now().Add(healthRateWindow + 15*time.Second)
	for {
		pastTheCache()
		if healthProblem(t, client, base, "auth-throttled") == nil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("auth-throttled is still reported long after the refusals stopped: the check reports a total, not a rate")
		}
	}
}

func TestAuthFailuresProblemRaisesAndClears(t *testing.T) {
	base, client, certsDir := startHealthRateCluster(t)
	// Eight addresses, each with its own burst of guesses that run and
	// fail before the limiter refuses the rest: forty failures at once.
	var sources []*http.Client
	for i := 2; i <= 9; i++ {
		sources = append(sources, httpsClientFrom(t, certsDir, "", "127.0.0."+itoa(i)))
	}
	fail := func(n int) {
		t.Helper()
		got := 0
		for _, c := range sources {
			for j := 0; j < 12 && got < n; j++ {
				if !guess(t, c, base) {
					got++
				}
			}
		}
		if got < n {
			t.Fatalf("only %d of %d guesses ran: the limiter refused the rest before verification", got, n)
		}
	}

	if p := healthProblem(t, client, base, "auth-failures"); p != nil {
		t.Fatalf("auth-failures on a node nobody has failed to sign in to: %s", p.Summary)
	}

	// Just under: two failures, read more than three seconds later.
	fail(2)
	pastTheCache()
	if p := healthProblem(t, client, base, "auth-failures"); p != nil {
		t.Fatalf("two failures in over three seconds raised auth-failures (threshold one a second): %s", p.Summary)
	}

	// Over: thirty more inside the window (the eight bursts hold
	// thirty-eight more; the refill adds a few).
	fail(30)
	pastTheCache()
	if p := healthProblem(t, client, base, "auth-failures"); p == nil {
		t.Fatal("thirty failed sign-ins in a few seconds did not raise auth-failures")
	}

	deadline := time.Now().Add(healthRateWindow + 15*time.Second)
	for {
		pastTheCache()
		if healthProblem(t, client, base, "auth-failures") == nil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("auth-failures is still reported long after the failures stopped: the check reports a total, not a rate")
		}
	}
}
