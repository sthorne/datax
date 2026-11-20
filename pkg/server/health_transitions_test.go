package server

import (
	"strings"
	"testing"
	"time"
)

// A problem's history (issue #207): a run of the checks records a
// problem when it appears and again when it clears, and never in
// between; Since is set when it appears and kept, whatever the summary
// says on later runs; a check that flaps records a pair per flap.
func TestProblemTrackerRecordsTransitionsOnly(t *testing.T) {
	var tr problemTracker
	down := func(age string) Problem {
		return Problem{Severity: SeverityCritical, Check: "node-down", Node: 3, Summary: "n3 has not heartbeated for " + age}
	}
	under := Problem{Severity: SeverityWarning, Check: "under-replicated", Range: 12, Summary: "r12 has 2 of 3 replicas"}

	// Run 1: two problems appear.
	run1 := []Problem{down("31s"), under}
	appeared, cleared := tr.settle(run1, 1_000)
	if len(appeared) != 2 || len(cleared) != 0 {
		t.Fatalf("run 1: appeared %d, cleared %d; want 2 and 0", len(appeared), len(cleared))
	}
	for _, p := range run1 {
		if p.Since != 1_000 {
			t.Fatalf("run 1: %s since %d, want 1000", p.Check, p.Since)
		}
	}

	// Runs 2 and 3: the same problems, with summaries that moved on. No
	// transition, and Since is the one they were given.
	for i, now := range []int64{4_000, 7_000} {
		run := []Problem{down("37s"), under}
		appeared, cleared = tr.settle(run, now)
		if len(appeared) != 0 || len(cleared) != 0 {
			t.Fatalf("run %d: appeared %d, cleared %d; a problem still open is not a transition", i+2, len(appeared), len(cleared))
		}
		for _, p := range run {
			if p.Since != 1_000 {
				t.Fatalf("run %d: %s since %d, want 1000 (re-dated because its summary changed?)", i+2, p.Check, p.Since)
			}
		}
	}

	// Run 4: the range recovers and n3 is still down.
	appeared, cleared = tr.settle([]Problem{down("40s")}, 10_000)
	if len(appeared) != 0 || len(cleared) != 1 || cleared[0].Check != "under-replicated" || cleared[0].Range != 12 {
		t.Fatalf("run 4: appeared %v, cleared %v; want under-replicated r12 cleared", appeared, cleared)
	}
	if cleared[0].Since != 1_000 {
		t.Fatalf("run 4: the cleared problem carries since %d, want 1000 so the record can say how long it was open", cleared[0].Since)
	}
	if got := clearedSummary(cleared[0], 10_000); got != "under-replicated on r12 cleared after 9s" {
		t.Fatalf("cleared summary %q", got)
	}
	if got := appearedSummary(down("31s")); got != "node-down on n3 (critical): n3 has not heartbeated for 31s" {
		t.Fatalf("appeared summary %q", got)
	}

	// Runs 5 to 8: the range flaps twice. Each flap is one pair of
	// records, and a re-appearance is dated afresh.
	pairs := 0
	for i, now := range []int64{13_000, 16_000, 19_000, 22_000} {
		run := []Problem{down("52s")}
		if i%2 == 0 {
			run = append(run, under)
		}
		appeared, cleared = tr.settle(run, now)
		if i%2 == 0 {
			if len(appeared) != 1 || len(cleared) != 0 {
				t.Fatalf("flap %d: appeared %d, cleared %d", i, len(appeared), len(cleared))
			}
			if run[1].Since != now {
				t.Fatalf("flap %d: re-appeared with since %d, want %d: a problem that cleared and came back began again", i, run[1].Since, now)
			}
		} else {
			if len(appeared) != 0 || len(cleared) != 1 {
				t.Fatalf("flap %d: appeared %d, cleared %d", i, len(appeared), len(cleared))
			}
			pairs++
		}
	}
	if pairs != 2 {
		t.Fatalf("%d pairs for two flaps", pairs)
	}

	// Run 9: everything clears, in a stable order, and the tracker is
	// empty, so a later run starts every problem afresh.
	appeared, cleared = tr.settle(nil, 25_000)
	if len(appeared) != 0 || len(cleared) != 1 || cleared[0].Check != "node-down" {
		t.Fatalf("run 9: appeared %v, cleared %v", appeared, cleared)
	}
	if len(tr.open) != 0 {
		t.Fatalf("tracker still holds %d after everything cleared", len(tr.open))
	}
	appeared, _ = tr.settle([]Problem{down("2m")}, 30_000)
	if len(appeared) != 1 || appeared[0].Since != 30_000 {
		t.Fatalf("after a full clear: appeared %v", appeared)
	}
}

// Two rows of one check tell apart by node or range, and a problem
// without either is one problem: identity is the check, the node and
// the range, never the summary.
func TestProblemIdentityIsCheckNodeAndRange(t *testing.T) {
	var tr problemTracker
	a := Problem{Check: "node-unresponsive", Node: 2, Summary: "n2 late"}
	b := Problem{Check: "node-unresponsive", Node: 3, Summary: "n3 late"}
	mixed := Problem{Check: "mixed-binaries", Summary: "v3..v4"}
	appeared, _ := tr.settle([]Problem{a, b, mixed}, 1)
	if len(appeared) != 3 {
		t.Fatalf("appeared %d, want 3: n2 and n3 are two problems", len(appeared))
	}
	mixed.Summary = "v3..v5"
	appeared, cleared := tr.settle([]Problem{b, mixed}, 2)
	if len(appeared) != 0 || len(cleared) != 1 || cleared[0].Node != 2 {
		t.Fatalf("appeared %v, cleared %v; want only n2 cleared", appeared, cleared)
	}
	// The cleared order is by key, so a run that clears several is
	// deterministic.
	appeared, cleared = tr.settle(nil, 3)
	if len(appeared) != 0 || len(cleared) != 2 || cleared[0].Check != "mixed-binaries" || cleared[1].Check != "node-unresponsive" {
		t.Fatalf("cleared %v, want mixed-binaries then node-unresponsive", cleared)
	}
	if s := clearedSummary(cleared[0], 3); !strings.HasPrefix(s, "mixed-binaries cleared after ") {
		t.Fatalf("a problem without a node or range names only its check: %q", s)
	}
}

// TestRateOverKeepsABaselinePastTheWindow: health is sampled only when
// read, so a rate must count what happened since the last read even
// when that read is older than the window. Dropping it left the sample
// just taken as the baseline, and a burst between two reads a window
// apart read as zero (TestAuthFailuresProblemRaisesAndClears failed
// that way when thirty guesses took longer than the test's window).
func TestRateOverKeepsABaselinePastTheWindow(t *testing.T) {
	t0 := time.Unix(1000, 0)
	const window = 6 * time.Second
	ss, d := rateOver(nil, 0, t0, window)
	if d != 0 || len(ss) != 1 {
		t.Fatalf("first sample: increase %v, %d samples", d, len(ss))
	}
	// Thirty more, read ten seconds later: past the window.
	ss, d = rateOver(ss, 30, t0.Add(10*time.Second), window)
	if d != 30 || !ss[0].at.Equal(t0) {
		t.Fatalf("a read past the window: increase %v from %v, want 30 from %v", d, ss[0].at, t0)
	}
	// Read again a second later with no change: the baseline is still
	// the newest sample a window old, so the increase holds, over 11s.
	ss, d = rateOver(ss, 30, t0.Add(11*time.Second), window)
	if d != 30 || !ss[0].at.Equal(t0) {
		t.Fatalf("a second read: increase %v from %v", d, ss[0].at)
	}
	// Once a read inside the burst's aftermath is a window old, the
	// increase is zero and a problem built on it clears.
	ss, d = rateOver(ss, 30, t0.Add(17*time.Second), window)
	if d != 0 {
		t.Fatalf("a window after the burst: increase %v from %v, want 0", d, ss[0].at)
	}
	if len(ss) > 3 {
		t.Fatalf("%d samples kept, want the baseline and the newer ones only", len(ss))
	}
	// Frequent reads keep the baseline about a window back.
	ss, _ = nil, 0
	for s := 0; s <= 20; s++ {
		ss, d = rateOver(ss, float64(s), t0.Add(time.Duration(s)*time.Second), window)
	}
	if d != 6 || !ss[0].at.Equal(t0.Add(14*time.Second)) {
		t.Fatalf("frequent reads: increase %v from %v, want 6 from +14s", d, ss[0].at)
	}
}
