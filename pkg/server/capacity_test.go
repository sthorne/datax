package server

import (
	"math"
	"testing"
	"time"
)

// samples builds a series that starts at `start` bytes free and changes
// by `perHour` bytes an hour, one reading every `every`, with `jitter`
// added in a repeating pattern so a "noisy" series can be built without
// a random source.
func samples(start, perHour float64, every time.Duration, n int, jitter float64) []tsample {
	t0 := time.Now().Add(-time.Duration(n) * every).UnixNano()
	out := make([]tsample, 0, n)
	for i := 0; i < n; i++ {
		at := t0 + int64(i)*int64(every)
		hours := float64(i) * every.Hours()
		wobble := jitter * float64((i%5)-2)
		out = append(out, tsample{t: at, v: start + perHour*hours + wobble})
	}
	return out
}

// A store steadily losing free space gets a projection, and the
// projection is the arithmetic an operator would do by hand.
func TestForecastFilling(t *testing.T) {
	// Losing 1 GiB an hour, from 100 GiB free a day ago — so about
	// 76 GiB free now, and a bit over three days left. The projection
	// runs from the CURRENT free space, not the window's start: an
	// operator asks how long they have from here.
	const gib = 1 << 30
	pts := samples(100*gib, -gib, 10*time.Minute, 144, 0)
	f := fitForecast(1, pts, time.Now())
	if !f.Filling {
		t.Fatalf("a store losing a GiB an hour is filling: %+v", f)
	}
	wantDays := pts[len(pts)-1].v / (24 * gib)
	if math.Abs(f.DaysToFull-wantDays) > 0.1 {
		t.Errorf("days to full %.2f, want about %.2f (free now over the daily rate): %+v", f.DaysToFull, wantDays, f)
	}
	if math.Abs(f.FreeBytes-pts[len(pts)-1].v) > 1 {
		t.Errorf("free bytes %.0f is not the newest reading %.0f", f.FreeBytes, pts[len(pts)-1].v)
	}
	if math.Abs(f.GrowthBytesPerDay-24*gib)/(24*gib) > 0.05 {
		t.Errorf("growth %.0f/day, want about %d: %+v", f.GrowthBytesPerDay, 24*gib, f)
	}
	if f.Fit < 0.99 {
		t.Errorf("a straight line should fit exactly, got %.3f", f.Fit)
	}
	if f.Reason != "" {
		t.Errorf("a forecast that was made needs no excuse: %q", f.Reason)
	}
}

// A flat or rising series is not a forecast of anything. This is the
// rule the issue asks for by name: never a number where there is no
// trend towards full.
func TestForecastNotFilling(t *testing.T) {
	const gib = 1 << 30
	for _, tc := range []struct {
		name    string
		perHour float64
	}{
		{"flat", 0},
		{"freeing space", +gib},
	} {
		f := fitForecast(1, samples(50*gib, tc.perHour, 10*time.Minute, 144, 0), time.Now())
		if f.Filling {
			t.Errorf("%s: claimed to be filling: %+v", tc.name, f)
		}
		if f.DaysToFull != 0 {
			t.Errorf("%s: claimed %v days to full", tc.name, f.DaysToFull)
		}
		if f.Reason == "" {
			t.Errorf("%s: no forecast and no reason given", tc.name)
		}
	}
}

// Too few readings, or too short a window, is "not known" — not a line
// through three points.
func TestForecastNotEnoughEvidence(t *testing.T) {
	const gib = 1 << 30
	few := fitForecast(1, samples(10*gib, -gib, time.Hour, 4, 0), time.Now())
	if few.Filling || few.Reason == "" {
		t.Errorf("four readings is not a trend: %+v", few)
	}
	// Enough readings, but they span minutes rather than hours.
	brief := fitForecast(1, samples(10*gib, -gib, 10*time.Second, 30, 0), time.Now())
	if brief.Filling || brief.Reason == "" {
		t.Errorf("five minutes of readings is not a trend: %+v", brief)
	}
}

// A series that wanders with the workload rather than trending gets no
// number: a bad fit is worse than no answer.
func TestForecastNoisySeriesRefused(t *testing.T) {
	const gib = 1 << 30
	// A tiny downward drift buried under large swings.
	f := fitForecast(1, samples(100*gib, -gib/100, 10*time.Minute, 144, 20*gib), time.Now())
	if f.Filling {
		t.Errorf("a series dominated by noise should not be forecast: fit %.3f, %+v", f.Fit, f)
	}
	if f.Fit >= capacityMinFit {
		t.Errorf("this series was meant to fit badly, got %.3f", f.Fit)
	}
	if f.GrowthBytesPerDay != 0 {
		t.Errorf("a refused fit must not report a growth rate: %+v", f)
	}
}

// An empty series says so rather than dividing by zero.
func TestForecastEmpty(t *testing.T) {
	f := fitForecast(3, nil, time.Now())
	if f.Filling || f.NodeID != 3 || f.Reason == "" {
		t.Fatalf("%+v", f)
	}
}

// The health check fires at the thresholds and stays quiet outside them.
func TestCapacityProblems(t *testing.T) {
	const gib = 1 << 30
	mk := func(days float64) Forecast {
		return Forecast{NodeID: 1, Filling: true, DaysToFull: days, GrowthBytesPerDay: gib, FreeBytes: float64(days) * gib}
	}
	if p := capacityProblems([]Forecast{mk(2)}); len(p) != 1 || p[0].Severity != SeverityCritical {
		t.Errorf("two days out should be critical: %+v", p)
	}
	if p := capacityProblems([]Forecast{mk(10)}); len(p) != 1 || p[0].Severity != SeverityWarning {
		t.Errorf("ten days out should warn: %+v", p)
	}
	if p := capacityProblems([]Forecast{mk(90)}); len(p) != 0 {
		t.Errorf("three months out is not a problem: %+v", p)
	}
	// A store that is not filling never produces a problem, whatever its
	// (unset) days-to-full says.
	if p := capacityProblems([]Forecast{{NodeID: 1, Filling: false}}); len(p) != 0 {
		t.Errorf("a store that is not filling is not a problem: %+v", p)
	}
}
