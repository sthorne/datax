package server

import (
	"testing"
	"time"
)

// counterIncrease (issue #208) sums a cumulative counter's rises over a
// window and treats a fall as a restart: the first sample is history,
// not increase; a reset counts the new value from zero; a flat stretch
// adds nothing and does not move the last rise.
func TestCounterIncreaseReadsResetsAsRestarts(t *testing.T) {
	ms := func(s int64) int64 { return s * int64(time.Second) }
	for _, c := range []struct {
		name     string
		pts      []tsample
		inc      float64
		lastRise int64
	}{
		{"nothing", nil, 0, 0},
		{"one sample is history, not increase", []tsample{{ms(10), 500}}, 0, 0},
		{"a rise", []tsample{{ms(10), 500}, {ms(20), 503}, {ms(30), 510}}, 10, 30000},
		{"a flat stretch", []tsample{{ms(10), 500}, {ms(20), 503}, {ms(30), 503}, {ms(40), 503}}, 3, 20000},
		{"a restart counts the new value from zero", []tsample{{ms(10), 500}, {ms(20), 503}, {ms(30), 2}, {ms(40), 6}}, 3 + 2 + 4, 40000},
		{"a restart to zero is not a rise", []tsample{{ms(10), 500}, {ms(20), 0}, {ms(30), 0}}, 0, 0},
	} {
		inc, last := counterIncrease(c.pts)
		if inc != c.inc || last != c.lastRise {
			t.Errorf("%s: increase %v (want %v), last rise %d (want %d)", c.name, inc, c.inc, last, c.lastRise)
		}
	}
}
