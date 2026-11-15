package types

import (
	"testing"
	"time"
)

// TestIntervalUnitAbbreviations pins the abbreviations that end in "s",
// which a plural-stripping match once read as other units ("1 ms" was a
// minute).
func TestIntervalUnitAbbreviations(t *testing.T) {
	for in, want := range map[string]time.Duration{
		"1 ms":        time.Millisecond,
		"2 msec":      2 * time.Millisecond,
		"2 msecs":     2 * time.Millisecond,
		"3 us":        3 * time.Microsecond,
		"3 usecs":     3 * time.Microsecond,
		"4 s":         4 * time.Second,
		"4 secs":      4 * time.Second,
		"5 m":         5 * time.Minute,
		"5 mins":      5 * time.Minute,
		"6 hrs":       6 * time.Hour,
		"7 MS":        7 * time.Millisecond,
		"1 hour 1 ms": time.Hour + time.Millisecond,
	} {
		iv, err := ParseInterval(in)
		if err != nil {
			t.Errorf("%q: %v", in, err)
			continue
		}
		if iv.Months != 0 || iv.Days != 0 || time.Duration(iv.Nanos) != want {
			t.Errorf("%q = %+v, want %v", in, iv, want)
		}
	}
}
