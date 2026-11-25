package types

import (
	"errors"
	"testing"
	"time"
)

// TestDateToTimestampRange: a date becomes a timestamp only when its
// midnight is in the int64-nanosecond range (1678-01-01 to 2261-12-31);
// outside it the conversion is ErrTimestampRange, never a value that
// wrapped (1500-01-01 used to become 2084-07-20 23:34:33.709552).
func TestDateToTimestampRange(t *testing.T) {
	day := func(s string) int64 {
		t.Helper()
		d, err := ParseDate(s)
		if err != nil {
			t.Fatal(err)
		}
		return d
	}
	for _, s := range []string{"1678-01-01", "1970-01-01", "2024-02-29", "2261-12-31"} {
		v, err := NewDate(day(s)).Coerce(Timestamp)
		if err != nil || FormatTimestamp(v.I, true) != s+" 00:00:00" {
			t.Errorf("%s: %v %v", s, v, err)
		}
	}
	for _, s := range []string{"1500-01-01", "1677-12-31", "2262-01-01", "3000-01-01"} {
		if v, err := NewDate(day(s)).Coerce(Timestamp); !errors.Is(err, ErrTimestampRange) {
			t.Errorf("%s: %v %v, want ErrTimestampRange", s, v.Text(), err)
		}
		if _, err := DateNanos(day(s)); !errors.Is(err, ErrTimestampRange) {
			t.Errorf("DateNanos(%s): %v, want ErrTimestampRange", s, err)
		}
	}
	for _, tm := range []time.Time{time.Date(1500, 1, 1, 0, 0, 0, 0, time.UTC), time.Date(2400, 1, 1, 0, 0, 0, 0, time.UTC)} {
		if _, err := TimeNanos(tm); !errors.Is(err, ErrTimestampRange) {
			t.Errorf("TimeNanos(%v): %v, want ErrTimestampRange", tm, err)
		}
	}
	if n, err := TimeNanos(time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)); err != nil || n != 1704067200*int64(time.Second) {
		t.Errorf("TimeNanos(2024-01-01): %d %v", n, err)
	}
}
