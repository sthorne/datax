package types

import (
	"testing"
	"time"
)

// TestTimestampMicroseconds (PG-F-019): timestamps keep PostgreSQL's
// microsecond precision. Input rounds to it (half away from zero),
// text output never shows more than six fractional digits, and a
// value's text reads back as the same value.
func TestTimestampMicroseconds(t *testing.T) {
	n, err := ParseTimestamp("2024-01-01 00:00:00.1234565+00")
	if err != nil || n%1000 != 0 || FormatTimestamp(n, false) != "2024-01-01 00:00:00.123457+00" {
		t.Fatalf("rounded input: %d %q %v", n, FormatTimestamp(n, false), err)
	}
	if n, _ = ParseTimestamp("2024-01-01 23:59:59.9999996"); FormatTimestamp(n, false) != "2024-01-02 00:00:00+00" {
		t.Fatalf("carry into the next day: %q", FormatTimestamp(n, false))
	}
	if n, _ = ParseTimestampNoTZ("1969-12-31 23:59:59.0000004"); FormatTimestamp(n, true) != "1969-12-31 23:59:59" {
		t.Fatalf("before the epoch: %q", FormatTimestamp(n, true))
	}
	ny, _ := time.LoadLocation("America/New_York")
	if n, _ = ParseTimestampIn("2024-07-01 08:00:00.0000015", ny); FormatTimestampIn(n, ny) != "2024-07-01 08:00:00.000002-04" {
		t.Fatalf("zoned input: %q", FormatTimestampIn(n, ny))
	}
	// A stored value from before rounding (nanoseconds) renders rounded,
	// and its text re-reads as the rounded value.
	raw := time.Date(2026, 11, 17, 13, 18, 4, 787705724, time.UTC).UnixNano()
	text := FormatTimestamp(raw, false)
	if text != "2026-11-17 13:18:04.787706+00" {
		t.Fatalf("nanosecond value renders %q", text)
	}
	back, err := ParseTimestamp(text)
	if err != nil || back != RoundMicros(raw) {
		t.Fatalf("re-read %q = %d, %v; want %d", text, back, err, RoundMicros(raw))
	}
	if TruncMicros(1999) != 1000 || TruncMicros(-1) != -1000 || TruncMicros(-1000) != -1000 {
		t.Fatal("TruncMicros")
	}
}
