package types

import (
	"testing"
	"time"
)

// TestIntervalFormat (issue #345): each IntervalStyle renders as
// PostgreSQL's regression tests (interval.out) show.
func TestIntervalFormat(t *testing.T) {
	ym := Interval{Months: 14}                                                                                // 1-2
	dt := Interval{Days: 3, Nanos: int64(4*time.Hour + 5*time.Minute + 6*time.Second + 700*time.Millisecond)} // 3 4:05:06.7
	for _, c := range []struct {
		iv    Interval
		style string
		want  string
	}{
		{Interval{}, "iso_8601", "PT0S"},
		{ym, "iso_8601", "P1Y2M"},
		{Interval{Days: 1, Nanos: int64(2*time.Hour + 3*time.Minute + 4*time.Second)}, "iso_8601", "P1DT2H3M4S"},
		{Interval{Nanos: int64(2*time.Hour + 3*time.Minute + 4*time.Second + 456790*time.Microsecond)}, "iso_8601", "PT2H3M4.45679S"},
		{ym.Add(dt), "iso_8601", "P1Y2M3DT4H5M6.7S"},
		{ym.Add(dt.Neg()), "iso_8601", "P1Y2M-3DT-4H-5M-6.7S"},
		{ym.Neg().Add(dt), "iso_8601", "P-1Y-2M3DT4H5M6.7S"},
		{Interval{}, "sql_standard", "0"},
		{ym, "sql_standard", "1-2"},
		{Interval{Days: 1, Nanos: int64(2*time.Hour + 3*time.Minute + 4*time.Second)}, "sql_standard", "1 2:03:04"},
		{ym.Neg(), "sql_standard", "-1-2"},
		{Interval{Days: -1, Nanos: -int64(2*time.Hour + 3*time.Minute + 4*time.Second)}, "sql_standard", "-1 2:03:04"},
		{Interval{Nanos: int64(3*time.Minute + 4*time.Second)}, "sql_standard", "0:03:04"},
		{ym.Add(dt), "sql_standard", "+1-2 +3 +4:05:06.7"},
		{ym.Add(dt.Neg()), "sql_standard", "+1-2 -3 -4:05:06.7"},
		{ym.Neg().Add(dt), "sql_standard", "-1-2 +3 +4:05:06.7"},
		{Interval{Days: -1, Nanos: int64(time.Hour)}, "sql_standard", "+0-0 -1 +1:00:00"},
		{Interval{}, "postgres_verbose", "@ 0"},
		{ym, "postgres_verbose", "@ 1 year 2 mons"},
		{Interval{Days: -1, Nanos: int64(2*time.Hour + 3*time.Minute)}, "postgres_verbose", "@ 1 day -2 hours -3 mins ago"},
		{Interval{Nanos: -int64(1500 * time.Millisecond)}, "postgres_verbose", "@ 1.5 secs ago"},
		{Interval{Days: 1, Nanos: int64(time.Second)}, "postgres_verbose", "@ 1 day 1 sec"},
		{Interval{Days: -1, Nanos: int64(2 * time.Hour)}, "postgres", "-1 days +02:00:00"},
	} {
		if got := c.iv.Format(c.style); got != c.want {
			t.Errorf("%+v in %s: %q, want %q", c.iv, c.style, got, c.want)
		}
	}
}

// TestFormatByteaEscape: bytea_output = escape.
func TestFormatByteaEscape(t *testing.T) {
	if got := FormatByteaEscape([]byte("ab\\c\x00\xff\n")); got != `ab\\c\000\377\012` {
		t.Fatalf("got %q", got)
	}
}
