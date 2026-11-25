package types

import (
	"testing"
	"time"
)

// TestCompareDateTimestamp: a date compares with a timestamp as
// PostgreSQL's date_cmp_timestamp(tz) does, promoted to its midnight —
// in the session's zone against a timestamp with time zone, as wall
// time against one without — and a date outside the timestamp range is
// before or after every timestamp rather than an error.
func TestCompareDateTimestamp(t *testing.T) {
	day := func(s string) Datum {
		d, err := ParseDate(s)
		if err != nil {
			t.Fatal(err)
		}
		return NewDate(d)
	}
	tz := func(s string) Datum {
		n, err := ParseTimestamp(s)
		if err != nil {
			t.Fatal(err)
		}
		return NewTimestamp(n)
	}
	wall := func(s string) Datum {
		d := tz(s)
		d.NoTZ = true
		return d
	}
	ny, err := time.LoadLocation("America/New_York")
	if err != nil {
		t.Skip("no zone database:", err)
	}
	for _, c := range []struct {
		d, ts Datum
		loc   *time.Location
		want  int
	}{
		{day("2024-01-01"), tz("2024-01-01 00:00:00+00"), nil, 0},
		{day("2024-01-01"), tz("2024-01-01 00:00:01+00"), nil, -1},
		{day("2024-01-02"), tz("2024-01-01 23:59:59+00"), nil, 1},
		// Midnight in New York is 05:00 UTC in January.
		{day("2024-01-01"), tz("2024-01-01 00:00:00+00"), ny, 1},
		{day("2024-01-01"), tz("2024-01-01 05:00:00+00"), ny, 0},
		// A timestamp without time zone ignores the zone.
		{day("2024-01-01"), wall("2024-01-01 00:00:00"), ny, 0},
		{day("2024-01-01"), wall("2023-12-31 23:00:00"), ny, 1},
		// Outside the timestamp range.
		{day("1500-01-01"), tz("1678-01-01 00:00:00+00"), nil, -1},
		{day("1500-01-01"), wall("2024-01-01 00:00:00"), ny, -1},
		{day("3000-01-01"), tz("2261-12-31 23:59:59+00"), ny, 1},
	} {
		got, err := c.d.CompareIn(c.ts, c.loc)
		if err != nil || got != c.want {
			t.Errorf("%s vs %s in %v: %d %v, want %d", c.d.Text(), c.ts.Text(), c.loc, got, err, c.want)
		}
		if back, err := c.ts.CompareIn(c.d, c.loc); err != nil || back != -c.want {
			t.Errorf("%s vs %s in %v: %d %v, want %d", c.ts.Text(), c.d.Text(), c.loc, back, err, -c.want)
		}
	}
	// Compare is CompareIn in UTC.
	if c, err := day("2024-01-01").Compare(tz("2024-01-01 00:00:00+00")); err != nil || c != 0 {
		t.Errorf("Compare: %d %v", c, err)
	}
}
