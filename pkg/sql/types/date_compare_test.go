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

// TestDateMidnight: the one conversion of a date to a timestamp — its
// midnight in the session's zone for a timestamp with time zone, its
// wall-clock midnight for one without — and the paths that take it: a
// coercion under a zone (CoerceIn), and a date carrying the zone as
// its Loc (a CASE's result), in Coerce and Compare alike.
func TestDateMidnight(t *testing.T) {
	ny, err := time.LoadLocation("America/New_York")
	if err != nil {
		t.Skip("no zone database:", err)
	}
	tokyo, err := time.LoadLocation("Asia/Tokyo")
	if err != nil {
		t.Skip("no zone database:", err)
	}
	days, err := ParseDate("2024-01-01")
	if err != nil {
		t.Fatal(err)
	}
	for _, c := range []struct {
		loc  *time.Location
		want string
	}{
		{nil, "2024-01-01 00:00:00+00"},
		{time.UTC, "2024-01-01 00:00:00+00"},
		{ny, "2024-01-01 05:00:00+00"},
		{tokyo, "2023-12-31 15:00:00+00"},
	} {
		n, err := DateMidnight(days, c.loc)
		if err != nil || NewTimestamp(n).Text() != c.want {
			t.Errorf("DateMidnight in %v: %s %v, want %s", c.loc, NewTimestamp(n).Text(), err, c.want)
		}
		v, err := NewDate(days).CoerceIn(Timestamp, c.loc)
		if err != nil || v.Text() != c.want {
			t.Errorf("CoerceIn %v: %s %v, want %s", c.loc, v.Text(), err, c.want)
		}
		d := NewDate(days)
		d.Loc = c.loc
		if v, err := d.Coerce(Timestamp); err != nil || v.Text() != c.want {
			t.Errorf("Coerce of a date in %v: %s %v, want %s", c.loc, v.Text(), err, c.want)
		}
		at, _ := ParseTimestamp(c.want)
		if cmp, err := d.Compare(NewTimestamp(at)); err != nil || cmp != 0 {
			t.Errorf("Compare of a date in %v with %s: %d %v, want 0", c.loc, c.want, cmp, err)
		}
	}
	// A date whose midnight in the zone falls outside the range.
	if _, err := DateMidnight(minTimestampDay, tokyo); err == nil {
		t.Error("1678-01-01 in Asia/Tokyo: no error, want ErrTimestampRange")
	}
	if c, err := NewDate(minTimestampDay).CompareIn(NewTimestamp(minTimestamp.UnixNano()), tokyo); err != nil || c != -1 {
		t.Errorf("1678-01-01 in Asia/Tokyo against the first timestamp: %d %v, want -1", c, err)
	}
}
