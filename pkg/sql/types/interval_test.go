package types

import (
	"testing"
	"time"
)

// TestParseInterval: the input syntaxes PostgreSQL's regression corpus
// exercises (verbose, SQL standard, ISO 8601, "ago"), the rendering, the
// comparison value, arithmetic and the justify_* rewrites.
func TestParseInterval(t *testing.T) {
	h, m, sec := int64(time.Hour), int64(time.Minute), int64(time.Second)
	for _, c := range []struct {
		in   string
		want Interval
		text string
	}{
		{"1 day", Interval{Days: 1}, "1 day"},
		{"@ 1 minute", Interval{Nanos: m}, "00:01:00"},
		{"@ 5 hour", Interval{Nanos: 5 * h}, "05:00:00"},
		{"@ 10 day", Interval{Days: 10}, "10 days"},
		{"@ 34 year", Interval{Months: 34 * 12}, "34 years"},
		{"@ 3 months", Interval{Months: 3}, "3 mons"},
		{"@ 14 seconds ago", Interval{Nanos: -14 * sec}, "-00:00:14"},
		{"1 day 2 hours 3 minutes 4 seconds", Interval{Days: 1, Nanos: 2*h + 3*m + 4*sec}, "1 day 02:03:04"},
		{"6 years", Interval{Months: 72}, "6 years"},
		{"5 months", Interval{Months: 5}, "5 mons"},
		{"5 months 12 hours", Interval{Months: 5, Nanos: 12 * h}, "5 mons 12:00:00"},
		{"1 year 2 mons 3 days 04:05:06", Interval{Months: 14, Days: 3, Nanos: 4*h + 5*m + 6*sec}, "1 year 2 mons 3 days 04:05:06"},
		{"1-2", Interval{Months: 14}, "1 year 2 mons"},
		{"-1-2", Interval{Months: -14}, "-1 years -2 mons"},
		{"-1 day 12:00:00", Interval{Days: -1, Nanos: 12 * h}, "-1 days +12:00:00"},
		{"1 day -2 hours", Interval{Days: 1, Nanos: -2 * h}, "1 day -02:00:00"},
		{"-1 day -2 hours", Interval{Days: -1, Nanos: -2 * h}, "-1 days -02:00:00"},
		{"1 month -1 day +2 hours", Interval{Months: 1, Days: -1, Nanos: 2 * h}, "1 mon -1 days +02:00:00"},
		{"1-2 3 04:05:06", Interval{Months: 14, Days: 3, Nanos: 4*h + 5*m + 6*sec}, "1 year 2 mons 3 days 04:05:06"},
		{"3 04:05:06", Interval{Days: 3, Nanos: 4*h + 5*m + 6*sec}, "3 days 04:05:06"},
		{"04:05:06.789", Interval{Nanos: 4*h + 5*m + 6*sec + 789*int64(time.Millisecond)}, "04:05:06.789"},
		{"04:05", Interval{Nanos: 4*h + 5*m}, "04:05:00"},
		{"-04:05:06", Interval{Nanos: -(4*h + 5*m + 6*sec)}, "-04:05:06"},
		{"P1Y2M3DT4H5M6S", Interval{Months: 14, Days: 3, Nanos: 4*h + 5*m + 6*sec}, "1 year 2 mons 3 days 04:05:06"},
		{"P1W", Interval{Days: 7}, "7 days"},
		{"PT36H", Interval{Nanos: 36 * h}, "36:00:00"},
		{"P0D", Interval{}, "00:00:00"},
		{"1.5 hours", Interval{Nanos: 90 * m}, "01:30:00"},
		{"1.5 days", Interval{Days: 1, Nanos: 12 * h}, "1 day 12:00:00"},
		{"2 weeks", Interval{Days: 14}, "14 days"},
		{"1 millennium 2 centuries 3 decades", Interval{Months: 12000 + 2400 + 360}, "1230 years"},
		{"250 milliseconds", Interval{Nanos: 250 * int64(time.Millisecond)}, "00:00:00.25"},
		{"90 minutes", Interval{Nanos: 90 * m}, "01:30:00"},
		{"2h30m", Interval{Nanos: 2*h + 30*m}, "02:30:00"},
		{"0", Interval{}, "00:00:00"},
		{"1 day 2", Interval{Days: 1, Nanos: 2 * sec}, "1 day 00:00:02"},
		{"42", Interval{Nanos: 42 * sec}, "00:00:42"},
	} {
		got, err := ParseInterval(c.in)
		if err != nil {
			t.Errorf("%q: %v", c.in, err)
			continue
		}
		if got != c.want {
			t.Errorf("%q: %+v, want %+v", c.in, got, c.want)
		}
		if got.String() != c.text {
			t.Errorf("%q renders %q, want %q", c.in, got.String(), c.text)
		}
	}
	for _, bad := range []string{"", "3 fortnights", "P", "abc", "1:2:3:4", "1 day x"} {
		if _, err := ParseInterval(bad); err == nil {
			t.Errorf("%q: parsed, want an error", bad)
		}
	}

	// Ordering: a month is 30 days, a day 24 hours.
	if (Interval{Months: 1}).CmpValue() != (Interval{Days: 30}).CmpValue() || (Interval{Days: 1}).CmpValue() != (Interval{Nanos: 24 * h}).CmpValue() {
		t.Fatal("comparison value")
	}
	a, b := NewInterval(Interval{Days: 31}), NewInterval(Interval{Months: 1})
	if c, _ := a.Compare(b); c != 1 {
		t.Fatalf("31 days vs 1 mon: %d", c)
	}

	// Arithmetic and justification.
	iv := Interval{Months: 1, Days: 1, Nanos: h}
	if got := iv.Scale(2); got != (Interval{Months: 2, Days: 2, Nanos: 2 * h}) {
		t.Fatalf("scale: %+v", got)
	}
	if got := (Interval{Months: 1}).Scale(1.5); got != (Interval{Months: 1, Days: 15}) {
		t.Fatalf("1.5 months: %+v", got)
	}
	if got := (Interval{Days: 1}).Scale(0.5); got != (Interval{Nanos: 12 * h}) {
		t.Fatalf("half a day: %+v", got)
	}
	if got := (Interval{Nanos: 27 * h}).JustifyHours(); got != (Interval{Days: 1, Nanos: 3 * h}) {
		t.Fatalf("justify_hours: %+v", got)
	}
	if got := (Interval{Days: 35}).JustifyDays(); got != (Interval{Months: 1, Days: 5}) {
		t.Fatalf("justify_days: %+v", got)
	}
	if got := (Interval{Months: 1, Days: -1}).Justify(); got != (Interval{Days: 29}) {
		t.Fatalf("justify_interval: %+v", got)
	}
	jan31 := time.Date(2024, 1, 31, 10, 0, 0, 0, time.UTC)
	if got := (Interval{Months: 1}).AddTo(jan31, 1); got != time.Date(2024, 2, 29, 10, 0, 0, 0, time.UTC) {
		t.Fatalf("Jan 31 + 1 month: %v", got)
	}
	if got := (Interval{Months: 1, Days: 1, Nanos: h}).AddTo(jan31, -1); got != time.Date(2023, 12, 30, 9, 0, 0, 0, time.UTC) {
		t.Fatalf("Jan 31 - 1 mon 1 day 1 hour: %v", got)
	}
}

// TestParseTime: TIME input forms and the rendering.
func TestParseTime(t *testing.T) {
	h, m, sec := int64(time.Hour), int64(time.Minute), int64(time.Second)
	for _, c := range []struct {
		in   string
		want int64
		text string
	}{
		{"04:05:06", 4*h + 5*m + 6*sec, "04:05:06"},
		{"04:05", 4*h + 5*m, "04:05:00"},
		{"04:05:06.789", 4*h + 5*m + 6*sec + 789*int64(time.Millisecond), "04:05:06.789"},
		{"4:05 PM", 16*h + 5*m, "16:05:00"},
		{"12:00 AM", 0, "00:00:00"},
		{"12:30 PM", 12*h + 30*m, "12:30:00"},
		{"04:05:06+05:30", 4*h + 5*m + 6*sec, "04:05:06"},
		{"04:05:06Z", 4*h + 5*m + 6*sec, "04:05:06"},
		{"2024-01-02 03:04:05", 3*h + 4*m + 5*sec, "03:04:05"},
		{"2024-01-02T03:04:05.5Z", 3*h + 4*m + 5*sec + 500*int64(time.Millisecond), "03:04:05.5"},
		{"24:00:00", 24 * h, "24:00:00"},
		{"4 PM", 16 * h, "16:00:00"},
		{"7", 7 * h, "07:00:00"},
	} {
		got, err := ParseTime(c.in)
		if err != nil {
			t.Errorf("%q: %v", c.in, err)
			continue
		}
		if got != c.want || NewTime(got).Text() != c.text {
			t.Errorf("%q: %d (%s), want %d (%s)", c.in, got, NewTime(got).Text(), c.want, c.text)
		}
	}
	for _, bad := range []string{"", "25:00", "04:60", "abc", "13:00 PM", "25"} {
		if _, err := ParseTime(bad); err == nil {
			t.Errorf("%q: parsed, want an error", bad)
		}
	}
	if d, err := NewString("2024-01-02 03:04:05.25").Coerce(Timestamp); err != nil {
		t.Fatal(err)
	} else if tm, err := d.Coerce(Time); err != nil || tm.Text() != "03:04:05.25" {
		t.Fatalf("timestamp → time: %v %v", tm, err)
	}
}
