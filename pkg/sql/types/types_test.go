package types

import (
	"testing"
	"time"
)

func TestTimestampParseRender(t *testing.T) {
	for _, in := range []string{
		"2026-08-30T01:02:03Z",
		"2026-08-30 01:02:03Z",
		"2026-08-30 01:02:03",
		"2026-08-30 01:02:03.123456+00",
		"2026-08-30 03:02:03.123456+02",
		"2026-08-30",
	} {
		if _, err := ParseTimestamp(in); err != nil {
			t.Fatalf("%q: %v", in, err)
		}
	}
	// Offsets normalize to UTC.
	a, _ := ParseTimestamp("2026-08-30 03:02:03+02")
	b, _ := ParseTimestamp("2026-08-30 01:02:03Z")
	if a != b {
		t.Fatalf("offset normalization: %d vs %d", a, b)
	}
	// Render → parse round trip.
	d := NewTimestamp(time.Date(2026, 8, 30, 1, 2, 3, 123456000, time.UTC).UnixNano())
	if got := d.Text(); got != "2026-08-30 01:02:03.123456+00" {
		t.Fatalf("Text() = %q", got)
	}
	back, err := ParseTimestamp(d.Text())
	if err != nil || back != d.I {
		t.Fatalf("round trip: %d, %v", back, err)
	}
}

func TestDateParseRender(t *testing.T) {
	days, err := ParseDate("1970-01-02")
	if err != nil || days != 1 {
		t.Fatalf("days = %d, %v", days, err)
	}
	if got := NewDate(days).Text(); got != "1970-01-02" {
		t.Fatalf("Text() = %q", got)
	}
	if d, _ := ParseDate("1969-12-31"); d != -1 {
		t.Fatalf("pre-epoch days = %d", d)
	}
	if NewDate(-1).Text() != "1969-12-31" {
		t.Fatalf("pre-epoch render = %q", NewDate(-1).Text())
	}
}

func TestBytesParseRender(t *testing.T) {
	b, err := ParseBytes(`\xdeadbeef`)
	if err != nil || string(b) != "\xde\xad\xbe\xef" {
		t.Fatalf("hex parse: %x, %v", b, err)
	}
	if got := NewBytes(b).Text(); got != `\xdeadbeef` {
		t.Fatalf("Text() = %q", got)
	}
	if b, _ := ParseBytes("raw"); string(b) != "raw" {
		t.Fatalf("raw parse: %q", b)
	}
	if _, err := ParseBytes(`\xzz`); err == nil {
		t.Fatal("bad hex accepted")
	}
}

func TestUUIDParseRender(t *testing.T) {
	const canon = "a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11"
	u, err := ParseUUID(canon)
	if err != nil {
		t.Fatal(err)
	}
	if got := NewUUID(u).Text(); got != canon {
		t.Fatalf("Text() = %q", got)
	}
	u2, err := ParseUUID("A0EEBC999C0B4EF8BB6D6BB9BD380A11")
	if err != nil || u2 != u {
		t.Fatalf("dashless parse: %v", err)
	}
	if _, err := ParseUUID("nope"); err == nil {
		t.Fatal("bad uuid accepted")
	}
}

func TestNewTypeCoerceCompare(t *testing.T) {
	ts, err := NewString("2026-01-01 00:00:00Z").Coerce(Timestamp)
	if err != nil || ts.Fam != Timestamp {
		t.Fatalf("coerce: %+v, %v", ts, err)
	}
	later, _ := NewString("2026-06-01").Coerce(Timestamp)
	if c, err := ts.Compare(later); err != nil || c >= 0 {
		t.Fatalf("compare: %d, %v", c, err)
	}
	// DATE coerces up to TIMESTAMPTZ.
	dd, _ := NewString("2026-01-01").Coerce(Date)
	up, err := dd.Coerce(Timestamp)
	if err != nil || up.I != ts.I {
		t.Fatalf("date→timestamp: %+v, %v", up, err)
	}
	u1, _ := NewString("00000000-0000-0000-0000-000000000001").Coerce(Uuid)
	u2, _ := NewString("00000000-0000-0000-0000-000000000002").Coerce(Uuid)
	if c, err := u1.Compare(u2); err != nil || c >= 0 {
		t.Fatalf("uuid compare: %d, %v", c, err)
	}
}

func TestDecimalDscaleText(t *testing.T) {
	for _, c := range []struct {
		s      string
		dscale int32
		want   string
	}{
		{"9.9", 2, "9.90"},
		{"1", 2, "1.00"},
		{"0", 2, "0.00"},
		{"-2.5", 2, "-2.50"},
		{"1.25", 2, "1.25"},
		{"42", 0, "42"},
		{"0.1", 4, "0.1000"},
	} {
		d := NewDecimal(c.s)
		d.Dscale = c.dscale
		if got := d.Text(); got != c.want {
			t.Fatalf("Text(%q, dscale %d) = %q, want %q", c.s, c.dscale, got, c.want)
		}
		// Dscale never changes identity.
		plain := NewDecimal(c.s)
		if cmp, err := d.Compare(plain); err != nil || cmp != 0 {
			t.Fatalf("Compare(%q dscale=%d, plain) = %d, %v", c.s, c.dscale, cmp, err)
		}
	}
}
