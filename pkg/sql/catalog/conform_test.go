package catalog

import (
	"errors"
	"testing"

	"github.com/sthorne/datax/pkg/sql/types"
)

// TestColumnConform: the write-path rules for the integer widths, the
// character lengths, CHAR(n) padding and the TIMESTAMP forms (issue
// #96) — and the rendering, typmod and SQL spelling they imply.
func TestColumnConform(t *testing.T) {
	code := func(err error) string {
		var ve *ValueError
		if errors.As(err, &ve) {
			return ve.Code
		}
		return ""
	}
	int2 := Column{Name: "s", Type: types.Int, Width: 2}
	int4 := Column{Name: "i", Type: types.Int, Width: 4}
	int8 := Column{Name: "b", Type: types.Int}
	for _, c := range []struct {
		col  Column
		v    int64
		want string
	}{
		{int2, 32767, ""}, {int2, -32768, ""}, {int2, 32768, "22003"}, {int2, -32769, "22003"},
		{int4, 2147483647, ""}, {int4, 2147483648, "22003"}, {int4, -2147483649, "22003"},
		{int8, 1 << 62, ""},
	} {
		_, err := c.col.Conform(types.NewInt(c.v))
		if code(err) != c.want {
			t.Errorf("%s %d: %v, want %q", c.col.TypeSQL(), c.v, err, c.want)
		}
	}

	vc := Column{Name: "v", Type: types.String, MaxLen: 3}
	ch := Column{Name: "c", Type: types.String, MaxLen: 3, Char: true}
	for _, c := range []struct {
		col        Column
		in, s, txt string
		want       string
	}{
		{vc, "abc", "abc", "abc", ""}, {vc, "abcd", "", "", "22001"}, {vc, "abc   ", "abc", "abc", ""}, {vc, "ab", "ab", "ab", ""},
		{vc, "héé", "héé", "héé", ""}, {vc, "hééé", "", "", "22001"},
		{ch, "ab", "ab", "ab ", ""}, {ch, "ab ", "ab", "ab ", ""}, {ch, "abcd", "", "", "22001"}, {ch, "abc  ", "abc", "abc", ""},
	} {
		d, err := c.col.Conform(types.NewString(c.in))
		if code(err) != c.want {
			t.Errorf("%s %q: %v, want %q", c.col.TypeSQL(), c.in, err, c.want)
			continue
		}
		if err == nil && (d.S != c.s || d.Text() != c.txt) {
			t.Errorf("%s %q: S=%q Text=%q, want %q / %q", c.col.TypeSQL(), c.in, d.S, d.Text(), c.s, c.txt)
		}
	}

	ts := Column{Name: "ts", Type: types.Timestamp, NoTZ: true}
	tz3 := Column{Name: "tz", Type: types.Timestamp, TimePrecision: 4}
	t0 := Column{Name: "t0", Type: types.Timestamp, NoTZ: true, TimePrecision: 1}
	d, err := ts.Conform(types.NewString("2024-01-02 03:04:05.5+05:00"))
	if err != nil || d.Text() != "2024-01-02 03:04:05.5" || !d.NoTZ {
		t.Fatalf("timestamp without time zone from text with an offset: %v %v", d.Text(), err)
	}
	if _, err := ts.Conform(types.NewString("not a time")); code(err) != "22007" {
		t.Fatalf("bad timestamp text: %v", err)
	}
	n, _ := types.ParseTimestamp("2024-01-02 03:04:05.1234567Z")
	d, err = tz3.Conform(types.NewTimestamp(n))
	if err != nil || d.Text() != "2024-01-02 03:04:05.123+00" || d.NoTZ {
		t.Fatalf("TIMESTAMPTZ(3): %v %v", d.Text(), err)
	}
	n, _ = types.ParseTimestamp("2024-01-02 03:04:05.9996Z")
	if d, _ = tz3.Conform(types.NewTimestamp(n)); d.Text() != "2024-01-02 03:04:06+00" {
		t.Fatalf("TIMESTAMPTZ(3) rounding: %v", d.Text())
	}

	n, _ = types.ParseTimestamp("2024-01-02 03:04:05.6Z")
	if d, _ = t0.Conform(types.NewTimestamp(n)); d.Text() != "2024-01-02 03:04:06" {
		t.Fatalf("TIMESTAMP(0) rounding: %v", d.Text())
	}

	// NULLs and foreign families pass through; plain columns are no-ops.
	if d, err := int2.Conform(types.DNull); err != nil || !d.Null {
		t.Fatal("NULL")
	}
	if d, err := vc.Conform(types.NewInt(7)); err != nil || d.I != 7 {
		t.Fatal("foreign family")
	}
	if (&Column{Type: types.String}).HasTypmod() || !int4.HasTypmod() || !ch.HasTypmod() || !ts.HasTypmod() || !tz3.HasTypmod() {
		t.Fatal("HasTypmod")
	}

	// Spelling and atttypmod.
	for _, c := range []struct {
		col    Column
		sql    string
		typmod int32
	}{
		{int2, "INT2", 0}, {int4, "INT4", 0}, {int8, "INT8", 0},
		{vc, "VARCHAR(3)", 7}, {ch, "CHAR(3)", 7}, {Column{Type: types.String}, "TEXT", 0},
		{ts, "TIMESTAMP", 0}, {tz3, "TIMESTAMPTZ(3)", 3}, {t0, "TIMESTAMP(0)", 0}, {Column{Type: types.Timestamp, NoTZ: true, TimePrecision: 7}, "TIMESTAMP(6)", 6},
		{Column{Type: types.Decimal, Precision: 10, Scale: 2}, "DECIMAL(10,2)", 10<<16 | 6},
	} {
		if got := c.col.TypeSQL(); got != c.sql {
			t.Errorf("TypeSQL: %q, want %q", got, c.sql)
		}
		if got := c.col.Typmod(); got != c.typmod {
			t.Errorf("%s Typmod: %d, want %d", c.sql, got, c.typmod)
		}
	}
}
