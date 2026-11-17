package types

import (
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	// The IANA database ships in the binary, so zone names resolve on a
	// host without /usr/share/zoneinfo.
	_ "time/tzdata"
)

// ErrUnknownZone wraps a time zone name that resolves to nothing.
var ErrUnknownZone = errors.New("time zone not recognized")

// zoneAbbrevs are the time zone abbreviations AT TIME ZONE and
// timezone() accept (fixed offsets, east positive), the common ones of
// PostgreSQL's default set. The TimeZone setting takes none of them,
// as in PostgreSQL.
var zoneAbbrevs = map[string]int{
	"UT": 0, "GMT": 0, "UTC": 0, "Z": 0, "ZULU": 0,
	"EST": -5 * 3600, "EDT": -4 * 3600, "CST": -6 * 3600, "CDT": -5 * 3600,
	"MST": -7 * 3600, "MDT": -6 * 3600, "PST": -8 * 3600, "PDT": -7 * 3600,
	"AKST": -9 * 3600, "AKDT": -8 * 3600, "HST": -10 * 3600,
	"WET": 0, "WEST": 3600, "BST": 3600, "CET": 3600, "CEST": 2 * 3600, "MET": 3600, "MEST": 2 * 3600,
	"EET": 2 * 3600, "EEST": 3 * 3600, "MSK": 3 * 3600, "IST": 2 * 3600,
	"JST": 9 * 3600, "KST": 9 * 3600, "HKT": 8 * 3600, "AWST": 8 * 3600,
	"ACST": 9*3600 + 1800, "AEST": 10 * 3600, "AEDT": 11 * 3600, "NZST": 12 * 3600, "NZDT": 13 * 3600,
}

// utcNames are the spellings of UTC itself.
var utcNames = map[string]bool{"utc": true, "gmt": true, "z": true, "zulu": true, "uct": true, "universal": true, "ut": true,
	"etc/utc": true, "etc/gmt": true, "etc/uct": true, "etc/universal": true, "etc/zulu": true, "gmt0": true, "etc/gmt0": true,
	"gmt+0": true, "gmt-0": true, "etc/gmt+0": true, "etc/gmt-0": true, "greenwich": true, "etc/greenwich": true}

// ParseZone resolves a time zone as PostgreSQL reads one: a full IANA
// name (any letter case), an abbreviation when abbrevs (AT TIME ZONE
// takes them, the TimeZone setting does not), or a POSIX-style offset
// ("+5", "-03:30", "UTC+5", "EST5") in which — unlike everywhere else
// — a positive number is WEST of Greenwich: 'UTC+5' is five hours
// behind UTC.
func ParseZone(s string, abbrevs bool) (*time.Location, error) {
	v := strings.TrimSpace(s)
	low := strings.ToLower(v)
	if utcNames[low] {
		return time.UTC, nil
	}
	if v == "" || low == "local" {
		return nil, fmt.Errorf("%w: %q", ErrUnknownZone, s)
	}
	if abbrevs {
		// An abbreviation wins over a zone of the same name (CET is
		// UTC+1 here, though the CET zone keeps summer time).
		if off, ok := zoneAbbrevs[strings.ToUpper(v)]; ok {
			return time.FixedZone(strings.ToUpper(v), off), nil
		}
	}
	if loc, ok := loadIANA(v); ok {
		return loc, nil
	}
	if off, ok := posixOffset(v); ok {
		if off == 0 {
			return time.UTC, nil
		}
		return time.FixedZone(v, off), nil
	}
	return nil, fmt.Errorf("%w: %q", ErrUnknownZone, s)
}

// loadIANA loads an IANA zone by name, as written or with each part
// capitalized (PostgreSQL matches zone names case-insensitively).
func loadIANA(v string) (*time.Location, bool) {
	if strings.ContainsAny(v, " .\\") || strings.HasPrefix(v, "/") {
		return nil, false
	}
	if c := v[0]; c == '+' || c == '-' || (c >= '0' && c <= '9') {
		return nil, false
	}
	if loc, err := time.LoadLocation(v); err == nil {
		return loc, true
	}
	parts := strings.Split(strings.ToLower(v), "/")
	for i, p := range parts {
		words := strings.Split(p, "_")
		for j, w := range words {
			if w != "" {
				words[j] = strings.ToUpper(w[:1]) + w[1:]
			}
		}
		parts[i] = strings.Join(words, "_")
	}
	if loc, err := time.LoadLocation(strings.Join(parts, "/")); err == nil {
		return loc, true
	}
	return nil, false
}

// posixOffset reads [name]±hh[:mm[:ss]] (a bare number included) as
// a POSIX zone: the returned offset is east-positive seconds, the
// opposite sign of what is written.
func posixOffset(v string) (int, bool) {
	i := 0
	for i < len(v) && (v[i] >= 'a' && v[i] <= 'z' || v[i] >= 'A' && v[i] <= 'Z') {
		i++
	}
	if i > 0 && i < 3 {
		return 0, false
	}
	rest := v[i:]
	if rest == "" {
		return 0, false
	}
	sign := 1
	switch rest[0] {
	case '+':
		rest = rest[1:]
	case '-':
		sign, rest = -1, rest[1:]
	}
	fields := strings.Split(rest, ":")
	if len(fields) > 3 {
		return 0, false
	}
	secs := 0
	for k, f := range fields {
		n, err := strconv.Atoi(f)
		if err != nil || n < 0 || f == "" || len(f) > 2 && k > 0 {
			return 0, false
		}
		if k > 0 && n > 59 {
			return 0, false
		}
		secs = secs*60 + n
	}
	for k := len(fields); k < 3; k++ {
		secs *= 60
	}
	if secs > 15*3600 {
		return 0, false
	}
	return -sign * secs, true
}

// ZoneIsUTC reports whether a zone is UTC at every instant datax can
// represent (Etc/UTC, '+00', 'GMT0' are; Africa/Abidjan, which kept
// local mean time until 1912, is not).
func ZoneIsUTC(loc *time.Location) bool {
	if loc == time.UTC {
		return true
	}
	for _, y := range []int{1678, 1850, 1900, 1950, 1970, 2000, 2030, 2100, 2261} {
		for _, m := range []time.Month{time.January, time.July} {
			if _, off := time.Date(y, m, 1, 0, 0, 0, 0, time.UTC).In(loc).Zone(); off != 0 {
				return false
			}
		}
	}
	return true
}

// WallInZone is the instant whose local time in loc reads as the wall
// clock w (UTC nanoseconds of the wall fields). A wall time that does
// not exist (skipped by a spring-forward) or exists twice (repeated
// by a fall-back) resolves as PostgreSQL does: to the later of the two
// instants the offsets around the transition give — the pre-transition
// offset in a gap, the post-transition one in an overlap.
func WallInZone(w int64, loc *time.Location) int64 {
	offAt := func(nanos int64) int64 {
		_, off := time.Unix(0, nanos).In(loc).Zone()
		return int64(off) * 1e9
	}
	const day = 36 * int64(time.Hour)
	before, after := offAt(w-day), offAt(w+day)
	tb, ta := w-before, w-after
	vb, va := offAt(tb) == before, offAt(ta) == after
	switch {
	case vb && !va:
		return tb
	case va && !vb:
		return ta
	case tb > ta:
		return tb
	}
	return ta
}

// ParseTimestampIn parses TIMESTAMPTZ input whose text may carry no
// offset: such a wall-clock time is read in loc (the session's
// TimeZone), as PostgreSQL reads it, with WallInZone's rule for a time
// a DST transition skips or repeats. An explicit offset ("+05",
// "Z") or a trailing zone name ("2024-07-01 12:00 America/New_York",
// "... EST") decides instead. A nil loc is UTC.
func ParseTimestampIn(s string, loc *time.Location) (int64, error) {
	if loc == nil {
		loc = time.UTC
	}
	for _, f := range timestampFormats {
		t, err := time.ParseInLocation(f, s, time.UTC)
		if err != nil {
			continue
		}
		if t.Before(minTimestamp) || !t.Before(maxTimestamp) {
			return 0, fmt.Errorf("%w: %q (1678-01-01 to 2261-12-31)", ErrTimestampRange, s)
		}
		if strings.Contains(f, "Z07") || loc == time.UTC {
			return t.UTC().UnixNano(), nil
		}
		return WallInZone(t.UnixNano(), loc), nil
	}
	if i := strings.LastIndexByte(strings.TrimSpace(s), ' '); i > 0 {
		head, name := strings.TrimSpace(s[:i]), strings.TrimSpace(s[i+1:])
		if c := name[0]; c != '+' && c != '-' && (c < '0' || c > '9') {
			if zl, err := ParseZone(name, true); err == nil {
				if n, err := ParseTimestampNoTZ(head); err == nil {
					return WallInZone(n, zl), nil
				}
			}
		}
	}
	return 0, fmt.Errorf("could not parse %q as TIMESTAMPTZ", s)
}

// InZone is the wall clock of UTC nanoseconds in loc, as UTC
// nanoseconds of those fields (timestamptz to timestamp).
func InZone(nanos int64, loc *time.Location) int64 {
	t := time.Unix(0, nanos).In(loc)
	return time.Date(t.Year(), t.Month(), t.Day(), t.Hour(), t.Minute(), t.Second(), t.Nanosecond(), time.UTC).UnixNano()
}
