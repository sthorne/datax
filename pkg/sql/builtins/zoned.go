package builtins

import (
	"strings"
	"time"

	"github.com/sthorne/datax/pkg/sql/types"
)

// The session's TimeZone. A timestamp with time zone is an instant
// (UTC nanoseconds) whatever the zone; the zone decides how one is read
// from text that names no offset and how its calendar fields are seen:
// rendered as text, cast to date, time or timestamp, truncated,
// extracted from, and stepped by days and months. The functions here
// are the zone-aware forms of Cast and DateArith; a nil or UTC zone
// gives the plain forms.

func zoned(loc *time.Location) bool { return loc != nil && loc != time.UTC }

// castName is a cast's type name without its modifier or schema.
func castName(typ string) string {
	name, _ := splitTypmod(strings.ToLower(strings.TrimSpace(typ)))
	if i := strings.LastIndexByte(name, '.'); i >= 0 {
		name = name[i+1:]
	}
	return name
}

func isTextType(name string) bool {
	switch name {
	case "text", "varchar", "char", "character", "bpchar", "string", "character varying", "name":
		return true
	}
	return false
}

func isTimestamptzType(name string) bool {
	return name == "timestamptz" || name == "timestamp with time zone"
}

// TextIn renders a value as text in loc: a timestamp with time zone
// with the zone's offset ("2024-07-01 08:00:00-04"), anything else as
// Text does.
func TextIn(d types.Datum, loc *time.Location) string {
	if zoned(loc) && !d.Null && d.Fam == types.Timestamp && !d.NoTZ {
		return types.FormatTimestampIn(d.I, loc)
	}
	return d.Text()
}

// CastIn is Cast under the session's zone.
func CastIn(d types.Datum, typ string, loc *time.Location) (types.Datum, error) {
	if !zoned(loc) || d.Null {
		return Cast(d, typ)
	}
	name := castName(typ)
	switch {
	case d.Fam == types.Timestamp && !d.NoTZ:
		switch {
		case isTextType(name):
			return Cast(types.NewString(types.FormatTimestampIn(d.I, loc)), typ)
		case name == "date":
			return types.NewDate(floorDiv(types.InZone(d.I, loc), types.NanosPerDay)), nil
		case name == "time" || name == "time without time zone":
			return Cast(types.NewTimestamp(types.InZone(d.I, loc)), typ)
		case name == "timestamp" || name == "timestamp without time zone":
			w := types.NewTimestamp(types.InZone(d.I, loc))
			w.NoTZ = true
			return Cast(w, typ)
		}
	case d.Fam == types.Timestamp && d.NoTZ && isTimestamptzType(name):
		return Cast(types.NewTimestamp(types.WallInZone(d.I, loc)), typ)
	case d.Fam == types.Date && isTimestamptzType(name):
		n, err := types.DateMidnight(d.I, loc)
		if err != nil {
			return types.Datum{}, timestampErr(err, d.Text())
		}
		return Cast(types.NewTimestamp(n), typ)
	case d.Fam == types.String && isTimestamptzType(name):
		d.Loc = loc
	}
	return Cast(d, typ)
}

// AddIntervalIn shifts an instant by sign × iv in loc: the months and
// days move the wall clock (a day across a DST change is 23 or 25
// hours), the clock part elapses.
func AddIntervalIn(t time.Time, iv Interval, sign int64, loc *time.Location) time.Time {
	if !zoned(loc) {
		return iv.AddTo(t, sign)
	}
	if iv.Months != 0 || iv.Days != 0 {
		wall := time.Unix(0, types.InZone(t.UnixNano(), loc)).UTC()
		wall = Interval{Months: iv.Months, Days: iv.Days}.AddTo(wall, sign)
		n, err := types.TimeNanos(wall)
		if err != nil {
			// Outside the timestamp range: the caller refuses it
			// (tsDatum); the zone's offset cannot bring it back.
			return wall
		}
		t = time.Unix(0, types.WallInZone(n, loc)).UTC()
	}
	return t.Add(time.Duration(sign * iv.Nanos))
}

// DateArithIn is DateArith under the session's zone: a timestamp with
// time zone plus or minus an interval steps its days and months in the
// zone; the rest is DateArith.
func DateArithIn(l types.Datum, op string, r types.Datum, loc *time.Location) (types.Datum, bool, error) {
	if !zoned(loc) || (op != "+" && op != "-") {
		return DateArith(l, op, r)
	}
	ts, iv := l, r
	if op == "+" && r.Fam == types.Timestamp && (l.Fam == types.IntervalFam || l.Fam == types.String) {
		ts, iv = r, l
	}
	if ts.Fam != types.Timestamp || ts.NoTZ || (iv.Fam != types.IntervalFam && iv.Fam != types.String) {
		return DateArith(l, op, r)
	}
	step, err := toInterval(iv)
	if err != nil {
		return DateArith(l, op, r)
	}
	sign := int64(1)
	if op == "-" {
		sign = -1
	}
	out, err := tsDatum(AddIntervalIn(tsTime(ts), step, sign, loc))
	return out, true, err
}
