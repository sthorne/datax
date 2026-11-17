package builtins

import (
	"strings"
	"time"

	"github.com/sthorne/datax/pkg/sql/types"
)

// zoneArg resolves timezone()'s zone: text, or an interval of hours
// and minutes (east positive, as ISO 8601 writes offsets).
func zoneArg(d types.Datum) (*time.Location, error) {
	if d.Fam == types.IntervalFam {
		iv := d.IntervalVal()
		if iv.Months != 0 || iv.Days != 0 {
			return nil, errf(CodeInvalidArgument, "interval time zone %q must not include months or days", iv.String())
		}
		return time.FixedZone(iv.String(), int(iv.Nanos/1e9)), nil
	}
	return ParseZone(d.Text(), true)
}

// ParseZone resolves a zone (types.ParseZone), an unknown one 22023.
func ParseZone(s string, abbrevs bool) (*time.Location, error) {
	loc, err := types.ParseZone(s, abbrevs)
	if err != nil {
		return nil, errf(CodeInvalidArgument, "time zone %q not recognized", s)
	}
	return loc, nil
}

// ZoneIsUTC is types.ZoneIsUTC.
func ZoneIsUTC(loc *time.Location) bool { return types.ZoneIsUTC(loc) }

// AtTimeZone is ts AT TIME ZONE zone: a timestamp with time zone gives
// the wall clock in the zone, a timestamp WITHOUT time zone (NoTZ) the
// instant that wall clock names in the zone. Text is read as a
// timestamp with time zone, PostgreSQL's preferred type; a date as its
// midnight.
func AtTimeZone(zone, ts types.Datum) (types.Datum, error) {
	if zone.Null || ts.Null {
		return types.DNull, nil
	}
	loc, err := zoneArg(zone)
	if err != nil {
		return types.Datum{}, err
	}
	switch ts.Fam {
	case types.Timestamp:
	case types.String, types.Date:
		v, cerr := ts.Coerce(types.Timestamp)
		if cerr != nil {
			return types.Datum{}, timestampErr(cerr, ts.Text())
		}
		ts = v
	default:
		return types.Datum{}, errf(CodeUndefined, "function timezone(text, %s) does not exist", strings.ToLower(ts.Fam.String()))
	}
	if ts.NoTZ {
		return types.NewTimestamp(types.WallInZone(ts.I, loc)), nil
	}
	t := time.Unix(0, ts.I).In(loc)
	wall := time.Date(t.Year(), t.Month(), t.Day(), t.Hour(), t.Minute(), t.Second(), t.Nanosecond(), time.UTC)
	out := types.NewTimestamp(wall.UnixNano())
	out.NoTZ = true
	return out, nil
}

func init() {
	register(&Builtin{Name: "timezone", Args: []types.Family{Any, Any}, MinArgs: 2, Ret: types.Timestamp, Category: catTime,
		Doc: "ts AT TIME ZONE zone: a timestamp with time zone becomes the wall-clock timestamp in the zone, a timestamp without time zone the instant it names there. The zone is an IANA name, an abbreviation, a POSIX offset ('UTC+5' is five hours west) or an interval.",
		Fn:  func(a []types.Datum) (types.Datum, error) { return AtTimeZone(a[0], a[1]) }})
}
