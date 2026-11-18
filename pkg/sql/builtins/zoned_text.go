package builtins

import (
	"strconv"
	"time"

	"github.com/sthorne/datax/pkg/sql/types"
)

// JSONTimeText renders a date or time value as to_jsonb does
// (PostgreSQL's XSD style): a timestamp with time zone as
// "2024-07-01T08:00:00.5-04:00" with the zone's offset (UTC for a nil
// loc), a timestamp without one as "2024-07-01T08:00:00.5", a date as
// "2024-07-01" and a time as "08:00:00.5". ok is false for any other
// value.
func JSONTimeText(d types.Datum, loc *time.Location) (string, bool) {
	if d.Null {
		return "", false
	}
	switch d.Fam {
	case types.Timestamp:
		if d.NoTZ {
			return time.Unix(0, types.RoundMicros(d.I)).UTC().Format("2006-01-02T15:04:05.999999"), true
		}
		if loc == nil {
			loc = time.UTC
		}
		return time.Unix(0, types.RoundMicros(d.I)).In(loc).Format("2006-01-02T15:04:05.999999-07:00"), true
	case types.Date:
		return d.Text(), true
	case types.Time:
		return types.FormatClock(types.RoundMicros(d.I)), true
	}
	return "", false
}

// zoneTextArgs renders the timestamps with time zone among a call's
// arguments (and their arrays' elements) as text in loc, for the
// functions that render their arguments as text (format,
// array_to_string, concat): the text then shows the session's offset,
// as PostgreSQL's does.
func zoneTextArgs(args []types.Datum, loc *time.Location) []types.Datum {
	out := make([]types.Datum, len(args))
	for i, a := range args {
		out[i] = zoneText(a, loc)
	}
	return out
}

func zoneText(a types.Datum, loc *time.Location) types.Datum {
	switch {
	case a.Null:
	case a.Fam == types.Timestamp && !a.NoTZ:
		return types.NewString(types.FormatTimestampIn(a.I, loc))
	case a.Fam == types.ArrayOf(types.Timestamp):
		elems := make([]types.Datum, len(a.A))
		for j, e := range a.A {
			elems[j] = zoneText(e, loc)
		}
		return types.NewArray(types.String, elems)
	}
	return a
}

// zoneJSONArgs renders the timestamps with time zone at the value
// positions of a JSON-building call as JSON strings in loc.
func zoneJSONArgs(args []types.Datum, loc *time.Location, valueAt func(i int) bool) []types.Datum {
	out := make([]types.Datum, len(args))
	for i, a := range args {
		out[i] = a
		if !valueAt(i) || a.Null || a.Fam != types.Timestamp || a.NoTZ {
			continue
		}
		if s, ok := JSONTimeText(a, loc); ok {
			out[i] = types.NewJsonb(strconv.Quote(s))
		}
	}
	return out
}

// setZFn gives a registered builtin (and its aliases) a zone-aware form.
func setZFn(name string, zfn func(args []types.Datum, loc *time.Location) (types.Datum, error)) {
	b, ok := registry[name]
	if !ok {
		panic("setZFn: no builtin " + name)
	}
	b.ZFn = zfn
	for _, a := range b.Aliases {
		registry[a].ZFn = zfn
	}
}

func init() {
	// Called after the registrations (files initialize in name order).
	for _, name := range []string{"format", "array_to_string", "concat", "concat_ws"} {
		b, ok := registry[name]
		if !ok {
			continue
		}
		fn := b.Fn
		setZFn(name, func(args []types.Datum, loc *time.Location) (types.Datum, error) {
			return fn(zoneTextArgs(args, loc))
		})
	}
	every := func(int) bool { return true }
	odd := func(i int) bool { return i%2 == 1 }
	for name, at := range map[string]func(int) bool{"to_jsonb": every, "jsonb_build_array": every, "jsonb_build_object": odd} {
		fn := registry[name].Fn
		setZFn(name, func(args []types.Datum, loc *time.Location) (types.Datum, error) {
			return fn(zoneJSONArgs(args, loc, at))
		})
	}
}
