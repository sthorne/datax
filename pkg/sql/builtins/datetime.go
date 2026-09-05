package builtins

import (
	"fmt"
	"math"
	"strconv"
	"strings"
	"time"

	"github.com/sthorne/datax/pkg/sql/types"
)

const catTime = "Date and time"

// Interval is the INTERVAL value (types.Interval): months and days kept
// apart from the clock part.
type Interval = types.Interval

// ParseInterval reads PostgreSQL's interval syntaxes (types.ParseInterval)
// and reports a failure as 22007.
func ParseInterval(s string) (Interval, error) {
	iv, err := types.ParseInterval(s)
	if err != nil {
		return iv, errf(CodeInvalidDatetime, "%v", err)
	}
	return iv, nil
}

// AddInterval shifts a timestamp by sign × an interval (Interval.AddTo).
func AddInterval(t time.Time, iv Interval, sign int64) time.Time { return iv.AddTo(t, sign) }

// toInterval reads an INTERVAL argument, or text in interval syntax.
func toInterval(d types.Datum) (Interval, error) {
	switch d.Fam {
	case types.IntervalFam:
		return d.IntervalVal(), nil
	case types.String:
		return ParseInterval(d.S)
	case types.Time:
		return Interval{Nanos: d.I}, nil
	}
	return Interval{}, errf(CodeUndefined, "function does not exist for argument type %s", strings.ToLower(d.Fam.String()))
}

// Between is the interval from a to b as age() computes it: whole
// years, months and days by the calendar, then the clock remainder.
func Between(b, a time.Time) Interval {
	neg := b.Before(a)
	if neg {
		a, b = b, a
	}
	months := int64((b.Year()-a.Year())*12 + int(b.Month()) - int(a.Month()))
	cursor := a.AddDate(0, int(months), 0)
	if cursor.After(b) {
		months--
		cursor = a.AddDate(0, int(months), 0)
	}
	days := int64(0)
	for cursor.AddDate(0, 0, 1).Compare(b) <= 0 {
		cursor = cursor.AddDate(0, 0, 1)
		days++
	}
	iv := Interval{Months: months, Days: days, Nanos: int64(b.Sub(cursor))}
	if neg {
		iv.Months, iv.Days, iv.Nanos = -iv.Months, -iv.Days, -iv.Nanos
	}
	return iv
}

func tsTime(d types.Datum) time.Time { return time.Unix(0, d.I).UTC() }
func dateTime(d types.Datum) time.Time {
	return time.Unix(d.I*86400, 0).UTC()
}
func tsDatum(t time.Time) types.Datum { return types.NewTimestamp(t.UTC().UnixNano()) }
func dateDatum(t time.Time) types.Datum {
	return types.NewDate(floorDiv(t.UTC().Unix(), 86400))
}

// DateArith evaluates l op r when a side is a date, timestamp, time or
// interval: timestamp / date ± interval (an INTERVAL value or text),
// date ± days, date − date (days), timestamp − timestamp and time − time
// (intervals), interval ± interval, interval × / number, time ±
// interval (wrapping at midnight), date + time (a timestamp). ok is
// false when the operands are not a date/time shape.
func DateArith(l types.Datum, op string, r types.Datum) (types.Datum, bool, error) {
	temporal := func(d types.Datum) bool { return d.Fam == types.Timestamp || d.Fam == types.Date }
	isNum := func(d types.Datum) bool { return d.Fam == types.Int || d.Fam == types.Float || d.Fam == types.Decimal }
	if !temporal(l) && !temporal(r) && l.Fam != types.IntervalFam && r.Fam != types.IntervalFam && l.Fam != types.Time && r.Fam != types.Time {
		return types.Datum{}, false, nil
	}
	if op != "+" && op != "-" && op != "*" && op != "/" {
		return types.Datum{}, false, nil
	}
	sign := int64(1)
	if op == "-" {
		sign = -1
	}
	// interval + date/timestamp/time and number × interval commute.
	if (!temporal(l) && l.Fam != types.Time && l.Fam != types.IntervalFam && op == "+") ||
		(l.Fam != types.IntervalFam && r.Fam == types.IntervalFam && op == "*") ||
		(l.Fam == types.IntervalFam && (r.Fam == types.Time || temporal(r)) && op == "+") {
		l, r = r, l
	}
	switch {
	case l.Fam == types.IntervalFam && r.Fam == types.IntervalFam && (op == "+" || op == "-"):
		iv := r.IntervalVal()
		if op == "-" {
			iv = iv.Neg()
		}
		return types.NewInterval(l.IntervalVal().Add(iv)), true, nil
	case l.Fam == types.IntervalFam && isNum(r) && (op == "*" || op == "/"):
		f, err := numeric(r)
		if err != nil {
			return types.Datum{}, true, err
		}
		if op == "/" {
			if f == 0 {
				return types.Datum{}, true, errf(CodeDivisionByZero, "division by zero")
			}
			f = 1 / f
		}
		return types.NewInterval(l.IntervalVal().Scale(f)), true, nil
	case l.Fam == types.Time && r.Fam == types.Time && op == "-":
		return types.NewInterval(Interval{Nanos: l.I - r.I}), true, nil
	case l.Fam == types.Time && (r.Fam == types.IntervalFam || r.Fam == types.String) && (op == "+" || op == "-"):
		iv, err := toInterval(r)
		if err != nil {
			return types.Datum{}, true, err
		}
		n := (l.I + sign*iv.Nanos) % types.NanosPerDay
		if n < 0 {
			n += types.NanosPerDay
		}
		return types.NewTime(n), true, nil
	case l.Fam == types.Date && r.Fam == types.Time && op == "+":
		return types.NewTimestamp(l.I*types.NanosPerDay + r.I), true, nil
	case l.Fam == types.Date && r.Fam == types.Date && op == "-":
		return types.NewInt(l.I - r.I), true, nil
	case l.Fam == types.Timestamp && temporal(r) && op == "-", l.Fam == types.Date && r.Fam == types.Timestamp && op == "-":
		lt, rt := temporalTime(l), temporalTime(r)
		d := lt.Sub(rt)
		days := int64(d / (24 * time.Hour))
		return types.NewInterval(Interval{Days: days, Nanos: int64(d - time.Duration(days)*24*time.Hour)}), true, nil
	case l.Fam == types.Date && r.Fam == types.Int && (op == "+" || op == "-"):
		return types.NewDate(l.I + sign*r.I), true, nil
	case temporal(l) && (r.Fam == types.String || r.Fam == types.IntervalFam) && (op == "+" || op == "-"):
		iv, err := toInterval(r)
		if err != nil {
			return types.Datum{}, true, err
		}
		// date ± interval is a timestamp, as in PostgreSQL.
		return tsDatum(iv.AddTo(temporalTime(l), sign)), true, nil
	case temporal(l) && isNum(r) && l.Fam == types.Timestamp:
		return types.Datum{}, true, errf(CodeUndefined, "operator does not exist: timestamptz %s %s", op, strings.ToLower(r.Fam.String()))
	}
	return types.Datum{}, true, errf(CodeUndefined, "operator does not exist: %s %s %s", strings.ToLower(l.Fam.String()), op, strings.ToLower(r.Fam.String()))
}

func temporalTime(d types.Datum) time.Time {
	if d.Fam == types.Date {
		return dateTime(d)
	}
	return tsTime(d)
}

// toTime coerces a timestamp, date or text argument to a time.
func toTime(d types.Datum) (time.Time, bool, error) {
	switch d.Fam {
	case types.Timestamp:
		return tsTime(d), false, nil
	case types.Date:
		return dateTime(d), true, nil
	case types.String:
		ts, err := d.Coerce(types.Timestamp)
		if err == nil {
			return tsTime(ts), false, nil
		}
		if dd, derr := d.Coerce(types.Date); derr == nil {
			return dateTime(dd), true, nil
		}
		return time.Time{}, false, timestampErr(err, d.S)
	}
	return time.Time{}, false, errf(CodeUndefined, "function does not exist for argument type %s", d.Fam)
}

func decimalText(f float64) types.Datum {
	d, err := types.ParseDecimal(strconv.FormatFloat(f, 'f', -1, 64))
	if err != nil {
		return types.NewFloat(f)
	}
	return d
}

func init() {
	register(&Builtin{Name: "extract", Args: []types.Family{types.String, Any}, MinArgs: 2, Ret: types.Decimal, Category: catTime,
		Doc:     "A field of a date or timestamp (also extract(field FROM x)): year, quarter, month, week, day, doy, dow, isodow, hour, minute, second, milliseconds, microseconds, epoch, century, decade, millennium.",
		Aliases: []string{"date_part"},
		Fn: func(a []types.Datum) (types.Datum, error) {
			switch a[1].Fam {
			case types.IntervalFam:
				return extractInterval(a[0].S, a[1].IntervalVal())
			case types.Time:
				return extractInterval(a[0].S, Interval{Nanos: a[1].I})
			}
			t, _, err := toTime(a[1])
			if err != nil {
				return types.Datum{}, err
			}
			switch strings.ToLower(a[0].S) {
			case "year", "years", "y":
				return i64Dec(int64(t.Year())), nil
			case "quarter":
				return i64Dec(int64((int(t.Month())-1)/3 + 1)), nil
			case "month", "months", "mon":
				return i64Dec(int64(t.Month())), nil
			case "week", "isoweek":
				_, w := t.ISOWeek()
				return i64Dec(int64(w)), nil
			case "isoyear":
				y, _ := t.ISOWeek()
				return i64Dec(int64(y)), nil
			case "day", "days", "d":
				return i64Dec(int64(t.Day())), nil
			case "doy":
				return i64Dec(int64(t.YearDay())), nil
			case "dow":
				return i64Dec(int64(t.Weekday())), nil
			case "isodow":
				wd := int64(t.Weekday())
				if wd == 0 {
					wd = 7
				}
				return i64Dec(wd), nil
			case "hour", "hours", "h":
				return i64Dec(int64(t.Hour())), nil
			case "minute", "minutes", "min", "m":
				return i64Dec(int64(t.Minute())), nil
			case "second", "seconds", "sec", "s":
				return decimalText(float64(t.Second()) + float64(t.Nanosecond())/1e9), nil
			case "milliseconds", "millisecond", "ms":
				return decimalText(float64(t.Second())*1000 + float64(t.Nanosecond())/1e6), nil
			case "microseconds", "microsecond", "us":
				return i64Dec(int64(t.Second())*1000000 + int64(t.Nanosecond())/1000), nil
			case "epoch":
				return decimalText(float64(t.UnixNano()) / 1e9), nil
			case "century", "centuries":
				return i64Dec(int64((t.Year()-1)/100 + 1)), nil
			case "decade", "decades":
				return i64Dec(int64(t.Year() / 10)), nil
			case "millennium", "millennia":
				return i64Dec(int64((t.Year()-1)/1000 + 1)), nil
			case "timezone", "timezone_hour", "timezone_minute":
				return i64Dec(0), nil
			}
			return types.Datum{}, errf(CodeInvalidArgument, "unit %q not recognized for extract", a[0].S)
		}})
	register(&Builtin{Name: "date_trunc", Args: []types.Family{types.String, Any}, MinArgs: 2, Ret: types.Timestamp, Category: catTime,
		Doc: "The timestamp truncated to the field: millennium, century, decade, year, quarter, month, week, day, hour, minute, second, milliseconds.",
		Fn: func(a []types.Datum) (types.Datum, error) {
			t, _, err := toTime(a[1])
			if err != nil {
				return types.Datum{}, err
			}
			y, m, d := t.Date()
			switch strings.ToLower(a[0].S) {
			case "millennium":
				t = time.Date((y-1)/1000*1000+1, 1, 1, 0, 0, 0, 0, time.UTC)
			case "century":
				t = time.Date((y-1)/100*100+1, 1, 1, 0, 0, 0, 0, time.UTC)
			case "decade":
				t = time.Date(y/10*10, 1, 1, 0, 0, 0, 0, time.UTC)
			case "year", "years":
				t = time.Date(y, 1, 1, 0, 0, 0, 0, time.UTC)
			case "quarter":
				t = time.Date(y, time.Month((int(m)-1)/3*3+1), 1, 0, 0, 0, 0, time.UTC)
			case "month", "months":
				t = time.Date(y, m, 1, 0, 0, 0, 0, time.UTC)
			case "week":
				wd := int(t.Weekday())
				if wd == 0 {
					wd = 7
				}
				t = time.Date(y, m, d-(wd-1), 0, 0, 0, 0, time.UTC)
			case "day", "days":
				t = time.Date(y, m, d, 0, 0, 0, 0, time.UTC)
			case "hour", "hours":
				t = t.Truncate(time.Hour)
			case "minute", "minutes":
				t = t.Truncate(time.Minute)
			case "second", "seconds":
				t = t.Truncate(time.Second)
			case "milliseconds", "millisecond":
				t = t.Truncate(time.Millisecond)
			case "microseconds", "microsecond":
				t = t.Truncate(time.Microsecond)
			default:
				return types.Datum{}, errf(CodeInvalidArgument, "unit %q not recognized for date_trunc", a[0].S)
			}
			return tsDatum(t), nil
		}})
	register(&Builtin{Name: "age", Args: []types.Family{Any, Any}, MinArgs: 1, Ret: types.IntervalFam, Vol: Stable, Category: catTime,
		Doc: "The interval from the second timestamp (today's midnight when omitted) to the first, in years, months, days and time.",
		Fn: func(a []types.Datum) (types.Datum, error) {
			to, _, err := toTime(a[0])
			if err != nil {
				return types.Datum{}, err
			}
			var from time.Time
			if len(a) > 1 {
				if from, _, err = toTime(a[1]); err != nil {
					return types.Datum{}, err
				}
			} else {
				now := time.Now().UTC()
				from = time.Date(now.Year(), now.Month(), now.Day(), 0, 0, 0, 0, time.UTC)
			}
			return types.NewInterval(Between(to, from)), nil
		}})
	register(&Builtin{Name: "to_timestamp", Args: []types.Family{Any, types.String}, MinArgs: 1, Ret: types.Timestamp, Category: catTime,
		Doc: "to_timestamp(seconds) converts a Unix epoch; to_timestamp(text, format) parses with the to_char patterns (YYYY, MM, DD, HH24, MI, SS, ...).",
		Fn: func(a []types.Datum) (types.Datum, error) {
			if len(a) == 1 {
				f, err := numeric(a[0])
				if err != nil {
					return types.Datum{}, err
				}
				sec := math.Floor(f)
				return tsDatum(time.Unix(int64(sec), int64((f-sec)*1e9)).UTC()), nil
			}
			t, err := parseWithPattern(a[0].Text(), a[1].S)
			if err != nil {
				return types.Datum{}, err
			}
			return tsDatum(t), nil
		}})
	register(&Builtin{Name: "to_date", Args: []types.Family{types.String, types.String}, MinArgs: 2, Ret: types.Date, Category: catTime,
		Doc: "Parses text with a to_char date pattern into a date.",
		Fn: func(a []types.Datum) (types.Datum, error) {
			t, err := parseWithPattern(a[0].S, a[1].S)
			if err != nil {
				return types.Datum{}, err
			}
			return dateDatum(t), nil
		}})
	register(&Builtin{Name: "to_char", Args: []types.Family{Any, types.String}, MinArgs: 2, Ret: types.String, Category: catTime,
		Doc: "Formats a timestamp or date (YYYY, YY, MM, Mon, Month, DD, Dy, Day, HH24, HH12, MI, SS, MS, US, AM, TZ, ...) or a number (9, 0, ., ,, FM, S) with a pattern.",
		Fn: func(a []types.Datum) (types.Datum, error) {
			if isNumeric(a[0]) {
				return types.NewString(formatNumber(a[0], a[1].S)), nil
			}
			t, _, err := toTime(a[0])
			if err != nil {
				return types.Datum{}, err
			}
			return types.NewString(formatTime(t, a[1].S)), nil
		}})
	register(&Builtin{Name: "make_date", Args: []types.Family{types.Int, types.Int, types.Int}, MinArgs: 3, Ret: types.Date, Category: catTime,
		Doc: "A date from year, month and day.",
		Fn: func(a []types.Datum) (types.Datum, error) {
			t := time.Date(int(a[0].I), time.Month(a[1].I), int(a[2].I), 0, 0, 0, 0, time.UTC)
			if t.Year() != int(a[0].I) || int(t.Month()) != int(a[1].I) || t.Day() != int(a[2].I) {
				return types.Datum{}, errf(CodeDatetimeField, "date field value out of range: %d-%d-%d", a[0].I, a[1].I, a[2].I)
			}
			return dateDatum(t), nil
		}})
	register(&Builtin{Name: "make_timestamp", Args: []types.Family{types.Int, types.Int, types.Int, types.Int, types.Int, Any}, MinArgs: 6, Ret: types.Timestamp, Category: catTime,
		Doc: "A timestamp from year, month, day, hour, minute and seconds.", Aliases: []string{"make_timestamptz"},
		Fn: func(a []types.Datum) (types.Datum, error) {
			sec, err := numeric(a[5])
			if err != nil {
				return types.Datum{}, err
			}
			whole := math.Floor(sec)
			t := time.Date(int(a[0].I), time.Month(a[1].I), int(a[2].I), int(a[3].I), int(a[4].I), int(whole), int((sec-whole)*1e9), time.UTC)
			if t.Year() != int(a[0].I) || int(t.Month()) != int(a[1].I) || t.Day() != int(a[2].I) || a[3].I > 23 || a[4].I > 59 || sec < 0 || sec >= 60 {
				return types.Datum{}, errf(CodeDatetimeField, "timestamp field value out of range")
			}
			return tsDatum(t), nil
		}})
	register(&Builtin{Name: "make_interval", Args: []types.Family{types.Int, types.Int, types.Int, types.Int, types.Int, types.Int, Any}, MinArgs: 0, Ret: types.IntervalFam, Category: catTime,
		Doc: "An interval from years, months, weeks, days, hours, minutes and seconds.",
		Fn: func(a []types.Datum) (types.Datum, error) {
			get := func(i int) int64 {
				if i < len(a) {
					return a[i].I
				}
				return 0
			}
			iv := Interval{Months: get(0)*12 + get(1), Days: get(2)*7 + get(3), Nanos: get(4)*int64(time.Hour) + get(5)*int64(time.Minute)}
			if len(a) > 6 {
				s, err := numeric(a[6])
				if err != nil {
					return types.Datum{}, err
				}
				iv.Nanos += int64(s * 1e9)
			}
			return types.NewInterval(iv), nil
		}})
	register(&Builtin{Name: "clock_timestamp", Args: nil, Ret: types.Timestamp, Vol: Volatile, Category: catTime,
		Doc: "The wall clock at the moment of the call (now() is the statement's start, the same for every row).",
		Fn:  func([]types.Datum) (types.Datum, error) { return tsDatum(time.Now()), nil }})
	register(&Builtin{Name: "justify_hours", Args: []types.Family{Any}, MinArgs: 1, Ret: types.IntervalFam, Category: catTime,
		Doc: "Rewrites an interval's hours beyond 24 as days.",
		Fn: func(a []types.Datum) (types.Datum, error) {
			iv, err := toInterval(a[0])
			if err != nil {
				return types.Datum{}, err
			}
			return types.NewInterval(iv.JustifyHours()), nil
		}})
	register(&Builtin{Name: "justify_days", Args: []types.Family{Any}, MinArgs: 1, Ret: types.IntervalFam, Category: catTime,
		Doc: "Rewrites an interval's days beyond 30 as months.",
		Fn: func(a []types.Datum) (types.Datum, error) {
			iv, err := toInterval(a[0])
			if err != nil {
				return types.Datum{}, err
			}
			return types.NewInterval(iv.JustifyDays()), nil
		}})
	register(&Builtin{Name: "justify_interval", Args: []types.Family{Any}, MinArgs: 1, Ret: types.IntervalFam, Category: catTime,
		Doc: "Rewrites an interval with justify_days and justify_hours, then aligns the signs of its parts.",
		Fn: func(a []types.Datum) (types.Datum, error) {
			iv, err := toInterval(a[0])
			if err != nil {
				return types.Datum{}, err
			}
			return types.NewInterval(iv.Justify()), nil
		}})
	register(&Builtin{Name: "make_time", Args: []types.Family{types.Int, types.Int, Any}, MinArgs: 3, Ret: types.Time, Category: catTime,
		Doc: "A time from hour, minute and seconds.",
		Fn: func(a []types.Datum) (types.Datum, error) {
			sec, err := numeric(a[2])
			if err != nil {
				return types.Datum{}, err
			}
			if a[0].I < 0 || a[0].I > 23 || a[1].I < 0 || a[1].I > 59 || sec < 0 || sec >= 60 {
				return types.Datum{}, errf(CodeDatetimeField, "time field value out of range: %d:%d:%v", a[0].I, a[1].I, sec)
			}
			return types.NewTime(a[0].I*int64(time.Hour) + a[1].I*int64(time.Minute) + int64(math.Round(sec*1e9))), nil
		}})
	register(&Builtin{Name: "isfinite", Args: []types.Family{Any}, MinArgs: 1, Ret: types.Bool, Category: catTime,
		Doc: "Whether a date or timestamp is finite (always true: datax has no infinities).",
		Fn:  func([]types.Datum) (types.Datum, error) { return types.NewBool(true), nil }})
}

func i64Dec(i int64) types.Datum { return types.NewDecimal(strconv.FormatInt(i, 10)) }

// extractInterval is extract(field FROM interval) (and FROM time, as an
// interval of its clock part): the field of the stored triple, as
// PostgreSQL reports it (days are not folded into months, nor hours
// into days).
func extractInterval(field string, iv Interval) (types.Datum, error) {
	clock := iv.Nanos
	sec := clock % int64(time.Minute)
	switch strings.ToLower(field) {
	case "millennium", "millennia":
		return i64Dec(iv.Months / 12000), nil
	case "century", "centuries":
		return i64Dec(iv.Months / 1200), nil
	case "decade", "decades":
		return i64Dec(iv.Months / 120), nil
	case "year", "years", "y":
		return i64Dec(iv.Months / 12), nil
	case "quarter":
		return i64Dec((iv.Months%12)/3 + 1), nil
	case "month", "months", "mon":
		return i64Dec(iv.Months % 12), nil
	case "day", "days", "d":
		return i64Dec(iv.Days), nil
	case "hour", "hours", "h":
		return i64Dec(clock / int64(time.Hour)), nil
	case "minute", "minutes", "min", "m":
		return i64Dec((clock % int64(time.Hour)) / int64(time.Minute)), nil
	case "second", "seconds", "sec", "s":
		return decimalText(float64(sec) / 1e9), nil
	case "milliseconds", "millisecond", "ms":
		return decimalText(float64(sec) / 1e6), nil
	case "microseconds", "microsecond", "us":
		return i64Dec(sec / 1000), nil
	case "epoch":
		return decimalText(float64(iv.Months)*30*86400 + float64(iv.Days)*86400 + float64(clock)/1e9), nil
	}
	return types.Datum{}, errf(CodeInvalidArgument, "unit %q not recognized for extract over an interval", field)
}

// ---- to_char / to_timestamp patterns ---------------------------------

// timePatterns are the to_char tokens, longest first.
var timePatterns = []string{
	"YYYY", "YYY", "YY", "Y", "IYYY", "IY", "MONTH", "Month", "month", "MON", "Mon", "mon", "MM", "DAY", "Day", "day", "DY", "Dy", "dy",
	"DDD", "DD", "D", "ID", "IW", "WW", "W", "HH24", "HH12", "HH", "MI", "SSSS", "SS", "MS", "US", "AM", "PM", "am", "pm", "A.M.", "P.M.",
	"TZ", "tz", "OF", "Q", "J", "CC", "FF1", "FF2", "FF3", "FF4", "FF5", "FF6",
}

func formatTime(t time.Time, pattern string) string {
	var b strings.Builder
	fm := false // fill mode: no padding
	for i := 0; i < len(pattern); {
		if strings.HasPrefix(pattern[i:], "FM") {
			fm = true
			i += 2
			continue
		}
		if pattern[i] == '"' {
			// A quoted literal.
			j := strings.IndexByte(pattern[i+1:], '"')
			if j < 0 {
				b.WriteString(pattern[i+1:])
				break
			}
			b.WriteString(pattern[i+1 : i+1+j])
			i += j + 2
			continue
		}
		matched := false
		for _, p := range timePatterns {
			if strings.HasPrefix(pattern[i:], p) {
				b.WriteString(timeField(t, p, fm))
				fm = false // FM modifies the next field only
				i += len(p)
				matched = true
				break
			}
		}
		if !matched {
			b.WriteByte(pattern[i])
			i++
		}
	}
	return b.String()
}

func timeField(t time.Time, p string, fm bool) string {
	pad := func(n, width int) string {
		if fm {
			return strconv.Itoa(n)
		}
		return fmt.Sprintf("%0*d", width, n)
	}
	h12 := t.Hour() % 12
	if h12 == 0 {
		h12 = 12
	}
	switch p {
	case "YYYY":
		return pad(t.Year(), 4)
	case "YYY":
		return pad(t.Year()%1000, 3)
	case "YY":
		return pad(t.Year()%100, 2)
	case "Y":
		return strconv.Itoa(t.Year() % 10)
	case "IYYY":
		y, _ := t.ISOWeek()
		return pad(y, 4)
	case "IY":
		y, _ := t.ISOWeek()
		return pad(y%100, 2)
	case "MONTH":
		return padName(strings.ToUpper(t.Month().String()), 9, fm)
	case "Month":
		return padName(t.Month().String(), 9, fm)
	case "month":
		return padName(strings.ToLower(t.Month().String()), 9, fm)
	case "MON":
		return strings.ToUpper(t.Month().String()[:3])
	case "Mon":
		return t.Month().String()[:3]
	case "mon":
		return strings.ToLower(t.Month().String()[:3])
	case "MM":
		return pad(int(t.Month()), 2)
	case "DAY":
		return padName(strings.ToUpper(t.Weekday().String()), 9, fm)
	case "Day":
		return padName(t.Weekday().String(), 9, fm)
	case "day":
		return padName(strings.ToLower(t.Weekday().String()), 9, fm)
	case "DY":
		return strings.ToUpper(t.Weekday().String()[:3])
	case "Dy":
		return t.Weekday().String()[:3]
	case "dy":
		return strings.ToLower(t.Weekday().String()[:3])
	case "DDD":
		return pad(t.YearDay(), 3)
	case "DD":
		return pad(t.Day(), 2)
	case "D":
		return strconv.Itoa(int(t.Weekday()) + 1)
	case "ID":
		wd := int(t.Weekday())
		if wd == 0 {
			wd = 7
		}
		return strconv.Itoa(wd)
	case "IW":
		_, w := t.ISOWeek()
		return pad(w, 2)
	case "WW":
		return pad((t.YearDay()-1)/7+1, 2)
	case "W":
		return strconv.Itoa((t.Day()-1)/7 + 1)
	case "HH24":
		return pad(t.Hour(), 2)
	case "HH12", "HH":
		return pad(h12, 2)
	case "MI":
		return pad(t.Minute(), 2)
	case "SSSS":
		return strconv.Itoa(t.Hour()*3600 + t.Minute()*60 + t.Second())
	case "SS":
		return pad(t.Second(), 2)
	case "MS":
		return fmt.Sprintf("%03d", t.Nanosecond()/1e6)
	case "US":
		return fmt.Sprintf("%06d", t.Nanosecond()/1e3)
	case "FF1", "FF2", "FF3", "FF4", "FF5", "FF6":
		n := int(p[2] - '0')
		return fmt.Sprintf("%09d", t.Nanosecond())[:n]
	case "AM", "PM":
		if t.Hour() < 12 {
			return "AM"
		}
		return "PM"
	case "am", "pm":
		if t.Hour() < 12 {
			return "am"
		}
		return "pm"
	case "A.M.", "P.M.":
		if t.Hour() < 12 {
			return "A.M."
		}
		return "P.M."
	case "TZ":
		return "UTC"
	case "tz":
		return "utc"
	case "OF":
		return "+00"
	case "Q":
		return strconv.Itoa((int(t.Month())-1)/3 + 1)
	case "J":
		return strconv.Itoa(int(t.Unix()/86400) + 2440588)
	case "CC":
		return pad((t.Year()-1)/100+1, 2)
	}
	return p
}

func padName(s string, width int, fm bool) string {
	if fm || len(s) >= width {
		return s
	}
	return s + strings.Repeat(" ", width-len(s))
}

// parseWithPattern reads text against a to_char pattern (the numeric
// fields and month names; separators must match by position).
func parseWithPattern(text, pattern string) (time.Time, error) {
	year, month, day, hour, minute := 1, 1, 1, 0, 0
	var sec float64
	pm, has12 := false, false
	ti := 0
	fail := func() (time.Time, error) {
		return time.Time{}, errf(CodeInvalidDatetime, "invalid value %q for pattern %q", text, pattern)
	}
	readInt := func(max int) (int, bool) {
		start := ti
		for ti < len(text) && ti-start < max && text[ti] >= '0' && text[ti] <= '9' {
			ti++
		}
		if ti == start {
			return 0, false
		}
		n, _ := strconv.Atoi(text[start:ti])
		return n, true
	}
	for i := 0; i < len(pattern); {
		if strings.HasPrefix(pattern[i:], "FM") {
			i += 2
			continue
		}
		matched := ""
		for _, p := range timePatterns {
			if strings.HasPrefix(pattern[i:], p) {
				matched = p
				break
			}
		}
		if matched == "" {
			// A separator: skip it in the text too.
			if ti < len(text) {
				ti++
			}
			i++
			continue
		}
		i += len(matched)
		var ok bool
		switch matched {
		case "YYYY":
			year, ok = readInt(4)
		case "YY":
			year, ok = readInt(2)
			year += 2000
		case "MM":
			month, ok = readInt(2)
		case "DD":
			day, ok = readInt(2)
		case "HH24", "HH12", "HH":
			hour, ok = readInt(2)
			has12 = matched != "HH24"
		case "MI":
			minute, ok = readInt(2)
		case "SS":
			var s int
			s, ok = readInt(2)
			sec = float64(s)
		case "MS":
			var ms int
			ms, ok = readInt(3)
			sec += float64(ms) / 1000
		case "US":
			var us int
			us, ok = readInt(6)
			sec += float64(us) / 1e6
		case "AM", "PM", "am", "pm":
			if ti+2 <= len(text) {
				pm = strings.EqualFold(text[ti:ti+2], "pm")
				ti += 2
				ok = true
			}
		case "MON", "Mon", "mon", "MONTH", "Month", "month":
			for m := time.January; m <= time.December; m++ {
				name := m.String()
				if strings.HasPrefix(strings.ToLower(text[ti:]), strings.ToLower(name)) {
					month, ti, ok = int(m), ti+len(name), true
					break
				}
				if strings.HasPrefix(strings.ToLower(text[ti:]), strings.ToLower(name[:3])) {
					month, ti, ok = int(m), ti+3, true
					break
				}
			}
		case "DDD":
			var doy int
			doy, ok = readInt(3)
			month, day = 1, doy
		default:
			// Fields that do not set a value (day names, TZ, Q): skip a
			// word.
			for ti < len(text) && text[ti] != ' ' && text[ti] != '-' && text[ti] != ':' {
				ti++
			}
			ok = true
		}
		if !ok {
			return fail()
		}
	}
	if has12 && pm && hour < 12 {
		hour += 12
	}
	whole := math.Floor(sec)
	t := time.Date(year, time.Month(month), day, hour, minute, int(whole), int((sec-whole)*1e9), time.UTC)
	if month < 1 || month > 12 || day < 1 || day > 31 || hour > 24 || minute > 59 || sec >= 61 {
		return fail()
	}
	return t, nil
}

// formatNumber implements to_char for numbers: 9 (digit or blank), 0
// (digit), the decimal point, comma grouping, S / MI for the sign, FM
// to drop padding, PR/PL ignored.
func formatNumber(d types.Datum, pattern string) string {
	f, _ := numeric(d)
	fm := strings.HasPrefix(pattern, "FM")
	pattern = strings.TrimPrefix(pattern, "FM")
	neg := f < 0
	f = math.Abs(f)
	// Digits requested on each side of the point.
	intPart, fracPart := pattern, ""
	if i := strings.IndexAny(pattern, ".D"); i >= 0 {
		intPart, fracPart = pattern[:i], pattern[i+1:]
	}
	fracDigits := strings.Count(fracPart, "9") + strings.Count(fracPart, "0")
	text := strconv.FormatFloat(f, 'f', fracDigits, 64)
	ip, fp := text, ""
	if i := strings.IndexByte(text, '.'); i >= 0 {
		ip, fp = text[:i], text[i+1:]
	}
	// Integer part right-aligned into the 9/0 slots with grouping.
	slots := 0
	for _, c := range intPart {
		if c == '9' || c == '0' {
			slots++
		}
	}
	if len(ip) > slots && slots > 0 {
		return strings.Repeat("#", len(pattern))
	}
	var out strings.Builder
	digits := strings.Repeat(" ", slots-len(ip)) + ip
	di := 0
	for _, c := range intPart {
		switch c {
		case '9':
			out.WriteByte(digits[di])
			di++
		case '0':
			if digits[di] == ' ' {
				out.WriteByte('0')
			} else {
				out.WriteByte(digits[di])
			}
			di++
		case ',', 'G':
			if di > 0 && digits[di-1] != ' ' {
				out.WriteByte(',')
			} else if !fm {
				out.WriteByte(' ')
			}
		case 'S':
			if neg {
				out.WriteByte('-')
			} else {
				out.WriteByte('+')
			}
		}
	}
	s := out.String()
	switch {
	case strings.Contains(intPart, "MI"):
		if neg {
			s += "-"
		} else {
			s += " "
		}
	case strings.Contains(intPart, "S"):
	case fm:
		if neg {
			s = "-" + strings.TrimLeft(s, " ")
		}
	default:
		// A sign position leads the number: blank when positive.
		if neg {
			s = "-" + s
		} else {
			s = " " + s
		}
	}
	if fracDigits > 0 {
		var fb strings.Builder
		fi := 0
		for _, c := range fracPart {
			if (c == '9' || c == '0') && fi < len(fp) {
				fb.WriteByte(fp[fi])
				fi++
			}
		}
		frac := fb.String()
		if fm {
			frac = strings.TrimRight(frac, "0")
			if frac == "" {
				return strings.TrimSpace(s)
			}
		}
		s += "." + frac
	}
	if fm {
		return strings.TrimSpace(s)
	}
	return s
}
