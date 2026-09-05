package types

import (
	"fmt"
	"math"
	"regexp"
	"strconv"
	"strings"
	"time"
)

// Interval is a PostgreSQL-style interval: months and days are kept
// apart from the clock part because their length varies (adding one
// month to January 31 lands on February 28). It is the value of an
// INTERVAL datum (Datum.Mo, Datum.Dy, Datum.I).
type Interval struct {
	Months int64
	Days   int64
	Nanos  int64
}

// NewInterval makes an INTERVAL datum.
func NewInterval(iv Interval) Datum {
	return Datum{Fam: IntervalFam, Mo: iv.Months, Dy: iv.Days, I: iv.Nanos}
}

// IntervalVal is the interval an INTERVAL datum holds.
func (d Datum) IntervalVal() Interval { return Interval{Months: d.Mo, Days: d.Dy, Nanos: d.I} }

// NewTime makes a TIME datum from nanoseconds since midnight.
func NewTime(nanos int64) Datum { return Datum{Fam: Time, I: nanos} }

// NanosPerDay is the length of a day in nanoseconds.
const NanosPerDay = int64(24 * time.Hour)

// CmpValue is the quantity PostgreSQL orders and compares intervals by:
// every month as 30 days, every day as 24 hours.
func (iv Interval) CmpValue() int64 {
	return (iv.Months*30+iv.Days)*NanosPerDay + iv.Nanos
}

// Neg is -iv.
func (iv Interval) Neg() Interval { return Interval{-iv.Months, -iv.Days, -iv.Nanos} }

// Add is iv + o, field by field.
func (iv Interval) Add(o Interval) Interval {
	return Interval{iv.Months + o.Months, iv.Days + o.Days, iv.Nanos + o.Nanos}
}

// Scale is iv × f as PostgreSQL computes it: the fractional months
// spill into days (30 per month) and the fractional days into the
// clock part.
func (iv Interval) Scale(f float64) Interval {
	months := float64(iv.Months) * f
	wholeMonths := math.Trunc(months)
	days := float64(iv.Days)*f + (months-wholeMonths)*30
	wholeDays := math.Trunc(days)
	nanos := float64(iv.Nanos)*f + (days-wholeDays)*float64(NanosPerDay)
	// Microsecond precision, as PostgreSQL computes it.
	return Interval{int64(wholeMonths), int64(wholeDays), int64(math.Round(nanos/1000)) * 1000}
}

// JustifyHours folds every 24 hours of the clock part into days.
func (iv Interval) JustifyHours() Interval {
	days := iv.Nanos / NanosPerDay
	iv.Days += days
	iv.Nanos -= days * NanosPerDay
	return iv
}

// JustifyDays folds every 30 days into months.
func (iv Interval) JustifyDays() Interval {
	months := iv.Days / 30
	iv.Months += months
	iv.Days -= months * 30
	return iv
}

// Justify applies JustifyHours then JustifyDays, then aligns the signs
// (as justify_interval does).
func (iv Interval) Justify() Interval {
	iv = iv.JustifyHours().JustifyDays()
	if iv.Months > 0 && (iv.Days < 0 || (iv.Days == 0 && iv.Nanos < 0)) {
		iv.Months--
		iv.Days += 30
	} else if iv.Months < 0 && (iv.Days > 0 || (iv.Days == 0 && iv.Nanos > 0)) {
		iv.Months++
		iv.Days -= 30
	}
	if iv.Days > 0 && iv.Nanos < 0 {
		iv.Days--
		iv.Nanos += NanosPerDay
	} else if iv.Days < 0 && iv.Nanos > 0 {
		iv.Days++
		iv.Nanos -= NanosPerDay
	}
	return iv
}

// AddTo shifts a timestamp by sign × the interval: months on the
// calendar with the day clamped to the target month's length (January
// 31 + 1 month is February 29, not March 2), then days, then the clock
// part.
func (iv Interval) AddTo(t time.Time, sign int64) time.Time {
	if m := sign * iv.Months; m != 0 {
		y, mo, d := t.Date()
		total := int64(y)*12 + int64(mo) - 1 + m
		ny, nm := int(total/12), time.Month(total%12+1)
		if last := time.Date(ny, nm+1, 0, 0, 0, 0, 0, time.UTC).Day(); d > last {
			d = last
		}
		t = time.Date(ny, nm, d, t.Hour(), t.Minute(), t.Second(), t.Nanosecond(), time.UTC)
	}
	return t.AddDate(0, 0, int(sign*iv.Days)).Add(time.Duration(sign * iv.Nanos))
}

// String renders the interval as PostgreSQL does (IntervalStyle
// postgres): "1 year 2 mons 3 days 04:05:06", a positive part after a
// negative one carrying an explicit sign ("-1 days +02:00:00").
func (iv Interval) String() string {
	var parts []string
	before := false
	part := func(n int64, unit string) {
		if n == 0 {
			return
		}
		sign := ""
		if before && n > 0 {
			sign = "+"
		}
		parts = append(parts, sign+plural(n, unit))
		before = n < 0
	}
	part(iv.Months/12, "year")
	part(iv.Months%12, "mon")
	part(iv.Days, "day")
	if iv.Nanos != 0 || len(parts) == 0 {
		clock := FormatClock(iv.Nanos)
		if before && iv.Nanos > 0 {
			clock = "+" + clock
		}
		parts = append(parts, clock)
	}
	return strings.Join(parts, " ")
}

// FormatClock renders signed nanoseconds as [-]HH:MM:SS[.ffffff]
// (a TIME value, or an interval's clock part), fractional digits
// trimmed to the microsecond.
func FormatClock(n int64) string {
	sign := ""
	if n < 0 {
		sign, n = "-", -n
	}
	h := n / int64(time.Hour)
	n -= h * int64(time.Hour)
	mi := n / int64(time.Minute)
	n -= mi * int64(time.Minute)
	sec := n / int64(time.Second)
	n -= sec * int64(time.Second)
	clock := fmt.Sprintf("%s%02d:%02d:%02d", sign, h, mi, sec)
	if n != 0 {
		clock += strings.TrimRight(fmt.Sprintf(".%09d", n), "0")
	}
	return clock
}

func plural(n int64, unit string) string {
	if n == 1 {
		return fmt.Sprintf("%d %s", n, unit)
	}
	return fmt.Sprintf("%d %ss", n, unit)
}

var (
	intervalUnitRE = regexp.MustCompile(`(?i)^([+-]?\d+(?:\.\d+)?)\s*([a-z]+)$`)
	compactUnitsRE = regexp.MustCompile(`(?i)^(?:[+-]?\d+(?:\.\d+)?[a-z]+){2,}$`)
	compactPairRE  = regexp.MustCompile(`(?i)([+-]?\d+(?:\.\d+)?)([a-z]+)`)
	yearsMonthsRE  = regexp.MustCompile(`^[+-]?\d+-\d+$`)
)

// ParseInterval reads PostgreSQL's verbose interval syntax: "1 day",
// "2 hours 30 minutes", "1 year 2 months", "3 weeks", "-1 day",
// "1 day 02:03:04", "02:03:04", "1.5 hours", "1-2" (years-months),
// "@ 1 day ago", the ISO 8601 form "P1Y2M3DT4H5M6S", and the SQL
// standard forms "1-2 3 04:05:06".
func ParseInterval(s string) (Interval, error) {
	var iv Interval
	text := strings.TrimSpace(s)
	text = strings.TrimSpace(strings.TrimPrefix(text, "@"))
	if text == "" {
		return iv, fmt.Errorf("invalid input syntax for type interval: %q", s)
	}
	if strings.HasPrefix(strings.ToUpper(text), "P") {
		iv, err := parseISOInterval(text)
		if err != nil {
			return iv, fmt.Errorf("invalid input syntax for type interval: %q", s)
		}
		return iv, nil
	}
	// "2h30m": compact unit runs split into their pairs.
	var fields []string
	for _, f := range strings.Fields(text) {
		if compactUnitsRE.MatchString(f) {
			for _, m := range compactPairRE.FindAllStringSubmatch(f, -1) {
				fields = append(fields, m[1]+m[2])
			}
			continue
		}
		fields = append(fields, f)
	}
	yearsMonths := false
	for i := 0; i < len(fields); i++ {
		f := fields[i]
		if strings.EqualFold(f, "ago") && i == len(fields)-1 {
			iv = iv.Neg()
			break
		}
		if strings.Contains(f, ":") {
			n, err := parseClock(f)
			if err != nil {
				return iv, fmt.Errorf("invalid input syntax for type interval: %q", s)
			}
			iv.Nanos += n
			continue
		}
		// "1-2": years-months (SQL standard).
		if yearsMonthsRE.MatchString(f) {
			ym := strings.SplitN(strings.TrimPrefix(strings.TrimPrefix(f, "-"), "+"), "-", 2)
			y, _ := strconv.ParseInt(ym[0], 10, 64)
			m, _ := strconv.ParseInt(ym[1], 10, 64)
			months := y*12 + m
			if strings.HasPrefix(f, "-") {
				months = -months
			}
			iv.Months += months
			yearsMonths = true
			continue
		}
		// "2 hours" as two fields, or "2hours" / "2h" as one; a bare
		// number is days after a years-months field (SQL standard) and
		// seconds otherwise.
		num, unit := f, ""
		if m := intervalUnitRE.FindStringSubmatch(f); m != nil {
			num, unit = m[1], m[2]
		} else if i+1 < len(fields) && !strings.Contains(fields[i+1], ":") {
			if _, err := strconv.ParseFloat(fields[i+1], 64); err != nil && !yearsMonthsRE.MatchString(fields[i+1]) {
				unit = fields[i+1]
				i++
			}
		}
		if unit == "" {
			if _, err := strconv.ParseFloat(num, 64); err != nil {
				return iv, fmt.Errorf("invalid input syntax for type interval: %q", s)
			}
			unit = "second"
			if yearsMonths || (i+1 < len(fields) && strings.Contains(fields[i+1], ":")) {
				unit, yearsMonths = "day", false
			}
		}
		q, err := strconv.ParseFloat(num, 64)
		if err != nil {
			return iv, fmt.Errorf("invalid input syntax for type interval: %q", s)
		}
		if err := iv.add(q, unit); err != nil {
			return iv, fmt.Errorf("invalid input syntax for type interval: %q (%v)", s, err)
		}
	}
	return iv, nil
}

func parseClock(f string) (int64, error) {
	neg := strings.HasPrefix(f, "-")
	f = strings.TrimPrefix(strings.TrimPrefix(f, "-"), "+")
	parts := strings.Split(f, ":")
	if len(parts) < 2 || len(parts) > 3 {
		return 0, fmt.Errorf("bad clock")
	}
	var n int64
	units := []int64{int64(time.Hour), int64(time.Minute), int64(time.Second)}
	for i, p := range parts {
		v, err := strconv.ParseFloat(p, 64)
		if err != nil {
			return 0, err
		}
		n += int64(math.Round(v * float64(units[i])))
	}
	if neg {
		n = -n
	}
	return n, nil
}

func (iv *Interval) add(q float64, unit string) error {
	u := strings.ToLower(strings.TrimSuffix(unit, "s"))
	switch u {
	case "millennium", "millennia":
		iv.Months += int64(q * 12000)
	case "century", "centurie", "c":
		iv.Months += int64(q * 1200)
	case "decade", "dec":
		iv.Months += int64(q * 120)
	case "year", "yr", "y":
		iv.Months += int64(q * 12)
	case "month", "mon":
		iv.Months += int64(q)
		iv.Days += int64((q - math.Trunc(q)) * 30)
	case "week", "w":
		whole := math.Trunc(q)
		iv.Days += int64(whole) * 7
		iv.Nanos += int64((q - whole) * 7 * float64(NanosPerDay))
	case "day", "d":
		whole := math.Trunc(q)
		iv.Days += int64(whole)
		iv.Nanos += int64((q - whole) * float64(NanosPerDay))
	case "hour", "hr", "h":
		iv.Nanos += int64(q * float64(time.Hour))
	case "minute", "min", "m":
		iv.Nanos += int64(q * float64(time.Minute))
	case "second", "sec", "":
		iv.Nanos += int64(q * float64(time.Second))
	case "millisecond", "msec", "ms":
		iv.Nanos += int64(q * float64(time.Millisecond))
	case "microsecond", "usec", "us":
		iv.Nanos += int64(q * float64(time.Microsecond))
	default:
		return fmt.Errorf("unknown unit %q", unit)
	}
	return nil
}

var isoRE = regexp.MustCompile(`(?i)^P(?:(-?\d+)Y)?(?:(-?\d+)M)?(?:(-?\d+)W)?(?:(-?\d+)D)?(?:T(?:(-?\d+)H)?(?:(-?\d+)M)?(?:(-?\d+(?:\.\d+)?)S)?)?$`)

func parseISOInterval(s string) (Interval, error) {
	m := isoRE.FindStringSubmatch(s)
	if m == nil || len(s) < 2 {
		return Interval{}, fmt.Errorf("bad ISO 8601 interval")
	}
	var iv Interval
	get := func(i int) float64 {
		if m[i] == "" {
			return 0
		}
		v, _ := strconv.ParseFloat(m[i], 64)
		return v
	}
	iv.Months = int64(get(1))*12 + int64(get(2))
	iv.Days = int64(get(3))*7 + int64(get(4))
	iv.Nanos = int64(get(5)*float64(time.Hour) + get(6)*float64(time.Minute) + get(7)*float64(time.Second))
	return iv, nil
}

// ParseTime parses a TIME input — HH:MM[:SS[.fraction]] with an
// optional AM / PM and an ignored offset, or a full timestamp text
// whose clock part is taken — to nanoseconds since midnight (24:00:00
// is allowed, as in PostgreSQL).
func ParseTime(s string) (int64, error) {
	text := strings.TrimSpace(s)
	if text == "" {
		return 0, fmt.Errorf("invalid input syntax for type time: %q", s)
	}
	// A timestamp text: keep its clock.
	if i := strings.IndexAny(text, " T"); i > 0 && strings.Count(text[:i], "-") == 2 {
		if n, err := ParseTimestampNoTZ(text); err == nil {
			t := time.Unix(0, n).UTC()
			return int64(t.Hour())*int64(time.Hour) + int64(t.Minute())*int64(time.Minute) + int64(t.Second())*int64(time.Second) + int64(t.Nanosecond()), nil
		}
	}
	upper := strings.ToUpper(text)
	pm, ampm := false, false
	for _, suffix := range []string{" AM", "AM", " PM", "PM"} {
		if strings.HasSuffix(upper, suffix) {
			ampm, pm = true, strings.HasSuffix(suffix, "PM")
			text = strings.TrimSpace(text[:len(text)-len(suffix)])
			break
		}
	}
	// An offset (timetz input) is ignored.
	if i := strings.LastIndexAny(text, "+-"); i > 0 {
		text = strings.TrimSpace(text[:i])
	} else if strings.HasSuffix(strings.ToUpper(text), "Z") {
		text = text[:len(text)-1]
	}
	parts := strings.Split(text, ":")
	if len(parts) < 1 || len(parts) > 3 {
		return 0, fmt.Errorf("invalid input syntax for type time: %q", s)
	}
	h, herr := strconv.ParseInt(parts[0], 10, 64)
	var m int64
	var merr error
	if len(parts) > 1 {
		m, merr = strconv.ParseInt(parts[1], 10, 64)
	}
	if herr != nil || merr != nil || h < 0 || m < 0 || m > 59 {
		return 0, fmt.Errorf("invalid input syntax for type time: %q", s)
	}
	var secNanos int64
	if len(parts) == 3 {
		sec, err := strconv.ParseFloat(parts[2], 64)
		if err != nil || sec < 0 || sec >= 61 {
			return 0, fmt.Errorf("invalid input syntax for type time: %q", s)
		}
		secNanos = int64(math.Round(sec * 1e9))
	}
	if ampm {
		if h < 1 || h > 12 {
			return 0, fmt.Errorf("invalid input syntax for type time: %q", s)
		}
		h %= 12
		if pm {
			h += 12
		}
	}
	n := h*int64(time.Hour) + m*int64(time.Minute) + secNanos
	if n > NanosPerDay {
		return 0, fmt.Errorf("time out of range: %q", s)
	}
	return n, nil
}
