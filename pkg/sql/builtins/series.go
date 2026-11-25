package builtins

import (
	"math"
	"strings"
	"time"

	"github.com/sthorne/datax/pkg/sql/types"
	"github.com/sthorne/datax/pkg/util/decimal"
)

const catSets = "Set-returning functions"

// seriesFamily is what generate_series yields for its argument
// families: timestamps when a bound is a timestamp or date or the
// step an interval, numeric when a bound or the step is, else int8.
func seriesFamily(args []types.Family) types.Family {
	dec := false
	for i, f := range args {
		if i < 2 && (f == types.Timestamp || f == types.Date) || i == 2 && f == types.IntervalFam {
			return types.Timestamp
		}
		if f == types.Decimal || f == types.Float {
			dec = true
		}
	}
	if dec {
		return types.Decimal
	}
	return types.Int
}

func datumFamilies(a []types.Datum) []types.Family {
	fs := make([]types.Family, len(a))
	for i, d := range a {
		fs[i] = d.Fam
	}
	return fs
}

func seriesZeroStep() error { return errf(CodeInvalidArgument, "step size cannot equal zero") }

// intSeries counts from start to stop by step, stopping before the
// next value would overflow.
func intSeries(a []types.Datum) (RowIter, error) {
	var vals [3]int64
	vals[2] = 1
	for i, d := range a {
		v, err := toInt(d)
		if err != nil {
			return nil, err
		}
		vals[i] = v.I
	}
	cur, stop, step := vals[0], vals[1], vals[2]
	if step == 0 {
		return nil, seriesZeroStep()
	}
	done := step > 0 && cur > stop || step < 0 && cur < stop
	return func() ([]types.Datum, bool, error) {
		if done {
			return nil, false, nil
		}
		v := cur
		if step > 0 && cur > math.MaxInt64-step || step < 0 && cur < math.MinInt64-step {
			done = true
		} else {
			cur += step
			done = step > 0 && cur > stop || step < 0 && cur < stop
		}
		return []types.Datum{types.NewInt(v)}, true, nil
	}, nil
}

// decScale is the number of fractional digits in canonical decimal
// text.
func decScale(s string) int32 {
	if i := strings.IndexByte(s, '.'); i >= 0 {
		return int32(len(s) - i - 1)
	}
	return 0
}

func decimalSeries(a []types.Datum) (RowIter, error) {
	var vals [3]decimal.Dec
	vals[2] = decimal.FromInt(1)
	var scale int32
	for i, d := range a {
		v, err := d.Coerce(types.Decimal)
		if err != nil {
			return nil, errf(CodeInvalidText, "invalid input syntax for type numeric: %q", d.Text())
		}
		if f, ok := types.DecimalSpecial(v); ok {
			what := [3]string{"start value", "stop value", "step size"}[i]
			if math.IsNaN(f) {
				return nil, errf(CodeInvalidArgument, "%s cannot be NaN", what)
			}
			return nil, errf(CodeInvalidArgument, "%s cannot be infinity", what)
		}
		if vals[i], err = v.DecimalVal(); err != nil {
			return nil, errf(CodeInvalidText, "invalid input syntax for type numeric: %q", d.Text())
		}
		if i != 1 {
			if s := decScale(v.S); s > scale {
				scale = s
			}
		}
	}
	cur, stop, step := vals[0], vals[1], vals[2]
	if step.Sign() == 0 {
		return nil, seriesZeroStep()
	}
	past := func(v decimal.Dec) bool {
		c := decimal.Cmp(v, stop)
		return step.Sign() > 0 && c > 0 || step.Sign() < 0 && c < 0
	}
	return func() ([]types.Datum, bool, error) {
		if past(cur) {
			return nil, false, nil
		}
		d := types.NewDecimal(cur.String())
		d.Dscale = scale
		cur = decimal.Add(cur, step)
		return []types.Datum{d}, true, nil
	}, nil
}

// timestampSeries steps a timestamp by an interval, each value the
// previous one plus the step (calendar arithmetic: a month after
// January 31 is February's last day, and the next one March 29).
func timestampSeries(a []types.Datum, loc *time.Location) (RowIter, error) {
	if len(a) < 3 {
		return nil, errf(CodeUndefined, "function generate_series(%s) does not exist: a timestamp series needs an interval step", familiesText(a))
	}
	var bounds [2]types.Datum
	for i := 0; i < 2; i++ {
		d := a[i]
		if d.Fam == types.String && loc != nil {
			d.Loc = loc
		}
		if d.Fam == types.Date && loc != nil {
			n, err := types.DateMidnight(d.I, loc)
			if err != nil {
				return nil, timestampErr(err, d.Text())
			}
			d = types.NewTimestamp(n)
		}
		if d.Fam != types.Timestamp {
			v, err := d.Coerce(types.Timestamp)
			if err != nil {
				return nil, timestampErr(err, d.Text())
			}
			d = v
		}
		bounds[i] = d
	}
	step, err := toInterval(a[2])
	if err != nil {
		return nil, err
	}
	sign := step.CmpValue()
	if sign == 0 {
		return nil, seriesZeroStep()
	}
	noTZ := a[0].NoTZ
	cur, stop := tsTime(bounds[0]), tsTime(bounds[1])
	return func() ([]types.Datum, bool, error) {
		if sign > 0 && cur.After(stop) || sign < 0 && cur.Before(stop) {
			return nil, false, nil
		}
		d, err := tsDatum(cur)
		if err != nil {
			return nil, false, err
		}
		d.NoTZ = noTZ
		next := step.AddTo(cur, 1)
		if loc != nil && !noTZ {
			next = AddIntervalIn(cur, step, 1, loc)
		}
		if !next.After(cur) && sign > 0 || !next.Before(cur) && sign < 0 {
			// A step that does not move (one month plus minus thirty
			// days, say) would never reach the bound.
			cur = stop.Add(1)
			if sign < 0 {
				cur = stop.Add(-1)
			}
		} else {
			cur = next
		}
		return []types.Datum{d}, true, nil
	}, nil
}

func init() {
	register(&Builtin{Name: "generate_series", Args: []types.Family{Any, Any, Any}, MinArgs: 2, Ret: Any, RetOf: seriesFamily, Category: catSets,
		Columns: []string{""},
		Doc:     "The values from start to stop (inclusive) by step (1 by default): integers, numerics, or timestamps stepped by an interval (generate_series(start, stop, interval '1 day')). A zero step is an error (22023).",
		Rows:    func(a []types.Datum) (RowIter, error) { return series(a, nil) },
		ZRows:   series})
}

// series is generate_series under the session's zone (nil is UTC), in
// which a timestamp with time zone steps by days and months.
func series(a []types.Datum, loc *time.Location) (RowIter, error) {
	switch seriesFamily(datumFamilies(a)) {
	case types.Timestamp:
		return timestampSeries(a, loc)
	case types.Decimal:
		return decimalSeries(a)
	}
	return intSeries(a)
}
