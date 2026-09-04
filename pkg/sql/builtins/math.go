package builtins

import (
	"math"
	"math/rand/v2"

	"github.com/sthorne/datax/pkg/sql/types"
	"github.com/sthorne/datax/pkg/util/decimal"
)

const catMath = "Math"

func f64(f float64) types.Datum { return types.NewFloat(f) }

// numeric coerces a numeric datum to float64 for the transcendental
// functions.
func numeric(d types.Datum) (float64, error) {
	switch d.Fam {
	case types.Int:
		return float64(d.I), nil
	case types.Float:
		return d.F, nil
	case types.Decimal:
		v, err := d.DecimalVal()
		if err != nil {
			return 0, errf(CodeInvalidText, "%v", err)
		}
		return v.Float64(), nil
	case types.String:
		c, err := d.Coerce(types.Float)
		if err != nil {
			return 0, errf(CodeInvalidText, "invalid input syntax for type numeric: %q", d.S)
		}
		return c.F, nil
	}
	return 0, errf(CodeUndefined, "function does not exist for argument type %s", d.Fam)
}

// isNumeric reports whether the datum is one of the numeric families.
func isNumeric(d types.Datum) bool {
	return d.Fam == types.Int || d.Fam == types.Float || d.Fam == types.Decimal
}

// unaryFloat registers a float function of one numeric argument.
func unaryFloat(name, doc string, fn func(float64) (float64, error), aliases ...string) {
	register(&Builtin{Name: name, Args: []types.Family{Any}, MinArgs: 1, Ret: types.Float, Category: catMath, Doc: doc, Aliases: aliases,
		Fn: func(a []types.Datum) (types.Datum, error) {
			x, err := numeric(a[0])
			if err != nil {
				return types.Datum{}, err
			}
			y, err := fn(x)
			if err != nil {
				return types.Datum{}, err
			}
			if math.IsInf(y, 0) || math.IsNaN(y) {
				return types.Datum{}, errf(CodeOutOfRange, "%s: result is out of range", name)
			}
			return f64(y), nil
		}})
}

func init() {
	register(&Builtin{Name: "abs", Args: []types.Family{Any}, MinArgs: 1, Ret: Any, SameAsArg: 0, Category: catMath,
		Doc: "The absolute value, in the argument's type.",
		Fn: func(a []types.Datum) (types.Datum, error) {
			d := a[0]
			switch d.Fam {
			case types.Int:
				if d.I == math.MinInt64 {
					return types.Datum{}, errf(CodeOutOfRange, "integer out of range")
				}
				if d.I < 0 {
					return i64(-d.I), nil
				}
				return d, nil
			case types.Float:
				return f64(math.Abs(d.F)), nil
			case types.Decimal:
				v, err := d.DecimalVal()
				if err != nil {
					return types.Datum{}, errf(CodeInvalidText, "%v", err)
				}
				if v.Sign() < 0 {
					return types.NewDecimal(decimal.Neg(v).String()), nil
				}
				return d, nil
			}
			return types.Datum{}, errf(CodeUndefined, "abs() requires a numeric argument, got %s", d.Fam)
		}})
	register(&Builtin{Name: "sign", Args: []types.Family{Any}, MinArgs: 1, Ret: types.Int, Category: catMath,
		Doc: "-1, 0 or 1 by the argument's sign.",
		Fn: func(a []types.Datum) (types.Datum, error) {
			x, err := numeric(a[0])
			if err != nil {
				return types.Datum{}, err
			}
			switch {
			case x < 0:
				return i64(-1), nil
			case x > 0:
				return i64(1), nil
			}
			return i64(0), nil
		}})
	register(&Builtin{Name: "round", Args: []types.Family{Any, types.Int}, MinArgs: 1, Ret: Any, SameAsArg: 0, Category: catMath,
		Doc: "Rounds to the nearest integer, or to the given number of decimal places (halves away from zero); a decimal stays exact.",
		Fn:  func(a []types.Datum) (types.Datum, error) { return roundTo(a, false) }})
	register(&Builtin{Name: "trunc", Args: []types.Family{Any, types.Int}, MinArgs: 1, Ret: Any, SameAsArg: 0, Category: catMath,
		Doc: "Truncates toward zero, to an integer or to the given number of decimal places.",
		Fn:  func(a []types.Datum) (types.Datum, error) { return roundTo(a, true) }})
	register(&Builtin{Name: "floor", Args: []types.Family{Any}, MinArgs: 1, Ret: Any, SameAsArg: 0, Category: catMath,
		Doc: "The largest integer not greater than the argument, in its type.",
		Fn:  func(a []types.Datum) (types.Datum, error) { return floorCeil(a[0], math.Floor) }})
	register(&Builtin{Name: "ceil", Args: []types.Family{Any}, MinArgs: 1, Ret: Any, SameAsArg: 0, Category: catMath,
		Doc: "The smallest integer not less than the argument, in its type.", Aliases: []string{"ceiling"},
		Fn: func(a []types.Datum) (types.Datum, error) { return floorCeil(a[0], math.Ceil) }})
	register(&Builtin{Name: "mod", Args: []types.Family{Any, Any}, MinArgs: 2, Ret: Any, SameAsArg: 0, Category: catMath,
		Doc: "The remainder of the division (the sign of the dividend), as the % operator.",
		Fn:  func(a []types.Datum) (types.Datum, error) { return Mod(a[0], a[1]) }})
	register(&Builtin{Name: "div", Args: []types.Family{Any, Any}, MinArgs: 2, Ret: types.Int, Category: catMath,
		Doc: "The integer quotient, truncated toward zero.",
		Fn: func(a []types.Datum) (types.Datum, error) {
			if a[0].Fam == types.Int && a[1].Fam == types.Int {
				if a[1].I == 0 {
					return types.Datum{}, errf(CodeDivisionByZero, "division by zero")
				}
				if a[0].I == math.MinInt64 && a[1].I == -1 {
					return types.Datum{}, errf(CodeOutOfRange, "integer out of range")
				}
				return i64(a[0].I / a[1].I), nil
			}
			x, err := numeric(a[0])
			if err != nil {
				return types.Datum{}, err
			}
			y, err := numeric(a[1])
			if err != nil {
				return types.Datum{}, err
			}
			if y == 0 {
				return types.Datum{}, errf(CodeDivisionByZero, "division by zero")
			}
			return i64(int64(math.Trunc(x / y))), nil
		}})
	register(&Builtin{Name: "power", Args: []types.Family{Any, Any}, MinArgs: 2, Ret: types.Float, Category: catMath,
		Doc: "The first argument raised to the second (also the ^ operator).", Aliases: []string{"pow"},
		Fn: func(a []types.Datum) (types.Datum, error) { return Power(a[0], a[1]) }})
	unaryFloat("sqrt", "The square root (an error below zero).", func(x float64) (float64, error) {
		if x < 0 {
			return 0, errf(CodeInvalidArgument, "cannot take square root of a negative number")
		}
		return math.Sqrt(x), nil
	})
	unaryFloat("cbrt", "The cube root.", func(x float64) (float64, error) { return math.Cbrt(x), nil })
	unaryFloat("exp", "e raised to the argument.", func(x float64) (float64, error) { return math.Exp(x), nil })
	unaryFloat("ln", "The natural logarithm (an error at or below zero).", func(x float64) (float64, error) {
		if x <= 0 {
			return 0, errf(CodeInvalidArgument, "cannot take logarithm of a nonpositive number")
		}
		return math.Log(x), nil
	})
	register(&Builtin{Name: "log", Args: []types.Family{Any, Any}, MinArgs: 1, Ret: types.Float, Category: catMath,
		Doc: "log(x) is the base-10 logarithm; log(b, x) the logarithm of x in base b.", Aliases: []string{"log10"},
		Fn: func(a []types.Datum) (types.Datum, error) {
			x, err := numeric(a[len(a)-1])
			if err != nil {
				return types.Datum{}, err
			}
			base := 10.0
			if len(a) == 2 {
				if base, err = numeric(a[0]); err != nil {
					return types.Datum{}, err
				}
			}
			if x <= 0 || base <= 0 {
				return types.Datum{}, errf(CodeInvalidArgument, "cannot take logarithm of a nonpositive number")
			}
			if base == 1 {
				return types.Datum{}, errf(CodeDivisionByZero, "division by zero")
			}
			return f64(math.Log(x) / math.Log(base)), nil
		}})
	register(&Builtin{Name: "pi", Args: nil, Ret: types.Float, Category: catMath, Doc: "π.",
		Fn: func([]types.Datum) (types.Datum, error) { return f64(math.Pi), nil }})
	register(&Builtin{Name: "random", Args: nil, Ret: types.Float, Vol: Volatile, Category: catMath,
		Doc: "A random value in [0, 1), fresh per row.",
		Fn:  func([]types.Datum) (types.Datum, error) { return f64(rand.Float64()), nil }})
	register(&Builtin{Name: "width_bucket", Args: []types.Family{Any, Any, Any, types.Int}, MinArgs: 4, Ret: types.Int, Category: catMath,
		Doc: "The bucket (1..count) the operand falls in when [low, high) is split into count equal buckets; 0 below, count+1 above.",
		Fn: func(a []types.Datum) (types.Datum, error) {
			x, err := numeric(a[0])
			if err != nil {
				return types.Datum{}, err
			}
			lo, err := numeric(a[1])
			if err != nil {
				return types.Datum{}, err
			}
			hi, err := numeric(a[2])
			if err != nil {
				return types.Datum{}, err
			}
			n := a[3].I
			if n <= 0 {
				return types.Datum{}, errf(CodeInvalidArgument, "count must be greater than zero")
			}
			if lo == hi {
				return types.Datum{}, errf(CodeInvalidArgument, "lower bound cannot equal upper bound")
			}
			if lo > hi {
				lo, hi = hi, lo
				x = lo + hi - x
			}
			switch {
			case x < lo:
				return i64(0), nil
			case x >= hi:
				return i64(n + 1), nil
			}
			return i64(int64(math.Floor((x-lo)/(hi-lo)*float64(n))) + 1), nil
		}})
	register(&Builtin{Name: "gcd", Args: []types.Family{types.Int, types.Int}, MinArgs: 2, Ret: types.Int, Category: catMath,
		Doc: "The greatest common divisor.",
		Fn: func(a []types.Datum) (types.Datum, error) {
			x, y := a[0].I, a[1].I
			if x < 0 {
				x = -x
			}
			if y < 0 {
				y = -y
			}
			for y != 0 {
				x, y = y, x%y
			}
			return i64(x), nil
		}})
}

// roundTo rounds (or truncates) the first argument to the scale in the
// second, in the argument's own type.
func roundTo(a []types.Datum, truncate bool) (types.Datum, error) {
	scale := int64(0)
	if len(a) > 1 {
		scale = a[1].I
	}
	d := a[0]
	switch d.Fam {
	case types.Int:
		if scale >= 0 {
			return d, nil
		}
		p := math.Pow10(int(-scale))
		v := float64(d.I) / p
		if truncate {
			v = math.Trunc(v)
		} else {
			v = math.Round(v)
		}
		return i64(int64(v * p)), nil
	case types.Float:
		p := math.Pow10(int(scale))
		if scale < 0 {
			p = 1 / math.Pow10(int(-scale))
		}
		v := d.F * p
		if truncate {
			v = math.Trunc(v)
		} else {
			v = math.Round(v)
		}
		return f64(v / p), nil
	case types.Decimal:
		v, err := d.DecimalVal()
		if err != nil {
			return types.Datum{}, errf(CodeInvalidText, "%v", err)
		}
		if scale < -1000 || scale > 1000 {
			return types.Datum{}, errf(CodeInvalidArgument, "scale out of range")
		}
		var q decimal.Dec
		if truncate {
			q = decimal.Truncate(v, int32(scale))
		} else {
			q = decimal.RoundHalfAway(v, int32(scale))
		}
		return types.NewDecimal(q.String()), nil
	case types.String:
		c, err := d.Coerce(types.Decimal)
		if err != nil {
			return types.Datum{}, errf(CodeInvalidText, "invalid input syntax for type numeric: %q", d.S)
		}
		return roundTo(append([]types.Datum{c}, a[1:]...), truncate)
	}
	return types.Datum{}, errf(CodeUndefined, "round() requires a numeric argument, got %s", d.Fam)
}

func floorCeil(d types.Datum, fn func(float64) float64) (types.Datum, error) {
	switch d.Fam {
	case types.Int:
		return d, nil
	case types.Float:
		return f64(fn(d.F)), nil
	case types.Decimal:
		v, err := d.DecimalVal()
		if err != nil {
			return types.Datum{}, errf(CodeInvalidText, "%v", err)
		}
		t := decimal.Truncate(v, 0)
		if decimal.Cmp(t, v) != 0 {
			// Truncation moved toward zero: floor of a negative and ceil
			// of a positive need one more step away from it.
			if (v.Sign() < 0) == (fn(-0.5) == -1) {
				step := decimal.FromInt(1)
				if v.Sign() < 0 {
					step = decimal.Neg(step)
				}
				t = decimal.Add(t, step)
			}
		}
		return types.NewDecimal(t.String()), nil
	case types.String:
		c, err := d.Coerce(types.Decimal)
		if err != nil {
			return types.Datum{}, errf(CodeInvalidText, "invalid input syntax for type numeric: %q", d.S)
		}
		return floorCeil(c, fn)
	}
	return types.Datum{}, errf(CodeUndefined, "floor()/ceil() require a numeric argument, got %s", d.Fam)
}

// Mod is the % operator and mod(): integer remainder with the dividend's
// sign, exact for decimals, float otherwise.
func Mod(l, r types.Datum) (types.Datum, error) {
	if l.Null || r.Null {
		return types.DNull, nil
	}
	if l.Fam == types.Int && r.Fam == types.Int {
		if r.I == 0 {
			return types.Datum{}, errf(CodeDivisionByZero, "division by zero")
		}
		if r.I == -1 {
			return i64(0), nil
		}
		return i64(l.I % r.I), nil
	}
	if (l.Fam == types.Decimal || l.Fam == types.Int) && (r.Fam == types.Decimal || r.Fam == types.Int) {
		lv, err := decimalOf(l)
		if err != nil {
			return types.Datum{}, err
		}
		rv, err := decimalOf(r)
		if err != nil {
			return types.Datum{}, err
		}
		if rv.Sign() == 0 {
			return types.Datum{}, errf(CodeDivisionByZero, "division by zero")
		}
		q, err := decimal.DivQuantize(lv, rv, 0)
		if err != nil {
			return types.Datum{}, errf(CodeDivisionByZero, "division by zero")
		}
		q = decimal.Truncate(q, 0)
		// A quantized quotient may have rounded away from zero; step back.
		prod := decimal.Mul(q, rv)
		rem := decimal.Sub(lv, prod)
		if rem.Sign() != 0 && rem.Sign() != lv.Sign() {
			if q.Sign() < 0 || (q.Sign() == 0 && lv.Sign() != rv.Sign()) {
				q = decimal.Add(q, decimal.FromInt(1))
			} else {
				q = decimal.Sub(q, decimal.FromInt(1))
			}
			rem = decimal.Sub(lv, decimal.Mul(q, rv))
		}
		return types.NewDecimal(rem.String()), nil
	}
	x, err := numeric(l)
	if err != nil {
		return types.Datum{}, err
	}
	y, err := numeric(r)
	if err != nil {
		return types.Datum{}, err
	}
	if y == 0 {
		return types.Datum{}, errf(CodeDivisionByZero, "division by zero")
	}
	return f64(math.Mod(x, y)), nil
}

// Power is the ^ operator and power(): integer when both sides are
// integers and the exponent is not negative, float otherwise.
func Power(l, r types.Datum) (types.Datum, error) {
	if l.Null || r.Null {
		return types.DNull, nil
	}
	if l.Fam == types.Int && r.Fam == types.Int && r.I >= 0 {
		result := int64(1)
		base := l.I
		for e := r.I; e > 0; e-- {
			next := result * base
			if base != 0 && next/base != result {
				return types.Datum{}, errf(CodeOutOfRange, "integer out of range")
			}
			result = next
		}
		return i64(result), nil
	}
	x, err := numeric(l)
	if err != nil {
		return types.Datum{}, err
	}
	y, err := numeric(r)
	if err != nil {
		return types.Datum{}, err
	}
	if x == 0 && y < 0 {
		return types.Datum{}, errf(CodeInvalidArgument, "zero raised to a negative power is undefined")
	}
	if x < 0 && y != math.Trunc(y) {
		return types.Datum{}, errf(CodeInvalidArgument, "a negative number raised to a non-integer power yields a complex result")
	}
	v := math.Pow(x, y)
	if math.IsInf(v, 0) || math.IsNaN(v) {
		return types.Datum{}, errf(CodeOutOfRange, "value out of range: overflow")
	}
	return f64(v), nil
}

func decimalOf(d types.Datum) (decimal.Dec, error) {
	if d.Fam == types.Int {
		return decimal.FromInt(d.I), nil
	}
	v, err := d.DecimalVal()
	if err != nil {
		return decimal.Dec{}, errf(CodeInvalidText, "%v", err)
	}
	return v, nil
}
