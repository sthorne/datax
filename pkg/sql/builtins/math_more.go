package builtins

import (
	"crypto/rand"
	"math"
	"math/big"
	mrand "math/rand/v2"
	"strings"

	"github.com/sthorne/datax/pkg/sql/types"
	"github.com/sthorne/datax/pkg/util/decimal"
)

// The trigonometric, hyperbolic and remaining numeric functions
// (PG-F-010), with PostgreSQL's float8 semantics: NaN in gives NaN out,
// an argument outside a function's domain is 22003 "input is out of
// range", and a result that overflows is 22003 too, except where
// PostgreSQL returns an infinity itself (cot(0), atanh(1), sinh and
// cosh of a large argument).

// CodeInvalidArgPower is invalid_argument_for_power_function (sqrt of a
// negative number, factorial of one, 0 ^ -1).
const CodeInvalidArgPower = "2201F"

// CodeInvalidArgLog is invalid_argument_for_logarithm (ln(0), log(-1)).
const CodeInvalidArgLog = "2201E"

func errInputRange() error { return errf(CodeOutOfRange, "input is out of range") }

func errFloatOverflow() error { return errf(CodeOutOfRange, "value out of range: overflow") }

// floatFunc registers a float8 function of one numeric argument whose
// fn decides its own domain errors and infinities.
func floatFunc(name, doc string, fn func(float64) (float64, error)) {
	register(&Builtin{Name: name, Args: []types.Family{Any}, MinArgs: 1, Ret: types.Float, Category: catMath, Doc: doc,
		Fn: func(a []types.Datum) (types.Datum, error) {
			x, err := numeric(a[0])
			if err != nil {
				return types.Datum{}, err
			}
			y, err := fn(x)
			if err != nil {
				return types.Datum{}, err
			}
			if y == 0 {
				y = 0 // no negative zero
			}
			return f64(y), nil
		}})
}

// floatFunc2 registers a float8 function of two numeric arguments.
func floatFunc2(name, doc string, fn func(y, x float64) (float64, error)) {
	register(&Builtin{Name: name, Args: []types.Family{Any, Any}, MinArgs: 2, Ret: types.Float, Category: catMath, Doc: doc,
		Fn: func(a []types.Datum) (types.Datum, error) {
			y, err := numeric(a[0])
			if err != nil {
				return types.Datum{}, err
			}
			x, err := numeric(a[1])
			if err != nil {
				return types.Datum{}, err
			}
			r, err := fn(y, x)
			if err != nil {
				return types.Datum{}, err
			}
			return f64(r), nil
		}})
}

// trig wraps a radian function: NaN passes, an infinite argument is
// out of range, an infinite result an overflow (float.c's dsin, dcos,
// dtan).
func trig(fn func(float64) float64) func(float64) (float64, error) {
	return func(x float64) (float64, error) {
		if math.IsNaN(x) {
			return x, nil
		}
		if math.IsInf(x, 0) {
			return 0, errInputRange()
		}
		y := fn(x)
		if math.IsInf(y, 0) {
			return 0, errFloatOverflow()
		}
		return y, nil
	}
}

// unitDomain wraps asin / acos and their degree forms: [-1, 1] only.
func unitDomain(fn func(float64) float64) func(float64) (float64, error) {
	return func(x float64) (float64, error) {
		if math.IsNaN(x) {
			return x, nil
		}
		if x < -1 || x > 1 {
			return 0, errInputRange()
		}
		return fn(x), nil
	}
}

// The degree-based functions follow float.c: the argument is reduced to
// the first quadrant and the result scaled so that the special angles
// come out exact (sind(30) = 0.5, tand(45) = 1).
// radiansPerDegree is a variable so that every product with it is a
// float64 multiplication at run time, as in C: a constant would be
// folded at Go's arbitrary precision and the special angles would no
// longer come out exact.
var radiansPerDegree = 0.0174532925199432957692

var (
	sin30         = math.Sin(30 * radiansPerDegree)
	oneMinusCos60 = 1 - math.Cos(60*radiansPerDegree)
	asin05        = math.Asin(0.5)
	acos05        = math.Acos(0.5)
	atan10        = math.Atan(1.0)
	tan45         = sindQ1(45) / cosdQ1(45)
	cot45         = cosdQ1(45) / sindQ1(45)
)

func sind0to30(x float64) float64 { return (math.Sin(x*radiansPerDegree) / sin30) / 2 }

func cosd0to60(x float64) float64 {
	return 1 - ((1-math.Cos(x*radiansPerDegree))/oneMinusCos60)/2
}

func sindQ1(x float64) float64 {
	if x <= 30 {
		return sind0to30(x)
	}
	return cosd0to60(90 - x)
}

func cosdQ1(x float64) float64 {
	if x <= 60 {
		return cosd0to60(x)
	}
	return sind0to30(90 - x)
}

func asindQ1(x float64) float64 {
	if x <= 0.5 {
		return (math.Asin(x) / asin05) * 30
	}
	return 90 - (math.Acos(x)/acos05)*60
}

func acosdQ1(x float64) float64 {
	if x <= 0.5 {
		return 90 - (math.Asin(x)/asin05)*30
	}
	return (math.Acos(x) / acos05) * 60
}

// degreeArg checks a degree argument: NaN passes through (ok false), an
// infinity is out of range.
func degreeArg(x float64) (ok bool, err error) {
	if math.IsNaN(x) {
		return false, nil
	}
	if math.IsInf(x, 0) {
		return false, errInputRange()
	}
	return true, nil
}

func sind(x float64) (float64, error) {
	if ok, err := degreeArg(x); !ok {
		return x, err
	}
	sign := 1.0
	x = math.Mod(x, 360)
	if x < 0 {
		x, sign = -x, -sign
	}
	if x > 180 {
		x, sign = 360-x, -sign
	}
	if x > 90 {
		x = 180 - x
	}
	return sign * sindQ1(x), nil
}

func cosd(x float64) (float64, error) {
	if ok, err := degreeArg(x); !ok {
		return x, err
	}
	sign := 1.0
	x = math.Mod(x, 360)
	if x < 0 {
		x = -x
	}
	if x > 180 {
		x = 360 - x
	}
	if x > 90 {
		x, sign = 180-x, -sign
	}
	return sign * cosdQ1(x), nil
}

// tancotd is tand (cot false) or cotd.
func tancotd(x float64, cot bool) (float64, error) {
	if ok, err := degreeArg(x); !ok {
		return x, err
	}
	sign := 1.0
	x = math.Mod(x, 360)
	if x < 0 {
		x, sign = -x, -sign
	}
	if x > 180 {
		x, sign = 360-x, -sign
	}
	if x > 90 {
		x, sign = 180-x, -sign
	}
	var r float64
	if cot {
		r = sign * ((cosdQ1(x) / sindQ1(x)) / cot45)
	} else {
		r = sign * ((sindQ1(x) / cosdQ1(x)) / tan45)
	}
	return r, nil
}

func init() {
	floatFunc("sin", "The sine of an angle in radians.", trig(math.Sin))
	floatFunc("cos", "The cosine of an angle in radians.", trig(math.Cos))
	floatFunc("tan", "The tangent of an angle in radians.", trig(math.Tan))
	floatFunc("cot", "The cotangent of an angle in radians (Infinity at 0).", func(x float64) (float64, error) {
		if math.IsNaN(x) {
			return x, nil
		}
		if math.IsInf(x, 0) {
			return 0, errInputRange()
		}
		return 1 / math.Tan(x), nil
	})
	floatFunc("asin", "The arcsine, in radians (an argument outside [-1, 1] is out of range).", unitDomain(math.Asin))
	floatFunc("acos", "The arccosine, in radians (an argument outside [-1, 1] is out of range).", unitDomain(math.Acos))
	floatFunc("atan", "The arctangent, in radians.", func(x float64) (float64, error) { return math.Atan(x), nil })
	floatFunc2("atan2", "atan2(y, x): the arctangent of y/x, in radians, in the quadrant of (x, y).", func(y, x float64) (float64, error) {
		return math.Atan2(y, x), nil
	})
	floatFunc("sind", "The sine of an angle in degrees (exact at the special angles: sind(30) = 0.5).", sind)
	floatFunc("cosd", "The cosine of an angle in degrees (exact at the special angles: cosd(60) = 0.5).", cosd)
	floatFunc("tand", "The tangent of an angle in degrees (exact at the special angles: tand(45) = 1).", func(x float64) (float64, error) { return tancotd(x, false) })
	floatFunc("cotd", "The cotangent of an angle in degrees (Infinity at 0).", func(x float64) (float64, error) { return tancotd(x, true) })
	floatFunc("asind", "The arcsine, in degrees.", unitDomain(func(x float64) float64 {
		if x >= 0 {
			return asindQ1(x)
		}
		return -asindQ1(-x)
	}))
	floatFunc("acosd", "The arccosine, in degrees.", unitDomain(func(x float64) float64 {
		if x >= 0 {
			return acosdQ1(x)
		}
		return 90 + asindQ1(-x)
	}))
	floatFunc("atand", "The arctangent, in degrees.", func(x float64) (float64, error) {
		if math.IsNaN(x) {
			return x, nil
		}
		return (math.Atan(x) / atan10) * 45, nil
	})
	floatFunc2("atan2d", "atan2d(y, x): the arctangent of y/x, in degrees.", func(y, x float64) (float64, error) {
		if math.IsNaN(y) || math.IsNaN(x) {
			return math.NaN(), nil
		}
		return (math.Atan2(y, x) / atan10) * 45, nil
	})
	floatFunc("sinh", "The hyperbolic sine.", func(x float64) (float64, error) { return math.Sinh(x), nil })
	floatFunc("cosh", "The hyperbolic cosine.", func(x float64) (float64, error) { return math.Cosh(x), nil })
	floatFunc("tanh", "The hyperbolic tangent.", func(x float64) (float64, error) { return math.Tanh(x), nil })
	floatFunc("asinh", "The inverse hyperbolic sine.", func(x float64) (float64, error) { return math.Asinh(x), nil })
	floatFunc("acosh", "The inverse hyperbolic cosine (out of range below 1).", func(x float64) (float64, error) {
		if x < 1 {
			return 0, errInputRange()
		}
		return math.Acosh(x), nil
	})
	floatFunc("atanh", "The inverse hyperbolic tangent (out of range outside [-1, 1]; ±Infinity at ±1).", func(x float64) (float64, error) {
		if x < -1 || x > 1 {
			return 0, errInputRange()
		}
		return math.Atanh(x), nil
	})
	floatFunc("degrees", "Radians converted to degrees.", func(x float64) (float64, error) {
		y := x / radiansPerDegree
		if math.IsInf(y, 0) && !math.IsInf(x, 0) {
			return 0, errFloatOverflow()
		}
		return y, nil
	})
	floatFunc("radians", "Degrees converted to radians.", func(x float64) (float64, error) { return x * radiansPerDegree, nil })
	floatFunc("erf", "The error function.", func(x float64) (float64, error) { return math.Erf(x), nil })
	floatFunc("erfc", "The complementary error function, 1 - erf(x) without the loss of precision.", func(x float64) (float64, error) { return math.Erfc(x), nil })

	register(&Builtin{Name: "factorial", Args: []types.Family{types.Int}, MinArgs: 1, Ret: types.Decimal, Category: catMath,
		Doc: "n! as a numeric (an error for a negative n).",
		Fn: func(a []types.Datum) (types.Datum, error) {
			n := a[0].I
			if n < 0 {
				return types.Datum{}, errf(CodeInvalidArgPower, "factorial of a negative number is undefined")
			}
			if n > 32177 {
				return types.Datum{}, errf(CodeOutOfRange, "value overflows numeric format")
			}
			if n <= 1 {
				return types.NewDecimal("1"), nil
			}
			return types.NewDecimal(new(big.Int).MulRange(1, n).String()), nil
		}})
	register(&Builtin{Name: "lcm", Args: []types.Family{types.Int, types.Int}, MinArgs: 2, Ret: types.Int, Category: catMath,
		Doc: "The least common multiple (0 when either argument is 0).",
		Fn: func(a []types.Datum) (types.Datum, error) {
			x, y := a[0].I, a[1].I
			if x == 0 || y == 0 {
				return i64(0), nil
			}
			g := new(big.Int).GCD(nil, nil, new(big.Int).Abs(big.NewInt(x)), new(big.Int).Abs(big.NewInt(y)))
			l := new(big.Int).Mul(new(big.Int).Quo(big.NewInt(x), g), big.NewInt(y))
			l.Abs(l)
			if !l.IsInt64() {
				return types.Datum{}, errf(CodeOutOfRange, "bigint out of range")
			}
			return i64(l.Int64()), nil
		}})
	register(&Builtin{Name: "scale", Args: []types.Family{types.Decimal}, MinArgs: 1, Ret: types.Int, Category: catMath,
		Doc: "The number of decimal digits in the fractional part (of the value as datax holds it: without trailing zeros).",
		Fn:  func(a []types.Datum) (types.Datum, error) { return i64(int64(decimalScale(a[0].S))), nil }})
	register(&Builtin{Name: "min_scale", Args: []types.Family{types.Decimal}, MinArgs: 1, Ret: types.Int, Category: catMath,
		Doc: "The fewest fractional digits that represent the value exactly.",
		Fn:  func(a []types.Datum) (types.Datum, error) { return i64(int64(decimalScale(a[0].S))), nil }})
	register(&Builtin{Name: "trim_scale", Args: []types.Family{types.Decimal}, MinArgs: 1, Ret: types.Decimal, Category: catMath,
		Doc: "The value with its trailing fractional zeros removed.",
		Fn: func(a []types.Datum) (types.Datum, error) {
			v, err := a[0].DecimalVal()
			if err != nil {
				return types.Datum{}, errf(CodeInvalidText, "%v", err)
			}
			return types.NewDecimal(v.String()), nil
		}})
	register(&Builtin{Name: "random_normal", Args: []types.Family{types.Float, types.Float}, MinArgs: 0, Ret: types.Float, Vol: Volatile, Category: catMath,
		Doc: "A normally distributed random value with the given mean (0) and standard deviation (1), fresh per row.",
		Fn: func(a []types.Datum) (types.Datum, error) {
			mean, stddev := 0.0, 1.0
			if len(a) > 0 {
				mean = a[0].F
			}
			if len(a) > 1 {
				stddev = a[1].F
			}
			return f64(mrand.NormFloat64()*stddev + mean), nil
		}})
}

// decimalScale is the number of digits after the point in a canonical
// decimal rendering.
func decimalScale(s string) int {
	if i := strings.IndexByte(s, '.'); i >= 0 {
		return len(s) - i - 1
	}
	return 0
}

// randomBetween is random(min, max): an integer in [min, max] for two
// integers, else a numeric in [min, max] at the larger of their scales.
func randomBetween(lo, hi types.Datum) (types.Datum, error) {
	if lo.Fam == types.Int && hi.Fam == types.Int {
		if lo.I > hi.I {
			return types.Datum{}, errf(CodeInvalidArgument, "lower bound must be less than or equal to upper bound")
		}
		span := uint64(hi.I - lo.I)
		if span == math.MaxUint64 {
			return i64(int64(mrand.Uint64())), nil
		}
		return i64(lo.I + int64(mrand.Uint64N(span+1))), nil
	}
	lv, err := decimalArg(lo)
	if err != nil {
		return types.Datum{}, err
	}
	hv, err := decimalArg(hi)
	if err != nil {
		return types.Datum{}, err
	}
	if decimal.Cmp(lv, hv) > 0 {
		return types.Datum{}, errf(CodeInvalidArgument, "lower bound must be less than or equal to upper bound")
	}
	scale := max(decimalScale(lv.String()), decimalScale(hv.String()))
	unit, _ := decimal.Parse("1e" + itoa(scale))
	lo10, hi10 := decimal.Mul(lv, unit), decimal.Mul(hv, unit)
	li, _ := new(big.Int).SetString(lo10.String(), 10)
	hiI, _ := new(big.Int).SetString(hi10.String(), 10)
	span := new(big.Int).Sub(hiI, li)
	r, err := rand.Int(rand.Reader, span.Add(span, big.NewInt(1)))
	if err != nil {
		return types.Datum{}, errf(CodeInvalidArgument, "%v", err)
	}
	r.Add(r, li)
	v, err := decimal.Parse(r.String() + "e-" + itoa(scale))
	if err != nil {
		return types.Datum{}, errf(CodeOutOfRange, "%v", err)
	}
	return types.NewDecimal(v.String()), nil
}

// decimalArg reads a numeric argument as an exact decimal.
func decimalArg(d types.Datum) (decimal.Dec, error) {
	switch d.Fam {
	case types.Int, types.Decimal:
		return decimalOf(d)
	case types.Float, types.String:
		c, err := d.Coerce(types.Decimal)
		if err != nil {
			return decimal.Dec{}, errf(CodeInvalidText, "invalid input syntax for type numeric: %q", d.Text())
		}
		return decimalOf(c)
	}
	return decimal.Dec{}, errf(CodeUndefined, "function does not exist for argument type %s", strings.ToLower(d.Fam.String()))
}

func itoa(i int) string { return big.NewInt(int64(i)).String() }

// IsBitwiseOp reports whether op is one of the integer bitwise
// operators & | # << >>.
func IsBitwiseOp(op string) bool {
	switch op {
	case "&", "|", "#", "<<", ">>":
		return true
	}
	return false
}

// pgTypeName names a family as PostgreSQL's operator errors do.
func pgTypeName(f types.Family) string {
	switch f {
	case types.Int:
		return "bigint"
	case types.Decimal:
		return "numeric"
	case types.Float:
		return "double precision"
	case types.String:
		return "text"
	case types.Bool:
		return "boolean"
	}
	return strings.ToLower(f.String())
}

// bitwiseOperand reads an integer operand: an integer, or text read
// as one (an untyped literal takes the other side's type).
func bitwiseOperand(d types.Datum) (int64, bool, error) {
	switch d.Fam {
	case types.Int:
		return d.I, true, nil
	case types.String:
		c, err := d.Coerce(types.Int)
		if err != nil {
			return 0, true, errf(CodeInvalidText, "invalid input syntax for type bigint: \"%s\"", d.S)
		}
		return c.I, true, nil
	}
	return 0, false, nil
}

// Bitwise evaluates l op r for the integer bitwise operators: & (and),
// | (or), # (xor), << and >> (shifts; the count is taken modulo 64, as
// PostgreSQL's int8 shifts behave on x86-64, and >> is arithmetic). An
// overflowing shift wraps, as in PostgreSQL.
func Bitwise(l types.Datum, op string, r types.Datum) (types.Datum, error) {
	if l.Null || r.Null {
		return types.DNull, nil
	}
	x, okx, err := bitwiseOperand(l)
	if err != nil {
		return types.Datum{}, err
	}
	y, oky, err := bitwiseOperand(r)
	if err != nil {
		return types.Datum{}, err
	}
	if !okx || !oky {
		return types.Datum{}, errf(CodeUndefined, "operator does not exist: %s %s %s", pgTypeName(l.Fam), op, pgTypeName(r.Fam))
	}
	switch op {
	case "&":
		return i64(x & y), nil
	case "|":
		return i64(x | y), nil
	case "#":
		return i64(x ^ y), nil
	}
	if y > math.MaxInt32 || y < math.MinInt32 {
		return types.Datum{}, errf(CodeOutOfRange, "integer out of range")
	}
	if op == "<<" {
		return i64(x << (uint64(y) & 63)), nil
	}
	return i64(x >> (uint64(y) & 63)), nil
}

func init() {
	register(&Builtin{Name: "int8not", Args: []types.Family{types.Int}, MinArgs: 1, Ret: types.Int, Category: catMath, Hidden: true,
		Doc: "~x: the bitwise complement of an integer.",
		Fn:  func(a []types.Datum) (types.Datum, error) { return i64(^a[0].I), nil }})
}
