// Package decimal implements exact arbitrary-precision decimal numbers on
// the standard library's math/big — value = Coeff × 10^Exp — for the SQL
// DECIMAL/NUMERIC type. The canonical form (no trailing zeros in the
// coefficient; zero is 0×10^0) makes the canonical String unique per
// value, which the SQL layer relies on for grouping, memo keys, and
// equality on stored text.
package decimal

import (
	"fmt"
	"math/big"
	"strconv"
	"strings"
)

// maxExp bounds the exponent (and thereby String's plain expansion);
// PostgreSQL numeric allows far more digits than anyone stores in a
// prototype row.
const maxExp = 2000

// Dec is an immutable decimal value: Coeff × 10^Exp, sign on Coeff.
type Dec struct {
	coeff *big.Int
	exp   int32
}

var ten = big.NewInt(10)

// New builds a Dec from an integer coefficient and exponent (canonicalized).
func New(coeff int64, exp int32) Dec {
	return canon(big.NewInt(coeff), exp)
}

// canon strips trailing zeros from the coefficient into the exponent.
func canon(c *big.Int, exp int32) Dec {
	if c.Sign() == 0 {
		return Dec{coeff: new(big.Int), exp: 0}
	}
	q, r := new(big.Int), new(big.Int)
	for {
		q.QuoRem(c, ten, r)
		if r.Sign() != 0 {
			break
		}
		c = new(big.Int).Set(q)
		exp++
	}
	return Dec{coeff: c, exp: exp}
}

// Parse reads a decimal literal: optional sign, digits, optional
// fraction, optional e±exp. NaN/Infinity are rejected.
func Parse(s string) (Dec, error) {
	t := strings.TrimSpace(s)
	if t == "" {
		return Dec{}, fmt.Errorf("invalid decimal %q", s)
	}
	mant := t
	var exp int64
	if i := strings.IndexAny(t, "eE"); i >= 0 {
		mant = t[:i]
		var err error
		if _, err = fmt.Sscanf(t[i+1:], "%d", &exp); err != nil || t[i+1:] == "" {
			return Dec{}, fmt.Errorf("invalid decimal %q", s)
		}
	}
	neg := false
	switch {
	case strings.HasPrefix(mant, "-"):
		neg, mant = true, mant[1:]
	case strings.HasPrefix(mant, "+"):
		mant = mant[1:]
	}
	intPart, fracPart := mant, ""
	if i := strings.IndexByte(mant, '.'); i >= 0 {
		intPart, fracPart = mant[:i], mant[i+1:]
	}
	digits := intPart + fracPart
	if digits == "" {
		return Dec{}, fmt.Errorf("invalid decimal %q", s)
	}
	for _, r := range digits {
		if r < '0' || r > '9' {
			return Dec{}, fmt.Errorf("invalid decimal %q", s)
		}
	}
	c, ok := new(big.Int).SetString(digits, 10)
	if !ok {
		return Dec{}, fmt.Errorf("invalid decimal %q", s)
	}
	if neg {
		c.Neg(c)
	}
	exp -= int64(len(fracPart))
	if exp > maxExp || exp < -maxExp {
		return Dec{}, fmt.Errorf("decimal exponent out of range in %q", s)
	}
	return canon(c, int32(exp)), nil
}

// String renders the canonical plain-decimal form (never scientific).
func (d Dec) String() string {
	if d.coeff == nil || d.coeff.Sign() == 0 {
		return "0"
	}
	digits := new(big.Int).Abs(d.coeff).String()
	neg := d.coeff.Sign() < 0
	var b strings.Builder
	if neg {
		b.WriteByte('-')
	}
	switch {
	case d.exp >= 0:
		b.WriteString(digits)
		for i := int32(0); i < d.exp; i++ {
			b.WriteByte('0')
		}
	case int(-d.exp) < len(digits):
		split := len(digits) + int(d.exp)
		b.WriteString(digits[:split])
		b.WriteByte('.')
		b.WriteString(digits[split:])
	default:
		b.WriteString("0.")
		for i := 0; i < int(-d.exp)-len(digits); i++ {
			b.WriteByte('0')
		}
		b.WriteString(digits)
	}
	return b.String()
}

// Sign reports -1, 0, or +1.
func (d Dec) Sign() int {
	if d.coeff == nil {
		return 0
	}
	return d.coeff.Sign()
}

// align returns both coefficients scaled to the smaller exponent.
func align(a, b Dec) (ca, cb *big.Int, exp int32) {
	ea, eb := a.exp, b.exp
	ca, cb = a.coeff, b.coeff
	if ca == nil {
		ca = new(big.Int)
	}
	if cb == nil {
		cb = new(big.Int)
	}
	switch {
	case ea == eb:
		return ca, cb, ea
	case ea > eb:
		scaled := new(big.Int).Set(ca)
		for i := eb; i < ea; i++ {
			scaled.Mul(scaled, ten)
		}
		return scaled, cb, eb
	default:
		scaled := new(big.Int).Set(cb)
		for i := ea; i < eb; i++ {
			scaled.Mul(scaled, ten)
		}
		return ca, scaled, ea
	}
}

// Add returns a + b.
func Add(a, b Dec) Dec {
	ca, cb, exp := align(a, b)
	return canon(new(big.Int).Add(ca, cb), exp)
}

// Sub returns a - b.
func Sub(a, b Dec) Dec {
	ca, cb, exp := align(a, b)
	return canon(new(big.Int).Sub(ca, cb), exp)
}

// Neg returns -a.
func Neg(a Dec) Dec {
	if a.coeff == nil {
		return Dec{coeff: new(big.Int)}
	}
	return Dec{coeff: new(big.Int).Neg(a.coeff), exp: a.exp}
}

// Mul returns a × b.
func Mul(a, b Dec) Dec {
	ca, cb := a.coeff, b.coeff
	if ca == nil || cb == nil {
		return Dec{coeff: new(big.Int)}
	}
	return canon(new(big.Int).Mul(ca, cb), a.exp+b.exp)
}

// Cmp compares a and b numerically.
func Cmp(a, b Dec) int {
	ca, cb, _ := align(a, b)
	return ca.Cmp(cb)
}

// DivQuantize returns a ÷ b rounded half-even to the given decimal scale
// (digits after the point), then canonicalized. Division by zero errors.
func DivQuantize(a, b Dec, scale int32) (Dec, error) {
	if b.Sign() == 0 {
		return Dec{}, fmt.Errorf("division by zero")
	}
	// Compute (a.coeff × 10^k) / b.coeff at exponent a.exp - b.exp - k,
	// choosing k so the result exponent is exactly -scale.
	k := int64(a.exp) - int64(b.exp) + int64(scale)
	num := new(big.Int).Set(a.coeff)
	den := new(big.Int).Set(b.coeff)
	switch {
	case k > 0:
		if k > 4*maxExp {
			return Dec{}, fmt.Errorf("decimal division out of range")
		}
		for i := int64(0); i < k; i++ {
			num.Mul(num, ten)
		}
	case k < 0:
		if -k > 4*maxExp {
			return Dec{}, fmt.Errorf("decimal division out of range")
		}
		for i := int64(0); i < -k; i++ {
			den.Mul(den, ten)
		}
	}
	// Round half-even on |num/den|, then restore the sign.
	sign := num.Sign() * den.Sign()
	num.Abs(num)
	den.Abs(den)
	q, r := new(big.Int).QuoRem(num, den, new(big.Int))
	r.Mul(r, big.NewInt(2))
	switch c := r.Cmp(den); {
	case c > 0:
		q.Add(q, big.NewInt(1))
	case c == 0: // exactly half: round to even
		if q.Bit(0) == 1 {
			q.Add(q, big.NewInt(1))
		}
	}
	if sign < 0 {
		q.Neg(q)
	}
	return canon(q, -scale), nil
}

// FromInt lifts an int64 exactly.
func FromInt(i int64) Dec { return New(i, 0) }

// Quantize returns a rounded half-even to the given decimal scale (digits
// after the point), then canonicalized — so a value already representable
// at that scale is unchanged. Used to enforce DECIMAL(p,s) column scale.
func Quantize(a Dec, scale int32) (Dec, error) {
	return DivQuantize(a, FromInt(1), scale)
}

// Float64 converts lossily (for Decimal→Float coercion), correctly
// rounded: parsing the canonical string yields the nearest float64, so
// Float64(Parse("0.42")) is bit-identical to the float literal 0.42.
// (Iterating ×10/÷10 instead compounds one ULP of error per step.)
func (d Dec) Float64() float64 {
	// String() is always a plain, valid decimal, so the only possible
	// error is ErrRange, which still hands back the correct ±Inf.
	f, _ := strconv.ParseFloat(d.String(), 64)
	return f
}

func coeffOf(d Dec) *big.Int {
	if d.coeff == nil {
		return new(big.Int)
	}
	return d.coeff
}

// Mantissa returns the value's sign, its decimal digits with no leading
// or trailing zeros, and the scientific exponent E such that
// value = ±0.digits × 10^E — the key encoding's normal form.
func (d Dec) Mantissa() (neg bool, digits string, e int64) {
	if d.Sign() == 0 {
		return false, "", 0
	}
	digits = new(big.Int).Abs(coeffOf(d)).String()
	return d.Sign() < 0, digits, int64(len(digits)) + int64(d.exp)
}

// FromMantissa rebuilds a Dec from Mantissa's normal form.
func FromMantissa(neg bool, digits string, e int64) (Dec, error) {
	if digits == "" {
		return New(0, 0), nil
	}
	c, ok := new(big.Int).SetString(digits, 10)
	if !ok {
		return Dec{}, fmt.Errorf("invalid mantissa %q", digits)
	}
	if neg {
		c.Neg(c)
	}
	exp := e - int64(len(digits))
	if exp > maxExp || exp < -maxExp {
		return Dec{}, fmt.Errorf("decimal exponent out of range")
	}
	return canon(c, int32(exp)), nil
}

// rescale returns a's coefficient at exponent -scale, with the digits
// beyond the scale in rem (rem's magnitude is below one unit of the
// scale); both carry a's sign.
func rescale(a Dec, scale int32) (q, rem, unit *big.Int) {
	c := new(big.Int).Set(coeffOf(a))
	exp := a.exp
	unit = big.NewInt(1)
	if exp > -scale {
		for ; exp > -scale; exp-- {
			c.Mul(c, ten)
		}
		return c, new(big.Int), unit
	}
	for ; exp < -scale; exp++ {
		unit.Mul(unit, ten)
	}
	q, rem = new(big.Int).QuoRem(c, unit, new(big.Int))
	return q, rem, unit
}

// Truncate drops the digits beyond the given scale (toward zero).
func Truncate(a Dec, scale int32) Dec {
	q, _, _ := rescale(a, scale)
	return canon(q, -scale)
}

// RoundHalfAway rounds to the given scale with halves away from zero
// (PostgreSQL's round() on numeric).
func RoundHalfAway(a Dec, scale int32) Dec {
	q, rem, unit := rescale(a, scale)
	if rem.Sign() != 0 {
		twice := new(big.Int).Abs(rem)
		twice.Mul(twice, big.NewInt(2))
		if twice.Cmp(unit) >= 0 {
			if rem.Sign() < 0 {
				q.Sub(q, big.NewInt(1))
			} else {
				q.Add(q, big.NewInt(1))
			}
		}
	}
	return canon(q, -scale)
}
