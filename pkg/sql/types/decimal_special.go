package types

import (
	"math"
	"strings"
)

// numeric's special values (PostgreSQL 14): NaN, Infinity and
// -Infinity. A Decimal datum holds one as its canonical text in S, as
// it holds a finite value; the decimal package itself knows only
// finite values, so every arithmetic and conversion path checks
// DecimalSpecial first.
const (
	DecimalNaN    = "NaN"
	DecimalPosInf = "Infinity"
	DecimalNegInf = "-Infinity"
)

// parseDecimalSpecial reads numeric input's special spellings, in any
// letter case: NaN, [+-]Infinity and [+-]inf.
func parseDecimalSpecial(s string) (string, bool) {
	switch strings.ToLower(strings.TrimSpace(s)) {
	case "nan":
		return DecimalNaN, true
	case "infinity", "+infinity", "inf", "+inf":
		return DecimalPosInf, true
	case "-infinity", "-inf":
		return DecimalNegInf, true
	}
	return "", false
}

// DecimalSpecial reports whether d is a numeric NaN or infinity, and
// which, as the float64 of the same meaning.
func DecimalSpecial(d Datum) (float64, bool) {
	if d.Null || d.Fam != Decimal {
		return 0, false
	}
	switch d.S {
	case DecimalNaN:
		return math.NaN(), true
	case DecimalPosInf:
		return math.Inf(1), true
	case DecimalNegInf:
		return math.Inf(-1), true
	}
	return 0, false
}

// DecimalFromSpecial is the numeric NaN or infinity of a float64 that
// is one (ok false for a finite float).
func DecimalFromSpecial(f float64) (Datum, bool) {
	switch {
	case math.IsNaN(f):
		return NewDecimal(DecimalNaN), true
	case math.IsInf(f, 1):
		return NewDecimal(DecimalPosInf), true
	case math.IsInf(f, -1):
		return NewDecimal(DecimalNegInf), true
	}
	return Datum{}, false
}

// specialRank orders numeric values as PostgreSQL does: -Infinity,
// the finite values, Infinity, then NaN (equal to itself). A Float
// operand ranks the same way, an Int as finite.
func specialRank(d Datum) int {
	f := 0.0
	switch d.Fam {
	case Decimal:
		v, ok := DecimalSpecial(d)
		if !ok {
			return 0
		}
		f = v
	case Float:
		f = d.F
	default:
		return 0
	}
	switch {
	case math.IsNaN(f):
		return 2
	case math.IsInf(f, 1):
		return 1
	case math.IsInf(f, -1):
		return -1
	}
	return 0
}

// ErrDecimalDivByZero is numeric division (or modulo) by zero.
var ErrDecimalDivByZero = &decimalError{"division by zero"}

type decimalError struct{ msg string }

func (e *decimalError) Error() string { return e.msg }

// DecimalSpecialArith evaluates l op r (+, -, *, /, %) when a numeric
// operand is NaN or an infinity, by PostgreSQL's rules: NaN in, NaN
// out (NaN / 0 included); Infinity - Infinity, Infinity × 0,
// Infinity / Infinity and Infinity % x are NaN; x / Infinity is 0 and
// x % Infinity is x; an infinity divided by zero is still an error.
// ok is false when neither operand is special.
func DecimalSpecialArith(l Datum, op string, r Datum) (Datum, bool, error) {
	ls, lok := DecimalSpecial(l)
	rs, rok := DecimalSpecial(r)
	if !lok && !rok {
		return Datum{}, false, nil
	}
	lf, rf := ls, rs
	if !lok {
		lf = finiteFloat(l)
	}
	if !rok {
		rf = finiteFloat(r)
	}
	nan := NewDecimal(DecimalNaN)
	if math.IsNaN(lf) || math.IsNaN(rf) {
		return nan, true, nil
	}
	var out float64
	switch op {
	case "+":
		out = lf + rf
	case "-":
		out = lf - rf
	case "*":
		out = lf * rf
	case "/":
		if rf == 0 {
			return Datum{}, true, ErrDecimalDivByZero
		}
		out = lf / rf
	case "%":
		if rf == 0 {
			return Datum{}, true, ErrDecimalDivByZero
		}
		if lok {
			return nan, true, nil
		}
		return l, true, nil // finite % infinity
	default:
		return Datum{}, false, nil
	}
	if d, ok := DecimalFromSpecial(out); ok {
		return d, true, nil
	}
	return NewDecimal("0"), true, nil
}

// finiteFloat is a finite numeric operand's float value: enough for
// its sign and whether it is zero, which is all the special rules
// read.
func finiteFloat(d Datum) float64 {
	switch d.Fam {
	case Int:
		return float64(d.I)
	case Float:
		return d.F
	case Decimal:
		if v, err := d.DecimalVal(); err == nil {
			return float64(v.Sign())
		}
	}
	return 0
}
