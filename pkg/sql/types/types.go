// Package types defines datax's SQL column types and datum values.
package types

import (
	"fmt"
	"strconv"
	"strings"
)

// Family is a column type.
type Family int

const (
	Unknown Family = iota
	Int            // INT8
	Float          // FLOAT8
	String         // TEXT
	Bool           // BOOL
)

func (f Family) String() string {
	switch f {
	case Int:
		return "INT8"
	case Float:
		return "FLOAT8"
	case String:
		return "TEXT"
	case Bool:
		return "BOOL"
	}
	return "UNKNOWN"
}

// ParseType resolves a SQL type name (with PostgreSQL-flavored aliases).
func ParseType(name string) (Family, error) {
	switch strings.ToUpper(name) {
	case "INT8", "INT", "INTEGER", "BIGINT", "INT4", "SMALLINT", "INT2":
		return Int, nil
	case "FLOAT8", "FLOAT", "DOUBLE", "REAL", "FLOAT4":
		return Float, nil
	case "TEXT", "STRING", "VARCHAR", "CHAR", "CHARACTER":
		return String, nil
	case "BOOL", "BOOLEAN":
		return Bool, nil
	}
	return Unknown, fmt.Errorf("unsupported type %q", name)
}

// Datum is a SQL value.
type Datum struct {
	Null bool    `json:"null,omitempty"`
	Fam  Family  `json:"fam,omitempty"`
	I    int64   `json:"i,omitempty"`
	F    float64 `json:"f,omitempty"`
	S    string  `json:"s,omitempty"`
	B    bool    `json:"b,omitempty"`
}

var DNull = Datum{Null: true}

func NewInt(v int64) Datum     { return Datum{Fam: Int, I: v} }
func NewFloat(v float64) Datum { return Datum{Fam: Float, F: v} }
func NewString(v string) Datum { return Datum{Fam: String, S: v} }
func NewBool(v bool) Datum     { return Datum{Fam: Bool, B: v} }

// Coerce converts d to the target family (e.g. an int literal into a FLOAT8
// column), or errors when the conversion is lossy/invalid.
func (d Datum) Coerce(target Family) (Datum, error) {
	if d.Null {
		return DNull, nil
	}
	if d.Fam == target {
		return d, nil
	}
	switch {
	case d.Fam == Int && target == Float:
		return NewFloat(float64(d.I)), nil
	case d.Fam == String && target == Int:
		i, err := strconv.ParseInt(d.S, 10, 64)
		if err != nil {
			return Datum{}, fmt.Errorf("could not parse %q as INT8", d.S)
		}
		return NewInt(i), nil
	case d.Fam == String && target == Float:
		f, err := strconv.ParseFloat(d.S, 64)
		if err != nil {
			return Datum{}, fmt.Errorf("could not parse %q as FLOAT8", d.S)
		}
		return NewFloat(f), nil
	case d.Fam == String && target == Bool:
		b, err := strconv.ParseBool(strings.ToLower(d.S))
		if err != nil {
			return Datum{}, fmt.Errorf("could not parse %q as BOOL", d.S)
		}
		return NewBool(b), nil
	}
	return Datum{}, fmt.Errorf("cannot use %s value as %s", d.Fam, target)
}

// Compare returns -1/0/1; comparing with NULL or across incomparable types
// returns an error (callers treat NULL comparisons as "no match").
func (d Datum) Compare(o Datum) (int, error) {
	if d.Null || o.Null {
		return 0, fmt.Errorf("NULL comparison")
	}
	if d.Fam == Int && o.Fam == Float {
		return cmpFloat(float64(d.I), o.F), nil
	}
	if d.Fam == Float && o.Fam == Int {
		return cmpFloat(d.F, float64(o.I)), nil
	}
	if d.Fam != o.Fam {
		return 0, fmt.Errorf("cannot compare %s with %s", d.Fam, o.Fam)
	}
	switch d.Fam {
	case Int:
		return cmpInt(d.I, o.I), nil
	case Float:
		return cmpFloat(d.F, o.F), nil
	case String:
		return strings.Compare(d.S, o.S), nil
	case Bool:
		return cmpBool(d.B, o.B), nil
	}
	return 0, fmt.Errorf("cannot compare %s values", d.Fam)
}

func cmpInt(a, b int64) int {
	switch {
	case a < b:
		return -1
	case a > b:
		return 1
	}
	return 0
}

func cmpFloat(a, b float64) int {
	switch {
	case a < b:
		return -1
	case a > b:
		return 1
	}
	return 0
}

func cmpBool(a, b bool) int {
	switch {
	case !a && b:
		return -1
	case a && !b:
		return 1
	}
	return 0
}

// Text renders the datum in PostgreSQL text format.
func (d Datum) Text() string {
	if d.Null {
		return "" // callers render NULL specially (wire: -1 length)
	}
	switch d.Fam {
	case Int:
		return strconv.FormatInt(d.I, 10)
	case Float:
		return strconv.FormatFloat(d.F, 'g', -1, 64)
	case String:
		return d.S
	case Bool:
		if d.B {
			return "t"
		}
		return "f"
	}
	return ""
}
