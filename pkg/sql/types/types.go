// Package types defines datax's SQL column types and datum values.
package types

import (
	"encoding/hex"
	"fmt"
	"strconv"
	"strings"
	"time"
)

// Family is a column type.
type Family int

const (
	Unknown   Family = iota
	Int              // INT8
	Float            // FLOAT8
	String           // TEXT
	Bool             // BOOL
	Timestamp        // TIMESTAMPTZ: UTC nanoseconds since the Unix epoch (in I)
	Date             // DATE: days since the Unix epoch (in I)
	Bytes            // BYTES/BYTEA: raw bytes (in S)
	Uuid             // UUID: 16 raw bytes (in S)
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
	case Timestamp:
		return "TIMESTAMPTZ"
	case Date:
		return "DATE"
	case Bytes:
		return "BYTES"
	case Uuid:
		return "UUID"
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
	case "TIMESTAMPTZ", "TIMESTAMP":
		return Timestamp, nil
	case "DATE":
		return Date, nil
	case "BYTES", "BYTEA", "BLOB":
		return Bytes, nil
	case "UUID":
		return Uuid, nil
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

// NewTimestamp is UTC nanoseconds since the Unix epoch.
func NewTimestamp(nanos int64) Datum { return Datum{Fam: Timestamp, I: nanos} }

// NewDate is days since the Unix epoch.
func NewDate(days int64) Datum { return Datum{Fam: Date, I: days} }

// NewBytes holds raw bytes (stored in S).
func NewBytes(v []byte) Datum { return Datum{Fam: Bytes, S: string(v)} }

// NewUUID holds the 16 raw bytes of a UUID (stored in S).
func NewUUID(v [16]byte) Datum { return Datum{Fam: Uuid, S: string(v[:])} }

// timestampFormats are the accepted TIMESTAMPTZ input layouts (a bare
// timestamp is UTC).
var timestampFormats = []string{
	time.RFC3339Nano,
	"2006-01-02 15:04:05.999999999Z07:00",
	"2006-01-02 15:04:05.999999999Z07",
	"2006-01-02 15:04:05.999999999",
	"2006-01-02T15:04:05.999999999",
	"2006-01-02",
}

// ParseTimestamp parses a timestamp string to UTC Unix nanoseconds.
func ParseTimestamp(s string) (int64, error) {
	for _, f := range timestampFormats {
		if t, err := time.ParseInLocation(f, s, time.UTC); err == nil {
			return t.UTC().UnixNano(), nil
		}
	}
	return 0, fmt.Errorf("could not parse %q as TIMESTAMPTZ", s)
}

// ParseDate parses a YYYY-MM-DD date string to days since the Unix epoch.
func ParseDate(s string) (int64, error) {
	t, err := time.ParseInLocation("2006-01-02", s, time.UTC)
	if err != nil {
		return 0, fmt.Errorf("could not parse %q as DATE", s)
	}
	return t.Unix() / 86400, nil
}

// ParseBytes parses PostgreSQL bytea text input: \x-prefixed hex, or the
// raw string bytes otherwise.
func ParseBytes(s string) ([]byte, error) {
	if strings.HasPrefix(s, `\x`) {
		b, err := hex.DecodeString(s[2:])
		if err != nil {
			return nil, fmt.Errorf("could not parse %q as BYTES", s)
		}
		return b, nil
	}
	return []byte(s), nil
}

// ParseUUID parses the canonical 8-4-4-4-12 form (dashes optional).
func ParseUUID(s string) ([16]byte, error) {
	var out [16]byte
	h := strings.ReplaceAll(strings.ToLower(s), "-", "")
	if len(h) != 32 {
		return out, fmt.Errorf("could not parse %q as UUID", s)
	}
	b, err := hex.DecodeString(h)
	if err != nil {
		return out, fmt.Errorf("could not parse %q as UUID", s)
	}
	copy(out[:], b)
	return out, nil
}

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
	case d.Fam == String && target == Timestamp:
		n, err := ParseTimestamp(d.S)
		if err != nil {
			return Datum{}, err
		}
		return NewTimestamp(n), nil
	case d.Fam == String && target == Date:
		days, err := ParseDate(d.S)
		if err != nil {
			return Datum{}, err
		}
		return NewDate(days), nil
	case d.Fam == Date && target == Timestamp:
		return NewTimestamp(d.I * 86400 * 1e9), nil
	case d.Fam == String && target == Bytes:
		b, err := ParseBytes(d.S)
		if err != nil {
			return Datum{}, err
		}
		return NewBytes(b), nil
	case d.Fam == String && target == Uuid:
		u, err := ParseUUID(d.S)
		if err != nil {
			return Datum{}, err
		}
		return NewUUID(u), nil
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
	case Int, Timestamp, Date:
		return cmpInt(d.I, o.I), nil
	case Float:
		return cmpFloat(d.F, o.F), nil
	case String, Bytes, Uuid:
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
	case Timestamp:
		// PostgreSQL text format: "2026-08-30 01:02:03.456+00".
		return time.Unix(0, d.I).UTC().Format("2006-01-02 15:04:05.999999999-07")
	case Date:
		return time.Unix(d.I*86400, 0).UTC().Format("2006-01-02")
	case Bytes:
		return `\x` + hex.EncodeToString([]byte(d.S))
	case Uuid:
		b := []byte(d.S)
		if len(b) != 16 {
			return ""
		}
		h := hex.EncodeToString(b)
		return h[:8] + "-" + h[8:12] + "-" + h[12:16] + "-" + h[16:20] + "-" + h[20:]
	}
	return ""
}
