// Package types defines datax's SQL column types and datum values.
package types

import (
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/sthorne/datax/pkg/util/decimal"
)

// Family is a column type.
type Family int

const (
	Unknown     Family = iota
	Int                // INT8
	Float              // FLOAT8
	String             // TEXT
	Bool               // BOOL
	Timestamp          // TIMESTAMPTZ: UTC nanoseconds since the Unix epoch (in I)
	Date               // DATE: days since the Unix epoch (in I)
	Bytes              // BYTES/BYTEA: raw bytes (in S)
	Uuid               // UUID: 16 raw bytes (in S)
	Decimal            // DECIMAL/NUMERIC: canonical decimal string (in S)
	Jsonb              // JSONB: normalized compact JSON text (in S)
	IntervalFam        // INTERVAL: months (Mo), days (Dy) and nanoseconds (I)
	Time               // TIME: nanoseconds since midnight (in I)
	Array              // the base of an array family: ArrayOf(elem) = Array | elem<<8 (elements in A)
	Enum               // a user-defined enum: the label's ordinal (in I) and the label (in S)
)

// The Family values above are ON-DISK FORMAT (JSON-serialized in table
// descriptors): append only, never renumber.

func (f Family) String() string {
	if f.IsArray() {
		return f.Elem().String() + "[]"
	}
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
	case Decimal:
		return "DECIMAL"
	case Jsonb:
		return "JSONB"
	case IntervalFam:
		return "INTERVAL"
	case Time:
		return "TIME"
	case Enum:
		return "ENUM"
	}
	return "UNKNOWN"
}

// NewEnum makes an enum datum: the label's ordinal in its type and
// the label itself. Ordinals order; labels render and compare with
// text.
func NewEnum(ordinal int64, label string) Datum { return Datum{Fam: Enum, I: ordinal, S: label} }

// ParseType resolves a SQL type name (with PostgreSQL-flavored aliases).
func ParseType(name string) (Family, error) {
	// INT8[] / INT8[][] / INT8 ARRAY: an array of the element type.
	if trimmed := strings.TrimSpace(name); strings.HasSuffix(trimmed, "]") || strings.HasSuffix(strings.ToUpper(trimmed), " ARRAY") {
		base := trimmed
		if strings.HasSuffix(strings.ToUpper(base), " ARRAY") {
			base = base[:len(base)-6]
		}
		for strings.HasSuffix(base, "]") {
			i := strings.LastIndexByte(base, '[')
			if i < 0 {
				return Unknown, fmt.Errorf("unsupported type %q", name)
			}
			base = strings.TrimSpace(base[:i])
		}
		elem, err := ParseType(base)
		if err != nil {
			return Unknown, err
		}
		if elem.IsArray() {
			return Unknown, fmt.Errorf("unsupported type %q", name)
		}
		return ArrayOf(elem), nil
	}
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
	case "DECIMAL", "NUMERIC", "DEC":
		return Decimal, nil
	case "JSONB", "JSON":
		return Jsonb, nil
	case "INTERVAL":
		return IntervalFam, nil
	case "TIME":
		return Time, nil
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
	// Dscale is DISPLAY-ONLY: a Decimal datum read from (or written to) a
	// DECIMAL(p,s) column carries the declared scale so Text() renders
	// fixed-scale ("9.90", not "9.9"). Identity is untouched: S keeps the
	// canonical (trailing-zero-stripped) text that equality, grouping, and
	// storage compare, and Dscale never participates in Compare.
	Dscale int32 `json:"dscale,omitempty"`
	// NoTZ is DISPLAY-ONLY: a Timestamp datum read from (or written to)
	// a TIMESTAMP (without time zone) column renders without the UTC
	// offset ("2026-08-30 01:02:03", not "...+00"). The value is the same
	// UTC wall-clock nanosecond count either way; Compare ignores it.
	NoTZ bool `json:"notz,omitempty"`
	// Pad is DISPLAY-ONLY: a String datum read from (or written to) a
	// CHAR(n) column renders blank-padded to n characters. S keeps the
	// trailing-space-trimmed text that equality, grouping and storage
	// compare.
	Pad int32 `json:"pad,omitempty"`
	// Mo and Dy are an INTERVAL's months and days (its clock part is in
	// I, as nanoseconds).
	Mo int64 `json:"mo,omitempty"`
	Dy int64 `json:"dy,omitempty"`
	// A holds an array's elements (Fam is ArrayOf(elem)); a NULL element
	// is a Null datum.
	A []Datum `json:"a,omitempty"`
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

// NewDecimal holds a decimal's CANONICAL string form (callers must
// canonicalize via ParseDecimal/decimal.Dec.String — grouping, memo keys,
// and equality compare the text).
func NewDecimal(canonical string) Datum { return Datum{Fam: Decimal, S: canonical} }

// NewJsonb holds NORMALIZED compact JSON text (callers must normalize via
// ParseJSONB — sorted object keys, no insignificant whitespace).
func NewJsonb(normalized string) Datum { return Datum{Fam: Jsonb, S: normalized} }

// timestampFormats are the accepted TIMESTAMPTZ input layouts (a bare
// timestamp is UTC).
var timestampFormats = []string{
	time.RFC3339Nano,
	"2006-01-02 15:04:05.999999999Z07:00",
	"2006-01-02 15:04:05.999999999Z07",
	"2006-01-02 15:04:05.999999999",
	"2006-01-02T15:04:05.999999999",
	"2006-01-02 15:04Z07:00",
	"2006-01-02 15:04",
	"2006-01-02T15:04",
	"2006-01-02",
}

// Timestamps are Unix nanoseconds in an int64, which spans the years
// 1678 to 2262; values outside are refused (ErrTimestampRange) rather
// than wrapped.
var (
	minTimestamp = time.Date(1678, 1, 1, 0, 0, 0, 0, time.UTC)
	maxTimestamp = time.Date(2262, 1, 1, 0, 0, 0, 0, time.UTC)
	// ErrTimestampRange wraps a parse of a timestamp outside the
	// representable years.
	ErrTimestampRange = errors.New("timestamp out of range")
)

// ParseTimestamp parses a timestamp string to UTC Unix nanoseconds.
func ParseTimestamp(s string) (int64, error) {
	for _, f := range timestampFormats {
		if t, err := time.ParseInLocation(f, s, time.UTC); err == nil {
			if t.Before(minTimestamp) || !t.Before(maxTimestamp) {
				return 0, fmt.Errorf("%w: %q (1678-01-01 to 2261-12-31)", ErrTimestampRange, s)
			}
			return t.UTC().UnixNano(), nil
		}
	}
	return 0, fmt.Errorf("could not parse %q as TIMESTAMPTZ", s)
}

// ParseTimestampNoTZ parses a TIMESTAMP (without time zone) input: the
// same layouts as ParseTimestamp, but an offset in the text is ignored
// — the wall-clock fields are taken as they stand, as PostgreSQL does
// for timestamp without time zone. Returns UTC wall-clock nanoseconds.
func ParseTimestampNoTZ(s string) (int64, error) {
	for _, f := range timestampFormats {
		if t, err := time.ParseInLocation(f, s, time.UTC); err == nil {
			wall := time.Date(t.Year(), t.Month(), t.Day(), t.Hour(), t.Minute(), t.Second(), t.Nanosecond(), time.UTC)
			if wall.Before(minTimestamp) || !wall.Before(maxTimestamp) {
				return 0, fmt.Errorf("%w: %q (1678-01-01 to 2261-12-31)", ErrTimestampRange, s)
			}
			return wall.UnixNano(), nil
		}
	}
	return 0, fmt.Errorf("could not parse %q as TIMESTAMP", s)
}

// RoundTimestamp rounds UTC nanoseconds to p fractional second digits
// (0 ≤ p ≤ 9; half away from zero, as PostgreSQL rounds a TIMESTAMP(p)
// input).
func RoundTimestamp(nanos int64, p int32) int64 {
	if p < 0 || p >= 9 {
		return nanos
	}
	unit := int64(1)
	for i := int32(0); i < 9-p; i++ {
		unit *= 10
	}
	half := unit / 2
	if nanos >= 0 {
		return (nanos + half) / unit * unit
	}
	return -((-nanos + half) / unit * unit)
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

// ParseDecimal parses and canonicalizes a decimal literal.
func ParseDecimal(s string) (Datum, error) {
	d, err := decimal.Parse(s)
	if err != nil {
		return Datum{}, fmt.Errorf("could not parse %q as DECIMAL", s)
	}
	return NewDecimal(d.String()), nil
}

// ParseJSONB parses and normalizes a JSON document: objects re-marshal
// with sorted keys (duplicate keys: last wins), whitespace is dropped,
// and HTML characters are NOT escaped. Numbers decode as json.Number, so
// they keep their exact ingest text — integers beyond float64 survive.
func ParseJSONB(s string) (Datum, error) {
	var v any
	dec := json.NewDecoder(strings.NewReader(s))
	dec.UseNumber()
	if err := dec.Decode(&v); err != nil {
		return Datum{}, fmt.Errorf("could not parse value as JSONB: %v", err)
	}
	// Trailing garbage after the document is an error.
	if dec.More() {
		return Datum{}, fmt.Errorf("could not parse value as JSONB: trailing content")
	}
	var b strings.Builder
	enc := json.NewEncoder(&b)
	enc.SetEscapeHTML(false)
	if err := enc.Encode(v); err != nil {
		return Datum{}, fmt.Errorf("could not normalize JSONB: %v", err)
	}
	return NewJsonb(strings.TrimSuffix(b.String(), "\n")), nil
}

// IsIndexable reports whether the family has an order-preserving key
// encoding (usable in primary keys and indexes).
func IsIndexable(f Family) bool { return f != Jsonb && f != Unknown && !f.IsArray() }

// DecimalVal parses the canonical form back out of a Decimal datum.
func (d Datum) DecimalVal() (decimal.Dec, error) {
	return decimal.Parse(d.S)
}

// Coerce converts d to the target family (e.g. an int literal into a FLOAT8
// column), or errors when the conversion is lossy/invalid.
// ConvertTo is Coerce plus the rendering conversion to TEXT (any value's
// canonical text) — the cast ALTER COLUMN TYPE applies.
func (d Datum) ConvertTo(target Family) (Datum, error) {
	if !d.Null && target == String && d.Fam != String {
		return NewString(d.Text()), nil
	}
	return d.Coerce(target)
}

func (d Datum) Coerce(target Family) (Datum, error) {
	if d.Null {
		return DNull, nil
	}
	if d.Fam == target {
		return d, nil
	}
	if target.IsArray() {
		switch {
		case d.Fam.IsArray():
			return d.coerceArray(target)
		case d.Fam == String:
			return ParseArray(d.S, target.Elem())
		}
		return Datum{}, fmt.Errorf("cannot use %s value as %s", d.Fam, target)
	}
	if d.Fam.IsArray() && target == String {
		return NewString(d.Text()), nil
	}
	if d.Fam == Enum && target == String {
		return NewString(d.S), nil
	}
	if target == Enum {
		// Only the column (with its labels) can make an enum value;
		// text stays text for the write path to convert.
		return Datum{}, fmt.Errorf("cannot use %s value as an enum without its type", d.Fam)
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
	case d.Fam == String && target == Decimal:
		return ParseDecimal(d.S)
	case d.Fam == Int && target == Decimal:
		return NewDecimal(decimal.FromInt(d.I).String()), nil
	case d.Fam == Decimal && target == Float:
		// Lossy narrowing is allowed on demand (the reverse is not:
		// Float→Decimal would launder binary rounding into "exact").
		v, err := d.DecimalVal()
		if err != nil {
			return Datum{}, err
		}
		return NewFloat(v.Float64()), nil
	case d.Fam == String && target == Jsonb:
		return ParseJSONB(d.S)
	case d.Fam == String && target == IntervalFam:
		iv, err := ParseInterval(d.S)
		if err != nil {
			return Datum{}, err
		}
		return NewInterval(iv), nil
	case d.Fam == String && target == Time:
		n, err := ParseTime(d.S)
		if err != nil {
			return Datum{}, err
		}
		return NewTime(n), nil
	case d.Fam == Timestamp && target == Time:
		t := time.Unix(0, d.I).UTC()
		return NewTime(int64(t.Hour())*int64(time.Hour) + int64(t.Minute())*int64(time.Minute) + int64(t.Second())*int64(time.Second) + int64(t.Nanosecond())), nil
	case d.Fam == IntervalFam && target == Time:
		// PostgreSQL's interval → time cast keeps the clock part modulo
		// a day.
		n := d.I % NanosPerDay
		if n < 0 {
			n += NanosPerDay
		}
		return NewTime(n), nil
	case d.Fam == Time && target == IntervalFam:
		return NewInterval(Interval{Nanos: d.I}), nil
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
	// Decimal cross-compares exactly with Int, and with Float by lifting
	// the float through its shortest decimal rendering.
	if d.Fam == Decimal || o.Fam == Decimal {
		if (d.Fam == Decimal || d.Fam == Int || d.Fam == Float) &&
			(o.Fam == Decimal || o.Fam == Int || o.Fam == Float) {
			dv, err := d.liftDecimal()
			if err != nil {
				return 0, err
			}
			ov, err := o.liftDecimal()
			if err != nil {
				return 0, err
			}
			return decimal.Cmp(dv, ov), nil
		}
	}
	if d.Fam.IsArray() || o.Fam.IsArray() {
		// An array compares with an array (a text literal coerces to
		// the other side's family) element by element.
		if !d.Fam.IsArray() {
			c, err := d.Coerce(o.Fam)
			if err != nil {
				return 0, err
			}
			d = c
		} else if o.Fam != d.Fam {
			c, err := o.Coerce(d.Fam)
			if err != nil {
				return 0, err
			}
			o = c
		}
		return compareArrays(d, o)
	}
	if (d.Fam == Enum && o.Fam == String) || (d.Fam == String && o.Fam == Enum) {
		// An enum against text: by label (equality is exact; an order
		// between the two is the labels' text order).
		return strings.Compare(d.S, o.S), nil
	}
	if d.Fam != o.Fam {
		return 0, fmt.Errorf("cannot compare %s with %s", d.Fam, o.Fam)
	}
	switch d.Fam {
	case Enum:
		return cmpInt(d.I, o.I), nil
	case Int, Timestamp, Date, Time:
		return cmpInt(d.I, o.I), nil
	case IntervalFam:
		return cmpInt(d.IntervalVal().CmpValue(), o.IntervalVal().CmpValue()), nil
	case Float:
		return cmpFloat(d.F, o.F), nil
	case String, Bytes, Uuid:
		return strings.Compare(d.S, o.S), nil
	case Bool:
		return cmpBool(d.B, o.B), nil
	case Decimal:
		return 0, fmt.Errorf("unreachable") // handled above
	case Jsonb:
		// Equality by canonical text; no defined ordering.
		if d.S == o.S {
			return 0, nil
		}
		return 0, fmt.Errorf("JSONB values have no order")
	}
	return 0, fmt.Errorf("cannot compare %s values", d.Fam)
}

// liftDecimal lifts a numeric datum to decimal (float via its shortest
// decimal rendering; NaN/Inf reject).
func (d Datum) liftDecimal() (decimal.Dec, error) {
	switch d.Fam {
	case Decimal:
		return d.DecimalVal()
	case Int:
		return decimal.FromInt(d.I), nil
	case Float:
		if math.IsNaN(d.F) || math.IsInf(d.F, 0) {
			return decimal.Dec{}, fmt.Errorf("cannot compare DECIMAL with non-finite FLOAT8")
		}
		return decimal.Parse(strconv.FormatFloat(d.F, 'g', -1, 64))
	}
	return decimal.Dec{}, fmt.Errorf("cannot lift %s to DECIMAL", d.Fam)
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
// FormatTimestamp renders UTC nanoseconds in PostgreSQL's text form,
// with the "+00" offset (TIMESTAMPTZ) or without it (TIMESTAMP).
func FormatTimestamp(nanos int64, noTZ bool) string {
	if noTZ {
		return time.Unix(0, nanos).UTC().Format("2006-01-02 15:04:05.999999999")
	}
	return time.Unix(0, nanos).UTC().Format("2006-01-02 15:04:05.999999999-07")
}

// FormatTimestampIn renders UTC nanoseconds in loc, PostgreSQL style:
// the offset as "-05" for whole hours, "+05:30" otherwise.
func FormatTimestampIn(nanos int64, loc *time.Location) string {
	t := time.Unix(0, nanos).In(loc)
	s := t.Format("2006-01-02 15:04:05.999999999-07:00")
	if strings.HasSuffix(s, ":00") {
		s = s[:len(s)-3]
	}
	return s
}

// PadTo blank-pads s to n characters (CHAR(n) output); a longer s is
// returned as it is.
func PadTo(s string, n int32) string {
	if c := int32(utf8.RuneCountInString(s)); c < n {
		return s + strings.Repeat(" ", int(n-c))
	}
	return s
}

func (d Datum) Text() string {
	if d.Null {
		return "" // callers render NULL specially (wire: -1 length)
	}
	if d.Fam.IsArray() {
		return FormatArray(d.A)
	}
	switch d.Fam {
	case Int:
		return strconv.FormatInt(d.I, 10)
	case Float:
		return strconv.FormatFloat(d.F, 'g', -1, 64)
	case String:
		if d.Pad > 0 {
			return PadTo(d.S, d.Pad)
		}
		return d.S
	case Bool:
		if d.B {
			return "t"
		}
		return "f"
	case Timestamp:
		// PostgreSQL text format: "2026-08-30 01:02:03.456+00"; without
		// the offset for a TIMESTAMP (without time zone) value.
		return FormatTimestamp(d.I, d.NoTZ)
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
	case Decimal:
		if d.Dscale > 0 {
			return padDecimalScale(d.S, d.Dscale)
		}
		return d.S // canonical text IS the wire text
	case Jsonb:
		return d.S // normalized text IS the wire text
	case IntervalFam:
		return d.IntervalVal().String()
	case Time:
		return FormatClock(d.I)
	case Enum:
		return d.S
	}
	return ""
}

// padDecimalScale renders canonical decimal text with exactly scale
// fraction digits (canonical text never has MORE than the declared scale
// — writes quantize — so this only pads, never truncates).
func padDecimalScale(s string, scale int32) string {
	frac := 0
	if i := strings.IndexByte(s, '.'); i >= 0 {
		frac = len(s) - i - 1
	} else {
		s += "."
	}
	for ; frac < int(scale); frac++ {
		s += "0"
	}
	return s
}
