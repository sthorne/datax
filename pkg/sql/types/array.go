package types

import (
	"fmt"
	"strings"
)

// Arrays. An array family is composite: Array | elem<<8, so one Family
// value names "INT8[]" wherever a family flows (a column's type, a
// described output column, a parameter, an expression's inferred
// type). The base value Array alone never names a real type; IsArray
// and Elem take a composite apart. Arrays are one-dimensional (a
// declared INT8[][] is INT8[], as PostgreSQL ignores the dimensions)
// and elements are any non-array scalar family.

// ArrayOf is the family of arrays whose elements are elem.
func ArrayOf(elem Family) Family { return Array | elem<<8 }

// IsArray reports whether f is an array family.
func (f Family) IsArray() bool { return f&0xff == Array && f>>8 != 0 }

// Elem is the element family of an array family (Unknown otherwise).
func (f Family) Elem() Family {
	if !f.IsArray() {
		return Unknown
	}
	return f >> 8
}

// NewArray makes an array datum of elem elements. Elements are stored
// as given; callers coerce them to elem first.
func NewArray(elem Family, elems []Datum) Datum {
	return Datum{Fam: ArrayOf(elem), A: elems}
}

// FormatArray renders elements as a PostgreSQL array literal
// ('{1,2}', '{a,"b c",NULL}'): an element is quoted when it is empty,
// contains a delimiter, brace, quote, backslash or space, or spells
// NULL.
func FormatArray(elems []Datum) string {
	var b strings.Builder
	b.WriteByte('{')
	for i, e := range elems {
		if i > 0 {
			b.WriteByte(',')
		}
		if e.Null {
			b.WriteString("NULL")
			continue
		}
		s := e.Text()
		if s == "" || strings.ContainsAny(s, `,{}" \`) || strings.EqualFold(s, "null") {
			b.WriteByte('"')
			b.WriteString(strings.NewReplacer(`\`, `\\`, `"`, `\"`).Replace(s))
			b.WriteByte('"')
			continue
		}
		b.WriteString(s)
	}
	b.WriteByte('}')
	return b.String()
}

// SplitArrayText splits a PostgreSQL array literal ('{a,"b c",NULL}')
// into its element texts; nulls marks the unquoted NULLs. A literal
// without braces is refused.
func SplitArrayText(s string) (elems []string, nulls []bool, err error) {
	text := strings.TrimSpace(s)
	if len(text) < 2 || text[0] != '{' || text[len(text)-1] != '}' {
		return nil, nil, fmt.Errorf("malformed array literal: %q", s)
	}
	text = text[1 : len(text)-1]
	if strings.TrimSpace(text) == "" {
		return nil, nil, nil
	}
	var cur strings.Builder
	inQuote, quoted, depth := false, false, 0
	flush := func() {
		v := cur.String()
		if !quoted {
			v = strings.TrimSpace(v)
		}
		elems = append(elems, v)
		nulls = append(nulls, !quoted && strings.EqualFold(v, "null"))
		cur.Reset()
		quoted = false
	}
	for i := 0; i < len(text); i++ {
		c := text[i]
		switch {
		case c == '\\' && i+1 < len(text):
			i++
			cur.WriteByte(text[i])
		case c == '"':
			inQuote = !inQuote
			quoted = true
		case !inQuote && c == '{':
			depth++
			cur.WriteByte(c)
		case !inQuote && c == '}':
			depth--
			cur.WriteByte(c)
		case !inQuote && depth == 0 && c == ',':
			flush()
		default:
			cur.WriteByte(c)
		}
	}
	if inQuote || depth != 0 {
		return nil, nil, fmt.Errorf("malformed array literal: %q", s)
	}
	flush()
	return elems, nulls, nil
}

// ParseArray parses an array literal into an array of elem, each
// element coerced from its text.
func ParseArray(s string, elem Family) (Datum, error) {
	texts, nulls, err := SplitArrayText(s)
	if err != nil {
		return Datum{}, err
	}
	out := make([]Datum, len(texts))
	for i, t := range texts {
		if nulls[i] {
			out[i] = DNull
			continue
		}
		d, err := NewString(t).Coerce(elem)
		if err != nil {
			return Datum{}, fmt.Errorf("array element %q: %v", t, err)
		}
		out[i] = d
	}
	return NewArray(elem, out), nil
}

// coerceArray converts an array datum to another array family,
// element by element.
func (d Datum) coerceArray(target Family) (Datum, error) {
	elem := target.Elem()
	out := make([]Datum, len(d.A))
	for i, e := range d.A {
		if e.Null {
			out[i] = DNull
			continue
		}
		c, err := e.Coerce(elem)
		if err != nil {
			return Datum{}, fmt.Errorf("array element %s: %v", e.Text(), err)
		}
		out[i] = c
	}
	return NewArray(elem, out), nil
}

// compareArrays orders arrays element by element, a shorter prefix
// first; a NULL element sorts last (as ORDER BY does by default).
func compareArrays(a, b Datum) (int, error) {
	for i := 0; i < len(a.A) && i < len(b.A); i++ {
		x, y := a.A[i], b.A[i]
		switch {
		case x.Null && y.Null:
			continue
		case x.Null:
			return 1, nil
		case y.Null:
			return -1, nil
		}
		c, err := x.Compare(y)
		if err != nil {
			return 0, err
		}
		if c != 0 {
			return c, nil
		}
	}
	return cmpInt(int64(len(a.A)), int64(len(b.A))), nil
}
