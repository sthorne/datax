package inverted

import (
	"encoding/json"
	"fmt"
	"strings"

	"github.com/sthorne/datax/pkg/sql/builtins"
	"github.com/sthorne/datax/pkg/sql/types"
	"github.com/sthorne/datax/pkg/util/decimal"
	"github.com/sthorne/datax/pkg/util/encoding"
)

// JSONBOps is the name of the JSONB kind, after PostgreSQL's default GIN
// operator class for jsonb.
const JSONBOps = "jsonb_ops"

// The JSONB kind. A document has two sorts of element.
//
// A PATH element is one scalar of the document together with the way to
// it:
//
//	0x02 | step... | value
//
//	step   0x10 + EncodeString(key)   into an object, by key
//	       0x11                       into an array (any position)
//	value  0x20                       null
//	       0x21 / 0x22                false / true
//	       0x23 + EncodeDecimal(n)    a number
//	       0x24 + the number's text   a number the decimal type refuses
//	       0x25 + the string's bytes  a string (to the end of the element)
//
// An array step carries no position, because containment ignores
// positions and duplicates: [1,2,1] and [2,1] have the same elements.
// The value's type is a tag of its own, so the string "1", the number 1
// and true can never meet. Numbers use the order-preserving decimal
// encoding, which gives 1, 1.0 and 1e0 one encoding — containment
// compares numbers numerically — and keeps the numbers under one path in
// numeric order. Step tags and value tags are disjoint and keys are
// self-delimiting, so an element reads back unambiguously and elements
// sort by path, then by type, then by value.
//
// Empty objects and arrays have no path element: a query that asks for
// one ('{"a": {}}') asks only that something of that shape be there,
// which no single element can answer.
//
// A KEY element is one string that the ? operator finds at the top
// level: an object key, a string element of a top-level array, or the
// document itself when it is a string:
//
//	0x01 | the string's bytes
//
// It exists because a key whose value is an object or an array would
// otherwise be found only by scanning every path under it.
//
// Elements longer than MaxElementLen are stored as a digest (Clamp), so
// those two properties — reading back and ordering — hold for short
// elements only; lookups by exact element, the only kind there is today,
// work for all.
const (
	jsonKeyTag  byte = 0x01
	jsonPathTag byte = 0x02

	jsonStepKey   byte = 0x10
	jsonStepArray byte = 0x11

	jsonNull    byte = 0x20
	jsonFalse   byte = 0x21
	jsonTrue    byte = 0x22
	jsonNumber  byte = 0x23
	jsonNumText byte = 0x24
	jsonString  byte = 0x25
)

type jsonbKind struct{}

func init() { Register(jsonbKind{}) }

func (jsonbKind) Name() string         { return JSONBOps }
func (jsonbKind) Method() string       { return "gin" }
func (jsonbKind) Family() types.Family { return types.Jsonb }

func decodeJSON(s string) (any, error) {
	var v any
	dec := json.NewDecoder(strings.NewReader(s))
	dec.UseNumber()
	if err := dec.Decode(&v); err != nil {
		return nil, fmt.Errorf("malformed jsonb: %v", err)
	}
	return v, nil
}

// appendJSONScalar appends a scalar's value encoding; ok is false for a
// container.
func appendJSONScalar(b []byte, v any) ([]byte, bool) {
	switch x := v.(type) {
	case nil:
		return append(b, jsonNull), true
	case bool:
		if x {
			return append(b, jsonTrue), true
		}
		return append(b, jsonFalse), true
	case json.Number:
		d, err := decimal.Parse(string(x))
		if err != nil {
			// Containment falls back to comparing the text of a number it
			// cannot parse; so does the element.
			return append(append(b, jsonNumText), x...), true
		}
		return encoding.EncodeDecimal(append(b, jsonNumber), d), true
	case string:
		return append(append(b, jsonString), x...), true
	}
	return b, false
}

func jsonKeyElement(key string) []byte {
	return Clamp(append([]byte{jsonKeyTag}, key...))
}

// Elements lists the document's elements.
func (jsonbKind) Elements(d types.Datum) ([][]byte, error) {
	if d.Null {
		return nil, nil
	}
	v, err := decodeJSON(d.S)
	if err != nil {
		return nil, err
	}
	// Distinct elements are what is bounded and what is stored: a long
	// array of one repeated value is one element.
	seen := map[string]struct{}{}
	var out [][]byte
	add := func(el []byte) error {
		if _, dup := seen[string(el)]; dup {
			return nil
		}
		if len(out) == MaxElementsPerValue {
			return fmt.Errorf("document has more than %d distinct indexable elements", MaxElementsPerValue)
		}
		seen[string(el)] = struct{}{}
		out = append(out, el)
		return nil
	}
	switch x := v.(type) {
	case map[string]any:
		for k := range x {
			if err := add(jsonKeyElement(k)); err != nil {
				return nil, err
			}
		}
	case []any:
		for _, e := range x {
			if s, ok := e.(string); ok {
				if err := add(jsonKeyElement(s)); err != nil {
					return nil, err
				}
			}
		}
	case string:
		if err := add(jsonKeyElement(x)); err != nil {
			return nil, err
		}
	}
	var walk func(path []byte, v any) error
	walk = func(path []byte, v any) error {
		switch x := v.(type) {
		case map[string]any:
			for k, e := range x {
				// The three-index slice keeps sibling keys from writing
				// over one another's path.
				p := encoding.EncodeString(append(path[:len(path):len(path)], jsonStepKey), k)
				if err := walk(p, e); err != nil {
					return err
				}
			}
		case []any:
			p := append(path[:len(path):len(path)], jsonStepArray)
			for _, e := range x {
				if err := walk(p, e); err != nil {
					return err
				}
			}
		default:
			el, _ := appendJSONScalar(path[:len(path):len(path)], v)
			return add(Clamp(el))
		}
		return nil
	}
	if err := walk([]byte{jsonPathTag}, v); err != nil {
		return nil, err
	}
	return SortDedup(out), nil
}

// Query serves @> (containment) and ?, ?|, ?& (top-level key
// existence). Everything else — <@, the negations, comparisons — is
// unconstrained.
func (jsonbKind) Query(op string, rhs types.Datum) (*Expr, error) {
	if rhs.Null {
		// The predicate is UNKNOWN for every row.
		return None(), nil
	}
	switch op {
	case "@>":
		r, err := rhs.Coerce(types.Jsonb)
		if err != nil {
			// Not a document: the filter reports the error.
			return nil, nil
		}
		v, err := decodeJSON(r.S)
		if err != nil {
			return nil, nil
		}
		return jsonContainsQuery(v), nil
	case "?":
		if rhs.Fam.IsArray() {
			return nil, nil
		}
		return Leaf(jsonKeyElement(rhs.Text())), nil
	case "?|", "?&":
		keys := builtins.TextArrayElems(rhs.Text())
		leaves := make([]*Expr, len(keys))
		for i, k := range keys {
			leaves[i] = Leaf(jsonKeyElement(k))
		}
		if op == "?|" {
			return Or(leaves...), nil
		}
		return And(leaves...), nil
	}
	return nil, nil
}

// jsonContainsQuery builds the lookups for `document @> v`.
func jsonContainsQuery(v any) *Expr {
	path := []byte{jsonPathTag}
	switch x := v.(type) {
	case map[string]any:
		kids := make([]*Expr, 0, len(x))
		for k, e := range x {
			sub := jsonContainsUnder(encoding.EncodeString(append(path[:1:1], jsonStepKey), k), e)
			if sub == nil {
				// The value asks only for a shape (an empty object or
				// array, or nests down to one). The key must be there all
				// the same, and that much the index knows.
				sub = Leaf(jsonKeyElement(k))
			}
			kids = append(kids, sub)
		}
		return And(kids...)
	case []any:
		return jsonContainsUnder(path, v)
	}
	// A scalar is contained by the same scalar and, at the top level
	// only, by an array holding it.
	direct, _ := appendJSONScalar(path[:1:1], v)
	inArray, _ := appendJSONScalar(append(path[:1:1], jsonStepArray), v)
	return Or(Leaf(Clamp(direct)), Leaf(Clamp(inArray)))
}

// jsonContainsUnder builds the lookups for a contained value found at
// path: every scalar in it must be in the document at the same path.
// That is necessary, not sufficient — two keys of one contained array
// element may be satisfied by two different elements of the document's
// array — which is one of the reasons every row is rechecked.
func jsonContainsUnder(path []byte, v any) *Expr {
	switch x := v.(type) {
	case map[string]any:
		kids := make([]*Expr, 0, len(x))
		for k, e := range x {
			kids = append(kids, jsonContainsUnder(encoding.EncodeString(append(path[:len(path):len(path)], jsonStepKey), k), e))
		}
		return And(kids...)
	case []any:
		p := append(path[:len(path):len(path)], jsonStepArray)
		kids := make([]*Expr, 0, len(x))
		for _, e := range x {
			kids = append(kids, jsonContainsUnder(p, e))
		}
		return And(kids...)
	}
	el, _ := appendJSONScalar(path[:len(path):len(path)], v)
	return Leaf(Clamp(el))
}

// Describe renders an element: `a.[].b = 1` for a path, `? key` for a
// key, `digest:…` for one stored as a digest.
func (jsonbKind) Describe(element []byte) string {
	if IsDigest(element) {
		return fmt.Sprintf("digest:%x", element[1:9])
	}
	if len(element) == 0 {
		return "<empty>"
	}
	switch element[0] {
	case jsonKeyTag:
		return "? " + quoteJSON(string(element[1:]))
	case jsonPathTag:
		steps, value, err := DecodeJSONPath(element)
		if err != nil {
			break
		}
		if len(steps) == 0 {
			return "$ = " + value
		}
		return strings.Join(steps, ".") + " = " + value
	}
	return fmt.Sprintf("%x", element)
}

func quoteJSON(s string) string {
	b, err := json.Marshal(s)
	if err != nil {
		return fmt.Sprintf("%q", s)
	}
	return string(b)
}

// DecodeJSONPath reads a path element back: the steps (an object key as
// its JSON string, an array step as "[]") and the value as JSON text.
// Digested elements do not read back.
func DecodeJSONPath(element []byte) (steps []string, value string, err error) {
	if len(element) < 2 || element[0] != jsonPathTag {
		return nil, "", fmt.Errorf("not a jsonb path element")
	}
	rest := element[1:]
	for len(rest) > 0 {
		tag := rest[0]
		rest = rest[1:]
		switch tag {
		case jsonStepKey:
			var k string
			if rest, k, err = encoding.DecodeString(rest); err != nil {
				return nil, "", err
			}
			steps = append(steps, quoteJSON(k))
			continue
		case jsonStepArray:
			steps = append(steps, "[]")
			continue
		case jsonNull:
			value = "null"
		case jsonFalse:
			value = "false"
		case jsonTrue:
			value = "true"
		case jsonNumber:
			var d decimal.Dec
			if rest, d, err = encoding.DecodeDecimal(rest); err != nil {
				return nil, "", err
			}
			value = d.String()
		case jsonNumText:
			value, rest = string(rest), nil
		case jsonString:
			value, rest = quoteJSON(string(rest)), nil
		default:
			return nil, "", fmt.Errorf("jsonb path element: unknown tag 0x%02x", tag)
		}
		if len(rest) != 0 {
			return nil, "", fmt.Errorf("jsonb path element: bytes after the value")
		}
		return steps, value, nil
	}
	return nil, "", fmt.Errorf("jsonb path element: no value")
}
