package sql

import (
	"bytes"
	"encoding/json"
	"strconv"
	"strings"

	"github.com/sthorne/datax/pkg/sql/parser"
	"github.com/sthorne/datax/pkg/sql/types"
	"github.com/sthorne/datax/pkg/util/decimal"
)

// applyPath applies a parsed ->/->> chain to a datum. Stored JSONB text is
// canonical (compact, sorted keys), so extracted sub-values are canonical
// too and can be returned without re-normalizing.
//
// Semantics follow PostgreSQL's jsonb operators: extraction from a
// non-object or a missing key yields SQL NULL; a present JSON null under
// -> is the jsonb value 'null' (still JSON), while ->> renders it as SQL
// NULL. Applying a path to a NULL column yields NULL; applying one to a
// non-JSONB column is an error.
func applyPath(d types.Datum, path []parser.PathStep) (types.Datum, error) {
	if len(path) == 0 {
		return d, nil
	}
	if d.Null {
		return types.DNull, nil
	}
	if d.Fam != types.Jsonb {
		return types.Datum{}, newErrf(CodeFeatureNotSupported, "cannot extract path from type %s (-> and ->> require jsonb)", d.Fam)
	}
	raw := json.RawMessage(d.S)
	for _, step := range path {
		keys := step.Keys
		if step.IsIndex {
			keys = []string{strconv.Itoa(step.Index)}
		} else if keys == nil {
			keys = []string{step.Key}
		}
		for _, key := range keys {
			v, ok := rawStep(raw, key)
			if !ok {
				return types.DNull, nil
			}
			raw = v
		}
		if step.Text { // terminal by grammar
			if string(raw) == "null" {
				return types.DNull, nil
			}
			if len(raw) > 0 && raw[0] == '"' {
				var s string
				if err := json.Unmarshal(raw, &s); err != nil {
					return types.Datum{}, newErrf(CodeInternal, "malformed stored jsonb string: %v", err)
				}
				return types.NewString(s), nil
			}
			return types.NewString(string(raw)), nil
		}
	}
	return types.NewJsonb(string(raw)), nil
}

// rawStep takes one step into a raw JSON value: a field of an object, or
// an element of an array by position (negative from the end) when the
// key is numeric.
func rawStep(raw json.RawMessage, key string) (json.RawMessage, bool) {
	trimmed := bytes.TrimSpace(raw)
	if len(trimmed) == 0 {
		return nil, false
	}
	switch trimmed[0] {
	case '{':
		var obj map[string]json.RawMessage
		if err := json.Unmarshal(raw, &obj); err != nil {
			return nil, false
		}
		v, ok := obj[key]
		return v, ok
	case '[':
		i, err := strconv.Atoi(key)
		if err != nil {
			return nil, false
		}
		var arr []json.RawMessage
		if err := json.Unmarshal(raw, &arr); err != nil {
			return nil, false
		}
		if i < 0 {
			i += len(arr)
		}
		if i < 0 || i >= len(arr) {
			return nil, false
		}
		return arr[i], true
	}
	return nil, false
}

// pathResultType is the SQL type a non-empty path chain produces: text for
// a terminal ->>, jsonb otherwise.
func pathResultType(path []parser.PathStep) types.Family {
	if path[len(path)-1].Text {
		return types.String
	}
	return types.Jsonb
}

// jsonbContains reports whether left structurally contains right —
// PostgreSQL's jsonb @> semantics:
//
//   - object ⊇ object: every right key present with a containing value;
//   - array ⊇ array: every right element contained by SOME left element
//     (order and duplicates irrelevant);
//   - top level only: an array contains a matching scalar ('[1,2]' @> '1'
//     is true, but '{"a":[1]}' @> '{"a":1}' is false);
//   - scalars: strings/bools/null compare by value, numbers compare
//     NUMERICALLY (1 contains 1.0, 100 contains 1e2) — exactly, via the
//     decimal package, so integers beyond float64 keep fidelity. Note the
//     asymmetry with jsonb '=', which compares normalized text
//     (documented).
//
// Both inputs are jsonb datums (normalized text).
func jsonbContains(left, right types.Datum) (bool, error) {
	var l, r any
	if err := decodeJSONNumber(left.S, &l); err != nil {
		return false, newErrf(CodeInternal, "malformed stored jsonb: %v", err)
	}
	if err := decodeJSONNumber(right.S, &r); err != nil {
		return false, newErrf(CodeInternal, "malformed stored jsonb: %v", err)
	}
	return jsonContains(l, r, true), nil
}

// decodeJSONNumber unmarshals with json.Number so numeric text survives
// exactly.
func decodeJSONNumber(s string, out *any) error {
	dec := json.NewDecoder(strings.NewReader(s))
	dec.UseNumber()
	return dec.Decode(out)
}

func jsonContains(l, r any, topLevel bool) bool {
	switch rv := r.(type) {
	case map[string]any:
		lv, ok := l.(map[string]any)
		if !ok {
			return false
		}
		for k, rval := range rv {
			lval, ok := lv[k]
			if !ok || !jsonContains(lval, rval, false) {
				return false
			}
		}
		return true
	case []any:
		lv, ok := l.([]any)
		if !ok {
			return false
		}
		for _, relem := range rv {
			found := false
			for _, lelem := range lv {
				if jsonContains(lelem, relem, false) {
					found = true
					break
				}
			}
			if !found {
				return false
			}
		}
		return true
	default:
		// r is a scalar. The one structural exception: at the TOP level an
		// array contains a matching scalar.
		if lv, ok := l.([]any); ok {
			if !topLevel {
				return false
			}
			for _, lelem := range lv {
				if jsonScalarEqual(lelem, r) {
					return true
				}
			}
			return false
		}
		return jsonScalarEqual(l, r)
	}
}

// jsonScalarEqual compares two JSON scalars: numbers numerically (exact),
// everything else by value. Containers never equal scalars.
func jsonScalarEqual(l, r any) bool {
	ln, lok := l.(json.Number)
	rn, rok := r.(json.Number)
	if lok != rok {
		return false
	}
	if lok {
		ld, lerr := decimal.Parse(string(ln))
		rd, rerr := decimal.Parse(string(rn))
		if lerr != nil || rerr != nil {
			return string(ln) == string(rn)
		}
		return decimal.Cmp(ld, rd) == 0
	}
	switch lv := l.(type) {
	case string:
		rv, ok := r.(string)
		return ok && lv == rv
	case bool:
		rv, ok := r.(bool)
		return ok && lv == rv
	case nil:
		return r == nil
	}
	return false
}
