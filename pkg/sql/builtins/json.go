package builtins

import (
	"bytes"
	"encoding/json"
	"sort"
	"strconv"
	"strings"

	"github.com/sthorne/datax/pkg/sql/types"
)

const catJSON = "JSON"

// decodeJSON parses stored jsonb text with numbers kept as json.Number.
func decodeJSON(s string) (any, error) {
	var v any
	dec := json.NewDecoder(strings.NewReader(s))
	dec.UseNumber()
	if err := dec.Decode(&v); err != nil {
		return nil, errf(CodeInvalidText, "invalid input syntax for type json: %v", err)
	}
	return v, nil
}

// encodeJSON renders a value as normalized jsonb text.
func encodeJSON(v any) (types.Datum, error) {
	var b bytes.Buffer
	enc := json.NewEncoder(&b)
	enc.SetEscapeHTML(false)
	if err := enc.Encode(v); err != nil {
		return types.Datum{}, errf(CodeInvalidText, "cannot encode json: %v", err)
	}
	return types.ParseJSONB(strings.TrimSuffix(b.String(), "\n"))
}

// jsonValue converts a datum to the Go value to_jsonb would produce.
func jsonValue(d types.Datum) (any, error) {
	if d.Null {
		return nil, nil
	}
	switch d.Fam {
	case types.Jsonb:
		return decodeJSON(d.S)
	case types.Int, types.Decimal:
		return json.Number(d.Text()), nil
	case types.Float:
		return json.Number(strconv.FormatFloat(d.F, 'f', -1, 64)), nil
	case types.Bool:
		return d.B, nil
	}
	return d.Text(), nil
}

// jsonArg parses a jsonb (or text holding JSON) argument.
func jsonArg(d types.Datum) (any, error) {
	if d.Fam == types.Jsonb || d.Fam == types.String {
		return decodeJSON(d.S)
	}
	return nil, errf(CodeUndefined, "function requires a jsonb argument, got %s", strings.ToLower(d.Fam.String()))
}

// JSONPath walks keys (object fields, or array indexes when numeric)
// into a value; ok is false when the path is absent.
func JSONPath(v any, keys []string) (any, bool) {
	for _, k := range keys {
		switch cur := v.(type) {
		case map[string]any:
			next, ok := cur[k]
			if !ok {
				return nil, false
			}
			v = next
		case []any:
			i, err := strconv.Atoi(k)
			if err != nil {
				return nil, false
			}
			if i < 0 {
				i += len(cur)
			}
			if i < 0 || i >= len(cur) {
				return nil, false
			}
			v = cur[i]
		default:
			return nil, false
		}
	}
	return v, true
}

// JSONText renders an extracted value as ->> does: strings bare, JSON
// null as SQL NULL, everything else as its JSON text.
func JSONText(v any) types.Datum {
	switch x := v.(type) {
	case nil:
		return types.DNull
	case string:
		return types.NewString(x)
	}
	d, err := encodeJSON(v)
	if err != nil {
		return types.DNull
	}
	return types.NewString(d.S)
}

// JSONTypeof is jsonb_typeof: object, array, string, number, boolean,
// null.
func JSONTypeof(v any) string {
	switch v.(type) {
	case map[string]any:
		return "object"
	case []any:
		return "array"
	case string:
		return "string"
	case json.Number:
		return "number"
	case bool:
		return "boolean"
	}
	return "null"
}

// JSONHasKey is the ? operator: an object has the key, an array holds
// the string, a string equals it.
func JSONHasKey(v any, key string) bool {
	switch x := v.(type) {
	case map[string]any:
		_, ok := x[key]
		return ok
	case []any:
		for _, e := range x {
			if s, ok := e.(string); ok && s == key {
				return true
			}
		}
	case string:
		return x == key
	}
	return false
}

// TextArrayElems splits a PostgreSQL text array literal ('{a,"b c"}')
// into its elements.
func TextArrayElems(s string) []string {
	s = strings.TrimSpace(s)
	if strings.HasPrefix(s, "{") && strings.HasSuffix(s, "}") {
		s = s[1 : len(s)-1]
	}
	if s == "" {
		return nil
	}
	var out []string
	var cur strings.Builder
	inQuote := false
	for i := 0; i < len(s); i++ {
		c := s[i]
		switch {
		case c == '"':
			inQuote = !inQuote
		case c == '\\' && i+1 < len(s):
			i++
			cur.WriteByte(s[i])
		case c == ',' && !inQuote:
			out = append(out, cur.String())
			cur.Reset()
		default:
			cur.WriteByte(c)
		}
	}
	out = append(out, cur.String())
	return out
}

func init() {
	register(&Builtin{Name: "to_jsonb", Args: []types.Family{Any}, MinArgs: 1, Ret: types.Jsonb, Category: catJSON,
		Doc: "The value as jsonb: text becomes a JSON string, numbers and booleans themselves, jsonb stays.", Aliases: []string{"to_json"},
		Fn: func(a []types.Datum) (types.Datum, error) { return ToJSONB(a[0]) }})
	register(&Builtin{Name: "jsonb_build_object", Args: []types.Family{Any}, MinArgs: 0, Variadic: true, Ret: types.Jsonb, NotStrict: true, Category: catJSON,
		Doc: "An object from alternating keys and values.", Aliases: []string{"json_build_object"},
		Fn: func(a []types.Datum) (types.Datum, error) {
			if len(a)%2 != 0 {
				return types.Datum{}, errf(CodeInvalidArgument, "argument list must have even number of elements")
			}
			obj := map[string]any{}
			for i := 0; i < len(a); i += 2 {
				if a[i].Null {
					return types.Datum{}, errf(CodeInvalidArgument, "argument %d: key must not be null", i+1)
				}
				v, err := jsonValue(a[i+1])
				if err != nil {
					return types.Datum{}, err
				}
				obj[a[i].Text()] = v
			}
			return encodeJSON(obj)
		}})
	register(&Builtin{Name: "jsonb_build_array", Args: []types.Family{Any}, MinArgs: 0, Variadic: true, Ret: types.Jsonb, NotStrict: true, Category: catJSON,
		Doc: "An array of the arguments.", Aliases: []string{"json_build_array"},
		Fn: func(a []types.Datum) (types.Datum, error) {
			arr := make([]any, 0, len(a))
			for _, d := range a {
				v, err := jsonValue(d)
				if err != nil {
					return types.Datum{}, err
				}
				arr = append(arr, v)
			}
			return encodeJSON(arr)
		}})
	register(&Builtin{Name: "jsonb_array_length", Args: []types.Family{types.Jsonb}, MinArgs: 1, Ret: types.Int, Category: catJSON,
		Doc: "The number of elements of a JSON array (an error for anything else).", Aliases: []string{"json_array_length"},
		Fn: func(a []types.Datum) (types.Datum, error) {
			v, err := jsonArg(a[0])
			if err != nil {
				return types.Datum{}, err
			}
			arr, ok := v.([]any)
			if !ok {
				return types.Datum{}, errf(CodeInvalidArgument, "cannot get array length of a non-array")
			}
			return i64(int64(len(arr))), nil
		}})
	register(&Builtin{Name: "jsonb_typeof", Args: []types.Family{types.Jsonb}, MinArgs: 1, Ret: types.String, Category: catJSON,
		Doc: "object, array, string, number, boolean or null.", Aliases: []string{"json_typeof"},
		Fn: func(a []types.Datum) (types.Datum, error) {
			v, err := jsonArg(a[0])
			if err != nil {
				return types.Datum{}, err
			}
			return str(JSONTypeof(v)), nil
		}})
	register(&Builtin{Name: "jsonb_extract_path", Args: []types.Family{types.Jsonb, types.String}, MinArgs: 1, Variadic: true, Ret: types.Jsonb, Category: catJSON,
		Doc: "The value at the path of keys (array indexes as text), as jsonb; NULL when absent (also the #> operator with a '{a,b}' path).", Aliases: []string{"json_extract_path"},
		Fn: func(a []types.Datum) (types.Datum, error) {
			v, err := jsonArg(a[0])
			if err != nil {
				return types.Datum{}, err
			}
			keys := make([]string, 0, len(a)-1)
			for _, k := range a[1:] {
				keys = append(keys, k.S)
			}
			found, ok := JSONPath(v, keys)
			if !ok {
				return types.DNull, nil
			}
			return encodeJSON(found)
		}})
	register(&Builtin{Name: "jsonb_extract_path_text", Args: []types.Family{types.Jsonb, types.String}, MinArgs: 1, Variadic: true, Ret: types.String, Category: catJSON,
		Doc: "The value at the path of keys as text; NULL when absent (also the #>> operator).", Aliases: []string{"json_extract_path_text"},
		Fn: func(a []types.Datum) (types.Datum, error) {
			v, err := jsonArg(a[0])
			if err != nil {
				return types.Datum{}, err
			}
			keys := make([]string, 0, len(a)-1)
			for _, k := range a[1:] {
				keys = append(keys, k.S)
			}
			found, ok := JSONPath(v, keys)
			if !ok {
				return types.DNull, nil
			}
			return JSONText(found), nil
		}})
	register(&Builtin{Name: "jsonb_set", Args: []types.Family{types.Jsonb, types.String, types.Jsonb, types.Bool}, MinArgs: 3, Ret: types.Jsonb, Category: catJSON,
		Doc: "The document with the value at the '{a,b}' path replaced (created when missing, unless the fourth argument is false).",
		Fn: func(a []types.Datum) (types.Datum, error) {
			v, err := jsonArg(a[0])
			if err != nil {
				return types.Datum{}, err
			}
			newVal, err := jsonArg(a[2])
			if err != nil {
				return types.Datum{}, err
			}
			create := len(a) < 4 || a[3].B
			keys := TextArrayElems(a[1].S)
			if len(keys) == 0 {
				return encodeJSON(v)
			}
			out, err := jsonSet(v, keys, newVal, create)
			if err != nil {
				return types.Datum{}, err
			}
			return encodeJSON(out)
		}})
	register(&Builtin{Name: "jsonb_pretty", Args: []types.Family{types.Jsonb}, MinArgs: 1, Ret: types.String, Category: catJSON,
		Doc: "The document indented for reading.",
		Fn: func(a []types.Datum) (types.Datum, error) {
			v, err := jsonArg(a[0])
			if err != nil {
				return types.Datum{}, err
			}
			var b bytes.Buffer
			enc := json.NewEncoder(&b)
			enc.SetEscapeHTML(false)
			enc.SetIndent("", "    ")
			if err := enc.Encode(v); err != nil {
				return types.Datum{}, errf(CodeInvalidText, "%v", err)
			}
			return str(strings.TrimSuffix(b.String(), "\n")), nil
		}})
	register(&Builtin{Name: "jsonb_strip_nulls", Args: []types.Family{types.Jsonb}, MinArgs: 1, Ret: types.Jsonb, Category: catJSON,
		Doc: "The document with every object field whose value is null removed.",
		Fn: func(a []types.Datum) (types.Datum, error) {
			v, err := jsonArg(a[0])
			if err != nil {
				return types.Datum{}, err
			}
			return encodeJSON(stripNulls(v))
		}})
	register(&Builtin{Name: "jsonb_object_keys_text", Args: []types.Family{types.Jsonb}, MinArgs: 1, Ret: types.String, Category: catJSON, Hidden: true,
		Doc: "The object's keys as a text array literal (jsonb_object_keys is set-returning in PostgreSQL).",
		Fn: func(a []types.Datum) (types.Datum, error) {
			v, err := jsonArg(a[0])
			if err != nil {
				return types.Datum{}, err
			}
			obj, ok := v.(map[string]any)
			if !ok {
				return types.Datum{}, errf(CodeInvalidArgument, "cannot call jsonb_object_keys on a non-object")
			}
			keys := make([]string, 0, len(obj))
			for k := range obj {
				keys = append(keys, k)
			}
			sort.Strings(keys)
			return str(TextArrayLiteral(keys, nil)), nil
		}})
	register(&Builtin{Name: "jsonb_contains", Args: []types.Family{types.Jsonb, types.Jsonb}, MinArgs: 2, Ret: types.Bool, Category: catJSON, Hidden: true,
		Doc: "Whether the first document contains the second (the @> operator).",
		Fn: func(a []types.Datum) (types.Datum, error) {
			l, err := jsonArg(a[0])
			if err != nil {
				return types.Datum{}, err
			}
			r, err := jsonArg(a[1])
			if err != nil {
				return types.Datum{}, err
			}
			return types.NewBool(JSONContains(l, r, true)), nil
		}})
	register(&Builtin{Name: "jsonb_exists", Args: []types.Family{types.Jsonb, types.String}, MinArgs: 2, Ret: types.Bool, Category: catJSON, Hidden: true,
		Doc: "Whether the key exists (the ? operator).",
		Fn: func(a []types.Datum) (types.Datum, error) {
			v, err := jsonArg(a[0])
			if err != nil {
				return types.Datum{}, err
			}
			return types.NewBool(JSONHasKey(v, a[1].S)), nil
		}})
}

func jsonSet(v any, keys []string, newVal any, create bool) (any, error) {
	if len(keys) == 0 {
		return newVal, nil
	}
	switch cur := v.(type) {
	case map[string]any:
		child, ok := cur[keys[0]]
		if !ok && !create {
			return cur, nil
		}
		if !ok && len(keys) > 1 {
			return cur, nil // PostgreSQL creates only the last key
		}
		out := make(map[string]any, len(cur)+1)
		for k, x := range cur {
			out[k] = x
		}
		set, err := jsonSet(child, keys[1:], newVal, create)
		if err != nil {
			return nil, err
		}
		out[keys[0]] = set
		return out, nil
	case []any:
		i, err := strconv.Atoi(keys[0])
		if err != nil {
			return nil, errf(CodeInvalidText, "path element at position 1 is not an integer: %q", keys[0])
		}
		if i < 0 {
			i += len(cur)
		}
		out := append([]any(nil), cur...)
		switch {
		case i >= 0 && i < len(cur):
			set, err := jsonSet(cur[i], keys[1:], newVal, create)
			if err != nil {
				return nil, err
			}
			out[i] = set
		case create && len(keys) == 1:
			if i < 0 {
				out = append([]any{newVal}, out...)
			} else {
				out = append(out, newVal)
			}
		}
		return out, nil
	}
	return v, nil
}

func stripNulls(v any) any {
	switch x := v.(type) {
	case map[string]any:
		out := make(map[string]any, len(x))
		for k, e := range x {
			if e == nil {
				continue
			}
			out[k] = stripNulls(e)
		}
		return out
	case []any:
		out := make([]any, len(x))
		for i, e := range x {
			out[i] = stripNulls(e)
		}
		return out
	}
	return v
}

// JSONContains is PostgreSQL's jsonb @>: objects contain objects whose
// every key is present with a containing value; arrays contain arrays
// whose every element some element contains; at the top level an array
// contains a matching scalar; scalars compare by value, numbers
// numerically.
func JSONContains(l, r any, topLevel bool) bool {
	switch rv := r.(type) {
	case map[string]any:
		lv, ok := l.(map[string]any)
		if !ok {
			return false
		}
		for k, rval := range rv {
			lval, ok := lv[k]
			if !ok || !JSONContains(lval, rval, false) {
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
				if JSONContains(lelem, relem, false) {
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

func jsonScalarEqual(l, r any) bool {
	ln, lok := l.(json.Number)
	rn, rok := r.(json.Number)
	if lok != rok {
		return false
	}
	if lok {
		ld, lerr := types.ParseDecimal(string(ln))
		rd, rerr := types.ParseDecimal(string(rn))
		if lerr != nil || rerr != nil {
			return string(ln) == string(rn)
		}
		c, err := ld.Compare(rd)
		return err == nil && c == 0
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

// JSONArrayOf builds a JSON array from values (NULL as null).
func JSONArrayOf(vals []types.Datum) (types.Datum, error) {
	arr := make([]any, 0, len(vals))
	for _, d := range vals {
		v, err := jsonValue(d)
		if err != nil {
			return types.Datum{}, err
		}
		arr = append(arr, v)
	}
	return encodeJSON(arr)
}

// JSONObjectOf builds a JSON object from parallel keys and values (a
// later duplicate key wins).
func JSONObjectOf(keys, vals []types.Datum) (types.Datum, error) {
	obj := make(map[string]any, len(keys))
	for i, k := range keys {
		v, err := jsonValue(vals[i])
		if err != nil {
			return types.Datum{}, err
		}
		obj[k.Text()] = v
	}
	return encodeJSON(obj)
}

// TextArrayLiteral renders elements as a PostgreSQL text array
// ('{a,"b c",NULL}'); nulls marks NULL elements.
func TextArrayLiteral(elems []string, nulls []bool) string {
	var b strings.Builder
	b.WriteByte('{')
	for i, e := range elems {
		if i > 0 {
			b.WriteByte(',')
		}
		if nulls != nil && nulls[i] {
			b.WriteString("NULL")
			continue
		}
		if e == "" || strings.ContainsAny(e, `,{}" \\`) || strings.EqualFold(e, "null") {
			b.WriteByte('"')
			b.WriteString(strings.NewReplacer(`\`, `\\`, `"`, `\"`).Replace(e))
			b.WriteByte('"')
			continue
		}
		b.WriteString(e)
	}
	b.WriteByte('}')
	return b.String()
}
