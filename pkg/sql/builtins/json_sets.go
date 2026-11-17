package builtins

import (
	"sort"
	"strconv"

	"github.com/sthorne/datax/pkg/sql/types"
)

// The JSON processing functions that return sets (jsonb_each,
// jsonb_array_elements, jsonb_object_keys) and jsonb_insert. The json_
// spellings are aliases: datax stores json as jsonb.

// jsonObjectArg is a jsonb object argument, refused (22023) as
// PostgreSQL refuses it when it is anything else.
func jsonObjectArg(fn string, d types.Datum) (map[string]any, []string, error) {
	v, err := jsonArg(d)
	if err != nil {
		return nil, nil, err
	}
	obj, ok := v.(map[string]any)
	if !ok {
		var what string
		switch _, isArray := v.([]any); {
		case fn != "jsonb_object_keys":
			what = "a non-object"
		case isArray:
			what = "an array"
		default:
			what = "a scalar"
		}
		return nil, nil, errf(CodeInvalidArgument, "cannot call %s on %s", fn, what)
	}
	keys := make([]string, 0, len(obj))
	for k := range obj {
		keys = append(keys, k)
	}
	sort.Strings(keys) // the order the stored document keeps
	return obj, keys, nil
}

// jsonArrayArg is a jsonb array argument.
func jsonArrayArg(d types.Datum) ([]any, error) {
	v, err := jsonArg(d)
	if err != nil {
		return nil, err
	}
	switch x := v.(type) {
	case []any:
		return x, nil
	case map[string]any:
		return nil, errf(CodeInvalidArgument, "cannot extract elements from an object")
	}
	return nil, errf(CodeInvalidArgument, "cannot extract elements from a scalar")
}

// jsonbOf renders a decoded value as a jsonb datum.
func jsonbOf(v any) (types.Datum, error) { return encodeJSON(v) }

// jsonInsert is jsonb_insert: the new value goes before (or after) the
// array element at the path, or under the object key at the path when
// that key is absent (an existing key is an error); a path whose
// parent is missing leaves the document as it is.
func jsonInsert(v any, keys []string, newVal any, after bool, depth int) (any, error) {
	switch cur := v.(type) {
	case map[string]any:
		child, ok := cur[keys[0]]
		if len(keys) == 1 {
			if ok {
				return nil, errf(CodeInvalidArgument, "cannot replace existing key")
			}
			out := make(map[string]any, len(cur)+1)
			for k, x := range cur {
				out[k] = x
			}
			out[keys[0]] = newVal
			return out, nil
		}
		if !ok {
			return cur, nil
		}
		set, err := jsonInsert(child, keys[1:], newVal, after, depth+1)
		if err != nil {
			return nil, err
		}
		out := make(map[string]any, len(cur))
		for k, x := range cur {
			out[k] = x
		}
		out[keys[0]] = set
		return out, nil
	case []any:
		i, err := strconv.Atoi(keys[0])
		if err != nil {
			return nil, errf(CodeInvalidText, "path element at position %d is not an integer: %q", depth+1, keys[0])
		}
		if i < 0 {
			i += len(cur)
		}
		if len(keys) > 1 {
			if i < 0 || i >= len(cur) {
				return cur, nil
			}
			set, err := jsonInsert(cur[i], keys[1:], newVal, after, depth+1)
			if err != nil {
				return nil, err
			}
			out := append([]any(nil), cur...)
			out[i] = set
			return out, nil
		}
		at := i
		if after {
			at++
		}
		switch {
		case i < 0:
			at = 0
		case i >= len(cur):
			at = len(cur)
		}
		out := make([]any, 0, len(cur)+1)
		out = append(out, cur[:at]...)
		out = append(out, newVal)
		return append(out, cur[at:]...), nil
	}
	return nil, errf(CodeInvalidArgument, "cannot set path in scalar")
}

func init() {
	register(&Builtin{Name: "jsonb_each", Args: []types.Family{types.Jsonb}, MinArgs: 1, Ret: types.String, Category: catJSON,
		Columns: []string{"key", "value"}, ColFams: []types.Family{types.Jsonb}, Aliases: []string{"json_each"},
		Doc: "One row per field of the object: its key and its value as jsonb (22023 for a value that is not an object).",
		Rows: func(a []types.Datum) (RowIter, error) {
			obj, keys, err := jsonObjectArg("jsonb_each", a[0])
			if err != nil {
				return nil, err
			}
			rows := make([][]types.Datum, len(keys))
			for i, k := range keys {
				v, err := jsonbOf(obj[k])
				if err != nil {
					return nil, err
				}
				rows[i] = []types.Datum{str(k), v}
			}
			return rowsOf(rows), nil
		}})
	register(&Builtin{Name: "jsonb_each_text", Args: []types.Family{types.Jsonb}, MinArgs: 1, Ret: types.String, Category: catJSON,
		Columns: []string{"key", "value"}, ColFams: []types.Family{types.String}, Aliases: []string{"json_each_text"},
		Doc: "One row per field of the object: its key and its value as text (strings unquoted, JSON null as NULL).",
		Rows: func(a []types.Datum) (RowIter, error) {
			obj, keys, err := jsonObjectArg("jsonb_each_text", a[0])
			if err != nil {
				return nil, err
			}
			rows := make([][]types.Datum, len(keys))
			for i, k := range keys {
				rows[i] = []types.Datum{str(k), JSONText(obj[k])}
			}
			return rowsOf(rows), nil
		}})
	register(&Builtin{Name: "jsonb_array_elements", Args: []types.Family{types.Jsonb}, MinArgs: 1, Ret: types.Jsonb, Category: catJSON,
		Columns: []string{"value"}, Aliases: []string{"json_array_elements"},
		Doc: "One row per element of the array, as jsonb (22023 for a value that is not an array).",
		Rows: func(a []types.Datum) (RowIter, error) {
			arr, err := jsonArrayArg(a[0])
			if err != nil {
				return nil, err
			}
			rows := make([][]types.Datum, len(arr))
			for i, el := range arr {
				v, err := jsonbOf(el)
				if err != nil {
					return nil, err
				}
				rows[i] = []types.Datum{v}
			}
			return rowsOf(rows), nil
		}})
	register(&Builtin{Name: "jsonb_array_elements_text", Args: []types.Family{types.Jsonb}, MinArgs: 1, Ret: types.String, Category: catJSON,
		Columns: []string{"value"}, Aliases: []string{"json_array_elements_text"},
		Doc: "One row per element of the array, as text (strings unquoted, JSON null as NULL).",
		Rows: func(a []types.Datum) (RowIter, error) {
			arr, err := jsonArrayArg(a[0])
			if err != nil {
				return nil, err
			}
			rows := make([][]types.Datum, len(arr))
			for i, el := range arr {
				rows[i] = []types.Datum{JSONText(el)}
			}
			return rowsOf(rows), nil
		}})
	register(&Builtin{Name: "jsonb_object_keys", Args: []types.Family{types.Jsonb}, MinArgs: 1, Ret: types.String, Category: catJSON,
		Columns: []string{""}, Aliases: []string{"json_object_keys"},
		Doc: "One row per key of the object (22023 for a value that is not an object).",
		Rows: func(a []types.Datum) (RowIter, error) {
			_, keys, err := jsonObjectArg("jsonb_object_keys", a[0])
			if err != nil {
				return nil, err
			}
			rows := make([][]types.Datum, len(keys))
			for i, k := range keys {
				rows[i] = []types.Datum{str(k)}
			}
			return rowsOf(rows), nil
		}})
	register(&Builtin{Name: "jsonb_insert", Args: []types.Family{types.Jsonb, types.String, types.Jsonb, types.Bool}, MinArgs: 3, Ret: types.Jsonb, Category: catJSON,
		Doc: "The document with the value inserted at the '{a,0}' path: before the array element there (after it when the fourth argument is true), or as a new object key (an existing key is an error, 22023).",
		Fn: func(a []types.Datum) (types.Datum, error) {
			v, err := jsonArg(a[0])
			if err != nil {
				return types.Datum{}, err
			}
			newVal, err := jsonArg(a[2])
			if err != nil {
				return types.Datum{}, err
			}
			keys := TextArrayElems(a[1].S)
			if len(keys) == 0 {
				return encodeJSON(v)
			}
			out, err := jsonInsert(v, keys, newVal, len(a) > 3 && a[3].B, 0)
			if err != nil {
				return types.Datum{}, err
			}
			return encodeJSON(out)
		}})
}
