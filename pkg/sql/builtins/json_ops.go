package builtins

import (
	"strconv"
	"strings"

	"github.com/sthorne/datax/pkg/sql/types"
)

// The jsonb operators of PG-F-005: - (delete a key, keys or an
// element), #- (delete at a path), and subscripting, j['a'][0], to read
// and, through UPDATE ... SET j['a'] = v, to assign.

// CodeDatatypeMismatch is datatype_mismatch (a subscript of a type
// jsonb does not take, an assignment of a non-jsonb value).
const CodeDatatypeMismatch = "42804"

// JSONBDelete is jsonb - r: r text deletes that key from an object (or
// the matching strings from an array), text[] each of its keys, an
// integer the array element at that index (from the end when
// negative). ok is false when l is not jsonb, and the operator is
// another one.
func JSONBDelete(l, r types.Datum) (d types.Datum, ok bool, err error) {
	if l.Fam != types.Jsonb {
		return types.Datum{}, false, nil
	}
	if l.Null || r.Null {
		return types.DNull, true, nil
	}
	v, err := decodeJSON(l.S)
	if err != nil {
		return types.Datum{}, true, err
	}
	var keys []string
	switch {
	case r.Fam == types.Int:
		arr, isArr := v.([]any)
		switch {
		case !isArr && isJSONScalar(v):
			return types.Datum{}, true, errf(CodeInvalidArgument, "cannot delete from scalar")
		case !isArr:
			return types.Datum{}, true, errf(CodeInvalidArgument, "cannot delete from object using integer index")
		}
		i := r.I
		if i < 0 {
			i += int64(len(arr))
		}
		if i < 0 || i >= int64(len(arr)) {
			return l, true, nil
		}
		out := append(append([]any{}, arr[:i]...), arr[i+1:]...)
		d, err := encodeJSON(out)
		return d, true, err
	case r.Fam == types.String:
		keys = []string{r.S}
	case r.Fam.IsArray() && r.Fam.Elem() == types.String:
		for _, e := range r.A {
			if !e.Null {
				keys = append(keys, e.S)
			}
		}
	default:
		return types.Datum{}, true, errf(CodeUndefined, "operator does not exist: jsonb - %s", pgTypeName(r.Fam))
	}
	switch x := v.(type) {
	case map[string]any:
		out := make(map[string]any, len(x))
		for k, e := range x {
			out[k] = e
		}
		for _, k := range keys {
			delete(out, k)
		}
		d, err := encodeJSON(out)
		return d, true, err
	case []any:
		out := make([]any, 0, len(x))
	next:
		for _, e := range x {
			if s, isStr := e.(string); isStr {
				for _, k := range keys {
					if s == k {
						continue next
					}
				}
			}
			out = append(out, e)
		}
		d, err := encodeJSON(out)
		return d, true, err
	}
	return types.Datum{}, true, errf(CodeInvalidArgument, "cannot delete from scalar")
}

func isJSONScalar(v any) bool {
	switch v.(type) {
	case map[string]any, []any:
		return false
	}
	return true
}

// pathArg reads a text[] path operand: an array, or text holding an
// array literal ('{a,0}').
func pathArg(r types.Datum) ([]string, []bool, error) {
	switch {
	case r.Fam.IsArray():
		keys := make([]string, len(r.A))
		nulls := make([]bool, len(r.A))
		for i, e := range r.A {
			if e.Null {
				nulls[i] = true
				continue
			}
			keys[i] = e.Text()
		}
		return keys, nulls, nil
	case r.Fam == types.String:
		s := strings.TrimSpace(r.S)
		if !strings.HasPrefix(s, "{") || !strings.HasSuffix(s, "}") {
			return nil, nil, errf(CodeInvalidText, "malformed array literal: \"%s\"", r.S)
		}
		arr, err := r.Coerce(types.ArrayOf(types.String))
		if err != nil {
			return nil, nil, errf(CodeInvalidText, "malformed array literal: \"%s\"", r.S)
		}
		return pathArg(arr)
	}
	return nil, nil, errf(CodeUndefined, "operator does not exist: jsonb #- %s", pgTypeName(r.Fam))
}

// arrayIndex reads a path element addressing an array.
func arrayIndex(key string, level int) (int64, error) {
	i, err := strconv.ParseInt(strings.TrimSpace(key), 10, 32)
	if err != nil || strings.TrimSpace(key) != key {
		return 0, errf(CodeInvalidText, "path element at position %d is not an integer: \"%s\"", level+1, key)
	}
	return i, nil
}

// JSONBDeletePath is jsonb #- text[]: the value without the element at
// the path (array elements addressed by index, from the end when
// negative); unchanged when the path does not exist.
func JSONBDeletePath(l, r types.Datum) (types.Datum, error) {
	if l.Null || r.Null {
		return types.DNull, nil
	}
	if l.Fam != types.Jsonb {
		return types.Datum{}, errf(CodeUndefined, "operator does not exist: %s #- %s", pgTypeName(l.Fam), pgTypeName(r.Fam))
	}
	keys, nulls, err := pathArg(r)
	if err != nil {
		return types.Datum{}, err
	}
	v, err := decodeJSON(l.S)
	if err != nil {
		return types.Datum{}, err
	}
	if isJSONScalar(v) {
		return types.Datum{}, errf(CodeInvalidArgument, "cannot delete path in scalar")
	}
	if len(keys) == 0 {
		return l, nil
	}
	out, err := deletePath(v, keys, nulls, 0)
	if err != nil {
		return types.Datum{}, err
	}
	return encodeJSON(out)
}

func deletePath(v any, keys []string, nulls []bool, level int) (any, error) {
	if nulls[level] {
		return nil, errf(CodeNullNotAllowed, "path element at position %d is null", level+1)
	}
	last := level == len(keys)-1
	switch x := v.(type) {
	case map[string]any:
		child, ok := x[keys[level]]
		if !ok {
			return v, nil
		}
		out := make(map[string]any, len(x))
		for k, e := range x {
			out[k] = e
		}
		if last {
			delete(out, keys[level])
			return out, nil
		}
		c, err := deletePath(child, keys, nulls, level+1)
		if err != nil {
			return nil, err
		}
		out[keys[level]] = c
		return out, nil
	case []any:
		i, err := arrayIndex(keys[level], level)
		if err != nil {
			return nil, err
		}
		if i < 0 {
			i += int64(len(x))
		}
		if i < 0 || i >= int64(len(x)) {
			return v, nil
		}
		if last {
			return append(append([]any{}, x[:i]...), x[i+1:]...), nil
		}
		c, err := deletePath(x[i], keys, nulls, level+1)
		if err != nil {
			return nil, err
		}
		out := append([]any{}, x...)
		out[i] = c
		return out, nil
	}
	return v, nil
}

// subscriptKey reads a jsonb subscript as the text key it stands for:
// an integer or text (NULL refuses as unsupported types do not).
func subscriptKey(d types.Datum) (string, error) {
	switch d.Fam {
	case types.Int:
		return strconv.FormatInt(d.I, 10), nil
	case types.String:
		return d.S, nil
	}
	return "", errf(CodeDatatypeMismatch, "subscript type %s is not supported", pgTypeName(d.Fam))
}

// jsonbSubscript is j[sub] over jsonb: an object's field by its text
// (an integer subscript as its text), an array's element by its index
// (from the end when negative); NULL when there is none.
func jsonbSubscript(a []types.Datum) (types.Datum, bool, error) {
	if len(a) != 2 || a[0].Fam != types.Jsonb {
		return types.Datum{}, false, nil
	}
	if a[0].Null || a[1].Null {
		return types.DNull, true, nil
	}
	key, err := subscriptKey(a[1])
	if err != nil {
		return types.Datum{}, true, err
	}
	v, err := decodeJSON(a[0].S)
	if err != nil {
		return types.Datum{}, true, err
	}
	if arr, ok := v.([]any); ok {
		i, err := strconv.ParseInt(key, 10, 64)
		if err != nil {
			return types.DNull, true, nil
		}
		if i < 0 {
			i += int64(len(arr))
		}
		if i < 0 || i >= int64(len(arr)) {
			return types.DNull, true, nil
		}
		d, err := encodeJSON(arr[i])
		return d, true, err
	}
	got, ok := JSONPath(v, []string{key})
	if !ok || isJSONScalar(v) {
		return types.DNull, true, nil
	}
	d, err := encodeJSON(got)
	return d, true, err
}

// SubscriptAssignFunc is the function an UPDATE's SET j['a'][0] = v
// calls: subscript_assign(j, 'j', v, 'a', 0) is the new value of j.
const SubscriptAssignFunc = "subscript_assign"

// subscriptAssign is jsonb subscript assignment, as PostgreSQL's
// jsonb_subscript_assign: a missing key or element is created, an
// array is padded with nulls up to the index, missing containers on
// the path are created (an array where the next subscript is an
// integer), and a NULL value starts from an empty object (an empty
// array when the first subscript is an integer). A path through an
// existing scalar is refused.
func subscriptAssign(a []types.Datum) (types.Datum, error) {
	target, col, val, subs := a[0], a[1].Text(), a[2], a[3:]
	if !target.Null && target.Fam != types.Jsonb {
		if target.Fam.IsArray() {
			return types.Datum{}, errf(CodeNotSupported, "array element assignment (%s[i] = ...) is not supported", col)
		}
		return types.Datum{}, errf(CodeDatatypeMismatch, "cannot subscript type %s because it does not support subscripting", pgTypeName(target.Fam))
	}
	var newVal any
	switch {
	case val.Null:
		newVal = nil
	case val.Fam == types.Jsonb, val.Fam == types.String:
		v, err := decodeJSON(val.S)
		if err != nil {
			return types.Datum{}, err
		}
		newVal = v
	default:
		return types.Datum{}, errf(CodeDatatypeMismatch, "subscripted assignment to \"%s\" requires type jsonb but expression is of type %s", col, pgTypeName(val.Fam))
	}
	keys := make([]string, len(subs))
	for i, sd := range subs {
		if sd.Null {
			return types.Datum{}, errf(CodeNullNotAllowed, "jsonb subscript in assignment must not be null")
		}
		k, err := subscriptKey(sd)
		if err != nil {
			return types.Datum{}, err
		}
		keys[i] = k
	}
	var root any
	if target.Null {
		if subs[0].Fam == types.Int {
			root = []any{}
		} else {
			root = map[string]any{}
		}
	} else {
		v, err := decodeJSON(target.S)
		if err != nil {
			return types.Datum{}, err
		}
		root = v
	}
	out, err := assignPath(root, keys, 0, newVal)
	if err != nil {
		return types.Datum{}, err
	}
	return encodeJSON(out)
}

func errReplaceScalar() error {
	return errf(CodeInvalidArgument, "cannot replace existing key")
}

func assignPath(v any, keys []string, level int, newVal any) (any, error) {
	last := level == len(keys)-1
	switch x := v.(type) {
	case map[string]any:
		out := make(map[string]any, len(x)+1)
		for k, e := range x {
			out[k] = e
		}
		child, ok := x[keys[level]]
		switch {
		case last:
			out[keys[level]] = newVal
		case !ok:
			out[keys[level]] = createPath(keys, level+1, newVal)
		default:
			if isJSONScalar(child) {
				return nil, errReplaceScalar()
			}
			c, err := assignPath(child, keys, level+1, newVal)
			if err != nil {
				return nil, err
			}
			out[keys[level]] = c
		}
		return out, nil
	case []any:
		i, err := arrayIndex(keys[level], level)
		if err != nil {
			return nil, err
		}
		n := int64(len(x))
		if i < 0 {
			if -i > n {
				return nil, errf(CodeInvalidArgument, "path element at position %d is out of range: %d", level+1, i)
			}
			i += n
		}
		out := append([]any{}, x...)
		for int64(len(out)) <= i {
			out = append(out, nil)
		}
		switch {
		case last:
			out[i] = newVal
		case i >= n:
			out[i] = createPath(keys, level+1, newVal)
		default:
			if isJSONScalar(x[i]) {
				return nil, errReplaceScalar()
			}
			c, err := assignPath(x[i], keys, level+1, newVal)
			if err != nil {
				return nil, err
			}
			out[i] = c
		}
		return out, nil
	}
	return nil, errReplaceScalar()
}

// createPath builds the containers for keys[level:] around newVal: an
// array (padded with nulls to the index) where the key is an integer,
// an object otherwise.
func createPath(keys []string, level int, newVal any) any {
	if level == len(keys) {
		return newVal
	}
	inner := createPath(keys, level+1, newVal)
	if i, err := strconv.ParseInt(keys[level], 10, 32); err == nil {
		arr := make([]any, 0, max(i, 0)+1)
		for j := int64(0); j < i; j++ {
			arr = append(arr, nil)
		}
		return append(arr, inner)
	}
	return map[string]any{keys[level]: inner}
}

func init() {
	register(&Builtin{Name: SubscriptAssignFunc, Args: []types.Family{Any, types.String, Any, Any}, MinArgs: 4, Variadic: true, Ret: types.Jsonb, NotStrict: true, Hidden: true, Category: catJSON,
		Doc: "UPDATE ... SET j['a'][0] = v: the jsonb j with v assigned at the subscripts.",
		Fn:  subscriptAssign})
}
