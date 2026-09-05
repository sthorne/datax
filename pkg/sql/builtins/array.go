package builtins

import (
	"strings"

	"github.com/sthorne/datax/pkg/sql/types"
)

const catArray = "Arrays"

// toArray reads an array argument: an array value, or text in array
// literal syntax (elements as text).
func toArray(d types.Datum) (types.Datum, error) {
	switch {
	case d.Fam.IsArray():
		return d, nil
	case d.Fam == types.String:
		a, err := types.ParseArray(d.S, types.String)
		if err != nil {
			return types.Datum{}, errf(CodeInvalidText, "%v", err)
		}
		return a, nil
	}
	return types.Datum{}, errf(CodeUndefined, "function does not exist for argument type %s", strings.ToLower(d.Fam.String()))
}

// elemOf coerces a value to an array's element family (text parses).
func elemOf(arr types.Datum, d types.Datum) (types.Datum, error) {
	if d.Null {
		return d, nil
	}
	c, err := d.Coerce(arr.Fam.Elem())
	if err != nil {
		return types.Datum{}, errf(CodeInvalidText, "%v", err)
	}
	return c, nil
}

// BuildArray is ARRAY[...]: the element family is the first non-NULL
// element's (text when there is none), and every element coerces to
// it.
func BuildArray(args []types.Datum) (types.Datum, error) {
	rank := func(f types.Family) int {
		switch f {
		case types.Int:
			return 1
		case types.Decimal:
			return 2
		case types.Float:
			return 3
		}
		return 0
	}
	// The element type unifies as PostgreSQL's does: numerics promote
	// INT8 → DECIMAL → FLOAT8, and a text literal takes the type of the
	// typed elements beside it.
	elem := types.Unknown
	for _, a := range args {
		switch {
		case a.Null:
		case elem == types.Unknown, elem == types.String && a.Fam != types.String:
			elem = a.Fam
		case rank(elem) > 0 && rank(a.Fam) > rank(elem):
			elem = a.Fam
		}
	}
	if elem == types.Unknown {
		elem = types.String
	}
	if elem.IsArray() {
		return types.Datum{}, errf(CodeNotSupported, "nested arrays are not supported")
	}
	out := make([]types.Datum, len(args))
	for i, a := range args {
		if a.Null {
			out[i] = a
			continue
		}
		c, err := a.Coerce(elem)
		if err != nil {
			return types.Datum{}, errf(CodeInvalidText, "ARRAY[...] element %d: %v", i+1, err)
		}
		out[i] = c
	}
	return types.NewArray(elem, out), nil
}

func init() {
	register(&Builtin{Name: "array_construct", Args: []types.Family{Any}, MinArgs: 0, Variadic: true, Ret: Any, NotStrict: true, Hidden: true, Category: catArray,
		Doc: "ARRAY[a, b, ...]: an array of the values (the first non-NULL element's type).",
		Fn:  BuildArray})
	register(&Builtin{Name: "array_subscript", Args: []types.Family{Any, types.Int}, MinArgs: 2, Ret: Any, Hidden: true, Category: catArray,
		Doc: "v[i]: the i-th element (1-based; NULL when out of range).",
		Fn: func(a []types.Datum) (types.Datum, error) {
			arr, err := toArray(a[0])
			if err != nil {
				return types.Datum{}, err
			}
			i := a[1].I
			if i < 1 || i > int64(len(arr.A)) {
				return types.DNull, nil
			}
			return arr.A[i-1], nil
		}})
	register(&Builtin{Name: "array_length", Args: []types.Family{Any, types.Int}, MinArgs: 2, Ret: types.Int, Category: catArray,
		Doc: "The number of elements of an array (dimension 1; NULL for an empty array or another dimension).",
		Fn: func(a []types.Datum) (types.Datum, error) {
			arr, err := toArray(a[0])
			if err != nil {
				return types.Datum{}, err
			}
			if a[1].I != 1 || len(arr.A) == 0 {
				return types.DNull, nil
			}
			return types.NewInt(int64(len(arr.A))), nil
		}})
	register(&Builtin{Name: "cardinality", Args: []types.Family{Any}, MinArgs: 1, Ret: types.Int, Category: catArray,
		Doc: "The number of elements of an array (0 for an empty one).",
		Fn: func(a []types.Datum) (types.Datum, error) {
			arr, err := toArray(a[0])
			if err != nil {
				return types.Datum{}, err
			}
			return types.NewInt(int64(len(arr.A))), nil
		}})
	register(&Builtin{Name: "array_upper", Args: []types.Family{Any, types.Int}, MinArgs: 2, Ret: types.Int, Category: catArray,
		Doc: "The upper bound of an array's dimension (its length; NULL when empty).",
		Fn: func(a []types.Datum) (types.Datum, error) {
			arr, err := toArray(a[0])
			if err != nil {
				return types.Datum{}, err
			}
			if a[1].I != 1 || len(arr.A) == 0 {
				return types.DNull, nil
			}
			return types.NewInt(int64(len(arr.A))), nil
		}})
	register(&Builtin{Name: "array_lower", Args: []types.Family{Any, types.Int}, MinArgs: 2, Ret: types.Int, Category: catArray,
		Doc: "The lower bound of an array's dimension (1; NULL when empty).",
		Fn: func(a []types.Datum) (types.Datum, error) {
			arr, err := toArray(a[0])
			if err != nil {
				return types.Datum{}, err
			}
			if a[1].I != 1 || len(arr.A) == 0 {
				return types.DNull, nil
			}
			return types.NewInt(1), nil
		}})
	register(&Builtin{Name: "array_ndims", Args: []types.Family{Any}, MinArgs: 1, Ret: types.Int, Category: catArray,
		Doc: "The number of dimensions of an array (1; NULL when empty).",
		Fn: func(a []types.Datum) (types.Datum, error) {
			arr, err := toArray(a[0])
			if err != nil {
				return types.Datum{}, err
			}
			if len(arr.A) == 0 {
				return types.DNull, nil
			}
			return types.NewInt(1), nil
		}})
	register(&Builtin{Name: "array_append", Args: []types.Family{Any, Any}, MinArgs: 2, Ret: Any, NotStrict: true, Category: catArray,
		Doc: "The array with the value appended.",
		Fn: func(a []types.Datum) (types.Datum, error) {
			if a[0].Null {
				return BuildArray(a[1:])
			}
			arr, err := toArray(a[0])
			if err != nil {
				return types.Datum{}, err
			}
			e, err := elemOf(arr, a[1])
			if err != nil {
				return types.Datum{}, err
			}
			return types.NewArray(arr.Fam.Elem(), append(append([]types.Datum{}, arr.A...), e)), nil
		}})
	register(&Builtin{Name: "array_prepend", Args: []types.Family{Any, Any}, MinArgs: 2, Ret: Any, SameAsArg: 1, NotStrict: true, Category: catArray,
		Doc: "The array with the value prepended.",
		Fn: func(a []types.Datum) (types.Datum, error) {
			if a[1].Null {
				return BuildArray(a[:1])
			}
			arr, err := toArray(a[1])
			if err != nil {
				return types.Datum{}, err
			}
			e, err := elemOf(arr, a[0])
			if err != nil {
				return types.Datum{}, err
			}
			return types.NewArray(arr.Fam.Elem(), append([]types.Datum{e}, arr.A...)), nil
		}})
	register(&Builtin{Name: "array_cat", Args: []types.Family{Any, Any}, MinArgs: 2, Ret: Any, NotStrict: true, Category: catArray,
		Doc: "The two arrays concatenated.",
		Fn: func(a []types.Datum) (types.Datum, error) {
			return ConcatArrays(a[0], a[1])
		}})
	register(&Builtin{Name: "array_position", Args: []types.Family{Any, Any}, MinArgs: 2, Ret: types.Int, NotStrict: true, Category: catArray,
		Doc: "The 1-based position of the first element equal to the value (NULL when absent).",
		Fn: func(a []types.Datum) (types.Datum, error) {
			if a[0].Null {
				return types.DNull, nil
			}
			arr, err := toArray(a[0])
			if err != nil {
				return types.Datum{}, err
			}
			e, err := elemOf(arr, a[1])
			if err != nil {
				return types.Datum{}, err
			}
			for i, x := range arr.A {
				if x.Null && e.Null {
					return types.NewInt(int64(i + 1)), nil
				}
				if x.Null || e.Null {
					continue
				}
				if c, cerr := x.Compare(e); cerr == nil && c == 0 {
					return types.NewInt(int64(i + 1)), nil
				}
			}
			return types.DNull, nil
		}})
	register(&Builtin{Name: "array_remove", Args: []types.Family{Any, Any}, MinArgs: 2, Ret: Any, NotStrict: true, Category: catArray,
		Doc: "The array without the elements equal to the value.",
		Fn: func(a []types.Datum) (types.Datum, error) {
			if a[0].Null {
				return types.DNull, nil
			}
			arr, err := toArray(a[0])
			if err != nil {
				return types.Datum{}, err
			}
			e, err := elemOf(arr, a[1])
			if err != nil {
				return types.Datum{}, err
			}
			var out []types.Datum
			for _, x := range arr.A {
				drop := x.Null && e.Null
				if !x.Null && !e.Null {
					if c, cerr := x.Compare(e); cerr == nil && c == 0 {
						drop = true
					}
				}
				if !drop {
					out = append(out, x)
				}
			}
			return types.NewArray(arr.Fam.Elem(), out), nil
		}})
	register(&Builtin{Name: "array_to_string", Args: []types.Family{Any, types.String, types.String}, MinArgs: 2, Ret: types.String, NotStrict: true, Category: catArray,
		Doc: "The elements joined by the separator; NULL elements are skipped unless the third argument gives their text.",
		Fn: func(a []types.Datum) (types.Datum, error) {
			if a[0].Null || a[1].Null {
				return types.DNull, nil
			}
			arr, err := toArray(a[0])
			if err != nil {
				return types.Datum{}, err
			}
			var parts []string
			for _, x := range arr.A {
				if x.Null {
					if len(a) > 2 && !a[2].Null {
						parts = append(parts, a[2].Text())
					}
					continue
				}
				parts = append(parts, x.Text())
			}
			return types.NewString(strings.Join(parts, a[1].Text())), nil
		}})
	register(&Builtin{Name: "string_to_array", Args: []types.Family{types.String, types.String, types.String}, MinArgs: 2, Ret: types.ArrayOf(types.String), NotStrict: true, Category: catArray,
		Doc: "The text split on the separator into a text array (an empty separator yields one element; a NULL separator splits into characters); the third argument names the text that becomes NULL.",
		Fn: func(a []types.Datum) (types.Datum, error) {
			if a[0].Null {
				return types.DNull, nil
			}
			var parts []string
			switch {
			case a[1].Null:
				for _, r := range a[0].S {
					parts = append(parts, string(r))
				}
			case a[1].S == "":
				parts = []string{a[0].S}
			default:
				if a[0].S != "" {
					parts = strings.Split(a[0].S, a[1].S)
				}
			}
			out := make([]types.Datum, len(parts))
			for i, p := range parts {
				if len(a) > 2 && !a[2].Null && p == a[2].S {
					out[i] = types.DNull
					continue
				}
				out[i] = types.NewString(p)
			}
			return types.NewArray(types.String, out), nil
		}})
}

// ConcatArrays is array || array (also array || element and element ||
// array): the elements of both, in order, in the array's element type.
func ConcatArrays(l, r types.Datum) (types.Datum, error) {
	switch {
	case l.Null && r.Null:
		return types.DNull, nil
	case l.Null:
		return toArray(r)
	case r.Null:
		return toArray(l)
	}
	switch {
	case l.Fam.IsArray() && r.Fam.IsArray():
		rc, err := r.Coerce(l.Fam)
		if err != nil {
			return types.Datum{}, errf(CodeInvalidText, "%v", err)
		}
		return types.NewArray(l.Fam.Elem(), append(append([]types.Datum{}, l.A...), rc.A...)), nil
	case l.Fam.IsArray():
		if r.Fam == types.String && strings.HasPrefix(strings.TrimSpace(r.S), "{") {
			rc, err := r.Coerce(l.Fam)
			if err == nil {
				return types.NewArray(l.Fam.Elem(), append(append([]types.Datum{}, l.A...), rc.A...)), nil
			}
		}
		e, err := elemOf(l, r)
		if err != nil {
			return types.Datum{}, err
		}
		return types.NewArray(l.Fam.Elem(), append(append([]types.Datum{}, l.A...), e)), nil
	case r.Fam.IsArray():
		if l.Fam == types.String && strings.HasPrefix(strings.TrimSpace(l.S), "{") {
			lc, err := l.Coerce(r.Fam)
			if err == nil {
				return types.NewArray(r.Fam.Elem(), append(append([]types.Datum{}, lc.A...), r.A...)), nil
			}
		}
		e, err := elemOf(r, l)
		if err != nil {
			return types.Datum{}, err
		}
		return types.NewArray(r.Fam.Elem(), append([]types.Datum{e}, r.A...)), nil
	}
	return types.Datum{}, errf(CodeUndefined, "array_cat requires array operands")
}

// ArrayOp evaluates the array operators @> (contains every element of),
// <@ (is contained by) and && (overlaps): NULL elements never match.
func ArrayOp(op string, l, r types.Datum) (bool, error) {
	la, err := toArray(l)
	if err != nil {
		return false, err
	}
	ra, err := toArray(r)
	if err != nil {
		return false, err
	}
	if la.Fam != ra.Fam {
		// A text literal takes the other side's element type.
		switch {
		case r.Fam == types.String:
			if ra, err = ra.Coerce(la.Fam); err != nil {
				return false, errf(CodeInvalidText, "%v", err)
			}
		case l.Fam == types.String:
			if la, err = la.Coerce(ra.Fam); err != nil {
				return false, errf(CodeInvalidText, "%v", err)
			}
		default:
			if ra, err = ra.Coerce(la.Fam); err != nil {
				return false, errf(CodeInvalidText, "%v", err)
			}
		}
	}
	has := func(arr types.Datum, e types.Datum) bool {
		if e.Null {
			return false
		}
		for _, x := range arr.A {
			if x.Null {
				continue
			}
			if c, cerr := x.Compare(e); cerr == nil && c == 0 {
				return true
			}
		}
		return false
	}
	switch op {
	case "@>":
		for _, e := range ra.A {
			if !has(la, e) {
				return false, nil
			}
		}
		return true, nil
	case "<@":
		for _, e := range la.A {
			if !has(ra, e) {
				return false, nil
			}
		}
		return true, nil
	case "&&":
		for _, e := range la.A {
			if has(ra, e) {
				return true, nil
			}
		}
		return false, nil
	}
	return false, errf(CodeUndefined, "operator does not exist: %s %s %s", strings.ToLower(l.Fam.String()), op, strings.ToLower(r.Fam.String()))
}
