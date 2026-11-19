package builtins

import (
	"fmt"
	"math/rand/v2"
	"sort"

	"github.com/sthorne/datax/pkg/sql/types"
)

// The array functions beyond the basics (PG-F-012). datax's arrays have
// one dimension and a lower bound of 1, so the dimension arguments
// accept that shape and refuse any other (0A000).

// arrayOrNull reads a possibly NULL array argument of a function that
// handles NULLs itself.
func arrayOrNull(d types.Datum) (types.Datum, bool, error) {
	if d.Null {
		return d, false, nil
	}
	a, err := toArray(d)
	return a, err == nil, err
}

// sameElem reports whether x and e match as IS NOT DISTINCT FROM does
// (two NULLs match), the comparison array_replace and array_positions
// use.
func sameElem(x, e types.Datum) bool {
	if x.Null || e.Null {
		return x.Null && e.Null
	}
	c, err := x.Compare(e)
	return err == nil && c == 0
}

// arrayRetOf is the family of an array argument (a text literal reads
// as text[]).
func arrayRetOf(args []types.Family) types.Family {
	if len(args) > 0 && args[0].IsArray() {
		return args[0]
	}
	return types.ArrayOf(types.String)
}

// dimArg reads array_fill's dimension or lower-bound argument: one
// integer (a single dimension).
func dimArg(d types.Datum, what string) (int64, error) {
	if d.Null {
		return 0, errf(CodeNullNotAllowed, "dimension array or low bound array cannot be null")
	}
	a, err := d.Coerce(types.ArrayOf(types.Int))
	if err != nil {
		return 0, errf(CodeInvalidText, "%v", err)
	}
	if len(a.A) != 1 {
		if len(a.A) == 0 && what == "dimension" {
			return 0, nil
		}
		return 0, errf(CodeNotSupported, "multidimensional arrays are not supported")
	}
	if a.A[0].Null {
		return 0, errf(CodeNullNotAllowed, "dimension values cannot be null")
	}
	return a.A[0].I, nil
}

func init() {
	register(&Builtin{Name: "array_dims", Args: []types.Family{Any}, MinArgs: 1, Ret: types.String, Category: catArray,
		Doc: "The text of an array's dimensions ([1:3] for three elements; NULL when empty).",
		Fn: func(a []types.Datum) (types.Datum, error) {
			arr, err := toArray(a[0])
			if err != nil {
				return types.Datum{}, err
			}
			if len(arr.A) == 0 {
				return types.DNull, nil
			}
			return types.NewString(fmt.Sprintf("[1:%d]", len(arr.A))), nil
		}})
	register(&Builtin{Name: "array_fill", Args: []types.Family{Any, Any, Any}, MinArgs: 2, Ret: Any, NotStrict: true, Category: catArray,
		RetOf: func(args []types.Family) types.Family {
			if len(args) == 0 || args[0] == types.Unknown || args[0].IsArray() {
				return types.ArrayOf(types.String)
			}
			return types.ArrayOf(args[0])
		},
		Doc: "An array of the given length (ARRAY[n]; one dimension, lower bound 1) filled with the value.",
		Fn: func(a []types.Datum) (types.Datum, error) {
			n, err := dimArg(a[1], "dimension")
			if err != nil {
				return types.Datum{}, err
			}
			if len(a) > 2 {
				lb, err := dimArg(a[2], "low bound")
				if err != nil {
					return types.Datum{}, err
				}
				if lb != 1 {
					return types.Datum{}, errf(CodeNotSupported, "array lower bounds other than 1 are not supported")
				}
			}
			if n < 0 {
				n = 0
			}
			if n > 1<<20 {
				return types.Datum{}, errf(CodeOutOfRange, "array size exceeds the maximum allowed (%d)", 1<<20)
			}
			elem := a[0].Fam
			if a[0].Null || elem == types.Unknown {
				elem = types.String
			}
			if elem.IsArray() {
				return types.Datum{}, errf(CodeNotSupported, "nested arrays are not supported")
			}
			out := make([]types.Datum, n)
			for i := range out {
				out[i] = a[0]
			}
			return types.NewArray(elem, out), nil
		}})
	register(&Builtin{Name: "array_positions", Args: []types.Family{Any, Any}, MinArgs: 2, Ret: types.ArrayOf(types.Int), NotStrict: true, Category: catArray,
		Doc: "The 1-based positions of every element equal to the value (IS NOT DISTINCT FROM: NULL finds the NULLs); empty when there is none.",
		Fn: func(a []types.Datum) (types.Datum, error) {
			arr, ok, err := arrayOrNull(a[0])
			if !ok {
				return types.DNull, err
			}
			e, err := elemOf(arr, a[1])
			if err != nil {
				return types.Datum{}, err
			}
			out := []types.Datum{}
			for i, x := range arr.A {
				if sameElem(x, e) {
					out = append(out, types.NewInt(int64(i+1)))
				}
			}
			return types.NewArray(types.Int, out), nil
		}})
	register(&Builtin{Name: "array_replace", Args: []types.Family{Any, Any, Any}, MinArgs: 3, Ret: Any, RetOf: arrayRetOf, NotStrict: true, Category: catArray,
		Doc: "The array with each element equal to the second argument (IS NOT DISTINCT FROM) replaced by the third.",
		Fn: func(a []types.Datum) (types.Datum, error) {
			arr, ok, err := arrayOrNull(a[0])
			if !ok {
				return types.DNull, err
			}
			from, err := elemOf(arr, a[1])
			if err != nil {
				return types.Datum{}, err
			}
			to, err := elemOf(arr, a[2])
			if err != nil {
				return types.Datum{}, err
			}
			out := make([]types.Datum, len(arr.A))
			for i, x := range arr.A {
				out[i] = x
				if sameElem(x, from) {
					out[i] = to
				}
			}
			return types.NewArray(arr.Fam.Elem(), out), nil
		}})
	register(&Builtin{Name: "array_reverse", Args: []types.Family{Any}, MinArgs: 1, Ret: Any, RetOf: arrayRetOf, Category: catArray,
		Doc: "The array's elements in reverse order.",
		Fn: func(a []types.Datum) (types.Datum, error) {
			arr, err := toArray(a[0])
			if err != nil {
				return types.Datum{}, err
			}
			out := make([]types.Datum, len(arr.A))
			for i, x := range arr.A {
				out[len(out)-1-i] = x
			}
			return types.NewArray(arr.Fam.Elem(), out), nil
		}})
	register(&Builtin{Name: "array_sample", Args: []types.Family{Any, types.Int}, MinArgs: 2, Ret: Any, RetOf: arrayRetOf, Vol: Volatile, Category: catArray,
		Doc: "n elements of the array chosen at random, in random order (22023 unless 0 ≤ n ≤ its length).",
		Fn: func(a []types.Datum) (types.Datum, error) {
			arr, err := toArray(a[0])
			if err != nil {
				return types.Datum{}, err
			}
			n := a[1].I
			if n < 0 || n > int64(len(arr.A)) {
				return types.Datum{}, errf(CodeInvalidArgument, "sample size must be between 0 and %d", len(arr.A))
			}
			perm := rand.Perm(len(arr.A))
			out := make([]types.Datum, n)
			for i := range out {
				out[i] = arr.A[perm[i]]
			}
			return types.NewArray(arr.Fam.Elem(), out), nil
		}})
	register(&Builtin{Name: "array_shuffle", Args: []types.Family{Any}, MinArgs: 1, Ret: Any, RetOf: arrayRetOf, Vol: Volatile, Category: catArray,
		Doc: "The array's elements in random order.",
		Fn: func(a []types.Datum) (types.Datum, error) {
			arr, err := toArray(a[0])
			if err != nil {
				return types.Datum{}, err
			}
			out := make([]types.Datum, len(arr.A))
			for i, p := range rand.Perm(len(arr.A)) {
				out[i] = arr.A[p]
			}
			return types.NewArray(arr.Fam.Elem(), out), nil
		}})
	register(&Builtin{Name: "array_sort", Args: []types.Family{Any, types.Bool, types.Bool}, MinArgs: 1, Ret: Any, RetOf: arrayRetOf, Category: catArray,
		Doc: "The array's elements sorted: ascending with NULLs last by default; descending (second argument true) puts NULLs first unless the third argument (nulls_first) says otherwise.",
		Fn: func(a []types.Datum) (types.Datum, error) {
			arr, err := toArray(a[0])
			if err != nil {
				return types.Datum{}, err
			}
			desc := len(a) > 1 && a[1].B
			nullsFirst := desc
			if len(a) > 2 {
				nullsFirst = a[2].B
			}
			out := append([]types.Datum(nil), arr.A...)
			var sortErr error
			sort.SliceStable(out, func(i, j int) bool {
				x, y := out[i], out[j]
				if x.Null || y.Null {
					if x.Null == y.Null {
						return false
					}
					return x.Null == nullsFirst
				}
				c, err := x.Compare(y)
				if err != nil && sortErr == nil {
					sortErr = err
				}
				if desc {
					return c > 0
				}
				return c < 0
			})
			if sortErr != nil {
				return types.Datum{}, errf(CodeUndefined, "could not identify a comparison function for type %s", arr.Fam.Elem())
			}
			return types.NewArray(arr.Fam.Elem(), out), nil
		}})
	register(&Builtin{Name: "trim_array", Args: []types.Family{Any, types.Int}, MinArgs: 2, Ret: Any, RetOf: arrayRetOf, Category: catArray,
		Doc: "The array without its last n elements (2202E unless 0 ≤ n ≤ its length).",
		Fn: func(a []types.Datum) (types.Datum, error) {
			arr, err := toArray(a[0])
			if err != nil {
				return types.Datum{}, err
			}
			n := a[1].I
			if n < 0 || n > int64(len(arr.A)) {
				return types.Datum{}, errf(CodeArraySubscript, "number of elements to trim must be between 0 and %d", len(arr.A))
			}
			return types.NewArray(arr.Fam.Elem(), append([]types.Datum(nil), arr.A[:int64(len(arr.A))-n]...)), nil
		}})
	register(&Builtin{Name: "generate_subscripts", Args: []types.Family{Any, types.Int, types.Bool}, MinArgs: 2, Ret: types.Int, Category: catSets,
		Columns: []string{""},
		Doc:     "The subscripts of the array's given dimension (1 to its length for dimension 1; none for an empty array or another dimension), in reverse when the third argument is true.",
		Rows: func(a []types.Datum) (RowIter, error) {
			arr, err := toArray(a[0])
			if err != nil {
				return nil, err
			}
			n := int64(len(arr.A))
			if a[1].I != 1 {
				n = 0
			}
			rows := make([][]types.Datum, n)
			for i := range rows {
				v := int64(i) + 1
				if len(a) > 2 && a[2].B {
					v = n - int64(i)
				}
				rows[i] = []types.Datum{types.NewInt(v)}
			}
			return rowsOf(rows), nil
		}})
}
