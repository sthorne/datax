package builtins

import "github.com/sthorne/datax/pkg/sql/types"

const catControl = "Conditionals"

func init() {
	register(&Builtin{Name: "coalesce", Args: []types.Family{Any}, MinArgs: 1, Variadic: true, Ret: Any, SameAsArg: 0, NotStrict: true, Category: catControl,
		Doc: "The first argument that is not NULL (NULL when all are).",
		Fn: func(a []types.Datum) (types.Datum, error) {
			for _, d := range a {
				if !d.Null {
					return d, nil
				}
			}
			return types.DNull, nil
		}})
	register(&Builtin{Name: "nullif", Args: []types.Family{Any, Any}, MinArgs: 2, Ret: Any, SameAsArg: 0, NotStrict: true, Category: catControl,
		Doc: "NULL when the two arguments are equal, else the first.",
		Fn: func(a []types.Datum) (types.Datum, error) {
			if a[0].Null || a[1].Null {
				return a[0], nil
			}
			if c, err := a[0].Compare(a[1]); err == nil && c == 0 {
				return types.DNull, nil
			}
			return a[0], nil
		}})
	register(&Builtin{Name: "greatest", Args: []types.Family{Any}, MinArgs: 1, Variadic: true, Ret: Any, SameAsArg: 0, NotStrict: true, Category: catControl,
		Doc: "The largest argument; NULLs are ignored (NULL when all are).",
		Fn:  func(a []types.Datum) (types.Datum, error) { return extreme(a, 1) }})
	register(&Builtin{Name: "least", Args: []types.Family{Any}, MinArgs: 1, Variadic: true, Ret: Any, SameAsArg: 0, NotStrict: true, Category: catControl,
		Doc: "The smallest argument; NULLs are ignored (NULL when all are).",
		Fn:  func(a []types.Datum) (types.Datum, error) { return extreme(a, -1) }})
}

// extreme picks the argument that compares highest (sign 1) or lowest
// (sign -1), skipping NULLs.
func extreme(a []types.Datum, sign int) (types.Datum, error) {
	best := types.DNull
	for _, d := range a {
		if d.Null {
			continue
		}
		if best.Null {
			best = d
			continue
		}
		c, err := d.Compare(best)
		if err != nil {
			return types.Datum{}, errf(CodeUndefined, "greatest/least: cannot compare %s with %s", d.Fam, best.Fam)
		}
		if c*sign > 0 {
			best = d
		}
	}
	return best, nil
}
