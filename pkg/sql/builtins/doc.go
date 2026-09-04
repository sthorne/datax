package builtins

import (
	"fmt"
	"sort"
	"strings"
)

//go:generate go run ./internal/gendoc ../../../docs/user/functions.md

// categoryOrder is the reference's section order.
var categoryOrder = []string{catControl, catString, catMath, catTime, catJSON, catSession}

// Reference renders the Functions reference (docs/user/functions.md)
// from the registry: one section per category, one entry per canonical
// function with its signature, aliases, volatility and description. The
// file is generated (go generate ./pkg/sql/builtins) and a test fails
// when it drifts from the registry.
func Reference() string {
	var b strings.Builder
	b.WriteString("# Functions reference\n\n")
	b.WriteString("<!-- Generated from pkg/sql/builtins by `go generate ./pkg/sql/builtins`; do not edit by hand. -->\n\n")
	b.WriteString("Every builtin function, by category, as `SHOW FUNCTIONS` lists them. ")
	b.WriteString("Signatures show argument types (`any` accepts every type; `[x]` is optional; `...` is variadic) and the result type. ")
	b.WriteString("An **immutable** function depends on its arguments alone and may appear in a `DEFAULT` or a `CHECK`; a **stable** one is fixed within a statement; a **volatile** one is evaluated afresh for every row. ")
	b.WriteString("Strict functions (the default) return NULL when any argument is NULL; the entries that say otherwise handle NULLs themselves. ")
	b.WriteString("Text arguments accept any value rendered as text; numeric arguments accept the three numeric types. ")
	b.WriteString("The operators (`||`, `%`, `^`, the comparisons, `LIKE`, `SIMILAR TO`, `BETWEEN`, `IS DISTINCT FROM`, the jsonb `->`, `->>`, `#>`, `#>>`, `@>`, `<@`, `?`, `?|`, `?&`) and casts are described in [the SQL reference](sql.md#reading).\n\n")
	byCat := map[string][]*Builtin{}
	for _, f := range All() {
		if f.Hidden {
			continue
		}
		byCat[f.Category] = append(byCat[f.Category], f)
	}
	cats := append([]string(nil), categoryOrder...)
	for c := range byCat {
		known := false
		for _, k := range cats {
			if k == c {
				known = true
			}
		}
		if !known {
			cats = append(cats, c)
		}
	}
	for _, c := range cats {
		fs := byCat[c]
		if len(fs) == 0 {
			continue
		}
		sort.Slice(fs, func(i, j int) bool { return fs[i].Name < fs[j].Name })
		fmt.Fprintf(&b, "## %s\n\n", c)
		for _, f := range fs {
			fmt.Fprintf(&b, "- `%s`", f.Signature())
			if len(f.Aliases) > 0 {
				fmt.Fprintf(&b, " (also `%s`)", strings.Join(f.Aliases, "`, `"))
			}
			extra := []string{f.Vol.String()}
			if f.NotStrict {
				extra = append(extra, "handles NULL arguments")
			}
			fmt.Fprintf(&b, " — %s *%s*\n", f.Doc, strings.Join(extra, ", "))
		}
		b.WriteString("\n")
	}
	return b.String()
}
