package sql

import (
	"strings"

	"github.com/sthorne/datax/pkg/sql/builtins"
	"github.com/sthorne/datax/pkg/sql/types"
)

// execShowFunctions lists the builtin functions from the registry: one
// row per function, aliases folded into their canonical entry.
func execShowFunctions() *Result {
	res := &Result{Columns: []ResultColumn{
		{Name: "name", Type: types.String}, {Name: "signature", Type: types.String}, {Name: "category", Type: types.String},
		{Name: "volatility", Type: types.String}, {Name: "aliases", Type: types.String}, {Name: "description", Type: types.String},
	}}
	for _, b := range builtins.All() {
		if b.Hidden {
			continue
		}
		aliases := types.DNull
		if len(b.Aliases) > 0 {
			aliases = types.NewString(strings.Join(b.Aliases, ", "))
		}
		res.Rows = append(res.Rows, []types.Datum{
			types.NewString(b.Name), types.NewString(b.Signature()), types.NewString(b.Category),
			types.NewString(b.Vol.String()), aliases, types.NewString(b.Doc),
		})
	}
	res.Tag = "SHOW FUNCTIONS"
	return res
}
