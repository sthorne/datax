package sql

import (
	"encoding/json"

	"github.com/sthorne/datax/pkg/sql/parser"
	"github.com/sthorne/datax/pkg/sql/types"
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
		var obj map[string]json.RawMessage
		if err := json.Unmarshal(raw, &obj); err != nil {
			return types.DNull, nil // scalar or array: no field to extract
		}
		v, ok := obj[step.Key]
		if !ok {
			return types.DNull, nil
		}
		raw = v
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

// pathResultType is the SQL type a non-empty path chain produces: text for
// a terminal ->>, jsonb otherwise.
func pathResultType(path []parser.PathStep) types.Family {
	if path[len(path)-1].Text {
		return types.String
	}
	return types.Jsonb
}
