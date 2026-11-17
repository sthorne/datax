package sql

import (
	"context"
	"strings"

	"github.com/sthorne/datax/pkg/kvclient"
	"github.com/sthorne/datax/pkg/sql/builtins"
	"github.com/sthorne/datax/pkg/sql/parser"
	"github.com/sthorne/datax/pkg/sql/types"
)

// selectSet is one set-returning call in a FROM-less select list: its
// position and the values it yields, one per output row.
type selectSet struct {
	at   int
	vals []types.Datum
}

// selectSRF reports whether a select item is a set-returning call
// (unnest(array) or a registry function such as generate_series),
// with the function's name, which names the output column.
func selectSRF(e parser.Expr) (string, bool) {
	if e.Func == "" || e.Left != nil {
		return "", false
	}
	if e.Func == "unnest" {
		return "unnest", len(e.Args) == 1 && e.Cast == "" && len(e.Path) == 0 && e.BinOp == ""
	}
	b, ok := builtins.Lookup(e.Func)
	return e.Func, ok && b.IsSetReturning()
}

// selectSetValues evaluates a set-returning select item: its values
// and its column. Whatever applies to the call's result in the item
// itself (generate_series(1, 3) * 2, ...::text) applies to each value.
func (s *Session) selectSetValues(ctx context.Context, txn *kvclient.Txn, e parser.Expr, name string, params []types.Datum) (selectSet, ResultColumn, error) {
	if e.Func == "unnest" {
		arr, err := evalExpr(e.Args[0], nil, nil, params)
		if err != nil {
			return selectSet{}, ResultColumn{}, err
		}
		if !arr.Null && !arr.Fam.IsArray() {
			if arr, err = arr.Coerce(types.ArrayOf(types.String)); err != nil {
				return selectSet{}, ResultColumn{}, newErrf(CodeInvalidTextRepresentation, "unnest: %v", err)
			}
		}
		fam := exprFamily(e.Args[0], nil).Elem()
		if !arr.Null && arr.Fam.Elem() != types.Unknown {
			fam = arr.Fam.Elem()
		}
		if fam == types.Unknown {
			fam = types.String
		}
		return selectSet{vals: arr.A}, ResultColumn{Name: name, Type: fam}, nil
	}
	b, _ := builtins.Lookup(e.Func)
	if len(b.Columns) > 1 {
		return selectSet{}, ResultColumn{}, newErrf(CodeFeatureNotSupported,
			"%s() returns rows of %d columns, which a select list cannot hold; call it in FROM (SELECT * FROM %s(...))", e.Func, len(b.Columns), e.Func)
	}
	call := parser.Expr{Func: e.Func, Args: e.Args, Zone: e.Zone}
	cols := srfColumns(b, call, name)
	rows, err := s.srfRows(call, params, cols)
	if err != nil {
		return selectSet{}, ResultColumn{}, err
	}
	col := cols[0]
	col.NoTZ = len(e.Args) > 0 && col.Type == types.Timestamp && exprNoTZ(e.Args[0], nil)
	rest := e
	rest.Func, rest.Args = "", nil
	plain := rest.Cast == "" && len(rest.Path) == 0 && rest.BinOp == ""
	if !plain {
		col.Type = exprFamily(e, nil)
		if col.Type == types.Unknown {
			col.Type = types.String
		}
		col.NoTZ = exprNoTZ(e, nil)
	}
	vals := make([]types.Datum, len(rows))
	for i, r := range rows {
		vals[i] = r[0]
		if plain {
			continue
		}
		v := r[0]
		rest.Lit = &v
		d, err := evalExpr(rest, nil, nil, params)
		if err != nil {
			return selectSet{}, ResultColumn{}, err
		}
		vals[i] = conformTo(d, col.Type)
	}
	return selectSet{vals: vals}, col, nil
}

// exprNoTZ reports whether an expression yields a timestamp WITHOUT
// time zone, which describes as timestamp (OID 1114) and renders
// without an offset: a ::timestamp cast, such a column (colNoTZ), and
// AT TIME ZONE applied to a timestamp with time zone (or the reverse
// of one without).
func exprNoTZ(e parser.Expr, colNoTZ func(name string) bool) bool {
	if e.Cast != "" {
		name := strings.ToLower(strings.TrimSpace(e.Cast))
		if i := strings.IndexByte(name, '('); i >= 0 {
			name = strings.TrimSpace(name[:i])
		}
		return name == "timestamp" || name == "timestamp without time zone" || name == "pg_catalog.timestamp"
	}
	if len(e.Path) > 0 {
		return false
	}
	if e.BinOp != "" {
		// timestamp ± interval stays a timestamp without time zone.
		if e.BinOp != "+" && e.BinOp != "-" || e.Right == nil {
			return false
		}
		base := e
		base.BinOp, base.Right = "", nil
		return exprNoTZ(base, colNoTZ) && exprFamily(*e.Right, nil) != types.Timestamp
	}
	switch {
	case e.Left != nil:
		return exprNoTZ(*e.Left, colNoTZ)
	case e.Func == "date_trunc" && len(e.Args) == 2:
		return exprNoTZ(e.Args[1], colNoTZ)
	case e.Func == "timezone" && len(e.Args) == 2:
		return !exprNoTZ(e.Args[1], colNoTZ)
	case e.Column != "" && colNoTZ != nil:
		return colNoTZ(e.Column)
	case e.Lit != nil:
		return !e.Lit.Null && e.Lit.Fam == types.Timestamp && e.Lit.NoTZ
	}
	return false
}
