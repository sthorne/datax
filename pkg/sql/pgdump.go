package sql

import (
	"context"
	"strconv"

	"github.com/sthorne/datax/pkg/kvclient"
	"github.com/sthorne/datax/pkg/sql/catalog"
	"github.com/sthorne/datax/pkg/sql/parser"
	"github.com/sthorne/datax/pkg/sql/types"
	"github.com/sthorne/datax/pkg/sql/vtable"
)

// The catalog functions pg_dump calls that no other client does.

// spliceACLDefault resolves acldefault(kind, owner): the ACL an object
// of that kind owned by that role has before any GRANT, as text. With
// constant arguments it is a value; with arguments read from the row
// (acldefault('n', n.nspowner)) a per-row call over the cluster's
// roles, read once here.
func (s *Session) spliceACLDefault(ctx context.Context, txn *kvclient.Txn, e parser.Expr, params []types.Datum, keepVolatile bool) (parser.Expr, error) {
	if len(e.Args) != 2 {
		return e, newErrf(CodeUndefinedFunction, "function acldefault() takes a kind and an owner")
	}
	roles, err := catalog.ListRoles(ctx, txn)
	if err != nil {
		return e, err
	}
	names := make(map[int64]string, len(roles))
	for _, r := range roles {
		names[vtable.RoleOID(r.Name)] = r.Name
	}
	fn := func(args []types.Datum) (types.Datum, error) {
		if args[0].Null || args[1].Null {
			return types.DNull, nil
		}
		owner, err := args[1].Coerce(types.Int)
		if err != nil {
			return types.Datum{}, newErrf(CodeInvalidTextRepresentation, "acldefault: owner: %v", err)
		}
		name, ok := names[owner.I]
		if !ok {
			name = strconv.FormatInt(owner.I, 10)
		}
		acl, ok := vtable.DefaultACL(args[0].Text(), name)
		if !ok {
			return types.Datum{}, newErrf(CodeInternal, "unrecognized object type abbreviation: %s", args[0].Text())
		}
		return types.NewString(acl), nil
	}
	args := make([]parser.Expr, len(e.Args))
	constant := true
	for i, a := range e.Args {
		r, err := s.resolveValueExprOpts(ctx, txn, a, params, keepVolatile)
		if err != nil {
			return e, err
		}
		args[i] = r
		constant = constant && !exprHasColumn(r)
	}
	if constant {
		vals := make([]types.Datum, len(args))
		for i, a := range args {
			d, err := evalExpr(a, nil, nil, params)
			if err != nil {
				return e, err
			}
			vals[i] = d
		}
		d, err := fn(vals)
		if err != nil {
			return e, err
		}
		return parser.Expr{Lit: &d}, nil
	}
	return s.registerRowFunc(fn, args), nil
}
