package sql

import (
	"strings"

	"github.com/sthorne/datax/pkg/sql/builtins"
	"github.com/sthorne/datax/pkg/sql/parser"
	"github.com/sthorne/datax/pkg/sql/types"
)

// The || operator over jsonb (issue #342). PostgreSQL resolves
// jsonb || jsonb to jsonb_concat, and an untyped operand (a bare string
// literal, a parameter, NULL) beside a jsonb takes the jsonb type; a
// text-typed operand instead picks anynonarray || text, which
// concatenates the texts. datax keeps no json/jsonb distinction in values
// (JSON is stored as JSONB), so json || json merges like jsonb: a
// deliberate difference from PostgreSQL, which has no || on json.

// concatOperand is what the || resolution reads about one operand: its
// family and whether it is untyped (PostgreSQL's unknown).
type concatOperand struct {
	fam     types.Family
	untyped bool
}

// untypedLeaf reports whether a node's own value (before its operator,
// path and cast) is untyped: a bare string literal, NULL or a parameter.
func untypedLeaf(x parser.Expr) bool {
	if x.Func != "" || x.Case != nil || x.Sub != nil || x.Column != "" || x.Cmp != nil || x.Agg != nil || x.Window != nil {
		return false
	}
	if x.Lit != nil {
		return x.Lit.Null || x.Lit.Fam == types.String
	}
	return x.Param > 0
}

// concatRight describes a whole operand expression.
func concatRight(x parser.Expr, fam types.Family) concatOperand {
	if x.Cast != "" {
		return concatOperand{fam: fam}
	}
	if x.BinOp != "" || len(x.Path) > 0 || x.Left != nil {
		return concatOperand{fam: fam}
	}
	return concatOperand{fam: fam, untyped: untypedLeaf(x)}
}

// concatLeft describes the left operand of the binary node e: its Left
// when set, else the node's own value with any ->/->> path applied.
func concatLeft(e parser.Expr, fam types.Family) concatOperand {
	switch {
	case len(e.Path) > 0:
		return concatOperand{fam: fam}
	case e.Left != nil:
		return concatRight(*e.Left, fam)
	}
	return concatOperand{fam: fam, untyped: untypedLeaf(e)}
}

// isJSONB: a json or jsonb operand (one family in datax).
func (o concatOperand) isJSONB() bool { return o.fam == types.Jsonb }

// jsonbConcatKind resolves l || r over json and jsonb operands: jsonb
// when it is jsonb concatenation, an error when PostgreSQL has no such
// operator, and ok false for every other || (text, arrays, tsvector),
// which keeps its existing rules.
func jsonbConcatKind(l, r concatOperand) (ok bool, err error) {
	switch {
	case l.isJSONB() && (r.isJSONB() || r.untyped):
		return true, nil
	case r.isJSONB() && l.untyped:
		return true, nil
	case l.isJSONB() && !concatsWithJSONB(r.fam), r.isJSONB() && !concatsWithJSONB(l.fam):
		return true, newErrf(CodeUndefinedFunction, "operator does not exist: %s || %s", l.typeName(), r.typeName())
	}
	return false, nil
}

// concatsWithJSONB: the families || accepts beside a typed jsonb under
// another operator — text (anynonarray || text), arrays and tsvector
// (their own operators decide), and an unknown family (NULL).
func concatsWithJSONB(f types.Family) bool {
	return f == types.String || f == types.Unknown || f == types.Tsvector || f.IsArray()
}

func (o concatOperand) typeName() string {
	return strings.ToLower(o.fam.String())
}

// evalJSONBConcat evaluates the || of node e when it resolves to jsonb
// concatenation; handled is false for every other ||.
func evalJSONBConcat(e parser.Expr, l, r types.Datum) (d types.Datum, handled bool, err error) {
	lo, ro := concatLeft(e, l.Fam), concatRight(*e.Right, r.Fam)
	if l.Null || r.Null {
		// A NULL operand's family is unknown at run time; the result is
		// NULL either way.
		return types.DNull, false, nil
	}
	ok, err := jsonbConcatKind(lo, ro)
	if !ok || err != nil {
		return types.Datum{}, ok, err
	}
	if l, err = asJSONB(l); err != nil {
		return types.Datum{}, true, err
	}
	if r, err = asJSONB(r); err != nil {
		return types.Datum{}, true, err
	}
	d, err = builtins.ConcatJSONB(l, r)
	return d, true, builtinErr(err)
}

// asJSONB reads an untyped operand as jsonb input.
func asJSONB(d types.Datum) (types.Datum, error) {
	if d.Fam == types.Jsonb {
		return d, nil
	}
	v, err := builtins.CastIn(d, "jsonb", nil)
	return v, builtinErr(err)
}

// jsonbConcatFamily is the static result family of the || of node e
// given its operands' families: Jsonb when it resolves to jsonb
// concatenation, ok false otherwise.
func jsonbConcatFamily(e parser.Expr, l, r types.Family) (types.Family, bool) {
	if e.BinOp != "||" || e.Right == nil {
		return types.Unknown, false
	}
	if ok, err := jsonbConcatKind(concatLeft(e, l), concatRight(*e.Right, r)); ok && err == nil {
		return types.Jsonb, true
	}
	return types.Unknown, false
}
