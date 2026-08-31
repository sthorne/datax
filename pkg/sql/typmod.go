package sql

import (
	"github.com/sthorne/datax/pkg/sql/catalog"
	"github.com/sthorne/datax/pkg/sql/types"
	"github.com/sthorne/datax/pkg/util/decimal"
)

// colTypmod is the wire typmod for a projected column: set only for
// enforced DECIMAL(p,s) columns, 0 (emitted as -1) otherwise.
func colTypmod(col catalog.Column) int32 {
	if col.Precision > 0 && col.Type == types.Decimal {
		return DecimalTypmod(col.Precision, col.Scale)
	}
	return 0
}

// enforceTypmod applies a DECIMAL(p,s) column's declared precision and
// scale to a value about to be stored: the value is rescaled to s
// (round-half-even), and rejected with SQLSTATE 22003 when its integer
// digits exceed p−s — PostgreSQL semantics, including the order (9.999
// into DECIMAL(3,2) first rounds to 10.00, then overflows). The returned
// datum keeps canonical text in S (identity/storage) and carries the
// declared scale in Dscale (display). Columns without a typmod, non-
// decimal columns, and NULLs pass through untouched.
func enforceTypmod(col catalog.Column, d types.Datum) (types.Datum, error) {
	if d.Null || col.Precision == 0 || col.Type != types.Decimal || d.Fam != types.Decimal {
		return d, nil
	}
	v, err := decimal.Parse(d.S)
	if err != nil {
		return d, newErrf(CodeInvalidTextRepresentation, "column %q: %v", col.Name, err)
	}
	q, err := decimal.Quantize(v, col.Scale)
	if err != nil {
		return d, newErrf(CodeNumericValueOutOfRange, "column %q: %v", col.Name, err)
	}
	abs := q
	if abs.Sign() < 0 {
		abs = decimal.Neg(abs)
	}
	limit := decimal.New(1, col.Precision-col.Scale)
	if decimal.Cmp(abs, limit) >= 0 {
		return d, newErrf(CodeNumericValueOutOfRange,
			"numeric field overflow in column %q: a field with precision %d, scale %d must round to an absolute value less than 10^%d",
			col.Name, col.Precision, col.Scale, col.Precision-col.Scale)
	}
	out := types.NewDecimal(q.String())
	out.Dscale = col.Scale
	return out, nil
}
