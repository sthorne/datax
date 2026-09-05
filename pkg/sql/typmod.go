package sql

import (
	"github.com/sthorne/datax/pkg/sql/catalog"
	"github.com/sthorne/datax/pkg/sql/types"
	"github.com/sthorne/datax/pkg/util/decimal"
)

// colResult describes an output column a table column backs directly:
// the family with the column's type modifiers, so the wire type and
// typmod are the column's.
func colResult(name string, col catalog.Column) ResultColumn {
	return ResultColumn{Name: name, Type: col.Type, Typmod: col.Typmod(),
		Width: col.Width, MaxLen: col.MaxLen, Char: col.Char, NoTZ: col.NoTZ, TimePrecision: col.TimePrecision,
		EnumType: col.EnumType, EnumName: col.EnumName}
}

// exprResult describes an output column computed from col: the
// column's modifiers carry over only while the expression keeps its
// family (a cast or arithmetic loses them, as in PostgreSQL).
func exprResult(name string, typ types.Family, col catalog.Column) ResultColumn {
	if typ != col.Type {
		return ResultColumn{Name: name, Type: typ}
	}
	return colResult(name, col)
}

// stampDisplay sets the display hints a stored value of a column
// carries: the declared scale of a DECIMAL(p,s), CHAR(n) padding,
// TIMESTAMP without time zone — per element for an array column.
func stampDisplay(col *catalog.Column, d types.Datum) types.Datum {
	if d.Null {
		return d
	}
	if col.Type.IsArray() && d.Fam.IsArray() {
		elem := *col
		elem.Type = col.Type.Elem()
		out := make([]types.Datum, len(d.A))
		for i, e := range d.A {
			out[i] = stampDisplay(&elem, e)
		}
		return types.NewArray(elem.Type, out)
	}
	switch {
	case col.Type == types.Decimal && col.Precision > 0 && d.Fam == types.Decimal:
		d.Dscale = col.Scale
	case col.Char || col.NoTZ:
		d = col.Stamp(d)
	}
	return d
}

// coerceColumn brings a value to its column's family the way the
// write path does: text into a TIMESTAMP (without time zone) column
// parses ignoring any offset, everything else takes the family's
// coercion. The SQLSTATE is 22P02 for text that does not parse.
func coerceColumn(col catalog.Column, d types.Datum) (types.Datum, *Error) {
	if !d.Null && col.Type == types.Enum {
		v, err := col.EnumValue(d)
		if err != nil {
			return d, ToSQLError(err)
		}
		return v, nil
	}
	if !d.Null && d.Fam == types.String && col.Type == types.Timestamp && col.NoTZ {
		n, err := types.ParseTimestampNoTZ(d.S)
		if err != nil {
			return d, newErrf(CodeInvalidTextRepresentation, "column %q: %v", col.Name, err)
		}
		return types.NewTimestamp(n), nil
	}
	out, err := d.Coerce(col.Type)
	if err != nil {
		return d, newErrf(CodeInvalidTextRepresentation, "column %q: %v", col.Name, err)
	}
	return out, nil
}

// pureWidening reports whether changing a column's declared type from
// old to new keeps every stored value valid unchanged — a wider integer,
// a longer (or unbounded) VARCHAR, a TIMESTAMP(p) gaining digits — so
// ALTER COLUMN TYPE can rewrite the descriptor alone, in the statement's
// transaction, with no row rewrite.
func pureWidening(old, new catalog.Column) bool {
	if old.Type != new.Type {
		return false
	}
	switch old.Type {
	case types.Enum:
		return old.EnumType == new.EnumType
	case types.Int:
		return new.IntWidth() >= old.IntWidth()
	case types.String:
		if old.Char != new.Char {
			return false
		}
		return new.MaxLen == 0 || (old.MaxLen > 0 && new.MaxLen >= old.MaxLen)
	case types.Timestamp:
		if old.NoTZ != new.NoTZ {
			return false
		}
		return new.TimePrecision == 0 || (old.TimePrecision > 0 && new.TimePrecision >= old.TimePrecision)
	case types.Decimal:
		return old.Precision == new.Precision && old.Scale == new.Scale
	}
	return true
}

// enforceTypmod applies a column's type modifiers to a value about to
// be stored. For DECIMAL(p,s) the value is rescaled to s (round-half-
// even), and rejected with SQLSTATE 22003 when its integer digits
// exceed p−s — PostgreSQL semantics, including the order (9.999 into
// DECIMAL(3,2) first rounds to 10.00, then overflows); the returned
// datum keeps canonical text in S (identity/storage) and carries the
// declared scale in Dscale (display). The integer width, character
// length, CHAR padding and TIMESTAMP modifiers are catalog.Column.
// Conform's (22003 / 22001 / 22007). Columns without a typmod and NULLs
// pass through untouched.
func enforceTypmod(col catalog.Column, d types.Datum) (types.Datum, error) {
	if d.Null {
		return d, nil
	}
	if col.Type.IsArray() {
		// Element by element, under the element type with the column's
		// modifiers.
		out, err := col.Conform(d)
		if err != nil {
			return d, ToSQLError(err)
		}
		if out.Fam.Elem() == types.Decimal && col.Precision > 0 {
			elem := col
			elem.Type = types.Decimal
			elems := make([]types.Datum, len(out.A))
			for i, e := range out.A {
				if elems[i], err = enforceTypmod(elem, e); err != nil {
					return d, err
				}
			}
			out = types.NewArray(types.Decimal, elems)
		}
		return out, nil
	}
	if col.Type != types.Decimal {
		out, err := col.Conform(d)
		if err != nil {
			return d, ToSQLError(err)
		}
		return out, nil
	}
	if col.Precision == 0 || d.Fam != types.Decimal {
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
