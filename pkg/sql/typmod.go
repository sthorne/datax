package sql

import (
	"errors"
	"math"

	"github.com/sthorne/datax/pkg/sql/builtins"
	"github.com/sthorne/datax/pkg/sql/catalog"
	"github.com/sthorne/datax/pkg/sql/types"
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
		if errors.Is(err, types.ErrTimestampRange) {
			return d, newErrf(CodeDatetimeFieldOverflow, "column %q: %v", col.Name, err)
		}
		return d, newErrf(CodeInvalidTextRepresentation, "column %q: %v", col.Name, err)
	}
	return out, nil
}

// assignColumn brings a value to its column's family for storing it:
// coerceColumn plus PostgreSQL's assignment casts (pg_cast castcontext
// 'a') that datax's implicit coercion does not make. A numeric value
// into an integer column rounds half away from zero (2.5 → 3, -2.5 →
// -3), a float8 value rounds half to even (rint: 2.5 → 2), and either
// out of the column's range fails with 22003 "smallint / integer /
// bigint out of range" (a numeric NaN or infinity with 0A000). INSERT
// (VALUES and SELECT), UPDATE and ON CONFLICT DO UPDATE store through
// it; comparisons keep coerceColumn, so WHERE id = 1.5 never matches
// id 2. A float8 value into a numeric column converts as float8::numeric
// does (float8_numeric: 15 significant digits; NaN and the infinities
// are numeric's), and the column's typmod then rounds it. An array value
// into an array column converts element by element with the same casts
// (numeric[] / float8[] into an integer array, float8[] into numeric[]);
// NULL elements stay NULL.
func assignColumn(col catalog.Column, d types.Datum) (types.Datum, *Error) {
	if d.Null {
		return coerceColumn(col, d)
	}
	if col.Type.IsArray() && d.Fam.IsArray() && assignCasts(col.Type.Elem(), d.Fam.Elem()) {
		elem := col
		elem.Type = col.Type.Elem()
		out := make([]types.Datum, len(d.A))
		for i, e := range d.A {
			v, err := assignColumn(elem, e)
			if err != nil {
				return d, err
			}
			out[i] = v
		}
		return types.NewArray(elem.Type, out), nil
	}
	switch {
	case col.Type == types.Decimal && d.Fam == types.Float:
		v, err := builtins.Cast(d, "numeric")
		if err != nil {
			return d, ToSQLError(builtinErr(err))
		}
		return v, nil
	case col.Type == types.Int && (d.Fam == types.Decimal || d.Fam == types.Float):
		name, cast, limit := "bigint", "int8", float64(0)
		switch col.IntWidth() {
		case 2:
			name, cast, limit = "smallint", "int2", math.MaxInt16
		case 4:
			name, cast, limit = "integer", "int4", math.MaxInt32
		}
		if d.Fam == types.Float {
			// dtoi2 / dtoi4 / dtoi8: rint, then the range check
			// (NaN and the infinities are out of range).
			f := math.RoundToEven(d.F)
			if math.IsNaN(f) || (limit > 0 && (f < -limit-1 || f > limit)) ||
				(limit == 0 && (f < math.MinInt64 || f >= math.MaxInt64)) {
				return d, newErrf(CodeNumericValueOutOfRange, "%s out of range", name)
			}
			return types.NewInt(int64(f)), nil
		}
		v, err := builtins.Cast(d, cast)
		if err != nil {
			serr := ToSQLError(builtinErr(err))
			if serr.Code == CodeNumericValueOutOfRange {
				// numeric_int2 / numeric_int4 name the target type even
				// when the value is past bigint too.
				serr = newErrf(CodeNumericValueOutOfRange, "%s out of range", name)
			}
			return d, serr
		}
		return v, nil
	}
	return coerceColumn(col, d)
}

// assignCasts reports whether storing a value of family from into a
// column of family to takes one of assignColumn's assignment casts
// rather than the implicit coercion.
func assignCasts(to, from types.Family) bool {
	switch to {
	case types.Int:
		return from == types.Decimal || from == types.Float
	case types.Decimal:
		return from == types.Float
	}
	return false
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
	if old.Type.IsArray() {
		// The modifiers apply per element: judge the element type
		// (DECIMAL(10,3)[] to DECIMAL(10,2)[] must round every element).
		old.Type, new.Type = old.Type.Elem(), new.Type.Elem()
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
	case types.Vector:
		// Dropping the declared dimension keeps every value valid;
		// declaring or changing one does not.
		return new.MaxLen == 0 || old.MaxLen == new.MaxLen
	}
	return true
}

// enforceTypmod applies a column's type modifiers to a value about to
// be stored. For DECIMAL(p,s) the value is rescaled to s (halves away
// from zero: 1.005 → 1.01, -1.005 → -1.01), and rejected with SQLSTATE 22003 when its integer digits
// exceed p−s — PostgreSQL semantics, including the order (9.999 into
// DECIMAL(3,2) first rounds to 10.00, then overflows); the returned
// datum keeps canonical text in S (identity/storage) and carries the
// declared scale in Dscale (display). That part is
// catalog.Column.ConformDecimal, which ALTER COLUMN TYPE's rewrite
// (rowenc.EncodeValue) applies too. The integer width, character
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
		if out, err = col.ConformDecimal(out); err != nil {
			return d, ToSQLError(err)
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
	out, err := col.ConformDecimal(d)
	if err != nil {
		return d, ToSQLError(err)
	}
	return out, nil
}
