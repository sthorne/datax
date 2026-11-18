package builtins

import (
	"errors"
	"math"
	"strconv"
	"strings"

	"github.com/sthorne/datax/pkg/sql/types"
	"github.com/sthorne/datax/pkg/util/decimal"
)

// Cast performs CAST(d AS typ) / d::typ with PostgreSQL's conversion
// rules: text parses into every type (22P02 when it does not fit the
// syntax), numbers convert among themselves (rounding into integers,
// 22003 when out of range), everything renders to text, booleans and
// integers convert both ways, dates and timestamps widen and narrow.
// typ is the lower-cased type name as written, typmod included
// (numeric(10,2), varchar(20)); the catalog-only types tools cast to
// (name, oid, regtype, regnamespace, "char", ...) and any other name
// datax does not model pass the value through unchanged, since it is
// already in its representational form. A name that is none of these
// (KnownCastType) is refused before execution (42704), except a
// user-defined type the session resolves.
func Cast(d types.Datum, typ string) (types.Datum, error) {
	name, mods := splitTypmod(strings.ToLower(strings.TrimSpace(typ)))
	if i := strings.LastIndexByte(name, '.'); i >= 0 {
		name = name[i+1:]
	}
	if d.Null {
		return types.DNull, nil
	}
	if d.Reg != "" && !strings.HasPrefix(name, "reg") {
		// A reg* value cast to anything else (::oid, ::int4) is its
		// OID; to text, its name (Text renders it before this).
		if isTextType(name) {
			return types.NewString(d.Reg), nil
		}
		d.Reg = ""
	}
	switch name {
	case "int2", "smallint", "int4", "int", "integer":
		if f, ok := types.DecimalSpecial(d); ok {
			target := "integer"
			if name == "int2" || name == "smallint" {
				target = "smallint"
			}
			if math.IsNaN(f) {
				return types.Datum{}, errf(CodeNotSupported, "cannot convert NaN to %s", target)
			}
			return types.Datum{}, errf(CodeNotSupported, "cannot convert infinity to %s", target)
		}
	}
	switch name {
	case "int2", "smallint":
		v, err := toInt(d)
		if err != nil {
			return types.Datum{}, err
		}
		if v.I < math.MinInt16 || v.I > math.MaxInt16 {
			return types.Datum{}, errf(CodeOutOfRange, "smallint out of range")
		}
		return v, nil
	case "int4", "int", "integer":
		v, err := toInt(d)
		if err != nil {
			return types.Datum{}, err
		}
		if v.I < math.MinInt32 || v.I > math.MaxInt32 {
			return types.Datum{}, errf(CodeOutOfRange, "integer out of range")
		}
		return v, nil
	case "int8", "bigint":
		return toInt(d)
	case "float4", "real":
		v, err := toFloat(d)
		if err != nil {
			return types.Datum{}, err
		}
		if !math.IsInf(v.F, 0) && (v.F > math.MaxFloat32 || v.F < -math.MaxFloat32) {
			return types.Datum{}, errf(CodeOutOfRange, "value out of range: overflow")
		}
		return types.NewFloat(float64(float32(v.F))), nil
	case "float8", "float", "double", "double precision":
		return toFloat(d)
	case "numeric", "decimal", "dec":
		v, err := toDecimal(d)
		if err != nil {
			return types.Datum{}, err
		}
		if len(mods) > 0 {
			return quantizeTo(v, mods)
		}
		return v, nil
	case "text", "varchar", "char", "character", "bpchar", "string", "character varying":
		s := d.Text()
		if len(mods) == 1 && (name == "varchar" || name == "char" || name == "character" || name == "bpchar") {
			if n, err := strconv.Atoi(mods[0]); err == nil && n >= 0 {
				if r := []rune(s); len(r) > n {
					s = string(r[:n])
				}
			}
		}
		return types.NewString(s), nil
	case "bool", "boolean":
		return toBool(d)
	case "timestamp", "timestamptz", "timestamp with time zone", "timestamp without time zone":
		// A timestamp WITHOUT time zone is the same wall-clock count
		// marked for display without an offset (the session's zone is
		// UTC, so the two convert into each other unchanged); text
		// input to it ignores any offset written, as in PostgreSQL.
		noTZ := name == "timestamp" || name == "timestamp without time zone"
		switch d.Fam {
		case types.Timestamp:
			d.NoTZ = noTZ
			return d, nil
		case types.String:
			if noTZ {
				n, err := types.ParseTimestampNoTZ(d.S)
				if err != nil {
					return types.Datum{}, timestampErr(err, d.Text())
				}
				v := types.NewTimestamp(n)
				v.NoTZ = true
				return v, nil
			}
			fallthrough
		case types.Date:
			v, err := d.Coerce(types.Timestamp)
			if err != nil {
				return types.Datum{}, timestampErr(err, d.Text())
			}
			v.NoTZ = noTZ
			return v, nil
		}
	case "interval":
		switch d.Fam {
		case types.IntervalFam:
			return d, nil
		case types.String, types.Time:
			iv, err := toInterval(d)
			if err != nil {
				return types.Datum{}, err
			}
			return types.NewInterval(iv), nil
		}
	case "time", "time without time zone", "time with time zone", "timetz":
		switch d.Fam {
		case types.Time:
			return d, nil
		case types.String:
			v, err := d.Coerce(types.Time)
			if err != nil {
				return types.Datum{}, errf(CodeInvalidDatetime, "%v", err)
			}
			return v, nil
		case types.Timestamp, types.IntervalFam:
			return d.Coerce(types.Time)
		}
	case "date":
		switch d.Fam {
		case types.Date:
			return d, nil
		case types.Timestamp:
			return types.NewDate(floorDiv(d.I, 86400*1e9)), nil
		case types.String:
			v, err := d.Coerce(types.Date)
			if err != nil {
				// A timestamp string narrows to its date.
				if ts, terr := d.Coerce(types.Timestamp); terr == nil {
					return types.NewDate(floorDiv(ts.I, 86400*1e9)), nil
				}
				return types.Datum{}, errf(CodeInvalidDatetime, "invalid input syntax for type date: %q", d.S)
			}
			return v, nil
		}
	case "bytea", "bytes", "blob":
		switch d.Fam {
		case types.Bytes:
			return d, nil
		case types.String:
			v, err := d.Coerce(types.Bytes)
			if err != nil {
				return types.Datum{}, errf(CodeInvalidText, "invalid input syntax for type bytea: %q", d.S)
			}
			return v, nil
		}
		return types.NewBytes([]byte(d.Text())), nil
	case "uuid":
		switch d.Fam {
		case types.Uuid:
			return d, nil
		case types.String:
			v, err := d.Coerce(types.Uuid)
			if err != nil {
				return types.Datum{}, errf(CodeInvalidText, "invalid input syntax for type uuid: %q", d.S)
			}
			return v, nil
		}
	case "jsonb", "json":
		if d.Fam == types.String {
			// Text parses as JSON (to_jsonb() is what wraps it as a string).
			v, err := types.ParseJSONB(d.S)
			if err != nil {
				return types.Datum{}, errf(CodeInvalidText, "invalid input syntax for type json: %v", err)
			}
			return v, nil
		}
		return ToJSONB(d)
	case "tsvector", "tsquery":
		// Text is read as a literal of the type: no normalization (that
		// is to_tsvector's and to_tsquery's job).
		fam := types.Tsvector
		if name == "tsquery" {
			fam = types.Tsquery
		}
		if d.Fam == fam {
			return d, nil
		}
		if d.Fam == types.String {
			v, err := d.Coerce(fam)
			if err != nil {
				return types.Datum{}, errf("42601", "%v", err)
			}
			return v, nil
		}
	case "vector":
		// Text in the '[1,2,3]' form and numeric arrays become vectors;
		// vector(n) also checks the dimension.
		v := d
		if d.Fam != types.Vector {
			a, err := asVector(d)
			if err != nil {
				return types.Datum{}, err
			}
			v = types.NewVector(a)
		}
		if len(mods) == 1 {
			if n, err := strconv.Atoi(mods[0]); err == nil && n != v.VectorDims() {
				return types.Datum{}, errf(CodeDataException, "expected %d dimensions, not %d", n, v.VectorDims())
			}
		}
		return v, nil
	}
	if passThroughTypes[name] {
		return d, nil
	}
	if strings.HasSuffix(name, "[]") {
		fam, err := types.ParseType(name)
		if err != nil {
			return d, nil // an array of a type datax does not model: left as it is
		}
		if d.Fam == fam {
			return d, nil
		}
		v, err := d.Coerce(fam)
		if err != nil {
			return types.Datum{}, errf(CodeInvalidText, "invalid input syntax for type %s: %v", strings.ToLower(fam.String()), err)
		}
		return v, nil
	}
	if _, err := types.ParseType(name); err != nil {
		// Not a type datax models: leave the value as it is.
		return d, nil
	}
	return types.Datum{}, errf(CodeNotSupported, "cannot cast type %s to %s", strings.ToLower(d.Fam.String()), name)
}

// passThroughTypes are the catalog-only types tools cast to: datax
// keeps such a value in its representational form.
var passThroughTypes = map[string]bool{
	"regclass": true, "name": true, "oid": true, "regtype": true, "regnamespace": true, "regproc": true,
	"regprocedure": true, "regrole": true, "regoper": true, "regoperator": true, "regconfig": true,
	"regdictionary": true, "xid": true, "xid8": true, "tid": true, "cid": true, "int2vector": true,
	"oidvector": true, "cstring": true, "aclitem": true, "pg_node_tree": true, "unknown": true, "any": true,
	"anyelement": true, "anyarray": true, "record": true, "void": true,
}

// castTypes are the type names Cast converts to (after the qualifier and
// typmod are removed), multi-word spellings included.
var castTypes = map[string]bool{
	"int2": true, "smallint": true, "int4": true, "int": true, "integer": true, "int8": true, "bigint": true,
	"float4": true, "real": true, "float8": true, "float": true, "double": true, "double precision": true,
	"numeric": true, "decimal": true, "dec": true,
	"text": true, "varchar": true, "char": true, "character": true, "bpchar": true, "string": true, "character varying": true,
	"bool": true, "boolean": true,
	"timestamp": true, "timestamptz": true, "timestamp with time zone": true, "timestamp without time zone": true,
	"interval": true, "time": true, "time without time zone": true, "time with time zone": true, "timetz": true,
	"date": true, "bytea": true, "bytes": true, "blob": true, "uuid": true, "json": true, "jsonb": true,
	"tsvector": true, "tsquery": true, "vector": true,
}

// KnownCastType reports whether typ (as a cast writes it: lower-cased,
// optionally qualified, with a typmod and array brackets) names a type
// datax has: one Cast converts to, a catalog type it passes through, or
// an array of either. Any other name is a user-defined type or none.
func KnownCastType(typ string) bool {
	name := CastBaseType(typ)
	return castTypes[name] || passThroughTypes[name]
}

// CastBaseType is typ without its qualifier, typmod and array brackets
// (pg_catalog.numeric(10,2)[] is numeric): the name a catalog lookup of
// a user-defined type takes.
func CastBaseType(typ string) string {
	// Case is kept: an unquoted name reaches here folded to lower case,
	// and a quoted mixed-case one ("Role") names a user-defined type.
	name, _ := splitTypmod(strings.TrimSpace(typ))
	for strings.HasSuffix(name, "[]") {
		name = strings.TrimSpace(strings.TrimSuffix(name, "[]"))
	}
	if i := strings.LastIndexByte(name, '.'); i >= 0 {
		name = name[i+1:]
	}
	return name
}

// splitTypmod separates numeric(10,2) into "numeric" and ["10", "2"].
func splitTypmod(typ string) (string, []string) {
	i := strings.IndexByte(typ, '(')
	if i < 0 {
		return typ, nil
	}
	j := strings.IndexByte(typ, ')')
	if j < i {
		return typ, nil
	}
	var mods []string
	for _, m := range strings.Split(typ[i+1:j], ",") {
		mods = append(mods, strings.TrimSpace(m))
	}
	return strings.TrimSpace(typ[:i]) + typ[j+1:], mods
}

func floorDiv(a, b int64) int64 {
	q := a / b
	if a%b != 0 && (a < 0) != (b < 0) {
		q--
	}
	return q
}

// toInt converts to INT8: text parses, floats and decimals round (halves
// away from zero), booleans are 0/1.
func toInt(d types.Datum) (types.Datum, error) {
	switch d.Fam {
	case types.Int:
		return d, nil
	case types.Float:
		if math.IsNaN(d.F) || d.F >= math.MaxInt64 || d.F < math.MinInt64 {
			return types.Datum{}, errf(CodeOutOfRange, "bigint out of range")
		}
		return types.NewInt(int64(math.RoundToEven(d.F))), nil
	case types.Decimal:
		if f, ok := types.DecimalSpecial(d); ok {
			if math.IsNaN(f) {
				return types.Datum{}, errf(CodeNotSupported, "cannot convert NaN to bigint")
			}
			return types.Datum{}, errf(CodeNotSupported, "cannot convert infinity to bigint")
		}
		v, err := d.DecimalVal()
		if err != nil {
			return types.Datum{}, errf(CodeInvalidText, "%v", err)
		}
		r := decimal.RoundHalfAway(v, 0)
		i, err := strconv.ParseInt(r.String(), 10, 64)
		if err != nil {
			return types.Datum{}, errf(CodeOutOfRange, "bigint out of range")
		}
		return types.NewInt(i), nil
	case types.Bool:
		if d.B {
			return types.NewInt(1), nil
		}
		return types.NewInt(0), nil
	case types.String:
		s := strings.TrimSpace(d.S)
		i, err := strconv.ParseInt(s, 10, 64)
		if err != nil {
			if ne, ok := err.(*strconv.NumError); ok && ne.Err == strconv.ErrRange {
				return types.Datum{}, errf(CodeOutOfRange, "value %q is out of range for type bigint", d.S)
			}
			// A decimal string rounds, as PostgreSQL's numeric → int does.
			if dd, derr := types.ParseDecimal(s); derr == nil {
				return toInt(dd)
			}
			return types.Datum{}, errf(CodeInvalidText, "invalid input syntax for type bigint: %q", d.S)
		}
		return types.NewInt(i), nil
	}
	return types.Datum{}, errf(CodeNotSupported, "cannot cast type %s to bigint", strings.ToLower(d.Fam.String()))
}

func toFloat(d types.Datum) (types.Datum, error) {
	switch d.Fam {
	case types.Float:
		return d, nil
	case types.Int, types.Decimal:
		v, err := d.Coerce(types.Float)
		if err != nil {
			return types.Datum{}, errf(CodeInvalidText, "%v", err)
		}
		return v, nil
	case types.Bool:
		if d.B {
			return types.NewFloat(1), nil
		}
		return types.NewFloat(0), nil
	case types.String:
		s := strings.TrimSpace(d.S)
		f, err := strconv.ParseFloat(s, 64)
		if err != nil {
			switch strings.ToLower(s) {
			case "nan":
				return types.NewFloat(math.NaN()), nil
			case "infinity", "inf", "+inf", "+infinity":
				return types.NewFloat(math.Inf(1)), nil
			case "-infinity", "-inf":
				return types.NewFloat(math.Inf(-1)), nil
			}
			return types.Datum{}, errf(CodeInvalidText, "invalid input syntax for type double precision: %q", d.S)
		}
		return types.NewFloat(f), nil
	}
	return types.Datum{}, errf(CodeNotSupported, "cannot cast type %s to double precision", strings.ToLower(d.Fam.String()))
}

func toDecimal(d types.Datum) (types.Datum, error) {
	switch d.Fam {
	case types.Decimal:
		return d, nil
	case types.Int:
		return types.NewDecimal(decimal.FromInt(d.I).String()), nil
	case types.Float:
		// An explicit cast may launder a float: its shortest decimal
		// rendering (what the user sees) is the value.
		if v, ok := types.DecimalFromSpecial(d.F); ok {
			return v, nil // NaN and the infinities are numeric's too
		}
		v, err := types.ParseDecimal(strconv.FormatFloat(d.F, 'f', -1, 64))
		if err != nil {
			return types.Datum{}, errf(CodeInvalidText, "%v", err)
		}
		return v, nil
	case types.Bool:
		if d.B {
			return types.NewDecimal("1"), nil
		}
		return types.NewDecimal("0"), nil
	case types.String:
		v, err := types.ParseDecimal(strings.TrimSpace(d.S))
		if err != nil {
			return types.Datum{}, errf(CodeInvalidText, "invalid input syntax for type numeric: %q", d.S)
		}
		return v, nil
	}
	return types.Datum{}, errf(CodeNotSupported, "cannot cast type %s to numeric", strings.ToLower(d.Fam.String()))
}

// quantizeTo applies a numeric(p[,s]) typmod: the scale rounds (halves
// away from zero), the precision bounds the integer digits (22003).
func quantizeTo(d types.Datum, mods []string) (types.Datum, error) {
	prec, err := strconv.Atoi(mods[0])
	if err != nil || prec < 1 {
		return d, nil
	}
	scale := 0
	if len(mods) > 1 {
		if scale, err = strconv.Atoi(mods[1]); err != nil {
			return d, nil
		}
	}
	if f, ok := types.DecimalSpecial(d); ok {
		if math.IsNaN(f) {
			return d, nil // NaN fits any numeric(p,s)
		}
		return types.Datum{}, errf(CodeOutOfRange, "numeric field overflow: a field with precision %d, scale %d cannot hold an infinite value", prec, scale)
	}
	v, err := d.DecimalVal()
	if err != nil {
		return types.Datum{}, errf(CodeInvalidText, "%v", err)
	}
	r := decimal.RoundHalfAway(v, int32(scale))
	_, digits, e := r.Mantissa()
	if digits != "" && e > int64(prec-scale) {
		return types.Datum{}, errf(CodeOutOfRange, "numeric field overflow: a field with precision %d, scale %d must round to an absolute value less than 10^%d", prec, scale, prec-scale)
	}
	out := types.NewDecimal(r.String())
	out.Dscale = int32(scale)
	return out, nil
}

func toBool(d types.Datum) (types.Datum, error) {
	switch d.Fam {
	case types.Bool:
		return d, nil
	case types.Int:
		return types.NewBool(d.I != 0), nil
	case types.Float:
		return types.NewBool(d.F != 0), nil
	case types.Decimal:
		v, err := d.DecimalVal()
		if err != nil {
			return types.Datum{}, errf(CodeInvalidText, "%v", err)
		}
		return types.NewBool(v.Sign() != 0), nil
	case types.String:
		switch strings.ToLower(strings.TrimSpace(d.S)) {
		case "t", "true", "y", "yes", "on", "1":
			return types.NewBool(true), nil
		case "f", "false", "n", "no", "off", "0":
			return types.NewBool(false), nil
		}
		return types.Datum{}, errf(CodeInvalidText, "invalid input syntax for type boolean: %q", d.S)
	}
	return types.Datum{}, errf(CodeNotSupported, "cannot cast type %s to boolean", strings.ToLower(d.Fam.String()))
}

// ToJSONB converts any value to jsonb the way to_jsonb() does: text
// becomes a JSON string, numbers and booleans themselves, jsonb stays.
func ToJSONB(d types.Datum) (types.Datum, error) {
	if d.Null {
		return types.DNull, nil
	}
	switch d.Fam {
	case types.Jsonb:
		return d, nil
	case types.Int, types.Float, types.Decimal:
		if d.Fam == types.Float && (math.IsNaN(d.F) || math.IsInf(d.F, 0)) {
			return types.Datum{}, errf(CodeInvalidText, "cannot convert %v to json", d.F)
		}
		if _, ok := types.DecimalSpecial(d); ok {
			// JSON has no NaN or Infinity: to_jsonb renders them as
			// strings.
			return types.ParseJSONB(strconv.Quote(d.S))
		}
		return types.ParseJSONB(d.Text())
	case types.Bool:
		return types.ParseJSONB(strconv.FormatBool(d.B))
	case types.String:
		return types.ParseJSONB(strconv.Quote(d.S))
	}
	if t, ok := JSONTimeText(d, nil); ok {
		return types.ParseJSONB(strconv.Quote(t))
	}
	return types.ParseJSONB(strconv.Quote(d.Text()))
}

// CastFamily is the family a cast to typ produces (types.Unknown when
// the value passes through), for output typing.
func CastFamily(typ string, in types.Family) types.Family {
	name, _ := splitTypmod(strings.ToLower(strings.TrimSpace(typ)))
	if i := strings.LastIndexByte(name, '.'); i >= 0 {
		name = name[i+1:]
	}
	switch name {
	case "text", "varchar", "char", "character", "bpchar", "string", "character varying":
		return types.String
	case "double precision":
		return types.Float
	case "timestamp with time zone", "timestamp without time zone":
		return types.Timestamp
	case "time without time zone", "time with time zone", "timetz":
		return types.Time
	}
	if f, err := types.ParseType(name); err == nil {
		return f
	}
	return in
}

// timestampErr is the SQL error for a timestamp that failed to parse:
// datetime_field_overflow when it is outside the representable range,
// invalid_datetime_format otherwise.
func timestampErr(err error, text string) error {
	if errors.Is(err, types.ErrTimestampRange) {
		return errf(CodeDatetimeField, "timestamp out of range: %q", text)
	}
	return errf(CodeInvalidDatetime, "invalid input syntax for type timestamp: %q", text)
}
