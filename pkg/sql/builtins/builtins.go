// Package builtins is the registry of SQL builtin scalar functions: each
// one's name, argument families, return family, volatility, and whether
// a DEFAULT or CHECK may use it. The parser checks arity against it,
// the evaluator calls through it, pg_proc lists it, and the Functions
// reference in the docs is generated from it, so the four never drift.
//
// Functions that need the session, the catalog or the transaction
// (now(), current_user, nextval, the pg_get_* renderings) are not here:
// the session splices or evaluates them before the row loop
// (pkg/sql/subquery.go, pkg/sql/sequence.go).
package builtins

import (
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/sthorne/datax/pkg/sql/types"
)

// Volatility classifies a function by what its result depends on.
type Volatility int

const (
	// Immutable: the arguments alone (usable in a DEFAULT or a CHECK).
	Immutable Volatility = iota
	// Stable: the arguments and the statement (now(), current_user).
	Stable
	// Volatile: anything; evaluated afresh per row (random(),
	// gen_random_uuid()).
	Volatile
)

func (v Volatility) String() string {
	switch v {
	case Immutable:
		return "immutable"
	case Stable:
		return "stable"
	}
	return "volatile"
}

// Any accepts an argument of any family.
const Any types.Family = types.Unknown

// Builtin describes one function.
type Builtin struct {
	Name string
	// Args are the parameter families by position (Any for any);
	// MinArgs the number required (the rest optional); Variadic accepts
	// more arguments of the last family.
	Args     []types.Family
	MinArgs  int
	Variadic bool
	// Ret is the result family; SameAsArg the position whose family the
	// result takes when Ret is Any (greatest, coalesce, round).
	Ret       types.Family
	SameAsArg int
	Vol       Volatility
	// Strict functions return NULL when any argument is NULL without
	// being called (the SQL default); the others see the NULLs.
	NotStrict bool
	// Fn evaluates the call on coerced arguments.
	Fn func(args []types.Datum) (types.Datum, error)
	// Alt, when set, sees the arguments before coercion and may take
	// the call (ok true) for a form the declared families do not
	// describe: substring's text pattern in place of its position.
	Alt func(args []types.Datum) (d types.Datum, ok bool, err error)
	// Category and Doc feed the reference: a section and one line.
	Category string
	Doc      string
	// Hidden keeps aliases (character_length, ceiling, ...) out of the
	// reference, which lists them beside their canonical name.
	Hidden bool
	// Aliases are other names for the same function.
	Aliases []string
	// Bytea marks a text function that also has a bytea form
	// (length, octet_length, substring, position): a bytea argument
	// reaches Fn as itself, its raw bytes in S, instead of being
	// rendered as its hex text, and Fn dispatches on the family.
	Bytea bool
	// Session marks a function the session evaluates itself (no Fn):
	// listed and arity-checked here, called elsewhere.
	Session bool
	// Rows marks a set-returning function (in place of Fn): it yields
	// rows of Columns, called in FROM or a FROM-less select list. A
	// column named "" takes the FROM alias (or the function's name), as
	// a scalar-returning PostgreSQL function's does. ColFams are the
	// columns' families after the first, whose family is the result
	// family (Ret, or RetOf).
	Rows    func(args []types.Datum) (RowIter, error)
	Columns []string
	ColFams []types.Family
	// ZFn and ZRows, when set, are Fn and Rows under a session TimeZone
	// that is not UTC: they read and render timestamps with time zone
	// in it (date_trunc, extract, to_char, generate_series, ...). A nil
	// zone is UTC.
	ZFn   func(args []types.Datum, loc *time.Location) (types.Datum, error)
	ZRows func(args []types.Datum, loc *time.Location) (RowIter, error)
	// RetOf, when set, is the result family given the argument
	// families (generate_series yields what its bounds are).
	RetOf func(args []types.Family) types.Family
}

// Error is an evaluation error with a SQLSTATE.
type Error struct {
	Code string
	Msg  string
}

func (e *Error) Error() string { return e.Msg }

// SQLSTATEs the builtins raise.
const (
	CodeInvalidText     = "22P02" // invalid_text_representation
	CodeOutOfRange      = "22003" // numeric_value_out_of_range
	CodeDivisionByZero  = "22012"
	CodeInvalidArgument = "22023" // invalid_parameter_value
	CodeUndefined       = "42883" // undefined_function
	CodeStringLength    = "22026" // string_data_length_mismatch
	CodeDatetimeField   = "22008" // datetime_field_overflow
	CodeInvalidDatetime = "22007" // invalid_datetime_format
	CodeNotSupported    = "0A000"
	CodeInvalidRegexp   = "2201B"
	CodeInvalidEscape   = "22025"
	// CodeInvalidEscapeUse is invalid_use_of_escape_character.
	CodeInvalidEscapeUse = "2200C"
)

func errf(code, format string, args ...any) error {
	return &Error{Code: code, Msg: fmt.Sprintf(format, args...)}
}

var registry = map[string]*Builtin{}

// register adds b under its name and aliases.
func register(b *Builtin) {
	if b.Category == "" {
		panic("builtin " + b.Name + " has no category")
	}
	if _, dup := registry[b.Name]; dup {
		panic("builtin " + b.Name + " registered twice")
	}
	registry[b.Name] = b
	for _, a := range b.Aliases {
		alias := *b
		alias.Name, alias.Hidden, alias.Aliases = a, true, nil
		if _, dup := registry[a]; dup {
			panic("builtin " + a + " registered twice")
		}
		registry[a] = &alias
	}
}

// Lookup finds a builtin by (lower-cased) name.
func Lookup(name string) (*Builtin, bool) {
	b, ok := registry[strings.ToLower(name)]
	return b, ok
}

// All lists every builtin by name, aliases included.
func All() []*Builtin {
	out := make([]*Builtin, 0, len(registry))
	for _, b := range registry {
		out = append(out, b)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out
}

// ArityOK reports whether a call with n arguments is well-formed.
func (b *Builtin) ArityOK(n int) bool {
	if n < b.MinArgs {
		return false
	}
	return n <= len(b.Args) || b.Variadic
}

// ArityText renders the accepted argument count for an error.
func (b *Builtin) ArityText() string {
	switch {
	case b.Variadic && b.MinArgs == len(b.Args):
		return fmt.Sprintf("at least %d", b.MinArgs)
	case b.Variadic:
		return fmt.Sprintf("at least %d", b.MinArgs)
	case b.MinArgs == len(b.Args):
		return fmt.Sprintf("%d", b.MinArgs)
	}
	return fmt.Sprintf("%d to %d", b.MinArgs, len(b.Args))
}

// argFamily is the family expected at position i.
// ArgFamily is the declared family of the i-th argument (Any when
// unconstrained; the variadic tail's family past the declared list).
func (b *Builtin) ArgFamily(i int) types.Family { return b.argFamily(i) }

func (b *Builtin) argFamily(i int) types.Family {
	if i < len(b.Args) {
		return b.Args[i]
	}
	return b.Args[len(b.Args)-1]
}

// ResultFamily is the family of the call's result given the argument
// families (types.Unknown when it cannot be told statically).
func (b *Builtin) ResultFamily(args []types.Family) types.Family {
	if b.RetOf != nil {
		return b.RetOf(args)
	}
	if b.Ret != Any {
		return b.Ret
	}
	if b.SameAsArg < len(args) {
		return args[b.SameAsArg]
	}
	return Any
}

// Call evaluates the builtin: NULL handling for strict functions, then
// argument coercion to the declared families (numeric families lift
// among themselves; text parses into the others), then Fn.
func (b *Builtin) Call(args []types.Datum) (types.Datum, error) { return b.CallIn(args, nil) }

// ZoneSensitive reports whether the builtin's result depends on the
// session's TimeZone.
func (b *Builtin) ZoneSensitive() bool { return b.ZFn != nil || b.ZRows != nil }

// CallIn is Call under the session's TimeZone (nil or UTC: Call).
func (b *Builtin) CallIn(args []types.Datum, loc *time.Location) (types.Datum, error) {
	if b.Rows != nil {
		return types.Datum{}, errf(CodeNotSupported, "set-returning function %s() is only supported in FROM or in a select list without FROM", b.Name)
	}
	if b.Alt != nil && b.ArityOK(len(args)) {
		if d, ok, err := b.Alt(args); ok {
			return d, err
		}
	}
	coerced, null, err := b.coerceArgs(args)
	if err != nil || null {
		return types.DNull, err
	}
	if b.ZFn != nil && loc != nil && loc != time.UTC {
		return b.ZFn(coerced, loc)
	}
	return b.Fn(coerced)
}

// RowIter yields a set-returning function's rows one at a time (ok
// false after the last), so the caller can account for each before
// the next is made.
type RowIter func() (row []types.Datum, ok bool, err error)

// rowsOf iterates over rows already made.
func rowsOf(rows [][]types.Datum) RowIter {
	return func() ([]types.Datum, bool, error) {
		if len(rows) == 0 {
			return nil, false, nil
		}
		r := rows[0]
		rows = rows[1:]
		return r, true, nil
	}
}

// CallRows evaluates a set-returning builtin: no rows when a strict
// argument is NULL, else an iterator over the rows of its columns.
func (b *Builtin) CallRows(args []types.Datum) (RowIter, error) { return b.CallRowsIn(args, nil) }

// CallRowsIn is CallRows under the session's TimeZone.
func (b *Builtin) CallRowsIn(args []types.Datum, loc *time.Location) (RowIter, error) {
	if b.Rows == nil {
		return nil, errf(CodeNotSupported, "function %s() does not return a set", b.Name)
	}
	coerced, null, err := b.coerceArgs(args)
	if err != nil {
		return nil, err
	}
	if null {
		return rowsOf(nil), nil
	}
	if b.ZRows != nil && loc != nil && loc != time.UTC {
		return b.ZRows(coerced, loc)
	}
	return b.Rows(coerced)
}

// IsSetReturning reports whether the builtin yields rows (Rows) rather
// than a value.
func (b *Builtin) IsSetReturning() bool { return b.Rows != nil }

// ColumnFamilies are a set-returning builtin's column families given
// the argument families: the result family first, then ColFams.
func (b *Builtin) ColumnFamilies(args []types.Family) []types.Family {
	return append([]types.Family{b.ResultFamily(args)}, b.ColFams...)
}

// coerceArgs checks the arity and coerces the arguments to the declared
// families; null reports a NULL argument to a strict function.
func (b *Builtin) coerceArgs(args []types.Datum) (coerced []types.Datum, null bool, err error) {
	if !b.ArityOK(len(args)) {
		return nil, false, errf(CodeUndefined, "%s() takes %s argument(s), got %d", b.Name, b.ArityText(), len(args))
	}
	coerced = make([]types.Datum, len(args))
	for i, a := range args {
		if a.Null {
			if !b.NotStrict {
				return nil, true, nil
			}
			coerced[i] = a
			continue
		}
		want := b.argFamily(i)
		if want == Any || a.Fam == want {
			coerced[i] = a
			continue
		}
		if want == types.String && a.Fam == types.Bytes {
			if b.Bytea {
				coerced[i] = a
				continue
			}
			// bytea has no implicit cast to text: a text function
			// without a bytea form does not take one.
			return nil, false, errf(CodeUndefined, "function %s(%s) does not exist", b.Name, familiesText(args))
		}
		if want == types.String {
			// Anything renders as text (lenient, like the || operator).
			coerced[i] = types.NewString(a.Text())
			continue
		}
		c, err := a.Coerce(want)
		if err != nil {
			return nil, false, errf(CodeUndefined, "function %s(%s) does not exist: argument %d is %s, want %s", b.Name, familiesText(args), i+1, a.Fam, want)
		}
		coerced[i] = c
	}
	return coerced, false, nil
}

func familiesText(args []types.Datum) string {
	names := make([]string, len(args))
	for i, a := range args {
		if a.Null {
			names[i] = "unknown"
		} else if a.Fam == types.Bytes {
			names[i] = "bytea"
		} else {
			names[i] = strings.ToLower(a.Fam.String())
		}
	}
	return strings.Join(names, ", ")
}

// Signature renders the reference's "name(args) → type" line.
func (b *Builtin) Signature() string {
	parts := make([]string, 0, len(b.Args))
	for i, f := range b.Args {
		name := "any"
		if f != Any {
			name = strings.ToLower(f.String())
		}
		if i >= b.MinArgs {
			name = "[" + name + "]"
		}
		parts = append(parts, name)
	}
	if b.Variadic {
		parts = append(parts, "...")
	}
	ret := "any"
	switch {
	case b.RetOf != nil && b.Rows != nil:
		ret = "the type of its bounds"
	case b.Ret != Any:
		ret = strings.ToLower(b.Ret.String())
	case b.SameAsArg < len(b.Args):
		ret = "the type of argument " + fmt.Sprint(b.SameAsArg+1)
	}
	if b.Rows != nil {
		if len(b.Columns) > 1 {
			cols := make([]string, len(b.Columns))
			for i, c := range b.Columns {
				f := b.Ret
				if i > 0 {
					f = b.ColFams[i-1]
				}
				cols[i] = c + " " + strings.ToLower(f.String())
			}
			ret = "(" + strings.Join(cols, ", ") + ")"
		}
		ret = "setof " + ret
	}
	return fmt.Sprintf("%s(%s) → %s", b.Name, strings.Join(parts, ", "), ret)
}
