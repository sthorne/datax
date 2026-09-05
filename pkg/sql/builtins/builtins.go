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
	// Category and Doc feed the reference: a section and one line.
	Category string
	Doc      string
	// Hidden keeps aliases (character_length, ceiling, ...) out of the
	// reference, which lists them beside their canonical name.
	Hidden bool
	// Aliases are other names for the same function.
	Aliases []string
	// Session marks a function the session evaluates itself (no Fn):
	// listed and arity-checked here, called elsewhere.
	Session bool
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
func (b *Builtin) Call(args []types.Datum) (types.Datum, error) {
	if !b.ArityOK(len(args)) {
		return types.Datum{}, errf(CodeUndefined, "%s() takes %s argument(s), got %d", b.Name, b.ArityText(), len(args))
	}
	coerced := make([]types.Datum, len(args))
	for i, a := range args {
		if a.Null {
			if !b.NotStrict {
				return types.DNull, nil
			}
			coerced[i] = a
			continue
		}
		want := b.argFamily(i)
		if want == Any || a.Fam == want {
			coerced[i] = a
			continue
		}
		if want == types.String {
			// Anything renders as text (lenient, like the || operator).
			coerced[i] = types.NewString(a.Text())
			continue
		}
		c, err := a.Coerce(want)
		if err != nil {
			return types.Datum{}, errf(CodeUndefined, "function %s(%s) does not exist: argument %d is %s, want %s", b.Name, familiesText(args), i+1, a.Fam, want)
		}
		coerced[i] = c
	}
	return b.Fn(coerced)
}

func familiesText(args []types.Datum) string {
	names := make([]string, len(args))
	for i, a := range args {
		if a.Null {
			names[i] = "unknown"
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
	if b.Ret != Any {
		ret = strings.ToLower(b.Ret.String())
	} else if b.SameAsArg < len(b.Args) {
		ret = "the type of argument " + fmt.Sprint(b.SameAsArg+1)
	}
	return fmt.Sprintf("%s(%s) → %s", b.Name, strings.Join(parts, ", "), ret)
}
