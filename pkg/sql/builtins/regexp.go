package builtins

import (
	"errors"
	"regexp"
	"regexp/syntax"
	"strconv"
	"strings"
	"sync"
	"unicode/utf8"

	"github.com/sthorne/datax/pkg/sql/types"
)

// The POSIX regular-expression functions. PostgreSQL's patterns are
// Advanced Regular Expressions; datax runs them on Go's RE2 engine,
// which takes the common syntax (classes, [[:alpha:]], quantifiers,
// groups, alternation, anchors) as it is. The word-boundary escapes
// \m \M \y \Y and \Z are translated; what RE2 cannot do — back
// references, lookahead and lookbehind — is refused as unsupported
// (0A000): PostgreSQL accepts those patterns, so they are not invalid
// (2201B is kept for a pattern PostgreSQL refuses too).

// reFlags are a call's parsed option letters.
type reFlags struct {
	global bool
	re     *regexp.Regexp
}

// regexCompile compiles a pattern under its flags argument: g (every match), i / c (case
// insensitive / sensitive), n or m (newline-sensitive), s (the
// default: . matches a newline, ^ and $ only at the ends), p and w
// (partial newline sensitivity), x (expanded syntax), q (the pattern
// is a literal string), t (tight syntax, the default).
func regexCompile(pattern, flags string) (reFlags, error) {
	var out reFlags
	fold, quote, expanded := false, false, false
	mode := "s"
	for _, c := range flags {
		switch c {
		case 'g':
			out.global = true
		case 'i':
			fold = true
		case 'c':
			fold = false
		case 'n', 'm':
			mode = "m"
		case 's':
			mode = "s"
		case 'p':
			mode = ""
		case 'w':
			mode = "sm"
		case 'x':
			expanded = true
		case 't':
			expanded = false
		case 'q':
			quote = true
		default:
			return out, errf(CodeInvalidArgument, "invalid regular expression option: %q", string(c))
		}
	}
	key := mode + "/" + strconv.FormatBool(fold) + strconv.FormatBool(quote) + strconv.FormatBool(expanded) + "/" + pattern
	if re, ok := regexpCache.get(key); ok {
		out.re = re
		return out, nil
	}
	body := pattern
	switch {
	case quote:
		body = regexp.QuoteMeta(pattern)
	default:
		if expanded {
			body = stripExpanded(body)
		}
		if err := refuseLookaround(body); err != nil {
			return out, err
		}
		var err error
		if body, err = translateARE(body); err != nil {
			return out, err
		}
	}
	prefix := mode
	if fold {
		prefix += "i"
	}
	if prefix != "" {
		body = "(?" + prefix + ")" + body
	}
	re, err := regexp.Compile(body)
	if err != nil {
		msg := err.Error()
		var se *syntax.Error
		if errors.As(err, &se) {
			msg = string(se.Code)
		}
		return out, errf(CodeInvalidRegexp, "invalid regular expression: %s", msg)
	}
	regexpCache.put(key, re)
	out.re = re
	return out, nil
}

// translateARE rewrites the PostgreSQL escapes RE2 spells differently
// and refuses the ones it lacks.
func translateARE(p string) (string, error) {
	if !strings.ContainsRune(p, '\\') {
		return p, nil
	}
	var b strings.Builder
	inClass := false
	for i := 0; i < len(p); i++ {
		c := p[i]
		switch {
		case c == '[' && !inClass:
			inClass = true
		case c == ']' && inClass:
			inClass = false
		}
		if c != '\\' || i+1 >= len(p) {
			b.WriteByte(c)
			continue
		}
		n := p[i+1]
		i++
		if inClass {
			b.WriteByte('\\')
			b.WriteByte(n)
			continue
		}
		switch {
		case n == 'm' || n == 'M' || n == 'y':
			b.WriteString(`\b`)
		case n == 'Y':
			b.WriteString(`\B`)
		case n == 'Z':
			b.WriteString(`\z`)
		case n >= '1' && n <= '9':
			return "", errf(CodeNotSupported, "regular expression back references (\\%c) are not supported", n)
		default:
			b.WriteByte('\\')
			b.WriteByte(n)
		}
	}
	return b.String(), nil
}

// refuseLookaround refuses the lookahead and lookbehind constraints
// (?= (?! (?<= (?<!, outside bracket expressions: RE2 has none.
func refuseLookaround(p string) error {
	inClass := false
	for i := 0; i < len(p); i++ {
		c := p[i]
		switch {
		case c == '\\':
			i++
		case inClass:
			if c == ']' {
				inClass = false
			}
		case c == '[':
			inClass = true
			if i+1 < len(p) && p[i+1] == '^' {
				i++
			}
			if i+1 < len(p) && p[i+1] == ']' {
				i++ // a leading ] is a member
			}
		case c == '(' && strings.HasPrefix(p[i:], "(?"):
			rest := p[i+2:]
			kind := ""
			switch {
			case strings.HasPrefix(rest, "="):
				kind = "lookahead"
			case strings.HasPrefix(rest, "!"):
				kind = "negative lookahead"
			case strings.HasPrefix(rest, "<="):
				kind = "lookbehind"
			case strings.HasPrefix(rest, "<!"):
				kind = "negative lookbehind"
			}
			if kind != "" {
				return errf(CodeNotSupported, "regular expression %s constraints are not supported", kind)
			}
		}
	}
	return nil
}

// RegexpMatch is the ~ and ~* operators: whether the POSIX pattern
// matches anywhere in s, compiled as the regexp_* functions compile it.
func RegexpMatch(s, pattern string, fold bool) (bool, error) {
	flags := ""
	if fold {
		flags = "i"
	}
	f, err := regexCompile(pattern, flags)
	if err != nil {
		return false, err
	}
	return f.re.MatchString(s), nil
}

// charPos is the 1-based character position of the byte offset off.
func charPos(s string, off int) int64 {
	return int64(utf8.RuneCountInString(s[:off])) + 1
}

// regexSubstring is substring(s FROM pattern): the part of s the first
// match of the POSIX pattern covers, or of its first parenthesized
// subexpression when it has one; NULL when nothing matches.
func regexSubstring(s, pattern string) (types.Datum, error) {
	f, err := regexCompile(pattern, "")
	if err != nil {
		return types.Datum{}, err
	}
	return firstGroup(f.re, s), nil
}

func firstGroup(re *regexp.Regexp, s string) types.Datum {
	m := re.FindStringSubmatchIndex(s)
	switch {
	case m == nil:
		return types.DNull
	case len(m) > 2:
		if m[2] < 0 {
			return types.DNull
		}
		return str(s[m[2]:m[3]])
	}
	return str(s[m[0]:m[1]])
}

// similarSubstring is substring(s SIMILAR pattern ESCAPE e) (and
// substring(s FROM pattern FOR e)): the pattern, a SIMILAR TO pattern,
// must match the whole string, and the result is the part between the
// two escape-double-quote separators (all of it without separators);
// NULL when it does not match.
func similarSubstring(s, pattern, escape string) (types.Datum, error) {
	esc := []rune(escape)
	if len(esc) > 1 {
		return types.Datum{}, errf(CodeInvalidEscape, "invalid escape string")
	}
	body, err := similarRegexp(pattern, esc)
	if err != nil {
		return types.Datum{}, err
	}
	re, err := regexp.Compile("(?s)" + body)
	if err != nil {
		return types.Datum{}, errf(CodeInvalidRegexp, "invalid regular expression: %v", err)
	}
	return firstGroup(re, s), nil
}

// similarRegexp translates a SIMILAR TO pattern into RE2 as
// PostgreSQL's similar_escape does: anchored, % and _ as .* and ., the
// SQL metacharacters | * + ? {m,n} [...] kept, parentheses
// non-capturing, the regular-expression ones (. ^ $ \) literal, and the
// escape-double-quote separators splitting it into a lazily matched
// head, the captured middle and a tail.
func similarRegexp(pattern string, esc []rune) (string, error) {
	var b strings.Builder
	b.WriteString("^(?:")
	quotes := 0
	escaped, inClass := false, false
	for _, r := range pattern {
		switch {
		case escaped:
			escaped = false
			if r == '"' {
				switch quotes {
				case 0:
					b.WriteString("){1,1}?(")
				case 1:
					b.WriteString("){1,1}(?:")
				default:
					return "", errf(CodeInvalidEscapeUse, "SQL regular expression may not contain more than two escape-double-quote separators")
				}
				quotes++
				continue
			}
			b.WriteString(regexp.QuoteMeta(string(r)))
		case len(esc) == 1 && r == esc[0]:
			escaped = true
		case inClass:
			if r == ']' {
				inClass = false
			}
			if r == '\\' {
				b.WriteString(`\\`)
				continue
			}
			b.WriteRune(r)
		case r == '[':
			inClass = true
			b.WriteRune(r)
		case r == '%':
			b.WriteString(".*")
		case r == '_':
			b.WriteString(".")
		case r == '(':
			b.WriteString("(?:")
		case strings.ContainsRune("|*+?{})", r):
			b.WriteRune(r)
		default:
			b.WriteString(regexp.QuoteMeta(string(r)))
		}
	}
	if escaped {
		return "", errf(CodeInvalidEscape, "invalid escape string")
	}
	b.WriteString(")$")
	return b.String(), nil
}

// substringPattern is substring's pattern forms, taken when its second
// argument is text: substring(s FROM pattern) and substring(s, pattern)
// (POSIX), substring(s SIMILAR pattern ESCAPE e) and
// substring(s FROM pattern FOR e) (SIMILAR TO).
func substringPattern(a []types.Datum) (types.Datum, bool, error) {
	if len(a) < 2 || a[1].Null || a[1].Fam != types.String || a[0].Fam == types.Bytes {
		return types.Datum{}, false, nil
	}
	for _, d := range a {
		if d.Null {
			return types.DNull, true, nil
		}
	}
	if len(a) == 2 {
		d, err := regexSubstring(a[0].Text(), a[1].S)
		return d, true, err
	}
	if a[2].Fam != types.String {
		return types.Datum{}, true, errf(CodeUndefined, "function substring(%s) does not exist", familiesText(a))
	}
	d, err := similarSubstring(a[0].Text(), a[1].S, a[2].S)
	return d, true, err
}

// stripExpanded drops the white space and #-comments of an expanded
// (x) pattern, outside bracket expressions and escapes.
func stripExpanded(p string) string {
	var b strings.Builder
	inClass := false
	for i := 0; i < len(p); i++ {
		c := p[i]
		switch {
		case c == '\\' && i+1 < len(p):
			b.WriteByte(c)
			b.WriteByte(p[i+1])
			i++
			continue
		case inClass:
			if c == ']' {
				inClass = false
			}
		case c == '[':
			inClass = true
		case c == ' ' || c == '\t' || c == '\n' || c == '\r':
			continue
		case c == '#':
			for i < len(p) && p[i] != '\n' {
				i++
			}
			continue
		}
		b.WriteByte(c)
	}
	return b.String()
}

// regexpCache holds compiled patterns, bounded (cleared when full).
var regexpCache = &reCache{m: map[string]*regexp.Regexp{}}

type reCache struct {
	mu sync.Mutex
	m  map[string]*regexp.Regexp
}

func (c *reCache) get(k string) (*regexp.Regexp, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	re, ok := c.m[k]
	return re, ok
}

func (c *reCache) put(k string, re *regexp.Regexp) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if len(c.m) >= 512 {
		c.m = map[string]*regexp.Regexp{}
	}
	c.m[k] = re
}

// flagsArg is the optional flags argument at position i ("" when
// absent).
func flagsArg(a []types.Datum, i int) string {
	if i < len(a) {
		return a[i].Text()
	}
	return ""
}

// noGlobal refuses the g flag for a function that matches once.
func noGlobal(fn string, f reFlags) error {
	if f.global {
		return errf(CodeInvalidArgument, "%s() does not support the \"global\" option", fn)
	}
	return nil
}

// byteOffset is the byte offset of the 1-based character position pos
// in s (len(s) past the end).
func byteOffset(s string, pos int64) int {
	if pos <= 1 {
		return 0
	}
	n := int64(1)
	for i := range s {
		if n == pos {
			return i
		}
		n++
	}
	return len(s)
}

// positiveArg reads an integer parameter that must be at least min.
func positiveArg(name string, d types.Datum, min int64) (int64, error) {
	v, err := toInt(d)
	if err != nil {
		return 0, err
	}
	if v.I < min {
		return 0, errf(CodeInvalidArgument, "invalid value for parameter \"%s\": %d", name, v.I)
	}
	return v.I, nil
}

// matchesFrom are the non-overlapping matches (submatch indexes) at or
// after byte offset from.
func matchesFrom(re *regexp.Regexp, s string, from int) [][]int {
	all := re.FindAllStringSubmatchIndex(s, -1)
	if from == 0 {
		return all
	}
	out := all[:0]
	for _, m := range all {
		if m[0] >= from {
			out = append(out, m)
		}
	}
	return out
}

// captures renders a match as regexp_match does: the parenthesized
// subexpressions (NULL for one that did not take part), or the whole
// match when there are none.
func captures(s string, m []int) types.Datum {
	if len(m) == 2 {
		return types.NewArray(types.String, []types.Datum{types.NewString(s[m[0]:m[1]])})
	}
	elems := make([]types.Datum, 0, len(m)/2-1)
	for g := 2; g+1 < len(m); g += 2 {
		if m[g] < 0 {
			elems = append(elems, types.DNull)
			continue
		}
		elems = append(elems, types.NewString(s[m[g]:m[g+1]]))
	}
	return types.NewArray(types.String, elems)
}

// expandReplacement renders a replacement for one match: \1 to \9 are
// the subexpressions, \& the whole match, \\ a backslash; any other
// backslash stays as written.
func expandReplacement(b *strings.Builder, repl, s string, m []int) {
	for i := 0; i < len(repl); i++ {
		c := repl[i]
		if c != '\\' || i+1 >= len(repl) {
			b.WriteByte(c)
			continue
		}
		n := repl[i+1]
		switch {
		case n >= '1' && n <= '9':
			g := int(n-'0') * 2
			if g+1 < len(m) && m[g] >= 0 {
				b.WriteString(s[m[g]:m[g+1]])
			}
			i++
		case n == '&':
			b.WriteString(s[m[0]:m[1]])
			i++
		case n == '\\':
			b.WriteByte('\\')
			i++
		default:
			b.WriteByte('\\')
		}
	}
}

// regexpSplit splits s at the matches of re, ignoring the zero-length
// matches at the start or end of the string or right after a match, as
// PostgreSQL's regexp_split functions do.
func regexpSplit(re *regexp.Regexp, s string) []types.Datum {
	var out []types.Datum
	last := 0
	for _, m := range re.FindAllStringIndex(s, -1) {
		if m[0] == m[1] && (m[0] == 0 || m[0] == len(s) || m[0] == last) {
			continue
		}
		out = append(out, types.NewString(s[last:m[0]]))
		last = m[1]
	}
	return append(out, types.NewString(s[last:]))
}

func init() {
	register(&Builtin{Name: "regexp_replace", Args: []types.Family{types.String, types.String, types.String, Any, types.Int, types.String}, MinArgs: 3, Ret: types.String, Category: catString,
		Doc: "Replaces the first match of the POSIX pattern (every match with the g flag) with the replacement, in which \\1 to \\9 name the subexpressions and \\& the whole match; regexp_replace(s, pattern, replacement, start [, n [, flags]]) begins at the character start and replaces the n-th match (every one for 0).",
		Fn: func(a []types.Datum) (types.Datum, error) {
			src, repl := a[0].S, a[2].S
			flags, start, nth := "", int64(1), int64(-1)
			if len(a) > 3 {
				switch a[3].Fam {
				case types.Int, types.Decimal, types.Float:
					var err error
					if start, err = positiveArg("start", a[3], 1); err != nil {
						return types.Datum{}, err
					}
					if len(a) > 4 {
						if nth, err = positiveArg("n", a[4], 0); err != nil {
							return types.Datum{}, err
						}
					}
					flags = flagsArg(a, 5)
				default:
					if len(a) > 4 {
						return types.Datum{}, errf(CodeUndefined, "function regexp_replace(%s) does not exist", familiesText(a))
					}
					flags = a[3].Text()
				}
			}
			f, err := regexCompile(a[1].S, flags)
			if err != nil {
				return types.Datum{}, err
			}
			ms := matchesFrom(f.re, src, byteOffset(src, start))
			switch {
			case nth > 0:
				if int64(len(ms)) < nth {
					return str(src), nil
				}
				ms = ms[nth-1 : nth]
			case nth == 0 || f.global:
			case len(ms) > 1:
				ms = ms[:1]
			}
			if len(ms) == 0 {
				return str(src), nil
			}
			var b strings.Builder
			last := 0
			for _, m := range ms {
				b.WriteString(src[last:m[0]])
				expandReplacement(&b, repl, src, m)
				last = m[1]
			}
			b.WriteString(src[last:])
			return str(b.String()), nil
		}})
	register(&Builtin{Name: "regexp_match", Args: []types.Family{types.String, types.String, types.String}, MinArgs: 2, Ret: types.ArrayOf(types.String), Category: catString,
		Doc: "The first match of the POSIX pattern as a text array: its parenthesized subexpressions, or the whole match when there are none (NULL when nothing matches).",
		Fn: func(a []types.Datum) (types.Datum, error) {
			f, err := regexCompile(a[1].S, flagsArg(a, 2))
			if err != nil {
				return types.Datum{}, err
			}
			if err := noGlobal("regexp_match", f); err != nil {
				return types.Datum{}, err
			}
			m := f.re.FindStringSubmatchIndex(a[0].S)
			if m == nil {
				return types.DNull, nil
			}
			return captures(a[0].S, m), nil
		}})
	register(&Builtin{Name: "regexp_matches", Args: []types.Family{types.String, types.String, types.String}, MinArgs: 2, Ret: types.ArrayOf(types.String), Category: catString,
		Columns: []string{""},
		Doc:     "One row per match of the POSIX pattern (the first only, unless the g flag is given), each a text array as regexp_match returns.",
		Rows: func(a []types.Datum) (RowIter, error) {
			f, err := regexCompile(a[1].S, flagsArg(a, 2))
			if err != nil {
				return nil, err
			}
			n := 1
			if f.global {
				n = -1
			}
			var rows [][]types.Datum
			for _, m := range f.re.FindAllStringSubmatchIndex(a[0].S, n) {
				rows = append(rows, []types.Datum{captures(a[0].S, m)})
			}
			return rowsOf(rows), nil
		}})
	register(&Builtin{Name: "regexp_split_to_array", Args: []types.Family{types.String, types.String, types.String}, MinArgs: 2, Ret: types.ArrayOf(types.String), Category: catString,
		Doc: "The string split at the matches of the POSIX pattern, as a text array.",
		Fn: func(a []types.Datum) (types.Datum, error) {
			f, err := regexCompile(a[1].S, flagsArg(a, 2))
			if err != nil {
				return types.Datum{}, err
			}
			if err := noGlobal("regexp_split_to_array", f); err != nil {
				return types.Datum{}, err
			}
			return types.NewArray(types.String, regexpSplit(f.re, a[0].S)), nil
		}})
	register(&Builtin{Name: "regexp_split_to_table", Args: []types.Family{types.String, types.String, types.String}, MinArgs: 2, Ret: types.String, Category: catString,
		Columns: []string{""},
		Doc:     "The string split at the matches of the POSIX pattern, one row per piece.",
		Rows: func(a []types.Datum) (RowIter, error) {
			f, err := regexCompile(a[1].S, flagsArg(a, 2))
			if err != nil {
				return nil, err
			}
			if err := noGlobal("regexp_split_to_table", f); err != nil {
				return nil, err
			}
			parts := regexpSplit(f.re, a[0].S)
			rows := make([][]types.Datum, len(parts))
			for i, p := range parts {
				rows[i] = []types.Datum{p}
			}
			return rowsOf(rows), nil
		}})
	register(&Builtin{Name: "regexp_count", Args: []types.Family{types.String, types.String, types.Int, types.String}, MinArgs: 2, Ret: types.Int, Category: catString,
		Doc: "The number of non-overlapping matches of the POSIX pattern, from the 1-based character start.",
		Fn: func(a []types.Datum) (types.Datum, error) {
			start := int64(1)
			if len(a) > 2 {
				var err error
				if start, err = positiveArg("start", a[2], 1); err != nil {
					return types.Datum{}, err
				}
			}
			f, err := regexCompile(a[1].S, flagsArg(a, 3))
			if err != nil {
				return types.Datum{}, err
			}
			if err := noGlobal("regexp_count", f); err != nil {
				return types.Datum{}, err
			}
			return i64(int64(len(matchesFrom(f.re, a[0].S, byteOffset(a[0].S, start))))), nil
		}})
	register(&Builtin{Name: "regexp_instr", Args: []types.Family{types.String, types.String, types.Int, types.Int, types.Int, types.String, types.Int}, MinArgs: 2, Ret: types.Int, Category: catString,
		Doc: "The 1-based character position of the n-th match (the first by default) of the POSIX pattern from the character start, or of its subexpr-th subexpression: where it begins, or just after it ends with endoption 1; 0 when there is none.",
		Fn: func(a []types.Datum) (types.Datum, error) {
			start, nth, endopt, sub := int64(1), int64(1), int64(0), int64(0)
			var err error
			if len(a) > 2 {
				if start, err = positiveArg("start", a[2], 1); err != nil {
					return types.Datum{}, err
				}
			}
			if len(a) > 3 {
				if nth, err = positiveArg("n", a[3], 1); err != nil {
					return types.Datum{}, err
				}
			}
			if len(a) > 4 {
				if endopt = a[4].I; endopt != 0 && endopt != 1 {
					return types.Datum{}, errf(CodeInvalidArgument, "invalid value for parameter \"endoption\": %d", endopt)
				}
			}
			if len(a) > 6 {
				if sub, err = positiveArg("subexpr", a[6], 0); err != nil {
					return types.Datum{}, err
				}
			}
			f, err := regexCompile(a[1].S, flagsArg(a, 5))
			if err != nil {
				return types.Datum{}, err
			}
			if err := noGlobal("regexp_instr", f); err != nil {
				return types.Datum{}, err
			}
			s := a[0].S
			ms := matchesFrom(f.re, s, byteOffset(s, start))
			if int64(len(ms)) < nth {
				return i64(0), nil
			}
			m := ms[nth-1]
			g := int(sub) * 2
			if g+1 >= len(m) || m[g] < 0 {
				return i64(0), nil
			}
			return i64(charPos(s, m[g+int(endopt)])), nil
		}})
	register(&Builtin{Name: "regexp_like", Args: []types.Family{types.String, types.String, types.String}, MinArgs: 2, Ret: types.Bool, Category: catString,
		Doc: "Whether the POSIX pattern matches anywhere in the string (the ~ operator, with flags).",
		Fn: func(a []types.Datum) (types.Datum, error) {
			f, err := regexCompile(a[1].S, flagsArg(a, 2))
			if err != nil {
				return types.Datum{}, err
			}
			if err := noGlobal("regexp_like", f); err != nil {
				return types.Datum{}, err
			}
			return types.NewBool(f.re.MatchString(a[0].S)), nil
		}})
	register(&Builtin{Name: "regexp_substr", Args: []types.Family{types.String, types.String, types.Int, types.Int, types.String, types.Int}, MinArgs: 2, Ret: types.String, Category: catString,
		Doc: "The text of the n-th match (the first by default) of the POSIX pattern from the character start, or of its subexpr-th subexpression; NULL when there is none.",
		Fn: func(a []types.Datum) (types.Datum, error) {
			start, nth, sub := int64(1), int64(1), int64(0)
			var err error
			if len(a) > 2 {
				if start, err = positiveArg("start", a[2], 1); err != nil {
					return types.Datum{}, err
				}
			}
			if len(a) > 3 {
				if nth, err = positiveArg("n", a[3], 1); err != nil {
					return types.Datum{}, err
				}
			}
			if len(a) > 5 {
				if sub, err = positiveArg("subexpr", a[5], 0); err != nil {
					return types.Datum{}, err
				}
			}
			f, err := regexCompile(a[1].S, flagsArg(a, 4))
			if err != nil {
				return types.Datum{}, err
			}
			if err := noGlobal("regexp_substr", f); err != nil {
				return types.Datum{}, err
			}
			ms := matchesFrom(f.re, a[0].S, byteOffset(a[0].S, start))
			if int64(len(ms)) < nth {
				return types.DNull, nil
			}
			m := ms[nth-1]
			g := int(sub) * 2
			if g+1 >= len(m) || m[g] < 0 {
				return types.DNull, nil
			}
			return str(a[0].S[m[g]:m[g+1]]), nil
		}})
}
