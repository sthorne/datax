package parser

import (
	"strings"
	"testing"
)

// grouped renders an expression with every binary operation in
// parentheses, to show how it bound.
func grouped(e Expr) string {
	if e.BinOp == "" {
		if e.Func != "" {
			args := make([]string, len(e.Args))
			for i, a := range e.Args {
				args[i] = grouped(a)
			}
			return e.Func + "(" + strings.Join(args, ", ") + ")"
		}
		return FormatExpr(e)
	}
	left := e
	left.BinOp, left.Right = "", nil
	if e.Left != nil {
		left = *e.Left
	}
	return "(" + grouped(left) + " " + e.BinOp + " " + grouped(*e.Right) + ")"
}

// TestBitwiseAndPrefixOperators (PG-F-011, PG-F-005): the bitwise,
// prefix and jsonb operators lex and bind as in PostgreSQL — the
// binary ones looser than + and -, left to right; a prefix operator
// over the operand up to + and -.
func TestBitwiseAndPrefixOperators(t *testing.T) {
	for src, want := range map[string]string{
		`1 + 2 & 3`:                           `((1 + 2) & 3)`,
		`1 & 2 | 3 # 4`:                       `(((1 & 2) | 3) # 4)`,
		`a << 2 * b`:                          `(a << (2 * b))`,
		`a >> 1`:                              `(a >> 1)`,
		`~a & 3`:                              `(int8not(a) & 3)`,
		`~ a + 1`:                             `int8not((a + 1))`,
		`@ -5`:                                `abs(-5)`,
		`|/ 25`:                               `sqrt(25)`,
		`||/ 27`:                              `cbrt(27)`,
		`'a'||'b'`:                            `('a' || 'b')`,
		`j #- '{a}'`:                          `(j #- '{a}')`,
		`j - 'a' #- '{b}'`:                    `((j - 'a') #- '{b}')`,
		`j['a'][0]`:                           `array_subscript(array_subscript(j, 'a'), 0)`,
		`normalize(s, nfkd)`:                  `normalize(s, 'NFKD')`,
		`overlay(s placing 'x' from 2 for 3)`: `overlay(s, 'x', 2, 3)`,
		`substring(s similar 'a#"b#"' escape '#')`: `substring(s, 'a#"b#"', '#')`,
	} {
		sel, err := Parse("SELECT " + src + " FROM t")
		if err != nil {
			t.Errorf("%s: %v", src, err)
			continue
		}
		if got := grouped(sel[0].(*Select).Exprs[0].Expr); got != want {
			t.Errorf("%s parses as %s, want %s", src, got, want)
		}
	}
	for _, src := range []string{`SELECT 5!`, `SELECT normalize(s, nfx) FROM t`} {
		if _, err := Parse(src); err == nil {
			t.Errorf("%s: accepted", src)
		}
	}
	// IS [NOT] [form] NORMALIZED.
	sel := parseOne(t, `SELECT id FROM t WHERE s IS NOT NFD NORMALIZED`).(*Select)
	if w := sel.Where; len(w) != 1 || w[0].Expr == nil || w[0].Expr.Func != "is_normalized" || w[0].Expr.Args[1].Lit.S != "NFD" || w[0].Value.Lit.B {
		t.Fatalf("IS NOT NFD NORMALIZED: %+v", sel.Where)
	}
	// UPDATE ... SET j['a'] = v, twice on one column.
	up := parseOne(t, `UPDATE t SET j['a'] = '1', n = 2, j['b'][0] = '2' WHERE id = 1`).(*Update)
	if len(up.Set) != 2 || up.Set[0].Column != "j" || FormatExpr(up.Set[0].Value) != `subscript_assign(subscript_assign(j, 'j', '1', 'a'), 'j', '2', 'b', 0)` {
		t.Fatalf("subscripted SET: %+v", up.Set)
	}
}
