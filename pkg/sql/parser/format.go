package parser

import (
	"strings"

	"github.com/sthorne/datax/pkg/sql/types"
)

// FormatExpr renders a value expression back to SQL — the subset a
// DEFAULT may hold (literals, function calls, arithmetic, casts) — for
// storing an expression default and for the catalogs to show it.
func FormatExpr(e Expr) string {
	var b strings.Builder
	formatExpr(&b, e)
	return b.String()
}

func formatExpr(b *strings.Builder, e Expr) {
	if e.Left != nil {
		b.WriteString("(")
		formatExpr(b, *e.Left)
		if e.BinOp != "" {
			b.WriteString(" " + e.BinOp + " ")
			formatExpr(b, *e.Right)
		}
		b.WriteString(")")
		if e.Cast != "" {
			b.WriteString("::" + e.Cast)
		}
		return
	}
	switch {
	case e.Lit != nil:
		b.WriteString(FormatLiteral(*e.Lit))
	case e.Func != "":
		b.WriteString(e.Func + "(")
		for i, a := range e.Args {
			if i > 0 {
				b.WriteString(", ")
			}
			formatExpr(b, a)
		}
		b.WriteString(")")
	case e.Column != "":
		b.WriteString(e.Column)
	case e.Param > 0:
		b.WriteString("$" + itoa(e.Param))
	case e.IsDefault:
		b.WriteString("DEFAULT")
	default:
		b.WriteString("NULL")
	}
	if e.BinOp != "" && e.Right != nil {
		// The flat historical shape: this node's own value is the left
		// operand.
		b.WriteString(" " + e.BinOp + " ")
		formatExpr(b, *e.Right)
	}
	if e.Cast != "" {
		b.WriteString("::" + e.Cast)
	}
}

// FormatLiteral renders a datum as a SQL literal.
func FormatLiteral(d types.Datum) string {
	if d.Null {
		return "NULL"
	}
	switch d.Fam {
	case types.Int, types.Float, types.Bool, types.Decimal:
		if d.Fam == types.Bool {
			if d.B {
				return "true"
			}
			return "false"
		}
		return d.Text()
	}
	return "'" + strings.ReplaceAll(d.Text(), "'", "''") + "'"
}

func itoa(n int) string {
	if n == 0 {
		return "0"
	}
	var buf []byte
	for n > 0 {
		buf = append([]byte{byte('0' + n%10)}, buf...)
		n /= 10
	}
	return string(buf)
}

// ParseExpr parses one value expression (an expression default read
// back from a descriptor).
func ParseExpr(text string) (Expr, error) {
	toks, err := lex(text)
	if err != nil {
		return Expr{}, err
	}
	p := &parser{toks: toks}
	e, err := p.parseValueOrColumnExpr()
	if err != nil {
		return Expr{}, err
	}
	if p.peek().kind != tkEOF {
		return Expr{}, p.errf("unexpected %q after expression", p.peek().text)
	}
	return e, nil
}
