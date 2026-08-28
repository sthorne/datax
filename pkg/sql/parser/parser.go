package parser

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/sthorne/datax/pkg/sql/types"
)

// SyntaxError carries a position for client-friendly messages.
type SyntaxError struct {
	Msg string
	Pos int
}

func (e *SyntaxError) Error() string { return fmt.Sprintf("syntax error: %s", e.Msg) }

// Parse parses a semicolon-separated statement list.
func Parse(src string) ([]Statement, error) {
	toks, err := lex(src)
	if err != nil {
		return nil, &SyntaxError{Msg: err.Error()}
	}
	p := &parser{toks: toks}
	var stmts []Statement
	for {
		for p.consumeOp(";") {
		}
		if p.peek().kind == tkEOF {
			return stmts, nil
		}
		s, err := p.parseStatement()
		if err != nil {
			return nil, err
		}
		stmts = append(stmts, s)
		if !p.consumeOp(";") && p.peek().kind != tkEOF {
			return nil, p.errf("unexpected %q after statement", p.peek().text)
		}
	}
}

type parser struct {
	toks []token
	i    int
}

func (p *parser) peek() token { return p.toks[p.i] }
func (p *parser) next() token { t := p.toks[p.i]; p.i++; return t }
func (p *parser) errf(format string, args ...any) error {
	return &SyntaxError{Msg: fmt.Sprintf(format, args...), Pos: p.peek().pos}
}

func (p *parser) consumeKeyword(kw string) bool {
	if t := p.peek(); t.kind == tkKeyword && t.text == kw {
		p.i++
		return true
	}
	return false
}

func (p *parser) expectKeyword(kw string) error {
	if !p.consumeKeyword(kw) {
		return p.errf("expected %s, found %q", kw, p.peek().text)
	}
	return nil
}

func (p *parser) consumeOp(op string) bool {
	if t := p.peek(); t.kind == tkOp && t.text == op {
		p.i++
		return true
	}
	return false
}

func (p *parser) expectOp(op string) error {
	if !p.consumeOp(op) {
		return p.errf("expected %q, found %q", op, p.peek().text)
	}
	return nil
}

func (p *parser) expectIdent() (string, error) {
	t := p.peek()
	if t.kind == tkIdent {
		p.i++
		return t.text, nil
	}
	// Allow non-reserved words used as identifiers where unambiguous.
	if t.kind == tkKeyword && (t.text == "KEY" || t.text == "TABLES") {
		p.i++
		return strings.ToLower(t.text), nil
	}
	return "", p.errf("expected identifier, found %q", t.text)
}

func (p *parser) parseStatement() (Statement, error) {
	t := p.peek()
	if t.kind != tkKeyword && t.kind != tkIdent {
		return nil, p.errf("unexpected %q", t.text)
	}
	switch t.text {
	case "CREATE":
		if nxt := p.toks[p.i+1]; nxt.kind == tkKeyword && (nxt.text == "UNIQUE" || nxt.text == "INDEX") {
			return p.parseCreateIndex()
		}
		return p.parseCreateTable()
	case "EXPLAIN":
		p.i++
		inner, err := p.parseStatement()
		if err != nil {
			return nil, err
		}
		return &Explain{Stmt: inner}, nil
	case "DROP":
		return p.parseDropTable()
	case "INSERT":
		return p.parseInsert()
	case "SELECT":
		return p.parseSelect()
	case "UPDATE":
		return p.parseUpdate()
	case "DELETE":
		return p.parseDelete()
	case "ALTER":
		return p.parseAlterTable()
	case "BEGIN":
		p.i++
		p.consumeKeyword("TRANSACTION")
		return &Begin{}, nil
	case "START":
		p.i++
		if err := p.expectKeyword("TRANSACTION"); err != nil {
			return nil, err
		}
		return &Begin{}, nil
	case "COMMIT", "END":
		p.i++
		p.consumeKeyword("TRANSACTION")
		return &Commit{}, nil
	case "ROLLBACK", "ABORT":
		p.i++
		p.consumeKeyword("TRANSACTION")
		return &Rollback{}, nil
	case "SHOW":
		p.i++
		if p.consumeKeyword("TABLES") {
			return &ShowTables{}, nil
		}
		// SHOW <anything else>: tolerated for client compatibility, treated
		// as an empty result via SetVar semantics.
		name, _ := p.expectIdent()
		return &SetVar{Name: "show:" + name}, nil
	case "SET":
		// SET [SESSION|LOCAL] name = value / SET ... TO ...: parse & ignore.
		p.i++
		p.consumeKeyword("SESSION")
		p.consumeKeyword("LOCAL")
		name, err := p.expectIdent()
		if err != nil {
			return nil, err
		}
		for !p.atStatementEnd() {
			p.i++
		}
		return &SetVar{Name: name}, nil
	}
	return nil, p.errf("unsupported statement %q", t.text)
}

func (p *parser) atStatementEnd() bool {
	t := p.peek()
	return t.kind == tkEOF || (t.kind == tkOp && t.text == ";")
}

func (p *parser) parseCreateTable() (Statement, error) {
	p.i++ // CREATE
	if err := p.expectKeyword("TABLE"); err != nil {
		return nil, err
	}
	ct := &CreateTable{}
	if p.consumeKeyword("IF") {
		if err := p.expectKeyword("NOT"); err != nil {
			return nil, err
		}
		if err := p.expectKeyword("EXISTS"); err != nil {
			return nil, err
		}
		ct.IfNotExists = true
	}
	name, err := p.expectIdent()
	if err != nil {
		return nil, err
	}
	ct.Name = name
	if err := p.expectOp("("); err != nil {
		return nil, err
	}
	for {
		if p.consumeKeyword("PRIMARY") {
			if err := p.expectKeyword("KEY"); err != nil {
				return nil, err
			}
			if err := p.expectOp("("); err != nil {
				return nil, err
			}
			for {
				col, err := p.expectIdent()
				if err != nil {
					return nil, err
				}
				ct.PrimaryKey = append(ct.PrimaryKey, col)
				if !p.consumeOp(",") {
					break
				}
			}
			if err := p.expectOp(")"); err != nil {
				return nil, err
			}
		} else {
			col, err := p.parseColumnDef()
			if err != nil {
				return nil, err
			}
			ct.Columns = append(ct.Columns, col)
		}
		if !p.consumeOp(",") {
			break
		}
	}
	if err := p.expectOp(")"); err != nil {
		return nil, err
	}
	return ct, nil
}

func (p *parser) parseColumnDef() (ColumnDef, error) {
	var def ColumnDef
	name, err := p.expectIdent()
	if err != nil {
		return def, err
	}
	def.Name = name
	t := p.peek()
	if t.kind != tkIdent && t.kind != tkKeyword {
		return def, p.errf("expected column type, found %q", t.text)
	}
	p.i++
	typeName := t.text
	// DOUBLE PRECISION / CHARACTER VARYING: absorb a trailing word.
	if strings.EqualFold(typeName, "double") || strings.EqualFold(typeName, "character") {
		if n := p.peek(); n.kind == tkIdent {
			p.i++
		}
	}
	// VARCHAR(n) etc.: absorb the length.
	if p.consumeOp("(") {
		if p.peek().kind == tkNumber {
			p.i++
		}
		if err := p.expectOp(")"); err != nil {
			return def, err
		}
	}
	fam, err := types.ParseType(typeName)
	if err != nil {
		return def, p.errf("%v", err)
	}
	def.Type = fam
	for {
		switch {
		case p.consumeKeyword("NOT"):
			if err := p.expectKeyword("NULL"); err != nil {
				return def, err
			}
			def.NotNull = true
		case p.consumeKeyword("NULL"):
		case p.consumeKeyword("PRIMARY"):
			if err := p.expectKeyword("KEY"); err != nil {
				return def, err
			}
			def.PrimaryKey = true
			def.NotNull = true
		default:
			return def, nil
		}
	}
}

func (p *parser) parseCreateIndex() (Statement, error) {
	p.i++ // CREATE
	ci := &CreateIndex{}
	if p.consumeKeyword("UNIQUE") {
		ci.Unique = true
	}
	if err := p.expectKeyword("INDEX"); err != nil {
		return nil, err
	}
	name, err := p.expectIdent()
	if err != nil {
		return nil, err
	}
	ci.Name = name
	if err := p.expectKeyword("ON"); err != nil {
		return nil, err
	}
	table, err := p.expectIdent()
	if err != nil {
		return nil, err
	}
	ci.Table = table
	if err := p.expectOp("("); err != nil {
		return nil, err
	}
	for {
		col, err := p.expectIdent()
		if err != nil {
			return nil, err
		}
		ci.Columns = append(ci.Columns, col)
		if !p.consumeOp(",") {
			break
		}
	}
	if err := p.expectOp(")"); err != nil {
		return nil, err
	}
	return ci, nil
}

func (p *parser) parseDropTable() (Statement, error) {
	p.i++ // DROP
	if err := p.expectKeyword("TABLE"); err != nil {
		return nil, err
	}
	dt := &DropTable{}
	if p.consumeKeyword("IF") {
		if err := p.expectKeyword("EXISTS"); err != nil {
			return nil, err
		}
		dt.IfExists = true
	}
	name, err := p.expectIdent()
	if err != nil {
		return nil, err
	}
	dt.Name = name
	return dt, nil
}

func (p *parser) parseInsert() (Statement, error) {
	p.i++ // INSERT
	if err := p.expectKeyword("INTO"); err != nil {
		return nil, err
	}
	ins := &Insert{}
	name, err := p.expectIdent()
	if err != nil {
		return nil, err
	}
	ins.Table = name
	if p.consumeOp("(") {
		for {
			col, err := p.expectIdent()
			if err != nil {
				return nil, err
			}
			ins.Columns = append(ins.Columns, col)
			if !p.consumeOp(",") {
				break
			}
		}
		if err := p.expectOp(")"); err != nil {
			return nil, err
		}
	}
	if err := p.expectKeyword("VALUES"); err != nil {
		return nil, err
	}
	for {
		if err := p.expectOp("("); err != nil {
			return nil, err
		}
		var row []Expr
		for {
			e, err := p.parseValueExpr()
			if err != nil {
				return nil, err
			}
			row = append(row, e)
			if !p.consumeOp(",") {
				break
			}
		}
		if err := p.expectOp(")"); err != nil {
			return nil, err
		}
		ins.Rows = append(ins.Rows, row)
		if !p.consumeOp(",") {
			break
		}
	}
	return ins, nil
}

func (p *parser) parseSelect() (Statement, error) {
	p.i++ // SELECT
	sel := &Select{Limit: -1}
	for {
		if p.consumeOp("*") {
			sel.Exprs = append(sel.Exprs, SelectExpr{Star: true})
		} else if se, ok, err := p.parseAggExpr(); err != nil {
			return nil, err
		} else if ok {
			if p.consumeKeyword("AS") {
				alias, err := p.expectIdent()
				if err != nil {
					return nil, err
				}
				se.Alias = alias
			}
			sel.Exprs = append(sel.Exprs, se)
		} else {
			e, err := p.parseValueOrColumnExpr()
			if err != nil {
				return nil, err
			}
			se := SelectExpr{Expr: e}
			if p.consumeKeyword("AS") {
				alias, err := p.expectIdent()
				if err != nil {
					return nil, err
				}
				se.Alias = alias
			}
			sel.Exprs = append(sel.Exprs, se)
		}
		if !p.consumeOp(",") {
			break
		}
	}
	if p.consumeKeyword("FROM") {
		name, err := p.expectIdent()
		if err != nil {
			return nil, err
		}
		sel.Table = name
	}
	var err error
	sel.Where, err = p.parseOptWhere()
	if err != nil {
		return nil, err
	}
	if p.consumeKeyword("ORDER") {
		if err := p.expectKeyword("BY"); err != nil {
			return nil, err
		}
		for {
			col, err := p.expectIdent()
			if err != nil {
				return nil, err
			}
			oc := OrderCol{Column: col}
			if p.consumeKeyword("DESC") {
				oc.Desc = true
			} else {
				p.consumeKeyword("ASC")
			}
			sel.OrderBy = append(sel.OrderBy, oc)
			if !p.consumeOp(",") {
				break
			}
		}
	}
	if p.consumeKeyword("LIMIT") {
		t := p.peek()
		if t.kind != tkNumber {
			return nil, p.errf("expected LIMIT count, found %q", t.text)
		}
		p.i++
		v, err := strconv.ParseInt(t.text, 10, 64)
		if err != nil {
			return nil, p.errf("invalid LIMIT %q", t.text)
		}
		sel.Limit = v
	}
	return sel, nil
}

var aggNames = map[string]bool{"count": true, "sum": true, "avg": true, "min": true, "max": true}

// parseAggExpr parses COUNT(*) / COUNT(col) / SUM(col) / AVG(col) /
// MIN(col) / MAX(col) when the next tokens form one; ok=false otherwise.
func (p *parser) parseAggExpr() (SelectExpr, bool, error) {
	t := p.peek()
	if t.kind != tkIdent || !aggNames[t.text] {
		return SelectExpr{}, false, nil
	}
	if nxt := p.toks[p.i+1]; nxt.kind != tkOp || nxt.text != "(" {
		return SelectExpr{}, false, nil
	}
	p.i += 2 // name (
	se := SelectExpr{Agg: strings.ToUpper(t.text)}
	if p.consumeOp("*") {
		if se.Agg != "COUNT" {
			return se, false, p.errf("%s(*) is not supported", se.Agg)
		}
		se.AggStar = true
	} else {
		col, err := p.expectIdent()
		if err != nil {
			return se, false, err
		}
		se.AggCol = col
	}
	if err := p.expectOp(")"); err != nil {
		return se, false, err
	}
	return se, true, nil
}

func (p *parser) parseAlterTable() (Statement, error) {
	p.i++ // ALTER
	if err := p.expectKeyword("TABLE"); err != nil {
		return nil, err
	}
	name, err := p.expectIdent()
	if err != nil {
		return nil, err
	}
	at := &AlterTable{Table: name}
	switch {
	case p.consumeKeyword("ADD"):
		p.consumeKeyword("COLUMN")
		def, err := p.parseColumnDef()
		if err != nil {
			return nil, err
		}
		at.AddCol = &def
	case p.consumeKeyword("DROP"):
		p.consumeKeyword("COLUMN")
		col, err := p.expectIdent()
		if err != nil {
			return nil, err
		}
		at.DropCol = col
	default:
		return nil, p.errf("expected ADD or DROP, found %q", p.peek().text)
	}
	return at, nil
}

func (p *parser) parseUpdate() (Statement, error) {
	p.i++ // UPDATE
	up := &Update{}
	name, err := p.expectIdent()
	if err != nil {
		return nil, err
	}
	up.Table = name
	if err := p.expectKeyword("SET"); err != nil {
		return nil, err
	}
	for {
		col, err := p.expectIdent()
		if err != nil {
			return nil, err
		}
		if err := p.expectOp("="); err != nil {
			return nil, err
		}
		e, err := p.parseValueOrColumnExpr()
		if err != nil {
			return nil, err
		}
		up.Set = append(up.Set, struct {
			Column string
			Value  Expr
		}{Column: col, Value: e})
		if !p.consumeOp(",") {
			break
		}
	}
	up.Where, err = p.parseOptWhere()
	if err != nil {
		return nil, err
	}
	return up, nil
}

func (p *parser) parseDelete() (Statement, error) {
	p.i++ // DELETE
	if err := p.expectKeyword("FROM"); err != nil {
		return nil, err
	}
	del := &Delete{}
	name, err := p.expectIdent()
	if err != nil {
		return nil, err
	}
	del.Table = name
	del.Where, err = p.parseOptWhere()
	if err != nil {
		return nil, err
	}
	return del, nil
}

func (p *parser) parseOptWhere() ([]Comparison, error) {
	if !p.consumeKeyword("WHERE") {
		return nil, nil
	}
	var out []Comparison
	for {
		col, err := p.expectIdent()
		if err != nil {
			return nil, err
		}
		t := p.peek()
		if t.kind != tkOp || !isCmpOp(t.text) {
			return nil, p.errf("expected comparison operator, found %q", t.text)
		}
		p.i++
		val, err := p.parseValueExpr()
		if err != nil {
			return nil, err
		}
		out = append(out, Comparison{Column: col, Op: t.text, Value: val})
		if !p.consumeKeyword("AND") {
			break
		}
	}
	return out, nil
}

func isCmpOp(op string) bool {
	switch op {
	case "=", "!=", "<", "<=", ">", ">=":
		return true
	}
	return false
}

// parseValueExpr parses a literal or parameter (with optional leading -).
func (p *parser) parseValueExpr() (Expr, error) {
	t := p.peek()
	neg := false
	if t.kind == tkOp && t.text == "-" {
		p.i++
		neg = true
		t = p.peek()
	}
	var e Expr
	switch t.kind {
	case tkNumber:
		p.i++
		if strings.ContainsAny(t.text, ".eE") {
			f, err := strconv.ParseFloat(t.text, 64)
			if err != nil {
				return e, p.errf("invalid number %q", t.text)
			}
			if neg {
				f = -f
			}
			d := types.NewFloat(f)
			e.Lit = &d
		} else {
			i, err := strconv.ParseInt(t.text, 10, 64)
			if err != nil {
				return e, p.errf("invalid number %q", t.text)
			}
			if neg {
				i = -i
			}
			d := types.NewInt(i)
			e.Lit = &d
		}
	case tkString:
		p.i++
		d := types.NewString(t.text)
		e.Lit = &d
	case tkParam:
		p.i++
		n, err := strconv.Atoi(t.text)
		if err != nil || n < 1 {
			return e, p.errf("invalid parameter $%s", t.text)
		}
		e.Param = n
	case tkKeyword:
		switch t.text {
		case "TRUE":
			p.i++
			d := types.NewBool(true)
			e.Lit = &d
		case "FALSE":
			p.i++
			d := types.NewBool(false)
			e.Lit = &d
		case "NULL":
			p.i++
			d := types.DNull
			e.Lit = &d
		default:
			return e, p.errf("expected value, found %q", t.text)
		}
	default:
		return e, p.errf("expected value, found %q", t.text)
	}
	if neg && e.Lit == nil {
		return e, p.errf("cannot negate non-numeric value")
	}
	// Optional ::type cast — absorbed (types come from the schema).
	if p.consumeOp("::") {
		if _, err := p.expectIdent(); err != nil {
			return e, err
		}
	}
	return e, nil
}

// parseValueOrColumnExpr additionally allows column references and
// col ± value (for SET balance = balance - 10 and SELECT col).
func (p *parser) parseValueOrColumnExpr() (Expr, error) {
	t := p.peek()
	if t.kind == tkIdent {
		p.i++
		e := Expr{Column: t.text}
		if op := p.peek(); op.kind == tkOp && (op.text == "+" || op.text == "-") {
			p.i++
			rhs, err := p.parseValueExpr()
			if err != nil {
				return e, err
			}
			e.BinOp = op.text
			e.Right = &rhs
		}
		return e, nil
	}
	return p.parseValueExpr()
}
