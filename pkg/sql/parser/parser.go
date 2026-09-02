package parser

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/sthorne/datax/pkg/sql/types"
)

// maxJoinTables caps a SELECT's FROM+JOIN table count: the executor is a
// left-deep nested loop, and every joined table multiplies its work.
const maxJoinTables = 8

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
func (p *parser) errf(format string, args ...any) error {
	return &SyntaxError{Msg: fmt.Sprintf(format, args...), Pos: p.peek().pos}
}

// parseColumnRef parses ident['.' ident] into a ColumnRef.
func (p *parser) parseColumnRef() (ColumnRef, error) {
	name, err := p.expectIdent()
	if err != nil {
		return ColumnRef{}, err
	}
	if p.consumeOp(".") {
		col, err := p.expectIdent()
		if err != nil {
			return ColumnRef{}, err
		}
		return ColumnRef{Table: name, Column: col}, nil
	}
	return ColumnRef{Column: name}, nil
}

// String renders the reference back to its source form ("t.c" or "c") —
// the single-string form carried by Expr.Column and friends.
func (r ColumnRef) String() string {
	if r.Table != "" {
		return r.Table + "." + r.Column
	}
	return r.Column
}

// expectColumnName parses a possibly-qualified column name into its
// single-string form.
func (p *parser) expectColumnName() (string, error) {
	ref, err := p.parseColumnRef()
	if err != nil {
		return "", err
	}
	return ref.String(), nil
}

// consumeIdentWord consumes the next token when it is the given
// (lower-cased) identifier — for words that are not reserved keywords.
func (p *parser) consumeIdentWord(word string) bool {
	if t := p.peek(); t.kind == tkIdent && t.text == word {
		p.i++
		return true
	}
	return false
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
		if nxt := p.toks[p.i+1]; nxt.kind == tkKeyword && nxt.text == "USER" {
			return p.parseUserStmt(false)
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
		if nxt := p.toks[p.i+1]; nxt.kind == tkKeyword && nxt.text == "USER" {
			p.i += 2 // DROP USER
			name, err := p.expectIdent()
			if err != nil {
				return nil, err
			}
			return &DropUser{Name: name}, nil
		}
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
		if nxt := p.toks[p.i+1]; nxt.kind == tkKeyword && nxt.text == "USER" {
			return p.parseUserStmt(true)
		}
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
		// ROLLBACK TO [SAVEPOINT] name
		if p.consumeKeyword("TO") {
			p.consumeIdentWord("savepoint")
			name, err := p.expectIdent()
			if err != nil {
				return nil, err
			}
			return &RollbackToSavepoint{Name: name}, nil
		}
		return &Rollback{}, nil
	case "copy": // not a reserved word; lexes as an identifier
		return p.parseCopy()
	case "grant", "revoke": // not reserved words; they lex as identifiers
		return p.parseGrantRevoke(t.text == "revoke")
	case "savepoint": // not a reserved word; lexes as an identifier
		p.i++
		name, err := p.expectIdent()
		if err != nil {
			return nil, err
		}
		return &Savepoint{Name: name}, nil
	case "release": // not a reserved word; lexes as an identifier
		p.i++
		p.consumeIdentWord("savepoint")
		name, err := p.expectIdent()
		if err != nil {
			return nil, err
		}
		return &ReleaseSavepoint{Name: name}, nil
	case "analyze": // not a reserved word; lexes as an identifier
		p.i++
		a := &Analyze{}
		if p.peek().kind == tkIdent {
			name, err := p.expectIdent()
			if err != nil {
				return nil, err
			}
			a.Table = name
		}
		return a, nil
	case "SHOW":
		p.i++
		if p.consumeKeyword("TABLES") {
			return &ShowTables{}, nil
		}
		// SHOW STATS FOR <table>: read-only statistics view.
		if p.consumeIdentWord("stats") {
			if !p.consumeIdentWord("for") {
				return nil, p.errf("expected FOR in SHOW STATS FOR <table>")
			}
			name, err := p.expectIdent()
			if err != nil {
				return nil, err
			}
			return &ShowStats{Table: name}, nil
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
	// Trailing storage options: WITH (name = value, ...). WITH is not a
	// keyword (that would collide with TIMESTAMP WITH TIME ZONE), so it
	// arrives as a plain identifier here.
	if p.consumeIdentWord("with") {
		opts, err := p.parseOptionList()
		if err != nil {
			return nil, err
		}
		ct.Options = opts
	}
	return ct, nil
}

// parseOptionList parses a parenthesized option list: ( name = value, ... ).
// Shared by CREATE TABLE ... WITH and ALTER TABLE ... SET.
func (p *parser) parseOptionList() (map[string]string, error) {
	if err := p.expectOp("("); err != nil {
		return nil, err
	}
	out := map[string]string{}
	for {
		name, err := p.expectIdent()
		if err != nil {
			return nil, err
		}
		if err := p.expectOp("="); err != nil {
			return nil, err
		}
		v := p.peek()
		val := v.text
		switch v.kind {
		case tkString, tkNumber:
			p.i++
		case tkIdent, tkKeyword:
			// Bare words (true, false) compare case-insensitively.
			p.i++
			val = strings.ToLower(val)
		default:
			return nil, p.errf("expected a value for option %q, found %q", name, v.text)
		}
		if _, dup := out[name]; dup {
			return nil, p.errf("duplicate option %q", name)
		}
		out[name] = val
		if !p.consumeOp(",") {
			break
		}
	}
	if err := p.expectOp(")"); err != nil {
		return nil, err
	}
	return out, nil
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
	// TIMESTAMP WITH[OUT] TIME ZONE: absorb the trailing words.
	if strings.EqualFold(typeName, "timestamp") {
		if p.peekIdentSeq("with", "time", "zone") || p.peekIdentSeq("without", "time", "zone") {
			p.i += 3
		}
	}
	fam, err := types.ParseType(typeName)
	if err != nil {
		return def, p.errf("%v", err)
	}
	def.Type = fam
	// VARCHAR(n) / DECIMAL(p[,s]) etc.: for DECIMAL the typmod is captured
	// and ENFORCED (precision/scale on the column descriptor); for every
	// other type it is accepted and ignored — storage is arbitrary-length
	// (documented).
	if p.consumeOp("(") {
		var mods []string
		if p.peek().kind == tkNumber {
			mods = append(mods, p.peek().text)
			p.i++
			if p.consumeOp(",") {
				if p.peek().kind != tkNumber {
					return def, p.errf("expected scale after ',' in type modifier")
				}
				mods = append(mods, p.peek().text)
				p.i++
			}
		}
		if err := p.expectOp(")"); err != nil {
			return def, err
		}
		if fam == types.Decimal && len(mods) > 0 {
			prec, perr := strconv.ParseInt(mods[0], 10, 32)
			if perr != nil {
				return def, p.errf("DECIMAL precision %q must be an integer", mods[0])
			}
			var scale int64
			if len(mods) > 1 {
				var serr error
				scale, serr = strconv.ParseInt(mods[1], 10, 32)
				if serr != nil {
					return def, p.errf("DECIMAL scale %q must be an integer", mods[1])
				}
			}
			if prec < 1 || prec > 1000 {
				return def, p.errf("DECIMAL precision %d must be between 1 and 1000", prec)
			}
			if scale < 0 || scale > prec {
				return def, p.errf("DECIMAL scale %d must be between 0 and the precision %d", scale, prec)
			}
			def.Precision, def.Scale = int32(prec), int32(scale)
		}
	}
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
		case p.consumeIdentWord("default"):
			// Constant defaults only ("default" is not a reserved word).
			e, err := p.parseValueExpr()
			if err != nil {
				return def, err
			}
			if e.Lit == nil {
				return def, p.errf("DEFAULT must be a constant literal")
			}
			def.Default = e.Lit
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
			e, err := p.parseValueOrColumnExpr()
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

// parseCopy parses COPY table [(col, ...)] FROM STDIN [format clause].
// Accepted format spellings — all of them appear in the wild:
//
//	COPY t FROM STDIN                        -- text (default)
//	COPY t FROM STDIN [WITH] (FORMAT csv)    -- PostgreSQL 9.0+ option list
//	COPY t FROM STDIN BINARY                 -- pre-9.0 trailing word (pgx)
//
// None of COPY/STDIN/FORMAT/WITH/BINARY are reserved words; they lex as
// identifiers.
func (p *parser) parseCopy() (Statement, error) {
	p.i++ // copy
	cf := &CopyFrom{Format: CopyFormatText}
	name, err := p.expectIdent()
	if err != nil {
		return nil, err
	}
	cf.Table = name
	if p.consumeOp("(") {
		for {
			col, err := p.expectIdent()
			if err != nil {
				return nil, err
			}
			cf.Columns = append(cf.Columns, col)
			if !p.consumeOp(",") {
				break
			}
		}
		if err := p.expectOp(")"); err != nil {
			return nil, err
		}
	}
	if p.consumeKeyword("TO") {
		return nil, p.errf("COPY TO is not supported")
	}
	if err := p.expectKeyword("FROM"); err != nil {
		return nil, err
	}
	if !p.consumeIdentWord("stdin") {
		return nil, p.errf("only COPY ... FROM STDIN is supported, found %q", p.peek().text)
	}
	switch {
	case p.consumeIdentWord("binary"):
		cf.Format = CopyFormatBinary
	default:
		hasWith := p.consumeIdentWord("with")
		if p.consumeOp("(") {
			if err := p.parseCopyOptions(cf); err != nil {
				return nil, err
			}
		} else if hasWith {
			return nil, p.errf("expected ( after WITH, found %q", p.peek().text)
		}
	}
	return cf, nil
}

// parseCopyOptions parses the body of a COPY option list after its opening
// paren: option [value] pairs with NO "=" (unlike parseOptionList's
// name = value shape used by CREATE TABLE ... WITH).
func (p *parser) parseCopyOptions(cf *CopyFrom) error {
	for {
		name, err := p.expectIdent()
		if err != nil {
			return err
		}
		switch name {
		case "format":
			t := p.peek()
			if t.kind != tkIdent && t.kind != tkString {
				return p.errf("expected a format name, found %q", t.text)
			}
			p.i++
			switch strings.ToLower(t.text) {
			case "text":
				cf.Format = CopyFormatText
			case "csv":
				cf.Format = CopyFormatCSV
			case "binary":
				cf.Format = CopyFormatBinary
			default:
				return p.errf("unknown COPY format %q", t.text)
			}
		default:
			return p.errf("unsupported COPY option %q", name)
		}
		if !p.consumeOp(",") {
			break
		}
	}
	return p.expectOp(")")
}

func (p *parser) parseSelect() (Statement, error) {
	p.i++ // SELECT
	sel := &Select{Limit: -1}
	// DISTINCT is not a reserved word — it lexes as an identifier.
	if p.consumeIdentWord("distinct") {
		sel.Distinct = true
	}
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
		// FROM (SELECT ...) AS alias — a derived table.
		if sub, ok, err := p.parseSubquery(); err != nil {
			return nil, err
		} else if ok {
			sel.Derived = sub
			sel.Alias = p.parseOptTableAlias(false)
			if sel.Alias == "" {
				return nil, p.errf("subquery in FROM must have an alias")
			}
			return p.finishSelect(sel)
		}
		name, err := p.expectIdent()
		if err != nil {
			return nil, err
		}
		sel.Table = name
		sel.Alias = p.parseOptTableAlias(true)
		// JOIN / INNER JOIN / LEFT [OUTER] JOIN chains — none are reserved
		// words. Joins execute left-deep in syntactic order.
		for {
			var left bool
			join := false
			switch {
			case p.consumeIdentWord("join"):
				join = true
			case p.consumeIdentWord("inner"):
				if !p.consumeIdentWord("join") {
					return nil, p.errf("expected JOIN after INNER, found %q", p.peek().text)
				}
				join = true
			case p.consumeIdentWord("left"):
				p.consumeIdentWord("outer")
				if !p.consumeIdentWord("join") {
					return nil, p.errf("expected JOIN after LEFT [OUTER], found %q", p.peek().text)
				}
				join, left = true, true
			}
			if !join {
				break
			}
			if len(sel.Joins) >= maxJoinTables-1 {
				return nil, p.errf("too many joined tables (limit %d)", maxJoinTables)
			}
			jt, err := p.expectIdent()
			if err != nil {
				return nil, err
			}
			jc := JoinClause{Left: left, Table: jt}
			jc.Alias = p.parseOptTableAlias(false)
			if err := p.expectKeyword("ON"); err != nil {
				return nil, err
			}
			for {
				l, err := p.parseColumnRef()
				if err != nil {
					return nil, err
				}
				if err := p.expectOp("="); err != nil {
					return nil, err
				}
				r, err := p.parseColumnRef()
				if err != nil {
					return nil, err
				}
				jc.On = append(jc.On, JoinCond{L: l, R: r})
				if !p.consumeKeyword("AND") {
					break
				}
			}
			sel.Joins = append(sel.Joins, jc)
		}
		// AS OF SYSTEM TIME <literal> (parseOptTableAlias leaves this AS
		// untouched; SYSTEM and TIME lex as identifiers).
		if p.consumeKeyword("AS") {
			for _, word := range []string{"of", "system", "time"} {
				if t := p.peek(); t.kind == tkIdent && t.text == word {
					p.i++
				} else {
					return nil, p.errf("expected %s in AS OF SYSTEM TIME, found %q", strings.ToUpper(word), t.text)
				}
			}
			// with_max_staleness('10s') asks for the freshest timestamp
			// the local store can serve within the bound (bounded
			// staleness); a bare literal pins an exact timestamp.
			if p.consumeIdentWord("with_max_staleness") {
				if err := p.expectOp("("); err != nil {
					return nil, err
				}
				t := p.peek()
				if t.kind != tkString {
					return nil, p.errf("with_max_staleness expects a duration string, found %q", t.text)
				}
				sel.AsOfMaxStaleness = t.text
				p.i++
				if err := p.expectOp(")"); err != nil {
					return nil, err
				}
			} else {
				t := p.peek()
				switch t.kind {
				case tkString, tkNumber:
					sel.AsOf = t.text
					p.i++
				default:
					return nil, p.errf("expected AS OF SYSTEM TIME operand, found %q", t.text)
				}
			}
		}
	}
	return p.finishSelect(sel)
}

// finishSelect parses the clauses shared by table and derived-table
// selects: WHERE, GROUP BY, HAVING, ORDER BY, LIMIT, FOR UPDATE.
func (p *parser) finishSelect(sel *Select) (Statement, error) {
	var err error
	sel.Where, err = p.parseOptWhere()
	if err != nil {
		return nil, err
	}
	// GROUP and HAVING are not reserved words — they lex as identifiers.
	if p.consumeIdentWord("group") {
		if err := p.expectKeyword("BY"); err != nil {
			return nil, err
		}
		for {
			// Possibly qualified: GROUP BY c.name over a join.
			ref, err := p.parseColumnRef()
			if err != nil {
				return nil, err
			}
			sel.GroupBy = append(sel.GroupBy, ref.String())
			if !p.consumeOp(",") {
				break
			}
		}
	}
	if p.consumeIdentWord("having") {
		sel.Having, err = p.parseHaving()
		if err != nil {
			return nil, err
		}
	}
	if p.consumeKeyword("ORDER") {
		if err := p.expectKeyword("BY"); err != nil {
			return nil, err
		}
		for {
			col, err := p.expectColumnName()
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
	// FOR UPDATE (FOR is not a reserved word — it lexes as an identifier).
	if t := p.peek(); t.kind == tkIdent && t.text == "for" {
		p.i++
		if err := p.expectKeyword("UPDATE"); err != nil {
			return nil, err
		}
		if sel.Table == "" {
			return nil, p.errf("FOR UPDATE requires a FROM clause")
		}
		sel.ForUpdate = true
	}
	return sel, nil
}

// tableClauseWords are non-reserved identifier words that begin a clause
// after a table name — never a bare table alias.
var tableClauseWords = map[string]bool{
	"join": true, "inner": true, "left": true, "group": true, "having": true, "for": true,
}

// peekIdentSeq reports whether the next tokens are exactly this sequence of
// (lower-cased) identifiers.
func (p *parser) peekIdentSeq(words ...string) bool {
	for j, w := range words {
		if p.i+j >= len(p.toks) {
			return false
		}
		if t := p.toks[p.i+j]; t.kind != tkIdent || t.text != w {
			return false
		}
	}
	return true
}

// parseOptTableAlias consumes an optional [AS] alias after a table name.
// In the outer-table position (allowAsOf) an AS followed by the
// of/system/time identifier sequence is AS OF SYSTEM TIME, not an alias,
// and is left for the caller.
func (p *parser) parseOptTableAlias(allowAsOf bool) string {
	if p.consumeKeyword("AS") {
		if allowAsOf && p.peekIdentSeq("of", "system", "time") {
			p.i-- // give AS back: it introduces AS OF SYSTEM TIME
			return ""
		}
		if t := p.peek(); t.kind == tkIdent {
			p.i++
			return t.text
		}
		p.i-- // AS not followed by an alias: leave it for the caller
		return ""
	}
	if t := p.peek(); t.kind == tkIdent && !tableClauseWords[t.text] {
		p.i++
		return t.text
	}
	return ""
}

// parseHaving parses the HAVING conjunction: each conjunct compares an
// aggregate call or a column/output name against a literal or parameter.
func (p *parser) parseHaving() ([]HavingCond, error) {
	var out []HavingCond
	for {
		var hc HavingCond
		if se, ok, err := p.parseAggExpr(); err != nil {
			return nil, err
		} else if ok {
			hc.Agg = &se
		} else {
			col, err := p.expectIdent()
			if err != nil {
				return nil, err
			}
			hc.Column = col
		}
		t := p.peek()
		if t.kind != tkOp || !isCmpOp(t.text) {
			return nil, p.errf("expected comparison operator, found %q", t.text)
		}
		p.i++
		val, err := p.parseValueOrColumnExpr()
		if err != nil {
			return nil, err
		}
		hc.Op, hc.Value = t.text, val
		out = append(out, hc)
		if !p.consumeKeyword("AND") {
			break
		}
	}
	return out, nil
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
		// Possibly qualified: SUM(o.total) over a join.
		ref, err := p.parseColumnRef()
		if err != nil {
			return se, false, err
		}
		se.AggCol = ref.String()
	}
	if err := p.expectOp(")"); err != nil {
		return se, false, err
	}
	return se, true, nil
}

// parseGrantRevoke parses GRANT/REVOKE ADMIN and per-table privileges.
func (p *parser) parseGrantRevoke(revoke bool) (Statement, error) {
	p.i++ // grant | revoke
	gr := &GrantRevoke{Revoke: revoke}
	linkKw := "TO"
	if revoke {
		linkKw = "FROM"
	}

	if p.consumeIdentWord("admin") {
		gr.Admin = true
		if err := p.expectKeyword(linkKw); err != nil {
			return nil, err
		}
		name, err := p.expectIdent()
		if err != nil {
			return nil, err
		}
		gr.User = name
		return gr, nil
	}

	for {
		t := p.peek()
		var priv string
		switch {
		case t.kind == tkKeyword && (t.text == "SELECT" || t.text == "INSERT" || t.text == "UPDATE" || t.text == "DELETE"):
			priv = t.text
			p.i++
		case t.kind == tkIdent && t.text == "all":
			priv = "ALL"
			p.i++
		default:
			return nil, p.errf("expected a privilege (SELECT, INSERT, UPDATE, DELETE, ALL), found %q", t.text)
		}
		gr.Privileges = append(gr.Privileges, priv)
		if !p.consumeOp(",") {
			break
		}
	}
	if err := p.expectKeyword("ON"); err != nil {
		return nil, err
	}
	p.consumeKeyword("TABLE")
	table, err := p.expectIdent()
	if err != nil {
		return nil, err
	}
	gr.Table = table
	if err := p.expectKeyword(linkKw); err != nil {
		return nil, err
	}
	name, err := p.expectIdent()
	if err != nil {
		return nil, err
	}
	gr.User = name
	return gr, nil
}

// parseUserStmt parses CREATE USER / ALTER USER name PASSWORD 'pw'.
func (p *parser) parseUserStmt(alter bool) (Statement, error) {
	p.i += 2 // CREATE|ALTER USER
	name, err := p.expectIdent()
	if err != nil {
		return nil, err
	}
	if err := p.expectKeyword("PASSWORD"); err != nil {
		return nil, err
	}
	t := p.peek()
	if t.kind != tkString {
		return nil, p.errf("expected password string, found %q", t.text)
	}
	p.i++
	return &CreateUser{Name: name, Password: t.text, Alter: alter}, nil
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
	case p.consumeKeyword("SET"):
		opts, err := p.parseOptionList()
		if err != nil {
			return nil, err
		}
		at.SetOptions = opts
	default:
		return nil, p.errf("expected ADD, DROP, or SET, found %q", p.peek().text)
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
	n, err := p.parseBoolOr()
	if err != nil {
		return nil, err
	}
	return lowerBool(n, false)
}

// boolNode is the parse-time boolean tree; lowerBool flattens it into the
// executor's conjunction-of-comparisons form, eliminating NOT on the way.
type boolNode struct {
	op   string // "and", "or", "not", "leaf"
	kids []boolNode
	leaf Comparison
}

func (p *parser) parseBoolOr() (boolNode, error) {
	n, err := p.parseBoolAnd()
	if err != nil {
		return n, err
	}
	if p.peek().kind == tkKeyword && p.peek().text == "OR" {
		node := boolNode{op: "or", kids: []boolNode{n}}
		for p.consumeKeyword("OR") {
			k, err := p.parseBoolAnd()
			if err != nil {
				return node, err
			}
			node.kids = append(node.kids, k)
		}
		return node, nil
	}
	return n, nil
}

func (p *parser) parseBoolAnd() (boolNode, error) {
	n, err := p.parseBoolFactor()
	if err != nil {
		return n, err
	}
	if p.peek().kind == tkKeyword && p.peek().text == "AND" {
		node := boolNode{op: "and", kids: []boolNode{n}}
		for p.consumeKeyword("AND") {
			k, err := p.parseBoolFactor()
			if err != nil {
				return node, err
			}
			node.kids = append(node.kids, k)
		}
		return node, nil
	}
	return n, nil
}

func (p *parser) parseBoolFactor() (boolNode, error) {
	// NOT EXISTS is a leaf (parseExistsCond claims it); any other NOT is
	// boolean negation.
	if t := p.peek(); t.kind == tkKeyword && t.text == "NOT" {
		if nxt := p.toks[p.i+1]; !(nxt.kind == tkKeyword && nxt.text == "EXISTS") {
			p.i++
			k, err := p.parseBoolFactor()
			if err != nil {
				return k, err
			}
			return boolNode{op: "not", kids: []boolNode{k}}, nil
		}
	}
	// Parenthesized boolean group. A leaf conjunct never starts with "(",
	// so the paren is unambiguous here.
	if p.peek().kind == tkOp && p.peek().text == "(" {
		p.i++
		n, err := p.parseBoolOr()
		if err != nil {
			return n, err
		}
		if err := p.expectOp(")"); err != nil {
			return n, err
		}
		return n, nil
	}
	cmp, err := p.parseConjunct()
	if err != nil {
		return boolNode{}, err
	}
	return boolNode{op: "leaf", leaf: cmp}, nil
}

// parseConjunct parses one atomic condition: [NOT] EXISTS(...), col IS
// [NOT] NULL, col [NOT] IN (...), or col op value.
func (p *parser) parseConjunct() (Comparison, error) {
	if cmp, ok, err := p.parseExistsCond(); err != nil {
		return Comparison{}, err
	} else if ok {
		return cmp, nil
	}
	lhs, err := p.parseAddExpr()
	if err != nil {
		return Comparison{}, err
	}
	col, path := lhs.Column, lhs.Path
	computed := lhs.Column == "" || lhs.BinOp != "" || lhs.Func != "" || lhs.Left != nil
	if computed {
		// Computed left-hand side: only plain comparisons apply.
		t := p.peek()
		if t.kind != tkOp || !isCmpOp(t.text) {
			return Comparison{}, p.errf("expected comparison operator, found %q", t.text)
		}
		p.i++
		val, err := p.parseValueOrColumnExpr()
		if err != nil {
			return Comparison{}, err
		}
		return Comparison{Expr: &lhs, Op: t.text, Value: val}, nil
	}
	// col IS [NOT] NULL ("is" is not a reserved keyword).
	if p.consumeIdentWord("is") {
		op := "IS NULL"
		if p.consumeKeyword("NOT") {
			op = "IS NOT NULL"
		}
		if err := p.expectKeyword("NULL"); err != nil {
			return Comparison{}, err
		}
		return Comparison{Column: col, Path: path, Op: op}, nil
	}
	// col [NOT] IN (value, ... | SELECT ...) ("in" is an identifier).
	notIn := false
	if p.peek().kind == tkKeyword && p.peek().text == "NOT" &&
		p.i+1 < len(p.toks) && p.toks[p.i+1].kind == tkIdent && p.toks[p.i+1].text == "in" {
		p.i++
		notIn = true
	}
	if notIn || p.consumeIdentWord("in") {
		if notIn {
			p.i++ // the "in" identifier
		}
		cmp := Comparison{Column: col, Path: path, Op: "IN"}
		if notIn {
			cmp.Op = "NOT IN"
		}
		if sub, ok, err := p.parseSubquery(); err != nil {
			return Comparison{}, err
		} else if ok {
			cmp.Sub = sub
		} else {
			if err := p.expectOp("("); err != nil {
				return Comparison{}, err
			}
			for {
				v, err := p.parseValueExpr()
				if err != nil {
					return Comparison{}, err
				}
				cmp.Values = append(cmp.Values, v)
				if !p.consumeOp(",") {
					break
				}
			}
			if err := p.expectOp(")"); err != nil {
				return Comparison{}, err
			}
		}
		return cmp, nil
	}
	t := p.peek()
	// JSONB containment: accepted only in this plain-column form (an
	// optional path on the left is fine — it must still produce jsonb).
	// HAVING and computed left-hand sides deliberately do not take @>.
	if t.kind != tkOp || (!isCmpOp(t.text) && t.text != "@>") {
		return Comparison{}, p.errf("expected comparison operator, found %q", t.text)
	}
	p.i++
	// The right side may itself be a column reference (a = b, or a
	// correlated outer.col inside a subquery); row-dependent values
	// are excluded from access planning and checked by the filter.
	val, err := p.parseValueOrColumnExpr()
	if err != nil {
		return Comparison{}, err
	}
	return Comparison{Column: col, Path: path, Op: t.text, Value: val}, nil
}

// lowerBool flattens the boolean tree into a conjunction of comparisons.
// NOT is eliminated by De Morgan plus operator negation — sound under SQL
// three-valued logic because WHERE keeps exactly the TRUE rows and both a
// negated UNKNOWN and an UNKNOWN negation stay UNKNOWN. An OR subtree
// becomes one Op-"OR" comparison holding a disjunction of conjunctions;
// subqueries inside OR are rejected (v1 — the subquery machinery splices
// over the flat conjunction only).
func lowerBool(n boolNode, negated bool) ([]Comparison, error) {
	switch n.op {
	case "not":
		return lowerBool(n.kids[0], !negated)
	case "and", "or":
		op := n.op
		if negated { // De Morgan
			if op == "and" {
				op = "or"
			} else {
				op = "and"
			}
		}
		if op == "and" {
			var out []Comparison
			for _, k := range n.kids {
				kc, err := lowerBool(k, negated)
				if err != nil {
					return nil, err
				}
				out = append(out, kc...)
			}
			return out, nil
		}
		var disjuncts [][]Comparison
		for _, k := range n.kids {
			kc, err := lowerBool(k, negated)
			if err != nil {
				return nil, err
			}
			for _, c := range kc {
				if err := rejectSubInOr(c); err != nil {
					return nil, err
				}
			}
			disjuncts = append(disjuncts, kc)
		}
		if in, ok := orAsIn(disjuncts); ok {
			return []Comparison{in}, nil
		}
		return []Comparison{{Op: "OR", Or: disjuncts}}, nil
	default: // leaf
		leaf := n.leaf
		if negated {
			neg, err := negateComparison(leaf)
			if err != nil {
				return nil, err
			}
			leaf = neg
		}
		return []Comparison{leaf}, nil
	}
}

// rejectSubInOr refuses subqueries anywhere inside an OR group.
func rejectSubInOr(c Comparison) error {
	if c.Sub != nil || exprContainsSub(c.Value) || (c.Expr != nil && exprContainsSub(*c.Expr)) {
		return fmt.Errorf("subqueries are not supported inside OR")
	}
	for _, v := range c.Values {
		if exprContainsSub(v) {
			return fmt.Errorf("subqueries are not supported inside OR")
		}
	}
	for _, d := range c.Or {
		for _, inner := range d {
			if err := rejectSubInOr(inner); err != nil {
				return err
			}
		}
	}
	return nil
}

func exprContainsSub(e Expr) bool {
	if e.Sub != nil {
		return true
	}
	if e.Left != nil && exprContainsSub(*e.Left) {
		return true
	}
	if e.Right != nil && exprContainsSub(*e.Right) {
		return true
	}
	for _, a := range e.Args {
		if exprContainsSub(a) {
			return true
		}
	}
	return false
}

// orAsIn rewrites `a = x OR a = y OR ...` (single-equality disjuncts on
// one bare column) into `a IN (x, y, ...)`.
func orAsIn(disjuncts [][]Comparison) (Comparison, bool) {
	var col string
	in := Comparison{Op: "IN"}
	for i, d := range disjuncts {
		if len(d) != 1 {
			return Comparison{}, false
		}
		c := d[0]
		if c.Op != "=" || c.Column == "" || len(c.Path) > 0 || len(c.Or) > 0 {
			return Comparison{}, false
		}
		if i == 0 {
			col = c.Column
		} else if c.Column != col {
			return Comparison{}, false
		}
		in.Values = append(in.Values, c.Value)
	}
	in.Column = col
	return in, true
}

// negateComparison negates one atomic condition exactly (three-valued
// logic: UNKNOWN stays UNKNOWN either way).
func negateComparison(c Comparison) (Comparison, error) {
	neg := map[string]string{
		"=": "!=", "!=": "=", "<": ">=", ">=": "<", ">": "<=", "<=": ">",
		"IS NULL": "IS NOT NULL", "IS NOT NULL": "IS NULL",
		"IN": "NOT IN", "NOT IN": "IN",
		"EXISTS": "NOT EXISTS", "NOT EXISTS": "EXISTS",
		"TRUE": "FALSE", "FALSE": "TRUE",
		"@>": "NOT @>", "NOT @>": "@>",
	}
	if c.Op == "OR" {
		// NOT over a lowered OR: De Morgan by hand — negate every inner
		// conjunct and swap the shape (the disjunction of conjunctions
		// becomes a conjunction of disjunctions, re-lowered as nested ORs).
		var conj []Comparison
		for _, d := range c.Or {
			var alts [][]Comparison
			for _, inner := range d {
				n, err := negateComparison(inner)
				if err != nil {
					return Comparison{}, err
				}
				alts = append(alts, []Comparison{n})
			}
			if len(alts) == 1 {
				conj = append(conj, alts[0][0])
			} else {
				conj = append(conj, Comparison{Op: "OR", Or: alts})
			}
		}
		if len(conj) == 1 {
			return conj[0], nil
		}
		// A conjunction can't be one comparison: wrap as a single-disjunct
		// OR (a disjunction with one conjunction is just that conjunction).
		return Comparison{Op: "OR", Or: [][]Comparison{conj}}, nil
	}
	op, ok := neg[c.Op]
	if !ok {
		return Comparison{}, fmt.Errorf("cannot negate %q", c.Op)
	}
	c.Op = op
	return c, nil
}

// parseExistsCond parses [NOT] EXISTS (SELECT ...) when present.
func (p *parser) parseExistsCond() (Comparison, bool, error) {
	not := false
	switch {
	case p.consumeKeyword("EXISTS"):
	case p.peek().kind == tkKeyword && p.peek().text == "NOT" &&
		p.i+1 < len(p.toks) && p.toks[p.i+1].kind == tkKeyword && p.toks[p.i+1].text == "EXISTS":
		p.i += 2
		not = true
	default:
		return Comparison{}, false, nil
	}
	sub, ok, err := p.parseSubquery()
	if err != nil {
		return Comparison{}, false, err
	}
	if !ok {
		return Comparison{}, false, p.errf("expected (SELECT ...) after EXISTS, found %q", p.peek().text)
	}
	cmp := Comparison{Op: "EXISTS", Sub: sub}
	if not {
		cmp.Op = "NOT EXISTS"
	}
	return cmp, true, nil
}

func isCmpOp(op string) bool {
	switch op {
	case "=", "!=", "<", "<=", ">", ">=":
		return true
	}
	return false
}

// parseSubquery parses (SELECT ...) when the next tokens open one;
// ok=false otherwise.
func (p *parser) parseSubquery() (*Select, bool, error) {
	if t := p.peek(); t.kind != tkOp || t.text != "(" {
		return nil, false, nil
	}
	if nxt := p.toks[p.i+1]; nxt.kind != tkKeyword || nxt.text != "SELECT" {
		return nil, false, nil
	}
	p.i++ // (
	stmt, err := p.parseSelect()
	if err != nil {
		return nil, false, err
	}
	if err := p.expectOp(")"); err != nil {
		return nil, false, err
	}
	return stmt.(*Select), true, nil
}

// parseValueExpr parses a literal, parameter, or scalar subquery (with
// optional leading - on numbers).
func (p *parser) parseValueExpr() (Expr, error) {
	if sub, ok, err := p.parseSubquery(); err != nil {
		return Expr{}, err
	} else if ok {
		return Expr{Sub: sub}, nil
	}
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
			// Non-integer numeric literals are exact DECIMALs (PostgreSQL
			// semantics): 0.1 must survive to a DECIMAL column unrounded.
			// Float columns coerce Decimal→Float on demand (lossy is fine
			// in that direction).
			txt := t.text
			if neg {
				txt = "-" + txt
			}
			d, err := types.ParseDecimal(txt)
			if err != nil {
				return e, p.errf("invalid number %q", t.text)
			}
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

// parsePathSteps parses a chained ->/->> JSONB extraction after a column
// reference. Keys are string literals; ->> renders text and is therefore
// terminal — jsonb -> 'a' ->> 'b' is fine, ->> 'a' -> 'b' is not.
func (p *parser) parsePathSteps() ([]PathStep, error) {
	var steps []PathStep
	for {
		t := p.peek()
		if t.kind != tkOp || (t.text != "->" && t.text != "->>") {
			return steps, nil
		}
		if len(steps) > 0 && steps[len(steps)-1].Text {
			return nil, p.errf("cannot apply %s after ->> (->> yields text, not jsonb)", t.text)
		}
		p.i++
		key := p.peek()
		if key.kind != tkString {
			return nil, p.errf("expected string key after %s, found %q", t.text, key.text)
		}
		p.i++
		steps = append(steps, PathStep{Key: key.text, Text: t.text == "->>"})
	}
}

// parseValueOrColumnExpr additionally allows column references and
// col ± value (for SET balance = balance - 10 and SELECT col).
func (p *parser) parseValueOrColumnExpr() (Expr, error) {
	return p.parseAddExpr()
}

// parseAddExpr / parseMulExpr implement left-associative arithmetic with
// the usual precedence (* / bind tighter than + -). A single binary op
// keeps the flat historical Expr shape; further chaining nests the
// accumulated expression through Left.
func (p *parser) parseAddExpr() (Expr, error) {
	e, err := p.parseMulExpr()
	if err != nil {
		return e, err
	}
	for {
		op := p.peek()
		if op.kind != tkOp || (op.text != "+" && op.text != "-") {
			return e, nil
		}
		p.i++
		rhs, err := p.parseMulExpr()
		if err != nil {
			return e, err
		}
		e = foldBinOp(e, op.text, rhs)
	}
}

func (p *parser) parseMulExpr() (Expr, error) {
	e, err := p.parsePrimaryExpr()
	if err != nil {
		return e, err
	}
	for {
		op := p.peek()
		if op.kind != tkOp || (op.text != "*" && op.text != "/") {
			return e, nil
		}
		p.i++
		rhs, err := p.parsePrimaryExpr()
		if err != nil {
			return e, err
		}
		e = foldBinOp(e, op.text, rhs)
	}
}

// foldBinOp attaches (lhs op rhs), reusing lhs's own node when it carries
// no operator yet (the flat historical shape) and nesting through Left
// otherwise (left associativity).
func foldBinOp(lhs Expr, op string, rhs Expr) Expr {
	if lhs.BinOp == "" {
		lhs.BinOp = op
		lhs.Right = &rhs
		return lhs
	}
	l := lhs
	return Expr{Left: &l, BinOp: op, Right: &rhs}
}

// scalarFuncs are the builtin scalar functions ("now" is spliced by the
// session before execution; the rest evaluate per row).
var scalarFuncs = map[string]int{ // name → arity (-1 = variadic, min 1)
	"now": 0, "length": 1, "lower": 1, "upper": 1, "abs": 1, "coalesce": -1,
}

// parsePrimaryExpr parses one operand: a parenthesized expression, a
// builtin call, a possibly-qualified column reference (with an optional
// ->/->> chain), or a literal/parameter/scalar subquery.
func (p *parser) parsePrimaryExpr() (Expr, error) {
	t := p.peek()
	if t.kind == tkOp && t.text == "(" {
		// A scalar subquery is "(SELECT"; anything else is grouping.
		if nxt := p.toks[p.i+1]; !(nxt.kind == tkKeyword && nxt.text == "SELECT") {
			p.i++
			e, err := p.parseAddExpr()
			if err != nil {
				return e, err
			}
			if err := p.expectOp(")"); err != nil {
				return e, err
			}
			return e, nil
		}
		return p.parseValueExpr()
	}
	if t.kind == tkIdent {
		// Builtin call: known name followed by "(".
		if arity, isFunc := scalarFuncs[t.text]; isFunc &&
			p.i+1 < len(p.toks) && p.toks[p.i+1].kind == tkOp && p.toks[p.i+1].text == "(" {
			p.i += 2
			e := Expr{Func: t.text}
			if !p.consumeOp(")") {
				for {
					a, err := p.parseAddExpr()
					if err != nil {
						return e, err
					}
					e.Args = append(e.Args, a)
					if !p.consumeOp(",") {
						break
					}
				}
				if err := p.expectOp(")"); err != nil {
					return e, err
				}
			}
			switch {
			case arity == -1 && len(e.Args) == 0:
				return e, p.errf("%s() requires at least one argument", t.text)
			case arity >= 0 && len(e.Args) != arity:
				return e, p.errf("%s() takes %d argument(s), got %d", t.text, arity, len(e.Args))
			}
			return e, nil
		}
		p.i++
		name := t.text
		if p.consumeOp(".") {
			col, err := p.expectIdent()
			if err != nil {
				return Expr{}, err
			}
			name += "." + col
		}
		e := Expr{Column: name}
		path, err := p.parsePathSteps()
		if err != nil {
			return Expr{}, err
		}
		e.Path = path
		return e, nil
	}
	return p.parseValueExpr()
}
