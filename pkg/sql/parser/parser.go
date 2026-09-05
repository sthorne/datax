package parser

import (
	"fmt"
	"github.com/sthorne/datax/pkg/sql/builtins"
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
	p := &parser{toks: toks, src: src}
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
	src  string
}

func (p *parser) peek() token { return p.toks[p.i] }
func (p *parser) errf(format string, args ...any) error {
	return &SyntaxError{Msg: fmt.Sprintf(format, args...), Pos: p.peek().pos}
}

// tableFuncs are the set-returning functions accepted in FROM.
var tableFuncs = map[string]bool{"unnest": true, "pg_partition_ancestors": true}

// parseFuncTable parses a FROM-clause table function call, if present.
func (p *parser) parseFuncTable() (*Expr, bool, error) {
	save := p.i
	if p.peekIdentSeq("pg_catalog") && p.i+1 < len(p.toks) && p.toks[p.i+1].kind == tkOp && p.toks[p.i+1].text == "." {
		p.i += 2
	}
	t := p.peek()
	if t.kind != tkIdent || !tableFuncs[t.text] || p.i+1 >= len(p.toks) || p.toks[p.i+1].kind != tkOp || p.toks[p.i+1].text != "(" {
		p.i = save
		return nil, false, nil
	}
	p.i += 2
	e := Expr{Func: t.text}
	for {
		a, err := p.parseAddExpr()
		if err != nil {
			return nil, false, err
		}
		e.Args = append(e.Args, a)
		if !p.consumeOp(",") {
			break
		}
	}
	if err := p.expectOp(")"); err != nil {
		return nil, false, err
	}
	return &e, true, nil
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

// joinEquality reports whether c is "colref = colref" — the join-key
// form — and returns both references.
func joinEquality(c Comparison) (l, r ColumnRef, ok bool) {
	if c.Op != "=" || c.Column == "" || len(c.Path) > 0 || c.Expr != nil {
		return l, r, false
	}
	v := c.Value
	if v.Column == "" || len(v.Path) > 0 || v.Lit != nil || v.Param != 0 || v.Sub != nil ||
		v.Func != "" || v.BinOp != "" || v.Left != nil || v.Case != nil {
		return l, r, false
	}
	return columnRefOf(c.Column), columnRefOf(v.Column), true
}

// columnRefOf splits a "t.c" string back into a ColumnRef.
func columnRefOf(name string) ColumnRef {
	if i := strings.LastIndexByte(name, '.'); i >= 0 {
		return ColumnRef{Table: name[:i], Column: name[i+1:]}
	}
	return ColumnRef{Column: name}
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

// parseTableName parses a possibly qualified table name: t, public.t,
// db.t or db.public.t. The only schema is public, so the result is the
// bare name or "db.name"; the session resolves an unqualified name in
// its current database.
func (p *parser) parseTableName() (string, error) {
	first, err := p.expectIdent()
	if err != nil {
		return "", err
	}
	parts := []string{first}
	for p.consumeOp(".") {
		next, err := p.expectIdent()
		if err != nil {
			return "", err
		}
		parts = append(parts, next)
	}
	switch len(parts) {
	case 1:
		return parts[0], nil
	case 2:
		if parts[0] == "public" {
			return parts[1], nil
		}
		return parts[0] + "." + parts[1], nil
	case 3:
		if parts[1] != "public" {
			return "", p.errf("schema %q does not exist (public is the only schema)", parts[1])
		}
		return parts[0] + "." + parts[2], nil
	}
	return "", p.errf("too many qualifiers in table name %q", strings.Join(parts, "."))
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
	if (t.kind == tkOp && t.text == "(") || (t.kind == tkKeyword && t.text == "VALUES") {
		// (SELECT ...) [UNION ...] [ORDER BY ...]: a parenthesized query;
		// VALUES (...), (...) as a statement of its own.
		sel, err := p.parseSetMember()
		if err != nil {
			return nil, err
		}
		return sel, nil
	}
	if t.kind == tkIdent && t.text == "with" {
		return p.parseWith()
	}
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
		if nxt := p.toks[p.i+1]; nxt.kind == tkIdent && nxt.text == "sequence" {
			return p.parseCreateSequence()
		}
		if nxt := p.toks[p.i+1]; nxt.kind == tkIdent && nxt.text == "database" {
			p.i += 2 // CREATE DATABASE
			cd := &CreateDatabase{}
			if p.consumeKeyword("IF") {
				if err := p.expectKeyword("NOT"); err != nil {
					return nil, err
				}
				if err := p.expectKeyword("EXISTS"); err != nil {
					return nil, err
				}
				cd.IfNotExists = true
			}
			name, err := p.expectIdent()
			if err != nil {
				return nil, err
			}
			cd.Name = name
			return cd, nil
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
		if nxt := p.toks[p.i+1]; nxt.kind == tkIdent && nxt.text == "sequence" {
			p.i += 2 // DROP SEQUENCE
			ds := &DropSequence{}
			if p.consumeKeyword("IF") {
				if err := p.expectKeyword("EXISTS"); err != nil {
					return nil, err
				}
				ds.IfExists = true
			}
			name, err := p.parseTableName()
			if err != nil {
				return nil, err
			}
			ds.Name = name
			return ds, nil
		}
		if nxt := p.toks[p.i+1]; nxt.kind == tkIdent && nxt.text == "database" {
			p.i += 2 // DROP DATABASE
			dd := &DropDatabase{}
			if p.consumeKeyword("IF") {
				if err := p.expectKeyword("EXISTS"); err != nil {
					return nil, err
				}
				dd.IfExists = true
			}
			name, err := p.expectIdent()
			if err != nil {
				return nil, err
			}
			dd.Name = name
			if p.consumeIdentWord("cascade") {
				dd.Cascade = true
			} else {
				p.consumeIdentWord("restrict")
			}
			return dd, nil
		}
		return p.parseDropTable()
	case "INSERT":
		return p.parseInsert(false)
	case "upsert": // not a reserved word; lexes as an identifier
		return p.parseInsert(true)
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
		if nxt := p.toks[p.i+1]; nxt.kind == tkIdent && nxt.text == "sequence" {
			p.i += 2 // ALTER SEQUENCE
			as := &AlterSequence{}
			name, err := p.parseTableName()
			if err != nil {
				return nil, err
			}
			as.Name = name
			if err := p.parseSequenceOptions(&as.Options, true); err != nil {
				return nil, err
			}
			return as, nil
		}
		if nxt := p.toks[p.i+1]; nxt.kind == tkIdent && nxt.text == "database" {
			p.i += 2 // ALTER DATABASE name RENAME TO new
			name, err := p.expectIdent()
			if err != nil {
				return nil, err
			}
			if !p.consumeIdentWord("rename") {
				return nil, p.errf("ALTER DATABASE supports only RENAME TO")
			}
			if err := p.expectKeyword("TO"); err != nil {
				return nil, err
			}
			newName, err := p.expectIdent()
			if err != nil {
				return nil, err
			}
			return &AlterDatabase{Name: name, NewName: newName}, nil
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
			name, err := p.parseTableName()
			if err != nil {
				return nil, err
			}
			a.Table = name
		}
		return a, nil
	case "SHOW":
		p.i++
		if p.consumeKeyword("TABLES") {
			st := &ShowTables{}
			if p.consumeKeyword("FROM") {
				db, err := p.expectIdent()
				if err != nil {
					return nil, err
				}
				st.Database = db
			}
			return st, nil
		}
		if p.consumeIdentWord("databases") {
			return &ShowDatabases{}, nil
		}
		if p.consumeIdentWord("sequences") {
			return &ShowSequences{}, nil
		}
		if p.consumeIdentWord("functions") {
			return &ShowFunctions{}, nil
		}
		if p.consumeIdentWord("columns") || p.consumeIdentWord("indexes") || p.consumeIdentWord("index") {
			kind := "columns"
			if p.toks[p.i-1].text != "columns" {
				kind = "indexes"
			}
			if err := p.expectKeyword("FROM"); err != nil {
				return nil, err
			}
			name, err := p.parseTableName()
			if err != nil {
				return nil, err
			}
			return &Show{Kind: kind, Table: name}, nil
		}
		if p.consumeKeyword("CREATE") {
			p.consumeKeyword("TABLE")
			name, err := p.parseTableName()
			if err != nil {
				return nil, err
			}
			return &Show{Kind: "create", Table: name}, nil
		}
		if p.consumeIdentWord("users") || p.consumeIdentWord("roles") {
			return &Show{Kind: "users"}, nil
		}
		if p.consumeIdentWord("grants") {
			sh := &Show{Kind: "grants"}
			if p.consumeKeyword("ON") {
				p.consumeKeyword("TABLE")
				name, err := p.parseTableName()
				if err != nil {
					return nil, err
				}
				sh.Table = name
			}
			if p.consumeIdentWord("for") {
				user, err := p.expectIdent()
				if err != nil {
					return nil, err
				}
				sh.User = user
			}
			return sh, nil
		}
		if p.consumeIdentWord("all") {
			return &Show{Kind: "all"}, nil
		}
		// SHOW STATS FOR <table>: read-only statistics view.
		if p.consumeIdentWord("stats") {
			if !p.consumeIdentWord("for") {
				return nil, p.errf("expected FOR in SHOW STATS FOR <table>")
			}
			name, err := p.parseTableName()
			if err != nil {
				return nil, err
			}
			return &ShowStats{Table: name}, nil
		}
		// SHOW <variable>: multi-word names join with "_" (SHOW TIME ZONE,
		// SHOW TRANSACTION ISOLATION LEVEL); the session resolves them.
		var words []string
		for !p.atStatementEnd() {
			t := p.peek()
			if t.kind != tkIdent && t.kind != tkKeyword {
				return nil, p.errf("expected a variable name after SHOW, found %q", t.text)
			}
			words = append(words, strings.ToLower(t.text))
			p.i++
		}
		if len(words) == 0 {
			return nil, p.errf("expected a variable name after SHOW")
		}
		return &SetVar{Name: "show:" + strings.Join(words, "_")}, nil
	case "SET":
		// SET [SESSION|LOCAL] name = value / SET ... TO ...: the value is
		// kept for the variables the session honors (database), the rest
		// are accepted and ignored.
		p.i++
		p.consumeKeyword("SESSION")
		p.consumeKeyword("LOCAL")
		name, err := p.expectIdent()
		if err != nil {
			return nil, err
		}
		sv := &SetVar{Name: name}
		if p.consumeOp("=") || p.consumeKeyword("TO") {
			if t := p.peek(); t.kind == tkIdent || t.kind == tkString || t.kind == tkNumber {
				sv.Value = t.text
			}
		}
		for !p.atStatementEnd() {
			p.i++
		}
		return sv, nil
	case "use": // not a reserved word; lexes as an identifier
		p.i++
		name, err := p.expectIdent()
		if err != nil {
			return nil, err
		}
		return &Use{Name: name}, nil
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
	name, err := p.parseTableName()
	if err != nil {
		return nil, err
	}
	ct.Name = name
	if err := p.expectOp("("); err != nil {
		return nil, err
	}
	for {
		cname := ""
		if p.consumeKeyword("CONSTRAINT") {
			n, err := p.expectIdent()
			if err != nil {
				return nil, err
			}
			cname = n
		}
		switch {
		case p.consumeKeyword("PRIMARY"):
			if len(ct.PrimaryKey) > 0 {
				return nil, p.errf("multiple primary key definitions")
			}
			if err := p.expectKeyword("KEY"); err != nil {
				return nil, err
			}
			cols, err := p.parseColumnList()
			if err != nil {
				return nil, err
			}
			ct.PrimaryKey, ct.PrimaryKeyName = cols, cname
		case p.peekKeyword("UNIQUE") || p.peekKeyword("CHECK") || p.peekKeyword("FOREIGN"):
			cd, err := p.parseTableConstraint(cname)
			if err != nil {
				return nil, err
			}
			ct.Constraints = append(ct.Constraints, cd)
		default:
			if cname != "" {
				return nil, p.errf("expected PRIMARY KEY, UNIQUE, CHECK or FOREIGN KEY after CONSTRAINT %s, found %q", cname, p.peek().text)
			}
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
	switch strings.ToUpper(typeName) {
	case "SERIAL", "BIGSERIAL", "SMALLSERIAL", "SERIAL8", "SERIAL4", "SERIAL2":
		// An owned sequence with DEFAULT nextval(...); NOT NULL implied.
		typeName = "INT8"
		def.Serial, def.NotNull = true, true
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
		// A column constraint may carry its own name.
		cname := ""
		if p.consumeKeyword("CONSTRAINT") {
			n, err := p.expectIdent()
			if err != nil {
				return def, err
			}
			cname = n
		}
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
		case p.consumeKeyword("UNIQUE"):
			def.Constraints = append(def.Constraints, ConstraintDef{Name: cname, Kind: "unique", Columns: []string{def.Name}})
		case p.peekKeyword("CHECK"):
			cd, err := p.parseCheckClause(cname)
			if err != nil {
				return def, err
			}
			def.Constraints = append(def.Constraints, cd)
		case p.consumeKeyword("REFERENCES"):
			cd := ConstraintDef{Name: cname, Kind: "foreign", Columns: []string{def.Name}}
			if err := p.parseReferences(&cd); err != nil {
				return def, err
			}
			def.Constraints = append(def.Constraints, cd)
		case p.consumeIdentWord("default"):
			// A constant stays a literal default; anything else (a
			// call, arithmetic) is an expression default ("default" is
			// not a reserved word).
			e, err := p.parseValueOrColumnExpr()
			if err != nil {
				return def, err
			}
			if e.Lit != nil && e.BinOp == "" && e.Cast == "" {
				def.Default = e.Lit
			} else {
				if e.Column != "" || exprContainsColumn(e) {
					return def, p.errf("DEFAULT cannot reference columns")
				}
				def.DefaultExpr = &e
			}
		case p.peekIdentWord("generated"):
			// GENERATED { ALWAYS | BY DEFAULT } AS IDENTITY [(options)]
			p.i++
			switch {
			case p.consumeIdentWord("always"):
				def.Identity = "always"
			case p.consumeKeyword("BY"):
				if !p.consumeIdentWord("default") {
					return def, p.errf("expected DEFAULT after GENERATED BY, found %q", p.peek().text)
				}
				def.Identity = "by default"
			default:
				return def, p.errf("expected ALWAYS or BY DEFAULT after GENERATED, found %q", p.peek().text)
			}
			if err := p.expectKeyword("AS"); err != nil {
				return def, err
			}
			if !p.consumeIdentWord("identity") {
				return def, p.errf("expected IDENTITY, found %q", p.peek().text)
			}
			def.NotNull = true
			if p.consumeOp("(") {
				def.IdentitySeq = &SequenceOptions{}
				if err := p.parseSequenceOptions(def.IdentitySeq, false); err != nil {
					return def, err
				}
				if err := p.expectOp(")"); err != nil {
					return def, err
				}
			}
		default:
			if cname != "" {
				return def, p.errf("expected a constraint after CONSTRAINT %s, found %q", cname, p.peek().text)
			}
			return def, nil
		}
	}
}

func (p *parser) peekKeyword(kw string) bool {
	t := p.peek()
	return t.kind == tkKeyword && t.text == kw
}

// parseColumnList parses a parenthesized, comma-separated column list.
func (p *parser) parseColumnList() ([]string, error) {
	if err := p.expectOp("("); err != nil {
		return nil, err
	}
	var cols []string
	for {
		col, err := p.expectIdent()
		if err != nil {
			return nil, err
		}
		cols = append(cols, col)
		if !p.consumeOp(",") {
			break
		}
	}
	if err := p.expectOp(")"); err != nil {
		return nil, err
	}
	return cols, nil
}

// parseTableConstraint parses UNIQUE (cols) | CHECK (expr) | FOREIGN KEY
// (cols) REFERENCES ... after an optional CONSTRAINT name.
func (p *parser) parseTableConstraint(name string) (ConstraintDef, error) {
	switch {
	case p.consumeKeyword("UNIQUE"):
		cols, err := p.parseColumnList()
		if err != nil {
			return ConstraintDef{}, err
		}
		return ConstraintDef{Name: name, Kind: "unique", Columns: cols}, nil
	case p.peekKeyword("CHECK"):
		return p.parseCheckClause(name)
	case p.consumeKeyword("FOREIGN"):
		if err := p.expectKeyword("KEY"); err != nil {
			return ConstraintDef{}, err
		}
		cols, err := p.parseColumnList()
		if err != nil {
			return ConstraintDef{}, err
		}
		cd := ConstraintDef{Name: name, Kind: "foreign", Columns: cols}
		if err := p.expectKeyword("REFERENCES"); err != nil {
			return ConstraintDef{}, err
		}
		if err := p.parseReferences(&cd); err != nil {
			return ConstraintDef{}, err
		}
		return cd, nil
	}
	return ConstraintDef{}, p.errf("expected UNIQUE, CHECK or FOREIGN KEY, found %q", p.peek().text)
}

// parseCheckClause parses CHECK (expr), keeping the expression's source
// text (what the descriptor stores and the catalogs show) and lowering
// its negation for validation.
func (p *parser) parseCheckClause(name string) (ConstraintDef, error) {
	if err := p.expectKeyword("CHECK"); err != nil {
		return ConstraintDef{}, err
	}
	if err := p.expectOp("("); err != nil {
		return ConstraintDef{}, err
	}
	start := p.peek().pos
	n, err := p.parseBoolOr()
	if err != nil {
		return ConstraintDef{}, err
	}
	end := p.peek().pos
	if err := p.expectOp(")"); err != nil {
		return ConstraintDef{}, err
	}
	fails, err := lowerBool(n, true)
	if err != nil {
		return ConstraintDef{}, p.errf("CHECK: %v", err)
	}
	text := strings.TrimSpace(p.src[start:end])
	return ConstraintDef{Name: name, Kind: "check", Check: text, CheckFails: fails}, nil
}

// parseReferences parses what follows REFERENCES: the table, optional
// column list, MATCH SIMPLE, and the ON DELETE / ON UPDATE actions.
func (p *parser) parseReferences(cd *ConstraintDef) error {
	table, err := p.parseTableName()
	if err != nil {
		return err
	}
	cd.RefTable = table
	if p.peek().kind == tkOp && p.peek().text == "(" {
		cols, err := p.parseColumnList()
		if err != nil {
			return err
		}
		cd.RefColumns = cols
	}
	for {
		switch {
		case p.consumeIdentWord("match"):
			if p.consumeIdentWord("simple") {
				continue
			}
			return p.errf("only MATCH SIMPLE is supported, found %q", p.peek().text)
		case p.consumeKeyword("ON"):
			var target *string
			switch {
			case p.consumeKeyword("DELETE"):
				target = &cd.OnDelete
			case p.consumeKeyword("UPDATE"):
				target = &cd.OnUpdate
			default:
				return p.errf("expected DELETE or UPDATE after ON, found %q", p.peek().text)
			}
			switch {
			case p.consumeIdentWord("restrict"):
				*target = "restrict"
			case p.consumeIdentWord("cascade"):
				*target = "cascade"
			case p.consumeIdentWord("no"):
				if !p.consumeIdentWord("action") {
					return p.errf("expected ACTION after NO, found %q", p.peek().text)
				}
				*target = "restrict"
			case p.consumeKeyword("SET"):
				if !p.consumeKeyword("NULL") {
					return p.errf("only ON %s SET NULL is supported (no SET DEFAULT), found %q", "DELETE/UPDATE", p.peek().text)
				}
				*target = "set null"
			default:
				return p.errf("expected RESTRICT, CASCADE, NO ACTION or SET NULL, found %q", p.peek().text)
			}
		default:
			return nil
		}
	}
}

// ParseCheck lowers a stored CHECK expression to the conjuncts that
// hold when a row VIOLATES it (NOT expr is true): the three-valued rule
// that a NULL result passes falls out of WHERE keeping only TRUE.
func ParseCheck(text string) ([]Comparison, error) {
	toks, err := lex(text)
	if err != nil {
		return nil, err
	}
	p := &parser{toks: toks, src: text}
	n, err := p.parseBoolOr()
	if err != nil {
		return nil, err
	}
	if p.peek().kind != tkEOF {
		return nil, p.errf("unexpected %q after expression", p.peek().text)
	}
	return lowerBool(n, true)
}

// exprContainsColumn reports whether an expression references a column.
func exprContainsColumn(e Expr) bool {
	if e.Column != "" {
		return true
	}
	if e.Left != nil && exprContainsColumn(*e.Left) || e.Right != nil && exprContainsColumn(*e.Right) {
		return true
	}
	for _, a := range e.Args {
		if exprContainsColumn(a) {
			return true
		}
	}
	return false
}

// parseCreateSequence parses CREATE SEQUENCE [IF NOT EXISTS] name [options].
func (p *parser) parseCreateSequence() (Statement, error) {
	p.i += 2 // CREATE SEQUENCE
	cs := &CreateSequence{}
	if p.consumeKeyword("IF") {
		if err := p.expectKeyword("NOT"); err != nil {
			return nil, err
		}
		if err := p.expectKeyword("EXISTS"); err != nil {
			return nil, err
		}
		cs.IfNotExists = true
	}
	name, err := p.parseTableName()
	if err != nil {
		return nil, err
	}
	cs.Name = name
	if err := p.parseSequenceOptions(&cs.Options, false); err != nil {
		return nil, err
	}
	return cs, nil
}

// parseSequenceOptions parses the option words of CREATE / ALTER SEQUENCE
// and identity columns, in any order, until something else appears.
func (p *parser) parseSequenceOptions(o *SequenceOptions, alter bool) error {
	num := func() (int64, error) {
		neg := p.consumeOp("-")
		t := p.peek()
		if t.kind != tkNumber {
			return 0, p.errf("expected a number, found %q", t.text)
		}
		p.i++
		v, err := strconv.ParseInt(t.text, 10, 64)
		if err != nil {
			return 0, p.errf("invalid number %q", t.text)
		}
		if neg {
			v = -v
		}
		return v, nil
	}
	for {
		switch {
		case p.consumeKeyword("AS"):
			// AS int2 | int4 | int8: the type is accepted and ignored
			// (every sequence is 64-bit).
			if t := p.peek(); t.kind == tkIdent || t.kind == tkKeyword {
				p.i++
			}
		case p.consumeIdentWord("increment"):
			p.consumeKeyword("BY")
			v, err := num()
			if err != nil {
				return err
			}
			o.Increment = &v
		case p.consumeIdentWord("minvalue"):
			v, err := num()
			if err != nil {
				return err
			}
			o.MinValue = &v
		case p.consumeIdentWord("maxvalue"):
			v, err := num()
			if err != nil {
				return err
			}
			o.MaxValue = &v
		case p.consumeKeyword("START") || p.consumeIdentWord("start"):
			p.consumeIdentWord("with")
			v, err := num()
			if err != nil {
				return err
			}
			o.Start = &v
		case p.consumeIdentWord("cache"):
			v, err := num()
			if err != nil {
				return err
			}
			o.Cache = &v
		case p.consumeIdentWord("cycle"):
			t := true
			o.Cycle = &t
		case p.peekIdentWord("no"):
			p.i++
			switch {
			case p.consumeIdentWord("cycle"):
				f := false
				o.Cycle = &f
			case p.consumeIdentWord("minvalue"):
				o.NoMin = true
			case p.consumeIdentWord("maxvalue"):
				o.NoMax = true
			default:
				return p.errf("expected CYCLE, MINVALUE or MAXVALUE after NO, found %q", p.peek().text)
			}
		case p.consumeIdentWord("owned"):
			if err := p.expectKeyword("BY"); err != nil {
				return err
			}
			if p.consumeIdentWord("none") {
				o.OwnedBy = "none"
				continue
			}
			ref, err := p.parseColumnRef()
			if err != nil {
				return err
			}
			if ref.Table == "" {
				return p.errf("OWNED BY takes table.column")
			}
			o.OwnedBy = ref.String()
		case alter && p.consumeIdentWord("restart"):
			o.RestartSet = true
			if p.consumeIdentWord("with") || p.peek().kind == tkNumber || (p.peek().kind == tkOp && p.peek().text == "-") {
				v, err := num()
				if err != nil {
					return err
				}
				o.Restart = &v
			}
		default:
			return nil
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
	table, err := p.parseTableName()
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
	name, err := p.parseTableName()
	if err != nil {
		return nil, err
	}
	dt.Name = name
	if p.consumeIdentWord("cascade") {
		dt.Cascade = true
	} else {
		p.consumeIdentWord("restrict")
	}
	return dt, nil
}

func (p *parser) parseInsert(upsert bool) (Statement, error) {
	p.i++ // INSERT / UPSERT
	if err := p.expectKeyword("INTO"); err != nil {
		return nil, err
	}
	ins := &Insert{Upsert: upsert}
	name, err := p.parseTableName()
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
	if p.consumeIdentWord("overriding") {
		switch {
		case p.consumeIdentWord("system"):
			ins.Overriding = "system"
		case p.consumeKeyword("USER"):
			ins.Overriding = "user"
		default:
			return nil, p.errf("expected SYSTEM or USER after OVERRIDING, found %q", p.peek().text)
		}
		if !p.consumeIdentWord("value") {
			return nil, p.errf("expected VALUE after OVERRIDING %s", strings.ToUpper(ins.Overriding))
		}
	}
	if p.peekIdentWord("default") && p.i+1 < len(p.toks) && p.toks[p.i+1].kind == tkKeyword && p.toks[p.i+1].text == "VALUES" {
		p.i += 2
		ins.DefaultValues = true
		if len(ins.Columns) > 0 {
			return nil, p.errf("DEFAULT VALUES takes no column list")
		}
	}
	if t := p.peek(); !ins.DefaultValues && ((t.kind == tkKeyword && t.text == "SELECT") || (t.kind == tkOp && t.text == "(") || (t.kind == tkIdent && t.text == "with")) {
		// INSERT INTO t [(cols)] SELECT ... | (query) | WITH ... SELECT ...
		src, err := p.parseSetMember()
		if err != nil {
			return nil, err
		}
		ins.Select = src
	} else if err := p.expectKeywordUnless("VALUES", ins.DefaultValues); err != nil {
		return nil, err
	}
	for !ins.DefaultValues && ins.Select == nil {
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
	// ON CONFLICT ... ("conflict", "do", "nothing" are not
	// reserved words).
	if p.peek().kind == tkKeyword && p.peek().text == "ON" && p.i+1 < len(p.toks) && p.toks[p.i+1].kind == tkIdent && p.toks[p.i+1].text == "conflict" {
		if upsert {
			return nil, p.errf("UPSERT does not take ON CONFLICT")
		}
		p.i += 2
		oc := &OnConflict{}
		if p.consumeOp("(") {
			for {
				col, err := p.expectIdent()
				if err != nil {
					return nil, err
				}
				oc.Columns = append(oc.Columns, col)
				if !p.consumeOp(",") {
					break
				}
			}
			if err := p.expectOp(")"); err != nil {
				return nil, err
			}
		} else if p.consumeKeyword("ON") {
			if !p.consumeKeyword("CONSTRAINT") {
				return nil, p.errf("expected CONSTRAINT after ON CONFLICT ON, found %q", p.peek().text)
			}
			name, err := p.expectIdent()
			if err != nil {
				return nil, err
			}
			oc.Constraint = name
		}
		if !p.consumeIdentWord("do") {
			return nil, p.errf("expected DO NOTHING or DO UPDATE after ON CONFLICT, found %q", p.peek().text)
		}
		switch {
		case p.consumeIdentWord("nothing"):
			oc.DoNothing = true
		case p.consumeKeyword("UPDATE"):
			if oc.Columns == nil && oc.Constraint == "" {
				return nil, p.errf("ON CONFLICT DO UPDATE requires a conflict target: ON CONFLICT (columns) or ON CONSTRAINT name")
			}
			set, err := p.parseSetClauses()
			if err != nil {
				return nil, err
			}
			oc.Set = set
			if oc.Where, err = p.parseOptWhere(); err != nil {
				return nil, err
			}
		default:
			return nil, p.errf("expected NOTHING or UPDATE after DO, found %q", p.peek().text)
		}
		ins.OnConflict = oc
	}
	var err2 error
	if ins.Returning, err2 = p.parseOptReturning(); err2 != nil {
		return nil, err2
	}
	return ins, nil
}

// expectKeywordUnless expects kw unless skip is set.
func (p *parser) expectKeywordUnless(kw string, skip bool) error {
	if skip {
		return nil
	}
	return p.expectKeyword(kw)
}

// parseSetClauses parses "col = expr, ..." after SET.
func (p *parser) parseSetClauses() ([]SetClause, error) {
	if err := p.expectKeyword("SET"); err != nil {
		return nil, err
	}
	var out []SetClause
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
		out = append(out, SetClause{Column: col, Value: e})
		if !p.consumeOp(",") {
			break
		}
	}
	return out, nil
}

// parseOptReturning parses RETURNING * | expr [AS alias], ... ("returning"
// is not a reserved word). nil means no clause.
func (p *parser) parseOptReturning() ([]SelectExpr, error) {
	if !p.consumeIdentWord("returning") {
		return nil, nil
	}
	var out []SelectExpr
	for {
		if p.consumeOp("*") {
			out = append(out, SelectExpr{Star: true})
		} else {
			e, err := p.parseValueOrBool()
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
			} else if t := p.peek(); t.kind == tkIdent {
				p.i++
				se.Alias = t.text
			}
			out = append(out, se)
		}
		if !p.consumeOp(",") {
			break
		}
	}
	return out, nil
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
	name, err := p.parseTableName()
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
			e, err := p.parseValueOrBool()
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
		derived := false
		if sub, ok, err := p.parseSubquery(); err != nil {
			return nil, err
		} else if ok {
			sel.Derived = sub
			sel.Alias = p.parseOptTableAlias(false)
			if sel.Alias == "" {
				return nil, p.errf("subquery in FROM must have an alias")
			}
			derived = true
		}
		// FROM [pg_catalog.]unnest(...) [AS] alias [(column)] — a
		// set-returning function as a one-column table.
		if fe, ok, err := p.parseFuncTable(); err != nil {
			return nil, err
		} else if ok {
			sel.FuncTable = fe
			if p.peekIdentSeq("with", "ordinality") {
				p.i += 2
			}
			sel.Alias = p.parseOptTableAlias(false)
			if sel.Alias == "" {
				sel.Alias = fe.Func
			}
			sel.FuncCol = sel.Alias
			if p.consumeOp("(") {
				col, err := p.expectIdent()
				if err != nil {
					return nil, err
				}
				sel.FuncCol = col
				if err := p.expectOp(")"); err != nil {
					return nil, err
				}
			}
			return p.finishSelect(sel)
		}
		if !derived {
			name, err := p.parseTableName()
			if err != nil {
				return nil, err
			}
			sel.Table = name
			sel.Alias = p.parseOptTableAlias(true)
		}
		// JOIN / INNER JOIN / LEFT [OUTER] JOIN chains — none are reserved
		// words. Joins execute left-deep in syntactic order. A derived
		// table joins too: it runs as a named relation.
		for {
			jk, join, err := p.parseJoinKind()
			if err != nil {
				return nil, err
			}
			if !join {
				break
			}
			left, cross := jk.left, jk.cross
			if len(sel.Joins) >= maxJoinTables-1 {
				return nil, p.errf("too many joined tables (limit %d)", maxJoinTables)
			}
			if fe, ok, err := p.parseFuncTable(); err != nil {
				return nil, err
			} else if ok {
				// A table function as a join member.
				if p.peekIdentSeq("with", "ordinality") {
					p.i += 2
				}
				jc := JoinClause{Left: left, Cross: cross, FuncTable: fe}
				jc.Alias = p.parseOptTableAlias(false)
				if jc.Alias == "" {
					jc.Alias = fe.Func
				}
				if p.consumeOp("(") {
					for {
						col, err := p.expectIdent()
						if err != nil {
							return nil, err
						}
						jc.FuncCols = append(jc.FuncCols, col)
						if !p.consumeOp(",") {
							break
						}
					}
					if err := p.expectOp(")"); err != nil {
						return nil, err
					}
				}
				if !cross {
					return nil, p.errf("a table function can only be cross-joined")
				}
				sel.Joins = append(sel.Joins, jc)
				continue
			}
			jt, err := p.parseTableName()
			if err != nil {
				return nil, err
			}
			jc := JoinClause{Left: left, Cross: cross, Right: jk.right, Full: jk.full, Natural: jk.natural, Table: jt}
			jc.Alias = p.parseOptTableAlias(false)
			if cross || jk.natural {
				sel.Joins = append(sel.Joins, jc)
				continue
			}
			if p.consumeIdentWord("using") {
				if err := p.expectOp("("); err != nil {
					return nil, err
				}
				for {
					col, err := p.expectIdent()
					if err != nil {
						return nil, err
					}
					jc.Using = append(jc.Using, col)
					if !p.consumeOp(",") {
						break
					}
				}
				if err := p.expectOp(")"); err != nil {
					return nil, err
				}
				sel.Joins = append(sel.Joins, jc)
				continue
			}
			if err := p.expectKeyword("ON"); err != nil {
				return nil, err
			}
			// The ON clause is a full boolean expression (parentheses,
			// AND/OR/NOT, IN, IS NULL ...). Top-level equalities between
			// two column references are the join keys; every other
			// conjunct is a filter evaluated per candidate match.
			n, err := p.parseBoolOr()
			if err != nil {
				return nil, err
			}
			conds, err := lowerBool(n, false)
			if err != nil {
				return nil, err
			}
			for _, c := range conds {
				if l, r, ok := joinEquality(c); ok {
					jc.On = append(jc.On, JoinCond{L: l, R: r})
					continue
				}
				jc.Filter = append(jc.Filter, c)
			}
			if len(jc.On) == 0 {
				return nil, p.errf("JOIN ... ON must equate a column of %s with one from an earlier table", jt)
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
	if err := p.parseOrderLimit(sel, true); err != nil {
		return nil, err
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
	if err := p.parseSetOps(sel); err != nil {
		return nil, err
	}
	return sel, nil
}

// joinKind is what a join introducer said: LEFT/RIGHT/FULL [OUTER],
// CROSS (or a comma), NATURAL, or a plain [INNER] JOIN.
type joinKind struct {
	left, right, full, cross, natural bool
}

// parseJoinKind consumes a join introducer, reporting its kind, or
// consumes nothing (ok false) when the next tokens do not start a join.
// None of the words are reserved.
func (p *parser) parseJoinKind() (jk joinKind, ok bool, err error) {
	if p.consumeOp(",") {
		// FROM a, b: a cross join (the WHERE clause carries the join
		// predicate, if any).
		return joinKind{cross: true}, true, nil
	}
	jk.natural = p.consumeIdentWord("natural")
	after := func(what string) (joinKind, bool, error) {
		if !p.consumeIdentWord("join") {
			return jk, false, p.errf("expected JOIN after %s, found %q", what, p.peek().text)
		}
		return jk, true, nil
	}
	switch {
	case p.consumeIdentWord("cross"):
		if jk.natural {
			return jk, false, p.errf("NATURAL CROSS JOIN is not valid")
		}
		jk.cross = true
		return after("CROSS")
	case p.consumeIdentWord("join"):
		return jk, true, nil
	case p.consumeIdentWord("inner"):
		return after("INNER")
	case p.consumeIdentWord("left"):
		p.consumeIdentWord("outer")
		jk.left = true
		return after("LEFT [OUTER]")
	case p.consumeIdentWord("right"):
		p.consumeIdentWord("outer")
		jk.right = true
		return after("RIGHT [OUTER]")
	case p.consumeIdentWord("full"):
		p.consumeIdentWord("outer")
		jk.full = true
		return after("FULL [OUTER]")
	}
	if jk.natural {
		return jk, false, p.errf("expected JOIN after NATURAL, found %q", p.peek().text)
	}
	return jk, false, nil
}

// parseOrderLimit parses the optional ORDER BY, LIMIT, OFFSET and FETCH
// clauses onto sel. With exprsKnown, a positional ORDER BY is checked
// against and rewritten to the select list's output names; otherwise
// (a parenthesized query) the position is kept for the executor.
func (p *parser) parseOrderLimit(sel *Select, exprsKnown bool) error {
	if p.consumeKeyword("ORDER") {
		if err := p.expectKeyword("BY"); err != nil {
			return err
		}
		for {
			var oc OrderCol
			if t := p.peek(); t.kind == tkNumber {
				n, err := strconv.Atoi(t.text)
				if err != nil || n < 1 || (exprsKnown && n > len(sel.Exprs)) {
					return p.errf("ORDER BY position %s is not in the select list", t.text)
				}
				p.i++
				if exprsKnown {
					oc = OrderCol{Column: outputName(sel.Exprs[n-1])}
				} else {
					oc = OrderCol{Position: n}
				}
			} else if se, ok, err := p.parseAggExpr(); err != nil {
				return err
			} else if ok {
				oc = OrderCol{Agg: &se}
			} else {
				// A bare (possibly qualified) name sorts by that column or
				// output name; anything else is a computed sort key.
				e, err := p.parseValueOrBool()
				if err != nil {
					return err
				}
				if isPlainColumn(e) {
					oc = OrderCol{Column: e.Column}
				} else {
					oc = OrderCol{Expr: &e}
				}
			}
			if p.consumeKeyword("DESC") {
				oc.Desc = true
			} else {
				p.consumeKeyword("ASC")
			}
			if p.consumeIdentWord("nulls") {
				switch {
				case p.consumeIdentWord("first"):
					oc.Nulls = "first"
				case p.consumeIdentWord("last"):
					oc.Nulls = "last"
				default:
					return p.errf("expected FIRST or LAST after NULLS, found %q", p.peek().text)
				}
			}
			sel.OrderBy = append(sel.OrderBy, oc)
			if !p.consumeOp(",") {
				break
			}
		}
	}
	// LIMIT n | LIMIT ALL, OFFSET n [ROW | ROWS], FETCH {FIRST | NEXT} [n]
	// {ROW | ROWS} ONLY — in either order, each at most once.
	seenLimit, seenOffset := false, false
	for {
		switch {
		case p.consumeKeyword("LIMIT"):
			if seenLimit {
				return p.errf("multiple LIMIT clauses")
			}
			seenLimit = true
			if p.consumeIdentWord("all") {
				sel.Limit = -1
				continue
			}
			n, param, err := p.parseCount("LIMIT")
			if err != nil {
				return err
			}
			sel.Limit, sel.LimitParam = n, param
		case p.consumeIdentWord("offset"):
			if seenOffset {
				return p.errf("multiple OFFSET clauses")
			}
			seenOffset = true
			n, param, err := p.parseCount("OFFSET")
			if err != nil {
				return err
			}
			sel.Offset, sel.OffsetParam = n, param
			if !p.consumeIdentWord("rows") {
				p.consumeIdentWord("row")
			}
		case p.consumeIdentWord("fetch"):
			if seenLimit {
				return p.errf("multiple LIMIT clauses")
			}
			seenLimit = true
			if !p.consumeIdentWord("first") && !p.consumeIdentWord("next") {
				return p.errf("expected FIRST or NEXT after FETCH, found %q", p.peek().text)
			}
			n, param := int64(1), 0
			if t := p.peek(); t.kind == tkNumber || t.kind == tkParam {
				var err error
				if n, param, err = p.parseCount("FETCH"); err != nil {
					return err
				}
			}
			if !p.consumeIdentWord("rows") && !p.consumeIdentWord("row") {
				return p.errf("expected ROW or ROWS in FETCH FIRST, found %q", p.peek().text)
			}
			if !p.consumeIdentWord("only") {
				return p.errf("expected ONLY in FETCH FIRST, found %q", p.peek().text)
			}
			sel.Limit, sel.LimitParam = n, param
		default:
			return nil
		}
	}
}

// parseCount parses the count of a LIMIT / OFFSET / FETCH clause: a
// non-negative integer, or a parameter ($n) resolved at execution (the
// count is then -1 and param names it).
func (p *parser) parseCount(clause string) (n int64, param int, err error) {
	t := p.peek()
	if t.kind == tkParam {
		p.i++
		idx, err := strconv.Atoi(t.text)
		if err != nil || idx < 1 {
			return 0, 0, p.errf("invalid parameter $%s", t.text)
		}
		return -1, idx, nil
	}
	if t.kind != tkNumber {
		return 0, 0, p.errf("expected %s count, found %q", clause, t.text)
	}
	v, err := strconv.ParseInt(t.text, 10, 64)
	if err != nil || v < 0 {
		return 0, 0, p.errf("invalid %s %q", clause, t.text)
	}
	p.i++
	return v, 0, nil
}

// parseSetOps parses a [UNION | INTERSECT | EXCEPT] [ALL | DISTINCT]
// member after sel, chaining it onto the END of sel's member list (none
// of the words are reserved). The member parses as a full query, so its
// own tail — further members, and the ORDER BY / LIMIT / OFFSET written
// after the last member — comes back with it; the members flatten into
// sel's list and the ordering clauses move to the head, which applies
// them to the whole result. Precedence (INTERSECT first, then left to
// right) is the executor's, over the flat list.
func (p *parser) parseSetOps(sel *Select) error {
	for {
		var op string
		switch {
		case p.consumeIdentWord("union"):
			op = "UNION"
		case p.consumeIdentWord("intersect"):
			op = "INTERSECT"
		case p.consumeIdentWord("except"):
			op = "EXCEPT"
		default:
			return nil
		}
		all := p.consumeIdentWord("all")
		if !all {
			p.consumeIdentWord("distinct")
		}
		if sel.ForUpdate {
			return p.errf("FOR UPDATE is not allowed with %s", op)
		}
		if len(sel.OrderBy) > 0 || sel.Limit >= 0 || sel.Offset > 0 || sel.LimitParam > 0 || sel.OffsetParam > 0 {
			return p.errf("ORDER BY, LIMIT and OFFSET must follow the last %s member", op)
		}
		tail, err := p.parseSetMember()
		if err != nil {
			return err
		}
		sel.OrderBy, tail.OrderBy = tail.OrderBy, nil
		sel.Limit, tail.Limit = tail.Limit, -1
		sel.Offset, tail.Offset = tail.Offset, 0
		sel.LimitParam, tail.LimitParam = tail.LimitParam, 0
		sel.OffsetParam, tail.OffsetParam = tail.OffsetParam, 0
		last := sel
		for last.Union != nil {
			last = last.Union
		}
		last.Union, last.UnionAll, last.SetOp = tail, all, op
		// The tail's own list continues from it; nothing more to parse
		// here unless it stopped at a parenthesized member's boundary.
	}
}

// parseSetMember parses one member of a set operation: SELECT ...,
// VALUES ..., or a parenthesized query.
func (p *parser) parseSetMember() (*Select, error) {
	t := p.peek()
	switch {
	case t.kind == tkOp && t.text == "(":
		return p.parseParenMember()
	case t.kind == tkKeyword && t.text == "SELECT":
		stmt, err := p.parseSelect()
		if err != nil {
			return nil, err
		}
		return stmt.(*Select), nil
	case t.kind == tkIdent && t.text == "with":
		stmt, err := p.parseWith()
		if err != nil {
			return nil, err
		}
		sel, ok := stmt.(*Select)
		if !ok {
			return nil, p.errf("a WITH used as a query must end in a SELECT")
		}
		return sel, nil
	case t.kind == tkKeyword && t.text == "VALUES":
		stmt, err := p.parseValuesQuery()
		if err != nil {
			return nil, err
		}
		head := stmt.(*Select)
		if err := p.parseOrderLimit(head, true); err != nil {
			return nil, err
		}
		if err := p.parseSetOps(head); err != nil {
			return nil, err
		}
		return head, nil
	}
	return nil, p.errf("expected SELECT, VALUES or a parenthesized query, found %q", t.text)
}

// parseParenMember parses (query) [ORDER BY ...] [LIMIT ...] [set ops]:
// the parenthesized query — with any ordering clauses of its own —
// becomes a derived table selected whole, so it can carry its own ORDER
// BY / LIMIT inside a set operation, and the clauses after the closing
// parenthesis apply to the result.
func (p *parser) parseParenMember() (*Select, error) {
	if err := p.expectOp("("); err != nil {
		return nil, err
	}
	inner, err := p.parseSetMember()
	if err != nil {
		return nil, err
	}
	if err := p.expectOp(")"); err != nil {
		return nil, err
	}
	sel := &Select{Limit: -1, Exprs: []SelectExpr{{Star: true}}, Derived: inner, Alias: "_q"}
	if err := p.parseOrderLimit(sel, false); err != nil {
		return nil, err
	}
	if err := p.parseSetOps(sel); err != nil {
		return nil, err
	}
	return sel, nil
}

// parseValuesQuery parses VALUES (a, b), (c, d) as a query: one FROM-less
// select per row, chained with UNION ALL.
func (p *parser) parseValuesQuery() (Statement, error) {
	p.i++ // VALUES
	var head, last *Select
	for {
		if err := p.expectOp("("); err != nil {
			return nil, err
		}
		row := &Select{Limit: -1}
		for {
			e, err := p.parseValueOrColumnExpr()
			if err != nil {
				return nil, err
			}
			row.Exprs = append(row.Exprs, SelectExpr{Expr: e, Alias: fmt.Sprintf("column%d", len(row.Exprs)+1)})
			if !p.consumeOp(",") {
				break
			}
		}
		if err := p.expectOp(")"); err != nil {
			return nil, err
		}
		if head == nil {
			head = row
		} else {
			last.Union, last.UnionAll = row, true
		}
		last = row
		if !p.consumeOp(",") {
			break
		}
	}
	return head, nil
}

// caseWords are the CASE-expression words that can follow a predicate.
var caseWords = map[string]bool{"then": true, "else": true, "end": true, "when": true}

// tableClauseWords are non-reserved identifier words that begin a clause
// after a table name — never a bare table alias.
var tableClauseWords = map[string]bool{
	"join": true, "inner": true, "left": true, "right": true, "full": true, "cross": true, "natural": true, "using": true,
	"group": true, "having": true, "for": true, "union": true, "intersect": true, "except": true, "offset": true, "fetch": true,
	"window": true,
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
		if op, ok := p.parseCmpOp(); ok {
			val, err := p.parseValueOrColumnExpr()
			if err != nil {
				return nil, err
			}
			hc.Op, hc.Value = op, val
		} else {
			// A bare boolean aggregate or column: HAVING bool_or(x).
			tr := types.NewBool(true)
			hc.Op, hc.Value = "=", Expr{Lit: &tr}
		}
		out = append(out, hc)
		if !p.consumeKeyword("AND") {
			break
		}
	}
	return out, nil
}

var aggNames = map[string]bool{
	"count": true, "sum": true, "avg": true, "min": true, "max": true,
	"string_agg": true, "array_agg": true, "bool_and": true, "bool_or": true, "every": true,
	"stddev": true, "stddev_samp": true, "stddev_pop": true, "variance": true, "var_samp": true, "var_pop": true,
	"percentile_cont": true, "percentile_disc": true,
	"json_agg": true, "jsonb_agg": true, "json_object_agg": true, "jsonb_object_agg": true,
}

// parseAggExpr parses an aggregate call when the next tokens form one:
// name([DISTINCT] * | expr [, expr ...]) [WITHIN GROUP (ORDER BY expr
// [ASC | DESC])] [FILTER (WHERE cond)]; ok=false otherwise.
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
	if p.consumeIdentWord("distinct") {
		se.AggDistinct = true
	}
	if p.consumeOp("*") {
		if se.Agg != "COUNT" {
			return se, false, p.errf("%s(*) is not supported", se.Agg)
		}
		se.AggStar = true
	} else {
		// A plain column keeps the column form (SUM(o.total) over a
		// join); anything else — a predicate included (bool_and(x > 1))
		// — is an expression evaluated per row.
		e, err := p.parseValueOrBool()
		if err != nil {
			return se, false, err
		}
		if isPlainColumn(e) && e.Cast == "" {
			se.AggCol = e.Column
		} else {
			se.AggArg = &e
		}
		for p.consumeOp(",") {
			a, err := p.parseAddExpr()
			if err != nil {
				return se, false, err
			}
			se.AggArgs = append(se.AggArgs, a)
		}
	}
	if err := p.expectOp(")"); err != nil {
		return se, false, err
	}
	if p.peekIdentSeq("within", "group") {
		p.i += 2
		if err := p.expectOp("("); err != nil {
			return se, false, err
		}
		if err := p.expectKeyword("ORDER"); err != nil {
			return se, false, err
		}
		if err := p.expectKeyword("BY"); err != nil {
			return se, false, err
		}
		e, err := p.parseAddExpr()
		if err != nil {
			return se, false, err
		}
		oc := OrderCol{Expr: &e}
		if isPlainColumn(e) {
			oc.Column, oc.Expr = e.Column, nil
		}
		if p.consumeKeyword("DESC") {
			oc.Desc = true
		} else {
			p.consumeKeyword("ASC")
		}
		if err := p.expectOp(")"); err != nil {
			return se, false, err
		}
		se.AggOrder = []OrderCol{oc}
	}
	if p.consumeIdentWord("filter") {
		if err := p.expectOp("("); err != nil {
			return se, false, err
		}
		if err := p.expectKeyword("WHERE"); err != nil {
			return se, false, err
		}
		n, err := p.parseBoolOr()
		if err != nil {
			return se, false, err
		}
		conds, err := lowerBool(n, false)
		if err != nil {
			return se, false, err
		}
		if err := p.expectOp(")"); err != nil {
			return se, false, err
		}
		se.AggFilter = conds
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
		case t.kind == tkKeyword && t.text == "CREATE", t.kind == tkIdent && t.text == "connect":
			priv = strings.ToUpper(t.text)
			p.i++
		default:
			return nil, p.errf("expected a privilege (SELECT, INSERT, UPDATE, DELETE, CREATE, CONNECT, ALL), found %q", t.text)
		}
		gr.Privileges = append(gr.Privileges, priv)
		if !p.consumeOp(",") {
			break
		}
	}
	if err := p.expectKeyword("ON"); err != nil {
		return nil, err
	}
	if p.consumeIdentWord("database") {
		db, err := p.expectIdent()
		if err != nil {
			return nil, err
		}
		gr.Database = db
	} else {
		p.consumeKeyword("TABLE")
		table, err := p.parseTableName()
		if err != nil {
			return nil, err
		}
		gr.Table = table
	}
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
	name, err := p.parseTableName()
	if err != nil {
		return nil, err
	}
	at := &AlterTable{Table: name}
	switch {
	case p.consumeKeyword("ADD"):
		cname, named := "", false
		if p.consumeKeyword("CONSTRAINT") {
			n, err := p.expectIdent()
			if err != nil {
				return nil, err
			}
			cname, named = n, true
		}
		if named || p.peekKeyword("UNIQUE") || p.peekKeyword("CHECK") || p.peekKeyword("FOREIGN") || p.peekKeyword("PRIMARY") {
			if p.peekKeyword("PRIMARY") {
				return nil, p.errf("ADD PRIMARY KEY is not supported: the primary key is fixed at CREATE TABLE")
			}
			cd, err := p.parseTableConstraint(cname)
			if err != nil {
				return nil, err
			}
			if p.consumeKeyword("NOT") {
				if !p.consumeIdentWord("valid") {
					return nil, p.errf("expected VALID after NOT, found %q", p.peek().text)
				}
				cd.NotValid = true
			}
			at.AddConstraint = &cd
			break
		}
		p.consumeKeyword("COLUMN")
		def, err := p.parseColumnDef()
		if err != nil {
			return nil, err
		}
		at.AddCol = &def
	case p.consumeKeyword("DROP"):
		if p.consumeKeyword("CONSTRAINT") {
			if p.consumeKeyword("IF") {
				if err := p.expectKeyword("EXISTS"); err != nil {
					return nil, err
				}
				at.DropConstraintIfExists = true
			}
			n, err := p.expectIdent()
			if err != nil {
				return nil, err
			}
			at.DropConstraint = n
			break
		}
		p.consumeKeyword("COLUMN")
		col, err := p.expectIdent()
		if err != nil {
			return nil, err
		}
		at.DropCol = col
	case p.consumeIdentWord("validate"):
		if err := p.expectKeyword("CONSTRAINT"); err != nil {
			return nil, err
		}
		n, err := p.expectIdent()
		if err != nil {
			return nil, err
		}
		at.ValidateConstraint = n
	case p.consumeKeyword("ALTER"):
		p.consumeKeyword("COLUMN")
		col, err := p.expectIdent()
		if err != nil {
			return nil, err
		}
		switch {
		case p.consumeKeyword("SET"):
			if err := p.expectKeyword("NOT"); err != nil {
				return nil, err
			}
			if err := p.expectKeyword("NULL"); err != nil {
				return nil, err
			}
			at.SetNotNull = col
		case p.consumeKeyword("DROP"):
			if err := p.expectKeyword("NOT"); err != nil {
				return nil, err
			}
			if err := p.expectKeyword("NULL"); err != nil {
				return nil, err
			}
			at.DropNotNull = col
		default:
			return nil, p.errf("expected SET NOT NULL or DROP NOT NULL, found %q", p.peek().text)
		}
	case p.consumeKeyword("SET"):
		opts, err := p.parseOptionList()
		if err != nil {
			return nil, err
		}
		at.SetOptions = opts
	default:
		return nil, p.errf("expected ADD, DROP, ALTER COLUMN, VALIDATE CONSTRAINT or SET, found %q", p.peek().text)
	}
	return at, nil
}

func (p *parser) parseUpdate() (Statement, error) {
	p.i++ // UPDATE
	up := &Update{}
	name, err := p.parseTableName()
	if err != nil {
		return nil, err
	}
	up.Table = name
	if up.Set, err = p.parseSetClauses(); err != nil {
		return nil, err
	}
	up.Where, err = p.parseOptWhere()
	if err != nil {
		return nil, err
	}
	if up.Returning, err = p.parseOptReturning(); err != nil {
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
	name, err := p.parseTableName()
	if err != nil {
		return nil, err
	}
	del.Table = name
	del.Where, err = p.parseOptWhere()
	if err != nil {
		return nil, err
	}
	if del.Returning, err = p.parseOptReturning(); err != nil {
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
	// A scalar subquery used as a predicate ((SELECT c.relkind = 'c' ...))
	// or as the left side of a comparison ((SELECT count(*) ...) = 2).
	if sub, ok, err := p.parseSubquery(); err != nil {
		return boolNode{}, err
	} else if ok {
		e := Expr{Sub: sub}
		if err := p.skipCasts(); err != nil {
			return boolNode{}, err
		}
		if op, ok := p.parseCmpOp(); ok {
			cmp, err := p.finishComparison(Comparison{Expr: &e}, op)
			if err != nil {
				return boolNode{}, err
			}
			return boolNode{op: "leaf", leaf: cmp}, nil
		}
		tr := types.NewBool(true)
		return boolNode{op: "leaf", leaf: Comparison{Expr: &e, Op: "=", Value: Expr{Lit: &tr}}}, nil
	}
	// Parenthesized boolean group — unless an operator follows the
	// closing paren ((x ->> 'n')::int > 2, (a + b) * 2 = c), which makes
	// it a grouped value inside a conjunct: then re-parse as one.
	if p.peek().kind == tkOp && p.peek().text == "(" {
		save := p.i
		p.i++
		n, err := p.parseBoolOr()
		if err == nil {
			err = p.expectOp(")")
		}
		if err == nil && !p.continuesValue() {
			return n, nil
		}
		p.i = save
	}
	conds, negated, err := p.parseConjuncts()
	if err != nil {
		return boolNode{}, err
	}
	var n boolNode
	if len(conds) == 1 {
		n = boolNode{op: "leaf", leaf: conds[0]}
	} else {
		n = boolNode{op: "and"}
		for _, c := range conds {
			n.kids = append(n.kids, boolNode{op: "leaf", leaf: c})
		}
	}
	if negated {
		n = boolNode{op: "not", kids: []boolNode{n}}
	}
	return n, nil
}

// parseConjunct parses one atomic condition as a single comparison (a
// BETWEEN's two conjuncts pack as a one-disjunct OR).
func (p *parser) parseConjunct() (Comparison, error) {
	conds, negated, err := p.parseConjuncts()
	if err != nil {
		return Comparison{}, err
	}
	var cmp Comparison
	if len(conds) == 1 {
		cmp = conds[0]
	} else {
		cmp = Comparison{Op: "OR", Or: [][]Comparison{conds}}
	}
	if negated {
		return negateComparison(cmp)
	}
	return cmp, nil
}

// continuesValue reports whether the next token extends a parenthesized
// group into a larger value or predicate (a cast, an operator, a path
// step, or a predicate suffix).
func (p *parser) continuesValue() bool {
	t := p.peek()
	if t.kind == tkOp {
		switch t.text {
		case "::", "+", "-", "*", "/", "%", "^", "||", "->", "->>", "#>", "#>>":
			return true
		}
	}
	return p.startsPredicateSuffix()
}

// notWords are the words a NOT may precede inside a predicate suffix.
var notWords = map[string]bool{"in": true, "between": true, "similar": true}

// parseConjuncts parses one atomic predicate: [NOT] EXISTS (...), x IS
// [NOT] NULL | TRUE | FALSE | DISTINCT FROM y, x [NOT] IN (...), x [NOT]
// BETWEEN [SYMMETRIC] a AND b (two conjuncts; negated reports the NOT for
// the caller to apply to both), x [NOT] LIKE | ILIKE | SIMILAR TO p
// [ESCAPE e], x @> y, x op y, or a bare boolean value.
func (p *parser) parseConjuncts() ([]Comparison, bool, error) {
	if cmp, ok, err := p.parseExistsCond(); err != nil {
		return nil, false, err
	} else if ok {
		return []Comparison{cmp}, false, nil
	}
	lhs, err := p.parseAddExpr()
	if err != nil {
		return nil, false, err
	}
	// A plain column reference keeps the key-bound form; anything
	// computed (a cast included) is evaluated per row.
	computed := lhs.Column == "" || lhs.BinOp != "" || lhs.Func != "" || lhs.Left != nil || lhs.Cast != ""
	base := Comparison{Column: lhs.Column, Path: lhs.Path}
	if computed {
		base = Comparison{Expr: &lhs}
	}
	one := func(c Comparison) ([]Comparison, bool, error) { return []Comparison{c}, false, nil }
	// x IS [NOT] NULL | TRUE | FALSE | DISTINCT FROM y ("is" and
	// "distinct" are not reserved words).
	if p.consumeIdentWord("is") {
		not := p.consumeKeyword("NOT")
		neg := func(op string) string {
			if not {
				return strings.Replace(op, "IS ", "IS NOT ", 1)
			}
			return op
		}
		switch {
		case p.consumeKeyword("NULL"):
			base.Op = neg("IS NULL")
		case p.consumeKeyword("TRUE"):
			base.Op = neg("IS TRUE")
		case p.consumeKeyword("FALSE"):
			base.Op = neg("IS FALSE")
		case p.consumeIdentWord("distinct"):
			if err := p.expectKeyword("FROM"); err != nil {
				return nil, false, err
			}
			v, err := p.parseValueOrColumnExpr()
			if err != nil {
				return nil, false, err
			}
			base.Op, base.Value = neg("IS DISTINCT FROM"), v
		default:
			return nil, false, p.errf("expected NULL, TRUE, FALSE or DISTINCT FROM after IS, found %q", p.peek().text)
		}
		return one(base)
	}
	// A NOT before IN / BETWEEN / SIMILAR TO (NOT LIKE is an operator).
	not := false
	if t := p.peek(); t.kind == tkKeyword && t.text == "NOT" && p.i+1 < len(p.toks) && p.toks[p.i+1].kind == tkIdent && notWords[p.toks[p.i+1].text] {
		p.i++
		not = true
	}
	if p.consumeIdentWord("in") {
		cmp := base
		cmp.Op = "IN"
		if not {
			cmp.Op = "NOT IN"
		}
		if sub, ok, err := p.parseSubquery(); err != nil {
			return nil, false, err
		} else if ok {
			cmp.Sub = sub
		} else {
			if err := p.expectOp("("); err != nil {
				return nil, false, err
			}
			for {
				v, err := p.parseAddExpr()
				if err != nil {
					return nil, false, err
				}
				cmp.Values = append(cmp.Values, v)
				if !p.consumeOp(",") {
					break
				}
			}
			if err := p.expectOp(")"); err != nil {
				return nil, false, err
			}
		}
		return one(cmp)
	}
	if p.consumeIdentWord("between") {
		symmetric := p.consumeIdentWord("symmetric")
		lo, err := p.parseAddExpr()
		if err != nil {
			return nil, false, err
		}
		if err := p.expectKeyword("AND"); err != nil {
			return nil, false, err
		}
		hi, err := p.parseAddExpr()
		if err != nil {
			return nil, false, err
		}
		if symmetric {
			lo, hi = Expr{Func: "least", Args: []Expr{lo, hi}}, Expr{Func: "greatest", Args: []Expr{lo, hi}}
		}
		low, high := base, base
		low.Op, low.Value = ">=", lo
		high.Op, high.Value = "<=", hi
		return []Comparison{low, high}, not, nil
	}
	if p.consumeIdentWord("similar") {
		if err := p.expectKeyword("TO"); err != nil {
			return nil, false, err
		}
		pat, err := p.parseValueOrColumnExpr()
		if err != nil {
			return nil, false, err
		}
		cmp := base
		cmp.Op, cmp.Value = "SIMILAR TO", pat
		if not {
			cmp.Op = "NOT SIMILAR TO"
		}
		if cmp.Escape, err = p.parseOptEscape(); err != nil {
			return nil, false, err
		}
		return one(cmp)
	}
	if not {
		return nil, false, p.errf("expected IN, BETWEEN or SIMILAR TO after NOT, found %q", p.peek().text)
	}
	op, ok := p.parseCmpOp()
	if !ok {
		t := p.peek()
		tr := types.NewBool(true)
		if computed {
			// A bare boolean call or value (pg_table_is_visible(c.oid)).
			if lhs.Func != "" || lhs.Lit != nil || lhs.Case != nil || lhs.Cast != "" {
				base.Op, base.Value = "=", Expr{Lit: &tr}
				return one(base)
			}
			return nil, false, p.errf("expected comparison operator, found %q", t.text)
		}
		if t.kind == tkEOF || (t.kind == tkOp && (t.text == ")" || t.text == ";" || t.text == ",")) || t.kind == tkKeyword ||
			(t.kind == tkIdent && (tableClauseWords[t.text] || caseWords[t.text])) {
			// A bare boolean column (WHERE a.attisdropped, ... AND NOT x).
			base.Op, base.Value = "=", Expr{Lit: &tr}
			return one(base)
		}
		return nil, false, p.errf("expected comparison operator, found %q", t.text)
	}
	cmp, err := p.finishComparison(base, op)
	if err != nil {
		return nil, false, err
	}
	if strings.HasSuffix(op, "LIKE") {
		if cmp.Escape, err = p.parseOptEscape(); err != nil {
			return nil, false, err
		}
	}
	p.skipCollate()
	return one(cmp)
}

// NoEscape is the Escape of a pattern given ESCAPE ”: no character
// escapes the wildcards.
const NoEscape = "\x00"

// parseOptEscape parses ESCAPE 'c' after a pattern ("" when absent).
func (p *parser) parseOptEscape() (string, error) {
	if !p.consumeIdentWord("escape") {
		return "", nil
	}
	t := p.peek()
	if t.kind != tkString {
		return "", p.errf("expected a string after ESCAPE, found %q", t.text)
	}
	p.i++
	if len([]rune(t.text)) > 1 {
		return "", p.errf("invalid escape string: must be empty or one character")
	}
	if t.text == "" {
		return NoEscape, nil
	}
	return t.text, nil
}

// finishComparison parses the right-hand side of "lhs op": a value, or
// ANY/SOME/ALL over a parenthesized array value or subquery. "= ANY
// (SELECT ...)" is IN and "<> ALL (SELECT ...)" is NOT IN; array forms
// keep the quantifier in the operator ("= ANY", "!= ALL").
func (p *parser) finishComparison(cmp Comparison, op string) (Comparison, error) {
	cmp.Op = op
	if t := p.peek(); t.kind == tkIdent && (t.text == "any" || t.text == "some" || t.text == "all") &&
		p.i+1 < len(p.toks) && p.toks[p.i+1].kind == tkOp && p.toks[p.i+1].text == "(" {
		quant := "ANY"
		if t.text == "all" {
			quant = "ALL"
		}
		p.i++
		if sub, ok, err := p.parseSubquery(); err != nil {
			return cmp, err
		} else if ok {
			switch {
			case op == "=" && quant == "ANY":
				cmp.Op, cmp.Sub = "IN", sub
			case op == "!=" && quant == "ALL":
				cmp.Op, cmp.Sub = "NOT IN", sub
			default:
				return cmp, p.errf("%s %s (SELECT ...) is not supported", op, quant)
			}
			return cmp, nil
		}
		if err := p.expectOp("("); err != nil {
			return cmp, err
		}
		val, err := p.parseValueOrColumnExpr()
		if err != nil {
			return cmp, err
		}
		if err := p.expectOp(")"); err != nil {
			return cmp, err
		}
		cmp.Op, cmp.Value = op+" "+quant, val
		return cmp, nil
	}
	val, err := p.parseValueOrColumnExpr()
	if err != nil {
		return cmp, err
	}
	cmp.Value = val
	return cmp, nil
}

// parseValueOrBool parses a value expression that may continue as a
// comparison ('d' = any(stxkind), f(x) = 'DEFAULT'), yielding a
// boolean-valued Expr in that case.
func (p *parser) parseValueOrBool() (Expr, error) {
	// NOT x / [NOT] EXISTS (...) at the head: a boolean expression.
	if t := p.peek(); t.kind == tkKeyword && (t.text == "NOT" || t.text == "EXISTS") {
		n, err := p.parseBoolFactor()
		if err != nil {
			return Expr{}, err
		}
		return p.finishBoolValue(n)
	}
	save := p.i
	e, err := p.parseValueOrColumnExpr()
	if err != nil {
		return e, err
	}
	if p.startsPredicateSuffix() {
		// x op y, x IS ..., x [NOT] IN | BETWEEN | LIKE | SIMILAR TO ...
		// as a boolean value: re-parse as a predicate.
		p.i = save
		conds, negated, err := p.parseConjuncts()
		if err != nil {
			return e, err
		}
		var n boolNode
		if len(conds) == 1 {
			n = boolNode{op: "leaf", leaf: conds[0]}
		} else {
			n = boolNode{op: "and"}
			for _, c := range conds {
				n.kids = append(n.kids, boolNode{op: "leaf", leaf: c})
			}
		}
		if negated {
			n = boolNode{op: "not", kids: []boolNode{n}}
		}
		return p.finishBoolValue(n)
	}
	if t := p.peek(); t.kind == tkKeyword && (t.text == "AND" || t.text == "OR") {
		return p.finishBoolValue(boolNode{op: "leaf", leaf: boolLeaf(e)})
	}
	return e, nil
}

// startsPredicateSuffix reports whether the next tokens continue a value
// into a predicate: a comparison operator, IS, [NOT] IN / BETWEEN /
// SIMILAR TO / LIKE / ILIKE, or @>.
func (p *parser) startsPredicateSuffix() bool {
	t := p.peek()
	switch t.kind {
	case tkOp:
		return isCmpOp(t.text) || t.text == "@>"
	case tkIdent:
		switch t.text {
		case "is", "in", "between", "similar", "like", "ilike":
			return true
		case "operator":
			return p.i+1 < len(p.toks) && p.toks[p.i+1].kind == tkOp && p.toks[p.i+1].text == "("
		}
	case tkKeyword:
		if t.text == "NOT" && p.i+1 < len(p.toks) && p.toks[p.i+1].kind == tkIdent {
			switch p.toks[p.i+1].text {
			case "in", "between", "similar", "like", "ilike":
				return true
			}
		}
	}
	return false
}

// finishBoolValue continues a boolean expression after its first factor
// (AND binds tighter than OR) and packs it into a boolean-valued Expr.
func (p *parser) finishBoolValue(first boolNode) (Expr, error) {
	n := first
	if t := p.peek(); t.kind == tkKeyword && t.text == "AND" {
		node := boolNode{op: "and", kids: []boolNode{first}}
		for p.consumeKeyword("AND") {
			k, err := p.parseBoolFactor()
			if err != nil {
				return Expr{}, err
			}
			node.kids = append(node.kids, k)
		}
		n = node
	}
	if t := p.peek(); t.kind == tkKeyword && t.text == "OR" {
		node := boolNode{op: "or", kids: []boolNode{n}}
		for p.consumeKeyword("OR") {
			k, err := p.parseBoolAnd()
			if err != nil {
				return Expr{}, err
			}
			node.kids = append(node.kids, k)
		}
		n = node
	}
	conds, err := lowerBool(n, false)
	if err != nil {
		return Expr{}, err
	}
	return boolValue(conds), nil
}

// boolLeaf turns a value expression into a predicate: a comparison
// stays itself, anything else is "= true".
func boolLeaf(e Expr) Comparison {
	if e.Cmp != nil {
		return *e.Cmp
	}
	tr := types.NewBool(true)
	return Comparison{Expr: &e, Op: "=", Value: Expr{Lit: &tr}}
}

// boolValue packs a conjunction into one boolean-valued Expr: a single
// conjunct as is, several as a one-disjunct OR group.
func boolValue(conds []Comparison) Expr {
	if len(conds) == 1 {
		c := conds[0]
		return Expr{Cmp: &c}
	}
	return Expr{Cmp: &Comparison{Op: "OR", Or: [][]Comparison{conds}}}
}

// isPlainColumn reports whether e is a bare column reference.
func isPlainColumn(e Expr) bool {
	return e.Column != "" && len(e.Path) == 0 && e.BinOp == "" && e.Func == "" && e.Left == nil &&
		e.Case == nil && e.Cmp == nil && e.Lit == nil && e.Sub == nil
}

// lowerBool flattens the boolean tree into a conjunction of comparisons.
// NOT is eliminated by De Morgan plus operator negation — sound under SQL
// three-valued logic because WHERE keeps exactly the TRUE rows and both a
// negated UNKNOWN and an UNKNOWN negation stay UNKNOWN. An OR subtree
// becomes one Op-"OR" comparison holding a disjunction of conjunctions;
// scalar subqueries may appear inside it (evaluated per disjunct), IN
// and EXISTS subqueries may not (the splice works on top-level conjuncts).
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

// HasSubInOr reports whether an [NOT] IN / [NOT] EXISTS subquery sits
// inside an OR group of conds — a shape the executor does not evaluate
// (scalar subqueries in value positions are evaluated per disjunct and
// are fine). Parsed, so a query over an always-empty catalog can still
// be answered.
func HasSubInOr(conds []Comparison) bool {
	for _, c := range conds {
		if len(c.Or) < 2 {
			continue // a one-disjunct group is a packed AND, not an OR
		}
		for _, d := range c.Or {
			for _, inner := range d {
				if inner.Sub != nil || HasSubInOr([]Comparison{inner}) {
					return true
				}
			}
		}
	}
	return false
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
		"@>": "NOT @>", "NOT @>": "@>", "<@": "NOT <@", "NOT <@": "<@",
		"?": "NOT ?", "NOT ?": "?", "?|": "NOT ?|", "NOT ?|": "?|", "?&": "NOT ?&", "NOT ?&": "?&",
		"LIKE": "NOT LIKE", "NOT LIKE": "LIKE", "ILIKE": "NOT ILIKE", "NOT ILIKE": "ILIKE",
		"SIMILAR TO": "NOT SIMILAR TO", "NOT SIMILAR TO": "SIMILAR TO",
		"~": "!~", "!~": "~", "~*": "!~*", "!~*": "~*",
		"IS TRUE": "IS NOT TRUE", "IS NOT TRUE": "IS TRUE", "IS FALSE": "IS NOT FALSE", "IS NOT FALSE": "IS FALSE",
		"IS DISTINCT FROM": "IS NOT DISTINCT FROM", "IS NOT DISTINCT FROM": "IS DISTINCT FROM",
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
	case "=", "!=", "<", "<=", ">", ">=", "~", "!~", "~*", "!~*", "@>", "<@", "?", "?|", "?&":
		return true
	}
	return false
}

// parseCmpOp reads a comparison operator, in its plain or its
// OPERATOR([schema.]op) spelling (psql writes the latter).
func (p *parser) parseCmpOp() (string, bool) {
	t := p.peek()
	if t.kind == tkOp && isCmpOp(t.text) {
		p.i++
		return t.text, true
	}
	// [NOT] LIKE / ILIKE ("like" and "ilike" are not reserved words).
	if t.kind == tkIdent && (t.text == "like" || t.text == "ilike") {
		p.i++
		return strings.ToUpper(t.text), true
	}
	if t.kind == tkKeyword && t.text == "NOT" && p.i+1 < len(p.toks) && p.toks[p.i+1].kind == tkIdent &&
		(p.toks[p.i+1].text == "like" || p.toks[p.i+1].text == "ilike") {
		p.i += 2
		return "NOT " + strings.ToUpper(p.toks[p.i-1].text), true
	}
	if t.kind == tkIdent && t.text == "operator" && p.i+1 < len(p.toks) && p.toks[p.i+1].kind == tkOp && p.toks[p.i+1].text == "(" {
		save := p.i
		p.i += 2
		if p.peek().kind == tkIdent { // schema qualifier
			p.i++
			if !p.consumeOp(".") {
				p.i = save
				return "", false
			}
		}
		op := p.peek()
		if op.kind == tkOp && isCmpOp(op.text) {
			p.i++
			if p.consumeOp(")") {
				return op.text, true
			}
		}
		p.i = save
	}
	return "", false
}

// skipCollate absorbs a COLLATE [schema.]name clause (collations are not
// modeled; the default is what every comparison uses).
func (p *parser) skipCollate() {
	if p.consumeIdentWord("collate") {
		p.i++ // the collation name (or its schema)
		if p.consumeOp(".") {
			p.i++
		}
	}
}

// parseSubquery parses (SELECT ...) when the next tokens open one;
// ok=false otherwise.
func (p *parser) parseSubquery() (*Select, bool, error) {
	if t := p.peek(); t.kind != tkOp || t.text != "(" {
		return nil, false, nil
	}
	nxt := p.toks[p.i+1]
	if !(nxt.kind == tkKeyword && nxt.text == "SELECT") && !(nxt.kind == tkIdent && nxt.text == "with") {
		return nil, false, nil
	}
	p.i++ // (
	var stmt Statement
	var err error
	if nxt.kind == tkIdent {
		stmt, err = p.parseWith()
		if _, isSel := stmt.(*Select); err == nil && !isSel {
			return nil, false, p.errf("a WITH inside a subquery must end in a SELECT")
		}
	} else {
		stmt, err = p.parseSelect()
	}
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
	// Optional ::type casts — absorbed (types come from the schema); the
	// type may be qualified (pg_catalog.regclass) and casts may chain.
	// A regclass cast is kept: it resolves a table name (a literal) or
	// renders one (an OID column).
	return p.applyCasts(e)
}

func (p *parser) skipCasts() error {
	_, err := p.parseCasts()
	return err
}

// parseCasts reads a chain of ::type casts.
func (p *parser) parseCasts() ([]string, error) {
	var casts []string
	for p.consumeOp("::") {
		name, err := p.skipTypeName()
		if err != nil {
			return nil, err
		}
		casts = append(casts, name)
	}
	return casts, nil
}

// applyCasts reads the ::type casts after e and attaches them: the
// first on e itself when it carries no operator yet, each further one
// wrapping the expression so far (x::a::b casts x to a, then to b). A
// node's Cast applies to its whole value; foldBinOp keeps a cast node
// from acquiring an operator.
func (p *parser) applyCasts(e Expr) (Expr, error) {
	casts, err := p.parseCasts()
	if err != nil {
		return e, err
	}
	e = castChain(e, casts)
	// A -> / ->> / #> / #>> chain may follow any value ('{...}'::jsonb
	// -> 'a', f(x) ->> 'k'); it applies to the value as cast, so a node
	// that already carries a cast (or an operator) is wrapped.
	steps, err := p.parsePathSteps()
	if err != nil {
		return e, err
	}
	if len(steps) > 0 {
		if e.Cast != "" || e.BinOp != "" || len(e.Path) > 0 {
			inner := e
			e = Expr{Left: &inner}
		}
		e.Path = steps
		return p.applyCasts(e)
	}
	return e, nil
}

func castChain(e Expr, casts []string) Expr {
	for _, c := range casts {
		if e.Cast == "" && e.BinOp == "" && e.Left == nil {
			e.Cast = c
			continue
		}
		inner := e
		e = Expr{Left: &inner, Cast: c}
	}
	return e
}

// skipTypeName absorbs a type name: [schema.]name, an optional (typmod)
// and array brackets. Type keywords (e.g. INT) lex as keywords.
func (p *parser) skipTypeName() (string, error) {
	var name string
	if t := p.peek(); t.kind == tkIdent || t.kind == tkKeyword {
		name = strings.ToLower(t.text)
		p.i++
	} else {
		return "", p.errf("expected type name, found %q", t.text)
	}
	if p.consumeOp(".") {
		n, err := p.expectIdent()
		if err != nil {
			return "", err
		}
		name = n
	}
	if p.consumeOp("(") {
		// The typmod: kept for the types where it changes the value
		// (numeric(p,s) rounds, varchar(n) truncates), dropped elsewhere.
		var mods []string
		for p.peek().kind != tkEOF && !(p.peek().kind == tkOp && p.peek().text == ")") {
			if t := p.peek(); t.kind == tkNumber {
				mods = append(mods, t.text)
			}
			p.i++
		}
		if err := p.expectOp(")"); err != nil {
			return "", err
		}
		switch name {
		case "numeric", "decimal", "dec", "varchar", "char", "character", "bpchar":
			if len(mods) > 0 {
				name += "(" + strings.Join(mods, ",") + ")"
			}
		}
	}
	for p.consumeOp("[") {
		if err := p.expectOp("]"); err != nil {
			return "", err
		}
		name += "[]"
	}
	return name, nil
}

// parsePathSteps parses a chained ->/->> JSONB extraction after a column
// reference. Keys are string literals; ->> renders text and is therefore
// terminal — jsonb -> 'a' ->> 'b' is fine, ->> 'a' -> 'b' is not.
func (p *parser) parsePathSteps() ([]PathStep, error) {
	var steps []PathStep
	for {
		t := p.peek()
		if t.kind != tkOp || (t.text != "->" && t.text != "->>" && t.text != "#>" && t.text != "#>>") {
			return steps, nil
		}
		if len(steps) > 0 && steps[len(steps)-1].Text {
			return nil, p.errf("cannot apply %s after a text extraction (->> / #>> yield text, not jsonb)", t.text)
		}
		p.i++
		key := p.peek()
		text := strings.HasSuffix(t.text, ">>")
		switch {
		case strings.HasPrefix(t.text, "#"):
			// #> '{a,b}': a path array literal.
			if key.kind != tkString {
				return nil, p.errf("expected a path array after %s, found %q", t.text, key.text)
			}
			p.i++
			steps = append(steps, PathStep{Keys: splitTextArray(key.text), Text: text})
		case key.kind == tkString:
			p.i++
			steps = append(steps, PathStep{Key: key.text, Text: text})
		case key.kind == tkNumber || (key.kind == tkOp && key.text == "-" && p.i+1 < len(p.toks) && p.toks[p.i+1].kind == tkNumber):
			neg := key.kind == tkOp
			if neg {
				p.i++
				key = p.peek()
			}
			n, err := strconv.Atoi(key.text)
			if err != nil {
				return nil, p.errf("expected an array index after %s, found %q", t.text, key.text)
			}
			if neg {
				n = -n
			}
			p.i++
			steps = append(steps, PathStep{IsIndex: true, Index: n, Text: text})
		default:
			return nil, p.errf("expected a key or index after %s, found %q", t.text, key.text)
		}
	}
}

// splitTextArray splits a '{a,b,"c d"}' literal into its elements.
func splitTextArray(s string) []string {
	s = strings.TrimSpace(s)
	if strings.HasPrefix(s, "{") && strings.HasSuffix(s, "}") {
		s = s[1 : len(s)-1]
	}
	if s == "" {
		return nil
	}
	var out []string
	var cur strings.Builder
	inQuote := false
	for i := 0; i < len(s); i++ {
		c := s[i]
		switch {
		case c == '"':
			inQuote = !inQuote
		case c == '\\' && i+1 < len(s):
			i++
			cur.WriteByte(s[i])
		case c == ',' && !inQuote:
			out = append(out, cur.String())
			cur.Reset()
		default:
			cur.WriteByte(c)
		}
	}
	return append(out, cur.String())
}

// parseSQLFuncForm parses the SQL-standard keyword forms of a few
// functions, after the opening parenthesis: substring(s FROM n [FOR m]),
// position(needle IN haystack), trim([BOTH | LEADING | TRAILING] [chars]
// FROM s), extract(field FROM x). Returns ok=false, consuming nothing,
// when the call is the plain form.
func (p *parser) parseSQLFuncForm(name string) (Expr, bool, error) {
	save := p.i
	plain := func() (Expr, bool, error) {
		p.i = save
		return Expr{}, false, nil
	}
	switch name {
	case "substring", "substr":
		s, err := p.parseAddExpr()
		if err != nil || !p.consumeKeyword("FROM") {
			return plain()
		}
		from, err := p.parseAddExpr()
		if err != nil {
			return Expr{}, false, err
		}
		args := []Expr{s, from}
		if p.consumeIdentWord("for") {
			n, err := p.parseAddExpr()
			if err != nil {
				return Expr{}, false, err
			}
			args = append(args, n)
		}
		if err := p.expectOp(")"); err != nil {
			return Expr{}, false, err
		}
		return Expr{Func: name, Args: args}, true, nil
	case "position":
		needle, err := p.parseAddExpr()
		if err != nil || !p.consumeIdentWord("in") {
			return plain()
		}
		hay, err := p.parseAddExpr()
		if err != nil {
			return Expr{}, false, err
		}
		if err := p.expectOp(")"); err != nil {
			return Expr{}, false, err
		}
		return Expr{Func: "position", Args: []Expr{needle, hay}}, true, nil
	case "trim":
		fn := "trim"
		switch {
		case p.consumeIdentWord("both"):
		case p.consumeIdentWord("leading"):
			fn = "ltrim"
		case p.consumeIdentWord("trailing"):
			fn = "rtrim"
		}
		var chars *Expr
		if !p.peekKeyword("FROM") {
			c, err := p.parseAddExpr()
			if err != nil {
				return plain()
			}
			chars = &c
		}
		if !p.consumeKeyword("FROM") {
			return plain()
		}
		s, err := p.parseAddExpr()
		if err != nil {
			return Expr{}, false, err
		}
		if err := p.expectOp(")"); err != nil {
			return Expr{}, false, err
		}
		args := []Expr{s}
		if chars != nil {
			args = append(args, *chars)
		}
		return Expr{Func: fn, Args: args}, true, nil
	case "extract":
		t := p.peek()
		if t.kind != tkIdent && t.kind != tkKeyword && t.kind != tkString {
			return plain()
		}
		p.i++
		if !p.consumeKeyword("FROM") {
			return plain()
		}
		x, err := p.parseAddExpr()
		if err != nil {
			return Expr{}, false, err
		}
		if err := p.expectOp(")"); err != nil {
			return Expr{}, false, err
		}
		field := types.NewString(strings.ToLower(t.text))
		return Expr{Func: "extract", Args: []Expr{{Lit: &field}, x}}, true, nil
	}
	return Expr{}, false, nil
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
		if op.kind != tkOp || (op.text != "+" && op.text != "-" && op.text != "||") {
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
	e, err := p.parsePowExpr()
	if err != nil {
		return e, err
	}
	for {
		op := p.peek()
		if op.kind != tkOp || (op.text != "*" && op.text != "/" && op.text != "%") {
			return e, nil
		}
		p.i++
		rhs, err := p.parsePowExpr()
		if err != nil {
			return e, err
		}
		e = foldBinOp(e, op.text, rhs)
	}
}

// parsePowExpr parses the ^ operator, which binds tighter than * / %
// and associates to the left, as in PostgreSQL.
func (p *parser) parsePowExpr() (Expr, error) {
	e, err := p.parsePrimaryExpr()
	if err != nil {
		return e, err
	}
	for {
		op := p.peek()
		if op.kind != tkOp || op.text != "^" {
			return e, nil
		}
		p.i++
		rhs, err := p.parsePrimaryExpr()
		if err != nil {
			return e, err
		}
		e = foldBinOp(e, "^", rhs)
	}
}

// foldBinOp attaches (lhs op rhs), reusing lhs's own node when it carries
// no operator yet (the flat historical shape) and nesting through Left
// otherwise (left associativity).
func foldBinOp(lhs Expr, op string, rhs Expr) Expr {
	if lhs.BinOp == "" && lhs.Cast == "" {
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
	"now": 0, "current_database": 0, "current_schema": 0, "current_timestamp": 0, "localtimestamp": 0, "current_date": 0,
	"statement_timestamp": 0, "transaction_timestamp": 0,
	// The catalog functions psql and ORMs call (evaluated by the session's
	// catalog splice, see pkg/sql/subquery.go).
	"version": 0, "current_user": 0, "session_user": 0, "pg_backend_pid": 0, "current_setting": -1,
	"pg_get_userbyid": 1, "pg_table_is_visible": 1, "pg_partition_ancestors": 1, "pg_encoding_to_char": 1, "obj_description": -1, "col_description": 2,
	"array_to_string": 2, "pg_get_indexdef": -1, "pg_get_constraintdef": -1, "format_type": 2, "pg_typeof": 1,
	"pg_get_expr": -1, "quote_ident": 1, "current_schemas": 1, "pg_get_viewdef": -1, "shobj_description": 2,
	"pg_get_statisticsobjdef_columns": 1, "pg_relation_is_publishable": 1,
	"pg_size_pretty": 1, "pg_table_size": 1, "pg_total_relation_size": 1, "pg_relation_size": -1, "pg_database_size": 1,
	"has_database_privilege": -1, "has_table_privilege": -1, "has_schema_privilege": -1, "pg_tablespace_location": 1,
	"pg_type_is_visible": 1, "pg_function_is_visible": 1, "pg_get_function_result": 1, "pg_get_function_arguments": 1,
	"pg_get_function_identity_arguments": 1, "pg_get_functiondef": 1, "pg_char_to_encoding": 1, "getdatabaseencoding": 0,
	"pg_get_triggerdef": -1, "pg_get_ruledef": -1,
}

// bareFuncs are called without parentheses (SQL keywords in PostgreSQL).
var bareFuncs = map[string]bool{"current_user": true, "session_user": true, "current_schema": true, "current_timestamp": true, "current_date": true, "localtimestamp": true}

// parsePrimaryExpr parses one operand: a parenthesized expression, a
// builtin call, a possibly-qualified column reference (with an optional
// ->/->> chain), or a literal/parameter/scalar subquery.
func (p *parser) parsePrimaryExpr() (Expr, error) {
	t := p.peek()
	if t.kind == tkOp && t.text == "(" {
		// (NOT x) / (EXISTS ...): a parenthesized boolean value.
		if nxt := p.toks[p.i+1]; nxt.kind == tkKeyword && (nxt.text == "NOT" || nxt.text == "EXISTS") {
			p.i++
			n, err := p.parseBoolOr()
			if err != nil {
				return Expr{}, err
			}
			if err := p.expectOp(")"); err != nil {
				return Expr{}, err
			}
			conds, err := lowerBool(n, false)
			if err != nil {
				return Expr{}, err
			}
			return boolValue(conds), nil
		}
		// A scalar subquery is "(SELECT" or "(WITH"; anything else is
		// grouping — of a value, or of a predicate used as a boolean value.
		if nxt := p.toks[p.i+1]; !(nxt.kind == tkKeyword && nxt.text == "SELECT") && !(nxt.kind == tkIdent && nxt.text == "with") {
			p.i++
			e, err := p.parseValueOrBool()
			if err != nil {
				return e, err
			}
			if err := p.expectOp(")"); err != nil {
				return e, err
			}
			// (a + b)::t casts the group as a whole.
			return p.applyCasts(e)
		}
		return p.parseValueExpr()
	}
	if t.kind == tkIdent {
		// CASE expressions ("case" is not a reserved word).
		if t.text == "case" {
			return p.parseCase()
		}
		// DEFAULT as a value (INSERT VALUES (DEFAULT), SET c = DEFAULT).
		if t.text == "default" && p.i+1 < len(p.toks) && (p.toks[p.i+1].kind == tkEOF ||
			p.toks[p.i+1].kind == tkKeyword || (p.toks[p.i+1].kind == tkOp && (p.toks[p.i+1].text == "," || p.toks[p.i+1].text == ")" || p.toks[p.i+1].text == ";"))) {
			p.i++
			return Expr{IsDefault: true}, nil
		}
		// CAST(expr AS type): the type is skipped, like ::type.
		if t.text == "cast" && p.i+1 < len(p.toks) && p.toks[p.i+1].kind == tkOp && p.toks[p.i+1].text == "(" {
			p.i += 2
			e, err := p.parseAddExpr()
			if err != nil {
				return e, err
			}
			if err := p.expectKeyword("AS"); err != nil {
				return e, err
			}
			name, err := p.skipTypeName()
			if err != nil {
				return e, err
			}
			if err := p.expectOp(")"); err != nil {
				return e, err
			}
			return p.applyCasts(castChain(e, []string{name}))
		}
		// array(SELECT ...): the subquery's single column as a text array.
		if t.text == "array" && p.i+1 < len(p.toks) && p.toks[p.i+1].kind == tkOp && p.toks[p.i+1].text == "(" {
			p.i++
			sub, ok, err := p.parseSubquery()
			if err != nil {
				return Expr{}, err
			}
			if !ok {
				return Expr{}, p.errf("array(...) takes a subquery")
			}
			e := Expr{Func: "array", Args: []Expr{{Sub: sub}}}
			return p.applyCasts(e)
		}
		// pg_catalog.f(...): the schema qualifier is dropped.
		if t.text == "pg_catalog" && p.i+3 < len(p.toks) && p.toks[p.i+1].kind == tkOp && p.toks[p.i+1].text == "." &&
			p.toks[p.i+2].kind == tkIdent && p.toks[p.i+3].kind == tkOp && p.toks[p.i+3].text == "(" {
			p.i += 2
			t = p.peek()
		}
		// current_user / session_user need no parentheses.
		if bareFuncs[t.text] && !(p.i+1 < len(p.toks) && p.toks[p.i+1].kind == tkOp && (p.toks[p.i+1].text == "(" || p.toks[p.i+1].text == ".")) {
			p.i++
			e := Expr{Func: t.text}
			return p.applyCasts(e)
		}
		// Function call: a name followed by "(". Known builtins have
		// their arity checked here; anything else parses and is refused
		// at evaluation (42883), so tools get a clear "unknown function"
		// rather than a syntax error.
		if p.i+1 < len(p.toks) && p.toks[p.i+1].kind == tkOp && p.toks[p.i+1].text == "(" && t.text != "array" {
			arity, isFunc := scalarFuncs[t.text]
			if !isFunc {
				arity = -2 // unchecked
			}
			bi, registered := builtins.Lookup(t.text)
			if registered && bi.Session {
				registered = false // the session's own: arity from scalarFuncs
			}
			p.i += 2
			e := Expr{Func: t.text}
			if fe, ok, err := p.parseSQLFuncForm(t.text); err != nil {
				return e, err
			} else if ok {
				return p.applyCasts(fe)
			}
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
			case registered:
				if !bi.ArityOK(len(e.Args)) {
					return e, p.errf("%s() takes %s argument(s), got %d", t.text, bi.ArityText(), len(e.Args))
				}
			case arity == -1 && len(e.Args) == 0:
				return e, p.errf("%s() requires at least one argument", t.text)
			case arity >= 0 && len(e.Args) != arity:
				return e, p.errf("%s() takes %d argument(s), got %d", t.text, arity, len(e.Args))
			}
			return p.applyCasts(e)
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
		return p.applyCasts(e)
	}
	return p.parseValueExpr()
}

// outputName is the name a select expression is sorted by: its alias,
// else the column it projects (a computed expression sorts as
// "?column?", which the executor refuses with a clear error).
func outputName(se SelectExpr) string {
	if se.Expr.Column != "" && se.Expr.BinOp == "" && se.Expr.Func == "" && len(se.Expr.Path) == 0 {
		return se.Expr.Column
	}
	if se.Alias != "" {
		return se.Alias
	}
	if se.Agg != "" {
		return strings.ToLower(se.Agg) // the aggregate's output name
	}
	return "?column?"
}

// parseCase parses CASE [operand] WHEN ... THEN ... [ELSE ...] END.
func (p *parser) parseCase() (Expr, error) {
	p.i++ // case
	ce := &CaseExpr{}
	if !p.peekIdentWord("when") {
		op, err := p.parseAddExpr()
		if err != nil {
			return Expr{}, err
		}
		ce.Operand = &op
	}
	for p.consumeIdentWord("when") {
		var w CaseWhen
		if ce.Operand != nil {
			v, err := p.parseAddExpr()
			if err != nil {
				return Expr{}, err
			}
			w.Value = &v
		} else {
			n, err := p.parseBoolOr()
			if err != nil {
				return Expr{}, err
			}
			conds, err := lowerBool(n, false)
			if err != nil {
				return Expr{}, err
			}
			w.Cond = conds
		}
		if !p.consumeIdentWord("then") {
			return Expr{}, p.errf("expected THEN in CASE, found %q", p.peek().text)
		}
		r, err := p.parseAddExpr()
		if err != nil {
			return Expr{}, err
		}
		w.Result = r
		ce.Whens = append(ce.Whens, w)
	}
	if len(ce.Whens) == 0 {
		return Expr{}, p.errf("CASE needs at least one WHEN")
	}
	if p.consumeIdentWord("else") {
		e, err := p.parseAddExpr()
		if err != nil {
			return Expr{}, err
		}
		ce.Else = &e
	}
	if !p.consumeKeyword("END") {
		return Expr{}, p.errf("expected END to close CASE, found %q", p.peek().text)
	}
	e := Expr{Case: ce}
	return p.applyCasts(e)
}

func (p *parser) peekIdentWord(word string) bool {
	t := p.peek()
	return t.kind == tkIdent && t.text == word
}

// parseWith parses WITH [RECURSIVE] name [(cols)] AS (query) [, ...]
// followed by the statement it scopes over (SELECT, VALUES, a
// parenthesized query, INSERT, UPSERT, UPDATE or DELETE), attaching the
// members to it. A member's query may itself be a WITH, a set operation,
// or a data-modifying statement (which must return rows).
func (p *parser) parseWith() (Statement, error) {
	p.i++ // with
	recursive := p.consumeIdentWord("recursive")
	var ctes []CTE
	for {
		name, err := p.expectIdent()
		if err != nil {
			return nil, err
		}
		cte := CTE{Name: name, Recursive: recursive}
		if p.consumeOp("(") {
			for {
				col, err := p.expectIdent()
				if err != nil {
					return nil, err
				}
				cte.Columns = append(cte.Columns, col)
				if !p.consumeOp(",") {
					break
				}
			}
			if err := p.expectOp(")"); err != nil {
				return nil, err
			}
		}
		if err := p.expectKeyword("AS"); err != nil {
			return nil, err
		}
		if err := p.expectOp("("); err != nil {
			return nil, err
		}
		q, err := p.parseStatement()
		if err != nil {
			return nil, err
		}
		switch q.(type) {
		case *Select, *Insert, *Update, *Delete:
		default:
			return nil, p.errf("WITH %s: a query, INSERT, UPDATE or DELETE is required", name)
		}
		cte.Query = q
		if err := p.expectOp(")"); err != nil {
			return nil, err
		}
		ctes = append(ctes, cte)
		if !p.consumeOp(",") {
			break
		}
	}
	stmt, err := p.parseStatement()
	if err != nil {
		return nil, err
	}
	switch t := stmt.(type) {
	case *Select:
		t.With = append(ctes, t.With...)
	case *Insert:
		t.With = append(ctes, t.With...)
	case *Update:
		t.With = append(ctes, t.With...)
	case *Delete:
		t.With = append(ctes, t.With...)
	default:
		return nil, p.errf("WITH must be followed by SELECT, INSERT, UPDATE or DELETE")
	}
	return stmt, nil
}
