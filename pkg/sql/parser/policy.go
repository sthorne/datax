package parser

import "strings"

// Row-level security (issue #294): ALTER TABLE ... ROW LEVEL SECURITY and
// the policy statements.

// CreatePolicy is CREATE POLICY name ON table [AS PERMISSIVE |
// RESTRICTIVE] [FOR ALL | SELECT | INSERT | UPDATE | DELETE] [TO role
// [, ...]] [USING (expr)] [WITH CHECK (expr)].
type CreatePolicy struct {
	Name  string
	Table string
	// Restrictive is AS RESTRICTIVE (the default is permissive).
	Restrictive bool
	// Command is "all" (the default), "select", "insert", "update" or
	// "delete".
	Command string
	// Roles are the TO roles, lower-cased; empty means PUBLIC.
	Roles []string
	// Using and Check are the expressions' source text ("" = not given):
	// what the descriptor stores and the catalogs show.
	Using string
	Check string
}

// AlterPolicy is ALTER POLICY name ON table RENAME TO new, or ALTER
// POLICY name ON table [TO role [, ...]] [USING (expr)] [WITH CHECK
// (expr)]: each clause given replaces the policy's; the command and
// the permissive / restrictive kind cannot be changed, as in PostgreSQL.
type AlterPolicy struct {
	Name     string
	Table    string
	RenameTo string
	// SetRoles marks a TO clause (Roles empty then means PUBLIC).
	SetRoles bool
	Roles    []string
	// Using / Check are nil when the clause was not given.
	Using *string
	Check *string
}

// DropPolicy is DROP POLICY [IF EXISTS] name ON table [CASCADE |
// RESTRICT].
type DropPolicy struct {
	Name     string
	Table    string
	IfExists bool
}

func (*CreatePolicy) stmt() {}
func (*AlterPolicy) stmt()  {}
func (*DropPolicy) stmt()   {}

// peekWord reports whether the token at offset ahead is word, whether it
// lexes as a keyword or an identifier.
func (p *parser) peekWord(ahead int, word string) bool {
	if p.i+ahead >= len(p.toks) {
		return false
	}
	t := p.toks[p.i+ahead]
	return t.kind == tkKeyword && t.text == strings.ToUpper(word) || t.kind == tkIdent && t.text == strings.ToLower(word)
}

// parseRowSecurity parses what follows ALTER TABLE t: ENABLE | DISABLE |
// FORCE | NO FORCE ROW LEVEL SECURITY, returning "enable", "disable",
// "force" or "no force" ("" when the next words are something else).
func (p *parser) parseRowSecurity() (string, error) {
	action := ""
	switch {
	case p.consumeWord("enable"):
		action = "enable"
	case p.consumeWord("disable"):
		action = "disable"
	case p.consumeWord("force"):
		action = "force"
	case p.peekWord(0, "no") && p.peekWord(1, "force"):
		p.i += 2
		action = "no force"
	default:
		return "", nil
	}
	for _, w := range []string{"row", "level", "security"} {
		if !p.consumeWord(w) {
			return "", p.errf("expected %s ROW LEVEL SECURITY, found %q", strings.ToUpper(action), p.peek().text)
		}
	}
	return action, nil
}

// parsePolicyTarget parses name ON table.
func (p *parser) parsePolicyTarget() (name, table string, err error) {
	if name, err = p.expectIdent(); err != nil {
		return "", "", err
	}
	if err = p.expectKeyword("ON"); err != nil {
		return "", "", err
	}
	table, err = p.parseTableName()
	return name, table, err
}

// parsePolicyRoles parses the role list of TO: names, PUBLIC (kept as
// "public"), CURRENT_USER and SESSION_USER are refused — a policy names
// roles, not whoever happened to create it.
func (p *parser) parsePolicyRoles() ([]string, error) {
	var roles []string
	for {
		if p.peekWord(0, "current_user") || p.peekWord(0, "session_user") || p.peekWord(0, "current_role") {
			return nil, p.errf("%s is not supported in a policy's TO list: name the role", strings.ToUpper(p.peek().text))
		}
		r, err := p.parseRoleName()
		if err != nil {
			return nil, err
		}
		roles = append(roles, strings.ToLower(r))
		if !p.consumeOp(",") {
			return roles, nil
		}
	}
}

// parsePolicyExpr parses a parenthesized policy expression and returns
// its source text. The expression is lowered here only to refuse at
// parse time what could never be applied.
func (p *parser) parsePolicyExpr(clause string) (string, error) {
	if err := p.expectOp("("); err != nil {
		return "", err
	}
	start := p.peek().pos
	n, err := p.parseBoolOr()
	if err != nil {
		return "", err
	}
	end := p.peek().pos
	if err := p.expectOp(")"); err != nil {
		return "", err
	}
	if _, err := lowerBool(n, false); err != nil {
		return "", p.errf("%s: %v", clause, err)
	}
	return strings.TrimSpace(p.src[start:end]), nil
}

// parsePolicyClauses parses the trailing [TO ...] [USING (...)] [WITH
// CHECK (...)] clauses shared by CREATE and ALTER POLICY.
func (p *parser) parsePolicyClauses(what string) (roles []string, setRoles bool, using, check *string, err error) {
	if p.consumeKeyword("TO") {
		if roles, err = p.parsePolicyRoles(); err != nil {
			return nil, false, nil, nil, err
		}
		setRoles = true
	}
	if p.consumeWord("using") {
		text, err := p.parsePolicyExpr("USING")
		if err != nil {
			return nil, false, nil, nil, err
		}
		using = &text
	}
	if p.consumeWord("with") {
		if err := p.expectKeyword("CHECK"); err != nil {
			return nil, false, nil, nil, err
		}
		text, err := p.parsePolicyExpr("WITH CHECK")
		if err != nil {
			return nil, false, nil, nil, err
		}
		check = &text
	}
	if !p.atStatementEnd() {
		return nil, false, nil, nil, p.errf("unexpected %q in %s", p.peek().text, what)
	}
	return roles, setRoles, using, check, nil
}

// parseCreatePolicy parses CREATE POLICY (positioned at CREATE).
func (p *parser) parseCreatePolicy() (Statement, error) {
	p.i += 2 // CREATE POLICY
	name, table, err := p.parsePolicyTarget()
	if err != nil {
		return nil, err
	}
	cp := &CreatePolicy{Name: name, Table: table, Command: "all"}
	if p.consumeKeyword("AS") {
		switch {
		case p.consumeWord("permissive"):
		case p.consumeWord("restrictive"):
			cp.Restrictive = true
		default:
			return nil, p.errf("expected PERMISSIVE or RESTRICTIVE after AS, found %q", p.peek().text)
		}
	}
	if p.consumeWord("for") {
		switch {
		case p.consumeWord("all"):
		case p.consumeKeyword("SELECT"):
			cp.Command = "select"
		case p.consumeKeyword("INSERT"):
			cp.Command = "insert"
		case p.consumeKeyword("UPDATE"):
			cp.Command = "update"
		case p.consumeKeyword("DELETE"):
			cp.Command = "delete"
		default:
			return nil, p.errf("expected ALL, SELECT, INSERT, UPDATE or DELETE after FOR, found %q", p.peek().text)
		}
	}
	roles, _, using, check, err := p.parsePolicyClauses("CREATE POLICY")
	if err != nil {
		return nil, err
	}
	cp.Roles = roles
	if using != nil {
		cp.Using = *using
	}
	if check != nil {
		cp.Check = *check
	}
	// The same refusals PostgreSQL makes: a SELECT or DELETE policy
	// writes no row to check, and an INSERT policy reads none to filter.
	switch {
	case (cp.Command == "select" || cp.Command == "delete") && check != nil:
		return nil, p.errf("WITH CHECK cannot be applied to a %s policy", strings.ToUpper(cp.Command))
	case cp.Command == "insert" && using != nil:
		return nil, p.errf("only a WITH CHECK expression is allowed for an INSERT policy")
	}
	return cp, nil
}

// parseAlterPolicy parses ALTER POLICY (positioned at ALTER).
func (p *parser) parseAlterPolicy() (Statement, error) {
	p.i += 2 // ALTER POLICY
	name, table, err := p.parsePolicyTarget()
	if err != nil {
		return nil, err
	}
	ap := &AlterPolicy{Name: name, Table: table}
	if p.consumeWord("rename") {
		if err := p.expectKeyword("TO"); err != nil {
			return nil, err
		}
		if ap.RenameTo, err = p.expectIdent(); err != nil {
			return nil, err
		}
		if !p.atStatementEnd() {
			return nil, p.errf("unexpected %q in ALTER POLICY", p.peek().text)
		}
		return ap, nil
	}
	if ap.Roles, ap.SetRoles, ap.Using, ap.Check, err = p.parsePolicyClauses("ALTER POLICY"); err != nil {
		return nil, err
	}
	if !ap.SetRoles && ap.Using == nil && ap.Check == nil {
		return nil, p.errf("ALTER POLICY needs RENAME TO, TO, USING or WITH CHECK")
	}
	return ap, nil
}

// parseDropPolicy parses DROP POLICY (positioned at DROP).
func (p *parser) parseDropPolicy() (Statement, error) {
	p.i += 2 // DROP POLICY
	dp := &DropPolicy{}
	if p.consumeKeyword("IF") {
		if err := p.expectKeyword("EXISTS"); err != nil {
			return nil, err
		}
		dp.IfExists = true
	}
	var err error
	if dp.Name, dp.Table, err = p.parsePolicyTarget(); err != nil {
		return nil, err
	}
	if !p.consumeIdentWord("cascade") {
		p.consumeIdentWord("restrict")
	}
	return dp, nil
}

// ParsePolicyExpr lowers a stored policy expression to the conjuncts
// that hold for the rows it admits. Like WHERE, a policy keeps only the
// rows its expression is TRUE for: a NULL result hides the row (USING)
// or refuses it (WITH CHECK).
func ParsePolicyExpr(text string) ([]Comparison, error) {
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
	return lowerBool(n, false)
}
