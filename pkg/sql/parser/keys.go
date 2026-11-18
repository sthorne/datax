package parser

import "strings"

// The key service's statements (issue #310, step 2;
// docs/design-key-service.md, "A. The key service").

// CreateEncryptionKey is CREATE ENCRYPTION KEY [IF NOT EXISTS] name.
type CreateEncryptionKey struct {
	Name        string
	IfNotExists bool
}

// AlterEncryptionKey is ALTER ENCRYPTION KEY name ROTATE | DISABLE |
// ENABLE | OWNER TO role.
type AlterEncryptionKey struct {
	Name string
	// Action is "rotate", "disable", "enable" or "owner".
	Action string
	// Owner is the new owner (Action "owner"), lower-cased.
	Owner string
}

// DropEncryptionKey is DROP ENCRYPTION KEY [IF EXISTS] name.
type DropEncryptionKey struct {
	Name     string
	IfExists bool
}

// ShowEncryptionKeys is SHOW ENCRYPTION KEYS.
type ShowEncryptionKeys struct{}

// SetClusterSetting is SET CLUSTER SETTING name = value | DEFAULT.
type SetClusterSetting struct {
	Name string
	// Value is the value's text; Default marks = DEFAULT (the setting
	// returns to its default).
	Value   string
	Default bool
}

// ShowClusterSetting is SHOW CLUSTER SETTING name.
type ShowClusterSetting struct {
	Name string
}

func (*CreateEncryptionKey) stmt() {}
func (*AlterEncryptionKey) stmt()  {}
func (*DropEncryptionKey) stmt()   {}
func (*ShowEncryptionKeys) stmt()  {}
func (*SetClusterSetting) stmt()   {}
func (*ShowClusterSetting) stmt()  {}

// peekEncryptionKey reports whether the tokens at offset ahead are
// ENCRYPTION KEY.
func (p *parser) peekEncryptionKey(ahead int) bool {
	return p.peekWord(ahead, "encryption") && p.peekWord(ahead+1, "key")
}

// parseEncryptionKeyName parses a key's name: an identifier (lower-cased
// unless quoted, as every name is).
func (p *parser) parseEncryptionKeyName() (string, error) {
	return p.expectIdent()
}

func (p *parser) expectEnd(what string) error {
	if !p.atStatementEnd() {
		return p.errf("unexpected %q in %s", p.peek().text, what)
	}
	return nil
}

// parseCreateEncryptionKey parses CREATE ENCRYPTION KEY (positioned at
// CREATE).
func (p *parser) parseCreateEncryptionKey() (Statement, error) {
	p.i += 3 // CREATE ENCRYPTION KEY
	ck := &CreateEncryptionKey{}
	if p.consumeKeyword("IF") {
		if err := p.expectKeyword("NOT"); err != nil {
			return nil, err
		}
		if err := p.expectKeyword("EXISTS"); err != nil {
			return nil, err
		}
		ck.IfNotExists = true
	}
	name, err := p.parseEncryptionKeyName()
	if err != nil {
		return nil, err
	}
	ck.Name = name
	return ck, p.expectEnd("CREATE ENCRYPTION KEY")
}

// parseAlterEncryptionKey parses ALTER ENCRYPTION KEY (positioned at
// ALTER).
func (p *parser) parseAlterEncryptionKey() (Statement, error) {
	p.i += 3 // ALTER ENCRYPTION KEY
	name, err := p.parseEncryptionKeyName()
	if err != nil {
		return nil, err
	}
	ak := &AlterEncryptionKey{Name: name}
	switch {
	case p.consumeWord("rotate"):
		ak.Action = "rotate"
	case p.consumeWord("disable"):
		ak.Action = "disable"
	case p.consumeWord("enable"):
		ak.Action = "enable"
	case p.consumeWord("owner"):
		if err := p.expectKeyword("TO"); err != nil {
			return nil, err
		}
		owner, err := p.parseRoleName()
		if err != nil {
			return nil, err
		}
		ak.Action, ak.Owner = "owner", strings.ToLower(owner)
	default:
		return nil, p.errf("ALTER ENCRYPTION KEY takes ROTATE, DISABLE, ENABLE or OWNER TO, found %q", p.peek().text)
	}
	return ak, p.expectEnd("ALTER ENCRYPTION KEY")
}

// parseDropEncryptionKey parses DROP ENCRYPTION KEY (positioned at
// DROP).
func (p *parser) parseDropEncryptionKey() (Statement, error) {
	p.i += 3 // DROP ENCRYPTION KEY
	dk := &DropEncryptionKey{}
	if p.consumeKeyword("IF") {
		if err := p.expectKeyword("EXISTS"); err != nil {
			return nil, err
		}
		dk.IfExists = true
	}
	name, err := p.parseEncryptionKeyName()
	if err != nil {
		return nil, err
	}
	dk.Name = name
	return dk, p.expectEnd("DROP ENCRYPTION KEY")
}

// parseClusterSetting parses what follows SET CLUSTER SETTING or SHOW
// CLUSTER SETTING (positioned at CLUSTER).
func (p *parser) parseClusterSetting(show bool) (Statement, error) {
	p.i += 2 // CLUSTER SETTING
	name, err := p.parseVarName()
	if err != nil {
		return nil, err
	}
	name = strings.ToLower(name)
	if show {
		return &ShowClusterSetting{Name: name}, p.expectEnd("SHOW CLUSTER SETTING")
	}
	if !p.consumeOp("=") && !p.consumeKeyword("TO") {
		return nil, p.errf("expected = after SET CLUSTER SETTING %s, found %q", name, p.peek().text)
	}
	sc := &SetClusterSetting{Name: name}
	neg := p.consumeOp("-")
	t := p.peek()
	switch {
	case neg && t.kind == tkNumber:
		p.i++
		sc.Value = "-" + t.text
	case neg:
		return nil, p.errf("expected a number after - for cluster setting %s, found %q", name, t.text)
	case t.kind == tkKeyword && t.text == "DEFAULT" || t.isWord("default"):
		p.i++
		sc.Default = true
	case t.kind == tkString || t.kind == tkNumber || t.kind == tkIdent:
		p.i++
		sc.Value = t.text
	default:
		return nil, p.errf("expected a value for cluster setting %s, found %q", name, t.text)
	}
	return sc, p.expectEnd("SET CLUSTER SETTING")
}

// privilegeFormAhead reports whether the GRANT / REVOKE at hand is the
// privilege form, looking for ON before the TO / FROM: how WRAP and
// UNWRAP are told apart from roles of those names.
func (p *parser) privilegeFormAhead() bool {
	for j := p.i; j < len(p.toks); j++ {
		t := p.toks[j]
		switch {
		case t.kind == tkEOF || t.kind == tkOp && t.text == ";":
			return false
		case t.kind == tkKeyword && t.text == "ON":
			return true
		case t.kind == tkKeyword && t.text == "TO", t.kind == tkKeyword && t.text == "FROM":
			return false
		}
	}
	return false
}
