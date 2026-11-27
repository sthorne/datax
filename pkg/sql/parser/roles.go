package parser

import (
	"fmt"
	"strconv"
	"strings"
)

// Roles, ownership and privilege scopes (issue #98).

// consumeWord consumes the next token when it is word, whether it lexes
// as a keyword (upper-cased) or an identifier (lower-cased).
func (p *parser) consumeWord(word string) bool {
	t := p.peek()
	if t.kind == tkKeyword && t.text == strings.ToUpper(word) || t.kind == tkIdent && t.text == strings.ToLower(word) {
		p.i++
		return true
	}
	return false
}

// parseRoleName parses a role name: an identifier, or a string (some
// tools quote role names), lower-cased unless quoted.
func (p *parser) parseRoleName() (string, error) {
	t := p.peek()
	if t.kind == tkString {
		p.i++
		return t.text, nil
	}
	if t.kind == tkKeyword && (t.text == "USER" || t.text == "SESSION") {
		p.i++
		return strings.ToLower(t.text), nil
	}
	return p.expectIdent()
}

// parseRoleList parses role [, ...].
func (p *parser) parseRoleList() ([]string, error) {
	var out []string
	for {
		name, err := p.parseRoleName()
		if err != nil {
			return nil, err
		}
		out = append(out, name)
		if !p.consumeOp(",") {
			return out, nil
		}
	}
}

// parseRoleStmt parses CREATE ROLE | USER [IF NOT EXISTS] name [WITH]
// option ... and ALTER ROLE | USER name [WITH] option ...
func (p *parser) parseRoleStmt(alter bool) (Statement, error) {
	p.i++ // CREATE | ALTER
	cr := &CreateRole{Alter: alter}
	if p.consumeKeyword("USER") {
		cr.IsUser = true
	} else if !p.consumeWord("role") {
		return nil, p.errf("expected ROLE or USER, found %q", p.peek().text)
	}
	if !alter && p.consumeKeyword("IF") {
		if err := p.expectKeyword("NOT"); err != nil {
			return nil, err
		}
		if err := p.expectKeyword("EXISTS"); err != nil {
			return nil, err
		}
		cr.IfNotExists = true
	}
	name, err := p.parseRoleName()
	if err != nil {
		return nil, err
	}
	cr.Name = name
	p.consumeWord("with")
	set := func(dst **bool, v bool) { b := v; *dst = &b }
	for !p.atStatementEnd() {
		t := p.peek()
		word := strings.ToLower(t.text)
		if t.kind != tkKeyword && t.kind != tkIdent {
			return nil, p.errf("unexpected %q in %s", t.text, map[bool]string{false: "CREATE ROLE", true: "ALTER ROLE"}[alter])
		}
		p.i++
		switch word {
		case "login":
			set(&cr.Login, true)
		case "nologin":
			set(&cr.Login, false)
		case "inherit":
			set(&cr.Inherit, true)
		case "noinherit":
			set(&cr.Inherit, false)
		case "encrypted", "unencrypted":
			if !p.consumeKeyword("PASSWORD") {
				return nil, p.errf("expected PASSWORD after %s", strings.ToUpper(word))
			}
			fallthrough
		case "password":
			pt := p.peek()
			switch {
			case pt.kind == tkString:
				p.i++
				pw := pt.text
				cr.Password = &pw
			case pt.kind == tkKeyword && pt.text == "NULL":
				p.i++
				empty := ""
				cr.Password = &empty
			default:
				return nil, p.errf("expected a password string or NULL, found %q", pt.text)
			}
		case "in":
			if !p.consumeWord("role") && !p.consumeWord("group") {
				return nil, p.errf("expected ROLE after IN")
			}
			roles, err := p.parseRoleList()
			if err != nil {
				return nil, err
			}
			cr.InRoles = append(cr.InRoles, roles...)
		case "bypassrls":
			set(&cr.BypassRLS, true)
		case "nobypassrls":
			set(&cr.BypassRLS, false)
		case "superuser":
			set(&cr.Superuser, true)
		case "nosuperuser":
			set(&cr.Superuser, false)
		case "createdb":
			set(&cr.CreateDB, true)
		case "nocreatedb":
			set(&cr.CreateDB, false)
		case "viewcluster":
			set(&cr.ViewCluster, true)
		case "noviewcluster":
			set(&cr.ViewCluster, false)
		case "createrole":
			set(&cr.CreateRole, true)
		case "nocreaterole":
			set(&cr.CreateRole, false)
		case "replication":
			set(&cr.Replication, true)
		case "noreplication":
			set(&cr.Replication, false)
		case "connection":
			if !p.consumeWord("limit") {
				return nil, p.errf("expected LIMIT after CONNECTION")
			}
			n, err := p.parseSignedInt("CONNECTION LIMIT")
			if err != nil {
				return nil, err
			}
			cr.ConnLimit = &n
		case "valid":
			if !p.consumeIdentWord("until") {
				return nil, p.errf("expected UNTIL after VALID")
			}
			vt := p.peek()
			if vt.kind != tkString {
				return nil, p.errf("VALID UNTIL takes a quoted timestamp, found %q", vt.text)
			}
			p.i++
			v := vt.text
			cr.ValidUntil = &v
		case "sysid", "role", "admin":
			return nil, p.errf("role option %s is not supported (use GRANT role TO role for membership)", strings.ToUpper(word))
		default:
			return nil, p.errf("unexpected %q in role options", t.text)
		}
		p.consumeOp(",")
	}
	return cr, nil
}

// parseDropRole parses DROP ROLE | USER [IF EXISTS] name [, ...].
func (p *parser) parseDropRole() (Statement, error) {
	p.i += 2 // DROP ROLE|USER
	dr := &DropRole{}
	if p.consumeKeyword("IF") {
		if err := p.expectKeyword("EXISTS"); err != nil {
			return nil, err
		}
		dr.IfExists = true
	}
	names, err := p.parseRoleList()
	if err != nil {
		return nil, err
	}
	dr.Names = names
	return dr, nil
}

// privilegeWords are the grantable privilege names; ALL stays ALL.
var privilegeWords = map[string]bool{
	"SELECT": true, "INSERT": true, "UPDATE": true, "DELETE": true, "TRUNCATE": true,
	"USAGE": true, "CREATE": true, "CONNECT": true, "ALL": true,
	// The key service's (issue #310): on an encryption key only.
	"WRAP": true, "UNWRAP": true,
}

// parseGrantRevoke parses GRANT / REVOKE in both forms.
func (p *parser) parseGrantRevoke(revoke bool) (Statement, error) {
	p.i++ // grant | revoke
	gr := &GrantRevoke{Revoke: revoke}
	linkKw := "TO"
	if revoke {
		linkKw = "FROM"
		// REVOKE [GRANT OPTION FOR | ADMIN OPTION FOR] ...
		switch {
		case p.peekIdentSeq("grant", "option", "for"):
			p.i += 3
			gr.GrantOption = true
		case p.peekIdentSeq("admin", "option", "for"):
			p.i += 3
			gr.AdminOption = true
		}
	}

	// The first word decides the form: a privilege name, or a role.
	first := p.peek()
	isPriv := (first.kind == tkKeyword || first.kind == tkIdent) && privilegeWords[strings.ToUpper(first.text)] &&
		!(strings.ToUpper(first.text) == "ALL" && p.i+1 < len(p.toks) && p.toks[p.i+1].kind == tkKeyword && p.toks[p.i+1].text == "TO")
	if w := strings.ToUpper(first.text); isPriv && (w == "WRAP" || w == "UNWRAP") {
		// Words that are no keyword anywhere else: a role may well be
		// called wrap. The privilege form has an ON before its TO.
		isPriv = p.privilegeFormAhead()
	}
	if !isPriv {
		roles, err := p.parseRoleList()
		if err != nil {
			return nil, err
		}
		gr.Roles = roles
		if err := p.expectKeyword(linkKw); err != nil {
			return nil, err
		}
		grantees, err := p.parseRoleList()
		if err != nil {
			return nil, err
		}
		gr.Grantees = grantees
		if !revoke && p.consumeWord("with") {
			if !p.consumeWord("admin") || !p.consumeWord("option") {
				return nil, p.errf("expected ADMIN OPTION after WITH")
			}
			gr.AdminOption = true
		}
		if revoke {
			p.consumeWord("cascade")
			p.consumeWord("restrict")
		}
		return gr, nil
	}

	for {
		t := p.peek()
		word := strings.ToUpper(t.text)
		if (t.kind != tkKeyword && t.kind != tkIdent) || !privilegeWords[word] {
			return nil, p.errf("expected a privilege (SELECT, INSERT, UPDATE, DELETE, TRUNCATE, USAGE, CREATE, CONNECT, WRAP, UNWRAP, ALL), found %q", t.text)
		}
		p.i++
		if word == "ALL" {
			p.consumeWord("privileges")
		}
		gr.Privileges = append(gr.Privileges, word)
		if !p.consumeOp(",") {
			break
		}
	}
	if err := p.expectKeyword("ON"); err != nil {
		return nil, err
	}
	switch {
	case p.consumeWord("all"):
		gr.AllInSchema = true
		switch {
		case p.consumeKeyword("TABLES"):
			gr.ObjectKind = "table"
		case p.consumeWord("sequences"):
			gr.ObjectKind = "sequence"
		default:
			return nil, p.errf("expected TABLES or SEQUENCES after ON ALL, found %q", p.peek().text)
		}
		if !p.consumeWord("in") || !p.consumeWord("schema") {
			return nil, p.errf("expected IN SCHEMA after ON ALL %sS", strings.ToUpper(gr.ObjectKind))
		}
		schemas, err := p.parseSchemaList()
		if err != nil {
			return nil, err
		}
		gr.Schemas = schemas
	case p.peekEncryptionKey(0):
		p.i += 2
		gr.ObjectKind = "encryption key"
		for {
			name, err := p.parseEncryptionKeyName()
			if err != nil {
				return nil, err
			}
			gr.Objects = append(gr.Objects, name)
			if !p.consumeOp(",") {
				break
			}
		}
	case p.consumeWord("database"):
		gr.ObjectKind = "database"
		names, err := p.parseRoleList()
		if err != nil {
			return nil, err
		}
		gr.Objects = names
	case p.consumeWord("schema"):
		gr.ObjectKind = "schema"
		schemas, err := p.parseSchemaList()
		if err != nil {
			return nil, err
		}
		gr.Objects = schemas
	case p.consumeWord("sequence"):
		gr.ObjectKind = "sequence"
		names, err := p.parseTableNameList()
		if err != nil {
			return nil, err
		}
		gr.Objects = names
	default:
		p.consumeKeyword("TABLE")
		gr.ObjectKind = "table"
		names, err := p.parseTableNameList()
		if err != nil {
			return nil, err
		}
		gr.Objects = names
	}
	if err := p.expectKeyword(linkKw); err != nil {
		return nil, err
	}
	grantees, err := p.parseGranteeList()
	if err != nil {
		return nil, err
	}
	gr.Grantees = grantees
	if !revoke && p.consumeWord("with") {
		if !p.consumeWord("grant") || !p.consumeWord("option") {
			return nil, p.errf("expected GRANT OPTION after WITH")
		}
		gr.GrantOption = true
	}
	if revoke {
		p.consumeWord("cascade")
		p.consumeWord("restrict")
	}
	return gr, nil
}

// parseSchemaList consumes a schema name list.
func (p *parser) parseSchemaList() ([]string, error) {
	var out []string
	for {
		name, err := p.expectIdent()
		if err != nil {
			return nil, err
		}
		out = append(out, name)
		if !p.consumeOp(",") {
			return out, nil
		}
	}
}

// parseTableNameList parses name [, ...] of possibly qualified names.
func (p *parser) parseTableNameList() ([]string, error) {
	var out []string
	for {
		name, err := p.parseTableName()
		if err != nil {
			return nil, err
		}
		out = append(out, name)
		if !p.consumeOp(",") {
			return out, nil
		}
	}
}

// parseGranteeList parses grantee [, ...]: role names, PUBLIC, and the
// [GROUP] role spelling.
func (p *parser) parseGranteeList() ([]string, error) {
	var out []string
	for {
		p.consumeWord("group")
		name, err := p.parseRoleName()
		if err != nil {
			return nil, err
		}
		out = append(out, name)
		if !p.consumeOp(",") {
			return out, nil
		}
	}
}

// parseOwnerTo parses OWNER TO role after ALTER <kind> name.
func (p *parser) parseOwnerTo(kind, name string) (Statement, error) {
	if err := p.expectKeyword("TO"); err != nil {
		return nil, err
	}
	owner, err := p.parseRoleName()
	if err != nil {
		return nil, err
	}
	return &AlterOwner{Kind: kind, Name: name, Owner: owner}, nil
}

// parseReassignOwned parses REASSIGN OWNED BY role [, ...] TO role.
func (p *parser) parseReassignOwned() (Statement, error) {
	p.i++ // reassign
	if !p.consumeWord("owned") || !p.consumeKeyword("BY") {
		return nil, p.errf("expected OWNED BY after REASSIGN")
	}
	from, err := p.parseRoleList()
	if err != nil {
		return nil, err
	}
	if err := p.expectKeyword("TO"); err != nil {
		return nil, err
	}
	to, err := p.parseRoleName()
	if err != nil {
		return nil, err
	}
	return &ReassignOwned{From: from, To: to}, nil
}

// parseDropOwned parses DROP OWNED BY role [, ...] [CASCADE | RESTRICT].
func (p *parser) parseDropOwned() (Statement, error) {
	p.i += 2 // DROP OWNED
	if err := p.expectKeyword("BY"); err != nil {
		return nil, err
	}
	roles, err := p.parseRoleList()
	if err != nil {
		return nil, err
	}
	do := &DropOwned{Roles: roles}
	if p.consumeWord("cascade") {
		do.Cascade = true
	} else {
		p.consumeWord("restrict")
	}
	return do, nil
}

// parseAlterDefaultPrivileges parses ALTER DEFAULT PRIVILEGES [FOR ROLE
// r [, ...]] [IN SCHEMA public] GRANT | REVOKE ....
func (p *parser) parseAlterDefaultPrivileges() (Statement, error) {
	p.i += 3 // ALTER DEFAULT PRIVILEGES
	adp := &AlterDefaultPrivileges{}
	for {
		switch {
		case p.consumeWord("for"):
			if !p.consumeWord("role") && !p.consumeKeyword("USER") {
				return nil, p.errf("expected ROLE after FOR")
			}
			roles, err := p.parseRoleList()
			if err != nil {
				return nil, err
			}
			adp.ForRoles = append(adp.ForRoles, roles...)
			continue
		case p.consumeWord("in"):
			if !p.consumeWord("schema") {
				return nil, p.errf("expected SCHEMA after IN")
			}
			schemas, err := p.parseSchemaList()
			if err != nil {
				return nil, err
			}
			for _, sc := range schemas {
				if sc != "public" {
					return nil, &SyntaxError{Msg: fmt.Sprintf("ALTER DEFAULT PRIVILEGES IN SCHEMA %s is not supported: only the public schema takes default privileges", sc), Pos: p.peek().pos, Code: "0A000"}
				}
			}
			continue
		}
		break
	}
	linkKw := "TO"
	switch {
	case p.consumeWord("grant"):
	case p.consumeWord("revoke"):
		adp.Revoke = true
		linkKw = "FROM"
		if p.peekIdentSeq("grant", "option", "for") {
			p.i += 3
			adp.GrantOption = true
		}
	default:
		return nil, p.errf("expected GRANT or REVOKE in ALTER DEFAULT PRIVILEGES, found %q", p.peek().text)
	}
	for {
		t := p.peek()
		word := strings.ToUpper(t.text)
		if (t.kind != tkKeyword && t.kind != tkIdent) || !privilegeWords[word] {
			return nil, p.errf("expected a privilege, found %q", t.text)
		}
		p.i++
		if word == "ALL" {
			p.consumeWord("privileges")
		}
		adp.Privileges = append(adp.Privileges, word)
		if !p.consumeOp(",") {
			break
		}
	}
	if err := p.expectKeyword("ON"); err != nil {
		return nil, err
	}
	switch {
	case p.consumeKeyword("TABLES"):
		adp.ObjectKind = "tables"
	case p.consumeWord("sequences"):
		adp.ObjectKind = "sequences"
	default:
		return nil, p.errf("expected TABLES or SEQUENCES, found %q", p.peek().text)
	}
	if err := p.expectKeyword(linkKw); err != nil {
		return nil, err
	}
	grantees, err := p.parseGranteeList()
	if err != nil {
		return nil, err
	}
	adp.Grantees = grantees
	if !adp.Revoke && p.consumeWord("with") {
		if !p.consumeWord("grant") || !p.consumeWord("option") {
			return nil, p.errf("expected GRANT OPTION after WITH")
		}
		adp.GrantOption = true
	}
	if adp.Revoke {
		p.consumeWord("cascade")
		p.consumeWord("restrict")
	}
	return adp, nil
}

// parseSignedInt parses an optionally negative integer literal (CONNECTION
// LIMIT -1).
func (p *parser) parseSignedInt(what string) (int64, error) {
	neg := p.consumeOp("-")
	t := p.peek()
	if t.kind != tkNumber {
		return 0, p.errf("%s takes an integer, found %q", what, t.text)
	}
	n, err := strconv.ParseInt(t.text, 10, 32)
	if err != nil {
		return 0, p.errf("%s takes an integer, found %q", what, t.text)
	}
	p.i++
	if neg {
		n = -n
	}
	return n, nil
}
