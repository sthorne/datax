package parser

import "strings"

// CREATE DATABASE's PostgreSQL options and ALTER DATABASE's forms
// (PG-D-022). The parser records what was written; the executor decides
// what datax can honour.

// DatabaseOption is one CREATE DATABASE option as written: its name
// lower-cased (CONNECTION LIMIT is "connection_limit"), its value as
// text, Default for the DEFAULT keyword.
type DatabaseOption struct {
	Name    string
	Value   string
	Default bool
}

// databaseOptionNames are PostgreSQL 16's CREATE DATABASE options (and
// OID, accepted by its grammar).
var databaseOptionNames = map[string]bool{
	"owner": true, "template": true, "encoding": true, "strategy": true, "locale": true,
	"lc_collate": true, "lc_ctype": true, "icu_locale": true, "icu_rules": true,
	"locale_provider": true, "collation_version": true, "tablespace": true,
	"allow_connections": true, "is_template": true, "oid": true,
}

// isPlacementOption reports whether the next token begins datax's
// placement option list (replicas = n, constraints = ...).
func (p *parser) isPlacementOption() bool {
	t := p.peek()
	if t.kind == tkOp && t.text == "(" {
		return true
	}
	if t.kind != tkIdent || t.quoted {
		return false
	}
	switch t.text {
	case "replicas", "replication_factor", "num_replicas", "constraints":
		return true
	}
	return false
}

// parseCreateDatabaseOptions parses CREATE DATABASE's option list after
// the name (and an optional WITH): PostgreSQL's options, each NAME [=]
// value, and datax's placement list, in any mix.
func (p *parser) parseCreateDatabaseOptions(cd *CreateDatabase) error {
	for !p.atStatementEnd() {
		if p.isPlacementOption() {
			if cd.Placement != nil {
				return p.errf("placement options given twice")
			}
			opts, err := p.parsePlacementOptions()
			if err != nil {
				return err
			}
			cd.Placement = opts
			continue
		}
		t := p.peek()
		if t.kind != tkIdent && t.kind != tkKeyword {
			return p.errf("unexpected %q in CREATE DATABASE", t.text)
		}
		name := strings.ToLower(t.text)
		p.i++
		if name == "connection" {
			if !p.consumeWord("limit") {
				return p.errf("expected LIMIT after CONNECTION")
			}
			name = "connection_limit"
		} else if !databaseOptionNames[name] {
			return &SyntaxError{Msg: `option "` + name + `" not recognized`, Pos: t.pos, Code: "42601"}
		}
		p.consumeOp("=")
		opt, err := p.parseDatabaseOptionValue(name)
		if err != nil {
			return err
		}
		for _, o := range cd.Options {
			if o.Name == name {
				return &SyntaxError{Msg: "conflicting or redundant options", Pos: t.pos, Code: "42601"}
			}
		}
		cd.Options = append(cd.Options, opt)
	}
	return nil
}

// parseDatabaseOptionValue reads one option's value: a name, a string,
// a (signed) number or DEFAULT.
func (p *parser) parseDatabaseOptionValue(name string) (DatabaseOption, error) {
	opt := DatabaseOption{Name: name}
	neg := p.consumeOp("-")
	v := p.peek()
	switch {
	case v.kind == tkNumber:
		opt.Value = v.text
		if neg {
			opt.Value = "-" + v.text
		}
	case neg:
		return opt, p.errf("%s takes a value, found %q", strings.ToUpper(name), v.text)
	case v.kind == tkKeyword && v.text == "DEFAULT" || v.isWord("default"):
		opt.Default = true
	case v.kind == tkString, v.kind == tkIdent:
		opt.Value = v.text
	case v.kind == tkKeyword:
		opt.Value = strings.ToLower(v.text)
	default:
		return opt, p.errf("%s takes a value, found %q", strings.ToUpper(strings.ReplaceAll(name, "_", " ")), v.text)
	}
	p.i++
	return opt, nil
}

// parseAlterDatabaseTail parses what follows ALTER DATABASE name besides
// RENAME TO and OWNER TO:
//
//	SET (placement options) | SET replicas = n ...
//	SET param {TO | =} {value | DEFAULT} | SET param FROM CURRENT
//	RESET param | RESET ALL
//	[WITH] CONNECTION LIMIT n | ALLOW_CONNECTIONS b | IS_TEMPLATE b
//	SET TABLESPACE t | REFRESH COLLATION VERSION
func (p *parser) parseAlterDatabaseTail(name string) (Statement, error) {
	ad := &AlterDatabase{Name: name}
	switch {
	case p.peek().kind == tkKeyword && p.peek().text == "SET":
		if p.i+1 < len(p.toks) {
			p.i++
			if p.isPlacementOption() {
				opts, err := p.parsePlacementOptions()
				if err != nil {
					return nil, err
				}
				ad.Placement = opts
				return ad, nil
			}
			if p.consumeIdentWord("tablespace") {
				ts, err := p.expectIdent()
				if err != nil {
					return nil, err
				}
				ad.Options = append(ad.Options, DatabaseOption{Name: "tablespace", Value: ts})
				return ad, nil
			}
			p.i-- // parseSet consumes SET
		}
		st, err := p.parseSet()
		if err != nil {
			return nil, err
		}
		sv, ok := st.(*SetVar)
		if !ok || sv.Local || sv.Name == "" {
			return nil, p.errf("ALTER DATABASE ... SET takes a configuration parameter")
		}
		ad.Set = sv
		return ad, nil
	case p.consumeIdentWord("reset"):
		sv := &SetVar{Reset: true}
		if !p.consumeIdentWord("all") {
			n, err := p.parseSettingName()
			if err != nil {
				return nil, err
			}
			sv.Name = n
		}
		ad.Set = sv
		return ad, nil
	case p.peekIdentSeq("refresh", "collation", "version"):
		p.i += 3
		ad.RefreshCollation = true
		return ad, nil
	}
	p.consumeIdentWord("with")
	for !p.atStatementEnd() {
		t := p.peek()
		var opt string
		switch {
		case p.consumeIdentWord("connection"):
			if !p.consumeWord("limit") {
				return nil, p.errf("expected LIMIT after CONNECTION")
			}
			opt = "connection_limit"
		case p.consumeIdentWord("allow_connections"):
			opt = "allow_connections"
		case p.consumeIdentWord("is_template"):
			opt = "is_template"
		default:
			if t.kind == tkIdent || t.kind == tkKeyword {
				return nil, &SyntaxError{Msg: `option "` + strings.ToLower(t.text) + `" not recognized`, Pos: t.pos, Code: "42601"}
			}
			return nil, p.errf("ALTER DATABASE supports RENAME TO, OWNER TO, SET, RESET, CONNECTION LIMIT, ALLOW_CONNECTIONS, IS_TEMPLATE and SET (placement)")
		}
		p.consumeOp("=")
		o, err := p.parseDatabaseOptionValue(opt)
		if err != nil {
			return nil, err
		}
		ad.Options = append(ad.Options, o)
	}
	if len(ad.Options) == 0 {
		return nil, p.errf("ALTER DATABASE supports RENAME TO, OWNER TO, SET, RESET, CONNECTION LIMIT, ALLOW_CONNECTIONS, IS_TEMPLATE and SET (placement)")
	}
	return ad, nil
}

// parseSettingName parses a configuration parameter's name: a word, or
// a dotted custom name (app.tenant).
func (p *parser) parseSettingName() (string, error) {
	t := p.peek()
	if t.kind != tkIdent && t.kind != tkKeyword {
		return "", p.errf("expected a configuration parameter, found %q", t.text)
	}
	p.i++
	name := strings.ToLower(t.text)
	if t.quoted {
		name = t.text
	}
	for p.peek().kind == tkOp && p.peek().text == "." {
		p.i++
		n := p.peek()
		if n.kind != tkIdent && n.kind != tkKeyword {
			return "", p.errf("expected a configuration parameter, found %q", n.text)
		}
		p.i++
		name += "." + strings.ToLower(n.text)
	}
	return name, nil
}
