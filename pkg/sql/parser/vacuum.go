package parser

import "strings"

// vacuumBoolOptions are VACUUM's Boolean options (PostgreSQL 16's
// ExecVacuum); vacuumValueOptions take a value of another kind.
var vacuumBoolOptions = map[string]bool{
	"verbose": true, "analyze": true, "freeze": true, "full": true,
	"disable_page_skipping": true, "skip_locked": true, "process_main": true,
	"process_toast": true, "truncate": true, "skip_database_stats": true,
	"only_database_stats": true,
}

var vacuumValueOptions = map[string]bool{
	"index_cleanup": true, "parallel": true, "buffer_usage_limit": true,
}

// parseVacuum parses VACUUM, both the parenthesized option list and the
// older keyword form:
//
//	VACUUM [FULL] [FREEZE] [VERBOSE] [ANALYZE] [table [(col, ...)], ...]
//	VACUUM (option [value], ...) [table [(col, ...)], ...]
func (p *parser) parseVacuum() (Statement, error) {
	p.i++ // VACUUM
	v := &Vacuum{}
	if p.consumeOp("(") {
		for {
			t := p.peek()
			if t.kind != tkIdent && t.kind != tkKeyword {
				return nil, p.errf("expected a VACUUM option, found %q", t.text)
			}
			name := strings.ToLower(t.text)
			p.i++
			hasValue := !(p.peek().kind == tkOp && (p.peek().text == "," || p.peek().text == ")"))
			var value token
			if hasValue {
				value = p.peek()
				p.i++
			}
			switch {
			case vacuumBoolOptions[name]:
				on := true
				if hasValue {
					b, ok := vacuumBool(value)
					if !ok {
						return nil, &SyntaxError{Msg: name + " requires a Boolean value", Pos: value.pos, Code: "42601"}
					}
					on = b
				}
				switch name {
				case "full":
					v.Full = on
				case "freeze":
					v.Freeze = on
				case "verbose":
					v.Verbose = on
				case "analyze":
					v.Analyze = on
				}
			case vacuumValueOptions[name]:
				if !hasValue && name != "index_cleanup" {
					return nil, &SyntaxError{Msg: name + " requires a value", Pos: t.pos, Code: "42601"}
				}
			default:
				return nil, &SyntaxError{Msg: `unrecognized VACUUM option "` + name + `"`, Pos: t.pos, Code: "42601"}
			}
			if p.consumeOp(")") {
				break
			}
			if err := p.expectOp(","); err != nil {
				return nil, err
			}
		}
	} else {
		// The keyword form takes its options in this order only.
		v.Full = p.consumeIdentWord("full")
		v.Freeze = p.consumeIdentWord("freeze")
		v.Verbose = p.consumeIdentWord("verbose")
		v.Analyze = p.consumeIdentWord("analyze")
		if v.Analyze && !v.Verbose {
			// VACUUM ANALYZE VERBOSE t is ANALYZE VERBOSE t vacuumed.
			v.Verbose = p.consumeIdentWord("verbose")
		}
	}
	for !p.atStatementEnd() {
		name, err := p.parseTableName()
		if err != nil {
			return nil, err
		}
		vt := VacuumTable{Name: name}
		if p.consumeOp("(") {
			for {
				col, err := p.expectIdent()
				if err != nil {
					return nil, err
				}
				vt.Columns = append(vt.Columns, col)
				if p.consumeOp(")") {
					break
				}
				if err := p.expectOp(","); err != nil {
					return nil, err
				}
			}
			if !v.Analyze {
				return nil, &SyntaxError{Msg: "ANALYZE option must be specified when a column list is provided", Code: "0A000"}
			}
		}
		v.Tables = append(v.Tables, vt)
		if !p.consumeOp(",") {
			break
		}
	}
	return v, nil
}

// vacuumBool reads a Boolean option value as PostgreSQL's defGetBoolean
// does: true/false, on/off, 1/0 (case-insensitive).
func vacuumBool(t token) (bool, bool) {
	switch strings.ToLower(t.text) {
	case "true", "on", "1":
		return true, true
	case "false", "off", "0":
		return false, true
	}
	return false, false
}
