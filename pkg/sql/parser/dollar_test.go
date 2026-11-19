package parser

import (
	"errors"
	"testing"
)

// TestDollarQuotedStrings (PG-P-005): $$...$$ and $tag$...$tag$ are
// string literals wherever a string literal may stand, taken exactly as
// written, and kept apart from $n parameters as PostgreSQL's scanner
// keeps them.
func TestDollarQuotedStrings(t *testing.T) {
	for src, want := range map[string]string{
		`$$abc$$`:                           "abc",
		`$$$$`:                              "",
		`$$it's \n "x"$$`:                   `it's \n "x"`,
		`$tag$a $$ b$tag$`:                  "a $$ b",
		`$a_1$x$a_1$`:                       "x",
		`$t$ $T$ $t$`:                       " $T$ ",
		"$$line\nbreak; -- not a comment$$": "line\nbreak; -- not a comment",
		`$$$1$$`:                            "$1",
		`$é$ü$é$`:                           "ü",
	} {
		toks, err := lex(src)
		if err != nil {
			t.Errorf("%s: %v", src, err)
			continue
		}
		if len(toks) != 2 || toks[0].kind != tkString || toks[0].text != want {
			t.Errorf("%s: %+v, want one string %q", src, toks, want)
		}
	}

	// Parameters are untouched; an identifier may hold a $.
	toks, err := lex(`$1 $23::text a$b x$$y$$`)
	if err != nil {
		t.Fatal(err)
	}
	if toks[0].kind != tkParam || toks[0].text != "1" || toks[1].kind != tkParam || toks[1].text != "23" {
		t.Errorf("params: %+v", toks[:2])
	}
	if toks[4].kind != tkIdent || toks[4].text != "a$b" || toks[5].kind != tkIdent || toks[5].text != "x$$y$$" {
		t.Errorf("identifiers with $: %+v", toks[4:])
	}

	for src, msg := range map[string]string{
		`SELECT $1$`:          `trailing junk after parameter at or near "$1$"`,
		`SELECT $1abc`:        `trailing junk after parameter at or near "$1a"`,
		`SELECT $$abc`:        `unterminated dollar-quoted string at or near "$$abc"`,
		`SELECT $q$abc$Q$`:    `unterminated dollar-quoted string at or near "$q$abc$Q$"`,
		`SELECT $ 1`:          `syntax error at or near "$"`,
		`SELECT $a b$ 'x'`:    `syntax error at or near "$"`,
		`SELECT $1a$ abc$1a$`: `trailing junk after parameter at or near "$1a"`,
	} {
		_, err := Parse(src)
		var se *SyntaxError
		if !errors.As(err, &se) || se.Code != "42601" || se.Msg != msg {
			t.Errorf("%s: %v, want 42601 %s", src, err, msg)
		}
	}

	// In every expression context, and as a DDL operand.
	for _, src := range []string{
		`SELECT $$a$$ || $x$b$x$, length($$abc$$), $$1$$::int + 1`,
		`SELECT * FROM t WHERE s = $$it's$$ AND v IN ($$a$$, $1)`,
		`INSERT INTO t VALUES (1, $body$x;y$body$)`,
		`UPDATE t SET v = $$z$$ WHERE k = $1`,
		`CREATE TABLE t (k INT PRIMARY KEY, v TEXT DEFAULT $$d$$ CHECK (v <> $$bad$$))`,
		`COMMENT ON TABLE t IS $$a 'quoted' comment$$`,
		`SET application_name = $$app$$`,
	} {
		if _, err := Parse(src); err != nil {
			t.Errorf("%s: %v", src, err)
		}
	}
	// A statement separator inside a dollar quote does not split.
	stmts, err := Parse(`SELECT $$a;b$$; SELECT 2`)
	if err != nil || len(stmts) != 2 {
		t.Fatalf("two statements: %d %v", len(stmts), err)
	}
}
