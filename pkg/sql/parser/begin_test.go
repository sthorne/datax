package parser

import (
	"fmt"
	"testing"
)

// TestParseBeginModes (issue #346): BEGIN [WORK | TRANSACTION] and START
// TRANSACTION take SET TRANSACTION's modes, carried as the SET
// TRANSACTION statements they stand for.
func TestParseBeginModes(t *testing.T) {
	modes := func(src string) string {
		t.Helper()
		b, ok := parseOne(t, src).(*Begin)
		if !ok {
			t.Fatalf("%s: not a Begin", src)
		}
		out := ""
		for _, m := range b.Modes {
			if !m.Local {
				t.Fatalf("%s: mode %s is not transaction-scoped", src, m.Name)
			}
			out += fmt.Sprintf("%s=%s;", m.Name, m.Value)
		}
		return out
	}
	for src, want := range map[string]string{
		`BEGIN`:                       "",
		`BEGIN WORK`:                  "",
		`BEGIN TRANSACTION`:           "",
		`START TRANSACTION`:           "",
		`BEGIN READ ONLY`:             "transaction_read_only=on;",
		`begin read write`:            "transaction_read_only=off;",
		`START TRANSACTION READ ONLY`: "transaction_read_only=on;",
		`BEGIN WORK ISOLATION LEVEL READ COMMITTED`:                                      "transaction_isolation=read committed;",
		`begin isolation level serializable read only`:                                   "transaction_isolation=serializable;transaction_read_only=on;",
		`begin isolation level repeatable read read only deferrable`:                     "transaction_isolation=repeatable read;transaction_read_only=on;",
		`BEGIN TRANSACTION ISOLATION LEVEL READ UNCOMMITTED, READ WRITE, NOT DEFERRABLE`: "transaction_isolation=read uncommitted;transaction_read_only=off;",
		`START TRANSACTION DEFERRABLE`:                                                   "",
		`BEGIN READ ONLY;`:                                                               "transaction_read_only=on;",
	} {
		if got := modes(src); got != want {
			t.Errorf("%s: modes %q, want %q", src, got, want)
		}
	}
	for _, bad := range []string{
		`BEGIN READ`,
		`BEGIN ISOLATION`,
		`BEGIN ISOLATION LEVEL`,
		`BEGIN ISOLATION LEVEL CHAOTIC`,
		`BEGIN ISOLATION LEVEL READ SOMETIMES`,
		`BEGIN NOT READ ONLY`,
		`START TRANSACTION WORK`,
		`START`,
		`BEGIN WORK TRANSACTION`,
		`SET TRANSACTION ISOLATION LEVEL READ SOMETIMES`,
	} {
		if _, err := Parse(bad); err == nil {
			t.Errorf("no error for %q", bad)
		}
	}
}
