package parser

import (
	"strings"
	"testing"
)

// TestParseDepthLimit (issue #135): nesting past maxParseDepth is a
// syntax error, not a stack overflow; nesting below it parses. Every
// recursive shape — parentheses, scalar subqueries, derived tables,
// CASE — is bounded by the one counter.
func TestParseDepthLimit(t *testing.T) {
	deep := func(n int) string { return "SELECT " + strings.Repeat("(", n) + "1" + strings.Repeat(")", n) }
	if _, err := Parse(deep(maxParseDepth / 2)); err != nil {
		t.Fatalf("%d levels of parentheses: %v", maxParseDepth/2, err)
	}
	for _, n := range []int{maxParseDepth + 10, 20000, 120000} {
		_, err := Parse(deep(n))
		if err == nil || !strings.Contains(err.Error(), "nests too deeply") {
			t.Fatalf("%d levels of parentheses: %v, want the nesting error", n, err)
		}
	}
	subq := "SELECT " + strings.Repeat("(SELECT ", 5000) + "1" + strings.Repeat(")", 5000)
	if _, err := Parse(subq); err == nil || !strings.Contains(err.Error(), "nests too deeply") {
		t.Fatalf("5,000 nested scalar subqueries: %v", err)
	}
	var sb strings.Builder
	sb.WriteString("SELECT * FROM ")
	for i := 0; i < 5000; i++ {
		sb.WriteString("(SELECT * FROM ")
	}
	sb.WriteString("t")
	for i := 0; i < 5000; i++ {
		sb.WriteString(") AS d")
	}
	if _, err := Parse(sb.String()); err == nil || !strings.Contains(err.Error(), "nests too deeply") {
		t.Fatalf("5,000 nested derived tables: %v", err)
	}
	cases := "SELECT " + strings.Repeat("CASE WHEN true THEN ", 5000) + "1" + strings.Repeat(" END", 5000)
	if _, err := Parse(cases); err == nil || !strings.Contains(err.Error(), "nests too deeply") {
		t.Fatalf("5,000 nested CASEs: %v", err)
	}
	// The counter unwinds: a deep failure leaves the parser usable for
	// the next statement of the same input, and ordinary statements are
	// untouched.
	if _, err := Parse("SELECT 1; SELECT (2)"); err != nil {
		t.Fatal(err)
	}
}
