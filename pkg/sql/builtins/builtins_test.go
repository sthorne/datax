package builtins

import (
	"os"
	"testing"

	"github.com/sthorne/datax/pkg/sql/types"
)

// TestReferenceCurrent: docs/user/functions.md is what the registry
// renders (regenerate with go generate ./pkg/sql/builtins).
func TestReferenceCurrent(t *testing.T) {
	got, err := os.ReadFile("../../../docs/user/functions.md")
	if err != nil {
		t.Fatalf("read the reference: %v", err)
	}
	if string(got) != Reference() {
		t.Fatal("docs/user/functions.md is stale: run `go generate ./pkg/sql/builtins`")
	}
}

// TestRegistryShape: every builtin has a category, a doc, a signature
// the arity check agrees with, and aliases that resolve to the same
// function.
func TestRegistryShape(t *testing.T) {
	for _, b := range All() {
		if b.Doc == "" || b.Category == "" {
			t.Errorf("%s: missing doc or category", b.Name)
		}
		if b.MinArgs > len(b.Args) {
			t.Errorf("%s: MinArgs %d exceeds %d declared arguments", b.Name, b.MinArgs, len(b.Args))
		}
		if !b.ArityOK(b.MinArgs) || (!b.Variadic && b.ArityOK(len(b.Args)+1)) {
			t.Errorf("%s: arity check disagrees with the declaration", b.Name)
		}
		if b.Session != (b.Fn == nil) {
			t.Errorf("%s: session functions carry no implementation, the rest must", b.Name)
		}
		for _, a := range b.Aliases {
			alias, ok := Lookup(a)
			if !ok || alias.Doc != b.Doc || !alias.Hidden {
				t.Errorf("%s: alias %s does not resolve to it", b.Name, a)
			}
		}
	}
}

// TestCallStrictness: a strict function sees no NULL; a lenient one
// does; coercion follows the declared families.
func TestCallStrictness(t *testing.T) {
	up, _ := Lookup("upper")
	if d, err := up.Call([]types.Datum{types.DNull}); err != nil || !d.Null {
		t.Fatalf("strict NULL: %v %v", d, err)
	}
	if d, err := up.Call([]types.Datum{types.NewInt(7)}); err != nil || d.S != "7" {
		t.Fatalf("int rendered as text: %v %v", d, err)
	}
	co, _ := Lookup("coalesce")
	if d, err := co.Call([]types.Datum{types.DNull, types.NewInt(3)}); err != nil || d.I != 3 {
		t.Fatalf("lenient: %v %v", d, err)
	}
	if _, err := up.Call(nil); err == nil {
		t.Fatal("arity not checked")
	}
	rd, _ := Lookup("round")
	if d, err := rd.Call([]types.Datum{types.NewString("2.5")}); err != nil || d.Fam != types.Decimal || d.S != "3" {
		t.Fatalf("round of text: %v %v", d, err)
	}
}

// TestIntervals: parsing, rendering and calendar arithmetic.
func TestIntervals(t *testing.T) {
	for in, want := range map[string]string{
		"1 day":                  "1 day",
		"2 hours 30 minutes":     "02:30:00",
		"1 year 2 months 3 days": "1 year 2 mons 3 days",
		"3 weeks":                "21 days",
		"1 day 02:03:04.5":       "1 day 02:03:04.5",
		"-1 day":                 "-1 days",
		"1.5 hours":              "01:30:00",
		"P1Y2M3DT4H5M6S":         "1 year 2 mons 3 days 04:05:06",
		"90 minutes":             "01:30:00",
		"2 days ago":             "-2 days",
		"1 millennium 1 century": "1100 years",
		"00:00:00":               "00:00:00",
	} {
		iv, err := ParseInterval(in)
		if err != nil {
			t.Errorf("%q: %v", in, err)
			continue
		}
		if got := iv.String(); got != want {
			t.Errorf("%q → %q, want %q", in, got, want)
		}
	}
	if _, err := ParseInterval("3 fortnights"); err == nil {
		t.Error("unknown unit accepted")
	}
}
