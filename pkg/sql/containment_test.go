package sql

import (
	"testing"

	"github.com/sthorne/datax/pkg/sql/types"
)

func TestJSONBContains(t *testing.T) {
	for _, c := range []struct {
		left, right string
		want        bool
	}{
		// Objects: recursive key/value containment.
		{`{"a":1,"b":2}`, `{"a":1}`, true},
		{`{"a":1,"b":2}`, `{"a":2}`, false},
		{`{"a":1}`, `{"a":1,"b":2}`, false},
		{`{"a":{"b":{"c":3}},"x":1}`, `{"a":{"b":{"c":3}}}`, true},
		{`{"a":{"b":{"c":3}}}`, `{"a":{"b":{}}}`, true},
		{`{}`, `{}`, true},
		{`{"a":1}`, `{}`, true},
		// Arrays: every right element contained by some left element.
		{`[1,2,3]`, `[1,3]`, true},
		{`[1,2,3]`, `[3,1]`, true},
		{`[1,2,3]`, `[4]`, false},
		{`[1]`, `[]`, true},
		{`[]`, `[]`, true},
		{`[1,2,3]`, `[1,1,1]`, true}, // duplicates irrelevant
		// Nested arrays: recursive containment, not equality (PG doc
		// example: '[1, 2, [1, 3]]' @> '[[1, 3]]' is true).
		{`[1,2,[1,3]]`, `[[1,3]]`, true},
		{`[[1,3]]`, `[[1]]`, true},
		{`[[1,3]]`, `[[4]]`, false},
		// Top-level array contains a scalar — top level ONLY.
		{`[1,2]`, `1`, true},
		{`["a","b"]`, `"a"`, true},
		{`{"a":[1]}`, `{"a":1}`, false},
		{`1`, `[1]`, false},
		// Scalars: value equality; numbers numerically.
		{`1`, `1`, true},
		{`1`, `1.0`, true},
		{`100`, `1e2`, true},
		{`1.5`, `1.50`, true},
		{`1`, `2`, false},
		{`"x"`, `"x"`, true},
		{`"1"`, `1`, false}, // string never contains number
		{`true`, `true`, true},
		{`true`, `false`, false},
		{`null`, `null`, true},
		// Integers beyond float64 keep fidelity.
		{`{"n":9007199254740993}`, `{"n":9007199254740993}`, true},
		{`{"n":9007199254740993}`, `{"n":9007199254740992}`, false},
		// Objects inside arrays.
		{`[{"a":1,"b":2},{"c":3}]`, `[{"a":1}]`, true},
		{`[{"a":1}]`, `[{"a":1,"b":2}]`, false},
		// Mixed shapes never contain each other.
		{`{"a":1}`, `[1]`, false},
		{`[1]`, `{"a":1}`, false},
	} {
		l, err := types.ParseJSONB(c.left)
		if err != nil {
			t.Fatalf("parse %q: %v", c.left, err)
		}
		r, err := types.ParseJSONB(c.right)
		if err != nil {
			t.Fatalf("parse %q: %v", c.right, err)
		}
		got, err := jsonbContains(l, r)
		if err != nil {
			t.Fatalf("%s @> %s: %v", c.left, c.right, err)
		}
		if got != c.want {
			t.Fatalf("%s @> %s = %v, want %v", c.left, c.right, got, c.want)
		}
	}
}
