package builtins

import (
	"testing"

	"github.com/sthorne/datax/pkg/sql/types"
)

// TestConcatJSONB: jsonb || jsonb as PostgreSQL's jsonb_concat
// (issue #342).
func TestConcatJSONB(t *testing.T) {
	for _, c := range []struct{ l, r, want string }{
		{`{"a":1}`, `{"b":2}`, `{"a":1,"b":2}`},
		{`{"a":1,"b":2}`, `{"b":3}`, `{"a":1,"b":3}`}, // the right side wins
		{`{"a":{"x":1}}`, `{"a":{"y":2}}`, `{"a":{"y":2}}`},
		{`{}`, `{}`, `{}`},
		{`[1,2]`, `[3]`, `[1,2,3]`},
		{`[]`, `[]`, `[]`},
		{`[1]`, `2`, `[1,2]`},
		{`[1]`, `{"a":1}`, `[1,{"a":1}]`},
		{`"x"`, `[1]`, `["x",1]`},
		{`{"a":1}`, `[1]`, `[{"a":1},1]`},
		{`1`, `"y"`, `[1,"y"]`},
		{`{"a":1}`, `2`, `[{"a":1},2]`},
		{`null`, `null`, `[null,null]`},
		{`{"n":9007199254740993}`, `{"m":1.50}`, `{"m":1.50,"n":9007199254740993}`},
	} {
		l, err := types.ParseJSONB(c.l)
		if err != nil {
			t.Fatal(err)
		}
		r, err := types.ParseJSONB(c.r)
		if err != nil {
			t.Fatal(err)
		}
		got, err := ConcatJSONB(l, r)
		if err != nil {
			t.Fatalf("%s || %s: %v", c.l, c.r, err)
		}
		if got.Fam != types.Jsonb || got.S != c.want {
			t.Errorf("%s || %s = %s %q, want jsonb %q", c.l, c.r, got.Fam, got.S, c.want)
		}
	}
}

// TestByteaStringFunctions: length, octet_length, substring, position,
// strpos and md5 over a bytea read its bytes, not its hex text (issue
// #344); over text they are unchanged.
func TestByteaStringFunctions(t *testing.T) {
	b := types.NewBytes([]byte{1, 2, 3, 4})
	call := func(name string, args ...types.Datum) types.Datum {
		t.Helper()
		fn, ok := Lookup(name)
		if !ok {
			t.Fatalf("no builtin %s", name)
		}
		d, err := fn.Call(args)
		if err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		return d
	}
	for _, name := range []string{"length", "octet_length"} {
		if d := call(name, b); d.I != 4 {
			t.Errorf("%s(bytea) = %d, want 4", name, d.I)
		}
	}
	if d := call("length", types.NewString("héllo")); d.I != 5 {
		t.Errorf("length(text) = %d, want 5", d.I)
	}
	if d := call("octet_length", types.NewString("héllo")); d.I != 6 {
		t.Errorf("octet_length(text) = %d, want 6", d.I)
	}
	for _, c := range []struct {
		args []types.Datum
		want []byte
	}{
		{[]types.Datum{b, types.NewInt(2), types.NewInt(2)}, []byte{2, 3}},
		{[]types.Datum{b, types.NewInt(2)}, []byte{2, 3, 4}},
		{[]types.Datum{b, types.NewInt(0), types.NewInt(2)}, []byte{1}},
		{[]types.Datum{b, types.NewInt(9)}, []byte{}},
		{[]types.Datum{b, types.NewInt(3), types.NewInt(9223372036854775807)}, []byte{3, 4}},
	} {
		for _, name := range []string{"substring", "substr"} {
			d := call(name, c.args...)
			if d.Fam != types.Bytes || d.S != string(c.want) {
				t.Errorf("%s(%v) = %s %q, want bytea %q", name, c.args[1:], d.Fam, d.S, c.want)
			}
		}
	}
	if d := call("substring", types.NewString("héllo"), types.NewInt(2), types.NewInt(2)); d.Fam != types.String || d.S != "él" {
		t.Errorf("substring(text) = %s %q, want text \"él\"", d.Fam, d.S)
	}
	if fn, _ := Lookup("substring"); fn.ResultFamily([]types.Family{types.Bytes, types.Int}) != types.Bytes ||
		fn.ResultFamily([]types.Family{types.String, types.Int}) != types.String {
		t.Error("substring's result family does not follow its first argument")
	}
	if _, err := mustLookup(t, "substring").Call([]types.Datum{b, types.NewInt(1), types.NewInt(-1)}); err == nil {
		t.Error("substring(bytea) with a negative length: no error")
	}
	needle := types.NewBytes([]byte{3})
	if d := call("position", needle, b); d.I != 3 {
		t.Errorf("position(bytea in bytea) = %d, want 3", d.I)
	}
	if d := call("strpos", b, needle); d.I != 3 {
		t.Errorf("strpos(bytea, bytea) = %d, want 3", d.I)
	}
	// An untyped literal beside a bytea is read as bytea input.
	if d := call("position", types.NewString(`\x0304`), b); d.I != 3 {
		t.Errorf("position('\\x0304' in bytea) = %d, want 3", d.I)
	}
	if d := call("position", types.NewBytes([]byte{9}), b); d.I != 0 {
		t.Errorf("position(absent) = %d, want 0", d.I)
	}
	if d := call("md5", b); d.S != "08d6c05a21512a79a1dfeb9d2a8f262f" {
		t.Errorf("md5(bytea) = %s", d.S)
	}
}

func mustLookup(t *testing.T, name string) *Builtin {
	t.Helper()
	b, ok := Lookup(name)
	if !ok {
		t.Fatalf("no builtin %s", name)
	}
	return b
}
