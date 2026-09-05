package types

import (
	"reflect"
	"testing"
)

// TestArrays: the composite array family, literal parsing and
// rendering (quoting), coercion of elements, and ordering.
func TestArrays(t *testing.T) {
	f := ArrayOf(Int)
	if !f.IsArray() || f.Elem() != Int || f.String() != "INT8[]" || Int.IsArray() || Array.IsArray() {
		t.Fatalf("family: %v %v %s", f.IsArray(), f.Elem(), f)
	}
	for _, c := range []struct {
		name string
		want Family
	}{
		{"INT8[]", ArrayOf(Int)}, {"text[]", ArrayOf(String)}, {"int8[][]", ArrayOf(Int)}, {"INT ARRAY", ArrayOf(Int)}, {"timestamptz[]", ArrayOf(Timestamp)},
	} {
		if got, err := ParseType(c.name); err != nil || got != c.want {
			t.Errorf("ParseType(%q) = %v, %v; want %v", c.name, got, err, c.want)
		}
	}
	if _, err := ParseType("int8[][]x"); err == nil {
		t.Fatal("bad array type parsed")
	}

	for _, c := range []struct {
		lit  string
		elem Family
		n    int
		text string
	}{
		{"{1,2,3}", Int, 3, "{1,2,3}"},
		{"{}", Int, 0, "{}"},
		{" { 1 , NULL , 3 } ", Int, 3, "{1,NULL,3}"},
		{`{a,"b c",NULL,"NULL","",x\"y,"q\"r"}`, String, 7, `{a,"b c",NULL,"NULL","","x\"y","q\"r"}`},
		{`{"2024-01-02 03:04:05Z"}`, Timestamp, 1, `{"2024-01-02 03:04:05+00"}`},
		{`{t,f}`, Bool, 2, "{t,f}"},
		{`{1.50,2}`, Decimal, 2, "{1.5,2}"},
	} {
		d, err := ParseArray(c.lit, c.elem)
		if err != nil {
			t.Errorf("%q: %v", c.lit, err)
			continue
		}
		if d.Fam != ArrayOf(c.elem) || len(d.A) != c.n || d.Text() != c.text {
			t.Errorf("%q: %v %d %q, want %d %q", c.lit, d.Fam, len(d.A), d.Text(), c.n, c.text)
		}
	}
	for _, bad := range []string{"", "1,2", "{1,2", "{a,b}", `{"x}`} {
		if _, err := ParseArray(bad, Int); err == nil {
			t.Errorf("%q: parsed as INT8[]", bad)
		}
	}

	// Coercion: text to a typed array, a text-element array to a typed
	// one, an array to text.
	d, err := NewString("{1,2}").Coerce(ArrayOf(Int))
	if err != nil || d.A[1].I != 2 {
		t.Fatalf("text → INT8[]: %v %v", d, err)
	}
	if d, err = NewArray(String, []Datum{NewString("3"), DNull}).Coerce(ArrayOf(Float)); err != nil || d.A[0].F != 3 || !d.A[1].Null {
		t.Fatalf("TEXT[] → FLOAT8[]: %v %v", d, err)
	}
	if s, err := NewArray(Int, []Datum{NewInt(1)}).Coerce(String); err != nil || s.S != "{1}" {
		t.Fatalf("INT8[] → text: %v %v", s, err)
	}
	if _, err := NewString("{x}").Coerce(ArrayOf(Int)); err == nil {
		t.Fatal("{x} as INT8[]")
	}

	// Ordering: element by element, a prefix first, NULL last; a text
	// literal compares through the other side's family.
	cmp := func(a, b Datum) int {
		c, err := a.Compare(b)
		if err != nil {
			t.Fatalf("compare %v %v: %v", a, b, err)
		}
		return c
	}
	ints := func(v ...int64) Datum {
		out := make([]Datum, len(v))
		for i, x := range v {
			out[i] = NewInt(x)
		}
		return NewArray(Int, out)
	}
	if cmp(ints(1, 2), ints(1, 2)) != 0 || cmp(ints(1), ints(1, 2)) != -1 || cmp(ints(2), ints(1, 9)) != 1 ||
		cmp(ints(1, 2), NewString("{1,3}")) != -1 || cmp(NewArray(Int, []Datum{DNull}), ints(5)) != 1 {
		t.Fatal("array ordering")
	}
	if !reflect.DeepEqual(NewArray(Int, nil).A, []Datum(nil)) || NewArray(Int, nil).Text() != "{}" {
		t.Fatal("empty array")
	}
}
