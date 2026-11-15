package types

import (
	"math"
	"strings"
	"testing"
)

// TestVectorDatum: a vector datum parses from text, renders back,
// converts to and from numeric arrays, and compares element by element.
func TestVectorDatum(t *testing.T) {
	d, err := NewString("[1, 2.5, -3]").Coerce(Vector)
	if err != nil {
		t.Fatal(err)
	}
	if d.Fam != Vector || d.VectorDims() != 3 || d.Text() != "[1,2.5,-3]" {
		t.Fatalf("parsed %+v (%s)", d, d.Text())
	}
	if len(d.S) != 12 {
		t.Fatalf("stored form is %d bytes, want 4 per element", len(d.S))
	}
	arr := NewArray(Int, []Datum{NewInt(1), NewInt(2)})
	fromArr, err := arr.Coerce(Vector)
	if err != nil || fromArr.Text() != "[1,2]" {
		t.Fatalf("array to vector: %s, %v", fromArr.Text(), err)
	}
	back, err := d.Coerce(ArrayOf(Float))
	if err != nil || back.Text() != "{1,2.5,-3}" {
		t.Fatalf("vector to float array: %s, %v", back.Text(), err)
	}
	if _, err := NewArray(Float, []Datum{NewFloat(1), DNull}).Coerce(Vector); err == nil {
		t.Error("an array with a NULL became a vector")
	}
	if _, err := NewArray(Float, []Datum{NewFloat(math.NaN())}).Coerce(Vector); err == nil {
		t.Error("an array with NaN became a vector")
	}
	if _, err := NewArray(Float, []Datum{NewFloat(1e300)}).Coerce(Vector); err == nil || !strings.Contains(err.Error(), "out of range") {
		t.Errorf("an array element beyond float4: %v", err)
	}
	if _, err := NewArray(Float, nil).Coerce(Vector); err == nil {
		t.Error("an empty array became a vector")
	}
	for _, bad := range []string{"[NaN]", "[]", "1,2", "[1e40]"} {
		if _, err := NewString(bad).Coerce(Vector); err == nil {
			t.Errorf("%q became a vector", bad)
		}
	}

	cmp := func(a, b string) int {
		t.Helper()
		da, _ := ParseVector(a)
		db, _ := ParseVector(b)
		c, err := da.Compare(db)
		if err != nil {
			t.Fatalf("compare %s %s: %v", a, b, err)
		}
		return c
	}
	if cmp("[1,2,3]", "[1,2,3]") != 0 || cmp("[1,2,3]", "[1,2,4]") != -1 || cmp("[2]", "[1,9,9]") != 1 || cmp("[1,2]", "[1,2,0]") != -1 {
		t.Error("vectors do not compare element by element, the shorter prefix first")
	}
	// Against a text literal the literal is read as a vector.
	if c, err := d.Compare(NewString("[1,2.5,-3]")); err != nil || c != 0 {
		t.Errorf("vector = text literal: %d, %v", c, err)
	}
	if _, err := d.Compare(NewInt(1)); err == nil {
		t.Error("a vector compared with an integer")
	}
	if IsIndexable(Vector) {
		t.Error("a vector has no ordered key encoding and must not claim one")
	}
	// The family number is on-disk format (descriptors store it): after
	// the text-search types.
	if Vector != 17 || Tsvector != 15 || Tsquery != 16 {
		t.Errorf("family numbers moved: Tsvector %d, Tsquery %d, Vector %d", Tsvector, Tsquery, Vector)
	}
	if f, err := ParseType("vector"); err != nil || f != Vector {
		t.Errorf("ParseType(vector) = %v, %v", f, err)
	}
	if _, err := ParseType("vector[]"); err == nil {
		t.Error("vector[] parsed as a type")
	}
}

// TestFloatNaNOrder: NaN sorts after every number and equals itself, so
// a sort over distances that include one is a total order.
func TestFloatNaNOrder(t *testing.T) {
	nan, one, inf := NewFloat(math.NaN()), NewFloat(1), NewFloat(math.Inf(1))
	for _, tc := range []struct {
		a, b Datum
		want int
	}{{nan, one, 1}, {one, nan, -1}, {nan, inf, 1}, {nan, nan, 0}, {one, inf, -1}, {one, one, 0}} {
		if c, err := tc.a.Compare(tc.b); err != nil || c != tc.want {
			t.Errorf("compare %v %v = %d (%v), want %d", tc.a.F, tc.b.F, c, err, tc.want)
		}
	}
}
