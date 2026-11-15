package builtins

import (
	"math"
	"testing"

	"github.com/sthorne/datax/pkg/sql/types"
)

func vec(t *testing.T, s string) types.Datum {
	t.Helper()
	d, err := types.ParseVector(s)
	if err != nil {
		t.Fatal(err)
	}
	return d
}

func code(err error) string {
	if be, ok := err.(*Error); ok {
		return be.Code
	}
	return ""
}

// TestVectorOperators: each operator against hand-computed values, with
// a vector, a text literal or an array on either side; a dimension
// mismatch is a data exception, as in pgvector.
func TestVectorOperators(t *testing.T) {
	a, b := vec(t, "[1,2,3]"), vec(t, "[4,6,3]")
	for _, tc := range []struct {
		op   string
		l, r types.Datum
		want float64
	}{
		{"<->", a, b, 5},
		{"<#>", a, b, -25},
		{"<=>", a, b, 1 - 25/(math.Sqrt(14)*math.Sqrt(61))},
		{"<->", a, types.NewString("[4,6,3]"), 5},
		{"<->", types.NewString("[1,2,3]"), b, 5},
		{"<#>", a, types.NewArray(types.Int, []types.Datum{types.NewInt(4), types.NewInt(6), types.NewInt(3)}), -25},
		{"<=>", vec(t, "[1,0]"), vec(t, "[0,1]"), 1},
		{"<=>", vec(t, "[2,0]"), vec(t, "[-1,0]"), 2},
	} {
		d, ok, err := VectorOp(tc.l, tc.op, tc.r)
		if !ok || err != nil || d.Fam != types.Float || math.Abs(d.F-tc.want) > 1e-12 {
			t.Errorf("%s %s %s = %v (ok %v, err %v), want %v", tc.l.Text(), tc.op, tc.r.Text(), d.F, ok, err, tc.want)
		}
	}
	if d, _, err := VectorOp(vec(t, "[0,0]"), "<=>", vec(t, "[1,1]")); err != nil || !math.IsNaN(d.F) {
		t.Errorf("cosine distance to the zero vector: %v, %v", d.F, err)
	}
	if _, _, err := VectorOp(a, "<->", vec(t, "[1,2]")); code(err) != CodeDataException || err.Error() != "different vector dimensions 3 and 2" {
		t.Errorf("dimension mismatch: %v", err)
	}
	if _, _, err := VectorOp(a, "<->", types.NewString("[1,NaN,3]")); code(err) != CodeInvalidText {
		t.Errorf("NaN in a literal operand: %v", err)
	}
	if _, _, err := VectorOp(a, "<->", types.NewInt(3)); err == nil {
		t.Error("a distance to an integer was computed")
	}
	if d, ok, err := VectorOp(a, "+", b); !ok || err != nil || d.Text() != "[5,8,6]" {
		t.Errorf("[1,2,3] + [4,6,3] = %s (%v)", d.Text(), err)
	}
	if d, ok, err := VectorOp(a, "-", b); !ok || err != nil || d.Text() != "[-3,-4,0]" {
		t.Errorf("[1,2,3] - [4,6,3] = %s (%v)", d.Text(), err)
	}
	if _, _, err := VectorOp(vec(t, "[3e38]"), "+", vec(t, "[3e38]")); code(err) != CodeOutOfRange {
		t.Errorf("overflowing sum: %v", err)
	}
	if _, ok, err := VectorOp(a, "*", b); !ok || err == nil {
		t.Error("vector * vector was not refused")
	}
	if _, ok, _ := VectorOp(types.NewInt(1), "+", types.NewInt(2)); ok {
		t.Error("integer addition was claimed as a vector operator")
	}
}

// TestVectorFunctions: the functions agree with the operators and with
// values worked by hand; NULL in, NULL out.
func TestVectorFunctions(t *testing.T) {
	call := func(name string, args ...types.Datum) types.Datum {
		t.Helper()
		b, ok := Lookup(name)
		if !ok {
			t.Fatalf("no builtin %s", name)
		}
		d, err := b.Call(args)
		if err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		return d
	}
	a, b := vec(t, "[1,2,3]"), vec(t, "[4,6,3]")
	if d := call("l2_distance", a, b); d.F != 5 {
		t.Errorf("l2_distance = %v", d.F)
	}
	if d := call("inner_product", a, b); d.F != 25 {
		t.Errorf("inner_product = %v", d.F)
	}
	if d := call("cosine_distance", a, types.NewString("[4,6,3]")); math.Abs(d.F-(1-25/(math.Sqrt(14)*math.Sqrt(61)))) > 1e-12 {
		t.Errorf("cosine_distance = %v", d.F)
	}
	if d := call("l1_distance", a, b); d.F != 7 {
		t.Errorf("l1_distance = %v", d.F)
	}
	if d := call("vector_dims", a); d.Fam != types.Int || d.I != 3 {
		t.Errorf("vector_dims = %+v", d)
	}
	if d := call("vector_norm", vec(t, "[3,4]")); d.F != 5 {
		t.Errorf("vector_norm = %v", d.F)
	}
	if d := call("l2_normalize", vec(t, "[3,4]")); d.Text() != "[0.6,0.8]" {
		t.Errorf("l2_normalize = %s", d.Text())
	}
	if d := call("l2_distance", a, types.DNull); !d.Null {
		t.Errorf("l2_distance with NULL = %+v", d)
	}
	bi, _ := Lookup("l2_distance")
	if _, err := bi.Call([]types.Datum{a, vec(t, "[1]")}); code(err) != CodeDataException {
		t.Errorf("l2_distance across dimensions: %v", err)
	}
	// The casts.
	if d, err := Cast(types.NewString("[1,2,3]"), "vector(3)"); err != nil || d.Fam != types.Vector {
		t.Errorf("text::vector(3): %+v, %v", d, err)
	}
	if _, err := Cast(types.NewString("[1,2,3]"), "vector(4)"); code(err) != CodeDataException || err.Error() != "expected 4 dimensions, not 3" {
		t.Errorf("text::vector(4): %v", err)
	}
	if d, err := Cast(types.NewArray(types.Float, []types.Datum{types.NewFloat(1.5), types.NewFloat(2)}), "vector"); err != nil || d.Text() != "[1.5,2]" {
		t.Errorf("float8[]::vector: %s, %v", d.Text(), err)
	}
	if d, err := Cast(a, "text"); err != nil || d.S != "[1,2,3]" {
		t.Errorf("vector::text: %+v, %v", d, err)
	}
	if d, err := Cast(a, "float8[]"); err != nil || d.Text() != "{1,2,3}" {
		t.Errorf("vector::float8[]: %s, %v", d.Text(), err)
	}
	if _, err := Cast(types.NewString("[1,inf]"), "vector"); code(err) != CodeInvalidText {
		t.Errorf("'[1,inf]'::vector: %v", err)
	}
}
