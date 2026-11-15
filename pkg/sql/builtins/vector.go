package builtins

import (
	"math"

	"github.com/sthorne/datax/pkg/sql/types"
	"github.com/sthorne/datax/pkg/sql/vector"
)

const catVector = "Vectors"

// CodeDataException is what pgvector raises for two vectors of
// different dimensions, and for a value a vector cannot hold.
const CodeDataException = "22000"

// asVector reads an operand as a vector: a vector itself, text in the
// '[1,2,3]' form (a literal or a parameter sent as text), or a numeric
// array.
func asVector(d types.Datum) ([]float32, error) {
	switch {
	case d.Fam == types.Vector:
		return d.VectorVal(), nil
	case d.Fam == types.String:
		v, err := vector.Parse(d.S)
		if err != nil {
			return nil, errf(CodeInvalidText, "%v", err)
		}
		return v, nil
	case d.Fam.IsArray():
		c, err := types.VectorFromArray(d)
		if err != nil {
			return nil, errf(CodeDataException, "%v", err)
		}
		return c.VectorVal(), nil
	}
	return nil, errf(CodeUndefined, "a %s value is not a vector", d.Fam)
}

// vectorPair reads both operands and refuses a dimension mismatch.
func vectorPair(l, r types.Datum) (a, b []float32, err error) {
	if a, err = asVector(l); err != nil {
		return nil, nil, err
	}
	if b, err = asVector(r); err != nil {
		return nil, nil, err
	}
	if err := vector.Dims(a, b); err != nil {
		return nil, nil, errf(CodeDataException, "%v", err)
	}
	return a, b, nil
}

// IsVectorOp reports whether op is one of the distance operators.
func IsVectorOp(op string) bool {
	_, ok := vector.MetricForOperator(op)
	return ok
}

// VectorOp evaluates an operator with a vector operand: the three
// distances (<-> Euclidean, <=> cosine, <#> negative inner product,
// each with a vector, a text literal or an array on either side) and
// element-wise + and - between two vectors. ok is false when the
// operator is not a vector one for these operands.
func VectorOp(l types.Datum, op string, r types.Datum) (d types.Datum, ok bool, err error) {
	if m, isDist := vector.MetricForOperator(op); isDist {
		if l.Fam == types.Vector && r.Fam == types.Vector && len(l.S) == len(r.S) {
			// Two stored vectors of one dimension: the case a scan meets
			// once per row.
			return f64(m.StoredDistance(l.S, r.S)), true, nil
		}
		a, b, err := vectorPair(l, r)
		if err != nil {
			return types.Datum{}, true, err
		}
		return f64(m.Distance(a, b)), true, nil
	}
	if l.Fam != types.Vector && r.Fam != types.Vector {
		return types.Datum{}, false, nil
	}
	if op != "+" && op != "-" {
		return types.Datum{}, true, errf(CodeUndefined, "operator %s is not defined for vectors (the distance operators <->, <=> and <#>, and + and -, are)", op)
	}
	a, b, err := vectorPair(l, r)
	if err != nil {
		return types.Datum{}, true, err
	}
	out := make([]float32, len(a))
	for i := range a {
		x := float64(a[i]) + float64(b[i])
		if op == "-" {
			x = float64(a[i]) - float64(b[i])
		}
		if math.Abs(x) > math.MaxFloat32 {
			return types.Datum{}, true, errf(CodeOutOfRange, "value out of range: overflow")
		}
		out[i] = float32(x)
	}
	return types.NewVector(out), true, nil
}

func init() {
	dist := func(name, doc string, fn func(a, b []float32) float64) {
		register(&Builtin{Name: name, Args: []types.Family{types.Vector, types.Vector}, MinArgs: 2, Ret: types.Float, Category: catVector, Doc: doc,
			Fn: func(args []types.Datum) (types.Datum, error) {
				a, b, err := vectorPair(args[0], args[1])
				if err != nil {
					return types.Datum{}, err
				}
				return f64(fn(a, b)), nil
			}})
	}
	dist("l2_distance", "Euclidean distance between two vectors of the same dimension (the `<->` operator).",
		func(a, b []float32) float64 { return math.Sqrt(vector.L2Squared(a, b)) })
	dist("cosine_distance", "One minus the cosine of the angle between two vectors (the `<=>` operator); NaN when either is the zero vector.",
		vector.CosineDistance)
	dist("inner_product", "Inner (dot) product of two vectors; the `<#>` operator is its negation, so that smaller is nearer.",
		vector.Dot)
	dist("l1_distance", "Taxicab distance between two vectors: the sum of the absolute element differences.",
		vector.L1)
	register(&Builtin{Name: "vector_dims", Args: []types.Family{types.Vector}, MinArgs: 1, Ret: types.Int, Category: catVector,
		Doc: "The number of elements of a vector.",
		Fn: func(args []types.Datum) (types.Datum, error) {
			return types.NewInt(int64(args[0].VectorDims())), nil
		}})
	register(&Builtin{Name: "vector_norm", Args: []types.Family{types.Vector}, MinArgs: 1, Ret: types.Float, Category: catVector,
		Doc: "The Euclidean length of a vector.",
		Fn: func(args []types.Datum) (types.Datum, error) {
			return f64(vector.Norm(args[0].VectorVal())), nil
		}})
	register(&Builtin{Name: "l2_normalize", Args: []types.Family{types.Vector}, MinArgs: 1, Ret: types.Vector, Category: catVector,
		Doc: "The vector scaled to unit length (the zero vector stays zero).",
		Fn: func(args []types.Datum) (types.Datum, error) {
			return types.NewVector(vector.Normalize(args[0].VectorVal())), nil
		}})
}
