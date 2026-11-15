package types

import (
	"fmt"
	"math"

	"github.com/sthorne/datax/pkg/sql/vector"
)

// A VECTOR datum (issue #304) keeps its elements in S as four
// big-endian bytes of IEEE-754 float32 apiece: the stored form, the
// payload of pgvector's binary wire form, and what an index entry
// carries are all the same bytes, so a value read from a row is handed
// to the wire or to a distance without being rebuilt. The dimension is
// len(S)/4. Two vectors are equal when their elements are.

// NewVector makes a vector datum from elements the caller has checked
// (vector.Check): finite, at least one, at most vector.MaxDims.
func NewVector(v []float32) Datum { return Datum{Fam: Vector, S: string(vector.Encode(v))} }

// ParseVector reads the text form '[1,2,3]'.
func ParseVector(s string) (Datum, error) {
	v, err := vector.Parse(s)
	if err != nil {
		return Datum{}, err
	}
	return NewVector(v), nil
}

// VectorVal is the datum's elements.
func (d Datum) VectorVal() []float32 { return vector.Decode(d.S) }

// VectorDims is the datum's dimension.
func (d Datum) VectorDims() int { return len(d.S) / 4 }

// VectorFromArray converts a numeric array to a vector; a NULL element
// has no place in one.
func VectorFromArray(d Datum) (Datum, error) {
	out := make([]float32, len(d.A))
	for i, e := range d.A {
		if e.Null {
			return Datum{}, fmt.Errorf("array must not contain nulls to become a vector")
		}
		f, err := e.Coerce(Float)
		if err != nil {
			return Datum{}, fmt.Errorf("cannot use %s value as VECTOR", d.Fam)
		}
		if !math.IsNaN(f.F) && !math.IsInf(f.F, 0) && math.Abs(f.F) > math.MaxFloat32 {
			return Datum{}, fmt.Errorf("%v is out of range for type vector", f.F)
		}
		out[i] = float32(f.F)
	}
	if err := vector.Check(out); err != nil {
		return Datum{}, err
	}
	return NewVector(out), nil
}
