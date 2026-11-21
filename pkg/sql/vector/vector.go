// Package vector is the arithmetic behind the VECTOR column type (issue
// #304): the value's text and binary forms, the three distances
// pgvector's operators compute, and the clustering an IVF index is built
// on (kmeans.go, centroids.go). It knows nothing of SQL datums or keys;
// pkg/sql/types wraps a vector in a datum and pkg/sql/rowenc keys the
// index entries.
//
// A vector is a fixed number of float32 elements, as in pgvector: half
// the bytes of float64 for values that are almost always model output
// with fewer than seven significant digits. Distances are accumulated
// in float64 so a sum over a few thousand squared float32 terms does
// not lose its low digits.
package vector

import (
	"encoding/binary"
	"fmt"
	"math"
	"strconv"
	"strings"
	"unsafe"
)

// OID is the type's object ID in pg_type and on the wire. pgvector is
// an extension, so PostgreSQL gives its type whatever OID is free when
// CREATE EXTENSION runs, and clients are written to look the number up
// by name (to_regtype('vector'), pg_type) rather than know it. Here the
// type is built in and the number is fixed: the same in every database
// and on every node. It sits in the range PostgreSQL leaves to objects
// created at initdb (12000–16383), below the first OID a user-defined
// type can have, so it meets neither a PostgreSQL built-in nor an enum.
const OID = 16000

const (
	// MaxDims is the most elements a vector value may have (pgvector's
	// limit for the type).
	MaxDims = 16000
	// MaxIndexDims is the most elements an indexed vector column may
	// have (pgvector's limit for its indexes).
	MaxIndexDims = 2000
)

// Parse reads the text form '[1, 2.5, -3e2]'. Every element must be a
// finite number that float32 can hold; an empty vector is refused.
func Parse(s string) ([]float32, error) {
	t := strings.TrimSpace(s)
	if len(t) < 2 || t[0] != '[' || t[len(t)-1] != ']' {
		return nil, fmt.Errorf("invalid input syntax for type vector: %q (a vector is written [1,2,3])", clip(s))
	}
	body := strings.TrimSpace(t[1 : len(t)-1])
	if body == "" {
		return nil, fmt.Errorf("vector must have at least 1 dimension")
	}
	n := strings.Count(body, ",") + 1
	if n > MaxDims {
		return nil, fmt.Errorf("vector cannot have more than %d dimensions", MaxDims)
	}
	out := make([]float32, 0, n)
	for len(body) > 0 {
		part := body
		if i := strings.IndexByte(body, ','); i >= 0 {
			part, body = body[:i], body[i+1:]
			if strings.TrimSpace(body) == "" {
				return nil, fmt.Errorf("invalid input syntax for type vector: %q", clip(s))
			}
		} else {
			body = ""
		}
		part = strings.TrimSpace(part)
		f, err := strconv.ParseFloat(part, 32)
		if err != nil {
			if ne, ok := err.(*strconv.NumError); ok && ne.Err == strconv.ErrRange {
				return nil, fmt.Errorf("%q is out of range for type vector", part)
			}
			return nil, fmt.Errorf("invalid input syntax for type vector: %q", clip(s))
		}
		if err := checkElement(f); err != nil {
			return nil, err
		}
		out = append(out, float32(f))
	}
	return out, nil
}

// clip shortens a value quoted in an error: a 1536-element vector is
// tens of kilobytes of text.
func clip(s string) string {
	if len(s) > 64 {
		return s[:64] + "…"
	}
	return s
}

func checkElement(f float64) error {
	switch {
	case math.IsNaN(f):
		return fmt.Errorf("NaN not allowed in vector")
	case math.IsInf(f, 0):
		return fmt.Errorf("infinite value not allowed in vector")
	}
	return nil
}

// Check refuses a vector no column may hold: no elements, too many, or
// an element that is not a finite number.
func Check(v []float32) error {
	if len(v) == 0 {
		return fmt.Errorf("vector must have at least 1 dimension")
	}
	if len(v) > MaxDims {
		return fmt.Errorf("vector cannot have more than %d dimensions", MaxDims)
	}
	for _, x := range v {
		if err := checkElement(float64(x)); err != nil {
			return err
		}
	}
	return nil
}

// Format renders the text form: '[1,2.5,-300]', each element in the
// shortest form that reads back as the same float32.
func Format(v []float32) string {
	b := make([]byte, 0, 2+len(v)*8)
	b = append(b, '[')
	for i, x := range v {
		if i > 0 {
			b = append(b, ',')
		}
		b = strconv.AppendFloat(b, float64(x), 'g', -1, 32)
	}
	return string(append(b, ']'))
}

// Encode is the stored form: each element's IEEE-754 bits, big-endian,
// four bytes apiece. The dimension is the length over four.
func Encode(v []float32) []byte {
	out := make([]byte, 4*len(v))
	for i, x := range v {
		binary.BigEndian.PutUint32(out[4*i:], math.Float32bits(x))
	}
	return out
}

// Decode reads the stored form.
func Decode(raw string) []float32 {
	out := make([]float32, len(raw)/4)
	for i := range out {
		out[i] = math.Float32frombits(uint32(raw[4*i])<<24 | uint32(raw[4*i+1])<<16 | uint32(raw[4*i+2])<<8 | uint32(raw[4*i+3]))
	}
	return out
}

// DecodeBytes is Decode over a byte slice (an index entry's value).
func DecodeBytes(raw []byte, into []float32) []float32 {
	n := len(raw) / 4
	if cap(into) < n {
		into = make([]float32, n)
	}
	into = into[:n]
	for i := range into {
		into[i] = math.Float32frombits(binary.BigEndian.Uint32(raw[4*i:]))
	}
	return into
}

// EncodeBinary is pgvector's binary wire form: the dimension and an
// unused word as two int16, then the elements as float4.
func EncodeBinary(stored string) []byte {
	out := make([]byte, 4, 4+len(stored))
	binary.BigEndian.PutUint16(out, uint16(len(stored)/4))
	return append(out, stored...)
}

// DecodeBinary reads pgvector's binary wire form into the stored form.
func DecodeBinary(raw []byte) (string, error) {
	if len(raw) < 4 {
		return "", fmt.Errorf("bad binary vector header")
	}
	dim := int(binary.BigEndian.Uint16(raw))
	if unused := binary.BigEndian.Uint16(raw[2:]); unused != 0 {
		return "", fmt.Errorf("bad binary vector header: expected unused to be 0, not %d", unused)
	}
	if len(raw) != 4+4*dim {
		return "", fmt.Errorf("bad binary vector: %d dimensions declared, %d bytes of elements", dim, len(raw)-4)
	}
	v := DecodeBytes(raw[4:], nil)
	if err := Check(v); err != nil {
		return "", err
	}
	return string(raw[4:]), nil
}

// ErrDims is a distance asked of two vectors of different dimensions.
type ErrDims struct{ A, B int }

func (e *ErrDims) Error() string {
	return fmt.Sprintf("different vector dimensions %d and %d", e.A, e.B)
}

// Metric is one of the three distances an index can be built for.
type Metric string

const (
	L2     Metric = "l2"     // <->  Euclidean distance
	Cosine Metric = "cosine" // <=>  1 − cosine similarity
	IP     Metric = "ip"     // <#>  negative inner product
)

// Operator is the SQL operator that computes the metric.
func (m Metric) Operator() string {
	switch m {
	case Cosine:
		return "<=>"
	case IP:
		return "<#>"
	}
	return "<->"
}

// OpClass is pgvector's operator class name for the metric.
func (m Metric) OpClass() string { return "vector_" + string(m) + "_ops" }

// MetricForOperator maps a distance operator to its metric.
func MetricForOperator(op string) (Metric, bool) {
	switch op {
	case "<->":
		return L2, true
	case "<=>":
		return Cosine, true
	case "<#>":
		return IP, true
	}
	return "", false
}

// MetricForOpClass maps a pgvector operator class to its metric.
func MetricForOpClass(name string) (Metric, bool) {
	switch name {
	case "vector_l2_ops":
		return L2, true
	case "vector_cosine_ops":
		return Cosine, true
	case "vector_ip_ops":
		return IP, true
	}
	return "", false
}

// Distance is the metric's operator applied to a and b, which must have
// the same dimension (the callers check; see Dims).
func (m Metric) Distance(a, b []float32) float64 {
	switch m {
	case Cosine:
		return CosineDistance(a, b)
	case IP:
		return -Dot(a, b)
	}
	return math.Sqrt(L2Squared(a, b))
}

// StoredDistance is Distance over two vectors in their stored form
// (Encode), of the same dimension, without decoding either into a
// slice: what a scan computes once per row.
func (m Metric) StoredDistance(a, b string) float64 {
	return m.storedDistance(stringBytes(a), stringBytes(b))
}

// StoredDistanceBytes is StoredDistance with the first vector as bytes
// (a row value or an index entry as read from the store), so neither
// side is copied or decoded. b must be at least as long as a.
func (m Metric) StoredDistanceBytes(a []byte, b string) float64 {
	return m.storedDistance(a, stringBytes(b))
}

// stringBytes views a string's bytes without copying them; the view is
// only read.
func stringBytes(s string) []byte { return unsafe.Slice(unsafe.StringData(s), len(s)) }

// storedDistance computes each metric exactly as Distance does over the
// decoded elements — the same float64 terms summed in the same order —
// so its results are bit-identical to Distance(Decode(a), Decode(b)).
// One loop per metric keeps the switch out of the inner loop, and each
// element is one big-endian load.
func (m Metric) storedDistance(a, b []byte) float64 {
	a = a[:len(a)&^3]
	b = b[:len(a)]
	switch m {
	case Cosine:
		var dot, na, nb float64
		for i := 0; i < len(a); i += 4 {
			x := float64(math.Float32frombits(binary.BigEndian.Uint32(a[i:])))
			y := float64(math.Float32frombits(binary.BigEndian.Uint32(b[i:])))
			dot += x * y
			na += x * x
			nb += y * y
		}
		sim := dot / math.Sqrt(na*nb)
		if math.IsNaN(sim) {
			return math.NaN()
		}
		if sim > 1 {
			sim = 1
		} else if sim < -1 {
			sim = -1
		}
		return 1 - sim
	case IP:
		var dot float64
		for i := 0; i < len(a); i += 4 {
			x := float64(math.Float32frombits(binary.BigEndian.Uint32(a[i:])))
			y := float64(math.Float32frombits(binary.BigEndian.Uint32(b[i:])))
			dot += x * y
		}
		return -dot
	}
	var l2 float64
	for i := 0; i < len(a); i += 4 {
		x := float64(math.Float32frombits(binary.BigEndian.Uint32(a[i:])))
		y := float64(math.Float32frombits(binary.BigEndian.Uint32(b[i:])))
		d := x - y
		l2 += d * d
	}
	return math.Sqrt(l2)
}

// Dims refuses two vectors of different dimensions.
func Dims(a, b []float32) error {
	if len(a) != len(b) {
		return &ErrDims{len(a), len(b)}
	}
	return nil
}

// L2Squared is the squared Euclidean distance.
func L2Squared(a, b []float32) float64 {
	b = b[:len(a)]
	var s float64
	for i, x := range a {
		d := float64(x) - float64(b[i])
		s += d * d
	}
	return s
}

// Dot is the inner product.
func Dot(a, b []float32) float64 {
	b = b[:len(a)]
	var s float64
	for i, x := range a {
		s += float64(x) * float64(b[i])
	}
	return s
}

// L1 is the taxicab distance.
func L1(a, b []float32) float64 {
	b = b[:len(a)]
	var s float64
	for i, x := range a {
		s += math.Abs(float64(x) - float64(b[i]))
	}
	return s
}

// Norm is the Euclidean length.
func Norm(a []float32) float64 { return math.Sqrt(Dot(a, a)) }

// CosineDistance is 1 − cos(a, b), clamped to [0, 2] against rounding.
// With a zero vector on either side the angle is undefined and the
// result is NaN, as in pgvector; NaN sorts after every number.
func CosineDistance(a, b []float32) float64 {
	b = b[:len(a)]
	var dot, na, nb float64
	for i, x := range a {
		y := float64(b[i])
		dot += float64(x) * y
		na += float64(x) * float64(x)
		nb += y * y
	}
	sim := dot / math.Sqrt(na*nb)
	if math.IsNaN(sim) {
		return math.NaN()
	}
	if sim > 1 {
		sim = 1
	} else if sim < -1 {
		sim = -1
	}
	return 1 - sim
}

// Normalize returns a scaled to unit length; a zero vector is returned
// as it is.
func Normalize(a []float32) []float32 {
	out := make([]float32, len(a))
	n := Norm(a)
	if n == 0 {
		return out
	}
	for i, x := range a {
		out[i] = float32(float64(x) / n)
	}
	return out
}

// Compare orders two vectors element by element, a shorter vector that
// is a prefix of a longer one first — pgvector's btree order.
func Compare(a, b []float32) int {
	for i := 0; i < len(a) && i < len(b); i++ {
		switch {
		case a[i] < b[i]:
			return -1
		case a[i] > b[i]:
			return 1
		}
	}
	switch {
	case len(a) < len(b):
		return -1
	case len(a) > len(b):
		return 1
	}
	return 0
}
