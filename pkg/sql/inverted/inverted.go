// Package inverted is the part of an inverted index that does not depend
// on what is being indexed.
//
// An ordinary secondary index has one entry per row. An inverted index
// has one entry per ELEMENT of the indexed value per row: a JSONB
// document's elements are its paths and values, a text document's would
// be its terms. The entry's key is
//
//	/t/<tableID>/<indexID>/ + EncodeBytes(element) + <primary key>
//
// (built by pkg/sql/rowenc), so all rows holding one element are one
// contiguous span, in primary-key order.
//
// What an element IS belongs to a Kind. A Kind supplies two things and
// nothing else: the elements of a value (the write path, the backfill)
// and, for a predicate over the indexed column, an Expr — a tree of
// AND/OR over element lookups whose result contains every row the
// predicate can be true for (the read path). Everything else — the key
// layout, maintenance on INSERT/UPDATE/DELETE, backfill, evaluating an
// Expr against the index, and the recheck of every row the index
// returns — is shared and knows only "bytes".
//
// The contract a Kind must keep, which is the whole correctness argument
// of the index: if Query(op, rhs) returns a non-nil Expr, then for every
// value v with `v op rhs` true, Elements(v) satisfies the Expr. The Expr
// may admit rows the predicate rejects (the executor rechecks each row
// against the real predicate); it may never miss one.
package inverted

import (
	"bytes"
	"crypto/sha256"
	"fmt"
	"sort"

	"github.com/sthorne/datax/pkg/sql/types"
)

// Kind is one way of indexing a value by its elements.
type Kind interface {
	// Name is what the index descriptor stores and what a CREATE INDEX
	// operator class names ("jsonb_ops").
	Name() string
	// Method is the access method the index is reported under ("gin").
	Method() string
	// Family is the column type the kind indexes.
	Family() types.Family
	// Elements returns the distinct elements of a non-NULL value, sorted.
	// A value with no elements (an empty document) has no entries.
	Elements(d types.Datum) ([][]byte, error)
	// Query turns `column op rhs` into the element lookups that every
	// matching value satisfies. A nil Expr means the predicate does not
	// constrain the index (a full scan must serve it).
	Query(op string, rhs types.Datum) (*Expr, error)
	// Describe renders an element for humans (key pretty-printing).
	Describe(element []byte) string
}

var kinds = map[string]Kind{}

// Register makes a kind available; called from init.
func Register(k Kind) { kinds[k.Name()] = k }

// Lookup finds a kind by the name an index descriptor carries.
func Lookup(name string) (Kind, bool) {
	k, ok := kinds[name]
	return k, ok
}

// Names lists the registered kinds, sorted.
func Names() []string {
	out := make([]string, 0, len(kinds))
	for n := range kinds {
		out = append(out, n)
	}
	sort.Strings(out)
	return out
}

// MaxElementsPerValue bounds the entries one row may have in one
// inverted index. A larger value is refused rather than partly indexed:
// an index missing some of a row's elements would silently drop the row
// from results.
const MaxElementsPerValue = 8192

// MaxElementLen is the longest element stored as it is. A longer one
// (a long string value, a deep path) is replaced by a fixed-size digest
// so that no document can produce an arbitrarily long key.
const MaxElementLen = 128

// digestTag starts a digested element. Kinds must not start an element
// of their own with it.
const digestTag byte = 0xfe

const digestLen = 20

// Clamp returns the element as it is stored: itself, or its digest when
// it is longer than MaxElementLen. Kinds call it on every element they
// produce, on the write side and the query side alike, so a lookup finds
// what a write stored. Two long elements can share a digest (with
// negligible probability); the recheck is what makes that harmless.
func Clamp(element []byte) []byte {
	if len(element) <= MaxElementLen {
		return element
	}
	sum := sha256.Sum256(element)
	out := make([]byte, 0, 1+digestLen)
	out = append(out, digestTag)
	return append(out, sum[:digestLen]...)
}

// IsDigest reports whether the element is a digest of a longer one.
func IsDigest(element []byte) bool {
	return len(element) == 1+digestLen && element[0] == digestTag
}

// SortDedup sorts elements and drops duplicates: a value that holds an
// element twice has one entry.
func SortDedup(elements [][]byte) [][]byte {
	sort.Slice(elements, func(i, j int) bool { return bytes.Compare(elements[i], elements[j]) < 0 })
	out := elements[:0]
	for i, e := range elements {
		if i > 0 && bytes.Equal(e, elements[i-1]) {
			continue
		}
		out = append(out, e)
	}
	return out
}

// Diff splits two sorted, distinct element lists into what only the old
// one has and what only the new one has — the entries an UPDATE deletes
// and writes. Elements both have are left alone.
func Diff(old, new [][]byte) (removed, added [][]byte) {
	i, j := 0, 0
	for i < len(old) && j < len(new) {
		switch c := bytes.Compare(old[i], new[j]); {
		case c == 0:
			i++
			j++
		case c < 0:
			removed = append(removed, old[i])
			i++
		default:
			added = append(added, new[j])
			j++
		}
	}
	removed = append(removed, old[i:]...)
	added = append(added, new[j:]...)
	return removed, added
}

// ---------------------------------------------------------------------------
// Query expressions.

// Op is an Expr node's operator.
type Op int

const (
	// OpLeaf: the rows holding Element.
	OpLeaf Op = iota
	// OpAnd: the rows every child admits. Never empty (And returns nil,
	// "unconstrained", for no children).
	OpAnd
	// OpOr: the rows some child admits. With no children: no rows.
	OpOr
)

// Expr is a set of rows described by element lookups. A nil *Expr is
// "every row": the predicate says nothing the index can use.
type Expr struct {
	Op      Op
	Element []byte
	Kids    []*Expr
}

// Leaf admits the rows holding the element.
func Leaf(element []byte) *Expr { return &Expr{Op: OpLeaf, Element: element} }

// None admits no row.
func None() *Expr { return &Expr{Op: OpOr} }

// And admits the rows all of kids admit. Unconstrained (nil) children
// add nothing and are dropped; with none left the result is nil. A
// child that admits no row makes the whole conjunction admit none.
func And(kids ...*Expr) *Expr {
	var out []*Expr
	for _, k := range kids {
		switch {
		case k == nil:
		case k.Op == OpOr && len(k.Kids) == 0:
			return None()
		case k.Op == OpAnd:
			out = append(out, k.Kids...)
		default:
			out = append(out, k)
		}
	}
	out = dedup(out)
	switch len(out) {
	case 0:
		return nil
	case 1:
		return out[0]
	}
	return &Expr{Op: OpAnd, Kids: out}
}

// Or admits the rows any of kids admits. One unconstrained (nil) child
// makes the whole disjunction unconstrained.
func Or(kids ...*Expr) *Expr {
	var out []*Expr
	for _, k := range kids {
		switch {
		case k == nil:
			return nil
		case k.Op == OpOr:
			out = append(out, k.Kids...)
		default:
			out = append(out, k)
		}
	}
	out = dedup(out)
	if len(out) == 1 {
		return out[0]
	}
	return &Expr{Op: OpOr, Kids: out}
}

// dedup drops repeated leaves (a ?& over a repeated key, a containment
// naming one element twice).
func dedup(kids []*Expr) []*Expr {
	out := kids[:0:0]
	for _, k := range kids {
		dup := false
		if k.Op == OpLeaf {
			for _, o := range out {
				if o.Op == OpLeaf && bytes.Equal(o.Element, k.Element) {
					dup = true
					break
				}
			}
		}
		if !dup {
			out = append(out, k)
		}
	}
	return out
}

// Leaves counts the element lookups in the expression.
func (e *Expr) Leaves() int {
	if e == nil {
		return 0
	}
	if e.Op == OpLeaf {
		return 1
	}
	n := 0
	for _, k := range e.Kids {
		n += k.Leaves()
	}
	return n
}

// Format renders the expression with describe naming each element
// (EXPLAIN).
func (e *Expr) Format(describe func([]byte) string) string {
	if e == nil {
		return "all"
	}
	switch e.Op {
	case OpLeaf:
		return describe(e.Element)
	case OpAnd, OpOr:
		if len(e.Kids) == 0 {
			return "none"
		}
		sep := " AND "
		if e.Op == OpOr {
			sep = " OR "
		}
		var b bytes.Buffer
		for i, k := range e.Kids {
			if i > 0 {
				b.WriteString(sep)
			}
			if k.Op != OpLeaf {
				b.WriteString("(" + k.Format(describe) + ")")
			} else {
				b.WriteString(k.Format(describe))
			}
		}
		return b.String()
	}
	return fmt.Sprintf("?op%d", e.Op)
}

// ---------------------------------------------------------------------------
// Evaluation.

// ScanFunc reads the row references stored under one element, in
// ascending order, at most limit of them (0 = all). A row reference is
// whatever follows the element in the entry's key — the encoded primary
// key — and is opaque here.
type ScanFunc func(element []byte, limit int) ([][]byte, error)

// ProbeLimit is how many references a conjunction reads from each of
// its elements before deciding which to use. See Eval.
const ProbeLimit = 1024

// Eval returns the references of the rows the expression admits, sorted
// and distinct. e must not be nil.
//
// There are no per-element statistics, so a conjunction finds its
// selective elements by looking: each element is read up to ProbeLimit
// references. The ones that end within the limit are complete and are
// intersected. The ones that do not are common elements — reading them
// in full would cost more than the rows they exclude are worth — and are
// left to the recheck, which is sound because a conjunction of fewer
// terms only admits more rows. Only when every term is common are they
// all read in full and intersected. A disjunction has no such choice:
// every branch is read in full.
func (e *Expr) Eval(scan ScanFunc) ([][]byte, error) {
	switch e.Op {
	case OpLeaf:
		return scan(e.Element, 0)
	case OpOr:
		var acc [][]byte
		for _, k := range e.Kids {
			refs, err := k.Eval(scan)
			if err != nil {
				return nil, err
			}
			acc = union(acc, refs)
		}
		return acc, nil
	case OpAnd:
		var acc [][]byte
		have := false
		var common []*Expr
		for _, k := range e.Kids {
			if k.Op != OpLeaf {
				common = append(common, k)
				continue
			}
			refs, err := scan(k.Element, ProbeLimit+1)
			if err != nil {
				return nil, err
			}
			if len(refs) > ProbeLimit {
				common = append(common, k)
				continue
			}
			if have {
				acc = intersect(acc, refs)
			} else {
				acc, have = refs, true
			}
			if len(acc) == 0 {
				return nil, nil
			}
		}
		if have {
			return acc, nil
		}
		for _, k := range common {
			refs, err := k.Eval(scan)
			if err != nil {
				return nil, err
			}
			if have {
				acc = intersect(acc, refs)
			} else {
				acc, have = refs, true
			}
			if len(acc) == 0 {
				return nil, nil
			}
		}
		return acc, nil
	}
	return nil, fmt.Errorf("inverted: unknown expression operator %d", e.Op)
}

func intersect(a, b [][]byte) [][]byte {
	var out [][]byte
	i, j := 0, 0
	for i < len(a) && j < len(b) {
		switch c := bytes.Compare(a[i], b[j]); {
		case c == 0:
			out = append(out, a[i])
			i++
			j++
		case c < 0:
			i++
		default:
			j++
		}
	}
	return out
}

func union(a, b [][]byte) [][]byte {
	if len(a) == 0 {
		return b
	}
	if len(b) == 0 {
		return a
	}
	out := make([][]byte, 0, len(a)+len(b))
	i, j := 0, 0
	for i < len(a) && j < len(b) {
		switch c := bytes.Compare(a[i], b[j]); {
		case c == 0:
			out = append(out, a[i])
			i++
			j++
		case c < 0:
			out = append(out, a[i])
			i++
		default:
			out = append(out, b[j])
			j++
		}
	}
	out = append(out, a[i:]...)
	return append(out, b[j:]...)
}
