package inverted

import (
	"fmt"
	"strconv"

	"github.com/sthorne/datax/pkg/sql/tsearch"
	"github.com/sthorne/datax/pkg/sql/types"
)

// The text-search kinds (issue #299). An element is a lexeme, as its
// bytes and nothing else: positions and weights stay in the vector, and
// the recheck reads them there. So the index answers "which rows hold
// this word" and every finer question — is the word at one of these
// weights, do two words stand next to each other — is answered by
// evaluating the real @@ on the rows it returns.
//
// A lexeme longer than MaxElementLen is stored CUT to that length, not
// digested as the JSONB kind's long elements are. A cut lexeme still
// starts with every prefix of itself up to the cut, which a digest does
// not, and prefix search (word:*) is a range scan over the elements
// that start with the prefix. A lookup for a long lexeme asks for its
// cut form and also finds the other lexemes that share those first
// bytes; the recheck drops them.
//
// TSVectorOps indexes a tsvector column. The two computed kinds index a
// TEXT column by the lexemes of to_tsvector('<config>', column), which
// is how CREATE INDEX ... USING GIN (to_tsvector('english', body)) is
// stored: there are no expression indexes in general, only this one
// expression, named by the kind.
const (
	TSVectorOps = "tsvector_ops"
	// textSearchPrefix starts the name of a computed kind; the
	// configuration follows.
	textSearchPrefix = "to_tsvector_"
)

// TextSearchKindName is the name of the kind that indexes a text column
// under a configuration.
func TextSearchKindName(config string) string { return textSearchPrefix + config }

// Computed is implemented by a kind that indexes a function of its
// column instead of the column's value. The index serves predicates
// whose left-hand side is exactly Func(Arg, column).
type Computed interface {
	// Computes names the function and its constant first argument.
	Computes() (fn, arg string)
}

type tsvectorKind struct {
	// cfg is nil for the kind over a tsvector column.
	cfg *tsearch.Config
}

func init() {
	Register(tsvectorKind{})
	for _, name := range tsearch.ConfigNames() {
		cfg, err := tsearch.LookupConfig(name)
		if err != nil {
			panic(err)
		}
		Register(tsvectorKind{cfg: cfg})
	}
}

func (k tsvectorKind) Name() string {
	if k.cfg != nil {
		return TextSearchKindName(k.cfg.Name)
	}
	return TSVectorOps
}

func (tsvectorKind) Method() string { return "gin" }

func (k tsvectorKind) Family() types.Family {
	if k.cfg != nil {
		return types.String
	}
	return types.Tsvector
}

// Computes implements Computed for the kinds over a text column.
func (k tsvectorKind) Computes() (fn, arg string) {
	if k.cfg == nil {
		return "", ""
	}
	return "to_tsvector", k.cfg.Name
}

func lexemeElement(lexeme string) []byte {
	if len(lexeme) > MaxElementLen {
		lexeme = lexeme[:MaxElementLen]
	}
	return []byte(lexeme)
}

// Elements lists the value's lexemes.
func (k tsvectorKind) Elements(d types.Datum) ([][]byte, error) {
	if d.Null {
		return nil, nil
	}
	var v tsearch.Vector
	var err error
	if k.cfg != nil {
		v, err = tsearch.ToVector(k.cfg, d.S)
	} else {
		v, err = tsearch.ParseVector(d.S)
	}
	if err != nil {
		return nil, err
	}
	if len(v) > MaxElementsPerValue {
		return nil, fmt.Errorf("document has more than %d distinct lexemes", MaxElementsPerValue)
	}
	out := make([][]byte, len(v))
	for i, e := range v {
		out[i] = lexemeElement(e.Lexeme)
	}
	// Sorted already unless two long lexemes were cut to the same bytes.
	return SortDedup(out), nil
}

// Query serves @@ with a tsquery on the right. The lookups follow the
// query's own shape — & is AND, | is OR — with three things left to
// the recheck: a phrase (a <-> b) asks the index only for rows holding
// both words; a negation asks it for nothing, because the index lists
// the rows that hold a word and not the ones that lack it, so "a & !b"
// looks up a alone and "!b" alone is a full scan; and weights are not
// in the index at all.
func (tsvectorKind) Query(op string, rhs types.Datum) (*Expr, error) {
	if op != "@@" {
		return nil, nil
	}
	if rhs.Null {
		return None(), nil
	}
	if rhs.Fam != types.Tsquery && rhs.Fam != types.String {
		return nil, nil
	}
	q, err := tsearch.ParseQuery(rhs.S)
	if err != nil {
		// Not a query: the filter reports the error.
		return nil, nil
	}
	if q.Root == nil {
		// Only stop words: matches nothing.
		return None(), nil
	}
	return tsQueryExpr(q.Root), nil
}

func tsQueryExpr(n *tsearch.Node) *Expr {
	switch n.Kind {
	case tsearch.NodeVal:
		if n.Prefix {
			return Prefix(lexemeElement(n.Lexeme))
		}
		return Leaf(lexemeElement(n.Lexeme))
	case tsearch.NodeAnd, tsearch.NodePhrase:
		return And(tsQueryExpr(n.L), tsQueryExpr(n.R))
	case tsearch.NodeOr:
		return Or(tsQueryExpr(n.L), tsQueryExpr(n.R))
	}
	// A negation, at any depth, constrains nothing.
	return nil
}

func (tsvectorKind) Describe(element []byte) string {
	return strconv.Quote(string(element))
}
