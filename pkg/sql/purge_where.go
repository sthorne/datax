package sql

import (
	"context"
	"fmt"
	"sort"
	"strings"

	"github.com/sthorne/datax/pkg/keys"
	"github.com/sthorne/datax/pkg/kvpb"
	"github.com/sthorne/datax/pkg/sql/catalog"
	"github.com/sthorne/datax/pkg/sql/parser"
	"github.com/sthorne/datax/pkg/sql/rowenc"
	"github.com/sthorne/datax/pkg/sql/types"
)

// ALTER TABLE t PURGE WHERE predicate (issue #375): a purge of the rows
// one primary-key prefix or range selects, such as one tenant's, and of
// nothing else that can be told apart from them.
//
// A purge acts on whole ranges (server.Node.PurgeSpan splits at the ends
// of the span it is given) and raises their GC threshold, refusing older
// reads there. The predicate is therefore turned into key spans, never
// evaluated row by row: an equality on the leading primary-key columns,
// and optionally a range on the next one, is one span of the primary
// index (one per shard bucket on a sharded table).
//
// The table's other data is purged as narrowly as its keys allow:
//
//   - A secondary index whose key begins with the constrained primary-key
//     columns holds the selected rows' entries under the same prefix, so
//     it is purged over that prefix (and range, when the index continues
//     with the range column) and nowhere else.
//   - Everything else in the table's key span — an index keyed by other
//     columns or by expressions, an inverted or vector index, the
//     table's auxiliary keys, a layout a re-shard or an online ALTER is
//     writing or has retired — cannot be narrowed to the selected rows,
//     and is purged whole, as ALTER TABLE t PURGE purges it. The purged
//     rows' old versions are then gone from every index, at the cost of
//     older reads of those indexes for every key. A NOTICE names each
//     index purged wider than the predicate.
//
// So the spans purged are the table's key span less, for the primary
// index and each narrowed index, the parts that hold other keys.

// purgeBound is one end of the range PURGE WHERE puts on a key column.
type purgeBound struct {
	d    types.Datum
	incl bool
}

// purgeKeyRange is what a PURGE WHERE predicate selects: equality values
// for the primary-key columns from off on, in key order, and an optional
// range on the column after them.
type purgeKeyRange struct {
	off    int // 1 on a sharded table: the hidden bucket column leads
	eq     []types.Datum
	lo, hi *purgeBound
	// cols are the constrained primary-key column IDs, the range
	// column's last when there is one.
	cols []catalog.ColumnID
}

func (r *purgeKeyRange) hasRange() bool { return r.lo != nil || r.hi != nil }

// purgeWhereHint names the table's primary key, for every refusal.
func purgeWhereHint(desc *catalog.TableDescriptor) string {
	var names []string
	for _, id := range desc.PrimaryKey {
		if c, ok := desc.ColByID(id); ok && !c.Hidden {
			names = append(names, quoteIdentIfNeeded(c.Name))
		}
	}
	if len(names) == 0 {
		return fmt.Sprintf("%q has no declared primary key, so PURGE WHERE cannot name its rows; use ALTER TABLE %s PURGE to purge the whole table.", desc.Name, desc.Name)
	}
	return fmt.Sprintf("The primary key of %q is (%s). PURGE WHERE takes an equality on its leading columns (a = 1 AND b = 2, or (a, b) = (1, 2)) and, optionally, one range (BETWEEN, <, <=, >, >=) on the column after them.", desc.Name, strings.Join(names, ", "))
}

func quoteIdentIfNeeded(s string) string {
	for _, r := range s {
		if !(r >= 'a' && r <= 'z' || r >= '0' && r <= '9' || r == '_') {
			return `"` + strings.ReplaceAll(s, `"`, `""`) + `"`
		}
	}
	return s
}

func purgeWhereErr(desc *catalog.TableDescriptor, code, format string, args ...any) *Error {
	e := newErrf(code, "PURGE WHERE: "+format, args...)
	e.Hint = purgeWhereHint(desc)
	return e
}

// purgeConstant reports whether e is a value PURGE WHERE may compare a
// key column with: a literal or a parameter, cast or not. Anything
// computed (a function, arithmetic, a column, a subquery) is refused, so
// that what is purged is plain from the statement.
func purgeConstant(e parser.Expr) bool {
	if e.Lit == nil && e.Param == 0 {
		return false
	}
	return e.Column == "" && len(e.Path) == 0 && e.Sub == nil && e.Func == "" && e.BinOp == "" &&
		e.Left == nil && e.Right == nil && e.Case == nil && e.Cmp == nil && !e.IsDefault && e.Window == nil && e.Agg == nil
}

// bareColumn reports whether e is a column reference and nothing more.
func bareColumn(e parser.Expr) bool {
	return e.Column != "" && len(e.Path) == 0 && e.Lit == nil && e.Param == 0 && e.Sub == nil && e.Func == "" &&
		e.BinOp == "" && e.Left == nil && e.Right == nil && e.Case == nil && e.Cmp == nil && e.Window == nil && e.Agg == nil
}

// flipOp is the operator of a comparison read the other way round
// (3 > a is a < 3).
func flipOp(op string) string {
	switch op {
	case "<":
		return ">"
	case "<=":
		return ">="
	case ">":
		return "<"
	case ">=":
		return "<="
	}
	return op
}

// purgeKeyValue evaluates a bound for column col and conforms it to the
// column's type. It must be exactly a value the column can hold: a key
// bound that the column's type rounds (1.5 for an integer, a timestamp
// finer than the column keeps) would name different keys from the
// predicate, and a purge must not reach rows the statement did not name.
func purgeKeyValue(desc *catalog.TableDescriptor, col catalog.Column, e parser.Expr, params []types.Datum) (types.Datum, *Error) {
	d, err := evalExpr(e, nil, nil, params)
	if err != nil {
		return types.Datum{}, ToSQLError(err)
	}
	if d.Null {
		return types.Datum{}, purgeWhereErr(desc, CodeInvalidParameterValue, "column %q is compared with NULL, which selects no rows", col.Name)
	}
	c, err := d.Coerce(col.Type)
	if err != nil && numericFamily(d.Fam) && numericFamily(col.Type) {
		return types.Datum{}, purgeWhereErr(desc, CodeInvalidParameterValue, "%s is not a value of column %q (%s): a purge bound must be one the key can hold exactly", d.Text(), col.Name, col.TypeSQL())
	}
	if err != nil {
		return types.Datum{}, newErrf(CodeInvalidTextRepresentation, "PURGE WHERE: column %q: %v", col.Name, err)
	}
	if numericFamily(d.Fam) && numericFamily(c.Fam) {
		if cmp, err := d.Compare(c); err != nil || cmp != 0 {
			return types.Datum{}, purgeWhereErr(desc, CodeInvalidParameterValue, "%s is not a value of column %q (%s): a purge bound must be one the key can hold exactly", d.Text(), col.Name, col.TypeSQL())
		}
	}
	conformed, err := col.Conform(c)
	if err != nil {
		return types.Datum{}, ToSQLError(err)
	}
	if !col.Char {
		if cmp, err := c.Compare(conformed); err == nil && cmp != 0 {
			return types.Datum{}, purgeWhereErr(desc, CodeInvalidParameterValue, "%s is not a value of column %q (%s): a purge bound must be one the key can hold exactly", c.Text(), col.Name, col.TypeSQL())
		}
	}
	return conformed, nil
}

func numericFamily(f types.Family) bool {
	return f == types.Int || f == types.Float || f == types.Decimal
}

// purgeKeyRangeOf turns PURGE WHERE's conjuncts into the primary-key
// prefix or range they select, or says why they do not name one.
func purgeKeyRangeOf(desc *catalog.TableDescriptor, conds []parser.Comparison, params []types.Datum) (*purgeKeyRange, *Error) {
	r := &purgeKeyRange{}
	if desc.ShardBuckets > 0 {
		r.off = 1
	}
	pkPos := map[catalog.ColumnID]int{}
	for i, id := range desc.PrimaryKey {
		pkPos[id] = i
	}
	eqs := map[int]parser.Expr{}
	type rangeCond struct {
		op string
		v  parser.Expr
	}
	ranges := map[int][]rangeCond{}
	for _, c := range conds {
		column, op, value := c.Column, c.Op, c.Value
		if c.Column == "" && c.Expr != nil && purgeConstant(*c.Expr) && bareColumn(c.Value) {
			// 3 > a: a constant on the left, a bare column on the right.
			column, op, value = c.Value.Column, flipOp(c.Op), *c.Expr
		}
		switch {
		case c.Op == "OR" || len(c.Or) > 0:
			return nil, purgeWhereErr(desc, CodeFeatureNotSupported, "OR is not supported: a purge names one key prefix or range (run one PURGE per prefix)")
		case c.Op == "IN" || c.Op == "NOT IN":
			return nil, purgeWhereErr(desc, CodeFeatureNotSupported, "IN is not supported: a purge names one key prefix or range (run one PURGE per prefix)")
		case c.Sub != nil || c.Op == "EXISTS" || c.Op == "NOT EXISTS":
			return nil, purgeWhereErr(desc, CodeFeatureNotSupported, "a subquery is not supported")
		case column == "" || len(c.Path) > 0:
			return nil, purgeWhereErr(desc, CodeFeatureNotSupported, "%q is not a comparison of a primary-key column with a value", c.Op)
		}
		col, ok := desc.Col(column)
		if !ok {
			return nil, newErrf(CodeUndefinedColumn, "column %q does not exist", column)
		}
		pos, inPK := pkPos[col.ID]
		if !inPK || col.Hidden {
			return nil, purgeWhereErr(desc, CodeInvalidColumnReference, "column %q is not a primary-key column of %q: a purge is a range of keys, and only the primary key orders them", column, desc.Name)
		}
		switch op {
		case "=", "<", "<=", ">", ">=":
		default:
			return nil, purgeWhereErr(desc, CodeFeatureNotSupported, "operator %s on column %q is not supported", op, column)
		}
		if !purgeConstant(value) {
			return nil, purgeWhereErr(desc, CodeFeatureNotSupported, "column %q must be compared with a constant or a parameter, not an expression", column)
		}
		if op == "=" {
			if _, dup := eqs[pos]; dup {
				return nil, purgeWhereErr(desc, CodeFeatureNotSupported, "column %q is compared for equality twice", column)
			}
			eqs[pos] = value
			continue
		}
		ranges[pos] = append(ranges[pos], rangeCond{op, value})
	}
	if len(eqs) == 0 && len(ranges) == 0 {
		return nil, purgeWhereErr(desc, CodeFeatureNotSupported, "the predicate names no primary-key column")
	}
	// The equalities must be a leading prefix of the (logical) key.
	k := 0
	for pos := r.off; pos < len(desc.PrimaryKey); pos++ {
		if _, ok := eqs[pos]; !ok {
			break
		}
		k++
	}
	for pos := range eqs {
		if pos >= r.off+k {
			col, _ := desc.ColByID(desc.PrimaryKey[pos])
			missing, _ := desc.ColByID(desc.PrimaryKey[r.off+k])
			return nil, purgeWhereErr(desc, CodeInvalidColumnReference, "column %q is not part of a leading primary-key prefix: %q comes before it and has no equality", col.Name, missing.Name)
		}
	}
	for pos := range ranges {
		col, _ := desc.ColByID(desc.PrimaryKey[pos])
		switch {
		case pos < r.off+k:
			return nil, purgeWhereErr(desc, CodeFeatureNotSupported, "column %q has both an equality and a range", col.Name)
		case pos > r.off+k:
			missing, _ := desc.ColByID(desc.PrimaryKey[r.off+k])
			return nil, purgeWhereErr(desc, CodeInvalidColumnReference, "a range on column %q needs an equality on every primary-key column before it, and %q has none", col.Name, missing.Name)
		}
	}
	for j := 0; j < k; j++ {
		col, _ := desc.ColByID(desc.PrimaryKey[r.off+j])
		d, serr := purgeKeyValue(desc, col, eqs[r.off+j], params)
		if serr != nil {
			return nil, serr
		}
		r.eq = append(r.eq, d)
		r.cols = append(r.cols, col.ID)
	}
	if rc := ranges[r.off+k]; len(rc) > 0 {
		col, _ := desc.ColByID(desc.PrimaryKey[r.off+k])
		r.cols = append(r.cols, col.ID)
		for _, c := range rc {
			d, serr := purgeKeyValue(desc, col, c.v, params)
			if serr != nil {
				return nil, serr
			}
			b := &purgeBound{d: d, incl: c.op == ">=" || c.op == "<="}
			if c.op == ">" || c.op == ">=" {
				if r.lo != nil {
					return nil, purgeWhereErr(desc, CodeFeatureNotSupported, "column %q has two lower bounds", col.Name)
				}
				r.lo = b
			} else {
				if r.hi != nil {
					return nil, purgeWhereErr(desc, CodeFeatureNotSupported, "column %q has two upper bounds", col.Name)
				}
				r.hi = b
			}
		}
	}
	return r, nil
}

// narrowed is the span of one index the range selects, below prefix:
// eqUsed equalities appended with add, then the range when useRange.
func (r *purgeKeyRange) narrowed(prefix keys.Key, eqUsed int, useRange bool, add func(k keys.Key, j int, d types.Datum) (keys.Key, error)) (keySpan, error) {
	key := prefix.Clone()
	var err error
	for j := 0; j < eqUsed; j++ {
		if key, err = add(key, j, r.eq[j]); err != nil {
			return keySpan{}, err
		}
	}
	sp := keySpan{key, key.PrefixEnd()}
	if !useRange {
		return sp, nil
	}
	j := len(r.eq)
	if r.lo != nil {
		b, err := add(key.Clone(), j, r.lo.d)
		if err != nil {
			return keySpan{}, err
		}
		if r.lo.incl {
			sp.start = b
		} else {
			sp.start = b.PrefixEnd()
		}
	}
	if r.hi != nil {
		b, err := add(key.Clone(), j, r.hi.d)
		if err != nil {
			return keySpan{}, err
		}
		if r.hi.incl {
			sp.end = b.PrefixEnd()
		} else {
			sp.end = b
		}
	}
	return sp, nil
}

// spanMinus returns [lo, hi) less the holes, as ordered disjoint spans.
func spanMinus(lo, hi keys.Key, holes []keySpan) []keySpan {
	hs := append([]keySpan(nil), holes...)
	sort.Slice(hs, func(i, j int) bool { return hs[i].start.Compare(hs[j].start) < 0 })
	var out []keySpan
	at := lo
	for _, h := range hs {
		if h.end.Compare(at) <= 0 {
			continue
		}
		if h.start.Compare(hi) >= 0 {
			break
		}
		if at.Compare(h.start) < 0 {
			out = append(out, keySpan{at, h.start})
		}
		at = h.end
	}
	if at.Compare(hi) < 0 {
		out = append(out, keySpan{at, hi})
	}
	return out
}

// purgeWhereSpans computes the key spans ALTER TABLE ... PURGE WHERE
// purges (see the comment at the top of this file), and the notices
// that name the indexes purged wider than the predicate.
func purgeWhereSpans(desc *catalog.TableDescriptor, conds []parser.Comparison, params []types.Datum) ([]keySpan, []string, *Error) {
	r, serr := purgeKeyRangeOf(desc, conds, params)
	if serr != nil {
		return nil, nil, serr
	}
	keyErr := func(err error) *Error {
		return newErrf(CodeInvalidParameterValue, "PURGE WHERE: %v", err)
	}
	// The primary index: one prefix per shard bucket on a sharded
	// table, or the row's own bucket when the whole key is named.
	pkAdd := func(k keys.Key, j int, d types.Datum) (keys.Key, error) {
		col, _ := desc.ColByID(desc.PrimaryKey[r.off+j])
		return rowenc.AppendKeyDatum(k, col.Type, d)
	}
	pkPrefix := rowenc.PrimaryKeyPrefixFor(desc)
	prefixes := []keys.Key{pkPrefix}
	if r.off == 1 {
		prefixes = nil
		if len(r.eq) == len(desc.PrimaryKey)-1 {
			b, err := rowenc.ShardBucket(desc, r.eq)
			if err != nil {
				return nil, nil, keyErr(err)
			}
			p, err := rowenc.AppendKeyDatum(pkPrefix.Clone(), types.Int, b)
			if err != nil {
				return nil, nil, keyErr(err)
			}
			prefixes = append(prefixes, p)
		} else {
			for b := int32(0); b < desc.ShardBuckets; b++ {
				p, err := rowenc.AppendKeyDatum(pkPrefix.Clone(), types.Int, types.NewInt(int64(b)))
				if err != nil {
					return nil, nil, keyErr(err)
				}
				prefixes = append(prefixes, p)
			}
		}
	}
	var pkSpans []keySpan
	for _, p := range prefixes {
		sp, err := r.narrowed(p, len(r.eq), r.hasRange(), pkAdd)
		if err != nil {
			return nil, nil, keyErr(err)
		}
		if sp.start.Compare(sp.end) >= 0 {
			return nil, nil, purgeWhereErr(desc, CodeInvalidParameterValue, "the range on column %q is empty", colName(desc, r.cols[len(r.cols)-1]))
		}
		pkSpans = append(pkSpans, sp)
	}
	lo, hi := rowenc.PrimarySpanFor(desc)
	keepOut := spanMinus(lo, hi, pkSpans)

	// The secondary indexes whose keys begin with the constrained
	// columns.
	var notices []string
	for i := range desc.Indexes {
		idx := &desc.Indexes[i]
		m := 0
		if !idx.IsInverted() && !idx.IsVector() {
			for m < len(r.cols) && m < len(idx.ColumnIDs) && !idx.IsExprKey(m) && idx.ColumnIDs[m] == r.cols[m] {
				m++
			}
		}
		eqUsed, useRange := m, false
		if m > len(r.eq) {
			eqUsed, useRange = len(r.eq), true
		}
		if eqUsed == 0 && !useRange {
			notices = append(notices, fmt.Sprintf("index %q does not begin with the primary-key columns PURGE WHERE names, so it was purged whole: its history is gone for every row of %q", idx.Name, desc.Name))
			continue
		}
		if m < len(r.cols) {
			var named []string
			for _, id := range r.cols[:m] {
				named = append(named, colName(desc, id))
			}
			notices = append(notices, fmt.Sprintf("index %q begins with only part of what PURGE WHERE names, so it was purged for every row with the same %s", idx.Name, strings.Join(named, ", ")))
		}
		add := func(k keys.Key, j int, d types.Datum) (keys.Key, error) {
			fam, _ := idx.KeyFamily(desc, j)
			return rowenc.AppendIndexKeyDatum(k, idx, fam, d)
		}
		sp, err := r.narrowed(keys.TableIndexPrefix(desc.ID, idx.ID), eqUsed, useRange, add)
		if err != nil {
			return nil, nil, keyErr(err)
		}
		ilo, ihi := keys.TableIndexSpan(desc.ID, idx.ID)
		keepOut = append(keepOut, spanMinus(ilo, ihi, []keySpan{sp})...)
	}
	tlo, thi := keys.TableDataSpan(desc.ID)
	return spanMinus(tlo, thi, keepOut), notices, nil
}

func colName(desc *catalog.TableDescriptor, id catalog.ColumnID) string {
	c, _ := desc.ColByID(id)
	return c.Name
}

// purgeSpans purges each span through the node's purge and merges the
// reports into one: complete only when every span's purge is.
func (s *Session) purgeSpans(ctx context.Context, spans []keySpan) (*kvpb.PurgeReport, error) {
	all := &kvpb.PurgeReport{Complete: true}
	for _, sp := range spans {
		rep, err := s.db.Purge(ctx, sp.start, sp.end)
		if err != nil {
			return nil, err
		}
		all.Ranges += rep.Ranges
		all.VersionsRemoved += rep.VersionsRemoved
		all.Replicas = append(all.Replicas, rep.Replicas...)
		all.Incomplete = append(all.Incomplete, rep.Incomplete...)
		all.Complete = all.Complete && rep.Complete
	}
	return all, nil
}
