package sql

import (
	"bytes"
	"context"
	"fmt"
	"strings"

	"github.com/sthorne/datax/pkg/keys"
	"github.com/sthorne/datax/pkg/kvclient"
	"github.com/sthorne/datax/pkg/sql/catalog"
	"github.com/sthorne/datax/pkg/sql/parser"
	"github.com/sthorne/datax/pkg/sql/rowenc"
	"github.com/sthorne/datax/pkg/sql/types"
)

// colBound is one end of a range constraint on a key column.
type colBound struct {
	val       types.Datum
	inclusive bool
}

// accessPlan is the chosen access path for one table read. There is no
// cost model: plans are ranked primary-key point > unique-index point >
// best constrained scan (2 points per equality-pinned column + 1 point for
// a range on the next column, ties preferring the primary key) > full scan.
type accessPlan struct {
	kind    planKind
	idx     *catalog.IndexDescriptor
	pkVals  []types.Datum // planPKPoint: one datum per PK column
	idxVals []types.Datum // pinned leading key column values (scans and unique points)
	eqCols  []string      // names of the pinned columns, for EXPLAIN

	// Range constraint on the key column immediately after the pinned
	// prefix: WHERE a = 1 AND b > 5 AND b <= 9 pins a and bounds b.
	lo, hi   *colBound
	rangeCol string // its name, for EXPLAIN

	// residual holds the conjuncts NOT guaranteed by the key bounds. The
	// executor always re-filters with the full WHERE clause (belt and
	// braces); residual only gates LIMIT pushdown into the KV scan.
	residual []parser.Comparison

	// fanBuckets (sharded timeseries planPKScan only): the scan runs once
	// per shard bucket — the pinned prefix and range bounds constrain the
	// LOGICAL primary key (PrimaryKey[1:]), and the executor prepends each
	// bucket value in turn. Fanned results are not in logical PK order.
	fanBuckets int32
}

type planKind int

const (
	planFullScan planKind = iota
	planPKPoint
	planUniquePoint
	planIndexScan
	planPKScan // primary-key prefix/range scan (no join needed)
)

// hasBounds reports whether the plan carries a range constraint.
func (p accessPlan) hasBounds() bool { return p.lo != nil || p.hi != nil }

// renderBoundDatum renders a bound value for EXPLAIN output.
func renderBoundDatum(d types.Datum) string {
	if d.Fam == types.String {
		return "'" + d.S + "'"
	}
	if d.Fam == types.Bool {
		if d.B {
			return "true"
		}
		return "false"
	}
	return d.Text()
}

// boundsString renders the pinned prefix and range bounds, e.g.
// "a = 1, b > 5, b <= 9".
func (p accessPlan) boundsString() string {
	var parts []string
	for i, name := range p.eqCols {
		parts = append(parts, fmt.Sprintf("%s = %s", name, renderBoundDatum(p.idxVals[i])))
	}
	if p.lo != nil {
		op := ">"
		if p.lo.inclusive {
			op = ">="
		}
		parts = append(parts, fmt.Sprintf("%s %s %s", p.rangeCol, op, renderBoundDatum(p.lo.val)))
	}
	if p.hi != nil {
		op := "<"
		if p.hi.inclusive {
			op = "<="
		}
		parts = append(parts, fmt.Sprintf("%s %s %s", p.rangeCol, op, renderBoundDatum(p.hi.val)))
	}
	return strings.Join(parts, ", ")
}

func (p accessPlan) String() string {
	switch p.kind {
	case planPKPoint:
		return "point lookup on primary key"
	case planUniquePoint:
		return fmt.Sprintf("point lookup via unique index %q", p.idx.Name)
	case planPKScan:
		out := fmt.Sprintf("range scan of primary key (%s)", p.boundsString())
		if p.fanBuckets > 0 {
			out += fmt.Sprintf(" (fan-out over %d shard buckets)", p.fanBuckets)
		}
		return out
	case planIndexScan:
		if p.hasBounds() {
			return fmt.Sprintf("scan of index %q (%s) + primary key join", p.idx.Name, p.boundsString())
		}
		return fmt.Sprintf("scan of index %q (%d column prefix) + primary key join", p.idx.Name, len(p.idxVals))
	}
	return "full table scan"
}

// planConj is one WHERE conjunct with its constraint value pre-evaluated.
type planConj struct {
	cmp    parser.Comparison
	col    catalog.Column
	d      types.Datum
	usable bool // comparison against a coerced, non-NULL value
}

// scanCandidate is one possible constrained scan while ranking paths.
type scanCandidate struct {
	idx      *catalog.IndexDescriptor // nil = primary key
	eqVals   []types.Datum
	eqCols   []string
	lo, hi   *colBound
	rangeCol string
	consumed map[int]bool // conjunct indices guaranteed by the bounds
	score    int
}

// tightenLo keeps the tighter of two lower bounds.
func tightenLo(cur *colBound, nb colBound) *colBound {
	if cur == nil {
		return &nb
	}
	if c, err := nb.val.Compare(cur.val); err == nil {
		if c > 0 || (c == 0 && !nb.inclusive) {
			return &nb
		}
	}
	return cur
}

// tightenHi keeps the tighter of two upper bounds.
func tightenHi(cur *colBound, nb colBound) *colBound {
	if cur == nil {
		return &nb
	}
	if c, err := nb.val.Compare(cur.val); err == nil {
		if c < 0 || (c == 0 && !nb.inclusive) {
			return &nb
		}
	}
	return cur
}

// pickPlan chooses the access path from the WHERE clause's equality and
// range conjuncts. The executor re-filters fetched rows with the complete
// WHERE clause regardless of path, so plan bounds only ever narrow the scan.
func pickPlan(desc *catalog.TableDescriptor, where []parser.Comparison, params []types.Datum) (accessPlan, error) {
	if pkVals, ok, err := pkPointValues(desc, where, params); err != nil {
		return accessPlan{}, err
	} else if ok {
		return accessPlan{kind: planPKPoint, pkVals: pkVals}, nil
	}

	// Evaluate every conjunct's comparison value once. `= NULL` never
	// matches and un-coercible values cannot either — such conjuncts are
	// unusable for bounds and left to the post-fetch filter.
	conjs := make([]planConj, len(where))
	for i, cmp := range where {
		if cmp.Column == "" {
			continue // constant conjuncts (TRUE/FALSE) bind no column
		}
		col, ok := desc.Col(cmp.Column)
		if !ok {
			return accessPlan{}, newErrf(CodeUndefinedColumn, "column %q does not exist", cmp.Column)
		}
		pc := planConj{cmp: cmp, col: col}
		switch cmp.Op {
		case "=", "<", "<=", ">", ">=":
			d, err := evalExpr(cmp.Value, nil, nil, params)
			if err != nil {
				return accessPlan{}, err
			}
			if d, cerr := d.Coerce(col.Type); cerr == nil && !d.Null {
				pc.d, pc.usable = d, true
			}
		}
		conjs[i] = pc
	}

	// nullSafe: rows where col is NULL provably cannot match the WHERE
	// clause (or cannot exist). Secondary-index entries are skipped when ANY
	// indexed column is NULL, so an index is a complete row source only for
	// null-safe indexed columns.
	nullSafe := func(col catalog.Column) bool {
		if col.NotNull || desc.IsPKCol(col.ID) {
			return true
		}
		for _, pc := range conjs {
			if pc.col.ID != col.ID {
				continue
			}
			if pc.usable || pc.cmp.Op == "IS NOT NULL" {
				return true
			}
		}
		return false
	}

	// First usable equality value per column.
	eq := map[catalog.ColumnID]types.Datum{}
	for _, pc := range conjs {
		if pc.cmp.Op == "=" && pc.usable {
			if _, dup := eq[pc.col.ID]; !dup {
				eq[pc.col.ID] = pc.d
			}
		}
	}

	// Unique index fully pinned by equality → point lookup.
	for i := range desc.Indexes {
		idx := &desc.Indexes[i]
		if !idx.Public() || !idx.Unique {
			continue
		}
		vals := make([]types.Datum, 0, len(idx.ColumnIDs))
		for _, colID := range idx.ColumnIDs {
			d, ok := eq[colID]
			if !ok {
				break
			}
			vals = append(vals, d)
		}
		if len(vals) == len(idx.ColumnIDs) {
			return accessPlan{kind: planUniquePoint, idx: idx, idxVals: vals}, nil
		}
	}

	// buildCandidate constrains the given key column order: an equality
	// prefix, then range bounds on the next column.
	buildCandidate := func(idx *catalog.IndexDescriptor, colIDs []catalog.ColumnID) scanCandidate {
		cand := scanCandidate{idx: idx, consumed: map[int]bool{}}
		pinned := 0
		for _, colID := range colIDs {
			d, ok := eq[colID]
			if !ok {
				break
			}
			col, _ := desc.ColByID(colID)
			cand.eqVals = append(cand.eqVals, d)
			cand.eqCols = append(cand.eqCols, col.Name)
			// Every equality conjunct pinning this column to the same value
			// is guaranteed by the span.
			for ci, pc := range conjs {
				if pc.cmp.Op == "=" && pc.usable && pc.col.ID == colID {
					if c, err := pc.d.Compare(d); err == nil && c == 0 {
						cand.consumed[ci] = true
					}
				}
			}
			pinned++
		}
		if pinned < len(colIDs) {
			rangeID := colIDs[pinned]
			for ci, pc := range conjs {
				if !pc.usable || pc.col.ID != rangeID {
					continue
				}
				switch pc.cmp.Op {
				case ">":
					cand.lo = tightenLo(cand.lo, colBound{val: pc.d})
				case ">=":
					cand.lo = tightenLo(cand.lo, colBound{val: pc.d, inclusive: true})
				case "<":
					cand.hi = tightenHi(cand.hi, colBound{val: pc.d})
				case "<=":
					cand.hi = tightenHi(cand.hi, colBound{val: pc.d, inclusive: true})
				default:
					continue
				}
				cand.consumed[ci] = true
			}
			if cand.lo != nil || cand.hi != nil {
				col, _ := desc.ColByID(rangeID)
				cand.rangeCol = col.Name
			}
		}
		cand.score = 2 * pinned
		if cand.lo != nil || cand.hi != nil {
			cand.score++
		}
		return cand
	}

	// Sharded timeseries tables plan against the LOGICAL primary key — the
	// hidden _shard column leads the real key but no query pins it; the
	// executor fans the resulting scan out across the buckets.
	pkCols := desc.PrimaryKey
	if desc.ShardBuckets > 0 && len(pkCols) > 1 {
		pkCols = pkCols[1:]
	}
	best := buildCandidate(nil, pkCols) // ties prefer the primary key (no join)
	for i := range desc.Indexes {
		idx := &desc.Indexes[i]
		if !idx.Public() {
			continue // write-only: maintained, but not readable yet
		}
		if !idx.Unique {
			// A non-unique index has no entry for rows with a NULL in ANY
			// indexed column: it is a complete row source only when every
			// indexed column is null-safe under this WHERE clause. (Unique
			// indexes reject NULLs at write time, so they are always
			// complete.)
			complete := true
			for _, colID := range idx.ColumnIDs {
				col, _ := desc.ColByID(colID)
				if !nullSafe(col) {
					complete = false
					break
				}
			}
			if !complete {
				continue
			}
		}
		if cand := buildCandidate(idx, idx.ColumnIDs); cand.score > best.score {
			best = cand
		}
	}

	if best.score == 0 {
		return accessPlan{kind: planFullScan, residual: where}, nil
	}
	plan := accessPlan{
		idx:      best.idx,
		idxVals:  best.eqVals,
		eqCols:   best.eqCols,
		lo:       best.lo,
		hi:       best.hi,
		rangeCol: best.rangeCol,
	}
	if best.idx == nil {
		plan.kind = planPKScan
		if desc.ShardBuckets > 0 {
			plan.fanBuckets = desc.ShardBuckets
		}
	} else {
		plan.kind = planIndexScan
	}
	for ci, cmp := range where {
		if !best.consumed[ci] {
			plan.residual = append(plan.residual, cmp)
		}
	}
	return plan, nil
}

// spanBounds builds the KV scan span for a constrained scan: the pinned
// prefix (already encoded into prefix by the caller), narrowed by the
// range bounds on the next key column of family fam.
func (p accessPlan) spanBounds(prefix keys.Key, fam types.Family) (start, end keys.Key, err error) {
	start = prefix.Clone()
	if p.lo != nil {
		start, err = rowenc.AppendKeyDatum(start, fam, p.lo.val)
		if err != nil {
			return nil, nil, err
		}
		if !p.lo.inclusive {
			// The encoding is self-delimiting, so PrefixEnd steps past
			// exactly the keys whose column equals the bound value.
			start = start.PrefixEnd()
		}
	}
	end = prefix.Clone()
	if p.hi != nil {
		end, err = rowenc.AppendKeyDatum(end, fam, p.hi.val)
		if err != nil {
			return nil, nil, err
		}
		if p.hi.inclusive {
			end = end.PrefixEnd()
		}
	} else {
		end = end.PrefixEnd()
	}
	return start, end, nil
}

// fetchByPrimaryKey reads and filters one row by its primary key.
func (s *Session) fetchByPrimaryKey(ctx context.Context, txn *kvclient.Txn, desc *catalog.TableDescriptor, pk []byte, where []parser.Comparison, params []types.Datum) ([]fetchedRow, error) {
	raw, err := txn.Get(ctx, pk)
	if err != nil || raw == nil {
		return nil, err
	}
	row, err := decodeFullRow(desc, pk, raw)
	if err != nil {
		return nil, err
	}
	match, err := matchesWhere(where, desc, row, params)
	if err != nil || !match {
		return nil, err
	}
	return []fetchedRow{{key: pk, row: row}}, nil
}

// ---------------------------------------------------------------------------
// Index maintenance. All entries ride the statement's WriteBatch, so index
// and row mutations commit atomically with the transaction.

// addIndexEntries buffers the row's entries in every secondary index.
// Unique conflicts are detected through the transaction (a racing insert's
// intent makes the conflict visible); seen catches duplicates within one
// statement whose writes are still buffered.
func addIndexEntries(ctx context.Context, txn *kvclient.Txn, desc *catalog.TableDescriptor, row map[catalog.ColumnID]types.Datum, wb *kvclient.WriteBatch, seen map[string]bool) error {
	for i := range desc.Indexes {
		idx := &desc.Indexes[i]
		key, val, skip, err := rowenc.EncodeIndexEntry(desc, idx, row)
		if err != nil {
			return newErrf(CodeInternal, "index %q: %v", idx.Name, err)
		}
		if skip {
			if idx.Unique {
				return newErrf(CodeNotNullViolation, "null value in column of unique index %q", idx.Name)
			}
			continue
		}
		if idx.Unique {
			if seen[string(key)] {
				return newErrf(CodeUniqueViolation, "duplicate key value violates unique constraint %q", idx.Name)
			}
			if existing, err := txn.Get(ctx, key); err != nil {
				return err
			} else if existing != nil {
				return newErrf(CodeUniqueViolation, "duplicate key value violates unique constraint %q", idx.Name)
			}
			seen[string(key)] = true
		}
		wb.Put(key, val)
	}
	return nil
}

// dropIndexEntries buffers deletion of the row's entries in every index.
func dropIndexEntries(desc *catalog.TableDescriptor, row map[catalog.ColumnID]types.Datum, wb *kvclient.WriteBatch) error {
	for i := range desc.Indexes {
		idx := &desc.Indexes[i]
		key, _, skip, err := rowenc.EncodeIndexEntry(desc, idx, row)
		if err != nil {
			return newErrf(CodeInternal, "index %q: %v", idx.Name, err)
		}
		if !skip {
			wb.Delete(key)
		}
	}
	return nil
}

// updateIndexEntries buffers the delete-old/put-new pair for every index
// whose entry changed between oldRow and newRow (the primary key is
// immutable under UPDATE, so only indexed-column changes move entries).
func updateIndexEntries(ctx context.Context, txn *kvclient.Txn, desc *catalog.TableDescriptor, oldRow, newRow map[catalog.ColumnID]types.Datum, wb *kvclient.WriteBatch, seen map[string]bool) error {
	for i := range desc.Indexes {
		idx := &desc.Indexes[i]
		oldKey, _, oldSkip, err := rowenc.EncodeIndexEntry(desc, idx, oldRow)
		if err != nil {
			return newErrf(CodeInternal, "index %q: %v", idx.Name, err)
		}
		newKey, newVal, newSkip, err := rowenc.EncodeIndexEntry(desc, idx, newRow)
		if err != nil {
			return newErrf(CodeInternal, "index %q: %v", idx.Name, err)
		}
		if newSkip && idx.Unique {
			return newErrf(CodeNotNullViolation, "null value in column of unique index %q", idx.Name)
		}
		if !oldSkip && !newSkip && bytes.Equal(oldKey, newKey) {
			continue // entry unchanged
		}
		if !oldSkip {
			wb.Delete(oldKey)
		}
		if !newSkip {
			if idx.Unique {
				if seen[string(newKey)] {
					return newErrf(CodeUniqueViolation, "duplicate key value violates unique constraint %q", idx.Name)
				}
				if existing, err := txn.Get(ctx, newKey); err != nil {
					return err
				} else if existing != nil {
					return newErrf(CodeUniqueViolation, "duplicate key value violates unique constraint %q", idx.Name)
				}
				seen[string(newKey)] = true
			}
			wb.Put(newKey, newVal)
		}
	}
	return nil
}

func copyRow(row map[catalog.ColumnID]types.Datum) map[catalog.ColumnID]types.Datum {
	out := make(map[catalog.ColumnID]types.Datum, len(row))
	for k, v := range row {
		out[k] = v
	}
	return out
}
