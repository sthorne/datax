package sql

import (
	"bytes"
	"context"
	"fmt"
	"strings"

	"github.com/sthorne/datax/pkg/keys"
	"github.com/sthorne/datax/pkg/kvclient"
	"github.com/sthorne/datax/pkg/sql/catalog"
	"github.com/sthorne/datax/pkg/sql/inverted"
	"github.com/sthorne/datax/pkg/sql/parser"
	"github.com/sthorne/datax/pkg/sql/rowenc"
	"github.com/sthorne/datax/pkg/sql/types"
	"github.com/sthorne/datax/pkg/version"
)

// colBound is one end of a range constraint on a key column.
type colBound struct {
	val       types.Datum
	inclusive bool
}

// accessPlan is the chosen access path for one table read. Without
// statistics, plans are ranked structurally: primary-key point >
// unique-index point > best constrained scan (2 points per
// equality-pinned column + 1 point for a range on the next column, ties
// preferring the primary key) > full scan. With statistics (ANALYZE or
// the background sampler), competing constrained scans and the full scan
// are ranked by estimated cost instead — see pickPlanWithStats. Where a
// scan of an inverted index fits in is said at pickPlanMode.
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

	// estRows is the statistics-based row estimate for this path (0 = no
	// statistics were used; the plan came from the structural fallback).
	// EXPLAIN renders it; execution ignores it.
	estRows float64

	// Execution-order directives, stamped by execSelect from the ORDER BY
	// decision (orderPlan) — never set by the planner itself. reverse
	// runs the scan(s) backwards (all-descending ORDER BY; only stamped
	// when the cluster-version gate allows reverse scans). mergeFan
	// (sharded planPKScan only) K-way-merges the per-bucket scans by key,
	// restoring logical-PK order — and, with a limit, stopping globally
	// after limit rows instead of concatenating whole buckets.
	reverse  bool
	mergeFan bool

	// top, also stamped by execSelect, asks the fetch to return only
	// the rows that sort first under the query's ORDER BY, up to its
	// LIMIT: by a top-k scan of the plan's span, or through a vector
	// index. See topk.go.
	top *topKSpec

	// inv (planInvertedScan) is what the WHERE clause says about the
	// inverted index's column: the element lookups whose result contains
	// every matching row. See inverted_scan.go.
	inv *inverted.Expr
}

type planKind int

const (
	planFullScan planKind = iota
	planPKPoint
	planUniquePoint
	planIndexScan
	planPKScan // primary-key prefix/range scan (no join needed)
	// planInvertedScan reads row references out of an inverted index,
	// fetches the rows and rechecks each against the WHERE clause.
	planInvertedScan
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
	case planInvertedScan:
		return invertedPlanString(p)
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
	return pickPlanWithStats(desc, nil, where, params)
}

// withLikeBounds derives key bounds from LIKE patterns with a literal
// prefix: col LIKE 'abc%' also satisfies col >= 'abc' AND col < 'abd',
// which the planner can turn into an index range (the LIKE itself
// still filters). Case-insensitive patterns and prefixes ending in the
// highest byte derive nothing.
func withLikeBounds(where []parser.Comparison) []parser.Comparison {
	var extra []parser.Comparison
	for _, cmp := range where {
		if cmp.Op != "LIKE" || cmp.Column == "" || len(cmp.Path) > 0 || cmp.Value.Lit == nil || cmp.Value.Lit.Fam != types.String {
			continue
		}
		prefix, ok := likePrefix(cmp.Value.Lit.S, cmp.Escape)
		if !ok || prefix == "" {
			continue
		}
		lo := types.NewString(prefix)
		extra = append(extra, parser.Comparison{Column: cmp.Column, Op: ">=", Value: parser.Expr{Lit: &lo}})
		if next, ok := nextPrefix(prefix); ok {
			hi := types.NewString(next)
			extra = append(extra, parser.Comparison{Column: cmp.Column, Op: "<", Value: parser.Expr{Lit: &hi}})
		}
	}
	if len(extra) == 0 {
		return where
	}
	return append(append([]parser.Comparison(nil), where...), extra...)
}

// likePrefix is the literal text before the first wildcard of a LIKE
// pattern (escapes resolved); ok is false when the pattern is one the
// planner should not touch.
func likePrefix(pattern, escape string) (string, bool) {
	esc, hasEsc := escapeRune(escape)
	var b strings.Builder
	escaped := false
	for _, r := range pattern {
		switch {
		case escaped:
			b.WriteRune(r)
			escaped = false
		case hasEsc && r == esc:
			escaped = true
		case r == '%' || r == '_':
			return b.String(), true
		default:
			b.WriteRune(r)
		}
	}
	if escaped {
		return "", false
	}
	return b.String(), true
}

// nextPrefix is the smallest string greater than every string with the
// prefix: the last byte incremented (bytes 0xff are dropped first).
func nextPrefix(prefix string) (string, bool) {
	b := []byte(prefix)
	for len(b) > 0 && b[len(b)-1] == 0xff {
		b = b[:len(b)-1]
	}
	if len(b) == 0 {
		return "", false
	}
	b[len(b)-1]++
	return string(b), true
}

// Cost-model constants (statistics present only). A non-unique index
// scan pays indexJoinCostMultiplier per estimated row for the per-entry
// primary-key Get that follows it; equality selectivity comes from the
// column's distinct count, ranges use a fixed fraction, and a column the
// statistics never saw gets a conservative guess. All deliberately
// simple: with NO statistics the structural ranking below runs
// byte-identically to the pre-statistics planner.
const (
	indexJoinCostMultiplier = 4
	rangeSelectivity        = 1.0 / 3
	unknownEqSelectivity    = 1.0 / 10
)

// pickPlanWithStats is pickPlan with optional table statistics: when st
// is non-nil, competing constrained scans are ranked by estimated cost
// (estimated rows × per-row cost) instead of the structural score, and a
// low-selectivity index scan loses to the full scan it would out-fetch.
func pickPlanWithStats(desc *catalog.TableDescriptor, st *catalog.TableStatistics, where []parser.Comparison, params []types.Datum) (accessPlan, error) {
	return pickPlanMode(desc, st, where, params, invertedAuto)
}

// pickPlanMode is pickPlanWithStats under an inverted_index_scan
// setting. Where an inverted index ranks: below both point lookups
// always. Without statistics, below a scan that pins a key column by
// equality and above everything else — a range alone or a full scan —
// since a containment or key test is usually the selective part of the
// clause it appears in. With statistics, by estimated cost like any
// other index scan, at a fixed assumed selectivity.
func pickPlanMode(desc *catalog.TableDescriptor, st *catalog.TableStatistics, where []parser.Comparison, params []types.Datum, mode invertedMode) (accessPlan, error) {
	userWhere := where
	where = withLikeBounds(where)
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
		if len(cmp.Path) > 0 {
			// A ->/->> conjunct constrains the extracted value, not the
			// column itself: never usable for key bounds (and never proof
			// the column matches anything) — post-fetch filter only.
			conjs[i] = pc
			continue
		}
		switch cmp.Op {
		// Any other operator is structurally unusable for key bounds and
		// runs as a post-fetch filter. (JSONB containment and key
		// existence can be served by an inverted index instead: see
		// pickInverted.)
		case "=", "<", "<=", ">", ">=":
			// A value that cannot be evaluated without a row (a column
			// reference on the right-hand side) is simply unusable for
			// bounds; the post-fetch filter evaluates it per row.
			if d, err := evalExpr(cmp.Value, nil, nil, params); err == nil {
				if d, cerr := coerceColumn(col, d); cerr == nil && !d.Null {
					pc.d, pc.usable = d, true
				}
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
		if !idx.Public() || !idx.Unique || idx.IsInverted() {
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
	// estRowsFor estimates the rows a candidate's bounds select: table
	// rows times the selectivity of each consumed conjunct (equalities by
	// 1/distinct, ranges by a fixed fraction; naive independence).
	tableRows := float64(1)
	if st != nil && st.RowCount > 0 {
		tableRows = float64(st.RowCount)
	}
	estRowsFor := func(cand scanCandidate) float64 {
		rows := tableRows
		for ci := range cand.consumed {
			pc := conjs[ci]
			switch pc.cmp.Op {
			case "=":
				sel := unknownEqSelectivity
				if cs, ok := st.Column(pc.col.ID); ok && cs.Distinct > 0 {
					sel = 1 / float64(cs.Distinct)
				}
				rows *= sel
			default: // range bounds
				rows *= rangeSelectivity
			}
		}
		if rows < 1 {
			rows = 1
		}
		return rows
	}
	// costFor: estimated rows times the per-row fetch cost — a non-unique
	// index entry costs an extra primary-key Get per row.
	costFor := func(cand scanCandidate) float64 {
		c := estRowsFor(cand)
		if cand.idx != nil && !cand.idx.Unique {
			c *= indexJoinCostMultiplier
		}
		return c
	}

	best := buildCandidate(nil, pkCols) // ties prefer the primary key (no join)
	bestCost := 0.0
	if st != nil {
		bestCost = costFor(best)
	}
	for i := range desc.Indexes {
		idx := &desc.Indexes[i]
		if !idx.Public() {
			continue // write-only: maintained, but not readable yet
		}
		if idx.IsInverted() {
			continue // no key order to bound; considered below
		}
		if idx.IsVector() {
			// Keyed by list, not by value: it serves ORDER BY distance
			// (pickVectorIndex), never a WHERE clause.
			continue
		}
		{
			// An index has no entry for rows with a NULL in ANY indexed
			// column — a unique one included, since NULLs are distinct
			// (and one declared NULLS NOT DISTINCT is held to the same
			// rule for simplicity): it is a complete row source only when
			// every indexed column is null-safe under this WHERE clause.
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
		cand := buildCandidate(idx, idx.ColumnIDs)
		if st == nil {
			if cand.score > best.score {
				best = cand
			}
			continue
		}
		// Cost-based: strictly cheaper wins; ties keep the earlier choice
		// (the PK candidate came first — no index→PK join on a tie).
		if cand.score > 0 && (best.score == 0 || costFor(cand) < bestCost) {
			best, bestCost = cand, costFor(cand)
		}
	}

	if mode != invertedOff {
		idx, inv, err := pickInverted(desc, userWhere, params)
		if err != nil {
			return accessPlan{}, err
		}
		if idx != nil {
			// Nothing is guaranteed by the lookups: every conjunct stays
			// residual, so no limit is pushed below the recheck.
			plan := accessPlan{kind: planInvertedScan, idx: idx, inv: inv, residual: userWhere}
			use := mode == invertedOn
			switch {
			case use:
			case st == nil:
				use = best.score < 2
			default:
				plan.estRows = tableRows * invertedSelectivity
				if plan.estRows < 1 {
					plan.estRows = 1
				}
				cost := plan.estRows * indexJoinCostMultiplier
				use = cost < tableRows && (best.score == 0 || cost < bestCost)
			}
			if use {
				return plan, nil
			}
		}
	}

	if best.score == 0 {
		plan := accessPlan{kind: planFullScan, residual: where}
		if st != nil {
			plan.estRows = tableRows
		}
		return plan, nil
	}
	// With statistics, a constrained scan that would fetch MORE than the
	// whole table (a low-selectivity index and its per-row PK joins) loses
	// to the full scan.
	if st != nil && bestCost >= tableRows {
		return accessPlan{kind: planFullScan, residual: where, estRows: tableRows}, nil
	}
	plan := accessPlan{
		idx:      best.idx,
		idxVals:  best.eqVals,
		eqCols:   best.eqCols,
		lo:       best.lo,
		hi:       best.hi,
		rangeCol: best.rangeCol,
	}
	if st != nil {
		plan.estRows = estRowsFor(best)
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
// range bounds on the next key column of family fam. On an index that
// keys NULLs (NULLS NOT DISTINCT) the span starts after the next
// column's NULL entries even without a lower bound: the planner only
// scans an index where a NULL cannot match, and a NULL entry inside the
// span would use up a pushed-down limit.
func (p accessPlan) spanBounds(prefix keys.Key, fam types.Family) (start, end keys.Key, err error) {
	start = prefix.Clone()
	if p.lo == nil && rowenc.TaggedIndexKey(p.idx) && len(p.idxVals) < len(p.idx.ColumnIDs) {
		start = append(start, rowenc.IndexKeyValueTag)
	}
	if p.lo != nil {
		start, err = rowenc.AppendIndexKeyDatum(start, p.idx, fam, p.lo.val)
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
		end, err = rowenc.AppendIndexKeyDatum(end, p.idx, fam, p.hi.val)
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
	row, err := decodeFullRow(ctx, s.rowSealer(), desc, pk, raw)
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

// invertedEntryErr turns a failure to list a row's inverted-index
// entries into the statement's error: the one a user can cause is a
// document with more elements than an index holds for one row.
func invertedEntryErr(idx *catalog.IndexDescriptor, err error) error {
	if idx.IsInverted() {
		return newErrf(CodeProgramLimitExceeded, "index %q: %v", idx.Name, err)
	}
	return newErrf(CodeInternal, "index %q: %v", idx.Name, err)
}

// reshardMirrorEntries returns the shadow-layout copies of a row's
// entries in index i during a re-shard's dual-write window: the same
// entries encoded at the index's shadow ID from shadowRow (which carries
// the new bucket in its hidden shard column). shadowRow == nil means no
// re-shard is pending — none. Mirrors carry no uniqueness checks of
// their own: they duplicate the live copy, whose checks already ran.
func reshardMirrorEntries(desc *catalog.TableDescriptor, i int, shadowRow map[catalog.ColumnID]types.Datum) ([]rowenc.IndexEntry, error) {
	if shadowRow == nil || desc.Reshard == nil || i >= len(desc.Reshard.NewIndexIDs) {
		return nil, nil
	}
	idx := &desc.Indexes[i]
	entries, err := rowenc.IndexEntriesAt(desc, idx, desc.Reshard.NewIndexIDs[i], shadowRow)
	if err != nil {
		return nil, newErrf(CodeInternal, "re-shard mirror of index %q: %v", idx.Name, err)
	}
	return entries, nil
}

// entryDiff splits two entry lists, each sorted by key with distinct
// keys, into the entries only the old one has and the entries only the
// new one has. An ordinary index has at most one entry per row, so this
// is "did the entry move"; an inverted index has one per element, and
// an UPDATE that changes one field of a document touches only the
// entries of that field. An entry whose key stays but whose value
// changes — a vector index's entry carries the row's vector, and a new
// vector is often filed under the same list — is written again: it
// counts as added, and the write replaces what is there.
func entryDiff(old, new []rowenc.IndexEntry) (removed, added []rowenc.IndexEntry) {
	i, j := 0, 0
	for i < len(old) && j < len(new) {
		switch c := bytes.Compare(old[i].Key, new[j].Key); {
		case c == 0:
			if !bytes.Equal(old[i].Value, new[j].Value) {
				added = append(added, new[j])
			}
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

// uniqueNullsDistinct reports whether a unique index takes rows with a
// NULL in an indexed column (PostgreSQL's NULLS DISTINCT: such a row has
// no entry and conflicts with nothing). Before cluster version v22 they
// are refused, as every earlier binary refuses them: a v21 planner takes
// a unique index for a complete row source and would miss such rows.
func uniqueNullsDistinct(txn *kvclient.Txn) bool {
	return txn == nil || txn.ClusterVersion() >= version.V22
}

// nullInUniqueIndex is the error for a NULL a unique index cannot take
// yet (uniqueNullsDistinct).
func nullInUniqueIndex(idx *catalog.IndexDescriptor) error {
	return newErrf(CodeNotNullViolation, "null value in column of unique index %q (NULLs in a unique index need cluster version %s: finalize the upgrade first)", idx.Name, version.V22)
}

// batchWriter is where a statement buffers its writes: the statement's
// kvclient.WriteBatch, or an UPDATE's updateWrites, which writes each
// key once.
type batchWriter interface {
	Put(key keys.Key, value []byte)
	Delete(key keys.Key)
}

// sameEntryKey reports whether a unique index's old entries (none or
// one) have key k.
func sameEntryKey(old []rowenc.IndexEntry, k keys.Key) bool {
	return len(old) == 1 && bytes.Equal(old[0].Key, k)
}

// addIndexEntries buffers the row's entries in every secondary index.
// Unique conflicts are detected through the transaction (a racing insert's
// intent makes the conflict visible); seen catches duplicates within one
// statement whose writes are still buffered. A nil seen says the caller
// has checked the row's unique keys itself (an UPDATE that moves rows
// checks the statement's keys together, checkUpdateKeys).
func addIndexEntries(ctx context.Context, txn *kvclient.Txn, desc *catalog.TableDescriptor, row map[catalog.ColumnID]types.Datum, wb batchWriter, seen map[string]bool, ps *probeSet) error {
	shadowRow, serr := reshardShadowRow(desc, row)
	if serr != nil {
		return newErrf(CodeInternal, "re-shard shadow row: %v", serr)
	}
	for i := range desc.Indexes {
		idx := &desc.Indexes[i]
		entries, err := rowenc.IndexEntries(desc, idx, row)
		if err != nil {
			return invertedEntryErr(idx, err)
		}
		if len(entries) == 0 {
			if idx.Unique && !uniqueNullsDistinct(txn) {
				return nullInUniqueIndex(idx)
			}
			continue
		}
		if idx.Unique && seen != nil {
			key := entries[0].Key
			if seen[string(key)] {
				return newErrf(CodeUniqueViolation, "duplicate key value violates unique constraint %q", idx.Name)
			}
			if existing, err := ps.get(ctx, txn, key); err != nil {
				return err
			} else if existing != nil {
				return newErrf(CodeUniqueViolation, "duplicate key value violates unique constraint %q", idx.Name)
			}
			seen[string(key)] = true
		}
		for _, e := range entries {
			wb.Put(e.Key, e.Value)
		}
		mirror, err := reshardMirrorEntries(desc, i, shadowRow)
		if err != nil {
			return err
		}
		for _, e := range mirror {
			wb.Put(e.Key, e.Value)
		}
	}
	return nil
}

// dropIndexEntries buffers deletion of the row's entries in every index.
func dropIndexEntries(desc *catalog.TableDescriptor, row map[catalog.ColumnID]types.Datum, wb batchWriter) error {
	shadowRow, serr := reshardShadowRow(desc, row)
	if serr != nil {
		return newErrf(CodeInternal, "re-shard shadow row: %v", serr)
	}
	for i := range desc.Indexes {
		idx := &desc.Indexes[i]
		entries, err := rowenc.IndexEntries(desc, idx, row)
		if err != nil {
			return invertedEntryErr(idx, err)
		}
		if len(entries) == 0 {
			continue
		}
		for _, e := range entries {
			wb.Delete(e.Key)
		}
		mirror, err := reshardMirrorEntries(desc, i, shadowRow)
		if err != nil {
			return err
		}
		for _, e := range mirror {
			wb.Delete(e.Key)
		}
	}
	return nil
}

// updateIndexEntries buffers, for every index, the deletion of the
// entries only oldRow has and the write of the entries only newRow has.
// An entry moves when its indexed columns change, or when the primary
// key does (a non-unique entry's key ends in it, a unique entry's value
// is it). A nil seen: the caller checked the unique keys (see
// addIndexEntries).
func updateIndexEntries(ctx context.Context, txn *kvclient.Txn, desc *catalog.TableDescriptor, oldRow, newRow map[catalog.ColumnID]types.Datum, wb batchWriter, seen map[string]bool, ps *probeSet) error {
	shadowOld, serr := reshardShadowRow(desc, oldRow)
	if serr != nil {
		return newErrf(CodeInternal, "re-shard shadow row: %v", serr)
	}
	shadowNew, serr := reshardShadowRow(desc, newRow)
	if serr != nil {
		return newErrf(CodeInternal, "re-shard shadow row: %v", serr)
	}
	for i := range desc.Indexes {
		idx := &desc.Indexes[i]
		if idx.IsVector() && len(idx.ColumnIDs) == 1 {
			// Filing a vector compares it with every centre. An UPDATE
			// that leaves the vector alone leaves its entry alone, and
			// need not work out twice where that entry is.
			o, n := oldRow[idx.ColumnIDs[0]], newRow[idx.ColumnIDs[0]]
			if o.Null == n.Null && o.Fam == n.Fam && o.S == n.S && !pkChanged(desc, oldRow, newRow) {
				continue
			}
		}
		oldEntries, err := rowenc.IndexEntries(desc, idx, oldRow)
		if err != nil {
			return invertedEntryErr(idx, err)
		}
		newEntries, err := rowenc.IndexEntries(desc, idx, newRow)
		if err != nil {
			return invertedEntryErr(idx, err)
		}
		if len(newEntries) == 0 && idx.Unique && !uniqueNullsDistinct(txn) {
			return nullInUniqueIndex(idx)
		}
		removed, added := entryDiff(oldEntries, newEntries)
		if len(removed) == 0 && len(added) == 0 {
			// Entries unchanged. The shadow entries are unchanged too:
			// the indexed columns and the primary key (which every entry
			// carries, in its key or its value) are what both layouts'
			// keys encode.
			continue
		}
		for _, e := range removed {
			wb.Delete(e.Key)
		}
		// A unique entry whose key stays and whose value changes (the
		// row's primary key moved) is the row's own: nothing to check.
		if idx.Unique && len(added) > 0 && seen != nil && !sameEntryKey(oldEntries, added[0].Key) {
			newKey := added[0].Key
			if seen[string(newKey)] {
				return newErrf(CodeUniqueViolation, "duplicate key value violates unique constraint %q", idx.Name)
			}
			if existing, err := ps.get(ctx, txn, newKey); err != nil {
				return err
			} else if existing != nil {
				return newErrf(CodeUniqueViolation, "duplicate key value violates unique constraint %q", idx.Name)
			}
			seen[string(newKey)] = true
		}
		for _, e := range added {
			wb.Put(e.Key, e.Value)
		}
		// The shadow layout takes the same difference, computed over its
		// own keys.
		mirrorOld, err := reshardMirrorEntries(desc, i, shadowOld)
		if err != nil {
			return err
		}
		mirrorNew, err := reshardMirrorEntries(desc, i, shadowNew)
		if err != nil {
			return err
		}
		mirrorRemoved, mirrorAdded := entryDiff(mirrorOld, mirrorNew)
		for _, e := range mirrorRemoved {
			wb.Delete(e.Key)
		}
		for _, e := range mirrorAdded {
			wb.Put(e.Key, e.Value)
		}
	}
	return nil
}

// appendUniqueIndexKeys appends the row's entry keys in every unique
// index (the keys an insert probes), for batching those probes.
func appendUniqueIndexKeys(ks []keys.Key, desc *catalog.TableDescriptor, row map[catalog.ColumnID]types.Datum) []keys.Key {
	for i := range desc.Indexes {
		idx := &desc.Indexes[i]
		if !idx.Unique {
			continue
		}
		key, _, skip, err := rowenc.EncodeIndexEntry(desc, idx, row)
		if err == nil && !skip {
			ks = append(ks, key)
		}
	}
	return ks
}

// appendMovedUniqueIndexKeys appends the new entry keys of the unique
// indexes whose entry changes between oldRow and newRow (the keys an
// update probes).
func appendMovedUniqueIndexKeys(ks []keys.Key, desc *catalog.TableDescriptor, oldRow, newRow map[catalog.ColumnID]types.Datum) []keys.Key {
	for i := range desc.Indexes {
		idx := &desc.Indexes[i]
		if !idx.Unique {
			continue
		}
		oldKey, _, oldSkip, err := rowenc.EncodeIndexEntry(desc, idx, oldRow)
		if err != nil {
			continue
		}
		newKey, _, newSkip, err := rowenc.EncodeIndexEntry(desc, idx, newRow)
		if err != nil || newSkip {
			continue
		}
		if !oldSkip && bytes.Equal(oldKey, newKey) {
			continue
		}
		ks = append(ks, newKey)
	}
	return ks
}

// pkChanged reports whether a row's primary-key columns differ between
// two versions of it.
func pkChanged(desc *catalog.TableDescriptor, oldRow, newRow map[catalog.ColumnID]types.Datum) bool {
	for _, id := range desc.PrimaryKey {
		if datumChanged(rowVal(oldRow, id), rowVal(newRow, id)) {
			return true
		}
	}
	return false
}

func copyRow(row map[catalog.ColumnID]types.Datum) map[catalog.ColumnID]types.Datum {
	out := make(map[catalog.ColumnID]types.Datum, len(row))
	for k, v := range row {
		out[k] = v
	}
	return out
}
