package sql

import (
	"bytes"
	"context"
	"fmt"

	"github.com/sthorne/datax/pkg/kvclient"
	"github.com/sthorne/datax/pkg/sql/catalog"
	"github.com/sthorne/datax/pkg/sql/parser"
	"github.com/sthorne/datax/pkg/sql/rowenc"
	"github.com/sthorne/datax/pkg/sql/types"
)

// accessPlan is the chosen access path for one table read. There is no
// cost model: plans are ranked primary-key point > unique-index point >
// index prefix scan (longest pinned prefix) > full scan.
type accessPlan struct {
	kind    planKind
	idx     *catalog.IndexDescriptor
	pkVals  []types.Datum // planPKPoint: one datum per PK column
	idxVals []types.Datum // pinned leading index column values
}

type planKind int

const (
	planFullScan planKind = iota
	planPKPoint
	planUniquePoint
	planIndexScan
)

func (p accessPlan) String() string {
	switch p.kind {
	case planPKPoint:
		return "point lookup on primary key"
	case planUniquePoint:
		return fmt.Sprintf("point lookup via unique index %q", p.idx.Name)
	case planIndexScan:
		return fmt.Sprintf("scan of index %q (%d column prefix) + primary key join", p.idx.Name, len(p.idxVals))
	}
	return "full table scan"
}

// pickPlan chooses the access path from the WHERE clause's equality
// conjuncts. Remaining conjuncts always filter after the fetch.
func pickPlan(desc *catalog.TableDescriptor, where []parser.Comparison, params []types.Datum) (accessPlan, error) {
	if pkVals, ok, err := pkPointValues(desc, where, params); err != nil {
		return accessPlan{}, err
	} else if ok {
		return accessPlan{kind: planPKPoint, pkVals: pkVals}, nil
	}

	// Equality-pinned columns (non-NULL, coercible values only: `= NULL`
	// never matches and un-coercible values cannot either — the post-fetch
	// filter handles both).
	eq := map[catalog.ColumnID]types.Datum{}
	for _, cmp := range where {
		if cmp.Op != "=" {
			continue
		}
		col, ok := desc.Col(cmp.Column)
		if !ok {
			return accessPlan{}, newErrf(CodeUndefinedColumn, "column %q does not exist", cmp.Column)
		}
		d, err := evalExpr(cmp.Value, nil, nil, params)
		if err != nil {
			return accessPlan{}, err
		}
		d, cerr := d.Coerce(col.Type)
		if cerr != nil || d.Null {
			continue
		}
		eq[col.ID] = d
	}

	pinned := func(idx *catalog.IndexDescriptor) []types.Datum {
		var vals []types.Datum
		for _, colID := range idx.ColumnIDs {
			d, ok := eq[colID]
			if !ok {
				break
			}
			vals = append(vals, d)
		}
		return vals
	}

	for i := range desc.Indexes {
		idx := &desc.Indexes[i]
		if !idx.Public() {
			continue // write-only: maintained, but not readable yet
		}
		if idx.Unique && len(pinned(idx)) == len(idx.ColumnIDs) {
			return accessPlan{kind: planUniquePoint, idx: idx, idxVals: pinned(idx)}, nil
		}
	}
	var best *catalog.IndexDescriptor
	var bestVals []types.Datum
	for i := range desc.Indexes {
		idx := &desc.Indexes[i]
		if !idx.Public() {
			continue
		}
		if vals := pinned(idx); len(vals) > len(bestVals) {
			best, bestVals = idx, vals
		}
	}
	if best != nil {
		return accessPlan{kind: planIndexScan, idx: best, idxVals: bestVals}, nil
	}
	return accessPlan{kind: planFullScan}, nil
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
