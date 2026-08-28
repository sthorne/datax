package sql

import (
	"context"
	"errors"
	"fmt"

	"github.com/sthorne/datax/pkg/keys"
	"github.com/sthorne/datax/pkg/kvclient"
	"github.com/sthorne/datax/pkg/sql/catalog"
	"github.com/sthorne/datax/pkg/sql/parser"
	"github.com/sthorne/datax/pkg/sql/rowenc"
	"github.com/sthorne/datax/pkg/sql/types"
)

// execStmt executes one data statement within txn. It is safe to re-run on
// transaction retry (all state flows through txn).
func (s *Session) execStmt(ctx context.Context, txn *kvclient.Txn, stmt parser.Statement, params []types.Datum) (*Result, error) {
	switch t := stmt.(type) {
	case *parser.CreateTable:
		return s.execCreateTable(ctx, txn, t)
	case *parser.CreateIndex:
		return s.execCreateIndex(ctx, txn, t)
	case *parser.Explain:
		return s.execExplain(ctx, txn, t, params)
	case *parser.DropTable:
		return s.execDropTable(ctx, txn, t)
	case *parser.Insert:
		return s.execInsert(ctx, txn, t, params)
	case *parser.Select:
		return s.execSelect(ctx, txn, t, params)
	case *parser.Update:
		return s.execUpdate(ctx, txn, t, params)
	case *parser.Delete:
		return s.execDelete(ctx, txn, t, params)
	case *parser.ShowTables:
		return s.execShowTables(ctx, txn)
	default:
		return nil, newErrf(CodeFeatureNotSupported, "unsupported statement %T", stmt)
	}
}

func (s *Session) execCreateTable(ctx context.Context, txn *kvclient.Txn, t *parser.CreateTable) (*Result, error) {
	desc := &catalog.TableDescriptor{Name: t.Name}
	seen := map[string]bool{}
	var colPK []string
	for i, cd := range t.Columns {
		if seen[cd.Name] {
			return nil, newErrf(CodeSyntaxError, "duplicate column %q", cd.Name)
		}
		seen[cd.Name] = true
		desc.Columns = append(desc.Columns, catalog.Column{
			ID: catalog.ColumnID(i + 1), Name: cd.Name, Type: cd.Type, NotNull: cd.NotNull,
		})
		if cd.PrimaryKey {
			colPK = append(colPK, cd.Name)
		}
	}
	pkNames := t.PrimaryKey
	if len(pkNames) == 0 {
		pkNames = colPK
	} else if len(colPK) > 0 {
		return nil, newErrf(CodeSyntaxError, "multiple primary key definitions")
	}
	if len(pkNames) == 0 {
		return nil, newErrf(CodeFeatureNotSupported, "table %q must declare a PRIMARY KEY", t.Name)
	}
	for _, name := range pkNames {
		col, ok := desc.Col(name)
		if !ok {
			return nil, newErrf(CodeUndefinedColumn, "primary key column %q does not exist", name)
		}
		desc.PrimaryKey = append(desc.PrimaryKey, col.ID)
		// PK columns are implicitly NOT NULL.
		for i := range desc.Columns {
			if desc.Columns[i].ID == col.ID {
				desc.Columns[i].NotNull = true
			}
		}
	}
	if err := s.cat.Create(ctx, txn, desc); err != nil {
		var ex *catalog.ErrTableExists
		if t.IfNotExists {
			if ok := asErr(err, &ex); ok {
				return &Result{Tag: "CREATE TABLE"}, nil
			}
		}
		return nil, err
	}
	return &Result{Tag: "CREATE TABLE"}, nil
}

func (s *Session) execDropTable(ctx context.Context, txn *kvclient.Txn, t *parser.DropTable) (*Result, error) {
	if _, err := s.cat.Drop(ctx, txn, t.Name); err != nil {
		var nf *catalog.ErrTableNotFound
		if t.IfExists && asErr(err, &nf) {
			return &Result{Tag: "DROP TABLE"}, nil
		}
		return nil, err
	}
	return &Result{Tag: "DROP TABLE"}, nil
}

func (s *Session) execInsert(ctx context.Context, txn *kvclient.Txn, t *parser.Insert, params []types.Datum) (*Result, error) {
	desc, err := s.cat.Lookup(ctx, txn, t.Table)
	if err != nil {
		return nil, err
	}
	// Resolve target columns.
	var target []catalog.Column
	if len(t.Columns) == 0 {
		target = desc.Columns
	} else {
		for _, name := range t.Columns {
			col, ok := desc.Col(name)
			if !ok {
				return nil, newErrf(CodeUndefinedColumn, "column %q does not exist", name)
			}
			target = append(target, col)
		}
	}
	count := 0
	var wb kvclient.WriteBatch
	inserted := map[string]bool{} // duplicates within this statement
	for _, exprRow := range t.Rows {
		if len(exprRow) != len(target) {
			return nil, newErrf(CodeSyntaxError, "INSERT has %d values but %d target columns", len(exprRow), len(target))
		}
		row := make(map[catalog.ColumnID]types.Datum, len(desc.Columns))
		for i, e := range exprRow {
			d, err := evalExpr(e, nil, nil, params)
			if err != nil {
				return nil, err
			}
			d, cerr := d.Coerce(target[i].Type)
			if cerr != nil {
				return nil, newErrf(CodeInternal, "column %q: %v", target[i].Name, cerr)
			}
			row[target[i].ID] = d
		}
		for _, col := range desc.Columns {
			d, ok := row[col.ID]
			if !ok {
				d = types.DNull
				row[col.ID] = d
			}
			if d.Null && col.NotNull {
				return nil, newErrf(CodeNotNullViolation, "null value in column %q violates not-null constraint", col.Name)
			}
		}
		key, verr := pkKey(desc, row)
		if verr != nil {
			return nil, verr
		}
		if inserted[string(key)] {
			return nil, newErrf(CodeUniqueViolation, "duplicate key value violates unique constraint on %q", t.Table)
		}
		if existing, err := txn.Get(ctx, key); err != nil {
			return nil, err
		} else if existing != nil {
			return nil, newErrf(CodeUniqueViolation, "duplicate key value violates unique constraint on %q", t.Table)
		}
		value, verr2 := rowenc.EncodeValue(desc, row)
		if verr2 != nil {
			return nil, verr2
		}
		// Writes are buffered and flushed once below: one routed batch (one
		// Raft proposal per touched range) per statement.
		wb.Put(key, value)
		if err := addIndexEntries(ctx, txn, desc, row, &wb, inserted); err != nil {
			return nil, err
		}
		inserted[string(key)] = true
		count++
	}
	if err := txn.RunBatch(ctx, &wb); err != nil {
		return nil, err
	}
	return &Result{Tag: fmt.Sprintf("INSERT 0 %d", count)}, nil
}

func pkKey(desc *catalog.TableDescriptor, row map[catalog.ColumnID]types.Datum) (keys.Key, error) {
	pkVals := make([]types.Datum, len(desc.PrimaryKey))
	for i, id := range desc.PrimaryKey {
		pkVals[i] = row[id]
	}
	return rowenc.EncodePK(desc, pkVals)
}

// fetchRows returns the full rows (colID → datum, including PK columns)
// matching the WHERE clause, along with their keys, in PK order.
type fetchedRow struct {
	key keys.Key
	row map[catalog.ColumnID]types.Datum
}

func (s *Session) fetchRows(ctx context.Context, txn *kvclient.Txn, desc *catalog.TableDescriptor, where []parser.Comparison, params []types.Datum, limit int64) ([]fetchedRow, error) {
	plan, err := pickPlan(desc, where, params)
	if err != nil {
		return nil, err
	}
	switch plan.kind {
	case planPKPoint:
		key, err := rowenc.EncodePK(desc, plan.pkVals)
		if err != nil {
			return nil, err
		}
		return s.fetchByPrimaryKey(ctx, txn, desc, key, where, params)

	case planUniquePoint:
		key, err := rowenc.EncodeIndexPrefix(desc, plan.idx, plan.idxVals)
		if err != nil {
			return nil, err
		}
		pkEnc, err := txn.Get(ctx, key)
		if err != nil || pkEnc == nil {
			return nil, err
		}
		pk := append(rowenc.PrimaryKeyPrefix(desc.ID), pkEnc...)
		rows, err := s.fetchByPrimaryKey(ctx, txn, desc, pk, where, params)
		if err != nil {
			return nil, err
		}
		if rows == nil {
			// The entry and the row commit atomically; a dangling entry is
			// corruption, not an empty result — unless the row was filtered.
			if raw, gerr := txn.Get(ctx, pk); gerr == nil && raw == nil {
				return nil, newErrf(CodeInternal, "index %q entry points at a missing row", plan.idx.Name)
			}
		}
		return rows, nil

	case planIndexScan:
		prefix, err := rowenc.EncodeIndexPrefix(desc, plan.idx, plan.idxVals)
		if err != nil {
			return nil, err
		}
		kvs, err := txn.Scan(ctx, prefix, prefix.PrefixEnd(), 0)
		if err != nil {
			return nil, err
		}
		var out []fetchedRow
		for _, kv := range kvs {
			pk, err := rowenc.IndexEntryPrimaryKey(desc, plan.idx, kv.Key, kv.Value)
			if err != nil {
				return nil, newErrf(CodeInternal, "%v", err)
			}
			raw, err := txn.Get(ctx, pk)
			if err != nil {
				return nil, err
			}
			if raw == nil {
				return nil, newErrf(CodeInternal, "index %q entry points at a missing row", plan.idx.Name)
			}
			row, err := decodeFullRow(desc, pk, raw)
			if err != nil {
				return nil, err
			}
			match, err := matchesWhere(where, desc, row, params)
			if err != nil {
				return nil, err
			}
			if !match {
				continue
			}
			out = append(out, fetchedRow{key: pk, row: row})
			if limit > 0 && int64(len(out)) == limit {
				break
			}
		}
		return out, nil
	}

	start, end := rowenc.PrimarySpan(desc.ID)
	kvs, err := txn.Scan(ctx, start, end, 0)
	if err != nil {
		return nil, err
	}
	var out []fetchedRow
	for _, kv := range kvs {
		row, err := decodeFullRow(desc, kv.Key, kv.Value)
		if err != nil {
			return nil, err
		}
		match, err := matchesWhere(where, desc, row, params)
		if err != nil {
			return nil, err
		}
		if !match {
			continue
		}
		out = append(out, fetchedRow{key: kv.Key, row: row})
		if limit > 0 && int64(len(out)) == limit {
			break
		}
	}
	return out, nil
}

// pkPointValues extracts PK datums when the WHERE clause pins every PK
// column with '='.
func pkPointValues(desc *catalog.TableDescriptor, where []parser.Comparison, params []types.Datum) ([]types.Datum, bool, error) {
	byCol := map[catalog.ColumnID]types.Datum{}
	for _, cmp := range where {
		if cmp.Op != "=" {
			continue
		}
		col, ok := desc.Col(cmp.Column)
		if !ok {
			return nil, false, newErrf(CodeUndefinedColumn, "column %q does not exist", cmp.Column)
		}
		if !desc.IsPKCol(col.ID) {
			continue
		}
		d, err := evalExpr(cmp.Value, nil, nil, params)
		if err != nil {
			return nil, false, err
		}
		d, cerr := d.Coerce(col.Type)
		if cerr != nil {
			return nil, false, nil // un-coercible: cannot match anything via point path; fall to scan
		}
		byCol[col.ID] = d
	}
	if len(byCol) != len(desc.PrimaryKey) {
		return nil, false, nil
	}
	out := make([]types.Datum, len(desc.PrimaryKey))
	for i, id := range desc.PrimaryKey {
		out[i] = byCol[id]
	}
	return out, true, nil
}

func decodeFullRow(desc *catalog.TableDescriptor, key keys.Key, value []byte) (map[catalog.ColumnID]types.Datum, error) {
	row, err := rowenc.DecodeValue(desc, value)
	if err != nil {
		return nil, err
	}
	pkVals, err := rowenc.DecodePK(desc, key)
	if err != nil {
		return nil, err
	}
	for i, id := range desc.PrimaryKey {
		row[id] = pkVals[i]
	}
	return row, nil
}

func (s *Session) execSelect(ctx context.Context, txn *kvclient.Txn, t *parser.Select, params []types.Datum) (*Result, error) {
	if t.Table == "" {
		// FROM-less SELECT: one row of evaluated expressions.
		res := &Result{}
		var row []types.Datum
		for _, se := range t.Exprs {
			if se.Star {
				return nil, newErrf(CodeSyntaxError, "SELECT * requires a FROM clause")
			}
			d, err := evalExpr(se.Expr, nil, nil, params)
			if err != nil {
				return nil, err
			}
			name := se.Alias
			if name == "" {
				name = "?column?"
			}
			fam := d.Fam
			if d.Null {
				fam = types.String
			}
			res.Columns = append(res.Columns, ResultColumn{Name: name, Type: fam})
			row = append(row, d)
		}
		res.Rows = [][]types.Datum{row}
		res.Tag = "SELECT 1"
		return res, nil
	}

	desc, err := s.cat.Lookup(ctx, txn, t.Table)
	if err != nil {
		return nil, err
	}
	proj, perr := resolveProjection(desc, t.Exprs)
	if perr != nil {
		return nil, perr
	}

	rows, err := s.fetchRows(ctx, txn, desc, t.Where, params, t.Limit)
	if err != nil {
		return nil, err
	}
	res := &Result{}
	for _, p := range proj {
		res.Columns = append(res.Columns, ResultColumn{Name: p.name, Type: p.col.Type})
	}
	for _, fr := range rows {
		out := make([]types.Datum, len(proj))
		for i, p := range proj {
			if p.expr != nil {
				d, err := evalExpr(*p.expr, desc, fr.row, params)
				if err != nil {
					return nil, err
				}
				out[i] = d
				continue
			}
			d, ok := fr.row[p.col.ID]
			if !ok {
				d = types.DNull
			}
			out[i] = d
		}
		res.Rows = append(res.Rows, out)
	}
	res.Tag = fmt.Sprintf("SELECT %d", len(res.Rows))
	return res, nil
}

func (s *Session) execUpdate(ctx context.Context, txn *kvclient.Txn, t *parser.Update, params []types.Datum) (*Result, error) {
	desc, err := s.cat.Lookup(ctx, txn, t.Table)
	if err != nil {
		return nil, err
	}
	for _, set := range t.Set {
		col, ok := desc.Col(set.Column)
		if !ok {
			return nil, newErrf(CodeUndefinedColumn, "column %q does not exist", set.Column)
		}
		if desc.IsPKCol(col.ID) {
			return nil, newErrf(CodeFeatureNotSupported, "updating primary key column %q is not supported", set.Column)
		}
	}
	rows, err := s.fetchRows(ctx, txn, desc, t.Where, params, 0)
	if err != nil {
		return nil, err
	}
	var wb kvclient.WriteBatch
	seen := map[string]bool{}
	for _, fr := range rows {
		oldRow := copyRow(fr.row)
		for _, set := range t.Set {
			col, _ := desc.Col(set.Column)
			d, err := evalExpr(set.Value, desc, fr.row, params)
			if err != nil {
				return nil, err
			}
			d, cerr := d.Coerce(col.Type)
			if cerr != nil {
				return nil, newErrf(CodeInternal, "column %q: %v", col.Name, cerr)
			}
			if d.Null && col.NotNull {
				return nil, newErrf(CodeNotNullViolation, "null value in column %q violates not-null constraint", col.Name)
			}
			fr.row[col.ID] = d
		}
		value, verr := rowenc.EncodeValue(desc, fr.row)
		if verr != nil {
			return nil, verr
		}
		wb.Put(fr.key, value)
		if err := updateIndexEntries(ctx, txn, desc, oldRow, fr.row, &wb, seen); err != nil {
			return nil, err
		}
	}
	if err := txn.RunBatch(ctx, &wb); err != nil {
		return nil, err
	}
	return &Result{Tag: fmt.Sprintf("UPDATE %d", len(rows))}, nil
}

func (s *Session) execDelete(ctx context.Context, txn *kvclient.Txn, t *parser.Delete, params []types.Datum) (*Result, error) {
	desc, err := s.cat.Lookup(ctx, txn, t.Table)
	if err != nil {
		return nil, err
	}
	rows, err := s.fetchRows(ctx, txn, desc, t.Where, params, 0)
	if err != nil {
		return nil, err
	}
	var wb kvclient.WriteBatch
	for _, fr := range rows {
		wb.Delete(fr.key)
		if err := dropIndexEntries(desc, fr.row, &wb); err != nil {
			return nil, err
		}
	}
	if err := txn.RunBatch(ctx, &wb); err != nil {
		return nil, err
	}
	return &Result{Tag: fmt.Sprintf("DELETE %d", len(rows))}, nil
}

// execCreateIndex adds a secondary index and backfills it from the
// table's current rows, all in one transaction. The backfill is offline:
// writers from other gateways that still hold the old descriptor are not
// blocked and will miss the index (documented limitation — no descriptor
// leases).
func (s *Session) execCreateIndex(ctx context.Context, txn *kvclient.Txn, t *parser.CreateIndex) (*Result, error) {
	shared, err := s.cat.Lookup(ctx, txn, t.Table)
	if err != nil {
		return nil, err
	}
	desc := shared.Clone()
	if t.Name == "primary" {
		return nil, newErrf(CodeSyntaxError, "index name %q is reserved", t.Name)
	}
	if _, exists := desc.Index(t.Name); exists {
		return nil, newErrf(CodeSyntaxError, "index %q already exists", t.Name)
	}
	var colIDs []catalog.ColumnID
	seenCol := map[catalog.ColumnID]bool{}
	for _, name := range t.Columns {
		col, ok := desc.Col(name)
		if !ok {
			return nil, newErrf(CodeUndefinedColumn, "column %q does not exist", name)
		}
		if seenCol[col.ID] {
			return nil, newErrf(CodeSyntaxError, "duplicate column %q in index", name)
		}
		seenCol[col.ID] = true
		colIDs = append(colIDs, col.ID)
	}
	if desc.NextIndexID < rowenc.PrimaryIndexID+1 {
		desc.NextIndexID = rowenc.PrimaryIndexID + 1
	}
	idx := catalog.IndexDescriptor{ID: desc.NextIndexID, Name: t.Name, Unique: t.Unique, ColumnIDs: colIDs}
	desc.NextIndexID++
	desc.Indexes = append(desc.Indexes, idx)

	// Backfill from the rows visible to this transaction.
	start, end := rowenc.PrimarySpan(desc.ID)
	kvs, err := txn.Scan(ctx, start, end, 0)
	if err != nil {
		return nil, err
	}
	var wb kvclient.WriteBatch
	seen := map[string]bool{}
	for _, kv := range kvs {
		row, err := decodeFullRow(desc, kv.Key, kv.Value)
		if err != nil {
			return nil, err
		}
		key, val, skip, err := rowenc.EncodeIndexEntry(desc, &idx, row)
		if err != nil {
			return nil, newErrf(CodeInternal, "index %q: %v", idx.Name, err)
		}
		if skip {
			if idx.Unique {
				return nil, newErrf(CodeNotNullViolation, "cannot create unique index %q: a row has NULL in an indexed column", idx.Name)
			}
			continue
		}
		if idx.Unique {
			if seen[string(key)] {
				return nil, newErrf(CodeUniqueViolation, "cannot create unique index %q: duplicate values exist", idx.Name)
			}
			seen[string(key)] = true
		}
		wb.Put(key, val)
	}
	if err := txn.RunBatch(ctx, &wb); err != nil {
		return nil, err
	}
	if err := s.cat.Update(ctx, txn, desc); err != nil {
		return nil, err
	}
	return &Result{Tag: "CREATE INDEX"}, nil
}

// execExplain describes the access plan of a SELECT without executing it.
func (s *Session) execExplain(ctx context.Context, txn *kvclient.Txn, t *parser.Explain, params []types.Datum) (*Result, error) {
	sel, ok := t.Stmt.(*parser.Select)
	if !ok || sel.Table == "" {
		return nil, newErrf(CodeFeatureNotSupported, "EXPLAIN supports table SELECT statements only")
	}
	desc, err := s.cat.Lookup(ctx, txn, sel.Table)
	if err != nil {
		return nil, err
	}
	plan, err := pickPlan(desc, sel.Where, params)
	if err != nil {
		return nil, err
	}
	return &Result{
		Columns: []ResultColumn{{Name: "plan", Type: types.String}},
		Rows:    [][]types.Datum{{types.NewString(plan.String())}},
		Tag:     "EXPLAIN",
	}, nil
}

func (s *Session) execShowTables(ctx context.Context, txn *kvclient.Txn) (*Result, error) {
	descs, err := s.cat.List(ctx, txn)
	if err != nil {
		return nil, err
	}
	res := &Result{Columns: []ResultColumn{{Name: "table_name", Type: types.String}}}
	for _, d := range descs {
		res.Rows = append(res.Rows, []types.Datum{types.NewString(d.Name)})
	}
	res.Tag = fmt.Sprintf("SHOW TABLES %d", len(res.Rows))
	return res, nil
}

// asErr is errors.As with less ceremony at call sites.
func asErr[T error](err error, target *T) bool {
	return errors.As(err, target)
}
