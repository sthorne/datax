package sql

import (
	"context"
	"errors"
	"fmt"
	"strconv"

	"github.com/sthorne/datax/pkg/keys"
	"github.com/sthorne/datax/pkg/kvclient"
	"github.com/sthorne/datax/pkg/metrics"
	"github.com/sthorne/datax/pkg/sql/catalog"
	"github.com/sthorne/datax/pkg/sql/parser"
	"github.com/sthorne/datax/pkg/sql/rowenc"
	"github.com/sthorne/datax/pkg/sql/types"
	"github.com/sthorne/datax/pkg/util/log"
)

// execStmt executes one data statement within txn. It is safe to re-run on
// transaction retry (all state flows through txn).
func (s *Session) execStmt(ctx context.Context, txn *kvclient.Txn, stmt parser.Statement, params []types.Datum) (*Result, error) {
	if requiresAdmin(stmt) {
		if err := s.checkAdmin(ctx, txn); err != nil {
			return nil, err
		}
	}
	switch t := stmt.(type) {
	case *parser.GrantRevoke:
		return s.execGrantRevoke(ctx, txn, t)
	case *parser.CreateTable:
		return s.execCreateTable(ctx, txn, t)
	case *parser.Explain:
		return s.execExplain(ctx, txn, t, params)
	case *parser.AlterTable:
		return s.execAlterTable(ctx, txn, t)
	case *parser.CreateUser:
		return s.execCreateUser(ctx, txn, t)
	case *parser.DropUser:
		return s.execDropUser(ctx, txn, t)
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
		col := catalog.Column{
			ID: catalog.ColumnID(i + 1), Name: cd.Name, Type: cd.Type, NotNull: cd.NotNull,
		}
		if cd.Default != nil && !cd.Default.Null {
			d, cerr := cd.Default.Coerce(cd.Type)
			if cerr != nil {
				return nil, newErrf(CodeSyntaxError, "DEFAULT for column %q: %v", cd.Name, cerr)
			}
			col.Default = &d
		}
		desc.Columns = append(desc.Columns, col)
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
	if err := applyTableOptions(desc, t.Options, pkNames); err != nil {
		return nil, err
	}
	if desc.ShardBuckets > 0 {
		// The hidden bucket column leads the primary key; the executor
		// fills it from the logical PK on every insert.
		shard := catalog.Column{
			ID:      catalog.ColumnID(len(desc.Columns) + 1),
			Name:    rowenc.ShardColumnName,
			Type:    types.Int,
			NotNull: true,
			Hidden:  true,
		}
		desc.Columns = append(desc.Columns, shard)
		pkNames = append([]string{rowenc.ShardColumnName}, pkNames...)
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
	desc.NextColumnID = catalog.ColumnID(len(desc.Columns) + 1)
	if err := s.cat.Create(ctx, txn, desc); err != nil {
		var ex *catalog.ErrTableExists
		if t.IfNotExists {
			if ok := asErr(err, &ex); ok {
				return &Result{Tag: "CREATE TABLE"}, nil
			}
		}
		return nil, err
	}
	s.presplitTimeseries(ctx, desc)
	return &Result{Tag: "CREATE TABLE"}, nil
}

// applyTableOptions validates the WITH (...) options and stamps the
// timeseries fields onto desc (before the hidden shard column is added).
func applyTableOptions(desc *catalog.TableDescriptor, options map[string]string, pkNames []string) error {
	if len(options) == 0 {
		return nil
	}
	for name := range options {
		switch name {
		case "timeseries", "retention", "shards":
		default:
			return newErrf(CodeSyntaxError, "unknown table option %q", name)
		}
	}
	switch options["timeseries"] {
	case "true":
	case "", "false":
		return newErrf(CodeSyntaxError, "table options require timeseries=true")
	default:
		return newErrf(CodeSyntaxError, "timeseries must be true or false")
	}
	desc.Timeseries = true

	// The timestamp must close the (logical) primary key so each series'
	// rows are stored in time order and retention trims a key suffix.
	last, ok := desc.Col(pkNames[len(pkNames)-1])
	if !ok || last.Type != types.Timestamp {
		return newErrf(CodeFeatureNotSupported, "a timeseries table's last primary key column must be TIMESTAMPTZ")
	}

	if v, ok := options["retention"]; ok {
		secs, err := parseRetention(v)
		if err != nil {
			return newErrf(CodeSyntaxError, "retention: %v", err)
		}
		desc.RetentionSeconds = secs
	}
	if v, ok := options["shards"]; ok {
		n, err := strconv.Atoi(v)
		if err != nil || n < 2 || n > 256 {
			return newErrf(CodeSyntaxError, "shards must be an integer in [2, 256]")
		}
		if _, exists := desc.Col(rowenc.ShardColumnName); exists {
			return newErrf(CodeSyntaxError, "column name %q is reserved for sharded timeseries tables", rowenc.ShardColumnName)
		}
		desc.ShardBuckets = int32(n)
	}
	return nil
}

// parseRetention parses a duration like 90d, 36h, 30m, or 45s into
// seconds.
func parseRetention(s string) (int64, error) {
	if len(s) < 2 {
		return 0, fmt.Errorf("want <number><d|h|m|s>, got %q", s)
	}
	n, err := strconv.ParseInt(s[:len(s)-1], 10, 64)
	if err != nil || n <= 0 {
		return 0, fmt.Errorf("want a positive number before the unit in %q", s)
	}
	var mult int64
	switch s[len(s)-1] {
	case 'd':
		mult = 24 * 3600
	case 'h':
		mult = 3600
	case 'm':
		mult = 60
	case 's':
		mult = 1
	default:
		return 0, fmt.Errorf("unit must be d, h, m, or s in %q", s)
	}
	if n > (1<<62)/mult {
		return 0, fmt.Errorf("retention %q overflows", s)
	}
	return n * mult, nil
}

// presplitTimeseries pre-carves a fresh timeseries table's key space:
// splits at the table's span boundaries (so ranges rarely straddle the
// table edge — the common case for per-table retention GC is then "range
// fully inside the table") and, for sharded tables, at each bucket
// prefix, so ingest parallelizes across ranges immediately instead of
// after autosplit catches up. Best-effort: splits are non-transactional
// and outlive an aborted enclosing txn, which is harmless — an aborted
// CREATE leaves empty ranges that the size-based merger re-absorbs.
func (s *Session) presplitTimeseries(ctx context.Context, desc *catalog.TableDescriptor) {
	if !desc.Timeseries || desc.ID == 0 {
		return
	}
	splitAt := []keys.Key{
		keys.TableDataPrefix(desc.ID),
		keys.TableDataPrefix(desc.ID).PrefixEnd(),
	}
	for b := int32(1); b < desc.ShardBuckets; b++ {
		k, err := rowenc.AppendKeyDatum(rowenc.PrimaryKeyPrefix(desc.ID), types.Int, types.NewInt(int64(b)))
		if err != nil {
			return
		}
		splitAt = append(splitAt, k)
	}
	for _, k := range splitAt {
		if _, err := s.db.AdminSplit(ctx, k); err != nil {
			log.Debugf("timeseries pre-split at %s: %v", k, err)
		}
	}
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
	t, err := s.resolveInsertSubs(ctx, txn, t, params)
	if err != nil {
		return nil, err
	}
	desc, err := s.cat.Lookup(ctx, txn, t.Table)
	if err != nil {
		return nil, err
	}
	if err := s.checkTablePriv(ctx, txn, desc, "INSERT"); err != nil {
		return nil, err
	}
	// Resolve target columns. Hidden columns (the _shard bucket) are never
	// insert targets: implicitly they are skipped, explicitly they error —
	// the executor computes them.
	var target []catalog.Column
	if len(t.Columns) == 0 {
		target = desc.VisibleColumns()
	} else {
		for _, name := range t.Columns {
			col, ok := desc.Col(name)
			if !ok {
				return nil, newErrf(CodeUndefinedColumn, "column %q does not exist", name)
			}
			if col.Hidden {
				return nil, newErrf(CodeSyntaxError, "column %q is system-managed and cannot be inserted", name)
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
				if col.Hidden && col.Name == rowenc.ShardColumnName && desc.ShardBuckets > 0 {
					// The shard bucket hashes the logical PK. It is the last
					// column in desc.Columns, so every logical PK datum is
					// already filled (and NOT-NULL-checked) by this point.
					logical := make([]types.Datum, len(desc.PrimaryKey)-1)
					for i, id := range desc.PrimaryKey[1:] {
						logical[i] = row[id]
					}
					sd, serr := rowenc.ShardBucket(desc, logical)
					if serr != nil {
						return nil, serr
					}
					row[col.ID] = sd
					continue
				}
				// An omitted column takes its DEFAULT (NULL when none).
				d = types.DNull
				if col.Default != nil {
					d = *col.Default
				}
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

func (s *Session) fetchRows(ctx context.Context, txn *kvclient.Txn, desc *catalog.TableDescriptor, where []parser.Comparison, params []types.Datum, limit int64) ([]fetchedRow, accessPlan, error) {
	plan, err := pickPlan(desc, where, params)
	if err != nil {
		return nil, plan, err
	}
	rows, err := s.executePlan(ctx, txn, desc, plan, where, params, limit)
	return rows, plan, err
}

func (s *Session) executePlan(ctx context.Context, txn *kvclient.Txn, desc *catalog.TableDescriptor, plan accessPlan, where []parser.Comparison, params []types.Datum, limit int64) ([]fetchedRow, error) {
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
		var fam types.Family
		if plan.hasBounds() {
			col, _ := desc.ColByID(plan.idx.ColumnIDs[len(plan.idxVals)])
			fam = col.Type
		}
		start, end, err := plan.spanBounds(prefix, fam)
		if err != nil {
			return nil, err
		}
		// Index entries are 1:1 with rows, so with no residual filter every
		// scanned entry yields a result row: the limit pushes into the scan.
		var scanLimit int64
		if len(plan.residual) == 0 {
			scanLimit = limit
		}
		kvs, err := txn.Scan(ctx, start, end, scanLimit)
		if err != nil {
			return nil, err
		}
		metrics.SQLRowsScanned.Add(float64(len(kvs)))
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

	case planPKScan:
		// The plan's pinned prefix and bounds constrain the logical PK; on
		// a sharded table (fanBuckets > 0) that is PrimaryKey[1:], and the
		// scan runs once per bucket with the bucket value prepended.
		pkCols := desc.PrimaryKey
		if plan.fanBuckets > 0 {
			pkCols = pkCols[1:]
		}
		buildSpan := func(prefix keys.Key) (keys.Key, keys.Key, error) {
			for i, d := range plan.idxVals {
				col, _ := desc.ColByID(pkCols[i])
				var err error
				prefix, err = rowenc.AppendKeyDatum(prefix, col.Type, d)
				if err != nil {
					return nil, nil, newErrf(CodeInternal, "pk bound: %v", err)
				}
			}
			var fam types.Family
			if plan.hasBounds() {
				col, _ := desc.ColByID(pkCols[len(plan.idxVals)])
				fam = col.Type
			}
			start, end, err := plan.spanBounds(prefix, fam)
			if err != nil {
				return nil, nil, newErrf(CodeInternal, "pk bound: %v", err)
			}
			return start, end, nil
		}
		if plan.fanBuckets == 0 {
			start, end, err := buildSpan(rowenc.PrimaryKeyPrefix(desc.ID))
			if err != nil {
				return nil, err
			}
			return s.scanPrimarySpan(ctx, txn, desc, plan, start, end, where, params, limit)
		}
		var out []fetchedRow
		for b := int32(0); b < plan.fanBuckets; b++ {
			bp, err := rowenc.AppendKeyDatum(rowenc.PrimaryKeyPrefix(desc.ID), types.Int, types.NewInt(int64(b)))
			if err != nil {
				return nil, newErrf(CodeInternal, "shard bound: %v", err)
			}
			start, end, err := buildSpan(bp)
			if err != nil {
				return nil, err
			}
			// The limit is only an upper bound per span (each span alone
			// cannot yield more result rows than the global limit); the
			// global limit re-applies to the concatenation below — and the
			// caller re-applies it after sorting when there is an ORDER BY,
			// in which case it passes limit 0 here.
			rows, err := s.scanPrimarySpan(ctx, txn, desc, plan, start, end, where, params, limit)
			if err != nil {
				return nil, err
			}
			out = append(out, rows...)
		}
		if limit > 0 && int64(len(out)) > limit {
			out = out[:limit]
		}
		return out, nil
	}

	start, end := rowenc.PrimarySpan(desc.ID)
	return s.scanPrimarySpan(ctx, txn, desc, plan, start, end, where, params, limit)
}

// scanPrimarySpan scans [start, end) of the primary index, filters with the
// full WHERE clause, and stops at limit. The limit is pushed into the KV
// scan itself when the plan has no residual filter (every scanned row is a
// result row).
func (s *Session) scanPrimarySpan(ctx context.Context, txn *kvclient.Txn, desc *catalog.TableDescriptor, plan accessPlan, start, end keys.Key, where []parser.Comparison, params []types.Datum, limit int64) ([]fetchedRow, error) {
	var scanLimit int64
	if len(plan.residual) == 0 {
		scanLimit = limit
	}
	kvs, err := txn.Scan(ctx, start, end, scanLimit)
	if err != nil {
		return nil, err
	}
	metrics.SQLRowsScanned.Add(float64(len(kvs)))
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
		if !desc.IsPKCol(col.ID) || col.Hidden {
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
	// Sharded timeseries: the query pins the LOGICAL primary key; the
	// hidden leading _shard value is recomputed from it, so a fully-pinned
	// lookup stays a single point read.
	pkCols := desc.PrimaryKey
	if desc.ShardBuckets > 0 && len(pkCols) > 1 {
		pkCols = pkCols[1:]
	}
	if len(byCol) != len(pkCols) {
		return nil, false, nil
	}
	out := make([]types.Datum, len(pkCols))
	for i, id := range pkCols {
		out[i] = byCol[id]
	}
	if desc.ShardBuckets > 0 {
		bucket, err := rowenc.ShardBucket(desc, out)
		if err != nil {
			return nil, false, err
		}
		out = append([]types.Datum{bucket}, out...)
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
	t, err := s.resolveSelectSubs(ctx, txn, t, params)
	if err != nil {
		return nil, err
	}
	if t.Derived != nil {
		return s.execDerivedSelect(ctx, txn, t, params)
	}
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
	if err := s.checkTablePriv(ctx, txn, desc, "SELECT"); err != nil {
		return nil, err
	}
	if t.Join != nil {
		return s.execJoinSelect(ctx, txn, desc, t, params)
	}
	if hasAggregates(t.Exprs) || len(t.GroupBy) > 0 {
		if t.ForUpdate {
			return nil, newErrf(CodeFeatureNotSupported, "FOR UPDATE is not allowed with GROUP BY or aggregate functions")
		}
		return s.execGroupedSelect(ctx, txn, desc, t, params)
	}
	if len(t.Having) > 0 {
		return nil, newErrf(CodeGrouping, "HAVING requires GROUP BY or aggregate functions")
	}
	if t.Distinct && t.ForUpdate {
		return nil, newErrf(CodeFeatureNotSupported, "FOR UPDATE is not allowed with DISTINCT")
	}
	proj, perr := resolveProjection(desc, t.Exprs)
	if perr != nil {
		return nil, perr
	}

	// With ORDER BY the limit applies only after sorting (unless the access
	// path already delivers the requested order); with DISTINCT only after
	// deduplication.
	fetchLimit := t.Limit
	if t.Distinct {
		fetchLimit = 0
	}
	needSort := false
	if len(t.OrderBy) > 0 {
		plan, err := pickPlan(desc, t.Where, params)
		if err != nil {
			return nil, err
		}
		needSort = !orderSatisfiedByPlan(desc, plan, t.OrderBy)
		if needSort {
			fetchLimit = 0
		}
	}
	rows, _, err := s.fetchRows(ctx, txn, desc, t.Where, params, fetchLimit)
	if err != nil {
		return nil, err
	}
	if t.ForUpdate {
		// Lock each selected row: the locking read re-verifies the row at
		// the transaction's read timestamp server-side (any newer committed
		// version surfaces as a retryable conflict), so the fetch-then-lock
		// gap cannot admit a stale read.
		for _, fr := range rows {
			if _, lerr := txn.GetForUpdate(ctx, fr.key); lerr != nil {
				return nil, lerr
			}
		}
	}
	if needSort {
		if err := sortRows(desc, rows, t.OrderBy); err != nil {
			return nil, err
		}
		if !t.Distinct && t.Limit > 0 && int64(len(rows)) > t.Limit {
			rows = rows[:t.Limit]
		}
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
	if t.Distinct {
		// Degenerate grouping over the projection: keep first occurrences
		// (rows are already in the requested order), then apply the limit.
		res.Rows = dedupeRows(res.Rows)
		if t.Limit > 0 && int64(len(res.Rows)) > t.Limit {
			res.Rows = res.Rows[:t.Limit]
		}
	}
	res.Tag = fmt.Sprintf("SELECT %d", len(res.Rows))
	return res, nil
}

func (s *Session) execUpdate(ctx context.Context, txn *kvclient.Txn, t *parser.Update, params []types.Datum) (*Result, error) {
	t, err := s.resolveUpdateSubs(ctx, txn, t, params)
	if err != nil {
		return nil, err
	}
	desc, err := s.cat.Lookup(ctx, txn, t.Table)
	if err != nil {
		return nil, err
	}
	if err := s.checkTablePriv(ctx, txn, desc, "UPDATE"); err != nil {
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
	rows, _, err := s.fetchRows(ctx, txn, desc, t.Where, params, 0)
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
	t, err := s.resolveDeleteSubs(ctx, txn, t, params)
	if err != nil {
		return nil, err
	}
	desc, err := s.cat.Lookup(ctx, txn, t.Table)
	if err != nil {
		return nil, err
	}
	if err := s.checkTablePriv(ctx, txn, desc, "DELETE"); err != nil {
		return nil, err
	}
	rows, _, err := s.fetchRows(ctx, txn, desc, t.Where, params, 0)
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

// execAlterTable adds or drops a column. Adds are nullable-only (existing
// rows simply decode the new column as NULL); drops are lazy (the bytes
// stay in old row values and are skipped on decode) and refused for
// primary-key or indexed columns. Column IDs are never reused, so a
// drop-then-re-add cannot resurrect old values.
func (s *Session) execAlterTable(ctx context.Context, txn *kvclient.Txn, t *parser.AlterTable) (*Result, error) {
	shared, err := s.cat.Lookup(ctx, txn, t.Table)
	if err != nil {
		return nil, err
	}
	desc := shared.Clone()
	if desc.NextColumnID == 0 {
		var max catalog.ColumnID
		for _, c := range desc.Columns {
			if c.ID > max {
				max = c.ID
			}
		}
		desc.NextColumnID = max + 1
	}
	switch {
	case t.AddCol != nil:
		def := t.AddCol
		if def.PrimaryKey {
			return nil, newErrf(CodeFeatureNotSupported, "ADD COLUMN cannot add a primary key column")
		}
		if def.NotNull && (def.Default == nil || def.Default.Null) {
			return nil, newErrf(CodeFeatureNotSupported, "ADD COLUMN ... NOT NULL requires a DEFAULT (existing rows need a value)")
		}
		if _, exists := desc.Col(def.Name); exists {
			return nil, newErrf(CodeSyntaxError, "column %q already exists", def.Name)
		}
		col := catalog.Column{
			ID: desc.NextColumnID, Name: def.Name, Type: def.Type, NotNull: def.NotNull,
		}
		if def.Default != nil && !def.Default.Null {
			d, cerr := def.Default.Coerce(def.Type)
			if cerr != nil {
				return nil, newErrf(CodeSyntaxError, "DEFAULT for column %q: %v", def.Name, cerr)
			}
			// Fill-on-read: rows written before this ADD lack the column and
			// decode as the default; NULLs written afterwards are stored
			// explicitly so they stay NULL.
			col.Default, col.FillDefault = &d, true
		}
		desc.Columns = append(desc.Columns, col)
		desc.NextColumnID++
	case t.DropCol != "":
		col, ok := desc.Col(t.DropCol)
		if !ok {
			return nil, newErrf(CodeUndefinedColumn, "column %q does not exist", t.DropCol)
		}
		if desc.IsPKCol(col.ID) {
			return nil, newErrf(CodeFeatureNotSupported, "cannot drop primary key column %q", t.DropCol)
		}
		for _, idx := range desc.Indexes {
			for _, id := range idx.ColumnIDs {
				if id == col.ID {
					return nil, newErrf(CodeFeatureNotSupported, "cannot drop column %q: used by index %q", t.DropCol, idx.Name)
				}
			}
		}
		kept := desc.Columns[:0]
		for _, c := range desc.Columns {
			if c.ID != col.ID {
				kept = append(kept, c)
			}
		}
		desc.Columns = kept
	default:
		return nil, newErrf(CodeSyntaxError, "ALTER TABLE requires ADD or DROP COLUMN")
	}
	if err := s.cat.Update(ctx, txn, desc); err != nil {
		return nil, err
	}
	return &Result{Tag: "ALTER TABLE"}, nil
}

// execExplain describes the access plan of a SELECT without executing it.
func (s *Session) execExplain(ctx context.Context, txn *kvclient.Txn, t *parser.Explain, params []types.Datum) (*Result, error) {
	sel, ok := t.Stmt.(*parser.Select)
	if !ok || (sel.Table == "" && sel.Derived == nil) {
		return nil, newErrf(CodeFeatureNotSupported, "EXPLAIN supports table SELECT statements only")
	}
	// Subqueries are evaluated (in this transaction) so the plan reflects
	// the spliced values, exactly as execution will see them.
	sel, err := s.resolveSelectSubs(ctx, txn, sel, params)
	if err != nil {
		return nil, err
	}
	if sel.Derived != nil {
		return &Result{
			Columns: []ResultColumn{{Name: "plan", Type: types.String}},
			Rows:    [][]types.Datum{{types.NewString(fmt.Sprintf("materialized subquery (derived table %q)", sel.Alias))}},
			Tag:     "EXPLAIN",
		}, nil
	}
	desc, err := s.cat.Lookup(ctx, txn, sel.Table)
	if err != nil {
		return nil, err
	}
	if sel.Join != nil {
		text, jerr := s.explainJoin(ctx, txn, desc, sel, params)
		if jerr != nil {
			return nil, jerr
		}
		return &Result{
			Columns: []ResultColumn{{Name: "plan", Type: types.String}},
			Rows:    [][]types.Datum{{types.NewString(text)}},
			Tag:     "EXPLAIN",
		}, nil
	}
	plan, err := pickPlan(desc, sel.Where, params)
	if err != nil {
		return nil, err
	}
	text := plan.String()
	grouped := hasAggregates(sel.Exprs) || len(sel.GroupBy) > 0 || sel.Distinct
	if len(sel.OrderBy) > 0 && !grouped {
		if orderSatisfiedByPlan(desc, plan, sel.OrderBy) {
			text += "; order satisfied by access path"
		} else {
			text += "; in-memory sort"
		}
	}
	if sel.Limit > 0 && !grouped && len(plan.residual) == 0 &&
		(len(sel.OrderBy) == 0 || orderSatisfiedByPlan(desc, plan, sel.OrderBy)) {
		switch plan.kind {
		case planFullScan, planPKScan, planIndexScan:
			text += "; limit pushed into scan"
		}
	}
	return &Result{
		Columns: []ResultColumn{{Name: "plan", Type: types.String}},
		Rows:    [][]types.Datum{{types.NewString(text)}},
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
