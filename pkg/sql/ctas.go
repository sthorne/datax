package sql

import (
	"context"
	"fmt"
	"strings"

	"github.com/sthorne/datax/pkg/kvclient"
	"github.com/sthorne/datax/pkg/sql/catalog"
	"github.com/sthorne/datax/pkg/sql/parser"
	"github.com/sthorne/datax/pkg/sql/types"
	"github.com/sthorne/datax/pkg/util/log"
)

// CREATE TABLE ... AS, CREATE TABLE ... (LIKE ...) and COMMENT ON (issue
// #95, the last DDL set).

// rowidColumn is the hidden primary key CREATE TABLE AS gives a table
// whose query names none: a node-local unique id, spread across ranges.
const rowidColumn = "rowid"

// execCreateTableAs runs CREATE TABLE ... AS query outside any explicit
// transaction: the table is created (and its lease drained) first, then
// the query runs once, at one timestamp, and its rows stream into the
// table through the COPY chunk path, a bounded transaction per chunk. A
// failure after the table exists drops it again.
func (s *Session) execCreateTableAs(ctx context.Context, t *parser.CreateTable, params []types.Datum) (*Result, *Error) {
	if parser.CountParams(t.As) > len(params) {
		return nil, newErrf(CodeSyntaxError, "CREATE TABLE AS: the query needs %d parameters", parser.CountParams(t.As))
	}
	if t.IfNotExists && s.tableExists(ctx, t.Name) {
		return &Result{Tag: "CREATE TABLE AS"}, nil
	}
	// The shape, described before anything is written.
	q, err := s.expandViews(ctx, nil, t.As)
	if err != nil {
		return nil, ToSQLError(err)
	}
	cols, serr := s.PlanColumns(ctx, q)
	if serr != nil {
		return nil, serr
	}
	if len(cols) == 0 {
		return nil, newErrf(CodeSyntaxError, "CREATE TABLE AS: the query returns no columns")
	}
	if len(t.AsColumns) > 0 && len(t.AsColumns) != len(cols) {
		return nil, newErrf(CodeSyntaxError, "CREATE TABLE AS: %d column names for %d query columns", len(t.AsColumns), len(cols))
	}
	ct := *t
	ct.As, ct.AsText, ct.AsColumns, ct.NoData = nil, "", nil, false
	ct.Columns = nil
	seen := map[string]bool{}
	for i, rc := range cols {
		name := strings.ToLower(rc.Name)
		if i < len(t.AsColumns) {
			name = strings.ToLower(t.AsColumns[i])
		}
		if name == "" || name == "?column?" {
			name = fmt.Sprintf("column%d", i+1)
		}
		if seen[name] {
			return nil, newErrf(CodeDuplicateObject, "column %q specified more than once", name)
		}
		seen[name] = true
		typ := rc.Type
		if typ == types.Unknown {
			typ = types.String
		}
		ct.Columns = append(ct.Columns, parser.ColumnDef{Name: name, Type: typ})
	}
	if len(ct.PrimaryKey) == 0 {
		if seen[rowidColumn] {
			return nil, newErrf(CodeDuplicateObject, "column %q is reserved for the generated primary key; give the table a PRIMARY KEY (cols) clause", rowidColumn)
		}
		e, perr := parser.ParseExpr("unique_rowid()")
		if perr != nil {
			return nil, ToSQLError(perr)
		}
		ct.Columns = append(ct.Columns, parser.ColumnDef{Name: rowidColumn, Type: types.Int, NotNull: true, DefaultExpr: &e, Hidden: true})
		ct.PrimaryKey = []string{rowidColumn}
	}
	// Step 1: the table.
	var created bool
	if err := s.db.RunTxn(ctx, "ctas-create", func(ctx context.Context, txn *kvclient.Txn) error {
		created = false
		if err := s.checkCreateInDatabase(ctx, txn, ct.Name); err != nil {
			return err
		}
		res, err := s.execCreateTable(ctx, txn, &ct)
		if err != nil {
			return err
		}
		created = res != nil
		return nil
	}); err != nil {
		return nil, ToSQLError(err)
	}
	if err := s.cat.FinishDDLIn(ctx, s.database, ct.Name); err != nil {
		return nil, ToSQLError(err)
	}
	if t.NoData {
		return &Result{Tag: "CREATE TABLE AS"}, nil
	}
	drop := func(cause error) (*Result, *Error) {
		if created {
			cctx := context.WithoutCancel(ctx)
			_ = s.db.RunTxn(cctx, "ctas-drop", func(ctx context.Context, txn *kvclient.Txn) error {
				_, err := s.execDropTable(ctx, txn, &parser.DropTable{Name: ct.Name, IfExists: true})
				return err
			})
			_ = s.cat.FinishDDLIn(cctx, s.database, ct.Name)
		}
		return nil, ToSQLError(cause)
	}
	// Step 2: the rows, at one timestamp.
	var src *Result
	if err := s.db.RunTxn(ctx, "ctas-query", func(ctx context.Context, txn *kvclient.Txn) error {
		var err error
		src, err = s.execStmt(ctx, txn, t.As, params)
		return err
	}); err != nil {
		return drop(err)
	}
	// Step 3: the bulk write through the COPY chunk path.
	var target []catalog.Column
	if err := s.db.RunTxn(ctx, "ctas-target", func(ctx context.Context, txn *kvclient.Txn) error {
		desc, err := s.lookup(ctx, txn, ct.Name)
		if err != nil {
			return err
		}
		target = nil
		for _, c := range desc.Columns {
			if !c.Hidden {
				target = append(target, c)
			}
		}
		return nil
	}); err != nil {
		return drop(err)
	}
	ci := &CopyIn{s: s, table: ct.Name, target: target}
	for _, row := range src.Rows {
		if err := ci.AddRow(ctx, row); err != nil {
			return drop(err)
		}
	}
	n, err := ci.Finish(ctx)
	if err != nil {
		return drop(err)
	}
	log.Audit("table-ddl", "stmt", "CREATE TABLE AS", "target", ct.Name, "rows", n, "principal", s.user)
	return &Result{Tag: fmt.Sprintf("CREATE TABLE AS %d", n)}, nil
}

// likeIndex is a plain (non-constraint) index copied by LIKE ... INCLUDING
// INDEXES, created once the new table has column IDs.
type likeIndex struct {
	name   string
	unique bool
	cols   []string
}

// expandLike replaces each LIKE clause of a CREATE TABLE with the source
// table's columns (types, typmods, NOT NULL; defaults, constraints,
// indexes and comments per the options), returning the statement to
// create and the plain indexes to add afterwards. The primary key is
// copied when the statement declares none (a table needs one).
func (s *Session) expandLike(ctx context.Context, txn *kvclient.Txn, t *parser.CreateTable) (*parser.CreateTable, []likeIndex, error) {
	if len(t.Like) == 0 {
		return t, nil, nil
	}
	out := *t
	out.Like = nil
	out.Columns = nil
	out.Constraints = append([]parser.ConstraintDef(nil), t.Constraints...)
	var indexes []likeIndex
	li := 0
	for i := 0; i <= len(t.Columns); i++ {
		for li < len(t.Like) && t.Like[li].Position == i {
			lc := t.Like[li]
			li++
			src, err := s.lookup(ctx, txn, lc.Table)
			if err != nil {
				return nil, nil, err
			}
			if src.Virtual != "" || src.IsView() {
				return nil, nil, newErrf(CodeWrongObjectType, "LIKE %s: not a table", lc.Table)
			}
			byID := map[catalog.ColumnID]string{}
			for _, c := range src.Columns {
				if c.Hidden {
					continue
				}
				byID[c.ID] = c.Name
				def := parser.ColumnDef{Name: c.Name, Type: c.Type, NotNull: c.NotNull, Precision: c.Precision, Scale: c.Scale}
				if lc.Defaults {
					switch {
					case c.DefaultExpr != "" && c.DefaultExpr != "NULL" && c.SequenceID == 0:
						e, perr := parser.ParseExpr(c.DefaultExpr)
						if perr == nil {
							def.DefaultExpr = &e
						}
					case c.Default != nil && (c.DefaultExpr == "" || c.DefaultExpr == "NULL"):
						d := *c.Default
						def.Default = &d
					}
				}
				out.Columns = append(out.Columns, def)
			}
			if len(out.PrimaryKey) == 0 {
				for _, id := range src.PrimaryKey {
					if name, ok := byID[id]; ok {
						out.PrimaryKey = append(out.PrimaryKey, name)
					}
				}
			}
			owned := map[uint64]bool{}
			for _, c := range src.Constraints {
				if c.Kind == catalog.ConstraintUnique || c.AutoIndex {
					owned[c.IndexID] = true
				}
				if !lc.Constraints {
					continue
				}
				switch c.Kind {
				case catalog.ConstraintCheck:
					fails, perr := parser.ParseCheck(c.Expr)
					if perr != nil {
						return nil, nil, newErrf(CodeInternal, "LIKE %s: constraint %q: %v", lc.Table, c.Name, perr)
					}
					var names []string
					for _, id := range c.Columns {
						names = append(names, byID[id])
					}
					out.Constraints = append(out.Constraints, parser.ConstraintDef{Kind: "check", Columns: names, Check: c.Expr, CheckFails: fails})
				case catalog.ConstraintUnique:
					if lc.Indexes {
						var names []string
						for _, id := range c.Columns {
							names = append(names, byID[id])
						}
						out.Constraints = append(out.Constraints, parser.ConstraintDef{Kind: "unique", Columns: names})
					}
				}
			}
			if lc.Indexes {
				for _, idx := range src.Indexes {
					if owned[idx.ID] || !idx.Public() {
						continue
					}
					var names []string
					for _, id := range idx.ColumnIDs {
						names = append(names, byID[id])
					}
					indexes = append(indexes, likeIndex{name: idx.Name, unique: idx.Unique, cols: names})
				}
			}
		}
		if i < len(t.Columns) {
			out.Columns = append(out.Columns, t.Columns[i])
		}
	}
	return &out, indexes, nil
}

// addLikeIndexes appends the copied plain indexes to a just-created
// (empty) table: nothing to backfill, so they are public at once.
func addLikeIndexes(desc *catalog.TableDescriptor, indexes []likeIndex) error {
	for _, li := range indexes {
		if _, exists := desc.Index(li.name); exists {
			continue
		}
		var ids []catalog.ColumnID
		for _, name := range li.cols {
			col, ok := desc.Col(name)
			if !ok {
				return newErrf(CodeUndefinedColumn, "LIKE: column %q does not exist", name)
			}
			ids = append(ids, col.ID)
		}
		if desc.NextIndexID < 2 {
			desc.NextIndexID = 2
		}
		desc.Indexes = append(desc.Indexes, catalog.IndexDescriptor{ID: desc.NextIndexID, Name: li.name, Unique: li.unique, ColumnIDs: ids, State: catalog.IndexStatePublic})
		desc.NextIndexID++
	}
	return nil
}

// execCommentOn is COMMENT ON TABLE | VIEW | INDEX | COLUMN ... IS text.
func (s *Session) execCommentOn(ctx context.Context, txn *kvclient.Txn, t *parser.CommentOn) (*Result, error) {
	text := ""
	if t.Text != nil {
		text = *t.Text
	}
	var desc *catalog.TableDescriptor
	switch t.Kind {
	case "index":
		d, idx, err := s.findIndex(ctx, txn, t.Name)
		if err != nil {
			return nil, err
		}
		idx.Comment = text
		desc = d
	default:
		shared, err := s.lookup(ctx, txn, t.Name)
		if err != nil {
			return nil, err
		}
		if shared.Virtual != "" {
			return nil, newErrf(CodeInsufficientPriv, "%s is a system catalog", shared.Virtual)
		}
		desc = shared.Clone()
		if t.Kind == "column" {
			found := false
			for i := range desc.Columns {
				if desc.Columns[i].Name == t.Column && !desc.Columns[i].Hidden {
					desc.Columns[i].Comment = text
					found = true
				}
			}
			if !found {
				return nil, newErrf(CodeUndefinedColumn, "column %q of relation %q does not exist", t.Column, desc.Name)
			}
		} else {
			desc.Comment = text
		}
	}
	if catalog.IsSystemTable(desc.Name) && !s.system {
		return nil, newErrf(CodeInsufficientPriv, "table %q belongs to the cluster", desc.Name)
	}
	if err := s.cat.Update(ctx, txn, desc); err != nil {
		return nil, err
	}
	s.noteDDL(desc.Name)
	return &Result{Tag: "COMMENT"}, nil
}
