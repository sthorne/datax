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

// withRowidKey returns t with the hidden generated primary key added
// when t declares none (issue #354): a table without a PRIMARY KEY — a
// join table, CREATE TABLE AS, an imported schema — is keyed by a
// hidden, NOT NULL INT8 column defaulting to unique_rowid(), so its
// rows may repeat as in PostgreSQL. The column is named rowid, or
// rowid_1, rowid_2, ... when the table has a column of that name
// (CockroachDB's rule). t itself is never modified: it may be a cached
// statement executed again.
func withRowidKey(t *parser.CreateTable) (*parser.CreateTable, error) {
	if len(t.PrimaryKey) > 0 {
		return t, nil
	}
	taken := map[string]bool{}
	for _, cd := range t.Columns {
		if cd.PrimaryKey {
			return t, nil
		}
		taken[cd.Name] = true
	}
	name := rowidColumn
	for i := 1; taken[name]; i++ {
		name = fmt.Sprintf("%s_%d", rowidColumn, i)
	}
	e, err := parser.ParseExpr("unique_rowid()")
	if err != nil {
		return nil, ToSQLError(err)
	}
	out := *t
	out.Columns = append(append(make([]parser.ColumnDef, 0, len(t.Columns)+1), t.Columns...),
		parser.ColumnDef{Name: name, Type: types.Int, NotNull: true, DefaultExpr: &e, Hidden: true})
	out.PrimaryKey = []string{name}
	return &out, nil
}

// execCreateTableAs runs CREATE TABLE ... AS query outside any explicit
// transaction: the table is created (and its lease drained) first, then
// the query runs once, at one timestamp, and its rows stream into the
// table through the COPY chunk path, a bounded transaction per chunk. A
// failure after the table exists drops it again.
func (s *Session) execCreateTableAs(ctx context.Context, t *parser.CreateTable, params []types.Datum) (*Result, *Error) {
	if parser.CountParams(t.As) > len(params) {
		return nil, newErrf(CodeSyntaxError, "CREATE TABLE AS: the query needs %d parameters", parser.CountParams(t.As))
	}
	t, perr := prepareTempCreate(t)
	if perr != nil {
		return nil, ToSQLError(perr)
	}
	exists := t.IfNotExists && s.tableExists(ctx, t.Name)
	if t.IfNotExists && t.Temp {
		// Only a temporary table of the name is in the way of one.
		exists = s.tableExists(ctx, catalog.TempSchema+"."+t.Name)
	}
	if exists {
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
		// A column the query takes straight from a table keeps its type
		// modifiers (INT4, VARCHAR(20), TIMESTAMP); a computed one is bare.
		cd := parser.ColumnDef{Name: name, Type: typ, Width: rc.Width, MaxLen: rc.MaxLen, Char: rc.Char, NoTZ: rc.NoTZ, TypeName: rc.EnumName}
		if typ == types.Enum && rc.EnumName == "" {
			cd.Type = types.String // a computed enum value without its type: text
		}
		cd.TimePrecision = rc.TimePrecision
		if typ == types.Decimal && rc.Typmod > 0 {
			cd.Precision, cd.Scale = rc.Typmod>>16, rc.Typmod&0xffff-4
		}
		ct.Columns = append(ct.Columns, cd)
	}
	// Step 1: the table.
	var created bool
	if err := s.db.RunTxn(ctx, "ctas-create", func(ctx context.Context, txn *kvclient.Txn) error {
		created = false
		if !ct.Temp {
			if err := s.checkCreateInDatabase(ctx, txn, ct.Name); err != nil {
				return err
			}
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
	if err := s.cat.FinishDDLIn(s.rctx(ctx), s.database, ct.Name); err != nil {
		return nil, ToSQLError(err)
	}
	if t.NoData {
		if t.Temp && t.OnCommit == "drop" {
			if serr := s.dropTempNow(ctx, ct.Name); serr != nil {
				return nil, serr
			}
		}
		return &Result{Tag: "CREATE TABLE AS"}, nil
	}
	drop := func(cause error) (*Result, *Error) {
		if created {
			cctx := context.WithoutCancel(ctx)
			s.extraDrops = nil
			if s.db.RunTxn(cctx, "ctas-drop", func(ctx context.Context, txn *kvclient.Txn) error {
				s.extraDrops = nil
				_, err := s.execDropTable(ctx, txn, &parser.DropTable{Name: ct.Name, IfExists: true})
				return err
			}) == nil {
				// The table was created, and drained, in a transaction of
				// its own: other gateways may hold it, so its drop drains
				// too (issue #377).
				_ = s.finishDrops(cctx, s.extraDrops)
			}
			s.extraDrops = nil
			_ = s.cat.FinishDDLIn(s.rctx(cctx), s.database, ct.Name)
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
	if t.Temp && t.OnCommit != "" && t.OnCommit != "preserve" {
		// Outside a transaction block the statement is its own
		// transaction, at whose commit ON COMMIT DELETE ROWS empties the
		// table and ON COMMIT DROP drops it: the rows are never kept.
		if t.OnCommit == "drop" {
			if serr := s.dropTempNow(ctx, ct.Name); serr != nil {
				return nil, serr
			}
		}
		return &Result{Tag: fmt.Sprintf("CREATE TABLE AS %d", len(src.Rows))}, nil
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
	log.Audit("table-ddl", "stmt", "CREATE TABLE AS", "target", ct.Name, "rows", n, "principal", s.sessionUser, "role", s.user)
	return &Result{Tag: fmt.Sprintf("CREATE TABLE AS %d", n)}, nil
}

// likeIndex is a plain (non-constraint) index copied by LIKE ... INCLUDING
// INDEXES, created once the new table has column IDs.
type likeIndex struct {
	name   string
	unique bool
	cols   []string
	// inverted is the source index's inverted kind ("" = ordinary).
	inverted string
	// nullsNotDistinct copies UNIQUE NULLS NOT DISTINCT.
	nullsNotDistinct bool
	// opClasses and collations copy the declared operator classes and
	// collations (parallel to cols; nil = none).
	opClasses, collations []string
	// exprs, exprTypes, predicate and directions copy expression keys
	// (whose cols entry is ""), a partial index's predicate and the
	// declared orders. The texts name columns, which the copy has by
	// the same names.
	exprs      []string
	exprTypes  []types.Family
	predicate  string
	directions []string
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
				def := parser.ColumnDef{Name: c.Name, Type: c.Type, NotNull: c.NotNull, Precision: c.Precision, Scale: c.Scale,
					Width: c.Width, MaxLen: c.MaxLen, Char: c.Char, NoTZ: c.NoTZ, TimePrecision: c.TimePrecision, TypeName: c.EnumName}
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
						nnd := false
						if idx, ok := src.IndexByID(c.IndexID); ok {
							nnd = idx.NullsNotDistinct
						}
						out.Constraints = append(out.Constraints, parser.ConstraintDef{Kind: "unique", Columns: names, NullsNotDistinct: nnd})
					}
				}
			}
			if lc.Indexes {
				for _, idx := range src.Indexes {
					if owned[idx.ID] || !idx.Public() {
						continue
					}
					if idx.IsVector() {
						// A vector index is its trained centres, which belong
						// to the source table's data; the copy starts empty
						// and is indexed once it has rows of its own.
						continue
					}
					var names []string
					for i, id := range idx.ColumnIDs {
						if idx.IsExprKey(i) {
							names = append(names, "")
							continue
						}
						names = append(names, byID[id])
					}
					indexes = append(indexes, likeIndex{name: idx.Name, unique: idx.Unique, cols: names, inverted: idx.Inverted, nullsNotDistinct: idx.NullsNotDistinct,
						opClasses: idx.OpClasses, collations: idx.Collations,
						exprs: append([]string(nil), idx.Exprs...), exprTypes: append([]types.Family(nil), idx.ExprTypes...), predicate: idx.Predicate,
						directions: append([]string(nil), idx.Directions...)})
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
		if (li.predicate != "" || len(li.exprs) > 0) && desc.Sealed() {
			return newErrf(CodeFeatureNotSupported, "table %q is sealed: index %q, on an expression or with a WHERE clause, cannot be copied to it (it would keep what it computes from sealed columns in plaintext)", desc.Name, li.name)
		}
		var ids []catalog.ColumnID
		for i, name := range li.cols {
			if i < len(li.exprs) && li.exprs[i] != "" {
				ids = append(ids, 0)
				continue
			}
			col, ok := desc.Col(name)
			if !ok {
				return newErrf(CodeUndefinedColumn, "LIKE: column %q does not exist", name)
			}
			ids = append(ids, col.ID)
		}
		if desc.NextIndexID < 2 {
			desc.NextIndexID = 2
		}
		desc.Indexes = append(desc.Indexes, catalog.IndexDescriptor{ID: desc.NextIndexID, Name: li.name, Unique: li.unique, ColumnIDs: ids, State: catalog.IndexStatePublic, Inverted: li.inverted, NullsNotDistinct: li.nullsNotDistinct,
			OpClasses: li.opClasses, Collations: li.collations,
			Exprs: li.exprs, ExprTypes: li.exprTypes, Predicate: li.predicate, Directions: li.directions})
		desc.NextIndexID++
	}
	return nil
}

// setConstraintComment sets the comment of desc's constraint name: its
// primary key (<table>_pkey), a CHECK or FOREIGN KEY constraint, or a
// UNIQUE one (a unique index, as pg_constraint lists it).
func setConstraintComment(desc *catalog.TableDescriptor, name, text string) error {
	if !desc.IsView() && name == desc.Name+"_pkey" {
		desc.PKComment = text
		return nil
	}
	for i := range desc.Constraints {
		c := &desc.Constraints[i]
		if c.Name == name && (c.Kind == catalog.ConstraintCheck || c.Kind == catalog.ConstraintForeign) {
			c.Comment = text
			return nil
		}
	}
	for i := range desc.Indexes {
		idx := &desc.Indexes[i]
		if idx.Name == name && idx.Unique {
			idx.ConstraintComment = text
			return nil
		}
	}
	return newErrf(CodeUndefinedObject, "constraint %q for table %q does not exist", name, desc.Name)
}

// execCommentOn is COMMENT ON TABLE | VIEW | INDEX | COLUMN | CONSTRAINT
// ... IS text.
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
		if err := s.checkTableOwner(ctx, txn, d); err != nil {
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
		if err := s.checkTableOwner(ctx, txn, shared); err != nil {
			return nil, err
		}
		desc = shared.Clone()
		if t.Kind == "constraint" {
			if err := setConstraintComment(desc, t.Column, text); err != nil {
				return nil, err
			}
		} else if t.Kind == "column" {
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

// dropTempNow drops the session's temporary table name in a transaction
// of its own: CREATE TEMP TABLE ... ON COMMIT DROP AS, which is its own
// transaction.
func (s *Session) dropTempNow(ctx context.Context, name string) *Error {
	err := s.db.RunTxn(ctx, "ctas-drop", func(ctx context.Context, txn *kvclient.Txn) error {
		s.extraDrops = nil
		_, err := s.execDropTable(ctx, txn, &parser.DropTable{Name: catalog.TempSchema + "." + name, IfExists: true})
		return err
	})
	s.extraDrops = nil
	return ToSQLError(err)
}
