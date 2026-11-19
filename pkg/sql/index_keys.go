package sql

import (
	"context"
	"fmt"
	"strconv"
	"strings"

	"github.com/sthorne/datax/pkg/kvclient"
	"github.com/sthorne/datax/pkg/sql/catalog"
	"github.com/sthorne/datax/pkg/sql/inverted"
	"github.com/sthorne/datax/pkg/sql/parser"
	"github.com/sthorne/datax/pkg/sql/rowenc"
	"github.com/sthorne/datax/pkg/sql/types"
	"github.com/sthorne/datax/pkg/version"
)

// What an ordinary index's key columns may declare (issue #348), the
// name CREATE INDEX picks when none is given, and CREATE INDEX on a
// table created in the same transaction block.

// textOpClasses are the btree operator classes of the string types.
// Every ordinary index compares its keys bytewise, which is the order
// of the C collation and of the *_pattern_ops classes (and, under the C
// collation, of the default classes too), so each is accepted on a
// string column and changes nothing. pattern records whether the class
// is shown by pg_get_indexdef: a default class is not, as in
// PostgreSQL.
var textOpClasses = map[string]bool{
	"text_ops": false, "varchar_ops": false, "bpchar_ops": false,
	"text_pattern_ops": true, "varchar_pattern_ops": true, "bpchar_pattern_ops": true,
}

// ordinaryOpClass checks the operator class written after a column of
// an ordinary (btree) index and returns what the descriptor records:
// the class's name when pg_get_indexdef shows it, "" otherwise.
func ordinaryOpClass(opclass string, col *catalog.Column) (string, error) {
	return keyOpClass(opclass, col.Type, fmt.Sprintf("column %q", col.Name))
}

// keyOpClass is ordinaryOpClass for a key of family fam, named by what
// in errors (a column, or an expression).
func keyOpClass(opclass string, fam types.Family, what string) (string, error) {
	if opclass == "" {
		return "", nil
	}
	shown, ok := textOpClasses[opclass]
	if !ok {
		return "", newErrf(CodeFeatureNotSupported, "operator class %q is not supported for %s of an ordinary index (an ordinary index compares bytewise; text_pattern_ops, varchar_pattern_ops and bpchar_pattern_ops are accepted on string keys)", opclass, what)
	}
	if fam != types.String {
		return "", newErrf(CodeDatatypeMismatch, "operator class %q does not accept data type %s", opclass, fam)
	}
	if !shown {
		return "", nil
	}
	return opclass, nil
}

// indexCollation checks a COLLATE written after an index key and
// returns what the descriptor records: "C" or "POSIX", or "" for the
// default. An index compares bytewise, which is what C and POSIX ask
// for; the default collation is the same order in datax (see
// docs/user/postgres-differences.md). Any other collation would order
// differently, so it is refused rather than ignored.
func indexCollation(collation string, fam types.Family) (string, error) {
	if collation == "" {
		return "", nil
	}
	if fam != types.String {
		return "", newErrf(CodeDatatypeMismatch, "collations are not supported by type %s", fam)
	}
	switch strings.ToLower(collation) {
	case "default":
		return "", nil
	case "c":
		return "C", nil
	case "posix":
		return "POSIX", nil
	}
	return "", newErrf(CodeFeatureNotSupported, "collation %q is not supported in an index: datax compares strings bytewise, as the \"C\" collation does (COLLATE \"C\", \"POSIX\" and \"default\" are accepted)", collation)
}

// isExprKey reports whether key i of CREATE INDEX is an expression.
func isExprKey(t *parser.CreateIndex, i int) bool {
	return i < len(t.Exprs) && t.Exprs[i] != nil
}

// hasExprKeys reports whether any key of CREATE INDEX is an expression
// (other than the text-search one, ExprFunc).
func hasExprKeys(t *parser.CreateIndex) bool {
	for i := range t.Exprs {
		if t.Exprs[i] != nil {
			return true
		}
	}
	return false
}

// checkIndexForm refuses what only an ordinary (btree) index takes —
// expression keys, ASC/DESC and NULLS FIRST/LAST — on another access
// method, and a predicate on a vector index, before anything else looks
// at the keys.
func checkIndexForm(t *parser.CreateIndex) error {
	method := t.Using
	if method == "" || method == "btree" {
		return nil
	}
	if hasExprKeys(t) {
		return newErrf(CodeFeatureNotSupported, "access method %q does not support expression keys (an expression is indexed USING btree; USING GIN takes to_tsvector('config', column))", method)
	}
	for _, d := range t.Directions {
		if d != "" {
			return newErrf(CodeFeatureNotSupported, "access method %q does not support ASC/DESC options", method)
		}
	}
	if t.Where != "" && (method == "ivfflat" || method == "hnsw") {
		return newErrf(CodeFeatureNotSupported, "access method %q does not support a WHERE clause (partial index)", method)
	}
	return nil
}

// newIndexDescriptor validates CREATE INDEX's keys against desc and
// appends the index to it in the given state, allocating its ID. kind
// is the inverted kind (nil for an ordinary or vector index).
func (s *Session) newIndexDescriptor(t *parser.CreateIndex, desc *catalog.TableDescriptor, kind inverted.Kind, vector bool, state string) (*catalog.IndexDescriptor, error) {
	if err := checkIndexForm(t); err != nil {
		return nil, err
	}
	var colIDs []catalog.ColumnID
	var opclasses, collations, exprs, directions []string
	var exprTypes []types.Family
	declared, computed, directed := false, false, false
	seenCol := map[catalog.ColumnID]bool{}
	for i, name := range t.Columns {
		var fam types.Family
		var what string
		if isExprKey(t, i) {
			if kind != nil || vector {
				return nil, newErrf(CodeFeatureNotSupported, "only an ordinary (btree) index takes an expression key")
			}
			text, f, err := indexKeyExpr(desc, *t.Exprs[i])
			if err != nil {
				return nil, err
			}
			colIDs = append(colIDs, 0)
			exprs = append(exprs, text)
			exprTypes = append(exprTypes, f)
			fam, what, computed = f, "expression "+text, true
		} else {
			col, ok := desc.Col(name)
			if !ok {
				return nil, newErrf(CodeUndefinedColumn, "column %q does not exist", name)
			}
			if kind == nil && !vector && !types.IsIndexable(col.Type) {
				if col.Type == types.Vector {
					return nil, newErrf(CodeFeatureNotSupported, "column %q of type %s has no ordered key encoding; index it for nearest-neighbour search with USING ivfflat", name, col.Type)
				}
				if col.Type == types.Jsonb || col.Type == types.Tsvector {
					return nil, newErrf(CodeFeatureNotSupported, "column %q of type %s has no ordered key encoding; index it with USING GIN", name, col.Type)
				}
				return nil, newErrf(CodeFeatureNotSupported, "column %q of type %s cannot be indexed (no ordered key encoding)", name, col.Type)
			}
			if kind != nil && col.Type != kind.Family() {
				return nil, newErrf(CodeFeatureNotSupported, "operator class %q does not index column %q of type %s", kind.Name(), name, col.Type)
			}
			if seenCol[col.ID] {
				return nil, newErrf(CodeSyntaxError, "duplicate column %q in index", name)
			}
			seenCol[col.ID] = true
			colIDs = append(colIDs, col.ID)
			exprs = append(exprs, "")
			exprTypes = append(exprTypes, 0)
			fam, what = col.Type, fmt.Sprintf("column %q", col.Name)
		}
		collation := ""
		if i < len(t.Collations) {
			var err error
			if collation, err = indexCollation(t.Collations[i], fam); err != nil {
				return nil, err
			}
		}
		opclass := ""
		if kind == nil && !vector && i < len(t.OpClasses) {
			var err error
			if opclass, err = keyOpClass(t.OpClasses[i], fam, what); err != nil {
				return nil, err
			}
		}
		direction := ""
		if i < len(t.Directions) {
			direction = t.Directions[i]
		}
		collations = append(collations, collation)
		opclasses = append(opclasses, opclass)
		directions = append(directions, direction)
		declared = declared || collation != "" || opclass != ""
		directed = directed || direction != ""
	}
	if t.Where != "" {
		if vector {
			return nil, newErrf(CodeFeatureNotSupported, "access method %q does not support a WHERE clause (partial index)", t.Using)
		}
		if err := checkIndexPredicate(desc, t.Where); err != nil {
			return nil, err
		}
	}
	if computed || t.Where != "" {
		if s.db.ClusterVersion() < version.V22 {
			return nil, newErrf(CodeFeatureNotSupported, "expression and partial indexes need cluster version %s (finalize the upgrade first)", version.V22)
		}
		if desc.Sealed() {
			// The entries would hold values computed from sealed
			// columns, or say which rows a condition on them holds for,
			// in plaintext.
			return nil, newErrf(CodeFeatureNotSupported, "table %q is sealed: an index on an expression, or with a WHERE clause, would keep what it computes from sealed columns in plaintext (index the columns themselves)", desc.Name)
		}
	}
	if desc.NextIndexID < rowenc.PrimaryIndexID+1 {
		desc.NextIndexID = rowenc.PrimaryIndexID + 1
	}
	idx := catalog.IndexDescriptor{
		ID: desc.NextIndexID, Name: t.Name, Unique: t.Unique,
		ColumnIDs: colIDs, State: state,
		// NULLS NOT DISTINCT means something only to a unique index;
		// on another it is accepted and has no effect, as in
		// PostgreSQL.
		NullsNotDistinct: t.Unique && t.NullsNotDistinct,
		Predicate:        t.Where,
	}
	if declared {
		idx.OpClasses, idx.Collations = opclasses, collations
	}
	if computed {
		idx.Exprs, idx.ExprTypes = exprs, exprTypes
	}
	if directed {
		idx.Directions = directions
	}
	if idx.NullsNotDistinct {
		if err := s.requireNullsNotDistinct(); err != nil {
			return nil, err
		}
	}
	if kind != nil {
		idx.Inverted = kind.Name()
	}
	desc.NextIndexID++
	desc.Indexes = append(desc.Indexes, idx)
	return &desc.Indexes[len(desc.Indexes)-1], nil
}

// chooseIndexName names an index CREATE INDEX left unnamed as
// PostgreSQL's ChooseRelationName does: <table>_<columns>_idx, cut to
// 63 bytes, with a number after "idx" when the name is taken.
func chooseIndexName(desc *catalog.TableDescriptor, t *parser.CreateIndex) string {
	parts := append([]string(nil), t.Columns...)
	if t.ExprFunc != "" {
		parts = []string{t.ExprFunc}
	}
	for i := range parts {
		if isExprKey(t, i) {
			parts[i] = exprKeyName(*t.Exprs[i])
		}
	}
	addition := strings.Join(parts, "_")
	taken := func(name string) bool {
		if name == desc.Name+"_pkey" || name == "primary" {
			return true
		}
		if _, ok := desc.Index(name); ok {
			return true
		}
		_, ok := desc.Constraint(name)
		return ok
	}
	for n := 0; ; n++ {
		label := "idx"
		if n > 0 {
			label += strconv.Itoa(n)
		}
		if name := makeObjectName(desc.Name, addition, label); !taken(name) {
			return name
		}
	}
}

// exprKeyName is the name PostgreSQL's FigureColname gives an index's
// expression key: a function's name, a cast column's name, otherwise
// "expr".
func exprKeyName(e parser.Expr) string {
	switch {
	case e.Func != "" && e.BinOp == "" && e.Left == nil:
		return e.Func
	case e.Column != "" && e.BinOp == "" && len(e.Path) == 0:
		return e.Column
	}
	return "expr"
}

// makeObjectName is PostgreSQL's: name1_name2_label within 63 bytes,
// shortening the longer of name1 and name2 first.
func makeObjectName(name1, name2, label string) string {
	const maxLen = 63
	avail := maxLen - len(label) - 1
	if name2 != "" {
		avail--
	}
	n1, n2 := len(name1), len(name2)
	for n1+n2 > avail {
		if n1 > n2 {
			n1--
		} else {
			n2--
		}
	}
	name := name1[:n1]
	if name2 != "" {
		name += "_" + name2[:n2]
	}
	return name + "_" + label
}

// noteCreatedTable records a table created in the open transaction
// block: until the block ends nothing but this session can see it.
func (s *Session) noteCreatedTable(txn *kvclient.Txn, id uint64) {
	if s.state != StateOpen || txn != s.txn {
		return
	}
	if s.createdTxn != txn {
		s.createdTxn, s.createdTables = txn, map[uint64]bool{}
	}
	s.createdTables[id] = true
}

// createdInBlock reports whether table id was created in the open
// transaction block. ROLLBACK TO SAVEPOINT forgets every table (see
// execRollbackToSavepoint), which errs towards the online build.
func (s *Session) createdInBlock(txn *kvclient.Txn, id uint64) bool {
	return s.state == StateOpen && txn == s.txn && s.createdTxn == txn && s.createdTables[id]
}

// execCreateIndexInTxn is CREATE INDEX inside a transaction block, on a
// table created in the same block. No other transaction can see that
// table until the block commits — no gateway holds a lease on it and no
// writer but this block has rows in it — so the online build's write-only
// phase, drains and chunked backfill have nothing to protect against:
// the index is published at once, filled from the block's own rows, and
// commits or rolls back with the block, like the table. On any other
// table the build has to be online and runs in transactions of its own,
// so it is refused in a block (25001).
//
// A vector index trains its centres in transactions of its own and is
// refused in a block whatever the table.
func (s *Session) execCreateIndexInTxn(ctx context.Context, txn *kvclient.Txn, t *parser.CreateIndex) (*Result, error) {
	if t.Concurrently {
		return nil, newErrf(CodeActiveTransaction, "CREATE INDEX CONCURRENTLY cannot run inside a transaction block")
	}
	if err := checkIndexForm(t); err != nil {
		return nil, err
	}
	shared, err := s.lookup(ctx, txn, t.Table)
	if err != nil {
		return nil, err
	}
	if err := mustBeReal(shared); err != nil {
		return nil, err
	}
	if !s.createdInBlock(txn, shared.ID) {
		return nil, newErrf(CodeActiveTransaction, "CREATE INDEX cannot run inside a transaction block (except on a table created in the same block): the index is built online, in transactions of its own")
	}
	if t.Using == "ivfflat" || t.Using == "hnsw" {
		return nil, newErrf(CodeActiveTransaction, "CREATE INDEX ... USING %s cannot run inside a transaction block: its centres are trained in transactions of their own", t.Using)
	}
	if err := s.checkTableOwner(ctx, txn, shared); err != nil {
		return nil, err
	}
	desc := shared.Clone()
	if t.Name == "" {
		c := *t
		c.Name = chooseIndexName(desc, t)
		t = &c
	}
	if t.Name == "primary" {
		return nil, newErrf(CodeSyntaxError, "index name %q is reserved", t.Name)
	}
	if _, taken := desc.Index(t.Name); taken {
		if t.IfNotExists {
			return &Result{Tag: "CREATE INDEX", Notices: []string{"relation \"" + t.Name + "\" already exists, skipping"}}, nil
		}
		return nil, newErrf(CodeDuplicateObject, "index %q already exists", t.Name)
	}
	kind, err := s.invertedKindFor(t, desc)
	if err != nil {
		return nil, err
	}
	idx, err := s.newIndexDescriptor(t, desc, kind, false, catalog.IndexStatePublic)
	if err != nil {
		return nil, err
	}
	// The rows the block has written so far, all of them this
	// transaction's own.
	start, end := rowenc.PrimarySpanFor(desc)
	for {
		kvs, err := txn.Scan(ctx, start, end, backfillChunkSize)
		if err != nil {
			return nil, err
		}
		if len(kvs) == 0 {
			break
		}
		if err := s.writeIndexEntries(ctx, txn, desc, idx, kvs); err != nil {
			return nil, err
		}
		if len(kvs) < backfillChunkSize {
			break
		}
		start = kvs[len(kvs)-1].Key.Next()
	}
	var notices []string
	if desc.Sealed() {
		notices = []string{sealedIndexNotice(desc, idx)}
	}
	if err := s.cat.Update(ctx, txn, desc); err != nil {
		return nil, err
	}
	s.noteDDL(catalog.RelationName(desc))
	return &Result{Tag: "CREATE INDEX", Notices: notices}, nil
}
