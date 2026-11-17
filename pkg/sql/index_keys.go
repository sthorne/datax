package sql

import (
	"context"
	"strconv"
	"strings"

	"github.com/sthorne/datax/pkg/kvclient"
	"github.com/sthorne/datax/pkg/sql/catalog"
	"github.com/sthorne/datax/pkg/sql/inverted"
	"github.com/sthorne/datax/pkg/sql/parser"
	"github.com/sthorne/datax/pkg/sql/rowenc"
	"github.com/sthorne/datax/pkg/sql/types"
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
	if opclass == "" {
		return "", nil
	}
	shown, ok := textOpClasses[opclass]
	if !ok {
		return "", newErrf(CodeFeatureNotSupported, "operator class %q is not supported for column %q of an ordinary index (an ordinary index compares bytewise; text_pattern_ops, varchar_pattern_ops and bpchar_pattern_ops are accepted on string columns)", opclass, col.Name)
	}
	if col.Type != types.String {
		return "", newErrf(CodeDatatypeMismatch, "operator class %q does not accept data type %s", opclass, col.Type)
	}
	if !shown {
		return "", nil
	}
	return opclass, nil
}

// indexCollation checks a COLLATE written after an index column and
// returns what the descriptor records: "C" or "POSIX", or "" for the
// default. An index compares bytewise, which is what C and POSIX ask
// for; the default collation is the same order in datax (see
// docs/user/postgres-differences.md). Any other collation would order
// differently, so it is refused rather than ignored.
func indexCollation(collation string, col *catalog.Column) (string, error) {
	if collation == "" {
		return "", nil
	}
	if col.Type != types.String {
		return "", newErrf(CodeDatatypeMismatch, "collations are not supported by type %s", col.Type)
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

// newIndexDescriptor validates CREATE INDEX's key columns against desc
// and appends the index to it in the given state, allocating its ID.
// kind is the inverted kind (nil for an ordinary or vector index).
func (s *Session) newIndexDescriptor(t *parser.CreateIndex, desc *catalog.TableDescriptor, kind inverted.Kind, vector bool, state string) (*catalog.IndexDescriptor, error) {
	var colIDs []catalog.ColumnID
	var opclasses, collations []string
	declared := false
	seenCol := map[catalog.ColumnID]bool{}
	for i, name := range t.Columns {
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
		collation := ""
		if i < len(t.Collations) {
			var err error
			if collation, err = indexCollation(t.Collations[i], &col); err != nil {
				return nil, err
			}
		}
		opclass := ""
		if kind == nil && !vector && i < len(t.OpClasses) {
			var err error
			if opclass, err = ordinaryOpClass(t.OpClasses[i], &col); err != nil {
				return nil, err
			}
		}
		collations = append(collations, collation)
		opclasses = append(opclasses, opclass)
		declared = declared || collation != "" || opclass != ""
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
	}
	if declared {
		idx.OpClasses, idx.Collations = opclasses, collations
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
	parts := t.Columns
	if t.ExprFunc != "" {
		parts = []string{t.ExprFunc}
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
	s.noteDDL(desc.Name)
	return &Result{Tag: "CREATE INDEX", Notices: notices}, nil
}
