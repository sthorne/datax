package sql

import (
	"context"
	"strings"

	"github.com/sthorne/datax/pkg/kvclient"
	"github.com/sthorne/datax/pkg/sql/catalog"
	"github.com/sthorne/datax/pkg/sql/parser"
	"github.com/sthorne/datax/pkg/sql/types"
	"github.com/sthorne/datax/pkg/util/log"
)

// Enum types (issue #96, part four): CREATE TYPE name AS ENUM (...),
// ALTER TYPE name ADD VALUE, DROP TYPE. A type is a descriptor of its
// labels in declaration order; a column of the type carries the type's
// ID and a copy of the labels, so a value's label converts to its
// ordinal (the stored form, which orders) without a lookup. ADD VALUE
// appends the label to the descriptor and to every column of the type,
// draining those tables like any DDL; DROP TYPE refuses while a column
// uses the type. Cluster version v10.

// lookupType resolves a type name (optionally database-qualified) in
// the session's database.
func (s *Session) lookupType(ctx context.Context, txn *kvclient.Txn, name string) (*catalog.TypeDescriptor, error) {
	db, bare := catalog.SplitTableName(name)
	if db == "" {
		db = s.database
	}
	dbID, err := s.cat.DatabaseID(ctx, txn, db)
	if err != nil {
		return nil, ToSQLError(err)
	}
	d, err := catalog.LookupType(ctx, txn, dbID, bare)
	if err != nil {
		var nf *catalog.ErrTypeNotFound
		if asErr(err, &nf) {
			return nil, newErrf(CodeUndefinedObject, "type %q does not exist", name)
		}
		return nil, ToSQLError(err)
	}
	return d, nil
}

// resolveEnumColumn fills an enum column's type fields from the named
// type.
func (s *Session) resolveEnumColumn(ctx context.Context, txn *kvclient.Txn, col *catalog.Column, typeName string) error {
	if col.Type != types.Enum {
		return nil
	}
	if err := s.requireV10(types.Enum); err != nil {
		return err
	}
	d, err := s.lookupType(ctx, txn, typeName)
	if err != nil {
		return err
	}
	col.EnumType, col.EnumName, col.EnumLabels = d.ID, d.Name, append([]string(nil), d.Labels...)
	return nil
}

// enumLiteral converts a text literal cast to an enum type
// ('happy'::mood) into the enum value, when name is a type of the
// session's database; ok is false for any other cast name.
func (s *Session) enumLiteral(ctx context.Context, txn *kvclient.Txn, name string, lit types.Datum) (types.Datum, bool, error) {
	if _, err := types.ParseType(name); err == nil {
		return lit, false, nil
	}
	if strings.ContainsAny(name, "()[] ") {
		return lit, false, nil
	}
	d, err := s.lookupType(ctx, txn, name)
	if err != nil {
		if serr, ok := err.(*Error); ok && serr.Code == CodeUndefinedObject {
			return lit, false, nil
		}
		return lit, false, err
	}
	if lit.Null {
		return lit, true, nil
	}
	col := catalog.Column{Type: types.Enum, EnumType: d.ID, EnumName: d.Name, EnumLabels: d.Labels}
	v, err := col.EnumValue(lit)
	if err != nil {
		return lit, true, ToSQLError(err)
	}
	return v, true, nil
}

func (s *Session) execCreateType(ctx context.Context, txn *kvclient.Txn, t *parser.CreateType) (*Result, error) {
	if err := s.requireV10(types.Enum); err != nil {
		return nil, err
	}
	if err := s.checkCreateInDatabase(ctx, txn, t.Name); err != nil {
		return nil, err
	}
	db, bare := catalog.SplitTableName(t.Name)
	if db == "" {
		db = s.database
	}
	dbID, err := s.cat.DatabaseID(ctx, txn, db)
	if err != nil {
		return nil, ToSQLError(err)
	}
	if _, err := types.ParseType(bare); err == nil {
		return nil, newErrf(CodeDuplicateObject, "type %q is a builtin type", bare)
	}
	d := &catalog.TypeDescriptor{Name: bare, DatabaseID: dbID, Labels: append([]string{}, t.Labels...)}
	if err := catalog.CreateType(ctx, txn, d); err != nil {
		var ex *catalog.ErrTypeExists
		if t.IfNotExists && asErr(err, &ex) {
			return &Result{Tag: "CREATE TYPE"}, nil
		}
		if asErr(err, &ex) {
			return nil, newErrf(CodeDuplicateObject, "%v", err)
		}
		return nil, ToSQLError(err)
	}
	log.Audit("type-ddl", "stmt", "CREATE TYPE", "target", bare, "principal", s.user)
	return &Result{Tag: "CREATE TYPE"}, nil
}

func (s *Session) execAlterType(ctx context.Context, txn *kvclient.Txn, t *parser.AlterType) (*Result, error) {
	d, err := s.lookupType(ctx, txn, t.Name)
	if err != nil {
		return nil, err
	}
	if err := s.checkCreateInDatabase(ctx, txn, t.Name); err != nil {
		return nil, err
	}
	for _, l := range d.Labels {
		if l == t.AddValue {
			if t.IfNotExistsVal {
				return &Result{Tag: "ALTER TYPE"}, nil
			}
			return nil, newErrf(CodeDuplicateObject, "enum label %q already exists", t.AddValue)
		}
	}
	d.Labels = append(d.Labels, t.AddValue)
	if err := catalog.UpdateType(ctx, txn, d); err != nil {
		return nil, ToSQLError(err)
	}
	// Every column of the type learns the label; its table drains after
	// the commit so every gateway writes the new ordinal the same way.
	tables, err := s.cat.List(ctx, txn)
	if err != nil {
		return nil, ToSQLError(err)
	}
	for _, shared := range tables {
		uses := false
		for _, c := range shared.Columns {
			if c.Type == types.Enum && c.EnumType == d.ID {
				uses = true
			}
		}
		if !uses {
			continue
		}
		desc := shared.Clone()
		for i := range desc.Columns {
			if c := &desc.Columns[i]; c.Type == types.Enum && c.EnumType == d.ID {
				c.EnumLabels = append([]string(nil), d.Labels...)
			}
		}
		if err := s.cat.Update(ctx, txn, desc); err != nil {
			return nil, ToSQLError(err)
		}
		s.noteDDL(s.qualifiedTableName(ctx, txn, desc))
	}
	log.Audit("type-ddl", "stmt", "ALTER TYPE", "target", d.Name, "label", t.AddValue, "principal", s.user)
	return &Result{Tag: "ALTER TYPE"}, nil
}

// qualifiedTableName names a table for the post-commit drain: bare in
// the session's database, db.name elsewhere.
func (s *Session) qualifiedTableName(ctx context.Context, txn *kvclient.Txn, desc *catalog.TableDescriptor) string {
	dbs, err := catalog.ListDatabases(ctx, txn)
	if err != nil {
		return desc.Name
	}
	for _, db := range dbs {
		if db.ID == desc.DatabaseID && db.Name != s.database {
			return db.Name + "." + desc.Name
		}
	}
	return desc.Name
}

func (s *Session) execDropType(ctx context.Context, txn *kvclient.Txn, t *parser.DropType) (*Result, error) {
	d, err := s.lookupType(ctx, txn, t.Name)
	if err != nil {
		if serr, ok := err.(*Error); ok && serr.Code == CodeUndefinedObject && t.IfExists {
			return &Result{Tag: "DROP TYPE"}, nil
		}
		return nil, err
	}
	if err := s.checkCreateInDatabase(ctx, txn, t.Name); err != nil {
		return nil, err
	}
	tables, err := s.cat.List(ctx, txn)
	if err != nil {
		return nil, ToSQLError(err)
	}
	for _, shared := range tables {
		for _, c := range shared.Columns {
			if c.Type == types.Enum && c.EnumType == d.ID {
				return nil, newErrf(CodeDependentObjectsExist, "cannot drop type %q because column %s.%s depends on it (drop the column, or the table)", d.Name, shared.Name, c.Name)
			}
		}
	}
	if err := catalog.DropType(ctx, txn, d); err != nil {
		return nil, ToSQLError(err)
	}
	log.Audit("type-ddl", "stmt", "DROP TYPE", "target", d.Name, "principal", s.user)
	return &Result{Tag: "DROP TYPE"}, nil
}
