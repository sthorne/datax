package vtable

import (
	"strings"

	"github.com/sthorne/datax/pkg/sql/builtins"
	"github.com/sthorne/datax/pkg/sql/catalog"
)

// virtualClassOIDBase is where the pg_class OIDs of the catalog tables
// without a PostgreSQL OID of their own start: the base plus the
// table's position in Names().
const virtualClassOIDBase = int64(1 << 30)

// classOIDAt is the pg_class OID of the i-th of Names(): PostgreSQL's
// own for a system catalog it has one for (pg_class is 1259, as
// 'pg_class'::regclass and pg_depend.classid say), else a stand-in.
func classOIDAt(i int, qualified string) int64 {
	if len(qualified) > len(PgCatalog)+1 && qualified[:len(PgCatalog)+1] == PgCatalog+"." {
		if oid, ok := catalogRelOIDs[qualified[len(PgCatalog)+1:]]; ok {
			return oid
		}
	}
	return virtualClassOIDBase + int64(i)
}

// ClassOID is the pg_class OID of a virtual table's descriptor (ok
// false for any other descriptor).
func ClassOID(d *catalog.TableDescriptor) (int64, bool) {
	if d.Virtual == "" {
		return 0, false
	}
	for i, n := range Names() {
		if n == d.Virtual {
			return classOIDAt(i, n), true
		}
	}
	return 0, false
}

// ByClassOID finds the virtual table pg_class lists under oid.
func ByClassOID(oid int64) (*Table, bool) {
	for i, n := range Names() {
		if classOIDAt(i, n) == oid {
			return Lookup(n)
		}
	}
	return nil, false
}

// EachClassOID calls fn with the pg_class OID and the bare name of
// every virtual table.
func EachClassOID(fn func(oid int64, name string)) {
	for i, n := range Names() {
		t, ok := Lookup(n)
		if !ok {
			continue
		}
		fn(classOIDAt(i, n), t.Name)
	}
}

// ProcOID is the pg_proc OID of the builtin named name.
func ProcOID(name string) (int64, bool) {
	for i, b := range builtins.All() {
		if b.Name == name {
			return procOIDBase + int64(i), true
		}
	}
	return 0, false
}

// BuiltinTypeOID reports whether pg_type lists oid among the built-in
// types (the families, the modifier-refined types and their arrays).
func BuiltinTypeOID(oid int64) bool {
	for _, f := range families {
		if TypeOID(f) == oid {
			return true
		}
	}
	for _, o := range ExtraTypeOIDs {
		if o == oid {
			return true
		}
	}
	for _, arr := range arrayTypeOIDs {
		if arr == oid {
			return true
		}
	}
	return false
}

// InformationSchemaClassOIDs are the pg_class OIDs of information_schema's
// views, which pg_table_is_visible reports invisible (the schema is not on
// PostgreSQL's search path).
func InformationSchemaClassOIDs() []int64 {
	var out []int64
	for i, n := range Names() {
		if strings.HasPrefix(n, InformationSchema+".") {
			out = append(out, classOIDAt(i, n))
		}
	}
	return out
}
