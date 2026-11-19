package sql

import (
	"context"
	"strconv"
	"strings"

	"github.com/sthorne/datax/pkg/kvclient"
	"github.com/sthorne/datax/pkg/sql/builtins"
	"github.com/sthorne/datax/pkg/sql/catalog"
	"github.com/sthorne/datax/pkg/sql/types"
	"github.com/sthorne/datax/pkg/sql/vtable"
)

// The object identifier types (PG-T-012): 'name'::regclass,
// 'lower'::regproc, 'lower(text)'::regprocedure, 'int4'::regtype,
// 'public'::regnamespace and 'root'::regrole, written on a literal
// (a name, or an OID). Each resolves to the object's OID, which it
// compares and casts (::oid, ::int4) as; a value of one renders as the
// object's name, as PostgreSQL prints it (types.Datum.Reg). A name that
// is no such object is the error PostgreSQL raises; an OID that is none
// renders as the number.

// regKind is the reg* type a cast names ("" for none).
func regKind(cast string) string {
	c := strings.TrimPrefix(strings.ToLower(strings.TrimSpace(cast)), "pg_catalog.")
	switch c {
	case "regclass", "regtype", "regproc", "regprocedure", "regnamespace", "regrole":
		return c
	}
	return ""
}

// typeOIDsByName are the built-in types a regtype literal names, by
// every spelling format_type and the parser accept.
var typeOIDsByName = map[string]int64{
	"bool": 16, "boolean": 16, "bytea": 17, "char": 18, "\"char\"": 18, "name": 19,
	"int8": 20, "bigint": 20, "int2": 21, "smallint": 21, "int4": 23, "int": 23, "integer": 23,
	"regproc": 24, "text": 25, "oid": 26, "json": 114, "float4": 700, "real": 700,
	"float8": 701, "double precision": 701, "bpchar": 1042, "character": 1042,
	"varchar": 1043, "character varying": 1043, "date": 1082, "time": 1083, "time without time zone": 1083,
	"timestamp": 1114, "timestamp without time zone": 1114, "timestamptz": 1184, "timestamp with time zone": 1184,
	"interval": 1186, "numeric": 1700, "decimal": 1700, "regprocedure": 2202, "regclass": 2205, "regtype": 2206,
	"uuid": 2950, "jsonb": 3802, "tsvector": 3614, "tsquery": 3615, "regnamespace": 4089, "regrole": 4096,
}

// regtypeOID resolves a builtin type name (an array of one included).
func regtypeOID(name string) (int64, bool) {
	n := strings.ToLower(strings.TrimSpace(name))
	n = strings.TrimPrefix(n, "pg_catalog.")
	if strings.HasSuffix(n, "[]") {
		elem, ok := regtypeOID(strings.TrimSuffix(n, "[]"))
		if !ok {
			return 0, false
		}
		arr := vtable.ArrayTypeOID(elem)
		return arr, arr != 0
	}
	if oid, ok := typeOIDsByName[n]; ok {
		return oid, true
	}
	if f, err := types.ParseType(n); err == nil {
		return vtable.TypeOID(f), true
	}
	return 0, false
}

// regtypeName is the name a regtype value prints: format_type's.
func regtypeName(oid int64) (string, bool) {
	if n, ok := map[int64]string{18: "\"char\"", 19: "name", 24: "regproc", 26: "oid", 114: "json", 700: "real",
		2202: "regprocedure", 2205: "regclass", 2206: "regtype", 4089: "regnamespace", 4096: "regrole"}[oid]; ok {
		return n, true
	}
	if n := vtable.FormatTypeOID(oid); n != "-" {
		return n, true
	}
	return "", false
}

// resolveRegLiteral resolves lit::kind: the object's OID, carrying its
// name for display.
func (s *Session) resolveRegLiteral(ctx context.Context, txn *kvclient.Txn, kind string, lit types.Datum) (types.Datum, error) {
	text := strings.TrimSpace(lit.Text())
	if n, err := strconv.ParseInt(text, 10, 64); err == nil {
		d := types.NewInt(n)
		if name, ok, err := s.regName(ctx, txn, kind, n); err != nil {
			return types.Datum{}, err
		} else if ok {
			d.Reg = name
		}
		return d, nil
	}
	var oid int64
	found := false
	switch kind {
	case "regclass":
		o, err := s.regclassOID(ctx, txn, text)
		if err != nil {
			return types.Datum{}, err
		}
		oid, found = o, true
	case "regtype":
		oid, found = regtypeOID(text)
		if !found {
			return types.Datum{}, newErrf(CodeUndefinedObject, "type %q does not exist", text)
		}
	case "regproc", "regprocedure":
		b, ok := lookupFunctionArg(text, kind == "regprocedure")
		if ok {
			oid, found = vtable.ProcOID(b.Name)
		}
		if !found {
			return types.Datum{}, newErrf(CodeUndefinedFunction, "function %q does not exist", text)
		}
	case "regnamespace":
		parts, ok := parseQualifiedName(text)
		if ok && len(parts) == 1 {
			o, ok, err := s.schemaOID(ctx, txn, parts[0])
			if err != nil {
				return types.Datum{}, err
			}
			oid, found = o, ok
		}
		if !found {
			return types.Datum{}, newErrf(CodeInvalidSchemaName, "schema %q does not exist", text)
		}
	case "regrole":
		parts, ok := parseQualifiedName(text)
		if ok && len(parts) == 1 {
			r, err := s.lookupRoleName(ctx, txn, parts[0])
			if err != nil {
				return types.Datum{}, err
			}
			if r != nil {
				oid, found = vtable.RoleOID(r.Name), true
			}
		}
		if !found {
			return types.Datum{}, newErrf(CodeUndefinedObject, "role %q does not exist", text)
		}
	}
	d := types.NewInt(oid)
	if name, ok, err := s.regName(ctx, txn, kind, oid); err != nil {
		return types.Datum{}, err
	} else if ok {
		d.Reg = name
	}
	return d, nil
}

// regName is the name a reg* value of oid prints (ok false when the
// OID names nothing, which prints as the number).
func (s *Session) regName(ctx context.Context, txn *kvclient.Txn, kind string, oid int64) (string, bool, error) {
	switch kind {
	case "regclass":
		names, err := s.regclassNames(ctx, txn)
		if err != nil {
			return "", false, err
		}
		for _, on := range names {
			if on.oid == oid {
				return on.name, true, nil
			}
		}
	case "regtype":
		if oid >= catalog.EnumOID(0) {
			return "", false, nil // an enum (resolved by its name above)
		}
		n, ok := regtypeName(oid)
		return n, ok, nil
	case "regproc", "regprocedure":
		for _, b := range builtins.All() {
			if o, ok := vtable.ProcOID(b.Name); ok && o == oid {
				if kind == "regprocedure" {
					return b.Name + "(" + procArgTypes(b) + ")", true, nil
				}
				return b.Name, true, nil
			}
		}
	case "regnamespace":
		return s.schemaByOID(ctx, txn, oid)
	case "regrole":
		return s.roleByOID(ctx, txn, oid)
	}
	return "", false, nil
}

// procArgTypes renders a builtin's argument types as regprocedure
// prints them.
func procArgTypes(b *builtins.Builtin) string {
	parts := make([]string, 0, len(b.Args))
	for _, f := range b.Args {
		if f == builtins.Any {
			parts = append(parts, "\"any\"")
			continue
		}
		parts = append(parts, pgTypeName(f))
	}
	return strings.Join(parts, ", ")
}
