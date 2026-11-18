package sql

import (
	"context"
	"errors"
	"strconv"
	"strings"

	"github.com/sthorne/datax/pkg/kvclient"
	"github.com/sthorne/datax/pkg/sql/builtins"
	"github.com/sthorne/datax/pkg/sql/catalog"
	"github.com/sthorne/datax/pkg/sql/parser"
	"github.com/sthorne/datax/pkg/sql/types"
	"github.com/sthorne/datax/pkg/sql/vtable"
)

// The system information and privilege functions applications, ORMs
// and dump tools call (issue #357): pg_get_serial_sequence, the to_reg*
// lookups, current_schemas, pg_is_in_recovery, pg_has_role and the
// has_*_privilege family. They need the catalog and the role graph, so
// the session evaluates them before execution (resolveValueNode) like
// the other catalog functions: with constant arguments a call becomes
// its value; with the object argument naming a column (has_table_
// privilege(c.oid, 'SELECT') over pg_class) it becomes a CASE over every
// object of the kind, keyed by OID and by name. Their results follow
// PostgreSQL's: NULL for a NULL argument or an OID that names nothing,
// the object kind's "does not exist" error for a name that does not.

// sysFunc describes one function: its argument counts, the position of
// the argument that may name a column (-1 for none) for each count, how
// it evaluates on constant arguments, and the objects such a column can
// hold.
type sysFunc struct {
	minArgs, maxArgs int
	objArg           func(n int) int
	eval             func(s *Session, ctx context.Context, txn *kvclient.Txn, c *sysCall) (types.Datum, error)
	domain           func(s *Session, ctx context.Context, txn *kvclient.Txn) ([]domainEntry, error)
}

// sysCall is a call with its arguments evaluated: the values and the
// resolved expressions (whose casts tell an OID from a name).
type sysCall struct {
	fn    string
	args  []types.Datum
	exprs []parser.Expr
}

// domainEntry is one object a column argument can name: the value the
// constant evaluation takes for it and the keys (its OID and name, as
// text) a row's value matches.
type domainEntry struct {
	arg  types.Datum
	keys []string
}

func noObjArg(int) int { return -1 }

// lastButOne is the object position of the (obj, priv) / (user, obj,
// priv) forms.
func lastButOne(n int) int { return n - 2 }

func firstArg(int) int { return 0 }

var sysFuncs map[string]*sysFunc

func init() {
	sysFuncs = map[string]*sysFunc{
		"pg_get_serial_sequence": {minArgs: 2, maxArgs: 2, objArg: noObjArg, eval: (*Session).evalSerialSequence},
		"pg_is_in_recovery": {minArgs: 0, maxArgs: 0, objArg: noObjArg, eval: func(*Session, context.Context, *kvclient.Txn, *sysCall) (types.Datum, error) {
			return types.NewBool(false), nil
		}},
		"current_schemas":      {minArgs: 1, maxArgs: 1, objArg: noObjArg, eval: (*Session).evalCurrentSchemas},
		"to_regclass":          {minArgs: 1, maxArgs: 1, objArg: firstArg, eval: (*Session).evalToRegclass, domain: (*Session).relationNameDomain},
		"to_regproc":           {minArgs: 1, maxArgs: 1, objArg: noObjArg, eval: (*Session).evalToRegproc},
		"to_regprocedure":      {minArgs: 1, maxArgs: 1, objArg: noObjArg, eval: (*Session).evalToRegproc},
		"to_regnamespace":      {minArgs: 1, maxArgs: 1, objArg: noObjArg, eval: (*Session).evalToRegnamespace},
		"to_regrole":           {minArgs: 1, maxArgs: 1, objArg: noObjArg, eval: (*Session).evalToRegrole},
		"pg_has_role":          {minArgs: 2, maxArgs: 3, objArg: lastButOne, eval: (*Session).evalPgHasRole, domain: (*Session).roleDomain},
		"has_column_privilege": {minArgs: 3, maxArgs: 4, objArg: noObjArg, eval: (*Session).evalColumnPrivilege},
	}
	for name, k := range privKinds {
		k := k
		sysFuncs[name] = &sysFunc{minArgs: 2, maxArgs: 3, objArg: lastButOne,
			eval: func(s *Session, ctx context.Context, txn *kvclient.Txn, c *sysCall) (types.Datum, error) {
				return s.evalObjectPrivilege(ctx, txn, c, k)
			},
			domain: k.domain,
		}
	}
	for name := range sysFuncs {
		splicedFuncs[name] = true
	}
}

// spliceSysFunc evaluates a call of one of the sysFuncs: its value, or
// a CASE over the objects its column argument can name.
func (s *Session) spliceSysFunc(ctx context.Context, txn *kvclient.Txn, e parser.Expr, params []types.Datum, keepVolatile bool) (parser.Expr, error) {
	f := sysFuncs[e.Func]
	n := len(e.Args)
	if n < f.minArgs || n > f.maxArgs {
		unknown := make([]string, n)
		for i := range unknown {
			unknown[i] = "unknown"
		}
		return e, newErrf(CodeUndefinedFunction, "function %s(%s) does not exist", e.Func, strings.Join(unknown, ", "))
	}
	exprs := make([]parser.Expr, n)
	colArg := -1
	for i, a := range e.Args {
		r, err := s.resolveValueExprOpts(ctx, txn, a, params, keepVolatile)
		if err != nil {
			return e, err
		}
		exprs[i] = r
		if exprHasColumn(r) {
			if colArg >= 0 || f.domain == nil || f.objArg(n) != i {
				return e, newErrf(CodeFeatureNotSupported, "%s(): only the object argument may name a column; the others must be constants", e.Func)
			}
			colArg = i
		}
	}
	args := make([]types.Datum, n)
	for i, x := range exprs {
		if i == colArg {
			continue
		}
		d, err := evalExpr(x, nil, nil, params)
		if err != nil {
			return e, err
		}
		args[i] = d
	}
	call := &sysCall{fn: e.Func, args: args, exprs: exprs}
	if colArg < 0 {
		for _, a := range args {
			if a.Null {
				return nullLit(), nil // strict
			}
		}
		d, err := f.eval(s, ctx, txn, call)
		if err != nil {
			return e, err
		}
		return parser.Expr{Lit: &d}, nil
	}
	// The object argument names a column: one WHEN per object, keyed by
	// its OID and its name as text; a value that names nothing is NULL.
	for i, a := range args {
		if i != colArg && a.Null {
			return nullLit(), nil
		}
	}
	entries, err := f.domain(s, ctx, txn)
	if err != nil {
		return e, err
	}
	operand := parser.Expr{Func: "concat", Args: []parser.Expr{exprs[colArg]}}
	ce := &parser.CaseExpr{Operand: &operand}
	seen := map[string]bool{}
	for _, ent := range entries {
		one := *call
		one.args = append([]types.Datum(nil), args...)
		one.args[colArg] = ent.arg
		one.exprs = append([]parser.Expr(nil), exprs...)
		one.exprs[colArg] = parser.Expr{Lit: &ent.arg}
		d, err := f.eval(s, ctx, txn, &one)
		if err != nil {
			return e, err
		}
		for _, k := range ent.keys {
			if seen[k] {
				continue
			}
			seen[k] = true
			key, val := types.NewString(k), d
			ce.Whens = append(ce.Whens, parser.CaseWhen{Value: &parser.Expr{Lit: &key}, Result: parser.Expr{Lit: &val}})
		}
	}
	if len(ce.Whens) == 0 {
		return nullLit(), nil
	}
	return parser.Expr{Case: ce}, nil
}

// nullLit is a NULL literal of its own (never the shared types.DNull,
// which a later rewrite of the literal must not reach).
func nullLit() parser.Expr {
	d := types.DNull
	return parser.Expr{Lit: &d}
}

// argIsOID reports whether argument i names its object by OID: an
// integer, or text cast to oid or a reg* type.
func (c *sysCall) argIsOID(i int) (int64, bool) {
	d := c.args[i]
	if d.Fam == types.Int {
		return d.I, true
	}
	switch strings.TrimPrefix(c.exprs[i].Cast, "pg_catalog.") {
	case "oid", "regclass", "regrole", "regnamespace", "regproc", "regprocedure", "regtype":
		if n, err := strconv.ParseInt(strings.TrimSpace(d.Text()), 10, 64); err == nil {
			return n, true
		}
	}
	return 0, false
}

// parseQualifiedName splits a possibly qualified, possibly quoted name
// the way PostgreSQL reads one (stringToQualifiedNameList): parts
// separated by dots, an unquoted part lower-cased, a quoted one kept as
// written ("" inside it is one "). ok is false when it is not a name.
func parseQualifiedName(text string) (parts []string, ok bool) {
	i := 0
	for {
		for i < len(text) && isNameSpace(text[i]) {
			i++
		}
		if i >= len(text) {
			return nil, false
		}
		var part strings.Builder
		if text[i] == '"' {
			i++
			for {
				if i >= len(text) {
					return nil, false
				}
				if text[i] == '"' {
					if i+1 < len(text) && text[i+1] == '"' {
						part.WriteByte('"')
						i += 2
						continue
					}
					i++
					break
				}
				part.WriteByte(text[i])
				i++
			}
			if part.Len() == 0 {
				return nil, false
			}
		} else {
			start := i
			for i < len(text) && text[i] != '.' && !isNameSpace(text[i]) && text[i] != '"' {
				i++
			}
			if i == start {
				return nil, false
			}
			part.WriteString(strings.ToLower(text[start:i]))
		}
		parts = append(parts, part.String())
		for i < len(text) && isNameSpace(text[i]) {
			i++
		}
		if i >= len(text) {
			return parts, true
		}
		if text[i] != '.' {
			return nil, false
		}
		i++
	}
}

func isNameSpace(c byte) bool {
	return c == ' ' || c == '\t' || c == '\n' || c == '\r' || c == '\f' || c == '\v'
}

// relationLookupName maps a parsed relation name to the form the
// session resolves: t; public.t is t; pg_catalog.t and
// information_schema.t stay qualified; db.t and db.public.t name another
// database's table. ok is false for a name no relation can have.
func relationLookupName(parts []string) (string, bool) {
	switch len(parts) {
	case 1:
		return parts[0], true
	case 2:
		if parts[0] == catalog.PublicSchema {
			return parts[1], true
		}
		return parts[0] + "." + parts[1], true
	case 3:
		if parts[1] == catalog.PublicSchema {
			return parts[0] + "." + parts[2], true
		}
	}
	return "", false
}

// relRef is what a relation argument resolved to: a table or view
// (desc, which for a sequence is its one-row relation), the sequence's
// descriptor when it is one, and its OID.
type relRef struct {
	desc *catalog.TableDescriptor
	seq  *catalog.SequenceDescriptor
	oid  int64
}

// lookupRelationName resolves a relation name as regclass input does;
// nil when nothing is so named.
func (s *Session) lookupRelationName(ctx context.Context, txn *kvclient.Txn, text string) (*relRef, error) {
	parts, ok := parseQualifiedName(text)
	if !ok {
		return nil, nil
	}
	name, ok := relationLookupName(parts)
	if !ok {
		return nil, nil
	}
	desc, err := s.lookup(ctx, txn, name)
	if err != nil || desc == nil {
		if err != nil && !isNotFound(err) {
			return nil, err
		}
		return nil, nil
	}
	return s.relationOf(ctx, txn, desc)
}

// isNotFound reports whether err says a looked-up object is missing.
func isNotFound(err error) bool {
	var nf *catalog.ErrTableNotFound
	var dnf *catalog.ErrDatabaseNotFound
	var snf *catalog.ErrSequenceNotFound
	var se *Error
	if errors.As(err, &se) {
		return se.Code == CodeUndefinedTable || se.Code == CodeInvalidCatalogName || se.Code == CodeInvalidSchemaName
	}
	return errors.As(err, &nf) || errors.As(err, &dnf) || errors.As(err, &snf)
}

// relationOf completes a relation from its descriptor.
func (s *Session) relationOf(ctx context.Context, txn *kvclient.Txn, desc *catalog.TableDescriptor) (*relRef, error) {
	r := &relRef{desc: desc, oid: vtable.TableOID(desc)}
	if desc.Virtual == "sequence" {
		sd, err := catalog.ReadSequence(ctx, txn, desc.ID)
		if err != nil {
			return nil, err
		}
		r.seq = sd
	}
	if oid, ok := vtable.ClassOID(desc); ok {
		r.oid = oid
	}
	return r, nil
}

// currentDatabaseID is the ID of the session's database.
func (s *Session) currentDatabaseID(ctx context.Context, txn *kvclient.Txn) (uint64, error) {
	db, err := s.cat.Database(ctx, txn, s.database)
	if err != nil {
		return 0, ToSQLError(err)
	}
	return db.ID, nil
}

// relationByOID resolves a relation OID: a table, view or sequence of
// the current database, or a catalog table. nil when none has it.
func (s *Session) relationByOID(ctx context.Context, txn *kvclient.Txn, oid int64) (*relRef, error) {
	if vt, ok := vtable.ByClassOID(oid); ok {
		return s.relationOf(ctx, txn, vt.Descriptor())
	}
	if oid <= 0 {
		return nil, nil
	}
	dbID, err := s.currentDatabaseID(ctx, txn)
	if err != nil {
		return nil, err
	}
	if desc, err := catalog.ReadTable(ctx, txn, vtable.TableIDOfOID(oid)); err == nil && desc != nil && desc.DatabaseID == dbID {
		return &relRef{desc: desc, oid: oid}, nil
	}
	if sd, err := catalog.ReadSequence(ctx, txn, uint64(oid)); err == nil && sd != nil && sd.DatabaseID == dbID {
		return &relRef{desc: sequenceRelation(sd), seq: sd, oid: oid}, nil
	}
	return nil, nil
}

// relationArg resolves argument i as a relation: by OID (nil when none
// has it) or by name (42P01 when none has it).
func (s *Session) relationArg(ctx context.Context, txn *kvclient.Txn, c *sysCall, i int) (*relRef, error) {
	if oid, ok := c.argIsOID(i); ok {
		return s.relationByOID(ctx, txn, oid)
	}
	r, err := s.lookupRelationName(ctx, txn, c.args[i].Text())
	if err != nil {
		return nil, err
	}
	if r == nil {
		return nil, newErrf(CodeUndefinedTable, "relation %q does not exist", c.args[i].Text())
	}
	return r, nil
}

// relationDomain lists every relation a pg_class row can name: the
// current database's tables, views and sequences, and the catalogs.
func (s *Session) relationDomain(ctx context.Context, txn *kvclient.Txn) ([]domainEntry, error) {
	dbID, err := s.currentDatabaseID(ctx, txn)
	if err != nil {
		return nil, err
	}
	all, err := s.cat.List(ctx, txn)
	if err != nil {
		return nil, err
	}
	var out []domainEntry
	for _, d := range all {
		if d.DatabaseID != dbID {
			continue
		}
		oid := vtable.TableOID(d)
		out = append(out, domainEntry{arg: types.NewInt(oid), keys: []string{strconv.FormatInt(oid, 10), d.Name}})
	}
	seqs, err := catalog.ListSequences(ctx, txn, dbID)
	if err != nil {
		return nil, err
	}
	for _, sq := range seqs {
		oid := int64(sq.ID)
		out = append(out, domainEntry{arg: types.NewInt(oid), keys: []string{strconv.FormatInt(oid, 10), sq.Name}})
	}
	vtable.EachClassOID(func(oid int64, name string) {
		out = append(out, domainEntry{arg: types.NewInt(oid), keys: []string{strconv.FormatInt(oid, 10), name}})
	})
	return out, nil
}

// relationNameDomain is relationDomain keyed by name alone (to_regclass
// of a text column).
func (s *Session) relationNameDomain(ctx context.Context, txn *kvclient.Txn) ([]domainEntry, error) {
	all, err := s.relationDomain(ctx, txn)
	if err != nil {
		return nil, err
	}
	out := make([]domainEntry, 0, len(all))
	for _, e := range all {
		name := e.keys[1]
		out = append(out, domainEntry{arg: types.NewString(quoteIdentifier(name)), keys: []string{name}})
	}
	return out, nil
}

// quoteIdentifier quotes a name when PostgreSQL's quote_ident would.
func quoteIdentifier(s string) string { return builtins.QuoteIdent(s) }

// evalSerialSequence is pg_get_serial_sequence(table, column): the
// sequence a SERIAL, identity or OWNED BY column draws from, as
// schema-qualified text, or NULL when it has none. The table name is
// read as SQL reads one (lower-cased unless quoted); the column name is
// taken as written.
func (s *Session) evalSerialSequence(ctx context.Context, txn *kvclient.Txn, c *sysCall) (types.Datum, error) {
	r, err := s.relationArg(ctx, txn, &sysCall{args: []types.Datum{types.NewString(c.args[0].Text())}, exprs: c.exprs[:1]}, 0)
	if err != nil {
		return types.Datum{}, err
	}
	colName := c.args[1].Text()
	col, ok := r.desc.Col(colName)
	if !ok || col.Hidden {
		return types.Datum{}, newErrf(CodeUndefinedColumn, "column %q of relation %q does not exist", colName, r.desc.Name)
	}
	if r.desc.Virtual != "" {
		return types.DNull, nil
	}
	var sd *catalog.SequenceDescriptor
	if col.SequenceID != 0 {
		if d, err := catalog.ReadSequence(ctx, txn, col.SequenceID); err == nil {
			sd = d
		}
	}
	if sd == nil {
		// A sequence made OWNED BY the column.
		seqs, err := catalog.ListSequences(ctx, txn, r.desc.DatabaseID)
		if err != nil {
			return types.Datum{}, err
		}
		for _, q := range seqs {
			if q.OwnerTable == r.desc.ID && q.OwnerColumn == col.ID {
				sd = q
				break
			}
		}
	}
	if sd == nil {
		return types.DNull, nil
	}
	return types.NewString(catalog.PublicSchema + "." + quoteIdentifier(sd.Name)), nil
}

// evalCurrentSchemas is current_schemas(include_implicit): the search
// path's schemas, pg_catalog first when the implicit ones are asked for.
func (s *Session) evalCurrentSchemas(_ context.Context, _ *kvclient.Txn, c *sysCall) (types.Datum, error) {
	implicit, err := c.args[0].Coerce(types.Bool)
	if err != nil {
		return types.Datum{}, newErrf(CodeInvalidTextRepresentation, "current_schemas: %v", err)
	}
	elems := []types.Datum{types.NewString(catalog.PublicSchema)}
	if implicit.B {
		elems = append([]types.Datum{types.NewString(vtable.PgCatalog)}, elems...)
	}
	return types.NewArray(types.String, elems), nil
}

// evalToRegclass is to_regclass(text): the relation's OID, NULL when
// nothing is so named.
func (s *Session) evalToRegclass(ctx context.Context, txn *kvclient.Txn, c *sysCall) (types.Datum, error) {
	r, err := s.lookupRelationName(ctx, txn, c.args[0].Text())
	if err != nil || r == nil {
		return types.DNull, err
	}
	return types.NewInt(r.oid), nil
}

// evalToRegproc is to_regproc(name) and to_regprocedure(name(args)):
// the function's pg_proc OID, NULL when no builtin is so named (or, for
// to_regprocedure, takes that many arguments).
func (s *Session) evalToRegproc(_ context.Context, _ *kvclient.Txn, c *sysCall) (types.Datum, error) {
	b, ok := lookupFunctionArg(c.args[0].Text(), c.fn == "to_regprocedure")
	if !ok {
		return types.DNull, nil
	}
	oid, ok := vtable.ProcOID(b.Name)
	if !ok {
		return types.DNull, nil
	}
	return types.NewInt(oid), nil
}

// lookupFunctionArg resolves a function named as regproc (name) or
// regprocedure (name(argtype, ...)) input reads it. With signature, the
// argument list is required and the function must accept that many
// arguments.
func lookupFunctionArg(text string, signature bool) (*builtins.Builtin, bool) {
	name, argList, hasArgs := strings.Cut(text, "(")
	if signature && !hasArgs {
		return nil, false
	}
	parts, ok := parseQualifiedName(name)
	if !ok || len(parts) > 2 || (len(parts) == 2 && parts[0] != vtable.PgCatalog) {
		return nil, false
	}
	b, ok := builtins.Lookup(parts[len(parts)-1])
	if !ok || b.Name != parts[len(parts)-1] {
		return nil, false
	}
	if hasArgs {
		inner, closed := strings.CutSuffix(strings.TrimSpace(argList), ")")
		if !closed {
			return nil, false
		}
		n := 0
		if strings.TrimSpace(inner) != "" {
			n = strings.Count(inner, ",") + 1
		}
		if !b.ArityOK(n) {
			return nil, false
		}
	}
	return b, true
}

// schemaOIDs are the schemas pg_namespace lists.
var schemaOIDs = map[string]int64{vtable.PgCatalog: vtable.OIDPgCatalog, catalog.PublicSchema: vtable.OIDPublic, vtable.InformationSchema: vtable.OIDInformationSchema}

// evalToRegnamespace is to_regnamespace(name): the schema's OID, NULL
// when there is none.
func (s *Session) evalToRegnamespace(_ context.Context, _ *kvclient.Txn, c *sysCall) (types.Datum, error) {
	parts, ok := parseQualifiedName(c.args[0].Text())
	if !ok || len(parts) != 1 {
		return types.DNull, nil
	}
	if oid, ok := schemaOIDs[parts[0]]; ok {
		return types.NewInt(oid), nil
	}
	return types.DNull, nil
}

// evalToRegrole is to_regrole(name): the role's OID, NULL when there is
// none (public is not a role).
func (s *Session) evalToRegrole(ctx context.Context, txn *kvclient.Txn, c *sysCall) (types.Datum, error) {
	parts, ok := parseQualifiedName(c.args[0].Text())
	if !ok || len(parts) != 1 {
		return types.DNull, nil
	}
	d, err := s.lookupRoleName(ctx, txn, parts[0])
	if err != nil || d == nil {
		return types.DNull, err
	}
	return types.NewInt(vtable.RoleOID(d.Name)), nil
}

// lookupRoleName reads a role (nil when none is so named; public is
// not one).
func (s *Session) lookupRoleName(ctx context.Context, txn *kvclient.Txn, name string) (*catalog.RoleDescriptor, error) {
	if name == catalog.PublicRole || name == "" {
		return nil, nil
	}
	return catalog.LookupRole(ctx, txn, name)
}

// roleByOID resolves a role OID to its name.
func (s *Session) roleByOID(ctx context.Context, txn *kvclient.Txn, oid int64) (string, bool, error) {
	roles, err := catalog.ListRoles(ctx, txn)
	if err != nil {
		return "", false, err
	}
	for _, r := range roles {
		if vtable.RoleOID(r.Name) == oid {
			return r.Name, true, nil
		}
	}
	return "", false, nil
}

// roleDomain lists every role, by OID and name (pg_has_role over
// pg_roles).
func (s *Session) roleDomain(ctx context.Context, txn *kvclient.Txn) ([]domainEntry, error) {
	roles, err := catalog.ListRoles(ctx, txn)
	if err != nil {
		return nil, err
	}
	out := make([]domainEntry, 0, len(roles))
	for _, r := range roles {
		oid := vtable.RoleOID(r.Name)
		out = append(out, domainEntry{arg: types.NewInt(oid), keys: []string{strconv.FormatInt(oid, 10), r.Name}})
	}
	return out, nil
}

// roleArg resolves argument i as a role: by OID (ok false when no role
// has it) or by name (42704 when no role has it; public only where
// allowPublic, as has_*_privilege's user argument takes it).
func (s *Session) roleArg(ctx context.Context, txn *kvclient.Txn, c *sysCall, i int, allowPublic bool) (string, bool, error) {
	if oid, ok := c.argIsOID(i); ok {
		return s.roleByOID(ctx, txn, oid)
	}
	name := c.args[i].Text()
	if allowPublic && name == catalog.PublicRole {
		return catalog.PublicRole, true, nil
	}
	if r, ok := predefinedRoles[name]; ok {
		if r == "" {
			r = name // no datax counterpart: a role only admins hold
		}
		return r, true, nil
	}
	d, err := s.lookupRoleName(ctx, txn, name)
	if err != nil {
		return "", false, err
	}
	if d == nil {
		return "", false, newErrf(CodeUndefinedObject, "role %q does not exist", name)
	}
	return d.Name, true, nil
}

// predefinedRoles are PostgreSQL's predefined roles, which tools name
// (psql's \l+ asks pg_has_role('pg_read_all_stats', 'USAGE')): the two
// with a datax counterpart answer as it, the others as a role no one
// but root and the admins holds.
var predefinedRoles = map[string]string{
	"pg_read_all_data": catalog.ReadAllRole, "pg_write_all_data": catalog.WriteAllRole,
	"pg_monitor": "", "pg_read_all_settings": "", "pg_read_all_stats": "", "pg_stat_scan_tables": "",
	"pg_signal_backend": "", "pg_read_server_files": "", "pg_write_server_files": "", "pg_execute_server_program": "",
	"pg_checkpoint": "", "pg_maintain": "", "pg_use_reserved_connections": "", "pg_create_subscription": "",
	"pg_database_owner": "",
}

// privSpec is one entry of a privilege-list argument: the privilege and
// whether the grant (or admin) option is asked for.
type privSpec struct {
	priv   string
	option bool
}

// parsePrivileges reads a privilege-list argument as PostgreSQL does:
// comma-separated, case-insensitive, each optionally followed by WITH
// GRANT OPTION (or, for roles, WITH ADMIN OPTION); 22023 for anything
// else.
func parsePrivileges(text string, valid []string, roles bool) ([]privSpec, error) {
	var out []privSpec
	for _, chunk := range strings.Split(text, ",") {
		chunk = strings.TrimSpace(chunk)
		up := strings.ToUpper(chunk)
		spec, found := privSpec{}, false
		for _, v := range valid {
			switch {
			case up == v:
				spec, found = privSpec{priv: v}, true
			case up == v+" WITH GRANT OPTION", roles && up == v+" WITH ADMIN OPTION":
				spec, found = privSpec{priv: v, option: true}, true
			}
		}
		if !found {
			return nil, newErrf(CodeInvalidParameterValue, "unrecognized privilege type: %q", chunk)
		}
		out = append(out, spec)
	}
	return out, nil
}

// asRole runs fn with the privilege checks acting as role.
func (s *Session) asRole(role string, fn func() (bool, error)) (bool, error) {
	saved := s.privAs
	s.privAs = role
	defer func() { s.privAs = saved }()
	return fn()
}

// privUser resolves the user argument of a has_*_privilege call: the
// first of n arguments when n is the long form's count, else the
// current role. ok is false for an OID no role has (PostgreSQL then
// answers as if for a role holding nothing but PUBLIC's grants).
func (s *Session) privUser(ctx context.Context, txn *kvclient.Txn, c *sysCall, longForm bool) (string, error) {
	if !longForm {
		return s.actor(), nil
	}
	name, ok, err := s.roleArg(ctx, txn, c, 0, true)
	if err != nil {
		return "", err
	}
	if !ok {
		return catalog.PublicRole, nil
	}
	return name, nil
}

// anyPriv reports whether any of the privileges holds.
func anyPriv(specs []privSpec, check func(privSpec) (bool, error)) (types.Datum, error) {
	for _, p := range specs {
		ok, err := check(p)
		if err != nil {
			return types.Datum{}, err
		}
		if ok {
			return types.NewBool(true), nil
		}
	}
	return types.NewBool(false), nil
}

// privKind is one has_<kind>_privilege function: the privileges it
// takes, how it resolves its object, how it answers, and the objects a
// column argument can name.
type privKind struct {
	privs   []string
	resolve func(s *Session, ctx context.Context, txn *kvclient.Txn, c *sysCall, i int) (any, error)
	check   func(s *Session, ctx context.Context, txn *kvclient.Txn, obj any, p privSpec) (bool, error)
	domain  func(s *Session, ctx context.Context, txn *kvclient.Txn) ([]domainEntry, error)
}

var privKinds = map[string]*privKind{
	"has_table_privilege": {
		privs:   []string{"SELECT", "INSERT", "UPDATE", "DELETE", "TRUNCATE", "REFERENCES", "TRIGGER", "MAINTAIN", "RULE"},
		resolve: resolveRelationObj, check: (*Session).relationPrivAllowed, domain: (*Session).relationDomain,
	},
	"has_any_column_privilege": {
		privs:   []string{"SELECT", "INSERT", "UPDATE", "REFERENCES"},
		resolve: resolveRelationObj, check: (*Session).relationPrivAllowed, domain: (*Session).relationDomain,
	},
	"has_sequence_privilege": {
		privs: []string{"USAGE", "SELECT", "UPDATE"},
		resolve: func(s *Session, ctx context.Context, txn *kvclient.Txn, c *sysCall, i int) (any, error) {
			r, err := s.relationArg(ctx, txn, c, i)
			if err != nil || r == nil {
				return nil, err
			}
			if r.seq == nil {
				return nil, newErrf(CodeWrongObjectType, "%q is not a sequence", c.args[i].Text())
			}
			return r, nil
		},
		check: (*Session).relationPrivAllowed, domain: (*Session).sequenceDomain,
	},
	"has_schema_privilege": {
		privs: []string{"CREATE", "USAGE"},
		resolve: func(s *Session, ctx context.Context, txn *kvclient.Txn, c *sysCall, i int) (any, error) {
			if oid, ok := c.argIsOID(i); ok {
				for name, o := range schemaOIDs {
					if o == oid {
						return name, nil
					}
				}
				return nil, nil
			}
			name := c.args[i].Text()
			if _, ok := schemaOIDs[name]; !ok {
				return nil, newErrf(CodeInvalidSchemaName, "schema %q does not exist", name)
			}
			return name, nil
		},
		check: (*Session).schemaPrivAllowed,
		domain: func(*Session, context.Context, *kvclient.Txn) ([]domainEntry, error) {
			var out []domainEntry
			for name, oid := range schemaOIDs {
				out = append(out, domainEntry{arg: types.NewInt(oid), keys: []string{strconv.FormatInt(oid, 10), name}})
			}
			return out, nil
		},
	},
	"has_database_privilege": {
		privs: []string{"CREATE", "CONNECT", "TEMPORARY", "TEMP"},
		resolve: func(s *Session, ctx context.Context, txn *kvclient.Txn, c *sysCall, i int) (any, error) {
			if oid, ok := c.argIsOID(i); ok {
				dbs, err := catalog.ListDatabases(ctx, txn)
				if err != nil {
					return nil, err
				}
				for _, d := range dbs {
					if vtable.DatabaseOID(d) == oid {
						return d, nil
					}
				}
				return nil, nil
			}
			name := c.args[i].Text()
			db, err := s.cat.Database(ctx, txn, name)
			if err != nil {
				if isNotFound(err) {
					return nil, newErrf(CodeInvalidCatalogName, "database %q does not exist", name)
				}
				return nil, ToSQLError(err)
			}
			return db, nil
		},
		check: func(s *Session, ctx context.Context, txn *kvclient.Txn, obj any, p privSpec) (bool, error) {
			db := obj.(*catalog.DatabaseDescriptor)
			priv := p.priv
			if priv == "TEMPORARY" || priv == "TEMP" {
				priv = catalog.PrivConnect // granted to PUBLIC with CONNECT; no grant of its own
			}
			if p.option {
				return s.holdsGrantOption(ctx, txn, db.Owner, db.GrantOptions, priv)
			}
			return s.databasePrivAllowed(ctx, txn, db, priv)
		},
		domain: func(s *Session, ctx context.Context, txn *kvclient.Txn) ([]domainEntry, error) {
			dbs, err := catalog.ListDatabases(ctx, txn)
			if err != nil {
				return nil, err
			}
			var out []domainEntry
			for _, d := range dbs {
				oid := vtable.DatabaseOID(d)
				out = append(out, domainEntry{arg: types.NewInt(oid), keys: []string{strconv.FormatInt(oid, 10), d.Name}})
			}
			return out, nil
		},
	},
	"has_function_privilege": {
		privs: []string{"EXECUTE"},
		resolve: func(s *Session, ctx context.Context, txn *kvclient.Txn, c *sysCall, i int) (any, error) {
			if oid, ok := c.argIsOID(i); ok {
				if _, ok := vtable.ProcName(oid); !ok {
					return nil, nil
				}
				return oid, nil
			}
			text := c.args[i].Text()
			b, ok := lookupFunctionArg(text, true)
			if !ok {
				return nil, newErrf(CodeUndefinedFunction, "function %s does not exist", strings.TrimSpace(text))
			}
			return b, nil
		},
		// EXECUTE on every function is PUBLIC's, as PostgreSQL grants it.
		check: func(*Session, context.Context, *kvclient.Txn, any, privSpec) (bool, error) { return true, nil },
		domain: func(*Session, context.Context, *kvclient.Txn) ([]domainEntry, error) {
			var out []domainEntry
			for _, b := range builtins.All() {
				if oid, ok := vtable.ProcOID(b.Name); ok {
					out = append(out, domainEntry{arg: types.NewInt(oid), keys: []string{strconv.FormatInt(oid, 10)}})
				}
			}
			return out, nil
		},
	},
	"has_type_privilege": {
		privs: []string{"USAGE"},
		resolve: func(s *Session, ctx context.Context, txn *kvclient.Txn, c *sysCall, i int) (any, error) {
			if oid, ok := c.argIsOID(i); ok {
				known, err := s.typeOIDKnown(ctx, txn, oid)
				if err != nil || !known {
					return nil, err
				}
				return oid, nil
			}
			name := c.args[i].Text()
			ok, err := s.typeNameKnown(ctx, txn, name)
			if err != nil {
				return nil, err
			}
			if !ok {
				return nil, newErrf(CodeUndefinedObject, "type %q does not exist", name)
			}
			return name, nil
		},
		// USAGE on every type is PUBLIC's, as PostgreSQL grants it.
		check: func(*Session, context.Context, *kvclient.Txn, any, privSpec) (bool, error) { return true, nil },
	},
	"has_tablespace_privilege": {
		privs: []string{"CREATE"},
		resolve: func(s *Session, ctx context.Context, txn *kvclient.Txn, c *sysCall, i int) (any, error) {
			spaces := map[string]int64{"pg_default": 1663, "pg_global": 1664}
			if oid, ok := c.argIsOID(i); ok {
				for name, o := range spaces {
					if o == oid {
						return name, nil
					}
				}
				return nil, nil
			}
			name := c.args[i].Text()
			if _, ok := spaces[name]; !ok {
				return nil, newErrf(CodeUndefinedObject, "tablespace %q does not exist", name)
			}
			return name, nil
		},
		// The tablespaces are root's, with no grants: admins only.
		check: func(s *Session, ctx context.Context, txn *kvclient.Txn, _ any, _ privSpec) (bool, error) {
			return s.actorIsAdmin(ctx, txn)
		},
	},
	"has_foreign_data_wrapper_privilege": {privs: []string{"USAGE"}, resolve: noneSuch("foreign-data wrapper"), check: neverHeld},
	"has_server_privilege":               {privs: []string{"USAGE"}, resolve: noneSuch("server"), check: neverHeld},
}

// noneSuch resolves the objects of a kind datax has none of: NULL for an
// OID, 42704 for a name.
func noneSuch(kind string) func(*Session, context.Context, *kvclient.Txn, *sysCall, int) (any, error) {
	return func(_ *Session, _ context.Context, _ *kvclient.Txn, c *sysCall, i int) (any, error) {
		if _, ok := c.argIsOID(i); ok {
			return nil, nil
		}
		return nil, newErrf(CodeUndefinedObject, "%s %q does not exist", kind, c.args[i].Text())
	}
}

func neverHeld(*Session, context.Context, *kvclient.Txn, any, privSpec) (bool, error) {
	return false, nil
}

func resolveRelationObj(s *Session, ctx context.Context, txn *kvclient.Txn, c *sysCall, i int) (any, error) {
	r, err := s.relationArg(ctx, txn, c, i)
	if err != nil || r == nil {
		return nil, err
	}
	return r, nil
}

// sequenceDomain lists the current database's sequences.
func (s *Session) sequenceDomain(ctx context.Context, txn *kvclient.Txn) ([]domainEntry, error) {
	dbID, err := s.currentDatabaseID(ctx, txn)
	if err != nil {
		return nil, err
	}
	seqs, err := catalog.ListSequences(ctx, txn, dbID)
	if err != nil {
		return nil, err
	}
	out := make([]domainEntry, 0, len(seqs))
	for _, sq := range seqs {
		oid := int64(sq.ID)
		out = append(out, domainEntry{arg: types.NewInt(oid), keys: []string{strconv.FormatInt(oid, 10), sq.Name}})
	}
	return out, nil
}

// evalObjectPrivilege is has_<kind>_privilege([user,] object,
// privilege).
func (s *Session) evalObjectPrivilege(ctx context.Context, txn *kvclient.Txn, c *sysCall, k *privKind) (types.Datum, error) {
	n := len(c.args)
	user, err := s.privUser(ctx, txn, c, n == 3)
	if err != nil {
		return types.Datum{}, err
	}
	obj, err := k.resolve(s, ctx, txn, c, n-2)
	if err != nil {
		return types.Datum{}, err
	}
	specs, err := parsePrivileges(c.args[n-1].Text(), k.privs, false)
	if err != nil {
		return types.Datum{}, err
	}
	if obj == nil {
		return types.DNull, nil
	}
	var out types.Datum
	_, err = s.asRole(user, func() (bool, error) {
		var err error
		out, err = anyPriv(specs, func(p privSpec) (bool, error) { return k.check(s, ctx, txn, obj, p) })
		return false, err
	})
	return out, err
}

// evalColumnPrivilege is has_column_privilege([user,] table, column,
// privilege). datax grants on tables, not columns: a column's privilege
// is its table's.
func (s *Session) evalColumnPrivilege(ctx context.Context, txn *kvclient.Txn, c *sysCall) (types.Datum, error) {
	n := len(c.args)
	user, err := s.privUser(ctx, txn, c, n == 4)
	if err != nil {
		return types.Datum{}, err
	}
	r, err := s.relationArg(ctx, txn, c, n-3)
	if err != nil {
		return types.Datum{}, err
	}
	specs, err := parsePrivileges(c.args[n-1].Text(), []string{"SELECT", "INSERT", "UPDATE", "REFERENCES"}, false)
	if err != nil {
		return types.Datum{}, err
	}
	if r == nil {
		return types.DNull, nil
	}
	colArg := c.args[n-2]
	if colArg.Fam == types.Int {
		// An attribute number: the visible columns from 1.
		num, found := int64(0), false
		for _, col := range r.desc.Columns {
			if col.Hidden {
				continue
			}
			num++
			if num == colArg.I {
				found = true
				break
			}
		}
		if !found {
			return types.DNull, nil
		}
	} else if col, ok := r.desc.Col(colArg.Text()); !ok || col.Hidden {
		return types.Datum{}, newErrf(CodeUndefinedColumn, "column %q of relation %q does not exist", colArg.Text(), r.desc.Name)
	}
	var out types.Datum
	_, err = s.asRole(user, func() (bool, error) {
		var err error
		out, err = anyPriv(specs, func(p privSpec) (bool, error) { return s.relationPrivAllowed(ctx, txn, r, p) })
		return false, err
	})
	return out, err
}

// actorIsAdmin reports whether the acting role is root or an admin.
func (s *Session) actorIsAdmin(ctx context.Context, txn *kvclient.Txn) (bool, error) {
	if s.actor() == catalog.RootRole {
		return true, nil
	}
	set, err := s.roleSet(ctx, txn)
	if err != nil {
		return false, err
	}
	return set.IsAdmin(), nil
}

// holdsGrantOption reports whether the acting role may grant priv on an
// object: root, admins and the owner may; others need the grant option.
func (s *Session) holdsGrantOption(ctx context.Context, txn *kvclient.Txn, owner string, options map[string][]string, priv string) (bool, error) {
	if s.actor() == catalog.RootRole {
		return true, nil
	}
	set, err := s.roleSet(ctx, txn)
	if err != nil {
		return false, err
	}
	return set.IsAdmin() || set.Has(catalog.OwnerOf(owner)) || catalog.HasPrivilege(options, set, priv), nil
}

// relationPrivAllowed answers a table, column or sequence privilege on
// a relation for the acting role. The privileges datax grants are its
// own checks' answers (tablePrivAllowed without the schema's USAGE,
// which PostgreSQL's functions do not consult either;
// sequencePrivAllowed); REFERENCES, TRIGGER and MAINTAIN, which datax
// does not grant, are the owner's and admins'; RULE is no one's.
func (s *Session) relationPrivAllowed(ctx context.Context, txn *kvclient.Txn, obj any, p privSpec) (bool, error) {
	r := obj.(*relRef)
	if p.priv == "RULE" {
		return false, nil
	}
	if r.seq != nil {
		if p.option {
			return s.holdsGrantOption(ctx, txn, r.seq.Owner, r.seq.GrantOptions, p.priv)
		}
		switch p.priv {
		case catalog.PrivUsage, "SELECT", "UPDATE":
			return s.sequencePrivAllowed(ctx, txn, r.seq, p.priv)
		}
		return s.actorIsAdmin(ctx, txn)
	}
	desc := r.desc
	owner := desc.Owner
	if desc.Virtual != "" {
		owner = catalog.RootRole
	}
	if p.option {
		return s.holdsGrantOption(ctx, txn, owner, desc.GrantOptions, p.priv)
	}
	switch p.priv {
	case "SELECT", "INSERT", "UPDATE", "DELETE", "TRUNCATE":
		return s.tablePrivAllowedSchema(ctx, txn, desc, p.priv, false)
	}
	if desc.Virtual != "" {
		return s.actorIsAdmin(ctx, txn)
	}
	return s.isOwner(ctx, txn, owner)
}

// schemaPrivAllowed answers USAGE and CREATE on a schema. public's are
// datax's own: USAGE is everyone's unless revoked from PUBLIC, CREATE
// is what creating a table in the database takes. The catalog schemas
// are everyone's to use and admins' alone to create in.
func (s *Session) schemaPrivAllowed(ctx context.Context, txn *kvclient.Txn, obj any, p privSpec) (bool, error) {
	if obj.(string) != catalog.PublicSchema {
		if p.priv == catalog.PrivUsage && !p.option {
			return true, nil
		}
		return s.actorIsAdmin(ctx, txn)
	}
	db, err := s.cat.Database(ctx, txn, s.database)
	if err != nil {
		return false, ToSQLError(err)
	}
	if p.option {
		return s.holdsGrantOption(ctx, txn, db.Owner, db.SchemaGrantOptions, p.priv)
	}
	if p.priv == catalog.PrivCreate {
		return s.databasePrivAllowed(ctx, txn, db, catalog.PrivCreate)
	}
	if s.actor() == catalog.RootRole {
		return true, nil
	}
	set, err := s.roleSet(ctx, txn)
	if err != nil {
		return false, err
	}
	return s.schemaUsageAllowed(ctx, txn, db.ID, set)
}

// evalPgHasRole is pg_has_role([user,] role, privilege): MEMBER (a
// member, directly or not, INHERIT or no), USAGE (holds the role's
// privileges: membership through INHERIT), SET (may SET ROLE to it),
// each WITH ADMIN OPTION (may grant it). Root and admins hold every
// role.
func (s *Session) evalPgHasRole(ctx context.Context, txn *kvclient.Txn, c *sysCall) (types.Datum, error) {
	n := len(c.args)
	user, userOK := s.actor(), true
	if n == 3 {
		var err error
		if user, userOK, err = s.roleArg(ctx, txn, c, 0, false); err != nil {
			return types.Datum{}, err
		}
	}
	role, roleOK, err := s.roleArg(ctx, txn, c, n-2, false)
	if err != nil {
		return types.Datum{}, err
	}
	specs, err := parsePrivileges(c.args[n-1].Text(), []string{"MEMBER", "USAGE", "SET"}, true)
	if err != nil {
		return types.Datum{}, err
	}
	if !userOK || !roleOK {
		return types.NewBool(false), nil
	}
	g := catalog.LazyRoleGraph(ctx, txn)
	effective, err := g.Effective(user)
	if err != nil {
		return types.Datum{}, err
	}
	if effective.IsAdmin() {
		return types.NewBool(true), nil
	}
	reachable, err := g.Reachable(user)
	if err != nil {
		return types.Datum{}, err
	}
	return anyPriv(specs, func(p privSpec) (bool, error) {
		if p.option {
			// The admin option on role, held directly or through a role
			// user belongs to.
			for name := range reachable {
				d, err := g.Role(name)
				if err != nil {
					return false, err
				}
				if d == nil {
					continue
				}
				if member, admin := d.IsMemberOf(role); member && admin {
					return true, nil
				}
			}
			return false, nil
		}
		if p.priv == "USAGE" {
			return effective.Has(role), nil
		}
		return reachable.Has(role), nil
	})
}

// typeNameKnown reports whether a type name (as a cast writes it, with
// an optional pg_catalog or public qualifier) names a type: a built-in
// one or one of the database's enums.
func (s *Session) typeNameKnown(ctx context.Context, txn *kvclient.Txn, name string) (bool, error) {
	n := strings.ToLower(strings.TrimSpace(name))
	n = strings.TrimPrefix(strings.TrimPrefix(n, vtable.PgCatalog+"."), catalog.PublicSchema+".")
	if builtins.KnownCastType(n) {
		return true, nil
	}
	dbID, err := s.currentDatabaseID(ctx, txn)
	if err != nil {
		return false, err
	}
	td, err := catalog.LookupType(ctx, txn, dbID, n)
	if err != nil {
		return false, nil
	}
	return td != nil, nil
}

// typeOIDKnown reports whether pg_type lists the OID.
func (s *Session) typeOIDKnown(ctx context.Context, txn *kvclient.Txn, oid int64) (bool, error) {
	if vtable.BuiltinTypeOID(oid) {
		return true, nil
	}
	dbID, err := s.currentDatabaseID(ctx, txn)
	if err != nil {
		return false, err
	}
	tds, err := catalog.ListTypes(ctx, txn, dbID)
	if err != nil {
		return false, err
	}
	for _, t := range tds {
		if catalog.EnumOID(t.ID) == oid {
			return true, nil
		}
	}
	return false, nil
}
