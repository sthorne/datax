package vtable

import (
	"context"
	"fmt"
	"sort"
	"strconv"
	"strings"

	"github.com/sthorne/datax/pkg/sql/catalog"
	"github.com/sthorne/datax/pkg/sql/types"
)

// information_schema, as PostgreSQL 16 defines it: every view with
// PostgreSQL's columns in PostgreSQL's order (isSpecs), its rows made
// from datax's catalog, and each view's own rule for which objects the
// reader sees — the rule of the view's definition in PostgreSQL's
// information_schema.sql, written here in terms of pgHasRole (the
// reader holds the privileges of a role: pg_has_role(role, 'USAGE')) and
// the reader's grants. The views for what datax does not have —
// routines, triggers, domains, composite types, foreign data — exist
// and are empty, as they are in a PostgreSQL database that has none
// (datax's built-in functions are not listed as routines).

// isSpec is one view's parsed column list.
type isSpec struct {
	cols []catalog.Column
	at   map[string]int
}

func parseISSpec(view string) *isSpec {
	text, ok := isSpecs[view]
	if !ok {
		panic("vtable: no information_schema spec for " + view)
	}
	sp := &isSpec{at: map[string]int{}}
	for _, f := range strings.Fields(text) {
		name, kind, _ := strings.Cut(f, ":")
		c := catalog.Column{Name: name, Type: types.String}
		switch kind {
		case "i":
			c.Type, c.Width = types.Int, 4
		case "y":
			c.MaxLen = 3
		case "t":
			c.Type, c.TimePrecision = types.Timestamp, 3 // timestamptz(2)
		}
		sp.at[name] = len(sp.cols)
		sp.cols = append(sp.cols, c)
	}
	return sp
}

// isRows collects one view's rows.
type isRows struct {
	sp   *isSpec
	env  *Env
	rows []Row
}

// add appends a row given as name, value pairs; a column not named is
// NULL. A value is a types.Datum, a string (text), an int or int64, or
// nil (NULL).
func (b *isRows) add(kv ...any) {
	r := make(Row, len(b.sp.cols))
	for i := range r {
		r[i] = null()
	}
	for i := 0; i+1 < len(kv); i += 2 {
		name := kv[i].(string)
		j, ok := b.sp.at[name]
		if !ok {
			panic("vtable: information_schema has no column " + name)
		}
		switch v := kv[i+1].(type) {
		case types.Datum:
			r[j] = v
		case string:
			r[j] = str(v)
		case int:
			r[j] = i64(int64(v))
		case int64:
			r[j] = i64(v)
		case nil:
		default:
			panic(fmt.Sprintf("vtable: information_schema value %T", v))
		}
	}
	b.rows = append(b.rows, r)
}

// isView registers an information_schema view; nil rows is a view that
// is always empty.
func isView(name string, rows func(ctx context.Context, env *Env, b *isRows) error) {
	sp := parseISSpec(name)
	if rows == nil {
		is(name, sp.cols, empty)
		return
	}
	is(name, sp.cols, func(ctx context.Context, env *Env) ([]Row, error) {
		b := &isRows{sp: sp, env: env}
		if err := rows(ctx, env, b); err != nil {
			return nil, err
		}
		return b.rows, nil
	})
}

// Privileges, as the views ask about them.

// pgHasRole is pg_has_role(role, 'USAGE') for the reader: it holds the
// role's privileges (it is the role, inherits it, or is a superuser).
// PUBLIC is no role of pg_authid's.
func (env *Env) pgHasRole(role string) bool {
	if env.RoleSet == nil || env.IsAdmin {
		return true
	}
	return role != catalog.PublicRole && env.RoleSet.Has(role)
}

// roleHasRole is pg_has_role(member, role, 'USAGE'): member holds
// role's privileges.
func (env *Env) roleHasRole(member, role string) bool {
	if member == role {
		return true
	}
	if member == catalog.PublicRole {
		return false
	}
	if env.effective == nil {
		env.effective = map[string]catalog.RoleSet{}
	}
	set, ok := env.effective[member]
	if !ok {
		if env.graph == nil {
			env.graph = catalog.NewRoleGraph(env.Roles)
		}
		set, _ = env.graph.Effective(member)
		env.effective[member] = set
	}
	return set.IsAdmin() || set.Has(role)
}

// tablePriv is has_table_privilege(t, privs) for the reader beyond
// ownership: a grant of one of privs to one of its roles (PUBLIC
// included), or read_all / write_all for the privileges they carry.
func (env *Env) tablePriv(t *catalog.TableDescriptor, privs ...string) bool {
	if env.RoleSet == nil || env.IsAdmin {
		return true
	}
	for _, p := range privs {
		if catalog.HasPrivilege(t.Privileges, env.RoleSet, p) {
			return true
		}
		switch p {
		case "SELECT":
			if env.RoleSet.Has(catalog.ReadAllRole) {
				return true
			}
		case "INSERT", "UPDATE", "DELETE", "TRUNCATE":
			if env.RoleSet.Has(catalog.WriteAllRole) {
				return true
			}
		}
	}
	return false
}

// ownsTable is pg_has_role(relowner, 'USAGE').
func (env *Env) ownsTable(t *catalog.TableDescriptor) bool {
	return env.pgHasRole(catalog.OwnerOf(t.Owner))
}

// columnsVisible is the columns view's rule: the owner, or a column
// privilege (SELECT, INSERT, UPDATE, REFERENCES; datax grants them on
// the table).
func (env *Env) columnsVisible(t *catalog.TableDescriptor) bool {
	return env.ownsTable(t) || env.tablePriv(t, "SELECT", "INSERT", "UPDATE", "REFERENCES")
}

// constraintsVisible is table_constraints' and referential_constraints'
// rule: the owner, or a privilege other than SELECT ("SELECT privilege
// omitted, per SQL standard").
func (env *Env) constraintsVisible(t *catalog.TableDescriptor) bool {
	return env.ownsTable(t) || env.tablePriv(t, "INSERT", "UPDATE", "DELETE", "TRUNCATE", "REFERENCES", "TRIGGER")
}

// aclEntry is one row of aclexplode(): a privilege the grantor gave the
// grantee ("public" for PUBLIC).
type aclEntry struct {
	grantor, grantee, priv string
	grantable              bool
}

// tablePrivOrder is the order aclexplode lists a relation's privileges
// in (their ACL bits).
var tablePrivOrder = []string{"INSERT", "SELECT", "UPDATE", "DELETE", "TRUNCATE", "REFERENCES", "TRIGGER"}

// sequencePrivOrder likewise for a sequence's.
var sequencePrivOrder = []string{"SELECT", "UPDATE", "USAGE"}

// explodeACL is aclexplode(coalesce(acl, acldefault(...))) for an object
// with an owner and grants, as ObjectACL renders its ACL: the owner holds
// ownerPrivs (what its default item carries) and each grantee what it
// was granted, every grant made by the owner. order lists the
// privileges in ACL-bit order.
func explodeACL(owner string, ownerPrivs []string, privs, grantOpts map[string][]string, order []string) []aclEntry {
	owner = catalog.OwnerOf(owner)
	var out []aclEntry
	for _, p := range ownerPrivs {
		out = append(out, aclEntry{grantor: owner, grantee: owner, priv: p})
	}
	grantees := make([]string, 0, len(privs))
	for g := range privs {
		if g != owner {
			grantees = append(grantees, g)
		}
	}
	sort.Strings(grantees)
	for _, g := range grantees {
		have, opt := map[string]bool{}, map[string]bool{}
		for _, p := range privs[g] {
			have[strings.ToUpper(p)] = true
		}
		for _, p := range grantOpts[g] {
			opt[strings.ToUpper(p)] = true
		}
		for _, p := range order {
			if have[p] {
				out = append(out, aclEntry{grantor: owner, grantee: g, priv: p, grantable: opt[p]})
			}
		}
	}
	return out
}

// tableACL is a table's or view's ACL exploded: the owner's default
// item (arwdDxt) and every grant.
func tableACL(t *catalog.TableDescriptor) []aclEntry {
	return explodeACL(t.Owner, tablePrivOrder, t.Privileges, t.GrantOptions, tablePrivOrder)
}

// sequenceACL is a sequence's ACL exploded as the views explode it:
// coalesce(relacl, acldefault('r', relowner)) — with no grant, the
// relation default (arwdDxt, no USAGE); with grants, the sequence's own
// ACL, whose owner item is rwU.
func sequenceACL(sq *catalog.SequenceDescriptor) []aclEntry {
	if len(sq.Privileges) == 0 {
		return explodeACL(sq.Owner, tablePrivOrder, nil, nil, sequencePrivOrder)
	}
	return explodeACL(sq.Owner, sequencePrivOrder, sq.Privileges, sq.GrantOptions, sequencePrivOrder)
}

// grantName is a grantee as the views name it: PUBLIC in capitals.
func grantName(role string) string {
	if role == catalog.PublicRole {
		return "PUBLIC"
	}
	return role
}

// privilegeListed is the *_privileges views' rule for a grant: the
// reader holds the grantor's or the grantee's privileges, or the grant
// is to PUBLIC.
func (env *Env) privilegeListed(e aclEntry) bool {
	return env.pgHasRole(e.grantor) || env.pgHasRole(e.grantee) || e.grantee == catalog.PublicRole
}

// grantListed is the role_*_grants views' rule: the grantor or the
// grantee is one of enabled_roles.
func (env *Env) grantListed(e aclEntry) bool {
	return env.pgHasRole(e.grantor) || env.pgHasRole(e.grantee)
}

// isGrantable is is_grantable: the grantee holds the owner's privileges
// (an owner may always grant), or the grant carries the grant option.
func (env *Env) isGrantable(e aclEntry, owner string) string {
	return yesNo(e.grantable || env.roleHasRole(e.grantee, catalog.OwnerOf(owner)))
}

// tableSchema is the schema a table is in: pg_catalog for the cluster's
// own tables (datax_audit, datax_metrics, ...), as pg_class has them;
// public for every other.
func tableSchema(t *catalog.TableDescriptor) string {
	if catalog.IsSystemTableID(t.ID) {
		return PgCatalog
	}
	return catalog.PublicSchema
}

// currentDatabase is the session's database descriptor (nil when it was
// dropped under the session).
func (env *Env) currentDatabase() *catalog.DatabaseDescriptor {
	for _, d := range env.Databases {
		if d.Name == env.Database {
			return d
		}
	}
	return nil
}

// Column types, as the views describe them.

// columnTypeInfo is what columns, attributes and element_types say about
// a column's type.
type columnTypeInfo struct {
	dataType, udtSchema, udtName                 string
	charMax, charOctet, numPrec, numRadix, scale types.Datum
	datetimePrec                                 types.Datum
}

func describeColumnType(c *catalog.Column) columnTypeInfo {
	info := columnTypeInfo{udtSchema: PgCatalog, udtName: ColumnTypeName(c),
		charMax: null(), charOctet: null(), numPrec: null(), numRadix: null(), scale: null(), datetimePrec: null()}
	switch {
	case c.Type.IsArray():
		info.dataType = "ARRAY"
		return info
	case c.Type == types.Enum:
		info.dataType, info.udtSchema = "USER-DEFINED", catalog.PublicSchema
		return info
	}
	oid := ColumnTypeOID(c)
	info.dataType = FormatTypeOID(oid)
	switch oid {
	case 25, 1043, 1042:
		if c.MaxLen > 0 {
			info.charMax, info.charOctet = i64(int64(c.MaxLen)), i64(int64(c.MaxLen)*4)
		} else {
			info.charOctet = i64(1073741824)
		}
	case 21, 23, 20:
		info.numPrec, info.numRadix, info.scale = i64(int64(c.IntWidth())*8), i64(2), i64(0)
	case 701:
		info.numPrec, info.numRadix = i64(53), i64(2)
	case 1700:
		info.numRadix = i64(10)
		if c.Precision > 0 {
			info.numPrec, info.scale = i64(int64(c.Precision)), i64(int64(c.Scale))
		}
	case 1082:
		info.datetimePrec = i64(0)
	case 1083, 1114, 1184:
		info.datetimePrec = i64(6)
		if p, ok := c.FracDigits(); ok {
			info.datetimePrec = i64(int64(p))
		}
	case 1186:
		info.datetimePrec = i64(6)
	}
	return info
}

// visibleColumns lists a table's columns with their attnums.
func visibleColumns(t *catalog.TableDescriptor) ([]*catalog.Column, []int) {
	var cols []*catalog.Column
	var nums []int
	n := 0
	for i := range t.Columns {
		if c := &t.Columns[i]; !c.Hidden {
			n++
			cols = append(cols, c)
			nums = append(nums, n)
		}
	}
	return cols, nums
}

// Constraints, as pg_constraint lists them.

// isConstraint is one of a table's constraints.
type isConstraint struct {
	name, contype string // p, u, c, f
	cols          []catalog.ColumnID
	idx           *catalog.IndexDescriptor // p, u
	con           *catalog.Constraint      // c, f
}

// tableConstraints lists a table's constraints as pg_constraint does:
// the primary key, the unique indexes that are constraints, the CHECK
// constraints and the foreign keys.
func tableConstraints(t *catalog.TableDescriptor) []isConstraint {
	var out []isConstraint
	if hasVisiblePK(t) {
		out = append(out, isConstraint{name: t.Name + "_pkey", contype: "p", cols: visiblePK(t)})
	}
	for i := range t.Indexes {
		idx := &t.Indexes[i]
		if !IsConstraintIndex(idx) {
			continue
		}
		kind := "u"
		if idx.Primary {
			kind = "p"
		}
		out = append(out, isConstraint{name: idx.Name, contype: kind, cols: idx.ColumnIDs, idx: idx})
	}
	for i := range t.Constraints {
		c := &t.Constraints[i]
		switch c.Kind {
		case catalog.ConstraintCheck:
			out = append(out, isConstraint{name: c.Name, contype: "c", cols: c.Columns, con: c})
		case catalog.ConstraintForeign:
			out = append(out, isConstraint{name: c.Name, contype: "f", cols: c.Columns, con: c})
		}
	}
	return out
}

// notNullName is the name PostgreSQL 14–17's information_schema gives a
// NOT NULL column's constraint: namespace OID, table OID, attnum.
func notNullName(t *catalog.TableDescriptor, attnum int) string {
	return fmt.Sprintf("%d_%d_%d_not_null", OIDPublic, TableOID(t), attnum)
}

// sequenceOf indexes the current database's sequences by ID.
func (env *Env) sequenceOf() map[uint64]*catalog.SequenceDescriptor {
	m := map[uint64]*catalog.SequenceDescriptor{}
	for _, sq := range env.currentSequences() {
		m[sq.ID] = sq
	}
	return m
}

// sequenceVisible is the sequences view's rule: the owner, a privilege
// on the sequence, or — a sequence a column owns takes its table's
// grants in datax — a privilege on the owning table.
func (env *Env) sequenceVisible(sq *catalog.SequenceDescriptor) bool {
	if env.pgHasRole(catalog.OwnerOf(sq.Owner)) {
		return true
	}
	for _, p := range []string{"SELECT", "UPDATE", catalog.PrivUsage} {
		if catalog.HasPrivilege(sq.Privileges, env.RoleSet, p) {
			return true
		}
	}
	if env.RoleSet.Has(catalog.ReadAllRole) || env.RoleSet.Has(catalog.WriteAllRole) {
		return true
	}
	if sq.OwnerTable != 0 {
		if t := env.tableByID(sq.OwnerTable); t != nil {
			return env.ownsTable(t) || env.tablePriv(t, "SELECT", "INSERT", "UPDATE")
		}
	}
	return false
}

// isIdentitySequence reports whether sq is an identity column's (which
// information_schema.sequences leaves out: the column's identity_*
// describe it).
func (env *Env) isIdentitySequence(sq *catalog.SequenceDescriptor) bool {
	if sq.OwnerTable == 0 {
		return false
	}
	t := env.tableByID(sq.OwnerTable)
	if t == nil {
		return false
	}
	c, ok := t.ColByID(sq.OwnerColumn)
	return ok && c.Identity != ""
}

// viewColumnsUsed lists the columns of ref that a view's query names:
// every column when it selects *, else each column whose name appears
// in the query as an identifier. datax keeps a view's query as text and
// its dependencies by table, not by column, so this reads the text.
func viewColumnsUsed(query string, ref *catalog.TableDescriptor) []string {
	words, star := queryIdentifiers(query)
	var out []string
	for i := range ref.Columns {
		c := &ref.Columns[i]
		if c.Hidden {
			continue
		}
		if star || words[c.Name] {
			out = append(out, c.Name)
		}
	}
	return out
}

// queryIdentifiers scans SQL text for its identifiers (lowercased when
// unquoted) and whether it selects * (a * not inside count(*)).
func queryIdentifiers(q string) (map[string]bool, bool) {
	words := map[string]bool{}
	star := false
	for i := 0; i < len(q); {
		ch := q[i]
		switch {
		case ch == '\'':
			i++
			for i < len(q) && q[i] != '\'' {
				i++
			}
			i++
		case ch == '"':
			j := i + 1
			for j < len(q) && q[j] != '"' {
				j++
			}
			words[q[i+1:min(j, len(q))]] = true
			i = j + 1
		case ch == '_' || ch >= 'a' && ch <= 'z' || ch >= 'A' && ch <= 'Z':
			j := i
			for j < len(q) && (q[j] == '_' || q[j] >= 'a' && q[j] <= 'z' || q[j] >= 'A' && q[j] <= 'Z' || q[j] >= '0' && q[j] <= '9' || q[j] == '$') {
				j++
			}
			words[strings.ToLower(q[i:j])] = true
			i = j
		case ch == '*':
			prev := strings.TrimRight(q[:i], " \t\n")
			if !strings.HasSuffix(prev, "(") {
				star = true
			}
			i++
		default:
			i++
		}
	}
	return words, star
}

func init() {
	// Roles.
	isView("enabled_roles", func(_ context.Context, env *Env, b *isRows) error {
		for _, r := range env.Roles {
			if r.Name != catalog.PublicRole && env.pgHasRole(r.Name) {
				b.add("role_name", r.Name)
			}
		}
		return nil
	})
	applicable := func(grantableOnly bool) func(context.Context, *Env, *isRows) error {
		return func(_ context.Context, env *Env, b *isRows) error {
			// pg_auth_members: root's implicit admin membership, then each
			// stored one.
			add := func(member, role string, admin bool) {
				if grantableOnly && !admin {
					return
				}
				if env.pgHasRole(member) {
					b.add("grantee", member, "role_name", role, "is_grantable", yesNo(admin))
				}
			}
			add(catalog.RootRole, catalog.AdminRole, true)
			for _, r := range env.Roles {
				for _, m := range r.MemberOf {
					add(r.Name, m.Role, m.Admin)
				}
			}
			return nil
		}
	}
	isView("applicable_roles", applicable(false))
	isView("administrable_role_authorizations", applicable(true))

	// The catalog and its schemas.
	isView("information_schema_catalog_name", func(_ context.Context, env *Env, b *isRows) error {
		b.add("catalog_name", env.Database)
		return nil
	})
	isView("schemata", func(_ context.Context, env *Env, b *isRows) error {
		// pg_catalog and information_schema belong to the bootstrap
		// superuser and grant USAGE to PUBLIC; public belongs to the
		// database's owner and grants it unless REVOKE took it away.
		db := env.currentDatabase()
		owner := catalog.RootRole
		usable := true
		if db != nil {
			owner = catalog.OwnerOf(db.Owner)
			usable = !db.UsageRestricted || env.RoleSet == nil ||
				catalog.HasPrivilege(db.SchemaPrivileges, env.RoleSet, catalog.PrivUsage) ||
				catalog.HasPrivilege(db.SchemaPrivileges, env.RoleSet, catalog.PrivCreate)
		}
		b.add("catalog_name", env.Database, "schema_name", InformationSchema, "schema_owner", catalog.RootRole)
		b.add("catalog_name", env.Database, "schema_name", PgCatalog, "schema_owner", catalog.RootRole)
		if usable || env.pgHasRole(owner) {
			b.add("catalog_name", env.Database, "schema_name", catalog.PublicSchema, "schema_owner", owner)
		}
		return nil
	})
	isView("character_sets", func(_ context.Context, env *Env, b *isRows) error {
		b.add("character_set_name", "UTF8", "character_repertoire", "UCS", "form_of_use", "UTF8",
			"default_collate_catalog", env.Database, "default_collate_schema", PgCatalog, "default_collate_name", "C")
		return nil
	})
	collations := []string{"default", "C", "POSIX"} // pg_collation's rows
	isView("collations", func(_ context.Context, env *Env, b *isRows) error {
		for _, c := range collations {
			b.add("collation_catalog", env.Database, "collation_schema", PgCatalog, "collation_name", c, "pad_attribute", "NO PAD")
		}
		return nil
	})
	isView("collation_character_set_applicability", func(_ context.Context, env *Env, b *isRows) error {
		for _, c := range collations {
			b.add("collation_catalog", env.Database, "collation_schema", PgCatalog, "collation_name", c, "character_set_name", "UTF8")
		}
		return nil
	})

	// Tables, views and columns.
	isView("tables", func(_ context.Context, env *Env, b *isRows) error {
		for _, t := range env.currentTables() {
			kind, insertable := "BASE TABLE", "YES"
			if t.IsView() {
				kind, insertable = "VIEW", "NO"
			}
			if t.Temp != nil {
				kind = "LOCAL TEMPORARY"
			}
			b.add("table_catalog", env.Database, "table_schema", tableSchema(t), "table_name", t.Name, "table_type", kind,
				"is_insertable_into", insertable, "is_typed", "NO")
		}
		// The catalogs themselves are views of pg_catalog and
		// information_schema that every role may read.
		for _, name := range Names() {
			sch, bare, _ := strings.Cut(name, ".")
			b.add("table_catalog", env.Database, "table_schema", sch, "table_name", bare, "table_type", "VIEW",
				"is_insertable_into", "NO", "is_typed", "NO")
		}
		return nil
	})
	isView("views", func(_ context.Context, env *Env, b *isRows) error {
		for _, t := range env.currentTables() {
			if !t.IsView() {
				continue
			}
			def := null()
			if env.ownsTable(t) {
				def = str(ViewDef(t.ViewQuery))
			}
			b.add("table_catalog", env.Database, "table_schema", tableSchema(t), "table_name", t.Name, "view_definition", def,
				"check_option", "NONE", "is_updatable", "NO", "is_insertable_into", "NO",
				"is_trigger_updatable", "NO", "is_trigger_deletable", "NO", "is_trigger_insertable_into", "NO")
		}
		return nil
	})
	isView("columns", func(_ context.Context, env *Env, b *isRows) error {
		seqs := env.sequenceOf()
		for _, t := range env.currentTables() {
			if !env.columnsVisible(t) {
				continue
			}
			cols, nums := visibleColumns(t)
			for i, c := range cols {
				info := describeColumnType(c)
				def := null()
				if text := ColumnDefault(c); text != "" && c.Identity == "" {
					def = str(text)
				}
				identity, idStart, idInc, idMax, idMin, idCycle := null(), null(), null(), null(), null(), "NO"
				switch c.Identity {
				case "always":
					identity = str("ALWAYS")
				case "by default":
					identity = str("BY DEFAULT")
				}
				if sq := seqs[c.SequenceID]; c.Identity != "" && sq != nil {
					idStart, idInc = str(strconv.FormatInt(sq.Start, 10)), str(strconv.FormatInt(sq.Increment, 10))
					idMax, idMin = str(strconv.FormatInt(sq.MaxValue, 10)), str(strconv.FormatInt(sq.MinValue, 10))
					idCycle = yesNo(sq.Cycle)
				}
				updatable := "YES"
				if t.IsView() {
					updatable = "NO"
				}
				b.add("table_catalog", env.Database, "table_schema", tableSchema(t), "table_name", t.Name, "column_name", c.Name,
					"ordinal_position", nums[i], "column_default", def, "is_nullable", yesNo(!c.NotNull), "data_type", info.dataType,
					"character_maximum_length", info.charMax, "character_octet_length", info.charOctet,
					"numeric_precision", info.numPrec, "numeric_precision_radix", info.numRadix, "numeric_scale", info.scale,
					"datetime_precision", info.datetimePrec,
					"udt_catalog", env.Database, "udt_schema", info.udtSchema, "udt_name", info.udtName,
					"dtd_identifier", strconv.Itoa(nums[i]), "is_self_referencing", "NO",
					"is_identity", yesNo(c.Identity != ""), "identity_generation", identity,
					"identity_start", idStart, "identity_increment", idInc, "identity_maximum", idMax, "identity_minimum", idMin, "identity_cycle", idCycle,
					"is_generated", "NEVER", "is_updatable", updatable)
			}
		}
		return nil
	})
	isView("column_udt_usage", func(_ context.Context, env *Env, b *isRows) error {
		// The columns of a type whose owner the reader holds the
		// privileges of: an enum's owner; the built-in types' is the
		// bootstrap superuser.
		owners := map[uint64]string{}
		for _, ty := range env.currentTypes() {
			owners[ty.ID] = catalog.OwnerOf(ty.Owner)
		}
		for _, t := range env.currentTables() {
			cols, _ := visibleColumns(t)
			for _, c := range cols {
				owner := catalog.RootRole
				if c.Type == types.Enum {
					owner = owners[c.EnumType]
				}
				if !env.pgHasRole(owner) {
					continue
				}
				info := describeColumnType(c)
				b.add("udt_catalog", env.Database, "udt_schema", info.udtSchema, "udt_name", info.udtName,
					"table_catalog", env.Database, "table_schema", tableSchema(t), "table_name", t.Name, "column_name", c.Name)
			}
		}
		return nil
	})
	isView("data_type_privileges", func(_ context.Context, env *Env, b *isRows) error {
		// The union of the views that describe a data type: here the
		// columns' (attributes, domains, parameters and routines have no
		// rows).
		for _, t := range env.currentTables() {
			if !env.columnsVisible(t) {
				continue
			}
			_, nums := visibleColumns(t)
			for _, n := range nums {
				b.add("object_catalog", env.Database, "object_schema", tableSchema(t), "object_name", t.Name, "object_type", "TABLE",
					"dtd_identifier", strconv.Itoa(n))
			}
		}
		return nil
	})
	isView("element_types", func(_ context.Context, env *Env, b *isRows) error {
		// The element type of every array column data_type_privileges
		// lists.
		for _, t := range env.currentTables() {
			if !env.columnsVisible(t) {
				continue
			}
			cols, nums := visibleColumns(t)
			for i, c := range cols {
				if !c.Type.IsArray() {
					continue
				}
				elem := *c
				elem.Type = c.Type.Elem()
				oid := ColumnTypeOID(&elem)
				b.add("object_catalog", env.Database, "object_schema", tableSchema(t), "object_name", t.Name, "object_type", "TABLE",
					"collection_type_identifier", strconv.Itoa(nums[i]), "data_type", FormatTypeOID(oid),
					"udt_catalog", env.Database, "udt_schema", PgCatalog, "udt_name", typeNameOID(oid), "dtd_identifier", "a"+strconv.Itoa(nums[i]))
			}
		}
		return nil
	})

	// Constraints.
	isView("table_constraints", func(_ context.Context, env *Env, b *isRows) error {
		kinds := map[string]string{"c": "CHECK", "f": "FOREIGN KEY", "p": "PRIMARY KEY", "u": "UNIQUE"}
		for _, t := range env.currentTables() {
			if t.IsView() || !env.constraintsVisible(t) {
				continue
			}
			for _, c := range tableConstraints(t) {
				nullsDistinct := null()
				if c.contype == "u" {
					nullsDistinct = str(yesNo(c.idx == nil || !c.idx.NullsNotDistinct))
				}
				b.add("constraint_catalog", env.Database, "constraint_schema", tableSchema(t), "constraint_name", c.name,
					"table_catalog", env.Database, "table_schema", tableSchema(t), "table_name", t.Name, "constraint_type", kinds[c.contype],
					"is_deferrable", "NO", "initially_deferred", "NO", "enforced", "YES", "nulls_distinct", nullsDistinct)
			}
			cols, nums := visibleColumns(t)
			for i, c := range cols {
				if c.NotNull {
					b.add("constraint_catalog", env.Database, "constraint_schema", tableSchema(t), "constraint_name", notNullName(t, nums[i]),
						"table_catalog", env.Database, "table_schema", tableSchema(t), "table_name", t.Name, "constraint_type", "CHECK",
						"is_deferrable", "NO", "initially_deferred", "NO", "enforced", "YES")
				}
			}
		}
		return nil
	})
	isView("check_constraints", func(_ context.Context, env *Env, b *isRows) error {
		// The owner's alone; check_clause is pg_get_constraintdef without
		// its leading "CHECK ".
		for _, t := range env.currentTables() {
			if t.IsView() || !env.ownsTable(t) {
				continue
			}
			for _, c := range tableConstraints(t) {
				if c.contype == "c" {
					b.add("constraint_catalog", env.Database, "constraint_schema", tableSchema(t), "constraint_name", c.name,
						"check_clause", strings.TrimPrefix(ConstraintDef(t, c.con, nil), "CHECK "))
				}
			}
			cols, nums := visibleColumns(t)
			for i, c := range cols {
				if c.NotNull {
					b.add("constraint_catalog", env.Database, "constraint_schema", tableSchema(t), "constraint_name", notNullName(t, nums[i]),
						"check_clause", c.Name+" IS NOT NULL")
				}
			}
		}
		return nil
	})
	isView("referential_constraints", func(_ context.Context, env *Env, b *isRows) error {
		rule := func(a string) string {
			switch a {
			case catalog.FKCascade:
				return "CASCADE"
			case catalog.FKSetNull:
				return "SET NULL"
			}
			return "NO ACTION"
		}
		for _, t := range env.currentTables() {
			if !env.constraintsVisible(t) {
				continue
			}
			for _, c := range tableConstraints(t) {
				if c.contype != "f" {
					continue
				}
				ref := t
				if c.con.RefTable != t.ID {
					ref = env.tableByID(c.con.RefTable)
				}
				ucat, uschema, uname := null(), null(), null()
				if ref != nil {
					if n := uniqueConstraintNameFor(ref, c.con.RefColumns); n != "" {
						ucat, uschema, uname = str(env.Database), str(tableSchema(ref)), str(n)
					}
				}
				b.add("constraint_catalog", env.Database, "constraint_schema", tableSchema(t), "constraint_name", c.name,
					"unique_constraint_catalog", ucat, "unique_constraint_schema", uschema, "unique_constraint_name", uname,
					"match_option", "NONE", "update_rule", rule(c.con.OnUpdate), "delete_rule", rule(c.con.OnDelete))
			}
		}
		return nil
	})
	isView("key_column_usage", func(_ context.Context, env *Env, b *isRows) error {
		for _, t := range env.currentTables() {
			if t.IsView() || !env.columnsVisible(t) {
				continue
			}
			for _, c := range tableConstraints(t) {
				if c.contype == "c" {
					continue
				}
				// A foreign key's column's position in the referenced
				// unique key.
				var refPos map[catalog.ColumnID]int
				if c.contype == "f" {
					ref := t
					if c.con.RefTable != t.ID {
						ref = env.tableByID(c.con.RefTable)
					}
					if ref != nil {
						refPos = uniqueKeyPositions(ref, c.con.RefColumns)
					}
				}
				for i, id := range c.cols {
					pos := null()
					if c.contype == "f" && i < len(c.con.RefColumns) {
						if p, ok := refPos[c.con.RefColumns[i]]; ok {
							pos = i64(int64(p))
						}
					}
					b.add("constraint_catalog", env.Database, "constraint_schema", tableSchema(t), "constraint_name", c.name,
						"table_catalog", env.Database, "table_schema", tableSchema(t), "table_name", t.Name, "column_name", columnName(t, id),
						"ordinal_position", i+1, "position_in_unique_constraint", pos)
				}
			}
		}
		return nil
	})
	isView("constraint_column_usage", func(_ context.Context, env *Env, b *isRows) error {
		// The columns a constraint uses, listed under the table that
		// holds them (a foreign key's: the referenced ones), for the
		// owner of that table.
		for _, t := range env.currentTables() {
			for _, c := range tableConstraints(t) {
				holder, ids := t, c.cols
				if c.contype == "f" {
					if c.con.RefTable != t.ID {
						holder = env.tableByID(c.con.RefTable)
					}
					ids = c.con.RefColumns
				}
				if holder == nil || !env.ownsTable(holder) {
					continue
				}
				for _, id := range ids {
					b.add("table_catalog", env.Database, "table_schema", tableSchema(holder), "table_name", holder.Name, "column_name", columnName(holder, id),
						"constraint_catalog", env.Database, "constraint_schema", tableSchema(t), "constraint_name", c.name)
				}
			}
		}
		return nil
	})
	isView("constraint_table_usage", func(_ context.Context, env *Env, b *isRows) error {
		for _, t := range env.currentTables() {
			for _, c := range tableConstraints(t) {
				holder := t
				switch c.contype {
				case "c":
					continue
				case "f":
					if c.con.RefTable != t.ID {
						holder = env.tableByID(c.con.RefTable)
					}
				}
				if holder == nil || !env.ownsTable(holder) {
					continue
				}
				b.add("table_catalog", env.Database, "table_schema", tableSchema(holder), "table_name", holder.Name,
					"constraint_catalog", env.Database, "constraint_schema", tableSchema(t), "constraint_name", c.name)
			}
		}
		return nil
	})

	// Views' dependencies, for the owner of the table depended on.
	isView("view_table_usage", func(_ context.Context, env *Env, b *isRows) error {
		for _, v := range env.currentTables() {
			for _, ref := range viewRefs(env, v) {
				b.add("view_catalog", env.Database, "view_schema", tableSchema(v), "view_name", v.Name,
					"table_catalog", env.Database, "table_schema", tableSchema(ref), "table_name", ref.Name)
			}
		}
		return nil
	})
	isView("view_column_usage", func(_ context.Context, env *Env, b *isRows) error {
		for _, v := range env.currentTables() {
			for _, ref := range viewRefs(env, v) {
				for _, name := range viewColumnsUsed(v.ViewQuery, ref) {
					b.add("view_catalog", env.Database, "view_schema", tableSchema(v), "view_name", v.Name,
						"table_catalog", env.Database, "table_schema", tableSchema(ref), "table_name", ref.Name, "column_name", name)
				}
			}
		}
		return nil
	})

	// Sequences.
	isView("sequences", func(_ context.Context, env *Env, b *isRows) error {
		for _, sq := range env.currentSequences() {
			if env.isIdentitySequence(sq) || !env.sequenceVisible(sq) {
				continue
			}
			b.add("sequence_catalog", env.Database, "sequence_schema", catalog.PublicSchema, "sequence_name", sq.Name,
				"data_type", "bigint", "numeric_precision", 64, "numeric_precision_radix", 2, "numeric_scale", 0,
				"start_value", strconv.FormatInt(sq.Start, 10), "minimum_value", strconv.FormatInt(sq.MinValue, 10),
				"maximum_value", strconv.FormatInt(sq.MaxValue, 10), "increment", strconv.FormatInt(sq.Increment, 10),
				"cycle_option", yesNo(sq.Cycle))
		}
		return nil
	})

	// Privileges.
	tablePrivRows := func(roleGrants bool) func(context.Context, *Env, *isRows) error {
		return func(_ context.Context, env *Env, b *isRows) error {
			for _, t := range env.currentTables() {
				for _, e := range tableACL(t) {
					if roleGrants && !env.grantListed(e) || !roleGrants && !env.privilegeListed(e) {
						continue
					}
					b.add("grantor", e.grantor, "grantee", grantName(e.grantee), "table_catalog", env.Database, "table_schema", tableSchema(t),
						"table_name", t.Name, "privilege_type", e.priv, "is_grantable", env.isGrantable(e, t.Owner), "with_hierarchy", yesNo(e.priv == "SELECT"))
				}
			}
			return nil
		}
	}
	isView("table_privileges", tablePrivRows(false))
	isView("role_table_grants", tablePrivRows(true))
	columnPrivRows := func(roleGrants bool) func(context.Context, *Env, *isRows) error {
		return func(_ context.Context, env *Env, b *isRows) error {
			for _, t := range env.currentTables() {
				cols, _ := visibleColumns(t)
				for _, e := range tableACL(t) {
					switch e.priv {
					case "INSERT", "SELECT", "UPDATE", "REFERENCES":
					default:
						continue
					}
					if roleGrants && !env.grantListed(e) || !roleGrants && !env.privilegeListed(e) {
						continue
					}
					for _, c := range cols {
						b.add("grantor", e.grantor, "grantee", grantName(e.grantee), "table_catalog", env.Database, "table_schema", tableSchema(t),
							"table_name", t.Name, "column_name", c.Name, "privilege_type", e.priv, "is_grantable", env.isGrantable(e, t.Owner))
					}
				}
			}
			return nil
		}
	}
	isView("column_privileges", columnPrivRows(false))
	isView("role_column_grants", columnPrivRows(true))
	usagePrivRows := func(roleGrants bool) func(context.Context, *Env, *isRows) error {
		return func(_ context.Context, env *Env, b *isRows) error {
			if !roleGrants || env.pgHasRole(catalog.RootRole) {
				// Collations have no privileges: each is shown as usable
				// by PUBLIC, granted by its owner (the bootstrap
				// superuser).
				for _, c := range collations {
					b.add("grantor", catalog.RootRole, "grantee", "PUBLIC", "object_catalog", env.Database, "object_schema", PgCatalog,
						"object_name", c, "object_type", "COLLATION", "privilege_type", "USAGE", "is_grantable", "NO")
				}
			}
			for _, sq := range env.currentSequences() {
				for _, e := range sequenceACL(sq) {
					if e.priv != catalog.PrivUsage {
						continue
					}
					if roleGrants && !env.grantListed(e) || !roleGrants && !env.privilegeListed(e) {
						continue
					}
					b.add("grantor", e.grantor, "grantee", grantName(e.grantee), "object_catalog", env.Database, "object_schema", catalog.PublicSchema,
						"object_name", sq.Name, "object_type", "SEQUENCE", "privilege_type", "USAGE", "is_grantable", env.isGrantable(e, sq.Owner))
				}
			}
			return nil
		}
	}
	isView("usage_privileges", usagePrivRows(false))
	isView("role_usage_grants", usagePrivRows(true))

	// What datax does not have: routines (its built-in functions are not
	// listed), triggers, domains, composite types and their attributes,
	// transforms, foreign data.
	for _, name := range []string{
		"attributes", "check_constraint_routine_usage", "column_column_usage", "column_domain_usage", "column_options",
		"domain_constraints", "domain_udt_usage", "domains", "foreign_data_wrapper_options", "foreign_data_wrappers",
		"foreign_server_options", "foreign_servers", "foreign_table_options", "foreign_tables", "parameters",
		"role_routine_grants", "role_udt_grants", "routine_column_usage", "routine_privileges", "routine_routine_usage",
		"routine_sequence_usage", "routine_table_usage", "routines", "transforms", "triggered_update_columns", "triggers",
		"udt_privileges", "user_defined_types", "user_mapping_options", "user_mappings", "view_routine_usage",
	} {
		isView(name, nil)
	}
}

// viewRefs lists the tables and views a view reads whose owner the
// reader holds the privileges of.
func viewRefs(env *Env, v *catalog.TableDescriptor) []*catalog.TableDescriptor {
	if !v.IsView() {
		return nil
	}
	var out []*catalog.TableDescriptor
	seen := map[uint64]bool{}
	for _, id := range v.ViewDepends {
		if seen[id] || id == v.ID {
			continue
		}
		seen[id] = true
		if ref := env.tableByID(id); ref != nil && env.ownsTable(ref) {
			out = append(out, ref)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out
}

// uniqueKeyPositions maps each column of the primary key or unique
// constraint that holds cols to its position in that key (from 1).
func uniqueKeyPositions(t *catalog.TableDescriptor, cols []catalog.ColumnID) map[catalog.ColumnID]int {
	name := uniqueConstraintNameFor(t, cols)
	if name == "" {
		return nil
	}
	for _, c := range tableConstraints(t) {
		if c.name == name && (c.contype == "p" || c.contype == "u") {
			out := map[catalog.ColumnID]int{}
			for i, id := range c.cols {
				out[id] = i + 1
			}
			return out
		}
	}
	return nil
}
