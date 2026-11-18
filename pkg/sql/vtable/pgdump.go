package vtable

import (
	"context"
	"sort"
	"strings"

	"github.com/sthorne/datax/pkg/sql/catalog"
	"github.com/sthorne/datax/pkg/sql/types"
)

// The catalogs pg_dump reads that no other client does. pg_default_acl
// carries the database's ALTER DEFAULT PRIVILEGES rules; the rest are
// for features datax does not have and are empty.

// OIDPgDefaultACL is pg_default_acl's own OID (its rows' tableoid).
const OIDPgDefaultACL = 826

func init() {
	catalogRelOIDs["pg_default_acl"] = OIDPgDefaultACL
	catalogRelOIDs["pg_transform"] = 3576
	catalogRelOIDs["pg_publication_namespace"] = 6237

	// pg_default_acl: one row per (role, object kind) of the current
	// database's rules, all database-wide (defaclnamespace 0: datax's
	// rules name no schema). defaclacl is the whole ACL objects of that
	// kind get, the creator's own default included, as PostgreSQL keeps
	// a database-wide entry.
	pg("pg_default_acl", []catalog.Column{
		col("oid", types.Int), col("defaclrole", types.Int), col("defaclnamespace", types.Int),
		col("defaclobjtype", types.String), col("defaclacl", types.String),
	}, func(ctx context.Context, env *Env) ([]Row, error) {
		var db *catalog.DatabaseDescriptor
		for _, d := range env.Databases {
			if d.Name == env.Database {
				db = d
			}
		}
		if db == nil || len(db.DefaultPrivileges) == 0 {
			return nil, nil
		}
		type key struct{ role, object string }
		privs := map[key]map[string][]string{}
		opts := map[key]map[string][]string{}
		for _, dp := range db.DefaultPrivileges {
			k := key{catalog.OwnerOf(dp.ForRole), dp.Object}
			if privs[k] == nil {
				privs[k], opts[k] = map[string][]string{}, map[string][]string{}
			}
			privs[k][dp.Grantee] = append(privs[k][dp.Grantee], dp.Privileges...)
			if dp.GrantOption {
				opts[k][dp.Grantee] = append(opts[k][dp.Grantee], dp.Privileges...)
			}
		}
		keys := make([]key, 0, len(privs))
		for k := range privs {
			keys = append(keys, k)
		}
		sort.Slice(keys, func(i, j int) bool {
			if keys[i].role != keys[j].role {
				return keys[i].role < keys[j].role
			}
			return keys[i].object < keys[j].object
		})
		var rows []Row
		for i, k := range keys {
			objtype, letters := "r", "arwdDxt"
			if k.object == "SEQUENCES" {
				objtype, letters = "S", "rwU"
			}
			rows = append(rows, Row{i64(DatabaseOID(db)<<8 | int64(i)), i64(RoleOID(k.role)), i64(0), str(objtype),
				ObjectACL(k.role, letters, privs[k], opts[k])})
		}
		return rows, nil
	})
	pg("pg_init_privs", []catalog.Column{
		col("objoid", types.Int), col("classoid", types.Int), col("objsubid", types.Int), col("privtype", types.String), col("initprivs", types.String),
	}, empty)
	pg("pg_transform", []catalog.Column{
		col("oid", types.Int), col("trftype", types.Int), col("trflang", types.Int), col("trffromsql", types.Int), col("trftosql", types.Int),
	}, empty)
	pg("pg_publication_namespace", []catalog.Column{
		col("oid", types.Int), col("pnpubid", types.Int), col("pnnspid", types.Int),
	}, empty)
	pg("pg_shseclabel", []catalog.Column{
		col("objoid", types.Int), col("classoid", types.Int), col("provider", types.String), col("label", types.String),
	}, empty)
	pg("pg_seclabels", []catalog.Column{
		col("objoid", types.Int), col("classoid", types.Int), col("objsubid", types.Int), col("objtype", types.String),
		col("objnamespace", types.Int), col("objname", types.String), col("provider", types.String), col("label", types.String),
	}, empty)
}

// ViewDef is pg_get_viewdef's rendering of a view's query: the query
// with a closing semicolon, as PostgreSQL writes it (pg_dump strips the
// last character as that semicolon before appending WITH CHECK OPTION
// or the statement's own).
func ViewDef(query string) string {
	q := strings.TrimRight(query, " \t\n;")
	return q + ";"
}

// OIDPgAttrdef is pg_attrdef's own OID (its rows' tableoid).
const OIDPgAttrdef = 2604

// defaultAndViewDepends are the pg_depend rows pg_dump orders its
// output by beyond sequence ownership: a column default on each
// sequence its nextval() names (deptype 'n'), which with the
// sequence's ownership is the cycle pg_dump breaks by setting a SERIAL
// default after both exist; and a view on each table or view its
// query reads. PostgreSQL records the latter on the view's _RETURN
// rule; pg_dump folds that rule into the view, so a row on the view
// itself orders the same.
func defaultAndViewDepends(env *Env) []Row {
	seqByName := map[string]*catalog.SequenceDescriptor{}
	for _, sq := range env.currentSequences() {
		seqByName[sq.Name] = sq
	}
	var rows []Row
	for _, t := range env.currentTables() {
		if t.IsView() {
			for _, dep := range t.ViewDepends {
				rows = append(rows, Row{i64(OIDPgClass), i64(TableOID(t)), i64(0), i64(OIDPgClass), i64(relOID(dep)), i64(0), str("n")})
			}
			continue
		}
		n := int64(0)
		for i := range t.Columns {
			c := &t.Columns[i]
			if c.Hidden {
				continue
			}
			n++
			def := ColumnDefault(c)
			if def == "" || c.Identity != "" {
				continue
			}
			for _, name := range nextvalSequences(def) {
				if sq := seqByName[name]; sq != nil {
					rows = append(rows, Row{i64(OIDPgAttrdef), i64(AttrdefOID(t, n)), i64(0), i64(OIDPgClass), i64(int64(sq.ID)), i64(0), str("n")})
				}
			}
		}
	}
	return rows
}

// AttrdefOID is the pg_attrdef OID of the default of t's n-th visible
// column.
func AttrdefOID(t *catalog.TableDescriptor, n int64) int64 { return TableOID(t)<<8 + n }

// nextvalSequences lists the sequences nextval('name') calls in a
// default expression name, unqualified.
func nextvalSequences(expr string) []string {
	var out []string
	rest := expr
	for {
		i := strings.Index(strings.ToLower(rest), "nextval(")
		if i < 0 {
			return out
		}
		rest = rest[i+len("nextval("):]
		arg := strings.TrimLeft(rest, " ")
		if !strings.HasPrefix(arg, "'") {
			continue
		}
		end := strings.IndexByte(arg[1:], '\'')
		if end < 0 {
			return out
		}
		name := strings.TrimPrefix(arg[1:1+end], "public.")
		if len(name) > 1 && name[0] == '"' && name[len(name)-1] == '"' {
			// A quoted name (nextval('"User_id_seq"')): "" inside is one ".
			name = strings.ReplaceAll(name[1:len(name)-1], `""`, `"`)
		}
		out = append(out, name)
	}
}

// PublicSchemaComment is the public schema's comment, initdb's.
const PublicSchemaComment = "standard public schema"

// publicNamespaceRow is pg_namespace's row for public. Its owner is the
// current database's (datax keeps the schema's privileges on the
// database, and its owner holds them all), and its ACL is PostgreSQL
// 15's default — the owner's USAGE and CREATE, PUBLIC's USAGE — with
// the database's GRANTs and REVOKEs on the schema applied.
func publicNamespaceRow(env *Env) Row {
	owner := catalog.RootRole
	var db *catalog.DatabaseDescriptor
	for _, d := range env.Databases {
		if d.Name == env.Database {
			db = d
		}
	}
	if db != nil {
		owner = catalog.OwnerOf(db.Owner)
	}
	o := aclName(owner)
	items := []string{o + "=UC/" + o}
	if db == nil || !db.UsageRestricted {
		items = append(items, "=U/"+o)
	}
	if db != nil {
		grantees := make([]string, 0, len(db.SchemaPrivileges))
		for g := range db.SchemaPrivileges {
			grantees = append(grantees, g)
		}
		sort.Strings(grantees)
		for _, g := range grantees {
			if g == owner {
				continue
			}
			privs := db.SchemaPrivileges[g]
			if g == catalog.PublicRole && !db.UsageRestricted {
				privs = append([]string{catalog.PrivUsage}, privs...)
			}
			acl := ObjectACL(owner, "", map[string][]string{g: privs}, map[string][]string{g: db.SchemaGrantOptions[g]})
			if acl.Null {
				continue
			}
			// ObjectACL's second item is the grantee's.
			if parts := splitACLItems(acl.S); len(parts) == 2 {
				if g == catalog.PublicRole && !db.UsageRestricted {
					items[1] = parts[1] // PUBLIC's own item, with what else it holds
					continue
				}
				items = append(items, parts[1])
			}
		}
	}
	return Row{i64(OIDPublic), str(catalog.PublicSchema), i64(RoleOID(owner)), str(aclArray(items))}
}

// splitACLItems splits an aclitem[] text ObjectACL wrote into its
// items, unquoted.
func splitACLItems(arr string) []string {
	arr = strings.TrimSuffix(strings.TrimPrefix(arr, "{"), "}")
	var out []string
	var b strings.Builder
	inQ := false
	for i := 0; i < len(arr); i++ {
		c := arr[i]
		switch {
		case c == '\\' && inQ && i+1 < len(arr):
			i++
			b.WriteByte(arr[i])
		case c == '"':
			inQ = !inQ
		case c == ',' && !inQ:
			out = append(out, b.String())
			b.Reset()
		default:
			b.WriteByte(c)
		}
	}
	return append(out, b.String())
}

// databaseACL is pg_database.datacl: NULL (PostgreSQL's default, which
// datax's matches — the owner's CREATE, TEMPORARY and CONNECT, PUBLIC's
// TEMPORARY and CONNECT) until a GRANT or REVOKE changes it; then the
// default as REVOKE CONNECT ... FROM PUBLIC left it, and each grant.
func databaseACL(d *catalog.DatabaseDescriptor) types.Datum {
	if len(d.Privileges) == 0 && !d.ConnectRestricted {
		return types.DNull
	}
	owner := catalog.OwnerOf(d.Owner)
	o := aclName(owner)
	public := "=Tc/" + o
	if d.ConnectRestricted {
		public = "=T/" + o
	}
	items := []string{public, o + "=CTc/" + o}
	if acl := ObjectACL(owner, "", d.Privileges, d.GrantOptions); !acl.Null {
		items = append(items, splitACLItems(acl.S)[1:]...)
	}
	return types.NewString(aclArray(items))
}
