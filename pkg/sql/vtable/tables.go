package vtable

import (
	"context"
	"github.com/sthorne/datax/pkg/sql/builtins"
	"sort"
	"strconv"
	"strings"

	"github.com/sthorne/datax/pkg/sql/catalog"
	"github.com/sthorne/datax/pkg/sql/types"
)

// The tables. Column sets follow PostgreSQL 16's, trimmed to what psql,
// pgx, lib/pq and the common ORMs read; anything a tool selects that we
// do not model is the column's zero value rather than an error.

var pgCatalogTables = map[string]*Table{}
var informationSchemaTables = map[string]*Table{}

func pg(name string, cols []catalog.Column, rows func(ctx context.Context, env *Env) ([]Row, error)) {
	pgCatalogTables[name] = &Table{Schema: PgCatalog, Name: name, Columns: cols, Rows: rows}
}

func is(name string, cols []catalog.Column, rows func(ctx context.Context, env *Env) ([]Row, error)) {
	informationSchemaTables[name] = &Table{Schema: InformationSchema, Name: name, Columns: cols, Rows: rows}
}

// visibleTables are the session's tables in its current database, the
// ones psql lists; every database's tables are reachable qualified.
// currentSequences lists the sequences of the session's database.
func (env *Env) currentSequences() []*catalog.SequenceDescriptor {
	dbID := uint64(0)
	for _, d := range env.Databases {
		if d.Name == env.Database {
			dbID = d.ID
		}
	}
	var out []*catalog.SequenceDescriptor
	for _, sq := range env.Sequences {
		if sq.DatabaseID == dbID || dbID == 0 {
			out = append(out, sq)
		}
	}
	return out
}

func yesNo(b bool) string {
	if b {
		return "YES"
	}
	return "NO"
}

func (env *Env) currentTables() []*catalog.TableDescriptor {
	var out []*catalog.TableDescriptor
	var cur *catalog.DatabaseDescriptor
	for _, d := range env.Databases {
		if d.Name == env.Database {
			cur = d
		}
	}
	for _, t := range env.Tables {
		if cur != nil && (t.DatabaseID == cur.ID || (t.DatabaseID == 0 && cur.Name == catalog.DefaultDatabase)) {
			out = append(out, t)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out
}

func ownerOf(env *Env) string { return "root" }

func init() {
	// pg_database: one row per database; encoding and locale are fixed.
	pg("pg_database", []catalog.Column{
		col("oid", types.Int), col("datname", types.String), col("datdba", types.Int), col("encoding", types.Int),
		col("datcollate", types.String), col("datctype", types.String), col("datistemplate", types.Bool),
		col("datallowconn", types.Bool), col("datconnlimit", types.Int), col("dattablespace", types.Int), col("datacl", types.String),
	}, func(ctx context.Context, env *Env) ([]Row, error) {
		var rows []Row
		for _, d := range env.Databases {
			rows = append(rows, Row{i64(DatabaseOID(d)), str(d.Name), i64(10), i64(6), str("C"), str("C"), boolean(false), boolean(true), i64(-1), i64(1663), null()})
		}
		return rows, nil
	})

	// pg_namespace: public, pg_catalog, information_schema.
	pg("pg_namespace", []catalog.Column{col("oid", types.Int), col("nspname", types.String), col("nspowner", types.Int), col("nspacl", types.String)},
		func(ctx context.Context, env *Env) ([]Row, error) {
			return []Row{
				{i64(OIDPgCatalog), str(PgCatalog), i64(10), null()},
				{i64(OIDPublic), str(catalog.PublicSchema), i64(10), null()},
				{i64(OIDInformationSchema), str(InformationSchema), i64(10), null()},
			}, nil
		})

	// pg_class: tables ('r') and their indexes ('i'), in the current
	// database; virtual tables themselves appear as views ('v') in
	// pg_catalog so `\dv` and introspection see them.
	pg("pg_class", []catalog.Column{
		col("oid", types.Int), col("relname", types.String), col("relnamespace", types.Int), col("reltype", types.Int),
		col("relowner", types.Int), col("relam", types.Int), col("relfilenode", types.Int), col("reltablespace", types.Int),
		col("relpages", types.Int), col("reltuples", types.Float), col("reltoastrelid", types.Int), col("relhasindex", types.Bool),
		col("relisshared", types.Bool), col("relpersistence", types.String), col("relkind", types.String), col("relnatts", types.Int),
		col("relchecks", types.Int), col("relhasrules", types.Bool), col("relhastriggers", types.Bool), col("relhassubclass", types.Bool),
		col("relrowsecurity", types.Bool), col("relforcerowsecurity", types.Bool), col("relispopulated", types.Bool),
		col("relreplident", types.String), col("relispartition", types.Bool), col("reloftype", types.Int), col("relacl", types.String), col("reloptions", types.String),
		col("relpartbound", types.String), hidden("__expr"),
	}, func(ctx context.Context, env *Env) ([]Row, error) {
		var rows []Row
		for _, t := range env.currentTables() {
			tuples := 0.0
			if st := env.Stats[t.ID]; st != nil {
				tuples = float64(st.RowCount)
			}
			// relchecks counts the CHECK constraints and relhastriggers
			// marks foreign keys either way: psql fetches a table's
			// constraints only when they say so.
			checks, triggers := int64(0), len(t.InboundFKs) > 0
			for i := range t.Constraints {
				switch t.Constraints[i].Kind {
				case catalog.ConstraintCheck:
					checks++
				case catalog.ConstraintForeign:
					triggers = true
				}
			}
			rows = append(rows, Row{i64(TableOID(t)), str(t.Name), i64(OIDPublic), i64(0), i64(10), i64(2), i64(TableOID(t)), i64(0),
				i64(0), types.NewFloat(tuples), i64(0), boolean(len(t.Indexes) > 0), boolean(false), str("p"), str("r"), i64(int64(len(t.VisibleColumns()))),
				i64(checks), boolean(false), boolean(triggers), boolean(false), boolean(false), boolean(false), boolean(true), str("d"), boolean(false), i64(0), null(), null(), null(), null()})
			rows = append(rows, Row{i64(IndexOID(t, 1)), str(t.Name + "_pkey"), i64(OIDPublic), i64(0), i64(10), i64(403), i64(IndexOID(t, 1)), i64(0),
				i64(0), types.NewFloat(tuples), i64(0), boolean(false), boolean(false), str("p"), str("i"), i64(int64(len(t.PrimaryKey))),
				i64(0), boolean(false), boolean(false), boolean(false), boolean(false), boolean(false), boolean(true), str("n"), boolean(false), i64(0), null(), null(), null(), null()})
			for _, idx := range t.Indexes {
				rows = append(rows, Row{i64(IndexOID(t, idx.ID)), str(idx.Name), i64(OIDPublic), i64(0), i64(10), i64(403), i64(IndexOID(t, idx.ID)), i64(0),
					i64(0), types.NewFloat(tuples), i64(0), boolean(false), boolean(false), str("p"), str("i"), i64(int64(len(idx.ColumnIDs))),
					i64(0), boolean(false), boolean(false), boolean(false), boolean(false), boolean(false), boolean(true), str("n"), boolean(false), i64(0), null(), null(), null(), null()})
			}
		}
		for _, sq := range env.currentSequences() {
			rows = append(rows, Row{i64(int64(sq.ID)), str(sq.Name), i64(OIDPublic), i64(0), i64(10), i64(0), i64(int64(sq.ID)), i64(0),
				i64(1), types.NewFloat(1), i64(0), boolean(false), boolean(false), str("p"), str("S"), i64(3),
				i64(0), boolean(false), boolean(false), boolean(false), boolean(false), boolean(false), boolean(true), str("n"), boolean(false), i64(0), null(), null(), null(), null()})
		}
		for i, name := range Names() {
			rows = append(rows, Row{i64(int64(1<<30) + int64(i)), str(name[len(PgCatalog)+1:]), i64(OIDPgCatalog), i64(0), i64(10), i64(0), i64(0), i64(0),
				i64(0), types.NewFloat(0), i64(0), boolean(false), boolean(false), str("p"), str("v"), i64(0),
				i64(0), boolean(false), boolean(false), boolean(false), boolean(false), boolean(false), boolean(true), str("n"), boolean(false), i64(0), null(), null(), null(), null()})
		}
		return rows, nil
	})

	// pg_attribute: every visible column of every table (attnum from 1).
	pg("pg_attribute", []catalog.Column{
		col("attrelid", types.Int), col("attname", types.String), col("atttypid", types.Int), col("attstattarget", types.Int),
		col("attlen", types.Int), col("attnum", types.Int), col("attndims", types.Int), col("atttypmod", types.Int),
		col("attbyval", types.Bool), col("attstorage", types.String), col("attalign", types.String), col("attnotnull", types.Bool),
		col("atthasdef", types.Bool), col("atthasmissing", types.Bool), col("attidentity", types.String), col("attgenerated", types.String),
		col("attisdropped", types.Bool), col("attislocal", types.Bool), col("attinhcount", types.Int), col("attcollation", types.Int), col("attacl", types.String),
		col("attcompression", types.String), col("attoptions", types.String), col("attfdwoptions", types.String), col("attmissingval", types.String),
		hidden("__format_type"), hidden("__indexdef"),
	}, func(ctx context.Context, env *Env) ([]Row, error) {
		var rows []Row
		attr := func(relid int64, c *catalog.Column, n int64, indexed bool) Row {
			typmod := int64(-1)
			if c.Type == types.Decimal && c.Precision > 0 {
				typmod = (int64(c.Precision)<<16 | int64(c.Scale)) + 4
			}
			indexdef := null()
			if indexed {
				indexdef = str(c.Name) // pg_get_indexdef(index, attnum, true): the key expression
			}
			identity := ""
			switch c.Identity {
			case "always":
				identity = "a"
			case "by default":
				identity = "d"
			}
			return Row{i64(relid), str(c.Name), i64(TypeOID(c.Type)), i64(-1),
				i64(-1), i64(n), i64(0), i64(typmod), boolean(false), str("p"), str("i"), boolean(c.NotNull),
				boolean(ColumnDefault(c) != ""), boolean(false), str(identity), str(""), boolean(false), boolean(true), i64(0), i64(0), null(),
				str(""), null(), null(), null(), str(FormatType(c)), indexdef}
		}
		for _, t := range env.currentTables() {
			n := int64(0)
			for i := range t.Columns {
				c := &t.Columns[i]
				if c.Hidden {
					continue
				}
				n++
				rows = append(rows, attr(TableOID(t), c, n, false))
			}
			// Indexes are relations too: their attributes are the key
			// columns, in key order.
			for j, id := range visiblePK(t) {
				if c, ok := t.ColByID(id); ok {
					rows = append(rows, attr(IndexOID(t, 1), &c, int64(j+1), true))
				}
			}
			for i := range t.Indexes {
				idx := &t.Indexes[i]
				for j, id := range idx.ColumnIDs {
					if c, ok := t.ColByID(id); ok {
						rows = append(rows, attr(IndexOID(t, idx.ID), &c, int64(j+1), true))
					}
				}
			}
		}
		// The catalog views themselves (\d pg_class).
		for i, name := range Names() {
			vt, _ := Lookup(name)
			n := int64(0)
			for _, c := range vt.Columns {
				if strings.HasPrefix(c.Name, "__") {
					continue
				}
				n++
				c := c
				rows = append(rows, attr(int64(1<<30)+int64(i), &c, n, false))
			}
		}
		return rows, nil
	})

	// pg_type: the ten types, with PostgreSQL's OIDs.
	pg("pg_type", []catalog.Column{
		col("oid", types.Int), col("typname", types.String), col("typnamespace", types.Int), col("typowner", types.Int),
		col("typlen", types.Int), col("typbyval", types.Bool), col("typtype", types.String), col("typcategory", types.String),
		col("typisdefined", types.Bool), col("typdelim", types.String), col("typrelid", types.Int), col("typelem", types.Int),
		col("typarray", types.Int), col("typinput", types.String), col("typoutput", types.String), col("typnotnull", types.Bool),
		col("typbasetype", types.Int), col("typtypmod", types.Int), col("typndims", types.Int), col("typcollation", types.Int), col("typdefault", types.String),
		col("typacl", types.String), hidden("__format_type"),
	}, func(ctx context.Context, env *Env) ([]Row, error) {
		var rows []Row
		for _, f := range []types.Family{types.Bool, types.Bytes, types.Int, types.String, types.Float, types.Date, types.Timestamp, types.Uuid, types.Decimal, types.Jsonb} {
			cat := "S"
			switch f {
			case types.Bool:
				cat = "B"
			case types.Int, types.Float, types.Decimal:
				cat = "N"
			case types.Date, types.Timestamp:
				cat = "D"
			case types.Bytes, types.Uuid, types.Jsonb:
				cat = "U"
			}
			name := TypeName(f)
			rows = append(rows, Row{i64(TypeOID(f)), str(name), i64(OIDPgCatalog), i64(10), i64(-1), boolean(false), str("b"), str(cat),
				boolean(true), str(","), i64(0), i64(0), i64(0), str(name + "in"), str(name + "out"), boolean(false), i64(0), i64(-1), i64(0), i64(0), null(),
				null(), str(FormatTypeOID(TypeOID(f)))})
		}
		return rows, nil
	})

	// pg_index: the primary index and every secondary, keyed to their
	// pg_class rows; indkey lists column attnums separated by spaces.
	pg("pg_index", []catalog.Column{
		col("indexrelid", types.Int), col("indrelid", types.Int), col("indnatts", types.Int), col("indnkeyatts", types.Int),
		col("indisunique", types.Bool), col("indisprimary", types.Bool), col("indisexclusion", types.Bool), col("indimmediate", types.Bool),
		col("indisclustered", types.Bool), col("indisvalid", types.Bool), col("indisready", types.Bool), col("indislive", types.Bool),
		col("indisreplident", types.Bool), col("indkey", types.String), col("indpred", types.String),
		hidden("__indexdef"), hidden("__expr"),
	}, func(ctx context.Context, env *Env) ([]Row, error) {
		var rows []Row
		for _, t := range env.currentTables() {
			pk := &catalog.IndexDescriptor{Name: t.Name + "_pkey", Unique: true, ColumnIDs: visiblePK(t)}
			rows = append(rows, Row{i64(IndexOID(t, 1)), i64(TableOID(t)), i64(int64(len(visiblePK(t)))), i64(int64(len(visiblePK(t)))),
				boolean(true), boolean(true), boolean(false), boolean(true), boolean(false), boolean(true), boolean(true), boolean(true),
				boolean(false), str(attnums(t, visiblePK(t))), null(), str(IndexDef(t, pk)), null()})
			for i := range t.Indexes {
				idx := &t.Indexes[i]
				rows = append(rows, Row{i64(IndexOID(t, idx.ID)), i64(TableOID(t)), i64(int64(len(idx.ColumnIDs))), i64(int64(len(idx.ColumnIDs))),
					boolean(idx.Unique), boolean(false), boolean(false), boolean(true), boolean(false), boolean(idx.Public()), boolean(true), boolean(true),
					boolean(false), str(attnums(t, idx.ColumnIDs)), null(), str(IndexDef(t, idx)), null()})
			}
		}
		return rows, nil
	})

	// pg_am: btree, the only access method.
	pg("pg_am", []catalog.Column{col("oid", types.Int), col("amname", types.String), col("amhandler", types.String), col("amtype", types.String)},
		func(ctx context.Context, env *Env) ([]Row, error) {
			return []Row{{i64(403), str("btree"), str("bthandler"), str("i")}, {i64(2), str("heap"), str("heap_tableam_handler"), str("t")}}, nil
		})

	// pg_collation: the default collation only (psql's \d consults it to
	// annotate non-default column collations).
	pg("pg_collation", []catalog.Column{
		col("oid", types.Int), col("collname", types.String), col("collnamespace", types.Int), col("collowner", types.Int),
		col("collprovider", types.String), col("collisdeterministic", types.Bool), col("collencoding", types.Int),
		col("collcollate", types.String), col("collctype", types.String),
	}, func(ctx context.Context, env *Env) ([]Row, error) {
		return []Row{
			{i64(100), str("default"), i64(OIDPgCatalog), i64(10), str("d"), boolean(true), i64(-1), null(), null()},
			{i64(950), str("C"), i64(OIDPgCatalog), i64(10), str("c"), boolean(true), i64(-1), str("C"), str("C")},
			{i64(951), str("POSIX"), i64(OIDPgCatalog), i64(10), str("c"), boolean(true), i64(-1), str("POSIX"), str("POSIX")},
		}, nil
	})

	// pg_tablespace: the two built-in tablespaces.
	pg("pg_tablespace", []catalog.Column{
		col("oid", types.Int), col("spcname", types.String), col("spcowner", types.Int), col("spcacl", types.String), col("spcoptions", types.String),
	}, func(ctx context.Context, env *Env) ([]Row, error) {
		return []Row{{i64(1663), str("pg_default"), i64(10), null(), null()}, {i64(1664), str("pg_global"), i64(10), null(), null()}}, nil
	})
	pg("pg_extension", []catalog.Column{
		col("oid", types.Int), col("extname", types.String), col("extowner", types.Int), col("extnamespace", types.Int),
		col("extrelocatable", types.Bool), col("extversion", types.String), col("extconfig", types.String), col("extcondition", types.String),
	}, empty)
	pg("pg_description", []catalog.Column{
		col("objoid", types.Int), col("classoid", types.Int), col("objsubid", types.Int), col("description", types.String),
	}, empty)
	// pg_proc: the builtin functions, from the registry (aliases included,
	// as PostgreSQL lists each name).
	pg("pg_proc", []catalog.Column{
		col("oid", types.Int), col("proname", types.String), col("pronamespace", types.Int), col("proowner", types.Int),
		col("prolang", types.Int), col("prokind", types.String), col("proretset", types.Bool), col("prorettype", types.Int),
		col("pronargs", types.Int), col("proargtypes", types.String), col("proargnames", types.String), col("prosrc", types.String),
		col("provolatile", types.String), col("proisstrict", types.Bool),
	}, func(ctx context.Context, env *Env) ([]Row, error) {
		var rows []Row
		for i, b := range builtins.All() {
			argOIDs := make([]string, len(b.Args))
			for j, f := range b.Args {
				argOIDs[j] = strconv.FormatInt(TypeOID(f), 10)
			}
			vol := "i"
			switch b.Vol {
			case builtins.Stable:
				vol = "s"
			case builtins.Volatile:
				vol = "v"
			}
			rows = append(rows, Row{i64(int64(1<<28) + int64(i)), str(b.Name), i64(OIDPgCatalog), i64(10), i64(12), str("f"), boolean(false),
				i64(TypeOID(b.ResultFamily(b.Args))), i64(int64(len(b.Args))), str(strings.Join(argOIDs, " ")), null(), str(b.Name), str(vol), boolean(!b.NotStrict)})
		}
		return rows, nil
	})

	// Catalogs psql consults for features datax does not have: row-level
	// security policies, extended statistics, logical-replication
	// publications, and table inheritance. All empty.
	pg("pg_policy", []catalog.Column{
		col("oid", types.Int), col("polname", types.String), col("polrelid", types.Int), col("polcmd", types.String),
		col("polpermissive", types.Bool), col("polroles", types.String), col("polqual", types.String), col("polwithcheck", types.String),
		hidden("__expr"),
	}, empty)
	pg("pg_statistic_ext", []catalog.Column{
		col("oid", types.Int), col("stxrelid", types.Int), col("stxname", types.String), col("stxnamespace", types.Int),
		col("stxowner", types.Int), col("stxstattarget", types.Int), col("stxkeys", types.String), col("stxkind", types.String),
	}, empty)
	pg("pg_publication", []catalog.Column{
		col("oid", types.Int), col("pubname", types.String), col("pubowner", types.Int), col("puballtables", types.Bool),
		col("pubinsert", types.Bool), col("pubupdate", types.Bool), col("pubdelete", types.Bool), col("pubtruncate", types.Bool), col("pubviaroot", types.Bool),
	}, empty)
	pg("pg_publication_rel", []catalog.Column{col("oid", types.Int), col("prpubid", types.Int), col("prrelid", types.Int)}, empty)
	pg("pg_cast", []catalog.Column{col("oid", types.Int), col("castsource", types.Int), col("casttarget", types.Int), col("castfunc", types.Int), col("castcontext", types.String), col("castmethod", types.String)}, empty)
	pg("pg_operator", []catalog.Column{col("oid", types.Int), col("oprname", types.String), col("oprnamespace", types.Int), col("oprowner", types.Int), col("oprkind", types.String), col("oprleft", types.Int), col("oprright", types.Int), col("oprresult", types.Int), col("oprcode", types.String)}, empty)
	pg("pg_language", []catalog.Column{col("oid", types.Int), col("tableoid", types.Int), col("lanname", types.String), col("lanowner", types.Int), col("lanispl", types.Bool), col("lanpltrusted", types.Bool), col("lanplcallfoid", types.Int), col("lanacl", types.String)}, empty)
	pg("pg_subscription", []catalog.Column{col("oid", types.Int), col("subdbid", types.Int), col("subname", types.String), col("subowner", types.Int), col("subenabled", types.Bool), col("subconninfo", types.String), col("subslotname", types.String), col("subpublications", types.String)}, empty)
	pg("pg_user_mappings", []catalog.Column{col("umid", types.Int), col("srvid", types.Int), col("srvname", types.String), col("umuser", types.Int), col("usename", types.String), col("umoptions", types.String)}, empty)
	pg("pg_ts_dict", []catalog.Column{col("oid", types.Int), col("dictname", types.String), col("dictnamespace", types.Int), col("dictowner", types.Int), col("dicttemplate", types.Int), col("dictinitoption", types.String)}, empty)
	pg("pg_ts_config", []catalog.Column{col("oid", types.Int), col("cfgname", types.String), col("cfgnamespace", types.Int), col("cfgowner", types.Int), col("cfgparser", types.Int)}, empty)
	pg("pg_ts_parser", []catalog.Column{col("oid", types.Int), col("prsname", types.String), col("prsnamespace", types.Int), col("prsstart", types.Int), col("prstoken", types.Int), col("prsend", types.Int), col("prsheadline", types.Int), col("prslextype", types.Int)}, empty)
	pg("pg_ts_template", []catalog.Column{col("oid", types.Int), col("tmplname", types.String), col("tmplnamespace", types.Int), col("tmplinit", types.Int), col("tmpllexize", types.Int)}, empty)
	pg("pg_largeobject_metadata", []catalog.Column{col("oid", types.Int), col("lomowner", types.Int), col("lomacl", types.String)}, empty)
	pg("pg_db_role_setting", []catalog.Column{col("setdatabase", types.Int), col("setrole", types.Int), col("setconfig", types.String)}, empty)
	pg("pg_foreign_data_wrapper", []catalog.Column{col("oid", types.Int), col("fdwname", types.String), col("fdwowner", types.Int), col("fdwhandler", types.Int), col("fdwvalidator", types.Int), col("fdwacl", types.String), col("fdwoptions", types.String)}, empty)
	pg("pg_foreign_server", []catalog.Column{col("oid", types.Int), col("srvname", types.String), col("srvowner", types.Int), col("srvfdw", types.Int), col("srvtype", types.String), col("srvversion", types.String), col("srvacl", types.String), col("srvoptions", types.String)}, empty)
	pg("pg_foreign_table", []catalog.Column{col("ftrelid", types.Int), col("ftserver", types.Int), col("ftoptions", types.String)}, empty)
	pg("pg_conversion", []catalog.Column{col("oid", types.Int), col("conname", types.String), col("connamespace", types.Int), col("conowner", types.Int), col("conforencoding", types.Int), col("contoencoding", types.Int), col("conproc", types.Int), col("condefault", types.Bool)}, empty)
	pg("pg_opclass", []catalog.Column{col("oid", types.Int), col("opcmethod", types.Int), col("opcname", types.String), col("opcnamespace", types.Int), col("opcowner", types.Int), col("opcfamily", types.Int), col("opcintype", types.Int), col("opcdefault", types.Bool), col("opckeytype", types.Int)}, empty)
	pg("pg_opfamily", []catalog.Column{col("oid", types.Int), col("opfmethod", types.Int), col("opfname", types.String), col("opfnamespace", types.Int), col("opfowner", types.Int)}, empty)
	pg("pg_trigger", []catalog.Column{col("oid", types.Int), col("tgrelid", types.Int), col("tgname", types.String), col("tgfoid", types.Int), col("tgtype", types.Int), col("tgenabled", types.String), col("tgisinternal", types.Bool), col("tgconstraint", types.Int), col("tgdeferrable", types.Bool), col("tginitdeferred", types.Bool)}, empty)
	pg("pg_rewrite", []catalog.Column{col("oid", types.Int), col("rulename", types.String), col("ev_class", types.Int), col("ev_type", types.String), col("ev_enabled", types.String), col("is_instead", types.Bool)}, empty)
	pg("pg_event_trigger", []catalog.Column{col("oid", types.Int), col("evtname", types.String), col("evtevent", types.String), col("evtowner", types.Int), col("evtfoid", types.Int), col("evtenabled", types.String), col("evttags", types.String)}, empty)
	pg("pg_sequence", []catalog.Column{col("seqrelid", types.Int), col("seqtypid", types.Int), col("seqstart", types.Int), col("seqincrement", types.Int), col("seqmax", types.Int), col("seqmin", types.Int), col("seqcache", types.Int), col("seqcycle", types.Bool)}, empty)
	pg("pg_views", []catalog.Column{col("schemaname", types.String), col("viewname", types.String), col("viewowner", types.String), col("definition", types.String)}, empty)
	pg("pg_matviews", []catalog.Column{col("schemaname", types.String), col("matviewname", types.String), col("matviewowner", types.String), col("ispopulated", types.Bool), col("definition", types.String)}, empty)
	pg("pg_shdescription", []catalog.Column{col("objoid", types.Int), col("classoid", types.Int), col("description", types.String)}, empty)
	pg("pg_auth_members", []catalog.Column{col("roleid", types.Int), col("member", types.Int), col("grantor", types.Int), col("admin_option", types.Bool)}, empty)
	pg("pg_depend", []catalog.Column{col("classid", types.Int), col("objid", types.Int), col("objsubid", types.Int), col("refclassid", types.Int), col("refobjid", types.Int), col("refobjsubid", types.Int), col("deptype", types.String)}, empty)
	pg("pg_enum", []catalog.Column{col("oid", types.Int), col("enumtypid", types.Int), col("enumsortorder", types.Float), col("enumlabel", types.String)}, empty)
	pg("pg_range", []catalog.Column{col("rngtypid", types.Int), col("rngsubtype", types.Int), col("rngmultitypid", types.Int), col("rngcollation", types.Int), col("rngsubopc", types.Int), col("rngcanonical", types.Int), col("rngsubdiff", types.Int)}, empty)
	pg("pg_partitioned_table", []catalog.Column{col("partrelid", types.Int), col("partstrat", types.String), col("partnatts", types.Int), col("partdefid", types.Int), col("partattrs", types.String)}, empty)
	pg("pg_sequences", []catalog.Column{col("schemaname", types.String), col("sequencename", types.String), col("sequenceowner", types.String), col("data_type", types.String), col("start_value", types.Int), col("min_value", types.Int), col("max_value", types.Int), col("increment_by", types.Int), col("cycle", types.Bool), col("cache_size", types.Int), col("last_value", types.Int)},
		func(ctx context.Context, env *Env) ([]Row, error) {
			var rows []Row
			for _, sq := range env.currentSequences() {
				last := null()
				if env.SequenceValue != nil {
					if v, called, err := env.SequenceValue(sq); err == nil && called {
						last = i64(v)
					}
				}
				rows = append(rows, Row{str(catalog.PublicSchema), str(sq.Name), str("root"), str("bigint"), i64(sq.Start), i64(sq.MinValue), i64(sq.MaxValue), i64(sq.Increment), boolean(sq.Cycle), i64(sq.Cache), last})
			}
			return rows, nil
		})
	pg("pg_sequence", []catalog.Column{col("seqrelid", types.Int), col("seqtypid", types.Int), col("seqstart", types.Int), col("seqincrement", types.Int), col("seqmax", types.Int), col("seqmin", types.Int), col("seqcache", types.Int), col("seqcycle", types.Bool)},
		func(ctx context.Context, env *Env) ([]Row, error) {
			var rows []Row
			for _, sq := range env.currentSequences() {
				rows = append(rows, Row{i64(int64(sq.ID)), i64(TypeOID(types.Int)), i64(sq.Start), i64(sq.Increment), i64(sq.MaxValue), i64(sq.MinValue), i64(sq.Cache), boolean(sq.Cycle)})
			}
			return rows, nil
		})
	pg("pg_inherits", []catalog.Column{
		col("inhrelid", types.Int), col("inhparent", types.Int), col("inhseqno", types.Int), col("inhdetachpending", types.Bool),
	}, empty)

	// pg_constraint: primary keys ('p'), unique indexes and constraints
	// ('u'), checks ('c') and foreign keys ('f').
	pg("pg_constraint", []catalog.Column{
		col("oid", types.Int), col("conname", types.String), col("connamespace", types.Int), col("contype", types.String),
		col("condeferrable", types.Bool), col("condeferred", types.Bool), col("convalidated", types.Bool), col("conrelid", types.Int),
		col("contypid", types.Int), col("conindid", types.Int), col("confrelid", types.Int), col("conkey", types.String), col("confkey", types.String), col("conbin", types.String),
		col("conparentid", types.Int), col("confupdtype", types.String), col("confdeltype", types.String), col("confmatchtype", types.String),
		hidden("__condef"),
	}, func(ctx context.Context, env *Env) ([]Row, error) {
		var rows []Row
		fkAction := func(a string) string {
			switch a {
			case catalog.FKCascade:
				return "c"
			case catalog.FKSetNull:
				return "n"
			}
			return "r"
		}
		for _, t := range env.currentTables() {
			rows = append(rows, Row{i64(IndexOID(t, 1)), str(t.Name + "_pkey"), i64(OIDPublic), str("p"), boolean(false), boolean(false), boolean(true),
				i64(TableOID(t)), i64(0), i64(IndexOID(t, 1)), i64(0), str(intArray(attnums(t, visiblePK(t)))), null(), null(), i64(0), str(" "), str(" "), str(" "), str(PrimaryKeyDef(t))})
			for i := range t.Indexes {
				idx := &t.Indexes[i]
				if !idx.Unique {
					continue
				}
				rows = append(rows, Row{i64(IndexOID(t, idx.ID)), str(idx.Name), i64(OIDPublic), str("u"), boolean(false), boolean(false), boolean(true),
					i64(TableOID(t)), i64(0), i64(IndexOID(t, idx.ID)), i64(0), str(intArray(attnums(t, idx.ColumnIDs))), null(), null(), i64(0), str(" "), str(" "), str(" "), str(UniqueDef(t, idx))})
			}
			for i := range t.Constraints {
				c := &t.Constraints[i]
				def := ConstraintDef(t, c, env.tableByID)
				switch c.Kind {
				case catalog.ConstraintCheck:
					rows = append(rows, Row{i64(ConstraintOID(t, c)), str(c.Name), i64(OIDPublic), str("c"), boolean(false), boolean(false), boolean(c.Validated),
						i64(TableOID(t)), i64(0), i64(0), i64(0), str(intArray(attnums(t, c.Columns))), null(), str(c.Expr), i64(0), str(" "), str(" "), str(" "), str(def)})
				case catalog.ConstraintForeign:
					ref := t
					if c.RefTable != t.ID {
						ref = env.tableByID(c.RefTable)
					}
					confkey, confrelid := null(), i64(int64(c.RefTable))
					if ref != nil {
						confkey = str(intArray(attnums(ref, c.RefColumns)))
					}
					rows = append(rows, Row{i64(ConstraintOID(t, c)), str(c.Name), i64(OIDPublic), str("f"), boolean(false), boolean(false), boolean(c.Validated),
						i64(TableOID(t)), i64(0), i64(0), confrelid, str(intArray(attnums(t, c.Columns))), confkey, null(), i64(0), str(fkAction(c.OnUpdate)), str(fkAction(c.OnDelete)), str("s"), str(def)})
				}
			}
		}
		return rows, nil
	})

	// pg_attrdef: column defaults, rendered as SQL.
	pg("pg_attrdef", []catalog.Column{col("oid", types.Int), col("adrelid", types.Int), col("adnum", types.Int), col("adbin", types.String), hidden("__expr")},
		func(ctx context.Context, env *Env) ([]Row, error) {
			var rows []Row
			for _, t := range env.currentTables() {
				n := int64(0)
				for _, c := range t.Columns {
					if c.Hidden {
						continue
					}
					n++
					if def := ColumnDefault(&c); def != "" {
						rows = append(rows, Row{i64(TableOID(t)<<8 + n), i64(TableOID(t)), i64(n), str(def), str(def)})
					}
				}
			}
			return rows, nil
		})

	// pg_roles / pg_user / pg_authid-lite: every SQL user; admins are
	// superusers for the tools' purposes.
	roleCols := []catalog.Column{
		col("oid", types.Int), col("rolname", types.String), col("rolsuper", types.Bool), col("rolinherit", types.Bool),
		col("rolcreaterole", types.Bool), col("rolcreatedb", types.Bool), col("rolcanlogin", types.Bool), col("rolreplication", types.Bool),
		col("rolconnlimit", types.Int), col("rolpassword", types.String), col("rolvaliduntil", types.Timestamp), col("rolbypassrls", types.Bool), col("rolconfig", types.String),
	}
	roleRows := func(ctx context.Context, env *Env) ([]Row, error) {
		var rows []Row
		for i, u := range env.Users {
			admin := u == "root" || env.Admins[u]
			rows = append(rows, Row{i64(int64(10 + i)), str(u), boolean(admin), boolean(true), boolean(admin), boolean(admin), boolean(true), boolean(false),
				i64(-1), str("********"), null(), boolean(admin), null()})
		}
		return rows, nil
	}
	pg("pg_roles", roleCols, roleRows)
	pg("pg_user", []catalog.Column{col("usename", types.String), col("usesysid", types.Int), col("usecreatedb", types.Bool), col("usesuper", types.Bool), col("userepl", types.Bool), col("usebypassrls", types.Bool), col("passwd", types.String), col("valuntil", types.Timestamp), col("useconfig", types.String)},
		func(ctx context.Context, env *Env) ([]Row, error) {
			var rows []Row
			for i, u := range env.Users {
				admin := u == "root" || env.Admins[u]
				rows = append(rows, Row{str(u), i64(int64(10 + i)), boolean(admin), boolean(admin), boolean(false), boolean(admin), str("********"), null(), null()})
			}
			return rows, nil
		})

	// pg_settings: the session variables.
	pg("pg_settings", []catalog.Column{col("name", types.String), col("setting", types.String), col("unit", types.String), col("category", types.String), col("short_desc", types.String), col("context", types.String), col("vartype", types.String), col("source", types.String), col("boot_val", types.String), col("reset_val", types.String)},
		func(ctx context.Context, env *Env) ([]Row, error) {
			var rows []Row
			for _, kv := range env.Settings {
				rows = append(rows, Row{str(kv[0]), str(kv[1]), null(), str("Client Connection Defaults"), str(kv[0]), str("user"), str("string"), str("default"), str(kv[1]), str(kv[1])})
			}
			return rows, nil
		})

	// pg_tables / pg_indexes: the convenience views.
	pg("pg_tables", []catalog.Column{col("schemaname", types.String), col("tablename", types.String), col("tableowner", types.String), col("tablespace", types.String), col("hasindexes", types.Bool), col("hasrules", types.Bool), col("hastriggers", types.Bool), col("rowsecurity", types.Bool)},
		func(ctx context.Context, env *Env) ([]Row, error) {
			var rows []Row
			for _, t := range env.currentTables() {
				rows = append(rows, Row{str(catalog.PublicSchema), str(t.Name), str(ownerOf(env)), null(), boolean(len(t.Indexes) > 0), boolean(false), boolean(false), boolean(false)})
			}
			return rows, nil
		})
	pg("pg_indexes", []catalog.Column{col("schemaname", types.String), col("tablename", types.String), col("indexname", types.String), col("tablespace", types.String), col("indexdef", types.String)},
		func(ctx context.Context, env *Env) ([]Row, error) {
			var rows []Row
			for _, t := range env.currentTables() {
				pkIdx := &catalog.IndexDescriptor{Name: t.Name + "_pkey", Unique: true, ColumnIDs: visiblePK(t)}
				rows = append(rows, Row{str(catalog.PublicSchema), str(t.Name), str(pkIdx.Name), null(), str(IndexDef(t, pkIdx))})
				for i := range t.Indexes {
					rows = append(rows, Row{str(catalog.PublicSchema), str(t.Name), str(t.Indexes[i].Name), null(), str(IndexDef(t, &t.Indexes[i]))})
				}
			}
			return rows, nil
		})

	// pg_stat_user_tables: row counts from statistics.
	pg("pg_stat_user_tables", []catalog.Column{col("relid", types.Int), col("schemaname", types.String), col("relname", types.String), col("n_live_tup", types.Int), col("n_dead_tup", types.Int), col("last_analyze", types.Timestamp)},
		func(ctx context.Context, env *Env) ([]Row, error) {
			var rows []Row
			for _, t := range env.currentTables() {
				live, analyzed := int64(0), null()
				if st := env.Stats[t.ID]; st != nil {
					live = st.RowCount
					analyzed = types.NewTimestamp(st.CollectedAt)
				}
				rows = append(rows, Row{i64(TableOID(t)), str(catalog.PublicSchema), str(t.Name), i64(live), i64(0), analyzed})
			}
			return rows, nil
		})

	// information_schema.
	is("schemata", []catalog.Column{col("catalog_name", types.String), col("schema_name", types.String), col("schema_owner", types.String), col("default_character_set_name", types.String)},
		func(ctx context.Context, env *Env) ([]Row, error) {
			var rows []Row
			for _, d := range env.Databases {
				for _, sch := range []string{catalog.PublicSchema, PgCatalog, InformationSchema} {
					rows = append(rows, Row{str(d.Name), str(sch), str("root"), str("UTF8")})
				}
			}
			return rows, nil
		})
	is("tables", []catalog.Column{col("table_catalog", types.String), col("table_schema", types.String), col("table_name", types.String), col("table_type", types.String), col("is_insertable_into", types.String)},
		func(ctx context.Context, env *Env) ([]Row, error) {
			var rows []Row
			for _, d := range env.Databases {
				for _, t := range env.Tables {
					if t.DatabaseID == d.ID || (t.DatabaseID == 0 && d.Name == catalog.DefaultDatabase) {
						rows = append(rows, Row{str(d.Name), str(catalog.PublicSchema), str(t.Name), str("BASE TABLE"), str("YES")})
					}
				}
			}
			for _, name := range Names() {
				sch, bare := name[:len(PgCatalog)], name[len(PgCatalog)+1:]
				if len(name) > len(InformationSchema) && name[:len(InformationSchema)] == InformationSchema {
					sch, bare = InformationSchema, name[len(InformationSchema)+1:]
				}
				rows = append(rows, Row{str(env.Database), str(sch), str(bare), str("VIEW"), str("NO")})
			}
			return rows, nil
		})
	is("columns", []catalog.Column{
		col("table_catalog", types.String), col("table_schema", types.String), col("table_name", types.String), col("column_name", types.String),
		col("ordinal_position", types.Int), col("column_default", types.String), col("is_nullable", types.String), col("data_type", types.String),
		col("character_maximum_length", types.Int), col("numeric_precision", types.Int), col("numeric_scale", types.Int), col("udt_name", types.String), col("is_identity", types.String), col("is_generated", types.String),
	}, func(ctx context.Context, env *Env) ([]Row, error) {
		var rows []Row
		for _, d := range env.Databases {
			for _, t := range env.Tables {
				if !(t.DatabaseID == d.ID || (t.DatabaseID == 0 && d.Name == catalog.DefaultDatabase)) {
					continue
				}
				n := int64(0)
				for i := range t.Columns {
					c := &t.Columns[i]
					if c.Hidden {
						continue
					}
					n++
					def, nullable := null(), "YES"
					if text := ColumnDefault(c); text != "" {
						def = str(text)
					}
					if c.NotNull {
						nullable = "NO"
					}
					prec, scale := null(), null()
					if c.Type == types.Decimal && c.Precision > 0 {
						prec, scale = i64(int64(c.Precision)), i64(int64(c.Scale))
					}
					rows = append(rows, Row{str(d.Name), str(catalog.PublicSchema), str(t.Name), str(c.Name), i64(n), def, str(nullable), str(FormatType(c)),
						null(), prec, scale, str(TypeName(c.Type)), str(yesNo(c.Identity != "")), str("NEVER")})
				}
			}
		}
		return rows, nil
	})
	is("table_constraints", []catalog.Column{col("constraint_catalog", types.String), col("constraint_schema", types.String), col("constraint_name", types.String), col("table_catalog", types.String), col("table_schema", types.String), col("table_name", types.String), col("constraint_type", types.String), col("is_deferrable", types.String), col("initially_deferred", types.String)},
		func(ctx context.Context, env *Env) ([]Row, error) {
			var rows []Row
			for _, t := range env.currentTables() {
				rows = append(rows, Row{str(env.Database), str(catalog.PublicSchema), str(t.Name + "_pkey"), str(env.Database), str(catalog.PublicSchema), str(t.Name), str("PRIMARY KEY"), str("NO"), str("NO")})
				for _, idx := range t.Indexes {
					if idx.Unique {
						rows = append(rows, Row{str(env.Database), str(catalog.PublicSchema), str(idx.Name), str(env.Database), str(catalog.PublicSchema), str(t.Name), str("UNIQUE"), str("NO"), str("NO")})
					}
				}
				for i := range t.Constraints {
					c := &t.Constraints[i]
					kind := ""
					switch c.Kind {
					case catalog.ConstraintCheck:
						kind = "CHECK"
					case catalog.ConstraintForeign:
						kind = "FOREIGN KEY"
					default:
						continue // unique constraints are listed as their index
					}
					rows = append(rows, Row{str(env.Database), str(catalog.PublicSchema), str(c.Name), str(env.Database), str(catalog.PublicSchema), str(t.Name), str(kind), str("NO"), str("NO")})
				}
			}
			return rows, nil
		})
	is("check_constraints", []catalog.Column{col("constraint_catalog", types.String), col("constraint_schema", types.String), col("constraint_name", types.String), col("check_clause", types.String)},
		func(ctx context.Context, env *Env) ([]Row, error) {
			var rows []Row
			for _, t := range env.currentTables() {
				for i := range t.Constraints {
					if c := &t.Constraints[i]; c.Kind == catalog.ConstraintCheck {
						rows = append(rows, Row{str(env.Database), str(catalog.PublicSchema), str(c.Name), str(c.Expr)})
					}
				}
			}
			return rows, nil
		})
	is("referential_constraints", []catalog.Column{col("constraint_catalog", types.String), col("constraint_schema", types.String), col("constraint_name", types.String), col("unique_constraint_catalog", types.String), col("unique_constraint_schema", types.String), col("unique_constraint_name", types.String), col("match_option", types.String), col("update_rule", types.String), col("delete_rule", types.String)},
		func(ctx context.Context, env *Env) ([]Row, error) {
			var rows []Row
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
				for i := range t.Constraints {
					c := &t.Constraints[i]
					if c.Kind != catalog.ConstraintForeign {
						continue
					}
					ref := t
					if c.RefTable != t.ID {
						ref = env.tableByID(c.RefTable)
					}
					unique := null()
					if ref != nil {
						unique = str(uniqueConstraintNameFor(ref, c.RefColumns))
					}
					rows = append(rows, Row{str(env.Database), str(catalog.PublicSchema), str(c.Name), str(env.Database), str(catalog.PublicSchema), unique, str("NONE"), str(rule(c.OnUpdate)), str(rule(c.OnDelete))})
				}
			}
			return rows, nil
		})
	is("constraint_column_usage", []catalog.Column{col("table_catalog", types.String), col("table_schema", types.String), col("table_name", types.String), col("column_name", types.String), col("constraint_catalog", types.String), col("constraint_schema", types.String), col("constraint_name", types.String)},
		func(ctx context.Context, env *Env) ([]Row, error) {
			var rows []Row
			for _, t := range env.currentTables() {
				for i := range t.Constraints {
					c := &t.Constraints[i]
					switch c.Kind {
					case catalog.ConstraintCheck, catalog.ConstraintUnique:
						for _, id := range c.Columns {
							rows = append(rows, Row{str(env.Database), str(catalog.PublicSchema), str(t.Name), str(columnName(t, id)), str(env.Database), str(catalog.PublicSchema), str(c.Name)})
						}
					case catalog.ConstraintForeign:
						ref := t
						if c.RefTable != t.ID {
							ref = env.tableByID(c.RefTable)
						}
						if ref == nil {
							continue
						}
						for _, id := range c.RefColumns {
							rows = append(rows, Row{str(env.Database), str(catalog.PublicSchema), str(ref.Name), str(columnName(ref, id)), str(env.Database), str(catalog.PublicSchema), str(c.Name)})
						}
					}
				}
			}
			return rows, nil
		})
	is("key_column_usage", []catalog.Column{col("constraint_catalog", types.String), col("constraint_schema", types.String), col("constraint_name", types.String), col("table_catalog", types.String), col("table_schema", types.String), col("table_name", types.String), col("column_name", types.String), col("ordinal_position", types.Int)},
		func(ctx context.Context, env *Env) ([]Row, error) {
			var rows []Row
			for _, t := range env.currentTables() {
				for i, id := range visiblePK(t) {
					rows = append(rows, Row{str(env.Database), str(catalog.PublicSchema), str(t.Name + "_pkey"), str(env.Database), str(catalog.PublicSchema), str(t.Name), str(columnName(t, id)), i64(int64(i + 1))})
				}
				for _, idx := range t.Indexes {
					if !idx.Unique {
						continue
					}
					for i, id := range idx.ColumnIDs {
						rows = append(rows, Row{str(env.Database), str(catalog.PublicSchema), str(idx.Name), str(env.Database), str(catalog.PublicSchema), str(t.Name), str(columnName(t, id)), i64(int64(i + 1))})
					}
				}
				for ci := range t.Constraints {
					c := &t.Constraints[ci]
					if c.Kind != catalog.ConstraintForeign {
						continue
					}
					for i, id := range c.Columns {
						rows = append(rows, Row{str(env.Database), str(catalog.PublicSchema), str(c.Name), str(env.Database), str(catalog.PublicSchema), str(t.Name), str(columnName(t, id)), i64(int64(i + 1))})
					}
				}
			}
			return rows, nil
		})
	is("statistics", []catalog.Column{col("table_catalog", types.String), col("table_schema", types.String), col("table_name", types.String), col("non_unique", types.Bool), col("index_schema", types.String), col("index_name", types.String), col("seq_in_index", types.Int), col("column_name", types.String)},
		func(ctx context.Context, env *Env) ([]Row, error) {
			var rows []Row
			for _, t := range env.currentTables() {
				for i, id := range visiblePK(t) {
					rows = append(rows, Row{str(env.Database), str(catalog.PublicSchema), str(t.Name), boolean(false), str(catalog.PublicSchema), str(t.Name + "_pkey"), i64(int64(i + 1)), str(columnName(t, id))})
				}
				for _, idx := range t.Indexes {
					for i, id := range idx.ColumnIDs {
						rows = append(rows, Row{str(env.Database), str(catalog.PublicSchema), str(t.Name), boolean(!idx.Unique), str(catalog.PublicSchema), str(idx.Name), i64(int64(i + 1)), str(columnName(t, id))})
					}
				}
			}
			return rows, nil
		})
	is("role_table_grants", []catalog.Column{col("grantor", types.String), col("grantee", types.String), col("table_catalog", types.String), col("table_schema", types.String), col("table_name", types.String), col("privilege_type", types.String), col("is_grantable", types.String)},
		func(ctx context.Context, env *Env) ([]Row, error) {
			var rows []Row
			for _, t := range env.currentTables() {
				users := make([]string, 0, len(t.Privileges))
				for u := range t.Privileges {
					users = append(users, u)
				}
				sort.Strings(users)
				for _, u := range users {
					for _, p := range t.Privileges[u] {
						rows = append(rows, Row{str("root"), str(u), str(env.Database), str(catalog.PublicSchema), str(t.Name), str(p), str("NO")})
					}
				}
			}
			return rows, nil
		})
}

// visiblePK is the primary key without the hidden shard column.
// uniqueConstraintNameFor names the primary key or unique index that
// holds cols, for referential_constraints.
func uniqueConstraintNameFor(t *catalog.TableDescriptor, cols []catalog.ColumnID) string {
	same := func(key []catalog.ColumnID) bool {
		if len(key) != len(cols) {
			return false
		}
		set := map[catalog.ColumnID]bool{}
		for _, id := range cols {
			set[id] = true
		}
		for _, id := range key {
			if !set[id] {
				return false
			}
		}
		return true
	}
	if same(visiblePK(t)) {
		return t.Name + "_pkey"
	}
	for i := range t.Indexes {
		if t.Indexes[i].Unique && same(t.Indexes[i].ColumnIDs) {
			return t.Indexes[i].Name
		}
	}
	return ""
}

func visiblePK(t *catalog.TableDescriptor) []catalog.ColumnID {
	var out []catalog.ColumnID
	for _, id := range t.PrimaryKey {
		if c, ok := columnByID(t, id); ok && c.Hidden {
			continue
		}
		out = append(out, id)
	}
	return out
}

// attnums renders column IDs as their pg_attribute attnums (positions
// among the visible columns), space separated like int2vector.
func attnums(t *catalog.TableDescriptor, ids []catalog.ColumnID) string {
	pos := map[catalog.ColumnID]int{}
	n := 0
	for _, c := range t.Columns {
		if c.Hidden {
			continue
		}
		n++
		pos[c.ID] = n
	}
	out := ""
	for i, id := range ids {
		if i > 0 {
			out += " "
		}
		out += itoa(pos[id])
	}
	return out
}

// intArray renders an int2vector ("1 2") as an int2[] literal ("{1,2}").
func intArray(vec string) string {
	return "{" + strings.ReplaceAll(vec, " ", ",") + "}"
}

func itoa(n int) string {
	if n == 0 {
		return "0"
	}
	var b []byte
	for n > 0 {
		b = append([]byte{byte('0' + n%10)}, b...)
		n /= 10
	}
	return string(b)
}

// empty is the row function of a catalog that is always empty.
func empty(context.Context, *Env) ([]Row, error) { return nil, nil }
