package parser

import (
	"reflect"
	"testing"
)

// TestParseVacuum (PG-O-003): both of VACUUM's forms.
func TestParseVacuum(t *testing.T) {
	for src, want := range map[string]Vacuum{
		`VACUUM`:                                {},
		`VACUUM FULL`:                           {Full: true},
		`VACUUM FULL FREEZE VERBOSE ANALYZE t`:  {Full: true, Freeze: true, Verbose: true, Analyze: true, Tables: []VacuumTable{{Name: "t"}}},
		`VACUUM ANALYZE VERBOSE t (a, b), u`:    {Analyze: true, Verbose: true, Tables: []VacuumTable{{Name: "t", Columns: []string{"a", "b"}}, {Name: "u"}}},
		`VACUUM (VERBOSE, ANALYZE) db.t`:        {Verbose: true, Analyze: true, Tables: []VacuumTable{{Name: "db.t"}}},
		`VACUUM (FULL off, INDEX_CLEANUP auto)`: {},
	} {
		got, ok := parseOne(t, src).(*Vacuum)
		if !ok || !reflect.DeepEqual(*got, want) {
			t.Errorf("%s: %+v, want %+v", src, got, want)
		}
	}
	for _, bad := range []string{`VACUUM (nosuch) t`, `VACUUM t (a)`, `VACUUM (ANALYZE maybe)`, `VACUUM (PARALLEL)`} {
		if _, err := Parse(bad); err == nil {
			t.Errorf("%s accepted", bad)
		}
	}
}

// TestParseAsOfForms (CR-AOST-001).
func TestParseAsOfForms(t *testing.T) {
	sel := parseOneSelect(t, `SELECT * FROM t AS OF SYSTEM TIME follower_read_timestamp()`)
	if sel.AsOf != FollowerReadTimestamp {
		t.Errorf("follower_read_timestamp(): %q", sel.AsOf)
	}
	sel = parseOneSelect(t, `SELECT * FROM t AS OF SYSTEM TIME with_min_timestamp('2026-01-01T00:00:00Z', TRUE)`)
	if sel.AsOfMinTimestamp != "2026-01-01T00:00:00Z" || !sel.AsOfNearestOnly {
		t.Errorf("with_min_timestamp: %+v", sel)
	}
	sel = parseOneSelect(t, `SELECT * FROM t AS OF SYSTEM TIME with_max_staleness('10s', FALSE)`)
	if sel.AsOfMaxStaleness != "10s" || sel.AsOfNearestOnly {
		t.Errorf("with_max_staleness: %+v", sel)
	}
	b := parseOne(t, `BEGIN TRANSACTION AS OF SYSTEM TIME '-5s' READ ONLY`).(*Begin)
	if b.AsOf == nil || b.AsOf.Exact != "-5s" || len(b.Modes) != 1 {
		t.Errorf("BEGIN AS OF SYSTEM TIME: %+v", b)
	}
	st := parseOne(t, `SET TRANSACTION AS OF SYSTEM TIME follower_read_timestamp()`).(*SetTransactionAsOf)
	if st.AsOf.Exact != FollowerReadTimestamp {
		t.Errorf("SET TRANSACTION AS OF SYSTEM TIME: %+v", st)
	}
	for _, bad := range []string{
		`SET SESSION CHARACTERISTICS AS TRANSACTION AS OF SYSTEM TIME '-5s'`,
		`SET TRANSACTION READ ONLY, AS OF SYSTEM TIME '-5s'`,
		`SELECT * FROM t AS OF SYSTEM TIME with_max_staleness('10s', 1)`,
		`SELECT * FROM t AS OF SYSTEM TIME now()`,
	} {
		if _, err := Parse(bad); err == nil {
			t.Errorf("%s accepted", bad)
		}
	}
}

// TestParseDatabaseOptions (PG-D-022).
func TestParseDatabaseOptions(t *testing.T) {
	cd := parseOne(t, `CREATE DATABASE d WITH OWNER = alice ENCODING 'UTF8' TEMPLATE template0 CONNECTION LIMIT = -1 (replicas = 3)`).(*CreateDatabase)
	want := []DatabaseOption{{Name: "owner", Value: "alice"}, {Name: "encoding", Value: "UTF8"}, {Name: "template", Value: "template0"}, {Name: "connection_limit", Value: "-1"}}
	if !reflect.DeepEqual(cd.Options, want) || cd.Placement == nil || cd.Placement.Replicas != 3 {
		t.Errorf("CREATE DATABASE: %+v %+v", cd.Options, cd.Placement)
	}
	ad := parseOne(t, `ALTER DATABASE d SET search_path TO public`).(*AlterDatabase)
	if ad.Set == nil || ad.Set.Name != "search_path" || ad.Placement != nil {
		t.Errorf("ALTER DATABASE SET: %+v", ad)
	}
	ad = parseOne(t, `ALTER DATABASE d SET replicas = 5`).(*AlterDatabase)
	if ad.Placement == nil || ad.Placement.Replicas != 5 {
		t.Errorf("ALTER DATABASE SET placement: %+v", ad)
	}
	ad = parseOne(t, `ALTER DATABASE d RESET ALL`).(*AlterDatabase)
	if ad.Set == nil || !ad.Set.Reset || ad.Set.Name != "" {
		t.Errorf("RESET ALL: %+v", ad)
	}
	ad = parseOne(t, `ALTER DATABASE d WITH CONNECTION LIMIT 3`).(*AlterDatabase)
	if len(ad.Options) != 1 || ad.Options[0] != (DatabaseOption{Name: "connection_limit", Value: "3"}) {
		t.Errorf("CONNECTION LIMIT: %+v", ad)
	}
	for _, bad := range []string{`CREATE DATABASE d FOO = 1`, `CREATE DATABASE d CONNECTION 3`, `ALTER DATABASE d FROB`} {
		if _, err := Parse(bad); err == nil {
			t.Errorf("%s accepted", bad)
		}
	}
}
