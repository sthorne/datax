package sql

import (
	"strings"
	"testing"

	"github.com/sthorne/datax/pkg/keys"
	"github.com/sthorne/datax/pkg/kvpb"
	"github.com/sthorne/datax/pkg/sql/catalog"
	"github.com/sthorne/datax/pkg/sql/parser"
	"github.com/sthorne/datax/pkg/sql/rowenc"
	"github.com/sthorne/datax/pkg/sql/types"
)

// purgeWhereDesc is a tenant table: PRIMARY KEY (tenant, id), an index
// that leads with tenant, one that leads with (tenant, id), and one on a
// value column that no key prefix narrows.
func purgeWhereDesc() *catalog.TableDescriptor {
	return &catalog.TableDescriptor{
		ID: 60, Name: "e",
		Columns: []catalog.Column{
			{ID: 1, Name: "tenant", Type: types.String, NotNull: true},
			{ID: 2, Name: "id", Type: types.Int, NotNull: true},
			{ID: 3, Name: "v", Type: types.String},
		},
		PrimaryKey: []catalog.ColumnID{1, 2},
		Indexes: []catalog.IndexDescriptor{
			{ID: 2, Name: "e_tenant_v", ColumnIDs: []catalog.ColumnID{1, 3}},
			{ID: 3, Name: "e_v", ColumnIDs: []catalog.ColumnID{3}},
			{ID: 4, Name: "e_tenant_id_v", ColumnIDs: []catalog.ColumnID{1, 2, 3}, Unique: true},
		},
	}
}

func purgeConds(t *testing.T, where string) []parser.Comparison {
	t.Helper()
	stmts, err := parser.Parse(`ALTER TABLE e PURGE WHERE ` + where)
	if err != nil {
		t.Fatalf("%s: %v", where, err)
	}
	return stmts[0].(*parser.AlterTable).PurgeWhere
}

func spansCover(spans []keySpan, k keys.Key) bool {
	for _, sp := range spans {
		if sp.start.Compare(k) <= 0 && k.Compare(sp.end) < 0 {
			return true
		}
	}
	return false
}

// rowKeys are every key a row (tenant, id, v) of purgeWhereDesc has, by
// index name ("primary" for the row itself).
func rowKeys(t *testing.T, desc *catalog.TableDescriptor, tenant string, id int64, v string) map[string]keys.Key {
	t.Helper()
	row := map[catalog.ColumnID]types.Datum{1: types.NewString(tenant), 2: types.NewInt(id), 3: types.NewString(v)}
	out := map[string]keys.Key{}
	pk, err := rowenc.EncodePK(desc, []types.Datum{row[1], row[2]})
	if err != nil {
		t.Fatal(err)
	}
	out["primary"] = pk
	for i := range desc.Indexes {
		idx := &desc.Indexes[i]
		k, _, skip, err := rowenc.EncodeIndexEntry(desc, idx, row)
		if err != nil || skip {
			t.Fatalf("index %s: %v %v", idx.Name, skip, err)
		}
		out[idx.Name] = k
	}
	return out
}

// TestPurgeWhereSpans (issue #375): the spans a PURGE WHERE purges hold
// every key of the rows it names — the row and its entry in every index
// — and, in the primary index and the indexes that lead with the named
// columns, no key of any other row. An index the predicate cannot narrow
// is purged whole and named in a notice.
func TestPurgeWhereSpans(t *testing.T) {
	desc := purgeWhereDesc()
	type rowCase struct {
		tenant string
		id     int64
		purged bool
	}
	for _, tc := range []struct {
		where   string
		rows    []rowCase
		notices []string
		// partial names the indexes narrowed to a prefix only: they hold
		// the other rows of that prefix too.
		partial map[string]bool
	}{
		{
			where:   `tenant = 'a'`,
			rows:    []rowCase{{"a", 1, true}, {"a", 99, true}, {"b", 1, false}, {"", 1, false}, {"a\x00", 1, false}, {"ab", 1, false}},
			notices: []string{`index "e_v" does not begin`},
		},
		{
			where:   `(tenant, id) = ('a', 5)`,
			rows:    []rowCase{{"a", 5, true}, {"a", 4, false}, {"a", 6, false}, {"b", 5, false}},
			notices: []string{`index "e_tenant_v" begins with only part`, `index "e_v" does not begin`},
			partial: map[string]bool{"e_tenant_v": true},
		},
		{
			where:   `tenant = 'a' AND id BETWEEN 5 AND 6`,
			rows:    []rowCase{{"a", 4, false}, {"a", 5, true}, {"a", 6, true}, {"a", 7, false}, {"b", 5, false}},
			notices: []string{`index "e_tenant_v" begins with only part`, `index "e_v" does not begin`},
			partial: map[string]bool{"e_tenant_v": true},
		},
		{
			where:   `tenant = 'a' AND id >= 10 AND id < 12`,
			rows:    []rowCase{{"a", 9, false}, {"a", 10, true}, {"a", 11, true}, {"a", 12, false}},
			notices: []string{`index "e_tenant_v" begins with only part`, `index "e_v" does not begin`},
			partial: map[string]bool{"e_tenant_v": true},
		},
		{
			where:   `tenant = 'a' AND 3 < id AND id <= 4`,
			rows:    []rowCase{{"a", 3, false}, {"a", 4, true}, {"a", 5, false}},
			notices: []string{`index "e_tenant_v" begins with only part`, `index "e_v" does not begin`},
			partial: map[string]bool{"e_tenant_v": true},
		},
		{
			where:   `tenant >= 'b' AND tenant < 'd'`,
			rows:    []rowCase{{"a", 1, false}, {"b", 1, true}, {"c", 1, true}, {"cz", 9, true}, {"d", 1, false}},
			notices: []string{`index "e_v" does not begin`},
		},
		{
			where:   `tenant > 'b'`,
			rows:    []rowCase{{"b", 1, false}, {"b\x00", 1, true}, {"c", 1, true}, {"zzz", 1, true}},
			notices: []string{`index "e_v" does not begin`},
		},
	} {
		spans, notices, serr := purgeWhereSpans(desc, purgeConds(t, tc.where), nil)
		if serr != nil {
			t.Fatalf("%s: %v", tc.where, serr)
		}
		for i := 1; i < len(spans); i++ {
			if spans[i-1].end.Compare(spans[i].start) >= 0 {
				t.Fatalf("%s: spans overlap or touch: %v", tc.where, spans)
			}
		}
		if len(notices) != len(tc.notices) {
			t.Errorf("%s: notices %q, want %d", tc.where, notices, len(tc.notices))
		}
		for i, want := range tc.notices {
			if i < len(notices) && !strings.Contains(notices[i], want) {
				t.Errorf("%s: notice %q, want %q", tc.where, notices[i], want)
			}
		}
		for _, rc := range tc.rows {
			for name, k := range rowKeys(t, desc, rc.tenant, rc.id, "val") {
				got := spansCover(spans, k)
				switch {
				case rc.purged && !got:
					t.Errorf("%s: row (%q, %d): its %s key is not purged", tc.where, rc.tenant, rc.id, name)
				case !rc.purged && got && name != "e_v" && !(tc.partial[name] && rc.tenant == "a"):
					t.Errorf("%s: row (%q, %d): its %s key is purged", tc.where, rc.tenant, rc.id, name)
				case name == "e_v" && !got:
					t.Errorf("%s: the unnarrowed index's key for (%q, %d) is not purged", tc.where, rc.tenant, rc.id)
				}
			}
		}
		// The table's auxiliary keys, and an index ID the descriptor does
		// not list (a layout being written or retired), are purged whole.
		for _, k := range []keys.Key{keys.TableIndexPrefix(desc.ID, keys.TableAuxIndexID), keys.TableIndexPrefix(desc.ID, 9)} {
			if !spansCover(spans, k) {
				t.Errorf("%s: %s is not purged", tc.where, keys.Pretty(k))
			}
		}
		// Nothing outside the table.
		tlo, thi := keys.TableDataSpan(desc.ID)
		if spans[0].start.Compare(tlo) < 0 || spans[len(spans)-1].end.Compare(thi) > 0 {
			t.Errorf("%s: spans leave the table: %v", tc.where, spans)
		}
	}
}

// TestPurgeWhereSharded: on a sharded table the hidden bucket column
// leads the key, so a prefix of the logical key is one span per bucket,
// and the whole logical key is the one bucket its row hashes to.
func TestPurgeWhereSharded(t *testing.T) {
	desc := &catalog.TableDescriptor{
		ID: 61, Name: "m", Timeseries: true, ShardBuckets: 4,
		Columns: []catalog.Column{
			{ID: 1, Name: rowenc.ShardColumnName, Type: types.Int, NotNull: true, Hidden: true},
			{ID: 2, Name: "tenant", Type: types.String, NotNull: true},
			{ID: 3, Name: "ts", Type: types.Timestamp, NotNull: true},
		},
		PrimaryKey: []catalog.ColumnID{1, 2, 3},
	}
	key := func(tenant string, ts int64) keys.Key {
		logical := []types.Datum{types.NewString(tenant), types.NewTimestamp(ts)}
		b, err := rowenc.ShardBucket(desc, logical)
		if err != nil {
			t.Fatal(err)
		}
		k, err := rowenc.EncodePK(desc, append([]types.Datum{b}, logical...))
		if err != nil {
			t.Fatal(err)
		}
		return k
	}
	spans, _, serr := purgeWhereSpans(desc, purgeConds(t, `tenant = 'a'`), nil)
	if serr != nil {
		t.Fatal(serr)
	}
	for ts := int64(0); ts < 40; ts++ {
		if !spansCover(spans, key("a", ts*1e9)) {
			t.Fatalf("tenant a's row at %d is not purged", ts)
		}
		if spansCover(spans, key("b", ts*1e9)) {
			t.Fatalf("tenant b's row at %d is purged", ts)
		}
	}
	spans, _, serr = purgeWhereSpans(desc, purgeConds(t, `(tenant, ts) = ('a', '1970-01-01 00:00:07+00')`), nil)
	if serr != nil {
		t.Fatal(serr)
	}
	if !spansCover(spans, key("a", 7e9)) || spansCover(spans, key("a", 8e9)) {
		t.Fatalf("one row of a sharded table: %v", spans)
	}
	if _, _, serr := purgeWhereSpans(desc, purgeConds(t, `_shard = 1`), nil); serr == nil || serr.Code != CodeInvalidColumnReference {
		t.Fatalf("the hidden bucket column: %v", serr)
	}
}

// TestPurgeWhereRefusals: predicates that do not name a primary-key
// prefix or range are refused with a SQLSTATE and a hint that names the
// key, and none of them purges anything.
func TestPurgeWhereRefusals(t *testing.T) {
	desc := purgeWhereDesc()
	for _, tc := range []struct {
		where, code, msg string
	}{
		{`v = 'x'`, CodeInvalidColumnReference, `column "v" is not a primary-key column`},
		{`id = 1`, CodeInvalidColumnReference, `"tenant" comes before it`},
		{`id BETWEEN 1 AND 2`, CodeInvalidColumnReference, `needs an equality on every primary-key column before it`},
		{`tenant = 'a' OR tenant = 'b'`, CodeFeatureNotSupported, `IN is not supported`},
		{`tenant = 'a' OR id = 1`, CodeFeatureNotSupported, `OR is not supported`},
		{`tenant IN ('a', 'b')`, CodeFeatureNotSupported, `IN is not supported`},
		{`tenant = lower('A')`, CodeFeatureNotSupported, `constant or a parameter`},
		{`lower(tenant) = 'a'`, CodeFeatureNotSupported, `not a comparison`},
		{`tenant = 'a' AND id = id`, CodeFeatureNotSupported, `constant or a parameter`},
		{`tenant = (SELECT 'a')`, CodeFeatureNotSupported, `constant or a parameter`},
		{`tenant != 'a'`, CodeFeatureNotSupported, `operator != `},
		{`tenant LIKE 'a%'`, CodeFeatureNotSupported, `not supported`},
		{`tenant IS NULL`, CodeFeatureNotSupported, `not supported`},
		{`tenant = 'a' AND tenant = 'b'`, CodeFeatureNotSupported, `equality twice`},
		{`tenant = 'a' AND tenant > 'b'`, CodeFeatureNotSupported, `both an equality and a range`},
		{`tenant = 'a' AND id > 1 AND id > 2`, CodeFeatureNotSupported, `two lower bounds`},
		{`tenant = 'a' AND id = 1.5`, CodeInvalidParameterValue, `not a value of column "id"`},
		{`tenant = 'a' AND id = NULL`, CodeInvalidParameterValue, `NULL`},
		{`tenant = 'a' AND id BETWEEN 6 AND 5`, CodeInvalidParameterValue, `is empty`},
		{`tenant = 'a' AND id = 'x'`, CodeInvalidTextRepresentation, `column "id"`},
		{`nope = 1`, CodeUndefinedColumn, `column "nope" does not exist`},
		{`TRUE`, CodeFeatureNotSupported, `not a comparison`},
	} {
		_, _, serr := purgeWhereSpans(desc, purgeConds(t, tc.where), nil)
		if serr == nil {
			t.Errorf("%s: accepted", tc.where)
			continue
		}
		if serr.Code != tc.code || !strings.Contains(serr.Msg, tc.msg) {
			t.Errorf("%s: %s %q, want %s containing %q", tc.where, serr.Code, serr.Msg, tc.code, tc.msg)
		}
		if serr.Code != CodeUndefinedColumn && serr.Code != CodeInvalidTextRepresentation && !strings.Contains(serr.Hint, `(tenant, id)`) {
			t.Errorf("%s: hint %q does not name the primary key", tc.where, serr.Hint)
		}
	}
	// A parameter is a value like a literal.
	spans, _, serr := purgeWhereSpans(desc, purgeConds(t, `tenant = $1 AND id < $2`), []types.Datum{types.NewString("a"), types.NewInt(3)})
	if serr != nil {
		t.Fatal(serr)
	}
	if !spansCover(spans, rowKeys(t, desc, "a", 2, "x")["primary"]) || spansCover(spans, rowKeys(t, desc, "a", 3, "x")["primary"]) {
		t.Fatalf("parameters: %v", spans)
	}
	// A table keyed by its hidden rowid has no key to name.
	rowid := &catalog.TableDescriptor{
		ID: 62, Name: "h",
		Columns: []catalog.Column{
			{ID: 1, Name: "rowid", Type: types.Int, NotNull: true, Hidden: true},
			{ID: 2, Name: "tenant", Type: types.String},
		},
		PrimaryKey: []catalog.ColumnID{1},
	}
	if _, _, serr := purgeWhereSpans(rowid, purgeConds(t, `tenant = 'a'`), nil); serr == nil || serr.Code != CodeInvalidColumnReference || !strings.Contains(serr.Hint, "no declared primary key") {
		t.Fatalf("rowid table: %v", serr)
	}
}

// TestMergePurgeReportHeld checks that a backup holding back any span of
// a PURGE WHERE holds back the statement's report (issue #376): the
// backups once each, the oldest stop, and a rerun only if every held
// span reruns.
func TestMergePurgeReportHeld(t *testing.T) {
	all := &kvpb.PurgeReport{Complete: true}
	mergePurgeReport(all, &kvpb.PurgeReport{Complete: true, Ranges: 1, VersionsRemoved: 2})
	if len(all.HeldBy) != 0 || all.Rerun || !all.Complete {
		t.Fatalf("unheld span: %+v", all)
	}
	mergePurgeReport(all, &kvpb.PurgeReport{HeldBy: []string{"b1"}, HeldBelowNanos: 50, Rerun: true})
	mergePurgeReport(all, &kvpb.PurgeReport{HeldBy: []string{"b1", "b2"}, HeldBelowNanos: 30, Rerun: true})
	mergePurgeReport(all, &kvpb.PurgeReport{HeldBy: []string{"b2"}, HeldBelowNanos: 40, Rerun: true})
	if all.Complete || all.Ranges != 1 || all.VersionsRemoved != 2 {
		t.Fatalf("totals: %+v", all)
	}
	if strings.Join(all.HeldBy, ",") != "b1,b2" || all.HeldBelowNanos != 30 || !all.Rerun {
		t.Fatalf("held: %+v", all)
	}
	mergePurgeReport(all, &kvpb.PurgeReport{HeldBy: []string{"b3"}, HeldBelowNanos: 60})
	if all.Rerun || all.HeldBelowNanos != 30 || len(all.HeldBy) != 3 {
		t.Fatalf("a span without a rerun: %+v", all)
	}
}
