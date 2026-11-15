package testcluster

import (
	"context"
	"encoding/json"
	"fmt"
	"math/rand"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/sthorne/datax/pkg/keys"
	"github.com/sthorne/datax/pkg/kvclient"
	"github.com/sthorne/datax/pkg/server"
	"github.com/sthorne/datax/pkg/sql"
	"github.com/sthorne/datax/pkg/sql/catalog"
	"github.com/sthorne/datax/pkg/sql/inverted"
	"github.com/sthorne/datax/pkg/sql/rowenc"
	"github.com/sthorne/datax/pkg/sql/types"
	"github.com/sthorne/datax/pkg/version"
)

// invertedEntries reads an inverted index raw and returns its keys, and
// the keys the table's rows say it should hold — every element of every
// row's document, once. Both are read in one transaction, so they are
// one snapshot, and intents of just-committed writers are resolved
// rather than tripped over.
func invertedEntries(t *testing.T, ctx context.Context, db *kvclient.DB, table, index string) (got, want []string) {
	t.Helper()
	err := db.RunTxn(ctx, "test-inverted-entries", func(ctx context.Context, txn *kvclient.Txn) error {
		got, want = nil, nil
		desc, err := catalog.NewAccessor().LookupFresh(ctx, txn, table)
		if err != nil {
			return err
		}
		var idx *catalog.IndexDescriptor
		for i := range desc.Indexes {
			if desc.Indexes[i].Name == index {
				idx = &desc.Indexes[i]
			}
		}
		if idx == nil {
			return fmt.Errorf("table %s has no index %s", table, index)
		}
		lo, hi := keys.TableIndexSpan(desc.ID, idx.ID)
		kvs, err := txn.Scan(ctx, lo, hi, 0)
		if err != nil {
			return err
		}
		for _, kv := range kvs {
			got = append(got, string(kv.Key))
		}
		plo, phi := rowenc.PrimarySpanFor(desc)
		rows, err := txn.Scan(ctx, plo, phi, 0)
		if err != nil {
			return err
		}
		for _, kv := range rows {
			row, err := rowenc.DecodeValue(desc, kv.Value)
			if err != nil {
				return err
			}
			pk, err := rowenc.DecodePK(desc, kv.Key)
			if err != nil {
				return err
			}
			for i, id := range desc.PrimaryKey {
				row[id] = pk[i]
			}
			entries, err := rowenc.IndexEntries(desc, idx, row)
			if err != nil {
				return err
			}
			for _, e := range entries {
				want = append(want, string(e.Key))
			}
		}
		return nil
	})
	if err != nil {
		t.Fatalf("reading index %s of %s: %v", index, table, err)
	}
	sort.Strings(want)
	return got, want
}

// checkInvertedIndex fails unless the index holds exactly the entries
// its table's rows imply. It returns the entry count.
func checkInvertedIndex(t *testing.T, ctx context.Context, db *kvclient.DB, table, index string) int {
	t.Helper()
	got, want := invertedEntries(t, ctx, db, table, index)
	if len(got) != len(want) {
		t.Fatalf("index %s holds %d entries, its rows imply %d", index, len(got), len(want))
	}
	for i := range got {
		if got[i] != want[i] {
			t.Fatalf("index %s entry %d is %s, its rows imply %s", index, i,
				keys.Pretty(keys.Key(got[i])), keys.Pretty(keys.Key(want[i])))
		}
	}
	return len(got)
}

// idsOf runs a query that selects id and returns the ids, sorted.
func idsOf(t *testing.T, ctx context.Context, s *sql.Session, query string) []int64 {
	t.Helper()
	res := execSQL(t, ctx, s, query)
	out := make([]int64, len(res.Rows))
	for i, r := range res.Rows {
		out[i] = r[0].I
	}
	sort.Slice(out, func(i, j int) bool { return out[i] < out[j] })
	return out
}

// sameBothWays runs the query through the inverted index and as a
// filter over the whole table and fails unless the rows agree; it
// returns them.
func sameBothWays(t *testing.T, ctx context.Context, s *sql.Session, query string) []int64 {
	t.Helper()
	execSQL(t, ctx, s, `SET inverted_index_scan = on`)
	if p := explainPlan(t, ctx, s, query); !strings.HasPrefix(p, "inverted scan of index") {
		t.Fatalf("%s: not served by the inverted index: %s", query, p)
	}
	indexed := idsOf(t, ctx, s, query)
	execSQL(t, ctx, s, `SET inverted_index_scan = off`)
	if p := explainPlan(t, ctx, s, query); strings.Contains(p, "inverted") {
		t.Fatalf("%s: inverted_index_scan = off still plans %s", query, p)
	}
	filtered := idsOf(t, ctx, s, query)
	execSQL(t, ctx, s, `RESET inverted_index_scan`)
	if fmt.Sprint(indexed) != fmt.Sprint(filtered) {
		t.Fatalf("%s:\n through the index: %v\n as a filter:       %v", query, indexed, filtered)
	}
	return indexed
}

func TestInvertedIndex(t *testing.T) {
	tc := Start(t, 3)
	ctx, cancel := context.WithTimeout(context.Background(), 240*time.Second)
	defer cancel()
	s := sql.NewSession(tc.Nodes[0].DB(), catalog.NewAccessor())
	db := tc.Nodes[0].DB()

	execSQL(t, ctx, s, `CREATE TABLE docs (id INT8 PRIMARY KEY, doc JSONB, note TEXT)`)
	execSQL(t, ctx, s, `INSERT INTO docs VALUES
		(1, '{"a": {"b": 1}, "tags": ["go", "db"], "n": 1}', 'x'),
		(2, '{"a": {"b": 2}, "tags": ["go"], "n": 1.0}', 'x'),
		(3, '{"a": {"b": 1, "c": null}, "tags": [], "n": "1"}', 'y'),
		(4, '[1, 2, "go"]', 'y'),
		(5, '"go"', 'y'),
		(6, NULL, 'z'),
		(7, '{}', 'z'),
		(8, '{"a": [{"b": 1}, {"c": 2}], "tags": {"go": true}}', 'z'),
		(9, '{"a": {}}', 'z'),
		(10, '1', 'z')`)

	// What is refused.
	for q, code := range map[string]string{
		`CREATE UNIQUE INDEX u ON docs USING GIN (doc)`:       sql.CodeFeatureNotSupported,
		`CREATE INDEX u ON docs USING GIN (doc, note)`:        sql.CodeFeatureNotSupported,
		`CREATE INDEX u ON docs USING GIN (note)`:             sql.CodeFeatureNotSupported,
		`CREATE INDEX u ON docs USING GIN (doc text_ops)`:     sql.CodeUndefinedObject,
		`CREATE INDEX u ON docs USING HASH (note)`:            sql.CodeFeatureNotSupported,
		`CREATE INDEX u ON docs (doc)`:                        sql.CodeFeatureNotSupported,
		`CREATE INDEX u ON docs USING BTREE (note jsonb_ops)`: sql.CodeFeatureNotSupported,
	} {
		if _, serr := trySQL(ctx, s, q); serr == nil || serr.Code != code {
			t.Fatalf("%s: got %v, want %s", q, serr, code)
		}
	}

	// Backfill over the rows already there.
	execSQL(t, ctx, s, `CREATE INDEX docs_gin ON docs USING GIN (doc)`)
	checkInvertedIndex(t, ctx, db, "docs", "docs_gin")

	// EXPLAIN names the index and the lookups.
	if p := explainPlan(t, ctx, s, `SELECT id FROM docs WHERE doc @> '{"a": {"b": 1}}'`); p != `inverted scan of index "docs_gin" ("a"."b" = 1) + primary key join, rows rechecked` {
		t.Fatalf("plan: %s", p)
	}
	if p := explainPlan(t, ctx, s, `SELECT id FROM docs WHERE doc ? 'tags'`); p != `inverted scan of index "docs_gin" (? "tags") + primary key join, rows rechecked` {
		t.Fatalf("plan: %s", p)
	}
	// What the index cannot serve is still a full scan.
	for _, q := range []string{
		`SELECT id FROM docs WHERE NOT (doc @> '{"n": 1}')`,
		`SELECT id FROM docs WHERE doc <@ '{"a": {"b": 1}, "x": 2}'`,
		`SELECT id FROM docs WHERE doc @> '{}'`,
		`SELECT id FROM docs WHERE doc @> '{"n": 1}' OR note = 'z'`,
		`SELECT id FROM docs WHERE doc->'a' @> '{"b": 1}'`,
		`SELECT id FROM docs WHERE note = 'x'`,
	} {
		if p := explainPlan(t, ctx, s, q); p != "full table scan" {
			t.Fatalf("%s: plan %s", q, p)
		}
	}
	// A point lookup still wins.
	if p := explainPlan(t, ctx, s, `SELECT id FROM docs WHERE id = 1 AND doc @> '{"n": 1}'`); p != "point lookup on primary key" {
		t.Fatalf("plan: %s", p)
	}

	// Results, against PostgreSQL's documented semantics, and the same
	// through the index as through the filter.
	for _, c := range []struct {
		where string
		want  []int64
	}{
		{`doc @> '{"a": {"b": 1}}'`, []int64{1, 3}},
		{`doc @> '{"n": 1}'`, []int64{1, 2}}, // 1 and 1.0, not "1"
		{`doc @> '{"n": "1"}'`, []int64{3}},  // the string
		{`doc @> '{"tags": ["go"]}'`, []int64{1, 2}},
		{`doc @> '{"tags": ["db", "go"]}'`, []int64{1}},
		{`doc @> '{"tags": []}'`, []int64{1, 2, 3}}, // any array, not the object of row 8
		{`doc @> '{"a": {}}'`, []int64{1, 2, 3, 9}}, // any object under a
		{`doc @> '{"a": {"c": null}}'`, []int64{3}},
		{`doc @> '[1]'`, []int64{4}},
		{`doc @> '1'`, []int64{4, 10}}, // a top-level array contains its scalar
		{`doc @> '"go"'`, []int64{4, 5}},
		{`doc @> '{"a": [{"b": 1}]}'`, []int64{8}},
		{`doc @> '{"a": [{"b": 1, "c": 2}]}'`, nil}, // b and c are in different elements
		{`doc @> '{"a": {"b": 1}}' AND note = 'y'`, []int64{3}},
		{`doc ? 'tags'`, []int64{1, 2, 3, 8}},
		{`doc ? 'go'`, []int64{4, 5}}, // an array's string element; the string itself
		{`doc ? 'b'`, nil},            // top level only
		{`doc ?| '{n,go}'`, []int64{1, 2, 3, 4, 5}},
		{`doc ?& '{a,n}'`, []int64{1, 2, 3}},
		{`doc ?& '{a,n}' AND doc @> '{"n": 1}'`, []int64{1, 2}},
		{`doc @> '{"n": "1"}' OR doc ? 'go'`, []int64{3, 4, 5}},
		{`doc @> '{"zzz": 1}'`, nil},
	} {
		q := `SELECT id FROM docs WHERE ` + c.where
		got := sameBothWays(t, ctx, s, q)
		if fmt.Sprint(got) != fmt.Sprint(append([]int64{}, c.want...)) {
			t.Fatalf("%s: got %v, want %v", q, got, c.want)
		}
	}
	// A parameter on the right-hand side is planned too.
	res := execSQL(t, ctx, s, `SELECT id FROM docs WHERE doc @> $1 ORDER BY id`, types.NewString(`{"a": {"b": 1}}`))
	if len(res.Rows) != 2 || res.Rows[0][0].I != 1 || res.Rows[1][0].I != 3 {
		t.Fatalf("parameterized containment: %+v", res.Rows)
	}
	// Rows come back in primary-key order, so ORDER BY id needs no sort.
	if p := explainPlan(t, ctx, s, `SELECT id FROM docs WHERE doc ? 'tags' ORDER BY id`); !strings.Contains(p, "order satisfied by access path") {
		t.Fatalf("plan: %s", p)
	}

	// Maintenance. An UPDATE that changes one value touches the entries
	// of that value only: n 1 -> 5 removes one entry and adds one.
	before, _ := invertedEntries(t, ctx, db, "docs", "docs_gin")
	execSQL(t, ctx, s, `UPDATE docs SET doc = '{"a": {"b": 1}, "tags": ["go", "db"], "n": 5}' WHERE id = 1`)
	after, _ := invertedEntries(t, ctx, db, "docs", "docs_gin")
	removed, added := diffStrings(before, after)
	if len(removed) != 1 || len(added) != 1 {
		t.Fatalf("one changed value moved %d entries out and %d in", len(removed), len(added))
	}
	checkInvertedIndex(t, ctx, db, "docs", "docs_gin")
	if got := sameBothWays(t, ctx, s, `SELECT id FROM docs WHERE doc @> '{"n": 1}'`); fmt.Sprint(got) != "[2]" {
		t.Fatalf("after update: %v", got)
	}
	if got := sameBothWays(t, ctx, s, `SELECT id FROM docs WHERE doc @> '{"n": 5}'`); fmt.Sprint(got) != "[1]" {
		t.Fatalf("after update: %v", got)
	}
	// An UPDATE of another column touches nothing.
	execSQL(t, ctx, s, `UPDATE docs SET note = 'q' WHERE id = 1`)
	if now, _ := invertedEntries(t, ctx, db, "docs", "docs_gin"); len(now) != len(after) {
		t.Fatalf("an unrelated update changed the index: %d entries, was %d", len(now), len(after))
	}
	// To NULL and back; a document with repeated values has one entry
	// for each distinct one.
	execSQL(t, ctx, s, `UPDATE docs SET doc = NULL WHERE id = 2`)
	execSQL(t, ctx, s, `UPDATE docs SET doc = '{"r": [7, 7, 7.0, 7]}' WHERE id = 6`)
	checkInvertedIndex(t, ctx, db, "docs", "docs_gin")
	if got := sameBothWays(t, ctx, s, `SELECT id FROM docs WHERE doc @> '{"r": [7]}'`); fmt.Sprint(got) != "[6]" {
		t.Fatalf("after update from NULL: %v", got)
	}
	execSQL(t, ctx, s, `DELETE FROM docs WHERE id = 3`)
	execSQL(t, ctx, s, `DELETE FROM docs WHERE doc ? 'go'`) // found through the index it then edits
	checkInvertedIndex(t, ctx, db, "docs", "docs_gin")
	if got := sameBothWays(t, ctx, s, `SELECT id FROM docs WHERE doc @> '{"a": {}}'`); fmt.Sprint(got) != "[1 9]" {
		t.Fatalf("after deletes: %v", got)
	}
	// A rolled-back write leaves the index as it was.
	n := checkInvertedIndex(t, ctx, db, "docs", "docs_gin")
	execSQL(t, ctx, s, `BEGIN`)
	execSQL(t, ctx, s, `INSERT INTO docs VALUES (50, '{"rolled": "back"}', 'r')`)
	if got := idsOf(t, ctx, s, `SELECT id FROM docs WHERE doc ? 'rolled'`); fmt.Sprint(got) != "[50]" {
		t.Fatalf("a transaction does not see its own entries: %v", got)
	}
	execSQL(t, ctx, s, `ROLLBACK`)
	if m := checkInvertedIndex(t, ctx, db, "docs", "docs_gin"); m != n {
		t.Fatalf("rollback left %d entries, was %d", m, n)
	}
	// A document with more elements than one row may have is refused,
	// not half indexed.
	var big strings.Builder
	big.WriteString("[")
	for i := 0; i <= inverted.MaxElementsPerValue; i++ {
		if i > 0 {
			big.WriteString(",")
		}
		fmt.Fprintf(&big, "%d", i)
	}
	big.WriteString("]")
	if _, serr := trySQL(ctx, s, `INSERT INTO docs VALUES (60, '`+big.String()+`', 'big')`); serr == nil || serr.Code != sql.CodeProgramLimitExceeded {
		t.Fatalf("oversized document: %v", serr)
	}

	// The catalogs show the index under its access method.
	res = execSQL(t, ctx, s, `SELECT indexdef FROM pg_indexes WHERE indexname = 'docs_gin'`)
	if len(res.Rows) != 1 || res.Rows[0][0].S != `CREATE INDEX docs_gin ON public.docs USING gin (doc)` {
		t.Fatalf("pg_indexes: %+v", res.Rows)
	}
	res = execSQL(t, ctx, s, `SELECT a.amname FROM pg_class c JOIN pg_am a ON a.oid = c.relam WHERE c.relname = 'docs_gin'`)
	if len(res.Rows) != 1 || res.Rows[0][0].S != "gin" {
		t.Fatalf("pg_class.relam: %+v", res.Rows)
	}

	// The other spellings PostgreSQL schemas use.
	execSQL(t, ctx, s, `CREATE INDEX IF NOT EXISTS docs_gin2 ON docs USING gin (doc jsonb_ops)`)
	execSQL(t, ctx, s, `CREATE INDEX docs_gin3 ON docs USING GIN (doc jsonb_path_ops)`)
	checkInvertedIndex(t, ctx, db, "docs", "docs_gin2")
	checkInvertedIndex(t, ctx, db, "docs", "docs_gin3")
	execSQL(t, ctx, s, `DROP INDEX docs_gin2`)
	execSQL(t, ctx, s, `DROP INDEX docs_gin3`)

	// Every node plans and reads through it.
	for i := 1; i < 3; i++ {
		si := sql.NewSession(tc.Nodes[i].DB(), catalog.NewAccessor())
		if got := sameBothWays(t, ctx, si, `SELECT id FROM docs WHERE doc @> '{"a": {}}'`); fmt.Sprint(got) != "[1 9]" {
			t.Fatalf("node %d: %v", i+1, got)
		}
	}

	// CREATE TABLE ... (LIKE ... INCLUDING INDEXES) copies the kind.
	execSQL(t, ctx, s, `CREATE TABLE docs2 (LIKE docs INCLUDING ALL)`)
	execSQL(t, ctx, s, `INSERT INTO docs2 SELECT id, doc, note FROM docs`)
	checkInvertedIndex(t, ctx, db, "docs2", "docs_gin")
	if p := explainPlan(t, ctx, s, `SELECT id FROM docs2 WHERE doc ? 'a'`); !strings.HasPrefix(p, "inverted scan") {
		t.Fatalf("copied index unused: %s", p)
	}

	// TRUNCATE empties the index with the table; rows written after it
	// are indexed again.
	execSQL(t, ctx, s, `TRUNCATE docs2`)
	if m := checkInvertedIndex(t, ctx, db, "docs2", "docs_gin"); m != 0 {
		t.Fatalf("truncate left %d entries", m)
	}
	execSQL(t, ctx, s, `INSERT INTO docs2 VALUES (1, '{"fresh": true}', '')`)
	if got := sameBothWays(t, ctx, s, `SELECT id FROM docs2 WHERE doc @> '{"fresh": true}'`); fmt.Sprint(got) != "[1]" {
		t.Fatalf("after truncate: %v", got)
	}

	// DROP INDEX: the planner stops using it and its entries go.
	var tableID, indexID uint64
	if err := db.RunTxn(ctx, "test-desc", func(ctx context.Context, txn *kvclient.Txn) error {
		d, err := catalog.NewAccessor().LookupFresh(ctx, txn, "docs")
		if err == nil {
			ix, _ := d.Index("docs_gin")
			tableID, indexID = d.ID, ix.ID
		}
		return err
	}); err != nil {
		t.Fatal(err)
	}
	execSQL(t, ctx, s, `DROP INDEX docs_gin`)
	if p := explainPlan(t, ctx, s, `SELECT id FROM docs WHERE doc ? 'a'`); p != "full table scan" {
		t.Fatalf("plan after drop: %s", p)
	}
	lo, hi := keys.TableIndexSpan(tableID, indexID)
	deadline := time.Now().Add(20 * time.Second)
	for {
		kvs, err := db.Scan(ctx, lo, hi, 0)
		if err != nil {
			t.Fatal(err)
		}
		if len(kvs) == 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("dropped index still holds %d entries", len(kvs))
		}
		time.Sleep(100 * time.Millisecond)
	}
	if got := idsOf(t, ctx, s, `SELECT id FROM docs WHERE doc @> '{"a": {}}'`); fmt.Sprint(got) != "[1 9]" {
		t.Fatalf("after drop: %v", got)
	}
}

func diffStrings(old, new []string) (removed, added []string) {
	in := map[string]bool{}
	for _, k := range new {
		in[k] = true
	}
	was := map[string]bool{}
	for _, k := range old {
		was[k] = true
		if !in[k] {
			removed = append(removed, k)
		}
	}
	for _, k := range new {
		if !was[k] {
			added = append(added, k)
		}
	}
	return removed, added
}

// randDoc builds a random document over a small vocabulary, so random
// queries match some rows and miss others.
func randDoc(r *rand.Rand, depth int) any {
	scalars := []any{nil, true, false, 0, 1, 2, 2.5, "a", "b", "1", "", strings.Repeat("long", 60)}
	if depth <= 0 || r.Intn(4) == 0 {
		return scalars[r.Intn(len(scalars))]
	}
	if r.Intn(3) == 0 {
		out := make([]any, r.Intn(4))
		for i := range out {
			out[i] = randDoc(r, depth-1)
		}
		return out
	}
	names := []string{"a", "b", "c", "d", "1"}
	out := map[string]any{}
	for i, n := 0, r.Intn(5); i < n; i++ {
		out[names[r.Intn(len(names))]] = randDoc(r, depth-1)
	}
	return out
}

func jsonText(t *testing.T, v any) string {
	t.Helper()
	b, err := json.Marshal(v)
	if err != nil {
		t.Fatal(err)
	}
	return strings.ReplaceAll(string(b), "'", "''")
}

// randPredicate builds a random indexable WHERE clause.
func randPredicate(t *testing.T, r *rand.Rand) string {
	one := func() string {
		switch r.Intn(6) {
		case 0:
			return fmt.Sprintf(`doc ? '%s'`, []string{"a", "b", "c", "1", "zz"}[r.Intn(5)])
		case 1:
			ks := []string{"a", "b", "c", "d", "zz"}
			r.Shuffle(len(ks), func(i, j int) { ks[i], ks[j] = ks[j], ks[i] })
			return fmt.Sprintf(`doc %s '{%s}'`, []string{"?|", "?&"}[r.Intn(2)], strings.Join(ks[:1+r.Intn(3)], ","))
		}
		return fmt.Sprintf(`doc @> '%s'`, jsonText(t, randDoc(r, 2)))
	}
	switch r.Intn(5) {
	case 0:
		return one() + " AND " + one()
	case 1:
		return one() + " OR " + one()
	}
	return one()
}

// TestInvertedIndexDifferential: over random documents, random
// containment and key predicates return the same rows through the index
// as through a filter of the whole table, before and after random
// updates and deletes; and the index holds exactly what the rows imply.
func TestInvertedIndexDifferential(t *testing.T) {
	tc := Start(t, 1)
	ctx, cancel := context.WithTimeout(context.Background(), 240*time.Second)
	defer cancel()
	s := sql.NewSession(tc.Nodes[0].DB(), catalog.NewAccessor())
	r := rand.New(rand.NewSource(300))

	execSQL(t, ctx, s, `CREATE TABLE rnd (id INT8 PRIMARY KEY, doc JSONB)`)
	const rows = 400
	insert := func(lo, hi int) {
		var vals []string
		for i := lo; i < hi; i++ {
			if r.Intn(25) == 0 {
				vals = append(vals, fmt.Sprintf("(%d, NULL)", i))
				continue
			}
			vals = append(vals, fmt.Sprintf("(%d, '%s')", i, jsonText(t, randDoc(r, 3))))
		}
		execSQL(t, ctx, s, `INSERT INTO rnd VALUES `+strings.Join(vals, ", "))
	}
	// Half before the index (backfilled), half after (maintained).
	insert(0, rows/2)
	execSQL(t, ctx, s, `CREATE INDEX rnd_gin ON rnd USING GIN (doc)`)
	insert(rows/2, rows)

	matched := 0
	round := func(queries int) {
		for i := 0; i < queries; i++ {
			q := `SELECT id FROM rnd WHERE ` + randPredicate(t, r)
			execSQL(t, ctx, s, `SET inverted_index_scan = on`)
			plan := explainPlan(t, ctx, s, q)
			indexed := idsOf(t, ctx, s, q)
			execSQL(t, ctx, s, `SET inverted_index_scan = off`)
			filtered := idsOf(t, ctx, s, q)
			if fmt.Sprint(indexed) != fmt.Sprint(filtered) {
				t.Fatalf("%s\n plan: %s\n through the index: %v\n as a filter:       %v", q, plan, indexed, filtered)
			}
			if strings.HasPrefix(plan, "inverted scan") && len(indexed) > 0 {
				matched++
			}
		}
	}
	round(250)
	checkInvertedIndex(t, ctx, tc.Nodes[0].DB(), "rnd", "rnd_gin")

	for i := 0; i < 150; i++ {
		id := r.Intn(rows)
		switch r.Intn(4) {
		case 0:
			execSQL(t, ctx, s, fmt.Sprintf(`DELETE FROM rnd WHERE id = %d`, id))
		case 1:
			execSQL(t, ctx, s, fmt.Sprintf(`UPDATE rnd SET doc = NULL WHERE id = %d`, id))
		default:
			execSQL(t, ctx, s, fmt.Sprintf(`UPDATE rnd SET doc = '%s' WHERE id = %d`, jsonText(t, randDoc(r, 3)), id))
		}
	}
	// A predicate-driven update and delete, located through the index.
	execSQL(t, ctx, s, `SET inverted_index_scan = on`)
	execSQL(t, ctx, s, `UPDATE rnd SET doc = '{"touched": true}' WHERE doc ? 'a' AND doc ? 'b'`)
	execSQL(t, ctx, s, `DELETE FROM rnd WHERE doc @> '{"c": 1}'`)
	checkInvertedIndex(t, ctx, tc.Nodes[0].DB(), "rnd", "rnd_gin")
	round(250)
	if matched < 50 {
		t.Fatalf("only %d of the random queries used the index and matched rows: the test is not exercising it", matched)
	}
}

// TestInvertedIndexBackfillUnderWrites: CREATE INDEX ... USING GIN on a
// populated table while other gateways insert, update and delete. When
// it returns, the index holds exactly what the rows imply — nothing a
// concurrent writer changed is missing or left behind.
func TestInvertedIndexBackfillUnderWrites(t *testing.T) {
	tc := Start(t, 3)
	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Second)
	defer cancel()
	// Every session is a gateway of its own, with a descriptor lease:
	// the build must drain the writers' leases before it backfills.
	const ttl = time.Second
	s := leasedSession(t, tc, 0, ttl)

	execSQL(t, ctx, s, `CREATE TABLE live (id INT8 PRIMARY KEY, doc JSONB)`)
	const seedRows = 600
	for lo := 0; lo < seedRows; lo += 100 {
		var vals []string
		for i := lo; i < lo+100; i++ {
			vals = append(vals, fmt.Sprintf(`(%d, '{"grp": %d, "tags": ["t%d", "t%d"], "seq": %d}')`, i, i%10, i%7, i%11, i))
		}
		execSQL(t, ctx, s, `INSERT INTO live VALUES `+strings.Join(vals, ", "))
	}

	var stop atomic.Bool
	var wg sync.WaitGroup
	var writes, failures atomic.Int64
	for w := 0; w < 3; w++ {
		ws := leasedSession(t, tc, w%3, ttl)
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			r := rand.New(rand.NewSource(int64(w) + 1))
			next := seedRows + w*100000
			for !stop.Load() {
				var q string
				switch r.Intn(4) {
				case 0:
					q = fmt.Sprintf(`INSERT INTO live VALUES (%d, '{"grp": %d, "tags": ["new"], "seq": %d}')`, next, r.Intn(10), next)
					next++
				case 1:
					// Deletes take the lower half of the seeded rows and
					// updates the upper half, so both kinds of row are
					// left to query however long the build runs.
					q = fmt.Sprintf(`DELETE FROM live WHERE id = %d`, r.Intn(seedRows/2))
				default:
					q = fmt.Sprintf(`UPDATE live SET doc = '{"grp": %d, "tags": ["t%d", "moved"], "w": %d}' WHERE id = %d`, r.Intn(10), r.Intn(7), w, seedRows/2+r.Intn(seedRows/2))
				}
				if _, serr := trySQL(ctx, ws, q); serr != nil {
					failures.Add(1)
				} else {
					writes.Add(1)
				}
			}
		}(w)
	}
	// Let the writers get going, build the index under them, and keep
	// them going a little after it is public.
	time.Sleep(300 * time.Millisecond)
	execSQL(t, ctx, s, `CREATE INDEX live_gin ON live USING GIN (doc)`)
	during := writes.Load()
	time.Sleep(300 * time.Millisecond)
	stop.Store(true)
	wg.Wait()
	if during == 0 {
		t.Fatal("no write committed while the index was being built")
	}
	t.Logf("%d writes committed during the build, %d after, %d failed", during, writes.Load()-during, failures.Load())

	checkInvertedIndex(t, ctx, tc.Nodes[0].DB(), "live", "live_gin")
	for _, q := range []string{
		`SELECT id FROM live WHERE doc @> '{"grp": 3}'`,
		`SELECT id FROM live WHERE doc @> '{"tags": ["moved"]}'`,
		`SELECT id FROM live WHERE doc @> '{"tags": ["new"]}'`,
		`SELECT id FROM live WHERE doc ? 'w'`,
		`SELECT id FROM live WHERE doc @> '{"tags": ["moved"]}' AND doc ? 'w'`,
	} {
		if got := sameBothWays(t, ctx, s, q); len(got) == 0 {
			t.Fatalf("%s matched nothing", q)
		}
	}
	// Whatever the writers left, any two-element containment agrees.
	for grp := 0; grp < 10; grp++ {
		for tag := 0; tag < 7; tag++ {
			sameBothWays(t, ctx, s, fmt.Sprintf(`SELECT id FROM live WHERE doc @> '{"grp": %d, "tags": ["t%d"]}'`, grp, tag))
		}
	}
}

// TestInvertedIndexSplitsAndRestart: the index spans several ranges
// (split inside the index's own keyspace), then every node restarts in
// turn; lookups, the recheck and maintenance keep working, on each node.
func TestInvertedIndexSplitsAndRestart(t *testing.T) {
	tc, engines := StartWithEngines(t, 3)
	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Second)
	defer cancel()
	s := sql.NewSession(tc.Nodes[0].DB(), catalog.NewAccessor())

	execSQL(t, ctx, s, `CREATE TABLE kept (id INT8 PRIMARY KEY, doc JSONB)`)
	execSQL(t, ctx, s, `CREATE INDEX kept_gin ON kept USING GIN (doc)`)
	for lo := 0; lo < 300; lo += 100 {
		var vals []string
		for i := lo; i < lo+100; i++ {
			vals = append(vals, fmt.Sprintf(`(%d, '{"grp": %d, "name": "n%d", "tags": ["t%d"]}')`, i, i%5, i, i%3))
		}
		execSQL(t, ctx, s, `INSERT INTO kept VALUES `+strings.Join(vals, ", "))
	}
	// Split the index at a few of its own entries, so one element's
	// entries and one lookup's elements fall in different ranges.
	got, _ := invertedEntries(t, ctx, tc.Nodes[0].DB(), "kept", "kept_gin")
	for _, at := range []int{len(got) / 4, len(got) / 2, 3 * len(got) / 4} {
		if _, err := tc.Nodes[0].DB().AdminSplit(ctx, keys.Key(got[at])); err != nil {
			t.Fatalf("split inside the index: %v", err)
		}
	}
	want := map[string]int{
		`SELECT id FROM kept WHERE doc @> '{"grp": 2}'`:                 60,
		`SELECT id FROM kept WHERE doc @> '{"grp": 2, "tags": ["t0"]}'`: 20,
		`SELECT id FROM kept WHERE doc @> '{"name": "n17"}'`:            1,
		`SELECT id FROM kept WHERE doc ?& '{grp,name,tags}'`:            300,
	}
	check := func(s *sql.Session, where string) {
		t.Helper()
		for q, n := range want {
			if got := sameBothWays(t, ctx, s, q); len(got) != n {
				t.Fatalf("%s: %s returned %d rows, want %d", where, q, len(got), n)
			}
		}
	}
	check(s, "after the splits")
	execSQL(t, ctx, s, `UPDATE kept SET doc = '{"grp": 2, "name": "moved", "tags": ["t0"]}' WHERE id = 1`)
	want[`SELECT id FROM kept WHERE doc @> '{"grp": 2}'`] = 61
	want[`SELECT id FROM kept WHERE doc @> '{"grp": 2, "tags": ["t0"]}'`] = 21
	checkInvertedIndex(t, ctx, tc.Nodes[0].DB(), "kept", "kept_gin")

	for i := 0; i < 3; i++ {
		tc.StopNode(i)
		n := tc.RestartNode(i, engines[i])
		si := sql.NewSession(n.DB(), catalog.NewAccessor())
		check(si, fmt.Sprintf("through node %d after its restart", i+1))
		// A write through the restarted node maintains the index.
		execSQL(t, ctx, si, fmt.Sprintf(`INSERT INTO kept VALUES (%d, '{"grp": 2, "name": "late", "tags": ["t0"]}')`, 1000+i))
		want[`SELECT id FROM kept WHERE doc @> '{"grp": 2}'`]++
		want[`SELECT id FROM kept WHERE doc @> '{"grp": 2, "tags": ["t0"]}'`]++
		want[`SELECT id FROM kept WHERE doc ?& '{grp,name,tags}'`]++
	}
	sn := sql.NewSession(tc.Node(0).DB(), catalog.NewAccessor())
	check(sn, "after every node restarted")
	checkInvertedIndex(t, ctx, tc.Node(0).DB(), "kept", "kept_gin")
}

// TestInvertedIndexReshard: a re-shard rebuilds every index at a shadow
// ID, because an entry's key ends in the primary key and the primary key
// starts with the shard bucket. An inverted index's many entries per row
// move with it, and are maintained by the writes made during and after.
func TestInvertedIndexReshard(t *testing.T) {
	n, _ := startGCNode(t)
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()
	s := sql.NewSession(n.DB(), catalog.NewAccessor())

	execSQL(t, ctx, s, `CREATE TABLE ev (series INT8, ts TIMESTAMPTZ, doc JSONB, PRIMARY KEY (series, ts))
		WITH (timeseries = true, shards = 2)`)
	execSQL(t, ctx, s, `CREATE INDEX ev_gin ON ev USING GIN (doc)`)
	for i := 0; i < 40; i++ {
		execSQL(t, ctx, s, `INSERT INTO ev VALUES ($1, $2, $3)`, types.NewInt(int64(i)), mustTS(t, 2026, 8, 30, i),
			types.NewString(fmt.Sprintf(`{"grp": %d, "tags": ["t%d", "all"]}`, i%4, i%3)))
	}
	checkInvertedIndex(t, ctx, n.DB(), "ev", "ev_gin")
	if got := sameBothWays(t, ctx, s, `SELECT series FROM ev WHERE doc @> '{"grp": 1}'`); len(got) != 10 {
		t.Fatalf("before the re-shard: %v", got)
	}

	execSQL(t, ctx, s, `ALTER TABLE ev SET (shards = 4)`)
	checkInvertedIndex(t, ctx, n.DB(), "ev", "ev_gin")
	if got := sameBothWays(t, ctx, s, `SELECT series FROM ev WHERE doc @> '{"grp": 1}'`); len(got) != 10 {
		t.Fatalf("after the re-shard: %v", got)
	}
	execSQL(t, ctx, s, `UPDATE ev SET doc = '{"grp": 1, "tags": ["moved"]}' WHERE series = 0`)
	execSQL(t, ctx, s, `DELETE FROM ev WHERE series = 1`)
	execSQL(t, ctx, s, `INSERT INTO ev VALUES (100, $1, '{"grp": 1}')`, mustTS(t, 2026, 8, 30, 50))
	checkInvertedIndex(t, ctx, n.DB(), "ev", "ev_gin")
	if got := sameBothWays(t, ctx, s, `SELECT series FROM ev WHERE doc @> '{"grp": 1}'`); len(got) != 11 {
		t.Fatalf("after writes to the re-sharded table: %v", got)
	}
	if got := sameBothWays(t, ctx, s, `SELECT series FROM ev WHERE doc @> '{"tags": ["all"]}' AND doc ? 'grp'`); len(got) != 38 {
		t.Fatalf("after writes to the re-sharded table: %v", got)
	}
}

// TestInvertedIndexBackupRestore: a table with an inverted index
// restores with the index usable and exact, full and incremental.
func TestInvertedIndexBackupRestore(t *testing.T) {
	tc := Start(t, 1)
	ctx, cancel := context.WithTimeout(context.Background(), 240*time.Second)
	defer cancel()
	s := sql.NewSession(tc.Nodes[0].DB(), catalog.NewAccessor())

	execSQL(t, ctx, s, `CREATE TABLE saved (id INT8 PRIMARY KEY, doc JSONB)`)
	execSQL(t, ctx, s, `CREATE INDEX saved_gin ON saved USING GIN (doc)`)
	for i := 0; i < 50; i++ {
		execSQL(t, ctx, s, fmt.Sprintf(`INSERT INTO saved VALUES (%d, '{"grp": %d, "tags": ["t%d"]}')`, i, i%5, i%3))
	}
	dirFull := t.TempDir()
	if _, err := tc.Nodes[0].RunBackup(ctx, dirFull, "", false, false); err != nil {
		t.Fatalf("full backup: %v", err)
	}
	// The incremental carries removed entries as well as new ones.
	execSQL(t, ctx, s, `UPDATE saved SET doc = '{"grp": 99}' WHERE id < 10`)
	execSQL(t, ctx, s, `DELETE FROM saved WHERE id >= 40`)
	execSQL(t, ctx, s, `INSERT INTO saved VALUES (100, '{"grp": 99, "tags": ["late"]}')`)
	dirInc := t.TempDir()
	if _, err := tc.Nodes[0].RunBackup(ctx, dirInc, dirFull, false, false); err != nil {
		t.Fatalf("incremental backup: %v", err)
	}

	tc2 := Start(t, 1)
	if _, err := tc2.Nodes[0].RunRestore(ctx, []string{dirFull, dirInc}); err != nil {
		t.Fatalf("restore: %v", err)
	}
	s2 := sql.NewSession(tc2.Nodes[0].DB(), catalog.NewAccessor())
	checkInvertedIndex(t, ctx, tc2.Nodes[0].DB(), "saved", "saved_gin")
	for _, q := range []string{
		`SELECT id FROM saved WHERE doc @> '{"grp": 99}'`,
		`SELECT id FROM saved WHERE doc @> '{"grp": 1}'`,
		`SELECT id FROM saved WHERE doc @> '{"tags": ["late"]}'`,
		`SELECT id FROM saved WHERE doc ? 'tags'`,
	} {
		src := sameBothWays(t, ctx, s, q)
		dst := sameBothWays(t, ctx, s2, q)
		if len(src) == 0 || fmt.Sprint(src) != fmt.Sprint(dst) {
			t.Fatalf("%s: source %v, restored %v", q, src, dst)
		}
	}
	// And the restored index is maintained.
	execSQL(t, ctx, s2, `INSERT INTO saved VALUES (200, '{"grp": 99}')`)
	if got := sameBothWays(t, ctx, s2, `SELECT id FROM saved WHERE doc @> '{"grp": 99}'`); len(got) != 12 {
		t.Fatalf("after a write to the restored table: %v", got)
	}
}

// TestInvertedIndexNeedsV21: until the cluster has finalized v21 a node
// may be running a binary that would maintain the index as an ordinary
// one, so none is created.
func TestInvertedIndexNeedsV21(t *testing.T) {
	tc := StartWithOptions(t, 1, func(c *server.Config) { c.BinaryVersionOverride = version.V20 })
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	s := sql.NewSession(tc.Nodes[0].DB(), catalog.NewAccessor())
	waitForDatabases(t, ctx, s)
	execSQL(t, ctx, s, `CREATE TABLE old (id INT8 PRIMARY KEY, doc JSONB, note TEXT)`)
	_, serr := trySQL(ctx, s, `CREATE INDEX old_gin ON old USING GIN (doc)`)
	if serr == nil || serr.Code != sql.CodeFeatureNotSupported || !strings.Contains(serr.Msg, "v21") {
		t.Fatalf("CREATE INDEX ... USING GIN at v20: %v", serr)
	}
	// Ordinary indexes, and the operators as filters, are untouched.
	execSQL(t, ctx, s, `CREATE INDEX old_note ON old (note)`)
	execSQL(t, ctx, s, `INSERT INTO old VALUES (1, '{"a": 1}', 'x')`)
	if got := idsOf(t, ctx, s, `SELECT id FROM old WHERE doc @> '{"a": 1}'`); fmt.Sprint(got) != "[1]" {
		t.Fatalf("containment as a filter at v20: %v", got)
	}
}

// BenchmarkJSONBContainment compares a containment query through an
// inverted index with the same query as a filter, over 50,000 documents
// of which the query matches 50:
//
//	go test ./pkg/testutils/testcluster -run - -bench JSONBContainment -benchtime 20x
func BenchmarkJSONBContainment(b *testing.B) {
	const docs, groups = 50000, 1000
	tc := Start(b, 1)
	ctx := context.Background()
	s := sql.NewSession(tc.Nodes[0].DB(), catalog.NewAccessor())
	exec := func(q string) *sql.Result {
		b.Helper()
		res, serr := trySQL(ctx, s, q)
		if serr != nil {
			b.Fatalf("%s: %v", q, serr)
		}
		return res
	}
	exec(`CREATE TABLE bench_docs (id INT8 PRIMARY KEY, doc JSONB)`)
	for lo := 0; lo < docs; lo += 500 {
		var vals []string
		for i := lo; i < lo+500; i++ {
			vals = append(vals, fmt.Sprintf(`(%d, '{"tenant": %d, "kind": "k%d", "tags": ["t%d", "t%d"], "attrs": {"seq": %d, "ok": %t}}')`,
				i, i%groups, i%7, i%50, (i/50)%50, i, i%2 == 0))
		}
		exec(`INSERT INTO bench_docs VALUES ` + strings.Join(vals, ", "))
	}
	start := time.Now()
	exec(`CREATE INDEX bench_docs_gin ON bench_docs USING GIN (doc)`)
	b.Logf("backfill of %d documents: %s", docs, time.Since(start).Round(time.Millisecond))

	for _, mode := range []string{"on", "off"} {
		name := "indexed"
		if mode == "off" {
			name = "filtered"
		}
		b.Run(name, func(b *testing.B) {
			exec(`SET inverted_index_scan = ` + mode)
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				res := exec(fmt.Sprintf(`SELECT id FROM bench_docs WHERE doc @> '{"tenant": %d}'`, i%groups))
				if len(res.Rows) != docs/groups {
					b.Fatalf("matched %d rows, want %d", len(res.Rows), docs/groups)
				}
			}
		})
	}
}
