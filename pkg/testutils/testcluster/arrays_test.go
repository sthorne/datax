package testcluster

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/sthorne/datax/pkg/server"
	"github.com/sthorne/datax/pkg/sql"
	"github.com/sthorne/datax/pkg/sql/catalog"
	"github.com/sthorne/datax/pkg/sql/types"
	"github.com/sthorne/datax/pkg/version"
)

// TestArrays (issue #96, part three): array columns of the scalar
// families — literals and ARRAY[...], subscripts, ANY / ALL, the
// containment and overlap operators, || concatenation, the array
// functions, unnest, array_agg, ordering and grouping, the catalogs,
// the wire through pgx (binary and text, results and parameters), the
// refusals (keys and indexes), and the v10 gate.
func TestArrays(t *testing.T) {
	tc := Start(t, 1)
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()
	s := sql.NewSession(tc.Nodes[0].DB(), catalog.NewAccessor())
	waitForDatabases(t, ctx, s)

	text := func(r *sql.Result) string {
		var b strings.Builder
		for _, row := range r.Rows {
			for i, d := range row {
				if i > 0 {
					b.WriteByte('|')
				}
				b.WriteString(d.Text())
			}
			b.WriteByte('\n')
		}
		return b.String()
	}
	one := func(q string) string {
		t.Helper()
		return strings.TrimSuffix(text(execSQL(t, ctx, s, q)), "\n")
	}
	refused := func(stmt, code string) {
		t.Helper()
		if _, serr := trySQL(ctx, s, stmt); serr == nil || serr.Code != code {
			t.Fatalf("%s: %v, want %s", stmt, serr, code)
		}
	}

	execSQL(t, ctx, s, `CREATE TABLE docs (id INT8 PRIMARY KEY, tags TEXT[], nums INT4[], prices DECIMAL(6,2)[], flags BOOL[], seen TIMESTAMPTZ[], small VARCHAR(3)[])`)
	execSQL(t, ctx, s, `INSERT INTO docs VALUES
		(1, '{a,b,"c d"}', '{1,2,3}', '{1.5,2}', '{t,f}', '{"2024-01-02 03:04:05Z"}', '{ab,c}'),
		(2, ARRAY['x', 'y'], ARRAY[10, 20], ARRAY[9.99], ARRAY[true], NULL, ARRAY['xy']),
		(3, '{}', '{NULL,5}', NULL, '{}', '{}', '{}'),
		(4, NULL, NULL, NULL, NULL, NULL, NULL)`)
	execSQL(t, ctx, s, `INSERT INTO docs (id, tags, nums) VALUES ($1, $2, $3)`, types.NewInt(5), types.NewString(`{"q r",s}`), types.NewString("{7}"))
	refused(`INSERT INTO docs (id, nums) VALUES (9, '{1,x}')`, sql.CodeInvalidTextRepresentation)
	refused(`INSERT INTO docs (id, nums) VALUES (9, '{3000000000}')`, sql.CodeNumericValueOutOfRange)
	refused(`INSERT INTO docs (id, small) VALUES (9, '{abcd}')`, "22001")
	refused(`INSERT INTO docs (id, tags) VALUES (9, 'not an array')`, sql.CodeInvalidTextRepresentation)

	got := text(execSQL(t, ctx, s, `SELECT id, tags, nums, prices, flags, seen, small FROM docs ORDER BY id`))
	want := "1|{a,b,\"c d\"}|{1,2,3}|{1.50,2.00}|{t,f}|{\"2024-01-02 03:04:05+00\"}|{ab,c}\n" +
		"2|{x,y}|{10,20}|{9.99}|{t}||{xy}\n" +
		"3|{}|{NULL,5}||{}|{}|{}\n" +
		"4||||||\n" +
		"5|{\"q r\",s}|{7}||||\n"
	if got != want {
		t.Fatalf("rows:\n%s\nwant:\n%s", got, want)
	}

	for _, c := range []struct{ q, want string }{
		{`SELECT tags[1], tags[3], tags[4], nums[2] + 1 FROM docs WHERE id = 1`, "a|c d||3"},
		{`SELECT id FROM docs WHERE 'b' = ANY(tags) OR 20 = ANY(nums) ORDER BY id`, "1\n2"},
		{`SELECT id FROM docs WHERE 5 < ALL(nums) ORDER BY id`, "2\n5"},
		{`SELECT id FROM docs WHERE tags @> '{a}' OR nums @> ARRAY[20] ORDER BY id`, "1\n2"},
		{`SELECT id FROM docs WHERE tags <@ ARRAY['x', 'y', 'z'] AND tags IS NOT NULL ORDER BY id`, "2\n3"},
		{`SELECT id FROM docs WHERE nums && ARRAY[3, 7] ORDER BY id`, "1\n5"},
		{`SELECT id FROM docs WHERE NOT (nums && ARRAY[3, 7]) ORDER BY id`, "2\n3"},
		{`SELECT tags || ARRAY['z'], tags || 'z', 'w' || tags, nums || nums FROM docs WHERE id = 2`, "{x,y,z}|{x,y,z}|{w,x,y}|{10,20,10,20}"},
		{`SELECT array_length(nums, 1), cardinality(nums), array_upper(nums, 1), array_lower(nums, 1), array_ndims(nums) FROM docs WHERE id = 1`, "3|3|3|1|1"},
		{`SELECT array_length(tags, 1), cardinality(tags) FROM docs WHERE id = 3`, "|0"},
		{`SELECT array_append(nums, 4), array_prepend(0, nums), array_cat(nums, ARRAY[9]), array_position(nums, 2), array_remove(nums, 2) FROM docs WHERE id = 1`, "{1,2,3,4}|{0,1,2,3}|{1,2,3,9}|2|{1,3}"},
		{`SELECT array_to_string(tags, ', '), array_to_string(nums, '-', 'x') FROM docs WHERE id = 3`, "|x-5"},
		{`SELECT string_to_array('a,b,,c', ','), string_to_array('abc', NULL), string_to_array('a,b', ',', 'b')`, "{a,b,\"\",c}|{a,b,c}|{a,NULL}"},
		{`SELECT ARRAY[1, 2] = '{1,2}', ARRAY[1, 2] < ARRAY[1, 3], ARRAY[1, 2]::text, '{1,2}'::int8[] || 3`, "t|t|{1,2}|{1,2,3}"},
		{`SELECT ARRAY[1, 2.5], ARRAY['2024-01-02'::date, '2024-02-03'], ARRAY[NULL, 1], ARRAY[]::int8[], ARRAY[now() > now()]`, "{1,2.5}|{2024-01-02,2024-02-03}|{NULL,1}|{}|{f}"},
		{`SELECT u FROM unnest(ARRAY[3, 1, 2]) AS u(u) ORDER BY u`, "1\n2\n3"},
		{`SELECT x * 2 FROM unnest('{1,2}'::int8[]) AS t(x) ORDER BY x`, "2\n4"},
		{`SELECT array_agg(id), array_agg(tags[1]) FROM docs`, "{1,2,3,4,5}|{a,x,NULL,NULL,\"q r\"}"},
		{`SELECT id, array_agg(x) FROM (SELECT 1 AS id, unnest('{1,2,3}'::int8[]) AS x) AS d GROUP BY id`, "1|{1,2,3}"},
		{`SELECT unnest(ARRAY[3, 1]), 'k'`, "3|k\n1|k"},
		{`SELECT count(*) FROM (SELECT unnest(ARRAY[]::int8[])) AS e`, "0"},
		{`SELECT array(SELECT id FROM docs WHERE id < 3 ORDER BY id)`, "{1,2}"},
		{`SELECT nums, count(*) FROM docs WHERE nums IS NOT NULL GROUP BY nums ORDER BY nums`, "{1,2,3}|1\n{7}|1\n{10,20}|1\n{NULL,5}|1"},
		{`SELECT id FROM docs WHERE nums IS NOT NULL ORDER BY nums DESC`, "3\n2\n5\n1"},
		{`SELECT id FROM docs WHERE tags = '{}' OR tags = ARRAY['x', 'y'] ORDER BY id`, "2\n3"},
		{`SELECT id FROM docs WHERE prices @> '{2}'`, "1"},
	} {
		if got := one(c.q); got != c.want {
			t.Errorf("%s: %q, want %q", c.q, got, c.want)
		}
	}
	execSQL(t, ctx, s, `UPDATE docs SET tags = array_append(tags, 'new'), nums = nums || 99 WHERE id = 2`)
	if got := one(`SELECT tags, nums FROM docs WHERE id = 2`); got != "{x,y,new}|{10,20,99}" {
		t.Fatalf("update: %q", got)
	}
	refused(`SELECT nums[1:2] FROM docs`, sql.CodeSyntaxError)
	refused(`CREATE TABLE bad (k INT8[] PRIMARY KEY)`, sql.CodeFeatureNotSupported)
	refused(`CREATE INDEX docs_tags ON docs (tags)`, sql.CodeFeatureNotSupported)
	refused(`CREATE TABLE bad (j JSONB[])`, sql.CodeSyntaxError)

	// Catalogs.
	create := execSQL(t, ctx, s, `SHOW CREATE TABLE docs`).Rows[0][1].S
	for _, w := range []string{"tags TEXT[]", "nums INT4[]", "prices NUMERIC(6,2)[]", "flags BOOL[]", "seen TIMESTAMPTZ[]", "small VARCHAR(3)[]"} {
		if !strings.Contains(create, w) {
			t.Fatalf("SHOW CREATE TABLE lacks %q:\n%s", w, create)
		}
	}
	if got := one(`SELECT column_name, data_type, udt_name FROM information_schema.columns WHERE table_name = 'docs' AND column_name IN ('tags', 'nums', 'prices') ORDER BY ordinal_position`); got !=
		"tags|ARRAY|_text\nnums|ARRAY|_int4\nprices|ARRAY|_numeric" {
		t.Fatalf("information_schema.columns: %q", got)
	}
	if got := one(`SELECT attname, atttypid, attndims, format_type(atttypid, atttypmod) FROM pg_attribute WHERE attrelid = 'docs'::regclass AND attname IN ('tags', 'nums', 'seen') ORDER BY attnum`); got != "tags|1009|1|text[]\nnums|1007|1|integer[]\nseen|1185|1|timestamp with time zone[]" {
		t.Fatalf("pg_attribute: %q", got)
	}
	if got := one(`SELECT t.typname, t.typelem, e.typname, e.typarray FROM pg_type t JOIN pg_type e ON e.oid = t.typelem WHERE t.oid IN (1016, 1009) ORDER BY t.oid`); got != "_text|25|text|1009\n_int8|20|int8|1016" {
		t.Fatalf("pg_type: %q", got)
	}

	// CREATE TABLE AS and LIKE keep the array types; ALTER COLUMN TYPE
	// from text.
	execSQL(t, ctx, s, `CREATE TABLE docs2 AS SELECT id, tags, nums || 1 AS more FROM docs`)
	if c := execSQL(t, ctx, s, `SHOW CREATE TABLE docs2`).Rows[0][1].S; !strings.Contains(c, "tags TEXT[]") || !strings.Contains(c, "more INT8[]") {
		t.Fatalf("CREATE TABLE AS:\n%s", c)
	}
	execSQL(t, ctx, s, `CREATE TABLE txt (id INT8 PRIMARY KEY, v TEXT)`)
	execSQL(t, ctx, s, `INSERT INTO txt VALUES (1, '{1,2}'), (2, '{}')`)
	execSQL(t, ctx, s, `ALTER TABLE txt ALTER COLUMN v TYPE INT8[]`)
	if got := one(`SELECT v[1], cardinality(v) FROM txt ORDER BY id`); got != "1|2\n|0" {
		t.Fatalf("after ALTER COLUMN TYPE: %q", got)
	}

	// The wire: Describe (1009 / 1007 / 1231), binary results into Go
	// slices, binary and text parameters, = ANY($1) with a slice.
	conn, err := pgx.Connect(ctx, pgURL(tc, 0))
	if err != nil {
		t.Fatalf("pgx connect: %v", err)
	}
	defer func() { _ = conn.Close(ctx) }()
	sd, err := conn.Prepare(ctx, "d", `SELECT tags, nums, prices, flags, seen, ARRAY[1, 2], array_agg(id) OVER (), tags[1] FROM docs`)
	if err != nil {
		t.Fatalf("prepare: %v", err)
	}
	wantOIDs := []uint32{1009, 1007, 1231, 1000, 1185, 1016, 1016, 25}
	for i, f := range sd.Fields {
		if f.DataTypeOID != wantOIDs[i] {
			t.Errorf("column %s describes as %d, want %d", f.Name, f.DataTypeOID, wantOIDs[i])
		}
	}
	var tags []string
	var nums []int32
	var flags []bool
	var seen []time.Time
	if err := conn.QueryRow(ctx, `SELECT tags, nums, flags, seen FROM docs WHERE id = 1`).Scan(&tags, &nums, &flags, &seen); err != nil {
		t.Fatalf("binary scan: %v", err)
	}
	if len(tags) != 3 || tags[2] != "c d" || len(nums) != 3 || nums[1] != 2 || len(flags) != 2 || flags[1] || len(seen) != 1 || !seen[0].Equal(time.Date(2024, 1, 2, 3, 4, 5, 0, time.UTC)) {
		t.Fatalf("binary values: %v %v %v %v", tags, nums, flags, seen)
	}
	var withNull []*int32
	if err := conn.QueryRow(ctx, `SELECT nums FROM docs WHERE id = 3`).Scan(&withNull); err != nil || len(withNull) != 2 || withNull[0] != nil || *withNull[1] != 5 {
		t.Fatalf("NULL element: %v %v", withNull, err)
	}
	if _, err := conn.Exec(ctx, `INSERT INTO docs (id, tags, nums, prices) VALUES ($1, $2, $3, $4)`, int64(10), []string{"p", "q r"}, []int32{4, 5}, []string{"1.25"}); err != nil {
		t.Fatalf("binary array params: %v", err)
	}
	if got := one(`SELECT tags, nums, prices FROM docs WHERE id = 10`); got != "{p,\"q r\"}|{4,5}|{1.25}" {
		t.Fatalf("rows from binary params: %q", got)
	}
	psd, err := conn.Prepare(ctx, "any", `SELECT id FROM docs WHERE id = ANY($1) ORDER BY id`)
	if err != nil || len(psd.ParamOIDs) != 1 || psd.ParamOIDs[0] != 1016 {
		t.Fatalf("= ANY($1) parameter: %v %v", psd.ParamOIDs, err)
	}
	rows, err := conn.Query(ctx, `SELECT id FROM docs WHERE id = ANY($1) ORDER BY id`, []int64{2, 5, 77})
	if err != nil {
		t.Fatalf("= ANY(slice): %v", err)
	}
	var ids []int64
	for rows.Next() {
		var id int64
		if err := rows.Scan(&id); err != nil {
			t.Fatal(err)
		}
		ids = append(ids, id)
	}
	rows.Close()
	if len(ids) != 2 || ids[0] != 2 || ids[1] != 5 {
		t.Fatalf("= ANY(slice): %v", ids)
	}
	var overlap int64
	if err := conn.QueryRow(ctx, `SELECT count(*) FROM docs WHERE tags && $1`, []string{"a", "p"}).Scan(&overlap); err != nil || overlap != 2 {
		t.Fatalf("&& $1: %d %v", overlap, err)
	}
	scfg, _ := pgx.ParseConfig(pgURL(tc, 0))
	scfg.DefaultQueryExecMode = pgx.QueryExecModeSimpleProtocol
	sconn, err := pgx.ConnectConfig(ctx, scfg)
	if err != nil {
		t.Fatalf("simple connect: %v", err)
	}
	defer func() { _ = sconn.Close(ctx) }()
	var ttags []string
	var tnums []int64
	if err := sconn.QueryRow(ctx, `SELECT tags, nums FROM docs WHERE id = 10`).Scan(&ttags, &tnums); err != nil || len(ttags) != 2 || ttags[1] != "q r" || len(tnums) != 2 || tnums[1] != 5 {
		t.Fatalf("text scan: %v %v %v", ttags, tnums, err)
	}
}

// TestArraysNeedV10: an array column is refused until the cluster
// version is finalized; array expressions work regardless.
func TestArraysNeedV10(t *testing.T) {
	tc := StartWithOptions(t, 1, func(c *server.Config) { c.BinaryVersionOverride = version.V9 })
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	s := sql.NewSession(tc.Nodes[0].DB(), catalog.NewAccessor())
	waitForDatabases(t, ctx, s)
	execSQL(t, ctx, s, `CREATE TABLE plain (id INT8 PRIMARY KEY, v TEXT)`)
	for _, stmt := range []string{`CREATE TABLE a (id INT8 PRIMARY KEY, t TEXT[])`, `ALTER TABLE plain ADD COLUMN t INT8[]`, `ALTER TABLE plain ALTER COLUMN v TYPE TEXT[]`} {
		if _, serr := trySQL(ctx, s, stmt); serr == nil || serr.Code != sql.CodeFeatureNotSupported {
			t.Fatalf("%s at v9: %v, want 0A000", stmt, serr)
		}
	}
	if r := execSQL(t, ctx, s, `SELECT ARRAY[1, 2] || 3, 2 = ANY(ARRAY[1, 2])`); r.Rows[0][0].Text() != "{1,2,3}" || !r.Rows[0][1].B {
		t.Fatalf("expressions at v9: %v", r.Rows)
	}
}
