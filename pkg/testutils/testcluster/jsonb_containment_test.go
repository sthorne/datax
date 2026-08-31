package testcluster

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
)

// TestJSONBContainmentOverPgwire: the @> operator end to end — structural
// containment with numeric scalar comparison, NOT, paths on the left,
// parameters in both wire formats, and the documented refusals. Issue #40.
func TestJSONBContainmentOverPgwire(t *testing.T) {
	tc := Start(t, 3)
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	conn, err := pgx.Connect(ctx, pgURL(tc, 0))
	if err != nil {
		t.Fatalf("pgx connect: %v", err)
	}
	defer func() { _ = conn.Close(ctx) }()

	if _, err := conn.Exec(ctx, `CREATE TABLE docs (id INT8 PRIMARY KEY, j JSONB)`); err != nil {
		t.Fatalf("create: %v", err)
	}
	if _, err := conn.Exec(ctx, `INSERT INTO docs VALUES
		(1, '{"tags":["go","db"],"meta":{"level":3,"ok":true}}'),
		(2, '{"tags":["rust"],"meta":{"level":1.0}}'),
		(3, '[1,2,{"k":"v"}]'),
		(4, NULL)`); err != nil {
		t.Fatalf("insert: %v", err)
	}

	ids := func(q string, args ...any) []int64 {
		t.Helper()
		rows, err := conn.Query(ctx, q, args...)
		if err != nil {
			t.Fatalf("%s: %v", q, err)
		}
		defer rows.Close()
		var out []int64
		for rows.Next() {
			var id int64
			if err := rows.Scan(&id); err != nil {
				t.Fatal(err)
			}
			out = append(out, id)
		}
		if rows.Err() != nil {
			t.Fatalf("%s: %v", q, rows.Err())
		}
		return out
	}

	// Object containment with a literal RHS.
	if got := ids(`SELECT id FROM docs WHERE j @> '{"meta":{"ok":true}}' ORDER BY id`); len(got) != 1 || got[0] != 1 {
		t.Fatalf("object containment: %v", got)
	}
	// Array element containment inside an object.
	if got := ids(`SELECT id FROM docs WHERE j @> '{"tags":["go"]}' ORDER BY id`); len(got) != 1 || got[0] != 1 {
		t.Fatalf("array-in-object: %v", got)
	}
	// Numeric scalar comparison: level 1.0 contains level 1.
	if got := ids(`SELECT id FROM docs WHERE j @> '{"meta":{"level":1}}' ORDER BY id`); len(got) != 1 || got[0] != 2 {
		t.Fatalf("numeric containment: %v", got)
	}
	// Top-level array contains a scalar.
	if got := ids(`SELECT id FROM docs WHERE j @> '2' ORDER BY id`); len(got) != 1 || got[0] != 3 {
		t.Fatalf("array-scalar: %v", got)
	}
	// NOT @>: NULL rows stay excluded (UNKNOWN).
	if got := ids(`SELECT id FROM docs WHERE NOT (j @> '{"tags":["go"]}') ORDER BY id`); len(got) != 2 || got[0] != 2 || got[1] != 3 {
		t.Fatalf("not containment: %v", got)
	}
	// Path on the left: j -> 'meta' is jsonb.
	if got := ids(`SELECT id FROM docs WHERE j -> 'meta' @> '{"ok":true}' ORDER BY id`); len(got) != 1 || got[0] != 1 {
		t.Fatalf("pathed lhs: %v", got)
	}
	// Parameter RHS (extended protocol; pgx sends jsonb binary).
	if got := ids(`SELECT id FROM docs WHERE j @> $1 ORDER BY id`, `{"tags":["db"]}`); len(got) != 1 || got[0] != 1 {
		t.Fatalf("param rhs: %v", got)
	}
	// Inside OR.
	if got := ids(`SELECT id FROM docs WHERE j @> '{"tags":["rust"]}' OR id = 3 ORDER BY id`); len(got) != 2 || got[0] != 2 || got[1] != 3 {
		t.Fatalf("in OR: %v", got)
	}
	// WHERE of a grouped query (pre-grouping filter).
	var gid, cnt int64
	if err := conn.QueryRow(ctx, `SELECT id, count(*) AS n FROM docs WHERE j @> '{"meta":{"ok":true}}' GROUP BY id ORDER BY id LIMIT 1`).Scan(&gid, &cnt); err != nil || gid != 1 || cnt != 1 {
		t.Fatalf("grouped where: id=%d n=%d, %v", gid, cnt, err)
	}
	// Derived table.
	if got := ids(`SELECT id FROM (SELECT id, j FROM docs) AS d WHERE j @> '{"tags":["go"]}'`); len(got) != 1 || got[0] != 1 {
		t.Fatalf("derived: %v", got)
	}

	// Text-format params on the simple protocol normalize through Coerce.
	scfg, _ := pgx.ParseConfig(pgURL(tc, 0))
	scfg.DefaultQueryExecMode = pgx.QueryExecModeSimpleProtocol
	sconn, err := pgx.ConnectConfig(ctx, scfg)
	if err != nil {
		t.Fatalf("simple connect: %v", err)
	}
	defer func() { _ = sconn.Close(ctx) }()
	var sid int64
	if err := sconn.QueryRow(ctx, `SELECT id FROM docs WHERE j @> '{"meta": {"level": 3}}'`).Scan(&sid); err != nil || sid != 1 {
		t.Fatalf("simple protocol: %d, %v", sid, err)
	}

	// Refusals.
	expectCode(t, ctx, conn, `SELECT id FROM docs WHERE j ->> 'k' @> '1'`, "0A000") // text lhs
	expectCode(t, ctx, conn, `SELECT id FROM docs WHERE id @> '1'`, "0A000")        // non-jsonb lhs
	if _, err := conn.Exec(ctx, `CREATE TABLE tx (id INT8 PRIMARY KEY)`); err != nil {
		t.Fatalf("create tx: %v", err)
	}
	expectCode(t, ctx, conn,
		`SELECT d.id FROM docs AS d JOIN tx AS x ON x.id = d.id WHERE d.j @> '{"a":1}'`, "0A000")
	// Malformed RHS document.
	expectCode(t, ctx, conn, `SELECT id FROM docs WHERE j @> 'not json'`, "22P02")

	// EXPLAIN: containment never plans as bounds.
	var plan string
	if err := conn.QueryRow(ctx, `EXPLAIN SELECT id FROM docs WHERE j @> '{"a":1}'`).Scan(&plan); err != nil {
		t.Fatalf("explain: %v", err)
	}
	if !strings.Contains(plan, "full table scan") {
		t.Fatalf("plan = %q, want full table scan", plan)
	}
}
