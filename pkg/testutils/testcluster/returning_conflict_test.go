package testcluster

import (
	"context"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/sthorne/datax/pkg/sql"
	"github.com/sthorne/datax/pkg/sql/catalog"
)

// TestReturningAndOnConflict: RETURNING on every write, ON CONFLICT DO
// NOTHING / DO UPDATE with EXCLUDED and WHERE, conflicts arbitrated by
// the primary key or a unique index (with index maintenance), ON
// CONSTRAINT, UPSERT INTO, and PostgreSQL's command tags and errors.
func TestReturningAndOnConflict(t *testing.T) {
	tc := Start(t, 1)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	s := sql.NewSession(tc.Nodes[0].DB(), catalog.NewAccessor())
	waitForDatabases(t, ctx, s)
	execSQL(t, ctx, s, `CREATE TABLE t (id INT8 PRIMARY KEY, email TEXT NOT NULL, hits INT8 DEFAULT 0, note TEXT)`)
	execSQL(t, ctx, s, `CREATE UNIQUE INDEX t_email ON t (email)`)
	execSQL(t, ctx, s, `CREATE INDEX t_hits ON t (hits)`)

	texts := func(r *sql.Result) []string {
		var out []string
		for _, row := range r.Rows {
			var cells []string
			for _, d := range row {
				if d.Null {
					cells = append(cells, "NULL")
				} else {
					cells = append(cells, d.Text())
				}
			}
			out = append(out, strings.Join(cells, "|"))
		}
		return out
	}
	expect := func(what string, r *sql.Result, tag string, rows ...string) {
		t.Helper()
		if r.Tag != tag || strings.Join(texts(r), ";") != strings.Join(rows, ";") {
			t.Fatalf("%s: tag %q rows %v, want %q %v", what, r.Tag, texts(r), tag, rows)
		}
	}

	// RETURNING: columns, *, expressions, aliases, and column names on
	// the result.
	r := execSQL(t, ctx, s, `INSERT INTO t (id, email) VALUES (1, 'a@x'), (2, 'b@x') RETURNING id, email`)
	expect("insert returning", r, "INSERT 0 2", "1|a@x", "2|b@x")
	r = execSQL(t, ctx, s, `INSERT INTO t (id, email, note) VALUES (3, 'c@x', 'n') RETURNING *`)
	expect("returning *", r, "INSERT 0 1", "3|c@x|0|n")
	if len(r.Columns) != 4 || r.Columns[0].Name != "id" || r.Columns[2].Name != "hits" {
		t.Fatalf("returning * columns: %+v", r.Columns)
	}
	r = execSQL(t, ctx, s, `UPDATE t SET hits = hits + 5 WHERE id <= 2 RETURNING id, hits * 2 AS doubled, upper(email)`)
	expect("update returning", r, "UPDATE 2", "1|10|A@X", "2|10|B@X")
	if r.Columns[1].Name != "doubled" || r.Columns[2].Name != "?column?" {
		t.Fatalf("update returning columns: %+v", r.Columns)
	}
	r = execSQL(t, ctx, s, `DELETE FROM t WHERE id = 3 RETURNING t.id, note`)
	expect("delete returning", r, "DELETE 1", "3|n")
	r = execSQL(t, ctx, s, `DELETE FROM t WHERE id = 99 RETURNING id`)
	expect("delete returning nothing", r, "DELETE 0")
	if r.Columns == nil {
		t.Fatal("an empty RETURNING result still describes its columns")
	}

	// ON CONFLICT DO NOTHING: the tag counts inserted rows only; with no
	// target any unique key arbitrates (here the email index).
	r = execSQL(t, ctx, s, `INSERT INTO t (id, email) VALUES (1, 'dup@x'), (4, 'd@x'), (5, 'a@x') ON CONFLICT DO NOTHING RETURNING id`)
	expect("do nothing", r, "INSERT 0 1", "4")
	r = execSQL(t, ctx, s, `INSERT INTO t (id, email) VALUES (1, 'z@x') ON CONFLICT (id) DO NOTHING`)
	expect("do nothing on pk", r, "INSERT 0 0")
	// A conflict on a key other than the named arbiter is still an error.
	if _, serr := trySQL(ctx, s, `INSERT INTO t (id, email) VALUES (6, 'a@x') ON CONFLICT (id) DO NOTHING`); serr == nil || serr.Code != sql.CodeUniqueViolation {
		t.Fatalf("non-arbiter conflict: %v", serr)
	}
	// The target must be a unique key.
	if _, serr := trySQL(ctx, s, `INSERT INTO t (id, email) VALUES (6, 'f@x') ON CONFLICT (note) DO NOTHING`); serr == nil || serr.Code != sql.CodeInvalidColumnReference {
		t.Fatalf("bad target: %v", serr)
	}

	// DO UPDATE with EXCLUDED arithmetic and a WHERE; the tag counts
	// inserted and updated rows.
	r = execSQL(t, ctx, s, `INSERT INTO t (id, email, hits) VALUES (1, 'a2@x', 7), (7, 'g@x', 1)
		ON CONFLICT (id) DO UPDATE SET hits = t.hits + excluded.hits, note = 'merged ' || excluded.email
		RETURNING id, email, hits, note`)
	expect("do update", r, "INSERT 0 2", "1|a@x|12|merged a2@x", "7|g@x|1|NULL")
	r = execSQL(t, ctx, s, `INSERT INTO t (id, email, hits) VALUES (1, 'x', 100), (2, 'y', 100)
		ON CONFLICT (id) DO UPDATE SET hits = excluded.hits WHERE t.hits > 10 RETURNING id, hits`)
	expect("do update where", r, "INSERT 0 1", "1|100")
	// Conflict on the unique index, not the PK: the existing row (by its
	// own PK) is updated and the secondary index follows the change.
	r = execSQL(t, ctx, s, `INSERT INTO t (id, email, hits) VALUES (50, 'b@x', 3) ON CONFLICT (email) DO UPDATE SET hits = t.hits + excluded.hits RETURNING id, hits`)
	expect("unique-index conflict", r, "INSERT 0 1", "2|8")
	r = execSQL(t, ctx, s, `SELECT id FROM t WHERE hits = 8`)
	expect("index maintained", r, "SELECT 1", "2")
	r = execSQL(t, ctx, s, `SELECT count(*) FROM t WHERE hits = 5`)
	expect("old entry gone", r, "SELECT 1", "0")
	// ON CONSTRAINT names the primary key or a unique index.
	r = execSQL(t, ctx, s, `INSERT INTO t (id, email) VALUES (2, 'q@x') ON CONFLICT ON CONSTRAINT t_pkey DO UPDATE SET email = excluded.email RETURNING email`)
	expect("on constraint", r, "INSERT 0 1", "q@x")
	if _, serr := trySQL(ctx, s, `INSERT INTO t (id, email) VALUES (2, 'q@x') ON CONFLICT ON CONSTRAINT nope DO NOTHING`); serr == nil || serr.Code != sql.CodeUndefinedObject {
		t.Fatalf("unknown constraint: %v", serr)
	}
	// The same key twice in one statement cannot be updated twice.
	if _, serr := trySQL(ctx, s, `INSERT INTO t (id, email) VALUES (8, 'h@x'), (8, 'h2@x') ON CONFLICT (id) DO UPDATE SET email = excluded.email`); serr == nil || serr.Code != sql.CodeCardinality {
		t.Fatalf("row affected twice: %v", serr)
	}
	// Changing the primary key in DO UPDATE is refused.
	if _, serr := trySQL(ctx, s, `INSERT INTO t (id, email) VALUES (1, 'a@x') ON CONFLICT (id) DO UPDATE SET id = 9`); serr == nil || serr.Code != sql.CodeFeatureNotSupported {
		t.Fatalf("pk update: %v", serr)
	}

	// UPSERT INTO: insert or replace by primary key.
	r = execSQL(t, ctx, s, `UPSERT INTO t (id, email, hits) VALUES (1, 'up@x', 1), (60, 'sixty@x', 6) RETURNING id, email, hits`)
	expect("upsert", r, "INSERT 0 2", "1|up@x|1", "60|sixty@x|6")
	r = execSQL(t, ctx, s, `SELECT id FROM t WHERE email = 'up@x'`)
	expect("upsert reindexed", r, "SELECT 1", "1")
	if _, serr := trySQL(ctx, s, `UPSERT INTO t (id, email) VALUES (61, 'sixty@x')`); serr == nil || serr.Code != sql.CodeUniqueViolation {
		t.Fatalf("upsert unique violation: %v", serr)
	}

	// Over the wire: pgx QueryRow with RETURNING, a bound parameter in
	// DO UPDATE, and the extended-protocol Describe of the result.
	conn, err := pgx.Connect(ctx, "postgres://root@"+tc.Nodes[0].SQLAddr()+"/datax?sslmode=disable")
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close(ctx)
	var id int64
	var email string
	if err := conn.QueryRow(ctx, `INSERT INTO t (id, email) VALUES ($1, $2) RETURNING id, email`, 70, "w@x").Scan(&id, &email); err != nil || id != 70 || email != "w@x" {
		t.Fatalf("pgx returning: %d %q %v", id, email, err)
	}
	var hits int64
	if err := conn.QueryRow(ctx, `INSERT INTO t (id, email, hits) VALUES ($1, $2, $3) ON CONFLICT (id) DO UPDATE SET hits = t.hits + excluded.hits RETURNING hits`, 70, "w@x", 4).Scan(&hits); err != nil || hits != 4 {
		t.Fatalf("pgx upsert: %d %v", hits, err)
	}
	tag, err := conn.Exec(ctx, `INSERT INTO t (id, email) VALUES ($1, $2) ON CONFLICT DO NOTHING`, 70, "w@x")
	if err != nil || tag.String() != "INSERT 0 0" {
		t.Fatalf("pgx do nothing tag: %q %v", tag, err)
	}
}

// TestUpsertConcurrency: two transactions upserting the same key — one
// commits, the other restarts (40001) and, retried, sees the first's
// row; the final row reflects both.
func TestUpsertConcurrency(t *testing.T) {
	tc := Start(t, 1)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	root := sql.NewSession(tc.Nodes[0].DB(), catalog.NewAccessor())
	waitForDatabases(t, ctx, root)
	execSQL(t, ctx, root, `CREATE TABLE c (k INT8 PRIMARY KEY, n INT8)`)

	// Each worker: BEGIN; upsert n = n + 1 (or 1); COMMIT — retrying the
	// whole transaction on 40001, as clients must.
	var wg sync.WaitGroup
	var retries int
	var mu sync.Mutex
	for w := 0; w < 4; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			s := sql.NewSession(tc.Nodes[0].DB(), catalog.NewAccessor())
			for i := 0; i < 5; i++ {
				for attempt := 0; ; attempt++ {
					_, serr := trySQL(ctx, s, `BEGIN`)
					if serr == nil {
						_, serr = trySQL(ctx, s, `INSERT INTO c (k, n) VALUES (1, 1) ON CONFLICT (k) DO UPDATE SET n = c.n + 1`)
					}
					if serr == nil {
						_, serr = trySQL(ctx, s, `COMMIT`)
					}
					if serr == nil {
						break
					}
					trySQL(ctx, s, `ROLLBACK`)
					if serr.Code != sql.CodeSerializationFailure || attempt > 50 {
						t.Errorf("worker: %v", serr)
						return
					}
					mu.Lock()
					retries++
					mu.Unlock()
				}
			}
		}()
	}
	wg.Wait()
	r := execSQL(t, ctx, root, `SELECT n FROM c WHERE k = 1`)
	if len(r.Rows) != 1 || r.Rows[0][0].I != 20 {
		t.Fatalf("final row: %+v (retries %d)", r.Rows, retries)
	}
}
