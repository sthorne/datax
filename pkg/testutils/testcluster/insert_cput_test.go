package testcluster

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

	"github.com/sthorne/datax/pkg/kvpb"
	"github.com/sthorne/datax/pkg/server"
	"github.com/sthorne/datax/pkg/version"
)

// An INSERT checks its keys' uniqueness with conditional puts in its
// write batch once the cluster has finalized v24 (kvpb.CPutRequest), and
// reads them first before. These tests run the same statements on a v23
// and a v24 cluster: every outcome — the SQLSTATE, message, detail and
// constraint of an error, the tag and rows of a success, what the tables
// hold after — must be the same.

// cputScript is the statements both versions run, one connection, in
// order. Table m is split at k = 10, so its rows below 10 are on one
// range and the rest, with its unique index's entries, on another.
var cputScript = []string{
	`CREATE TABLE p (id INT8 PRIMARY KEY)`,
	`INSERT INTO p VALUES (1), (2)`,
	`CREATE TABLE t (k INT8 PRIMARY KEY, u INT8 UNIQUE, v INT8 CHECK (v >= 0), w INT8, pid INT8 REFERENCES p (id))`,
	`CREATE UNIQUE INDEX t_w_big ON t (w) WHERE w > 100`,
	`INSERT INTO t VALUES (1, 10, 0, 1, 1), (2, 20, 0, 200, NULL)`,

	// The primary key.
	`INSERT INTO t VALUES (1, 11, 0, NULL, NULL)`,
	`INSERT INTO t (k) VALUES (1)`,
	`INSERT INTO t VALUES (3, 30, 0, NULL, NULL), (1, 31, 0, NULL, NULL)`,
	`INSERT INTO t VALUES (4, 40, 0, NULL, NULL), (4, 41, 0, NULL, NULL)`,
	`INSERT INTO t VALUES (1, 12, 0, NULL, NULL) RETURNING k, u`,
	// A unique index, and which of two failures a statement reports.
	`INSERT INTO t VALUES (5, 10, 0, NULL, NULL)`,
	`INSERT INTO t VALUES (6, 60, 0, NULL, NULL), (7, 60, 0, NULL, NULL)`,
	`INSERT INTO t VALUES (8, 10, 0, NULL, NULL), (1, 81, 0, NULL, NULL)`,
	`INSERT INTO t VALUES (1, 10, 0, NULL, NULL)`,
	`INSERT INTO t VALUES (9, 91, 0, NULL, NULL), (10, 92, 0, NULL, NULL), (2, 93, 0, NULL, NULL)`,
	// Against a CHECK, a foreign key and RETURNING, the row before wins.
	`INSERT INTO t VALUES (1, 13, -1, NULL, NULL)`,
	`INSERT INTO t VALUES (11, 14, -1, NULL, NULL)`,
	`INSERT INTO t VALUES (1, 15, 0, NULL, NULL), (12, 16, -1, NULL, NULL)`,
	`INSERT INTO t VALUES (13, 17, -1, NULL, NULL), (1, 18, 0, NULL, NULL)`,
	`INSERT INTO t VALUES (1, 19, 0, NULL, 99)`,
	`INSERT INTO t VALUES (14, 21, 0, NULL, 99)`,
	`INSERT INTO t VALUES (2, 22, 0, NULL, NULL), (15, 23, 0, NULL, 99)`,
	`INSERT INTO t VALUES (16, 24, 0, NULL, 99), (2, 25, 0, NULL, NULL)`,
	`INSERT INTO t VALUES (1, 26, 0, NULL, NULL), (17, 27, 0, NULL, NULL) RETURNING 1 / (k - 17)`,
	`INSERT INTO t VALUES (17, 27, 0, NULL, NULL), (1, 26, 0, NULL, NULL) RETURNING 1 / (k - 17)`,
	// NULLs in a unique column, and a partial unique index.
	`INSERT INTO t VALUES (20, NULL, 0, NULL, NULL), (21, NULL, 0, NULL, NULL)`,
	`INSERT INTO t VALUES (22, 220, 0, 50, NULL), (23, 230, 0, 50, NULL)`,
	`INSERT INTO t VALUES (24, 240, 0, 200, NULL)`,
	`INSERT INTO t VALUES (25, 250, 0, 300, NULL), (26, 260, 0, 300, NULL)`,
	// INSERT ... SELECT.
	`INSERT INTO t SELECT k, u + 1000, v, NULL, NULL FROM t WHERE k = 1`,
	`INSERT INTO t SELECT k + 100, u, v, NULL, NULL FROM t WHERE k = 2`,
	`INSERT INTO t SELECT k + 200, u + 2000, v, NULL, NULL FROM t WHERE k IN (1, 2)`,
	// ON CONFLICT and UPSERT keep reading first.
	`INSERT INTO t VALUES (1, 10, 5, NULL, NULL) ON CONFLICT DO NOTHING`,
	`INSERT INTO t VALUES (1, 99, 5, NULL, NULL) ON CONFLICT (k) DO NOTHING`,
	`INSERT INTO t VALUES (30, 10, 5, NULL, NULL) ON CONFLICT (k) DO NOTHING`,
	`INSERT INTO t VALUES (1, 10, 7, NULL, NULL) ON CONFLICT (k) DO UPDATE SET v = excluded.v RETURNING k, v`,
	`INSERT INTO t VALUES (31, 10, 8, NULL, NULL) ON CONFLICT (u) DO UPDATE SET v = excluded.v RETURNING k, v`,
	`INSERT INTO t VALUES (32, 20, 8, NULL, NULL) ON CONFLICT (k) DO UPDATE SET v = excluded.v`,
	`UPSERT INTO t (k, u, v) VALUES (1, 10, 9), (33, 330, 1)`,
	`UPSERT INTO t (k, u, v) VALUES (34, 20, 1)`,
	// In an explicit transaction: the transaction's own writes, a
	// delete and re-insert, and a failure rolled back to a savepoint.
	`BEGIN`,
	`INSERT INTO t VALUES (40, 400, 0, NULL, NULL)`,
	`INSERT INTO t VALUES (40, 401, 0, NULL, NULL)`,
	`ROLLBACK`,
	`BEGIN`,
	`INSERT INTO t VALUES (41, 410, 0, NULL, NULL)`,
	`DELETE FROM t WHERE k = 41`,
	`INSERT INTO t VALUES (41, 411, 0, NULL, NULL)`,
	`DELETE FROM t WHERE k = 1`,
	`INSERT INTO t VALUES (1, 10, 1, NULL, NULL)`,
	`SAVEPOINT s`,
	`INSERT INTO t VALUES (42, 420, 0, NULL, NULL), (41, 421, 0, NULL, NULL)`,
	`SELECT 1`,
	`ROLLBACK TO SAVEPOINT s`,
	`INSERT INTO t VALUES (42, 422, 0, NULL, NULL)`,
	`COMMIT`,
	`SELECT k, u, v, w, pid FROM t ORDER BY k`,

	// Across two ranges (m is split at 10, its index entries on the
	// upper range). The first write of a transaction, whose part on the
	// record's range is sent first and alone; a later one, whose parts go
	// out together; and an autocommit one, a parallel commit: each time
	// the row earlier in the statement reports its failure.
	`CREATE TABLE m (k INT8 PRIMARY KEY, u INT8 UNIQUE)`,
	`ALTER TABLE m SPLIT AT VALUES (10)`,
	`INSERT INTO m VALUES (1, 100), (2, 101), (50, 200)`,
	`BEGIN`,
	`INSERT INTO m VALUES (5, 200), (1, 300)`,
	`ROLLBACK`,
	`BEGIN`,
	`INSERT INTO p VALUES (3)`,
	`INSERT INTO m VALUES (6, 200), (1, 301)`,
	`ROLLBACK`,
	`INSERT INTO m VALUES (7, 200), (2, 302)`,
	`INSERT INTO m VALUES (7, 700), (2, 302)`,
	// A failure after another range's part applied: nothing of it stays.
	`BEGIN`,
	`INSERT INTO m VALUES (20, 600), (1, 304)`,
	`ROLLBACK`,
	`BEGIN`,
	`INSERT INTO p VALUES (4)`,
	`SAVEPOINT s`,
	`INSERT INTO m VALUES (21, 601), (2, 305)`,
	`ROLLBACK TO SAVEPOINT s`,
	`INSERT INTO m VALUES (22, 602)`,
	`COMMIT`,
	`INSERT INTO m VALUES (20, 600), (21, 601), (3, 603)`,
	`SELECT k, u FROM m ORDER BY k`,
	`SELECT id FROM p ORDER BY id`,
}

// runCPutScript runs the script on a fresh one-node cluster at version
// cv and returns each statement's outcome, and how many conditional puts
// the gateway sent.
func runCPutScript(t *testing.T, cv version.Version) ([]string, int64) {
	t.Helper()
	tc := StartWithOptions(t, 1, func(c *server.Config) { c.BinaryVersionOverride = cv })
	ctx, cancel := context.WithTimeout(context.Background(), 150*time.Second)
	defer cancel()
	conn := pgConn(t, ctx, tc, 0)

	var cputs atomic.Int64
	tc.Nodes[0].DB().TestingSetSendObserver(func(_ context.Context, ba *kvpb.BatchRequest, _ *kvpb.BatchResponse) {
		for _, u := range ba.Requests {
			if u.CPut != nil {
				cputs.Add(1)
			}
		}
	})
	defer tc.Nodes[0].DB().TestingSetSendObserver(nil)

	out := make([]string, 0, len(cputScript))
	for _, q := range cputScript {
		start := time.Now()
		res := stmtOutcome(ctx, conn, q)
		if d := time.Since(start); d > 3*time.Second {
			// An intent left behind blocks the next writer of its key
			// until its transaction expires.
			res += fmt.Sprintf(" (took %s)", d.Round(time.Second))
		}
		out = append(out, q+"\n    => "+res)
	}
	return out, cputs.Load()
}

// stmtOutcome renders what a statement did: its error's SQLSTATE,
// message, detail and constraint, or its tag and rows.
func stmtOutcome(ctx context.Context, conn *pgx.Conn, q string) string {
	rows, err := conn.Query(ctx, q)
	var got []string
	if err == nil {
		for rows.Next() {
			vals, verr := rows.Values()
			if verr != nil {
				err = verr
				break
			}
			parts := make([]string, len(vals))
			for i, v := range vals {
				parts[i] = fmt.Sprint(v)
			}
			got = append(got, strings.Join(parts, "|"))
		}
		rows.Close()
		if err == nil {
			err = rows.Err()
		}
	}
	if err != nil {
		var pe *pgconn.PgError
		if errors.As(err, &pe) {
			return fmt.Sprintf("ERROR %s %q detail=%q constraint=%q", pe.Code, pe.Message, pe.Detail, pe.ConstraintName)
		}
		return "ERROR " + err.Error()
	}
	return rows.CommandTag().String() + " [" + strings.Join(got, "; ") + "]"
}

func TestInsertUniquenessSameBeforeAndAfterV24(t *testing.T) {
	before, beforeCPuts := runCPutScript(t, version.V23)
	after, afterCPuts := runCPutScript(t, version.Current)
	if beforeCPuts != 0 {
		t.Errorf("a v23 cluster was sent %d conditional puts", beforeCPuts)
	}
	if afterCPuts == 0 {
		t.Errorf("a v24 cluster's INSERTs sent no conditional put")
	}
	for i := range before {
		if before[i] != after[i] {
			t.Errorf("statement %d differs:\n v23: %s\n v24: %s", i+1, before[i], after[i])
		}
	}
	if t.Failed() {
		t.Logf("v24 outcomes:\n%s", strings.Join(after, "\n"))
		return
	}
	// The outcomes are PostgreSQL's (pinned so that both versions
	// cannot drift together).
	want := map[string]string{
		`INSERT INTO t VALUES (1, 11, 0, NULL, NULL)`:                                                 `ERROR 23505 "duplicate key value violates unique constraint on \"t\""`,
		`INSERT INTO t VALUES (8, 10, 0, NULL, NULL), (1, 81, 0, NULL, NULL)`:                         `ERROR 23505 "duplicate key value violates unique constraint \"t_u_key\""`,
		`INSERT INTO t VALUES (1, 10, 0, NULL, NULL)`:                                                 `ERROR 23505 "duplicate key value violates unique constraint on \"t\""`,
		`INSERT INTO t VALUES (13, 17, -1, NULL, NULL), (1, 18, 0, NULL, NULL)`:                       `ERROR 23514`,
		`INSERT INTO t VALUES (1, 15, 0, NULL, NULL), (12, 16, -1, NULL, NULL)`:                       `ERROR 23505`,
		`INSERT INTO t VALUES (16, 24, 0, NULL, 99), (2, 25, 0, NULL, NULL)`:                          `ERROR 23503`,
		`INSERT INTO t VALUES (1, 26, 0, NULL, NULL), (17, 27, 0, NULL, NULL) RETURNING 1 / (k - 17)`: `ERROR 23505`,
		`INSERT INTO t VALUES (25, 250, 0, 300, NULL), (26, 260, 0, 300, NULL)`:                       `ERROR 23505 "duplicate key value violates unique constraint \"t_w_big\""`,
		`INSERT INTO t VALUES (22, 220, 0, 50, NULL), (23, 230, 0, 50, NULL)`:                         `INSERT 0 2`,
		`INSERT INTO m VALUES (5, 200), (1, 300)`:                                                     `ERROR 23505 "duplicate key value violates unique constraint \"m_u_key\""`,
		`INSERT INTO m VALUES (6, 200), (1, 301)`:                                                     `ERROR 23505 "duplicate key value violates unique constraint \"m_u_key\""`,
		`INSERT INTO m VALUES (7, 200), (2, 302)`:                                                     `ERROR 23505 "duplicate key value violates unique constraint \"m_u_key\""`,
		`INSERT INTO m VALUES (7, 700), (2, 302)`:                                                     `ERROR 23505 "duplicate key value violates unique constraint on \"m\""`,
		`INSERT INTO m VALUES (20, 600), (21, 601), (3, 603)`:                                         `INSERT 0 3`,
	}
	seen := 0
	for _, o := range after {
		q, res, _ := strings.Cut(o, "\n    => ")
		if w, ok := want[q]; ok {
			seen++
			if !strings.HasPrefix(res, w) {
				t.Errorf("%s\n got %s\nwant %s...", q, res, w)
			}
		}
		if strings.Contains(res, "took") {
			t.Errorf("%s: %s", q, res)
		}
	}
	if seen != len(want) {
		t.Errorf("checked %d of %d pinned outcomes", seen, len(want))
	}
}

// TestInsertSameKeyConcurrently: inserters racing for one key — in
// autocommit statements and in explicit transactions — leave exactly one
// row; every loser fails with 23505 (or, in an explicit transaction,
// 40001), as before.
func TestInsertSameKeyConcurrently(t *testing.T) {
	tc := Start(t, 3)
	ctx, cancel := context.WithTimeout(context.Background(), 150*time.Second)
	defer cancel()
	setup := pgConn(t, ctx, tc, 0)
	mustExec(t, ctx, setup, `CREATE TABLE race (k INT8 PRIMARY KEY, u INT8 UNIQUE, who INT8)`)

	const workers, rounds = 6, 8
	conns := make([]*pgx.Conn, workers)
	for i := range conns {
		conns[i] = pgConn(t, ctx, tc, i%3)
	}
	for round := 0; round < rounds; round++ {
		explicit := round%2 == 1
		byUnique := round%4 >= 2 // race on the unique index instead of the key
		var (
			wg      sync.WaitGroup
			mu      sync.Mutex
			wins    int
			outcome []string
		)
		start := make(chan struct{})
		for w := 0; w < workers; w++ {
			wg.Add(1)
			go func(w int) {
				defer wg.Done()
				k, u := int64(round*100), int64(round*100)
				if byUnique {
					k += int64(w) + 1
				}
				<-start
				var err error
				if explicit {
					var tx pgx.Tx
					if tx, err = conns[w].Begin(ctx); err == nil {
						if _, err = tx.Exec(ctx, `INSERT INTO race VALUES ($1, $2, $3)`, k, u, w); err == nil {
							err = tx.Commit(ctx)
						} else {
							_ = tx.Rollback(ctx)
						}
					}
				} else {
					_, err = conns[w].Exec(ctx, `INSERT INTO race VALUES ($1, $2, $3)`, k, u, w)
				}
				mu.Lock()
				defer mu.Unlock()
				switch code := sqlState(err); {
				case err == nil:
					wins++
				case code == "23505", explicit && code == "40001":
					outcome = append(outcome, code)
				default:
					t.Errorf("round %d worker %d: %v", round, w, err)
				}
			}(w)
		}
		close(start)
		wg.Wait()
		if wins != 1 {
			t.Fatalf("round %d (explicit %v, unique %v): %d inserters won, losers %v", round, explicit, byUnique, wins, outcome)
		}
		n := queryStrings(t, ctx, setup, fmt.Sprintf(`SELECT count(*) FROM race WHERE u = %d`, round*100))
		if len(n) != 1 || n[0] != "1" {
			t.Fatalf("round %d: %v rows with the raced key", round, n)
		}
	}
}
