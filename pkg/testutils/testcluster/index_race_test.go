package testcluster

import (
	"context"
	"fmt"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/sthorne/datax/pkg/keys"
	"github.com/sthorne/datax/pkg/kvclient"
	"github.com/sthorne/datax/pkg/sql"
	"github.com/sthorne/datax/pkg/sql/catalog"
	"github.com/sthorne/datax/pkg/sql/rowenc"
	"github.com/sthorne/datax/pkg/sql/types"
)

// Issue #368: two online CREATE INDEX statements on one table, from two
// sessions, could lose an index. Each phase of the build rewrote the
// table descriptor from the gateway's leased copy, which it had not read
// in its transaction, so the write was blind: a build that took its copy
// before another build's publish committed wrote the table back without
// the other index, and handed out the same index ID a second time. Both
// statements then reported success, both builds backfilled and flipped
// public the one index that held the ID, and the other index was gone;
// at other interleavings one build failed with "index vanished during
// backfill". pg_restore -j, which builds a table's indexes at the same
// time from several connections, hit it every time.

// checkIndexExact checks that the committed descriptor of table holds
// each named index, public and under an ID of its own, and that each
// index's keyspace holds exactly the entries the table's rows have in it:
// every row's, and nothing else. It reads the rows and the entries in one
// transaction and returns the descriptor.
func checkIndexExact(t *testing.T, ctx context.Context, db *kvclient.DB, table string, names ...string) *catalog.TableDescriptor {
	t.Helper()
	desc := lookupDescriptor(t, ctx, db, table)
	ids := map[uint64]string{}
	for _, ix := range desc.Indexes {
		if prev, dup := ids[ix.ID]; dup {
			t.Fatalf("table %s: indexes %q and %q share ID %d", table, prev, ix.Name, ix.ID)
		}
		ids[ix.ID] = ix.Name
	}
	reader := db.NewTxn("index-check")
	defer func() { _ = reader.Rollback(ctx) }()
	plo, phi := rowenc.PrimarySpanFor(desc)
	rows, err := reader.Scan(ctx, plo, phi, 0)
	if err != nil {
		t.Fatal(err)
	}
	decoded := make([]map[catalog.ColumnID]types.Datum, len(rows))
	for i, kv := range rows {
		row, err := rowenc.DecodeValue(desc, kv.Value)
		if err != nil {
			t.Fatalf("decoding row %s: %v", kv.Key, err)
		}
		pk, err := rowenc.DecodePK(desc, kv.Key)
		if err != nil {
			t.Fatalf("decoding row key %s: %v", kv.Key, err)
		}
		for j, id := range desc.PrimaryKey {
			row[id] = pk[j]
		}
		decoded[i] = row
	}
	for _, name := range names {
		idx, ok := desc.Index(name)
		if !ok {
			var have []string
			for _, ix := range desc.Indexes {
				have = append(have, fmt.Sprintf("%s(id %d, %s)", ix.Name, ix.ID, ix.State))
			}
			t.Fatalf("table %s: index %q is missing; the descriptor holds %v", table, name, have)
		}
		if !idx.Public() || idx.State != catalog.IndexStatePublic {
			t.Fatalf("table %s: index %q is %q, want public", table, name, idx.State)
		}
		want := map[string]string{}
		for i, row := range decoded {
			entries, err := rowenc.IndexEntries(desc, &idx, row)
			if err != nil {
				t.Fatalf("index %q: the entries of row %s: %v", name, rows[i].Key, err)
			}
			for _, e := range entries {
				want[string(e.Key)] = string(e.Value)
			}
		}
		lo, hi := keys.TableIndexSpan(desc.ID, idx.ID)
		got, err := reader.Scan(ctx, lo, hi, 0)
		if err != nil {
			t.Fatal(err)
		}
		extra := 0
		for _, kv := range got {
			v, ok := want[string(kv.Key)]
			if !ok || v != string(kv.Value) {
				extra++
				continue
			}
			delete(want, string(kv.Key))
		}
		if extra != 0 || len(want) != 0 {
			t.Fatalf("table %s, index %q (id %d): %d entries for %d rows; %d entries no row has, %d row entries missing",
				table, name, idx.ID, len(got), len(rows), extra, len(want))
		}
	}
	return desc
}

// TestCreateIndexConcurrentPublishKeepsBoth forces the interleaving of
// issue #368: build A has taken the table's descriptor and is about to
// publish its write-only index when build B, from another gateway,
// publishes its own. A then writes. Before the fix A's write put back the
// table without B's index and gave A's index B's ID; both statements
// reported success, and only one index was left.
func TestCreateIndexConcurrentPublishKeepsBoth(t *testing.T) {
	tc := Start(t, 3)
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	const ttl = 3 * time.Second
	sA := leasedSession(t, tc, 0, ttl)
	sB := leasedSession(t, tc, 1, ttl)
	execSQL(t, ctx, sA, `CREATE TABLE t (id INT PRIMARY KEY, a INT, b INT)`)
	var vals []string
	for i := 0; i < 200; i++ {
		vals = append(vals, fmt.Sprintf("(%d, %d, %d)", i, i%17, i%5))
	}
	execSQL(t, ctx, sA, `INSERT INTO t VALUES `+strings.Join(vals, ", "))
	// Both gateways lease the table, so each build starts from a cached
	// copy, as statements do.
	execSQL(t, ctx, sA, `SELECT count(*) FROM t`)
	execSQL(t, ctx, sB, `SELECT count(*) FROM t`)

	arrived := make(chan struct{})
	release := make(chan struct{})
	aPublished := make(chan struct{}, 1)
	var held atomic.Bool
	sql.TestingCreateIndexHook = func(point, index string) {
		switch {
		case index == "t_a" && point == sql.CreateIndexBeforePublishWrite && held.CompareAndSwap(false, true):
			close(arrived)
			<-release
		case index == "t_a" && point == sql.CreateIndexPublished:
			select {
			case aPublished <- struct{}{}:
			default:
			}
		case index == "t_b" && point == sql.CreateIndexPublished:
			// B's write-only index has committed: now let A write, and
			// wait until A's publish has committed too, before B goes on.
			close(release)
			select {
			case <-aPublished:
			case <-time.After(60 * time.Second):
				t.Error("build A never published its index")
			}
		}
	}
	defer func() { sql.TestingCreateIndexHook = nil }()

	errA := make(chan *sql.Error, 1)
	go func() {
		_, serr := trySQL(ctx, sA, `CREATE INDEX t_a ON t (a)`)
		errA <- serr
	}()
	select {
	case <-arrived:
	case <-time.After(60 * time.Second):
		t.Fatal("build A never reached its publish")
	}
	if _, serr := trySQL(ctx, sB, `CREATE INDEX t_b ON t (b)`); serr != nil {
		t.Fatalf("CREATE INDEX t_b: [%s] %s", serr.Code, serr.Msg)
	}
	if serr := <-errA; serr != nil {
		t.Fatalf("CREATE INDEX t_a: [%s] %s", serr.Code, serr.Msg)
	}
	sql.TestingCreateIndexHook = nil

	checkIndexExact(t, ctx, tc.Nodes[0].DB(), "t", "t_a", "t_b")
	// Each gateway plans with each index.
	for _, s := range []*sql.Session{sA, sB} {
		if p := explainPlan(t, ctx, s, `SELECT id FROM t WHERE a = 3`); !strings.Contains(p, `"t_a"`) {
			t.Fatalf("plan for a = 3: %q", p)
		}
		if p := explainPlan(t, ctx, s, `SELECT id FROM t WHERE b = 3`); !strings.Contains(p, `"t_b"`) {
			t.Fatalf("plan for b = 3: %q", p)
		}
	}
}

// TestCreateIndexConcurrentPublicKeepsOther forces the other write the
// build makes: build A is about to flip its backfilled index public when
// build B publishes its write-only index. A's flip must keep B's index.
func TestCreateIndexConcurrentPublicKeepsOther(t *testing.T) {
	tc := Start(t, 3)
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	const ttl = 3 * time.Second
	sA := leasedSession(t, tc, 0, ttl)
	sB := leasedSession(t, tc, 2, ttl)
	execSQL(t, ctx, sA, `CREATE TABLE t (id INT PRIMARY KEY, a INT, b TEXT)`)
	var vals []string
	for i := 0; i < 150; i++ {
		vals = append(vals, fmt.Sprintf("(%d, %d, 'v%d')", i, i%11, i%7))
	}
	execSQL(t, ctx, sA, `INSERT INTO t VALUES `+strings.Join(vals, ", "))
	execSQL(t, ctx, sB, `SELECT count(*) FROM t`)

	arrived := make(chan struct{})
	release := make(chan struct{})
	var held atomic.Bool
	sql.TestingCreateIndexHook = func(point, index string) {
		switch {
		case index == "t_a" && point == sql.CreateIndexBeforePublicWrite && held.CompareAndSwap(false, true):
			close(arrived)
			<-release
		case index == "t_b" && point == sql.CreateIndexPublished:
			close(release)
		}
	}
	defer func() { sql.TestingCreateIndexHook = nil }()

	errA := make(chan *sql.Error, 1)
	go func() {
		_, serr := trySQL(ctx, sA, `CREATE INDEX t_a ON t (a)`)
		errA <- serr
	}()
	select {
	case <-arrived:
	case <-time.After(60 * time.Second):
		t.Fatal("build A never reached its flip")
	}
	if _, serr := trySQL(ctx, sB, `CREATE INDEX t_b ON t (b) WHERE a > 3`); serr != nil {
		t.Fatalf("CREATE INDEX t_b: [%s] %s", serr.Code, serr.Msg)
	}
	if serr := <-errA; serr != nil {
		t.Fatalf("CREATE INDEX t_a: [%s] %s", serr.Code, serr.Msg)
	}
	sql.TestingCreateIndexHook = nil
	checkIndexExact(t, ctx, tc.Nodes[0].DB(), "t", "t_a", "t_b")
}

// raceRounds is how many times the randomized tests below repeat;
// DATAX_INDEX_RACE_ROUNDS raises it for a soak.
func raceRounds(t *testing.T, def int) int {
	if v := os.Getenv("DATAX_INDEX_RACE_ROUNDS"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil || n < 1 {
			t.Fatalf("DATAX_INDEX_RACE_ROUNDS=%q", v)
		}
		return n
	}
	return def
}

// runConcurrently runs each statement on its own session, all at once,
// and returns their errors in order.
func runConcurrently(ctx context.Context, sessions []*sql.Session, stmts []string) []*sql.Error {
	errs := make([]*sql.Error, len(stmts))
	var start, wg sync.WaitGroup
	start.Add(1)
	for i := range stmts {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			start.Wait()
			_, errs[i] = trySQL(ctx, sessions[i], stmts[i])
		}(i)
	}
	start.Done()
	wg.Wait()
	return errs
}

// TestConcurrentCreateIndexes runs several CREATE INDEX statements on one
// table at once, each from a session of its own spread over the three
// nodes, while another session writes to the table, over several rounds:
// afterwards every index is there, public, and holds exactly one entry
// per row.
func TestConcurrentCreateIndexes(t *testing.T) {
	tc := Start(t, 3)
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Minute)
	defer cancel()

	const ttl = 3 * time.Second
	const builds = 6
	var sessions []*sql.Session
	for i := 0; i < builds; i++ {
		sessions = append(sessions, leasedSession(t, tc, i%3, ttl))
	}
	writer := leasedSession(t, tc, 1, ttl)
	setup := leasedSession(t, tc, 0, ttl)

	for round := 0; round < raceRounds(t, 3); round++ {
		table := fmt.Sprintf("r%d", round)
		var cols []string
		for c := 0; c < builds; c++ {
			cols = append(cols, fmt.Sprintf("c%d INT", c))
		}
		execSQL(t, ctx, setup, fmt.Sprintf(`CREATE TABLE %s (id INT PRIMARY KEY, %s, s TEXT)`, table, strings.Join(cols, ", ")))
		var vals []string
		for i := 0; i < 300; i++ {
			vals = append(vals, fmt.Sprintf("(%d, %d, %d, %d, %d, %d, %d, 'S%d')", i, i%3, i%5, i%7, i%11, i%13, i%17, i%19))
		}
		execSQL(t, ctx, setup, fmt.Sprintf(`INSERT INTO %s VALUES %s`, table, strings.Join(vals, ", ")))
		for _, s := range append(sessions, writer) {
			execSQL(t, ctx, s, fmt.Sprintf(`SELECT count(*) FROM %s`, table))
		}

		// The forms of #358/#359 among them: an expression key, a
		// predicate, a descending key, an unnamed index.
		stmts := []string{
			fmt.Sprintf(`CREATE INDEX %s_i0 ON %s (c0)`, table, table),
			fmt.Sprintf(`CREATE INDEX %s_i1 ON %s (c1, c2)`, table, table),
			fmt.Sprintf(`CREATE INDEX %s_i2 ON %s (lower(s))`, table, table),
			fmt.Sprintf(`CREATE INDEX %s_i3 ON %s (c3) WHERE c4 > 2`, table, table),
			fmt.Sprintf(`CREATE INDEX %s_i4 ON %s (c5 DESC)`, table, table),
			fmt.Sprintf(`CREATE INDEX ON %s (c4)`, table),
		}
		names := []string{table + "_i0", table + "_i1", table + "_i2", table + "_i3", table + "_i4", table + "_c4_idx"}

		stop := make(chan struct{})
		werr := make(chan error, 1)
		go func() {
			for i := 0; ; i++ {
				select {
				case <-stop:
					werr <- nil
					return
				default:
				}
				var q string
				switch i % 3 {
				case 0:
					q = fmt.Sprintf(`INSERT INTO %s VALUES (%d, 1, 2, 3, 4, 5, 6, 'W%d')`, table, 10000+i, i)
				case 1:
					q = fmt.Sprintf(`UPDATE %s SET c0 = c0 + 1, s = 'U%d' WHERE id = %d`, table, i, i%300)
				default:
					q = fmt.Sprintf(`DELETE FROM %s WHERE id = %d`, table, 299-i%300)
				}
				if _, serr := trySQL(ctx, writer, q); serr != nil && serr.Code != "40001" {
					werr <- fmt.Errorf("%s: [%s] %s", q, serr.Code, serr.Msg)
					return
				}
			}
		}()
		errs := runConcurrently(ctx, sessions, stmts)
		close(stop)
		if err := <-werr; err != nil {
			t.Fatal(err)
		}
		for i, serr := range errs {
			if serr != nil {
				t.Fatalf("round %d: %s: [%s] %s", round, stmts[i], serr.Code, serr.Msg)
			}
		}
		checkIndexExact(t, ctx, tc.Nodes[round%3].DB(), table, names...)
	}
}

// TestConcurrentCreateIndexWithDropAndAlter runs CREATE INDEX at the
// same time as DROP INDEX of another index of the table and as a
// multi-action ALTER TABLE that builds an index of its own (its
// PendingAlter published alongside): each change is made and none undoes
// another.
func TestConcurrentCreateIndexWithDropAndAlter(t *testing.T) {
	tc := Start(t, 3)
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Minute)
	defer cancel()

	const ttl = 3 * time.Second
	var sessions []*sql.Session
	for i := 0; i < 5; i++ {
		sessions = append(sessions, leasedSession(t, tc, i%3, ttl))
	}
	setup := leasedSession(t, tc, 0, ttl)

	for round := 0; round < raceRounds(t, 3); round++ {
		table := fmt.Sprintf("m%d", round)
		execSQL(t, ctx, setup, fmt.Sprintf(`CREATE TABLE %s (id INT PRIMARY KEY, a INT, b INT, u INT, s TEXT)`, table))
		var vals []string
		for i := 0; i < 250; i++ {
			vals = append(vals, fmt.Sprintf("(%d, %d, %d, %d, 'S%d')", i, i%9, i%4, i, i%6))
		}
		execSQL(t, ctx, setup, fmt.Sprintf(`INSERT INTO %s VALUES %s`, table, strings.Join(vals, ", ")))
		execSQL(t, ctx, setup, fmt.Sprintf(`CREATE INDEX %s_old1 ON %s (b)`, table, table))
		execSQL(t, ctx, setup, fmt.Sprintf(`CREATE INDEX %s_old2 ON %s (s)`, table, table))
		for _, s := range sessions {
			execSQL(t, ctx, s, fmt.Sprintf(`SELECT count(*) FROM %s`, table))
		}

		stmts := []string{
			fmt.Sprintf(`CREATE INDEX %s_n1 ON %s (a)`, table, table),
			fmt.Sprintf(`DROP INDEX %s_old1`, table),
			fmt.Sprintf(`ALTER TABLE %s ADD COLUMN z INT DEFAULT 7, ADD CONSTRAINT %s_u_key UNIQUE (u)`, table, table),
			fmt.Sprintf(`CREATE INDEX %s_n2 ON %s (a, b) WHERE b < 3`, table, table),
			fmt.Sprintf(`CREATE INDEX %s_n3 ON %s (upper(s))`, table, table),
		}
		for i, serr := range runConcurrently(ctx, sessions, stmts) {
			if serr != nil {
				t.Fatalf("round %d: %s: [%s] %s", round, stmts[i], serr.Code, serr.Msg)
			}
		}
		desc := checkIndexExact(t, ctx, tc.Nodes[round%3].DB(), table,
			table+"_n1", table+"_n2", table+"_n3", table+"_old2", table+"_u_key")
		if _, ok := desc.Index(table + "_old1"); ok {
			t.Fatalf("round %d: the dropped index %s_old1 is back", round, table)
		}
		if _, ok := desc.Col("z"); !ok {
			t.Fatalf("round %d: the ALTER's column z is missing", round)
		}
		if c, ok := desc.Constraint(table + "_u_key"); !ok || c.Kind != catalog.ConstraintUnique {
			t.Fatalf("round %d: the ALTER's constraint is missing: %+v", round, c)
		}
		if desc.Alter != nil {
			t.Fatalf("round %d: the ALTER is still pending: %+v", round, desc.Alter)
		}
		if res := execSQL(t, ctx, setup, fmt.Sprintf(`SELECT count(*) FROM %s WHERE z = 7`, table)); res.Rows[0][0].I != 250 {
			t.Fatalf("round %d: %d rows read z = 7", round, res.Rows[0][0].I)
		}
		if _, serr := trySQL(ctx, setup, fmt.Sprintf(`INSERT INTO %s (id, u) VALUES (9999, 5)`, table)); serr == nil || serr.Code != "23505" {
			t.Fatalf("round %d: a duplicate u was not refused: %+v", round, serr)
		}
	}
}

// TestCreateIndexWaitsOutWriterIntents: when the drain of the write-only
// index ends, a writer that maintains the index has an open transaction
// whose entry in it is still an intent. The build empties the index
// before backfilling it, with a scan outside any transaction; that scan
// used to fail the statement with "conflicting write intents" (seen
// under pg_restore -j, which loads a table while its indexes are built).
// It waits for the writer instead, and the writer's row is indexed.
func TestCreateIndexWaitsOutWriterIntents(t *testing.T) {
	tc := Start(t, 3)
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	const ttl = 3 * time.Second
	sA := leasedSession(t, tc, 0, ttl)
	sW := leasedSession(t, tc, 1, ttl)
	execSQL(t, ctx, sA, `CREATE TABLE t (id INT PRIMARY KEY, a INT)`)
	execSQL(t, ctx, sA, `INSERT INTO t VALUES (1, 10), (2, 20), (3, 30)`)
	execSQL(t, ctx, sW, `SELECT count(*) FROM t`)

	committed := make(chan *sql.Error, 1)
	var fired atomic.Bool
	sql.TestingCreateIndexHook = func(point, index string) {
		if point != sql.CreateIndexDrained || !fired.CompareAndSwap(false, true) {
			return
		}
		// The writer's gateway has adopted the write-only index: its
		// INSERT lays an intent in the index's span, and holds it.
		for _, q := range []string{`BEGIN`, `INSERT INTO t VALUES (4, 40)`} {
			if _, serr := trySQL(ctx, sW, q); serr != nil {
				t.Errorf("%s: [%s] %s", q, serr.Code, serr.Msg)
			}
		}
		go func() {
			time.Sleep(500 * time.Millisecond)
			_, serr := trySQL(ctx, sW, `COMMIT`)
			committed <- serr
		}()
	}
	defer func() { sql.TestingCreateIndexHook = nil }()

	if _, serr := trySQL(ctx, sA, `CREATE INDEX t_a ON t (a)`); serr != nil {
		t.Fatalf("CREATE INDEX: [%s] %s", serr.Code, serr.Msg)
	}
	sql.TestingCreateIndexHook = nil
	if !fired.Load() {
		t.Fatal("the build never reached the end of its drain")
	}
	if serr := <-committed; serr != nil {
		t.Fatalf("the writer's COMMIT: [%s] %s", serr.Code, serr.Msg)
	}
	if res := execSQL(t, ctx, sA, `SELECT count(*) FROM t`); res.Rows[0][0].I != 4 {
		t.Fatalf("%d rows", res.Rows[0][0].I)
	}
	checkIndexExact(t, ctx, tc.Nodes[0].DB(), "t", "t_a")
}
