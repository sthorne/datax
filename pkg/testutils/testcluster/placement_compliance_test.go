package testcluster

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
)

// Placement compliance and cluster topology over SQL (issue #405). The
// test cluster runs no HTTP listener, so everything here is read over
// pgwire only, by an ordinary role where the issue's start-up check
// would read it.

type placementRow struct {
	db, constraints, source                                 string
	replicas                                                int64
	satisfied                                               *bool
	ranges, misplaced, unsatisfiable, unattributed, unknown *int64
}

func showPlacement(t *testing.T, ctx context.Context, conn *pgx.Conn, db string) placementRow {
	t.Helper()
	var r placementRow
	err := conn.QueryRow(ctx, `SHOW PLACEMENT FOR DATABASE `+db).Scan(&r.db, &r.replicas, &r.constraints, &r.source,
		&r.satisfied, &r.ranges, &r.misplaced, &r.unsatisfiable, &r.unattributed, &r.unknown)
	if err != nil {
		t.Fatalf("SHOW PLACEMENT FOR DATABASE %s: %v", db, err)
	}
	if r.satisfied == nil || r.ranges == nil || r.misplaced == nil || r.unsatisfiable == nil || r.unattributed == nil || r.unknown == nil {
		t.Fatalf("SHOW PLACEMENT FOR DATABASE %s: compliance unavailable: %+v", db, r)
	}
	return r
}

func (r placementRow) String() string {
	return fmt.Sprintf("satisfied=%v ranges=%d misplaced=%d unsatisfiable=%d unattributed=%d unknown=%d",
		*r.satisfied, *r.ranges, *r.misplaced, *r.unsatisfiable, *r.unattributed, *r.unknown)
}

func pcExec(t *testing.T, ctx context.Context, conn *pgx.Conn, stmts ...string) {
	t.Helper()
	for _, s := range stmts {
		if _, err := conn.Exec(ctx, s); err != nil {
			t.Fatalf("%s: %v", s, err)
		}
	}
}

func TestPlacementComplianceOverSQL(t *testing.T) {
	tc := Start(t, 4,
		"region=us-east-1,zone=a", "region=us-east-2,zone=b", "region=us-east-1,zone=c", "region=eu-west-1,zone=d")
	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Second)
	defer cancel()

	root, err := pgx.Connect(ctx, pgURL(tc, 0))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = root.Close(ctx) }()
	pcExec(t, ctx, root,
		`CREATE DATABASE resid`,
		`CREATE DATABASE secret`,
		`REVOKE CONNECT ON DATABASE secret FROM public`,
		`CREATE ROLE app LOGIN`,
		`USE resid`,
		`CREATE TABLE ledger (id INT8 PRIMARY KEY, note TEXT)`,
		`INSERT INTO ledger SELECT i, 'row' FROM generate_series(1, 50) AS g(i)`,
	)
	if err := tc.waitForReplication(ctx, 3, ""); err != nil {
		t.Fatal(err)
	}

	// The application's own role, on another node, with no admin rights.
	app, err := pgx.Connect(ctx, fmt.Sprintf("postgres://app@%s/resid?sslmode=disable", tc.Nodes[1].SQLAddr()))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = app.Close(ctx) }()

	// No policy: nothing can be out of place.
	if r := showPlacement(t, ctx, app, "resid"); !*r.satisfied || *r.ranges == 0 || r.source != "cluster default" {
		t.Fatalf("before the policy: %s (%+v)", r, r)
	}

	// Three nodes match the US policy, so it is satisfiable, and once the
	// allocator has moved every replica off the EU node it is satisfied.
	pcExec(t, ctx, root, `ALTER DATABASE resid SET (constraints = ('region=us-east-1', 'region=us-east-2'))`)
	deadline := time.Now().Add(120 * time.Second)
	var r placementRow
	sawMisplaced := false
	for {
		r = showPlacement(t, ctx, app, "resid")
		if *r.misplaced > 0 {
			sawMisplaced = true
		}
		if *r.unsatisfiable != 0 || *r.unknown != 0 {
			t.Fatalf("US policy with three US nodes: %s", r)
		}
		if *r.satisfied {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("US policy never satisfied: %s", r)
		}
		time.Sleep(250 * time.Millisecond)
	}
	if *r.misplaced != 0 || *r.unattributed != 0 || *r.ranges == 0 {
		t.Fatalf("satisfied with %s", r)
	}
	t.Logf("satisfied: %s (a misplaced range was seen on the way: %v)", r, sawMisplaced)

	// SHOW RANGES agrees: every range is the ledger's, all in the US. A
	// role that cannot see the table gets neither its name nor a key
	// carrying a row.
	hidden, err := app.Query(ctx, `SHOW RANGES FROM DATABASE resid`)
	if err != nil {
		t.Fatal(err)
	}
	for hidden.Next() {
		vals, err := hidden.Values()
		if err != nil {
			t.Fatal(err)
		}
		if vals[3] != nil || strings.Contains(fmt.Sprint(vals[1], vals[2]), "ledger") {
			t.Fatalf("a role without privileges on ledger sees %v", vals)
		}
	}
	if err := hidden.Err(); err != nil {
		t.Fatal(err)
	}
	pcExec(t, ctx, root, `GRANT SELECT ON ledger TO app`)
	rows, err := app.Query(ctx, `SHOW RANGES FROM DATABASE resid`)
	if err != nil {
		t.Fatal(err)
	}
	n := 0
	for rows.Next() {
		var id int64
		var start, end, status string
		var table *string
		var replicas []int64
		var locs []string
		if err := rows.Scan(&id, &start, &end, &table, &replicas, &locs, &status); err != nil {
			t.Fatal(err)
		}
		n++
		if table == nil || *table != "ledger" || status != "ok" || len(replicas) != 3 || len(locs) != 3 {
			t.Fatalf("range r%d [%s, %s): table %v status %s replicas %v localities %v", id, start, end, table, status, replicas, locs)
		}
		for _, l := range locs {
			if !strings.HasPrefix(l, "region=us-east-") {
				t.Fatalf("range r%d has a replica at %s", id, l)
			}
		}
	}
	if err := rows.Err(); err != nil || int64(n) != *r.ranges {
		t.Fatalf("SHOW RANGES: %d rows (%v), SHOW PLACEMENT counted %d", n, err, *r.ranges)
	}
	// A database the role cannot connect to is not shown.
	wantSQLState(t, ctx, app, "42501", `SHOW RANGES FROM DATABASE secret`)

	// One EU node cannot hold three replicas: unsatisfiable at once, and
	// the US replicas that cannot move are counted as misplaced too.
	pcExec(t, ctx, root, `ALTER DATABASE resid SET (constraints = ('region=eu-west-1'))`)
	r = showPlacement(t, ctx, app, "resid")
	if *r.satisfied || *r.unsatisfiable != *r.ranges || *r.misplaced == 0 {
		t.Fatalf("EU policy with one EU node: %s", r)
	}
	// Fewer matching nodes than replicas, not only none: two of the
	// three needed.
	pcExec(t, ctx, root, `ALTER DATABASE resid SET (constraints = ('region=us-east-2', 'region=eu-west-1'))`)
	if r = showPlacement(t, ctx, app, "resid"); *r.satisfied || *r.unsatisfiable != *r.ranges {
		t.Fatalf("two matching nodes for three replicas: %s", r)
	}

	// SHOW NODES: the admin role, or a role's own VIEWCLUSTER.
	wantSQLState(t, ctx, app, "42501", `SHOW NODES`)
	pcExec(t, ctx, root, `CREATE ROLE viewers VIEWCLUSTER`, `GRANT viewers TO app`)
	wantSQLState(t, ctx, app, "42501", `SHOW NODES`)
	pcExec(t, ctx, root, `ALTER ROLE app VIEWCLUSTER`)
	nrows, err := app.Query(ctx, `SHOW NODES`)
	if err != nil {
		t.Fatal(err)
	}
	var got []string
	for nrows.Next() {
		var id int64
		var addr, loc, state string
		var draining, stopping bool
		var hb *time.Time
		if err := nrows.Scan(&id, &addr, &loc, &state, &draining, &stopping, &hb); err != nil {
			t.Fatal(err)
		}
		if state != "live" || draining || stopping || hb == nil || addr == "" {
			t.Fatalf("n%d: %s %s draining=%v shutting_down=%v heartbeat=%v", id, addr, state, draining, stopping, hb)
		}
		got = append(got, fmt.Sprintf("n%d %s", id, loc))
	}
	if err := nrows.Err(); err != nil {
		t.Fatal(err)
	}
	want := []string{"n1 region=us-east-1,zone=a", "n2 region=us-east-2,zone=b", "n3 region=us-east-1,zone=c", "n4 region=eu-west-1,zone=d"}
	if strings.Join(got, "; ") != strings.Join(want, "; ") {
		t.Fatalf("SHOW NODES: %v, want %v", got, want)
	}
	pcExec(t, ctx, root, `ALTER ROLE app NOVIEWCLUSTER`)
	wantSQLState(t, ctx, app, "42501", `SHOW NODES`)
	if _, err := root.Exec(ctx, `SHOW NODES`); err != nil {
		t.Fatalf("SHOW NODES as root: %v", err)
	}
}
