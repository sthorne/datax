package testcluster

import (
	"bytes"
	"context"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/sthorne/datax/pkg/keys"
	"github.com/sthorne/datax/pkg/kvpb"
)

// TestKVRoundTripsPerStatement pins the KV batches, and the requests in
// them, that common statement shapes send through the gateway once warm:
// a deterministic guard against a change that adds a round trip to every
// statement — an uncached catalog read, say — which a throughput
// benchmark on a shared machine cannot see. Each shape runs over the
// extended (prepared) and the simple protocol, on one node and on three.
//
// Background work (heartbeats, leases, housekeeping) routes batches
// through the same DB at any moment, and only ever adds to a window, so
// each shape runs several times and the fewest batches any execution sent
// is its own cost. A change that adds a batch to every execution still
// shows.
//
// A batch a statement sends today that it should not — a known regression
// being fixed elsewhere — is not folded into the expected count: the shape
// names it as a knownExtra, which allows exactly one batch matching it per
// statement that resolves a table (extras), and fails once it no longer
// occurs so the allowance goes with the fix. Any other extra batch fails.
func TestKVRoundTripsPerStatement(t *testing.T) {
	for _, nodes := range []int{1, 3} {
		t.Run(fmt.Sprintf("nodes=%d", nodes), func(t *testing.T) {
			testKVRoundTrips(t, nodes)
		})
	}
}

// knownExtra is one batch a statement is known to send beyond what it
// should: a regression with an owner, allowed by name until it is fixed.
type knownExtra struct {
	why string
	// match reports whether a batch is the known extra one.
	match func(ba *kvpb.BatchRequest) bool
}

// schemansLookup: every statement that resolves a table reads its
// database's schema namespace (Get /system/schemans/<db>/"root")
// without the cache, a second batch on a point SELECT. Being fixed in
// parallel; when the fix lands, change this line to
// `var schemansLookup *knownExtra` and every shape that names it expects
// its correct count with no allowance (the names can go later).
var schemansLookup *knownExtra

func isSchemaNamespaceGet(ba *kvpb.BatchRequest) bool {
	lo, hi := keys.AllSchemaNamespaceSpan()
	for _, ru := range ba.Requests {
		in := ru.GetInner()
		if k := in.Header().Key; in.Method() != "Get" || bytes.Compare(k, lo) < 0 || bytes.Compare(k, hi) >= 0 {
			return false
		}
	}
	return len(ba.Requests) > 0
}

// roundTripShape is one statement shape and what it should cost.
type roundTripShape struct {
	name string
	// stmts run in order, each with its args from args(i) for execution
	// i, as one measured execution (an explicit transaction is several).
	stmts []string
	args  func(i int) [][]any
	// batches and requests are the exact KV batches, and requests across
	// them, the execution should send.
	batches, requests int
	// onePhase: the execution's last batch commits its writes in the same
	// batch (an EndTxn beside them), with no separate commit round trip.
	onePhase bool
	// knownExtra, when set, allows exactly extras more batches matching
	// it (default 1: one per statement that resolves a table); nil once
	// the regression is fixed.
	knownExtra *knownExtra
	extras     int
}

// observedBatch is one batch the gateway routed: its methods and keys.
type observedBatch struct {
	methods []string
	keys    []string
}

func (b observedBatch) String() string {
	parts := make([]string, len(b.methods))
	for i := range b.methods {
		parts[i] = b.methods[i] + " " + b.keys[i]
	}
	return "[" + strings.Join(parts, ", ") + "]"
}

func observe(ba *kvpb.BatchRequest) observedBatch {
	var b observedBatch
	for _, ru := range ba.Requests {
		in := ru.GetInner()
		b.methods = append(b.methods, in.Method())
		b.keys = append(b.keys, keys.Pretty(in.Header().Key))
	}
	return b
}

// batchRecorder collects the batches a DB routes while it is armed.
type batchRecorder struct {
	mu    sync.Mutex
	armed bool
	got   []*kvpb.BatchRequest
}

func (r *batchRecorder) observe(_ context.Context, ba *kvpb.BatchRequest, _ *kvpb.BatchResponse) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.armed {
		r.got = append(r.got, ba)
	}
}

func (r *batchRecorder) record(fn func()) []*kvpb.BatchRequest {
	r.mu.Lock()
	r.armed, r.got = true, nil
	r.mu.Unlock()
	fn()
	r.mu.Lock()
	defer r.mu.Unlock()
	r.armed = false
	return r.got
}

func testKVRoundTrips(t *testing.T, nodes int) {
	tc := Start(t, nodes)
	ctx, cancel := context.WithTimeout(context.Background(), 150*time.Second)
	defer cancel()

	ext, err := pgx.Connect(ctx, pgURL(tc, 0))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = ext.Close(ctx) }()
	scfg, err := pgx.ParseConfig(pgURL(tc, 0))
	if err != nil {
		t.Fatal(err)
	}
	scfg.DefaultQueryExecMode = pgx.QueryExecModeSimpleProtocol
	simple, err := pgx.ConnectConfig(ctx, scfg)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = simple.Close(ctx) }()

	for _, q := range []string{
		`CREATE TABLE rt (k INT8 PRIMARY KEY, v INT8, s TEXT)`,
		`CREATE INDEX rt_s ON rt (s)`,
		`INSERT INTO rt SELECT g, g, 's' || g::TEXT FROM generate_series(1, 100) AS g`,
	} {
		if _, err := ext.Exec(ctx, q); err != nil {
			t.Fatalf("%s: %v", q, err)
		}
	}

	one := func(f func(i int) []any) func(i int) [][]any {
		return func(i int) [][]any { return [][]any{f(i)} }
	}
	shapes := []roundTripShape{
		{
			name:    "point SELECT by primary key",
			stmts:   []string{`SELECT v FROM rt WHERE k = $1`},
			args:    one(func(i int) []any { return []any{int64(1 + i%50)} }),
			batches: 1, requests: 1,
			knownExtra: schemansLookup,
		},
		{
			name:  "point UPDATE by primary key, autocommit",
			stmts: []string{`UPDATE rt SET v = v + 1 WHERE k = $1`},
			args:  one(func(i int) []any { return []any{int64(1 + i%50)} }),
			// The row read, then the write committed in one batch (s is
			// unchanged, so no index entry).
			batches: 2, requests: 3, onePhase: true,
			knownExtra: schemansLookup,
		},
		{
			name:  "point INSERT, autocommit",
			stmts: []string{`INSERT INTO rt VALUES ($1, 0, 'new')`},
			args:  one(func(i int) []any { return []any{int64(100000 + i)} }),
			// The primary key's existence check, then the row and its
			// index entry committed in one batch.
			batches: 2, requests: 4, onePhase: true,
			knownExtra: schemansLookup,
		},
		{
			name:  "DELETE by primary key",
			stmts: []string{`DELETE FROM rt WHERE k = $1`},
			args:  one(func(i int) []any { return []any{int64(100000 + i)} }),
			// The row read, then the row and its index entry deleted and
			// committed in one batch.
			batches: 2, requests: 4, onePhase: true,
			knownExtra: schemansLookup,
		},
		{
			name:  "SELECT through a secondary index (index join)",
			stmts: []string{`SELECT v FROM rt WHERE s = $1`},
			args:  one(func(i int) []any { return []any{fmt.Sprintf("s%d", 51+i%50)} }),
			// The index scan, then the primary fetch.
			batches: 2, requests: 2,
			knownExtra: schemansLookup,
		},
		{
			name:  "two-statement explicit transaction",
			stmts: []string{`BEGIN`, `SELECT v FROM rt WHERE k = $1`, `UPDATE rt SET v = v + 1 WHERE k = $1`, `COMMIT`},
			args: func(i int) [][]any {
				k := int64(51 + i%50)
				return [][]any{nil, {k}, {k}, nil}
			},
			// SELECT's read; UPDATE's read and write; COMMIT's EndTxn;
			// and the intent resolution the commit sends before it
			// returns.
			batches: 5, requests: 5,
			knownExtra: schemansLookup, extras: 2, // SELECT and UPDATE each
		},
		{
			name:    "SELECT 1", // no KV at all
			stmts:   []string{`SELECT 1`},
			args:    one(func(int) []any { return nil }),
			batches: 0, requests: 0,
		},
	}

	gw := tc.Nodes[0].DB()
	rec := &batchRecorder{}
	gw.TestingSetSendObserver(rec.observe)
	defer gw.TestingSetSendObserver(nil)

	const warmup, reps = 3, 12
	for _, proto := range []struct {
		name string
		conn *pgx.Conn
	}{{"extended", ext}, {"simple", simple}} {
		for _, sh := range shapes {
			t.Run(proto.name+"/"+sh.name, func(t *testing.T) {
				run := func(i int) {
					t.Helper()
					args := sh.args(i)
					for j, q := range sh.stmts {
						var a []any
						if j < len(args) {
							a = args[j]
						}
						rows, err := proto.conn.Query(ctx, q, a...)
						if err == nil {
							for rows.Next() {
							}
							rows.Close()
							err = rows.Err()
						}
						if err != nil {
							t.Fatalf("%s %v: %v", q, a, err)
						}
					}
				}
				i := 0
				for ; i < warmup; i++ {
					run(i)
				}
				var best []*kvpb.BatchRequest
				for n := 0; n < reps; n++ {
					got := rec.record(func() { run(i) })
					i++
					if best == nil || len(got) < len(best) {
						best = got
					}
				}
				checkRoundTrips(t, sh, best)
			})
		}
	}
}

// checkRoundTrips compares the batches of the cheapest execution with
// the shape's expectation, listing every batch on a mismatch.
func checkRoundTrips(t *testing.T, sh roundTripShape, got []*kvpb.BatchRequest) {
	t.Helper()
	var list []string
	for i, ba := range got {
		list = append(list, fmt.Sprintf("  %d: %s", i+1, observe(ba)))
	}
	listing := strings.Join(list, "\n")
	if len(list) == 0 {
		listing = "  (none)"
	}
	t.Logf("%s: %d batches\n%s", sh.name, len(got), listing)

	own, extras := got, 0
	if sh.knownExtra != nil {
		own = nil
		for _, ba := range got {
			if sh.knownExtra.match(ba) {
				extras++
				continue
			}
			own = append(own, ba)
		}
		want := sh.extras
		if want == 0 {
			want = 1
		}
		switch {
		case extras == 0:
			t.Errorf("%s: the known extra batch (%s) no longer occurs: the regression is fixed, so remove the allowance (var schemansLookup *knownExtra)\nbatches:\n%s",
				sh.name, sh.knownExtra.why, listing)
		case extras != want:
			t.Errorf("%s: %d batches match the known extra (%s), allowed %d\nbatches:\n%s",
				sh.name, extras, sh.knownExtra.why, want, listing)
		}
	}
	requests := 0
	for _, ba := range own {
		requests += len(ba.Requests)
	}
	allowed := ""
	if extras > 0 {
		allowed = fmt.Sprintf(", besides the %d allowed (%s)", extras, sh.knownExtra.why)
	}
	if len(own) != sh.batches || requests != sh.requests {
		t.Errorf("%s: %d KV batches with %d requests, want %d batches with %d requests%s\nbatches:\n%s",
			sh.name, len(own), requests, sh.batches, sh.requests, allowed, listing)
	}
	if sh.onePhase && len(own) > 0 {
		last := observe(own[len(own)-1])
		if !(len(last.methods) > 1 && last.methods[len(last.methods)-1] == "EndTxn") {
			t.Errorf("%s: not a one-phase commit: the last batch %s does not carry the EndTxn beside the writes\nbatches:\n%s",
				sh.name, last, listing)
		}
	}
}
