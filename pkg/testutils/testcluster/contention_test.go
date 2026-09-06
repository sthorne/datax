package testcluster

import (
	"context"
	"encoding/json"
	"errors"
	"net"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

	"github.com/sthorne/datax/pkg/server"
)

// TestContentionAttribution (issue #154): a serializable database lives
// and dies on retries, and a 40001 rate on its own says nothing about
// what to change. Two transactions made to conflict must move the node's
// serialization-failure count AND land on the statement shape that
// produced them, attributed to the user who ran it — otherwise the
// console's retry hot list is a table that stays empty while the rate
// climbs.
func TestContentionAttribution(t *testing.T) {
	httpLis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	first := true
	tc := StartWithOptions(t, 3, func(cfg *server.Config) {
		if first {
			cfg.HTTPListener = httpLis
			first = false
		}
	})
	ctx, cancel := context.WithTimeout(context.Background(), 180*time.Second)
	defer cancel()

	// Over the wire, not in-process: the attribution is kept by the SQL
	// server's activity tracker, which only sees what its own clients
	// run — which is exactly the scope the console reports.
	open := func() *pgx.Conn {
		t.Helper()
		c, cerr := pgx.Connect(ctx, pgURL(tc, 0))
		if cerr != nil {
			t.Fatalf("pgx connect: %v", cerr)
		}
		return c
	}
	setup := open()
	defer func() { _ = setup.Close(ctx) }()
	for _, q := range []string{
		`CREATE TABLE accounts (id INT8 PRIMARY KEY, balance INT8 NOT NULL)`,
		`INSERT INTO accounts VALUES (1, 100), (2, 100)`,
	} {
		if _, err := setup.Exec(ctx, q); err != nil {
			t.Fatalf("%s: %v", q, err)
		}
	}

	before := activityDoc(t, tc, 0)
	if n := len(before.RetryShapes); n != 0 {
		t.Fatalf("a fresh cluster has retried nothing, got %d shapes: %+v", n, before.RetryShapes)
	}

	// Two sessions writing the same row with a read in between: one of
	// them must be told to retry. Which of the pair loses is a race and
	// both orders are correct, so this runs until one loses.
	a, b := open(), open()
	defer func() { _ = a.Close(ctx) }()
	defer func() { _ = b.Close(ctx) }()
	is40001 := func(err error) bool {
		var pg *pgconn.PgError
		return errors.As(err, &pg) && pg.Code == "40001"
	}
	got := false
	deadline := time.Now().Add(90 * time.Second)
	for !got && time.Now().Before(deadline) {
		var errs []error
		for _, step := range []struct {
			c *pgx.Conn
			q string
		}{
			{a, `BEGIN`}, {b, `BEGIN`},
			{a, `SELECT balance FROM accounts WHERE id = 1`},
			{b, `SELECT balance FROM accounts WHERE id = 1`},
			{a, `UPDATE accounts SET balance = balance - 7 WHERE id = 1`},
			{b, `UPDATE accounts SET balance = balance - 9 WHERE id = 1`},
			{a, `COMMIT`}, {b, `COMMIT`},
		} {
			_, err := step.c.Exec(ctx, step.q)
			errs = append(errs, err)
		}
		for _, err := range errs {
			if is40001(err) {
				got = true
			}
		}
		_, _ = a.Exec(ctx, `ROLLBACK`)
		_, _ = b.Exec(ctx, `ROLLBACK`)
	}
	if !got {
		t.Fatal("no transaction was told to retry in 90s; the test cannot check attribution it never produced")
	}

	after := activityDoc(t, tc, 0)
	if after.Summary == nil || after.Summary.SerializationFailures == 0 {
		t.Fatalf("the rate figure did not move: %+v", after.Summary)
	}
	if len(after.RetryShapes) == 0 {
		t.Fatalf("the node counted %d serialization failures and attributed none of them to a statement: %+v",
			after.Summary.SerializationFailures, after)
	}
	// The shape must be the UPDATE that conflicted, with its literals
	// replaced — that is what makes a hot list a list rather than a log.
	var total uint64
	found := false
	for _, sh := range after.RetryShapes {
		total += sh.Count
		if strings.Contains(sh.Shape, "UPDATE accounts SET balance = (balance - ?) WHERE id = ?") {
			found = true
			if sh.Count == 0 {
				t.Fatalf("shape listed with no failures: %+v", sh)
			}
			if len(sh.Users) == 0 {
				t.Fatalf("shape attributed to no user: %+v", sh)
			}
		}
		if strings.Contains(sh.Shape, "balance - 7") || strings.Contains(sh.Shape, "balance - 9") {
			t.Fatalf("literals survived into the shape, so every retry is its own row: %q", sh.Shape)
		}
	}
	if !found {
		t.Fatalf("the conflicting UPDATE is not among the shapes: %+v", after.RetryShapes)
	}
	// Both writes are the same shape, so the two orders of the race
	// collapse onto one row. Other shapes may also carry failures — a
	// transaction can be told to retry at COMMIT rather than at the
	// statement that conflicted — so this asserts the UPDATE is ONE row,
	// not that it is the only row.
	updates := 0
	for _, sh := range after.RetryShapes {
		if strings.HasPrefix(sh.Shape, "UPDATE accounts") {
			updates++
		}
	}
	if updates != 1 {
		t.Fatalf("the two orders of one shape should be one row, got %d: %+v", updates, after.RetryShapes)
	}
	if total+after.RetryShapesOther != after.Summary.SerializationFailures {
		t.Fatalf("the attribution does not add up to the count: %d attributed + %d overflow != %d counted",
			total, after.RetryShapesOther, after.Summary.SerializationFailures)
	}
	byUser := uint64(0)
	for _, c := range after.RetriesByUser {
		byUser += c
	}
	if byUser != after.Summary.SerializationFailures {
		t.Fatalf("the by-user breakdown does not add up: %d vs %d", byUser, after.Summary.SerializationFailures)
	}
}

// TestIdleTxnDetail (issue #154): "oldest idle transaction: 3m" names
// nobody to talk to. A session left inside an open transaction must be
// reported with the user, the client, and the statement it last ran.
func TestIdleTxnDetail(t *testing.T) {
	httpLis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	first := true
	tc := StartWithOptions(t, 1, func(cfg *server.Config) {
		if first {
			cfg.HTTPListener = httpLis
			first = false
		}
	})
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	// The pgwire path is what tracks connections, so this needs a real
	// client rather than an in-process session.
	conn, cerr := pgx.Connect(ctx, pgURL(tc, 0))
	if cerr != nil {
		t.Fatalf("pgx connect: %v", cerr)
	}
	defer func() { _ = conn.Close(ctx) }()
	exec := func(q string) {
		t.Helper()
		if _, err := conn.Exec(ctx, q); err != nil {
			t.Fatalf("%s: %v", q, err)
		}
	}
	exec(`CREATE TABLE t (id INT8 PRIMARY KEY, v INT8)`)
	exec(`INSERT INTO t VALUES (1, 1)`)
	exec(`BEGIN`)
	exec(`UPDATE t SET v = 2 WHERE id = 1`)
	// Now idle inside an open transaction, holding an intent on id 1.

	deadline := time.Now().Add(30 * time.Second)
	for {
		doc := activityDoc(t, tc, 0)
		if len(doc.IdleTxns) > 0 {
			it := doc.IdleTxns[0]
			if it.PID == 0 {
				t.Fatalf("no pid, so the session cannot be cancelled from what this shows: %+v", it)
			}
			if it.Remote == "" {
				t.Fatalf("no client address: %+v", it)
			}
			if !strings.Contains(it.Last, "UPDATE t SET v") {
				t.Fatalf("the last statement is not reported: %+v", it)
			}
			if it.TxnMillis <= 0 {
				t.Fatalf("the transaction block's age is not reported: %+v", it)
			}
			if it.IdleMillis < 0 {
				t.Fatalf("negative idle time: %+v", it)
			}
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("a session idle inside an open transaction was never reported")
		}
		time.Sleep(200 * time.Millisecond)
	}
	exec(`ROLLBACK`)
}

// TestContentionGating (issue #154): the rate figures are not sensitive
// and stay open to any authenticated user; the statement shapes and the
// sessions behind them carry data, so they stay behind the admin gate.
// The console draws both on one page, and this is the line between them.
func TestContentionGating(t *testing.T) {
	httpLis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	tc, certsDir := startSecureCluster(t, "topsecret", func(i int, cfg *server.Config) {
		if i == 0 {
			cfg.HTTPListener = httpLis
		}
	})
	base := "https://" + tc.Nodes[0].HTTPAddr()
	client := httpsClient(t, certsDir, "")

	deadline := time.Now().Add(30 * time.Second)
	for {
		if code, _, _ := authedGet(t, client, base+"/status", "root", "topsecret"); code == http.StatusOK {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("root basic auth never succeeded")
		}
		time.Sleep(200 * time.Millisecond)
	}

	// A non-admin to be refused with. Created over the wire as root,
	// the way an operator would.
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	rootConn, err := connectSecure(ctx, secureURL(tc, certsDir, "root", "topsecret"))
	if err != nil {
		t.Fatalf("connect as root: %v", err)
	}
	defer func() { _ = rootConn.Close(ctx) }()
	if _, err := rootConn.Exec(ctx, `CREATE USER scraper PASSWORD 'metrics-pw'`); err != nil {
		t.Fatalf("create scraper: %v", err)
	}
	for {
		if code, _, _ := authedGet(t, client, base+"/status", "scraper", "metrics-pw"); code == http.StatusOK {
			break
		}
		if time.Now().After(deadline.Add(30 * time.Second)) {
			t.Fatal("scraper basic auth never succeeded")
		}
		time.Sleep(200 * time.Millisecond)
	}

	// The rates the transactions section charts come from the metrics
	// catalog and table, which any authenticated user may read.
	code, body, _ := authedGet(t, client, base+"/api/metrics", "scraper", "metrics-pw")
	if code != http.StatusOK {
		t.Fatalf("/api/metrics as a non-admin: %d %s, want 200 — a rate is not sensitive", code, body)
	}
	for _, name := range []string{"txn.commits", "txn.aborts", "txn.retries", "kv.batch_p99_us"} {
		if !strings.Contains(body, name) {
			t.Fatalf("%s is not in the catalog a non-admin can read: %s", name, body)
		}
	}
	// The statements and sessions behind them are not.
	if code, body, _ := authedGet(t, client, base+"/api/activity", "scraper", "metrics-pw"); code != http.StatusForbidden {
		t.Fatalf("/api/activity as a non-admin: %d %s, want 403", code, body)
	}
	// And an admin gets the whole document, with both new sections
	// present rather than absent (an absent field reads as "none").
	code, body, _ = authedGet(t, client, base+"/api/activity", "root", "topsecret")
	if code != http.StatusOK {
		t.Fatalf("/api/activity as root: %d %s", code, body)
	}
	var doc server.ActivityStatus
	if err := json.Unmarshal([]byte(body), &doc); err != nil {
		t.Fatal(err)
	}
	if doc.RetryShapes == nil || doc.IdleTxns == nil {
		t.Fatalf("the contention sections are absent rather than empty: %s", body)
	}
}

func activityDoc(t *testing.T, tc *TestCluster, i int) server.ActivityStatus {
	t.Helper()
	code, _, body := httpGet(t, "http://"+tc.Nodes[i].HTTPAddr()+"/api/activity")
	if code != 200 {
		t.Fatalf("n%d /api/activity: HTTP %d: %s", i+1, code, body)
	}
	var doc server.ActivityStatus
	if err := json.Unmarshal([]byte(body), &doc); err != nil {
		t.Fatal(err)
	}
	return doc
}
