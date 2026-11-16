package testcluster

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"errors"
	"io"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

	"github.com/sthorne/datax/pkg/server"
	"github.com/sthorne/datax/pkg/sql"
	"github.com/sthorne/datax/pkg/sql/parser"
)

// loTestGrace is the upload grace period these tests run under: long
// enough that an upload a test is in the middle of is never taken for
// abandoned on a loaded machine (its heartbeat is a tenth of it), short
// enough to wait out where a test wants to see one reclaimed.
const loTestGrace = 4 * time.Second

// startLOCluster starts a 3-node cluster whose large-object sweeper
// runs only when the test drives it.
func startLOCluster(t testing.TB) *TestCluster {
	t.Helper()
	// Registered before the cluster's own cleanup, so it runs after the
	// nodes have stopped and no session is left to read the variable.
	sql.TestingLargeObjectHeartbeat = loTestGrace / 10
	t.Cleanup(func() { sql.TestingLargeObjectHeartbeat = 0 })
	return StartWithOptions(t, 3, func(c *server.Config) {
		c.LargeObjectSweepInterval = -1
		c.LargeObjectUploadGrace = loTestGrace
	})
}

func randomBytes(t testing.TB, n int) []byte {
	t.Helper()
	b := make([]byte, n)
	if _, err := rand.Read(b); err != nil {
		t.Fatal(err)
	}
	return b
}

func pgConn(t testing.TB, ctx context.Context, tc *TestCluster, node int) *pgx.Conn {
	t.Helper()
	conn, err := pgx.Connect(ctx, pgURL(tc, node))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close(context.Background()) })
	return conn
}

// loOf is tx.LargeObjects() as a value whose methods can be called.
func loOf(tx pgx.Tx) *pgx.LargeObjects {
	los := tx.LargeObjects()
	return &los
}

// loStore writes data as a new large object in one transaction, in
// writes of step bytes, and returns its id.
func loStore(t testing.TB, ctx context.Context, conn *pgx.Conn, data []byte, step int) uint32 {
	t.Helper()
	tx, err := conn.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	los := tx.LargeObjects()
	oid, err := los.Create(ctx, 0)
	if err != nil {
		t.Fatalf("lo_create: %v", err)
	}
	obj, err := los.Open(ctx, oid, pgx.LargeObjectModeWrite)
	if err != nil {
		t.Fatalf("lo_open: %v", err)
	}
	for off := 0; off < len(data); off += step {
		end := off + step
		if end > len(data) {
			end = len(data)
		}
		if _, err := obj.Write(data[off:end]); err != nil {
			t.Fatalf("lowrite at %d: %v", off, err)
		}
	}
	if err := obj.Close(); err != nil {
		t.Fatalf("lo_close: %v", err)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatalf("commit: %v", err)
	}
	return oid
}

// loFetch reads a whole large object in reads of step bytes.
func loFetch(t testing.TB, ctx context.Context, conn *pgx.Conn, oid uint32, step int) []byte {
	t.Helper()
	tx, err := conn.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	los := tx.LargeObjects()
	obj, err := los.Open(ctx, oid, pgx.LargeObjectModeRead)
	if err != nil {
		t.Fatalf("lo_open %d: %v", oid, err)
	}
	var out bytes.Buffer
	buf := make([]byte, step)
	for {
		n, err := obj.Read(buf)
		out.Write(buf[:n])
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			t.Fatalf("loread at %d: %v", out.Len(), err)
		}
	}
	return out.Bytes()
}

// loReadFull fills p from obj, failing with the first error a read
// meets. Not io.ReadFull: pgx's LargeObject.Read reports the whole
// buffer as read when its loread fails (it counts the preallocated scan
// target), and io.ReadFull drops an error that comes with a full count.
// The statement's error — a 40001 on a transaction a conflict aborted —
// would be lost, and the client's next call would meet 25P02 instead.
func loReadFull(obj *pgx.LargeObject, p []byte) error {
	for n := 0; n < len(p); {
		m, err := obj.Read(p[n:])
		if errors.Is(err, io.EOF) {
			return io.ErrUnexpectedEOF
		}
		if err != nil {
			return err
		}
		n += m
	}
	return nil
}

// loCounts is what the large-object tables hold, read as root.
func loCounts(t testing.TB, ctx context.Context, conn *pgx.Conn) (objects, pages, pending int64) {
	t.Helper()
	for q, dst := range map[string]*int64{
		`SELECT count(*) FROM datax_largeobject_metadata`:               &objects,
		`SELECT count(*) FROM (SELECT pageno FROM datax_largeobject) p`: &pages,
		`SELECT count(*) FROM datax_largeobject_pending`:                &pending,
	} {
		if err := conn.QueryRow(ctx, q).Scan(dst); err != nil {
			// Nothing has made the tables yet.
			var pgErr *pgconn.PgError
			if errors.As(err, &pgErr) && pgErr.Code == "42P01" {
				*dst = 0
				continue
			}
			t.Fatalf("%s: %v", q, err)
		}
	}
	return
}

// TestLargeObjectRoundTrip: objects of sizes around the page boundary
// and several pages long come back byte for byte, written through one
// gateway and read through another, over the extended protocol (binary
// parameters and results) and the simple one (text).
func TestLargeObjectRoundTrip(t *testing.T) {
	tc := startLOCluster(t)
	ctx, cancel := context.WithTimeout(context.Background(), 240*time.Second)
	defer cancel()
	const page = sql.LargeObjectPageSize
	sizes := []int{0, 1, page - 1, page, page + 1, 3*page + 17, 5 << 20}
	for _, mode := range []pgx.QueryExecMode{pgx.QueryExecModeCacheStatement, pgx.QueryExecModeSimpleProtocol} {
		cfg, err := pgx.ParseConfig(pgURL(tc, 0))
		if err != nil {
			t.Fatal(err)
		}
		cfg.DefaultQueryExecMode = mode
		w, err := pgx.ConnectConfig(ctx, cfg)
		if err != nil {
			t.Fatal(err)
		}
		cfg2, _ := pgx.ParseConfig(pgURL(tc, 2))
		cfg2.DefaultQueryExecMode = mode
		r, err := pgx.ConnectConfig(ctx, cfg2)
		if err != nil {
			t.Fatal(err)
		}
		for _, size := range sizes {
			data := randomBytes(t, size)
			// Writes and reads that do not line up with the pages, or
			// with each other.
			oid := loStore(t, ctx, w, data, 100_003)
			if got := loFetch(t, ctx, r, oid, 77_777); !bytes.Equal(got, data) {
				t.Fatalf("mode %v, %d bytes: read back %d bytes that differ", mode, size, len(got))
			}
			var whole []byte
			if err := r.QueryRow(ctx, `SELECT lo_get($1)`, oid).Scan(&whole); err != nil || !bytes.Equal(whole, data) {
				t.Fatalf("mode %v, %d bytes: lo_get returned %d bytes that differ, %v", mode, size, len(whole), err)
			}
		}
		_ = w.Close(ctx)
		_ = r.Close(ctx)
	}
}

// TestLargeObjectDescriptorSemantics: seeking, telling, truncating,
// holes, overwriting part of a committed object, and the one-call
// functions behave as PostgreSQL's do.
func TestLargeObjectDescriptorSemantics(t *testing.T) {
	tc := startLOCluster(t)
	ctx, cancel := context.WithTimeout(context.Background(), 180*time.Second)
	defer cancel()
	conn := pgConn(t, ctx, tc, 0)
	const page = sql.LargeObjectPageSize

	// A model kept beside the object: every step is checked against it.
	model := randomBytes(t, 2*page+1000)
	oid := loStore(t, ctx, conn, model, 64<<10)
	check := func(what string) {
		t.Helper()
		if got := loFetch(t, ctx, conn, oid, 1<<20); !bytes.Equal(got, model) {
			t.Fatalf("%s: object is %d bytes and differs from the model's %d", what, len(got), len(model))
		}
	}

	// Overwrite across a page boundary of the committed object, in a
	// transaction that rolls back, then in one that commits.
	patch := randomBytes(t, 5000)
	for _, commit := range []bool{false, true} {
		tx, err := conn.Begin(ctx)
		if err != nil {
			t.Fatal(err)
		}
		obj, err := loOf(tx).Open(ctx, oid, pgx.LargeObjectModeWrite)
		if err != nil {
			t.Fatal(err)
		}
		if pos, err := obj.Seek(page-2500, io.SeekStart); err != nil || pos != page-2500 {
			t.Fatalf("seek: %d, %v", pos, err)
		}
		if _, err := obj.Write(patch); err != nil {
			t.Fatal(err)
		}
		if pos, err := obj.Tell(); err != nil || pos != page+2500 {
			t.Fatalf("tell after write: %d, %v", pos, err)
		}
		// The writer reads its own write before committing.
		if _, err := obj.Seek(page-2500, io.SeekStart); err != nil {
			t.Fatal(err)
		}
		own := make([]byte, len(patch))
		if err := loReadFull(obj, own); err != nil || !bytes.Equal(own, patch) {
			t.Fatalf("the writer does not read its own write: %v", err)
		}
		if commit {
			if err := tx.Commit(ctx); err != nil {
				t.Fatal(err)
			}
			copy(model[page-2500:], patch)
		} else if err := tx.Rollback(ctx); err != nil {
			t.Fatal(err)
		}
		check("overwrite, commit=" + map[bool]string{true: "yes", false: "no"}[commit])
	}

	// Seek from the end; a write past the end leaves a hole of zeros.
	tx, err := conn.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	obj, err := loOf(tx).Open(ctx, oid, pgx.LargeObjectModeWrite)
	if err != nil {
		t.Fatal(err)
	}
	if pos, err := obj.Seek(0, io.SeekEnd); err != nil || pos != int64(len(model)) {
		t.Fatalf("seek to end: %d, %v (want %d)", pos, err, len(model))
	}
	tail := randomBytes(t, 300)
	if _, err := obj.Seek(3*page+10, io.SeekCurrent); err != nil {
		t.Fatal(err)
	}
	if _, err := obj.Write(tail); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	model = append(append(model, make([]byte, 3*page+10)...), tail...)
	check("write past the end")

	// Truncate: shorter (into the hole, mid-page), then longer.
	for _, size := range []int{4*page + 123, page, page + 4000, 0, 10} {
		tx, err := conn.Begin(ctx)
		if err != nil {
			t.Fatal(err)
		}
		obj, err := loOf(tx).Open(ctx, oid, pgx.LargeObjectModeWrite)
		if err != nil {
			t.Fatal(err)
		}
		if err := obj.Truncate(int64(size)); err != nil {
			t.Fatalf("truncate to %d: %v", size, err)
		}
		if err := tx.Commit(ctx); err != nil {
			t.Fatal(err)
		}
		if size <= len(model) {
			model = model[:size]
		} else {
			model = append(model, make([]byte, size-len(model))...)
		}
		check("truncate")
	}

	// The one-call functions, each its own transaction.
	var made uint32
	if err := conn.QueryRow(ctx, `SELECT lo_from_bytea(0, $1)`, []byte("hello, large world")).Scan(&made); err != nil {
		t.Fatal(err)
	}
	if _, err := conn.Exec(ctx, `SELECT lo_put($1, 7, $2)`, made, []byte("LARGE")); err != nil {
		t.Fatal(err)
	}
	var whole, part []byte
	if err := conn.QueryRow(ctx, `SELECT lo_get($1)`, made).Scan(&whole); err != nil {
		t.Fatal(err)
	}
	if err := conn.QueryRow(ctx, `SELECT lo_get($1, 7, 5)`, made).Scan(&part); err != nil {
		t.Fatal(err)
	}
	if string(whole) != "hello, LARGE world" || string(part) != "LARGE" {
		t.Fatalf("lo_get: %q, %q", whole, part)
	}

	// Errors carry PostgreSQL's SQLSTATEs.
	for _, c := range []struct{ q, code string }{
		{`SELECT lo_open(4242, 262144)`, "42704"},
		{`SELECT lo_unlink(4242)`, "42704"},
		{`SELECT loread(99, 10)`, "42704"},
		{`SELECT lo_create(` + itoa(int(made)) + `)`, "42710"},
	} {
		if _, err := conn.Exec(ctx, c.q); sqlState(err) != c.code {
			t.Fatalf("%s: got %v, want SQLSTATE %s", c.q, err, c.code)
		}
	}
	// A descriptor opened read-only refuses a write, and one does not
	// outlive its transaction.
	tx, err = conn.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	ro, err := loOf(tx).Open(ctx, made, pgx.LargeObjectModeRead)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := ro.Write([]byte("x")); sqlState(err) != "55000" {
		t.Fatalf("write through a read-only descriptor: %v", err)
	}
	_ = tx.Rollback(ctx)
	tx, err = conn.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec(ctx, `SELECT loread(0, 10)`); sqlState(err) != "42704" {
		t.Fatalf("a descriptor of an earlier transaction still answers: %v", err)
	}
	_ = tx.Rollback(ctx)

	// A read-only transaction reads objects and changes none.
	tx, err = conn.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec(ctx, `SET transaction_read_only = on`); err != nil {
		t.Fatal(err)
	}
	if got, err := tx.Exec(ctx, `SELECT lo_get($1, 0, 5)`, made); err != nil {
		t.Fatalf("lo_get in a read-only transaction: %v, %v", got, err)
	}
	if _, err := tx.Exec(ctx, `SELECT lo_create(0)`); sqlState(err) != "25006" {
		t.Fatalf("lo_create in a read-only transaction: %v", err)
	}
	_ = tx.Rollback(ctx)

	// The tables cannot be changed through SQL, by root either.
	for _, q := range []string{
		`DELETE FROM datax_largeobject`,
		`UPDATE datax_largeobject_metadata SET owner = 'x'`,
		`DROP TABLE datax_largeobject_pending`,
	} {
		if _, err := conn.Exec(ctx, q); sqlState(err) != "42501" {
			t.Fatalf("%s: got %v, want 42501", q, err)
		}
	}
}

// TestLargeObjectTransactionLimit: what a transaction writes into an
// existing object it writes as itself, and that is bounded (54000);
// the same bytes go into a new object without limit, and a refused
// write leaves the existing object as it was.
func TestLargeObjectTransactionLimit(t *testing.T) {
	const limit = 1 << 20
	sql.TestingLargeObjectTxnWriteLimit = limit
	t.Cleanup(func() { sql.TestingLargeObjectTxnWriteLimit = 0 })
	tc := startLOCluster(t)
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()
	conn := pgConn(t, ctx, tc, 0)
	small := randomBytes(t, 1000)
	oid := loStore(t, ctx, conn, small, 1<<20)
	big := randomBytes(t, 3*limit)

	tx, err := conn.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	obj, err := loOf(tx).Open(ctx, oid, pgx.LargeObjectModeWrite)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := obj.Write(big); sqlState(err) != "54000" {
		t.Fatalf("writing %d bytes into an existing object in one transaction: %v (want 54000)", len(big), err)
	}
	_ = tx.Rollback(ctx)
	if got := loFetch(t, ctx, conn, oid, 1<<20); !bytes.Equal(got, small) {
		t.Fatal("a refused write changed the object")
	}
	if _, err := conn.Exec(ctx, `SELECT lo_from_bytea(0, $1)`, big); sqlState(err) != "54000" {
		t.Fatalf("lo_from_bytea of %d bytes outside a transaction block: %v (want 54000)", len(big), err)
	}
	// The same bytes as a new object in a transaction block.
	if got := loFetch(t, ctx, conn, loStore(t, ctx, conn, big, 1<<20), 1<<20); !bytes.Equal(got, big) {
		t.Fatal("a new object larger than the transaction limit read back differently")
	}
	// A read larger than the statement may hold is refused by the
	// statement's memory limit, not attempted.
	if _, err := conn.Exec(ctx, `SET statement_memory_limit = '1MB'`); err != nil {
		t.Fatal(err)
	}
	huge := loStore(t, ctx, conn, big, 1<<20)
	if _, err := conn.Exec(ctx, `SELECT lo_get($1)`, huge); sqlState(err) != "53200" {
		t.Fatalf("lo_get of %d bytes under a 1MB statement_memory_limit: %v (want 53200)", len(big), err)
	}
	// In pieces it reads under the same limit.
	if got := loFetch(t, ctx, conn, huge, 256<<10); !bytes.Equal(got, big) {
		t.Fatal("reading in pieces under the memory limit returned different bytes")
	}
}

// TestLargeObjectOwnership: an object belongs to the role that made it.
func TestLargeObjectOwnership(t *testing.T) {
	tc := startLOCluster(t)
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()
	root := pgConn(t, ctx, tc, 0)
	for _, q := range []string{`CREATE USER ann`, `CREATE USER bob`} {
		if _, err := root.Exec(ctx, q); err != nil {
			t.Fatal(err)
		}
	}
	as := func(user string) *pgx.Conn {
		conn, err := pgx.Connect(ctx, strings.Replace(pgURL(tc, 1), "root@", user+"@", 1))
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = conn.Close(context.Background()) })
		return conn
	}
	ann, bob := as("ann"), as("bob")
	data := randomBytes(t, 400_000)
	oid := loStore(t, ctx, ann, data, 1<<20)
	if got := loFetch(t, ctx, ann, oid, 1<<20); !bytes.Equal(got, data) {
		t.Fatal("the owner cannot read its object back")
	}
	for _, q := range []string{`SELECT lo_get($1)`, `SELECT lo_unlink($1)`, `SELECT lo_open($1, 262144)`, `SELECT lo_put($1, 0, '\x00')`} {
		if _, err := bob.Exec(ctx, q, oid); sqlState(err) != "42501" {
			t.Fatalf("bob: %s: got %v, want 42501", q, err)
		}
	}
	// Nor through the tables: reading them takes an admin or a grant.
	if _, err := bob.Exec(ctx, `SELECT count(*) FROM datax_largeobject`); sqlState(err) != "42501" {
		t.Fatalf("bob reads the page table: %v", err)
	}
	// pg_largeobject_metadata lists the objects (to an admin: the
	// catalogs are not open to other roles).
	var listed int64
	if err := root.QueryRow(ctx, `SELECT count(*) FROM pg_largeobject_metadata WHERE oid = $1`, oid).Scan(&listed); err != nil || listed != 1 {
		t.Fatalf("pg_largeobject_metadata lists the object %d times, %v", listed, err)
	}
	// An admin may do what the owner may.
	if got := loFetch(t, ctx, root, oid, 1<<20); !bytes.Equal(got, data) {
		t.Fatal("root cannot read ann's object")
	}
}

// TestLargeObjectInterruptedWrite is the issue's second acceptance
// item: a write that does not reach its commit — rolled back, or its
// connection cut mid-stream — leaves no visible object and, once the
// sweeper has run, no pages; an upload still in flight is left alone by
// the sweeper and commits whole; an unlinked object's pages go too.
func TestLargeObjectInterruptedWrite(t *testing.T) {
	tc := startLOCluster(t)
	ctx, cancel := context.WithTimeout(context.Background(), 240*time.Second)
	defer cancel()
	root := pgConn(t, ctx, tc, 1)
	sweep := func() sql.LargeObjectSweep {
		t.Helper()
		res, err := tc.Nodes[2].RunLargeObjectSweepOnce(ctx)
		if err != nil {
			t.Fatalf("sweep: %v", err)
		}
		return res
	}
	const page = sql.LargeObjectPageSize
	data := randomBytes(t, 6*page+99)

	// begin starts an upload and stops partway: pages are stored (they
	// are written as they fill), nothing is committed.
	begin := func(conn *pgx.Conn) (pgx.Tx, *pgx.LargeObject, uint32) {
		t.Helper()
		tx, err := conn.Begin(ctx)
		if err != nil {
			t.Fatal(err)
		}
		oid, err := loOf(tx).Create(ctx, 0)
		if err != nil {
			t.Fatal(err)
		}
		obj, err := loOf(tx).Open(ctx, oid, pgx.LargeObjectModeWrite)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := obj.Write(data[:4*page+5]); err != nil {
			t.Fatal(err)
		}
		return tx, obj, oid
	}
	invisible := func(what string, oid uint32) {
		t.Helper()
		if _, err := root.Exec(ctx, `SELECT lo_open($1, 262144)`, oid); sqlState(err) != "42704" {
			t.Fatalf("%s: the object is visible to another session: %v", what, err)
		}
	}

	// 1. Rolled back.
	conn := pgConn(t, ctx, tc, 0)
	tx, _, oid := begin(conn)
	if objects, pages, pending := loCounts(t, ctx, root); objects != 0 || pages < 4 || pending != 1 {
		t.Fatalf("mid-upload: %d objects, %d pages stored (want at least 4), %d pending", objects, pages, pending)
	}
	invisible("mid-upload", oid)
	if err := tx.Rollback(ctx); err != nil {
		t.Fatal(err)
	}
	invisible("after rollback", oid)
	if objects, pages, pending := loCounts(t, ctx, root); objects != 0 || pages < 4 || pending != 1 {
		t.Fatalf("after rollback, before the sweep: %d objects, %d pages, %d pending", objects, pages, pending)
	}
	if res := sweep(); res.Contents != 1 || res.Pages < 4 {
		t.Fatalf("sweep after rollback reclaimed %+v", res)
	}
	if objects, pages, pending := loCounts(t, ctx, root); objects != 0 || pages != 0 || pending != 0 {
		t.Fatalf("after the sweep: %d objects, %d pages, %d pending", objects, pages, pending)
	}

	// 2. The connection is cut mid-stream, with no word to the server.
	conn2, err := pgx.Connect(ctx, pgURL(tc, 0))
	if err != nil {
		t.Fatal(err)
	}
	_, _, oid = begin(conn2)
	if err := conn2.PgConn().Conn().Close(); err != nil {
		t.Fatal(err)
	}
	invisible("after the connection was cut", oid)
	deadline := time.Now().Add(60 * time.Second)
	for {
		sweep()
		_, pages, pending := loCounts(t, ctx, root)
		if pages == 0 && pending == 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("a cut upload's pages were never reclaimed: %d pages, %d pending", pages, pending)
		}
		time.Sleep(200 * time.Millisecond)
	}

	// 3. An upload in flight is not an orphan: it outlasts the grace
	// period as long as its client keeps going, whatever the sweeper
	// does meanwhile, and commits whole.
	tx, obj, oid := begin(conn)
	rest := data[4*page+5:]
	for started := time.Now(); time.Since(started) < loTestGrace+loTestGrace/2; {
		if res := sweep(); res.Contents != 0 || res.Pages != 0 {
			t.Fatalf("the sweeper reclaimed %+v from an upload in flight", res)
		}
		if len(rest) > 1000 {
			if _, err := obj.Write(rest[:1000]); err != nil {
				t.Fatal(err)
			}
			rest = rest[1000:]
		}
		time.Sleep(loTestGrace / 8)
	}
	if _, err := obj.Write(rest); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	sweep()
	if got := loFetch(t, ctx, root, oid, 1<<20); !bytes.Equal(got, data) {
		t.Fatalf("an upload the sweeper ran beside read back %d bytes that differ from the %d written", len(got), len(data))
	}
	if objects, pages, pending := loCounts(t, ctx, root); objects != 1 || pages != 7 || pending != 0 {
		t.Fatalf("after the commit: %d objects, %d pages, %d pending", objects, pages, pending)
	}

	// 3b. An upload whose client goes quiet for longer than the grace
	// period is what a dead gateway's upload looks like, and is
	// reclaimed. If the client does come back, it is told — it cannot
	// commit an object whose pages are gone.
	idleConn := pgConn(t, ctx, tc, 2)
	idleTx, idleObj, idleOID := begin(idleConn)
	time.Sleep(loTestGrace + loTestGrace/4)
	if res := sweep(); res.Contents != 1 || res.Pages < 4 {
		t.Fatalf("sweep of an idle upload reclaimed %+v", res)
	}
	if _, err := idleObj.Write(data[:10]); sqlState(err) != "55000" {
		t.Fatalf("a write to a reclaimed upload: %v (want 55000)", err)
	}
	_ = idleTx.Rollback(ctx)
	invisible("after an idle upload was reclaimed", idleOID)
	// The same, with the client going straight to COMMIT.
	idleTx, _, idleOID = begin(idleConn)
	time.Sleep(loTestGrace + loTestGrace/4)
	if res := sweep(); res.Contents != 1 {
		t.Fatalf("sweep of an idle upload reclaimed %+v", res)
	}
	if err := idleTx.Commit(ctx); sqlState(err) != "55000" {
		t.Fatalf("commit of a reclaimed upload: %v (want 55000)", err)
	}
	invisible("after a reclaimed upload's commit was refused", idleOID)
	if objects, pages, pending := loCounts(t, ctx, root); objects != 1 || pages != 7 || pending != 0 {
		t.Fatalf("after the idle uploads: %d objects, %d pages, %d pending", objects, pages, pending)
	}

	// 4. Unlink: in a transaction that rolls back the object stays;
	// committed, it is gone at once and its pages after the sweep.
	tx, err = conn.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := loOf(tx).Unlink(ctx, oid); err != nil {
		t.Fatal(err)
	}
	if err := tx.Rollback(ctx); err != nil {
		t.Fatal(err)
	}
	if res := sweep(); res.Contents != 0 {
		t.Fatalf("a rolled-back unlink gave the sweeper %+v", res)
	}
	if got := loFetch(t, ctx, root, oid, 1<<20); !bytes.Equal(got, data) {
		t.Fatal("a rolled-back unlink changed the object")
	}
	if _, err := conn.Exec(ctx, `SELECT lo_unlink($1)`, oid); err != nil {
		t.Fatal(err)
	}
	invisible("after unlink", oid)
	if res := sweep(); res.Contents != 1 || res.Pages != 7 {
		t.Fatalf("sweep after unlink reclaimed %+v", res)
	}
	if objects, pages, pending := loCounts(t, ctx, root); objects != 0 || pages != 0 || pending != 0 {
		t.Fatalf("after unlink and sweep: %d objects, %d pages, %d pending", objects, pages, pending)
	}

	// 5. An id is free again after its object is unlinked, and the new
	// object under it is not the old one's pages — including while the
	// old pages are still waiting for the sweeper.
	first, second := randomBytes(t, 2*page), randomBytes(t, page/2)
	if _, err := conn.Exec(ctx, `SELECT lo_from_bytea(777777, $1)`, first); err != nil {
		t.Fatal(err)
	}
	if _, err := conn.Exec(ctx, `SELECT lo_unlink(777777)`); err != nil {
		t.Fatal(err)
	}
	if _, err := conn.Exec(ctx, `SELECT lo_from_bytea(777777, $1)`, second); err != nil {
		t.Fatal(err)
	}
	sweep()
	if got := loFetch(t, ctx, root, 777777, 1<<20); !bytes.Equal(got, second) {
		t.Fatalf("a reused id reads back %d bytes, not the second object's %d", len(got), len(second))
	}

	// 6. Created and unlinked in one transaction: nothing remains.
	tx, _, oid = begin(conn)
	if err := loOf(tx).Unlink(ctx, oid); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	sweep()
	if objects, pages, pending := loCounts(t, ctx, root); objects != 1 || pages != 1 || pending != 0 {
		t.Fatalf("at the end: %d objects, %d pages, %d pending (want the one small object)", objects, pages, pending)
	}
}

// TestLargeObjectSavepoint: rolling back to a savepoint undoes changes
// to an existing object like any other write; a transaction that has
// streamed a new object cannot be partly undone and is refused its
// commit.
func TestLargeObjectSavepoint(t *testing.T) {
	tc := startLOCluster(t)
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()
	conn := pgConn(t, ctx, tc, 0)
	data := randomBytes(t, 300_000)
	oid := loStore(t, ctx, conn, data, 1<<20)

	tx, err := conn.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec(ctx, `SAVEPOINT a`); err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec(ctx, `SELECT lo_put($1, 10, '\xffffffff')`, oid); err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec(ctx, `ROLLBACK TO SAVEPOINT a`); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	if got := loFetch(t, ctx, conn, oid, 1<<20); !bytes.Equal(got, data) {
		t.Fatal("a write rolled back to a savepoint is in the object")
	}

	tx, err = conn.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	fresh, err := loOf(tx).Create(ctx, 0)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec(ctx, `SAVEPOINT a`); err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec(ctx, `ROLLBACK TO SAVEPOINT a`); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(ctx); sqlState(err) != "25P02" {
		t.Fatalf("commit after rolling back to a savepoint under a streamed object: %v", err)
	}
	if _, err := conn.Exec(ctx, `SELECT lo_open($1, 262144)`, fresh); sqlState(err) != "42704" {
		t.Fatalf("the object of a refused commit exists: %v", err)
	}
}

// TestLargeObjectConcurrent: sessions on every gateway create, read
// back and unlink objects at once while the sweeper runs; two writers
// of one existing object serialize; afterwards exactly the kept
// objects' pages remain.
func TestLargeObjectConcurrent(t *testing.T) {
	tc := startLOCluster(t)
	ctx, cancel := context.WithTimeout(context.Background(), 240*time.Second)
	defer cancel()
	root := pgConn(t, ctx, tc, 0)
	const page = sql.LargeObjectPageSize
	const workers, rounds = 6, 4

	stopSweep := make(chan struct{})
	var sweepWG sync.WaitGroup
	sweepWG.Add(1)
	go func() {
		defer sweepWG.Done()
		for {
			select {
			case <-stopSweep:
				return
			case <-time.After(50 * time.Millisecond):
				_, _ = tc.Nodes[1].RunLargeObjectSweepOnce(ctx)
			}
		}
	}()

	type kept struct {
		oid uint32
		sum [32]byte
		n   int
	}
	var mu sync.Mutex
	var keep []kept
	var wg sync.WaitGroup
	errs := make(chan error, workers)
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			conn, err := pgx.Connect(ctx, pgURL(tc, w%3))
			if err != nil {
				errs <- err
				return
			}
			defer func() { _ = conn.Close(ctx) }()
			for r := 0; r < rounds; r++ {
				data := randomBytes(t, (w+1)*page/2+r*1000+1)
				oid := loStore(t, ctx, conn, data, 90_000)
				if got := loFetch(t, ctx, conn, oid, 200_000); !bytes.Equal(got, data) {
					errs <- errors.New("an object read back differs from what its session wrote")
					return
				}
				if r%2 == 0 {
					if _, err := conn.Exec(ctx, `SELECT lo_unlink($1)`, oid); err != nil {
						errs <- err
						return
					}
					continue
				}
				mu.Lock()
				keep = append(keep, kept{oid, sha256.Sum256(data), len(data)})
				mu.Unlock()
			}
		}(w)
	}
	wg.Wait()
	close(stopSweep)
	sweepWG.Wait()
	select {
	case err := <-errs:
		t.Fatal(err)
	default:
	}
	if _, err := tc.Nodes[1].RunLargeObjectSweepOnce(ctx); err != nil {
		t.Fatal(err)
	}
	wantPages := int64(0)
	for _, k := range keep {
		got := loFetch(t, ctx, root, k.oid, 1<<20)
		if len(got) != k.n || sha256.Sum256(got) != k.sum {
			t.Fatalf("object %d: %d bytes read back, %d written, or the bytes differ", k.oid, len(got), k.n)
		}
		wantPages += int64((k.n + page - 1) / page)
	}
	if objects, pages, pending := loCounts(t, ctx, root); objects != int64(len(keep)) || pages != wantPages || pending != 0 {
		t.Fatalf("%d objects, %d pages, %d pending; want %d, %d, 0", objects, pages, pending, len(keep), wantPages)
	}

	// Two sessions each add their own byte to every byte of one object,
	// in one transaction each, at once: whatever the order, both
	// additions are in the result, or one of them failed and said so.
	base := make([]byte, page+100)
	target := loStore(t, ctx, root, base, 1<<20)
	add := func(node int, delta byte) error {
		conn, err := pgx.Connect(ctx, pgURL(tc, node))
		if err != nil {
			return err
		}
		defer func() { _ = conn.Close(ctx) }()
		tx, err := conn.Begin(ctx)
		if err != nil {
			return err
		}
		defer func() { _ = tx.Rollback(ctx) }()
		obj, err := loOf(tx).Open(ctx, target, pgx.LargeObjectModeWrite)
		if err != nil {
			return err
		}
		cur := make([]byte, len(base))
		if err := loReadFull(obj, cur); err != nil {
			return err
		}
		for i := range cur {
			cur[i] += delta
		}
		if _, err := obj.Seek(0, io.SeekStart); err != nil {
			return err
		}
		if _, err := obj.Write(cur); err != nil {
			return err
		}
		return tx.Commit(ctx)
	}
	results := make([]error, 2)
	var wg2 sync.WaitGroup
	for i, delta := range []byte{1, 16} {
		wg2.Add(1)
		go func(i int, delta byte) {
			defer wg2.Done()
			results[i] = add(i+1, delta)
		}(i, delta)
	}
	wg2.Wait()
	want := byte(0)
	for i, delta := range []byte{1, 16} {
		if results[i] == nil {
			want += delta
		} else if sqlState(results[i]) != "40001" {
			t.Fatalf("concurrent writer %d: %v", i, results[i])
		}
	}
	if want == 0 {
		t.Fatal("both concurrent writers failed")
	}
	for i, b := range loFetch(t, ctx, root, target, 1<<20) {
		if b != want {
			t.Fatalf("byte %d is %d after concurrent writers; want %d (a lost update)", i, b, want)
		}
	}
}

// TestLargeObjectReadErrorReported: an error on loread reaches the
// client as that read's error, and only the block's later statements
// meet 25P02. TestLargeObjectConcurrent once failed with 25P02 as a
// writer's first error: the conflict that aborted its transaction
// surfaced as a 40001 on loread, which the server sent, but pgx's
// LargeObject.Read reported a full read alongside it and io.ReadFull
// dropped the error. Here the loread fails deterministically (a panic
// injected before it executes: XX000).
func TestLargeObjectReadErrorReported(t *testing.T) {
	var armed atomic.Bool
	// Set before the cluster starts and cleared after it stops, so no
	// session reads the variable while it changes.
	sql.TestingPanicBeforeExec = func(stmt parser.Statement) {
		if sel, ok := stmt.(*parser.Select); ok && len(sel.Exprs) == 1 && sel.Exprs[0].Expr.Func == "loread" && armed.CompareAndSwap(true, false) {
			panic("injected: loread fails")
		}
	}
	t.Cleanup(func() { sql.TestingPanicBeforeExec = nil })
	tc := startLOCluster(t)
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()
	conn := pgConn(t, ctx, tc, 0)
	data := randomBytes(t, sql.LargeObjectPageSize+100)
	oid := loStore(t, ctx, conn, data, 1<<20)

	// read runs one transaction block: open, a failing read through
	// readFull, then a seek. It returns the read's error and the seek's.
	read := func(readFull func(*pgx.LargeObject, []byte) error) (rerr, serr error) {
		tx, err := conn.Begin(ctx)
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = tx.Rollback(ctx) }()
		obj, err := loOf(tx).Open(ctx, oid, pgx.LargeObjectModeRead)
		if err != nil {
			t.Fatal(err)
		}
		armed.Store(true)
		rerr = readFull(obj, make([]byte, len(data)))
		if armed.Load() {
			t.Fatal("the injected loread failure did not fire")
		}
		_, serr = obj.Seek(0, io.SeekStart)
		return rerr, serr
	}

	rerr, serr := read(loReadFull)
	if pgErrCode(rerr) != sql.CodeInternal || !strings.Contains(rerr.Error(), "injected") {
		t.Fatalf("the failing loread returned %v; want its XX000", rerr)
	}
	if pgErrCode(serr) != sql.CodeInFailedTransaction {
		t.Fatalf("the statement after the failed loread: %v; want 25P02", serr)
	}

	// The trap the helper avoids, as the CI failure met it (as long as
	// pgx's Read still reports a full count with the error).
	rerr, serr = read(func(obj *pgx.LargeObject, p []byte) error {
		_, err := io.ReadFull(obj, p)
		return err
	})
	if rerr == nil && pgErrCode(serr) != sql.CodeInFailedTransaction {
		t.Fatalf("io.ReadFull lost the loread's error and the next statement got %v; want 25P02", serr)
	}
	if rerr != nil && pgErrCode(rerr) != sql.CodeInternal {
		t.Fatalf("the failing loread through io.ReadFull returned %v; want its XX000", rerr)
	}
	if rerr == nil {
		t.Logf("io.ReadFull over pgx's LargeObject dropped the loread's error; the next statement got %v", serr)
	}
}
