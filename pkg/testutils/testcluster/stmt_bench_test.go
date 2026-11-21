package testcluster

import (
	"bufio"
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/sthorne/datax/pkg/server"
)

// The prepared point-statement benchmarks measure the server's fixed cost
// per statement: a prepared point SELECT and a point UPDATE, each one
// Bind/Execute/Sync round trip over a real pgwire connection to an
// in-process node, as the `datax bench kv` workload sends them. The client
// side is a hand-rolled frame loop over the hijacked connection that
// allocates nothing per op, so allocs/op is the server's (plus whatever
// background work the node does meanwhile, which is small at one node).
//
//	go test ./pkg/testutils/testcluster -run - -bench 'BenchmarkPrepared' -benchtime 5000x
//
// Watch allocs/op and B/op first: ns/op is noisy on a shared machine.

const stmtBenchRows = 1000

// stmtBenchConn is a prepared statement on a hijacked connection.
type stmtBenchConn struct {
	conn io.ReadWriteCloser
	r    *bufio.Reader
	out  []byte
	in   []byte
}

func startStmtBench(b *testing.B, query string) *stmtBenchConn {
	b.Helper()
	tc := Start(b, 1)
	ctx := context.Background()
	setup, err := pgx.Connect(ctx, pgURL(tc, 0))
	if err != nil {
		b.Fatal(err)
	}
	if _, err := setup.Exec(ctx, "CREATE TABLE kv (k INT8 PRIMARY KEY, v TEXT)"); err != nil {
		b.Fatal(err)
	}
	var sb strings.Builder
	sb.WriteString("INSERT INTO kv VALUES ")
	for i := 0; i < stmtBenchRows; i++ {
		if i > 0 {
			sb.WriteString(", ")
		}
		fmt.Fprintf(&sb, "(%d, 'v%d')", i, i)
	}
	if _, err := setup.Exec(ctx, sb.String()); err != nil {
		b.Fatal(err)
	}
	_ = setup.Close(ctx)

	pc, err := pgconn.Connect(ctx, pgURL(tc, 0))
	if err != nil {
		b.Fatal(err)
	}
	if _, err := pc.Prepare(ctx, "s", query, nil); err != nil {
		b.Fatal(err)
	}
	hj, err := pc.Hijack()
	if err != nil {
		b.Fatal(err)
	}
	c := &stmtBenchConn{conn: hj.Conn, r: bufio.NewReaderSize(hj.Conn, 16<<10)}
	b.Cleanup(func() { _ = c.conn.Close() })
	return c
}

// run sends Bind(s, params)/Execute/Sync and reads to ReadyForQuery. The
// params are ($1 = text, $2 = k) when withText, else ($1 = k); k goes as a
// binary INT8 and text as text. Results are binary.
func (c *stmtBenchConn) run(b *testing.B, withText bool, text string, k int64) {
	out := c.out[:0]
	// Bind.
	out = append(out, 'B', 0, 0, 0, 0)
	out = append(out, 0)      // unnamed portal
	out = append(out, 's', 0) // statement
	if withText {
		out = append(out, 0, 2, 0, 0, 0, 1) // formats: text, binary
		out = append(out, 0, 2)
		out = binary.BigEndian.AppendUint32(out, uint32(len(text)))
		out = append(out, text...)
	} else {
		out = append(out, 0, 1, 0, 1) // formats: binary
		out = append(out, 0, 1)
	}
	out = binary.BigEndian.AppendUint32(out, 8)
	out = binary.BigEndian.AppendUint64(out, uint64(k))
	out = binary.BigEndian.AppendUint16(out, 1)
	out = binary.BigEndian.AppendUint16(out, 1) // binary results
	binary.BigEndian.PutUint32(out[1:5], uint32(len(out)-1))
	// Execute, Sync.
	out = append(out, 'E', 0, 0, 0, 9, 0, 0, 0, 0, 0)
	out = append(out, 'S', 0, 0, 0, 4)
	c.out = out
	if _, err := c.conn.Write(out); err != nil {
		b.Fatal(err)
	}
	var hdr [5]byte
	for {
		if _, err := io.ReadFull(c.r, hdr[:]); err != nil {
			b.Fatal(err)
		}
		n := int(binary.BigEndian.Uint32(hdr[1:])) - 4
		if cap(c.in) < n {
			c.in = make([]byte, n)
		}
		body := c.in[:n]
		if _, err := io.ReadFull(c.r, body); err != nil {
			b.Fatal(err)
		}
		switch hdr[0] {
		case 'E':
			b.Fatalf("server error: %q", body)
		case 'Z':
			return
		}
	}
}

// BenchmarkPreparedPointSelect is the read of `datax bench kv`:
// SELECT v FROM kv WHERE k = $1, prepared once.
func BenchmarkPreparedPointSelect(b *testing.B) {
	c := startStmtBench(b, "SELECT v FROM kv WHERE k = $1")
	for i := 0; i < 200; i++ {
		c.run(b, false, "", int64(i%stmtBenchRows))
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		c.run(b, false, "", int64(i%stmtBenchRows))
	}
}

// BenchmarkPreparedPointUpdate is the write of `datax bench kv`:
// UPDATE kv SET v = $1 WHERE k = $2, prepared once, as an implicit
// transaction.
func BenchmarkPreparedPointUpdate(b *testing.B) {
	c := startStmtBench(b, "UPDATE kv SET v = $1 WHERE k = $2")
	for i := 0; i < 200; i++ {
		c.run(b, true, "warm", int64(i%stmtBenchRows))
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		c.run(b, true, "u12345678", int64(i%stmtBenchRows))
	}
}

// TestPreparedStatementOneShape: every execution of a prepared statement
// is charged to one shape, the one its text has in the simple protocol,
// although planning rewrites the parsed statement in place (a qualified
// column loses its qualifier at Describe and at the first execution).
// The shape is taken once, at Parse.
func TestPreparedStatementOneShape(t *testing.T) {
	httpLis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	tc := StartWithOptions(t, 1, func(c *server.Config) { c.HTTPListener = httpLis })
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	conn, err := pgx.Connect(ctx, pgURL(tc, 0))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = conn.Close(ctx) }()
	if _, err := conn.Exec(ctx, `CREATE TABLE shapes1 (id INT8 PRIMARY KEY, amount INT8)`); err != nil {
		t.Fatal(err)
	}
	if _, err := conn.Exec(ctx, `INSERT INTO shapes1 VALUES (1, 10), (2, 20)`); err != nil {
		t.Fatal(err)
	}
	const q = `SELECT shapes1.amount FROM shapes1 WHERE shapes1.id = $1`
	for i := 0; i < 3; i++ { // prepared once (pgx's statement cache), executed three times
		var amount int64
		if err := conn.QueryRow(ctx, q, int64(1+i%2)).Scan(&amount); err != nil {
			t.Fatal(err)
		}
	}
	var amount int64
	if err := conn.QueryRow(ctx, q, pgx.QueryExecModeSimpleProtocol, int64(1)).Scan(&amount); err != nil {
		t.Fatal(err)
	}
	code, _, body := httpGet(t, "http://"+tc.Nodes[0].HTTPAddr()+"/api/statements")
	if code != 200 {
		t.Fatalf("/api/statements: %d %s", code, body)
	}
	var doc server.StatementsStatus
	if err := json.Unmarshal([]byte(body), &doc); err != nil {
		t.Fatal(err)
	}
	var shapes []string
	var count uint64
	for _, s := range doc.Statements {
		if strings.Contains(strings.ToLower(s.Shape), "from shapes1") && s.Kind == "select" {
			shapes = append(shapes, s.Shape)
			count += s.Count
		}
	}
	if len(shapes) != 1 || count != 4 {
		t.Fatalf("want one shape run 4 times, got %d executions under %q", count, shapes)
	}
}
