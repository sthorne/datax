package testcluster

import (
	"bytes"
	"context"
	"encoding/binary"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgproto3"

	"github.com/sthorne/datax/pkg/sql"
	"github.com/sthorne/datax/pkg/sql/catalog"
)

// libpqLOQuery is, to the letter, the query libpq runs once per
// connection to learn the large-object functions' OIDs before it calls
// them through the fast-path interface (fe-lobj.c, lo_initialize).
const libpqLOQuery = "select proname, oid from pg_catalog.pg_proc " +
	"where proname in (" +
	"'lo_open', 'lo_close', 'lo_creat', 'lo_create', 'lo_unlink', " +
	"'lo_lseek', 'lo_lseek64', 'lo_tell', 'lo_tell64', " +
	"'lo_truncate', 'lo_truncate64', 'loread', 'lowrite') " +
	"and pronamespace = (select oid from pg_catalog.pg_namespace " +
	"where nspname = 'pg_catalog')"

// TestLargeObjectFastPath: the large-object functions answer the
// fast-path function call, which is how libpq (and so psql, psycopg) and
// the JDBC driver reach them: OIDs from pg_proc by libpq's own query,
// binary arguments of the widths libpq sends, results of the widths it
// reads.
func TestLargeObjectFastPath(t *testing.T) {
	tc := startLOCluster(t)
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()
	conn := pgConn(t, ctx, tc, 0)

	oids := map[string]uint32{}
	rows, err := conn.Query(ctx, libpqLOQuery, pgx.QueryExecModeSimpleProtocol)
	if err != nil {
		t.Fatalf("libpq's function lookup: %v", err)
	}
	for rows.Next() {
		var name string
		var oid uint32
		if err := rows.Scan(&name, &oid); err != nil {
			t.Fatal(err)
		}
		oids[name] = oid
	}
	if err := rows.Err(); err != nil || len(oids) != 13 {
		t.Fatalf("libpq's function lookup found %d of 13 functions, %v: %v", len(oids), err, oids)
	}

	i4 := func(v int32) []byte { return binary.BigEndian.AppendUint32(nil, uint32(v)) }
	i8 := func(v int64) []byte { return binary.BigEndian.AppendUint64(nil, uint64(v)) }
	// call makes one fast-path call and returns its result, or the
	// error's SQLSTATE.
	call := func(fn string, args ...[]byte) ([]byte, string) {
		t.Helper()
		fe := conn.PgConn().Frontend()
		fe.Send(&pgproto3.FunctionCall{Function: oids[fn], ArgFormatCodes: []uint16{1}, Arguments: args, ResultFormatCode: 1})
		if err := fe.Flush(); err != nil {
			t.Fatal(err)
		}
		var result []byte
		code := ""
		for {
			msg, err := fe.Receive()
			if err != nil {
				t.Fatalf("%s: %v", fn, err)
			}
			switch m := msg.(type) {
			case *pgproto3.FunctionCallResponse:
				result = append([]byte{}, m.Result...)
			case *pgproto3.ErrorResponse:
				code = m.Code
			case *pgproto3.ReadyForQuery:
				return result, code
			}
		}
	}
	must := func(fn string, args ...[]byte) []byte {
		t.Helper()
		res, code := call(fn, args...)
		if code != "" {
			t.Fatalf("%s: SQLSTATE %s", fn, code)
		}
		return res
	}

	const page = sql.LargeObjectPageSize
	data := randomBytes(t, 2*page+4321)
	if _, err := conn.Exec(ctx, `BEGIN`); err != nil {
		t.Fatal(err)
	}
	oidRaw := must("lo_creat", i4(0x20000|0x40000))
	if len(oidRaw) != 4 {
		t.Fatalf("lo_creat returned %d bytes", len(oidRaw))
	}
	fd := must("lo_open", oidRaw, i4(0x20000))
	if len(fd) != 4 {
		t.Fatalf("lo_open returned %d bytes", len(fd))
	}
	// libpq writes in pieces of 8 kB.
	for off := 0; off < len(data); off += 8192 {
		end := off + 8192
		if end > len(data) {
			end = len(data)
		}
		if n := must("lowrite", fd, data[off:end]); len(n) != 4 || int(binary.BigEndian.Uint32(n)) != end-off {
			t.Fatalf("lowrite at %d returned %v", off, n)
		}
	}
	if pos := must("lo_tell64", fd); len(pos) != 8 || binary.BigEndian.Uint64(pos) != uint64(len(data)) {
		t.Fatalf("lo_tell64: %v", pos)
	}
	must("lo_close", fd)
	if _, err := conn.Exec(ctx, `COMMIT`); err != nil {
		t.Fatal(err)
	}

	oid := binary.BigEndian.Uint32(oidRaw)
	if got := loFetch(t, ctx, conn, oid, 1<<20); !bytes.Equal(got, data) {
		t.Fatal("an object written through fast-path calls reads back differently")
	}
	if _, err := conn.Exec(ctx, `BEGIN`); err != nil {
		t.Fatal(err)
	}
	fd = must("lo_open", oidRaw, i4(0x40000))
	if pos := must("lo_lseek64", fd, i8(page-10), i4(0)); binary.BigEndian.Uint64(pos) != page-10 {
		t.Fatalf("lo_lseek64: %v", pos)
	}
	if got := must("loread", fd, i4(100)); !bytes.Equal(got, data[page-10:page+90]) {
		t.Fatalf("loread returned %d bytes that differ", len(got))
	}
	if pos := must("lo_lseek", fd, i4(-5), i4(2)); len(pos) != 4 || int(binary.BigEndian.Uint32(pos)) != len(data)-5 {
		t.Fatalf("lo_lseek from the end: %v", pos)
	}
	if got := must("loread", fd, i4(100)); !bytes.Equal(got, data[len(data)-5:]) {
		t.Fatalf("loread at the end returned %d bytes", len(got))
	}
	if got := must("loread", fd, i4(100)); got == nil || len(got) != 0 {
		t.Fatalf("loread past the end returned %v (want an empty value, not NULL)", got)
	}
	// An error ends the call with ReadyForQuery like any other, and
	// fails the transaction.
	if _, code := call("loread", i4(77), i4(1)); code != "42704" {
		t.Fatalf("loread of a bad descriptor: SQLSTATE %q", code)
	}
	if _, err := conn.Exec(ctx, `ROLLBACK`); err != nil {
		t.Fatal(err)
	}
	if res := must("lo_unlink", oidRaw); binary.BigEndian.Uint32(res) != 1 {
		t.Fatalf("lo_unlink: %v", res)
	}
	// Only the large-object functions are callable this way.
	var lengthOID uint32
	if err := conn.QueryRow(ctx, `SELECT oid FROM pg_catalog.pg_proc WHERE proname = 'length'`).Scan(&lengthOID); err != nil {
		t.Fatal(err)
	}
	oids["length"] = lengthOID
	if _, code := call("length", []byte("abc")); code != "0A000" {
		t.Fatalf("a fast-path call of length(): SQLSTATE %q", code)
	}
	// The connection is in step afterwards.
	var one int
	if err := conn.QueryRow(ctx, `SELECT 1`).Scan(&one); err != nil || one != 1 {
		t.Fatalf("after the fast-path calls: %d, %v", one, err)
	}
}

// TestLargeObjectPsql: psql's \lo_import, \lo_export, \lo_list and
// \lo_unlink, which are libpq's large-object API end to end. Skipped
// where psql is not installed.
func TestLargeObjectPsql(t *testing.T) {
	psql, err := exec.LookPath("psql")
	if err != nil {
		t.Skip("psql is not installed")
	}
	tc := startLOCluster(t)
	ctx, cancel := context.WithTimeout(context.Background(), 180*time.Second)
	defer cancel()
	// A node creates the large-object tables when it starts; this
	// cluster has that loop off, so they are made here, as they would
	// have been, before a client arrives. (psql looks the functions up in
	// pg_proc inside its transaction, and a transaction that has read
	// the catalog cannot commit across the tables' creation.)
	if _, err := sql.EnsureLargeObjectTables(ctx, tc.Nodes[0].DB(), catalog.NewAccessor()); err != nil {
		t.Fatal(err)
	}
	dir := t.TempDir()
	in, out := filepath.Join(dir, "in.bin"), filepath.Join(dir, "out.bin")
	data := randomBytes(t, 3<<20+17)
	if err := os.WriteFile(in, data, 0o600); err != nil {
		t.Fatal(err)
	}
	run := func(node int, cmd string) string {
		t.Helper()
		c := exec.CommandContext(ctx, psql, pgURL(tc, node), "-X", "-v", "ON_ERROR_STOP=1", "-c", cmd)
		b, err := c.CombinedOutput()
		if err != nil {
			t.Fatalf("psql -c %q: %v\n%s", cmd, err, b)
		}
		return string(b)
	}
	m := regexp.MustCompile(`lo_import (\d+)`).FindStringSubmatch(run(0, `\lo_import `+in))
	if m == nil {
		t.Fatal("\\lo_import did not report an object id")
	}
	oid := m[1]
	run(1, `\lo_export `+oid+` `+out)
	got, err := os.ReadFile(out)
	if err != nil || !bytes.Equal(got, data) {
		t.Fatalf("\\lo_export wrote %d bytes that differ from the %d imported, %v", len(got), len(data), err)
	}
	if list := run(2, `\lo_list`); !strings.Contains(list, oid) {
		t.Fatalf("\\lo_list does not show object %s:\n%s", oid, list)
	}
	run(2, `\lo_unlink `+oid)
	conn := pgConn(t, ctx, tc, 0)
	if _, err := conn.Exec(ctx, `SELECT lo_open(`+oid+`, 262144)`); sqlState(err) != "42704" {
		t.Fatalf("after \\lo_unlink: %v", err)
	}
}
