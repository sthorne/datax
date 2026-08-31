package testcluster

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgproto3"
)

// rawFrontend drives the wire protocol directly — the shape JDBC-style
// drivers with fetch sizes produce, which pgx never does.
type rawFrontend struct {
	t  *testing.T
	fe *pgproto3.Frontend
	nc net.Conn
}

func dialRaw(t *testing.T, tc *TestCluster) *rawFrontend {
	t.Helper()
	nc, err := net.Dial("tcp", tc.Nodes[0].SQLAddr())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = nc.Close() })
	fe := pgproto3.NewFrontend(nc, nc)
	fe.Send(&pgproto3.StartupMessage{ProtocolVersion: pgproto3.ProtocolVersionNumber,
		Parameters: map[string]string{"user": "root", "database": "datax"}})
	if err := fe.Flush(); err != nil {
		t.Fatal(err)
	}
	r := &rawFrontend{t: t, fe: fe, nc: nc}
	// Drain startup: auth ok, parameter statuses, backend key data, ready.
	for {
		if _, ok := r.recv().(*pgproto3.ReadyForQuery); ok {
			return r
		}
	}
}

func (r *rawFrontend) recv() pgproto3.BackendMessage {
	r.t.Helper()
	_ = r.nc.SetReadDeadline(time.Now().Add(15 * time.Second))
	msg, err := r.fe.Receive()
	if err != nil {
		r.t.Fatalf("receive: %v", err)
	}
	return msg
}

func (r *rawFrontend) send(msgs ...pgproto3.FrontendMessage) {
	r.t.Helper()
	for _, m := range msgs {
		r.fe.Send(m)
	}
	if err := r.fe.Flush(); err != nil {
		r.t.Fatal(err)
	}
}

// expect asserts the exact type of the next backend message, skipping
// nothing — the flow tests are byte-order-exact.
func expect[T pgproto3.BackendMessage](r *rawFrontend) T {
	r.t.Helper()
	msg := r.recv()
	m, ok := msg.(T)
	if !ok {
		r.t.Fatalf("expected %T, got %T (%+v)", *new(T), msg, msg)
	}
	return m
}

// TestPortalSuspension: a row-limited Execute returns exactly that many
// rows and PortalSuspended; re-Execute resumes where it left off;
// exhaustion completes; a completed portal stays completed; Sync outside
// a transaction destroys portals.
func TestPortalSuspension(t *testing.T) {
	tc := Start(t, 3)
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	// Seed via pgx.
	conn, err := pgx.Connect(ctx, pgURL(tc, 0))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := conn.Exec(ctx, `CREATE TABLE nums (id INT8 PRIMARY KEY)`); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 10; i++ {
		if _, err := conn.Exec(ctx, `INSERT INTO nums VALUES ($1)`, int64(i)); err != nil {
			t.Fatal(err)
		}
	}
	_ = conn.Close(ctx)

	r := dialRaw(t, tc)

	// Parse/Bind once, fetch 4+4+4: 4 rows, suspend; 4 rows, suspend;
	// 2 rows, complete.
	r.send(
		&pgproto3.Parse{Name: "st", Query: "SELECT id FROM nums ORDER BY id"},
		&pgproto3.Bind{DestinationPortal: "pt", PreparedStatement: "st"},
		&pgproto3.Execute{Portal: "pt", MaxRows: 4},
		&pgproto3.Sync{},
	)
	expect[*pgproto3.ParseComplete](r)
	expect[*pgproto3.BindComplete](r)
	for i := 0; i < 4; i++ {
		expect[*pgproto3.DataRow](r)
	}
	expect[*pgproto3.PortalSuspended](r)
	expect[*pgproto3.ReadyForQuery](r)

	// Outside a transaction, that Sync destroyed the portal.
	r.send(&pgproto3.Execute{Portal: "pt", MaxRows: 4}, &pgproto3.Sync{})
	expect[*pgproto3.ErrorResponse](r)
	expect[*pgproto3.ReadyForQuery](r)

	// Inside an explicit transaction the portal survives Sync between
	// Executes: JDBC's fetch loop shape.
	r.send(&pgproto3.Query{String: "BEGIN"})
	expect[*pgproto3.CommandComplete](r)
	if rq := expect[*pgproto3.ReadyForQuery](r); rq.TxStatus != 'T' {
		t.Fatalf("tx status %c, want T", rq.TxStatus)
	}
	r.send(
		&pgproto3.Bind{DestinationPortal: "pt", PreparedStatement: "st"},
		&pgproto3.Execute{Portal: "pt", MaxRows: 4},
		&pgproto3.Sync{},
	)
	expect[*pgproto3.BindComplete](r)
	var got []string
	for i := 0; i < 4; i++ {
		got = append(got, string(expect[*pgproto3.DataRow](r).Values[0]))
	}
	expect[*pgproto3.PortalSuspended](r)
	expect[*pgproto3.ReadyForQuery](r)

	r.send(&pgproto3.Execute{Portal: "pt", MaxRows: 4}, &pgproto3.Sync{})
	for i := 0; i < 4; i++ {
		got = append(got, string(expect[*pgproto3.DataRow](r).Values[0]))
	}
	expect[*pgproto3.PortalSuspended](r)
	expect[*pgproto3.ReadyForQuery](r)

	r.send(&pgproto3.Execute{Portal: "pt", MaxRows: 4}, &pgproto3.Sync{})
	for i := 0; i < 2; i++ {
		got = append(got, string(expect[*pgproto3.DataRow](r).Values[0]))
	}
	if cc := expect[*pgproto3.CommandComplete](r); string(cc.CommandTag) != "SELECT 10" {
		t.Fatalf("tag %q", cc.CommandTag)
	}
	expect[*pgproto3.ReadyForQuery](r)

	want := []string{"0", "1", "2", "3", "4", "5", "6", "7", "8", "9"}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("resumed rows = %v, want %v", got, want)
		}
	}

	// A completed portal stays completed: no rows, just CommandComplete.
	r.send(&pgproto3.Execute{Portal: "pt", MaxRows: 4}, &pgproto3.Sync{})
	expect[*pgproto3.CommandComplete](r)
	expect[*pgproto3.ReadyForQuery](r)

	// Explicit Close destroys it even inside the transaction.
	r.send(
		&pgproto3.Bind{DestinationPortal: "pt2", PreparedStatement: "st"},
		&pgproto3.Execute{Portal: "pt2", MaxRows: 1},
		&pgproto3.Close{ObjectType: 'P', Name: "pt2"},
		&pgproto3.Execute{Portal: "pt2", MaxRows: 1},
		&pgproto3.Sync{},
	)
	expect[*pgproto3.BindComplete](r)
	expect[*pgproto3.DataRow](r)
	expect[*pgproto3.PortalSuspended](r)
	expect[*pgproto3.CloseComplete](r)
	expect[*pgproto3.ErrorResponse](r)
	expect[*pgproto3.ReadyForQuery](r)

	// COMMIT via simple query ends the transaction and reaps portals.
	r.send(
		&pgproto3.Bind{DestinationPortal: "pt3", PreparedStatement: "st"},
		&pgproto3.Execute{Portal: "pt3", MaxRows: 1},
		&pgproto3.Sync{},
	)
	expect[*pgproto3.BindComplete](r)
	expect[*pgproto3.DataRow](r)
	expect[*pgproto3.PortalSuspended](r)
	expect[*pgproto3.ReadyForQuery](r)
	r.send(&pgproto3.Query{String: "COMMIT"})
	expect[*pgproto3.CommandComplete](r)
	expect[*pgproto3.ReadyForQuery](r)
	r.send(&pgproto3.Execute{Portal: "pt3", MaxRows: 1}, &pgproto3.Sync{})
	expect[*pgproto3.ErrorResponse](r)
	expect[*pgproto3.ReadyForQuery](r)

	// MaxRows = 0 still means "all rows" (pgx's default path, unchanged).
	r.send(
		&pgproto3.Bind{DestinationPortal: "", PreparedStatement: "st"},
		&pgproto3.Execute{Portal: ""},
		&pgproto3.Sync{},
	)
	expect[*pgproto3.BindComplete](r)
	for i := 0; i < 10; i++ {
		expect[*pgproto3.DataRow](r)
	}
	expect[*pgproto3.CommandComplete](r)
	expect[*pgproto3.ReadyForQuery](r)
}
