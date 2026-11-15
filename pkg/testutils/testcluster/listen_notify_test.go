package testcluster

import (
	"context"
	"errors"
	"fmt"
	"net"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgproto3"

	"github.com/sthorne/datax/pkg/base"
	"github.com/sthorne/datax/pkg/cluster"
	"github.com/sthorne/datax/pkg/rpc"
	"github.com/sthorne/datax/pkg/security"
	"github.com/sthorne/datax/pkg/sql/notify"
	"github.com/sthorne/datax/pkg/util/hlc"
)

// notifyConn opens a pgx connection to a node of the cluster.
func notifyConn(t *testing.T, ctx context.Context, tc *TestCluster, node int) *pgx.Conn {
	t.Helper()
	conn, err := pgx.Connect(ctx, pgURL(tc, node))
	if err != nil {
		t.Fatalf("connect to node %d: %v", node, err)
	}
	t.Cleanup(func() { _ = conn.Close(context.Background()) })
	return conn
}

func mustExec(t *testing.T, ctx context.Context, conn *pgx.Conn, q string, args ...any) {
	t.Helper()
	if _, err := conn.Exec(ctx, q, args...); err != nil {
		t.Fatalf("%s: %v", q, err)
	}
}

// waitNotify waits for the connection's next notification without
// sending anything: WaitForNotification only reads the socket.
func waitNotify(t *testing.T, ctx context.Context, conn *pgx.Conn) *pgconn.Notification {
	t.Helper()
	wctx, cancel := context.WithTimeout(ctx, 20*time.Second)
	defer cancel()
	n, err := conn.WaitForNotification(wctx)
	if err != nil {
		t.Fatalf("waiting for a notification: %v", err)
	}
	return n
}

// noNotify asserts nothing arrives on the connection for a short while.
func noNotify(t *testing.T, ctx context.Context, conn *pgx.Conn) {
	t.Helper()
	wctx, cancel := context.WithTimeout(ctx, 300*time.Millisecond)
	defer cancel()
	if n, err := conn.WaitForNotification(wctx); err == nil {
		t.Fatalf("unexpected notification on %q: %q", n.Channel, n.Payload)
	} else if !errors.Is(err, context.DeadlineExceeded) && !pgconn.Timeout(err) {
		t.Fatalf("waiting for no notification: %v", err)
	}
}

func wantNotify(t *testing.T, ctx context.Context, conn *pgx.Conn, channel, payload string) *pgconn.Notification {
	t.Helper()
	n := waitNotify(t, ctx, conn)
	if n.Channel != channel || n.Payload != payload {
		t.Fatalf("got notification %q %q, want %q %q", n.Channel, n.Payload, channel, payload)
	}
	return n
}

func sqlState(err error) string {
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) {
		return pgErr.Code
	}
	return ""
}

// TestListenNotify (issue #297): LISTEN / UNLISTEN / NOTIFY / pg_notify
// through a stock driver, on one node and across the cluster, with
// PostgreSQL's transactional behaviour.
func TestListenNotify(t *testing.T) {
	tc := Start(t, 3)
	ctx, cancel := context.WithTimeout(context.Background(), 240*time.Second)
	defer cancel()

	t.Run("same node", func(t *testing.T) {
		listener, notifier := notifyConn(t, ctx, tc, 0), notifyConn(t, ctx, tc, 0)
		mustExec(t, ctx, listener, `LISTEN jobs`)
		var pid uint32
		if err := notifier.QueryRow(ctx, `SELECT pg_backend_pid()`).Scan(&pid); err != nil {
			t.Fatal(err)
		}
		mustExec(t, ctx, notifier, `NOTIFY jobs, 'hello'`)
		if n := wantNotify(t, ctx, listener, "jobs", "hello"); n.PID != pid {
			t.Fatalf("notification PID %d, want the notifier's %d", n.PID, pid)
		}
		// No payload is the empty payload; a quoted channel keeps its
		// case and an unquoted one folds, as identifiers do.
		mustExec(t, ctx, listener, `LISTEN "Mixed"`)
		mustExec(t, ctx, notifier, `NOTIFY JOBS`)
		wantNotify(t, ctx, listener, "jobs", "")
		mustExec(t, ctx, notifier, `NOTIFY "Mixed", 'm'`)
		wantNotify(t, ctx, listener, "Mixed", "m")
		mustExec(t, ctx, notifier, `NOTIFY mixed, 'folded'`)
		noNotify(t, ctx, listener)
	})

	t.Run("across nodes", func(t *testing.T) {
		l1, l2 := notifyConn(t, ctx, tc, 1), notifyConn(t, ctx, tc, 2)
		notifier := notifyConn(t, ctx, tc, 0)
		mustExec(t, ctx, l1, `LISTEN xnode`)
		mustExec(t, ctx, l2, `LISTEN xnode`)
		var pid uint32
		if err := notifier.QueryRow(ctx, `SELECT pg_backend_pid()`).Scan(&pid); err != nil {
			t.Fatal(err)
		}
		mustExec(t, ctx, notifier, `NOTIFY xnode, 'from node 1'`)
		for _, l := range []*pgx.Conn{l1, l2} {
			if n := wantNotify(t, ctx, l, "xnode", "from node 1"); n.PID != pid {
				t.Fatalf("notification PID %d, want the notifier's %d", n.PID, pid)
			}
			noNotify(t, ctx, l) // once
		}
		// And the other way, from a node that so far only listened.
		mustExec(t, ctx, notifier, `LISTEN back`)
		mustExec(t, ctx, l2, `NOTIFY back, 'from node 3'`)
		wantNotify(t, ctx, notifier, "back", "from node 3")
	})

	t.Run("commit and rollback", func(t *testing.T) {
		listener, notifier := notifyConn(t, ctx, tc, 1), notifyConn(t, ctx, tc, 0)
		mustExec(t, ctx, listener, `LISTEN txn`)
		mustExec(t, ctx, notifier, `CREATE TABLE IF NOT EXISTS notify_t (id INT PRIMARY KEY)`)

		mustExec(t, ctx, notifier, `BEGIN`)
		mustExec(t, ctx, notifier, `INSERT INTO notify_t VALUES (1)`)
		mustExec(t, ctx, notifier, `NOTIFY txn, 'rolled back'`)
		noNotify(t, ctx, listener) // nothing before the commit
		mustExec(t, ctx, notifier, `ROLLBACK`)

		mustExec(t, ctx, notifier, `BEGIN`)
		mustExec(t, ctx, notifier, `INSERT INTO notify_t VALUES (2)`)
		mustExec(t, ctx, notifier, `NOTIFY txn, 'committed'`)
		noNotify(t, ctx, listener)
		mustExec(t, ctx, notifier, `COMMIT`)
		wantNotify(t, ctx, listener, "txn", "committed")

		// A transaction that fails is rolled back by its COMMIT.
		mustExec(t, ctx, notifier, `BEGIN`)
		mustExec(t, ctx, notifier, `NOTIFY txn, 'failed'`)
		if _, err := notifier.Exec(ctx, `INSERT INTO notify_t VALUES (2)`); err == nil {
			t.Fatal("duplicate key accepted")
		}
		mustExec(t, ctx, notifier, `COMMIT`)

		// A savepoint rollback discards what was notified after it.
		mustExec(t, ctx, notifier, `BEGIN`)
		mustExec(t, ctx, notifier, `NOTIFY txn, 'before'`)
		mustExec(t, ctx, notifier, `SAVEPOINT s`)
		mustExec(t, ctx, notifier, `NOTIFY txn, 'undone'`)
		mustExec(t, ctx, notifier, `SELECT pg_notify('txn', 'undone too')`)
		mustExec(t, ctx, notifier, `ROLLBACK TO SAVEPOINT s`)
		mustExec(t, ctx, notifier, `NOTIFY txn, 'after'`)
		mustExec(t, ctx, notifier, `SAVEPOINT r`)
		mustExec(t, ctx, notifier, `NOTIFY txn, 'released'`)
		mustExec(t, ctx, notifier, `RELEASE SAVEPOINT r`)
		mustExec(t, ctx, notifier, `COMMIT`)
		wantNotify(t, ctx, listener, "txn", "before")
		wantNotify(t, ctx, listener, "txn", "after")
		wantNotify(t, ctx, listener, "txn", "released")
		noNotify(t, ctx, listener)

		// LISTEN is transactional too.
		mustExec(t, ctx, notifier, `BEGIN`)
		mustExec(t, ctx, notifier, `LISTEN undone`)
		mustExec(t, ctx, notifier, `ROLLBACK`)
		mustExec(t, ctx, listener, `NOTIFY undone`)
		noNotify(t, ctx, notifier)
	})

	t.Run("duplicates collapse", func(t *testing.T) {
		listener, notifier := notifyConn(t, ctx, tc, 2), notifyConn(t, ctx, tc, 0)
		mustExec(t, ctx, listener, `LISTEN dup`)
		mustExec(t, ctx, notifier, `BEGIN`)
		for i := 0; i < 3; i++ {
			mustExec(t, ctx, notifier, `NOTIFY dup, 'x'`)
			mustExec(t, ctx, notifier, `SELECT pg_notify('dup', 'x')`)
		}
		mustExec(t, ctx, notifier, `NOTIFY dup, 'y'`)
		mustExec(t, ctx, notifier, `NOTIFY dup, 'x'`)
		mustExec(t, ctx, notifier, `COMMIT`)
		wantNotify(t, ctx, listener, "dup", "x")
		wantNotify(t, ctx, listener, "dup", "y")
		noNotify(t, ctx, listener)
		// Separate transactions are separate notifications.
		mustExec(t, ctx, notifier, `NOTIFY dup, 'x'`)
		mustExec(t, ctx, notifier, `NOTIFY dup, 'x'`)
		wantNotify(t, ctx, listener, "dup", "x")
		wantNotify(t, ctx, listener, "dup", "x")
	})

	t.Run("unlisten", func(t *testing.T) {
		listener, notifier := notifyConn(t, ctx, tc, 1), notifyConn(t, ctx, tc, 0)
		mustExec(t, ctx, listener, `LISTEN a`)
		mustExec(t, ctx, listener, `LISTEN b`)
		mustExec(t, ctx, listener, `LISTEN c`)
		mustExec(t, ctx, listener, `UNLISTEN a`)
		mustExec(t, ctx, listener, `UNLISTEN never_listened`)
		mustExec(t, ctx, notifier, `NOTIFY a`)
		mustExec(t, ctx, notifier, `NOTIFY b, 'still'`)
		wantNotify(t, ctx, listener, "b", "still")
		mustExec(t, ctx, listener, `UNLISTEN *`)
		mustExec(t, ctx, notifier, `NOTIFY b`)
		mustExec(t, ctx, notifier, `NOTIFY c`)
		noNotify(t, ctx, listener)
	})

	t.Run("own notifications", func(t *testing.T) {
		conn := notifyConn(t, ctx, tc, 0)
		mustExec(t, ctx, conn, `LISTEN self`)
		mustExec(t, ctx, conn, `NOTIFY self, 'me'`)
		wantNotify(t, ctx, conn, "self", "me")
		// Listening and notifying in one transaction hears itself too.
		mustExec(t, ctx, conn, `BEGIN`)
		mustExec(t, ctx, conn, `LISTEN self2`)
		mustExec(t, ctx, conn, `NOTIFY self2, 'both'`)
		mustExec(t, ctx, conn, `COMMIT`)
		wantNotify(t, ctx, conn, "self2", "both")
	})

	t.Run("limits", func(t *testing.T) {
		listener, notifier := notifyConn(t, ctx, tc, 1), notifyConn(t, ctx, tc, 0)
		mustExec(t, ctx, listener, `LISTEN big`)
		tooLong := strings.Repeat("x", 8000)
		if _, err := notifier.Exec(ctx, `NOTIFY big, '`+tooLong+`'`); sqlState(err) != "22023" {
			t.Fatalf("NOTIFY with an 8000-byte payload: %v, want SQLSTATE 22023", err)
		}
		if _, err := notifier.Exec(ctx, `SELECT pg_notify($1, $2)`, "big", tooLong); sqlState(err) != "22023" {
			t.Fatalf("pg_notify with an 8000-byte payload: %v, want SQLSTATE 22023", err)
		}
		if _, err := notifier.Exec(ctx, `SELECT pg_notify($1, 'x')`, strings.Repeat("c", 64)); sqlState(err) != "22023" {
			t.Fatalf("pg_notify with a 64-byte channel: %v, want SQLSTATE 22023", err)
		}
		if _, err := notifier.Exec(ctx, `SELECT pg_notify('', 'x')`); sqlState(err) != "22023" {
			t.Fatalf("pg_notify with an empty channel: %v, want SQLSTATE 22023", err)
		}
		largest := strings.Repeat("y", 7999)
		mustExec(t, ctx, notifier, `SELECT pg_notify($1, $2)`, "big", largest)
		wantNotify(t, ctx, listener, "big", largest)
		noNotify(t, ctx, listener)
	})

	t.Run("pg_notify", func(t *testing.T) {
		listener, notifier := notifyConn(t, ctx, tc, 2), notifyConn(t, ctx, tc, 0)
		mustExec(t, ctx, listener, `LISTEN fn`)
		// The extended protocol plans the statement before it runs it;
		// only running it notifies.
		mustExec(t, ctx, notifier, `SELECT pg_notify($1, $2)`, "fn", "bound")
		wantNotify(t, ctx, listener, "fn", "bound")
		mustExec(t, ctx, notifier, `SELECT pg_notify('fn', 'p' || '1')`)
		wantNotify(t, ctx, listener, "fn", "p1")
		mustExec(t, ctx, notifier, `BEGIN`)
		mustExec(t, ctx, notifier, `SELECT pg_notify('fn', 'never')`)
		mustExec(t, ctx, notifier, `ROLLBACK`)
		noNotify(t, ctx, listener)
		// In a statement whose rows are streamed to the client, the
		// notification goes out when the statement's transaction
		// commits, at the end of the stream; and a statement that fails
		// notifies nothing.
		mustExec(t, ctx, notifier, `CREATE TABLE IF NOT EXISTS notify_rows (id INT PRIMARY KEY)`)
		var values []string
		for i := 1; i <= 500; i++ {
			values = append(values, fmt.Sprintf("(%d)", i))
		}
		mustExec(t, ctx, notifier, `INSERT INTO notify_rows VALUES `+strings.Join(values, ", "))
		rows, err := notifier.Query(ctx, `SELECT id, pg_notify('fn', 'streamed') FROM notify_rows`)
		if err != nil {
			t.Fatal(err)
		}
		count := 0
		for rows.Next() {
			count++
		}
		if err := rows.Err(); err != nil || count != 500 {
			t.Fatalf("streamed select: %d rows, %v", count, err)
		}
		wantNotify(t, ctx, listener, "fn", "streamed")
		if _, err := notifier.Exec(ctx, `SELECT pg_notify('fn', 'failed'), 1/0`); err == nil {
			t.Fatal("division by zero accepted")
		}
		noNotify(t, ctx, listener)
	})

	t.Run("listener disconnects", func(t *testing.T) {
		gone, stays := notifyConn(t, ctx, tc, 1), notifyConn(t, ctx, tc, 1)
		notifier := notifyConn(t, ctx, tc, 0)
		mustExec(t, ctx, gone, `LISTEN leave`)
		mustExec(t, ctx, stays, `LISTEN leave`)
		if err := gone.Close(ctx); err != nil {
			t.Fatal(err)
		}
		for i := 0; i < 20; i++ {
			mustExec(t, ctx, notifier, `NOTIFY leave, '`+fmt.Sprint(i)+`'`)
		}
		for i := 0; i < 20; i++ {
			wantNotify(t, ctx, stays, "leave", fmt.Sprint(i))
		}
		// A listener whose socket simply dies is forgotten the same way.
		raw, err := pgx.Connect(ctx, pgURL(tc, 1))
		if err != nil {
			t.Fatal(err)
		}
		mustExec(t, ctx, raw, `LISTEN leave`)
		_ = raw.PgConn().Conn().Close()
		mustExec(t, ctx, notifier, `NOTIFY leave, 'after'`)
		wantNotify(t, ctx, stays, "leave", "after")
	})

	t.Run("many in order", func(t *testing.T) {
		remote, local := notifyConn(t, ctx, tc, 1), notifyConn(t, ctx, tc, 0)
		notifier := notifyConn(t, ctx, tc, 0)
		mustExec(t, ctx, remote, `LISTEN seq`)
		mustExec(t, ctx, local, `LISTEN seq`)
		const commits, perTxn = 1500, 300
		go func() {
			for i := 0; i < commits; i++ {
				if _, err := notifier.Exec(ctx, `SELECT pg_notify('seq', $1)`, fmt.Sprint(i)); err != nil {
					t.Errorf("notify %d: %v", i, err)
					return
				}
			}
			// One transaction's notifications keep their order as well.
			tx, err := notifier.Begin(ctx)
			if err != nil {
				t.Error(err)
				return
			}
			for i := 0; i < perTxn; i++ {
				if _, err := tx.Exec(ctx, `SELECT pg_notify('seq', $1)`, fmt.Sprintf("t%d", i)); err != nil {
					t.Errorf("notify t%d: %v", i, err)
					return
				}
			}
			if err := tx.Commit(ctx); err != nil {
				t.Error(err)
			}
		}()
		for _, l := range []*pgx.Conn{remote, local} {
			for i := 0; i < commits; i++ {
				wantNotify(t, ctx, l, "seq", fmt.Sprint(i))
			}
			for i := 0; i < perTxn; i++ {
				wantNotify(t, ctx, l, "seq", fmt.Sprintf("t%d", i))
			}
		}
	})

	// A client that issues LISTEN and then only reads — a pool's
	// dedicated notification connection — is sent each notification as
	// it commits. Spoken at the protocol level, so that it is certain
	// the client sent nothing after the LISTEN.
	t.Run("pushed while idle", func(t *testing.T) {
		for _, node := range []int{0, 2} {
			nc, err := net.Dial("tcp", tc.Nodes[node].SQLAddr())
			if err != nil {
				t.Fatal(err)
			}
			defer nc.Close()
			_ = nc.SetDeadline(time.Now().Add(30 * time.Second))
			fe := pgproto3.NewFrontend(nc, nc)
			fe.Send(&pgproto3.StartupMessage{ProtocolVersion: pgproto3.ProtocolVersionNumber,
				Parameters: map[string]string{"user": "root", "database": "datax"}})
			if err := fe.Flush(); err != nil {
				t.Fatal(err)
			}
			untilReady := func() {
				t.Helper()
				for {
					msg, err := fe.Receive()
					if err != nil {
						t.Fatal(err)
					}
					switch m := msg.(type) {
					case *pgproto3.ErrorResponse:
						t.Fatalf("server error: %s", m.Message)
					case *pgproto3.ReadyForQuery:
						return
					}
				}
			}
			untilReady()
			fe.Send(&pgproto3.Query{String: "LISTEN idle"})
			if err := fe.Flush(); err != nil {
				t.Fatal(err)
			}
			untilReady()

			notifier := notifyConn(t, ctx, tc, 0)
			for i := 0; i < 3; i++ {
				// Spaced out, so each arrives on a connection that has
				// already been pushed to and gone back to waiting.
				time.Sleep(50 * time.Millisecond)
				mustExec(t, ctx, notifier, `NOTIFY idle, '`+fmt.Sprint(i)+`'`)
				msg, err := fe.Receive()
				if err != nil {
					t.Fatalf("node %d: nothing pushed to an idle listener: %v", node+1, err)
				}
				n, ok := msg.(*pgproto3.NotificationResponse)
				if !ok || n.Channel != "idle" || n.Payload != fmt.Sprint(i) {
					t.Fatalf("node %d: got %#v, want notification %d on idle", node+1, msg, i)
				}
			}
			// The connection still serves statements afterwards, and a
			// notification that arrives during one precedes its
			// ReadyForQuery rather than interrupting its answer.
			fe.Send(&pgproto3.Query{String: "SELECT pg_notify('idle', 'mine'), 1"})
			if err := fe.Flush(); err != nil {
				t.Fatal(err)
			}
			var order []string
			for done := false; !done; {
				msg, err := fe.Receive()
				if err != nil {
					t.Fatal(err)
				}
				switch m := msg.(type) {
				case *pgproto3.ErrorResponse:
					t.Fatalf("server error: %s", m.Message)
				case *pgproto3.ReadyForQuery:
					done = true
				default:
					order = append(order, fmt.Sprintf("%T", msg))
				}
			}
			want := "[*pgproto3.RowDescription *pgproto3.DataRow *pgproto3.CommandComplete *pgproto3.NotificationResponse]"
			if fmt.Sprint(order) != want {
				t.Fatalf("node %d: message order %v, want %s", node+1, order, want)
			}
		}
	})

	// A session inside a transaction is not sent notifications until the
	// transaction ends, as in PostgreSQL.
	t.Run("held during a transaction", func(t *testing.T) {
		listener, notifier := notifyConn(t, ctx, tc, 1), notifyConn(t, ctx, tc, 0)
		mustExec(t, ctx, listener, `LISTEN held`)
		mustExec(t, ctx, listener, `BEGIN`)
		mustExec(t, ctx, notifier, `NOTIFY held, 'wait'`)
		noNotify(t, ctx, listener)
		mustExec(t, ctx, listener, `SELECT 1`)
		noNotify(t, ctx, listener)
		mustExec(t, ctx, listener, `COMMIT`)
		wantNotify(t, ctx, listener, "held", "wait")
	})

	// Channels belong to a database.
	t.Run("per database", func(t *testing.T) {
		admin := notifyConn(t, ctx, tc, 0)
		mustExec(t, ctx, admin, `CREATE DATABASE notify_other`)
		other, err := pgx.Connect(ctx, fmt.Sprintf("postgres://root@%s/notify_other?sslmode=disable", tc.Nodes[1].SQLAddr()))
		if err != nil {
			t.Fatal(err)
		}
		defer other.Close(context.Background())
		same := notifyConn(t, ctx, tc, 1)
		mustExec(t, ctx, other, `LISTEN shared`)
		mustExec(t, ctx, same, `LISTEN shared`)
		mustExec(t, ctx, admin, `NOTIFY shared, 'datax only'`)
		wantNotify(t, ctx, same, "shared", "datax only")
		noNotify(t, ctx, other)
		mustExec(t, ctx, other, `NOTIFY shared, 'other only'`)
		wantNotify(t, ctx, other, "shared", "other only")
		noNotify(t, ctx, same)
	})

	// A node that cannot reach a peer keeps what it owes it and delivers
	// it, in order, once it can.
	t.Run("peer unreachable", func(t *testing.T) {
		far, near := notifyConn(t, ctx, tc, 2), notifyConn(t, ctx, tc, 1)
		notifier := notifyConn(t, ctx, tc, 0)
		mustExec(t, ctx, far, `LISTEN cut`)
		mustExec(t, ctx, near, `LISTEN cut`)
		tc.Nodes[0].InjectRPCDrop(func(to base.NodeID) bool { return to == 3 })
		defer tc.Nodes[0].InjectRPCDrop(nil)
		for i := 0; i < 5; i++ {
			mustExec(t, ctx, notifier, `NOTIFY cut, '`+fmt.Sprint(i)+`'`)
		}
		for i := 0; i < 5; i++ {
			wantNotify(t, ctx, near, "cut", fmt.Sprint(i))
		}
		noNotify(t, ctx, far)
		tc.Nodes[0].InjectRPCDrop(nil)
		for i := 0; i < 5; i++ {
			wantNotify(t, ctx, far, "cut", fmt.Sprint(i))
		}
		noNotify(t, ctx, far)
	})

	// A listener that does not take its notifications — here because it
	// sits in a transaction, where delivery waits — is disconnected when
	// its queue fills, with an error that says why; the notifier is not
	// held up and other listeners are not affected.
	t.Run("slow listener", func(t *testing.T) {
		slow, quick := notifyConn(t, ctx, tc, 1), notifyConn(t, ctx, tc, 1)
		notifier := notifyConn(t, ctx, tc, 0)
		mustExec(t, ctx, slow, `LISTEN flood`)
		mustExec(t, ctx, quick, `LISTEN flood`)
		mustExec(t, ctx, slow, `BEGIN`)
		payload := strings.Repeat("z", 7900)
		const n = 2400 // well past the 16 MiB a session may have waiting
		go func() {
			for i := 0; i < n; i++ {
				if _, err := notifier.Exec(ctx, `SELECT pg_notify('flood', $1)`, fmt.Sprintf("%d %s", i, payload)); err != nil {
					t.Errorf("notify %d: %v", i, err)
					return
				}
			}
		}()
		for i := 0; i < n; i++ {
			if got := waitNotify(t, ctx, quick); !strings.HasPrefix(got.Payload, fmt.Sprintf("%d ", i)) {
				t.Fatalf("quick listener: notification %d out of order", i)
			}
		}
		_, err := slow.Exec(ctx, `COMMIT`)
		if err == nil {
			_, err = slow.Exec(ctx, `SELECT 1`)
		}
		if sqlState(err) != "54000" {
			t.Fatalf("slow listener: %v, want a FATAL with SQLSTATE 54000", err)
		}
	})

	// A drained node says goodbye to an idle listener the way it does to
	// any idle connection. Last, because the node is of no further use.
	t.Run("drain", func(t *testing.T) {
		listener := notifyConn(t, ctx, tc, 2)
		mustExec(t, ctx, listener, `LISTEN drained`)
		dctx, dcancel := context.WithTimeout(ctx, 20*time.Second)
		defer dcancel()
		go tc.Nodes[2].Drain(dctx)
		wctx, wcancel := context.WithTimeout(ctx, 20*time.Second)
		defer wcancel()
		if _, err := listener.WaitForNotification(wctx); sqlState(err) != "57P01" {
			t.Fatalf("idle listener on a draining node: %v, want SQLSTATE 57P01", err)
		}
	})
}

// TestListenNotifySecure: across nodes of a secure cluster, where the
// fan-out call is authorized by the node certificate and the push to an
// idle client goes through TLS.
func TestListenNotifySecure(t *testing.T) {
	tc, certsDir := startSecureCluster(t, "topsecret")
	ctx, cancel := context.WithTimeout(context.Background(), 180*time.Second)
	defer cancel()
	connect := func(node int) *pgx.Conn {
		t.Helper()
		url := fmt.Sprintf("postgres://root:topsecret@%s/datax?sslmode=verify-ca&sslrootcert=%s",
			tc.Nodes[node].SQLAddr(), filepath.Join(certsDir, "ca.crt"))
		var conn *pgx.Conn
		var err error
		// Root's credential is seeded asynchronously.
		for deadline := time.Now().Add(60 * time.Second); ; {
			if conn, err = pgx.Connect(ctx, url); err == nil {
				break
			}
			if time.Now().After(deadline) {
				t.Fatalf("connect to node %d: %v", node, err)
			}
			time.Sleep(200 * time.Millisecond)
		}
		t.Cleanup(func() { _ = conn.Close(context.Background()) })
		return conn
	}
	notifier, local, remote := connect(0), connect(0), connect(2)
	mustExec(t, ctx, local, `LISTEN sec`)
	mustExec(t, ctx, remote, `LISTEN sec`)
	for i := 0; i < 200; i++ {
		mustExec(t, ctx, notifier, `NOTIFY sec, '`+fmt.Sprint(i)+`'`)
	}
	for _, l := range []*pgx.Conn{local, remote} {
		for i := 0; i < 200; i++ {
			wantNotify(t, ctx, l, "sec", fmt.Sprint(i))
		}
		// The connection is usable after being pushed to.
		mustExec(t, ctx, l, `SELECT 1`)
	}

	// The fan-out call is a node's to make. An operator's certificate,
	// root's included, cannot use it to put a notification in front of
	// listeners without a statement (and an identity) behind it.
	if err := security.CreateClientCert(certsDir, "root"); err != nil {
		t.Fatal(err)
	}
	tlsCfg, err := security.LoadClientTLS(certsDir, "root")
	if err != nil {
		t.Fatal(err)
	}
	trans := rpc.NewTransport(hlc.NewClock(nil, base.DefaultMaxClockOffset), nil, nil)
	trans.SetTLS(tlsCfg)
	var resp cluster.AdminResponse
	forged := &notify.Batch{From: 9, Epoch: 1, Seq: 1, Notifications: []notify.Notification{{Database: "datax", Channel: "sec", Payload: "forged"}}}
	if err := trans.Call(ctx, tc.Nodes[0].Addr(), "admin", cluster.AdminRequest{Op: "notify", Notify: forged}, &resp); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(resp.Error, "permission denied") {
		t.Fatalf("notify as root over the admin RPC: %q, want permission denied", resp.Error)
	}
	noNotify(t, ctx, local)
}
