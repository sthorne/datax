package pgwire

import (
	"container/list"
	"context"
	"crypto/tls"
	"encoding/binary"
	"errors"
	"fmt"
	"github.com/sthorne/datax/pkg/metrics"
	"github.com/sthorne/datax/pkg/util/log"
	"math"
	"net"
	"os"
	"runtime/debug"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/jackc/pgx/v5/pgproto3"

	"github.com/sthorne/datax/pkg/kvclient"
	"github.com/sthorne/datax/pkg/sql"
	"github.com/sthorne/datax/pkg/sql/catalog"
	"github.com/sthorne/datax/pkg/sql/notify"
	"github.com/sthorne/datax/pkg/sql/parser"
	"github.com/sthorne/datax/pkg/sql/tsearch"
	"github.com/sthorne/datax/pkg/sql/types"
	"github.com/sthorne/datax/pkg/sql/vector"
)

// PostgreSQL type OIDs for datax's SQL types.
const (
	oidBool        = 16
	oidBytea       = 17
	oidInt8        = 20
	oidInt2        = 21
	oidInt4        = 23
	oidText        = 25
	oidFloat8      = 701
	oidBpchar      = 1042
	oidVarchar     = 1043
	oidDate        = 1082
	oidTimestamp   = 1114
	oidTimestamptz = 1184
	oidUUID        = 2950
	oidNumeric     = 1700
	oidJsonb       = 3802
	oidTsvector    = 3614
	oidTsquery     = 3615
	oidTime        = 1083
	oidInterval    = 1186
	oidVector      = vector.OID
)

// pgEpochOffsetMicros converts between the Unix epoch and PostgreSQL's
// binary-format epoch (2000-01-01).
const (
	pgEpochOffsetMicros = int64(946684800) * 1e6
	pgEpochOffsetDays   = int64(10957)
)

// colOID is the wire type of a result column: the family's OID, refined
// by the backing column's modifiers (int2 / int4, varchar / bpchar,
// timestamp without time zone).
func colOID(col sql.ResultColumn) uint32 {
	if col.Type == types.Enum && col.EnumType != 0 {
		return uint32(catalog.EnumOID(col.EnumType))
	}
	if col.Type.IsArray() {
		elem := col
		elem.Type = col.Type.Elem()
		if oid, ok := arrayOIDs[colOID(elem)]; ok {
			return oid
		}
		return 1009
	}
	switch col.Type {
	case types.Int:
		switch col.Width {
		case 2:
			return oidInt2
		case 4:
			return oidInt4
		}
	case types.String:
		switch {
		case col.Char:
			return oidBpchar
		case col.MaxLen > 0:
			return oidVarchar
		}
	case types.Timestamp:
		if col.NoTZ {
			return oidTimestamp
		}
	}
	return typeOID(col.Type)
}

// colSize is pg_type.typlen for a result column (-1 = variable).
func colSize(col sql.ResultColumn) int16 {
	if col.Type == types.Int && (col.Width == 2 || col.Width == 4) {
		return int16(col.Width)
	}
	return typeSize(col.Type)
}

// arrayOIDs maps an element type's OID to its array type's.
var arrayOIDs = map[uint32]uint32{
	oidBool: 1000, oidBytea: 1001, oidInt2: 1005, oidInt4: 1007, oidInt8: 1016, oidText: 1009, oidVarchar: 1015, oidBpchar: 1014,
	oidFloat8: 1022, oidNumeric: 1231, oidDate: 1182, oidTimestamp: 1115, oidTimestamptz: 1185, oidUUID: 2951, oidJsonb: 3807,
	oidInterval: 1187, oidTime: 1183, oidTsvector: 3643, oidTsquery: 3645,
}

func typeOID(f types.Family) uint32 {
	if f.IsArray() {
		if oid, ok := arrayOIDs[typeOID(f.Elem())]; ok {
			return oid
		}
		return 1009
	}
	switch f {
	case types.Int:
		return oidInt8
	case types.Float:
		return oidFloat8
	case types.Bool:
		return oidBool
	case types.Timestamp:
		return oidTimestamptz
	case types.Date:
		return oidDate
	case types.Bytes:
		return oidBytea
	case types.Uuid:
		return oidUUID
	case types.Decimal:
		return oidNumeric
	case types.Jsonb:
		return oidJsonb
	case types.Tsvector:
		return oidTsvector
	case types.Tsquery:
		return oidTsquery
	case types.IntervalFam:
		return oidInterval
	case types.Time:
		return oidTime
	case types.Vector:
		return oidVector
	case types.Enum:
		return oidText // a value without its type describes as text
	default:
		return oidText
	}
}

func typeSize(f types.Family) int16 {
	switch f {
	case types.Int, types.Float, types.Timestamp, types.Time:
		return 8
	case types.IntervalFam:
		return 16
	case types.Bool:
		return 1
	case types.Date:
		return 4
	case types.Uuid:
		return 16
	default:
		return -1
	}
}

type prepared struct {
	text      string
	stmt      parser.Statement // nil = empty statement
	nParams   int
	paramFams []types.Family // inferred; Unknown = text
	cols      []sql.ResultColumn
	// shape is the statement's fingerprint for the activity accounting,
	// taken once at Parse from the statement as written (planning may
	// rewrite the tree in place: stripTableAlias), so every execution
	// is charged to the shape the same text has in the simple protocol.
	// nil without accounting.
	shape *parser.Shape
}

type portal struct {
	stmt       *prepared
	params     []types.Datum
	resFormats []int16 // normalized per result column (0 text, 1 binary)
	// Suspension state (row-limited Execute): the statement runs once on
	// the first Execute and the materialized result is then served across
	// Executes — up to MaxRows rows each, PortalSuspended in between,
	// CommandComplete when exhausted. PostgreSQL portal semantics: a
	// portal executes its statement once; re-running requires a re-Bind
	// (which replaces this struct and resets the state).
	// A streamed result (res.Stream) is pulled MaxRows rows per Execute
	// instead; it is closed when the portal is.
	res    *sql.Result
	offset int
	done   bool
}

// close releases a portal's stream, if any.
func (pt *portal) close(ctx context.Context) {
	if pt.res != nil && pt.res.Stream != nil {
		pt.res.Stream.Close(ctx)
	}
}

type conn struct {
	backend *pgproto3.Backend
	nc      net.Conn
	session *sql.Session
	opts    ServerOptions
	tlsDone bool

	stmts   map[string]*prepared
	portals map[string]*portal
	// parsed is the connection's parse cache for the simple protocol:
	// query text → its statements, bounded (parseCacheSize, LRU). A
	// client that repeats a text (a dashboard's queries, a driver in
	// simple mode re-sending the same statement) skips the parse, and the
	// session's plan cache sees the same statement each time (issue
	// #107).
	parsed *parseCache
	// ioErr is a failed write of streamed rows (the connection is gone);
	// the run loop ends on it.
	ioErr error
	// skipToSync: an extended-protocol error occurred; ignore messages
	// until Sync (PostgreSQL protocol rule).
	skipToSync bool
	// peeked is a message (or the error receiving it) read ahead of its
	// turn by an Execute deciding whether it ends its pipeline
	// (readAhead); run handles it next instead of receiving.
	peeked    bool
	peekedMsg pgproto3.FrontendMessage
	peekedErr error
	// act is the server's client accounting (nil in unit tests that build
	// a conn without a server); srv the server (nil likewise).
	act *Activity
	srv *Server
	// authDeadline is when startup's socket deadline expires (zero once
	// startup is over); the verifier lookup is bounded by it too.
	authDeadline time.Time
	// pid and secret are the connection's BackendKeyData; stmtCancel
	// (under mu) cancels the statement in flight.
	pid        int32
	secret     uint32
	stmtCancel context.CancelFunc

	// mu guards st, the connection's phase as seen by Server.Drain: the
	// connection goroutine records when it is idle (waiting for the next
	// client message) and whether a transaction is open; Drain sets the
	// draining flags and, when the connection is idle, wakes its Receive
	// with a read deadline so the goroutine itself says goodbye.
	mu sync.Mutex
	st connState

	// listener is the session's LISTEN subscriptions and its queue of
	// undelivered notifications (nil without a hub). The rest is the
	// machinery that pushes them to an idle client; see notify.go.
	listener  *notify.Listener
	pushMu    sync.Mutex
	pushOK    bool
	pushWake  chan struct{}
	pushStart sync.Once
	// done is closed when the connection's goroutine returns.
	done chan struct{}

	// dataRow and complete are the connection's reused DataRow and
	// CommandComplete messages (rowMsg, sendComplete).
	dataRow pgproto3.DataRow
	// rowBuf is the buffer result rows are encoded into, kept between
	// results (sendStream, sendDataRowRange).
	rowBuf   []byte
	complete pgproto3.CommandComplete
	// tok is the activity token of the statement in flight (Activity.begin).
	tok stmtToken
}

// connState is the drain-visible phase of a connection. idle means
// waiting for a client message at a cycle boundary — after a simple
// Query or a Sync has been answered with ReadyForQuery — never between
// the messages of one extended-protocol pipeline, where a goodbye would
// land between an Execute already applied and its Sync and the client
// would take the 57P01 as that statement's outcome.
type connState struct {
	idle, inTxn bool
	// draining asks the connection to end at its next idle point outside
	// a transaction; cut asks it to end at its next idle point regardless
	// (the drain deadline).
	draining, cut bool
	// bye, when set, is the error the connection ends with instead of
	// adminShutdown (a listener that missed notifications).
	bye *pgproto3.ErrorResponse
}

// adminShutdown is the error a drained connection ends with: PostgreSQL's
// FATAL 57P01, which drivers report as the failure of the statement in
// flight (or the next one) before reconnecting.
var adminShutdown = &pgproto3.ErrorResponse{
	Severity: "FATAL", SeverityUnlocalized: "FATAL", Code: "57P01",
	Message: "terminating connection due to administrator command",
}

// drain asks the connection to end: soft (at its next idle point
// outside a transaction) or hard (now: an idle connection is woken to
// say goodbye, a busy one is closed under its statement).
func (c *conn) drain(hard bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.st.draining = true
	if hard {
		c.st.cut = true
	}
	switch {
	case c.st.idle && (hard || !c.st.inTxn):
		_ = c.nc.SetReadDeadline(time.Now()) // wakes Receive; see run
	case hard:
		_ = c.nc.Close()
	}
}

// setIdle records the connection's phase and reports whether a pending
// drain means it should say goodbye now rather than serve (or wait
// for) another message.
func (c *conn) setIdle(idle bool) (goodbye bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.st.idle = idle
	c.st.inTxn = c.session.State() == sql.StateOpen
	return c.st.cut || (c.st.draining && !c.st.inTxn)
}

// goodbye ends a drained connection: the admin_shutdown error, flushed;
// the caller returns and the deferred cleanup rolls back and closes.
func (c *conn) goodbye() {
	_ = c.nc.SetReadDeadline(time.Time{})
	c.mu.Lock()
	bye := c.st.bye
	c.mu.Unlock()
	if bye == nil {
		bye = adminShutdown
	}
	c.backend.Send(bye)
	_ = c.backend.Flush()
}

// isTimeout reports a read that ended because of a deadline (the way a
// drain wakes an idle connection) rather than a broken connection.
func isTimeout(err error) bool {
	var ne net.Error
	return errors.Is(err, os.ErrDeadlineExceeded) || (errors.As(err, &ne) && ne.Timeout())
}

// execute runs a statement under the connection's cancel registration
// and activity accounting. emit, when given, delivers the result's rows
// inside that scope (a streamed result is pulled there, so cancellation
// covers the pull) and returns how many it sent.
//
// shape is the statement's fingerprint when the caller has it (a
// prepared statement's, taken at Parse); nil computes it here.
func (c *conn) execute(ctx context.Context, stmt parser.Statement, text string, shape *parser.Shape, params []types.Datum, emit func(context.Context, *sql.Result) (int64, *sql.Error)) (*sql.Result, *sql.Error) {
	ctx, cancel := c.beginStatement(ctx)
	defer c.endStatement(cancel)
	_, isSet := stmt.(*parser.SetVar)
	discardAll := false
	if d, ok := stmt.(*parser.Discard); ok && d.Kind == "all" {
		isSet, discardAll = true, true
	}
	var before [][2]string
	if isSet {
		before = c.session.ReportedParams()
	}
	var tok *stmtToken
	if c.act != nil {
		tok = c.act.begin(c, stmt, text, shape)
	}
	res, serr := c.session.Execute(ctx, stmt, params)
	var rows int64
	if res != nil {
		rows = int64(len(res.Rows))
		for _, msg := range res.Notices {
			if !c.session.SendsNotices() {
				break // client_min_messages above NOTICE
			}
			c.backend.Send(&pgproto3.NoticeResponse{Severity: "NOTICE", SeverityUnlocalized: "NOTICE", Code: "00000", Message: msg})
		}
		for _, msg := range res.Warnings {
			if !c.session.SendsWarnings() {
				break // client_min_messages = error
			}
			c.backend.Send(&pgproto3.NoticeResponse{Severity: "WARNING", SeverityUnlocalized: "WARNING", Code: "01000", Message: msg})
		}
	}
	if serr == nil && emit != nil && res != nil {
		rows, serr = emit(ctx, res)
	}
	if tok != nil {
		// The rows this statement read, taken (and reset) here so the
		// next statement on this session is not charged for them.
		tok.scanned = c.session.TakeScanned()
		tok.end(rows, serr, c.session.State() == sql.StateOpen)
		c.act.setSession(c, c.session.Database(), c.session.ApplicationName())
	}
	if discardAll && serr == nil {
		// DISCARD ALL deallocates every prepared statement: the named
		// protocol-level ones too (they share PostgreSQL's one table).
		for name := range c.stmts {
			if name != "" {
				delete(c.stmts, name)
			}
		}
	}
	if isSet && serr == nil {
		// A changed reported parameter (application_name, TimeZone, ...)
		// is announced, as PostgreSQL's GUC_REPORT does.
		for i, kv := range c.session.ReportedParams() {
			if i < len(before) && before[i][1] != kv[1] {
				c.backend.Send(&pgproto3.ParameterStatus{Name: kv[0], Value: kv[1]})
			}
		}
	}
	return res, serr
}

// newPreAuthBackend is a backend on nc with the pre-authentication
// message size cap; startup lifts it once the client has authenticated.
func newPreAuthBackend(nc net.Conn) *pgproto3.Backend {
	be := pgproto3.NewBackend(nc, nc)
	be.SetMaxBodyLen(maxPreAuthMessageSize)
	return be
}

func newConn(nc net.Conn, db *kvclient.DB, cat *catalog.Accessor, opts ServerOptions) *conn {
	session := sql.NewSession(db, cat)
	session.SetStreaming(true)
	c := &conn{
		backend:  newPreAuthBackend(nc),
		nc:       nc,
		session:  session,
		opts:     opts,
		stmts:    make(map[string]*prepared),
		portals:  make(map[string]*portal),
		pushWake: make(chan struct{}, 1),
		done:     make(chan struct{}),
	}
	if opts.Notify != nil {
		c.listener = opts.Notify.NewListener(c.wakePush, c.notifyEnd)
		session.SetNotify(opts.Notify, c.listener)
	}
	session.SetKeyService(opts.KeyService)
	session.SetTempRegistry(opts.TempTables)
	if opts.Advisory != nil {
		session.SetAdvisory(opts.Advisory.NewHolder(c.advisoryLost))
	}
	session.SetTransport(false, isLoopbackAddr(nc.RemoteAddr()))
	return c
}

// isLoopbackAddr reports whether a client connected from this machine:
// a loopback address, or a Unix socket.
func isLoopbackAddr(a net.Addr) bool {
	switch v := a.(type) {
	case *net.UnixAddr:
		return true
	case *net.TCPAddr:
		return v.IP.IsLoopback()
	case nil:
		return false
	}
	host, _, err := net.SplitHostPort(a.String())
	if err != nil {
		return false
	}
	ip := net.ParseIP(host)
	return ip != nil && ip.IsLoopback()
}

// Client message size caps (the body after the type byte and length).
// pgproto3 allocates a message's declared length before it reads a byte
// of the body, so without a cap 5 bytes from anyone who can open a
// socket ('p' and a length of 0x7ffffff0) cost 2 GiB.
const (
	// maxPreAuthMessageSize bounds every message read before
	// authentication completes. The SASL messages are a few hundred
	// bytes; 10000 is PostgreSQL's MAX_STARTUP_PACKET_LENGTH, the bound
	// pgproto3 itself applies to the startup packet (which is read by
	// ReceiveStartupMessage, not under this cap).
	maxPreAuthMessageSize = 10000
	// DefaultMaxMessageSize bounds a message once the connection is
	// authenticated. PostgreSQL's bound is 1 GB, its largest field; a
	// datax value is bounded by what one replicated write carries
	// (60 MiB to a range), so 256 MiB leaves room for such a value in a
	// statement's text, hex-encoded (twice its size), with plenty to
	// spare. COPY streams its data in CopyData messages that drivers cut
	// at 64 KiB (pgx) or a row (psql), so its total size is unbounded.
	DefaultMaxMessageSize = 256 << 20
)

// maxMessageSize is the post-authentication cap in force (0 = none).
func (c *conn) maxMessageSize() int {
	switch {
	case c.opts.MaxMessageSize < 0:
		return 0
	case c.opts.MaxMessageSize == 0:
		return DefaultMaxMessageSize
	}
	return c.opts.MaxMessageSize
}

// refuseOversized ends the connection when err is a message over the
// size cap and reports whether it was: the client is told with a FATAL
// 08P01 (protocol_violation, as PostgreSQL reports an invalid message
// length) and the socket is closed. pgproto3 has read only the header,
// so the stream is at an unknown point and nothing more can be read.
func (c *conn) refuseOversized(err error) bool {
	var tooLong *pgproto3.ExceededMaxBodyLenErr
	if !errors.As(err, &tooLong) {
		return false
	}
	_ = c.nc.SetWriteDeadline(time.Now().Add(time.Second))
	c.sendFatal(sql.CodeInvalidParameter, fmt.Sprintf("invalid message length: %d bytes exceeds the limit of %d",
		tooLong.ActualBodyLen, tooLong.MaxExpectedBodyLen))
	_ = c.nc.Close()
	return true
}

// testingPanicOnMessage, when set, sees every message the message loop
// receives: a test's way to inject a panic into a connection.
var testingPanicOnMessage func(pgproto3.FrontendMessage)

// errCancelServed ends a connection that carried a CancelRequest: it
// never authenticates and carries nothing else, so it does not go on to
// the message loop (where it would sit, unauthenticated and now past its
// deadline, until the client closed it).
var errCancelServed = errors.New("cancel request served")

// authTimeout is the startup deadline in force (zero = none).
func (c *conn) authTimeout() time.Duration {
	switch {
	case c.opts.AuthTimeout < 0:
		return 0
	case c.opts.AuthTimeout == 0:
		return DefaultAuthTimeout
	}
	return c.opts.AuthTimeout
}

func (c *conn) run(ctx context.Context) (err error) {
	defer close(c.done)
	if c.listener != nil {
		defer c.listener.Close()
	}
	defer c.session.Close(context.Background())
	defer c.reapPortals() // streams first: their transactions are the session's
	// A panic on this connection's goroutine ends this connection, not
	// the node: it is logged at error level with its stack (the evidence
	// of the bug underneath), the client is told with a FATAL XX000 when
	// the socket still takes it, and the deferred cleanup above runs as
	// for any other end.
	defer func() {
		if r := recover(); r != nil {
			log.Errorf("panic on pgwire connection %s (recovered; the connection is closed): %v\n%s", c.nc.RemoteAddr(), r, debug.Stack())
			_ = c.nc.SetWriteDeadline(time.Now().Add(time.Second))
			c.backend.Send(&pgproto3.ErrorResponse{Severity: "FATAL", SeverityUnlocalized: "FATAL", Code: sql.CodeInternal,
				Message: fmt.Sprintf("internal error: %v", r)})
			_ = c.backend.Flush()
			_ = c.nc.Close()
			err = fmt.Errorf("panic: %v", r)
		}
	}()
	if err := c.startup(ctx); err != nil {
		if errors.Is(err, errCancelServed) {
			return nil
		}
		return err
	}
	boundary := true // the next message starts a new protocol cycle
	for {
		if c.setIdle(boundary) && boundary {
			c.goodbye()
			return nil
		}
		// Idle inside a transaction block past
		// idle_in_transaction_session_timeout: the block is rolled back
		// (its intents released) and the connection ends with 25P03.
		idleArmed := false
		if d := c.session.IdleInTransactionTimeout(); boundary && d > 0 && c.session.State() != sql.StateIdle {
			_ = c.nc.SetReadDeadline(time.Now().Add(d))
			idleArmed = true
		}
		// Idle outside a transaction is when notifications are pushed to
		// the client as they arrive (notify.go).
		pushing := boundary && c.listener != nil && c.session.State() == sql.StateIdle
		if pushing {
			c.allowPush()
		}
		var msg pgproto3.FrontendMessage
		if c.peeked {
			msg, err = c.peekedMsg, c.peekedErr
			c.peeked, c.peekedMsg, c.peekedErr = false, nil, nil
		} else {
			msg, err = c.backend.Receive()
		}
		if err == nil && testingPanicOnMessage != nil {
			testingPanicOnMessage(msg)
		}
		if idleArmed {
			_ = c.nc.SetReadDeadline(time.Time{})
		}
		goodbye := c.setIdle(false)
		if pushing && !c.endPush(err != nil) {
			return err
		}
		if err != nil {
			if c.refuseOversized(err) {
				return err
			}
			if goodbye && boundary && isTimeout(err) {
				c.goodbye() // woken by drain, not a broken connection
				return nil
			}
			if idleArmed && isTimeout(err) {
				c.session.Close(context.Background())
				c.backend.Send(idleInTxnTimeout)
				_ = c.backend.Flush()
				return nil
			}
			return err
		}
		switch msg.(type) {
		case *pgproto3.Query, *pgproto3.Sync, *pgproto3.Terminate, *pgproto3.FunctionCall:
			boundary = true
		default:
			boundary = false
		}
		if c.ioErr != nil {
			return c.ioErr
		}
		switch m := msg.(type) {
		case *pgproto3.Query:
			c.skipToSync = false
			c.handleSimpleQuery(ctx, m.String)
			if c.session.State() != sql.StateOpen {
				c.reapPortals() // a simple-query COMMIT/ROLLBACK ends them
			}
			c.sendReady()
			if err := c.backend.Flush(); err != nil {
				return err
			}

		case *pgproto3.FunctionCall:
			c.skipToSync = false
			c.handleFunctionCall(ctx, m)
			// A pipeline's implicit transaction it ran in ends with it.
			c.endImplicit(ctx, true)
			if c.session.State() != sql.StateOpen {
				c.reapPortals()
			}
			c.sendReady()
			if err := c.backend.Flush(); err != nil {
				return err
			}

		case *pgproto3.Parse:
			if !c.skipToSync {
				c.handleParse(ctx, m)
			}
		case *pgproto3.Bind:
			if !c.skipToSync {
				c.handleBind(m)
			}
		case *pgproto3.Describe:
			if !c.skipToSync {
				c.handleDescribe(m)
			}
		case *pgproto3.Execute:
			if !c.skipToSync {
				m = c.readAhead(ctx, m)
				c.handleExecute(ctx, m)
			}
		case *pgproto3.Close:
			if !c.skipToSync {
				switch m.ObjectType {
				case 'S':
					delete(c.stmts, m.Name)
				case 'P':
					if pt, ok := c.portals[m.Name]; ok {
						pt.close(ctx)
						delete(c.portals, m.Name)
					}
				}
				c.backend.Send(&pgproto3.CloseComplete{})
			}
		case *pgproto3.Sync:
			// Sync ends the pipeline's implicit transaction: committed,
			// or rolled back if any of its messages failed.
			c.endImplicit(ctx, !c.skipToSync)
			c.skipToSync = false
			// Portals do not survive the implicit transaction: outside an
			// explicit BEGIN, Sync ends the cycle and reaps them (inside
			// one they live until the transaction ends).
			if c.session.State() != sql.StateOpen {
				c.reapPortals()
			}
			c.sendReady()
			if err := c.backend.Flush(); err != nil {
				return err
			}
		case *pgproto3.Flush:
			if err := c.backend.Flush(); err != nil {
				return err
			}
		case *pgproto3.Terminate:
			return nil
		case *pgproto3.CopyData, *pgproto3.CopyDone, *pgproto3.CopyFail:
			// Copy sub-protocol messages outside copy mode are silently
			// discarded (PostgreSQL behavior). This also drains a client
			// that streamed CopyData optimistically before seeing the
			// ErrorResponse for a COPY that failed to start (pgx does).
		default:
			c.sendError(&sql.Error{Code: sql.CodeFeatureNotSupported, Msg: fmt.Sprintf("unsupported message %T", msg)})
			c.skipToSync = true
		}
	}
}

// startup runs handleStartup under the authentication deadline (issue
// #212): a connection that sends nothing, or stalls anywhere in the
// exchange — between SSLRequest and the TLS handshake, between the
// server-first and the client's final — is closed at the deadline
// instead of being parked with its goroutines, session and registry
// entry. The deadline is on writes too, so a peer that stops reading
// the server's half is cut the same way. It is set on the raw socket
// and cleared on whatever c.nc is afterwards (the tls.Conn forwards to
// the socket underneath), and the pre-authentication slot the accept
// loop took is released however startup ends.
func (c *conn) startup(ctx context.Context) error {
	if c.srv != nil {
		defer c.srv.pendingDone()
	}
	if d := c.authTimeout(); d > 0 {
		c.authDeadline = time.Now().Add(d)
		_ = c.nc.SetDeadline(c.authDeadline)
	}
	err := c.handleStartup(ctx)
	_ = c.nc.SetDeadline(time.Time{})
	c.authDeadline = time.Time{}
	if err == nil {
		c.backend.SetMaxBodyLen(c.maxMessageSize())
	}
	if err != nil && isTimeout(err) {
		metrics.SQLPreAuthClosed.WithLabelValues(metrics.PreAuthHandshakeTimeout).Inc()
		log.Audit("sql-preauth-closed", "remote", c.nc.RemoteAddr().String(), "cause", metrics.PreAuthHandshakeTimeout)
		return fmt.Errorf("startup not completed within %s", c.authTimeout())
	}
	return err
}

func (c *conn) handleStartup(ctx context.Context) error {
	for {
		msg, err := c.backend.ReceiveStartupMessage()
		if err != nil {
			return err
		}
		switch m := msg.(type) {
		case *pgproto3.SSLRequest:
			if c.opts.TLS != nil {
				if _, err := c.nc.Write([]byte{'S'}); err != nil {
					return err
				}
				tc := tls.Server(c.nc, c.opts.TLS)
				if err := tc.Handshake(); err != nil {
					return err
				}
				c.nc = tc
				c.backend = newPreAuthBackend(tc)
				c.tlsDone = true
				c.session.SetTransport(true, isLoopbackAddr(tc.RemoteAddr()))
				continue
			}
			// Insecure mode: reply 'N', client continues in cleartext.
			if _, err := c.nc.Write([]byte{'N'}); err != nil {
				return err
			}
		case *pgproto3.GSSEncRequest:
			if _, err := c.nc.Write([]byte{'N'}); err != nil {
				return err
			}
		case *pgproto3.StartupMessage:
			if c.opts.TLS != nil && !c.tlsDone {
				c.sendError(&sql.Error{Code: "28000", Msg: "connection requires TLS"})
				_ = c.backend.Flush()
				return fmt.Errorf("cleartext startup refused in secure mode")
			}
			if c.opts.Auth != nil {
				// A CA-verified client certificate whose CommonName matches
				// the startup user authenticates without a password.
				user := m.Parameters["user"]
				if user != "" && c.clientCertUser() == user {
					if c.opts.CanLogin != nil {
						ok, err := c.opts.CanLogin(ctx, user)
						if err != nil || !ok {
							metrics.AuthFailures.Inc()
							log.Audit("sql-auth-failure", "principal", user, "remote", c.nc.RemoteAddr().String(), "reason", "certificate role cannot log in")
							c.sendError(&sql.Error{Code: "28000", Msg: fmt.Sprintf("role %q is not permitted to log in", user)})
							_ = c.backend.Flush()
							return fmt.Errorf("certificate role %q cannot log in", user)
						}
					}
					c.backend.Send(&pgproto3.AuthenticationOk{})
					c.srv.Activity().setAuthVia(c, "cert")
				} else if err := c.authenticateSCRAM(user); err != nil {
					return err
				} else {
					c.srv.Activity().setAuthVia(c, "scram")
				}
			} else {
				c.backend.Send(&pgproto3.AuthenticationOk{}) // trust auth
				c.srv.Activity().setAuthVia(c, "trust")
			}
			// Privileges are enforced against this identity. In trust mode
			// it is client-claimed (nothing verified it) — documented.
			c.session.SetUser(m.Parameters["user"])
			if c.act != nil {
				c.act.setUser(c, m.Parameters["user"])
			}
			// The startup database selects the session's current database;
			// an unknown one is refused like PostgreSQL does (3D000), and
			// one whose CONNECT was revoked from PUBLIC needs a grant.
			if dbName := m.Parameters["database"]; dbName != "" {
				if serr := c.session.UseDatabase(ctx, dbName); serr != nil {
					c.sendError(serr)
					_ = c.backend.Flush()
					return fmt.Errorf("startup database %q: %v", dbName, serr)
				}
			}
			// The limits and defaults are read best effort: a catalog
			// that cannot be read in time costs them, not the session.
			ictx, icancel := context.WithTimeout(ctx, 5*time.Second)
			info, serr := c.session.StartupInfo(ictx)
			icancel()
			if serr != nil {
				log.Warnf("reading connection limits and database defaults for %q: %v", m.Parameters["user"], serr)
				info = sql.StartupInfo{RoleLimit: -1, DatabaseLimit: -1}
			}
			if serr = c.checkConnectionLimits(info); serr != nil {
				c.sendError(serr)
				_ = c.backend.Flush()
				return fmt.Errorf("startup refused: %v", serr)
			}
			// ALTER DATABASE ... SET's defaults, before the client's own
			// parameters, as PostgreSQL applies them; the reported
			// parameters below carry their values.
			c.session.ApplyDatabaseDefaults(ctx, info.Settings)
			// Then the settings the startup packet carries (TimeZone,
			// DateStyle, ... and those of options' -c name=value), so
			// ParameterStatus tells the client the session's own
			// TimeZone, as PostgreSQL's does.
			for _, kv := range startupSettings(m.Parameters) {
				_, _ = c.session.Execute(ctx, &parser.SetVar{Name: kv[0], Value: kv[1]}, nil)
			}
			if app := m.Parameters["application_name"]; app != "" {
				_, _ = c.session.Execute(ctx, &parser.SetVar{Name: "application_name", Value: app}, nil)
			}
			// PostgreSQL 14's reported parameters, each once, with the
			// session's values.
			for _, kv := range c.session.StartupParams(ctx) {
				c.backend.Send(&pgproto3.ParameterStatus{Name: kv[0], Value: kv[1]})
			}
			c.backend.Send(&pgproto3.BackendKeyData{ProcessID: uint32(c.pid), SecretKey: binary.BigEndian.AppendUint32(nil, c.secret)})
			c.sendReady()
			return c.backend.Flush()
		case *pgproto3.CancelRequest:
			// An out-of-band cancel: this connection carries nothing else.
			// It is unauthenticated by design — PostgreSQL's is too — but
			// on a secure listener it is refused in cleartext for the same
			// reason StartupMessage is above: the secret is the whole
			// authorization, and in the clear it is there for anyone on
			// the path to read and replay. pgx (v5.10) and libpq (PG17+)
			// encrypt the cancel connection whenever the connection it
			// cancels is encrypted; libpq before PG17 does not.
			if c.opts.TLS != nil && !c.tlsDone {
				noteCleartextCancel(c.nc.RemoteAddr().String())
				return fmt.Errorf("cleartext cancel refused in secure mode")
			}
			if c.srv != nil && len(m.SecretKey) >= 4 {
				c.srv.handleCancelRequest(int32(m.ProcessID), binary.BigEndian.Uint32(m.SecretKey[:4]))
			}
			return errCancelServed
		default:
			return fmt.Errorf("unexpected startup message %T", msg)
		}
	}
}

func (c *conn) sendReady() {
	status := byte('I')
	switch c.session.State() {
	case sql.StateOpen:
		status = 'T'
	case sql.StateFailed:
		status = 'E'
	}
	if status == 'I' && c.listener != nil {
		// Notifications wait for the end of a transaction, as in
		// PostgreSQL, and precede the ReadyForQuery that ends it.
		c.sendNotifications()
	}
	c.backend.Send(&pgproto3.ReadyForQuery{TxStatus: status})
}

func (c *conn) sendError(serr *sql.Error) {
	c.backend.Send(&pgproto3.ErrorResponse{
		Severity:            "ERROR",
		SeverityUnlocalized: "ERROR",
		Code:                serr.Code,
		Message:             serr.Msg,
		Detail:              serr.Detail,
		Hint:                serr.Hint,
	})
}

// parseCacheSize bounds a connection's parse cache.
const parseCacheSize = 64

// parseCache is a connection's text → statements cache (LRU).
type parseCache struct {
	entries map[string]*list.Element
	lru     *list.List
}

type parsedQuery struct {
	text  string
	stmts []parser.Statement
}

// parse returns the statements of q, from the connection's parse cache
// when it has seen the text before.
func (c *conn) parse(q string) ([]parser.Statement, error) {
	if c.parsed == nil {
		c.parsed = &parseCache{entries: make(map[string]*list.Element), lru: list.New()}
	}
	if el, ok := c.parsed.entries[q]; ok {
		c.parsed.lru.MoveToFront(el)
		metrics.SQLParseCacheHits.Inc()
		return el.Value.(*parsedQuery).stmts, nil
	}
	stmts, err := parser.Parse(q)
	if err != nil {
		return nil, err
	}
	c.parsed.entries[q] = c.parsed.lru.PushFront(&parsedQuery{text: q, stmts: stmts})
	for c.parsed.lru.Len() > parseCacheSize {
		old := c.parsed.lru.Back()
		c.parsed.lru.Remove(old)
		delete(c.parsed.entries, old.Value.(*parsedQuery).text)
	}
	return stmts, nil
}

// handleSimpleQuery runs a (possibly multi-statement) simple query.
//
// Outside a transaction block the statements of a multi-statement query
// run as one implicit transaction, PostgreSQL's: committed when the last
// has run, rolled back if one fails (the rest are not run). A COMMIT or
// ROLLBACK among them ends that transaction early and the statements
// after it start another; a BEGIN turns it into a transaction block that
// outlives the message. One of datax's online statements commits it too,
// and runs alone (Session.BeginImplicit). A query of one statement runs as its own
// implicit transaction (Session.Execute), automatically re-run on a
// serialization failure.
func (c *conn) handleSimpleQuery(ctx context.Context, q string) {
	stmts, err := c.parse(q)
	if err != nil {
		c.sendError(sql.ToSQLError(err))
		c.endImplicit(ctx, false)
		return
	}
	if len(stmts) == 0 {
		c.backend.Send(&pgproto3.EmptyQueryResponse{})
		c.endImplicit(ctx, true)
		return
	}
	ok := c.runSimpleStatements(ctx, q, stmts)
	c.endImplicit(ctx, ok)
}

// endImplicit ends the implicit transaction of a simple query or a
// pipeline, if one is open: committed when ok, else rolled back. A
// failed COMMIT is reported to the client.
func (c *conn) endImplicit(ctx context.Context, ok bool) {
	if !c.session.InImplicit() {
		return
	}
	if serr := c.session.EndImplicit(ctx, ok); serr != nil {
		c.sendError(serr)
	}
	if c.act != nil {
		c.act.txnEnded(c)
	}
}

// runSimpleStatements runs a simple query's statements in order, up to
// the first that fails, and reports whether all succeeded.
func (c *conn) runSimpleStatements(ctx context.Context, q string, stmts []parser.Statement) bool {
	multi := len(stmts) > 1
	for i, stmt := range stmts {
		if multi {
			c.session.BeginImplicit() // no-op inside a block
		}
		if cf, ok := stmt.(*parser.CopyFrom); ok {
			// COPY takes over the message stream, so it may only end a
			// simple query — running statements after copy mode would need
			// a continuation machine no client exercises.
			if i != len(stmts)-1 {
				c.sendError(&sql.Error{Code: sql.CodeFeatureNotSupported,
					Msg: "COPY FROM STDIN must be the last statement of a simple query"})
				return false
			}
			return c.handleCopyIn(ctx, cf)
		}
		res, serr := c.execute(ctx, stmt, q, nil, nil, func(ctx context.Context, res *sql.Result) (int64, *sql.Error) {
			if res.Copy != nil {
				// COPY ... TO STDOUT: the rows go out in copy-out mode.
				// It reads nothing from the client, so unlike COPY FROM
				// it may stand anywhere in a multi-statement query.
				return c.sendCopyOut(ctx, res)
			}
			if res.Columns == nil {
				return 0, nil
			}
			c.backend.Send(rowDescription(res.Columns))
			if res.Stream == nil {
				c.sendDataRows(res, nil)
				return int64(len(res.Rows)), nil
			}
			n, _, serr := c.sendStream(ctx, res, nil, 0)
			return n, serr
		})
		if serr != nil {
			c.sendError(serr)
			return false
		}
		c.sendComplete(res.Tag)
	}
	return true
}

// readAhead is an Execute's look at the message after it, taken before
// it runs, outside a transaction block. A Sync next means the Execute is
// its pipeline's only statement: it runs as its own implicit
// transaction, which commits with it and is re-run automatically on a
// serialization failure. Anything else means more of the pipeline may
// follow, and PostgreSQL runs everything up to the Sync as one implicit
// transaction: one is opened here for the Sync (or the end of a simple
// Query) to commit, or for the rollback should a message fail first or
// the connection end without a Sync. A statement that runs only outside
// a block (Session.RunsAlone) is not read ahead of: it runs alone, as
// the first Execute of a pipeline does in PostgreSQL, and the next
// Execute decides for the rest. The message read ahead is handled
// next by run. It returns the Execute, copied: reading the next message
// may reuse the one m points to.
func (c *conn) readAhead(ctx context.Context, m *pgproto3.Execute) *pgproto3.Execute {
	if c.session.State() != sql.StateIdle || c.peeked {
		return m
	}
	if pt, ok := c.portals[m.Portal]; ok && pt.res == nil && pt.stmt.stmt != nil && c.session.RunsAlone(ctx, pt.stmt.stmt) {
		// A statement that runs only outside a block runs on its own,
		// as the first of a pipeline does in PostgreSQL; the next
		// Execute opens the implicit transaction for the rest.
		return m
	}
	ex := *m
	c.peekedMsg, c.peekedErr = c.backend.Receive()
	c.peeked = true
	if _, sync := c.peekedMsg.(*pgproto3.Sync); !sync || c.peekedErr != nil {
		c.session.BeginImplicit()
	}
	return &ex
}

func rowDescription(cols []sql.ResultColumn) *pgproto3.RowDescription {
	rd := &pgproto3.RowDescription{}
	for _, col := range cols {
		typmod := int32(-1)
		if col.Typmod != 0 {
			typmod = col.Typmod
		}
		rd.Fields = append(rd.Fields, pgproto3.FieldDescription{
			Name:         []byte(col.Name),
			DataTypeOID:  colOID(col),
			DataTypeSize: colSize(col),
			TypeModifier: typmod,
			Format:       0,
		})
	}
	return rd
}

// sendDataRows emits rows; formats is per-column (nil = all text).
// reapPortals destroys every portal (end of implicit cycle or of the
// enclosing transaction).
func (c *conn) reapPortals() {
	for name, pt := range c.portals {
		pt.close(context.Background())
		delete(c.portals, name)
	}
}

func (c *conn) sendDataRows(res *sql.Result, formats []int16) {
	c.sendDataRowRange(res, formats, 0, len(res.Rows))
}

// sendDataRowRange sends rows [from, to) of a materialized result.
func (c *conn) sendDataRowRange(res *sql.Result, formats []int16, from, to int) {
	ts := c.textStyle()
	buf := c.rowBuf[:0]
	for _, row := range res.Rows[from:to] {
		var err error
		if buf, err = appendDataRow(buf, row, formats, res.Columns, ts); err != nil {
			// A row DataRow cannot carry: hand it to the backend the
			// way it always was, which records the encode error.
			if len(buf) > 0 {
				c.backend.Send(rawRows(buf))
				buf = buf[:0]
			}
			dr := c.rowMsg(len(row))
			for i, d := range row {
				format := int16(0)
				if formats != nil && i < len(formats) {
					format = formats[i]
				}
				var col sql.ResultColumn
				if i < len(res.Columns) {
					col = res.Columns[i]
				}
				dr.Values[i] = encodeDatumIn(d, format, col, ts)
			}
			c.backend.Send(dr)
			clear(dr.Values)
			continue
		}
		if len(buf) >= streamFlushBytes {
			c.backend.Send(rawRows(buf))
			buf = buf[:0]
		}
	}
	if len(buf) > 0 {
		c.backend.Send(rawRows(buf))
	}
	c.keepRowBuf(buf)
}

// keepRowBuf keeps buf (whose contents have been sent) for the next
// result's rows, unless one huge row grew it past what is worth
// holding on to between statements.
func (c *conn) keepRowBuf(buf []byte) {
	if cap(buf) > 4*streamFlushBytes {
		buf = nil
	}
	c.rowBuf = buf[:0]
}

// rowMsg is the connection's DataRow message, reused for every row it
// sends (Send and Encode copy the values out at once), with n values.
func (c *conn) rowMsg(n int) *pgproto3.DataRow {
	if cap(c.dataRow.Values) < n {
		c.dataRow.Values = make([][]byte, n)
	}
	c.dataRow.Values = c.dataRow.Values[:n]
	return &c.dataRow
}

// sendComplete sends CommandComplete with tag, through the connection's
// reused message (Send encodes it at once).
func (c *conn) sendComplete(tag string) {
	c.complete.CommandTag = append(c.complete.CommandTag[:0], tag...)
	c.backend.Send(&c.complete)
}

// encodeDatum renders a datum in the requested wire format.
// encodeDatum renders a value in the requested format under the
// described column: the column's width picks the binary integer size,
// and its TIMESTAMP / CHAR(n) modifiers the text rendering (what
// Describe promised, whatever hint the datum carries).
// encodeDatumIn is encodeDatum with the session's text settings: a
// TIMESTAMPTZ renders in its time zone, an INTERVAL in its IntervalStyle,
// a BYTEA in its bytea_output and a FLOAT8 at its extra_float_digits, in
// text format (the binary forms are the same whatever the settings).
func encodeDatumIn(d types.Datum, format int16, col sql.ResultColumn, ts textStyle) []byte {
	if format == 0 && !d.Null {
		switch {
		case d.Fam == types.Timestamp && !col.NoTZ && !d.NoTZ && ts.loc != nil && ts.loc != time.UTC:
			return []byte(types.FormatTimestampIn(d.I, ts.loc))
		case d.Fam == types.IntervalFam && ts.intervalStyle != "" && ts.intervalStyle != "postgres":
			return []byte(d.IntervalVal().Format(ts.intervalStyle))
		case d.Fam == types.Bytes && ts.byteaEscape:
			return []byte(types.FormatByteaEscape([]byte(d.S)))
		case d.Fam == types.Float && ts.extraFloatDigits <= 0:
			return []byte(types.FormatFloat8Digits(d.F, ts.extraFloatDigits))
		}
	}
	return encodeDatum(d, format, col)
}

// textStyle is the session's settings that change how a value renders
// in text format.
type textStyle struct {
	loc              *time.Location
	intervalStyle    string
	byteaEscape      bool
	extraFloatDigits int
}

func (c *conn) textStyle() textStyle {
	return textStyle{loc: c.session.TimeZone(), intervalStyle: c.session.IntervalStyle(), byteaEscape: c.session.ByteaEscape(),
		extraFloatDigits: c.session.ExtraFloatDigits()}
}

func encodeDatum(d types.Datum, format int16, col sql.ResultColumn) []byte {
	if d.Null {
		return nil
	}
	if d.Reg != "" && col.Type != types.Int {
		// A reg* value under a column described as its name.
		return []byte(d.Reg)
	}
	if d.Fam.IsArray() {
		if format == 0 {
			return []byte(d.Text())
		}
		return encodeBinaryArray(d, col)
	}
	if format == 0 {
		switch {
		case d.Fam == types.Timestamp && col.Type == types.Timestamp:
			return []byte(types.FormatTimestamp(d.I, col.NoTZ))
		case d.Fam == types.String && col.Type == types.String && !col.Char:
			return []byte(d.S)
		}
		return []byte(d.Text())
	}
	switch d.Fam {
	case types.Int:
		switch col.Width {
		case 2:
			return binary.BigEndian.AppendUint16(nil, uint16(int16(d.I)))
		case 4:
			return binary.BigEndian.AppendUint32(nil, uint32(int32(d.I)))
		}
		return binary.BigEndian.AppendUint64(nil, uint64(d.I))
	case types.Float:
		if math.IsNaN(d.F) {
			// PostgreSQL's NaN (get_float8_nan): Go's math.NaN() sets
			// the low mantissa bit as well.
			return binary.BigEndian.AppendUint64(nil, 0x7ff8000000000000)
		}
		return binary.BigEndian.AppendUint64(nil, math.Float64bits(d.F))
	case types.Bool:
		if d.B {
			return []byte{1}
		}
		return []byte{0}
	case types.Timestamp:
		// Binary timestamptz: microseconds since 2000-01-01.
		// A value with nanoseconds (one written before input rounded to
		// microseconds) rounds as its text form does.
		return binary.BigEndian.AppendUint64(nil, uint64(types.RoundMicros(d.I)/1000-pgEpochOffsetMicros))
	case types.Date:
		// Binary date: days since 2000-01-01, int32.
		return binary.BigEndian.AppendUint32(nil, uint32(int32(d.I-pgEpochOffsetDays)))
	case types.Bytes, types.Uuid:
		return []byte(d.S)
	case types.Decimal:
		// Real base-10000 NUMERIC: a text fallthrough under OID 1700 would
		// corrupt clients that requested binary. Text() (not S) so a
		// DECIMAL(p,s) column's declared scale becomes the wire dscale.
		if enc, err := encodePGNumeric(d.Text()); err == nil {
			return enc
		}
		return []byte(d.Text())
	case types.Jsonb:
		// Binary jsonb: version byte 1 + the UTF-8 text.
		return append([]byte{1}, d.S...)
	case types.Tsvector:
		// tsvectorsend's layout (pkg/sql/tsearch). A stored value always
		// parses; were one not to, its text is the best that can be sent.
		if v, err := tsearch.ParseVector(d.S); err == nil {
			return v.AppendBinary(nil)
		}
		return []byte(d.S)
	case types.Tsquery:
		if q, err := tsearch.ParseQuery(d.S); err == nil {
			return q.AppendBinary(nil)
		}
		return []byte(d.S)
	case types.IntervalFam:
		// Binary interval: microseconds (int64), days (int32), months (int32).
		out := binary.BigEndian.AppendUint64(nil, uint64(d.I/1000))
		out = binary.BigEndian.AppendUint32(out, uint32(int32(d.Dy)))
		return binary.BigEndian.AppendUint32(out, uint32(int32(d.Mo)))
	case types.Time:
		// Binary time: microseconds since midnight.
		return binary.BigEndian.AppendUint64(nil, uint64(d.I/1000))
	case types.Vector:
		return vector.EncodeBinary(d.S)
	default:
		return []byte(d.Text()) // an enum's binary form is its label
	}
}

// encodeBinaryArray renders an array in PostgreSQL's binary format:
// ndim, a has-null flag, the element OID, then per dimension the
// length and lower bound, then each element as a length-prefixed
// binary value (-1 for NULL).
func encodeBinaryArray(d types.Datum, col sql.ResultColumn) []byte {
	elem := col
	elem.Type = d.Fam.Elem()
	if col.Type.IsArray() {
		elem.Type = col.Type.Elem()
	}
	out := make([]byte, 0, 20+16*len(d.A))
	if len(d.A) == 0 {
		out = binary.BigEndian.AppendUint32(out, 0)
		out = binary.BigEndian.AppendUint32(out, 0)
		return binary.BigEndian.AppendUint32(out, colOID(elem))
	}
	hasNull := uint32(0)
	for _, e := range d.A {
		if e.Null {
			hasNull = 1
		}
	}
	out = binary.BigEndian.AppendUint32(out, 1)
	out = binary.BigEndian.AppendUint32(out, hasNull)
	out = binary.BigEndian.AppendUint32(out, colOID(elem))
	out = binary.BigEndian.AppendUint32(out, uint32(len(d.A)))
	out = binary.BigEndian.AppendUint32(out, 1)
	for _, e := range d.A {
		if e.Null {
			out = binary.BigEndian.AppendUint32(out, 0xffffffff)
			continue
		}
		v := e
		if !elem.Type.IsArray() && v.Fam != elem.Type {
			if c, err := v.Coerce(elem.Type); err == nil {
				v = c
			}
		}
		b := encodeDatum(v, 1, elem)
		out = binary.BigEndian.AppendUint32(out, uint32(len(b)))
		out = append(out, b...)
	}
	return out
}

// decodeBinaryArray reads PostgreSQL's binary array format into an
// array of fam's element family (one dimension; an empty array has
// none).
func decodeBinaryArray(raw []byte, fam types.Family) (types.Datum, error) {
	if len(raw) < 12 {
		return types.Datum{}, fmt.Errorf("bad binary array header")
	}
	ndim := binary.BigEndian.Uint32(raw[:4])
	raw = raw[12:]
	elem := fam.Elem()
	if ndim == 0 {
		return types.NewArray(elem, nil), nil
	}
	if ndim != 1 {
		return types.Datum{}, fmt.Errorf("multidimensional arrays are not supported")
	}
	if len(raw) < 8 {
		return types.Datum{}, fmt.Errorf("bad binary array dimensions")
	}
	n := binary.BigEndian.Uint32(raw[:4])
	raw = raw[8:]
	// Every element takes at least its 4-byte length, so a count the
	// rest of the value cannot hold is refused before it sizes anything:
	// a 20-byte value declaring 0xF0000000 elements used to ask the
	// runtime for hundreds of gigabytes and kill the process.
	if uint64(n) > uint64(len(raw)/4) {
		return types.Datum{}, &sql.Error{Code: sql.CodeInvalidBinaryRepresentation,
			Msg: fmt.Sprintf("binary array declares %d elements in %d bytes", n, len(raw))}
	}
	elems := make([]types.Datum, 0, n)
	for i := uint32(0); i < n; i++ {
		if len(raw) < 4 {
			return types.Datum{}, fmt.Errorf("bad binary array element %d", i)
		}
		size := int32(binary.BigEndian.Uint32(raw[:4]))
		raw = raw[4:]
		if size < 0 {
			elems = append(elems, types.DNull)
			continue
		}
		if len(raw) < int(size) {
			return types.Datum{}, fmt.Errorf("bad binary array element %d", i)
		}
		d, err := decodeBinaryParam(raw[:size], elem)
		if err != nil {
			return types.Datum{}, err
		}
		raw = raw[size:]
		elems = append(elems, d)
	}
	return types.NewArray(elem, elems), nil
}

// The range a binary datetime parameter may hold: what the text input
// accepts, refused outside with 22008 rather than wrapped. A timestamp
// is Unix nanoseconds in an int64, the years 1678 to 2261 (as
// types.ParseTimestamp refuses outside them); a date the years 0000 to
// 9999 that types.ParseDate reads. Timestamp bounds are microseconds
// from PostgreSQL's epoch (as sent), date bounds days from the Unix
// epoch (as stored).
var (
	minBinaryTimestamp = time.Date(1678, 1, 1, 0, 0, 0, 0, time.UTC).UnixMicro() - pgEpochOffsetMicros
	maxBinaryTimestamp = time.Date(2262, 1, 1, 0, 0, 0, 0, time.UTC).UnixMicro() - pgEpochOffsetMicros // exclusive
	minBinaryDate      = time.Date(0, 1, 1, 0, 0, 0, 0, time.UTC).Unix() / 86400
	maxBinaryDate      = time.Date(9999, 12, 31, 0, 0, 0, 0, time.UTC).Unix() / 86400
)

// datetimeOverflow is the 22008 (datetime_field_overflow) for a binary
// datetime parameter outside what datax represents. PostgreSQL encodes
// infinity as the type's extreme value; datax has no infinite datetimes
// (the text 'infinity' is refused too), so it is named in the message.
func datetimeOverflow(typ string, infinite bool) *sql.Error {
	msg := typ + " out of range"
	if infinite {
		msg = fmt.Sprintf("%s: infinite %s values are not supported", msg, typ)
	}
	return &sql.Error{Code: codeDatetimeOverflow, Msg: msg}
}

// codeDatetimeOverflow is PostgreSQL's datetime_field_overflow.
const codeDatetimeOverflow = "22008"

// binaryErrCode is the SQLSTATE for a binary decode failure: the
// decoder's own when it gave one (*sql.Error), def otherwise.
func binaryErrCode(err error, def string) string {
	var serr *sql.Error
	if errors.As(err, &serr) && serr.Code != "" {
		return serr.Code
	}
	return def
}

// decodeBinaryParam decodes a binary-format parameter of a known type.
func decodeBinaryParam(raw []byte, fam types.Family) (types.Datum, error) {
	if fam.IsArray() {
		return decodeBinaryArray(raw, fam)
	}
	switch fam {
	case types.Int:
		switch len(raw) {
		case 8:
			return types.NewInt(int64(binary.BigEndian.Uint64(raw))), nil
		case 4:
			return types.NewInt(int64(int32(binary.BigEndian.Uint32(raw)))), nil
		case 2:
			return types.NewInt(int64(int16(binary.BigEndian.Uint16(raw)))), nil
		}
		return types.Datum{}, fmt.Errorf("bad binary int length %d", len(raw))
	case types.Float:
		var f float64
		switch len(raw) {
		case 8:
			f = math.Float64frombits(binary.BigEndian.Uint64(raw))
		case 4:
			f = float64(math.Float32frombits(binary.BigEndian.Uint32(raw)))
		default:
			return types.Datum{}, fmt.Errorf("bad binary float length %d", len(raw))
		}
		if math.IsNaN(f) {
			// Every NaN is the one NaN, as in PostgreSQL (and as the
			// text form 'NaN' reads): equal to itself in comparisons,
			// grouping and EXCEPT whatever its payload bits.
			f = math.NaN()
		}
		return types.NewFloat(f), nil
	case types.Bool:
		if len(raw) == 1 {
			return types.NewBool(raw[0] != 0), nil
		}
		return types.Datum{}, fmt.Errorf("bad binary bool length %d", len(raw))
	case types.Timestamp:
		if len(raw) == 8 {
			pgMicros := int64(binary.BigEndian.Uint64(raw))
			if pgMicros < minBinaryTimestamp || pgMicros >= maxBinaryTimestamp {
				return types.Datum{}, datetimeOverflow("timestamp", pgMicros == math.MinInt64 || pgMicros == math.MaxInt64)
			}
			return types.NewTimestamp((pgMicros + pgEpochOffsetMicros) * 1000), nil
		}
		return types.Datum{}, fmt.Errorf("bad binary timestamptz length %d", len(raw))
	case types.Date:
		if len(raw) == 4 {
			pgDays := int32(binary.BigEndian.Uint32(raw))
			days := int64(pgDays) + pgEpochOffsetDays
			if days < minBinaryDate || days > maxBinaryDate {
				return types.Datum{}, datetimeOverflow("date", pgDays == math.MinInt32 || pgDays == math.MaxInt32)
			}
			return types.NewDate(days), nil
		}
		return types.Datum{}, fmt.Errorf("bad binary date length %d", len(raw))
	case types.Bytes:
		return types.NewBytes(raw), nil
	case types.Uuid:
		if len(raw) == 16 {
			var u [16]byte
			copy(u[:], raw)
			return types.NewUUID(u), nil
		}
		return types.Datum{}, fmt.Errorf("bad binary uuid length %d", len(raw))
	case types.Decimal:
		text, err := decodePGNumeric(raw)
		if err != nil {
			return types.Datum{}, err
		}
		return types.ParseDecimal(text)
	case types.Jsonb:
		if len(raw) < 1 || raw[0] != 1 {
			return types.Datum{}, fmt.Errorf("bad binary jsonb version")
		}
		return types.ParseJSONB(string(raw[1:]))
	case types.Tsvector:
		v, err := tsearch.ParseVectorBinary(raw)
		if err != nil {
			return types.Datum{}, err
		}
		return types.NewTsvector(v), nil
	case types.Tsquery:
		q, err := tsearch.ParseQueryBinary(raw)
		if err != nil {
			return types.Datum{}, err
		}
		return types.NewTsquery(q), nil
	case types.IntervalFam:
		if len(raw) != 16 {
			return types.Datum{}, fmt.Errorf("bad binary interval length %d", len(raw))
		}
		micros := int64(binary.BigEndian.Uint64(raw[:8]))
		if micros > math.MaxInt64/1000 || micros < math.MinInt64/1000 {
			return types.Datum{}, datetimeOverflow("interval", micros == math.MinInt64 || micros == math.MaxInt64)
		}
		return types.NewInterval(types.Interval{
			Nanos:  micros * 1000,
			Days:   int64(int32(binary.BigEndian.Uint32(raw[8:12]))),
			Months: int64(int32(binary.BigEndian.Uint32(raw[12:16]))),
		}), nil
	case types.Time:
		if len(raw) != 8 {
			return types.Datum{}, fmt.Errorf("bad binary time length %d", len(raw))
		}
		micros := int64(binary.BigEndian.Uint64(raw))
		if micros < 0 || micros > types.NanosPerDay/1000 {
			return types.Datum{}, datetimeOverflow("time", false)
		}
		return types.NewTime(micros * 1000), nil
	case types.Vector:
		stored, err := vector.DecodeBinary(raw)
		if err != nil {
			return types.Datum{}, err
		}
		return types.Datum{Fam: types.Vector, S: stored}, nil
	default:
		return types.NewString(string(raw)), nil // text: binary == raw bytes
	}
}

func (c *conn) extError(serr *sql.Error) {
	c.sendError(serr)
	c.skipToSync = true
}

func (c *conn) handleParse(ctx context.Context, m *pgproto3.Parse) {
	stmts, err := parser.Parse(m.Query)
	if err != nil {
		c.extError(sql.ToSQLError(err))
		return
	}
	p := &prepared{text: m.Query}
	switch len(stmts) {
	case 0:
	case 1:
		if _, ok := stmts[0].(*parser.CopyFrom); ok {
			// pgx's CopyFrom sends the COPY statement itself via a simple
			// Query; only its preliminary column-discovery SELECT arrives
			// here. Copy-in mode inside the extended protocol is
			// territory no client needs. (COPY TO runs in either
			// protocol: it is described as NoData, takes no parameters,
			// and its Execute answers in copy-out mode.)
			c.extError(&sql.Error{Code: sql.CodeFeatureNotSupported,
				Msg: "COPY FROM STDIN is only supported in the simple query protocol"})
			return
		}
		p.stmt = stmts[0]
		p.nParams = parser.CountParams(stmts[0])
		if c.act != nil {
			shape := parser.Fingerprint(stmts[0])
			p.shape = &shape
		}
		cols, serr := c.session.PlanColumns(ctx, stmts[0])
		if serr != nil {
			c.extError(serr)
			return
		}
		p.cols = cols
		fams, serr := c.session.PlanParams(ctx, stmts[0])
		if serr != nil {
			c.extError(serr)
			return
		}
		// A parameter the client declared a vector is one, wherever it
		// stands in the statement: its binary form is not text.
		for i, oid := range m.ParameterOIDs {
			if oid == oidVector && i < len(fams) && fams[i] == types.Unknown {
				fams[i] = types.Vector
			}
		}
		p.paramFams = fams
	default:
		c.extError(&sql.Error{Code: sql.CodeSyntaxError, Msg: "cannot insert multiple commands into a prepared statement"})
		return
	}
	c.stmts[m.Name] = p
	c.backend.Send(&pgproto3.ParseComplete{})
}

func (c *conn) handleBind(m *pgproto3.Bind) {
	p, ok := c.stmts[m.PreparedStatement]
	if !ok {
		c.extError(&sql.Error{Code: sql.CodeInvalidSQLStatementName, Msg: fmt.Sprintf("prepared statement %q does not exist", m.PreparedStatement)})
		return
	}
	if len(m.Parameters) != p.nParams {
		c.extError(&sql.Error{Code: sql.CodeInvalidParameter,
			Msg: fmt.Sprintf("bind supplies %d parameters, statement needs %d", len(m.Parameters), p.nParams)})
		return
	}
	// A format-code list holds no code (all text), one (for every item)
	// or one per item; pgproto3 decodes whatever count the client sent.
	if n := len(m.ParameterFormatCodes); n > 1 && n != len(m.Parameters) {
		c.extError(&sql.Error{Code: sql.CodeInvalidParameter,
			Msg: fmt.Sprintf("bind message has %d parameter formats but %d parameters", n, len(m.Parameters))})
		return
	}
	if n := len(m.ResultFormatCodes); n > 1 && n != len(p.cols) {
		c.extError(&sql.Error{Code: sql.CodeInvalidParameter,
			Msg: fmt.Sprintf("bind message has %d result formats but query has %d columns", n, len(p.cols))})
		return
	}
	paramFormat := func(i int) int16 {
		switch len(m.ParameterFormatCodes) {
		case 0:
			return 0
		case 1:
			return m.ParameterFormatCodes[0]
		default:
			return m.ParameterFormatCodes[i]
		}
	}
	params := make([]types.Datum, len(m.Parameters))
	for i, raw := range m.Parameters {
		if raw == nil {
			params[i] = types.DNull
			continue
		}
		fam := types.Unknown
		if i < len(p.paramFams) {
			fam = p.paramFams[i]
		}
		if paramFormat(i) == 0 {
			// Text parameters stay strings; execution coerces them to the
			// column type.
			params[i] = types.NewString(string(raw))
			continue
		}
		d, err := decodeBinaryParam(raw, fam)
		if err != nil {
			c.extError(&sql.Error{Code: binaryErrCode(err, sql.CodeInvalidBinaryRepresentation), Msg: fmt.Sprintf("parameter $%d: %v", i+1, err)})
			return
		}
		params[i] = d
	}
	nCols := len(p.cols)
	resFormats := make([]int16, nCols)
	switch len(m.ResultFormatCodes) {
	case 0:
	case 1:
		for i := range resFormats {
			resFormats[i] = m.ResultFormatCodes[0]
		}
	default:
		copy(resFormats, m.ResultFormatCodes)
	}
	if old, ok := c.portals[m.DestinationPortal]; ok {
		old.close(context.Background())
	}
	c.portals[m.DestinationPortal] = &portal{stmt: p, params: params, resFormats: resFormats}
	c.backend.Send(&pgproto3.BindComplete{})
}

func (c *conn) handleDescribe(m *pgproto3.Describe) {
	switch m.ObjectType {
	case 'S':
		p, ok := c.stmts[m.Name]
		if !ok {
			c.extError(&sql.Error{Code: sql.CodeInvalidSQLStatementName, Msg: fmt.Sprintf("prepared statement %q does not exist", m.Name)})
			return
		}
		pd := &pgproto3.ParameterDescription{}
		for i := 0; i < p.nParams; i++ {
			oid := uint32(oidText)
			if i < len(p.paramFams) && p.paramFams[i] != types.Unknown {
				oid = typeOID(p.paramFams[i])
			}
			pd.ParameterOIDs = append(pd.ParameterOIDs, oid)
		}
		c.backend.Send(pd)
		if p.cols != nil {
			c.backend.Send(rowDescription(p.cols))
		} else {
			c.backend.Send(&pgproto3.NoData{})
		}
	case 'P':
		pt, ok := c.portals[m.Name]
		if !ok {
			c.extError(&sql.Error{Code: sql.CodeInvalidCursorName, Msg: fmt.Sprintf("portal %q does not exist", m.Name)})
			return
		}
		if pt.stmt.cols != nil {
			// A portal's description carries the result formats its Bind
			// chose, which its rows are sent in (PostgreSQL's
			// SendRowDescriptionMessage with the portal's formats).
			rd := rowDescription(pt.stmt.cols)
			for i := range rd.Fields {
				if i < len(pt.resFormats) {
					rd.Fields[i].Format = pt.resFormats[i]
				}
			}
			c.backend.Send(rd)
		} else {
			c.backend.Send(&pgproto3.NoData{})
		}
	}
}

func (c *conn) handleExecute(ctx context.Context, m *pgproto3.Execute) {
	pt, ok := c.portals[m.Portal]
	if !ok {
		c.extError(&sql.Error{Code: sql.CodeInvalidCursorName, Msg: fmt.Sprintf("portal %q does not exist", m.Portal)})
		return
	}
	if pt.stmt.stmt == nil {
		c.backend.Send(&pgproto3.EmptyQueryResponse{})
		return
	}
	if pt.done {
		// A completed portal stays completed: further Executes return no
		// rows (re-running requires a re-Bind).
		c.sendComplete(pt.res.Tag)
		return
	}
	if pt.res == nil {
		var suspended bool
		_, serr := c.execute(ctx, pt.stmt.stmt, pt.stmt.text, pt.stmt.shape, pt.params, func(ctx context.Context, res *sql.Result) (int64, *sql.Error) {
			pt.res = res
			if res.Copy != nil {
				// COPY ... TO STDOUT runs to completion in one Execute
				// whatever its row limit, as in PostgreSQL.
				pt.done = true
				n, serr := c.sendCopyOut(ctx, res)
				if serr != nil {
					pt.close(context.Background())
				}
				return n, serr
			}
			if serr := resultMatchesDescribe(pt.stmt.cols, res); serr != nil {
				pt.done = true
				pt.close(context.Background())
				return 0, serr
			}
			if res.Stream == nil {
				return int64(len(res.Rows)), nil
			}
			// A streamed portal pulls its first page here, under the
			// statement's cancel registration; the rest on later Executes.
			n, more, serr := c.sendStream(ctx, res, pt.resFormats, int64(m.MaxRows))
			suspended = more
			if serr != nil {
				pt.done = true
				pt.close(context.Background())
			}
			return n, serr
		})
		if serr != nil {
			c.extError(serr)
			return
		}
		if pt.res.Copy != nil {
			c.sendComplete(pt.res.Tag)
			return
		}
		if pt.res.Stream != nil {
			c.finishStreamExecute(pt, suspended, nil)
			return
		}
	}
	if pt.res.Stream != nil {
		if pt.done {
			c.sendComplete(pt.res.Tag)
			return
		}
		if c.session.State() == sql.StateFailed {
			pt.done = true
			pt.close(context.Background())
			c.extError(&sql.Error{Code: sql.CodeInFailedTransaction, Msg: "current transaction is aborted, commands ignored until end of transaction block"})
			return
		}
		ctx, cancel := c.beginStatement(ctx)
		_, suspended, serr := c.sendStream(ctx, pt.res, pt.resFormats, int64(m.MaxRows))
		c.endStatement(cancel)
		c.finishStreamExecute(pt, suspended, serr)
		return
	}
	if pt.res.Columns == nil {
		// Row-less statements complete in one Execute regardless of limit.
		pt.done = true
		c.sendComplete(pt.res.Tag)
		return
	}
	end := len(pt.res.Rows)
	if m.MaxRows > 0 && pt.offset+int(m.MaxRows) < end {
		end = pt.offset + int(m.MaxRows)
	}
	c.sendDataRowRange(pt.res, pt.resFormats, pt.offset, end)
	pt.offset = end
	if pt.offset < len(pt.res.Rows) {
		c.backend.Send(&pgproto3.PortalSuspended{})
		return
	}
	pt.done = true
	c.sendComplete(pt.res.Tag)
}

// resultMatchesDescribe checks a result against the columns Describe
// reported for its statement: a client told NoData has no
// RowDescription to read rows with, and one told n columns cannot read
// rows of another width. PostgreSQL refuses the second case with the
// same error; the first is a statement whose result shape PlanColumns
// does not know, refused here rather than sent as rows the client
// cannot interpret.
func resultMatchesDescribe(described []sql.ResultColumn, res *sql.Result) *sql.Error {
	switch {
	case res.Columns == nil:
		// No rows: CommandComplete only, whatever Describe said.
		return nil
	case described == nil:
		return &sql.Error{Code: sql.CodeInternal, Msg: "statement returned rows but was described as returning none"}
	case len(described) != len(res.Columns):
		return &sql.Error{Code: sql.CodeFeatureNotSupported, Msg: "cached plan must not change result type"}
	}
	return nil
}

// finishStreamExecute ends one Execute on a streamed portal: an error
// (after the rows already sent), a suspension, or completion.
func (c *conn) finishStreamExecute(pt *portal, suspended bool, serr *sql.Error) {
	switch {
	case serr != nil:
		pt.done = true
		pt.close(context.Background())
		c.extError(serr)
	case suspended:
		c.backend.Send(&pgproto3.PortalSuspended{})
	default:
		pt.done = true
		c.sendComplete(pt.res.Tag)
	}
}

// streamFlushBytes is how many encoded row bytes accumulate before they
// are written to the client.
const streamFlushBytes = 64 << 10

// rawRows carries already-encoded DataRow messages through the backend's
// own buffer, so a result that never reached a flush goes out with the
// CommandComplete and ReadyForQuery behind it in one write, as a
// materialized result does.
type rawRows []byte

func (r rawRows) Backend()                          {}
func (r rawRows) Decode([]byte) error               { return errors.New("rawRows: not decodable") }
func (r rawRows) Encode(dst []byte) ([]byte, error) { return append(dst, r...), nil }

// sendStream pulls up to max rows (0 = all) from a streamed result and
// writes them as DataRows, flushing every streamFlushBytes. It reports
// the rows sent and whether the stream has more (a suspended portal).
// Rows are encoded into a private buffer so that a restart before the
// first flush can discard them; once anything is written the stream is
// told, and a later error is surfaced after the rows already sent. On
// completion the result's tag is the stream's.
func (c *conn) sendStream(ctx context.Context, res *sql.Result, formats []int16, max int64) (sent int64, more bool, serr *sql.Error) {
	st := res.Stream
	buf := c.rowBuf[:0]
	c.rowBuf = nil // buf is this call's until it hands it back
	defer func() { c.keepRowBuf(buf) }()
	ts := c.textStyle()
	flush := func() bool {
		if len(buf) == 0 {
			return true
		}
		// Anything queued on the backend (the RowDescription) goes first.
		if err := c.backend.Flush(); err != nil {
			c.ioErr = err
			return false
		}
		if _, err := c.nc.Write(buf); err != nil {
			c.ioErr = err
			return false
		}
		buf = buf[:0]
		st.Flushed()
		return true
	}
	// finish hands what remains to the backend's buffer: the stream is
	// over (no restart can follow), and the trailing messages go out
	// with it in one write.
	finish := func() {
		if len(buf) > 0 {
			c.backend.Send(rawRows(buf))
			buf = buf[:0]
		}
	}
	for max <= 0 || sent < max {
		row, ok, err := st.Next(ctx)
		if err != nil {
			if errors.Is(err, sql.ErrStreamRestarted) {
				buf, sent = buf[:0], 0
				continue
			}
			finish()
			return sent, false, sql.ToSQLError(err)
		}
		if !ok {
			res.Tag = st.Tag()
			finish()
			return sent, false, nil
		}
		buf, err = appendDataRow(buf, row, formats, res.Columns, ts)
		if err != nil {
			st.Close(context.Background())
			return sent, false, &sql.Error{Code: sql.CodeInternal, Msg: err.Error()}
		}
		sent++
		if len(buf) >= streamFlushBytes && !flush() {
			st.Close(context.Background())
			return sent, false, &sql.Error{Code: sql.CodeInternal, Msg: "client connection lost"}
		}
	}
	// Suspended: the rows go out with the PortalSuspended behind them,
	// and the stream may not restart from here on.
	finish()
	st.Flushed()
	return sent, true, nil
}

// startupSettings lists the settings a startup packet carries, in a
// fixed order: every parameter but the connection's own (user,
// database, replication, options), then each -c name=value (or
// --name=value) of options.
func startupSettings(params map[string]string) [][2]string {
	var out [][2]string
	for k, v := range params {
		switch strings.ToLower(k) {
		case "user", "database", "replication", "options":
			continue
		}
		out = append(out, [2]string{k, v})
	}
	sort.Slice(out, func(i, j int) bool { return out[i][0] < out[j][0] })
	fields := strings.Fields(params["options"])
	for i := 0; i < len(fields); i++ {
		f := fields[i]
		switch {
		case f == "-c" && i+1 < len(fields):
			i++
			f = fields[i]
		case strings.HasPrefix(f, "-c"):
			f = f[2:]
		case strings.HasPrefix(f, "--"):
			f = f[2:]
		default:
			continue
		}
		if name, value, ok := strings.Cut(f, "="); ok {
			out = append(out, [2]string{strings.ReplaceAll(name, "-", "_"), value})
		}
	}
	return out
}

// checkConnectionLimits refuses a session past its role's or its
// database's CONNECTION LIMIT (53300, PostgreSQL's too_many_connections),
// counting this node's sessions: the limits are per node. An admin is
// exempt, as PostgreSQL exempts superusers. As in PostgreSQL, two
// sessions starting at once may both get in at the limit.
func (c *conn) checkConnectionLimits(info sql.StartupInfo) *sql.Error {
	if c.act == nil {
		return nil
	}
	user, db := c.session.SessionUser(), c.session.Database()
	byUser, byDB := c.act.countSessions(c, user, db)
	switch {
	case info.Exempt:
	case info.RoleLimit >= 0 && byUser >= info.RoleLimit:
		return &sql.Error{Code: "53300", Msg: fmt.Sprintf("too many connections for role %q", user)}
	case info.DatabaseLimit >= 0 && byDB >= info.DatabaseLimit:
		return &sql.Error{Code: "53300", Msg: fmt.Sprintf("too many connections for database %q", db)}
	}
	return nil
}
