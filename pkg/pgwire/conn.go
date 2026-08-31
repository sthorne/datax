package pgwire

import (
	"context"
	"crypto/tls"
	"encoding/binary"
	"fmt"
	"math"
	"net"

	"github.com/jackc/pgx/v5/pgproto3"

	"github.com/sthorne/datax/pkg/kvclient"
	"github.com/sthorne/datax/pkg/sql"
	"github.com/sthorne/datax/pkg/sql/catalog"
	"github.com/sthorne/datax/pkg/sql/parser"
	"github.com/sthorne/datax/pkg/sql/types"
)

// PostgreSQL type OIDs for datax's SQL types.
const (
	oidBool        = 16
	oidBytea       = 17
	oidInt8        = 20
	oidText        = 25
	oidFloat8      = 701
	oidDate        = 1082
	oidTimestamptz = 1184
	oidUUID        = 2950
	oidNumeric     = 1700
	oidJsonb       = 3802
)

// pgEpochOffsetMicros converts between the Unix epoch and PostgreSQL's
// binary-format epoch (2000-01-01).
const (
	pgEpochOffsetMicros = int64(946684800) * 1e6
	pgEpochOffsetDays   = int64(10957)
)

func typeOID(f types.Family) uint32 {
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
	default:
		return oidText
	}
}

func typeSize(f types.Family) int16 {
	switch f {
	case types.Int, types.Float, types.Timestamp:
		return 8
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
	res    *sql.Result
	offset int
	done   bool
}

type conn struct {
	backend *pgproto3.Backend
	nc      net.Conn
	session *sql.Session
	opts    ServerOptions
	tlsDone bool

	stmts   map[string]*prepared
	portals map[string]*portal
	// skipToSync: an extended-protocol error occurred; ignore messages
	// until Sync (PostgreSQL protocol rule).
	skipToSync bool
}

func newConn(nc net.Conn, db *kvclient.DB, cat *catalog.Accessor, opts ServerOptions) *conn {
	return &conn{
		backend: pgproto3.NewBackend(nc, nc),
		nc:      nc,
		session: sql.NewSession(db, cat),
		opts:    opts,
		stmts:   make(map[string]*prepared),
		portals: make(map[string]*portal),
	}
}

func (c *conn) run(ctx context.Context) error {
	defer c.session.Close(context.Background())
	if err := c.handleStartup(); err != nil {
		return err
	}
	for {
		msg, err := c.backend.Receive()
		if err != nil {
			return err
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
				c.handleExecute(ctx, m)
			}
		case *pgproto3.Close:
			if !c.skipToSync {
				switch m.ObjectType {
				case 'S':
					delete(c.stmts, m.Name)
				case 'P':
					delete(c.portals, m.Name)
				}
				c.backend.Send(&pgproto3.CloseComplete{})
			}
		case *pgproto3.Sync:
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

func (c *conn) handleStartup() error {
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
				c.backend = pgproto3.NewBackend(tc, tc)
				c.tlsDone = true
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
					c.backend.Send(&pgproto3.AuthenticationOk{})
				} else if err := c.authenticateSCRAM(user); err != nil {
					return err
				}
			} else {
				c.backend.Send(&pgproto3.AuthenticationOk{}) // trust auth
			}
			// Privileges are enforced against this identity. In trust mode
			// it is client-claimed (nothing verified it) — documented.
			c.session.SetUser(m.Parameters["user"])
			for _, kv := range [][2]string{
				{"server_version", "13.0 datax"},
				{"server_encoding", "UTF8"},
				{"client_encoding", "UTF8"},
				{"DateStyle", "ISO"},
				{"integer_datetimes", "on"},
				{"standard_conforming_strings", "on"},
				{"TimeZone", "UTC"},
			} {
				c.backend.Send(&pgproto3.ParameterStatus{Name: kv[0], Value: kv[1]})
			}
			c.backend.Send(&pgproto3.BackendKeyData{ProcessID: 0, SecretKey: []byte{0, 0, 0, 0}})
			c.sendReady()
			return c.backend.Flush()
		case *pgproto3.CancelRequest:
			return nil // out-of-band cancel: not supported, just close
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
	c.backend.Send(&pgproto3.ReadyForQuery{TxStatus: status})
}

func (c *conn) sendError(serr *sql.Error) {
	c.backend.Send(&pgproto3.ErrorResponse{
		Severity:            "ERROR",
		SeverityUnlocalized: "ERROR",
		Code:                serr.Code,
		Message:             serr.Msg,
	})
}

// handleSimpleQuery runs a (possibly multi-statement) simple query.
func (c *conn) handleSimpleQuery(ctx context.Context, q string) {
	stmts, err := parser.Parse(q)
	if err != nil {
		c.sendError(sql.ToSQLError(err))
		return
	}
	if len(stmts) == 0 {
		c.backend.Send(&pgproto3.EmptyQueryResponse{})
		return
	}
	for i, stmt := range stmts {
		if cf, ok := stmt.(*parser.CopyFrom); ok {
			// COPY takes over the message stream, so it may only end a
			// simple query — running statements after copy mode would need
			// a continuation machine no client exercises.
			if i != len(stmts)-1 {
				c.sendError(&sql.Error{Code: sql.CodeFeatureNotSupported,
					Msg: "COPY FROM STDIN must be the last statement of a simple query"})
				return
			}
			c.handleCopyIn(ctx, cf)
			return
		}
		res, serr := c.session.Execute(ctx, stmt, nil)
		if serr != nil {
			c.sendError(serr)
			return
		}
		if res.Columns != nil {
			c.backend.Send(rowDescription(res.Columns))
			c.sendDataRows(res, nil)
		}
		c.backend.Send(&pgproto3.CommandComplete{CommandTag: []byte(res.Tag)})
	}
}

func rowDescription(cols []sql.ResultColumn) *pgproto3.RowDescription {
	rd := &pgproto3.RowDescription{}
	for _, col := range cols {
		rd.Fields = append(rd.Fields, pgproto3.FieldDescription{
			Name:         []byte(col.Name),
			DataTypeOID:  typeOID(col.Type),
			DataTypeSize: typeSize(col.Type),
			TypeModifier: -1,
			Format:       0,
		})
	}
	return rd
}

// sendDataRows emits rows; formats is per-column (nil = all text).
// reapPortals destroys every portal (end of implicit cycle or of the
// enclosing transaction).
func (c *conn) reapPortals() {
	for name := range c.portals {
		delete(c.portals, name)
	}
}

func (c *conn) sendDataRows(res *sql.Result, formats []int16) {
	c.sendDataRowRange(res, formats, 0, len(res.Rows))
}

// sendDataRowRange sends rows [from, to) of a materialized result.
func (c *conn) sendDataRowRange(res *sql.Result, formats []int16, from, to int) {
	for _, row := range res.Rows[from:to] {
		dr := &pgproto3.DataRow{Values: make([][]byte, len(row))}
		for i, d := range row {
			format := int16(0)
			if formats != nil && i < len(formats) {
				format = formats[i]
			}
			dr.Values[i] = encodeDatum(d, format)
		}
		c.backend.Send(dr)
	}
}

// encodeDatum renders a datum in the requested wire format.
func encodeDatum(d types.Datum, format int16) []byte {
	if d.Null {
		return nil
	}
	if format == 0 {
		return []byte(d.Text())
	}
	switch d.Fam {
	case types.Int:
		return binary.BigEndian.AppendUint64(nil, uint64(d.I))
	case types.Float:
		return binary.BigEndian.AppendUint64(nil, math.Float64bits(d.F))
	case types.Bool:
		if d.B {
			return []byte{1}
		}
		return []byte{0}
	case types.Timestamp:
		// Binary timestamptz: microseconds since 2000-01-01.
		return binary.BigEndian.AppendUint64(nil, uint64(d.I/1000-pgEpochOffsetMicros))
	case types.Date:
		// Binary date: days since 2000-01-01, int32.
		return binary.BigEndian.AppendUint32(nil, uint32(int32(d.I-pgEpochOffsetDays)))
	case types.Bytes, types.Uuid:
		return []byte(d.S)
	case types.Decimal:
		// Real base-10000 NUMERIC: a text fallthrough under OID 1700 would
		// corrupt clients that requested binary.
		if enc, err := encodePGNumeric(d.S); err == nil {
			return enc
		}
		return []byte(d.Text())
	case types.Jsonb:
		// Binary jsonb: version byte 1 + the UTF-8 text.
		return append([]byte{1}, d.S...)
	default:
		return []byte(d.Text())
	}
}

// decodeBinaryParam decodes a binary-format parameter of a known type.
func decodeBinaryParam(raw []byte, fam types.Family) (types.Datum, error) {
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
		if len(raw) == 8 {
			return types.NewFloat(math.Float64frombits(binary.BigEndian.Uint64(raw))), nil
		}
		if len(raw) == 4 {
			return types.NewFloat(float64(math.Float32frombits(binary.BigEndian.Uint32(raw)))), nil
		}
		return types.Datum{}, fmt.Errorf("bad binary float length %d", len(raw))
	case types.Bool:
		if len(raw) == 1 {
			return types.NewBool(raw[0] != 0), nil
		}
		return types.Datum{}, fmt.Errorf("bad binary bool length %d", len(raw))
	case types.Timestamp:
		if len(raw) == 8 {
			micros := int64(binary.BigEndian.Uint64(raw)) + pgEpochOffsetMicros
			return types.NewTimestamp(micros * 1000), nil
		}
		return types.Datum{}, fmt.Errorf("bad binary timestamptz length %d", len(raw))
	case types.Date:
		if len(raw) == 4 {
			return types.NewDate(int64(int32(binary.BigEndian.Uint32(raw))) + pgEpochOffsetDays), nil
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
			// here. Copy mode inside the extended protocol is undefined
			// territory no client needs.
			c.extError(&sql.Error{Code: sql.CodeFeatureNotSupported,
				Msg: "COPY FROM STDIN is only supported in the simple query protocol"})
			return
		}
		p.stmt = stmts[0]
		p.nParams = parser.CountParams(stmts[0])
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
		c.extError(&sql.Error{Code: sql.CodeInternal, Msg: fmt.Sprintf("unknown prepared statement %q", m.PreparedStatement)})
		return
	}
	if len(m.Parameters) != p.nParams {
		c.extError(&sql.Error{Code: sql.CodeInvalidParameter,
			Msg: fmt.Sprintf("bind supplies %d parameters, statement needs %d", len(m.Parameters), p.nParams)})
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
			c.extError(&sql.Error{Code: sql.CodeInvalidParameter, Msg: fmt.Sprintf("parameter $%d: %v", i+1, err)})
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
	c.portals[m.DestinationPortal] = &portal{stmt: p, params: params, resFormats: resFormats}
	c.backend.Send(&pgproto3.BindComplete{})
}

func (c *conn) handleDescribe(m *pgproto3.Describe) {
	switch m.ObjectType {
	case 'S':
		p, ok := c.stmts[m.Name]
		if !ok {
			c.extError(&sql.Error{Code: sql.CodeInternal, Msg: fmt.Sprintf("unknown prepared statement %q", m.Name)})
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
			c.extError(&sql.Error{Code: sql.CodeInternal, Msg: fmt.Sprintf("unknown portal %q", m.Name)})
			return
		}
		if pt.stmt.cols != nil {
			c.backend.Send(rowDescription(pt.stmt.cols))
		} else {
			c.backend.Send(&pgproto3.NoData{})
		}
	}
}

func (c *conn) handleExecute(ctx context.Context, m *pgproto3.Execute) {
	pt, ok := c.portals[m.Portal]
	if !ok {
		c.extError(&sql.Error{Code: sql.CodeInternal, Msg: fmt.Sprintf("unknown portal %q", m.Portal)})
		return
	}
	if pt.stmt.stmt == nil {
		c.backend.Send(&pgproto3.EmptyQueryResponse{})
		return
	}
	if pt.done {
		// A completed portal stays completed: further Executes return no
		// rows (re-running requires a re-Bind).
		c.backend.Send(&pgproto3.CommandComplete{CommandTag: []byte(pt.res.Tag)})
		return
	}
	if pt.res == nil {
		res, serr := c.session.Execute(ctx, pt.stmt.stmt, pt.params)
		if serr != nil {
			c.extError(serr)
			return
		}
		pt.res = res
	}
	if pt.res.Columns == nil {
		// Row-less statements complete in one Execute regardless of limit.
		pt.done = true
		c.backend.Send(&pgproto3.CommandComplete{CommandTag: []byte(pt.res.Tag)})
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
	c.backend.Send(&pgproto3.CommandComplete{CommandTag: []byte(pt.res.Tag)})
}
