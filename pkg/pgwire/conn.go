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
	oidBool   = 16
	oidInt8   = 20
	oidText   = 25
	oidFloat8 = 701
)

func typeOID(f types.Family) uint32 {
	switch f {
	case types.Int:
		return oidInt8
	case types.Float:
		return oidFloat8
	case types.Bool:
		return oidBool
	default:
		return oidText
	}
}

func typeSize(f types.Family) int16 {
	switch f {
	case types.Int, types.Float:
		return 8
	case types.Bool:
		return 1
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
				if err := c.authenticateSCRAM(m.Parameters["user"]); err != nil {
					return err
				}
			} else {
				c.backend.Send(&pgproto3.AuthenticationOk{}) // trust auth
			}
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
	for _, stmt := range stmts {
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
func (c *conn) sendDataRows(res *sql.Result, formats []int16) {
	for _, row := range res.Rows {
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
	res, serr := c.session.Execute(ctx, pt.stmt.stmt, pt.params)
	if serr != nil {
		c.extError(serr)
		return
	}
	if res.Columns != nil {
		c.sendDataRows(res, pt.resFormats)
	}
	c.backend.Send(&pgproto3.CommandComplete{CommandTag: []byte(res.Tag)})
}
