package pgwire

import (
	"context"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"strconv"
	"strings"

	"github.com/jackc/pgx/v5/pgproto3"

	"github.com/sthorne/datax/pkg/sql"
	"github.com/sthorne/datax/pkg/sql/types"
	"github.com/sthorne/datax/pkg/sql/vtable"
)

// The fast-path function call ('F'): one function, called by OID with
// binary arguments, answered with one value and no row description.
// PostgreSQL calls the interface obsolescent, but it is how libpq's
// large-object API works, and through libpq psql's \lo_import and
// \lo_export, psycopg's lobject and — with its own implementation of
// the same message — the JDBC driver's LargeObjectManager (issue #303).
// So the functions those clients call are served, and no others: any
// other function is one SELECT away.
//
// A call is run as the statement SELECT fn($1, ...) would be: same
// privileges, same transaction state, same accounting.

// fastPathFunc describes one callable function: what its arguments are
// (i = an integer, o = an object id, b = bytes) and how wide PostgreSQL declares its
// integer result (0 = it returns bytes), which is the width a client
// reads.
type fastPathFunc struct {
	args   string
	result int
}

var fastPathFuncs = map[string]fastPathFunc{
	"lo_creat":      {"i", 4},
	"lo_create":     {"o", 4},
	"lo_open":       {"oi", 4},
	"lo_close":      {"i", 4},
	"loread":        {"ii", 0},
	"lowrite":       {"ib", 4},
	"lo_lseek":      {"iii", 4},
	"lo_lseek64":    {"iii", 8},
	"lo_tell":       {"i", 4},
	"lo_tell64":     {"i", 8},
	"lo_truncate":   {"ii", 4},
	"lo_truncate64": {"ii", 4},
	"lo_unlink":     {"o", 4},
}

// handleFunctionCall serves one fast-path call. Like a simple query, it
// is always followed by ReadyForQuery, which the caller sends.
func (c *conn) handleFunctionCall(ctx context.Context, m *pgproto3.FunctionCall) {
	name, ok := vtable.ProcName(int64(m.Function))
	fn, known := fastPathFuncs[name]
	if !ok || !known {
		what := fmt.Sprintf("function with OID %d", m.Function)
		if ok {
			what = "function " + name
		}
		c.sendError(&sql.Error{Code: sql.CodeFeatureNotSupported, Msg: what + " cannot be called through the fast-path interface (only the large-object functions can); call it in a SELECT"})
		return
	}
	if len(m.Arguments) != len(fn.args) {
		c.sendError(&sql.Error{Code: sql.CodeInvalidParameter, Msg: fmt.Sprintf("function %s takes %d argument(s), the call passes %d", name, len(fn.args), len(m.Arguments))})
		return
	}
	params := make([]types.Datum, len(m.Arguments))
	holders := make([]string, len(m.Arguments))
	for i, raw := range m.Arguments {
		holders[i] = "$" + strconv.Itoa(i+1)
		binaryArg := false
		switch len(m.ArgFormatCodes) {
		case 0:
		case 1:
			binaryArg = m.ArgFormatCodes[0] == 1
		default:
			if i < len(m.ArgFormatCodes) {
				binaryArg = m.ArgFormatCodes[i] == 1
			}
		}
		d, err := fastPathArg(fn.args[i], raw, binaryArg)
		if err != nil {
			c.sendError(&sql.Error{Code: sql.CodeInvalidParameter, Msg: fmt.Sprintf("function %s, argument %d: %v", name, i+1, err)})
			return
		}
		params[i] = d
	}
	text := "SELECT " + name + "(" + strings.Join(holders, ", ") + ")"
	stmts, err := c.parse(text)
	if err != nil {
		c.sendError(sql.ToSQLError(err))
		return
	}
	var out types.Datum
	_, serr := c.execute(ctx, stmts[0], text, params, func(ctx context.Context, res *sql.Result) (int64, *sql.Error) {
		if len(res.Rows) != 1 || len(res.Rows[0]) != 1 {
			return 0, &sql.Error{Code: sql.CodeInternal, Msg: fmt.Sprintf("function %s returned %d rows", name, len(res.Rows))}
		}
		out = res.Rows[0][0]
		return 1, nil
	})
	if serr != nil {
		c.sendError(serr)
		return
	}
	resp := &pgproto3.FunctionCallResponse{}
	switch {
	case out.Null:
	case fn.result == 0 && m.ResultFormatCode == 1:
		resp.Result = []byte(out.S)
	case fn.result == 0:
		resp.Result = []byte(`\x` + hex.EncodeToString([]byte(out.S)))
	case m.ResultFormatCode != 1:
		resp.Result = []byte(strconv.FormatInt(out.I, 10))
	case fn.result == 8:
		resp.Result = binary.BigEndian.AppendUint64(nil, uint64(out.I))
	default:
		// Object ids are unsigned 32-bit and travel as the same four
		// bytes.
		resp.Result = binary.BigEndian.AppendUint32(nil, uint32(out.I))
	}
	if resp.Result == nil && !out.Null {
		resp.Result = []byte{} // an empty value, not NULL
	}
	c.backend.Send(resp)
}

// fastPathArg decodes one argument.
func fastPathArg(kind byte, raw []byte, binaryArg bool) (types.Datum, error) {
	if raw == nil {
		return types.DNull, nil
	}
	if kind == 'b' {
		if binaryArg {
			return types.NewBytes(raw), nil
		}
		return types.NewString(string(raw)), nil // bytea's text form; the function coerces it
	}
	if !binaryArg {
		n, err := strconv.ParseInt(string(raw), 10, 64)
		if err != nil {
			return types.Datum{}, fmt.Errorf("%q is not an integer", raw)
		}
		return types.NewInt(n), nil
	}
	switch len(raw) {
	case 4:
		// An object id is unsigned; everything else (a descriptor, an
		// offset, a length, a mode) is signed.
		if kind == 'o' {
			return types.NewInt(int64(binary.BigEndian.Uint32(raw))), nil
		}
		return types.NewInt(int64(int32(binary.BigEndian.Uint32(raw)))), nil
	case 8:
		return types.NewInt(int64(binary.BigEndian.Uint64(raw))), nil
	case 2:
		return types.NewInt(int64(int16(binary.BigEndian.Uint16(raw)))), nil
	}
	return types.Datum{}, fmt.Errorf("an integer of %d bytes", len(raw))
}
