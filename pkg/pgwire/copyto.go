package pgwire

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5/pgproto3"

	"github.com/sthorne/datax/pkg/sql"
	"github.com/sthorne/datax/pkg/sql/parser"
	"github.com/sthorne/datax/pkg/sql/types"
)

// The pgwire half of COPY ... TO STDOUT (issue #365): the copy-out
// message flow and the three output formats. The session runs the COPY
// as its query (pkg/sql/copyto.go) and marks the result with its format;
// the rows are sent here as CopyOutResponse, one CopyData per row, and
// CopyDone, byte for byte as PostgreSQL's copyto.c writes them. A value
// is first rendered the way a DataRow would carry it (encodeDatumIn:
// text with the session's TimeZone, IntervalStyle and bytea_output, or
// the binary send form) and then escaped or quoted for the format.

// copyOutEncoder writes a COPY TO's rows in its format.
type copyOutEncoder struct {
	co   *sql.CopyOut
	cols []sql.ResultColumn
	ts   textStyle
}

// copyBinaryHeader is the binary format's file header: the signature,
// the flags word (no OIDs) and an empty header extension.
var copyBinaryHeader = append(append([]byte{}, copyBinarySig...), 0, 0, 0, 0, 0, 0, 0, 0)

// beginCopyData starts a CopyData message in buf; endCopyData fills in
// its length once the payload is appended.
func beginCopyData(buf []byte) ([]byte, int) {
	at := len(buf)
	return append(buf, 'd', 0, 0, 0, 0), at
}

func endCopyData(buf []byte, at int) []byte {
	binary.BigEndian.PutUint32(buf[at+1:], uint32(len(buf)-at-1))
	return buf
}

// header appends what precedes the first row: the column-name line for
// text and CSV with HEADER, as its own CopyData. The binary file header
// is not a message of its own (PostgreSQL sends it in the first row's
// CopyData); row and trailer prefix it.
func (e *copyOutEncoder) header(buf []byte) []byte {
	if !e.co.Header || e.co.Format == parser.CopyFormatBinary {
		return buf
	}
	buf, at := beginCopyData(buf)
	for i, col := range e.cols {
		if i > 0 {
			buf = append(buf, e.co.Delim)
		}
		if e.co.Format == parser.CopyFormatCSV {
			buf = e.appendCSV(buf, []byte(col.Name), false)
		} else {
			buf = e.appendText(buf, []byte(col.Name))
		}
	}
	buf = append(buf, '\n')
	return endCopyData(buf, at)
}

// row appends one row as a CopyData message; first marks the first row
// of a binary COPY, which carries the file header.
func (e *copyOutEncoder) row(buf []byte, row []types.Datum, first bool) []byte {
	buf, at := beginCopyData(buf)
	if e.co.Format == parser.CopyFormatBinary {
		if first {
			buf = append(buf, copyBinaryHeader...)
		}
		buf = binary.BigEndian.AppendUint16(buf, uint16(len(row)))
		for i, d := range row {
			if d.Null {
				buf = binary.BigEndian.AppendUint32(buf, 0xffffffff)
				continue
			}
			v := encodeDatumIn(d, 1, e.col(i), e.ts)
			buf = binary.BigEndian.AppendUint32(buf, uint32(len(v)))
			buf = append(buf, v...)
		}
		return endCopyData(buf, at)
	}
	for i, d := range row {
		if i > 0 {
			buf = append(buf, e.co.Delim)
		}
		if d.Null {
			buf = append(buf, e.co.Null...)
			continue
		}
		v := encodeDatumIn(d, 0, e.col(i), e.ts)
		if e.co.Format == parser.CopyFormatCSV {
			force := e.co.ForceQuoteAll || (i < len(e.co.ForceQuote) && e.co.ForceQuote[i])
			buf = e.appendCSV(buf, v, force)
		} else {
			buf = e.appendText(buf, v)
		}
	}
	buf = append(buf, '\n')
	return endCopyData(buf, at)
}

// trailer appends the binary format's end-of-data marker (with the file
// header when there were no rows); text and CSV have none.
func (e *copyOutEncoder) trailer(buf []byte, rows int64) []byte {
	if e.co.Format != parser.CopyFormatBinary {
		return buf
	}
	buf, at := beginCopyData(buf)
	if rows == 0 {
		buf = append(buf, copyBinaryHeader...)
	}
	buf = append(buf, 0xff, 0xff)
	return endCopyData(buf, at)
}

func (e *copyOutEncoder) col(i int) sql.ResultColumn {
	if i < len(e.cols) {
		return e.cols[i]
	}
	return sql.ResultColumn{}
}

// appendText escapes a value for the text format (CopyAttributeOutText):
// a backslash, the delimiter and the control characters \b \f \n \r \t
// \v are backslash-escaped (the control characters by their letter);
// every other byte is written as is.
func (e *copyOutEncoder) appendText(buf, v []byte) []byte {
	delim := e.co.Delim
	for _, c := range v {
		if c < 0x20 {
			var esc byte
			switch c {
			case '\b':
				esc = 'b'
			case '\f':
				esc = 'f'
			case '\n':
				esc = 'n'
			case '\r':
				esc = 'r'
			case '\t':
				esc = 't'
			case '\v':
				esc = 'v'
			default:
				if c == delim {
					esc = c
				}
			}
			if esc != 0 {
				buf = append(buf, '\\', esc)
			} else {
				buf = append(buf, c)
			}
			continue
		}
		if c == '\\' || c == delim {
			buf = append(buf, '\\')
		}
		buf = append(buf, c)
	}
	return buf
}

// appendCSV writes a value for the CSV format (CopyAttributeOutCSV): it
// is quoted when forced, when it equals the NULL string (so that it reads
// back as a value), when it is \. alone on a line of one column (so it is
// not taken for the end-of-data marker), or when it holds the delimiter,
// the quote, a newline or a carriage return; inside quotes the quote and
// escape characters are preceded by the escape character.
func (e *copyOutEncoder) appendCSV(buf, v []byte, force bool) []byte {
	co := e.co
	quote := force || string(v) == co.Null
	if !quote {
		if len(e.cols) == 1 && string(v) == `\.` {
			quote = true
		} else {
			for _, c := range v {
				if c == co.Delim || c == co.Quote || c == '\n' || c == '\r' {
					quote = true
					break
				}
			}
		}
	}
	if !quote {
		return append(buf, v...)
	}
	buf = append(buf, co.Quote)
	for _, c := range v {
		if c == co.Quote || c == co.Escape {
			buf = append(buf, co.Escape)
		}
		buf = append(buf, c)
	}
	return append(buf, co.Quote)
}

// sendCopyOut sends a COPY TO's result: CopyOutResponse, the rows as
// CopyData, CopyDone. Rows are encoded into a private buffer flushed
// every streamFlushBytes, as sendStream does, so a streamed result
// holds one page of rows and one buffer however large the table is,
// and a restart before the first flush starts the output over. An error
// after rows were sent is returned for the caller to report (with no
// CopyDone, as PostgreSQL ends a failed copy-out). On success the
// result's tag is COPY n.
func (c *conn) sendCopyOut(ctx context.Context, res *sql.Result) (int64, *sql.Error) {
	co := res.Copy
	var code uint16
	if co.Format == parser.CopyFormatBinary {
		code = 1
	}
	resp := &pgproto3.CopyOutResponse{OverallFormat: byte(code), ColumnFormatCodes: make([]uint16, len(res.Columns))}
	for i := range resp.ColumnFormatCodes {
		resp.ColumnFormatCodes[i] = code
	}
	c.backend.Send(resp)

	enc := &copyOutEncoder{co: co, cols: res.Columns, ts: c.textStyle()}
	st := res.Stream
	buf := enc.header(nil)
	var sent int64
	flush := func() bool {
		if err := c.backend.Flush(); err != nil {
			c.ioErr = err
			return false
		}
		if _, err := c.nc.Write(buf); err != nil {
			c.ioErr = err
			return false
		}
		buf = buf[:0]
		if st != nil {
			st.Flushed()
		}
		return true
	}
	finish := func() {
		if len(buf) > 0 {
			c.backend.Send(rawRows(buf))
			buf = nil
		}
	}
	lost := func() *sql.Error {
		if st != nil {
			st.Close(context.Background())
		}
		return &sql.Error{Code: sql.CodeInternal, Msg: "client connection lost"}
	}
	for i := 0; ; i++ {
		var row []types.Datum
		if st == nil {
			if i >= len(res.Rows) {
				break
			}
			row = res.Rows[i]
		} else {
			r, ok, err := st.Next(ctx)
			if err != nil {
				if errors.Is(err, sql.ErrStreamRestarted) {
					buf, sent = enc.header(buf[:0]), 0
					continue
				}
				finish()
				return sent, sql.ToSQLError(err)
			}
			if !ok {
				break
			}
			row = r
		}
		buf = enc.row(buf, row, sent == 0)
		sent++
		if len(buf) >= streamFlushBytes && !flush() {
			return sent, lost()
		}
	}
	buf = enc.trailer(buf, sent)
	finish()
	c.backend.Send(&pgproto3.CopyDone{})
	res.Tag = fmt.Sprintf("COPY %d", sent)
	return sent, nil
}
