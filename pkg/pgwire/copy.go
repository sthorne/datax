package pgwire

import (
	"bufio"
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"strconv"
	"strings"

	"github.com/jackc/pgx/v5/pgproto3"

	"github.com/sthorne/datax/pkg/sql"
	"github.com/sthorne/datax/pkg/sql/catalog"
	"github.com/sthorne/datax/pkg/sql/parser"
	"github.com/sthorne/datax/pkg/sql/types"
)

// The pgwire half of COPY FROM STDIN: copy-in message-flow handling and
// the three data-format decoders (text, CSV, binary). Semantics (chunked
// commits, the shared insert pipeline) live in pkg/sql/copy.go.

// errCopyFail is the client's own abort (a CopyFail message).
type errCopyFail struct{ msg string }

func (e errCopyFail) Error() string { return "COPY terminated by client: " + e.msg }

// copyDataReader adapts the copy-in message stream to io.Reader: CopyData
// payloads are the byte stream, CopyDone is EOF, CopyFail surfaces as
// errCopyFail. Flush and Sync are ignored during copy-in (PostgreSQL
// rule); anything else is a protocol violation aborting the COPY. Clients
// chunk CopyData with no row alignment (pgx cuts at ~64 KiB mid-field),
// which is why the decoders all read through one bufio.Reader over this.
type copyDataReader struct {
	backend *pgproto3.Backend
	buf     []byte // unread remainder of scratch
	scratch []byte // owned copy of the latest CopyData payload
	done    bool
}

func (r *copyDataReader) Read(p []byte) (int, error) {
	for len(r.buf) == 0 {
		if r.done {
			return 0, io.EOF
		}
		msg, err := r.backend.Receive()
		if err != nil {
			r.done = true
			return 0, err
		}
		switch m := msg.(type) {
		case *pgproto3.CopyData:
			// pgproto3 aliases its receive buffer: copy before it is reused.
			r.scratch = append(r.scratch[:0], m.Data...)
			r.buf = r.scratch
		case *pgproto3.CopyDone:
			r.done = true
			return 0, io.EOF
		case *pgproto3.CopyFail:
			r.done = true
			return 0, errCopyFail{msg: m.Message}
		case *pgproto3.Flush, *pgproto3.Sync:
			// Ignored during copy-in.
		default:
			r.done = true
			return 0, fmt.Errorf("unexpected %T during COPY FROM STDIN", msg)
		}
	}
	n := copy(p, r.buf)
	r.buf = r.buf[n:]
	return n, nil
}

// drain silently consumes the rest of the copy stream after an error, so
// the connection returns to a clean message boundary (PostgreSQL discards
// the remaining data the same way).
func (r *copyDataReader) drain() {
	for !r.done {
		msg, err := r.backend.Receive()
		if err != nil {
			r.done = true
			return
		}
		switch msg.(type) {
		case *pgproto3.CopyDone, *pgproto3.CopyFail:
			r.done = true
		}
	}
}

// handleCopyIn drives one COPY table FROM STDIN exchange end to end. The
// caller (run's simple-query path) sends ReadyForQuery afterwards.
func (c *conn) handleCopyIn(ctx context.Context, cf *parser.CopyFrom) {
	ci, err := c.session.BeginCopy(ctx, cf)
	if err != nil {
		// A client that streamed CopyData optimistically (pgx) is drained
		// by run()'s silent-discard cases.
		c.sendError(sql.ToSQLError(err))
		return
	}
	cols := ci.Columns()
	opts := ci.Options()
	var code uint16
	if opts.Format == parser.CopyFormatBinary {
		code = 1
	}
	resp := &pgproto3.CopyInResponse{OverallFormat: byte(code), ColumnFormatCodes: make([]uint16, len(cols))}
	for i := range resp.ColumnFormatCodes {
		resp.ColumnFormatCodes[i] = code
	}
	c.backend.Send(resp)
	if err := c.backend.Flush(); err != nil {
		return
	}

	r := &copyDataReader{backend: c.backend}
	br := bufio.NewReaderSize(r, 64<<10)
	var derr error
	switch opts.Format {
	case parser.CopyFormatBinary:
		derr = copyBinaryLoop(ctx, ci, br, cols)
	case parser.CopyFormatCSV:
		derr = copyCSVLoop(ctx, ci, br, cols, opts)
	default:
		derr = copyTextLoop(ctx, ci, br, cols, opts)
	}
	if derr == nil {
		n, ferr := ci.Finish(ctx)
		if ferr != nil {
			c.sendError(sql.ToSQLError(ferr))
			return
		}
		if c.act != nil {
			c.act.copied(int64(n))
		}
		c.backend.Send(&pgproto3.CommandComplete{CommandTag: []byte(fmt.Sprintf("COPY %d", n))})
		return
	}
	ci.Abort()
	var serr *sql.Error
	var fail errCopyFail
	if errors.As(derr, &fail) {
		serr = &sql.Error{Code: sql.CodeQueryCanceled, Msg: fail.Error()}
	} else {
		serr = sql.ToSQLError(derr)
	}
	// A failed COPY leaves its committed chunks behind — say so.
	if n := ci.RowsCommitted(); n > 0 {
		serr = &sql.Error{Code: serr.Code, Msg: fmt.Sprintf("%s; %d rows already committed", serr.Msg, n)}
	}
	c.sendError(serr)
	r.drain()
}

// copyFieldDatum converts a text or CSV field to its column's type. An
// enum label stays text: the write path resolves it against the column's
// labels (as it does an INSERT's), which a bare Coerce cannot.
func copyFieldDatum(ci *sql.CopyIn, raw string, col catalog.Column) (types.Datum, error) {
	if col.Type == types.Enum {
		return types.NewString(raw), nil
	}
	return ci.Text(raw).Coerce(col.Type)
}

// rowFmtErr labels a format-level decode failure with the failing row.
func rowFmtErr(ci *sql.CopyIn, code string, format string, args ...any) error {
	return &sql.Error{Code: code, Msg: fmt.Sprintf("COPY row %d: %s", ci.RowsRead()+1, fmt.Sprintf(format, args...))}
}

// ---------------------------------------------------------------------------
// Text format.

func copyTextLoop(ctx context.Context, ci *sql.CopyIn, br *bufio.Reader, cols []catalog.Column, opts *sql.CopyOut) error {
	header := opts.Header
	for {
		line, err := readTextLine(br)
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}
		if header {
			// HEADER: the first line names the columns; skipped, or with
			// MATCH checked against them.
			header = false
			if opts.HeaderMatch {
				var names []*string
				for _, f := range splitTextLine(line, opts.Delim) {
					if f == opts.Null {
						names = append(names, nil)
						continue
					}
					raw, derr := decodeTextField(f)
					if derr != nil {
						return rowFmtErr(ci, sql.CodeBadCopyFormat, "%v", derr)
					}
					names = append(names, &raw)
				}
				if err := matchCopyHeader(names, cols, opts.Null); err != nil {
					return err
				}
			}
			continue
		}
		if line == `\.` {
			// End-of-data marker: ignore anything after it.
			discardToEOF(br)
			return nil
		}
		fields := splitTextLine(line, opts.Delim)
		vals := make([]types.Datum, len(fields))
		for i, f := range fields {
			if f == opts.Null {
				vals[i] = types.DNull
				continue
			}
			raw, derr := decodeTextField(f)
			if derr != nil {
				return rowFmtErr(ci, sql.CodeBadCopyFormat, "%v", derr)
			}
			if i < len(cols) {
				d, cerr := copyFieldDatum(ci, raw, cols[i])
				if cerr != nil {
					return rowFmtErr(ci, sql.CodeInvalidTextRepresentation, "column %q: %v", cols[i].Name, cerr)
				}
				vals[i] = d
			} else {
				vals[i] = types.NewString(raw) // arity error reported by AddRow
			}
		}
		if err := ci.AddRow(ctx, vals); err != nil {
			return err
		}
	}
}

// splitTextLine splits a text-format line at each delimiter that no
// backslash escapes, leaving every field as written (the NULL string is
// matched before de-escaping, as PostgreSQL does).
func splitTextLine(line string, delim byte) []string {
	var fields []string
	start := 0
	for i := 0; i < len(line); i++ {
		switch line[i] {
		case '\\':
			i++
		case delim:
			fields = append(fields, line[start:i])
			start = i + 1
		}
	}
	return append(fields, line[start:])
}

// matchCopyHeader checks a header line's fields (nil: the NULL string)
// against the columns, as HEADER MATCH does, with PostgreSQL's errors.
func matchCopyHeader(names []*string, cols []catalog.Column, null string) error {
	if len(names) != len(cols) {
		return &sql.Error{Code: sql.CodeBadCopyFormat,
			Msg: fmt.Sprintf("wrong number of fields in header line: got %d, expected %d", len(names), len(cols))}
	}
	for i, n := range names {
		if n == nil {
			return &sql.Error{Code: sql.CodeBadCopyFormat,
				Msg: fmt.Sprintf("column name mismatch in header line field %d: got null value (%q), expected %q", i+1, null, cols[i].Name)}
		}
		if *n != cols[i].Name {
			return &sql.Error{Code: sql.CodeBadCopyFormat,
				Msg: fmt.Sprintf("column name mismatch in header line field %d: got %q, expected %q", i+1, *n, cols[i].Name)}
		}
	}
	return nil
}

// readTextLine reads one \n-terminated line (tolerating \r\n), returning
// io.EOF only at a clean end of stream.
func readTextLine(br *bufio.Reader) (string, error) {
	line, err := br.ReadString('\n')
	if err == io.EOF {
		if line == "" {
			return "", io.EOF
		}
		// Final line without a trailing newline.
	} else if err != nil {
		return "", err
	} else {
		line = line[:len(line)-1]
	}
	line = strings.TrimSuffix(line, "\r")
	return line, nil
}

func discardToEOF(br *bufio.Reader) {
	_, _ = io.Copy(io.Discard, br)
}

// decodeTextField undoes PostgreSQL text-format escaping: \b \f \n \r \t
// \v \\, octal \NNN (1-3 digits), hex \xH[H]; a backslash before any
// other character yields that character (PG behavior). \N (NULL) is
// handled by the caller before unescaping.
func decodeTextField(s string) (string, error) {
	if !strings.ContainsRune(s, '\\') {
		return s, nil
	}
	var b strings.Builder
	b.Grow(len(s))
	for i := 0; i < len(s); i++ {
		ch := s[i]
		if ch != '\\' {
			b.WriteByte(ch)
			continue
		}
		i++
		if i >= len(s) {
			return "", fmt.Errorf("field ends with a lone backslash")
		}
		switch c := s[i]; c {
		case 'b':
			b.WriteByte('\b')
		case 'f':
			b.WriteByte('\f')
		case 'n':
			b.WriteByte('\n')
		case 'r':
			b.WriteByte('\r')
		case 't':
			b.WriteByte('\t')
		case 'v':
			b.WriteByte('\v')
		case 'x':
			j := i + 1
			for j < len(s) && j <= i+2 && isHexDigit(s[j]) {
				j++
			}
			if j == i+1 {
				b.WriteByte('x') // \x with no digits is a literal x
				continue
			}
			v, _ := strconv.ParseUint(s[i+1:j], 16, 8)
			b.WriteByte(byte(v))
			i = j - 1
		case '0', '1', '2', '3', '4', '5', '6', '7':
			j := i
			for j < len(s) && j < i+3 && s[j] >= '0' && s[j] <= '7' {
				j++
			}
			v, _ := strconv.ParseUint(s[i:j], 8, 16)
			b.WriteByte(byte(v)) // PG truncates >255 the same way
			i = j - 1
		default:
			b.WriteByte(c)
		}
	}
	return b.String(), nil
}

func isHexDigit(c byte) bool {
	return c >= '0' && c <= '9' || c >= 'a' && c <= 'f' || c >= 'A' && c <= 'F'
}

// ---------------------------------------------------------------------------
// CSV format.

// csvField carries quotedness: in PG CSV an UNQUOTED empty field is NULL
// while a quoted "" is the empty string — which is why encoding/csv
// (which erases quoting) cannot be used here.
type csvField struct {
	text   string
	quoted bool
}

func copyCSVLoop(ctx context.Context, ci *sql.CopyIn, br *bufio.Reader, cols []catalog.Column, opts *sql.CopyOut) error {
	header := opts.Header
	for {
		fields, err := readCSVRecord(br, opts.Delim, opts.Quote, opts.Escape)
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return rowFmtErr(ci, sql.CodeBadCopyFormat, "%v", err)
		}
		if header {
			header = false
			if opts.HeaderMatch {
				names := make([]*string, len(fields))
				for i := range fields {
					if fields[i].quoted || fields[i].text != opts.Null {
						names[i] = &fields[i].text
					}
				}
				if err := matchCopyHeader(names, cols, opts.Null); err != nil {
					return err
				}
			}
			continue
		}
		if len(fields) == 1 && !fields[0].quoted && fields[0].text == `\.` {
			discardToEOF(br)
			return nil
		}
		vals := make([]types.Datum, len(fields))
		for i, f := range fields {
			// An unquoted NULL string is NULL (FORCE_NOT_NULL: the
			// string); a quoted one is the string (FORCE_NULL: NULL).
			isNull := !f.quoted && f.text == opts.Null
			if i < len(opts.ForceNotNull) && opts.ForceNotNull[i] {
				isNull = false
			}
			if i < len(opts.ForceNull) && opts.ForceNull[i] && f.text == opts.Null {
				isNull = true
			}
			if isNull {
				vals[i] = types.DNull
				continue
			}
			if i < len(cols) {
				d, cerr := copyFieldDatum(ci, f.text, cols[i])
				if cerr != nil {
					return rowFmtErr(ci, sql.CodeInvalidTextRepresentation, "column %q: %v", cols[i].Name, cerr)
				}
				vals[i] = d
			} else {
				vals[i] = types.NewString(f.text)
			}
		}
		if err := ci.AddRow(ctx, vals); err != nil {
			return err
		}
	}
}

// readCSVRecord reads one CSV record, streaming, as PostgreSQL's CSV
// reader does: a quote starts a quoted section anywhere in a field,
// which may embed delimiters and newlines; inside it the escape
// character before the quote or itself stands for that character (the
// doubled quote, by default); records end at an unquoted newline.
// Returns io.EOF only at a clean end of stream.
func readCSVRecord(br *bufio.Reader, delim, quote, escape byte) ([]csvField, error) {
	var (
		fields   []csvField
		cur      strings.Builder
		quoted   bool // current field started with a quote
		inQuotes bool
		read     bool // any byte consumed for this record
	)
	endField := func() {
		fields = append(fields, csvField{text: cur.String(), quoted: quoted})
		cur.Reset()
		quoted = false
	}
	for {
		ch, err := br.ReadByte()
		if err == io.EOF {
			if !read {
				return nil, io.EOF
			}
			if inQuotes {
				return nil, fmt.Errorf("unterminated quoted CSV field")
			}
			endField()
			return fields, nil
		}
		if err != nil {
			return nil, err
		}
		read = true
		if inQuotes {
			if ch == escape {
				nxt, nerr := br.ReadByte()
				if nerr == nil && (nxt == escape || nxt == quote) {
					cur.WriteByte(nxt)
					continue
				}
				if nerr == nil {
					_ = br.UnreadByte()
				}
			}
			if ch == quote {
				inQuotes = false
				continue
			}
			cur.WriteByte(ch)
			continue
		}
		switch ch {
		case quote:
			quoted = true
			inQuotes = true
		case delim:
			endField()
		case '\n':
			endField()
			return fields, nil
		case '\r':
			nxt, nerr := br.ReadByte()
			if nerr == nil && nxt != '\n' {
				_ = br.UnreadByte()
				cur.WriteByte('\r')
				continue
			}
			endField()
			return fields, nil
		default:
			cur.WriteByte(ch)
		}
	}
}

// ---------------------------------------------------------------------------
// Binary format.

var copyBinarySig = []byte("PGCOPY\n\xff\r\n\x00")

func copyBinaryLoop(ctx context.Context, ci *sql.CopyIn, br *bufio.Reader, cols []catalog.Column) error {
	hdr := make([]byte, 11+4+4)
	if _, err := io.ReadFull(br, hdr); err != nil {
		return &sql.Error{Code: sql.CodeBadCopyFormat, Msg: fmt.Sprintf("reading COPY binary header: %v", err)}
	}
	if !bytes.Equal(hdr[:11], copyBinarySig) {
		return &sql.Error{Code: sql.CodeBadCopyFormat, Msg: "invalid COPY binary signature"}
	}
	flags := binary.BigEndian.Uint32(hdr[11:15])
	if flags&0xFFFF0000 != 0 {
		// Critical flag bits (16-31): the only one defined is OIDs, which
		// datax has no use for.
		return &sql.Error{Code: sql.CodeBadCopyFormat, Msg: fmt.Sprintf("unsupported COPY binary flags 0x%08x", flags)}
	}
	extLen := int64(int32(binary.BigEndian.Uint32(hdr[15:19])))
	if extLen < 0 {
		return &sql.Error{Code: sql.CodeBadCopyFormat, Msg: "negative COPY binary header extension"}
	}
	if _, err := io.CopyN(io.Discard, br, extLen); err != nil {
		return &sql.Error{Code: sql.CodeBadCopyFormat, Msg: fmt.Sprintf("reading COPY binary header extension: %v", err)}
	}

	var b2 [2]byte
	var b4 [4]byte
	for {
		if _, err := io.ReadFull(br, b2[:]); err != nil {
			if err == io.EOF {
				return nil // pgx omits the -1 trailer; EOF here is a clean end
			}
			return rowFmtErr(ci, sql.CodeBadCopyFormat, "reading tuple field count: %v", err)
		}
		cnt := int16(binary.BigEndian.Uint16(b2[:]))
		if cnt == -1 {
			discardToEOF(br) // the trailer; nothing may follow
			return nil
		}
		if int(cnt) != len(cols) {
			return rowFmtErr(ci, sql.CodeBadCopyFormat, "tuple has %d fields, expected %d", cnt, len(cols))
		}
		vals := make([]types.Datum, len(cols))
		for i := range cols {
			if _, err := io.ReadFull(br, b4[:]); err != nil {
				return rowFmtErr(ci, sql.CodeBadCopyFormat, "reading field length: %v", err)
			}
			flen := int32(binary.BigEndian.Uint32(b4[:]))
			if flen == -1 {
				vals[i] = types.DNull
				continue
			}
			if flen < 0 {
				return rowFmtErr(ci, sql.CodeBadCopyFormat, "negative field length %d", flen)
			}
			raw, err := readCopyField(br, int(flen))
			if err != nil {
				return rowFmtErr(ci, sql.CodeBadCopyFormat, "reading field data: %v", err)
			}
			d, derr := decodeBinaryParam(raw, cols[i].Type)
			if derr != nil {
				return rowFmtErr(ci, binaryErrCode(derr, sql.CodeInvalidTextRepresentation), "column %q: %v", cols[i].Name, derr)
			}
			vals[i] = d
		}
		if err := ci.AddRow(ctx, vals); err != nil {
			return err
		}
	}
}

// copyFieldChunk is the most a binary COPY field's buffer is sized
// to on the strength of its declared length alone.
const copyFieldChunk = 64 << 10

// readCopyField reads a binary COPY field of the declared length n. The
// length is the client's word: a field up to copyFieldChunk is read
// into a buffer of that size, a longer one into a buffer that grows
// with the bytes that actually arrive, so a 4-byte length declaring
// 2 GiB costs memory only as the data is sent.
func readCopyField(r io.Reader, n int) ([]byte, error) {
	if n <= copyFieldChunk {
		raw := make([]byte, n)
		if _, err := io.ReadFull(r, raw); err != nil {
			return nil, err
		}
		return raw, nil
	}
	var b bytes.Buffer
	b.Grow(copyFieldChunk)
	got, err := io.CopyN(&b, r, int64(n))
	if err != nil {
		if err == io.EOF {
			err = io.ErrUnexpectedEOF
		}
		return nil, fmt.Errorf("%w after %d of %d bytes", err, got, n)
	}
	return b.Bytes(), nil
}
