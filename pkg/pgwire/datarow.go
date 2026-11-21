package pgwire

import (
	"encoding/binary"
	"errors"
	"math"
	"strconv"

	"github.com/sthorne/datax/pkg/sql"
	"github.com/sthorne/datax/pkg/sql/types"
)

// appendDataRow appends one DataRow message for row to buf: what
// encodeDatumIn per value and pgproto3.DataRow.Encode produce, byte for
// byte, without the intermediate []byte per value. The common values
// (NULL, integers, text, bytea and uuid) are written straight into buf;
// every other value goes through encodeDatumIn. The errors are
// DataRow.Encode's; on one, buf is returned as it was.
func appendDataRow(buf []byte, row []types.Datum, formats []int16, cols []sql.ResultColumn, ts textStyle) ([]byte, error) {
	if len(row) > math.MaxUint16 {
		return buf, errTooManyValues
	}
	start := len(buf)
	buf = append(buf, 'D', 0, 0, 0, 0)
	buf = binary.BigEndian.AppendUint16(buf, uint16(len(row)))
	for i := range row {
		d := &row[i]
		if d.Null {
			buf = append(buf, 0xff, 0xff, 0xff, 0xff)
			continue
		}
		format := int16(0)
		if formats != nil && i < len(formats) {
			format = formats[i]
		}
		var col *sql.ResultColumn
		if i < len(cols) {
			col = &cols[i]
		}
		at := len(buf)
		buf = append(buf, 0, 0, 0, 0)
		var ok bool
		if buf, ok = appendDatumFast(buf, d, format, col); !ok {
			var c sql.ResultColumn
			if col != nil {
				c = *col
			}
			v := encodeDatumIn(*d, format, c, ts)
			if v == nil {
				buf = append(buf[:at], 0xff, 0xff, 0xff, 0xff)
				continue
			}
			buf = append(buf, v...)
		}
		binary.BigEndian.PutUint32(buf[at:], uint32(len(buf)-at-4))
	}
	n := len(buf) - start - 1
	if n > maxMessageBodyLen {
		return buf[:start], errMessageTooLarge
	}
	binary.BigEndian.PutUint32(buf[start+1:], uint32(n))
	return buf, nil
}

// DataRow.Encode's limits and errors (pgproto3).
const maxMessageBodyLen = 0x3fffffff - 1

var (
	errTooManyValues   = errors.New("too many values")
	errMessageTooLarge = errors.New("message body too large")
)

// appendDatumFast appends the wire form of a non-NULL d when it is one
// of the shapes it knows, mirroring encodeDatumIn and encodeDatum for
// them exactly; ok=false leaves buf alone and the caller falls back to
// encodeDatumIn. None of the shapes depends on the session's text
// style (encodeDatumIn's session-dependent cases are timestamps,
// intervals, bytea in text and floats).
func appendDatumFast(buf []byte, d *types.Datum, format int16, col *sql.ResultColumn) ([]byte, bool) {
	if d.Reg != "" || d.Pad != 0 {
		return buf, false
	}
	var colType types.Family
	var width int32
	var char bool
	if col != nil {
		colType, width, char = col.Type, col.Width, col.Char
	}
	switch format {
	case 0:
		switch d.Fam {
		case types.Int:
			return strconv.AppendInt(buf, d.I, 10), true
		case types.String:
			if colType == types.String && !char {
				return append(buf, d.S...), true
			}
		}
	case 1:
		switch d.Fam {
		case types.Int:
			switch width {
			case 2:
				return binary.BigEndian.AppendUint16(buf, uint16(int16(d.I))), true
			case 4:
				return binary.BigEndian.AppendUint32(buf, uint32(int32(d.I))), true
			}
			return binary.BigEndian.AppendUint64(buf, uint64(d.I)), true
		case types.Bytes, types.Uuid:
			return append(buf, d.S...), true
		case types.String:
			// Binary text is the text itself (Pad, which pads it, is
			// excluded above).
			return append(buf, d.S...), true
		}
	}
	return buf, false
}
