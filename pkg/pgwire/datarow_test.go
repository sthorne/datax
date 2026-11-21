package pgwire

import (
	"bytes"
	"math"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgproto3"

	"github.com/sthorne/datax/pkg/sql"
	"github.com/sthorne/datax/pkg/sql/types"
)

// referenceDataRow is how a DataRow was encoded before appendDataRow:
// encodeDatumIn per value, then pgproto3's DataRow.Encode.
func referenceDataRow(t testing.TB, row []types.Datum, formats []int16, cols []sql.ResultColumn, ts textStyle) []byte {
	t.Helper()
	dr := &pgproto3.DataRow{Values: make([][]byte, len(row))}
	for i, d := range row {
		format := int16(0)
		if formats != nil && i < len(formats) {
			format = formats[i]
		}
		var col sql.ResultColumn
		if i < len(cols) {
			col = cols[i]
		}
		dr.Values[i] = encodeDatumIn(d, format, col, ts)
	}
	out, err := dr.Encode(nil)
	if err != nil {
		t.Fatal(err)
	}
	return out
}

// wireCase is one value under the column that describes it.
type wireCase struct {
	name string
	col  sql.ResultColumn
	d    types.Datum
}

func wireCases(t testing.TB) []wireCase {
	t.Helper()
	ts := time.Date(2026, 8, 30, 1, 2, 3, 456789000, time.UTC).UnixNano()
	iv := types.Interval{Months: 14, Days: 3, Nanos: 4*int64(time.Hour) + 5*int64(time.Second) + 6000}
	vec, err := types.ParseVector("[1,-2.5,3]")
	if err != nil {
		t.Fatal(err)
	}
	ints := types.NewArray(types.Int, []types.Datum{types.NewInt(1), types.DNull, types.NewInt(-3)})
	strs := types.NewArray(types.String, []types.Datum{types.NewString("a b"), types.NewString(""), types.DNull, types.NewString(`q"x`)})
	col := func(fam types.Family) sql.ResultColumn { return sql.ResultColumn{Name: "c", Type: fam} }
	return []wireCase{
		{"null int", col(types.Int), types.DNull},
		{"null text", col(types.String), types.DNull},
		{"int8", col(types.Int), types.NewInt(-9223372036854775808)},
		{"int8 max", col(types.Int), types.NewInt(math.MaxInt64)},
		{"int8 zero", col(types.Int), types.NewInt(0)},
		{"int4", sql.ResultColumn{Name: "c", Type: types.Int, Width: 4}, types.NewInt(-123456)},
		{"int2", sql.ResultColumn{Name: "c", Type: types.Int, Width: 2}, types.NewInt(-1234)},
		{"int under no column", sql.ResultColumn{}, types.NewInt(77)},
		{"regclass as name", col(types.String), types.Datum{Fam: types.Int, I: 16384, Reg: "t"}},
		{"regclass as oid", col(types.Int), types.Datum{Fam: types.Int, I: 16384, Reg: "t"}},
		{"text", col(types.String), types.NewString("héllo, wörld")},
		{"empty text", col(types.String), types.NewString("")},
		{"varchar", sql.ResultColumn{Name: "c", Type: types.String, MaxLen: 5}, types.NewString("abc")},
		{"char(5)", sql.ResultColumn{Name: "c", Type: types.String, MaxLen: 5, Char: true}, types.Datum{Fam: types.String, S: "ab", Pad: 5}},
		{"padded text under a text column", col(types.String), types.Datum{Fam: types.String, S: "ab", Pad: 4}},
		{"text under int column", col(types.Int), types.NewString("12")},
		{"long text", col(types.String), types.NewString(strings.Repeat("x", 300))},
		{"bytea", col(types.Bytes), types.NewBytes([]byte{0, 1, 0xfe, '\\', 'a'})},
		{"empty bytea", col(types.Bytes), types.NewBytes(nil)},
		{"uuid", col(types.Uuid), types.NewUUID([16]byte{0x12, 0x34, 0x56, 0x78, 0x9a, 0xbc, 0xde, 0xf0, 1, 2, 3, 4, 5, 6, 7, 8})},
		{"bool true", col(types.Bool), types.NewBool(true)},
		{"bool false", col(types.Bool), types.NewBool(false)},
		{"float8", col(types.Float), types.NewFloat(3.141592653589793)},
		{"float8 NaN", col(types.Float), types.NewFloat(math.NaN())},
		{"float8 -Inf", col(types.Float), types.NewFloat(math.Inf(-1))},
		{"numeric", col(types.Decimal), types.NewDecimal("-12345.678")},
		{"numeric(10,2)", sql.ResultColumn{Name: "c", Type: types.Decimal, Typmod: (10 << 16) | (2 + 4)}, types.Datum{Fam: types.Decimal, S: "9.9", Dscale: 2}},
		{"numeric NaN", col(types.Decimal), types.NewDecimal("NaN")},
		{"timestamptz", col(types.Timestamp), types.NewTimestamp(ts)},
		{"timestamp", sql.ResultColumn{Name: "c", Type: types.Timestamp, NoTZ: true}, types.Datum{Fam: types.Timestamp, I: ts, NoTZ: true}},
		{"date", col(types.Date), types.NewDate(20000)},
		{"time", col(types.Time), types.NewTime(int64(13*time.Hour + 14*time.Minute + 15*time.Second))},
		{"interval", col(types.IntervalFam), types.NewInterval(iv)},
		{"jsonb", col(types.Jsonb), types.NewJsonb(`{"a": [1, 2.5, "x"], "b": null}`)},
		{"jsonb string", col(types.Jsonb), types.NewJsonb(`"s"`)},
		{"int8[]", col(types.ArrayOf(types.Int)), ints},
		{"text[]", col(types.ArrayOf(types.String)), strs},
		{"empty int8[]", col(types.ArrayOf(types.Int)), types.NewArray(types.Int, nil)},
		{"enum", sql.ResultColumn{Name: "c", Type: types.Enum, EnumType: 100, EnumName: "mood"}, types.NewEnum(1, "happy")},
		{"vector", col(types.Vector), vec},
	}
}

// wireStyles are the session settings that change a value's text.
var wireStyles = map[string]textStyle{
	"default": {loc: time.UTC},
	"zone":    {loc: time.FixedZone("x", -(3*3600 + 30*60))},
	"styles":  {loc: time.UTC, intervalStyle: "iso_8601", byteaEscape: true, extraFloatDigits: -3},
	"nil loc": {},
}

// TestAppendDataRowMatchesEncode: the DataRow appendDataRow writes is
// the one encodeDatumIn and DataRow.Encode wrote, byte for byte, for
// every value shape in text and binary, alone and in one wide row,
// under every session text style.
func TestAppendDataRowMatchesEncode(t *testing.T) {
	cases := wireCases(t)
	for styleName, ts := range wireStyles {
		for _, format := range []int16{0, 1} {
			formats := []int16{format}
			for _, c := range cases {
				row := []types.Datum{c.d}
				cols := []sql.ResultColumn{c.col}
				want := referenceDataRow(t, row, formats, cols, ts)
				prefix := []byte("prefix")
				got, err := appendDataRow(append([]byte(nil), prefix...), row, formats, cols, ts)
				if err != nil {
					t.Fatalf("%s/%d/%s: %v", styleName, format, c.name, err)
				}
				if !bytes.HasPrefix(got, prefix) || !bytes.Equal(got[len(prefix):], want) {
					t.Errorf("%s/format %d/%s:\n got % x\nwant % x", styleName, format, c.name, got[len(prefix):], want)
				}
			}
			// One wide row of every case, after a row already in the
			// buffer; per-column formats; and nil formats (all text).
			var row []types.Datum
			var cols []sql.ResultColumn
			var mixed []int16
			for i, c := range cases {
				row, cols = append(row, c.d), append(cols, c.col)
				mixed = append(mixed, int16(i%2))
			}
			for _, fs := range [][]int16{formats, mixed, nil, {1}} {
				want := referenceDataRow(t, row, fs, cols, ts)
				first, err := appendDataRow(nil, row[:2], fs, cols, ts)
				if err != nil {
					t.Fatal(err)
				}
				got, err := appendDataRow(append([]byte(nil), first...), row, fs, cols, ts)
				if err != nil {
					t.Fatal(err)
				}
				if !bytes.Equal(got[len(first):], want) {
					t.Errorf("%s/formats %v: wide row differs:\n got % x\nwant % x", styleName, fs, got[len(first):], want)
				}
			}
			// More values than described columns.
			want := referenceDataRow(t, row, formats, cols[:3], ts)
			got, err := appendDataRow(nil, row, formats, cols[:3], ts)
			if err != nil || !bytes.Equal(got, want) {
				t.Errorf("%s/%d: row wider than its columns differs (%v)", styleName, format, err)
			}
		}
	}
	// An empty row.
	got, err := appendDataRow(nil, nil, nil, nil, textStyle{})
	if want := referenceDataRow(t, nil, nil, nil, textStyle{}); err != nil || !bytes.Equal(got, want) {
		t.Errorf("empty row: % x, want % x (%v)", got, want, err)
	}
}

// TestAppendDataRowTooManyValues: a row DataRow cannot carry fails with
// DataRow.Encode's error and leaves the buffer as it was.
func TestAppendDataRowTooManyValues(t *testing.T) {
	row := make([]types.Datum, math.MaxUint16+1)
	buf, err := appendDataRow([]byte("keep"), row, nil, nil, textStyle{})
	if _, werr := (&pgproto3.DataRow{Values: make([][]byte, len(row))}).Encode(nil); err == nil || werr == nil || err.Error() != werr.Error() {
		t.Fatalf("got %v, want %v", err, werr)
	}
	if string(buf) != "keep" {
		t.Fatalf("buffer changed: %q", buf)
	}
}

// benchRows is a page of rows shaped like the scan bench's (INT8 key,
// 256-byte TEXT).
func benchRows() ([][]types.Datum, []sql.ResultColumn) {
	rows := make([][]types.Datum, 1024)
	pad := strings.Repeat("p", 256)
	for i := range rows {
		rows[i] = []types.Datum{types.NewInt(int64(i) * 7919), types.NewString(pad)}
	}
	return rows, []sql.ResultColumn{{Name: "k", Type: types.Int}, {Name: "pad", Type: types.String}}
}

// BenchmarkDataRowEncode: per-row cost of encoding result rows into the
// wire buffer (ns/op and allocs/op are per row).
func BenchmarkDataRowEncode(b *testing.B) {
	rows, cols := benchRows()
	ts := textStyle{loc: time.UTC}
	for _, format := range []int16{0, 1} {
		name := map[int16]string{0: "text", 1: "binary"}[format]
		formats := []int16{format}
		b.Run(name+"/append", func(b *testing.B) {
			b.ReportAllocs()
			buf := make([]byte, 0, streamFlushBytes+4096)
			for i := 0; i < b.N; i++ {
				var err error
				if buf, err = appendDataRow(buf, rows[i%len(rows)], formats, cols, ts); err != nil {
					b.Fatal(err)
				}
				if len(buf) >= streamFlushBytes {
					buf = buf[:0]
				}
			}
		})
		b.Run(name+"/encode", func(b *testing.B) {
			b.ReportAllocs()
			buf := make([]byte, 0, streamFlushBytes+4096)
			dr := &pgproto3.DataRow{Values: make([][]byte, len(cols))}
			for i := 0; i < b.N; i++ {
				row := rows[i%len(rows)]
				for j, d := range row {
					dr.Values[j] = encodeDatumIn(d, format, cols[j], ts)
				}
				var err error
				if buf, err = dr.Encode(buf); err != nil {
					b.Fatal(err)
				}
				if len(buf) >= streamFlushBytes {
					buf = buf[:0]
				}
			}
		})
	}
}
