// Package rowenc encodes SQL rows as KV pairs (encoding v2):
//
//	key   = /t/<tableID>/<indexID>/ + order-preserving encoding of the
//	        index columns; primary rows are index PrimaryIndexID (1)
//	value = one version byte, then for each non-NULL non-PK column in
//	        ascending column-ID order: colID uvarint, a type tag byte, and
//	        the type's payload (NULL = absent)
//
// Unknown column IDs are skipped on decode — every payload is
// self-describing via its tag — which is what makes lazy DROP COLUMN and
// nullable ADD COLUMN free: old rows simply carry extra or missing columns.
package rowenc

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"math"
	"sort"

	"github.com/sthorne/datax/pkg/keys"
	"github.com/sthorne/datax/pkg/sql/catalog"
	"github.com/sthorne/datax/pkg/sql/inverted"
	"github.com/sthorne/datax/pkg/sql/types"
	"github.com/sthorne/datax/pkg/sql/vector"
	"github.com/sthorne/datax/pkg/util/encoding"
)

// PrimaryIndexID is the index ID of a table's primary rows.
const PrimaryIndexID = 1

// valueVersion tags the row-value encoding; bump on incompatible change.
// It is the first byte of every EncodeValue output.
const valueVersion byte = 1

// SealedValueMarker is the first byte of a sealed row value: the value of
// a sealed table's row, encrypted under the table's key (issue #310,
// pkg/sql/rowseal). No EncodeValue output begins with it — every one
// begins with valueVersion, which must never take this value — so a
// value tells by its first byte which form it is in, and DecodeValue
// handed a sealed value fails rather than misreading it.
const SealedValueMarker byte = 0xD5

// ValueVersion is valueVersion, for tests elsewhere that check the claim
// above.
const ValueVersion = valueVersion

// Column payload tags. Each payload is self-describing so unknown columns
// can be skipped.
const (
	tagInt       byte = 1 // 8-byte big-endian two's complement
	tagFloat     byte = 2 // 8-byte big-endian IEEE-754 bits
	tagString    byte = 3 // uvarint length + bytes
	tagBool      byte = 4 // 1 byte (0/1)
	tagTimestamp byte = 5 // 8-byte big-endian Unix nanoseconds
	tagDate      byte = 6 // 8-byte big-endian Unix days
	tagBytes     byte = 7 // uvarint length + raw bytes
	tagUUID      byte = 8 // 16 raw bytes
	// tagNull marks an explicit NULL (no payload). Only written for
	// fill-on-read (ALTER-added DEFAULT) columns, where a missing column
	// means "row predates the column" rather than NULL.
	tagNull     byte = 9
	tagDecimal  byte = 10 // uvarint length + canonical decimal string
	tagJsonb    byte = 11 // uvarint length + normalized JSON text
	tagInterval byte = 12 // 3 × 8-byte big-endian: months, days, nanoseconds
	tagTime     byte = 13 // 8-byte big-endian nanoseconds since midnight
	// tagArray: uvarint element family, uvarint count, then per element
	// a 0 byte (NULL) or 1 + the element's order-preserving key encoding
	// (self-delimiting for every scalar family).
	tagArray byte = 14
	tagEnum  byte = 15 // uvarint ordinal, uvarint length + label
	// tagTsvector and tagTsquery: uvarint length + the value's canonical
	// text, as JSONB is stored.
	tagTsvector byte = 16
	tagTsquery  byte = 17
	// tagVector: uvarint byte length + the elements, four big-endian
	// bytes of float32 each (the datum's own form).
	tagVector byte = 18
)

// PrimaryKeyPrefix is the key prefix of a table's ORIGINAL primary rows
// (index 1). Prefer PrimaryKeyPrefixFor: a re-sharded table's live
// primary index is a later ID.
func PrimaryKeyPrefix(tableID uint64) keys.Key {
	return keys.TableIndexPrefix(tableID, PrimaryIndexID)
}

// PrimaryKeyPrefixFor is the key prefix of the table's LIVE primary rows.
func PrimaryKeyPrefixFor(desc *catalog.TableDescriptor) keys.Key {
	return keys.TableIndexPrefix(desc.ID, desc.LivePrimaryIndex())
}

// PrimarySpan covers a table's original (index 1) primary rows; prefer
// PrimarySpanFor.
func PrimarySpan(tableID uint64) (keys.Key, keys.Key) {
	return keys.TableIndexSpan(tableID, PrimaryIndexID)
}

// PrimarySpanFor covers the table's live primary rows.
func PrimarySpanFor(desc *catalog.TableDescriptor) (keys.Key, keys.Key) {
	return keys.TableIndexSpan(desc.ID, desc.LivePrimaryIndex())
}

// EncodePK builds the row key for the given primary key values (one datum
// per PK column, coerced and non-null) at the table's live primary index.
func EncodePK(desc *catalog.TableDescriptor, pkVals []types.Datum) (keys.Key, error) {
	return EncodePKAt(desc, desc.LivePrimaryIndex(), pkVals)
}

// EncodePKAt is EncodePK at an explicit index ID — the re-shard backfill
// and dual-write build the NEW layout's keys with it while the live index
// still points at the old one.
func EncodePKAt(desc *catalog.TableDescriptor, indexID uint64, pkVals []types.Datum) (keys.Key, error) {
	if len(pkVals) != len(desc.PrimaryKey) {
		return nil, fmt.Errorf("expected %d primary key values, got %d", len(desc.PrimaryKey), len(pkVals))
	}
	k := keys.TableIndexPrefix(desc.ID, indexID)
	for i, colID := range desc.PrimaryKey {
		col, _ := desc.ColByID(colID)
		d := pkVals[i]
		if d.Null {
			return nil, fmt.Errorf("null value in primary key column %q", col.Name)
		}
		var err error
		k, err = appendDatum(k, col.Type, d)
		if err != nil {
			return nil, fmt.Errorf("column %q: %w", col.Name, err)
		}
	}
	return k, nil
}

// AppendKeyDatum appends the order-preserving key encoding of d (coerced to
// fam) to k. It is how the planner turns a range predicate's bound value
// into a scan bound: the encoding is self-delimiting, so PrefixEnd on the
// result steps past exactly the keys carrying that column value.
func AppendKeyDatum(k keys.Key, fam types.Family, d types.Datum) (keys.Key, error) {
	return appendDatum(k, fam, d)
}

func appendDatum(k keys.Key, fam types.Family, d types.Datum) (keys.Key, error) {
	d, err := d.Coerce(fam)
	if err != nil {
		return nil, err
	}
	switch fam {
	case types.Int, types.Timestamp, types.Date, types.Time:
		return keys.Key(encoding.EncodeInt64(k, d.I)), nil
	case types.Enum:
		// Ordered by ordinal (declaration order); the label follows so
		// a key decodes without the type.
		k = keys.Key(encoding.EncodeInt64(k, d.I))
		return keys.Key(encoding.EncodeString(k, d.S)), nil
	case types.IntervalFam:
		// Ordered by PostgreSQL's comparison value, then the stored
		// triple keeps distinct spellings of one length distinct keys.
		iv := d.IntervalVal()
		k = keys.Key(encoding.EncodeInt64(k, iv.CmpValue()))
		k = keys.Key(encoding.EncodeInt64(k, iv.Months))
		return keys.Key(encoding.EncodeInt64(k, iv.Days)), nil
	case types.Float:
		return keys.Key(encoding.EncodeFloat64(k, d.F)), nil
	case types.String, types.Bytes, types.Uuid:
		return keys.Key(encoding.EncodeString(k, d.S)), nil
	case types.Bool:
		return keys.Key(encoding.EncodeBool(k, d.B)), nil
	case types.Decimal:
		out, err := encoding.EncodeDecimalText(k, d.S)
		if err != nil {
			return nil, err
		}
		return keys.Key(out), nil
	}
	return nil, fmt.Errorf("unencodable type %s", fam)
}

// decodeKeyDatum decodes one order-preserving key encoding of fam
// (appendDatum's inverse).
func decodeKeyDatum(rest []byte, fam types.Family) ([]byte, types.Datum, error) {
	var err error
	var d types.Datum
	switch fam {
	case types.Int, types.Timestamp, types.Date, types.Time:
		var v int64
		rest, v, err = encoding.DecodeInt64(rest)
		d = types.Datum{Fam: fam, I: v}
	case types.Enum:
		var ord int64
		var label string
		if rest, ord, err = encoding.DecodeInt64(rest); err == nil {
			rest, label, err = encoding.DecodeString(rest)
		}
		d = types.NewEnum(ord, label)
	case types.IntervalFam:
		var cmp, months, days int64
		if rest, cmp, err = encoding.DecodeInt64(rest); err == nil {
			if rest, months, err = encoding.DecodeInt64(rest); err == nil {
				rest, days, err = encoding.DecodeInt64(rest)
			}
		}
		d = types.NewInterval(types.Interval{Months: months, Days: days, Nanos: cmp - (months*30+days)*types.NanosPerDay})
	case types.Float:
		var v float64
		rest, v, err = encoding.DecodeFloat64(rest)
		d = types.NewFloat(v)
	case types.String, types.Bytes, types.Uuid:
		var v string
		rest, v, err = encoding.DecodeString(rest)
		d = types.Datum{Fam: fam, S: v}
	case types.Bool:
		var v bool
		rest, v, err = encoding.DecodeBool(rest)
		d = types.NewBool(v)
	case types.Decimal:
		var v string
		rest, v, err = encoding.DecodeDecimalText(rest)
		d = types.NewDecimal(v)
	default:
		err = fmt.Errorf("undecodable type %s", fam)
	}
	return rest, d, err
}

// DecodePK decodes the PK datums from a primary row key.
func DecodePK(desc *catalog.TableDescriptor, key keys.Key) ([]types.Datum, error) {
	prefix := PrimaryKeyPrefixFor(desc)
	if !bytes.HasPrefix(key, prefix) {
		return nil, fmt.Errorf("key not in table %d's primary index", desc.ID)
	}
	rest := []byte(key[len(prefix):])
	out := make([]types.Datum, 0, len(desc.PrimaryKey))
	for _, colID := range desc.PrimaryKey {
		col, _ := desc.ColByID(colID)
		var err error
		var d types.Datum
		rest, d, err = decodeKeyDatum(rest, col.Type)
		if err != nil {
			return nil, fmt.Errorf("decoding pk column %q: %w", col.Name, err)
		}
		out = append(out, d)
	}
	return out, nil
}

// LeadingPKColumn returns the leading primary-key column of a row key of
// desc at any of its indexes' prefixes (/t/<id>/<index>/...): the bytes
// of its key encoding — what tells one value from another in KV — and the
// value decoded. A per-tenant sealed table (issue #310) picks a row's key
// by it.
func LeadingPKColumn(desc *catalog.TableDescriptor, key keys.Key) ([]byte, types.Datum, error) {
	p := keys.TableDataPrefix(desc.ID)
	if !bytes.HasPrefix(key, p) || len(key) < len(p)+8 {
		return nil, types.Datum{}, fmt.Errorf("key not in table %d's rows", desc.ID)
	}
	if len(desc.PrimaryKey) == 0 {
		return nil, types.Datum{}, fmt.Errorf("table %d has no primary key", desc.ID)
	}
	col, ok := desc.ColByID(desc.PrimaryKey[0])
	if !ok {
		return nil, types.Datum{}, fmt.Errorf("table %d: unknown primary key column %d", desc.ID, desc.PrimaryKey[0])
	}
	at := []byte(key[len(p)+8:])
	rest, d, err := decodeKeyDatum(at, col.Type)
	if err != nil {
		return nil, types.Datum{}, fmt.Errorf("decoding pk column %q: %w", col.Name, err)
	}
	return at[:len(at)-len(rest)], d, nil
}

// EncodeValue builds the binary row value from the full row (colID → datum).
// PK columns are skipped (they live in the key); NULLs are omitted; columns
// are emitted in ascending ID order (deterministic bytes for identical rows).
func EncodeValue(desc *catalog.TableDescriptor, row map[catalog.ColumnID]types.Datum) ([]byte, error) {
	// An ALTER COLUMN TYPE in flight: the shadow column's value is the
	// original's, converted — on every write, whoever writes it.
	for _, col := range desc.Columns {
		if col.RetypeFrom == 0 {
			continue
		}
		src, ok := row[col.RetypeFrom]
		if !ok {
			continue
		}
		from, _ := desc.ColByID(col.RetypeFrom)
		var d types.Datum
		var err error
		if col.Type == types.Enum {
			d = src // the label converts in Conform below
		} else if d, err = src.ConvertTo(col.Type); err != nil {
			return nil, fmt.Errorf("column %q: value cannot convert to %s: %w", from.Name, col.Type, err)
		}
		named := col
		named.Name = from.Name // errors name the column being retyped, not its shadow
		if d, err = named.Conform(d); err != nil {
			return nil, err // a *catalog.ValueError: the caller keeps its SQLSTATE
		}
		row[col.ID] = d
	}
	ids := make([]catalog.ColumnID, 0, len(row))
	for colID, d := range row {
		if desc.IsPKCol(colID) {
			continue
		}
		if d.Null {
			// NULLs are normally omitted — except for fill-on-read columns,
			// where absence means "predates the column" (= default).
			if col, ok := desc.ColByID(colID); !ok || !col.FillDefault {
				continue
			}
		}
		ids = append(ids, colID)
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })

	out := []byte{valueVersion}
	for _, colID := range ids {
		col, ok := desc.ColByID(colID)
		if !ok {
			return nil, fmt.Errorf("unknown column id %d", colID)
		}
		if row[colID].Null {
			out = encoding.EncodeUvarint(out, uint64(colID))
			out = append(out, tagNull)
			continue
		}
		d, err := row[colID].Coerce(col.Type)
		if err != nil {
			return nil, fmt.Errorf("column %q: %w", col.Name, err)
		}
		out = encoding.EncodeUvarint(out, uint64(colID))
		if col.Type.IsArray() {
			elem := col.Type.Elem()
			out = append(out, tagArray)
			out = encoding.EncodeUvarint(out, uint64(elem))
			out = encoding.EncodeUvarint(out, uint64(len(d.A)))
			for _, e := range d.A {
				if e.Null {
					out = append(out, 0)
					continue
				}
				out = append(out, 1)
				k, err := appendDatum(nil, elem, e)
				if err != nil {
					return nil, fmt.Errorf("column %q: %w", col.Name, err)
				}
				out = append(out, k...)
			}
			continue
		}
		switch col.Type {
		case types.Int, types.Timestamp, types.Date, types.Time:
			tag := tagInt
			switch col.Type {
			case types.Timestamp:
				tag = tagTimestamp
			case types.Date:
				tag = tagDate
			case types.Time:
				tag = tagTime
			}
			out = append(out, tag)
			out = binary.BigEndian.AppendUint64(out, uint64(d.I))
		case types.IntervalFam:
			out = append(out, tagInterval)
			out = binary.BigEndian.AppendUint64(out, uint64(d.Mo))
			out = binary.BigEndian.AppendUint64(out, uint64(d.Dy))
			out = binary.BigEndian.AppendUint64(out, uint64(d.I))
		case types.Enum:
			out = append(out, tagEnum)
			out = encoding.EncodeUvarint(out, uint64(d.I))
			out = encoding.EncodeUvarint(out, uint64(len(d.S)))
			out = append(out, d.S...)
		case types.Float:
			out = append(out, tagFloat)
			out = binary.BigEndian.AppendUint64(out, math.Float64bits(d.F))
		case types.String, types.Bytes, types.Decimal, types.Jsonb, types.Tsvector, types.Tsquery, types.Vector:
			tag := tagString
			switch col.Type {
			case types.Tsvector:
				tag = tagTsvector
			case types.Tsquery:
				tag = tagTsquery
			case types.Bytes:
				tag = tagBytes
			case types.Decimal:
				tag = tagDecimal
			case types.Jsonb:
				tag = tagJsonb
			case types.Vector:
				tag = tagVector
			}
			out = append(out, tag)
			out = encoding.EncodeUvarint(out, uint64(len(d.S)))
			out = append(out, d.S...)
		case types.Uuid:
			if len(d.S) != 16 {
				return nil, fmt.Errorf("column %q: UUID must be 16 bytes", col.Name)
			}
			out = append(out, tagUUID)
			out = append(out, d.S...)
		case types.Bool:
			out = append(out, tagBool)
			if d.B {
				out = append(out, 1)
			} else {
				out = append(out, 0)
			}
		default:
			return nil, fmt.Errorf("unencodable type %s", col.Type)
		}
	}
	return out, nil
}

// DecodeValue parses a binary row value into colID → datum (missing =
// NULL). Columns the descriptor does not know are skipped — dropped
// columns' bytes, or columns added by a newer descriptor.
func DecodeValue(desc *catalog.TableDescriptor, raw []byte) (map[catalog.ColumnID]types.Datum, error) {
	if len(raw) == 0 {
		return nil, fmt.Errorf("corrupt row value: empty")
	}
	if raw[0] == SealedValueMarker {
		return nil, fmt.Errorf("row value is sealed: it must be opened under its table's key before it is decoded")
	}
	if raw[0] != valueVersion {
		return nil, fmt.Errorf("corrupt row value: unknown version 0x%02x", raw[0])
	}
	rest := raw[1:]
	out := make(map[catalog.ColumnID]types.Datum)
	for len(rest) > 0 {
		var id uint64
		var err error
		rest, id, err = encoding.DecodeUvarint(rest)
		if err != nil {
			return nil, fmt.Errorf("corrupt row value: %w", err)
		}
		if len(rest) == 0 {
			return nil, fmt.Errorf("corrupt row value: truncated at column %d", id)
		}
		tag := rest[0]
		rest = rest[1:]

		colID := catalog.ColumnID(id)
		col, known := desc.ColByID(colID)

		switch tag {
		case tagNull:
			if known {
				out[colID] = types.DNull
			}
		case tagInt, tagTimestamp, tagDate, tagTime:
			if len(rest) < 8 {
				return nil, fmt.Errorf("corrupt row value: short int payload")
			}
			v := int64(binary.BigEndian.Uint64(rest[:8]))
			rest = rest[8:]
			want := types.Int
			switch tag {
			case tagTimestamp:
				want = types.Timestamp
			case tagDate:
				want = types.Date
			case tagTime:
				want = types.Time
			}
			if known && col.Type == want {
				out[colID] = types.Datum{Fam: want, I: v}
			}
		case tagArray:
			var elemN, n uint64
			var err error
			if rest, elemN, err = encoding.DecodeUvarint(rest); err != nil {
				return nil, fmt.Errorf("corrupt row value: %w", err)
			}
			if rest, n, err = encoding.DecodeUvarint(rest); err != nil {
				return nil, fmt.Errorf("corrupt row value: %w", err)
			}
			elem := types.Family(elemN)
			elems := make([]types.Datum, 0, n)
			for j := uint64(0); j < n; j++ {
				if len(rest) == 0 {
					return nil, fmt.Errorf("corrupt row value: short array payload")
				}
				flag := rest[0]
				rest = rest[1:]
				if flag == 0 {
					elems = append(elems, types.DNull)
					continue
				}
				var d types.Datum
				if rest, d, err = decodeKeyDatum(rest, elem); err != nil {
					return nil, fmt.Errorf("corrupt row value: array element: %w", err)
				}
				elems = append(elems, d)
			}
			if known && col.Type == types.ArrayOf(elem) {
				out[colID] = types.NewArray(elem, elems)
			}
		case tagEnum:
			var ord, n uint64
			var err error
			if rest, ord, err = encoding.DecodeUvarint(rest); err != nil {
				return nil, fmt.Errorf("corrupt row value: %w", err)
			}
			if rest, n, err = encoding.DecodeUvarint(rest); err != nil {
				return nil, fmt.Errorf("corrupt row value: %w", err)
			}
			if uint64(len(rest)) < n {
				return nil, fmt.Errorf("corrupt row value: short enum payload")
			}
			label := string(rest[:n])
			rest = rest[n:]
			if known && col.Type == types.Enum {
				out[colID] = types.NewEnum(int64(ord), label)
			}
		case tagInterval:
			if len(rest) < 24 {
				return nil, fmt.Errorf("corrupt row value: short interval payload")
			}
			iv := types.Interval{
				Months: int64(binary.BigEndian.Uint64(rest[:8])),
				Days:   int64(binary.BigEndian.Uint64(rest[8:16])),
				Nanos:  int64(binary.BigEndian.Uint64(rest[16:24])),
			}
			rest = rest[24:]
			if known && col.Type == types.IntervalFam {
				out[colID] = types.NewInterval(iv)
			}
		case tagFloat:
			if len(rest) < 8 {
				return nil, fmt.Errorf("corrupt row value: short float payload")
			}
			v := math.Float64frombits(binary.BigEndian.Uint64(rest[:8]))
			rest = rest[8:]
			if known && col.Type == types.Float {
				out[colID] = types.NewFloat(v)
			}
		case tagString, tagBytes, tagDecimal, tagJsonb, tagTsvector, tagTsquery, tagVector:
			var n uint64
			var err error
			rest, n, err = encoding.DecodeUvarint(rest)
			if err != nil || uint64(len(rest)) < n {
				return nil, fmt.Errorf("corrupt row value: bad string payload")
			}
			v := string(rest[:n])
			rest = rest[n:]
			want := types.String
			switch tag {
			case tagBytes:
				want = types.Bytes
			case tagDecimal:
				want = types.Decimal
			case tagJsonb:
				want = types.Jsonb
			case tagTsvector:
				want = types.Tsvector
			case tagTsquery:
				want = types.Tsquery
			case tagVector:
				want = types.Vector
			}
			if known && col.Type == want {
				out[colID] = types.Datum{Fam: want, S: v}
			}
		case tagUUID:
			if len(rest) < 16 {
				return nil, fmt.Errorf("corrupt row value: short uuid payload")
			}
			v := string(rest[:16])
			rest = rest[16:]
			if known && col.Type == types.Uuid {
				out[colID] = types.Datum{Fam: types.Uuid, S: v}
			}
		case tagBool:
			if len(rest) < 1 {
				return nil, fmt.Errorf("corrupt row value: short bool payload")
			}
			v := rest[0] != 0
			rest = rest[1:]
			if known && col.Type == types.Bool {
				out[colID] = types.NewBool(v)
			}
		default:
			return nil, fmt.Errorf("corrupt row value: unknown tag 0x%02x for column %d", tag, id)
		}
	}
	// Fill-on-read: a fill-default column absent from the value predates
	// the column's ADD — it reads as the default. (Explicit NULLs were
	// stored as tagNull above and are present in out.)
	for _, col := range desc.Columns {
		if !col.FillDefault || col.Default == nil {
			continue
		}
		if _, ok := out[col.ID]; !ok {
			out[col.ID] = *col.Default
		}
	}
	return out, nil
}

// VectorColumn finds a VECTOR column's stored elements in an opened row
// value (EncodeValue's form) without decoding the rest of the row: the
// returned bytes alias raw. ok is false whenever DecodeValue would not
// produce a non-NULL vector datum from those very bytes — the column is
// NULL, absent (fill-on-read, or a row that predates it), stored under
// another tag, not a vector column of desc, or the value does not parse
// — and the caller then decodes the whole row, which reports or
// resolves whatever the case is exactly as it always has.
func VectorColumn(desc *catalog.TableDescriptor, raw []byte, id catalog.ColumnID) (stored []byte, ok bool) {
	if col, known := desc.ColByID(id); !known || col.Type != types.Vector {
		return nil, false
	}
	if len(raw) == 0 || raw[0] != valueVersion {
		return nil, false
	}
	rest := raw[1:]
	for len(rest) > 0 {
		var cid uint64
		var err error
		if rest, cid, err = encoding.DecodeUvarint(rest); err != nil || len(rest) == 0 {
			return nil, false
		}
		tag := rest[0]
		rest = rest[1:]
		if catalog.ColumnID(cid) == id {
			if tag != tagVector {
				return nil, false
			}
			var n uint64
			if rest, n, err = encoding.DecodeUvarint(rest); err != nil || uint64(len(rest)) < n {
				return nil, false
			}
			// A column appears once in a value; DecodeValue would keep
			// the last occurrence, so a value that repeats it is left to
			// DecodeValue too.
			stored = rest[:n]
			if after, ok := skipValueColumns(rest[n:], id); !ok || after {
				return nil, false
			}
			return stored, true
		}
		if rest, ok = skipValuePayload(tag, rest); !ok {
			return nil, false
		}
	}
	return nil, false
}

// skipValueColumns walks the rest of a value; found reports whether
// column id appears in it again, ok whether it parsed.
func skipValueColumns(rest []byte, id catalog.ColumnID) (found, ok bool) {
	for len(rest) > 0 {
		var cid uint64
		var err error
		if rest, cid, err = encoding.DecodeUvarint(rest); err != nil || len(rest) == 0 {
			return false, false
		}
		if catalog.ColumnID(cid) == id {
			return true, true
		}
		tag := rest[0]
		if rest, ok = skipValuePayload(tag, rest[1:]); !ok {
			return false, false
		}
	}
	return false, true
}

// skipValuePayload steps over one column payload of the given tag, as
// DecodeValue reads it.
func skipValuePayload(tag byte, rest []byte) ([]byte, bool) {
	fixed := func(n int) ([]byte, bool) {
		if len(rest) < n {
			return nil, false
		}
		return rest[n:], true
	}
	switch tag {
	case tagNull:
		return rest, true
	case tagInt, tagTimestamp, tagDate, tagTime, tagFloat:
		return fixed(8)
	case tagInterval:
		return fixed(24)
	case tagUUID:
		return fixed(16)
	case tagBool:
		return fixed(1)
	case tagString, tagBytes, tagDecimal, tagJsonb, tagTsvector, tagTsquery, tagVector:
		var n uint64
		var err error
		if rest, n, err = encoding.DecodeUvarint(rest); err != nil || uint64(len(rest)) < n {
			return nil, false
		}
		return rest[n:], true
	case tagEnum:
		var n uint64
		var err error
		if rest, _, err = encoding.DecodeUvarint(rest); err != nil {
			return nil, false
		}
		if rest, n, err = encoding.DecodeUvarint(rest); err != nil || uint64(len(rest)) < n {
			return nil, false
		}
		return rest[n:], true
	case tagArray:
		var elemN, n uint64
		var err error
		if rest, elemN, err = encoding.DecodeUvarint(rest); err != nil {
			return nil, false
		}
		if rest, n, err = encoding.DecodeUvarint(rest); err != nil {
			return nil, false
		}
		for j := uint64(0); j < n; j++ {
			if len(rest) == 0 {
				return nil, false
			}
			flag := rest[0]
			rest = rest[1:]
			if flag == 0 {
				continue
			}
			if rest, _, err = decodeKeyDatum(rest, types.Family(elemN)); err != nil {
				return nil, false
			}
		}
		return rest, true
	}
	return nil, false
}

// ---------------------------------------------------------------------------
// Secondary index entries.

// EncodeIndexPrefix builds an index key prefix from the leading index
// column values (fewer datums than index columns = a scan prefix).
func EncodeIndexPrefix(desc *catalog.TableDescriptor, idx *catalog.IndexDescriptor, vals []types.Datum) (keys.Key, error) {
	if len(vals) > len(idx.ColumnIDs) {
		return nil, fmt.Errorf("index %q has %d columns, got %d values", idx.Name, len(idx.ColumnIDs), len(vals))
	}
	k := keys.TableIndexPrefix(desc.ID, idx.ID)
	for i, d := range vals {
		fam, ok := idx.KeyFamily(desc, i)
		if !ok {
			return nil, fmt.Errorf("index %q references unknown column %d", idx.Name, idx.ColumnIDs[i])
		}
		if d.Null {
			if !TaggedIndexKey(idx) {
				return nil, fmt.Errorf("cannot encode NULL in an index key")
			}
			k = append(k, IndexKeyNullTag)
			continue
		}
		var err error
		k, err = AppendIndexKeyDatum(k, idx, fam, d)
		if err != nil {
			return nil, fmt.Errorf("%s: %w", keyColumnLabel(desc, idx, i), err)
		}
	}
	return k, nil
}

// keyColumnLabel names key column i of the index in an error.
func keyColumnLabel(desc *catalog.TableDescriptor, idx *catalog.IndexDescriptor, i int) string {
	if idx.IsExprKey(i) {
		return fmt.Sprintf("expression %s", idx.Exprs[i])
	}
	col, _ := desc.ColByID(idx.ColumnIDs[i])
	return fmt.Sprintf("column %q", col.Name)
}

// KeyComputer computes what an index with an expression key or a
// predicate (catalog.IndexDescriptor.Computed) holds for a row: the
// value of every key column, the plain ones included, and whether the
// row satisfies the predicate (has entries at all). Evaluating SQL is
// the SQL layer's, so it registers the function (SetKeyComputer); this
// package only encodes what it returns.
type KeyComputer func(desc *catalog.TableDescriptor, idx *catalog.IndexDescriptor, row map[catalog.ColumnID]types.Datum) (vals []types.Datum, include bool, err error)

var keyComputer KeyComputer

// SetKeyComputer registers the function every encoder of an index's
// entries calls for a computed index. It is set once, at init.
func SetKeyComputer(f KeyComputer) { keyComputer = f }

// indexKeyValues returns the values of the index's key columns for row,
// and include=false when the row has no entry in it because it does not
// satisfy the index's predicate.
func indexKeyValues(desc *catalog.TableDescriptor, idx *catalog.IndexDescriptor, row map[catalog.ColumnID]types.Datum) ([]types.Datum, bool, error) {
	if idx.Computed() {
		if keyComputer == nil {
			return nil, false, fmt.Errorf("index %q has expression keys or a predicate, and nothing here computes them", idx.Name)
		}
		return keyComputer(desc, idx, row)
	}
	vals := make([]types.Datum, 0, len(idx.ColumnIDs))
	for _, colID := range idx.ColumnIDs {
		d, ok := row[colID]
		if !ok {
			d = types.DNull
		}
		vals = append(vals, d)
	}
	return vals, true, nil
}

// The tags of a NULLS NOT DISTINCT unique index's key columns: each
// column's encoding is preceded by one of them, so that a NULL has a key
// of its own that no value's encoding can equal, and NULLs sort first.
const (
	IndexKeyNullTag  byte = 0x00
	IndexKeyValueTag byte = 0x01
)

// TaggedIndexKey reports whether the index keys every column with a tag
// byte first (a unique index declared NULLS NOT DISTINCT).
func TaggedIndexKey(idx *catalog.IndexDescriptor) bool {
	return idx != nil && idx.Unique && idx.NullsNotDistinct
}

// AppendIndexKeyDatum appends one non-NULL key column of the index, as
// the index's entries encode it: AppendKeyDatum, after the value tag
// when the index tags its columns. A scan bound on an index column is
// built with it.
func AppendIndexKeyDatum(k keys.Key, idx *catalog.IndexDescriptor, fam types.Family, d types.Datum) (keys.Key, error) {
	if TaggedIndexKey(idx) {
		k = append(k, IndexKeyValueTag)
	}
	return appendDatum(k, fam, d)
}

// nonUniqueIndexValue marks a non-unique index entry present (values must be
// non-empty: an empty MVCC read is indistinguishable from absence).
var nonUniqueIndexValue = []byte{0}

// EncodeIndexEntry builds an ordinary index's KV pair for a full row
// (IndexEntries serves both kinds of index). skip=true when
// an indexed column is NULL: the row simply has no entry in that index
// (SQL equality never matches NULL, so equality lookups stay correct).
// For a unique index that is PostgreSQL's NULLS DISTINCT: a row with a
// NULL conflicts with no other row. A unique index declared NULLS NOT
// DISTINCT is the exception: it keys a NULL like a value (see
// TaggedIndexKey), so every row has an entry.
func EncodeIndexEntry(desc *catalog.TableDescriptor, idx *catalog.IndexDescriptor, row map[catalog.ColumnID]types.Datum) (key keys.Key, value []byte, skip bool, err error) {
	return EncodeIndexEntryAt(desc, idx, idx.ID, row)
}

// EncodeIndexEntryAt is EncodeIndexEntry with the entry's index ID
// overridden: the re-shard machinery rebuilds every secondary index at a
// shadow ID (the entry's primary-key suffix embeds the shard bucket, so
// the two layouts' entries must live in disjoint keyspaces). The row map
// supplies the suffix, so a shadow row carrying the new bucket value
// yields the new-layout entry.
func EncodeIndexEntryAt(desc *catalog.TableDescriptor, idx *catalog.IndexDescriptor, entryID uint64, row map[catalog.ColumnID]types.Datum) (key keys.Key, value []byte, skip bool, err error) {
	vals, include, err := indexKeyValues(desc, idx, row)
	if err != nil || !include {
		return nil, nil, true, err
	}
	if !TaggedIndexKey(idx) {
		for _, d := range vals {
			if d.Null {
				return nil, nil, true, nil
			}
		}
	}
	at := *idx
	at.ID = entryID
	k, err := EncodeIndexPrefix(desc, &at, vals)
	if err != nil {
		return nil, nil, false, err
	}
	pkEnc, err := encodePKSuffix(desc, row)
	if err != nil {
		return nil, nil, false, err
	}
	if idx.Unique {
		return k, pkEnc, false, nil
	}
	return append(k, pkEnc...), nonUniqueIndexValue, false, nil
}

// IndexEntry is one KV pair of a secondary index.
type IndexEntry struct {
	Key   keys.Key
	Value []byte
}

// IndexEntries returns every entry the row has in the index, at the
// index's own ID: none or one for an ordinary index, one per element of
// the indexed value for an inverted one. It is what the write path, the
// backfill and the re-shard copy all encode with, so no caller needs to
// know which kind of index it holds.
func IndexEntries(desc *catalog.TableDescriptor, idx *catalog.IndexDescriptor, row map[catalog.ColumnID]types.Datum) ([]IndexEntry, error) {
	return IndexEntriesAt(desc, idx, idx.ID, row)
}

// IndexEntriesAt is IndexEntries with the entries' index ID overridden
// (see EncodeIndexEntryAt).
func IndexEntriesAt(desc *catalog.TableDescriptor, idx *catalog.IndexDescriptor, entryID uint64, row map[catalog.ColumnID]types.Datum) ([]IndexEntry, error) {
	if idx.IsVector() {
		return vectorEntries(desc, idx, entryID, row)
	}
	if !idx.IsInverted() {
		key, val, skip, err := EncodeIndexEntryAt(desc, idx, entryID, row)
		if err != nil || skip {
			return nil, err
		}
		return []IndexEntry{{Key: key, Value: val}}, nil
	}
	if idx.IsPartial() {
		// A partial inverted index: a row the predicate does not hold
		// for has no entries.
		if _, include, err := indexKeyValues(desc, idx, row); err != nil || !include {
			return nil, err
		}
	}
	elements, err := invertedElements(desc, idx, row)
	if err != nil || len(elements) == 0 {
		return nil, err
	}
	pkEnc, err := encodePKSuffix(desc, row)
	if err != nil {
		return nil, err
	}
	prefix := keys.TableIndexPrefix(desc.ID, entryID)
	out := make([]IndexEntry, len(elements))
	for i, el := range elements {
		k := make(keys.Key, 0, len(prefix)+encoding.EncodedBytesLen(el)+len(pkEnc))
		k = append(k, prefix...)
		k = encoding.EncodeBytes(k, el)
		out[i] = IndexEntry{Key: append(k, pkEnc...), Value: nonUniqueIndexValue}
	}
	return out, nil
}

// ErrCentroidsNotLoaded is returned for a vector index whose centres
// this process has not read yet. The SQL layer loads them when it looks
// the table up (they need a transaction to read, which this package
// does not have), so reaching this is a path that skipped the lookup:
// the write fails rather than file the row under a guess.
type ErrCentroidsNotLoaded struct{ Index string }

func (e *ErrCentroidsNotLoaded) Error() string {
	return fmt.Sprintf("the centroids of vector index %q are not loaded", e.Index)
}

// vectorEntries returns the row's entry in a vector (IVF) index: keyed
// by the list its vector is filed under and then the primary key, and
// carrying the vector itself, so that a query ranks a list's rows from
// the index alone and fetches only the few it returns. A NULL vector
// has no entry. The list is a function of the vector and the build's
// centres only, which is what lets an UPDATE or DELETE recompute the
// key of the entry the INSERT wrote.
func vectorEntries(desc *catalog.TableDescriptor, idx *catalog.IndexDescriptor, entryID uint64, row map[catalog.ColumnID]types.Datum) ([]IndexEntry, error) {
	if len(idx.ColumnIDs) != 1 {
		return nil, fmt.Errorf("vector index %q must have exactly one column", idx.Name)
	}
	d, ok := row[idx.ColumnIDs[0]]
	if !ok || d.Null {
		return nil, nil
	}
	if d.Fam != types.Vector {
		return nil, fmt.Errorf("vector index %q: column holds a %s value", idx.Name, d.Fam)
	}
	if d.VectorDims() != idx.Vector.Dims {
		return nil, fmt.Errorf("vector index %q: expected %d dimensions, not %d", idx.Name, idx.Vector.Dims, d.VectorDims())
	}
	cents, ok := vector.Lookup(idx.Vector.Build)
	if !ok {
		return nil, &ErrCentroidsNotLoaded{Index: idx.Name}
	}
	pkEnc, err := encodePKSuffix(desc, row)
	if err != nil {
		return nil, err
	}
	k := VectorListPrefix(desc.ID, entryID, cents.Assign(d.VectorVal()))
	return []IndexEntry{{Key: append(k, pkEnc...), Value: []byte(d.S)}}, nil
}

// VectorListPrefix is the key prefix of one list of a vector index;
// the entries under it are the list's rows in primary-key order.
func VectorListPrefix(tableID, indexID uint64, list int) keys.Key {
	return keys.Key(encoding.EncodeUint64(keys.TableIndexPrefix(tableID, indexID), uint64(list)))
}

// VectorEntryPKSuffix returns the encoded primary key at the end of a
// vector index entry's key.
func VectorEntryPKSuffix(desc *catalog.TableDescriptor, idx *catalog.IndexDescriptor, key keys.Key) ([]byte, error) {
	at := len(keys.TableIndexPrefix(desc.ID, idx.ID)) + 8
	if len(key) < at {
		return nil, fmt.Errorf("index %q entry: short key", idx.Name)
	}
	return key[at:], nil
}

// invertedElements returns the sorted, distinct elements of the row's
// value in an inverted index's column. A NULL value has none: no
// predicate the index serves is true of NULL.
func invertedElements(desc *catalog.TableDescriptor, idx *catalog.IndexDescriptor, row map[catalog.ColumnID]types.Datum) ([][]byte, error) {
	kind, ok := inverted.Lookup(idx.Inverted)
	if !ok {
		return nil, fmt.Errorf("index %q: unknown inverted index kind %q", idx.Name, idx.Inverted)
	}
	if len(idx.ColumnIDs) != 1 {
		return nil, fmt.Errorf("inverted index %q must have exactly one column", idx.Name)
	}
	d, ok := row[idx.ColumnIDs[0]]
	if !ok || d.Null {
		return nil, nil
	}
	return kind.Elements(d)
}

// encodePKSuffix encodes the row's primary key column values, without
// any prefix: what follows the indexed part in a non-unique entry's key.
func encodePKSuffix(desc *catalog.TableDescriptor, row map[catalog.ColumnID]types.Datum) (keys.Key, error) {
	var pkEnc keys.Key
	for _, colID := range desc.PrimaryKey {
		col, _ := desc.ColByID(colID)
		var err error
		pkEnc, err = appendDatum(pkEnc, col.Type, row[colID])
		if err != nil {
			return nil, fmt.Errorf("pk column %q: %w", col.Name, err)
		}
	}
	return pkEnc, nil
}

// InvertedElementSpan is the span of an inverted index holding the
// entries of one element: one key per row whose value has the element,
// in primary-key order.
func InvertedElementSpan(desc *catalog.TableDescriptor, idx *catalog.IndexDescriptor, element []byte) (start, end keys.Key) {
	start = keys.Key(encoding.EncodeBytes(keys.TableIndexPrefix(desc.ID, idx.ID), element))
	return start, start.PrefixEnd()
}

// InvertedPrefixSpan is the span holding the entries of every element
// that starts with prefix. An element's key encoding is its bytes
// (escaped) and then a terminator, so the elements sharing a prefix are
// the keys sharing the prefix's encoding without its terminator.
func InvertedPrefixSpan(desc *catalog.TableDescriptor, idx *catalog.IndexDescriptor, prefix []byte) (start, end keys.Key) {
	enc := encoding.EncodeBytes(keys.TableIndexPrefix(desc.ID, idx.ID), prefix)
	start = keys.Key(enc[:len(enc)-2])
	return start, start.PrefixEnd()
}

// InvertedEntryElement splits an inverted index entry's key into its
// element and the encoded primary key that follows it.
func InvertedEntryElement(desc *catalog.TableDescriptor, idx *catalog.IndexDescriptor, key keys.Key) (element, pkSuffix []byte, err error) {
	prefix := keys.TableIndexPrefix(desc.ID, idx.ID)
	if !bytes.HasPrefix(key, prefix) {
		return nil, nil, fmt.Errorf("key not in index %q", idx.Name)
	}
	pkSuffix, element, err = encoding.DecodeBytes(key[len(prefix):])
	if err != nil {
		return nil, nil, fmt.Errorf("index %q entry: %w", idx.Name, err)
	}
	return element, pkSuffix, nil
}

// IndexEntryPrimaryKey recovers the primary row key from an index entry.
func IndexEntryPrimaryKey(desc *catalog.TableDescriptor, idx *catalog.IndexDescriptor, key keys.Key, value []byte) (keys.Key, error) {
	if idx.IsVector() {
		pkSuffix, err := VectorEntryPKSuffix(desc, idx, key)
		if err != nil {
			return nil, err
		}
		return append(PrimaryKeyPrefixFor(desc), pkSuffix...), nil
	}
	if idx.IsInverted() {
		_, pkSuffix, err := InvertedEntryElement(desc, idx, key)
		if err != nil {
			return nil, err
		}
		return append(PrimaryKeyPrefixFor(desc), pkSuffix...), nil
	}
	if idx.Unique {
		return append(PrimaryKeyPrefixFor(desc), value...), nil
	}
	prefix := keys.TableIndexPrefix(desc.ID, idx.ID)
	if !bytes.HasPrefix(key, prefix) {
		return nil, fmt.Errorf("key not in index %q", idx.Name)
	}
	rest := []byte(key[len(prefix):])
	for i, colID := range idx.ColumnIDs {
		fam, ok := idx.KeyFamily(desc, i)
		if !ok {
			return nil, fmt.Errorf("index %q references unknown column %d", idx.Name, colID)
		}
		var err error
		rest, err = skipDatum(rest, fam)
		if err != nil {
			return nil, fmt.Errorf("index %q entry: %w", idx.Name, err)
		}
	}
	return append(PrimaryKeyPrefixFor(desc), rest...), nil
}

// DecodeTrailingTimestamp decodes the LAST primary-key column — a
// timestamp — from rest, the encoded PK suffix of a row key (the bytes
// after the table/index prefix), given the PK column families in
// physical order. Returns UTC nanoseconds. The row-level retention sweep
// uses it to age rows by their timestamp column without a full decode.
func DecodeTrailingTimestamp(rest []byte, fams []types.Family) (int64, bool) {
	if len(fams) == 0 || fams[len(fams)-1] != types.Timestamp {
		return 0, false
	}
	var err error
	for _, fam := range fams[:len(fams)-1] {
		if rest, err = skipDatum(rest, fam); err != nil {
			return 0, false
		}
	}
	tail, nanos, err := encoding.DecodeInt64(rest)
	if err != nil || len(tail) != 0 {
		return 0, false
	}
	return nanos, true
}

func skipDatum(b []byte, fam types.Family) ([]byte, error) {
	var err error
	switch fam {
	case types.Int, types.Timestamp, types.Date, types.Time:
		b, _, err = encoding.DecodeInt64(b)
	case types.Enum:
		if b, _, err = encoding.DecodeInt64(b); err == nil {
			b, _, err = encoding.DecodeString(b)
		}
	case types.IntervalFam:
		for i := 0; i < 3 && err == nil; i++ {
			b, _, err = encoding.DecodeInt64(b)
		}
	case types.Float:
		b, _, err = encoding.DecodeFloat64(b)
	case types.String, types.Bytes, types.Uuid:
		b, _, err = encoding.DecodeString(b)
	case types.Bool:
		b, _, err = encoding.DecodeBool(b)
	case types.Decimal:
		b, _, err = encoding.DecodeDecimalText(b)
	default:
		err = fmt.Errorf("unskippable type %s", fam)
	}
	return b, err
}
