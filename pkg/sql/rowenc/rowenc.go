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
	"github.com/sthorne/datax/pkg/sql/types"
	"github.com/sthorne/datax/pkg/util/decimal"
	"github.com/sthorne/datax/pkg/util/encoding"
)

// PrimaryIndexID is the index ID of a table's primary rows.
const PrimaryIndexID = 1

// valueVersion tags the row-value encoding; bump on incompatible change.
const valueVersion byte = 1

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
		v, err := d.DecimalVal()
		if err != nil {
			return nil, err
		}
		return keys.Key(encoding.EncodeDecimal(k, v)), nil
	}
	return nil, fmt.Errorf("unencodable type %s", fam)
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
		switch col.Type {
		case types.Int, types.Timestamp, types.Date, types.Time:
			var v int64
			rest, v, err = encoding.DecodeInt64(rest)
			d = types.Datum{Fam: col.Type, I: v}
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
			d = types.Datum{Fam: col.Type, S: v}
		case types.Bool:
			var v bool
			rest, v, err = encoding.DecodeBool(rest)
			d = types.NewBool(v)
		case types.Decimal:
			var v decimal.Dec
			rest, v, err = encoding.DecodeDecimal(rest)
			d = types.NewDecimal(v.String())
		default:
			err = fmt.Errorf("undecodable type %s", col.Type)
		}
		if err != nil {
			return nil, fmt.Errorf("decoding pk column %q: %w", col.Name, err)
		}
		out = append(out, d)
	}
	return out, nil
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
		d, err := src.ConvertTo(col.Type)
		if err != nil {
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
		case types.Float:
			out = append(out, tagFloat)
			out = binary.BigEndian.AppendUint64(out, math.Float64bits(d.F))
		case types.String, types.Bytes, types.Decimal, types.Jsonb:
			tag := tagString
			switch col.Type {
			case types.Bytes:
				tag = tagBytes
			case types.Decimal:
				tag = tagDecimal
			case types.Jsonb:
				tag = tagJsonb
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
		case tagString, tagBytes, tagDecimal, tagJsonb:
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
		col, ok := desc.ColByID(idx.ColumnIDs[i])
		if !ok {
			return nil, fmt.Errorf("index %q references unknown column %d", idx.Name, idx.ColumnIDs[i])
		}
		if d.Null {
			return nil, fmt.Errorf("cannot encode NULL in an index key")
		}
		var err error
		k, err = appendDatum(k, col.Type, d)
		if err != nil {
			return nil, fmt.Errorf("column %q: %w", col.Name, err)
		}
	}
	return k, nil
}

// nonUniqueIndexValue marks a non-unique index entry present (values must be
// non-empty: an empty MVCC read is indistinguishable from absence).
var nonUniqueIndexValue = []byte{0}

// EncodeIndexEntry builds the index KV pair for a full row. skip=true when
// an indexed column is NULL: the row simply has no entry in that index
// (SQL equality never matches NULL, so equality lookups stay correct;
// unique indexes reject NULLs at the executor instead).
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
	vals := make([]types.Datum, 0, len(idx.ColumnIDs))
	for _, colID := range idx.ColumnIDs {
		d, ok := row[colID]
		if !ok || d.Null {
			return nil, nil, true, nil
		}
		vals = append(vals, d)
	}
	at := *idx
	at.ID = entryID
	k, err := EncodeIndexPrefix(desc, &at, vals)
	if err != nil {
		return nil, nil, false, err
	}
	// Encoded PK column values (without any prefix).
	var pkEnc keys.Key
	for _, colID := range desc.PrimaryKey {
		col, _ := desc.ColByID(colID)
		pkEnc, err = appendDatum(pkEnc, col.Type, row[colID])
		if err != nil {
			return nil, nil, false, fmt.Errorf("pk column %q: %w", col.Name, err)
		}
	}
	if idx.Unique {
		return k, pkEnc, false, nil
	}
	return append(k, pkEnc...), nonUniqueIndexValue, false, nil
}

// IndexEntryPrimaryKey recovers the primary row key from an index entry.
func IndexEntryPrimaryKey(desc *catalog.TableDescriptor, idx *catalog.IndexDescriptor, key keys.Key, value []byte) (keys.Key, error) {
	if idx.Unique {
		return append(PrimaryKeyPrefixFor(desc), value...), nil
	}
	prefix := keys.TableIndexPrefix(desc.ID, idx.ID)
	if !bytes.HasPrefix(key, prefix) {
		return nil, fmt.Errorf("key not in index %q", idx.Name)
	}
	rest := []byte(key[len(prefix):])
	for _, colID := range idx.ColumnIDs {
		col, ok := desc.ColByID(colID)
		if !ok {
			return nil, fmt.Errorf("index %q references unknown column %d", idx.Name, colID)
		}
		var err error
		rest, err = skipDatum(rest, col.Type)
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
		b, _, err = encoding.DecodeDecimal(b)
	default:
		err = fmt.Errorf("unskippable type %s", fam)
	}
	return b, err
}
