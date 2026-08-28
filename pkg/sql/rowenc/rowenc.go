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
	"github.com/sthorne/datax/pkg/util/encoding"
)

// PrimaryIndexID is the index ID of a table's primary rows.
const PrimaryIndexID = 1

// valueVersion tags the row-value encoding; bump on incompatible change.
const valueVersion byte = 1

// Column payload tags. Each payload is self-describing so unknown columns
// can be skipped.
const (
	tagInt    byte = 1 // 8-byte big-endian two's complement
	tagFloat  byte = 2 // 8-byte big-endian IEEE-754 bits
	tagString byte = 3 // uvarint length + bytes
	tagBool   byte = 4 // 1 byte (0/1)
)

// PrimaryKeyPrefix is the key prefix of a table's primary rows.
func PrimaryKeyPrefix(tableID uint64) keys.Key {
	return keys.TableIndexPrefix(tableID, PrimaryIndexID)
}

// PrimarySpan covers a table's primary rows.
func PrimarySpan(tableID uint64) (keys.Key, keys.Key) {
	return keys.TableIndexSpan(tableID, PrimaryIndexID)
}

// EncodePK builds the row key for the given primary key values (one datum
// per PK column, coerced and non-null).
func EncodePK(desc *catalog.TableDescriptor, pkVals []types.Datum) (keys.Key, error) {
	if len(pkVals) != len(desc.PrimaryKey) {
		return nil, fmt.Errorf("expected %d primary key values, got %d", len(desc.PrimaryKey), len(pkVals))
	}
	k := PrimaryKeyPrefix(desc.ID)
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

func appendDatum(k keys.Key, fam types.Family, d types.Datum) (keys.Key, error) {
	d, err := d.Coerce(fam)
	if err != nil {
		return nil, err
	}
	switch fam {
	case types.Int:
		return keys.Key(encoding.EncodeInt64(k, d.I)), nil
	case types.Float:
		return keys.Key(encoding.EncodeFloat64(k, d.F)), nil
	case types.String:
		return keys.Key(encoding.EncodeString(k, d.S)), nil
	case types.Bool:
		return keys.Key(encoding.EncodeBool(k, d.B)), nil
	}
	return nil, fmt.Errorf("unencodable type %s", fam)
}

// DecodePK decodes the PK datums from a primary row key.
func DecodePK(desc *catalog.TableDescriptor, key keys.Key) ([]types.Datum, error) {
	prefix := PrimaryKeyPrefix(desc.ID)
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
		case types.Int:
			var v int64
			rest, v, err = encoding.DecodeInt64(rest)
			d = types.NewInt(v)
		case types.Float:
			var v float64
			rest, v, err = encoding.DecodeFloat64(rest)
			d = types.NewFloat(v)
		case types.String:
			var v string
			rest, v, err = encoding.DecodeString(rest)
			d = types.NewString(v)
		case types.Bool:
			var v bool
			rest, v, err = encoding.DecodeBool(rest)
			d = types.NewBool(v)
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
	ids := make([]catalog.ColumnID, 0, len(row))
	for colID, d := range row {
		if desc.IsPKCol(colID) || d.Null {
			continue
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
		d, err := row[colID].Coerce(col.Type)
		if err != nil {
			return nil, fmt.Errorf("column %q: %w", col.Name, err)
		}
		out = encoding.EncodeUvarint(out, uint64(colID))
		switch col.Type {
		case types.Int:
			out = append(out, tagInt)
			out = binary.BigEndian.AppendUint64(out, uint64(d.I))
		case types.Float:
			out = append(out, tagFloat)
			out = binary.BigEndian.AppendUint64(out, math.Float64bits(d.F))
		case types.String:
			out = append(out, tagString)
			out = encoding.EncodeUvarint(out, uint64(len(d.S)))
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
		case tagInt:
			if len(rest) < 8 {
				return nil, fmt.Errorf("corrupt row value: short int payload")
			}
			v := int64(binary.BigEndian.Uint64(rest[:8]))
			rest = rest[8:]
			if known && col.Type == types.Int {
				out[colID] = types.NewInt(v)
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
		case tagString:
			var n uint64
			var err error
			rest, n, err = encoding.DecodeUvarint(rest)
			if err != nil || uint64(len(rest)) < n {
				return nil, fmt.Errorf("corrupt row value: bad string payload")
			}
			v := string(rest[:n])
			rest = rest[n:]
			if known && col.Type == types.String {
				out[colID] = types.NewString(v)
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
	return out, nil
}
