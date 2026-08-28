// Package rowenc encodes SQL rows as KV pairs:
//
//	key   = /t/<tableID>/ + order-preserving encoding of the PK columns
//	value = JSON object { "<colID>": value } of the non-PK columns
//	        (NULL = absent; int64 as exact-precision json.Number)
package rowenc

import (
	"bytes"
	"encoding/json"
	"fmt"
	"strconv"

	"github.com/sthorne/datax/pkg/keys"
	"github.com/sthorne/datax/pkg/sql/catalog"
	"github.com/sthorne/datax/pkg/sql/types"
	"github.com/sthorne/datax/pkg/util/encoding"
)

// EncodePK builds the row key for the given primary key values (one datum
// per PK column, coerced and non-null).
func EncodePK(desc *catalog.TableDescriptor, pkVals []types.Datum) (keys.Key, error) {
	if len(pkVals) != len(desc.PrimaryKey) {
		return nil, fmt.Errorf("expected %d primary key values, got %d", len(desc.PrimaryKey), len(pkVals))
	}
	k := keys.TableDataPrefix(desc.ID)
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

// DecodePK decodes the PK datums from a row key.
func DecodePK(desc *catalog.TableDescriptor, key keys.Key) ([]types.Datum, error) {
	prefix := keys.TableDataPrefix(desc.ID)
	if !bytes.HasPrefix(key, prefix) {
		return nil, fmt.Errorf("key not in table %d", desc.ID)
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

// EncodeValue builds the row value from the full row (colID → datum).
// PK columns are skipped (they live in the key); NULLs are omitted.
func EncodeValue(desc *catalog.TableDescriptor, row map[catalog.ColumnID]types.Datum) ([]byte, error) {
	obj := make(map[string]any, len(row))
	for colID, d := range row {
		if desc.IsPKCol(colID) || d.Null {
			continue
		}
		col, ok := desc.ColByID(colID)
		if !ok {
			return nil, fmt.Errorf("unknown column id %d", colID)
		}
		d, err := d.Coerce(col.Type)
		if err != nil {
			return nil, fmt.Errorf("column %q: %w", col.Name, err)
		}
		key := strconv.Itoa(int(colID))
		switch col.Type {
		case types.Int:
			obj[key] = json.Number(strconv.FormatInt(d.I, 10))
		case types.Float:
			obj[key] = d.F
		case types.String:
			obj[key] = d.S
		case types.Bool:
			obj[key] = d.B
		}
	}
	return json.Marshal(obj)
}

// DecodeValue parses a row value into colID → datum (missing = NULL).
func DecodeValue(desc *catalog.TableDescriptor, raw []byte) (map[catalog.ColumnID]types.Datum, error) {
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.UseNumber()
	var obj map[string]any
	if err := dec.Decode(&obj); err != nil {
		return nil, fmt.Errorf("corrupt row value: %w", err)
	}
	out := make(map[catalog.ColumnID]types.Datum, len(obj))
	for k, v := range obj {
		id, err := strconv.Atoi(k)
		if err != nil {
			return nil, fmt.Errorf("corrupt row value key %q", k)
		}
		colID := catalog.ColumnID(id)
		col, ok := desc.ColByID(colID)
		if !ok {
			continue // column dropped (not in v1, but be lenient)
		}
		switch col.Type {
		case types.Int:
			n, ok := v.(json.Number)
			if !ok {
				return nil, fmt.Errorf("column %q: expected number", col.Name)
			}
			i, err := n.Int64()
			if err != nil {
				return nil, fmt.Errorf("column %q: %w", col.Name, err)
			}
			out[colID] = types.NewInt(i)
		case types.Float:
			n, ok := v.(json.Number)
			if !ok {
				return nil, fmt.Errorf("column %q: expected number", col.Name)
			}
			f, err := n.Float64()
			if err != nil {
				return nil, fmt.Errorf("column %q: %w", col.Name, err)
			}
			out[colID] = types.NewFloat(f)
		case types.String:
			s, ok := v.(string)
			if !ok {
				return nil, fmt.Errorf("column %q: expected string", col.Name)
			}
			out[colID] = types.NewString(s)
		case types.Bool:
			b, ok := v.(bool)
			if !ok {
				return nil, fmt.Errorf("column %q: expected bool", col.Name)
			}
			out[colID] = types.NewBool(b)
		}
	}
	return out, nil
}
