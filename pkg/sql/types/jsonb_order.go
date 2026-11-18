package types

import (
	"encoding/json"
	"math/big"
	"sort"
	"strings"

	"github.com/sthorne/datax/pkg/util/decimal"
)

// compareJSONB orders two jsonb values as PostgreSQL's jsonb btree
// opclass does: object > array > boolean > number > string > null; an
// object with more pairs, or an array with more elements, is the
// greater; objects with as many pairs compare key, value, key, value
// in storage order (shorter keys first); arrays element by element;
// strings by bytes, numbers numerically, false < true. At the top
// level a scalar sorts below any array but the empty one (PostgreSQL's
// raw-scalar quirk). ok is false when a document does not parse.
func compareJSONB(a, b string) (c int, ok bool) {
	va, err := decodeJSONB(a)
	if err != nil {
		return 0, false
	}
	vb, err := decodeJSONB(b)
	if err != nil {
		return 0, false
	}
	return jsonbCmp(va, vb, true), true
}

func decodeJSONB(s string) (any, error) {
	dec := json.NewDecoder(strings.NewReader(s))
	dec.UseNumber()
	var v any
	err := dec.Decode(&v)
	return v, err
}

// jsonbRank is jbvType's order: null, string, number, bool, array,
// object.
func jsonbRank(v any) int {
	switch v.(type) {
	case nil:
		return 0
	case string:
		return 1
	case json.Number:
		return 2
	case bool:
		return 3
	case []any:
		return 4
	}
	return 5
}

func jsonbCmp(a, b any, top bool) int {
	if top {
		// A top-level scalar is a one-element raw array: below an
		// array, except that the empty array has fewer elements.
		arrA, isArrA := a.([]any)
		arrB, isArrB := b.([]any)
		scalarA := jsonbRank(a) < 4
		scalarB := jsonbRank(b) < 4
		switch {
		case scalarA && isArrB:
			if len(arrB) == 0 {
				return 1
			}
			return -1
		case isArrA && scalarB:
			if len(arrA) == 0 {
				return -1
			}
			return 1
		}
	}
	ra, rb := jsonbRank(a), jsonbRank(b)
	if ra != rb {
		return cmpInt(int64(ra), int64(rb))
	}
	switch x := a.(type) {
	case nil:
		return 0
	case string:
		return strings.Compare(x, b.(string))
	case json.Number:
		return cmpJSONNumber(x, b.(json.Number))
	case bool:
		return cmpBool(x, b.(bool))
	case []any:
		y := b.([]any)
		if len(x) != len(y) {
			return cmpInt(int64(len(x)), int64(len(y)))
		}
		for i := range x {
			if c := jsonbCmp(x[i], y[i], false); c != 0 {
				return c
			}
		}
		return 0
	case map[string]any:
		y := b.(map[string]any)
		if len(x) != len(y) {
			return cmpInt(int64(len(x)), int64(len(y)))
		}
		kx, ky := storageOrder(x), storageOrder(y)
		for i := range kx {
			if c := strings.Compare(kx[i], ky[i]); c != 0 {
				return c
			}
			if c := jsonbCmp(x[kx[i]], y[ky[i]], false); c != 0 {
				return c
			}
		}
		return 0
	}
	return 0
}

// storageOrder lists an object's keys as jsonb stores them: shorter
// first, then by bytes.
func storageOrder(m map[string]any) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Slice(keys, func(i, j int) bool {
		if len(keys[i]) != len(keys[j]) {
			return len(keys[i]) < len(keys[j])
		}
		return keys[i] < keys[j]
	})
	return keys
}

func cmpJSONNumber(a, b json.Number) int {
	da, errA := decimal.Parse(string(a))
	db, errB := decimal.Parse(string(b))
	if errA == nil && errB == nil {
		return decimal.Cmp(da, db)
	}
	ra, okA := new(big.Rat).SetString(string(a))
	rb, okB := new(big.Rat).SetString(string(b))
	if okA && okB {
		return ra.Cmp(rb)
	}
	return strings.Compare(string(a), string(b))
}
