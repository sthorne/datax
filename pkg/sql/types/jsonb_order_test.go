package types

import "testing"

// TestJSONBOrder: PostgreSQL's jsonb btree order (PG-F-005).
func TestJSONBOrder(t *testing.T) {
	// Ascending, as PostgreSQL sorts them.
	asc := []string{
		`[]`, `null`, `""`, `"a"`, `"b"`, `-1`, `1.5`, `2`, `false`, `true`,
		`[null]`, `[1]`, `[2]`, `["x", 1]`, `{}`, `{"b": 1}`, `{"a": 1, "b": 2}`, `{"a": 2, "b": 1}`,
		`{"b": 1, "d": 1}`, `{"aa": 1, "c": 1}`, // shorter keys first: c before aa
	}
	ds := make([]Datum, len(asc))
	for i, s := range asc {
		d, err := ParseJSONB(s)
		if err != nil {
			t.Fatal(err)
		}
		ds[i] = d
	}
	for i := range ds {
		for j := range ds {
			c, err := ds[i].Compare(ds[j])
			if err != nil {
				t.Fatalf("%s vs %s: %v", asc[i], asc[j], err)
			}
			want := 0
			switch {
			case i < j:
				want = -1
			case i > j:
				want = 1
			}
			if c != want {
				t.Errorf("%s vs %s = %d, want %d", asc[i], asc[j], c, want)
			}
		}
	}
	// Numerically equal documents order by their text, so only equal
	// texts are equal (jsonb = compares normalized text).
	one, _ := ParseJSONB(`1`)
	onePt, _ := ParseJSONB(`1.0`)
	if c, _ := one.Compare(onePt); c == 0 {
		t.Error("1 and 1.0 compare equal")
	}
}
