package parser

import (
	"testing"

	"github.com/sthorne/datax/pkg/sql/types"
)

// TestParseColumnTypeSpec: the type modifiers datax enforces (issue #96)
// — integer widths, VARCHAR(n) / CHAR(n), TIMESTAMP with and without
// time zone and TIMESTAMP(p) — land on the column definition; the
// SERIAL variants pick their width; invalid modifiers are refused.
func TestParseColumnTypeSpec(t *testing.T) {
	ct := parseOne(t, `CREATE TABLE t (
		a INT, b INTEGER, c INT4, d SMALLINT, e INT2, f INT8, g BIGINT,
		h VARCHAR(20), i CHARACTER VARYING(5), j CHAR(3), k CHARACTER(2), l CHAR, m VARCHAR, n TEXT,
		o TIMESTAMP, p TIMESTAMP WITHOUT TIME ZONE, q TIMESTAMPTZ, r TIMESTAMP WITH TIME ZONE,
		s TIMESTAMP(3), u TIMESTAMPTZ(0), v TIMESTAMP(6) WITH TIME ZONE,
		w SERIAL, x BIGSERIAL, y SMALLSERIAL, z DECIMAL(10,2)
	)`).(*CreateTable)
	want := map[string]ColumnDef{
		"a": {Type: types.Int, Width: 4}, "b": {Type: types.Int, Width: 4}, "c": {Type: types.Int, Width: 4},
		"d": {Type: types.Int, Width: 2}, "e": {Type: types.Int, Width: 2},
		"f": {Type: types.Int}, "g": {Type: types.Int},
		"h": {Type: types.String, MaxLen: 20}, "i": {Type: types.String, MaxLen: 5},
		"j": {Type: types.String, MaxLen: 3, Char: true}, "k": {Type: types.String, MaxLen: 2, Char: true},
		"l": {Type: types.String, MaxLen: 1, Char: true}, "m": {Type: types.String}, "n": {Type: types.String},
		"o": {Type: types.Timestamp, NoTZ: true}, "p": {Type: types.Timestamp, NoTZ: true},
		"q": {Type: types.Timestamp}, "r": {Type: types.Timestamp},
		"s": {Type: types.Timestamp, NoTZ: true, TimePrecision: 4}, "u": {Type: types.Timestamp, TimePrecision: 1},
		"v": {Type: types.Timestamp, TimePrecision: 7},
		"w": {Type: types.Int, Width: 4, Serial: true, NotNull: true}, "x": {Type: types.Int, Serial: true, NotNull: true},
		"y": {Type: types.Int, Width: 2, Serial: true, NotNull: true},
		"z": {Type: types.Decimal, Precision: 10, Scale: 2},
	}
	if len(ct.Columns) != len(want) {
		t.Fatalf("%d columns, want %d", len(ct.Columns), len(want))
	}
	for _, c := range ct.Columns {
		w := want[c.Name]
		if c.Type != w.Type || c.Width != w.Width || c.MaxLen != w.MaxLen || c.Char != w.Char || c.NoTZ != w.NoTZ ||
			c.Precision != w.Precision || c.Scale != w.Scale || c.Serial != w.Serial || c.TimePrecision != w.TimePrecision {
			t.Errorf("column %s: %+v, want %+v", c.Name, c, w)
		}
	}

	// ALTER COLUMN TYPE carries the same spec.
	at := parseOne(t, `ALTER TABLE t ALTER COLUMN a SET DATA TYPE VARCHAR(8)`).(*AlterTable)
	if st := at.SetType; st == nil || st.Type != types.String || st.MaxLen != 8 || st.Char {
		t.Fatalf("SET DATA TYPE VARCHAR(8): %+v", at.SetType)
	}
	at = parseOne(t, `ALTER TABLE t ALTER a TYPE TIMESTAMP(2)`).(*AlterTable)
	if st := at.SetType; st == nil || st.Type != types.Timestamp || !st.NoTZ || st.TimePrecision != 3 {
		t.Fatalf("TYPE TIMESTAMP(2): %+v", at.SetType)
	}

	// Typmods on other types are still accepted and ignored.
	ct = parseOne(t, `CREATE TABLE t (a FLOAT8(3), b BYTEA(9))`).(*CreateTable)
	if ct.Columns[0].Precision != 0 || ct.Columns[0].TimePrecision != 0 || ct.Columns[1].MaxLen != 0 {
		t.Fatalf("ignored typmods: %+v", ct.Columns)
	}

	for _, bad := range []string{
		`CREATE TABLE t (a VARCHAR(0))`,
		`CREATE TABLE t (a CHAR(-1))`,
		`CREATE TABLE t (a TIMESTAMP(7))`,
		`CREATE TABLE t (a TIMESTAMPTZ(10))`,
	} {
		if _, err := Parse(bad); err == nil {
			t.Errorf("%s: parsed, want an error", bad)
		}
	}
}
