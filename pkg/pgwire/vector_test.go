package pgwire

import (
	"bytes"
	"strings"
	"testing"

	"github.com/sthorne/datax/pkg/sql"
	"github.com/sthorne/datax/pkg/sql/types"
	"github.com/sthorne/datax/pkg/sql/vector"
)

// TestVectorWireCodec: a vector describes under its own OID, travels as
// text in the '[1,2,3]' form, and in binary as pgvector lays it out
// (int16 dimension, int16 zero, float4 elements), both ways.
func TestVectorWireCodec(t *testing.T) {
	col := sql.ResultColumn{Name: "embedding", Type: types.Vector}
	if got := colOID(col); got != vector.OID || typeOID(types.Vector) != vector.OID {
		t.Fatalf("a vector column describes as OID %d, want %d", got, vector.OID)
	}
	if got := colSize(col); got != -1 {
		t.Fatalf("typlen %d, want -1 (variable)", got)
	}
	d, err := types.ParseVector("[1,-2.5,3]")
	if err != nil {
		t.Fatal(err)
	}
	if got := string(encodeDatum(d, 0, col)); got != "[1,-2.5,3]" {
		t.Fatalf("text form %q", got)
	}
	bin := encodeDatum(d, 1, col)
	want := []byte{0, 3, 0, 0, 0x3f, 0x80, 0, 0, 0xc0, 0x20, 0, 0, 0x40, 0x40, 0, 0}
	if !bytes.Equal(bin, want) {
		t.Fatalf("binary form % x, want % x", bin, want)
	}
	back, err := decodeBinaryParam(bin, types.Vector)
	if err != nil || back.Fam != types.Vector || back.S != d.S {
		t.Fatalf("binary parameter decoded to %+v (%v)", back, err)
	}
	for name, raw := range map[string][]byte{
		"truncated":       bin[:len(bin)-1],
		"wrong dimension": append([]byte{0, 4}, bin[2:]...),
		"NaN":             {0, 1, 0, 0, 0x7f, 0xc0, 0, 0},
		"infinity":        {0, 1, 0, 0, 0x7f, 0x80, 0, 0},
		"empty":           {0, 0, 0, 0},
	} {
		if _, err := decodeBinaryParam(raw, types.Vector); err == nil {
			t.Errorf("a %s binary vector was accepted", name)
		} else if name == "NaN" && !strings.Contains(err.Error(), "NaN") {
			t.Errorf("NaN refused as %v", err)
		}
	}
}
