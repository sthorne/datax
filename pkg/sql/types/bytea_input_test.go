package types

import (
	"bytes"
	"testing"
)

// TestByteaEscapeInput (PG-T-003): bytea input takes the escape format
// beside hex: \\ is a backslash, \ooo an octal byte, and any other
// backslash is refused, as PostgreSQL's byteain does.
func TestByteaEscapeInput(t *testing.T) {
	for in, want := range map[string][]byte{
		`abc\000`:   {'a', 'b', 'c', 0},
		`a\\b`:      {'a', '\\', 'b'},
		`\101\377`:  {'A', 0xff},
		`plain`:     []byte("plain"),
		`\x0102`:    {1, 2},
		"\\x01 02":  {1, 2},
		`\x`:        {},
		`\\x0102`:   []byte(`\x0102`),
		`tab\011ok`: {'t', 'a', 'b', '\t', 'o', 'k'},
	} {
		got, err := ParseBytes(in)
		if err != nil || !bytes.Equal(got, want) {
			t.Errorf("ParseBytes(%q) = %q, %v; want %q", in, got, err, want)
		}
	}
	for _, in := range []string{`a\q`, `\4000`, `\01`, `end\`, `\x0g`} {
		if got, err := ParseBytes(in); err == nil {
			t.Errorf("ParseBytes(%q) = %q, want an error", in, got)
		}
	}
}
