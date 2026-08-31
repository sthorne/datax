package pgwire

import (
	"bufio"
	"io"
	"strings"
	"testing"
)

func TestDecodeTextField(t *testing.T) {
	cases := []struct{ in, want string }{
		{"plain", "plain"},
		{`a\tb`, "a\tb"},
		{`a\nb`, "a\nb"},
		{`a\rb`, "a\rb"},
		{`a\\b`, `a\b`},
		{`\b\f\v`, "\b\f\v"},
		{`\101`, "A"},   // octal, 3 digits
		{`\10`, "\x08"}, // octal, 2 digits
		{`\7x`, "\x07x"},
		{`\x41`, "A"}, // hex, 2 digits
		{`\xA!`, "\n!"},
		{`\x`, "x"},   // \x with no digits: literal x
		{`\q`, "q"},   // backslash-other: the char itself
		{`\.d`, ".d"}, // escaped dot (not the terminator)
	}
	for _, c := range cases {
		got, err := decodeTextField(c.in)
		if err != nil || got != c.want {
			t.Fatalf("decodeTextField(%q) = %q, %v; want %q", c.in, got, err, c.want)
		}
	}
	if _, err := decodeTextField(`half\`); err == nil {
		t.Fatal("lone trailing backslash accepted")
	}
}

func TestReadCSVRecord(t *testing.T) {
	read := func(t *testing.T, src string) [][]csvField {
		t.Helper()
		br := bufio.NewReader(strings.NewReader(src))
		var recs [][]csvField
		for {
			rec, err := readCSVRecord(br)
			if err == io.EOF {
				return recs
			}
			if err != nil {
				t.Fatalf("%q: %v", src, err)
			}
			recs = append(recs, rec)
		}
	}

	recs := read(t, "a,b,c\n1,,\"\"\n")
	if len(recs) != 2 {
		t.Fatalf("records: %d", len(recs))
	}
	if recs[0][0].text != "a" || recs[0][2].text != "c" {
		t.Fatalf("row 0: %+v", recs[0])
	}
	// Unquoted empty (NULL) vs quoted empty (empty string).
	if recs[1][1].quoted || recs[1][1].text != "" {
		t.Fatalf("unquoted empty: %+v", recs[1][1])
	}
	if !recs[1][2].quoted || recs[1][2].text != "" {
		t.Fatalf("quoted empty: %+v", recs[1][2])
	}

	// Quoted commas, newlines, and "" escapes; \r\n endings; final line
	// without a newline.
	recs = read(t, "\"a,b\",\"l1\nl2\",\"say \"\"hi\"\"\"\r\nlast,row")
	if len(recs) != 2 {
		t.Fatalf("records: %d", len(recs))
	}
	if recs[0][0].text != "a,b" || recs[0][1].text != "l1\nl2" || recs[0][2].text != `say "hi"` {
		t.Fatalf("quoted row: %+v", recs[0])
	}
	if recs[1][0].text != "last" || recs[1][1].text != "row" {
		t.Fatalf("final row: %+v", recs[1])
	}

	// Unterminated quote errors.
	br := bufio.NewReader(strings.NewReader("\"open\n"))
	if _, err := readCSVRecord(br); err == nil {
		t.Fatal("unterminated quote accepted")
	}
}
