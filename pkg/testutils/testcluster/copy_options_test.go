package testcluster

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
)

// TestCopyFromOptions covers COPY FROM's options (gap PG-Q-019 of
// docs/compatibility-gaps.md) over the wire: HEADER (and HEADER MATCH),
// DELIMITER, NULL, QUOTE, ESCAPE, FORCE_NULL, FORCE_NOT_NULL and
// ENCODING, in the parenthesized syntax and the pre-9.0 keywords, with
// PostgreSQL's checks of the options and its errors.
func TestCopyFromOptions(t *testing.T) {
	tc := Start(t, 1)
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()
	conn := pgConn(t, ctx, tc, 0)
	pc := conn.PgConn()
	if _, err := pc.Exec(ctx, `CREATE TABLE cf (id INT8 PRIMARY KEY, a TEXT, c TEXT, n INT8)`).ReadAll(); err != nil {
		t.Fatal(err)
	}
	copyIn := func(stmt, data string) (int64, error) {
		t.Helper()
		tag, err := pc.CopyFrom(ctx, strings.NewReader(data), stmt)
		return tag.RowsAffected(), err
	}
	ok := func(stmt, data string, want int64) {
		t.Helper()
		n, err := copyIn(stmt, data)
		if err != nil {
			t.Fatalf("%s: %v", stmt, err)
		}
		if n != want {
			t.Fatalf("%s: COPY %d, want %d", stmt, n, want)
		}
	}
	fails := func(stmt, data, code, msg string) {
		t.Helper()
		_, err := copyIn(stmt, data)
		var pgErr *pgconn.PgError
		if !errors.As(err, &pgErr) || pgErr.Code != code || !strings.Contains(pgErr.Message, msg) {
			t.Fatalf("%s: got %v, want %s %q", stmt, err, code, msg)
		}
	}

	// The pre-9.0 syntax: CSV with a header line, which is skipped.
	ok(`COPY cf (id, a, n) FROM STDIN WITH CSV HEADER`, "id,a,n\n1,x,10\n2,,\n", 2)
	// The parenthesized syntax: another delimiter, NULL string, quote and
	// escape; a quote may open mid-field, as in PostgreSQL.
	ok(`COPY cf (id, a, c) FROM STDIN WITH (FORMAT csv, HEADER true, DELIMITER ';', NULL 'NA', QUOTE '''', ESCAPE '\')`,
		"id;a;c\n3;'semi;colon';NA\n4;'it\\'s';x'y;z'\n5;NA;''\n", 3)
	// The same in the old keywords.
	ok(`COPY cf (id, a, c) FROM STDIN CSV DELIMITER AS '|' NULL AS 'nil' QUOTE AS '*' ESCAPE AS '!'`, "6|*a|b*|nil\n7|*!**|**\n", 2)
	// FORCE_NULL: a quoted NULL string is NULL; FORCE_NOT_NULL: an
	// unquoted one is the string.
	ok(`COPY cf (id, a, c) FROM STDIN (FORMAT csv, FORCE_NULL (a), FORCE_NOT_NULL (c))`, "8,\"\",\n9,\"\",\"\"\n", 2)
	ok(`COPY cf (id, a, c) FROM STDIN CSV FORCE NOT NULL a, c`, "10,,\n", 1)
	ok(`COPY cf (id, a, c) FROM STDIN (FORMAT csv, FORCE_NULL *)`, "11,\"\",x\n", 1)
	// Text format: another delimiter (escaped in data), NULL string and
	// a header; the default delimiter escaped in data too.
	ok(`COPY cf (id, a, c) FROM STDIN WITH (DELIMITER '|', NULL '', HEADER)`, "id|a|c\n12|p\\|q|\n13||r\n", 2)
	ok(`COPY cf (id, a) FROM STDIN`, "14\ttab\\\there\n", 1)
	// HEADER MATCH checks the names.
	ok(`COPY cf (id, c) FROM STDIN (FORMAT csv, HEADER match, ENCODING 'UTF8')`, "id,c\n15,z\n", 1)
	ok(`COPY cf (id, c) FROM STDIN (HEADER MATCH, ENCODING 'utf-8')`, "id\tc\n16\tw\n", 1)

	rows, err := pc.Exec(ctx, `SELECT id, coalesce(a, '<null>'), coalesce(c, '<null>'), coalesce(n::text, '<null>') FROM cf ORDER BY id`).ReadAll()
	if err != nil {
		t.Fatal(err)
	}
	var got []string
	for _, r := range rows[0].Rows {
		got = append(got, string(r[0])+"|"+string(r[1])+"|"+string(r[2])+"|"+string(r[3]))
	}
	want := []string{
		"1|x|<null>|10", "2|<null>|<null>|<null>",
		"3|semi;colon|<null>|<null>", "4|it's|xy;z|<null>", "5|<null>||<null>",
		"6|a|b|<null>|<null>", "7|*||<null>",
		"8|<null>||<null>", "9|<null>||<null>", "10|||<null>", "11|<null>|x|<null>",
		"12|p|q|<null>|<null>", "13|<null>|r|<null>", "14|tab\there|<null>|<null>",
		"15|<null>|z|<null>", "16|<null>|w|<null>",
	}
	if strings.Join(got, "\n") != strings.Join(want, "\n") {
		t.Fatalf("rows:\n%s\nwant:\n%s", strings.Join(got, "\n"), strings.Join(want, "\n"))
	}

	// HEADER MATCH refuses a header that does not name the columns.
	fails(`COPY cf (id, c) FROM STDIN (FORMAT csv, HEADER match)`, "id,x\n17,z\n", "22P04",
		`column name mismatch in header line field 2: got "x", expected "c"`)
	fails(`COPY cf (id, c) FROM STDIN (FORMAT csv, HEADER match)`, "id\n17\n", "22P04",
		"wrong number of fields in header line: got 1, expected 2")
	fails(`COPY cf (id, c) FROM STDIN (FORMAT csv, HEADER match)`, "id,\n17,z\n", "22P04",
		`column name mismatch in header line field 2: got null value (""), expected "c"`)
	// The options are checked as PostgreSQL checks them.
	for _, c := range []struct{ stmt, code, msg string }{
		{`COPY cf FROM STDIN (FORMAT nope)`, "22023", `COPY format "nope" not recognized`},
		{`COPY cf FROM STDIN (DELIMITER '|', DELIMITER ',')`, "42601", "conflicting or redundant options"},
		{`COPY cf FROM STDIN (ON_ERROR ignore)`, "42601", `option "on_error" not recognized`},
		{`COPY cf FROM STDIN (FORMAT csv, FORCE_QUOTE (a))`, "0A000", "COPY force quote only available using COPY TO"},
		{`COPY cf FROM STDIN (FORCE_NULL (a))`, "0A000", "COPY force null available only in CSV mode"},
		{`COPY cf FROM STDIN (FORCE_NOT_NULL (a))`, "0A000", "COPY force not null available only in CSV mode"},
		{`COPY cf (id) FROM STDIN (FORMAT csv, FORCE_NULL (a))`, "42P10", `FORCE_NULL column "a" not referenced by COPY`},
		{`COPY cf FROM STDIN (FORMAT csv, FORCE_NOT_NULL (zz))`, "42703", `column "zz" of relation "cf" does not exist`},
		{`COPY cf FROM STDIN (QUOTE '"')`, "0A000", "COPY quote available only in CSV mode"},
		{`COPY cf FROM STDIN (FORMAT csv, DELIMITER ',', QUOTE ',')`, "22023", "COPY delimiter and quote must be different"},
		{`COPY cf FROM STDIN (DELIMITER 'ab')`, "0A000", "COPY delimiter must be a single one-byte character"},
		{`COPY cf FROM STDIN (FORMAT binary, HEADER)`, "0A000", "cannot specify HEADER in BINARY mode"},
		{`COPY cf FROM STDIN (FORMAT binary, NULL 'x')`, "42601", "cannot specify NULL in BINARY mode"},
		{`COPY cf FROM STDIN (HEADER maybe)`, "42601", "header requires a Boolean value or \"match\""},
		{`COPY cf FROM STDIN (ENCODING 'nope')`, "22023", `argument to option "encoding" must be a valid encoding name`},
		{`COPY cf FROM STDIN (ENCODING 'LATIN1')`, "0A000", "UTF8 only"},
		{`COPY cf TO STDOUT (FORMAT csv, HEADER match)`, "0A000", `cannot use "match" with HEADER in COPY TO`},
	} {
		_, err := pc.Exec(ctx, c.stmt).ReadAll()
		var pgErr *pgconn.PgError
		if !errors.As(err, &pgErr) || pgErr.Code != c.code || !strings.Contains(pgErr.Message, c.msg) {
			t.Errorf("%s: got %v, want %s %q", c.stmt, err, c.code, c.msg)
		}
	}
	// The connection is still usable.
	if _, err := pc.Exec(ctx, `SELECT 1`).ReadAll(); err != nil {
		t.Fatal(err)
	}
}
