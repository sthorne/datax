package parser

import (
	"strings"
	"testing"
)

// TestCopyTo: COPY ... TO in its table and query forms, every target,
// and both option spellings (issue #365).
func TestCopyTo(t *testing.T) {
	parse1 := func(t *testing.T, src string) *CopyTo {
		t.Helper()
		stmts, err := Parse(src)
		if err != nil {
			t.Fatalf("%s: %v", src, err)
		}
		ct, ok := stmts[0].(*CopyTo)
		if !ok {
			t.Fatalf("%s: %T", src, stmts[0])
		}
		return ct
	}
	opts := func(ct *CopyTo) string {
		var parts []string
		for _, o := range ct.Options {
			s := o.Name
			switch {
			case o.HasArg:
				s += "=" + o.Arg
			case o.Star:
				s += "=*"
			case o.HasList:
				s += "=(" + strings.Join(o.List, ",") + ")"
			}
			parts = append(parts, s)
		}
		return strings.Join(parts, " ")
	}
	for _, c := range []struct{ src, table, cols, opts, file string }{
		{`COPY t TO STDOUT`, "t", "", "", ""},
		{`copy public.t (a, b) to stdout;`, "public.t", "a,b", "", ""},
		{`COPY t TO STDIN`, "t", "", "", ""},
		{`COPY t TO STDOUT WITH (FORMAT csv, HEADER, DELIMITER ';', NULL 'n', QUOTE '''', ESCAPE '\', FORCE_QUOTE (a, b), ENCODING 'UTF8')`, "t", "",
			`format=csv header delimiter=; null=n quote=' escape=\ force_quote=(a,b) encoding=UTF8`, ""},
		{`COPY t TO STDOUT (FORMAT 'binary', HEADER false, FREEZE true, force_quote *, null on)`, "t", "", "format=binary header=false freeze=true force_quote=* null=on", ""},
		{`COPY t TO STDOUT WITH CSV HEADER`, "t", "", "format=csv header", ""},
		{`COPY t TO STDOUT CSV DELIMITER AS ';' NULL AS 'x' QUOTE '"' ESCAPE AS '\' FORCE QUOTE a, b`, "t", "",
			`format=csv delimiter=; null=x quote=" escape=\ force_quote=(a,b)`, ""},
		{`COPY t TO STDOUT WITH CSV FORCE QUOTE *`, "t", "", "format=csv force_quote=*", ""},
		{`COPY t TO STDOUT CSV FORCE NOT NULL a`, "t", "", "format=csv force_not_null=(a)", ""},
		{`COPY t TO STDOUT CSV FORCE NULL a, b ENCODING 'latin1' FREEZE`, "t", "", "format=csv force_null=(a,b) encoding=latin1 freeze", ""},
		{`COPY BINARY t TO STDOUT`, "t", "", "format=binary", ""},
		{`COPY t TO STDOUT BINARY`, "t", "", "format=binary", ""},
		{`COPY t TO '/tmp/f'`, "t", "", "", "/tmp/f"},
	} {
		ct := parse1(t, c.src)
		if ct.Table != c.table || strings.Join(ct.Columns, ",") != c.cols || opts(ct) != c.opts || ct.File != c.file || ct.Query != nil || ct.Program {
			t.Fatalf("%s: table %q cols %v opts %q file %q", c.src, ct.Table, ct.Columns, opts(ct), ct.File)
		}
	}
	ct := parse1(t, `COPY t TO PROGRAM 'gzip > /tmp/x'`)
	if !ct.Program || ct.File != "gzip > /tmp/x" {
		t.Fatalf("program: %+v", ct)
	}
	for _, q := range []string{
		`COPY (SELECT a, b FROM t WHERE a > 1 ORDER BY a) TO STDOUT`,
		`COPY (VALUES (1), (2)) TO STDOUT WITH (FORMAT csv)`,
		`COPY (WITH x AS (SELECT 1 AS a) SELECT a FROM x) TO STDOUT`,
		`COPY (INSERT INTO t VALUES (1) RETURNING a) TO STDOUT`,
		`COPY (DELETE FROM t RETURNING *) TO STDOUT`,
		`COPY (UPDATE t SET a = 2 RETURNING a) TO STDOUT`,
		`COPY (SELECT $1) TO STDOUT`,
	} {
		if ct := parse1(t, q); ct.Query == nil || ct.Table != "" {
			t.Fatalf("%s: %+v", q, ct)
		}
	}
	// The utility statement takes no parameters of its own: a $n in its
	// query is the session's 42P02, not a Bind parameter.
	if n := CountParams(parse1(t, `COPY (SELECT $1) TO STDOUT`)); n != 0 {
		t.Fatalf("COPY (SELECT $1) counts %d parameters", n)
	}
	for _, bad := range []string{
		`COPY (SELECT 1) FROM STDIN`,
		`COPY (CREATE TABLE x (a INT)) TO STDOUT`,
		`COPY t TO STDOUT WITH OIDS`,
		`COPY t TO`,
		`COPY t TO STDOUT WITH (FORMAT csv`,
		`COPY t TO PROGRAM`,
		`COPY (SELECT 1) TO STDOUT WITH (,)`,
		`COPY t TO STDOUT CSV QUOTE`,
	} {
		if _, err := Parse(bad); err == nil {
			t.Fatalf("%q parsed", bad)
		}
	}
}
