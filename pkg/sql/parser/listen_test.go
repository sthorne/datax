package parser

import "testing"

// TestParseListenNotify: LISTEN, UNLISTEN and NOTIFY (issue #297). A
// channel is an identifier — folded unless quoted, and allowed to be a
// word the grammar reserves elsewhere; a payload is a string literal.
func TestParseListenNotify(t *testing.T) {
	if l := parseOne(t, `LISTEN Jobs`).(*Listen); l.Channel != "jobs" {
		t.Fatalf("listen: %+v", l)
	}
	if l := parseOne(t, `listen "Jobs"`).(*Listen); l.Channel != "Jobs" {
		t.Fatalf("listen quoted: %+v", l)
	}
	if l := parseOne(t, `LISTEN key`).(*Listen); l.Channel != "key" {
		t.Fatalf("listen on a keyword: %+v", l)
	}
	if u := parseOne(t, `UNLISTEN jobs`).(*Unlisten); u.Channel != "jobs" || u.All {
		t.Fatalf("unlisten: %+v", u)
	}
	if u := parseOne(t, `UNLISTEN *`).(*Unlisten); !u.All || u.Channel != "" {
		t.Fatalf("unlisten *: %+v", u)
	}
	if n := parseOne(t, `NOTIFY jobs`).(*Notify); n.Channel != "jobs" || n.Payload != "" {
		t.Fatalf("notify: %+v", n)
	}
	if n := parseOne(t, `NOTIFY jobs, 'it''s here'`).(*Notify); n.Channel != "jobs" || n.Payload != "it's here" {
		t.Fatalf("notify with payload: %+v", n)
	}
	stmts, err := Parse(`LISTEN a; NOTIFY a, 'x'; UNLISTEN *`)
	if err != nil || len(stmts) != 3 {
		t.Fatalf("three statements: %v %v", stmts, err)
	}
	for _, bad := range []string{
		`LISTEN`,
		`LISTEN 'jobs'`,
		`LISTEN a, b`,
		`LISTEN *`,
		`UNLISTEN`,
		`NOTIFY`,
		`NOTIFY jobs,`,
		`NOTIFY jobs, payload`,
		`NOTIFY jobs, $1`,
		`NOTIFY jobs 'x'`,
	} {
		if _, err := Parse(bad); err == nil {
			t.Fatalf("%q parsed", bad)
		}
	}
	// The payload is a value: it is not part of the statement's shape.
	for sql, want := range map[string]string{
		`LISTEN jobs`:               "LISTEN",
		`UNLISTEN *`:                "UNLISTEN",
		`NOTIFY jobs, 'card 4111'`:  "NOTIFY",
		`SELECT pg_notify('a','b')`: "SELECT pg_notify(?, ?)",
	} {
		if got := Fingerprint(parseOne(t, sql)).Text; got != want {
			t.Errorf("fingerprint of %q = %q, want %q", sql, got, want)
		}
	}
}
