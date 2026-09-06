package pgwire

import "testing"

// TestFingerprint: statements of one shape share one key, and the shape
// keeps enough of the statement to be recognised — the table it names
// above all (issue #154).
func TestFingerprint(t *testing.T) {
	for _, tc := range []struct{ in, want string }{
		{"UPDATE accounts SET balance = 5 WHERE id = 3", "UPDATE accounts SET balance = ? WHERE id = ?"},
		{"UPDATE accounts SET balance = 91 WHERE id = 7", "UPDATE accounts SET balance = ? WHERE id = ?"},
		{"UPDATE accounts SET balance = $1 WHERE id = $2", "UPDATE accounts SET balance = ? WHERE id = ?"},
		// Whitespace is not a shape.
		{"SELECT  *\n  FROM t\tWHERE a = 1", "SELECT * FROM t WHERE a = ?"},
		// A list of three and a list of one are one shape.
		{"SELECT * FROM t WHERE id IN (1, 2, 3)", "SELECT * FROM t WHERE id IN (?)"},
		{"SELECT * FROM t WHERE id IN (9)", "SELECT * FROM t WHERE id IN (?)"},
		// Strings collapse; quoted identifiers do not.
		{`INSERT INTO "Orders" (name) VALUES ('anvil')`, `INSERT INTO "Orders" (name) VALUES (?)`},
		{`INSERT INTO "Orders" (name) VALUES ('rope')`, `INSERT INTO "Orders" (name) VALUES (?)`},
		// A quote inside a string does not end it.
		{`SELECT * FROM t WHERE s = 'it''s'`, "SELECT * FROM t WHERE s = ?"},
		{`SELECT * FROM t WHERE s = 'a\'b'`, "SELECT * FROM t WHERE s = ?"},
		// Digits inside an identifier are part of it, not a literal.
		{"SELECT a1 FROM t2 WHERE b3 = 4", "SELECT a1 FROM t2 WHERE b3 = ?"},
		// Floats and exponents are one literal each.
		{"SELECT * FROM t WHERE v > 1.5e-3", "SELECT * FROM t WHERE v > ?"},
	} {
		if got := Fingerprint(tc.in); got != tc.want {
			t.Errorf("Fingerprint(%q)\n got %q\nwant %q", tc.in, got, tc.want)
		}
	}
}

// Two statements that differ only in their literals must land on one
// key, and two that differ in the table they touch must not — that is
// the whole point of the hot list.
func TestFingerprintGroups(t *testing.T) {
	a := Fingerprint("UPDATE accounts SET balance = balance - 7 WHERE id = 1")
	b := Fingerprint("UPDATE accounts SET balance = balance - 7 WHERE id = 12")
	c := Fingerprint("UPDATE items SET balance = balance - 7 WHERE id = 1")
	if a != b {
		t.Errorf("same shape, different keys: %q vs %q", a, b)
	}
	if a == c {
		t.Errorf("different tables collapsed to one key: %q", a)
	}
}

// An unterminated string must not run off the end of the input.
func TestFingerprintUnterminated(t *testing.T) {
	if got := Fingerprint("SELECT * FROM t WHERE s = 'oops"); got != "SELECT * FROM t WHERE s = ?" {
		t.Errorf("got %q", got)
	}
	if got := Fingerprint(`SELECT * FROM "oops`); got != `SELECT * FROM "oops` {
		t.Errorf("got %q", got)
	}
}
