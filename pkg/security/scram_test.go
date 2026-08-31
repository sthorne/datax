package security

import (
	"crypto/pbkdf2"
	"crypto/sha256"
	"encoding/base64"
	"strings"
	"testing"
)

// TestRFC7677Vector replays the published SCRAM-SHA-256 example exchange
// (RFC 7677 section 3) against the server implementation.
func TestRFC7677Vector(t *testing.T) {
	salt, err := base64.StdEncoding.DecodeString("W22ZaJ0SNY7soEsUEjb6gQ==")
	if err != nil {
		t.Fatal(err)
	}
	v, err := makeVerifier("pencil", salt, 4096)
	if err != nil {
		t.Fatal(err)
	}
	s := NewScramServer(v)
	first, err := s.handleClientFirst(MechScram, "n,,n=user,r=rOprNGfwEbeRWgbNEkqO", "%hvYDpWUa2RaTCAfuxFIlj)hNlF$k0")
	if err != nil {
		t.Fatal(err)
	}
	wantFirst := "r=rOprNGfwEbeRWgbNEkqO%hvYDpWUa2RaTCAfuxFIlj)hNlF$k0,s=W22ZaJ0SNY7soEsUEjb6gQ==,i=4096"
	if first != wantFirst {
		t.Fatalf("server-first:\n got %s\nwant %s", first, wantFirst)
	}
	final, err := s.HandleClientFinal("c=biws,r=rOprNGfwEbeRWgbNEkqO%hvYDpWUa2RaTCAfuxFIlj)hNlF$k0,p=dHzbZapWIk4jUhN+Ute9ytag9zjfMHgsqmmiz7AndVQ=")
	if err != nil {
		t.Fatal(err)
	}
	if final != "v=6rriTRBi23WpRR/wtup+mMhUZUn/dB5nLTJRsjl95G4=" {
		t.Fatalf("server-final: %s", final)
	}
}

// TestScramWrongProof: a proof derived from the wrong password fails, with
// no distinguishable error detail.
func TestScramWrongProof(t *testing.T) {
	v, err := MakeScramVerifier("correct-password")
	if err != nil {
		t.Fatal(err)
	}
	s := NewScramServer(v)
	if _, err := s.HandleClientFirst(MechScram, "n,,n=user,r=clientnonce123456789"); err != nil {
		t.Fatal(err)
	}
	proof := base64.StdEncoding.EncodeToString(make([]byte, 32))
	_, err = s.HandleClientFinal("c=biws,r=" + s.fullNonce + ",p=" + proof)
	if err == nil || !strings.Contains(err.Error(), "proof verification failed") {
		t.Fatalf("bogus proof: %v", err)
	}
}

// TestVerifierRoundTrip: storage encoding survives, and two verifiers for
// the same password differ (fresh salts).
func TestVerifierRoundTrip(t *testing.T) {
	v1, err := MakeScramVerifier("hunter2")
	if err != nil {
		t.Fatal(err)
	}
	raw, err := MarshalVerifier(v1)
	if err != nil {
		t.Fatal(err)
	}
	v2, err := UnmarshalVerifier(raw)
	if err != nil {
		t.Fatal(err)
	}
	if string(v2.StoredKey) != string(v1.StoredKey) || v2.Iterations != v1.Iterations {
		t.Fatal("verifier round trip lost data")
	}
	v3, err := MakeScramVerifier("hunter2")
	if err != nil {
		t.Fatal(err)
	}
	if string(v3.Salt) == string(v1.Salt) {
		t.Fatal("two verifiers share a salt")
	}
}

// TestSASLprep: PRECIS OpaqueString mappings apply (RFC 4013's successor,
// and exactly what pgx applies client-side); inputs the profile rejects
// fall back to the raw string (PostgreSQL/pgx behavior).
func TestSASLprep(t *testing.T) {
	if got := SASLprep("a\u00a0b"); got != "a b" { // NBSP maps to space
		t.Fatalf("nbsp: %q", got)
	}
	if got := SASLprep("plain"); got != "plain" {
		t.Fatalf("ascii: %q", got)
	}
	if got := SASLprep("bad\x07ctl"); got != "bad\x07ctl" { // prohibited → raw
		t.Fatalf("control fallback: %q", got)
	}
	if got := SASLprep("bad\xffutf8"); got != "bad\xffutf8" { // invalid → raw
		t.Fatalf("utf8 fallback: %q", got)
	}
	// Verifier derivation uses the normalized form: proving with the
	// normalized password against a verifier made from the raw form works.
	salt := []byte("0123456789abcdef")
	vRaw, err := makeVerifier(SASLprep("pa\u00a0word"), salt, 4096)
	if err != nil {
		t.Fatal(err)
	}
	vNorm, err := makeVerifier("pa word", salt, 4096)
	if err != nil {
		t.Fatal(err)
	}
	if string(vRaw.StoredKey) != string(vNorm.StoredKey) {
		t.Fatal("SASLprep-equal passwords produced different verifiers")
	}
}

// scramClientFinal computes a client-final-message for tests.
func scramClientFinal(t *testing.T, password string, salt []byte, iters int, authPrefix, c, fullNonce string) string {
	t.Helper()
	salted, err := pbkdf2Key(password, salt, iters)
	if err != nil {
		t.Fatal(err)
	}
	clientKey := hmacSHA256(salted, "Client Key")
	stored := sha256.Sum256(clientKey)
	withoutProof := "c=" + c + ",r=" + fullNonce
	authMessage := authPrefix + "," + withoutProof
	sig := hmacSHA256(stored[:], authMessage)
	proof := make([]byte, len(clientKey))
	for i := range proof {
		proof[i] = clientKey[i] ^ sig[i]
	}
	return withoutProof + ",p=" + base64.StdEncoding.EncodeToString(proof)
}

func pbkdf2Key(password string, salt []byte, iters int) ([]byte, error) {
	return pbkdf2.Key(sha256.New, password, salt, iters, sha256.Size)
}

// TestScramPlusExchange: a full SCRAM-SHA-256-PLUS conversation with
// tls-server-end-point binding verifies; tampered binding data fails; a
// downgraded "y" gs2 flag is rejected when the server advertised -PLUS.
func TestScramPlusExchange(t *testing.T) {
	salt := []byte("0123456789abcdef")
	const iters = 4096
	v, err := makeVerifier("pencil", salt, iters)
	if err != nil {
		t.Fatal(err)
	}
	cb := []byte{0xde, 0xad, 0xbe, 0xef, 0x01}

	run := func(cbForClient []byte) error {
		s := NewScramServerTLS(v, cb)
		clientFirst := "p=tls-server-end-point,,n=user,r=clientnonceclientnonce"
		serverFirst, err := s.HandleClientFirst(MechScramPlus, clientFirst)
		if err != nil {
			return err
		}
		c := base64.StdEncoding.EncodeToString(append([]byte("p=tls-server-end-point,,"), cbForClient...))
		bare := "n=user,r=clientnonceclientnonce"
		final := scramClientFinal(t, "pencil", salt, iters, bare+","+serverFirst, c, s.fullNonce)
		_, err = s.HandleClientFinal(final)
		return err
	}
	if err := run(cb); err != nil {
		t.Fatalf("valid -PLUS exchange failed: %v", err)
	}
	if err := run([]byte{0x00}); err == nil || !strings.Contains(err.Error(), "channel binding") {
		t.Fatalf("tampered binding accepted: %v", err)
	}

	// Downgrade: server advertised -PLUS, client claims the server can't.
	s := NewScramServerTLS(v, cb)
	if _, err := s.HandleClientFirst(MechScram, "y,,n=user,r=abcabcabcabc"); err == nil ||
		!strings.Contains(err.Error(), "downgrade") {
		t.Fatalf("downgrade not rejected: %v", err)
	}
	// Plain mechanism with a "n" flag stays fine even when -PLUS was
	// advertised (a client without channel-binding support).
	s = NewScramServerTLS(v, cb)
	if _, err := s.HandleClientFirst(MechScram, "n,,n=user,r=abcabcabcabc"); err != nil {
		t.Fatalf("plain mechanism alongside -PLUS: %v", err)
	}
}

// TestScramChannelAttrVerified: the non-PLUS c= attribute must be the
// base64 gs2 header — a mismatched value fails even with a valid proof.
func TestScramChannelAttrVerified(t *testing.T) {
	salt := []byte("0123456789abcdef")
	v, err := makeVerifier("pencil", salt, 4096)
	if err != nil {
		t.Fatal(err)
	}
	s := NewScramServer(v)
	serverFirst, err := s.HandleClientFirst(MechScram, "n,,n=user,r=clientnonceclientnonce")
	if err != nil {
		t.Fatal(err)
	}
	bare := "n=user,r=clientnonceclientnonce"
	// c= claims "y,," but the gs2 header said "n,,".
	final := scramClientFinal(t, "pencil", salt, 4096, bare+","+serverFirst, "eSws", s.fullNonce)
	if _, err := s.HandleClientFinal(final); err == nil || !strings.Contains(err.Error(), "channel binding") {
		t.Fatalf("gs2/c= mismatch accepted: %v", err)
	}
}

// TestVerifyPassword: the non-interactive plaintext check agrees with the
// stored verifier — correct password passes, wrong/empty fail, non-ASCII
// passwords match via SASLprep normalization, and the dummy verifier
// matches nothing (not even the phrase it was derived from).
func TestVerifyPassword(t *testing.T) {
	v, err := MakeScramVerifier("pencil")
	if err != nil {
		t.Fatal(err)
	}
	if !VerifyPassword(v, "pencil") {
		t.Fatal("correct password rejected")
	}
	// (No "pencil\x00" case: PBKDF2 keys HMAC with the password and HMAC
	// zero-pads short keys, so trailing NULs are equivalent by
	// construction — PostgreSQL shares the property.)
	for _, bad := range []string{"Pencil", "pencil ", "", "encil"} {
		if VerifyPassword(v, bad) {
			t.Fatalf("wrong password %q accepted", bad)
		}
	}
	if VerifyPassword(nil, "pencil") {
		t.Fatal("nil verifier accepted a password")
	}

	// SASLprep: a password stored with a non-breaking space verifies from
	// the plain-space form (both normalize to the same string).
	nb, err := MakeScramVerifier("pass word")
	if err != nil {
		t.Fatal(err)
	}
	if !VerifyPassword(nb, "pass word") {
		t.Fatal("SASLprep-equivalent password rejected")
	}

	d := DummyVerifier()
	for _, pw := range []string{"this-password-can-never-verify", "", "x"} {
		if VerifyPassword(d, pw) {
			t.Fatalf("dummy verifier accepted %q", pw)
		}
	}
}
