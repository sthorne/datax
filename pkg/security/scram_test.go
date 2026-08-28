package security

import (
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
	first, err := s.handleClientFirst("n,,n=user,r=rOprNGfwEbeRWgbNEkqO", "%hvYDpWUa2RaTCAfuxFIlj)hNlF$k0")
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
	if _, err := s.HandleClientFirst("n,,n=user,r=clientnonce123456789"); err != nil {
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
