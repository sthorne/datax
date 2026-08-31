package decimal

import (
	"math/big"
	"math/rand/v2"
	"strconv"
	"testing"
)

func mustParse(t *testing.T, s string) Dec {
	t.Helper()
	d, err := Parse(s)
	if err != nil {
		t.Fatalf("Parse(%q): %v", s, err)
	}
	return d
}

func TestParseAndCanonicalString(t *testing.T) {
	cases := map[string]string{
		"0":            "0",
		"-0":           "0",
		"0.0":          "0",
		"000.000":      "0",
		"1":            "1",
		"01.10":        "1.1",
		"-1.5":         "-1.5",
		"12000":        "12000",
		"1.2e4":        "12000",
		"1.2E-3":       "0.0012",
		"-0.000120":    "-0.00012",
		".5":           "0.5",
		"5.":           "5",
		"+3.14":        "3.14",
		"12345.678900": "12345.6789",
	}
	for in, want := range cases {
		if got := mustParse(t, in).String(); got != want {
			t.Fatalf("Parse(%q).String() = %q, want %q", in, got, want)
		}
	}
	for _, bad := range []string{"", ".", "e5", "1..2", "NaN", "Infinity", "1e999999", "--1", "1e", "0x10"} {
		if _, err := Parse(bad); err == nil {
			t.Fatalf("Parse(%q) accepted", bad)
		}
	}
}

func TestExactArithmetic(t *testing.T) {
	// The float-breaking classic.
	sum := Add(mustParse(t, "0.1"), mustParse(t, "0.2"))
	if sum.String() != "0.3" {
		t.Fatalf("0.1+0.2 = %s", sum.String())
	}
	if Cmp(sum, mustParse(t, "0.3")) != 0 {
		t.Fatal("0.1+0.2 != 0.3")
	}
	if got := Sub(mustParse(t, "1"), mustParse(t, "0.999999999999999999999")).String(); got != "0.000000000000000000001" {
		t.Fatalf("high-precision sub: %s", got)
	}
	if got := Mul(mustParse(t, "1.5"), mustParse(t, "-2.4")).String(); got != "-3.6" {
		t.Fatalf("mul: %s", got)
	}
	if got := Neg(mustParse(t, "-7.25")).String(); got != "7.25" {
		t.Fatalf("neg: %s", got)
	}
}

func TestDivQuantize(t *testing.T) {
	cases := []struct{ a, b, want string }{
		{"1", "3", "0.333333"},
		{"2", "3", "0.666667"},
		{"1", "8", "0.125"},
		{"-1", "8", "-0.125"},
		{"5", "2", "2.5"},
		{"0.05", "0.2", "0.25"},
		// Half-even at scale 6: 0.0000025/1 → 2.5e-6 rounds to even 2e-6...
		{"0.0000025", "1", "0.000002"},
		{"0.0000035", "1", "0.000004"},
	}
	for _, tc := range cases {
		got, err := DivQuantize(mustParse(t, tc.a), mustParse(t, tc.b), 6)
		if err != nil {
			t.Fatal(err)
		}
		if got.String() != tc.want {
			t.Fatalf("%s/%s = %s, want %s", tc.a, tc.b, got.String(), tc.want)
		}
	}
	if _, err := DivQuantize(mustParse(t, "1"), mustParse(t, "0"), 6); err == nil {
		t.Fatal("division by zero accepted")
	}
}

// TestCmpAgainstRatOracle: random decimals compare identically to
// math/big.Rat.
func TestCmpAgainstRatOracle(t *testing.T) {
	rng := rand.New(rand.NewPCG(1, 2))
	randDec := func() (Dec, *big.Rat) {
		coeff := rng.Int64N(2_000_000_000) - 1_000_000_000
		exp := int32(rng.Int64N(21) - 10)
		d := New(coeff, exp)
		r := new(big.Rat).SetInt64(coeff)
		ten := big.NewRat(10, 1)
		for i := int32(0); i < exp; i++ {
			r.Mul(r, ten)
		}
		for i := exp; i < 0; i++ {
			r.Quo(r, ten)
		}
		return d, r
	}
	for i := 0; i < 2000; i++ {
		a, ra := randDec()
		b, rb := randDec()
		if got, want := Cmp(a, b), ra.Cmp(rb); got != want {
			t.Fatalf("Cmp(%s, %s) = %d, want %d", a, b, got, want)
		}
		// String round-trips.
		back := mustParse(t, a.String())
		if Cmp(a, back) != 0 || back.String() != a.String() {
			t.Fatalf("round-trip %s -> %s", a, back)
		}
	}
}

func TestMantissaRoundTrip(t *testing.T) {
	for _, s := range []string{"0", "1", "-1", "0.5", "-0.055", "12000", "123.456", "-99999999999999999999.000001"} {
		d := mustParse(t, s)
		neg, digits, e := d.Mantissa()
		back, err := FromMantissa(neg, digits, e)
		if err != nil {
			t.Fatal(err)
		}
		if Cmp(d, back) != 0 || back.String() != d.String() {
			t.Fatalf("mantissa round-trip %s -> %s", d, back)
		}
	}
}

// TestFloat64CorrectlyRounded: Float64 must match Go's own parse of the
// same text — one ULP off turns a stored FLOAT8 0.42 into
// 0.42000000000000004 on read.
func TestFloat64CorrectlyRounded(t *testing.T) {
	for _, s := range []string{
		"0.42", "0.57", "0.1", "0.2", "0.3", "123.456", "-9.99",
		"1e300", "-1e-300", "2.2250738585072014e-308", "0",
	} {
		d, err := Parse(s)
		if err != nil {
			t.Fatalf("parse %q: %v", s, err)
		}
		want, err := strconv.ParseFloat(s, 64)
		if err != nil {
			t.Fatalf("reference parse %q: %v", s, err)
		}
		if got := d.Float64(); got != want {
			t.Fatalf("Float64(%q) = %v, want %v", s, got, want)
		}
	}
}
