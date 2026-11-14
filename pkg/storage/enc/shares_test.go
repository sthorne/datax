package enc

import (
	"bytes"
	"context"
	"crypto/rand"
	"strings"
	"testing"
	"time"
)

// cycle is a reader that repeats its bytes: the polynomials' coefficients
// when a test needs to know them.
type cycle struct {
	b []byte
	i int
}

func (c *cycle) Read(p []byte) (int, error) {
	for i := range p {
		p[i] = c.b[c.i%len(c.b)]
		c.i++
	}
	return len(p), nil
}

func combos(n, k int) [][]int {
	var out [][]int
	var rec func(start int, cur []int)
	rec = func(start int, cur []int) {
		if len(cur) == k {
			out = append(out, append([]int(nil), cur...))
			return
		}
		for i := start; i < n; i++ {
			rec(i+1, append(cur, i))
		}
	}
	rec(0, nil)
	return out
}

func randomKey(t *testing.T) []byte {
	t.Helper()
	k := make([]byte, KeyLen)
	if _, err := rand.Read(k); err != nil {
		t.Fatal(err)
	}
	return k
}

// TestShareArithmeticKnownAnswers pins the field and the polynomial
// evaluation to values worked out independently of this code: the
// product is the one FIPS 197 gives as its example, and the shares are
// of the byte 0x53 under the polynomial 0x53 + 0xca·x + 0x11·x².
func TestShareArithmeticKnownAnswers(t *testing.T) {
	if got := gfMul(0x57, 0x83); got != 0xc1 {
		t.Fatalf("0x57·0x83 = %#x, want 0xc1", got)
	}
	for a := 1; a < 256; a++ {
		if got := gfMul(byte(a), gfInv(byte(a))); got != 1 {
			t.Fatalf("%#x times its inverse = %#x", a, got)
		}
	}
	key := bytes.Repeat([]byte{0x53}, KeyLen)
	shares, err := SplitRootKey(&cycle{b: []byte{0xca, 0x11}}, key, 3, 5)
	if err != nil {
		t.Fatal(err)
	}
	want := []byte{0x88, 0x98, 0x43, 0x5d, 0x86}
	for i, s := range shares {
		for b := range s.value {
			if s.value[b] != want[i] {
				t.Fatalf("share %d byte %d = %#x, want %#x", i+1, b, s.value[b], want[i])
			}
		}
	}
	got, err := CombineShares([]Share{shares[4], shares[1], shares[2]})
	if err != nil || !bytes.Equal(got, key) {
		t.Fatalf("combining shares 5, 2, 3: %x, %v", got, err)
	}
}

// TestAnyThresholdOfSharesRebuildTheKey: every choice of k shares out of
// n gives the key back, in any order, through the text form a holder
// keeps.
func TestAnyThresholdOfSharesRebuildTheKey(t *testing.T) {
	for _, c := range []struct{ k, n int }{{2, 2}, {2, 3}, {3, 5}, {5, 7}, {2, 255}, {255, 255}} {
		key := randomKey(t)
		shares, err := SplitRootKey(nil, key, c.k, c.n)
		if err != nil {
			t.Fatal(err)
		}
		sets := [][]int{}
		if c.n <= 7 {
			sets = combos(c.n, c.k)
		} else {
			first, last := make([]int, c.k), make([]int, c.k)
			for i := range first {
				first[i], last[i] = i, c.n-1-i
			}
			sets = append(sets, first, last)
		}
		for _, set := range sets {
			var use []Share
			for _, i := range set {
				s, err := ParseShare(strings.ToUpper(shares[i].String()) + "\n")
				if err != nil {
					t.Fatalf("%d/%d share %d does not read back: %v", c.k, c.n, i+1, err)
				}
				use = append([]Share{s}, use...)
			}
			got, err := CombineShares(use)
			if err != nil || !bytes.Equal(got, key) {
				t.Fatalf("%d/%d shares %v: %v", c.k, c.n, set, err)
			}
			if _, err := CombineShares(use[1:]); err == nil {
				t.Fatalf("%d/%d: %d shares combined", c.k, c.n, c.k-1)
			}
		}
	}
}

// TestFewerThanThresholdSharesFixNothing: k-1 shares are consistent with
// every key. For any other key at all there is a k-th share that, with
// the k-1 an attacker holds, rebuilds that key instead — so the k-1
// cannot favour one key over another.
func TestFewerThanThresholdSharesFixNothing(t *testing.T) {
	for _, c := range []struct{ k, n int }{{2, 3}, {3, 5}, {4, 4}} {
		key := randomKey(t)
		shares, err := SplitRootKey(nil, key, c.k, c.n)
		if err != nil {
			t.Fatal(err)
		}
		held := shares[:c.k-1]
		for trial := 0; trial < 20; trial++ {
			other := randomKey(t)
			forged := Share{Threshold: c.k, Total: c.n, Index: c.k, KeyID: shares[0].KeyID}
			set := append(append([]Share(nil), held...), forged)
			partial := interpolateAtZero(set) // the forged share is all zeroes: the held shares' part alone
			w := lagrangeWeight(set, c.k-1)
			for b := range forged.value {
				forged.value[b] = gfMul(partial[b]^other[b], gfInv(w))
			}
			set[c.k-1] = forged
			if got := interpolateAtZero(set); !bytes.Equal(got, other) {
				t.Fatalf("%d/%d: no share completes the held ones to an arbitrary key", c.k, c.n)
			}
			// And that is why the key's id is checked: the forged share is
			// well-formed, and only the id says the result is not the key.
			if _, err := CombineShares(set); err == nil || !strings.Contains(err.Error(), "do not rebuild the key they name") {
				t.Fatalf("a forged share was combined: %v", err)
			}
		}
	}
}

func TestSplitAndShareTextRefusals(t *testing.T) {
	key := randomKey(t)
	for _, c := range []struct{ k, n int }{{1, 3}, {0, 0}, {4, 3}, {2, 256}} {
		if _, err := SplitRootKey(nil, key, c.k, c.n); err == nil {
			t.Errorf("split %d/%d was accepted", c.k, c.n)
		}
	}
	if _, err := SplitRootKey(nil, key[:16], 2, 3); err == nil {
		t.Error("a 16-byte root key was split")
	}
	shares, err := SplitRootKey(nil, key, 2, 3)
	if err != nil {
		t.Fatal(err)
	}
	good := shares[0].String()
	if !strings.HasPrefix(good, SharePrefix) || strings.Contains(good, "=") {
		t.Fatalf("share text %q", good)
	}
	// One character changed anywhere after the prefix is caught, and the
	// refusal does not repeat what was typed.
	for i := len(SharePrefix); i < len(good); i++ {
		c := byte('a')
		if good[i] == 'a' {
			c = 'b'
		}
		bad := good[:i] + string(c) + good[i+1:]
		_, err := ParseShare(bad)
		if err == nil {
			t.Fatalf("a share with character %d changed was read", i)
		}
		if strings.Contains(err.Error(), bad[len(SharePrefix):len(SharePrefix)+12]) {
			t.Fatalf("the refusal repeats the share: %v", err)
		}
	}
	for _, bad := range []string{"", "hunter2", good[:len(good)-3], good + "aa", "datax-share-!!!"} {
		if _, err := ParseShare(bad); err == nil {
			t.Errorf("%q was read as a share", bad)
		}
	}
}

// TestShareRootUnsealsOnAQuorum walks a sealed root through the refusals
// the issue names — one share, a repeated share, a share of another key,
// a damaged share — and then a quorum.
func TestShareRootUnsealsOnAQuorum(t *testing.T) {
	ctx := context.Background()
	key := randomKey(t)
	shares, err := SplitRootKey(nil, key, 2, 3)
	if err != nil {
		t.Fatal(err)
	}
	foreign, err := SplitRootKey(nil, randomKey(t), 2, 3)
	if err != nil {
		t.Fatal(err)
	}
	resplit, err := SplitRootKey(nil, key, 3, 4)
	if err != nil {
		t.Fatal(err)
	}

	open := NewShareRoot()
	if err := open.Open(key); err != nil {
		t.Fatal(err)
	}
	wrapped, err := open.Wrap(ctx, []byte("a store key, thirty-two bytes..!"))
	if err != nil {
		t.Fatal(err)
	}

	for _, pair := range combos(3, 2) {
		r := NewShareRoot()
		r.Expect(RootKeyID(key))
		if _, err := r.Unwrap(ctx, wrapped); err != ErrSealed {
			t.Fatalf("a sealed root unwrapped: %v", err)
		}
		if _, err := r.Wrap(ctx, key); err != ErrSealed {
			t.Fatalf("a sealed root wrapped: %v", err)
		}
		if _, err := r.Submit(foreign[0].String()); err == nil || !strings.Contains(err.Error(), "share of root key") {
			t.Fatalf("a share of another key: %v", err)
		}
		st, err := r.Submit(shares[pair[0]].String())
		if err != nil || !st.Sealed || st.Have != 1 || st.Need != 2 || len(st.Indexes) != 1 || st.Indexes[0] != pair[0]+1 {
			t.Fatalf("first share: %+v, %v", st, err)
		}
		if _, err := r.Submit(shares[pair[0]].String()); err == nil || !strings.Contains(err.Error(), "already been submitted") {
			t.Fatalf("a repeated share: %v", err)
		}
		damaged := shares[pair[1]].String()
		damaged = damaged[:len(damaged)-1] + map[bool]string{true: "b", false: "a"}[damaged[len(damaged)-1] == 'a']
		if _, err := r.Submit(damaged); err == nil || !strings.Contains(err.Error(), "mistyped or damaged") {
			t.Fatalf("a damaged share: %v", err)
		}
		if _, err := r.Submit(resplit[3].String()); err == nil || !strings.Contains(err.Error(), "different split") {
			t.Fatalf("a share of another split: %v", err)
		}
		if st := r.Status(); !st.Sealed || st.Have != 1 {
			t.Fatalf("refusals moved the count: %+v", st)
		}
		select {
		case <-r.Opened():
			t.Fatal("opened on one share")
		default:
		}
		st, err = r.Submit(shares[pair[1]].String())
		if err != nil || st.Sealed {
			t.Fatalf("second share: %+v, %v", st, err)
		}
		<-r.Opened()
		got, err := r.Unwrap(ctx, wrapped)
		if err != nil || string(got) != "a store key, thirty-two bytes..!" {
			t.Fatalf("shares %v: unwrap after unseal: %q, %v", pair, got, err)
		}
		if _, err := r.Submit(shares[pair[0]].String()); err == nil {
			t.Fatal("an unsealed root took a share")
		}
		// A new split of the key it holds opens what the old one did.
		again, err := r.Resplit(nil, 3, 5)
		if err != nil {
			t.Fatal(err)
		}
		if k, err := CombineShares(again[1:4]); err != nil || !bytes.Equal(k, key) {
			t.Fatalf("resplit: %v", err)
		}
	}

	// A root with no store to say which key it waits for takes the first
	// share's word, and holds the rest to it.
	r := NewShareRoot()
	if _, err := r.Submit(shares[0].String()); err != nil {
		t.Fatal(err)
	}
	if _, err := r.Submit(foreign[1].String()); err == nil {
		t.Fatal("a share of a second key was taken")
	}
	if r.KeyID() != RootKeyID(key) {
		t.Fatalf("key id %q", r.KeyID())
	}

	// A forged share that names the key and passes its own check: the
	// quorum is refused rather than opened on a wrong key, and the holders
	// start again.
	r = NewShareRoot()
	forged := shares[1]
	forged.value[7] ^= 0x40
	if _, err := r.Submit(shares[0].String()); err != nil {
		t.Fatal(err)
	}
	if st, err := r.Submit(forged.String()); err == nil || !st.Sealed || st.Have != 0 {
		t.Fatalf("a forged quorum: %+v, %v", st, err)
	}
	if _, err := r.Unwrap(ctx, wrapped); err != ErrSealed {
		t.Fatalf("after a forged quorum: %v", err)
	}
}

func TestShareRootLimitsRefusedSubmissions(t *testing.T) {
	r := NewShareRoot()
	now := time.Unix(1_700_000_000, 0)
	r.now = func() time.Time { return now }
	shares, err := SplitRootKey(nil, randomKey(t), 2, 2)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < shareFailureLimit; i++ {
		if _, err := r.Submit("nonsense"); err == nil || strings.Contains(err.Error(), "too many") {
			t.Fatalf("attempt %d: %v", i, err)
		}
	}
	if _, err := r.Submit(shares[0].String()); err == nil || !strings.Contains(err.Error(), "too many") {
		t.Fatalf("past the limit a share was taken: %v", err)
	}
	now = now.Add(shareFailureWindow + time.Second)
	if _, err := r.Submit(shares[0].String()); err != nil {
		t.Fatalf("after the window: %v", err)
	}
	if st, err := r.Submit(shares[1].String()); err != nil || st.Sealed {
		t.Fatalf("quorum after the window: %+v, %v", st, err)
	}
}

func TestParseRootKnowsShares(t *testing.T) {
	r, err := ParseRoot(" shares ")
	if err != nil {
		t.Fatal(err)
	}
	s, ok := r.(*ShareRoot)
	if !ok || !s.Sealed() || r.Kind() != "shares" {
		t.Fatalf("%T %v", r, r)
	}
	if _, err := ParseRoot("shares:2/3"); err == nil {
		t.Fatal("shares took an argument: the split is made by `datax debug enc-root split`, not named at start")
	}
}
