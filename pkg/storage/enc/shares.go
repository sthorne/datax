package enc

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/base32"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"sort"
	"strings"
	"sync"
	"time"
)

// The operator-share root (issues #292, #306).
//
// The file and exec roots keep the root key somewhere: a file, a device,
// a service. This one keeps it nowhere. The root key is made once, split
// into n shares of which any k rebuild it (Shamir's scheme), the shares
// are handed to people, and the key is forgotten. A node started with
// this root has nothing to unwrap its store key with: it starts SEALED,
// listening but with its store closed, and stays that way until k
// share-holders have each given it a share. Then it rebuilds the root
// key in memory, unwraps its store key, and starts as any node does.
//
// What that buys: a stolen disk, a stolen backup of every disk, or a
// stolen machine that has been powered off opens nothing, and no single
// person can open it either. What it costs: a cold start needs k people
// (issue #307 lets a running cluster vouch for a restarting node, which
// leaves the people for the case where everything is down).
//
// The scheme. Each byte of the key is the constant term of its own
// random polynomial of degree k-1 over GF(256); share x holds each
// polynomial's value at x. Any k points fix a polynomial of degree k-1,
// so k shares give the constant terms back; k-1 points are consistent
// with every possible constant term equally, so k-1 shares say nothing
// about the key at all — not "little", nothing.
//
// A share also carries what lets a mistake be caught: a check over its
// own bytes, so a mistyped share is refused when it is entered, and an
// identifier of the root key it is a share of, so a share of another
// key is refused when it is entered and a set that rebuilds the wrong
// key can never be mistaken for one that rebuilt the right one. The
// identifier is a hash of the key under a label of its own: the key is
// 256 random bits, and the hash narrows that by nothing an attacker can
// use.

const (
	shareVersion = 1
	// SharePrefix starts every share's text, so one is recognisable in a
	// password manager and to a secret scanner.
	SharePrefix = "datax-share-"
	shareIDLen  = 8
	// version, threshold, total, index, key id, value, check
	shareLen      = 4 + shareIDLen + KeyLen + shareCheckLen
	shareCheckLen = 4
	// shareRootMagic prefixes a store key wrapped by a share root.
	shareRootMagic = "DXS1"
	// MaxShares is the most shares a key can be split into: a share's
	// index is a non-zero byte.
	MaxShares = 255
)

// ErrSealed is returned by a share root asked to wrap or unwrap before
// enough shares have been given to it.
var ErrSealed = errors.New("sealed: the root key has not been rebuilt (not enough shares have been submitted)")

var shareEncoding = base32.StdEncoding.WithPadding(base32.NoPadding)

// Share is one share of a root key.
type Share struct {
	// Threshold is how many shares rebuild the key, Total how many the
	// key was split into, Index which of them this is (1..Total).
	Threshold, Total, Index int
	// KeyID identifies the root key (RootKeyID), in hex.
	KeyID string
	value [KeyLen]byte
}

// RootKeyID names a root key without revealing it.
func RootKeyID(key []byte) string {
	sum := sha256.Sum256(append([]byte("datax share root key id v1\x00"), key...))
	return hex.EncodeToString(sum[:shareIDLen])
}

func shareCheck(body []byte) []byte {
	sum := sha256.Sum256(append([]byte("datax share check v1\x00"), body...))
	return sum[:shareCheckLen]
}

// String is the share as its holder keeps it.
func (s Share) String() string {
	b := make([]byte, 0, shareLen)
	b = append(b, shareVersion, byte(s.Threshold), byte(s.Total), byte(s.Index))
	id, _ := hex.DecodeString(s.KeyID)
	b = append(b, id...)
	b = append(b, s.value[:]...)
	b = append(b, shareCheck(b)...)
	return SharePrefix + strings.ToLower(shareEncoding.EncodeToString(b))
}

// ParseShare reads a share as String wrote it. Case and surrounding
// space do not matter. The errors never repeat the input: a share that
// was mistyped is still most of a share.
func ParseShare(text string) (Share, error) {
	t := strings.ToLower(strings.TrimSpace(text))
	rest, ok := strings.CutPrefix(t, SharePrefix)
	if !ok {
		return Share{}, fmt.Errorf("not a share: it does not begin with %q", SharePrefix)
	}
	b, err := shareEncoding.DecodeString(strings.ToUpper(rest))
	// The last character carries one bit that is not part of the share;
	// a text that sets it decodes to the same bytes and is still not what
	// was written down.
	if err != nil || len(b) != shareLen || strings.ToLower(shareEncoding.EncodeToString(b)) != rest {
		return Share{}, errors.New("not a share: it is the wrong length or has characters a share cannot (check it was copied whole)")
	}
	body, check := b[:shareLen-shareCheckLen], b[shareLen-shareCheckLen:]
	if subtle.ConstantTimeCompare(shareCheck(body), check) != 1 {
		return Share{}, errors.New("the share's check does not match its contents: it was mistyped or damaged")
	}
	if body[0] != shareVersion {
		return Share{}, fmt.Errorf("share format version %d is not one this build reads (it reads %d)", body[0], shareVersion)
	}
	s := Share{Threshold: int(body[1]), Total: int(body[2]), Index: int(body[3]), KeyID: hex.EncodeToString(body[4 : 4+shareIDLen])}
	copy(s.value[:], body[4+shareIDLen:])
	if s.Threshold < 2 || s.Total < s.Threshold || s.Index < 1 || s.Index > s.Total {
		return Share{}, errors.New("the share is malformed: its index and threshold do not agree")
	}
	return s, nil
}

// ---- GF(256) ----

// gfMul multiplies in GF(2^8) modulo x^8+x^4+x^3+x+1, in time that does
// not depend on its operands: a table indexed by a byte of the key would
// leak that byte through the cache.
func gfMul(a, b byte) byte {
	var p byte
	for i := 0; i < 8; i++ {
		p ^= a & -(b & 1)
		hi := a >> 7
		a = (a << 1) ^ (0x1b & -hi)
		b >>= 1
	}
	return p
}

// gfInv is the multiplicative inverse: a^254. Its operands here are
// share indexes, which are not secret.
func gfInv(a byte) byte {
	r := byte(1)
	for i := 0; i < 254; i++ {
		r = gfMul(r, a)
	}
	return r
}

// SplitRootKey splits key into total shares of which any threshold
// rebuild it. The randomness is the polynomials' coefficients: it must
// be as good as the key's own, and rnd is crypto/rand when nil.
func SplitRootKey(rnd io.Reader, key []byte, threshold, total int) ([]Share, error) {
	if len(key) != KeyLen {
		return nil, fmt.Errorf("a root key is %d bytes, not %d", KeyLen, len(key))
	}
	if threshold < 2 {
		return nil, errors.New("the threshold must be at least 2: with 1, every share is the key")
	}
	if total < threshold || total > MaxShares {
		return nil, fmt.Errorf("want between %d (the threshold) and %d shares, not %d", threshold, MaxShares, total)
	}
	if rnd == nil {
		rnd = rand.Reader
	}
	shares := make([]Share, total)
	id := RootKeyID(key)
	for i := range shares {
		shares[i] = Share{Threshold: threshold, Total: total, Index: i + 1, KeyID: id}
	}
	coef := make([]byte, threshold-1)
	for b := 0; b < KeyLen; b++ {
		if _, err := io.ReadFull(rnd, coef); err != nil {
			return nil, err
		}
		for i := range shares {
			// Horner's rule, highest coefficient first, the key's byte last.
			x, y := byte(i+1), byte(0)
			for j := len(coef) - 1; j >= 0; j-- {
				y = gfMul(y, x) ^ coef[j]
			}
			shares[i].value[b] = gfMul(y, x) ^ key[b]
		}
	}
	for i := range coef {
		coef[i] = 0
	}
	return shares, nil
}

// CombineShares rebuilds the root key from shares of it. It needs the
// threshold's worth of distinct shares of one key, and refuses a result
// that is not the key the shares say they are shares of — which is what
// a share altered in a way its own check missed, or shares of two
// splits mixed, would otherwise produce in silence.
func CombineShares(shares []Share) ([]byte, error) {
	if len(shares) == 0 {
		return nil, errors.New("no shares given")
	}
	first := shares[0]
	seen := map[int]bool{}
	for _, s := range shares {
		if s.KeyID != first.KeyID || s.Threshold != first.Threshold || s.Total != first.Total {
			return nil, errors.New("the shares are not all of the same split of the same key")
		}
		if seen[s.Index] {
			return nil, fmt.Errorf("share %d was given twice", s.Index)
		}
		seen[s.Index] = true
	}
	if len(shares) < first.Threshold {
		return nil, fmt.Errorf("%d share(s) given, %d needed", len(shares), first.Threshold)
	}
	key := interpolateAtZero(shares[:first.Threshold])
	if RootKeyID(key) != first.KeyID {
		for i := range key {
			key[i] = 0
		}
		return nil, errors.New("the shares do not rebuild the key they name: one of them is not a share of it")
	}
	return key, nil
}

// interpolateAtZero is Lagrange interpolation of the shares' polynomials
// at zero, where the key is: the weight of share i is the product over
// the others of xj / (xj - xi), and subtraction in this field is xor.
func interpolateAtZero(use []Share) []byte {
	key := make([]byte, KeyLen)
	for i, si := range use {
		w := lagrangeWeight(use, i)
		for b := 0; b < KeyLen; b++ {
			key[b] ^= gfMul(si.value[b], w)
		}
	}
	return key
}

func lagrangeWeight(use []Share, i int) byte {
	num, den := byte(1), byte(1)
	for j, sj := range use {
		if i == j {
			continue
		}
		num = gfMul(num, byte(sj.Index))
		den = gfMul(den, byte(sj.Index)^byte(use[i].Index))
	}
	return gfMul(num, gfInv(den))
}

// ---- The root ----

// Limits on bad submissions to a sealed node. A share cannot be guessed
// (it is 256 bits), so this is not what keeps the key safe; it is what
// keeps a confused script from filling the audit log.
const (
	shareFailureWindow = time.Minute
	shareFailureLimit  = 10
)

// ShareRoot is the root whose key is rebuilt from shares. It is made
// sealed; Submit takes shares until there are enough.
type ShareRoot struct {
	mu sync.Mutex
	// expect is the id of the key this root must turn out to hold: the
	// one the store's wrapped key names, or, for a store with none yet,
	// the one the first share names.
	expect string
	held   map[int]Share
	key    []byte
	// failures are the times of recent refused submissions.
	failures []time.Time
	now      func() time.Time
	// opened is closed when the key has been rebuilt.
	opened chan struct{}
}

// NewShareRoot returns a sealed share root.
func NewShareRoot() *ShareRoot {
	return &ShareRoot{held: map[int]Share{}, now: time.Now, opened: make(chan struct{})}
}

func (r *ShareRoot) Kind() string   { return "shares" }
func (r *ShareRoot) String() string { return "shares" }

// KeyID is the id of the key the root holds or is waiting for; empty
// until a store or a share has said which.
func (r *ShareRoot) KeyID() string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.expect
}

// Expect says which key the root must rebuild, so a share of any other
// is refused as it arrives rather than after a quorum of them.
func (r *ShareRoot) Expect(keyID string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.key == nil && len(r.held) == 0 {
		r.expect = keyID
	}
}

// Sealed reports whether the root is still waiting for shares.
func (r *ShareRoot) Sealed() bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.key == nil
}

// Opened is closed once the root key has been rebuilt.
func (r *ShareRoot) Opened() <-chan struct{} { return r.opened }

// SealStatus is where a share root stands.
type SealStatus struct {
	Sealed bool `json:"sealed"`
	// Have and Need count distinct shares submitted and the threshold;
	// Need is zero until a first share has said what the threshold is.
	Have  int    `json:"have"`
	Need  int    `json:"need"`
	KeyID string `json:"key_id,omitempty"`
	// Indexes are the shares submitted so far, so the people at the
	// keyboard can see whose is still to come. An index is not secret.
	Indexes []int `json:"indexes,omitempty"`
}

// Status reports the root's progress.
func (r *ShareRoot) Status() SealStatus {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.statusLocked()
}

func (r *ShareRoot) statusLocked() SealStatus {
	st := SealStatus{Sealed: r.key == nil, Have: len(r.held), KeyID: r.expect}
	for i, s := range r.held {
		st.Indexes = append(st.Indexes, i)
		st.Need = s.Threshold
	}
	sort.Ints(st.Indexes)
	return st
}

// Submit gives the root one share. It reports where the root then
// stands; the error says why a share was refused, and never contains
// the share.
func (r *ShareRoot) Submit(text string) (SealStatus, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.key != nil {
		return r.statusLocked(), errors.New("already unsealed")
	}
	now := r.now()
	recent := r.failures[:0]
	for _, t := range r.failures {
		if now.Sub(t) < shareFailureWindow {
			recent = append(recent, t)
		}
	}
	r.failures = recent
	if len(r.failures) >= shareFailureLimit {
		return r.statusLocked(), fmt.Errorf("too many refused shares in the last %s; wait before submitting another", shareFailureWindow)
	}
	refuse := func(err error) (SealStatus, error) {
		r.failures = append(r.failures, now)
		return r.statusLocked(), err
	}
	s, err := ParseShare(text)
	if err != nil {
		return refuse(err)
	}
	if r.expect != "" && s.KeyID != r.expect {
		return refuse(fmt.Errorf("this is a share of root key %s; this node is waiting for shares of root key %s (a share of another cluster, or of a root this store has moved away from)", s.KeyID, r.expect))
	}
	for _, h := range r.held {
		if h.Threshold != s.Threshold || h.Total != s.Total {
			return refuse(errors.New("this share is of a different split of the key than the shares already submitted; shares of two splits cannot be combined"))
		}
	}
	if h, ok := r.held[s.Index]; ok {
		if h.value == s.value {
			// Not counted against the limit: two people reading the same
			// envelope is a mistake of procedure, not of typing.
			return r.statusLocked(), fmt.Errorf("share %d has already been submitted; %d more distinct share(s) are needed", s.Index, s.Threshold-len(r.held))
		}
		return refuse(fmt.Errorf("a different share with index %d has already been submitted; one of the two is not genuine", s.Index))
	}
	r.held[s.Index] = s
	r.expect = s.KeyID
	if len(r.held) < s.Threshold {
		return r.statusLocked(), nil
	}
	all := make([]Share, 0, len(r.held))
	for _, h := range r.held {
		all = append(all, h)
	}
	key, err := CombineShares(all)
	if err != nil {
		// Every share passed its own check and names this key, yet they
		// do not rebuild it: one is forged. Which cannot be told, so all
		// are dropped and the holders start again.
		r.held = map[int]Share{}
		return refuse(fmt.Errorf("%w; the shares submitted so far have been discarded", err))
	}
	r.key = HoldKey(key)
	Zero(key)
	st := r.statusLocked()
	for i, h := range r.held {
		h.value = [KeyLen]byte{}
		r.held[i] = h
	}
	r.held = map[int]Share{}
	close(r.opened)
	st.Sealed = false
	return st, nil
}

// Open gives the root its key directly: for the tool that has just made
// or rebuilt one.
func (r *ShareRoot) Open(key []byte) error {
	if len(key) != KeyLen {
		return fmt.Errorf("a root key is %d bytes, not %d", KeyLen, len(key))
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.key != nil {
		return errors.New("already unsealed")
	}
	r.key = HoldKey(key)
	r.expect = RootKeyID(key)
	r.held = map[int]Share{}
	close(r.opened)
	return nil
}

func (r *ShareRoot) rootKey() ([]byte, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.key == nil {
		return nil, ErrSealed
	}
	return r.key, nil
}

func (r *ShareRoot) Wrap(_ context.Context, plaintext []byte) ([]byte, error) {
	key, err := r.rootKey()
	if err != nil {
		return nil, err
	}
	return Seal(shareRootMagic, key, plaintext)
}

func (r *ShareRoot) Unwrap(_ context.Context, wrapped []byte) ([]byte, error) {
	key, err := r.rootKey()
	if err != nil {
		return nil, err
	}
	return Unseal(shareRootMagic, key, wrapped)
}

// Resplit makes a new split of the key an unsealed root holds: for when
// a share-holder leaves, or the number of holders changes. The old
// shares still rebuild the key among themselves — a new split does not
// cancel them, only a new root key does — so this is for a holder who
// is trusted to have destroyed a share, not for one who is not.
func (r *ShareRoot) Resplit(rnd io.Reader, threshold, total int) ([]Share, error) {
	key, err := r.rootKey()
	if err != nil {
		return nil, err
	}
	return SplitRootKey(rnd, key, threshold, total)
}

// SharedRoots returns the share roots among roots.
func SharedRoots(roots []Root) []*ShareRoot {
	var out []*ShareRoot
	for _, r := range roots {
		if s, ok := r.(*ShareRoot); ok {
			out = append(out, s)
		}
	}
	return out
}
