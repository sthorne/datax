package storage

import (
	"bytes"
	"context"
	"fmt"
	"testing"

	"github.com/sthorne/datax/pkg/keys"
	"github.com/sthorne/datax/pkg/util/hlc"
)

// splitShapeKeys returns every engine-key shape datax writes for the
// user key k: metadata key, version keys (timestamps chosen so the
// suffix bytes cover 0x00, 0x01 and 0xff, including a suffix that ends
// in 0x00 0x01), the re-encryption seeds of each, and the key's upper
// bound.
func splitShapeKeys(k keys.Key) (prefix []byte, own, suffixed [][]byte) {
	meta, upper := mvccKeyBounds(k)
	own = append(own, meta, upper)
	for _, ts := range []hlc.Timestamp{
		{WallTime: 1}, {WallTime: 0x0100, Logical: 1}, {WallTime: -1, Logical: -1},
		// ^logical = 0x00000001 → the suffix ends in 0x00 0x00 0x00 0x01.
		{WallTime: 0x0001_0100_0000_0000, Logical: ^int32(1)},
		// ^wall ends in 0x00 0x01 at the terminator's offset of a shorter key.
		{WallTime: ^int64(0x0100), Logical: 0},
	} {
		v := EncodeMVCCKey(k, ts)
		suffixed = append(suffixed, v, append(append([]byte(nil), v...), 0))
	}
	suffixed = append(suffixed, append(append([]byte(nil), meta...), 0))
	return meta, own, suffixed
}

// TestMVCCSplit: the O(1) Split cuts every engine-key shape at the
// terminator — a user key's metadata key, versions and seeds share one
// prefix; bounds, separators and local keys are their own — for user
// keys built from the bytes the escaping moves around.
func TestMVCCSplit(t *testing.T) {
	if keys.LocalPrefix[0] != localPrefixByte {
		t.Fatalf("local prefix byte %#x, comparer assumes %#x", keys.LocalPrefix[0], localPrefixByte)
	}
	users := [][]byte{{}, {0}, {1}, {0xff}, {0, 0}, {0, 1}, {1, 0}, {0xff, 0}, {2, 0, 1}, {0, 0xff, 0, 1}, {0, 0xff, 0xff}, {0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0}}
	for _, b := range []byte{0, 1, 2, 0xfe, 0xff} {
		for l := 1; l <= 20; l++ {
			users = append(users, bytes.Repeat([]byte{b}, l))
		}
	}
	for _, u := range users {
		k := append(keys.TablePrefix.Clone(), u...)
		prefix, own, suffixed := splitShapeKeys(k)
		if got := mvccSplit(prefix); got != len(prefix) {
			t.Fatalf("metadata key %x: split %d, want %d", prefix, got, len(prefix))
		}
		for _, o := range own {
			if got := mvccSplit(o); got != len(o) {
				t.Fatalf("%x: split %d, want the whole key (%d)", o, got, len(o))
			}
		}
		for _, sk := range suffixed {
			if got := mvccSplit(sk); got != len(prefix) || !bytes.Equal(sk[:got], prefix) {
				t.Fatalf("%x: prefix %x, want %x", sk, sk[:got], prefix)
			}
		}
	}
	// Local keys are their own prefix, whatever their tail looks like:
	// range 1's log keys carry 0x00 0x01 exactly where a version key's
	// terminator would be.
	for _, lk := range [][]byte{
		keys.RaftLogKey(1, 7), keys.RaftHardStateKey(1), keys.RaftLogKey(256, 1<<40),
		keys.TransactionKey(keys.Key("anchor"), [16]byte{}), keys.StoreClusterVersionKey(),
	} {
		if got := mvccSplit(lk); got != len(lk) {
			t.Fatalf("local key %x: split %d, want %d", lk, got, len(lk))
		}
	}
}

// TestMVCCSplitProperties checks Pebble's three Split properties by
// brute force over the engine keys of user keys from a small alphabet
// of the bytes the escaping moves around, plus every local key shape:
// a prefix sorts before its suffixed keys, prefixes order keys before
// suffixes, and equal prefixes order by suffix alone.
func TestMVCCSplitProperties(t *testing.T) {
	var all [][]byte
	alphabet := []byte{0, 1, 0xff, 'a'}
	var gen func(prefix []byte, depth int)
	gen = func(prefix []byte, depth int) {
		k := append(keys.TablePrefix.Clone(), prefix...)
		_, own, suffixed := splitShapeKeys(k)
		all = append(all, own...)
		all = append(all, suffixed...)
		if depth == 0 {
			return
		}
		for _, b := range alphabet {
			gen(append(append([]byte(nil), prefix...), b), depth-1)
		}
	}
	gen(nil, 3)
	all = append(all, keys.RaftLogKey(1, 1), keys.RaftLogKey(1, 2), keys.RaftHardStateKey(1),
		keys.RangeDescriptorKey(1), keys.TransactionKey(keys.Key{4, 0, 1}, [16]byte{1}))
	cmp := MVCCComparer.Compare
	prefixOf := func(k []byte) []byte { return k[:mvccSplit(k)] }
	for _, a := range all {
		if pa := prefixOf(a); len(pa) < len(a) && cmp(pa, a) >= 0 {
			t.Fatalf("prefix %x does not sort before %x", pa, a)
		}
		for _, b := range all {
			pa, pb := prefixOf(a), prefixOf(b)
			c, pc := cmp(a, b), cmp(pa, pb)
			if c <= 0 && pc > 0 {
				t.Fatalf("%x <= %x but prefixes %x > %x", a, b, pa, pb)
			}
			if pc < 0 && c >= 0 {
				t.Fatalf("prefixes %x < %x but %x >= %x", pa, pb, a, b)
			}
			if pc == 0 && c != MVCCComparer.ComparePointSuffixes(a[len(pa):], b[len(pb):]) {
				t.Fatalf("%x vs %x: equal prefixes but the suffixes order differently", a, b)
			}
		}
	}
}

// legacySeparators returns the index-block separators Pebble's DEFAULT
// comparer would write between k's engine keys: it truncates a key
// wherever the bytes allow, with no notion of a suffix to respect. A
// store upgraded to v15 is full of them, and prefix mode reads those
// tables as they are, so Split has to cope with a valid prefix followed
// by a PARTIAL suffix — a shape datax itself never writes, which is why
// the corpora built from EncodeMVCCKey alone never covered it (#178).
func legacySeparators(k keys.Key) [][]byte {
	meta, _ := mvccKeyBounds(k)
	var out [][]byte
	for _, ts := range []hlc.Timestamp{
		{WallTime: 1}, {WallTime: -1, Logical: -1}, {WallTime: 0x0100, Logical: 1},
	} {
		v := EncodeMVCCKey(k, ts)
		// Every truncation of the version suffix, down to the bare
		// prefix: Pebble's Separator stops at the first byte that lets
		// it separate, so any of these can end up in an index block.
		for cut := len(meta); cut <= len(v); cut++ {
			out = append(out, append([]byte(nil), v[:cut]...))
		}
	}
	return out
}

// TestMVCCSplitLegacySeparators is the regression test for #178: Split
// must agree with Compare on the separators older tables carry.
//
// The property is Pebble's, and the assertion comparer enforces it at
// runtime under -race: when two keys' prefixes differ, their order must
// be the order of their prefixes. It broke because a truncated
// separator was called its own prefix, making its "prefix" a strict
// extension of the prefix of the very keys it separates.
func TestMVCCSplitLegacySeparators(t *testing.T) {
	var all [][]byte
	for _, u := range []string{"a", "ab", "k", "\x00", "\xff", "a\x00b"} {
		k := append(keys.TablePrefix.Clone(), u...)
		_, own, suffixed := splitShapeKeys(k)
		all = append(all, own...)
		all = append(all, suffixed...)
		all = append(all, legacySeparators(k)...)
	}
	cmp := MVCCComparer.Compare
	prefixOf := func(k []byte) []byte { return k[:mvccSplit(k)] }
	for _, a := range all {
		for _, b := range all {
			pa, pb := prefixOf(a), prefixOf(b)
			c, pc := cmp(a, b), cmp(pa, pb)
			// Differing prefixes must decide the order.
			if pc != 0 && ((pc < 0) != (c < 0) || (pc > 0) != (c > 0)) {
				t.Fatalf("Compare(%x, %x) = %d but Compare(prefixes %x, %x) = %d", a, b, c, pa, pb, pc)
			}
			if pc == 0 && c != MVCCComparer.ComparePointSuffixes(a[len(pa):], b[len(pb):]) {
				t.Fatalf("%x vs %x: equal prefixes but the suffixes order differently", a, b)
			}
		}
	}
	// No prefix may strictly extend another: that is the shape that
	// breaks the property above, and the terminator rule forbids it.
	for _, a := range all {
		for _, b := range all {
			pa, pb := prefixOf(a), prefixOf(b)
			if len(pa) < len(pb) && bytes.HasPrefix(pb, pa) && len(pa) < len(a) {
				t.Fatalf("prefix %x (of %x) is a strict extension of prefix %x (of a suffixed key %x)", pb, b, pa, a)
			}
		}
	}
}

// tailSplit is the Split shipped in 0.43.0, which decided the boundary
// from the key's tail (#178).
func tailSplit(k []byte) int {
	n := len(k)
	if n == 0 || k[0] == localPrefixByte {
		return n
	}
	switch {
	case n >= 14 && k[n-14] == 0 && k[n-13] == 1:
		return n - versionSuffixLen
	case n >= 15 && k[n-15] == 0 && k[n-14] == 1:
		return n - versionSuffixLen - 1
	case n >= 3 && k[n-3] == 0 && k[n-2] == 1:
		return n - 1
	}
	return n
}

// TestMVCCSplitReadsExistingTables is why mvccKeySchemaName does not
// change with the #178 fix. A columnar table stores its keys already
// split, so a store written at v15 is only readable under the same
// schema name if the new Split agrees with the old one on every key
// datax writes. It does: the two differ only on the shapes datax never
// writes — the truncated separators Pebble's default comparer left in
// tables from before prefix mode — and those live in index blocks,
// which the fix reads correctly for the first time.
func TestMVCCSplitReadsExistingTables(t *testing.T) {
	agree, differ := 0, 0
	for _, u := range []string{"", "a", "ab", "k", "\x00", "\xff", "a\x00b", "\x00\x01", "\xff\xff\xff\xff\xff\xff\xff\xff"} {
		k := append(keys.TablePrefix.Clone(), u...)
		_, own, suffixed := splitShapeKeys(k)
		for _, ek := range append(append([][]byte(nil), own...), suffixed...) {
			if mvccSplit(ek) != tailSplit(ek) {
				t.Fatalf("engine key %x: split %d, the shipped Split said %d — a v15 table would not read back",
					ek, mvccSplit(ek), tailSplit(ek))
			}
			agree++
		}
		// The separators are the shapes that must differ; that
		// difference is the fix.
		for _, sep := range legacySeparators(k) {
			if mvccSplit(sep) != tailSplit(sep) {
				differ++
			}
		}
	}
	for _, lk := range [][]byte{keys.RaftLogKey(1, 7), keys.RaftHardStateKey(1), keys.StoreClusterVersionKey()} {
		if mvccSplit(lk) != tailSplit(lk) {
			t.Fatalf("local key %x: split %d, the shipped Split said %d", lk, mvccSplit(lk), tailSplit(lk))
		}
		agree++
	}
	if agree == 0 || differ == 0 {
		t.Fatalf("expected agreement on written keys (%d) and disagreement on legacy separators (%d)", agree, differ)
	}
}

// TestMVCCSeparatorSuccessor: index keys are prefix keys strictly
// between (or above) what they separate, and the immediate successor of
// a prefix is the next prefix key.
func TestMVCCSeparatorSuccessor(t *testing.T) {
	c := MVCCComparer
	var ks [][]byte
	for _, u := range []string{"a", "a\x00", "ab", "b", "b\x00\x00", "\xff", "\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff"} {
		k := append(keys.TablePrefix.Clone(), u...)
		_, own, suffixed := splitShapeKeys(k)
		ks = append(ks, own[0])
		ks = append(ks, suffixed...)
	}
	ks = append(ks, keys.RaftLogKey(1, 1), keys.RaftLogKey(1, 2))
	for _, a := range ks {
		for _, b := range ks {
			if c.Compare(a, b) >= 0 {
				continue
			}
			sep := c.Separator(nil, a, b)
			if c.Compare(a, sep) > 0 || c.Compare(sep, b) >= 0 {
				t.Fatalf("separator(%x, %x) = %x is not in [a, b)", a, b, sep)
			}
			// A shortened separator is a prefix key; otherwise it is a
			// itself (an engine key, which Split handles as such).
			if !bytes.Equal(sep, a) && mvccSplit(sep) != len(sep) {
				t.Fatalf("separator(%x, %x) = %x is neither a nor a prefix key", a, b, sep)
			}
		}
		suc := c.Successor(nil, a)
		if c.Compare(a, suc) > 0 {
			t.Fatalf("successor(%x) = %x < a", a, suc)
		}
		if !bytes.Equal(suc, a) && mvccSplit(suc) != len(suc) {
			t.Fatalf("successor(%x) = %x is neither a nor a prefix key", a, suc)
		}
		p := a[:mvccSplit(a)]
		is := c.ImmediateSuccessor(nil, p)
		if mvccSplit(is) != len(is) || c.Compare(p, is) >= 0 {
			t.Fatalf("immediate successor of %x = %x", p, is)
		}
		// What NextPrefix needs: every key sharing the prefix sorts
		// below it. It does NOT need to extend the prefix, and since
		// #178 it cannot — a terminated prefix has no prefix-key
		// extensions, because its own terminator stays the first one.
		for _, k := range ks {
			if bytes.Equal(k[:mvccSplit(k)], p) && c.Compare(k, is) >= 0 {
				t.Fatalf("%x shares the prefix %x but does not sort below its successor %x", k, p, is)
			}
		}
	}
	// The immediate successor of a metadata key increments the
	// terminator: appending anything would leave the terminator in
	// place and so would not be a prefix key at all (#178). Nothing is
	// representable in between — the only keys above esc(K) 0x00 0x01
	// and below esc(K) 0x00 0x02 extend the former, and those all split
	// back to it.
	meta := EncodeMVCCKey(keys.Key("k"), hlc.Timestamp{})
	want := append([]byte(nil), meta...)
	want[len(want)-1] = 2
	is := c.ImmediateSuccessor(nil, meta)
	if !bytes.Equal(is, want) {
		t.Fatalf("immediate successor of a metadata key: %x, want %x", is, want)
	}
	if mvccSplit(is) != len(is) {
		t.Fatalf("immediate successor %x is not a prefix key", is)
	}
	// The old seed-shaped answer is no longer a prefix key, which is
	// what made the append loop non-terminating.
	if seed := append(append([]byte(nil), meta...), 0, 0); mvccSplit(seed) == len(seed) {
		t.Fatalf("%x should split back to the metadata key", seed)
	}
}

// TestPrefixBloomConsulted (issue #161): in prefix mode a point read of
// an absent key is answered by the filters — Pebble counts a filter
// miss per sstable excluded — and reads of present keys, their
// versions and intents are unchanged; without prefix mode the same
// reads consult no filter.
func TestPrefixBloomConsulted(t *testing.T) {
	for _, prefix := range []bool{false, true} {
		t.Run(fmt.Sprintf("prefix=%v", prefix), func(t *testing.T) {
			eng, err := Open(t.TempDir(), Options{PrefixBloom: prefix})
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = eng.Close() }()
			if eng.PrefixBloom() != prefix {
				t.Fatalf("PrefixBloom() = %v", eng.PrefixBloom())
			}
			// Two sstables with disjoint sets of present keys, three
			// versions each; probes for absent keys fall between them.
			benchKeyOf := func(i int) keys.Key {
				return append(keys.TablePrefix.Clone(), fmt.Sprintf("/%05d", i)...)
			}
			for round := 0; round < 2; round++ {
				b := eng.NewBatch()
				for i := round; i < 400; i += 2 {
					k := benchKeyOf(i * 2)
					for v := 1; v <= 3; v++ {
						if err := MVCCPut(b, k, ts(int64(v), 0), []byte(fmt.Sprintf("v%d", v)), nil); err != nil {
							t.Fatal(err)
						}
					}
				}
				if err := b.Commit(true); err != nil {
					t.Fatal(err)
				}
				_ = b.Close()
				if err := eng.Flush(); err != nil {
					t.Fatal(err)
				}
			}
			// Let the flushes settle wherever Pebble's compactions put
			// them (an L6 table's filter is consulted too).
			compactAll(t, eng)
			t.Logf("tables per level: %v", tablesPerLevel(t, eng))
			h0, m0 := eng.FilterMetrics()
			for i := 0; i < 400; i++ {
				absent := benchKeyOf(i*2 + 1)
				if v, err := MVCCGet(eng, absent, ts(2, 0), MVCCGetOptions{}); err != nil || v != nil {
					t.Fatalf("%s: %q, %v; want absent", absent, v, err)
				}
			}
			h1, m1 := eng.FilterMetrics()
			for i := 0; i < 400; i++ {
				k := benchKeyOf(i * 2)
				v, err := MVCCGet(eng, k, ts(2, 0), MVCCGetOptions{})
				if err != nil || string(v) != "v2" {
					t.Fatalf("%s at 2: %q, %v", k, v, err)
				}
			}
			h2, m2 := eng.FilterMetrics()
			// Pebble counts a hit when a filter spared a data block read
			// and a miss when it was consulted and could not.
			t.Logf("absent reads: %d filtered, %d admitted; present reads: %d filtered, %d admitted", h1-h0, m1-m0, h2-h1, m2-m1)
			if !prefix && (h2 != h0 || m2 != m0) {
				t.Fatalf("without prefix mode the filters were consulted: %d filtered, %d admitted", h2-h0, m2-m0)
			}
			if prefix && (h1-h0 < 400 || m2-m1 < 400) {
				// Every absent key is filtered out (false positives aside);
				// every present key is consulted and admitted.
				t.Fatalf("prefix mode: absent reads %d filtered, %d admitted; present reads %d filtered, %d admitted", h1-h0, m1-m0, h2-h1, m2-m1)
			}
			// Intents and read-your-writes through the prefix seeks.
			txn := newTxn(ts(10, 0))
			k := benchKeyOf(100)
			b := eng.NewBatch()
			if err := MVCCPut(b, k, ts(10, 0), []byte("prov"), txn); err != nil {
				t.Fatal(err)
			}
			if v, err := MVCCGet(b, k, ts(10, 0), MVCCGetOptions{Txn: txn}); err != nil || string(v) != "prov" {
				t.Fatalf("own intent: %q, %v", v, err)
			}
			if _, err := MVCCGet(b, k, ts(10, 0), MVCCGetOptions{}); err == nil {
				t.Fatal("a foreign read of the intent did not conflict")
			}
			if v, err := MVCCGet(b, k, ts(2, 0), MVCCGetOptions{Inconsistent: true}); err != nil || string(v) != "v2" {
				t.Fatalf("beneath the intent: %q, %v", v, err)
			}
			_ = b.Close()
		})
	}
}

// compactAll runs a manual compaction over the whole keyspace.
func compactAll(t testing.TB, eng *Engine) {
	t.Helper()
	if err := eng.db.Compact(context.Background(), []byte{0}, []byte{0xff, 0xff}, false); err != nil {
		t.Fatal(err)
	}
}

// tablesPerLevel counts the live sstables of each level.
func tablesPerLevel(t testing.TB, eng *Engine) []int {
	t.Helper()
	levels, err := eng.db.SSTables()
	if err != nil {
		t.Fatal(err)
	}
	out := make([]int, len(levels))
	for i, l := range levels {
		out[i] = len(l)
	}
	return out
}
