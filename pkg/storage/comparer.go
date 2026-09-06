package storage

import (
	"bytes"

	"github.com/cockroachdb/pebble/v2"
	"github.com/cockroachdb/pebble/v2/bloom"
	"github.com/cockroachdb/pebble/v2/sstable"
	"github.com/cockroachdb/pebble/v2/sstable/colblk"
)

// Prefix bloom filters (issue #161). Pebble consults an sstable's bloom
// filter only on SeekPrefixGE (and inside DB.Get), and only for the
// prefix its Comparer.Split cuts off the key. Without a Split the
// filters #101 configured hashed whole engine keys — so a version read
// of K (a seek to K's metadata key, then to a version) never asked
// them, and a point read walked every level's sstables. MVCCComparer
// splits an engine key into the user key's encoding (with terminator)
// and its version suffix, so one prefix covers K's metadata key and
// every version of K, and a filter can rule out the sstables that hold
// nothing of K at all.
//
// The comparer keeps Pebble's default name and Compare (the name is
// stamped in the MANIFEST and every sstable; it promises the ordering,
// which is unchanged). What Split does leave on disk is looked up by
// name and versioned by it: the filter policy (a filter is stored as
// "fullfilter.<name>" and consulted only if that name is configured, so
// a whole-key filter written before is never asked a prefix question —
// it is simply not consulted until compaction rewrites the table) and
// the columnar key schema (its name is a table property; the seeker of
// a block splits seek keys with the comparer the schema was built
// with, and Pebble is built for several schemas over a store's life).
// So a store switches with no rewrite and no key-format change; the
// tables from before lose nothing but the filter skip, and
// FilterRewritePass rewrites them in the background.
//
// Every engine key datax writes is one of
//
//	local key                 0x01 ...                      (raw)
//	metadata key              esc(K) 0x00 0x01
//	version key               esc(K) 0x00 0x01 ts[12]
//	re-encryption seeds       <version or metadata key> 0x00
//
// and esc(K) never contains 0x00 0x01 (a 0x00 in K is escaped to
// 0x00 0xff, see pkg/util/encoding). So the prefix ends at the FIRST
// 0x00 0x01, and Split finds it by searching from the left.
//
// It does not decide from the tail (issue #178). The tail is ambiguous
// for a key datax did not write: an index-block separator in a table
// from before prefix mode is a valid prefix followed by the first few
// bytes of a timestamp, because Pebble's default comparer truncated it
// with no suffix to respect. Reading such a key's whole length as its
// prefix — or cutting a fixed 12 bytes off a partial suffix — makes its
// "prefix" a strict extension of the prefix of the keys it separates,
// which contradicts Compare: the two keys then order one way whole and
// the other way by prefix. Pebble's assertion comparer panics on that
// under -race, and in a release build SeekPrefixGE can walk a legacy
// table's index to the wrong block and report a key absent.
//
// Searching from the left is what makes the prefixes prefix-free, which
// is the property Compare needs: if one prefix strictly extended
// another, the longer key would carry the shorter one's terminator and
// so would have been cut there instead.
//
// Local keys are their own prefix by fiat; their raw layouts (a range
// ID's big-endian bytes) could otherwise contain 0x00 0x01.

// mvccPrefixBloomName names the filter policy of prefix mode: the same
// bloom filter as before, under a name that tells a reader its hashes
// are of Split prefixes.
const mvccPrefixBloomName = "datax.mvcc-prefix-bloom"

// mvccKeySchemaName names the columnar key schema of prefix mode.
const mvccKeySchemaName = "datax.mvcc.v1"

// prefixL6Filters: prefix-mode iterators consult the filters of L6
// tables too (Pebble skips them by default, on the theory that a key
// is usually found in L6). A store's bulk rests in L6, and the reads
// that gain most — uniqueness probes, intent lookups for keys that are
// not there — would otherwise read an L6 index and data block for
// every miss. A variable so the benchmarks can measure both settings.
var prefixL6Filters = true

// localPrefixByte is keys.LocalPrefix's byte (asserted by TestMVCCSplit).
const localPrefixByte = 0x01

// mvccTerminator ends the escaped user key of every engine key that has
// one. esc(K) cannot contain it, so its first occurrence is the prefix
// boundary.
var mvccTerminator = []byte{0, 1}

// mvccSplit is MVCCComparer's Split.
func mvccSplit(k []byte) int {
	if len(k) == 0 || k[0] == localPrefixByte {
		return len(k)
	}
	if i := bytes.Index(k, mvccTerminator); i >= 0 {
		return i + len(mvccTerminator)
	}
	// A bound or a separator with no terminator at all: its own prefix.
	return len(k)
}

// mvccSeparator shortens within the prefix only, so index keys are
// prefix keys: Pebble's byte-wise separator of the two prefixes when it
// finds one strictly between them, else a itself.
func mvccSeparator(dst, a, b []byte) []byte {
	pa, pb := a[:mvccSplit(a)], b[:mvccSplit(b)]
	if bytes.Equal(pa, pb) {
		return append(dst, a...)
	}
	n := len(dst)
	dst = pebble.DefaultComparer.Separator(dst, pa, pb)
	if bytes.Equal(dst[n:], pa) {
		return append(dst[:n], a...)
	}
	return dst
}

// mvccSuccessor is the byte-wise successor of a's prefix, or a itself
// when the prefix has none.
func mvccSuccessor(dst, a []byte) []byte {
	pa := a[:mvccSplit(a)]
	n := len(dst)
	dst = pebble.DefaultComparer.Successor(dst, pa)
	if bytes.Equal(dst[n:], pa) {
		return append(dst[:n], a...)
	}
	return dst
}

// mvccImmediateSuccessor is the smallest prefix key above a.
//
// Nothing appended to a terminated prefix is a prefix key — the
// terminator it already carries stays the first one, so Split keeps
// cutting there — which is why this increments the terminator instead
// of extending the key. esc(K) 0x00 0x02 carries no terminator, so it
// is its own prefix; it sorts above every version of K (whose next byte
// is 0x01) and below every longer prefix, so nothing lies between.
//
// A prefix with no terminator (a bound, a local key) has an immediate
// successor in the ordinary way: append 0x00.
func mvccImmediateSuccessor(dst, a []byte) []byte {
	n := len(a)
	if n >= 2 && a[n-2] == 0 && a[n-1] == mvccTerminator[1] {
		dst = append(dst, a...)
		dst[len(dst)-1] = mvccTerminator[1] + 1
		return dst
	}
	return append(append(dst, a...), 0)
}

// MVCCComparer is the engine comparer of prefix mode: Pebble's default
// ordering with the MVCC Split.
var MVCCComparer = func() *pebble.Comparer {
	c := *pebble.DefaultComparer
	c.Split = mvccSplit
	c.Separator = mvccSeparator
	c.Successor = mvccSuccessor
	c.ImmediateSuccessor = mvccImmediateSuccessor
	return &c
}()

// mvccPrefixBloom is the bloom filter under prefix mode's name.
type mvccPrefixBloom struct{ bloom.FilterPolicy }

func (mvccPrefixBloom) Name() string { return mvccPrefixBloomName }

// applyKeySchemas registers both columnar key schemas — the default
// comparer's, under the name Pebble gave it, and the MVCC comparer's —
// and selects the one new tables are written with. Both modes register
// both: a schema's seeker splits keys with the comparer the schema was
// built with, so a table written in either mode reads correctly under
// either, and the node's first open at start (before it has read the
// store's cluster version) can be a plain one over a prefix-mode store.
// The filter policy is what must not cross modes: a prefix filter asked
// about a whole key would deny keys that are there, so its name is
// configured in prefix mode only and the tables from the other mode
// go unconsulted.
func applyKeySchemas(opts *pebble.Options, prefix bool) {
	legacy := colblk.DefaultKeySchema(pebble.DefaultComparer, 16)
	mvcc := colblk.DefaultKeySchema(MVCCComparer, 16)
	mvcc.Name = mvccKeySchemaName
	opts.KeySchema = legacy.Name
	if prefix {
		opts.KeySchema = mvcc.Name
	}
	opts.KeySchemas = sstable.MakeKeySchemas(&legacy, &mvcc)
}

// applyPrefixBloom puts the engine in prefix mode: the MVCC comparer,
// filters under the prefix-mode name, and its key schema for new tables.
func applyPrefixBloom(opts *pebble.Options) {
	opts.Comparer = MVCCComparer
	for i := range opts.Levels {
		opts.Levels[i].FilterPolicy = mvccPrefixBloom{bloom.FilterPolicy(BloomBitsPerKey)}
		opts.Levels[i].FilterType = pebble.TableFilter
	}
	applyKeySchemas(opts, true)
}
