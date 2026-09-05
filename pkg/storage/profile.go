package storage

import (
	"fmt"
	"runtime"

	"github.com/cockroachdb/pebble"
	"github.com/cockroachdb/pebble/bloom"
)

// Profile selects the engine's Pebble tuning. Both profiles share the
// read-path settings every store wants (issue #101): a block cache sized
// from the machine's memory, bloom filters on every level (a missing-key
// point read — the common case on datax's write path: uniqueness probes,
// intent lookups — skips the levels that cannot hold the key), the newest
// sstable format the pinned Pebble supports, and an open-file budget from
// the descriptor limit. The balanced profile leaves the flush and
// compaction shape at Pebble's defaults; the ingest profile trades read
// amplification and memory for sustained write throughput.
type Profile string

const (
	// ProfileBalanced is the default: Pebble's flush and compaction
	// defaults, with the shared read-path settings.
	ProfileBalanced Profile = "balanced"
	// ProfileIngest tunes for sustained high-rate keyed writes: larger and
	// more numerous memtables (fewer, bigger flushes), eager L0 compaction
	// with more concurrency, a wider L-base, and a hard write-stall ceiling
	// raised far above the soft backpressure gate — datax sheds load with
	// retryable errors long before Pebble would freeze every write.
	ProfileIngest Profile = "ingest"
)

// ParseProfile resolves a profile name ("" = balanced).
func ParseProfile(s string) (Profile, error) {
	switch Profile(s) {
	case "", ProfileBalanced:
		return ProfileBalanced, nil
	case ProfileIngest:
		return ProfileIngest, nil
	}
	return "", fmt.Errorf("unknown storage profile %q (want balanced or ingest)", s)
}

// Options configures Open. The zero value is the balanced profile with no
// encryption — today's behavior exactly.
type Options struct {
	Profile Profile
	// EncryptionKey is the 32-byte store key for encryption at rest; nil
	// opens the store in plaintext.
	EncryptionKey []byte
	// MemTableSize overrides the profile's memtable size (0 = the
	// profile's); crash tests shrink it so flushes happen within seconds.
	MemTableSize int
	// DisableWAL opens the engine without a write-ahead log: a write is
	// durable only once its memtable has flushed (Close flushes). The
	// state-machine engine of a split store runs this way — its
	// durability comes from the raft log (issue #105); FlushedSeqNum says
	// what has reached an sstable.
	DisableWAL bool
	// Raft tunes the engine for the raft log of a split store: small
	// memtables (the log is appended and truncated, never read back in
	// bulk) so its flushes stay cheap.
	Raft bool
	// CacheSize is the block cache size in bytes (0 = the profile's
	// DefaultCacheSize). The cache is shared by every engine of the
	// process: the first engine's size holds until the last one closes.
	CacheSize int64
}

// softGate holds the profile's backpressure thresholds: when the engine
// crosses any of them, Overloaded() reports true and the KV write path
// sheds table-data writes with a retryable error. They sit well below
// Pebble's own stall thresholds so datax backpressures before Pebble hard-
// stalls (a hard stall freezes raft appends and heartbeats for every range
// on the store).
type softGate struct {
	l0Sublevels   int
	l0Files       int
	memtableBytes uint64
	// Compaction-debt gate, with hysteresis: enter at or above debtHigh, exit at or
	// below debtLow (a single threshold would flap on every compaction).
	// Debt measures scheduled-but-unfinished compaction work, so a
	// sustained ingest that outruns compaction trips it long before L0
	// shape does.
	debtHigh uint64
	debtLow  uint64
}

// BloomBitsPerKey is the filter density: ~1 % false positives.
const BloomBitsPerKey = 10

// applyCommon sets the read-path options every profile shares.
func applyCommon(opts *pebble.Options) {
	opts.Levels = make([]pebble.LevelOptions, 7)
	for i := range opts.Levels {
		opts.Levels[i].FilterPolicy = bloom.FilterPolicy(BloomBitsPerKey)
		opts.Levels[i].FilterType = pebble.TableFilter
	}
	opts.FormatMajorVersion = pebble.FormatNewest
	opts.MaxOpenFiles = maxOpenFiles()
}

// apply sets the profile's Pebble options and returns its soft gate.
func (p Profile) apply(opts *pebble.Options) softGate {
	applyCommon(opts)
	switch p {
	case ProfileIngest:
		opts.MemTableSize = 64 << 20         // absorb bursts; fewer, larger flushes
		opts.MemTableStopWritesThreshold = 4 // up to 256 MiB queued before Pebble's own stall
		opts.L0CompactionThreshold = 2       // drain L0 eagerly (read-amp is secondary)
		opts.L0StopWritesThreshold = 1000    // hard ceiling far above the soft gate
		opts.LBaseMaxBytes = 256 << 20       // wider Lbase cuts L0→Lbase write amp
		opts.BytesPerSync = 1 << 20          // smoother background writeback
		opts.MaxConcurrentCompactions = func() int {
			n := runtime.NumCPU() / 2
			if n < 2 {
				n = 2
			}
			if n > 6 {
				n = 6
			}
			return n
		}
		// memtableBytes: three full memtables' worth buffered (of the four
		// the stop threshold allows) means flushing is clearly behind.
		// (Pebble's MemTable.Count is just len(queue) and idles well above
		// 1, so bytes — not count — is the backlog signal.)
		// Debt: the ingest profile compacts aggressively, so debt this deep
		// means ingest has outrun the compaction budget for a while.
		return softGate{l0Sublevels: 20, l0Files: 1500, memtableBytes: 3 * (64 << 20),
			debtHigh: 8 << 30, debtLow: 4 << 30}
	default:
		// Balanced: the flush and compaction shape at Pebble's defaults; no
		// memtable criterion (tiny default memtables make byte thresholds
		// twitchy).
		return softGate{l0Sublevels: 10, l0Files: 400,
			debtHigh: 2 << 30, debtLow: 1 << 30}
	}
}
