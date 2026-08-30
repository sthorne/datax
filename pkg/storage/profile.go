package storage

import (
	"fmt"
	"runtime"

	"github.com/cockroachdb/pebble"
)

// Profile selects the engine's Pebble tuning. The balanced profile sets no
// options at all — Pebble's defaults, exactly datax's historical behavior —
// so the default profile can never regress existing workloads. The ingest
// profile trades read amplification and memory for sustained write
// throughput.
type Profile string

const (
	// ProfileBalanced is the default: Pebble's own defaults, untouched.
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
}

// apply sets the profile's Pebble options and returns its soft gate.
func (p Profile) apply(opts *pebble.Options) softGate {
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
		return softGate{l0Sublevels: 20, l0Files: 1500, memtableBytes: 3 * (64 << 20)}
	default:
		// Balanced: leave every Pebble option at its default; no memtable
		// criterion (tiny default memtables make byte thresholds twitchy).
		return softGate{l0Sublevels: 10, l0Files: 400}
	}
}
