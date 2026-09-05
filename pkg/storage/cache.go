package storage

import (
	"bufio"
	"os"
	"strconv"
	"strings"
	"sync"
	"syscall"

	"github.com/cockroachdb/pebble/v2"
)

// Block cache (issue #101). Pebble's default cache is 8 MiB; every block
// read past it is decompressed and parsed again. Engines of one process
// share one cache sized from the machine's memory (a profile value, or
// Options.CacheSize), reference-counted so the last engine to close
// releases it — the test suite opens and closes hundreds of engines.

// Cache sizing per profile: a share of total memory, capped, never below
// the floor.
const (
	cacheFloor          = 64 << 20
	balancedCacheShare  = 0.25
	balancedCacheCap    = 8 << 30
	ingestCacheShare    = 0.10
	ingestCacheCap      = 2 << 30
	fallbackTotalMemory = 4 << 30
)

// DefaultCacheSize is the profile's block cache size on this machine:
// 25 % of memory (capped at 8 GiB) for balanced — the read working set —
// and 10 % (capped at 2 GiB) for ingest, whose working set is the write
// path.
func DefaultCacheSize(p Profile) int64 {
	total := totalMemory()
	share, limit := balancedCacheShare, int64(balancedCacheCap)
	if p == ProfileIngest {
		share, limit = ingestCacheShare, int64(ingestCacheCap)
	}
	size := int64(float64(total) * share)
	if size > limit {
		size = limit
	}
	if size < cacheFloor {
		size = cacheFloor
	}
	return size
}

// totalMemory reads MemTotal from /proc/meminfo (bytes), falling back to
// 4 GiB where it is unreadable.
func totalMemory() int64 {
	f, err := os.Open("/proc/meminfo")
	if err != nil {
		return fallbackTotalMemory
	}
	defer func() { _ = f.Close() }()
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		line := sc.Text()
		if !strings.HasPrefix(line, "MemTotal:") {
			continue
		}
		fields := strings.Fields(line)
		if len(fields) >= 2 {
			if kb, err := strconv.ParseInt(fields[1], 10, 64); err == nil && kb > 0 {
				return kb * 1024
			}
		}
	}
	return fallbackTotalMemory
}

// sharedCache is the process's block cache and the engines holding it.
var sharedCache struct {
	mu      sync.Mutex
	cache   *pebble.Cache
	size    int64
	engines int
}

// acquireCache returns the shared cache for a new engine, creating it at
// size when no engine holds one (the first engine's size wins for the
// cache's lifetime). Pebble takes its own reference in Open and drops it
// in Close; this package holds the creating reference until the last
// engine releases it.
func acquireCache(size int64) *pebble.Cache {
	sharedCache.mu.Lock()
	defer sharedCache.mu.Unlock()
	if sharedCache.cache == nil {
		sharedCache.cache = pebble.NewCache(size)
		sharedCache.size = size
	}
	sharedCache.engines++
	return sharedCache.cache
}

// releaseCache drops an engine's hold; the last one releases the cache's
// memory.
func releaseCache() {
	sharedCache.mu.Lock()
	defer sharedCache.mu.Unlock()
	if sharedCache.engines == 0 {
		return
	}
	sharedCache.engines--
	if sharedCache.engines == 0 {
		sharedCache.cache.Unref()
		sharedCache.cache, sharedCache.size = nil, 0
	}
}

// SharedCacheSize reports the size of the process's block cache (0 when
// no engine is open).
func SharedCacheSize() int64 {
	sharedCache.mu.Lock()
	defer sharedCache.mu.Unlock()
	return sharedCache.size
}

// sharedCacheEngines reports how many engines hold the cache (tests).
func sharedCacheEngines() int {
	sharedCache.mu.Lock()
	defer sharedCache.mu.Unlock()
	return sharedCache.engines
}

// maxOpenFiles is Pebble's open-file budget: half the process's soft file
// descriptor limit, between Pebble's own default (1000) and 16384, so a
// store with many sstables does not thrash table-cache evictions while
// SQL connections and RPC streams keep their share of descriptors.
func maxOpenFiles() int {
	var lim syscall.Rlimit
	if err := syscall.Getrlimit(syscall.RLIMIT_NOFILE, &lim); err != nil || lim.Cur == 0 {
		return 1000
	}
	n := int(lim.Cur / 2)
	if n < 1000 {
		n = 1000
	}
	if n > 16384 {
		n = 16384
	}
	return n
}
