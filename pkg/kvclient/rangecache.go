package kvclient

import (
	"sort"
	"sync"

	"github.com/sthorne/datax/pkg/base"
	"github.com/sthorne/datax/pkg/keys"
	"github.com/sthorne/datax/pkg/kvpb"
)

// rangeCache caches range descriptors (sorted by end key) and per-range
// leader hints. Stale entries are evicted when the server corrects us
// (NotLeader / RangeKeyMismatch / RangeNotFound).
type rangeCache struct {
	mu    sync.Mutex
	descs []kvpb.RangeDescriptor // sorted by EndKey
	hints map[base.RangeID]base.NodeID
	// lastMeta is the last known descriptor covering the meta span — the
	// bootstrap invariant. Evict may drop the meta range's entry (e.g. a
	// RangeNotFound from a node that just shed its replica carries no fresh
	// descriptors), but meta lookups must never lose all routing: Lookup
	// falls back to this possibly-stale copy, and the replica fan-out plus
	// mismatch corrections converge it.
	lastMeta *kvpb.RangeDescriptor
}

func newRangeCache() *rangeCache {
	return &rangeCache{hints: make(map[base.RangeID]base.NodeID)}
}

// Lookup returns the cached descriptor covering key.
func (c *rangeCache) Lookup(key keys.Key) (kvpb.RangeDescriptor, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	i := sort.Search(len(c.descs), func(i int) bool {
		return key.Compare(c.descs[i].EndKey) < 0
	})
	if i < len(c.descs) && c.descs[i].ContainsKey(key) {
		return c.descs[i], true
	}
	if c.lastMeta != nil && c.lastMeta.ContainsKey(key) {
		return *c.lastMeta, true
	}
	return kvpb.RangeDescriptor{}, false
}

// Insert adds or refreshes descriptors, dropping any cached descriptor that
// overlaps a newer one.
func (c *rangeCache) Insert(descs ...kvpb.RangeDescriptor) {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, d := range descs {
		filtered := c.descs[:0]
		for _, cur := range c.descs {
			overlaps := cur.StartKey.Compare(d.EndKey) < 0 && d.StartKey.Compare(cur.EndKey) < 0
			if overlaps {
				if cur.Generation >= d.Generation && cur.RangeID == d.RangeID {
					// Cached one is at least as fresh; keep it, skip insert.
					d.RangeID = 0
					filtered = append(filtered, cur)
				}
				continue
			}
			filtered = append(filtered, cur)
		}
		c.descs = filtered
		if d.RangeID != 0 {
			c.descs = append(c.descs, d)
		}
		if metaStart, _ := keys.MetaSpan(); d.ContainsKey(metaStart) &&
			(c.lastMeta == nil || d.Generation >= c.lastMeta.Generation) {
			held := d
			c.lastMeta = &held
		}
	}
	sort.Slice(c.descs, func(i, j int) bool {
		return c.descs[i].EndKey.Compare(c.descs[j].EndKey) < 0
	})
}

// Evict removes the descriptor for a range (after a routing miss).
func (c *rangeCache) Evict(rangeID base.RangeID) {
	c.mu.Lock()
	defer c.mu.Unlock()
	for i, d := range c.descs {
		if d.RangeID == rangeID {
			c.descs = append(c.descs[:i], c.descs[i+1:]...)
			break
		}
	}
	delete(c.hints, rangeID)
}

func (c *rangeCache) Hint(rangeID base.RangeID) base.NodeID {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.hints[rangeID]
}

func (c *rangeCache) SetHint(rangeID base.RangeID, node base.NodeID) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if node == 0 {
		delete(c.hints, rangeID)
	} else {
		c.hints[rangeID] = node
	}
}
