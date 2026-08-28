package kvserver

import (
	"sync"

	"github.com/sthorne/datax/pkg/util/hlc"
)

// tsCache is the range's read timestamp cache, v1 edition: a single
// high-water mark of the maximum timestamp at which any read was served.
// Writers at or below it are pushed above (they would otherwise invalidate
// a read that already happened). Coarse — any read pushes all writers on
// the range — but small and correct. It lives on the leader; on leadership
// acquisition it is bumped to now() because a new leader cannot know what
// the old one served.
type tsCache struct {
	mu  sync.Mutex
	low hlc.Timestamp
}

func (c *tsCache) Bump(ts hlc.Timestamp) {
	c.mu.Lock()
	c.low = c.low.Forward(ts)
	c.mu.Unlock()
}

func (c *tsCache) Get() hlc.Timestamp {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.low
}
