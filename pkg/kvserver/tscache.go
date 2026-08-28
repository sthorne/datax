package kvserver

import (
	"sync"

	"github.com/google/uuid"
	"github.com/sthorne/datax/pkg/util/hlc"
)

// tsCache is the range's read timestamp cache, v1 edition: a single
// high-water mark of the maximum timestamp at which any read was served,
// with the reading transaction's ID attached. Writers strictly below the
// mark are pushed above it; a writer AT the mark is allowed only if it is
// the same transaction that performed the read (reading your own key and
// then writing it is the normal transactional pattern). If two different
// transactions read at the identical timestamp the ID degrades to nil,
// which conservatively rejects all equal-timestamp writes.
//
// It lives on the leader; on leadership acquisition it is bumped to now(),
// because a new leader cannot know what the old one served.
type tsCache struct {
	mu    sync.Mutex
	low   hlc.Timestamp
	txnID uuid.UUID // reader at low; Nil = unknown/multiple
}

// Bump records a read at ts by the given transaction (uuid.Nil for
// non-transactional reads).
func (c *tsCache) Bump(ts hlc.Timestamp, txnID uuid.UUID) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.low.Less(ts) {
		c.low, c.txnID = ts, txnID
	} else if c.low.Equal(ts) && c.txnID != txnID {
		c.txnID = uuid.Nil
	}
}

// AllowsWrite reports whether a write at ts by txnID (uuid.Nil for
// non-transactional) may proceed, and if not, the floor it must exceed.
func (c *tsCache) AllowsWrite(ts hlc.Timestamp, txnID uuid.UUID) (bool, hlc.Timestamp) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.low.Less(ts) {
		return true, c.low
	}
	if c.low.Equal(ts) && txnID != uuid.Nil && txnID == c.txnID {
		return true, c.low
	}
	return false, c.low
}
