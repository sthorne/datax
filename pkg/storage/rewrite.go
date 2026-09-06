package storage

import (
	"context"

	"github.com/cockroachdb/pebble/v2"
)

// Background rewrite of the sstables written before prefix mode (issue
// #161, comparer.go). Their bloom filters hash whole keys under the old
// policy name, so a prefix-mode reader does not consult them: reads of
// those tables are correct and merely skip nothing until compaction
// rewrites them with prefix filters. Natural churn does that for hot
// data; the cold bulk of a store rests in L6 and would keep its old
// filters indefinitely, so the node runs this pass once after a store's
// first prefix-mode open. It is the re-encryption pass's machinery
// (reencrypt.go, issue #69) selecting by a different property: the
// table's filter policy name.

// filterStaleTables sweeps the live sstables and returns those whose
// filter is not the prefix-mode one; none unless the engine runs in
// prefix mode.
func (e *Engine) filterStaleTables() ([]staleTable, error) {
	if !e.prefixBloom {
		return nil, nil
	}
	levels, err := e.db.SSTables(pebble.WithProperties())
	if err != nil {
		return nil, err
	}
	var out []staleTable
	for l, level := range levels {
		for _, t := range level {
			if t.Properties != nil && t.Properties.FilterPolicyName == mvccPrefixBloomName {
				continue
			}
			out = append(out, staleTable{
				fileNum:  uint64(t.FileNum),
				level:    l,
				size:     t.Size,
				smallest: append([]byte(nil), t.Smallest.UserKey...),
				largest:  append([]byte(nil), t.Largest.UserKey...),
			})
		}
	}
	return out, nil
}

// FilterRewriteStatus reports live sstable bytes and files still carrying
// whole-key filters (zero for an engine not in prefix mode), refreshed at
// most every reencStatusTTL; the same caveats as ReencryptionStatus.
func (e *Engine) FilterRewriteStatus() (remainingBytes int64, remainingFiles int, err error) {
	return e.filterRewrite.status(e.filterStaleTables)
}

// FilterRewritePass compacts up to maxBytes (0 = unlimited) of the
// sstables still carrying whole-key filters, as ReencryptPass does for
// retired keys, then re-sweeps; callers loop until remaining hits zero.
func (e *Engine) FilterRewritePass(ctx context.Context, maxBytes int64, attempted map[uint64]bool) (targeted int64, remainingBytes int64, remainingFiles int, err error) {
	stale, err := e.filterStaleTables()
	if err != nil {
		return 0, 0, 0, err
	}
	if targeted, err = e.compactStale(ctx, stale, maxBytes, attempted); err != nil {
		return targeted, 0, 0, err
	}
	stale, err = e.filterStaleTables()
	if err != nil {
		return targeted, 0, 0, err
	}
	remainingBytes, remainingFiles = sumStale(stale)
	e.filterRewrite.record(remainingBytes, remainingFiles)
	return targeted, remainingBytes, remainingFiles, nil
}
