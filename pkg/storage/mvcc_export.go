package storage

import (
	"bytes"

	"github.com/sthorne/datax/pkg/keys"
	"github.com/sthorne/datax/pkg/util/hlc"
)

// ExportedRecord is one key MVCCExport emits: the newest version at or
// below the export timestamp of a key that changed inside the window.
// Deleted marks a tombstone (the change was a deletion); Value is nil then.
type ExportedRecord struct {
	Key     keys.Key
	Value   []byte
	Deleted bool
	// Timestamp is the version's own timestamp: its commit time. An
	// export of every revision yields several records per key, each with
	// its own; an export of the newest versions carries the newest one's,
	// which is what a backup of a time-series table keeps so that a
	// restore can apply the table's retention (issue #316).
	Timestamp hlc.Timestamp
}

// ExportResult is the outcome of an MVCCExport.
type ExportResult struct {
	Records []ExportedRecord
	// Resume, if non-nil, is where a continuation export should start (the
	// export stopped early because it reached max records).
	Resume keys.Key
}

// MVCCExport walks [start, end) and emits, for every user key whose newest
// version at or below endTS lies in (startTS, endTS], that version — a
// value or a tombstone. startTS zero therefore exports every key visible
// at endTS plus nothing deleted (a full backup ignores tombstones' keys
// only when they were never written before — a key whose newest version is
// a tombstone still emits a Deleted record, which restore treats as a
// no-op on an empty target); non-zero startTS exports the delta since a
// prior export at startTS, deletions included.
//
// Consistency matches MVCCScan: a foreign intent at or below endTS may
// commit inside the window, so it conflicts (WriteIntentError, aggregated
// up to maxIntentsPerError); intents strictly above endTS cannot commit at
// or below it (resolution only moves timestamps forward) and are read
// beneath. Export is always non-transactional and never uncertain — endTS
// is a past timestamp the caller chose.
func MVCCExport(r Reader, start, end keys.Key, startTS, endTS hlc.Timestamp, max int64) (ExportResult, error) {
	return mvccExport(r, start, end, startTS, endTS, max, false)
}

// MVCCExportRevisions is MVCCExport keeping the history: for every user
// key it emits every version in (startTS, endTS], newest first, each with
// its timestamp — what a point-in-time restore replays up to a chosen
// moment (issue #302). The window is whole only while garbage collection
// has not passed startTS; the caller refuses the export otherwise (the
// replica does, against its GC threshold).
//
// A key's versions are never split across calls: max is checked when a
// key is finished, so a result may run past it by the versions of its
// last key, and Resume always names a key boundary.
func MVCCExportRevisions(r Reader, start, end keys.Key, startTS, endTS hlc.Timestamp, max int64) (ExportResult, error) {
	return mvccExport(r, start, end, startTS, endTS, max, true)
}

func mvccExport(r Reader, start, end keys.Key, startTS, endTS hlc.Timestamp, max int64, revisions bool) (ExportResult, error) {
	lower := EncodeMVCCKey(start, hlc.Timestamp{})
	upper := EncodeMVCCKey(end, hlc.Timestamp{})
	it := r.NewIter(lower, upper)
	defer func() { _ = it.Close() }()

	var res ExportResult
	var intents []Intent

	ok := it.SeekGE(lower)
	for ok {
		userKey, vts, err := DecodeMVCCKey(it.Key())
		if err != nil {
			return ExportResult{}, err
		}
		cur := keys.Key(userKey).Clone()

		if vts.IsEmpty() {
			// Intent metadata for cur.
			meta, err := decodeMeta(it.Value())
			if err != nil {
				return ExportResult{}, err
			}
			if !endTS.Less(meta.Timestamp) {
				// The intent could commit at or below endTS: conflict.
				intents = append(intents, Intent{Key: cur, Txn: meta.Txn})
				if len(intents) >= maxIntentsPerError {
					return ExportResult{}, &WriteIntentError{Intents: intents}
				}
				ok = it.SeekGE(EncodeMVCCKey(cur.Next(), hlc.Timestamp{}))
				continue
			}
			// Intent above endTS: its provisional version also sits above
			// endTS, so the seek below lands beneath it naturally.
		}

		// Newest version of cur at or below endTS.
		ok = it.SeekGE(EncodeMVCCKey(cur, endTS))
		for ok {
			k, cvts, err := DecodeMVCCKey(it.Key())
			if err != nil {
				return ExportResult{}, err
			}
			if !bytes.Equal(k, cur) {
				break // no version at or below endTS
			}
			if cvts.LessEq(startTS) {
				break // unchanged inside the window
			}
			data, tombstone, err := decodeMVCCValue(it.Value())
			if err != nil {
				return ExportResult{}, err
			}
			if len(intents) == 0 {
				rec := ExportedRecord{Key: cur, Deleted: tombstone}
				if !tombstone {
					rec.Value = append([]byte(nil), data...)
				}
				rec.Timestamp = cvts
				res.Records = append(res.Records, rec)
				if !revisions && max > 0 && int64(len(res.Records)) == max {
					res.Resume = cur.Next()
					return res, nil
				}
			}
			if !revisions {
				break
			}
			// Older versions of cur follow in the iterator's order.
			ok = it.Next()
		}
		if revisions && max > 0 && int64(len(res.Records)) >= max {
			res.Resume = cur.Next()
			return res, nil
		}

		ok = it.SeekGE(EncodeMVCCKey(cur.Next(), hlc.Timestamp{}))
	}

	if len(intents) > 0 {
		return ExportResult{}, &WriteIntentError{Intents: intents}
	}
	return res, nil
}
