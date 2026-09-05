package storage

import (
	"bytes"
	"fmt"
	"math/rand"
	"testing"

	"github.com/google/uuid"

	"github.com/sthorne/datax/pkg/keys"
	"github.com/sthorne/datax/pkg/storage/enginepb"
	"github.com/sthorne/datax/pkg/util/hlc"
)

// scanSeekPerRow is the scan as it was before issue #160 — a seek to the
// next user key after every row, and a seek to (key, ts) inside the
// per-key read — kept here as the oracle for the stepping scan. Its
// per-key read is the pre-#160 mvccScanKey verbatim except for the
// helper signature.
func scanSeekPerRow(r Reader, start, end keys.Key, ts hlc.Timestamp, max int64, opts MVCCGetOptions, reverse bool) (ScanResult, error) {
	lower := EncodeMVCCKey(start, hlc.Timestamp{})
	upper := EncodeMVCCKey(end, hlc.Timestamp{})
	it := r.NewIter(lower, upper)
	defer func() { _ = it.Close() }()
	var res ScanResult
	var intents []Intent
	var total int64
	var ok bool
	if reverse {
		ok = it.SeekLT(upper)
	} else {
		ok = it.SeekGE(lower)
	}
	for ok {
		userKey, _, err := DecodeMVCCKey(it.Key())
		if err != nil {
			return ScanResult{}, err
		}
		cur := keys.Key(userKey).Clone()
		curStart := EncodeMVCCKey(cur, hlc.Timestamp{})
		if reverse && !it.SeekGE(curStart) {
			break
		}
		value, found, err := seekScanKey(it, cur, ts, opts, &intents)
		if err != nil {
			return ScanResult{}, err
		}
		if found && len(intents) == 0 {
			res.KVs = append(res.KVs, KeyValue{Key: cur, Value: value})
			total += int64(len(cur) + len(value))
			if max > 0 && int64(len(res.KVs)) == max || opts.TargetBytes > 0 && total >= opts.TargetBytes {
				if reverse {
					res.Resume = cur
				} else {
					res.Resume = cur.Next()
				}
				return res, nil
			}
		}
		if reverse {
			ok = it.SeekLT(curStart)
		} else {
			ok = it.SeekGE(EncodeMVCCKey(cur.Next(), hlc.Timestamp{}))
		}
	}
	if len(intents) > 0 {
		return ScanResult{}, &WriteIntentError{Intents: intents}
	}
	return res, nil
}

func seekScanKey(it Iterator, cur keys.Key, ts hlc.Timestamp, opts MVCCGetOptions, intents *[]Intent) ([]byte, bool, error) {
	_, vts, err := DecodeMVCCKey(it.Key())
	if err != nil {
		return nil, false, err
	}
	var readAt, skipAt hlc.Timestamp
	if vts.IsEmpty() {
		meta, err := decodeMeta(it.Value())
		if err != nil {
			return nil, false, err
		}
		switch {
		case opts.Txn != nil && meta.Txn.ID == opts.Txn.ID:
			if meta.Txn.Epoch == opts.Txn.Epoch {
				readAt = meta.Timestamp
			} else {
				skipAt = meta.Timestamp
			}
		case opts.Inconsistent:
			skipAt = meta.Timestamp
		case intentAboveRead(meta.Timestamp, ts, opts.UncertaintyLimit):
			skipAt = meta.Timestamp
		default:
			*intents = append(*intents, Intent{Key: cur, Txn: meta.Txn})
			if len(*intents) >= maxIntentsPerError {
				return nil, false, &WriteIntentError{Intents: *intents}
			}
			return nil, false, nil
		}
		if !it.Next() {
			return nil, false, nil
		}
	}
	var value []byte
	var found bool
	if !readAt.IsEmpty() {
		if it.SeekGE(EncodeMVCCKey(cur, readAt)) {
			k, pvts, err := DecodeMVCCKey(it.Key())
			if err != nil {
				return nil, false, err
			}
			if bytes.Equal(k, cur) && pvts.Equal(readAt) {
				data, tombstone, err := decodeMVCCValue(it.Value())
				if err != nil {
					return nil, false, err
				}
				if !tombstone {
					value, found = append([]byte(nil), data...), true
				}
			}
		}
		return value, found, nil
	}
	if len(*intents) == 0 && !opts.UncertaintyLimit.IsEmpty() && ts.Less(opts.UncertaintyLimit) {
		for uok := it.Valid(); uok; uok = it.Next() {
			ku, uvts, err := DecodeMVCCKey(it.Key())
			if err != nil {
				return nil, false, err
			}
			if !bytes.Equal(ku, cur) {
				break
			}
			if uvts.LessEq(opts.UncertaintyLimit) && !(!skipAt.IsEmpty() && uvts.Equal(skipAt)) {
				if ts.Less(uvts) {
					return nil, false, &UncertaintyError{ReadTimestamp: ts, ExistingTimestamp: uvts}
				}
				break
			}
		}
	}
	ok := it.SeekGE(EncodeMVCCKey(cur, ts))
	for ok {
		k, cvts, err := DecodeMVCCKey(it.Key())
		if err != nil {
			return nil, false, err
		}
		if !bytes.Equal(k, cur) {
			break
		}
		if !skipAt.IsEmpty() && cvts.Equal(skipAt) {
			ok = it.Next()
			continue
		}
		data, tombstone, err := decodeMVCCValue(it.Value())
		if err != nil {
			return nil, false, err
		}
		if !tombstone {
			value, found = append([]byte(nil), data...), true
		}
		break
	}
	return value, found, nil
}

// TestScanStepMatchesSeekPerRow (issue #160): a keyspace with version
// chains of every depth from one to well past the step bound, tombstones,
// the reader's own intents (current and old epoch) and foreign intents
// above and below the read; forward and reverse scans at several
// timestamps, with limits, with an uncertainty window, as the intent's
// owner and as a stranger — the stepping scan returns exactly what the
// seek-per-row scan does, rows and errors alike.
func TestScanStepMatchesSeekPerRow(t *testing.T) {
	eng := openTestEngine(t)
	rng := rand.New(rand.NewSource(160))
	// Committed versions land at 100–340; intents sit above them all.
	me := &enginepb.TxnMeta{ID: uuid.New(), Epoch: 2, WriteTimestamp: ts(500, 0)}
	other := &enginepb.TxnMeta{ID: uuid.New(), Epoch: 1, WriteTimestamp: ts(400, 0)}
	otherHigh := &enginepb.TxnMeta{ID: uuid.New(), Epoch: 1, WriteTimestamp: ts(900, 0)}
	for i := 0; i < 300; i++ {
		k := fmt.Sprintf("k%04d", i)
		depth := []int{1, 1, 2, 3, 7, 8, 9, 12, 25}[rng.Intn(9)]
		for v := 0; v < depth; v++ {
			at := ts(int64(100+v*10), int32(rng.Intn(3)))
			if rng.Intn(6) == 0 {
				mustDelete(t, eng, k, at, nil)
			} else {
				mustPut(t, eng, k, at, fmt.Sprintf("%s@%d", k, v), nil)
			}
		}
		switch rng.Intn(12) {
		case 0:
			// The reader's own intent, current epoch.
			mustPut(t, eng, k, me.WriteTimestamp, k+"-mine", me)
		case 1:
			// The reader's own intent from an older epoch (skipped).
			old := *me
			old.Epoch = 1
			mustPut(t, eng, k, ts(500, 0), k+"-old-epoch", &old)
		case 2:
			// A foreign intent at 400: blocks reads at or above it, is read
			// beneath by reads below it.
			mustPut(t, eng, k, other.WriteTimestamp, k+"-theirs", other)
		case 3:
			// A foreign intent at 900: the same, higher up.
			mustPut(t, eng, k, otherHigh.WriteTimestamp, k+"-theirs-high", otherHigh)
		}
	}
	type scanCase struct {
		name string
		opts MVCCGetOptions
	}
	cases := []scanCase{
		{"plain", MVCCGetOptions{}},
		{"inconsistent", MVCCGetOptions{Inconsistent: true}},
		{"owner", MVCCGetOptions{Txn: me}},
		{"uncertain", MVCCGetOptions{UncertaintyLimit: ts(175, 0)}},
		{"uncertain-wide", MVCCGetOptions{UncertaintyLimit: ts(700, 0)}},
		{"owner-uncertain", MVCCGetOptions{Txn: me, UncertaintyLimit: ts(600, 0)}},
		{"target-bytes", MVCCGetOptions{Inconsistent: true, TargetBytes: 700}},
	}
	starts := []string{"k0000", "k0050", "k0123", "k0299", "k0300"}
	ends := []string{"k0010", "k0077", "k0200", "k0300", "k9999"}
	for _, c := range cases {
		for _, readAt := range []hlc.Timestamp{ts(95, 0), ts(100, 0), ts(115, 1), ts(150, 0), ts(190, 2), ts(350, 0), ts(450, 0), ts(500, 0), ts(600, 0), ts(1000, 0)} {
			for i := range starts {
				for _, max := range []int64{0, 1, 7} {
					for _, reverse := range []bool{false, true} {
						start, end := keys.Key(starts[i]), keys.Key(ends[i])
						var want, got ScanResult
						var werr, gerr error
						want, werr = scanSeekPerRow(eng, start, end, readAt, max, c.opts, reverse)
						if reverse {
							got, gerr = MVCCReverseScan(eng, start, end, readAt, max, c.opts)
						} else {
							got, gerr = MVCCScan(eng, start, end, readAt, max, c.opts)
						}
						label := fmt.Sprintf("%s ts=%s [%s,%s) max=%d reverse=%v", c.name, readAt, start, end, max, reverse)
						if (werr == nil) != (gerr == nil) || (werr != nil && werr.Error() != gerr.Error()) {
							t.Fatalf("%s: errors differ: seek-per-row %v, stepping %v", label, werr, gerr)
						}
						if werr != nil {
							continue
						}
						if len(got.KVs) != len(want.KVs) || !bytes.Equal(got.Resume, want.Resume) {
							t.Fatalf("%s: %d rows (resume %q), want %d (resume %q)", label, len(got.KVs), got.Resume, len(want.KVs), want.Resume)
						}
						for j := range want.KVs {
							if !bytes.Equal(got.KVs[j].Key, want.KVs[j].Key) || !bytes.Equal(got.KVs[j].Value, want.KVs[j].Value) {
								t.Fatalf("%s: row %d %q=%q, want %q=%q", label, j, got.KVs[j].Key, got.KVs[j].Value, want.KVs[j].Key, want.KVs[j].Value)
							}
						}
					}
				}
			}
		}
	}
}
