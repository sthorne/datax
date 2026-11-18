package server

import (
	"bufio"
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"hash"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/sthorne/datax/pkg/keys"
	"github.com/sthorne/datax/pkg/kvpb"
	"github.com/sthorne/datax/pkg/sql/catalog"
	"github.com/sthorne/datax/pkg/storage/enc"
	"github.com/sthorne/datax/pkg/util/encoding"
	"github.com/sthorne/datax/pkg/util/hlc"
)

// Revision history and point-in-time restore (issue #302).
//
// An incremental backup holds, for each key changed since its base, the
// key as it stood at the backup's own timestamp: enough to bring a chain
// to its end and to nowhere in between. An incremental taken with
// revision history holds every version written in (base, end], newest
// first within a key, each with its timestamp. A restore to a moment T
// inside that interval applies the chain before it as always and then,
// from this one file, the newest version of each key at or below T — a
// value, a tombstone, or nothing where the key did not change by then.
//
// A transaction's writes all carry its commit timestamp, on every range
// it touched, so cutting at T takes each transaction whole or not at all.
// The export waits out or pushes any intent at or below the backup's
// timestamp (exportAll), so no version in the window is missing because
// its transaction had not finished resolving.
//
// What a history backup does not hold is the history of the schema. The
// descriptors, roles and sequences in a manifest are those at its end
// timestamp, as in every backup. A restore to a moment inside an
// interval therefore refuses a table whose definition changed in that
// interval (tableStableAcross): its rows at T are in the layout of a
// descriptor the backup does not have.

// writeBackupRevision frames one version of a key: a record as
// writeBackupRecord frames it, then the version's timestamp as two
// uvarints, wall time and logical.
func writeBackupRevision(w io.Writer, rec kvpb.ExportRecord) error {
	if err := writeBackupRecord(w, rec); err != nil {
		return err
	}
	buf := encoding.EncodeUvarint(nil, uint64(rec.Timestamp.WallTime))
	buf = encoding.EncodeUvarint(buf, uint64(rec.Timestamp.Logical))
	_, err := w.Write(buf)
	return err
}

// backupReader reads a data file's records one at a time, holding one
// record (and, for a sealed file, one chunk) in memory.
type backupReader struct {
	path    string
	f       *os.File
	r       *bufio.Reader
	history bool
}

// openBackupRecords opens a data file for reading. dataKey is the
// backup's data key for a sealed backup and nil for a plaintext one;
// history says the records carry timestamps.
func openBackupRecords(path string, dataKey []byte, history bool) (*backupReader, error) {
	f, err := openBackupFile(path)
	if err != nil {
		return nil, err
	}
	var src io.Reader = f
	if dataKey != nil {
		// The name bound into the file is the one it is being read under.
		if src, err = enc.NewStreamReader(f, dataKey, filepath.Base(path)); err != nil {
			_ = f.Close()
			return nil, fmt.Errorf("%s: %w", path, err)
		}
	}
	r := bufio.NewReaderSize(src, 64<<10)
	if dataKey == nil {
		// A sealed file read without its key would otherwise be parsed
		// as length-prefixed records, and random ciphertext now and
		// then parses cleanly.
		if hdr, _ := r.Peek(enc.SealedStreamHeaderLen); enc.IsSealedStreamHeader(hdr) {
			_ = f.Close()
			return nil, fmt.Errorf("%s is a sealed backup file and no key was given", path)
		}
	}
	return &backupReader{path: path, f: f, r: r, history: history}, nil
}

func (rd *backupReader) Close() { _ = rd.f.Close() }

// field reads n bytes. It grows with what arrives rather than allocating
// n up front: n is whatever a damaged plaintext file says.
func (rd *backupReader) field(n uint64, what string) ([]byte, error) {
	var buf bytes.Buffer
	if _, err := io.CopyN(&buf, rd.r, int64(n)); err != nil {
		if err == io.EOF {
			return nil, fmt.Errorf("%s: truncated record %s", rd.path, what)
		}
		return nil, fmt.Errorf("%s: %w", rd.path, err)
	}
	return buf.Bytes(), nil
}

// uvarint reads one length or timestamp part. first says the end of the
// file is acceptable here, between records.
func (rd *backupReader) uvarint(first bool) (uint64, bool, error) {
	n, err := binary.ReadUvarint(rd.r)
	switch {
	case err == nil:
		return n, false, nil
	case err == io.EOF && first:
		return 0, true, nil
	}
	// A length cut short or malformed, or — from a sealed file — a chunk
	// that did not authenticate, which the error then says.
	return 0, false, fmt.Errorf("%s: corrupt record framing: %w", rd.path, err)
}

// next returns the file's next record; ok is false at its end.
func (rd *backupReader) next() (rec kvpb.ExportRecord, ok bool, err error) {
	klen, end, err := rd.uvarint(true)
	if err != nil || end {
		return rec, false, err
	}
	key, err := rd.field(klen, "key")
	if err != nil {
		return rec, false, err
	}
	vlen, _, err := rd.uvarint(false)
	if err != nil {
		return rec, false, err
	}
	val, err := rd.field(vlen, "value")
	if err != nil {
		return rec, false, err
	}
	deleted, err := rd.r.ReadByte()
	if err == io.EOF {
		return rec, false, fmt.Errorf("%s: truncated record value", rd.path)
	} else if err != nil {
		return rec, false, fmt.Errorf("%s: %w", rd.path, err)
	}
	rec = kvpb.ExportRecord{Key: keys.Key(key), Deleted: deleted == 1}
	if !rec.Deleted {
		rec.Value = val
	}
	if rd.history {
		wall, _, err := rd.uvarint(false)
		if err != nil {
			return rec, false, err
		}
		logical, _, err := rd.uvarint(false)
		if err != nil {
			return rec, false, err
		}
		if wall > 1<<63-1 || logical > 1<<31-1 {
			return rec, false, fmt.Errorf("%s: a record carries a timestamp no clock produces", rd.path)
		}
		rec.Timestamp = hlc.Timestamp{WallTime: int64(wall), Logical: int32(logical)}
	}
	return rec, true, nil
}

// tableStream reads one table's data file as one record per key: the
// file's records as they are for a backup without history, and for one
// with history the newest version of each key at or below cutoff (the
// newest of all when cutoff is zero). A key with no version at or below
// the cutoff did not change by then and yields nothing. A record carries
// its commit time when the file has one (backupTable.timed).
//
// Every record is held to the table's span before it is acted on
// (checkBackupRecord, issue #255), and a history file to its own shape:
// keys ascending, a key's versions strictly descending, every timestamp
// inside the backup's interval. Restore picks versions by position, so a
// file out of that order would have it pick one that is not the newest.
// ordered asks the same of a file without history (keys strictly
// ascending), which the merge of a table restore relies on — and, at the
// file's end, that the file is the one its manifest describes: the same
// number of records and the same checksum. A restore of everything
// reports checksums for the operator to compare; a table restore lands
// beside live data with nobody comparing, so it holds each file to its
// manifest itself. That catches a damaged or swapped plaintext file; it
// is not a seal — whoever can rewrite the file can rewrite the manifest —
// and a sealed backup needs none of it to be safe.
type tableStream struct {
	rd      *backupReader
	t       backupTable
	cutoff  hlc.Timestamp
	ordered bool

	// What the file has held so far, counted the way backup counts it.
	sum         hash.Hash
	keys, total int64

	// The record read ahead of the group being decided, if any.
	ahead    kvpb.ExportRecord
	hasAhead bool
	done     bool
	lastKey  keys.Key
}

func openTableStream(path string, dataKey []byte, t backupTable, cutoff hlc.Timestamp, ordered bool) (*tableStream, error) {
	rd, err := openBackupRecords(path, dataKey, t.timed())
	if err != nil {
		return nil, err
	}
	return &tableStream{rd: rd, t: t, cutoff: cutoff, ordered: ordered, sum: sha256.New()}, nil
}

// checkManifest is the comparison made at the end of an ordered stream.
func (s *tableStream) checkManifest() error {
	versions := s.total
	if !s.t.history {
		versions = 0
	}
	if got := hex.EncodeToString(s.sum.Sum(nil)); got != s.t.SHA256 || s.keys != s.t.Records || versions != s.t.Versions {
		return fmt.Errorf("%s is not the file its backup's manifest describes for table %s (it holds %d records with checksum %s; the manifest says %d with %s): the file was damaged or replaced after the backup wrote it",
			s.rd.path, s.t.Name, s.keys, got, s.t.Records, s.t.SHA256)
	}
	return nil
}

func (s *tableStream) Close() { s.rd.Close() }

// read returns the file's next raw record, checked against the table's
// span and, in a history file, the backup's interval.
func (s *tableStream) read() (kvpb.ExportRecord, bool, error) {
	if s.hasAhead {
		s.hasAhead = false
		return s.ahead, true, nil
	}
	if s.done {
		return kvpb.ExportRecord{}, false, nil
	}
	rec, ok, err := s.rd.next()
	if err != nil {
		return rec, false, err
	}
	if !ok {
		s.done = true
		if s.ordered {
			return rec, false, s.checkManifest()
		}
		return rec, false, nil
	}
	s.total++
	if err := checkBackupRecord(s.rd.path, s.t, rec); err != nil {
		return rec, false, err
	}
	if s.t.timed() {
		// A commit time too: a full backup's interval starts at zero.
		if ts := rec.Timestamp; !s.t.window[0].Less(ts) || s.t.window[1].Less(ts) {
			return rec, false, fmt.Errorf("%s: table %s's data file carries a version at %s, outside the backup's own interval (%s, %s]",
				s.rd.path, s.t.Name, ts, s.t.window[0], s.t.window[1])
		}
	}
	return rec, true, nil
}

// next returns the stream's next key; ok is false at the end of the file.
func (s *tableStream) next() (kvpb.ExportRecord, bool, error) {
	for {
		first, ok, err := s.read()
		if err != nil || !ok {
			return first, false, err
		}
		if s.lastKey != nil && (s.ordered || s.t.history) && s.lastKey.Compare(first.Key) >= 0 {
			return first, false, fmt.Errorf("%s: table %s's data file is out of key order at %s: not a file datax backup wrote",
				s.rd.path, s.t.Name, keys.Key(first.Key))
		}
		if s.ordered || s.t.history {
			s.lastKey = keys.Key(first.Key).Clone()
		}
		// The newest version of the key is what a backup counts and
		// hashes, with or without history.
		s.keys++
		if !first.Deleted {
			hashLiveRecord(s.sum, first)
		}
		if !s.t.history {
			return first, true, nil
		}
		// The rest of the key's versions, to the first record of the next
		// key: the one to keep is the first at or below the cutoff.
		chosen, have := first, s.cutoff.IsEmpty() || first.Timestamp.LessEq(s.cutoff)
		prev := first.Timestamp
		for {
			rec, ok, err := s.read()
			if err != nil {
				return rec, false, err
			}
			if !ok {
				break
			}
			if !keys.Key(rec.Key).Equal(first.Key) {
				s.ahead, s.hasAhead = rec, true
				break
			}
			if !rec.Timestamp.Less(prev) {
				return rec, false, fmt.Errorf("%s: table %s's data file holds the versions of %s out of order: not a file datax backup wrote",
					s.rd.path, s.t.Name, keys.Key(rec.Key))
			}
			prev = rec.Timestamp
			if !have && rec.Timestamp.LessEq(s.cutoff) {
				chosen, have = rec, true
			}
		}
		if have {
			return chosen, true, nil
		}
	}
}

// readTableRecords streams one table's data file through fn as one record
// per key (tableStream), reading the file to its end either way — which
// for a sealed file authenticates all of it.
func readTableRecords(path string, dataKey []byte, t backupTable, cutoff hlc.Timestamp, ordered bool, fn func(kvpb.ExportRecord) error) error {
	s, err := openTableStream(path, dataKey, t, cutoff, ordered)
	if err != nil {
		return err
	}
	defer s.Close()
	for {
		rec, ok, err := s.next()
		if err != nil || !ok {
			return err
		}
		if err := fn(rec); err != nil {
			return err
		}
	}
}

// formatBackupTS writes a backup timestamp the way --as-of reads one:
// Unix nanoseconds, with ",<logical>" when the logical part is not zero.
func formatBackupTS(ts hlc.Timestamp) string {
	if ts.Logical == 0 {
		return strconv.FormatInt(ts.WallTime, 10)
	}
	return fmt.Sprintf("%d,%d", ts.WallTime, ts.Logical)
}

// describeBackupTS is formatBackupTS with the wall-clock time beside it,
// for a message an operator reads.
func describeBackupTS(ts hlc.Timestamp) string {
	return fmt.Sprintf("%s (%s)", formatBackupTS(ts), time.Unix(0, ts.WallTime).UTC().Format(time.RFC3339Nano))
}

// ParseBackupTS reads a restore's --as-of: Unix nanoseconds, optionally
// with ",<logical>" (what a backup's summary prints), or an RFC 3339
// time. It is the same timestamp AS OF SYSTEM TIME takes, so a moment
// checked there with a query is the moment restored to here.
func ParseBackupTS(s string) (hlc.Timestamp, error) {
	s = strings.TrimSpace(s)
	wallPart, logicalPart, hasLogical := strings.Cut(s, ",")
	if wall, err := strconv.ParseInt(wallPart, 10, 64); err == nil {
		ts := hlc.Timestamp{WallTime: wall}
		if hasLogical {
			l, lerr := strconv.ParseInt(logicalPart, 10, 32)
			if lerr != nil || l < 0 {
				return hlc.Timestamp{}, fmt.Errorf("cannot interpret %q as a timestamp: the part after the comma is the logical counter, a small non-negative integer", s)
			}
			ts.Logical = int32(l)
		}
		if wall <= 0 {
			return hlc.Timestamp{}, fmt.Errorf("timestamp %q is not a time", s)
		}
		return ts, nil
	}
	if t, err := time.Parse(time.RFC3339Nano, s); err == nil {
		return hlc.Timestamp{WallTime: t.UnixNano()}, nil
	}
	return hlc.Timestamp{}, fmt.Errorf("cannot interpret %q as Unix nanoseconds (optionally \"<nanos>,<logical>\") or an RFC 3339 time", s)
}

// restorableTimes says, for an error message, which moments a chain
// restores to: the end of each backup, and every moment of an interval
// backed up with revision history.
func restorableTimes(mans []*backupManifest) string {
	var parts []string
	for i := 0; i < len(mans); i++ {
		if !mans[i].RevisionHistory {
			parts = append(parts, "exactly "+describeBackupTS(mans[i].EndTS))
			continue
		}
		// Consecutive history intervals read as one.
		j := i
		for j+1 < len(mans) && mans[j+1].RevisionHistory {
			j++
		}
		from := mans[i].BaseTS
		if len(parts) > 0 {
			// The interval's start is the previous backup's end, already
			// listed; fold the two.
			parts = parts[:len(parts)-1]
		}
		parts = append(parts, fmt.Sprintf("any time from %s to %s", describeBackupTS(from), describeBackupTS(mans[j].EndTS)))
		i = j
	}
	return strings.Join(parts, "; ")
}

// chainCut decides what a restore to asOf reads of a chain: its first n
// backups, and — when asOf falls inside the last of those rather than at
// its end — the cutoff to read that one's history to. A zero asOf is the
// end of the chain. A moment before the full backup, after the last
// backup, or inside an interval backed up without history is refused with
// what the chain does restore to.
func chainCut(srcs []string, mans []*backupManifest, asOf hlc.Timestamp) (n int, cutoff hlc.Timestamp, err error) {
	if asOf.IsEmpty() {
		return len(mans), hlc.Timestamp{}, nil
	}
	for i, man := range mans {
		if asOf.Equal(man.EndTS) {
			return i + 1, hlc.Timestamp{}, nil
		}
		if i == 0 || !man.BaseTS.Less(asOf) || !asOf.Less(man.EndTS) {
			continue
		}
		if !man.RevisionHistory {
			return 0, hlc.Timestamp{}, fmt.Errorf("cannot restore to %s: it falls between two backups, and %s was taken without --revision-history, so it holds the keys as they stood at its end and not the versions in between. This chain restores to: %s",
				describeBackupTS(asOf), srcs[i], restorableTimes(mans))
		}
		return i + 1, asOf, nil
	}
	which := "is later than the chain's last backup"
	if asOf.Less(mans[0].EndTS) {
		which = "is earlier than the full backup"
	}
	return 0, hlc.Timestamp{}, fmt.Errorf("cannot restore to %s: it %s. This chain restores to: %s", describeBackupTS(asOf), which, restorableTimes(mans))
}

// tableStableAcross refuses a table whose definition is not the same at
// the two ends of the interval a restore cuts into. The backup holds the
// descriptor at the interval's end only; rows at a moment inside it are
// laid out by whatever descriptor was current then, which is known to be
// this one only if nothing changed. What cannot change a row's layout —
// the owner, grants, comments, other tables' foreign keys pointing here,
// and the version counter those bump — is left out of the comparison.
func tableStableAcross(prev, cur *backupManifest, t backupTable, cutoff hlc.Timestamp) error {
	if t.Full {
		// The interval's history of the table had been collected when the
		// backup was taken (issue #376); it holds the table at its end.
		return fmt.Errorf("cannot restore table %s to %s: the backup ending at %s holds it only as it stood then, because its history since %s had been collected (a purge, or garbage collection past the base). Restore to %s instead",
			t.Name, describeBackupTS(cutoff), describeBackupTS(cur.EndTS), describeBackupTS(prev.EndTS), describeBackupTS(cur.EndTS))
	}
	layout := func(raw json.RawMessage) (string, error) {
		var d catalog.TableDescriptor
		if err := json.Unmarshal(raw, &d); err != nil {
			return "", err
		}
		d.Owner, d.Privileges, d.GrantOptions = "", nil, nil
		d.Comment, d.Version, d.InboundFKs = "", 0, nil
		for i := range d.Columns {
			d.Columns[i].Comment = ""
		}
		for i := range d.Indexes {
			d.Indexes[i].Comment = ""
		}
		out, err := json.Marshal(&d)
		return string(out), err
	}
	for _, p := range prev.Tables {
		if p.ID != t.ID {
			continue
		}
		before, err := layout(p.Descriptor)
		if err != nil {
			return fmt.Errorf("table %s: corrupt descriptor in the backup: %w", p.Name, err)
		}
		after, err := layout(t.Descriptor)
		if err != nil {
			return fmt.Errorf("table %s: corrupt descriptor in the backup: %w", t.Name, err)
		}
		if before != after {
			return fmt.Errorf("cannot restore table %s to %s: its definition changed between %s and %s, and a backup holds a table's definition at its end only, not as it was in between. Restore to either of those two times instead",
				t.Name, describeBackupTS(cutoff), describeBackupTS(prev.EndTS), describeBackupTS(cur.EndTS))
		}
		return nil
	}
	if catalog.IsSystemTableID(t.ID) {
		// The cluster's own table (the audit log, the metrics table),
		// made by a node inside the interval. Its definition is the
		// binary's, not a user's, so "as what" is known, and its rows at
		// the cut are what the history holds up to it: none if it did not
		// exist yet. Refusing would block a point-in-time restore over a
		// table the operator never made (issue #327).
		return nil
	}
	return fmt.Errorf("cannot restore table %s to %s: it was created between %s and %s, and a backup holds a table's definition at its end only, so whether it existed at that moment, and as what, is not known. Restore to %s instead",
		t.Name, describeBackupTS(cutoff), describeBackupTS(prev.EndTS), describeBackupTS(cur.EndTS), describeBackupTS(cur.EndTS))
}
