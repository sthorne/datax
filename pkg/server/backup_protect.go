package server

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/sthorne/datax/pkg/base"
	"github.com/sthorne/datax/pkg/keys"
	"github.com/sthorne/datax/pkg/kvclient"
	"github.com/sthorne/datax/pkg/util/hlc"
	"github.com/sthorne/datax/pkg/util/log"
)

// Backup protection (issue #376). A backup reads every table at one
// timestamp, its end, over minutes or hours; a purge, or garbage
// collection under a short gc_ttl, that raises a range's GC threshold to
// that timestamp or past it while the backup runs refuses the backup's
// reads there, and the backup fails. So a backup protects its timestamp:
//
//   - Before it reads anything it writes a record (keys.
//     BackupProtectionKey) with a timestamp at or below the one it will
//     read at, and a lease: an expiry it renews at a quarter of
//     BackupProtectionTTL while it runs. It removes the record when it
//     ends, whether it succeeded or not. A backup whose node died leaves
//     a record that lapses when its lease does, and then holds nothing:
//     a crash cannot block collection for longer than the TTL.
//   - Garbage collection (kvserver.StoreConfig.GCLimit, gcLimit) sets no
//     threshold at or above the oldest live record's timestamp.
//   - A purge (purgeHold, in PurgeSpan, so every purge — ALTER TABLE ...
//     PURGE, `datax debug purge`, a key-range purge, the dropped-table
//     reclaim — goes through it) first waits PurgeBackupWait for the
//     backups in progress to end. If they have not, it purges up to just
//     below the oldest of them, which removes every version deleted
//     before that backup began, and reports itself incomplete, naming
//     the backups (PurgeReport.HeldBy): a version deleted after the
//     backup's timestamp is still on disk. It never reports that as
//     done. The node that ran it purges the span again by itself once
//     those backups have ended (schedulePurgeRerun), as a purge of its
//     own with its own report and audit record; a purge held again by a
//     newer backup schedules another. The rerun is this node's memory:
//     if the node restarts first, the first report still says
//     incomplete, and the purge is the operator's to run again.
//
// Only the backup's own timestamp is protected, not its base. Holding a
// purge to an incremental's base would leave the deleted data's history
// on disk for a whole backup interval; instead an incremental whose base
// has been collected under a table exports that table in full (backup.
// go), and a restore takes the table from there.
//
// The race between a purge and a backup that starts while it reads the
// records is closed by time, the way a transaction's uncertainty is: a
// purge collects no higher than the timestamp it read the records at,
// and a backup takes its timestamp only after its record is written and
// twice the maximum clock offset has passed. A record the purge did not
// see was written after its read, so the backup's timestamp is above it.

const (
	defaultBackupProtectionTTL = 2 * time.Minute
	defaultPurgeBackupWait     = 10 * time.Second
	// purgeHoldPoll is how often a waiting purge, and a scheduled rerun,
	// look at the backups in progress again.
	purgeHoldPoll = 250 * time.Millisecond
	// backupProtectionReadTimeout bounds one read of the records by
	// garbage collection and purges.
	backupProtectionReadTimeout = 10 * time.Second
)

// backupProtection is one backup's record.
type backupProtection struct {
	ID     string      `json:"id"`
	NodeID base.NodeID `json:"node_id"`
	// Dest is the backup's directory on its node, for the operator who
	// reads a held purge's report.
	Dest string `json:"dest"`
	// TS is at or below the timestamp the backup reads at: nothing at or
	// above it may be collected while the record is live. BaseTS is an
	// incremental's base, for the report only: it is not protected.
	TS     hlc.Timestamp `json:"ts"`
	BaseTS hlc.Timestamp `json:"base_ts,omitempty"`
	// ExpiresNanos is the wall time the record lapses at unless renewed.
	ExpiresNanos int64 `json:"expires_nanos"`
}

func (p backupProtection) String() string {
	return fmt.Sprintf("the backup to %s on n%d, reading at %s", p.Dest, p.NodeID, describeBackupTS(p.TS))
}

func (n *Node) backupProtectionTTL() time.Duration {
	if n.cfg.BackupProtectionTTL > 0 {
		return n.cfg.BackupProtectionTTL
	}
	return defaultBackupProtectionTTL
}

func (n *Node) purgeBackupWait() time.Duration {
	switch {
	case n.cfg.PurgeBackupWait < 0:
		return 0
	case n.cfg.PurgeBackupWait == 0:
		return defaultPurgeBackupWait
	}
	return n.cfg.PurgeBackupWait
}

// liveBackupProtections reads the backup protection records, oldest
// timestamp first, leaving out the lapsed ones, and returns the
// timestamp it read them at.
func (n *Node) liveBackupProtections(ctx context.Context) ([]backupProtection, hlc.Timestamp, error) {
	if n.db == nil {
		return nil, hlc.Timestamp{}, errors.New("the node is not serving yet")
	}
	ctx, cancel := context.WithTimeout(ctx, backupProtectionReadTimeout)
	defer cancel()
	var live []backupProtection
	var readTS hlc.Timestamp
	lo, hi := keys.BackupProtectionSpan()
	err := n.db.RunTxn(ctx, "backup-protections", func(ctx context.Context, txn *kvclient.Txn) error {
		live = live[:0]
		kvs, err := txn.Scan(ctx, lo, hi, 0)
		if err != nil {
			return err
		}
		now := n.clock.Now().WallTime
		for _, kv := range kvs {
			var p backupProtection
			if err := json.Unmarshal(kv.Value, &p); err != nil {
				// A record nobody can read protects nothing anyone can
				// name; say so rather than hold collection forever.
				log.Warnf("backup protection %s: %v; ignored", keys.Key(kv.Key), err)
				continue
			}
			if p.ExpiresNanos > now {
				live = append(live, p)
			}
		}
		readTS = txn.ReadTimestamp()
		return nil
	})
	if err != nil {
		return nil, hlc.Timestamp{}, err
	}
	sort.Slice(live, func(i, j int) bool { return live[i].TS.Less(live[j].TS) })
	return live, readTS, nil
}

// below is the highest GC threshold that leaves reads at ts possible.
func below(ts hlc.Timestamp) hlc.Timestamp {
	if ts.Logical > 0 {
		return hlc.Timestamp{WallTime: ts.WallTime, Logical: ts.Logical - 1}
	}
	return hlc.Timestamp{WallTime: ts.WallTime - 1, Logical: int32(^uint32(0) >> 1)}
}

// collectionLimit is the highest threshold a collection that read the
// records at readTS may set.
func collectionLimit(live []backupProtection, readTS hlc.Timestamp) hlc.Timestamp {
	limit := readTS
	if len(live) > 0 && below(live[0].TS).Less(limit) {
		limit = below(live[0].TS)
	}
	return limit
}

// gcLimit implements kvserver.StoreConfig.GCLimit.
func (n *Node) gcLimit(ctx context.Context) (hlc.Timestamp, bool, error) {
	live, readTS, err := n.liveBackupProtections(ctx)
	if err != nil {
		return hlc.Timestamp{}, false, err
	}
	return collectionLimit(live, readTS), true, nil
}

// backupGuard is a running backup's hold on its protection record.
type backupGuard struct {
	n    *Node
	key  keys.Key
	rec  backupProtection
	stop chan struct{}
	done chan struct{}

	mu   sync.Mutex
	lost error // set when the record could not be renewed
}

// protectBackup writes a backup's protection record, at a timestamp the
// backup will read at or above, and starts renewing it. The caller takes
// its read timestamp from the node's clock after this returns, and then
// raises the record to it (raise).
func (n *Node) protectBackup(ctx context.Context, dest string, baseTS hlc.Timestamp) (*backupGuard, error) {
	var suffix [4]byte
	_, _ = rand.Read(suffix[:])
	id := fmt.Sprintf("n%d-%s-%s", n.ident.NodeID, newOpID(), hex.EncodeToString(suffix[:]))
	g := &backupGuard{
		n:    n,
		key:  keys.BackupProtectionKey(id),
		rec:  backupProtection{ID: id, NodeID: n.ident.NodeID, Dest: dest, BaseTS: baseTS, TS: n.clock.Now()},
		stop: make(chan struct{}),
		done: make(chan struct{}),
	}
	if err := g.write(ctx, false); err != nil {
		return nil, err
	}
	// A purge that read the records before this one was written collects
	// no higher than its read timestamp, which the clock of this node may
	// trail by up to twice the maximum offset (both clocks may be off,
	// in opposite directions). Waiting that long puts the timestamp the
	// caller takes next above every such purge.
	if wait := 2 * n.clock.MaxOffset(); wait > 0 {
		select {
		case <-ctx.Done():
			g.release()
			return nil, ctx.Err()
		case <-time.After(wait):
		}
	}
	go g.renewLoop()
	return g, nil
}

// write puts the record with a fresh lease. With mustExist it refuses to
// put back a record that is gone — removed by a reader that found it
// lapsed — since the backup was unprotected in between and a purge may
// have collected through it.
func (g *backupGuard) write(ctx context.Context, mustExist bool) error {
	ctx, cancel := context.WithTimeout(ctx, backupProtectionReadTimeout)
	defer cancel()
	return g.n.db.RunTxn(ctx, "backup-protect", func(ctx context.Context, txn *kvclient.Txn) error {
		if mustExist {
			raw, err := txn.Get(ctx, g.key)
			if err != nil {
				return err
			}
			var cur backupProtection
			if raw == nil || json.Unmarshal(raw, &cur) != nil || cur.ExpiresNanos <= g.n.clock.Now().WallTime {
				return errors.New("the backup's protection lapsed before it was renewed")
			}
		}
		g.mu.Lock()
		rec := g.rec
		g.mu.Unlock()
		rec.ExpiresNanos = g.n.clock.Now().WallTime + g.n.backupProtectionTTL().Nanoseconds()
		raw, err := json.Marshal(rec)
		if err != nil {
			return err
		}
		return txn.Put(ctx, g.key, raw)
	})
}

// raise moves the record's timestamp up to ts, the backup's read
// timestamp, so that collection is held no lower than it must be.
func (g *backupGuard) raise(ctx context.Context, ts hlc.Timestamp) error {
	g.mu.Lock()
	if g.rec.TS.Less(ts) {
		g.rec.TS = ts
	}
	g.mu.Unlock()
	return g.write(ctx, true)
}

// err reports a lapse of the record while the backup ran.
func (g *backupGuard) err() error {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.lost
}

func (g *backupGuard) renewLoop() {
	defer close(g.done)
	ticker := time.NewTicker(g.n.backupProtectionTTL() / 4)
	defer ticker.Stop()
	for {
		select {
		case <-g.stop:
			return
		case <-ticker.C:
		}
		if err := g.write(context.Background(), true); err != nil {
			if strings.Contains(err.Error(), "lapsed") {
				g.mu.Lock()
				g.lost = err
				g.mu.Unlock()
				log.Warnf("backup to %s: %v", g.rec.Dest, err)
				return
			}
			// A renewal that failed for another reason is tried again
			// at the next tick, while the lease still has three left.
			log.Warnf("backup to %s: renewing its protection: %v", g.rec.Dest, err)
		}
	}
}

// release stops the renewals and removes the record. A removal that
// fails leaves a record that lapses with its lease.
func (g *backupGuard) release() {
	select {
	case <-g.stop:
		return // released already
	default:
	}
	close(g.stop)
	ctx, cancel := context.WithTimeout(context.Background(), backupProtectionReadTimeout)
	defer cancel()
	if err := g.n.db.RunTxn(ctx, "backup-unprotect", func(ctx context.Context, txn *kvclient.Txn) error {
		return txn.Delete(ctx, g.key)
	}); err != nil {
		log.Warnf("backup to %s: removing its protection record: %v (it lapses on its own)", g.rec.Dest, err)
	}
}

// purgeHold reads the backups in progress for a purge: it waits up to
// PurgeBackupWait for them to end, and returns the highest threshold
// the purge may collect at and the backups that held it below its own
// time, if any.
func (n *Node) purgeHold(ctx context.Context) (hlc.Timestamp, []backupProtection, error) {
	deadline := time.Now().Add(n.purgeBackupWait())
	for {
		live, readTS, err := n.liveBackupProtections(ctx)
		if err != nil {
			// Not knowing which backups run is no reason to cut one off.
			return hlc.Timestamp{}, nil, fmt.Errorf("purge: reading the backups in progress: %w", err)
		}
		if len(live) == 0 || !time.Now().Before(deadline) {
			return collectionLimit(live, readTS), live, nil
		}
		select {
		case <-ctx.Done():
			return hlc.Timestamp{}, nil, ctx.Err()
		case <-time.After(purgeHoldPoll):
		}
	}
}

func purgeRerunKey(start, end keys.Key) string {
	return string(start) + "\x00" + string(end)
}

// schedulePurgeRerun purges [start, end) again once every backup in held
// has ended (removed its record, or let it lapse). One rerun per span is
// pending at a time; it reports whether one is.
func (n *Node) schedulePurgeRerun(start, end keys.Key, held []backupProtection) bool {
	k := purgeRerunKey(start, end)
	token := new(int)
	n.purgeRerunMu.Lock()
	if n.purgeReruns[k] != nil {
		n.purgeRerunMu.Unlock()
		return true
	}
	if n.purgeReruns == nil {
		n.purgeReruns = map[string]*int{}
	}
	n.purgeReruns[k] = token
	n.purgeRerunMu.Unlock()
	// done withdraws this rerun's mark, and not a later rerun's: the
	// purge below may be held again and schedule the next.
	done := func() {
		n.purgeRerunMu.Lock()
		if n.purgeReruns[k] == token {
			delete(n.purgeReruns, k)
		}
		n.purgeRerunMu.Unlock()
	}
	ids := map[string]bool{}
	for _, p := range held {
		ids[p.ID] = true
	}
	start, end = start.Clone(), end.Clone()
	err := n.stopper.RunWorker(func(ctx context.Context) {
		for {
			select {
			case <-ctx.Done():
				done()
				return
			case <-time.After(purgeHoldPoll):
			}
			live, _, err := n.liveBackupProtections(ctx)
			if err != nil {
				continue
			}
			waiting := false
			for _, p := range live {
				waiting = waiting || ids[p.ID]
			}
			if !waiting {
				break
			}
		}
		done()
		log.Infof("purge of %s to %s: the backups that held it have ended; purging again", keys.Pretty(start), keys.Pretty(end))
		if _, err := n.PurgeSpan(ctx, start, end); err != nil {
			log.Warnf("purge of %s to %s, run again after the backups that held it: %v", keys.Pretty(start), keys.Pretty(end), err)
		}
	})
	if err != nil {
		done()
		return false
	}
	return true
}

// TestingAbandonBackupProtection writes a backup protection record at
// the node's now and leaves it, neither renewed nor removed: what a
// backup whose node died mid-way leaves behind. Tests only.
func (n *Node) TestingAbandonBackupProtection(ctx context.Context, dest string) (hlc.Timestamp, error) {
	g, err := n.protectBackup(ctx, dest, hlc.Timestamp{})
	if err != nil {
		return hlc.Timestamp{}, err
	}
	close(g.stop)
	<-g.done
	return g.rec.TS, nil
}
