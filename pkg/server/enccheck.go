package server

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/sthorne/datax/pkg/keys"
	"github.com/sthorne/datax/pkg/storage"
	"github.com/sthorne/datax/pkg/util/log"
)

// The switch to authenticated encryption, cross-checked (issue #248).
//
// A store records its two cutover steps in its sealed key registry
// (enc.MarkAuthenticatedWrites, MarkAuthenticated), and neither flag can
// be cleared without the store key. But the registry can be replaced
// wholesale by an older, validly sealed copy — a backup or a volume
// snapshot from before the switch — and the store then reopens
// unauthenticated: writing the old format until the node next observes
// cluster version v18, accepting the old format until the cutover
// completes again, which is the window the whole switch exists to
// close. That is the whole-file rollback class docs/encryption.md
// documents, applied to the one file whose rollback turns the feature
// off.
//
// Two records of the cutover now sit where a registry rollback cannot
// reach them, and the store is held to both:
//
//   - A marker in the state engine itself (keys.StoreEncryptionMarkerKey),
//     written after each registry transition. The registry file alone
//     rolled back — restored from a backup beside a store that was not
//     — is caught at open, before Pebble has written a file: the marker
//     says more than the registry, and the node refuses to start.
//   - A record per store in replicated KV
//     (keys.StoreEncryptionCutoverKey), written by the node once it has
//     made each step, which a single node's disk cannot roll back. A
//     whole volume rolled back — marker and all — is caught once the
//     node can read KV: the record says more than the store had at
//     open, and the node stops serving.
//
// Both compare against the state the engines had when they were opened,
// not the state now: the node re-enables authenticated writes within
// seconds of observing v18, which is what a rolled-back store would
// show by the time the record is read, and the question is what the
// store opened with. An operator who meant the rollback — a store
// restored from an old backup on purpose — starts the node once with
// --accept-encryption-rollback: the marker and the record are lowered
// to what the store has, and rise again as it re-completes the switch.

// encryptionCutover is one engine's, or the store's, position in the
// switch: Writes once new files are authenticated, Only once the old
// format is refused.
type encryptionCutover struct {
	Writes bool  `json:"writes"`
	Only   bool  `json:"only"`
	At     int64 `json:"at,omitempty"`
}

// exceeds reports whether c records a step o has not taken.
func (c encryptionCutover) exceeds(o encryptionCutover) bool {
	return (c.Writes && !o.Writes) || (c.Only && !o.Only)
}

func (c encryptionCutover) String() string {
	switch {
	case c.Only:
		return "authenticated only"
	case c.Writes:
		return "authenticated writes"
	}
	return "unauthenticated"
}

// encryptionMarker is the store-local record: the state engine's and the
// raft engine's positions.
type encryptionMarker struct {
	State encryptionCutover `json:"state"`
	Raft  encryptionCutover `json:"raft"`
}

func engineCutover(e *storage.Engine) encryptionCutover {
	if e == nil || !e.Encrypted() {
		return encryptionCutover{}
	}
	return encryptionCutover{Writes: e.AuthenticatedFiles(), Only: e.AuthenticatedOnly()}
}

// enginesCutover is both engines' positions now.
func (n *Node) enginesCutover() encryptionMarker {
	return encryptionMarker{State: engineCutover(n.engine), Raft: engineCutover(n.raftEngine)}
}

// storeCutover folds both engines into the store's position: a step is
// taken when every encrypted engine has taken it.
func (m encryptionMarker) storeCutover(raft bool) encryptionCutover {
	c := m.State
	if raft {
		c.Writes = c.Writes && m.Raft.Writes
		c.Only = c.Only && m.Raft.Only
	}
	return c
}

// encryptionCheck is the node's state for the cross-check: what the
// engines had at open, what has been recorded in KV, and whether the
// record has been read.
type encryptionCheck struct {
	mu       sync.Mutex
	atOpen   encryptionMarker
	audited  bool
	refused  bool
	recorded encryptionCutover
}

func (n *Node) readEncryptionMarker() (encryptionMarker, bool, error) {
	raw, err := n.engine.Get(keys.StoreEncryptionMarkerKey())
	if err != nil || raw == nil {
		return encryptionMarker{}, false, err
	}
	var m encryptionMarker
	if err := json.Unmarshal(raw, &m); err != nil {
		return encryptionMarker{}, false, fmt.Errorf("corrupt store encryption marker %q: %w", raw, err)
	}
	return m, true, nil
}

func (n *Node) writeEncryptionMarker(m encryptionMarker) error {
	raw, err := json.Marshal(m)
	if err != nil {
		return err
	}
	if err := n.engine.Put(keys.StoreEncryptionMarkerKey(), raw); err != nil {
		return err
	}
	if n.engine.WALDisabled() {
		return n.engine.Flush()
	}
	return nil
}

// persistEncryptionMarker raises the marker to the engines' positions
// now; it never lowers it. Called after each registry transition.
func (n *Node) persistEncryptionMarker() error {
	if n.engine == nil || !n.engine.Encrypted() {
		return nil
	}
	now := n.enginesCutover()
	m, _, err := n.readEncryptionMarker()
	if err != nil {
		return err
	}
	raised := encryptionMarker{
		State: encryptionCutover{Writes: m.State.Writes || now.State.Writes, Only: m.State.Only || now.State.Only},
		Raft:  encryptionCutover{Writes: m.Raft.Writes || now.Raft.Writes, Only: m.Raft.Only || now.Raft.Only},
	}
	if raised == m {
		return nil
	}
	return n.writeEncryptionMarker(raised)
}

// checkEncryptionMarker runs at open, before anything else touches the
// engines: it remembers what they opened with, and refuses a store whose
// marker says more than its registry does — the registry alone was
// rolled back — unless the operator accepted the rollback, in which case
// the marker is lowered to what the store has.
func (n *Node) checkEncryptionMarker() error {
	if n.engine == nil || !n.engine.Encrypted() {
		return nil
	}
	n.encCheck.atOpen = n.enginesCutover()
	m, found, err := n.readEncryptionMarker()
	if err != nil {
		return err
	}
	if !found {
		return nil
	}
	have := n.encCheck.atOpen
	var behind string
	switch {
	case m.State.exceeds(have.State):
		behind = fmt.Sprintf("the state engine's registry says %s, the store's own record says %s", have.State, m.State)
	case m.Raft.exceeds(have.Raft):
		behind = fmt.Sprintf("the raft engine's registry says %s, the store's own record says %s", have.Raft, m.Raft)
	default:
		return nil
	}
	if n.cfg.AcceptEncryptionRollback {
		log.Warnf("encryption: %s; the rollback is accepted (--accept-encryption-rollback) and the store's record lowered to match; the store re-completes the switch as the cluster version asks (issue #248)", behind)
		return n.writeEncryptionMarker(have)
	}
	return fmt.Errorf("encryption: the key registry was rolled back: %s. "+
		"An older copy of the registry opens the store unauthenticated, which is the downgrade the authenticated format exists to refuse. "+
		"Restore the registry from the same point as the store, or start once with --accept-encryption-rollback to accept it (issue #248)", behind)
}

// auditEncryptionCutover runs on the heartbeat tick once the node can
// read KV: the first time, it holds what the store opened with to the
// cluster's record of this store, and refuses to serve when the record
// says more; every time, it raises the record to what the store has now.
func (n *Node) auditEncryptionCutover(ctx context.Context) {
	if n.engine == nil || !n.engine.Encrypted() || n.db == nil {
		return
	}
	key := keys.StoreEncryptionCutoverKey(n.ident.StoreID)
	raft := n.raftEngine != nil && n.raftEngine.Encrypted()
	now := n.enginesCutover().storeCutover(raft)
	n.encCheck.mu.Lock()
	defer n.encCheck.mu.Unlock()
	if n.encCheck.refused {
		return // stopping; nothing more is recorded
	}
	if !n.encCheck.audited {
		raw, err := n.db.Get(ctx, key)
		if err != nil {
			return // retried next tick
		}
		n.encCheck.audited = true
		if raw != nil {
			var rec encryptionCutover
			if err := json.Unmarshal(raw, &rec); err != nil {
				log.Warnf("encryption: corrupt cutover record for store %s: %v", n.ident.StoreID, err)
			} else if opened := n.encCheck.atOpen.storeCutover(raft); rec.exceeds(opened) {
				if !n.cfg.AcceptEncryptionRollback {
					msg := fmt.Sprintf("the store opened %s, but the cluster's record says it had completed %s: the store was rolled back to before its switch to authenticated encryption, and an older store opens unauthenticated, which is the downgrade the authenticated format exists to refuse. Restore the store from the same point as the cluster's record, or start once with --accept-encryption-rollback to accept it (issue #248)", opened, rec)
					log.Errorf("encryption: %s; this node stops serving", msg)
					n.events.Record("security", "encryption rollback refused: %s", msg)
					n.encCheck.refused = true
					go n.Stop()
					return
				}
				log.Warnf("encryption: the store opened %s where the cluster's record says %s; the rollback is accepted (--accept-encryption-rollback) and the record lowered to match (issue #248)", opened, rec)
				rec = now
				if err := n.putEncryptionCutover(ctx, key, rec); err != nil {
					n.encCheck.audited = false
					return
				}
			}
			n.encCheck.recorded = rec
		}
	}
	if now.exceeds(n.encCheck.recorded) {
		rec := encryptionCutover{Writes: now.Writes || n.encCheck.recorded.Writes, Only: now.Only || n.encCheck.recorded.Only}
		if err := n.putEncryptionCutover(ctx, key, rec); err != nil {
			return // retried next tick
		}
		n.encCheck.recorded = rec
		log.Infof("encryption: recorded in the cluster that store %s has %s (issue #248)", n.ident.StoreID, rec)
	}
}

func (n *Node) putEncryptionCutover(ctx context.Context, key keys.Key, rec encryptionCutover) error {
	rec.At = time.Now().Unix()
	raw, err := json.Marshal(rec)
	if err != nil {
		return err
	}
	if err := n.db.Put(ctx, key, raw); err != nil {
		log.Debugf("encryption: recording the cutover of store %s: %v", n.ident.StoreID, err)
		return err
	}
	return nil
}
