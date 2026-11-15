package server

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/cockroachdb/pebble/v2/vfs"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/sthorne/datax/pkg/storage/enc"
	"github.com/sthorne/datax/pkg/util/log"
)

// What a node does about a root that is a service (issue #308): it binds
// its wrapped store key to itself, and it keeps asking whether the root
// would still work — because a running node does not need its root, and
// one that has become unreachable is otherwise discovered at the next
// restart.

// storeKeyBinding is what this node's wrapped store key is bound to.
func (n *Node) storeKeyBinding() enc.Binding {
	return enc.Binding{ClusterID: n.ident.ClusterID.String(), NodeID: uint64(n.ident.NodeID)}
}

// noteWrapped records what the wrapped-key file now holds, after a wrap
// under root bound to b.
func (n *Node) noteWrapped(root enc.Root, b enc.Binding) {
	n.encKeyMu.Lock()
	n.encBinding, n.encEntryKeyID = b, root.KeyID()
	n.encKeyMu.Unlock()
}

// bindStoreKey runs once the node knows its identity, which is after its
// store has opened and so after its key was unwrapped — the identity is
// in the store. Two things follow from that order.
//
// A key bound to another node is refused here rather than at the unwrap.
// The store it opened is then that node's too (a key opens one store),
// so what this catches is a wrapped-key file edited to claim an identity
// its store does not have; the refusal a service makes, of a key
// presented under another node's context, happened at the unwrap.
//
// And a key that is bound to nothing — a new node's, wrapped before it
// had an id, or a store from before bindings — is bound now, for a root
// that takes the binding to its service. For the other roots the entry
// is left as it is until the operator rewraps: binding it would write an
// entry that a release from before bindings refuses to read, and that
// is not a thing to do to a store unasked.
func (n *Node) bindStoreKey(ctx context.Context) error {
	n.encKeyMu.Lock()
	root, have := n.encRoot, n.encBinding
	key := append([]byte(nil), n.encKey...)
	n.encKeyMu.Unlock()
	defer enc.Zero(key)
	if root == nil {
		return nil
	}
	want := n.storeKeyBinding()
	if !have.IsZero() {
		if have != want {
			return fmt.Errorf("the wrapped store key in %s is bound to %s, but the store it opens is %s: the wrapped-key file is not this store's own", n.cfg.Dir, have, want)
		}
		return nil
	}
	if _, takes := root.(enc.ContextRoot); !takes {
		return nil
	}
	if err := enc.RewrapStoreKey(enc.WithBinding(ctx, want), vfs.Default, n.cfg.Dir, root, key); err != nil {
		// The node has its key and can run. What it lacks is the
		// binding, and /api/security says so until a rewrap succeeds.
		log.Warnf("the store key could not be bound to %s; it stays wrapped for no node in particular, and `datax debug enc-root rewrap` binds it: %v", want, err)
		return nil
	}
	n.noteWrapped(root, want)
	log.Infof("store key bound to %s under the %s root (%s)", want, root.Kind(), rootKeyID(root))
	log.Audit("store-key-wrap", "root", root.Kind(), "key_id", root.KeyID(), "reason", "bound to the node's identity")
	return nil
}

// defaultRootHealthInterval is how often the root is checked. A check of
// a KMS root is two calls to the service, which the service bills and
// logs; a minute finds an outage long before a restart would, without
// being a load.
const defaultRootHealthInterval = time.Minute

// rootHealthState is the outcome of the latest health check of the root.
type rootHealthState struct {
	mu sync.Mutex
	// checked is false until a check has run.
	checked     bool
	healthy     bool
	at          time.Time
	lastHealthy time.Time
	err         string
	latestKeyID string
}

func (h *rootHealthState) reset() {
	h.mu.Lock()
	h.checked, h.healthy, h.err, h.latestKeyID = false, false, "", ""
	h.mu.Unlock()
}

// usedJustNow records that the root has this moment wrapped and
// unwrapped the store key, which is everything a check would learn.
func (h *rootHealthState) usedJustNow(keyID string) {
	now := time.Now()
	h.mu.Lock()
	defer h.mu.Unlock()
	if !h.checked {
		return
	}
	h.healthy, h.err, h.at, h.lastHealthy, h.latestKeyID = true, "", now, now, keyID
}

// startRootHealth begins checking the root, when it is one that can be
// checked. The store key was unwrapped moments ago, so the root starts
// out healthy.
func (n *Node) startRootHealth() error {
	n.encKeyMu.Lock()
	root := n.encRoot
	n.encKeyMu.Unlock()
	if _, ok := root.(enc.HealthChecker); !ok || n.cfg.EncRootHealthInterval < 0 {
		return nil
	}
	interval := n.cfg.EncRootHealthInterval
	if interval == 0 {
		interval = defaultRootHealthInterval
	}
	now := time.Now()
	n.rootHealth.mu.Lock()
	n.rootHealth.checked, n.rootHealth.healthy, n.rootHealth.at, n.rootHealth.lastHealthy = true, true, now, now
	n.rootHealth.mu.Unlock()
	return n.stopper.RunWorker(func(ctx context.Context) {
		t := time.NewTicker(interval)
		defer t.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-t.C:
				n.checkRootHealth(ctx)
			}
		}
	})
}

// checkRootHealth asks the root in use whether it works and records the
// answer. A change either way is logged and put on the event timeline;
// a root that stays down is not logged again every interval.
func (n *Node) checkRootHealth(ctx context.Context) {
	n.encKeyMu.Lock()
	root, binding := n.encRoot, n.encBinding
	n.encKeyMu.Unlock()
	hc, ok := root.(enc.HealthChecker)
	if !ok {
		// The store was moved to a root that cannot be asked.
		n.rootHealth.reset()
		return
	}
	// The probe carries the node's binding, as its key does, so that a
	// policy written to require the binding lets the probe through.
	got, err := hc.CheckHealth(enc.WithBinding(ctx, binding))
	if ctx.Err() != nil {
		return // the node is stopping; that is not the root's failure
	}
	now := time.Now()
	h := &n.rootHealth
	h.mu.Lock()
	was := h.healthy || !h.checked
	h.checked, h.healthy, h.at = true, err == nil, now
	if err == nil {
		h.err, h.lastHealthy, h.latestKeyID = "", now, got.LatestKeyID
	} else {
		h.err = err.Error()
	}
	h.mu.Unlock()
	switch {
	case err != nil && was:
		log.Warnf("the %s root this node's store key is under is not usable: %v. The node keeps running — it holds its key — but it could not restart until the root answers", root.Kind(), err)
		n.events.Record("enc-root", "the %s root is not usable; this node could not restart now: %v", root.Kind(), err)
	case err == nil && !was:
		log.Infof("the %s root this node's store key is under is usable again", root.Kind())
		n.events.Record("enc-root", "the %s root is usable again", root.Kind())
	}
}

// rootHealthCollector exports the root's health. It emits nothing for a
// root that is not checked (file, exec, shares): a constant 1 for a root
// nobody asks would be a claim with nothing behind it.
type rootHealthCollector struct{ node *Node }

var (
	rootHealthyDesc = prometheus.NewDesc("datax_enc_root_healthy",
		"1 while the root this node's store key is kept under (a KMS, issue #308) is reachable and can use its key, 0 while it is not. A node at 0 keeps running but could not restart. Absent for a root that is not checked.", nil, nil)
	rootLastHealthyDesc = prometheus.NewDesc("datax_enc_root_last_healthy_timestamp_seconds",
		"Unix time of the last check that found the root this node's store key is kept under usable.", nil, nil)
)

func (c *rootHealthCollector) Describe(ch chan<- *prometheus.Desc) {
	ch <- rootHealthyDesc
	ch <- rootLastHealthyDesc
}

func (c *rootHealthCollector) Collect(ch chan<- prometheus.Metric) {
	h := &c.node.rootHealth
	h.mu.Lock()
	checked, healthy, last := h.checked, h.healthy, h.lastHealthy
	h.mu.Unlock()
	if !checked {
		return
	}
	v := 0.0
	if healthy {
		v = 1
	}
	ch <- prometheus.MustNewConstMetric(rootHealthyDesc, prometheus.GaugeValue, v)
	if !last.IsZero() {
		ch <- prometheus.MustNewConstMetric(rootLastHealthyDesc, prometheus.GaugeValue, float64(last.Unix()))
	}
}

// securityRoot is what /api/security says of the root a store's key is
// under.
func (n *Node) securityRoot() *SecurityRoot {
	n.encKeyMu.Lock()
	r, binding, entryID := n.encRoot, n.encBinding, n.encEntryKeyID
	n.encKeyMu.Unlock()
	if r == nil {
		return nil
	}
	out := &SecurityRoot{Kind: r.Kind(), KeyID: r.KeyID(), Bound: !binding.IsZero()}
	// A root whose key id carries the service's key version is reported
	// by what the wrapped-key file records: the version the store key is
	// under, whatever the root has seen since.
	if entryID != "" {
		out.KeyID = entryID
	}
	h := &n.rootHealth
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.checked {
		healthy := h.healthy
		out.Healthy, out.HealthError, out.HealthCheckedAt = &healthy, h.err, h.at.UTC().Format(time.RFC3339)
		if !h.lastHealthy.IsZero() {
			out.LastHealthyAt = h.lastHealthy.UTC().Format(time.RFC3339)
		}
		if h.latestKeyID != "" && h.latestKeyID != out.KeyID {
			out.LatestKeyID = h.latestKeyID
		}
	}
	return out
}
