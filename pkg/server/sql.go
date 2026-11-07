package server

import (
	"context"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/binary"
	"encoding/json"
	"errors"
	"github.com/sthorne/datax/pkg/cluster"
	"github.com/sthorne/datax/pkg/kvclient"
	"github.com/sthorne/datax/pkg/version"
	"net"
	"sort"
	"sync"
	"time"

	"github.com/sthorne/datax/pkg/base"
	"github.com/sthorne/datax/pkg/keys"
	"github.com/sthorne/datax/pkg/pgwire"
	"github.com/sthorne/datax/pkg/security"
	"github.com/sthorne/datax/pkg/sql/catalog"
	"github.com/sthorne/datax/pkg/util/log"
)

func kvNodeID(v int64) base.NodeID { return base.NodeID(v) }

// startSQL brings up the SQL (PostgreSQL wire protocol) listener.
func (n *Node) startSQL() error {
	lis := n.cfg.PGListener
	if lis == nil {
		if n.cfg.PGListen == "" {
			return nil
		}
		var err error
		lis, err = net.Listen("tcp", n.cfg.PGListen)
		if err != nil {
			return err
		}
	}
	cat, err := n.catalogAccessor()
	if err != nil {
		return err
	}
	opts := pgwire.ServerOptions{
		SlowStatementThreshold: n.cfg.SlowStatementThreshold, NodeID: int32(n.ident.NodeID),
		AuthTimeout: n.cfg.AuthTimeout, MaxPendingAuth: n.cfg.SQLMaxPendingAuth,
	}
	opts.Forward = func(ctx context.Context, node, pid int32, secret uint32, terminate bool) (bool, error) {
		addr, err := n.registry.Resolve(base.NodeID(node))
		if err != nil {
			return false, nil // no such node: no such session
		}
		var resp cluster.AdminResponse
		if err := n.trans.Call(ctx, addr, "admin", cluster.AdminRequest{Op: "cancel-query", PID: pid, Secret: secret, Terminate: terminate}, &resp); err != nil {
			return false, err
		}
		if resp.Error != "" {
			return false, errors.New(resp.Error)
		}
		var found bool
		_ = json.Unmarshal(resp.Status, &found)
		return found, nil
	}
	if n.tlsCfgs != nil {
		opts.TLS = n.tlsCfgs.PGServer
		opts.Auth = n.lookupVerifier
		opts.CanLogin = n.canLogin
		opts.MockSecret = n.mockSecret
		opts.MockIterations = n.standInIterations
		opts.AuthLimiter = sqlAuthLimiter{n.authLimit}
		if n.cfg.RootPassword != "" {
			if err := n.stopper.RunWorker(n.seedRootUser); err != nil {
				return err
			}
		}
	}
	srv := pgwire.Serve(lis, n.db, cat, n.stopper, opts)
	n.pgServer.Store(srv)
	log.Infof("node %s serving SQL at %s", n.ident.NodeID, srv.Addr())
	return nil
}

// catalogAccessor returns the node's catalog accessor, built on first use
// with statistics and descriptor leasing enabled (the SQL server and the
// metrics recorder share it, so one lease serves both).
func (n *Node) catalogAccessor() (*catalog.Accessor, error) {
	n.catOnce.Do(func() {
		cat := catalog.NewAccessor()
		cat.EnableStats(n.db)
		if n.cfg.DescLeaseTTL >= 0 {
			if err := cat.StartLeasing(n.db, n.clock, n.stopper, n.cfg.DescLeaseTTL); err != nil {
				n.catErr = err
				return
			}
		}
		n.cat = cat
	})
	return n.cat, n.catErr
}

// lookupVerifier reads a role's stored SCRAM verifier (nil, nil = no such
// role, a role that cannot log in, or one without a password; the wire
// layer reports every failure uniformly).
func (n *Node) lookupVerifier(ctx context.Context, user string) (*security.ScramVerifier, error) {
	r, err := catalog.LookupRole(ctx, n.db, user)
	if err != nil || r == nil || !r.Login || len(r.Verifier) == 0 {
		return nil, err
	}
	return security.UnmarshalVerifier(r.Verifier)
}

// mockSecret returns the cluster's authentication secret, the key under
// which the wire layer derives the stand-in SCRAM salt for a user that
// does not exist (issue #137): read from keys.AuthSecretKey on first
// use, created there — in a transaction, so concurrent first users on
// different nodes settle on one value — when the cluster has none yet
// (a cluster bootstrapped before the key existed). nil when the read
// fails; the wire layer then uses a per-process secret until the next
// attempt succeeds.
// authSecretReadable reports whether this node has the cluster's
// authentication secret. It does not go to KV when the answer is already
// cached, so the health check costs nothing in the normal case.
func (n *Node) authSecretReadable(ctx context.Context) bool {
	n.authSecretMu.Lock()
	cached := n.authSecret != nil
	n.authSecretMu.Unlock()
	if cached {
		return true
	}
	return len(n.mockSecret(ctx)) > 0
}

func (n *Node) mockSecret(ctx context.Context) []byte {
	n.authSecretMu.Lock()
	defer n.authSecretMu.Unlock()
	if n.authSecret != nil {
		return n.authSecret
	}
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	var secret []byte
	err := n.db.RunTxn(ctx, "auth-secret", func(ctx context.Context, txn *kvclient.Txn) error {
		raw, err := txn.Get(ctx, keys.AuthSecretKey())
		if err != nil {
			return err
		}
		if len(raw) > 0 {
			secret = raw
			return nil
		}
		secret = make([]byte, 32)
		if _, err := rand.Read(secret); err != nil {
			return err
		}
		return txn.Put(ctx, keys.AuthSecretKey(), secret)
	})
	if err != nil {
		log.Warnf("reading the cluster's authentication secret: %v (stand-in SCRAM salts are per node until it can be read)", err)
		return nil
	}
	n.authSecret = secret
	return secret
}

// canLogin reports whether a role exists and may open a session (the
// certificate path, which needs no password).
func (n *Node) canLogin(ctx context.Context, user string) (bool, error) {
	r, err := catalog.LookupRole(ctx, n.db, user)
	if err != nil || r == nil {
		return false, err
	}
	return r.Login, nil
}

// seedRootUser writes root's verifier at startup if none exists yet (the
// bootstrap path for a secure cluster's first credential). Retries until
// the cluster can serve writes.
func (n *Node) seedRootUser(ctx context.Context) {
	for {
		wctx, cancel := context.WithTimeout(ctx, 5*time.Second)
		existing, err := catalog.LookupRole(wctx, n.db, catalog.RootRole)
		if err == nil && existing != nil && len(existing.Verifier) > 0 {
			cancel()
			return // already seeded (either layout)
		}
		if err == nil {
			v, verr := security.MakeScramVerifier(n.cfg.RootPassword)
			if verr == nil {
				enc, merr := security.MarshalVerifier(v)
				if merr == nil {
					// A cluster at v11 gets a role descriptor; an older one
					// the credential record a pre-role node can read.
					var perr error
					if n.readClusterVersion(wctx) >= version.V11 {
						perr = n.db.RunTxn(wctx, "seed-root", func(ctx context.Context, txn *kvclient.Txn) error {
							d := &catalog.RoleDescriptor{Name: catalog.RootRole, Login: true}
							if cur, err := catalog.LookupRole(ctx, txn, catalog.RootRole); err != nil {
								return err
							} else if cur != nil && !cur.Legacy {
								d = cur.Clone() // keep memberships written before the seed
							}
							d.Login, d.Verifier = true, enc
							return catalog.PutRole(ctx, txn, d)
						})
					} else {
						perr = n.db.Put(wctx, keys.UserKey(catalog.RootRole), enc)
					}
					if perr == nil {
						cancel()
						log.Infof("seeded root user credential")
						return
					}
				}
			}
		}
		cancel()
		select {
		case <-ctx.Done():
			return
		case <-time.After(time.Second):
		}
	}
}

// sqlServer is the SQL server once startSQL has started it (nil before,
// and on a node without a SQL listener).
func (n *Node) sqlServer() *pgwire.Server { return n.pgServer.Load() }

// The stand-in verifier's iteration count (issue #225). A real user's
// verifier carries the count it was made with, and ScramIterations
// governs only new ones, so after a raise the cluster holds verifiers at
// both counts until every password has been rotated — which, on a
// cluster with no rotation policy, is never. Before the raise there was
// no count oracle at all: one constant, every verifier matching it. A
// stand-in at the constant would open one — a name whose server-first
// says the old count is a real user whose password predates the raise —
// and a stand-in at the highest stored count only points it the other
// way (InfoSec on the PR). So the stand-in draws its count per name
// from the counts the real verifiers carry, in their proportions: a PRF
// of the name under the cluster's authentication secret — the same
// secret, and the same shape, as the per-name salt (issue #137) —
// picks one of the stored counts, so a name that does not exist shows
// whatever a real name could show, stably across handshakes and nodes
// (the same replicated catalog, the same secret), and a count says
// nothing about whether the name exists.
//
// What remains: while the catalog cannot be read, the stand-in falls
// back to ScramIterations, which no real user may carry; that is the one
// window the oracle is open, for the duration of the outage, and the
// failure is logged. The scan is not on the handshake's path: the counts
// are cached and refreshed in the background once standInRefreshAfter
// old, so an unknown user's handshake costs what a known user's does —
// one role lookup — except for the very first on a node, which fills the
// cache.
const standInRefreshAfter = 5 * time.Second

type standInState struct {
	mu         sync.Mutex
	counts     []int // the stored verifiers' counts, sorted; nil until read
	at         time.Time
	refreshing bool
}

func (n *Node) standInIterations(ctx context.Context, user string) int {
	n.standIn.mu.Lock()
	counts, at, refreshing := n.standIn.counts, n.standIn.at, n.standIn.refreshing
	if counts != nil && time.Since(at) >= standInRefreshAfter && !refreshing {
		n.standIn.refreshing = true
		go n.refreshStandInCounts(n.stopper.Ctx())
	}
	n.standIn.mu.Unlock()
	if counts == nil {
		n.refreshStandInCounts(ctx)
		n.standIn.mu.Lock()
		counts = n.standIn.counts
		n.standIn.mu.Unlock()
	}
	return standInCount(counts, n.mockSecret(ctx), user)
}

func (n *Node) refreshStandInCounts(ctx context.Context) {
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	roles, err := catalog.ListRoles(ctx, n.db)
	n.standIn.mu.Lock()
	defer n.standIn.mu.Unlock()
	n.standIn.refreshing = false
	if err != nil {
		log.Warnf("reading the stored verifiers for the stand-in SCRAM count: %v (the stand-in advertises %d until they can be read)", err, security.ScramIterations)
		if n.standIn.counts == nil {
			n.standIn.counts = []int{}
		}
		return
	}
	n.standIn.counts, n.standIn.at = verifierCounts(roles), time.Now()
}

// verifierCounts is every stored verifier's iteration count, one entry
// per verifier, sorted: the distribution the stand-in draws from.
func verifierCounts(roles []*catalog.RoleDescriptor) []int {
	counts := []int{}
	for _, r := range roles {
		if len(r.Verifier) == 0 {
			continue
		}
		if v, err := security.UnmarshalVerifier(r.Verifier); err == nil && v.Iterations > 0 {
			counts = append(counts, v.Iterations)
		}
	}
	sort.Ints(counts)
	return counts
}

// standInCount picks user's count from counts under secret: the same
// entry for the same name on every node that holds the same catalog and
// secret, distributed as the real verifiers are. With no counts to draw
// from (no verifier stored, or the catalog unreadable) or no secret, the
// constant.
func standInCount(counts []int, secret []byte, user string) int {
	if len(counts) == 0 || len(secret) == 0 {
		return security.ScramIterations
	}
	mac := hmac.New(sha256.New, secret)
	mac.Write([]byte("scram-mock-iterations\x00" + user))
	idx := binary.BigEndian.Uint64(mac.Sum(nil)[:8]) % uint64(len(counts))
	return counts[idx]
}
