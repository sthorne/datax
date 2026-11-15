package server

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/sthorne/datax/pkg/base"
	"github.com/sthorne/datax/pkg/cluster"
	"github.com/sthorne/datax/pkg/keys"
	"github.com/sthorne/datax/pkg/kvclient"
	"github.com/sthorne/datax/pkg/kvpb"
	"github.com/sthorne/datax/pkg/metrics"
	"github.com/sthorne/datax/pkg/rpc"
	"github.com/sthorne/datax/pkg/security"
	"github.com/sthorne/datax/pkg/sql/catalog"
	"github.com/sthorne/datax/pkg/util/log"
	"github.com/sthorne/datax/pkg/version"
)

// handleAdmin serves cluster admin RPCs (datax debug ...).
func (n *Node) handleAdmin(ctx context.Context, data []byte) ([]byte, error) {
	var req cluster.AdminRequest
	if err := json.Unmarshal(data, &req); err != nil {
		return nil, err
	}
	resp := n.serveAdmin(ctx, req)
	return json.Marshal(resp)
}

// adminReadOnlyOps are admin ops any authenticated principal may run.
// Everything else changes cluster state or exposes per-replica internals:
// admin role required.
var adminReadOnlyOps = map[string]bool{
	"ranges":           true,
	"nodes":            true,
	"collect-checksum": true,
	"wait-applied":     true,
	"reencrypt-status": true,
	"seal-status":      true,
	// What the cluster key is and who holds it (issue #310): no secret.
	"cluster-key-status": true,
}

// adminUnauditedOps skip the audit log: the read-only ops (collect-checksum
// fires on every consistency sweep) and node-status, which /api/range fans
// out to every replica holder under the node identity on each dashboard
// drill-down. node-status is admin-only like /api/range, whose data
// source it is; it is just not worth a record per fan-out.
var adminUnauditedOps = map[string]bool{
	"ranges":           true,
	"nodes":            true,
	"collect-checksum": true,
	"wait-applied":     true,
	"reencrypt-status": true,
	"seal-status":      true,
	// peer-unseal writes its own record, with the node asked about and
	// the reason for a refusal.
	"peer-unseal": true,
	// The cluster key's node-to-node steps write their own records, as
	// peer-unseal does (issue #310).
	"cluster-key-status":   true,
	"cluster-key-handover": true,
	"cluster-key-offer":    true,
	"cluster-key-install":  true,
	"node-status":          true,
	"node-detail":          true,
	// notify is one node handing another its committed notifications: a
	// record per batch would be a record per NOTIFY.
	"notify": true,
	// The steps of a purge, asked by the purging node of the others; the
	// purge writes one record, with the whole report (purge.go).
	"purge-range":   true,
	"purge-replica": true,
	"purge-scrub":   true,
}

// isAdminPrincipal reports whether an authenticated identity carries
// admin authority: the cluster's own certificate ("node" — internode
// forwarding and probes act with operator authority), root, or an
// admin-role member (the same rule the SQL layer applies).
func (n *Node) isAdminPrincipal(ctx context.Context, cn string) bool {
	if cn == security.NodePrincipal || cn == "root" {
		return true
	}
	if cn == "" {
		return false
	}
	ok, err := catalog.IsMember(ctx, n.db, cn, catalog.AdminRole)
	return err == nil && ok
}

// isMetricsPrincipal reports whether an identity may scrape /metrics:
// admins, and members of the built-in metrics role.
func (n *Node) isMetricsPrincipal(ctx context.Context, cn string) bool {
	if n.isAdminPrincipal(ctx, cn) {
		return true
	}
	ok, err := catalog.IsMember(ctx, n.db, cn, catalog.MetricsRole)
	return err == nil && ok
}

// adminRoleRequired reports whether an op must be run by an admin
// principal on this node. Everything outside adminReadOnlyOps changes
// cluster state or exposes per-replica internals. cancel-query is the
// one op whose answer depends on the request rather than the name: a
// plain cancel carrying a non-zero secret is authorized by that secret,
// which is a wire CancelRequest's only identity and is held by the
// client whose own statement it cancels. Terminating a session, or
// cancelling without the secret (pg_cancel_backend), is authority the
// SQL layer reserves to the admin role — so it is checked here, on the
// node that acts, and not merely on whichever node the caller reached
// first (issue #211).
func adminRoleRequired(req cluster.AdminRequest) bool {
	if req.Op == "cancel-query" {
		return req.Terminate || req.Secret == 0
	}
	return !adminReadOnlyOps[req.Op]
}

func (n *Node) serveAdmin(ctx context.Context, req cluster.AdminRequest) cluster.AdminResponse {
	// In secure mode the caller's identity is its CA-verified client
	// certificate (mutual TLS is mandatory on this port); state-changing
	// ops require the admin role and every one is audited with the
	// principal. Insecure mode has no identities to check, matching
	// pgwire's trust semantics.
	principal := "(insecure)"
	if n.tlsCfgs != nil {
		cn := rpc.PeerCN(ctx)
		principal = cn
		if adminRoleRequired(req) && !n.isAdminPrincipal(ctx, cn) {
			metrics.AdminDenied.Inc()
			log.Audit("admin-denied", "op", req.Op, "principal", cn)
			return cluster.AdminResponse{Error: fmt.Sprintf(
				"permission denied: admin operation %q requires the admin role (connected as %q)", req.Op, cn)}
		}
	}
	if adminUnauditedOps[req.Op] {
		return n.serveAdminOp(ctx, req)
	}
	resp := n.serveAdminOp(ctx, req)
	// Audited after the fact so the record carries the outcome and the
	// target: an operator tracing a rejected op, or a restore from the
	// wrong path, can tell what actually happened. Store-key material
	// (rotate-store-key) is never logged.
	kv := []any{"op", req.Op, "principal", principal, "range", int64(req.RangeID), "node", int64(req.NodeID)}
	if req.ToNode != 0 {
		kv = append(kv, "to_node", int64(req.ToNode))
	}
	if req.FromNode != 0 {
		kv = append(kv, "from_node", int64(req.FromNode))
	}
	if req.Cancel {
		kv = append(kv, "cancel", true)
	}
	if len(req.Key) != 0 {
		kv = append(kv, "key", keys.Key(req.Key).String())
	}
	if req.Path != "" {
		kv = append(kv, "path", req.Path)
	}
	if req.BasePath != "" {
		kv = append(kv, "base_path", req.BasePath)
	}
	if len(req.Paths) != 0 {
		kv = append(kv, "paths", strings.Join(req.Paths, ","))
	}
	if req.Version != 0 {
		kv = append(kv, "version", int64(req.Version))
	}
	// What a restore was asked to bring back, and to when.
	if req.AsOf != "" {
		kv = append(kv, "as_of", req.AsOf)
	}
	if len(req.Tables) != 0 {
		kv = append(kv, "tables", strings.Join(req.Tables, ","))
	}
	if len(req.Into) != 0 {
		kv = append(kv, "into", strings.Join(req.Into, ","))
	}
	if req.Database != "" {
		kv = append(kv, "database", req.Database, "into_database", req.IntoDatabase)
	}
	if req.DropForeignKeys {
		kv = append(kv, "drop_foreign_keys", true)
	}
	// That a backup key came with the request is worth a record; the key
	// is not, and no field of it is listed here.
	if requestCarriesBackupKey(req) {
		kv = append(kv, "backup_key", "given")
	}
	if resp.Error != "" {
		kv = append(kv, "outcome", "error", "error", resp.Error)
	} else {
		kv = append(kv, "outcome", "ok")
	}
	log.Audit("admin-op", kv...)
	return resp
}

func requestCarriesBackupKey(req cluster.AdminRequest) bool {
	return len(req.BackupKey) != 0 || len(req.BaseBackupKey) != 0 || len(req.BackupKeys) != 0
}

// backupKeyAccepted is the gate on a backup key arriving in a request
// (issue #291). The backup is written, and read back, by the serving
// node, so the key has to reach it; over mutual TLS it does so encrypted,
// from an authenticated admin. Without TLS it would cross the network in
// the clear, to be read by anyone on the path — after which the sealed
// backup is sealed against nobody — so it is accepted only from the
// node's own machine, the rule a root key's shares follow
// (sealAuthorized). rotate-store-key refuses an insecure cluster
// outright; this does not, because a single-machine cluster without TLS
// is a real deployment whose backups leave the machine all the same.
func (n *Node) backupKeyAccepted(ctx context.Context, req cluster.AdminRequest) error {
	return backupKeyTransportOK(requestCarriesBackupKey(req), n.tlsCfgs != nil, rpc.PeerIsLoopback(ctx))
}

func backupKeyTransportOK(carriesKey, secure, loopback bool) error {
	if !carriesKey || secure || loopback {
		return nil
	}
	return errors.New("this node runs without TLS: a backup key would cross the network in the clear, so it is accepted only from the node's own machine (run the command there, against a loopback address) or over a secure cluster's mutual TLS")
}

// serveAdminOp dispatches an admin op the caller is already authorized for.
func (n *Node) serveAdminOp(ctx context.Context, req cluster.AdminRequest) cluster.AdminResponse {
	switch req.Op {
	case "split":
		if len(req.Key) == 0 {
			return cluster.AdminResponse{Error: "split requires a key"}
		}
		// An operator's split is a manual one: sticky, so the merge
		// queue does not fold two small halves back together (issue
		// #265). `datax debug unsplit` releases it.
		sr, err := n.db.AdminSplitSticky(ctx, keys.Key(req.Key), kvpb.StickyForever)
		if err != nil {
			return cluster.AdminResponse{Error: err.Error()}
		}
		resp := cluster.AdminResponse{Right: &sr.Right, TableNames: n.tableNamesFor(ctx, sr.Left, sr.Right)}
		if sr.Left.RangeID != 0 {
			resp.Left = &sr.Left
		}
		return resp

	case "unsplit":
		if len(req.Key) == 0 {
			return cluster.AdminResponse{Error: "unsplit requires a key"}
		}
		ur, err := n.db.AdminUnsplit(ctx, keys.Key(req.Key))
		if err != nil {
			return cluster.AdminResponse{Error: err.Error()}
		}
		return cluster.AdminResponse{Ranges: []kvpb.RangeDescriptor{ur.Desc}, TableNames: n.tableNamesFor(ctx, ur.Desc)}

	case "ranges":
		descs, err := n.listRanges(ctx)
		if err != nil {
			return cluster.AdminResponse{Error: err.Error()}
		}
		return cluster.AdminResponse{Ranges: descs, TableNames: n.tableNamesFor(ctx, descs...)}

	case "nodes":
		return cluster.AdminResponse{
			Nodes:          n.registry.All(),
			ClusterVersion: int(n.readClusterVersion(ctx)),
		}

	case "upgrade-cluster":
		return n.serveUpgradeCluster(ctx, req)

	case "rebalance":
		return n.serveRebalance(ctx, req)

	case "transfer-lease":
		return n.serveTransferLease(ctx, req)

	case "decommission":
		return n.serveDecommission(ctx, req)

	case "backup", "backup-sealed":
		// "backup-sealed" is a backup with a key under an op of its own,
		// so a node from before sealed backups refuses it rather than
		// ignore the key and write plaintext (issue #313). "backup" still
		// takes a key, as a command from before the new name sends it.
		if req.Path == "" {
			return cluster.AdminResponse{Error: "backup requires a destination path"}
		}
		if req.Op == "backup-sealed" && len(req.BackupKey) == 0 {
			return cluster.AdminResponse{Error: "backup-sealed carries the backup key, and this request has none"}
		}
		if err := n.backupKeyAccepted(ctx, req); err != nil {
			return cluster.AdminResponse{Error: err.Error()}
		}
		sum, err := n.RunBackupWith(ctx, BackupOptions{
			Dest: req.Path, BasePath: req.BasePath, Key: req.BackupKey, BaseKey: req.BaseBackupKey,
			AllowPlaintext: req.AllowPlaintext, IncludeMetrics: req.IncludeMetrics,
		})
		if err != nil {
			return cluster.AdminResponse{Error: err.Error()}
		}
		return cluster.AdminResponse{Backup: sum}

	case "backup-history":
		// "backup" with revision history: an op of its own, so a node
		// from before it refuses rather than write a plain incremental.
		if req.Path == "" {
			return cluster.AdminResponse{Error: "backup requires a destination path"}
		}
		if err := n.backupKeyAccepted(ctx, req); err != nil {
			return cluster.AdminResponse{Error: err.Error()}
		}
		sum, err := n.RunBackupWith(ctx, BackupOptions{
			Dest: req.Path, BasePath: req.BasePath, Key: req.BackupKey, BaseKey: req.BaseBackupKey,
			AllowPlaintext: req.AllowPlaintext, IncludeMetrics: req.IncludeMetrics, RevisionHistory: true,
		})
		if err != nil {
			return cluster.AdminResponse{Error: err.Error()}
		}
		return cluster.AdminResponse{Backup: sum}

	case "restore-select":
		// "restore" of chosen tables, or to a chosen moment; an op of its
		// own for the same reason.
		if err := n.backupKeyAccepted(ctx, req); err != nil {
			return cluster.AdminResponse{Error: err.Error()}
		}
		opts := RestoreOptions{
			Srcs: req.Paths, Keys: req.BackupKeys, Database: req.Database, IntoDatabase: req.IntoDatabase,
			DropForeignKeys: req.DropForeignKeys,
		}
		if req.AsOf != "" {
			ts, err := ParseBackupTS(req.AsOf)
			if err != nil {
				return cluster.AdminResponse{Error: "--as-of: " + err.Error()}
			}
			opts.AsOf = ts
		}
		if len(req.Into) != 0 && len(req.Into) != len(req.Tables) {
			return cluster.AdminResponse{Error: "restore: one --into per --table, in the same order"}
		}
		for i, name := range req.Tables {
			rt := RestoreTable{Name: name}
			if len(req.Into) != 0 {
				rt.Into = req.Into[i]
			}
			opts.Tables = append(opts.Tables, rt)
		}
		sum, err := n.RunRestoreOpts(ctx, opts)
		if err != nil {
			return cluster.AdminResponse{Error: err.Error()}
		}
		return cluster.AdminResponse{Backup: sum}

	case "restore", "restore-sealed":
		// "restore-sealed": a restore given keys, under an op an older
		// node refuses rather than restore a plaintext directory in a
		// sealed backup's place without the keys (issue #313).
		if req.Op == "restore-sealed" && !requestCarriesBackupKey(req) {
			return cluster.AdminResponse{Error: "restore-sealed carries the backup keys, and this request has none"}
		}
		if err := n.backupKeyAccepted(ctx, req); err != nil {
			return cluster.AdminResponse{Error: err.Error()}
		}
		sum, err := n.RunRestoreWith(ctx, req.Paths, req.BackupKeys)
		if err != nil {
			return cluster.AdminResponse{Error: err.Error()}
		}
		return cluster.AdminResponse{Backup: sum}

	case "collect-checksum":
		if req.RangeID == 0 || req.CheckID == "" {
			return cluster.AdminResponse{Error: "collect-checksum requires a range and check ID"}
		}
		sum, stats, idx, ok := n.store.LookupChecksum(ctx, req.RangeID, req.CheckID)
		if !ok {
			return cluster.AdminResponse{Error: "checksum not available"}
		}
		return cluster.AdminResponse{Checksum: sum, ChecksumStats: stats, AppliedIndex: idx}

	case "wait-applied":
		// The merge driver's pre-proposal check (see kvserver.adminMerge):
		// block until this node's replica has applied req.Index, bounded
		// here as well as by the caller's deadline.
		if req.RangeID == 0 {
			return cluster.AdminResponse{Error: "wait-applied requires a range"}
		}
		wctx, cancel := context.WithTimeout(ctx, 15*time.Second)
		defer cancel()
		idx, err := n.store.WaitForApplied(wctx, req.RangeID, req.Index)
		if err != nil {
			return cluster.AdminResponse{Error: err.Error()}
		}
		return cluster.AdminResponse{AppliedIndex: idx}

	case "rotate-store-key":
		return n.serveRotateStoreKey(ctx, req)

	case "rewrap-store-key":
		return n.serveRewrapStoreKey(ctx, req)

	case "seal-status":
		return n.serveSealStatus()

	case "unseal":
		return n.serveStageShare(ctx, req)

	case "peer-unseal":
		return n.servePeerUnseal(ctx, req)

	case "peer-unseal-setting":
		return n.servePeerUnsealSetting(ctx, req)

	case "cluster-key-status":
		return n.clusterKeyStatusResponse(ctx)

	case "cluster-key-init":
		return n.serveClusterKeyInit(ctx)

	case "cluster-key-rotate":
		return n.serveClusterKeyRotate(ctx)

	case "cluster-key-sync":
		return n.serveClusterKeySync(ctx)

	case "cluster-key-export":
		return n.serveClusterKeyExport(ctx, req)

	case "cluster-key-import":
		return n.serveClusterKeyImport(ctx, req)

	case "cluster-key-handover":
		return n.serveClusterKeyHandover(ctx, req)

	case "cluster-key-offer":
		return n.serveClusterKeyOffer(ctx, req)

	case "cluster-key-install":
		return n.serveClusterKeyInstall(ctx, req)

	case "reencrypt":
		return n.serveReencrypt(req)

	case "reencrypt-status":
		if n.engine == nil || !n.engine.Encrypted() {
			return cluster.AdminResponse{Error: "store is not encrypted"}
		}
		return cluster.AdminResponse{Reencryption: n.reencryptionStatus()}

	case "cancel-query":
		// A backend cancel or terminate, for a connection that lives on
		// this node: forwarded from the node a CancelRequest landed on,
		// forwarded from wherever pg_cancel_backend ran, or sent by an
		// operator directly. adminRoleRequired has already decided which
		// authority the caller needed and serveAdmin has checked it, so
		// what is left here is only to spend the one the caller brought
		// — the secret cancels that single connection and nothing else,
		// the admin role does the rest.
		found := false
		if srv := n.sqlServer(); srv != nil {
			if req.Secret != 0 && !req.Terminate {
				found = srv.CancelBySecret(req.PID, req.Secret)
			} else {
				found = srv.ControlBackend(req.PID, req.Terminate)
			}
		}
		raw, _ := json.Marshal(found)
		return cluster.AdminResponse{Status: raw}

	case "notify":
		// Another node's committed notifications, for the sessions
		// listening here (issue #297). Only a node sends these: the call
		// is not audited (a record per batch would be a record per
		// NOTIFY), so it is not left open to every admin certificate —
		// an operator who wants to notify has NOTIFY, under their own
		// name.
		if n.tlsCfgs != nil && rpc.PeerCN(ctx) != security.NodePrincipal {
			return cluster.AdminResponse{Error: "permission denied: notify is sent by nodes only"}
		}
		n.notifyHub().Deliver(req.Notify)
		return cluster.AdminResponse{}

	case "node-detail":
		// This node's /api/node document, for another node's dashboard
		// (/api/node?id=N fans out under the node identity; admin-only
		// like node-status, so the admin-only material is included).
		raw, err := json.Marshal(n.localNodeDetail(ctx, allKeysViewer()))
		if err != nil {
			return cluster.AdminResponse{Error: err.Error()}
		}
		return cluster.AdminResponse{Status: raw}

	case "node-status":
		// This node's /status document, optionally filtered to one range —
		// the cross-node drill-down's data source (/api/range fans this out
		// to every replica holder). During a mixed-version roll an old
		// receiver answers "unknown admin op", which the caller surfaces as
		// that replica's error rather than failing the whole document.
		st := n.statusSummary()
		if req.RangeID != 0 {
			var only []RangeStatus
			for _, rs := range st.Ranges {
				if rs.RangeID == int64(req.RangeID) {
					only = append(only, rs)
				}
			}
			st.Ranges = only
		}
		raw, err := json.Marshal(st)
		if err != nil {
			return cluster.AdminResponse{Error: err.Error()}
		}
		return cluster.AdminResponse{Status: raw}

	case "purge":
		start, end := keys.Key(req.Key), keys.Key(req.EndKey)
		if req.Table != "" {
			id, err := n.tableIDByName(ctx, req.Table)
			if err != nil {
				return cluster.AdminResponse{Error: err.Error()}
			}
			start, end = keys.TableDataSpan(id)
		}
		rep, err := n.PurgeSpan(ctx, start, end)
		if err != nil {
			return cluster.AdminResponse{Error: err.Error()}
		}
		return cluster.AdminResponse{Purge: rep, TableNames: n.tableNamesFor(ctx, kvpb.RangeDescriptor{StartKey: start, EndKey: end})}

	case "purge-range", "purge-replica", "purge-scrub":
		return n.servePurgeStep(ctx, req)

	case "merge":
		if req.RangeID == 0 {
			return cluster.AdminResponse{Error: "merge requires --range"}
		}
		desc, err := n.findRange(ctx, req.RangeID)
		if err != nil {
			return cluster.AdminResponse{Error: err.Error()}
		}
		mr, err := n.db.AdminMerge(ctx, desc.StartKey)
		if err != nil {
			return cluster.AdminResponse{Error: err.Error()}
		}
		return cluster.AdminResponse{Ranges: []kvpb.RangeDescriptor{mr.Desc}}

	default:
		return cluster.AdminResponse{Error: "unknown admin op " + req.Op}
	}
}

// serveUpgradeCluster finalizes a cluster upgrade: once every live,
// non-draining node runs a binary at or above the target version, the
// replicated cluster version advances to it. The version only ever moves
// forward; repeating the op is idempotent.
func (n *Node) serveUpgradeCluster(ctx context.Context, req cluster.AdminRequest) cluster.AdminResponse {
	target := version.Version(req.Version)
	if target == 0 {
		target = n.binaryVersion()
	}
	if target > n.binaryVersion() {
		return cluster.AdminResponse{Error: fmt.Sprintf(
			"cannot upgrade to %s: this node's binary supports at most %s", target, n.binaryVersion())}
	}
	var stragglers []string
	for _, nd := range n.liveNodes() {
		if nd.Draining {
			continue
		}
		bv := version.Version(nd.BinaryVersion)
		if nd.NodeID == n.ident.NodeID {
			// Our own registry row may lag a heartbeat; we know our binary.
			bv = n.binaryVersion()
		} else if bv == 0 {
			bv = version.V1
		}
		if bv < target {
			stragglers = append(stragglers, fmt.Sprintf("%s (%s)", nd.NodeID, bv))
		}
	}
	if len(stragglers) > 0 {
		sort.Strings(stragglers)
		return cluster.AdminResponse{Error: fmt.Sprintf(
			"cannot finalize %s: live nodes still run older binaries: %s",
			target, strings.Join(stragglers, ", "))}
	}
	cur := n.readClusterVersion(ctx)
	if cur >= target {
		return cluster.AdminResponse{ClusterVersion: int(cur)}
	}
	// Paired, short though it is: this is the one irreversible operation
	// in the product, so "did it finish, and did the migrations under it
	// finish" being unanswerable is worse here than anywhere else
	// (issue #192).
	opID := newOpID()
	n.events.RecordStart("upgrade", opID, "finalizing the cluster version at %s", target)
	failed := func(format string, args ...any) cluster.AdminResponse {
		msg := fmt.Sprintf(format, args...)
		n.events.RecordEnd("upgrade", opID, "failed", "finalizing at %s failed: %s", target, msg)
		return cluster.AdminResponse{Error: msg}
	}
	truncates := 0
	if err := n.db.RunTxn(ctx, "finalize-cluster-version", func(ctx context.Context, txn *kvclient.Txn) error {
		truncates = 0
		if cur < version.V11 && target >= version.V11 {
			// Before v11 a DELETE grant was what TRUNCATE took; from v11
			// TRUNCATE is its own privilege and DELETE no longer covers
			// it (issue #312). The grants made before keep their meaning:
			// every holder of DELETE also gets TRUNCATE, in the
			// transaction that moves the version, so no grant made in
			// between is missed.
			var err error
			if truncates, err = catalog.GrantTruncateWithDelete(ctx, txn); err != nil {
				return err
			}
		}
		return txn.Put(ctx, keys.ClusterVersionKey(), []byte(strconv.Itoa(int(target))))
	}); err != nil {
		return failed("%v", err)
	}
	if truncates > 0 {
		log.Infof("privilege migration: TRUNCATE granted alongside DELETE on %d table(s)", truncates)
		n.events.Record("upgrade", "privilege migration: TRUNCATE granted alongside DELETE on %d table(s)", truncates)
	}
	if target >= version.V6 {
		// The v6 catalog migration: every pre-existing table moves under the
		// default database. Idempotent; a node that starts later at v6 runs
		// it again as a no-op (ensureDatabaseCatalog).
		if moved, err := catalog.MigrateNamespace(ctx, n.db); err != nil {
			return failed("cluster version is %s but the database catalog migration failed: %v (rerun the upgrade)", target, err)
		} else if moved > 0 {
			log.Infof("catalog migration: %d table(s) moved under database %q", moved, catalog.DefaultDatabase)
			n.events.Record("upgrade", "catalog migration: %d table(s) moved under database %q", moved, catalog.DefaultDatabase)
		}
	}
	if target >= version.V11 {
		// The v11 role migration: credential records and admin markers
		// become role descriptors. Idempotent (ensureRoleCatalog).
		if moved, err := catalog.MigrateRoles(ctx, n.db); err != nil {
			return failed("cluster version is %s but the role migration failed: %v (rerun the upgrade)", target, err)
		} else if moved > 0 {
			log.Infof("role migration: %d record(s) rewritten as role descriptors", moved)
			n.events.Record("upgrade", "role migration: %d record(s) rewritten as role descriptors", moved)
		}
	}
	n.mirrorClusterVersion(ctx)
	log.Infof("cluster version finalized at %s", target)
	n.events.RecordEnd("upgrade", opID, "ok", "cluster version finalized at %s", target)
	return cluster.AdminResponse{ClusterVersion: int(target)}
}

// listRanges reads all range descriptors from the /meta records. The
// inconsistent scan can briefly surface two records for one range (a split
// or merge whose meta repair has not landed), so duplicates are collapsed
// to the highest generation.
func (n *Node) listRanges(ctx context.Context) ([]kvpb.RangeDescriptor, error) {
	start, end := keys.MetaSpan()
	ba := &kvpb.BatchRequest{Header: kvpb.BatchHeader{Timestamp: n.clock.Now(), ReadInconsistent: true}}
	ba.Add(&kvpb.ScanRequest{RequestHeader: kvpb.RequestHeader{Key: start, EndKey: end}})
	br, kerr := n.db.Send(ctx, ba)
	if kerr != nil {
		return nil, kerr
	}
	byID := map[base.RangeID]kvpb.RangeDescriptor{}
	var order []base.RangeID
	for _, kv := range br.Responses[0].Scan.Rows {
		var d kvpb.RangeDescriptor
		if err := json.Unmarshal(kv.Value, &d); err != nil {
			continue
		}
		if cur, ok := byID[d.RangeID]; !ok {
			byID[d.RangeID] = d
			order = append(order, d.RangeID)
		} else if d.Generation > cur.Generation {
			byID[d.RangeID] = d
		}
	}
	out := make([]kvpb.RangeDescriptor, 0, len(order))
	for _, id := range order {
		out = append(out, byID[id])
	}
	return out, nil
}
