package server

import (
	"context"
	"time"

	"github.com/sthorne/datax/pkg/keys"
	"github.com/sthorne/datax/pkg/kvclient"
	"github.com/sthorne/datax/pkg/sql"
	"github.com/sthorne/datax/pkg/sql/catalog"
	"github.com/sthorne/datax/pkg/util/log"
	"github.com/sthorne/datax/pkg/version"
)

// Seal jobs (issue #310, step 5; pkg/sql/sealjobs.go): the background
// rewrite of a sealed table's rows after ALTER TABLE ... SET
// (encryption_key | encryption_key_by) sealed a table that held rows, or
// ALTER TABLE ... ROTATE ENCRYPTION KEY or datax_rotate_tenant_key gave a
// key a new version. Each table has at most one, as a record under
// keys.SealJobKey that the SQL layer writes with the change.
//
// Every node runs the loop and the node leading range 1 acts, like the
// row-TTL job. Each tick it reads the records — a read of the jobs in
// progress, not of the catalog — and works on each in turn, one batch
// after another (sql.Session.SealJobStep: up to SealJobBatchSize rows in
// one ordinary transaction, which also records how far the job got),
// for at most one interval per job, so one large table does not starve
// the others. A restart, or a move of range 1's leadership, loses at
// most the batch in flight: the next leader goes on from the record.
// For a moment two nodes may both act; their batches are serializable
// transactions over the same record and rows, so one of them retries.
//
// A run of batches on one job is one operation of the node's events
// (kind seal-job), and so is the job's end.

const (
	defaultSealJobInterval   = 10 * time.Second
	defaultSealJobBatchSize  = 100
	defaultSealJobBatchPause = 20 * time.Millisecond
)

func (n *Node) sealJobLoop(ctx context.Context) {
	interval := n.cfg.SealJobInterval
	if interval < 0 {
		return
	}
	if interval == 0 {
		interval = defaultSealJobInterval
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
		r1, ok := n.store.GetReplica(1)
		if !ok || !r1.IsLeader() || n.ClusterVersion() < version.V22 {
			continue
		}
		n.RunSealJobsOnce(ctx)
	}
}

// SealJobRun is what one run of batches on one job did.
type SealJobRun struct {
	TableID   uint64
	Table     string
	Kind      string
	Rewritten int
	// Done: the job finished (or its table is gone) in this run.
	Done bool
	// Destroyed counts the key versions a finished rotation destroyed.
	Destroyed int
	Err       error
}

// RunSealJobsOnce works on every seal job, each for at most one
// interval, and returns what it did to each job it touched. Exported for
// tests, which drive runs instead of waiting on the loop.
func (n *Node) RunSealJobsOnce(ctx context.Context) []SealJobRun {
	lo, hi := keys.SealJobSpan()
	kvs, err := n.db.Scan(ctx, lo, hi, 0)
	if err != nil {
		log.Debugf("seal jobs: %v", err)
		return nil
	}
	budget := n.cfg.SealJobInterval
	if budget <= 0 {
		budget = defaultSealJobInterval
	}
	var out []SealJobRun
	for _, kv := range kvs {
		if ctx.Err() != nil {
			break
		}
		j, err := catalog.DecodeSealJob(kv.Value)
		if err != nil {
			log.Warnf("seal jobs: %s: %v", keys.Key(kv.Key), err)
			continue
		}
		run := n.runSealJob(ctx, j, time.Now().Add(budget))
		if run.Rewritten > 0 || run.Done || run.Err != nil {
			out = append(out, run)
		}
	}
	return out
}

// runSealJob runs batches of one job until it is done, waiting, past
// deadline, or failing. The deadline is checked between batches: a batch
// is never cut short, which would leave its transaction's locks on the
// job's record and rows for others to wait out.
func (n *Node) runSealJob(ctx context.Context, j *catalog.SealJob, deadline time.Time) SealJobRun {
	run := SealJobRun{TableID: j.TableID, Table: j.Table, Kind: j.Kind}
	batch := n.cfg.SealJobBatchSize
	if batch <= 0 {
		batch = defaultSealJobBatchSize
	}
	pause := n.cfg.SealJobBatchPause
	if pause == 0 {
		pause = defaultSealJobBatchPause
	}
	sess, err := n.systemSession()
	if err != nil {
		run.Err = err
		return run
	}
	what := "sealing the rows of " + j.Table
	if j.Kind == catalog.SealJobRotate {
		what = "moving the rows of " + j.Table + " to its rotated key's new version"
	}
	var opID string
	start := func() {
		if opID == "" {
			opID = newOpID()
			n.events.RecordStart("seal-job", opID, "%s (table id %d)", what, j.TableID)
		}
	}
	defer func() {
		if opID == "" && run.Err == nil {
			return
		}
		start()
		outcome, detail := "ok", "paused, to go on at its next turn"
		switch {
		case run.Err != nil:
			outcome, detail = "error", "will retry: "+run.Err.Error()
		case run.Done:
			detail = "done"
			if run.Destroyed > 0 {
				detail += "; older key versions destroyed"
			}
		}
		n.events.RecordEnd("seal-job", opID, outcome, "%s: %d row(s) rewritten in this run, %s", j.Table, run.Rewritten, detail)
	}()
	clock := func() time.Time { return time.Unix(0, n.clock.Now().WallTime) }
	for {
		if ctx.Err() != nil || time.Now().After(deadline) {
			return run
		}
		res, err := sess.SealJobStep(ctx, j.TableID, batch, clock)
		run.Rewritten += res.Rewritten
		run.Destroyed += res.Destroyed
		if err != nil {
			if ctx.Err() != nil {
				return run // the node is stopping: not the job's error
			}
			run.Err = err
			nctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
			if nerr := sess.NoteSealJobError(nctx, j.TableID, err, clock()); nerr != nil {
				log.Debugf("seal job of %s: recording its error: %v", j.Table, nerr)
			}
			cancel()
			log.Warnf("seal job of %s (table id %d): %v", j.Table, j.TableID, err)
			return run
		}
		switch res.Outcome {
		case sql.SealJobNone, sql.SealJobWaiting:
			return run
		case sql.SealJobDone, sql.SealJobGone:
			start()
			run.Done = true
			log.Infof("seal job of %s (table id %d): done", j.Table, j.TableID)
			return run
		}
		if res.Rewritten > 0 {
			start()
		}
		if pause > 0 {
			select {
			case <-ctx.Done():
				return run
			case <-time.After(pause):
			}
		}
	}
}

// putSealJobFor writes, in txn, a seal job for a table being sealed that
// has none: one restored while its rows were being sealed, or one whose
// record was lost (a whole-cluster restore brings descriptors back, not
// the jobs in flight). Nothing when it has one.
func putSealJobFor(ctx context.Context, txn *kvclient.Txn, d *catalog.TableDescriptor) error {
	raw, err := txn.Get(ctx, keys.SealJobKey(d.ID))
	if err != nil || raw != nil {
		return err
	}
	j := &catalog.SealJob{TableID: d.ID, Table: d.Name, Kind: catalog.SealJobSeal, DescVersion: d.Version, Pass: 1, Created: time.Now().UnixNano()}
	out, err := j.Encode()
	if err != nil {
		return err
	}
	return txn.Put(ctx, keys.SealJobKey(d.ID), out)
}
