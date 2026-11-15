package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"os"
	"strconv"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/sthorne/datax/pkg/audit"
	"github.com/sthorne/datax/pkg/cli"
	"github.com/sthorne/datax/pkg/sql/catalog"
)

// datax debug audit (issue #293): the audit table read out of the
// cluster and checked outside it. The reading is plain SQL, as any role
// granted SELECT on the table — the checking deliberately happens here
// and not in the node, so that the answer does not depend on trusting
// the thing being checked.
//
//	export   every record, one JSON object per line
//	heads    the last record of every chain: the few bytes to keep
//	         somewhere the cluster cannot write
//	verify   check the chains, from the cluster or from an export, and
//	         optionally against heads saved earlier

// auditPage is how many records one SELECT fetches.
const auditPage = 1000

type auditConnFlags struct {
	url, certsDir, user *string
	timeout             *time.Duration
}

func addAuditConnFlags(fs *flag.FlagSet) auditConnFlags {
	return auditConnFlags{
		url:      fs.String("url", "postgres://root@127.0.0.1:26433/datax?sslmode=disable", "database URL; the user must be an admin or hold SELECT on "+catalog.AuditTableName),
		certsDir: fs.String("certs-dir", "", "certificate directory for a secure cluster (presents client.<user>.crt)"),
		user:     fs.String("user", "", "username (default: the URL's user)"),
		timeout:  connectTimeoutFlag(fs),
	}
}

func (f auditConnFlags) connect(ctx context.Context) (*pgx.Conn, error) {
	cfg, err := cli.SQLConfig(*f.url, *f.certsDir, *f.user)
	if err != nil {
		return nil, err
	}
	cfg.DefaultQueryExecMode = pgx.QueryExecModeSimpleProtocol
	var conn *pgx.Conn
	err = cli.Connect(ctx, nil, cli.SQLTarget(cfg), cli.SQLKind(cfg, *f.certsDir), *f.timeout, func(ctx context.Context) error {
		var err error
		conn, err = pgx.ConnectConfig(ctx, cfg)
		return err
	})
	return conn, err
}

// auditChainNodes lists the nodes with a chain, seeking from one
// node's records to the next's.
func auditChainNodes(ctx context.Context, conn *pgx.Conn) ([]int64, error) {
	var out []int64
	after := int64(-1 << 62)
	for {
		var node int64
		err := conn.QueryRow(ctx, "SELECT node FROM "+catalog.AuditTableName+" WHERE node > $1 ORDER BY node, seq LIMIT 1", after).Scan(&node)
		if err == pgx.ErrNoRows {
			return out, nil
		}
		if err != nil {
			return nil, err
		}
		out = append(out, node)
		after = node
	}
}

// readAuditTable reads every record, a page of one chain at a time.
func readAuditTable(ctx context.Context, conn *pgx.Conn) ([]audit.Record, error) {
	nodes, err := auditChainNodes(ctx, conn)
	if err != nil {
		return nil, err
	}
	var out []audit.Record
	for _, node := range nodes {
		after := int64(0)
		for {
			rows, err := conn.Query(ctx, "SELECT seq, at, event, principal, target, outcome, detail, prev_hash, hash FROM "+catalog.AuditTableName+
				" WHERE node = $1 AND seq > $2 ORDER BY seq LIMIT "+strconv.Itoa(auditPage), node, after)
			if err != nil {
				return nil, err
			}
			got := 0
			for rows.Next() {
				r := audit.Record{Node: node}
				var at time.Time
				if err := rows.Scan(&r.Seq, &at, &r.Event, &r.Principal, &r.Target, &r.Outcome, &r.Detail, &r.PrevHash, &r.Hash); err != nil {
					rows.Close()
					return nil, err
				}
				r.AtMicros = at.UnixMicro()
				out = append(out, r)
				after = r.Seq
				got++
			}
			rows.Close()
			if err := rows.Err(); err != nil {
				return nil, err
			}
			if got < auditPage {
				break
			}
		}
	}
	return out, nil
}

// readAuditHeads reads the last record of every chain.
func readAuditHeads(ctx context.Context, conn *pgx.Conn) ([]audit.Head, error) {
	nodes, err := auditChainNodes(ctx, conn)
	if err != nil {
		return nil, err
	}
	out := make([]audit.Head, 0, len(nodes))
	for _, node := range nodes {
		h := audit.Head{Node: node}
		err := conn.QueryRow(ctx, "SELECT seq, hash FROM "+catalog.AuditTableName+" WHERE node = $1 ORDER BY seq DESC LIMIT 1", node).Scan(&h.Seq, &h.Hash)
		if err == pgx.ErrNoRows {
			continue
		}
		if err != nil {
			return nil, err
		}
		out = append(out, h)
	}
	return out, nil
}

func runDebugAudit(args []string) error { return debugAudit(args, os.Stdout) }

func debugAudit(args []string, stdout io.Writer) error {
	if len(args) < 1 {
		return fmt.Errorf("usage: datax debug audit <export|heads|verify> [flags]")
	}
	sub := args[0]
	fs := flag.NewFlagSet("debug audit "+sub, flag.ContinueOnError)
	cf := addAuditConnFlags(fs)
	var out, file, headsFile *string
	switch sub {
	case "export", "heads":
		out = fs.String("out", "", "write to this file instead of standard output")
	case "verify":
		file = fs.String("file", "", "verify this export (from `datax debug audit export`) instead of reading the cluster")
		headsFile = fs.String("heads", "", "heads saved earlier with `datax debug audit heads`: the chains must still contain them")
	default:
		return fmt.Errorf("unknown debug audit subcommand %q (want export, heads or verify)", sub)
	}
	if err := fs.Parse(args[1:]); err != nil {
		return err
	}
	ctx := context.Background()

	w := stdout
	if out != nil && *out != "" {
		f, err := os.OpenFile(*out, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o600)
		if err != nil {
			return err
		}
		defer func() { _ = f.Close() }()
		w = f
	}

	if sub == "verify" && *file != "" {
		f, err := os.Open(*file)
		if err != nil {
			return err
		}
		recs, err := audit.ReadJSONLines(f)
		_ = f.Close()
		if err != nil {
			return fmt.Errorf("%s: %w", *file, err)
		}
		return reportAuditVerify(stdout, recs, *headsFile)
	}

	conn, err := cf.connect(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = conn.Close(ctx) }()
	switch sub {
	case "heads":
		heads, err := readAuditHeads(ctx, conn)
		if err != nil {
			return err
		}
		enc := json.NewEncoder(w)
		enc.SetIndent("", "  ")
		return enc.Encode(heads)
	case "export":
		recs, err := readAuditTable(ctx, conn)
		if err != nil {
			return err
		}
		return audit.WriteJSONLines(w, recs)
	}
	recs, err := readAuditTable(ctx, conn)
	if err != nil {
		return err
	}
	return reportAuditVerify(stdout, recs, *headsFile)
}

// reportAuditVerify prints what verification found and returns an error
// — so the command exits non-zero — when it found anything.
func reportAuditVerify(w io.Writer, recs []audit.Record, headsFile string) error {
	var trusted []audit.Head
	if headsFile != "" {
		raw, err := os.ReadFile(headsFile)
		if err != nil {
			return err
		}
		if err := json.Unmarshal(raw, &trusted); err != nil {
			return fmt.Errorf("%s: %w", headsFile, err)
		}
	}
	rep := audit.Verify(recs, trusted)
	for _, c := range rep.Chains {
		state := "ok"
		if len(c.Problems) > 0 {
			state = "BROKEN"
		}
		fmt.Fprintf(w, "node %d: %s: %d records, seq %d-%d, head %s\n", c.Node, state, c.Records, c.FirstSeq, c.Head.Seq, c.Head.Hash)
		for _, p := range c.Problems {
			fmt.Fprintf(w, "  problem: %s\n", p)
		}
		for _, n := range c.Notes {
			fmt.Fprintf(w, "  note: %s\n", n)
		}
	}
	for _, p := range rep.Problems {
		fmt.Fprintf(w, "problem: %s\n", p)
	}
	if len(rep.Chains) == 0 && len(rep.Problems) == 0 {
		fmt.Fprintln(w, "no audit records")
	}
	if !rep.OK() {
		return fmt.Errorf("the audit chain does not verify")
	}
	if len(trusted) == 0 {
		fmt.Fprintln(w, "verified against the table alone: this shows the records are consistent with each other, not that none were rewritten together; pass --heads with heads saved earlier to show that")
	}
	return nil
}
