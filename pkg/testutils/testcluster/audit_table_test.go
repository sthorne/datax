package testcluster

import (
	"context"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/sthorne/datax/pkg/audit"
	"github.com/sthorne/datax/pkg/server"
	"github.com/sthorne/datax/pkg/sql"
	"github.com/sthorne/datax/pkg/sql/catalog"
	"github.com/sthorne/datax/pkg/sql/types"
	"github.com/sthorne/datax/pkg/util/log"
	"github.com/sthorne/datax/pkg/version"
)

// auditOn turns the audit table on for a test cluster's nodes, with a
// flush fast enough not to be what the test waits for.
func auditOn(c *server.Config) { c.AuditFlushInterval = 20 * time.Millisecond }

// auditRecords reads the whole audit table as the node would write it.
func auditRecords(t *testing.T, ctx context.Context, s *sql.Session) []audit.Record {
	t.Helper()
	res, serr := trySQL(ctx, s, `SELECT node, seq, at, event, principal, target, outcome, detail, prev_hash, hash FROM datax_audit ORDER BY node, seq`)
	if serr != nil {
		if serr.Code == sql.CodeUndefinedTable {
			return nil
		}
		t.Fatalf("reading datax_audit: [%s] %s", serr.Code, serr.Msg)
	}
	out := make([]audit.Record, 0, len(res.Rows))
	for _, r := range res.Rows {
		out = append(out, audit.Record{
			Node: r[0].I, Seq: r[1].I, AtMicros: r[2].I / 1000, Event: r[3].S, Principal: r[4].S,
			Target: r[5].S, Outcome: r[6].S, Detail: r[7].S, PrevHash: r[8].S, Hash: r[9].S,
		})
	}
	return out
}

// findAudit returns the first record matching event and target.
func findAudit(recs []audit.Record, event, target string) *audit.Record {
	for i := range recs {
		if recs[i].Event == event && recs[i].Target == target {
			return &recs[i]
		}
	}
	return nil
}

// waitAudit polls until the table holds a record of event on target.
func waitAudit(t *testing.T, ctx context.Context, s *sql.Session, event, target string) []audit.Record {
	t.Helper()
	deadline := time.Now().Add(30 * time.Second)
	for {
		recs := auditRecords(t, ctx, s)
		if findAudit(recs, event, target) != nil {
			return recs
		}
		if time.Now().After(deadline) {
			t.Fatalf("no %s record for %q after 30s (%d records)", event, target, len(recs))
		}
		time.Sleep(50 * time.Millisecond)
	}
}

// plantChain writes a chain of n records for a node that is not in the
// cluster, as that node would have: what a decommissioned node leaves.
func plantChain(t *testing.T, ctx context.Context, system *sql.Session, node int64, n int, at time.Time) []audit.Record {
	t.Helper()
	prev := audit.GenesisHash
	var out []audit.Record
	for i := int64(1); i <= int64(n); i++ {
		r := audit.Record{Node: node, Seq: i, AtMicros: at.UnixMicro() + i, Event: "role-ddl", Principal: "root", Target: "gone", Outcome: "ok", Detail: "{}", PrevHash: prev}
		r.Hash = r.ComputeHash()
		prev = r.Hash
		out = append(out, r)
		execSQL(t, ctx, system, `INSERT INTO datax_audit (node, seq, at, event, principal, target, outcome, detail, prev_hash, hash) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)`,
			types.NewInt(r.Node), types.NewInt(r.Seq), types.NewTimestamp(r.AtMicros*1000), types.NewString(r.Event), types.NewString(r.Principal),
			types.NewString(r.Target), types.NewString(r.Outcome), types.NewString(r.Detail), types.NewString(r.PrevHash), types.NewString(r.Hash))
	}
	return out
}

func auditProblems(rep *audit.Report) string {
	var all []string
	for _, c := range rep.Chains {
		all = append(all, c.Problems...)
	}
	return strings.Join(append(all, rep.Problems...), "\n")
}

// mustVerify fails the test unless the table's chains verify.
func mustVerify(t *testing.T, ctx context.Context, s *sql.Session, trusted []audit.Head) *audit.Report {
	t.Helper()
	rep := audit.Verify(auditRecords(t, ctx, s), trusted)
	if !rep.OK() {
		t.Fatalf("the audit chain does not verify:\n%s", auditProblems(rep))
	}
	return rep
}

// TestAuditTable (issue #293): audit events become records in
// datax_audit with their principal, target and outcome; no record holds
// a password; only admins and roles granted SELECT read the table, and
// no role — root, an admin, the reader — changes it through SQL in any
// way but the retention setting; the chain verifies, /api/security
// publishes its head, and a record changed or removed underneath the
// cluster is found.
func TestAuditTable(t *testing.T) {
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	first := true
	tc := StartWithOptions(t, 3, func(c *server.Config) {
		auditOn(c)
		if first {
			c.HTTPListener = lis
			first = false
		}
	})
	ctx, cancel := context.WithTimeout(context.Background(), 180*time.Second)
	defer cancel()
	root := sql.NewSession(tc.Nodes[0].DB(), catalog.NewAccessor())

	const secret = "hunter2-Correct-Horse"
	execSQL(t, ctx, root, `CREATE ROLE audit_alice LOGIN PASSWORD '`+secret+`'`)
	execSQL(t, ctx, root, `ALTER ROLE audit_alice PASSWORD '`+secret+`-2'`)
	execSQL(t, ctx, root, `CREATE ROLE audit_carol LOGIN`)
	execSQL(t, ctx, root, `GRANT admin TO audit_carol`)
	execSQL(t, ctx, root, `CREATE ROLE audit_reader LOGIN`)
	execSQL(t, ctx, root, `CREATE ROLE audit_nosy LOGIN`)
	execSQL(t, ctx, root, `GRANT read_all TO audit_nosy`)
	execSQL(t, ctx, root, `CREATE TABLE audit_t (id INT8 PRIMARY KEY, v TEXT)`)
	execSQL(t, ctx, root, `GRANT SELECT ON audit_t TO audit_alice`)

	// A second principal acting, through another node; and the kinds of
	// record that are refusals rather than actions.
	carol := sql.NewSessionForUser(tc.Nodes[1].DB(), catalog.NewAccessor(), "audit_carol")
	execSQL(t, ctx, carol, `CREATE ROLE audit_dave`)
	log.Audit("sql-auth-failure", "principal", "audit_mallory", "remote", "203.0.113.7:50123")
	log.Audit("admin-denied", "op", "audit-test-op", "principal", "audit_alice")
	log.Audit("admin-op", "op", "audit-test-backup", "principal", "audit_carol", "outcome", "error", "error", "no such directory")

	recs := waitAudit(t, ctx, root, "admin-op", "audit-test-backup")
	for _, want := range []struct{ event, target, principal, outcome string }{
		{"role-ddl", "audit_alice", "root", "ok"},
		{"role-ddl", "audit_dave", "audit_carol", "ok"},
		{"privilege-ddl", "audit_t", "root", "ok"},
		{"sql-auth-failure", "", "audit_mallory", "failed"},
		{"admin-denied", "audit-test-op", "audit_alice", "denied"},
		{"admin-op", "audit-test-backup", "audit_carol", "error"},
	} {
		var got *audit.Record
		for i := range recs {
			if recs[i].Event == want.event && recs[i].Target == want.target && recs[i].Principal == want.principal {
				got = &recs[i]
				break
			}
		}
		if got == nil {
			t.Fatalf("no %s record on %q by %q", want.event, want.target, want.principal)
		}
		if got.Outcome != want.outcome {
			t.Errorf("%s on %q: outcome %q, want %q", want.event, want.target, got.Outcome, want.outcome)
		}
		if age := time.Since(time.UnixMicro(got.AtMicros)); age < 0 || age > 5*time.Minute {
			t.Errorf("%s on %q: recorded at %v", want.event, want.target, time.UnixMicro(got.AtMicros))
		}
	}
	if r := findAudit(recs, "role-ddl", "audit_dave"); !strings.Contains(r.Detail, `"stmt":"CREATE ROLE"`) || !strings.Contains(r.Detail, `"role":"audit_carol"`) {
		t.Errorf("role-ddl detail: %s", r.Detail)
	}
	// The refusal keeps the address and not the port it came from.
	for i := range recs {
		if recs[i].Principal == "audit_mallory" && !strings.Contains(recs[i].Detail, `"remote":"203.0.113.7"`) {
			t.Errorf("auth failure detail: %s", recs[i].Detail)
		}
	}

	// No record holds the password, in any column.
	raw := execSQL(t, ctx, root, `SELECT * FROM datax_audit`)
	for _, row := range raw.Rows {
		for _, d := range row {
			if strings.Contains(d.S, "hunter2") || strings.Contains(d.S, "Correct-Horse") {
				t.Fatalf("a password reached the audit table: %q", d.S)
			}
		}
	}

	// Reading: root and admins; a role granted SELECT on the table; and
	// nobody else, read_all included.
	alice := sql.NewSessionForUser(tc.Nodes[2].DB(), catalog.NewAccessor(), "audit_alice")
	nosy := sql.NewSessionForUser(tc.Nodes[2].DB(), catalog.NewAccessor(), "audit_nosy")
	reader := sql.NewSessionForUser(tc.Nodes[2].DB(), catalog.NewAccessor(), "audit_reader")
	for name, s := range map[string]*sql.Session{"an ordinary role": alice, "read_all": nosy, "the reader before its grant": reader} {
		if _, serr := trySQL(ctx, s, `SELECT count(*) FROM datax_audit`); serr == nil || serr.Code != sql.CodeInsufficientPriv {
			t.Fatalf("%s read the audit table: %v", name, serr)
		}
	}
	if _, serr := trySQL(ctx, alice, `GRANT SELECT ON datax_audit TO audit_alice`); serr == nil {
		t.Fatal("a non-admin granted itself SELECT on the audit table")
	}
	execSQL(t, ctx, carol, `GRANT SELECT ON datax_audit TO audit_reader`)
	// A session without descriptor leasing keeps the descriptor it first
	// read, so the grant is seen by a new one.
	reader = sql.NewSessionForUser(tc.Nodes[2].DB(), catalog.NewAccessor(), "audit_reader")
	for name, s := range map[string]*sql.Session{"an admin": carol, "the reader": reader} {
		if res, serr := trySQL(ctx, s, `SELECT count(*) FROM datax_audit WHERE event = 'role-ddl'`); serr != nil || res.Rows[0][0].I < 5 {
			t.Fatalf("%s could not read the audit table: %v", name, serr)
		}
	}

	// Writing: nobody, by any statement.
	for name, s := range map[string]*sql.Session{"root": root, "an admin": carol, "the reader": reader, "an ordinary role": alice} {
		for _, stmt := range []string{
			`INSERT INTO datax_audit (node, seq, at, event, principal, target, outcome, detail, prev_hash, hash) VALUES (1, 100000, now(), 'x', 'x', 'x', 'x', '{}', 'x', 'x')`,
			`UPDATE datax_audit SET principal = 'nobody' WHERE node = 1`,
			`DELETE FROM datax_audit WHERE node = 1`,
			`DELETE FROM datax_audit`,
			`TRUNCATE datax_audit`,
			`DROP TABLE datax_audit`,
			`ALTER TABLE datax_audit ADD COLUMN note TEXT`,
			`ALTER TABLE datax_audit DROP COLUMN detail`,
			`ALTER TABLE datax_audit RENAME TO audit_gone`,
			`ALTER TABLE datax_audit OWNER TO audit_carol`,
			`ALTER TABLE datax_audit SET (shards = 4)`,
			`ALTER TABLE datax_audit ADD CONSTRAINT nothing CHECK (seq < 0)`,
			`CREATE INDEX audit_by_event ON datax_audit (event)`,
			`COMMENT ON TABLE datax_audit IS 'mine'`,
			`GRANT INSERT ON datax_audit TO audit_alice`,
			`GRANT ALL ON datax_audit TO audit_alice`,
			`GRANT SELECT ON datax_audit TO audit_alice WITH GRANT OPTION`,
			`CREATE TABLE audit_ref (node INT8, seq INT8, PRIMARY KEY (node, seq), FOREIGN KEY (node, seq) REFERENCES datax_audit (node, seq) ON DELETE CASCADE)`,
			`CREATE TABLE datax_audit (id INT8 PRIMARY KEY)`,
		} {
			if _, serr := trySQL(ctx, s, stmt); serr == nil {
				t.Fatalf("%s ran %q", name, stmt)
			} else if serr.Code != sql.CodeInsufficientPriv {
				t.Errorf("%s, %q: [%s] %s, want insufficient privilege", name, stmt, serr.Code, serr.Msg)
			}
		}
	}
	before := len(recs)
	if after := auditRecords(t, ctx, root); len(after) < before {
		t.Fatalf("the table shrank from %d to %d records", before, len(after))
	}

	// The one setting: how long records are kept. Admins only, never
	// under a day, and the change is a record.
	if _, serr := trySQL(ctx, reader, `ALTER TABLE datax_audit SET (retention = '30d')`); serr == nil || serr.Code != sql.CodeInsufficientPriv {
		t.Fatalf("a non-admin set the retention: %v", serr)
	}
	if _, serr := trySQL(ctx, carol, `ALTER TABLE datax_audit SET (retention = '1h')`); serr == nil || !strings.Contains(serr.Msg, "at least one day") {
		t.Fatalf("retention under a day: %v", serr)
	}
	execSQL(t, ctx, carol, `ALTER TABLE datax_audit SET (retention = '30d')`)
	recs = waitAudit(t, ctx, root, audit.EventRetentionSet, "datax_audit")
	if r := findAudit(recs, audit.EventRetentionSet, "datax_audit"); r.Principal != "audit_carol" || !strings.Contains(r.Detail, `"seconds":2592000`) {
		t.Errorf("retention change record: %+v", r)
	}

	// One node of this process records (the first), in its own chain,
	// and the chain verifies.
	rep := mustVerify(t, ctx, root, nil)
	if len(rep.Chains) != 1 || rep.Chains[0].Node != 1 || rep.Chains[0].FirstSeq != 1 {
		t.Fatalf("chains: %+v", rep.Chains)
	}

	// /api/security publishes the head, and it is the table's.
	tc.Nodes[0].AuditFlush(ctx)
	var doc server.SecurityStatus
	code, _, body := httpGet(t, "http://"+tc.Nodes[0].HTTPAddr()+"/api/security")
	if code != 200 {
		t.Fatalf("/api/security: %d %s", code, body)
	}
	if err := jsonUnmarshal([]byte(body), &doc); err != nil {
		t.Fatal(err)
	}
	rep = mustVerify(t, ctx, root, nil)
	if !doc.Audit.Enabled || !doc.Audit.Ready || doc.Audit.Head == nil || doc.Audit.Table != "datax_audit" {
		t.Fatalf("/api/security audit: %+v", doc.Audit)
	}
	if h := rep.Chains[0].Head; doc.Audit.Head.Seq > h.Seq || doc.Audit.Head.Seq < h.Seq-5 {
		t.Fatalf("/api/security head %+v, table head %+v", doc.Audit.Head, h)
	}
	trusted := []audit.Head{*doc.Audit.Head}
	mustVerify(t, ctx, root, trusted)

	// Tampering needs more than SQL gives anyone: the node's own
	// session stands in here for somebody writing the keys directly.
	system := sql.NewSystemSession(tc.Nodes[0].DB(), catalog.NewAccessor())
	target := findAudit(recs, "role-ddl", "audit_dave")
	execSQL(t, ctx, system, `UPDATE datax_audit SET principal = 'root' WHERE node = $1 AND seq = $2`, types.NewInt(target.Node), types.NewInt(target.Seq))
	if got := auditProblems(audit.Verify(auditRecords(t, ctx, root), nil)); !strings.Contains(got, "the record does not match its hash: it was altered") {
		t.Fatalf("an altered record verified: %q", got)
	}
	execSQL(t, ctx, system, `DELETE FROM datax_audit WHERE node = $1 AND seq = $2`, types.NewInt(target.Node), types.NewInt(target.Seq))
	if got := auditProblems(audit.Verify(auditRecords(t, ctx, root), nil)); !strings.Contains(got, "are missing: records were removed") {
		t.Fatalf("a removed record verified: %q", got)
	}
	// The newest records removed: no link is broken, the node's own
	// memory of its head is what notices, and it says so in the chain.
	head := rep.Chains[0].Head
	execSQL(t, ctx, system, `DELETE FROM datax_audit WHERE node = 1 AND seq > $1`, types.NewInt(head.Seq-2))
	execSQL(t, ctx, root, `CREATE ROLE audit_after_cut`)
	recs = waitAudit(t, ctx, root, "role-ddl", "audit_after_cut")
	if r := findAudit(recs, audit.EventRestart, "datax_audit"); r == nil || !strings.Contains(r.Detail, `"lost_seq"`) {
		t.Fatalf("no restart record after the head was removed: %+v", r)
	}
	if got := auditProblems(audit.Verify(recs, trusted)); !strings.Contains(got, "seq "+itoa(int(trusted[0].Seq))+" is not the record the trusted head saw") &&
		!strings.Contains(got, "newest records were removed") {
		t.Fatalf("a removed tail verified against the head saved earlier: %q", got)
	}
}

// TestAuditTableUnavailable (issue #293): recording an event never
// waits on the cluster. With the recording node cut off from its peers
// an audit call returns at once and the event waits in the node; when
// the partition heals it is written, in order, and the chain verifies.
// With a node stopped the others record as before, and when the
// recording node itself stops another takes over in a chain of its own.
func TestAuditTableUnavailable(t *testing.T) {
	tc, engines := StartWithEngines(t, 3, auditOn)
	ctx, cancel := context.WithTimeout(context.Background(), 180*time.Second)
	defer cancel()
	root := sql.NewSession(tc.Nodes[1].DB(), catalog.NewAccessor())
	execSQL(t, ctx, root, `CREATE ROLE audit_before`)
	waitAudit(t, ctx, root, "role-ddl", "audit_before")

	tc.Isolate(0)
	start := time.Now()
	for i := 0; i < 20; i++ {
		log.Audit("admin-op", "op", "audit-partitioned-"+itoa(i), "principal", "root", "outcome", "ok")
	}
	if d := time.Since(start); d > 2*time.Second {
		t.Fatalf("20 audit calls took %v with the node partitioned", d)
	}
	// The node tries, fails, and keeps them.
	deadline := time.Now().Add(60 * time.Second)
	for {
		st := tc.Nodes[0].AuditStatus()
		if st.WriteErrors > 0 && st.Buffered >= 20 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("partitioned node's audit state: %+v", st)
		}
		time.Sleep(100 * time.Millisecond)
	}
	tc.Heal()
	recs := waitAudit(t, ctx, root, "admin-op", "audit-partitioned-19")
	last := int64(0)
	for i := 0; i < 20; i++ {
		r := findAudit(recs, "admin-op", "audit-partitioned-"+itoa(i))
		if r == nil || r.Seq <= last {
			t.Fatalf("event %d after the partition: %+v (previous seq %d)", i, r, last)
		}
		last = r.Seq
		if age := time.Since(time.UnixMicro(r.AtMicros)); age > 3*time.Minute {
			t.Fatalf("event %d carries the time it was written, not when it happened: %v", i, age)
		}
	}
	mustVerify(t, ctx, root, nil)

	// A node down: nothing changes for the others.
	tc.StopNode(2)
	execSQL(t, ctx, root, `CREATE ROLE audit_one_down`)
	waitAudit(t, ctx, root, "role-ddl", "audit_one_down")
	head1 := mustVerify(t, ctx, root, nil).Chains[0].Head

	// The recording node down (and the other back): the next node of
	// the process records, in a chain of its own; the first one's is
	// left as it was and still verifies.
	tc.RestartNode(2, engines[2], auditOn)
	execSQL(t, ctx, sql.NewSession(tc.Nodes[2].DB(), catalog.NewAccessor()), `SELECT count(*) FROM datax_audit`)
	tc.StopNode(0)
	execSQL(t, ctx, root, `CREATE ROLE audit_takeover`)
	recs = waitAudit(t, ctx, root, "role-ddl", "audit_takeover")
	if r := findAudit(recs, "role-ddl", "audit_takeover"); r.Node != 2 {
		t.Fatalf("the takeover record is in node %d's chain, want node 2's", r.Node)
	}
	rep := mustVerify(t, ctx, root, []audit.Head{head1})
	if len(rep.Chains) != 2 || rep.Chains[0].Head != head1 {
		t.Fatalf("chains after takeover: %+v", rep.Chains)
	}
}

// TestAuditTableRetention (issue #293): records older than the
// retention leave the table from the old end of each chain, the chain
// first recording where it is cut, so what remains still verifies and
// a cut nothing accounts for does not.
func TestAuditTableRetention(t *testing.T) {
	tc := StartWithOptions(t, 1, func(c *server.Config) {
		auditOn(c)
		c.AuditRetention = 3 * time.Second
		c.AuditHousekeepingInterval = time.Hour // the test runs the passes itself
	})
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()
	root := sql.NewSession(tc.Nodes[0].DB(), catalog.NewAccessor())
	for i := 0; i < 5; i++ {
		execSQL(t, ctx, root, `CREATE ROLE audit_old_`+itoa(i))
	}
	old := waitAudit(t, ctx, root, "role-ddl", "audit_old_4")
	oldHead := old[len(old)-1]

	// Nothing is old enough yet: a pass removes nothing and records
	// nothing but the anchor.
	tc.Nodes[0].AuditHousekeep(ctx)
	if recs := auditRecords(t, ctx, root); recs[0].Seq != 1 || findAudit(recs, audit.EventRetention, "node 1") != nil {
		t.Fatalf("a pass before anything expired changed the chain: first seq %d", recs[0].Seq)
	}

	time.Sleep(3500 * time.Millisecond)
	execSQL(t, ctx, root, `CREATE ROLE audit_young`)
	waitAudit(t, ctx, root, "role-ddl", "audit_young")
	tc.Nodes[0].AuditHousekeep(ctx)

	recs := auditRecords(t, ctx, root)
	for i := 0; i < 5; i++ {
		if findAudit(recs, "role-ddl", "audit_old_"+itoa(i)) != nil {
			t.Fatalf("audit_old_%d outlived the retention", i)
		}
	}
	if findAudit(recs, "role-ddl", "audit_young") == nil {
		t.Fatal("a record younger than the retention was removed")
	}
	cut := findAudit(recs, audit.EventRetention, "node 1")
	if cut == nil || recs[0].Seq <= oldHead.Seq || !strings.Contains(cut.Detail, `"through_seq":`+itoa(int(recs[0].Seq-1))) {
		t.Fatalf("after retention the chain starts at seq %d; cut record %+v", recs[0].Seq, cut)
	}
	rep := mustVerify(t, ctx, root, nil)
	if notes := strings.Join(rep.Chains[0].Notes, "\n"); !strings.Contains(notes, "were aged out by retention") {
		t.Fatalf("notes: %q", notes)
	}
	// The chain of a node that has left the cluster is aged out by the
	// node leading range 1, which records the cut in its own chain. The
	// last record stays, as a chain's head always does.
	system := sql.NewSystemSession(tc.Nodes[0].DB(), catalog.NewAccessor())
	gone := plantChain(t, ctx, system, 9, 6, time.Now().Add(-time.Hour))
	tc.Nodes[0].AuditHousekeep(ctx)
	recs = auditRecords(t, ctx, root)
	var left []audit.Record
	for _, r := range recs {
		if r.Node == 9 {
			left = append(left, r)
		}
	}
	if len(left) != 1 || left[0] != gone[5] {
		t.Fatalf("the departed node's chain after retention: %+v", left)
	}
	if c := findAudit(recs, audit.EventRetention, "node 9"); c == nil || c.Node != 1 || !strings.Contains(c.Detail, `"through_seq":5`) {
		t.Fatalf("cut record for the departed node's chain: %+v", c)
	}
	mustVerify(t, ctx, root, nil)

	// The same cut made by hand, with no record of it, is a finding.
	recs = auditRecords(t, ctx, root)
	execSQL(t, ctx, system, `DELETE FROM datax_audit WHERE node = 1 AND seq = $1`, types.NewInt(recs[0].Seq))
	if got := auditProblems(audit.Verify(auditRecords(t, ctx, root), nil)); !strings.Contains(got, "no retention record accounts for") {
		t.Fatalf("an unrecorded cut verified: %q", got)
	}
}

// TestAuditTableRestartAndRestore (issue #293): a node that restarts
// carries its chain on from where the table has it; a restore replaces
// the table's history with the backup's, and the node says in the chain
// that the head it had written is gone before it carries on.
func TestAuditTableRestartAndRestore(t *testing.T) {
	tc, engines := StartWithEngines(t, 1, auditOn)
	ctx, cancel := context.WithTimeout(context.Background(), 180*time.Second)
	defer cancel()
	root := sql.NewSession(tc.Nodes[0].DB(), catalog.NewAccessor())
	execSQL(t, ctx, root, `CREATE ROLE audit_first_life`)
	recs := waitAudit(t, ctx, root, "role-ddl", "audit_first_life")
	before := recs[len(recs)-1]

	tc.StopNode(0)
	tc.RestartNode(0, engines[0], auditOn)
	root = sql.NewSession(tc.Nodes[0].DB(), catalog.NewAccessor())
	execSQL(t, ctx, root, `CREATE ROLE audit_second_life`)
	recs = waitAudit(t, ctx, root, "role-ddl", "audit_second_life")
	if r := findAudit(recs, "role-ddl", "audit_second_life"); r.Seq <= before.Seq {
		t.Fatalf("after the restart the chain went back: seq %d, was at %d", r.Seq, before.Seq)
	}
	if findAudit(recs, audit.EventRestart, "datax_audit") != nil {
		t.Fatal("an ordinary restart was recorded as a chain restart")
	}
	rep := mustVerify(t, ctx, root, []audit.Head{{Node: before.Node, Seq: before.Seq, Hash: before.Hash}})
	if rep.Chains[0].FirstSeq != 1 {
		t.Fatalf("chain after restart: %+v", rep.Chains[0])
	}

	// Back the cluster up, then restore into a fresh one that has a
	// short audit history of its own.
	dir := t.TempDir()
	if _, err := tc.Nodes[0].RunBackup(ctx, dir, "", true, false); err != nil {
		t.Fatalf("backup: %v", err)
	}
	backedUp := auditRecords(t, ctx, root)
	tc.StopAll()

	tc2 := StartWithOptions(t, 1, auditOn)
	root2 := sql.NewSession(tc2.Nodes[0].DB(), catalog.NewAccessor())
	log.Audit("admin-op", "op", "audit-target-history", "principal", "root", "outcome", "ok")
	waitAudit(t, ctx, root2, "admin-op", "audit-target-history")
	if _, err := tc2.Nodes[0].RunRestore(ctx, []string{dir}); err != nil {
		t.Fatalf("restore: %v", err)
	}
	log.Audit("admin-op", "op", "audit-after-restore", "principal", "root", "outcome", "ok")
	recs = waitAudit(t, ctx, root2, "admin-op", "audit-after-restore")
	if findAudit(recs, "role-ddl", "audit_first_life") == nil {
		t.Fatal("the backup's audit records were not restored")
	}
	if findAudit(recs, "admin-op", "audit-target-history") != nil {
		t.Fatal("the target's own records were interleaved with the backup's")
	}
	restart := findAudit(recs, audit.EventRestart, "datax_audit")
	if restart == nil || restart.Seq <= backedUp[len(backedUp)-1].Seq {
		t.Fatalf("no restart record after the restore: %+v", restart)
	}
	rep = audit.Verify(recs, []audit.Head{{Node: before.Node, Seq: before.Seq, Hash: before.Hash}})
	if !rep.OK() || !strings.Contains(strings.Join(rep.Chains[0].Notes, "\n"), "restarted its chain") {
		t.Fatalf("chain after restore: %s / %v", auditProblems(rep), rep.Chains[0].Notes)
	}
}

// TestAuditTableAnchors (issue #293): the node leading range 1 writes
// every chain's head into its own, once per movement and not forever;
// a chain shortened behind an anchor's back is found from the anchor.
func TestAuditTableAnchors(t *testing.T) {
	tc := StartWithOptions(t, 1, func(c *server.Config) {
		auditOn(c)
		c.AuditHousekeepingInterval = time.Hour
	})
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()
	root := sql.NewSession(tc.Nodes[0].DB(), catalog.NewAccessor())
	execSQL(t, ctx, root, `CREATE ROLE audit_anchor_me`)
	waitAudit(t, ctx, root, "role-ddl", "audit_anchor_me")

	// A chain left behind by a node that is no longer a member.
	system := sql.NewSystemSession(tc.Nodes[0].DB(), catalog.NewAccessor())
	gone := plantChain(t, ctx, system, 9, 4, time.Now())

	tc.Nodes[0].AuditHousekeep(ctx)
	recs := auditRecords(t, ctx, root)
	anchor := findAudit(recs, audit.EventAnchor, "datax_audit")
	if anchor == nil || !strings.Contains(anchor.Detail, `{"node":9,"seq":4,"hash":"`+gone[3].Hash+`"}`) {
		t.Fatalf("anchor: %+v", anchor)
	}
	n := len(recs)
	tc.Nodes[0].AuditHousekeep(ctx)
	tc.Nodes[0].AuditHousekeep(ctx)
	if recs = auditRecords(t, ctx, root); len(recs) != n {
		t.Fatalf("idle passes wrote %d more records", len(recs)-n)
	}
	mustVerify(t, ctx, root, nil)

	execSQL(t, ctx, system, `DELETE FROM datax_audit WHERE node = 9 AND seq > 2`)
	if got := auditProblems(audit.Verify(auditRecords(t, ctx, root), nil)); !strings.Contains(got, "saw this chain at seq 4 and it now ends at seq 2") {
		t.Fatalf("a chain shortened behind an anchor verified: %q", got)
	}
}

// TestAuditTableBeforeFinalize (issue #293, compatibility rule 4): a
// cluster that has not finalized v21 has nodes that would treat the
// table as anyone's, so nothing creates it; audit records are in the
// log, as before, and are not saved up for later.
func TestAuditTableBeforeFinalize(t *testing.T) {
	tc, _ := StartWithEngines(t, 1, func(c *server.Config) {
		auditOn(c)
		c.BinaryVersionOverride = version.V20
	})
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	root := sql.NewSession(tc.Nodes[0].DB(), catalog.NewAccessor())
	var logged int
	log.SetAuditSink(func(event string, kv []any) { logged++ })
	defer log.SetAuditSink(nil)
	execSQL(t, ctx, root, `CREATE ROLE audit_too_early`)
	if logged == 0 {
		t.Fatal("the audit log lost the record")
	}
	deadline := time.Now().Add(10 * time.Second)
	for tc.Nodes[0].AuditStatus().Buffered > 0 {
		if time.Now().After(deadline) {
			t.Fatalf("events are being held for a table that cannot exist yet: %+v", tc.Nodes[0].AuditStatus())
		}
		time.Sleep(50 * time.Millisecond)
	}
	if _, serr := trySQL(ctx, root, `SELECT count(*) FROM datax_audit`); serr == nil || serr.Code != sql.CodeUndefinedTable {
		t.Fatalf("datax_audit exists before v21 is finalized: %v", serr)
	}
	if st := tc.Nodes[0].AuditStatus(); st.Ready || st.Head != nil {
		t.Fatalf("audit status before finalize: %+v", st)
	}
}

// TestAuditTableSecure (issue #293): the same over the wire on a secure
// cluster, where the events are real ones — a wrong password is a
// record naming the account tried, the statement that set a password is
// a record without it, and a client connected as an ordinary user can
// neither read the table nor, as root, change it.
func TestAuditTableSecure(t *testing.T) {
	tc, certsDir := startSecureCluster(t, "topsecret", func(i int, c *server.Config) { auditOn(c) })
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	var conn *pgx.Conn
	deadline := time.Now().Add(30 * time.Second)
	for {
		var err error
		if conn, err = connectSecure(ctx, secureURL(tc, certsDir, "root", "topsecret")); err == nil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("secure SQL never came up: %v", err)
		}
		time.Sleep(200 * time.Millisecond)
	}
	defer func() { _ = conn.Close(ctx) }()
	if _, err := conn.Exec(ctx, `CREATE USER audit_wire PASSWORD 'wire-pw-Zebra'`); err != nil {
		t.Fatal(err)
	}
	if c, err := connectSecure(ctx, secureURL(tc, certsDir, "audit_wire", "not-the-password")); err == nil {
		_ = c.Close(ctx)
		t.Fatal("a wrong password was accepted")
	}

	type row struct {
		event, principal, target, outcome, detail string
	}
	read := func() []row {
		rows, err := conn.Query(ctx, `SELECT event, principal, target, outcome, detail FROM datax_audit ORDER BY node, seq`)
		if err != nil {
			return nil // not created yet
		}
		defer rows.Close()
		var out []row
		for rows.Next() {
			var r row
			if err := rows.Scan(&r.event, &r.principal, &r.target, &r.outcome, &r.detail); err != nil {
				t.Fatal(err)
			}
			out = append(out, r)
		}
		return out
	}
	var all []row
	deadline = time.Now().Add(30 * time.Second)
	for {
		all = read()
		created, failed := false, false
		for _, r := range all {
			created = created || (r.event == "role-ddl" && r.target == "audit_wire" && r.principal == "root" && r.outcome == "ok")
			failed = failed || (r.event == "sql-auth-failure" && r.principal == "audit_wire" && r.outcome == "failed" && strings.Contains(r.detail, `"remote":"127.0.0.1"`))
		}
		if created && failed {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("created=%v failed=%v in %+v", created, failed, all)
		}
		time.Sleep(100 * time.Millisecond)
	}
	for _, r := range all {
		for _, secret := range []string{"wire-pw-Zebra", "not-the-password", "topsecret"} {
			if strings.Contains(r.detail+r.target+r.principal, secret) {
				t.Fatalf("a password reached the audit table: %+v", r)
			}
		}
	}

	if _, err := conn.Exec(ctx, `DELETE FROM datax_audit`); err == nil || !strings.Contains(err.Error(), "42501") {
		t.Fatalf("root deleted audit records over the wire: %v", err)
	}
	user, err := connectSecure(ctx, secureURL(tc, certsDir, "audit_wire", "wire-pw-Zebra"))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = user.Close(ctx) }()
	if _, err := user.Exec(ctx, `SELECT count(*) FROM datax_audit`); err == nil || !strings.Contains(err.Error(), "42501") {
		t.Fatalf("an ordinary user read the audit table over the wire: %v", err)
	}
}
