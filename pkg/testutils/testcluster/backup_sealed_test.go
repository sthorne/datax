package testcluster

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/base64"
	"encoding/hex"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/sthorne/datax/pkg/base"
	"github.com/sthorne/datax/pkg/cluster"
	"github.com/sthorne/datax/pkg/keys"
	"github.com/sthorne/datax/pkg/rpc"
	"github.com/sthorne/datax/pkg/server"
	"github.com/sthorne/datax/pkg/sql"
	"github.com/sthorne/datax/pkg/sql/catalog"
	"github.com/sthorne/datax/pkg/sql/types"
	"github.com/sthorne/datax/pkg/storage/enc"
	"github.com/sthorne/datax/pkg/util/hlc"
	"github.com/sthorne/datax/pkg/util/log"
)

func newBackupKey(t *testing.T) []byte {
	t.Helper()
	k := make([]byte, enc.KeyLen)
	if _, err := rand.Read(k); err != nil {
		t.Fatal(err)
	}
	return k
}

// sealedBackupSchema makes tables whose names, keys and values are easy
// to look for, and one (ledger) whose data file runs to several of the
// sealed format's 64 KiB chunks, of incompressible rows.
func sealedBackupSchema(t *testing.T, ctx context.Context, s *sql.Session) {
	t.Helper()
	execSQL(t, ctx, s, `CREATE TABLE patient_accounts (handle TEXT PRIMARY KEY, holder TEXT)`)
	execSQL(t, ctx, s, `INSERT INTO patient_accounts VALUES ('acct-zebra-0001', 'holder-quokka'), ('acct-zebra-0002', 'holder-wombat')`)
	execSQL(t, ctx, s, `CREATE TABLE ledger (id INT8 PRIMARY KEY, body TEXT)`)
	for i := 0; i < 48; i++ {
		raw := make([]byte, 3000)
		if _, err := rand.Read(raw); err != nil {
			t.Fatal(err)
		}
		execSQL(t, ctx, s, `INSERT INTO ledger VALUES ($1, $2)`, types.NewInt(int64(i)), types.NewString(hex.EncodeToString(raw)))
	}
	execSQL(t, ctx, s, `CREATE USER auditor_marmot PASSWORD 'trustno1'`)
}

// sumsByName is each table's live bytes and checksum. The record count is
// left out: a backup's counts the tombstones it carries and a restore's
// the live rows.
func sumsByName(sum *cluster.BackupSummary) map[string]string {
	out := map[string]string{}
	for _, t := range sum.Tables {
		out[t.Name] = fmt.Sprintf("%d/%s", t.Bytes, t.SHA256)
	}
	return out
}

func requireSameSums(t *testing.T, what string, got, want *cluster.BackupSummary) {
	t.Helper()
	g, w := sumsByName(got), sumsByName(want)
	if len(g) != len(w) || len(g) == 0 {
		t.Fatalf("%s: %d tables, want %d", what, len(g), len(w))
	}
	for name, sum := range w {
		if g[name] != sum {
			t.Fatalf("%s: table %s is %s, want %s", what, name, g[name], sum)
		}
	}
}

// dirContains reports which of the needles appear in the bytes or the
// names of dir's files.
func dirContains(t *testing.T, dir string, needles []string) []string {
	t.Helper()
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	found := map[string]bool{}
	for _, e := range entries {
		raw, err := os.ReadFile(filepath.Join(dir, e.Name()))
		if err != nil {
			t.Fatal(err)
		}
		for _, n := range needles {
			if bytes.Contains(raw, []byte(n)) || strings.Contains(e.Name(), n) {
				found[n] = true
			}
		}
	}
	var out []string
	for _, n := range needles {
		if found[n] {
			out = append(out, n)
		}
	}
	return out
}

// TestSealedBackupRestore (issue #291): a sealed full backup and sealed
// incrementals on it — one under the same key, one under another —
// restore to exactly what an unsealed backup of the same data holds, by
// the summary's checksums, which are over the plaintext and so equal
// between the two; and nothing of the data, its keys, the table names or
// the cluster ID is in the sealed directories.
func TestSealedBackupRestore(t *testing.T) {
	tc := Start(t, 1)
	ctx, cancel := context.WithTimeout(context.Background(), 240*time.Second)
	defer cancel()
	n := tc.Nodes[0]
	s := sql.NewSession(n.DB(), catalog.NewAccessor())
	sealedBackupSchema(t, ctx, s)
	keyA, keyB := newBackupKey(t), newBackupKey(t)

	dirPlain, dirFull := t.TempDir(), t.TempDir()
	sumPlain, err := n.RunBackup(ctx, dirPlain, "", false, false)
	if err != nil {
		t.Fatal(err)
	}
	sumFull, err := n.RunBackupWith(ctx, server.BackupOptions{Dest: dirFull, Key: keyA})
	if err != nil {
		t.Fatalf("sealed full backup: %v", err)
	}
	if !sumFull.Sealed || sumPlain.Sealed {
		t.Fatalf("sealed flags: sealed backup %v, plain backup %v", sumFull.Sealed, sumPlain.Sealed)
	}
	requireSameSums(t, "sealed backup against an unsealed one of the same data", sumFull, sumPlain)
	for i, tb := range sumFull.Tables {
		if tb != sumPlain.Tables[i] {
			t.Fatalf("sealed backup reports %+v where the unsealed one reports %+v", tb, sumPlain.Tables[i])
		}
	}

	execSQL(t, ctx, s, `INSERT INTO patient_accounts VALUES ('acct-zebra-0003', 'holder-numbat')`)
	execSQL(t, ctx, s, `DELETE FROM patient_accounts WHERE handle = 'acct-zebra-0002'`)
	execSQL(t, ctx, s, `UPDATE ledger SET body = 'rewritten-okapi' WHERE id = 7`)

	// An incremental under another key names its base's key; one under
	// the base's own key need not.
	dirIncB, dirIncA := t.TempDir(), t.TempDir()
	if _, err := n.RunBackupWith(ctx, server.BackupOptions{Dest: dirIncB, BasePath: dirFull, Key: keyB}); err == nil || !strings.Contains(err.Error(), "does not open this backup") {
		t.Fatalf("incremental whose key does not open its sealed base: %v", err)
	}
	if _, err := os.Stat(filepath.Join(dirIncB, "BACKUP.json")); err == nil {
		t.Fatal("the refused incremental left a manifest")
	}
	if _, err := n.RunBackupWith(ctx, server.BackupOptions{Dest: dirIncB, BasePath: dirFull, Key: keyB, BaseKey: keyA}); err != nil {
		t.Fatalf("sealed incremental under a second key: %v", err)
	}
	if _, err := n.RunBackupWith(ctx, server.BackupOptions{Dest: dirIncA, BasePath: dirFull, Key: keyA}); err != nil {
		t.Fatalf("sealed incremental under the base's key: %v", err)
	}
	// An unsealed incremental on a sealed base still has to open the base.
	if _, err := n.RunBackupWith(ctx, server.BackupOptions{Dest: t.TempDir(), BasePath: dirFull}); err == nil || !strings.Contains(err.Error(), "pass its key with --base-key") {
		t.Fatalf("unsealed incremental on a sealed base, no key: %v", err)
	}
	// A base that is plaintext is refused when a key is named for it.
	if _, err := n.RunBackupWith(ctx, server.BackupOptions{Dest: t.TempDir(), BasePath: dirPlain, Key: keyA, BaseKey: keyA}); err == nil || !strings.Contains(err.Error(), "is not a sealed backup") {
		t.Fatalf("plaintext base with a base key: %v", err)
	}
	if _, err := n.RunBackupWith(ctx, server.BackupOptions{Dest: t.TempDir(), Key: keyA, AllowPlaintext: true}); err == nil || !strings.Contains(err.Error(), "pass one of them") {
		t.Fatalf("a key and --allow-plaintext together: %v", err)
	}
	if _, err := n.RunBackupWith(ctx, server.BackupOptions{Dest: t.TempDir(), Key: keyA[:16]}); err == nil || !strings.Contains(err.Error(), "must be 32 bytes") {
		t.Fatalf("a short key: %v", err)
	}
	dirRef := t.TempDir()
	sumRef, err := n.RunBackup(ctx, dirRef, "", false, false)
	if err != nil {
		t.Fatal(err)
	}

	// Nothing readable in the sealed directories; the same needles are
	// all in the unsealed ones, so the search is looking for the right
	// things.
	needles := []string{
		"patient_accounts", "ledger", "acct-zebra-0001", "holder-quokka",
		"acct-zebra-0003", "holder-numbat", "rewritten-okapi", sumPlain.ClusterID,
	}
	plainHas := map[string]bool{}
	for _, dir := range []string{dirPlain, dirRef} {
		for _, f := range dirContains(t, dir, needles) {
			plainHas[f] = true
		}
	}
	for _, needle := range needles {
		if !plainHas[needle] {
			t.Fatalf("%q is not in the unsealed backups either: the search proves nothing", needle)
		}
	}
	for _, dir := range []string{dirFull, dirIncA, dirIncB} {
		if found := dirContains(t, dir, needles); len(found) != 0 {
			t.Fatalf("sealed backup %s holds %q in the clear", dir, found)
		}
	}
	for _, k := range [][]byte{keyA, keyB} {
		for _, dir := range []string{dirFull, dirIncA, dirIncB} {
			if found := dirContains(t, dir, []string{string(k), hex.EncodeToString(k), base64.StdEncoding.EncodeToString(k)}); len(found) != 0 {
				t.Fatalf("sealed backup %s holds its backup key", dir)
			}
		}
	}

	// Two keys, one per directory; and one key for a whole chain.
	tc2 := Start(t, 1)
	rsum, err := tc2.Nodes[0].RunRestoreWith(ctx, []string{dirFull, dirIncB}, [][]byte{keyA, keyB})
	if err != nil {
		t.Fatalf("restore of a chain under two keys: %v", err)
	}
	requireSameSums(t, "restore of a chain under two keys", rsum, sumRef)
	s2 := sql.NewSession(tc2.Nodes[0].DB(), catalog.NewAccessor())
	if res := execSQL(t, ctx, s2, `SELECT holder FROM patient_accounts ORDER BY handle`); len(res.Rows) != 2 || res.Rows[0][0].S != "holder-quokka" || res.Rows[1][0].S != "holder-numbat" {
		t.Fatalf("restored rows: %+v", res.Rows)
	}
	if res := execSQL(t, ctx, s2, `SELECT body FROM ledger WHERE id = 7`); res.Rows[0][0].S != "rewritten-okapi" {
		t.Fatalf("restored update: %+v", res.Rows)
	}
	if r, err := catalog.LookupRole(ctx, tc2.Nodes[0].DB(), "auditor_marmot"); err != nil || r == nil {
		t.Fatalf("role not restored: %+v %v", r, err)
	}
	tc3 := Start(t, 1)
	rsum, err = tc3.Nodes[0].RunRestoreWith(ctx, []string{dirFull, dirIncA}, [][]byte{keyA})
	if err != nil {
		t.Fatalf("restore of a chain under one key: %v", err)
	}
	requireSameSums(t, "restore of a chain under one key", rsum, sumRef)

	// The keys are paired with the directories by position.
	tc4 := Start(t, 1)
	for _, c := range []struct {
		what string
		keys [][]byte
		want string
	}{
		{"keys in the wrong order", [][]byte{keyB, keyA}, "does not open this backup"},
		{"one key where the chain has two", [][]byte{keyA}, "does not open this backup"},
		{"three keys for two directories", [][]byte{keyA, keyB, keyA}, "3 backup keys for 2 backup directories"},
		{"an empty entry for a sealed directory", [][]byte{keyA, nil}, "is a sealed backup and no key was given"},
	} {
		if _, err := tc4.Nodes[0].RunRestoreWith(ctx, []string{dirFull, dirIncB}, c.keys); err == nil || !strings.Contains(err.Error(), c.want) {
			t.Fatalf("%s: %v", c.what, err)
		}
	}
	// An unsealed full backup with a sealed incremental on it: the empty
	// entry is how the chain says which is which.
	execSQL(t, ctx, s, `INSERT INTO patient_accounts VALUES ('acct-zebra-0004', 'holder-bilby')`)
	dirMixed := t.TempDir()
	if _, err := n.RunBackupWith(ctx, server.BackupOptions{Dest: dirMixed, BasePath: dirRef, Key: keyB}); err != nil {
		t.Fatalf("sealed incremental on an unsealed base: %v", err)
	}
	if _, err := tc4.Nodes[0].RunRestoreWith(ctx, []string{dirRef, dirMixed}, [][]byte{keyB}); err == nil || !strings.Contains(err.Error(), "is not a sealed backup") {
		t.Fatalf("one key for a chain that starts unsealed: %v", err)
	}
	if _, err := tc4.Nodes[0].RunRestoreWith(ctx, []string{dirRef, dirMixed}, [][]byte{nil, keyB}); err != nil {
		t.Fatalf("restore of an unsealed full backup and a sealed incremental: %v", err)
	}
	s4 := sql.NewSession(tc4.Nodes[0].DB(), catalog.NewAccessor())
	if res := execSQL(t, ctx, s4, `SELECT COUNT(*) FROM patient_accounts`); res.Rows[0][0].I != 3 {
		t.Fatalf("rows after the mixed chain: %+v", res.Rows)
	}
}

// TestSealedRestoreRefusesTampering (issue #291): every way a sealed
// backup can be other than what was written — or opened other than as
// what it is — is refused, and refused before anything is applied: the
// target stays empty through all of it, and takes the untouched backup
// afterwards.
func TestSealedRestoreRefusesTampering(t *testing.T) {
	tc := Start(t, 1)
	ctx, cancel := context.WithTimeout(context.Background(), 240*time.Second)
	defer cancel()
	n := tc.Nodes[0]
	s := sql.NewSession(n.DB(), catalog.NewAccessor())
	sealedBackupSchema(t, ctx, s)
	key, otherKey := newBackupKey(t), newBackupKey(t)

	sealed, other, plain := t.TempDir(), t.TempDir(), t.TempDir()
	sum, err := n.RunBackupWith(ctx, server.BackupOptions{Dest: sealed, Key: key})
	if err != nil {
		t.Fatal(err)
	}
	// A second backup of the same data under the same key: its manifest
	// names the same files and opens with the same key.
	if _, err := n.RunBackupWith(ctx, server.BackupOptions{Dest: other, Key: key}); err != nil {
		t.Fatal(err)
	}
	if _, err := n.RunBackup(ctx, plain, "", false, false); err != nil {
		t.Fatal(err)
	}
	file := map[string]string{}
	var tableIDs []uint64
	for _, tb := range sum.Tables {
		file[tb.Name] = fmt.Sprintf("table_%d.dxbk", tb.ID)
		tableIDs = append(tableIDs, tb.ID)
	}
	ledger, accounts := file["ledger"], file["patient_accounts"]
	if ledger == "" || accounts == "" {
		t.Fatalf("tables in the summary: %v", file)
	}
	// The sealed format's layout, for cutting at a chunk boundary: a
	// 32-byte header, then chunks of 64 KiB of plaintext with 24 bytes of
	// nonce and tag each.
	const header, chunk = 32, 64<<10 + 24
	if fi, err := os.Stat(filepath.Join(sealed, ledger)); err != nil || fi.Size() < header+3*chunk {
		t.Fatalf("the ledger's data file is too small to cut between chunks: %v %v", fi, err)
	}

	// crafted copies the sealed backup and lets the case change the copy.
	crafted := func(change func(dir string)) string {
		dir := t.TempDir()
		copyTree(t, sealed, dir)
		change(dir)
		return dir
	}
	rewrite := func(path string, change func([]byte) []byte) {
		raw, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, change(raw), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	copyFile := func(from, to string) {
		raw, err := os.ReadFile(from)
		if err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(to, raw, 0o644); err != nil {
			t.Fatal(err)
		}
	}
	truncateTo := func(name string, size int) func(string) {
		return func(dir string) {
			rewrite(filepath.Join(dir, name), func(b []byte) []byte { return b[:size] })
		}
	}
	const noAuth = "does not authenticate"
	cases := []struct {
		what string
		dir  string
		keys [][]byte
		want string
	}{
		{"a flipped byte in a data file", crafted(func(dir string) {
			rewrite(filepath.Join(dir, ledger), func(b []byte) []byte { b[len(b)/2] ^= 1; return b })
		}), [][]byte{key}, noAuth},
		{"a flipped byte in a data file's header", crafted(func(dir string) {
			rewrite(filepath.Join(dir, ledger), func(b []byte) []byte { b[20] ^= 1; return b })
		}), [][]byte{key}, noAuth},
		{"a flipped byte in a data file's last chunk", crafted(func(dir string) {
			rewrite(filepath.Join(dir, accounts), func(b []byte) []byte { b[len(b)-1] ^= 1; return b })
		}), [][]byte{key}, noAuth},
		{"a data file truncated inside a chunk", crafted(truncateTo(ledger, header+chunk+1000)), [][]byte{key}, noAuth},
		{"a data file truncated at a chunk boundary", crafted(truncateTo(ledger, header+chunk)), [][]byte{key}, noAuth},
		{"a data file truncated at its second chunk boundary", crafted(truncateTo(ledger, header+2*chunk)), [][]byte{key}, noAuth},
		{"a data file truncated to its header", crafted(truncateTo(ledger, header)), [][]byte{key}, noAuth},
		{"a data file truncated to nothing", crafted(truncateTo(ledger, 0)), [][]byte{key}, "reading the header"},
		{"a data file with a chunk appended", crafted(func(dir string) {
			rewrite(filepath.Join(dir, accounts), func(b []byte) []byte { return append(b, b[header:]...) })
		}), [][]byte{key}, noAuth},
		{"two data files swapped", crafted(func(dir string) {
			copyFile(filepath.Join(sealed, ledger), filepath.Join(dir, accounts))
			copyFile(filepath.Join(sealed, accounts), filepath.Join(dir, ledger))
		}), [][]byte{key}, noAuth},
		{"the manifest of another backup", crafted(func(dir string) {
			copyFile(filepath.Join(other, "BACKUP.json"), filepath.Join(dir, "BACKUP.json"))
		}), [][]byte{key}, noAuth},
		{"a data file of another backup", crafted(func(dir string) {
			copyFile(filepath.Join(other, ledger), filepath.Join(dir, ledger))
		}), [][]byte{key}, noAuth},
		{"an unsealed data file in a sealed backup", crafted(func(dir string) {
			copyFile(filepath.Join(plain, ledger), filepath.Join(dir, ledger))
		}), [][]byte{key}, "not a sealed stream"},
		{"an unsealed manifest in a sealed backup", crafted(func(dir string) {
			copyFile(filepath.Join(plain, "BACKUP.json"), filepath.Join(dir, "BACKUP.json"))
		}), [][]byte{key}, "is not a sealed backup"},
		{"a changed manifest", crafted(func(dir string) {
			rewrite(filepath.Join(dir, "BACKUP.json"), func(b []byte) []byte {
				// A base64 character inside the sealed manifest, swapped
				// for another: the envelope still parses.
				i := bytes.Index(b, []byte(`"manifest":"`)) + 200
				if b[i] == 'A' {
					b[i] = 'B'
				} else {
					b[i] = 'A'
				}
				return b
			})
		}), [][]byte{key}, "the sealed manifest does not open"},
		{"a wrong key", sealed, [][]byte{otherKey}, "does not open this backup"},
		{"no key for a sealed backup", sealed, nil, "is a sealed backup and no key was given"},
		{"a key for a plaintext backup", plain, [][]byte{key}, "is not a sealed backup"},
	}
	target := Start(t, 1)
	tn := target.Nodes[0]
	for _, c := range cases {
		_, err := tn.RunRestoreWith(ctx, []string{c.dir}, c.keys)
		if err == nil {
			t.Fatalf("restore accepted %s", c.what)
		}
		if !strings.Contains(err.Error(), c.want) {
			t.Fatalf("%s was refused for another reason: %v", c.what, err)
		}
		// Nothing applied: no table descriptor, no row, no role.
		descStart, descEnd := keys.TableDescSpan()
		if kvs, err := tn.DB().Scan(ctx, descStart, descEnd, 0); err != nil {
			t.Fatal(err)
		} else {
			for _, kv := range kvs {
				if bytes.Contains(kv.Value, []byte("ledger")) || bytes.Contains(kv.Value, []byte("patient_accounts")) {
					t.Fatalf("after refusing %s the target has a restored table descriptor", c.what)
				}
			}
		}
		for _, id := range tableIDs {
			start, end := keys.TableDataSpan(id)
			if kvs, err := tn.DB().Scan(ctx, start, end, 0); err != nil || len(kvs) != 0 {
				t.Fatalf("after refusing %s the target holds %d records of table %d (%v)", c.what, len(kvs), id, err)
			}
		}
		if r, err := catalog.LookupRole(ctx, tn.DB(), "auditor_marmot"); err != nil || r != nil {
			t.Fatalf("after refusing %s the target has a restored role: %+v %v", c.what, r, err)
		}
	}
	rsum, err := tn.RunRestoreWith(ctx, []string{sealed}, [][]byte{key})
	if err != nil {
		t.Fatalf("restore of the untouched sealed backup after the refusals: %v", err)
	}
	requireSameSums(t, "restore after the refusals", rsum, sum)
}

// TestBackupKeyOverTheAdminRPC (issue #291): the key reaches the serving
// node in the admin request — here over the insecure listener from the
// same machine, which is the one insecure path that takes it — and is in
// nothing the node writes about the request: not the audit record, not
// the log, not an error sent back.
func TestBackupKeyOverTheAdminRPC(t *testing.T) {
	// Everything the process logs while the test runs, audit records
	// included (log.Audit writes them to the log as well as to sinks).
	logFile, err := os.Create(filepath.Join(t.TempDir(), "stderr"))
	if err != nil {
		t.Fatal(err)
	}
	prevStderr := os.Stderr
	os.Stderr = logFile
	log.SetVerbose(false)
	restoreLog := func() {
		os.Stderr = prevStderr
		log.SetVerbose(false)
	}
	defer restoreLog()
	var auditMu sync.Mutex
	var audited []string
	remove := log.AddAuditSink(func(event string, kv []any) {
		auditMu.Lock()
		defer auditMu.Unlock()
		audited = append(audited, event+" "+fmt.Sprintln(kv...))
	})
	defer remove()

	tc := Start(t, 1)
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()
	s := sql.NewSession(tc.Nodes[0].DB(), catalog.NewAccessor())
	execSQL(t, ctx, s, `CREATE TABLE t (id INT8 PRIMARY KEY, v TEXT)`)
	execSQL(t, ctx, s, `INSERT INTO t VALUES (1, 'kept')`)
	key, wrongKey := newBackupKey(t), newBackupKey(t)

	trans := rpc.NewTransport(hlc.NewClock(nil, base.DefaultMaxClockOffset), nil, nil)
	var replies []string
	call := func(addr string, req cluster.AdminRequest) cluster.AdminResponse {
		t.Helper()
		var resp cluster.AdminResponse
		if err := trans.Call(ctx, addr, "admin", req, &resp); err != nil {
			t.Fatalf("admin %s: %v", req.Op, err)
		}
		replies = append(replies, fmt.Sprintf("%+v %+v", resp, resp.Backup))
		return resp
	}
	addr := tc.Nodes[0].Addr()
	full, inc := t.TempDir(), t.TempDir()
	resp := call(addr, cluster.AdminRequest{Op: "backup", Path: full, BackupKey: key})
	if resp.Error != "" || resp.Backup == nil || !resp.Backup.Sealed {
		t.Fatalf("sealed backup over the admin RPC: %q %+v", resp.Error, resp.Backup)
	}
	execSQL(t, ctx, s, `INSERT INTO t VALUES (2, 'later')`)
	if resp := call(addr, cluster.AdminRequest{Op: "backup", Path: inc, BasePath: full, BackupKey: wrongKey, BaseBackupKey: key}); resp.Error != "" {
		t.Fatalf("sealed incremental over the admin RPC: %s", resp.Error)
	}
	if resp := call(addr, cluster.AdminRequest{Op: "backup", Path: t.TempDir(), BasePath: full, BackupKey: wrongKey}); !strings.Contains(resp.Error, "does not open this backup") {
		t.Fatalf("incremental with the wrong base key: %q", resp.Error)
	}

	target := Start(t, 1)
	taddr := target.Nodes[0].Addr()
	if resp := call(taddr, cluster.AdminRequest{Op: "restore", Paths: []string{full, inc}, BackupKeys: [][]byte{wrongKey, key}}); !strings.Contains(resp.Error, "does not open this backup") {
		t.Fatalf("restore with the keys exchanged: %q", resp.Error)
	}
	if resp := call(taddr, cluster.AdminRequest{Op: "restore", Paths: []string{full, inc}}); !strings.Contains(resp.Error, "no key was given") {
		t.Fatalf("restore without keys: %q", resp.Error)
	}
	if resp := call(taddr, cluster.AdminRequest{Op: "restore", Paths: []string{full, inc}, BackupKeys: [][]byte{key, wrongKey}}); resp.Error != "" {
		t.Fatalf("restore over the admin RPC: %s", resp.Error)
	}
	s2 := sql.NewSession(target.Nodes[0].DB(), catalog.NewAccessor())
	if res := execSQL(t, ctx, s2, `SELECT v FROM t ORDER BY id`); len(res.Rows) != 2 || res.Rows[1][0].S != "later" {
		t.Fatalf("restored rows: %+v", res.Rows)
	}

	restoreLog()
	if err := logFile.Close(); err != nil {
		t.Fatal(err)
	}
	logged, err := os.ReadFile(logFile.Name())
	if err != nil {
		t.Fatal(err)
	}
	auditMu.Lock()
	audit := strings.Join(audited, "\n")
	auditMu.Unlock()
	// The requests were audited and logged, so there was something to
	// leak into.
	for _, want := range []string{"op backup", "op restore", "backup_key given", full} {
		if !strings.Contains(audit, want) {
			t.Fatalf("audit records lack %q:\n%s", want, audit)
		}
	}
	if !bytes.Contains(logged, []byte("admin-op")) || !bytes.Contains(logged, []byte("backup: table")) {
		t.Fatalf("the captured log holds neither the audit records nor the backup's own lines (%d bytes)", len(logged))
	}
	for _, k := range [][]byte{key, wrongKey} {
		forms := []string{
			string(k), hex.EncodeToString(k), strings.ToUpper(hex.EncodeToString(k)),
			base64.StdEncoding.EncodeToString(k), base64.RawStdEncoding.EncodeToString(k), base64.URLEncoding.EncodeToString(k),
			fmt.Sprint(k), fmt.Sprintf("%q", k),
		}
		for _, form := range forms {
			if strings.Contains(audit, form) {
				t.Fatal("a backup key is in the audit records")
			}
			if bytes.Contains(logged, []byte(form)) {
				t.Fatal("a backup key is in the log")
			}
			if strings.Contains(strings.Join(replies, "\n"), form) {
				t.Fatal("a backup key is in an admin reply")
			}
		}
	}
}

// TestSealedBackupFromAnEncryptedStore (issue #291): on an encrypted
// store a backup must say what it is — sealed, with a key, or unsealed,
// with --allow-plaintext — and a key answers the question.
func TestSealedBackupFromAnEncryptedStore(t *testing.T) {
	keyPath := filepath.Join(t.TempDir(), "store.key")
	if err := os.WriteFile(keyPath, newBackupKey(t), 0o600); err != nil {
		t.Fatal(err)
	}
	tc := &TestCluster{T: t}
	n := startDiskNode(t, t.TempDir(), true, "", func(cfg *server.Config) { cfg.EncKeyPath = keyPath })
	tc.Nodes = append(tc.Nodes, n)
	defer tc.StopAll()
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()
	s := sql.NewSession(n.DB(), catalog.NewAccessor())
	execSQL(t, ctx, s, `CREATE TABLE t (id INT8 PRIMARY KEY, v TEXT)`)
	execSQL(t, ctx, s, `INSERT INTO t VALUES (1, 'canary-pangolin')`)

	if _, err := n.RunBackupWith(ctx, server.BackupOptions{Dest: t.TempDir()}); err == nil || !strings.Contains(err.Error(), "--backup-key") || !strings.Contains(err.Error(), "--allow-plaintext") {
		t.Fatalf("backup of an encrypted store with neither flag: %v", err)
	}
	sealed, plain := t.TempDir(), t.TempDir()
	sum, err := n.RunBackupWith(ctx, server.BackupOptions{Dest: sealed, Key: newBackupKey(t)})
	if err != nil || !sum.Sealed {
		t.Fatalf("sealed backup of an encrypted store: %+v %v", sum, err)
	}
	if found := dirContains(t, sealed, []string{"canary-pangolin"}); len(found) != 0 {
		t.Fatal("the sealed backup holds the row in the clear")
	}
	if sum, err := n.RunBackupWith(ctx, server.BackupOptions{Dest: plain, AllowPlaintext: true}); err != nil || sum.Sealed {
		t.Fatalf("unsealed backup with --allow-plaintext: %+v %v", sum, err)
	}
	if found := dirContains(t, plain, []string{"canary-pangolin"}); len(found) != 1 {
		t.Fatal("the unsealed backup does not hold the row: the search proves nothing")
	}
}
