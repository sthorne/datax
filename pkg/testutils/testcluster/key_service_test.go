package testcluster

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/sthorne/datax/pkg/server"
)

// keyLease is the key cache lease the key-service cluster runs with: short,
// so the statements that wait it out (DROP, DISABLE, REVOKE) stay cheap.
const keyLease = time.Second

// ksConn opens a SQL connection to node i as user, over TLS, with the
// extended protocol: data keys travel as bytea parameters, as an
// application should send them.
func ksConn(t *testing.T, ctx context.Context, c *ckCluster, i int, user, password string) *pgx.Conn {
	t.Helper()
	url := fmt.Sprintf("postgres://%s:%s@%s/datax?sslmode=verify-ca&sslrootcert=%s",
		user, password, c.tc.Nodes[i].SQLAddr(), filepath.Join(c.certsDir, "ca.crt"))
	deadline := time.Now().Add(30 * time.Second)
	for {
		conn, err := pgx.Connect(ctx, url)
		if err == nil {
			t.Cleanup(func() { _ = conn.Close(context.Background()) })
			return conn
		}
		if time.Now().After(deadline) {
			t.Fatalf("%s could not connect to n%d: %v", user, i+1, err)
		}
		time.Sleep(200 * time.Millisecond)
	}
}

func wantState(t *testing.T, what string, err error, code, contains string) {
	t.Helper()
	if sqlState(err) != code || !strings.Contains(fmt.Sprint(err), contains) {
		t.Fatalf("%s: %v, want SQLSTATE %s containing %q", what, err, code, contains)
	}
}

func randBytes(n int) []byte {
	b := make([]byte, n)
	_, _ = rand.Read(b)
	return b
}

func wrapWith(ctx context.Context, conn *pgx.Conn, key string, data, kctx []byte) ([]byte, error) {
	var out []byte
	err := conn.QueryRow(ctx, `SELECT datax_key_wrap($1, $2, $3)`, key, data, kctx).Scan(&out)
	return out, err
}

func unwrapWith(ctx context.Context, conn *pgx.Conn, key string, blob, kctx []byte) ([]byte, error) {
	var out []byte
	err := conn.QueryRow(ctx, `SELECT datax_key_unwrap($1, $2, $3)`, key, blob, kctx).Scan(&out)
	return out, err
}

// TestKeyService (issue #310, step 2): the key service end to end on one
// secure cluster — refused before the cluster key is enabled; wrap on one
// node and unwrap on another; WRAP, UNWRAP and USAGE held apart; context,
// key and blob binding; encrypt and decrypt with their cap; rotation
// keeping old blobs; REVOKE and DISABLE effective on every gateway when
// the statement returns; the rate limit; the audit records, with no
// material in them; ownership and DROP OWNED, REASSIGN OWNED, DROP ROLE;
// and DROP making every node refuse.
func TestKeyService(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Second)
	defer cancel()
	c := startCKCluster(t, ctx, func(i int, cfg *server.Config) {
		cfg.KeyCacheLease = keyLease
		cfg.AuditFlushInterval = 50 * time.Millisecond
	})
	c.waitReported(3)
	root := ksConn(t, ctx, c, 0, "root", "topsecret")

	t.Run("refused before the cluster key", func(t *testing.T) {
		_, err := root.Exec(ctx, `CREATE ENCRYPTION KEY early`)
		wantState(t, "CREATE before the cluster key", err, "55000", "cluster-key init")
		_, err = wrapWith(ctx, root, "early", []byte("k"), []byte("c"))
		wantState(t, "wrap before the cluster key", err, "55000", "cluster-key init")
	})

	if errText, _ := c.op(0, "init"); errText != "" {
		t.Fatalf("init: %s", errText)
	}
	c.waitFor("every node to hold the cluster key", 30*time.Second, func() bool {
		st := c.status(0)
		return holdsCurrent(st, 1) && holdsCurrent(st, 2) && holdsCurrent(st, 3)
	})

	for _, u := range []string{"app", "ingest", "reader", "nobody", "tmp", "keeper"} {
		mustExec(t, ctx, root, fmt.Sprintf(`CREATE USER %s WITH PASSWORD 'pw-%s'`, u, u))
	}
	mustExec(t, ctx, root, `CREATE ENCRYPTION KEY docs`)
	mustExec(t, ctx, root, `CREATE ENCRYPTION KEY other`)
	mustExec(t, ctx, root, `CREATE ENCRYPTION KEY IF NOT EXISTS docs`)
	_, err := root.Exec(ctx, `CREATE ENCRYPTION KEY docs`)
	wantState(t, "a second key of the same name", err, "42710", "already exists")
	mustExec(t, ctx, root, `GRANT WRAP ON ENCRYPTION KEY docs TO ingest`)
	mustExec(t, ctx, root, `GRANT UNWRAP ON ENCRYPTION KEY docs TO reader`)
	mustExec(t, ctx, root, `GRANT USAGE ON ENCRYPTION KEY docs, other TO app`)

	conn := func(i int, user string) *pgx.Conn { return ksConn(t, ctx, c, i, user, "pw-"+user) }
	ingest1, reader2, app3, app1 := conn(0, "ingest"), conn(1, "reader"), conn(2, "app"), conn(0, "app")
	tenant := []byte("tenant:4f6c")
	dataKey := randBytes(32)

	var blob []byte
	t.Run("wrap on one node, unwrap on another", func(t *testing.T) {
		var err error
		if blob, err = wrapWith(ctx, ingest1, "docs", dataKey, tenant); err != nil {
			t.Fatal(err)
		}
		if len(blob) != 32+41 || blob[0] != 0x01 || blob[12] != 1 || bytes.Contains(blob, dataKey) {
			t.Fatalf("blob %x", blob)
		}
		got, err := unwrapWith(ctx, reader2, "docs", blob, tenant)
		if err != nil || !bytes.Equal(got, dataKey) {
			t.Fatalf("unwrap on n2: %x %v", got, err)
		}
		if got, err := unwrapWith(ctx, app3, "docs", blob, tenant); err != nil || !bytes.Equal(got, dataKey) {
			t.Fatalf("unwrap with USAGE on n3: %x %v", got, err)
		}
		// The simple protocol's literals work too.
		var lit []byte
		if err := app1.QueryRow(ctx, fmt.Sprintf(`SELECT datax_key_unwrap('docs', '\x%s', 'tenant:4f6c')`, hex.EncodeToString(blob)), pgx.QueryExecModeSimpleProtocol).Scan(&lit); err != nil || !bytes.Equal(lit, dataKey) {
			t.Fatalf("unwrap with literals: %x %v", lit, err)
		}
	})

	t.Run("WRAP and UNWRAP are apart", func(t *testing.T) {
		_, err := unwrapWith(ctx, ingest1, "docs", blob, tenant)
		wantState(t, "WRAP-only role unwrapping", err, "42501", "needs UNWRAP")
		_, err = wrapWith(ctx, reader2, "docs", dataKey, tenant)
		wantState(t, "UNWRAP-only role wrapping", err, "42501", "needs WRAP")
		_, err = wrapWith(ctx, conn(1, "nobody"), "docs", dataKey, tenant)
		wantState(t, "no privilege", err, "42501", "permission denied")
		_, err = wrapWith(ctx, app1, "nosuch", dataKey, tenant)
		wantState(t, "unknown key", err, "42704", "does not exist")
	})

	t.Run("binding", func(t *testing.T) {
		_, err := unwrapWith(ctx, app1, "docs", blob, []byte("tenant:4f6d"))
		wantState(t, "another context", err, "39000", "does not open")
		_, err = unwrapWith(ctx, app1, "other", blob, tenant)
		wantState(t, "another key", err, "39000", "does not open")
		bad := append([]byte(nil), blob...)
		bad[len(bad)-3] ^= 0x40
		_, err = unwrapWith(ctx, app1, "docs", bad, tenant)
		wantState(t, "a damaged blob", err, "39000", "does not open")
		_, err = unwrapWith(ctx, app1, "docs", []byte("short"), tenant)
		wantState(t, "not a blob", err, "39000", "does not open")
	})

	t.Run("encrypt and decrypt", func(t *testing.T) {
		secret := []byte("api-token-123")
		var ct, pt []byte
		if err := app1.QueryRow(ctx, `SELECT datax_encrypt('docs', $1, $2)`, secret, tenant).Scan(&ct); err != nil {
			t.Fatal(err)
		}
		if err := app3.QueryRow(ctx, `SELECT datax_decrypt('docs', $1, $2)`, ct, tenant).Scan(&pt); err != nil || !bytes.Equal(pt, secret) {
			t.Fatalf("decrypt: %q %v", pt, err)
		}
		err := app1.QueryRow(ctx, `SELECT datax_decrypt('docs', $1, $2)`, blob, tenant).Scan(&pt)
		wantState(t, "a wrapped key given to decrypt", err, "39000", "does not open")
		err = app1.QueryRow(ctx, `SELECT datax_encrypt('docs', $1, $2)`, make([]byte, 64<<10+1), tenant).Scan(&ct)
		wantState(t, "over the cap", err, "54000", "at most 65536 bytes")
		if err := app1.QueryRow(ctx, `SELECT datax_encrypt('docs', $1, $2)`, make([]byte, 64<<10), tenant).Scan(&ct); err != nil {
			t.Fatalf("at the cap: %v", err)
		}
		var isNull *[]byte
		if err := app1.QueryRow(ctx, `SELECT datax_encrypt('docs', NULL, $1)`, tenant).Scan(&isNull); err != nil || isNull != nil {
			t.Fatalf("a NULL argument: %v %v", isNull, err)
		}
		mustExec(t, ctx, root, `CREATE TABLE secrets (id INT PRIMARY KEY, v BYTEA)`)
		mustExec(t, ctx, root, `GRANT ALL ON TABLE secrets TO app`)
		mustExec(t, ctx, app1, `INSERT INTO secrets VALUES (1, datax_encrypt('docs', $1, 'row-1'))`, secret)
		_, err = app1.Exec(ctx, `SELECT datax_decrypt('docs', v, 'row-1') FROM secrets`)
		wantState(t, "a column argument", err, "0A000", "constants or parameters")
	})

	t.Run("rotation keeps old blobs", func(t *testing.T) {
		mustExec(t, ctx, root, `ALTER ENCRYPTION KEY docs ROTATE`)
		time.Sleep(keyLease + 200*time.Millisecond) // other gateways move to v2 within a lease
		nb, err := wrapWith(ctx, app3, "docs", dataKey, tenant)
		if err != nil || nb[12] != 2 {
			t.Fatalf("a wrap after rotation: %x %v", nb, err)
		}
		for _, b := range [][]byte{blob, nb} {
			if got, err := unwrapWith(ctx, reader2, "docs", b, tenant); err != nil || !bytes.Equal(got, dataKey) {
				t.Fatalf("version %d blob after rotation: %v", b[12], err)
			}
		}
	})

	t.Run("REVOKE is effective when it returns", func(t *testing.T) {
		mustExec(t, ctx, root, `GRANT UNWRAP ON ENCRYPTION KEY docs TO tmp`)
		tmp2 := conn(1, "tmp")
		if _, err := unwrapWith(ctx, tmp2, "docs", blob, tenant); err != nil { // n2 caches the key
			t.Fatal(err)
		}
		start := time.Now()
		mustExec(t, ctx, root, `REVOKE UNWRAP ON ENCRYPTION KEY docs FROM tmp`)
		if d := time.Since(start); d < keyLease {
			t.Fatalf("REVOKE returned after %s, inside the lease", d)
		}
		_, err := unwrapWith(ctx, tmp2, "docs", blob, tenant)
		wantState(t, "unwrap after REVOKE", err, "42501", "permission denied")
		// A grant is seen at once: a refusal re-reads the key first.
		mustExec(t, ctx, root, `GRANT USAGE ON ENCRYPTION KEY docs TO tmp`)
		if _, err := unwrapWith(ctx, tmp2, "docs", blob, tenant); err != nil {
			t.Fatalf("unwrap right after a GRANT: %v", err)
		}
	})

	t.Run("DISABLE is effective when it returns", func(t *testing.T) {
		if _, err := unwrapWith(ctx, app3, "docs", blob, tenant); err != nil { // n3 caches the key
			t.Fatal(err)
		}
		start := time.Now()
		mustExec(t, ctx, root, `ALTER ENCRYPTION KEY docs DISABLE`)
		if d := time.Since(start); d < keyLease {
			t.Fatalf("DISABLE returned after %s, inside the lease", d)
		}
		_, err := unwrapWith(ctx, app3, "docs", blob, tenant)
		wantState(t, "unwrap of a disabled key", err, "55000", "disabled")
		_, err = wrapWith(ctx, app1, "docs", dataKey, tenant)
		wantState(t, "wrap under a disabled key", err, "55000", "disabled")
		mustExec(t, ctx, root, `ALTER ENCRYPTION KEY docs ENABLE`)
		if _, err := unwrapWith(ctx, app3, "docs", blob, tenant); err != nil {
			t.Fatalf("unwrap right after ENABLE: %v", err)
		}
	})

	t.Run("rate limit", func(t *testing.T) {
		mustExec(t, ctx, root, `SET CLUSTER SETTING key_service.rate_limit = 5`)
		var v string
		if err := root.QueryRow(ctx, `SHOW CLUSTER SETTING key_service.rate_limit`).Scan(&v); err != nil || v != "5" {
			t.Fatalf("SHOW CLUSTER SETTING: %q %v", v, err)
		}
		_, err := root.Exec(ctx, `SET CLUSTER SETTING key_service.rate_limit = -1`)
		wantState(t, "a negative rate", err, "22023", "whole number")
		_, err = app1.Exec(ctx, `SET CLUSTER SETTING key_service.rate_limit = 0`)
		wantState(t, "a setting by a non-admin", err, "42501", "not an admin")
		time.Sleep(keyLease + 200*time.Millisecond) // the gateways read the setting once a lease
		refused := 0
		for i := 0; i < 30; i++ {
			if _, err := unwrapWith(ctx, reader2, "docs", blob, tenant); err != nil {
				wantState(t, "past the rate", err, "53400", "rate limit")
				refused++
			}
		}
		if refused < 20 {
			t.Fatalf("%d of 30 calls refused at 5 a second", refused)
		}
		// Another role has a bucket of its own.
		if _, err := unwrapWith(ctx, conn(1, "app"), "docs", blob, tenant); err != nil {
			t.Fatalf("another role while reader is throttled: %v", err)
		}
		mustExec(t, ctx, root, `SET CLUSTER SETTING key_service.rate_limit = DEFAULT`)
		time.Sleep(keyLease + 200*time.Millisecond)
	})

	t.Run("audit", func(t *testing.T) {
		var rows [][]string
		c.waitFor("key-use audit records", 20*time.Second, func() bool {
			r, err := root.Query(ctx, `SELECT event, principal, target, outcome, detail FROM datax_audit WHERE event IN ('key-use', 'key-use-throttled', 'key-ddl')`)
			if err != nil {
				t.Fatal(err)
			}
			rows = nil
			for r.Next() {
				var e, p, tg, o, d string
				if err := r.Scan(&e, &p, &tg, &o, &d); err != nil {
					t.Fatal(err)
				}
				rows = append(rows, []string{e, p, tg, o, d})
			}
			r.Close()
			seen := map[string]bool{}
			for _, row := range rows {
				seen[row[0]+"/"+row[3]] = true
			}
			return seen["key-use/ok"] && seen["key-use/denied"] && seen["key-use/failed"] && seen["key-use/disabled"] && seen["key-use-throttled/throttled"] && seen["key-ddl/ok"]
		})
		var sawReader bool
		for _, row := range rows {
			all := strings.Join(row, " ")
			for _, secret := range [][]byte{dataKey, blob, tenant} {
				if strings.Contains(all, hex.EncodeToString(secret)) || strings.Contains(all, string(secret)) {
					t.Fatalf("an audit record holds material, a blob or a context: %v", row)
				}
			}
			if row[0] == "key-use" && row[1] == "reader" && row[2] == "docs" && row[3] == "ok" &&
				strings.Contains(row[4], `"op":"unwrap"`) && strings.Contains(row[4], `"key_id":`) && strings.Contains(row[4], `"version":`) {
				sawReader = true
			}
		}
		if !sawReader {
			t.Fatalf("no record of reader's unwrap with key id and version: %v", rows)
		}
	})

	t.Run("catalog", func(t *testing.T) {
		r, err := root.Query(ctx, `SELECT name, state, version, owner, privileges FROM datax_keys WHERE name = 'docs'`)
		if err != nil {
			t.Fatal(err)
		}
		var n int
		for r.Next() {
			var name, state, owner string
			var version int64
			var privs *string
			if err := r.Scan(&name, &state, &version, &owner, &privs); err != nil {
				t.Fatal(err)
			}
			if state != "enabled" || version != 2 || owner != "root" || privs == nil || !strings.Contains(*privs, "ingest=WRAP") || !strings.Contains(*privs, "app=WRAP,UNWRAP") {
				t.Fatalf("datax_keys row: %s %s %d %s %v", name, state, version, owner, privs)
			}
			n++
		}
		r.Close()
		if n != 1 {
			t.Fatalf("%d rows for docs", n)
		}
		if _, err := root.Exec(ctx, `SELECT material FROM datax_keys`); err == nil {
			t.Fatal("datax_keys has a material column")
		}
		// A non-admin is shown the keys it can use, and nothing more.
		r, err = conn(0, "ingest").Query(ctx, `SHOW ENCRYPTION KEYS`)
		if err != nil {
			t.Fatal(err)
		}
		var names []string
		for r.Next() {
			vals, _ := r.Values()
			names = append(names, fmt.Sprint(vals[0]))
		}
		r.Close()
		if strings.Join(names, ",") != "docs" {
			t.Fatalf("SHOW ENCRYPTION KEYS for ingest: %v", names)
		}
		_, err = root.Exec(ctx, `BEGIN; ALTER ENCRYPTION KEY docs ROTATE; COMMIT`)
		if err == nil || !strings.Contains(err.Error(), "transaction block") {
			t.Fatalf("a key statement in a transaction block: %v", err)
		}
		mustExec(t, ctx, root, `ROLLBACK`)
	})

	t.Run("ownership", func(t *testing.T) {
		mustExec(t, ctx, root, `CREATE ENCRYPTION KEY kept`)
		_, err := conn(0, "keeper").Exec(ctx, `CREATE ENCRYPTION KEY mine`)
		wantState(t, "CREATE by a non-admin", err, "42501", "admin role")
		_, err = conn(0, "keeper").Exec(ctx, `ALTER ENCRYPTION KEY kept ROTATE`)
		wantState(t, "ROTATE by a non-owner", err, "42501", "must be owner")
		mustExec(t, ctx, root, `ALTER ENCRYPTION KEY kept OWNER TO keeper`)
		keeper := conn(0, "keeper")
		mustExec(t, ctx, keeper, `ALTER ENCRYPTION KEY kept ROTATE`)
		mustExec(t, ctx, keeper, `GRANT WRAP ON ENCRYPTION KEY kept TO tmp`)
		if _, err := wrapWith(ctx, keeper, "kept", dataKey, []byte("c")); err != nil { // the owner uses its key
			t.Fatalf("the owner wrapping: %v", err)
		}
		_, err = root.Exec(ctx, `DROP ROLE keeper`)
		wantState(t, "DROP ROLE of a key's owner", err, "2BP01", "owner of encryption key kept")
		_, err = root.Exec(ctx, `DROP OWNED BY keeper`)
		wantState(t, "DROP OWNED of a key's owner", err, "2BP01", "owns encryption keys kept")
		mustExec(t, ctx, root, `REASSIGN OWNED BY keeper TO root`)
		mustExec(t, ctx, root, `DROP ROLE keeper`)
		// tmp holds grants on docs and kept: DROP OWNED revokes them,
		// and DROP ROLE goes through.
		tmp1 := conn(0, "tmp")
		mustExec(t, ctx, root, `DROP OWNED BY tmp`)
		_, err = unwrapWith(ctx, tmp1, "docs", blob, tenant)
		wantState(t, "unwrap after DROP OWNED", err, "42501", "permission denied")
		_ = tmp1.Close(ctx)
		mustExec(t, ctx, root, `GRANT USAGE ON ENCRYPTION KEY docs TO tmp`)
		mustExec(t, ctx, root, `DROP ROLE tmp`)
		var privs string
		if err := root.QueryRow(ctx, `SELECT privileges FROM datax_keys WHERE name = 'docs'`).Scan(&privs); err != nil || strings.Contains(privs, "tmp") {
			t.Fatalf("docs's privileges after DROP ROLE tmp: %q %v", privs, err)
		}
	})

	t.Run("DROP refuses on every node", func(t *testing.T) {
		app2 := conn(1, "app")
		for _, cn := range []*pgx.Conn{app1, app2, app3} {
			if _, err := unwrapWith(ctx, cn, "docs", blob, tenant); err != nil { // every gateway caches it
				t.Fatal(err)
			}
		}
		start := time.Now()
		mustExec(t, ctx, root, `DROP ENCRYPTION KEY docs`)
		if d := time.Since(start); d < keyLease {
			t.Fatalf("DROP returned after %s, inside the lease", d)
		}
		for i, cn := range []*pgx.Conn{app1, app2, app3} {
			_, err := unwrapWith(ctx, cn, "docs", blob, tenant)
			wantState(t, fmt.Sprintf("unwrap on n%d after DROP", i+1), err, "55000", "destroyed")
		}
		mustExec(t, ctx, root, `DROP ENCRYPTION KEY IF EXISTS nosuch`)
		_, err := root.Exec(ctx, `CREATE ENCRYPTION KEY docs`)
		wantState(t, "reusing a dropped key's name", err, "42710", "keeps its name")
	})
}
