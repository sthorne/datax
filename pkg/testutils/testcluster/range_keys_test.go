package testcluster

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/sthorne/datax/pkg/server"
)

// TestRangeKeysAreShownPerPrivilege (issue #213): a range boundary is a
// row key, and the range lists and split events rendered it back into
// its value for any authenticated user. Two tables, each split at a
// literal row value; a user who may read one of them sees that table's
// name and boundary value on every document, and neither for the other
// — where an admin sees both — and the user's table list on
// /api/cluster agrees with the same user's /api/schema.
func TestRangeKeysAreShownPerPrivilege(t *testing.T) {
	// Every node serves HTTP: a split is recorded on the node that ran
	// it, and the event check below looks wherever that was.
	var httpLis [3]net.Listener
	for i := range httpLis {
		lis, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		httpLis[i] = lis
	}
	tc, certsDir := startSecureCluster(t, "topsecret", func(i int, cfg *server.Config) {
		cfg.HTTPListener = httpLis[i]
	})
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()
	root := waitForRoot(t, ctx, tc, certsDir)
	defer root.Close(ctx)
	const openRow, sealedRow = "open-row-value", "sealed-row-value"
	for _, stmt := range []string{
		`CREATE TABLE openbook (id TEXT PRIMARY KEY, v INT8)`,
		`CREATE TABLE sealedbook (id TEXT PRIMARY KEY, v INT8)`,
		`INSERT INTO openbook VALUES ('` + openRow + `', 1), ('zz', 2)`,
		`INSERT INTO sealedbook VALUES ('` + sealedRow + `', 1), ('zz', 2)`,
		`ALTER TABLE openbook SPLIT AT VALUES ('` + openRow + `')`,
		`ALTER TABLE sealedbook SPLIT AT VALUES ('` + sealedRow + `')`,
		`CREATE USER reader PASSWORD 'reader-pw'`,
		`GRANT SELECT ON openbook TO reader`,
	} {
		if _, err := root.Exec(ctx, stmt); err != nil {
			t.Fatalf("%s: %v", stmt, err)
		}
	}

	client := httpsClient(t, certsDir, "")
	var bases []string
	for _, n := range tc.Nodes {
		bases = append(bases, "https://"+n.HTTPAddr())
	}
	get := func(url, user, pass string) string {
		t.Helper()
		code, body, _ := authedGet(t, client, url, user, pass)
		if code != http.StatusOK {
			t.Fatalf("%s as %s: %d (%s)", url, user, code, body)
		}
		return body
	}

	// Wait until every document the test reads carries, for the admin,
	// both boundaries and both tables' names. The pieces land at
	// different moments: the schema cache's name map fills on its own
	// refresh, and /status lists the node's own replicas, whose
	// descriptors take the split a beat after the meta range that
	// /api/cluster reads — under a loaded suite either has been seen to
	// arrive a poll after the other. The test is about what each reader
	// is shown of a document, not about when the document is complete.
	paths := []string{"/api/cluster", "/status", "/api/overview", "/api/node"}
	wants := []string{sealedRow, openRow, `"sealedbook"`, `"openbook"`}
	deadline := time.Now().Add(30 * time.Second)
	for {
		complete := true
		for _, path := range paths {
			body := get(bases[0]+path, "root", "topsecret")
			for _, want := range wants {
				if !strings.Contains(body, want) {
					complete = false
				}
			}
		}
		if complete {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("the admin's documents never all rendered the split boundaries and the table names")
		}
		time.Sleep(200 * time.Millisecond)
	}

	// The documents that list ranges, as the admin and as the reader.
	for _, path := range paths {
		asRoot := get(bases[0]+path, "root", "topsecret")
		for _, want := range wants {
			if !strings.Contains(asRoot, want) {
				t.Errorf("%s as root does not carry %s", path, want)
			}
		}
		asReader := get(bases[0]+path, "reader", "reader-pw")
		for _, leak := range []string{sealedRow, "sealedbook"} {
			if strings.Contains(asReader, leak) {
				t.Errorf("%s as reader carries %q, a table the reader has no privilege on:\n%s", path, leak, asReader)
			}
		}
		for _, want := range []string{openRow, `"openbook"`} {
			if !strings.Contains(asReader, want) {
				t.Errorf("%s as reader does not carry %s, from the table the reader may read", path, want)
			}
		}
	}

	// The reader's range list names no table its schema does not.
	var cluster server.ClusterStatus
	if err := json.Unmarshal([]byte(get(bases[0]+"/api/cluster", "reader", "reader-pw")), &cluster); err != nil {
		t.Fatal(err)
	}
	// And the document says its keys are the shown form, so the console
	// does not copy one under a key's name (QA on the combined PR: the
	// flag has to be seen set, not only declared); the admin's says the
	// opposite.
	if !cluster.KeysRedacted {
		t.Error("the reader's /api/cluster does not set keys_redacted")
	}
	var rootCluster server.ClusterStatus
	if err := json.Unmarshal([]byte(get(bases[0]+"/api/cluster", "root", "topsecret")), &rootCluster); err != nil {
		t.Fatal(err)
	}
	if rootCluster.KeysRedacted {
		t.Error("root's /api/cluster sets keys_redacted")
	}
	var schema server.SchemaStatus
	if err := json.Unmarshal([]byte(get(bases[0]+"/api/schema", "reader", "reader-pw")), &schema); err != nil {
		t.Fatal(err)
	}
	visible := map[string]bool{}
	for _, tb := range schema.Tables {
		visible[tb.Name] = true
	}
	if !visible["openbook"] || visible["sealedbook"] {
		t.Fatalf("reader's schema: %v", visible)
	}
	type labelled struct{ table, start string }
	var ranges []labelled
	for _, r := range cluster.Ranges {
		ranges = append(ranges, labelled{r.Table, r.StartKey})
	}
	for _, r := range cluster.Local.Ranges {
		ranges = append(ranges, labelled{r.Table, r.StartKey})
	}
	var sealedPrefix string
	for _, r := range ranges {
		if r.table != "" && !visible[r.table] {
			t.Errorf("reader's /api/cluster names %q, which the reader's /api/schema does not", r.table)
		}
		if r.table == "" && strings.HasPrefix(r.start, "/table/") {
			sealedPrefix = r.start
		}
	}
	// A hidden table's range still says which table and index it is,
	// by id — the operational meaning without the data.
	if sealedPrefix == "" || strings.Count(sealedPrefix, "/") < 2 || strings.Contains(sealedPrefix, `"`) {
		t.Errorf("no hidden range rendered at its table-and-index prefix (got %q)", sealedPrefix)
	}

	// The split events, on whichever node recorded them: the admin sees
	// the boundary value, the reader sees the same event at the prefix —
	// and still sees the value for the table it may read.
	var sealedSeen, openSeen bool
	for i, base := range bases {
		// The documents that embed events, on every node, as the reader.
		for _, path := range []string{"/api/overview", "/api/node", "/status", "/api/cluster"} {
			if body := get(base+path, "reader", "reader-pw"); strings.Contains(body, sealedRow) || strings.Contains(body, "sealedbook") {
				t.Errorf("n%d %s as reader carries the sealed table:\n%s", i+1, path, body)
			}
		}
		var asRoot, asReader server.EventsStatus
		if err := json.Unmarshal([]byte(get(base+"/api/events", "root", "topsecret")), &asRoot); err != nil {
			t.Fatal(err)
		}
		if err := json.Unmarshal([]byte(get(base+"/api/events", "reader", "reader-pw")), &asReader); err != nil {
			t.Fatal(err)
		}
		rootSplits := 0
		for _, ev := range asRoot.Events {
			if ev.Kind == "split" && strings.Contains(ev.Summary, sealedRow) {
				rootSplits++
			}
		}
		readerSplits := 0
		for _, ev := range asReader.Events {
			if strings.Contains(ev.Summary, sealedRow) || strings.Contains(ev.Summary, "sealedbook") {
				t.Errorf("n%d /api/events as reader: %q carries the sealed table", i+1, ev.Summary)
			}
			if ev.Kind == "split" && strings.Contains(ev.Summary, "/table/") && !strings.Contains(ev.Summary, `"`) {
				readerSplits++
			}
			if strings.Contains(ev.Summary, openRow) {
				openSeen = true
			}
		}
		if rootSplits > 0 {
			sealedSeen = true
			if readerSplits == 0 {
				t.Errorf("n%d recorded the sealed split for the admin but the reader sees no split at a prefix:\n%s", i+1, fmt.Sprint(asReader.Events))
			}
		}
	}
	if !sealedSeen {
		t.Error("no node's /api/events carries the sealed split for the admin: the test is not proving what it claims")
	}
	if !openSeen {
		t.Error("the reader sees no split event carrying the readable table's boundary value")
	}
}
