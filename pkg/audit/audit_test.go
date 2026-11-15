package audit

import (
	"bytes"
	"encoding/json"
	"fmt"
	"strings"
	"testing"
)

// chain builds n records for node, chained from prev at seq from+1.
func chain(node, from int64, prev string, n int) []Record {
	out := make([]Record, 0, n)
	for i := 0; i < n; i++ {
		r := Record{Node: node, Seq: from + int64(i) + 1, AtMicros: 1_700_000_000_000_000 + int64(i), Event: "role-ddl",
			Principal: "root", Target: fmt.Sprintf("role%d", i), Outcome: "ok", Detail: `{"stmt":"CREATE ROLE"}`, PrevHash: prev}
		r.Hash = r.ComputeHash()
		prev = r.Hash
		out = append(out, r)
	}
	return out
}

// appendEvent chains one record of the given event and detail on.
func appendEvent(recs []Record, event string, detail any) []Record {
	last := recs[len(recs)-1]
	d, _ := json.Marshal(detail)
	r := Record{Node: last.Node, Seq: last.Seq + 1, AtMicros: last.AtMicros + 1, Event: event, Principal: "node:1", Outcome: "ok", Detail: string(d), PrevHash: last.Hash}
	r.Hash = r.ComputeHash()
	return append(recs, r)
}

func problems(rep *Report) string {
	var all []string
	for _, c := range rep.Chains {
		all = append(all, c.Problems...)
	}
	return strings.Join(append(all, rep.Problems...), "\n")
}

func clone(recs []Record) []Record { return append([]Record(nil), recs...) }

// TestVerify (issue #293): an intact chain verifies, in any order; a
// record removed, altered, or exchanged with its neighbour does not,
// and the finding says which.
func TestVerify(t *testing.T) {
	recs := chain(1, 0, GenesisHash, 8)
	recs = append(recs, chain(2, 0, GenesisHash, 3)...)
	if rep := Verify(recs, nil); !rep.OK() || len(rep.Chains) != 2 || rep.Chains[0].Head.Seq != 8 {
		t.Fatalf("intact: %s / %+v", problems(rep), rep.Chains)
	}
	// Order of the input is not the chain's order.
	rev := clone(recs)
	for i, j := 0, len(rev)-1; i < j; i, j = i+1, j-1 {
		rev[i], rev[j] = rev[j], rev[i]
	}
	if rep := Verify(rev, nil); !rep.OK() {
		t.Fatalf("reversed input: %s", problems(rep))
	}

	// A record removed from the middle.
	del := append(clone(recs[:3]), recs[4:]...)
	if got := problems(Verify(del, nil)); !strings.Contains(got, "seq 4-4 are missing") {
		t.Fatalf("deleted record: %q", got)
	}
	// The first records removed, with nothing in any chain to say why.
	if got := problems(Verify(recs[2:], nil)); !strings.Contains(got, "begins at seq 3") {
		t.Fatalf("deleted prefix: %q", got)
	}
	// A field changed: the stored hash no longer matches.
	alt := clone(recs)
	alt[2].Principal = "mallory"
	if got := problems(Verify(alt, nil)); !strings.Contains(got, "seq 3: the record does not match its hash") {
		t.Fatalf("altered record: %q", got)
	}
	// Changed and re-hashed: its own hash is right, the next record's
	// link to it is not.
	alt = clone(recs)
	alt[2].Principal = "mallory"
	alt[2].Hash = alt[2].ComputeHash()
	if got := problems(Verify(alt, nil)); !strings.Contains(got, "seq 4 does not follow seq 3") {
		t.Fatalf("altered and re-hashed: %q", got)
	}
	// Two neighbours exchanged, each keeping the other's place.
	swap := clone(recs)
	swap[4], swap[5] = swap[5], swap[4]
	swap[4].Seq, swap[5].Seq = 5, 6
	if got := problems(Verify(swap, nil)); !strings.Contains(got, "seq 5") || !strings.Contains(got, "seq 6") {
		t.Fatalf("reordered pair: %q", got)
	}
	// And exchanged with their hashes recomputed to match.
	swap[4].Hash, swap[5].Hash = swap[4].ComputeHash(), swap[5].ComputeHash()
	if got := problems(Verify(swap, nil)); !strings.Contains(got, "altered or reordered") {
		t.Fatalf("reordered and re-hashed pair: %q", got)
	}
	// The same record twice.
	dup := append(clone(recs), recs[1])
	if got := problems(Verify(dup, nil)); !strings.Contains(got, "seq 2 appears twice") {
		t.Fatalf("duplicate: %q", got)
	}
}

// TestVerifyRetention: a chain cut by retention verifies when a record
// says where it was cut — in its own chain or, for a node that has
// left, in another's — and only then.
func TestVerifyRetention(t *testing.T) {
	recs := chain(1, 0, GenesisHash, 10)
	recs = appendEvent(recs, EventRetention, Cut{Node: 1, ThroughSeq: 4, ThroughHash: recs[3].Hash})
	rep := Verify(recs[4:], nil)
	if !rep.OK() || !strings.Contains(strings.Join(rep.Chains[0].Notes, " "), "through seq 4 were aged out") {
		t.Fatalf("cut chain: %s / %v", problems(rep), rep.Chains[0].Notes)
	}
	// Cut further than the record says.
	if got := problems(Verify(recs[6:], nil)); !strings.Contains(got, "begins at seq 7") {
		t.Fatalf("cut past the retention record: %q", got)
	}
	// The cut recorded, the records not yet removed (a crash between
	// the two): still a chain.
	if rep := Verify(recs, nil); !rep.OK() {
		t.Fatalf("cut recorded, not applied: %s", problems(rep))
	}
	// A departed node's chain, cut by the node leading range 1.
	gone := chain(7, 0, GenesisHash, 6)
	leader := chain(1, 0, GenesisHash, 2)
	leader = appendEvent(leader, EventRetention, Cut{Node: 7, ThroughSeq: 2, ThroughHash: gone[1].Hash})
	if rep := Verify(append(clone(gone[2:]), leader...), nil); !rep.OK() {
		t.Fatalf("chain cut by another node: %s", problems(rep))
	}
	// A retention record that was itself forged (its hash is wrong)
	// explains nothing.
	forged := clone(leader)
	forged[2].Detail = strings.Replace(forged[2].Detail, `"through_seq":2`, `"through_seq":4`, 1)
	if got := problems(Verify(append(clone(gone[4:]), forged...), nil)); !strings.Contains(got, "begins at seq 5") {
		t.Fatalf("forged cut: %q", got)
	}
}

// TestVerifyHeads: the newest records of a chain can be removed without
// breaking any link. What catches it is a head remembered elsewhere —
// an anchor in another node's chain, or one taken out of the cluster —
// and the same catches a chain rewritten whole.
func TestVerifyHeads(t *testing.T) {
	a := chain(1, 0, GenesisHash, 6)
	b := chain(2, 0, GenesisHash, 2)
	b = appendEvent(b, EventAnchor, Anchor{Heads: []Head{{Node: 1, Seq: 6, Hash: a[5].Hash}, {Node: 2, Seq: 2, Hash: b[1].Hash}}})
	if rep := Verify(append(clone(a), b...), nil); !rep.OK() {
		t.Fatalf("anchored: %s", problems(rep))
	}
	if got := problems(Verify(append(clone(a[:4]), b...), nil)); !strings.Contains(got, "now ends at seq 4: its newest records were removed") {
		t.Fatalf("tail removed under an anchor: %q", got)
	}
	// Rewritten from seq 3 on, consistently: only the anchor disagrees.
	rewritten := append(clone(a[:2]), chain(1, 2, a[1].Hash, 4)...)
	rewritten[3].Principal = "nobody"
	for i := 2; i < len(rewritten); i++ {
		rewritten[i].PrevHash = rewritten[i-1].Hash
		rewritten[i].Hash = rewritten[i].ComputeHash()
	}
	if got := problems(Verify(append(clone(rewritten), b...), nil)); !strings.Contains(got, "seq 6 is not the record the anchor at node 2 seq 3 saw") {
		t.Fatalf("rewritten under an anchor: %q", got)
	}
	// With no anchor, the rewrite verifies against the table alone —
	// this is the limit of an unkeyed chain — and fails against a head
	// that was kept outside.
	if rep := Verify(rewritten, nil); !rep.OK() {
		t.Fatalf("a consistent rewrite is not detectable from the table alone, and was reported: %s", problems(rep))
	}
	trusted := []Head{{Node: 1, Seq: 6, Hash: a[5].Hash}}
	if got := problems(Verify(rewritten, trusted)); !strings.Contains(got, "is not the record the trusted head saw: the chain was rewritten") {
		t.Fatalf("rewritten, against a trusted head: %q", got)
	}
	if got := problems(Verify(a[:5], trusted)); !strings.Contains(got, "newest records were removed") {
		t.Fatalf("tail removed, against a trusted head: %q", got)
	}
	if got := problems(Verify(b, trusted)); !strings.Contains(got, "no record of that node") {
		t.Fatalf("chain removed whole, against a trusted head: %q", got)
	}
	if rep := Verify(a, trusted); !rep.OK() {
		t.Fatalf("intact, against a trusted head: %s", problems(rep))
	}
}

// TestJSONLines: an export read back is the same records, and a line
// that is not a record is an error rather than a record skipped.
func TestJSONLines(t *testing.T) {
	recs := chain(3, 0, GenesisHash, 4)
	var buf bytes.Buffer
	if err := WriteJSONLines(&buf, recs); err != nil {
		t.Fatal(err)
	}
	if n := strings.Count(buf.String(), "\n"); n != 4 {
		t.Fatalf("%d lines", n)
	}
	got, err := ReadJSONLines(strings.NewReader(buf.String() + "\n"))
	if err != nil || len(got) != 4 || got[3] != recs[3] {
		t.Fatalf("round trip: %v %+v", err, got)
	}
	if rep := Verify(got, nil); !rep.OK() {
		t.Fatal(problems(rep))
	}
	if _, err := ReadJSONLines(strings.NewReader(buf.String() + "not json\n")); err == nil || !strings.Contains(err.Error(), "line 5") {
		t.Fatalf("garbage line: %v", err)
	}
}
