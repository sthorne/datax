// Package audit holds the format of the audit table's records and the
// check that a set of them is an unbroken chain (issue #293). It is
// deliberately free of the server: the node writes records with it, and
// `datax debug audit verify` checks an export with the very same code,
// on a machine that need not be able to reach the cluster.
//
// Every node keeps its own chain. A record carries the hash of the one
// before it in that node's chain, and its own hash covers every one of
// its fields, so removing, altering or reordering records breaks a link
// that Verify finds.
//
// What that proves has an edge worth stating. The hashes are not keyed:
// whoever can write the table's keys directly can rewrite a chain from
// some record onwards and it will verify again. Three things narrow
// that. A node remembers its own head and says so in its chain when the
// table disagrees (EventRestart). The node leading range 1 periodically
// writes every chain's head into its own (EventAnchor), so rewriting one
// chain means rewriting the others that vouch for it. And a head taken
// out of the cluster (`datax debug audit heads`) cannot be rewritten at
// all: verification against it catches anything done to the records it
// covers.
package audit

import (
	"bufio"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"sort"
	"strconv"
	"strings"
)

// The events the audit table writes about itself. They are records in
// a chain like any other, which is what makes them trustworthy: a cut
// or a restart that is not explained by one of these is a finding.
const (
	// EventRetention says a chain was cut: the records of Cut.Node up to
	// and including Cut.ThroughSeq were aged out, and the last of them
	// had hash Cut.ThroughHash. It is written before the records go.
	EventRetention = "audit-retention"
	// EventAnchor records the head of every chain, as the node leading
	// range 1 saw them.
	EventAnchor = "audit-anchor"
	// EventRestart says the node's own memory of its chain's head did
	// not match the table (a restore, or records removed underneath it),
	// so it carried on from what the table holds.
	EventRestart = "audit-chain-restart"
	// EventGap says how many events this node could not record: its
	// buffer filled while the table was unwritable, or a flood of
	// refusals went past the rate at which they are recorded.
	EventGap = "audit-gap"
	// EventRetentionSet records a change of the retention period.
	EventRetentionSet = "audit-retention-set"
)

// GenesisHash is the previous-hash of the first record of a chain.
const GenesisHash = "0000000000000000000000000000000000000000000000000000000000000000"

// Record is one row of the audit table.
type Record struct {
	Node int64 `json:"node"`
	Seq  int64 `json:"seq"`
	// AtMicros is the event's time in microseconds since the Unix epoch:
	// the precision a TIMESTAMPTZ carries over the wire, so the hash is
	// the same whether computed by the node or from an export.
	AtMicros  int64  `json:"at_us"`
	Event     string `json:"event"`
	Principal string `json:"principal"`
	Target    string `json:"target"`
	Outcome   string `json:"outcome"`
	// Detail is a JSON object of the record's remaining attributes.
	Detail   string `json:"detail"`
	PrevHash string `json:"prev_hash"`
	Hash     string `json:"hash"`
}

// ComputeHash is the hash a record with these fields must carry. Each
// field is length-prefixed, so no two different records share an
// encoding by moving a character from one field to the next.
func (r *Record) ComputeHash() string {
	h := sha256.New()
	var lenBuf [binary.MaxVarintLen64]byte
	field := func(s string) {
		n := binary.PutUvarint(lenBuf[:], uint64(len(s)))
		h.Write(lenBuf[:n])
		io.WriteString(h, s)
	}
	field("datax-audit-v1")
	field(strconv.FormatInt(r.Node, 10))
	field(strconv.FormatInt(r.Seq, 10))
	field(strconv.FormatInt(r.AtMicros, 10))
	field(r.Event)
	field(r.Principal)
	field(r.Target)
	field(r.Outcome)
	field(r.Detail)
	field(r.PrevHash)
	return hex.EncodeToString(h.Sum(nil))
}

// Head names the last record of one chain.
type Head struct {
	Node int64  `json:"node"`
	Seq  int64  `json:"seq"`
	Hash string `json:"hash"`
}

// Cut is the detail of an EventRetention record.
type Cut struct {
	Node        int64  `json:"node"`
	ThroughSeq  int64  `json:"through_seq"`
	ThroughHash string `json:"through_hash"`
}

// Anchor is the detail of an EventAnchor record.
type Anchor struct {
	Heads []Head `json:"heads"`
}

// Restart is the detail of an EventRestart record.
type Restart struct {
	Reason   string `json:"reason"`
	LostSeq  int64  `json:"lost_seq"`
	LostHash string `json:"lost_hash,omitempty"`
}

// WriteJSONLines writes records one JSON object per line.
func WriteJSONLines(w io.Writer, recs []Record) error {
	enc := json.NewEncoder(w)
	for i := range recs {
		if err := enc.Encode(&recs[i]); err != nil {
			return err
		}
	}
	return nil
}

// ReadJSONLines reads what WriteJSONLines wrote. Blank lines are
// skipped; anything else that is not a record is an error, because a
// line silently dropped here would read as a record removed.
func ReadJSONLines(r io.Reader) ([]Record, error) {
	var out []Record
	sc := bufio.NewScanner(r)
	sc.Buffer(make([]byte, 0, 64<<10), 4<<20)
	line := 0
	for sc.Scan() {
		line++
		text := strings.TrimSpace(sc.Text())
		if text == "" {
			continue
		}
		var rec Record
		if err := json.Unmarshal([]byte(text), &rec); err != nil {
			return nil, fmt.Errorf("line %d: %v", line, err)
		}
		out = append(out, rec)
	}
	return out, sc.Err()
}

// ChainReport is what Verify found in one node's chain.
type ChainReport struct {
	Node     int64 `json:"node"`
	Records  int   `json:"records"`
	FirstSeq int64 `json:"first_seq"`
	Head     Head  `json:"head"`
	// Problems are findings: each one means records were removed,
	// altered or reordered. Notes explain what is expected to look odd —
	// a chain cut by retention, a restart the node recorded.
	Problems []string `json:"problems,omitempty"`
	Notes    []string `json:"notes,omitempty"`
}

// Report is the result of Verify.
type Report struct {
	Chains []ChainReport `json:"chains"`
	// Problems not attributable to a chain that is present: a chain a
	// trusted head names that has no records at all.
	Problems []string `json:"problems,omitempty"`
}

// OK reports whether nothing was found.
func (r *Report) OK() bool {
	if len(r.Problems) > 0 {
		return false
	}
	for _, c := range r.Chains {
		if len(c.Problems) > 0 {
			return false
		}
	}
	return true
}

// Verify checks every chain among recs, which may be in any order and
// must be the whole table (a partial export reads as records missing).
// trusted are heads obtained earlier and kept somewhere the cluster
// cannot write; they may be nil.
func Verify(recs []Record, trusted []Head) *Report {
	byNode := map[int64][]Record{}
	for _, r := range recs {
		byNode[r.Node] = append(byNode[r.Node], r)
	}
	nodes := make([]int64, 0, len(byNode))
	for n := range byNode {
		nodes = append(nodes, n)
		chain := byNode[n]
		sort.SliceStable(chain, func(i, j int) bool { return chain[i].Seq < chain[j].Seq })
	}
	sort.Slice(nodes, func(i, j int) bool { return nodes[i] < nodes[j] })

	// The cuts and anchors of every chain, gathered first: a chain's cut
	// may be recorded in another's (a node that left is aged out by the
	// node leading range 1), and an anchor speaks about all of them. A
	// record whose own hash is wrong is not believed.
	type sourced struct {
		node, seq int64
	}
	cuts := map[int64][]Cut{}
	type anchoredHead struct {
		Head
		from sourced
	}
	var anchors []anchoredHead
	for _, n := range nodes {
		for i := range byNode[n] {
			r := &byNode[n][i]
			if r.Hash != r.ComputeHash() {
				continue
			}
			switch r.Event {
			case EventRetention:
				var c Cut
				if json.Unmarshal([]byte(r.Detail), &c) == nil {
					cuts[c.Node] = append(cuts[c.Node], c)
				}
			case EventAnchor:
				var a Anchor
				if json.Unmarshal([]byte(r.Detail), &a) == nil {
					for _, h := range a.Heads {
						anchors = append(anchors, anchoredHead{Head: h, from: sourced{r.Node, r.Seq}})
					}
				}
			}
		}
	}

	rep := &Report{}
	index := map[int64]int{}
	for _, n := range nodes {
		chain := byNode[n]
		cr := ChainReport{Node: n, Records: len(chain), FirstSeq: chain[0].Seq}
		last := chain[len(chain)-1]
		cr.Head = Head{Node: n, Seq: last.Seq, Hash: last.Hash}

		first := chain[0]
		switch {
		case first.Seq == 1:
			if first.PrevHash != GenesisHash {
				cr.Problems = append(cr.Problems, "seq 1 does not start from the genesis hash: the start of the chain was replaced")
			}
		default:
			explained := false
			for _, c := range cuts[n] {
				if c.ThroughSeq == first.Seq-1 && c.ThroughHash == first.PrevHash {
					explained = true
				}
			}
			if explained {
				cr.Notes = append(cr.Notes, fmt.Sprintf("records through seq %d were aged out by retention", first.Seq-1))
			} else {
				cr.Problems = append(cr.Problems, fmt.Sprintf("the chain begins at seq %d and no retention record accounts for seq 1-%d: records were removed", first.Seq, first.Seq-1))
			}
		}
		for i := range chain {
			r := &chain[i]
			if r.Hash != r.ComputeHash() {
				cr.Problems = append(cr.Problems, fmt.Sprintf("seq %d: the record does not match its hash: it was altered", r.Seq))
			}
			if i > 0 {
				prev := &chain[i-1]
				switch {
				case r.Seq == prev.Seq:
					cr.Problems = append(cr.Problems, fmt.Sprintf("seq %d appears twice", r.Seq))
				case r.Seq != prev.Seq+1:
					cr.Problems = append(cr.Problems, fmt.Sprintf("seq %d-%d are missing: records were removed", prev.Seq+1, r.Seq-1))
				case r.PrevHash != prev.Hash:
					cr.Problems = append(cr.Problems, fmt.Sprintf("seq %d does not follow seq %d (its previous-hash is another record's): records were altered or reordered", r.Seq, prev.Seq))
				}
			}
			switch r.Event {
			case EventRestart:
				var rs Restart
				_ = json.Unmarshal([]byte(r.Detail), &rs)
				cr.Notes = append(cr.Notes, fmt.Sprintf("seq %d: the node restarted its chain (%s; the head it remembered was seq %d)", r.Seq, rs.Reason, rs.LostSeq))
			case EventGap:
				cr.Notes = append(cr.Notes, fmt.Sprintf("seq %d: the node reports events it could not record: %s", r.Seq, r.Detail))
			}
		}
		// A cut that names records still present must agree with them.
		for _, c := range cuts[n] {
			if i := c.ThroughSeq - chain[0].Seq; i >= 0 && i < int64(len(chain)) && chain[i].Seq == c.ThroughSeq && chain[i].Hash != c.ThroughHash {
				cr.Problems = append(cr.Problems, fmt.Sprintf("seq %d is not the record a retention cut was taken at: it was replaced", c.ThroughSeq))
			}
		}
		index[n] = len(rep.Chains)
		rep.Chains = append(rep.Chains, cr)
	}

	// checkHead holds one remembered head against the chain it names.
	checkHead := func(h Head, source string, external bool) {
		i, ok := index[h.Node]
		if !ok {
			// A whole chain may age out, so only a trusted head — whose
			// holder knows how old it is — makes its absence a finding.
			if external {
				rep.Problems = append(rep.Problems, fmt.Sprintf("%s names node %d at seq %d, and the table holds no record of that node (aged out whole, if the head is older than the retention period; removed otherwise)", source, h.Node, h.Seq))
			}
			return
		}
		cr := &rep.Chains[i]
		chain := byNode[h.Node]
		switch {
		case h.Seq < chain[0].Seq:
			if external {
				cr.Notes = append(cr.Notes, fmt.Sprintf("%s (seq %d) is older than the oldest record kept: it can no longer be checked", source, h.Seq))
			}
		case h.Seq > cr.Head.Seq:
			cr.Problems = append(cr.Problems, fmt.Sprintf("%s saw this chain at seq %d and it now ends at seq %d: its newest records were removed", source, h.Seq, cr.Head.Seq))
		default:
			for j := range chain {
				if chain[j].Seq == h.Seq {
					if chain[j].Hash != h.Hash {
						cr.Problems = append(cr.Problems, fmt.Sprintf("seq %d is not the record %s saw: the chain was rewritten", h.Seq, source))
					}
					return
				}
			}
			// A gap at exactly this seq is already reported above.
		}
	}
	for _, a := range anchors {
		if a.Node == a.from.node {
			continue // a chain vouching for itself adds nothing
		}
		checkHead(a.Head, fmt.Sprintf("the anchor at node %d seq %d", a.from.node, a.from.seq), false)
	}
	for _, h := range trusted {
		checkHead(h, "the trusted head", true)
	}
	return rep
}
