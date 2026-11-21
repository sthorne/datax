package tsearch

import (
	"fmt"
	"math/rand"
	"strings"
	"testing"
)

// fusedVocab is what the random documents are made of: stop words,
// words that stem together (connect, connected, connecting), stemming's
// exceptions, words with digits, numbers, addresses, upper case,
// accents and the letters whose lower case is longer in UTF-8.
var fusedVocab = []string{
	"the", "a", "of", "and", "not", "is", "was", "in",
	"cat", "cats", "Cat", "CATS", "hat", "hats", "dog", "dogs", "fat", "rat",
	"connect", "connected", "connecting", "connection", "connections",
	"run", "running", "runs", "ran", "runner",
	"generous", "generously", "communism", "skies", "dying", "news", "early",
	"topic1", "topic12", "topic123", "serial7", "3rd", "42", "3.14", "007",
	"x@example.com", "example.com", "notes.txt", "well-known", "it's",
	"café", "Café", "naïve", "Ⱥpple", "ⱥpple", "straße", "über", "İstanbul",
	"report", "reports", "reported", "reporting", "status", "statuses",
	"quarterly", "figures", "committee", "estimates", "shipping",
}

func randDoc(r *rand.Rand) string {
	n := r.Intn(40)
	if r.Intn(20) == 0 {
		n = 300 + r.Intn(300) // past MaxPositions for some lexeme
	}
	seps := []string{" ", " ", " ", ", ", ". ", "-", "_", "/", "  ", "\n", "!"}
	var b strings.Builder
	for i := 0; i < n; i++ {
		if i > 0 {
			b.WriteString(seps[r.Intn(len(seps))])
		}
		b.WriteString(fusedVocab[r.Intn(len(fusedVocab))])
	}
	return b.String()
}

// randOperand is a lexeme a query may name: a normalized vocabulary
// word, a piece of one (for a prefix), or a word no document has.
func randOperand(r *rand.Rand, cfg *Config) *Node {
	var lex string
	for lex == "" {
		switch r.Intn(10) {
		case 0:
			lex = "zzz"
		default:
			for _, tok := range Tokenize(fusedVocab[r.Intn(len(fusedVocab))]) {
				if l, ok := cfg.Lexeme(tok); ok {
					lex = l
				}
			}
		}
	}
	n := &Node{Kind: NodeVal, Lexeme: lex}
	if r.Intn(4) == 0 {
		n.Prefix = true
		if len(lex) > 1 && r.Intn(2) == 0 {
			n.Lexeme = lex[:1+r.Intn(len(lex)-1)]
		}
	}
	if r.Intn(5) == 0 {
		n.Weights = uint8(1 + r.Intn(15))
	}
	return n
}

func randQuery(r *rand.Rand, cfg *Config, depth int) *Node {
	if depth == 0 || r.Intn(3) == 0 {
		return randOperand(r, cfg)
	}
	switch r.Intn(5) {
	case 0:
		return &Node{Kind: NodeNot, L: randQuery(r, cfg, depth-1)}
	case 1:
		return &Node{Kind: NodeAnd, L: randQuery(r, cfg, depth-1), R: randQuery(r, cfg, depth-1)}
	case 2:
		return &Node{Kind: NodeOr, L: randQuery(r, cfg, depth-1), R: randQuery(r, cfg, depth-1)}
	case 3:
		return &Node{Kind: NodePhrase, Distance: r.Intn(4), L: randQuery(r, cfg, depth-1), R: randQuery(r, cfg, depth-1)}
	}
	return randOperand(r, cfg)
}

// TestFusedMatchesFullPath: MatchText is Match over ToVector, for
// random documents and random queries of every operator (prefixes,
// weights, negation, phrases at any depth, and queries with many
// lexemes, which take the map), under both configurations; and the
// sorted ToVector is the builder's.
func TestFusedMatchesFullPath(t *testing.T) {
	r := rand.New(rand.NewSource(1))
	for _, name := range []string{"english", "simple"} {
		cfg, err := LookupConfig(name)
		if err != nil {
			t.Fatal(err)
		}
		docs := make([]string, 300)
		vecs := make([]Vector, len(docs))
		for i := range docs {
			docs[i] = randDoc(r)
			v, err := ToVector(cfg, docs[i])
			if err != nil {
				t.Fatal(err)
			}
			ref, err := toVectorBuilder(cfg, docs[i])
			if err != nil {
				t.Fatal(err)
			}
			if v.String() != ref.String() {
				t.Fatalf("%s: ToVector(%q)\n got  %s\n want %s", name, docs[i], v, ref)
			}
			vecs[i] = v
		}
		matched := 0
		for qi := 0; qi < 600; qi++ {
			q := Query{Root: randQuery(r, cfg, 1+qi%5)}
			if qi%50 == 0 {
				// Many lexemes: the exact ones are looked up in a map.
				root := randOperand(r, cfg)
				for k := 0; k < 12; k++ {
					root = &Node{Kind: NodeOr, L: root, R: randOperand(r, cfg)}
				}
				if qi%100 == 0 {
					root = &Node{Kind: NodePhrase, Distance: 1, L: root, R: randOperand(r, cfg)}
				}
				q.Root = root
			}
			m := NewMatcher(q)
			for i, doc := range docs {
				want := Match(vecs[i], q)
				got, err := m.MatchText(cfg, doc)
				if err != nil {
					t.Fatal(err)
				}
				if got != want {
					t.Fatalf("%s: %q @@ %q: fused %v, full %v (vector %s)", name, doc, q, got, want, vecs[i])
				}
				if want {
					matched++
				}
			}
		}
		if matched == 0 {
			t.Fatalf("%s: no query matched anything: the test proves nothing", name)
		}
	}
}

// TestFusedParsedQueries: the same over queries written as text, as
// to_tsquery, plainto_tsquery, phraseto_tsquery and websearch_to_tsquery
// make them.
func TestFusedParsedQueries(t *testing.T) {
	cfg, _ := LookupConfig("english")
	docs := []string{
		"The fat cats sat on the mat", "a fat rat", "cat in the hat", "the cat", "Connecting the connections",
		"skies over İstanbul", "x@example.com wrote notes.txt", "", "the and of", "rats! fat ones",
	}
	queries := []string{
		"cat", "cats & fat", "fat | dog", "!cat", "!dog & !cat", "cat <-> hat", "cat <3> hat", "cat <-> !hat",
		"connect:*", "fat:A", "fat:D", "fat:*AB", "rat <-> !fat", "!(cat <-> hat)", "(fat | cat) <-> (rat | sat)",
		"sky & istanbul", "x@example.com", "the", "!the", "notes.txt & !cat",
	}
	for _, qs := range queries {
		var qq []Query
		q, err := ToQuery(cfg, qs)
		if err != nil {
			t.Fatalf("%q: %v", qs, err)
		}
		qq = append(qq, q, PlainToQuery(cfg, qs), PhraseToQuery(cfg, qs), WebsearchToQuery(cfg, qs))
		for _, q := range qq {
			m := NewMatcher(q)
			for _, doc := range docs {
				v, err := ToVector(cfg, doc)
				if err != nil {
					t.Fatal(err)
				}
				got, err := m.MatchText(cfg, doc)
				if err != nil {
					t.Fatal(err)
				}
				if want := Match(v, q); got != want {
					t.Errorf("%q @@ %q: fused %v, full %v", doc, q, got, want)
				}
			}
		}
	}
}

// TestFusedLongText: a text too long to be sure of its vector's size
// goes through ToVector, positions past MaxPos and all, and reports the
// error to_tsvector reports.
func TestFusedLongText(t *testing.T) {
	cfg, _ := LookupConfig("english")
	long := strings.Repeat(strings.Repeat("filler", 200)+" ", fusedMaxText/1200+1) + "cat hat"
	m := NewMatcher(mustQuery(t, cfg, "cat <-> hat"))
	if ok, err := m.MatchText(cfg, long); err != nil || !ok {
		t.Fatalf("long text: %v, %v", ok, err)
	}
	// Past MaxPos the positions all read MaxPos: "cat" and "hat" are
	// at the same place, not one apart, as in the full vector.
	for _, qs := range []string{"cat <-> hat", "cat <0> hat", "cat & hat"} {
		doc := strings.Repeat("w ", MaxPos+10) + "cat hat"
		q := mustQuery(t, cfg, qs)
		v, _ := ToVector(cfg, doc)
		got, err := NewMatcher(q).MatchText(cfg, doc)
		if err != nil {
			t.Fatal(err)
		}
		if want := Match(v, q); got != want {
			t.Errorf("past MaxPos, %s: fused %v, full %v", qs, got, want)
		}
	}
	var b strings.Builder
	for i := 0; b.Len() <= MaxVectorBytes*5/4; i++ {
		fmt.Fprintf(&b, "w%07d ", i)
	}
	_, wantErr := ToVector(cfg, b.String())
	if wantErr == nil {
		t.Fatal("ToVector took more than MaxVectorBytes of lexemes")
	}
	if _, err := m.MatchText(cfg, b.String()); err == nil || err.Error() != wantErr.Error() {
		t.Fatalf("error %v, want %v", err, wantErr)
	}
}

// TestFusedAllocations: matching a short document allocates nothing
// for a query without a phrase.
func TestFusedAllocations(t *testing.T) {
	cfg, _ := LookupConfig("english")
	doc := benchDoc(7, 1000)
	for _, qs := range []string{"topic7", "topic8", "serial7:*", "status & !topic7", "connect | (fat & !cat)"} {
		m := NewMatcher(mustQuery(t, cfg, qs))
		if n := testing.AllocsPerRun(100, func() { _, _ = m.MatchText(cfg, doc) }); n != 0 {
			t.Errorf("%s: %v allocations per document, want 0", qs, n)
		}
	}
}
