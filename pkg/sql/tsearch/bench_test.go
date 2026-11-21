package tsearch

import (
	"fmt"
	"strings"
	"testing"
)

// benchFiller and benchDoc mirror the documents of `datax bench
// text-search` (cmd/datax/bench.go): sixteen short English words.
var benchFiller = []string{"quarterly", "figures", "were", "reviewed", "by", "the", "committee", "and", "found", "consistent",
	"with", "earlier", "estimates", "although", "several", "regions", "reported", "delays", "in", "shipping"}

func benchDoc(j, groups int) string {
	var b strings.Builder
	fmt.Fprintf(&b, "common topic%d serial%d", j%groups, j)
	if j%2 == 0 {
		fmt.Fprintf(&b, " status report%d", (j%groups)%97)
	} else {
		fmt.Fprintf(&b, " report%d filed status", (j%groups)%97)
	}
	for i := 0; i < 11; i++ {
		b.WriteByte(' ')
		b.WriteString(benchFiller[(j*7+i*3)%len(benchFiller)])
	}
	return b.String()
}

func benchCorpus() []string {
	docs := make([]string, 1000)
	for j := range docs {
		docs[j] = benchDoc(j, 1000)
	}
	return docs
}

var benchQueries = []struct{ name, query string }{
	{"word", "topic7"},
	{"prefix", "serial12:*"},
	{"and-not", "status & !topic7"},
	{"phrase", "topic7 & status <-> report7"},
}

// BenchmarkMatchFullPath is what a filter `to_tsvector(cfg, body) @@
// q` cost per document before the fused path: the whole vector built,
// rendered as a datum, parsed back, and the query parsed, for every
// row. Reported per document.
func BenchmarkMatchFullPath(b *testing.B) {
	cfg, _ := LookupConfig("english")
	docs := benchCorpus()
	for _, bq := range benchQueries {
		q, err := ToQuery(cfg, bq.query)
		if err != nil {
			b.Fatal(err)
		}
		qtext := q.String()
		b.Run(bq.name, func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				v, err := ToVector(cfg, docs[i%len(docs)])
				if err != nil {
					b.Fatal(err)
				}
				pv, err := ParseVector(v.String())
				if err != nil {
					b.Fatal(err)
				}
				pq, err := ParseQuery(qtext)
				if err != nil {
					b.Fatal(err)
				}
				Match(pv, pq)
			}
		})
	}
}

// BenchmarkToVector is to_tsvector alone, per document.
func BenchmarkToVector(b *testing.B) {
	cfg, _ := LookupConfig("english")
	docs := benchCorpus()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		if _, err := ToVector(cfg, docs[i%len(docs)]); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkMatchText is the same filter fused: the query prepared once
// per statement, each document's lexemes tested as they are made, and
// the walk stopped once the answer is known. Reported per document.
func BenchmarkMatchText(b *testing.B) {
	cfg, _ := LookupConfig("english")
	docs := benchCorpus()
	for _, bq := range benchQueries {
		q, err := ToQuery(cfg, bq.query)
		if err != nil {
			b.Fatal(err)
		}
		m := NewMatcher(q)
		b.Run(bq.name, func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				if _, err := m.MatchText(cfg, docs[i%len(docs)]); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

// BenchmarkParseVector is reading a tsvector datum back, per document:
// what @@ costs on a stored tsvector column.
func BenchmarkParseVector(b *testing.B) {
	cfg, _ := LookupConfig("english")
	docs := benchCorpus()
	texts := make([]string, len(docs))
	for i, d := range docs {
		v, err := ToVector(cfg, d)
		if err != nil {
			b.Fatal(err)
		}
		texts[i] = v.String()
	}
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		if _, err := ParseVector(texts[i%len(texts)]); err != nil {
			b.Fatal(err)
		}
	}
}
