package tsearch

import (
	"fmt"
	"strconv"
	"strings"
)

// HeadlineOptions are ts_headline's options.
type HeadlineOptions struct {
	StartSel, StopSel  string
	MaxWords, MinWords int
	HighlightAll       bool
}

// DefaultHeadlineOptions are PostgreSQL's defaults.
func DefaultHeadlineOptions() HeadlineOptions {
	return HeadlineOptions{StartSel: "<b>", StopSel: "</b>", MaxWords: 35, MinWords: 15}
}

// ParseHeadlineOptions reads "Name=value, Name=value" (names in any
// case, values optionally in double quotes). ShortWord, MaxFragments
// and FragmentDelimiter are accepted and have no effect: the headline
// is always one fragment.
func ParseHeadlineOptions(s string) (HeadlineOptions, error) {
	o := DefaultHeadlineOptions()
	for _, part := range splitOptions(s) {
		name, value, ok := strings.Cut(part, "=")
		if !ok {
			return o, fmt.Errorf("invalid headline option %q", strings.TrimSpace(part))
		}
		name = strings.ToLower(strings.TrimSpace(name))
		value = strings.TrimSpace(value)
		if len(value) >= 2 && value[0] == '"' && value[len(value)-1] == '"' {
			value = value[1 : len(value)-1]
		}
		num := func() (int, error) {
			n, err := strconv.Atoi(value)
			if err != nil || n < 0 {
				return 0, fmt.Errorf("invalid value %q for headline option %s", value, name)
			}
			return n, nil
		}
		var err error
		switch name {
		case "startsel":
			o.StartSel = value
		case "stopsel":
			o.StopSel = value
		case "maxwords":
			o.MaxWords, err = num()
		case "minwords":
			o.MinWords, err = num()
		case "highlightall":
			switch strings.ToLower(value) {
			case "true", "t", "on", "1", "yes":
				o.HighlightAll = true
			case "false", "f", "off", "0", "no":
				o.HighlightAll = false
			default:
				err = fmt.Errorf("invalid value %q for headline option highlightall", value)
			}
		case "shortword", "maxfragments":
			_, err = num()
		case "fragmentdelimiter":
		default:
			err = fmt.Errorf("unrecognized headline parameter: %q", name)
		}
		if err != nil {
			return o, err
		}
	}
	if o.MinWords >= o.MaxWords {
		return o, fmt.Errorf("MinWords should be less than MaxWords")
	}
	return o, nil
}

// splitOptions splits at the commas outside double quotes.
func splitOptions(s string) []string {
	var out []string
	quoted := false
	start := 0
	for i := 0; i < len(s); i++ {
		switch s[i] {
		case '"':
			quoted = !quoted
		case ',':
			if !quoted {
				out = append(out, s[start:i])
				start = i + 1
			}
		}
	}
	if strings.TrimSpace(s[start:]) != "" {
		out = append(out, s[start:])
	}
	return out
}

// Headline is ts_headline: a stretch of the document with the words the
// query names marked.
//
// The fragment is chosen more simply than PostgreSQL chooses it: of the
// stretches of at most MaxWords words that start at a marked word, the
// one that holds the most distinct query words (then the most marked
// words, then the earliest) is taken, cut after its last marked word
// and extended to MinWords words. A document that holds none of the
// query's words yields its first MinWords words. With HighlightAll the
// whole document is returned.
func Headline(cfg *Config, document string, q Query, o HeadlineOptions) string {
	toks := Tokenize(document)
	ops := q.Operands()
	// hit[i] is 1 + the index of the operand token i satisfies.
	hit := make([]int, len(toks))
	hits := 0
	for i, tok := range toks {
		lex, ok := cfg.Lexeme(tok)
		if !ok {
			continue
		}
		for j, op := range ops {
			if lex == op.Lexeme || (op.Prefix && strings.HasPrefix(lex, op.Lexeme)) {
				hit[i] = j + 1
				hits++
				break
			}
		}
	}
	first, last := 0, len(toks)-1
	switch {
	case len(toks) == 0:
		if o.HighlightAll {
			return document
		}
		return ""
	case o.HighlightAll:
	case hits == 0:
		last = min(len(toks), o.MinWords) - 1
	default:
		bestDistinct, bestHits := -1, -1
		for i := range toks {
			if hit[i] == 0 {
				continue
			}
			seen := map[int]bool{}
			n, end := 0, i
			for j := i; j < len(toks) && j < i+o.MaxWords; j++ {
				if hit[j] != 0 {
					seen[hit[j]] = true
					n++
					end = j
				}
			}
			if len(seen) > bestDistinct || (len(seen) == bestDistinct && n > bestHits) {
				bestDistinct, bestHits = len(seen), n
				first, last = i, end
			}
		}
		// Too short a fragment reads badly: take following words, then
		// preceding ones, up to MinWords.
		for last-first+1 < o.MinWords && last < len(toks)-1 {
			last++
		}
		for last-first+1 < o.MinWords && first > 0 {
			first--
		}
	}
	var b strings.Builder
	from, to := toks[first].Start, toks[last].End
	if o.HighlightAll {
		from, to = 0, len(document)
	}
	at := from
	for i := first; i <= last; i++ {
		if hit[i] == 0 {
			continue
		}
		b.WriteString(document[at:toks[i].Start])
		b.WriteString(o.StartSel)
		b.WriteString(document[toks[i].Start:toks[i].End])
		b.WriteString(o.StopSel)
		at = toks[i].End
	}
	b.WriteString(document[at:to])
	return b.String()
}
