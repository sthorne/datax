package tsearch

import (
	"fmt"
	"strings"
)

// Config is a text-search configuration: how a token becomes a lexeme.
type Config struct {
	Name string
	stop map[string]struct{}
	// stem is Stem for english, nil for simple; lexemeBytes calls
	// stemInPlace, its form over bytes, directly.
	stem func(string) string
}

// englishStopWords is the stop-word list of Snowball's English stemmer,
// the one PostgreSQL installs as english.stop.
const englishStopWords = `i me my myself we our ours ourselves you your yours yourself yourselves
he him his himself she her hers herself it its itself they them their theirs themselves
what which who whom this that these those am is are was were be been being
have has had having do does did doing a an the and but if or because as until while
of at by for with about against between into through during before after above below
to from up down in out on off over under again further then once here there when where why how
all any both each few more most other some such no nor not only own same so than too very
s t can will just don should now`

var configs = map[string]*Config{}

func init() {
	configs["simple"] = &Config{Name: "simple"}
	stop := map[string]struct{}{}
	for _, w := range strings.Fields(englishStopWords) {
		stop[w] = struct{}{}
	}
	configs["english"] = &Config{Name: "english", stop: stop, stem: Stem}
}

// DefaultConfig is the configuration a session starts with
// (default_text_search_config).
const DefaultConfig = "english"

// ConfigNames lists the configurations, sorted.
func ConfigNames() []string { return []string{"english", "simple"} }

// LookupConfig finds a configuration by name; the pg_catalog schema may
// qualify it, as PostgreSQL's own default ("pg_catalog.english") does.
func LookupConfig(name string) (*Config, error) {
	n := strings.ToLower(strings.TrimSpace(name))
	n = strings.TrimPrefix(n, "pg_catalog.")
	if c, ok := configs[n]; ok {
		return c, nil
	}
	return nil, fmt.Errorf("text search configuration %q does not exist", name)
}

// Lexeme normalizes a token; ok is false for a stop word.
func (c *Config) Lexeme(t Token) (lexeme string, ok bool) {
	if c.stem == nil || t.Kind != TokWord || !allLetters(t.Text) {
		return t.Text, true
	}
	if _, stop := c.stop[t.Text]; stop {
		return "", false
	}
	return c.stem(t.Text), true
}

// lexemeBytes is Lexeme over the token a tokenizer holds: the lexeme
// is written over text (a stem is never longer than its word) and
// returned; ok is false for a stop word. Nothing is allocated.
func (c *Config) lexemeBytes(kind TokenKind, text []byte) (lexeme []byte, ok bool) {
	if c.stem == nil || kind != TokWord || !allLetters(text) {
		return text, true
	}
	if _, stop := c.stop[string(text)]; stop {
		return nil, false
	}
	return stemInPlace(text), true
}

// lexemes walks a text's lexemes in order, as ToVector keeps them.
// Stop words and over-long words are skipped but, as in PostgreSQL,
// still count as positions.
type lexemes struct {
	cfg *Config
	t   tokenizer
	n   int // tokens read
	// only, when set, skips the tokens whose (lower-cased) first byte it
	// does not hold, before any normalizing: a lexeme starts with its
	// token's first byte (see Matcher.first), so a caller that wants
	// only lexemes starting with certain bytes is spared the stop-word
	// lookup and the stemming of every other token.
	only *[256]bool
}

func newLexemes(cfg *Config, text string) lexemes {
	return lexemes{cfg: cfg, t: tokenizer{s: text}}
}

// next returns the next lexeme and its position; ok is false at the
// end of the text. The lexeme is written to buf (appended to buf[:0]),
// so a caller that keeps buf on its stack walks without allocating.
func (w *lexemes) next(buf []byte) (lexeme []byte, pos int, ok bool) {
	for w.t.next() {
		w.n++
		text := w.t.lower(buf[:0])
		if w.only != nil && !w.only[text[0]] {
			continue
		}
		lex, keep := w.cfg.lexemeBytes(w.t.kind, text)
		if keep && len(lex) <= MaxLexemeLen {
			return lex, min(w.n, MaxPos), true
		}
	}
	return nil, 0, false
}

// allLetters: no digit in the token. A word with a digit in it is a
// part number or an identifier, not English.
func allLetters[S string | []byte](s S) bool {
	for i := 0; i < len(s); i++ {
		if s[i] >= '0' && s[i] <= '9' {
			return false
		}
	}
	return true
}
