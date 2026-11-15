package tsearch

import (
	"fmt"
	"strings"
)

// Config is a text-search configuration: how a token becomes a lexeme.
type Config struct {
	Name string
	stop map[string]struct{}
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

// allLetters: no digit in the token. A word with a digit in it is a
// part number or an identifier, not English.
func allLetters(s string) bool {
	for i := 0; i < len(s); i++ {
		if s[i] >= '0' && s[i] <= '9' {
			return false
		}
	}
	return true
}
