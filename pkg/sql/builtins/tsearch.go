package builtins

import (
	"strconv"
	"strings"

	"github.com/sthorne/datax/pkg/sql/tsearch"
	"github.com/sthorne/datax/pkg/sql/types"
)

const catTextSearch = "Full-text search"

// HeadlineWithDefault is the name ts_headline is called under once the
// session has put its default configuration in front of the arguments.
// ts_headline's own argument list does not say whether a leading text
// is a configuration or the document (ts_headline(a, b, c) is either
// (config, document, query) or (document, query, options)); only the
// values do, so the default travels with the call and the function
// decides when it sees them.
const HeadlineWithDefault = "ts_headline_with_default"

// TextSearchDefaultArg reports, for a call of a text-search function
// with nargs arguments, whether the session must supply its default
// configuration as a new first argument, and the name to call then.
func TextSearchDefaultArg(name string, nargs int) (callAs string, ok bool) {
	switch name {
	case "to_tsvector", "to_tsquery", "plainto_tsquery", "phraseto_tsquery", "websearch_to_tsquery":
		return name, nargs == 1
	case "ts_headline":
		return HeadlineWithDefault, true
	}
	return "", false
}

func tsConfig(d types.Datum) (*tsearch.Config, error) {
	cfg, err := tsearch.LookupConfig(d.Text())
	if err != nil {
		return nil, errf("42704", "%v", err)
	}
	return cfg, nil
}

// TSVectorArg reads a tsvector argument; text is parsed as a tsvector
// literal.
func TSVectorArg(d types.Datum) (tsearch.Vector, error) {
	if d.Fam != types.Tsvector && d.Fam != types.String {
		return nil, errf(CodeUndefined, "expected a tsvector, got %s", strings.ToLower(d.Fam.String()))
	}
	v, err := tsearch.ParseVector(d.S)
	if err != nil {
		return nil, errf(CodeInvalidText, "%v", err)
	}
	return v, nil
}

// TSQueryArg reads a tsquery argument; text is parsed as a tsquery
// literal.
func TSQueryArg(d types.Datum) (tsearch.Query, error) {
	if d.Fam != types.Tsquery && d.Fam != types.String {
		return tsearch.Query{}, errf(CodeUndefined, "expected a tsquery, got %s", strings.ToLower(d.Fam.String()))
	}
	q, err := tsearch.ParseQuery(d.S)
	if err != nil {
		return tsearch.Query{}, errf("42601", "%v", err)
	}
	return q, nil
}

// TSMatch is the @@ operator. One side is a tsvector and the other a
// tsquery, in either order; a text operand beside a typed one is read
// as a literal of the other type. Text on the left of a tsquery is
// refused rather than read as a tsvector literal: PostgreSQL would run
// it through to_tsvector, and guessing wrongly here would silently
// match the wrong rows.
func TSMatch(l, r types.Datum) (bool, error) {
	if l.Fam == types.Tsquery {
		l, r = r, l
	}
	switch {
	case l.Fam == types.Tsvector && (r.Fam == types.Tsquery || r.Fam == types.String):
	case l.Fam == types.String && r.Fam == types.Tsquery:
		return false, errf(CodeNotSupported, "text @@ tsquery is not supported: write to_tsvector(config, text) @@ query")
	default:
		return false, errf(CodeUndefined, "operator does not exist: %s @@ %s", strings.ToLower(l.Fam.String()), strings.ToLower(r.Fam.String()))
	}
	v, err := TSVectorArg(l)
	if err != nil {
		return false, err
	}
	q, err := TSQueryArg(r)
	if err != nil {
		return false, err
	}
	return tsearch.Match(v, q), nil
}

// ConcatTSVectors is tsvector || tsvector.
func ConcatTSVectors(l, r types.Datum) (types.Datum, error) {
	lv, err := TSVectorArg(l)
	if err != nil {
		return types.Datum{}, err
	}
	rv, err := TSVectorArg(r)
	if err != nil {
		return types.Datum{}, err
	}
	out, err := lv.Concat(rv)
	if err != nil {
		return types.Datum{}, errf("54000", "%v", err)
	}
	return types.NewTsvector(out), nil
}

// float4 rounds to single precision and back by way of the shortest
// text that names the single-precision value: PostgreSQL's ranks are
// float4, and 0.06079271 should not print as 0.06079271063208580.
func float4(f float64) types.Datum {
	v, _ := strconv.ParseFloat(strconv.FormatFloat(float64(float32(f)), 'g', -1, 32), 64)
	return types.NewFloat(v)
}

// rankArgs unpacks ([weights,] vector, query [, normalization]).
func rankArgs(name string, a []types.Datum) (w [4]float64, v tsearch.Vector, q tsearch.Query, norm int, err error) {
	w = tsearch.DefaultWeights
	if a[0].Fam.IsArray() {
		if len(a[0].A) != 4 {
			return w, nil, q, 0, errf("2202E", "array of weight is too short (must be 4 values: D, C, B, A)")
		}
		for i, el := range a[0].A {
			if el.Null {
				return w, nil, q, 0, errf("22004", "array of weight must not contain nulls")
			}
			f, cerr := el.Coerce(types.Float)
			if cerr != nil {
				return w, nil, q, 0, errf(CodeInvalidArgument, "weights must be numbers")
			}
			// A negative weight means "the default", as in PostgreSQL.
			if f.F >= 0 {
				w[i] = f.F
			}
			if w[i] > 1 {
				return w, nil, q, 0, errf(CodeInvalidArgument, "weight out of range")
			}
		}
		a = a[1:]
	}
	if len(a) < 2 || len(a) > 3 {
		return w, nil, q, 0, errf(CodeUndefined, "%s takes ([weights,] tsvector, tsquery [, normalization])", name)
	}
	if v, err = TSVectorArg(a[0]); err != nil {
		return
	}
	if q, err = TSQueryArg(a[1]); err != nil {
		return
	}
	if len(a) == 3 {
		n, cerr := a[2].Coerce(types.Int)
		if cerr != nil {
			return w, nil, q, 0, errf(CodeInvalidArgument, "%s: the normalization must be an integer", name)
		}
		norm = int(n.I)
	}
	return
}

func headline(cfg *tsearch.Config, a []types.Datum) (types.Datum, error) {
	q, err := TSQueryArg(a[1])
	if err != nil {
		return types.Datum{}, err
	}
	opts := tsearch.DefaultHeadlineOptions()
	if len(a) == 3 {
		if opts, err = tsearch.ParseHeadlineOptions(a[2].Text()); err != nil {
			return types.Datum{}, errf(CodeInvalidArgument, "%v", err)
		}
	}
	return types.NewString(tsearch.Headline(cfg, a[0].Text(), q, opts)), nil
}

func init() {
	text := types.String
	// The five functions that reduce text take the configuration first,
	// when they take one; with one argument the session's default has
	// already been supplied (see TextSearchDefaultArg), and a call that
	// reaches here without it — nothing does today — gets the built-in
	// default.
	withConfig := func(a []types.Datum) (*tsearch.Config, string, error) {
		if len(a) == 1 {
			cfg, err := tsearch.LookupConfig(tsearch.DefaultConfig)
			return cfg, a[0].S, err
		}
		cfg, err := tsConfig(a[0])
		return cfg, a[1].S, err
	}
	register(&Builtin{Name: "to_tsvector", Args: []types.Family{text, text}, MinArgs: 1, Ret: types.Tsvector, Vol: Stable, Category: catTextSearch,
		Doc: "to_tsvector([config,] text): the text reduced to a tsvector — its lexemes under the configuration (simple or english; default_text_search_config when omitted), each with the positions it occurs at.",
		Fn: func(a []types.Datum) (types.Datum, error) {
			cfg, s, err := withConfig(a)
			if err != nil {
				return types.Datum{}, err
			}
			v, err := tsearch.ToVector(cfg, s)
			if err != nil {
				return types.Datum{}, errf("54000", "%v", err)
			}
			return types.NewTsvector(v), nil
		}})
	register(&Builtin{Name: "to_tsquery", Args: []types.Family{text, text}, MinArgs: 1, Ret: types.Tsquery, Vol: Stable, Category: catTextSearch,
		Doc: "to_tsquery([config,] text): a tsquery from its text form — words joined by & (and), | (or), ! (not) and <-> or <N> (followed by), with parentheses, word:* for a prefix and word:AB for weights — each word normalized by the configuration; stop words are dropped.",
		Fn: func(a []types.Datum) (types.Datum, error) {
			cfg, s, err := withConfig(a)
			if err != nil {
				return types.Datum{}, err
			}
			q, err := tsearch.ToQuery(cfg, s)
			if err != nil {
				return types.Datum{}, errf("42601", "%v", err)
			}
			return types.NewTsquery(q), nil
		}})
	for _, f := range []struct {
		name, doc string
		fn        func(*tsearch.Config, string) tsearch.Query
	}{
		{"plainto_tsquery", "plainto_tsquery([config,] text): a tsquery requiring every word of the text (the words normalized and joined by &); punctuation and operators in the text mean nothing.", tsearch.PlainToQuery},
		{"phraseto_tsquery", "phraseto_tsquery([config,] text): a tsquery requiring the words of the text in that order (joined by <->, wider where stop words were dropped).", tsearch.PhraseToQuery},
		{"websearch_to_tsquery", "websearch_to_tsquery([config,] text): a tsquery from search-box syntax — words are required, \"quoted text\" is a phrase, or separates alternatives and -word excludes; never raises a syntax error.", tsearch.WebsearchToQuery},
	} {
		register(&Builtin{Name: f.name, Args: []types.Family{text, text}, MinArgs: 1, Ret: types.Tsquery, Vol: Stable, Category: catTextSearch, Doc: f.doc,
			Fn: func(a []types.Datum) (types.Datum, error) {
				cfg, s, err := withConfig(a)
				if err != nil {
					return types.Datum{}, err
				}
				return types.NewTsquery(f.fn(cfg, s)), nil
			}})
	}
	register(&Builtin{Name: "ts_rank", Args: []types.Family{Any, Any, Any, Any}, MinArgs: 2, Ret: types.Float, Category: catTextSearch,
		Doc: "ts_rank([weights,] tsvector, tsquery [, normalization]): a relevance score from how often the query's lexemes occur in the vector and, for a query that requires them together, how near each other. weights is an array of four numbers scoring D, C, B and A positions ({0.1, 0.2, 0.4, 1.0} by default); normalization is a bit mask (1 divides by 1 + log of the document length, 2 by the length, 8 by the number of unique words, 16 by 1 + its log, 32 by the rank + 1).",
		Fn: func(a []types.Datum) (types.Datum, error) {
			w, v, q, norm, err := rankArgs("ts_rank", a)
			if err != nil {
				return types.Datum{}, err
			}
			return float4(tsearch.Rank(w, v, q, norm)), nil
		}})
	register(&Builtin{Name: "ts_rank_cd", Args: []types.Family{Any, Any, Any, Any}, MinArgs: 2, Ret: types.Float, Category: catTextSearch,
		Doc: "ts_rank_cd([weights,] tsvector, tsquery [, normalization]): cover density ranking — a score from the shortest stretches of the document that satisfy the query, higher the fewer other words they hold. Arguments as ts_rank; normalization 4 also divides by the mean harmonic distance between the stretches. Needs positions: a stripped vector scores as if every word stood at one place.",
		Fn: func(a []types.Datum) (types.Datum, error) {
			w, v, q, norm, err := rankArgs("ts_rank_cd", a)
			if err != nil {
				return types.Datum{}, err
			}
			return float4(tsearch.RankCD(w, v, q, norm)), nil
		}})
	register(&Builtin{Name: "ts_headline", Args: []types.Family{Any, Any, Any, Any}, MinArgs: 2, Ret: text, Vol: Stable, Category: catTextSearch,
		Doc: "ts_headline([config,] document, tsquery [, options]): a fragment of the document with the query's words marked. options is 'Name=value, ...' with StartSel and StopSel (<b> and </b>), MaxWords (35), MinWords (15) and HighlightAll (false); ShortWord, MaxFragments and FragmentDelimiter are accepted and ignored. With three arguments the first is the configuration unless the second is a tsquery value.",
		Fn: func(a []types.Datum) (types.Datum, error) {
			// Reached with the configuration written out: all four
			// arguments, or three with the query last.
			if len(a) == 2 || (len(a) == 3 && a[1].Fam == types.Tsquery) {
				cfg, _ := tsearch.LookupConfig(tsearch.DefaultConfig)
				return headline(cfg, a)
			}
			cfg, err := tsConfig(a[0])
			if err != nil {
				return types.Datum{}, err
			}
			return headline(cfg, a[1:])
		}})
	register(&Builtin{Name: HeadlineWithDefault, Args: []types.Family{text, Any, Any, Any, Any}, MinArgs: 3, Ret: text, Vol: Stable, Category: catTextSearch, Hidden: true,
		Doc: "ts_headline with the session's default configuration as its first argument.",
		Fn: func(a []types.Datum) (types.Datum, error) {
			rest := a[1:]
			// Three arguments are (document, query, options) when the
			// second is a tsquery, and (config, document, query)
			// otherwise — which is also how three untyped literals
			// resolve in PostgreSQL.
			if len(rest) == 4 || (len(rest) == 3 && rest[1].Fam != types.Tsquery) {
				cfg, err := tsConfig(rest[0])
				if err != nil {
					return types.Datum{}, err
				}
				return headline(cfg, rest[1:])
			}
			cfg, err := tsConfig(a[0])
			if err != nil {
				return types.Datum{}, err
			}
			return headline(cfg, rest)
		}})
	register(&Builtin{Name: "setweight", Args: []types.Family{types.Tsvector, text}, MinArgs: 2, Ret: types.Tsvector, Category: catTextSearch,
		Doc: "setweight(tsvector, weight): the vector with every position labelled A, B, C or D, for ranking the parts of a document differently (a title A, a body D).",
		Fn: func(a []types.Datum) (types.Datum, error) {
			v, err := TSVectorArg(a[0])
			if err != nil {
				return types.Datum{}, err
			}
			if len(a[1].S) != 1 {
				return types.Datum{}, errf(CodeInvalidArgument, "unrecognized weight: %q", a[1].S)
			}
			w, ok := tsearch.ParseWeight(a[1].S[0])
			if !ok {
				return types.Datum{}, errf(CodeInvalidArgument, "unrecognized weight: %q", a[1].S)
			}
			return types.NewTsvector(v.SetWeight(w)), nil
		}})
	register(&Builtin{Name: "strip", Args: []types.Family{types.Tsvector}, MinArgs: 1, Ret: types.Tsvector, Category: catTextSearch,
		Doc: "strip(tsvector): the vector without positions and weights — smaller, still matched by & | !, never by a phrase.",
		Fn: func(a []types.Datum) (types.Datum, error) {
			v, err := TSVectorArg(a[0])
			if err != nil {
				return types.Datum{}, err
			}
			return types.NewTsvector(v.Strip()), nil
		}})
	register(&Builtin{Name: "numnode", Args: []types.Family{types.Tsquery}, MinArgs: 1, Ret: types.Int, Category: catTextSearch,
		Doc: "numnode(tsquery): the number of lexemes and operators in the query; 0 for a query that reduced to nothing (only stop words).",
		Fn: func(a []types.Datum) (types.Datum, error) {
			q, err := TSQueryArg(a[0])
			if err != nil {
				return types.Datum{}, err
			}
			return i64(int64(q.NumNodes())), nil
		}})
	register(&Builtin{Name: "tsvector_to_array", Args: []types.Family{types.Tsvector}, MinArgs: 1, Ret: types.ArrayOf(text), Category: catTextSearch,
		Doc: "tsvector_to_array(tsvector): the vector's lexemes, sorted, as a text array.",
		Fn: func(a []types.Datum) (types.Datum, error) {
			v, err := TSVectorArg(a[0])
			if err != nil {
				return types.Datum{}, err
			}
			lex := v.Lexemes()
			elems := make([]types.Datum, len(lex))
			for i, l := range lex {
				elems[i] = types.NewString(l)
			}
			return types.NewArray(text, elems), nil
		}})
	b := &Builtin{Name: "get_current_ts_config", Ret: text, Vol: Stable, Category: catTextSearch, Session: true,
		Doc: "The session's default text-search configuration (default_text_search_config)."}
	register(b)
}
