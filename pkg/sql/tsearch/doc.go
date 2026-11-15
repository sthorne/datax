// Package tsearch is full-text search in the PostgreSQL style (issue
// #299): documents reduced to a tsvector (a sorted set of lexemes, each
// with the positions it occurs at), searches written as a tsquery (a
// boolean expression over lexemes with a phrase operator), the match
// between the two, ranking and headlines.
//
// The package knows nothing of SQL datums, so that pkg/sql/types can
// parse and render the two types with it. Everything here is a pure
// function of its arguments; the session's default configuration is
// resolved by the caller.
//
// # Reducing text to lexemes
//
// A text-search configuration is a tokenizer followed by a normalizer.
// The tokenizer is the same for every configuration and is far simpler
// than PostgreSQL's parser (see Tokenize for the rules). Two
// configurations exist:
//
//   - simple: every token, lower-cased.
//   - english: tokens made only of letters are dropped when they are
//     English stop words and are otherwise stemmed with the Snowball
//     English (Porter2) algorithm; every other token (numbers, words
//     with digits, e-mail addresses, host names) is kept lower-cased.
//
// Positions count every token, stop words included, from 1, as
// PostgreSQL's do, so a phrase search knows that "cat" and "hat" in
// "cat in the hat" are three apart.
//
// # Limits
//
// They are PostgreSQL's: a lexeme is at most MaxLexemeLen bytes (longer
// tokens are skipped by ToVector and refused in a literal), a position
// is at most MaxPos (later ones are recorded as MaxPos), a lexeme keeps
// at most MaxPositions positions, and a vector's lexemes total at most
// MaxVectorBytes.
package tsearch

// The limits of a tsvector.
const (
	MaxLexemeLen   = 2046
	MaxPos         = 16383
	MaxPositions   = 256
	MaxVectorBytes = 1 << 20
	// MaxDistance is the largest N of a <N> phrase operator.
	MaxDistance = 16384
)
