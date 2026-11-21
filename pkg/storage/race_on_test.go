//go:build race

package storage

// raceEnabled reports whether the tests were built with the race
// detector, whose instrumentation allocates: allocation-count tests skip.
const raceEnabled = true
