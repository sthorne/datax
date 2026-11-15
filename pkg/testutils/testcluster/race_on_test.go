//go:build race

package testcluster

// raceEnabled reports whether the tests were built with the race
// detector, which runs this package several times slower: the few tests
// whose cost is their volume (a recall measurement over thousands of
// vectors, a large object streamed in tens of megabytes) run at a
// smaller size there. What they check is the same at either size.
const raceEnabled = true
