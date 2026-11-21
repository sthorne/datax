package tsearch

import "bytes"

// The Snowball English stemmer ("Porter2"), written from the published
// definition of the algorithm: prelude, regions R1 and R2, the two
// exception lists, steps 0 to 5 and the postlude. PostgreSQL's english
// configuration uses the same algorithm. Later revisions of the
// Snowball sources adjusted a handful of words (PostgreSQL 17 and later
// ship one); where they differ, this follows the long-standing
// definition.
//
// The word is held as bytes. Only ASCII lower-case letters take part:
// a word with any other rune has no English suffix rules that could be
// trusted on it, and is returned unchanged.

// stemExceptions are the words stemmed by table, before anything else.
var stemExceptions = map[string]string{
	"skis": "ski", "skies": "sky", "dying": "die", "lying": "lie", "tying": "tie",
	"idly": "idl", "gently": "gentl", "ugly": "ugli", "early": "earli", "only": "onli", "singly": "singl",
	"sky": "sky", "news": "news", "howe": "howe", "atlas": "atlas", "cosmos": "cosmos", "bias": "bias", "andes": "andes",
}

// stemStopAfter1a are left alone once step 1a has run.
var stemStopAfter1a = map[string]bool{
	"inning": true, "outing": true, "canning": true, "herring": true, "earring": true,
	"proceed": true, "exceed": true, "succeed": true,
}

// The lengths of the words in the two lists, which spare most words
// the map lookups.
const (
	maxExceptionLen   = 6
	minStopAfter1aLen = 6
)

type stemmer struct {
	b      []byte
	p1, p2 int
}

func isVowel(c byte) bool {
	switch c {
	case 'a', 'e', 'i', 'o', 'u', 'y':
		return true
	}
	return false
}

func (s *stemmer) ends(suffix string) bool {
	n, m := len(s.b), len(suffix)
	if n < m {
		return false
	}
	// From the last byte back: most suffixes fail on it.
	for k := 1; k <= m; k++ {
		if s.b[n-k] != suffix[m-k] {
			return false
		}
	}
	return true
}

// inR1 and inR2 report whether a suffix of n bytes lies inside the
// region.
func (s *stemmer) inR1(n int) bool { return len(s.b)-n >= s.p1 }
func (s *stemmer) inR2(n int) bool { return len(s.b)-n >= s.p2 }

// replace swaps the last n bytes for repl, which is never longer: the
// word shrinks in place. (Reslicing rather than appending also keeps
// the bytes on their caller's stack.)
func (s *stemmer) replace(n int, repl string) {
	k := len(s.b) - n
	s.b = s.b[:k+len(repl)]
	copy(s.b[k:], repl)
}

// addE appends an e where step 1b has just removed a longer suffix.
func (s *stemmer) addE() {
	n := len(s.b)
	s.b = s.b[:n+1]
	s.b[n] = 'e'
}

// hasVowel reports whether b[:end] contains a vowel.
func (s *stemmer) hasVowel(end int) bool {
	for i := 0; i < end; i++ {
		if isVowel(s.b[i]) {
			return true
		}
	}
	return false
}

// shortSyllableAt reports whether b[:end] ends in a short syllable: a
// vowel between a non-vowel and a non-vowel other than w, x and Y, or a
// vowel at the start of the word followed by a non-vowel.
func (s *stemmer) shortSyllableAt(end int) bool {
	b := s.b
	if end == 2 {
		return isVowel(b[0]) && !isVowel(b[1])
	}
	if end < 3 {
		return false
	}
	c := b[end-1]
	return !isVowel(b[end-3]) && isVowel(b[end-2]) && !isVowel(c) && c != 'w' && c != 'x' && c != 'Y'
}

// isShort: the word ends in a short syllable and R1 is empty.
func (s *stemmer) isShort() bool {
	return s.p1 >= len(s.b) && s.shortSyllableAt(len(s.b))
}

func (s *stemmer) markRegions() {
	b := s.b
	n := len(b)
	s.p1, s.p2 = n, n
	i := 0
	switch {
	case bytes.HasPrefix(b, []byte("gener")), bytes.HasPrefix(b, []byte("arsen")):
		i = 5
	case bytes.HasPrefix(b, []byte("commun")):
		i = 6
	default:
		for i < n && !isVowel(b[i]) {
			i++
		}
		for i < n && isVowel(b[i]) {
			i++
		}
		if i >= n {
			return
		}
		i++
	}
	s.p1 = i
	for i < n && !isVowel(b[i]) {
		i++
	}
	for i < n && isVowel(b[i]) {
		i++
	}
	if i >= n {
		return
	}
	s.p2 = i + 1
}

// longest returns the longest of the suffixes the word ends with ("" if
// none). The steps choose their suffix first and test its region
// second: a longer suffix outside the region does not give way to a
// shorter one inside it.
func (s *stemmer) longest(set *suffixSet) string {
	if len(s.b) == 0 {
		return ""
	}
	best := ""
	for _, suf := range set.byLast[s.b[len(s.b)-1]] {
		if len(suf) > len(best) && s.ends(suf) {
			best = suf
		}
	}
	return best
}

// suffixSet is a step's suffixes, grouped by their last byte: a word
// is compared only with the suffixes that end as it does.
type suffixSet struct {
	byLast [256][]string
}

func newSuffixSet(suffixes ...string) *suffixSet {
	set := &suffixSet{}
	for _, suf := range suffixes {
		c := suf[len(suf)-1]
		set.byLast[c] = append(set.byLast[c], suf)
	}
	return set
}

var (
	step0Set  = newSuffixSet("'s'", "'s", "'")
	step1aSet = newSuffixSet("sses", "ied", "ies", "us", "ss", "s")
	step1bSet = newSuffixSet("eedly", "ingly", "edly", "eed", "ing", "ed")
	step2Set  = newSuffixSet(step2Suffixes...)
	step3Set  = newSuffixSet("ational", "tional", "alize", "icate", "iciti", "ical", "ful", "ness", "ative")
	step4Set  = newSuffixSet("al", "ance", "ence", "er", "ic", "able", "ible", "ant", "ement", "ment", "ent",
		"ism", "ate", "iti", "ous", "ive", "ize", "ion")
)

func (s *stemmer) step0() {
	if suf := s.longest(step0Set); suf != "" {
		s.replace(len(suf), "")
	}
}

func (s *stemmer) step1a() {
	switch suf := s.longest(step1aSet); suf {
	case "sses":
		s.replace(2, "")
	case "ied", "ies":
		if len(s.b) > 4 {
			s.replace(3, "i")
		} else {
			s.replace(3, "ie")
		}
	case "s":
		// Deleted when a vowel comes somewhere before the letter in
		// front of it: "gaps" loses it, "gas" and "this" do not.
		if s.hasVowel(len(s.b) - 2) {
			s.replace(1, "")
		}
	}
}

func isDouble(a, b byte) bool {
	if a != b {
		return false
	}
	switch a {
	case 'b', 'd', 'f', 'g', 'm', 'n', 'p', 'r', 't':
		return true
	}
	return false
}

func (s *stemmer) step1b() {
	suf := s.longest(step1bSet)
	switch suf {
	case "":
		return
	case "eed", "eedly":
		if s.inR1(len(suf)) {
			s.replace(len(suf), "ee")
		}
		return
	}
	if !s.hasVowel(len(s.b) - len(suf)) {
		return
	}
	s.replace(len(suf), "")
	n := len(s.b)
	switch {
	case s.ends("at"), s.ends("bl"), s.ends("iz"):
		s.addE()
	case n >= 2 && isDouble(s.b[n-2], s.b[n-1]):
		s.b = s.b[:n-1]
	case s.isShort():
		s.addE()
	}
}

func (s *stemmer) step1c() {
	n := len(s.b)
	if n > 2 && (s.b[n-1] == 'y' || s.b[n-1] == 'Y') && !isVowel(s.b[n-2]) {
		s.b[n-1] = 'i'
	}
}

var step2Suffixes = []string{
	"tional", "enci", "anci", "abli", "entli", "izer", "ization", "ational", "ation", "ator",
	"alism", "aliti", "alli", "fulness", "ousli", "ousness", "iveness", "iviti", "biliti", "bli",
	"ogi", "fulli", "lessli", "li",
}

var step2Replace = map[string]string{
	"tional": "tion", "enci": "ence", "anci": "ance", "abli": "able", "entli": "ent",
	"izer": "ize", "ization": "ize", "ational": "ate", "ation": "ate", "ator": "ate",
	"alism": "al", "aliti": "al", "alli": "al", "fulness": "ful", "ousli": "ous", "ousness": "ous",
	"iveness": "ive", "iviti": "ive", "biliti": "ble", "bli": "ble", "fulli": "ful", "lessli": "less",
}

func isValidLi(c byte) bool {
	switch c {
	case 'c', 'd', 'e', 'g', 'h', 'k', 'm', 'n', 'r', 't':
		return true
	}
	return false
}

func (s *stemmer) step2() {
	suf := s.longest(step2Set)
	if suf == "" || !s.inR1(len(suf)) {
		return
	}
	n := len(s.b)
	switch suf {
	case "ogi":
		if n > 3 && s.b[n-4] == 'l' {
			s.replace(3, "og")
		}
	case "li":
		if n > 2 && isValidLi(s.b[n-3]) {
			s.replace(2, "")
		}
	default:
		s.replace(len(suf), step2Replace[suf])
	}
}

func (s *stemmer) step3() {
	suf := s.longest(step3Set)
	if suf == "" || !s.inR1(len(suf)) {
		return
	}
	switch suf {
	case "ational":
		s.replace(len(suf), "ate")
	case "tional":
		s.replace(len(suf), "tion")
	case "alize":
		s.replace(len(suf), "al")
	case "icate", "iciti", "ical":
		s.replace(len(suf), "ic")
	case "ful", "ness":
		s.replace(len(suf), "")
	case "ative":
		if s.inR2(len(suf)) {
			s.replace(len(suf), "")
		}
	}
}

func (s *stemmer) step4() {
	suf := s.longest(step4Set)
	if suf == "" || !s.inR2(len(suf)) {
		return
	}
	if suf == "ion" {
		n := len(s.b)
		if n < 4 || (s.b[n-4] != 's' && s.b[n-4] != 't') {
			return
		}
	}
	s.replace(len(suf), "")
}

func (s *stemmer) step5() {
	n := len(s.b)
	if n == 0 {
		return
	}
	switch s.b[n-1] {
	case 'e':
		if s.inR2(1) || (s.inR1(1) && !s.shortSyllableAt(n-1)) {
			s.b = s.b[:n-1]
		}
	case 'l':
		if s.inR2(1) && n > 1 && s.b[n-2] == 'l' {
			s.b = s.b[:n-1]
		}
	}
}

// Stem returns the Snowball English stem of a lower-case word.
func Stem(word string) string {
	if !stemmable(word) {
		return word
	}
	if stem, ok := stemExceptions[word]; ok {
		return stem
	}
	return string(stemBytes([]byte(word)))
}

// stemmable: the word is long enough to have a suffix and made only of
// ASCII lower-case letters and apostrophes.
func stemmable[S string | []byte](word S) bool {
	if len(word) <= 2 {
		return false
	}
	for i := 0; i < len(word); i++ {
		if c := word[i]; (c < 'a' || c > 'z') && c != '\'' {
			return false
		}
	}
	return true
}

// stemInPlace is Stem over bytes: it overwrites word and returns its
// stem, a prefix of word (or of word[1:]) — a stem is never longer than
// its word.
func stemInPlace(word []byte) []byte {
	if !stemmable(word) {
		return word
	}
	if len(word) <= maxExceptionLen {
		if stem, ok := stemExceptions[string(word)]; ok {
			return append(word[:0], stem...)
		}
	}
	return stemBytes(word)
}

// stemBytes runs the algorithm on a stemmable word that is not an
// exception, in place.
func stemBytes(word []byte) []byte {
	s := stemmer{b: word}
	// Prelude: a leading apostrophe goes; y is a consonant (written Y)
	// at the start of the word and after a vowel.
	if s.b[0] == '\'' {
		s.b = s.b[1:]
	}
	if len(s.b) <= 2 {
		return s.b
	}
	if s.b[0] == 'y' {
		s.b[0] = 'Y'
	}
	for i := 1; i < len(s.b); i++ {
		if s.b[i] == 'y' && isVowel(s.b[i-1]) {
			s.b[i] = 'Y'
		}
	}
	s.markRegions()
	s.step0()
	s.step1a()
	if len(s.b) < minStopAfter1aLen || !stemStopAfter1a[string(s.b)] {
		s.step1b()
		s.step1c()
		s.step2()
		s.step3()
		s.step4()
		s.step5()
	}
	for i, c := range s.b {
		if c == 'Y' {
			s.b[i] = 'y'
		}
	}
	return s.b
}
