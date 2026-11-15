package tsearch

import (
	"strings"
	"testing"
)

// stemSample is the sample of the Snowball English stemmer's published
// vocabulary that its description of the algorithm prints: two runs of
// consecutive words with the stems the reference implementation gives.
const stemSample = `
	consign consign consigned consign consigning consign consignment consign consist consist consisted consist
	consistency consist consistent consist consistently consist consisting consist consists consist consolation consol
	consolations consol consolatory consolatori console consol consoled consol consoles consol consolidate consolid
	consolidated consolid consolidating consolid consoling consol consolingly consol consols consol consonant conson
	consort consort consorted consort consorting consort conspicuous conspicu conspicuously conspicu conspiracy conspiraci
	conspirator conspir conspirators conspir conspire conspir conspired conspir conspiring conspir constable constabl
	constables constabl constance constanc constancy constanc constant constant knack knack knackeries knackeri
	knacks knack knag knag knave knave knaves knave knavish knavish kneaded knead
	kneading knead knee knee kneel kneel kneeled kneel kneeling kneel kneels kneel
	knees knee knell knell knelt knelt knew knew knick knick knif knif
	knife knife knight knight knightly knight knights knight knit knit knits knit
	knitted knit knitting knit knives knive knob knob knobs knob knock knock
	knocked knock knocker knocker knockers knocker knocking knock knocks knock knopp knopp
	knot knot knots knot
`

// stemPairs are further words with their Snowball English stems: the
// examples the algorithm's description gives for each step and
// exception, and common words whose stems were worked out by hand from
// that description (and, for the ones PostgreSQL's documentation shows
// in a to_tsvector result, checked against it). They were NOT produced
// by running a reference implementation, which this repository does not
// carry.
const stemPairs = `
	running run runs run ran ran easily easili fairly fair cats cat
	ponies poni ties tie cries cri caresses caress gas gas this this
	gaps gap kiwis kiwi agreed agre feed feed luxuriated luxuri hopping hop
	hoping hope hoped hope filing file filling fill falling fall dropped drop
	cry cri by by say say happy happi sensational sensat conditional condit
	rational ration valenci valenc hesitanci hesit digitizer digit conformabli conform radicalli radic
	differentli differ analogousli analog vietnamization vietnam predication predic operator oper feudalism feudal
	decisiveness decis hopefulness hope callousness callous formaliti formal sensitiviti sensit sensibiliti sensibl
	triplicate triplic formative format formalize formal electriciti electr electrical electr hopeful hope
	goodness good revival reviv allowance allow inference infer airliner airlin gyroscopic gyroscop
	adjustable adjust defensible defens irritant irrit replacement replac adjustment adjust dependent depend
	adoption adopt communism communism activate activ angulariti angular homologous homolog effective effect
	bowdlerize bowdler probate probat rate rate cease ceas controll control roll roll
	generate generat generates generat generously generous generic generic general general communication communic
	community communiti communal communal arsenic arsenic arsenal arsenal university univers universe univers
	universal univers supernovae supernova stars star documents document similarity similar query queri
	queries queri containing contain organization organ relational relat national nation international intern
	happiness happi beautiful beauti beautifully beauti quickly quick jumped jump jumping jump
	jumps jump lazy lazi dogs dog foxes fox boxes box churches church
	matches match flies fli flying fli died die dies die dying die
	lying lie tying tie sky sky skies sky news news early earli
	only onli singly singl ugly ugli gently gentl idly idl inning inning
	outing outing canning canning herring herring earring earring proceed proceed exceed exceed
	succeed succeed exceeded exceed proceeding proceed successful success argument argument arguments argument
	arguing argu argued argu argue argu database databas databases databas indexing index
	indexes index indexed index index index searching search searched search searches search
	search search ranking rank ranked rank ranks rank rank rank computers comput
	computer comput computing comput computation comput computational comput compute comput stemming stem
	stemmed stem stemmer stemmer stems stem stem stem connection connect connections connect
	connected connect connecting connect connect connect relate relat related relat relating relat
	relativity relat relative relat relatively relat nationality nation nationalize nation hopefully hope
	hopeless hopeless hopelessly hopeless hopelessness hopeless ability abil abilities abil able abl
	ably abli possible possibl possibly possibl possibility possibl probably probabl probable probabl
	probability probabl electric electr electricity electr yes yes yellow yellow you you
	yours your boy boy boys boy toy toy enjoy enjoy enjoyed enjoy
	enjoying enjoy playing play played play plays play player player players player
	saying say says say said said animals anim animal anim generalization general
	generalizations general triumphs triumph triumph triumph meetings meet meeting meet agreement agreement
	agreements agreement disagreement disagr oscillators oscil oscillate oscil oscillation oscil oscillating oscil
	flowers flower flower flower flowering flower flowered flower sized size sizing size
	size size bed bed beds bed bedding bed red red reds red
	shed shed shedding shed bleed bleed bleeding bleed feeding feed need need
	needed need needing need needs need speed speed speeding speed ring ring
	rings ring ringing ring sing sing singing sing singer singer king king
	kings king bring bring bringing bring thing thing things thing something someth
	anything anyth nothing noth string string strings string spring spring opening open
	opened open opens open open open happen happen happened happen happening happen
	visited visit visiting visit visit visit visitor visitor visitors visitor edited edit
	editing edit edit edit editor editor traveled travel travelling travel travelled travel
	travel travel traveler travel cancellation cancel cancelled cancel canceling cancel cancel cancel
	programs program programming program programmed program programmer programm program program logic logic
	logical logic logically logic logistics logist motion motion motions motion emotional emot
	emotion emot emotions emot nation nation nations nation station station stations station
	stationary stationari mention mention mentioned mention question question questions question questioning question
	apple appl apples appl simple simpl simply simpli simplify simplifi simplified simplifi
	simplification simplif single singl double doubl doubled doubl doubling doubl trouble troubl
	troubles troubl troubled troubl bottle bottl bottles bottl little littl title titl
	titles titl table tabl tables tabl cable cabl cables cabl stable stabl
	stability stabil stabilize stabil unstable unstabl terrible terribl terribly terribl horrible horribl
	incredible incred available avail availability avail variable variabl variables variabl various various
	variety varieti vary vari varies vari varied vari varying vari
`

func TestStem(t *testing.T) {
	n := 0
	for _, table := range []string{stemSample, stemPairs} {
		f := strings.Fields(table)
		if len(f)%2 != 0 {
			t.Fatal("odd number of fields in a stem table")
		}
		for i := 0; i < len(f); i += 2 {
			n++
			if got := Stem(f[i]); got != f[i+1] {
				t.Errorf("Stem(%q) = %q, want %q", f[i], got, f[i+1])
			}
		}
	}
	if n < 400 {
		t.Errorf("only %d stem pairs", n)
	}
	// What is not an English word is left alone.
	for _, w := range []string{"", "a", "is", "naïve", "straße", "abc123", "CATS", "москва"} {
		if got := Stem(w); got != w {
			t.Errorf("Stem(%q) = %q, want it unchanged", w, got)
		}
	}
	// Apostrophes, which the tokenizer never passes on but a caller of
	// Stem might.
	for in, want := range map[string]string{"dog's": "dog", "dogs'": "dog", "'tis": "tis"} {
		if got := Stem(in); got != want {
			t.Errorf("Stem(%q) = %q, want %q", in, got, want)
		}
	}
	// Stemming a stem changes nothing more often than not, and never
	// panics on odd input.
	for _, w := range []string{"aaa", "yyy", "eee", "sss", "ied", "ies", "eed", "ing", "ly", "ion", "y", "ye", "generalizations'"} {
		_ = Stem(w)
	}
}

func BenchmarkStem(b *testing.B) {
	words := strings.Fields(stemPairs)
	for i := 0; i < b.N; i++ {
		Stem(words[i%len(words)])
	}
}
