package cli

import (
	"bufio"
	"errors"
	"io"
	"unicode/utf8"
)

// Terminal key decoding for the SQL shell's editor (issue #175).
//
// golang.org/x/term decodes a useful subset but not the one people
// reach for: it parses ESC [ 1 ; 3 C/D — modifier 3, Alt — and has no
// case for modifier 5, Ctrl, which is what Ctrl+arrow sends in xterm,
// iTerm2, GNOME Terminal and Windows Terminal alike. Decoding here means
// the editor can accept every spelling of "move by a word" a terminal
// might send, and can be tested by feeding it bytes.

// Key is a decoded keystroke: either a printable rune, or one of the
// named keys below with the modifiers that came with it.
type Key struct {
	Rune rune
	Code KeyCode
	Ctrl bool
	Alt  bool
}

// KeyCode names a non-printable key. KeyRune means Rune carries the
// character typed.
type KeyCode int

const (
	KeyRune KeyCode = iota
	KeyEnter
	KeyBackspace
	KeyDelete
	KeyLeft
	KeyRight
	KeyUp
	KeyDown
	KeyHome
	KeyEnd
	KeyTab
	KeyInterrupt   // Ctrl-C
	KeyEOF         // Ctrl-D on an empty buffer
	KeyKillToEnd   // Ctrl-K
	KeyKillToStart // Ctrl-U
	KeyKillWord    // Ctrl-W, and Ctrl/Alt+Backspace
	KeyClearScreen
	KeyPasteStart
	KeyPasteEnd
	// KeyUnknown is a sequence that decoded cleanly but means nothing
	// here; the editor ignores it rather than inserting its bytes as
	// text, which is what makes an unhandled function key harmless.
	KeyUnknown
)

// keyReader decodes keystrokes from a byte stream.
type keyReader struct {
	r *bufio.Reader
}

func newKeyReader(r io.Reader) *keyReader { return &keyReader{r: bufio.NewReader(r)} }

// ReadKey returns the next keystroke, blocking until one is available.
func (k *keyReader) ReadKey() (Key, error) {
	b, err := k.r.ReadByte()
	if err != nil {
		return Key{}, err
	}
	switch b {
	case 1: // ^A
		return Key{Code: KeyHome}, nil
	case 2: // ^B
		return Key{Code: KeyLeft}, nil
	case 3: // ^C
		return Key{Code: KeyInterrupt}, nil
	case 4: // ^D
		return Key{Code: KeyEOF}, nil
	case 5: // ^E
		return Key{Code: KeyEnd}, nil
	case 6: // ^F
		return Key{Code: KeyRight}, nil
	case 8: // ^H — Ctrl-Backspace on many terminals
		return Key{Code: KeyKillWord}, nil
	case 9:
		return Key{Code: KeyTab}, nil
	case 10, 13: // LF, CR
		return Key{Code: KeyEnter}, nil
	case 11: // ^K
		return Key{Code: KeyKillToEnd}, nil
	case 12: // ^L
		return Key{Code: KeyClearScreen}, nil
	case 14: // ^N
		return Key{Code: KeyDown}, nil
	case 16: // ^P
		return Key{Code: KeyUp}, nil
	case 21: // ^U
		return Key{Code: KeyKillToStart}, nil
	case 23: // ^W
		return Key{Code: KeyKillWord}, nil
	case 127:
		return Key{Code: KeyBackspace}, nil
	case 27:
		return k.readEscape()
	}
	if b < 0x20 {
		return Key{Code: KeyUnknown}, nil
	}
	return k.readRune(b)
}

// readRune completes a UTF-8 sequence whose first byte is b.
func (k *keyReader) readRune(b byte) (Key, error) {
	if b < utf8.RuneSelf {
		return Key{Code: KeyRune, Rune: rune(b)}, nil
	}
	buf := []byte{b}
	for len(buf) < utf8.UTFMax {
		if utf8.FullRune(buf) {
			break
		}
		nb, err := k.r.ReadByte()
		if err != nil {
			return Key{}, err
		}
		buf = append(buf, nb)
	}
	r, _ := utf8.DecodeRune(buf)
	if r == utf8.RuneError {
		return Key{Code: KeyUnknown}, nil
	}
	return Key{Code: KeyRune, Rune: r}, nil
}

// readEscape decodes what follows an ESC. An ESC with nothing after it
// (the user pressed Escape) reads as unknown.
func (k *keyReader) readEscape() (Key, error) {
	b, err := k.r.ReadByte()
	if err != nil {
		if errors.Is(err, io.EOF) {
			return Key{Code: KeyUnknown}, nil
		}
		return Key{}, err
	}
	switch b {
	case '[':
		return k.readCSI()
	case 'O':
		// Application cursor mode: ESC O A..D are the arrows.
		c, err := k.r.ReadByte()
		if err != nil {
			return Key{}, err
		}
		return Key{Code: arrowCode(c)}, nil
	case 'b': // Alt-b
		return Key{Code: KeyLeft, Alt: true}, nil
	case 'f': // Alt-f
		return Key{Code: KeyRight, Alt: true}, nil
	case 'd': // Alt-d: delete the word ahead
		return Key{Code: KeyDelete, Alt: true}, nil
	case 127: // Alt-Backspace
		return Key{Code: KeyKillWord}, nil
	}
	return Key{Code: KeyUnknown}, nil
}

// readCSI decodes ESC [ ... — the parameters, then the final byte. The
// modifier parameter is xterm's: 1 plus a bitmask of shift(1), alt(2)
// and ctrl(4), so 5 is Ctrl, 3 is Alt and 7 is both.
func (k *keyReader) readCSI() (Key, error) {
	var params []byte
	for {
		b, err := k.r.ReadByte()
		if err != nil {
			return Key{}, err
		}
		if b >= 0x40 && b <= 0x7e { // final byte
			return csiKey(params, b), nil
		}
		params = append(params, b)
		if len(params) > 32 { // a sequence this long is not one of ours
			return Key{Code: KeyUnknown}, nil
		}
	}
}

func csiKey(params []byte, final byte) Key {
	mod := csiModifier(params)
	key := Key{Ctrl: mod&4 != 0, Alt: mod&2 != 0}
	switch final {
	case 'A', 'B', 'C', 'D':
		key.Code = arrowCode(final)
		return key
	case 'H':
		key.Code = KeyHome
		return key
	case 'F':
		key.Code = KeyEnd
		return key
	case '~':
		switch csiFirstParam(params) {
		case 1, 7:
			key.Code = KeyHome
		case 3:
			key.Code = KeyDelete
		case 4, 8:
			key.Code = KeyEnd
		case 200:
			key.Code = KeyPasteStart
		case 201:
			key.Code = KeyPasteEnd
		default:
			key.Code = KeyUnknown
		}
		return key
	}
	return Key{Code: KeyUnknown}
}

// csiFirstParam is the numeric parameter before any ';'.
func csiFirstParam(params []byte) int {
	n, seen := 0, false
	for _, c := range params {
		if c == ';' {
			break
		}
		if c < '0' || c > '9' {
			return -1
		}
		n, seen = n*10+int(c-'0'), true
	}
	if !seen {
		return -1
	}
	return n
}

// csiModifier is the xterm modifier parameter minus one (a bitmask of
// shift=1, alt=2, ctrl=4), or 0 when the sequence carries none.
func csiModifier(params []byte) int {
	i := -1
	for j, c := range params {
		if c == ';' {
			i = j
			break
		}
	}
	if i < 0 {
		return 0
	}
	n, seen := 0, false
	for _, c := range params[i+1:] {
		if c < '0' || c > '9' {
			break
		}
		n, seen = n*10+int(c-'0'), true
	}
	if !seen || n < 1 {
		return 0
	}
	return n - 1
}

func arrowCode(final byte) KeyCode {
	switch final {
	case 'A':
		return KeyUp
	case 'B':
		return KeyDown
	case 'C':
		return KeyRight
	case 'D':
		return KeyLeft
	}
	return KeyUnknown
}

// WordMotion reports whether the key means "by a word" rather than "by a
// character": Ctrl+arrow, Alt+arrow, or Alt-b / Alt-f.
func (k Key) WordMotion() bool { return k.Ctrl || k.Alt }
