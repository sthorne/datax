package cli

import (
	"errors"
	"io"
	"strings"
	"testing"
)

// The escape sequences a terminal actually sends, written out so a test
// failure names the key rather than a byte string.
const (
	seqLeft        = "\x1b[D"
	seqRight       = "\x1b[C"
	seqUp          = "\x1b[A"
	seqDown        = "\x1b[B"
	seqCtrlLeft    = "\x1b[1;5D" // xterm, iTerm2, GNOME Terminal, Windows Terminal
	seqCtrlRight   = "\x1b[1;5C"
	seqAltLeft     = "\x1b[1;3D"
	seqAltRight    = "\x1b[1;3C"
	seqCtrlAltLeft = "\x1b[1;7D"
	seqAppLeft     = "\x1bOD" // application cursor mode
	seqHome        = "\x1b[H"
	seqEnd         = "\x1b[F"
	seqDelete      = "\x1b[3~"
	seqCtrlDelete  = "\x1b[3;5~"
	seqAltB        = "\x1bb"
	seqAltF        = "\x1bf"
	seqPasteOn     = "\x1b[200~"
	seqPasteOff    = "\x1b[201~"
	seqBackspace   = "\x7f"
	seqEnter       = "\r"
	ctrlA          = "\x01"
	ctrlC          = "\x03"
	ctrlD          = "\x04"
	ctrlE          = "\x05"
	ctrlK          = "\x0b"
	ctrlU          = "\x15"
	ctrlW          = "\x17"
)

// runEditorRaw feeds input to an editor and returns it with whatever
// ReadStatement returned.
func runEditorRaw(t *testing.T, input string, setup func(*Editor)) (*Editor, string, error) {
	t.Helper()
	e := NewEditor(strings.NewReader(input), io.Discard)
	e.Complete = func(s string) bool { return strings.HasSuffix(strings.TrimSpace(s), ";") }
	if setup != nil {
		setup(e)
	}
	text, err := e.ReadStatement()
	return e, text, err
}

// runEditor is runEditorRaw for the tests that inspect the buffer after
// the keys run out: input ending is not a failure there, it is the end
// of the keystrokes the test supplied.
func runEditor(t *testing.T, input string, setup func(*Editor)) (*Editor, string, error) {
	t.Helper()
	e, text, err := runEditorRaw(t, input, setup)
	if errors.Is(err, io.EOF) {
		err = nil
	}
	return e, text, err
}

// TestEditorJoinsLinesOnBackspace is the reported bug: with the old
// single-line editor, backspace at the start of a continuation line had
// nowhere to go and the cursor stopped. It must join the two lines and
// leave the cursor where they met.
func TestEditorJoinsLinesOnBackspace(t *testing.T) {
	e, _, err := runEditor(t, "SELECT 1\nFROM t"+seqHome+seqBackspace, nil)
	if err != nil {
		t.Fatal(err)
	}
	if got := e.Text(); got != "SELECT 1FROM t" {
		t.Fatalf("after joining: %q", got)
	}
	row, col := e.Cursor()
	if row != 0 || col != len("SELECT 1") {
		t.Fatalf("cursor at %d,%d; want 0,%d — the join point", row, col, len("SELECT 1"))
	}
	// And again from the start of the (now only) line: nothing to join,
	// nothing removed.
	e2, _, err := runEditor(t, "abc"+seqHome+seqBackspace, nil)
	if err != nil {
		t.Fatal(err)
	}
	if e2.Text() != "abc" {
		t.Fatalf("backspace at the very start deleted %q", e2.Text())
	}
}

// TestEditorDeleteAcrossLines: forward delete at the end of a line pulls
// the next one up, the mirror of the join above.
func TestEditorDeleteAcrossLines(t *testing.T) {
	e, _, err := runEditor(t, "one\ntwo"+seqUp+ctrlE+seqDelete, nil)
	if err != nil {
		t.Fatal(err)
	}
	if got := e.Text(); got != "onetwo" {
		t.Fatalf("after pulling the next line up: %q", got)
	}
	row, col := e.Cursor()
	if row != 0 || col != 3 {
		t.Fatalf("cursor at %d,%d; want 0,3", row, col)
	}
}

// TestEditorWordMotion: every spelling of "by a word" a terminal sends
// moves by a word, and a SQL identifier is one word per part.
func TestEditorWordMotion(t *testing.T) {
	const line = "SELECT customer_id FROM orders"
	for _, tc := range []struct {
		name string
		seq  string
		want int
	}{
		{"ctrl+left", seqCtrlLeft, len("SELECT customer_id FROM ")},
		{"alt+left", seqAltLeft, len("SELECT customer_id FROM ")},
		{"ctrl+alt+left", seqCtrlAltLeft, len("SELECT customer_id FROM ")},
		{"alt-b", seqAltB, len("SELECT customer_id FROM ")},
		{"twice", seqCtrlLeft + seqCtrlLeft, len("SELECT customer_id ")},
		{"three times, into the identifier", seqCtrlLeft + seqCtrlLeft + seqCtrlLeft, len("SELECT ")},
	} {
		t.Run(tc.name, func(t *testing.T) {
			e, _, err := runEditor(t, line+tc.seq, nil)
			if err != nil {
				t.Fatal(err)
			}
			if _, col := e.Cursor(); col != tc.want {
				t.Fatalf("cursor at %d, want %d (%q)", col, tc.want, line[:tc.want])
			}
		})
	}
	// Rightwards, from the start.
	e, _, err := runEditor(t, line+seqHome+seqCtrlRight+seqCtrlRight, nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, col := e.Cursor(); col != len("SELECT customer_id") {
		t.Fatalf("two words right: cursor at %d, want %d", col, len("SELECT customer_id"))
	}
	// Alt-f and the ;3 form agree with it.
	for _, seq := range []string{seqAltF, seqAltRight} {
		e, _, err := runEditor(t, line+seqHome+seq, nil)
		if err != nil {
			t.Fatal(err)
		}
		if _, col := e.Cursor(); col != len("SELECT") {
			t.Fatalf("%q: cursor at %d, want %d", seq, col, len("SELECT"))
		}
	}
}

// TestEditorWordMotionCrossesLines: at the edge of a line, a word jump
// carries on into the neighbouring line rather than stopping.
func TestEditorWordMotionCrossesLines(t *testing.T) {
	e, _, err := runEditor(t, "one two\nthree"+seqHome+seqCtrlLeft, nil)
	if err != nil {
		t.Fatal(err)
	}
	row, col := e.Cursor()
	if row != 0 || col != len("one two") {
		t.Fatalf("cursor at %d,%d; want the end of line 0 (%d)", row, col, len("one two"))
	}
}

// TestEditorWordDelete: Ctrl-W, Alt-Backspace and Ctrl-Delete remove a
// word rather than a character, and at a line edge fall back to the join.
func TestEditorWordDelete(t *testing.T) {
	e, _, err := runEditor(t, "SELECT customer_id"+ctrlW, nil)
	if err != nil {
		t.Fatal(err)
	}
	if got := e.Text(); got != "SELECT " {
		t.Fatalf("ctrl-w left %q", got)
	}
	// Forward word-delete from the start removes the first word only.
	e, _, err = runEditor(t, "SELECT a, b"+seqHome+seqCtrlDelete, nil)
	if err != nil {
		t.Fatal(err)
	}
	if got := e.Text(); got != " a, b" {
		t.Fatalf("ctrl-delete left %q", got)
	}
	// Word-delete at the start of a continuation line joins, like plain
	// backspace does.
	e, _, err = runEditor(t, "one\ntwo"+seqHome+ctrlW, nil)
	if err != nil {
		t.Fatal(err)
	}
	if got := e.Text(); got != "onetwo" {
		t.Fatalf("word-delete at a line start left %q", got)
	}
}

// TestEditorSubmitsOnlyWhenComplete: Enter opens a new line until the
// shell's rule says the statement is done, which is what makes editing a
// multi-line statement possible at all.
func TestEditorSubmitsOnlyWhenComplete(t *testing.T) {
	_, text, err := runEditor(t, "SELECT 1\nFROM t\nWHERE x = 1;\r", nil)
	if err != nil {
		t.Fatal(err)
	}
	if text != "SELECT 1\nFROM t\nWHERE x = 1;" {
		t.Fatalf("submitted %q", text)
	}
	// Enter in the middle of the buffer splits the line rather than
	// submitting.
	e, _, err := runEditor(t, "SELECT FROM"+seqHome+seqCtrlRight+seqEnter, nil)
	if err != nil {
		t.Fatal(err)
	}
	if got := e.Text(); got != "SELECT\n FROM" {
		t.Fatalf("Enter mid-buffer gave %q", got)
	}
	if row, col := e.Cursor(); row != 1 || col != 0 {
		t.Fatalf("after splitting a line the cursor is at %d,%d; want 1,0", row, col)
	}
}

// TestEditorPasteDoesNotSubmit: a pasted multi-line statement arrives
// between bracketed-paste markers and must land as text, even though it
// contains newlines and a semicolon.
func TestEditorPasteDoesNotSubmit(t *testing.T) {
	e, _, err := runEditor(t, seqPasteOn+"SELECT 1;\nSELECT 2;"+seqPasteOff, nil)
	if err != nil {
		t.Fatal(err)
	}
	if got := e.Text(); got != "SELECT 1;\nSELECT 2;" {
		t.Fatalf("paste landed as %q", got)
	}
}

// TestEditorInterruptAndEOF: Ctrl-C abandons the statement, Ctrl-D on an
// empty buffer ends the session, and Ctrl-D with text deletes forward.
func TestEditorInterruptAndEOF(t *testing.T) {
	if _, _, err := runEditorRaw(t, "SELECT 1"+ctrlC, nil); !errors.Is(err, ErrInterrupted) {
		t.Fatalf("ctrl-c: %v", err)
	}
	if _, _, err := runEditorRaw(t, ctrlD, nil); !errors.Is(err, io.EOF) {
		t.Fatalf("ctrl-d on an empty buffer: %v", err)
	}
	e, _, err := runEditor(t, "ab"+seqHome+ctrlD, nil)
	if err != nil {
		t.Fatal(err)
	}
	if e.Text() != "b" {
		t.Fatalf("ctrl-d with text left %q", e.Text())
	}
}

// TestEditorLineMotion: Up and Down move between the buffer's lines
// before they reach for history, and Left/Right wrap across lines.
func TestEditorLineMotion(t *testing.T) {
	e, _, err := runEditor(t, "one\ntwo\nthree"+seqUp+seqUp, nil)
	if err != nil {
		t.Fatal(err)
	}
	if row, _ := e.Cursor(); row != 0 {
		t.Fatalf("two ups from line 2 landed on line %d", row)
	}
	e, _, err = runEditor(t, "ab\ncd"+seqHome+seqLeft, nil)
	if err != nil {
		t.Fatal(err)
	}
	row, col := e.Cursor()
	if row != 0 || col != 2 {
		t.Fatalf("left at column 0 landed at %d,%d; want the end of line 0", row, col)
	}
	e, _, err = runEditor(t, "ab\ncd"+seqUp+ctrlE+seqRight, nil)
	if err != nil {
		t.Fatal(err)
	}
	row, col = e.Cursor()
	if row != 1 || col != 0 {
		t.Fatalf("right at the end of line 0 landed at %d,%d; want 1,0", row, col)
	}
}

// TestEditorHistory: Up past the first line recalls, Down comes back to
// what was being typed, and a submitted statement is recorded.
func TestEditorHistory(t *testing.T) {
	h := &History{}
	h.Add("SELECT 1;")
	h.Add("SELECT 2;")
	e, _, err := runEditor(t, "half"+seqUp, func(e *Editor) { e.History = h })
	if err != nil {
		t.Fatal(err)
	}
	if got := e.Text(); got != "SELECT 2;" {
		t.Fatalf("up recalled %q", got)
	}
	e, _, err = runEditor(t, "half"+seqUp+seqUp, func(e *Editor) { e.History = h })
	if err != nil {
		t.Fatal(err)
	}
	if got := e.Text(); got != "SELECT 1;" {
		t.Fatalf("two ups recalled %q", got)
	}
	e, _, err = runEditor(t, "half"+seqUp+seqDown, func(e *Editor) { e.History = h })
	if err != nil {
		t.Fatal(err)
	}
	if got := e.Text(); got != "half" {
		t.Fatalf("down did not restore the buffer being typed: %q", got)
	}
	_, text, err := runEditor(t, "SELECT 3;\r", func(e *Editor) { e.History = h })
	if err != nil || text != "SELECT 3;" {
		t.Fatalf("submit: %q %v", text, err)
	}
	if h.At(0) != "SELECT 3;" {
		t.Fatalf("history did not record the statement: %q", h.At(0))
	}
}

// TestEditorKillLine: Ctrl-K and Ctrl-U cut to the ends of the line, and
// Ctrl-K at the end of a line pulls the next one up.
func TestEditorKillLine(t *testing.T) {
	e, _, err := runEditor(t, "SELECT 1 FROM t"+seqHome+seqCtrlRight+ctrlK, nil)
	if err != nil {
		t.Fatal(err)
	}
	if got := e.Text(); got != "SELECT" {
		t.Fatalf("ctrl-k left %q", got)
	}
	e, _, err = runEditor(t, "SELECT 1"+ctrlU, nil)
	if err != nil {
		t.Fatal(err)
	}
	if got := e.Text(); got != "" {
		t.Fatalf("ctrl-u left %q", got)
	}
	e, _, err = runEditor(t, "one\ntwo"+seqUp+ctrlE+ctrlK, nil)
	if err != nil {
		t.Fatal(err)
	}
	if got := e.Text(); got != "onetwo" {
		t.Fatalf("ctrl-k at a line end left %q", got)
	}
}

// TestEditorUnknownSequencesAreIgnored: a function key or a mouse report
// must not end up as text in the statement.
func TestEditorUnknownSequencesAreIgnored(t *testing.T) {
	e, _, err := runEditor(t, "SEL\x1b[15~\x1b[?1000hECT", nil)
	if err != nil {
		t.Fatal(err)
	}
	if got := e.Text(); got != "SELECT" {
		t.Fatalf("an unknown sequence landed in the buffer: %q", got)
	}
}

// TestEditorRedrawsInPlace: the drawn output moves the cursor back over
// the block it last drew and clears it, rather than leaving the previous
// state on screen.
func TestEditorRedrawsInPlace(t *testing.T) {
	var out strings.Builder
	e := NewEditor(strings.NewReader("ab\ncd"), &out)
	e.Complete = func(string) bool { return false }
	_, _ = e.ReadStatement()
	s := out.String()
	if !strings.Contains(s, "\x1b[0J") {
		t.Fatal("the redraw never clears to the end of the screen")
	}
	if !strings.Contains(s, "\x1b[1A") {
		t.Fatal("the redraw never moves back up over a two-line block")
	}
	if !strings.Contains(s, "datax> ") || !strings.Contains(s, "    -> ") {
		t.Fatalf("the prompts are not both drawn: %q", s)
	}
}

// TestEditorWrapsWideLines: a line longer than the terminal occupies
// more than one row, and the cursor accounting follows it.
func TestEditorWrapsWideLines(t *testing.T) {
	var out strings.Builder
	e := NewEditor(strings.NewReader(strings.Repeat("x", 45)), &out)
	e.Width = 20
	e.Complete = func(string) bool { return false }
	_, _ = e.ReadStatement()
	// 7 (prompt) + 45 runes over a 20-column terminal is three rows, so
	// once the text passes two rows a redraw has to climb back over two.
	if !strings.Contains(out.String(), "\x1b[2A") {
		t.Fatalf("a wrapped line was not accounted for: %q", out.String())
	}
}
