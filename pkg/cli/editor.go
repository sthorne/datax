package cli

import (
	"errors"
	"fmt"
	"io"
	"strings"
)

// The SQL shell's line editor (issue #175). The whole statement is the
// editor's buffer, not one physical line at a time, which is what makes
// backspace at the start of a continuation line join it to the line
// above instead of stopping dead — the shell used to compose a statement
// by calling a single-line editor once per line, so the previous line
// was already gone by the time the cursor reached column 0.
//
// It reads decoded keystrokes (keys.go) and writes the redraw to a
// Writer, so a test drives it with bytes and reads back what a terminal
// would have shown, with no terminal involved.

// ErrInterrupted is returned when the user abandons the statement with
// Ctrl-C. The shell discards the buffer and prompts afresh.
var ErrInterrupted = errors.New("interrupted")

// Editor edits one statement across as many lines as it takes.
type Editor struct {
	keys *keyReader
	out  io.Writer

	// Prompt opens the first line, Cont every line after it. They should
	// be the same width so the text lines up.
	Prompt, Cont string
	// Width is the terminal's, for wrapping; 0 means assume 80.
	Width int
	// Complete decides whether Enter runs the statement or opens a new
	// line. The shell owns that rule (a trailing semicolon, or a
	// meta-command), not the editor.
	Complete func(text string) bool
	// History, when set, is recalled with Up and Down past the ends of
	// the buffer and appended to on submit.
	History *History

	lines []string // the buffer, one entry per line, never empty
	row   int      // cursor line
	col   int      // cursor column, in runes, within lines[row]

	cursorRow int  // where the cursor sits in the block last drawn
	drawnRows int  // how many rows that block occupied
	pasting   bool // inside a bracketed paste: newlines are text

	histIdx  int      // 0 = the buffer being typed, 1.. = older entries
	histSave []string // the buffer stashed while browsing history
}

// NewEditor returns an editor reading keystrokes from in and drawing to
// out.
func NewEditor(in io.Reader, out io.Writer) *Editor {
	return &Editor{
		keys:     newKeyReader(in),
		out:      out,
		Prompt:   "datax> ",
		Cont:     "    -> ",
		Complete: func(string) bool { return true },
		lines:    []string{""},
	}
}

// Text is the buffer's contents.
func (e *Editor) Text() string { return strings.Join(e.lines, "\n") }

// SetText replaces the buffer and puts the cursor at its end (used by
// history recall, and by tests).
func (e *Editor) SetText(s string) {
	e.lines = strings.Split(s, "\n")
	if len(e.lines) == 0 {
		e.lines = []string{""}
	}
	e.row = len(e.lines) - 1
	e.col = len([]rune(e.lines[e.row]))
}

// Cursor is the cursor's line and column, in runes.
func (e *Editor) Cursor() (row, col int) { return e.row, e.col }

// ReadStatement edits until the user submits a complete statement,
// abandons it (ErrInterrupted) or ends the input (io.EOF).
func (e *Editor) ReadStatement() (string, error) {
	e.lines = []string{""}
	e.row, e.col = 0, 0
	e.histIdx, e.histSave = 0, nil
	e.cursorRow, e.drawnRows = 0, 0
	e.draw()
	for {
		k, err := e.keys.ReadKey()
		if err != nil {
			if errors.Is(err, io.EOF) && e.Text() == "" {
				e.endBlock()
				return "", io.EOF
			}
			if errors.Is(err, io.EOF) {
				e.endBlock()
				return "", io.EOF
			}
			return "", err
		}
		done, text, err := e.handle(k)
		if err != nil {
			return "", err
		}
		if done {
			return text, nil
		}
		e.draw()
	}
}

// handle applies one keystroke. It returns done when the statement is
// finished, with the text to run.
func (e *Editor) handle(k Key) (bool, string, error) {
	// Inside a bracketed paste every byte is text, including newlines:
	// pasting a multi-line statement must not run it a line at a time.
	if e.pasting {
		switch k.Code {
		case KeyPasteEnd:
			e.pasting = false
		case KeyEnter:
			e.insertNewline()
		case KeyRune:
			e.insertRune(k.Rune)
		case KeyTab:
			e.insertRune(' ')
		}
		return false, "", nil
	}
	switch k.Code {
	case KeyPasteStart:
		e.pasting = true
	case KeyRune:
		e.insertRune(k.Rune)
	case KeyTab:
		e.insertRune(' ')
	case KeyEnter:
		text := e.Text()
		if strings.TrimSpace(text) != "" && e.Complete(text) {
			e.endBlock()
			if e.History != nil {
				e.History.Add(text)
			}
			return true, text, nil
		}
		e.insertNewline()
	case KeyBackspace:
		if k.WordMotion() {
			e.deleteWordLeft()
		} else {
			e.backspace()
		}
	case KeyDelete:
		if k.WordMotion() {
			e.deleteWordRight()
		} else {
			e.deleteForward()
		}
	case KeyKillWord:
		e.deleteWordLeft()
	case KeyLeft:
		if k.WordMotion() {
			e.moveWordLeft()
		} else {
			e.moveLeft()
		}
	case KeyRight:
		if k.WordMotion() {
			e.moveWordRight()
		} else {
			e.moveRight()
		}
	case KeyUp:
		e.moveUp()
	case KeyDown:
		e.moveDown()
	case KeyHome:
		e.col = 0
	case KeyEnd:
		e.col = len(e.runes())
	case KeyKillToEnd:
		r := e.runes()
		if e.col < len(r) {
			e.setLine(string(r[:e.col]))
		} else if e.row < len(e.lines)-1 {
			e.joinNext()
		}
	case KeyKillToStart:
		r := e.runes()
		e.setLine(string(r[e.col:]))
		e.col = 0
	case KeyClearScreen:
		// Clear the screen and redraw the buffer at the top.
		fmt.Fprint(e.out, "\x1b[2J\x1b[H")
		e.cursorRow, e.drawnRows = 0, 0
	case KeyInterrupt:
		e.endBlock()
		return false, "", ErrInterrupted
	case KeyEOF:
		if e.Text() == "" {
			e.endBlock()
			return false, "", io.EOF
		}
		e.deleteForward()
	}
	return false, "", nil
}

// ---- buffer edits ----

func (e *Editor) runes() []rune     { return []rune(e.lines[e.row]) }
func (e *Editor) setLine(s string)  { e.lines[e.row] = s }
func (e *Editor) lineLen(i int) int { return len([]rune(e.lines[i])) }

func (e *Editor) insertRune(r rune) {
	line := e.runes()
	out := make([]rune, 0, len(line)+1)
	out = append(out, line[:e.col]...)
	out = append(out, r)
	out = append(out, line[e.col:]...)
	e.setLine(string(out))
	e.col++
}

func (e *Editor) insertNewline() {
	line := e.runes()
	head, tail := string(line[:e.col]), string(line[e.col:])
	e.setLine(head)
	rest := append([]string{tail}, e.lines[e.row+1:]...)
	e.lines = append(e.lines[:e.row+1], rest...)
	e.row++
	e.col = 0
}

// backspace deletes the rune before the cursor, or — at the start of a
// line other than the first — joins this line to the one above and puts
// the cursor where they met. That join is the bug this editor exists to
// fix.
func (e *Editor) backspace() {
	if e.col > 0 {
		line := e.runes()
		e.setLine(string(line[:e.col-1]) + string(line[e.col:]))
		e.col--
		return
	}
	if e.row == 0 {
		return
	}
	e.row--
	e.col = e.lineLen(e.row)
	e.joinNext()
}

// deleteForward deletes the rune under the cursor, or pulls the next
// line up when the cursor is at the end of this one.
func (e *Editor) deleteForward() {
	line := e.runes()
	if e.col < len(line) {
		e.setLine(string(line[:e.col]) + string(line[e.col+1:]))
		return
	}
	if e.row < len(e.lines)-1 {
		e.joinNext()
	}
}

// joinNext appends the following line to the current one.
func (e *Editor) joinNext() {
	e.lines[e.row] += e.lines[e.row+1]
	e.lines = append(e.lines[:e.row+1], e.lines[e.row+2:]...)
}

func (e *Editor) moveLeft() {
	if e.col > 0 {
		e.col--
		return
	}
	if e.row > 0 {
		e.row--
		e.col = e.lineLen(e.row)
	}
}

func (e *Editor) moveRight() {
	if e.col < len(e.runes()) {
		e.col++
		return
	}
	if e.row < len(e.lines)-1 {
		e.row++
		e.col = 0
	}
}

// moveUp moves to the line above, or recalls an older history entry when
// there is no line above.
func (e *Editor) moveUp() {
	if e.row > 0 {
		e.row--
		e.col = min(e.col, e.lineLen(e.row))
		return
	}
	e.historyStep(1)
}

// moveDown moves to the line below, or walks back down the history.
func (e *Editor) moveDown() {
	if e.row < len(e.lines)-1 {
		e.row++
		e.col = min(e.col, e.lineLen(e.row))
		return
	}
	e.historyStep(-1)
}

func (e *Editor) historyStep(delta int) {
	if e.History == nil {
		return
	}
	idx := e.histIdx + delta
	if idx < 0 {
		return
	}
	if idx == 0 {
		e.histIdx = 0
		if e.histSave != nil {
			e.lines = e.histSave
			e.histSave = nil
		} else {
			e.lines = []string{""}
		}
		e.row = len(e.lines) - 1
		e.col = e.lineLen(e.row)
		return
	}
	if idx-1 >= e.History.Len() {
		return
	}
	entry := e.History.At(idx - 1)
	if e.histIdx == 0 {
		e.histSave = append([]string(nil), e.lines...)
	}
	e.histIdx = idx
	e.SetText(entry)
}

// ---- word motion ----
//
// A word is a run of letters, digits and underscores — the shape of a
// SQL identifier, so Ctrl+Left from the end of `orders.customer_id`
// stops at `customer_id` and then at `orders`.

func isWordRune(r rune) bool {
	return r == '_' ||
		(r >= '0' && r <= '9') ||
		(r >= 'a' && r <= 'z') ||
		(r >= 'A' && r <= 'Z') ||
		r > 127
}

// wordLeft is the column the cursor lands on moving one word left
// within the current line, or -1 when it is already at the start.
func (e *Editor) wordLeft() int {
	line := e.runes()
	i := e.col
	if i == 0 {
		return -1
	}
	for i > 0 && !isWordRune(line[i-1]) {
		i--
	}
	for i > 0 && isWordRune(line[i-1]) {
		i--
	}
	return i
}

func (e *Editor) wordRight() int {
	line := e.runes()
	i := e.col
	if i >= len(line) {
		return -1
	}
	for i < len(line) && !isWordRune(line[i]) {
		i++
	}
	for i < len(line) && isWordRune(line[i]) {
		i++
	}
	return i
}

func (e *Editor) moveWordLeft() {
	if i := e.wordLeft(); i >= 0 {
		e.col = i
		return
	}
	if e.row > 0 { // at the start of a line: over to the end of the one above
		e.row--
		e.col = e.lineLen(e.row)
	}
}

func (e *Editor) moveWordRight() {
	if i := e.wordRight(); i >= 0 {
		e.col = i
		return
	}
	if e.row < len(e.lines)-1 {
		e.row++
		e.col = 0
	}
}

func (e *Editor) deleteWordLeft() {
	if i := e.wordLeft(); i >= 0 {
		line := e.runes()
		e.setLine(string(line[:i]) + string(line[e.col:]))
		e.col = i
		return
	}
	e.backspace() // at the start of a line: join to the one above
}

func (e *Editor) deleteWordRight() {
	if i := e.wordRight(); i >= 0 {
		line := e.runes()
		e.setLine(string(line[:e.col]) + string(line[i:]))
		return
	}
	e.deleteForward()
}

// ---- drawing ----

func (e *Editor) width() int {
	if e.Width > 0 {
		return e.Width
	}
	return 80
}

// promptFor is the prompt that opens line i.
func (e *Editor) promptFor(i int) string {
	if i == 0 {
		return e.Prompt
	}
	return e.Cont
}

// rowsFor is how many terminal rows line i occupies, prompt included.
func (e *Editor) rowsFor(i int) int {
	n := len([]rune(e.promptFor(i))) + e.lineLen(i)
	w := e.width()
	if n == 0 {
		return 1
	}
	return (n + w - 1) / w
}

// draw repaints the buffer in place: back to the top of the block the
// last draw left, clear to the end of the screen, write it again, then
// put the cursor where it belongs. Redrawing whole is what keeps a
// wrapped line, a joined line and a history recall all correct without a
// separate case for each.
func (e *Editor) draw() {
	var b strings.Builder
	b.WriteString("\r")
	if e.cursorRow > 0 {
		fmt.Fprintf(&b, "\x1b[%dA", e.cursorRow)
	}
	b.WriteString("\x1b[0J") // clear from the cursor to the end of the screen
	rows := 0
	for i := range e.lines {
		if i > 0 {
			b.WriteString("\r\n")
		}
		b.WriteString(e.promptFor(i))
		b.WriteString(e.lines[i])
		rows += e.rowsFor(i)
	}
	// Where the cursor should end up, in rows from the top of the block
	// and columns from the left edge.
	target := 0
	for i := 0; i < e.row; i++ {
		target += e.rowsFor(i)
	}
	cur := len([]rune(e.promptFor(e.row))) + e.col
	target += cur / e.width()
	col := cur % e.width()
	// The cursor is currently at the end of the last line drawn.
	last := rows - 1
	if last > target {
		fmt.Fprintf(&b, "\x1b[%dA", last-target)
	}
	b.WriteString("\r")
	if col > 0 {
		fmt.Fprintf(&b, "\x1b[%dC", col)
	}
	e.cursorRow, e.drawnRows = target, rows
	fmt.Fprint(e.out, b.String())
}

// endBlock moves past the drawn block so whatever prints next starts on
// a clean line.
func (e *Editor) endBlock() {
	var b strings.Builder
	if down := e.drawnRows - 1 - e.cursorRow; down > 0 {
		fmt.Fprintf(&b, "\x1b[%dB", down)
	}
	b.WriteString("\r\n")
	e.cursorRow, e.drawnRows = 0, 0
	fmt.Fprint(e.out, b.String())
}
