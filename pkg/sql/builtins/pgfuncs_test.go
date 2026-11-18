package builtins

import (
	"errors"
	"math"
	"strings"
	"testing"

	"github.com/sthorne/datax/pkg/sql/types"
)

// callCase is one builtin call: the function, its arguments, and the
// rendered result (NULL for NULL) or the SQLSTATE it raises.
type callCase struct {
	fn   string
	args []types.Datum
	want string
	code string
}

func n(v int64) types.Datum    { return types.NewInt(v) }
func fl(v float64) types.Datum { return types.NewFloat(v) }
func dec(v string) types.Datum { return types.NewDecimal(v) }
func by(v string) types.Datum  { return types.NewBytes([]byte(v)) }

func runCallCases(t *testing.T, cases []callCase) {
	t.Helper()
	for _, c := range cases {
		b, ok := Lookup(c.fn)
		if !ok {
			t.Errorf("%s: not registered", c.fn)
			continue
		}
		d, err := b.Call(c.args)
		label := c.fn + "(" + familiesText(c.args) + ")"
		if c.code != "" {
			var be *Error
			if !errors.As(err, &be) || be.Code != c.code {
				t.Errorf("%s %v: %v, want %s", label, c.args, err, c.code)
			}
			continue
		}
		if err != nil {
			t.Errorf("%s %v: %v", label, c.args, err)
			continue
		}
		got := "NULL"
		if !d.Null {
			got = d.Text()
		}
		if got != c.want {
			t.Errorf("%s %v = %q, want %q", label, c.args, got, c.want)
		}
	}
}

// TestRegexLeftovers (PG-F-001): regexp_instr, the substring pattern
// forms, and the refusal of back references and lookaround.
func TestRegexLeftovers(t *testing.T) {
	runCallCases(t, []callCase{
		{fn: "regexp_instr", args: []types.Datum{s("number of your street, town zip, FR"), s("[^,]+"), n(1), n(2)}, want: "23"},
		{fn: "regexp_instr", args: []types.Datum{s("ABCDEFGHI"), s("(c..)(...)"), n(1), n(1), n(0), s("i"), n(2)}, want: "6"},
		{fn: "regexp_instr", args: []types.Datum{s("abcabc"), s("b")}, want: "2"},
		{fn: "regexp_instr", args: []types.Datum{s("abcabc"), s("b"), n(3)}, want: "5"},
		{fn: "regexp_instr", args: []types.Datum{s("abcabc"), s("bc"), n(1), n(1), n(1)}, want: "4"},
		{fn: "regexp_instr", args: []types.Datum{s("héllo"), s("l+"), n(1), n(1), n(1)}, want: "5"},
		{fn: "regexp_instr", args: []types.Datum{s("abc"), s("z")}, want: "0"},
		{fn: "regexp_instr", args: []types.Datum{s("abc"), s("b"), n(1), n(2)}, want: "0"},
		{fn: "regexp_instr", args: []types.Datum{s("abc"), s("(b)"), n(1), n(1), n(0), s(""), n(3)}, want: "0"},
		{fn: "regexp_instr", args: []types.Datum{s("abc"), s("b"), n(0)}, code: "22023"},
		{fn: "regexp_instr", args: []types.Datum{s("abc"), s("b"), n(1), n(1), n(2)}, code: "22023"},
		{fn: "regexp_instr", args: []types.Datum{s("abc"), s("b"), n(1), n(1), n(0), s("g")}, code: "22023"},
		{fn: "regexp_instr", args: []types.Datum{types.DNull, s("b")}, want: "NULL"},
		// substring(s FROM pattern): POSIX, the first group or the match.
		{fn: "substring", args: []types.Datum{s("Thomas"), s("...$")}, want: "mas"},
		{fn: "substring", args: []types.Datum{s("foobar"), s("o(.)b")}, want: "o"},
		{fn: "substring", args: []types.Datum{s("foobar"), s("z")}, want: "NULL"},
		{fn: "substring", args: []types.Datum{s("foobar"), s("(z)|b")}, want: "NULL"},
		{fn: "substring", args: []types.Datum{s("Thomas"), n(2), n(3)}, want: "hom"},
		// substring(s SIMILAR pattern ESCAPE e).
		{fn: "substring", args: []types.Datum{s("Thomas"), s(`%#"o_a#"_`), s("#")}, want: "oma"},
		{fn: "substring", args: []types.Datum{s("foobar"), s(`%#"o_b#"%`), s("#")}, want: "oob"},
		{fn: "substring", args: []types.Datum{s("foobar"), s(`#"o_b#"%`), s("#")}, want: "NULL"},
		{fn: "substring", args: []types.Datum{s("foobar"), s(`f(o)*bar`), s("#")}, want: "foobar"},
		{fn: "substring", args: []types.Datum{s("a.c"), s(`a.c`), s("")}, want: "a.c"},
		{fn: "substring", args: []types.Datum{s("abc"), s(`a.c`), s("")}, want: "NULL"},
		{fn: "substring", args: []types.Datum{s("abc"), s(`#"a#"#"b#"c`), s("#")}, code: "2200C"},
		{fn: "substring", args: []types.Datum{s("abc"), s(`a`), s("##")}, code: "22025"},
		{fn: "substring", args: []types.Datum{s("abc"), s(`a`), types.DNull}, want: "NULL"},
		// Back references and lookaround: unsupported, not invalid.
		{fn: "regexp_match", args: []types.Datum{s("aa"), s(`(a)\1`)}, code: "0A000"},
		{fn: "regexp_match", args: []types.Datum{s("ab"), s(`a(?=b)`)}, code: "0A000"},
		{fn: "regexp_match", args: []types.Datum{s("ab"), s(`a(?!b)`)}, code: "0A000"},
		{fn: "regexp_match", args: []types.Datum{s("ab"), s(`(?<=a)b`)}, code: "0A000"},
		{fn: "regexp_match", args: []types.Datum{s("ab"), s(`(?<!a)b`)}, code: "0A000"},
		{fn: "regexp_match", args: []types.Datum{s("a(?=b"), s(`[(?=]b`)}, want: "{=b}"},
		{fn: "regexp_match", args: []types.Datum{s("a"), s(`(`)}, code: "2201B"},
	})
	if ok, err := RegexpMatch("a\nb", "a.b", false); err != nil || !ok {
		t.Errorf(`'a\nb' ~ 'a.b': %v %v (. matches a newline by default)`, ok, err)
	}
	if ok, err := RegexpMatch("x HELLO", `\mhello\M`, true); err != nil || !ok {
		t.Errorf(`~* with \m \M: %v %v`, ok, err)
	}
}

// TestStringFunctions (PG-F-009).
func TestStringFunctions(t *testing.T) {
	runCallCases(t, []callCase{
		{fn: "overlay", args: []types.Datum{s("Txxxxas"), s("hom"), n(2), n(4)}, want: "Thomas"},
		{fn: "overlay", args: []types.Datum{s("Txxxxas"), s("hom"), n(2)}, want: "Thomxas"},
		{fn: "overlay", args: []types.Datum{s("abc"), s("XY"), n(5)}, want: "abcXY"},
		{fn: "overlay", args: []types.Datum{s("abc"), s("X"), n(2), n(-1)}, want: "aXabc"},
		{fn: "overlay", args: []types.Datum{s("héllo"), s("E"), n(2), n(1)}, want: "hEllo"},
		{fn: "overlay", args: []types.Datum{s("abc"), s("X"), n(0)}, code: "22011"},
		{fn: "overlay", args: []types.Datum{by("\x01\x02\x03\x04"), by("\x09\x09"), n(2), n(3)}, want: `\x010909`},
		{fn: "bit_length", args: []types.Datum{s("jose")}, want: "32"},
		{fn: "bit_length", args: []types.Datum{s("é")}, want: "16"},
		{fn: "bit_length", args: []types.Datum{by("\x00\x01")}, want: "16"},
		{fn: "to_ascii", args: []types.Datum{s("Karel")}, code: "0A000"},
		{fn: "to_ascii", args: []types.Datum{by("Kar\xe9l"), s("LATIN1")}, want: "Karel"},
		{fn: "to_ascii", args: []types.Datum{by("\xbc\xbd"), s("latin9")}, want: "Ee"},
		{fn: "to_ascii", args: []types.Datum{by("\xa3\xb3"), s("LATIN2")}, want: "Ll"},
		{fn: "to_ascii", args: []types.Datum{by("\x8a"), n(29)}, want: "S"},
		{fn: "to_ascii", args: []types.Datum{s("x"), s("nonesuch")}, code: "42704"},
		{fn: "to_ascii", args: []types.Datum{s("x"), s("UTF8")}, code: "0A000"},
		{fn: "convert_to", args: []types.Datum{s("café"), s("LATIN1")}, want: `\x636166e9`},
		{fn: "convert_to", args: []types.Datum{s("café"), s("utf-8")}, want: `\x636166c3a9`},
		{fn: "convert_to", args: []types.Datum{s("€"), s("LATIN1")}, code: "22P05"},
		{fn: "convert_to", args: []types.Datum{s("€"), s("WIN1252")}, want: `\x80`},
		{fn: "convert_to", args: []types.Datum{s("x"), s("klingon")}, code: "22023"},
		{fn: "convert_to", args: []types.Datum{s("café"), s("SQL_ASCII")}, want: `\x636166c3a9`},
		{fn: "convert_from", args: []types.Datum{by("caf\xe9"), s("LATIN1")}, want: "café"},
		{fn: "convert_from", args: []types.Datum{by("caf\xc3\xa9"), s("UTF8")}, want: "café"},
		{fn: "convert_from", args: []types.Datum{by("\xff"), s("UTF8")}, code: "22021"},
		{fn: "convert_from", args: []types.Datum{by("\x81"), s("WIN1252")}, code: "22P05"},
		{fn: "convert_from", args: []types.Datum{s(`\x636166e9`), s("LATIN1")}, want: "café"},
		{fn: "convert", args: []types.Datum{by("caf\xc3\xa9"), s("UTF8"), s("LATIN1")}, want: `\x636166e9`},
		{fn: "unistr", args: []types.Datum{s(`d\0061t\+000061`)}, want: "data"},
		{fn: "unistr", args: []types.Datum{s(`dat\U00000061`)}, want: "data"},
		{fn: "unistr", args: []types.Datum{s(`a\\b`)}, want: `a\b`},
		{fn: "unistr", args: []types.Datum{s(`\D83D\DE00`)}, want: "\U0001F600"},
		{fn: "unistr", args: []types.Datum{s(`\u00`)}, code: "42601"},
		{fn: "unistr", args: []types.Datum{s(`\zzzz`)}, code: "42601"},
		{fn: "unistr", args: []types.Datum{s(`\D83Dx`)}, code: "42601"},
		{fn: "unistr", args: []types.Datum{s(`\+110000`)}, code: "22023"},
		{fn: "normalize", args: []types.Datum{s("é")}, want: "é"},
		{fn: "normalize", args: []types.Datum{s("é"), s("NFD")}, want: "é"},
		{fn: "normalize", args: []types.Datum{s("ﬁ"), s("NFKC")}, want: "fi"},
		{fn: "normalize", args: []types.Datum{s("x"), s("NFX")}, code: "22023"},
		{fn: "is_normalized", args: []types.Datum{s("é")}, want: "f"},
		{fn: "is_normalized", args: []types.Datum{s("é"), s("NFD")}, want: "t"},
		{fn: "parse_ident", args: []types.Datum{s(`"SomeSchema".someTable`)}, want: "{SomeSchema,sometable}"},
		{fn: "parse_ident", args: []types.Datum{s(` a . "b""c" `)}, want: `{a,"b\"c"}`},
		{fn: "parse_ident", args: []types.Datum{s(`a.b(x)`)}, code: "22023"},
		{fn: "parse_ident", args: []types.Datum{s(`a.b(x)`), types.NewBool(false)}, want: "{a,b}"},
		{fn: "parse_ident", args: []types.Datum{s(`a.`)}, code: "22023"},
		{fn: "parse_ident", args: []types.Datum{s(`.a`)}, code: "22023"},
		{fn: "parse_ident", args: []types.Datum{s(`"a`)}, code: "22023"},
		{fn: "parse_ident", args: []types.Datum{s(`""`)}, code: "22023"},
		{fn: "sha224", args: []types.Datum{s("abc")}, want: `\x23097d223405d8228642a477bda255b32aadbce4bda0b3f7e36c9da7`},
		{fn: "sha384", args: []types.Datum{by("abc")}, want: `\xcb00753f45a35e8bb5a03d699ac65007272c32ab0eded1631a8b605a43ff5bed8086072ba1e7cc2358baeca134c825a7`},
		{fn: "sha512", args: []types.Datum{s("")}, want: `\xcf83e1357eefb8bdf1542850d66d8007d620e4050b5715dc83f4a921d36ce9ce47d0d13c5d85f2b0ff8318d2877eec2f63b931bd47417a81a538327af927da3e`},
		{fn: "crc32", args: []types.Datum{s("abc")}, want: "891568578"},
		{fn: "crc32c", args: []types.Datum{s("abc")}, want: "910901175"},
		{fn: "get_byte", args: []types.Datum{s(`\x1234567890`), n(4)}, want: "144"},
		{fn: "get_byte", args: []types.Datum{s(`\x1234567890`), n(5)}, code: "2202E"},
		{fn: "set_byte", args: []types.Datum{s(`\x1234567890`), n(4), n(64)}, want: `\x1234567840`},
		{fn: "get_bit", args: []types.Datum{s(`\x1234567890`), n(30)}, want: "1"},
		{fn: "get_bit", args: []types.Datum{s(`\x1234567890`), n(40)}, code: "2202E"},
		{fn: "set_bit", args: []types.Datum{s(`\x1234567890`), n(30), n(0)}, want: `\x1234563890`},
		{fn: "set_bit", args: []types.Datum{s(`\x12`), n(0), n(2)}, code: "22023"},
		// format(): widths and the - flag.
		{fn: "format", args: []types.Datum{s("|%10s|"), s("foo")}, want: "|       foo|"},
		{fn: "format", args: []types.Datum{s("|%-10s|"), s("foo")}, want: "|foo       |"},
		{fn: "format", args: []types.Datum{s("|%*s|"), n(10), s("foo")}, want: "|       foo|"},
		{fn: "format", args: []types.Datum{s("|%*s|"), n(-10), s("foo")}, want: "|foo       |"},
		{fn: "format", args: []types.Datum{s("|%-*s|"), n(10), s("foo")}, want: "|foo       |"},
		{fn: "format", args: []types.Datum{s("|%1$*2$s|"), s("foo"), n(5)}, want: "|  foo|"},
		{fn: "format", args: []types.Datum{s("%3$s, %2$s, %s"), s("a"), s("b"), s("c")}, want: "c, b, c"},
		{fn: "format", args: []types.Datum{s("%2$s %s"), s("a"), s("b"), s("c")}, want: "b c"},
		{fn: "format", args: []types.Datum{s("|%5L|%-6I|"), types.DNull, s("ab")}, want: "| NULL|ab    |"},
		{fn: "format", args: []types.Datum{s("|%3s|"), s("abcdef")}, want: "|abcdef|"},
		{fn: "format", args: []types.Datum{s("%5"), s("x")}, code: "22023"},
		{fn: "format", args: []types.Datum{s("%5d"), s("x")}, code: "22023"},
		{fn: "format", args: []types.Datum{s("%0$s"), s("x")}, code: "22023"},
		{fn: "format", args: []types.Datum{s("%*2s"), s("x")}, code: "22023"},
		{fn: "format", args: []types.Datum{s("%I"), types.DNull}, code: "22004"},
	})
}

// TestMathFunctions (PG-F-010).
func TestMathFunctions(t *testing.T) {
	runCallCases(t, []callCase{
		{fn: "sin", args: []types.Datum{n(0)}, want: "0"},
		{fn: "cos", args: []types.Datum{n(0)}, want: "1"},
		{fn: "sin", args: []types.Datum{n(1)}, want: "0.8414709848078965"},
		{fn: "cos", args: []types.Datum{n(1)}, want: "0.5403023058681398"},
		{fn: "tan", args: []types.Datum{n(0)}, want: "0"},
		{fn: "cot", args: []types.Datum{dec("0.5")}, want: "1.830487721712452"},
		{fn: "cot", args: []types.Datum{n(0)}, want: "Infinity"},
		{fn: "sin", args: []types.Datum{fl(math.Inf(1))}, code: "22003"},
		{fn: "sin", args: []types.Datum{fl(math.NaN())}, want: "NaN"},
		{fn: "asin", args: []types.Datum{n(1)}, want: "1.5707963267948966"},
		{fn: "acos", args: []types.Datum{n(2)}, code: "22003"},
		{fn: "asin", args: []types.Datum{dec("-1.5")}, code: "22003"},
		{fn: "atan", args: []types.Datum{n(1)}, want: "0.7853981633974483"},
		{fn: "atan2", args: []types.Datum{n(1), n(0)}, want: "1.5707963267948966"},
		{fn: "sind", args: []types.Datum{n(30)}, want: "0.5"},
		{fn: "sind", args: []types.Datum{n(-30)}, want: "-0.5"},
		{fn: "sind", args: []types.Datum{n(180)}, want: "0"},
		{fn: "sind", args: []types.Datum{n(390)}, want: "0.5"},
		{fn: "cosd", args: []types.Datum{n(60)}, want: "0.5"},
		{fn: "cosd", args: []types.Datum{n(120)}, want: "-0.5"},
		{fn: "cosd", args: []types.Datum{n(90)}, want: "0"},
		{fn: "tand", args: []types.Datum{n(45)}, want: "1"},
		{fn: "tand", args: []types.Datum{n(135)}, want: "-1"},
		{fn: "tand", args: []types.Datum{n(90)}, want: "Infinity"},
		{fn: "cotd", args: []types.Datum{n(45)}, want: "1"},
		{fn: "cotd", args: []types.Datum{n(0)}, want: "Infinity"},
		{fn: "cosd", args: []types.Datum{fl(math.Inf(-1))}, code: "22003"},
		{fn: "asind", args: []types.Datum{dec("0.5")}, want: "30"},
		{fn: "asind", args: []types.Datum{dec("-0.5")}, want: "-30"},
		{fn: "acosd", args: []types.Datum{dec("0.5")}, want: "60"},
		{fn: "acosd", args: []types.Datum{dec("-0.5")}, want: "120"},
		{fn: "acosd", args: []types.Datum{n(-2)}, code: "22003"},
		{fn: "atand", args: []types.Datum{n(1)}, want: "45"},
		{fn: "atan2d", args: []types.Datum{n(1), n(0)}, want: "90"},
		{fn: "sinh", args: []types.Datum{n(1)}, want: "1.1752011936438014"},
		{fn: "cosh", args: []types.Datum{n(0)}, want: "1"},
		{fn: "cosh", args: []types.Datum{n(1000)}, want: "Infinity"},
		{fn: "tanh", args: []types.Datum{n(1)}, want: "0.7615941559557649"},
		{fn: "asinh", args: []types.Datum{n(1)}, want: "0.881373587019543"},
		{fn: "acosh", args: []types.Datum{n(1)}, want: "0"},
		{fn: "acosh", args: []types.Datum{dec("0.5")}, code: "22003"},
		{fn: "atanh", args: []types.Datum{dec("0.5")}, want: "0.5493061443340548"},
		{fn: "atanh", args: []types.Datum{n(1)}, want: "Infinity"},
		{fn: "atanh", args: []types.Datum{n(2)}, code: "22003"},
		{fn: "degrees", args: []types.Datum{dec("0.5")}, want: "28.64788975654116"},
		{fn: "radians", args: []types.Datum{n(45)}, want: "0.7853981633974483"},
		{fn: "erf", args: []types.Datum{n(1)}, want: "0.8427007929497149"},
		{fn: "erfc", args: []types.Datum{n(1)}, want: "0.15729920705028513"},
		{fn: "factorial", args: []types.Datum{n(5)}, want: "120"},
		{fn: "factorial", args: []types.Datum{n(0)}, want: "1"},
		{fn: "factorial", args: []types.Datum{n(25)}, want: "15511210043330985984000000"},
		{fn: "factorial", args: []types.Datum{n(-1)}, code: "2201F"},
		{fn: "factorial", args: []types.Datum{n(40000)}, code: "22003"},
		{fn: "lcm", args: []types.Datum{n(1071), n(462)}, want: "23562"},
		{fn: "lcm", args: []types.Datum{n(-4), n(6)}, want: "12"},
		{fn: "lcm", args: []types.Datum{n(0), n(6)}, want: "0"},
		{fn: "lcm", args: []types.Datum{n(math.MaxInt64), n(2)}, code: "22003"},
		{fn: "scale", args: []types.Datum{dec("8.41")}, want: "2"},
		{fn: "scale", args: []types.Datum{n(8)}, want: "0"},
		{fn: "min_scale", args: []types.Datum{dec("8.41")}, want: "2"},
		{fn: "trim_scale", args: []types.Datum{s("8.4100")}, want: "8.41"},
		{fn: "random", args: []types.Datum{n(5), n(5)}, want: "5"},
		{fn: "random", args: []types.Datum{dec("1.25"), dec("1.25")}, want: "1.25"},
		{fn: "random", args: []types.Datum{n(2), n(1)}, code: "22023"},
		{fn: "random", args: []types.Datum{n(2)}, code: "42883"},
		{fn: "ln", args: []types.Datum{n(0)}, code: "2201E"},
		{fn: "ln", args: []types.Datum{n(-1)}, code: "2201E"},
		{fn: "log", args: []types.Datum{n(-1), n(2)}, code: "2201E"},
		{fn: "sqrt", args: []types.Datum{n(-1)}, code: "2201F"},
		{fn: "power", args: []types.Datum{n(0), n(-1)}, code: "2201F"},
	})
	// random(min, max) stays in range and at the bounds' scale.
	rnd, _ := Lookup("random")
	for i := 0; i < 200; i++ {
		d, err := rnd.Call([]types.Datum{n(-3), n(3)})
		if err != nil || d.Fam != types.Int || d.I < -3 || d.I > 3 {
			t.Fatalf("random(-3, 3) = %v %v", d, err)
		}
		d, err = rnd.Call([]types.Datum{dec("1.5"), dec("2.25")})
		if err != nil || d.Fam != types.Decimal {
			t.Fatalf("random(1.5, 2.25) = %v %v", d, err)
		}
		v, _ := d.DecimalVal()
		if f := v.Float64(); f < 1.5 || f > 2.25 || decimalScale(d.S) > 2 {
			t.Fatalf("random(1.5, 2.25) = %s", d.S)
		}
	}
	rn, _ := Lookup("random_normal")
	if d, err := rn.Call([]types.Datum{fl(10), fl(0)}); err != nil || d.F != 10 {
		t.Fatalf("random_normal(10, 0) = %v %v", d, err)
	}
	if !strings.Contains(Reference(), "`sind(any) → float8`") {
		t.Error("the reference does not list sind")
	}
}
