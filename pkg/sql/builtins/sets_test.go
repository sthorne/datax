package builtins

import (
	"strings"
	"testing"
	"time"

	"github.com/sthorne/datax/pkg/sql/types"
)

func callFn(t *testing.T, name string, args ...types.Datum) (types.Datum, error) {
	t.Helper()
	b, ok := Lookup(name)
	if !ok {
		t.Fatalf("%s is not registered", name)
	}
	return b.Call(args)
}

func rowsOfCall(t *testing.T, name string, args ...types.Datum) ([]string, error) {
	t.Helper()
	b, ok := Lookup(name)
	if !ok {
		t.Fatalf("%s is not registered", name)
	}
	next, err := b.CallRows(args)
	if err != nil {
		return nil, err
	}
	var out []string
	for {
		row, ok, err := next()
		if err != nil {
			return out, err
		}
		if !ok {
			return out, nil
		}
		cells := make([]string, len(row))
		for i, d := range row {
			cells[i] = "NULL"
			if !d.Null {
				cells[i] = d.Text()
			}
		}
		out = append(out, strings.Join(cells, "|"))
	}
}

func s(v string) types.Datum { return types.NewString(v) }

// TestRegexpFunctions: the POSIX regular-expression family, with
// PostgreSQL's documented examples, flags, NULLs and error codes.
func TestRegexpFunctions(t *testing.T) {
	for _, c := range []struct {
		fn   string
		args []types.Datum
		want string // text of the result; "NULL"
		code string
	}{
		{fn: "regexp_replace", args: []types.Datum{s("foobarbaz"), s("b.."), s("X")}, want: "fooXbaz"},
		{fn: "regexp_replace", args: []types.Datum{s("foobarbaz"), s("b.."), s("X"), s("g")}, want: "fooXX"},
		{fn: "regexp_replace", args: []types.Datum{s("foobarbaz"), s("b(..)"), s(`X\1Y`), s("g")}, want: "fooXarYXazY"},
		{fn: "regexp_replace", args: []types.Datum{s("Thomas"), s(".[mN]a."), s("M")}, want: "ThM"},
		{fn: "regexp_replace", args: []types.Datum{s("ABC"), s("b"), s(`[\&]`), s("i")}, want: "A[B]C"},
		{fn: "regexp_replace", args: []types.Datum{s("A PostgreSQL function"), s("a|e|i|o|u"), s("X"), types.NewInt(1), types.NewInt(0), s("i")}, want: "X PXstgrXSQL fXnctXXn"},
		{fn: "regexp_replace", args: []types.Datum{s("A PostgreSQL function"), s("a|e|i|o|u"), s("X"), types.NewInt(1), types.NewInt(3), s("i")}, want: "A PostgrXSQL function"},
		{fn: "regexp_replace", args: []types.Datum{s("a$b"), s("$"), s("$1")}, want: "a$b$1"},
		{fn: "regexp_replace", args: []types.Datum{s("x"), s("("), s("y")}, code: CodeInvalidRegexp},
		{fn: "regexp_replace", args: []types.Datum{s("x"), s(`(a)\1`), s("y")}, code: CodeInvalidRegexp},
		{fn: "regexp_replace", args: []types.Datum{s("x"), s("x"), s("y"), s("z")}, code: CodeInvalidArgument},
		{fn: "regexp_replace", args: []types.Datum{s("x"), s("x"), s("y"), types.NewInt(0)}, code: CodeInvalidArgument},
		{fn: "regexp_replace", args: []types.Datum{types.DNull, s("x"), s("y")}, want: "NULL"},
		{fn: "regexp_match", args: []types.Datum{s("foobarbequebaz"), s("bar.*que")}, want: "{barbeque}"},
		{fn: "regexp_match", args: []types.Datum{s("foobarbequebaz"), s("(bar)(beque)")}, want: "{bar,beque}"},
		{fn: "regexp_match", args: []types.Datum{s("abc"), s("(x)?b")}, want: "{NULL}"},
		{fn: "regexp_match", args: []types.Datum{s("abc"), s("z")}, want: "NULL"},
		{fn: "regexp_match", args: []types.Datum{s("abc"), s("b"), s("g")}, code: CodeInvalidArgument},
		{fn: "regexp_like", args: []types.Datum{s("a\nb"), s("a.b")}, want: "t"},         // . matches a newline by default
		{fn: "regexp_like", args: []types.Datum{s("a\nb"), s("a.b"), s("n")}, want: "f"}, // but not newline-sensitively
		{fn: "regexp_like", args: []types.Datum{s("x\nab"), s("^ab"), s("n")}, want: "t"},
		{fn: "regexp_match", args: []types.Datum{s("one two"), s(`\mtwo\M`)}, want: "{two}"},
		{fn: "regexp_split_to_array", args: []types.Datum{s("hello world"), s(`\s+`)}, want: "{hello,world}"},
		{fn: "regexp_split_to_array", args: []types.Datum{s("hello"), s("")}, want: "{h,e,l,l,o}"},
		{fn: "regexp_split_to_array", args: []types.Datum{s("a b"), s(`\s*`)}, want: "{a,b}"},
		{fn: "regexp_count", args: []types.Datum{s("ABCABCAXYaxy"), s("A.")}, want: "3"},
		{fn: "regexp_count", args: []types.Datum{s("ABCABCAXYaxy"), s("A."), types.NewInt(1), s("i")}, want: "4"},
		{fn: "regexp_count", args: []types.Datum{s("ABCABCAXYaxy"), s("A."), types.NewInt(4)}, want: "2"},
		{fn: "regexp_like", args: []types.Datum{s("Hello World"), s("world$"), s("i")}, want: "t"},
		{fn: "regexp_like", args: []types.Datum{s("Hello World"), s("world$")}, want: "f"},
		{fn: "regexp_like", args: []types.Datum{s("x"), s("x"), s("k")}, code: CodeInvalidArgument},
		{fn: "regexp_substr", args: []types.Datum{s("number of your street, town zip, FR"), s("[^,]+"), types.NewInt(1), types.NewInt(2)}, want: " town zip"},
		{fn: "regexp_substr", args: []types.Datum{s("ABCDEFGHI"), s("(c..)(...)"), types.NewInt(1), types.NewInt(1), s("i"), types.NewInt(2)}, want: "FGH"},
		{fn: "regexp_substr", args: []types.Datum{s("abc"), s("z")}, want: "NULL"},
	} {
		d, err := callFn(t, c.fn, c.args...)
		if c.code != "" {
			if code(err) != c.code {
				t.Errorf("%s%v: %v, want %s", c.fn, c.args, err, c.code)
			}
			continue
		}
		if err != nil {
			t.Errorf("%s%v: %v", c.fn, c.args, err)
			continue
		}
		got := "NULL"
		if !d.Null {
			got = d.Text()
		}
		if got != c.want {
			t.Errorf("%s%v = %q, want %q", c.fn, c.args, got, c.want)
		}
	}
	rows, err := rowsOfCall(t, "regexp_matches", s("foobarbequebazilbarfbonk"), s("(b[^b]+)(b[^b]+)"), s("g"))
	if err != nil || strings.Join(rows, ";") != "{bar,beque};{bazil,barf}" {
		t.Errorf("regexp_matches g: %v %v", rows, err)
	}
	rows, err = rowsOfCall(t, "regexp_matches", s("foobarbequebazilbarfbonk"), s("(b[^b]+)(b[^b]+)"))
	if err != nil || strings.Join(rows, ";") != "{bar,beque}" {
		t.Errorf("regexp_matches: %v %v", rows, err)
	}
	rows, err = rowsOfCall(t, "regexp_split_to_table", s("the quick  fox"), s(`\s+`))
	if err != nil || strings.Join(rows, ";") != "the;quick;fox" {
		t.Errorf("regexp_split_to_table: %v %v", rows, err)
	}
	if rows, err = rowsOfCall(t, "regexp_split_to_table", types.DNull, s(",")); err != nil || len(rows) != 0 {
		t.Errorf("regexp_split_to_table(NULL): %v %v", rows, err)
	}
	if _, err := callFn(t, "regexp_matches", s("a"), s("a")); code(err) != CodeNotSupported {
		t.Errorf("a set-returning function called as a scalar: %v", err)
	}
}

// TestGenerateSeries: integer, numeric and timestamp series, the step's
// sign, the zero step (22023), NULLs and the result families.
func TestGenerateSeries(t *testing.T) {
	ts := func(v string) types.Datum {
		n, err := types.ParseTimestamp(v)
		if err != nil {
			t.Fatal(err)
		}
		return types.NewTimestamp(n)
	}
	iv := func(v string) types.Datum {
		i, err := ParseInterval(v)
		if err != nil {
			t.Fatal(err)
		}
		return types.NewInterval(i)
	}
	for _, c := range []struct {
		args []types.Datum
		want string
		code string
	}{
		{args: []types.Datum{types.NewInt(1), types.NewInt(3)}, want: "1;2;3"},
		{args: []types.Datum{types.NewInt(5), types.NewInt(1), types.NewInt(-2)}, want: "5;3;1"},
		{args: []types.Datum{types.NewInt(3), types.NewInt(1)}, want: ""},
		{args: []types.Datum{types.NewInt(9223372036854775806), types.NewInt(9223372036854775807)}, want: "9223372036854775806;9223372036854775807"},
		{args: []types.Datum{types.NewInt(1), types.NewInt(3), types.NewInt(0)}, code: CodeInvalidArgument},
		{args: []types.Datum{types.NewInt(1), types.DNull}, want: ""},
		{args: []types.Datum{types.NewDecimal("0.5"), types.NewInt(2), types.NewDecimal("0.5")}, want: "0.5;1.0;1.5;2.0"},
		{args: []types.Datum{ts("2024-01-31"), ts("2024-04-30"), iv("1 month")}, want: "2024-01-31 00:00:00+00;2024-02-29 00:00:00+00;2024-03-29 00:00:00+00;2024-04-29 00:00:00+00"},
		{args: []types.Datum{ts("2024-03-10"), s("2024-03-09"), s("-12 hours")}, want: "2024-03-10 00:00:00+00;2024-03-09 12:00:00+00;2024-03-09 00:00:00+00"},
		{args: []types.Datum{types.NewDate(0), s("1970-01-02"), iv("1 day")}, want: "1970-01-01 00:00:00+00;1970-01-02 00:00:00+00"},
		{args: []types.Datum{ts("2024-01-01"), ts("2024-01-02"), iv("0 days")}, code: CodeInvalidArgument},
		{args: []types.Datum{ts("2024-01-01"), ts("2024-01-02")}, code: CodeUndefined},
	} {
		rows, err := rowsOfCall(t, "generate_series", c.args...)
		if c.code != "" {
			if code(err) != c.code {
				t.Errorf("generate_series%v: %v, want %s", c.args, err, c.code)
			}
			continue
		}
		if err != nil || strings.Join(rows, ";") != c.want {
			t.Errorf("generate_series%v = %q %v, want %q", c.args, strings.Join(rows, ";"), err, c.want)
		}
	}
	b, _ := Lookup("generate_series")
	for _, c := range []struct {
		args []types.Family
		want types.Family
	}{
		{[]types.Family{types.Int, types.Int}, types.Int},
		{[]types.Family{types.Int, types.Decimal}, types.Decimal},
		{[]types.Family{types.Timestamp, types.String, types.String}, types.Timestamp},
		{[]types.Family{types.Date, types.Date, types.IntervalFam}, types.Timestamp},
		{[]types.Family{types.String, types.String, types.IntervalFam}, types.Timestamp},
	} {
		if got := b.ResultFamily(c.args); got != c.want {
			t.Errorf("generate_series%v → %v, want %v", c.args, got, c.want)
		}
	}
	if !strings.Contains(b.Signature(), "setof") {
		t.Errorf("signature: %s", b.Signature())
	}
}

// TestJSONSets: jsonb_each, jsonb_each_text, jsonb_array_elements[_text],
// jsonb_object_keys and jsonb_insert, with their 22023 refusals.
func TestJSONSets(t *testing.T) {
	j := func(v string) types.Datum {
		d, err := types.ParseJSONB(v)
		if err != nil {
			t.Fatal(err)
		}
		return d
	}
	for _, c := range []struct {
		fn   string
		arg  types.Datum
		want string
		code string
	}{
		{fn: "jsonb_each", arg: j(`{"b":[1,2],"a":"x"}`), want: `a|"x";b|[1,2]`},
		{fn: "jsonb_each_text", arg: j(`{"b":2,"a":"x","n":null}`), want: `a|x;b|2;n|NULL`},
		{fn: "json_each", arg: j(`{"a":true}`), want: `a|true`},
		{fn: "jsonb_each", arg: j(`[1]`), code: CodeInvalidArgument},
		{fn: "jsonb_array_elements", arg: j(`[1,"a",null,{"k":1}]`), want: `1;"a";null;{"k":1}`},
		{fn: "jsonb_array_elements_text", arg: j(`[1,"a",null]`), want: `1;a;NULL`},
		{fn: "jsonb_array_elements", arg: j(`{"a":1}`), code: CodeInvalidArgument},
		{fn: "jsonb_array_elements", arg: j(`3`), code: CodeInvalidArgument},
		{fn: "jsonb_object_keys", arg: j(`{"b":1,"a":2}`), want: `a;b`},
		{fn: "jsonb_object_keys", arg: j(`[1]`), code: CodeInvalidArgument},
		{fn: "jsonb_array_elements", arg: types.DNull, want: ``},
	} {
		rows, err := rowsOfCall(t, c.fn, c.arg)
		if c.code != "" {
			if code(err) != c.code {
				t.Errorf("%s(%s): %v, want %s", c.fn, c.arg.Text(), err, c.code)
			}
			continue
		}
		if err != nil || strings.Join(rows, ";") != c.want {
			t.Errorf("%s(%s) = %q %v, want %q", c.fn, c.arg.Text(), strings.Join(rows, ";"), err, c.want)
		}
	}
	for _, c := range []struct {
		args []types.Datum
		want string
		code string
	}{
		{args: []types.Datum{j(`{"a":[0,1,2]}`), s("{a,1}"), j(`"new"`)}, want: `{"a":[0,"new",1,2]}`},
		{args: []types.Datum{j(`{"a":[0,1,2]}`), s("{a,1}"), j(`"new"`), types.NewBool(true)}, want: `{"a":[0,1,"new",2]}`},
		{args: []types.Datum{j(`{"a":[0,1,2]}`), s("{a,-1}"), j(`9`)}, want: `{"a":[0,1,9,2]}`},
		{args: []types.Datum{j(`{"a":[0]}`), s("{a,10}"), j(`9`)}, want: `{"a":[0,9]}`},
		{args: []types.Datum{j(`{"a":{"b":1}}`), s("{a,c}"), j(`2`)}, want: `{"a":{"b":1,"c":2}}`},
		{args: []types.Datum{j(`{"a":{"b":1}}`), s("{x,c}"), j(`2`)}, want: `{"a":{"b":1}}`},
		{args: []types.Datum{j(`{"a":{"b":1}}`), s("{a,b}"), j(`2`)}, code: CodeInvalidArgument},
		{args: []types.Datum{j(`1`), s("{a}"), j(`2`)}, code: CodeInvalidArgument},
	} {
		d, err := callFn(t, "jsonb_insert", c.args...)
		if c.code != "" {
			if code(err) != c.code {
				t.Errorf("jsonb_insert%v: %v, want %s", c.args, err, c.code)
			}
			continue
		}
		if err != nil || d.Text() != c.want {
			t.Errorf("jsonb_insert%v = %s %v, want %s", c.args, d.Text(), err, c.want)
		}
	}
}

// TestTimeZones: zone names, abbreviations and POSIX offsets (west
// positive), AT TIME ZONE both ways, and the DST gap and overlap.
func TestTimeZones(t *testing.T) {
	for _, c := range []struct {
		zone    string
		abbrevs bool
		off     int // seconds east, at 2024-07-01
		bad     bool
	}{
		{zone: "UTC", off: 0},
		{zone: "Etc/UTC", off: 0},
		{zone: "america/new_york", off: -4 * 3600},
		{zone: "Asia/Kolkata", off: 5*3600 + 1800},
		{zone: "+5", off: -5 * 3600},
		{zone: "UTC+5", off: -5 * 3600},
		{zone: "-03:30", off: 3*3600 + 1800},
		{zone: "EST5EDT", off: -4 * 3600},
		{zone: "PST", abbrevs: true, off: -8 * 3600},
		{zone: "PST", bad: true},
		{zone: "Mars/Olympus", bad: true},
		{zone: "local", bad: true},
		{zone: "+99", bad: true},
	} {
		loc, err := ParseZone(c.zone, c.abbrevs)
		if c.bad {
			if code(err) != CodeInvalidArgument {
				t.Errorf("%q: %v %v, want 22023", c.zone, loc, err)
			}
			continue
		}
		if err != nil {
			t.Errorf("%q: %v", c.zone, err)
			continue
		}
		if _, off := time.Date(2024, 7, 1, 0, 0, 0, 0, time.UTC).In(loc).Zone(); off != c.off {
			t.Errorf("%q: offset %d, want %d", c.zone, off, c.off)
		}
	}
	for zone, utc := range map[string]bool{"UTC": true, "Etc/GMT": true, "+00:00": true, "GMT0": true, "Europe/London": false, "Africa/Abidjan": false} {
		loc, err := ParseZone(zone, false)
		if err != nil || ZoneIsUTC(loc) != utc {
			t.Errorf("ZoneIsUTC(%q) = %v %v, want %v", zone, loc != nil && ZoneIsUTC(loc), err, utc)
		}
	}
	tstz := func(v string) types.Datum {
		n, err := types.ParseTimestamp(v)
		if err != nil {
			t.Fatal(err)
		}
		return types.NewTimestamp(n)
	}
	wall := func(v string) types.Datum {
		n, err := types.ParseTimestampNoTZ(v)
		if err != nil {
			t.Fatal(err)
		}
		d := types.NewTimestamp(n)
		d.NoTZ = true
		return d
	}
	for _, c := range []struct {
		zone string
		ts   types.Datum
		want string
	}{
		{"America/New_York", tstz("2024-07-01 12:00:00Z"), "2024-07-01 08:00:00"},
		{"America/New_York", tstz("2024-01-01 12:00:00Z"), "2024-01-01 07:00:00"},
		{"America/New_York", wall("2024-07-01 12:00:00"), "2024-07-01 16:00:00+00"},
		// Spring forward: 02:30 does not exist; the pre-transition
		// offset (EST) applies, so it is 07:30Z (03:30 EDT).
		{"America/New_York", wall("2024-03-10 02:30:00"), "2024-03-10 07:30:00+00"},
		// Fall back: 01:30 happens twice; the post-transition (EST)
		// reading wins.
		{"America/New_York", wall("2024-11-03 01:30:00"), "2024-11-03 06:30:00+00"},
		{"America/New_York", wall("2024-11-03 00:30:00"), "2024-11-03 04:30:00+00"},
		{"America/New_York", wall("2024-11-03 02:30:00"), "2024-11-03 07:30:00+00"},
		{"America/New_York", tstz("2024-11-03 05:30:00Z"), "2024-11-03 01:30:00"},
		{"America/New_York", tstz("2024-11-03 06:30:00Z"), "2024-11-03 01:30:00"},
		{"Europe/Berlin", wall("2024-03-31 02:30:00"), "2024-03-31 01:30:00+00"},
		{"UTC+5", tstz("2024-07-01 12:00:00Z"), "2024-07-01 07:00:00"},
		{"Asia/Kolkata", s("2024-07-01 12:00:00+00"), "2024-07-01 17:30:00"},
	} {
		d, err := AtTimeZone(s(c.zone), c.ts)
		if err != nil || d.Text() != c.want {
			t.Errorf("%s AT TIME ZONE %q = %q %v, want %q", c.ts.Text(), c.zone, d.Text(), err, c.want)
		}
	}
	minus8, _ := ParseInterval("-08:00")
	if d, err := AtTimeZone(types.NewInterval(minus8), tstz("2024-07-01 12:00:00Z")); err != nil || d.Text() != "2024-07-01 04:00:00" {
		t.Errorf("AT TIME ZONE INTERVAL '-08:00': %q %v", d.Text(), err)
	}
	oneDay, _ := ParseInterval("1 day")
	if _, err := AtTimeZone(types.NewInterval(oneDay), tstz("2024-07-01 12:00:00Z")); code(err) != CodeInvalidArgument {
		t.Errorf("a zone interval of days: %v", err)
	}
	if d, err := AtTimeZone(types.DNull, tstz("2024-07-01 12:00:00Z")); err != nil || !d.Null {
		t.Errorf("NULL zone: %v %v", d, err)
	}

	// Under a session zone: input without an offset, rendering, calendar
	// fields, truncation and day arithmetic are New York's.
	ny, _ := ParseZone("America/New_York", false)
	in := s("2024-07-01 22:30:00")
	in.Loc = ny
	v, err := in.Coerce(types.Timestamp)
	if err != nil || TextIn(v, ny) != "2024-07-01 22:30:00-04" || v.Text() != "2024-07-02 02:30:00+00" {
		t.Fatalf("zone-less input in New York: %q %v", TextIn(v, ny), err)
	}
	for _, c := range []struct {
		typ, want string
	}{{"text", "2024-07-01 22:30:00-04"}, {"varchar(10)", "2024-07-01"}, {"date", "2024-07-01"}, {"time", "22:30:00"}, {"timestamp", "2024-07-01 22:30:00"}} {
		if d, err := CastIn(v, c.typ, ny); err != nil || d.Text() != c.want {
			t.Errorf("::%s in New York = %q %v, want %q", c.typ, d.Text(), err, c.want)
		}
	}
	trunc, _ := Lookup("date_trunc")
	if d, err := trunc.CallIn([]types.Datum{s("day"), v}, ny); err != nil || TextIn(d, ny) != "2024-07-01 00:00:00-04" {
		t.Errorf("date_trunc in New York: %q %v", TextIn(d, ny), err)
	}
	ext, _ := Lookup("date_part")
	if d, err := ext.CallIn([]types.Datum{s("hour"), v}, ny); err != nil || d.Text() != "22" {
		t.Errorf("date_part in New York: %q %v", d.Text(), err)
	}
	day, _ := ParseInterval("1 day")
	before := tstz("2024-03-09 17:00:00Z") // noon EST
	if d, ok, err := DateArithIn(before, "+", types.NewInterval(day), ny); !ok || err != nil || TextIn(d, ny) != "2024-03-10 12:00:00-04" {
		t.Errorf("a day across spring-forward: %q %v", TextIn(d, ny), err)
	}
	if d, _, _ := DateArith(before, "+", types.NewInterval(day)); d.Text() != "2024-03-10 17:00:00+00" {
		t.Errorf("a day in UTC: %q", d.Text())
	}
	gap := s("2024-03-10 02:30:00")
	gap.Loc = ny
	if d, err := gap.Coerce(types.Timestamp); err != nil || TextIn(d, ny) != "2024-03-10 03:30:00-04" {
		t.Errorf("a time in the spring-forward gap: %q %v", TextIn(d, ny), err)
	}
	if d, err := s("2024-07-01 12:00 Europe/Paris").Coerce(types.Timestamp); err != nil || d.Text() != "2024-07-01 10:00:00+00" {
		t.Errorf("a zone name in the input: %q %v", d.Text(), err)
	}
}
