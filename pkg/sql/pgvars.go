package sql

import (
	"math"
	"regexp"
	"strconv"
	"strings"
)

// PostgreSQL's own settings (issue #345): the ones frameworks, drivers
// and pg_dump SET on every connection, registered with PostgreSQL 18's
// name, type, unit, default and values, and checked as PostgreSQL
// checks them (a Boolean, an integer with a memory unit, a real, one of
// an enum's values). Each is stored per session, reported by SHOW,
// current_setting() and pg_settings, and RESET to its default. Most
// change nothing in datax; the ones that do are IntervalStyle and
// bytea_output (the text format of those values on the wire),
// client_min_messages (whether NOTICEs reach the client) and
// row_security (off refuses a query its policies would filter). The
// list of the others is in docs/user/postgres-differences.md.

// pgVar describes one such setting.
type pgVar struct {
	name    string // PostgreSQL's spelling (IntervalStyle)
	vartype string // bool, integer, real, enum or string
	unit    string // the base unit of an integer: kB or 8kB ("" for none)
	boot    string // the default, in the stored form (base units, on / off)
	// enum are an enum's values as pg_settings' enumvals lists them;
	// hidden maps the other spellings PostgreSQL accepts to the value
	// they set (synchronous_commit = true is on).
	enum   []string
	hidden map[string]string
	// min and max bound an integer or a real.
	min, max float64
	// context is pg_settings' context: user, superuser or internal (a
	// value that cannot be changed: server_version_num).
	context string
	// noShowAll keeps the setting out of SHOW ALL and pg_settings, as
	// PostgreSQL's GUC_NO_SHOW_ALL does (default_with_oids); SHOW and
	// current_setting() still read it.
	noShowAll bool
	// check refuses a value PostgreSQL's check hook would (nil: none).
	check func(name, value string) *Error
	// show, when set, is what SHOW reports whatever the stored value
	// (default_transaction_isolation: serializable, the only level).
	show func(s *Session) string
}

const maxKilobytes = math.MaxInt32 // PostgreSQL's MAX_KILOBYTES on 64-bit builds

var (
	boolVar = func(name, boot string) pgVar { return pgVar{name: name, vartype: "bool", boot: boot, context: "user"} }
	// synchronousCommitHidden are the Boolean spellings
	// synchronous_commit takes besides its values.
	synchronousCommitHidden = map[string]string{"true": "on", "false": "off", "yes": "on", "no": "off", "1": "on", "0": "off"}
	// localeName is a locale name PostgreSQL could know (en_US.UTF-8,
	// de_DE@euro); C and POSIX are checked apart.
	localeName = regexp.MustCompile(`^[A-Za-z]{2,3}(_[A-Za-z]{2})?(\.[A-Za-z0-9-]+)?(@[A-Za-z0-9]+)?$`)
)

// checkLocale accepts C, POSIX, ” (the server's) and the shape of a
// locale name; datax formats every value in the C locale whatever it is.
func checkLocale(name, value string) *Error {
	switch value {
	case "", "C", "POSIX", "C.UTF-8", "C.utf8":
		return nil
	}
	if localeName.MatchString(value) {
		return nil
	}
	return newErrf(CodeInvalidParameterValue, "invalid value for parameter %q: %q", name, value)
}

// pgVarList are the settings, in no particular order (SHOW ALL sorts).
var pgVarList = []pgVar{
	// Statement behavior.
	{name: "client_min_messages", vartype: "enum", boot: "notice", context: "user",
		enum:   []string{"debug5", "debug4", "debug3", "debug2", "debug1", "log", "notice", "warning", "error"},
		hidden: map[string]string{"debug": "debug2", "info": "info"}},
	{name: "IntervalStyle", vartype: "enum", boot: "postgres", context: "user",
		enum: []string{"postgres", "postgres_verbose", "sql_standard", "iso_8601"}},
	boolVar("check_function_bodies", "on"),
	{name: "xmloption", vartype: "enum", boot: "content", context: "user", enum: []string{"content", "document"}},
	{name: "default_tablespace", vartype: "string", boot: "", context: "user", check: func(name, value string) *Error {
		// The tablespace must exist; datax has PostgreSQL's two.
		switch value {
		case "", "pg_default", "pg_global":
			return nil
		}
		return newErrf(CodeUndefinedObject, "invalid value for parameter %q: %q", name, value)
	}},
	boolVar("row_security", "on"),
	{name: "bytea_output", vartype: "enum", boot: "hex", context: "user", enum: []string{"escape", "hex"}},
	boolVar("default_transaction_deferrable", "off"),
	{name: "default_transaction_isolation", vartype: "enum", boot: "serializable", context: "user",
		enum: []string{"serializable", "repeatable read", "read committed", "read uncommitted"},
		// Every transaction is serializable, whatever was asked for, as
		// SHOW transaction_isolation reports.
		show: func(*Session) string { return "serializable" }},
	{name: "session_replication_role", vartype: "enum", boot: "origin", context: "superuser", enum: []string{"origin", "replica", "local"}},

	// Resource usage and the planner.
	{name: "work_mem", vartype: "integer", unit: "kB", boot: "4096", min: 64, max: maxKilobytes, context: "user"},
	{name: "maintenance_work_mem", vartype: "integer", unit: "kB", boot: "65536", min: 1024, max: maxKilobytes, context: "user"},
	{name: "effective_cache_size", vartype: "integer", unit: "8kB", boot: "524288", min: 1, max: math.MaxInt32, context: "user"},
	{name: "random_page_cost", vartype: "real", boot: "4", min: 0, max: math.MaxFloat64, context: "user"},
	{name: "seq_page_cost", vartype: "real", boot: "1", min: 0, max: math.MaxFloat64, context: "user"},
	boolVar("jit", "on"),
	boolVar("geqo", "on"),
	boolVar("enable_seqscan", "on"),
	boolVar("enable_indexscan", "on"),
	boolVar("enable_indexonlyscan", "on"),
	boolVar("enable_bitmapscan", "on"),
	boolVar("enable_hashjoin", "on"),
	boolVar("enable_mergejoin", "on"),
	boolVar("enable_nestloop", "on"),
	boolVar("enable_sort", "on"),
	boolVar("enable_hashagg", "on"),
	boolVar("enable_material", "on"),
	{name: "synchronous_commit", vartype: "enum", boot: "on", context: "user",
		enum: []string{"local", "remote_write", "remote_apply", "on", "off"}, hidden: synchronousCommitHidden},

	// Client formatting and locale.
	{name: "extra_float_digits", vartype: "integer", boot: "1", min: -15, max: 3, context: "user"},
	{name: "lc_messages", vartype: "string", boot: "C", context: "superuser", check: checkLocale},
	{name: "lc_monetary", vartype: "string", boot: "C", context: "user", check: checkLocale},
	{name: "lc_numeric", vartype: "string", boot: "C", context: "user", check: checkLocale},
	{name: "lc_time", vartype: "string", boot: "C", context: "user", check: checkLocale},

	// Compatibility.
	boolVar("escape_string_warning", "on"),
	boolVar("array_nulls", "on"),
	boolVar("quote_all_identifiers", "off"),
	boolVar("synchronize_seqscans", "on"), // pg_dump sets it off
	{name: "default_with_oids", vartype: "bool", boot: "off", context: "user", noShowAll: true, check: func(name, value string) *Error {
		if value == "on" {
			return newErrf(CodeFeatureNotSupported, "tables declared WITH OIDS are not supported")
		}
		return nil
	}},

	// Preset options: what the server is, never changed.
	{name: "server_version_num", vartype: "integer", boot: "140000", min: 140000, max: 140000, context: "internal"},
	{name: "max_identifier_length", vartype: "integer", boot: "63", min: 63, max: 63, context: "internal"},
}

// pgVars is pgVarList by name.
var pgVars = func() map[string]*pgVar {
	m := make(map[string]*pgVar, len(pgVarList))
	for i := range pgVarList {
		m[pgVarList[i].name] = &pgVarList[i]
	}
	return m
}()

// withPGVars appends the settings SHOW ALL lists to varNames.
func withPGVars(names []struct{ name, vartype, unit string }) []struct{ name, vartype, unit string } {
	for _, v := range pgVarList {
		if !v.noShowAll {
			names = append(names, struct{ name, vartype, unit string }{v.name, v.vartype, v.unit})
		}
	}
	return names
}

// lookupPGVar resolves a name case-insensitively to its pgVar.
func lookupPGVar(name string) *pgVar {
	for i := range pgVarList {
		if strings.EqualFold(pgVarList[i].name, name) {
			return &pgVarList[i]
		}
	}
	return nil
}

// pgVarValue is the setting's stored value: the session's, or the
// default.
func (vars *sessionVars) pgVarValue(v *pgVar) string {
	if val, ok := vars.pg[v.name]; ok {
		return val
	}
	return v.boot
}

// showPGVar renders the setting as SHOW reports it: a memory size in
// its largest exact unit (4MB), the rest as stored.
func (s *Session) showPGVar(v *pgVar) string {
	if v.show != nil {
		return v.show(s)
	}
	val := s.vars.pgVarValue(v)
	if v.unit == "" {
		return val
	}
	n, err := strconv.ParseInt(val, 10, 64)
	if err != nil || n <= 0 {
		return val
	}
	bytes := n * memoryUnitBytes(v.unit)
	for _, u := range []struct {
		name string
		size int64
	}{{"TB", 1 << 40}, {"GB", 1 << 30}, {"MB", 1 << 20}, {"kB", 1 << 10}} {
		if bytes%u.size == 0 {
			return strconv.FormatInt(bytes/u.size, 10) + u.name
		}
	}
	return val
}

// memoryUnitBytes is the size of a memory unit in bytes (0: not one).
func memoryUnitBytes(unit string) int64 {
	switch unit {
	case "B":
		return 1
	case "kB":
		return 1 << 10
	case "8kB":
		return 8 << 10
	case "MB":
		return 1 << 20
	case "GB":
		return 1 << 30
	case "TB":
		return 1 << 40
	}
	return 0
}

// pgSettingRaw renders a setting's SHOW value as pg_settings' setting,
// boot_val and reset_val columns report it: in the base unit (4096 for
// work_mem's 4MB).
func pgSettingRaw(name, shown string) string {
	v, ok := pgVars[name]
	if !ok || v.unit == "" {
		return shown
	}
	if n, err := parsePGInt(v, shown); err == nil {
		return strconv.FormatInt(n, 10)
	}
	return shown
}

// pgSettingContexts maps the settings whose pg_settings context is not
// user to it.
var pgSettingContexts = func() map[string]string {
	m := map[string]string{}
	for _, v := range pgVarList {
		if v.context != "user" && !v.noShowAll {
			m[v.name] = v.context
		}
	}
	return m
}()

// applyPGVar sets (or, with reset, restores) one of the settings on
// vars, checking the value as PostgreSQL does.
func applyPGVar(vars *sessionVars, v *pgVar, value string, reset bool) error {
	if v.context == "internal" {
		return newErrf(CodeCantChangeRuntimeParam, "parameter %q cannot be changed", v.name)
	}
	stored := v.boot
	if !reset {
		raw := strings.TrimSpace(value)
		if len(raw) >= 2 && (raw[0] == '\'' && raw[len(raw)-1] == '\'' || raw[0] == '"' && raw[len(raw)-1] == '"') {
			raw = raw[1 : len(raw)-1]
		}
		var err error
		if stored, err = parsePGVar(v, raw); err != nil {
			return err
		}
		if v.check != nil {
			if serr := v.check(v.name, stored); serr != nil {
				return serr
			}
		}
	}
	next := make(map[string]string, len(vars.pg)+1)
	for k, val := range vars.pg {
		next[k] = val
	}
	if stored == v.boot {
		delete(next, v.name)
	} else {
		next[v.name] = stored
	}
	vars.pg = next
	return nil
}

// parsePGVar checks a value and returns it in the stored form, with
// PostgreSQL's messages (all 22023).
func parsePGVar(v *pgVar, value string) (string, error) {
	invalid := func() error {
		return newErrf(CodeInvalidParameterValue, "invalid value for parameter %q: %q", v.name, value)
	}
	switch v.vartype {
	case "bool":
		b, ok := parsePGBool(value)
		if !ok {
			return "", newErrf(CodeInvalidParameterValue, "parameter %q requires a Boolean value", v.name)
		}
		return onOff(b), nil
	case "enum":
		for _, e := range v.enum {
			if strings.EqualFold(e, value) {
				return e, nil
			}
		}
		for k, e := range v.hidden {
			if strings.EqualFold(k, value) {
				return e, nil
			}
		}
		return "", invalid()
	case "integer":
		n, err := parsePGInt(v, value)
		if err != nil {
			return "", err
		}
		if float64(n) < v.min || float64(n) > v.max {
			sp, unit := "", v.unit
			if unit != "" {
				sp = " "
			}
			return "", newErrf(CodeInvalidParameterValue, "%d%s%s is outside the valid range for parameter %q (%d%s%s .. %d%s%s)",
				n, sp, unit, v.name, int64(v.min), sp, unit, int64(v.max), sp, unit)
		}
		return strconv.FormatInt(n, 10), nil
	case "real":
		f, err := strconv.ParseFloat(strings.TrimSpace(value), 64)
		if err != nil || math.IsNaN(f) {
			return "", invalid()
		}
		if f < v.min || f > v.max {
			return "", newErrf(CodeInvalidParameterValue, "%s is outside the valid range for parameter %q (%s .. %s)",
				formatG(f), v.name, formatG(v.min), formatG(v.max))
		}
		return formatG(f), nil
	}
	return value, nil
}

// formatG is C's %g: six significant digits.
func formatG(f float64) string { return strconv.FormatFloat(f, 'g', 6, 64) }

// parsePGBool is PostgreSQL's parse_bool: any unambiguous prefix of
// true, false, yes, no, on, off, or 1 / 0, in any letter case.
func parsePGBool(value string) (bool, bool) {
	v := strings.ToLower(value)
	if v == "" {
		return false, false
	}
	prefix := func(word string) bool { return strings.HasPrefix(word, v) }
	switch {
	case v == "1":
		return true, true
	case v == "0":
		return false, true
	case prefix("true"), prefix("yes"):
		return true, true
	case prefix("false"), prefix("no"):
		return false, true
	case len(v) >= 2 && prefix("on"):
		return true, true
	case len(v) >= 2 && prefix("off"):
		return false, true
	}
	return false, false
}

var pgIntRE = regexp.MustCompile(`^\s*([+-]?(?:0[xX][0-9a-fA-F]+|(?:\d+\.?\d*|\.\d+)(?:[eE][+-]?\d+)?))\s*(.*?)\s*$`)

// parsePGInt is PostgreSQL's parse_int: a number (a fraction rounds),
// and for a setting with a unit an optional memory unit (B, kB, MB, GB,
// TB, in that letter case), converted to the setting's base unit.
func parsePGInt(v *pgVar, value string) (int64, error) {
	// PostgreSQL adds a HINT naming the valid units, which datax's
	// errors do not carry.
	invalid := func() error {
		return newErrf(CodeInvalidParameterValue, "invalid value for parameter %q: %q", v.name, value)
	}
	m := pgIntRE.FindStringSubmatch(value)
	if m == nil {
		return 0, invalid()
	}
	var f float64
	if num := strings.TrimLeft(m[1], "+-"); strings.HasPrefix(num, "0x") || strings.HasPrefix(num, "0X") {
		n, err := strconv.ParseInt(num[2:], 16, 64)
		if err != nil {
			return 0, invalid()
		}
		f = float64(n)
		if strings.HasPrefix(m[1], "-") {
			f = -f
		}
	} else {
		var err error
		if f, err = strconv.ParseFloat(m[1], 64); err != nil {
			return 0, invalid()
		}
	}
	if unit := m[2]; unit != "" {
		if v.unit == "" {
			return 0, invalid()
		}
		size := memoryUnitBytes(unit)
		if size == 0 || unit == "8kB" {
			return 0, invalid()
		}
		f = f * float64(size) / float64(memoryUnitBytes(v.unit))
	}
	f = math.RoundToEven(f)
	if f > math.MaxInt32 || f < math.MinInt32 {
		return 0, invalid()
	}
	return int64(f), nil
}

// pgVarString is the stored value of a setting of the session, for the
// few that change behavior.
func (s *Session) pgVarString(name string) string { return s.vars.pgVarValue(pgVars[name]) }

// IntervalStyle is the session's IntervalStyle: how an INTERVAL renders
// in text on the wire.
func (s *Session) IntervalStyle() string { return s.pgVarString("IntervalStyle") }

// ExtraFloatDigits is the session's extra_float_digits: 1 to 3 print a
// float8 in its shortest exact form, 0 and below round it to 15 + n
// significant digits, in text on the wire.
func (s *Session) ExtraFloatDigits() int {
	n, err := strconv.Atoi(s.pgVarString("extra_float_digits"))
	if err != nil {
		return 1
	}
	return n
}

// ByteaEscape reports bytea_output = escape: a BYTEA renders in
// PostgreSQL's escape format in text on the wire, not as \x hex.
func (s *Session) ByteaEscape() bool { return s.pgVarString("bytea_output") == "escape" }

// SendsNotices reports whether client_min_messages lets a NOTICE reach
// the client (it does unless the level is warning or error).
func (s *Session) SendsNotices() bool {
	switch s.pgVarString("client_min_messages") {
	case "warning", "error":
		return false
	}
	return true
}

// SendsWarnings reports whether client_min_messages lets a WARNING
// reach the client (it does unless the level is error).
func (s *Session) SendsWarnings() bool { return s.pgVarString("client_min_messages") != "error" }

// rowSecurityOff refuses a query that a table's policies would filter
// while row_security is off, as PostgreSQL does (pg_dump sets it so a
// dump never silently misses rows).
func (s *Session) rowSecurityOff(name string) error {
	if s.pgVarString("row_security") == "off" {
		return newErrf(CodeInsufficientPriv, "query would be affected by row-level security policy for table %q", name)
	}
	return nil
}
