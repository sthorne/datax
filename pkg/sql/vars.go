package sql

import (
	"context"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/sthorne/datax/pkg/sql/builtins"
	"github.com/sthorne/datax/pkg/sql/catalog"
	"github.com/sthorne/datax/pkg/sql/parser"
	"github.com/sthorne/datax/pkg/sql/tsearch"
	"github.com/sthorne/datax/pkg/sql/types"
	"github.com/sthorne/datax/pkg/sql/vtable"
)

// Session variables (issue #97): the settings SET / RESET / SHOW and
// pg_settings work with, each honored — a timeout enforced, a rendering
// changed, a transaction made read-only — or accepted with its real
// value reported. An unknown variable is 42704, an invalid value 22023.
// SET LOCAL inside a transaction block overrides the session's value
// until the block ends.

// sessionVars are the honored settings.
type sessionVars struct {
	applicationName   string
	searchPath        string
	timeZone          *time.Location
	timeZoneName      string
	dateStyle         string
	clientEncoding    string
	defaultReadOnly   bool
	txnReadOnly       bool
	isolation         string
	statementTimeout  time.Duration
	lockTimeout       time.Duration
	idleInTxnTimeout  time.Duration
	cascadeLimit      int
	cascadeLimitIsSet bool
	// memoryLimit is statement_memory_limit in bytes (0 = none).
	memoryLimit int64
	// role is SET ROLE's role ("" = none: the session user).
	role string
	// invertedScan is inverted_index_scan: whether the planner may (auto),
	// must (on) or must not (off) read an inverted index that can serve
	// a statement. For comparing the two paths; auto is right otherwise.
	invertedScan invertedMode
	// textSearchConfig is default_text_search_config: the configuration
	// the text-search functions use when a call names none ("" = the
	// built-in default).
	textSearchConfig string
	// custom holds the custom settings (issue #294): two-part names an
	// application defines for itself (app.tenant), which a row-security
	// policy reads with current_setting(). The map is replaced, never
	// changed in place, on every SET: sessionVars is copied by value to
	// save a transaction's SET LOCAL, and the copy must keep the old one.
	custom map[string]string
	// ivfProbes is ivfflat.probes: how many lists of a vector index a
	// query reads (0 = the square root of the index's lists, the default).
	ivfProbes int
	// noVectorScan is vector_index_scan = off: nearest-neighbour queries
	// are answered exactly, by scanning, whatever index exists.
	noVectorScan bool
	// pg holds PostgreSQL's settings of pgvars.go the session changed
	// from their defaults, by name. Replaced, never changed in place,
	// like custom.
	pg map[string]string
}

// isCustomVar reports whether name is a custom setting's: PostgreSQL's
// rule, a name with a class prefix (class.name) — unless the name is a
// setting datax defines itself under such a prefix (ivfflat.probes, as
// pgvector names it), which is typed and checked like any other, as an
// extension's settings are in PostgreSQL once it is loaded.
func isCustomVar(name string) bool {
	i := strings.IndexByte(name, '.')
	return i > 0 && i < len(name)-1 && canonicalVar(name) == ""
}

// setCustomVar sets a custom setting on vars.
func setCustomVar(vars *sessionVars, name, value string) {
	next := make(map[string]string, len(vars.custom)+1)
	for k, v := range vars.custom {
		next[k] = v
	}
	next[name] = value
	vars.custom = next
}

func defaultVars() sessionVars {
	return sessionVars{searchPath: catalog.PublicSchema, timeZone: time.UTC, timeZoneName: "UTC", dateStyle: "ISO, MDY", clientEncoding: "UTF8", isolation: "serializable", memoryLimit: defaultMemoryLimit}
}

// varNames are the settings in SHOW ALL order, with their pg_settings
// vartype and unit; PostgreSQL's of pgvars.go follow.
var varNames = withPGVars([]struct{ name, vartype, unit string }{
	{"application_name", "string", ""},
	{"client_encoding", "string", ""},
	{"database", "string", ""},
	{"DateStyle", "string", ""},
	{"default_text_search_config", "string", ""},
	{"default_transaction_read_only", "bool", ""},
	{"foreign_key_cascade_limit", "integer", ""},
	{"idle_in_transaction_session_timeout", "integer", "ms"},
	{"integer_datetimes", "bool", ""},
	{"inverted_index_scan", "enum", ""},
	{"ivfflat.probes", "integer", ""},
	{"lock_timeout", "integer", "ms"},
	{"role", "string", ""},
	{"search_path", "string", ""},
	{"server_encoding", "string", ""},
	{"server_version", "string", ""},
	{"standard_conforming_strings", "bool", ""},
	{"statement_memory_limit", "integer", "B"},
	{"statement_timeout", "integer", "ms"},
	{"TimeZone", "string", ""},
	{"transaction_isolation", "string", ""},
	{"transaction_read_only", "bool", ""},
	{"vector_index_scan", "bool", ""},
})

// settingTypes is varNames' vartype and unit by name, for pg_settings.
var settingTypes = func() map[string][2]string {
	m := make(map[string][2]string, len(varNames))
	for _, v := range varNames {
		m[v.name] = [2]string{v.vartype, v.unit}
	}
	return m
}()

// canonicalVar resolves a SET / SHOW name (case-insensitively, with the
// spelled-out SHOW forms) to its canonical name; "" when unknown.
func canonicalVar(name string) string {
	switch strings.ToLower(strings.TrimSpace(name)) {
	case "time_zone", "timezone":
		return "TimeZone"
	case "transaction_isolation_level", "transaction_isolation":
		return "transaction_isolation"
	case "datestyle":
		return "DateStyle"
	case "session_authorization":
		return ""
	}
	for _, v := range varNames {
		if strings.EqualFold(v.name, name) {
			return v.name
		}
	}
	if v := lookupPGVar(strings.TrimSpace(name)); v != nil {
		return v.name // one SHOW ALL leaves out (default_with_oids)
	}
	return ""
}

// value renders a setting as SHOW reports it.
func (s *Session) varValue(name string) string {
	v := s.vars
	switch name {
	case "application_name":
		return v.applicationName
	case "role":
		if v.role == "" {
			return "none"
		}
		return v.role
	case "client_encoding":
		return v.clientEncoding
	case "database":
		return s.database
	case "DateStyle":
		return v.dateStyle
	case "default_text_search_config":
		// Schema-qualified, as PostgreSQL reports it.
		return "pg_catalog." + s.textSearchConfig()
	case "default_transaction_read_only":
		return onOff(v.defaultReadOnly)
	case "foreign_key_cascade_limit":
		return strconv.Itoa(s.fkCascadeLimit())
	case "idle_in_transaction_session_timeout":
		return strconv.FormatInt(v.idleInTxnTimeout.Milliseconds(), 10)
	case "integer_datetimes", "standard_conforming_strings":
		return "on"
	case "inverted_index_scan":
		return v.invertedScan.String()
	case "ivfflat.probes":
		if v.ivfProbes == 0 {
			return "auto"
		}
		return strconv.Itoa(v.ivfProbes)
	case "vector_index_scan":
		return onOff(!v.noVectorScan)
	case "lock_timeout":
		return strconv.FormatInt(v.lockTimeout.Milliseconds(), 10)
	case "search_path":
		return v.searchPath
	case "server_encoding":
		return "UTF8"
	case "server_version":
		return "14.0 datax"
	case "statement_timeout":
		return strconv.FormatInt(v.statementTimeout.Milliseconds(), 10)
	case "statement_memory_limit":
		return strconv.FormatInt(v.memoryLimit, 10)
	case "TimeZone":
		return v.timeZoneName
	case "transaction_isolation":
		return "serializable" // the truth, whatever was requested
	case "transaction_read_only":
		return onOff(v.txnReadOnly || (s.state != StateOpen && v.defaultReadOnly))
	}
	if pv, ok := pgVars[name]; ok {
		return s.showPGVar(pv)
	}
	return ""
}

func onOff(b bool) string {
	if b {
		return "on"
	}
	return "off"
}

// settings lists the session variables SHOW ALL and pg_settings report,
// by name: the built-in ones and the custom ones this session has set
// (issue #330), as PostgreSQL lists its placeholder variables.
func (s *Session) settings() [][2]string {
	out := make([][2]string, 0, len(varNames)+len(s.vars.custom))
	for _, v := range varNames {
		out = append(out, [2]string{v.name, s.varValue(v.name)})
	}
	for name, value := range s.vars.custom {
		out = append(out, [2]string{name, value})
	}
	sort.SliceStable(out, func(i, j int) bool { return strings.ToLower(out[i][0]) < strings.ToLower(out[j][0]) })
	return out
}

// settingDefaults maps each setting to the value a new session has:
// what this one reports with its own variables reset.
func (s *Session) settingDefaults() map[string]string {
	saved := s.vars
	s.vars = defaultVars()
	defer func() { s.vars = saved }()
	out := make(map[string]string, len(varNames))
	for _, kv := range s.settings() {
		out[kv[0]] = kv[1]
	}
	return out
}

// unrecognizedSettingFunc is the function a per-row current_setting()
// falls to when the row's name is none of the session's settings (see
// resolveValueExprOpts): it raises 42704, or returns NULL with
// missing_ok. A query that names it ("current_setting:unrecognized"(x))
// gets just that, which is harmless.
const unrecognizedSettingFunc = "current_setting:unrecognized"

// setting resolves a SHOW name to the setting's canonical name and value.
func (s *Session) setting(name string) (string, string, bool) {
	if isCustomVar(name) {
		// A custom setting exists once it has been set; before that it is
		// unrecognized (current_setting's missing_ok turns that into NULL).
		lname := strings.ToLower(strings.TrimSpace(name))
		v, ok := s.vars.custom[lname]
		return lname, v, ok
	}
	c := canonicalVar(name)
	if c == "" {
		return "", "", false
	}
	return c, s.varValue(c), true
}

// ReportedParams are the settings the wire announces with ParameterStatus
// when they change (PostgreSQL's GUC_REPORT set).
func (s *Session) ReportedParams() [][2]string {
	return [][2]string{
		{"application_name", s.vars.applicationName},
		{"client_encoding", s.vars.clientEncoding},
		{"DateStyle", s.vars.dateStyle},
		{"TimeZone", s.vars.timeZoneName},
		{"IntervalStyle", s.IntervalStyle()},
	}
}

// TimeZone is the session's zone for rendering TIMESTAMPTZ values on
// the wire (storage and comparison stay UTC).
func (s *Session) TimeZone() *time.Location { return s.vars.timeZone }

// IdleInTransactionTimeout is the session's
// idle_in_transaction_session_timeout (0 = none).
func (s *Session) IdleInTransactionTimeout() time.Duration { return s.vars.idleInTxnTimeout }

// StatementTimeout is the session's statement_timeout (0 = none).
func (s *Session) StatementTimeout() time.Duration { return s.vars.statementTimeout }

// ApplicationName is the session's application_name.
func (s *Session) ApplicationName() string { return s.vars.applicationName }

// defaultMemoryLimit is statement_memory_limit's default: what one
// statement may hold in memory on the gateway (64 MiB).
const defaultMemoryLimit = 64 << 20

// parseMemory reads a memory setting: bytes, or a number with a unit
// (B, kB, MB, GB, TB, in PostgreSQL's binary sense); 0 disables the
// limit.
func parseMemory(value string) (int64, error) {
	v := strings.TrimSpace(strings.Trim(value, "'\""))
	if v == "" {
		return 0, fmt.Errorf("empty memory size")
	}
	i := len(v)
	for i > 0 && (v[i-1] < '0' || v[i-1] > '9') && v[i-1] != '.' {
		i--
	}
	num, unit := strings.TrimSpace(v[:i]), strings.ToLower(strings.TrimSpace(v[i:]))
	f, err := strconv.ParseFloat(num, 64)
	if err != nil || f < 0 {
		return 0, fmt.Errorf("%q is not a memory size", value)
	}
	var mult float64
	switch unit {
	case "", "b":
		mult = 1
	case "kb", "k", "kib":
		mult = 1 << 10
	case "mb", "m", "mib":
		mult = 1 << 20
	case "gb", "g", "gib":
		mult = 1 << 30
	case "tb", "t", "tib":
		mult = 1 << 40
	default:
		return 0, fmt.Errorf("%q is not a memory size (use B, kB, MB, GB or TB)", value)
	}
	return int64(f * mult), nil
}

// parseDuration reads a timeout setting: PostgreSQL's integer
// milliseconds or a number with a unit (us, ms, s, min, h, d); 0
// disables.
func parseDuration(value string) (time.Duration, error) {
	v := strings.TrimSpace(strings.Trim(value, "'\""))
	if v == "" {
		return 0, fmt.Errorf("empty duration")
	}
	i := len(v)
	for i > 0 && (v[i-1] < '0' || v[i-1] > '9') && v[i-1] != '.' {
		i--
	}
	num, unit := strings.TrimSpace(v[:i]), strings.ToLower(strings.TrimSpace(v[i:]))
	f, err := strconv.ParseFloat(num, 64)
	if err != nil || f < 0 {
		return 0, fmt.Errorf("%q is not a non-negative duration", value)
	}
	mult := time.Millisecond
	switch unit {
	case "", "ms":
	case "us":
		mult = time.Microsecond
	case "s":
		mult = time.Second
	case "min":
		mult = time.Minute
	case "h":
		mult = time.Hour
	case "d":
		mult = 24 * time.Hour
	default:
		return 0, fmt.Errorf("unknown unit %q in %q (use us, ms, s, min, h or d)", unit, value)
	}
	return time.Duration(f * float64(mult)), nil
}

func parseOnOff(value string) (bool, error) {
	switch strings.ToLower(strings.TrimSpace(strings.Trim(value, "'\""))) {
	case "on", "true", "yes", "1", "t":
		return true, nil
	case "off", "false", "no", "0", "f":
		return false, nil
	}
	return false, fmt.Errorf("%q is not a boolean", value)
}

// applyVar sets one variable on vars (the session's or a transaction's
// local copy); reset restores the default.
func (s *Session) applyVar(vars *sessionVars, name, value string, reset bool) error {
	def := defaultVars()
	unquote := func(v string) string { return strings.Trim(strings.TrimSpace(v), "'\"") }
	invalid := func(format string, args ...any) error {
		return newErrf(CodeInvalidParameterValue, "invalid value for parameter %q: "+format, append([]any{name}, args...)...)
	}
	switch name {
	case "application_name":
		vars.applicationName = ""
		if !reset {
			vars.applicationName = unquote(value)
		}
	case "role":
		// Validated by execSetVar (it needs a transaction); NONE resets.
		vars.role = ""
		if !reset {
			if v := strings.ToLower(unquote(value)); v != "none" {
				vars.role = v
			}
		}
	case "client_encoding":
		if !reset {
			switch strings.ToUpper(strings.ReplaceAll(unquote(value), "-", "")) {
			case "UTF8", "UNICODE":
			default:
				return invalid("only UTF8 is supported")
			}
		}
		vars.clientEncoding = "UTF8"
	case "DateStyle":
		vars.dateStyle = def.dateStyle
		if !reset {
			v := strings.ToUpper(unquote(value))
			if !strings.HasPrefix(v, "ISO") {
				return invalid("only the ISO date style is supported")
			}
			vars.dateStyle = v
		}
	case "default_transaction_read_only":
		vars.defaultReadOnly = false
		if !reset {
			b, err := parseOnOff(value)
			if err != nil {
				return invalid("%v", err)
			}
			vars.defaultReadOnly = b
		}
	case "transaction_read_only":
		vars.txnReadOnly = false
		if !reset {
			b, err := parseOnOff(value)
			if err != nil {
				return invalid("%v", err)
			}
			vars.txnReadOnly = b
		}
	case "default_text_search_config":
		vars.textSearchConfig = ""
		if !reset {
			cfg, err := tsearch.LookupConfig(unquote(value))
			if err != nil {
				return invalid("%v (the configurations are %s)", err, strings.Join(tsearch.ConfigNames(), " and "))
			}
			vars.textSearchConfig = cfg.Name
		}
	case "inverted_index_scan":
		vars.invertedScan = invertedAuto
		if !reset {
			switch strings.ToLower(unquote(value)) {
			case "auto", "default":
			case "on", "true":
				vars.invertedScan = invertedOn
			case "off", "false":
				vars.invertedScan = invertedOff
			default:
				return invalid("must be auto, on or off, not %q", value)
			}
		}
	case "ivfflat.probes":
		vars.ivfProbes = 0
		if !reset && !strings.EqualFold(unquote(value), "auto") {
			n, err := strconv.Atoi(unquote(value))
			if err != nil || n < 1 || n > maxVectorLists {
				return invalid("must be auto or an integer between 1 and %d, not %q", maxVectorLists, value)
			}
			vars.ivfProbes = n
		}
	case "vector_index_scan":
		vars.noVectorScan = false
		if !reset {
			b, err := parseOnOff(value)
			if err != nil {
				return invalid("%v", err)
			}
			vars.noVectorScan = !b
		}
	case "foreign_key_cascade_limit":
		vars.cascadeLimit, vars.cascadeLimitIsSet = 0, false
		if !reset {
			n, err := strconv.Atoi(unquote(value))
			if err != nil || n < 1 {
				return invalid("must be a positive integer, not %q", value)
			}
			vars.cascadeLimit, vars.cascadeLimitIsSet = n, true
		}
	case "statement_memory_limit":
		vars.memoryLimit = def.memoryLimit
		if !reset {
			n, err := parseMemory(value)
			if err != nil {
				return invalid("%v", err)
			}
			vars.memoryLimit = n
		}
	case "statement_timeout", "lock_timeout", "idle_in_transaction_session_timeout":
		var d time.Duration
		if !reset {
			var err error
			if d, err = parseDuration(value); err != nil {
				return invalid("%v", err)
			}
		}
		switch name {
		case "statement_timeout":
			vars.statementTimeout = d
		case "lock_timeout":
			vars.lockTimeout = d
		default:
			vars.idleInTxnTimeout = d
		}
	case "search_path":
		vars.searchPath = def.searchPath
		if !reset {
			vars.searchPath = unquote(value)
		}
	case "TimeZone":
		vars.timeZone, vars.timeZoneName = time.UTC, "UTC"
		if !reset {
			v := unquote(value)
			if strings.EqualFold(v, "default") || strings.EqualFold(v, "local") {
				v = "UTC"
			}
			// An IANA name or a POSIX offset ('UTC+5' is five hours
			// WEST of Greenwich, as in PostgreSQL); abbreviations are
			// for AT TIME ZONE and timestamp input only.
			loc, err := builtins.ParseZone(v, false)
			if err != nil {
				return invalid("time zone %q not recognized", v)
			}
			if builtins.ZoneIsUTC(loc) {
				loc = time.UTC // the session does no zone work
			}
			vars.timeZone, vars.timeZoneName = loc, v
		}
	case "transaction_isolation":
		vars.isolation = def.isolation
		if !reset {
			switch strings.ToLower(unquote(value)) {
			case "serializable", "repeatable read", "read committed", "read uncommitted":
			default:
				return invalid("unknown isolation level %q", value)
			}
			vars.isolation = strings.ToLower(unquote(value))
		}
	case "integer_datetimes", "standard_conforming_strings", "server_encoding", "server_version":
		if !reset {
			return newErrf(CodeInvalidParameterValue, "parameter %q cannot be changed", name)
		}
	default:
		if pv, ok := pgVars[name]; ok {
			return applyPGVar(vars, pv, value, reset)
		}
		return newErrf(CodeUndefinedObject, "unrecognized configuration parameter %q", name)
	}
	return nil
}

// execSetVar runs SET / SET LOCAL / RESET.
func (s *Session) execSetVar(ctx context.Context, t *parser.SetVar) (*Result, *Error) {
	if t.Name == "database" || t.Name == "session_authorization" && false {
		if t.Reset {
			return &Result{Tag: "RESET"}, nil
		}
		if serr := s.UseDatabase(ctx, strings.Trim(t.Value, "'\"")); serr != nil {
			return nil, serr
		}
		return &Result{Tag: "SET"}, nil
	}
	tag := "SET"
	if t.Reset {
		tag = "RESET"
		if t.Name == "" {
			// RESET ALL: every variable to its default (the database stays).
			s.vars = defaultVars()
			s.localVars = nil
			s.applyRole()
			return &Result{Tag: tag}, nil
		}
	}
	if isCustomVar(t.Name) {
		// A custom setting takes any text; RESET leaves it set and empty,
		// as PostgreSQL does. SET LOCAL lasts until the block ends.
		if t.Local && s.state != StateOpen {
			return &Result{Tag: tag}, nil
		}
		if t.Local && s.localVars == nil {
			saved := s.vars
			s.localVars = &saved
		}
		value := t.Value
		if t.Reset {
			value = ""
		}
		setCustomVar(&s.vars, strings.ToLower(t.Name), value)
		// set_config() arrives here mid-statement: whatever the statement
		// reads next binds its policies to the new value.
		s.policyMemo = nil
		return &Result{Tag: tag}, nil
	}
	name := canonicalVar(t.Name)
	if name == "" {
		return nil, newErrf(CodeUndefinedObject, "unrecognized configuration parameter %q", t.Name)
	}
	if t.Local && s.state != StateOpen {
		// PostgreSQL warns and applies nothing outside a block.
		return &Result{Tag: tag}, nil
	}
	if name == "role" && !t.Reset {
		role := strings.ToLower(strings.Trim(t.Value, "'\""))
		if role != "none" {
			if serr := s.checkSetRole(ctx, role); serr != nil {
				return nil, serr
			}
		}
	}
	target := &s.vars
	if t.Local || name == "transaction_read_only" && s.state == StateOpen {
		if s.localVars == nil {
			saved := s.vars
			s.localVars = &saved
		}
	}
	if err := s.applyVar(target, name, t.Value, t.Reset); err != nil {
		return nil, ToSQLError(err)
	}
	switch name {
	case "role":
		s.applyRole()
	case "transaction_read_only":
		// SET TRANSACTION READ WRITE lifts a block's default read-only
		// start; READ ONLY sets it.
		if s.state == StateOpen {
			s.txnStartedReadOnly = s.vars.txnReadOnly
		}
	}
	return &Result{Tag: tag}, nil
}

// endTxnVars restores the session's variables after a transaction block
// that SET LOCAL (or SET TRANSACTION) changed them.
func (s *Session) endTxnVars() {
	if s.localVars != nil {
		s.vars = *s.localVars
		s.localVars = nil
		s.applyRole()
	}
	s.vars.txnReadOnly = false
}

// readOnly reports whether the statement running now is in a read-only
// transaction: SET TRANSACTION / BEGIN READ ONLY, or
// default_transaction_read_only for an implicit or a block's start.
func (s *Session) readOnly() bool {
	return s.vars.txnReadOnly || (s.state != StateOpen && s.vars.defaultReadOnly) || (s.state == StateOpen && s.txnStartedReadOnly)
}

// readOnlyViolation refuses a write in a read-only transaction (25006).
func (s *Session) readOnlyViolation(stmt parser.Statement) *Error {
	if !s.readOnly() {
		return nil
	}
	if kind := writeKind(stmt); kind != "" {
		return newErrf(CodeReadOnlyTransaction, "cannot execute %s in a read-only transaction", kind)
	}
	return nil
}

// writeKind names a statement that writes ("" for a read).
func writeKind(stmt parser.Statement) string {
	switch stmt.(type) {
	case *parser.Insert:
		return "INSERT"
	case *parser.Update:
		return "UPDATE"
	case *parser.Delete:
		return "DELETE"
	case *parser.CopyFrom:
		return "COPY"
	case *parser.CreateTable, *parser.DropTable, *parser.AlterTable, *parser.CreateIndex, *parser.DropIndex, *parser.AlterIndex,
		*parser.CreateSequence, *parser.AlterSequence, *parser.DropSequence, *parser.CreateType, *parser.AlterType, *parser.DropType,
		*parser.CreateView, *parser.DropView, *parser.CreateDatabase, *parser.DropDatabase, *parser.AlterDatabase,
		*parser.CreateRole, *parser.DropRole, *parser.GrantRevoke, *parser.AlterOwner, *parser.ReassignOwned, *parser.DropOwned,
		*parser.AlterDefaultPrivileges, *parser.Truncate, *parser.CommentOn, *parser.Analyze,
		*parser.CreatePolicy, *parser.AlterPolicy, *parser.DropPolicy,
		*parser.CreateEncryptionKey, *parser.AlterEncryptionKey, *parser.DropEncryptionKey, *parser.SetClusterSetting:
		return "DDL"
	}
	return ""
}

// SessionInfo is one session of this node as SHOW SESSIONS and
// pg_stat_activity report it.
type SessionInfo = vtable.SessionInfo

// sessionRows renders the sessions the hook reports. A statement's text
// carries its literals — another tenant's keys, on a table shared under
// row security — so a role that is not an admin reads the text of its
// own sessions only, as in PostgreSQL.
func (s *Session) sessionRows(admin bool) [][]types.Datum {
	if s.SessionsHook == nil {
		return nil
	}
	var rows [][]types.Datum
	for _, si := range s.SessionsHook() {
		ts := func(t time.Time) types.Datum {
			if t.IsZero() {
				return types.DNull
			}
			return types.NewTimestamp(t.UnixNano())
		}
		query := si.Query
		if !admin && si.User != s.sessionUser {
			query = vtable.QueryHidden
		}
		rows = append(rows, []types.Datum{types.NewInt(int64(si.PID)), types.NewString(si.User), types.NewString(si.Database), types.NewString(si.Application),
			types.NewString(si.ClientAddr), types.NewString(si.State), types.NewString(query), ts(si.BackendStart), ts(si.QueryStart), ts(si.XactStart)})
	}
	return rows
}

// textSearchConfig is the session's default text-search configuration.
func (s *Session) textSearchConfig() string {
	if s.vars.textSearchConfig == "" {
		return tsearch.DefaultConfig
	}
	return s.vars.textSearchConfig
}
