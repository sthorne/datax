package sql

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/sthorne/datax/pkg/sql/catalog"
	"github.com/sthorne/datax/pkg/sql/parser"
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
	// role is SET ROLE's role ("" = none: the session user).
	role string
}

func defaultVars() sessionVars {
	return sessionVars{searchPath: catalog.PublicSchema, timeZone: time.UTC, timeZoneName: "UTC", dateStyle: "ISO, MDY", clientEncoding: "UTF8", isolation: "serializable"}
}

// varNames are the settings in SHOW ALL order, with their pg_settings
// vartype and unit.
var varNames = []struct{ name, vartype, unit string }{
	{"application_name", "string", ""},
	{"client_encoding", "string", ""},
	{"database", "string", ""},
	{"DateStyle", "string", ""},
	{"default_transaction_read_only", "bool", ""},
	{"foreign_key_cascade_limit", "integer", ""},
	{"idle_in_transaction_session_timeout", "integer", "ms"},
	{"integer_datetimes", "bool", ""},
	{"lock_timeout", "integer", "ms"},
	{"role", "string", ""},
	{"search_path", "string", ""},
	{"server_encoding", "string", ""},
	{"server_version", "string", ""},
	{"standard_conforming_strings", "bool", ""},
	{"statement_timeout", "integer", "ms"},
	{"TimeZone", "string", ""},
	{"transaction_isolation", "string", ""},
	{"transaction_read_only", "bool", ""},
}

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
	case "default_transaction_read_only":
		return onOff(v.defaultReadOnly)
	case "foreign_key_cascade_limit":
		return strconv.Itoa(s.fkCascadeLimit())
	case "idle_in_transaction_session_timeout":
		return strconv.FormatInt(v.idleInTxnTimeout.Milliseconds(), 10)
	case "integer_datetimes", "standard_conforming_strings":
		return "on"
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
	case "TimeZone":
		return v.timeZoneName
	case "transaction_isolation":
		return "serializable" // the truth, whatever was requested
	case "transaction_read_only":
		return onOff(v.txnReadOnly || (s.state != StateOpen && v.defaultReadOnly))
	}
	return ""
}

func onOff(b bool) string {
	if b {
		return "on"
	}
	return "off"
}

// settings lists the session variables SHOW ALL and pg_settings report.
func (s *Session) settings() [][2]string {
	out := make([][2]string, 0, len(varNames))
	for _, v := range varNames {
		out = append(out, [2]string{v.name, s.varValue(v.name)})
	}
	return out
}

// setting resolves a SHOW name to the setting's canonical name and value.
func (s *Session) setting(name string) (string, string, bool) {
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
	case "foreign_key_cascade_limit":
		vars.cascadeLimit, vars.cascadeLimitIsSet = 0, false
		if !reset {
			n, err := strconv.Atoi(unquote(value))
			if err != nil || n < 1 {
				return invalid("must be a positive integer, not %q", value)
			}
			vars.cascadeLimit, vars.cascadeLimitIsSet = n, true
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
			loc, err := loadTimeZone(v)
			if err != nil {
				return invalid("time zone %q not recognized", v)
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
		return newErrf(CodeUndefinedObject, "unrecognized configuration parameter %q", name)
	}
	return nil
}

// loadTimeZone resolves a TimeZone value: an IANA name, "UTC", or a
// fixed offset ("+05:30", "-8", "UTC+2" in PostgreSQL's POSIX sense is
// not supported).
func loadTimeZone(v string) (*time.Location, error) {
	if strings.EqualFold(v, "utc") || strings.EqualFold(v, "gmt") || strings.EqualFold(v, "z") {
		return time.UTC, nil
	}
	if v != "" && (v[0] == '+' || v[0] == '-') {
		sign := 1
		if v[0] == '-' {
			sign = -1
		}
		parts := strings.SplitN(v[1:], ":", 2)
		h, err := strconv.Atoi(parts[0])
		if err != nil {
			return nil, err
		}
		m := 0
		if len(parts) == 2 {
			if m, err = strconv.Atoi(parts[1]); err != nil {
				return nil, err
			}
		}
		return time.FixedZone(v, sign*(h*3600+m*60)), nil
	}
	return time.LoadLocation(v)
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
			s.cascadeLimit = 0
			s.applyRole()
			return &Result{Tag: tag}, nil
		}
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
	case "foreign_key_cascade_limit":
		s.cascadeLimit = s.vars.cascadeLimit
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

// readOnlyViolation refuses a write in a read-only transaction (25006).
func (s *Session) readOnlyViolation(stmt parser.Statement) *Error {
	readOnly := s.vars.txnReadOnly || (s.state != StateOpen && s.vars.defaultReadOnly) || (s.state == StateOpen && s.txnStartedReadOnly)
	if !readOnly {
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
		*parser.AlterDefaultPrivileges, *parser.Truncate, *parser.CommentOn, *parser.Analyze:
		return "DDL"
	}
	return ""
}

// SessionInfo is one session of this node as SHOW SESSIONS and
// pg_stat_activity report it.
type SessionInfo = vtable.SessionInfo

// sessionRows renders the sessions the hook reports.
func (s *Session) sessionRows() [][]types.Datum {
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
		rows = append(rows, []types.Datum{types.NewInt(int64(si.PID)), types.NewString(si.User), types.NewString(si.Database), types.NewString(si.Application),
			types.NewString(si.ClientAddr), types.NewString(si.State), types.NewString(si.Query), ts(si.BackendStart), ts(si.QueryStart), ts(si.XactStart)})
	}
	return rows
}
