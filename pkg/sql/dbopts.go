package sql

import (
	"context"
	"sort"
	"strconv"
	"strings"

	"github.com/sthorne/datax/pkg/kvclient"
	"github.com/sthorne/datax/pkg/sql/catalog"
	"github.com/sthorne/datax/pkg/sql/parser"
	"github.com/sthorne/datax/pkg/util/log"
)

// CREATE DATABASE's options and ALTER DATABASE's forms (PG-D-022), as
// Rails, Django and pg_dump send them. datax has one encoding (UTF8),
// one collation (bytewise, C) and no template databases to copy, so the
// options that would ask for anything else are refused with PostgreSQL's
// errors, and the ones that ask for what datax is are accepted.

// pgEncodings are PostgreSQL's server encoding names, normalised as
// pg_char_to_encoding does (lower case, alphanumerics only), with its
// aliases.
var pgEncodings = map[string]string{
	"utf8": "UTF8", "unicode": "UTF8", "sqlascii": "SQL_ASCII",
	"eucjp": "EUC_JP", "euccn": "EUC_CN", "euckr": "EUC_KR", "euctw": "EUC_TW", "eucjis2004": "EUC_JIS_2004",
	"muleinternal": "MULE_INTERNAL", "latin1": "LATIN1", "latin2": "LATIN2", "latin3": "LATIN3", "latin4": "LATIN4",
	"latin5": "LATIN5", "latin6": "LATIN6", "latin7": "LATIN7", "latin8": "LATIN8", "latin9": "LATIN9", "latin10": "LATIN10",
	"iso88591": "LATIN1", "iso88592": "LATIN2", "iso88593": "LATIN3", "iso88594": "LATIN4", "iso88599": "LATIN5",
	"iso885910": "LATIN6", "iso885913": "LATIN7", "iso885914": "LATIN8", "iso885915": "LATIN9", "iso885916": "LATIN10",
	"iso88595": "ISO_8859_5", "iso88596": "ISO_8859_6", "iso88597": "ISO_8859_7", "iso88598": "ISO_8859_8",
	"win866": "WIN866", "win874": "WIN874", "win1250": "WIN1250", "win1251": "WIN1251", "win1252": "WIN1252",
	"win1253": "WIN1253", "win1254": "WIN1254", "win1255": "WIN1255", "win1256": "WIN1256", "win1257": "WIN1257",
	"win1258": "WIN1258", "koi8r": "KOI8R", "koi8u": "KOI8U", "koi8": "KOI8R",
	"sjis": "SJIS", "shiftjis2004": "SHIFT_JIS_2004", "big5": "BIG5", "gbk": "GBK", "uhc": "UHC", "gb18030": "GB18030", "johab": "JOHAB",
}

// normaliseEncoding is PostgreSQL's clean_encoding_name.
func normaliseEncoding(name string) string {
	var b strings.Builder
	for _, r := range strings.ToLower(name) {
		if r >= 'a' && r <= 'z' || r >= '0' && r <= '9' {
			b.WriteRune(r)
		}
	}
	return b.String()
}

// supportedLocales are the locale names a database may be created with:
// the C locale and the UTF-8 locales whose behaviour datax's (bytewise)
// text comparison stands in for. The value is the name reported.
var supportedLocales = map[string]string{
	"c": "C", "posix": "POSIX", "c.utf8": "C.UTF-8", "c.utf-8": "C.UTF-8",
	"en_us.utf8": "en_US.UTF-8", "en_us.utf-8": "en_US.UTF-8",
}

// checkDatabaseLocale validates a locale name for category ("LC_COLLATE",
// "LC_CTYPE") and returns the name to record.
func checkDatabaseLocale(category, name string) (string, error) {
	if v, ok := supportedLocales[strings.ToLower(name)]; ok {
		return v, nil
	}
	return "", &Error{Code: CodeWrongObjectType, Msg: "invalid " + category + " locale name: \"" + name + "\"",
		Hint: "datax supports the locales C, POSIX, C.UTF-8 and en_US.UTF-8; text compares bytewise, as in C."}
}

// parseConnLimit reads a CONNECTION LIMIT value.
func parseConnLimit(o parser.DatabaseOption) (*int64, error) {
	if o.Default {
		return nil, nil
	}
	n, err := strconv.ParseInt(o.Value, 10, 32)
	if err != nil {
		return nil, newErrf(CodeInvalidParameterValue, "invalid connection limit: %q", o.Value)
	}
	if n < -1 {
		return nil, newErrf(CodeInvalidParameterValue, "invalid connection limit: %d", n)
	}
	if n == -1 {
		return nil, nil
	}
	return &n, nil
}

// optionBool reads a Boolean option value as defGetBoolean does.
func optionBool(o parser.DatabaseOption) (bool, error) {
	switch strings.ToLower(o.Value) {
	case "true", "on", "1", "t", "yes":
		return true, nil
	case "false", "off", "0", "f", "no":
		return false, nil
	}
	return false, newErrf(CodeSyntaxError, "%s requires a Boolean value", o.Name)
}

// applyCreateDatabaseOptions validates CREATE DATABASE's PostgreSQL
// options and records what they set on d.
func (s *Session) applyCreateDatabaseOptions(ctx context.Context, txn *kvclient.Txn, d *catalog.DatabaseDescriptor, opts []parser.DatabaseOption) error {
	var locale, collate, ctype string
	for _, o := range opts {
		if o.Default && o.Name != "owner" && o.Name != "connection_limit" {
			continue // DEFAULT is the template's value: what datax has.
		}
		switch o.Name {
		case "owner":
			if o.Default {
				continue
			}
			owner := o.Value
			if _, err := s.lookupRoleOrErr(ctx, txn, owner); err != nil {
				return err
			}
			if err := s.checkCanBecome(ctx, txn, owner); err != nil {
				return err
			}
			d.Owner = owner
		case "encoding":
			enc := ""
			if n, err := strconv.Atoi(o.Value); err == nil {
				if n == 6 {
					enc = "UTF8"
				} else {
					return &Error{Code: CodeUndefinedObject, Msg: o.Value + " is not a valid encoding code"}
				}
			} else if e, ok := pgEncodings[normaliseEncoding(o.Value)]; ok {
				enc = e
			} else {
				return &Error{Code: CodeUndefinedObject, Msg: "\"" + o.Value + "\" is not a valid encoding name"}
			}
			if enc != "UTF8" {
				return &Error{Code: CodeInvalidParameterValue, Msg: "encoding \"" + enc + "\" is not supported",
					Detail: "datax stores all text as UTF8: a database's encoding is always UTF8."}
			}
		case "template":
			switch o.Value {
			case "template0", "template1":
			default:
				if _, err := catalog.LookupDatabase(ctx, txn, o.Value); err != nil {
					var nf *catalog.ErrDatabaseNotFound
					if asErr(err, &nf) {
						return &Error{Code: CodeInvalidCatalogName, Msg: "template database \"" + o.Value + "\" does not exist"}
					}
					return err
				}
				return &Error{Code: CodeFeatureNotSupported, Msg: "datax cannot copy database \"" + o.Value + "\": TEMPLATE takes template0 or template1, which both stand for an empty database"}
			}
		case "locale":
			locale = o.Value
		case "lc_collate":
			collate = o.Value
		case "lc_ctype":
			ctype = o.Value
		case "locale_provider":
			switch strings.ToLower(o.Value) {
			case "libc", "builtin":
			case "icu":
				return &Error{Code: CodeFeatureNotSupported, Msg: "ICU is not supported in this build"}
			default:
				return &Error{Code: CodeInvalidParameterValue, Msg: "unrecognized locale provider: " + o.Value}
			}
		case "icu_locale", "icu_rules":
			return &Error{Code: CodeFeatureNotSupported, Msg: "ICU is not supported in this build"}
		case "strategy":
			switch strings.ToLower(o.Value) {
			case "wal_log", "file_copy":
			default:
				return &Error{Code: CodeInvalidParameterValue, Msg: "invalid create database strategy \"" + o.Value + "\"",
					Hint: "Valid strategies are \"wal_log\", and \"file_copy\"."}
			}
		case "tablespace":
			if o.Value != "pg_default" {
				return &Error{Code: CodeUndefinedObject, Msg: "tablespace \"" + o.Value + "\" does not exist"}
			}
		case "connection_limit":
			n, err := parseConnLimit(o)
			if err != nil {
				return err
			}
			d.ConnLimit = n
		case "allow_connections":
			b, err := optionBool(o)
			if err != nil {
				return err
			}
			if !b {
				return newErrf(CodeFeatureNotSupported, "ALLOW_CONNECTIONS false is not supported")
			}
		case "is_template":
			b, err := optionBool(o)
			if err != nil {
				return err
			}
			if b {
				return newErrf(CodeFeatureNotSupported, "IS_TEMPLATE true is not supported: datax has no template databases")
			}
		case "collation_version":
			// Recorded by PostgreSQL to notice a changed collation
			// library; datax's collation does not change.
		case "oid":
			return newErrf(CodeFeatureNotSupported, "CREATE DATABASE ... OID is not supported")
		}
	}
	if locale != "" {
		if collate == "" {
			collate = locale
		}
		if ctype == "" {
			ctype = locale
		}
	}
	if collate != "" {
		v, err := checkDatabaseLocale("LC_COLLATE", collate)
		if err != nil {
			return err
		}
		d.Collate = v
	}
	if ctype != "" {
		v, err := checkDatabaseLocale("LC_CTYPE", ctype)
		if err != nil {
			return err
		}
		d.Ctype = v
	}
	return nil
}

// checkCanBecome refuses (42501) making role the owner of a new object
// unless the actor is an admin or a member of it, as PostgreSQL asks
// that the creator be able to SET ROLE to the owner.
func (s *Session) checkCanBecome(ctx context.Context, txn *kvclient.Txn, role string) error {
	if s.actor() == catalog.RootRole || s.actor() == role {
		return nil
	}
	set, err := s.roleSet(ctx, txn)
	if err != nil {
		return err
	}
	if set.IsAdmin() || set.Has(role) {
		return nil
	}
	return newErrf(CodeInsufficientPriv, "must be able to SET ROLE %q", role)
}

// canCreateDatabase reports whether the actor may run CREATE DATABASE:
// an admin, or a role with the CREATEDB attribute — its own, not
// inherited, as in PostgreSQL.
func (s *Session) canCreateDatabase(ctx context.Context, txn *kvclient.Txn) (bool, error) {
	if ok, err := s.isAdmin(ctx, txn); err != nil || ok {
		return ok, err
	}
	r, err := catalog.LookupRole(ctx, txn, s.actor())
	if err != nil || r == nil {
		return false, err
	}
	return r.CreateDB, nil
}

// execAlterDatabaseOptions runs ALTER DATABASE ... SET / RESET param,
// CONNECTION LIMIT, ALLOW_CONNECTIONS, IS_TEMPLATE, SET TABLESPACE and
// REFRESH COLLATION VERSION: the owner's (or an admin's).
func (s *Session) execAlterDatabaseOptions(ctx context.Context, txn *kvclient.Txn, t *parser.AlterDatabase) (*Result, error) {
	if err := s.requireV6(ctx, txn); err != nil {
		return nil, err
	}
	db, err := catalog.LookupDatabase(ctx, txn, t.Name)
	if err != nil {
		var nf *catalog.ErrDatabaseNotFound
		if asErr(err, &nf) {
			return nil, newErrf(CodeInvalidCatalogName, "database %q does not exist", t.Name)
		}
		return nil, ToSQLError(err)
	}
	if t.Name == catalog.SystemDatabase && !s.system {
		return nil, newErrf(CodeInsufficientPriv, "database %q is reserved for the cluster", t.Name)
	}
	if err := s.checkOwner(ctx, txn, "database", db.Name, db.Owner); err != nil {
		return nil, err
	}
	if t.RefreshCollation {
		// PostgreSQL's NOTICE when the version has not changed: datax's
		// collation never does.
		return &Result{Tag: "ALTER DATABASE", Notices: []string{"version has not changed"}}, nil
	}
	d := db.Clone()
	if t.Set != nil {
		if err := s.applyDatabaseSetting(d, t.Set); err != nil {
			return nil, err
		}
	}
	for _, o := range t.Options {
		switch o.Name {
		case "connection_limit":
			n, err := parseConnLimit(o)
			if err != nil {
				return nil, err
			}
			d.ConnLimit = n
		case "allow_connections":
			b, err := optionBool(o)
			if err != nil {
				return nil, err
			}
			if !b {
				return nil, newErrf(CodeFeatureNotSupported, "ALLOW_CONNECTIONS false is not supported")
			}
		case "is_template":
			b, err := optionBool(o)
			if err != nil {
				return nil, err
			}
			if b {
				return nil, newErrf(CodeFeatureNotSupported, "IS_TEMPLATE true is not supported: datax has no template databases")
			}
		case "tablespace":
			if o.Value != "pg_default" {
				return nil, newErrf(CodeUndefinedObject, "tablespace %q does not exist", o.Value)
			}
		}
	}
	if err := s.cat.UpdateDatabase(ctx, txn, d); err != nil {
		return nil, err
	}
	log.Audit("database-ddl", "stmt", "ALTER DATABASE", "target", t.Name, "connlimit", d.ConnectionLimit(), "principal", s.sessionUser, "role", s.user)
	return &Result{Tag: "ALTER DATABASE"}, nil
}

// applyDatabaseSetting records ALTER DATABASE ... SET / RESET on d,
// checking the parameter and its value as SET would.
func (s *Session) applyDatabaseSetting(d *catalog.DatabaseDescriptor, sv *parser.SetVar) error {
	if sv.Reset && sv.Name == "" {
		d.Settings = nil // RESET ALL
		return nil
	}
	name := strings.ToLower(strings.TrimSpace(sv.Name))
	if !isCustomVar(name) {
		name = canonicalVar(sv.Name)
		switch name {
		case "":
			return newErrf(CodeUndefinedObject, "unrecognized configuration parameter %q", sv.Name)
		case "role", "transaction_isolation", "transaction_read_only", "transaction_deferrable":
			return newErrf(CodeCantChangeRuntimeParam, "parameter %q cannot be set as a database default", sv.Name)
		}
		if !sv.Reset {
			scratch := s.vars
			if err := s.applyVar(&scratch, name, sv.Value, false); err != nil {
				return err
			}
		}
	}
	if sv.Reset {
		delete(d.Settings, name)
		if len(d.Settings) == 0 {
			d.Settings = nil
		}
		return nil
	}
	if d.Settings == nil {
		d.Settings = map[string]string{}
	}
	d.Settings[name] = sv.Value
	return nil
}

// StartupInfo is what a new session's start needs from the catalog:
// the CONNECTION LIMITs of its role and database (-1 for none), whether
// the role is exempt from them (an admin, as PostgreSQL exempts
// superusers), and the database's ALTER DATABASE ... SET defaults.
type StartupInfo struct {
	RoleLimit, DatabaseLimit int64
	Exempt                   bool
	Settings                 map[string]string
}

// StartupInfo reads the session's StartupInfo in one transaction.
func (s *Session) StartupInfo(ctx context.Context) (StartupInfo, *Error) {
	info := StartupInfo{RoleLimit: -1, DatabaseLimit: -1}
	if s.db == nil {
		return info, nil // a session without a cluster (wire-layer tests)
	}
	err := s.db.RunTxn(ctx, "session-startup", func(ctx context.Context, txn *kvclient.Txn) error {
		info = StartupInfo{RoleLimit: -1, DatabaseLimit: -1}
		d, err := catalog.LookupDatabase(ctx, txn, s.database)
		if err != nil {
			return err
		}
		info.Settings = d.Settings
		info.DatabaseLimit = d.ConnectionLimit()
		r, err := catalog.LookupRole(ctx, txn, s.sessionUser)
		if err != nil {
			return err
		}
		if r != nil {
			info.RoleLimit = r.ConnectionLimit()
		}
		if info.RoleLimit < 0 && info.DatabaseLimit < 0 {
			return nil
		}
		info.Exempt, err = s.isAdmin(ctx, txn)
		return err
	})
	if err != nil {
		return StartupInfo{RoleLimit: -1, DatabaseLimit: -1}, ToSQLError(err)
	}
	return info, nil
}

// ApplyDatabaseDefaults sets the session's parameters to the defaults
// ALTER DATABASE ... SET recorded for its database (StartupInfo's
// Settings), as PostgreSQL does when a session starts. A default that
// no longer applies is skipped, as PostgreSQL warns and goes on.
func (s *Session) ApplyDatabaseDefaults(ctx context.Context, settings map[string]string) {
	names := make([]string, 0, len(settings))
	for k := range settings {
		names = append(names, k)
	}
	sort.Strings(names)
	for _, k := range names {
		if _, serr := s.execSetVar(ctx, &parser.SetVar{Name: k, Value: settings[k]}); serr != nil {
			log.Warnf("database %q default %s = %s: %v", s.database, k, settings[k], serr)
		}
	}
}
