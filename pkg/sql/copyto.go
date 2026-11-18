package sql

import (
	"context"
	"strings"

	"github.com/sthorne/datax/pkg/kvclient"
	"github.com/sthorne/datax/pkg/sql/catalog"
	"github.com/sthorne/datax/pkg/sql/parser"
)

// COPY ... TO STDOUT (issue #365). A COPY TO is a query: COPY t [(cols)]
// TO reads as SELECT cols FROM t and COPY (query) TO as the query, so
// Execute rewrites the statement into that query and runs it the
// ordinary way. Everything a SELECT gets, COPY TO gets with it: the
// privilege checks and row-level security (PostgreSQL also turns COPY t
// TO into a query when the table has row security), one transaction and
// so one snapshot for the whole result, streaming in pages for a
// scan-shaped read (bounded memory however large the table), the
// statement timeout, and cancellation. The result carries the COPY's
// format (Result.Copy), and the wire layer sends its rows as CopyData
// instead of DataRows; the encoders live there (pkg/pgwire/copyto.go).

// CopyOut is the output format of a COPY TO, its options validated and
// defaults filled in as PostgreSQL's ProcessCopyOptions does.
type CopyOut struct {
	Format parser.CopyFormat
	// Delim is the column delimiter (text and CSV).
	Delim byte
	// Null is the string a NULL is written as (text and CSV).
	Null string
	// Header asks for a first line of column names (text and CSV).
	Header bool
	// Quote and Escape are CSV's quote and escape characters.
	Quote, Escape byte
	// ForceQuote marks, per output column, a CSV column whose non-NULL
	// values are always quoted (FORCE_QUOTE); ForceQuoteAll is
	// FORCE_QUOTE *.
	ForceQuote    []bool
	ForceQuoteAll bool
}

// copyFileHint is PostgreSQL's hint for a COPY to or from the server's
// files that the role may not run.
const copyFileHint = `Anyone can COPY to stdout or from stdin. psql's \copy command also works for anyone.`

// planCopyTo validates a COPY TO and returns the query it runs and the
// format its rows are written in.
func (s *Session) planCopyTo(ctx context.Context, ct *parser.CopyTo) (parser.Statement, *CopyOut, error) {
	if ct.File != "" {
		return nil, nil, s.refuseCopyFile(ctx, ct)
	}
	co, forceQuote, err := processCopyToOptions(ct.Options)
	if err != nil {
		return nil, nil, err
	}
	if ct.Query != nil {
		return s.planCopyQuery(ctx, ct, co, forceQuote)
	}

	// COPY table [(cols)] TO: the table must be a table, and the
	// columns its own, each named once.
	var visible []catalog.Column
	err = s.copyPlanTxn(ctx, func(ctx context.Context, txn *kvclient.Txn) error {
		desc, err := s.lookup(ctx, txn, ct.Table)
		if err != nil {
			return err
		}
		if desc.Virtual == "sequence" {
			// A sequence reads as a relation, but is not a table to copy.
			return newErrf(CodeWrongObjectType, "cannot copy from sequence %q", desc.Name)
		}
		if desc.IsView() {
			return &Error{Code: CodeWrongObjectType, Msg: "cannot copy from view \"" + desc.Name + "\"",
				Hint: "Try the COPY (SELECT ...) TO variant."}
		}
		visible = desc.VisibleColumns()
		return nil
	})
	if err != nil {
		return nil, nil, err
	}
	_, bare := catalog.SplitTableName(ct.Table)
	names := make([]string, 0, len(visible))
	if len(ct.Columns) == 0 {
		for _, c := range visible {
			names = append(names, c.Name)
		}
	} else {
		seen := map[string]bool{}
		for _, name := range ct.Columns {
			if !hasColumn(visible, name) {
				return nil, nil, newErrf(CodeUndefinedColumn, "column %q of relation %q does not exist", name, bare)
			}
			if seen[name] {
				return nil, nil, newErrf(CodeDuplicateColumn, "column %q specified more than once", name)
			}
			seen[name] = true
			names = append(names, name)
		}
	}
	if forceQuote != nil {
		co.ForceQuote = make([]bool, len(names))
		for _, name := range forceQuote {
			if !hasColumn(visible, name) {
				return nil, nil, newErrf(CodeUndefinedColumn, "column %q of relation %q does not exist", name, bare)
			}
			i := indexOf(names, name)
			if i < 0 {
				return nil, nil, newErrf(CodeInvalidColumnReference, "FORCE_QUOTE column %q not referenced by COPY", name)
			}
			co.ForceQuote[i] = true
		}
	}

	// The query, as text: the parser builds the SELECT the way it builds
	// any other.
	var b strings.Builder
	b.WriteString("SELECT ")
	for i, name := range names {
		if i > 0 {
			b.WriteString(", ")
		}
		b.WriteString(copyQuoteIdent(name))
	}
	if len(names) == 0 {
		b.WriteString("*")
	}
	b.WriteString(" FROM ")
	if db, rel := catalog.SplitTableName(ct.Table); db != "" {
		b.WriteString(copyQuoteIdent(db) + "." + copyQuoteIdent(rel))
	} else {
		b.WriteString(copyQuoteIdent(rel))
	}
	stmts, err := parser.Parse(b.String())
	if err != nil {
		return nil, nil, err
	}
	return stmts[0], co, nil
}

// planCopyQuery validates COPY (query) TO.
func (s *Session) planCopyQuery(ctx context.Context, ct *parser.CopyTo, co *CopyOut, forceQuote []string) (parser.Statement, *CopyOut, error) {
	switch t := ct.Query.(type) {
	case *parser.Insert:
		if t.Returning == nil {
			return nil, nil, newErrf(CodeFeatureNotSupported, "COPY query must have a RETURNING clause")
		}
	case *parser.Update:
		if t.Returning == nil {
			return nil, nil, newErrf(CodeFeatureNotSupported, "COPY query must have a RETURNING clause")
		}
	case *parser.Delete:
		if t.Returning == nil {
			return nil, nil, newErrf(CodeFeatureNotSupported, "COPY query must have a RETURNING clause")
		}
	}
	// COPY is a utility statement: its query takes no parameters, and
	// PostgreSQL's analysis of it reports the first $n it meets.
	first := 0
	note := func(n int) {
		if first == 0 && n > 0 {
			first = n
		}
	}
	w := parser.Walker{
		OnExpr:  func(e *parser.Expr) bool { note(e.Param); return true },
		OnParam: note,
	}
	w.Statement(ct.Query)
	if first > 0 {
		return nil, nil, newErrf(CodeUndefinedParameter, "there is no parameter $%d", first)
	}
	if forceQuote != nil {
		cols, serr := s.PlanColumns(ctx, ct.Query)
		if serr != nil {
			return nil, nil, serr
		}
		co.ForceQuote = make([]bool, len(cols))
		for _, name := range forceQuote {
			found := false
			for i, c := range cols {
				if c.Name == name {
					co.ForceQuote[i], found = true, true
				}
			}
			if !found {
				return nil, nil, newErrf(CodeUndefinedColumn, "column %q does not exist", name)
			}
		}
	}
	return ct.Query, co, nil
}

// refuseCopyFile refuses COPY TO a server file or program. PostgreSQL
// lets only roles with pg_write_server_files (pg_execute_server_program)
// do it; datax has no server files to write, so the roles that would hold
// those (root and the admins) are told it is not supported, and every
// other role gets PostgreSQL's privilege error.
func (s *Session) refuseCopyFile(ctx context.Context, ct *parser.CopyTo) error {
	var admin bool
	err := s.copyPlanTxn(ctx, func(ctx context.Context, txn *kvclient.Txn) error {
		var err error
		admin, err = s.isAdmin(ctx, txn)
		return err
	})
	if err != nil {
		return err
	}
	switch {
	case admin && ct.Program:
		return &Error{Code: CodeFeatureNotSupported, Msg: "COPY TO PROGRAM is not supported", Hint: copyFileHint}
	case admin:
		return &Error{Code: CodeFeatureNotSupported, Msg: "COPY to a file is not supported", Hint: copyFileHint}
	case ct.Program:
		return &Error{Code: CodeInsufficientPriv, Msg: "permission denied to COPY to or from an external program",
			Detail: `Only roles with privileges of the "pg_execute_server_program" role may COPY to or from an external program.`,
			Hint:   copyFileHint}
	}
	return &Error{Code: CodeInsufficientPriv, Msg: "permission denied to COPY to a file",
		Detail: `Only roles with privileges of the "pg_write_server_files" role may COPY to a file.`,
		Hint:   copyFileHint}
}

// copyPlanTxn runs fn in the session's open transaction, or in a
// throwaway one outside a block (a table this block created is visible
// to its COPY).
func (s *Session) copyPlanTxn(ctx context.Context, fn func(ctx context.Context, txn *kvclient.Txn) error) error {
	if s.state == StateOpen {
		return fn(ctx, s.txn)
	}
	return s.db.RunTxn(ctx, "copy-plan", fn)
}

func hasColumn(cols []catalog.Column, name string) bool {
	for _, c := range cols {
		if c.Name == name {
			return true
		}
	}
	return false
}

func indexOf(names []string, name string) int {
	for i, n := range names {
		if n == name {
			return i
		}
	}
	return -1
}

// copyQuoteIdent quotes an identifier for the query text COPY builds.
func copyQuoteIdent(s string) string {
	return `"` + strings.ReplaceAll(s, `"`, `""`) + `"`
}

// processCopyToOptions validates a COPY TO's options and fills in the
// defaults, with PostgreSQL's checks, order, messages and SQLSTATEs
// (ProcessCopyOptions). It returns FORCE_QUOTE's column names, resolved
// by the caller against the COPY's columns.
func processCopyToOptions(opts []parser.CopyOption) (*CopyOut, []string, error) {
	co := &CopyOut{Format: parser.CopyFormatText}
	var (
		seen                                  = map[string]bool{}
		delim, null, quote, escape, dflt      *string
		forceQuote                            []string
		forceNotNull, forceNull, forceQuoteOn bool
	)
	conflict := func(name string) error {
		if seen[name] {
			return newErrf(CodeSyntaxError, "conflicting or redundant options")
		}
		seen[name] = true
		return nil
	}
	str := func(o parser.CopyOption) (string, error) {
		switch {
		case o.HasArg:
			return o.Arg, nil
		case o.Star:
			return "*", nil
		case o.HasList:
			return strings.Join(o.List, ", "), nil
		}
		return "", newErrf(CodeSyntaxError, "%s requires a parameter", o.Name)
	}
	boolean := func(o parser.CopyOption) (bool, bool) {
		if !o.HasArg && !o.Star && !o.HasList {
			return true, true
		}
		if !o.HasArg {
			return false, false
		}
		switch strings.ToLower(o.Arg) {
		case "true", "on", "1":
			return true, true
		case "false", "off", "0":
			return false, true
		}
		return false, false
	}
	for _, o := range opts {
		switch o.Name {
		case "format":
			if err := conflict("format"); err != nil {
				return nil, nil, err
			}
			v, err := str(o)
			if err != nil {
				return nil, nil, err
			}
			switch v {
			case "text":
				co.Format = parser.CopyFormatText
			case "csv":
				co.Format = parser.CopyFormatCSV
			case "binary":
				co.Format = parser.CopyFormatBinary
			default:
				return nil, nil, newErrf(CodeInvalidParameterValue, "COPY format %q not recognized", v)
			}
		case "freeze":
			// Meaningful only to COPY FROM; PostgreSQL accepts and ignores
			// it here.
			if err := conflict("freeze"); err != nil {
				return nil, nil, err
			}
			if _, ok := boolean(o); !ok {
				return nil, nil, newErrf(CodeSyntaxError, "%s requires a Boolean value", o.Name)
			}
		case "delimiter", "null", "default", "quote", "escape", "encoding":
			if err := conflict(o.Name); err != nil {
				return nil, nil, err
			}
			v, err := str(o)
			if err != nil {
				return nil, nil, err
			}
			switch o.Name {
			case "delimiter":
				delim = &v
			case "null":
				null = &v
			case "default":
				dflt = &v
			case "quote":
				quote = &v
			case "escape":
				escape = &v
			case "encoding":
				if err := checkCopyEncoding(v); err != nil {
					return nil, nil, err
				}
			}
		case "header":
			if err := conflict("header"); err != nil {
				return nil, nil, err
			}
			if o.HasArg && strings.EqualFold(o.Arg, "match") {
				return nil, nil, newErrf(CodeFeatureNotSupported, "cannot use %q with HEADER in COPY TO", o.Arg)
			}
			b, ok := boolean(o)
			if !ok {
				return nil, nil, newErrf(CodeSyntaxError, "%s requires a Boolean value or \"match\"", o.Name)
			}
			co.Header = b
		case "force_quote":
			if err := conflict("force_quote"); err != nil {
				return nil, nil, err
			}
			switch {
			case o.Star:
				co.ForceQuoteAll = true
			case o.HasList:
				forceQuote = append([]string{}, o.List...)
			default:
				return nil, nil, newErrf(CodeInvalidParameterValue, "argument to option %q must be a list of column names", o.Name)
			}
			forceQuoteOn = true
		case "force_not_null", "force_null", "convert_selectively":
			if err := conflict(o.Name); err != nil {
				return nil, nil, err
			}
			if !o.HasList && !(o.Star && o.Name != "convert_selectively") {
				return nil, nil, newErrf(CodeInvalidParameterValue, "argument to option %q must be a list of column names", o.Name)
			}
			switch o.Name {
			case "force_not_null":
				forceNotNull = true
			case "force_null":
				forceNull = true
			}
		default:
			return nil, nil, newErrf(CodeSyntaxError, "option %q not recognized", o.Name)
		}
	}

	binary := co.Format == parser.CopyFormatBinary
	csv := co.Format == parser.CopyFormatCSV
	if binary && delim != nil {
		return nil, nil, newErrf(CodeSyntaxError, "cannot specify DELIMITER in BINARY mode")
	}
	if binary && null != nil {
		return nil, nil, newErrf(CodeSyntaxError, "cannot specify NULL in BINARY mode")
	}
	if binary && dflt != nil {
		return nil, nil, newErrf(CodeSyntaxError, "cannot specify DEFAULT in BINARY mode")
	}
	d, n := "\t", `\N`
	if csv {
		d, n = ",", ""
	}
	if delim != nil {
		d = *delim
	}
	if null != nil {
		n = *null
	}
	q, e := quote, escape
	if csv {
		if q == nil {
			dq := `"`
			q = &dq
		}
		if e == nil {
			e = q
		}
	}
	if len(d) != 1 {
		return nil, nil, newErrf(CodeFeatureNotSupported, "COPY delimiter must be a single one-byte character")
	}
	if strings.ContainsAny(d, "\r\n") {
		return nil, nil, newErrf(CodeInvalidParameterValue, "COPY delimiter cannot be newline or carriage return")
	}
	if strings.ContainsAny(n, "\r\n") {
		return nil, nil, newErrf(CodeInvalidParameterValue, "COPY null representation cannot use newline or carriage return")
	}
	if !csv && strings.Contains(`\.abcdefghijklmnopqrstuvwxyz0123456789`, d) {
		return nil, nil, newErrf(CodeInvalidParameterValue, "COPY delimiter cannot be \"%s\"", d)
	}
	if binary && co.Header {
		return nil, nil, newErrf(CodeFeatureNotSupported, "cannot specify HEADER in BINARY mode")
	}
	if !csv && quote != nil {
		return nil, nil, newErrf(CodeFeatureNotSupported, "COPY quote available only in CSV mode")
	}
	if csv && len(*q) != 1 {
		return nil, nil, newErrf(CodeFeatureNotSupported, "COPY quote must be a single one-byte character")
	}
	if csv && d[0] == (*q)[0] {
		return nil, nil, newErrf(CodeInvalidParameterValue, "COPY delimiter and quote must be different")
	}
	if !csv && escape != nil {
		return nil, nil, newErrf(CodeFeatureNotSupported, "COPY escape available only in CSV mode")
	}
	if csv && len(*e) != 1 {
		return nil, nil, newErrf(CodeFeatureNotSupported, "COPY escape must be a single one-byte character")
	}
	if !csv && forceQuoteOn {
		return nil, nil, newErrf(CodeFeatureNotSupported, "COPY force quote available only in CSV mode")
	}
	if !csv && forceNotNull {
		return nil, nil, newErrf(CodeFeatureNotSupported, "COPY force not null available only in CSV mode")
	}
	if forceNotNull {
		return nil, nil, newErrf(CodeFeatureNotSupported, "COPY force not null only available using COPY FROM")
	}
	if !csv && forceNull {
		return nil, nil, newErrf(CodeFeatureNotSupported, "COPY force null available only in CSV mode")
	}
	if forceNull {
		return nil, nil, newErrf(CodeFeatureNotSupported, "COPY force null only available using COPY FROM")
	}
	if strings.Contains(n, d) {
		return nil, nil, newErrf(CodeFeatureNotSupported, "COPY delimiter must not appear in the NULL specification")
	}
	if csv && strings.Contains(n, *q) {
		return nil, nil, newErrf(CodeFeatureNotSupported, "CSV quote character must not appear in the NULL specification")
	}
	if dflt != nil {
		return nil, nil, newErrf(CodeFeatureNotSupported, "COPY DEFAULT only available using COPY FROM")
	}
	co.Delim, co.Null = d[0], n
	if csv {
		co.Quote, co.Escape = (*q)[0], (*e)[0]
	}
	return co, forceQuote, nil
}

// checkCopyEncoding validates COPY's ENCODING option. Any name
// PostgreSQL knows passes its check; datax converts to none of them, so
// only the ones that need no conversion from UTF8 (UTF8 itself and
// SQL_ASCII, which PostgreSQL never converts to) are accepted.
func checkCopyEncoding(name string) error {
	var b strings.Builder
	for _, r := range strings.ToLower(name) {
		if r >= 'a' && r <= 'z' || r >= '0' && r <= '9' {
			b.WriteRune(r)
		}
	}
	clean := b.String()
	switch clean {
	case "utf8", "unicode", "sqlascii":
		return nil
	}
	if !pgEncodingNames[clean] {
		return newErrf(CodeInvalidParameterValue, "argument to option \"encoding\" must be a valid encoding name")
	}
	return newErrf(CodeFeatureNotSupported, "COPY ENCODING %q is not supported: datax writes UTF8 only", name)
}

// pgEncodingNames are PostgreSQL's encoding names and aliases, cleaned
// as pg_char_to_encoding cleans them (lower case, letters and digits).
var pgEncodingNames = func() map[string]bool {
	m := map[string]bool{}
	for _, n := range []string{
		"abc", "alt", "big5", "euccn", "eucjis2004", "eucjp", "euckr", "euctw", "gb18030", "gbk",
		"iso88591", "iso885910", "iso885913", "iso885914", "iso885915", "iso885916", "iso88592", "iso88593",
		"iso88594", "iso88595", "iso88596", "iso88597", "iso88598", "iso88599", "johab", "koi8", "koi8r", "koi8u",
		"latin1", "latin10", "latin2", "latin3", "latin4", "latin5", "latin6", "latin7", "latin8", "latin9",
		"mskanji", "muleinternal", "shiftjis", "shiftjis2004", "sjis", "tcvn", "tcvn5712", "uhc", "vscii",
		"win", "win1250", "win1251", "win1252", "win1253", "win1254", "win1255", "win1256", "win1257", "win1258",
		"win866", "win874", "win932", "win936", "win949", "win950", "windows1250", "windows1251", "windows1252",
		"windows1253", "windows1254", "windows1255", "windows1256", "windows1257", "windows1258", "windows866",
		"windows874", "windows932", "windows936", "windows949", "windows950",
	} {
		m[n] = true
	}
	return m
}()
