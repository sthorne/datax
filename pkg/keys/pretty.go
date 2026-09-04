package keys

import (
	"encoding/hex"
	"fmt"
	"strings"
	"sync/atomic"
	"unicode"
	"unicode/utf8"

	"github.com/google/uuid"
	"github.com/sthorne/datax/pkg/util/encoding"
)

// Pretty renders a key the way a person reads the keyspace: /Min, /Max,
// /meta/<end key>, /system/<name>/<parts>, /table/<id>/<index>/<datums>,
// /local/... Datums under a table key have no schema here, so they are
// decoded by shape (a self-terminating string, an 8-byte integer, a
// decimal); the SQL layer's rowenc.PrettyKey names the table and index
// and types every datum when a descriptor is at hand. Unparseable bytes
// render as hex, never as an error: this is for logs and dashboards.
func Pretty(k Key) string {
	switch {
	case len(k) == 0:
		return "/"
	case k.Equal(MinKey):
		return "/Min"
	case k.Equal(MaxKey):
		return "/Max"
	}
	var b strings.Builder
	rest := []byte(k)
	switch rest[0] {
	case localPrefixByte:
		prettyLocal(&b, rest[1:])
	case metaPrefixByte:
		b.WriteString("/meta")
		b.WriteString(Pretty(Key(rest[1:])))
	case systemPrefixByte:
		b.WriteString("/system")
		prettySystem(&b, rest[1:])
	case tablePrefixByte:
		b.WriteString("/table")
		prettyTable(&b, rest[1:])
	default:
		b.WriteString("/")
		b.WriteString(hexOf(rest))
	}
	return b.String()
}

// Raw renders the exact bytes, Go-quoted, for the rare place that needs
// them verbatim (debug tooling); String renders Pretty.
func (k Key) Raw() string { return fmt.Sprintf("%q", []byte(k)) }

func hexOf(b []byte) string { return "0x" + hex.EncodeToString(b) }

func prettyLocal(b *strings.Builder, rest []byte) {
	b.WriteString("/local")
	if len(rest) == 0 {
		return
	}
	switch rest[0] {
	case localUnreplicatedByte:
		rest = rest[1:]
		if len(rest) > 0 && rest[0] == 'r' {
			tail, id, err := encoding.DecodeUint64(rest[1:])
			if err == nil {
				fmt.Fprintf(b, "/r%d", id)
				if len(tail) > 0 {
					// The suffix is a short tag (hs, log, as, desc, tomb),
					// optionally followed by a log index.
					tag := tail
					var idx []byte
					if i := strings.Index(string(tail), "log"); i == 0 && len(tail) > 3 {
						tag, idx = tail[:3], tail[3:]
					}
					b.WriteString("/" + string(tag))
					if len(idx) == 8 {
						_, n, _ := encoding.DecodeUint64(idx)
						fmt.Fprintf(b, "/%d", n)
					} else if len(idx) > 0 {
						b.WriteString("/" + hexOf(idx))
					}
				}
				return
			}
		}
		b.WriteString("/store/" + string(rest))
	case localAddressedByte:
		tail, addr, err := encoding.DecodeBytes(rest[1:])
		if err != nil {
			b.WriteString("/" + hexOf(rest))
			return
		}
		b.WriteString("/addressed")
		b.WriteString(Pretty(Key(addr)))
		if strings.HasPrefix(string(tail), "txn") && len(tail) == 3+16 {
			id, _ := uuid.FromBytes(tail[3:])
			b.WriteString("/txn/" + id.String())
		} else if len(tail) > 0 {
			b.WriteString("/" + hexOf(tail))
		}
	default:
		b.WriteString("/" + hexOf(rest))
	}
}

func prettySystem(b *strings.Builder, rest []byte) {
	rest, name, err := encoding.DecodeString(rest)
	if err != nil {
		b.WriteString("/" + hexOf(rest))
		return
	}
	b.WriteString("/" + name)
	// The parts each system table keys by, in order.
	var shape string
	switch name {
	case "nodes", "desc", "stats", "db":
		shape = "u"
	case "idgen", "users", "admins", "ns", "dbns":
		shape = "s"
	case "nsdb":
		shape = "us"
	case "lease":
		shape = "uU"
	}
	for _, p := range shape {
		if len(rest) == 0 {
			return
		}
		switch p {
		case 'u':
			tail, v, err := encoding.DecodeUint64(rest)
			if err != nil {
				b.WriteString("/" + hexOf(rest))
				return
			}
			fmt.Fprintf(b, "/%d", v)
			rest = tail
		case 's':
			tail, s, err := encoding.DecodeString(rest)
			if err != nil {
				b.WriteString("/" + hexOf(rest))
				return
			}
			fmt.Fprintf(b, "/%q", s)
			rest = tail
		case 'U':
			if len(rest) < 16 {
				b.WriteString("/" + hexOf(rest))
				return
			}
			id, _ := uuid.FromBytes(rest[:16])
			b.WriteString("/" + id.String())
			rest = rest[16:]
		}
	}
	if len(rest) > 0 {
		b.WriteString(PrettyDatums(rest))
	}
}

// tableNamer, when set (SetTableNamer), lends table names to Pretty so
// log lines read /table/orders/... instead of /table/3/...: the server
// installs its schema cache's lookup.
var tableNamer atomic.Pointer[func(uint64) (string, bool)]

// SetTableNamer installs the table-ID → name lookup Pretty consults. The
// lookup must be cheap and safe to call from any goroutine.
func SetTableNamer(f func(uint64) (string, bool)) {
	if f == nil {
		tableNamer.Store(nil)
		return
	}
	tableNamer.Store(&f)
}

// TableName resolves a table ID through the installed namer.
func TableName(id uint64) (string, bool) {
	if f := tableNamer.Load(); f != nil {
		return (*f)(id)
	}
	return "", false
}

func prettyTable(b *strings.Builder, rest []byte) {
	tail, id, err := encoding.DecodeUint64(rest)
	if err != nil {
		b.WriteString("/" + hexOf(rest))
		return
	}
	if name, ok := TableName(id); ok {
		b.WriteString("/" + name)
	} else {
		fmt.Fprintf(b, "/%d", id)
	}
	if len(tail) == 0 {
		return
	}
	rest, idx, err := encoding.DecodeUint64(tail)
	if err != nil {
		b.WriteString("/" + hexOf(tail))
		return
	}
	fmt.Fprintf(b, "/%d", idx)
	b.WriteString(PrettyDatums(rest))
}

// PrettyDatums renders a run of key-encoded datums without a schema,
// each as "/value": a decimal by its marker byte, a self-terminating
// string when it decodes to printable text, an 8-byte integer otherwise,
// and hex for what is left (a PrefixEnd-bumped bound, for instance).
func PrettyDatums(rest []byte) string {
	var b strings.Builder
	for len(rest) > 0 {
		if rest[0] >= 0x14 && rest[0] <= 0x16 {
			if tail, d, err := encoding.DecodeDecimal(rest); err == nil {
				b.WriteString("/" + d.String())
				rest = tail
				continue
			}
		}
		if tail, s, err := encoding.DecodeString(rest); err == nil && printable(s) {
			fmt.Fprintf(&b, "/%q", s)
			rest = tail
			continue
		}
		if len(rest) >= 8 {
			tail, v, _ := encoding.DecodeInt64(rest)
			fmt.Fprintf(&b, "/%d", v)
			rest = tail
			continue
		}
		b.WriteString("/" + hexOf(rest))
		break
	}
	return b.String()
}

func printable(s string) bool {
	if !utf8.ValidString(s) {
		return false
	}
	for _, r := range s {
		if !unicode.IsPrint(r) {
			return false
		}
	}
	return true
}
