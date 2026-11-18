package vtable

import (
	"sort"
	"strings"

	"github.com/sthorne/datax/pkg/sql/catalog"
	"github.com/sthorne/datax/pkg/sql/types"
)

// ACLs as PostgreSQL renders them (aclitem[] as text): the relacl,
// nspacl, datacl ... columns and acldefault(). pg_dump reads both and
// turns their difference into GRANT and REVOKE statements; psql's \dp
// prints them.

// aclLetters maps a privilege to its aclitem letter.
var aclLetters = map[string]byte{
	"INSERT": 'a', "SELECT": 'r', "UPDATE": 'w', "DELETE": 'd', "TRUNCATE": 'D', "REFERENCES": 'x', "TRIGGER": 't',
	"EXECUTE": 'X', "USAGE": 'U', "CREATE": 'C', "CONNECT": 'c', "TEMPORARY": 'T', "TEMP": 'T',
}

// aclOrder is the order PostgreSQL writes an item's letters in.
const aclOrder = "arwdDxtXUCTcsAm"

// DefaultACL is acldefault(kind, owner) for an owner's name: what an
// object of that kind grants before any GRANT or REVOKE. kind is
// PostgreSQL's: 'r' relation, 's' sequence, 'd' database, 'f'
// function, 'l' language, 'L' large object, 'n' schema, 't'
// tablespace, 'F' foreign-data wrapper, 'S' foreign server, 'T' type.
// The second result is false for a kind PostgreSQL does not know.
func DefaultACL(kind, owner string) (string, bool) {
	o := aclName(owner)
	var items []string
	switch kind {
	case "r":
		items = []string{o + "=arwdDxt/" + o}
	case "s":
		items = []string{o + "=rwU/" + o}
	case "d":
		items = []string{"=Tc/" + o, o + "=CTc/" + o}
	case "f":
		items = []string{"=X/" + o, o + "=X/" + o}
	case "l", "T":
		items = []string{"=U/" + o, o + "=U/" + o}
	case "L":
		items = []string{o + "=rw/" + o}
	case "n":
		items = []string{o + "=UC/" + o}
	case "t":
		items = []string{o + "=C/" + o}
	case "F", "S":
		items = []string{o + "=U/" + o}
	default:
		return "", false
	}
	return aclArray(items), true
}

// ObjectACL renders an object's ACL from its owner and its grants
// (grantee → privilege names, and the subset held WITH GRANT OPTION):
// NULL when nothing was granted, which PostgreSQL reads as the
// default; otherwise the owner's default item (ownerLetters) and one
// item per grantee, each granted by the owner (datax records no other
// grantor).
func ObjectACL(owner, ownerLetters string, privs, grantOpts map[string][]string) types.Datum {
	if len(privs) == 0 {
		return types.DNull
	}
	o := aclName(catalog.OwnerOf(owner))
	items := []string{o + "=" + ownerLetters + "/" + o}
	grantees := make([]string, 0, len(privs))
	for g := range privs {
		grantees = append(grantees, g)
	}
	sort.Strings(grantees)
	for _, g := range grantees {
		opts := map[string]bool{}
		for _, p := range grantOpts[g] {
			opts[strings.ToUpper(p)] = true
		}
		have := map[byte]bool{}
		withOpt := map[byte]bool{}
		for _, p := range privs[g] {
			p = strings.ToUpper(p)
			if l, ok := aclLetters[p]; ok {
				have[l] = true
				withOpt[l] = withOpt[l] || opts[p]
			}
		}
		var b strings.Builder
		for i := 0; i < len(aclOrder); i++ {
			if l := aclOrder[i]; have[l] {
				b.WriteByte(l)
				if withOpt[l] {
					b.WriteByte('*')
				}
			}
		}
		if b.Len() == 0 {
			continue
		}
		name := ""
		if g != catalog.PublicRole {
			name = aclName(g)
		}
		items = append(items, name+"="+b.String()+"/"+o)
	}
	return types.NewString(aclArray(items))
}

// aclName writes a role name as an aclitem does: double-quoted unless
// it is all letters, digits and underscores, with a double quote
// doubled.
func aclName(name string) string {
	plain := name != ""
	for i := 0; i < len(name); i++ {
		c := name[i]
		if !(c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c >= '0' && c <= '9' || c == '_' || c >= 0x80) {
			plain = false
			break
		}
	}
	if plain {
		return name
	}
	return `"` + strings.ReplaceAll(name, `"`, `""`) + `"`
}

// aclArray writes aclitems as the text of an aclitem[]: an item that
// holds a quote, comma, space, brace or backslash is quoted, with its
// quotes and backslashes escaped.
func aclArray(items []string) string {
	out := make([]string, len(items))
	for i, it := range items {
		if strings.ContainsAny(it, "\"{},\\ ") {
			it = `"` + strings.NewReplacer(`\`, `\\`, `"`, `\"`).Replace(it) + `"`
		}
		out[i] = it
	}
	return "{" + strings.Join(out, ",") + "}"
}
