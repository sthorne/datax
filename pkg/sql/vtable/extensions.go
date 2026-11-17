package vtable

// Extension is one of the PostgreSQL extensions whose surface datax has
// built in. Nothing is installed or removed: CREATE EXTENSION and DROP
// EXTENSION accept these names and do nothing, and pg_extension and
// pg_available_extensions always list them as installed, so a tool that
// checks before creating one and a tool that just creates it agree.
type Extension struct {
	Name    string
	Version string
	Comment string
	// OID is the extension's pg_extension OID: fixed, below the range
	// PostgreSQL hands out to user objects.
	OID int64
	// Namespace is the schema the extension's objects live in
	// (pg_catalog for plpgsql, public for the others, as a default
	// PostgreSQL install puts them).
	Namespace   int64
	Relocatable bool
}

// Extensions are the extensions datax accepts, in name order.
//
//   - plpgsql is installed in every PostgreSQL database.
//   - pgcrypto: gen_random_uuid() only (in core since PostgreSQL 13, and
//     the reason most schemas create the extension). digest, hmac,
//     crypt, gen_salt, gen_random_bytes and the pgp_* functions do not
//     exist.
//   - uuid-ossp: uuid_generate_v4() only.
//   - vector: pgvector's type, operators and ivfflat index.
var Extensions = []Extension{
	{Name: "pgcrypto", Version: "1.3", Comment: "cryptographic functions", OID: 13502, Namespace: OIDPublic, Relocatable: true},
	{Name: "plpgsql", Version: "1.0", Comment: "PL/pgSQL procedural language", OID: 13501, Namespace: OIDPgCatalog},
	{Name: "uuid-ossp", Version: "1.1", Comment: "generate universally unique identifiers (UUIDs)", OID: 13503, Namespace: OIDPublic, Relocatable: true},
	{Name: "vector", Version: "0.8.0", Comment: "vector data type and ivfflat access method", OID: 13504, Namespace: OIDPublic, Relocatable: true},
}

// LookupExtension finds an accepted extension by name.
func LookupExtension(name string) (Extension, bool) {
	for _, e := range Extensions {
		if e.Name == name {
			return e, true
		}
	}
	return Extension{}, false
}

// ExtensionNames lists the accepted names, quoted, for messages.
func ExtensionNames() string {
	out := ""
	for i, e := range Extensions {
		if i > 0 {
			out += ", "
		}
		out += `"` + e.Name + `"`
	}
	return out
}
