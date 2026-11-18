# Functions reference

<!-- Generated from pkg/sql/builtins by `go generate ./pkg/sql/builtins`; do not edit by hand. -->

Every builtin function, by category, as `SHOW FUNCTIONS` lists them. Signatures show argument types (`any` accepts every type; `[x]` is optional; `...` is variadic) and the result type. An **immutable** function depends on its arguments alone and may appear in a `DEFAULT` or a `CHECK`; a **stable** one is fixed within a statement; a **volatile** one is evaluated afresh for every row. Strict functions (the default) return NULL when any argument is NULL; the entries that say otherwise handle NULLs themselves. Text arguments accept any value rendered as text; numeric arguments accept the three numeric types. The operators (`||`, `%`, `^`, the comparisons, `LIKE`, `SIMILAR TO`, `BETWEEN`, `IS DISTINCT FROM`, the jsonb `->`, `->>`, `#>`, `#>>`, `@>`, `<@`, `?`, `?|`, `?&`, the text-search `@@`, the vector distances `<->`, `<=>`, `<#>`) and casts are described in [the SQL reference](sql.md#reading).

## Conditionals

- `coalesce(any, ...) → the type of argument 1` — The first argument that is not NULL (NULL when all are). *immutable, handles NULL arguments*
- `greatest(any, ...) → the type of argument 1` — The largest argument; NULLs are ignored (NULL when all are). *immutable, handles NULL arguments*
- `least(any, ...) → the type of argument 1` — The smallest argument; NULLs are ignored (NULL when all are). *immutable, handles NULL arguments*
- `nullif(any, any) → the type of argument 1` — NULL when the two arguments are equal, else the first. *immutable, handles NULL arguments*

## Strings

- `ascii(text) → int8` — The code point of the first character (0 for the empty string). *immutable*
- `chr(int8) → text` — The character with the code point. *immutable*
- `concat(any, ...) → text` — Concatenates the arguments as text; NULLs are skipped. *immutable, handles NULL arguments*
- `concat_ws(text, any, ...) → text` — Concatenates the arguments after the first with it as the separator; NULLs are skipped. *immutable, handles NULL arguments*
- `decode(text, text) → bytes` — Decodes 'hex', 'base64' or 'escape' text into bytes. *immutable*
- `encode(any, text) → text` — Encodes bytes (or text) as 'hex', 'base64' or 'escape' text. *immutable*
- `format(text, [any], ...) → text` — Formats with %s (text), %I (an identifier, quoted when needed), %L (a literal, quoted; NULL for NULL) and %%. *immutable, handles NULL arguments*
- `initcap(text) → text` — The first letter of each word upper-cased, the rest lower-cased. *immutable*
- `left(text, int8) → text` — The first n characters (all but the last -n when negative). *immutable*
- `length(text) → int8` (also `char_length`, `character_length`) — Number of characters in the string (of bytes in a bytea). *immutable*
- `lower(text) → text` — The string in lower case. *immutable*
- `lpad(text, int8, [text]) → text` — Pads the string on the left to the length with the fill (spaces by default), truncating when longer. *immutable*
- `ltrim(text, [text]) → text` — Removes the given characters (spaces by default) from the start. *immutable*
- `md5(text) → text` — The MD5 hash as 32 hex characters (of a bytea's bytes). *immutable*
- `octet_length(text) → int8` — Number of bytes in the string or bytea. *immutable*
- `position(text, text) → int8` — 1-based position of the first argument's first occurrence in the second, 0 when absent (also position(needle IN haystack); over bytea, the byte offset); strpos(haystack, needle) takes them the other way round. *immutable*
- `quote_literal(any) → text` — The value as a quoted SQL literal. *immutable*
- `quote_nullable(any) → text` — The value as a quoted SQL literal, or NULL unquoted. *immutable, handles NULL arguments*
- `regexp_count(text, text, [int8], [text]) → int8` — The number of non-overlapping matches of the POSIX pattern, from the 1-based character start. *immutable*
- `regexp_like(text, text, [text]) → bool` — Whether the POSIX pattern matches anywhere in the string (the ~ operator, with flags). *immutable*
- `regexp_match(text, text, [text]) → text[]` — The first match of the POSIX pattern as a text array: its parenthesized subexpressions, or the whole match when there are none (NULL when nothing matches). *immutable*
- `regexp_matches(text, text, [text]) → setof text[]` — One row per match of the POSIX pattern (the first only, unless the g flag is given), each a text array as regexp_match returns. *immutable*
- `regexp_replace(text, text, text, [any], [int8], [text]) → text` — Replaces the first match of the POSIX pattern (every match with the g flag) with the replacement, in which \1 to \9 name the subexpressions and \& the whole match; regexp_replace(s, pattern, replacement, start [, n [, flags]]) begins at the character start and replaces the n-th match (every one for 0). *immutable*
- `regexp_split_to_array(text, text, [text]) → text[]` — The string split at the matches of the POSIX pattern, as a text array. *immutable*
- `regexp_split_to_table(text, text, [text]) → setof text` — The string split at the matches of the POSIX pattern, one row per piece. *immutable*
- `regexp_substr(text, text, [int8], [int8], [text], [int8]) → text` — The text of the n-th match (the first by default) of the POSIX pattern from the character start, or of its subexpr-th subexpression; NULL when there is none. *immutable*
- `repeat(text, int8) → text` — The string repeated n times. *immutable*
- `replace(text, text, text) → text` — Replaces every occurrence of the second argument with the third. *immutable*
- `reverse(text) → text` — The characters in reverse order. *immutable*
- `right(text, int8) → text` — The last n characters (all but the first -n when negative). *immutable*
- `rpad(text, int8, [text]) → text` — Pads the string on the right to the length with the fill (spaces by default), truncating when longer. *immutable*
- `rtrim(text, [text]) → text` — Removes the given characters (spaces by default) from the end. *immutable*
- `sha256(any) → bytes` — The SHA-256 hash, as bytes (encode(sha256(x), 'hex') for the hex text). *immutable*
- `split_part(text, text, int8) → text` — The n-th field (1-based; negative counts from the end) after splitting on the delimiter. *immutable*
- `starts_with(text, text) → bool` — Whether the string starts with the prefix. *immutable*
- `substring(text, int8, [int8]) → text` (also `substr`) — The substring starting at a 1-based position, optionally of a given length (also substring(s FROM n [FOR m])); over a bytea, the bytes. *immutable*
- `to_hex(int8) → text` — The integer in hexadecimal. *immutable*
- `translate(text, text, text) → text` — Replaces each character found in the second argument with the one at the same position in the third (dropped when the third is shorter). *immutable*
- `trim(text, [text]) → text` (also `btrim`) — Removes the given characters (spaces by default) from both ends (also trim([BOTH | LEADING | TRAILING] [chars] FROM s)). *immutable*
- `upper(text) → text` — The string in upper case. *immutable*

## Math

- `abs(any) → the type of argument 1` — The absolute value, in the argument's type. *immutable*
- `cbrt(any) → float8` — The cube root. *immutable*
- `ceil(any) → the type of argument 1` (also `ceiling`) — The smallest integer not less than the argument, in its type. *immutable*
- `div(any, any) → int8` — The integer quotient, truncated toward zero. *immutable*
- `exp(any) → float8` — e raised to the argument. *immutable*
- `floor(any) → the type of argument 1` — The largest integer not greater than the argument, in its type. *immutable*
- `gcd(int8, int8) → int8` — The greatest common divisor. *immutable*
- `ln(any) → float8` — The natural logarithm (an error at or below zero). *immutable*
- `log(any, [any]) → float8` (also `log10`) — log(x) is the base-10 logarithm; log(b, x) the logarithm of x in base b. *immutable*
- `mod(any, any) → the type of argument 1` — The remainder of the division (the sign of the dividend), as the % operator. *immutable*
- `pi() → float8` — π. *immutable*
- `power(any, any) → float8` (also `pow`) — The first argument raised to the second (also the ^ operator). *immutable*
- `random() → float8` — A random value in [0, 1), fresh per row. *volatile*
- `round(any, [int8]) → the type of argument 1` — Rounds to the nearest integer, or to the given number of decimal places (halves away from zero); a decimal stays exact. *immutable*
- `sign(any) → int8` — -1, 0 or 1 by the argument's sign. *immutable*
- `sqrt(any) → float8` — The square root (an error below zero). *immutable*
- `trunc(any, [int8]) → the type of argument 1` — Truncates toward zero, to an integer or to the given number of decimal places. *immutable*
- `width_bucket(any, any, any, int8) → int8` — The bucket (1..count) the operand falls in when [low, high) is split into count equal buckets; 0 below, count+1 above. *immutable*

## Date and time

- `age(any, [any]) → interval` — The interval from the second timestamp (today's midnight when omitted) to the first, in years, months, days and time. *stable*
- `clock_timestamp() → timestamptz` — The wall clock at the moment of the call (now() is the statement's start, the same for every row). *volatile*
- `date_trunc(text, any) → timestamptz` — The timestamp truncated to the field: millennium, century, decade, year, quarter, month, week, day, hour, minute, second, milliseconds. *immutable*
- `extract(text, any) → decimal` (also `date_part`) — A field of a date or timestamp (also extract(field FROM x)): year, quarter, month, week, day, doy, dow, isodow, hour, minute, second, milliseconds, microseconds, epoch, century, decade, millennium. *immutable*
- `isfinite(any) → bool` — Whether a date or timestamp is finite (always true: datax has no infinities). *immutable*
- `justify_days(any) → interval` — Rewrites an interval's days beyond 30 as months. *immutable*
- `justify_hours(any) → interval` — Rewrites an interval's hours beyond 24 as days. *immutable*
- `justify_interval(any) → interval` — Rewrites an interval with justify_days and justify_hours, then aligns the signs of its parts. *immutable*
- `make_date(int8, int8, int8) → date` — A date from year, month and day. *immutable*
- `make_interval([int8], [int8], [int8], [int8], [int8], [int8], [any]) → interval` — An interval from years, months, weeks, days, hours, minutes and seconds. *immutable*
- `make_time(int8, int8, any) → time` — A time from hour, minute and seconds. *immutable*
- `make_timestamp(int8, int8, int8, int8, int8, any) → timestamptz` (also `make_timestamptz`) — A timestamp from year, month, day, hour, minute and seconds. *immutable*
- `timezone(any, any) → timestamptz` — ts AT TIME ZONE zone: a timestamp with time zone becomes the wall-clock timestamp in the zone, a timestamp without time zone the instant it names there. The zone is an IANA name, an abbreviation, a POSIX offset ('UTC+5' is five hours west) or an interval. *immutable*
- `to_char(any, text) → text` — Formats a timestamp or date (YYYY, YY, MM, Mon, Month, DD, Dy, Day, HH24, HH12, MI, SS, MS, US, AM, TZ, ...) or a number (9, 0, ., ,, FM, S) with a pattern. *immutable*
- `to_date(text, text) → date` — Parses text with a to_char date pattern into a date. *immutable*
- `to_timestamp(any, [text]) → timestamptz` — to_timestamp(seconds) converts a Unix epoch; to_timestamp(text, format) parses with the to_char patterns (YYYY, MM, DD, HH24, MI, SS, ...). *immutable*

## JSON

- `jsonb_array_elements(jsonb) → setof jsonb` (also `json_array_elements`) — One row per element of the array, as jsonb (22023 for a value that is not an array). *immutable*
- `jsonb_array_elements_text(jsonb) → setof text` (also `json_array_elements_text`) — One row per element of the array, as text (strings unquoted, JSON null as NULL). *immutable*
- `jsonb_array_length(jsonb) → int8` (also `json_array_length`) — The number of elements of a JSON array (an error for anything else). *immutable*
- `jsonb_build_array([any], ...) → jsonb` (also `json_build_array`) — An array of the arguments. *immutable, handles NULL arguments*
- `jsonb_build_object([any], ...) → jsonb` (also `json_build_object`) — An object from alternating keys and values. *immutable, handles NULL arguments*
- `jsonb_each(jsonb) → setof (key text, value jsonb)` (also `json_each`) — One row per field of the object: its key and its value as jsonb (22023 for a value that is not an object). *immutable*
- `jsonb_each_text(jsonb) → setof (key text, value text)` (also `json_each_text`) — One row per field of the object: its key and its value as text (strings unquoted, JSON null as NULL). *immutable*
- `jsonb_extract_path(jsonb, [text], ...) → jsonb` (also `json_extract_path`) — The value at the path of keys (array indexes as text), as jsonb; NULL when absent (also the #> operator with a '{a,b}' path). *immutable*
- `jsonb_extract_path_text(jsonb, [text], ...) → text` (also `json_extract_path_text`) — The value at the path of keys as text; NULL when absent (also the #>> operator). *immutable*
- `jsonb_insert(jsonb, text, jsonb, [bool]) → jsonb` — The document with the value inserted at the '{a,0}' path: before the array element there (after it when the fourth argument is true), or as a new object key (an existing key is an error, 22023). *immutable*
- `jsonb_object_keys(jsonb) → setof text` (also `json_object_keys`) — One row per key of the object (22023 for a value that is not an object). *immutable*
- `jsonb_pretty(jsonb) → text` — The document indented for reading. *immutable*
- `jsonb_set(jsonb, text, jsonb, [bool]) → jsonb` — The document with the value at the '{a,b}' path replaced (created when missing, unless the fourth argument is false). *immutable*
- `jsonb_strip_nulls(jsonb) → jsonb` — The document with every object field whose value is null removed. *immutable*
- `jsonb_typeof(jsonb) → text` (also `json_typeof`) — object, array, string, number, boolean or null. *immutable*
- `to_jsonb(any) → jsonb` (also `to_json`) — The value as jsonb: text becomes a JSON string, numbers and booleans themselves, jsonb stays. *immutable*

## Set-returning functions

- `generate_series(any, any, [any]) → setof the type of its bounds` — The values from start to stop (inclusive) by step (1 by default): integers, numerics, or timestamps stepped by an interval (generate_series(start, stop, interval '1 day')). A zero step is an error (22023). *immutable*

## Full-text search

- `get_current_ts_config() → text` — The session's default text-search configuration (default_text_search_config). *stable*
- `numnode(tsquery) → int8` — numnode(tsquery): the number of lexemes and operators in the query; 0 for a query that reduced to nothing (only stop words). *immutable*
- `phraseto_tsquery(text, [text]) → tsquery` — phraseto_tsquery([config,] text): a tsquery requiring the words of the text in that order (joined by <->, wider where stop words were dropped). *stable*
- `plainto_tsquery(text, [text]) → tsquery` — plainto_tsquery([config,] text): a tsquery requiring every word of the text (the words normalized and joined by &); punctuation and operators in the text mean nothing. *stable*
- `setweight(tsvector, text) → tsvector` — setweight(tsvector, weight): the vector with every position labelled A, B, C or D, for ranking the parts of a document differently (a title A, a body D). *immutable*
- `strip(tsvector) → tsvector` — strip(tsvector): the vector without positions and weights — smaller, still matched by & | !, never by a phrase. *immutable*
- `to_tsquery(text, [text]) → tsquery` — to_tsquery([config,] text): a tsquery from its text form — words joined by & (and), | (or), ! (not) and <-> or <N> (followed by), with parentheses, word:* for a prefix and word:AB for weights — each word normalized by the configuration; stop words are dropped. *stable*
- `to_tsvector(text, [text]) → tsvector` — to_tsvector([config,] text): the text reduced to a tsvector — its lexemes under the configuration (simple or english; default_text_search_config when omitted), each with the positions it occurs at. *stable*
- `ts_headline(any, any, [any], [any]) → text` — ts_headline([config,] document, tsquery [, options]): a fragment of the document with the query's words marked. options is 'Name=value, ...' with StartSel and StopSel (<b> and </b>), MaxWords (35), MinWords (15) and HighlightAll (false); ShortWord, MaxFragments and FragmentDelimiter are accepted and ignored. With three arguments the first is the configuration unless the second is a tsquery value. *stable*
- `ts_rank(any, any, [any], [any]) → float8` — ts_rank([weights,] tsvector, tsquery [, normalization]): a relevance score from how often the query's lexemes occur in the vector and, for a query that requires them together, how near each other. weights is an array of four numbers scoring D, C, B and A positions ({0.1, 0.2, 0.4, 1.0} by default); normalization is a bit mask (1 divides by 1 + log of the document length, 2 by the length, 8 by the number of unique words, 16 by 1 + its log, 32 by the rank + 1). *immutable*
- `ts_rank_cd(any, any, [any], [any]) → float8` — ts_rank_cd([weights,] tsvector, tsquery [, normalization]): cover density ranking — a score from the shortest stretches of the document that satisfy the query, higher the fewer other words they hold. Arguments as ts_rank; normalization 4 also divides by the mean harmonic distance between the stretches. Needs positions: a stripped vector scores as if every word stood at one place. *immutable*
- `tsvector_to_array(tsvector) → text[]` — tsvector_to_array(tsvector): the vector's lexemes, sorted, as a text array. *immutable*
- `websearch_to_tsquery(text, [text]) → tsquery` — websearch_to_tsquery([config,] text): a tsquery from search-box syntax — words are required, "quoted text" is a phrase, or separates alternatives and -word excludes; never raises a syntax error. *stable*

## Vectors

- `cosine_distance(vector, vector) → float8` — One minus the cosine of the angle between two vectors (the `<=>` operator); NaN when either is the zero vector. *immutable*
- `inner_product(vector, vector) → float8` — Inner (dot) product of two vectors; the `<#>` operator is its negation, so that smaller is nearer. *immutable*
- `l1_distance(vector, vector) → float8` — Taxicab distance between two vectors: the sum of the absolute element differences. *immutable*
- `l2_distance(vector, vector) → float8` — Euclidean distance between two vectors of the same dimension (the `<->` operator). *immutable*
- `l2_normalize(vector) → vector` — The vector scaled to unit length (the zero vector stays zero). *immutable*
- `vector_dims(vector) → int8` — The number of elements of a vector. *immutable*
- `vector_norm(vector) → float8` — The Euclidean length of a vector. *immutable*

## Session and system

- `current_database() → text` — The session's database (also the keyword current_catalog, without parentheses). *stable*
- `current_date() → date` — Today's date, from the statement's start time. *stable*
- `current_schema() → text` — public: the only schema. *stable*
- `current_setting(text, [bool]) → text` — The value of a session setting, built in or custom (app.tenant). An unknown or never-set name is an error (42704) unless missing_ok is true, which returns NULL. Its arguments may name columns: the name is looked up per row. *stable*
- `current_time() → time` (also `localtime`) — The time of day of the statement's start time, in the session's time zone. current_time, current_timestamp, localtime and localtimestamp are written without parentheses, as in PostgreSQL, and take an optional fractional-seconds precision: current_timestamp(3). *stable*
- `current_user() → text` — The current role: the session user, or the role SET ROLE selected. Also written current_role and user; like session_user, all three are keywords written without parentheses. *stable*
- `currval(text) → int8` — The value nextval last returned for the sequence in this session (55000 before any). *stable*
- `gen_random_uuid() → uuid` (also `uuid_generate_v4`) — A random (version 4) UUID. *volatile*
- `lastval() → int8` — The value nextval last returned in this session, whatever the sequence. *stable*
- `nextval(text) → int8` — Advances the sequence and returns its next value; never rolled back. *volatile*
- `now() → timestamptz` (also `current_timestamp`, `localtimestamp`, `statement_timestamp`, `transaction_timestamp`) — The statement's start time, the same for every row (also current_timestamp, localtimestamp, statement_timestamp(), transaction_timestamp()). *stable*
- `pg_notify(text, text) → text` — Sends a notification to the sessions listening on the channel (NOTIFY with a computed channel or payload); delivered when the transaction commits. Evaluated once per statement: its arguments cannot name columns. *volatile*
- `session_user() → text` — The authenticated session user (unchanged by SET ROLE). *stable*
- `set_config(text, text, bool) → text` — Sets a session setting, as SET does (SET LOCAL when is_local is true), and returns the value: SET with a value that can be a parameter. Evaluated once per statement: its arguments cannot name columns. *volatile*
- `setval(text, int8, [bool]) → int8` — Sets the sequence's counter; with is_called false the value itself is the next one handed out. *volatile*
- `unique_rowid() → int8` — A node-local monotonic 64-bit id (48 bits of microsecond time above the node ID): unique across nodes with no coordination, spread across ranges unlike a sequence. *volatile*
- `version() → text` — The server version string (PostgreSQL 14.0 datax <release>). *stable*

## System information and privileges

- `current_schemas(bool) → text[]` — The schemas of the search path: {pg_catalog,public} when the argument (include implicit schemas) is true, {public} when false. *stable*
- `has_any_column_privilege(any, any, [text]) → bool` — has_any_column_privilege([user,] table, privilege): SELECT, INSERT, UPDATE or REFERENCES on any column of the table. datax grants on tables, so this is has_table_privilege's answer. *stable*
- `has_column_privilege(any, any, any, [text]) → bool` — has_column_privilege([user,] table, column, privilege): SELECT, INSERT, UPDATE or REFERENCES on the column (a name, or an attribute number). datax grants on tables, so this is the table's answer. 42703 for an unknown column name, NULL for an unknown number. *stable*
- `has_database_privilege(any, any, [text]) → bool` — has_database_privilege([user,] database, privilege): CREATE, CONNECT, or TEMPORARY (TEMP), which follows CONNECT. 3D000 for an unknown database. *stable*
- `has_foreign_data_wrapper_privilege(any, any, [text]) → bool` — There are no foreign-data wrappers: 42704 for a name, NULL for an OID. *stable*
- `has_function_privilege(any, any, [text]) → bool` — has_function_privilege([user,] function, privilege): EXECUTE, which every role holds on every function. The function is a pg_proc OID or name(argument types); 42883 when none matches. *stable*
- `has_schema_privilege(any, any, [text]) → bool` — has_schema_privilege([user,] schema, privilege): USAGE or CREATE. On public, USAGE is everyone's unless revoked from PUBLIC and CREATE is what creating a table in the database takes; pg_catalog and information_schema are everyone's to use and admins' to create in. 3F000 for an unknown schema. *stable*
- `has_sequence_privilege(any, any, [text]) → bool` — has_sequence_privilege([user,] sequence, privilege): USAGE, SELECT or UPDATE on the sequence, as nextval, currval and setval check them. 42809 when the relation is not a sequence. *stable*
- `has_server_privilege(any, any, [text]) → bool` — There are no foreign servers: 42704 for a name, NULL for an OID. *stable*
- `has_table_privilege(any, any, [text]) → bool` — has_table_privilege([user,] table, privilege): whether the user (the current role when omitted; a name, an OID or public) holds the privilege on the table, view or sequence (a name or an OID) — SELECT, INSERT, UPDATE, DELETE and TRUNCATE as GRANT, ownership, admin, read_all and write_all confer them; REFERENCES, TRIGGER and MAINTAIN are the owner's and admins'. A comma-separated list is true when any holds; WITH GRANT OPTION asks for the grant option. NULL for an OID that names nothing, 42P01 for an unknown name, 22023 for an unknown privilege. The schema's USAGE is not consulted, as in PostgreSQL. *stable*
- `has_tablespace_privilege(any, any, [text]) → bool` — has_tablespace_privilege([user,] tablespace, privilege): CREATE on pg_default or pg_global, which only admins hold. 42704 for any other tablespace. *stable*
- `has_type_privilege(any, any, [text]) → bool` — has_type_privilege([user,] type, privilege): USAGE, which every role holds on every type. 42704 for an unknown type. *stable*
- `pg_get_serial_sequence(text, text) → text` — The sequence a SERIAL, identity or OWNED BY column draws from, as public.name (quoted where needed), or NULL when it has none. The table name is read as SQL reads one ('"Mixed"', 'public.t'); the column name is taken as written. 42P01 for no such table, 42703 for no such column. *stable*
- `pg_has_role(any, any, [text]) → bool` — pg_has_role([user,] role, privilege): whether the user (the current role when omitted; a name or an OID) is a MEMBER of the role (directly or not), has its privileges (USAGE: membership through INHERIT), or may SET ROLE to it (SET); WITH ADMIN OPTION asks whether it may grant the role. A comma-separated list is true when any holds. Root and admins hold every role. 42704 for an unknown role name, 22023 for an unknown privilege. *stable*
- `pg_is_in_recovery() → bool` — false: every node serves reads and writes; there is no standby mode. *stable*
- `pg_options_to_table(text) → setof (option_name text, option_value text)` — One row per name=value element of an options array (reloptions, a foreign server's options): the name and the value (NULL when the element has no =). *immutable*
- `to_regclass(text) → int8` — The OID of the table, view, sequence or catalog so named (read as SQL reads a name), or NULL when there is none — 'name'::regclass without the error. *stable*
- `to_regnamespace(text) → int8` — The OID of the schema (public, pg_catalog or information_schema), or NULL. *stable*
- `to_regproc(text) → int8` — The pg_proc OID of the builtin function so named, or NULL. *stable*
- `to_regprocedure(text) → int8` — The pg_proc OID of the builtin function named with its argument list ('lower(text)'), or NULL when there is none or it takes another number of arguments. *stable*
- `to_regrole(text) → int8` — The OID of the role, or NULL when there is none. *stable*

## Large objects

- `lo_close(int8) → int8` — Closes a large-object descriptor (the end of the transaction closes those left open). *volatile*
- `lo_creat(int8) → int8` — Creates an empty large object with a free id (the argument is ignored, as in PostgreSQL) and returns the id. *volatile*
- `lo_create(int8) → int8` — Creates an empty large object with the given id (0 picks a free one) and returns the id. Created inside a transaction block, the object can then be written as a stream of any length. *volatile*
- `lo_from_bytea(int8, bytes) → int8` — Creates a large object holding the given bytes (id 0 picks a free one) and returns its id. *volatile*
- `lo_get(int8, [int8], [int8]) → bytes` — The object's bytes, or the given number of them from an offset, as one value (bounded by statement_memory_limit; read large objects with loread). *volatile*
- `lo_lseek64(int8, int8, int8) → int8` (also `lo_lseek`) — Moves the descriptor's position to an offset from the start (whence 0), the current position (1) or the end (2) and returns it. *volatile*
- `lo_open(int8, int8) → int8` — Opens a large object and returns a descriptor valid until the transaction ends; mode 131072 (INV_WRITE) allows writing, 262144 (INV_READ) is read-only. *volatile*
- `lo_put(int8, int8, bytes) → text` — Writes the bytes into the object at an offset. *volatile*
- `lo_tell64(int8) → int8` (also `lo_tell`) — The descriptor's position. *volatile*
- `lo_truncate64(int8, int8) → int8` (also `lo_truncate`) — Cuts the large object to the given length, or extends it with zeros. *volatile*
- `lo_unlink(int8) → int8` — Removes a large object; its storage is reclaimed in the background. *volatile*
- `loread(int8, int8) → bytes` — Reads up to the given number of bytes at the descriptor's position and advances it; fewer bytes mean the end of the object. *volatile*
- `lowrite(int8, bytes) → int8` — Writes the bytes at the descriptor's position, advances it, and returns the number written. *volatile*

## Advisory locks

- `pg_advisory_lock(int8, [int8]) → text` — Takes an exclusive session-level advisory lock on a bigint key, or two int4 keys (a different lock from any bigint one), waiting until it is free (or lock_timeout, 55P03, or statement_timeout or a cancel, 57014). Locks are cluster-wide and reentrant: each call needs its own pg_advisory_unlock. Released by pg_advisory_unlock, pg_advisory_unlock_all or the session's end, not by the transaction's. Returns nothing (an empty value). *volatile*
- `pg_advisory_lock_shared(int8, [int8]) → text` — pg_advisory_lock in shared mode: held by any number of sessions at once, it conflicts only with an exclusive hold. *volatile*
- `pg_advisory_unlock(int8, [int8]) → bool` — Gives back one session-level exclusive hold of the lock: true, or false with a WARNING (you don't own a lock of type ExclusiveLock) when the session held none. *volatile*
- `pg_advisory_unlock_all() → text` — Gives back every session-level advisory lock the session holds (transaction-level ones stay until the transaction ends). Returns nothing. *volatile*
- `pg_advisory_unlock_shared(int8, [int8]) → bool` — pg_advisory_unlock for a shared hold (the warning names ShareLock). *volatile*
- `pg_advisory_xact_lock(int8, [int8]) → text` — pg_advisory_lock held by the transaction: released when it commits or rolls back (or by a ROLLBACK TO a savepoint taken before it), and by nothing else. *volatile*
- `pg_advisory_xact_lock_shared(int8, [int8]) → text` — pg_advisory_xact_lock in shared mode. *volatile*
- `pg_try_advisory_lock(int8, [int8]) → bool` — pg_advisory_lock without the wait: true when the lock was taken, false at once when another session holds it. *volatile*
- `pg_try_advisory_lock_shared(int8, [int8]) → bool` — pg_try_advisory_lock in shared mode. *volatile*
- `pg_try_advisory_xact_lock(int8, [int8]) → bool` — pg_advisory_xact_lock without the wait: true when taken, false when another session holds the lock. *volatile*
- `pg_try_advisory_xact_lock_shared(int8, [int8]) → bool` — pg_try_advisory_xact_lock in shared mode. *volatile*

## Arrays

- `array_append(any, any) → the type of argument 1` — The array with the value appended. *immutable, handles NULL arguments*
- `array_cat(any, any) → the type of argument 1` — The two arrays concatenated. *immutable, handles NULL arguments*
- `array_length(any, int8) → int8` — The number of elements of an array (dimension 1; NULL for an empty array or another dimension). *immutable*
- `array_lower(any, int8) → int8` — The lower bound of an array's dimension (1; NULL when empty). *immutable*
- `array_ndims(any) → int8` — The number of dimensions of an array (1; NULL when empty). *immutable*
- `array_position(any, any) → int8` — The 1-based position of the first element equal to the value (NULL when absent). *immutable, handles NULL arguments*
- `array_prepend(any, any) → the type of argument 2` — The array with the value prepended. *immutable, handles NULL arguments*
- `array_remove(any, any) → the type of argument 1` — The array without the elements equal to the value. *immutable, handles NULL arguments*
- `array_to_string(any, text, [text]) → text` — The elements joined by the separator; NULL elements are skipped unless the third argument gives their text. *immutable, handles NULL arguments*
- `array_upper(any, int8) → int8` — The upper bound of an array's dimension (its length; NULL when empty). *immutable*
- `cardinality(any) → int8` — The number of elements of an array (0 for an empty one). *immutable*
- `string_to_array(text, text, [text]) → text[]` — The text split on the separator into a text array (an empty separator yields one element; a NULL separator splits into characters); the third argument names the text that becomes NULL. *immutable, handles NULL arguments*

