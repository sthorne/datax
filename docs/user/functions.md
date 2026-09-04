# Functions reference

<!-- Generated from pkg/sql/builtins by `go generate ./pkg/sql/builtins`; do not edit by hand. -->

Every builtin function, by category, as `SHOW FUNCTIONS` lists them. Signatures show argument types (`any` accepts every type; `[x]` is optional; `...` is variadic) and the result type. An **immutable** function depends on its arguments alone and may appear in a `DEFAULT` or a `CHECK`; a **stable** one is fixed within a statement; a **volatile** one is evaluated afresh for every row. Strict functions (the default) return NULL when any argument is NULL; the entries that say otherwise handle NULLs themselves. Text arguments accept any value rendered as text; numeric arguments accept the three numeric types. The operators (`||`, `%`, `^`, the comparisons, `LIKE`, `SIMILAR TO`, `BETWEEN`, `IS DISTINCT FROM`, the jsonb `->`, `->>`, `#>`, `#>>`, `@>`, `<@`, `?`, `?|`, `?&`) and casts are described in [the SQL reference](sql.md#reading).

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
- `length(text) → int8` (also `char_length`, `character_length`) — Number of characters in the string. *immutable*
- `lower(text) → text` — The string in lower case. *immutable*
- `lpad(text, int8, [text]) → text` — Pads the string on the left to the length with the fill (spaces by default), truncating when longer. *immutable*
- `ltrim(text, [text]) → text` — Removes the given characters (spaces by default) from the start. *immutable*
- `md5(text) → text` — The MD5 hash as 32 hex characters. *immutable*
- `octet_length(text) → int8` — Number of bytes in the string. *immutable*
- `position(text, text) → int8` — 1-based position of the first argument's first occurrence in the second, 0 when absent (also position(needle IN haystack)); strpos(haystack, needle) takes them the other way round. *immutable*
- `quote_literal(any) → text` — The value as a quoted SQL literal. *immutable*
- `quote_nullable(any) → text` — The value as a quoted SQL literal, or NULL unquoted. *immutable, handles NULL arguments*
- `repeat(text, int8) → text` — The string repeated n times. *immutable*
- `replace(text, text, text) → text` — Replaces every occurrence of the second argument with the third. *immutable*
- `reverse(text) → text` — The characters in reverse order. *immutable*
- `right(text, int8) → text` — The last n characters (all but the first -n when negative). *immutable*
- `rpad(text, int8, [text]) → text` — Pads the string on the right to the length with the fill (spaces by default), truncating when longer. *immutable*
- `rtrim(text, [text]) → text` — Removes the given characters (spaces by default) from the end. *immutable*
- `sha256(any) → bytes` — The SHA-256 hash, as bytes (encode(sha256(x), 'hex') for the hex text). *immutable*
- `split_part(text, text, int8) → text` — The n-th field (1-based; negative counts from the end) after splitting on the delimiter. *immutable*
- `starts_with(text, text) → bool` — Whether the string starts with the prefix. *immutable*
- `substring(text, int8, [int8]) → text` (also `substr`) — The substring starting at a 1-based position, optionally of a given length (also substring(s FROM n [FOR m])). *immutable*
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

- `age(any, [any]) → text` — The interval from the second timestamp (today's midnight when omitted) to the first, in years, months, days and time. *stable*
- `clock_timestamp() → timestamptz` — The wall clock at the moment of the call (now() is the statement's start, the same for every row). *volatile*
- `date_trunc(text, any) → timestamptz` — The timestamp truncated to the field: millennium, century, decade, year, quarter, month, week, day, hour, minute, second, milliseconds. *immutable*
- `extract(text, any) → decimal` (also `date_part`) — A field of a date or timestamp (also extract(field FROM x)): year, quarter, month, week, day, doy, dow, isodow, hour, minute, second, milliseconds, microseconds, epoch, century, decade, millennium. *immutable*
- `isfinite(any) → bool` — Whether a date or timestamp is finite (always true: datax has no infinities). *immutable*
- `justify_hours(text) → text` — Rewrites an interval's hours beyond 24 as days. *immutable*
- `make_date(int8, int8, int8) → date` — A date from year, month and day. *immutable*
- `make_interval([int8], [int8], [int8], [int8], [int8], [int8], [any]) → text` — An interval from years, months, weeks, days, hours, minutes and seconds (as text: datax has no interval type yet). *immutable*
- `make_timestamp(int8, int8, int8, int8, int8, any) → timestamptz` (also `make_timestamptz`) — A timestamp from year, month, day, hour, minute and seconds. *immutable*
- `to_char(any, text) → text` — Formats a timestamp or date (YYYY, YY, MM, Mon, Month, DD, Dy, Day, HH24, HH12, MI, SS, MS, US, AM, TZ, ...) or a number (9, 0, ., ,, FM, S) with a pattern. *immutable*
- `to_date(text, text) → date` — Parses text with a to_char date pattern into a date. *immutable*
- `to_timestamp(any, [text]) → timestamptz` — to_timestamp(seconds) converts a Unix epoch; to_timestamp(text, format) parses with the to_char patterns (YYYY, MM, DD, HH24, MI, SS, ...). *immutable*

## JSON

- `jsonb_array_length(jsonb) → int8` (also `json_array_length`) — The number of elements of a JSON array (an error for anything else). *immutable*
- `jsonb_build_array([any], ...) → jsonb` (also `json_build_array`) — An array of the arguments. *immutable, handles NULL arguments*
- `jsonb_build_object([any], ...) → jsonb` (also `json_build_object`) — An object from alternating keys and values. *immutable, handles NULL arguments*
- `jsonb_extract_path(jsonb, [text], ...) → jsonb` (also `json_extract_path`) — The value at the path of keys (array indexes as text), as jsonb; NULL when absent (also the #> operator with a '{a,b}' path). *immutable*
- `jsonb_extract_path_text(jsonb, [text], ...) → text` (also `json_extract_path_text`) — The value at the path of keys as text; NULL when absent (also the #>> operator). *immutable*
- `jsonb_pretty(jsonb) → text` — The document indented for reading. *immutable*
- `jsonb_set(jsonb, text, jsonb, [bool]) → jsonb` — The document with the value at the '{a,b}' path replaced (created when missing, unless the fourth argument is false). *immutable*
- `jsonb_strip_nulls(jsonb) → jsonb` — The document with every object field whose value is null removed. *immutable*
- `jsonb_typeof(jsonb) → text` (also `json_typeof`) — object, array, string, number, boolean or null. *immutable*
- `to_jsonb(any) → jsonb` (also `to_json`) — The value as jsonb: text becomes a JSON string, numbers and booleans themselves, jsonb stays. *immutable*

## Session and system

- `current_database() → text` — The session's database. *stable*
- `current_date() → date` — Today's date, from the statement's start time. *stable*
- `current_schema() → text` — public: the only schema. *stable*
- `current_user() → text` (also `session_user`) — The session's user (also session_user). *stable*
- `currval(text) → int8` — The value nextval last returned for the sequence in this session (55000 before any). *stable*
- `gen_random_uuid() → uuid` (also `uuid_generate_v4`) — A random (version 4) UUID. *volatile*
- `lastval() → int8` — The value nextval last returned in this session, whatever the sequence. *stable*
- `nextval(text) → int8` — Advances the sequence and returns its next value; never rolled back. *volatile*
- `now() → timestamptz` (also `current_timestamp`, `localtimestamp`, `statement_timestamp`, `transaction_timestamp`) — The statement's start time, the same for every row (also current_timestamp, localtimestamp, statement_timestamp(), transaction_timestamp()). *stable*
- `setval(text, int8, [bool]) → int8` — Sets the sequence's counter; with is_called false the value itself is the next one handed out. *volatile*
- `unique_rowid() → int8` — A node-local monotonic 64-bit id (48 bits of microsecond time above the node ID): unique across nodes with no coordination, spread across ranges unlike a sequence. *volatile*
- `version() → text` — The server version string (PostgreSQL 14.0 datax <release>). *stable*

