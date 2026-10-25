# Typed range field contract

Status: shared field semantics and captured range-read context. The active key format stays unchanged.

## Current state

A schema selects its hash field and range field from its existing fields.
Each selected field already has a declared type in `field_types`.
The key path converts scalar values to strings and compares range strings.
The shared type contract includes explicit `Date` and `DateTime` types.

## Date types

`Date` and `DateTime` belong to the shared `FieldValueType` enum.
Both types use JSON strings. Validation preserves each original value.

| Type | Accepted value |
| --- | --- |
| `Date` | An exact `YYYY-MM-DD` date in the proleptic Gregorian calendar, with a four-digit year from `0000` through `9999`. |
| `DateTime` | An RFC 3339 instant with seconds and an explicit `Z` or numeric offset. Both the written year and UTC year stay within `0000..9999`. |

`DateTime` permits one through nine fractional digits, or no fraction.
It accepts lowercase `t` and `z`, as RFC 3339 permits.
It rejects leap seconds, missing offsets, spaces, extra precision, and trailing text.
It does not round a value or convert its stored text to UTC.
The shared validator checks calendar dates and offset limits through Chrono.
Strict shape checks reject formats and precision that a general date parser can accept.
See the [Chrono date parser reference](https://docs.rs/chrono/0.4.44/chrono/struct.DateTime.html#method.parse_from_rfc3339).

Arrays, maps, objects, and `OneOf` use the existing recursive validation rules.
A nullable date uses `OneOf([Date, Null])`.
An explicit union with `String` still accepts any string through that branch.
Inference continues to return `String` for every JSON string.
No field changes type because its value looks like a date.

## Key admission

CAUTION: Current disk formats retain text order. Date keys remain disabled.

Schema admission refuses `Date` and `DateTime` in either key field.
The refusal also covers nested object or map keys and any temporal branch of a union.
An unrelated date field does not prevent a string field from serving as the key.
Wire schema loads, runtime field creation, schema-service proposals, and direct schema writes use the same check.
The schema service resolves canonical types and checks the result before a new schema or expansion reaches storage or memory.
It also resolves new reference fields before that check, to preserve their existing value validation.
This refusal prevents an explicit date type from silently using the current string order.

## Comparison context and cursors

Each schema read captures its declared key types, molecule bindings, and actual storage formats.
Object paths, map values, and union branches supply their declared types. Unknown declarations remain `Any`.
The read keeps this context through memory selection, disk reads, counts, and final response order.
A nested read refuses a different context. Each codec must match the captured order version.
Plain and OPE-v1 files both declare `LegacyUtf8V1`; their current key bytes and range order stay unchanged.
Raw molecule and maintenance keys retain an explicit storage comparator.

The shared comparison library also defines canonical values for future typed profiles:

- String: the complete UTF-8 value.
- Integer: an exact signed 64-bit value, with no float conversion.
- Date: the signed Gregorian day ordinal, where `0001-01-01` is day 1.
- DateTime: signed 128-bit nanoseconds from the Unix epoch, normalized to UTC.
- Boolean: false before true.

Typed comparison refuses unsupported domains and mixed scalar unions.
A nullable declaration permits its one non-null scalar domain; a required key cannot be null.
Typed text prefixes require String. Float, Number, and Any have no approved typed order yet.
These semantic functions do not activate a new disk format or change existing Integer ranges.

New query cursors add an opaque `comparison` identifier beside `hash` and `range`.
It binds the captured schema/type/format context, selector, projection, sort direction, and database scope.
A different identifier receives a refusal with a request to restart the page sequence.
The identifier is a compatibility hash. It is not an authentication token.
Older bare cursors remain accepted for the current UTF-8 format only.
The SDK preserves the full cursor and its string coordinates without date or numeric conversion.
Malformed cursor metadata receives a refusal. The range consumers retain the exact coordinate checks and verify a stable identifier when present.
Broad operations return the full cursor to the node. Paired latency operations use identical supported legacy requests on both program versions.
The paired input and final-value checks remain unchanged.

No context metadata is stored on each key. The cursor identifier adds bytes to a page response only.

## Compatibility and size

Existing enum names, JSON values, field identities, and key bytes stay unchanged.
Existing `String` fields retain their type, even when they contain dates.
No existing schema or Schema.org seed changes type automatically.
The change adds no second molecule, stored key copy, or ordered lookup.
The schema declaration alone carries the new type name.
Older readers cannot accept the new enum variants; they need an upgrade before a new date declaration reaches them.

## Next packages

1. Carry canonical values through the new profile's key writes, exact lookups, and bounds.
2. Connect the shared date and datetime comparisons to that profile's ordered disk representation.
3. Preserve nanosecond precision across the full year domain through conversion and recovery.
4. Select a compact, reviewed encryption profile for each supported key type.
5. Define one versioned key representation per molecule, with explicit conversion and recovery.

Date keys remain disabled until these paths agree.
The plan adds no separate `range_type` option that can contradict the selected field.
It does not require a cloud reader now.

## Validation and release

Local checks cover source format, source size, and the diff.
CI supplies builds, lint results, and the required measurements.
The retired tests and their replacements stay absent.
The PR uses the existing merge gates.
A primary upgrade remains a separate decision.
