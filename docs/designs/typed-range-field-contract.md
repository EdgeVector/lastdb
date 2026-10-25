# Typed range field contract

Status: shared field semantics, captured comparison context, and explicit typed key conversion APIs.
The active node key format stays unchanged. This package does not convert a home.

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

## Typed storage profiles

The shared `RangeStorageProfile` enum defines versioned, reversible range formats.
An explicit call selects a profile from the selected field's declared type.
The profile describes one molecule. It adds no type tag to each stored key.
No schema admission, startup, query, or writer selects these profiles yet.

| Profile | Ordered payload before the existing range encoding | Bytes |
| --- | --- | --- |
| `legacy_utf8_v1` | The existing complete text, with text order | Variable |
| `string_utf8_v1` | The complete UTF-8 string, with byte order | Variable |
| `integer_i64_v1` | The signed 64-bit integer with a sign bias, in fixed-width lowercase hex | 16 |
| `date_days_v1` | The Gregorian day offset from `0000-01-01`, in fixed-width lowercase hex | 6 |
| `date_time_nanos_v1` | The UTC nanosecond offset from `0000-01-01T00:00:00Z`, in fixed-width lowercase hex | 18 |
| `boolean_v1` | `0` for false, `1` for true | 1 |

Equal integer values produce the same payload, even when their source text differs.
Equal datetime instants produce the same payload, even when their written offsets differ.
The datetime profile keeps all nine fractional digits through the full UTC year domain.
Decode returns canonical integer text, date text, or UTC datetime text with nine fractional digits.
Atom content retains its original value. Conversion cannot restore a discarded key spelling without the source snapshot.
String values retain every byte, including control bytes and Unicode.
Only the String and legacy profiles permit text prefixes.
Unsupported types, mixed scalar unions, unknown versions, and malformed fixed-width payloads receive a refusal.

`ProfiledRangeCodec` composes these payloads with the existing plain or OPE-v1 range encoding.
It retains the existing molecule keys and encryption modes.
Plain ranges keep the payload bytes. OPE-v1 retains its existing eightfold byte expansion and existing security limits.
This package does not select or claim a stronger encryption scheme.
Some integer keys grow under the fixed-width format; the profile does not promise a size decrease for every type.
No second lookup key or original range copy enters storage.
The ordinary node codec still reports `LegacyUtf8V1` and uses its original bytes.

## Explicit key conversion

`RangeKeyConversion` converts a caller-supplied set of current `mk:` range keys for one molecule.
The caller selects both source and target profiles and supplies the existing encryption configuration.
The converter retains the exact hash bytes, including a blinded hash.
It changes only the range portion and returns the complete mapping in target byte order.
It never calls storage or writes an atom, tip, profile record, or format marker.

The converter refuses duplicate inputs and canonical collisions under the same hash.
The same range under different hashes remains distinct.
It also refuses malformed framing, another molecule, invalid values, and exceeded key-count or byte limits.
The byte limits account for the current range encoding's expansion before the target allocation.
The plan retains source and target key strings in bounded temporary memory.
Atom values do not enter the plan.
No partial mapping reaches a publication callback because the API has no such callback.

Decode uses the selected format. It never guesses encryption from a hex-shaped string.
OPE-v1 decode also checks a full re-encoding with the supplied molecule and key.
This check detects format or key mismatch. It does not authenticate the legacy encoding.
Codec diagnostic text excludes encryption keys.

CAUTION: A key conversion plan does not prove that the supplied set contains every key in a home.

The caller must supply real range values from one fixed logical snapshot.
Absent range slots for hash-only and Single fields need separate layout treatment.
The current API does not enumerate disk generations, delete markers, conflicts, or captured mutation keys.
It does not publish a typed profile, lift the date-key fence, or enable a typed node reader.

## Next packages

1. Capture the complete logical key set, layout slots, delete barriers, and other key references from one inactive home.
2. Define durable conversion progress and an unsupported-format fence before any key rewrite.
3. Publish a complete converted generation and its molecule profile together, with explicit resume and recovery rules.
4. Preserve counters, atom references, protein bindings, and file references through that publication.
5. Carry the selected profile through writes, exact reads, bounds, memory comparison, and cursors.

Date keys remain disabled until these paths agree. Conversion never runs at startup.
The current encryption stays in use. A replacement requires a separate review and decision.
The plan adds no separate `range_type` option that can contradict the selected field.
It does not require a cloud reader now.

## Validation and release

Local checks cover source format, source size, and the diff.
CI supplies builds, lint results, and the required measurements.
The retired tests and their replacements stay absent.
The PR uses the existing merge gates.
A primary upgrade remains a separate decision.
