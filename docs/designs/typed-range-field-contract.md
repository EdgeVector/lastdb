# Typed range field contract

Status: first implementation package. The active key format stays unchanged.

## Current state

A schema selects its hash field and range field from its existing fields.
Each selected field already has a declared type in `field_types`.
The key path converts scalar values to strings and compares range strings.
The current shared type contract has no date type.

## Date types

This package adds `Date` and `DateTime` to the shared `FieldValueType` enum.
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

CAUTION: This package does not enable date keys or typed range comparisons.

Schema admission refuses `Date` and `DateTime` in either key field.
The refusal also covers nested object or map keys and any temporal branch of a union.
An unrelated date field does not prevent a string field from serving as the key.
Wire schema loads, runtime field creation, schema-service proposals, and direct schema writes use the same check.
The schema service resolves canonical types and checks the result before a new schema or expansion reaches storage or memory.
It also resolves new reference fields before that check, to preserve their existing value validation.
This refusal prevents an explicit date type from silently using the current string order.

## Compatibility and size

Existing enum names, JSON values, field identities, and key bytes stay unchanged.
Existing `String` fields retain their type, even when they contain dates.
No existing schema or Schema.org seed changes type automatically.
The change adds no second molecule, stored key copy, or ordered lookup.
The schema declaration alone carries the new type name.
Older readers cannot accept the new enum variants; they need an upgrade before a new date declaration reaches them.

## Next packages

1. Carry the declared field type through key creation, comparisons, bounds, and cursors.
2. Define date order by calendar day and datetime order by UTC instant.
3. Use a wide enough time value for nanosecond precision across the full year domain.
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
