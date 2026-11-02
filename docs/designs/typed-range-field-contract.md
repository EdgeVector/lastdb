# Typed range field contract

Status: every node home is a format 8 home: disk format v2 with blinded hashes and typed ranges.
The node marker is `format=8\nhome=disk-run\ncas=disk-cas\n`. The key format entry `layout:keys` holds `typed-ranges`.
An older home moves with one manual copy into a new home. The program has no migration code.

## New home creation

A new home uses blinded hashes and typed ordered ranges inside the existing encrypted V2 blocks.
Each range field needs a declared supported scalar type. An unknown type receives a refusal.
No setting selects another hash or range encoding.

The creator holds the node directory lock and creates both native homes.
It commits the key format entry and the account-key proof together before it publishes the format 8 node marker.
The existing block encryption protects both entries.
The creation intent stays until logical construction checks the proofs.
Only namespace registration can enter a home while that fence remains.

A crash before the node marker leaves creation state without application records.
The next creator can remove that state under the directory lock and retry.
A crash after the marker retains the complete authenticated proof batch.
Reopen verifies that batch before any writer cleanup, then completes the creation fence.
Missing, partial, mismatched, and wrong-key proofs receive a refusal.

The program opens only the format 8 marker with the `typed-ranges` key format.
Any other marker gets the generic failure "the node range format is unsupported".
Startup never rewrites a home.

## Key fields

A schema selects its hash field and range field from its existing fields.
Each selected field has a declared type in `field_types`.
The range order is the declared scalar order and its stored field key profile.
The shared type contract includes explicit `Date` and `DateTime` types.

## Encryption boundary

The user identity supplies the encryption root. Recovery retains that same identity.
The key derivation converts the Ed25519 seed to an X25519 secret, then uses HKDF-SHA256 for the account encryption key.
Separate HKDF labels derive the main and CAS home keys from the encryption key.
Each field key bundle holds its own blind key for hashes.
No lookup key uses OPE, and the key derivation has no OPE key.

| Part | Representation and protection |
| --- | --- |
| Memory | Selected keys and values are readable while the node uses them. |
| Main run blocks | Keys, field key metadata, records, alternate entries, blob counters, queue entries, and control values share the existing block encryption. |
| Lookup blocks | The sparse lookup, its first keys, and its filters use encrypted blocks too. |
| Field value | Plain logical JSON inside encrypted V2 blocks. No additional record envelope is required. |
| Portable files | Existing file ciphertext and access bundles retain their separate portable format. |
| Cloud backup | The adapter sends the same encrypted native files. It adds no per-record encryption layer. |
| Local identity file | The 32-byte seed in `identity.key` is plaintext with owner-only file permissions. |

The writer orders records, compresses each block when useful, then uses AES-256-GCM.
A run identity selects a derived frame key. A block counter supplies its nonce.
The authenticated header binds the frame. A CURRENT publication uses a fresh random frame identity.
An exact read decrypts the required lookup and data blocks. A range merges the matching ordered records from the required runs.
Neither operation requires all schema keys in memory.

The clear descriptors, file footers, and frame headers expose format, file identities, byte positions, and lengths.
The cloud also sees account/object identities, object counts, and request times.
The protected payload contains the logical range order and values.
The native backup excludes the local identity file and cloud credentials. Recovery supplies the private identity separately.

CAUTION: A person who obtains the identity seed can derive the data encryption keys.
This design protects database files without that seed. It does not encrypt the complete node home or replace operating-system disk protection.
The runtime, local key file, logs, and external app files remain separate security boundaries.

A future browser client can decrypt selected lookup and data blocks with the same private identity.
It also needs cloud access and authenticated file-location metadata.
The ordered file layout leaves that option open without server OPE.
The current program has no browser reader.

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

Every home permits Date and DateTime ranges. Temporal hash fields remain unsupported.
The checks cover nested object or map keys and temporal branches of a union.
An unrelated date field does not prevent a string field from serving as the key.
Wire schema loads, runtime field creation, schema-service proposals, and direct schema writes use the same check.
The schema service resolves canonical types and checks the result before a new schema or expansion reaches storage or memory.
It also resolves new reference fields before that check, to preserve their existing value validation.

## Comparison context and cursors

Each schema read captures its declared key types, field key bindings, and actual storage formats.
Object paths, map values, and union branches supply their declared types. Unknown declarations remain `Any`.
The read keeps this context through memory selection, disk reads, counts, and final response order.
A nested read refuses a different context.
Raw field key and maintenance keys retain an explicit storage comparator.

The shared comparison library defines canonical values for the typed profiles:

- String: the complete UTF-8 value.
- Integer: an exact signed 64-bit value, with no float conversion.
- Date: the signed Gregorian day ordinal, where `0001-01-01` is day 1.
- DateTime: signed 128-bit nanoseconds from the Unix epoch, normalized to UTC.
- Boolean: false before true.

Typed comparison refuses unsupported domains and mixed scalar unions.
A nullable declaration permits its one non-null scalar domain; a required key cannot be null.
Typed text prefixes require String. Float, Number, and Any have no approved typed order yet.

A query cursor holds an opaque `comparison` identifier beside `hash` and `range`.
It binds the captured schema/type/format context, selector, projection, sort direction, and database scope.
A different identifier receives a refusal with a request to restart the page sequence.
A bare cursor without the identifier receives HTTP 409 with the same request.
The identifier is a compatibility hash. It is not an authentication token.
The SDK preserves the full cursor and its string coordinates without date or numeric conversion.
Malformed cursor metadata receives a refusal. The range consumers retain the exact coordinate checks.

No context metadata is stored on each key. The cursor identifier adds bytes to a page response only.

## Compatibility and size

Existing enum names, JSON values, and field identities stay unchanged.
Existing `String` fields retain their type, even when they contain dates.
No existing schema or Schema.org seed changes type automatically.
The design adds no second field key, stored key copy, or ordered lookup.
The schema declaration alone carries the type name.
Older readers cannot accept the new enum variants; they need an upgrade before a new date declaration reaches them.

## Typed storage profiles

The shared `RangeStorageProfile` enum defines versioned, reversible range formats.
An explicit call selects a profile from the selected field's declared type.
The profile describes one field key. It adds no type tag to each stored key.
The field key contract selects the profile for schema admission, queries, and writes.

`FieldRangeProfile` defines the field key metadata contract.
`empty_v1` permits only the empty slot of a field key without a range field.
`typed_v1` names one scalar profile.
The declared range type must agree with the stored profile.
Schema reads fetch uncached field key metadata in one batch, then fetch the required key wraps in one batch.
The reader validates the complete set before it puts key bundles in the memory cache.
The runtime refuses a missing, incompatible, or incomplete contract before it serves records.

The field key metadata uses version 2.
Each field key has bundle authority: an independent field key bundle and its node wrap.
Schema publication creates every field key with bundle authority.
A share copies only that field key bundle. It never copies account keys into a shareable bundle.
A normal mutation refuses a missing profile. Only schema publication can create a new field key contract.
The existing three-key bundle wire form stays unchanged. Its third key has no user.

Every field key uses its stored coordinate for its conflict IDs. The reader refuses any other conflict-ID domain.
History responses use canonical API keys. Stored history and signed provenance retain their original bytes.

Range TTL retains its 20-digit timestamp prefix in a String range.
Typed Integer, Date, DateTime, and Boolean ranges do not inherit that TTL rule.
Their range-TTL policies receive a refusal until a retention-policy change defines the expiry clock.
Hash and Single schemas retain their existing write-time age records.

| Profile | Ordered payload | Bytes |
| --- | --- | --- |
| `string_utf8_v1` | The complete UTF-8 string, with byte order | Variable |
| `integer_i64_v1` | The signed 64-bit integer with a sign bias, in fixed-width lowercase hex | 16 |
| `date_days_v1` | The Gregorian day offset from `0000-01-01`, in fixed-width lowercase hex | 6 |
| `date_time_nanos_v1` | The UTC nanosecond offset from `0000-01-01T00:00:00Z`, in fixed-width lowercase hex | 18 |
| `boolean_v1` | `0` for false, `1` for true | 1 |

Equal integer values produce the same payload, even when their source text differs.
Equal datetime instants produce the same payload, even when their written offsets differ.
The datetime profile keeps all nine fractional digits through the full UTC year domain.
Decode returns canonical integer text, date text, or UTC datetime text with nine fractional digits.
Field value retains its original value.
String values retain every byte, including control bytes and Unicode.
Only the String profile permits text prefixes.
Unsupported types, mixed scalar unions, unknown versions, and malformed fixed-width payloads receive a refusal.

A format 8 home uses the plain range encoding: the stored range bytes are the profile payload.
The block encryption of the run files protects them. Some integer keys grow under the fixed-width format.
No second lookup key or original range copy enters storage.
All stored range decode paths use the exact field key codec.
Normal reads refuse a malformed range. V2 block encryption supplies authentication.

## Cloud backup

The paired V2 backup adapter binds the typed logical contract.
Backup profile version 6 names the format 8 node marker.
Any other profile version gets the generic failure.
The profile keeps the encrypted main and CAS files and the opaque cloud protocol.
The profile version binds the exact node marker and its byte count to the backup root.
Capture and restore check each schema against its existing field key profile and key authority at the held roots.
Those checks use bounded batch reads. They create no key and change no profile.

## Codec rules

`FieldKeyCodec` always blinds a hash and always uses the range profile of the field key.
The field key bundle metadata requires its range profile, key authority, and conflict identity.
The key comparison context and its normalize helpers take a required range profile.
A field read with no field key and no active comparison context uses its key without normalization.
