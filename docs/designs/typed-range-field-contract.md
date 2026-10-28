# Typed range field contract

Status: every node home is a format 4 home: disk format v2 with blinded hashes and typed ranges.
On 2026-10-28 the program stopped support for format 3 homes.
The typed-range converter, its stopped-home inventory, the legacy UTF-8 range profile, OPE-v1, node key authority, and the old conflict-ID domain are gone.
An older home moves with one manual copy into a new home. The program has no migration code.
Git history keeps the transition design (main `bb2452651`).

## New home creation

A new home uses blinded hashes and typed ordered ranges inside the existing encrypted V2 blocks.
Each range field needs a declared supported scalar type. An unknown type receives a refusal.
No setting selects another hash or range encoding.

The creator holds the node directory lock and creates both native homes.
It commits logical format 7 and the account-key proof together before it publishes node format 4.
The existing block encryption protects both records.
The creation intent stays until logical construction checks the proofs.
Only namespace registration can enter a home while that fence remains.

A crash before the node marker leaves creation state without application records.
The next creator can remove that state under the directory lock and retry.
A crash after the marker retains the complete authenticated proof batch.
Reopen verifies that batch before any writer cleanup, then completes the creation fence.
Missing, partial, mismatched, and wrong-key proofs receive a refusal.

The program opens only the format 4 marker and logical key format 7.
A format 3 home, logical key format 6, and an unknown marker receive a refusal.
Startup never rewrites a home.

## Key fields

A schema selects its hash field and range field from its existing fields.
Each selected field has a declared type in `field_types`.
The range order is the declared scalar order and its stored molecule profile.
The shared type contract includes explicit `Date` and `DateTime` types.

## Encryption boundary

The user identity supplies the encryption root. Recovery retains that same identity.
The existing key derivation converts the Ed25519 seed to an X25519 secret, then uses HKDF-SHA256 for separate encryption and hash keys.
Separate HKDF labels derive the main and CAS home keys from the encryption key.
No lookup key uses OPE, and the key derivation has no OPE key.

| Part | Representation and protection |
| --- | --- |
| Memory | Selected keys and values are readable while the node uses them. |
| Main run blocks | Keys, molecule metadata, atoms, tips, counters, and control values share the existing block encryption. |
| Lookup blocks | The sparse lookup, its first keys, and its filters use encrypted blocks too. |
| Atom content | Plain logical JSON inside encrypted V2 blocks. No additional atom envelope is required. |
| Portable files | Existing file ciphertext and access bundles retain their separate portable format. |
| Cloud backup | The adapter sends the same encrypted native files. It adds no per-atom encryption layer. |
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
The ordered file layout leaves that option open without server OPE or complete schema-key preload.
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

Each schema read captures its declared key types, molecule bindings, and actual storage formats.
Object paths, map values, and union branches supply their declared types. Unknown declarations remain `Any`.
The read keeps this context through memory selection, disk reads, counts, and final response order.
A nested read refuses a different context.
Raw molecule and maintenance keys retain an explicit storage comparator.

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
The design adds no second molecule, stored key copy, or ordered lookup.
The schema declaration alone carries the type name.
Older readers cannot accept the new enum variants; they need an upgrade before a new date declaration reaches them.

## Typed storage profiles

The shared `RangeStorageProfile` enum defines versioned, reversible range formats.
An explicit call selects a profile from the selected field's declared type.
The profile describes one molecule. It adds no type tag to each stored key.
The molecule contract selects the profile for schema admission, queries, and writes.

`MoleculeRangeProfile` defines the molecule metadata contract.
`empty_v1` permits only the empty slot of a molecule without a range field.
`typed_v1` names one scalar profile.
The declared range type must agree with the stored profile.
Schema reads fetch uncached molecule metadata in one batch, then fetch the required key wraps in one batch.
The reader validates the complete set before it puts key bundles in the memory cache.
The runtime refuses a missing, incompatible, or incomplete contract before it serves records.

The molecule metadata uses version 2 and names its key authority explicitly.
Bundle authority is the only authority: an independent molecule bundle and its node wrap.
Schema publication creates every molecule with bundle authority. The reader refuses any other authority.
A share copies only that molecule bundle. It never copies account keys into a shareable bundle.
A normal mutation refuses a missing profile. Only schema publication can create a new molecule contract.
The existing three-key bundle wire form stays unchanged. Its third key has no user.

Every molecule uses its stored coordinate for its conflict IDs. The reader refuses any other conflict-ID domain.
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
Atom content retains its original value.
String values retain every byte, including control bytes and Unicode.
Only the String profile permits text prefixes.
Unsupported types, mixed scalar unions, unknown versions, and malformed fixed-width payloads receive a refusal.

A format 4 home uses the plain range encoding: the stored range bytes are the profile payload.
The block encryption of the run files protects them. Some integer keys grow under the fixed-width format.
No second lookup key or original range copy enters storage.
All stored range decode paths use the exact molecule codec.
Normal reads refuse a malformed range. V2 block encryption supplies authentication.

## Cloud backup

The paired V2 backup adapter binds the typed logical contract.
Backup profile version 2 names node format 4 and logical key format 7.
The program refuses profile version 1, which named node format 3 and logical key format 6.
The profile keeps the encrypted main and CAS files and the opaque cloud protocol.
The profile version binds the exact node marker and its byte count to the backup root.
Capture and restore check each schema against its existing molecule profile and key authority at the held roots.
Those checks use bounded batch reads. They create no key and change no profile.

## Later codec cleanup

These parts stay in the codec. A later change removes them:

- The plain hash encoding of `MoleculeKeyCodec`.
- The optional range profile in `MoleculeKeyCodec` and in the reader and writer calls that take `Option<MoleculeRangeProfile>`.
