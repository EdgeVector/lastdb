# Typed range field contract

Status: shared field semantics, captured comparison context, typed key conversion APIs, and a stopped-home inventory command.
The active node key format stays unchanged. Database conversion and activation remain pending.
The V2 review below revises the next packages. It activates no key or encryption format.

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

## Stopped-home inventory

The next conversion stage uses the product command:

```sh
lastdb --data-dir /absolute/path/to/stopped-development-home db inspect-range-conversion --source-range-encoding ope_v1 --json
```

The source range encoding is an explicit assertion about the old node configuration.
The command accepts `plain` or `ope_v1`. It never guesses a format from key appearance.
The report includes that assertion and its count and byte limits.
The command refuses the primary, legacy primary, and archived primary paths, including aliases.
It has no primary override. It does not create a database, start a node, or copy a home.

The source must stop cleanly and retain its shutdown flush receipt.
The command holds the existing native owner locks and one fixed publication.
It supports both the packed and run-file disk formats.
The run-file reader uses an existing lock and authenticated CURRENT without the writer's orphan cleanup.
The packed reader requires an existing lifetime lock before it opens a read.
No maintenance lock is created or truncated.
Publication and descriptor identities are checked again before the report returns.
An account-key proof and the normal replay-floor check precede the metadata inventory.

The inventory captures all metadata collections that the native namespace directory names, plus the current authoritative molecule collections.
It excludes atom bodies/counters and CAS bytes from the in-memory metadata set.
The packed visitor consumes excluded payloads without their retention in memory.
It streams the complete held generation; the capture limits count selected metadata only.
Run-file reads use bounded calls and resume strictly after the last examined key at the same fixed root.
Tombstones do not consume a cumulative allowance for live metadata.
The namespace directory uses the same continuation rule.
The report binds the source publications and captured metadata with SHA-256 fingerprints.
These fingerprints identify a snapshot. They are not authentication or permission tokens.

The schema catalog supplies each field's actual molecule identity and declared range type.
Mapped fields, shared molecules, absent range slots, and undeclared molecules remain explicit.
Different declared range domains on one molecule produce a blocker.
Unsupported range types and missing declarations produce blockers; no String field changes type automatically.
Existing per-molecule bundles supply the same range keys as the normal reader.
Missing bundle halves, malformed metadata, and source-format mismatches receive a refusal.

The inventory includes direct tips, every stored generation, sparse generation deletes, and Delete barriers.
It resolves current generation winners with the normal reader's functions.
It also captures history, conflict, retention, catalog, idempotency, and other control records in the metadata fingerprint and family counts.
Explicit Delete references and retention/drain slot pointers join the distinct key set, even without a live tip.
Retention repair can leave a valid queue entry with no slot or directory pointer.
The inventory preserves and counts that entry, reports an unresolved reference, and blocks its typed proposal.
It never invents a coordinate or treats the repair representation as malformed data.
History API coordinates are counted separately; they are not guessed to be storage coordinates.
The report does not save the captured metadata.
A future converter must capture it again from the same bound source publication.

For supported molecules, proposed keys preserve exact scope and hash bytes.
The inventory checks source range encoding, target scalar validation, collisions, and key byte limits.
Repeated references to the same stored coordinate count once.
The same coordinate under different scopes stays separate.
Counts distinguish checked source ranges from checked target mappings.
An unsupported proposal does not report a zero-byte target.
Key byte totals describe distinct normalized molecule coordinates; they do not predict final database size.
They exclude generation prefixes, barrier encodings, pointer values, counters, atoms, and files.

The default limits are one million metadata records, 128 MiB of captured metadata, and 64 KiB per key.
The command refuses an incomplete inventory when a limit is exceeded.
These limits bound captured data and key plans, not total process memory or temporary reader buffers.
The report always keeps `database_converted` and `activation_ready` false.
It does not certify counter integrity, classify every opaque control value, or authorize a file rewrite.

## V2 alignment review (2026-10-26)

Source: LastDB `32fc575f6`, including [disk V2 compact values, PR #418](https://github.com/EdgeVector/lastdb/pull/418).
Disk format V2 and cloud protocol V2 name separate formats.
This section supersedes any assumption that a V2 atom retains a separate content envelope.

### What fits the accepted V2 code

| Part | V2 behavior | Effect on typed ranges |
| --- | --- | --- |
| Field types and comparisons | The schema still declares each selected key field's type. | Keep the type contract, comparison context, and cursor checks. |
| Hash keys | The ordinary node still uses blinded hashes. | Preserve each existing hash byte. No hash conversion is needed. |
| Range keys | The ordinary node still uses the legacy OPE form. | The new physical format does not activate a typed range profile. |
| Atom values | V2 holds plain atom JSON inside encrypted run blocks. | Preserve this representation. Do not add an atom envelope or bundle name. |
| Stored values | V2 adds a value tag and a compact JSON form. | Use the home reader and writer. Do not parse stored run bytes as JSON. |
| Read-only inventory | It holds both homes and uses the shared `RunRead` methods. | Keep this path. Its range pages receive decoded metadata values. |
| Old experimental V2 | The shared descriptor reader refuses the untagged `LDRHOME2` format. | Do not add a fallback. Accepted compact homes use `LDRHOME3`. |
| Cloud backup | Current client capture still requires disk V1. | Disk V2 capture and restore need the separate client adapter. |

The inventory does not read atom bodies or CAS values.
Its compatibility with compact metadata follows from the shared value decoder.
That source finding does not prove an operational inventory run on a V2 home.
The next package must obtain that result before a conversion uses the inventory.

Relevant code:

- `AtomStore::with_namespaced_store` selects the V2 plain atom path.
- `encode_plain_atom` and `decode_plain_atom` define that atom representation.
- `RunRead::accept` and `RunHashRange::page` decode the stored value tag.
- `RunRead::open_existing` uses the shared descriptor and manifest readers.
- `range_inventory::FrozenSource` binds both native homes and checks the account proof.

### Simpler target for review

Recommend blinded hashes plus canonical ordered ranges inside V2 encrypted blocks.
Use the existing typed payloads, including fixed-width hex for scalar types.
Do not add a new binary key format, second molecule, range copy, or separate ordered lookup.
The existing user identity supplies the keys for hash blinding and physical encryption.

V2 run files already hold keys in order.
A read searches the run's sparse index, then the required blocks.
A range merges the matching records from the required runs.
These operations do not require every range key of a schema in memory.
The block encryption protects the stored keys; the range payload itself needs no OPE for those local operations.

Removal of the legacy OPE form remains a proposed key-format decision.
This review changes neither that setting nor the current encryption behavior.
Keep the existing OPE reader for explicit source conversion until that decision and the full reader contract are accepted.
Keep legacy String fields as String. Date-shaped text does not authorize a type change.

The current inventory uses one molecule codec for source decode and target estimates.
An OPE source therefore gets an OPE target estimate today.
Before a different target is estimated, separate the source and target codecs in the report and job identity.
Extend the key conversion API too; its constructor also gives both profiles one existing codec.
Do not present today's estimate as the size of a target without OPE.
Coordinate-byte totals still do not predict compressed file size.

### Physical conversion, cloud, and size boundaries

The V2 converter already handles old atom envelopes and writes plain V2 atom values.
It preserves the existing logical key bytes.
Keep that physical conversion separate from typed range conversion.
The first typed converter should accept an already converted, stopped V2 development home and write a V2 target.
V1 remains an inventory source; the V2 converter owns its physical normalization.
This avoids a second implementation of old envelope and atom conversion rules.

Use V2 home commits for target values, with fresh run identities and normal publication rules.
Keep atom identities, atom content, counters, and portable CAS ciphertext unchanged.
Transform only the range coordinates and their explicit references.
The stored value tag belongs to LastStore; the converter must not add or remove it itself.

The job must bind the source descriptors, both `CURRENT` files, logical format, profiles, and source and target codec choices.
An inventory fingerprint alone does not validate atoms, counters, or CAS files.
Full target verification still needs the checks below.

Cloud remains a store of opaque bytes.
A future browser reader needs client-side block navigation and decryption; server-side OPE comparison is not a requirement of that route.
Current backup support does not establish that future read capability.
The disk V2 client adapter must retain and restore both homes across restart before cloud-backed activation is claimed.
Keep that adapter with its existing cloud work; this range plan adds no service-side LastDB format logic.
Keep the current disk V2 capture refusal until that adapter is ready.
The V1 export encrypts metadata and sequence chunks, but leaves record body chunks as their stored bytes.
An unchanged export of V2 plain atom bodies would lack the run blocks' encryption.
Prefer the encrypted V2 files: main-home files go to R2, and CAS-home files go to B2.
The client manifest must retain both roots and their exact dependencies together.
This preserves the existing block ciphertext and avoids a new per-atom encryption layer for backup.

A separate target temporarily needs space beside its source, retained rollback files, and merge outputs.
Account for that total before a job starts. Do not claim the normal V2 size ratio covers conversion peaks.
Do not add per-key profile records or a permanent second lookup.
The [V2 size measurement PR #432](https://github.com/EdgeVector/lastdb/pull/432) is a dependency to observe, not an accepted size guarantee.

## Conversion and recovery boundary

A separate target keeps the source unchanged and simplifies recovery.
The future converter must bind its durable job to the source fingerprint, declared profiles, and explicit encoding configuration.
It must refuse a target from another job or a different source publication.
It must not copy the primary or use a copy of the primary.

The target first receives an unsupported in-progress logical marker.
Normal startup must refuse that marker, including after an interruption.
The converter then builds target records from the fixed source and records durable progress.
Every changed generation, barrier, retention pointer, and captured key reference needs an explicit format rule.
Opaque values must not receive speculative text substitutions.
Verification must cover all original records, atom references, counters, protein bindings, file references, and the replay floor.
Profile metadata and the final supported format marker publish only after those checks succeed.

An interrupted job resumes only through the explicit converter and the same bound source.
A failed source check leaves the target closed.
An abort discards only the exact owned target after its identity checks.
Startup never resumes or activates this typed range conversion.
The separately selected V1-to-V2 physical conversion retains its own rules.
Primary promotion remains a separate action.

## Remaining packages

1. Verify the inventory operationally on a stopped compact V2 development home. Record descriptor and codec facts explicitly.
2. Review the target range choice. Give the key conversion API, inventory, and job identity separate source and target codecs.
3. Build explicit V2-to-V2 target progress and its unsupported-format fence. Reuse the existing V2 reader, writer, and publication rules.
4. Define every changed coordinate and API reference, including conflicts and retention. Verify counters, atoms, proteins, files, and the replay floor.
5. Carry the selected profile through writes, exact reads, bounds, memory comparison, and cursors. Publish profiles and the supported marker last.

Date keys remain disabled until these paths agree. Typed range conversion never runs at startup.
V2 keeps its current block encryption and plain atom representation.
An active range-format change requires a separate review and decision.
The plan adds no separate `range_type` option that can contradict the selected field.
It does not require a cloud reader now.

## Validation and release

Local checks cover source format, source size, and the diff.
CI supplies builds, lint results, and the required measurements.
The retired tests and their replacements stay absent.
The PR uses the existing merge gates.
A primary upgrade remains a separate decision.
