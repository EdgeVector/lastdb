# Typed range field contract

Status: the runtime, separate-target converter, paired cloud adapter, and explicit CI comparison are in review.
Task-owned development homes use the typed format through product commands.
The primary and fresh-home default retain the legacy format. Primary promotion remains separate.

## Current state

A schema selects its hash field and range field from its existing fields.
Each selected field already has a declared type in `field_types`.
The legacy key path compares range strings. The typed path uses the declared scalar order and its stored molecule profile.
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

CAUTION: Legacy homes retain text order. They refuse Date and DateTime keys.

Typed homes permit Date and DateTime ranges. Temporal hash fields remain unsupported.
The checks cover nested object or map keys and temporal branches of a union.
An unrelated date field does not prevent a string field from serving as the key.
Wire schema loads, runtime field creation, schema-service proposals, and direct schema writes use the same check.
The schema service resolves canonical types and checks the result before a new schema or expansion reaches storage or memory.
It also resolves new reference fields before that check, to preserve their existing value validation.
The stored node format controls admission. A declaration cannot change the order of an existing home.

## Comparison context and cursors

Each schema read captures its declared key types, molecule bindings, and actual storage formats.
Object paths, map values, and union branches supply their declared types. Unknown declarations remain `Any`.
The read keeps this context through memory selection, disk reads, counts, and final response order.
A nested read refuses a different context. Each codec must match the captured order version.
Plain and OPE-v1 files both declare `LegacyUtf8V1`; their current key bytes and range order stay unchanged.
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
Only explicit offline conversion changes an existing home's range format. Legacy Integer ranges retain text order until that conversion.

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

Existing enum names, JSON values, and field identities stay unchanged. Conversion changes range bytes and their explicit storage references.
Existing `String` fields retain their type, even when they contain dates.
No existing schema or Schema.org seed changes type automatically.
The change adds no second molecule, stored key copy, or ordered lookup.
The schema declaration alone carries the new type name.
Older readers cannot accept the new enum variants; they need an upgrade before a new date declaration reaches them.

## Typed storage profiles

The shared `RangeStorageProfile` enum defines versioned, reversible range formats.
An explicit call selects a profile from the selected field's declared type.
The profile describes one molecule. It adds no type tag to each stored key.
The persisted node format and molecule contract select the profile for schema admission, queries, and writes.

`MoleculeRangeProfile` defines the molecule metadata contract.
`empty_v1` permits only the empty slot of a molecule without a range field.
`typed_v1` names one scalar profile and refuses the legacy payload.
The declared range type must agree with the stored profile.
The current metadata reader refuses a range profile in a legacy molecule record.
It does not reinterpret that record or activate a typed reader.
Schema reads fetch uncached molecule metadata in one batch, then fetch the required key wraps in one batch.
The reader validates the complete set before it puts key bundles in the memory cache.
The runtime refuses a missing, incompatible, or incomplete contract before it serves typed records.

The typed metadata uses version 2 and names its key authority explicitly.
Bundle authority retains the existing independent molecule bundle and its node wrap.
Node authority retains an old account-derived key domain and requires no molecule wrap.
A share refuses node authority. It never copies account keys into a shareable bundle.
A normal mutation refuses a missing profile. Only schema publication can create a new typed molecule contract.
The existing three-key bundle wire form stays unchanged.

Historical conflict IDs retain their source key domain after conversion.
The existing molecule metadata names that domain once. No second range copy enters each key.
The runtime uses that domain only to reproduce retained conflict IDs.
An old OPE domain therefore remains available for this compatibility calculation, never for a typed lookup key.
Existing history supplies an original key spelling when canonical conversion changes that spelling.
The existing slot directory locates those history rows with bounded reads.
A repeated conflict preserves its old row, atom references, and resolved state.
The runtime refuses inconsistent history or an exceeded allowance.
New molecules use the current stored coordinate for their conflict IDs.
History responses use canonical API keys. Stored history and signed provenance retain their original bytes.
The converter preserves the replay floor and all retained history IDs.

Range TTL retains its existing 20-digit timestamp prefix in a String range.
Typed Integer, Date, DateTime, and Boolean ranges do not inherit that TTL rule.
Their range-TTL policies receive a refusal until an explicit retention-policy conversion defines the expiry clock.
Hash and Single schemas retain their existing write-time age records.
The offline converter must check this policy condition before it publishes a typed target.

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

`TypedScalarV1` defines the future API comparison order.
Its explicit constructor accepts one supported scalar domain.
A comparison returns an error for an invalid coordinate.
The captured read retains a comparison error and refuses the result.
Response rows validate their range coordinates before the sort.
Legacy comparisons retain text order. The ordinary node does not select the typed comparison yet.

All stored range decode paths use the exact molecule codec.
A plain range that resembles an OPE token stays plain.
Normal reads refuse a malformed range. V2 block encryption supplies authentication.
Offline conversion also requires the same bytes after re-encoding under the selected molecule key.
Raw diagnostics can retain the stored row without an API range value.
This check does not make OPE-v1 authenticated or stop its known plaintext disclosure.

## Explicit key conversion

`RangeKeyConversion` converts a caller-supplied set of current `mk:` range keys for one molecule.
The caller selects both source and target profiles and an explicit target range encoding.
The source codec supplies its existing range encoding and keys.
The target retains those keys and the hash encoding; only its range encoding can differ.
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

The inventory uses the product command:

```sh
lastdb --data-dir /absolute/path/to/stopped-development-home db inspect-range-conversion --source-range-encoding ope_v1 --target-range-encoding plain --json
```

The source range encoding is an explicit assertion about the old node configuration.
The command accepts `plain` or `ope_v1`. It never guesses a format from key appearance.
The target range encoding is also explicit and required.
The version-2 report includes both choices and its count and byte limits.
It also reports the checked logical key marker, replay floor, and held home generations and frontiers.
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
The converter captures it again from the same bound source publication.

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
| Cloud backup | The paired client adapter retains encrypted V2 main and CAS files. | Its version-2 profile binds the typed node and logical markers. |

The inventory does not read atom bodies or CAS values.
Its compatibility with compact metadata follows from the shared value decoder.
Stopped-home inventory and conversion operations now use CI-built programs on fresh development data.
The manifest retention alias, original source hashes, and an 8 MiB file also pass those operations.

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

The explicit converter removes OPE from typed lookup keys. The existing block encryption remains in place.
The OPE reader remains for legacy sources and historical conflict identifiers.
Keep legacy String fields as String. Date-shaped text does not authorize a type change.

The inventory and key conversion API now select the target range encoding explicitly.
Source decode retains its existing encoding and keys.
An OPE source can therefore estimate a typed target without OPE, with its hash bytes unchanged.
These estimates do not convert a home or activate a profile.
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
The paired adapter uses the existing opaque protocol and validates the typed contract before capture or restore publication.
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

The implementation in review adds `lastdb db convert-typed-ranges` for a stopped compact V2 development home.
It requires a separate target and explicit source hash and range encodings.
The first converter preserves an explicitly blinded hash domain.
The old logical format does not record that setting. The source choice therefore remains a caller assertion.
The converter never selects the source encoding from a key's appearance.

An offline snapshot cursor reads every physical collection, including collections absent from the namespace list.
The target receives fresh encrypted main and CAS files.
Main copy batches include their encrypted checkpoint in the same commit.
CAS batches publish first. An interruption can repeat identical CAS keys and values.
The checkpoint binds the source publications, metadata, codec choices, limits, target path, and target file identities.

The converter closes and reopens both target homes before verification.
It compares every source record with its explicit target form and checks the exact target record count.
The existing product reference verifier checks atoms, counters, molecules, retained records, and files on the inactive target.
The converter does not create a second reference verifier or a fixture harness.
The final node marker publishes only after those checks pass.
Product operations cover initial-fence, copy-page, and final-publication interruptions, explicit resume, and wrong-job refusal.
Normal startup refuses each interrupted target without data-file changes.

A separate target keeps the source unchanged and simplifies recovery.
The converter binds its durable job to the source fingerprint, declared profiles, and explicit encoding configuration.
It refuses a target from another job or a different source publication.
It cannot copy the primary or use a copy of the primary.

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

## Implementation packages

| Package | Result |
| --- | --- |
| Inventory and codecs | The source and target choices remain independent. Stopped compact V2 operations preserve all source hashes. |
| Persistent contract | Node format 4, logical key format 7, and one molecule profile select typed order. Unknown or mismatched contracts receive a refusal. |
| Runtime | Product operations cover all five scalar domains, exact and range reads, prefixes, counts, cursors, changes, deletes, TTL, flush, and reopen. |
| Converter | The separate target uses encrypted progress, complete physical collection copies, explicit reference translation, exact verification, and final marker publication. |
| Cloud | Actual DEV backup and restore survive process interruption and explicit resume. Wrong-account restore receives a refusal before destination creation. |
| Measurements | The explicit paired CI comparison completes. It records size reductions and repeated speed advisories. Normal review and merge gates remain required. |

Typed range conversion never runs at startup.
V2 keeps its current block encryption and plain atom representation.
An active primary range-format change requires a separate review and decision.
The plan adds no separate `range_type` option that can contradict the selected field.
It does not require a cloud reader now.

## Validation and release

The existing paired V2 backup adapter also needs the typed logical contract.
Backup profile version 1 retains node format 3 and logical key format 6.
Backup profile version 2 names node format 4 and logical key format 7.
Both profiles retain the same encrypted main and CAS files and the same opaque cloud protocol.
The profile version binds the exact node marker and its byte count to the backup root.
Typed capture and restore check each schema against its existing molecule profile and key authority at the held roots.
Those checks use bounded batch reads. They create no key and change no profile.
Older clients refuse the new profile version.
Typed backup, interruption, and fresh restore operations pass on the CI-built adapter.
The bounded historical schema-envelope decoder still requires its current-head CI and review.
The plan adds no cloud service format logic and no browser reader.

Local checks cover source format, source size, and the diff.
CI supplies builds, lint results, and the required measurements.
The retired tests and their replacements stay absent.
The PR uses the existing merge gates.
A primary upgrade remains a separate decision.

## Reference and replay audit

| Record or path | Conversion and runtime rule |
| --- | --- |
| `mk`, `mgr`, `mgd` | Translate the range suffix. Preserve the scope, hash, molecule, generation, and tip values. |
| `mgp`, molecule headers | Preserve generation pointers and header identities. The inventory uses the normal generation winner functions. |
| `rdel` | Translate `DeleteBarrier.mk_key` and recompute its derived barrier key. Preserve the clock and source identity. |
| Retained records | Translate the exact slot and directory. Preserve history, conflict, queue identities, atom references, and expiry times. |
| Retained drains | Translate the slot and directory prefix. Recompute only the queue identity derived from that prefix. |
| History and conflicts | Preserve historical IDs and original field coordinates. Translate only the captured Delete storage reference. Canonicalize API responses separately. |
| Signed provenance | Preserve original mutation coordinates, UUIDs, signatures, clocks, and provenance. Runtime preparation computes a separate canonical lookup key. |
| Replay floor | Preserve the existing floor value. Typed startup accepts it only with the matching format marker. Imported clocks retain the existing signature and cutoff checks. |
| Proteins | Preserve molecule identities, field names, and bindings. Key moves use the selected molecule codec. Source-coordinate checks use typed range equality. |
| Atoms, counters, file records | Preserve exact logical values. The existing product reference verifier checks the inactive target before publication. |
| Other collections | The physical snapshot cursor copies every collection. A control record absent from the bound metadata inventory receives a refusal. |

The metadata inventory uses the namespace directory and mandatory molecule collections.
The complete physical copy does not trust that directory as a collection census.
Exact record comparison and target counts cover both native homes after reopen.
These source conclusions do not claim a new live cloud replay operation or a new browser reader.

## Measurement limits and rollout

The [explicit CI comparison](https://github.com/EdgeVector/lastdb/actions/runs/40410695806) uses identical product source on both sides.
Both sides use the same String schema and values. Only the candidate converts its generated source.
Every source-file hash remains unchanged. Every measured request completes without an error.

The main sources contain 110,302 physical records. Conversion takes 14.60 to 19.21 seconds.
The source occupies 24.13 to 24.79 MiB. The typed target occupies 10.57 MiB.
The retained source-plus-target total occupies 34.70 to 35.35 MiB. Transient merge peaks remain unmeasured.
The first converter process uses a maximum of 138.60 MiB of memory on that CI host.

Cold single-read p95 changes from 3.748 to 1.504 ms. Cold batch-32 p95 changes from 20.865 to 17.915 ms.
Two speed advisories repeat: four-client mixed reads and batch-128 inserts.
Mixed-read p95 changes from 3.864 to 5.388 ms, then from 3.859 to 5.141 ms in the repeat.
Insert p95 changes from 211.725 to 264.954 ms, then from 230.327 to 347.788 ms in the repeat.
The artifact includes request phases. These measurements do not establish one cause for the slowdowns.

The memory-growth notice does not repeat its complete threshold. This does not prove a universal memory bound.
Disk use improves, but update and insert checkpoints still exceed the existing absolute size budget.
Their maximum ratio changes from 4.460 to 3.829, and from 4.460 to 3.725 in the repeat.
The limits remain visible for primary rollout review. Speed advisories do not replace required correctness or artifact checks.

The actual typed DEV restore preserves all 13 captured file hashes before the first start.
Two node starts return all 14 records across six schemas and fetch the complete 8 MiB file with its original hash.
The recovery home contains credentials only. The restored home contains no cloud credentials.
The primary remains unchanged. A later primary plan must review capacity, rollback retention, and the measured advisories before activation.
