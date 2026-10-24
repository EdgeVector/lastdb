# OPE Range Keys: Recommendation And Implementation Plan

Status: Implementation started at Tom's request, 2026-10-24. Product activation and service changes remain separate steps.

Select the published CLWW-derived exact OPE construction with backward carry and high-bit placement.
Implement it independently in LastDB under Apache-2.0 with existing RustCrypto primitives.
The [codec contract](../../lastdb_range_crypto/README.md) fixes the private-key derivation and candidate byte representation.
Use it for range selection under one named hash, with or without optional preload.
Keep hash blinding, atom-content encryption, and file-blob encryption.
Keep the native pack engine and its ordinary byte comparison.

The selected implementation is LastDB-owned source, without the restricted vendor library.
The later construction and this byte profile still require independent cryptographic review before activation.
The [permissive implementation review](ope-permissive-implementation-review.md) records the available alternatives and the private-key requirement.
The earlier [mandatory-preload design](encrypted-range-keys-local-preload.md) remains an alternative.
This plan replaces its delivery sequence as the current recommendation.
The [primary adoption plan](ope-primary-cutover-plan.md) defines the later release and data-conversion gates.
It does not authorize a primary change.

## 1. Current state

The first implementation starts from accepted main revision `0a9d8cf48470c21f0b7b9fef250a2d5598ca2b09` in a separate DEV worktree.
The earlier inventory uses wrapper-branch revision `1e45b9a458dce2c80166a3b322f81293608c265b` and remains a source map.
Refresh each affected reader and writer against its accepted implementation base before its package starts.
Keep this work separate from [PR 321](https://github.com/EdgeVector/lastdb/pull/321).

The current `ope_v1` copies each plaintext byte into the low byte of an eight-character hex word.
Its decoder recovers the range without a key.
Replace that encoding; it supplies no range confidentiality.
See the [encoder and decoder](../../fold_db/crates/core/src/crypto/e2e.rs#L148).

Current readers use both disk order and optional complete key sets in memory.
The [tree reader](../../vendor/laststore/src/disk_pack/tree_cursor.rs#L70) fetches a page, compares keys, and follows a child pointer.
Real OPE fits that route without a full pack-file load.
An object store still needs a reader or query service to execute those steps.

The earlier [reader and writer inventory](encrypted-range-keys-reader-writer-inventory.md) identifies 18 reader families and 19 writer families.
Its 80 explicit source matches are review candidates, rather than an edit count.
Its impact descriptions assume mandatory preload; the scope decisions below replace those descriptions for this proposal.

## 2. Scheme recommendation and limits

CLWW means Chenette, Lewi, Weis, and Wu.
Their [FSE 2016 paper](https://saweis.net/pdfs/pore-clww.pdf) describes practical ORE and its leakage model.
It does not establish ordinary content-encryption secrecy for searchable terms.

The selected published extension places each plaintext bit at the high bit of its output byte and retains the backward carry.
The [construction note](https://cipherstash.com/research/backward-carry-ope) describes that extension.
LastDB uses HMAC-SHA256, a fixed input marker, and its own scoped derivation and stored-byte identity.
This is a later engineering construction, rather than the original paper's probabilistic OPE conversion.
The original paper does not establish the extension's security or this profile's full leakage.
Do not reinterpret ordinary CLWW ORE bytes as OPE or use the earlier incomplete carry fix.
The vendor [explains the order errors in those variants](https://cipherstash.com/blog/fixing-a-1-in-256-bug-in-cllw-ope).

The previously inspected vendor reference is `clww-ore` 0.6.0 at source commit `190f062fb9865e562f293f1e42fbdea3336ed07f`.
The [package metadata](https://github.com/cipherstash/ore.rs/blob/190f062fb9865e562f293f1e42fbdea3336ed07f/packages/clww-ore/Cargo.toml) supplies that version.
This reference identifies inspected source; it does not establish a published release or an independent audit.

| Choice | Benefit | Cost or reason to defer |
|---|---|---|
| CLWW OPE candidate | Ordinary byte order; direct tree seeks; deterministic exact terms | Order, equality, length, and additional prefix information remain visible |
| Lewi-Wu Block ORE | A more restricted leakage profile than CLWW | A custom comparator and a wider pack-engine change |
| Encrypted tree pages with client comparison | No searchable range terms on the server | A new page protocol and more client/cloud round trips |
| Mandatory local preload | Ordinary encryption for stored ranges | A complete local key set becomes a correctness requirement |

The [Block ORE implementation](https://github.com/cipherstash/ore.rs/tree/190f062fb9865e562f293f1e42fbdea3336ed07f/packages/ore-rs) remains an alternative if the CLWW leakage is unacceptable.
It also requires its own license and implementation review.
Do not switch schemes silently if the selected candidate fails a release condition.

CAUTION: OPE deliberately exposes order and equality. Known values and a small range domain can permit value inference without the encryption key.

The review must define additional leakage from the exact variant, its variable-length representation, and repeated queries.
Do not promise that the cloud cannot infer a range value.
Scope comparable terms to one molecule and one blinded hash to limit unnecessary cross-hash comparison.
This does not remove inference within that hash or ordinary access-pattern leakage.
The confidentiality goal assumes the observer lacks the molecule keys and their root keys.
A readable local identity key or authorized process can expose the protected values.

The vendor reference's [license](https://github.com/cipherstash/ore.rs/blob/190f062fb9865e562f293f1e42fbdea3336ed07f/packages/clww-ore/LICENSE) is PolyForm Internal Use 1.0.0 and does not permit distribution.
Do not add or copy that library into public LastDB.
The independent LastDB source uses existing permitted primitives and the published mathematical construction.
Its first package does not select a new native codec or claim release-ready confidentiality.

## 3. Proposed stored form

### Key scope and byte order

Use a portable Rust module without LastStore, cloud-service, daemon, or Node dependencies.
Give native and future browser clients one byte contract.
Reserve `clww_ope_v2` for the future native selector.
The first portable profile is `lastdb:clww-carry128:hmac-sha256:v1`.
Assign the native home marker before the first writer merges.
The provider version and the LastDB format version are different identities.

Use exact unsigned UTF-8 byte order from today's API.
Do not add numeric interpretation, Unicode normalization, case conversion, or accent conversion.
The reference library's text target changes text equality and order.
LastDB's independent implementation consumes raw bytes and has no provider text normalization.
See the [scheme entry points](https://github.com/cipherstash/ore.rs/blob/190f062fb9865e562f293f1e42fbdea3336ed07f/packages/clww-ore/src/order_scheme.rs).

Derive the range root directly from the user's 32-byte private identity seed through a separate HKDF-SHA256 purpose.
Derive a molecule secret from that root, format, exact molecule identity, and epoch.
Derive the stream key from that molecule secret and the stored blinded hash.
Use length-framed context fields and separate key purposes for OPE and range-value encryption.
Keep this deterministic range purpose distinct from the current random molecule bundle and existing content keys.
The original-range encryption key remains a separate integration purpose, rather than the OPE stream key.
Bounds and stored values use the same context and byte target.
Different devices with the same authorized bundle produce the same exact lookup term.
Do not share a keystream between OPE and ORE terms; no ORE term is required by this design.

Retain the outer key frame:

```text
mk:{molecule}:{escaped_blinded_hash}\0{lowercase_hex(ope_term)}
```

Lowercase hex preserves unsigned byte order and fits the current string key interfaces.
Keep the format and epoch fixed for each ordered stream.
Do not merge terms from different epochs or codecs through one ordinary byte comparator.
Store the selected format explicitly in molecule metadata and the home compatibility boundary.

Encode an empty HashRange range as a real term.
Keep the hash-only empty slot distinct through the schema's key type.
Audit every current empty-range shortcut and the synthetic first-word lane prefix.
Neither the old empty token nor the old six-character lane prefix is a valid assumption for the new format.

Define the complete expanded key-size limits before selection of the format.
The portable profile consumes a fixed marker plus the complete input, then emits one byte per bit and one carry byte.
For `N` logical bytes, the raw term uses `8*N + 9` bytes and hex uses `16*N + 18` bytes.
The current native stored-key cap is 65,535 bytes; `ope_v1` currently uses eight characters per logical range byte.
Preserve the complete existing logical range allowance through a versioned storage-limit change before native activation.
Do not silently halve that allowance merely because the new hex form is larger.
Stored original-range encryption increases value size separately from that key limit.
Reject an oversized write before its ack; never truncate a range.
Inventory existing key lengths before a migration policy is fixed.

### Original range value

OPE is the searchable term, rather than the authoritative source of API text.
The reference exposes no decryption API.
Store an independently encrypted original range in the tip value, with authenticated context that includes its molecule, hash, term, format, and epoch.
Reuse LastDB's established content-encryption primitive with a separate key purpose.
Use the primitive's required nonce rules and freeze prepared ciphertext for a retry.

Add that field to [PerKeyRecord](../../fold_db/crates/core/src/db_operations/atom_store/types.rs#L100).
Generation slots retain it through their embedded records.
Return the original value only after decryption and a match between its regenerated OPE term and the stored key.
An exact query can use the supplied range, but it still validates any returned protected range field.
Missing or invalid fields produce an error in the new format, without a token-as-plaintext fallback.

This protects one range value that atom content does not necessarily contain.
It does not encrypt the entire tip, atom header, pack file, or generic stored value.
The tip's atom reference and permitted structural metadata remain available to the reader.
Atom content and file bytes retain their existing separate encryption purposes.

## 4. Reads and optional preload

| Read | New route |
|---|---|
| Exact hash plus range | Blind the hash, produce the deterministic OPE term, and fetch the existing exact tip path |
| Several exact keys | Prepare all terms, then use one batch read by key |
| Range between two bounds | Encrypt both bounds in one hash context; seek and page through that ordered interval |
| Next page after a range | Encrypt the cursor range; use the existing exclusive cursor rule |
| Prefix under one hash | Convert the plaintext byte prefix to a half-open interval, then encrypt its bounds |
| All ranges under one hash | Use that hash's ordered stream, with explicit minimum and maximum bounds |

For prefix `p`, use `[p, successor(p))` in unsigned byte order.
The successor increments the rightmost byte below `0xff` and removes the suffix after it.
No successor means an unbounded upper end within that named hash.
An empty prefix selects that hash's complete range stream.
The bound encoder accepts byte slices, including bounds that are not valid UTF-8 strings.
Never assume that the encrypted prefix is a byte prefix of encrypted complete values.

Normalize these bounds once for disk selection, ready-memory selection, and final response filters.
Keep inclusive and exclusive semantics explicit.
Replace prefix checks and old lane shortcuts in all three paths.
An authenticated cursor must also bind its query, scope, format, and epoch if the cloud accepts opaque cursors later.

Keep preload optional.
A ready local key set may retain sortable protected terms and decrypt only the selected records.
A memory refusal, timeout, invalidation, or absent ready set selects the equivalent disk route.
Those events must not change query results or require an unbounded load.
Original range recovery belongs after tip selection; raw key publication need not decrypt every key.

Preserve the product's exact-key and batched-key routes.
A range seek stays scoped to one named hash and costs the seek plus the candidates and results it examines.
Do not add a full-schema query or a hidden complete preload to an exact lookup.

## 5. Source impact

| Area | Required change | Principal source |
|---|---|---|
| Codec and original value | Scoped term creation, byte bounds, size refusal, encrypted range field | [Codec](../../fold_db/crates/core/src/atom/molecule_key_codec.rs), [crypto](../../fold_db/crates/core/src/crypto/e2e.rs), [types](../../fold_db/crates/core/src/db_operations/atom_store/types.rs) |
| Key bundles | Separate range purposes, format and epoch, shared-device stability | [Bundle store](../../fold_db/crates/core/src/db_operations/molecule_key_store.rs), [frozen reader](../../fold_db/crates/core/src/db_operations/molecule_key_store/frozen.rs) |
| Query selection | Hash context, prefix intervals, cursor bounds, original API values | [Selection](../../fold_db/crates/core/src/db_operations/atom_store/filter/hash_selection.rs), [disk pages](../../fold_db/crates/core/src/db_operations/atom_store/filter/partition_page.rs), [narrowed reads](../../fold_db/crates/core/src/db_operations/atom_store/filter/load/hash_range_scan.rs) |
| Field selection | Remove encrypted-prefix and keyless-decoder assumptions | [Stored bounds](../../fold_db/crates/core/src/schema/types/field/filter_utils/storage_bounds.rs), [field resolution](../../fold_db/crates/core/src/schema/types/field/variant/read/resolve_scan.rs) |
| Optional preload | Admit the new term format; preserve disk fallback; recover only selected original values | [Ready windows](../../fold_db/crates/core/src/db_operations/atom_store/schema_keys/hash_window.rs), [publication](../../fold_db/crates/core/src/storage/disk_pack/schema_keys/publication.rs) |
| Ordinary and protein writes | Prepare the term and encrypted range once; update related molecules in the accepted batch | [Changed keys](../../fold_db/crates/core/src/db_operations/atom_store/molecules/store/changed_keys.rs), [key moves](../../fold_db/crates/core/src/fold_db_core/mutation_manager/molecules/persist/key_moves.rs) |
| Controls and maintenance | Use the same term in tips, generations, deletes, retention, TTL, repair, and reference updates | [Counter planner](../../fold_db/crates/core/src/db_operations/atom_store/counter/build.rs), [barriers](../../fold_db/crates/core/src/atom/delete_barrier.rs), [retention keys](../../fold_db/crates/core/src/db_operations/atom_store/retention/keys.rs) |
| Conversion and recovery | Rewrite related identities and compatibility markers; preserve encrypted originals | [Format](../../fold_db/crates/core/src/db_operations/atom_store/key_format.rs), [native transfer](../../lastdb_node/src/native_transfer_target.rs), [restore](../../lastdb_node/src/native_cloud_restore/records.rs) |
| Native pack engine | Check expanded key limits and tree reuse; retain ordinary byte comparison | [Hash-range reader](../../vendor/laststore/src/disk_pack/hash_range.rs), [tree reader](../../vendor/laststore/src/disk_pack/tree_cursor.rs) |

Preserve the outer `mk:`, `mgr:v1:`, `mgd:v1:`, `mgp:v1:`, `rdel:v2:`, and `crs:v1:` purposes.
Helpers that treat the range suffix as opaque may remain unchanged.
Conversion must still rewrite each stored identity and each value that embeds that identity.
No counter changes its ownership rule merely because the key encoding changes.

Audit original range copies outside tips before any claim of cloud range confidentiality.
The earlier inventory identifies conflict values, durable app change events, sync re-export markers, and local search delivery files.
Protect private key fields in core-owned stored and cloud-delivered records, or exclude them through an explicit supported boundary.
Do not restore the generic storage wrapper as a shortcut.
Authorized local apps can still receive plaintext ranges; their own storage is a separate privacy boundary.
State that boundary explicitly, including the local search inbox.

## 6. Delivery sequence

Each package produces a reviewable result with its own description and required CI results.
Product work starts in a new DEV worktree from accepted main.
Do not resume, merge, or expand the paused wrapper PR as an implied step of this plan.

| Package | Reviewable result | Depends on |
|---|---|---|
| 0. Scheme and byte contract | Published extension, owned implementation, review scope, explicit leakage limits, size compatibility, and private-seed derivation | Started |
| 1. Portable codec | Whole-byte range terms, deterministic scoped keys, prefix intervals, bounded hex format, and portable source | Candidate source started with 0 |
| 2. Writers and private copies | All writers produce coherent terms and original-range fields; controls and retained key copies use the same contract | 1 |
| 3. Readers and optional preload | Exact, batch, range, prefix, cursor, shared-source, maintenance, and ready-memory readers use the same format | 1 and 2 |
| 4. Conversion and restore | Separate-target conversion, compatibility refusal, backup restore, and old-reader refusal | 2 and 3 |
| 5. Local release | Required CI and operational results, completed crypto review, and explicit new-format selection | 0 through 4 |
| 6. Direct cloud and browser reads | A separate authorized query protocol and browser client using the same byte contract | 5 and an accepted cloud read design |

Packages 1 through 4 keep the accepted format selected until the complete new route and independent cryptographic review are ready.
Do not merge an active writer before every serving reader accepts its output.
If cryptographic review rejects the candidate, stop adoption and present the alternative cost.
Do not leave an experimental provider as the default.

### Package 0 decisions

Record the owned source, primitive dependency licenses, and precise stored-byte version.
Independent review covers exact order, variable-length values, prefix bounds, key contexts, and the vendor variant's leakage.
It also covers key derivation, nonce use, wrong-key errors, and the integrity limits of public searchable terms.
Freeze this profile's derivation and byte rules; a dependency update must not silently change stored terms.
Resolve expanded tip-key bytes and supported old formats before package 2 introduces an active writer.

### First PR boundary

The first PR adds the `lastdb_range_crypto` crate and its public core re-export.
It implements private-seed derivation, scoped symmetric keys, full-byte OPE terms, prefix intervals, and bounded canonical hex.
It updates this plan and carries the earlier design alternatives as context.
It changes no active encoding selector, home marker, molecule writer, primary, or cloud service.
Build and lint results do not establish cryptographic assurance or browser runtime support.
Encrypted original-range fields move to the reader/writer integration packages before native activation.

### Conversion and key epochs

Use an explicit offline converter with an owned, separate, inactive target.
Never convert a live home in place, copy the primary, or use the archived primary as migration material.
Conversion trials use fresh DEV data in CI.
The existing native transfer path does not perform this key-identity rewrite.
Primary adoption requires separate, candidate-specific approval for logical record transfer and target checks.
The primary adoption plan keeps that approval distinct from the prohibition on filesystem clones.

Read the source's explicit format and molecule bundles.
Recover legacy original ranges only through that format's decoder; validate their regenerated source keys.
Use the stored blinded hash as context where its original hash is unavailable.
Rewrite tips, generation records and deletes, barrier keys and values, retention identities, and other embedded key references.
Preserve atom identities, contents, write-order facts, reference counts, and selected-generation precedence.
Do not retain a legacy term beside its replacement as an online compatibility route.
Conversion does not protect old backups, old pack files, or earlier copies of the reversible terms.
Define their retirement through the normal retention and recovery policy before a claim about all retained cloud data.

Verify target completeness and format-specific key relationships through the converter's normal validation step.
Publish the target's serving marker only after successful final validation.
An interruption leaves a non-serving target and an unchanged source.
Define the supported resume or restart procedure before release.
Old programs must refuse the new format; the new program must refuse an unsupported old home with a conversion instruction.

An OPE key change rewrites the affected ordered streams.
An access-domain key change can re-encrypt its molecule bundle without changing the OPE key.
Do not confuse those operations or promise that former members lose data they already received.

### Verification and release

Tests remain retired. Do not add or run a test suite, fixture harness, or test-coverage gate.
Tom authorized one CI-only crypto proof program through "Do all of these" on 2026-10-24.
That narrow exception covers the portable OPE proof requested on PR 365; it does not restore other tests.
Local work uses source, syntax, documentation-link, and format checks only.
Run LastDB builds, lint builds, and measurements in GitHub Actions.
Product PRs retain `ci-required`, `generated-evidence`, and `benchmark-complete`.
Any new shared workflow or service change requires Tom's approval after preparation for review.

Use normal product commands on fresh CI DEV data for operational evidence.
Read exact and batched keys, prefixes, between bounds, and cursor pages before and after reopen and restore.
Include empty ranges, Unicode bytes, case distinctions, embedded zero bytes, long keys, deletes, protein key moves, and retained conflicts.
Compare preload enabled and disabled through the same supported commands.
Confirm wrong-key refusal and refusal of unsupported stored formats through normal open and restore operations.
These are operational observations; source review alone does not establish the live result.

Measure cold range seeks, exact reads, large ranges, write and flush cost, memory size, disk size, and expanded key limits.
Use the existing CI benchmark workflow; do not run local builds or benchmarks while CI waits.
Keep raw output outside Git and use CI summaries and artifact links for the validation record.
Read PR comments, reviews, and CI after every publication; continue until each implementation PR merges or closes.
No primary upgrade or cloud activation forms part of the planning task.

## 7. Future cloud and browser boundary

The local change preserves an ordered representation that a future cloud reader can compare without a range key.
A browser holds an authorized molecule bundle, blinds the hash, produces range bounds, and requests matching encrypted records.
The cloud uses ordered seeks; the browser decrypts original ranges and atom content.
The browser checks the returned range against its term and requested bounds.
It does not need every range key or a complete pack file.

OPE does not supply cloud authorization, a coherent revision, freshness, or proof that no result is missing.
Define those properties, resource bounds, and cursor rules in the separate cloud read protocol.
Protect responses and data through the established transport and content-encryption contracts.
A full-pack backup route alone does not implement direct logical queries.
No shared service deployment is required for the local format work.

## 8. First implementation action

Publish and validate the first portable codec PR through existing GitHub checks and review.
Then complete the cryptographic review and the native size contract before an active format writer proceeds.
The next product package adds encrypted original-range fields and coherent writer integration.
