# Native disk engine cutover

The development branch now uses the native packed disk engine as its sole logical engine.
Normal startup and direct FoldDB constructors select it without a feature flag.
An empty home initializes directly. An older nonempty format receives an error.
No migration code or automatic conversion was added.
Cloud activation stays disabled for this round.
The primary and its archive stay untouched.

## Source and release

| Item | Identity |
|---|---|
| Branch | `fix/core-owned-writes` |
| Full API workflow commit | `a60b27d43f11fd6d1f3cf514d1579229d2256d5c` |
| Full API workflow tree | `9c8d8dd75a5d2ebe8848b04808796db2ea87a386` |
| Full API workflow release SHA256 | `bef1ff5b9047e8f76f2b42ec5f581bc385467d99785b6325a5a56a6d3f496f5a` |
| Final product commit | `436cc86ad82c4ca2486e0b7f55e4a1b840dfeaed` |
| Final product tree | `55bfe92b78ad52b02706c41a9e9b53dfd0e4379c` |
| Final release SHA256 | `66879950810b0c9b09756ff1ed0d6981b3ce3af849a89ab3436d7f23bf880045` |
| Source inputs | 1,883; all match the committed product tree. |
| Full API workflow driver | V7, 22 inputs; SHA256 `898f396743ad339e3873318d0287ba094a0edb4b0f272e4cbc6488b7745e3ca0`. |

The [cohort release identity](candidate-final-identity.json) records the exact build and host settings for the three full workflows.
The [committed-source check](committed-final-source-verification.json) matches captured hashes, sizes, current bytes, and Git blobs.
The [final release identity](response-final-identity.json) records the later public error-message correction.
The [final committed-source check](committed-response-source-verification.json) matches all 1,883 inputs to the final product commit.
The [product delta](response-product-delta.json) confirms one changed product input, with no added or removed inputs.
That change removes four unsupported selectors from the advice list. It changes no admission, lookup, count, or write logic.
The release uses normal default features.
The [validation report](VALIDATION.md) records the compiler and source checks.

## Design and cleanup

The core model owns current schema definitions, molecule definitions, keys, tips, confirmed absence, and full atom values.
The recency list contains logical record references and selects clean records for purge.
One call captures its core state and disk root. Captured values keep their memory charges until the call releases them.
A private write queue keeps prepared disk bytes and resources until flush. Ordinary reads cannot inspect that queue.
The [source review](REVIEW.md) records the responsibilities and detailed limits.

The old serving graph, logical caches, alternate write branch, disabled range helpers, and dummy worker are removed.
Normal logical constructors reject a physical store without the native runtime.
Public query and field helpers share the supported-layout checks and captured current read state.
Hash and HashRange remain supported. Standalone Range, Single, unsupported selectors, and historical reads receive explicit errors.
The public error response recommends only the five supported native selectors.
Current flat and compound keys still share one physical prefix union. That current format rule does not select an older engine.

The purge guard now reads actual current core heads and excludes only the exact slots that the purge removes.
Another current slot can still protect a shared atom.
Keyed atom content and molecule key wraps require encryption markers.
Literal text that starts with `ENC:`, `ENZ:`, or `ENB:` receives encryption as user data.
The native journal remains for recovery. The local mutation capture router has no attached cloud engine.

## Ordinary API results

Every workflow uses a fresh owned synthetic home and the exact verified release.
Every start removes engine-selection, memory-mode, replay, and binary-content overrides.
The driver requires the native format marker from normal product startup.

| Workflow | Result |
|---|---|
| Hash and HashRange | Pass. Create, partial update, mixed changes, separate delete and recreate, range membership, concurrent complete values, flush, and reopen. |
| Conditional writes | Pass. The correct prior value permits a write. A stale prior value returns conflict and preserves the current values. |
| Repeated public writes | Pass. Repeated payloads receive fresh mutation identities and retain correct values. |
| Unsupported operations | Pass. Empty public batches, unsupported layouts and selectors, and historical query and count requests receive the required errors. |
| Literal encryption prefixes | Pass. Six records retain exact values and atom identities through first reads and reopen. First reads make zero file reads. |
| Warm unique purge | Pass. The logical keys disappear. Four selected unique atoms return 404 after ACK and after reopen. |
| Cloud activation | Pass. Cloud On returns the disabled error and leaves known intent files unchanged. |
| Protein | Pass. Shared values, alternate keys, member moves, independent deletion, and reopen. |
| Named databases | Pass. Scoped create, update, delete, independent personal values, and reopen. |

The first queued-create status precedes the first value read.
It reports accepted sequence 15, completed sequence 13, one accepted batch, and 512 dirty core paths.
The following value reads use memory. Some other writes correctly finish on disk before their sampled status.
The result does not claim that every queued request avoids a disk wait.

The logical workflow completes 156 successful requests and 13 expected refusals across six normal node sessions.
The protein workflow completes 30 successful requests and 14 keyed value checks across two normal node sessions.
The named-database workflow completes 43 successful requests and 24 keyed value checks across two normal node sessions.
Together, the workflows contain 124 keyed checks and 66 required zero-file checks. All ten node sessions stop normally.
The later error-message correction has a separate ordinary API receipt. The full workflows do not rerun for that static list change.
The [final error-response check](accepted-response-v2/acceptance.json) passes on the final release.
All seven unsupported selectors return errors. Three structured refusals name exactly the five supported selectors.
Its one native node session stops normally.
These are bounded product checks on generated data. They do not establish every caller or large-home behavior.

## Discovery receipts and limits

V5 stops when its JSON-only decoder receives the expected plain-text empty-batch error.
V6 accepts that error form, then stops when a new check compares an omitted range with a returned null range.
V7 uses the existing logical key comparison in its two new checks.
All value, count, file-read, and UUID gates stay unchanged.
Both failed discovery runs stay unchanged and separate from the accepted results.
The product source and executable stay unchanged between these attempts.
The first final-response wrapper stops before node startup because its receipt call uses the wrong argument shape.
The corrected wrapper passes with the same final product executable. The preparation failure stays separate and creates no product-result claim.

True cached duplicate identities and the internal empty transaction receive source review only.
The public route creates fresh mutation clocks and rejects empty public arrays before the core transaction.
Direct embedded helper entry, malformed key-wrap refusal, and old-format startup refusal also receive source review only.
The purge result proves logical and public UUID-addressable absence. It does not measure reclaimed disk bytes.
The cloud result checks the public refusal and local intent files. It does not include network packet capture.

The warm record cap remains 10,000. Separate allowances cover metadata, complete selected keys, and accepted write values.
Clean read values have a record cap rather than a separate global body-byte cap.
These controls do not define a whole-process memory maximum.

No new performance comparison forms part of this cleanup.
The [earlier matched benchmarks](../core-owned-writes-20261018/RESULTS.md) measure their earlier product source, rather than this cutover.
No tests, test suite, private fixture program, or coverage gate ran.
No primary lifecycle, installation, merge, or deployment action forms part of this result.

## Receipt retention and cleanup

The tracked acceptance summaries preserve the results and exact source, program, driver, and raw-receipt hashes.
Full request and response receipts stay local under this proof directory and remain outside Git under the existing policy.
The three receipt seals cover 357 files, including the two failed discovery runs and the pre-node preparation failure.
All nineteen node sessions stop normally.
Supported cleanup removes all nine exact owned temporary roots after owner, file, socket, process, and receipt checks.
The [cleanup result](cleanup-verification.json) and [independent final check](root-post-cleanup-check.json) confirm their absence and unchanged sealed receipts.
The [cleanup note](CLEANUP.md) records the scope.
