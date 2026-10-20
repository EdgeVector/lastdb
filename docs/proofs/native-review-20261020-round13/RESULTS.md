# Native memory and disk review — 20 October 2026

The review fixes confirmed problems and removes unused state and duplicate work.
The accepted core model and native packed-file engine remain the current design.
No migration or primary database operation runs.

## Changes

| Area | Earlier problem | Current rule |
|---|---|---|
| Read admission | A second atom body fills the 4 MiB source-proof allowance and prevents large-value reuse. | A compact certificate covers every atom field and its exact source. Admission transfers the decoded atom after verification. |
| Schema key pages | Another mapped key layout consumes the limit before membership checks. | Both coordinates pass logical membership before the page limit. Rejected candidates cause bounded refill. |
| Write IDs | Mixed batches omit Delete IDs. Change notices take another input's ID. | Every successful batch returns one accepted input ID per caller input, in caller order. Notices use that exact association. |
| Catalog changes | Decisions and cleanup use separate boundaries and opposite lock orders. | The native write boundary precedes molecule locks and protects the complete catalog decision and data change. |
| Catalog Search removal | Physical coordinates escape as Search keys. | The schema-facing reader supplies logical keys. Search receives changes after commit. |
| Encryption wraps | One instance's last-reference decision removes a node-global wrap. | Instance cleanup retains shared molecule wraps and reports zero reclaimed wraps. |
| Identity format | Delimiter joins and UUID input boundaries are ambiguous. | Source bindings use explicit tuples. Molecule UUID input uses framed fields. No old-format fallback runs. |
| Cutover fence | An earlier initialized logical format can reach the changed identity parser. | One shared v3 logical marker controls creation, open, and snapshot checks. Earlier initialized formats receive the existing fresh-home refusal. |
| Unused state | HashRange molecules retain a write-only list of replaced tips. | The list and its helpers are absent. Current conflict handling remains. |
| File cleanup | A separate 4,096-generation cutoff rejects valid disk homes. | Inventory uses the existing bounded home file allowance. Closure and exact-file checks remain. |

A receipt ID identifies an accepted input, including an idempotent repeat or an absent-key Delete.
It does not identify the atom that wins a field conflict.
The host reuses the complete list for cloud-capture metadata instead of another input-ID copy.
The write decision, author clock, flush order, and durable acknowledgement rules stay intact.

## Release identities

| Release | Source commit | Binary SHA256 |
|---|---|---|
| Initial reviewed release | `912a74e847ff32ff2e4d585ed10ef2f7e39338a4` | `ff0df66caa3c2ef504b083a159b7aedeb011ac58d58a160822abd673ecec9f1e` |
| Receipt-order release | `9f2bb284a6a47a69a58e16eb9ac4d468a44f4d77` | `c3db0790219b0a3a029610ee99d7bdb24b44a6894f208735675ab917183f4f5a` |
| Final format-fenced release | `fdb13f0667f92226048a51884ac038753fe21b63` | `a27051ad056539361d9255247d41fa0309511ca1d54bcbcecd72368a8c9abd19` |

The initial release runs eight fresh synthetic cases. The receipt-order release fixes the mixed-batch ID omission and repeats the core cohorts.
The final release advances the logical format marker to `layout:v3:keys` with value `3`.
It repeats the core, catalog, plain-range, and packed large-value cases on four new homes.
The physical node format remains unchanged. No migration, conversion, or earlier-format read path is present.
Each source inventory records 1,814 source and build inputs, including `folddb_profile/environments.json`.
Source and executable bytes remain fixed throughout each operational window.
Each private executable matches its release before and after operations.
[The source ledger](release-source-ledger.json) reconstructs complete inventories from a committed base and exact deltas.
Compiler-only attempts remain distinct from releases that run a node.

Both final Clippy lanes and the final release build pass.
The host Cargo lock serializes each build and node window.
Actual lock acquisition uses 3,600 seconds; the unchanged identity helper retains its historical 600-second command template.
[The final build receipt](format-fenced-release/build-checks.json) records exact child commands and exit codes.

## Product results

All 13 runs use fresh, owned synthetic homes and ordinary owner API calls.
The nodes use normal release defaults without engine-selection flags.
[The existing helper hashes](operation-helper-identity.json) remain unchanged.

The initial eight runs record 29 normal node sessions and 158 focused HTTP calls.
Those calls contain 294 responses after batch expansion.
The separate core cohorts exercise 620 mutations, 74 read observations, 386 query responses, and 3,363 returned records.
Their field checks inspect 13,452 metadata maps. All 29 required zero-file checks pass.

The final release core case records 129 HTTP calls, 627 mutations, 390 query responses, and 3,365 records.
It checks 13,456 field metadata maps and again passes all 29 required zero-file checks.
Concurrent reads retain complete earlier values; reads after acknowledgement see complete new values.
Seven flushes and six queue-drain observations pass. All four core sessions stop normally.
The other final cases add 36 catalog, 23 plain-range, and 13 large-value HTTP calls.
All 16 final-release sessions stop normally.

The mixed core batch now returns eight IDs for eight accepted inputs.
Eleven additional calls verify mixed Delete/Update/Create IDs, a single Update ID, an absent-key Delete ID, and their exact change notices.
The calls follow actual returned cursors, including an empty target page before later matching events.
Final keyed reads confirm the deleted keys are absent and other keys contain complete current values.
These notices appear asynchronously; acknowledgement alone does not prove immediate notice availability.

Mapped HashRange reads, counts, and key pages agree in OPE and plain form through key moves, Delete, flush, and reopen.
The initial supplements verify six actual cursor continuations. The final release verifies four more.
The first two collectors omit continuation because the page report is inside the `list` member.
[The coverage record](execution-coverage.json) preserves that limitation and identifies the corrective runs.

The catalog case retains shared data after its first alias disappears.
Last-alias cleanup reports `removed_rows: 20`, one Search tombstone, and zero reclaimed wraps.
After reopen, a new binding at the same locator cannot read the old empty-range key.
A second empty cleanup removes no data. Personal-scope values remain unchanged.
This proves tombstone construction, not delivery to an external Search service.

## Memory reuse and file packing

Each large-value case contains 32 distinct 204,800-byte values, or 6.25 MiB of value bytes.
Each value repeats a generated 1,024-byte chunk 200 times. This checks admission size, not compression throughput.
Cold admission retains 128 complete field paths with a 7,571,504-byte warm charge.
Source certificates retain metadata instead of a second value body under their shared 4 MiB allowance.

A queued replacement reports 128 dirty paths, 17,229,104 body-reservation bytes, and 2,447,224 model-metadata bytes.
Final flush releases all held, dirty, and body-reservation charges.
These counters do not define or measure a whole-process memory cap.

Repeated large-value reads use zero file opens and zero file-read operations.
Packing reduces the observed file count from 224 to 43.
Every updated value and its returned metadata remain correct after pack, reclaim, and normal reopen.
The first subsequent read uses five file opens and 158 file-read operations. Its repeat uses zero of both.

The final large-value case records 120.159 ms cold and 77.841 ms repeated for the 32 values.
After pack and reopen, it records 165.671 ms cold and 102.492 ms repeated.
These are single request observations. Host load varies, and the operating system cache is uncontrolled.
No old-engine speed comparison or maximum write-throughput claim follows from them.

## Limits

- Catalog cleanup must fit the native operation limit and its 128 MiB planning and preparation allowances. Oversize cleanup receives an atomic refusal.
- These allowances do not bound the complete process. Read pages, private prepared changes, and other model allowances also use memory.
- Cloud remains disabled. Domain-aware wrap revocation needs a separate design before cloud activation.
- Cached/new idempotent input combinations and Purge receipt IDs receive source review only. The owner API cannot submit explicit UUIDs and clocks.
- Separator-bearing public-library names, homes above 4,096 generations, and older-format refusal receive source review only. No old home is loaded.
- These runs do not prove a 20 GB database, an M5 machine, whole-process memory peaks, or maximum write throughput.
- No tests, new test harness, migration, primary-home copy, archive access, or shared-node change runs.

## Evidence and cleanup

[The archive manifest](receipt-archives.json) preserves exact full request/response receipts in ordered gzip parts.
Concatenate each listed part as bytes, then decompress. Check the recorded source and archive hashes.
Large HTTP bodies also retain exact gzip copies beside their metadata records.

Format, size, writer-boundary, dependency-lane, artifact, and secret checks pass.
[The local gate receipt](format-fenced-release/non-test-checks.json) records the checks.
[The final review record](final-review.json) covers independent memory, query, and write scopes.

The first nine homes account for 33 normal sessions and 553 unchanged sealed original files.
The final four homes add 16 normal sessions, for 49 in total.
Supported exact-home cleanup removes all 13 homes after independent ownership and process checks.
All 827 sealed originals remain unchanged. No recorded owner process remains.
Execution receipts cover [the initial homes](cleanup-before-receipt-order-execute.json), [the receipt-order home](cleanup-final-receipt-order-execute.json), and [the format-fenced homes](cleanup-format-fenced-release-execute.json).
