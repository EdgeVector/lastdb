# One field model for queries, writes, and related deletes

## Changes

| Review comment | Final change |
|---|---|
| Exact reads skip field conditions and limits. | Every exact and range read uses the common query route. |
| Range conditions use older field atoms than the returned record. | Remove the separate record-envelope value. Current field atoms supply both selection and results. |
| A current value retains metadata from an older atom. | Each field result retains its own atom and metadata. There is no later document substitution. |
| Delete chooses related keys before an update changes them. | Resolve current related keys inside the native write turn, then commit all affected schemas together. |

The final design uses the canonical field molecules and field atoms for every logical record.
The optional record-compaction API, CLI, document readers, document writers, record proteins, and RecordMapper configuration are removed.
Obsolete schema-level configuration and reserved internal field names receive explicit refusals.
Native file packing, batch reads, batch writes, field sharing, proteins, and the core warm set remain.

A cold full-record read fetches its field values through native batch reads.
A warm read uses the same logical field values in the core.
No second logical value format or conversion route remains.
Hash writes also supply an omitted declared key field from the lookup key in the same field batch.
Field conditions can therefore find a record whose caller supplies only the key and payload.

Both user Delete forms check the original caller's conditions before peer expansion.
Current field atoms supply peer keys and conditional-delete values.
Derived peers do not inherit the caller's expected-value or must-exist requirement.
Each affected schema retains its cleanup steps. Search receives the deletion after the full disk commit.
Only the original caller's mutation IDs return.
Conditions read the named field directly, even when Delete has no payload or Update does not change that field.

Fixed read, operation, and byte limits return an explicit refusal.
The native write scope retains a typed fixed-limit failure until it discards all private work.
This prevents an oversized peer deletion from an endless lane retry or a partial publication.
Temporary queue and memory pressure retains its existing retry behavior.

## Boundaries

Ordinary Delete retains its existing per-field clock order.
A lower Delete can remove older fields while a newer update survives on other fields.
The synthetic mixed cohort classifies the changed key field separately from full-record projections.
It preserves every outcome and does not repeat the cohort to obtain a preferred winner.

A mixed update and delete request still has separate phases.
An update can succeed before a later conditional delete receives a refusal.
The explicit Purge route retains its separate destructive rules.

Cloud activation stays disabled.
Before cloud returns, partial mixed requests and rejected deletes need capture selection for each accepted intent.
This change does not alter the existing capture marker or claim cloud replay correctness.
The primary database and its archive remain untouched. No migration code forms part of this change.

## Validation

The final product source is `7e457dabd819f1d56467e233148a5b13e98c081a`.
The normal release binary has SHA-256 `1b79500275f732f15b6405bd0d46d63fe853362790dcd76934cead37651b1c6c`.
`build-identity-cas.json` verifies all 1,878 product inputs against the committed source.
The workspace Clippy checks, the separate embedding-package Clippy checks, the release build, and the format check pass.
The final source gates cover file and function size, new lint exceptions, whitespace, write routes, app registration, and build artifacts.

| Ordinary product checks | Source | Result |
|---|---|---|
| Exact and range conditions, limits, offsets, field order, projections, metadata, and reopen | `c3ff5c91` | 54 query items pass. |
| Obsolete schema options, reserved field names, removed API, and omitted Hash key | `c3ff5c91` | Explicit refusals and current values pass, including reopen. |
| One fixed 16-record Hash protein cohort | `c3ff5c91` | 11 Deletes win; five Updates survive. Later Deletes remove all 144 known fields, including reopen. |
| Conditional Hash Update with its expected field outside the payload | `7e457dab` | Six query items pass, including reopen. |
| HashRange key move, reverse protein update, conditional Delete, and missing-target refusal | `7e457dab` | 107 query items pass, including reopen. |

Only the condition-field reader changes between those two product sources.
`cas-source-delta.json` verifies the other 1,877 product inputs remain unchanged.
The earlier query and fixed-cohort receipts retain their actual source labels.
The final release repeats both affected conditional operations with fresh synthetic data.
No cohort is repeated to obtain a preferred outcome.

The final HashRange case confirms the current source and peer share the same field atoms.
A stale expected value receives HTTP 409 and leaves the records unchanged.
The correct expected value with `must_exist` deletes the source and its current peer.
All 12 known source, old-peer, and new-peer fields remain absent after reopen. Unrelated records stay unchanged.
A second `must_exist` Delete receives an explicit HTTP 400 refusal.
The response reports the local commit and unavailable cloud capture separately; cloud remains disabled.

Repeated controlled warm reads perform zero file reads after the update and after reopen.
These small-data file counts are operational evidence, not a new throughput benchmark or a cold-disk speed claim.
`product-results.json` provides the source-labeled result index. `final-api-root-review.json` records the independent receipt review.
Both final synthetic homes exit normally and receive supported cleanup.
The independent ownership review checks the exact roots, process exits, sockets, and open files before cleanup.
All 67 final sealed files retain their original size and hash after cleanup.
All ten owned synthetic homes from this review are reclaimed.

The original request inputs remain in `operation-inputs.json`; each later procedure records its exact changes separately.
Git stores that canonical input once. Exact generated copies stay local and unchanged for receipt seals.
`input-deduplication.json` identifies those copies and their recovery bytes.
No tests, coverage commands, primary operations, or existing-home loads run.

## Preserved earlier results

Two early procedures stop with `complete=false` on source `8c26bd4`.
The first compares metadata before record compaction, outside its document metadata rule.
The second omits the declared hash field from its expected projection.
The established query route retains that field for record identity.
The corrected requests explicitly include it. Both original results stay unchanged in `diagnostic-seal.json`.
Supported cleanup removes those two stopped synthetic homes. All 66 sealed files retain their original size and hash.

Source `b50e6c1d` passes 56 document query checks and the Hash protein cohort.
That cohort contains eight winning Deletes and eight surviving Updates.
The later 16 Deletes pass 144 field-absence checks before reopen and 144 after reopen.
The original Hash protein procedure stops on a role-label error; its existing receipts receive separate offline validation and a same-home continuation.
It is not rerun to choose different mutation winners.

The compacted HashRange protein case on that source exposes an actual product failure.
The acknowledged key change removes the old peer but leaves the new peer absent.
A separate source review finds that repeated record compaction can restore older values.
Those results motivate removal of the second logical representation. That source is not accepted for merge.
All four result trees remain in `interim-seal.json`, including the failed case.
Supported cleanup removes their three stopped synthetic homes. All 133 sealed files retain their original size and hash.

Per-field source `c3ff5c91` passes 54 controlled query checks and the schema/field refusals.
The omitted Hash key checks pass after create, update, and reopen.
The fixed Hash cohort contains 11 winning Deletes and five surviving Updates.
Its shared atom identities and 144 field absences after the later Deletes pass, including normal reopen.
The HashRange key move and reverse peer update also pass.

That source then fails the correct conditional Delete: its condition reader reports a present field as absent.
The reader restores only payload fields, while Delete has no payload.
The final correction reads the named condition field through the existing current-field reader.
Only `cas_current_state.rs` changes between these product candidates.
The final release repeats the affected Hash and HashRange conditional operations on fresh synthetic data.
The 54 query checks and fixed Hash cohort retain their original source labels; they are not reported as new-release runs.

Four linked read/cutover procedures contain response-format assumptions that stop them with `complete=false`.
Their existing responses receive explicit offline checks and same-home continuations; no original outcome changes.
All six result trees remain in `per-field-attempt-seal.json`, including the actual conditional-delete failure.
Supported cleanup removes three stopped synthetic homes. All 206 sealed files retain their original size and hash.
