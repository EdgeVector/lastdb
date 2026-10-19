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

Both user Delete forms check the original caller's conditions before peer expansion.
Current field atoms supply peer keys and conditional-delete values.
Derived peers do not inherit the caller's expected-value or must-exist requirement.
Each affected schema retains its cleanup steps. Search receives the deletion after the full disk commit.
Only the original caller's mutation IDs return.

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

Final source, build, ordinary API checks, and cleanup results follow before acceptance.
The original request inputs remain in `operation-inputs.json`; each later procedure records its exact changes separately.
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
