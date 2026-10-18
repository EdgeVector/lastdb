# Write routes and operational evidence

Status: candidate source work continues. This matrix separates the source design from measured product results.
The final source review must attach exact paths and functions after the release freeze.
No result in this file establishes candidate completion yet.

| Route | Source boundary and required state | Ordinary API evidence | Current gap |
|---|---|---|---|
| Logical create and update | `MutationManager` prepares complete immutable atoms, document envelopes, and molecule headers. The accepted barrier installs typed core effects. | `cohorts.py`: Hash and HashRange creates, partial updates, complete replacements, no prior value read, queued and durable modes. | Candidate run pending. Reference passes complete values; first-value file costs remain observations. |
| Logical batch | The same mutation path publishes one permitted accepted state. Erasures follow creates and updates under the existing batch entry. | Independent creates, updates, and deletes in one batch. Same-key delete/recreate uses separate acks. | v1 input-order assumption fails and stays preserved. It does not establish a promised input-order defect. |
| Delete and purge | Logical erasures publish confirmed absence. Storage-slot purge translates only source-linked paths that the operation knows. | Logical delete, separate recreate, old completion repeated absence and membership churn. | Candidate run pending. No arbitrary raw-key or private-helper trigger is added. |
| Protein shared fields | Related field and record molecules receive the complete accepted change, including alternate keys omitted from a partial update. | `protein.py`: two valid local schema declarations, bootstrap records, source-only update, alternate-key move, delete, reopen. | Reference partial update returns stale sibling values. Candidate must satisfy the unchanged gate. |
| Named database | The actual scope and share copies receive their own typed paths. Personal state remains independent. | `scoped.py`: local membership declarations, scoped create/update/delete/recreate, personal values, reopen. | Reference scoped create acks but first scoped read is absent. Candidate runs this unchanged workflow first. |
| Public Atom UUID read | The scalar Atom wrapper establishes the native captured read scope through every fallback. | `atom_supplement.py`: one value query obtains UUID metadata, then eight public Atom requests run in parallel with exact content checks. Native status brackets the calls before explicit flush. | Candidate pending. UUID discovery precedes the direct fetch. The mutation reply contains no Atom UUID. |
| Personal share fanout | Actual share prefixes receive the same accepted typed logical result. | Source inventory and the named-database workflow cover different boundaries. | No independent ordinary personal-share enrollment cohort exists yet. Do not claim full fanout runtime proof. |
| Schema definitions and state | One outer native scope publishes the exact typed schema snapshot for supported create/state/successor/drop routes. | Prior completion schema lifecycle and molecule-map repair operations run unchanged before new cohorts. | Candidate run pending. Exact source inventory must separate schema controls from value mutations. |
| Raw administration and replay | Operations that cannot translate into typed logical effects stay private until disk commit. Commit publishes invalidation and complete-key changes before success. | Known local mutation-log export/replay APIs, source inventory, prior mapping controls where applicable. | Native replay cannot boot through its existing backup/cloud activation gate. No bypass or alternate store is used. |
| Import into inactive store | The importer targets a separate inactive store. It does not promise active core publication. | Source inventory only unless a supported ordinary command provides a safe trigger. | No active write-through claim. No new fixture harness. |
| Retention, repair, and backfill | Each active raw route must reach the common disk-first commit and invalidation boundary. | Existing public operation where a known safe trigger exists. | Final exact source inventory and any remaining API gap must stay explicit. |
| Concurrent reads | A call captures typed core state and the disk root. An older call retains its charged owners until release. | Four bounded readers, complete revision batches, flush, complete values after ack. | Ordinary calls do not pause at a selected internal capture instruction. Source review remains necessary. |

## Source boundaries at the first candidate freeze

All paths below use the `fold_db/crates/core/src/` prefix. These facts describe source review, not runtime results.

| Purpose | Path and function |
|---|---|
| Native mutation selection | `fold_db_core/mutation_manager/write/create_update_batch.rs::commit_new_mutations` selects native before the legacy publication gate. |
| Mutation commit | `fold_db_core/mutation_manager/write/native.rs::{commit_native_request,commit_native_deltas}`. |
| Scope and share publication | `fold_db_core/mutation_manager/write/native_paths.rs::{stage_native_paths,stage_native_scope}`. |
| Positive and erased paths | `fold_db_core/mutation_manager/write/native_paths/{positive,removed}.rs` stages final tips, headers, and confirmed absence. |
| Deferred durable lane | `fold_db_core/mutation_manager/molecules/persist/deferred_job.rs::run_deferred_job` uses one native transaction scope. |
| Purge lane | `fold_db_core/mutation_manager/molecules/persist/purge_job.rs::run_purge_job` uses one native transaction scope. |
| Storage-slot purge lane | `fold_db_core/mutation_manager/molecules/persist/storage_slot_job.rs::run_storage_slot_purge_job` uses one native transaction scope. |
| Schema controls | `db_operations/schema_store/write.rs::{store_schema,store_schema_state,store_superseded_by,drop_schema}` uses one outer native scope for each operation. |
| Exact schema state | `db_operations/schema_store/native_memory.rs::stage_native_snapshot` stages schema, state, and successor. |
| UUID-only body access | `db_operations/atom_store/atoms/locator_memory.rs::stage_atom_locators` supports the same-owner scalar and batch typed reads. |
| Physical body certificate | `storage/disk_pack/logical_paths/source/certified.rs::certified_atoms` checks UUID, complete Atom equality, exact scope, and the actual primary collection. |
| Generic raw writes | `storage/disk_pack/kv.rs` change, compare-and-swap, delete, and batch operations converge at the native transaction and queue boundary. |
| Raw completeness decision | `storage/disk_pack/logical_paths/completeness.rs` forces disk-first behavior for unsupported typed coverage. Unknown main families and namespaces default to that route. |

Native create/update skips legacy graph Atom and tip publication. Native purge skips legacy graph tombstones.
`storage/disk_pack/read.rs::ReadContext::capture` captures no flush-log entries for an ordinary read.
Only serialized private write preparation can inspect those existing `PendingBatch` entries.
Legacy backend compatibility retains `ResidentGraph` code. This change removes its native value-serving and publication role, not every symbol.
The protein key-move runtime gate stays unchanged even if source review identifies another existing semantic gap.

The remaining system routes use a disk-first boundary. Their source review does not establish their runtime behavior.

| System route | Source entry and contract |
|---|---|
| Raw replay | `sync/engine/replay/apply.rs::{replay_put,replay_structured_put}`, `records.rs::{replay_per_key_record,replay_header_record}`, and `entry/ops.rs::{replay_put_op,replay_delete_op,replay_batch_put_op}` call the common raw store boundary. |
| Raw replay batch | Replay writes each item separately. It does not promise one atomic replay entry. |
| Logical mutation replay | `MutationIntent` replay returns through the normal typed `MutationManager` writer. |
| Inactive org import | `sync/cloud_purge_peer/scope.rs::import_org_into_inactive_store`, `inbound_share/data.rs`, and `db_operations/molecule_key_store/cloud_purge_import.rs` target an inactive store. |
| Restore | `sync/cloud_purge/restore.rs` follows the separate restore contract. |
| Molecule generation repair | `db_operations/atom_store/molecules/generation/activate.rs::activate_molecule_generation` changes active raw state through the common boundary. |
| Dangling-tip repair | `db_operations/admin_db/dangling_tips/repair_group.rs` uses the raw store boundary. |
| Retention metadata | `db_operations/schema_store/retention_layers.rs::apply_manifest_retention` and `retention_move.rs::realign_retention_anchor` change raw policy and tracking. |
| Protein member and backfill controls | `protein/ops.rs::{protein_add_member,protein_backfill_member_tips,protein_set_member_tip}` and its children use the raw store boundary rather than an early typed mutation ack. |
| Namespace removal | `storage/disk_pack/namespace_delete.rs::delete_namespace` uses the native commit boundary. |
| Physical compaction | `storage/disk_pack/maintenance.rs::maintain_native` replaces the committed root under the same captured-read lifetime rules. |

Runtime proof for raw replay stays blocked at native activation. Inactive import does not become an active write-through claim.
Retention, repair, direct member controls, namespace removal, and physical compaction receive source review in this workload.
No private API or raw-key fixture replaces a missing safe public trigger.

The final fence deliberately commits these controls to disk before success:

- Unknown main families; protein, molecule-protein, field-protein, and protein work controls.
- Conflict, molecule conflict, and history controls; generation and reference controls.
- Schema, state, successor, molecule-key definition, database membership, and share-binding controls.

Commit invalidates the exact current-core dependencies and complete-key state.
Known typed schema, header, and absence effects can then enter the clean core state.
A zero-file first value read after one of these controls does not imply an early memory ack.
Only audited idempotency, schema-index, small retained side records, and main update-order/liveness bookkeeping accompany early typed value publication.

## Repeated partial protein moves

The separately frozen `protein_move_supplement.py` adds five bucket-only member moves: A→B→C→A→B→A.
Two source IDs share identical title and body content. Only one source bucket changes.
Each ack precedes one batched read of both source IDs and every known old and new member key.
The gate requires complete values, old-key absence, unchanged shared content UUIDs, and the unrelated record at A.
A normal reopen follows the first return to A. Two more moves then reuse the earlier content.
Public query metadata exposes UUIDs, not the internal move and content clocks. The report must retain that observation limit.
The gate follows `docs/lastdb-canonical-model.md:132-133`; it does not change the main v5 workflow.

## Memory observations

The v3 reference pressure run uses 64 records with 32 KiB bodies and a 16 MiB write allowance.
The raw-plus-typed quote can exceed that allowance. The route can therefore choose disk-first behavior.
All value, reported-cap, drain, and recovery checks pass. Published charge samples are zero.
Those samples do not establish a zero charge or captured-owner behavior.

The separately frozen v4 pressure cohort uses 16 records with the same body size and allowance.
The write quote uses four times the staged raw bytes plus twice the typed-path and absence bytes.
The smaller batch targets a queued ack within the allowance; actual reply modes remain part of the receipts.
Candidate acceptance requires a queued overwrite and a positive direct body reservation observation.

The native `core_model` object supplies these direct counters:

| Counter | Meaning |
|---|---|
| `metadata_current_bytes` | Metadata owned by the current core state. |
| `metadata_used_bytes` | All live charged metadata, including earlier or privately prepared versions. |
| `metadata_retained_bytes` | The live metadata charge outside the current state. |
| `metadata_peak_bytes` | The recorded peak live metadata charge. |
| `body_reserved_current_bytes` | Distinct reservation owners retained by current core slots, including clean slots after flush. |
| `body_reserved_used_bytes` | Distinct live body reservation owners tracked by weak references. |
| `body_reserved_retained_bytes` | Live owners outside the current slots, including earlier reads or private preparation. |
| `body_reserved_peak_bytes` | The recorded peak tracked body reservation bytes. |

One whole-request owner counts once. Weak observation does not keep its bytes alive.
The direct counters can prove a positive charge when the published `/api/status` sample misses a short interval.
The retained difference can include private preparation. Client-call overlap does not identify one exact internal read snapshot.
The final report must retain that limit even when it observes a positive retained charge.

## Replay limit

The guarded source exits before health with the native backup/cloud gate error.
No owner provision, API export, or replay occurs in the reference run.
Its source log and exit code remain in `before-replay-v3`.
The source inventory can review the common disk-first boundary, but it cannot replace runtime replay proof.
The final result must state this operational limit unless the supported product gate changes within the authorized design.
