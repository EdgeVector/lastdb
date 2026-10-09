//! `MutationType::Purge` dispatch.
//!
//! Purge is the compliance/GDPR verb — it irreversibly removes a
//! `(schema, key)` record: the molecule entries for the key, its `tv:`
//! tip-version chain, the atoms only that key referenced (live head AND
//! superseded), the legacy `history:` rows that mention the key's
//! field_key, and the record's resident (T0) tips and atom bodies.
//! `MutationType::Delete` peels through this same module (slice A of
//! north-star-lastdb-delete-returns-the-bytes) so a user delete is not N
//! per-field tombstone atoms, but **live** `Delete` (missing-target policy
//! `Skip`, i.e. `HardEraseVerb::Delete`) no longer reaches
//! [`purge_records_bulk`] itself: [`converge_delete_tips`] persists tip
//! absence only — molecule entries and the `tv:` chain — and leaves atom
//! reclaim to a later janitor pass (see
//! `design-lastdb-delete-converge-then-reclaim`). `must_exist` delete
//! (`HardEraseVerb::DeleteMustExist`) and compliance `Purge` are unchanged:
//! both still run the full atom-reachability erasure below, and the
//! distinction that remains between them is **missing-target policy**:
//! compliance `Purge` is still a loud error when the target is absent;
//! `must_exist` delete is the same loud contract worded for the caller's
//! own verb. There is nothing left for the admin restore endpoint to undo
//! after either.
//!
//! The historical reach is planned from the **tip-version chain**, not
//! from `history:`. Those rows' only writer is test-only code — the
//! production write path retired them when the `tv:` chain replaced them
//! — so a plan built from them collapsed to the live head, and every
//! superseded value of a purged key survived while
//! `BulkPurgeReport::history_rows_deleted` reported 0 because nothing had
//! run. The `history:` scan is still here for legacy stores that do carry
//! those rows; on a current store it contributes nothing. Card
//! `lastdb-purge-never-prunes-the-tip-version-chain`.
//!
//! **Thin-tip limit of that reach:** minting `tv:` rows is policy-gated
//! (`MutationManager::set_tip_history_enabled_for_writes`) and the settled
//! Mini default is OFF — ordinary updates replace the head and archive
//! nothing. On such a store an updated-then-purged key has no chain to
//! walk, `tip_versions_pruned` is honestly 0, and the superseded body
//! atoms survive this verb as unreferenced residue. That is by design,
//! not a planner gap: the erasure contract is completed by the layered
//! sweep — `gc-atoms` reaps the unreferenced bodies, then `gc-file-blobs`
//! reaps blobs nothing references. Pinned end-to-end (residue, reap, blob
//! layering) in `atom_delete_ledger_test.rs`; brain
//! `papercut-purge-update-superseded-atom-survives-no-tv-rows-written`.
//!
//! Deleting a chain node and deleting the atom it names are separate
//! decisions, and conflating them is how this path breaks a sibling. A
//! `tv:` row is private to one slot, so the target's rows always go. The
//! atoms they name are content-addressed by `(schema, content)` and can
//! be shared, so each is checked against BOTH guards — live heads
//! (`collect_live_atom_uuids`) and retained keys' chains
//! (`collect_retained_chain_atoms`) — before it is hard-deleted.
//!
//! File blobs: purge destroys the pointer (and the DEK it carries), counts
//! the destroyed pointers into the delete-ledger row
//! (`file_pointer_atoms_purged`), and drops the resident-cache copies of the
//! refs those atoms named. It does NOT delete the durable blob rows: a
//! blob_ref is a hash of the file's plaintext, shared freely across records
//! and schemas, and proving "no other referrer" needs the reachability scan
//! only an admin sweep may run. The reclaim path for the sealed bytes is
//! `db_operations::file_blob_gc::gc_orphan_file_blobs` (`gc-file-blobs`).
//!
//! Erasure must reach every tier that can answer a read, not just
//! storage. See the resident-eviction block in [`purge_records_bulk`]
//! for the defect that taught us this and why an eviction API is not an
//! erasure API.
//!
//! Idempotency note: **compliance** purge of a target that doesn't exist
//! (already purged, or never written) is a *loud* error, not a silent
//! no-op — audit needs to know the target wasn't found. The **delete**
//! caller of this same path uses [`PurgeMissingPolicy::Skip`]: missing
//! targets are filtered out and a fully-missing batch is a successful
//! no-op.
//!
//! That contract is about the **request**, and the policy is chosen by the
//! caller's origin, not by the verb alone. A purge arriving from mutation-log
//! **replay** is `Skip`: it is a restatement of an act that already happened,
//! there is no requester to inform, and on the authoring node the local apply
//! is precisely what made the targets absent. See `WriteOrigin` in
//! `mutation_manager::write` for what a `Refuse` there costs — a pinned sync
//! cursor and a stopped backup queue, not a warning.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use crate::db_operations::DbOperations;
use crate::schema::types::field::build_storage_key;
use crate::schema::types::key_value::KeyValue;
use crate::schema::{SchemaCore, SchemaError};

mod bulk;
mod converge;
mod delete_barrier_flush;
mod guarded;
mod helpers;
mod storage_slot;
mod storage_slot_resident;

pub(super) use bulk::{
    plan_guarded_complement_retained, purge_records_bulk, validate_purge_targets_present,
};
pub(super) use converge::converge_delete_tips;
use guarded::execute_guarded_purge;

pub use storage_slot::*;
pub(in crate::fold_db_core) use storage_slot_resident::resident_api_keys_for_storage_slots;

// Re-export helpers so production paths and unit tests share one surface.
pub(super) use helpers::{
    atom_ref_cutover_ready, collect_event_atom_uuids, collect_live_atom_uuids,
    collect_resident_live_atom_uuids, collect_retained_chain_atoms_for_storage_slots,
    collect_target_chain, collect_target_tip_chain, current_atom_for_key, describe_key,
    field_key_matches_value, refresh_runtime_field_molecules,
    refresh_runtime_field_molecules_for_purge_keys, remove_key_from_field, storage_form_key,
    storage_form_keys, validate_purge_key_shape,
};

type OwnedMoleculeKeyRemoval = (
    String,
    crate::db_operations::MoleculeData,
    Vec<(String, String)>,
    Option<String>,
);

/// Resolve every affected molecule key from the schema and the signed source
/// mutation. This includes a key whose local tip is already absent.
pub(crate) fn plan_normal_delete_barriers(
    db_ops: &Arc<DbOperations>,
    schema: &crate::schema::types::Schema,
    mutations: &[crate::schema::types::Mutation],
) -> Result<Vec<crate::atom::delete_barrier::DeleteBarrier>, SchemaError> {
    use crate::atom::delete_barrier::{DeleteBarrier, DeleteKind};
    let mut by_key: HashMap<String, DeleteBarrier> = HashMap::new();
    for mutation in mutations {
        let written_at = mutation.imported_written_at.ok_or_else(|| {
            SchemaError::InvalidData("normal Delete lacks the original device write time".into())
        })?;
        let device_id = if mutation.author_clock_writer_id.is_empty() {
            mutation.pub_key.clone()
        } else {
            mutation.author_clock_writer_id.clone()
        };
        for field in schema.runtime_fields.values() {
            let Some(molecule_uuid) = field.common().molecule_uuid() else {
                continue;
            };
            let storage_key = helpers::storage_form_key(db_ops, field, &mutation.key_value)?;
            let Some((hash, range)) = field.disk_slot_for_key(&storage_key) else {
                continue;
            };
            let mk_key = build_storage_key(
                field.common().storage_prefix(),
                &crate::atom::molecule_key_codec::hash_range_record_key(
                    molecule_uuid,
                    &hash,
                    &range,
                ),
            );
            let barrier = DeleteBarrier {
                mk_key: mk_key.clone(),
                written_at,
                logical_counter: mutation.logical_counter,
                device_id: device_id.clone(),
                mutation_uuid: mutation.uuid.clone(),
                kind: DeleteKind::Normal,
                displaced_atom_uuid: None,
                cloud_sequence: None,
            };
            match by_key.entry(mk_key) {
                std::collections::hash_map::Entry::Vacant(slot) => {
                    slot.insert(barrier);
                }
                std::collections::hash_map::Entry::Occupied(mut slot) => {
                    if barrier.is_newer_than(slot.get()) {
                        slot.insert(barrier);
                    }
                }
            }
        }
    }
    let mut barriers: Vec<_> = by_key.into_values().collect();
    barriers.sort_unstable_by(|a, b| a.mk_key.cmp(&b.mk_key));
    Ok(barriers)
}

/// What to do when a key in the batch has no molecule / history / tip-version
/// trace at all.
///
/// Label for one peel of [`purge_records_bulk`]: ledger `verb` and miss copy.
/// Not a third [`PurgeMissingPolicy`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum HardEraseVerb {
    /// Wire `purge`. Ledger + CDC `"purge"`.
    Purge,
    /// Wire `delete` without must_exist. Ledger + CDC `"delete"`.
    Delete,
    /// Wire `delete` + must_exist. Ledger + CDC `"delete"`.
    DeleteMustExist,
}

impl HardEraseVerb {
    pub(super) fn ledger_verb(self) -> &'static str {
        match self {
            Self::Purge => crate::db_operations::LEDGER_VERB_PURGE,
            Self::Delete | Self::DeleteMustExist => crate::db_operations::LEDGER_VERB_DELETE,
        }
    }

    fn miss_noun(self) -> &'static str {
        match self {
            Self::Purge => "Purge",
            Self::Delete | Self::DeleteMustExist => "Delete",
        }
    }

    fn miss_suffix(self) -> &'static str {
        match self {
            Self::Purge => "(compliance verb)",
            Self::DeleteMustExist => "(must_exist)",
            // Skip path never emits a miss string. Distinct from must_exist
            // so a future mis-route cannot claim the loud-delete wording.
            Self::Delete => "(idempotent delete)",
        }
    }
}

/// Compliance `Purge` keeps [`Self::Refuse`] so operators learn a target was
/// already gone. User-facing `Delete` uses [`Self::Skip`] so re-delete is a
/// safe no-op.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum PurgeMissingPolicy {
    /// Loud `SchemaError::InvalidData` before any mutation (compliance).
    Refuse,
    /// Drop untraced keys from the batch; empty remainder → success no-op.
    Skip,
}

/// Per-batch report for [`purge_records_bulk`]. Counts are for the whole batch;
/// `records_purged` is what lets a caller log one line instead of N.
pub(super) struct BulkPurgeReport {
    pub records_purged: usize,
    /// Number of legacy `history:` rows deleted (across all fields of the
    /// schema). Zero on any store written by the current write path, which
    /// retired those rows — [`Self::tip_versions_pruned`] is where a purge's
    /// historical reach shows up now.
    pub history_rows_deleted: usize,
    /// Number of `tv:` tip-version chain nodes deleted — the purged keys'
    /// superseded values.
    pub tip_versions_pruned: usize,
    /// Number of `atom:` rows deleted (after refcount-safe filtering).
    pub atom_rows_deleted: usize,
    /// Number of embedding rows deleted (live + graveyard).
    pub embedding_rows_deleted: usize,
    /// Exact molecule slots addressed by this purge pass.
    pub target_slots: u64,
    /// Distinct atom bodies whose reverse-edge partitions were candidates.
    pub candidate_atoms: u64,
    /// Candidate atom partitions read from `AtomRefEdges`.
    pub reverse_edge_reads: u64,
}

impl BulkPurgeReport {
    fn empty() -> Self {
        Self {
            records_purged: 0,
            history_rows_deleted: 0,
            tip_versions_pruned: 0,
            atom_rows_deleted: 0,
            embedding_rows_deleted: 0,
            target_slots: 0,
            candidate_atoms: 0,
            reverse_edge_reads: 0,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum PurgeReachability {
    GuardedComplement,
    AtomRefEdges,
}

/// Counts exclusive guarded-purge barrier acquisitions for each schema.
///
/// Always compiled (see the sibling counter in
/// [`crate::atom::legacy_history_memo`]). This is the quantity the zero-yield
/// fix is defined against. A test that measures how long a purge took depends
/// on a machine. A barrier-acquisition count tests the selected purge path.
///
/// Keyed by schema because the barrier is per-schema: a global total cannot
/// identify which schema used the guarded purge path.
fn barrier_acquisitions() -> &'static std::sync::Mutex<std::collections::HashMap<String, u64>> {
    static COUNTS: std::sync::OnceLock<std::sync::Mutex<std::collections::HashMap<String, u64>>> =
        std::sync::OnceLock::new();
    COUNTS.get_or_init(|| std::sync::Mutex::new(std::collections::HashMap::new()))
}

/// Exclusive purge-barrier acquisitions on `schema_name` since process start.
pub fn barrier_write_acquisitions(schema_name: &str) -> u64 {
    barrier_acquisitions()
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .get(schema_name)
        .copied()
        .unwrap_or(0)
}

/// Called by the caller that owns the barrier, immediately after acquiring it.
pub(crate) fn record_barrier_write_acquisition(schema_name: &str) {
    *barrier_acquisitions()
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .entry(schema_name.to_string())
        .or_insert(0) += 1;
}

/// Named sub-step time recorded inside one destructive purge, so its caller
/// can report [`RequestPhase::PurgeCommit`] as the RESIDUAL of the exclusive
/// hold instead of as a total that double-counts its own parts.
///
/// Every phase surface in this node treats its buckets as DISJOINT —
/// `PhaseTimings::total_us` sums them all and `unphased_us` subtracts that sum
/// from the request's wall clock — so a sub-phase added without narrowing its
/// parent does not add detail, it makes every purging request read `over=`.
/// `apply` and `persist` are already residuals for this reason.
///
/// The accumulator exists rather than a bare `add_phase` at each site because
/// the two writes — report the phase, and remember it so the parent can
/// subtract it — must not be separable. [`Self::record`] does both or neither.
#[derive(Debug, Default, Clone, Copy)]
pub(super) struct PurgeCommitAccounting {
    named_us: u64,
}

impl PurgeCommitAccounting {
    /// Report `elapsed` under `phase` and remember it as part of the hold.
    fn record(&mut self, phase: crate::request_phases::RequestPhase, elapsed: std::time::Duration) {
        crate::request_phases::add_phase(phase, elapsed);
        self.named_us = self
            .named_us
            .saturating_add(u64::try_from(elapsed.as_micros()).unwrap_or(u64::MAX));
    }

    /// Time already attributed to a named sub-step of this hold.
    ///
    /// Saturating on the caller's side: the sub-steps are measured inside the
    /// hold and cannot legitimately exceed it, but clock coarseness must not be
    /// able to underflow the residual.
    pub(super) fn named(self) -> std::time::Duration {
        std::time::Duration::from_micros(self.named_us)
    }
}

/// Everything the planner learns about a batch before it destroys anything.
///
/// Used by compliance erasure and its retained-atom plan. Live `Delete` uses
/// only exact current tips and their archived chains in [`converge_delete_tips`];
/// it must not read this trace's legacy field history or atom candidates.
pub(super) struct PurgeTrace {
    /// Atoms any target referenced. A set because content-addressing means two
    /// targets can name the same atom.
    candidate_atoms: HashSet<String>,
    history_keys: Vec<String>,
    tip_version_keys: Vec<String>,
    atom_ref_edge_keys: Vec<String>,
    atom_ref_v2_edge_keys: Vec<String>,
    history_referenced_by_retained: HashSet<String>,
    /// Indices into `keys` that left any trace. Tracked per key so the
    /// not-found error can name the offenders rather than just failing the
    /// batch.
    traced: HashSet<usize>,
}

/// Result of the read-only presence check for a loud hard erasure.
///
/// Storage failures stay in the outer `Result` so the persist lane retries
/// them. A missing target is a successful read with a terminal caller error.
pub(super) enum PurgeTargetPresence {
    Present,
    Missing(SchemaError),
}

/// Walk a list of mutations and peel hard-erasure verbs out of the batch
/// before the regular Create/Update write path runs.
///
/// Returns `(purges, deletes, rest)`. Both purge and delete execute
/// [`purge_records_bulk`]; they differ only in [`PurgeMissingPolicy`]
/// (compliance loud-fail vs delete idempotent skip).
pub(super) fn split_off_hard_erasures(
    mutations: Vec<crate::schema::types::Mutation>,
) -> (
    Vec<crate::schema::types::Mutation>,
    Vec<crate::schema::types::Mutation>,
    Vec<crate::schema::types::Mutation>,
) {
    let mut purges = Vec::new();
    let mut deletes = Vec::new();
    let mut rest = Vec::new();
    for m in mutations {
        match m.mutation_type {
            crate::schema::types::operations::MutationType::Purge => purges.push(m),
            crate::schema::types::operations::MutationType::Delete => deletes.push(m),
            _ => rest.push(m),
        }
    }
    (purges, deletes, rest)
}
