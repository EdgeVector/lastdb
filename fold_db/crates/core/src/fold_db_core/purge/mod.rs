//! User Delete and physical Purge dispatch.
//!
//! Purge is the compliance/GDPR verb — it irreversibly removes a
//! `(schema, key)` record: the molecule entries for the key, its `tv:`
//! tip-version chain, the atoms only that key referenced (live head AND
//! superseded), the legacy `history:` rows that mention the key's
//! field_key, and the record's resident (T0) tips and atom bodies.
//! Both user Delete forms use [`converge_delete_tips`]: they remove the
//! selected molecule tips and leave shared atom bodies for later reclamation.
//! `must_exist` adds a caller precondition; it does not select physical Purge.
//! The native worker checks that condition before it resolves related keys
//! and removes their tips in the same complete transaction. Explicit Purge
//! alone reaches [`purge_records_bulk`] and its atom-reachability erasure.
//!
//! Current logical writes keep only the current tip. They create no `tv:`
//! history nodes or archived-tip reverse references. An updated-then-purged
//! current key therefore has no history chain to walk; superseded atom bodies
//! become reclaim candidates rather than retained history. `gc-atoms` reaps
//! unreferenced bodies, then `gc-file-blobs` reaps blobs nothing references.
//!
//! The explicit physical cleanup routes still understand existing `tv:`
//! chains and old `history:` rows. Their historical reach comes from the
//! tip-version chain; current logical writes do not create either format.
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
//! caller normally uses [`PurgeMissingPolicy::Skip`]: missing targets are
//! filtered out and a fully-missing batch is a successful no-op. A user
//! Delete with `must_exist` refuses a missing caller key before tip removal.
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

mod absent;
mod bulk;
mod chain_walk;
mod converge;
mod converge_candidates;
mod converge_plan;
mod converge_steps;
mod core_live;
mod delete_barrier_flush;
mod delete_candidates;
mod guarded;
mod guarded_prepare;
mod guarded_steps;
mod helpers;
mod storage_slot;
mod target_chain;

pub(super) use absent::absent_purge_targets;
pub(super) use bulk::{
    plan_guarded_complement_retained, purge_records_bulk, validate_purge_targets_present,
};
pub(super) use converge::converge_delete_tips;
pub(crate) use delete_candidates::{delete_candidates_for_key, plan_normal_delete_barriers};
use guarded::execute_guarded_purge;

pub use storage_slot::*;

// Re-export helpers for sibling modules under `fold_db_core`.
pub(super) use helpers::{
    atom_ref_cutover_ready, collect_core_live_atom_uuids, collect_event_atom_uuids,
    collect_live_atom_uuids, collect_retained_chain_atoms_for_storage_slots, collect_target_chain,
    current_atom_for_key, describe_key, field_key_matches_value, refresh_runtime_field_molecules,
    refresh_runtime_field_molecules_for_purge_keys, remove_key_from_field, storage_form_keys,
    validate_purge_key_shape,
};

type OwnedMoleculeKeyRemoval = (
    String,
    crate::db_operations::MoleculeData,
    Vec<(String, String)>,
    Option<String>,
);

/// Internal erasure label and missing-target wording. Both Delete labels
/// remove tips only; the must-exist label preserves the caller's loud miss.
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
    pub(super) fn is_delete(self) -> bool {
        matches!(self, Self::Delete | Self::DeleteMustExist)
    }

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

/// Compliance Purge and must-exist Delete refuse a missing caller key.
/// Ordinary Delete and derived peer cleanup skip a missing key.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum PurgeMissingPolicy {
    /// Loud `SchemaError::InvalidData` before any mutation.
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

/// What every purge entry point is aimed at: one schema of one node.
#[derive(Clone, Copy)]
pub(super) struct PurgeScope<'a> {
    pub(super) db_ops: &'a Arc<DbOperations>,
    pub(super) schema_manager: &'a Arc<SchemaCore>,
    pub(super) schema_name: &'a str,
}

/// How a purge proves reachability and what it evicts afterwards.
pub(super) struct PurgeMode {
    pub(super) reachability: PurgeReachability,
    pub(super) precomputed_retained: Option<HashSet<String>>,
    /// Who asked, for the delete ledger row.
    pub(super) caller: &'static str,
}

/// A planned destructive purge, ready for [`guarded::execute_guarded_purge`].
pub(super) struct GuardedPurge {
    pub(super) candidate_atoms: HashSet<String>,
    pub(super) history_keys: Vec<String>,
    pub(super) tip_version_keys: Vec<String>,
    pub(super) atom_ref_edge_keys: Vec<String>,
    pub(super) atom_ref_v2_edge_keys: Vec<String>,
    pub(super) history_referenced_by_retained: HashSet<String>,
    pub(super) records_purged: usize,
    pub(super) mode: PurgeMode,
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
