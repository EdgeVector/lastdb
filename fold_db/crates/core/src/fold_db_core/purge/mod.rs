//! Delete removes exact current tips. Purge also removes unreferenced bodies
//! and current mutation-history events that identify the selected keys.
//! Current aref2 references authorize body deletion after tip removal; shared
//! tips and retained history remain protected. No archived-tip format is read.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use crate::db_operations::DbOperations;
use crate::schema::types::field::build_storage_key;
use crate::schema::types::key_value::KeyValue;
use crate::schema::{SchemaCore, SchemaError};

mod absent;
mod bulk;
mod converge;
mod converge_candidates;
mod converge_plan;
mod converge_steps;
mod core_live;
mod delete_barrier_flush;
mod delete_candidates;
mod guarded;
mod guarded_atoms;
mod guarded_prepare;
mod guarded_steps;
mod helpers;
mod storage_slot;
mod target_edges;

pub(super) use absent::absent_purge_targets;
pub(super) use bulk::{purge_records_bulk, validate_purge_targets_present};
pub(super) use converge::converge_delete_tips;
pub(crate) use delete_candidates::{delete_candidates_for_key, plan_normal_delete_barriers};
use guarded::execute_guarded_purge;

pub use storage_slot::*;

// Re-export helpers for sibling modules under `fold_db_core`.
pub(super) use helpers::{
    atom_ref_cutover_ready, collect_core_live_atom_uuids, collect_event_atom_uuids,
    collect_live_atom_uuids, collect_target_edges, current_atom_for_key, describe_key,
    field_key_matches_value, refresh_runtime_field_molecules,
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
    /// Retained report field; current writes create no archived tip nodes.
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

/// What every purge entry point is aimed at: one schema in one database scope.
#[derive(Clone, Copy)]
pub(super) struct PurgeScope<'a> {
    pub(super) db_ops: &'a Arc<DbOperations>,
    pub(super) schema_manager: &'a Arc<SchemaCore>,
    pub(super) schema_name: &'a str,
    pub(super) storage_prefix: Option<&'a str>,
}

impl PurgeScope<'_> {
    /// Definitions are global; every data load uses the selected database.
    fn schema(&self) -> Result<crate::schema::Schema, SchemaError> {
        let mut schema = self
            .schema_manager
            .get_schema_metadata(self.schema_name)?
            .ok_or_else(|| {
                SchemaError::InvalidData(format!(
                    "Schema '{}' not found for purge",
                    self.schema_name
                ))
            })?;
        crate::fold_db_core::mutation_manager::helpers::clear_storage_prefix_on_schema(&mut schema);
        crate::fold_db_core::mutation_manager::helpers::apply_storage_prefix_to_schema(
            &mut schema,
            self.storage_prefix,
        );
        Ok(schema)
    }
}

/// How a purge proves reachability and what it evicts afterwards.
pub(super) struct PurgeMode {
    pub(super) reachability: PurgeReachability,
    /// Who asked, for the delete ledger row.
    pub(super) caller: &'static str,
}

/// A planned destructive purge, ready for [`guarded::execute_guarded_purge`].
pub(super) struct GuardedPurge {
    pub(super) candidate_atoms: HashSet<String>,
    pub(super) history_keys: Vec<String>,
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
/// only exact current tips in [`converge_delete_tips`];
/// it must not read this trace's legacy field history or atom candidates.
pub(super) struct PurgeTrace {
    /// Atoms any target referenced. A set because content-addressing means two
    /// targets can name the same atom.
    candidate_atoms: HashSet<String>,
    history_keys: Vec<String>,
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
