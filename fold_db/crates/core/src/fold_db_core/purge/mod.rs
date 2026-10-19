//! User Delete and physical Purge.
//!
//! Both verbs remove the tips of the keys they name and let the atom counter
//! decide which bodies go. `erase.rs` reads the tips of the keys with one batch
//! read inside the native write gate, hands every tip change to the planner in
//! `db_operations::atom_store::counter`, and stages one batch. A body goes in
//! that batch when its count reaches zero. A purge writes no delete barrier. A
//! delete writes its barrier, so a later replayed Put cannot revive the key.
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

mod delete_candidates;
mod erase;
mod helpers;
mod removals;
mod targets;

pub(crate) use delete_candidates::{delete_candidates_for_key, plan_normal_delete_barriers};
pub(super) use erase::{erase_keys, EraseOptions};
pub(super) use targets::{absent_purge_targets, validate_purge_targets_present};

pub(super) use helpers::{describe_key, validate_purge_key_shape};

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

/// Per-batch report for an erase. Counts are for the whole batch;
/// `records_purged` is what lets a caller log one line instead of N.
pub(super) struct BulkPurgeReport {
    pub records_purged: usize,
    /// Number of `atom:` bodies deleted because their count reached zero.
    pub atom_rows_deleted: usize,
    /// Exact molecule slots removed by this erase.
    pub target_slots: u64,
    /// Keys that a purge left alone, because a tip of the key is newer than
    /// the cutoff of its mutation.
    pub spared_keys: Vec<KeyValue>,
}

impl BulkPurgeReport {
    fn empty() -> Self {
        Self {
            records_purged: 0,
            atom_rows_deleted: 0,
            target_slots: 0,
            spared_keys: Vec::new(),
        }
    }
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
/// Returns `(purges, deletes, rest)`. Both purge and delete run the erase in
/// `erase.rs`; they differ in [`PurgeMissingPolicy`] (compliance loud-fail vs
/// delete idempotent skip) and in the delete barrier.
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
