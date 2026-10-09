//! Resident and storage-slot purge envelopes and the persist-lane job enum.

use super::*;

#[derive(Debug, Clone)]
pub(crate) struct ResidentPurgeSlot {
    pub(in crate::fold_db_core::mutation_manager) molecule_uuid: String,
    pub(in crate::fold_db_core::mutation_manager) hash: String,
    pub(in crate::fold_db_core::mutation_manager) range: String,
}

pub(crate) struct PurgeCompletion {
    pub(in crate::fold_db_core::mutation_manager) result:
        Result<Vec<String>, crate::schema::SchemaError>,
    pub(in crate::fold_db_core::mutation_manager) phases: crate::request_phases::RequestPhaseTotals,
}

pub(super) enum PurgeAttemptError {
    /// The store or a later durable stage failed. Keep the envelope at the
    /// lane head and retry it.
    Retry(SchemaError),
    /// The one-time read proved that a loud target was absent. Complete the
    /// envelope with the caller error and remove its resident overlays.
    Missing(SchemaError),
}

pub(crate) struct PurgeEnvelope {
    /// The request author clock must reach durable metadata before this data.
    pub(in crate::fold_db_core::mutation_manager) author_clock_barrier:
        Option<super::super::super::author_clock::AuthorClockPersistBarrier>,
    pub(in crate::fold_db_core::mutation_manager) erasures: Vec<crate::schema::types::Mutation>,
    pub(in crate::fold_db_core::mutation_manager) storage_prefix: Option<String>,
    pub(in crate::fold_db_core::mutation_manager) missing:
        crate::fold_db_core::purge::PurgeMissingPolicy,
    /// True after the lane proves that every `Refuse` target exists. All
    /// durable attempts after this point use `Skip`, so partial progress can
    /// converge without a false terminal miss.
    pub(in crate::fold_db_core::mutation_manager) validated_present: bool,
    /// A prior core attempt can erase rows before its schema store, schema
    /// reload, or flush fails. The next attempt must repeat those final stages
    /// even when its idempotent `Skip` pass finds no durable trace.
    pub(in crate::fold_db_core::mutation_manager) retry_requires_finalize: bool,
    pub(in crate::fold_db_core::mutation_manager) search_batch:
        Option<crate::db_operations::search_index::IndexChangeBatch>,
    pub(in crate::fold_db_core::mutation_manager) retention_keys:
        Option<Vec<crate::schema::types::KeyValue>>,
    pub(in crate::fold_db_core::mutation_manager) verb: crate::fold_db_core::purge::HardEraseVerb,
    pub(in crate::fold_db_core::mutation_manager) tombstone_id: u64,
    pub(in crate::fold_db_core::mutation_manager) schema_tombstones: Vec<(String, String, String)>,
    pub(in crate::fold_db_core::mutation_manager) slots: Vec<ResidentPurgeSlot>,
    /// Retained-chain atoms collected off the persist lane. `None` for
    /// barrierless cutover and for Skip-Delete converge.
    pub(in crate::fold_db_core::mutation_manager) guarded_complement_retained:
        Option<std::collections::HashSet<String>>,
    pub(in crate::fold_db_core::mutation_manager) completion:
        Option<tokio::sync::oneshot::Sender<PurgeCompletion>>,
    /// Set when the purge is terminal. The lane sends it after byte release.
    pub(in crate::fold_db_core::mutation_manager) pending_completion: Option<PurgeCompletion>,
}

/// Retained durable intent for one legacy storage-slot drain page.
///
/// `evidence_checkpoint` records a slot before its first destructive store
/// operation. `search_batch` is built once and then retained byte-for-byte
/// across Search delivery retries.
pub(crate) struct StorageSlotPurgeEnvelope {
    pub(in crate::fold_db_core::mutation_manager) schema_name: String,
    pub(in crate::fold_db_core::mutation_manager) targets:
        Vec<crate::fold_db_core::purge::StorageSlotPurgeTarget>,
    pub(in crate::fold_db_core::mutation_manager) evidence_checkpoint:
        Vec<crate::fold_db_core::purge::StorageSlotPurgeEvidence>,
    /// True after the destructive core and all schema finalization complete.
    /// A Search-only retry must not enter that core again.
    pub(in crate::fold_db_core::mutation_manager) durable_complete: bool,
    /// A failed core attempt can remove rows before schema finalization fails.
    /// The next successful attempt must replay those final stages.
    pub(in crate::fold_db_core::mutation_manager) retry_requires_finalize: bool,
    pub(in crate::fold_db_core::mutation_manager) search_batch:
        Option<crate::db_operations::search_index::IndexChangeBatch>,
    pub(in crate::fold_db_core::mutation_manager) completion:
        Option<tokio::sync::oneshot::Sender<StorageSlotPurgeCompletion>>,
    /// Set when the page is terminal. The lane sends it after byte release.
    pub(in crate::fold_db_core::mutation_manager) pending_completion:
        Option<StorageSlotPurgeCompletion>,
}

pub(crate) struct StorageSlotPurgeCompletion {
    pub(in crate::fold_db_core::mutation_manager) evidence:
        Vec<crate::fold_db_core::purge::StorageSlotPurgeEvidence>,
    pub(in crate::fold_db_core::mutation_manager) phases: crate::request_phases::RequestPhaseTotals,
}

/// One ordered durable action on a schema persist lane.
pub(crate) enum LanePersistJob {
    Write {
        job: Box<DeferredPersistJob>,
        // Only durable callers await these counts. Post-ack resident work
        // belongs to the worker metrics, never to an already-returned request.
        counts: crate::request_phases::RequestCounts,
        counts_completion:
            Option<tokio::sync::oneshot::Sender<crate::request_phases::RequestCounts>>,
        _pending_task: crate::fold_db_core::pending_task_tracker::PendingTask,
    },
    Purge {
        job: Box<PurgeEnvelope>,
        _pending_task: crate::fold_db_core::pending_task_tracker::PendingTask,
    },
    StorageSlotPurge {
        job: Box<StorageSlotPurgeEnvelope>,
        _pending_task: crate::fold_db_core::pending_task_tracker::PendingTask,
    },
}
