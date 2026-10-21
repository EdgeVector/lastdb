//! Purge envelope and the persist-lane job enum.

use super::*;

pub(crate) struct PurgeCompletion {
    pub(in crate::fold_db_core::mutation_manager) result:
        Result<Vec<String>, crate::schema::SchemaError>,
    pub(in crate::fold_db_core::mutation_manager) phases: crate::request_phases::RequestPhaseTotals,
}

pub(super) enum PurgeAttemptError {
    /// The store or a later durable stage failed. Keep the envelope at the
    /// lane head and retry it.
    Retry(SchemaError),
    /// A caller condition or private native preparation failed. Complete the
    /// envelope with that error; the native scope discards every private stage.
    Rejected(SchemaError),
}

impl From<crate::storage::StorageError> for PurgeAttemptError {
    fn from(error: crate::storage::StorageError) -> Self {
        // A remembered read-limit refusal can replace the inner preparation
        // result at the native read boundary. It still precedes publication
        // and must reach the caller rather than hold the lane in a retry loop.
        match error {
            crate::storage::StorageError::ReadResourceLimit { .. }
            | crate::storage::StorageError::MutationLimit { .. } => Self::Rejected(error.into()),
            error => Self::Retry(error.into()),
        }
    }
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
    pub(in crate::fold_db_core::mutation_manager) retention_keys:
        Option<Vec<crate::schema::types::KeyValue>>,
    pub(in crate::fold_db_core::mutation_manager) verb: crate::fold_db_core::purge::HardEraseVerb,
    pub(in crate::fold_db_core::mutation_manager) completion:
        Option<tokio::sync::oneshot::Sender<PurgeCompletion>>,
    /// Set when the purge is terminal. The lane sends it after byte release.
    pub(in crate::fold_db_core::mutation_manager) pending_completion: Option<PurgeCompletion>,
}

/// One ordered durable action on a schema persist lane.
pub(crate) enum LanePersistJob {
    Purge {
        job: Box<PurgeEnvelope>,
        _pending_task: crate::fold_db_core::pending_task_tracker::PendingTask,
    },
}
