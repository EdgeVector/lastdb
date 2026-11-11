//! Mutation Manager — handles all mutation operations.

pub mod portable_checkpoint;
mod portable_definitions;
mod portable_journal;
pub mod portable_replay;
mod validation;
mod write;
mod write_effects;
mod write_records;

use std::collections::{BTreeMap, HashMap};
use std::path::PathBuf;
use std::sync::Arc;

use crate::db_operations::search_index::inbox_fields::{inbox_fields, searchable_fields};
use crate::db_operations::search_index::{
    IndexChange, IndexChangeBatch, IndexChangeKind, SearchOutboxSink,
};
use crate::db_operations::DbOperations;
use crate::schema::types::operations::MutationType;
use crate::schema::types::{KeyValue, Mutation, Schema};
use crate::schema::SchemaCore;

/// Whether the acknowledgment claims disk.
///
/// The default write path acknowledges after the resident commit and lets the
/// per-schema FIFO lane write the envelope afterwards, so the honest answer is
/// [`Self::Queued`]. A caller that waited for the local persist token gets
/// [`Self::Durable`]. Design law §4.8: the response states the durability state
/// separately rather than letting `success` imply it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ResidentDurability {
    /// The envelope is on the persist lane; disk has not been waited for.
    Queued,
    /// The local persist token completed before this receipt was returned.
    Durable,
}

impl ResidentDurability {
    /// Wire spelling, stable for the `/api/mutation` and `/api/mutations/batch`
    /// payloads.
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Queued => "queued",
            Self::Durable => "durable",
        }
    }
}

/// Per-class operation counts for one resident commit.
///
/// Counts cover complete records accepted in the native transaction.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ResidentCommitOperations {
    /// `MutationType::Create` rows applied.
    pub created: u64,
    /// `MutationType::Replace` rows applied.
    pub updated: u64,
    /// `MutationType::Delete` and `MutationType::Purge` rows applied.
    pub deleted: u64,
}

impl ResidentCommitOperations {
    /// Every complete operation accepted by this commit.
    #[must_use]
    pub fn total(&self) -> u64 {
        self.created
            .saturating_add(self.updated)
            .saturating_add(self.deleted)
    }

    /// Fold `other` into this set. Hard-erasure peels and the create/update
    /// pipeline each produce a partial set for the same logical batch.
    pub fn merge(&mut self, other: Self) {
        self.created = self.created.saturating_add(other.created);
        self.updated = self.updated.saturating_add(other.updated);
        self.deleted = self.deleted.saturating_add(other.deleted);
    }
}

/// Stage times for one resident commit, in microseconds.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ResidentCommitStages {
    /// Record preparation for every schema in the batch.
    pub prepare_us: u64,
    /// Whole-batch wall clock to the acknowledgment, including finalize.
    pub ack_us: u64,
}

/// One logical resident commit's answer to its caller.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResidentCommitReceipt {
    /// One server-generated identity per accepted input, in caller order.
    pub mutation_ids: Vec<String>,
    /// Complete-record revisions, indexed by the server-generated mutation ID.
    pub record_revisions: BTreeMap<String, String>,
    /// The one committed revision for the primary record and every exact
    /// projection in this batch (`None` only for an empty batch).
    pub revision: Option<u64>,
    /// Per-class operation counts.
    pub operations: ResidentCommitOperations,
    /// Whether the acknowledgment claims disk.
    pub durability: ResidentDurability,
    /// Stage times for this commit.
    pub stages: ResidentCommitStages,
}

impl ResidentCommitReceipt {
    /// An empty commit: no mutation reached the pipeline.
    #[must_use]
    pub fn empty() -> Self {
        Self {
            mutation_ids: Vec::new(),
            record_revisions: BTreeMap::new(),
            revision: None,
            operations: ResidentCommitOperations::default(),
            durability: ResidentDurability::Queued,
            stages: ResidentCommitStages::default(),
        }
    }
}

/// Manages mutation operations for the FoldDB system
pub struct MutationManager {
    /// Database operations for persistence
    db_ops: Arc<DbOperations>,
    /// Schema manager for schema operations
    schema_manager: Arc<SchemaCore>,
    /// Signing keypair for the author clock
    signer: Arc<crate::security::Ed25519KeyPair>,
    /// Explicit Search app inbox for this database instance. Daemon boots pass
    /// this so two `Host::boot` calls in one process do not race through
    /// process-global `LASTDB_HOME`.
    search_outbox_inbox: Option<PathBuf>,
    /// Resident high-water author clock for this device. Boot point-loads the
    /// durable state once; requests advance and sign in memory and persist the
    /// high-water state atomically in the native write batch.
    author_clock_state: std::sync::Mutex<crate::schema::types::MutationAuthorClockState>,
}

impl MutationManager {
    /// Creates a new MutationManager instance
    pub fn new(
        db_ops: Arc<DbOperations>,
        schema_manager: Arc<SchemaCore>,
        signer: Arc<crate::security::Ed25519KeyPair>,
        search_outbox_inbox: Option<PathBuf>,
        author_clock_state: crate::schema::types::MutationAuthorClockState,
    ) -> Self {
        Self {
            db_ops,
            schema_manager,
            signer,
            search_outbox_inbox,
            author_clock_state: std::sync::Mutex::new(author_clock_state),
        }
    }

    /// Build one Search change batch without durable side effects. An upsert
    /// carries only the searchable fields, with file keys removed.
    fn build_index_change_batch<'a>(
        schema_name: &str,
        schema: &Schema,
        schema_mutations: impl ExactSizeIterator<Item = &'a Mutation>,
        mutation_key_values: &[KeyValue],
    ) -> Option<IndexChangeBatch> {
        if !crate::db_operations::search_index::queue_policy::enabled() {
            return None;
        }
        let searchable = searchable_fields(schema);
        let mut changes = Vec::with_capacity(schema_mutations.len());
        for (mutation, key_value) in schema_mutations.zip(mutation_key_values) {
            match mutation.mutation_type {
                MutationType::Create | MutationType::Replace => {
                    changes.push(IndexChange {
                        mutation_id: mutation.uuid.clone(),
                        kind: IndexChangeKind::Upsert,
                        key_value: key_value.clone(),
                        record_fields: inbox_fields(&mutation.record_fields, searchable.as_ref()),
                    });
                }
                // Purge is hard-erase: Search must drop vectors the same way
                // as Delete. Skipping Purge left stale embeddings forever once
                // delete routes through purge (Search only reacts to
                // IndexChangeKind::Tombstone).
                MutationType::Delete | MutationType::Purge => {
                    changes.push(IndexChange {
                        mutation_id: mutation.uuid.clone(),
                        kind: IndexChangeKind::Tombstone,
                        key_value: key_value.clone(),
                        record_fields: HashMap::default(),
                    });
                }
            }
        }
        if changes.is_empty() {
            return None;
        }
        Some(IndexChangeBatch {
            format_version: 1,
            schema_name: schema_name.to_string(),
            searchable_fields: searchable,
            changes,
        })
    }

    /// Send one batch after a native commit. A sink error becomes a storage error.
    pub(crate) fn deliver_search_outbox(
        inbox: Option<PathBuf>,
        batch: &IndexChangeBatch,
    ) -> Result<(), crate::storage::StorageError> {
        SearchOutboxSink::deliver(inbox, batch)
            .map_err(|error| crate::storage::StorageError::BackendError(error.to_string()))
    }
}
