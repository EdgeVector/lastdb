//! Mutation Manager — public write entry points, native transaction commits, and Search outbox.

mod validation;
mod write_effects;
mod write_records;

use std::collections::{BTreeMap, HashMap};
use std::path::PathBuf;
use std::sync::Arc;

use futures::{future::BoxFuture, FutureExt};
use lastdb_model::portable_mutation::Stamp;

use crate::db_operations::search_index::inbox_fields::{inbox_fields, searchable_fields};
use crate::db_operations::search_index::{
    deliver_outbox, IndexChange, IndexChangeBatch, IndexChangeKind,
};
use crate::db_operations::DbOperations;
use crate::schema::mutation_author::sign_mutation_author_clock;
use crate::schema::types::operations::MutationType;
use crate::schema::types::{KeyValue, Mutation, Schema};
use crate::schema::{SchemaCore, SchemaError};
use crate::storage::disk_pack::transaction;

pub(super) struct AuthoredMutation {
    pub(super) mutation: Mutation,
    pub(super) stamp: Stamp,
}

/// Whether the acknowledgment claims disk.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ResidentDurability {
    Queued,
    Durable,
}

impl ResidentDurability {
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Queued => "queued",
            Self::Durable => "durable",
        }
    }
}

/// Per-class operation counts for one resident commit.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ResidentCommitOperations {
    pub created: u64,
    pub updated: u64,
    pub deleted: u64,
}

impl ResidentCommitOperations {
    #[must_use]
    pub fn total(&self) -> u64 {
        self.created
            .saturating_add(self.updated)
            .saturating_add(self.deleted)
    }
}

/// Stage times for one resident commit, in microseconds.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ResidentCommitStages {
    pub prepare_us: u64,
    pub ack_us: u64,
}

/// One logical resident commit's answer to its caller.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResidentCommitReceipt {
    pub mutation_ids: Vec<String>,
    pub record_revisions: BTreeMap<String, String>,
    pub revision: Option<u64>,
    pub operations: ResidentCommitOperations,
    pub durability: ResidentDurability,
    pub stages: ResidentCommitStages,
}

impl ResidentCommitReceipt {
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

/// Manages mutation operations for the FoldDB system.
pub struct MutationManager {
    db_ops: Arc<DbOperations>,
    schema_manager: Arc<SchemaCore>,
    signer: Arc<crate::security::Ed25519KeyPair>,
    search_outbox_inbox: Option<PathBuf>,
    author_clock_state: std::sync::Mutex<crate::schema::types::MutationAuthorClockState>,
}

impl MutationManager {
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

    pub fn write_mutations(
        &self,
        mutations: Vec<Mutation>,
    ) -> BoxFuture<'_, Result<Vec<String>, SchemaError>> {
        async move { Ok(self.write_mutations_receipt(mutations).await?.mutation_ids) }.boxed()
    }

    pub async fn write_mutations_receipt(
        &self,
        mut mutations: Vec<Mutation>,
    ) -> Result<ResidentCommitReceipt, SchemaError> {
        Box::pin(async move {
            let now = crate::clock::unix_nanos();
            self.validate_mutation_batch(&mutations)?;
            if mutations.is_empty() {
                return Ok(ResidentCommitReceipt::empty());
            }
            self.prepare_mutation_author_clocks(&mut mutations, now)?;
            let revision = mutations.first().map(|m| m.logical_counter);
            self.reject_blocked_mutation_targets(&mutations)?;
            for mutation in &mutations {
                mutation.validate_record_intent()?;
            }
            let start = std::time::Instant::now();
            let mut receipt = self.commit_native_batch(mutations, start).await?;
            receipt.revision = revision;
            Ok(receipt)
        })
        .await
    }

    fn prepare_mutation_author_clocks(
        &self,
        mutations: &mut [Mutation],
        now: u64,
    ) -> Result<(), SchemaError> {
        let local_writer_id = self.signer.public_key_base64();
        let mut resident_state = self
            .author_clock_state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let mut state = *resident_state;
        let (mut times, logical_counter) = state
            .advance_local_batch(now, mutations.len())
            .ok_or_else(|| {
                SchemaError::InvalidData("mutation author-clock range exhausted".into())
            })?;
        for mutation in mutations {
            mutation.imported_written_at =
                Some(times.next().expect("one reserved time per local mutation"));
            mutation.logical_counter = logical_counter;
            mutation.author_clock_writer_id = local_writer_id.clone();
            mutation.author_clock_signature_version =
                crate::schema::types::MUTATION_AUTHOR_SIGNATURE_VERSION;
            mutation.author_clock_signature =
                sign_mutation_author_clock(mutation, self.signer.as_ref());
        }
        *resident_state = state;
        Ok(())
    }

    async fn commit_native_batch(
        &self,
        mutations: Vec<Mutation>,
        start: std::time::Instant,
    ) -> Result<ResidentCommitReceipt, SchemaError> {
        self.require_replay_cutoff(&mutations).await?;
        let runtime = self.db_ops.native_runtime();
        let mut stages = ResidentCommitStages::default();
        let (mutation_ids, record_revisions, operations) =
            transaction::request_scope(Arc::clone(&runtime), async {
                runtime.record_read_stage("request-start", mutations.len());
                if let Some(admission) = mutations.iter().find_map(|m| m.contribution.as_ref()) {
                    if mutations.len() != 1 {
                        return Err(SchemaError::InvalidData(
                            "a contribution requires one native mutation".into(),
                        ));
                    }
                    if self
                        .db_ops
                        .metadata()
                        .stage_contribution_receipt(&admission.receipt_id, &admission.intent_digest)
                        .await?
                    {
                        return Ok((
                            vec![mutations[0].uuid.clone()],
                            BTreeMap::new(),
                            ResidentCommitOperations::default(),
                        ));
                    }
                }
                let mut record_revisions = BTreeMap::new();
                let operations = count_applied_operations(&mutations);
                let schemas: Vec<&str> = mutations.iter().map(|m| m.schema_name.as_str()).collect();
                let stamps = self.db_ops.metadata().stage_local_stamps(&schemas).await?;
                for (mutation, stamp) in mutations.iter().zip(&stamps) {
                    record_revisions.insert(mutation.uuid.clone(), stamp.revision());
                }
                self.db_ops
                    .records()
                    .stage_accepted_mutations(&mutations)
                    .await?;
                let mutation_ids = mutations.iter().map(|m| m.uuid.clone()).collect();
                let authored = mutations
                    .into_iter()
                    .zip(stamps)
                    .map(|(mutation, stamp)| AuthoredMutation { mutation, stamp })
                    .collect();
                let group_count = self
                    .stage_record_groups(authored, &mut stages, &runtime)
                    .await?;
                runtime.record_read_stage("stored-stages-done", group_count);
                self.stage_native_author_clock().await?;
                Ok::<_, SchemaError>((mutation_ids, record_revisions, operations))
            })
            .await?;
        stages.ack_us = u64::try_from(start.elapsed().as_micros()).unwrap_or(u64::MAX);
        Ok(ResidentCommitReceipt {
            mutation_ids,
            record_revisions,
            revision: None,
            operations,
            durability: ResidentDurability::Durable,
            stages,
        })
    }

    async fn stage_native_author_clock(&self) -> Result<(), SchemaError> {
        let writer = self.signer.public_key_base64();
        let state = *self
            .author_clock_state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let mut durable = self
            .db_ops
            .metadata()
            .author_clock(&writer)
            .await?
            .unwrap_or_default();
        durable.observe_remote(state.physical_nanos, state.logical_counter);
        self.db_ops
            .metadata()
            .stage_author_clock(&writer, &durable)
            .await
    }

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
            let (kind, record_fields) = match mutation.mutation_type {
                MutationType::Create | MutationType::Replace => (
                    IndexChangeKind::Upsert,
                    inbox_fields(&mutation.record_fields, searchable.as_ref()),
                ),
                MutationType::Delete | MutationType::Purge => {
                    (IndexChangeKind::Tombstone, HashMap::default())
                }
            };
            changes.push(IndexChange {
                mutation_id: mutation.uuid.clone(),
                kind,
                key_value: key_value.clone(),
                record_fields,
            });
        }
        (!changes.is_empty()).then(|| IndexChangeBatch {
            format_version: 1,
            schema_name: schema_name.to_string(),
            searchable_fields: searchable,
            changes,
        })
    }

    pub(crate) fn deliver_search_outbox(
        inbox: Option<PathBuf>,
        batch: &IndexChangeBatch,
    ) -> Result<(), crate::storage::StorageError> {
        deliver_outbox(inbox, batch)
            .map_err(|error| crate::storage::StorageError::BackendError(error.to_string()))
    }
}

fn count_applied_operations(mutations: &[Mutation]) -> ResidentCommitOperations {
    let mut operations = ResidentCommitOperations::default();
    for mutation in mutations {
        match mutation.mutation_type {
            MutationType::Create => operations.created += 1,
            MutationType::Replace => operations.updated += 1,
            MutationType::Delete | MutationType::Purge => operations.deleted += 1,
        }
    }
    operations
}
