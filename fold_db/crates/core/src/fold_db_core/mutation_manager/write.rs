//! Public write entry points and native batch transaction orchestration.

use std::collections::BTreeMap;
use std::sync::Arc;

use lastdb_model::portable_mutation::Stamp;

use super::{
    MutationManager, ResidentCommitOperations, ResidentCommitReceipt, ResidentCommitStages,
    ResidentDurability,
};
use crate::schema::mutation_author::sign_mutation_author_clock;
use crate::schema::types::{Mutation, MutationType};
use crate::schema::SchemaError;
use crate::storage::disk_pack::{transaction, PackRuntime};
use crate::storage::format::metadata::JournalSchema;

pub(super) struct AuthoredMutation {
    pub(super) mutation: Mutation,
    pub(super) stamp: Stamp,
    pub(super) shared_schema: Option<JournalSchema>,
}

/// Mutable state threaded through the phases of one batch write.
#[derive(Clone, Copy)]
pub(super) struct BatchState {
    pub(super) force_durable: bool,
    pub(super) stages: ResidentCommitStages,
}

impl BatchState {
    fn new(force_durable: bool) -> Self {
        Self {
            force_durable,
            stages: ResidentCommitStages::default(),
        }
    }
}

pub(super) struct NativeRequest {
    pub runtime: Arc<PackRuntime>,
    pub mutations: Vec<Mutation>,
}

struct NativeCommit {
    mutation_ids: Vec<String>,
    record_revisions: BTreeMap<String, String>,
    operations: ResidentCommitOperations,
}

impl MutationManager {
    /// [`Self::write_mutations_receipt`] returning only the accepted input
    /// mutation ids, in caller order.
    ///
    /// The future is boxed here. A caller that spawns a task on it would
    /// otherwise make the compiler prove `Send` through the whole write graph
    /// from the spawn site, and that proof overflows the recursion limit.
    pub fn write_mutations(
        &self,
        mutations: Vec<Mutation>,
    ) -> futures::future::BoxFuture<'_, Result<Vec<String>, SchemaError>> {
        use futures::FutureExt;
        async move { Ok(self.write_mutations_receipt(mutations).await?.mutation_ids) }.boxed()
    }

    /// Write a batch of mutations and answer with the full
    /// [`ResidentCommitReceipt`].
    pub async fn write_mutations_receipt(
        &self,
        mutations: Vec<Mutation>,
    ) -> Result<ResidentCommitReceipt, SchemaError> {
        let future: futures::future::BoxFuture<'_, Result<ResidentCommitReceipt, SchemaError>> =
            Box::pin(async move {
                let now = crate::clock::unix_nanos();
                self.write_mutations_batch_with_receipt_admitted(mutations, now)
                    .await
            });
        future.await
    }

    pub(super) async fn write_mutations_batch_with_receipt_admitted(
        &self,
        mut mutations: Vec<Mutation>,
        now: u64,
    ) -> Result<ResidentCommitReceipt, SchemaError> {
        self.validate_mutation_batch(&mutations)?;
        self.prepare_mutation_author_clocks(&mut mutations, now)?;
        // One logical counter per caller batch. Empty input has none.
        let revision = mutations.first().map(|mutation| mutation.logical_counter);
        let mut receipt = self.write_mutations_batch_inner(mutations).await?;
        receipt.revision = revision;
        Ok(receipt)
    }

    fn prepare_mutation_author_clocks(
        &self,
        mutations: &mut [Mutation],
        now: u64,
    ) -> Result<(), SchemaError> {
        if mutations.is_empty() {
            return Ok(());
        }
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
            let written_at = times.next().expect("one reserved time per local mutation");
            mutation.imported_written_at = Some(written_at);
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

    pub(super) async fn write_mutations_batch_inner(
        &self,
        mutations: Vec<Mutation>,
    ) -> Result<ResidentCommitReceipt, SchemaError> {
        if mutations.is_empty() {
            return Ok(ResidentCommitReceipt::empty());
        }
        self.validate_mutation_batch(&mutations)?;
        self.reject_blocked_mutation_targets(&mutations)?;
        for mutation in &mutations {
            mutation.validate_record_intent()?;
        }
        // Complete replacements and erasures share one native request.
        // Every revision condition succeeds before any record is published.
        self.write_create_update_batch_async(mutations).await
    }

    /// Prepare one native Create/Update batch and await its durable commit.
    pub(super) async fn write_create_update_batch_async(
        &self,
        mutations: Vec<Mutation>,
    ) -> Result<ResidentCommitReceipt, SchemaError> {
        if mutations.is_empty() {
            return Ok(ResidentCommitReceipt::empty());
        }
        let force_durable = mutations
            .iter()
            .any(|mutation| mutation.synchronous == Some(true));

        self.reject_blocked_mutation_targets(&mutations)?;
        Self::log_batch_start(mutations.len());

        let start_time = std::time::Instant::now();
        let state = BatchState::new(force_durable);
        self.write_native_batch(
            NativeRequest {
                runtime: self.db_ops.native_runtime(),
                mutations,
            },
            start_time,
            state,
        )
        .await
    }

    fn log_batch_start(batch_len: usize) {
        tracing::info!(
            "write_create_update_batch_async: Starting batch of {} mutations",
            batch_len
        );
        tracing::debug!(
            "DEBUG: MutationManager::write_create_update_batch_async started for {} mutations",
            batch_len
        );
    }

    /// Report one accepted batch and its server-generated record identities.
    pub(super) fn build_commit_receipt(
        mutation_ids: Vec<String>,
        record_revisions: BTreeMap<String, String>,
        operations: ResidentCommitOperations,
        start_time: std::time::Instant,
        state: BatchState,
    ) -> ResidentCommitReceipt {
        let BatchState {
            force_durable,
            mut stages,
        } = state;
        stages.ack_us = u64::try_from(start_time.elapsed().as_micros()).unwrap_or(u64::MAX);
        ResidentCommitReceipt {
            mutation_ids,
            record_revisions,
            revision: None,
            operations,
            durability: Self::commit_durability(force_durable),
            stages,
        }
    }

    fn commit_durability(force_durable: bool) -> ResidentDurability {
        if force_durable {
            ResidentDurability::Durable
        } else {
            ResidentDurability::Queued
        }
    }

    /// Count the complete writes accepted by the native transaction.
    pub(super) fn count_applied_operations(mutations: &[Mutation]) -> ResidentCommitOperations {
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

    pub(super) async fn write_native_batch(
        &self,
        request: NativeRequest,
        start: std::time::Instant,
        mut state: BatchState,
    ) -> Result<ResidentCommitReceipt, SchemaError> {
        let committed = self.commit_native_request(request, &mut state).await?;
        Ok(Self::build_commit_receipt(
            committed.mutation_ids,
            committed.record_revisions,
            committed.operations,
            start,
            state,
        ))
    }

    async fn commit_native_request(
        &self,
        request: NativeRequest,
        state: &mut BatchState,
    ) -> Result<NativeCommit, SchemaError> {
        // Owned inputs and the startup-frozen floor need no write gate.
        // Offline maintenance retains its exclusive no-writer contract.
        self.require_replay_cutoff(&request.mutations).await?;
        state.force_durable = true;
        let committed = transaction::request_scope(Arc::clone(&request.runtime), async {
            request
                .runtime
                .record_read_stage("request-start", request.mutations.len());
            let mutations = request.mutations;
            if let Some(admission) = mutations.iter().find_map(|item| item.contribution.as_ref()) {
                if mutations.len() != 1 {
                    return Err(SchemaError::InvalidData(
                        "a contribution requires one native mutation".into(),
                    ));
                }
                let replay = self
                    .db_ops
                    .metadata()
                    .stage_contribution_receipt(&admission.receipt_id, &admission.intent_digest)
                    .await?;
                if replay {
                    return Ok(NativeCommit {
                        mutation_ids: vec![mutations[0].uuid.clone()],
                        record_revisions: BTreeMap::new(),
                        operations: ResidentCommitOperations::default(),
                    });
                }
            }
            let mut record_revisions = BTreeMap::new();
            let operations = Self::count_applied_operations(&mutations);
            let schemas: Vec<&str> = mutations
                .iter()
                .map(|mutation| mutation.schema_name.as_str())
                .collect();
            let stamps = self.db_ops.metadata().stage_local_stamps(&schemas).await?;
            for (mutation, stamp) in mutations.iter().zip(&stamps) {
                record_revisions.insert(mutation.uuid.clone(), stamp.stamp.revision());
            }
            let mutation_ids = mutations
                .iter()
                .map(|mutation| mutation.uuid.clone())
                .collect();
            let mutations = mutations
                .into_iter()
                .zip(stamps)
                .map(|(mutation, authored)| AuthoredMutation {
                    mutation,
                    stamp: authored.stamp,
                    shared_schema: authored.shared_schema,
                })
                .collect();
            // Revision conditions and record changes share this exclusive gate.
            let group_count = self
                .stage_record_groups(mutations, &mut state.stages, &request.runtime)
                .await?;
            request
                .runtime
                .record_read_stage("stored-stages-done", group_count);
            self.stage_native_author_clock().await?;
            Ok::<_, SchemaError>(NativeCommit {
                mutation_ids,
                record_revisions,
                operations,
            })
        })
        .await?;
        Ok(committed)
    }

    pub(super) async fn stage_native_author_clock(&self) -> Result<(), SchemaError> {
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
            .await?;
        Ok(())
    }
}
