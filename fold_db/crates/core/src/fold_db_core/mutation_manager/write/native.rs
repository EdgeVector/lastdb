//! One native request owns every schema's stored bytes before publication.

use super::*;
use crate::storage::disk_pack::{transaction, PackRuntime};
use crate::write_preparation::measure;
use std::sync::Arc;

pub(super) struct NativeRequest {
    pub runtime: Arc<PackRuntime>,
    pub mutations: Vec<Mutation>,
    pub author_clock: Option<super::super::author_clock::AuthorClockPersistBarrier>,
}

struct NativeCommit {
    mutation_ids: Vec<String>,
    record_revisions: std::collections::BTreeMap<String, String>,
    operations: ResidentCommitOperations,
}

impl MutationManager {
    pub(super) async fn write_native_batch(
        &self,
        request: NativeRequest,
        start: std::time::Instant,
        mut state: super::create_update_batch::BatchState,
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
        state: &mut super::create_update_batch::BatchState,
    ) -> Result<NativeCommit, SchemaError> {
        // The clock queue uses native storage too. Complete its barrier before
        // taking the request's exclusive storage gate, never while owning it.
        if let Some(clock) = request.author_clock {
            clock.wait().await?;
        }
        // Owned inputs and the startup-frozen floor need no write gate.
        // Offline maintenance retains its exclusive no-writer contract.
        self.require_replay_cutoff(&request.mutations).await?;
        state.force_durable = true;
        let committed = transaction::request_scope(Arc::clone(&request.runtime), async {
            request
                .runtime
                .record_read_stage("request-start", request.mutations.len());
            let mutations = request.mutations;
            let mut record_revisions = std::collections::BTreeMap::new();
            let notifications = crate::db_operations::ChangeFeedStore::native_events(&mutations);
            let operations = Self::count_applied_operations(&mutations);
            let stamps = self
                .db_ops
                .metadata()
                .stage_local_stamps(mutations.len())
                .await?;
            for (mutation, stamp) in mutations.iter().zip(&stamps) {
                record_revisions.insert(mutation.uuid.clone(), stamp.revision());
            }
            let mutation_ids = mutations
                .iter()
                .map(|mutation| mutation.uuid.clone())
                .collect();
            let mutations = mutations
                .into_iter()
                .zip(stamps)
                .map(|(mutation, stamp)| AuthoredMutation { mutation, stamp })
                .collect();
            // Revision conditions and record changes share this exclusive gate.
            let group_count = self
                .stage_record_groups(
                    mutations,
                    &mut state.timing_breakdown,
                    &mut state.stages,
                    &request.runtime,
                )
                .await?;
            request
                .runtime
                .record_read_stage("stored-stages-done", group_count);
            measure("author_clock_us", self.stage_native_author_clock()).await?;
            measure(
                "change_feed_us",
                self.db_ops.change_feed().stage_native_events(notifications),
            )
            .await?;
            measure("memory_reserve_us", async {
                self.reserve_native_core_memory(&request.runtime)
            })
            .await?;
            Ok::<_, SchemaError>(NativeCommit {
                mutation_ids,
                record_revisions,
                operations,
            })
        })
        .await?;
        Ok(committed)
    }

    pub(in crate::fold_db_core::mutation_manager) async fn stage_native_author_clock(
        &self,
    ) -> Result<(), SchemaError> {
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

    /// Charge the request's staged core memory to the defer window, or make
    /// the request wait for disk when the window is full.
    pub(in crate::fold_db_core::mutation_manager) fn reserve_native_core_memory(
        &self,
        runtime: &PackRuntime,
    ) -> Result<(), SchemaError> {
        let bytes = transaction::memory_quote(runtime)?;
        if let Some(reservation) = self.try_reserve_defer(bytes) {
            transaction::retain_raw_memory_reservation(runtime, reservation)?;
        } else {
            transaction::require_durable(runtime)?;
        }
        Ok(())
    }
}
